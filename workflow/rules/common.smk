import queue
import threading
import time
import boto3
import sqlite3

from snakemake.io import from_queue, Wildcards
from dataclasses import dataclass
from os import PathLike


@dataclass
class MetadataRecord:
    bucket: str
    key: str
    state: str


class MetadataDb:
    def __init__(self, db_path: PathLike):
        self._db_path = db_path
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self.create_table()

    def create_table(
        self,
    ):
        print(f"Creating metadata table in {self._db_path}")
        with sqlite3.connect(self._db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS metadata (
                bucket TEXT,
                key TEXT,
                state TEXT,
                PRIMARY KEY (bucket, key)
            )
            """
            )
        print("Metadata table created")

    def get_state(self, bucket: str, key: str) -> str | None:
        print(f"Getting state for {bucket}/{key}")
        with sqlite3.connect(self._db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
            SELECT state FROM metadata WHERE bucket = ? AND key = ?
            """,
                (bucket, key),
            )

            result = cursor.fetchone()
            if result is None:
                return None
            else:
                return result[0]

    def update_state(self, bucket: str, key: str, state: str):
        print(f"Updating state for {bucket}/{key} to {state}")
        with sqlite3.connect(self._db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
            INSERT OR REPLACE INTO metadata (bucket, key, state) VALUES (?, ?, ?)
            """,
                (bucket, key, state),
            )

    def records(self, bucket: str) -> list[MetadataRecord]:
        print(f"Getting records for {bucket}")
        with sqlite3.connect(self._db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
            SELECT * FROM metadata WHERE bucket = ?
            """,
                (bucket,),
            )

            return [MetadataRecord(*row) for row in cursor.fetchall()]


SENTINEL = object()
INPUT_QUEUE: queue.Queue = queue.Queue()
metadata_db = MetadataDb(config["monitor"]["metadata_db"])


def update_submission_queue(bucket_name: str, key: str):
    print(f"Updating submission queue for {bucket_name}/{key}")
    INPUT_QUEUE.put(f"results/{bucket_name}/target/{key}")


# TODO: use md5sum of key as local_case_id


def check_buckets():
    print("Initializing S3 session …")
    session = boto3.Session(
        aws_access_key_id=config["grz_inbox"]["s3_config"]["access_key_id"],
        aws_secret_access_key=config["grz_inbox"]["s3_config"]["secret_access_key"],
    )

    endpoint_url = config["grz_inbox"]["s3_config"]["endpoint_url"]
    buckets = config["grz_inbox"]["s3_config"]["buckets"]
    print(f"Monitoring buckets {buckets} …")

    print("Retrieving S3 resource …")
    s3 = session.resource("s3", endpoint_url=endpoint_url)
    sleep = int(config["monitor"].get("interval", "3600"))

    while True:
        for bucket_name in buckets:
            print(f"Checking bucket {bucket_name} for new submissions …")
            bucket_keys: set[str] = list_metadata_objects(bucket_name, s3)
            print(f"Found {len(bucket_keys)} metadata files in bucket {bucket_name}")
            bucket_db_records = metadata_db.records(bucket_name)
            bucket_db_keys = {record.key for record in bucket_db_records}

            new_keys = bucket_keys - bucket_db_keys
            existing_keys = bucket_keys & bucket_db_keys
            missing_keys = bucket_db_keys - bucket_keys

            for key in new_keys:
                print(f"Found new submission '{key}' in bucket '{bucket_name}'")
                state = "new"
                update_submission_queue(bucket_name, key)
                metadata_db.update_state(bucket_name, key, state)

            for key in existing_keys:
                print(f"Submission '{key}' in bucket '{bucket_name}' already seen")
                state = "seen"
                metadata_db.update_state(bucket_name, key, state)

            for key in missing_keys:
                print(f"Submission '{key}' in bucket '{bucket_name}' is missing")
                state = "missing"
                metadata_db.update_state(bucket_name, key, state)

            print(INPUT_QUEUE.qsize())

        time.sleep(sleep)


def list_metadata_objects(bucket_name: str, s3: boto3.resource) -> set[str]:
    print(f"Checking bucket {bucket_name}")
    bucket = s3.Bucket(bucket_name)
    keys: set[tuple[str, str]] = set()
    for obj in bucket.objects.all():
        if obj.key.endswith("metadata.json"):
            # TODO: key → local_case_id, for now use top-level directory name
            key, *_ = obj.key.split("/")
            keys.add(key)
    return keys


UPDATE_THREAD: threading.Thread = threading.Thread(target=check_buckets, daemon=False)
UPDATE_THREAD.start()


# FIXME: use this on SIGINT / keyboard interrupt, e.g. via signal module
def stop_updater(timeout: float | None = None):
    INPUT_QUEUE.put(SENTINEL)
    INPUT_QUEUE.join()
    UPDATE_THREAD.join(timeout=timeout)


def check_validation_flag(wildcards: Wildcards) -> bool:
    """
    Reads the qc_flag file to determine if QC is necessary.
    """
    bucket_name = wildcards.bucket_name
    key = wildcards.key
    with open(f"results/{bucket_name}/validation_flag/{key}") as f:
        return f.read().strip() == "true"


def check_qc_flag(wildcards: Wildcards) -> bool:
    """
    Reads the qc_flag file to determine if QC is necessary.
    """
    bucket_name = wildcards.bucket_name
    key = wildcards.key
    with open(f"results/{bucket_name}/qc_flag/{key}") as f:
        return f.read().strip() == "true"


def check_consent_flag(wildcards: Wildcards) -> bool:
    """
    Reads the consent_flag file to determine if consent is given.
    """
    bucket_name = wildcards.bucket_name
    key = wildcards.key
    with open(f"results/{bucket_name}/consent_flag/{key}") as f:
        return f.read().strip() == "true"


def get_target_public_key(wildcards: Wildcards) -> str:
    if check_consent_flag(wildcards):
        return config["ghga"]["public_key"]
    else:
        return config["grz_internal"]["public_key"]


def get_s3_config(wildcards: Wildcards) -> dict:
    if check_consent_flag(wildcards):
        return config["ghga"]["s3_config"]
    else:
        return config["grz_internal"]["s3_config"]
