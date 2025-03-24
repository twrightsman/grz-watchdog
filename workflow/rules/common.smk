import json
import queue
import threading
import time
from collections import defaultdict
from filelock import FileLock

import boto3

from snakemake.io import from_queue, Wildcards

SENTINEL = object()
INPUT_QUEUE = queue.Queue()


def update_submission_queue(bucket_name: str, key: str):
    INPUT_QUEUE.put(f"results/{bucket_name}/target/{key}")


# TODO: use md5sum of key as local_case_id
# Global state to track seen metadata files
# Read existing state from state/metadata.jsonl
seen_metadata_files = defaultdict(dict)
with open("state/metadata.jsonl", "r") as f:
    for line in f:
        obj = json.loads(line.strip())
        bucket, key, state = obj["bucket"], obj["key"], obj["state"]
        seen_metadata_files[bucket][key] = state


def check_buckets():
    session = boto3.Session(
        aws_access_key_id=config["grz_inbox"]["s3_config"]["access_key_id"],
        aws_secret_access_key=config["grz_inbox"]["s3_config"]["secret_access_key"],
    )

    endpoint_url = config["grz_inbox"]["s3_config"]["endpoint_url"]
    buckets = config["grz_inbox"]["s3_config"]["buckets"]

    s3 = session.resource("s3", endpoint_url=endpoint_url)
    state_file = config["monitor"].get("state_file", "state/metadata.jsonl")
    lock = FileLock(state_file + ".lock")

    sleep = int(config["monitor"].get("interval", "3600"))

    while True:
        keys = []
        for bucket_name in buckets:
            keys.append(list_metadata_files(bucket_name, s3))

        for bucket_name, key in keys:
            bucket_files = seen_metadata_files.get(bucket_name, {})
            if key not in bucket_files:
                state = "new"
                print(f"Found new metadata file: {key} in bucket {bucket_name}")
                update_submission_queue(bucket_name, key)
            else:
                state = "seen"
            bucket_files[key] = state

            with lock:
                with open(state_file, "a") as f:
                    f.write(
                        json.dumps({"bucket": bucket_name, "key": key, "state": state})
                        + "\n"
                    )

        time.sleep(sleep)


def list_metadata_files(bucket_name: str, s3: boto3.resource) -> list[tuple[str, str]]:
    print(f"Checking bucket {bucket_name}")
    bucket = s3.Bucket(bucket_name)
    keys: list[tuple[str, str]] = []
    for obj in bucket.objects.all():
        if obj.key.endswith("metadata.json"):
            # TODO: key → local_case_id, for now use top-level directory name
            key, *_ = obj.key.split("/")
            keys.append((bucket_name, key))
    return keys


def setup_updater():
    return threading.Thread(target=check_buckets)


def fetch_submission_queue():
    if config["monitor"].get("active", False):
        global UPDATE_THREAD
        UPDATE_THREAD = setup_updater()
        UPDATE_THREAD.start()
        return from_queue(INPUT_QUEUE, finish_sentinel=SENTINEL)
    else:
        return []


def stop_updater(timeout: float | None = None):
    INPUT_QUEUE.put(SENTINEL)
    INPUT_QUEUE.join()
    UPDATE_THREAD.join(timeout=timeout)


def check_validation_flag(wildcards):
    """
    Reads the qc_flag file to determine if QC is necessary.
    """
    bucket_name = wildcards.bucket_name
    key = wildcards.key
    with open(f"results/{bucket_name}/validation_flag/{key}") as f:
        return f.read().strip() == "true"


def check_qc_flag(wildcards):
    """
    Reads the qc_flag file to determine if QC is necessary.
    """
    bucket_name = wildcards.bucket_name
    key = wildcards.key
    with open(f"results/{bucket_name}/qc_flag/{key}") as f:
        return f.read().strip() == "true"


def check_consent_flag(wildcards):
    """
    Reads the consent_flag file to determine if consent is given.
    """
    bucket_name = wildcards.bucket_name
    key = wildcards.key
    with open(f"results/{bucket_name}/consent_flag/{key}") as f:
        return f.read().strip() == "true"


def get_target_public_key(wildcards: Wildcards):
    if check_consent_flag(wildcards):
        return config["ghga"]["public_key"]
    else:
        return config["grz_internal"]["public_key"]


def get_s3_config(wildcards: Wildcards):
    if check_consent_flag(wildcards):
        return config["ghga"]["s3_config"]
    else:
        return config["grz_internal"]["s3_config"]
