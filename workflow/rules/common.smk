import datetime
import json
import queue
import signal
import subprocess
import threading
import time
from tempfile import NamedTemporaryFile

import yaml

from snakemake.io import Wildcards

SENTINEL = object()
INPUT_QUEUE: queue.Queue = queue.Queue()


def update_submission_queue(bucket_name: str, key: str):
    print(f"Updating submission queue for {bucket_name}/{key}")
    INPUT_QUEUE.put(f"results/{bucket_name}/target/{key}")


def check_buckets():
    buckets = config["inbox"]["s3"]["buckets"]
    print(f"Monitoring buckets {buckets} …")
    sleep = int(config["monitor"].get("interval", "3600"))

    while True:
        for bucket_name in buckets:
            with NamedTemporaryFile("w+t") as config_file:
                inbox_config = config["inbox"]
                del inbox_config["buckets"]
                inbox_config["bucket"] = bucket_name
                yaml.dump(inbox_config, config_file.name)
                available_submissions = json.loads(
                    subprocess.run(
                        ["grzctl", "list", "--config-file", config_file, "--json"],
                        capture_output=True,
                        text=True,
                        check=True,
                    ).stdout
                )
                available_submissions = list(
                    sorted(
                        filter(lambda r: r.complete is True, available_submissions),
                        key=lambda r: datetime.datetime.strptime(
                            r.oldest_upload, "%Y-%m-%d %H:%M:%S"
                        ),
                    )
                )
                print(available_submissions)
                for available_submission in available_submissions:
                    INPUT_QUEUE.put(available_submission.submission_id)

            print(INPUT_QUEUE.qsize())

        time.sleep(sleep)


UPDATE_THREAD: threading.Thread = threading.Thread(target=check_buckets, daemon=False)


# FIXME: use this on SIGINT / keyboard interrupt, e.g. via signal module
def stop_updater(timeout: float | None = None):
    print("Stopping updater…")
    INPUT_QUEUE.put(SENTINEL)
    INPUT_QUEUE.join()
    UPDATE_THREAD.join(timeout=timeout)
    print("Stopped updater.")


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
        return config["consented"]["public_key"]
    else:
        return config["nonconsented"]["public_key"]


def get_s3(wildcards: Wildcards) -> dict:
    if check_consent_flag(wildcards):
        return config["consented"]["s3"]
    else:
        return config["nonconsented"]["s3"]


def signal_handler(sig, frame):
    stop_updater(30)
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
