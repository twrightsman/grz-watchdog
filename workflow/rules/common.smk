import datetime
import json
import queue
import signal
import subprocess
import threading
import time

from snakemake.io import Wildcards, InputFiles

SENTINEL = object()
INPUT_QUEUE: queue.Queue = queue.Queue()


def update_submission_queue(bucket_name: str, key: str):
    print(f"Updating submission queue for {bucket_name}/{key}")
    INPUT_QUEUE.put(f"results/{bucket_name}/target/{key}")


def check_buckets():
    buckets = config["buckets"]["inbox"].keys()
    print(f"Monitoring buckets {buckets} …")
    sleep = int(config["monitor"].get("interval", "3600"))

    while True:
        for bucket_name in buckets:
            inbox_config_file = config["buckets"]["inbox"][bucket_name]
            available_submissions = json.loads(
                subprocess.run(
                    ["grzctl", "list", "--config-file", inbox_config_file, "--json"],
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


def check_validation_flag_file(file) -> bool:
    """
    Reads the qc_flag file to determine if QC is necessary.
    """
    with open(file) as f:
        return f.read().strip() == "true"


def check_validation_flag(wildcards: Wildcards) -> bool:
    """
    Reads the qc_flag file to determine if QC is necessary.
    """
    bucket_name = wildcards.bucket_name
    key = wildcards.key
    return check_validation_flag_file(f"results/{bucket_name}/validation_flag/{key}")


def get_pruefbericht_params(_wildcards: Wildcards, input_files: InputFiles):
    if check_validation_flag_file(input_files.validation_flag):
        return ""
    else:
        return "--fail"


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
    return check_consent_flag_file(f"results/{bucket_name}/consent_flag/{key}")


def check_consent_flag_file(file) -> bool:
    """
    Reads the consent_flag file to determine if consent is given.
    """
    with open(file) as f:
        return f.read().strip() == "true"


def get_target_config_file(_wildcards: Wildcards, input_files: InputFiles) -> str:
    if check_consent_flag_file(input_files.consent_flag):
        return config["buckets"]["consented"]
    else:
        return config["buckets"]["nonconsented"]


def signal_handler(sig, frame):
    stop_updater(30)
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
