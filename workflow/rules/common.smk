import datetime
import json
import queue
import signal
import subprocess
import threading
import time

from snakemake.io import Wildcards

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


def get_ready_marker(wildcards: Wildcards) -> str:
    qc_flag_file = checkpoints.determine_if_qc_is_necessary.get(
        bucket_name=wildcards.bucket_name, submission_id=wildcards.submission_id
    ).output.needs_qc

    with open(qc_flag_file) as f:
        qc_needed = f.read().strip() == "true"

    if qc_needed:
        return f"results/{wildcards.bucket_name}/qc/{wildcards.submission_id}"
    else:
        return f"results/{wildcards.bucket_name}/pruefbericht_answer/{wildcards.submission_id}"


def signal_handler(sig, frame):
    stop_updater(30)
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
