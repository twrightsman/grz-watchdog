import datetime
import json
import queue
import signal
import subprocess
import threading
import time
from random import randint
from typing import Any

SENTINEL = object()
INPUT_QUEUE: queue.Queue = queue.Queue()


wildcard_constraints:
    bucket_name=r"|".join(config["buckets"]["inbox"].keys()),


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

            def select_for_qc(
                submission: dict[str, Any], submissions: list[dict[str, Any]]
            ) -> bool:
                # TODO: filter submissions to current/last X weeks, pick N from that
                qc_chance: int = 2
                if randint(1, 100) <= qc_chance:
                    return True
                return False

            for available_submission in available_submissions:
                submission_id = available_submission.submission_id
                if select_for_qc(available_submission, available_submissions):
                    target = f"results/{bucket_name}/target/with_qc/{submission_id}"
                    print(
                        f"Queueing QC path for {submission_id}: {target}",
                        file=sys.stderr,
                    )
                else:
                    target = f"results/{bucket_name}/target/without_qc/{submission_id}"
                    print(
                        f"Queueing non-QC path for {submission_id}: {target}",
                        file=sys.stderr,
                    )
                INPUT_QUEUE.put(target)

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


def signal_handler(sig, frame):
    stop_updater(30)
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
