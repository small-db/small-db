import datetime
import json
import os
import re
import subprocess

import matplotlib.pyplot as plt
import numpy as np
import xiaochen_py


class BenchmarkRecord:
    smalldb_commitid: str
    start_time: str
    target_attributes: dict[str, object]
    test_result: dict[str, object]

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __repr__(self):
        return f"{self.smalldb_commitid}, {self.start_time}, {self.target_attributes}, {self.test_result}"


def json_loader(**kwargs):
    if "smalldb_commitid" in kwargs:
        return BenchmarkRecord(**kwargs)

    return kwargs


def benchmark():
    total_actions = 100 * 1000

    thread_count_list = [1]
    for i in range(1, 11):
        thread_count_list.append(i * 10)

    records = []

    for thread_count in thread_count_list:
        r = run_test_speed(total_actions, thread_count)
        records.append(r)

    # dump records to a file in json format
    records_json = json.dumps(records, default=lambda x: x.__dict__, indent=4)
    record_path = os.path.join(
        "docs",
        "record",
        f"benchmark_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
    )

    with open(record_path, "w") as f:
        f.write(records_json)


def run_test_speed(
    total_actions: int,
    thread_count: int,
) -> BenchmarkRecord:
    threads_count = thread_count
    action_per_thread = total_actions // thread_count
    print(f"thread_count: {thread_count}, action_per_thread: {action_per_thread}")

    # set environment variable
    variables = {
        "THREAD_COUNT": threads_count,
        "ACTION_PER_THREAD": action_per_thread,
        "RUST_LOG": "info",
    }
    for k, v in variables.items():
        os.environ[k] = str(v)

    commands = [
        "cargo",
        "test",
        "--features",
        '"benchmark"',  # enable benchmark
        "--",
        "--test-threads=1",
        "--nocapture",
        "test_insert_parallel",
    ]

    # "debug_command" is the command that contains environment variables and thus can be
    # used for debugging.
    debug_command = ""
    for k, v in variables.items():
        debug_command += f"{k}={v} "
    debug_command += " ".join(commands)

    output, _ = xiaochen_py.run_command(
        debug_command, raise_on_failure=True, log_path="out"
    )

    x = re.search(r"ms:(\d+)", output.decode("utf-8"))
    duration_ms = int(x.group(1))
    duration_s = duration_ms / 1000

    insert_per_second = total_actions / duration_s

    r = BenchmarkRecord()
    r.smalldb_commitid = get_git_commitid()
    r.start_time = datetime.datetime.now().isoformat()
    r.target_attributes = {
        # hardware
        "os": "macos",
        "cpu": "M3",
        "disk_type": "SSD",
        # software configuration
        "total_actions": total_actions,
        "thread_count": thread_count,
        "action_per_thread": action_per_thread,
    }
    r.test_result = {
        "duration_ms": duration_ms,
        "insert_per_second": insert_per_second,
    }

    return r


def get_git_commitid():
    output = subprocess.check_output(["git", "rev-parse", "HEAD"])
    return output.decode("utf-8").strip()


if __name__ == "__main__":
    benchmark()
