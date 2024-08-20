import datetime
import json
import os
import re
import subprocess

import matplotlib.pyplot as plt
import numpy as np


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

    # total_actions = 1000
    # thread_count_list = [1, 2, 5]

    records = []

    # latch_strategy: "page_latch"
    for thread_count in thread_count_list:
        r = run_test_speed(total_actions, thread_count, latch_strategy="page_latch")
        records.append(r)

    # latch_strategy: "tree_latch"
    for thread_count in thread_count_list:
        r = run_test_speed(total_actions, thread_count, latch_strategy="tree_latch")
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
    latch_strategy: str,
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

    # cargo test --features "benchmark, {latch_strategy}" -- --test-threads=1 --nocapture test_speed
    features = f"benchmark, {latch_strategy}"
    # output = subprocess.check_output(
    #     [
    #         "cargo",
    #         "test",
    #         "--features",
    #         features,
    #         "--",
    #         "--test-threads=1",
    #         "--nocapture",
    #         "test_speed",
    #     ]
    # )

    commands = [
        "cargo",
        "test",
        "--features",
        features,
        "--",
        "--test-threads=1",
        "--nocapture",
        "test_speed",
    ]
    command = ""
    for k, v in variables.items():
        command += f"{k}={v} "
    command += " ".join(commands)
    print(f"start subprocess, command:\n{command}")
    # exit(0)

    process = subprocess.Popen(
        [
            "cargo",
            "test",
            "--features",
            features,
            "--",
            "--test-threads=1",
            "--nocapture",
            "test_speed",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )

    # Collect the output of the test.
    # 
    # For cargo, the stdout is the output of cargo itself, and the stderr is the output of the test.
    output = ""
    for line in iter(process.stderr.readline, ""):
        # print(">>> " + line, end="")
        output += line

    # print(f"finished subprocess, returncode: {process.returncode}")

    # Capture the rest of the output after the process completes
    remained_stdout, remained_stderr = process.communicate()
    # print(f"remained_stdout: {remained_stdout}, remained_stderr: {remained_stderr}")

    if process.returncode != 0:
        print(f"Error: {remained_stderr}")
        exit(1)

    # txt = output.decode("utf-8")
    x = re.search(r"ms:(\d+)", output)
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
        "latch_strategy": latch_strategy,
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
