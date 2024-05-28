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


def benchmark():
    # total_actions = 100 * 1000

    # thread_count_list = [1]
    # for i in range(1, 11):
    #     thread_count_list.append(i * 10)

    total_actions = 1000
    thread_count_list = [1, 2, 5]

    records = []

    # latch_strategy: "page_latch"
    for thread_count in thread_count_list:
        r = run_test_speed(total_actions, thread_count, latch_strategy="page_latch")
        records.append(r)

    # dump records to a file in json format
    records_json = json.dumps(records, default=lambda x: x.__dict__, indent=4)
    record_path = os.path.join(
        "docs", f"benchmark_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )

    with open(record_path, "w") as f:
        f.write(records_json)

    return
    plt.plot(thread_count_list, insert_per_second)
    plt.scatter(thread_count_list, insert_per_second)

    # latch_strategy: "tree_latch"
    insert_per_second = run_test_speed(
        total_actions, thread_count_list, latch_strategy="tree_latch"
    )
    plt.plot(thread_count_list, insert_per_second)
    plt.scatter(thread_count_list, insert_per_second)

    plt.xlabel("Concurrent Transactions")
    plt.ylabel("Insertions per Second")

    top = max(insert_per_second) + 1000
    plt.ylim(bottom=0, top=top)

    plt.savefig("./docs/insertions_per_second.png")
    return


def run_test_speed(
    total_actions: int,
    thread_count: int,
    latch_strategy: str,
) -> BenchmarkRecord:
    threads_count = thread_count
    action_per_thread = total_actions // thread_count
    print(f"thread_count: {thread_count}, action_per_thread: {action_per_thread}")

    # set environment variable
    os.environ["THREAD_COUNT"] = str(threads_count)
    os.environ["ACTION_PER_THREAD"] = str(action_per_thread)

    # cargo test --features "benchmark, {latch_strategy}" -- --test-threads=1 --nocapture test_speed
    features = f"benchmark, {latch_strategy}"
    output = subprocess.check_output(
        [
            "cargo",
            "test",
            "--features",
            features,
            "--",
            "--test-threads=1",
            "--nocapture",
            "test_speed",
        ]
    )

    txt = output.decode("utf-8")
    x = re.search(r"ms:(\d+)", txt)
    duration_ms = int(x.group(1))
    duration_s = duration_ms / 1000

    insert_per_second = total_actions / duration_s

    r = BenchmarkRecord()
    r.smalldb_commitid = get_git_commitid()
    r.start_time = datetime.datetime.now().isoformat()
    r.target_attributes = {
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
