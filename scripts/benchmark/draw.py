import os
import re
import subprocess
import matplotlib.pyplot as plt
import numpy as np


def draw():
    total_actions = 100 * 1000

    thread_count_list = [1]
    for i in range(1, 11):
        thread_count_list.append(i * 10)

    # total_actions = 1000
    # thread_count_list = [1, 2, 5]

    # latch_strategy: "page_latch"
    insert_per_second = get_insert_per_second(
        total_actions, thread_count_list, latch_strategy="page_latch"
    )
    plt.plot(thread_count_list, insert_per_second)
    plt.scatter(thread_count_list, insert_per_second)

    # latch_strategy: "tree_latch"
    insert_per_second = get_insert_per_second(
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


def get_insert_per_second(
    total_actions: int,
    thread_count_list: list[int],
    latch_strategy: str,
) -> list[int]:
    insert_per_second = []

    for thread_count in thread_count_list:
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

        insert_per_second.append(total_actions / duration_s)

    return insert_per_second


if __name__ == "__main__":
    draw()
