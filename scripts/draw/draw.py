import os
import re
import subprocess
import matplotlib.pyplot as plt
import numpy as np


def draw():
    # total_actions = 100 * 1000

    # concurrent_list = [1]
    # for i in range(1, 11):
    #     concurrent_list.append(i * 10)

    total_actions = 1000
    thread_count_list = [1, 2, 5]

    insert_per_second = []

    for thread_count in thread_count_list:
        threads_count = thread_count
        action_per_thread = total_actions // thread_count
        print(f"thread_count: {thread_count}, action_per_thread: {action_per_thread}")

        # set environment variable
        os.environ["THREAD_COUNT"] = str(threads_count)
        os.environ["ACTION_PER_THREAD"] = str(action_per_thread)

        # cargo test --features "benchmark, page_latch" -- --test-threads=1 --nocapture test_speed
        features = "benchmark, page_latch"
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

    plt.plot(thread_count_list, insert_per_second)
    plt.scatter(thread_count_list, insert_per_second)
    plt.xlabel("Concurrent Transactions")
    plt.ylabel("Insertions per Second")
    plt.show()
    return


if __name__ == "__main__":
    print("Hello, World!")
    draw()
