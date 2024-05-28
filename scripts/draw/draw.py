import os
import subprocess
import matplotlib.pyplot as plt
import numpy as np


def draw():
    # set env var
    os.environ["THREADS_COUNT"] = "1"

    # cargo test --features "benchmark" -- --test-threads=1 --nocapture test_speed
    features = 'benchmark, tree_latch'
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
    print(output.decode("utf-8"))

    # 11 random data
    insert_page_latch = [43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53]
    insert_tree_latch = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]

    concurrent_list = [1]
    for i in range(1, 11):
        concurrent_list.append(i * 10)

    plt.plot(concurrent_list, insert_page_latch, concurrent_list, insert_tree_latch)
    plt.scatter(concurrent_list, insert_page_latch)
    plt.scatter(concurrent_list, insert_tree_latch)
    plt.xlabel("Concurrent Transactions")
    plt.ylabel("Insertions per Second")
    plt.show()
    return


if __name__ == "__main__":
    print("Hello, World!")
    draw()
