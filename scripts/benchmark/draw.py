import datetime
import json
import os
import re
import subprocess

import matplotlib.pyplot as plt
import numpy as np
from benchmark import BenchmarkRecord, json_loader


def get_report_path():
    report_dir = os.path.join("docs", "record")
    report_files = os.listdir(report_dir)

    # sort by the time in the file name in descending order
    #
    # example of file name: docs/record/benchmark_20240527_220536.json
    def sort_key(x):
        result = re.findall(r"\d+_\d+", x)[0]
        tm = datetime.datetime.strptime(result, "%Y%m%d_%H%M%S")
        return tm

    report_files.sort(key=lambda x: sort_key(x), reverse=True)
    return os.path.join(report_dir, report_files[0])


def draw():
    report_path = get_report_path()

    # parse the json to list(BenchmarkRecord)
    f = open(report_path, "r")
    all_records = json.load(f, object_hook=lambda x: json_loader(**x))

    points_list = []

    for latch_strategy in ["tree_latch", "page_latch"]:
        records = list(
            filter(
                lambda x: x.target_attributes["latch_strategy"] == latch_strategy,
                all_records,
            )
        )

        # sort by thread_count
        records.sort(key=lambda x: x.target_attributes["thread_count"])

        thread_count_list = [r.target_attributes["thread_count"] for r in records]
        insert_per_second = [r.test_result["insert_per_second"] for r in records]

        plt.plot(thread_count_list, insert_per_second)
        points = plt.scatter(thread_count_list, insert_per_second, label=latch_strategy)
        points_list.append(points)

    plt.xlabel("Concurrent Transactions")
    plt.ylabel("Insertions per Second")

    top = max([r.test_result["insert_per_second"] for r in all_records]) * 1.3
    plt.ylim(bottom=0, top=top)

    plt.legend(handles=points_list, loc="upper right")

    plt.savefig("./docs/img/insertions_per_second.png")
    return


if __name__ == "__main__":
    draw()
