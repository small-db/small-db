#!/usr/bin/env python3

import datetime
import json
import os
import re

import matplotlib.pyplot as plt
import xiaochen_py


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
    def get_target(records: list[xiaochen_py.BenchmarkRecord]) -> str:
        return records[0].target_attributes["target"]

    report_path = get_report_path()

    # parse the json to list(BenchmarkRecord)
    f = open(report_path, "r")
    all_records = json.load(f, object_hook=lambda x: xiaochen_py.json_loader(**x))

    targets = set([r.target_attributes["target"] for r in all_records])
    points_list = []
    for target in targets:
        # filter and sort
        records = [r for r in all_records if r.target_attributes["target"] == target]
        records.sort(key=lambda x: x.target_attributes["threads_count"])

        threads_count_list = [r.target_attributes["threads_count"] for r in records]
        insert_per_second = [r.test_result["insert_per_second"] for r in records]

        # draw points
        points = plt.scatter(threads_count_list, insert_per_second, label=f"{target}")

        # draw lines
        plt.plot(threads_count_list, insert_per_second)
        points_list.append(points)

    plt.xlabel("Concurrent Transactions")
    plt.ylabel("Insertions per Second")

    top = max([r.test_result["insert_per_second"] for r in records]) * 1.3
    plt.ylim(bottom=0, top=top)

    plt.legend(handles=points_list, loc="upper right")

    tm = xiaochen_py.timestamp()
    plt.savefig(f"./docs/img/concurrent_insert_{tm}.png")
    return


if __name__ == "__main__":
    draw()
