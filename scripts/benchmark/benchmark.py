#!/usr/bin/env python3

# env:
# sudo apt-get install libpq-dev
# pip install psycopg2

import datetime
import os
import re
import psycopg2
import threading
import random
import string
from time import sleep

import xiaochen_py

DB_NAME = "db"
DB_USER = "postgres"


def setup():
    DATA_DIR = "/media/xiaochen/large/cs_data/postgres"

    xiaochen_py.run_command("docker rm -f postgres")
    xiaochen_py.run_command(f"sudo rm -rf {DATA_DIR}")
    xiaochen_py.run_command(
        f"docker run --detach --name postgres -e POSTGRES_DB={DB_NAME} -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1:5432:5432 -v {DATA_DIR}:/var/lib/postgresql/data postgres:17 -c max_connections=1000"
    )
    wait_for_postgres()


def teardown():
    xiaochen_py.run_command("docker rm -f postgres")


def wait_for_postgres():
    conn = None
    while conn is None:
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password="", host="localhost", port=5432
            )
        except psycopg2.OperationalError:
            sleep(1)
    conn.close()


def concurrent_insert(
    total_actions: int,
    threads_count: int,
):
    action_per_thread = total_actions // threads_count
    total_actions = action_per_thread * threads_count

    db_config = {
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": "",
        "host": "localhost",
        "port": 5432,
    }

    TABLE_NAME = "foo"

    # Function to generate random tuples
    def generate_random_tuple():
        # Signed int64 range
        min_int64 = -9223372036854775808
        max_int64 = 9223372036854775807

        # Generate random int64
        v1 = random.randint(min_int64, max_int64)
        return (v1, v1)

    conn = psycopg2.connect(**db_config)

    with conn:
        with conn.cursor() as curs:
            curs.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")

    with conn:
        with conn.cursor() as curs:
            curs.execute(
                f"CREATE TABLE {TABLE_NAME} (column1 bigint primary key, column2 bigint)"
            )

    conn.close()

    def insert_random_tuples(thread_id):
        try:
            conn = psycopg2.connect(**db_config)
            with conn.cursor() as curs:
                for _ in range(action_per_thread):
                    data = generate_random_tuple()
                    curs.execute(
                        f"INSERT INTO {TABLE_NAME} (column1, column2) VALUES (%s, %s)",
                        data,
                    )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error in thread {thread_id}: {e}")

    threads = []

    start_time = datetime.datetime.now()
    for i in range(threads_count):
        thread = threading.Thread(target=insert_random_tuples, args=(i,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    duration = datetime.datetime.now() - start_time
    insert_per_second = total_actions / duration.total_seconds()
    print(f"duration: {duration}")

    # check the number of rows
    conn = psycopg2.connect(**db_config)
    with conn:
        with conn.cursor() as curs:
            curs.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            row = curs.fetchone()
            assert row[0] == total_actions

    r = xiaochen_py.BenchmarkRecord()
    r.target_attributes = {
        "os": "linux",
        "disk": "hdd",
        "target": "postgres",
        "total_actions": total_actions,
        "threads_count": threads_count,
        "action_per_thread": action_per_thread,
    }
    r.test_result = {
        "duration_ms": duration.total_seconds() * 1000,
        "insert_per_second": insert_per_second,
    }
    return r


def concurrent_insert_smalldb_raw(
    total_actions: int,
    threads_count: int,
) -> xiaochen_py.BenchmarkRecord:
    action_per_thread = total_actions // threads_count
    total_actions = action_per_thread * threads_count

    # set environment variable
    variables = {
        "THREADS_COUNT": threads_count,
        "ACTION_PER_THREAD": action_per_thread,
        "RUST_LOG": "info",
    }
    for k, v in variables.items():
        os.environ[k] = str(v)

    # don't add quotes, python will add quotes automatically
    features = f'"benchmark"'

    commands = [
        "cargo",
        "test",
        "--features",
        features,
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

    r = xiaochen_py.BenchmarkRecord()
    r.target_attributes = {
        "os": "linux",
        "disk": "hdd",
        "target": "smalldb-raw",
        "total_actions": total_actions,
        "threads_count": threads_count,
        "action_per_thread": action_per_thread,
    }
    r.test_result = {
        "duration_ms": duration_ms,
        "insert_per_second": insert_per_second,
    }

    return r


if __name__ == "__main__":
    setup()

    total_actions = 1000 * 1000
    thread_count_list = [1]
    for i in range(1, 12):
        thread_count_list.append(i * 10)

    records = []

    for threads_count in thread_count_list:
        r = concurrent_insert(total_actions=total_actions, threads_count=threads_count)
        records.append(r)

        r = concurrent_insert_smalldb_raw(
            total_actions=total_actions, threads_count=threads_count
        )
        records.append(r)
    xiaochen_py.dump_records(records, "docs/record")

    # teardown()
