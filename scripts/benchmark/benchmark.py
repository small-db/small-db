#!/usr/bin/env python3

# env:
# sudo apt-get install libpq-dev
# pip install psycopg2

import datetime
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
    xiaochen_py.run_command(
        f"docker run --detach --rm --name postgres -e POSTGRES_DB={DB_NAME} -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1:5432:5432 -v {DATA_DIR}:/var/lib/postgresql/data postgres:17",
    )


def teardown():
    xiaochen_py.run_command("docker rm -f postgres")


def case_concurrent_insert(
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

    cur = conn.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    conn.commit()

    cur.execute(
        f"CREATE TABLE {TABLE_NAME} (column1 bigint primary key, column2 bigint)"
    )
    conn.commit()
    cur.close()

    def insert_random_tuples(thread_id):
        try:
            # conn = psycopg2.connect(**db_config)
            cur = conn.cursor()

            for _ in range(action_per_thread):
                data = generate_random_tuple()
                cur.execute(
                    f"INSERT INTO {TABLE_NAME} (column1, column2) VALUES (%s, %s)",
                    data,
                )

            conn.commit()
            cur.close()
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
    conn.close()

    duration = datetime.datetime.now() - start_time
    insert_per_second = total_actions / duration.total_seconds()
    print(f"duration: {duration}")

    r = xiaochen_py.BenchmarkRecord()
    r.target_attributes = {
        "os": "linux",
        "disk": "hdd",
        "server": "postgres",
        "total_actions": total_actions,
        "threads_count": threads_count,
        "action_per_thread": action_per_thread,
    }
    r.test_result = {
        "duration_ms": duration.total_seconds() * 1000,
        "insert_per_second": insert_per_second,
    }
    return r


if __name__ == "__main__":
    # setup()

    total_actions = 1000 * 1000

    thread_count_list = [1]
    for i in range(1, 12):
        thread_count_list.append(i * 10)

    records = []

    for threads_count in thread_count_list:
        r = case_concurrent_insert(
            total_actions=total_actions, threads_count=threads_count
        )
        records.append(r)
    xiaochen_py.dump_records(records, "docs/record")

    # teardown()
