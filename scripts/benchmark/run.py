#!/usr/bin/env python3

# env:
# sudo apt-get install libpq-dev
# pip install psycopg2

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
        f"docker run --rm --name postgres -e POSTGRES_DB={DB_NAME} -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1:5432:5432 -v {DATA_DIR}:/var/lib/postgresql/data postgres"
    )


def teardown():
    xiaochen_py.run_command("docker rm -f postgres")


def case_concurrent_insert():
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
        # Example tuple with random string and integer
        random_string = "".join(random.choices(string.ascii_lowercase, k=10))
        random_int = random.randint(1, 100)
        return (random_string, random_int)

    conn = psycopg2.connect(**db_config)

    # create table
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {TABLE_NAME} (column1 VARCHAR(10), column2 INT)")

    # Function to insert 10 random tuples into the table
    def insert_random_tuples(thread_id):
        try:
            conn = psycopg2.connect(**db_config)
            cur = conn.cursor()

            # Insert 10 random tuples
            for _ in range(10):
                data = generate_random_tuple()
                cur.execute(
                    f"INSERT INTO {TABLE_NAME} (column1, column2) VALUES (%s, %s)",
                    data,
                )

            conn.commit()  # Commit after all inserts
            print(f"Thread {thread_id} committed 10 inserts.")

            cur.close()
            conn.close()
        except Exception as e:
            print(f"Error in thread {thread_id}: {e}")

    # List to hold all threads
    threads = []

    # Start 100 threads
    for i in range(100):
        thread = threading.Thread(target=insert_random_tuples, args=(i,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    print("All threads have finished.")


if __name__ == "__main__":
    setup()
    case_concurrent_insert()
    teardown()
