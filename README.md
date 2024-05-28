# small-db

[![test](https://github.com/small-db/small-db/actions/workflows/test.yml/badge.svg)](https://github.com/small-db/small-db/actions/workflows/test.yml)
[![docs](https://docs.rs/small-db/badge.svg)](https://docs.rs/small-db)

A small database.

**Thanks for your attention. For any issues/bugs/thoughts, please feel free to open an [issue](https://github.com/small-db/small-db/issues) or send an email to [Xiaochen Cui](mailto:jcnlcxc.new@gmail.com)**

## Features

### Two Types of Latch Strategies

- **page-latch** strategy (each page has its own latch)
- **tree-latch** strategy (each leaf page has its own latch, and all other pages are protected by a tree latch)

![insertions per second](./docs/img/insertions_per_second.png)

## Status

- [ ] Index (B+ tree)
  - [x] clustered index (InnoDB flavor, index organized tables) (<https://dev.mysql.com/doc/refman/8.0/en/innodb-index-types.html>)
  - [ ] all-secondary indexes (PostgreSQL flavor, heap organized tables) (<https://rcoh.me/posts/postgres-indexes-under-the-hood/>) (<https://www.postgresql.org/docs/current/btree-implementation.html>)
  - [ ] support table with no primary key
- [x] Buffer pool
- [ ] WAL (Write ahead log)
  - [x] ARIES
  - [ ] Innodb
  - [ ] PostgreSQL
- [ ] Gap Lock
- [x] PostgreSQL protocol
- [ ] TPCC benchmark
- [ ] MVCC (Multi-version concurrency control)
- [ ] Optimistic concurrency control
- [ ] Pessimistic concurrency control
- [ ] Snapshot isolation
- [ ] Distributed transaction
- [ ] Distributed lock
- [ ] Distributed index

### Non-functional targets

- [ ] WIP: run "test_big_table" in 5 seconds
- [ ] Code coverage (<https://blog.rng0.io/how-to-do-code-coverage-in-rust>)

## Development

### Run all tests

```bash
make test
```

Run all tests, this will be run in the CI. Log lovel is "info".

### Run a specific test

```bash
make <test_name>
# e.g:
make test_big_table
# Note: the test name must has the prefix "test_".
```

Run a specific test and store the output to file "out". Log level is "debug".

### Trace a specific test

- linux

  ```bash
  ./scripts/trace.sh <test_path>

  # e.g:
  ./scripts/trace.sh integretions::btree_test::test_big_table
  ```

- macOS

  ```bash
  CARGO_PROFILE_BENCH_DEBUG=true sudo cargo flamegraph --test <target> -- <test_path>

  # e.g:
  CARGO_PROFILE_BENCH_DEBUG=true sudo cargo flamegraph --test small_tests -- integretions::btree_test::test_concurrent
  ```

## Notes

### questions about mysql

- what is "bufferfixed"?
- what is "fsp latch"?

### The simplified version of the B+ tree latch strategy

- no tree latch
- when accessing a page (either leaf or internal), all ancestor pages of the page must be latched (why? if not latched, two directions of tree-traversal may happen at the same time, and lead to a deadlock)

### The imitate-mysql version of the B+ tree latch strategy

- there is a tree latch

### Draft

What's the exact meaning of "flash a page"?
It means that the modified page is written to the disk, thus ensuring the durability of the data. (Durability
means the data is not lost even if the system crashes.)

Why we have to flash related pages in the beginning of the transaction commit?
To ensure the durability of the data. Durability is the requirement of the transaction.

During flashing, the first step is to write an "update log" to the log file, why?
So that the system can recover the data in the page if the system crashes before the write operation is completed.

If the tree is protected by a tree latch, do we still have to flash internal pages?
Yes, because the internal pages may be modified by the transaction. And we need to record the changes in the log file. Actually, we need to record the changes of all pages no matter which latch strategy is used. If you just change a page without recording to the log file, the system will not be able to recover the data in the page if the system crashes before the write operation is completed.

If the tree is protected by a tree latch, do we still have to record the relationship between transaction and internal pages?
Yes, but we only have to record modified internal pages.

What's the best way to record dirty pages (pages that have been modified by a transaction)?
Since a transaction only use one thread in the current implementation, we pass dirty pages as a parameter. If we use
multiple threads for a transaction in the future, a better approach have to be used.
But different with repo "simple-db-hw-2022", we store dirty pages in the "transaction" context instead of using a standalone "dirty pages" parameter.

Why should I record its own starting offset at the end of each log record?
So the log manager can read the log file in reverse order.

Why the log manager need to read the log file in reverse order?
TODO

What's the best way to record the relationship between transaction and its dirty pages?
Due to the existence of the "flash_all" api, we must record the relationship globally, so we can get the
transaction of a dirty page when we flash all pages.

Why do we need "flash_all" api, and when should we use it?
TODO

When there is a tree latch, do we still need the lock manager ("ConcurrentStatus")?
Yes, since the "RWLock" is not enough to protect a leaf page from being modified by other transactions. The lock
from the lock manager and the "RWLock" attached to the leaf page have different life time. (The life time of the lock
from the lock manager is longer.)

Define "before image" precisely.
The "before image" is the content of a page before the transaction accesses it.
(Note it's not the content before the transaction starts, since other transactions may commit after
the transaction starts but before the transaction accesses the page, and these changes are "durable".)

Define "after image" precisely.
TODO

What's the transaction isolation strategy of the current implementation?
TODO

What's the transaction isolation level of the current implementation?
It's "READ UNCOMMITTED", which means a transaction can read the uncommitted data of other transactions.

When comes to the relationship between the transaction and the pages, why there are too fields ("hold_pages"
and "dirty_pages")?
"hold_pages" records all leaf pages that locked by the transaction, and "dirty_pages" records all
pages that have been modified by the transaction. (In the current implementation, "hold_pages" is a subset of "dirty_pages".)

(In InnoDB latch approach) What happens if a inner page is modified by another transaction, after the X-Latch
is released by the current transaction?
TODO
