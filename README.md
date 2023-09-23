# small-db

[![test](https://github.com/small-db/small-db/actions/workflows/test.yml/badge.svg)](https://github.com/small-db/small-db/actions/workflows/test.yml)
[![docs](https://docs.rs/small-db/badge.svg)](https://docs.rs/small-db)

A small database.

## Features

### Status

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

### Run a specific test

```bash
make <test_name>
# e.g:
make test_big_table
```

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
