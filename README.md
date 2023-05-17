# small-db

[![test](https://github.com/small-db/small-db/actions/workflows/test.yml/badge.svg)](https://github.com/small-db/small-db/actions/workflows/test.yml)
[![docs](https://docs.rs/small-db/badge.svg)](https://docs.rs/small-db)

A small database.

## Features

### Status

- [x] B+ tree storage
  - [x] insert
  - [x] delete
  - [x] search
- [x] Buffer pool
- [x] Write ahead log (ARIES)
- [ ]Code coverage (<https://blog.rng0.io/how-to-do-code-coverage-in-rust>)

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

### Run a specific test with flamegraph

```bash
CARGO_PROFILE_BENCH_DEBUG=true sudo cargo flamegraph --test <target> -- <test_path>
# e.g.
CARGO_PROFILE_BENCH_DEBUG=true sudo cargo flamegraph --test small_tests -- integretions::btree_test::test_big_table
```

## Code Style

> Here I agree with someone who wrote "If you need recursive locks, your code is too complex." After experiencing several deadlocks stemming from ridiculously complex code, I can say that all operations within a critical section should only be memory operations - assignment, memcpy etc - no syscalls, no locks and no calls of complex functions.
>
> [Is there a crate that implements a reentrant rwlock? : rust](https://www.reddit.com/r/rust/comments/a2jht3/comment/eb3dhak/?utm_source=share&utm_medium=web2x&context=3)
