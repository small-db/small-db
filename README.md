# small-db

[![test](https://github.com/small-db/small-db/actions/workflows/test.yml/badge.svg)](https://github.com/small-db/small-db/actions/workflows/test.yml)
[![docs](https://docs.rs/small-db/badge.svg)](https://docs.rs/small-db)

A small database.

## Features

### Implemented

- B+ tree storage (insert, delete, search)
- Buffer pool
- Write ahead log (ARIES)

### Todo

- Code coverage (https://blog.rng0.io/how-to-do-code-coverage-in-rust)

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
CARGO_PROFILE_BENCH_DEBUG=true sudo cargo flamegraph --test mod -- integretions::btree_test::test_big_table
```

## License


```
TODO:
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FXiaochenCui%2Fsmall-db.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2FXiaochenCui%2Fsmall-db?ref=badge_large)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4128/badge)](https://bestpractices.coreinfrastructure.org/projects/4128)

run test:
FILE=btree_delete_test
TEST=test_delete_internal_pages
export FILE=btree_delete_test TEST=test_delete_internal_pages RUST_LOG=info RUST_BACKTRACE=1 && cargo test --package small-db --test $FILE --all-features -- $TEST --exact --nocapture 2>&1 | tee out

build the test binary:
cargo test --no-run

run the test binary:
target/debug/deps/btree_insert_test-633392dbbebdad3c --
insert_duplicate_tuples --exact --nocapture

binary name example:
target/debug/deps/btree_insert_test-633392dbbebdad3c

flamegraph:
export TEST_BINARY=./target/debug/deps/btree_delete_test-2d878ed737dff71a
export TEST_FUNC=test_redistribute_internal_pages
RUST_LOG=info sudo flamegraph ${TEST_BINARY} -- ${TEST_FUNC} --exact --nocapture

show flamegraph:
open flamegraph.svg
*/
```

> Here I agree with someone who wrote "If you need recursive locks, your code is too complex." After experiencing several deadlocks stemming from ridiculously complex code, I can say that all operations within a critical section should only be memory operations - assignment, memcpy etc - no syscalls, no locks and no calls of complex functions.
>
> [Is there a crate that implements a reentrant rwlock? : rust](https://www.reddit.com/r/rust/comments/a2jht3/comment/eb3dhak/?utm_source=share&utm_medium=web2x&context=3)
