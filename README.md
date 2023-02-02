# small-db

[![test](https://github.com/small-db/small-db/actions/workflows/test.yml/badge.svg)](https://github.com/small-db/small-db/actions/workflows/test.yml)

A small database writing in rust, inspired from mit 6.830

## Features

### Implemented

- B+ tree storage structure
- B+ tree insert
- B+ tree delete
- Buffer pool
- Iterator
- Search by condition

### TODO

- WAL
- Code coverage (https://blog.rng0.io/how-to-do-code-coverage-in-rust)

## License


```
TODO:
[![docs](https://docs.rs/small-db/badge.svg)](https://docs.rs/small-db)
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
