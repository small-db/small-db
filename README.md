# small-db

[![docs](https://docs.rs/small-db/badge.svg)](https://docs.rs/small-db)

[![Rust](https://github.com/XiaochenCui/small-db/actions/workflows/rust.yml/badge.svg)](https://github.com/XiaochenCui/small-db/actions/workflows/rust.yml)
[![codecov](https://codecov.io/gh/XiaochenCui/small-db/branch/master/graph/badge.svg)](https://codecov.io/gh/XiaochenCui/small-db)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4128/badge)](https://bestpractices.coreinfrastructure.org/projects/4128)

A small database writing in rust, inspired from mit 6.830

## Roadmap

### 0.0.0

- B+ tree storage structure
- Buffer pool
- Insert

### 0.1.0

- Publish to crates.io

### 0.2.0

- Iterator
- Search by condition

### 0.3.0

- Delete, query, update

### 1.0.0

- SQL (used for TPC test)
- Page topology

## License

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FXiaochenCui%2Fsmall-db.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2FXiaochenCui%2Fsmall-db?ref=badge_large)

```
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
