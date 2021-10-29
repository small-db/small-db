/*
run test:
FILE=btree_delete_test
TEST=test_delete_internal_pages
RUST_LOG=info RUST_BACKTRACE=1 cargo test --package simple-db-rust --test $FILE --all-features -- $TEST --exact --nocapture 2>&1 | tee out

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
