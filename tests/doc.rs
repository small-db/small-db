// run test:
// cargo test --package simple-db-rust --test btree_insert_test --all-features
// -- insert_duplicate_tuples --exact --nocapture
//
// binary name example:
// target/debug/deps/btree_insert_test-633392dbbebdad3c
//
// run binary:
// target/debug/deps/btree_insert_test-633392dbbebdad3c --
// insert_duplicate_tuples --exact --nocapture
//
// flamegraph:
// RUST_LOG=info sudo flamegraph
// target/debug/deps/btree_insert_test-633392dbbebdad3c --
// insert_duplicate_tuples --exact --nocapture
