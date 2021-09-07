#!/bin/bash

RUST_LOG=info cargo test --package simple-db-rust --test btree_insert_test --all-features -- "$1" --exact --nocapture  2>&1 | tee out
