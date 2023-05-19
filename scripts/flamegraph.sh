#!/bin/bash
# 
# Used to generate flamegraphs for a specific test.
# 
# Usage:
#   ./scripts/flamegraph.sh <test_name>

set -o xtrace
set -o errexit
set -o nounset

BINARY=$(python3 ./scripts/get_test_binary.py)
TEST_NAME=$1

echo $BINARY

# for maxOS
# 
# install:
# cargo install flamegraph
# 
# RUST_LOG=info \
#     sudo cargo flamegraph \
#         --test small_tests \
#         -- $TEST_NAME

# for Linux
# 
# e.g: ./scripts/flamegraph.sh integretions::btree_test::test_big_table
#
# RUST_LOG=info \
#     perf record -F 1000 --call-graph dwarf -- \
#     $BINARY -- \
#     $TEST_NAME --exact --nocapture

RUST_LOG=info \
    sudo perf stat -e 'syscalls:sys_enter_*' -- \
    $BINARY -- \
    $TEST_NAME --exact --nocapture \
    2>&1 | grep syscalls | sort \
    && sudo rm -rf data