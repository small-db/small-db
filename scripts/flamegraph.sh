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
RUST_LOG=info \
    sudo cargo flamegraph \
        --test small_tests \
        -- $TEST_NAME

# RUST_LOG=info \
#     sudo flamegraph \
#     $BINARY -- \
#     $TEST_NAME --exact --nocapture

# for Linux
# apt-get install linux-tools-common linux-tools-generic linux-tools-`uname -r`
# 
# RUST_LOG=info \
#     sudo perf record -F 99 -g -- \
#     $BINARY -- \
#     $TEST_NAME --exact --nocapture
