#!/bin/bash
# Used to generate flamegraphs for a specific test.
# Usage:
#   ./scripts/flamegraph.sh <test_name>

set -o xtrace
set -o errexit
set -o nounset

BINARY=$(python3 ./scripts/get_test_binary.py)
TEST_NAME=$1

echo $BINARY

RUST_LOG=info \
    sudo flamegraph \
    $BINARY -- \
    $TEST_NAME --exact --nocapture
