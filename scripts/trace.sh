#!/bin/bash
# 
# Used to trace a specific test.
#   - Gather syscall
#   - Generate flamegraph
# 
# Usage:
#   ./scripts/trace.sh <test_name>
# 
# e.g:
#   ./scripts/trace.sh integretions::btree_test::test_big_table

set -o xtrace
set -o errexit
set -o nounset

# request sudo access
sudo echo "sudo access granted"

# ===============================================================
# get binary path
# ===============================================================

BINARY=$(python3 ./scripts/get_test_binary.py)
TEST_NAME=$1
echo "BINARY: $BINARY"

# ===============================================================
# run target, gather syscall
# ===============================================================

RUST_LOG=info \
    sudo perf stat -e 'syscalls:sys_enter_*' -- \
    $BINARY -- \
    $TEST_NAME --exact --nocapture \
    2>&1 | grep syscalls | sort \
    && sudo rm -rf data

# ===============================================================
# run target, gather flamegraph
# ===============================================================

RUST_LOG=info \
    sudo perf record -F 1000 --call-graph dwarf -- \
    $BINARY -- \
    $TEST_NAME --exact --nocapture \
    && sudo rm -rf data

perf script | ../FlameGraph/stackcollapse-perf.pl > out.perf-folded

../FlameGraph/flamegraph.pl out.perf-folded > perf.svg