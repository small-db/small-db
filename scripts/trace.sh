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
#   ./scripts/trace.sh test_big_table

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

# # clear logs
# sudo rm -f out

# RUST_LOG=info \
#     sudo perf stat -e 'syscalls:sys_enter_*' -- \
#     $BINARY -- \
#     $TEST_NAME --exact --nocapture \
#     2>&1 | grep syscalls | sort \
#     >> out \
#     && sudo rm -rf data

# ===============================================================
# run target, gather flamegraph
# ===============================================================

RUST_LOG=debug \
    # perf record -F 1000 --call-graph dwarf -- \
    # sudo perf record -F 200 -g -- \
    sudo perf record -F 99 -g -- \
    $BINARY -- \
    $TEST_NAME --exact --nocapture \
    >> out \
    && sudo rm -rf data

sudo perf script | ../FlameGraph/stackcollapse-perf.pl > out.perf-folded

../FlameGraph/flamegraph.pl out.perf-folded > perf.svg

echo "Done, flamegraph: http://10.10.29.13:8000/perf.svg"