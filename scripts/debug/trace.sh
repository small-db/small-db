#!/bin/bash

# Trace a specific test.
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

function trace_linux() {
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

    # echo "Running target, gathering syscall..."

    # # clear logs
    # sudo rm -f out

    # RUST_LOG=info \
    #     sudo perf stat -e 'syscalls:sys_enter_*' -- \
    #     $BINARY -- \
    #     $TEST_NAME --exact --nocapture \
    #     2>&1 | grep syscalls | sort \
    #     >>out &&
    #     sudo rm -rf data

    # ===============================================================
    # run target, generate flamegraph (and perf report)
    # ===============================================================

    echo "Running target, generating flamegraph..."

    # sudo perf record -F 1000 --call-graph dwarf -- \
    # sudo perf record -F 99 -g -- \

    RUST_LOG=debug \
        sudo perf record -F 500 -g -- \
        $BINARY -- \
        $TEST_NAME --exact --nocapture \
        >>out &&
        sudo rm -rf data

    sudo perf script | ../FlameGraph/stackcollapse-perf.pl >out.perf-folded

    ../FlameGraph/flamegraph.pl out.perf-folded >perf.svg

    echo "Done, flamegraph: http://10.10.29.13:8000/perf.svg"
}

function trace_mac() {
    TEST_NAME=$1
    CARGO_PROFILE_BENCH_DEBUG=true sudo cargo flamegraph --test small_tests -- ${TEST_NAME} --exact --nocapture
    sudo chmod -R 777 ./data
}

function cargo_approach() {
    cargo install flamegraph
    sudo sysctl -w kernel.kptr_restrict=0
    CARGO_PROFILE_BENCH_DEBUG=true cargo flamegraph -F 300 --test small_tests -- integretions::btree_test::test_insert_benchmark
    echo "Done, flamegraph: http://10.10.29.13:8000/flamegraph.svg"
}

# trace_linux "$@"
trace_mac "$@"
