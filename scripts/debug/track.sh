#!/bin/bash

# Trace a specific test.
#   - Gather syscall
#   - Generate flamegraph
#
# Usage:
#   ./scripts/debug/track.sh <test_name>
#
# e.g:
#   ./scripts/debug/track.sh test_insert_parallel

set -o errexit
set -o nounset

DATA_DIR="/media/xiaochen/large/cs_data/smalldb"

function trace_linux() {
    # request sudo access
    sudo echo "sudo access granted"

    # ===============================================================
    # get binary path
    # ===============================================================

    BINARY=$(python3 ./scripts/debug/get_test_binary.py)
    TEST_NAME=$1
    echo "BINARY: $BINARY"

    # ===============================================================
    # run target, generate flamegraph (and perf report)
    # ===============================================================

    echo "running target ..."

    RUST_LOG=debug \
        sudo perf record -F 100 --call-graph dwarf -- \
        $BINARY -- \
        $TEST_NAME --exact --nocapture \
        >>out

    sudo chmod -R 777 "$DATA_DIR"

    echo "generating flamegraph ..."

    sudo perf script | ../FlameGraph/stackcollapse-perf.pl >out.perf-folded

    ../FlameGraph/flamegraph.pl out.perf-folded >perf.svg

    echo "done, flamegraph: http://10.0.0.90:8000/perf.svg"

    if [ -d "$DATA_DIR" ]; then
        sudo chmod -R 777 $DATA_DIR
    fi

    python3 -m http.server 8000
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

trace_linux "$@"
# trace_mac "$@"
