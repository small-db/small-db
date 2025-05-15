#!/bin/bash

# Run a specific test until it crashes.
#
# Usage:
#   ./scripts/debug/run_until_crash.sh <test_name>
#
# e.g:
#   ./scripts/debug/run_until_crash.sh test_concurrent

set -o errexit
set -o nounset

TEST_NAME=$1

# Make the pipeline will not continue if any command fails.
set -o pipefail

for i in $(seq 1 1000); do
    echo "Running test $i..."
    make $TEST_NAME
done


