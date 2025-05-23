#!/usr/bin/env bash

set -o xtrace
set -o errexit
set -o nounset

rm -rf ./data/
./build/debug/test/integration_test/sql_test
