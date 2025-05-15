#!/usr/bin/env bash

set -o xtrace
set -o errexit
set -o nounset

./build/debug/test/integration_test/sql_test
