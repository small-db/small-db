#!/usr/bin/env bash

set -o xtrace
set -o errexit
set -o nounset

cmake --preset=debug
cmake --build ./build/debug
