#!/usr/bin/env bash

set -o xtrace
set -o errexit
set -o nounset

PLAY_DIR="/home/xiaochen/code/small-db/play"

cmake \
    -S "$PLAY_DIR"/grpc-system \
    -B "$PLAY_DIR"/gprc-system/build \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DCMAKE_C_COMPILER=/usr/bin/clang-18 \
    -DCMAKE_CXX_COMPILER=/usr/bin/clang++-18 \
    -DCMAKE_PREFIX_PATH=~/local/cpplib
