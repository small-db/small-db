#!/usr/bin/env bash

set -o xtrace
set -o errexit
set -o nounset

PLAY_DIR="/home/xiaochen/code/small-db/play"

# ======================================================================== #
# grpc-system
# ======================================================================== #

cd "$PLAY_DIR"/grpc-system/grpc

LOCAL_INSTALL_DIR="$PLAY_DIR"/grpc-system/local_libs

cmake -DgRPC_INSTALL=ON \
    -DgRPC_BUILD_TESTS=OFF \
    -DCMAKE_CXX_STANDARD=17 \
    -DCMAKE_INSTALL_PREFIX=$LOCAL_INSTALL_DIR \
    -S . \
    -B ./cmake/build

cd ./cmake/build
make -j8
make install


cd /home/xiaochen/code/grpc/examples/cpp/helloworld
cmake -S . \
    -B ./cmake/build \
    -DCMAKE_PREFIX_PATH=/home/xiaochen/code/small-db/play/grpc-system/local_libs
cmake --build ./cmake/build


cmake -S . -B ./build/fuck -DCMAKE_PREFIX_PATH=~/local/cpplib
