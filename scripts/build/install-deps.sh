#!/usr/bin/env bash

set -o xtrace
set -o errexit
set -o nounset

sudo apt-get install -y \
    libpq-dev \
    libpqxx-dev \
    uuid-dev

# grpc
git clone --branch=v1.71.0 --recurse-submodules --depth=1 https://github.com/grpc/grpc.git
cd grpc
cmake \
    -S . \
    -B ./cmake/build \
    -G Ninja \
    -DCMAKE_CXX_STANDARD=17 \
    -DgRPC_INSTALL=ON \
    -DCMAKE_INSTALL_PREFIX=~/local/cpplib
cmake --build ./cmake/build --target install