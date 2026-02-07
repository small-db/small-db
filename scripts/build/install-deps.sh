#!/usr/bin/env bash

set -o xtrace
set -o errexit
set -o nounset

# ======================================================================== #
# install libraries from system source
# ======================================================================== #
sudo apt-get update -y

sudo apt-get install -y \
    libpq-dev \
    libpqxx-dev \
    uuid-dev \
    libdw-dev \
    binutils-dev

# ======================================================================== #
# install libraries locally
# ======================================================================== #

# Install these packages locally by using an appropriately set CMAKE_INSTALL_PREFIX,
# because there is no easy way to uninstall a package after you’ve installed it
# globally.

LIBS_INSTALL_DIR=$(realpath ./cmake/libs_install)
if [ ! -d "$LIBS_INSTALL_DIR" ]; then
    mkdir -p "$LIBS_INSTALL_DIR"
fi

LIBS_SOURCE_DIR=$(realpath ./cmake/libs_source)
if [ ! -d "$LIBS_SOURCE_DIR" ]; then
    mkdir -p "$LIBS_SOURCE_DIR"
fi

# It's important to use the same compiler for all libraries, otherwise
# you may run into ABI incompatibility issues.
C_COMPILER="/usr/bin/clang-18"
CXX_COMPILER="/usr/bin/clang++-18"

pushd "$LIBS_SOURCE_DIR"

# fetach grpc
if [ ! -d "grpc" ]; then
    git clone \
        --recurse-submodules \
        --shallow-submodules \
        --depth 1 \
        -b v1.72.0 \
        https://github.com/grpc/grpc
fi

# install grpc
cd grpc
cmake \
    -S . \
    -B ./cmake/build \
    -G Ninja \
    -DCMAKE_CXX_STANDARD=17 \
    -DCMAKE_C_COMPILER="$C_COMPILER" \
    -DCMAKE_CXX_COMPILER="$CXX_COMPILER" \
    -DgRPC_INSTALL=ON \
    -DgRPC_BUILD_TESTS=OFF \
    -DCMAKE_INSTALL_PREFIX="$LIBS_INSTALL_DIR" \
    -DCMAKE_BUILD_WITH_INSTALL_RPATH=ON
cmake --build ./cmake/build --target install
