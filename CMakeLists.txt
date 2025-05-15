# ============================================================================== #
# base configuration
# ============================================================================== #
cmake_minimum_required(VERSION 3.28)
project(small-db-v2)

# ============================================================================== #
# compiler settings
# ============================================================================== #

set(CMAKE_CXX_STANDARD 20)

# ======================================================================== #
# helper functions
# ======================================================================== #

add_subdirectory(cmake/helper)

# ======================================================================== #
# third-party packages (system wide)
# ======================================================================== #

# "grpc" is installed on system wide since it's too large.
# 
# process of installing "grpc":
# > gcl --depth 1 git@github.com:grpc/grpc.git
# > cd grpc
# > cmake -S . -B ./cmake/build -DCMAKE_CXX_STANDARD=17 -DgRPC_INSTALL=ON -DCMAKE_INSTALL_PREFIX=~/local/cpplib -G Ninja
# > cmake --build ./cmake/build --target install
# 
# "grpc" bundles following packages:
# - "absl" (<CMAKE_INSTALL_PREFIX>/lib/cmake/absl/abslConfig.cmake)
# - "c-ares" (<CMAKE_INSTALL_PREFIX>/lib/cmake/c-ares/c-aresConfig.cmake)
# - "gRPC" (<CMAKE_INSTALL_PREFIX>/lib/cmake/grpc/gRPCConfig.cmake)
# - "protobuf" (<CMAKE_INSTALL_PREFIX>/lib/cmake/protobuf/protobuf-config.cmake)
# - "re2" (<CMAKE_INSTALL_PREFIX>/lib/cmake/re2/re2Config.cmake)
# - "utf_range" (<CMAKE_INSTALL_PREFIX>/lib/cmake/utf_range/utf_range-config.cmake)

find_package(Protobuf CONFIG REQUIRED)

# "gRPC" must comes after "Protobuf" since it depends on "Protobuf"
find_package(gRPC CONFIG REQUIRED)

# Find "absl" and "re2" from system wide installation.
# 
# We cannot use our local installation of these packages since "gRPC" brings its own
# version of these packages. If we include them locally (e.g. via FetchContent),
# ODR ("one definition rule" violation) will be violated and cause linking errors.
#
# Some explanations about ODR:
# - https://github.com/abseil/abseil-cpp/blob/master/FAQ.md#what-is-abi-and-why-dont-you-recommend-using-a-pre-compiled-version-of-abseil
find_package(absl CONFIG REQUIRED)
find_package(re2 CONFIG REQUIRED)

# ======================================================================== #
# third-party packages (local)
# ======================================================================== #

# step 1: update CMAKE_MODULE_PATH
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/third_party/")

# step 2: include FetchContent module
include(FetchContent)
set(FETCHCONTENT_QUIET OFF)

# step 3: include third-party packages
find_package(Arrow REQUIRED)
find_package(Spdlog REQUIRED)
find_package(PgQuery REQUIRED)
find_package(GoogleTest REQUIRED)
find_package(RocksDB REQUIRED)
find_package(Json REQUIRED)
find_package(MagicEnum REQUIRED)
find_package(CLI11 REQUIRED)
# find_package(UUID_V4 REQUIRED)

# ============================================================================== #
# source code
# ============================================================================== #

include_directories(
    ${CMAKE_SOURCE_DIR}
)

# define macros
add_compile_definitions(SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_TRACE)

add_subdirectory(src)
add_subdirectory(test)
