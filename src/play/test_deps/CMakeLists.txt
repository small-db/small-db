# ============================================================================== #
# base configuration
# ============================================================================== #
cmake_minimum_required(VERSION 3.28)

project(project_test_deps LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/../../../cmake/recipes/external")
include(grpc)