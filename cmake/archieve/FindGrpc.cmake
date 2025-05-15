# > cmake -S . -B ./cmake/build -DCMAKE_CXX_STANDARD=17 -DgRPC_INSTALL=ON -DCMAKE_INSTALL_PREFIX=~/local/cpplib -G Ninja
# > cmake --build ./cmake/build
# need sudo since zlib doesn't care about CMAKE_INSTALL_PREFIX
# https://github.com/madler/zlib/blob/develop/CMakeLists.txt
# > sudo cmake --build ./cmake/build --target install

# set(gRPC_PROTOBUF_PROVIDER "package")
# set(gRPC_ABSL_PROVIDER "package")

include(FetchContent)

set(protobuf_BUILD_TESTS OFF)
set(protobuf_INSTALL OFF)

FetchContent_Declare(
    protobuf
    GIT_REPOSITORY "https://github.com/protocolbuffers/protobuf.git"
    GIT_TAG "v30.2"
    GIT_SHALLOW TRUE
)

set(FETCHCONTENT_QUIET OFF)
FetchContent_MakeAvailable(protobuf)

set(FETCHCONTENT_QUIET OFF)
set(gRPC_BUILD_CODEGEN ON)
set(gRPC_INSTALL OFF)
set(gRPC_BUILD_TESTS OFF)

FetchContent_Declare(
    grpc
    GIT_REPOSITORY https://github.com/grpc/grpc.git
    GIT_TAG v1.71.0
    GIT_SHALLOW TRUE
)

get_all_targets(. BEFORE_TARGETS)

set(FETCHCONTENT_QUIET OFF)
FetchContent_MakeAvailable(grpc)

get_all_targets(. AFTER_TARGETS)
print_added_target(BEFORE_TARGETS AFTER_TARGETS)

if (TARGET gRPC::grpc_cpp_plugin)
    get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)
    message(STATUS "grpc_cpp_plugin found at: ${grpc_cpp_plugin_location}")
else()
    message(FATAL_ERROR "grpc_cpp_plugin was not built!")
endif()
