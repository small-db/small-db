# ============================================================================== #
# base configuration
# ============================================================================== #
cmake_minimum_required(VERSION 3.28)
project(grpc-system LANGUAGES CXX)

# ============================================================================== #
# compiler settings
# ============================================================================== #

set(CMAKE_CXX_STANDARD 20)

# ============================================================================== #
# external packages
# ============================================================================== #

find_package(Protobuf CONFIG REQUIRED)

# "gRPC" must comes after "Protobuf" since it depends on "Protobuf"
find_package(gRPC CONFIG REQUIRED)

# ======================================================================== #
# protobuf target
# ======================================================================== #

add_library(foo_proto
    foo.proto
)
target_link_libraries(foo_proto
    PUBLIC
    # protobuf::libprotobuf
    # gRPC::grpc
    # gRPC::grpc++
    absl::check
    absl::flags
    absl::flags_parse
    absl::log
    gRPC::grpc++_reflection
    protobuf::libprotobuf
)
target_include_directories(foo_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)
protobuf_generate(TARGET foo_proto
    LANGUAGE cpp
)
protobuf_generate(
    TARGET foo_proto
    LANGUAGE grpc
    GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
    PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin_location}"
)

# ======================================================================== #
# program target
# ======================================================================== #

add_executable(main
    main.cc
)

target_link_libraries(main
    PUBLIC
    foo_proto
)
