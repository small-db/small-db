# ======================================================================== #
# cpp targets
# ======================================================================== #

add_library(small_type
    type.cc
    type.h
)

target_link_libraries(small_type
    PUBLIC
    absl::statusor
    arrow_lib
    magic_enum
    pqxx
)

add_library(small::type ALIAS small_type)

# ======================================================================== #
# protobuf targets
# ======================================================================== #

add_library(small_type_proto
    type.proto
)
target_link_libraries(small_type_proto
    PUBLIC
    protobuf::libprotobuf
    gRPC::grpc
    gRPC::grpc++
)
target_include_directories(small_type_proto
    PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}
)

protobuf_generate(
    TARGET small_type_proto
    LANGUAGE cpp
    IMPORT_DIRS ${CMAKE_SOURCE_DIR}
    PROTOC_OUT_DIR "${CMAKE_BINARY_DIR}"
)
protobuf_generate(
    TARGET small_type_proto
    LANGUAGE grpc
    GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
    PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin_location}"
    IMPORT_DIRS ${CMAKE_SOURCE_DIR}
    PROTOC_OUT_DIR "${CMAKE_BINARY_DIR}"
)

add_library(small::type_proto ALIAS small_type_proto)
