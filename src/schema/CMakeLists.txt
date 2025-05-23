add_library(small_schema
    const.h
    schema.cc
    schema.h
    partition.cc
    partition.h
)

target_link_libraries(small_schema
    PUBLIC
    nlohmann_json::nlohmann_json
    PRIVATE
    small::schema_proto
    small::rocks
    small::type
    small::server_info
    small::id
    rocksdb
    spdlog
    absl::status
    libpg_query_lib
)

add_library(small::schema ALIAS small_schema)

# ======================================================================== #
# protobuf targets
# ======================================================================== #

add_library(small_schema_proto
    schema.proto
)
target_link_libraries(small_schema_proto
    PUBLIC
    protobuf::libprotobuf
    gRPC::grpc
    gRPC::grpc++
)

protobuf_generate(
    TARGET small_schema_proto
    LANGUAGE cpp
    IMPORT_DIRS ${CMAKE_SOURCE_DIR}
    PROTOC_OUT_DIR "${CMAKE_BINARY_DIR}"
)
protobuf_generate(
    TARGET small_schema_proto
    LANGUAGE grpc
    GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
    PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin_location}"
    IMPORT_DIRS ${CMAKE_SOURCE_DIR}
    PROTOC_OUT_DIR "${CMAKE_BINARY_DIR}"
)

add_library(small::schema_proto ALIAS small_schema_proto)
