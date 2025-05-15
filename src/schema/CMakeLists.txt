add_library(small_schema
    const.h
    schema.cc
    schema.h
    partition.cc
    partition.h
)

target_link_libraries(small_schema
    PUBLIC
    rocksdb
    spdlog
    absl::status
    libpg_query_lib
    nlohmann_json::nlohmann_json
    small::rocks
    id_lib
    small::type
    small::encode
    small::server_info
    small::insert
)

add_library(small::schema ALIAS small_schema)

# ======================================================================== #
# protobuf target
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
target_include_directories(small_schema_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)
protobuf_generate(TARGET small_schema_proto LANGUAGE cpp)
protobuf_generate(TARGET small_schema_proto LANGUAGE grpc GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin_location}")

add_library(small::schema_proto ALIAS small_schema_proto)
