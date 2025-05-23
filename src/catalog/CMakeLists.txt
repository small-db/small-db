add_library(small_catalog
    catalog.h
    catalog.cc
)

target_link_libraries(small_catalog
    PUBLIC
    small::catalog_proto
    small::rocks
    libpg_query_lib
    PRIVATE
    small::schema
    small::gossip
    spdlog::spdlog
    nlohmann_json::nlohmann_json
    rocksdb
)

add_library(small::catalog ALIAS small_catalog)

# ======================================================================== #
# protobuf target
# ======================================================================== #

add_library(small_catalog_proto
    catalog.proto
)
target_link_libraries(small_catalog_proto
    PUBLIC
    small::schema_proto
    protobuf::libprotobuf
    gRPC::grpc
    gRPC::grpc++
)
target_include_directories(small_catalog_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)
protobuf_generate(
    TARGET small_catalog_proto
    LANGUAGE cpp
    IMPORT_DIRS ${CMAKE_SOURCE_DIR}
    PROTOC_OUT_DIR "${CMAKE_BINARY_DIR}"
)
protobuf_generate(
    TARGET small_catalog_proto
    LANGUAGE grpc
    GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
    PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin_location}"
    IMPORT_DIRS ${CMAKE_SOURCE_DIR}
    PROTOC_OUT_DIR "${CMAKE_BINARY_DIR}"
)

add_library(small::catalog_proto ALIAS small_catalog_proto)
