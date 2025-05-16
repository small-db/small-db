add_library(small_catalog
    catalog.h
    catalog.cc
)

target_link_libraries(small_catalog
    PRIVATE
    spdlog::spdlog
    nlohmann_json::nlohmann_json
    small::schema
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
    protobuf::libprotobuf
    gRPC::grpc
    gRPC::grpc++
)
target_include_directories(small_catalog_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)
protobuf_generate(TARGET small_catalog_proto
    LANGUAGE cpp
)
protobuf_generate(
    TARGET small_catalog_proto
    LANGUAGE grpc
    GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
    PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin_location}"
)

add_library(small::catalog_proto ALIAS small_catalog_proto)
