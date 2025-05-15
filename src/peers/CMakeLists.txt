add_library(server_registry
    server_registry.cc
    server_registry.h
)

target_link_libraries(server_registry
    PUBLIC
    spdlog
    absl::status
    server_registry_proto
    small::server_info
    small::util::ip
)

# ======================================================================== #
# protobuf target
# ======================================================================== #

add_library(server_registry_proto
    server_registry.proto
)
target_link_libraries(server_registry_proto
    PUBLIC
    protobuf::libprotobuf
    gRPC::grpc
    gRPC::grpc++
)
target_include_directories(server_registry_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)
protobuf_generate(TARGET server_registry_proto LANGUAGE cpp)
protobuf_generate(TARGET server_registry_proto LANGUAGE grpc GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin_location}")
