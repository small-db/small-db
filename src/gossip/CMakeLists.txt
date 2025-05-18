add_library(small_gossip
    gossip.h
    gossip.cc
)

target_link_libraries(small_gossip
    PUBLIC
    small::gossip_proto
    PRIVATE
    small::server_info
    nlohmann_json::nlohmann_json
    spdlog::spdlog
)

add_library(small::gossip ALIAS small_gossip)

# ======================================================================== #
# protobuf target
# ======================================================================== #

add_library(small_gossip_proto
    gossip.proto
)
target_link_libraries(small_gossip_proto
    PUBLIC
    protobuf::libprotobuf
    gRPC::grpc
    gRPC::grpc++
)
target_include_directories(small_gossip_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)
protobuf_generate(TARGET small_gossip_proto
    LANGUAGE cpp
)
protobuf_generate(
    TARGET small_gossip_proto
    LANGUAGE grpc
    GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
    PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin_location}"
)
add_library(small::gossip_proto ALIAS small_gossip_proto)
