add_library(small_insert
    insert.cc
    insert.h
)

target_link_libraries(small_insert
    PUBLIC
    small::insert_proto
    PRIVATE
    small::rocks
    small::schema
    small::semantics
    # small::encode
    small::catalog
    small::gossip
    spdlog
    absl::status
    libpg_query_lib
    arrow_lib
    # server_registry
    magic_enum
)

add_library(small::insert ALIAS small_insert)

# ======================================================================== #
# protobuf target
# ======================================================================== #

# add_library(small_insert_proto
#     insert.proto
# )
# target_link_libraries(small_insert_proto
#     PUBLIC
#     protobuf::libprotobuf
#     gRPC::grpc
#     gRPC::grpc++
# )

# get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)
# protobuf_generate(TARGET small_insert_proto LANGUAGE cpp)
# protobuf_generate(TARGET small_insert_proto LANGUAGE grpc GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin_location}")

# add_library(small::insert_proto ALIAS small_insert_proto)

small_proto_target(insert_proto insert.proto)
