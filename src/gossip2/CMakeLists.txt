add_library(small_gossip
  gossip.h
  gossip.cc
)

target_link_libraries(small_gossip
  hw_grpc_proto
)

add_library(small::gossip ALIAS small_gossip)

# ======================================================================== #
# protobuf target (experimental)
# ======================================================================== #

get_filename_component(hw_proto "./gossip.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/gossip.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/gossip.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/gossip.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/gossip.grpc.pb.h")
add_custom_command(
  OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
  COMMAND ${_PROTOBUF_PROTOC}
  ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
  --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
  -I "${hw_proto_path}"
  --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
  "${hw_proto}"
  DEPENDS "${hw_proto}")

# hw_grpc_proto
add_library(hw_grpc_proto
  ${hw_grpc_srcs}
  ${hw_grpc_hdrs}
  ${hw_proto_srcs}
  ${hw_proto_hdrs})
target_link_libraries(hw_grpc_proto
  absl::check
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

# Include generated *.pb.h files
target_include_directories(hw_grpc_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
