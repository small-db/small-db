set(PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(GRPC_REFLECTION gRPC::grpc++GRPC_REFLECTION)
set(GRPC_GRPCPP gRPC::grpc++)

message(STATUS "third-party (external): creating target ${PROTOBUF_LIBPROTOBUF}")
message(STATUS "third-party (external): creating target ${GRPC_REFLECTION}")
message(STATUS "third-party (external): creating target ${GRPC_GRPCPP}")

# Assumes that gRPC and all its dependencies are already installed
# on this system, so they can be located by find_package().

# Find Protobuf installation
# Looks for protobuf-config.cmake file installed by Protobuf's cmake installation.
option(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

set(PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(GRPC_REFLECTION gRPC::grpc++GRPC_REFLECTION)
if(CMAKE_CROSSCOMPILING)
    find_program(_PROTOBUF_PROTOC protoc)
else()
    set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
endif()

# Find gRPC installation
# Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(GRPC_GRPCPP gRPC::grpc++)
if(CMAKE_CROSSCOMPILING)
    find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else()
    set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif()
