set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_GRPC_REFLECTION gRPC::grpc++_GRPC_REFLECTION)
set(_GRPC_GRPCPP gRPC::grpc++)

message(STATUS "third-party (external): creating target ${_PROTOBUF_LIBPROTOBUF}")
message(STATUS "third-party (external): creating target ${_GRPC_REFLECTION}")
message(STATUS "third-party (external): creating target ${_GRPC_GRPCPP}")

# Assumes that gRPC and all its dependencies are already installed
# on this system, so they can be located by find_package().

# Find Protobuf installation
# Looks for protobuf-config.cmake file installed by Protobuf's cmake installation.
option(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_GRPC_REFLECTION gRPC::grpc++_GRPC_REFLECTION)
if(CMAKE_CROSSCOMPILING)
    find_program(_PROTOBUF_PROTOC protoc)
else()
    set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
endif()

# Find gRPC installation
# Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_GRPC_GRPCPP gRPC::grpc++)
if(CMAKE_CROSSCOMPILING)
    find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else()
    set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif()

get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)

function(small_proto_target NAME)
    set(TARGET_NAME "small_${NAME}")
    set(PROTO_FILES ${ARGN})

    add_library(${TARGET_NAME}
        ${PROTO_FILES}
    )

    target_link_libraries(${TARGET_NAME}
        PUBLIC
        protobuf::libprotobuf
        gRPC::grpc
        gRPC::grpc++
    )

    protobuf_generate(
        TARGET ${TARGET_NAME}
        LANGUAGE cpp
        IMPORT_DIRS ${CMAKE_SOURCE_DIR}
        PROTOC_OUT_DIR "${CMAKE_BINARY_DIR}"
    )

    protobuf_generate(
        TARGET ${TARGET_NAME}
        LANGUAGE grpc
        GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
        PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin_location}"
        IMPORT_DIRS ${CMAKE_SOURCE_DIR}
        PROTOC_OUT_DIR "${CMAKE_BINARY_DIR}"
    )

    add_library(small::${NAME} ALIAS ${TARGET_NAME})
endfunction()
