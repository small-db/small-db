add_library(small_execution
    query.cc
    query.h
    update.cc
    update.h
    insert.cc
    insert.h
)

target_link_libraries(small_execution
    PUBLIC
    small::execution_proto
    PRIVATE
    small::rocks
    small::schema
    small::server_info
    small::semantics
    small::catalog
    small::gossip
    small::type
    spdlog
    absl::status
    libpg_query_lib
    arrow_lib
    magic_enum
)

add_library(small::execution ALIAS small_execution)

# ======================================================================== #
# protobuf target
# ======================================================================== #

small_proto_target(
    TARGET execution_proto
    SOURCES execution.proto
)

add_dependencies(small_execution small_execution_proto)
