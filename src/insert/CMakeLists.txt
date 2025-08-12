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
    small::catalog
    small::gossip
    spdlog
    absl::status
    libpg_query_lib
    arrow_lib
    magic_enum
)

add_library(small::insert ALIAS small_insert)

# ======================================================================== #
# protobuf target
# ======================================================================== #

small_proto_target(
    TARGET insert_proto 
    SOURCES insert.proto
)
