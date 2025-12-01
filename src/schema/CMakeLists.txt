add_library(small_schema
    const.h
    schema.cc
    schema.h
    partition.cc
    partition.h
)

target_link_libraries(small_schema
    PUBLIC
    nlohmann_json::nlohmann_json
    PRIVATE
    small::schema_proto
    small::rocks
    small::type
    small::id
    rocksdb
    spdlog
    absl::status
    libpg_query_lib
)

add_library(small::schema ALIAS small_schema)

# ======================================================================== #
# protobuf targets
# ======================================================================== #

small_proto_target(
    TARGET schema_proto
    SOURCES schema.proto
    DEPS type_proto
)
