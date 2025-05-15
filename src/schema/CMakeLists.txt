add_library(small_schema
    const.h
    schema.cc
    schema.h
    partition.cc
    partition.h
)

target_link_libraries(small_schema
    PUBLIC
    rocksdb
    spdlog
    absl::status
    libpg_query_lib
    nlohmann_json::nlohmann_json
    small::rocks
    id_lib
    small::type
    small::encode
    small::server_info
    small::insert
)

add_library(small::schema ALIAS small_schema)