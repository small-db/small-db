add_library(small_query
    query.cc
    query.h
)

target_link_libraries(small_query
    PRIVATE
    small::rocks
    small::schema
    small::server_info
    small::catalog
    small::type
    spdlog
    absl::status
    libpg_query_lib
    arrow_lib
    magic_enum
)

add_library(small::query ALIAS small_query)