add_library(query_lib
    query.cc
    query.h
)

target_link_libraries(query_lib
    PUBLIC
    spdlog
    absl::status
    libpg_query_lib
    arrow_lib
    small::rocks
    small::schema
    magic_enum
    small::server_info
)
