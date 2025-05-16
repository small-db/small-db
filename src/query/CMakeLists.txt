add_library(query_lib
    query.cc
    query.h
)

target_link_libraries(query_lib
    PRIVATE
    small::rocks
    small::schema
    small::server_info
    small::catalog
    spdlog
    absl::status
    libpg_query_lib
    arrow_lib
    magic_enum
)
