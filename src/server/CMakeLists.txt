FetchContent_Declare(backward
    GIT_REPOSITORY https://github.com/bombela/backward-cpp
    GIT_TAG master # or a version tag, such as v1.6
    SYSTEM # optional, the Backward include directory will be treated as system directory
)
FetchContent_MakeAvailable(backward)

add_library(small_server
    server.cc
    server.h
    stmt_handler.cc
    stmt_handler.h
)

target_link_libraries(small_server
    PUBLIC
    small::server_info
    Backward::Interface
    PRIVATE
    small::schema
    small::insert_proto
    small::catalog
    small::gossip
    small::semantics
    small::util::ip
    small::pg_wire
    small::insert
    small::query
    spdlog::spdlog
    libpg_query_lib
    ssl
    crypto
    absl::status
)

add_library(small::server ALIAS small_server)

add_executable(server
    main.cc
)

target_link_libraries(server
    PRIVATE
    small::server
    spdlog::spdlog
    CLI11::CLI11
)
