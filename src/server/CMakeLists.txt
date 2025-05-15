add_library(small_server
    server.cc
    server.h
    stmt_handler.cc
    stmt_handler.h
)

target_link_libraries(small_server
    PUBLIC
    small::server_info
    PRIVATE
    small::schema
    small::insert_proto
    small::catalog
    small::gossip
    small::semantics
    small::util::ip
    small::pg_wire
    spdlog::spdlog
    libpg_query_lib
    query_lib
    small::insert
    ssl
    crypto
    server_registry_proto
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
