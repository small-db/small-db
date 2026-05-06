add_library(small_server
    server.cc
    server.h
)

target_link_libraries(small_server
    PUBLIC
    small::server_info
    PRIVATE
    small::schema
    small::execution_proto
    small::catalog
    small::gossip
    small::txn
    small::txn_handle
    small::id
    small::rocks
    small::semantics
    small::util::ip
    small::pg_wire
    small::execution
    spdlog::spdlog
    libpg_query_lib
    ssl
    crypto
    magic_enum
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
