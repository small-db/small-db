add_library(small_txn
    txn.h
    txn.cc
)

target_link_libraries(small_txn
    PUBLIC
    small::txn_proto
    PRIVATE
    small::rocks
    spdlog::spdlog
    absl::status
    absl::strings
)

add_library(small::txn ALIAS small_txn)

# ======================================================================== #
# protobuf target
# ======================================================================== #

small_proto_target(
    TARGET txn_proto
    SOURCES txn.proto
)

add_dependencies(small_txn small_txn_proto)

# ======================================================================== #
# transaction handle (driver). Separate library so it can depend on
# small::execution without creating a cycle through small::txn.
# ======================================================================== #

add_library(small_txn_handle
    handle.h
    handle.cc
)

target_link_libraries(small_txn_handle
    PUBLIC
    arrow_lib
    PRIVATE
    small::catalog
    small::execution
    small::id
    small::rocks
    small::schema
    small::semantics
    small::server_info
    small::txn
    small::type
    spdlog::spdlog
    absl::status
    absl::strings
    libpg_query_lib
    magic_enum
)

add_library(small::txn_handle ALIAS small_txn_handle)
