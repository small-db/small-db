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
