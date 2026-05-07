add_library(small_closedts
    registry.h
    registry.cc
)

target_link_libraries(small_closedts
    PUBLIC
    absl::status
    absl::statusor
    PRIVATE
    small::txn
    spdlog::spdlog
    absl::strings
)

add_library(small::closedts ALIAS small_closedts)
