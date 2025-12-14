add_library(small_rocks
    rocks.h
    rocks.cc
)

target_link_libraries(small_rocks
    PUBLIC
    rocksdb
    absl::status
    small::schema
    PRIVATE
    spdlog
    small::type
    small::server_info
)

add_library(small::rocks ALIAS small_rocks)

add_executable(rocks_scan
    rocks_scan.cc
)

target_link_libraries(rocks_scan
    PRIVATE
    small::rocks
    spdlog::spdlog
    CLI11::CLI11
)

