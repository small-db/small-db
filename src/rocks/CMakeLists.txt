add_library(small_rocks
    rocks.h
    rocks.cc
)

target_link_libraries(small_rocks
    PUBLIC
    rocksdb
    spdlog
    absl::status
    small::type
    small::encode
    small::schema
)

add_library(small::rocks ALIAS small_rocks)
