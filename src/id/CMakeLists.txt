add_library(small_id
    generator.cc
    generator.h
)

target_link_libraries(small_id
    PRIVATE
    small::server_info
    spdlog
    absl::status
)

add_library(small::id ALIAS small_id)
