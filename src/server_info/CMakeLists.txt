add_library(small_server_info
    info.h
    info.cc
)

target_link_libraries(small_server_info
    PUBLIC
    absl::statusor
    nlohmann_json::nlohmann_json
    PRIVATE
    spdlog
    uuid
)

add_library(small::server_info ALIAS small_server_info)
