add_library(small_server_info
    info.h
    info.cc
)

target_link_libraries(small_server_info
    PUBLIC
    absl::statusor
    PRIVATE
    spdlog
    # spdlog::spdlog
    # nlohmann_json::nlohmann_json
    uuid
)

add_library(small::server_info ALIAS small_server_info)
