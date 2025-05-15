add_library(small_gossip
    gossip.h
    gossip.cc
)

target_link_libraries(small_gossip
    PRIVATE
    small::server_info
    spdlog
    absl::status
)

add_library(small::gossip ALIAS small_gossip)
