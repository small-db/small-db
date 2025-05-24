add_library(small_gossip
  gossip.h
  gossip.cc
)

target_link_libraries(small_gossip
  PUBLIC
  small::gossip_proto
  PRIVATE
  small::server_info
  spdlog::spdlog
)

add_library(small::gossip ALIAS small_gossip)

# ======================================================================== #
# protobuf target
# ======================================================================== #

small_proto_target(
  TARGET gossip_proto
  SOURCES gossip.proto
)
