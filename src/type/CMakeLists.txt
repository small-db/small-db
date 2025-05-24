# ======================================================================== #
# cpp targets
# ======================================================================== #

add_library(small_type
    type.cc
    type.h
)

target_link_libraries(small_type
    PUBLIC
    absl::statusor
    arrow_lib
    magic_enum
    pqxx
)

add_library(small::type ALIAS small_type)

# ======================================================================== #
# protobuf targets
# ======================================================================== #

small_proto_target(
    TARGET type_proto
    SOURCES type.proto
)
