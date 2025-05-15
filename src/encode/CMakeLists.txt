add_library(small_encode
    encode.cc
    encode.h
)

target_link_libraries(small_encode
    PUBLIC
    spdlog
    absl::status
    small::type
)

add_library(small::encode ALIAS small_encode)
