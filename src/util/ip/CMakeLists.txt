add_library(small_util_ip
    ip.cc
    ip.h
)

target_link_libraries(small_util_ip
    PRIVATE
    spdlog::spdlog
)

add_library(small::util::ip ALIAS small_util_ip)

