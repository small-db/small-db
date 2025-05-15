add_library(small_util_ip
    ip.cc
    ip.h
)

add_library(small::util::ip ALIAS small_util_ip)
