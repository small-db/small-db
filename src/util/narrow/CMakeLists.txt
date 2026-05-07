add_library(small_util_narrow INTERFACE)

target_sources(small_util_narrow
    INTERFACE
    narrow.h
)

add_library(small::util::narrow ALIAS small_util_narrow)
