add_library(small_util_time INTERFACE)

target_sources(small_util_time
    INTERFACE
    time.h
)

add_library(small::util::time ALIAS small_util_time)
