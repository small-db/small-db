add_library(small_semantics
    check.cc
    check.h
    extract.cc
    extract.h
)

target_link_libraries(small_semantics
    PRIVATE
    libpg_query_lib
    magic_enum
    spdlog
    small::type
)

add_library(small::semantics ALIAS small_semantics)
