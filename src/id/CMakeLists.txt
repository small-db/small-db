add_library(id_lib
    generator.cc
    generator.h
)

target_link_libraries(id_lib
    PUBLIC
    spdlog
    absl::status
)
