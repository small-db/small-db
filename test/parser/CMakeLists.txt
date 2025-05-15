add_library(parser_lib
    parser.cc
    parser.h
)

target_link_libraries(parser_lib
    PRIVATE
    spdlog
    absl::status
    absl::statusor
    absl::strings
    small::type
)
