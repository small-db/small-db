add_executable(play_format
    main.cc
)

target_link_libraries(play_format
    PRIVATE
    spdlog::spdlog
)
