FetchContent_Declare(
    spdlog_content
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG v1.15.1
    GIT_SHALLOW TRUE
)

FetchContent_MakeAvailable(spdlog_content)
