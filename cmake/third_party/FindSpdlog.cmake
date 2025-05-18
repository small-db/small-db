FetchContent_Declare(
    spdlog_content
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG v1.15.1
    GIT_SHALLOW TRUE
)

# SPDLOG_USE_STD_FORMAT
# set(SPDLOG_USE_STD_FORMAT ON)

FetchContent_MakeAvailable(spdlog_content)
