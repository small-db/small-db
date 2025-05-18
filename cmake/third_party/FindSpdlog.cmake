FetchContent_Declare(
    spdlog_content
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG v1.15.1
    GIT_SHALLOW TRUE
)

# We use fmt::format instead of std::format in small-db.
# 
# This allows us to define custom formatters (fmt::formatter<T>) for
# std types (like std::unordered_map) without causing compiler errors
# or undefined behavior.
set(SPDLOG_USE_STD_FORMAT OFF)

FetchContent_MakeAvailable(spdlog_content)
