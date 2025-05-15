FetchContent_Declare(
    json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.3
    GIT_SHALLOW TRUE
)

get_all_targets(. BEFORE_TARGETS)

FetchContent_MakeAvailable(json)

get_all_targets(. AFTER_TARGETS)
print_added_target(BEFORE_TARGETS AFTER_TARGETS)

