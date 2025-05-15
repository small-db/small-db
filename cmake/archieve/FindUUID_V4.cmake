FetchContent_Declare(
    uuid_v4
    GIT_REPOSITORY https://github.com/mariusbancila/stduuid.git
    GIT_TAG v1.0.0
    GIT_SHALLOW TRUE
)

get_all_targets(. BEFORE_TARGETS)

FetchContent_MakeAvailable(uuid_v4)

get_all_targets(. AFTER_TARGETS)
print_added_target(BEFORE_TARGETS AFTER_TARGETS)

