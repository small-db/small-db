FetchContent_Declare(
    majic_enum
    GIT_REPOSITORY https://github.com/Neargye/magic_enum.git
    GIT_TAG v0.9.7
    GIT_SHALLOW TRUE
)

get_all_targets(. BEFORE_TARGETS)

FetchContent_MakeAvailable(majic_enum)

get_all_targets(. AFTER_TARGETS)
print_added_target(BEFORE_TARGETS AFTER_TARGETS)

