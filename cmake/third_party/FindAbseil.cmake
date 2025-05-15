FetchContent_Declare(Abseil
  GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
  GIT_TAG lts_2025_01_27
  GIT_SHALLOW TRUE
)

get_all_targets(. BEFORE_TARGETS)

FetchContent_MakeAvailable(Abseil)

get_all_targets(. AFTER_TARGETS)
print_added_target(BEFORE_TARGETS AFTER_TARGETS)