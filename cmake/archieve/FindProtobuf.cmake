set(protobuf_BUILD_TESTS OFF)
set(protobuf_INSTALL OFF)

FetchContent_Declare(
    protobuf
    GIT_REPOSITORY "https://github.com/protocolbuffers/protobuf.git"
    GIT_TAG "v30.2"
    GIT_SHALLOW TRUE
)
get_all_targets(. BEFORE_TARGETS)

set(FETCHCONTENT_QUIET OFF)
FetchContent_MakeAvailable(protobuf)

get_all_targets(. AFTER_TARGETS)
print_added_target(BEFORE_TARGETS AFTER_TARGETS)

