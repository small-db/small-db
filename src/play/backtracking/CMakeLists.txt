find_package(Boost REQUIRED COMPONENTS stacktrace_basic)
find_package(Boost REQUIRED COMPONENTS stacktrace_addr2line)

FetchContent_Declare(backward
    GIT_REPOSITORY https://github.com/bombela/backward-cpp
    GIT_TAG v1.6 # or a version tag, such as v1.6
    SYSTEM # optional, the Backward include directory will be treated as system directory
)
FetchContent_MakeAvailable(backward)

add_executable(backtracking
    main.cc
)

target_link_libraries(backtracking
    PRIVATE
    # Boost::stacktrace_basic
    # Boost::stacktrace_addr2line
    # Backward::Interface
    Backward::Backward
)
