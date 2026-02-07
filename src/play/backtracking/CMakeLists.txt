# ============================================================================== #
# base configuration
# ============================================================================== #
cmake_minimum_required(VERSION 3.28)
project(project_backtracking LANGUAGES CXX)

include(FetchContent)

FetchContent_Declare(backward
    GIT_REPOSITORY https://github.com/bombela/backward-cpp
    GIT_TAG v1.6 # or a version tag, such as v1.6
    SYSTEM # optional, the Backward include directory will be treated as system directory
)
FetchContent_MakeAvailable(backward)

add_executable(backtracking
    main.cc
)

# Link backward library and required system libraries
target_link_libraries(backtracking PUBLIC 
    backward
    dl          # Dynamic linking library
    dw          # DWARF library (libdw)
    bfd         # Binary file descriptor library
)

target_include_directories(backtracking PRIVATE ${backward_SOURCE_DIR})

# Enable debug symbols and optimization flags for better stack traces
target_compile_options(backtracking PRIVATE 
    -g          # Debug symbols
    -O1         # Light optimization (better than -O0 for stack traces)
    -fno-omit-frame-pointer  # Keep frame pointers
)
