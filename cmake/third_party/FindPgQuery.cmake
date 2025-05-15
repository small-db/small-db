FetchContent_Declare(
    libpg_query
    GIT_REPOSITORY https://github.com/pganalyze/libpg_query.git
    GIT_TAG 17-6.0.0
)

message(STATUS "libpg_query_POPULATED: ${libpg_query_POPULATED}")

if(NOT libpg_query_POPULATED)
    FetchContent_Populate(libpg_query)

    # Build the library using make
    execute_process(COMMAND make -j libpg_query.a libpg_query.so
        WORKING_DIRECTORY ${libpg_query_SOURCE_DIR}
    )
endif()


message(STATUS "libpg_query_POPULATED: ${libpg_query_POPULATED}")

add_library(libpg_query_lib INTERFACE IMPORTED)

target_include_directories(
    libpg_query_lib
    INTERFACE
    ${libpg_query_SOURCE_DIR}
    ${libpg_query_SOURCE_DIR}/protobuf
    ${libpg_query_SOURCE_DIR}/vendor
)

target_link_libraries(
    libpg_query_lib
    INTERFACE
    ${libpg_query_SOURCE_DIR}/libpg_query.a

    # this doesn't work, it will leave "libpg_query.so.1705.1 => not found" in the binary
    # ${libpg_query_SOURCE_DIR}/libpg_query.so
)

target_link_directories(
    libpg_query_lib
    INTERFACE
    ${libpg_query_SOURCE_DIR}
)

message(STATUS "libpg_query_SOURCE_DIR: ${libpg_query_SOURCE_DIR}")
