enable_testing()

add_executable(
    sql_test
    sql_test.cc
)

target_link_libraries(
    sql_test
    PRIVATE
    small::type
    small::server
    GTest::gtest_main
    pqxx
    parser_lib
    spdlog::spdlog
)

# Avoid letting gtest use gcc's cxxabi.h, as it conflicts with llvm's cxxabi.h.  
# The latter is required by arrow gandiva and cannot be blocked.
# 
# source code:
# https://github.com/google/googletest/blob/e90fe2485641bab0d6af4500192dc503384950d1/googletest/include/gtest/internal/gtest-type-util.h#L48
target_compile_definitions(sql_test PRIVATE GTEST_HAS_CXXABI_H_=0)

include(GoogleTest)
gtest_discover_tests(sql_test)
