add_executable(
    dirty_read_test
    dirty_read_test.cc
)

target_link_libraries(
    dirty_read_test
    PRIVATE
    small::catalog
    small::execution
    small::gossip
    small::server_info
    small::txn
    small::txn_handle
    GTest::gtest_main
    spdlog::spdlog
    absl::status
    absl::statusor
    ssl
    crypto
)

target_compile_definitions(dirty_read_test PRIVATE GTEST_HAS_CXXABI_H_=0)

include(GoogleTest)
gtest_discover_tests(dirty_read_test)
