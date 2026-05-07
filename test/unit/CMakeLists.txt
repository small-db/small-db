function(small_unit_test name)
    add_executable(${name} ${name}.cc)
    target_link_libraries(
        ${name}
        PRIVATE
        small::catalog
        small::closedts
        small::execution
        small::gossip
        small::rocks
        small::server_info
        small::txn
        small::txn_handle
        GTest::gtest_main
        spdlog::spdlog
        absl::status
        absl::statusor
        absl::strings
        ssl
        crypto
    )
    target_compile_definitions(${name} PRIVATE GTEST_HAS_CXXABI_H_=0)
    gtest_discover_tests(${name})
endfunction()

include(GoogleTest)

small_unit_test(dirty_read_test)
small_unit_test(intent_promote_test)
small_unit_test(closed_ts_test)
