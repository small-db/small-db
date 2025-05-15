block()
  set(WITH_GFLAGS OFF CACHE BOOL "gflags")
  set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "rocksdb build shared")
  set(WITH_TESTS OFF CACHE BOOL "rocksdb without tests")
  set(WITH_BENCHMARK_TOOLS OFF CACHE BOOL "rocksdb without benchmarking")

  # due to a problem compiling on clang++ 18.1.3 we need to disable deprecated
  # declaration errors
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations")
  set(FAIL_ON_WARNINGS YES CACHE BOOL "rocksdb warnings are ok")

  FetchContent_Declare(RocksDB
    GIT_REPOSITORY https://github.com/facebook/rocksdb.git
    GIT_TAG v9.10.0
    GIT_SHALLOW TRUE
  )
  FetchContent_MakeAvailable(RocksDB)
endblock()