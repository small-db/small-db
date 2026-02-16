# Build System Details

- CMake 3.28+ with Ninja generator, clang-18 compiler
- Build preset `debug` outputs to `build/debug/`
- Proto code generation uses `small_proto_target()` defined in `cmake/recipes/external/grpc.cmake`
- Libraries follow `small::module` naming convention (e.g., `small::server`, `small::rocks`)
- Third-party deps fetched via CMake's FetchContent; gRPC installed to `cmake/libs_install/`
- Generated proto headers go to `CMAKE_BINARY_DIR` (build/debug/)
