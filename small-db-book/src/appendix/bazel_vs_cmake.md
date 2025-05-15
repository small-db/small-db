# Bazel v.s. CMake

## Comparison

| Feature | Bazel | CMake |
| ------- | ----- | ----- |
| symbol information (source code location) | ❌ relative path, inconvenient for debugging | ✅ absolute path, convenient for debugging |
| import cmake c++ package | ⚠️ available via "rules_foreign_cc", usability is limited | ✅ natively supported |
| import make c++ package | ❌ available via "rules_foreign_cc", hard to use, relies on standard "make install" | ⚠️ available through "execute_process" |
| debug failed compile | ⚠️ inconvenient due to the use of "sandbox" | ✅ convenient due to native build |
| (vscode) intellisense of the build language | ❌ "Bazel" extension is misfunctional | ✅ "CMake Tools" extension is good |
| (vscode) debug the build system | ❌ | ✅ |
| generate "compile_commands.json" | ⚠️ supported by "hedron_compile_commands", requires extra setup | ✅ natively supported by option "CMAKE_EXPORT_COMPILE_COMMANDS" |
