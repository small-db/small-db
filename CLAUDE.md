# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

small-db is a distributed SQL database written in C++20. It supports PostgreSQL wire protocol, LIST-based partitioning across regions, gossip-based replication, and uses RocksDB for storage.

## Build Commands

```bash
# Install dependencies (first time only)
./scripts/setup/install-deps.sh

# Build (debug preset with clang-18 + Ninja)
./scripts/setup/build.sh
# Or manually:
cmake --preset=debug && cmake --build ./build/debug

# Run integration tests (starts 3 server instances, runs .sqltest files)
./scripts/test/test.sh

# Lint (cpplint with Google style)
./scripts/format/lint.sh

# Run the server
./build/debug/src/server/server --sql-port 5001 --grpc-port 50001 --data-dir /tmp/us --region us --join ""
```

The test binary is at `./build/debug/test/integration_test/sql_test`. Tests fork 3 server processes (us/eu/asia regions on ports 5001-5003 and gRPC 50001-50003), then run SQL test cases from `test/integration_test/test.sqltest`.

## Architecture

**Request flow:**
```
PostgreSQL client → pg_wire/ (wire protocol) → server/stmt_handler (routing)
  → semantics/ (SQL validation via libpg_query) → execution/ (query/insert/update)
  → schema/ + catalog/ (metadata) → rocks/ (RocksDB storage)
```

**Key modules under `src/`:**
- `server/` — Main entry point, TCP accept loop, statement handler dispatch
- `pg_wire/` — PostgreSQL wire protocol encoding/decoding
- `semantics/` — SQL semantic analysis and AST extraction (uses libpg_query)
- `execution/` — Query (SELECT), Insert, Update execution engines; uses Arrow for columnar results
- `schema/` — Table schema definitions and LIST partition management
- `catalog/` — Schema update coordination via gRPC
- `rocks/` — RocksDB wrapper; keys are `/<table>/<primary_key>/<column>`
- `gossip/` — Gossip protocol for cross-region replication via gRPC
- `type/` — Data type system (INT64, STRING) with PG OID mapping
- `server_info/` — Server configuration (ports, region, data dir, join address)
- `id/` — Snowflake-like unique ID generator

**Storage format:** Each row is stored as multiple RocksDB key-value pairs: `/<table_name>/<primary_key_hex>/<column_name> → value`. This enables prefix scanning for row/table retrieval and selective column reads.

**Inter-server communication:** gRPC for catalog updates, insert/update forwarding to partition owners, and gossip replication. Protobuf definitions live alongside their modules (e.g., `src/gossip/gossip.proto`).

## Build System Details

- CMake 3.28+ with Ninja generator, clang-18 compiler
- Build preset `debug` outputs to `build/debug/`
- Proto code generation uses `small_proto_target()` defined in `cmake/recipes/external/grpc.cmake`
- Libraries follow `small::module` naming convention (e.g., `small::server`, `small::rocks`)
- Third-party deps fetched via CMake's FetchContent; gRPC installed to `cmake/libs_install/`
- Generated proto headers go to `CMAKE_BINARY_DIR` (build/debug/)

## Code Style

- Google C++ style (enforced by cpplint and clang-format)
- 4-space indentation, C++20 standard
- clang-tidy enabled with 100+ checks (bugprone, cert, modernize, etc.)
- Config files: `.clang-format`, `.clang-tidy`, `CPPLINT.cfg`

## Test Format

Tests use `.sqltest` files with this format:
```
statement ok
<SQL that should succeed>

query <type_chars>
<SQL query>
----
<expected tabular output>
```
Type characters: `T` for text columns. The test framework validates column names, types, and row data.
