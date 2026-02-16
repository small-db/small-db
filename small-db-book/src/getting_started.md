# Getting Started

## Operating System

Ubuntu 24.04 LTS (or newer).

## Build from Source

```bash
# clone
git clone https://github.com/small-db/small-db.git
cd small-db

# install dependencies
./scripts/setup/install-deps.sh

# (optional) check environment
uv run ./scripts/setup/check-env.py

# build (debug preset with clang-18 + Ninja)
./scripts/setup/build.sh

# or manually:
cmake --preset=debug && cmake --build ./build/debug
```

## Run the Server

```bash
./build/debug/src/server/server \
    --sql-port 5001 \
    --grpc-port 50001 \
    --data-dir /tmp/us \
    --region us \
    --join ""
```

## Run Tests

```bash
# run integration tests (starts 3 server instances, runs .sqltest files)
./scripts/test/test.sh
```

The test binary is at `./build/debug/test/integration_test/sql_test`. Tests fork 3 server processes (us/eu/asia regions on ports 5001-5003 and gRPC 50001-50003), then run SQL test cases from `test/integration_test/test.sqltest`.

## Lint

```bash
# cpplint with Google style
./scripts/format/lint.sh
```
