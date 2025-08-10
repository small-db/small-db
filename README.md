# small-db

[![build](https://github.com/small-db/small-db/actions/workflows/ci.yml/badge.svg)](https://github.com/small-db/small-db/actions/workflows/ci.yml)

## Development

### Environment

- Ubuntu 24.04 LTS (or newer version)
- CMake 3.21.3 (or newer version)

### Build From Source

```bash
# clone
git clone https://github.com/small-db/small-db.git

# install dependencies
./scripts/build/install-deps.sh

# build
./scripts/build/build.sh
```

### Run Tests

```bash
# run all tests
./scripts/test/test.sh
```

### Start Server

```shell
# TODO: this is broken, need to fix it
./build/src/server/server --port=5432
```

### Code Style

- [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html)

## Partitioning

- <https://www.cockroachlabs.com/docs/stable/partitioning>
- <https://www.postgresql.org/docs/current/ddl-partitioning.html>
- <https://rasiksuhail.medium.com/guide-to-postgresql-table-partitioning-c0814b0fbd9b>

> A database may only be opened by one process at a time. - <https://github.com/facebook/rocksdb/wiki/basic-operations#concurrency>

## TODO - CI

- valgrind
- sanitizer (address, memory, undefined)
