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
