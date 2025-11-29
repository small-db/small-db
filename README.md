# small-db

[![build](https://github.com/small-db/small-db/actions/workflows/ci.yml/badge.svg)](https://github.com/small-db/small-db/actions/workflows/ci.yml)

## Development

### Environment

- Ubuntu 24.04 LTS (or newer version)
- CMake 3.21.3 (or newer version)

### Check Environment

```bash
./scripts/build/check-env.py
```

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
./build/debug/src/server/server --sql-port 5001 --grpc-port 50001 --data-dir /tmp/us --region us --join ""
```

## Jepsen Test

```bash
./scripts/test/jepsen-test.py
```

- Don't use `libvirt` as provider, [vagrant-libvirt](https://github.com/vagrant-libvirt/vagrant-libvirt) is not well maintained.

## Book Writing

### Local Writing

```bash
cd small-db-book
mdbook serve --hostname 0.0.0.0
```
