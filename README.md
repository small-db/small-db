# small-db

[![build](https://github.com/small-db/small-db/actions/workflows/ci.yml/badge.svg)](https://github.com/small-db/small-db/actions/workflows/ci.yml)

## Development

### Operating System

- Ubuntu 24.04 LTS (or newer version)

### Build From Source

```bash
# clone
git clone https://github.com/small-db/small-db.git
cd small-db

# install dependencies
./scripts/setup/install-deps.sh

# (optional) check environment
uv run ./scripts/setup/check-env.py

# build
./scripts/setup/build.sh
```

### Run Tests

```bash
# run integration tests
./scripts/test/test.sh
```

### Start Server

```shell
./build/debug/src/server/server --sql-port 5001 --grpc-port 50001 --data-dir /tmp/us --region us --join ""
```

## Jepsen Test

```bash
# run jepsen test
./scripts/test/jepsen-test.py

# ssh into the test machine
ssh -i ~/.vagrant.d/insecure_private_key vagrant@asia

# connect to database via psql client
TODO
```

- Don't use `libvirt` as provider, use `virtualbox` instead, [vagrant-libvirt](https://github.com/vagrant-libvirt/vagrant-libvirt) is not well maintained.

## C/C++ Memory Sefety

## Book Writing

### Local Writing

```bash
cd small-db-book
mdbook serve --hostname 0.0.0.0
```
