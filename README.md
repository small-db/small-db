# small-db

[![build](https://github.com/small-db/small-db/actions/workflows/ci.yml/badge.svg)](https://github.com/small-db/small-db/actions/workflows/ci.yml)

## Deveopment

### Essential Commands & Environment

#### Operating System

- Ubuntu 24.04 LTS (or newer version)

#### Build From Source

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

#### Run Tests

```bash
# run integration tests
./scripts/test/test.sh
```

#### Start Server

```shell
./build/debug/src/server/server --sql-port 5001 --grpc-port 50001 --data-dir /tmp/us --region us --join ""
```

### Advanced Tools

#### Print Physical Storage Layout

```bash
# print the underlying key-value pairs of a table
./build/debug/src/rocks/rocks_scan

# output example:
# [2026-02-16 10:43:21.142] [info] [rocks_scan.cc:115] scan data dir: ./data
# [2026-02-16 10:43:21.142] [info] [rocks_scan.cc:115] scan data dir: ./data/us
#         Key: /default_schema.users/2/balance, Value: 1941
#         Key: /default_schema.users/2/country, Value: USA
#         Key: /default_schema.users/2/id, Value: 2
#         Key: /default_schema.users/2/name, Value: Bob
```

#### Jepsen Test

```bash
# run jepsen test
./scripts/test/jepsen-test.py

# ssh into the vm
ssh -i ~/.vagrant.d/insecure_private_key vagrant@asia

# connect to database via psql client (inside the vm)
psql --host=localhost --port=5001
```

- Don't use `libvirt` as provider, use `virtualbox` instead, [vagrant-libvirt](https://github.com/vagrant-libvirt/vagrant-libvirt) is not well maintained.

#### C/C++ Memory Sefety

## The Book

### Local Writing

```bash
cd small-db-book
mdbook serve --hostname 0.0.0.0
```
