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

## Book Writing

### Local Writing

```bash
cd small-db-book
mdbook serve --hostname 0.0.0.0
```

## Jepsen Test

```bash
cd small-db-jepsen

# start virtual machines
vagrant up

# update hosts file
echo "192.168.56.110 asia
192.168.56.120 europe
192.168.56.130 america" | sudo tee -a /etc/hosts

# (optional) verify status by ssh (user: vagrant, password: vagrant)
ssh vagrant@asia

# run jepsen test
lein run test --node=asia --username=vagrant --password=vagrant
```

- If see error: `VirtualBox can't enable the AMD-V extension. Please disable the KVM kernel extension, recompile your kernel and reboot (VERR_SVM_IN_USE)`, paste it to ChatGPT and fix it.
- Don't use libvirt as provider, [vagrant-libvirt](https://github.com/vagrant-libvirt/vagrant-libvirt) is not well maintained.
