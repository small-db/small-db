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
./build/debug/src/server/server --sql-port 5001 --grpc-port 50001 --data-dir /tmp/us --region us --join ""
```

## Jepsen Test

```bash
cd small-db-jepsen/vagrant

# start virtual machines
vagrant up

# (optional) verify status by ssh (user: vagrant, password: vagrant)
ssh vagrant@asia

# update hosts file
echo "192.168.56.110 asia
192.168.56.120 europe
192.168.56.130 america" | sudo tee -a /etc/hosts

vagrant upload /lib/x86_64-linux-gnu/libatomic.so.1.2.0 /tmp/lib/libatomic.so.1 asia
vagrant upload /lib/x86_64-linux-gnu/libpqxx-7.8.so /tmp/lib/libpqxx-7.8.so asia
vagrant upload /lib/x86_64-linux-gnu/libLLVM-18.so.1 /tmp/lib/libLLVM-18.so.18.1 asia
vagrant upload /lib/x86_64-linux-gnu/libpq.so.5.16 /tmp/lib/libpq.so.5 asia
```

```bash
cd small-db-jepsen

# run jepsen test
lein run test --node=asia --username=vagrant --password=vagrant
lein run test --node=asia --username=vagrant --password=vagrant --ssh-private-key=/home/xiaochen/code/small-db/small-db-jepsen/vagrant/.vagrant/machines/asia/virtualbox/private_key
lein run test --node=asia --node=europe --node=america --username=vagrant --password=vagrant
```

- If see error: `VirtualBox can't enable the AMD-V extension. Please disable the KVM kernel extension, recompile your kernel and reboot (VERR_SVM_IN_USE)`, paste it to ChatGPT and fix it.
- Don't use libvirt as provider, [vagrant-libvirt](https://github.com/vagrant-libvirt/vagrant-libvirt) is not well maintained.

## Book Writing

### Local Writing

```bash
cd small-db-book
mdbook serve --hostname 0.0.0.0
```