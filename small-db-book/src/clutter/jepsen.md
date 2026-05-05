# Jepsen Testing

Jepsen tests verify distributed correctness by running the database across 3 Vagrant VMs (america, europe, asia) and checking invariants like balance conservation.

## Install Requirements

1. Install required packages:

    ```bash
    sudo apt install openjdk-17-jdk libjna-java gnuplot graphviz
    ```

2. Install [Leiningen](https://leiningen.org/#install) manually.

3. Install Vagrant and VirtualBox. Do not use `libvirt` as the Vagrant provider -- [vagrant-libvirt](https://github.com/vagrant-libvirt/vagrant-libvirt) is not well maintained.

## Running

```bash
# 1. Build the server binary first
./scripts/setup/build.sh

# 2. Full setup (requires sudo for hostctl)
python scripts/test/jepsen-test.py

# 3. Day-to-day runs (VMs already up, no sudo needed)
cd small-db-jepsen && lein run test-all \
    --node america --node europe --node asia \
    --ssh-private-key ~/.vagrant.d/insecure_private_key \
    --username vagrant
```

The full script (`jepsen-test.py`) handles `vagrant up` -> hostctl DNS setup -> `lein run`, but requires sudo for the hostctl step. When VMs are already running, use the `lein run` command directly.

The test copies the built binary from `build/debug/src/server/server` and its dynamic libraries into each VM.

## Available Tests

| Test | Description |
|------|-------------|
| `bank-test` | Transfers between accounts, checks total balance is conserved |
| `query-test` | Runs system table queries on all nodes |

## VM Details

3 nodes with private IPs:

| Node | IP |
|------|----|
| america | 192.168.56.130 |
| europe | 192.168.56.120 |
| asia | 192.168.56.110 |

SSH into a VM:

```bash
ssh -i ~/.vagrant.d/insecure_private_key vagrant@<node>
```

Connect to the database inside a VM:

```bash
psql --host=localhost --port=5001
```

VMs are managed from `small-db-jepsen/vagrant/`.

## Debugging Failures

Test results are stored in `small-db-jepsen/store/<test-name>/<timestamp>/`:

| File | Description |
|------|-------------|
| `jepsen.log` | Full Jepsen framework log (test orchestration, assertions, checker results) |
| `<node>/server.log` | Per-node small-db server log |
| `history.edn` / `history.txt` | Operation history |
| `results.edn` | Checker output (pass/fail with details) |
