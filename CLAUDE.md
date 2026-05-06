# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working Style

**When a bug or test failure shows up, discuss before patching.** Don't jump straight to writing a fix. Explain what's happening, propose candidate fixes with trade-offs, and wait for direction. The cost of a paused conversation is small; the cost of committing to the wrong fix is rewriting it. Even when the cause looks obvious, surface the diagnosis first so the user can redirect or confirm.

**Gather concrete evidence before discussion or fix.** When something fails, do not stop at hypotheses. Read the relevant logs, scan the actual on-disk state, trace the failing operation in the recorded history -- whatever it takes to ground the discussion in observed facts rather than guesses. Two hypotheses with the same client-visible symptom usually have different signatures in the storage layer or the logs; the right next step is to look, not to argue. Bring the evidence first, then we discuss. "I think it's X" is not an answer; "I scanned the VMs and Bob's version_ts is Y while Alice's is Z, so it's X" is.

**Implement only the core idea of each book post.** When implementing what a book chapter / page describes, code only the central mechanism the page is teaching. Do not add safeguards, refinements, or non-trivial extensions on your own initiative -- even when the page itself mentions them as caveats or "subtleties." Non-trivial logic (bump rules, retry loops, phase-1 prepares, fallback paths, defensive checks beyond the bare requirement) only goes in when the user explicitly asks for it. If you think an extension is needed, surface it as a question, not a commit.

## Project Overview

small-db is a distributed SQL database written in C++20. It supports PostgreSQL wire protocol, LIST-based partitioning across regions, gossip-based replication, and uses RocksDB for storage.

## Environment Check

Before building or running tests, verify toolchain, kernel modules, and versions with:

```bash
uv run ./scripts/setup/check-env.py
```

This is the authoritative source for required tools and their minimum versions (build, Jepsen, debugging, book). It also flags kernel-module conflicts (e.g. `kvm_amd` blocking VirtualBox). Run it first whenever something looks wrong with the environment.

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
- `rocks/` — RocksDB wrapper; keys are `/<schema.table>/<primary_key>/<timestamp>`
- `gossip/` — Gossip protocol for cross-region replication via gRPC
- `type/` — Data type system (INT64, STRING) with PG OID mapping
- `server_info/` — Server configuration (ports, region, data dir, join address)
- `id/` — Snowflake-like unique ID generator

**Storage format:** Each row version is stored as a single RocksDB key-value pair: `/<schema.table_name>/<primary_key>/<written_timestamp> → JSON`. This enables MVCC (multi-version concurrency control) and prefix scanning for row/table retrieval.

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

## Jepsen Testing

Jepsen tests verify distributed correctness by running the database across 3 Vagrant VMs (america, europe, asia) and checking invariants like balance conservation.

Jepsen framework source is at `/home/xiaochen/code/jepsen` (external to this repo).

**Prerequisites:** Vagrant, VirtualBox, hostctl, Leiningen (lein), and VirtualBox kernel modules loaded.

**Running:**
```bash
# 1. Verify environment — VMs running, /etc/hosts entries present, all tools installed
uv run ./scripts/setup/check-env.py

# 2. Build the server binary
./scripts/setup/build.sh

# 3. Run the Jepsen test directly via lein
cd small-db-jepsen && lein run test-all --node america --node europe --node asia --ssh-private-key ~/.vagrant.d/insecure_private_key --username vagrant
```

`check-env.py` verifies VM state and hostname resolution (america/europe/asia → the IPs in `small-db-jepsen/vagrant/nodes`); fix any ✗ before running the test. The `scripts/test/jepsen-test.py` wrapper exists for first-time setup (it runs `vagrant up` and `sudo hostctl add ...`), but day-to-day runs should use the `lein run` command above directly — `check-env.py` already covers what the wrapper would set up. The test copies the built binary from `build/debug/src/server/server` and its dynamic libraries into each VM.

**Available tests** (defined in `small-db-jepsen/src/small_db_jepsen/runner.clj`):
- `bank-test` — Transfers between accounts, checks total balance is conserved
- `query-test` — Runs system table queries on all nodes

**Web UI for results:** "Start the Jepsen server" means the test-results web UI:
```bash
cd small-db-jepsen && lein run serve
```
Listens on http://localhost:8080/ and browses `small-db-jepsen/store/`. This is `jepsen.cli/serve-cmd`, not a small-db server and not the test runner.

**Debugging failures:** Test results are stored in `small-db-jepsen/store/<test-name>/<timestamp>/`. Jepsen also maintains `small-db-jepsen/store/latest` (and `store/current`) as symlinks to the most recent run — reference those instead of computing the latest timestamped dir via `ls -td`. Files:
- `jepsen.log` — Full Jepsen framework log (test orchestration, assertions, checker results)
- `<node>/server.log` — Per-node small-db server log (america, europe, asia)
- `history.edn` / `history.txt` — Operation history
- `results.edn` — Checker output (pass/fail with details)

**VM details:** 3 nodes with private IPs (america=192.168.56.130, europe=192.168.56.120, asia=192.168.56.110). SSH: `ssh -i ~/.vagrant.d/insecure_private_key vagrant@<node>`. VMs managed from `small-db-jepsen/vagrant/`.

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

## Adding Claude permission rules

When adding entries to `.claude/settings.json`, avoid these two anti-patterns:

1. **System-wide modification commands.** Don't allowlist commands that change system state outside the project — package managers (`apt`, `pip install`, `flatpak`, `snap`, `dpkg`), kernel/module queries (`lsmod`), VM lifecycle (`vagrant up`), `/etc/hosts` mutators (`hostctl`), `ssh` to non-project hosts. Run those manually when needed; don't grant blanket permission.

2. **Rules with specific args (one-off literals).** Entries like

   ```
   Bash(python3 /tmp/drawio_to_svg.py path/to/specific.drawio path/to/specific.svg)
   ```

   are useless — they only match that exact command line and never fire again. Either generalize to a useful prefix (`Bash(python3 -c:*)`, `Bash(./scripts/test/**:*)`) or leave the entry out and accept the single prompt.

Prefer prefix patterns (`Bash(grep:*)`) and path globs (`Bash(./build/debug/**:*)`) that cover a class of commands you'll run repeatedly.

Note: a compound command containing shell variable expansion (`$n`, `${var}`) will still prompt with "Contains simple_expansion" even when every sub-command is allowlisted. The check fires on the variable itself — the static allowlist can't verify what the expansion will resolve to. To skip the prompt, either inline the values (no variable) or move the loop into a script and allowlist the script's path.
