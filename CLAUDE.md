# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working Style

**When a bug or test failure shows up, discuss before patching.** Don't jump straight to writing a fix. Explain what's happening, propose candidate fixes with trade-offs, and wait for direction. The cost of a paused conversation is small; the cost of committing to the wrong fix is rewriting it. Even when the cause looks obvious, surface the diagnosis first so the user can redirect or confirm.

**Gather concrete evidence before discussion or fix.** When something fails, do not stop at hypotheses. Read the relevant logs, scan the actual on-disk state, trace the failing operation in the recorded history -- whatever it takes to ground the discussion in observed facts rather than guesses. Two hypotheses with the same client-visible symptom usually have different signatures in the storage layer or the logs; the right next step is to look, not to argue. Bring the evidence first, then we discuss. "I think it's X" is not an answer; "I scanned the VMs and Bob's version_ts is Y while Alice's is Z, so it's X" is.

**Implement only the core idea of each book post.** When implementing what a book chapter / page describes, code only the central mechanism the page is teaching. Do not add safeguards, refinements, or non-trivial extensions on your own initiative -- even when the page itself mentions them as caveats or "subtleties." Non-trivial logic (bump rules, retry loops, phase-1 prepares, fallback paths, defensive checks beyond the bare requirement) only goes in when the user explicitly asks for it. If you think an extension is needed, surface it as a question, not a commit.

## Project Overview

small-db is a distributed SQL database written in C++20. It supports PostgreSQL wire protocol, LIST-based partitioning across regions, gossip-based replication, and uses RocksDB for storage.

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

## Build & Environment

**Environment check.** Before building or running tests, verify toolchain, kernel modules, and versions:

```bash
uv run ./scripts/setup/check-env.py
```

This is the authoritative source for required tools and their minimum versions (build, Jepsen, debugging, book). It also flags kernel-module conflicts (e.g. `kvm_amd` blocking VirtualBox). Run it first whenever something looks wrong with the environment.

**Build, lint, run.**

```bash
# Install dependencies (first time only)
./scripts/setup/install-deps.sh

# Build (debug preset with clang-18 + Ninja)
./scripts/setup/build.sh

# Lint (cpplint + clang-format)
./scripts/format/lint.sh

# Run the server
./build/debug/src/server/server --sql-port 5001 --grpc-port 50001 --data-dir /tmp/us --region us --join ""
```

**Build system internals.**
- CMake 3.28+ with Ninja generator, clang-18 compiler.
- Build preset `debug` outputs to `build/debug/`.
- Proto code generation uses `small_proto_target()` defined in `cmake/recipes/external/grpc.cmake`.
- Libraries follow `small::module` naming convention (e.g., `small::server`, `small::rocks`).
- Third-party deps fetched via CMake's FetchContent; gRPC installed to `cmake/libs_install/`.
- Generated proto headers go to `CMAKE_BINARY_DIR` (`build/debug/`).

## Testing

**Integration & unit tests.**

```bash
./scripts/test/test.sh
```

The integration test binary is at `./build/debug/test/integration_test/sql_test`. It forks 3 server processes (us/eu/asia regions on ports 5001-5003 and gRPC 50001-50003), then runs SQL test cases from `test/integration_test/test.sqltest`.

**SQLTEST format.**

```
statement ok
<SQL that should succeed>

query <type_chars>
<SQL query>
----
<expected tabular output>
```

Type characters: `T` for text columns. The framework validates column names, types, and row data.

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

**Investigating a historical failure (after the bug is fixed):** A previously-failing run is enough to ground a postmortem even when the working tree no longer reproduces it. Combine:
- The run's `store/<test>/<timestamp>/` directory (logs, history, results) — captures the exact symptom and timeline.
- Server-log statements tagged `/* op=N */` — line them up with the matching op-index in `history.txt` to reconstruct what each transaction did.
- The code as it was at the time of the run — `git log --until="<timestamp from results.edn>"`, then `git show <commit>:<file>` for the code path that produced the failure. The current code may already have moved past the bug; the historical commit is what matters for explaining the trace.

A run can be referenced by timestamp directory directly (`small-db-jepsen/store/bank-test/20260506T131345.793-0700`); the same path is what the `lein run serve` web UI exposes under `/files/<test>/<timestamp>`.

**VM details:** 3 nodes with private IPs (america=192.168.56.130, europe=192.168.56.120, asia=192.168.56.110). SSH: `ssh -i ~/.vagrant.d/insecure_private_key vagrant@<node>`. VMs managed from `small-db-jepsen/vagrant/`.

## Code Style

**Tooling.** Google C++ style, 4-space indent, C++20. Enforced by clang-format, cpplint, and clang-tidy (100+ checks: bugprone, cert, modernize, …). Configs: `.clang-format`, `.clang-tidy`, `CPPLINT.cfg`.

**Include layout in `.cc` files.** Group includes into named sections separated by a blank line and a banner comment. Within each section, sort alphabetically. Sections, in order:

1. **c std** — C/POSIX system headers (`<unistd.h>`, `<sys/socket.h>`, `<arpa/inet.h>`, …)
2. **c++ std** — C++ standard library, including `<c*>` wrappers (`<cstdint>`, `<cerrno>`, `<string>`, `<vector>`)
3. **third-party libraries** — external deps (`spdlog/...`, `CLI/CLI.hpp`, `nlohmann/json.hpp`, `pg_query.h`, …). When a library contributes more than one include, prefix the group with a short sub-comment naming the library (e.g., `// spdlog`).
4. **small-db libraries** — in-tree headers (`src/...`)
5. **self header** — the matching `.h` for this `.cc`, last (omit for executables with no header)

Banner format:
```
// =====================================================================
// <section name>
// =====================================================================
```

Skip a section entirely if it has no entries. Canonical example: `src/server/server.cc`.

**Guiding principle.** Write for the caller's mental model. That single idea drives everything below; bend the rules when the situation genuinely warrants.

**Naming — describe what the caller sees, not the internal mechanism.**
- Functions: name what the caller gets, not how. `latest_committed` over `read_for_writer`; `WaitUntilSafeToRead` over `WaitForClosedTs`.
- Arguments: name what the value means at the call site. `snapshot_ts` over `min_ts`.
- Types: name what the data is, not who consumes it. `CommittedRow` over `WriterPreimage`. For a two-field bundle with no natural noun, return `std::pair` instead of inventing one.

**Comments — default to none.** A well-named identifier is its own documentation. Write one only when a careful reader would still miss something: a hidden constraint, a non-obvious failure mode, a subtle invariant.

When a comment is warranted, scope-specific shape:
- **Public API**: up to three short sentences (separated by blank lines) — what it does in the caller's vocabulary; what the caller can rely on (skip if obvious from the first); what `false`/`nullopt`/error means (skip if N/A). If the contract won't fit, tighten the signature before lengthening the prose.
- **Class**: one or two sentences for the role.
- **Field/member**: one short clause for what the name doesn't convey.

**Hard rules.**
- No change history in comments ("replaces X", "previously did Y") — that's the commit message's job.
- No enumeration of internal branches the public contract already covers — branch reasoning lives at the branch, not in the header.

**Cleanup anti-patterns** — delete or trim if any apply:
- *Restates the code.* `// Pause so wall clock advances` above `sleep_for(50ms)` — no.
- *Narrates the body.* Step-by-step prose mirroring each statement — no.
- *Names APIs the field's name already implies.* `last_advertised_ts_` does NOT need `// Updated by WaitUntilSafeToRead`; the name implies an advertising API, readers can grep. State only what the name does NOT convey (e.g., `// monotonically increasing`).
- *Uses internal jargon callers don't think in.* `// Returns the stored bound` — `stored bound` is the implementation, not the contract.
- *Documents the obvious.* `// Returns true on success` on `IsValid()` — no.
- *Enumerates callers or implementation paragraphs in class headers.* No bulleted caller list. No "Cleanup is lazy: …" paragraph.

**Logic cleanup.**
- *Duplicated scans/loops* → extract one helper, call from each.
- *Unused public APIs* → delete. No "for future use."
- *Test-only branches in production code* → delete.
- *Test-only APIs on production classes* — no `ClearForTest`, no `friend class FooTest`. Use a separate test binary if isolation requires it.
- *Half-finished implementations / TODO scaffolds* → finish or remove.
- *Sentinel constants with one user* → fold inline.
- *State that exists only to feed a comment* → delete the comment, then the state.

## Unit Test Style

Unit tests under `test/unit/` exercise internal C++ APIs but should read as if from a user's perspective. The test verifies *what behavior the database guarantees*, not *how the implementation achieves it*.

- **No internal jargon in names, comments, or assertion messages.** Don't mention intents, txn records, snapshots, MVCC versions, ACTIVE/COMMITTED status, push protocols, etc. A reader who only knows SQL semantics should understand what's being checked. "uncommitted write must not be visible" — yes. "intent must remain ACTIVE-filtered" — no.
- **Name variables by their role**, not by index. `writer`, `concurrent_reader`, `post_commit_reader` — yes. `tx_a`, `tx_b`, `t1`, `t2` — no. The role is what makes the test self-documenting.
- **Extract a helper for repeated check patterns.** When the same shape of action+assert appears more than once, lift it into a local lambda so the body reads like a sequence of facts. Example from `dirty_read_test.cc`:
   ```cpp
   auto expect_balance = [&](std::string_view want, std::string_view why) {
       small::txn::Txn t;
       auto r = t.QueryScalar("SELECT balance FROM " + unique_table_ +
                              " WHERE id = 1");
       ASSERT_TRUE(r.ok()) << r.status().ToString();
       EXPECT_EQ(r.value(), want) << why;
   };
   // ... after the writer's UPDATE but before its commit:
   expect_balance("100", "uncommitted write must not be visible");
   ASSERT_TRUE(writer.Commit().ok());
   expect_balance("200", "committed write must be visible");
   ```
- **Every EXPECT/ASSERT carries a "why" message** stating the invariant in user-perspective language. The message appears on test failure and is the first thing a maintainer reads — make it state the rule, not the literal expected value (gtest already prints actual vs. expected).
- **Keep the test body short.** Push environment setup into the fixture (`TxnTestFixture`), repeated checks into helpers. A single-behavior test should fit on one screen.
- **Test name is concise.** 1–4 words, matching peers in the file (`WaitForWriterCommit`, `ClosedTsMonotonic`). Long noun-phrase names (`RegisterCannotRegressBelowAdvertisedClosedTs`) read as a sentence and clutter test output.
- **Lead comment states the invariant in one or two sentences.** Not a step-by-step narration of the body. The invariant is the rule; the body demonstrates it. Yes: `// T_closed is monotonic: a writer registering after a snapshot ts has been advertised as safe must commit strictly above it.` No: `// Reader observes "safe at ts = advertised" against an empty registry. The fast path returns immediately because ComputedClosedTs() == +infinity. Then a writer arrives with a low lower_bound...`
- **Body is structured as numbered steps** (`// 1.`, `// 2.`, …) describing each phase. Step comments describe the phase, not the next line of code. Don't restate what the code already shows; if a step would just paraphrase the EXPECT message, drop the comment.
- **No inline argument-name comments.** `Register(777, 1, "")` — not `Register(/*txn_id=*/777, /*lower_bound=*/1, /*coordinator_addr=*/"")`. The IDE shows them; comments duplicate.
- **Use small literal values for test-author-controlled parameters.** When only the relative ordering matters, prefer `2` over `1'000'000'000'000`. Magnitude theatre adds noise without adding signal.
- **No test-only APIs on production classes.** Don't add methods like `ClearForTest()`, `ResetForTest()`, or `friend class FooTest` just to make a test work. APIs are designed for the architecture; if a member shouldn't be reachable by other modules it stays private, regardless of what tests need. When a test wants isolation the singleton can't provide, put it in its own test binary so the process boundary handles it (see `test/unit/closed_ts_register_test.cc`).

## Claude Permission Rules

When adding entries to `.claude/settings.json`, avoid these two anti-patterns:

1. **System-wide modification commands.** Don't allowlist commands that change system state outside the project — package managers (`apt`, `pip install`, `flatpak`, `snap`, `dpkg`), kernel/module queries (`lsmod`), VM lifecycle (`vagrant up`), `/etc/hosts` mutators (`hostctl`), `ssh` to non-project hosts. Run those manually when needed; don't grant blanket permission.

2. **Rules with specific args (one-off literals).** Entries like

   ```
   Bash(python3 /tmp/drawio_to_svg.py path/to/specific.drawio path/to/specific.svg)
   ```

   are useless — they only match that exact command line and never fire again. Either generalize to a useful prefix (`Bash(python3 -c:*)`, `Bash(./scripts/test/**:*)`) or leave the entry out and accept the single prompt.

Prefer prefix patterns (`Bash(grep:*)`) and path globs (`Bash(./build/debug/**:*)`) that cover a class of commands you'll run repeatedly.

Note: a compound command containing shell variable expansion (`$n`, `${var}`) will still prompt with "Contains simple_expansion" even when every sub-command is allowlisted. The check fires on the variable itself — the static allowlist can't verify what the expansion will resolve to. To skip the prompt, either inline the values (no variable) or move the loop into a script and allowlist the script's path.
