# Narrative Styles for an AI-Coding-Era Tutorial

This appendix collects design angles for writing a build-it-yourself tutorial
when the reader is *driving an AI agent* (Claude Code, Cursor, etc.) rather than
typing every line by hand. It is a brainstorm, not a prescription — pick the
angles that fit, drop the rest.

The reference point is Alex Chi's *mini-lsm* book. Its per-chapter loop is:

> Chapter overview → "you will implement…" bullets → `Task 1: modify src/foo.rs`
> → conceptual prose → optional `<details>` pseudocode spoiler → "Test Your
> Understanding" questions → bonus tasks.

That loop assumes the bottleneck is **typing the code**. With an AI agent in the
loop, typing is cheap. The bottleneck shifts to **specifying what you want**,
**reading what the agent produced**, and **pushing back when it's subtly
wrong**. The angles below adapt the mini-lsm loop to that reality.

All examples below are grounded in small-db's actual surface area: the memtable
freeze path, MVCC timestamp encoding, the RocksDB key format
`/<schema.table>/<pk>/<ts>`, gossip replication, LIST partitioning, and the
Jepsen `bank-test` invariant.

---

## 1. From "Tasks" to "Contracts"

**Definition.** A *task* says "go modify this file." A *contract* says "after
this unit, the system must hold these invariants, and the agent's diff is only
allowed to touch these files." The reader's job stops being "implement X" and
becomes "force the agent to deliver something that satisfies the contract."

**Why it matters.** When the agent writes code in seconds, the bottleneck is no
longer construction. It's *acceptance*. Telling the reader "modify
`src/mem_table.cpp`" is now near-trivial guidance; telling them what the result
must satisfy, and what it must not break, is the actual lesson.

**Concrete example — memtable freeze unit.**

Instead of:

> **Task 3:** In `src/mem_table.cpp`, implement `force_freeze_memtable` to
> freeze the active memtable and create a new one.

Write:

> **Contract: the freeze unit.**
>
> After this unit, the engine must satisfy:
>
> - **Atomicity of swap.** A reader holding the state lock either sees the old
>   active memtable or the new one — never both, never neither.
> - **No I/O under the state write lock.** The cost of holding the write lock
>   is bounded by an in-memory pointer swap, not by WAL `fsync`.
> - **Idempotent under racing freezes.** If two writers cross the freeze
>   threshold simultaneously, exactly one freeze happens.
>
> The agent may touch: `src/mem_table.cpp`, `src/mem_table.h`, `src/lsm_storage.cpp`.
> The agent may not touch: `src/rocks/*`, `src/gossip/*`, `src/pg_wire/*`.
>
> Acceptance test: `bazel test //test/integration_test:freeze_race_test`.

The contract version teaches what the freeze unit *is*. The task version
teaches where to type.

**Tradeoff.** Contracts are harder to write than tasks. You have to know the
invariants ahead of time; task-style tutorials can be vaguer.

---

## 2. Spec-First — and What "Property" Means

**Why this gets its own long section.** Several of the other angles below
depend on the reader knowing what a "property" is, so we'll define it
carefully.

### What is a property?

A **property** is a statement about behavior that should hold for *all* inputs
of some kind, not just one specific input. Compare:

- **Unit test (specific case).**
  ```cpp
  Put("a", "1");
  Put("a", "2");
  ASSERT_EQ(Get("a"), "2");
  ```
  This checks one scenario. If the engine is broken for keys longer than 64
  bytes, this test will not catch it.

- **Property (universal claim).**
  > For any key `K` and any sequence of writes ending with `Put(K, V_last)`,
  > `Get(K)` returns `V_last`.

  This claim covers infinitely many keys and write sequences. To check it, you
  use a **property-based testing** library (e.g., `rapidcheck` for C++,
  `proptest` for Rust, `Hypothesis` for Python). The library generates random
  inputs, runs the property, and if it fails, **shrinks** the input to the
  smallest counterexample.

Properties are the natural specification language for storage engines because
the behaviors that matter — durability, ordering, isolation, conservation —
are universally quantified. "After crash recovery, every committed write is
visible" is a property; you don't write it as one test, you write it as a
predicate over all (write history, crash point) pairs.

### Properties small-db actually has

The Jepsen `bank-test` already runs on a property: **total balance is
conserved across all concurrent transfers.** That's a single sentence covering
unlimited operation histories. Other small-db-shaped properties:

- **MVCC snapshot read.** For any read at timestamp `T`, the result equals the
  committed state of the database immediately after the last commit with
  `commit_ts ≤ T`.
- **Memtable flush is lossless.** For every key `K` present in the memtable
  before flush, `Get(K)` after flush returns the same value.
- **Compaction preserves user state.** For every `(K, V)` reachable from the
  user-facing API before compaction, `(K, V)` is reachable after.
- **Gossip convergence.** If writes stop, every node's view of the catalog
  converges to the same state within bounded time.
- **Key encoding round-trip.** For any `(schema, table, pk, ts)`,
  `decode(encode(schema, table, pk, ts))` returns the original tuple.

### Spec-first chapter structure

A spec-first unit *opens* with the property and a failing test, then drives the
agent until the property holds. The implementation prose disappears.

**Concrete example — MVCC snapshot read unit.**

```markdown
## Unit: Snapshot Reads

### Property

For any read at timestamp T_read, the value returned for key K equals the
value written by the commit C such that:

  - C wrote K, and
  - C.commit_ts is the largest commit_ts ≤ T_read among commits that wrote K.

### Failing test

  bazel test //test/integration_test:snapshot_read_property_test

Currently fails: read at T=5 returns the value committed at T=10. The
storage layer is not filtering by timestamp.

### Your job

Drive the agent to a passing test. The contract:
  - The fix lives in src/rocks/scan.cpp and src/execution/query.cpp.
  - The change must not affect non-MVCC code paths (queries with no
    explicit read timestamp).
  - Performance: a point read at a specific timestamp should still be a
    single RocksDB seek, not a full prefix scan.

### What the agent will probably get wrong

(See "Anti-spoilers" in §4 for the failure-mode list.)
```

Notice: the prose never tells the reader *how* to fix it. It tells them what
"fixed" means. That is the spec-first move.

**Tradeoff.** Writing rigorous properties is hard. A weak property ("the
function returns") gives a false sense of done. You have to invest in the
property as carefully as in the code, and you need property-test infrastructure
(`rapidcheck`, `proptest`, etc.) wired into the project.

---

## 3. Critique-Driven Units (Broken Transcripts)

**Definition.** A unit opens with a **deliberately broken AI transcript** — a
real-looking prompt and a plausible-but-wrong response — and the reader's job
is to spot what's wrong before reading the analysis.

**Why it matters.** Code review of AI output is a learnable skill, and most
people are bad at it because the output looks confident and idiomatic. The
best way to teach the skill is to practice it on examples where you already
know the answer.

**Concrete example — opening of the freeze-memtable unit.**

> ### Transcript: a naive freeze attempt
>
> **Prompt:** "Implement `force_freeze_memtable` in `src/mem_table.cpp` to freeze
> the current memtable and create a new one. Make sure it's thread-safe."
>
> **Agent's reply:**
> ```cpp
> void LsmStorage::ForceFreezeMemtable() {
>     std::unique_lock lock(state_mutex_);
>     auto wal = std::make_unique<Wal>(NextWalPath());  // creates a file
>     state_->immutable_memtables.push_back(state_->memtable);
>     state_->memtable = std::make_shared<MemTable>(std::move(wal));
> }
> ```
>
> **Before reading on:** find at least three things wrong with this code, given
> the memtable contract from §1.
>
> ---
>
> **Analysis.**
>
> 1. **WAL creation under the write lock.** `std::make_unique<Wal>(...)` opens a
>    file — potentially milliseconds of latency. The contract forbids I/O under
>    the state write lock. The agent took the prompt's "thread-safe" cue and
>    over-locked.
> 2. **No double-check after acquiring the lock.** Two writers can both decide
>    to freeze, both call this function, and we end up freezing an empty
>    memtable. The contract requires idempotence.
> 3. **`state_` is mutated in place, not Copy-on-Write.** Any reader holding a
>    `shared_ptr<LsmStorageState>` from before the freeze now observes a
>    half-mutated state. The atomicity invariant is broken.
>
> Now ask the agent to fix each of these in turn, and re-run
> `freeze_race_test`.

**Why this is pedagogically strong.** The reader experiences the *exact*
review move they need to make in their own work: hold the contract in mind,
read the diff, find the violations. The lesson is the critique, not the
construction.

**Tradeoff.** Transcripts age fast as model behavior changes. Plan to refresh
them periodically, or generalize the failure modes (§4) and let readers
generate their own transcripts.

---

## 4. Anti-Spoilers — Hide Failure Modes, Not Solutions

**Definition.** Mini-lsm hides pseudocode behind `<details>` tags so readers
must translate to working code themselves. With an AI agent, any pseudocode
spoiler is instantly executable, so the lever is gone. Replace it with hidden
**failure-mode warnings** — "what the agent will probably do wrong here, and
how to spot it."

**Concrete example — the `<details>` block in the snapshot read unit.**

````markdown
<details>
<summary>Failure modes the agent will likely produce</summary>

1. **Returns the most recent version, ignoring T_read.** The simplest "snapshot
   read" implementation is no filtering at all. Watch for this when the diff is
   too small — if no comparison against `read_ts` appears, the property cannot
   hold. Verify with: a write at T=10, a read at T=5 must see the pre-T=10 value.

2. **Filters in the executor, not the storage layer.** The agent may add a
   `if (row.commit_ts > read_ts) continue;` in `src/execution/query.cpp`. This
   passes the property test but does a full prefix scan even for point reads,
   blowing the performance contract. Verify with: profile a point read; expect
   one RocksDB seek, not N.

3. **Off-by-one on the equality.** "Largest `commit_ts` ≤ `T_read`" is easy to
   write as `<` instead of `≤`. Verify with: write at T=5, read at T=5 — the
   write must be visible.

4. **Skips uncommitted writes by checking a flag, but doesn't honor commit
   ordering.** If the agent uses an `is_committed` boolean instead of comparing
   timestamps, the property fails for any history with concurrent commits.
   Verify with: two writers, interleaved commits, read between them.

</details>
````

**The shape of an anti-spoiler.** Each entry has three parts: *what the agent
does*, *how to notice from the diff alone*, *how to confirm with a test*. That
trio is the actual review craft you're trying to teach.

**Tradeoff.** Anti-spoilers feel less satisfying than "here's a hint." They're
less of a hint, more of armor. Some readers will miss the value because they
expected hand-holding.

---

## 5. The Three-Beat Rhythm: Delegate / Read / Push Back

**Definition.** Replace `Task 1, Task 2, Task 3…` with a recurring three-move
rhythm inside each unit:

- **Delegate.** What to ask the agent for. Sometimes a literal prompt
  template, more often a *spec* the reader hands over.
- **Read.** What to look for in the diff before accepting it.
- **Push back.** The objections the reader should be ready to raise, with the
  reasoning behind each.

**Why it matters.** Working with an AI agent is a loop, and naming the three
moves separately gives readers vocabulary for what they're doing. "I'm in the
*read* phase, I haven't pushed back yet" is a useful self-observation; "I'm on
Task 3" is not.

**Concrete example — gossip replication unit.**

```markdown
### Delegate

> "In `src/gossip/`, add a periodic anti-entropy push: every 500ms, each node
> picks a random peer from the membership list and sends its current catalog
> version. The peer responds with any catalog entries it has that the sender
> doesn't.
>
> Constraints:
> - Must not block the request-handling thread.
> - Must use the existing `GossipService` gRPC stub, not a new transport.
> - The 500ms cadence must be configurable via `ServerInfo`."

### Read

When the diff comes back, check:

- Is the periodic loop on a dedicated thread, or scheduled on the main I/O
  pool? (The latter will starve under load.)
- Is the random peer selection biased? `rand() % peers.size()` looks fine
  but produces nonuniform distributions for small `peers.size()` on some
  RNGs.
- Is there a fan-out cap? An untuned implementation will send to N-1 peers
  every tick; you want one.
- Does the catalog-version comparison use the gossip vector clock, or a
  simpler "highest seen" counter? The latter is incorrect under partitions.

### Push back

Likely objections to raise:

- "You introduced a `std::thread` directly. We use the existing
  `BackgroundExecutor` everywhere else — please move it there."
- "The 500ms is hardcoded. The constraint said configurable."
- "There's no jitter. Synchronized 500ms ticks across nodes will create a
  thundering herd at startup."

Each push-back should cite either the contract (§1) or a project convention
visible elsewhere in the codebase. Pushbacks not anchored that way are just
preferences and the agent will reasonably ignore them.
```

**Tradeoff.** Prompt templates date quickly. Treat them as *examples* of the
delegate move, not as canonical wording — readers should adapt them.

---

## 6. CLAUDE.md as a Build Artifact

**Definition.** Mini-lsm accumulates `src/`. Your tutorial accumulates
`CLAUDE.md`. Each unit contributes a fragment encoding that unit's invariants,
which the reader pastes into their project. Future units' agents inherit prior
units' constraints automatically.

**Why it matters.** The hardest thing about a multi-chapter project with an AI
is that the agent forgets context that's "obviously true" by chapter 12.
Codifying it in `CLAUDE.md` — which the harness loads on every turn — turns
"things the reader has internalized" into "things the agent will respect
without being reminded."

**Concrete example — fragment from the MVCC unit.**

````markdown
### CLAUDE.md fragment from this unit

Append the following to your project's `CLAUDE.md`:

```markdown
## Storage key format

All row data is stored in RocksDB with keys of the form:

    /<schema.table_name>/<primary_key>/<commit_ts>

where `commit_ts` is a 64-bit big-endian integer. Do not introduce alternate
encodings; downstream prefix scans depend on this exact layout.

## MVCC reads

Reads accept an optional `read_ts`. When set, the executor must return the
value with the largest `commit_ts ≤ read_ts`, filtered at the storage layer
(in `src/rocks/scan.cpp`), not in the executor. Filtering in the executor
passes correctness tests but breaks the point-read performance contract.

## MVCC writes

Writes carry a single monotonically-increasing `commit_ts` assigned at commit
time, not at statement time. Per-statement timestamps would violate snapshot
isolation under multi-statement transactions.
```
````

By the time the reader reaches the gossip unit, their `CLAUDE.md` already
encodes the storage format, MVCC timing rules, partition-routing invariants,
and the test-file conventions. The agent now produces gossip code that
respects those invariants without the reader re-stating them.

**Tradeoff.** Couples the tutorial to Claude Code's conventions. Cursor users,
Copilot users, etc. would need an equivalent file (`.cursorrules`, etc.). You
can mitigate by also publishing the fragments in a tool-agnostic
`PROJECT_INVARIANTS.md` that readers paste into whatever their tool reads.

---

## 7. "Test Your Understanding" → "Interrogate Your Codebase"

**Definition.** Mini-lsm's reflection questions are pure thought exercises:
"Why doesn't the memtable provide a delete API?" Yours can be **agent-mediated
investigations** — questions the reader answers by interrogating their own
codebase with the agent's help, where the agent's first answer is often
plausible and wrong.

**Why it matters.** The questions now serve two pedagogical jobs at once: they
check the reader's understanding *and* they exercise the reader's review
craft. A reader who accepts a wrong agent answer fails twice — they don't
understand the system, and they didn't catch the agent.

**Concrete example — end of the freeze unit.**

```markdown
### Interrogate Your Codebase

For each question below: ask Claude to answer it from your code, then verify
the answer by reading the code yourself or running the suggested check.

1. **What happens if `force_freeze_memtable` is called while a `Get` for an
   unflushed key is in flight?** Ask Claude to trace the locks and pointer
   swaps. Then add a `std::this_thread::sleep_for(100ms)` inside the freeze
   path between "create new memtable" and "swap state pointer," run the
   `freeze_race_test`, and check whether Claude's trace matches what you
   observe.

2. **Is the WAL fsync-ed before a write returns to the client?** Ask Claude
   to find the fsync call. Then grep the code yourself: `grep -rn 'fsync\|fdatasync\|sync_file_range' src/`.
   Compare. If Claude's answer cited a function that does not appear in
   `grep`, it hallucinated.

3. **What is the maximum number of immutable memtables we can accumulate
   before a flush?** Ask Claude. Then look in `src/server_info/` for the
   actual config value. If Claude gave you a number without citing the
   config, it guessed.
```

The trick is that *each* question has a verification step. The reader is not
just asking the agent — they're auditing the agent.

**Tradeoff.** Quality depends on whether the agent's wrong answer is
interestingly wrong. Often it is, but you need to spot-check periodically as
models improve.

---

## 8. The "Hands-Off" Unit (Repurposed Snack Time)

**Definition.** Mini-lsm's "snack time" is a lighter-weight chapter for
batch-write APIs, checksums, etc. Repurpose it as the unit where **the reader
does not drive at all** — they specify acceptance criteria, hand the whole
thing to the agent, and only review.

**Why it matters.** The delegation muscle is real and trainable. Most of the
tutorial teaches you to push back; this unit teaches you when *not* to. If
acceptance criteria are precise enough and the surface area is small enough,
intervention adds noise.

**Concrete example — checksums unit.**

```markdown
### Hands-Off Unit: Block-Level Checksums

This unit you do not write or correct any code. You write a spec, hand it to
the agent, and review only the final diff.

#### Spec

Every SST block written to disk must include a CRC32C checksum at the end of
the block. On read, the checksum is verified; a mismatch returns an error
that propagates to the caller as `Status::Corruption`.

- Format: `[block bytes] [4-byte little-endian CRC32C of block bytes]`.
- Compute using the existing `crc32c` library already linked in
  `MODULE.bazel`. Do not add a new dependency.
- Apply to all block types (data blocks, index blocks, filter blocks).
- Backwards compatibility is not required — there is no production data.

#### Acceptance

  bazel test //test/integration_test:checksum_test
  bazel test //test/integration_test:sql_test  (existing tests must still pass)

#### Your job

  1. Refine the spec above if anything is ambiguous.
  2. Hand it to the agent in one shot.
  3. Read the resulting diff once. If it satisfies the spec, accept it.
     Resist the urge to edit cosmetic things.
  4. Run both test targets.

If you find yourself wanting to redirect the agent mid-stream, stop and ask
yourself: is the redirect anchored in the spec, or in your aesthetic
preference? If the latter, accept the diff and move on.
```

**Tradeoff.** Some readers won't trust the agent enough to do this honestly.
That's actually the lesson — they should notice the urge to intervene.

---

## 9. Adversarial Bonuses

**Definition.** Mini-lsm bonuses gate work behind effort ("implement an
alternate memtable"). With AI, that gate is gone — bonuses are nearly free to
attempt. Repurpose bonuses as **adversarial extensions**: the reader's job is
to break their own implementation.

**Why it matters.** Anyone can ask an agent to implement compaction. Far fewer
people can construct a workload that exposes a bug in the compaction they
just shipped. The latter skill is rarer and more valuable.

**Concrete example — bonuses for the compaction unit.**

```markdown
### Adversarial Bonuses

1. **Find a write history where your compaction loses data.** Construct a
   sequence of `Put`/`Delete`/`flush`/`compact` calls under which the final
   `Get` returns the wrong value or no value. If you cannot construct one,
   ask the agent to construct one. (Hint: think about delete tombstones,
   timestamp ordering, and the watermark.)

2. **Find a write history where your compaction is correct but pessimal.**
   Construct a sequence under which compaction does the maximum possible
   work for the minimum reduction in read amplification. Estimate the
   write-amp factor.

3. **Race the watermark.** Construct an interleaving of (long-running read
   at T=R, compaction at T=C with R < C) under which the compaction
   garbage-collects a version the read still needs. If your implementation
   prevents this, document the mechanism. If it does not, fix it.

For each bonus you complete, append a regression test to
`test/integration_test/compaction_adversarial.sqltest`.
```

The bonuses are now generative ("produce a problem") rather than consumptive
("solve a given problem").

**Tradeoff.** Adversarial bonuses are open-ended. Less satisfying for readers
who like checklists. Pair with concrete examples (the parenthetical hints
above) to lower the floor.

---

## 10. Negative Space — Files the Agent May Not Touch

**Definition.** Mini-lsm tells you which files to modify. With an agent, the
more useful constraint is the *negative space* — which files are off-limits
for this unit, and what the maximum acceptable diff size is.

**Why it matters.** Agents drift. Asked to add a feature in `src/foo`, they
will helpfully refactor `src/bar` while they're there. That refactor is
usually low-quality (it lacks the contextual judgment a human would bring) and
it inflates the review surface. A touch budget makes drift visible.

**Concrete example — opening of every unit.**

```markdown
### Scope

  Files the agent may modify:
    - src/mvcc/timestamp.cpp
    - src/mvcc/timestamp.h
    - test/unit_test/timestamp_test.cpp

  Files the agent may not touch:
    - anything under src/rocks/, src/gossip/, src/pg_wire/
    - any *.proto file (interface changes are out of scope for this unit)
    - any BUILD or MODULE.bazel file

  Diff budget: ≤ 200 lines added, ≤ 50 lines removed. If the agent's first
  diff exceeds this, push back and ask for a smaller change before reviewing
  details.
```

**Why a budget specifically.** A 200-line cap forces the reader to notice when
the agent is over-engineering. If the spec is "add a 64-bit timestamp field
and a comparison helper," the diff should not be 800 lines. The budget makes
that visible without requiring the reader to estimate independently.

**Tradeoff.** Budgets occasionally bite when a unit genuinely needs more
lines. Treat them as defaults to be overridden with reason ("this unit's
budget is 600 because we're touching the proto definitions") rather than
hard rules.

---

## A Recommended Combination

If you want one coherent style instead of ten knobs, this combination works
well together and reinforces a single underlying message — *the reader's
job is to hold the agent to a specification*:

- **Each unit opens with a contract** (§1) and a **scope/negative-space
  block** (§10).
- **The contract is a property** in the §2 sense, with a **failing test** the
  reader can run from the start.
- **The unit body uses the Delegate / Read / Push Back rhythm** (§5) instead
  of `Task 1, 2, 3`.
- **Hidden `<details>` blocks contain failure modes** (§4), not pseudocode.
- **Each unit appends a `CLAUDE.md` fragment** (§6) so later units inherit
  earlier constraints.
- **End-of-unit questions are codebase interrogations** (§7), not pure
  thought experiments.
- **One unit per part is hands-off** (§8) to train the delegation muscle.
- **Bonuses are adversarial** (§9): "produce a workload that breaks your
  implementation."

The whole-course three-layer arc from mini-lsm — **build → harden → generalize
** — still works. What changes is the texture inside each unit: less typing,
more refereeing.

---

## What to Read Next

If you want to feel the difference on the page rather than describe it, the
next step is to draft one small-db unit in this combined style — for example,
"MVCC timestamp encoding" — and compare it side-by-side with how mini-lsm
would have written the same chapter. That comparison will surface the parts of
the style that work for you and the parts that don't.
