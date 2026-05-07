# Intent Atomicity Race (Notes)

> **Status:** scratch notes from the 2026-05-06 Jepsen run on commit `a61c31c`
> ("feat(txn): lazy intent promotion + ACTIVE-abort"). Not in `SUMMARY.md`.
> Promote / restructure into a proper chapter when ready.

## What Was Run

```bash
cd small-db-jepsen && lein run test-all \
    --node america --node europe --node asia \
    --ssh-private-key ~/.vagrant.d/insecure_private_key \
    --username vagrant
```

`lein run test-all` against `:bank` test (5 accounts, total `10000`, 60s time-limit, 100 ops cap, 3 workers). Exit 1, 1 failure. Log: `small-db-jepsen/store/bank-test/20260506T145627.472-0700/`.

## Failure Summary

```
:bank {:valid? false,
       :read-count 52,
       :error-count 15,
       :errors {:wrong-total {:count 15, :lowest 9928, :highest 10078, ...}}}
```

15 `:wrong-total` reads observed (totals ranging 9928 to 10078 vs. expected 10000). 15 transfers failed with `:fail :transfer ... active intent on .../X for txn_id=Y; retry`. The two counts coincidentally equal — they are independent failure modes.

## Mode 1: ACTIVE-Aborts (caused by this commit)

**Symptom.** Workers see errors like:

```
INTERNAL: failed to update into server america:50001:
active intent on default_schema.users/2 for txn_id=310535604194947072; retry
```

The runner's `try`/`catch` issues `ROLLBACK`, marks the op `:fail`. Jepsen treats `:fail` as definitively-not-applied, so these don't directly create wrong-total reads. They are operational noise, not corruption.

**Why it fires.** `latest_committed_version_ts` (in `src/txn/txn.cc`) now returns `AbortedError` on `ResolveIntentResponse::ACTIVE`, where the prior code logged a warning and continued (silently overwriting the in-flight intent — a real data-loss bug the abort closes). Under sustained concurrency, two writers regularly hit the same row in the window between writer A releasing its row lock and writer A's coordinator flipping the txn record to `COMMITTED`. The chapter `write_intents.md` calls this out: "the row lock plus single-owner-per-row partitioning means any intent on R belongs to a transaction that has released its lock, and a transaction that has released its lock has flipped its status. The case still has to be handled, because a coordinator that crashed between writing intents and flipping its txn record leaves a stale ACTIVE record behind." In practice that "still has to be handled" case fires routinely under three concurrent workers, not only after a coordinator crash.

**Verdict.** Correct behavior given the chosen semantics (abort on ACTIVE). Annoying frequency. Two ways to make it less noisy without re-introducing the data-loss bug:

- **Wait + retry on ACTIVE.** A short bounded wait with a re-check would absorb the common case (writer A is about to flip COMMITTED) without bouncing the client. Stops short of full waiter queues.
- **Hold the row lock until COMMIT/ROLLBACK** (as `write_intents.md` originally described under TxnState's `held_locks`, but which the current code does *not* do — the lock is released at the end of `update()`). Closes the same-row window entirely. Requires either threading the lock through `Txn` state on the row's owner across gRPC calls, or having the coordinator drive an explicit BEGIN/COMMIT RPC against each owner. Substantial protocol change.

## Mode 2: `:wrong-total` Reads (NOT caused by this commit)

**Symptom.** A `:read` returns balances summing to ≠ 10000. First failure was index 7:

```
{:index 7, :time 20292595245, :type :ok, :process 1, :f :read,
 :value {1 1000, 3 1500, 2 1947, 4 3000, 5 2500}}    ; sum 9947
```

Bracketing context (from `history.txt`):

```
{:index 0, :time 20166100743, :type :invoke, :process 0, :f :transfer, :value {:from 2, :to 1, :amount 53}}
{:index 1, :time 20168109420, :type :invoke, :process 1, :f :read}
{:index 3, :time 20190463869, :type :ok,     :process 0, :f :transfer, :value {:from 2, :to 1, :amount 53}}
{:index 5, :time 20288774321, :type :ok,     :process 2, :f :read,
   :value {4 3000, 5 2500, 1 1053, 3 1500, 2 1947}}   ; sum 10000 — full transfer visible
{:index 7, :time 20292595245, :type :ok,     :process 1, :f :read,
   :value {1 1000, 3 1500, 2 1947, 4 3000, 5 2500}}   ; sum  9947 — only debit visible
```

Worker 0's transfer `2→1, 53` succeeded; worker 2 (asia) saw both legs (`{1=1053, 2=1947}`); 4 ms later, worker 1 (europe) saw only the debit (`{1=1000, 2=1947}`).

**Trace (server-side).**

```
14:57:37.393  america  /* op=0 */ BEGIN;                              (worker 0's BEGIN)
14:57:37.393  europe   /* op=1 */ SELECT id, balance FROM users;      (worker 1's read starts)
14:57:37.394  america  /* op=0 */ UPDATE ... SET balance = balance - 53 WHERE id = 2;
14:57:37.404  america  /* op=0 */ UPDATE ... SET balance = balance + 53 WHERE id = 1;
14:57:37.412  america  WriteTxnRecord: /_txn/310535603693486080 status=1 commit_ts=1778104657393
                       (worker 0's COMMIT — txn record flipped to COMMITTED)
14:57:37.412  europe   WriteIntent:    /default_schema.users/1/INTENT
                       txn_id=310535603693486080 coordinator=america:50001
                       (row 1's intent appears on europe)
... worker 1's SELECT response returns at 14:57:37.515 ...
```

Notice the row-1 intent on europe was written at **the same millisecond** as worker 0's COMMIT on america. Worker 1's SELECT was already running on europe (started at 14:57:37.393).

**The race.** Worker 1's SELECT on europe fans out to all three nodes via gRPC. Each node's `query` handler runs `ReadTableWithResolver` on its own RocksDB:

| Row | Owner | Resolver call shape | Likely outcome at this moment |
|---|---|---|---|
| Row 2 (`{2=1947}`) | america | `america`'s local resolver issues a *loopback* gRPC to its own `TxnService` for `txn_id=…486080` | Loopback is fast → the local lookup of `/_txn/<id>` hits *after* `14:57:37.412` → **COMMITTED** → intent visible at commit_ts (1947 surfaces) |
| Row 1 (`{1=1000}`) | europe  | `europe`'s local resolver issues a *cross-region* gRPC to `america:50001` for the same `txn_id` | Network RTT ≥ a few ms; the request races worker 0's COMMIT-flip. If the lookup observes the txn record before `SetTxnStatus`, response is **ACTIVE** → resolver returns `(false, 0)` → intent skipped → seed (1000) surfaces |

Worker 1's read returns `{2=1947, 1=1000}` — atomicity violated. Different physical resolver-call paths to the same logical txn-record, with the COMMIT flip landing inside the latency gap between the two paths.

Worker 2 (4 ms earlier in `:invoke` time but reaching `:ok` at the same instant) happens to land on the lucky side of both races, sees both legs.

**This race is not from this commit.** Pre-`a61c31c`, the read-side resolver returned `(false, 0)` for ACTIVE just like it does now (see `default_resolver` in `src/txn/txn.cc`). Half-promote / full-promote run only on resolved-COMMITTED — they cannot influence a read whose resolver got back ACTIVE. Confirmed by `git show 1a9635e:src/rocks/rocks.cc` — the `if (!pair.first) continue;` skip path is identical. The race has been latent since intents landed.

## Why It Hides on Single-Node and Integration Tests

- **Single-statement integration test** (`scripts/test/test.sh`): one client, sequential statements, COMMIT lands before any subsequent read. No concurrent reader to catch the window.
- **`dirty_read_test`** (unit): explicitly checks ACTIVE → invisible, then COMMIT → visible. Tests the two endpoints of the race; doesn't probe the *transition* under load.
- **`intent_promote_test`** (unit): plants states directly; doesn't drive multi-statement commits across nodes.
- **Bank test under Jepsen**: 3 workers issuing transfers and reads at high rate, intents land on different region owners → cross-region resolver RPCs vs. local resolver RPCs in the same read → the latency asymmetry exposes the window.

## Fix Options

Listed against `write_intents.md`'s "Doesn't" bullets at the bottom — these are the deferred items now made concrete:

1. **Block-on-ACTIVE on the read path.** When a reader's resolver gets `ACTIVE`, wait briefly (e.g. retry the resolve a small number of times with a few-ms backoff) before deciding "skip." Crude but local fix; absorbs the common "writer is about to commit" case. Doesn't help ABORTED→stays-not-skipped or genuinely-stuck transactions.
2. **Wait-for graphs / push-the-writer.** Production design (CockroachDB-style). A reader that hits ACTIVE either waits on the writer's commit, or pushes the writer's `commit_ts` past the reader's snapshot so the reader can ignore the intent without losing data. New chapter material.
3. **Coordinator-side commit fence.** Before the coordinator returns `:ok` for `COMMIT`, propagate the new status to every node that owns one of this txn's intents (or wait long enough that those nodes' resolvers will observe it on next RPC). Eliminates the race at the source. Cost: COMMIT latency ≥ longest path RTT × number of intent-bearing nodes.
4. **Hold lock until COMMIT/ROLLBACK** (already discussed in Mode 1). Doesn't directly fix Mode 2 — a reader doesn't take row locks — but reduces the window during which intents linger in mixed states.

For the book, option 2 is the right teaching arc: introduce the failure with this trace, then walk through "why the obvious wait isn't enough" and arrive at waiter queues / pushes as the production answer.

## Files / Pointers

- Test result dir: `small-db-jepsen/store/bank-test/20260506T145627.472-0700/` (also `store/latest`).
  - `jepsen.log` — the orchestration log; lines 280, 296, 298 (and 12 others) are the ACTIVE-aborts; full op trace.
  - `history.txt` / `history.edn` — the canonical op sequence; index 7 above came from there.
  - `results.edn` — the checker output reproduced at the top of this note.
  - `<node>/server.log` — per-node small-db logs (binary-ish; use `grep -a`).
- The smoking-gun timestamps from the section above:
  - `america/server.log` → `WriteTxnRecord: /_txn/310535603693486080 status=1 commit_ts=1778104657393` at 14:57:37.412.
  - `europe/server.log` → `WriteIntent: /default_schema.users/1/INTENT txn_id=310535603693486080 coordinator=america:50001` at 14:57:37.412.
- Code touched by this commit: `src/rocks/rocks.{h,cc}` (HalfPromoteIntent / FullPromoteIntent), `src/txn/txn.cc` (`latest_committed_version_ts` -> abort on ACTIVE), `small-db-book/src/distributed_database/write_intents.md` (Promotion subsection, ACTIVE bullet), `test/unit/intent_promote_test.cc` (two new unit tests).
- Pre-existing relevant files: `src/execution/update.cc` (writer path with the lock-release-at-end-of-update behavior), `src/lock/lock_manager.{h,cc}`, `small-db-jepsen/src/small_db_jepsen/runner.clj` (transfer = explicit `BEGIN; UPDATE; UPDATE; COMMIT`).

## Open Questions for the Writeup Pass

- Does the chapter want to introduce both modes together (one is operational, one is correctness), or split? Mode 2 alone makes the cleaner "next chapter" arc since it's the deeper problem.
- Is there an even shorter local mitigation that's worth landing alongside the chapter — e.g. coordinator's COMMIT does a fan-out write of `(/_txn/<id>, COMMITTED)` to every intent-bearing node before returning :ok? That converts every cross-region resolver RPC into a local one. Worth thinking through as a stepping stone before waiter queues.
- The "lock held until COMMIT" gap (Mode 1) is a separate doc-vs-code drift that should probably be either fixed or removed from the chapter; right now `write_intents.md` describes a model the code doesn't implement.
