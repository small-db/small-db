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

**Trace (server-side, single millisecond resolution).**

| Time | Node    | Event |
|------|---------|-------|
| .393 | america | T0 BEGIN (writes `/_txn/<T0>` ACTIVE) |
| .393 | europe  | worker 1's `SELECT` arrives, dispatch begins |
| .394 | america | T0 UPDATE row 2 → WriteIntent `/users/2/INTENT(T0)` |
| **.396** | **europe**  | **worker 1's europe-loopback scan runs (snapshot_ts=.393)** |
| .404 | america | T0 UPDATE row 1 dispatched to all peers |
| .412 | america | T0 SetTxnStatus(COMMITTED, commit_ts=.393) |
| .412 | europe  | T0's UPDATE-row-1 dispatch arrives → WriteIntent `/users/1/INTENT(T0)` |
| **.445** | **america** | **worker 1's america cross-region scan runs (snapshot_ts=.393)** |
| .515 | client  | worker 1's `:ok :read` reported |

The two starred lines are the smoking gun. `dispatch=false` log lines (server.log query.cc:118) confirm both timestamps:

```
europe   .396  query: dispatch=false snapshot_ts=1778104657393   (= worker 1's BEGIN, .393)
america  .445  query: dispatch=false snapshot_ts=1778104657393
```

Same `SELECT`, same `snapshot_ts`. The two local scans ran 49 ms apart — the gap was wide enough for *all* of T0's transfer (UPDATE/dispatch/COMMIT) to land inside it.

**The race.** Atomic from worker 0's perspective; non-atomic from worker 1's perspective:

- **europe scan at .396 reads `/users/1/*`.** At that wall-clock moment the only key under that prefix is the seed (`/users/1/<seed_ts> = 1000`). T0's UPDATE-row-1 hasn't even been dispatched from america yet; T0's intent doesn't exist on europe until .412. Result: row 1 = seed = **1000**. T0's credit invisible.
- **america scan at .445 reads `/users/2/*`.** At that wall-clock moment T0's intent has been on disk since .394, and T0 flipped to COMMITTED at .412. Resolver lookup of `/_txn/<T0>` (loopback, micro-second) returns COMMITTED at commit_ts=.393. Result: row 2 = intent value = **1947**. T0's debit visible.

`{row 1 = 1000, row 2 = 1947}` — the credit leg is invisible while the debit leg is visible. Total 9947, lost 53.

**Why one scan ran so much earlier than the other.** Both scans are triggered by the same `SELECT` dispatched from europe at .393. Europe's loopback dispatch reaches its own query handler in ~3 ms (.396). The cross-region dispatch europe→america takes 52 ms in this run (.393→.445) — gRPC channel/connection setup, queueing, etc. T0's whole transfer (BEGIN→COMMIT, .393→.412 ≈ 19 ms) and the row-1 dispatch (.404→.412 ≈ 8 ms more) fit comfortably inside that 52 ms gap.

Worker 2 at index 5 (4 ms earlier `:ok` time but luckier scan timing — its europe scan ran at .444, after .412) saw both legs.

**The flip side: net-positive reads.** Run 2 (`20260506T170618`) skewed almost entirely toward `:wrong-total > 10000`. Same race, opposite asymmetry: in those reads the *credit* leg was visible while the *debit* leg's scan ran too early. Symmetric phenomenon, same fix space.

**This race is not from this commit.** Pre-`a61c31c`, the read-side resolver returned `(false, 0)` for ACTIVE just like it does now (see `default_resolver` in `src/txn/txn.cc`). The atomicity hole is in the read-dispatch itself — different nodes scan their local DB at different wall-clock times, with no shared cut. Confirmed by `git show 1a9635e:src/rocks/rocks.cc` — the read path's behavior on intent resolution is identical. The race has been latent since intents landed.

<p><img src="./intent_atomicity_race.svg" alt="Two local scans of one SELECT land 49 ms apart; T0's intents-and-COMMIT fit entirely inside the gap." style="max-width:100%;height:auto"/></p>

## Why It Hides on Single-Node and Integration Tests

- **Single-statement integration test** (`scripts/test/test.sh`): one client, sequential statements, COMMIT lands before any subsequent read. No concurrent reader to catch the window.
- **`dirty_read_test`** (unit): explicitly checks ACTIVE → invisible, then COMMIT → visible. Tests the two endpoints of the race; doesn't probe the *transition* under load.
- **`intent_promote_test`** (unit): plants states directly; doesn't drive multi-statement commits across nodes.
- **Bank test under Jepsen**: 3 workers issuing transfers and reads at high rate, intents land on different region owners → cross-region resolver RPCs vs. local resolver RPCs in the same read → the latency asymmetry exposes the window.

## Fix Options

The hole is in the read-dispatch: each node's local scan happens at a different wall-clock instant with no shared cut. `snapshot_ts` is propagated identically to every node, but `snapshot_ts` only filters out writes whose `version_ts > snapshot_ts` — it can't surface a write that hasn't physically landed on this node yet.

1. **Two-phase commit (real 2PC).** Coordinator's `COMMIT` does (a) PREPARE → wait for every intent-bearing peer to ack durably, then (b) DECISION → wait for every peer to ack the status flip. By the time the client sees `:ok`, every node that owns any of T's intents has them on disk *and* the COMMITTED status visible to its resolvers. Two extra RTTs per commit, but it closes both Mode 2 and the spurious aborts of Mode 1. The "right" answer.
2. **Read-path "wait for in-flight writes ≤ snapshot_ts".** Each node tracks a per-shard high-water mark of received-and-applied writes; a scan at `snapshot_ts` blocks until that high-water mark passes `snapshot_ts`. Cheap on the write side, costs reader latency. Doesn't help if the writer hasn't yet *issued* the write toward this node (it has to be in flight for "wait" to mean anything) — needs to be combined with something that delays COMMIT until issue.
3. **Two-phase reads (snapshot consensus).** Coordinator picks `snapshot_ts`, polls all peers ("have you applied everything ≤ S?"), then dispatches the actual scans. Reader-side analog of 2PC. One extra RTT per read.
4. **HLC + commit-wait.** The shadowed-writes chapter's options 2 and 3 — replace wall-clock `commit_ts` with HLC, or add Spanner-style commit-wait. Both make `commit_ts` carry "all causally-prior writes are visible by now" semantics, which combined with read-side wait gives external consistency. Big architectural moves; orthogonal to the per-row mechanics of intents.
5. **Reader block-on-ACTIVE / waiter queues (CockroachDB-style).** Useful but doesn't actually close *this* race — Mode 2 mostly fires when an intent isn't on the local node yet, not when it's there in ACTIVE state. Worth having for the cases where the intent *is* present, and for noise-reduction on Mode 1.

For the book: option 1 (2PC) is the cleanest follow-on chapter — it's a familiar, well-named protocol, it directly closes the failure traced above, and it sets up later distinctions (vs. Paxos commit, vs. coordinated commits, vs. the various workarounds for 2PC's blocking nature). Option 2 alone is interesting as a "minimal local fix" stepping stone if you want to teach that arc first.

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
