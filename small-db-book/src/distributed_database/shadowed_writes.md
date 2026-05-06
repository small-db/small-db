# Shadowed Writes

## The Anomaly

**Behavior.** A transaction successfully commits a write, the new row version lands on disk, the transaction reports `:ok` to its client -- and yet a subsequent `SELECT` returns a value as if the transaction never happened. The new version is on disk; it just isn't the one MVCC declares "current." The application sees the cluster's invariants drift even though every individual transaction was correctly computed and successfully persisted.

**Root cause.** MVCC selects the visible version of a row by **lex-largest `version_ts`**. The system stamps each write with its coordinator's `commit_ts`, which is the coordinator's wall clock at the moment of commit. With multiple coordinators on different machines and even slightly skewed wall clocks, two concurrent commits to the same row can produce `commit_ts` values whose lex order does not match their commit order. The chronologically-later write, if it picked a smaller `commit_ts` than an earlier-committing one, lands at a smaller key than its predecessor and is invisible to all future MVCC reads. The earlier write's value is now "the current state" forever -- as if the later one never happened.

**Does this happen in single-server databases?** No. A single server has exactly one wall clock; every `commit_ts` it issues is monotonic, so chronologically-later commits always have larger `commit_ts` values, and lex order on disk per row matches commit order automatically. The anomaly is unique to multi-coordinator MVCC systems where commits can be timestamped by different clocks. It does not appear in MySQL InnoDB or Postgres at any isolation level on a single node, because there's no second clock to disagree with.

**Typical solutions.** Three families:

- **Per-key monotonicity bump.** At write time, raise `version_ts` to `max(caller_ts, latest_version_ts_for_pk + 1)`. Guarantees lex order on disk per key matches commit order against that key, regardless of cross-coordinator clock skew. Cheap when an exclusive row lock is already in place (no two writers race the read-then-bump), and the bump is a few extra reads of the pk's chain. The simplest answer for a partitioned system where each row has exactly one owner and writes for that row necessarily serialize through the owner.
- **Causally consistent timestamps (Hybrid Logical Clocks).** Track a `(physical, logical)` pair per node; advance the logical counter on every event seen, so every node's timestamp is at least as large as any timestamp it has observed. CockroachDB, YugabyteDB, FoundationDB. Solves the problem at the timestamp source, so individual writes don't need per-key bumping -- but every gRPC, every read, and every write threads HLC values through to maintain causality across boundaries.
- **Bounded clock skew + commit-wait.** Bound inter-node clock uncertainty to some `ε` (Spanner's TrueTime: `~7ms` via GPS + atomic clocks). At commit, wait until the local clock has advanced past `commit_ts + ε`, so by the time the commit is observable elsewhere, every other node's clock has caught up. Spanner. Trivially enforces "external consistency" but requires hardware clocks for `ε` to be small enough that commit-wait isn't crippling.

A fourth lighter approach for systems that already pin writes to a leader: **route all writes for a key through a single leader** so exactly one clock ever stamps a given key. Spanner's range-leader model on top of TrueTime; CockroachDB's range-leader model on top of HLC. Sidesteps the problem at the cost of an extra hop to the leader for every write.

## The Problem (in this system)

The first error in a recent bank-test run was an `:ok :read` returning `{1: 1162, 3: 1439, 4: 2907, 5: 2498, 2: 1887}` -- summing to `9893`, short by `107`. Two committed transfers' credits were missing from the read:

- T4 (process 0, transfer `4→2 amount 93`): credit `+93` on Bob (account 2) is invisible.
- T1 (process 0, transfer `2→3 amount 14`): credit `+14` on Charlie (account 3) is invisible.

Walking T1 vs T2 from server logs (Charlie's case, simpler than Bob's; the same pattern explains both):

- T1 was coordinated on **america**. america's `commit_ts` for T1: `1778037811870`.
- T2 (process 1, transfer `3→1 amount 61`) was coordinated on **europe**, also touching Charlie. europe's `commit_ts` for T2: `1778037811873`. (europe's wall clock is 3 ms ahead of america's.)

Both UPDATEs dispatched to europe (Charlie's owner). T2's gRPC was local to europe and arrived first; T1's gRPC traversed the network from america and arrived second.

```
                              europe (Charlie's owner)
                           ┌──────────────────────────────────────┐
                           │  Charlie chain on disk (RocksDB)     │
                           │                                      │
  T2 (commit_ts=…873)      │  /users/3/<initial>      = 1500      │
   ──arrives first──►      │  acquire lock(Charlie)               │
                           │  ReadLatest → 1500                   │
                           │  compute 1500 − 61 = 1439            │
                           │  WriteRow at version_ts=…873  ──►    │
                           │  release lock                        │
                           │  /users/3/00…873         = 1439      │
                           │                                      │
  T1 (commit_ts=…870)      │  acquire lock(Charlie)               │
   ──arrives second──►     │  ReadLatest → 1439  ✓ correct        │
   (network delay from     │  compute 1439 + 14 = 1453            │
    america)               │  WriteRow at version_ts=…870  ──►    │
                           │  release lock                        │
                           │  /users/3/00…870         = 1453      │
                           │                                      │
                           │  Sorted lex on disk:                 │
                           │  /users/3/<initial>      = 1500      │
                           │  /users/3/00…870         = 1453  ◄ T1 (shadowed)
                           │  /users/3/00…873         = 1439  ◄ T2 (lex-largest)
                           └──────────────────────────────────────┘

  MVCC read at any snapshot_ts ≥ …873:
    scan in lex order, last visible wins  →  Charlie = 1439
  T1's correct value 1453 is on disk but never observable.
```

What every layer beneath MVCC delivered correctly:

- Both transactions held one `commit_ts` for both their writes (multi-statement-transactions plumbing works).
- The lock manager serialized T1 and T2's RMW on Charlie (no read-write interleave).
- T1's read-latest under the lock saw T2's just-committed value; T1's arithmetic produced the correct post-T1+T2 balance (`1453`).
- T1's `1453` was persisted to RocksDB.

What MVCC's read path delivered incorrectly: it returned T2's `1439`, declaring T2 the "current" state of Charlie because T2's `version_ts` is lex-larger, even though T1 committed *after* T2 against this row.

T4 vs T5 against Bob has the same shape (T4's america `commit_ts=…889`, T5's asia `commit_ts=…892`; T5 lands first, T4 lands second and correctly computes from T5's value but writes at the smaller key and is shadowed). The two shadowed writes together explain the `−107` deficit in the failing read.

## What "Fixing It" Has to Guarantee

The invariant: **for any row R, the lex order of `version_ts` values written to R must match the chronological order in which their commits took effect on R.** Equivalently: when a new commit writes R, its `version_ts` must be strictly greater than every previously-committed `version_ts` for R.

This is *per-row* monotonicity, not global. Two writes to *different* rows can land in any lex order without breaking MVCC's read semantics, because MVCC's "latest visible" rule applies independently to each row. The expensive thing -- a globally-monotonic timestamp source -- isn't necessary. The cheap thing -- per-row bumping at write time -- is.

The rest of this page surveys the options that enforce this invariant, and what each costs.

## The Solution Space

### 1. Per-Key Monotonic Bump

At write time, before persisting, scan the row's existing versions for the largest `version_ts`. Set the new version's `version_ts` to `max(caller_ts, latest + 1)`. Persist.

| | |
|---|---|
| **Implementation** | ~5 lines in `WriteRow`; one prefix scan per write |
| **Granularity** | Per-key, per-write |
| **What it fixes** | Shadowed writes from cross-coordinator clock skew |
| **Cross-node** | Not relevant here -- each row has one owner; the bump runs locally |
| **Concurrency cost** | One extra short scan per write; the row lock already in place ensures no two writers race the read-then-bump |
| **Client visible** | No aborts; commits succeed at the bumped `version_ts` |

This is the smallest fix that closes the anomaly. It does not require any cross-coordinator coordination, doesn't change the wire protocol, and preserves all existing semantics. Postgres does effectively this via its commit log: a tuple's `xmin` is monotonic per row because Postgres assigns XIDs from a single counter, and tuple visibility is checked against the commit log rather than raw timestamps.

The downside is that `version_ts` no longer always equals the coordinator's `commit_ts`; sometimes it's `latest + 1`. Anything that depends on the coordinator's intended timestamp matching the on-disk timestamp has to consult both. In a system that uses `version_ts` purely for MVCC (which is ours), this is a non-issue.

### 2. Hybrid Logical Clocks (HLC)

Replace wall-clock `commit_ts` with an HLC pair `(physical, logical)`. Every node tracks its current HLC. Every gRPC carries an HLC value; receivers advance their HLC to be at least as large. Every commit produces an HLC by `max(self.physical, observed.physical, self.HLC) + (logical+1 if tied)`.

| | |
|---|---|
| **Implementation** | New HLC module; thread HLC through every gRPC and every storage write/read; replace wall-clock `commit_ts` with HLC across the codebase |
| **Granularity** | Cluster-wide -- timestamps causally monotonic across all nodes |
| **What it fixes** | Shadowed writes; also subtle causality issues (a node observing X cannot subsequently issue a timestamp smaller than X) |
| **Cross-node** | Solves at the timestamp source; per-key bump becomes unnecessary |
| **Concurrency cost** | One read + atomic update of the local HLC per gRPC and per commit; cheap |
| **Client visible** | No aborts; commits proceed at the HLC value |

The principled answer for a multi-master MVCC system. CockroachDB and YugabyteDB use HLC for exactly this reason. The implementation cost is real -- HLCs ride on every wire, read, and write -- but the result is that timestamps across the cluster are causally monotonic, which is a stronger property than per-key bumping.

### 3. TrueTime + Commit-Wait

Bound inter-node clock skew to `ε`. At commit, after picking `commit_ts` from the local clock, wait until the local clock has advanced past `commit_ts + ε` before declaring the commit visible. By the time any other node observes the commit, every node's wall clock is at or past `commit_ts`, so no later commit on any node can produce a smaller `commit_ts`.

| | |
|---|---|
| **Implementation** | A clock service that returns `(earliest, latest)` bounds; a wait loop at COMMIT |
| **Granularity** | Cluster-wide external consistency (Spanner's "linearizability") |
| **What it fixes** | Shadowed writes; also gives external consistency for snapshot reads |
| **Cross-node** | Yes -- this is what TrueTime is for |
| **Concurrency cost** | Commit latency increases by `ε`; with hardware clocks (Spanner) `ε ≈ 7ms`; with NTP `ε ≈ 100ms` |
| **Client visible** | Commits are slower by `ε` |

Spanner's mechanism. Beautiful for read consistency, expensive without atomic-clock infrastructure. Out of proportion for closing only the shadowed-writes issue, but if the system later wants externally-consistent reads, this is the path.

### 4. Single Leader per Range

Designate one node per partition (range) as the writer. All writes for keys in that range go through the leader. The leader's clock is monotonic by construction, so `commit_ts` values for the same range are always in commit order.

| | |
|---|---|
| **Implementation** | A leader-election mechanism per range; all writes route to leader; cross-range writes need 2PC across leaders |
| **Granularity** | Per-range; cross-range still needs cross-coordinator coordination |
| **What it fixes** | Shadowed writes within a range |
| **Cross-node** | Single-leader pattern moves the cross-coord problem from "every key" to "writes that span multiple ranges"; the multi-range case still has the same shadowed-write issue and needs a separate mechanism |
| **Concurrency cost** | Extra hop for non-leader nodes; leader is a hot spot |
| **Client visible** | Leader fail-over latency on rare occasions |

Spanner's per-Paxos-group leader model and CockroachDB's per-Raft-range leader. In our system, every row already has exactly one partition owner (the LIST partitioning maps a row to one node), so this is *almost* what we have -- except that our writes broadcast to all peers and only the owner applies. Pinning the actual coordination to the owner would remove the need for the bump rule for single-key writes, at the cost of an extra hop on every UPDATE.

### 5. Phase-1 Prepare at COMMIT (Coordinator-Side Bump)

At `COMMIT`, before dispatching, the coordinator queries each peer that owns a row touched by the transaction for the latest `version_ts` for those rows. It picks `commit_ts = max(start_ts, now(), max(reported_latest) + 1)`. Then it dispatches with this `commit_ts`. Receivers write at exactly `commit_ts` -- no per-row bump needed because `commit_ts` is already big enough.

| | |
|---|---|
| **Implementation** | New gRPC for "report latest for these pks"; an extra round-trip at COMMIT |
| **Granularity** | Per-transaction; one network round-trip phase 1, one phase 2 |
| **What it fixes** | Shadowed writes |
| **Cross-node** | Yes -- explicitly distributed |
| **Concurrency cost** | Extra round-trip per commit |
| **Client visible** | Latency adds one RTT to every commit |

This is the design the original `multi_statement_transactions.md` page sketched. It works but is more expensive than option 1 for the same correctness guarantee. The per-key bump achieves the same invariant with a local prefix scan instead of a network round-trip.

## Comparison

| Approach | Correctness | Code change | Latency cost | Cross-node? | Solves more than asked? |
|---|---|---|---|---|---|
| 1. Per-key bump | Yes | Tiny | One prefix scan per write | No (local) | No -- minimal scope |
| 2. HLC | Yes | Large | Negligible per op | Yes -- causal | Yes -- causal consistency |
| 3. TrueTime + commit-wait | Yes | Large | `ε` per commit | Yes -- external consistency | Yes -- externally consistent reads |
| 4. Single-leader-per-range | Yes (per range) | Large | Extra hop per write | Partial | Yes -- replication architecture |
| 5. Phase-1 prepare | Yes | Medium | One RTT per commit | Yes | No -- same scope as (1), more expensive |

Reading the matrix:

- **All five close the shadowed-writes anomaly.** The differences are how much else they buy and how much they cost.
- **Option 1 is the cheapest** -- it's a local fix on the partition owner, exploits the lock that's already there, and adds no network round-trips. The trade is conceptual: `version_ts` no longer equals the coordinator's `commit_ts`; on a row's chain, `version_ts` is "commit_ts unless commit_ts wasn't large enough."
- **Option 2 (HLC) buys causality** for free with the timestamp -- two writes from any node have a partial order that respects the cluster's causality graph. Worth it if the system grows to need read-your-writes or stronger consistency guarantees.
- **Option 3 (TrueTime) buys external consistency** -- the strongest standard guarantee. Out of scope without GPS/atomic-clock hardware.
- **Option 4 (single-leader-per-range)** is an architectural move that addresses many problems at once (replication, consensus, write conflicts), with shadowed writes as a side benefit.
- **Option 5 (phase-1)** does the same thing as option 1 but pays a network round-trip per commit. The only reason to prefer it is if local `LatestVersionTs` lookups can't be trusted (e.g., the coordinator's local DB lags the partition owner). In our broadcast-to-all-peers model, the local view is good enough -- making option 1 strictly better.

## What's Currently in the Code

A previous version of small-db's `WriteRow` implemented option (1), the per-key bump rule. It was removed when we tightened the implementation scope to "MVCC in storage, transaction `ts`, UPDATE reads latest" only. Without it, the bank test reliably triggers the failure documented above -- both `+` (money created when a credit's bumped sibling's debit gets shadowed) and `−` (money lost when a credit gets shadowed) outcomes.

Re-adding option (1) is a ~5-line change in `WriteRow`: scan the pk for `latest_version_ts`, set `version_ts = max(caller_ts, latest + 1)` before `Put`. The lock manager already prevents the only race that would matter (two concurrent writers reading the same `latest`).

The other options on this page are larger architectural decisions and would each warrant their own implementation page.
