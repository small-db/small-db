# Closed Timestamps

## The Anomaly

**Behavior.** A `SELECT` against a partitioned table returns a snapshot that includes one transaction's effect on some rows but not others. The application sees a state that no atomic moment ever held: a transfer's debit is visible while its credit is not, or vice versa. Total balance shifts. Invariants the transaction was supposed to preserve atomically appear broken from the reader's point of view, even though the transaction committed correctly from the writer's.

The shape is identical to single-node [Read Skew](./read_skew.md), but with a different underlying cause. Read skew on one node is "the writer commits between two reads"; this anomaly is "the reader's per-shard scans land on opposite sides of one writer's progression," within what looks like a single read.

**Root cause.** A multi-shard `SELECT` visits each shard independently. Even when the read coordinator hands every shard the same `snapshot_ts`, the per-shard scans run at different physical instants — milliseconds apart in normal operation, more under load. Each shard makes its visibility decision against its *local* state at *its* scan time. Nothing binds those independent decisions to a shared logical instant.

A concurrent writer's transaction is propagating intents to shards over time. Its `commit_ts` may be ≤ the reader's `snapshot_ts`, making the write nominally visible to the reader. But the per-shard scans observe the writer at different points in its lifecycle: one shard may have the intent already landed and the writer's status flipped to COMMITTED; another may still be empty because the writer's gRPC for that shard hasn't arrived yet. The reader's aggregated answer is the union of these inconsistent observations.

**Does this happen in single-server databases?** No. A single server has one storage substrate; an MVCC snapshot is a single cut applied uniformly across every row. The reader observes one consistent state because there's only one state to observe. The anomaly is fundamentally a property of *distribution* — it requires multiple physically-separate states being observed independently.

**Does this happen in real distributed databases?** Yes, every Spanner-lineage system has had to build infrastructure to close this race. CockroachDB and YugabyteDB use closed timestamps (called "closed timestamps" and "safe time" respectively). TiKV uses `resolved_ts`. Spanner uses TrueTime + commit-wait, which closes the race from the writer side instead of the reader side. Centralized-oracle systems (TiDB+PD, FoundationDB) sidestep it by serializing transaction timestamps through one node. Eventually-consistent systems (Cassandra, DynamoDB at default consistency) silently accept the anomaly. See [Closed Timestamps (mechanism survey)](./mechanism_closed_timestamps.md) for system-by-system detail.

**Typical solutions.**

- **Closed timestamps.** Each shard publishes a value below which the world is settled — all writes with `commit_ts ≤ T_closed` are durable on every replica of every shard. Readers wait for `T_closed ≥ snapshot_ts` on every shard before scanning. CockroachDB, YugabyteDB, TiKV. Reader-side cost; writer-side correctness comes from bounding `T_closed` below the in-flight write set. The natural fit when the system already has per-shard consensus and MVCC.
- **Synchronous prepare + commit-wait.** Writer's `COMMIT` blocks until every shard has acked the write durably *and* the local clock has provably passed `commit_ts`. Spanner via TrueTime; commit-wait is bounded by clock uncertainty `ε` (~7 ms with hardware clocks). Writer-side cost; readers don't wait. Requires a clock infrastructure most systems can't justify.
- **Centralized timestamp oracle.** All transaction timestamps come from a single replicated service (TiDB's PD, FoundationDB's proxy/cluster controller). Reads at version V are guaranteed consistent because the oracle holding V also serializes commits. Avoids the race by avoiding distributed timestamp issuance entirely. Simpler protocol; the oracle layer becomes a horizontal-scaling bottleneck unless itself sharded.

## The Problem (in this system)

Run `20260506T145627` of the bank test. Index 7 in the recorded history (`small-db-jepsen/store/bank-test/20260506T145627.472-0700/history.txt`):

```
{:index 0, :time 20166100743, :type :invoke, :process 0, :f :transfer, :value {:from 2, :to 1, :amount 53}}
{:index 1, :time 20168109420, :type :invoke, :process 1, :f :read, :value nil}
{:index 3, :time 20190463869, :type :ok,     :process 0, :f :transfer, :value {:from 2, :to 1, :amount 53}}
{:index 5, :time 20288774321, :type :ok,     :process 2, :f :read,
   :value {4 3000, 5 2500, 1 1053, 3 1500, 2 1947}}   ; sum 10000 — full transfer visible
{:index 7, :time 20292595245, :type :ok,     :process 1, :f :read,
   :value {1 1000, 3 1500, 2 1947, 4 3000, 5 2500}}   ; sum  9947 — credit leg invisible
```

Worker 0's transfer `2 → 1, 53` succeeded. Worker 2's read at index 5 saw both legs (`{1=1053, 2=1947}`, total 10000). Four milliseconds later, worker 1's read at index 7 saw only the debit (`{1=1000, 2=1947}`, total 9947 — short by 53).

Server-side timeline (drawn from `america/server.log` and `europe/server.log`, `grep -a` to bypass the binary marker):

| Wall clock | Node | Event |
|---|---|---|
| .393 | america | T0 BEGIN, txn record ACTIVE, `start_ts = .393`, `commit_ts = .393` |
| .393 | europe | worker 1 SELECT received, dispatch to all 3 nodes |
| .394 | america | T0 UPDATE row 2 → `WriteIntent /users/2/INTENT(T0)` |
| **.396** | **europe** | **worker 1's europe-loopback scan runs** with `snapshot_ts = .393` |
| .404 | america | T0 UPDATE row 1 dispatched |
| .412 | europe  | T0 UPDATE-row-1 dispatch arrives → `WriteIntent /users/1/INTENT(T0)` |
| .412 | america | T0 SetTxnStatus(COMMITTED, `commit_ts = .393`) |
| **.445** | **america** | **worker 1's america cross-region scan runs** with `snapshot_ts = .393` |
| .515 | client  | worker 1 returns `{1=1000, 2=1947, ...}` — total 9947 |

The `query: dispatch=false snapshot_ts=...` log line on each server confirms the per-shard scan times. Both bear `snapshot_ts = 1778104657393` (= `.393`), but their wall-clock execution times differ by 49 ms.

What every layer below MVCC delivered correctly:

- T0's transfer was atomic from the coordinator's perspective: BEGIN → UPDATE → UPDATE → COMMIT, all in one client connection.
- Both UPDATE dispatches completed and acked durable (the synchronous gRPC pattern).
- Worker 1's SELECT was issued at `snapshot_ts = .393` and propagated identically to all three nodes.
- Per-node scans correctly applied `version_ts ≤ snapshot_ts` filters.

What specifically went wrong: worker 1's europe scan at `.396` ran *before* T0's WriteIntent landed on europe (at `.412`). The reader's local view of row 1 at `.396` was just the seed; T0's intent didn't exist on europe yet. Meanwhile, worker 1's america scan at `.445` ran *after* T0's intent had been on disk since `.394` and after T0's status had flipped to COMMITTED at `.412`. The reader correctly resolved that intent and included T0's value at row 2.

The two per-shard scans observed T0 at incompatible points in its lifecycle. The aggregated result mixes "before T0" (row 1) with "after T0" (row 2). Same `snapshot_ts`, same comparison rule, different physical instants — which is enough.

<p><img src="./intent_atomicity_race.svg" alt="Two local scans of one SELECT land 49 ms apart; T0's intents-and-COMMIT fit entirely inside the gap." style="max-width:100%;height:auto"/></p>

The diagram is shared with [scratch notes on this race](./intent_atomicity_race.md), where the trace is reproduced more compactly.

## What "Fixing It" Has to Guarantee

**For every shard `S` a `SELECT` at snapshot `T` visits, the per-shard scan must observe `S` at a moment when no write with eventual `commit_ts ≤ T` is still in flight toward `S`.**

Equivalently: the reader's per-shard observations must all be drawn from the same logical instant, where "the same logical instant" means an instant past which no write at `commit_ts ≤ T` will ever appear on any shard the reader touches.

The invariant is *cross-shard* — single-shard correctness is already given by snapshot reads from [Read Skew](./read_skew.md). What's new is the binding: per-shard scans, made at different physical instants, must agree on what's settled at the reader's snapshot.

## The Solution Space

### 1. Single-leader per partition

Pin every write for a key to one node. That node's wall clock is the only clock that ever stamps the key, so per-key `commit_ts` values are monotonic by construction. The reader still scans multiple shards, but each shard's local state is fully serialized — no in-flight ambiguity within a shard, only across shards.

| | |
|---|---|
| **Implementation** | Already true in small-db: every key has exactly one partition owner under LIST partitioning. The fix is to additionally route the *coordinator* role to the partition owner — eliminating cross-coordinator clock skew. ~50 lines of dispatch routing. |
| **Granularity** | Per-key |
| **What it fixes** | Same-key write reordering (and the original [Shadowed Writes](./shadowed_writes.md) anomaly). Doesn't directly fix cross-shard read atomicity. |
| **Cross-node** | Partial — single-key consistency only. |
| **Concurrency cost** | Extra gRPC hop on every write; leader is a hot spot for popular keys. |
| **Client visible** | Slight latency increase per write; failover during leader changes. |

**Real-system adoption.** Spanner pins writes to the Paxos group's leader; CockroachDB pins them to the range's leaseholder. Both pair this with closed timestamps for the cross-shard half. Used everywhere a system has per-range consensus. The reason these systems pick it: leader-routing is a side benefit of having Raft/Paxos already, not an additional design choice — the leader exists for replication consistency, write routing comes for free.

### 2. Closed timestamps with per-node in-flight registry

Each node tracks the set of in-flight writers that have written intents to it, together with each writer's lower-bound `commit_ts` (its `start_ts` at registration time, or higher if bumped). The node computes `T_closed = min(in-flight writers' lower bounds) − 1`, advancing as writers commit or abort. Readers query each touched node's `T_closed` and wait until `T_closed ≥ snapshot_ts` before scanning. Writers set their final `commit_ts = max(running commit_ts, now() at COMMIT)` so that the writer's eventual commit_ts is strictly greater than any `T_closed` observed *before* the writer registered.

| | |
|---|---|
| **Implementation** | New `closedts` module: per-node registry, `T_closed` compute, gRPC for reader queries. Hooks in `WriteIntent` (register), `Txn::Commit` (deregister fan-out + `commit_ts` bump). ~700 lines plus the reader-side gate in `query.cc`. |
| **Granularity** | Per-shard (= per-row-owner node in our setup). |
| **What it fixes** | Cross-shard read atomicity (this anomaly). |
| **Cross-node** | Yes — explicitly distributed. |
| **Concurrency cost** | Reader latency proportional to the longest in-flight writer on each shard. Writers pay nothing extra at write time; one extra fan-out RPC at COMMIT. |
| **Client visible** | Reads can wait briefly for a settled snapshot. No client-side aborts. |

**Real-system adoption.** CockroachDB, YugabyteDB, TiKV. The mechanism is the dominant choice in modern distributed-SQL systems built on per-shard Raft. The reason: it lets reads run on any replica (follower reads) without requiring linearizable round-trips to the leaseholder, and it provides the consistent-cut primitive that change data capture, schema migrations, and incremental backups need. The reader-side latency cost is small in absolute terms because writers are typically short-lived; the throughput win from follower reads usually dominates.

### 3. Two-phase commit with synchronous PREPARE and bumped commit_ts

Writer's `COMMIT` runs as two phases. PREPARE: coordinator polls every owner of an intent, confirms each intent is durably present. DECISION: coordinator chooses `commit_ts = max(start_ts, now() at end of PREPARE)`, then writes COMMITTED into the txn record. Readers don't wait; instead, for any in-flight intent they encounter, they push the writer to either commit or abort.

| | |
|---|---|
| **Implementation** | Explicit PREPARE phase RPC; commit_ts assignment moves to end of PREPARE; reader-side push protocol on ACTIVE intents. ~400 lines plus the push protocol (~300 more). |
| **Granularity** | Per-transaction. |
| **What it fixes** | Cross-shard read atomicity (via post-PREPARE commit_ts) plus reader semantics for in-flight intents (via push). |
| **Cross-node** | Yes. |
| **Concurrency cost** | Two RTTs at COMMIT (PREPARE + DECISION). Reader push adds RTT to the writer's coordinator on every encountered ACTIVE intent. |
| **Client visible** | Slower commits; readers can have intents pushed to abort under contention. |

**Real-system adoption.** Spanner does this with TrueTime: `commit_ts ≥ TT.now().latest`, then commit-wait until `TT.now().earliest > commit_ts` ensures every node's clock has passed `commit_ts` before COMMIT returns. Pure 2PC without the clock-wait piece is rarely used in modern systems because closed timestamps achieve the same correctness with cheaper writes. Its appeal is when commit-wait is already needed for external consistency (Spanner's design constraint).

### 4. Hybrid Logical Clocks (HLC) with closed timestamps

Add HLC on top of solution 2 to handle physical clock skew across replicas. `T_closed` and `commit_ts` become `(physical, logical)` pairs; receiving an HLC from another node bumps the local HLC to be at least as large. With the logical counter handling skew, `margin` shrinks to zero and `T_closed` advances aggressively.

| | |
|---|---|
| **Implementation** | Replace wall-clock timestamps with HLC across the system. Threading through every write and resolver call. ~500 lines of refactor on top of solution 2. |
| **Granularity** | Cluster-wide. |
| **What it fixes** | Same as solution 2, plus correct behavior under bounded clock skew. |
| **Cross-node** | Yes. |
| **Concurrency cost** | One HLC bump per RPC; cheap. |
| **Client visible** | None directly. |

**Real-system adoption.** CockroachDB, YugabyteDB, TiKV all use HLC. The reason: NTP synchronization isn't tight enough (typically 1–100 ms drift in cloud environments) for closed-timestamp correctness without a logical layer. Spanner avoids this by using TrueTime hardware infrastructure to bound `δ` directly. The HLC approach is the cheaper "good enough" answer for systems that can't justify atomic clocks.

### 5. Centralized timestamp oracle

All transactions get their `start_ts` and `commit_ts` from a single replicated service. Reads at version V are guaranteed consistent because the oracle that issued V also serializes the commits ≤ V. No per-shard `T_closed` needed; the oracle's monotonic issuance is the consistency mechanism.

| | |
|---|---|
| **Implementation** | New oracle service backed by Raft; every commit and every read RPCs the oracle. Substantial protocol change. ~1000 lines plus the new service. |
| **Granularity** | Cluster-wide. |
| **What it fixes** | Cross-shard read atomicity, plus eliminates clock skew problems. |
| **Cross-node** | Yes. |
| **Concurrency cost** | Every transaction touches the oracle; oracle becomes a throughput bottleneck. |
| **Client visible** | Latency floor = RTT to oracle, even for fast reads. |

**Real-system adoption.** TiDB's PD, FoundationDB's proxies. The reason these systems pick it: a centralized oracle is conceptually the simplest possible answer to distributed timestamp issuance, and PD/proxies already have other roles (placement, conflict resolution) that the oracle responsibility piggybacks on. The cost of a centralized service is amortized across multiple jobs.

## Comparison

| Approach | Correctness | Code change | Concurrency cost | Aborts to client? | Granularity | Cross-node? |
|---|---|---|---|---|---|---|
| 1. Single-leader-per-partition | Same-key only | Small | Extra hop per write | No | Per-key | Partial |
| 2. Closed timestamps + registry | Cross-shard | Medium | Reader-side wait | No | Per-shard | Yes |
| 3. 2PC + push-the-writer | Cross-shard | Medium-large | Slower commits, push aborts | Yes (under contention) | Per-txn | Yes |
| 4. HLC + closed timestamps | Cross-shard, skew-tolerant | Large | Reader-side wait | No | Cluster-wide | Yes |
| 5. Centralized oracle | Cross-shard | Large | Oracle RTT per txn | No | Cluster-wide | Yes |

Reading the matrix:

- **Cost-axis split between writers and readers.** Solution 2 (closed timestamps) puts the cost on readers that have to wait for `T_closed`. Solution 3 (2PC + push) puts it on writers that pay the extra RTT and on readers that may have writes pushed to abort. The right choice depends on workload mix — read-heavy favors writer-side cost, write-heavy favors reader-side cost.
- **Solution 1 alone is insufficient.** Single-leader fixes same-key ordering but doesn't address the cross-shard atomicity invariant. It's an *enabler* of solutions 2 and 3, not an alternative.
- **HLC (4) is solution 2 hardened.** The protocol shape is identical; HLC just hardens the clock layer to tolerate real-world skew. Worth doing once solution 2 is in place and the next workload exposes skew.
- **Solution 5 trades a bottleneck for protocol simplicity.** Workable for small clusters; the oracle has to be sharded itself once the cluster grows past the oracle's RTT capacity.
- **Closed timestamps (2) is the path with the lowest marginal cost over the existing code.** Single-leader is already true in small-db; we just need to add the registry, `T_closed` computation, reader gate, and the one-line `commit_ts` move. No clock infrastructure changes, no centralized service to operate, no new abort paths in the writer.

## Implementing Closed Timestamps

The chapter commits to **solution 2**, with the simplest possible variant: shared wall clock (no HLC), no margin, in-flight registry only. Each node's `T_closed` advances exactly as fast as in-flight writers settle.

### The new module

A new `src/closedts/` subdirectory with two pieces:

```cpp
// src/closedts/registry.h
namespace small::closedts {

// Per-node registry of writers that have written intents on this node
// and have not yet committed/aborted. Owned by the local server, accessed
// by WriteIntent (register), Txn::Commit/Rollback fan-out (deregister),
// and the closed-ts service handler (compute T_closed).
class InFlightRegistry {
 public:
    // Called when a writer first writes an intent on this node.
    // Idempotent: re-registering the same txn_id refreshes the
    // entry but doesn't change the protocol's lower bound.
    void Register(int64_t txn_id, int64_t lower_bound_commit_ts);

    // Called when the writer's coordinator confirms COMMITTED or
    // ABORTED status for the txn.
    void Deregister(int64_t txn_id);

    // T_closed = min(lower_bound for txn in registry) - 1, or
    // infinity-ish (a sentinel large value) if registry is empty.
    int64_t ComputedClosedTs() const;
};

}  // namespace small::closedts
```

```proto
// src/closedts/closedts.proto
service ClosedTsService {
    // Long-poll: block until T_closed >= min_ts on this node, then
    // return the current value. Cap the wait at deadline (passed
    // via gRPC client context); on deadline, return the current
    // value even if it's still below min_ts and let the caller
    // retry with a fresh deadline.
    rpc WaitForClosedTs(WaitForClosedTsRequest) returns (WaitForClosedTsResponse);
}

message WaitForClosedTsRequest { int64 min_ts = 1; }
message WaitForClosedTsResponse { int64 closed_ts = 1; }
```

### The flow change

**Write path** (`src/execution/update.cc` peer-side):
1. Acquire `lock(table, pk)` as today.
2. Read pre-image and per-row latest as today.
3. Bump `commit_ts = max(commit_ts, latest_ts + 1)` as today.
4. **New:** before WriteIntent, register on the local closed-ts registry: `registry.Register(txn_id, commit_ts)`.
5. Write the intent.
6. Lock released on update() return.

**Commit path** (`src/txn/handle.cc` in `Txn::Commit`):
1. **New:** `commit_ts = max(commit_ts, now_ms())` — Mechanism A.
2. `SetTxnStatus(COMMITTED, commit_ts)` on the coordinator's local DB.
3. **New:** for each `intent_key` in the txn record's `intent_keys[]`, fan out a `Deregister` RPC to that key's owner. Best-effort fire-and-forget — the registry is a cache; correctness comes from worst-case fall-through to the next RPC's ResolveIntent path (deferred to a v1 enhancement; for v0 we trust the deregister fan-out).

**Rollback path** (`src/txn/handle.cc` in `Txn::Rollback`): same fan-out as Commit, but with ABORTED.

**Read path** (`src/execution/query.cc` per-shard handler):
1. **New:** before scanning, call `registry.WaitForClosedTs(snapshot_ts)` on the local closed-ts service. Block until satisfied or deadline.
2. Scan as today.
3. Apply visibility rule against `snapshot_ts` as today.

The reader-side wait is per-shard, run inside each node's local `query` handler. The aggregate read coordinator doesn't need to know about closed timestamps; it just collects per-shard responses as before.

### Edge cases / scope decisions

- **Registry recovery on startup.** The in-memory registry is reconstructed by scanning the local intent keyspace and RPCing each intent's coordinator for status. Out of scope for this chapter; we assume server processes don't restart mid-test.
- **Coordinator crash mid-flight.** If a writer's coordinator dies between `WriteIntent` and `Txn::Commit/Rollback`, the writer stays registered on its intent owners forever; `T_closed` cannot advance past its `start_ts`. Out of scope for this chapter; the production fix is heartbeats on the txn record, deferred to a future page.
- **Lost deregister RPC.** If the COMMIT-time fan-out fails to reach an intent owner, the registry on that owner accumulates a stale entry. Mitigated in v0 by treating Deregister as best-effort plus a periodic refresh (every 1s, the registry RPCs each entry's coordinator and drops terminal-status entries). Costs a small amount of background traffic.
- **Read-side deadline.** `WaitForClosedTs` blocks server-side; the gRPC deadline (set by the read coordinator) caps the wait. On deadline, the read returns a retryable error to the SQL client.
- **No margin.** Pure in-flight-driven. If a shard has no in-flight writers, `T_closed` is effectively `now()` (a sentinel value the registry returns). Writers' `commit_ts` is set at COMMIT to `now_ms()`, naturally above any `T_closed` published before they registered.
- **No HLC.** The shared-clock assumption (3 VMs on one host) is good enough. Replacing wall-clock with HLC is a future chapter when we observe a skew-induced failure.

### What This Buys (and What It Doesn't)

**Buys.**

- **Cross-shard read atomicity for the bank test.** The Mode 2 partial-read race traced above stops occurring. Worker 1's read at index 7 either waits past T0's COMMIT (and sees both legs) or proceeds early enough that T0's `commit_ts > snapshot_ts` (and sees neither leg). No more torn reads.
- **A consistent-cut primitive** that future features (changefeeds, online backups, schema migrations) can build on without re-deriving the protocol.
- **`commit_ts` set at COMMIT time** is now the rule, eliminating a class of subtle bugs where `start_ts == snapshot_ts` made the visibility check incorrectly return "visible" for an in-flight writer.

**Doesn't.**

- **Mode 1 spurious aborts** (`:fail :transfer ... active intent on .../X for txn_id=Y; retry`). When two writers collide on the same row, the per-row lock manager and the abort-on-ACTIVE rule still fire. The Jepsen `active intent` count stays roughly where it was. The next chapter — Push-the-Writer — replaces self-aborts with pushes that resolve the conflict deterministically.
- **Coordinator-failure recovery.** A writer's coordinator crashing mid-flight blocks `T_closed` advancement on the writer's owners until the orphan is cleaned up. The mechanism (txn record heartbeats) is deferred to a future chapter.
- **Bounded clock skew.** The current implementation relies on the 3-VMs-on-one-host shared-clock assumption. Under real network deployment with NTP-only clocks, `T_closed` advancement and `commit_ts` assignment can disagree by the skew amount, reintroducing race windows. The HLC chapter that follows replaces wall clocks with hybrid logical clocks to close this.

The Jepsen test after this chapter lands should show `:wrong-total` count drop to ~0, `:fail :transfer ... active intent` count unchanged. That's the chapter-1 success criterion; chapter 2's success criterion is the latter going to ~0 too.
