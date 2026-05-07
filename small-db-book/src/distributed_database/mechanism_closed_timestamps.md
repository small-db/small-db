# Closed Timestamps

A survey of how major distributed databases provide consistent reads in the face of concurrent writes spread across multiple nodes. The mechanism appears under different names — "closed timestamps," "safe time," "resolved timestamp," "high watermark" — but the structural pattern is the same: each shard publishes a value below which the world is settled, and readers gate themselves against that value.

## The problem this solves

A distributed read of a multi-row dataset must reconcile observations made on different nodes at slightly different physical instants. If row A's owner returns its data at wall-clock T₁ and row B's owner returns its data at wall-clock T₂ ≠ T₁, the two halves of the read can land on opposite sides of an in-flight write — one half sees the write, the other doesn't, and the reader's "consistent snapshot" is anything but. This is the *intent atomicity race* (see [intent_atomicity_race.md](./intent_atomicity_race.md)) in its most general form.

The fix has to bind the reader's per-shard observations to a shared logical instant such that visibility decisions are a pure function of `(eternal facts about each row, shared snapshot timestamp)`. Closed timestamps are how most production systems achieve this without trapping every read behind a global serialization point.

## The mechanism, abstracted

Strip away the implementation differences and the same five components appear in every closed-timestamp system:

1. **Per-shard tracker.** Each shard (range, region, partition, tablet — the term varies) maintains a value `T_closed`. The invariant: *no write with `commit_ts ≤ T_closed` will ever be admitted on this shard*. Once `T_closed` advances past a value, it can never retreat.

2. **Advancement protocol.** `T_closed` is moved forward as in-flight writers settle. The essential bound is:

   ```
   T_closed ≤ min(in-flight writers' lower-bound commit_ts) − 1
              (unbounded above if no in-flight writers)
   ```

   The `−1` is a convention artifact, not a safety buffer. The protocol uses `T_closed ≥ S` as the reader gate (inclusive) and `commit_ts > T_closed` as the writer constraint (strict). For both to be simultaneously satisfiable, `T_closed` must be strictly below every in-flight writer's lower bound — hence the `−1` with integer-valued timestamps. An exclusive-convention variant (reader gate `T_closed > S`, writer constraint `commit_ts ≥ T_closed`) eliminates the `−1` but moves the strict inequality to the read path. Same correctness, different cosmetics.

   Some systems additionally cap `T_closed ≤ now() − margin`. This is a deliberate optimization, *not* part of the core correctness story:
   - **Clock-skew tolerance.** If `T_closed` is replicated to followers and a follower's physical clock disagrees with the leader's by `δ`, a margin of `δ` keeps the published value conservative. Relevant under HLC; irrelevant under a shared clock.
   - **Reduce writer bumps.** Without margin, an idle shard has `T_closed = now()`, so every new writer's `start_ts ≤ T_closed` and the writer must bump immediately. A small margin gives writers a "fresh window" where their `start_ts` is naturally below `T_closed`. Pure throughput optimization.

   Systems with a shared clock or no cross-replica `T_closed` publication don't need the margin for correctness. TiKV's `resolved_ts` doesn't use one (it tracks only in-flight 2PC transactions). CockroachDB does (3 s default), driven by HLC skew tolerance + bump amortization.

3. **Publication.** The new `T_closed` is replicated to every replica of the shard, usually via the same consensus log that carries data writes (Raft, Paxos, or equivalent). Followers learn `T_closed` along with their normal log replay.

4. **Reader gate.** A read at snapshot `S` waits, for each shard it touches, until that shard's published `T_closed ≥ S`. After the wait, every per-shard observation is at a moment when nothing ≤ `S` is in flight on any shard. Per-shard visibility decisions are now a pure function of `commit_ts ≤ S`.

5. **Writer constraint.** A writer's `commit_ts` must be `> T_closed` on every shard it writes to. Two ways to enforce this:
   - **Pre-declared lower bound:** the writer registers a candidate commit_ts when it writes its first intent on a shard; the shard's `T_closed` advancement is bounded below it.
   - **Bump-on-write:** the writer checks `T_closed` at write time; if its candidate `commit_ts ≤ T_closed`, bump.

The variations between systems show up in: how `T_closed` is published (Raft log vs. side channel vs. heartbeat), what bounds advancement (Paxos state vs. transaction tracker vs. proxy), how the writer's commit_ts is established (HLC vs. TrueTime vs. wall clock vs. central oracle), and what the value is used for (follower reads, CDC, snapshot isolation, garbage collection).

## Per-system survey

### CockroachDB — Closed Timestamps

The system that popularized the term in modern usage. Closed timestamps in CockroachDB went through two design iterations: an initial "v1" that piggybacked on the Raft log, and a redesigned "v2" that introduced a separate side-transport channel for performance.

**Mechanism.**
- Each range's leaseholder maintains a closed timestamp value, an HLC paired with a `LAI` (Lease Applied Index — the Raft log index up to which the leaseholder has applied entries).
- Advancement happens on two paths:
  - **Raft path.** Every Raft proposal carries a closed-timestamp update. Followers learn `T_closed` along with the entries they apply.
  - **Side transport** (v2, default since CRDB 21.1). A separate gossip-like protocol publishes closed timestamps independently of the Raft log, every `kv.closed_timestamp.side_transport_interval` (default 200 ms). This decouples closed-ts advancement from data-write throughput.
- The leaseholder caps `T_closed` at `max(in-flight write lower bound − 1, current_time − target_duration)`. The `target_duration` is `kv.closed_timestamp.target_duration` (default **3 seconds**).
- Lease transfers: when a lease moves to a new replica, the new leaseholder must observe a closed timestamp ≥ the previous leaseholder's. This is enforced by the lease-transfer protocol.
- Writers carry their HLC `commit_ts` from the start of the transaction. When a writer writes an intent on a range, the range checks the writer's `commit_ts` against its local `T_closed`. If `commit_ts ≤ T_closed`, the writer must bump (push its `commit_ts` forward and refresh its read set).

**Use cases.**
- **Follower reads.** A read at snapshot `S ≤ T_closed` on every range it touches can be served by any replica, not just the leaseholder. This is the marquee feature: it lets read-heavy workloads parallelize across replicas without paying for a leaseholder hop. Available via `SELECT ... AS OF SYSTEM TIME follower_read_timestamp()` or `SET TRANSACTION AS OF SYSTEM TIME ...`.
- **`AS OF SYSTEM TIME` queries.** Historical reads at any closed-ts-bounded snapshot. Used for backups, analytical queries, and anything that wants snapshot consistency without blocking writes.
- **Bounded-staleness reads.** `SELECT ... AS OF SYSTEM TIME with_max_staleness('10s')` — the planner picks a timestamp guaranteed to be closed and within 10 s of `now()`.
- **Changefeeds (CDC).** A changefeed consumer learns "all writes at ts ≤ X are now visible on this range" precisely because `T_closed` advanced to X. The consumer relies on this to emit ordered, atomic events.
- **MVCC garbage collection.** GC can run at any `gc_threshold ≤ T_closed` because everything below is settled — no in-flight writer can come back and reference an older version.

**Tunables.**
- `kv.closed_timestamp.target_duration`: how far behind `now()` `T_closed` lags. Default 3 s. Reducing it makes follower reads fresher but pressures advancement.
- `kv.closed_timestamp.side_transport_interval`: cadence of the side-transport publication. Default 200 ms.
- `kv.closed_timestamp.lead_for_global_reads_override`: for global tables, the closed timestamp leads `now()` instead of lagging — used for low-latency reads against multi-region tables. Inverts the usual model.

**Code references.** `pkg/kv/kvserver/closedts/` in the CockroachDB tree. The two RFCs ("Range Leases Holdoff" and "Closed Timestamps v2") describe the protocol evolution.

### YugabyteDB — Safe Time

YugabyteDB is architecturally Spanner-inspired but uses Hybrid Logical Clocks (called HybridTime in YB) instead of TrueTime, similar to CockroachDB. Its closed-timestamp analog is called *safe time*.

**Mechanism.**
- Each tablet (= shard) has a leader and followers replicating via Raft.
- The tablet leader maintains `safe_time` — the HybridTime up to which all writes are committed and applied locally on the leader.
- `safe_time` is bounded below by any in-flight 2PC transaction's prepared timestamp. Once a transaction commits or aborts, the bound is released.
- The leader propagates `safe_time` to followers via heartbeats and Raft log entries.
- Default advancement cadence: on the order of **500 ms**, more aggressive than CockroachDB's 3 s default.

**Use cases.**
- **Follower reads.** Enabled with `SET yb_read_from_followers = on`. A read on a follower is served at min(`safe_time` across tablets), bounded by `yb_follower_read_staleness_ms`.
- **Strongly consistent snapshot reads.** Used for snapshot transactions: pick a `read_time = safe_time`, scan every tablet at that `read_time`. Atomic across tablets because `safe_time` is a global cut.
- **xCluster replication.** YugabyteDB's async replication between clusters uses safe_time as the consistency cut: the target cluster knows it has received all writes ≤ `safe_time` from the source.
- **Change Data Capture.** Same principle as CockroachDB's changefeeds — consumers receive writes in `safe_time` order.

**Distinction from CockroachDB.** YugabyteDB's safe_time is per-tablet; CockroachDB's closed_ts is per-range. Both decompose to per-shard tracking, but YB's tablet boundaries are usually coarser (table-aligned) while CRDB ranges are finer (~64 MB by default). The advertisement cadences also differ.

### TiKV — Resolved Timestamp (`resolved_ts`)

TiKV is the distributed key-value store underneath TiDB. The closed-timestamp analog in TiKV is called the *resolved timestamp*.

**Mechanism.**
- Each TiKV region (= shard) maintains a `resolved_ts` value, computed by tracking all in-progress 2PC transactions that have written intents to the region.
- TiKV uses Percolator-style 2PC: writes go through a Prewrite phase (writes locks/intents) and a Commit phase (writes the commit record with `commit_ts`).
- The region's leader runs a `Resolver` component that scans the region's Lock CF for active locks. `resolved_ts = min(start_ts of all active locks) − 1`. If no locks, `resolved_ts = current_ts`.
- The resolver subscribes to lock changes via the region's apply log, so it doesn't need to re-scan from scratch on every advancement.
- Published to followers via Raft.

**Use cases.**
- **TiCDC (TiDB Change Data Capture).** TiCDC pulls events from TiKV regions; it relies on `resolved_ts` to know "all writes at ts ≤ X are now ordered and final" before forwarding events downstream. Without `resolved_ts`, TiCDC could miss late-arriving commits.
- **Stale reads.** TiDB exposes `SET tidb_read_staleness = 5` (read 5 seconds in the past). The query is routed at `now() − 5s`, which is below `resolved_ts` everywhere, so any region replica can serve it.
- **TiFlash sync.** TiFlash is TiDB's columnar replica engine. It applies updates from TiKV in `resolved_ts` order, producing analytical-read-friendly columnar snapshots that are transactionally consistent up to a known timestamp.
- **Stale read with replicas.** Combined with TiKV's Raft-based replicas, stale reads can be served by any peer (not just the leader) for a region whose `resolved_ts` is high enough.

**Note on TiDB itself.** TiDB at the SQL layer doesn't use `resolved_ts` directly for its primary read path. TiDB asks PD (the Placement Driver) for a transaction `read_ts`, then reads at that `read_ts` via TiKV. PD is a centralized timestamp oracle, more like FoundationDB's proxy than CockroachDB's distributed closed-ts. The `resolved_ts` machinery is a separate layer used for stale reads, CDC, and TiFlash, all of which need "what's settled?" semantics that the PD oracle doesn't directly provide.

### Spanner — Safe Time (`t_safe`)

Spanner predates the modern distributed-SQL closed-timestamp implementations and inspired most of them. Its safe-time mechanism uses TrueTime instead of HLC for the clock layer.

**Mechanism.**
- Each Paxos group (= shard) maintains `t_safe = min(t_safe_paxos, t_safe_TM)`:
  - **`t_safe_paxos`** is the highest applied write timestamp in this Paxos group's state. It advances as new writes are applied.
  - **`t_safe_TM`** is the floor below which transaction-manager state is settled. If there are no in-progress 2PC transactions involving this group, `t_safe_TM = ∞`. If there are, `t_safe_TM = min(prepared_ts of all such transactions) − 1`.
- TrueTime guarantees that `TT.now()` returns an interval `[earliest, latest]` such that the true wall-clock time is somewhere inside it. The interval width `ε` is a few milliseconds with Google's GPS+atomic-clock infrastructure.
- A commit picks `commit_ts ≥ TT.now().latest`, then performs **commit-wait**: the coordinator waits until `TT.now().earliest > commit_ts` before declaring the commit visible. By the time `:ok` returns to the client, every node's clock has provably passed `commit_ts`. After that, `t_safe` can advance past `commit_ts` because no future commit can land ≤ `commit_ts`.
- Followers learn `t_safe` from the Paxos leader through normal Paxos replication.

**Use cases.**
- **Snapshot reads at any `t ≤ t_safe`** on every Paxos group involved, served by any replica. Reads at the leader, follower, or "preferred" replica are all consistent because they all observe the same `t_safe`.
- **Backups.** A consistent backup at timestamp `T` requires every Paxos group to have `t_safe ≥ T`. Once that holds globally, the backup process scans every group at `T` and produces a transactionally consistent snapshot.
- **Schema migrations.** Long-running schema changes use a known stable snapshot to avoid racing with concurrent writes.
- **Read-only transactions.** Spanner supports lock-free read-only transactions that pick a read timestamp ≤ `t_safe` and execute without acquiring any locks. Massively reduces contention for read-heavy workloads.

**TrueTime's role.** TrueTime is what makes commit-wait correct. Without bounded clock uncertainty, you can't say "every node's clock has passed `commit_ts`" without a global synchronization step. TrueTime gives Spanner the bound; commit-wait converts the bound into "after this point, the rest of the cluster has caught up."

The original Spanner paper (Corbett et al., OSDI 2012) is the primary reference. Subsequent papers and Cloud Spanner documentation expand on the protocol.

### MongoDB — Majority Committed Snapshot

MongoDB's mechanism is per-replica-set rather than per-shard, but the structural pattern is the same.

**Mechanism.**
- A replica set has one primary (writer) and N secondaries (readers + replication followers).
- The primary writes operations to its oplog with monotonically-increasing OpTimes (timestamp + term).
- Secondaries pull oplog entries from the primary (or from a chained source) and apply them in order.
- Each node periodically computes `majorityCommittedOpTime` = the latest OpTime that has been replicated to a majority of the set. This is a quorum-derived value: every node can independently determine it by tracking other nodes' replication progress (advertised via heartbeats).
- Each node retains a snapshot pinned at `majorityCommittedOpTime` — the snapshot is durable and reflects committed-everywhere state.

**Use cases.**
- **`readConcern: "majority"`.** Returns data as of `majorityCommittedOpTime`. Survives primary failover (the data has been replicated). The default for many MongoDB drivers in production deployments.
- **`readConcern: "snapshot"` transactions** (4.0+). Multi-statement transactions that see a stable snapshot for the duration. Implemented on top of the majority-committed snapshot.
- **Causal consistency.** Each operation returns a `$clusterTime` token. Subsequent operations from the same client carry the token; the server ensures the read snapshot is at or after it. Implements "read your writes" and other causal guarantees.
- **Backup at a stable snapshot.** Same idea as Spanner backups, scaled to MongoDB's replica-set model.

**Sharded clusters.** A MongoDB sharded cluster has multiple replica sets, one per shard. Cross-shard transactions use 2PC; cross-shard snapshot reads require coordinating snapshots across shards. The `majorityCommittedOpTime` is replica-set-local; the cluster-wide snapshot mechanism uses cluster-time tokens to bridge across shards.

### FoundationDB — Read Versions (different shape, same role)

FoundationDB has a different architecture from the systems above: it uses a centralized transaction-management plane rather than per-shard consensus groups. Its "read version" plays the closed-timestamp role.

**Mechanism.**
- Every transaction starts by getting a **read version** from one of the cluster's transaction proxies. The read version is the latest globally-committed version at that moment.
- Writes go through proxies → resolvers → log servers. Resolvers detect read-write conflicts; log servers persist committed mutations. The proxy that handed out the read version knows what's committed because it serializes commits through itself.
- Reads in the transaction execute against the read version, fetching values from the storage servers (which apply log mutations and serve historical reads).
- The proxy guarantees that no commit at a version ≤ the read version it issued is still in flight: the read version is, by construction, a closed timestamp.

**Differences from the per-shard model.**
- Centralized issuance: one proxy decides what "the latest" is. No per-shard advertisement, no advancement protocol.
- No follower-read parallelism in the same sense — every read transaction goes through a proxy first to get its read version. After that, reads can be parallelized across storage servers.
- The cluster controller/proxy is a serialization point, but FoundationDB's design horizontally scales proxies + resolvers + log servers, so it's not a single bottleneck.

**Use cases.**
- Every read transaction. There's no separate "stale read" or "snapshot read" API — all reads are snapshot reads at the issued read version.
- The model is closer to TiDB+PD than to CockroachDB or Spanner. It chooses centralized simplicity over distributed agreement.

### Apache Kafka — High Watermark

Worth including because the structural pattern is identical even though Kafka isn't a database.

**Mechanism.**
- Each topic-partition has a set of in-sync replicas (ISR) — followers caught up to the leader within a configurable lag.
- Each replica's *log end offset* (LEO) is the highest message offset it has appended.
- The partition's *high watermark* (HW) is `min(LEO across ISR)` — the offset up to which messages are replicated to all ISR members.
- HW is published to followers via the fetch response (the leader includes its HW when followers ask for new messages). Each follower learns the HW it can serve to consumers.
- HW only advances forward; never retreats.

**Use cases.**
- **Consumer reads.** Consumers can only read messages with offset < HW. This guarantees that any message a consumer sees has been replicated to all ISR — it survives any single failure.
- **Replica catch-up.** A new replica catches up to the leader's LEO before joining the ISR; only after that does it count toward HW computation.

This is the closed-timestamp pattern applied to log offsets instead of HLC timestamps. Kafka Streams' "watermarks" and Apache Flink's event-time watermarks generalize the idea further: a watermark is a closed timestamp on event-time progression.

### Postgres physical replication and Aurora

Both use a per-replica analog: each replica has applied WAL up to some position (LSN in Postgres / Aurora). Reads on a hot standby can serve queries up to the replica's `replay_lsn`.

This is closed-timestamps-of-one — there's no inter-shard atomicity to coordinate because Postgres physical replication has a single primary. Aurora's distributed storage layer lifts this slightly: storage nodes maintain a committed_LSN, and any reader can read at any LSN ≤ `committed_LSN` and get a consistent state. But there's still a single writer in classic Aurora (Aurora Multi-Master changes this).

## Systems that don't use closed timestamps

### Apache Cassandra / ScyllaDB / DynamoDB

These are eventually consistent by default. The "consistency mechanism" is a tunable per-operation read/write quorum (R + W > N for strong consistency on a single key) plus read repair and gossip-based replication. There's no distributed snapshot concept because the system explicitly accepts inter-replica disagreement on a per-key basis.

DynamoDB exposes a `ConsistentRead` flag that routes a read to the leader replica — but that's a per-request linearizability guarantee, not a snapshot. DynamoDB Transactions (since 2018) provide cross-key atomicity via 2PC, but the read path doesn't use closed-timestamp-like settled snapshots.

ScyllaDB's "Lightweight Transactions" use Paxos for per-key linearizability but don't introduce closed timestamps for reads. Cassandra's CDC mechanism is per-replica, doesn't provide ordered global delivery.

### etcd / ZooKeeper / Consul KV

Single Raft group (or a small fixed number of Raft groups). All reads either go through the leader (linearizable) or a follower with `WithSerializable()` (potentially stale, not snapshot-consistent). There's no closed timestamp because there's no inter-shard atomicity problem to solve — the entire keyspace is a single shard.

For workloads where this single-shard limit matters, you scale horizontally by sharding above etcd, in which case the shard layer needs its own consistency mechanism — usually closed-timestamp-shaped if cross-shard reads need to be consistent.

### Calvin / FaunaDB / VoltDB

Deterministic-execution systems. Every transaction is appended to a global input log; the system replays the log on every replica deterministically. The serialization point is the log, not a snapshot timestamp.

Reads see the state after a deterministic prefix of the log. There's no MVCC, no clock skew, and consequently no need for closed timestamps. The cost is that the log itself becomes the throughput bottleneck — Calvin-style systems trade horizontal write scalability for protocol simplicity.

### TiDB at the SQL layer

As noted in the TiKV section: TiDB's SQL layer asks PD (the Placement Driver) for transaction timestamps. PD is a centralized timestamp oracle backed by a small Raft group. Every commit gets a `commit_ts` from PD; every read gets a `read_ts` from PD. PD's monotonic timestamp issuance gives the cluster a total order without needing per-shard closed timestamps.

The closed-timestamp-equivalent (`resolved_ts`) lives in TiKV underneath TiDB and is used for stale reads, CDC, and TiFlash sync — but not for the primary OLTP read path.

## Patterns and design tradeoffs

### Per-shard tracking vs. centralized oracle

| Approach | Examples | Trade |
|---|---|---|
| Per-shard closed_ts | CockroachDB, YugabyteDB, TiKV, Spanner | Horizontal scalability; reader-side wait per shard. Each shard tracks its own in-flight writers. |
| Centralized oracle | TiDB+PD, FoundationDB | Simpler protocol; serialization point can be a bottleneck and SPOF unless replicated. Reads don't wait for advancement, they wait for the oracle's response. |

The choice tends to follow the rest of the architecture. Spanner-lineage systems (CockroachDB, YugabyteDB) commit to per-shard consensus and naturally end up with per-shard closed timestamps. Systems with a separate transaction-management tier (TiDB, FoundationDB) can centralize timestamp issuance because they already have a tier to host it.

### Reader-side wait vs. writer-side wait

Closed timestamps put the cost on readers: a reader at snapshot `S` must wait until `T_closed ≥ S` on every shard it touches. Spanner's commit-wait puts the cost on writers: the coordinator waits until `TT.now().earliest > commit_ts` before declaring commit visible.

Both achieve the same correctness. The choice depends on the workload:

- **Read-heavy**: writer-side wait is preferable. Writers pay; the read path is fast.
- **Write-heavy**: reader-side wait is preferable. Reads tolerate `margin` of staleness; writes are unblocked.
- **Mixed**: typically pick one and tune `margin` to balance.

Spanner can do writer-side wait because TrueTime gives a bounded `ε` (~7 ms) — the writer pays a few ms per commit. Without bounded clock uncertainty, writer-side wait would have to use a conservative `ε` (hundreds of ms with NTP), making commits expensive. That's why CockroachDB and YugabyteDB chose reader-side wait + HLC.

### Margin: optimization, not part of the core protocol

The `margin` (time by which `T_closed` lags `now()`) doesn't appear in every system. It's an optimization that buys two things:

- **Tolerance for cross-replica HLC skew.** If `T_closed` is replicated to followers and followers serve reads, the followers' physical clock may disagree with the leader's by `δ`. `margin > δ` keeps the published value conservative.
- **Amortizing writer bumps.** A small margin gives new writers a "fresh window" where `start_ts ≤ now() − margin = T_closed_max` is unlikely, so writers don't have to bump on every commit.

Industry choices:

| System | Default margin | Reason |
|---|---|---|
| CockroachDB | 3 s | HLC skew tolerance + bump amortization |
| YugabyteDB | ~500 ms | similar; less aggressive than CRDB |
| Spanner | bounded by TrueTime `ε` (~7 ms) | hardware clocks make `δ` tiny |
| MongoDB | replication-lag-driven, tens-to-hundreds of ms | follows oplog replication |
| TiKV (`resolved_ts`) | none | tracks only in-flight 2PC transactions |

For systems with a shared clock and no cross-replica `T_closed` publication, the margin disappears entirely — the protocol degenerates to "T_closed advances exactly as in-flight writers settle." TiKV is the canonical example, and it's the formulation small-db should reach for: simplest, no magic numbers, no unexplained "wait for `margin`" steps.

### Closed timestamps and 2PC are orthogonal

A common confusion: 2PC is about *write atomicity across shards*; closed timestamps are about *read consistency across shards*. They're independent mechanisms.

- A system can have 2PC without closed timestamps (e.g., a system that only supports linearizable reads through leaseholders never needs to settle a snapshot for follower reads).
- A system can have closed timestamps without 2PC (e.g., a system that doesn't support cross-shard transactions can still benefit from closed-ts for snapshot reads on a single shard).
- All Spanner-lineage systems use both.

In practice, the absence of 2PC limits the consistency story (no cross-shard atomic commits), and the absence of closed timestamps limits the read story (no snapshot reads, no follower reads, no efficient CDC). Production systems typically need both.

### CDC is just a closed-timestamp consumer

A change data capture pipeline emitting "all writes at ts ≤ X are now final" is structurally identical to a reader that gates on `T_closed ≥ X`. CockroachDB's changefeed, TiCDC, YugabyteDB's xCluster, MongoDB's change streams — all of them rely on the underlying closed-timestamp mechanism to produce ordered, atomic events.

This is one of the strongest reasons to invest in closed timestamps: even if your read workload can tolerate eventual consistency, your downstream pipelines almost certainly can't, and a closed-ts mechanism is the natural primitive for consistent CDC.

### Failure recovery

What happens when a writer's coordinator crashes mid-flight?

- The in-flight write registry on each affected shard still tracks the writer, capping `T_closed` advancement.
- Without a recovery mechanism, `T_closed` is stuck forever — readers wait indefinitely.
- Production systems handle this with **transaction-record heartbeats**: the writer's coordinator updates a heartbeat field on its txn record at a known cadence (CockroachDB: ~1 s). If a shard observes a stale heartbeat (>5 s old, configurable), it's allowed to force-abort the writer regardless of priority.
- Spanner uses Paxos-replicated transaction state, so the txn coordinator can survive node failure via leader election in the txn's Paxos group.

This is one of the edge cases that distinguishes a teaching implementation from a production one. The closed-ts mechanism itself is straightforward; making it survive failures is what most of the production complexity is.

### Closed timestamps and clock layers

The closed-timestamp protocol is independent of the underlying clock semantics. Different choices for the clock layer yield different practical properties:

| Clock layer | Used by | Implication |
|---|---|---|
| Wall clock + safety margin | (small-db's planned approach) | Simple, requires `margin > max clock skew` |
| Hybrid Logical Clocks (HLC) | CockroachDB, YugabyteDB | Handles bounded skew dynamically; adds logical counter |
| TrueTime (bounded `ε`) | Spanner | Hardware infrastructure; smallest `margin` possible |
| Centralized oracle | TiDB+PD, FoundationDB | No clock disagreement to resolve |

The closed-ts protocol — per-shard tracker, advancement, publication, reader gate, writer constraint — works the same way across all of these. The clock layer only changes what "compare two timestamps" means and how `commit_ts` is established.

## Why this matters for distributed reads

Closed timestamps unlock a set of capabilities that's hard to get any other way:

1. **Follower reads at snapshot consistency.** Without closed timestamps, every consistent read has to go to the leader/leaseholder. Closed-ts lets followers serve consistent reads at slightly stale snapshots, which dramatically improves read throughput in read-heavy workloads.
2. **Bounded-staleness reads.** A user can request "read at most 5 s in the past" and trade staleness for predictable parallelism.
3. **Atomic cross-shard reads.** Snapshot reads across multiple shards need a shared cut. Closed timestamps provide it without requiring per-read 2PC.
4. **Ordered, atomic CDC.** Downstream consumers see events in commit order, with the guarantee that a given timestamp's events are complete.
5. **Snapshot-isolated read transactions.** Multi-statement read-only transactions can execute lock-free at a closed timestamp, drastically reducing contention with writers.
6. **Online schema migrations.** Schema changes can pin a stable snapshot to validate against.
7. **Garbage collection of MVCC versions.** Old versions can be GCed below `T_closed` because nothing in flight can reference them.

The cost is per-shard machinery (tracker, advancement, publication) and reader-side latency (waiting for `T_closed` to catch up). For most production OLTP workloads the cost is well-amortized: the per-shard machinery costs are constant per shard, and the reader latency is typically below the network RTT to a leaseholder, so follower reads end up faster than leaseholder reads despite the wait.

## Further reading

For depth on any individual system:

- **CockroachDB**: the "Closed Timestamps" RFC (`docs/RFCS/20180603_follower_reads.md` and the v2 redesign), and the `pkg/kv/kvserver/closedts/` source tree.
- **YugabyteDB**: the documentation pages on safe-time, follower reads, and xCluster replication.
- **TiKV**: the resolved-ts design doc in the TiKV repo (`docs/design/resolved-ts.md` or similar) and the TiCDC architecture posts.
- **Spanner**: the OSDI 2012 paper "Spanner: Google's Globally-Distributed Database" (Corbett et al.) is the primary source. The follow-up paper on Spanner SQL (SIGMOD 2017) covers the read-only-transaction details.
- **MongoDB**: the documentation on read concerns, replica sets, and causal consistency.
- **FoundationDB**: the FDB architecture documentation; the "FoundationDB: A Distributed Unbundled Transactional Key Value Store" paper (SIGMOD 2021).
- **Apache Kafka**: the Kafka improvement proposals (KIPs) on consistent consumers, particularly KIP-101 and the high-watermark machinery in `kafka.cluster.Partition`.

The mechanism's core idea — published, per-shard, advancing, gating reads — is more than fifteen years old at this point (Spanner predated CockroachDB by ~5 years). What changes between systems is the engineering: how closely it integrates with the consensus layer, how aggressively it advances, what fault-tolerance story sits underneath, and what the API surface looks like to applications. The mechanism itself is one of the more universal patterns in modern distributed databases.
