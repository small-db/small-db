# Write Intents

A continuation of [Shadowed Writes](./shadowed_writes.md). That page surveyed five fixes for the anomaly. The cheapest one -- per-row monotonic bump -- closes the anomaly when paired with the deferred-writes pattern from [Multi-Statement Transactions](./multi_statement_transactions.md). This page implements a different fix, **write intents**, motivated by a property bump-at-commit doesn't give us: cheap **timestamp push**.

## What's a Push, and Why Care?

A timestamp push raises a transaction's `commit_ts` after the transaction has begun writing. Two common triggers:

- A subsequent UPDATE in the same transaction hits a row whose latest committed `version_ts` is larger than the transaction's current `commit_ts`. To keep per-row lex order matching commit order (the [Shadowed Writes](./shadowed_writes.md) invariant), `commit_ts` must move past it.
- A concurrent reader has snapshotted at a `read_ts` past the writer's intended `commit_ts`; the writer pushes to keep its commit external to that read.

The cost of a push depends on how in-flight writes are stored.

- **Buffered in memory until COMMIT** (the [Multi-Statement Transactions](./multi_statement_transactions.md) model): a push is an in-memory arithmetic adjustment before flush. The bump runs once per pending row in `CommitTxn`, with no disk I/O until the final batched flush. Cost stays bounded by `pending_writes.size()` * O(1).
- **Eager-flushed to disk at write time**, with `commit_ts` baked into the key as `version_ts`: a push means rewriting every already-flushed row at the new ts -- O(N) Puts and Deletes, where N is "rows already flushed."

Eager flushing is what production systems move to once transactions get large (millions of rows; long-held locks contending with concurrent writers). Under that model, a per-row push cost is what kills you. The on-disk record has to stop committing to `version_ts` at write time.

**Intents do that.** The disk record at write time is a placeholder carrying the new value and a back-pointer to the transaction. The effective `version_ts` is whatever the transaction record's `commit_ts` says at read time. A push updates one record (the txn record on the coordinator) -- regardless of how many intents the transaction has flushed.

## Disk Layout

Two new key shapes alongside the existing committed versions:

```
/<schema.table>/<pk>/INTENT     →  { value, txn_id, coordinator_addr }
/_txn/<txn_id>                  →  { status, start_ts, commit_ts, intent_keys[] }
```

- **Intent rows** sit in each row owner's RocksDB. The literal suffix `INTENT` sorts above every numeric `version_ts`, so an existing prefix scan over `/<table>/<pk>/` surfaces the intent first if one exists. The row lock prevents two intents on the same `(table, pk)` -- at most one intent per row at a time.
- **The transaction record** lives only on the *coordinator's* RocksDB, under the `/_txn/` prefix. `status` is `PENDING`, `COMMITTED`, or `ABORTED`. `commit_ts` is the timestamp every intent attached to this txn resolves at once `status = COMMITTED`.

We don't have shared storage. The intent therefore embeds `coordinator_addr` (gRPC endpoint of the coordinator that owns the txn record). A reader on any node uses that to fetch the record.

## Two Operations

### Writing an intent

Inside an active transaction, UPDATE on row R becomes:

1. Acquire `lock(R)`, held until COMMIT/ROLLBACK.
2. Read latest committed `version_ts` for R: `L = LatestVersionTs(R)`.
3. If `L >= txn.commit_ts`: **push.** Set `txn.commit_ts := L + 1`, persisted by one Put on the coordinator's `/_txn/<txn_id>`.
4. Write `Put(/<table>/<pk>/INTENT, { value, txn_id, coordinator_addr })` on R's owner.
5. Append the intent key to `intent_keys[]` in the txn record (one more Put on the coordinator).

Step 3 is the load-bearing one for this page: the push is a single Put on the coordinator, independent of how many intents are already out. Step 5 is mostly for post-mortem inspection -- it lets a debugger reading the coordinator's data dir enumerate which keys this transaction touched.

### Reading at snapshot_ts

A read at `snapshot_ts` over `(table, pk)` prefix-scans `/(table)/(pk)/` and may encounter:

- **Committed versions** with `version_ts <= snapshot_ts`: standard MVCC pick (lex-largest wins).
- **An intent** for transaction `T`: RPC `coordinator_addr` for `T`'s status.
  - `COMMITTED` at `commit_ts`: treat the intent as a committed version at `commit_ts`, then continue the usual MVCC pick under `<= snapshot_ts`.
  - `ABORTED`: skip the intent.
  - `PENDING`: simplest first cut -- the reader waits briefly, then errors back to the client. The client retries. Production systems push the writer, queue the reader, or build a wait-for graph; we defer that.

A reader that finds a `COMMITTED` intent may rewrite it as `/<table>/<pk>/<commit_ts> → value` and delete the INTENT key (lazy promotion). The system is correct without that cleanup; we defer it to a later page.

## Why It Fixes Shadowed Writes

Replay [Shadowed Writes](./shadowed_writes.md)'s Charlie scenario:

1. T2 (europe coordinator, `commit_ts = 873`) arrives at europe -- Charlie's owner -- first via the local hop. Acquires `lock(Charlie)`, reads latest committed = 1500, writes the intent at `/users/3/INTENT` with value 1439.
2. T2 commits. Europe's coordinator Puts `/_txn/T2 := { COMMITTED, commit_ts: 873 }`. Lock(Charlie) released.
3. T1 (america coordinator, `commit_ts = 870`) arrives at europe via the network hop. Acquires `lock(Charlie)`. Reads latest committed: scan resolves T2's intent (or its already-promoted `/users/3/...873`) → 1439 at `version_ts = 873`.
4. `L = 873 >= T1.commit_ts = 870` → **push**. `T1.commit_ts := 874`. One Put on america's `/_txn/T1`.
5. T1 writes its intent at `/users/3/INTENT` with value 1453.
6. T1 commits. America's coordinator Puts `/_txn/T1 := { COMMITTED, commit_ts: 874 }`.
7. A subsequent SELECT scans Charlie's chain, resolves both intents (or sees them already promoted to `/.../873` and `/.../874`), and picks lex-largest: `874 → 1453`. T1's value wins. No shadowing.

Step 4 is the key. The per-row bump from `multi_statement_transactions.md` would do the same arithmetic at COMMIT for buffered writes; intents do it at the moment of conflict, by updating one txn record instead of N rows.

## Persisting the Txn Record (and Why)

The txn record lives in RocksDB rather than coordinator memory because the project's debug workflow scans on-disk state after a Jepsen run. The runner tags every SQL statement with `op=N` (in `runner.clj`); combined with a persisted txn record this gives end-to-end post-mortem visibility:

- `/_txn/<txn_id>` on each region's data dir → who was PENDING / COMMITTED / ABORTED at shutdown, plus `start_ts` and `commit_ts`.
- `/<table>/<pk>/INTENT` keys left behind → which rows had unresolved intents.
- `server.log` `op=N` lines → which Jepsen op authored each statement.

Cost: one extra Put per state transition (BEGIN, push, COMMIT/ABORT) and one Put per intent dispatched. Negligible vs. the data writes themselves.

The persisted record also lays the groundwork for coordinator-restart recovery in a later page. We don't implement recovery here; we just don't make it impossible.

## What This Buys (and What It Doesn't)

**Buys.**

- **O(1) timestamp push.** A push updates one record on the coordinator -- regardless of how many intents the transaction has flushed.
- **Eager flush.** UPDATEs inside BEGIN/COMMIT no longer buffer; they hit disk immediately. Long transactions stop bounding system memory by their write-set size.
- **Same shadowed-writes guarantee** as the per-row bump approach: the lock+push protocol forces the second writer's `commit_ts` to be strictly larger than every previously committed `version_ts` on every row in its write set.

**Doesn't.**

- **Coordinator failure.** If the coordinator dies between writing intents and flipping its txn record to `COMMITTED`, the intents stay `PENDING` forever from any reader's perspective. A future page covers the cleanup protocol (TTL on `PENDING` records; orphan-intent takeover).
- **Reader-on-PENDING.** This page's reader simply errors and retries. Production systems push the writer or queue the reader.
- **Intent cleanup.** Resolved intents stay on disk as `INTENT` keys until a writer reuses the row or a sweeper rewrites them. Deferred.

The connection back to [Shadowed Writes](./shadowed_writes.md): that page's solution space focused on choices that don't change the on-disk write format. Intents do change the format -- in exchange for cheap pushes and a path to lazy resolution. CockroachDB and YugabyteDB landed on the same design for the same reason.
