# Write Intents

A continuation of [Shadowed Writes](./shadowed_writes.md). That page surveyed five fixes for the anomaly. The cheapest one -- per-row monotonic bump -- closes the anomaly when paired with the deferred-writes pattern from [Multi-Statement Transactions](./multi_statement_transactions.md). This page implements a different fix, **write intents**, motivated by a property bump-at-commit doesn't give us: cheap **timestamp push**.

> **Naming:** the txn carries two timestamps. `start_ts` is fixed at BEGIN and used as the read snapshot. `write_ts` is mutable: initialized to `start_ts`, pushed forward by the per-row bump rule on UPDATE, and *promoted* to the txn's final commit timestamp at COMMIT (the value at which every intent it wrote becomes visible). On disk the txn record stores `write_ts` for both the mid-flight and the post-COMMIT phase; readers that see the txn as COMMITTED treat `write_ts` as the txn's commit timestamp. See `closed_timestamps.md` for the additional bump that runs at COMMIT.

## What's a Push, and Why Care?

A timestamp push raises a transaction's `write_ts` after the transaction has begun writing. Two common triggers:

- A subsequent UPDATE in the same transaction hits a row whose latest committed `version_ts` is larger than the transaction's current `write_ts`. To keep per-row lex order matching commit order (the [Shadowed Writes](./shadowed_writes.md) invariant), `write_ts` must move past it.
- A concurrent reader has snapshotted at a `read_ts` past the writer's intended `write_ts`; the writer pushes to keep its commit external to that read.

The cost of a push depends on how in-flight writes are stored.

- **Buffered in memory until COMMIT** (the [Multi-Statement Transactions](./multi_statement_transactions.md) model): a push is an in-memory arithmetic adjustment before flush. The bump runs once per pending row in `CommitTxn`, with no disk I/O until the final batched flush. Cost stays bounded by `pending_writes.size()` * O(1).
- **Eager-flushed to disk at write time**, with `write_ts` baked into the key as `version_ts`: a push means rewriting every already-flushed row at the new ts -- O(N) Puts and Deletes, where N is "rows already flushed."

Eager flushing is what production systems move to once transactions get large (millions of rows; long-held locks contending with concurrent writers). Under that model, a per-row push cost is what kills you. The on-disk record has to stop committing to `version_ts` at write time.

**Intents do that.** The disk record at write time is a placeholder carrying the new value and a back-pointer to the transaction. The effective `version_ts` is whatever the transaction record's `write_ts` says at read time. A push updates one record (the txn record on the coordinator) -- regardless of how many intents the transaction has flushed.

## Disk Layout

Two new key shapes alongside the existing committed versions:

```
/<schema.table>/<pk>/INTENT     →  { value, txn_id, coordinator_addr }
/_txn/<txn_id>                  →  { status, start_ts, write_ts, intent_keys[] }
```

- **Intent rows** sit in each row owner's RocksDB. The literal suffix `INTENT` sorts above every numeric `version_ts`, so an existing prefix scan over `/<table>/<pk>/` surfaces the intent first if one exists. The row lock prevents two intents on the same `(table, pk)` -- at most one intent per row at a time.
- **The transaction record** lives only on the *coordinator's* RocksDB, under the `/_txn/` prefix. `status` is `PENDING`, `COMMITTED`, or `ABORTED`. `write_ts` is the timestamp every intent attached to this txn resolves at once `status = COMMITTED` -- it is the txn's commit timestamp post-COMMIT.

We don't have shared storage. The intent therefore embeds `coordinator_addr` (gRPC endpoint of the coordinator that owns the txn record). A reader on any node uses that to fetch the record.

## Two Operations

### Writing an intent

Inside an active transaction, UPDATE on row R becomes:

1. Acquire `lock(R)`, held until COMMIT/ROLLBACK.
2. Read latest committed `version_ts` for R: `L = LatestVersionTs(R)`.
3. If `L >= txn.write_ts`: **push.** Set `txn.write_ts := L + 1`, persisted by one Put on the coordinator's `/_txn/<txn_id>`.
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

### Promotion

When a reader resolves an intent to `COMMITTED`, it persists the resolution as a numeric MVCC version: `Put /<table>/<pk>/<commit_ts> = value`. **It does not delete the INTENT key.** Call this *half-promotion*.

Why not also delete? The reader has no row lock, and `/<table>/<pk>/INTENT` is a *slot* whose contents change as transactions come and go. Between a reader's `Get(/INTENT)` and a hypothetical `Delete(/INTENT)`, a writer can take `lock(R)`, replace the slot's contents with its own freshly-laid intent, and release the lock. The reader's path-addressed Delete then nukes the writer's brand-new intent, silently losing that write. The Put on `/<commit_ts>` is *content-addressed* (the key is derived from the txn's permanent `commit_ts`) and idempotent across all races; the Delete is not. Half-promote keeps the safe half.

A writer, by contrast, holds `lock(R)` and is the only party that can mutate the slot. When a writer encounters a prior `COMMITTED` intent under its lock it does *full-promotion*: a single atomic write batch of `Put /<commit_ts>` + `Delete /INTENT`. After that the row's chain has the prior commit as a numeric version and the slot is empty for the writer's own intent.

What half-promotion buys: the resolved value survives any future GC of the txn record, and the next writer's full-promote degenerates to just a `Delete`. What it does *not* buy: every reader on a row whose intent has not yet been full-promoted by a writer still pays one resolve-RPC. On rows with frequent writes this is amortized away; on cold rows the intent lingers. Async cleanup -- a sweeper that takes the row lock and full-promotes orphaned intents -- closes that gap and deserves its own chapter.

## Why It Fixes Shadowed Writes

Replay [Shadowed Writes](./shadowed_writes.md)'s Charlie scenario:

1. T2 (europe coordinator, `write_ts = 873`) arrives at europe -- Charlie's owner -- first via the local hop. Acquires `lock(Charlie)`, reads latest committed = 1500, writes the intent at `/users/3/INTENT` with value 1439.
2. T2 commits. Europe's coordinator Puts `/_txn/T2 := { COMMITTED, write_ts: 873 }`. Lock(Charlie) released. T2's commit timestamp is 873.
3. T1 (america coordinator, `write_ts = 870`) arrives at europe via the network hop. Acquires `lock(Charlie)`. Reads latest committed: scan resolves T2's intent (or its already-promoted `/users/3/...873`) → 1439 at `version_ts = 873`.
4. `L = 873 >= T1.write_ts = 870` → **push**. `T1.write_ts := 874`. One Put on america's `/_txn/T1`.
5. T1 writes its intent at `/users/3/INTENT` with value 1453.
6. T1 commits. America's coordinator Puts `/_txn/T1 := { COMMITTED, write_ts: 874 }`. T1's commit timestamp is 874.
7. A subsequent SELECT scans Charlie's chain, resolves both intents (or sees them already promoted to `/.../873` and `/.../874`), and picks lex-largest: `874 → 1453`. T1's value wins. No shadowing.

Step 4 is the key. The per-row bump from `multi_statement_transactions.md` would do the same arithmetic at COMMIT for buffered writes; intents do it at the moment of conflict, by updating one txn record instead of N rows.

## Persisting the Txn Record (and Why)

The txn record lives in RocksDB rather than coordinator memory because the project's debug workflow scans on-disk state after a Jepsen run. The runner tags every SQL statement with `op=N` (in `runner.clj`); combined with a persisted txn record this gives end-to-end post-mortem visibility:

- `/_txn/<txn_id>` on each region's data dir → who was PENDING / COMMITTED / ABORTED at shutdown, plus `start_ts` and `write_ts` (the latter being the txn's commit timestamp once status is COMMITTED).
- `/<table>/<pk>/INTENT` keys left behind → which rows had unresolved intents.
- `server.log` `op=N` lines → which Jepsen op authored each statement.

Cost: one extra Put per state transition (BEGIN, push, COMMIT/ABORT) and one Put per intent dispatched. Negligible vs. the data writes themselves.

The persisted record also lays the groundwork for coordinator-restart recovery in a later page. We don't implement recovery here; we just don't make it impossible.

## The Implementation

Six pieces. Most are small; the read-path change is the largest because the prefix scan now has to surface and resolve intents.

**Every statement runs inside a transaction.** An explicit `BEGIN`/`COMMIT` is one transaction; an auto-commit statement (no surrounding `BEGIN`) is wrapped by the dispatcher in an implicit single-statement transaction that goes through the same code below. There is no separate auto-commit fast path. This means every UPDATE -- including the auto-commit transfers used by the bank test in earlier chapters -- now writes an intent, pushes its `write_ts` if needed, and flips a txn record at commit. A few extra Puts per statement vs. the prior direct `WriteRow`; in exchange, the shadowed-writes invariant covers every write the cluster issues, not just those inside an explicit transaction.

### 1. Per-Connection Transaction State

`Txn` in `src/txn/handle.h` drops `pending_writes` (no in-memory buffer; intents are eager-flushed) and gains `txn_id` and `write_ts`:

```cpp
class Txn {
    bool active_ = false;
    int64_t txn_id_ = 0;
    int64_t start_ts_ = 0;
    int64_t write_ts_ = 0;  // == start_ts at BEGIN; pushed by the bump rule;
                            // promoted to commit_ts at COMMIT
};
```

The list of intent keys this txn has written is kept *only* on the on-disk txn record (`/_txn/<txn_id>.intent_keys[]`). No in-memory mirror -- nothing in the live commit/rollback path reads it. The on-disk list exists for a future sweeper/recovery page to walk when reclaiming aborted intents.

### 2. `BEGIN` Persists a Txn Record

```cpp
absl::Status Txn::Begin() {
    if (active_) return absl::FailedPreconditionError("nested BEGIN");
    active_   = true;
    txn_id_   = id::generate_id();
    start_ts_ = now_ms();
    write_ts_ = start_ts_;
    db->WriteTxnRecord(txn_id_, TxnRecord{
        TxnStatus::ACTIVE, start_ts_, write_ts_, {}});
    return absl::OkStatus();
}
```

The Put goes to the *coordinator's* RocksDB only -- the connection-handling node owns this txn record for its lifetime.

### 3. `UPDATE` Writes an Intent

The active-transaction branch in `src/execution/update.cc` no longer stages a `PendingWrite`. It writes an intent and updates the txn record:

```cpp
auto lock = LockManager::Acquire(table->name(), pk);

// Read latest committed version_ts on this row.
int64_t L = db->LatestVersionTs(table->name(), pk);

// Push if our write_ts isn't already past it.
if (L >= txn.write_ts) {
    txn.write_ts = L + 1;
    db->UpdateTxnWriteTs(txn.txn_id, txn.write_ts);  // one Put on coordinator
}

// Write the intent on the row's owner (reuses the existing dispatch path).
auto intent_key = absl::StrFormat("/%s/%s/INTENT", table->name(), pk);
db->WriteIntent(intent_key, new_value, txn.txn_id, coordinator_addr);

// Append to the txn record's intent_keys (on-disk only).
db->AppendTxnIntentKey(txn.txn_id, intent_key);  // one Put on coordinator

// Lock stays in held_locks until COMMIT/ROLLBACK.
txn.held_locks.push_back(std::move(lock));
```

`LatestVersionTs` becomes intent-aware. Its prefix scan over `/<table>/<pk>/` may encounter an `INTENT` key for a prior transaction. It resolves via `ResolveIntent` and acts on the result:

- `COMMITTED`: full-promote (atomic `Put /<commit_ts>` + `Delete /INTENT` -- safe because we hold `lock(R)`) and use the resolved `commit_ts` as the candidate for "latest." The slot is now empty for the writer's own intent.
- `ABORTED` / `UNKNOWN`: skip; the writer's own `WriteIntent` will overwrite the slot.
- `ACTIVE`: abort the current transaction with a retryable error. In the steady state this should not happen -- the row lock plus single-owner-per-row partitioning means any intent on `R` belongs to a transaction that has released its lock, and a transaction that has released its lock has flipped its status. The case still has to be handled, because a coordinator that crashed between writing intents and flipping its txn record leaves a stale `ACTIVE` record behind. Pushing the other transaction or queueing a waiter is deferred to a later page.

### 4. `COMMIT` / `ROLLBACK` Flip the Txn Record

```cpp
absl::Status Txn::Commit() {
    if (!active_) return absl::FailedPreconditionError("no active txn");
    // Mechanism A bump (see closed_timestamps.md): final push to now()
    // before promoting write_ts to the txn's commit timestamp.
    if (now_ms() > write_ts_) write_ts_ = now_ms();
    db->SetTxnStatus(txn_id_, TxnStatus::COMMITTED, write_ts_);
    active_ = false;
    return absl::OkStatus();
}

absl::Status Txn::Rollback() {
    if (!active_) return absl::FailedPreconditionError("no active txn");
    db->SetTxnStatus(txn_id_, TxnStatus::ABORTED, /*write_ts=*/0);
    active_ = false;
    txn.held_locks.clear();
    return absl::OkStatus();
}
```

No flush -- intents are already on disk. The single Put on `/_txn/<txn_id>` is the atomicity boundary: every reader that subsequently resolves any of this txn's intents observes the new status.

### 5. The Resolve RPC

A new gRPC service exposed by every server, alongside the existing `gossip` and catalog services:

```proto
service TxnService {
    rpc ResolveIntent(ResolveIntentRequest) returns (ResolveIntentResponse);
}

message ResolveIntentRequest { int64 txn_id = 1; }
message ResolveIntentResponse {
    enum Status { ACTIVE = 0; COMMITTED = 1; ABORTED = 2; UNKNOWN = 3; }
    Status  status    = 1;
    int64   commit_ts = 2;  // valid iff status == COMMITTED
}
```

The handler reads `/_txn/<txn_id>` from local RocksDB and returns its contents. `UNKNOWN` covers the "no record" case (corrupt intent, post-cleanup race) -- the reader treats it as aborted.

### 6. Read Path Resolves Intents

`src/execution/query.cc`'s prefix scan over `/<table>/<pk>/` now distinguishes numeric-`version_ts` keys from `INTENT` keys. Numeric keys sort below `INTENT`, so committed versions surface first:

```cpp
for (auto iter = db->NewPrefixIterator(prefix); iter.Valid(); iter.Next()) {
    if (iter.IsIntent()) {
        auto intent = iter.AsIntent();
        TxnService::Stub stub(intent.coordinator_addr);
        auto resp = stub.ResolveIntent({intent.txn_id});
        switch (resp.status) {
            case ABORTED:
            case UNKNOWN:
                continue;
            case ACTIVE:
                return absl::AbortedError("intent pending; client retry");
            case COMMITTED:
                if (resp.commit_ts <= snapshot_ts) {
                    candidates.push_back({resp.commit_ts, intent.value});
                }
                continue;
        }
    }
    // numeric version_ts
    if (iter.version_ts() <= snapshot_ts) {
        candidates.push_back({iter.version_ts(), iter.value()});
    }
}
// MVCC pick: largest version_ts among candidates.
```

`ACTIVE` returns a retryable error to the client. Push-the-writer and waiter queues are deferred to a later page; this implementation does the simplest thing that's correct.

## What This Buys (and What It Doesn't)

**Buys.**

- **O(1) timestamp push.** A push updates one record on the coordinator -- regardless of how many intents the transaction has flushed.
- **Eager flush.** UPDATEs inside BEGIN/COMMIT no longer buffer; they hit disk immediately. Long transactions stop bounding system memory by their write-set size.
- **Same shadowed-writes guarantee** as the per-row bump approach: the lock+push protocol forces the second writer's `write_ts` (and therefore its commit timestamp) to be strictly larger than every previously committed `version_ts` on every row in its write set.

**Doesn't.**

- **Coordinator failure.** If the coordinator dies between writing intents and flipping its txn record to `COMMITTED`, the intents stay `PENDING` forever from any reader's perspective. A future page covers the cleanup protocol (TTL on `PENDING` records; orphan-intent takeover).
- **Reader-on-PENDING.** This page's reader simply errors and retries. Production systems push the writer or queue the reader.
- **Intent cleanup.** Resolved intents stay on disk as `INTENT` keys until a writer reuses the row or a sweeper rewrites them. Deferred.

The connection back to [Shadowed Writes](./shadowed_writes.md): that page's solution space focused on choices that don't change the on-disk write format. Intents do change the format -- in exchange for cheap pushes and a path to lazy resolution. CockroachDB and YugabyteDB landed on the same design for the same reason.
