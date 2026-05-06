# Multi-Statement Transactions

## The Anomaly

**Behavior.** A multi-statement transaction (`BEGIN; UPDATE; UPDATE; COMMIT`) commits each statement at a different timestamp. A concurrent reader can land at a snapshot in between, observing some of the transaction's writes and not others. From the application's perspective, the transaction "wasn't atomic": invariants the application coded as "true between transactions" appear violated mid-transaction.

**Root cause.** The system has no notion of a multi-statement transaction. `BEGIN` and `COMMIT` are no-ops -- in `stmt_handler.cc` the `TRANSACTION_STMT` case just logs the statement and returns. Each statement runs as its own auto-commit transaction, picks its own `ts` via `pick_ts()`, and commits independently.

**Does this happen in single-server databases?** No, not in any production database. SQL transaction semantics -- atomicity in particular -- have been baseline since the 1970s. Every mainstream RDBMS (MySQL, Postgres, Oracle, SQLite, ...) implements `BEGIN`/`COMMIT` with proper atomicity. Distributed systems have to extend this across nodes (Spanner, CockroachDB, YugabyteDB do), but atomicity itself isn't a distribution problem -- it's a transaction-management problem that single-node databases solved decades ago.

**Typical solutions.** Three families:

- **Transaction ID + commit log.** Postgres assigns each transaction an XID. Writes are stamped with the XID at the moment of the write. Reads check the commit log to learn whether a tuple's XID is committed, aborted, or in flight. `COMMIT` is a single update to the commit log that flips visibility for every write the transaction made. Atomicity from one commit-log entry; no buffering of writes.
- **Provisional records / intents.** CockroachDB and YugabyteDB write "intents" -- provisional values tagged with the writing transaction's ID. Other readers encountering an intent consult the transaction's record to learn its status. `COMMIT` updates the txn record once; intents are resolved lazily on subsequent reads. Atomicity from the txn-record update.
- **Deferred writes.** The simplest approach: buffer all of the transaction's writes in memory until `COMMIT`, then flush them all at a single `commit_ts`. Atomicity from the single batched flush. Less efficient under long transactions (memory grows with the write set), but no commit log or intent-resolution machinery required.

For small-db, deferred writes is the smallest change. We don't have a commit log; intents would require their own resolution protocol; buffering pending writes in per-connection state is straightforward.

## The Problem (in this system)

The previous two pages -- [Read Skew](./read_skew.md) and [Lost Updates](./lost_update.md) -- shipped two fixes:

- **MVCC + snapshot reads** so a single SELECT sees a consistent point-in-time view.
- **Per-row exclusive locking with read-latest under the lock** so concurrent writers to the same row can't clobber each other's pre-images. (Strictly, this is single-statement mutual exclusion; it becomes proper two-phase locking later in *this* page, once locks start being held across multiple statements.)

Together they cut the bank test's failure rate sharply but did not pass it:

| | Before MVCC + per-row lock | After |
|---|---|---|
| Reads observed | 51 | 44 |
| Errors | 50 (98%) | 28 (64%) |
| Total range | 9793 → 10042 | 9855 → 9988 |

Two changes worth noticing in that table. **The error rate fell from 98% to 64%** -- many reads now succeed because the cluster no longer carries permanent on-disk corruption from lost updates. **Every observed total is now strictly less than 10,000** -- the "money created" cases (10042 in the previous run) are gone, because lost-update was the only path by which money could be invented from thin air. What remains is purely "money temporarily missing": a SELECT lands in the middle of a transfer, sees the debit, doesn't see the credit, and returns a deficit.

Concretely, here is what happens during one of the remaining failures:

```
client: BEGIN;                        -- no-op in our system
client: UPDATE balance = balance - 42 WHERE id = 3;
        coordinator picks ts_a, e.g. 1000
        commits Charlie at version_ts ~ 1000
client: UPDATE balance = balance + 42 WHERE id = 5;
        coordinator picks ts_b, e.g. 1001
        commits Eve at version_ts ~ 1001
client: COMMIT;                       -- no-op in our system
```

A concurrent SELECT with `snapshot_ts = S`:

| `S` range | Charlie visible at | Eve visible at | Sum |
|---|---|---|---|
| `S < 1000`              | pre-debit          | pre-credit      | `10000` ✓ |
| `1000 ≤ S < 1001`       | post-debit         | pre-credit      | `9958` ✗ |
| `S ≥ 1001`              | post-debit         | post-credit     | `10000` ✓ |

The 1-ms window where `1000 ≤ S < 1001` is small, but with hundreds of transfers per second the bank test's read operations land in such windows often.

The SELECT itself is internally consistent -- it returns the cluster's state at `S`, which was real. The bank invariant fails because the *transfer* isn't atomic in our system: it commits as two separate write transactions at two separate timestamps, with no notion that they belong together.

## What "Fixing It" Has to Guarantee

A multi-statement transaction `T` must satisfy two properties:

- **All-or-nothing visibility.** There exists a single moment at which all of `T`'s writes become visible to other transactions; before that moment, none of them are visible.
- **Single commit timestamp.** All of `T`'s writes share one `version_ts`, so MVCC reads see them as a unit.

The simplest path that gets there: per-connection state holding a buffer of pending writes; on `COMMIT`, flush them all at a single `commit_ts`.

> **This page pairs with [Read Skew](./read_skew.md).** The two together solve the same observable failure -- an application sees only part of a logical operation -- from opposite ends. Read skew is the *reader's* half: even when writers are atomic, a SELECT visiting rows independently can straddle a writer's commit. MVCC's snapshot read fixes that. This page is the *writer's* half: even when readers snapshot, a writer that commits its rows at multiple timestamps is observable mid-flight. Real `BEGIN`/`COMMIT` fixes that. Either fix on its own leaves a hole; together they give "all-or-nothing visibility for a logical operation," which is what the bank test's invariant actually depends on.

## The Implementation

Five pieces. None of them are large; the largest is per-connection state.

### 1. Per-Connection Transaction State

Each TCP connection in the server's connection-handling loop gets its own state:

```cpp
struct PendingWrite {
    std::shared_ptr<small::schema::Table> table;
    std::string pk;
    std::vector<std::string> values;
};

struct TxnState {
    bool active = false;
    int64_t start_ts = 0;

    // Writes staged during the transaction, flushed atomically on COMMIT.
    std::vector<PendingWrite> pending_writes;

    // Per-row locks acquired during UPDATEs, held until COMMIT/ROLLBACK.
    std::vector<small::lock::LockManager::Lock> held_locks;
};
```

The connection-handling code passes a `TxnState&` into `handle_stmt`.

### 2. Real `BEGIN` / `COMMIT` / `ROLLBACK`

Replace the no-op `TRANSACTION_STMT` case:

```cpp
case PG_QUERY__NODE__NODE_TRANSACTION_STMT: {
    auto kind = stmt->transaction_stmt->kind;
    if (kind == PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_BEGIN) {
        if (txn.active) return absl::FailedPreconditionError("nested BEGIN");
        txn.active = true;
        txn.start_ts = now_ms();
    } else if (kind == PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_COMMIT) {
        return CommitTxn(txn);
    } else if (kind == PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_ROLLBACK) {
        return RollbackTxn(txn);
    }
    return EmptyBatch();
}
```

### 3. `UPDATE` Defers Its Write

Inside an active transaction, `UPDATE` no longer calls `WriteRow` directly. It:

1. Acquires the row's lock (transferred into `txn.held_locks`, not released at end of statement).
2. Reads the latest committed version under the lock.
3. Computes new column values in memory.
4. Stages a `PendingWrite` into `txn.pending_writes`.
5. Returns OK.

If no transaction is active (auto-commit), `UPDATE` behaves as before -- lock, read-latest, compute, write at the statement's `ts`, release lock.

> **This is where we get real two-phase locking.** Until this page, our per-row locks were acquired and released within a single UPDATE statement -- one lock at a time, no protocol to speak of. With locks now transferred into `txn.held_locks` and released only at `COMMIT`/`ROLLBACK`, a transaction's lock acquisitions form a proper *growing phase* (each subsequent `UPDATE` adds one lock without releasing any), and `COMMIT` is the *shrinking phase* (every lock released at once). That's strict 2PL with exclusive-only locks. The previous page's "per-row exclusive lock" was a degenerate case of the same protocol, with one lock per "transaction" and the growing/shrinking phases collapsed; multi-statement transactions are what make the protocol actually compose.

### 4. `COMMIT` Picks One `commit_ts` and Flushes

```cpp
absl::Status CommitTxn(TxnState& txn) {
    if (!txn.active) return absl::FailedPreconditionError("no active txn");

    auto db = small::rocks::RocksDBWrapper::GetInstance().value();

    // commit_ts must exceed start_ts AND every latest_version_ts of pks
    // we wrote, so that all our writes are lex-greater than any prior
    // committed version on those rows.
    int64_t commit_ts = txn.start_ts;
    for (const auto& w : txn.pending_writes) {
        int64_t latest = db->LatestVersionTs(w.table->name(), w.pk);
        if (latest >= commit_ts) commit_ts = latest + 1;
    }

    // Apply all writes at the same commit_ts. Since we hold the locks
    // for every pk in pending_writes, no other writer can interleave
    // here; the writes appear atomically from any reader's perspective.
    for (const auto& w : txn.pending_writes) {
        db->WriteRow(w.table, w.pk, w.values, commit_ts);
    }

    txn.active = false;
    txn.pending_writes.clear();
    txn.held_locks.clear();  // RAII releases the row locks
    return absl::OkStatus();
}
```

`ROLLBACK` is simpler -- discard `pending_writes`, release locks, no writes hit disk.

### 5. `SELECT` Inside a Transaction

A SELECT issued during an active transaction should use `txn.start_ts` as its snapshot, not `now()`. The transaction's reads then see a stable view from the moment of `BEGIN`, regardless of what other transactions commit in between:

```cpp
case PG_QUERY__NODE__NODE_SELECT_STMT: {
    int64_t snapshot_ts = txn.active ? txn.start_ts : now_ms();
    return small::execution::query(stmt->select_stmt, true, snapshot_ts);
}
```

For the bank test this isn't load-bearing -- the bank test's clients don't issue SELECTs inside their transfers -- but it's the correct behavior and falls out for free.

## How `commit_ts` Composes With the Bump Rule From the Previous Page

`WriteRow` from the previous page applies a per-row bump: `version_ts = max(caller_ts, latest_version_ts_for_pk + 1)`. Inside `CommitTxn` we picked `commit_ts` to be greater than every relevant `latest_version_ts`, so when `WriteRow` runs with `caller_ts = commit_ts`, the bump is a no-op -- every write lands at exactly `commit_ts`.

This is intentional. The bump rule keeps protecting auto-commit statements from out-of-order coordinator timestamps; transactions opt into a stronger property (all writes at one timestamp) by pre-computing `commit_ts` themselves.

## What This Buys (and What It Doesn't)

**Buys.** The bank test should pass. Each transfer commits at one `commit_ts`; SELECTs see either the entire transfer or none of it. The `9855..9988` deficit pattern goes away because there's no observable interval during which a transfer is half-applied.

**Doesn't.**

- **Write skew across transactions.** Two transactions that read overlapping rows and write disjoint rows can still produce schedules that aren't equivalent to any serial order. Postgres needed SSI to catch this; we'd need rw-conflict tracking. The bank test doesn't exercise it.
- **Cross-partition atomicity for one UPDATE.** A statement that writes rows on multiple partition owners still uses gRPC fan-out; if one peer's gRPC fails after another's succeeds, the cluster has a partial commit. Spanner uses 2PC for this; we don't have it. The bank test's transfer hits two partitions but as two separate UPDATEs, so each individual UPDATE is single-partition and the deferred-write/commit_ts mechanism above is sufficient.
- **Long-running transactions.** `pending_writes` buffers in memory; a million-row UPDATE inside a transaction would OOM. Production systems eventually spill to disk or use intent-based MVCC for this reason.

With this change the three-page arc through the distributed-database section now closes the bank test:

| Page | Anomaly | Fix |
|---|---|---|
| [Read Skew](./read_skew.md) | A SELECT sees a torn point-in-time view | MVCC snapshot reads + shared `snapshot_ts` across the scatter-gather |
| [Lost Updates](./lost_update.md) | Two writers' computations both based on the same stale pre-image | Per-row exclusive lock + read-latest under the lock + version_ts bump |
| Multi-Statement Transactions (this page) | A transfer's two halves commit at different times | Deferred writes with a single `commit_ts` at `COMMIT` |

Each page solves one anomaly; together they make the bank test green.
