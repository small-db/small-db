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

## The Solution Space

Six approaches close this anomaly, in roughly cost-ascending order. They are not interchangeable -- each makes different trade-offs about *when* writes hit storage, *how* timestamp pushes work, and *how much* infrastructure is required.

### 1. Deferred Writes at the Coordinator

Buffer all of a transaction's writes in per-connection state. On `COMMIT`, pick one `commit_ts` and flush every buffered write at that timestamp. Locks acquired during UPDATE are held until COMMIT.

| | |
|---|---|
| **Implementation** | Small. A per-connection `TxnState` plus `BEGIN`/`COMMIT` handling in stmt_handler. |
| **Where writes live before commit** | In coordinator memory only. Nothing on disk. |
| **Granularity** | Per-transaction, all writes share one `commit_ts`. |
| **Cross-node** | Single-coordinator only. A transaction whose buffered writes span multiple partition owners still has to dispatch them sequentially at COMMIT time. |
| **Concurrency cost** | Row locks held from first UPDATE until COMMIT (proper 2PL). |
| **Client visible** | No aborts in the normal case; long transactions hold locks. |

Simplest of the family. Memory grows with the write set, so a million-row UPDATE inside one transaction would OOM the coordinator. For OLTP workloads (a few rows per transaction), this is fine. **This is what we implement.**

### 2. Eager Writes with Intents + Txn Record

Each write lays down a provisional *intent* on the row at write time, tagged with the writing transaction's ID. A separate txn record holds `{status, commit_ts}`. `COMMIT` is a single update of the txn record; readers consult it on demand. Intent resolution (rewriting intents to canonical version entries) runs lazily.

| | |
|---|---|
| **Implementation** | Medium-large. Intent encoding, txn record store, resolution, push protocol. |
| **Where writes live before commit** | On the row, as provisional records. |
| **Granularity** | Per-row intent + per-txn record. |
| **Cross-node** | Yes. The txn record is one logical entity; intents on different nodes still flip atomically by the single record update. |
| **Concurrency cost** | Readers may consult the txn record on every encounter with an intent; resolution is amortized. |
| **Client visible** | Aborts on write-write conflict (push or wait). |

CockroachDB and YugabyteDB. Most flexible model in the family -- handles multi-statement atomicity, cheap timestamp push, and cross-node commit, all at the cost of more machinery. The next chapter, [Write Intents](./write_intents.md), is dedicated to this approach.

### 3. Eager Writes with a Commit Log (Postgres XID Model)

Each write lands on disk immediately, tagged with the writing transaction's XID. A separate commit log records each XID's status. Readers consult the commit log on every tuple visit to learn whether the XID committed or aborted, and at what time.

| | |
|---|---|
| **Implementation** | Large. Commit log infrastructure, vacuum to GC dead tuples. |
| **Where writes live before commit** | On the row, tagged with XID. |
| **Granularity** | Per-XID; one log entry per txn. |
| **Cross-node** | The commit log is single-machine. To extend across nodes, layer 2PC. |
| **Concurrency cost** | Commit log lookups on every read; vacuum overhead. |
| **Client visible** | Aborts on serialization failures. |

Postgres's design. Atomicity from "one commit log entry flips visibility for every write the txn made." Heavier than intents in steady state but cheaper for very short transactions (the commit log is tiny). Doesn't naturally extend across coordinators -- Postgres is single-server, and its replication is asynchronous.

### 4. Two-Phase Commit (2PC)

Layered on top of any per-node transaction model. The coordinator drives a `PREPARE` round across participants (each participant durably promises to commit if asked); a `COMMIT` round finalizes. Failure during commit is recovered via the durable PREPARE state.

| | |
|---|---|
| **Implementation** | Large. Coordinator state, participant state, recovery, timeouts. |
| **Where writes live before commit** | At each participant, in a "prepared" but uncommitted state. |
| **Granularity** | Per-transaction, distributed. |
| **Cross-node** | Yes. This is its purpose. |
| **Concurrency cost** | Two extra network round trips per commit; locks held longer (through PREPARE). |
| **Client visible** | Aborts; long tail latency on participant failure. |

Spanner's mechanism for cross-Paxos-group transactions. Solves multi-coordinator atomicity but doesn't by itself solve the *single-coordinator* multi-statement problem -- it presupposes that. Mention here only because for cross-coordinator transactions, no other approach in this list is sufficient on its own.

### 5. Single-Leader Serialization

Designate one node as the leader for each range; route every transaction touching that range through the leader. The leader's clock is the only one used; commits are naturally ordered.

| | |
|---|---|
| **Implementation** | Medium. Range-leader infrastructure, leader election, leader fail-over. |
| **Where writes live before commit** | At the leader, in its uncommitted state. |
| **Granularity** | Per-range. Cross-range transactions need 2PC on top. |
| **Cross-node** | Per-range only. |
| **Concurrency cost** | Extra hop to leader for non-local clients. |
| **Client visible** | Latency on leader fail-over. |

Spanner's per-Paxos-group leader model and CockroachDB's per-Raft-range leader. Sidesteps cross-coordinator clock issues entirely for single-range transactions. Heavier than deferred-writes for a single-server build, lighter for an already-replicated system.

### 6. Push the Problem to the Client

Refuse to support multi-statement transactions. Clients must compose multi-row mutations into one SQL statement (e.g., `WITH ... UPDATE ... RETURNING`, `MERGE`, or stored procedures). The single-statement engine handles atomicity; the application avoids the multi-statement case.

| | |
|---|---|
| **Implementation** | Zero. |
| **Where writes live before commit** | N/A; no commit boundary distinct from statement boundary. |
| **Granularity** | Per-statement only. |
| **Cross-node** | Whatever the single-statement engine does. |
| **Concurrency cost** | None added. |
| **Client visible** | Clients must restructure code; some workloads cannot be expressed this way. |

Worth naming as the option that exists. Some embedded databases (early SQLite, key-value stores with batch APIs) work like this. For our SQL surface area, declining to support `BEGIN`/`COMMIT` would also decline a contract every Postgres client expects.

## Comparison

| Approach | Code change | Memory at coordinator | Where pre-commit writes live | Cross-node | Aborts to client? |
|---|---|---|---|---|---|
| 1. Deferred writes (coord buffer) | Small | O(write set) | Coordinator only | Single-coord | No |
| 2. Intents + txn record | Medium-large | O(1) | On-row provisional | Yes | Yes |
| 3. XID + commit log | Large | O(1) | On-row tagged | No (alone) | Yes |
| 4. 2PC | Large | O(prepared txns) | At each participant, prepared | Yes | Yes |
| 5. Single-leader | Medium | O(in-flight at leader) | At leader | Per-range | On fail-over |
| 6. Push to client | Zero | -- | -- | -- | -- |

Reading the matrix:

- **Memory pressure shapes the choice for long transactions.** Deferred writes (1) buffers everything in coordinator memory; intents (2) and commit-log (3) free that memory by writing to disk eagerly. For OLTP-shaped workloads (small transactions, high throughput), all three are fine; for analytics-shaped workloads, (2) or (3) are the only viable answers.
- **Push-friendliness comes from indirection.** Approaches (2) and (3) put the commit decision in a separate central record (the txn record or the commit log), so a `commit_ts` push is a single update of that record. Approaches (1) and (5) don't have that indirection — push there means re-stamping every flushed write or rewriting buffered values.
- **Cross-node atomicity is mostly orthogonal.** (4) is the answer for cross-node, layered on top of any of the others. We don't need it for the bank test (each transfer's UPDATEs hit separate single-row partitions, and our bank-test client doesn't issue a single UPDATE that spans partitions).
- **All but (6) require some commit-time work.** The trade is *what* that work is — a buffer flush (1), a record update (2/3), a coordination round (4), or a leader hop (5).

The cheapest answer that fits small-db's current shape is (1). The next chapter, [Write Intents](./write_intents.md), is the principled long-term answer (2).

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

    // Pick one commit_ts for every write of this transaction.
    int64_t commit_ts = now_ms();
    if (commit_ts < txn.start_ts) commit_ts = txn.start_ts;

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

## What This Buys (and What It Doesn't)

**Buys.** Each transaction's writes share one `commit_ts`. A snapshot read at any `S` either sees every write of one transaction or none of them. For a single-coordinator transaction, the multi-statement atomicity contract holds.

**Doesn't.**

- **Shadowed writes from cross-coordinator commits.** Two transactions running concurrently on different coordinators each pick their own `commit_ts`. If both happen to write the same row, the chronologically-later writer can land at a lex-smaller `version_ts` and be silently shadowed -- even though everything on this page is still doing its job. The single-`commit_ts`-per-transaction property is preserved; what's broken is *across* transactions, not within one. That's a separate anomaly with its own chapter -- [Shadowed Writes](./shadowed_writes.md).
- **Write skew across transactions.** Two transactions that read overlapping rows and write disjoint rows can still produce schedules that aren't equivalent to any serial order. Postgres needed SSI to catch this; we'd need rw-conflict tracking. The bank test doesn't exercise it.
- **Cross-partition atomicity for one UPDATE.** A statement that writes rows on multiple partition owners still uses gRPC fan-out; if one peer's gRPC fails after another's succeeds, the cluster has a partial commit. Spanner uses 2PC for this; we don't have it. The bank test's transfer hits two partitions but as two separate UPDATEs, so each individual UPDATE is single-partition.
- **Long-running transactions.** `pending_writes` buffers in memory; a million-row UPDATE inside a transaction would OOM. Production systems eventually spill to disk or use intent-based MVCC for this reason.

| Page | Anomaly | What that page's fix delivers |
|---|---|---|
| [Read Skew](./read_skew.md) | A SELECT sees a torn point-in-time view | MVCC snapshot reads + shared `snapshot_ts` across the scatter-gather |
| [Lost Updates](./lost_update.md) | Two writers' computations both based on the same stale pre-image | Per-row exclusive lock + read-latest under the lock |
| Multi-Statement Transactions (this page) | A transfer's two halves commit at different times | Deferred writes with a single `commit_ts` at `COMMIT` |
| [Shadowed Writes](./shadowed_writes.md) | A chronologically-later commit lands at a smaller `version_ts` and is invisible | (still open in the codebase; see that chapter for the menu of fixes) |

Each page closes one anomaly. The bank test exercises all four; the first three pages get us most of the way, but the fourth is what makes the totals balance under heavy concurrency.
