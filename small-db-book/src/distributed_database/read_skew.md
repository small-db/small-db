# Read Skew, and Why MVCC Fixes It

## The Problem

The standard name for this anomaly in the database literature is **read skew** (Berenson et al., *A Critique of ANSI SQL Isolation Levels*, 1995; also chapter 7 of *Designing Data-Intensive Applications*). Informally: a transaction reads row `A`, another transaction modifies both `A` and `B`, and then the first transaction reads `B`. The reader has now observed `A` from before the writer and `B` from after the writer -- a state that no point in time ever actually held.

The Jepsen bank test issues two kinds of operations against the cluster: `transfer` and `read`. A transfer is the two-statement sequence

```sql
BEGIN;
UPDATE users SET balance = balance - amount WHERE id = from;
UPDATE users SET balance = balance + amount WHERE id = to;
COMMIT;
```

and a read is

```sql
SELECT id, balance FROM users;
```

The bank checker walks every read in the recorded history and asserts that the balances sum to `10,000`. Because every transfer subtracts `amount` from one account and adds it to another, the sum is invariant *as long as each transfer is observed atomically* -- a reader must either see both legs or neither.

Today's read path cannot promise that. `ReadTable` (`src/rocks/rocks.cc`) returns "the latest version per primary key" with no notion of "as of time T":

```cpp
for (it->Seek(scan_prefix); ...; it->Next()) {
    // parse pk from key, ignore the timestamp suffix
    result[pk] = nlohmann::json::parse(value).get<...>();
}
return result;
```

If a `SELECT` runs concurrently with a transfer, the iterator can visit `from`'s post-debit version and `to`'s pre-credit version. The returned snapshot is missing `amount` from one side, the sum is `10,000 - amount`, and the bank checker fails. This is read skew in its textbook form: row `from` is observed at a later point in time than row `to`.

The cross-node case is worse. A transfer between Alice (Germany / `eu`) and Bob (USA / `america`) writes one half on each node's RocksDB. The scatter-gather `SELECT` queries both nodes; one peer can have applied its half while the other hasn't, and the assembled result is skewed even if each individual peer was consistent in isolation. The fix has to span both partitions.

## Why MVCC

Read skew happens because the read path always sees the *current* state of storage, and "current" advances continuously while the read is in flight. The fix is to make the read pin a single point in time and refuse to see anything later. This guarantee is called **snapshot isolation**, and the standard mechanism for providing it is multi-version concurrency control (MVCC).

Two simpler alternatives exist:

- **Lock the table during reads.** Correct, but kills concurrency: every read blocks every transfer and vice versa. Across a partitioned table it requires distributed lock coordination.
- **Lock per row.** Cheaper, but a `SELECT` over the whole table still has to take five locks (one per account) before reading any of them, and a transfer takes two; the inter-leaving rules get fiddly fast, and cross-node lock acquisition is its own distributed-systems problem.

MVCC takes a different shape. Instead of preventing concurrent access, it **versions** every write. Each row mutation produces a new immutable version tagged with a timestamp. A read picks one timestamp at the start, and only ever sees versions tagged at or before it. Two properties fall out:

1. **Readers never block writers, writers never block readers.** No locks; no waiting.
2. **A single `snapshot_ts` shared across nodes makes the scatter-gather consistent.** When the coordinator sends the same `snapshot_ts` to every peer, every peer filters its local data the same way, and the assembled result is a snapshot of the whole partitioned table at one point in time.

Property (2) is the one that decides it for us. Without distributed locking, MVCC is essentially the only mechanism that makes a *partitioned* SELECT observe a consistent view across nodes. Locks would have to coordinate across machines; timestamps don't -- once the coordinator has chosen `T`, every peer can answer independently.

The cost is real but bounded: storage grows with version count, and old versions need eventual GC. We accept that.

## How: Implementing MVCC

The implementation has three pieces: a storage format that can hold multiple versions, a clear definition of what the timestamp means, and the plumbing that gets the right timestamp into every read and write.

### 1. Storage Format (already landed)

Every row write produces its own RocksDB key, suffixed with a millisecond timestamp:

```
/{table}/{pk}/{ts}  →  {"col1": "...", "col2": "..."}
```

`WriteRow` (`src/rocks/rocks.cc`) builds the key, generates a zero-padded 20-digit timestamp, and `Put`s a new entry. Old versions are never overwritten. The zero-padding matters: it makes RocksDB's lexicographic order coincide with chronological order, so a prefix scan over `/{table}/{pk}/` returns versions oldest-first.

This is the foundation. By itself it changes nothing about behavior -- the engine still writes "now" and reads "latest" -- but every row history is now addressable.

### 2. Defining `ts`

Before wiring the timestamp anywhere, we pin down what it means with a single sentence:

> **`ts` is the start time of the transaction that produced (or is reading) this row version.**

Not wall-clock-at-`Put`-time, not commit time, not arrival-at-the-storage-layer time. It is a property of the *transaction*, not of the individual storage call. Two writes from the same transaction share the same `ts`. A read started at `T` sees only versions with `ts <= T`.

This definition is what makes the storage format meaningful:

- All rows touched by one transaction become visible together (atomic in time).
- A reader at `T` sees a consistent snapshot -- every row as it stood at `T`.
- The lex order of versions per `pk` becomes a real history, not just a write log.

### 3. Plumbing `ts` Through Writes and Reads

The timestamp has to come from one well-defined place: the **coordinator node**, the node that received the user's SQL request. Whoever owns the connection owns the transaction.

**Writes (`INSERT` / `UPDATE`).** The coordinator picks `ts` once when it accepts the statement, then passes that same `ts` down through every step:

```
client ──SQL──▶ coordinator (picks ts)
                    │
                    ├─ for each row:
                    │     dispatch via gRPC (request carries ts)
                    │       │
                    │       ▼
                    │   partition owner: WriteRow(table, pk, values, ts)
                    │       └─ key: /{table}/{pk}/{ts}
                    ▼
                  return OK
```

Every row written for that statement lands under the *same* `ts`. The `dispatch` request gets a new field carrying it. `WriteRow` gains a `ts` parameter and stops calling `now()` itself.

**Reads (`SELECT`).** Same shape: the coordinator picks a `snapshot_ts` when the query arrives, and that `snapshot_ts` rides along with the scatter-gather to every peer:

```
client ──SELECT──▶ coordinator (picks snapshot_ts)
                       │
                       ├─ scatter to each partition owner
                       │     (request carries snapshot_ts)
                       │       │
                       │       ▼
                       │   peer: ReadTable(table_name, snapshot_ts)
                       │       └─ ignore versions with version_ts > snapshot_ts
                       │       └─ for visible versions, latest-wins per pk
                       ▼
                  concat batches and return
```

`ReadTable` gains a `snapshot_ts` parameter and skips versions newer than it. Every peer in a scatter-gather filters by the same number, so the assembled result is internally consistent across partitions.

## What This Buys (and What It Doesn't)

With this plumbing in place, the bank test's read operation becomes correct. A `SELECT id, balance FROM users` will see every account as of one consistent point in time. Read skew on the SELECT path is gone.

The write race does not. Two concurrent transfers can still both pick `ts` values, both read the same starting balances, and both write new versions -- each transaction is internally consistent, but they overwrite each other in the lexicographic ordering, and money is still lost. Closing that gap requires the next layer: write-conflict detection at commit, so that one of the two transactions is forced to abort. That's the subject of the next page.

> Historical note: commit `e88b510` previously introduced exactly this plumbing (snapshot reads + per-txn `commit_ts`) and commit `1ff91ee` reverted it. The work below is effectively re-landing that change with a cleaner story around what `ts` means and where it originates.
