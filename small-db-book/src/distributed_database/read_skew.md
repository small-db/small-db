# Read Skew, and Why MVCC Fixes It

## The Anomaly

**Behavior.** A `SELECT` returns a snapshot that no point in time ever actually held: some rows are observed before a concurrent writer's effect, others after. Invariants the writer was supposed to preserve appear violated to the reader, even though the writer committed atomically from its own point of view.

**Root cause.** Reading multiple rows is a non-atomic compound operation. The `SELECT` visits each row independently; a concurrent writer that touches more than one of those rows can commit *between* the reader's visits, so the read sees part of the writer's effect and not the rest.

**Does this happen in single-server databases?** Yes. Read skew is a concurrency anomaly, not a distribution anomaly. A single-server SQL database with no isolation control will exhibit it whenever two clients run concurrently: client A's `SELECT` is mid-scan when client B's `UPDATE` commits, and A reads some rows from before B and others from after. Berenson et al. catalogued it as anomaly A5A in 1995, well before partitioning was mainstream. Distribution makes the window longer (each row visit is a network round-trip rather than a memory read) and makes pessimistic fixes more expensive (locks have to coordinate across nodes), but the anomaly itself is fundamental to concurrency, not to distribution.

**Typical solutions.**

- **2PL with shared locks.** The `SELECT` takes a shared lock on every row it visits (plus gap locks for predicate `WHERE` clauses), held until commit. Concurrent writers block on those locks. Standard in pre-MVCC systems; MySQL InnoDB at `SERIALIZABLE` works this way. Correct, but reads block writes and vice versa.
- **MVCC with snapshot reads.** The `SELECT` pins a timestamp at the start; every row visit ignores versions written after that timestamp. Concurrent writes neither block the read nor affect it. Postgres (`REPEATABLE READ` and `SERIALIZABLE`), Oracle (default since forever), CockroachDB, Spanner, YugabyteDB, MySQL InnoDB at `REPEATABLE READ` -- the dominant approach in modern databases. **This is what the rest of this page implements.**
- **Live with it.** Postgres's default `READ COMMITTED` permits read skew openly; users who care wrap their reads in `BEGIN ... COMMIT` at a stronger isolation level. Pragmatic; pushes the burden up to the application.

For partitioned systems, MVCC has a structural advantage over locking: the snapshot timestamp is a single number that the coordinator can ship to every peer with no further coordination, and each peer answers independently. Spanner reinforces this with TrueTime to bound clock uncertainty across nodes for externally-consistent cross-node snapshots; CockroachDB and YugabyteDB use Hybrid Logical Clocks for the same goal. A lock-based equivalent would need a distributed lock manager, which is the cost (7) on the [lost-updates page](./lost_update.md) is paying.

## The Problem (in this system)

The Jepsen bank test from the [previous chapter](./bank_test.md) immediately failed when pointed at the MVP cluster. The first error in the recorded history came from a `SELECT id, balance FROM users` whose returned snapshot summed to `9916` rather than the expected `10,000` -- short by `84`.

Looking at the on-disk write timeline for that failure:

```
13:28:33.426  initial state (Eve = 2500, Charlie = 1500)   -- INSERT
13:28:33.474  Eve     = 2416    (T1 debit:  2500 - 84, on asia)
13:28:33.489  Charlie = 1584    (T1 credit: 1500 + 84, on europe)
```

T1 is the transfer `(from = 5, to = 3, amount = 84)` -- Eve in Japan paying Charlie in France. The Jepsen client wraps both legs of every transfer in a single transaction:

```sql
BEGIN;
UPDATE users SET balance = balance - amount WHERE id = from;
UPDATE users SET balance = balance + amount WHERE id = to;
COMMIT;
```

But the two `UPDATE`s land on *different nodes* (Eve's row lives on asia, Charlie's on europe), and they execute as two separate gRPC calls dispatched in sequence. The 15-millisecond gap between `.474` and `.489` is a real interval during which the cluster's state contains Eve's debit but not Charlie's credit -- a state that no point in time should ever have held atomically.

A `SELECT` whose execution lands in that gap reads each peer's *current* latest version. The pre-MVCC read path, in `src/rocks/rocks.cc`:

```cpp
for (it->Seek(scan_prefix); ...; it->Next()) {
    // parse pk from key, ignore the timestamp suffix
    result[pk] = nlohmann::json::parse(value).get<...>();
}
return result;
```

Asia returns Eve at `2416` (the post-debit version). Europe returns Charlie at `1500` (the credit hasn't been applied there yet). The coordinator concatenates the partition results:

```
{1: 1000, 3: 1500, 4: 3000, 5: 2416, 2: 2000}   -- sums to 9916
```

The bank checker compares against the invariant (`10,000`) and records a violation.

This is the textbook **read skew** anomaly (Berenson et al., *A Critique of ANSI SQL Isolation Levels*, 1995, anomaly A5A; also chapter 7 of *Designing Data-Intensive Applications*). Informally: a transaction reads row `A`, another transaction modifies both `A` and `B`, and then the first transaction reads `B`. The reader has observed `A` from before the writer and `B` from after the writer -- a state that no point in time ever actually held.

Two structural facts make this guaranteed to happen in our system, not just possible:

1. `ReadTable` returns "the latest version per primary key" with no notion of "as of time T." Each peer in a scatter-gather answers independently, using whatever happens to be on disk when its iterator runs.
2. The transfer's two halves are written by two separate `UPDATE` gRPCs to two separate partition owners. There is no atomic moment when both halves appear together; the gap between them is a real, observable interval.

The fix has to address both at once -- give the read path a notion of "as of time T," and give that T enough meaning that every peer in the scatter-gather agrees on which writes are visible.

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

> **MVCC is the reader's half of "atomic-looking transactions."** This page makes a single SELECT pin a snapshot and observe a coherent point in time. That alone is not enough for the bank test's invariant -- the reader could still observe a state in which a writer is half-committed, if the writer's two halves commit at two different timestamps. The matching half (the *writer's* contribution) is on a later page: real `BEGIN`/`COMMIT` semantics where every write inside a transaction shares one `commit_ts`, so a snapshot at any `S` either sees all of them or none. Both halves are needed for the application to perceive a transfer as atomic.

The write race does not. Two concurrent transfers can still both pick `ts` values, both read the same starting balances, and both write new versions -- each transaction is internally consistent, but they overwrite each other in the lexicographic ordering, and money is still lost. Closing that gap requires the next layer: write-conflict detection at commit, so that one of the two transactions is forced to abort. That's the subject of the next page.

> Historical note: commit `e88b510` previously introduced exactly this plumbing (snapshot reads + per-txn `commit_ts`) and commit `1ff91ee` reverted it. The work below is effectively re-landing that change with a cleaner story around what `ts` means and where it originates.
