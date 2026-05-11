# Lost Updates

## The Anomaly

**Behavior.** Two transactions both successfully commit writes to the same row -- both report success to their clients -- yet one transaction's effect is silently lost on disk. The system's invariants drift, even though every individual transaction looked correct from its own point of view.

**Root cause.** Two concurrent writes to the same row land at version_ts whose lex order disagrees with their commit order, so the on-disk lex-largest version is no longer the latest commit and one writer's effect is silently shadowed. Two sub-mechanisms produce this shape: without serialization, both writers read the same pre-image and write back values computed from a state that did not include the other's commit; with version_ts assigned by per-coordinator clocks, even a writer that read the other's commit can land at a smaller version_ts and be shadowed by the earlier-committed write. Either way, there is no equivalent serial schedule -- neither "T_a then T_b" nor "T_b then T_a" matches what storage now contains.

**Does this happen in single-server databases?** Yes. Lost update is a concurrency anomaly, not a distribution anomaly. Berenson et al. catalogued it as anomaly P4 in 1995, and the canonical textbook example is two ATM withdrawals racing on the same account: both read the balance, both subtract, both write back, one withdrawal silently vanishes. A single-server SQL database without write locks or version-check semantics exhibits it as readily as a distributed one. Distribution makes the window longer (network round-trips replace memory accesses) and the recovery harder (more places to push intermediate state), but it does not introduce the problem.

**Typical solutions.** Three families, each with multiple real-system instantiations:

- **Pessimistic locking.** The writer acquires an exclusive row lock from before its read until after its commit; concurrent writers wait. MySQL InnoDB and Postgres both take row-level X locks for `UPDATE`. Spanner uses strict 2PL backed by Paxos leaders. CockroachDB and YugabyteDB use *transactional intents* -- provisional records on the row that act as locks for concurrent writers, so the mechanism is operationally pessimistic even when the system describes itself as optimistic.
- **Optimistic conflict detection.** Read at a snapshot, defer conflict checking to commit, abort the loser. Postgres `REPEATABLE READ` and `SERIALIZABLE` (the latter via SSI), Oracle, application-level `UPDATE ... WHERE version = X` patterns (Hibernate, Rails), DynamoDB `ConditionExpression`, etcd `compareAndSet`, Cassandra lightweight transactions (Paxos-backed CAS). Cheap when contention is low; abort rates climb sharply under hot rows.
- **Atomic compound operations.** Sidestep the read-modify-write entirely by exposing the operation as a native primitive. Redis `INCR`/`DECR`, RocksDB merge operators, Cassandra counters, DynamoDB's `UpdateExpression` with `ADD`. Constrained to operations the engine knows how to combine (counters, sets, append) but truly wait-free when applicable -- and a natural fit for the bank test specifically, since debits and credits are both increments.

For partitioned systems like small-db, lost-update prevention is fundamentally a *local* problem on the row's owner node: the row lives on one node, the contention is on that node, and any single-node mechanism is sufficient. Cross-node coordination only becomes necessary when one transaction *writes more than one row across partitions*, which is the separate problem of multi-row atomicity.

The rest of this page surveys ten variants of those three families in detail and compares them, then tours how production systems pick among them.

## The Problem (in this system)

The MVCC plumbing from the [previous chapter](./read_skew.md) gave readers a consistent snapshot, but the bank test still failed. The on-disk evidence from one run:

```
13:28:33.489  Charlie = 1584    (earlier transfer's credit)
13:28:33.623  Charlie = 1542    (T5 debit: 1584 - 42)   ← process 0
13:28:33.627  Charlie = 1519    (T4 debit: 1584 - 65)   ← process 2
13:28:33.636  Eve     = 2530    (T5 credit applied)
13:28:33.638  Alice   =  972    (T4 credit applied)
```

Two concurrent transfers -- T5 (3→5, amount 42) and T4 (3→1, amount 65) -- both touched Charlie. Both `UPDATE` statements ran the read-modify-write sequence in `src/execution/update.cc`:

1. `ReadTable(table, snapshot_ts)` returns Charlie at `1584`.
2. Compute the new balance in memory (`1584 - 42` and `1584 - 65` respectively).
3. `WriteRow(table, pk, new_values, ts)` appends a new version.

Both transactions read `1584` because both started before either committed. T5 appended `1542` at version_ts ≈ `.623`, T4 appended `1519` at version_ts ≈ `.627`. Lex order on the version_ts suffix means T4's `1519` shadows T5's `1542` for all subsequent reads. **T5's debit is permanently lost on disk.**

After both transfers reported `:ok` to their clients, the cluster's effective state is: account 5 has T5's credit (+42), account 3 has only T4's debit (-65) -- T5's debit has vanished. The total is `10042`, and it stays `10042` until another race shifts it.

This is the textbook **lost update** anomaly (Berenson et al. 1995, *A Critique of ANSI SQL Isolation Levels*, anomaly P4). MVCC alone does not prevent it. MVCC's read-side filter -- skip versions with `version_ts > snapshot_ts` -- says nothing about what happens when two writers concurrently modify the same row.

## What "Fixing It" Has to Guarantee

The invariant that must hold: **for every committed transaction T that wrote row R, the value T wrote was computed from a pre-image that was still the latest committed version at the moment T's write took effect.** Equivalently: between T's read of R and T's write of R, no other committed write to R may intervene without one of the transactions aborting.

There is no shortage of ways to enforce this. The rest of this page surveys them: how each works, what it costs, and where each falls short. The next page picks one and implements it.

## The Solution Space

### 1. Per-Node Table-Level Mutex

The bluntest fix. Wrap the entire local-execution block of `update.cc` (the read-modify-write between lines 109-203) in a `std::mutex` keyed by table name. All `UPDATE` gRPCs received by one node serialize on that table.

| | |
|---|---|
| **Implementation** | ~10 lines in `update.cc` |
| **Granularity** | One mutex per `(node, table)` |
| **What it fixes** | Lost updates within a single node |
| **Cross-node** | Not needed: small-db's LIST partitioning means every row lives on exactly one node, so the contention is necessarily local |
| **Concurrency cost** | All `UPDATE`s on a table serialize on the owner, even those touching disjoint rows |
| **Client visible** | No aborts; transactions wait their turn |

Works precisely because of the partitioned architecture: two concurrent updates to Charlie both arrive on europe, the mutex orders them, and the second one's `ReadTable` now sees the first's just-committed version. The cost is pessimism: `UPDATE ... WHERE id = 1` blocks `UPDATE ... WHERE id = 5` even though they touch disjoint rows on the same partition owner.

### 2. Per-Row Pessimistic Lock

A lock manager keyed by `(table, pk)`. Each `update()` acquires the row's lock before reading the pre-image and releases it after the write commits. Updaters of different rows proceed in parallel; updaters of the same row queue.

Strict two-phase locking (2PL) holds the lock until *transaction* commit, not statement commit, so that all of a transaction's locks are released together at the end of its shrinking phase. In a system with real multi-statement transactions, the two scopes differ: a transaction may write row `A`, then later read row `B`, and 2PL guarantees no concurrent writer can move row `A` between those two events. small-db has no such transactions today -- `BEGIN`/`COMMIT` are no-ops, every `UPDATE` auto-commits -- so "statement commit" and "transaction commit" coincide, and a lock held for one statement's read-modify-write satisfies 2PL coincidentally. The moment `BEGIN`/`COMMIT` start carrying real semantics, this scheme stops being 2PL; preventing write skew or read-then-write hazards across statements would then need a transaction object that tracks held locks and releases them at COMMIT/ROLLBACK.

| | |
|---|---|
| **Implementation** | New `LockManager` module; ~50-100 lines |
| **Granularity** | Per `(table, pk)` |
| **What it fixes** | Lost updates with full per-row parallelism |
| **Cross-node** | Same answer as (1): not needed in our model |
| **Concurrency cost** | Disjoint-row workloads run fully in parallel; readers still pass through under MVCC |
| **Client visible** | No aborts; queueing only |

Two real complications. **(a) Lock acquisition order.** `UPDATE ... WHERE balance < 100` doesn't know which pks it'll touch until the WHERE evaluates. Standard 2PL acquires locks as rows are encountered during the read, which works but means lock acquisition is interleaved with computation. **(b) Deadlocks.** As long as `update()` only ever holds one lock at a time (single-row update path), deadlocks are impossible. The moment a future change introduces "lock all matching rows then write" semantics, we're back to needing deadlock detection or an ordering rule.

### 3. RocksDB `OptimisticTransactionDB`

RocksDB ships with `OptimisticTransactionDB` (and the heavier `TransactionDB`). Wrap reads and writes inside a transaction object; on `Commit()`, RocksDB checks whether any key the transaction read or wrote was modified by another committed transaction since this one's snapshot. If yes, `Commit()` returns `Status::Busy` and the caller must retry.

| | |
|---|---|
| **Implementation** | Switch DB type in `RocksDBWrapper`; replace `Put`/`Get` with the transaction API; surface aborts |
| **Granularity** | Per-key, in storage |
| **What it fixes** | Lost updates with optimistic semantics; storage layer handles conflict detection |
| **Cross-node** | Doesn't help; the txn is local to one RocksDB instance |
| **Concurrency cost** | Best for low-contention; abort rate climbs sharply with hot rows |
| **Client visible** | Aborts surface as `Status::Busy` -- caller must retry or fail |

Pushes the problem onto storage's existing primitives. The abort path has to be plumbed through `update.cc` → `UpdateServiceImpl::Update` → coordinator → client. The bank test's `transfer` op currently doesn't retry on conflict (it just records `:fail`), so aborts would change *what* the test sees -- valid but failed transfers instead of invariant-violating successful ones.

### 4. Application-Level OCC: First-Committer-Wins via Version Check

Pure-MVCC analog of (3), without depending on RocksDB's transaction primitives. At commit time, for each row T wrote, scan the pk's versions and check whether any version exists with `version_ts > T.start_ts`. If yes, abort T.

| | |
|---|---|
| **Implementation** | Extra scan per row at commit; abort plumbed to client; ~30-50 lines |
| **Granularity** | Per-key, in our code |
| **What it fixes** | Lost updates with optimistic semantics |
| **Cross-node** | Same as (3): local to one node |
| **Concurrency cost** | One extra short scan per write; cheap under low contention |
| **Client visible** | Aborts |

A subtlety. The check works only if the *new* version's timestamp is the commit time (`now()` at commit), not the transaction's start time. Otherwise two transactions starting at `t_a < t_b` would both see "no version newer than my start" and both commit. Adopting (4) therefore requires distinguishing `start_ts` (used as snapshot for reads) from `commit_ts` (used as the version_ts of writes). That breaks the "one `ts` per transaction" symmetry the previous chapter just established, and re-introduces the `commit_ts` plumbing that the reverted `e88b510` had.

Multi-row updates have a partial-write hazard if checks happen row-by-row -- batching all version checks before any writes solves it but adds a serialization point.

### 5. Compare-and-Set in `WriteRow`

A narrower form of (4). The caller of `WriteRow` passes the `version_ts` of the row it read; `WriteRow` atomically checks that no version newer than that exists for the pk and rejects otherwise. The caller can decide whether to retry inside `update()` or surface a failure.

| | |
|---|---|
| **Implementation** | `WriteRow` learns to check the pre-image version; ~20 lines |
| **Granularity** | Per-key |
| **What it fixes** | Lost updates with retry buried inside `update()` |
| **Cross-node** | Local |
| **Concurrency cost** | Same as (4); retries are local rather than client-driven |
| **Client visible** | Looks like waiting; retries internal |

For single-row updates this is clean. For multi-row updates -- where one row's CAS may succeed and another's may fail -- the engine has to decide whether to undo earlier writes, retry, or surface a partial-commit error. Multi-row UPDATE doesn't exist yet, but designing for it means picking that policy now.

### 6. Serializable Snapshot Isolation (SSI)

The principled answer to lost updates *and* read skew *and* write skew, in one mechanism. Track each transaction's read set and write set; at commit time, check for "dangerous structures" (rw-antidependency cycles in the conflict graph); abort any transaction that would close such a cycle. Postgres's SERIALIZABLE level is SSI.

| | |
|---|---|
| **Implementation** | Substantial -- read-set tracking, write-set tracking, predicate locks, rw-conflict graph; weeks-to-months |
| **Granularity** | Per-key conflict graph across all in-flight transactions |
| **What it fixes** | Lost updates, read skew, write skew, all phantoms -- full serializable isolation |
| **Cross-node** | Becomes the dominant cost; possible but heavy |
| **Concurrency cost** | Lower than 2PL in low-contention; comparable under high-contention |
| **Client visible** | Aborts under *any* serialization conflict, not only writes |

Doing this well presupposes multi-statement transactions. Without `BEGIN`/`COMMIT`, "transaction" is just "statement" and SSI's machinery is overkill. SSI is the answer if and when small-db acquires real transactions; it's premature otherwise.

### 7. Distributed Lock Manager + 2PC

The fully-general answer for a partitioned system. A separate lock service (or a leader-elected lock-holder per range) coordinates locks across nodes; multi-row writes use two-phase commit to atomically apply on all owners.

| | |
|---|---|
| **Implementation** | A new service, leader election, lock leases, 2PC; large |
| **Granularity** | Per-key, distributed |
| **What it fixes** | Lost updates *across nodes* for multi-row updates spanning partitions |
| **Cross-node** | Yes -- this is what it's for |
| **Concurrency cost** | Network round trips per commit |
| **Client visible** | Aborts on lock conflict; tail latency from coordination |

Out of proportion to the failure we have. The bank test's transfer is two single-row UPDATEs, not one two-row UPDATE -- nothing in the workload writes across partitions in a single statement. Worth naming as the option that exists; not worth writing.

## Comparison

| Approach | Correctness | Code change | Concurrency under contention | Aborts to client? | Granularity | Cross-node? |
|---|---|---|---|---|---|---|
| 1. Per-node table mutex | Yes | Tiny | Worst (table-wide serial) | No | Per-table | N/A |
| 2. Per-row pessimistic lock | Yes | Small | Good (per-row serial) | No | Per-row | No (sufficient) |
| 3. RocksDB OptDB | Yes | Medium | Good | Yes | Per-row | No |
| 4. App-level OCC | Yes | Medium | Good | Yes | Per-row | No |
| 5. CAS in `WriteRow` | Yes | Small | Good | Internal retry | Per-row | No |
| 6. SSI | Yes (+ skew) | Huge | Good | Yes | Per-key graph | Extensible |
| 7. DLM + 2PC | Yes (+ multi-row) | Huge | Worst | Yes | Per-key, distributed | Yes |

Reading the matrix:

- **All seven options close failure mode 1.** The differences are cost, blast radius, and what additional anomalies they incidentally fix.
- **Pessimistic vs. optimistic** is the first axis. (1), (2), and (7) make conflicting writers *wait*; (3), (4), (5), (6) make them *abort* and retry. Pessimistic suits high-contention workloads; optimistic suits low-contention.
- **In-process vs. in-storage** is the second axis. (3) puts conflict detection in RocksDB; (4) puts it in our code. Same external behavior; different ownership of the bookkeeping.
- **Solves more than asked** is the third axis. (6) and (7) fix problems we haven't surfaced yet. Adopting them now is paying for capacity we don't currently use.
- **Cross-node**, in our model, is a non-question for this failure mode. Each row has exactly one owner; "concurrent writers to the same row" is necessarily a same-node phenomenon. (7)'s only real value is multi-row writes that span partitions, which we don't have.

## Implementing the Per-Row Lock

We pick option (2): per-row pessimistic locking. The reasoning, briefly: it fixes the failure with no client-visible aborts, leaves snapshot reads alone (MVCC keeps doing what it does), uses a fully local mechanism on each partition owner, and is small enough to land in one commit. (1) is the bluntest version of the same idea; (3)-(5) push the same problem onto an abort-and-retry path the bank test isn't structured to handle; (6) and (7) solve more than we're asking.

A scope note up front: the lock is held for the duration of one `UPDATE` statement's read-modify-write and released when that statement returns. Because `BEGIN`/`COMMIT` are no-ops in small-db today, statement boundary and transaction boundary are the same event, and this scheme satisfies strict 2PL trivially. We are not building 2PL in the general sense -- we are building a per-statement row mutex that *happens to be* 2PL-equivalent for the workload we have. When real multi-statement transactions arrive, the two scopes diverge and this scheme would have to be extended into actual 2PL (a transaction-scoped lock list released at COMMIT/ROLLBACK) to keep its current guarantees.

The implementation has three pieces: a lock manager, a tweak to the `update()` flow that takes the lock and reads "latest" instead of "at snapshot," and a small rule that keeps the on-disk version order monotonic per row.

### The Lock Manager

A new module at `src/lock/lock_manager.{h,cc}`. Process-wide singleton, keyed by `(table_name, pk_string)`:

```cpp
using RowKey = std::tuple<std::string, std::string>;  // (table, pk)

struct RowKeyHash {
    size_t operator()(const RowKey& k) const {
        size_t h1 = std::hash<std::string>{}(std::get<0>(k));
        size_t h2 = std::hash<std::string>{}(std::get<1>(k));
        return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
    }
};

class LockManager {
   public:
    static LockManager* GetInstance();

    // RAII handle: acquires on construction, releases on destruction.
    class Lock {
        ...
    };
    Lock Acquire(const std::string& table, const std::string& pk);

   private:
    std::mutex map_mu_;
    std::unordered_map<RowKey, std::shared_ptr<std::mutex>, RowKeyHash> locks_;
};
```

Using a `tuple<string, string>` instead of a composed `"table\0pk"` string says directly what the key *is*: the pair `(table, pk)`. The `RowKeyHash` is a small custom hasher because the standard library doesn't ship `std::hash<std::tuple<...>>` out of the box; the constant `0x9e3779b9` is the standard hash-combine multiplier (the reciprocal of the golden ratio in 32-bit fixed point), the same one Boost uses.

`Acquire` looks up (or creates) the per-row mutex under `map_mu_`, then locks the per-row mutex itself outside `map_mu_` so the map's mutex is held only briefly. Exclusive locks only -- there is no shared-lock variant; readers go through MVCC and don't touch the lock manager. Map entries live forever for now; a real workload would eventually need a refcounted GC, but the bank test has 5 keys and that doesn't matter yet.

### The Updated `update()` Flow

In `src/execution/update.cc`, the local-execution path (`dispatch=false`, the receiver-side handler) becomes:

```
1. Parse the WHERE clause to extract the target pk.
   (Bank test always has WHERE id = X. Anything else asserts and crashes.)
2. Acquire LockManager::Acquire(table, pk).        ← RAII; held to end of scope
3. Read the latest committed version of pk
   (NOT a snapshot read; ignore the coordinator's ts).
4. Compute the new column values in memory.
5. Write the new version at version_ts =
       max(coordinator_ts, latest_version_ts_for_pk + 1).
6. Lock released automatically as the RAII handle goes out of scope.
```

The lock is held across the read-modify-write, so two concurrent UPDATEs to the same pk cannot interleave their reads and writes. The second to acquire sees the first's just-committed version in step 3.

### Read-Latest, Not Read-at-Snapshot

The pre-image read inside the lock deliberately does *not* go through `ReadTable(table, snapshot_ts)`. Reason: the lock prevents two UPDATEs from interleaving, but if the second to arrive uses an older `snapshot_ts` than the first's committed version, MVCC's filter (`version_ts > snapshot_ts → skip`) will skip the just-committed version and read a stale pre-image -- the lost update returns under a different costume.

Inside the lock the writer wants the *truly current* state of the row. We add a sibling helper for this:

```cpp
// rocks/rocks.h
std::optional<std::map<std::string, std::string>> ReadLatest(
    const std::string& table_name, const std::string& pk);
```

`ReadLatest` does a prefix scan on `/{table}/{pk}/`, returns the lex-largest version's columns, and ignores any timestamp filter. This is what step 3 calls. It's the same pattern Postgres and MySQL InnoDB follow: an UPDATE's pre-image is read at "now" under the row lock, not at the transaction's snapshot.

The cost of this design choice -- discussed earlier on the page -- is that an UPDATE statement does not respect `snapshot_ts`. SELECTs still do. The cluster has two read modes: snapshot reads for SELECT, read-latest for UPDATE. Documenting this honestly is more important than hiding it.

### The version_ts Bump Rule

Step 5 writes the new version at `max(coordinator_ts, latest_version_ts_for_pk + 1)`, not at `coordinator_ts` directly. The reason: coordinator timestamps from different nodes can arrive in non-ts order. If T_b (coordinator ts=105) commits its write first and T_a (coordinator ts=100) acquires the lock second, T_a's write at version_ts=100 would lex-sort *before* T_b's at 105 and be silently shadowed -- the same lost-update shape, now hidden inside a system that thought it was protected.

Bumping the version_ts ensures the on-disk order matches the commit order on each row. Reads at any future `snapshot_ts >= bumped_ts` see T_a's write as the latest. Reads at `snapshot_ts` between T_b's commit and T_a's commit see T_b's value -- which is correct, because at that snapshot, T_a hadn't committed.

The bump rule lives in `WriteRow`: before writing, scan the pk for the largest existing version_ts and bump if the caller-supplied `ts` doesn't exceed it.

### Scope Decisions

A few choices we make explicit so future-us doesn't have to reverse-engineer them:

- **Single-pk UPDATE only.** `WHERE id = X` style. `WHERE balance > 100` (predicate WHERE that resolves to multiple rows) asserts and crashes -- not because it's hard to support, but because supporting it correctly requires (a) lock-ordering rules to avoid deadlock, (b) deciding whether the lock list is acquired before or during the predicate scan. The bank test never issues such an UPDATE; we'll cross that bridge if a workload arrives that does.
- **Locks are statement-scoped, not transaction-scoped.** The bank test's `BEGIN; UPDATE ...; UPDATE ...; COMMIT` releases the first UPDATE's lock before the second UPDATE starts. This is fine for failure mode 1 (lost updates on a single row); it does *not* prevent a SELECT from observing the state between the two UPDATEs of one transfer. But that's a non-atomic-transfer problem, not a lock-scope problem -- in our system `BEGIN`/`COMMIT` are no-ops and each UPDATE is its own auto-commit transaction. Making the transfer atomic is a separate page.
- **Catalog DDL bypasses the lock manager.** `CatalogManager::UpdateTable` writes directly through `WriteRow`. DDL is not concurrent with itself in any current path, and routing it through the lock manager would mean serializing every CREATE TABLE behind the same map mutex.
- **The dispatch fan-out side acquires the lock unconditionally.** UPDATEs broadcast to all three peers; two of them have no row matching the WHERE and do a no-op. They still acquire and release the lock briefly. Cheaper to keep the path uniform than to special-case it.

### What This Buys (and What It Doesn't)

**Buys.** Failure mode 1 disappears. Two concurrent transfers touching the same balance now serialize on that row's lock; the second sees the first's commit; both effects survive on disk. The bank test's "total > 10,000" failures (where money was created from thin air) should not appear in subsequent runs. By extension, "total < 10,000" failures whose root cause was a lost update -- as opposed to read skew within a single SELECT, which MVCC already addressed -- also disappear.

**Doesn't.** Three things explicitly remain:

- **Non-atomic multi-statement transfers.** The bank test's transfer is `BEGIN; UPDATE_debit; UPDATE_credit; COMMIT`, which our system runs as **two separate auto-commit transactions** -- `BEGIN` and `COMMIT` are no-ops in `stmt_handler.cc`, and each UPDATE picks its own `ts`. The two halves commit at different timestamps with no notion that they belong together. A SELECT whose `snapshot_ts` falls between the two timestamps correctly returns the cluster's state at that snapshot -- which happens to be "after the debit, before the credit." The SELECT itself is internally consistent; what's broken is that the *writer* isn't atomic. This is not read skew (MVCC already fixed read skew); it's the lack of multi-statement transactions. Fixing it is the next page.
- **Write skew.** Two transactions read overlapping data, write disjoint rows, the combined effect violates an invariant. Postgres needed SSI to catch this; the bank test doesn't exercise it.
- **Multi-row UPDATE.** Asserts. Not in scope.

A passing bank test is not the goal of this change -- a passing bank test additionally requires real `BEGIN`/`COMMIT` semantics so that a transfer commits atomically. What this change does deliver is "the on-disk state never gains or loses money," even if a transient SELECT can still observe a partially-applied transfer.
