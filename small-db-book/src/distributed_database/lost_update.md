# Lost Updates

## The Anomaly

**Behavior.** Two transactions both successfully commit writes to the same row -- both report success to their clients -- yet one transaction's effect is silently lost on disk. The system's invariants drift, even though every individual transaction looked correct from its own point of view.

**Root cause.** Read-modify-write is a non-atomic compound operation, and we let two of them on the same row interleave their reads and writes. Each writer reads the same pre-image, computes a new value, and writes it; whichever write lex-sorts last on disk wins, but its value was computed from a state that didn't include the other writer's commit. There is no equivalent serial schedule -- neither "T_a then T_b" nor "T_b then T_a" matches what storage now contains.

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

### 2. Per-Row Pessimistic Lock (2PL)

A lock manager keyed by `(table, pk)`. Each `update()` acquires the row's lock before reading the pre-image and releases it after the write commits. Updaters of different rows proceed in parallel; updaters of the same row queue.

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
| 2. Per-row 2PL | Yes | Small | Good (per-row serial) | No | Per-row | No (sufficient) |
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

The next page commits to one of these and walks through what the implementation actually looks like.
