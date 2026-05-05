# Testing the MVP: The Bank Test

Once the MVP cluster from the [previous chapter](./from_single_node.md) was running, the natural next step was: prove it broken. That's what Jepsen is for.

## What Jepsen Is

[Jepsen](https://jepsen.io/) is a distributed-systems testing framework written in Clojure. It drives concurrent client operations against a cluster, records every invocation and response in a *history*, and then runs that history through a *checker* that decides whether the recorded behavior is consistent with some claimed invariant (linearizability, serializability, balance conservation, etc.). It can also inject faults -- network partitions, clock skew, process kills -- to make the cluster's behavior under stress observable.

For small-db we are not yet at the point of injecting faults. The cluster fails its invariants under plain concurrent traffic, with no network partitions at all. That's what the bank test exercises.

## Our Bank Test

The bank test is a classic Jepsen workload: a fixed pool of accounts with a fixed total balance, and clients that move money between them. The invariant is simple: the *sum of all balances* is constant across every read. If a transfer ever loses or duplicates money, the sum changes and the checker fails.

The full definition lives in `small-db-jepsen/src/small_db_jepsen/runner.clj`. The interesting parts:

**Schema.** A single LIST-partitioned table, sharded by country across the three regions:

```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    name STRING,
    balance INT,
    country STRING
) PARTITION BY LIST (country);

CREATE TABLE users_us   PARTITION OF users FOR VALUES IN ('USA', 'Canada');
CREATE TABLE users_eu   PARTITION OF users FOR VALUES IN ('Germany', 'France', 'Italy');
CREATE TABLE users_asia PARTITION OF users FOR VALUES IN ('China', 'Japan', 'Korea');
```

Every region owns one partition, so transfers between certain account pairs are guaranteed to cross node boundaries.

**Initial state.** Five accounts, total balance `10,000`:

| id | name    | balance | country | owner node |
|----|---------|--------:|---------|------------|
| 1  | Alice   |   1,000 | Germany | europe     |
| 2  | Bob     |   2,000 | USA     | america    |
| 3  | Charlie |   1,500 | France  | europe     |
| 4  | David   |   3,000 | China   | asia       |
| 5  | Eve     |   2,500 | Japan   | asia       |

**Workload.** 100 operations, mixed between two op types from `jepsen.tests.bank`:

- `read` -- `SELECT id, balance FROM users` against one client's connection.
- `diff-transfer` -- pick two distinct accounts and an amount in `[1, max-transfer]`, then debit one and credit the other. Each transfer wraps the two `UPDATE` statements in `BEGIN; ... COMMIT;`.

**Checker.** `jepsen.tests.bank/checker` with `:negative-balances? false`. It walks every `read` op in the history and asserts that every observed snapshot of balances sums to `10,000` and contains no negative entries.

**Knobs:** `:total-amount 10000`, `:max-transfer 100`, `:accounts [1 2 3 4 5]`.

## Why This Should Be Hard

A passing bank test does not require multi-region consensus or strict serializability -- it only requires that every transfer be **atomic** (debit and credit either both happen or neither does) and that reads see a **consistent snapshot** (no read interleaved between the debit and the credit of the same transfer). Both are baseline guarantees for any database that calls itself transactional.

For a partitioned table where the two accounts in a transfer can live on different nodes, "atomic across two nodes" is the hard part -- it's the problem distributed transactions exist to solve.

## The Failure

> *Placeholder for the actual failure analysis from the most recent `bank-test` run.*
>
> Things to cover here when filling this in:
>
> - The exact checker output from `small-db-jepsen/store/bank-test/latest/results.edn` -- which read snapshot violated the invariant, and what the sum was vs. the expected `10,000`.
> - The matching slice of `history.edn` -- which transfer(s) preceded the bad read, and on which client/node they ran.
> - Whether the lost money is from same-node concurrent updates (the lost-update race in `update.cc`) or from cross-node transfer atomicity (a debit that committed on one node while the credit failed or was delayed on another).
> - Whether `BEGIN`/`COMMIT` made any difference, given that the C++ path currently treats every statement as auto-commit.

## Fixing It

> *Placeholder for the fix.*
>
> Probable directions, in rough order of how much they buy us:
>
> 1. **Per-row serialization on UPDATE.** Close the local lost-update race in `src/execution/update.cc` -- read, mutate, and write back under a per-key lock or via a CAS on the latest MVCC version. This alone won't fix cross-node transfers, but it removes the cheapest source of failures.
> 2. **Re-land the reverted commit-ts work.** Commit `e88b510` introduced snapshot reads and a per-transaction `commit_ts`; commit `1ff91ee` reverted it. Bringing it back gives `BEGIN`/`COMMIT` real meaning on a single node.
> 3. **Cross-node atomicity.** Some form of two-phase commit, or routing both legs of a transfer through the same coordinator with a write-ahead log, so a transfer either lands on both partitions or neither.
> 4. **Replication acks.** Today gossip is best-effort and 3-second-paced; reads on a peer can miss writes that have already returned success to the client. Synchronous replication to a quorum -- or at least to the partition owner -- closes this gap.

When this section is written for real, link from each fix back to the commit(s) that introduced it, so the chapter doubles as a tour of the diff.
