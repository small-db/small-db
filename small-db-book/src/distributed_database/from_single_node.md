# From Single-Node to Distributed (MVP)

This page traces the smallest set of changes that turned small-db from a single-process database into a 3-node cluster. The goal of the MVP was not to be correct under contention -- it was to make the topology, catalog, and routing work end-to-end on three machines so Jepsen would have something to attack.

## The Starting Point

Before any of this, small-db was a single server: PostgreSQL wire protocol → `stmt_handler` → `execution/` → RocksDB. Every statement was local. There was a schema catalog, but it lived only in this one process's RocksDB.

Going distributed meant answering four questions:

1. **How do nodes find each other?**
2. **How does a node know who its peers are at any given moment?**
3. **How do schema changes reach every node?**
4. **How does a statement reach the node that owns the data?**

Each question maps to one of the changes below.

## 1. Gossip Protocol for Node-to-Node Communication

Every node runs a `GossipServer` (`src/gossip/gossip.cc`) that exchanges peer information over gRPC. A node bootstraps by being given a `--join <addr>` flag at startup -- that single seed address is enough to discover the rest of the cluster, because gossip is transitive: once you know one peer, you eventually learn all peers it knows.

The server runs on each node's gRPC port (50001-50003 in the local 3-region setup). Every 3 seconds it picks a random subset of known peers and exchanges keys, timestamps, and values.

> Gossip here also carries replicated rows -- not just membership. That conflation is intentional for the MVP and is one of the reasons the system is eventually consistent. See [the consistency note](#what-we-deferred) below.

## 2. In-Memory Peer List

Each `GossipServer` keeps an in-memory map of `node_id → ImmutableInfo` (region, gRPC address, SQL port, etc.). Other modules consume it via two helpers:

| Helper | Returns |
|--------|---------|
| `gossip::GossipServer::get_nodes()` | All known peers including self |
| `gossip::get_nodes(constraints)` | Peers whose `ImmutableInfo` matches the given key/value constraints (e.g. `region=us`) |

The constrained variant is what makes partition routing work in §4 -- a partition stores a *constraint* (`region=us`), and the executor asks gossip "give me the node that satisfies this constraint."

Peers are ephemeral state by design: there's no persisted membership list. Restart a node and it rediscovers the cluster from its `--join` seed.

## 3. Catalog Replication on Schema Change

A `CREATE TABLE` has to land on every node, otherwise an `INSERT` routed to a peer that hasn't seen the schema would fail. The catalog (`src/catalog/catalog.cc`) handles this with a two-step write:

1. **Local persist.** Write the new table (and its partition rows) into the system tables `__system.tables` and `__system.partitions` via RocksDB. These are real tables stored in the same MVCC format as user data.
2. **Broadcast.** If `broadcast=true`, iterate over every peer from gossip and call the `Catalog::UpdateTable` gRPC on each. The receiving node calls `UpdateTable(..., broadcast=false)` to avoid a fan-out storm.

```
client → CREATE TABLE → catalog::UpdateTable(broadcast=true)
                          ├─ write to local RocksDB
                          └─ for peer in gossip.get_nodes():
                               peer.UpdateTable(broadcast=false)
                                 └─ write to peer's local RocksDB
```

The broadcast is synchronous and best-effort: if any peer's gRPC call fails, the originating call returns an error. There is no two-phase commit and no retry -- a partial failure leaves the cluster with divergent catalogs until the next manual fix.

## 4. Partition-Aware SQL Dispatch

The catalog stores LIST-partition definitions per table. Each partition carries a constraint map (e.g. `{region: us}`) that names *which kind of node* owns that partition. Combined with the constrained `gossip::get_nodes(constraints)` lookup from §2, that's enough to route a row to its owner.

```
   INSERT INTO users (id, region, name) VALUES
     (1, 'us',   'alice'),
     (2, 'eu',   'bob'),
     (3, 'asia', 'carol');
                │
                ▼
   ┌──────────────────────────────┐
   │ catalog: users               │
   │   partition column = region  │
   │   partitions:                │
   │     p_us    {region = us}    │
   │     p_eu    {region = eu}    │
   │     p_asia  {region = asia}  │
   └───────────────┬──────────────┘
                   │  for each row:
                   │    1. read partition column value
                   │    2. lookup partition definition
                   │    3. gossip.get_nodes(constraints)
                   ▼
   ┌──────────┐    ┌──────────┐    ┌──────────┐
   │  node us │    │  node eu │    │ node asia│
   │ region=us│    │ region=eu│    │region=asia│
   └────▲─────┘    └────▲─────┘    └─────▲────┘
        │               │                │
   (1,us,alice)    (2,eu,bob)     (3,asia,carol)
```

`INSERT` is the canonical example (`src/execution/insert.cc`):

1. Look up the table in the local catalog.
2. If the table has a `list_partition`, find the partition column in the INSERT.
3. For each row, compute the partition value and look up the partition definition.
4. Ask gossip for the peer that satisfies that partition's constraints.
5. Open a gRPC channel to that peer and forward the row.

If the table is not partitioned, the row is written locally and gossip eventually replicates it.

`SELECT` (`src/execution/query.cc`) does a scatter-gather. When called with `dispatch=true` on a partitioned table, the coordinator packs the `SelectStmt`, sends it via the `Query` gRPC to every peer, and each peer runs the query locally against its own RocksDB and returns an Arrow IPC-encoded `RecordBatch`. The coordinator then concatenates the per-peer batches column-by-column into a single result. Non-partitioned and system tables are replicated identically on every node, so they skip the fan-out and execute locally. There's no partition pruning yet -- a `WHERE region = 'us'` still queries all three nodes.

`UPDATE` (`src/execution/update.cc`) takes a coarser shortcut: when called with `dispatch=true` it fans the packed AST out to **every** peer, and each peer re-executes the update locally. Cheaper to write than computing affected partitions; we'll come back to it.

## A Simple System

This is deliberately a simple system that does not care about ACID in a distributed setting. Each statement is auto-commit, writes return as soon as the local node applies them, and cross-region replication is best-effort gossip. Concurrent updates can lose writes; cross-region reads can be stale. Those are not bugs to fix on this page -- they are the surface that the [Jepsen `bank-test`](../clutter/jepsen.md) attacks, and the layer that later chapters will build on top of.
