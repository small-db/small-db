# Architecture

## Request Flow

```
PostgreSQL client → pg_wire/ (wire protocol) → server/stmt_handler (routing)
  → semantics/ (SQL validation via libpg_query) → execution/ (query/insert/update)
  → schema/ + catalog/ (metadata) → rocks/ (RocksDB storage)
```

## Key Modules

| Module | Description |
|--------|-------------|
| `server/` | Main entry point, TCP accept loop, statement handler dispatch |
| `pg_wire/` | PostgreSQL wire protocol encoding/decoding |
| `semantics/` | SQL semantic analysis and AST extraction (uses libpg_query) |
| `execution/` | Query (SELECT), Insert, Update execution engines; uses Arrow for columnar results |
| `schema/` | Table schema definitions and LIST partition management |
| `catalog/` | Schema update coordination via gRPC |
| `rocks/` | RocksDB wrapper; keys are `/<schema.table>/<primary_key>/<timestamp>` |
| `gossip/` | Gossip protocol for cross-region replication via gRPC |
| `type/` | Data type system (INT64, STRING) with PG OID mapping |
| `server_info/` | Server configuration (ports, region, data dir, join address) |
| `id/` | Snowflake-like unique ID generator |

## Storage

Each row version is stored as a single RocksDB key-value pair: `/<schema.table_name>/<primary_key>/<written_timestamp> → JSON`. This enables MVCC (multi-version concurrency control) and prefix scanning for row/table retrieval. See [Storage Format](./storage/storage_format.md) for details.

## Inter-Server Communication

gRPC is used for:
- **Catalog updates** -- Propagating schema changes across nodes
- **Query forwarding** -- Forwarding inserts/updates to the partition owner
- **Gossip replication** -- Cross-region data replication

Protobuf definitions live alongside their modules (e.g., `src/gossip/gossip.proto`).
