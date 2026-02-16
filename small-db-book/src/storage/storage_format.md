# Storage Format

small-db uses RocksDB as its underlying storage engine. Each row version is stored as a single key-value pair, with multi-version support for concurrent transactions.

## Key Format

```
/<schema.table_name>/<primary_key>/<written>
```

| Segment            | Description                                          |
|--------------------|------------------------------------------------------|
| `schema.table_name`| Schema-qualified table name (e.g., `default_schema.users`) |
| `primary_key`      | The row's primary key value                          |
| `written`          | Zero-padded commit timestamp of the transaction that wrote this row |

The `written` timestamp enables multi-version storage -- multiple versions of the same row can coexist under the same `(table_name, primary_key)` prefix, each distinguished by its commit timestamp.

## Value Format

The value is a JSON object containing all column values of the row.

## Example

For a table `users` with columns `(id, name, balance, country)`, inserting a row `(2, "Bob", 2000, "USA")` produces:

```
Key:   /default_schema.users/2/00000001771270774149
Value: {"balance":"2000","country":"USA","id":"2","name":"Bob"}
```

A subsequent update to `balance=1941` adds a new version:

```
Key:   /default_schema.users/2/00000001771270774168
Value: {"balance":"1941","country":"USA","id":"2","name":"Bob"}
```

Both versions exist in storage. Reads select the appropriate version based on the transaction's read timestamp.

## System Tables

System metadata follows the same key-value format:

```
Key:   /system.tables/default_schema.users/00000001771270774031
Value: {"columns":"[{\"is_primary_key\":true,\"name\":\"id\",\"type\":0},...]","table_name":"default_schema.users"}

Key:   /system.partitions/users_us/00000001771270774058
Value: {"column_name":"country","constraint":"{\"region\":\"us\"}","partition_name":"users_us","partition_value":"[\"USA\",\"Canada\"]","table_name":"default_schema.users"}
```

## Prefix Scanning

Since all keys for a row share the same `(table_name, primary_key)` prefix, they are stored contiguously in RocksDB. This allows efficient retrieval via prefix scans.

For example:

```sql
SELECT * FROM users WHERE id = 2
```

translates to:

```
Scan(/default_schema.users/2/, /default_schema.users/2/Ω)
```

Similarly, scanning the entire table can be done with:

```
Scan(/default_schema.users/, /default_schema.users/Ω)
```

## Key Safety

- **No slash conflicts** -- Table and column names never contain `/`, so key parsing is unambiguous.

## Physical Storage Inspection

You can inspect the raw key-value pairs stored in RocksDB using the `rocks_scan` tool:

```bash
./build/debug/src/rocks/rocks_scan --data-path ./data
```

Example output (showing only user data rows for brevity):

```
[2026-02-16 11:39:38.683] [info] [rocks_scan.cc:115] scan data dir: ./data/us
    Key: /default_schema.users/2/00000001771270774149, Value: {"balance":"2000","country":"USA","id":"2","name":"Bob"}
    Key: /default_schema.users/2/00000001771270774168, Value: {"balance":"1941","country":"USA","id":"2","name":"Bob"}

[2026-02-16 11:39:38.683] [info] [rocks_scan.cc:115] scan data dir: ./data/eu
    Key: /default_schema.users/1/00000001771270774146, Value: {"balance":"1000","country":"Germany","id":"1","name":"Alice"}
    Key: /default_schema.users/3/00000001771270774151, Value: {"balance":"1500","country":"France","id":"3","name":"Charlie"}

[2026-02-16 11:39:38.683] [info] [rocks_scan.cc:115] scan data dir: ./data/asia
    Key: /default_schema.users/4/00000001771270774160, Value: {"balance":"3000","country":"China","id":"4","name":"David"}
    Key: /default_schema.users/5/00000001771270774162, Value: {"balance":"2500","country":"Japan","id":"5","name":"Eve"}
```

## References

- [SQL in CockroachDB: Mapping table data to key-value storage](https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/)
- [Implementing column families in CockroachDB](https://www.cockroachlabs.com/blog/sql-cockroachdb-column-families/)
- [CockroachDB RFC: SQL Column Families](https://github.com/cockroachdb/cockroach/blob/b3fa0b4b15113a9294b8b75c1f603dd52843e13c/docs/RFCS/20151214_sql_column_families.md)
