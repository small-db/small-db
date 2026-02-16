# Storage Format

small-db uses RocksDB as its underlying storage engine. Each row is stored as a single key-value pair.

## Key Format

```
/<table_name>/<primary_key>/<written>
```

| Segment      | Description                                          |
|--------------|------------------------------------------------------|
| `table_name` | Name of the table                                    |
| `primary_key`| The row's primary key value                          |
| `written`    | Commit timestamp of the transaction that wrote this row |

The `written` timestamp enables multi-version storage -- multiple versions of the same row can coexist under the same `(table_name, primary_key)` prefix, each distinguished by its commit timestamp.

## Value Format

The value is `encoded_columns`, which contains the column values of the row encoded together.

## Example

For a table `accounts` with columns `(id, name, balance)`, inserting a row `(1, "alice", 100)` at transaction commit timestamp `1700000000` produces:

```
Key:   /accounts/1/1700000000
Value: <encoded (id=1, name="alice", balance=100)>
```

A subsequent update to `balance=80` at timestamp `1700000005` adds a new entry:

```
Key:   /accounts/1/1700000005
Value: <encoded (id=1, name="alice", balance=80)>
```

Both versions exist in storage. Reads select the appropriate version based on the transaction's read timestamp.
