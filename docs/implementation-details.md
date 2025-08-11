# Implementation Details

## Storage Format

Each row is represented as multiple key-value pairs, where every non-primary column is stored under a key in the format:

```
/<table_name>/<primary_key>/<column_name>
```

For example, in a `users` table with primary key `10` and columns `name` and `age`:

```
/users/10/name -> "Alice"
/users/10/age  -> 30
```

Since all keys for a row share the same primary key prefix, they are stored contiguously. This allows all columns of a row to be retrieved with a single prefix scan.

For example:

```
SELECT * FROM users WHERE id = 10
```

translates to:

```
Scan(/users/10/, /users/10/Ω)
```

Similarly, scanning the entire table can be done with:

```
Scan(/users/, /users/Ω)
```

### Safety

- **No slash conflicts** — Table and column names never contain `/`, so key parsing is unambiguous.

- **Primary key encoding** — String primary keys are hex-encoded before storage, ensuring binary safety and avoiding collisions with special characters.
