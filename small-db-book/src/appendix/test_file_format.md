# Test File Format

This document describes the format used for SQL test files in the small-db project. Test files have a `.sqltest` extension and contain a series of SQL statements and expected results that are executed sequentially to verify database functionality.

## File Structure

A test file consists of multiple test blocks, each containing:
1. A **directive** (statement type or query type)
2. The **SQL statement** to execute
3. For queries: **expected results** separated by a delimiter

## Statement Types

### `statement ok`

Used for SQL statements that should execute successfully without returning results.

**Format:**
```
statement ok
<SQL_STATEMENT>;
```

**Examples:**
```sql
statement ok
DROP TABLE users;

statement ok
CREATE TABLE users (
    id INT PRIMARY KEY,
    name STRING,
    balance INT,
    country STRING
) PARTITION BY LIST (country);
```

**Use cases:**
- DDL statements (CREATE, DROP, ALTER)
- DML statements (INSERT, UPDATE, DELETE)
- Any statement that doesn't return a result set

## Query Types

### `query <COLUMN_TYPES>`

Used for SQL queries that return result sets. The column types specify the expected data types for each column in the result.

**Format:**
```
query <COLUMN_TYPES>
<SQL_QUERY>;
----
<EXPECTED_RESULTS>
```

**Column Type Codes:**
- `T` - Text/String
- `I` - Integer
- `F` - Float
- `B` - Boolean
- `D` - Date/DateTime

**Examples:**
```sql
query TT
SELECT * FROM system.tables;
----
 table_name | columns
------------+--------
 users      | id:int(PK), name:str, balance:int, country:str

query TTTTT
SELECT * FROM system.partitions WHERE table_name = 'users';
----
table_name | partition_name | constraint        | column_name  | partition_value
-----------+----------------+-------------------+--------------+---------------------------------
users      | users_asia     | {"region":"asia"} | country      | ["China","Japan","Korea"]
users      | users_eu       | {"region":"eu"}   | country      | ["Germany","France","Italy"]
users      | users_us       | {"region":"us"}   | country      | ["USA","Canada"]
```

**Column Type Examples:**
- `query TT` - Expects 2 text columns
- `query TTTTT` - Expects 5 text columns
- `query TIT` - Expects text, integer, text columns

## Result Delimiter

The expected results section is separated from the SQL query by a line containing exactly four hyphens (`----`).

## Test Execution Flow

1. **Sequential Execution**: Tests are executed in the order they appear in the file
2. **Statement Validation**: `statement ok` blocks verify that SQL executes without errors
3. **Result Validation**: `query` blocks verify that results match expected output exactly
4. **Error Handling**: If any test fails, the entire test suite fails

## Best Practices

### 1. Test Setup and Cleanup
```sql
-- Clean up before testing
statement ok
DROP TABLE IF EXISTS users;

-- Set up test data
statement ok
CREATE TABLE users (id INT, name STRING);
```

### 2. Meaningful Test Names
Use descriptive table and column names that clearly indicate what is being tested.

### 3. Comprehensive Coverage
Test both positive cases (expected behavior) and edge cases (boundary conditions).

### 4. Data Validation
Use `query` blocks to verify that data was inserted, updated, or deleted correctly.

### 5. Constraint Testing
Test that constraints, indexes, and other database objects are created and function properly.

## Example Test File

Here's a complete example demonstrating the format:

```sql
-- Test user table creation and partitioning
statement ok
DROP TABLE IF EXISTS users;

statement ok
CREATE TABLE users (
    id INT PRIMARY KEY,
    name STRING,
    balance INT,
    country STRING
) PARTITION BY LIST (country);

-- Test partition creation
statement ok
CREATE TABLE users_eu PARTITION OF users FOR VALUES IN ('Germany', 'France', 'Italy');

-- Verify table structure
query TT
SELECT table_name, columns FROM system.tables WHERE table_name = 'users';
----
table_name | columns
-----------+--------
users      | id:int(PK), name:str, balance:int, country:str

-- Test data insertion
statement ok
INSERT INTO users (id, name, balance, country) VALUES (1, 'Alice', 1000, 'Germany');

-- Verify data was inserted
query TIT
SELECT name, balance, country FROM users WHERE id = 1;
----
name  | balance | country
------+---------+--------
Alice | 1000    | Germany
```

## Error Cases

### Statement Failures
If a `statement ok` block fails (e.g., syntax error, constraint violation), the test will fail.

### Query Mismatches
If a `query` block returns different results than expected, the test will fail. This includes:
- Different number of rows
- Different column values
- Different column types
- Different column order

### System Errors
Database crashes, connection failures, or other system-level errors will cause test failures.

## Integration with Test Framework

The `.sqltest` files are typically executed by a test runner that:
1. Parses the file format
2. Executes SQL statements against a test database
3. Compares actual results with expected results
4. Reports pass/fail status for each test block
5. Provides detailed error information for failed tests

This format allows for comprehensive testing of SQL functionality while maintaining readability and ease of maintenance.
