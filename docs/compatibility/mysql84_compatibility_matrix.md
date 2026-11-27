# MySQL 8.4 compatibility matrix

This matrix is the executable baseline for the compatibility plan. A case is
only considered passed when the reference and target executions both produce
the expected result shape; a connection or execution error is recorded as a
failure, never as an empty result.

## Result contract

Each case is represented as JSON with:

```json
{
  "name": "foreign-key-read",
  "sql": "SELECT id, parent_id FROM compat_child ORDER BY id",
  "expected_rows": [[1, 1]],
  "expected_columns": ["id", "parent_id"],
  "expected_error": null,
  "expected_warnings": []
}
```

The runner writes one report under `reports/compatibility/` containing the
server version, git revision, elapsed time, columns, rows, warnings, errno and
SQLSTATE for both endpoints.

## Initial cases

| Area | Case | SQL/behavior |
|---|---|---|
| DDL | `compat-parent-child` | create parent and child with a named foreign key |
| Constraint | `foreign-key-read` | insert a valid parent/child pair and read it back |
| Metadata | `full-tables` | `SHOW FULL TABLES` includes `BASE TABLE` |
| Metadata | `columns` | `information_schema.COLUMNS` returns ordinal columns |
| Protocol | `prepared-long-data` | send long data in chunks and consume once on execute |
| Transaction | `rollback-constraint` | constraint failure leaves rows and indexes unchanged |

## Gate

The matrix is a differential evidence tool, not a claim that every MySQL
feature is implemented. Unsupported cases must have an explicit errno and
SQLSTATE and must not silently return success.
