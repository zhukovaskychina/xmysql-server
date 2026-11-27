# Superpower JDBC Compatibility Closure: Review & Fix Plan

## Overview / ??

This document is the output of a systematic code review conducted against the
superpower plan [2026-07-16-jdbc-mysql-compatibility-closure.md](../superpowers/plans/2026-07-16-jdbc-mysql-compatibility-closure.md).

Each finding maps to a specific task and step in the superpower plan, with concrete
code changes, file paths, and line references.

????? superpower ?? `2026-07-16-jdbc-mysql-compatibility-closure.md`
?????????????????????????????? Task ? Step?
?????????????????

---

## 1. SHOW FULL TABLES -- Parser + Executor Gap

**Superpower plan**: Task 1 (DDL Metadata Baseline)
**Status**: Not implemented
**Blocking**: Blocks all JDBC table browsing (DBeaver, IntelliJ, etc.)

### Root cause / ??

Two independent failures:

1. **Parser layer** (`enginx.go:366`): `sqlparser.Parse(query)` fails on `SHOW FULL TABLES`.
   The Vitess-based parser does not support the `FULL` modifier. The error never reaches the executor.

2. **Executor layer** (`show_executor.go:Init()`): The ShowExecutor only handles 3 cases:
   `DATABASES`, `TABLES`, `COLUMNS`. No `FULL` modifier, no `WHERE` filtering.

### Changes required / ???????

| Step | File | Change |
|------|------|--------|
| 1.1 | `enginx.go` ~L366 | Before `sqlparser.Parse()`, detect `SHOW FULL TABLES` and strip `FULL`. Store `isFull` flag. |
| 1.2 | `executor.go` | Thread the `isFull` flag through `executeShowStatementWithQuery()` to `buildShowExecutor()` |
| 1.3 | `show_executor.go` | Extend `buildShowExecutor()` to accept `isFull bool` param |
| 1.4 | `show_executor.go` | In `Init()`, handle table list based on `isFull` flag |
| 1.5 | `show_executor.go` | When `isFull=true`, return 2 columns: `Tables_in_X` + `Table_type` |
| 1.6 | `show_executor.go` | Populate `Table_type` as `"BASE TABLE"` or `"VIEW"` from metadata |

### Effort / ???

~1 day.

---

## 2. INFORMATION_SCHEMA Query Routing -- SystemVariableEngine Misroute

**Superpower plan**: Task 1, Steps 3 & 6
**Status**: Bug
**Blocking**: All `information_schema` queries via JDBC fail

### Root cause / ??

In `query_dispatcher.go`, `isSystemVariableQuery()` returns `true` for any query
containing `INFORMATION_SCHEMA`. This routes queries like:
- `SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'x'`
- `SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS WHERE ...`

...to the `SystemVariableEngine`, which cannot handle them and returns
`"invalid system variable query"`.

### Changes required / ???????

| Step | File | Change |
|------|------|--------|
| 2.1 | `query_dispatcher.go` | In `isSystemVariableQuery()`, check for pure `INFORMATION_SCHEMA` SELECTs FIRST and return `false` |
| 2.2 | `query_dispatcher.go` | Add explicit routing: `SELECT ... FROM information_schema.*` to `"innodb"` engine |
| 2.3 | `system_variable_engine.go` | Add fallback in `ExecuteQuery()` -- if no handler matches, forward to innodb engine |
| 2.4 | `system_variable_engine.go` | Add information_schema keywords to the exclusion list |

### Logic for Step 2.1

```go
func (r *DefaultSQLRouter) isSystemVariableQuery(query string) bool {
    // INFORMATION_SCHEMA queries should go to innodb, not system_variable
    if isInformationSchemaMetadataQuery(query) {
        return false
    }
    // ... rest of existing logic ...
}
```

### Effort / ???

~0.5 day. Pure routing logic fix.

---

## 3. INFORMATION_SCHEMA Tables -- Missing Data Implementation

**Superpower plan**: Task 1, Step 6
**Status**: Not implemented
**Blocking**: JDBC `DatabaseMetaData.getTables()` and `getColumns()` return empty

### Root cause / ??

`metadata/information_schemas.go` defines the `InfoSchemaManager` interface but does not
implement the TABLES, COLUMNS, ENGINES, SCHEMATA, ROUTINES, or PARAMETERS virtual tables
with actual data populated from the storage manager.

### Changes required / ???????

| Step | File | Change |
|------|------|--------|
| 3.1 | `metadata/information_schemas.go` | Implement `TABLES` table: `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `TABLE_TYPE`, `ENGINE` |
| 3.2 | `metadata/information_schemas.go` | Implement `COLUMNS` table: `TABLE_SCHEM`, `TABLE_NAME`, `COLUMN_NAME`, `DATA_TYPE`, `IS_NULLABLE`, `COLUMN_DEFAULT`, `ORDINAL_POSITION`, `COLUMN_KEY`, `EXTRA`, `CHARACTER_MAXIMUM_LENGTH`, `COLUMN_TYPE` |
| 3.3 | `metadata/information_schemas.go` | Implement `SCHEMATA` table: `SCHEMA_NAME`, `DEFAULT_CHARACTER_SET_NAME`, `DEFAULT_COLLATION_NAME` |
| 3.4 | `metadata/information_schemas.go` | Implement `ENGINES` table: at minimum return InnoDB row with `SUPPORT = 'DEFAULT'` |
| 3.5 | `metadata/information_schemas.go` | Implement empty `ROUTINES` and `PARAMETERS` tables to avoid JDBC errors |

### Minimum columns for TABLES response

```go
func (m *impl) QueryInformationSchemaTables(ctx context.Context, schemaName string) ([]map[string]string, error) {
    tables, err := m.GetAllTables(ctx, schemaName)
    if err != nil { return nil, err }
    result := make([]map[string]string, 0, len(tables))
    for _, t := range tables {
        result = append(result, map[string]string{
            "TABLE_CAT":   schemaName,
            "TABLE_SCHEM": schemaName,
            "TABLE_NAME":  t.Name,
            "TABLE_TYPE":  "BASE TABLE",
        })
    }
    return result, nil
}
```

### Effort / ???

~2-3 days. Bulk of work is querying storage manager for metadata and formatting rows.

---

## 4. ALTER TABLE ADD COLUMN -- Executor Column Append Missing

**Superpower plan**: Task 2 (Minimal ALTER TABLE ADD COLUMN)
**Status**: Partially implemented
**Blocking**: JDBC `DDLOperationsTest` fails on ALTER

### Root cause / ??

`executor.go` has `case "alter"` in the DDL switch, but the dispatched
`executeAlterTableStatement()` does not actually read/write the `.frm` file to add
columns. The column-append logic is a stub.

### Changes required / ???????

| Step | File | Change |
|------|------|--------|
| 4.1 | `executor.go` | Verify `executeAlterTableStatement()` wiring -- ensure the DDL switch routes correctly |
| 4.2 | `executor.go` | Implement `alterTableAddColumns()`: read .frm JSON, parse, append column, marshal, write |
| 4.3 | `executor.go` | Add validation: duplicate column names, type compatibility checks |

### Pseudocode

```go
func (e *XMySQLExecutor) alterTableAddColumns(dbName, tableName string, columns []*sqlparser.ColumnDefinition) error {
    frmPath := filepath.Join(e.getDataDir(), dbName, tableName+".frm")
    raw, err := os.ReadFile(frmPath)
    if err != nil { return fmt.Errorf("read frm failed: %w", err) }
    var tableInfo map[string]interface{}
    if err := json.Unmarshal(raw, &tableInfo); err != nil { return fmt.Errorf("parse frm failed: %w", err) }
    existing, _ := tableInfo["columns"].([]interface{})
    for _, col := range columns {
        colName := col.Name.String()
        // Check for duplicates
        for _, existingCol := range existing {
            if m, ok := existingCol.(map[string]interface{}); ok && strings.EqualFold(m["name"].(string), colName) {
                return fmt.Errorf("duplicate column '%s'", colName)
            }
        }
        existing = append(existing, map[string]interface{}{
            "name":     colName,
            "type":     col.Type.Type,
            "nullable": !col.Type.NotNull,
        })
    }
    tableInfo["columns"] = existing
    out, _ := json.MarshalIndent(tableInfo, "", "  ")
    return os.WriteFile(frmPath, out, 0644)
}
```

### Effort / ???

~1 day.

---

## 5. Transaction Command Routing -- BEGIN/COMMIT Parser Bypass Missing

**Superpower plan**: Task 4 (JDBC Transaction Command Baseline)
**Status**: Routing incomplete
**Blocking**: JDBC `Connection.commit()`, `rollback()`, `setAutoCommit(false)` fail

### Root cause / ??

`enginx.go` does not contain `IsTransactionCommand`. The transaction handlers
exist inside `executor.go` but are only reachable if the Vitess parser successfully
parses the query first. For raw text like `BEGIN` or `COMMIT`, the parser may fail
before the executor switch is reached.

### Changes required / ???????

| Step | File | Change |
|------|------|--------|
| 5.1 | `enginx.go` | Add `normalizedTransactionCommand()` to extract (cmd, name) from raw query before parsing |
| 5.2 | `enginx.go` | Route matched transaction commands directly to executor, bypassing parser |
| 5.3 | `enhanced_message_handler.go` | Fix reference to `engine.IsTransactionCommand` |

### Code for Step 5.1

```go
func normalizedTransactionCommand(query string) (cmd string, name string, ok bool) {
    q := strings.TrimSpace(strings.TrimRight(query, ";"))
    lower := strings.ToLower(q)
    switch {
    case lower == "begin" || lower == "start transaction":
        return "begin", "", true
    case lower == "commit":
        return "commit", "", true
    case lower == "rollback":
        return "rollback", "", true
    case strings.HasPrefix(lower, "savepoint "):
        return "savepoint", strings.TrimSpace(q[len("savepoint "):]), true
    case strings.HasPrefix(lower, "rollback to savepoint "):
        return "rollback_to_savepoint", strings.TrimSpace(q[len("rollback to savepoint "):]), true
    case strings.HasPrefix(lower, "release savepoint "):
        return "release_savepoint", strings.TrimSpace(q[len("release savepoint "):]), true
    default:
        return "", "", false
    }
}
```

### Effort / ???

~0.5 day.

---

## 6. SHOW VARIABLES LIKE -- LIKE Clause Extraction Bug

**Superpower plan**: Task 1 (DDL Metadata Baseline)
**Status**: Likely bug
**Blocking**: `SHOW VARIABLES LIKE 'pattern'` returns error instead of results

### Root cause / ??

In `system_variable_engine.go`, `executeShowVariables()` accepts `likePattern string`
as parameter. The caller `executeShowStatement()` parses the AST `*sqlparser.Show`
and extracts the LIKE pattern. The extraction may not handle wildcard patterns
(`lower_case_%`) correctly.

### Changes required / ???????

| Step | File | Change |
|------|------|--------|
| 6.1 | `system_variable_engine.go` | Debug `executeShowStatement()` to `executeShowVariables()` call chain. Check LIKE clause extraction |
| 6.2 | `system_variable_engine.go` | Add fallback: if SHOW VARIABLES LIKE extraction fails, return empty results instead of error |

### Effort / ???

~0.5 day for debug + fix.

---

## 7. NOT NULL Constraint -- Missing Pre-write Validation

**Superpower plan**: Task 5 (Core Constraint Semantics), Step 3
**Status**: Not implemented
**Blocking**: JDBC constraint tests fail on NOT NULL violations

### Root cause / ??

`storage_integrated_dml_executor.go` does not check column nullability before
inserting/updating rows. NOT NULL columns accept NULL values silently.

### Changes required / ???????

| Step | File | Change |
|------|------|--------|
| 7.1 | `storage_integrated_dml_executor.go` | Add NOT NULL validation before row encoding: load column metadata, reject null for nullable=false columns without defaults |

### Logic

```go
// In insert/update path, before encoding row:
columns, err := resolveTableColumns(schemaName, tableName)
for _, col := range columns {
    val, hasValue := rowValues[col.Name]
    if !hasValue || val == nil {
        if !col.Nullable && col.DefaultValue == nil {
            return fmt.Errorf("Column '%s' cannot be null", col.Name)
        }
    }
}
```

### Effort / ???

~0.5 day.

---

## 8. Composite Primary Key -- Multi-column Uniqueness Check

**Superpower plan**: Task 5 (Core Constraint Semantics), Step 4
**Status**: Unverified -- needs code audit

### Root cause / ??

If `storage_integrated_index_helper.go` assumes the primary key is only the first
column, then `PRIMARY KEY (user_id, group_id)` will only check `user_id` for
uniqueness, allowing duplicate `(user_id, group_id)` pairs.

### Changes required / ???????

| Step | File | Change |
|------|------|--------|
| 8.1 | `storage_integrated_index_helper.go` | Audit primary key uniqueness check: verify all PK columns used in composite key |
| 8.2 | `storage_integrated_index_helper.go` | If not, implement `buildCompositeKey()` that concatenates all PK columns |

### Effort / ???

~0.5 day for audit + fix.

---

## 9. TRUNCATE Metadata Remap -- Verification

**Superpower plan**: Task 3 (TRUNCATE Metadata Mapping Closure), Step 3
**Status**: Likely already fixed. `ReplaceTableStorage` exists in table_storage_mapping.go.

### Changes required / ???????

| Step | File | Change |
|------|------|--------|
| 9.1 | `executor.go` | Verify `truncateTableImpl` uses `ReplaceTableStorage` (not unregister+register) |
| 9.2 | New test | Add `TestTruncateKeepsTableMetadataResolvable`: CREATE -> INSERT -> TRUNCATE -> INSERT -> SELECT |

### Effort / ???

~0.3 day for test addition.

---

## Implementation Order / ????

| Round | Issues | Goal | Est. days |
|-------|--------|------|-----------|
| Round 1 | 2 (routing) + 3 (information_schema data) | JDBC metadata queries stop failing | 2.5 |
| Round 2 | 1 (SHOW FULL TABLES) + 6 (VARIABLES LIKE) | SHOW commands work correctly | 1.5 |
| Round 3 | 4 (ALTER TABLE) + 5 (transactions) | DDL + transaction commands acceptable | 1.5 |
| Round 4 | 7 (NOT NULL) + 8 (composite PK) | Core constraints enforced | 1.0 |
| Round 5 | 9 (TRUNCATE verification) | Verify + add test | 0.3 |

**Total estimated effort: ~7 days**

---

## Verification / ????

### Round 1+2 (metadata + SHOW)

```bash
go run . -configPath=conf/jdbc_local.ini
cd jdbc_client && mvn test -Pjdbc-connectivity
cd jdbc_client && mvn test -Dtest=DDLOperationsTest
```

### Round 3 (transactions)

```bash
cd jdbc_client && mvn test -Dtest=TransactionTest
# Expected: no "unsupported statement type" or SAVEPOINT parse errors
```

### Round 4 (constraints)

```bash
cd jdbc_client && mvn test -Dtest=IndexAndConstraintTest
# Expected: NOT NULL and primary key tests pass
```

### Go package regression

```bash
go test ./server/innodb/engine ./server/innodb/manager ./server/dispatcher ./server/net -count=1
```

---

## References / ????

| Document | Path |
|----------|------|
| Superpower JDBC closure plan | `docs/superpowers/plans/2026-07-16-jdbc-mysql-compatibility-closure.md` |
| JDBC fix plan (first draft) | `docs/fix-plan/JDBC_COMPATIBILITY_FIX_PLAN.md` |
| Development roadmap | `docs/planning/DEVELOPMENT_ROADMAP.md` |
| P0 capability backlog | `docs/planning/P0_CAPABILITY_BACKLOG_20260715.md` |
