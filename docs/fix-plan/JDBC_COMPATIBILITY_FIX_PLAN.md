# JDBC Query Compatibility Fix Plan

## Overview / 概述

This document describes the root cause analysis and remediation plan for five known
compatibility issues identified during JDBC client interactions. These issues prevent
standard MySQL clients and ORM frameworks from connecting and browsing the database.

本文档针对 JDBC 客户端交互中发现的五个兼容性问题进行根因分析和修复规划。

---

## Issue 1: SHOW FULL TABLES parse error / SHOW FULL TABLES 解析错误

### Symptom / 现象

```
[ERRO] syntax error at position 17 near 'TABLES'
```

Client query: `SHOW FULL TABLES WHERE Table_type != 'VIEW'`

### Root cause / 根因

The Vitess-based SQL parser (in `server/innodb/sqlparser/`) does not support the `FULL`
modifier on `SHOW TABLES`. The query reaches `sqlparser.Parse()` at `enginx.go:366` and
fails before any executor logic runs.

Also, the current `ShowExecutor` (`show_executor.go`) handles only three cases in its
`Init()` method -- `DATABASES`, `TABLES`, `COLUMNS` -- with no support for `FULL` or
`WHERE` filtering.

### Fix plan / 修复方案

| Step | File(s) | Description | Priority |
|---|---|---|---|
| 1.1 | `enginx.go` or `executor.go` | Add query pre-processing: before `sqlparser.Parse()`, detect `SHOW FULL TABLES` and strip the `FULL` keyword. Store the `isFull` flag for later use. | P0 |
| 1.2 | `show_executor.go` | Extend `buildShowExecutor()` to accept an `isFull` parameter. When `isFull=true`, add a second column `Table_type` to the result schema and populate it with `"BASE TABLE"` or `"VIEW"` based on table metadata. | P0 |
| 1.3 | `show_executor.go` | Add `WHERE Table_type != 'VIEW'` filtering to `showTables()`. If a WHERE clause specifies `Table_type`, apply post-filter after collecting all tables. | P1 |
| 1.4 | `enginx.go` | Add `SHOW FULL TABLES` to the `*sqlparser.Show` case in the type switch, or pre-parse it to a `*sqlparser.Show` with `Full` flag. | P0 |

### Effort estimate / 工作量估计

~1-2 days for a developer familiar with the codebase.

---

## Issue 2: Unsupported statement type / 不支持的语句类型

### Symptom / 现象

```
[ERRO] SQL execution error: unsupported statement type
```

### Root cause / 根因

The `SystemVariableEngine` (`system_variable_engine.go`) can be the target engine for
queries that match `isSystemVariableQuery()` -- which includes anything containing
`INFORMATION_SCHEMA` -- but the engine's `ExecuteQuery()` cannot handle every query that
passes through this filter. Some queries reach the fallthrough case and return an error.

Specific problematic paths:
- `SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME ... FROM information_schema.SCHEMATA`
  is routed to `system_variable` engine because it contains `INFORMATION_SCHEMA`
- The `system_variable` engine can't process this query, returns "unsupported"

### Fix plan / 修复方案

| Step | File(s) | Description | Priority |
|---|---|---|---|
| 2.1 | `query_dispatcher.go` | Fix `isInformationSchemaMetadataQuery()` routing: information_schema SELECT queries should go to `innodb`, NOT `system_variable`. The `isSystemVariableQuery()` function should check for `INFORMATION_SCHEMA` before the `@@` check, and return false for pure information_schema SELECTs. | P0 |
| 2.2 | `system_variable_engine.go` | Add a fallback in `ExecuteQuery()`: if the query is not a system variable query after parsing, forward it to the innodb engine instead of returning an error. | P1 |
| 2.3 | `query_dispatcher.go` | Review and clean up the routing logic: remove `INFORMATION_SCHEMA` from `isSystemVariableQuery()` checks, add specific routing for `SELECT ... FROM information_schema.*` to `innodb`. | P0 |

### Effort estimate / 工作量估计

~0.5 day. Primarily routing logic fixes.

---

## Issue 3: Invalid system variable query / 无效的系统变量查询

### Symptom / 现象

```
[ERRO] SQL execution error: invalid system variable query
```

### Root cause / 根因

The `SystemVariableEngine` (`system_variable_engine.go`) `ExecuteQuery()` method passes
the query through multiple handlers (`executeShowStatement`, `executeSetStatement`,
`executeSystemFunctionQuery`, `executeInformationSchemaTablesQuery`). If none match, it
returns `fmt.Errorf("invalid system variable query")`.

The `SHOW VARIABLES LIKE 'lower_case_%'` query is matched by `isShowSystemVariable()`
and routed to the engine, but the engine's internal parser may not correctly handle the
`LIKE` clause format, causing it to fall through to the error path.

### Fix plan / 修复方案

| Step | File(s) | Description | Priority |
|---|---|---|---|
| 3.1 | `system_variable_engine.go` | Fix `executeShowStatement()` to correctly parse `LIKE` clauses with wildcard patterns (`lower_case_%`). Currently `executeShowVariables()` accepts `likePattern string` but may not correctly extract it from the parsed AST. | P0 |
| 3.2 | `system_variable_engine.go` | Add a fallback path in `ExecuteQuery()`: when no handler matches, route to `innodb` engine rather than returning an error. This ensures unmatched queries degrade gracefully. | P1 |
| 3.3 | `system_variable_engine.go` | Expand the `canHandleShowStatement()` known show types to include all standard MySQL SHOW types, with a comment explaining which are implemented vs pending. | P1 |

### Effort estimate / 工作量估计

~1 day for the LIKE parsing fix and fallback routing.

---

## Issue 4: Information_schema query gaps / 系统表查询缺口

### Symptom / 现象

Multiple information_schema queries executed by JDBC drivers fail or return empty
results:
- `SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'def'`
- `SELECT DISTINCT ROUTINE_SCHEMA ... JOIN PARAMETERS ...`
- `SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_TYPE ... FROM
   information_schema.COLUMNS WHERE TABLE_SCHEMA = 'def'`

### Root cause / 根因

The `information_schema` metadata is partially implemented. While `InfoSchemaManager`
provides basic schema/table/column queries, many `information_schema` tables are either
empty or return zero rows. The database `def` is a JDBC default schema that doesn't
exist in the server.

### Fix plan / 修复方案

| Step | File(s) | Description | Priority |
|---|---|---|---|
| 4.1 | `metadata/information_schemas.go` | Implement the `TABLES` table in information_schema: populate `TABLE_SCHEMA`, `TABLE_NAME`, `TABLE_TYPE`, `ENGINE`, `TABLE_ROWS`, etc. | P0 |
| 4.2 | `metadata/information_schemas.go` | Implement the `COLUMNS` table: populate `TABLE_SCHEMA`, `TABLE_NAME`, `COLUMN_NAME`, `ORDINAL_POSITION`, `COLUMN_DEFAULT`, `IS_NULLABLE`, `DATA_TYPE`, `CHARACTER_MAXIMUM_LENGTH`, `COLUMN_TYPE`, `COLUMN_KEY`, `EXTRA`, etc. | P0 |
| 4.3 | `metadata/information_schemas.go` | Implement the `ROUTINES`, `PARAMETERS` tables -- even as empty tables -- to avoid JDBC driver errors. | P1 |
| 4.4 | `metadata/information_schemas.go` | Implement the `ENGINES` table to support `SELECT ... FROM information_schema.ENGINES WHERE Engine = 'ndbcluster'`. Return at least the `InnoDB` engine row. | P1 |
| 4.5 | `metadata/information_schemas.go` | Implement the `SCHEMATA` table if not already fully populated: `SCHEMA_NAME`, `DEFAULT_CHARACTER_SET_NAME`, `DEFAULT_COLLATION_NAME`. | P0 |
| 4.6 | `query_dispatcher.go` | Fix routing: queries targeting `INFORMATION_SCHEMA` should always route to `innodb`, not `system_variable`. See Issue 2.1. | P0 |

### Effort estimate / 工作量估计

~2-3 days. The bulk of the work is in `metadata/information_schemas.go` which requires
querying the storage manager for each table's metadata and returning well-formed rows.

---

## Issue 5: Database 'def' does not exist / 数据库 'def' 不存在

### Symptom / 现象

```
[WARN] database 'def' does not exist (COM_INIT_DB: def)
```

### Root cause / 根因

JDBC connector/J sends `COM_INIT_DB: def` as part of its connection initialization
(using `def` as a default placeholder schema). The server correctly reports that `def`
does not exist but continues processing. This is harmless -- the JDBC driver ignores
the error and continues with the actual database.

### Fix plan / 修复方案

This is a **low priority** issue:

| Option | Description | Effort |
|---|---|---|
| A. Suppress log | Add `def` to a known list of COM_INIT_DB false positives; log at DEBUG instead of WARN. | ~0.1 day |
| B. No action | The JDBC driver handles this gracefully. Only the log warning is noisy. | None |

**Recommendation**: Option A (suppress log) + Option B (no functional change).

---

## Implementation order / 实施顺序

| Priority | Issue | Reason |
|---|---|---|
| P0 | Issue 1: SHOW FULL TABLES | Blocks all JDBC/SQL client table browsing |
| P0 | Issue 2 + 4: Routing + information_schema | Blocks all ORM tooling and JDBC metadata queries |
| P1 | Issue 3: SHOW VARIABLES LIKE | Blocks SET/VARIABLES exploration |
| P2 | Issue 5: database 'def' | Harmless warning, cosmetic only |

---

## Verification / 验证方法

1. Run the existing test suite to confirm no regressions:
   ```bash
   go test ./server/dispatcher ./server/innodb/engine
   ```

2. Connect with a JDBC client (e.g. DBeaver, IntelliJ) and verify:
   - Database tree loads correctly (SHOW DATABASES, SHOW TABLES)
   - Table structure is visible (SHOW COLUMNS, information_schema.COLUMNS)
   - System variables are queryable (SHOW VARIABLES LIKE ...)
   - Queries against information_schema return data

3. Run the JDBC test suite:
   ```bash
   cd jdbc_client && mvn test -Pjdbc-connectivity
   ```
*** End of File
