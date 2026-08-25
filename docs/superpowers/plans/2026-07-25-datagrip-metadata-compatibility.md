# DataGrip Metadata Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make XMySQL tolerate DataGrip full metadata introspection without `invalid system variable query`, `unsupported statement type`, or parser failures for the observed metadata SQL.

**Architecture:** Keep compatibility handling in the existing dispatcher and InnoDB executor metadata path. Route all supported `information_schema` and selected `mysql.*` metadata probes away from the system-variable engine, then return valid MySQL-shaped empty result sets unless the engine can cheaply derive real rows from `.frm` metadata.

**Tech Stack:** Go 1.24.3 from `/Users/zhukovasky/sdk/go1.24.3/bin/go`, Maven/JUnit JDBC tests under `jdbc_client`, MySQL Connector/J 8.0.33, existing SQL parser under `server/innodb/sqlparser`.

---

## Current Evidence

DataGrip connects to `127.0.0.1:3309` successfully, then fails during metadata introspection. The observed failing query families are:

- `select table_name, auto_increment from information_schema.tables where table_schema = 'performance_schema' and auto_increment is not null`
- `select table_name, partition_name, ... from information_schema.partitions ...`
- `select trigger_name, ... from information_schema.triggers ...`
- `select event_name, ... from information_schema.events ...`
- `select table_name, view_definition, definer from information_schema.views ...`
- `select ... from information_schema.column_privileges ... union all select ... from information_schema.table_privileges ...`
- `select Host, User, Routine_name, Proc_priv, Routine_type = 'PROCEDURE' as is_proc from mysql.procs_priv where Db = 'performance_schema'`

Current error classes:

- `invalid system variable query`: metadata table is still classified as a system table/query.
- `unsupported statement type`: parsed `UNION ALL` reaches a statement dispatch path that only handles `SELECT`.
- `parse error ... near 'auto_increment'`: DataGrip projection/filter shape for `information_schema.tables` is not handled before the parser path rejects it.

## File Structure

- Modify: `server/dispatcher/system_variable_engine.go`
  - Add the complete DataGrip metadata table allowlist so these probes route to InnoDB instead of the system-variable engine.
- Modify: `server/dispatcher/system_variable_engine_test.go`
  - Add routing tests for `views`, `partitions`, `triggers`, `events`, and `mysql.procs_priv`.
- Modify: `server/innodb/engine/executor.go`
  - Add metadata fast paths for the new `information_schema` tables, `mysql.procs_priv`, and metadata `UNION ALL`.
  - Make `information_schema.tables` column shape follow the requested projection for `auto_increment`.
- Modify: `server/innodb/engine/executor_ddl_test.go`
  - Add executor tests for DataGrip metadata SQL shapes.
- Modify: `jdbc_client/src/test/java/com/xmysql/server/test/JdbcConnectionTest.java`
  - Add a DataGrip-style metadata probe test that executes the exact observed SQL strings over JDBC.

## Compatibility Boundary

This plan does not implement real views, partitions, triggers, events, routines, or grants. It implements MySQL-compatible metadata result shapes so clients can finish introspection. Empty rows are valid when the feature is not implemented.

---

### Task 1: Route All Observed Metadata Probes To InnoDB

**Files:**
- Modify: `server/dispatcher/system_variable_engine.go`
- Modify: `server/dispatcher/system_variable_engine_test.go`

- [ ] **Step 1: Add failing dispatcher tests**

Add these cases to `TestSystemVariableEngine_DoesNotRouteJDBCInformationSchemaProbes` in `server/dispatcher/system_variable_engine_test.go`:

```go
queries := []string{
	"SELECT ROUTINE_SCHEMA AS PROCEDURE_CAT FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME LIKE '%'",
	"SELECT SPECIFIC_SCHEMA AS PROCEDURE_CAT FROM INFORMATION_SCHEMA.PARAMETERS WHERE SPECIFIC_NAME LIKE '%'",
	"SELECT TABLE_SCHEMA AS TABLE_CAT FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_NAME = 'users'",
	"SELECT A.TABLE_SCHEMA AS FKTABLE_CAT FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE A JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS B USING (TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME)",
	"SELECT R.CONSTRAINT_NAME FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS R",
	"select table_name, view_definition, definer from information_schema.views where table_schema = 'app'",
	"select table_name, partition_name from information_schema.partitions where table_schema = 'app'",
	"select trigger_name, event_manipulation from information_schema.triggers where trigger_schema = 'app'",
	"select event_name, event_definition from information_schema.events where event_schema = 'app'",
	"select Host, User, Routine_name, Proc_priv, Routine_type = 'PROCEDURE' as is_proc from mysql.procs_priv where Db = 'app'",
}
```

- [ ] **Step 2: Run dispatcher test and verify failure**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/dispatcher -run 'TestSystemVariableEngine_DoesNotRouteJDBCInformationSchemaProbes' -count=1
```

Expected before implementation: at least `views`, `partitions`, `triggers`, `events`, or `mysql.procs_priv` is routed to `system_variable` or returns `CanHandle=true`.

- [ ] **Step 3: Extend dispatcher metadata allowlist**

In `server/dispatcher/system_variable_engine.go`, replace `informationSchemaMetadataTableNames()` with:

```go
func informationSchemaMetadataTableNames() []string {
	return []string{
		"tables",
		"columns",
		"schemata",
		"routines",
		"parameters",
		"statistics",
		"key_column_usage",
		"table_constraints",
		"referential_constraints",
		"column_privileges",
		"table_privileges",
		"views",
		"partitions",
		"triggers",
		"events",
	}
}
```

Add a helper for DataGrip's `mysql.procs_priv` probe:

```go
func isMySQLMetadataQuery(query string) bool {
	lower := strings.ToLower(strings.TrimSpace(query))
	lower = strings.ReplaceAll(lower, "`", "")
	lower = regexp.MustCompile(`\s*\.\s*`).ReplaceAllString(lower, ".")
	return strings.Contains(lower, "mysql.procs_priv")
}
```

Update `CanHandle` and `isSystemTable` checks so `mysql.procs_priv` returns `false` from the system-variable engine:

```go
if isInformationSchemaMetadataQuery(query) || isMySQLMetadataQuery(query) {
	logger.Debugf(" [SystemVariableEngine.CanHandle] metadata query uses innodb engine")
	return false
}
```

In `isSystemTable`, before returning true for `MYSQL.PROCS_PRIV`, add:

```go
if qualifierStr == "MYSQL" && tableNameStr == "PROCS_PRIV" {
	return false
}
```

- [ ] **Step 4: Run dispatcher test and verify pass**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/dispatcher -run 'TestSystemVariableEngine_DoesNotRouteJDBCInformationSchemaProbes' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/dispatcher/system_variable_engine.go server/dispatcher/system_variable_engine_test.go
git commit -m "fix: route datagrip metadata probes to innodb"
```

---

### Task 2: Add Empty Result Shapes For New Metadata Tables

**Files:**
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/innodb/engine/executor_ddl_test.go`

- [ ] **Step 1: Add failing executor tests**

Append these cases to `TestInformationSchemaJDBCProbeTablesReturnEmptyMetadataResults` in `server/innodb/engine/executor_ddl_test.go`:

```go
{
	name:    "views",
	query:   "select table_name, view_definition, definer from information_schema.views where table_schema = 'performance_schema'",
	columns: []string{"TABLE_NAME", "VIEW_DEFINITION", "DEFINER"},
},
{
	name:    "partitions",
	query:   "select table_name, partition_name, subpartition_name, partition_ordinal_position, subpartition_ordinal_position, partition_method, subpartition_method, partition_expression, subpartition_expression, partition_description, table_rows, avg_row_length, data_length, max_data_length, index_length, data_free, create_time, update_time, check_time, checksum, partition_comment, nodegroup, tablespace_name from information_schema.partitions where table_schema = 'performance_schema'",
	columns: []string{"TABLE_NAME", "PARTITION_NAME", "SUBPARTITION_NAME", "PARTITION_ORDINAL_POSITION", "SUBPARTITION_ORDINAL_POSITION", "PARTITION_METHOD", "SUBPARTITION_METHOD", "PARTITION_EXPRESSION", "SUBPARTITION_EXPRESSION", "PARTITION_DESCRIPTION", "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE", "CREATE_TIME", "UPDATE_TIME", "CHECK_TIME", "CHECKSUM", "PARTITION_COMMENT", "NODEGROUP", "TABLESPACE_NAME"},
},
{
	name:    "triggers",
	query:   "select trigger_name, event_manipulation, event_object_table, action_statement, action_timing, definer from information_schema.triggers where trigger_schema = 'performance_schema'",
	columns: []string{"TRIGGER_NAME", "EVENT_MANIPULATION", "EVENT_OBJECT_TABLE", "ACTION_STATEMENT", "ACTION_TIMING", "DEFINER"},
},
{
	name:    "events",
	query:   "select event_name, event_definition, event_type, execute_at, interval_value, interval_field, status, definer from information_schema.events where event_schema = 'performance_schema'",
	columns: []string{"EVENT_NAME", "EVENT_DEFINITION", "EVENT_TYPE", "EXECUTE_AT", "INTERVAL_VALUE", "INTERVAL_FIELD", "STATUS", "DEFINER"},
},
{
	name:    "mysql procs priv",
	query:   "select Host, User, Routine_name, Proc_priv, Routine_type = 'PROCEDURE' as is_proc from mysql.procs_priv where Db = 'performance_schema'",
	columns: []string{"HOST", "USER", "ROUTINE_NAME", "PROC_PRIV", "IS_PROC"},
},
```

- [ ] **Step 2: Run executor test and verify failure**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestInformationSchemaJDBCProbeTablesReturnEmptyMetadataResults' -count=1 -timeout=120s
```

Expected before implementation: one or more cases fail with unsupported query, invalid system-variable query, or wrong columns.

- [ ] **Step 3: Extend engine metadata allowlist**

In `server/innodb/engine/executor.go`, update `informationSchemaMetadataTableNames()` to include the same new tables as Task 1:

```go
func informationSchemaMetadataTableNames() []string {
	return []string{
		"tables",
		"columns",
		"schemata",
		"routines",
		"parameters",
		"statistics",
		"key_column_usage",
		"table_constraints",
		"referential_constraints",
		"column_privileges",
		"table_privileges",
		"views",
		"partitions",
		"triggers",
		"events",
	}
}
```

- [ ] **Step 4: Add metadata column helpers**

Add these helpers near the existing `jdbcTablePrivilegesMetadataColumns()` helper in `server/innodb/engine/executor.go`:

```go
func jdbcViewsMetadataColumns() []string {
	return []string{"TABLE_NAME", "VIEW_DEFINITION", "DEFINER"}
}

func jdbcPartitionsMetadataColumns() []string {
	return []string{"TABLE_NAME", "PARTITION_NAME", "SUBPARTITION_NAME", "PARTITION_ORDINAL_POSITION", "SUBPARTITION_ORDINAL_POSITION", "PARTITION_METHOD", "SUBPARTITION_METHOD", "PARTITION_EXPRESSION", "SUBPARTITION_EXPRESSION", "PARTITION_DESCRIPTION", "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE", "CREATE_TIME", "UPDATE_TIME", "CHECK_TIME", "CHECKSUM", "PARTITION_COMMENT", "NODEGROUP", "TABLESPACE_NAME"}
}

func jdbcTriggersMetadataColumns() []string {
	return []string{"TRIGGER_NAME", "EVENT_MANIPULATION", "EVENT_OBJECT_TABLE", "ACTION_STATEMENT", "ACTION_TIMING", "DEFINER"}
}

func jdbcEventsMetadataColumns() []string {
	return []string{"EVENT_NAME", "EVENT_DEFINITION", "EVENT_TYPE", "EXECUTE_AT", "INTERVAL_VALUE", "INTERVAL_FIELD", "STATUS", "DEFINER"}
}

func jdbcMySQLProcsPrivMetadataColumns() []string {
	return []string{"HOST", "USER", "ROUTINE_NAME", "PROC_PRIV", "IS_PROC"}
}
```

- [ ] **Step 5: Wire new result shapes**

In `executeInformationSchemaMetadataSelect(query string)`, add cases:

```go
case strings.Contains(lower, "information_schema.views"):
	return newInformationSchemaSelectResult("information_schema_views", jdbcViewsMetadataColumns(), nil), true, nil
case strings.Contains(lower, "information_schema.partitions"):
	return newInformationSchemaSelectResult("information_schema_partitions", jdbcPartitionsMetadataColumns(), nil), true, nil
case strings.Contains(lower, "information_schema.triggers"):
	return newInformationSchemaSelectResult("information_schema_triggers", jdbcTriggersMetadataColumns(), nil), true, nil
case strings.Contains(lower, "information_schema.events"):
	return newInformationSchemaSelectResult("information_schema_events", jdbcEventsMetadataColumns(), nil), true, nil
case strings.Contains(lower, "mysql.procs_priv"):
	return newInformationSchemaSelectResult("mysql_procs_priv", jdbcMySQLProcsPrivMetadataColumns(), nil), true, nil
```

Also update the early guard:

```go
if !isInformationSchemaMetadataQuery(query) && !isMySQLMetadataQuery(query) {
	return nil, false, nil
}
```

Add the same `isMySQLMetadataQuery` helper used in the dispatcher if it is not shared.

- [ ] **Step 6: Run executor test and verify pass**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestInformationSchemaJDBCProbeTablesReturnEmptyMetadataResults' -count=1 -timeout=120s
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add server/innodb/engine/executor.go server/innodb/engine/executor_ddl_test.go
git commit -m "fix: add datagrip metadata result shapes"
```

---

### Task 3: Handle DataGrip Metadata UNION ALL

**Files:**
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/innodb/engine/executor_ddl_test.go`

- [ ] **Step 1: Add failing UNION test**

Add this test to `server/innodb/engine/executor_ddl_test.go`:

```go
func TestInformationSchemaPrivilegesUnionAllReturnsEmptyResult(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)

	query := "select grantee, table_name, column_name, privilege_type, is_grantable from information_schema.column_privileges where table_schema = 'performance_schema' union all select grantee, table_name, null as column_name, privilege_type, is_grantable from information_schema.table_privileges where table_schema = 'performance_schema'"
	got := <-executor.ExecuteQuery(nil, query, "")
	require.NoError(t, got.Err)

	result, ok := got.Data.(*SelectResult)
	require.True(t, ok, "expected SelectResult, got %T", got.Data)
	require.Equal(t, []string{"GRANTEE", "TABLE_NAME", "COLUMN_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE"}, result.Columns)
	require.Empty(t, result.Records)
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestInformationSchemaPrivilegesUnionAllReturnsEmptyResult' -count=1 -timeout=120s
```

Expected before implementation: FAIL with `unsupported statement type: *sqlparser.Union` or equivalent.

- [ ] **Step 3: Add metadata union fast path before statement dispatch rejects Union**

In `server/innodb/engine/executor.go`, locate the top-level statement dispatch that emits `unsupported statement type`. Before the default branch, handle `*sqlparser.Union`:

```go
case *sqlparser.Union:
	if result, handled, err := e.executeInformationSchemaMetadataSelect(query); handled {
		if err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_ERROR, Message: err.Error()}
			return
		}
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: result.Message}
		return
	}
```

Use the local variable names from the existing function. If the dispatch function does not have `ctx`, send to the existing `results` channel in the same shape used for `SELECT`.

- [ ] **Step 4: Add explicit union result shape**

At the top of `executeInformationSchemaMetadataSelect(query string)`, add:

```go
if strings.Contains(lower, "information_schema.column_privileges") &&
	strings.Contains(lower, "information_schema.table_privileges") &&
	strings.Contains(lower, " union all ") {
	return newInformationSchemaSelectResult(
		"information_schema_privileges_union",
		[]string{"GRANTEE", "TABLE_NAME", "COLUMN_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE"},
		nil,
	), true, nil
}
```

- [ ] **Step 5: Run test and verify pass**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestInformationSchemaPrivilegesUnionAllReturnsEmptyResult' -count=1 -timeout=120s
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add server/innodb/engine/executor.go server/innodb/engine/executor_ddl_test.go
git commit -m "fix: support datagrip metadata union probe"
```

---

### Task 4: Support Requested Projection For `information_schema.tables`

**Files:**
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/innodb/engine/executor_ddl_test.go`

- [ ] **Step 1: Add failing auto_increment projection test**

Add this test to `server/innodb/engine/executor_ddl_test.go`:

```go
func TestInformationSchemaTablesAutoIncrementProjectionReturnsRequestedColumns(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)

	got := <-executor.ExecuteQuery(nil,
		"select table_name, auto_increment from information_schema.tables where table_schema = 'performance_schema' and auto_increment is not null",
		"performance_schema",
	)
	require.NoError(t, got.Err)

	result, ok := got.Data.(*SelectResult)
	require.True(t, ok, "expected SelectResult, got %T", got.Data)
	require.Equal(t, []string{"TABLE_NAME", "AUTO_INCREMENT"}, result.Columns)
	require.Empty(t, result.Records)
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestInformationSchemaTablesAutoIncrementProjectionReturnsRequestedColumns' -count=1 -timeout=120s
```

Expected before implementation: FAIL with parser error near `auto_increment`, wrong columns, or non-empty incompatible row shape.

- [ ] **Step 3: Add projection detector**

In `server/innodb/engine/executor.go`, add:

```go
func informationSchemaTablesRequestedColumns(query string) []string {
	lower := strings.ToLower(query)
	if strings.Contains(lower, "table_name") && strings.Contains(lower, "auto_increment") {
		return []string{"TABLE_NAME", "AUTO_INCREMENT"}
	}
	return manager.JDBCTablesMetadataColumns()
}
```

- [ ] **Step 4: Apply requested projection**

Change the final line of `executeInformationSchemaTablesSelect`:

```go
return newInformationSchemaSelectResult("information_schema_tables", informationSchemaTablesRequestedColumns(query), rows)
```

At the start of `executeInformationSchemaTablesSelect`, after `schemaPattern` and `tablePattern` are computed, return an empty compatible result for `auto_increment is not null`:

```go
if columns := informationSchemaTablesRequestedColumns(query); len(columns) == 2 && columns[1] == "AUTO_INCREMENT" {
	return newInformationSchemaSelectResult("information_schema_tables", columns, nil)
}
```

Because this engine currently does not expose table-level next auto-increment in metadata, returning zero rows for `auto_increment is not null` is the compatible behavior.

- [ ] **Step 5: Run test and verify pass**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestInformationSchemaTablesAutoIncrementProjectionReturnsRequestedColumns' -count=1 -timeout=120s
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add server/innodb/engine/executor.go server/innodb/engine/executor_ddl_test.go
git commit -m "fix: support datagrip tables auto increment metadata"
```

---

### Task 5: Add JDBC DataGrip Introspection Regression

**Files:**
- Modify: `jdbc_client/src/test/java/com/xmysql/server/test/JdbcConnectionTest.java`

- [ ] **Step 1: Add JDBC regression test**

Add this test after `testDataGripMetadataProbeQueriesDoNotFail`:

```java
@Test
@Order(9)
@DisplayName("DataGrip完整元数据SQL探测不应失败")
public void testDataGripFullMetadataSqlProbesDoNotFail() throws Exception {
    assumeTrue(SERVER_AVAILABLE, "XMySQL 未运行在 localhost:3309，跳过连接测试");
    try (Connection conn = DriverManager.getConnection(BASE_URL, USER, PASSWORD);
         Statement stmt = conn.createStatement()) {
        String[] queries = {
            "select table_name, auto_increment from information_schema.tables where table_schema = 'performance_schema' and auto_increment is not null",
            "select table_name, index_name, index_comment, index_type, non_unique, column_name, sub_part, collation, expression from information_schema.statistics where table_schema = 'performance_schema'",
            "select c.constraint_name, c.constraint_schema, c.table_name, c.constraint_type, c.enforced = 'YES' enforced from information_schema.table_constraints c where c.table_schema = 'performance_schema'",
            "select constraint_name, table_name, column_name, referenced_table_schema, referenced_table_name, referenced_column_name from information_schema.key_column_usage where table_schema = 'performance_schema'",
            "select table_name, partition_name, subpartition_name, partition_ordinal_position, subpartition_ordinal_position, partition_method, subpartition_method, partition_expression, subpartition_expression, partition_description, table_rows, avg_row_length, data_length, max_data_length, index_length, data_free, create_time, update_time, check_time, checksum, partition_comment, nodegroup, tablespace_name from information_schema.partitions where table_schema = 'performance_schema'",
            "select trigger_name, event_manipulation, event_object_table, action_statement, action_timing, definer from information_schema.triggers where trigger_schema = 'performance_schema'",
            "select event_name, event_definition, event_type, execute_at, interval_value, interval_field, status, definer from information_schema.events where event_schema = 'performance_schema'",
            "select routine_name, routine_type, routine_definition, routine_comment, dtd_identifier, definer from information_schema.routines where routine_schema = 'performance_schema'",
            "select Host, User, Routine_name, Proc_priv, Routine_type = 'PROCEDURE' as is_proc from mysql.procs_priv where Db = 'performance_schema'",
            "select grantee, table_name, column_name, privilege_type, is_grantable from information_schema.column_privileges where table_schema = 'performance_schema' union all select grantee, table_name, null as column_name, privilege_type, is_grantable from information_schema.table_privileges where table_schema = 'performance_schema'",
            "select table_name, view_definition, definer from information_schema.views where table_schema = 'performance_schema'"
        };

        for (String query : queries) {
            try (ResultSet rs = stmt.executeQuery(query)) {
                ResultSetMetaData meta = rs.getMetaData();
                assertThat(meta.getColumnCount()).as(query).isGreaterThan(0);
                while (rs.next()) {
                    // Compatibility boundary: DataGrip metadata SQL must return a valid result set.
                }
            }
        }
    }
}
```

- [ ] **Step 2: Start server for JDBC test**

Run in repo root:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go run . --configPath=conf/jdbc_local.ini
```

Expected: server listens on `127.0.0.1:3309`.

- [ ] **Step 3: Run JDBC regression and verify failure before fixes**

Run in another shell:

```bash
cd /Users/zhukovasky/GolandProjects/xmysql-server/jdbc_client
mvn -q '-Dtest=JdbcConnectionTest#testDataGripFullMetadataSqlProbesDoNotFail' test
```

Expected before Tasks 1-4 are complete: FAIL with one of the observed metadata errors.

- [ ] **Step 4: Run JDBC regression and verify pass after fixes**

Run:

```bash
cd /Users/zhukovasky/GolandProjects/xmysql-server/jdbc_client
mvn -q '-Dtest=JdbcConnectionTest#testDataGripFullMetadataSqlProbesDoNotFail' test
```

Expected after Tasks 1-4 are complete: PASS.

- [ ] **Step 5: Commit**

```bash
git add jdbc_client/src/test/java/com/xmysql/server/test/JdbcConnectionTest.java
git commit -m "test: cover datagrip full metadata probes"
```

---

### Task 6: Final Verification Against DataGrip

**Files:**
- Read: `tmp/jdbc_logs/error.log`
- Read: live `go run . --configPath=conf/jdbc_local.ini` terminal output

- [ ] **Step 1: Run focused Go tests**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/dispatcher -run 'TestSystemVariableEngine_DoesNotRouteJDBCInformationSchemaProbes' -count=1 -timeout=60s
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestInformationSchema|Test.*Metadata|Test.*PrivilegesUnion|Test.*AutoIncrement' -count=1 -timeout=120s
```

Expected: PASS.

- [ ] **Step 2: Run full JDBC connection suite**

With the server running on `127.0.0.1:3309`, run:

```bash
cd /Users/zhukovasky/GolandProjects/xmysql-server/jdbc_client
mvn -q -Dtest=JdbcConnectionTest test
```

Expected: PASS.

- [ ] **Step 3: Manually retry DataGrip**

Use DataGrip with:

```text
Host: 127.0.0.1
Port: 3309
User: root
Password: root@1234
Driver: MySQL Connector/J
```

Expected: schema introspection completes without these messages in current logs:

```text
invalid system variable query
unsupported statement type
parse error: syntax error at position 74 near 'auto_increment'
```

- [ ] **Step 4: Inspect error log**

Run:

```bash
tail -n 200 tmp/jdbc_logs/error.log
```

Expected: no new DataGrip metadata errors after the retry timestamp.

- [ ] **Step 5: Clean generated runtime artifacts before commit or PR**

Check:

```bash
git status --short
```

Do not commit generated files from:

```text
jdbc_client/target/
server/net/data/
tmp/
```

- [ ] **Step 6: Final commit**

If Tasks 1-5 were not already committed separately:

```bash
git add server/dispatcher/system_variable_engine.go server/dispatcher/system_variable_engine_test.go server/innodb/engine/executor.go server/innodb/engine/executor_ddl_test.go jdbc_client/src/test/java/com/xmysql/server/test/JdbcConnectionTest.java
git commit -m "fix: support datagrip metadata introspection"
```

## Self-Review

- Spec coverage: covers all observed DataGrip failures from live logs: `views`, `partitions`, `triggers`, `events`, `routines`, `mysql.procs_priv`, privileges `UNION ALL`, and `tables.auto_increment`.
- Placeholder scan: no unresolved placeholder markers or unspecified test commands are left in this plan.
- Type consistency: all Go snippets use existing `SelectResult`, `newInformationSchemaSelectResult`, `common.RESULT_TYPE_QUERY`, `ExecuteQuery`, and current test helper patterns.
