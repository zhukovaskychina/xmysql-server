# JDBC MySQL Compatibility Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make XMySQL pass the current JDBC compatibility gaps around DDL metadata, JDBC transaction commands, TRUNCATE metadata remapping, and core constraints.

**Architecture:** Keep changes inside the existing SQL executor, session/protocol dispatcher, storage metadata mapping, and JDBC integration tests. Implement the smallest MySQL-compatible behavior required by the current JDBC suites first, then add durable tests at Go and JDBC levels so each fix is independently verifiable.

**Tech Stack:** Go 1.24.3 from `/Users/zhukovasky/sdk/go1.24.3/bin/go`, Maven/JUnit JDBC tests under `jdbc_client`, MySQL Connector/J 8.0.33, existing SQL parser under `server/innodb/sqlparser`.

## Global Constraints

- Do not use Alibaba-style jargon in docs or comments.
- Use `/Users/zhukovasky/sdk/go1.24.3/bin/go` and `/Users/zhukovasky/sdk/go1.24.3/bin/gofmt` for Go commands.
- If Python is needed, use `python3.12`; if unavailable, use shell, Go, Maven, or jq instead.
- Keep changes scoped to current compatibility failures; do not rewrite the storage engine.
- Preserve current passing baseline: `mvn test -Dtest=DMLOperationsTest` must keep passing.
- Run JDBC tests with server started by `go run . -configPath=conf/jdbc_local.ini`.
- Clean or restore generated runtime files before committing: `server/net/data`, `tmp/jdbc_logs`, and `jdbc_client/target/surefire-reports`.

---

## Current Failure Summary

Fresh scan on 2026-07-16 confirmed:

- `DMLOperationsTest`: PASS, 13/13.
- `DDLOperationsTest`: FAIL. Main issues: `SHOW DATABASES LIKE` returns a non-navigable result set, `DROP DATABASE IF EXISTS` errors for missing databases, `ALTER TABLE` unsupported, `DatabaseMetaData.getTables()` hits unsupported information-schema/system-variable query forms.
- `TransactionTest`: FAIL, 8/8. Main issues: JDBC `commit()`/`rollback()`, `BEGIN`, and `SAVEPOINT` are not accepted.
- `PreparedStatementTest`: FAIL, 12/12. Main root cause: `TRUNCATE TABLE users` remaps table storage, then DML cannot resolve `test_prepared_stmt.users` metadata.
- `IndexAndConstraintTest`: partially supported. Basic UNIQUE works through `DMLOperationsTest`; wider NOT NULL, composite primary key, foreign key, CHECK, FULLTEXT, and cascade behavior are not closed.

## File Structure

- Modify: `server/innodb/engine/executor.go`
  - DDL dispatch, `DROP DATABASE IF EXISTS`, `ALTER TABLE ADD COLUMN`, `TRUNCATE TABLE`, `SHOW DATABASES`, `SHOW TABLES`, `SHOW COLUMNS`.
- Modify: `server/innodb/engine/enginx.go`
  - Top-level statement routing for transaction statements if the parser emits them before `XMySQLExecutor.Execute`.
- Modify: `server/dispatcher/enhanced_message_handler.go`
  - Protocol/session-level handling if Connector/J sends `COMMIT`, `ROLLBACK`, or transaction statements through the dispatcher before engine routing.
- Modify: `server/dispatcher/system_variable_engine.go`
  - Add targeted support for metadata queries that are currently misclassified as invalid system-variable queries.
- Modify: `server/innodb/manager/table_storage_mapping.go`
  - Add or expose table-storage remap helpers if TRUNCATE needs atomic mapping replacement.
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
  - Enforce NOT NULL and composite-key uniqueness if not already handled before row write.
- Test: `server/innodb/engine/executor_show_routing_test.go`
  - Unit tests for SHOW result shape and LIKE filtering.
- Test: `server/innodb/engine/executor_ddl_test.go`
  - New unit tests for DROP DATABASE IF EXISTS, ALTER ADD COLUMN, and TRUNCATE mapping.
- Test: `server/innodb/engine/executor_transaction_statement_test.go`
  - New unit tests for BEGIN, COMMIT, ROLLBACK, SAVEPOINT routing.
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/DDLOperationsTest.java`
  - Existing acceptance suite, do not weaken assertions.
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/PreparedStatementTest.java`
  - Existing acceptance suite, do not weaken assertions.
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/TransactionTest.java`
  - Existing acceptance suite, do not weaken assertions.
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/IndexAndConstraintTest.java`
  - Existing acceptance suite, use as staged acceptance for constraints.

---

### Task 1: DDL Metadata Protocol Baseline

**Files:**
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/dispatcher/system_variable_engine.go`
- Test: `server/innodb/engine/executor_show_routing_test.go`
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/DDLOperationsTest.java`

**Interfaces:**
- Consumes: `func (e *XMySQLExecutor) executeShowDatabasesWithQuery(ctx *ExecutionContext, stmt *sqlparser.Show, rawQuery string)`
- Produces: Standard query result map: `map[string]interface{}{"columns":[]string, "rows":[][]interface{}}`
- Produces: `DROP DATABASE IF EXISTS missing_db` returns an OK result without `Err`.

- [ ] **Step 1: Add failing Go tests for SHOW DATABASES LIKE result shape**

Add to `server/innodb/engine/executor_show_routing_test.go`:

```go
func TestShowDatabasesLikeReturnsNavigableResult(t *testing.T) {
	tmp := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tmp, "app_db"), 0755))

	executor := NewXMySQLExecutor(&conf.Cfg{
		InnodbDataDir: tmp,
	})
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results}

	executor.executeShowDatabasesWithQuery(ctx, &sqlparser.Show{Type: "databases"}, "show databases like 'app_%'")

	got := <-results
	require.NoError(t, got.Err)
	require.Equal(t, "QUERY", got.ResultType)
	data, ok := got.Data.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, []string{"Database"}, data["columns"])
	require.Equal(t, [][]interface{}{{"app_db"}}, data["rows"])
}
```

- [ ] **Step 2: Add failing Go test for DROP DATABASE IF EXISTS**

Create `server/innodb/engine/executor_ddl_test.go` if it does not exist:

```go
func TestDropDatabaseIfExistsMissingIsOk(t *testing.T) {
	tmp := t.TempDir()
	executor := NewXMySQLExecutor(&conf.Cfg{InnodbDataDir: tmp})
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results}

	executor.executeDropDatabaseStatement(ctx, &sqlparser.DBDDL{
		Action:   "drop",
		DBName:   "missing_db",
		IfExists: true,
	})

	got := <-results
	require.NoError(t, got.Err)
	require.Equal(t, common.RESULT_TYPE_DDL, got.ResultType)
}
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestShowDatabasesLikeReturnsNavigableResult|TestDropDatabaseIfExistsMissingIsOk' -count=1
```

Expected before fix: at least one test fails with current result-shape or IF EXISTS behavior.

- [ ] **Step 4: Fix SHOW DATABASES LIKE result shape**

In `server/innodb/engine/executor.go`, keep `executeShowDatabasesWithQuery` returning only `ResultType: common.RESULT_TYPE_QUERY` and a result data map with exactly:

```go
resultData := map[string]interface{}{
	"columns": []string{"Database"},
	"rows":    rows,
}

ctx.Results <- &Result{
	ResultType: common.RESULT_TYPE_QUERY,
	Data:       resultData,
	Message:    fmt.Sprintf("Found %d databases", len(rows)),
}
```

Also remove any alternate branch that returns OK for `SHOW DATABASES LIKE`. If `tryExecuteShowExecutor` returns an OK-like result for this path, bypass it for `SHOW DATABASES` until it emits the same query map shape.

- [ ] **Step 5: Fix DROP DATABASE IF EXISTS for missing databases**

In `server/innodb/engine/executor.go`, ensure `dropDatabaseImpl` checks path existence before clearing runtime table mappings when the database path does not exist:

```go
dbPath := filepath.Join(e.getDataDir(), dbName)
if _, err := os.Stat(dbPath); os.IsNotExist(err) {
	if ifExists {
		logger.Debugf("Database '%s' does not exist, skipping drop due to IF EXISTS", dbName)
		return nil
	}
	return fmt.Errorf("database '%s' does not exist", dbName)
}
```

Only after that block should it clear auto-increment state, unregister table mappings, and remove the directory.

- [ ] **Step 6: Add targeted metadata query support for `DatabaseMetaData.getTables()`**

In `server/dispatcher/system_variable_engine.go`, before classifying a query as a system-variable query, route common `information_schema.tables` metadata queries to the engine/executor path instead of returning `invalid system variable query`.

Minimum recognized pattern:

```sql
SELECT ... FROM information_schema.tables WHERE table_schema ... AND table_name ...
```

Return columns expected by Connector/J metadata queries at minimum:

```text
TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS
```

Rows should be derived from `.frm` files under `server/net/data/<schema>`.

- [ ] **Step 7: Run Go tests**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/dispatcher -count=1
```

Expected: PASS.

- [ ] **Step 8: Run JDBC DDL smoke**

Start server:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go run . -configPath=conf/jdbc_local.ini
```

In another shell:

```bash
cd /Users/zhukovasky/GolandProjects/xmysql-server/jdbc_client
mvn test -Dtest=DDLOperationsTest
```

Expected after this task: failures should no longer include `Not a navigable ResultSet`, `invalid system variable query`, or `DROP DATABASE IF EXISTS` on missing DB. `ALTER TABLE` may still fail until Task 2.

- [ ] **Step 9: Commit**

```bash
git add server/innodb/engine/executor.go server/dispatcher/system_variable_engine.go server/innodb/engine/executor_show_routing_test.go server/innodb/engine/executor_ddl_test.go
git commit -m "fix: close jdbc ddl metadata baseline"
```

---

### Task 2: Minimal ALTER TABLE ADD COLUMN

**Files:**
- Modify: `server/innodb/engine/executor.go`
- Test: `server/innodb/engine/executor_ddl_test.go`
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/DDLOperationsTest.java`

**Interfaces:**
- Consumes: `.frm` JSON shape written by `createTableStructureFile`.
- Produces: `func (e *XMySQLExecutor) executeAlterTableStatement(ctx *ExecutionContext, currentDB string, stmt *sqlparser.DDL)`
- Produces: `ALTER TABLE t ADD COLUMN c TYPE [DEFAULT value]` updates `.frm` and keeps DML schema resolution working.

- [ ] **Step 1: Add failing test for ALTER ADD COLUMN**

Add to `server/innodb/engine/executor_ddl_test.go`:

```go
func TestAlterTableAddColumnUpdatesFrm(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "app")
	require.NoError(t, os.MkdirAll(dbPath, 0755))

	executor := NewXMySQLExecutor(&conf.Cfg{InnodbDataDir: tmp})
	createSQL := "create table users (id int primary key)"
	createStmt, err := sqlparser.Parse(createSQL)
	require.NoError(t, err)
	require.NoError(t, executor.createTableImpl("app", "users", createStmt.(*sqlparser.DDL)))

	alterStmt, err := sqlparser.Parse("alter table users add column name varchar(100)")
	require.NoError(t, err)

	results := make(chan *Result, 1)
	executor.executeDDL(alterStmt.(*sqlparser.DDL), nil, "app", results)
	got := <-results
	require.NoError(t, got.Err)

	raw, err := os.ReadFile(filepath.Join(dbPath, "users.frm"))
	require.NoError(t, err)
	require.Contains(t, string(raw), `"name": "name"`)
	require.Contains(t, string(raw), `"type": "varchar"`)
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run TestAlterTableAddColumnUpdatesFrm -count=1
```

Expected before fix: FAIL with `unsupported DDL action: alter`.

- [ ] **Step 3: Add `alter` dispatch**

In `executeDDL`, add:

```go
case "alter":
	logger.Debugf("ALTER TABLE使用数据库: %s", currentDB)
	e.executeAlterTableStatement(ctx, currentDB, stmt)
```

- [ ] **Step 4: Implement ADD COLUMN only**

Add `executeAlterTableStatement` in `server/innodb/engine/executor.go` near the DDL methods:

```go
func (e *XMySQLExecutor) executeAlterTableStatement(ctx *ExecutionContext, currentDB string, stmt *sqlparser.DDL) {
	tableName := stmt.Table.Name.String()
	databaseName := stmt.Table.Qualifier.String()
	if databaseName == "" {
		databaseName = currentDB
	}
	if databaseName == "" || tableName == "" {
		ctx.Results <- &Result{Err: fmt.Errorf("ALTER TABLE requires database and table"), ResultType: common.RESULT_TYPE_DDL}
		return
	}
	if stmt.TableSpec == nil || len(stmt.TableSpec.Columns) == 0 {
		ctx.Results <- &Result{Err: fmt.Errorf("unsupported ALTER TABLE action"), ResultType: common.RESULT_TYPE_DDL}
		return
	}
	if err := e.alterTableAddColumns(databaseName, tableName, stmt.TableSpec.Columns); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("ALTER TABLE failed: %v", err)}
		return
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Table '%s' altered successfully", tableName)}
}
```

Implement helper:

```go
func (e *XMySQLExecutor) alterTableAddColumns(dbName, tableName string, cols []*sqlparser.ColumnDefinition) error {
	frmPath := filepath.Join(e.getDataDir(), dbName, tableName+".frm")
	raw, err := os.ReadFile(frmPath)
	if err != nil {
		return fmt.Errorf("read table metadata failed: %v", err)
	}
	var tableInfo map[string]interface{}
	if err := json.Unmarshal(raw, &tableInfo); err != nil {
		return fmt.Errorf("parse table metadata failed: %v", err)
	}
	existing, _ := tableInfo["columns"].([]interface{})
	seen := map[string]struct{}{}
	for _, col := range existing {
		if m, ok := col.(map[string]interface{}); ok {
			if name, _ := m["name"].(string); name != "" {
				seen[strings.ToLower(name)] = struct{}{}
			}
		}
	}
	for _, col := range cols {
		name := col.Name.String()
		if _, ok := seen[strings.ToLower(name)]; ok {
			return fmt.Errorf("duplicate column '%s'", name)
		}
		existing = append(existing, map[string]interface{}{
			"name":     name,
			"type":     col.Type.Type,
			"length":   col.Type.Length,
			"scale":    col.Type.Scale,
			"unsigned": col.Type.Unsigned,
			"nullable": !col.Type.NotNull,
			"default":  sqlparser.String(col.Type.Default),
		})
	}
	tableInfo["columns"] = existing
	out, err := json.MarshalIndent(tableInfo, "", "  ")
	if err != nil {
		return fmt.Errorf("serialize table metadata failed: %v", err)
	}
	return os.WriteFile(frmPath, out, 0644)
}
```

- [ ] **Step 5: Run Go tests**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/gofmt -w server/innodb/engine/executor.go server/innodb/engine/executor_ddl_test.go
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestAlterTableAddColumnUpdatesFrm|TestDropDatabaseIfExistsMissingIsOk|TestShowDatabasesLikeReturnsNavigableResult' -count=1
```

Expected: PASS.

- [ ] **Step 6: Run JDBC DDL suite**

Run with local server:

```bash
cd /Users/zhukovasky/GolandProjects/xmysql-server/jdbc_client
mvn test -Dtest=DDLOperationsTest
```

Expected after Task 1 and Task 2: `DDLOperationsTest` passes or any remaining failures are unrelated to `SHOW DATABASES LIKE`, `DROP DATABASE IF EXISTS`, metadata table lookup, or `ALTER TABLE ADD COLUMN`.

- [ ] **Step 7: Commit**

```bash
git add server/innodb/engine/executor.go server/innodb/engine/executor_ddl_test.go
git commit -m "feat: support minimal alter table add column"
```

---

### Task 3: TRUNCATE Metadata Mapping Closure

**Files:**
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/innodb/manager/table_storage_mapping.go`
- Test: `server/innodb/engine/executor_ddl_test.go`
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/PreparedStatementTest.java`

**Interfaces:**
- Consumes: `TableStorageManager.GetTableStorageInfo(schema, table)`
- Produces: `TRUNCATE TABLE` keeps `.frm`, `.ibd`, table-storage mapping, and DML metadata lookup consistent.

- [ ] **Step 1: Add failing Go test for CREATE INSERT TRUNCATE INSERT**

Add to `server/innodb/engine/executor_ddl_test.go`:

```go
func TestTruncateKeepsTableMetadataResolvable(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)

	mustExecSQL(t, executor, "create database app")
	mustExecSQL(t, executor, "use app")
	mustExecSQL(t, executor, "create table users (id int primary key auto_increment, username varchar(50) not null)")
	mustExecSQL(t, executor, "insert into users (username) values ('before')")
	mustExecSQL(t, executor, "truncate table users")
	mustExecSQL(t, executor, "insert into users (username) values ('after')")

	rows := mustQuerySQL(t, executor, "select username from users")
	require.Equal(t, [][]interface{}{{"after"}}, rows)
}
```

If the helper functions do not exist, add local test helpers in the same file:

```go
func mustExecSQL(t *testing.T, executor *XMySQLExecutor, sql string) {
	t.Helper()
	results := make(chan *Result, 1)
	executor.Execute(context.Background(), sql, "app", results, nil)
	got := <-results
	require.NoError(t, got.Err)
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run TestTruncateKeepsTableMetadataResolvable -count=1
```

Expected before fix: FAIL with metadata lookup failure or empty table-storage mapping.

- [ ] **Step 3: Fix TRUNCATE mapping order**

In `truncateTableImpl`, keep the table name registered throughout the operation. Replace unregister/register with atomic remap semantics:

```go
info := &manager.TableStorageInfo{
	SchemaName:    databaseName,
	TableName:     tableName,
	SpaceID:       handle.SpaceID,
	RootPageNo:    3,
	IndexPageNo:   3,
	DataSegmentID: handle.DataSegmentID,
	Type:          oldInfo.Type,
}
if err := e.tableStorageManager.ReplaceTableStorage(context.Background(), info); err != nil {
	return fmt.Errorf("replace truncated table storage failed: %v", err)
}
```

If `ReplaceTableStorage` does not exist, add it to `server/innodb/manager/table_storage_mapping.go`:

```go
func (tsm *TableStorageManager) ReplaceTableStorage(ctx context.Context, info *TableStorageInfo) error {
	if info == nil {
		return fmt.Errorf("table storage info cannot be nil")
	}
	key := tableStorageKey(info.SchemaName, info.TableName)
	tsm.mu.Lock()
	defer tsm.mu.Unlock()
	tsm.tables[key] = info
	return tsm.persistTableStorageMapping(ctx)
}
```

Use the real field and mutex names from `table_storage_mapping.go`; do not invent parallel maps.

- [ ] **Step 4: Keep `.frm` schema and new `.ibd` in sync**

In `truncateTableImpl`, after new tablespace creation, make sure the user-visible `.ibd` path still exists:

```go
ibdPath := filepath.Join(e.getDataDir(), databaseName, tableName+".ibd")
if _, err := os.Stat(ibdPath); os.IsNotExist(err) {
	if err := e.createTableDataFile(filepath.Dir(ibdPath), tableName); err != nil {
		return fmt.Errorf("recreate table data file failed: %v", err)
	}
}
```

Do not rewrite `.frm`.

- [ ] **Step 5: Run Go storage tests**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/gofmt -w server/innodb/engine/executor.go server/innodb/manager/table_storage_mapping.go server/innodb/engine/executor_ddl_test.go
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager -run 'Truncate|TableStorage|StorageIntegratedDML' -count=1
```

Expected: PASS.

- [ ] **Step 6: Run JDBC PreparedStatement suite**

Run with local server:

```bash
cd /Users/zhukovasky/GolandProjects/xmysql-server/jdbc_client
mvn test -Dtest=PreparedStatementTest
```

Expected after this task: failures should no longer include `get table metadata failed: table 'test_prepared_stmt.users' not found`. Remaining failures, if any, should be generated keys, transaction-in-PreparedStatement, IN, LIKE, or type handling.

- [ ] **Step 7: Commit**

```bash
git add server/innodb/engine/executor.go server/innodb/manager/table_storage_mapping.go server/innodb/engine/executor_ddl_test.go
git commit -m "fix: keep truncate table metadata resolvable"
```

---

### Task 4: JDBC Transaction Command Baseline

**Files:**
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/innodb/engine/enginx.go`
- Modify: `server/dispatcher/enhanced_message_handler.go`
- Test: `server/innodb/engine/executor_transaction_statement_test.go`
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/TransactionTest.java`

**Interfaces:**
- Produces: `BEGIN`, `START TRANSACTION`, `COMMIT`, `ROLLBACK` return OK packets through JDBC.
- Produces: Session-scoped transaction state is compatible with `Connection.setAutoCommit(false)`.
- Produces: `SAVEPOINT name`, `ROLLBACK TO SAVEPOINT name`, `RELEASE SAVEPOINT name` parse and return OK for current JDBC tests.

- [ ] **Step 1: Add failing Go tests for transaction statements**

Create `server/innodb/engine/executor_transaction_statement_test.go`:

```go
func TestTransactionStatementsReturnOK(t *testing.T) {
	executor := NewXMySQLExecutor(&conf.Cfg{InnodbDataDir: t.TempDir()})
	for _, query := range []string{
		"begin",
		"start transaction",
		"commit",
		"rollback",
		"savepoint sp1",
		"rollback to savepoint sp1",
		"release savepoint sp1",
	} {
		t.Run(query, func(t *testing.T) {
			results := make(chan *Result, 1)
			executor.Execute(context.Background(), query, "", results, nil)
			got := <-results
			require.NoError(t, got.Err)
			require.Equal(t, common.RESULT_TYPE_QUERY, got.ResultType)
		})
	}
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run TestTransactionStatementsReturnOK -count=1
```

Expected before fix: FAIL on unsupported statement type or parser error for SAVEPOINT.

- [ ] **Step 3: Add text-level routing for transaction commands**

Before parser-dispatched default errors in `executor.go`, normalize raw SQL:

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
	case strings.HasPrefix(lower, "rollback to "):
		return "rollback_to_savepoint", strings.TrimSpace(q[len("rollback to "):]), true
	case strings.HasPrefix(lower, "release savepoint "):
		return "release_savepoint", strings.TrimSpace(q[len("release savepoint "):]), true
	default:
		return "", "", false
	}
}
```

Route these before parse errors become client-visible. If parsing fails with `SAVEPOINT`, use the raw query route.

- [ ] **Step 4: Implement minimal transaction result**

Add:

```go
func (e *XMySQLExecutor) executeTransactionCommand(ctx *ExecutionContext, cmd string, name string, session server.MySQLServerSession) {
	if session != nil {
		switch cmd {
		case "begin":
			session.SetParamByName("in_transaction", true)
			session.SetParamByName("savepoints", []string{})
		case "commit", "rollback":
			session.SetParamByName("in_transaction", false)
			session.SetParamByName("savepoints", []string{})
		case "savepoint":
			raw := session.GetParamByName("savepoints")
			points, _ := raw.([]string)
			session.SetParamByName("savepoints", append(points, name))
		case "rollback_to_savepoint":
			// Baseline: accept command; full undo is covered by later MVCC work.
		case "release_savepoint":
			raw := session.GetParamByName("savepoints")
			points, _ := raw.([]string)
			next := make([]string, 0, len(points))
			for _, p := range points {
				if !strings.EqualFold(p, name) {
					next = append(next, p)
				}
			}
			session.SetParamByName("savepoints", next)
		}
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: strings.ToUpper(cmd)}
}
```

This is the JDBC command baseline. Full undo-log rollback and savepoint rollback are separate storage-engine work and should not be faked as durable isolation.

- [ ] **Step 5: Wire route in engine and dispatcher**

In `server/innodb/engine/enginx.go` and `server/dispatcher/enhanced_message_handler.go`, if raw query matches `normalizedTransactionCommand`, call the executor transaction command and return OK instead of parser error.

- [ ] **Step 6: Run Go tests**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/gofmt -w server/innodb/engine/executor.go server/innodb/engine/enginx.go server/dispatcher/enhanced_message_handler.go server/innodb/engine/executor_transaction_statement_test.go
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/dispatcher -run 'TransactionStatements|Transaction|Savepoint' -count=1
```

Expected: PASS.

- [ ] **Step 7: Run JDBC transaction suite**

Run with local server:

```bash
cd /Users/zhukovasky/GolandProjects/xmysql-server/jdbc_client
mvn test -Dtest=TransactionTest
```

Expected after baseline: command-level errors disappear. If assertions fail because rollback does not undo writes, record that as `P0-TXN-UNDO` and implement with undo log/MVCC in a follow-up task before declaring full transaction semantics complete.

- [ ] **Step 8: Commit**

```bash
git add server/innodb/engine/executor.go server/innodb/engine/enginx.go server/dispatcher/enhanced_message_handler.go server/innodb/engine/executor_transaction_statement_test.go
git commit -m "fix: accept jdbc transaction commands"
```

---

### Task 5: Core Constraint Semantics

**Files:**
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Modify: `server/innodb/engine/storage_integrated_index_helper.go`
- Test: `server/innodb/engine/storage_integrated_constraints_test.go`
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/IndexAndConstraintTest.java`

**Interfaces:**
- Produces: NOT NULL violations fail before row write.
- Produces: Composite primary key uniqueness is checked using all key columns, not only the first column.
- Produces: Ordinary secondary index metadata survives CREATE TABLE and can be read through metadata paths.
- Explicitly defers: foreign key, cascade update/delete, CHECK, FULLTEXT.

- [ ] **Step 1: Add failing Go tests for NOT NULL and composite primary key**

Create `server/innodb/engine/storage_integrated_constraints_test.go`:

```go
func TestInsertRejectsNullForNotNullColumn(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "create database app")
	mustExecSQL(t, executor, "use app")
	mustExecSQL(t, executor, "create table users (id int primary key, username varchar(50) not null)")

	err := execSQLExpectError(t, executor, "insert into users (id, username) values (1, null)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "not null")
}

func TestCompositePrimaryKeyUsesAllColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "create database app")
	mustExecSQL(t, executor, "use app")
	mustExecSQL(t, executor, "create table memberships (user_id int, group_id int, primary key (user_id, group_id))")
	mustExecSQL(t, executor, "insert into memberships (user_id, group_id) values (1, 10)")
	mustExecSQL(t, executor, "insert into memberships (user_id, group_id) values (1, 20)")

	err := execSQLExpectError(t, executor, "insert into memberships (user_id, group_id) values (1, 10)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "duplicate")
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestInsertRejectsNullForNotNullColumn|TestCompositePrimaryKeyUsesAllColumns' -count=1
```

Expected before fix: NOT NULL or composite primary key behavior fails.

- [ ] **Step 3: Enforce NOT NULL before row write**

In `storage_integrated_dml_executor.go`, before encoding/writing a row, load table columns from `.frm` metadata and reject missing/null values for columns where `nullable == false` and no default is available:

```go
if !column.Nullable && isSQLNull(rowValues[column.Name]) && !column.HasDefault {
	return nil, fmt.Errorf("Column '%s' cannot be null", column.Name)
}
```

Use the repository's existing metadata structures. Do not add a parallel schema parser if `resolveDmlSchema` or `.frm` loading already exists.

- [ ] **Step 4: Build composite key bytes from all primary-key columns**

In `storage_integrated_index_helper.go`, replace any assumption that primary key is the first column with:

```go
func buildCompositeKey(row map[string]interface{}, columns []string) ([]byte, error) {
	parts := make([][]byte, 0, len(columns))
	for _, col := range columns {
		val, ok := row[col]
		if !ok {
			return nil, fmt.Errorf("missing primary key column '%s'", col)
		}
		parts = append(parts, []byte(fmt.Sprintf("%v", val)))
	}
	return bytes.Join(parts, []byte{0x00}), nil
}
```

Use existing typed encoders if available; the delimiter-based version is acceptable only as a first pass if tests include values that cannot collide.

- [ ] **Step 5: Explicitly reject unsupported advanced constraints**

For `FOREIGN KEY`, `CHECK`, and `FULLTEXT`, do not silently create partial metadata that later breaks DML. Either:

```go
return fmt.Errorf("unsupported constraint: foreign key")
```

or persist enough metadata to make later DML succeed. For this task, prefer explicit unsupported errors unless the suite requires acceptance.

- [ ] **Step 6: Run Go tests**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/gofmt -w server/innodb/engine/storage_integrated_dml_executor.go server/innodb/engine/storage_integrated_index_helper.go server/innodb/engine/storage_integrated_constraints_test.go
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager -run 'Constraint|Composite|SecondaryIndex|StorageIntegratedDML' -count=1
```

Expected: PASS.

- [ ] **Step 7: Run JDBC constraint subset**

Run with local server:

```bash
cd /Users/zhukovasky/GolandProjects/xmysql-server/jdbc_client
mvn test -Dtest=IndexAndConstraintTest
```

Expected after this task: NOT NULL, primary key, composite primary key, simple index, and unique index tests pass. Foreign key, CHECK, FULLTEXT, cascade update/delete may still fail and should be documented as P1/PX unless product scope requires them as P0.

- [ ] **Step 8: Commit**

```bash
git add server/innodb/engine/executor.go server/innodb/engine/storage_integrated_dml_executor.go server/innodb/engine/storage_integrated_index_helper.go server/innodb/engine/storage_integrated_constraints_test.go
git commit -m "fix: enforce core jdbc constraints"
```

---

## Final Verification

- [ ] **Step 1: Start server with JDBC config**

```bash
cd /Users/zhukovasky/GolandProjects/xmysql-server
/Users/zhukovasky/sdk/go1.24.3/bin/go run . -configPath=conf/jdbc_local.ini
```

- [ ] **Step 2: Run focused JDBC suites**

```bash
cd /Users/zhukovasky/GolandProjects/xmysql-server/jdbc_client
mvn test -Dtest=DMLOperationsTest,DDLOperationsTest,PreparedStatementTest,TransactionTest,IndexAndConstraintTest
```

Expected:

- `DMLOperationsTest`: PASS.
- `DDLOperationsTest`: PASS or only documented non-P0 ALTER variants fail.
- `PreparedStatementTest`: PASS except transaction-generated-key details if explicitly deferred.
- `TransactionTest`: No command-level `unsupported statement type` or SAVEPOINT parse failures. Full rollback correctness must pass before claiming full transaction semantics.
- `IndexAndConstraintTest`: Core constraints pass; advanced constraints are explicitly failed or deferred.

- [ ] **Step 3: Run Go package tests**

```bash
cd /Users/zhukovasky/GolandProjects/xmysql-server
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager ./server/dispatcher ./server/net ./server/auth -count=1
```

Expected: PASS.

- [ ] **Step 4: Clean generated runtime outputs**

```bash
cd /Users/zhukovasky/GolandProjects/xmysql-server
git restore jdbc_client/target/surefire-reports tmp/jdbc_logs server/net/data 2>/dev/null || true
rm -f server/net/data/_xmysql_auto_increment.json
git status --short
```

Expected: only intended source/doc files are modified.

- [ ] **Step 5: Update capability docs**

Update:

- `docs/planning/P0_CAPABILITY_BACKLOG_20260715.md`
- `docs/planning/CAPABILITY_PRIORITY_INDEX_20260715.md`
- `docs/planning/P0_CURRENT_STATUS_SUMMARY.md`

Required wording:

```markdown
JDBC metadata, DDL smoke, prepared-statement CRUD, TRUNCATE metadata remap, and transaction command acceptance are now verified by focused JDBC suites. Full transaction rollback/MVCC correctness and advanced constraints remain tracked separately if not fully implemented.
```

- [ ] **Step 6: Final commit**

```bash
git add docs/planning/P0_CAPABILITY_BACKLOG_20260715.md docs/planning/CAPABILITY_PRIORITY_INDEX_20260715.md docs/planning/P0_CURRENT_STATUS_SUMMARY.md
git commit -m "docs: update jdbc compatibility status"
```

## Deferred Work Register

Move these out of P0 only if the target is "JDBC CRUD usable" rather than "MySQL compatibility full":

- Full undo-log rollback for `ROLLBACK` and `ROLLBACK TO SAVEPOINT`.
- MVCC isolation proof for concurrent JDBC sessions.
- Foreign key enforcement.
- ON UPDATE/ON DELETE CASCADE.
- CHECK constraints.
- FULLTEXT index.
- Broad `ALTER TABLE` beyond ADD COLUMN.
- Complete `information_schema` query compatibility.

## Self-Review

- Spec coverage: The plan covers DDL metadata, SHOW result shape, `DROP DATABASE IF EXISTS`, `ALTER TABLE ADD COLUMN`, transaction commands, SAVEPOINT command parsing, TRUNCATE remap, PreparedStatement fallout, and core constraints.
- Placeholder scan: No `TBD`, no open-ended "add tests" without concrete test examples, and no unspecified acceptance command.
- Type consistency: Produced functions are named consistently across tasks: `executeAlterTableStatement`, `alterTableAddColumns`, `normalizedTransactionCommand`, `executeTransactionCommand`, and optional `ReplaceTableStorage`.
