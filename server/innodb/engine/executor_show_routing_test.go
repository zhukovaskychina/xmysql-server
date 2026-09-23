package engine

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

type testMySQLSession struct {
	mu     sync.RWMutex
	params map[string]interface{}
	ctx    *server.SessionContext
}

func newTestMySQLSession() *testMySQLSession {
	return &testMySQLSession{
		params: make(map[string]interface{}),
		ctx:    server.NewSessionContext("test"),
	}
}

func (s *testMySQLSession) GetLastActiveTime() time.Time { return time.Now() }
func (s *testMySQLSession) SendOK()                      {}
func (s *testMySQLSession) SendHandleOk()                {}
func (s *testMySQLSession) SendSelectFields()            {}
func (s *testMySQLSession) SessionContext() *server.SessionContext {
	return s.ctx
}
func (s *testMySQLSession) GetParamByName(name string) interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.params[name]
}
func (s *testMySQLSession) SetParamByName(name string, value interface{}) {
	s.mu.Lock()
	s.params[name] = value
	s.mu.Unlock()
	if name == "in_transaction" {
		if b, ok := value.(bool); ok {
			s.ctx.SetInTransaction(b)
		}
	}
}

func TestXMySQLExecutor_ExecuteQuery_ShowRoutesToExecuteShowStatement(t *testing.T) {
	executor := &XMySQLExecutor{}
	results := make(chan *Result, 4)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}

	executor.executeQuery(ctx, nil, "show variables", "testdb", results)

	result, ok := <-results
	assert.True(t, ok)
	assert.NotNil(t, result)
	assert.Equal(t, "QUERY", result.ResultType)
	assert.NotNil(t, result.Data)

	data, ok := result.Data.(map[string]interface{})
	assert.True(t, ok)
	assert.Contains(t, data, "columns")
	assert.Contains(t, data, "rows")
}

func TestXMySQLExecutor_ShowPluginsReturnsBuiltinsAndSupportsLike(t *testing.T) {
	executor := &XMySQLExecutor{}
	engine := &XMySQLEngine{QueryExecutor: executor}
	session := newTestMySQLSession()

	all := mustSelectResultSQL(t, engine, "", "show plugins")
	require.Equal(t, []string{"Name", "Status", "Type", "Library", "License", "Load_option"}, all.Columns)
	require.NotEmpty(t, all.Records)

	filtered := mustSelectResultSQL(t, engine, "", "show plugins like 'InnoDB'")
	require.Equal(t, 1, filtered.RowCount)
	require.Equal(t, "InnoDB", filtered.Records[0].GetValues()[0].String())
	_ = session
}

func TestXMySQLExecutor_ShowOpenTablesReturnsVisibleTablesAndSupportsLike(t *testing.T) {
	tempDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "testdb"), 0o755))
	frmContent := `{
  "table_name": "users",
  "columns": [
    {"name": "id", "type": "INT", "length": 11, "nullable": false}
  ]
}`
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "testdb", "users.frm"), []byte(frmContent), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "testdb", "audit.frm"), []byte(frmContent), 0644))

	executor := &XMySQLExecutor{conf: &conf.Cfg{DataDir: tempDir}}
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results, DatabaseName: "testdb", RawQuery: "show open tables from testdb like 'users'"}
	executor.executeQuery(ctx, nil, ctx.RawQuery, "testdb", results)

	result := <-results
	require.NoError(t, result.Err)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok, "expected SHOW OPEN TABLES result map, got %T", result.Data)
	assert.Equal(t, []string{"Database", "Table", "In_use", "Name_locked"}, data["columns"])
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok, "expected rows, got %T", data["rows"])
	require.Len(t, rows, 1)
	assert.Equal(t, []interface{}{"testdb", "users", int64(0), int64(0)}, rows[0])
}

func TestXMySQLExecutor_ShowOpenTablesProjectsLiveLockUsage(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	observer := newTestMySQLSession()
	owner.SessionContext().SetConnectionID(1001)
	observer.SessionContext().SetConnectionID(1002)
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables users write")

	result := <-executor.ExecuteQuery(observer, "show open tables from app like 'users'", "app")
	require.NoError(t, result.Err)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"app", "users", int64(1), int64(1)}}, rows)

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	result = <-executor.ExecuteQuery(observer, "show open tables from app like 'users'", "app")
	require.NoError(t, result.Err)
	data, ok = result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok = data["rows"].([][]interface{})
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"app", "users", int64(0), int64(0)}}, rows)
}

func TestXMySQLExecutor_ShowStorageEnginesUsesShowEnginesShape(t *testing.T) {
	executor := &XMySQLExecutor{}
	engine := &XMySQLEngine{QueryExecutor: executor}
	result := <-engine.ExecuteQuery(nil, "show storage engines", "")
	require.NoError(t, result.Err)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok, "expected SHOW ENGINES result map, got %T", result.Data)
	require.Equal(t, []string{"Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"}, data["columns"])
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok, "expected rows, got %T", data["rows"])
	require.NotEmpty(t, rows)
	require.Equal(t, "InnoDB", rows[0][0])
}

func TestXMySQLExecutor_InformationSchemaPluginsSupportsProjectionAndFilter(t *testing.T) {
	executor := &XMySQLEngine{QueryExecutor: &XMySQLExecutor{}}
	rows := mustQuerySQL(t, executor, "", "select plugin_name, plugin_status, plugin_type from information_schema.plugins where plugin_name = 'InnoDB'")
	require.Equal(t, [][]interface{}{{"InnoDB", "ACTIVE", "STORAGE ENGINE"}}, rows)
	result := mustSelectResultSQL(t, executor, "", "select * from information_schema.plugins where plugin_name = 'InnoDB'")
	require.Equal(t, []string{"PLUGIN_NAME", "PLUGIN_VERSION", "PLUGIN_STATUS", "PLUGIN_TYPE", "PLUGIN_TYPE_VERSION", "PLUGIN_LIBRARY", "PLUGIN_LIBRARY_VERSION", "PLUGIN_AUTHOR", "PLUGIN_DESCRIPTION", "PLUGIN_LICENSE", "LOAD_OPTION"}, result.Columns)
	require.Len(t, result.Records, 1)
}

func TestXMySQLExecutor_InformationSchemaPluginsFiltersAuthorAndLicense(t *testing.T) {
	executor := &XMySQLEngine{QueryExecutor: &XMySQLExecutor{}}

	wrongAuthor := mustQuerySQL(t, executor, "", "select plugin_name from information_schema.plugins where plugin_author = 'not-an-author'")
	require.Empty(t, wrongAuthor)

	licensed := mustQuerySQL(t, executor, "", "select plugin_name from information_schema.plugins where plugin_license = 'GPL'")
	require.Len(t, licensed, 4)
}

func TestXMySQLExecutor_InformationSchemaCollationsSelectStarUsesNativeShape(t *testing.T) {
	executor := &XMySQLEngine{QueryExecutor: &XMySQLExecutor{}}
	result := mustSelectResultSQL(t, executor, "", "select * from information_schema.collations where collation_name = 'utf8mb4_0900_ai_ci'")
	require.Equal(t, []string{
		"COLLATION_NAME", "CHARACTER_SET_NAME", "ID", "IS_DEFAULT", "IS_COMPILED", "SORTLEN", "PAD_ATTRIBUTE",
	}, result.Columns)
	require.Len(t, result.Records, 1)
}

func TestXMySQLExecutor_InformationSchemaFilesSelectStarUsesNativeShape(t *testing.T) {
	executor := &XMySQLEngine{QueryExecutor: &XMySQLExecutor{}}
	result := mustSelectResultSQL(t, executor, "", "select * from information_schema.files")
	require.Equal(t, []string{
		"FILE_ID", "FILE_NAME", "FILE_TYPE", "TABLESPACE_NAME", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME",
		"LOGFILE_GROUP_NAME", "LOGFILE_GROUP_NUMBER", "ENGINE", "FULLTEXT_KEYS", "DELETED_ROWS", "UPDATE_COUNT",
		"FREE_EXTENTS", "TOTAL_EXTENTS", "EXTENT_SIZE", "INITIAL_SIZE", "MAXIMUM_SIZE", "AUTOEXTEND_SIZE",
		"CREATION_TIME", "LAST_UPDATE_TIME", "LAST_ACCESS_TIME", "RECOVER_TIME", "TRANSACTION_COUNTER", "VERSION",
		"ROW_FORMAT", "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE",
		"CREATE_TIME", "UPDATE_TIME", "CHECK_TIME", "CHECKSUM", "STATUS", "EXTRA", "NODEGROUP_ID", "TABLESPACE_TYPE",
	}, result.Columns)
	metadata := mustSelectResultSQL(t, executor, "", "select column_name from information_schema.columns where table_schema = 'information_schema' and table_name = 'files'")
	require.Len(t, metadata.Records, 40)
}

func TestXMySQLExecutor_InformationSchemaRegisteredShapesMatchColumnsCatalog(t *testing.T) {
	executor := &XMySQLEngine{QueryExecutor: &XMySQLExecutor{}}
	for _, table := range informationSchemaMetadataTableNames() {
		result := mustSelectResultSQL(t, executor, "", "select * from information_schema."+table)
		metadata := mustSelectResultSQL(t, executor, "", "select column_name from information_schema.columns where table_schema = 'information_schema' and table_name = '"+table+"'")
		require.Equal(t, len(metadata.Records), len(result.Columns), "shape mismatch for information_schema.%s", table)
	}
}

func TestXMySQLExecutor_PerformanceSchemaRegisteredShapesMatchColumnsCatalog(t *testing.T) {
	executor := &XMySQLEngine{QueryExecutor: &XMySQLExecutor{}}
	for _, table := range performanceSchemaTableNames() {
		result := mustSelectResultSQL(t, executor, "", "select * from performance_schema."+table)
		metadata := mustSelectResultSQL(t, executor, "", "select column_name from information_schema.columns where table_schema = 'performance_schema' and table_name = '"+table+"'")
		require.Equal(t, len(metadata.Records), len(result.Columns), "shape mismatch for performance_schema.%s", table)
	}
}

func TestXMySQLExecutor_EnabledRolesReflectsActiveConnectionRoles(t *testing.T) {
	executor := &XMySQLExecutor{}
	session := newTestMySQLSession()
	session.SetParamByName("active_roles", []string{"report_reader@localhost", "audit_reader@localhost"})

	rows := mustQuerySessionSQL(t, &XMySQLEngine{QueryExecutor: executor}, session, "", "SELECT ROLE_NAME, ROLE_HOST FROM information_schema.enabled_roles")

	assert.Equal(t, [][]interface{}{
		{"report_reader", "localhost"},
		{"audit_reader", "localhost"},
	}, rows)
}

func TestXMySQLExecutor_ExecuteShowCreateTable_ExtractsNameFromType(t *testing.T) {
	executor := &XMySQLExecutor{}
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}

	executor.executeShowCreateTable(ctx, &sqlparser.Show{Type: "create table users"}, "show create table users")

	result := <-results
	assert.NoError(t, result.Err)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.NotEmpty(t, rows)
	assert.Equal(t, "users", rows[0][0])
}

func TestXMySQLExecutor_ExecuteQuery_ShowTablesWithoutSessionReturnsError(t *testing.T) {
	executor := &XMySQLExecutor{}
	results := make(chan *Result, 4)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}

	executor.executeQuery(ctx, nil, "show tables", "testdb", results)

	result, ok := <-results
	assert.True(t, ok)
	assert.NotNil(t, result)
	assert.Error(t, result.Err)
	assert.Contains(t, result.Err.Error(), "session is required for SHOW TABLES")
}

func TestXMySQLExecutor_ExecuteQuery_ShowTablesWithoutDatabaseReturnsError(t *testing.T) {
	executor := &XMySQLExecutor{}
	results := make(chan *Result, 4)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}
	session := newTestMySQLSession()

	executor.executeQuery(ctx, session, "show tables", "testdb", results)

	result, ok := <-results
	assert.True(t, ok)
	assert.NotNil(t, result)
	require.Error(t, result.Err)
	assert.Contains(t, result.Err.Error(), "no database selected")
}

func TestXMySQLExecutor_ExecuteQuery_ShowCreateTableReturnsQueryData(t *testing.T) {
	executor := &XMySQLExecutor{}
	results := make(chan *Result, 4)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}

	executor.executeQuery(ctx, nil, "show create table users", "testdb", results)

	result, ok := <-results
	assert.True(t, ok)
	assert.NotNil(t, result)
	assert.NoError(t, result.Err)
	assert.Equal(t, "QUERY", result.ResultType)

	data, ok := result.Data.(map[string]interface{})
	assert.True(t, ok)
	assert.Contains(t, data, "columns")
	assert.Contains(t, data, "rows")

	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.NotEmpty(t, rows)
	require.Len(t, rows[0], 2)
	assert.Equal(t, "users", rows[0][0])
	assert.Contains(t, rows[0][1], "CREATE TABLE `users`")
}

func TestXMySQLExecutor_ExecuteQuery_ShowCreateTableWithSchemaNameParsesTable(t *testing.T) {
	executor := &XMySQLExecutor{}
	results := make(chan *Result, 4)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}

	executor.executeQuery(ctx, nil, "show create table `testdb`.`users`", "testdb", results)

	result, ok := <-results
	assert.True(t, ok)
	assert.NotNil(t, result)
	assert.NoError(t, result.Err)
	assert.Equal(t, "QUERY", result.ResultType)

	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.NotEmpty(t, rows)
	require.Len(t, rows[0], 2)
	assert.Equal(t, "users", rows[0][0])
	assert.Contains(t, rows[0][1], "CREATE TABLE `users`")
}

func TestShowCreateTableWaitsForExplicitTableMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	reader := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table show_table_lock (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables show_table_lock write")

	showDone := make(chan *Result, 1)
	go func() {
		showDone <- <-executor.ExecuteQuery(reader, "show create table show_table_lock", "app")
	}()

	select {
	case result := <-showDone:
		t.Fatalf("SHOW CREATE TABLE completed while the table metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-showDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("SHOW CREATE TABLE did not complete after UNLOCK TABLES released the metadata lock")
	}
}

func TestShowIndexWaitsForExplicitTableMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	reader := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table show_index_lock (id int primary key, value int, index idx_value (value))")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables show_index_lock write")

	showDone := make(chan *Result, 1)
	go func() {
		showDone <- <-executor.ExecuteQuery(reader, "show index from show_index_lock", "app")
	}()

	select {
	case result := <-showDone:
		t.Fatalf("SHOW INDEX completed while the table metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-showDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("SHOW INDEX did not complete after UNLOCK TABLES released the metadata lock")
	}
}

func TestDescribeWaitsForExplicitTableMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	reader := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table describe_lock (id int primary key, value int)")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables describe_lock write")

	describeDone := make(chan *Result, 1)
	go func() {
		describeDone <- <-executor.ExecuteQuery(reader, "describe describe_lock", "app")
	}()

	select {
	case result := <-describeDone:
		t.Fatalf("DESCRIBE completed while the table metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-describeDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("DESCRIBE did not complete after UNLOCK TABLES released the metadata lock")
	}
}

func TestShowTableStatusWaitsForExplicitTableMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	reader := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table table_status_lock (id int primary key, value int)")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables table_status_lock write")

	statusDone := make(chan *Result, 1)
	go func() {
		statusDone <- <-executor.ExecuteQuery(reader, "show table status from app", "app")
	}()

	select {
	case result := <-statusDone:
		t.Fatalf("SHOW TABLE STATUS completed while the table metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-statusDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("SHOW TABLE STATUS did not complete after UNLOCK TABLES released the metadata lock")
	}
}

func TestShowColumnsWaitsForExplicitTableMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	reader := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table show_columns_lock (id int primary key, value int)")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables show_columns_lock write")

	columnsDone := make(chan *Result, 1)
	go func() {
		columnsDone <- <-executor.ExecuteQuery(reader, "show columns from show_columns_lock", "app")
	}()

	select {
	case result := <-columnsDone:
		t.Fatalf("SHOW COLUMNS completed while the table metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-columnsDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("SHOW COLUMNS did not complete after UNLOCK TABLES released the metadata lock")
	}
}

func TestShowFullColumnsWaitsForExplicitTableMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	reader := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table show_full_columns_lock (id int primary key, value int)")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables show_full_columns_lock write")

	columnsDone := make(chan *Result, 1)
	go func() {
		columnsDone <- <-executor.ExecuteQuery(reader, "show full columns from show_full_columns_lock", "app")
	}()

	select {
	case result := <-columnsDone:
		t.Fatalf("SHOW FULL COLUMNS completed while the table metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-columnsDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("SHOW FULL COLUMNS did not complete after UNLOCK TABLES released the metadata lock")
	}
}

func TestXMySQLExecutor_ExecuteQuery_ShowColumnsReturnsStubColumns(t *testing.T) {
	tempDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "testdb"), 0o755))
	frmContent := `{
  "table_name": "users",
  "columns": [
    {"name": "id", "type": "INT", "length": 11, "nullable": false}
  ]
}`
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "testdb", "users.frm"), []byte(frmContent), 0644))

	executor := &XMySQLExecutor{
		conf: &conf.Cfg{DataDir: tempDir},
	}
	results := make(chan *Result, 4)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}

	executor.executeQuery(ctx, nil, "show columns from users", "testdb", results)

	result, ok := <-results
	assert.True(t, ok)
	assert.NotNil(t, result)
	assert.NoError(t, result.Err)
	assert.Equal(t, "QUERY", result.ResultType)

	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.NotEmpty(t, rows)
	assert.Equal(t, "id", rows[0][0])
}

func TestXMySQLExecutor_ExecuteQuery_ShowFieldsReturnsStubColumns(t *testing.T) {
	tempDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "testdb"), 0o755))
	frmContent := `{
  "table_name": "users",
  "columns": [
    {"name": "id", "type": "INT", "length": 11, "nullable": false}
  ]
}`
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "testdb", "users.frm"), []byte(frmContent), 0644))

	executor := &XMySQLExecutor{
		conf: &conf.Cfg{DataDir: tempDir},
	}
	results := make(chan *Result, 4)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}

	executor.executeQuery(ctx, nil, "show fields from `testdb`.`users`;", "testdb", results)

	result, ok := <-results
	assert.True(t, ok)
	assert.NotNil(t, result)
	assert.NoError(t, result.Err)
	assert.Equal(t, "QUERY", result.ResultType)

	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.NotEmpty(t, rows)
	assert.Equal(t, "id", rows[0][0])
}

func TestXMySQLExecutor_ExecuteQuery_ShowTablesReturnsRows(t *testing.T) {
	tempDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "testdb"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "testdb", "users.frm"), []byte("{}"), 0644))

	executor := &XMySQLExecutor{
		conf: &conf.Cfg{DataDir: tempDir},
	}
	results := make(chan *Result, 4)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}
	session := newTestMySQLSession()
	session.SetParamByName("database", "testdb")

	executor.executeQuery(ctx, session, "show tables", "testdb", results)

	result, ok := <-results
	assert.True(t, ok)
	assert.NotNil(t, result)
	assert.NoError(t, result.Err)
	assert.Equal(t, "QUERY", result.ResultType)

	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.NotEmpty(t, rows)
	assert.Equal(t, "Found "+strconv.Itoa(len(rows))+" tables", result.Message)
	assert.Equal(t, "users", rows[0][0])
}

func TestXMySQLExecutor_ExecuteQuery_ShowTablesFallsBackToFilesWhenInfoSchemaEmpty(t *testing.T) {
	tempDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "testdb"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "testdb", "users.frm"), []byte("{}"), 0o644))

	executor := &XMySQLExecutor{
		conf:               &conf.Cfg{DataDir: tempDir},
		infosSchemaManager: &fakeShowInfoSchema{schemas: []string{"testdb"}},
	}
	results := make(chan *Result, 4)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}
	session := newTestMySQLSession()
	session.SetParamByName("database", "testdb")

	executor.executeQuery(ctx, session, "show tables", "testdb", results)

	result := <-results
	require.NoError(t, result.Err)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"users"}}, rows)
}

func TestXMySQLExecutor_ExecuteQuery_ShowTablesLikeFiltersRows(t *testing.T) {
	tempDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "testdb"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "testdb", "users.frm"), []byte("{}"), 0644))

	executor := &XMySQLExecutor{
		conf: &conf.Cfg{DataDir: tempDir},
	}
	results := make(chan *Result, 4)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}
	session := newTestMySQLSession()
	session.SetParamByName("database", "testdb")

	executor.executeQuery(ctx, session, "show tables like 'user%'", "testdb", results)

	result, ok := <-results
	assert.True(t, ok)
	assert.NotNil(t, result)
	assert.NoError(t, result.Err)
	assert.Equal(t, "QUERY", result.ResultType)
	assert.Equal(t, "Found 1 tables", result.Message)

	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "users", rows[0][0])
}

func TestXMySQLExecutor_ExecuteQuery_ShowDatabasesReturnsQueryData(t *testing.T) {
	executor := &XMySQLExecutor{}
	results := make(chan *Result, 4)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}

	executor.executeQuery(ctx, nil, "show databases", "testdb", results)

	result, ok := <-results
	assert.True(t, ok)
	assert.NotNil(t, result)
	assert.NoError(t, result.Err)
	assert.Equal(t, common.RESULT_TYPE_QUERY, result.ResultType)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, []string{"Database"}, data["columns"])
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	assert.Equal(t, "Found "+strconv.Itoa(len(rows))+" databases", result.Message)
}

func TestShowDatabasesLikeReturnsNavigableResult(t *testing.T) {
	tmp := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tmp, "app_db"), 0o755))

	executor := &XMySQLExecutor{conf: &conf.Cfg{InnodbDataDir: tmp}}
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results}

	executor.executeShowDatabasesWithQuery(ctx, &sqlparser.Show{Type: "databases"}, "show databases like 'app_%'")

	got := <-results
	require.NoError(t, got.Err)
	require.Equal(t, common.RESULT_TYPE_QUERY, got.ResultType)
	data, ok := got.Data.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, []string{"Database"}, data["columns"])
	require.Equal(t, [][]interface{}{{"app_db"}}, data["rows"])
}

func TestXMySQLExecutor_ExecuteShowStatementWithQuery_ShowDatabasesLikeFiltersRows(t *testing.T) {
	tempDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "app_main"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "archive"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, ".hidden"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "_internal"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "mysql"), 0o755))

	executor := &XMySQLExecutor{
		conf: &conf.Cfg{DataDir: tempDir},
	}
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}

	executor.executeShowStatementWithQuery(ctx, &sqlparser.Show{Type: "databases"}, nil, "show databases like 'app%'")

	result := <-results
	assert.NoError(t, result.Err)
	assert.Equal(t, common.RESULT_TYPE_QUERY, result.ResultType)
	assert.Equal(t, "Found 1 databases", result.Message)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "app_main", rows[0][0])
}

func TestXMySQLExecutor_ExecuteShowStatementWithQuery_ShowDatabasesUsesStmtFilterWithoutRawQuery(t *testing.T) {
	tempDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "app_main"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "archive"), 0o755))

	executor := &XMySQLExecutor{
		conf: &conf.Cfg{DataDir: tempDir},
	}
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}
	stmt := &sqlparser.Show{
		Type:   "databases",
		Filter: &sqlparser.ShowFilter{Like: "app%"},
	}

	executor.executeShowStatementWithQuery(ctx, stmt, nil, "")

	result := <-results
	assert.NoError(t, result.Err)
	assert.Equal(t, common.RESULT_TYPE_QUERY, result.ResultType)
	assert.Equal(t, "Found 1 databases", result.Message)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "app_main", rows[0][0])
}

func TestXMySQLExecutor_ExecuteShowStatementWithQuery_ShowVariablesLikeFiltersRows(t *testing.T) {
	executor := &XMySQLExecutor{}
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}

	executor.executeShowStatementWithQuery(ctx, &sqlparser.Show{Type: "variables"}, nil, "show variables like 'version%'")

	result := <-results
	assert.NoError(t, result.Err)
	assert.Equal(t, "QUERY", result.ResultType)
	assert.Equal(t, "Found 2 variables", result.Message)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, rows, 2)
	assert.Equal(t, "version", rows[0][0])
	assert.Equal(t, "version_comment", rows[1][0])
}

func TestXMySQLExecutor_ExecuteShowStatementWithQuery_ShowGlobalStatusLikeFiltersRows(t *testing.T) {
	executor := &XMySQLExecutor{}
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}

	executor.executeShowStatementWithQuery(ctx, &sqlparser.Show{Type: "status", Scope: "global"}, nil, "show global status like 'Up%'")

	result := <-results
	assert.NoError(t, result.Err)
	assert.Equal(t, "QUERY", result.ResultType)
	assert.Equal(t, "Found 1 status rows", result.Message)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "Uptime", rows[0][0])
}

func TestXMySQLExecutor_ExecuteShowStatementWithQuery_ShowVariablesUsesStmtFilterWithoutRawQuery(t *testing.T) {
	executor := &XMySQLExecutor{}
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}
	stmt := &sqlparser.Show{
		Type:   "variables",
		Filter: &sqlparser.ShowFilter{Like: "version%"},
	}

	executor.executeShowStatementWithQuery(ctx, stmt, nil, "")

	result := <-results
	assert.NoError(t, result.Err)
	assert.Equal(t, "QUERY", result.ResultType)
	assert.Equal(t, "Found 2 variables", result.Message)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, rows, 2)
	assert.Equal(t, "version", rows[0][0])
	assert.Equal(t, "version_comment", rows[1][0])
}

func TestXMySQLExecutor_ExecuteShowStatementWithQuery_ShowStatusUsesStmtFilterWithoutRawQuery(t *testing.T) {
	executor := &XMySQLExecutor{}
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}
	stmt := &sqlparser.Show{
		Type:   "status",
		Scope:  "global",
		Filter: &sqlparser.ShowFilter{Like: "Up%"},
	}

	executor.executeShowStatementWithQuery(ctx, stmt, nil, "")

	result := <-results
	assert.NoError(t, result.Err)
	assert.Equal(t, "QUERY", result.ResultType)
	assert.Equal(t, "Found 1 status rows", result.Message)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "Uptime", rows[0][0])
}

func TestXMySQLExecutor_ExecuteShowStatementWithQuery_ShowVariablesUsesStmtWhereWithoutRawQuery(t *testing.T) {
	executor := &XMySQLExecutor{}
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}
	stmt := &sqlparser.Show{
		Type: "variables",
		Filter: &sqlparser.ShowFilter{
			Filter: &sqlparser.ComparisonExpr{
				Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("Variable_name")},
				Operator: sqlparser.EqualStr,
				Right:    sqlparser.NewStrVal([]byte("version")),
			},
		},
	}

	executor.executeShowStatementWithQuery(ctx, stmt, nil, "")

	result := <-results
	assert.NoError(t, result.Err)
	assert.Equal(t, "QUERY", result.ResultType)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "version", rows[0][0])
}

func TestXMySQLExecutor_ExecuteQuery_ShowDatabasesWhereFiltersRows(t *testing.T) {
	tempDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "app_main"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "archive"), 0o755))

	executor := &XMySQLExecutor{
		conf: &conf.Cfg{DataDir: tempDir},
	}
	results := make(chan *Result, 2)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}

	executor.executeQuery(ctx, nil, "show databases where `Database` = 'app_main'", "testdb", results)

	result, ok := <-results
	assert.True(t, ok)
	assert.NoError(t, result.Err)
	assert.Equal(t, common.RESULT_TYPE_QUERY, result.ResultType)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "app_main", rows[0][0])
}

func TestXMySQLExecutor_ExecuteQuery_ShowTablesWhereFiltersRows(t *testing.T) {
	tempDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "testdb"), 0o755))

	executor := &XMySQLExecutor{
		conf: &conf.Cfg{DataDir: tempDir},
	}
	results := make(chan *Result, 2)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}
	session := newTestMySQLSession()
	session.SetParamByName("database", "testdb")

	executor.executeQuery(ctx, session, "show tables where 1 = 0", "testdb", results)

	result, ok := <-results
	assert.True(t, ok)
	assert.NoError(t, result.Err)
	assert.Equal(t, "QUERY", result.ResultType)
	assert.Equal(t, "Found 0 tables", result.Message)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, rows, 0)
}

func TestXMySQLExecutor_ExecuteQuery_ShowProcesslistUsesSessionState(t *testing.T) {
	executor := &XMySQLExecutor{}
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results, RawQuery: "show processlist", DatabaseName: "app"}
	session := newTestMySQLSession()
	session.SetParamByName("session_id", int64(42))
	session.SetParamByName("user", "alice")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("database", "app")
	session.SetParamByName("in_transaction", true)
	executor.executeShowStatementWithQuery(ctx, &sqlparser.Show{Type: "processlist"}, session, "show processlist")
	result := <-results
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, []string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info", "Rows_sent", "Rows_examined"}, selectResult.Columns)
	require.Len(t, selectResult.Records, 1)
	require.Equal(t, int64(42), selectResult.Records[0].GetValues()[0].Int())
	require.Equal(t, "In transaction", selectResult.Records[0].GetValues()[6].String())
}

func TestXMySQLExecutor_ProcesslistProviderReturnsAllSessions(t *testing.T) {
	executor := &XMySQLExecutor{}
	current := newTestMySQLSession()
	current.ctx.SetConnectionID(42)
	current.SetParamByName("user", "alice")
	current.SetParamByName("host", "localhost")
	current.SetParamByName("global_privileges", []common.PrivilegeType{common.ProcessPriv})
	other := newTestMySQLSession()
	other.ctx.SetConnectionID(43)
	other.SetParamByName("user", "bob")
	other.SetParamByName("host", "10.0.0.2")
	other.SetParamByName("processlist_query", "SELECT 1")
	other.SetParamByName("processlist_start_time", time.Now().Add(-3*time.Second))
	other.SetParamByName("processlist_rows_sent", int64(4))
	other.SetParamByName("processlist_rows_examined", int64(9))
	executor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{current, other}
	})

	result := executor.executeProcesslistSelect(current, "SHOW PROCESSLIST")
	require.Len(t, result.Records, 2)
	require.Equal(t, int64(42), result.Records[0].GetValues()[0].Int())
	require.Equal(t, "alice", result.Records[0].GetValues()[1].String())
	require.Equal(t, int64(43), result.Records[1].GetValues()[0].Int())
	require.Equal(t, "Query", result.Records[1].GetValues()[4].String())
	require.Equal(t, "SELECT 1", result.Records[1].GetValues()[7].String())
	require.GreaterOrEqual(t, result.Records[1].GetValues()[5].Int(), int64(2))
	require.Equal(t, int64(4), result.Records[1].GetValues()[8].Int())
	require.Equal(t, int64(9), result.Records[1].GetValues()[9].Int())
}

func TestXMySQLExecutor_ProcesslistWithoutProcessPrivilegeHidesOtherUsers(t *testing.T) {
	executor := &XMySQLExecutor{}
	current := newTestMySQLSession()
	current.ctx.SetConnectionID(51)
	current.SetParamByName("user", "alice")
	other := newTestMySQLSession()
	other.ctx.SetConnectionID(52)
	other.SetParamByName("user", "bob")
	executor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{current, other}
	})

	result := executor.executeProcesslistSelect(current, "SHOW PROCESSLIST")
	require.Len(t, result.Records, 1)
	require.Equal(t, int64(51), result.Records[0].GetValues()[0].Int())
	require.Equal(t, "alice", result.Records[0].GetValues()[1].String())
}

func TestXMySQLExecutor_ProcesslistAppliesSimpleWhereFilters(t *testing.T) {
	executor := &XMySQLExecutor{}
	current := newTestMySQLSession()
	current.ctx.SetConnectionID(61)
	current.SetParamByName("user", "alice")
	current.SetParamByName("host", "localhost")
	current.SetParamByName("global_privileges", []common.PrivilegeType{common.ProcessPriv})
	other := newTestMySQLSession()
	other.ctx.SetConnectionID(62)
	other.SetParamByName("user", "bob")
	other.SetParamByName("host", "10.0.0.2")
	other.SetParamByName("processlist_query", "SELECT orders FROM app")
	executor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{current, other}
	})

	result := executor.executeProcesslistSelect(current, "SELECT * FROM information_schema.processlist WHERE ID = 62 AND INFO LIKE '%orders%'")
	require.Len(t, result.Records, 1)
	require.Equal(t, int64(62), result.Records[0].GetValues()[0].Int())
}

func TestXMySQLExecutor_ProcesslistAppliesCommonComparisonAndDisjunctionFilters(t *testing.T) {
	executor := &XMySQLExecutor{}
	current := newTestMySQLSession()
	current.ctx.SetConnectionID(71)
	current.SetParamByName("user", "alice")
	current.SetParamByName("global_privileges", []common.PrivilegeType{common.ProcessPriv})
	querySession := newTestMySQLSession()
	querySession.ctx.SetConnectionID(72)
	querySession.SetParamByName("user", "bob")
	querySession.SetParamByName("processlist_query", "SELECT orders FROM app")
	sleepSession := newTestMySQLSession()
	sleepSession.ctx.SetConnectionID(73)
	sleepSession.SetParamByName("user", "carol")
	executor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{current, querySession, sleepSession}
	})

	result := executor.executeProcesslistSelect(current, "SELECT * FROM information_schema.processlist WHERE COMMAND <> 'Sleep' AND USER IN ('bob', 'carol')")
	require.Len(t, result.Records, 1)
	require.Equal(t, int64(72), result.Records[0].GetValues()[0].Int())

	result = executor.executeProcesslistSelect(current, "SELECT * FROM information_schema.processlist WHERE ID = 71 OR ID = 73")
	require.Len(t, result.Records, 2)
	require.Equal(t, int64(71), result.Records[0].GetValues()[0].Int())
	require.Equal(t, int64(73), result.Records[1].GetValues()[0].Int())

	result = executor.executeProcesslistSelect(current, "SELECT * FROM information_schema.processlist WHERE INFO NOT LIKE '%orders%'")
	require.Len(t, result.Records, 1)
	require.Equal(t, int64(73), result.Records[0].GetValues()[0].Int())
}

func TestProcesslistConditionMatchesNullAndNotIn(t *testing.T) {
	indexes := map[string]int{"id": 0, "user": 1, "host": 2, "db": 3, "command": 4, "state": 6, "info": 7}
	row := []interface{}{int64(81), "alice", nil, "app", "Sleep", int64(0), nil, nil, int64(0), int64(0)}
	require.True(t, processlistConditionMatches(row, indexes, "HOST IS NULL"))
	require.False(t, processlistConditionMatches(row, indexes, "INFO IS NOT NULL"))
	require.True(t, processlistConditionMatches(row, indexes, "USER NOT IN ('bob', 'carol')"))
	require.True(t, processlistConditionMatches(row, indexes, "USER IN ('alice', 'carol')"))
	require.False(t, processlistConditionMatches(row, indexes, "USER IN ('bob', 'carol')"))
}

func TestXMySQLExecutor_PerformanceSchemaThreadsUsesLiveSessionProvider(t *testing.T) {
	executor := &XMySQLExecutor{}
	current := newTestMySQLSession()
	current.ctx.SetConnectionID(91)
	current.SetParamByName("user", "alice")
	current.SetParamByName("global_privileges", []common.PrivilegeType{common.ProcessPriv})
	other := newTestMySQLSession()
	other.ctx.SetConnectionID(92)
	other.SetParamByName("user", "bob")
	other.SetParamByName("database", "orders")
	other.SetParamByName("processlist_query", "SELECT 1")
	executor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{current, other}
	})

	result := executor.executePerformanceSchemaThreadsSelect("SELECT * FROM performance_schema.threads", current)
	require.Len(t, result.Records, 2)
	require.Equal(t, int64(91), result.Records[0].GetValues()[0].Int())
	require.Equal(t, int64(92), result.Records[1].GetValues()[0].Int())
	require.Equal(t, "orders", result.Records[1].GetValues()[6].String())
	require.Equal(t, "Query", result.Records[1].GetValues()[7].String())

	result = executor.executeProcesslistSelect(current, "SELECT * FROM information_schema.processlist WHERE ID <> CONNECTION_ID()")
	require.Len(t, result.Records, 1)
	require.Equal(t, int64(92), result.Records[0].GetValues()[0].Int())
}
