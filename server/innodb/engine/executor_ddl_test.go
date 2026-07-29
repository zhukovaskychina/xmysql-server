package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestDropDatabaseIfExistsMissingIsOk(t *testing.T) {
	tmp := t.TempDir()
	executor := &XMySQLExecutor{conf: &conf.Cfg{InnodbDataDir: tmp}}
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

func TestAlterTableAddColumnUpdatesFrm(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "app")
	require.NoError(t, os.MkdirAll(dbPath, 0755))

	executor := NewXMySQLExecutor(nil, &conf.Cfg{InnodbDataDir: tmp})
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

func TestAlterTableAddColumnsWithoutDefaultsOmitDefaultMetadata(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "app")
	require.NoError(t, os.MkdirAll(dbPath, 0755))

	executor := NewXMySQLExecutor(nil, &conf.Cfg{InnodbDataDir: tmp})
	createStmt, err := sqlparser.Parse("create table users (id int primary key)")
	require.NoError(t, err)
	require.NoError(t, executor.createTableImpl("app", "users", createStmt.(*sqlparser.DDL)))

	for _, query := range []string{
		"alter table users add column nickname varchar(100)",
		"alter table users add column login_count int not null",
	} {
		alterStmt, err := sqlparser.Parse(query)
		require.NoError(t, err)

		results := make(chan *Result, 1)
		executor.executeDDL(alterStmt.(*sqlparser.DDL), nil, "app", results)
		require.NoError(t, (<-results).Err)
	}

	columns := readFrmColumns(t, filepath.Join(dbPath, "users.frm"))
	for _, name := range []string{"nickname", "login_count"} {
		column := columns[name]
		require.NotNil(t, column)
		_, hasDefault := column["default"]
		require.False(t, hasDefault, "column %q without SQL DEFAULT must omit default metadata", name)
	}
	require.True(t, columns["nickname"]["nullable"].(bool))
	require.False(t, columns["login_count"]["nullable"].(bool))
}

func TestAlterTableAddColumnRefreshesDMLMetadata(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "app")
	require.NoError(t, os.MkdirAll(dbPath, 0755))

	cfg := &conf.Cfg{InnodbDataDir: tmp}
	executor := NewXMySQLExecutor(nil, cfg)
	createStmt, err := sqlparser.Parse("create table users (id int primary key)")
	require.NoError(t, err)
	require.NoError(t, executor.createTableImpl("app", "users", createStmt.(*sqlparser.DDL)))

	engine := &XMySQLEngine{conf: cfg, QueryExecutor: executor}
	alterResult := <-engine.ExecuteQuery(nil, "alter table users add column nickname varchar(100)", "app")
	require.NoError(t, alterResult.Err)

	dml := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	dml.SetDataDir(tmp)
	dml.schemaName = "app"
	dml.tableName = "users"
	tableMeta, err := dml.getTableMetadata()
	require.NoError(t, err)
	require.Len(t, tableMeta.Columns, 2)
	require.Equal(t, "nickname", tableMeta.Columns[1].Name)
}

func TestAlterTableAddColumnExistingRowsReadNullAndDefault(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)

	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1)")
	mustExecSQL(t, executor, "app", "alter table users add column nickname varchar(100)")
	mustExecSQL(t, executor, "app", "alter table users add column score int default 7")

	got := <-executor.ExecuteQuery(nil, "select nickname, score from users where id = 1", "app")
	require.NoError(t, got.Err)
	result, ok := got.Data.(*SelectResult)
	require.True(t, ok, "expected SelectResult, got %T", got.Data)
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	require.Len(t, values, 2)
	require.True(t, values[0].IsNull())
	require.Equal(t, int64(7), values[1].Int())
}

func TestTruncateKeepsTableMetadataResolvable(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)

	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key auto_increment, username varchar(50) not null)")
	mustExecSQL(t, executor, "app", "insert into users (username) values ('before')")
	mustExecSQL(t, executor, "app", "truncate table users")
	mustExecSQL(t, executor, "app", "insert into users (username) values ('after')")

	rows := mustQuerySQL(t, executor, "app", "select username from users")
	require.Equal(t, [][]interface{}{{"after"}}, rows)
}

func TestInformationSchemaTablesSelectReturnsJDBCMetadataColumns(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)

	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")

	got := <-executor.ExecuteQuery(nil,
		"select TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS from information_schema.tables where table_schema = 'app' and table_name = 'users'",
		"app",
	)
	require.NoError(t, got.Err)

	result, ok := got.Data.(*SelectResult)
	require.True(t, ok, "expected SelectResult, got %T", got.Data)
	require.Equal(t, []string{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS"}, result.Columns)
	require.Len(t, result.Records, 1)

	values := result.Records[0].GetValues()
	require.Len(t, values, 5)
	require.Equal(t, "app", values[0].ToString())
	require.Nil(t, values[1].Raw())
	require.Equal(t, "users", values[2].ToString())
	require.Equal(t, "TABLE", values[3].ToString())
	require.Equal(t, "", values[4].ToString())
}

func TestInformationSchemaJDBCProbeTablesReturnEmptyMetadataResults(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)

	cases := []struct {
		name    string
		query   string
		columns []string
	}{
		{
			name:    "procedures",
			query:   "SELECT ROUTINE_SCHEMA AS PROCEDURE_CAT, NULL AS PROCEDURE_SCHEM, ROUTINE_NAME AS PROCEDURE_NAME, NULL AS RESERVED_1, NULL AS RESERVED_2, NULL AS RESERVED_3, ROUTINE_COMMENT AS REMARKS, CASE WHEN ROUTINE_TYPE = 'PROCEDURE' THEN 1 ELSE 0 END AS PROCEDURE_TYPE, ROUTINE_NAME AS SPECIFIC_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME LIKE '%'",
			columns: []string{"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "RESERVED_1", "RESERVED_2", "RESERVED_3", "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME"},
		},
		{
			name:    "functions",
			query:   "SELECT ROUTINE_SCHEMA AS FUNCTION_CAT, NULL AS FUNCTION_SCHEM, ROUTINE_NAME AS FUNCTION_NAME, ROUTINE_COMMENT AS REMARKS, CASE WHEN ROUTINE_TYPE = 'FUNCTION' THEN 1 ELSE 0 END AS FUNCTION_TYPE, ROUTINE_NAME AS SPECIFIC_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME LIKE '%'",
			columns: []string{"FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME"},
		},
		{
			name:    "primary keys",
			query:   "SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, SEQ_IN_INDEX AS KEY_SEQ, 'PRIMARY' AS PK_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_NAME = 'missing' AND INDEX_NAME='PRIMARY'",
			columns: []string{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"},
		},
		{
			name:    "indexes",
			query:   "SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, NON_UNIQUE, NULL AS INDEX_QUALIFIER, INDEX_NAME, 3 AS TYPE, SEQ_IN_INDEX AS ORDINAL_POSITION, COLUMN_NAME, COLLATION AS ASC_OR_DESC, CARDINALITY, 0 AS PAGES, NULL AS FILTER_CONDITION FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_NAME = 'missing'",
			columns: []string{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES", "FILTER_CONDITION"},
		},
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
			name:    "collations",
			query:   "select collation_name, character_set_name, is_default from information_schema.collations",
			columns: []string{"COLLATION_NAME", "CHARACTER_SET_NAME", "IS_DEFAULT"},
		},
		{
			name:    "user privileges",
			query:   "select grantee, privilege_type, is_grantable from information_schema.user_privileges",
			columns: []string{"GRANTEE", "PRIVILEGE_TYPE", "IS_GRANTABLE"},
		},
		{
			name:    "schema privileges",
			query:   "select grantee, table_schema, privilege_type, is_grantable from information_schema.schema_privileges",
			columns: []string{"GRANTEE", "TABLE_SCHEMA", "PRIVILEGE_TYPE", "IS_GRANTABLE"},
		},
		{
			name:    "mysql procs priv",
			query:   "select Host, User, Routine_name, Proc_priv, Routine_type = 'PROCEDURE' as is_proc from mysql.procs_priv where Db = 'performance_schema'",
			columns: []string{"HOST", "USER", "ROUTINE_NAME", "PROC_PRIV", "IS_PROC"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := <-executor.ExecuteQuery(nil, tc.query, "")
			require.NoError(t, got.Err)

			result, ok := got.Data.(*SelectResult)
			require.True(t, ok, "expected SelectResult, got %T", got.Data)
			require.Equal(t, tc.columns, result.Columns)
			require.Empty(t, result.Records)
		})
	}
}

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

func TestShowFullTablesReturnsTableTypeColumn(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	session := newTestMySQLSession()

	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "use app")
	mustExecSessionSQL(t, executor, session, "app", "create table users (id int primary key)")

	got := <-executor.ExecuteQuery(session, "show full tables from app like 'users'", "app")
	require.NoError(t, got.Err)
	data, ok := got.Data.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, []string{"Tables_in_app", "Table_type"}, data["columns"])
	require.Equal(t, [][]interface{}{{"users", "BASE TABLE"}}, data["rows"])
}

func TestDropTableUsesCurrentDatabaseContext(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)

	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "drop table users")

	_, err := os.Stat(filepath.Join(tmp, "app", "users.frm"))
	require.True(t, os.IsNotExist(err))
}

func TestTransactionRollbackRestoresDMLChanges(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	session := newTestMySQLSession()

	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table accounts (id int primary key auto_increment, account_name varchar(50) not null)")

	mustExecSessionSQL(t, executor, session, "app", "begin")
	mustExecSessionSQL(t, executor, session, "app", "insert into accounts (account_name) values ('Alice')")
	mustExecSessionSQL(t, executor, session, "app", "rollback")

	rows := mustQuerySessionSQL(t, executor, session, "app", "select account_name from accounts")
	require.Empty(t, rows)
}

func TestTransactionRollbackToSavepointRestoresPartialDMLChanges(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	session := newTestMySQLSession()

	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table accounts (id int primary key auto_increment, account_name varchar(50) not null)")

	mustExecSessionSQL(t, executor, session, "app", "begin")
	mustExecSessionSQL(t, executor, session, "app", "insert into accounts (account_name) values ('Alice')")
	mustExecSessionSQL(t, executor, session, "app", "savepoint sp1")
	mustExecSessionSQL(t, executor, session, "app", "insert into accounts (account_name) values ('Bob')")
	mustExecSessionSQL(t, executor, session, "app", "rollback to savepoint sp1")
	mustExecSessionSQL(t, executor, session, "app", "insert into accounts (account_name) values ('Charlie')")
	mustExecSessionSQL(t, executor, session, "app", "commit")

	rows := mustQuerySessionSQL(t, executor, session, "app", "select account_name from accounts order by account_name")
	require.Equal(t, [][]interface{}{{"Alice"}, {"Charlie"}}, rows)
}

func TestTransactionRollbackAfterSetAutocommitOffRestoresDMLChanges(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	session := newTestMySQLSession()

	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table accounts (id int primary key auto_increment, account_name varchar(50) not null)")

	mustExecSessionSQL(t, executor, session, "app", "set autocommit=0")
	mustExecSessionSQL(t, executor, session, "app", "insert into accounts (account_name) values ('Alice')")
	mustExecSessionSQL(t, executor, session, "app", "rollback")

	rows := mustQuerySessionSQL(t, executor, session, "app", "select account_name from accounts")
	require.Empty(t, rows)
}

func newTestStorageIntegratedExecutor(t *testing.T, dataDir string) *XMySQLEngine {
	t.Helper()
	executor := NewXMySQLEngine(&conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        dataDir,
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	})
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	return executor
}

func mustExecSQL(t *testing.T, executor *XMySQLEngine, databaseName, sql string) {
	t.Helper()
	mustExecSessionSQL(t, executor, nil, databaseName, sql)
}

func mustExecSessionSQL(t *testing.T, executor *XMySQLEngine, session server.MySQLServerSession, databaseName, sql string) {
	t.Helper()
	got := <-executor.ExecuteQuery(session, sql, databaseName)
	require.NoError(t, got.Err)
}

func mustQuerySQL(t *testing.T, executor *XMySQLEngine, databaseName, sql string) [][]interface{} {
	t.Helper()
	return mustQuerySessionSQL(t, executor, nil, databaseName, sql)
}

func mustQuerySessionSQL(t *testing.T, executor *XMySQLEngine, session server.MySQLServerSession, databaseName, sql string) [][]interface{} {
	t.Helper()
	got := <-executor.ExecuteQuery(session, sql, databaseName)
	require.NoError(t, got.Err)

	result, ok := got.Data.(*SelectResult)
	require.True(t, ok, "expected SelectResult, got %T", got.Data)
	rows := make([][]interface{}, len(result.Records))
	for i, record := range result.Records {
		values := record.GetValues()
		rows[i] = make([]interface{}, len(values))
		for j, value := range values {
			raw := value.Raw()
			if bytes, ok := raw.([]byte); ok {
				rows[i][j] = string(bytes)
				continue
			}
			rows[i][j] = raw
		}
	}
	return rows
}

func readFrmColumns(t *testing.T, frmPath string) map[string]map[string]interface{} {
	t.Helper()
	raw, err := os.ReadFile(frmPath)
	require.NoError(t, err)

	var tableInfo struct {
		Columns []map[string]interface{} `json:"columns"`
	}
	require.NoError(t, json.Unmarshal(raw, &tableInfo))

	columns := make(map[string]map[string]interface{}, len(tableInfo.Columns))
	for _, column := range tableInfo.Columns {
		columns[column["name"].(string)] = column
	}
	return columns
}
