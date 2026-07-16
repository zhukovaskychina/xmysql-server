package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
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
	got := <-executor.ExecuteQuery(nil, sql, databaseName)
	require.NoError(t, got.Err)
}

func mustQuerySQL(t *testing.T, executor *XMySQLEngine, databaseName, sql string) [][]interface{} {
	t.Helper()
	got := <-executor.ExecuteQuery(nil, sql, databaseName)
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
