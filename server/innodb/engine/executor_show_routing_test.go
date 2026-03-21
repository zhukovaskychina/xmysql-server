package engine

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

type testMySQLSession struct {
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
	return s.params[name]
}
func (s *testMySQLSession) SetParamByName(name string, value interface{}) {
	s.params[name] = value
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

func TestXMySQLExecutor_ExecuteQuery_ShowColumnsReturnsStubColumns(t *testing.T) {
	executor := &XMySQLExecutor{}
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
	executor := &XMySQLExecutor{}
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
	executor := &XMySQLExecutor{}
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

func TestXMySQLExecutor_ExecuteQuery_ShowTablesLikeFiltersRows(t *testing.T) {
	executor := &XMySQLExecutor{}
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
	assert.Equal(t, "QUERY", result.ResultType)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, []string{"Database"}, data["columns"])
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	assert.Equal(t, "Found "+strconv.Itoa(len(rows))+" databases", result.Message)
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
	assert.Equal(t, "QUERY", result.ResultType)
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
	assert.Equal(t, "QUERY", result.ResultType)
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
	assert.Equal(t, "QUERY", result.ResultType)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "app_main", rows[0][0])
}

func TestXMySQLExecutor_ExecuteQuery_ShowTablesWhereFiltersRows(t *testing.T) {
	executor := &XMySQLExecutor{}
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
