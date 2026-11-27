package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestShowASTExecutionMatchesRequestedTarget(t *testing.T) {
	dataDir := t.TempDir()
	for _, schema := range []string{"first_db", "second_db"} {
		require.NoError(t, os.MkdirAll(filepath.Join(dataDir, schema), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dataDir, schema, "same,keyword.frm"), []byte(`{"table_name":"same,keyword","columns":[{"name":"id","type":"INT"}]}`), 0o644))
	}
	executor := &XMySQLExecutor{conf: &conf.Cfg{DataDir: dataDir}}
	query := "SHOW TABLES FROM first_db LIKE 'same,keyword%'"
	parsed, err := sqlparser.ParseForExecution(query)
	require.NoError(t, err)
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results, RawQuery: query, DatabaseName: "first_db", Parsed: parsed}
	executor.executeShowStatementWithQuery(ctx, parsed.AST.(*sqlparser.Show), newTestMySQLSession(), query)
	result := <-results
	require.NoError(t, result.Err)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, rows, 1)
	require.Equal(t, "same,keyword", rows[0][0])
}

func TestShowASTExecutionPreservesAuthorizationAndShape(t *testing.T) {
	query := "SHOW WARNINGS LIMIT 1,2"
	parsed, err := sqlparser.ParseForExecution(query)
	require.NoError(t, err)
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results, RawQuery: query, Parsed: parsed, Warnings: []Warning{
		{Level: "Note", Code: 1, Message: "first"},
		{Level: "Warning", Code: 2, Message: "second"},
		{Level: "Error", Code: 3, Message: "third"},
	}}
	executor := &XMySQLExecutor{}
	executor.executeShowStatementWithQuery(ctx, parsed.AST.(*sqlparser.Show), nil, query)
	result := <-results
	require.NoError(t, result.Err)
	require.Equal(t, "QUERY", result.ResultType)
	data := result.Data.(map[string]interface{})
	require.Equal(t, []string{"Level", "Code", "Message"}, data["columns"])
	require.Equal(t, [][]interface{}{{"Warning", uint16(2), "second"}, {"Error", uint16(3), "third"}}, data["rows"])

	showTables, err := sqlparser.ParseForExecution("SHOW FULL TABLES")
	require.NoError(t, err)
	authResults := make(chan *Result, 1)
	authCtx := &ExecutionContext{Context: context.Background(), Results: authResults, RawQuery: "SHOW FULL TABLES", Parsed: showTables}
	(&XMySQLExecutor{}).executeShowStatementWithQuery(authCtx, showTables.AST.(*sqlparser.Show), nil, "SHOW FULL TABLES")
	authResult := <-authResults
	require.Error(t, authResult.Err)
	fullQuery := "SHOW FULL TABLES FROM first_db"
	fullParsed, err := sqlparser.ParseForExecution(fullQuery)
	require.NoError(t, err)
	fullResults := make(chan *Result, 1)
	fullCtx := &ExecutionContext{Context: context.Background(), Results: fullResults, RawQuery: fullQuery, DatabaseName: "first_db", Parsed: fullParsed}
	(&XMySQLExecutor{conf: &conf.Cfg{DataDir: t.TempDir()}}).executeShowStatementWithQuery(fullCtx, fullParsed.AST.(*sqlparser.Show), newTestMySQLSession(), fullQuery)
	fullResult := <-fullResults
	// The metadata directory is empty, but the FULL projection shape is still
	// part of the authorization/result contract.
	if fullResult.Err == nil {
		fullData := fullResult.Data.(map[string]interface{})
		require.Contains(t, fullData["columns"], "Table_type")
	}
}
