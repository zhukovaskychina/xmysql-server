package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShowWarningsUsesSessionStateAndClearsAtStatementBoundary(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	ctx := &ExecutionContext{Session: session, Results: make(chan *Result, 1)}
	ctx.addWarning("Warning", 1265, "Data truncated")
	executor.QueryExecutor.executeShowWarnings(ctx)
	result := <-ctx.Results
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	require.Len(t, data["rows"], 1)
	// The next statement starts a new warning context, matching MySQL's
	// statement-boundary behavior.
	_ = <-executor.ExecuteQuery(session, "select 1", "")
	next := <-executor.ExecuteQuery(session, "show warnings", "")
	require.NoError(t, next.Err)
	nextData := next.Data.(map[string]interface{})
	require.Empty(t, nextData["rows"])
}

func TestShowWarningsSupportsLimitAndOffset(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()

	ctx := &ExecutionContext{Session: session, RawQuery: "show warnings limit 1, 1", Results: make(chan *Result, 1)}
	ctx.addWarning("Warning", 1001, "first")
	ctx.addWarning("Warning", 1002, "second")
	ctx.addWarning("Warning", 1003, "third")
	executor.QueryExecutor.executeShowWarnings(ctx)
	result := <-ctx.Results
	data := result.Data.(map[string]interface{})
	require.Equal(t, [][]interface{}{{"Warning", uint16(1002), "second"}}, data["rows"])

	ctx = &ExecutionContext{Session: session, RawQuery: "show warnings limit 1 offset 2", Results: make(chan *Result, 1)}
	ctx.Warnings = []Warning{
		{Level: "Warning", Code: 1001, Message: "first"},
		{Level: "Warning", Code: 1002, Message: "second"},
		{Level: "Warning", Code: 1003, Message: "third"},
	}
	session.SetParamByName("warnings", ctx.Warnings)
	executor.QueryExecutor.executeShowWarnings(ctx)
	result = <-ctx.Results
	data = result.Data.(map[string]interface{})
	require.Equal(t, [][]interface{}{{"Warning", uint16(1003), "third"}}, data["rows"])

	queryResult := <-executor.ExecuteQuery(session, "show warnings limit 1, 1", "")
	require.NoError(t, queryResult.Err)
	queryData := queryResult.Data.(map[string]interface{})
	require.Equal(t, [][]interface{}{{"Warning", uint16(1002), "second"}}, queryData["rows"])
}

func TestShowWarningCountReturnsSessionWarningCount(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("warnings", []Warning{
		{Level: "Warning", Code: 1001, Message: "first"},
		{Level: "Warning", Code: 1002, Message: "second"},
	})

	result := <-executor.ExecuteQuery(session, "show count(*) warnings", "")
	require.NoError(t, result.Err)
	data := result.Data.(map[string]interface{})
	require.Equal(t, []string{"@@session.warning_count"}, data["columns"])
	require.Equal(t, [][]interface{}{{int64(2)}}, data["rows"])

	session.SetParamByName("warnings", []Warning{{Level: "Error", Code: 1064, Message: "syntax error"}})
	result = <-executor.ExecuteQuery(session, "show count(*) errors", "")
	require.NoError(t, result.Err)
	data = result.Data.(map[string]interface{})
	require.Equal(t, []string{"@@session.error_count"}, data["columns"])
	require.Equal(t, [][]interface{}{{int64(1)}}, data["rows"])

	session.SetParamByName("warnings", []Warning{
		{Level: "Error", Code: 1064, Message: "first error"},
		{Level: "Error", Code: 1146, Message: "second error"},
	})
	result = <-executor.ExecuteQuery(session, "show errors limit 1", "")
	require.NoError(t, result.Err)
	data = result.Data.(map[string]interface{})
	require.Equal(t, [][]interface{}{{"Error", uint16(1064), "first error"}}, data["rows"])
}

func TestFailedStatementIsAvailableThroughShowErrors(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()

	failed := <-executor.ExecuteQuery(session, "select * from missing_table", "app")
	require.Error(t, failed.Err)

	result := <-executor.ExecuteQuery(session, "show errors", "")
	require.NoError(t, result.Err)
	data := result.Data.(map[string]interface{})
	rows := data["rows"].([][]interface{})
	require.Len(t, rows, 1)
	require.Equal(t, "Error", rows[0][0])
	require.NotEqual(t, uint16(0), rows[0][1])
	require.Contains(t, rows[0][2], "missing_table")

	count := <-executor.ExecuteQuery(session, "show count(*) errors", "")
	require.NoError(t, count.Err)
	countData := count.Data.(map[string]interface{})
	require.Equal(t, [][]interface{}{{int64(1)}}, countData["rows"])
}
