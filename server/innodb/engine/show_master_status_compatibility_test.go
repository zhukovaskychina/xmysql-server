package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func TestShowMasterStatusExposesSourcePositionAndGTIDs(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	source, err := replication.NewSource(t.TempDir(), "master-status", 7)
	require.NoError(t, err)
	events, err := source.AppendCommitted([]replication.Statement{{Database: "app", SQL: "insert into t values (1)"}})
	require.NoError(t, err)
	executor := engine.QueryExecutor
	executor.SetReplicationSourceProvider(func() *replication.Source { return source })
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results, RawQuery: "show master status"}
	stmt, err := sqlparser.Parse("show master status")
	require.NoError(t, err)
	executor.executeShowStatementWithQuery(ctx, stmt.(*sqlparser.Show), nil, "show master status")
	result := <-results
	require.NoError(t, result.Err)
	selectResult := result.Data.(*SelectResult)
	require.Len(t, selectResult.Records, 1)
	require.Equal(t, "master-status:1", selectResult.Records[0].GetValues()[4].String())
	_, nativePosition := source.NativeCurrentFilePosition()
	require.Equal(t, nativePosition, uint64(selectResult.Records[0].GetValues()[1].Int()))
	require.Greater(t, events[len(events)-1].Position, uint64(0))
}

func TestShowMasterStatusUsesRotatedNativeFileAndPosition(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	sourceDir := t.TempDir()
	source, err := replication.NewSource(sourceDir, "master-status-rotate", 7)
	require.NoError(t, err)
	_, err = source.AppendCommitted([]replication.Statement{{Database: "app", SQL: "insert into t values (1)"}})
	require.NoError(t, err)
	_, err = source.Rotate()
	require.NoError(t, err)
	_, err = source.AppendCommitted([]replication.Statement{{Database: "app", SQL: "insert into t values (2)"}})
	require.NoError(t, err)
	executor := engine.QueryExecutor
	executor.SetReplicationSourceProvider(func() *replication.Source { return source })
	results := make(chan *Result, 1)
	query := "show source status"
	stmt, err := sqlparser.Parse(query)
	require.NoError(t, err)
	executor.executeShowStatementWithQuery(&ExecutionContext{Context: context.Background(), Results: results, RawQuery: query}, stmt.(*sqlparser.Show), nil, query)
	result := <-results
	require.NoError(t, result.Err)
	selectResult := result.Data.(*SelectResult)
	require.Len(t, selectResult.Records, 1)
	file, position := source.NativeCurrentFilePosition()
	require.Equal(t, file, selectResult.Records[0].GetValues()[0].String())
	require.Equal(t, position, uint64(selectResult.Records[0].GetValues()[1].Int()))
	require.Equal(t, "binlog.000002", file)
	reloaded, err := replication.NewSource(sourceDir, "master-status-rotate", 7)
	require.NoError(t, err)
	reloadedFile, reloadedPosition := reloaded.NativeCurrentFilePosition()
	require.Equal(t, file, reloadedFile)
	require.Equal(t, position, reloadedPosition)
}
