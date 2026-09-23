package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func TestShowBinaryLogsAndBinlogEventsUseConfiguredSource(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "source-1", 17)
	require.NoError(t, err)
	_, err = source.AppendTransaction(1, nil, []replication.Statement{{Database: "app", SQL: "insert into docs values (1)"}})
	require.NoError(t, err)
	executor := &XMySQLExecutor{}
	executor.SetReplicationSourceProvider(func() *replication.Source { return source })

	for _, query := range []string{"show binary logs", "show binlog events"} {
		results := make(chan *Result, 1)
		ctx := &ExecutionContext{Context: context.Background(), Results: results, RawQuery: query}
		stmt, parseErr := sqlparser.Parse(query)
		require.NoError(t, parseErr)
		executor.executeShowStatementWithQuery(ctx, stmt.(*sqlparser.Show), nil, query)
		result := <-results
		require.NoError(t, result.Err, query)
		selectResult, ok := result.Data.(*SelectResult)
		require.True(t, ok, query)
		require.NotEmpty(t, selectResult.Records, query)
		if query == "show binary logs" {
			require.Equal(t, []string{"Log_name", "File_size", "Encrypted"}, selectResult.Columns)
			require.Greater(t, selectResult.Records[0].GetValues()[1].Int(), int64(0))
		} else {
			require.Equal(t, []string{"Log_name", "Pos", "Event_type", "Server_id", "End_log_pos", "Info"}, selectResult.Columns)
		}
	}
}

func TestShowBinaryLogsListsRotatedNativeFiles(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "source-1", 17)
	require.NoError(t, err)
	_, err = source.AppendCommitted([]replication.Statement{{Database: "app", SQL: "insert into docs values (1)"}})
	require.NoError(t, err)
	_, err = source.Rotate()
	require.NoError(t, err)
	_, err = source.AppendCommitted([]replication.Statement{{Database: "app", SQL: "insert into docs values (2)"}})
	require.NoError(t, err)

	executor := &XMySQLExecutor{}
	executor.SetReplicationSourceProvider(func() *replication.Source { return source })
	results := make(chan *Result, 1)
	query := "show binary logs"
	stmt, err := sqlparser.Parse(query)
	require.NoError(t, err)
	executor.executeShowStatementWithQuery(&ExecutionContext{Context: context.Background(), Results: results, RawQuery: query}, stmt.(*sqlparser.Show), nil, query)
	result := <-results
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, selectResult.Records, 2)
	require.Equal(t, "binlog.000001", selectResult.Records[0].GetValues()[0].String())
	require.Equal(t, "binlog.000002", selectResult.Records[1].GetValues()[0].String())
	require.Greater(t, selectResult.Records[0].GetValues()[1].Int(), int64(0))
	require.Greater(t, selectResult.Records[1].GetValues()[1].Int(), int64(0))
}

func TestShowBinlogEventsHonorsInFromAndLimit(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "source-1", 17)
	require.NoError(t, err)
	_, err = source.AppendCommitted([]replication.Statement{{Database: "app", SQL: "insert into docs values (1)"}})
	require.NoError(t, err)
	_, err = source.AppendCommitted([]replication.Statement{{Database: "app", SQL: "insert into docs values (2)"}})
	require.NoError(t, err)
	executor := &XMySQLExecutor{}
	executor.SetReplicationSourceProvider(func() *replication.Source { return source })

	query := "show binlog events in 'binlog.000001' from 4 limit 1, 2"
	stmt, err := sqlparser.Parse(query)
	require.NoError(t, err)
	results := make(chan *Result, 1)
	executor.executeShowStatementWithQuery(&ExecutionContext{Context: context.Background(), Results: results, RawQuery: query}, stmt.(*sqlparser.Show), nil, query)
	result := <-results
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, selectResult.Records, 2)
	for _, record := range selectResult.Records {
		require.Equal(t, "binlog.000001", record.GetValues()[0].String())
	}
}

func TestShowBinlogEventsFiltersRotatedFile(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "source-1", 17)
	require.NoError(t, err)
	_, err = source.AppendCommitted([]replication.Statement{{Database: "app", SQL: "insert into docs values (1)"}})
	require.NoError(t, err)
	_, err = source.Rotate()
	require.NoError(t, err)
	_, err = source.AppendCommitted([]replication.Statement{{Database: "app", SQL: "insert into docs values (2)"}})
	require.NoError(t, err)

	executor := &XMySQLExecutor{}
	executor.SetReplicationSourceProvider(func() *replication.Source { return source })
	query := "show binlog events in 'binlog.000002' from 4"
	stmt, err := sqlparser.Parse(query)
	require.NoError(t, err)
	results := make(chan *Result, 1)
	executor.executeShowStatementWithQuery(&ExecutionContext{Context: context.Background(), Results: results, RawQuery: query}, stmt.(*sqlparser.Show), nil, query)
	result := <-results
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, selectResult.Records, 3)
	for _, record := range selectResult.Records {
		require.Equal(t, "binlog.000002", record.GetValues()[0].String())
		require.NotContains(t, record.GetValues()[5].String(), "values (1)")
	}
}

func TestShowBinlogEventsReportsRotatedTargetFile(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "source-1", 17)
	require.NoError(t, err)
	_, err = source.AppendCommitted([]replication.Statement{{Database: "app", SQL: "insert into docs values (1)"}})
	require.NoError(t, err)
	_, err = source.Rotate()
	require.NoError(t, err)
	executor := &XMySQLExecutor{}
	executor.SetReplicationSourceProvider(func() *replication.Source { return source })
	query := "show binlog events in 'binlog.000001'"
	stmt, err := sqlparser.Parse(query)
	require.NoError(t, err)
	results := make(chan *Result, 1)
	executor.executeShowStatementWithQuery(&ExecutionContext{Context: context.Background(), Results: results, RawQuery: query}, stmt.(*sqlparser.Show), nil, query)
	result := <-results
	require.NoError(t, result.Err)
	selectResult := result.Data.(*SelectResult)
	require.NotEmpty(t, selectResult.Records)
	rotateInfo := ""
	for _, record := range selectResult.Records {
		if record.GetValues()[2].String() == "Rotate" {
			rotateInfo = record.GetValues()[5].String()
		}
	}
	require.Equal(t, "binlog.000002", rotateInfo)
}

func TestShowBinlogEventsUsesNativePhysicalPositions(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "source-1", 17)
	require.NoError(t, err)
	_, err = source.AppendCommitted([]replication.Statement{{Database: "app", SQL: "insert into docs values (1)"}})
	require.NoError(t, err)
	native, err := source.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	require.Len(t, native, 5)
	queryPosition := native[3].Position // FDE, PREVIOUS_GTIDS, GTID, QUERY

	executor := &XMySQLExecutor{}
	executor.SetReplicationSourceProvider(func() *replication.Source { return source })
	query := fmt.Sprintf("show binlog events in 'binlog.000001' from %d", queryPosition)
	stmt, err := sqlparser.Parse(query)
	require.NoError(t, err)
	results := make(chan *Result, 1)
	executor.executeShowStatementWithQuery(&ExecutionContext{Context: context.Background(), Results: results, RawQuery: query}, stmt.(*sqlparser.Show), nil, query)
	result := <-results
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, selectResult.Records, 2)
	require.Equal(t, int64(queryPosition), selectResult.Records[0].GetValues()[1].Int())
	require.Greater(t, selectResult.Records[0].GetValues()[4].Int(), int64(queryPosition))
	require.Equal(t, "Xid", selectResult.Records[1].GetValues()[2].String())
}

func TestShowBinlogEventsReportsNativeRowEventTypes(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "source-1", 17)
	require.NoError(t, err)
	_, err = source.AppendTransaction(1, []replication.RowChange{{
		Table: "app.docs", Action: "insert", Columns: []string{"id"},
		After: map[string]interface{}{"id": int64(1)},
	}}, nil)
	require.NoError(t, err)
	executor := &XMySQLExecutor{}
	executor.SetReplicationSourceProvider(func() *replication.Source { return source })
	query := "show binlog events in 'binlog.000001' from 4"
	stmt, err := sqlparser.Parse(query)
	require.NoError(t, err)
	results := make(chan *Result, 1)
	executor.executeShowStatementWithQuery(&ExecutionContext{Context: context.Background(), Results: results, RawQuery: query}, stmt.(*sqlparser.Show), nil, query)
	result := <-results
	require.NoError(t, result.Err)
	selectResult := result.Data.(*SelectResult)
	seen := map[string]bool{}
	for _, record := range selectResult.Records {
		seen[record.GetValues()[2].String()] = true
	}
	require.True(t, seen["Table_map"])
	require.True(t, seen["Write_rows"])
}

func TestShowRelaylogEventsReportsAppliedReplicaTransactions(t *testing.T) {
	replica, err := replication.NewReplica(t.TempDir())
	require.NoError(t, err)
	gtid := replication.GTID{UUID: "source-1", Seq: 7}
	require.NoError(t, replica.Apply([]replication.BinlogEvent{
		{Type: replication.EventBegin, ServerID: 17, Position: 4, GTID: gtid},
		{Type: replication.EventRow, ServerID: 17, Position: 5, GTID: gtid, Changes: []replication.RowChange{{Table: "app.docs", Action: "insert"}}},
		{Type: replication.EventCommit, ServerID: 17, Position: 6, GTID: gtid},
	}))

	executor := &XMySQLExecutor{}
	executor.SetReplicationReplicaProvider(func() *replication.Replica { return replica })
	query := "show relaylog events from 5 limit 0, 2"
	stmt, err := sqlparser.Parse(query)
	require.NoError(t, err)
	results := make(chan *Result, 1)
	executor.executeShowStatementWithQuery(&ExecutionContext{Context: context.Background(), Results: results, RawQuery: query}, stmt.(*sqlparser.Show), nil, query)
	result := <-results
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, []string{"Log_name", "Pos", "Event_type", "Server_id", "End_log_pos", "Info"}, selectResult.Columns)
	require.Len(t, selectResult.Records, 2)
	require.Equal(t, "relaylog.000001", selectResult.Records[0].GetValues()[0].String())
	require.Equal(t, "Write_rows", selectResult.Records[0].GetValues()[2].String())
	require.Equal(t, "Xid", selectResult.Records[1].GetValues()[2].String())
}
