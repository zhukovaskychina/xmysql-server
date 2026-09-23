package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func TestShowReplicaStatusExposesReplicationRuntimeState(t *testing.T) {
	executor := &XMySQLExecutor{}
	executor.SetReplicationStatusProvider(func() replication.StatusSnapshot {
		return replication.StatusSnapshot{
			Role:              replication.RoleReplica,
			UUID:              "replica-1",
			SourceURL:         "http://source:8080",
			ExecutedGTIDs:     "source-1:1-4",
			SourcePosition:    42,
			ReplicationLagSec: 0.25,
		}
	})
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results, RawQuery: "show replica status"}

	executor.executeShowStatementWithQuery(ctx, &sqlparser.Show{Type: "replica"}, nil, "show replica status")
	result := <-results
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, selectResult.Records, 1)
	require.Contains(t, selectResult.Columns, "Replica_IO_Running")
	require.Contains(t, selectResult.Columns, "Seconds_Behind_Source")
	require.Contains(t, selectResult.Columns, "Executed_Gtid_Set")

	values := selectResult.Records[0].GetValues()
	for i, column := range selectResult.Columns {
		switch column {
		case "Replica_IO_Running":
			require.Equal(t, "Yes", values[i].String())
		case "Seconds_Behind_Source":
			require.Equal(t, "0.25", values[i].String())
		case "Executed_Gtid_Set":
			require.Equal(t, "source-1:1-4", values[i].String())
		}
	}
}
