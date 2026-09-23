package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func TestShowReplicaHostsExposesNativeRegistrations(t *testing.T) {
	registry := replication.NewReplicaRegistry()
	registry.Register("replica-7", replication.ReplicaRegistration{
		ServerID: 7, ReportHost: "replica-7", ReportPort: 3310, MasterID: 1,
	})
	registry.Register("replica-3", replication.ReplicaRegistration{
		ServerID: 3, ReportHost: "replica-3", ReportPort: 3311, MasterID: 1,
	})

	executor := &XMySQLExecutor{}
	executor.SetReplicaRegistrationProvider(registry.Snapshot)
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results, RawQuery: "show slave hosts"}
	stmt, err := sqlparser.Parse("show slave hosts")
	require.NoError(t, err)
	executor.executeShowStatementWithQuery(ctx, stmt.(*sqlparser.Show), nil, "show slave hosts")

	result := <-results
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, []string{"Server_id", "Host", "Port", "Master_id", "Slave_UUID"}, selectResult.Columns)
	require.Len(t, selectResult.Records, 2)
	require.Equal(t, "3", selectResult.Records[0].GetValues()[0].String())
	require.Equal(t, "replica-7", selectResult.Records[1].GetValues()[1].String())
}

func TestShowReplicasReturnsEmptyResultWithoutRegistrations(t *testing.T) {
	executor := &XMySQLExecutor{}
	executor.SetReplicaRegistrationProvider(func() []replication.ReplicaRegistration { return nil })
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results, RawQuery: "show replicas"}
	stmt, err := sqlparser.Parse("show replicas")
	require.NoError(t, err)
	executor.executeShowStatementWithQuery(ctx, stmt.(*sqlparser.Show), nil, "show replicas")

	result := <-results
	require.NoError(t, result.Err)
	selectResult := result.Data.(*SelectResult)
	require.Empty(t, selectResult.Records)
	require.Len(t, selectResult.Columns, 5)
}
