package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGTIDFunctionsExecuteThroughSQLProjection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, executor.Close()) })

	rows := mustQuerySQL(t, executor, "", "select GTID_SUBSET('uuid-b:4-5:8,uuid-a:1-3', 'uuid-a:1-10,uuid-b:1-9') as is_subset, GTID_SUBTRACT('uuid-a:1-10,uuid-b:4-8', 'uuid-a:3-5,uuid-b:6') as remainder")
	// The query result path exposes scalar projection values as their wire-facing
	// text representation, matching the existing SELECT literal contract.
	require.Equal(t, [][]interface{}{{"1", "uuid-a:1-2:6-10,uuid-b:4-5:7-8"}}, rows)
}

func TestGTIDFunctionsRejectInvalidSQLArguments(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, executor.Close()) })

	result := <-executor.ExecuteQuery(nil, "select GTID_SUBSET('uuid:0', 'uuid:1')", "")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "GTID")
}
