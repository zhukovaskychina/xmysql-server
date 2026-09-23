package engine

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestLockWaitIsVisibleInProcesslistAndPerformanceSchema(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 7, 1, 9, manager.LOCK_X))
	require.ErrorIs(t, locks.AcquireLock(2, 7, 1, 9, manager.LOCK_X), manager.ErrLockConflict)
	var waiting bool
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond * 5) {
		if len(locks.WaitGraphSnapshot()) > 0 {
			waiting = true
			break
		}
	}
	require.True(t, waiting, "expected wait graph edge")

	executor := &XMySQLExecutor{}
	executor.SetLockManager(locks)
	session := newTestMySQLSession()
	session.SetParamByName("session_id", int64(2))
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results, RawQuery: "show processlist"}
	executor.executeShowStatementWithQuery(ctx, &sqlparser.Show{Type: "processlist"}, session, "show processlist")
	process := (<-results).Data.(*SelectResult)
	require.Equal(t, "Waiting for lock", process.Records[0].GetValues()[6].String())

	pSchema := executor.executePerformanceSchemaThreadsSelect("select * from performance_schema.threads")
	require.Equal(t, "Waiting for lock", pSchema.Records[0].GetValues()[9].String())
	locks.ReleaseLocks(1)
	require.NoError(t, locks.AcquireLock(2, 7, 1, 9, manager.LOCK_X))
	locks.ReleaseLocks(2)
}
