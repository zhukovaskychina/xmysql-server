package engine

import (
	"context"
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
