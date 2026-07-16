package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

func TestTransactionStatementsReturnOK(t *testing.T) {
	executor := NewXMySQLExecutor(nil, &conf.Cfg{InnodbDataDir: t.TempDir()})
	session := newTestMySQLSession()

	for _, query := range []string{
		"begin",
		"start transaction",
		"commit; ",
		"rollback",
		"savepoint sp1",
		"rollback to savepoint sp1",
		"release savepoint sp1",
	} {
		t.Run(query, func(t *testing.T) {
			results := executor.ExecuteWithQuery(session, query, "")
			got := <-results
			require.NoError(t, got.Err)
			require.Equal(t, common.RESULT_TYPE_QUERY, got.ResultType)
		})
	}
}

func TestInvalidTransactionStatementsAreNotAccepted(t *testing.T) {
	for _, query := range []string{
		"savepoint ",
		"rollback to savepoint ",
		"rollback to ",
		"release savepoint ",
	} {
		t.Run(query, func(t *testing.T) {
			_, _, ok := normalizedTransactionCommand(query)
			require.False(t, ok)
		})
	}
}

func TestEngineTransactionStatementsReturnOKBeforeParse(t *testing.T) {
	cfg := &conf.Cfg{InnodbDataDir: t.TempDir()}
	executor := NewXMySQLExecutor(nil, cfg)
	xengine := &XMySQLEngine{conf: cfg, QueryExecutor: executor}
	session := newTestMySQLSession()

	for _, query := range []string{
		"begin",
		"savepoint sp1",
		"rollback to savepoint sp1",
		"release savepoint sp1",
		"commit",
	} {
		t.Run(query, func(t *testing.T) {
			got := <-xengine.ExecuteQuery(session, query, "")
			require.NoError(t, got.Err)
			require.Equal(t, common.RESULT_TYPE_QUERY, got.ResultType)
		})
	}

	require.Equal(t, false, session.GetParamByName("in_transaction"))
	require.False(t, session.SessionContext().GetInTransaction())
	require.Empty(t, session.GetParamByName("savepoints"))
}
