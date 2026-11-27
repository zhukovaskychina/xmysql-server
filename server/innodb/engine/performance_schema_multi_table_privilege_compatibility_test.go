package engine

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPerformanceSchemaMultiTableSelectRequiresEachTablePrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'p_s_join_reader'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "p_s_join_reader")
	session.SetParamByName("host", "localhost")
	query := "select c.name from performance_schema.setup_consumers c join performance_schema.setup_instruments i on 1=1 where c.name='events_statements_history_long'"

	denied := <-executor.ExecuteQuery(session, query, "")
	require.Error(t, denied.Err)
	require.Contains(t, strings.ToLower(denied.Err.Error()), "lacks select privilege")

	mustExecSQL(t, executor, "", "grant select on performance_schema.setup_consumers to 'p_s_join_reader'@'localhost'")
	deniedSecond := <-executor.ExecuteQuery(session, query, "")
	require.Error(t, deniedSecond.Err)
	require.Contains(t, strings.ToLower(deniedSecond.Err.Error()), "setup_instruments")

	mustExecSQL(t, executor, "", "grant select on performance_schema.setup_instruments to 'p_s_join_reader'@'localhost'")
	allowed := <-executor.ExecuteQuery(session, query, "")
	require.NoError(t, allowed.Err)
}
