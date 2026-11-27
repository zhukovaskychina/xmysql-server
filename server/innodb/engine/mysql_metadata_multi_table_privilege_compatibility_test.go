package engine

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMySQLMetadataMultiTableSelectRequiresEachTablePrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'mysql_join_reader'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "mysql_join_reader")
	session.SetParamByName("host", "localhost")
	query := "select u.User, d.Db from mysql.user u join mysql.db d on 1=1"

	denied := <-executor.ExecuteQuery(session, query, "")
	require.Error(t, denied.Err)
	require.Contains(t, strings.ToLower(denied.Err.Error()), "lacks select privilege")

	mustExecSQL(t, executor, "", "grant select on mysql.user to 'mysql_join_reader'@'localhost'")
	deniedSecond := <-executor.ExecuteQuery(session, query, "")
	require.Error(t, deniedSecond.Err)
	require.Contains(t, strings.ToLower(deniedSecond.Err.Error()), "mysql.db")

	mustExecSQL(t, executor, "", "grant select on mysql.db to 'mysql_join_reader'@'localhost'")
	allowed := <-executor.ExecuteQuery(session, query, "")
	require.NoError(t, allowed.Err)
}
