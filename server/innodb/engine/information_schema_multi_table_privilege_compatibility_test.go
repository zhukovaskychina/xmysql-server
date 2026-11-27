package engine

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/common"
)

func TestInformationSchemaMultiTableSelectRequiresProcessForEachProcessTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'i_s_join_reader'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "i_s_join_reader")
	session.SetParamByName("host", "localhost")
	query := "select t.table_name, ts.name from information_schema.tables t join information_schema.tablespaces ts on 1=1"

	denied := <-executor.ExecuteQuery(session, query, "")
	require.Error(t, denied.Err)
	require.Contains(t, strings.ToLower(denied.Err.Error()), "process")

	mustExecSQL(t, executor, "", "grant process on *.* to 'i_s_join_reader'@'localhost'")
	session.SetParamByName("global_privileges", []common.PrivilegeType{common.ProcessPriv})
	allowed := <-executor.ExecuteQuery(session, query, "")
	require.NoError(t, allowed.Err)
}
