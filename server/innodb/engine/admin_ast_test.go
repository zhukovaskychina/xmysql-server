package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestAdminASTPrivilegesAndLocks(t *testing.T) {
	parsed, err := sqlparser.ParseForExecution("LOCK TABLES app.a READ, app.b WRITE")
	require.NoError(t, err)
	stmt := parsed.AST.(*sqlparser.AdminStatement)
	require.Len(t, stmt.Targets, 2)
	require.Equal(t, []string{"read", "write"}, stmt.TargetModes)
	ctx := &ExecutionContext{Context: context.Background(), DatabaseName: "app"}
	_, err = (&XMySQLExecutor{}).executeParsedAdmin(ctx, stmt, nil)
	require.Error(t, err)
	killParsed, err := sqlparser.ParseForExecution("KILL QUERY 42")
	require.NoError(t, err)
	killed := uint32(0)
	executor := &XMySQLExecutor{sessionQueryKill: func(id uint32, _ server.MySQLServerSession) error { killed = id; return nil }}
	result, err := executor.executeParsedAdmin(&ExecutionContext{Context: context.Background()}, killParsed.AST.(*sqlparser.AdminStatement), newTestMySQLSession())
	require.NoError(t, err)
	require.Equal(t, uint32(42), killed)
	require.NoError(t, result.Err)
}

func TestExtendedAdminASTSessionIsolation(t *testing.T) {
	first, second := newTestMySQLSession(), newTestMySQLSession()
	sqlPreparedStatementManagerForSession(first).Prepare("p", "select 1")
	if _, ok := sqlPreparedStatementManagerForSession(second).Get("p"); ok {
		t.Fatal("prepared statement leaked across sessions")
	}
	handlerCursors(first)["app.h"] = &handlerCursorState{Database: "app", Table: "t", Alias: "h"}
	if _, ok := handlerCursors(second)["app.h"]; ok {
		t.Fatal("HANDLER alias leaked across sessions")
	}
}

func TestTypedReplicationSourceURLUsesStructuredOptions(t *testing.T) {
	parsed, err := sqlparser.ParseForExecution("CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=3307, SOURCE_USER='repl', SOURCE_PASSWORD='secret', SOURCE_AUTO_POSITION=1")
	require.NoError(t, err)
	stmt := parsed.AST.(*sqlparser.AdminStatement)
	url, err := typedReplicationSourceURL(stmt)
	require.NoError(t, err)
	require.Contains(t, url, "mysql://repl:secret@127.0.0.1:3307")
	require.Contains(t, url, "gtid_auto_position=true")
}

func TestTypedReplicationFilterUsesStructuredOptions(t *testing.T) {
	parsed, err := sqlparser.ParseForExecution("CHANGE REPLICATION FILTER REPLICATE_DO_DB=('app','audit'), REPLICATE_IGNORE_TABLE=('app.secret')")
	require.NoError(t, err)
	stmt := parsed.AST.(*sqlparser.AdminStatement)
	config, err := typedReplicationFilterConfig(stmt)
	require.NoError(t, err)
	require.Equal(t, []string{"app", "audit"}, config.ReplicateDoDB)
	require.Equal(t, []string{"app.secret"}, config.ReplicateIgnoreTable)
}
