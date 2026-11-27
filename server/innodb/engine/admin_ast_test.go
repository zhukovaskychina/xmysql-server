package engine

import (
	"context"
	"net/url"
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

func TestReplicationASTPreservesLiteralBoundaries(t *testing.T) {
	config, err := parseReplicationFilter("CHANGE REPLICATION FILTER REPLICATE_DO_DB=('a,b', 'a''b', ')'), REPLICATE_REWRITE_DB=(( 'old,db', 'new)db'), (source_db,target_db)) FOR CHANNEL 'analytics'")
	require.NoError(t, err)
	require.Equal(t, []string{"a,b", "a'b", ")"}, config.ReplicateDoDB)
	require.Len(t, config.ReplicateRewriteDB, 2)
	require.Equal(t, "old,db", config.ReplicateRewriteDB[0].From)
	require.Equal(t, "new)db", config.ReplicateRewriteDB[0].To)
	filter, err := sqlparser.ParseForExecution("CHANGE REPLICATION FILTER REPLICATE_DO_DB=('a,b', 'a''b', ')')")
	require.NoError(t, err)
	config, err = typedReplicationFilterConfig(filter.AST.(*sqlparser.AdminStatement))
	require.NoError(t, err)
	require.Equal(t, []string{"a,b", "a'b", ")"}, config.ReplicateDoDB)
	parsed, err := sqlparser.ParseForExecution("CHANGE REPLICATION SOURCE TO SOURCE_HOST='localhost', SOURCE_USER='repl', SOURCE_PASSWORD='a,b''c FOR CHANNEL ''fake''' FOR CHANNEL 'real'")
	require.NoError(t, err)
	stmt := parsed.AST.(*sqlparser.AdminStatement)
	require.Equal(t, "real", stmt.Channel)
	source, err := typedReplicationSourceURL(stmt)
	require.NoError(t, err)
	address, err := url.Parse(source)
	require.NoError(t, err)
	password, present := address.User.Password()
	require.True(t, present)
	require.Equal(t, "a,b'c FOR CHANNEL 'fake'", password)
}

func TestReplicationASTRejectsMalformedOptions(t *testing.T) {
	for _, query := range []string{
		"CHANGE REPLICATION SOURCE TO SOURCE_HOST='localhost' SOURCE_USER='repl'",
		"CHANGE REPLICATION SOURCE TO SOURCE_HOST='localhost',",
		"CHANGE REPLICATION SOURCE TO SOURCE_HOST='localhost' FOR CHANNEL 'a' FOR CHANNEL 'b'",
		"CHANGE REPLICATION FILTER REPLICATE_DO_DB=('app') garbage",
		"CHANGE REPLICATION FILTER",
	} {
		_, err := sqlparser.ParseForExecution(query)
		require.Error(t, err, query)
	}
}
