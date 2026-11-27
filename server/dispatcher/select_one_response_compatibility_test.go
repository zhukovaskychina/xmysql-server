package dispatcher

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
)

type selectOneDispatchEngine struct{}

func (selectOneDispatchEngine) Name() string { return "innodb" }

func (selectOneDispatchEngine) CanHandle(string) bool { return true }

func (selectOneDispatchEngine) ExecuteQuery(server.MySQLServerSession, string, string) <-chan *SQLResult {
	results := make(chan *SQLResult, 1)
	results <- &SQLResult{
		ResultType:  "SELECT",
		Columns:     []string{"first_value"},
		ColumnTypes: []string{"BIGINT"},
		Rows:        [][]interface{}{{int64(7)}},
		Message:     "Query OK, 1 row in set",
	}
	close(results)
	return results
}

func newSelectOneDispatchHandler() *EnhancedBusinessMessageHandler {
	dispatcher := &SQLDispatcher{
		engines: map[string]SQLEngine{"innodb": selectOneDispatchEngine{}},
		router:  NewDefaultSQLRouter(),
	}
	return &EnhancedBusinessMessageHandler{
		sqlDispatcher: dispatcher,
		authService:   denyAuthService{},
	}
}

func TestHandleQueryWithRealSessionUsesDispatcherForSelectOne(t *testing.T) {
	handler := newSelectOneDispatchHandler()
	session := NewEnhancedMockMySQLServerSession("client", "")

	response, err := handler.HandleQueryWithRealSession(session, "SELECT 1 AS first_value", "")
	require.NoError(t, err)
	result, ok := response.(*protocol.ResponseMessage)
	require.True(t, ok)
	require.Equal(t, []string{"first_value"}, result.Result.Columns)
	require.Equal(t, [][]interface{}{{int64(7)}}, result.Result.Rows)
}

func TestHandleMessageUsesDispatcherForSelectOne(t *testing.T) {
	handler := newSelectOneDispatchHandler()
	msg := &protocol.QueryMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, "client", nil),
		SQL:         "SELECT 1 AS first_value",
	}

	response, err := handler.HandleMessage(msg)
	require.NoError(t, err)
	result, ok := response.(*protocol.ResponseMessage)
	require.True(t, ok)
	require.Equal(t, []string{"first_value"}, result.Result.Columns)
	require.Equal(t, [][]interface{}{{int64(7)}}, result.Result.Rows)
}
