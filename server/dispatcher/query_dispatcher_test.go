package dispatcher

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
)

func TestPersistSessionLastInsertIDFromDMLResult(t *testing.T) {
	session := newTestDispatcherSession()
	result := &engine.Result{
		ResultType: common.RESULT_TYPE_QUERY,
		Data: &engine.DMLResult{
			LastInsertId: 42,
		},
	}

	persistSessionLastInsertID(session, result)

	require.Equal(t, uint64(42), session.GetParamByName("last_insert_id"))
}

func TestPersistSessionLastInsertIDDoesNotClearPreviousValueForZeroDMLID(t *testing.T) {
	session := newTestDispatcherSession()
	session.SetParamByName("last_insert_id", uint64(42))
	result := &engine.Result{
		ResultType: common.RESULT_TYPE_QUERY,
		Data: &engine.DMLResult{
			LastInsertId: 0,
		},
	}

	persistSessionLastInsertID(session, result)

	require.Equal(t, uint64(42), session.GetParamByName("last_insert_id"))
}

func TestPersistSessionExecutionStateFromDMLResult(t *testing.T) {
	session := newTestDispatcherSession()
	result := &engine.Result{
		ResultType: common.RESULT_TYPE_QUERY,
		Data: &engine.DMLResult{
			AffectedRows: 3,
			LastInsertId: 42,
		},
	}

	persistSessionExecutionState(session, result)

	require.Equal(t, int64(3), session.GetParamByName("row_count"))
	require.Equal(t, uint64(42), session.GetParamByName("last_insert_id"))
}

func TestPersistSessionExecutionStateUsesMySQLSelectRowCount(t *testing.T) {
	session := newTestDispatcherSession()
	persistSessionExecutionState(session, &engine.Result{ResultType: common.RESULT_TYPE_SELECT})

	require.Equal(t, int64(-1), session.GetParamByName("row_count"))
}

func TestDefaultSQLRouterRoutesLastInsertIDToSystemVariableEngine(t *testing.T) {
	require.Equal(t, "system_variable", NewDefaultSQLRouter().Route(nil, "SELECT LAST_INSERT_ID()"))
	require.Equal(t, "system_variable", NewDefaultSQLRouter().Route(nil, "SELECT LAST_INSERT_ID(99)"))
}
