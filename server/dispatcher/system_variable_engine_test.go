package dispatcher

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestSystemVariableEngine_InformationSchemaTablesFiltersEqualityPredicates(t *testing.T) {
	useInformationSchemaTablesFixture(t, map[string][]string{
		"app_schema":   {"orders", "archive"},
		"other_schema": {"orders"},
	})

	result := (&SystemVariableEngine{}).executeInformationSchemaTablesQuery(
		"SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = 'app_schema' AND table_name = 'orders'",
	)

	require.NotNil(t, result)
	require.NoError(t, result.Err)
	assert.Equal(t, informationSchemaTablesColumns(), result.Columns)
	assert.Equal(t, [][]interface{}{informationSchemaTableRow("app_schema", "orders")}, result.Rows)
}

func TestSystemVariableEngine_InformationSchemaTablesFiltersLikePredicates(t *testing.T) {
	useInformationSchemaTablesFixture(t, map[string][]string{
		"app_schema":   {"orders", "archive"},
		"other_schema": {"orders"},
	})

	result := (&SystemVariableEngine{}).executeInformationSchemaTablesQuery(
		"SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema LIKE 'app_%' AND table_name LIKE 'ord%'",
	)

	require.NotNil(t, result)
	require.NoError(t, result.Err)
	assert.Equal(t, [][]interface{}{informationSchemaTableRow("app_schema", "orders")}, result.Rows)
}

func TestSystemVariableEngine_InformationSchemaTablesIgnoresNonSelectStatements(t *testing.T) {
	result := (&SystemVariableEngine{}).executeInformationSchemaTablesQuery("DELETE FROM information_schema.tables")

	assert.Nil(t, result)
}

func TestSystemVariableEngine_InformationSchemaTablesUsesConfiguredDataDir(t *testing.T) {
	dataDir := t.TempDir()
	frmPath := filepath.Join(dataDir, "app_schema", "orders.frm")
	require.NoError(t, os.MkdirAll(filepath.Dir(frmPath), 0o755))
	require.NoError(t, os.WriteFile(frmPath, nil, 0o644))

	storageManager := manager.NewStorageManager(&conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        dataDir,
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	})
	t.Cleanup(func() { require.NoError(t, storageManager.Close()) })
	result := (&SystemVariableEngine{storageManager: storageManager}).executeInformationSchemaTablesQuery(
		"SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = 'app_schema' AND table_name = 'orders'",
	)

	require.NotNil(t, result)
	require.NoError(t, result.Err)
	assert.Equal(t, [][]interface{}{informationSchemaTableRow("app_schema", "orders")}, result.Rows)
}

func TestSystemVariableEngine_DoesNotRouteInformationSchemaMetadataQueries(t *testing.T) {
	query := "SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS FROM information_schema.tables WHERE table_schema = 'app_schema'"

	assert.Equal(t, "innodb", NewDefaultSQLRouter().Route(nil, query))
	assert.False(t, (&SystemVariableEngine{}).CanHandle(query))
}

func TestSystemVariableEngine_DoesNotRouteJDBCInformationSchemaProbes(t *testing.T) {
	queries := []string{
		"SELECT ROUTINE_SCHEMA AS PROCEDURE_CAT FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME LIKE '%'",
		"SELECT SPECIFIC_SCHEMA AS PROCEDURE_CAT FROM INFORMATION_SCHEMA.PARAMETERS WHERE SPECIFIC_NAME LIKE '%'",
		"SELECT TABLE_SCHEMA AS TABLE_CAT FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_NAME = 'users'",
		"SELECT A.TABLE_SCHEMA AS FKTABLE_CAT FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE A JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS B USING (TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME)",
		"SELECT R.CONSTRAINT_NAME FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS R",
		"select table_name, view_definition, definer from information_schema.views where table_schema = 'app'",
		"select table_name, partition_name from information_schema.partitions where table_schema = 'app'",
		"select trigger_name, event_manipulation from information_schema.triggers where trigger_schema = 'app'",
		"select event_name, event_definition from information_schema.events where event_schema = 'app'",
		"select collation_name, character_set_name, is_default from information_schema.collations",
		"select grantee, privilege_type, is_grantable from information_schema.user_privileges",
		"select grantee, table_schema, privilege_type, is_grantable from information_schema.schema_privileges",
		"select Host, User, Routine_name, Proc_priv, Routine_type = 'PROCEDURE' as is_proc from mysql.procs_priv where Db = 'app'",
	}

	for _, query := range queries {
		assert.Equal(t, "innodb", NewDefaultSQLRouter().Route(nil, query), query)
		assert.False(t, (&SystemVariableEngine{}).CanHandle(query), query)
	}
}

func TestSystemVariableEngine_DataGripSessionInfoQueryDoesNotFail(t *testing.T) {
	engine := &SystemVariableEngine{
		name:          "system_variable",
		sysVarManager: manager.NewSystemVariablesManager(),
	}
	session := newTestDispatcherSession()
	session.SetParamByName("session_id", "sess-datagrip-info")
	session.SetParamByName("database", "app")
	session.SetParamByName("user", "root")

	result := engine.executeSystemFunctionQuery(
		session,
		"select database(), schema(), left(user(), instr(concat(user(),'@'),'@')-1)",
		"app",
	)

	require.NotNil(t, result)
	require.NoError(t, result.Err)
	require.Equal(t, "select", result.ResultType)
	require.Equal(t, []string{"database()", "schema()", "user"}, result.Columns)
	require.Equal(t, [][]interface{}{{"app", "app", "root"}}, result.Rows)
}

func TestSystemVariableEngine_CurrentRoleReflectsConnectionRoleState(t *testing.T) {
	engine := &SystemVariableEngine{
		name:          "system_variable",
		sysVarManager: manager.NewSystemVariablesManager(),
	}
	session := newTestDispatcherSession()
	session.SetParamByName("active_roles", []string{"report_reader@localhost", "audit_reader@localhost"})

	result := engine.executeSystemFunctionQuery(session, "SELECT CURRENT_ROLE() AS active_role", "app")

	require.NotNil(t, result)
	require.NoError(t, result.Err)
	require.Equal(t, []string{"active_role"}, result.Columns)
	require.Equal(t, [][]interface{}{{"report_reader@localhost,audit_reader@localhost"}}, result.Rows)

	session.SetParamByName("active_roles", []string{})
	result = engine.executeSystemFunctionQuery(session, "SELECT CURRENT_ROLE()", "app")
	require.NotNil(t, result)
	require.Equal(t, [][]interface{}{{"NONE"}}, result.Rows)
}

func TestSystemVariableEngine_LastInsertIDReadsAndSetsSessionValue(t *testing.T) {
	engine := &SystemVariableEngine{
		name:          "system_variable",
		sysVarManager: manager.NewSystemVariablesManager(),
	}
	session := newTestDispatcherSession()
	session.SetParamByName("last_insert_id", uint64(41))

	result := engine.executeSystemFunctionQuery(session, "SELECT LAST_INSERT_ID() AS current_id", "app")
	require.NotNil(t, result)
	require.NoError(t, result.Err)
	assert.Equal(t, []string{"current_id"}, result.Columns)
	assert.Equal(t, [][]interface{}{{uint64(41)}}, result.Rows)

	result = engine.executeSystemFunctionQuery(session, "SELECT LAST_INSERT_ID(99) AS assigned_id", "app")
	require.NotNil(t, result)
	require.NoError(t, result.Err)
	assert.Equal(t, [][]interface{}{{uint64(99)}}, result.Rows)
	assert.Equal(t, uint64(99), session.GetParamByName("last_insert_id"))

	result = engine.executeSystemFunctionQuery(session, "SELECT LAST_INSERT_ID(40 + 2) AS expression_id", "app")
	require.NotNil(t, result)
	require.NoError(t, result.Err)
	assert.Equal(t, [][]interface{}{{uint64(42)}}, result.Rows)
	assert.Equal(t, uint64(42), session.GetParamByName("last_insert_id"))
}

func TestSystemVariableEngine_RowCountReadsSessionValue(t *testing.T) {
	engine := &SystemVariableEngine{
		name:          "system_variable",
		sysVarManager: manager.NewSystemVariablesManager(),
	}
	session := newTestDispatcherSession()
	session.SetParamByName("row_count", int64(3))

	result := engine.executeSystemFunctionQuery(session, "SELECT ROW_COUNT() AS affected", "app")
	require.NotNil(t, result)
	require.NoError(t, result.Err)
	assert.Equal(t, []string{"affected"}, result.Columns)
	assert.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)
}

func informationSchemaTableRow(schemaName, tableName string) []interface{} {
	return []interface{}{schemaName, nil, tableName, "TABLE", "", nil, nil, nil, nil, nil}
}

func useInformationSchemaTablesFixture(t *testing.T, schemas map[string][]string) {
	t.Helper()
	previousDir, err := os.Getwd()
	require.NoError(t, err)
	tempDir := t.TempDir()
	require.NoError(t, os.Chdir(tempDir))
	t.Cleanup(func() {
		require.NoError(t, os.Chdir(previousDir))
	})

	for schema, tables := range schemas {
		for _, table := range tables {
			frmPath := filepath.Join("server", "net", "data", schema, table+".frm")
			require.NoError(t, os.MkdirAll(filepath.Dir(frmPath), 0o755))
			require.NoError(t, os.WriteFile(frmPath, nil, 0o644))
		}
	}
}

type testDispatcherSession struct {
	params map[string]interface{}
	ctx    *server.SessionContext
}

func newTestDispatcherSession() *testDispatcherSession {
	return &testDispatcherSession{
		params: map[string]interface{}{},
		ctx:    server.NewSessionContext("dispatcher-test"),
	}
}

func (s *testDispatcherSession) GetLastActiveTime() time.Time { return time.Now() }
func (s *testDispatcherSession) SendOK()                      {}
func (s *testDispatcherSession) SendHandleOk()                {}
func (s *testDispatcherSession) SendSelectFields()            {}
func (s *testDispatcherSession) SessionContext() *server.SessionContext {
	return s.ctx
}
func (s *testDispatcherSession) GetParamByName(name string) interface{} {
	return s.params[name]
}
func (s *testDispatcherSession) SetParamByName(name string, value interface{}) {
	s.params[name] = value
}

func TestSystemVariableEngine_ExecuteShowStatement_GlobalVariablesUsesGlobalScope(t *testing.T) {
	sysVarMgr := manager.NewSystemVariablesManager()
	engine := &SystemVariableEngine{
		name:          "system_variable",
		sysVarManager: sysVarMgr,
	}

	session := newTestDispatcherSession()
	session.SetParamByName("session_id", "sess-global")
	session.SetParamByName("database", "testdb")
	sysVarMgr.CreateSession("sess-global")
	require.NoError(t, sysVarMgr.SetVariable("sess-global", "autocommit", "OFF", manager.GlobalScope))
	require.NoError(t, sysVarMgr.SetVariable("sess-global", "autocommit", "ON", manager.SessionScope))

	result := engine.executeShowStatement(session, "show global variables like 'autocommit'", "testdb")

	require.NotNil(t, result)
	require.NotEmpty(t, result.Rows)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "autocommit", result.Rows[0][0])
	assert.Equal(t, "OFF", result.Rows[0][1])
}

func TestSystemVariableEngine_SetAutocommitSyncsSessionState(t *testing.T) {
	sysVarMgr := manager.NewSystemVariablesManager()
	engine := &SystemVariableEngine{
		name:          "system_variable",
		sysVarManager: sysVarMgr,
	}
	session := newTestDispatcherSession()
	session.SetParamByName("session_id", "sess-set-autocommit")
	session.SetParamByName("autocommit", "1")

	result := engine.executeSetStatement(session, "set autocommit=0", "testdb")

	require.NotNil(t, result)
	require.NoError(t, result.Err)
	assert.Equal(t, "0", session.GetParamByName("autocommit"))
}

func TestSystemVariableEngine_SetNamesCollationSyncsSessionState(t *testing.T) {
	sysVarMgr := manager.NewSystemVariablesManager()
	engine := &SystemVariableEngine{
		name:          "system_variable",
		sysVarManager: sysVarMgr,
	}
	session := newTestDispatcherSession()
	session.SetParamByName("session_id", "sess-set-names")

	result := engine.executeSetStatement(session, "set names latin1 collate latin1_swedish_ci", "testdb")

	require.NotNil(t, result)
	require.NoError(t, result.Err)
	assert.Equal(t, "latin1", session.GetParamByName("character_set_client"))
	assert.Equal(t, "latin1_swedish_ci", session.GetParamByName("collation_connection"))
}

func TestSystemVariableEngine_SuperReadOnlySynchronizesGlobalState(t *testing.T) {
	sysVarMgr := manager.NewSystemVariablesManager()
	engine := &SystemVariableEngine{
		name:          "system_variable",
		sysVarManager: sysVarMgr,
	}
	session := newTestDispatcherSession()
	session.SetParamByName("session_id", "sess-super-read-only")
	session.SetParamByName("global_privileges", []common.PrivilegeType{common.SuperPriv})
	sysVarMgr.CreateSession("sess-super-read-only")

	result := engine.executeSetStatement(session, "set global super_read_only = on", "testdb")
	require.NotNil(t, result)
	require.NoError(t, result.Err)
	superReadOnly, err := sysVarMgr.GetVariable("", "super_read_only", manager.GlobalScope)
	require.NoError(t, err)
	assert.Equal(t, "1", fmt.Sprint(superReadOnly))
	readOnly, err := sysVarMgr.GetVariable("", "read_only", manager.GlobalScope)
	require.NoError(t, err)
	assert.Equal(t, "ON", fmt.Sprint(readOnly))

	result = engine.executeSetStatement(session, "set global read_only = off", "testdb")
	require.NotNil(t, result)
	require.NoError(t, result.Err)
	superReadOnly, err = sysVarMgr.GetVariable("", "super_read_only", manager.GlobalScope)
	require.NoError(t, err)
	assert.Equal(t, "OFF", strings.ToUpper(fmt.Sprint(superReadOnly)))
	readOnly, err = sysVarMgr.GetVariable("", "read_only", manager.GlobalScope)
	require.NoError(t, err)
	assert.Equal(t, "OFF", strings.ToUpper(fmt.Sprint(readOnly)))
}

func TestSystemVariableEngine_SetGlobalTransactionAliasesStaySynchronized(t *testing.T) {
	sysVarMgr := manager.NewSystemVariablesManager()
	engine := &SystemVariableEngine{
		name:          "system_variable",
		sysVarManager: sysVarMgr,
	}
	session := newTestDispatcherSession()
	session.SetParamByName("session_id", "sess-global-tx")
	session.SetParamByName("global_privileges", []common.PrivilegeType{common.SuperPriv})
	sysVarMgr.CreateSession("sess-global-tx")
	result := engine.executeSetStatement(session, "set global transaction isolation level serializable", "testdb")
	require.NotNil(t, result)
	require.NoError(t, result.Err)

	for _, name := range []string{"transaction_isolation", "tx_isolation"} {
		value, err := sysVarMgr.GetVariable("", name, manager.GlobalScope)
		require.NoError(t, err)
		assert.Equal(t, "SERIALIZABLE", fmt.Sprint(value), name)
	}

	result = engine.executeSetStatement(session, "set global transaction read only", "testdb")
	require.NotNil(t, result)
	require.NoError(t, result.Err)
	for _, name := range []string{"transaction_read_only", "tx_read_only"} {
		value, err := sysVarMgr.GetVariable("", name, manager.GlobalScope)
		require.NoError(t, err)
		assert.Equal(t, "1", fmt.Sprint(value), name)
	}
}

func TestSystemVariableEngine_ExecuteShowStatement_SessionVariablesUsesSessionScope(t *testing.T) {
	sysVarMgr := manager.NewSystemVariablesManager()
	engine := &SystemVariableEngine{
		name:          "system_variable",
		sysVarManager: sysVarMgr,
	}

	session := newTestDispatcherSession()
	session.SetParamByName("session_id", "sess-session")
	session.SetParamByName("database", "testdb")
	sysVarMgr.CreateSession("sess-session")
	require.NoError(t, sysVarMgr.SetVariable("sess-session", "autocommit", "OFF", manager.GlobalScope))
	require.NoError(t, sysVarMgr.SetVariable("sess-session", "autocommit", "ON", manager.SessionScope))

	result := engine.executeShowStatement(session, "show session variables like 'autocommit'", "testdb")

	require.NotNil(t, result)
	require.NotEmpty(t, result.Rows)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "autocommit", result.Rows[0][0])
	assert.Equal(t, "ON", result.Rows[0][1])
}

func TestSystemVariableEngine_ExecuteShowStatement_StatusScopeAndLike(t *testing.T) {
	sysVarMgr := manager.NewSystemVariablesManager()
	engine := &SystemVariableEngine{
		name:          "system_variable",
		sysVarManager: sysVarMgr,
	}

	session := newTestDispatcherSession()
	session.SetParamByName("session_id", "sess-status")
	session.SetParamByName("database", "testdb")
	globalResult := engine.executeShowStatement(session, "show global status like 'Up%'", "testdb")
	require.NotNil(t, globalResult)
	require.Len(t, globalResult.Rows, 1)
	assert.Equal(t, "Uptime", globalResult.Rows[0][0])

	sessionResult := engine.executeShowStatement(session, "show session status like 'Com_%'", "testdb")
	require.NotNil(t, sessionResult)
	require.Len(t, sessionResult.Rows, 2)
	assert.ElementsMatch(t, []string{"Com_insert", "Com_select"}, []string{
		sessionResult.Rows[0][0].(string),
		sessionResult.Rows[1][0].(string),
	})
}

func TestExtractShowVariablesLikePattern(t *testing.T) {
	assert.Equal(t, "version%", engine.ExtractShowVariablesLikePatternFromQuery("show variables like 'version%'"))
	assert.Equal(t, "char%", engine.ExtractShowVariablesLikePatternFromQuery("show global variables like 'char%'"))
	assert.Equal(t, "", engine.ExtractShowVariablesLikePatternFromQuery("show variables"))
}

func TestExtractShowStatusLikePattern(t *testing.T) {
	assert.Equal(t, "Up%", engine.ExtractShowStatusLikePatternFromQuery("show status like 'Up%'"))
	assert.Equal(t, "Com_%", engine.ExtractShowStatusLikePatternFromQuery("show session status like 'Com_%'"))
	assert.Equal(t, "", engine.ExtractShowStatusLikePatternFromQuery("show status"))
}

func TestSystemVariableEngine_ShowParserCarriesLikeFilter(t *testing.T) {
	cases := []struct {
		query        string
		expectedLike string
	}{
		{query: "show variables like 'version%'", expectedLike: "version%"},
		{query: "show global status like 'Up%'", expectedLike: "Up%"},
		{query: "show databases like 'app%'", expectedLike: "app%"},
	}
	for _, tc := range cases {
		stmtRaw, err := sqlparser.Parse(tc.query)
		require.NoError(t, err)
		showStmt, ok := stmtRaw.(*sqlparser.Show)
		require.True(t, ok)
		require.NotNil(t, showStmt.Filter)
		assert.Equal(t, tc.expectedLike, showStmt.Filter.Like)
	}
}

func TestSystemVariableEngine_ExecuteShowStatement_VariablesWhereFiltersRows(t *testing.T) {
	sysVarMgr := manager.NewSystemVariablesManager()
	engine := &SystemVariableEngine{
		name:          "system_variable",
		sysVarManager: sysVarMgr,
	}

	session := newTestDispatcherSession()
	session.SetParamByName("session_id", "sess-where-var")
	session.SetParamByName("database", "testdb")
	sysVarMgr.CreateSession("sess-where-var")
	require.NoError(t, sysVarMgr.SetVariable("sess-where-var", "autocommit", "OFF", manager.GlobalScope))

	result := engine.executeShowStatement(session, "show global variables where Variable_name = 'autocommit'", "testdb")

	require.NotNil(t, result)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "autocommit", result.Rows[0][0])
	assert.Equal(t, "OFF", result.Rows[0][1])
}

func TestSystemVariableEngine_ExecuteShowStatement_StatusWhereFiltersRows(t *testing.T) {
	sysVarMgr := manager.NewSystemVariablesManager()
	engine := &SystemVariableEngine{
		name:          "system_variable",
		sysVarManager: sysVarMgr,
	}

	session := newTestDispatcherSession()
	session.SetParamByName("session_id", "sess-where-status")
	session.SetParamByName("database", "testdb")

	result := engine.executeShowStatement(session, "show global status where Variable_name = 'Uptime'", "testdb")

	require.NotNil(t, result)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "Uptime", result.Rows[0][0])
}

func TestSystemVariableEngine_ExecuteShowStatement_StatusWhereNotExprFiltersRows(t *testing.T) {
	sysVarMgr := manager.NewSystemVariablesManager()
	engine := &SystemVariableEngine{
		name:          "system_variable",
		sysVarManager: sysVarMgr,
	}

	session := newTestDispatcherSession()
	session.SetParamByName("session_id", "sess-where-not")
	session.SetParamByName("database", "testdb")

	result := engine.executeShowStatement(session, "show global status where not (Variable_name = 'Uptime')", "testdb")

	require.NotNil(t, result)
	require.NotEmpty(t, result.Rows)
	for _, row := range result.Rows {
		assert.NotEqual(t, "Uptime", row[0])
	}
}

func TestSystemVariableEngine_ExecuteShowStatement_VariablesWhereIsNotNullFiltersRows(t *testing.T) {
	sysVarMgr := manager.NewSystemVariablesManager()
	engine := &SystemVariableEngine{
		name:          "system_variable",
		sysVarManager: sysVarMgr,
	}

	session := newTestDispatcherSession()
	session.SetParamByName("session_id", "sess-where-is-not-null")
	session.SetParamByName("database", "testdb")

	result := engine.executeShowStatement(session, "show global variables where Value is not null", "testdb")

	require.NotNil(t, result)
	require.NotEmpty(t, result.Rows)
}

func TestSystemVariableEngine_ExecuteShowStatement_StatusWhereIsTrueIsFalse(t *testing.T) {
	sysVarMgr := manager.NewSystemVariablesManager()
	engine := &SystemVariableEngine{
		name:          "system_variable",
		sysVarManager: sysVarMgr,
	}

	session := newTestDispatcherSession()
	session.SetParamByName("session_id", "sess-where-is-bool")
	session.SetParamByName("database", "testdb")

	trueResult := engine.executeShowStatement(session, "show global status where 1 is true", "testdb")
	require.NotNil(t, trueResult)
	require.NotEmpty(t, trueResult.Rows)

	falseResult := engine.executeShowStatement(session, "show global status where 1 is false", "testdb")
	require.NotNil(t, falseResult)
	require.Len(t, falseResult.Rows, 0)
}

func TestSystemVariableEngine_ExecuteShowStatement_StatusWhereIsNotTrueIsNotFalse(t *testing.T) {
	sysVarMgr := manager.NewSystemVariablesManager()
	engine := &SystemVariableEngine{
		name:          "system_variable",
		sysVarManager: sysVarMgr,
	}

	session := newTestDispatcherSession()
	session.SetParamByName("session_id", "sess-where-is-not-bool")
	session.SetParamByName("database", "testdb")

	notTrueResult := engine.executeShowStatement(session, "show global status where 1 is not true", "testdb")
	require.NotNil(t, notTrueResult)
	require.Len(t, notTrueResult.Rows, 0)

	notFalseResult := engine.executeShowStatement(session, "show global status where 1 is not false", "testdb")
	require.NotNil(t, notFalseResult)
	require.NotEmpty(t, notFalseResult.Rows)
}
