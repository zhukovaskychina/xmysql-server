package engine

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
)

func TestTypeFunctionCompatibilityMatrix(t *testing.T) {
	columns := []*metadata.Column{
		{Name: "amount", DataType: metadata.TypeDecimal, IsNullable: true},
		{Name: "created", DataType: metadata.TypeDateTime, IsNullable: true},
		{Name: "status", DataType: metadata.TypeEnum, IsNullable: false},
		{Name: "tags", DataType: metadata.TypeSet, IsNullable: true},
		{Name: "payload", DataType: metadata.TypeJSON, IsNullable: true},
		{Name: "raw", DataType: metadata.TypeVarBinary, CharMaxLength: 32, IsNullable: true},
	}
	for _, column := range columns {
		require.NoError(t, column.Validate(), column.Name)
	}

	cases := []struct {
		name string
		expr plan.Expression
		want interface{}
	}{
		{"coalesce null", &plan.Function{FuncName: "COALESCE", FuncArgs: []plan.Expression{&plan.Constant{Value: nil}, &plan.Constant{Value: "ok"}}}, "ok"},
		{"lower utf8", &plan.Function{FuncName: "LOWER", FuncArgs: []plan.Expression{&plan.Constant{Value: "ÄÖ"}}}, "äö"},
		{"round decimal", &plan.Function{FuncName: "ROUND", FuncArgs: []plan.Expression{&plan.Constant{Value: 1.25}, &plan.Constant{Value: int64(1)}}}, 1.3},
		{"bit count positive", &plan.Function{FuncName: "BIT_COUNT", FuncArgs: []plan.Expression{&plan.Constant{Value: int64(7)}}}, int64(3)},
		{"bit count negative", &plan.Function{FuncName: "BIT_COUNT", FuncArgs: []plan.Expression{&plan.Constant{Value: int64(-1)}}}, int64(64)},
		{"json extract", &plan.Function{FuncName: "JSON_EXTRACT", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"a":1}`}, &plan.Constant{Value: "$.a"}}}, float64(1)},
		{"json extract array", &plan.Function{FuncName: "JSON_EXTRACT", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"items":[1,2]}`}, &plan.Constant{Value: "$.items[1]"}}}, float64(2)},
		{"json extract multiple paths", &plan.Function{FuncName: "JSON_EXTRACT", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"a":1,"b":2}`}, &plan.Constant{Value: "$.a"}, &plan.Constant{Value: "$.b"}}}, `[1,2]`},
		{"json extract array wildcard", &plan.Function{FuncName: "JSON_EXTRACT", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"items":[1,2]}`}, &plan.Constant{Value: "$.items[*]"}}}, `[1,2]`},
		{"json extract object wildcard", &plan.Function{FuncName: "JSON_EXTRACT", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"obj":{"b":2,"a":1}}`}, &plan.Constant{Value: "$.obj.*"}}}, `[1,2]`},
		{"json extract nested wildcard", &plan.Function{FuncName: "JSON_EXTRACT", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"items":[{"id":10},{"id":20}]}`}, &plan.Constant{Value: "$.items[*].id"}}}, `[10,20]`},
		{"json extract array range", &plan.Function{FuncName: "JSON_EXTRACT", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"items":[10,20,30,40]}`}, &plan.Constant{Value: "$.items[1 to 2]"}}}, `[20,30]`},
		{"json extract last index", &plan.Function{FuncName: "JSON_EXTRACT", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"items":[10,20,30]}`}, &plan.Constant{Value: "$.items[last]"}}}, float64(30)},
		{"json extract last minus index", &plan.Function{FuncName: "JSON_EXTRACT", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"items":[10,20,30]}`}, &plan.Constant{Value: "$.items[last-1]"}}}, float64(20)},
		{"json extract recursive descent", &plan.Function{FuncName: "JSON_EXTRACT", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"id":1,"nested":{"id":2,"deep":{"id":3}}}`}, &plan.Constant{Value: "$**.id"}}}, `[1,2,3]`},
		{"greatest", &plan.Function{FuncName: "GREATEST", FuncArgs: []plan.Expression{&plan.Constant{Value: int64(2)}, &plan.Constant{Value: int64(7)}}}, int64(7)},
		{"date format", &plan.Function{FuncName: "DATE_FORMAT", FuncArgs: []plan.Expression{&plan.Constant{Value: "2024-03-05 14:06:07"}, &plan.Constant{Value: "%Y-%m-%d %H:%i:%s"}}}, "2024-03-05 14:06:07"},
		{"date add", &plan.Function{FuncName: "DATE_ADD", FuncArgs: []plan.Expression{&plan.Constant{Value: "2024-03-05 14:06:07"}, &plan.Constant{Value: "1 MONTH"}}}, "2024-04-05 14:06:07"},
		{"date diff", &plan.Function{FuncName: "DATEDIFF", FuncArgs: []plan.Expression{&plan.Constant{Value: "2024-03-05"}, &plan.Constant{Value: "2024-02-05"}}}, int64(29)},
		{"timestamp diff", &plan.Function{FuncName: "TIMESTAMPDIFF", FuncArgs: []plan.Expression{&plan.Constant{Value: "DAY"}, &plan.Constant{Value: "2024-02-05"}, &plan.Constant{Value: "2024-03-05"}}}, int64(29)},
		{"timestamp add", &plan.Function{FuncName: "TIMESTAMPADD", FuncArgs: []plan.Expression{&plan.Constant{Value: "MONTH"}, &plan.Constant{Value: int64(1)}, &plan.Constant{Value: "2024-03-05 14:06:07"}}}, "2024-04-05 14:06:07"},
		{"cast signed", &plan.Function{FuncName: "CAST", FuncArgs: []plan.Expression{&plan.Constant{Value: "42"}, &plan.Constant{Value: "SIGNED"}}}, int64(42)},
		{"cast signed decimal", &plan.Function{FuncName: "CAST", FuncArgs: []plan.Expression{&plan.Constant{Value: "42.9"}, &plan.Constant{Value: "SIGNED"}}}, int64(42)},
		{"cast unsigned decimal", &plan.Function{FuncName: "CAST", FuncArgs: []plan.Expression{&plan.Constant{Value: "42.9"}, &plan.Constant{Value: "UNSIGNED"}}}, uint64(42)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.expr.Eval(&plan.EvalContext{Row: map[string]interface{}{}})
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestUUIDShortThroughEnginePersistsAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()
	newConfig := func() *conf.Cfg {
		return &conf.Cfg{
			DataDir:              dataDir,
			InnodbDataDir:        dataDir,
			InnodbBufferPoolSize: 16 * 1024 * 1024,
			InnodbPageSize:       16384,
			ReplicationServerID:  37,
		}
	}
	parse := func(value interface{}) uint64 {
		parsed, err := strconv.ParseUint(value.(string), 10, 64)
		require.NoError(t, err)
		return parsed
	}

	firstEngine := NewXMySQLEngine(newConfig())
	firstRows := mustQuerySQL(t, firstEngine, "", "select uuid_short(), uuid_short()")
	firstEngine.Close()
	firstValue := parse(firstRows[0][0])
	secondValue := parse(firstRows[0][1])
	require.Equal(t, uint64(1), secondValue-firstValue)

	secondEngine := NewXMySQLEngine(newConfig())
	secondRows := mustQuerySQL(t, secondEngine, "", "select uuid_short()")
	require.NoError(t, secondEngine.Close())
	require.Greater(t, parse(secondRows[0][0]), secondValue)
}

func TestAdditionalStringAndJSONFunctions(t *testing.T) {
	cases := []struct {
		name string
		expr plan.Expression
		want interface{}
	}{
		{"concat ws", &plan.Function{FuncName: "CONCAT_WS", FuncArgs: []plan.Expression{&plan.Constant{Value: "-"}, &plan.Constant{Value: "a"}, &plan.Constant{Value: nil}, &plan.Constant{Value: "b"}}}, "a-b"},
		{"substring index", &plan.Function{FuncName: "SUBSTRING_INDEX", FuncArgs: []plan.Expression{&plan.Constant{Value: "a,b,c"}, &plan.Constant{Value: ","}, &plan.Constant{Value: int64(2)}}}, "a,b"},
		{"json array", &plan.Function{FuncName: "JSON_ARRAY", FuncArgs: []plan.Expression{&plan.Constant{Value: int64(1)}, &plan.Constant{Value: "x"}}}, `[1,"x"]`},
		{"json array aggregate", &plan.Function{FuncName: "JSON_ARRAYAGG", FuncArgs: []plan.Expression{&plan.Constant{Value: []interface{}{int64(1), nil, "x"}}}}, `[1,null,"x"]`},
		{"json object aggregate", &plan.Function{FuncName: "JSON_OBJECTAGG", FuncArgs: []plan.Expression{&plan.Constant{Value: []interface{}{"a", "b"}}, &plan.Constant{Value: []interface{}{int64(1), int64(2)}}}}, `{"a":1,"b":2}`},
		{"any value aggregate", &plan.Function{FuncName: "ANY_VALUE", FuncArgs: []plan.Expression{&plan.Constant{Value: []interface{}{"first", "second"}}}}, "first"},
		{"json object", &plan.Function{FuncName: "JSON_OBJECT", FuncArgs: []plan.Expression{&plan.Constant{Value: "a"}, &plan.Constant{Value: int64(1)}}}, `{"a":1}`},
		{"json merge preserve duplicate key", &plan.Function{FuncName: "JSON_MERGE_PRESERVE", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"a":1}`}, &plan.Constant{Value: `{"a":2}`}}}, `{"a":[1,2]}`},
		{"json merge preserve arrays", &plan.Function{FuncName: "JSON_MERGE_PRESERVE", FuncArgs: []plan.Expression{&plan.Constant{Value: `[1]`}, &plan.Constant{Value: `[2,3]`}}}, `[1,2,3]`},
		{"json merge deprecated alias", &plan.Function{FuncName: "JSON_MERGE", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"a":1}`}, &plan.Constant{Value: `{"a":2}`}}}, `{"a":[1,2]}`},
		{"json contains", &plan.Function{FuncName: "JSON_CONTAINS", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"a":1,"b":2}`}, &plan.Constant{Value: `{"a":1}`}}}, int64(1)},
		{"regexp replace", &plan.Function{FuncName: "REGEXP_REPLACE", FuncArgs: []plan.Expression{&plan.Constant{Value: "a1a2"}, &plan.Constant{Value: "a"}, &plan.Constant{Value: "x"}}}, "x1x2"},
		{"regexp substr", &plan.Function{FuncName: "REGEXP_SUBSTR", FuncArgs: []plan.Expression{&plan.Constant{Value: "a1a2"}, &plan.Constant{Value: "a[0-9]"}}}, "a1"},
		{"regexp substr occurrence", &plan.Function{FuncName: "REGEXP_SUBSTR", FuncArgs: []plan.Expression{&plan.Constant{Value: "a1a2"}, &plan.Constant{Value: "a[0-9]"}, &plan.Constant{Value: int64(1)}, &plan.Constant{Value: int64(2)}}}, "a2"},
		{"regexp replace occurrence", &plan.Function{FuncName: "REGEXP_REPLACE", FuncArgs: []plan.Expression{&plan.Constant{Value: "a1a2"}, &plan.Constant{Value: "a"}, &plan.Constant{Value: "x"}, &plan.Constant{Value: int64(2)}, &plan.Constant{Value: int64(1)}}}, "a1x2"},
		{"regexp like match type", &plan.Function{FuncName: "REGEXP_LIKE", FuncArgs: []plan.Expression{&plan.Constant{Value: "Abc"}, &plan.Constant{Value: "^a"}, &plan.Constant{Value: "i"}}}, true},
		{"json set", &plan.Function{FuncName: "JSON_SET", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"a":1}`}, &plan.Constant{Value: "$.a"}, &plan.Constant{Value: int64(2)}}}, `{"a":2}`},
		{"json set nested", &plan.Function{FuncName: "JSON_SET", FuncArgs: []plan.Expression{&plan.Constant{Value: `{}`}, &plan.Constant{Value: "$.a.b"}, &plan.Constant{Value: int64(2)}}}, `{"a":{"b":2}}`},
		{"json replace", &plan.Function{FuncName: "JSON_REPLACE", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"a":1}`}, &plan.Constant{Value: "$.a"}, &plan.Constant{Value: int64(3)}}}, `{"a":3}`},
		{"json remove", &plan.Function{FuncName: "JSON_REMOVE", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"a":1,"b":2}`}, &plan.Constant{Value: "$.b"}}}, `{"a":1}`},
		{"json array append", &plan.Function{FuncName: "JSON_ARRAY_APPEND", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"items":[1]}`}, &plan.Constant{Value: "$.items"}, &plan.Constant{Value: int64(2)}}}, `{"items":[1,2]}`},
		{"json array insert", &plan.Function{FuncName: "JSON_ARRAY_INSERT", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"items":[1,3]}`}, &plan.Constant{Value: "$.items[1]"}, &plan.Constant{Value: int64(2)}}}, `{"items":[1,2,3]}`},
		{"json search", &plan.Function{FuncName: "JSON_SEARCH", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"a":"hello","b":"world"}`}, &plan.Constant{Value: "one"}, &plan.Constant{Value: "%ell%"}}}, "$.a"},
		{"json insert", &plan.Function{FuncName: "JSON_INSERT", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"a":1}`}, &plan.Constant{Value: "$.a"}, &plan.Constant{Value: int64(2)}, &plan.Constant{Value: "$.b"}, &plan.Constant{Value: int64(3)}}}, `{"a":1,"b":3}`},
		{"json array path set", &plan.Function{FuncName: "JSON_SET", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"items":[1,2]}`}, &plan.Constant{Value: "$.items[1]"}, &plan.Constant{Value: int64(3)}}}, `{"items":[1,3]}`},
		{"json array path remove", &plan.Function{FuncName: "JSON_REMOVE", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"items":[1,2]}`}, &plan.Constant{Value: "$.items[0]"}}}, `{"items":[2]}`},
		{"json array path length", &plan.Function{FuncName: "JSON_LENGTH", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"items":[1,2]}`}, &plan.Constant{Value: "$.items"}}}, int64(2)},
		{"json length", &plan.Function{FuncName: "JSON_LENGTH", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"a":1,"b":2}`}}}, int64(2)},
		{"json type", &plan.Function{FuncName: "JSON_TYPE", FuncArgs: []plan.Expression{&plan.Constant{Value: `{"a":1}`}}}, "OBJECT"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.expr.Eval(&plan.EvalContext{Row: map[string]interface{}{}})
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestIntervalFunctionThroughStorageSelect(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	rows := mustQuerySQL(t, executor, "app", "select interval(5, 10, 20, 30), interval(20, 10, 20, 30), interval(25, 10, 20, 30), interval(35, 10, 20, 30), interval(null, 10, 20)")
	require.Equal(t, [][]interface{}{{"0", "2", "2", "3", nil}}, rows)
}

func TestLastInsertIDThroughSessionSQL(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "create table users (id int primary key auto_increment, name varchar(20))")
	mustExecSessionSQL(t, executor, session, "app", "insert into users (name) values ('alice')")

	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySessionSQL(t, executor, session, "app", "select last_insert_id()"))
	require.Equal(t, [][]interface{}{{"42"}}, mustQuerySessionSQL(t, executor, session, "app", "select last_insert_id(42)"))
	require.Equal(t, [][]interface{}{{"42"}}, mustQuerySessionSQL(t, executor, session, "app", "select last_insert_id()"))
	require.Equal(t, [][]interface{}{{"77"}}, mustQuerySessionSQL(t, executor, session, "app", "select last_insert_id(77) from users"))
	require.Equal(t, [][]interface{}{{"77"}}, mustQuerySessionSQL(t, executor, session, "app", "select last_insert_id()"))

	mustExecSessionSQL(t, executor, session, "app", "insert into users (name) values ('bob')")
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySessionSQL(t, executor, session, "app", "select last_insert_id()"))
	result := <-executor.ExecuteQuery(session, "insert into users (name) values ('carol'), ('dave')", "app")
	require.NoError(t, result.Err)
	require.Equal(t, uint64(3), result.LastInsertID)
	require.Equal(t, [][]interface{}{{"3"}}, mustQuerySessionSQL(t, executor, session, "app", "select last_insert_id()"))
}

func TestSessionMetadataFunctionsThroughSessionSQL(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	session := newTestMySQLSession()
	session.SetParamByName("database", "app")
	mustExecSessionSQL(t, executor, session, "app", "create table session_metadata_rows (id int primary key)")
	mustExecSessionSQL(t, executor, session, "app", "insert into session_metadata_rows values (1)")
	session.SetParamByName("user", "alice")
	session.SetParamByName("host", "127.0.0.1")
	session.SetParamByName("active_roles", []string{"app_reader", "app_writer"})
	session.SetParamByName("connection_id", int64(42))

	rows := mustQuerySessionSQL(t, executor, session, "app", "select database(), schema(), user(), current_user(), session_user(), system_user(), current_role(), version(), connection_id()")
	require.Equal(t, [][]interface{}{{"app", "app", "alice@127.0.0.1", "alice@127.0.0.1", "alice@127.0.0.1", "alice@127.0.0.1", "app_reader,app_writer", "8.0.32", "42"}}, rows)
	rows = mustQuerySessionSQL(t, executor, session, "app", "select database(), schema(), user(), current_user(), session_user(), system_user(), current_role(), version(), connection_id() from session_metadata_rows")
	require.Equal(t, [][]interface{}{{"app", "app", "alice@127.0.0.1", "alice@127.0.0.1", "alice@127.0.0.1", "alice@127.0.0.1", "app_reader,app_writer", "8.0.32", "42"}}, rows)
}

func TestSessionMetadataFunctionsPropagateThroughDerivedProjection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	session := newTestMySQLSession()
	session.SetParamByName("database", "app")
	session.SetParamByName("user", "alice")
	session.SetParamByName("host", "127.0.0.1")
	session.SetParamByName("active_roles", []string{"app_reader", "app_writer"})
	session.SetParamByName("connection_id", int64(42))

	rows := mustQuerySessionSQL(t, executor, session, "app", "select database(), user(), current_role(), connection_id() from (select 1 as marker) as d")
	require.Equal(t, [][]interface{}{{"app", "alice@127.0.0.1", "app_reader,app_writer", "42"}}, rows)
}

func TestSessionMetadataFunctionsPropagateThroughCTEProjection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	session := newTestMySQLSession()
	session.SetParamByName("database", "app")
	session.SetParamByName("user", "alice")
	session.SetParamByName("host", "127.0.0.1")
	session.SetParamByName("active_roles", []string{"app_reader", "app_writer"})
	session.SetParamByName("connection_id", int64(42))

	rows := mustQuerySessionSQL(t, executor, session, "app", "with selected as (select 1 as marker) select database(), user(), current_role(), connection_id() from selected")
	require.Equal(t, [][]interface{}{{"app", "alice@127.0.0.1", "app_reader,app_writer", "42"}}, rows)
}

func TestSessionMetadataFunctionsPropagateThroughRecursiveCTE(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	session := newTestMySQLSession()
	session.SetParamByName("database", "app")
	session.SetParamByName("user", "alice")
	session.SetParamByName("host", "127.0.0.1")
	session.SetParamByName("active_roles", []string{"app_reader", "app_writer"})
	session.SetParamByName("connection_id", int64(42))

	rows := mustQuerySessionSQL(t, executor, session, "app", "with recursive nums(n, role_name) as (select 1, current_role() union all select n + 1, role_name from nums where n < 2) select role_name from nums order by n")
	require.Equal(t, [][]interface{}{{"app_reader,app_writer"}, {"app_reader,app_writer"}}, rows)
}

func TestSessionMetadataFunctionsPropagateThroughSetDerivedProjection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	session := newTestMySQLSession()
	session.SetParamByName("database", "app")
	session.SetParamByName("user", "alice")
	session.SetParamByName("host", "127.0.0.1")
	session.SetParamByName("active_roles", []string{"app_reader", "app_writer"})
	session.SetParamByName("connection_id", int64(42))

	rows := mustQuerySessionSQL(t, executor, session, "app", "select database(), current_role(), connection_id() from ((select 1 as marker) intersect (select 1 as marker)) as d")
	require.Equal(t, [][]interface{}{{"app", "app_reader,app_writer", "42"}}, rows)
}

func TestSessionMetadataFunctionsPropagateThroughDerivedJoinProjection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	session := newTestMySQLSession()
	session.SetParamByName("database", "app")
	session.SetParamByName("user", "alice")
	session.SetParamByName("host", "127.0.0.1")
	session.SetParamByName("active_roles", []string{"app_reader", "app_writer"})
	session.SetParamByName("connection_id", int64(42))

	rows := mustQuerySessionSQL(t, executor, session, "app", "select database(), current_role(), connection_id() from (select 1 as left_marker) as left_rows join (select 1 as right_marker) as right_rows on left_rows.left_marker = right_rows.right_marker")
	require.Equal(t, [][]interface{}{{"app", "app_reader,app_writer", "42"}}, rows)
}

func TestSetNamesCollationUpdatesConnectionSession(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()

	mustExecSessionSQL(t, executor, session, "", "set names latin1 collate latin1_swedish_ci")
	require.Equal(t, "latin1", session.GetParamByName("character_set_client"))
	require.Equal(t, "latin1", session.GetParamByName("character_set_connection"))
	require.Equal(t, "latin1", session.GetParamByName("character_set_results"))
	require.Equal(t, "latin1_swedish_ci", session.GetParamByName("collation_connection"))
}

func TestRowCountThroughSessionSQLPreservesPriorValueDuringSelect(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSessionSQL(t, executor, nil, "", "create database app")
	session := newTestMySQLSession()
	session.SetParamByName("row_count", int64(7))

	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySessionSQL(t, executor, session, "app", "select row_count()"))
	require.Equal(t, int64(-1), session.GetParamByName("row_count"))
	mustExecSessionSQL(t, executor, session, "app", "create table row_count_ddl (id int primary key)")
	require.Equal(t, [][]interface{}{{"0"}}, mustQuerySessionSQL(t, executor, session, "app", "select row_count()"))
	session.SetParamByName("row_count", int64(9))
	require.Error(t, (<-executor.ExecuteQuery(session, "select id from missing_row_count_table", "app")).Err)
	require.Equal(t, int64(9), session.GetParamByName("row_count"))
}

func TestRowCountTracksDMLAffectedRowsThroughSessionSQL(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSessionSQL(t, executor, nil, "", "create database app")
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "create table row_counts (id int primary key, value int)")
	mustExecSessionSQL(t, executor, session, "app", "insert into row_counts values (1, 10), (2, 20)")
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySessionSQL(t, executor, session, "app", "select row_count()"))
	mustExecSessionSQL(t, executor, session, "app", "update row_counts set value = value + 1 where id = 1")
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySessionSQL(t, executor, session, "app", "select row_count()"))
	mustExecSessionSQL(t, executor, session, "app", "delete from row_counts where id = 2")
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySessionSQL(t, executor, session, "app", "select row_count()"))
}

func TestCaseExpressionProjectionUsesSearchedAndSimpleSemantics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table statuses (id int primary key, code int)")
	mustExecSQL(t, executor, "app", "insert into statuses (id, code) values (1, 10), (2, 20), (3, 30)")
	rows := mustQuerySQL(t, executor, "app", "select id, case when code = 10 then 'low' when code = 20 then 'medium' else 'high' end as searched, case code when 10 then 'L' when 20 then 'M' else 'H' end as simple from statuses order by id")
	require.Equal(t, [][]interface{}{{"1", "low", "L"}, {"2", "medium", "M"}, {"3", "high", "H"}}, rows)
}

func TestAdditionalDateFunctions(t *testing.T) {
	cases := []struct {
		name string
		expr plan.Expression
		want interface{}
	}{
		{"quarter", &plan.Function{FuncName: "QUARTER", FuncArgs: []plan.Expression{&plan.Constant{Value: "2024-03-05"}}}, int64(1)},
		{"weekday", &plan.Function{FuncName: "WEEKDAY", FuncArgs: []plan.Expression{&plan.Constant{Value: "2024-03-04"}}}, int64(0)},
		{"day of week", &plan.Function{FuncName: "DAYOFWEEK", FuncArgs: []plan.Expression{&plan.Constant{Value: "2024-03-03"}}}, int64(1)},
		{"day of year", &plan.Function{FuncName: "DAYOFYEAR", FuncArgs: []plan.Expression{&plan.Constant{Value: "2024-03-05"}}}, int64(65)},
		{"last day", &plan.Function{FuncName: "LAST_DAY", FuncArgs: []plan.Expression{&plan.Constant{Value: "2024-02-05"}}}, "2024-02-29"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.expr.Eval(&plan.EvalContext{Row: map[string]interface{}{}})
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestConvertTZNamedTimezoneIsAvailableThroughSQL(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	require.Equal(t, [][]interface{}{{"2024-07-15 16:00:00"}}, mustQuerySQL(t, executor, "app", "select convert_tz('2024-07-15 12:00:00', 'America/New_York', 'UTC')"))
}

func TestNetworkAndDigestCompatibilityFunctions(t *testing.T) {
	cases := []struct {
		name string
		expr plan.Expression
		want interface{}
	}{
		{"md5", &plan.Function{FuncName: "MD5", FuncArgs: []plan.Expression{&plan.Constant{Value: "abc"}}}, "900150983cd24fb0d6963f7d28e17f72"},
		{"sha1", &plan.Function{FuncName: "SHA1", FuncArgs: []plan.Expression{&plan.Constant{Value: "abc"}}}, "a9993e364706816aba3e25717850c26c9cd0d89d"},
		{"sha2 256", &plan.Function{FuncName: "SHA2", FuncArgs: []plan.Expression{&plan.Constant{Value: "abc"}, &plan.Constant{Value: int64(256)}}}, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{"crc32", &plan.Function{FuncName: "CRC32", FuncArgs: []plan.Expression{&plan.Constant{Value: "abc"}}}, uint64(891568578)},
		{"inet aton", &plan.Function{FuncName: "INET_ATON", FuncArgs: []plan.Expression{&plan.Constant{Value: "127.0.0.1"}}}, uint64(2130706433)},
		{"inet ntoa", &plan.Function{FuncName: "INET_NTOA", FuncArgs: []plan.Expression{&plan.Constant{Value: int64(2130706433)}}}, "127.0.0.1"},
		{"inet6 aton ipv4", &plan.Function{FuncName: "INET6_ATON", FuncArgs: []plan.Expression{&plan.Constant{Value: "127.0.0.1"}}}, []byte{127, 0, 0, 1}},
		{"inet6 aton ipv6", &plan.Function{FuncName: "INET6_ATON", FuncArgs: []plan.Expression{&plan.Constant{Value: "2001:db8::1"}}}, []byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}},
		{"inet6 ntoa ipv4", &plan.Function{FuncName: "INET6_NTOA", FuncArgs: []plan.Expression{&plan.Constant{Value: []byte{127, 0, 0, 1}}}}, "127.0.0.1"},
		{"inet6 ntoa ipv6", &plan.Function{FuncName: "INET6_NTOA", FuncArgs: []plan.Expression{&plan.Constant{Value: []byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}}}}, "2001:db8::1"},
		{"is ipv4", &plan.Function{FuncName: "IS_IPV4", FuncArgs: []plan.Expression{&plan.Constant{Value: "127.0.0.1"}}}, int64(1)},
		{"is ipv6", &plan.Function{FuncName: "IS_IPV6", FuncArgs: []plan.Expression{&plan.Constant{Value: "2001:db8::1"}}}, int64(1)},
		{"is ipv4 compatible", &plan.Function{FuncName: "IS_IPV4_COMPAT", FuncArgs: []plan.Expression{&plan.Constant{Value: "::127.0.0.1"}}}, int64(1)},
		{"is ipv4 mapped", &plan.Function{FuncName: "IS_IPV4_MAPPED", FuncArgs: []plan.Expression{&plan.Constant{Value: "::ffff:127.0.0.1"}}}, int64(1)},
		{"uuid to bin", &plan.Function{FuncName: "UUID_TO_BIN", FuncArgs: []plan.Expression{&plan.Constant{Value: "6ccd780c-baba-1026-9564-5b8c656024db"}}}, []byte{0x6c, 0xcd, 0x78, 0x0c, 0xba, 0xba, 0x10, 0x26, 0x95, 0x64, 0x5b, 0x8c, 0x65, 0x60, 0x24, 0xdb}},
		{"uuid to bin swap", &plan.Function{FuncName: "UUID_TO_BIN", FuncArgs: []plan.Expression{&plan.Constant{Value: "6ccd780c-baba-1026-9564-5b8c656024db"}, &plan.Constant{Value: int64(1)}}}, []byte{0x10, 0x26, 0xba, 0xba, 0x6c, 0xcd, 0x78, 0x0c, 0x95, 0x64, 0x5b, 0x8c, 0x65, 0x60, 0x24, 0xdb}},
		{"bin to uuid", &plan.Function{FuncName: "BIN_TO_UUID", FuncArgs: []plan.Expression{&plan.Constant{Value: []byte{0x6c, 0xcd, 0x78, 0x0c, 0xba, 0xba, 0x10, 0x26, 0x95, 0x64, 0x5b, 0x8c, 0x65, 0x60, 0x24, 0xdb}}}}, "6ccd780c-baba-1026-9564-5b8c656024db"},
		{"bin to uuid swap", &plan.Function{FuncName: "BIN_TO_UUID", FuncArgs: []plan.Expression{&plan.Constant{Value: []byte{0x10, 0x26, 0xba, 0xba, 0x6c, 0xcd, 0x78, 0x0c, 0x95, 0x64, 0x5b, 0x8c, 0x65, 0x60, 0x24, 0xdb}}, &plan.Constant{Value: int64(1)}}}, "6ccd780c-baba-1026-9564-5b8c656024db"},
		{"is uuid valid", &plan.Function{FuncName: "IS_UUID", FuncArgs: []plan.Expression{&plan.Constant{Value: "6ccd780c-baba-1026-9564-5b8c656024db"}}}, int64(1)},
		{"is uuid invalid", &plan.Function{FuncName: "IS_UUID", FuncArgs: []plan.Expression{&plan.Constant{Value: "not-a-uuid"}}}, int64(0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.expr.Eval(&plan.EvalContext{Row: map[string]interface{}{}})
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	for _, tc := range []struct {
		name string
		expr plan.Expression
	}{
		{"md5 null", &plan.Function{FuncName: "MD5", FuncArgs: []plan.Expression{&plan.Constant{Value: nil}}}},
		{"sha2 unsupported bits", &plan.Function{FuncName: "SHA2", FuncArgs: []plan.Expression{&plan.Constant{Value: "abc"}, &plan.Constant{Value: int64(128)}}}},
		{"inet aton invalid", &plan.Function{FuncName: "INET_ATON", FuncArgs: []plan.Expression{&plan.Constant{Value: "not-an-ip"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.expr.Eval(&plan.EvalContext{Row: map[string]interface{}{}})
			require.NoError(t, err)
			require.Nil(t, got)
		})
	}
}

func TestBinaryAndWhitespaceStringCompatibilityFunctions(t *testing.T) {
	cases := []struct {
		name string
		expr plan.Expression
		want interface{}
	}{
		{"hex string", &plan.Function{FuncName: "HEX", FuncArgs: []plan.Expression{&plan.Constant{Value: "abc"}}}, "616263"},
		{"hex integer", &plan.Function{FuncName: "HEX", FuncArgs: []plan.Expression{&plan.Constant{Value: int64(255)}}}, "FF"},
		{"unhex", &plan.Function{FuncName: "UNHEX", FuncArgs: []plan.Expression{&plan.Constant{Value: "616263"}}}, []byte("abc")},
		{"bin", &plan.Function{FuncName: "BIN", FuncArgs: []plan.Expression{&plan.Constant{Value: int64(10)}}}, "1010"},
		{"oct", &plan.Function{FuncName: "OCT", FuncArgs: []plan.Expression{&plan.Constant{Value: int64(10)}}}, "12"},
		{"trim", &plan.Function{FuncName: "TRIM", FuncArgs: []plan.Expression{&plan.Constant{Value: "  abc  "}}}, "abc"},
		{"ltrim", &plan.Function{FuncName: "LTRIM", FuncArgs: []plan.Expression{&plan.Constant{Value: "  abc  "}}}, "abc  "},
		{"rtrim", &plan.Function{FuncName: "RTRIM", FuncArgs: []plan.Expression{&plan.Constant{Value: "  abc  "}}}, "  abc"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.expr.Eval(&plan.EvalContext{Row: map[string]interface{}{}})
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestCommonCompatibilityFunctionsThroughSQL(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table values_table (id int primary key, created datetime)")
	mustExecSQL(t, executor, "app", "insert into values_table values (1, '2024-03-05 14:06:07')")

	require.Equal(t, [][]interface{}{{"7", "alpha", "2024-03-05 14:06:07", "42", "abc", "abc", "2024-04-05 14:06:07", "2024-02-05 14:06:07", "29", "29"}}, mustQuerySQL(t, executor, "app", "select greatest(2, 7), least('beta', 'alpha'), date_format(created, '%Y-%m-%d %H:%i:%s'), cast('42' as signed), convert('abc' using utf8mb4), 'abc' collate utf8mb4_general_ci, date_add(created, interval 1 month), date_sub(created, interval 1 month), datediff(created, '2024-02-05'), timestampdiff(day, '2024-02-05', created) from values_table"))
	require.Equal(t, [][]interface{}{{string([]byte{0xe9})}}, mustQuerySQL(t, executor, "app", "select convert('é', char character set latin1)"))
	require.Equal(t, [][]interface{}{{string([]byte{0xc3, 0xa9})}}, mustQuerySQL(t, executor, "app", "select convert('é', binary)"))
	require.Equal(t, [][]interface{}{{"abc"}}, mustQuerySQL(t, executor, "app", "select convert('abcdef', char(3))"))
	require.Equal(t, [][]interface{}{{"123.46"}}, mustQuerySQL(t, executor, "app", "select convert('123.456', decimal(8, 2))"))
	require.Equal(t, [][]interface{}{{string([]byte{0xe9})}}, mustQuerySQL(t, executor, "app", "select convert('éx', char(1) character set latin1)"))
	require.Equal(t, [][]interface{}{{string([]byte{0xc3, 0xa9})}}, mustQuerySQL(t, executor, "app", "select convert('éx', binary(2))"))
	require.Equal(t, [][]interface{}{{"42"}}, mustQuerySQL(t, executor, "app", "select cast('42.9' as signed)"))
	require.Equal(t, [][]interface{}{{"42"}}, mustQuerySQL(t, executor, "app", "select cast('42.9' as unsigned)"))
	require.Equal(t, [][]interface{}{{"42"}}, mustQuerySQL(t, executor, "app", `select json_value('{"n":"42"}', '$.n' returning unsigned)`))
	require.Equal(t, [][]interface{}{{"123.46"}}, mustQuerySQL(t, executor, "app", `select json_value('{"n":"123.456"}', '$.n' returning decimal(8, 2))`))
	require.Equal(t, [][]interface{}{{"abc"}}, mustQuerySQL(t, executor, "app", `select json_value('{"n":"abcdef"}', '$.n' returning char(3))`))
	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySQL(t, executor, "app", `select json_value('{"n":"42"}', '$.missing' returning unsigned default 7 on empty)`))
	require.Equal(t, [][]interface{}{{nil}}, mustQuerySQL(t, executor, "app", `select json_value('{"n":"42"}', '$.missing' returning unsigned null on empty)`))
	require.Equal(t, [][]interface{}{{"9"}}, mustQuerySQL(t, executor, "app", `select json_value('{"n":{}}', '$.n' returning unsigned default 9 on error)`))
	require.Equal(t, [][]interface{}{{nil}}, mustQuerySQL(t, executor, "app", `select json_value('{"n":', '$.n' returning unsigned null on error)`))
	require.Equal(t, [][]interface{}{{"%m.%d.%Y", "%Y-%m-%d %H:%i:%s", "%H%i%s"}}, mustQuerySQL(t, executor, "app", "select get_format('DATE', 'USA'), get_format('DATETIME', 'ISO'), get_format('TIME', 'INTERNAL')"))
	require.Equal(t, [][]interface{}{{"2024-01-01 20:00:00", "2024-01-01 04:00:00"}}, mustQuerySQL(t, executor, "app", "select convert_tz('2024-01-01 12:00:00', '+00:00', '+08:00'), convert_tz('2024-01-01 12:00:00', '+08:00', 'UTC')"))
	require.Equal(t, [][]interface{}{{"2024", "1", "3", "14", "6", "7"}}, mustQuerySQL(t, executor, "app", "select extract(year from created), extract(quarter from created), extract(month from created), extract(hour from created), extract(minute from created), extract(second from created) from values_table"))
	require.Equal(t, [][]interface{}{{"123456"}}, mustQuerySQL(t, executor, "app", "select extract(microsecond from '2024-03-05 14:06:07.123456')"))
	require.Equal(t, [][]interface{}{{"065 2 09 10 09 10 2024 2024 14:06:07 02:06:07 PM 5th"}}, mustQuerySQL(t, executor, "app", "select date_format(created, '%j %w %U %u %V %v %X %x %T %r %D') from values_table"))
	require.Equal(t, [][]interface{}{{"2024-03-05 00:00:00"}}, mustQuerySQL(t, executor, "app", "select str_to_date('Tuesday, Mar 05 2024', '%W, %b %d %Y')"))
	require.Equal(t, [][]interface{}{{"2024-03-05 14:06:07"}}, mustQuerySQL(t, executor, "app", "select str_to_date('03/05/2024 02:06:07 PM', '%m/%d/%Y %r')"))
	require.Equal(t, [][]interface{}{{"2024-03-05 14:06:07"}}, mustQuerySQL(t, executor, "app", "select str_to_date('2024-03-05 14:06:07', '%Y-%m-%d %T')"))
	require.Equal(t, [][]interface{}{{"2024-03-05 14:06:07.123456"}}, mustQuerySQL(t, executor, "app", "select str_to_date('2024-03-05 14:06:07.123456', '%Y-%m-%d %H:%i:%s.%f')"))
	require.Equal(t, [][]interface{}{{"2024-03-05 02:06:07"}}, mustQuerySQL(t, executor, "app", "select str_to_date('2024-03-05 2:06:07', '%Y-%m-%d %l:%i:%s')"))
	require.Equal(t, [][]interface{}{{"2024-03-05 02:06:07"}}, mustQuerySQL(t, executor, "app", "select str_to_date('2024-03-05 2:06:07', '%Y-%m-%d %k:%i:%s')"))
	require.Equal(t, [][]interface{}{{"10", "A", "-A"}}, mustQuerySQL(t, executor, "app", "select conv('a', 16, 10), conv(10, 10, 16), conv('-10', 10, 16)"))
	require.Equal(t, [][]interface{}{{"65", "a,c", "Y,N,Y,N"}}, mustQuerySQL(t, executor, "app", "select ord('A'), make_set(5, 'a', 'b', 'c'), export_set(5, 'Y', 'N', ',', 4)"))
	require.Equal(t, [][]interface{}{{"H400", "R163"}}, mustQuerySQL(t, executor, "app", "select soundex('Hello'), soundex('Robert')"))
	require.Equal(t, [][]interface{}{{"x1x2", "a1"}}, mustQuerySQL(t, executor, "app", "select regexp_replace('a1a2', 'a', 'x'), regexp_substr('a1a2', 'a[0-9]')"))
	require.Equal(t, [][]interface{}{{`{"a":2}`, "2", "OBJECT"}}, mustQuerySQL(t, executor, "app", "select json_set('{\"a\":1}', '$.a', 2), json_length('{\"a\":1,\"b\":2}'), json_type('{\"a\":1}')"))
	require.Equal(t, [][]interface{}{{`{"items":[1,2]}`, `{"a":1,"b":3}`}}, mustQuerySQL(t, executor, "app", "select json_array_append('{\"items\":[1]}', '$.items', 2), json_insert('{\"a\":1}', '$.a', 2, '$.b', 3)"))
	require.Equal(t, [][]interface{}{{`{"items":[1,2,3]}`}}, mustQuerySQL(t, executor, "app", "select json_array_insert('{\"items\":[1,3]}', '$.items[1]', 2)"))
	require.Equal(t, [][]interface{}{{"$.a", `["$.a","$.b"]`}}, mustQuerySQL(t, executor, "app", "select json_search('{\"a\":\"hello\",\"b\":\"hello\"}', 'one', '%ell%'), json_search('{\"a\":\"hello\",\"b\":\"hello\"}', 'all', 'hello')"))
	require.Equal(t, [][]interface{}{{"900150983cd24fb0d6963f7d28e17f72", "127.0.0.1", "891568578", "1", "1"}}, mustQuerySQL(t, executor, "app", "select md5('abc'), inet_ntoa(inet_aton('127.0.0.1')), crc32('abc'), is_ipv4('127.0.0.1'), is_ipv6('2001:db8::1')"))
	require.Equal(t, [][]interface{}{{"616263", "abc", "1010", "12", "abc", "abc  ", "  abc"}}, mustQuerySQL(t, executor, "app", "select hex('abc'), unhex('616263'), bin(10), oct(10), trim('  abc  '), ltrim('  abc  '), rtrim('  abc  ')"))
}

func TestBitColumnSupportsStoragePredicatesAndMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table bit_values (id int primary key, flags bit(8) not null)")
	mustExecSQL(t, executor, "app", "insert into bit_values (id, flags) values (1, 5), (2, 128)")

	require.Equal(t, [][]interface{}{{"1", "5"}, {"2", "128"}}, mustQuerySQL(t, executor, "app", "select id, flags from bit_values order by id"))
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySQL(t, executor, "app", "select id from bit_values where flags = 128"))
	mustExecSQL(t, executor, "app", "update bit_values set flags = 7 where id = 1")
	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySQL(t, executor, "app", "select flags from bit_values where id = 1"))
	require.Equal(t, [][]interface{}{{"BIT(8)"}}, mustQuerySQL(t, executor, "app", "select column_type from information_schema.columns where table_schema = 'app' and table_name = 'bit_values' and column_name = 'flags'"))
}

func TestAggregateCompatibilityFunctionsThroughSQL(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table aggregate_values (id int primary key, group_name varchar(10), name varchar(10), score int)")
	mustExecSQL(t, executor, "app", "insert into aggregate_values values (1, 'a', 'one', 1), (2, 'a', 'two', 3), (3, 'a', 'one', 5)")

	rows := mustQuerySQL(t, executor, "app", "select group_concat(distinct name separator '|'), group_concat(score order by score desc separator '|'), stddev_pop(score), var_pop(score), bit_or(score) from aggregate_values")
	require.Equal(t, [][]interface{}{{"one|two", "5|3|1", "1.632993161855452", "2.6666666666666665", "7"}}, rows)
	require.Equal(t, [][]interface{}{{"one"}}, mustQuerySQL(t, executor, "app", "select any_value(name) from aggregate_values"))
	mustExecSQL(t, executor, "app", "insert into aggregate_values values (4, 'a', null, null)")
	require.Equal(t, [][]interface{}{{"\x00\x00\x00\x00\x00\x00\x00\x04", "\x00\x00\x00\x00\x00\x00\x00\x03", "one"}}, mustQuerySQL(t, executor, "app", "select count(*), count(score), any_value(name) from aggregate_values"))
	mustExecSQL(t, executor, "app", "create table empty_aggregate_values (id int, score int)")
	require.Equal(t, [][]interface{}{{"\x00\x00\x00\x00\x00\x00\x00\x00", "\x00\x00\x00\x00\x00\x00\x00\x00", nil}}, mustQuerySQL(t, executor, "app", "select count(*), count(score), sum(score) from empty_aggregate_values"))
}

func TestJSONArrayAggregateThroughStorageSelect(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table json_aggregate_values (id int primary key, score int)")
	mustExecSQL(t, executor, "app", "insert into json_aggregate_values values (1, 1), (2, null), (3, 3)")

	require.Equal(t, [][]interface{}{{"[1,null,3]"}}, mustQuerySQL(t, executor, "app", "select json_arrayagg(score) from json_aggregate_values"))
	mustExecSQL(t, executor, "app", "alter table json_aggregate_values add column label varchar(20)")
	mustExecSQL(t, executor, "app", "update json_aggregate_values set label = case id when 1 then 'one' when 2 then 'two' else 'three' end")
	require.Equal(t, [][]interface{}{{`{"one":1,"three":3,"two":null}`}}, mustQuerySQL(t, executor, "app", "select json_objectagg(label, score) from json_aggregate_values"))
}
