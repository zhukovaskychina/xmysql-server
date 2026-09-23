package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestDropDatabaseIfExistsMissingIsOk(t *testing.T) {
	tmp := t.TempDir()
	executor := &XMySQLExecutor{conf: &conf.Cfg{InnodbDataDir: tmp}}
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results}

	executor.executeDropDatabaseStatement(ctx, &sqlparser.DBDDL{
		Action:   "drop",
		DBName:   "missing_db",
		IfExists: true,
	})

	got := <-results
	require.NoError(t, got.Err)
	require.Equal(t, common.RESULT_TYPE_DDL, got.ResultType)
}

func TestDropDatabaseReleasesActiveTableSpacesBeforeDirectoryRemoval(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	mustExecSQL(t, executor, "", "create database cleanup_db")
	mustExecSQL(t, executor, "cleanup_db", "create table cleanup_target (id int primary key, value varchar(32))")
	mustExecSQL(t, executor, "cleanup_db", "insert into cleanup_target values (1, 'active')")
	require.NoError(t, (<-executor.ExecuteQuery(nil, "select id, value from cleanup_target", "cleanup_db")).Err)

	result := <-executor.ExecuteQuery(nil, "drop database cleanup_db", "")
	require.NoError(t, result.Err)
	_, err := os.Stat(filepath.Join(tmp, "cleanup_db"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestShowCharacterSetAndCollationReturnMetadataShape(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	charsetResult := <-executor.ExecuteQuery(nil, "show character set like 'utf8%'", "")
	require.NoError(t, charsetResult.Err)
	charsetData, ok := charsetResult.Data.(map[string]interface{})
	require.True(t, ok, "SHOW CHARACTER SET result = %T", charsetResult.Data)
	require.Equal(t, []string{"Charset", "Description", "Default collation", "Maxlen"}, charsetData["columns"])
	charsetRows, ok := charsetData["rows"].([][]interface{})
	require.True(t, ok, "SHOW CHARACTER SET rows = %T", charsetData["rows"])
	require.NotEmpty(t, charsetRows)
	require.Equal(t, "utf8mb4", fmt.Sprint(charsetRows[0][0]))

	collationResult := <-executor.ExecuteQuery(nil, "show collation like 'utf8mb4%'", "")
	require.NoError(t, collationResult.Err)
	collationData, ok := collationResult.Data.(map[string]interface{})
	require.True(t, ok, "SHOW COLLATION result = %T", collationResult.Data)
	require.Equal(t, []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen", "Pad_attribute"}, collationData["columns"])
	collationRows, ok := collationData["rows"].([][]interface{})
	require.True(t, ok, "SHOW COLLATION rows = %T", collationData["rows"])
	require.NotEmpty(t, collationRows)
	require.Equal(t, "utf8mb4_0900_ai_ci", fmt.Sprint(collationRows[0][0]))

	whereResult := <-executor.ExecuteQuery(nil, "show collation where Charset = 'utf8mb4'", "")
	require.NoError(t, whereResult.Err)
	whereData, ok := whereResult.Data.(map[string]interface{})
	require.True(t, ok, "SHOW COLLATION WHERE result = %T", whereResult.Data)
	whereRows, ok := whereData["rows"].([][]interface{})
	require.True(t, ok, "SHOW COLLATION WHERE rows = %T", whereData["rows"])
	require.Len(t, whereRows, 2)
}

func TestAlterTableAddColumnUpdatesFrm(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "app")
	require.NoError(t, os.MkdirAll(dbPath, 0755))

	executor := NewXMySQLExecutor(nil, &conf.Cfg{InnodbDataDir: tmp})
	createSQL := "create table users (id int primary key)"
	createStmt, err := sqlparser.Parse(createSQL)
	require.NoError(t, err)
	require.NoError(t, executor.createTableImpl("app", "users", createStmt.(*sqlparser.DDL)))

	alterStmt, err := sqlparser.Parse("alter table users add column name varchar(100)")
	require.NoError(t, err)

	results := make(chan *Result, 1)
	executor.executeDDL(alterStmt.(*sqlparser.DDL), nil, "app", results)
	got := <-results
	require.NoError(t, got.Err)

	raw, err := os.ReadFile(filepath.Join(dbPath, "users.frm"))
	require.NoError(t, err)
	require.Contains(t, string(raw), `"name": "name"`)
	require.Contains(t, string(raw), `"type": "varchar"`)
}

func TestAlterTableAddColumnsWithoutDefaultsOmitDefaultMetadata(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "app")
	require.NoError(t, os.MkdirAll(dbPath, 0755))

	executor := NewXMySQLExecutor(nil, &conf.Cfg{InnodbDataDir: tmp})
	createStmt, err := sqlparser.Parse("create table users (id int primary key)")
	require.NoError(t, err)
	require.NoError(t, executor.createTableImpl("app", "users", createStmt.(*sqlparser.DDL)))

	for _, query := range []string{
		"alter table users add column nickname varchar(100)",
		"alter table users add column login_count int not null",
	} {
		alterStmt, err := sqlparser.Parse(query)
		require.NoError(t, err)

		results := make(chan *Result, 1)
		executor.executeDDL(alterStmt.(*sqlparser.DDL), nil, "app", results)
		require.NoError(t, (<-results).Err)
	}

	columns := readFrmColumns(t, filepath.Join(dbPath, "users.frm"))
	for _, name := range []string{"nickname", "login_count"} {
		column := columns[name]
		require.NotNil(t, column)
		_, hasDefault := column["default"]
		require.False(t, hasDefault, "column %q without SQL DEFAULT must omit default metadata", name)
	}
	require.True(t, columns["nickname"]["nullable"].(bool))
	require.False(t, columns["login_count"]["nullable"].(bool))
}

func TestAlterTableAddColumnRefreshesDMLMetadata(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "app")
	require.NoError(t, os.MkdirAll(dbPath, 0755))

	cfg := &conf.Cfg{InnodbDataDir: tmp}
	executor := NewXMySQLExecutor(nil, cfg)
	createStmt, err := sqlparser.Parse("create table users (id int primary key)")
	require.NoError(t, err)
	require.NoError(t, executor.createTableImpl("app", "users", createStmt.(*sqlparser.DDL)))

	engine := &XMySQLEngine{conf: cfg, QueryExecutor: executor}
	alterResult := <-engine.ExecuteQuery(nil, "alter table users add column nickname varchar(100)", "app")
	require.NoError(t, alterResult.Err)

	dml := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	dml.SetDataDir(tmp)
	dml.schemaName = "app"
	dml.tableName = "users"
	tableMeta, err := dml.getTableMetadata()
	require.NoError(t, err)
	require.Len(t, tableMeta.Columns, 2)
	require.Equal(t, "nickname", tableMeta.Columns[1].Name)
}

func TestAlterTableAddAndDropIndexUpdatesFrm(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(100))")

	mustExecSQL(t, executor, "app", "alter table users add index idx_name (name)")
	indexes := readFrmIndexes(t, filepath.Join(tmp, "app", "users.frm"))
	require.Contains(t, indexes, "idx_name")

	mustExecSQL(t, executor, "app", "alter table users rename index idx_name to idx_full_name")
	indexes = readFrmIndexes(t, filepath.Join(tmp, "app", "users.frm"))
	require.NotContains(t, indexes, "idx_name")
	require.Contains(t, indexes, "idx_full_name")

	mustExecSQL(t, executor, "app", "alter table users drop index idx_full_name")
	indexes = readFrmIndexes(t, filepath.Join(tmp, "app", "users.frm"))
	require.NotContains(t, indexes, "idx_full_name")
}

func TestAlterTableDropColumnUpdatesFrm(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(100), age int)")

	mustExecSQL(t, executor, "app", "alter table users drop column age")
	columns := readFrmColumns(t, filepath.Join(tmp, "app", "users.frm"))
	require.NotContains(t, columns, "age")
}

func TestAlterTableDropColumnPreservesRowsAndFutureDML(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(100), age int)")
	mustExecSQL(t, executor, "app", "insert into users values (1, 'alice', 30)")

	mustExecSQL(t, executor, "app", "alter table users drop column age")
	require.Equal(t, [][]interface{}{{"1", "alice"}}, mustQuerySQL(t, executor, "app", "select * from users"))
	mustExecSQL(t, executor, "app", "insert into users values (2, 'bob')")
	require.Equal(t, [][]interface{}{{"1", "alice"}, {"2", "bob"}}, mustQuerySQL(t, executor, "app", "select * from users order by id"))
}

func TestAlterTableDropMiddleColumnRewritesFollowingValues(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(100), age int)")
	mustExecSQL(t, executor, "app", "insert into users values (1, 'alice', 30)")

	mustExecSQL(t, executor, "app", "alter table users drop column name")
	require.Equal(t, [][]interface{}{{"1", "30"}}, mustQuerySQL(t, executor, "app", "select * from users"))
}

func TestAlterTableModifyChangeAndRenamePreserveMetadataAtomically(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(20), index idx_name (name))")
	mustExecSQL(t, executor, "app", "insert into users (id, name) values (1, 'alice')")

	mustExecSQL(t, executor, "app", "alter table users modify column name varchar(50) not null")
	columns := readFrmColumns(t, filepath.Join(tmp, "app", "users.frm"))
	require.Equal(t, "varchar", columns["name"]["type"])
	require.Equal(t, float64(50), columns["name"]["length"])
	require.False(t, columns["name"]["nullable"].(bool))

	mustExecSQL(t, executor, "app", "alter table users change column name full_name varchar(80) default 'guest'")
	columns = readFrmColumns(t, filepath.Join(tmp, "app", "users.frm"))
	require.NotContains(t, columns, "name")
	require.Equal(t, "varchar", columns["full_name"]["type"])
	require.Equal(t, "guest", columns["full_name"]["default"])
	indexes := readFrmIndexes(t, filepath.Join(tmp, "app", "users.frm"))
	require.Contains(t, indexes, "idx_name")

	mustExecSQL(t, executor, "app", "rename table users to people")
	require.FileExists(t, filepath.Join(tmp, "app", "people.frm"))
	require.NoFileExists(t, filepath.Join(tmp, "app", "users.frm"))
	rows := mustQuerySQL(t, executor, "app", "select id, full_name from people")
	require.Equal(t, [][]interface{}{{"1", "alice"}}, rows)
}

func TestAlterTableAddColumnFirstRewritesExistingRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(100), age int)")
	mustExecSQL(t, executor, "app", "insert into users values (1, 'alice', 30)")

	mustExecSQL(t, executor, "app", "alter table users add column marker int first")
	require.Equal(t, [][]interface{}{{nil, "1", "alice", "30"}}, mustQuerySQL(t, executor, "app", "select * from users"))
}

func TestAlterTableModifyColumnHonorsFirstAndAfterPositions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(20), age int)")
	mustExecSQL(t, executor, "app", "insert into users values (1, 'alice', 30)")

	mustExecSQL(t, executor, "app", "alter table users modify column age bigint first")
	tableInfo, err := readTableMetadataMap(filepath.Join(executor.GetDataDir(), "app", "users.frm"))
	require.NoError(t, err)
	columns := tableInfo["columns"].([]interface{})
	require.Equal(t, "age", columns[0].(map[string]interface{})["name"])
	modified := mustSelectResultSQL(t, executor, "app", "select * from users")
	require.Len(t, modified.Records, 1)
	require.Equal(t, int64(30), modified.Records[0].GetValues()[0].Int())
	require.Equal(t, "1", modified.Records[0].GetValues()[1].String())
	require.Equal(t, "alice", modified.Records[0].GetValues()[2].String())

	mustExecSQL(t, executor, "app", "alter table users change column name display_name varchar(40) after age")
	tableInfo, err = readTableMetadataMap(filepath.Join(executor.GetDataDir(), "app", "users.frm"))
	require.NoError(t, err)
	columns = tableInfo["columns"].([]interface{})
	require.Equal(t, "age", columns[0].(map[string]interface{})["name"])
	require.Equal(t, "display_name", columns[1].(map[string]interface{})["name"])
	changed := mustSelectResultSQL(t, executor, "app", "select * from users")
	require.Len(t, changed.Records, 1)
	require.Equal(t, int64(30), changed.Records[0].GetValues()[0].Int())
	require.Equal(t, "alice", changed.Records[0].GetValues()[1].String())
	require.Equal(t, "1", changed.Records[0].GetValues()[2].String())
}

func TestAlterTableRenameColumnPreservesExistingRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(100), age int)")
	mustExecSQL(t, executor, "app", "insert into users values (1, 'alice', 30)")

	mustExecSQL(t, executor, "app", "alter table users rename column name to display_name")
	renamed := mustSelectResultSQL(t, executor, "app", "select * from users")
	require.Len(t, renamed.Records, 1)
	require.Equal(t, "1", renamed.Records[0].GetValues()[0].String())
	require.Equal(t, "alice", renamed.Records[0].GetValues()[1].String())
	require.Equal(t, "30", renamed.Records[0].GetValues()[2].String())
}

func TestShowIndexReturnsDeclaredIndexes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(100), index idx_name (name))")

	got := <-executor.ExecuteQuery(nil, "show index from users", "app")
	require.NoError(t, got.Err)
	data, ok := got.Data.(map[string]interface{})
	require.True(t, ok, "expected SHOW INDEX result map, got %T", got.Data)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, rows, 2)
	require.Equal(t, "PRIMARY", rows[0][2])
	require.Equal(t, "idx_name", rows[1][2])
}

func TestAlterIndexVisibilityPersistsInShowIndexAndInformationSchema(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(100), index idx_name (name))")

	mustExecSQL(t, executor, "app", "alter table users alter index idx_name invisible")
	show := <-executor.ExecuteQuery(nil, "show index from users", "app")
	require.NoError(t, show.Err)
	showData := show.Data.(map[string]interface{})
	showRows := showData["rows"].([][]interface{})
	require.Equal(t, "NO", showRows[1][13])
	create := <-executor.ExecuteQuery(nil, "show create table users", "app")
	require.NoError(t, create.Err)
	createRows := create.Data.(map[string]interface{})["rows"].([][]interface{})
	require.Contains(t, strings.ToLower(fmt.Sprint(createRows[0][1])), "idx_name` (`name`) invisible")

	statistics := <-executor.ExecuteQuery(nil, "select index_name, is_visible from information_schema.statistics where table_schema = 'app' and table_name = 'users' and index_name = 'idx_name'", "app")
	require.NoError(t, statistics.Err)
	statisticsResult := statistics.Data.(*SelectResult)
	require.Len(t, statisticsResult.Records, 1)
	require.Equal(t, "NO", statisticsResult.Records[0].GetValues()[1].String())

	mustExecSQL(t, executor, "app", "alter table users alter index idx_name visible")
	show = <-executor.ExecuteQuery(nil, "show index from users", "app")
	require.NoError(t, show.Err)
	showRows = show.Data.(map[string]interface{})["rows"].([][]interface{})
	require.Equal(t, "YES", showRows[1][13])
}

func TestCreateTableIndexVisibilityIsPersisted(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table create_index_visibility (id int primary key, value varchar(20), index idx_value (value) invisible)")

	show := <-executor.ExecuteQuery(nil, "show index from create_index_visibility", "app")
	require.NoError(t, show.Err)
	showRows := show.Data.(map[string]interface{})["rows"].([][]interface{})
	require.Equal(t, "NO", showRows[1][13])
}

func TestShowCreateTableReturnsPersistedDefinition(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(100) not null default 'guest', unique key uk_name (name))")

	got := <-executor.ExecuteQuery(nil, "show create table users", "app")
	require.NoError(t, got.Err)
	data, ok := got.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, rows, 1)
	createSQL, ok := rows[0][1].(string)
	require.True(t, ok)
	require.Contains(t, strings.ToLower(createSQL), "`name` varchar(100) not null default 'guest'")
	require.Contains(t, strings.ToLower(createSQL), "unique key `uk_name` (`name`)")
}

func TestDescribeAndExplainUsePersistedMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key auto_increment, name varchar(100) not null)")

	describe := <-executor.ExecuteQuery(nil, "describe users", "app")
	require.NoError(t, describe.Err)
	describeData, ok := describe.Data.(map[string]interface{})
	require.True(t, ok)
	describeRows, ok := describeData["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, describeRows, 2)
	require.Equal(t, "id", describeRows[0][0])
	require.Equal(t, "PRI", describeRows[0][3])
	require.Equal(t, "auto_increment", describeRows[0][5])

	explain := <-executor.ExecuteQuery(nil, "explain select id from users where id = 1", "app")
	require.NoError(t, explain.Err)
	explainData, ok := explain.Data.(map[string]interface{})
	require.True(t, ok)
	explainRows, ok := explainData["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, explainRows, 1)
	require.Equal(t, "users", explainRows[0][2])
	require.Equal(t, "PRIMARY", explainRows[0][5])
}

func TestMaintenanceStatementsValidatePersistedTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")

	for _, statement := range []string{"check table users", "analyze table users", "optimize table users"} {
		got := <-executor.ExecuteQuery(nil, statement, "app")
		require.NoError(t, got.Err, statement)
		data, ok := got.Data.(map[string]interface{})
		require.True(t, ok, statement)
		rows, ok := data["rows"].([][]interface{})
		require.True(t, ok, statement)
		require.Len(t, rows, 1, statement)
		require.Equal(t, "users", rows[0][0], statement)
		require.Equal(t, "OK", rows[0][3], statement)
	}
}

func TestRepairTableReportsInnoDBUnsupported(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table repair_target (id int primary key)")

	result := <-executor.ExecuteQuery(nil, "repair table repair_target", "app")
	require.Error(t, result.Err)
	require.Contains(t, strings.ToLower(result.Err.Error()), "doesn't support repair")
}

func TestInformationSchemaConstraintRowsComeFromFrm(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, constraint fk_parent foreign key (parent_id) references parents(id) on delete cascade)")

	got := <-executor.ExecuteQuery(nil, "select constraint_name, table_name, column_name, referenced_table_name, referenced_column_name from information_schema.key_column_usage where table_schema = 'app' and table_name = 'children' and constraint_name = 'fk_parent'", "app")
	require.NoError(t, got.Err)
	result, ok := got.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	require.Equal(t, "fk_parent", values[0].ToString())
	require.Equal(t, "children", values[1].ToString())
	require.Equal(t, "parent_id", values[2].ToString())
	require.Equal(t, "parents", values[3].ToString())
	require.Equal(t, "id", values[4].ToString())

	got = <-executor.ExecuteQuery(nil, "select constraint_name, constraint_type from information_schema.table_constraints where table_schema = 'app' and table_name = 'children'", "app")
	require.NoError(t, got.Err)
	result, ok = got.Data.(*SelectResult)
	require.True(t, ok)
	require.Contains(t, result.Columns, "CONSTRAINT_TYPE")
	require.Len(t, result.Records, 2)

	got = <-executor.ExecuteQuery(nil, "select index_name, column_name, non_unique from information_schema.statistics where table_schema = 'app' and table_name = 'children'", "app")
	require.NoError(t, got.Err)
	result, ok = got.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, result.Records, 2)
}

func TestCreateTablePersistsCommonTableOptions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table configured_rows (id int primary key) engine=innodb default charset=utf8mb4 collate=utf8mb4_bin comment='configured table'")

	raw, err := os.ReadFile(filepath.Join(executor.conf.InnodbDataDir, "app", "configured_rows.frm"))
	require.NoError(t, err)
	var tableInfo struct {
		Options map[string]interface{} `json:"options"`
	}
	require.NoError(t, json.Unmarshal(raw, &tableInfo))
	require.Equal(t, "InnoDB", tableInfo.Options["engine"])
	require.Equal(t, "utf8mb4", tableInfo.Options["charset"])
	require.Equal(t, "utf8mb4_bin", tableInfo.Options["collation"])
	require.Equal(t, "configured table", tableInfo.Options["comment"])
	require.Equal(t, [][]interface{}{{"utf8mb4_bin"}}, mustQuerySQL(t, executor, "app", "select table_collation from information_schema.tables where table_schema = 'app' and table_name = 'configured_rows'"))
	show := <-executor.ExecuteQuery(nil, "show create table configured_rows", "app")
	require.NoError(t, show.Err)
	showData, ok := show.Data.(map[string]interface{})
	require.True(t, ok)
	showRows, ok := showData["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, showRows, 1)
	require.Contains(t, strings.ToLower(fmt.Sprint(showRows[0][1])), "collate=utf8mb4_bin")
}

func TestCreateTablePersistsRowFormatMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table compressed_rows (id int primary key) engine=innodb row_format=compressed")

	raw, err := os.ReadFile(filepath.Join(executor.conf.InnodbDataDir, "app", "compressed_rows.frm"))
	require.NoError(t, err)
	var tableInfo struct {
		Options map[string]interface{} `json:"options"`
	}
	require.NoError(t, json.Unmarshal(raw, &tableInfo))
	require.Equal(t, "Compressed", tableInfo.Options["row_format"])
	require.Equal(t, [][]interface{}{{"Compressed"}}, mustQuerySQL(t, executor, "app", "select row_format from information_schema.tables where table_schema = 'app' and table_name = 'compressed_rows'"))

	show := <-executor.ExecuteQuery(nil, "show create table compressed_rows", "app")
	require.NoError(t, show.Err)
	showData, ok := show.Data.(map[string]interface{})
	require.True(t, ok)
	showRows, ok := showData["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, showRows, 1)
	require.Contains(t, strings.ToLower(fmt.Sprint(showRows[0][1])), "row_format=compressed")
}

func TestAlterTablePersistsCharacterSetAndCollationOptions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table alter_options (id int primary key)")
	mustExecSQL(t, executor, "app", "alter table alter_options default character set=utf8mb4 collate=utf8mb4_bin")
	require.Equal(t, [][]interface{}{{"utf8mb4_bin"}}, mustQuerySQL(t, executor, "app", "select table_collation from information_schema.tables where table_schema = 'app' and table_name = 'alter_options'"))

	show := <-executor.ExecuteQuery(nil, "show create table alter_options", "app")
	require.NoError(t, show.Err)
	showData, ok := show.Data.(map[string]interface{})
	require.True(t, ok)
	showRows, ok := showData["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, showRows, 1)
	require.Contains(t, strings.ToLower(fmt.Sprint(showRows[0][1])), "collate=utf8mb4_bin")
}

func TestAlterTableConvertsCharacterSetAndCollationOptions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table converted_options (id int primary key)")
	mustExecSQL(t, executor, "app", "alter table converted_options convert to character set utf8mb4 collate utf8mb4_bin")
	require.Equal(t, [][]interface{}{{"utf8mb4_bin"}}, mustQuerySQL(t, executor, "app", "select table_collation from information_schema.tables where table_schema = 'app' and table_name = 'converted_options'"))
}

func TestAlterTablePersistsDefaultCollationOnly(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table collation_only (id int primary key)")
	mustExecSQL(t, executor, "app", "alter table collation_only default collate=utf8mb4_bin")
	require.Equal(t, [][]interface{}{{"utf8mb4_bin"}}, mustQuerySQL(t, executor, "app", "select table_collation from information_schema.tables where table_schema = 'app' and table_name = 'collation_only'"))
}

func TestAlterTablePersistsRowFormatOption(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table alter_row_format (id int primary key)")
	mustExecSQL(t, executor, "app", "alter table alter_row_format row_format=compressed")
	require.Equal(t, [][]interface{}{{"Compressed"}}, mustQuerySQL(t, executor, "app", "select row_format from information_schema.tables where table_schema = 'app' and table_name = 'alter_row_format'"))

	show := <-executor.ExecuteQuery(nil, "show create table alter_row_format", "app")
	require.NoError(t, show.Err)
	showData, ok := show.Data.(map[string]interface{})
	require.True(t, ok)
	showRows, ok := showData["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, showRows, 1)
	require.Contains(t, strings.ToLower(fmt.Sprint(showRows[0][1])), "row_format=compressed")
}

func TestCreateAndDropViewPersistsDefinition(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (7)")
	mustExecSQL(t, executor, "app", "create view active_users as select id from users")

	viewPath := filepath.Join(tmp, "app", "active_users.view.json")
	raw, err := os.ReadFile(viewPath)
	require.NoError(t, err)
	require.Contains(t, string(raw), "select id from users")
	show := <-executor.ExecuteQuery(nil, "show create view active_users", "app")
	require.NoError(t, show.Err)
	showData, ok := show.Data.(map[string]interface{})
	require.True(t, ok)
	showRows, ok := showData["rows"].([][]interface{})
	require.True(t, ok)
	require.Contains(t, strings.ToLower(showRows[0][1].(string)), "select id from users")
	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySQL(t, executor, "app", "select * from active_users"))
	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySQL(t, executor, "app", "select id from active_users where id = 7"))
	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySQL(t, executor, "app", "select au.id from active_users as au where au.id = 7"))

	mustExecSQL(t, executor, "app", "drop view active_users")
	_, err = os.Stat(viewPath)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestCreateViewOptionsPersistAndExecute(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (7)")

	mustExecSQL(t, executor, "app", "create algorithm = merge sql security invoker view configured_users as select id from users with cascaded check option")
	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySQL(t, executor, "app", "select id from configured_users"))
	raw, err := os.ReadFile(filepath.Join(tmp, "app", "configured_users.view.json"))
	require.NoError(t, err)
	require.Contains(t, strings.ToLower(string(raw)), "algorithm")
	require.Contains(t, strings.ToLower(string(raw)), "sql_security")
	require.Contains(t, strings.ToLower(string(raw)), "check_option")
}

func TestCreateViewRejectsDuplicateAndOrReplaceReplaces(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (7), (8)")
	mustExecSQL(t, executor, "app", "create view active_users as select id from users where id = 7")

	duplicate := <-executor.ExecuteQuery(nil, "create view active_users as select id from users where id = 8", "app")
	require.Error(t, duplicate.Err)
	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySQL(t, executor, "app", "select id from active_users"))
	mustExecSQL(t, executor, "app", "create or replace view active_users as select id from users where id = 8")
	require.Equal(t, [][]interface{}{{"8"}}, mustQuerySQL(t, executor, "app", "select id from active_users"))
}

func TestCreateViewExplicitColumnListRenamesProjection(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (7)")

	mustExecSQL(t, executor, "app", "create view configured_users (user_id) as select id from users")
	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySQL(t, executor, "app", "select user_id from configured_users"))
	show := <-executor.ExecuteQuery(nil, "show create view configured_users", "app")
	require.NoError(t, show.Err)
	showData, ok := show.Data.(map[string]interface{})
	require.True(t, ok)
	showRows, ok := showData["rows"].([][]interface{})
	require.True(t, ok)
	require.Contains(t, strings.ToLower(showRows[0][1].(string)), "as user_id")
}

func TestInformationSchemaTablesIncludesViews(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "create view active_users as select id from users")

	require.Equal(t, [][]interface{}{{"active_users", "VIEW"}, {"users", "TABLE"}}, mustQuerySQL(t, executor, "app", "select table_name, table_type from information_schema.tables where table_schema = 'app' and table_name in ('active_users', 'users') order by table_name"))
}

func TestInformationSchemaViewsReflectsPersistedOptions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "create algorithm = merge definer = 'alice'@'localhost' sql security invoker view configured_users as select id from users with cascaded check option")

	require.Equal(t, [][]interface{}{{"configured_users", "CASCADED", "alice@localhost", "INVOKER"}}, mustQuerySQL(t, executor, "app", "select table_name, check_option, definer, security_type from information_schema.views where table_schema = 'app' and table_name = 'configured_users'"))
}

func TestShowTablesIncludesViewsWithViewType(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "create view active_users as select id from users")
	session := newTestMySQLSession()
	session.SetParamByName("database", "app")

	show := <-executor.ExecuteQuery(session, "show tables", "app")
	require.NoError(t, show.Err)
	showData, ok := show.Data.(map[string]interface{})
	require.True(t, ok)
	showRows, ok := showData["rows"].([][]interface{})
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"active_users"}, {"users"}}, showRows)

	full := <-executor.ExecuteQuery(session, "show full tables", "app")
	require.NoError(t, full.Err)
	fullData, ok := full.Data.(map[string]interface{})
	require.True(t, ok)
	fullRows, ok := fullData["rows"].([][]interface{})
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"active_users", "VIEW"}, {"users", "BASE TABLE"}}, fullRows)
}

func TestViewColumnMetadataIsVisibleToShowAndInformationSchema(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "create view active_users (user_id) as select id from users")

	show := <-executor.ExecuteQuery(nil, "show columns from active_users", "app")
	require.NoError(t, show.Err)
	showData, ok := show.Data.(map[string]interface{})
	require.True(t, ok)
	showRows, ok := showData["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, showRows, 1)
	require.Equal(t, "user_id", showRows[0][0])

	full := <-executor.ExecuteQuery(nil, "show full columns from active_users", "app")
	require.NoError(t, full.Err)
	fullData, ok := full.Data.(map[string]interface{})
	require.True(t, ok)
	fullRows, ok := fullData["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, fullRows, 1)
	require.Equal(t, "user_id", fullRows[0][0])
	require.Equal(t, "int", fullRows[0][1])

	require.Equal(t, [][]interface{}{{"active_users", "user_id"}}, mustQuerySQL(t, executor, "app", "select table_name, column_name from information_schema.columns where table_schema = 'app' and table_name = 'active_users'"))
}

func TestViewDDLRejectsMissingSourceAtomically(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	mustExecSQL(t, executor, "", "create database app")

	create := <-executor.ExecuteQuery(nil, "create view missing_source as select id from missing_table", "app")
	require.Error(t, create.Err)
	require.NoFileExists(t, filepath.Join(tmp, "app", "missing_source.view.json"))

	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "create view active_users as select id from users")
	replace := <-executor.ExecuteQuery(nil, "create or replace view active_users as select id from missing_table", "app")
	require.Error(t, replace.Err)
	require.Equal(t, [][]interface{}{{"7"}}, func() [][]interface{} {
		mustExecSQL(t, executor, "app", "insert into users (id) values (7)")
		return mustQuerySQL(t, executor, "app", "select id from active_users")
	}())
}

func TestCreateViewWithCTESourceValidatesAndExecutes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users (id, label) values (1, 'active'), (2, 'inactive')")

	mustExecSQL(t, executor, "app", "create view active_users as with recent as (select id, label from users where id = 1) select id, label from recent")
	require.Equal(t, [][]interface{}{{"1", "active"}}, mustQuerySQL(t, executor, "app", "select id, label from active_users"))
	full := <-executor.ExecuteQuery(nil, "show full columns from active_users", "app")
	require.NoError(t, full.Err)
	fullData, ok := full.Data.(map[string]interface{})
	require.True(t, ok)
	fullRows, ok := fullData["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, fullRows, 2)
	require.Contains(t, strings.ToUpper(fmt.Sprint(fullRows[0][1])), "INT")
	require.Contains(t, strings.ToUpper(fmt.Sprint(fullRows[1][1])), "VARCHAR")
}

func TestCreateViewWithNestedDerivedCTESourceExecutes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users (id, label) values (1, 'active'), (2, 'inactive')")

	mustExecSQL(t, executor, "app", "create view active_users_nested as select id, label from (with recent as (select id, label from users where id = 1) select id, label from recent) derived_users")
	require.Equal(t, [][]interface{}{{"1", "active"}}, mustQuerySQL(t, executor, "app", "select id, label from active_users_nested"))
}

func TestCrossDatabaseViewPreservesQualifiedSource(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "", "create database reporting")
	mustExecSQL(t, executor, "", "create database other")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users (id, label) values (7, 'app-user')")
	mustExecSQL(t, executor, "reporting", "create view users_report as select id, label from app.users")

	// The view must keep its stored source binding even when the caller's
	// default database is unrelated to the view and its source.
	require.Equal(t, [][]interface{}{{"7", "app-user"}}, mustQuerySQL(t, executor, "other", "select id, label from reporting.users_report"))
}

func TestViewBindsUnqualifiedSourcesToItsOwningDatabase(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "", "create database reporting")
	mustExecSQL(t, executor, "", "create database other")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users (id, label) values (7, 'app-user')")
	mustExecSQL(t, executor, "reporting", "create table users (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "reporting", "insert into users (id, label) values (8, 'reporting-user')")
	mustExecSQL(t, executor, "reporting", "create view users_report as select id, label from users")

	require.Equal(t, [][]interface{}{{"8", "reporting-user"}}, mustQuerySQL(t, executor, "other", "select id, label from reporting.users_report"))
}

func TestCrossDatabaseViewJoinUsesEachSourceSchema(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "", "create database reporting")
	mustExecSQL(t, executor, "", "create database other")
	mustExecSQL(t, executor, "app", "create table accounts (id int primary key, account_name varchar(20))")
	mustExecSQL(t, executor, "app", "insert into accounts (id, account_name) values (7, 'app-account')")
	mustExecSQL(t, executor, "reporting", "create table labels (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "reporting", "insert into labels (id, label) values (7, 'reporting-label')")
	mustExecSQL(t, executor, "reporting", "create view account_labels as select a.id, l.label from app.accounts a join reporting.labels l on a.id = l.id")

	require.Equal(t, [][]interface{}{{"7", "reporting-label"}}, mustQuerySQL(t, executor, "other", "select id, label from reporting.account_labels"))
}

func TestShowColumnsFiltersViewMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "create view active_users as select id, label from users")

	like := <-executor.ExecuteQuery(nil, "show columns from active_users like 'label%'", "app")
	require.NoError(t, like.Err)
	likeData, ok := like.Data.(map[string]interface{})
	require.True(t, ok)
	likeRows, ok := likeData["rows"].([][]interface{})
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"label", "VARCHAR(20)", "YES", "", nil, ""}}, likeRows)

	where := <-executor.ExecuteQuery(nil, "show columns from active_users where Field = 'id'", "app")
	require.NoError(t, where.Err)
	whereData, ok := where.Data.(map[string]interface{})
	require.True(t, ok)
	whereRows, ok := whereData["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, whereRows, 1)
	require.Equal(t, "id", whereRows[0][0])

	full := <-executor.ExecuteQuery(nil, "show full columns from active_users like 'label%'", "app")
	require.NoError(t, full.Err)
	fullData, ok := full.Data.(map[string]interface{})
	require.True(t, ok)
	fullRows, ok := fullData["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, fullRows, 1)
	require.Equal(t, "label", fullRows[0][0])
}

func TestAlterViewReplacesPersistedDefinition(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (7), (8)")
	mustExecSQL(t, executor, "app", "create view active_users as select id from users")

	mustExecSQL(t, executor, "app", "alter view active_users as select id from users where id = 7 with local check option")
	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySQL(t, executor, "app", "select id from active_users"))
	raw, err := os.ReadFile(filepath.Join(tmp, "app", "active_users.view.json"))
	require.NoError(t, err)
	require.Contains(t, strings.ToLower(string(raw)), "where id = 7")
	require.Contains(t, strings.ToLower(string(raw)), "check_option")
	show := <-executor.ExecuteQuery(nil, "show create view active_users", "app")
	require.NoError(t, show.Err)
	showData, ok := show.Data.(map[string]interface{})
	require.True(t, ok)
	showRows, ok := showData["rows"].([][]interface{})
	require.True(t, ok)
	require.Contains(t, strings.ToLower(showRows[0][1].(string)), "with local check option")
	mustExecSQL(t, executor, "app", "alter algorithm = merge sql security invoker view active_users as select id from users where id = 8")
	require.Equal(t, [][]interface{}{{"8"}}, mustQuerySQL(t, executor, "app", "select id from active_users"))
	raw, err = os.ReadFile(filepath.Join(tmp, "app", "active_users.view.json"))
	require.NoError(t, err)
	require.Contains(t, strings.ToLower(string(raw)), "algorithm")
	require.Contains(t, strings.ToLower(string(raw)), "sql_security")
}

func TestDropViewMultipleTargetsRemovesAllDefinitions(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "create view first_view as select id from users")
	mustExecSQL(t, executor, "app", "create view second_view as select id from users")

	failed := <-executor.ExecuteQuery(nil, "drop view app.first_view, missing_view", "app")
	require.Error(t, failed.Err)
	require.FileExists(t, filepath.Join(tmp, "app", "first_view.view.json"))
	mustExecSQL(t, executor, "app", "drop view if exists app.missing_view, app.first_view, second_view")
	require.NoFileExists(t, filepath.Join(tmp, "app", "first_view.view.json"))
	require.NoFileExists(t, filepath.Join(tmp, "app", "second_view.view.json"))
}

func TestRenameTableRenamesViewDefinition(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (7)")
	mustExecSQL(t, executor, "app", "create view active_users as select id from users")

	mustExecSQL(t, executor, "app", "rename table active_users to enabled_users")
	require.NoFileExists(t, filepath.Join(tmp, "app", "active_users.view.json"))
	require.FileExists(t, filepath.Join(tmp, "app", "enabled_users.view.json"))
	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySQL(t, executor, "app", "select id from enabled_users"))
	missing := <-executor.ExecuteQuery(nil, "select id from active_users", "app")
	require.Error(t, missing.Err)
}

func TestAlterTableAddColumnExistingRowsReadNullAndDefault(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)

	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1)")
	mustExecSQL(t, executor, "app", "alter table users add column nickname varchar(100)")
	mustExecSQL(t, executor, "app", "alter table users add column score int default 7")

	got := <-executor.ExecuteQuery(nil, "select nickname, score from users where id = 1", "app")
	require.NoError(t, got.Err)
	result, ok := got.Data.(*SelectResult)
	require.True(t, ok, "expected SelectResult, got %T", got.Data)
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	require.Len(t, values, 2)
	require.True(t, values[0].IsNull())
	require.Equal(t, int64(7), values[1].Int())
}

func TestAlterTableModifyColumnConvertsExistingRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, score int)")
	mustExecSQL(t, executor, "app", "insert into users (id, score) values (1, 7)")
	mustExecSQL(t, executor, "app", "alter table users modify column score varchar(3)")

	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySQL(t, executor, "app", "select score from users where id = 1"))
}

func TestAlterTableFailureKeepsMetadataAndStorageResolvable(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users (id, name) values (1, 'alice')")

	frmPath := filepath.Join(tmp, "app", "users.frm")
	before, err := os.ReadFile(frmPath)
	require.NoError(t, err)

	failed := <-executor.ExecuteQuery(nil, "alter table users drop column missing, add column nickname varchar(20)", "app")
	require.Error(t, failed.Err)
	after, err := os.ReadFile(frmPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, [][]interface{}{{"alice"}}, mustQuerySQL(t, executor, "app", "select name from users where id = 1"))
}

func TestTruncateKeepsTableMetadataResolvable(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)

	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key auto_increment, username varchar(50) not null)")
	mustExecSQL(t, executor, "app", "insert into users (username) values ('before')")
	mustExecSQL(t, executor, "app", "truncate table users")
	mustExecSQL(t, executor, "app", "insert into users (username) values ('after')")

	rows := mustQuerySQL(t, executor, "app", "select username from users")
	require.Equal(t, [][]interface{}{{"after"}}, rows)
}

func TestTruncateReferencedParentTableIsRejected(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table truncate_parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table truncate_child (id int primary key, parent_id int, constraint fk_truncate_parent foreign key (parent_id) references truncate_parent (id))")
	mustExecSQL(t, executor, "app", "insert into truncate_parent values (1)")
	result := <-executor.ExecuteQuery(nil, "truncate table truncate_parent", "app")
	require.Error(t, result.Err)
	require.Contains(t, strings.ToLower(result.Err.Error()), "foreign key")
	sqlErr, ok := result.Err.(*common.SQLError)
	require.True(t, ok)
	require.Equal(t, uint16(common.ErrTruncateIllegalFk), sqlErr.Code)
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "select id from truncate_parent"))
}

func TestTruncateSelfReferencingTableIsRejected(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table truncate_self (id int primary key, parent_id int, constraint fk_truncate_self foreign key (parent_id) references truncate_self (id))")
	mustExecSQL(t, executor, "app", "insert into truncate_self values (1, null)")
	result := <-executor.ExecuteQuery(nil, "truncate table truncate_self", "app")
	require.Error(t, result.Err)
	require.Contains(t, strings.ToLower(result.Err.Error()), "foreign key")
	sqlErr, ok := result.Err.(*common.SQLError)
	require.True(t, ok)
	require.Equal(t, uint16(common.ErrTruncateIllegalFk), sqlErr.Code)
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "select id from truncate_self"))
}

func TestAlterTableAddsSelfReferentialForeignKey(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table alter_self_fk (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "alter table alter_self_fk add constraint fk_alter_self foreign key (parent_id) references alter_self_fk (id)")
	rows := mustQuerySQL(t, executor, "app", "select constraint_name, referenced_table_name from information_schema.key_column_usage where table_schema = 'app' and table_name = 'alter_self_fk' and constraint_name = 'fk_alter_self'")
	require.Equal(t, [][]interface{}{{"fk_alter_self", "alter_self_fk"}}, rows)
}

func TestForeignKeyDDLRequiresReferencesPrivilegeOnParent(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table fk_priv_parent (id int primary key)")
	mustExecSQL(t, executor, "", "create user 'fk_priv_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant create, alter, insert on app.* to 'fk_priv_user'@'localhost'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "fk_priv_user")
	session.SetParamByName("host", "localhost")

	createDenied := <-executor.ExecuteQuery(session, "create table fk_priv_create (id int primary key, parent_id int, constraint fk_create foreign key (parent_id) references fk_priv_parent (id))", "app")
	require.Error(t, createDenied.Err)
	require.Contains(t, strings.ToLower(createDenied.Err.Error()), "references privilege")

	mustExecSQL(t, executor, "", "grant references on app.fk_priv_parent to 'fk_priv_user'@'localhost'")
	mustExecSessionSQL(t, executor, session, "app", "create table fk_priv_create (id int primary key, parent_id int, constraint fk_create foreign key (parent_id) references fk_priv_parent (id))")
	mustExecSessionSQL(t, executor, session, "app", "create table fk_priv_alter (id int primary key, parent_id int)")

	mustExecSQL(t, executor, "", "revoke references on app.fk_priv_parent from 'fk_priv_user'@'localhost'")
	alterDenied := <-executor.ExecuteQuery(session, "alter table fk_priv_alter add constraint fk_alter foreign key (parent_id) references fk_priv_parent (id)", "app")
	require.Error(t, alterDenied.Err)
	require.Contains(t, strings.ToLower(alterDenied.Err.Error()), "references privilege")

	mustExecSQL(t, executor, "", "grant references on app.fk_priv_parent to 'fk_priv_user'@'localhost'")
	mustExecSessionSQL(t, executor, session, "app", "alter table fk_priv_alter add constraint fk_alter foreign key (parent_id) references fk_priv_parent (id)")
}

func TestCreateTableLikeCopiesDefinitionWithoutRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table source_users (id int primary key, name varchar(20) not null, key name_idx (name))")
	mustExecSQL(t, executor, "app", "insert into source_users values (1, 'alice')")

	result := <-executor.ExecuteQuery(nil, "create table copied_users like source_users", "app")
	require.NoError(t, result.Err)
	require.Empty(t, mustQuerySQL(t, executor, "app", "select id, name from copied_users"))
	show := <-executor.ExecuteQuery(nil, "show create table copied_users", "app")
	require.NoError(t, show.Err)
	showData, ok := show.Data.(map[string]interface{})
	require.True(t, ok)
	showRows, ok := showData["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, showRows, 1)
	createSQL, ok := showRows[0][1].(string)
	require.True(t, ok)
	require.Contains(t, strings.ToLower(createSQL), "`name` varchar(20) not null")
	require.Contains(t, strings.ToLower(createSQL), "key `name_idx` (`name`)")
	require.Equal(t, [][]interface{}{{"1", "alice"}}, mustQuerySQL(t, executor, "app", "select id, name from source_users"))
}

func TestCreateTableAsSelectCopiesProjectedRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table source_rows (id int primary key, name varchar(20), ignored int)")
	mustExecSQL(t, executor, "app", "insert into source_rows values (1, 'one', 10), (2, 'two', 20), (3, 'three', 30)")

	mustExecSQL(t, executor, "app", "create table copied_rows as select id, name from source_rows where id >= 2")
	require.Equal(t, [][]interface{}{{"2", "two"}, {"3", "three"}}, mustQuerySQL(t, executor, "app", "select id, name from copied_rows order by id"))

	mustExecSQL(t, executor, "app", "create table copied_explicit (ident bigint not null, label varchar(10)) as select id, name from source_rows where id = 1")
	require.Equal(t, [][]interface{}{{"1", "one"}}, mustQuerySQL(t, executor, "app", "select ident, label from copied_explicit"))

	mustExecSQL(t, executor, "app", "create table copied_union select id, name from source_rows where id = 1 union all select id, name from source_rows where id = 3")
	require.Equal(t, [][]interface{}{{"1", "one"}, {"3", "three"}}, mustQuerySQL(t, executor, "app", "select id, name from copied_union order by id"))
}

func TestCreateTableAsSelectSupportsNonRecursiveCTE(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ctas_source (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into ctas_source values (1, 'one'), (2, 'two')")

	mustExecSQL(t, executor, "app", "create table ctas_from_cte as with selected as (select id, label from ctas_source where id = 2) select id, label from selected")
	require.Equal(t, [][]interface{}{{"2", "two"}}, mustQuerySQL(t, executor, "app", "select id, label from ctas_from_cte"))
}

func TestCreateTableAsSelectSupportsNestedDerivedCTE(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ctas_nested_source (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into ctas_nested_source (id, label) values (1, 'one'), (2, 'two')")
	mustExecSQL(t, executor, "app", "create table ctas_nested_target as select id, label from (with selected as (select id, label from ctas_nested_source where id = 2) select id, label from selected) nested_rows")
	require.Equal(t, [][]interface{}{{"2", "two"}}, mustQuerySQL(t, executor, "app", "select id, label from ctas_nested_target"))
}

func TestCreateTableAsSelectSupportsRecursiveCTE(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")

	mustExecSQL(t, executor, "app", "create table ctas_from_recursive as with recursive nums(n) as (select 1 union all select n + 1 from nums where n < 3) select n from nums")
	require.Equal(t, [][]interface{}{{"1"}, {"2"}, {"3"}}, mustQuerySQL(t, executor, "app", "select n from ctas_from_recursive order by n"))
}

func TestCreateTableAsSelectSupportsChainedCTEAndTemporaryCTE(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ctas_source (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into ctas_source values (1, 'one'), (2, 'two'), (3, 'three')")

	mustExecSQL(t, executor, "app", "create table ctas_from_chain as with first_rows as (select id, label from ctas_source), filtered_rows as (select id, label from first_rows where id >= 2) select id, label from filtered_rows")
	require.Equal(t, [][]interface{}{{"2", "two"}, {"3", "three"}}, mustQuerySQL(t, executor, "app", "select id, label from ctas_from_chain order by id"))

	mustExecSessionSQL(t, executor, session, "app", "create temporary table temp_from_cte as with selected as (select id, label from ctas_source where id = 1) select id, label from selected")
	require.Equal(t, [][]interface{}{{"1", "one"}}, mustQuerySessionSQL(t, executor, session, "app", "select id, label from temp_from_cte"))
}

func TestCreateTableAsSelectSupportsIgnoreAndReplace(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table source_rows (id int, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into source_rows values (1, 'first')")

	mustExecSQL(t, executor, "app", "create table copied_ignore (id int primary key, label varchar(20)) ignore as select id, label from source_rows union all select id, 'second' from source_rows")
	require.Equal(t, [][]interface{}{{"1", "first"}}, mustQuerySQL(t, executor, "app", "select id, label from copied_ignore"))

	mustExecSQL(t, executor, "app", "create table copied_replace (id int primary key, label varchar(20)) replace as select id, label from source_rows union all select id, 'second' from source_rows")
	require.Equal(t, [][]interface{}{{"1", "second"}}, mustQuerySQL(t, executor, "app", "select id, label from copied_replace"))

	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "create temporary table temp_ignore (id int primary key, label varchar(20)) ignore as select id, label from source_rows union all select id, 'second' from source_rows")
	require.Equal(t, [][]interface{}{{"1", "first"}}, mustQuerySessionSQL(t, executor, session, "app", "select id, label from temp_ignore"))
}

func TestCreateTableAsSelectIfNotExistsPreservesExistingTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table source_rows (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into source_rows values (1, 'source')")
	mustExecSQL(t, executor, "app", "create table copied_rows (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into copied_rows values (9, 'existing')")

	mustExecSQL(t, executor, "app", "create table if not exists copied_rows as select id, label from source_rows")
	require.Equal(t, [][]interface{}{{"9", "existing"}}, mustQuerySQL(t, executor, "app", "select id, label from copied_rows"))
}

func TestCreateTableAsSelectAcceptsCommonInnoDBTableOptions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table source_rows (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into source_rows values (1, 'one')")

	mustExecSQL(t, executor, "app", "create table copied_rows engine=innodb default charset=utf8mb4 collate=utf8mb4_bin as select id, label from source_rows")
	require.Equal(t, [][]interface{}{{"1", "one"}}, mustQuerySQL(t, executor, "app", "select id, label from copied_rows"))

	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "create temporary table selected_rows engine=innodb default charset=utf8mb4 as select id, label from source_rows")
	require.Equal(t, [][]interface{}{{"1", "one"}}, mustQuerySessionSQL(t, executor, session, "app", "select id, label from selected_rows"))
	mustExecSessionSQL(t, executor, session, "app", "create temporary table compressed_rows row_format=compressed as select id, label from source_rows")
	require.Equal(t, [][]interface{}{{"Compressed"}}, mustQuerySessionSQL(t, executor, session, "app", "select row_format from information_schema.tables where table_schema = 'app' and table_name = 'compressed_rows'"))
}

func TestCreateTableAsSelectPersistsRowFormatOption(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table source_rows (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into source_rows values (1, 'one')")

	mustExecSQL(t, executor, "app", "create table copied_compressed row_format=compressed as select id, label from source_rows")
	require.Equal(t, [][]interface{}{{"Compressed"}}, mustQuerySQL(t, executor, "app", "select row_format from information_schema.tables where table_schema = 'app' and table_name = 'copied_compressed'"))
	show := <-executor.ExecuteQuery(nil, "show create table copied_compressed", "app")
	require.NoError(t, show.Err)
	showData, ok := show.Data.(map[string]interface{})
	require.True(t, ok)
	showRows, ok := showData["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, showRows, 1)
	require.Contains(t, strings.ToLower(fmt.Sprint(showRows[0][1])), "row_format=compressed")
}

func TestTemporaryTableIsSessionScopedAndShadowsPersistentTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table shadow (id int)")
	mustExecSQL(t, executor, "app", "insert into shadow values (100)")

	first := newTestMySQLSession()
	second := newTestMySQLSession()
	first.SetParamByName("database", "app")
	second.SetParamByName("database", "app")
	mustExecSessionSQL(t, executor, first, "app", "create temporary table shadow (id int)")
	mustExecSessionSQL(t, executor, first, "app", "insert into shadow values (1)")
	showTables := func(session server.MySQLServerSession) []string {
		result := <-executor.ExecuteQuery(session, "show tables", "app")
		require.NoError(t, result.Err)
		data, ok := result.Data.(map[string]interface{})
		require.True(t, ok)
		rawRows, ok := data["rows"].([][]interface{})
		require.True(t, ok)
		names := make([]string, 0, len(rawRows))
		for _, row := range rawRows {
			if len(row) > 0 {
				names = append(names, fmt.Sprint(row[0]))
			}
		}
		return names
	}
	require.Equal(t, []string{"shadow"}, showTables(first))
	require.Equal(t, []string{"shadow"}, showTables(second))
	mustExecSessionSQL(t, executor, first, "app", "create temporary table temp_only (id int, label varchar(10))")
	require.Equal(t, [][]interface{}{{"temp_only"}}, mustQuerySessionSQL(t, executor, first, "app", "select table_name from information_schema.tables where table_schema = 'app' and table_name = 'temp_only'"))
	require.Empty(t, mustQuerySessionSQL(t, executor, second, "app", "select table_name from information_schema.tables where table_schema = 'app' and table_name = 'temp_only'"))
	require.Equal(t, [][]interface{}{{"id"}, {"label"}}, mustQuerySessionSQL(t, executor, first, "app", "select column_name from information_schema.columns where table_schema = 'app' and table_name = 'temp_only' order by ordinal_position"))

	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySessionSQL(t, executor, first, "app", "select shadow.id from shadow"))
	require.Equal(t, [][]interface{}{{"100"}}, mustQuerySessionSQL(t, executor, second, "app", "select id from shadow"))

	mustExecSessionSQL(t, executor, first, "app", "drop table shadow")
	require.Equal(t, [][]interface{}{{"100"}}, mustQuerySessionSQL(t, executor, first, "app", "select id from shadow"))

	mustExecSessionSQL(t, executor, first, "app", "create temporary table shadow (id int)")
	mustExecSessionSQL(t, executor, first, "app", "insert into shadow values (2)")
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySessionSQL(t, executor, first, "app", "select id from shadow"))
	require.NoError(t, executor.ResetSession(first))
	require.Equal(t, [][]interface{}{{"100"}}, mustQuerySessionSQL(t, executor, first, "app", "select id from shadow"))
}

func TestTemporaryTableSupportsCTASAndLike(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table source_rows (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into source_rows values (1, 'one'), (2, 'two')")

	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "create temporary table selected_rows as select id, label from source_rows")
	require.Equal(t, [][]interface{}{{"1", "one"}, {"2", "two"}}, mustQuerySessionSQL(t, executor, session, "app", "select id, label from selected_rows order by id"))

	mustExecSessionSQL(t, executor, session, "app", "create temporary table empty_rows like source_rows")
	require.Empty(t, mustQuerySessionSQL(t, executor, session, "app", "select id, label from empty_rows"))
	mustExecSessionSQL(t, executor, session, "app", "insert into empty_rows values (3, 'three')")
	require.Equal(t, [][]interface{}{{"3", "three"}}, mustQuerySessionSQL(t, executor, session, "app", "select id, label from empty_rows"))
	require.Equal(t, [][]interface{}{{"empty_rows", "PRIMARY", "id"}}, mustQuerySessionSQL(t, executor, session, "app", "select table_name, index_name, column_name from information_schema.statistics where table_schema = 'app' and table_name = 'empty_rows'"))
	require.Equal(t, [][]interface{}{{"empty_rows", "PRIMARY", "PRIMARY KEY"}}, mustQuerySessionSQL(t, executor, session, "app", "select table_name, constraint_name, constraint_type from information_schema.table_constraints where table_schema = 'app' and table_name = 'empty_rows'"))
	require.Equal(t, [][]interface{}{{"empty_rows", "PRIMARY", "id"}}, mustQuerySessionSQL(t, executor, session, "app", "select table_name, constraint_name, column_name from information_schema.key_column_usage where table_schema = 'app' and table_name = 'empty_rows'"))
	require.Empty(t, mustQuerySessionSQL(t, executor, newTestMySQLSession(), "app", "select table_name from information_schema.statistics where table_schema = 'app' and table_name = 'empty_rows'"))
}

func TestTemporaryTableSupportsExplicitColumnsWithoutASInCTAS(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table source_rows (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into source_rows values (1, 'one'), (2, 'two')")

	mustExecSessionSQL(t, executor, session, "app", "create temporary table selected_rows (ident bigint, text_value varchar(20)) select id, label from source_rows")
	require.Equal(t, [][]interface{}{{"1", "one"}, {"2", "two"}}, mustQuerySessionSQL(t, executor, session, "app", "select ident, text_value from selected_rows order by ident"))
}

func TestShowTableStatusReturnsMySQLMetadataShape(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table status_rows (id int auto_increment primary key, label varchar(20)) engine=innodb row_format=compact comment='status table'")
	mustExecSQL(t, executor, "app", "create table other_rows (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into status_rows (label) values ('one'), ('two')")
	mustExecSQL(t, executor, "app", "analyze table status_rows")

	result := <-executor.ExecuteQuery(nil, "show table status from app like 'status%'", "app")
	require.NoError(t, result.Err)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok, "expected SHOW result map, got %T", result.Data)
	require.Equal(t, []string{"Name", "Engine", "Version", "Row_format", "Rows", "Avg_row_length", "Data_length", "Max_data_length", "Index_length", "Data_free", "Auto_increment", "Create_time", "Update_time", "Check_time", "Collation", "Checksum", "Create_options", "Comment"}, data["columns"])
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok, "expected SHOW rows, got %T", data["rows"])
	require.Len(t, rows, 1)
	values := rows[0]
	require.Equal(t, "status_rows", fmt.Sprint(values[0]))
	require.Equal(t, "InnoDB", fmt.Sprint(values[1]))
	require.Equal(t, "Compact", fmt.Sprint(values[3]))
	require.Equal(t, "2", fmt.Sprint(values[4]))
	require.Greater(t, values[5].(int64), int64(0))
	require.Greater(t, values[6].(int64), int64(0))
	require.Greater(t, values[8].(int64), int64(0))
	require.Equal(t, "3", fmt.Sprint(values[10]))
	require.Equal(t, "status table", fmt.Sprint(values[17]))

	filtered := <-executor.ExecuteQuery(nil, "show table status from app where Name = 'other_rows'", "app")
	require.NoError(t, filtered.Err)
	filteredData, ok := filtered.Data.(map[string]interface{})
	require.True(t, ok, "expected filtered SHOW result map, got %T", filtered.Data)
	filteredRows, ok := filteredData["rows"].([][]interface{})
	require.True(t, ok, "expected filtered SHOW rows, got %T", filteredData["rows"])
	require.Len(t, filteredRows, 1)
	require.Equal(t, "other_rows", fmt.Sprint(filteredRows[0][0]))
}

func TestEngineStartRemovesOrphanedTemporaryTables(t *testing.T) {
	dataDir := t.TempDir()
	cfg := &conf.Cfg{DataDir: dataDir, InnodbDataDir: dataDir, InnodbBufferPoolSize: 16 * 1024 * 1024, InnodbPageSize: 16384}
	first := NewXMySQLEngine(cfg)
	session := newTestMySQLSession()
	mustExecSQL(t, first, "", "create database app")
	mustExecSessionSQL(t, first, session, "app", "create temporary table orphaned (id int)")
	require.NoError(t, first.Close())
	orphanedBeforeStart, err := filepath.Glob(filepath.Join(dataDir, "app", "__xmysql_tmp_*.frm"))
	require.NoError(t, err)
	require.NotEmpty(t, orphanedBeforeStart)

	second := NewXMySQLEngine(cfg)
	require.NoError(t, second.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	orphanedAfterStart, err := filepath.Glob(filepath.Join(dataDir, "app", "__xmysql_tmp_*.frm"))
	require.NoError(t, err)
	require.Empty(t, orphanedAfterStart)
	require.Empty(t, mustQuerySQL(t, second, "app", "select table_name from information_schema.tables where table_schema = 'app' and table_name like '__xmysql_tmp_%'"))
}

func TestTemporaryTableDMLRollbackPreservesTableAndUndoesRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create temporary table staged_rows (id int primary key, label varchar(20))")

	mustExecSessionSQL(t, executor, session, "app", "begin")
	mustExecSessionSQL(t, executor, session, "app", "insert into staged_rows values (1, 'rolled back')")
	mustExecSessionSQL(t, executor, session, "app", "rollback")

	require.Empty(t, mustQuerySessionSQL(t, executor, session, "app", "select id, label from staged_rows"))
	mustExecSessionSQL(t, executor, session, "app", "insert into staged_rows values (2, 'committed')")
	require.Equal(t, [][]interface{}{{"2", "committed"}}, mustQuerySessionSQL(t, executor, session, "app", "select id, label from staged_rows"))
}

func TestTemporaryTableShowCreateUsesLogicalName(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create temporary table visible_definition (id int primary key)")

	got := <-executor.ExecuteQuery(session, "show create table visible_definition", "app")
	require.NoError(t, got.Err)
	data, ok := got.Data.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, []string{"Table", "Create Table"}, data["columns"])
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, rows, 1)
	require.Equal(t, "visible_definition", rows[0][0])
	require.Contains(t, strings.ToLower(fmt.Sprint(rows[0][1])), "create table `visible_definition`")

	indexResult := <-executor.ExecuteQuery(session, "show index from visible_definition", "app")
	require.NoError(t, indexResult.Err)
	indexData, ok := indexResult.Data.(map[string]interface{})
	require.True(t, ok)
	indexRows, ok := indexData["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, indexRows, 1)
	require.Equal(t, "visible_definition", indexRows[0][0])
}

func TestTemporaryTableSupportsAlterTableAgainstPhysicalBinding(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create temporary table alterable_rows (id int primary key)")
	mustExecSessionSQL(t, executor, session, "app", "alter table alterable_rows add column label varchar(20)")
	mustExecSessionSQL(t, executor, session, "app", "insert into alterable_rows values (1, 'ready')")

	require.Equal(t, [][]interface{}{{"1", "ready"}}, mustQuerySessionSQL(t, executor, session, "app", "select id, label from alterable_rows"))
	require.Equal(t, [][]interface{}{{"label", "VARCHAR"}}, mustQuerySessionSQL(t, executor, session, "app", "select column_name, column_type from information_schema.columns where table_schema = 'app' and table_name = 'alterable_rows' and column_name = 'label'"))
	state := getTemporaryTableSessionState(session, false)
	require.NotNil(t, state)
	state.mu.RLock()
	binding, ok := state.tables[temporaryTableKey("app", "alterable_rows")]
	state.mu.RUnlock()
	require.True(t, ok)
	_, err := os.Stat(filepath.Join(dataDir, "app", "alterable_rows.frm"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(dataDir, "app", binding.Physical+".frm"))
	require.NoError(t, err)
}

func TestTemporaryTableRenamePreservesLogicalBinding(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create temporary table before_rename (id int)")
	mustExecSessionSQL(t, executor, session, "app", "insert into before_rename values (1)")
	mustExecSessionSQL(t, executor, session, "app", "alter table before_rename rename to after_rename")

	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySessionSQL(t, executor, session, "app", "select id from after_rename"))
	state := getTemporaryTableSessionState(session, false)
	require.NotNil(t, state)
	state.mu.RLock()
	binding, exists := state.tables[temporaryTableKey("app", "after_rename")]
	_, oldExists := state.tables[temporaryTableKey("app", "before_rename")]
	state.mu.RUnlock()
	require.True(t, exists)
	require.False(t, oldExists)
	require.True(t, strings.HasPrefix(binding.Physical, "__xmysql_tmp_"))

	mustExecSessionSQL(t, executor, session, "app", "rename table after_rename to final_rename")
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySessionSQL(t, executor, session, "app", "select id from final_rename"))
}

func TestTemporaryTableSupportsDroppingMultipleTables(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create temporary table first_tmp (id int)")
	mustExecSessionSQL(t, executor, session, "app", "create temporary table second_tmp (id int)")
	mustExecSessionSQL(t, executor, session, "app", "drop temporary table first_tmp, second_tmp")
	mustExecSessionSQL(t, executor, session, "app", "drop temporary table if exists first_tmp, second_tmp")

	require.Empty(t, mustQuerySessionSQL(t, executor, session, "app", "select table_name from information_schema.tables where table_schema = 'app' and table_name in ('first_tmp', 'second_tmp')"))
}

func TestTemporaryTableSupportsTruncate(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create temporary table truncate_rows (id int)")
	mustExecSessionSQL(t, executor, session, "app", "insert into truncate_rows values (1), (2)")
	mustExecSessionSQL(t, executor, session, "app", "truncate table truncate_rows")

	require.Empty(t, mustQuerySessionSQL(t, executor, session, "app", "select id from truncate_rows"))
}

func TestInformationSchemaTablesSelectReturnsJDBCMetadataColumns(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)

	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")

	got := <-executor.ExecuteQuery(nil,
		"select TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS from information_schema.tables where table_schema = 'app' and table_name = 'users'",
		"app",
	)
	require.NoError(t, got.Err)

	result, ok := got.Data.(*SelectResult)
	require.True(t, ok, "expected SelectResult, got %T", got.Data)
	require.Equal(t, []string{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS"}, result.Columns)
	require.Len(t, result.Records, 1)

	values := result.Records[0].GetValues()
	require.Len(t, values, 5)
	require.Equal(t, "app", values[0].ToString())
	require.Nil(t, values[1].Raw())
	require.Equal(t, "users", values[2].ToString())
	require.Equal(t, "TABLE", values[3].ToString())
	require.Equal(t, "", values[4].ToString())
}

func TestInformationSchemaTablesSelectReturnsRequestedNativeColumnsAndRowCount(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1), (2)")

	got := <-executor.ExecuteQuery(nil, "select table_schema, table_name, engine, table_rows from information_schema.tables where table_schema = 'app' and table_name = 'users'", "app")
	require.NoError(t, got.Err)
	result, ok := got.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, []string{"TABLE_SCHEMA", "TABLE_NAME", "ENGINE", "TABLE_ROWS"}, result.Columns)
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	require.Equal(t, "app", values[0].ToString())
	require.Equal(t, "users", values[1].ToString())
	require.Equal(t, "InnoDB", values[2].ToString())
	require.Equal(t, int64(2), values[3].Int())
}

func TestInformationSchemaCheckConstraintsReturnsPersistedDefinitions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table checks (id int primary key, amount int)")
	mustExecSQL(t, executor, "app", "alter table checks add constraint chk_amount check (amount > 0)")

	got := <-executor.ExecuteQuery(nil, "select constraint_schema, constraint_name, table_name, check_clause, enforced from information_schema.check_constraints where constraint_schema = 'app' and table_name = 'checks' and constraint_name = 'chk_amount'", "app")
	require.NoError(t, got.Err)
	result, ok := got.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, []string{"CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_NAME", "CHECK_CLAUSE", "ENFORCED"}, result.Columns)
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	require.Equal(t, "app", values[0].ToString())
	require.Equal(t, "chk_amount", values[1].ToString())
	require.Equal(t, "checks", values[2].ToString())
	require.Equal(t, "amount > 0", values[3].ToString())
	require.Equal(t, "YES", values[4].ToString())
}

func TestInformationSchemaJDBCProbeTablesReturnEmptyMetadataResults(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)

	cases := []struct {
		name         string
		query        string
		columns      []string
		expectedRows int
	}{
		{
			name:    "procedures",
			query:   "SELECT ROUTINE_SCHEMA AS PROCEDURE_CAT, NULL AS PROCEDURE_SCHEM, ROUTINE_NAME AS PROCEDURE_NAME, NULL AS RESERVED_1, NULL AS RESERVED_2, NULL AS RESERVED_3, ROUTINE_COMMENT AS REMARKS, CASE WHEN ROUTINE_TYPE = 'PROCEDURE' THEN 1 ELSE 0 END AS PROCEDURE_TYPE, ROUTINE_NAME AS SPECIFIC_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME LIKE '%'",
			columns: []string{"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "RESERVED_1", "RESERVED_2", "RESERVED_3", "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME"},
		},
		{
			name:    "functions",
			query:   "SELECT ROUTINE_SCHEMA AS FUNCTION_CAT, NULL AS FUNCTION_SCHEM, ROUTINE_NAME AS FUNCTION_NAME, ROUTINE_COMMENT AS REMARKS, CASE WHEN ROUTINE_TYPE = 'FUNCTION' THEN 1 ELSE 0 END AS FUNCTION_TYPE, ROUTINE_NAME AS SPECIFIC_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME LIKE '%'",
			columns: []string{"FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME"},
		},
		{
			name:    "primary keys",
			query:   "SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, SEQ_IN_INDEX AS KEY_SEQ, 'PRIMARY' AS PK_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_NAME = 'missing' AND INDEX_NAME='PRIMARY'",
			columns: []string{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"},
		},
		{
			name:    "indexes",
			query:   "SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, NON_UNIQUE, NULL AS INDEX_QUALIFIER, INDEX_NAME, 3 AS TYPE, SEQ_IN_INDEX AS ORDINAL_POSITION, COLUMN_NAME, COLLATION AS ASC_OR_DESC, CARDINALITY, 0 AS PAGES, NULL AS FILTER_CONDITION FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_NAME = 'missing'",
			columns: []string{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES", "FILTER_CONDITION"},
		},
		{
			name:    "views",
			query:   "select table_name, view_definition, definer from information_schema.views where table_schema = 'performance_schema'",
			columns: []string{"TABLE_NAME", "VIEW_DEFINITION", "DEFINER"},
		},
		{
			name:    "partitions",
			query:   "select table_name, partition_name, subpartition_name, partition_ordinal_position, subpartition_ordinal_position, partition_method, subpartition_method, partition_expression, subpartition_expression, partition_description, table_rows, avg_row_length, data_length, max_data_length, index_length, data_free, create_time, update_time, check_time, checksum, partition_comment, nodegroup, tablespace_name from information_schema.partitions where table_schema = 'performance_schema'",
			columns: []string{"TABLE_NAME", "PARTITION_NAME", "SUBPARTITION_NAME", "PARTITION_ORDINAL_POSITION", "SUBPARTITION_ORDINAL_POSITION", "PARTITION_METHOD", "SUBPARTITION_METHOD", "PARTITION_EXPRESSION", "SUBPARTITION_EXPRESSION", "PARTITION_DESCRIPTION", "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE", "CREATE_TIME", "UPDATE_TIME", "CHECK_TIME", "CHECKSUM", "PARTITION_COMMENT", "NODEGROUP", "TABLESPACE_NAME"},
		},
		{
			name:    "triggers",
			query:   "select trigger_name, event_manipulation, event_object_table, action_statement, action_timing, definer from information_schema.triggers where trigger_schema = 'performance_schema'",
			columns: []string{"TRIGGER_NAME", "EVENT_MANIPULATION", "EVENT_OBJECT_TABLE", "ACTION_STATEMENT", "ACTION_TIMING", "DEFINER"},
		},
		{
			name:    "events",
			query:   "select event_name, event_definition, event_type, execute_at, interval_value, interval_field, status, definer from information_schema.events where event_schema = 'performance_schema'",
			columns: []string{"EVENT_NAME", "EVENT_DEFINITION", "EVENT_TYPE", "EXECUTE_AT", "INTERVAL_VALUE", "INTERVAL_FIELD", "STATUS", "DEFINER"},
		},
		{
			name:         "collations",
			query:        "select collation_name, character_set_name, is_default from information_schema.collations",
			columns:      []string{"COLLATION_NAME", "CHARACTER_SET_NAME", "IS_DEFAULT"},
			expectedRows: 4,
		},
		{
			name:    "user privileges",
			query:   "select grantee, privilege_type, is_grantable from information_schema.user_privileges",
			columns: []string{"GRANTEE", "PRIVILEGE_TYPE", "IS_GRANTABLE"},
		},
		{
			name:    "schema privileges",
			query:   "select grantee, table_schema, privilege_type, is_grantable from information_schema.schema_privileges",
			columns: []string{"GRANTEE", "TABLE_SCHEMA", "PRIVILEGE_TYPE", "IS_GRANTABLE"},
		},
		{
			name:    "mysql procs priv",
			query:   "select Host, User, Routine_name, Proc_priv, Routine_type = 'PROCEDURE' as is_proc from mysql.procs_priv where Db = 'performance_schema'",
			columns: []string{"HOST", "USER", "ROUTINE_NAME", "PROC_PRIV", "IS_PROC"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := <-executor.ExecuteQuery(nil, tc.query, "")
			require.NoError(t, got.Err)

			result, ok := got.Data.(*SelectResult)
			require.True(t, ok, "expected SelectResult, got %T", got.Data)
			require.Equal(t, tc.columns, result.Columns)
			if tc.expectedRows > 0 {
				require.Len(t, result.Records, tc.expectedRows)
			} else {
				require.Empty(t, result.Records)
			}
		})
	}
}

func TestInformationSchemaCharacterSetsAndEnginesExposeCommonRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	characterSets := mustQuerySQL(t, executor, "", "select character_set_name, default_collate_name, maxlen from information_schema.character_sets where character_set_name = 'utf8mb4'")
	require.Equal(t, [][]interface{}{{"utf8mb4", "utf8mb4_0900_ai_ci", "4"}}, characterSets)
	engines := mustQuerySQL(t, executor, "", "select engine, support, transactions from information_schema.engines where engine = 'InnoDB'")
	require.Equal(t, [][]interface{}{{"InnoDB", "DEFAULT", "YES"}}, engines)
}

func TestInformationSchemaEnginesAppliesNativeFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	require.Empty(t, mustQuerySQL(t, executor, "", "select engine from information_schema.engines where engine = 'MissingEngine'"))
	require.Equal(t, [][]interface{}{{"InnoDB", "YES"}}, mustQuerySQL(t, executor, "", "select engine, xa from information_schema.engines where support = 'DEFAULT' and transactions = 'YES' and xa = 'YES'"))
	require.Empty(t, mustQuerySQL(t, executor, "", "select engine from information_schema.engines where savepoints = 'NO'"))
}

func TestInformationSchemaPrivilegesUnionAllReturnsEmptyResult(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)

	query := "select grantee, table_name, column_name, privilege_type, is_grantable from information_schema.column_privileges where table_schema = 'performance_schema' union all select grantee, table_name, null as column_name, privilege_type, is_grantable from information_schema.table_privileges where table_schema = 'performance_schema'"
	got := <-executor.ExecuteQuery(nil, query, "")
	require.NoError(t, got.Err)

	result, ok := got.Data.(*SelectResult)
	require.True(t, ok, "expected SelectResult, got %T", got.Data)
	require.Equal(t, []string{"GRANTEE", "TABLE_NAME", "COLUMN_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE"}, result.Columns)
	require.Empty(t, result.Records)
}

func TestInformationSchemaTablesAutoIncrementProjectionReturnsRequestedColumns(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)

	got := <-executor.ExecuteQuery(nil,
		"select table_name, auto_increment from information_schema.tables where table_schema = 'performance_schema' and auto_increment is not null",
		"performance_schema",
	)
	require.NoError(t, got.Err)

	result, ok := got.Data.(*SelectResult)
	require.True(t, ok, "expected SelectResult, got %T", got.Data)
	require.Equal(t, []string{"TABLE_NAME", "AUTO_INCREMENT"}, result.Columns)
	require.Empty(t, result.Records)
}

func TestShowFullTablesReturnsTableTypeColumn(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	session := newTestMySQLSession()

	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "use app")
	mustExecSessionSQL(t, executor, session, "app", "create table users (id int primary key)")

	got := <-executor.ExecuteQuery(session, "show full tables from app like 'users'", "app")
	require.NoError(t, got.Err)
	data, ok := got.Data.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, []string{"Tables_in_app", "Table_type"}, data["columns"])
	require.Equal(t, [][]interface{}{{"users", "BASE TABLE"}}, data["rows"])
}

func TestDropTableUsesCurrentDatabaseContext(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)

	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "drop table users")

	_, err := os.Stat(filepath.Join(tmp, "app", "users.frm"))
	require.True(t, os.IsNotExist(err))
}

func TestTransactionRollbackRestoresDMLChanges(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	session := newTestMySQLSession()

	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table accounts (id int primary key auto_increment, account_name varchar(50) not null)")

	mustExecSessionSQL(t, executor, session, "app", "begin")
	mustExecSessionSQL(t, executor, session, "app", "insert into accounts (account_name) values ('Alice')")
	mustExecSessionSQL(t, executor, session, "app", "rollback")

	rows := mustQuerySessionSQL(t, executor, session, "app", "select account_name from accounts")
	require.Empty(t, rows)
}

func TestTransactionRollbackToSavepointRestoresPartialDMLChanges(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	session := newTestMySQLSession()

	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table accounts (id int primary key auto_increment, account_name varchar(50) not null)")

	mustExecSessionSQL(t, executor, session, "app", "begin")
	mustExecSessionSQL(t, executor, session, "app", "insert into accounts (account_name) values ('Alice')")
	mustExecSessionSQL(t, executor, session, "app", "savepoint sp1")
	mustExecSessionSQL(t, executor, session, "app", "insert into accounts (account_name) values ('Bob')")
	mustExecSessionSQL(t, executor, session, "app", "rollback to savepoint sp1")
	mustExecSessionSQL(t, executor, session, "app", "insert into accounts (account_name) values ('Charlie')")
	mustExecSessionSQL(t, executor, session, "app", "commit")

	rows := mustQuerySessionSQL(t, executor, session, "app", "select account_name from accounts order by account_name")
	require.Equal(t, [][]interface{}{{"Alice"}, {"Charlie"}}, rows)
}

func TestTransactionRollbackAfterSetAutocommitOffRestoresDMLChanges(t *testing.T) {
	tmp := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, tmp)
	session := newTestMySQLSession()

	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table accounts (id int primary key auto_increment, account_name varchar(50) not null)")

	mustExecSessionSQL(t, executor, session, "app", "set autocommit=0")
	mustExecSessionSQL(t, executor, session, "app", "insert into accounts (account_name) values ('Alice')")
	mustExecSessionSQL(t, executor, session, "app", "rollback")

	rows := mustQuerySessionSQL(t, executor, session, "app", "select account_name from accounts")
	require.Empty(t, rows)
}

func newTestStorageIntegratedExecutor(t *testing.T, dataDir string) *XMySQLEngine {
	t.Helper()
	executor := NewXMySQLEngine(&conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        dataDir,
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	})
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	return executor
}

func mustExecSQL(t *testing.T, executor *XMySQLEngine, databaseName, sql string) {
	t.Helper()
	mustExecSessionSQL(t, executor, nil, databaseName, sql)
}

func mustExecSessionSQL(t *testing.T, executor *XMySQLEngine, session server.MySQLServerSession, databaseName, sql string) {
	t.Helper()
	got := <-executor.ExecuteQuery(session, sql, databaseName)
	require.NoError(t, got.Err)
}

func mustQuerySQL(t *testing.T, executor *XMySQLEngine, databaseName, sql string) [][]interface{} {
	t.Helper()
	return mustQuerySessionSQL(t, executor, nil, databaseName, sql)
}

func mustSelectResultSQL(t *testing.T, executor *XMySQLEngine, databaseName, sql string) *SelectResult {
	t.Helper()
	got := <-executor.ExecuteQuery(nil, sql, databaseName)
	require.NoError(t, got.Err)
	result, ok := got.Data.(*SelectResult)
	require.True(t, ok, "expected SelectResult, got %T", got.Data)
	return result
}

func mustQuerySessionSQL(t *testing.T, executor *XMySQLEngine, session server.MySQLServerSession, databaseName, sql string) [][]interface{} {
	t.Helper()
	got := <-executor.ExecuteQuery(session, sql, databaseName)
	require.NoError(t, got.Err)

	result, ok := got.Data.(*SelectResult)
	require.True(t, ok, "expected SelectResult, got %T", got.Data)
	rows := make([][]interface{}, len(result.Records))
	for i, record := range result.Records {
		values := record.GetValues()
		rows[i] = make([]interface{}, len(values))
		for j, value := range values {
			raw := value.Raw()
			if bytes, ok := raw.([]byte); ok {
				rows[i][j] = string(bytes)
				continue
			}
			rows[i][j] = raw
		}
	}
	return rows
}

func readFrmColumns(t *testing.T, frmPath string) map[string]map[string]interface{} {
	t.Helper()
	raw, err := os.ReadFile(frmPath)
	require.NoError(t, err)

	var tableInfo struct {
		Columns []map[string]interface{} `json:"columns"`
	}
	require.NoError(t, json.Unmarshal(raw, &tableInfo))

	columns := make(map[string]map[string]interface{}, len(tableInfo.Columns))
	for _, column := range tableInfo.Columns {
		columns[column["name"].(string)] = column
	}
	return columns
}

func readFrmIndexes(t *testing.T, frmPath string) map[string]map[string]interface{} {
	t.Helper()
	raw, err := os.ReadFile(frmPath)
	require.NoError(t, err)

	var tableInfo struct {
		Indexes []map[string]interface{} `json:"indexes"`
	}
	require.NoError(t, json.Unmarshal(raw, &tableInfo))

	indexes := make(map[string]map[string]interface{}, len(tableInfo.Indexes))
	for _, index := range tableInfo.Indexes {
		indexes[index["name"].(string)] = index
	}
	return indexes
}
