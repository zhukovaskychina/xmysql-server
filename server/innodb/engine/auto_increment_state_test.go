package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestAutoIncrementStatePersistsAcrossExecutors(t *testing.T) {
	dataDir := t.TempDir()
	meta := autoIncrementTestTableMeta()

	firstExecutor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	firstExecutor.SetDataDir(dataDir)
	firstExecutor.schemaName = "app"
	firstExecutor.tableName = "users"

	first := &InsertRowData{ColumnValues: map[string]interface{}{}, ColumnTypes: map[string]metadata.DataType{}}
	firstPK, err := firstExecutor.generatePrimaryKey(first, meta)
	if err != nil {
		t.Fatalf("generatePrimaryKey(first) error = %v", err)
	}
	second := &InsertRowData{ColumnValues: map[string]interface{}{}, ColumnTypes: map[string]metadata.DataType{}}
	secondPK, err := firstExecutor.generatePrimaryKey(second, meta)
	if err != nil {
		t.Fatalf("generatePrimaryKey(second) error = %v", err)
	}
	if firstPK != int64(1) || secondPK != int64(2) {
		t.Fatalf("allocated keys = %#v, %#v, want int64(1), int64(2)", firstPK, secondPK)
	}

	reloadedExecutor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	reloadedExecutor.SetDataDir(dataDir)
	reloadedExecutor.schemaName = "app"
	reloadedExecutor.tableName = "users"

	third := &InsertRowData{ColumnValues: map[string]interface{}{}, ColumnTypes: map[string]metadata.DataType{}}
	thirdPK, err := reloadedExecutor.generatePrimaryKey(third, meta)
	if err != nil {
		t.Fatalf("generatePrimaryKey(third) error = %v", err)
	}
	if thirdPK != int64(3) {
		t.Fatalf("reloaded allocated key = %#v, want int64(3)", thirdPK)
	}
}

func TestAutoIncrementStateExplicitValueAdvancesNextAllocation(t *testing.T) {
	dataDir := t.TempDir()
	meta := autoIncrementTestTableMeta()
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	executor.SetDataDir(dataDir)
	executor.schemaName = "app"
	executor.tableName = "users"

	explicit := &InsertRowData{
		ColumnValues: map[string]interface{}{"id": int64(41)},
		ColumnTypes:  map[string]metadata.DataType{"id": metadata.TypeInt},
	}
	if pk, err := executor.generatePrimaryKey(explicit, meta); err != nil || pk != int64(41) {
		t.Fatalf("generatePrimaryKey(explicit) = %#v, %v; want int64(41), nil", pk, err)
	}

	next := &InsertRowData{ColumnValues: map[string]interface{}{}, ColumnTypes: map[string]metadata.DataType{}}
	nextPK, err := executor.generatePrimaryKey(next, meta)
	if err != nil {
		t.Fatalf("generatePrimaryKey(next) error = %v", err)
	}
	if nextPK != int64(42) {
		t.Fatalf("next allocated key = %#v, want int64(42)", nextPK)
	}
}

func TestAlterTableAutoIncrementSetsNextGeneratedValue(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key auto_increment, name varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users (name) values ('before')")
	mustExecSQL(t, executor, "app", "alter table users auto_increment=100")
	mustExecSQL(t, executor, "app", "insert into users (name) values ('after')")
	if got := mustQuerySQL(t, executor, "app", "select id, name from users order by id"); len(got) != 2 || got[1][0] != "100" {
		t.Fatalf("rows after ALTER TABLE AUTO_INCREMENT = %#v, want second id 100", got)
	}
}

func TestCreateTableAutoIncrementOptionSetsNextGeneratedValue(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key auto_increment, name varchar(20)) auto_increment=100")
	assertInformationSchemaAutoIncrement(t, executor, "app", "users", 100)
	mustExecSQL(t, executor, "app", "insert into users (name) values ('first')")
	assertInformationSchemaAutoIncrement(t, executor, "app", "users", 101)
	require.Equal(t, [][]interface{}{{"100", "first"}}, mustQuerySQL(t, executor, "app", "select id, name from users"))
}

func assertInformationSchemaAutoIncrement(t *testing.T, executor *XMySQLEngine, schemaName, tableName string, expected int64) {
	t.Helper()
	got := <-executor.ExecuteQuery(nil, "select auto_increment from information_schema.tables where table_schema = '"+schemaName+"' and table_name = '"+tableName+"'", schemaName)
	require.NoError(t, got.Err)
	result, ok := got.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, result.Records, 1)
	require.Equal(t, expected, result.Records[0].GetValueByIndex(0).Int())
}

func autoIncrementTestTableMeta() *metadata.TableMeta {
	return &metadata.TableMeta{
		Name: "users",
		Columns: []*metadata.ColumnMeta{
			{Name: "id", Type: metadata.TypeInt, IsPrimary: true, IsAutoIncrement: true},
			{Name: "name", Type: metadata.TypeVarchar},
		},
		PrimaryKey: []string{"id"},
	}
}
