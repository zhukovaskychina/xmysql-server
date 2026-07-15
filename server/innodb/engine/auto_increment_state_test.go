package engine

import (
	"testing"

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
