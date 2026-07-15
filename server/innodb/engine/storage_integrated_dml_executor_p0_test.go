package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestStorageIntegratedDMLExecuteInsertMissingStorageMappingReturnsExecutionError(t *testing.T) {
	dml := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	stmt, err := sqlparser.Parse("insert into p0e_db.t1 values (1)")
	if err != nil {
		t.Fatalf("parse insert: %v", err)
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("ExecuteInsert should return ExecutionError instead of panic: %v", recovered)
		}
	}()

	_, err = dml.ExecuteInsert(context.Background(), stmt.(*sqlparser.Insert), "p0e_db")
	if err == nil {
		t.Fatal("expected missing storage mapping error")
	}
	var execErr *ExecutionError
	if !errors.As(err, &execErr) {
		t.Fatalf("expected ExecutionError, got %T: %v", err, err)
	}
	if execErr.ErrorCode != ExecutionErrorCodeStorageMissing {
		t.Fatalf("expected %s, got %s", ExecutionErrorCodeStorageMissing, execErr.ErrorCode)
	}
	if execErr.Schema != "p0e_db" || execErr.Table != "t1" {
		t.Fatalf("expected schema/table p0e_db.t1, got %s.%s", execErr.Schema, execErr.Table)
	}
}

func TestStorageIntegratedDMLGetTableMetadataFallsBackToFrm(t *testing.T) {
	dataDir := t.TempDir()
	dbDir := filepath.Join(dataDir, "p0e_db")
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		t.Fatalf("mkdir db dir: %v", err)
	}
	frm := `{"table_name":"t1","columns":[{"name":"id","type":"INT","length":11,"nullable":false,"primary":true,"unique":true,"auto_increment":true},{"name":"name","type":"VARCHAR","length":50,"nullable":true}]}`
	if err := os.WriteFile(filepath.Join(dbDir, "t1.frm"), []byte(frm), 0644); err != nil {
		t.Fatalf("write frm: %v", err)
	}

	dml := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	dml.SetDataDir(dataDir)
	dml.schemaName = "p0e_db"
	dml.tableName = "t1"

	meta, err := dml.getTableMetadata()
	if err != nil {
		t.Fatalf("getTableMetadata returned error: %v", err)
	}
	if len(meta.Columns) != 2 || meta.Columns[0].Name != "id" || meta.Columns[1].Name != "name" {
		t.Fatalf("unexpected metadata columns: %#v", meta.Columns)
	}
	if !meta.Columns[0].IsPrimary || !meta.Columns[0].IsAutoIncrement {
		t.Fatalf("expected id to be primary auto_increment, got %#v", meta.Columns[0])
	}
	if len(meta.PrimaryKey) != 1 || meta.PrimaryKey[0] != "id" {
		t.Fatalf("unexpected primary key metadata: %#v", meta.PrimaryKey)
	}
}

func TestStorageIntegratedDMLGeneratePrimaryKeyWritesAutoIncrementColumn(t *testing.T) {
	dml := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	row := &InsertRowData{
		ColumnValues: map[string]interface{}{"name": "alice"},
		ColumnTypes:  map[string]metadata.DataType{"name": metadata.TypeVarchar},
	}
	meta := &metadata.TableMeta{
		Name: "users",
		Columns: []*metadata.ColumnMeta{
			{Name: "id", Type: metadata.TypeInt, IsPrimary: true, IsAutoIncrement: true},
			{Name: "name", Type: metadata.TypeVarchar},
		},
	}

	pk, err := dml.generatePrimaryKey(row, meta)
	if err != nil {
		t.Fatalf("generatePrimaryKey returned error: %v", err)
	}
	if pk == nil || row.ColumnValues["id"] != pk {
		t.Fatalf("expected generated id to be written back, pk=%v row=%#v", pk, row.ColumnValues)
	}
	if row.ColumnTypes["id"] != metadata.TypeInt {
		t.Fatalf("expected id column type to be restored, got %v", row.ColumnTypes["id"])
	}
}

func TestStorageIntegratedDMLInsertFailsWhenBTreeManagerMissing(t *testing.T) {
	dml := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("insertRowToStorage should return an error instead of panicking: %v", recovered)
		}
	}()

	_, err := dml.insertRowToStorage(
		context.Background(),
		nil,
		&InsertRowData{ColumnValues: map[string]interface{}{"id": int64(1)}},
		nil,
		&manager.TableStorageInfo{SpaceID: 1, RootPageNo: 1},
		nil,
	)
	if err == nil {
		t.Fatal("expected missing btree manager error")
	}
	if !strings.Contains(err.Error(), "B+树管理器") {
		t.Fatalf("expected B+ tree manager error, got %v", err)
	}
}

func TestXMySQLExecutorDMLRejectsMissingStorageIntegratedManagers(t *testing.T) {
	txManager, err := manager.NewTransactionManager(
		filepath.Join(t.TempDir(), "redo"),
		filepath.Join(t.TempDir(), "undo"),
	)
	if err != nil {
		t.Fatalf("create transaction manager: %v", err)
	}
	storageManager := &manager.StorageManager{}
	storageManager.SetTransactionManager(txManager)

	executor := NewXMySQLExecutor(nil, nil)
	executor.SetTransactionManager(txManager)
	executor.SetAdditionalManagers(nil, storageManager, nil)
	ctx := &ExecutionContext{Context: context.Background(), DatabaseName: "testdb"}

	cases := []struct {
		name string
		sql  string
		run  func(*ExecutionContext, interface{}) (*DMLResult, error)
	}{
		{
			name: "insert",
			sql:  "insert into users(id, name) values (1, 'alice')",
			run: func(ctx *ExecutionContext, parsed interface{}) (*DMLResult, error) {
				return executor.executeInsertStatement(ctx, parsed.(*sqlparser.Insert), "testdb")
			},
		},
		{
			name: "update",
			sql:  "update users set name = 'bob' where id = 1",
			run: func(ctx *ExecutionContext, parsed interface{}) (*DMLResult, error) {
				return executor.executeUpdateStatement(ctx, parsed.(*sqlparser.Update), "testdb")
			},
		},
		{
			name: "delete",
			sql:  "delete from users where id = 1",
			run: func(ctx *ExecutionContext, parsed interface{}) (*DMLResult, error) {
				return executor.executeDeleteStatement(ctx, parsed.(*sqlparser.Delete), "testdb")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := sqlparser.Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse %s: %v", tc.name, err)
			}

			_, err = tc.run(ctx, stmt)
			if err == nil {
				t.Fatalf("expected missing storage-integrated managers error")
			}
			var execErr *ExecutionError
			if !errors.As(err, &execErr) {
				t.Fatalf("expected ExecutionError, got %T: %v", err, err)
			}
			if execErr.ErrorCode != ExecutionErrorCodeStorageMissing {
				t.Fatalf("expected %s, got %s: %v", ExecutionErrorCodeStorageMissing, execErr.ErrorCode, err)
			}
			if !strings.Contains(err.Error(), "storage-integrated DML requires") {
				t.Fatalf("expected storage-integrated DML manager error, got %v", err)
			}
		})
	}
}
