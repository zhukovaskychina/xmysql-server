package engine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
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
