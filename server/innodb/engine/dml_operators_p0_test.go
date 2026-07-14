package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

type p0ScanOperator struct {
	BaseOperator
	records []Record
	index   int
}

func (s *p0ScanOperator) Open(ctx context.Context) error {
	s.opened = true
	return nil
}

func (s *p0ScanOperator) Next(ctx context.Context) (Record, error) {
	if s.index >= len(s.records) {
		return nil, nil
	}
	record := s.records[s.index]
	s.index++
	return record, nil
}

func p0UsersSchema() *metadata.Table {
	schema := metadata.NewTable("users")
	schema.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeBigInt})
	schema.AddColumn(&metadata.Column{Name: "name", DataType: metadata.TypeVarchar, IsNullable: true})
	_ = schema.AddIndex(&metadata.Index{Name: "PRIMARY", Columns: []string{"id"}, IsPrimary: true, IsUnique: true})
	return schema
}

func p0UserRecord(id int64, name string) Record {
	return NewExecutorRecordFromValues([]basic.Value{
		basic.NewInt64Value(id),
		basic.NewStringValue(name),
	}, metadata.FromTable(p0UsersSchema()))
}

func TestUpdateOperatorApplySetClauseChangesTargetColumn(t *testing.T) {
	operator := &UpdateOperator{
		stmt: &sqlparser.Update{
			Exprs: sqlparser.UpdateExprs{
				{
					Name: &sqlparser.ColName{Name: sqlparser.NewColIdent("name")},
					Expr: sqlparser.NewStrVal([]byte("bob")),
				},
			},
		},
	}
	schema := metadata.NewTable("users")
	schema.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeBigInt})
	schema.AddColumn(&metadata.Column{Name: "name", DataType: metadata.TypeVarchar})

	oldRecord := NewExecutorRecordFromValues([]basic.Value{
		basic.NewInt64Value(1),
		basic.NewStringValue("alice"),
	}, nil)

	newRecord, err := operator.applySetClause(oldRecord, schema)
	if err != nil {
		t.Fatalf("apply set clause: %v", err)
	}

	values := newRecord.GetValues()
	if got := string(values[1].Bytes()); got != "bob" {
		t.Fatalf("expected name to be updated to bob, got %v", got)
	}
	if got := values[0].ToString(); got != oldRecord.GetValues()[0].ToString() {
		t.Fatalf("expected id to remain unchanged, got %v", got)
	}
}

func TestInsertOperatorInsertRowWritesThroughStorageAdapter(t *testing.T) {
	var inserted []map[string]interface{}
	adapter := &StorageAdapter{
		insertRecordFunc: func(ctx context.Context, schemaName, tableName string, row map[string]interface{}, schema *metadata.Table, txn *Transaction) error {
			inserted = append(inserted, row)
			return nil
		},
	}
	operator := &InsertOperator{
		schemaName:     "test",
		tableName:      "users",
		storageAdapter: adapter,
	}

	err := operator.insertRow(context.Background(), &Transaction{TxnID: 7}, map[string]interface{}{"id": int64(1), "name": "alice"}, p0UsersSchema())
	if err != nil {
		t.Fatalf("insert row: %v", err)
	}
	if len(inserted) != 1 {
		t.Fatalf("expected one storage insert, got %d", len(inserted))
	}
	if inserted[0]["id"] != int64(1) || inserted[0]["name"] != "alice" {
		t.Fatalf("unexpected inserted row: %#v", inserted[0])
	}
}

func TestInsertOperatorDuplicateKeyReturnsError(t *testing.T) {
	adapter := &StorageAdapter{
		insertRecordFunc: func(ctx context.Context, schemaName, tableName string, row map[string]interface{}, schema *metadata.Table, txn *Transaction) error {
			return basic.ErrDuplicateKey
		},
	}
	operator := &InsertOperator{
		schemaName:     "test",
		tableName:      "users",
		storageAdapter: adapter,
	}

	err := operator.insertRow(context.Background(), &Transaction{TxnID: 7}, map[string]interface{}{"id": int64(1)}, p0UsersSchema())
	if !errors.Is(err, basic.ErrDuplicateKey) {
		t.Fatalf("expected duplicate key error, got %v", err)
	}
}

func TestInsertOperatorFindDuplicateRecordUsesStorageAdapter(t *testing.T) {
	existing := p0UserRecord(1, "alice")
	adapter := &StorageAdapter{
		findDuplicateRecordFunc: func(ctx context.Context, schemaName, tableName string, row map[string]interface{}, schema *metadata.Table, txn *Transaction) (Record, error) {
			return existing, nil
		},
	}
	operator := &InsertOperator{
		schemaName:     "test",
		tableName:      "users",
		storageAdapter: adapter,
	}

	record, err := operator.findDuplicateRecord(context.Background(), &Transaction{TxnID: 7}, map[string]interface{}{"id": int64(1)}, p0UsersSchema())
	if err != nil {
		t.Fatalf("find duplicate: %v", err)
	}
	if record != existing {
		t.Fatalf("expected duplicate record from adapter")
	}
}

func TestUpdateOperatorPersistsUpdatedRecord(t *testing.T) {
	var oldRecord Record
	var newRecord Record
	adapter := &StorageAdapter{
		updateRecordFunc: func(ctx context.Context, schemaName, tableName string, oldRec Record, newRec Record, schema *metadata.Table, txn *Transaction) error {
			oldRecord = oldRec
			newRecord = newRec
			return nil
		},
	}
	operator := &UpdateOperator{
		schemaName:     "test",
		tableName:      "users",
		storageAdapter: adapter,
	}

	err := operator.updateInPlace(context.Background(), &Transaction{TxnID: 7}, p0UserRecord(1, "alice"), p0UserRecord(1, "bob"), p0UsersSchema())
	if err != nil {
		t.Fatalf("update in place: %v", err)
	}
	if oldRecord == nil || newRecord == nil {
		t.Fatalf("expected update to be persisted through storage adapter")
	}
	if got := string(newRecord.GetValues()[1].Bytes()); got != "bob" {
		t.Fatalf("expected persisted name bob, got %q", got)
	}
}

func TestDeleteOperatorDeletesOnlyScannedRecords(t *testing.T) {
	var deleted []Record
	adapter := &StorageAdapter{
		deleteRecordFunc: func(ctx context.Context, schemaName, tableName string, record Record, schema *metadata.Table, txn *Transaction) error {
			deleted = append(deleted, record)
			return nil
		},
	}
	operator := &DeleteOperator{
		schemaName:     "test",
		tableName:      "users",
		storageAdapter: adapter,
		scanOperator: &p0ScanOperator{
			records: []Record{p0UserRecord(1, "alice")},
		},
	}

	affected, err := operator.executeDelete(context.Background(), &Transaction{TxnID: 7})
	if err != nil {
		t.Fatalf("execute delete: %v", err)
	}
	if affected != 1 || len(deleted) != 1 {
		t.Fatalf("expected one deleted row, affected=%d deleted=%d", affected, len(deleted))
	}
	if got := deleted[0].GetValues()[0].ToString(); got != p0UserRecord(1, "alice").GetValues()[0].ToString() {
		t.Fatalf("deleted wrong record")
	}
}
