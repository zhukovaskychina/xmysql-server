package engine

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestClusteredRecordPersistsAcrossStorageManagerRestart(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	tableMeta := &metadata.TableMeta{
		Name: "users",
		Columns: []*metadata.ColumnMeta{
			{Name: "id", Type: metadata.TypeInt, IsPrimary: true},
			{Name: "name", Type: metadata.TypeVarchar},
		},
		PrimaryKey: []string{"id"},
	}

	firstStorage := newRestartTestStorageManager(dataDir)
	firstTableStorage := manager.NewTableStorageManager(firstStorage)
	_, err := firstStorage.CreateTablespace("mysql/user")
	if err != nil {
		t.Fatalf("CreateTablespace(first) error = %v", err)
	}
	firstBTree, err := firstTableStorage.CreateBTreeManagerForTable(ctx, "mysql", "user")
	if err != nil {
		t.Fatalf("CreateBTreeManagerForTable(first) error = %v", err)
	}
	firstInfo, err := firstTableStorage.GetTableStorageInfo("mysql", "user")
	if err != nil {
		t.Fatalf("GetTableStorageInfo(first) error = %v", err)
	}
	row := &InsertRowData{ColumnValues: map[string]interface{}{"id": int64(1), "name": "alice"}}
	encoded, err := EncodeClusteredRecord(row, tableMeta)
	if err != nil {
		t.Fatalf("EncodeClusteredRecord() error = %v", err)
	}
	if err := firstBTree.Insert(ctx, int64(1), encoded); err != nil {
		t.Fatalf("Insert(first) error = %v", err)
	}
	firstRows, err := NewClusteredIndexScanner(firstBTree, tableMeta).Scan(ctx, nil)
	if err != nil {
		t.Fatalf("Scan(first) error = %v", err)
	}
	if len(firstRows) != 1 {
		t.Fatalf("first row count = %d, want 1", len(firstRows))
	}
	if err := firstStorage.Flush(); err != nil {
		t.Fatalf("Flush(first) error = %v", err)
	}
	if err := firstStorage.Close(); err != nil {
		t.Fatalf("Close(first) error = %v", err)
	}

	secondStorage := newRestartTestStorageManager(dataDir)
	secondTableStorage := manager.NewTableStorageManager(secondStorage)
	_, err = secondStorage.CreateTablespace("mysql/user")
	if err != nil {
		t.Fatalf("CreateTablespace(second) error = %v", err)
	}
	secondInfo, err := secondTableStorage.GetTableStorageInfo("mysql", "user")
	if err != nil {
		t.Fatalf("GetTableStorageInfo(second) error = %v", err)
	}
	secondInfo.RootPageNo = firstInfo.RootPageNo
	secondInfo.IndexPageNo = firstInfo.IndexPageNo
	secondBTree, err := secondTableStorage.CreateBTreeManagerForTable(ctx, "mysql", "user")
	if err != nil {
		t.Fatalf("CreateBTreeManagerForTable(second) error = %v", err)
	}

	scanner := NewClusteredIndexScanner(secondBTree, tableMeta)
	rows, err := scanner.Scan(ctx, nil)
	if err != nil {
		t.Fatalf("Scan(second) error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("persisted row count = %d, want 1", len(rows))
	}
	if rows[0].ColumnValues["name"] != "alice" {
		t.Fatalf("persisted row = %#v", rows[0].ColumnValues)
	}
}

func newRestartTestStorageManager(dataDir string) *manager.StorageManager {
	return manager.NewStorageManager(&conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        filepath.Join(dataDir, "innodb"),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	})
}
