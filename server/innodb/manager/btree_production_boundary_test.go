package manager

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

func TestTableStorageManagerCreatesEnhancedBTreeForUserTables(t *testing.T) {
	ctx := context.Background()
	storage := newProductionBoundaryTestStorageManager(t)
	tableStorage := NewTableStorageManager(storage)
	handle, err := storage.CreateTablespace("app/users")
	if err != nil {
		t.Fatalf("CreateTablespace() error = %v", err)
	}
	if err := tableStorage.RegisterTable(ctx, &TableStorageInfo{
		SchemaName:    "app",
		TableName:     "users",
		SpaceID:       handle.SpaceID,
		RootPageNo:    3,
		IndexPageNo:   3,
		DataSegmentID: handle.DataSegmentID,
		Type:          TableTypeUser,
	}); err != nil {
		t.Fatalf("RegisterTable() error = %v", err)
	}

	btree, err := tableStorage.CreateBTreeManagerForTable(ctx, "app", "users")
	if err != nil {
		t.Fatalf("CreateBTreeManagerForTable() error = %v", err)
	}
	if _, ok := btree.(*EnhancedBTreeAdapter); !ok {
		t.Fatalf("user table B+Tree type = %T, want *EnhancedBTreeAdapter", btree)
	}
	if _, ok := btree.(*DefaultBPlusTreeManager); ok {
		t.Fatalf("user table B+Tree must not use legacy DefaultBPlusTreeManager")
	}
}

func TestIndexManagerWithoutStorageDoesNotCreateLegacyBTreeManager(t *testing.T) {
	indexManager := NewIndexManager(nil, nil, nil)
	if _, ok := indexManager.btreeManager.(*DefaultBPlusTreeManager); ok {
		t.Fatalf("NewIndexManager without storage must not create legacy DefaultBPlusTreeManager")
	}
	if indexManager.btreeManager != nil {
		t.Fatalf("NewIndexManager without storage btreeManager = %T, want nil", indexManager.btreeManager)
	}
}

func TestIndexManagerWithStorageUsesEnhancedBTreeAdapter(t *testing.T) {
	storage := newProductionBoundaryTestStorageManager(t)
	indexManager := NewIndexManagerWithStorage(nil, storage.GetBufferPoolManager(), storage, nil)
	if _, ok := indexManager.btreeManager.(*EnhancedBTreeAdapter); !ok {
		t.Fatalf("NewIndexManagerWithStorage btreeManager = %T, want *EnhancedBTreeAdapter", indexManager.btreeManager)
	}
	if _, ok := indexManager.btreeManager.(*DefaultBPlusTreeManager); ok {
		t.Fatalf("NewIndexManagerWithStorage must not use legacy DefaultBPlusTreeManager")
	}
}

func TestIndexManagerWithoutBTreeReturnsUnavailableError(t *testing.T) {
	indexManager := &IndexManager{
		indexes: map[uint64]*Index{
			1: {
				IndexID: 1,
				TableID: 1,
				State:   IndexStateActive,
			},
		},
		stats:  &IndexManagerStats{},
		config: &IndexManagerConfig{},
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("InsertKey() panicked with missing B+Tree manager: %v", recovered)
		}
	}()
	if err := indexManager.InsertKey(1, int64(1), []byte("value")); err == nil || !strings.Contains(err.Error(), "B+tree manager unavailable") {
		t.Fatalf("InsertKey() error = %v, want B+tree manager unavailable", err)
	}
}

func newProductionBoundaryTestStorageManager(t *testing.T) *StorageManager {
	t.Helper()
	dataDir := t.TempDir()
	storage := NewStorageManager(&conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        filepath.Join(dataDir, "innodb"),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	})
	t.Cleanup(func() {
		_ = storage.Close()
	})
	return storage
}

var _ basic.BPlusTreeManager = (*EnhancedBTreeAdapter)(nil)
