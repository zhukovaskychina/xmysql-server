package manager

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

func TestEnhancedBTreeAdapterFullScanDoesNotReadSidecarWithoutIndex(t *testing.T) {
	dataDir := t.TempDir()
	storage := NewStorageManager(&conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        filepath.Join(dataDir, "innodb"),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	})
	adapter := NewEnhancedBTreeAdapter(storage, DefaultBTreeConfig)
	adapter.spaceID = 7

	sidecarDir := filepath.Join(dataDir, "innodb", "_xmysql_btree_records")
	if err := os.MkdirAll(sidecarDir, 0755); err != nil {
		t.Fatalf("mkdir sidecar dir: %v", err)
	}
	sidecar := `[{"Key":"azE=","Value":"djE=","PageNo":1,"SlotNo":0,"TxnID":0,"DeleteMark":false}]`
	if err := os.WriteFile(filepath.Join(sidecarDir, "space_7_page_1.json"), []byte(sidecar), 0644); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}

	rows, err := adapter.FullScan(context.Background())
	if err == nil {
		t.Fatalf("expected missing index error instead of sidecar rows, got rows=%d", len(rows))
	}
	if !strings.Contains(err.Error(), "failed to get index") {
		t.Fatalf("expected missing index error, got %v", err)
	}
}

func TestEnhancedBTreeAdapterLeafChainPersistsAcrossReload(t *testing.T) {
	ctx := context.Background()
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

	handle, err := storage.CreateTablespace("app/users")
	if err != nil {
		t.Fatalf("CreateTablespace() error = %v", err)
	}

	adapter := NewEnhancedBTreeAdapter(storage, DefaultBTreeConfig)
	if err := adapter.Init(ctx, handle.SpaceID, 3); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("key_%03d", i)
		value := []byte(strings.Repeat(fmt.Sprintf("value_%03d", i), 120))
		if err := adapter.Insert(ctx, key, value); err != nil {
			t.Fatalf("Insert(%s) error = %v", key, err)
		}
	}

	reloaded := NewEnhancedBTreeAdapter(storage, DefaultBTreeConfig)
	if err := reloaded.Init(ctx, handle.SpaceID, adapter.rootPageNo); err != nil {
		t.Fatalf("reload Init() error = %v", err)
	}

	rows, err := reloaded.FullScan(ctx)
	if err != nil {
		t.Fatalf("FullScan() error = %v", err)
	}
	if len(rows) != 25 {
		t.Fatalf("FullScan() rows = %d, want 25", len(rows))
	}

	rangeRows, err := reloaded.RangeSearch(ctx, "key_005", "key_009")
	if err != nil {
		t.Fatalf("RangeSearch() error = %v", err)
	}
	if len(rangeRows) != 5 {
		t.Fatalf("RangeSearch() rows = %d, want 5", len(rangeRows))
	}
}

func TestEnhancedBTreeAdapterDeletesEmptyLeafAndReusesPage(t *testing.T) {
	ctx := context.Background()
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

	handle, err := storage.CreateTablespace("app/reuse_users")
	if err != nil {
		t.Fatalf("CreateTablespace() error = %v", err)
	}

	adapter := NewEnhancedBTreeAdapter(storage, DefaultBTreeConfig)
	if err := adapter.Init(ctx, handle.SpaceID, 3); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("key_%03d", i)
		value := []byte(strings.Repeat(fmt.Sprintf("value_%03d", i), 120))
		if err := adapter.Insert(ctx, key, value); err != nil {
			t.Fatalf("Insert(%s) error = %v", key, err)
		}
	}

	leafPages, err := adapter.GetAllLeafPages(ctx)
	if err != nil {
		t.Fatalf("GetAllLeafPages() error = %v", err)
	}
	if len(leafPages) < 2 {
		t.Fatalf("test needs split leaf chain, got pages=%v", leafPages)
	}

	victimPage := uint32(0)
	keysOnVictim := make([]string, 0)
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("key_%03d", i)
		pageNo, _, err := adapter.Search(ctx, key)
		if err != nil {
			t.Fatalf("Search(%s) error = %v", key, err)
		}
		if pageNo == adapter.rootPageNo {
			continue
		}
		if victimPage == 0 {
			victimPage = pageNo
		}
		if pageNo == victimPage {
			keysOnVictim = append(keysOnVictim, key)
		}
	}
	if victimPage == 0 || len(keysOnVictim) == 0 {
		t.Fatalf("failed to find non-root leaf page, pages=%v", leafPages)
	}

	for _, key := range keysOnVictim {
		if err := adapter.Delete(ctx, key); err != nil {
			t.Fatalf("Delete(%s) error = %v", key, err)
		}
	}

	leafPagesAfterDelete, err := adapter.GetAllLeafPages(ctx)
	if err != nil {
		t.Fatalf("GetAllLeafPages() after delete error = %v", err)
	}
	for _, pageNo := range leafPagesAfterDelete {
		if pageNo == victimPage {
			t.Fatalf("empty leaf page %d still linked after deleting all records, pages=%v", victimPage, leafPagesAfterDelete)
		}
	}

	reloaded := NewEnhancedBTreeAdapter(storage, DefaultBTreeConfig)
	if err := reloaded.Init(ctx, handle.SpaceID, adapter.rootPageNo); err != nil {
		t.Fatalf("reload Init() error = %v", err)
	}
	rootBufferPage, err := storage.GetBufferPoolManager().GetPage(handle.SpaceID, adapter.rootPageNo)
	if err != nil {
		t.Fatalf("GetPage(root) error = %v", err)
	}
	if !uint32SliceContains(parsePersistentFreePageList(rootBufferPage.GetContent()), victimPage) {
		t.Fatalf("persisted free list does not contain page %d: %v", victimPage, parsePersistentFreePageList(rootBufferPage.GetContent()))
	}

	reuseKey := "key_reuse"
	if err := reloaded.Insert(ctx, reuseKey, []byte(strings.Repeat("reuse_value", 120))); err != nil {
		t.Fatalf("Insert(%s) error = %v", reuseKey, err)
	}
	reusedPage, _, err := reloaded.Search(ctx, reuseKey)
	if err != nil {
		t.Fatalf("Search(%s) error = %v", reuseKey, err)
	}
	if reusedPage != victimPage {
		t.Fatalf("new insert page = %d, want reused page %d", reusedPage, victimPage)
	}
}

func TestSimpleRow_SetTransactionId(t *testing.T) {
	row := &SimpleRow{data: []byte{1, 2, 3, 4, 5}}

	row.SetTransactionId(12345)

	assert.Len(t, row.data, 13)
	assert.Equal(t, byte(1), row.data[0])
	assert.Equal(t, byte(2), row.data[1])
	assert.Equal(t, byte(3), row.data[2])
	assert.Equal(t, byte(4), row.data[3])
	assert.Equal(t, byte(5), row.data[4])
	assert.Equal(t, uint64(12345), binary.LittleEndian.Uint64(row.data[5:13]))
}
