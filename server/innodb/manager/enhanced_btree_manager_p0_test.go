package manager

import (
	"context"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
)

func newTestEnhancedBTreeManagerForP0(t *testing.T) (*EnhancedBTreeManager, *OptimizedBufferPoolManager) {
	t.Helper()

	bpm, err := NewOptimizedBufferPoolManager(&BufferPoolConfig{
		PoolSize:        16,
		PageSize:        PAGE_SIZE,
		FlushInterval:   time.Hour,
		StorageProvider: newMockOptimizedStorageProvider(),
		YoungListRatio:  YOUNG_LIST_RATIO,
		OldListRatio:    OLD_LIST_RATIO,
		OldBlockTime:    OLD_BLOCK_TIME,
	})
	if err != nil {
		t.Fatalf("new buffer pool manager: %v", err)
	}

	storageManager := &StorageManager{bufferPoolMgr: bpm}
	btreeManager := NewEnhancedBTreeManager(storageManager, &BTreeConfig{
		MaxCacheSize:   1000,
		CachePolicy:    "LRU",
		PrefetchSize:   4,
		PageSize:       PAGE_SIZE,
		FillFactor:     0.8,
		MinFillFactor:  0.4,
		SplitThreshold: 0.9,
		MergeThreshold: 0.3,
		AsyncIO:        false,
		EnableStats:    true,
		StatsInterval:  time.Hour,
	})

	t.Cleanup(func() {
		_ = btreeManager.Close()
		_ = bpm.Close()
	})

	return btreeManager, bpm
}

func newTestIndexMetadataForP0() *IndexMetadata {
	return &IndexMetadata{
		TableID:   10,
		SpaceID:   1,
		IndexName: "idx_user_id",
		IndexType: IndexTypeSecondary,
		Columns: []IndexColumn{
			{ColumnName: "user_id", ColumnPos: 0, KeyLength: 8},
		},
		KeyLength: 8,
	}
}

func TestEnhancedBTreeManagerRebuildIndexReloadsIndex(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestEnhancedBTreeManagerForP0(t)

	index, err := manager.CreateIndex(ctx, newTestIndexMetadataForP0())
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	metadata := index.GetMetadata()
	beforeUpdate := metadata.UpdateTime

	if err := manager.RebuildIndex(ctx, metadata.IndexID); err != nil {
		t.Fatalf("rebuild index: %v", err)
	}

	reloaded, err := manager.GetIndex(metadata.IndexID)
	if err != nil {
		t.Fatalf("get rebuilt index: %v", err)
	}
	if reloaded.GetRootPageNo() != metadata.RootPageNo {
		t.Fatalf("root page changed during rebuild: got=%d want=%d", reloaded.GetRootPageNo(), metadata.RootPageNo)
	}
	if metadata.IndexState != EnhancedIndexStateActive {
		t.Fatalf("unexpected index state after rebuild: %v", metadata.IndexState)
	}
	if !metadata.IsLoaded {
		t.Fatal("expected rebuilt index to be loaded")
	}
	if !metadata.UpdateTime.After(beforeUpdate) {
		t.Fatal("expected rebuild to update metadata timestamp")
	}
}

func TestEnhancedBTreeManagerRebuildIndexPreservesRecords(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestEnhancedBTreeManagerForP0(t)

	index, err := manager.CreateIndex(ctx, newTestIndexMetadataForP0())
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	indexID := index.GetIndexID()

	if err := manager.Insert(ctx, indexID, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("insert before rebuild: %v", err)
	}

	if err := manager.RebuildIndex(ctx, indexID); err != nil {
		t.Fatalf("rebuild index: %v", err)
	}

	record, err := manager.Search(ctx, indexID, []byte("k1"))
	if err != nil {
		t.Fatalf("search after rebuild: %v", err)
	}
	if string(record.Value) != "v1" {
		t.Fatalf("unexpected rebuilt record value: %q", string(record.Value))
	}
}

func TestEnhancedBTreeManagerDropIndexRemovesMetadataCacheAndRootPage(t *testing.T) {
	ctx := context.Background()
	manager, bpm := newTestEnhancedBTreeManagerForP0(t)

	index, err := manager.CreateIndex(ctx, newTestIndexMetadataForP0())
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	indexID := index.GetIndexID()
	rootPageNo := index.GetRootPageNo()
	if !bpm.lruCache.Has(1, rootPageNo) {
		t.Fatalf("expected root page %d to be cached before drop", rootPageNo)
	}

	if err := manager.DropIndex(ctx, indexID); err != nil {
		t.Fatalf("drop index: %v", err)
	}

	if _, err := manager.metadataManager.GetIndexMetadata(indexID); err == nil {
		t.Fatal("expected index metadata to be removed")
	}
	if manager.GetLoadedIndexCount() != 0 {
		t.Fatalf("expected no loaded indexes, got %d", manager.GetLoadedIndexCount())
	}
	if bpm.lruCache.Has(1, rootPageNo) {
		t.Fatalf("expected root page %d to be removed from buffer pool", rootPageNo)
	}
}

func TestEnhancedBTreeManagerDropIndexFreesLoadedCachedPages(t *testing.T) {
	ctx := context.Background()
	manager, bpm := newTestEnhancedBTreeManagerForP0(t)

	index, err := manager.CreateIndex(ctx, newTestIndexMetadataForP0())
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	enhancedIndex := index.(*EnhancedBTreeIndex)
	extraPageNo := uint32(99)
	enhancedIndex.pageCache[extraPageNo] = &BTreePage{
		PageNo:      extraPageNo,
		PageType:    BTreePageTypeLeaf,
		RecordCount: 0,
	}
	enhancedIndex.pageLoadOrder = append(enhancedIndex.pageLoadOrder, extraPageNo)

	extraPage, err := bpm.AllocatePage(1)
	if err != nil {
		t.Fatalf("allocate extra page: %v", err)
	}
	extraPage.Init(1, extraPageNo, []byte("extra-index-page"))
	if err := bpm.lruCache.Set(1, extraPageNo, buffer_pool.NewBufferBlock(extraPage)); err != nil {
		t.Fatalf("cache extra page: %v", err)
	}

	if err := manager.DropIndex(ctx, index.GetIndexID()); err != nil {
		t.Fatalf("drop index: %v", err)
	}

	if bpm.lruCache.Has(1, extraPageNo) {
		t.Fatalf("expected cached page %d to be removed from buffer pool", extraPageNo)
	}
}
