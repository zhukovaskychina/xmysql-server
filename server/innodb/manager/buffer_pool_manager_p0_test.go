package manager

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
)

func newTestLegacyBufferPoolManagerForP0(t *testing.T) (*BufferPoolManager, *MockStorageProvider) {
	t.Helper()

	storage := NewMockStorageProvider()
	bpm, err := NewBufferPoolManager(&BufferPoolConfig{
		PoolSize:        2,
		PageSize:        PAGE_SIZE,
		FlushInterval:   time.Hour,
		StorageProvider: storage,
		YoungListRatio:  YOUNG_LIST_RATIO,
		OldListRatio:    OLD_LIST_RATIO,
		OldBlockTime:    OLD_BLOCK_TIME,
	})
	if err != nil {
		t.Fatalf("new buffer pool manager: %v", err)
	}
	t.Cleanup(func() {
		_ = bpm.Close()
	})

	return bpm, storage
}

func TestBufferPoolManagerEvictPageRemovesCleanUnpinnedPage(t *testing.T) {
	bpm, storage := newTestLegacyBufferPoolManagerForP0(t)
	storage.ResetCounters()

	page := buffer_pool.NewBufferPage(1, 100)
	page.Init(1, 100, []byte("clean-page"))
	page.SetDirty(false)

	if err := bpm.bufferPool.PutPage(page); err != nil {
		t.Fatalf("put page: %v", err)
	}

	evicted := bpm.evictPage()
	if evicted == nil {
		t.Fatal("expected a clean page to be evicted")
	}
	if evicted.GetSpaceID() != 1 || evicted.GetPageNo() != 100 {
		t.Fatalf("evicted wrong page: space=%d page=%d", evicted.GetSpaceID(), evicted.GetPageNo())
	}
	if storage.GetWriteCount() != 0 {
		t.Fatalf("clean eviction should not flush page, writes=%d", storage.GetWriteCount())
	}
	if got := atomic.LoadUint64(&bpm.stats.evictions); got != 1 {
		t.Fatalf("expected one eviction, got %d", got)
	}
}

func TestBufferPoolManagerEvictPageFlushesDirtyUnpinnedPage(t *testing.T) {
	bpm, storage := newTestLegacyBufferPoolManagerForP0(t)
	storage.ResetCounters()

	page := buffer_pool.NewBufferPage(1, 101)
	page.Init(1, 101, []byte("dirty-page"))
	page.SetDirty(true)

	if err := bpm.bufferPool.PutPage(page); err != nil {
		t.Fatalf("put page: %v", err)
	}

	evicted := bpm.evictPage()
	if evicted == nil {
		t.Fatal("expected a dirty page to be evicted")
	}
	if evicted.GetSpaceID() != 1 || evicted.GetPageNo() != 101 {
		t.Fatalf("evicted wrong page: space=%d page=%d", evicted.GetSpaceID(), evicted.GetPageNo())
	}
	if storage.GetWriteCount() != 1 {
		t.Fatalf("dirty eviction should flush page once, writes=%d", storage.GetWriteCount())
	}
	if evicted.IsDirty() {
		t.Fatal("evicted page should be clean after successful flush")
	}
	if got := atomic.LoadUint64(&bpm.stats.evictions); got != 1 {
		t.Fatalf("expected one eviction, got %d", got)
	}
}

func TestBufferPoolManagerEvictPageSkipsPinnedPage(t *testing.T) {
	bpm, _ := newTestLegacyBufferPoolManagerForP0(t)

	pinned := buffer_pool.NewBufferPage(1, 200)
	pinned.Init(1, 200, []byte("pinned"))
	pinned.Pin()

	unpinned := buffer_pool.NewBufferPage(1, 201)
	unpinned.Init(1, 201, []byte("unpinned"))

	if err := bpm.bufferPool.PutPage(pinned); err != nil {
		t.Fatalf("put pinned page: %v", err)
	}
	if err := bpm.bufferPool.PutPage(unpinned); err != nil {
		t.Fatalf("put unpinned page: %v", err)
	}

	evicted := bpm.evictPage()
	if evicted == nil {
		t.Fatal("expected an unpinned page to be evicted")
	}
	if evicted.GetPageNo() == pinned.GetPageNo() {
		t.Fatal("pinned page must not be evicted")
	}
}
