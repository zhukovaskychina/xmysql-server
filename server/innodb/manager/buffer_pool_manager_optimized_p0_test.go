package manager

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
)

type blockingOptimizedStorageProvider struct {
	*mockOptimizedStorageProvider
	writeStarted chan struct{}
	releaseWrite chan struct{}
	writeOnce    sync.Once
}

func (p *blockingOptimizedStorageProvider) WritePage(spaceID uint32, pageNo uint32, data []byte) error {
	p.writeOnce.Do(func() { close(p.writeStarted) })
	<-p.releaseWrite
	return p.mockOptimizedStorageProvider.WritePage(spaceID, pageNo, data)
}

type delayedOptimizedStorageProvider struct {
	*mockOptimizedStorageProvider
	delay          time.Duration
	activeReads    int32
	maxActiveReads int32
}

func (p *delayedOptimizedStorageProvider) ReadPage(spaceID uint32, pageNo uint32) ([]byte, error) {
	active := atomic.AddInt32(&p.activeReads, 1)
	for {
		maxActive := atomic.LoadInt32(&p.maxActiveReads)
		if active <= maxActive || atomic.CompareAndSwapInt32(&p.maxActiveReads, maxActive, active) {
			break
		}
	}
	time.Sleep(p.delay)
	data, err := p.mockOptimizedStorageProvider.ReadPage(spaceID, pageNo)
	atomic.AddInt32(&p.activeReads, -1)
	return data, err
}

func TestOptimizedBufferPoolManagerCountsLRUSetEvictions(t *testing.T) {
	bpm, err := NewOptimizedBufferPoolManager(&BufferPoolConfig{
		PoolSize:        2,
		PageSize:        PAGE_SIZE,
		FlushInterval:   time.Hour,
		StorageProvider: newMockOptimizedStorageProvider(),
		YoungListRatio:  0.5,
		OldListRatio:    0.5,
		OldBlockTime:    OLD_BLOCK_TIME,
	})
	if err != nil {
		t.Fatalf("new optimized buffer pool manager: %v", err)
	}
	defer bpm.Close()

	for pageNo := uint32(1); pageNo <= 3; pageNo++ {
		if _, err := bpm.GetPage(1, pageNo); err != nil {
			t.Fatalf("get page %d: %v", pageNo, err)
		}
	}

	stats := bpm.GetStatistics()
	if stats.Evictions == 0 {
		t.Fatalf("expected evictions to be counted, got %+v", stats)
	}
	if stats.CacheSize > 2 {
		t.Fatalf("cache exceeded configured pool size: %d", stats.CacheSize)
	}
}

func TestOptimizedBufferPoolManagerCloseIsIdempotent(t *testing.T) {
	bpm, err := NewOptimizedBufferPoolManager(&BufferPoolConfig{
		PoolSize:        2,
		PageSize:        PAGE_SIZE,
		FlushInterval:   time.Hour,
		StorageProvider: newMockOptimizedStorageProvider(),
	})
	if err != nil {
		t.Fatalf("new optimized buffer pool manager: %v", err)
	}

	if err := bpm.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := bpm.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestOptimizedBufferPoolManagerAppliesReadAheadHints(t *testing.T) {
	bpm, err := NewOptimizedBufferPoolManager(&BufferPoolConfig{
		PoolSize:        2,
		PageSize:        PAGE_SIZE,
		FlushInterval:   time.Hour,
		StorageProvider: newMockOptimizedStorageProvider(),
	})
	if err != nil {
		t.Fatalf("new optimized buffer pool manager: %v", err)
	}

	if err := bpm.ApplyHint("aggressive"); err != nil {
		t.Fatalf("apply aggressive hint: %v", err)
	}
	if got := bpm.config.ReadAheadPages; got != 16 {
		t.Fatalf("aggressive read-ahead = %d, want 16", got)
	}
	if err := bpm.ApplyHint("read_ahead=3"); err != nil {
		t.Fatalf("apply explicit read-ahead hint: %v", err)
	}
	if got := bpm.config.ReadAheadPages; got != 3 {
		t.Fatalf("explicit read-ahead = %d, want 3", got)
	}
	if err := bpm.ApplyHint("unknown"); err == nil {
		t.Fatal("unknown buffer pool hint should fail")
	}
	if err := bpm.SetReadAheadPages(-1); err == nil {
		t.Fatal("negative read-ahead should fail")
	}

	if err := bpm.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := bpm.SetReadAheadPages(1); err == nil {
		t.Fatal("setting read-ahead after close should fail")
	}
	// A late prefetch request must be ignored rather than sending on a closed
	// queue. This is a shutdown-safety contract, not a data-path operation.
	bpm.PrefetchPage(1, 1)
}

func TestOptimizedBufferPoolManagerBoundsPrefetchConcurrency(t *testing.T) {
	provider := &delayedOptimizedStorageProvider{
		mockOptimizedStorageProvider: newMockOptimizedStorageProvider(),
		delay:                        20 * time.Millisecond,
	}
	bpm, err := NewOptimizedBufferPoolManager(&BufferPoolConfig{
		PoolSize:        128,
		PageSize:        PAGE_SIZE,
		FlushInterval:   time.Hour,
		StorageProvider: provider,
		PrefetchWorkers: 2,
		MaxQueueSize:    64,
	})
	if err != nil {
		t.Fatalf("new optimized buffer pool manager: %v", err)
	}
	defer bpm.Close()

	for pageNo := uint32(1); pageNo <= 20; pageNo++ {
		bpm.PrefetchPage(7, pageNo)
	}
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadUint64(&bpm.stats.pageReads) < 20 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := atomic.LoadUint64(&bpm.stats.pageReads); got != 20 {
		t.Fatalf("prefetch reads = %d, want 20", got)
	}
	if got := atomic.LoadInt32(&provider.maxActiveReads); got > 2 {
		t.Fatalf("prefetch concurrency = %d, want at most 2 workers", got)
	}
}

func TestOptimizedBufferPoolManagerCountsReadAheadEvictions(t *testing.T) {
	bpm, err := NewOptimizedBufferPoolManager(&BufferPoolConfig{
		PoolSize:        1,
		PageSize:        PAGE_SIZE,
		FlushInterval:   time.Hour,
		StorageProvider: newMockOptimizedStorageProvider(),
		PrefetchWorkers: 1,
		MaxQueueSize:    4,
	})
	if err != nil {
		t.Fatalf("new optimized buffer pool manager: %v", err)
	}
	defer bpm.Close()

	bpm.PrefetchPage(1, 1)
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadUint64(&bpm.stats.readAhead) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := atomic.LoadUint64(&bpm.stats.readAhead); got == 0 {
		t.Fatal("prefetch did not complete before eviction test")
	}

	if _, err := bpm.GetPage(1, 2); err != nil {
		t.Fatalf("load page that evicts prefetched page: %v", err)
	}
	if got := atomic.LoadUint64(&bpm.stats.readAheadEvicted); got != 1 {
		t.Fatalf("read-ahead evictions = %d, want 1", got)
	}
}

func TestOptimizedBufferPoolManagerReportsReadAheadRate(t *testing.T) {
	bpm, err := NewOptimizedBufferPoolManager(&BufferPoolConfig{
		PoolSize:        32,
		PageSize:        PAGE_SIZE,
		FlushInterval:   time.Hour,
		StorageProvider: newMockOptimizedStorageProvider(),
		PrefetchWorkers: 1,
		MaxQueueSize:    16,
	})
	if err != nil {
		t.Fatalf("new optimized buffer pool manager: %v", err)
	}
	defer bpm.Close()

	_ = bpm.GetStats()
	for pageNo := uint32(1); pageNo <= 4; pageNo++ {
		bpm.PrefetchPage(2, pageNo)
	}
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadUint64(&bpm.stats.readAhead) < 4 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := atomic.LoadUint64(&bpm.stats.readAhead); got < 4 {
		t.Fatalf("prefetch reads = %d, want at least 4", got)
	}

	stats := bpm.GetStats()
	rate, ok := stats["read_ahead_rate"].(float64)
	if !ok || rate <= 0 {
		t.Fatalf("read-ahead rate = %#v, want positive rate", stats["read_ahead_rate"])
	}
}

func TestOptimizedBufferPoolManagerReportsYoungPagePromotionCounters(t *testing.T) {
	bpm, err := NewOptimizedBufferPoolManager(&BufferPoolConfig{
		PoolSize:        8,
		PageSize:        PAGE_SIZE,
		FlushInterval:   time.Hour,
		StorageProvider: newMockOptimizedStorageProvider(),
	})
	if err != nil {
		t.Fatalf("new optimized buffer pool manager: %v", err)
	}
	defer bpm.Close()

	page := buffer_pool.NewBufferPage(12, 34)
	bpm.lruCache.SetOld(12, 34, buffer_pool.NewBufferBlock(page))
	for i := 0; i < 4; i++ {
		if _, err := bpm.GetPage(12, 34); err != nil {
			t.Fatalf("get old page %d: %v", i, err)
		}
	}

	stats := bpm.GetStats()
	madeYoung, ok := stats["pages_made_young"].(uint64)
	if !ok || madeYoung != 1 {
		t.Fatalf("pages_made_young = %#v, want 1", stats["pages_made_young"])
	}
	notMadeYoung, ok := stats["pages_not_made_young"].(uint64)
	if !ok || notMadeYoung != 2 {
		t.Fatalf("pages_not_made_young = %#v, want 2", stats["pages_not_made_young"])
	}
}

func TestOptimizedBufferPoolManagerReportsPendingBufferWork(t *testing.T) {
	bpm, err := NewOptimizedBufferPoolManager(&BufferPoolConfig{
		PoolSize:        8,
		PageSize:        PAGE_SIZE,
		FlushInterval:   time.Hour,
		StorageProvider: newMockOptimizedStorageProvider(),
	})
	if err != nil {
		t.Fatalf("new optimized buffer pool manager: %v", err)
	}
	defer bpm.Close()

	if _, err := bpm.GetDirtyPage(4, 9); err != nil {
		t.Fatalf("get dirty page: %v", err)
	}
	stats := bpm.GetStats()
	pendingFlush, ok := stats["pending_flush_list"].(int64)
	if !ok || pendingFlush != 1 {
		t.Fatalf("pending_flush_list = %#v, want 1", stats["pending_flush_list"])
	}
}

func TestOptimizedBufferPoolManagerFlushesDirtyLRUEviction(t *testing.T) {
	provider := &blockingOptimizedStorageProvider{
		mockOptimizedStorageProvider: newMockOptimizedStorageProvider(),
		writeStarted:                 make(chan struct{}),
		releaseWrite:                 make(chan struct{}),
	}
	bpm, err := NewOptimizedBufferPoolManager(&BufferPoolConfig{
		PoolSize:        1,
		PageSize:        PAGE_SIZE,
		FlushInterval:   time.Hour,
		MaxQueueSize:    4,
		StorageProvider: provider,
	})
	if err != nil {
		t.Fatalf("new optimized buffer pool manager: %v", err)
	}
	defer bpm.Close()

	page, err := bpm.GetDirtyPage(21, 1)
	if err != nil {
		t.Fatalf("get dirty page: %v", err)
	}
	page.SetContent([]byte("lru-eviction-must-persist"))
	if _, err := bpm.GetPage(21, 2); err != nil {
		t.Fatalf("load page that evicts dirty page: %v", err)
	}
	select {
	case <-provider.writeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("dirty LRU eviction did not schedule a flush")
	}

	stats := bpm.GetStats()
	pending, ok := stats["pending_flush_lru"].(int64)
	if !ok || pending != 1 {
		t.Fatalf("pending_flush_lru = %#v, want 1", stats["pending_flush_lru"])
	}
	close(provider.releaseWrite)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats = bpm.GetStats()
		if pending, ok = stats["pending_flush_lru"].(int64); ok && pending == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pending != 0 {
		t.Fatalf("pending_flush_lru after flush = %d, want 0", pending)
	}
	data, err := provider.ReadPage(21, 1)
	if err != nil {
		t.Fatalf("read flushed eviction: %v", err)
	}
	if string(data[:len("lru-eviction-must-persist")]) != "lru-eviction-must-persist" {
		t.Fatalf("flushed eviction data = %q", data[:len("lru-eviction-must-persist")])
	}
}

func TestOptimizedBufferPoolManagerReportsLRUIOWithIndependentCurrentWindow(t *testing.T) {
	bpm, err := NewOptimizedBufferPoolManager(&BufferPoolConfig{
		PoolSize:        8,
		PageSize:        PAGE_SIZE,
		FlushInterval:   time.Hour,
		StorageProvider: newMockOptimizedStorageProvider(),
	})
	if err != nil {
		t.Fatalf("new optimized buffer pool manager: %v", err)
	}
	defer bpm.Close()

	if _, err := bpm.GetPage(15, 1); err != nil {
		t.Fatalf("load page: %v", err)
	}

	total, current := bpm.GetLRUIOStatsAndResetCurrent()
	if total != 1 || current != 1 {
		t.Fatalf("lru io stats = %d/%d, want 1/1", total, current)
	}

	total, current = bpm.GetLRUIOStatsAndResetCurrent()
	if total != 1 || current != 0 {
		t.Fatalf("lru io stats after current reset = %d/%d, want 1/0", total, current)
	}
}

func TestOptimizedBufferPoolManagerFlushesMutationsFromCachedPage(t *testing.T) {
	provider := newMockOptimizedStorageProvider()
	bpm, err := NewOptimizedBufferPoolManager(&BufferPoolConfig{
		PoolSize:        8,
		PageSize:        PAGE_SIZE,
		FlushInterval:   time.Hour,
		StorageProvider: provider,
	})
	if err != nil {
		t.Fatalf("new optimized buffer pool manager: %v", err)
	}
	defer bpm.Close()

	page, err := bpm.GetDirtyPage(9, 3)
	if err != nil {
		t.Fatalf("get dirty page: %v", err)
	}
	page.SetContent([]byte("persisted mutation"))
	if err := bpm.FlushPage(9, 3); err != nil {
		t.Fatalf("flush mutated page: %v", err)
	}
	data, err := provider.ReadPage(9, 3)
	if err != nil {
		t.Fatalf("read flushed page: %v", err)
	}
	if string(data[:len("persisted mutation")]) != "persisted mutation" {
		t.Fatalf("flushed page prefix = %q, want persisted mutation", data[:len("persisted mutation")])
	}
}

func TestOptimizedBufferPoolManagerDoesNotDoubleCountDirtyPage(t *testing.T) {
	provider := newMockOptimizedStorageProvider()
	bpm, err := NewOptimizedBufferPoolManager(&BufferPoolConfig{
		PoolSize:        8,
		PageSize:        PAGE_SIZE,
		FlushInterval:   time.Hour,
		StorageProvider: provider,
	})
	if err != nil {
		t.Fatalf("new optimized buffer pool manager: %v", err)
	}
	defer bpm.Close()

	if _, err := bpm.GetDirtyPage(11, 5); err != nil {
		t.Fatalf("first get dirty page: %v", err)
	}
	if _, err := bpm.GetDirtyPage(11, 5); err != nil {
		t.Fatalf("second get dirty page: %v", err)
	}
	if got := bpm.GetStatistics().DirtyPages; got != 1 {
		t.Fatalf("dirty page count = %d, want 1", got)
	}

	if err := bpm.FlushPage(11, 5); err != nil {
		t.Fatalf("flush dirty page: %v", err)
	}
	if got := bpm.GetStatistics().DirtyPages; got != 0 {
		t.Fatalf("dirty page count after flush = %d, want 0", got)
	}
}

func TestOptimizedBufferPoolManagerFreePageDoesNotUnderflowTotalPages(t *testing.T) {
	bpm, err := NewOptimizedBufferPoolManager(&BufferPoolConfig{
		PoolSize:        8,
		PageSize:        PAGE_SIZE,
		FlushInterval:   time.Hour,
		StorageProvider: newMockOptimizedStorageProvider(),
	})
	if err != nil {
		t.Fatalf("new optimized buffer pool manager: %v", err)
	}
	defer bpm.Close()

	if err := bpm.FreePage(13, 7); err != nil {
		t.Fatalf("free page: %v", err)
	}
	if got := bpm.GetStatistics().TotalPages; got != 0 {
		t.Fatalf("total page count after freeing an untracked page = %d, want 0", got)
	}
}
