package manager

import (
	"testing"
	"time"
)

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
