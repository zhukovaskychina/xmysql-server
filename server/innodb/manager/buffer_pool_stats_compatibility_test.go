package manager

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
)

func TestBufferPoolManagerGetStatsExposesRuntimeHitCounters(t *testing.T) {
	bpm := &BufferPoolManager{
		bufferPool: buffer_pool.NewBufferPool(&buffer_pool.BufferPoolConfig{
			TotalPages: 8, PageSize: PAGE_SIZE, BufferPoolSize: 8 * PAGE_SIZE,
			YoungListPercent: 0.75, OldListPercent: 0.25, OldBlocksTime: 1000,
			PrefetchWorkers: 1,
		}),
		config: &BufferPoolConfig{PoolSize: 8},
	}
	bpm.stats.hits = 3
	bpm.stats.misses = 1
	bpm.stats.youngHits = 2
	bpm.stats.oldHits = 1

	stats := bpm.GetStats()
	if got := stats["hit_rate"]; got != 0.75 {
		t.Fatalf("hit_rate = %#v, want 0.75", got)
	}
	if got := stats["young_hits"]; got != uint64(2) {
		t.Fatalf("young_hits = %#v, want 2", got)
	}
	if got := stats["old_hits"]; got != uint64(1) {
		t.Fatalf("old_hits = %#v, want 1", got)
	}
}
