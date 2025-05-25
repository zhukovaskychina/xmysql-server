package buffer_pool

import (
	"sync/atomic"
	"time"
)

// BufferPoolStats 缓冲池统计信息
type BufferPoolStats struct {
	// 页面统计
	TotalPages int64
	FreePages  int64
	DirtyPages int64
	OldPages   int64
	YoungPages int64

	// 命中率统计
	PageRequests int64
	PageHits     int64
	PageMisses   int64

	// IO统计
	PageReads     int64
	PageWrites    int64
	PageEvictions int64

	// 预读统计
	PrefetchRequests int64
	PrefetchHits     int64
	PrefetchMisses   int64

	// 刷新统计
	FlushRequests  int64
	FlushSuccesses int64
	FlushFailures  int64

	// 性能统计
	ReadLatencyTotal  int64 // 纳秒
	WriteLatencyTotal int64 // 纳秒
	LastResetTime     time.Time
}

// NewBufferPoolStats 创建新的统计对象
func NewBufferPoolStats() *BufferPoolStats {
	return &BufferPoolStats{
		LastResetTime: time.Now(),
	}
}

// RecordPageRequest 记录页面请求
func (s *BufferPoolStats) RecordPageRequest(hit bool) {
	atomic.AddInt64(&s.PageRequests, 1)
	if hit {
		atomic.AddInt64(&s.PageHits, 1)
	} else {
		atomic.AddInt64(&s.PageMisses, 1)
	}
}

// RecordPageIO 记录页面IO
func (s *BufferPoolStats) RecordPageIO(isRead bool, latencyNs int64) {
	if isRead {
		atomic.AddInt64(&s.PageReads, 1)
		atomic.AddInt64(&s.ReadLatencyTotal, latencyNs)
	} else {
		atomic.AddInt64(&s.PageWrites, 1)
		atomic.AddInt64(&s.WriteLatencyTotal, latencyNs)
	}
}

// RecordPrefetch 记录预读统计
func (s *BufferPoolStats) RecordPrefetch(hit bool) {
	atomic.AddInt64(&s.PrefetchRequests, 1)
	if hit {
		atomic.AddInt64(&s.PrefetchHits, 1)
	} else {
		atomic.AddInt64(&s.PrefetchMisses, 1)
	}
}

// RecordFlush 记录刷新统计
func (s *BufferPoolStats) RecordFlush(success bool) {
	atomic.AddInt64(&s.FlushRequests, 1)
	if success {
		atomic.AddInt64(&s.FlushSuccesses, 1)
	} else {
		atomic.AddInt64(&s.FlushFailures, 1)
	}
}

// UpdatePageCounts 更新页面计数
func (s *BufferPoolStats) UpdatePageCounts(total, free, dirty, old, young int64) {
	atomic.StoreInt64(&s.TotalPages, total)
	atomic.StoreInt64(&s.FreePages, free)
	atomic.StoreInt64(&s.DirtyPages, dirty)
	atomic.StoreInt64(&s.OldPages, old)
	atomic.StoreInt64(&s.YoungPages, young)
}

// GetHitRatio 获取命中率
func (s *BufferPoolStats) GetHitRatio() float64 {
	requests := atomic.LoadInt64(&s.PageRequests)
	if requests == 0 {
		return 0
	}
	hits := atomic.LoadInt64(&s.PageHits)
	return float64(hits) / float64(requests)
}

// GetPrefetchHitRatio 获取预读命中率
func (s *BufferPoolStats) GetPrefetchHitRatio() float64 {
	requests := atomic.LoadInt64(&s.PrefetchRequests)
	if requests == 0 {
		return 0
	}
	hits := atomic.LoadInt64(&s.PrefetchHits)
	return float64(hits) / float64(requests)
}

// GetAvgReadLatency 获取平均读取延迟(纳秒)
func (s *BufferPoolStats) GetAvgReadLatency() float64 {
	reads := atomic.LoadInt64(&s.PageReads)
	if reads == 0 {
		return 0
	}
	total := atomic.LoadInt64(&s.ReadLatencyTotal)
	return float64(total) / float64(reads)
}

// GetAvgWriteLatency 获取平均写入延迟(纳秒)
func (s *BufferPoolStats) GetAvgWriteLatency() float64 {
	writes := atomic.LoadInt64(&s.PageWrites)
	if writes == 0 {
		return 0
	}
	total := atomic.LoadInt64(&s.WriteLatencyTotal)
	return float64(total) / float64(writes)
}

// Reset 重置统计信息
func (s *BufferPoolStats) Reset() {
	atomic.StoreInt64(&s.PageRequests, 0)
	atomic.StoreInt64(&s.PageHits, 0)
	atomic.StoreInt64(&s.PageMisses, 0)
	atomic.StoreInt64(&s.PageReads, 0)
	atomic.StoreInt64(&s.PageWrites, 0)
	atomic.StoreInt64(&s.PageEvictions, 0)
	atomic.StoreInt64(&s.PrefetchRequests, 0)
	atomic.StoreInt64(&s.PrefetchHits, 0)
	atomic.StoreInt64(&s.PrefetchMisses, 0)
	atomic.StoreInt64(&s.FlushRequests, 0)
	atomic.StoreInt64(&s.FlushSuccesses, 0)
	atomic.StoreInt64(&s.FlushFailures, 0)
	atomic.StoreInt64(&s.ReadLatencyTotal, 0)
	atomic.StoreInt64(&s.WriteLatencyTotal, 0)
	s.LastResetTime = time.Now()
}
