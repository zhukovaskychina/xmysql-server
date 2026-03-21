package buffer_pool

import (
	"sync"
	"testing"
	"time"
)

func newTestBufferPool() *BufferPool {
	return NewBufferPool(&BufferPoolConfig{
		TotalPages:       100,
		PageSize:         16384,
		BufferPoolSize:   16384 * 100,
		YoungListPercent: 0.8,
		OldListPercent:   0.2,
		OldBlocksTime:    1000,
		PrefetchSize:     4,
		MaxQueueSize:     10,
		PrefetchWorkers:  2,
		StorageManager:   &MockStorageManager{},
	})
}

func waitUntil(t *testing.T, timeout time.Duration, condition func() bool, description string) {
	t.Helper()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		if condition() {
			return
		}

		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatalf("timed out waiting for %s", description)
		}
	}
}

func TestPrefetchManagerBehavior(t *testing.T) {
	// 创建BufferPool
	bufferPool := newTestBufferPool()

	// 测试预读请求队列
	t.Run("TestPrefetchQueue", func(t *testing.T) {
		pm := bufferPool.prefetchManager

		// 添加一些预读请求
		pm.TriggerPrefetchWithPriority(1, 100, 5, time.Second)
		pm.TriggerPrefetchWithPriority(1, 200, 8, time.Second)
		pm.TriggerPrefetchWithPriority(1, 300, 3, time.Second)

		// 验证队列长度
		if length := pm.GetQueueLength(); length != 3 {
			t.Errorf("Expected queue length 3, got %d", length)
		}

		// 获取请求并验证优先级顺序
		req := pm.getNextRequest()
		if req.Priority != 8 {
			t.Errorf("Expected highest priority 8, got %d", req.Priority)
		}
	})

	// 测试队列满时的行为
	t.Run("TestQueueFull", func(t *testing.T) {
		pm := NewPrefetchManager(bufferPool, 64, 2, 1) // 最大队列长度为2

		// 添加3个请求
		pm.TriggerPrefetchWithPriority(1, 100, 5, time.Second)
		pm.TriggerPrefetchWithPriority(1, 200, 8, time.Second)
		pm.TriggerPrefetchWithPriority(1, 300, 3, time.Second)

		// 验证队列长度不超过最大值
		if length := pm.GetQueueLength(); length > 2 {
			t.Errorf("Queue length %d exceeds maximum 2", length)
		}
	})

	// 测试清空队列
	t.Run("TestClearQueue", func(t *testing.T) {
		pm := bufferPool.prefetchManager

		// 添加一些请求
		pm.TriggerPrefetchWithPriority(1, 100, 5, time.Second)
		pm.TriggerPrefetchWithPriority(1, 200, 8, time.Second)

		// 清空队列
		pm.ClearQueue()

		// 验证队列为空
		if length := pm.GetQueueLength(); length != 0 {
			t.Errorf("Expected empty queue, got length %d", length)
		}
	})

	// 测试过期请求处理
	t.Run("TestExpiredRequests", func(t *testing.T) {
		pm := bufferPool.prefetchManager

		// 添加一个立即过期的请求
		pm.TriggerPrefetchWithPriority(1, 100, 5, -time.Second)

		waitUntil(t, time.Second, func() bool {
			return pm.GetQueueLength() == 0
		}, "expired prefetch request removal")
	})
}

func TestPrefetchIntegration(t *testing.T) {
	// 创建BufferPool
	bufferPool := newTestBufferPool()

	// 测试RangePageLoad触发预读
	t.Run("TestRangePageLoadPrefetch", func(t *testing.T) {
		// 加载一段页面
		if err := bufferPool.RangePageLoad(1, 0, 10); err != nil {
			t.Fatalf("RangePageLoad failed: %v", err)
		}

		// 验证后续页面最终被预读进缓存，而不是依赖队列瞬时状态。
		waitUntil(t, 2*time.Second, func() bool {
			for pageNo := uint32(10); pageNo < 14; pageNo++ {
				if _, err := bufferPool.lruCache.Get(1, pageNo); err != nil {
					return false
				}
			}
			return true
		}, "prefetched pages to appear in cache after range load")
	})

	// 测试并发预读
	t.Run("TestConcurrentPrefetch", func(t *testing.T) {
		var wg sync.WaitGroup

		// 并发触发多个预读请求
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				bufferPool.prefetchManager.TriggerPrefetchWithPriority(1, uint32(i*100), 5, time.Second)
			}(i)
		}

		wg.Wait()

		// 验证队列状态
		length := bufferPool.prefetchManager.GetQueueLength()
		if length > bufferPool.prefetchManager.maxQueueSize {
			t.Errorf("Queue length %d exceeds maximum size", length)
		}
	})
}
