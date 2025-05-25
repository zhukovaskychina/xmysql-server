package buffer_pool

import (
	"testing"
	"time"
)

func TestPrefetchManager(t *testing.T) {
	// 创建BufferPool
	bufferPool := NewBufferPool(16384*100, 0.8, 0.2, 1000, nil)

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

		// 等待一小段时间让工作线程处理
		time.Sleep(time.Millisecond * 100)

		// 验证过期请求被移除
		if length := pm.GetQueueLength(); length != 0 {
			t.Errorf("Expected expired request to be removed, got queue length %d", length)
		}
	})
}

func TestPrefetchIntegration(t *testing.T) {
	// 创建BufferPool
	bufferPool := NewBufferPool(16384*100, 0.8, 0.2, 1000, nil)

	// 测试RangePageLoad触发预读
	t.Run("TestRangePageLoadPrefetch", func(t *testing.T) {
		// 加载一段页面
		bufferPool.RangePageLoad(1, 0, 10)

		// 验证预读队列中包含后续页面的预读请求
		length := bufferPool.prefetchManager.GetQueueLength()
		if length == 0 {
			t.Error("Expected prefetch requests in queue after RangePageLoad")
		}
	})

	// 测试并发预读
	t.Run("TestConcurrentPrefetch", func(t *testing.T) {
		// 并发触发多个预读请求
		for i := 0; i < 10; i++ {
			go func(i int) {
				bufferPool.prefetchManager.TriggerPrefetchWithPriority(1, uint32(i*100), 5, time.Second)
			}(i)
		}

		// 等待所有请求被处理
		time.Sleep(time.Millisecond * 500)

		// 验证队列状态
		length := bufferPool.prefetchManager.GetQueueLength()
		if length > bufferPool.prefetchManager.maxQueueSize {
			t.Errorf("Queue length %d exceeds maximum size", length)
		}
	})
}
