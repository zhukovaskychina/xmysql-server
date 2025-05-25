package buffer_pool

import (
	"math"
	"sync"
	"time"
)

// AutoTuner 自动调优器
type AutoTuner struct {
	bufferPool *BufferPool
	stats      *BufferPoolStats

	// 调优参数
	youngListRatio   float64 // young列表比例
	oldListRatio     float64 // old列表比例
	targetHitRatio   float64 // 目标命中率
	minYoungRatio    float64 // 最小young比例
	maxYoungRatio    float64 // 最大young比例
	adjustmentFactor float64 // 调整因子

	// 监控窗口
	windowSize     time.Duration // 监控窗口大小
	lastAdjustment time.Time     // 上次调整时间

	// 并发控制
	mu sync.Mutex
}

// NewAutoTuner 创建自动调优器
func NewAutoTuner(bufferPool *BufferPool, stats *BufferPoolStats) *AutoTuner {
	return &AutoTuner{
		bufferPool:       bufferPool,
		stats:            stats,
		youngListRatio:   0.625, // 默认值
		oldListRatio:     0.375,
		targetHitRatio:   0.95,
		minYoungRatio:    0.25,
		maxYoungRatio:    0.75,
		adjustmentFactor: 0.1,
		windowSize:       time.Minute * 5,
	}
}

// Start 启动自动调优
func (at *AutoTuner) Start() {
	go at.tuningLoop()
}

// tuningLoop 调优循环
func (at *AutoTuner) tuningLoop() {
	ticker := time.NewTicker(at.windowSize)
	defer ticker.Stop()

	for range ticker.C {
		at.adjust()
	}
}

// adjust 调整缓冲池参数
func (at *AutoTuner) adjust() {
	at.mu.Lock()
	defer at.mu.Unlock()

	// 1. 分析当前性能
	currentHitRatio := at.stats.GetHitRatio()
	currentPrefetchHitRatio := at.stats.GetPrefetchHitRatio()
	avgReadLatency := at.stats.GetAvgReadLatency()
	avgWriteLatency := at.stats.GetAvgWriteLatency()

	// 2. 调整young/old列表比例
	if currentHitRatio < at.targetHitRatio {
		// 如果命中率低于目标,增加young列表比例
		newYoungRatio := at.youngListRatio + at.adjustmentFactor
		if newYoungRatio <= at.maxYoungRatio {
			at.youngListRatio = newYoungRatio
			at.oldListRatio = 1 - newYoungRatio
		}
	} else {
		// 如果命中率高于目标,尝试减小young列表以节省内存
		newYoungRatio := at.youngListRatio - at.adjustmentFactor
		if newYoungRatio >= at.minYoungRatio {
			at.youngListRatio = newYoungRatio
			at.oldListRatio = 1 - newYoungRatio
		}
	}

	// 3. 调整预读参数
	at.adjustPrefetchParameters(currentPrefetchHitRatio)

	// 4. 调整刷新策略
	at.adjustFlushStrategy(avgWriteLatency)

	// 5. 调整LRU策略
	at.adjustLRUStrategy(avgReadLatency)

	// 6. 重置统计信息
	at.stats.Reset()
	at.lastAdjustment = time.Now()
}

// adjustPrefetchParameters 调整预读参数
func (at *AutoTuner) adjustPrefetchParameters(hitRatio float64) {
	// 根据预读命中率调整预读大小和队列长度
	if hitRatio < 0.5 {
		// 预读效果不好,减小预读量
		at.bufferPool.prefetchManager.prefetchSize = int(float64(at.bufferPool.prefetchManager.prefetchSize) * 0.8)
	} else if hitRatio > 0.9 {
		// 预读效果好,增加预读量
		at.bufferPool.prefetchManager.prefetchSize = int(float64(at.bufferPool.prefetchManager.prefetchSize) * 1.2)
	}
}

// adjustFlushStrategy 调整刷新策略
func (at *AutoTuner) adjustFlushStrategy(avgWriteLatency float64) {
	// 根据写入延迟调整刷新阈值
	dirtyRatio := at.bufferPool.GetDirtyPageRatio()
	if avgWriteLatency > 1e6 { // 1ms
		// 写入延迟高,降低脏页比例阈值
		at.bufferPool.flushThreshold = math.Max(0.5, dirtyRatio*0.8)
	} else {
		// 写入延迟低,提高脏页比例阈值
		at.bufferPool.flushThreshold = math.Min(0.9, dirtyRatio*1.2)
	}
}

// adjustLRUStrategy 调整LRU策略
func (at *AutoTuner) adjustLRUStrategy(avgReadLatency float64) {
	// 根据读取延迟调整LRU参数
	if avgReadLatency > 1e6 { // 1ms
		// 读取延迟高,增加old块的存活时间
		at.bufferPool.oldBlockTime = int(float64(at.bufferPool.oldBlockTime) * 1.2)
	} else {
		// 读取延迟低,减少old块的存活时间
		at.bufferPool.oldBlockTime = int(float64(at.bufferPool.oldBlockTime) * 0.8)
	}
}

// GetCurrentParameters 获取当前参数
func (at *AutoTuner) GetCurrentParameters() map[string]interface{} {
	at.mu.Lock()
	defer at.mu.Unlock()

	return map[string]interface{}{
		"young_list_ratio": at.youngListRatio,
		"old_list_ratio":   at.oldListRatio,
		"target_hit_ratio": at.targetHitRatio,
		"prefetch_size":    at.bufferPool.prefetchManager.prefetchSize,
		"flush_threshold":  at.bufferPool.flushThreshold,
		"old_block_time":   at.bufferPool.oldBlockTime,
		"last_adjustment":  at.lastAdjustment,
	}
}

// SetTargetHitRatio 设置目标命中率
func (at *AutoTuner) SetTargetHitRatio(ratio float64) {
	at.mu.Lock()
	defer at.mu.Unlock()

	if ratio > 0 && ratio <= 1 {
		at.targetHitRatio = ratio
	}
}

// SetAdjustmentFactor 设置调整因子
func (at *AutoTuner) SetAdjustmentFactor(factor float64) {
	at.mu.Lock()
	defer at.mu.Unlock()

	if factor > 0 && factor < 1 {
		at.adjustmentFactor = factor
	}
}

// SetWindowSize 设置监控窗口大小
func (at *AutoTuner) SetWindowSize(duration time.Duration) {
	at.mu.Lock()
	defer at.mu.Unlock()

	if duration > 0 {
		at.windowSize = duration
	}
}
