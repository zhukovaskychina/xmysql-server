package manager

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
)

const (
	DEFAULT_BUFFER_POOL_SIZE = 16384       // 默认缓冲池大小（页数）
	PAGE_SIZE                = 16384       // 页大小（字节）
	FLUSH_INTERVAL           = time.Second // 刷新间隔
	MAX_DIRTY_RATIO          = 0.25        // 最大脏页比例
	YOUNG_LIST_RATIO         = 0.75        // young list比例
	OLD_LIST_RATIO           = 0.25        // old list比例
	OLD_BLOCK_TIME           = 1000        // old区块时间（毫秒）

	// 自适应刷新参数
	MIN_FLUSH_INTERVAL      = 100 * time.Millisecond // 最小刷新间隔
	MAX_FLUSH_INTERVAL      = 10 * time.Second       // 最大刷新间隔
	BATCH_FLUSH_SIZE        = 100                    // 批量刷新大小
	AGGRESSIVE_FLUSH_RATIO  = 0.75                   // 激进刷新阈值（脏页比例）
	MODERATE_FLUSH_RATIO    = 0.50                   // 中等刷新阈值
	LIGHT_FLUSH_RATIO       = 0.25                   // 轻度刷新阈值
	ADAPTIVE_ADJUST_FACTOR  = 0.1                    // 自适应调整因子
	MAX_FLUSH_PAGES_PER_SEC = 1000                   // 每秒最大刷新页数
)

// BufferPoolConfig 缓冲池配置
type BufferPoolConfig struct {
	// 基本配置
	PoolSize      uint32        // 缓冲池大小（页数）
	PageSize      uint32        // 页面大小（字节）
	FlushInterval time.Duration // 刷新间隔

	// LRU区域大小
	youngSize uint32 // young区大小
	oldSize   uint32 // old区大小

	// 存储提供者
	StorageProvider basic.StorageProvider

	// LRU配置
	YoungListRatio float64 // young list比例
	OldListRatio   float64 // old list比例
	OldBlockTime   int     // old区块时间（毫秒）

	// 预读配置
	PrefetchSize    uint32 // 预读大小
	MaxQueueSize    uint32 // 最大队列大小
	PrefetchWorkers uint32 // 预读工作线程数
	ReadAheadPages  uint32 // 预读页面数
	poolSize        uint32
}

// BufferPoolManager 缓冲池管理器
type BufferPoolManager struct {
	mu sync.RWMutex

	// 核心组件
	bufferPool *buffer_pool.BufferPool // 底层缓冲池
	config     *BufferPoolConfig       // 配置信息

	// 统计信息
	stats struct {
		hits       uint64 // 缓存命中次数
		misses     uint64 // 缓存未命中次数
		evictions  uint64 // 页面驱逐次数
		flushes    uint64 // 页面刷新次数
		pageReads  uint64 // 页面读取次数
		pageWrites uint64 // 页面写入次数
		youngHits  uint64 // young区命中次数
		oldHits    uint64 // old区命中次数
	}

	// 后台线程控制
	stopChan    chan struct{}
	flushTicker *time.Ticker

	// 自适应刷新控制
	currentFlushInterval time.Duration // 当前刷新间隔
	flushRateLimit       int           // 刷新速率限制（页/秒）
	lastFlushTime        time.Time     // 上次刷新时间
	lastFlushCount       int           // 上次刷新页数
}

// NewBufferPoolManager creates a new buffer pool manager
func NewBufferPoolManager(config *BufferPoolConfig) (*BufferPoolManager, error) {
	// Validate configuration
	if config == nil {
		return nil, fmt.Errorf("buffer pool config is required")
	}
	if config.StorageProvider == nil {
		return nil, fmt.Errorf("storage provider is required")
	}

	// Set default values if not specified
	if config.PoolSize == 0 {
		config.PoolSize = DEFAULT_BUFFER_POOL_SIZE
	}
	if config.PageSize == 0 {
		config.PageSize = PAGE_SIZE
	}
	if config.FlushInterval == 0 {
		config.FlushInterval = FLUSH_INTERVAL
	}
	if config.YoungListRatio == 0 {
		config.YoungListRatio = YOUNG_LIST_RATIO
	}
	if config.OldListRatio == 0 {
		config.OldListRatio = OLD_LIST_RATIO
	}
	if config.OldBlockTime == 0 {
		config.OldBlockTime = OLD_BLOCK_TIME
	}

	// 初始化 LRU 区域大小
	config.youngSize = uint32(float64(config.PoolSize) * config.YoungListRatio)
	config.oldSize = config.PoolSize - config.youngSize

	// Create buffer pool config
	bpConfig := &buffer_pool.BufferPoolConfig{
		// Basic configuration
		TotalPages:     config.PoolSize,
		PageSize:       config.PageSize,
		BufferPoolSize: uint64(config.PoolSize) * uint64(config.PageSize),

		// Storage provider
		StorageManager:  newStorageProviderSpaceManager(config.StorageProvider),
		StorageProvider: config.StorageProvider,

		// LRU configuration
		YoungListPercent: config.YoungListRatio,
		OldListPercent:   config.OldListRatio,
		OldBlocksTime:    config.OldBlockTime,

		// Prefetch configuration
		PrefetchSize:    config.PrefetchSize,
		MaxQueueSize:    config.MaxQueueSize,
		PrefetchWorkers: config.PrefetchWorkers,
	}

	// Create buffer pool manager
	bpm := &BufferPoolManager{
		bufferPool:           buffer_pool.NewBufferPool(bpConfig),
		config:               config,
		stopChan:             make(chan struct{}),
		currentFlushInterval: config.FlushInterval,
		flushRateLimit:       MAX_FLUSH_PAGES_PER_SEC,
		lastFlushTime:        time.Now(),
	}

	// Start background threads
	bpm.startBackgroundThreads()

	return bpm, nil
}

// GetPage gets a page from buffer pool
func (bpm *BufferPoolManager) GetPage(spaceID, pageNo uint32) (*buffer_pool.BufferPage, error) {
	// Try to get page from buffer pool
	page, err := bpm.bufferPool.GetPage(spaceID, pageNo)
	if err != nil {
		// Update statistics
		atomic.AddUint64(&bpm.stats.misses, 1)
		atomic.AddUint64(&bpm.stats.pageReads, 1)
		return nil, fmt.Errorf("failed to get page %d: %v", pageNo, err)
	}

	// Update statistics
	atomic.AddUint64(&bpm.stats.hits, 1)

	return page, nil
}

// GetDirtyPage gets a page and marks it as dirty
func (bpm *BufferPoolManager) GetDirtyPage(spaceID, pageNo uint32) (*buffer_pool.BufferPage, error) {
	// Get the page
	page, err := bpm.GetPage(spaceID, pageNo)
	if err != nil {
		return nil, err
	}

	// Mark as dirty
	page.SetDirty(true)

	return page, nil
}

// FlushPage flushes a specific page to disk
func (bpm *BufferPoolManager) FlushPage(spaceID, pageNo uint32) error {
	// Get the page
	page, err := bpm.GetPage(spaceID, pageNo)
	if err != nil {
		return err
	}

	// If not dirty, nothing to do
	if !page.IsDirty() {
		return nil
	}

	// Flush page to disk
	if err := bpm.bufferPool.FlushPage(page); err != nil {
		return fmt.Errorf("failed to flush page %d: %v", pageNo, err)
	}

	// Update statistics
	atomic.AddUint64(&bpm.stats.flushes, 1)
	atomic.AddUint64(&bpm.stats.pageWrites, 1)

	return nil
}

// UnpinPage decrements the pin count of a page
func (bpm *BufferPoolManager) UnpinPage(spaceID, pageNo uint32) error {
	// Get the page
	page, err := bpm.GetPage(spaceID, pageNo)
	if err != nil {
		return err
	}

	// Unpin the page
	page.Unpin()

	return nil
}

// MarkDirty marks a page as dirty
func (bpm *BufferPoolManager) MarkDirty(spaceID, pageNo uint32) error {
	// Get the page
	page, err := bpm.GetPage(spaceID, pageNo)
	if err != nil {
		return err
	}

	// Mark as dirty
	page.SetDirty(true)

	return nil
}

// FlushAllPages flushes all dirty pages to disk
func (bpm *BufferPoolManager) FlushAllPages() error {
	// Get all dirty pages
	dirtyPages := bpm.bufferPool.GetDirtyPages()

	// Flush all dirty pages
	for _, page := range dirtyPages {
		if err := bpm.FlushPage(page.GetSpaceID(), page.GetPageNo()); err != nil {
			return fmt.Errorf("failed to flush all pages: %v", err)
		}
	}

	return nil
}

// backgroundFlush performs background page flushing with adaptive strategy
func (bpm *BufferPoolManager) backgroundFlush() {
	// 获取脏页统计
	dirtyPages := bpm.bufferPool.GetDirtyPages()
	totalPages := bpm.config.PoolSize
	dirtyRatio := float64(len(dirtyPages)) / float64(totalPages)

	// 根据脏页比例调整刷新策略
	flushBatchSize := bpm.calculateFlushBatchSize(dirtyRatio)
	if flushBatchSize == 0 {
		return // 脏页比例很低，不需要刷新
	}

	// 应用速率限制
	flushBatchSize = bpm.applyRateLimit(flushBatchSize)

	// 使用 BufferPool 的刷新策略选择要刷新的页面
	if err := bpm.bufferPool.FlushDirtyPagesWithLimit(flushBatchSize); err != nil {
		logger.Debugf("Error during background flush: %v", err)
	}

	// 更新统计信息
	bpm.lastFlushTime = time.Now()
	bpm.lastFlushCount = flushBatchSize

	// 自适应调整刷新间隔
	bpm.adjustFlushInterval(dirtyRatio)
}

// calculateFlushBatchSize 根据脏页比例计算批量刷新大小
func (bpm *BufferPoolManager) calculateFlushBatchSize(dirtyRatio float64) int {
	var batchSize int

	switch {
	case dirtyRatio >= AGGRESSIVE_FLUSH_RATIO:
		// 激进刷新：脏页比例 >= 75%
		batchSize = BATCH_FLUSH_SIZE * 4
		logger.Debugf("Aggressive flush mode: dirty ratio %.2f%%", dirtyRatio*100)

	case dirtyRatio >= MODERATE_FLUSH_RATIO:
		// 中等刷新：脏页比例 >= 50%
		batchSize = BATCH_FLUSH_SIZE * 2
		logger.Debugf("Moderate flush mode: dirty ratio %.2f%%", dirtyRatio*100)

	case dirtyRatio >= LIGHT_FLUSH_RATIO:
		// 轻度刷新：脏页比例 >= 25%
		batchSize = BATCH_FLUSH_SIZE
		logger.Debugf("Light flush mode: dirty ratio %.2f%%", dirtyRatio*100)

	default:
		// 脏页比例很低，不需要刷新
		batchSize = 0
	}

	return batchSize
}

// applyRateLimit 应用速率限制
func (bpm *BufferPoolManager) applyRateLimit(requestedPages int) int {
	// 计算自上次刷新以来的时间
	elapsed := time.Since(bpm.lastFlushTime)
	if elapsed == 0 {
		elapsed = 1 * time.Millisecond
	}

	// 计算允许的最大页数（基于速率限制）
	maxPagesAllowed := int(float64(bpm.flushRateLimit) * elapsed.Seconds())

	// 返回较小值
	if requestedPages > maxPagesAllowed {
		logger.Debugf("Rate limit applied: requested %d, allowed %d", requestedPages, maxPagesAllowed)
		return maxPagesAllowed
	}

	return requestedPages
}

// adjustFlushInterval 自适应调整刷新间隔
func (bpm *BufferPoolManager) adjustFlushInterval(dirtyRatio float64) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	oldInterval := bpm.currentFlushInterval

	switch {
	case dirtyRatio >= AGGRESSIVE_FLUSH_RATIO:
		// 脏页比例很高，减少刷新间隔（更频繁刷新）
		bpm.currentFlushInterval = time.Duration(float64(bpm.currentFlushInterval) * (1 - ADAPTIVE_ADJUST_FACTOR))

	case dirtyRatio >= MODERATE_FLUSH_RATIO:
		// 脏页比例中等，略微减少刷新间隔
		bpm.currentFlushInterval = time.Duration(float64(bpm.currentFlushInterval) * (1 - ADAPTIVE_ADJUST_FACTOR/2))

	case dirtyRatio < LIGHT_FLUSH_RATIO:
		// 脏页比例很低，增加刷新间隔（减少刷新频率）
		bpm.currentFlushInterval = time.Duration(float64(bpm.currentFlushInterval) * (1 + ADAPTIVE_ADJUST_FACTOR))
	}

	// 限制刷新间隔在合理范围内
	if bpm.currentFlushInterval < MIN_FLUSH_INTERVAL {
		bpm.currentFlushInterval = MIN_FLUSH_INTERVAL
	}
	if bpm.currentFlushInterval > MAX_FLUSH_INTERVAL {
		bpm.currentFlushInterval = MAX_FLUSH_INTERVAL
	}

	// 如果间隔发生变化，重启定时器
	if oldInterval != bpm.currentFlushInterval {
		logger.Debugf("Flush interval adjusted: %v -> %v (dirty ratio: %.2f%%)",
			oldInterval, bpm.currentFlushInterval, dirtyRatio*100)

		// 重启定时器
		if bpm.flushTicker != nil {
			bpm.flushTicker.Stop()
			bpm.flushTicker = time.NewTicker(bpm.currentFlushInterval)
		}
	}
}

// GetStats returns buffer pool statistics
func (bpm *BufferPoolManager) GetStats() map[string]interface{} {
	dirtyPages := bpm.bufferPool.GetDirtyPages()
	dirtyRatio := float64(len(dirtyPages)) / float64(bpm.config.PoolSize)

	return map[string]interface{}{
		"hits":                   atomic.LoadUint64(&bpm.stats.hits),
		"misses":                 atomic.LoadUint64(&bpm.stats.misses),
		"evictions":              atomic.LoadUint64(&bpm.stats.evictions),
		"flushes":                atomic.LoadUint64(&bpm.stats.flushes),
		"page_reads":             atomic.LoadUint64(&bpm.stats.pageReads),
		"page_writes":            atomic.LoadUint64(&bpm.stats.pageWrites),
		"dirty_pages":            len(dirtyPages),
		"dirty_ratio":            dirtyRatio,
		"total_pages":            bpm.config.PoolSize,
		"current_flush_interval": bpm.currentFlushInterval.String(),
		"flush_rate_limit":       bpm.flushRateLimit,
		"last_flush_count":       bpm.lastFlushCount,
	}
}

// GetDirtyPageRatio 获取脏页比例
func (bpm *BufferPoolManager) GetDirtyPageRatio() float64 {
	dirtyPages := bpm.bufferPool.GetDirtyPages()
	return float64(len(dirtyPages)) / float64(bpm.config.PoolSize)
}

// GetDirtyPageCount 获取脏页数量
func (bpm *BufferPoolManager) GetDirtyPageCount() int {
	dirtyPages := bpm.bufferPool.GetDirtyPages()
	return len(dirtyPages)
}

// SetFlushRateLimit 设置刷新速率限制
func (bpm *BufferPoolManager) SetFlushRateLimit(pagesPerSecond int) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	if pagesPerSecond > 0 {
		bpm.flushRateLimit = pagesPerSecond
		logger.Debugf("Flush rate limit set to %d pages/sec", pagesPerSecond)
	}
}

// maintainLRULists 维护LRU列表
func (bpm *BufferPoolManager) maintainLRULists() {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	// 计算命中率
	totalHits := atomic.LoadUint64(&bpm.stats.youngHits) + atomic.LoadUint64(&bpm.stats.oldHits)
	if totalHits > 0 {
		youngHitRatio := float64(atomic.LoadUint64(&bpm.stats.youngHits)) / float64(totalHits)

		// 根据命中率调整young和old区大小
		if youngHitRatio < 0.8 && bpm.config.youngSize > bpm.config.poolSize/4 {
			// 减少young区大小
			bpm.config.youngSize = uint32(float64(bpm.config.youngSize) * 0.95)
			bpm.config.oldSize = bpm.config.poolSize - bpm.config.youngSize
		} else if youngHitRatio > 0.9 && bpm.config.youngSize < bpm.config.poolSize*3/4 {
			// 增加young区大小
			bpm.config.youngSize = uint32(float64(bpm.config.youngSize) * 1.05)
			bpm.config.oldSize = bpm.config.poolSize - bpm.config.youngSize
		}
	}

	// 重置统计信息
	atomic.StoreUint64(&bpm.stats.youngHits, 0)
	atomic.StoreUint64(&bpm.stats.oldHits, 0)
}

// evictPage 驱逐一个页面
func (bpm *BufferPoolManager) evictPage() *buffer_pool.BufferBlock {
	// TODO: 实现页面驱逐策略
	// 1. 优先驱逐未固定的干净页
	// 2. 如果没有干净页，则选择最旧的脏页刷新并驱逐
	return nil
}

// Close 关闭缓冲池管理器
func (bpm *BufferPoolManager) Close() error {
	// 停止后台线程
	close(bpm.stopChan)
	bpm.flushTicker.Stop()

	// 刷新所有脏页
	return bpm.FlushAllPages()
}

// startBackgroundThreads 启动后台线程
func (bpm *BufferPoolManager) startBackgroundThreads() {
	// 创建刷新定时器
	bpm.flushTicker = time.NewTicker(bpm.currentFlushInterval)

	// 启动后台刷新线程
	go func() {
		logger.Debugf("Background flush thread started with interval %v", bpm.currentFlushInterval)

		for {
			select {
			case <-bpm.flushTicker.C:
				// 执行后台刷新
				bpm.backgroundFlush()

			case <-bpm.stopChan:
				// 收到停止信号，退出
				logger.Debugf("Background flush thread stopped")
				return
			}
		}
	}()

	// 启动 LRU 维护线程
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		logger.Debugf("LRU maintenance thread started")

		for {
			select {
			case <-ticker.C:
				// 维护 LRU 列表
				bpm.maintainLRULists()

			case <-bpm.stopChan:
				// 收到停止信号，退出
				logger.Debugf("LRU maintenance thread stopped")
				return
			}
		}
	}()
}

// makePageID 生成页面ID
func makePageID(spaceID, pageNo uint32) uint64 {
	return uint64(spaceID)<<32 | uint64(pageNo)
}
