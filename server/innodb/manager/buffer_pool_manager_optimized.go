package manager

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
)

// OptimizedBufferPoolManager 优化的缓冲池管理器
// 主要优化点：
// 1. 使用优化的LRU缓存减少锁竞争
// 2. 读写锁分离，提高并发性能
// 3. 后台刷新线程优化
// 4. 更精细的统计信息
type OptimizedBufferPoolManager struct {
	// 读写分离的锁
	mu sync.RWMutex

	// 核心组件
	lruCache *buffer_pool.OptimizedLRUCache // 使用优化的LRU缓存
	config   *BufferPoolConfig              // 配置信息
	storage  basic.StorageProvider          // 存储提供者

	// 页面缓存池
	pagePool sync.Pool // 对象池，减少内存分配

	// 统计信息（使用原子操作）
	stats struct {
		hits          uint64 // 缓存命中次数
		misses        uint64 // 缓存未命中次数
		evictions     uint64 // 页面驱逐次数
		flushes       uint64 // 页面刷新次数
		pageReads     uint64 // 页面读取次数
		pageWrites    uint64 // 页面写入次数
		youngHits     uint64 // young区命中次数
		oldHits       uint64 // old区命中次数
		dirtyPages    uint64 // 脏页数量
		totalPages    uint64 // 总页数
		backgroundOps uint64 // 后台操作次数
	}

	// 后台线程控制
	stopChan    chan struct{}
	flushTicker *time.Ticker
	wg          sync.WaitGroup

	// 脏页管理
	dirtyPageList map[uint64]*buffer_pool.BufferPage // 脏页列表
	dirtyMutex    sync.RWMutex                       // 脏页列表锁

	// 预读队列
	prefetchQueue chan PrefetchRequest
	prefetchWg    sync.WaitGroup
}

// PrefetchRequest 预读请求
type PrefetchRequest struct {
	SpaceID uint32
	PageNo  uint32
}

// NewOptimizedBufferPoolManager 创建优化的缓冲池管理器
func NewOptimizedBufferPoolManager(config *BufferPoolConfig) (*OptimizedBufferPoolManager, error) {
	// 验证配置
	if config == nil {
		return nil, fmt.Errorf("buffer pool config is required")
	}
	if config.StorageProvider == nil {
		return nil, fmt.Errorf("storage provider is required")
	}

	// 设置默认值
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
	if config.PrefetchWorkers == 0 {
		config.PrefetchWorkers = 2 // 默认2个预读工作线程
	}
	if config.MaxQueueSize == 0 {
		config.MaxQueueSize = 1000 // 默认队列大小
	}

	// 创建优化的LRU缓存
	lruCache := buffer_pool.NewOptimizedLRUCache(
		int(config.PoolSize),
		config.YoungListRatio,
		config.OldListRatio,
		config.OldBlockTime,
	)

	// 创建管理器
	bpm := &OptimizedBufferPoolManager{
		lruCache:      lruCache,
		config:        config,
		storage:       config.StorageProvider,
		stopChan:      make(chan struct{}),
		dirtyPageList: make(map[uint64]*buffer_pool.BufferPage),
		prefetchQueue: make(chan PrefetchRequest, config.MaxQueueSize),
	}

	// 初始化对象池
	bpm.pagePool.New = func() interface{} {
		return &buffer_pool.BufferPage{}
	}

	// 启动后台线程
	bpm.startBackgroundThreads()

	return bpm, nil
}

// GetPage 获取页面（优化版本）
func (bpm *OptimizedBufferPoolManager) GetPage(spaceID, pageNo uint32) (*buffer_pool.BufferPage, error) {
	// 首先尝试从缓存获取
	if block, err := bpm.lruCache.Get(spaceID, pageNo); err == nil {
		atomic.AddUint64(&bpm.stats.hits, 1)

		// 包装为BufferPage
		page := bpm.pagePool.Get().(*buffer_pool.BufferPage)
		page.Init(spaceID, pageNo, block.GetContent())

		return page, nil
	}

	// 缓存未命中，从存储读取
	atomic.AddUint64(&bpm.stats.misses, 1)
	return bpm.loadPageFromStorage(spaceID, pageNo)
}

// loadPageFromStorage 从存储加载页面
func (bpm *OptimizedBufferPoolManager) loadPageFromStorage(spaceID, pageNo uint32) (*buffer_pool.BufferPage, error) {
	// 从存储读取页面数据
	data, err := bpm.storage.ReadPage(spaceID, pageNo)
	if err != nil {
		atomic.AddUint64(&bpm.stats.pageReads, 1)
		return nil, fmt.Errorf("failed to read page from storage: %v", err)
	}

	// 创建BufferBlock
	bufferPage := buffer_pool.NewBufferPage(spaceID, pageNo)
	bufferPage.SetContent(data)
	block := buffer_pool.NewBufferBlock(bufferPage)

	// 添加到LRU缓存
	if err := bpm.lruCache.Set(spaceID, pageNo, block); err != nil {
		return nil, fmt.Errorf("failed to add page to cache: %v", err)
	}

	// 创建BufferPage
	page := bpm.pagePool.Get().(*buffer_pool.BufferPage)
	page.Init(spaceID, pageNo, data)

	atomic.AddUint64(&bpm.stats.pageReads, 1)
	atomic.AddUint64(&bpm.stats.totalPages, 1)

	return page, nil
}

// GetDirtyPage 获取页面并标记为脏页
func (bpm *OptimizedBufferPoolManager) GetDirtyPage(spaceID, pageNo uint32) (*buffer_pool.BufferPage, error) {
	page, err := bpm.GetPage(spaceID, pageNo)
	if err != nil {
		return nil, err
	}

	// 标记为脏页
	page.SetDirty(true)

	// 添加到脏页列表
	pageID := makePageID(spaceID, pageNo)
	bpm.dirtyMutex.Lock()
	bpm.dirtyPageList[pageID] = page
	bpm.dirtyMutex.Unlock()

	atomic.AddUint64(&bpm.stats.dirtyPages, 1)

	return page, nil
}

// FlushPage 刷新特定页面到磁盘
func (bpm *OptimizedBufferPoolManager) FlushPage(spaceID, pageNo uint32) error {
	page, err := bpm.GetPage(spaceID, pageNo)
	if err != nil {
		return err
	}

	// 如果不是脏页，直接返回
	if !page.IsDirty() {
		return nil
	}

	// 写入存储
	if err := bpm.storage.WritePage(spaceID, pageNo, page.GetData()); err != nil {
		return fmt.Errorf("failed to write page to storage: %v", err)
	}

	// 清除脏标记
	page.SetDirty(false)

	// 从脏页列表移除
	pageID := makePageID(spaceID, pageNo)
	bpm.dirtyMutex.Lock()
	delete(bpm.dirtyPageList, pageID)
	bpm.dirtyMutex.Unlock()

	atomic.AddUint64(&bpm.stats.flushes, 1)
	atomic.AddUint64(&bpm.stats.pageWrites, 1)
	atomic.AddUint64(&bpm.stats.dirtyPages, ^uint64(0)) // 原子减1

	// 返回页面到对象池
	bpm.pagePool.Put(page)

	return nil
}

// UnpinPage 减少页面的引用计数
func (bpm *OptimizedBufferPoolManager) UnpinPage(spaceID, pageNo uint32) error {
	page, err := bpm.GetPage(spaceID, pageNo)
	if err != nil {
		return err
	}

	// 减少引用计数（简化实现，实际应该有pin count管理）
	page.Unpin()

	return nil
}

// MarkDirty 标记页面为脏页
func (bpm *OptimizedBufferPoolManager) MarkDirty(spaceID, pageNo uint32) error {
	page, err := bpm.GetPage(spaceID, pageNo)
	if err != nil {
		return err
	}

	if !page.IsDirty() {
		page.SetDirty(true)

		// 添加到脏页列表
		pageID := makePageID(spaceID, pageNo)
		bpm.dirtyMutex.Lock()
		bpm.dirtyPageList[pageID] = page
		bpm.dirtyMutex.Unlock()

		atomic.AddUint64(&bpm.stats.dirtyPages, 1)
	}

	return nil
}

// FlushAllPages 刷新所有脏页
func (bpm *OptimizedBufferPoolManager) FlushAllPages() error {
	// 获取所有脏页
	bpm.dirtyMutex.RLock()
	dirtyPages := make([]*buffer_pool.BufferPage, 0, len(bpm.dirtyPageList))
	for _, page := range bpm.dirtyPageList {
		dirtyPages = append(dirtyPages, page)
	}
	bpm.dirtyMutex.RUnlock()

	// 并发刷新脏页
	errChan := make(chan error, len(dirtyPages))
	sem := make(chan struct{}, 10) // 限制并发数

	for _, page := range dirtyPages {
		go func(p *buffer_pool.BufferPage) {
			sem <- struct{}{}
			defer func() { <-sem }()

			if err := bpm.FlushPage(p.GetSpaceID(), p.GetPageNo()); err != nil {
				errChan <- err
				return
			}
			errChan <- nil
		}(page)
	}

	// 等待所有刷新完成
	var lastErr error
	for i := 0; i < len(dirtyPages); i++ {
		if err := <-errChan; err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// PrefetchPage 预读页面
func (bpm *OptimizedBufferPoolManager) PrefetchPage(spaceID, pageNo uint32) {
	select {
	case bpm.prefetchQueue <- PrefetchRequest{SpaceID: spaceID, PageNo: pageNo}:
		// 成功添加到预读队列
	default:
		// 队列满了，忽略这次预读请求
	}
}

// GetStats 获取统计信息
func (bpm *OptimizedBufferPoolManager) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"hits":           atomic.LoadUint64(&bpm.stats.hits),
		"misses":         atomic.LoadUint64(&bpm.stats.misses),
		"hit_rate":       bpm.calculateHitRate(),
		"evictions":      atomic.LoadUint64(&bpm.stats.evictions),
		"flushes":        atomic.LoadUint64(&bpm.stats.flushes),
		"page_reads":     atomic.LoadUint64(&bpm.stats.pageReads),
		"page_writes":    atomic.LoadUint64(&bpm.stats.pageWrites),
		"young_hits":     atomic.LoadUint64(&bpm.stats.youngHits),
		"old_hits":       atomic.LoadUint64(&bpm.stats.oldHits),
		"dirty_pages":    atomic.LoadUint64(&bpm.stats.dirtyPages),
		"total_pages":    atomic.LoadUint64(&bpm.stats.totalPages),
		"background_ops": atomic.LoadUint64(&bpm.stats.backgroundOps),
		"cache_size":     bpm.lruCache.Len(),
	}
}

// calculateHitRate 计算缓存命中率
func (bpm *OptimizedBufferPoolManager) calculateHitRate() float64 {
	hits := atomic.LoadUint64(&bpm.stats.hits)
	misses := atomic.LoadUint64(&bpm.stats.misses)
	total := hits + misses

	if total == 0 {
		return 0.0
	}

	return float64(hits) / float64(total)
}

// Close 关闭缓冲池管理器
func (bpm *OptimizedBufferPoolManager) Close() error {
	// 停止后台线程
	close(bpm.stopChan)

	// 等待所有后台线程结束
	bpm.wg.Wait()
	bpm.prefetchWg.Wait()

	// 刷新所有脏页
	if err := bpm.FlushAllPages(); err != nil {
		return fmt.Errorf("failed to flush pages during close: %v", err)
	}

	// 清空缓存
	bpm.lruCache.Purge()

	// 停止定时器
	if bpm.flushTicker != nil {
		bpm.flushTicker.Stop()
	}

	// 关闭预读队列
	close(bpm.prefetchQueue)

	return nil
}

// startBackgroundThreads 启动后台线程
func (bpm *OptimizedBufferPoolManager) startBackgroundThreads() {
	// 启动刷新线程
	bpm.flushTicker = time.NewTicker(bpm.config.FlushInterval)
	bpm.wg.Add(1)
	go bpm.backgroundFlush()

	// 启动LRU维护线程
	bpm.wg.Add(1)
	go bpm.backgroundLRUMaintenance()

	// 启动预读工作线程
	for i := uint32(0); i < bpm.config.PrefetchWorkers; i++ {
		bpm.prefetchWg.Add(1)
		go bpm.prefetchWorker()
	}
}

// backgroundFlush 后台刷新线程
func (bpm *OptimizedBufferPoolManager) backgroundFlush() {
	defer bpm.wg.Done()

	for {
		select {
		case <-bpm.stopChan:
			return
		case <-bpm.flushTicker.C:
			// 检查脏页比例
			dirtyCount := atomic.LoadUint64(&bpm.stats.dirtyPages)
			totalCount := atomic.LoadUint64(&bpm.stats.totalPages)

			if totalCount > 0 {
				dirtyRatio := float64(dirtyCount) / float64(totalCount)
				if dirtyRatio > MAX_DIRTY_RATIO {
					// 脏页比例过高，触发刷新
					bpm.flushSomeDirtyPages()
				}
			}

			atomic.AddUint64(&bpm.stats.backgroundOps, 1)
		}
	}
}

// backgroundLRUMaintenance 后台LRU维护线程
func (bpm *OptimizedBufferPoolManager) backgroundLRUMaintenance() {
	defer bpm.wg.Done()

	ticker := time.NewTicker(5 * time.Second) // 每5秒维护一次
	defer ticker.Stop()

	for {
		select {
		case <-bpm.stopChan:
			return
		case <-ticker.C:
			// 检查缓存大小，如果超过限制则进行淘汰
			if bpm.lruCache.Len() > bpm.config.PoolSize {
				// 淘汰一些页面
				for i := 0; i < 10 && bpm.lruCache.Len() > bpm.config.PoolSize; i++ {
					if page := bpm.lruCache.Evict(); page != nil {
						atomic.AddUint64(&bpm.stats.evictions, 1)
					}
				}
			}

			atomic.AddUint64(&bpm.stats.backgroundOps, 1)
		}
	}
}

// prefetchWorker 预读工作线程
func (bpm *OptimizedBufferPoolManager) prefetchWorker() {
	defer bpm.prefetchWg.Done()

	for {
		select {
		case <-bpm.stopChan:
			return
		case req, ok := <-bpm.prefetchQueue:
			if !ok {
				return
			}

			// 检查页面是否已在缓存中
			if bpm.lruCache.Has(req.SpaceID, req.PageNo) {
				continue
			}

			// 预读页面（异步）
			go func(spaceID, pageNo uint32) {
				if _, err := bpm.loadPageFromStorage(spaceID, pageNo); err != nil {
					// 预读失败，记录但不处理
				}
			}(req.SpaceID, req.PageNo)
		}
	}
}

// flushSomeDirtyPages 刷新一些脏页
func (bpm *OptimizedBufferPoolManager) flushSomeDirtyPages() {
	bpm.dirtyMutex.RLock()

	// 批量刷新，最多刷新100个页面
	count := 0
	const maxFlush = 100

	for _, page := range bpm.dirtyPageList {
		if count >= maxFlush {
			break
		}

		// 异步刷新
		go func(p *buffer_pool.BufferPage) {
			bpm.FlushPage(p.GetSpaceID(), p.GetPageNo())
		}(page)

		count++
	}

	bpm.dirtyMutex.RUnlock()
}
