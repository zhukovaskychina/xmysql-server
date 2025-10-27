package io

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

/*
IOOptimizer 表空间IO优化器

优化表空间的IO操作，提供预读、批量写、IO调度等功能。

核心优化策略:
1. 顺序IO优化: 预读机制、批量读取
2. 随机IO优化: IO合并、请求排序
3. 缓存策略: 读缓存、写缓存
4. IO调度: 优先级队列、电梯算法

设计要点:
- 预读窗口: 基于访问模式动态调整
- 批量写入: 聚合小写入，减少IO次数
- IO调度: 减少磁盘寻道时间
- 缓存分层: 热数据缓存、预读缓存
*/

const (
	// 预读配置
	DefaultReadAheadSize = 64  // 默认预读页数
	MaxReadAheadSize     = 256 // 最大预读页数
	MinReadAheadSize     = 8   // 最小预读页数

	// 批量写配置
	DefaultBatchWriteSize = 32  // 默认批量写页数
	MaxBatchWriteSize     = 128 // 最大批量写页数
	FlushIntervalMS       = 100 // 刷盘间隔(毫秒)

	// 缓存配置
	ReadCacheSize  = 1024 // 读缓存页数
	WriteCacheSize = 512  // 写缓存页数

	// IO调度配置
	IOQueueSize     = 1000 // IO请求队列大小
	MaxConcurrentIO = 32   // 最大并发IO数
)

// IOOptimizer IO优化器
type IOOptimizer struct {
	// 预读管理
	readAhead *ReadAheadManager

	// 批量写管理
	batchWriter *BatchWriteManager

	// IO调度器
	scheduler *IOScheduler

	// 读缓存
	readCache *IOCache

	// 写缓存
	writeCache *IOCache

	// 配置
	config *IOOptimizerConfig

	// 统计
	stats *IOStats

	// 停止信号
	stopChan chan struct{}

	mu sync.RWMutex
}

// IOOptimizerConfig IO优化器配置
type IOOptimizerConfig struct {
	EnableReadAhead   bool
	EnableBatchWrite  bool
	EnableIOScheduler bool
	EnableReadCache   bool
	EnableWriteCache  bool

	ReadAheadSize  int
	BatchWriteSize int
	FlushInterval  int // 毫秒
}

// IOStats IO统计
type IOStats struct {
	// 读统计
	totalReads      uint64
	sequentialReads uint64
	randomReads     uint64
	readAheadHits   uint64
	readAheadMisses uint64

	// 写统计
	totalWrites  uint64
	batchWrites  uint64
	singleWrites uint64

	// 缓存统计
	readCacheHits    uint64
	readCacheMisses  uint64
	writeCacheHits   uint64
	writeCacheMisses uint64

	// 性能统计
	avgReadLatency  uint64 // 纳秒
	avgWriteLatency uint64 // 纳秒
	totalIOTime     uint64 // 纳秒
}

// ReadAheadManager 预读管理器
type ReadAheadManager struct {
	// 预读窗口
	windowSize int

	// 访问模式检测
	lastPageNo    uint32
	sequenceCount int

	// 预读缓存
	prefetchCache map[uint32][]byte
	cacheMu       sync.RWMutex

	// 配置
	enabled  bool
	adaptive bool // 自适应预读
}

// BatchWriteManager 批量写管理器
type BatchWriteManager struct {
	// 写缓冲区
	buffer   map[uint32][]byte
	bufferMu sync.Mutex

	// 刷盘控制
	flushTimer *time.Timer

	// 配置
	enabled       bool
	batchSize     int
	flushInterval time.Duration
}

// IOScheduler IO调度器
type IOScheduler struct {
	// IO请求队列
	queue   *list.List
	queueMu sync.Mutex

	// 当前IO位置（电梯算法）
	currentPos uint32
	direction  int // 1: 向上, -1: 向下

	// 工作协程
	workers int

	// 配置
	enabled bool
}

// IORequest IO请求
type IORequest struct {
	Type       string // "read" or "write"
	SpaceID    uint32
	PageNo     uint32
	Data       []byte
	Priority   int
	Timestamp  time.Time
	ResultChan chan *IOResult
}

// IOResult IO结果
type IOResult struct {
	Data  []byte
	Error error
}

// IOCache IO缓存
type IOCache struct {
	cache   map[uint32][]byte
	lruList *list.List
	lruMap  map[uint32]*list.Element
	maxSize int
	mu      sync.RWMutex
}

// NewIOOptimizer 创建IO优化器
func NewIOOptimizer(config *IOOptimizerConfig) *IOOptimizer {
	if config == nil {
		config = &IOOptimizerConfig{
			EnableReadAhead:   true,
			EnableBatchWrite:  true,
			EnableIOScheduler: true,
			EnableReadCache:   true,
			EnableWriteCache:  true,
			ReadAheadSize:     DefaultReadAheadSize,
			BatchWriteSize:    DefaultBatchWriteSize,
			FlushInterval:     FlushIntervalMS,
		}
	}

	ioo := &IOOptimizer{
		config:   config,
		stats:    &IOStats{},
		stopChan: make(chan struct{}),
	}

	// 初始化组件
	if config.EnableReadAhead {
		ioo.readAhead = &ReadAheadManager{
			windowSize:    config.ReadAheadSize,
			prefetchCache: make(map[uint32][]byte),
			enabled:       true,
			adaptive:      true,
		}
	}

	if config.EnableBatchWrite {
		ioo.batchWriter = &BatchWriteManager{
			buffer:        make(map[uint32][]byte),
			enabled:       true,
			batchSize:     config.BatchWriteSize,
			flushInterval: time.Duration(config.FlushInterval) * time.Millisecond,
		}
		go ioo.batchWriteWorker()
	}

	if config.EnableIOScheduler {
		ioo.scheduler = &IOScheduler{
			queue:     list.New(),
			enabled:   true,
			workers:   4,
			direction: 1,
		}
		go ioo.ioSchedulerWorker()
	}

	if config.EnableReadCache {
		ioo.readCache = NewIOCache(ReadCacheSize)
	}

	if config.EnableWriteCache {
		ioo.writeCache = NewIOCache(WriteCacheSize)
	}

	return ioo
}

// ReadPage 读取页面（带优化）
func (ioo *IOOptimizer) ReadPage(spaceID, pageNo uint32) ([]byte, error) {
	startTime := time.Now()
	defer func() {
		elapsed := time.Since(startTime).Nanoseconds()
		atomic.AddUint64(&ioo.stats.avgReadLatency, uint64(elapsed))
		atomic.AddUint64(&ioo.stats.totalIOTime, uint64(elapsed))
	}()

	atomic.AddUint64(&ioo.stats.totalReads, 1)

	// 1. 检查读缓存
	if ioo.config.EnableReadCache {
		if data := ioo.readCache.Get(pageNo); data != nil {
			atomic.AddUint64(&ioo.stats.readCacheHits, 1)
			return data, nil
		}
		atomic.AddUint64(&ioo.stats.readCacheMisses, 1)
	}

	// 2. 检查预读缓存
	if ioo.config.EnableReadAhead {
		if data := ioo.readAhead.checkPrefetchCache(pageNo); data != nil {
			atomic.AddUint64(&ioo.stats.readAheadHits, 1)
			return data, nil
		}
		atomic.AddUint64(&ioo.stats.readAheadMisses, 1)
	}

	// 3. 实际读取
	data, err := ioo.doRead(spaceID, pageNo)
	if err != nil {
		return nil, err
	}

	// 4. 更新缓存
	if ioo.config.EnableReadCache {
		ioo.readCache.Put(pageNo, data)
	}

	// 5. 触发预读
	if ioo.config.EnableReadAhead {
		ioo.triggerReadAhead(spaceID, pageNo)
	}

	return data, nil
}

// WritePage 写入页面（带优化）
func (ioo *IOOptimizer) WritePage(spaceID, pageNo uint32, data []byte) error {
	startTime := time.Now()
	defer func() {
		elapsed := time.Since(startTime).Nanoseconds()
		atomic.AddUint64(&ioo.stats.avgWriteLatency, uint64(elapsed))
		atomic.AddUint64(&ioo.stats.totalIOTime, uint64(elapsed))
	}()

	atomic.AddUint64(&ioo.stats.totalWrites, 1)

	// 1. 更新写缓存
	if ioo.config.EnableWriteCache {
		ioo.writeCache.Put(pageNo, data)
	}

	// 2. 批量写
	if ioo.config.EnableBatchWrite {
		return ioo.batchWriter.addWrite(pageNo, data)
	}

	// 3. 直接写入
	atomic.AddUint64(&ioo.stats.singleWrites, 1)
	return ioo.doWrite(spaceID, pageNo, data)
}

// doRead 实际读取操作（模拟）
func (ioo *IOOptimizer) doRead(spaceID, pageNo uint32) ([]byte, error) {
	// 这里应该调用实际的磁盘IO操作
	// 当前为模拟实现
	data := make([]byte, 16384) // 16KB页面
	return data, nil
}

// doWrite 实际写入操作（模拟）
func (ioo *IOOptimizer) doWrite(spaceID, pageNo uint32, data []byte) error {
	// 这里应该调用实际的磁盘IO操作
	// 当前为模拟实现
	return nil
}

// triggerReadAhead 触发预读
func (ioo *IOOptimizer) triggerReadAhead(spaceID, pageNo uint32) {
	ioo.readAhead.cacheMu.Lock()
	defer ioo.readAhead.cacheMu.Unlock()

	// 检测访问模式
	isSequential := false
	if pageNo == ioo.readAhead.lastPageNo+1 {
		ioo.readAhead.sequenceCount++
		if ioo.readAhead.sequenceCount >= 3 {
			isSequential = true
			atomic.AddUint64(&ioo.stats.sequentialReads, 1)
		}
	} else {
		ioo.readAhead.sequenceCount = 0
		atomic.AddUint64(&ioo.stats.randomReads, 1)
	}
	ioo.readAhead.lastPageNo = pageNo

	// 顺序访问时触发预读
	if isSequential {
		windowSize := ioo.readAhead.windowSize
		if ioo.readAhead.adaptive {
			// 自适应调整预读窗口
			if ioo.readAhead.sequenceCount > 10 {
				windowSize = min(windowSize*2, MaxReadAheadSize)
			}
		}

		// 异步预读
		go func() {
			for i := 1; i <= windowSize; i++ {
				nextPageNo := pageNo + uint32(i)
				if _, exists := ioo.readAhead.prefetchCache[nextPageNo]; !exists {
					data, err := ioo.doRead(spaceID, nextPageNo)
					if err == nil {
						ioo.readAhead.cacheMu.Lock()
						ioo.readAhead.prefetchCache[nextPageNo] = data
						ioo.readAhead.cacheMu.Unlock()
					}
				}
			}
		}()
	}
}

// checkPrefetchCache 检查预读缓存
func (ram *ReadAheadManager) checkPrefetchCache(pageNo uint32) []byte {
	ram.cacheMu.RLock()
	defer ram.cacheMu.RUnlock()

	data, exists := ram.prefetchCache[pageNo]
	if exists {
		delete(ram.prefetchCache, pageNo) // 使用后删除
		return data
	}
	return nil
}

// addWrite 添加到批量写缓冲
func (bwm *BatchWriteManager) addWrite(pageNo uint32, data []byte) error {
	bwm.bufferMu.Lock()
	defer bwm.bufferMu.Unlock()

	bwm.buffer[pageNo] = data

	// 达到批量大小时立即刷盘
	if len(bwm.buffer) >= bwm.batchSize {
		return bwm.flush()
	}

	return nil
}

// flush 刷盘
func (bwm *BatchWriteManager) flush() error {
	if len(bwm.buffer) == 0 {
		return nil
	}

	// 批量写入
	for pageNo, data := range bwm.buffer {
		// 实际写入操作
		_ = pageNo
		_ = data
	}

	// 清空缓冲区
	bwm.buffer = make(map[uint32][]byte)
	return nil
}

// batchWriteWorker 批量写工作协程
func (ioo *IOOptimizer) batchWriteWorker() {
	ticker := time.NewTicker(ioo.batchWriter.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ioo.batchWriter.bufferMu.Lock()
			ioo.batchWriter.flush()
			ioo.batchWriter.bufferMu.Unlock()

		case <-ioo.stopChan:
			return
		}
	}
}

// ioSchedulerWorker IO调度工作协程
func (ioo *IOOptimizer) ioSchedulerWorker() {
	for {
		select {
		case <-ioo.stopChan:
			return
		default:
			ioo.scheduler.processNextRequest()
			time.Sleep(1 * time.Millisecond)
		}
	}
}

// processNextRequest 处理下一个IO请求（电梯算法）
func (sched *IOScheduler) processNextRequest() {
	sched.queueMu.Lock()
	defer sched.queueMu.Unlock()

	if sched.queue.Len() == 0 {
		return
	}

	// 电梯算法：选择与当前方向一致且最近的请求
	var selectedElem *list.Element
	var minDistance uint32 = ^uint32(0)

	for e := sched.queue.Front(); e != nil; e = e.Next() {
		req := e.Value.(*IORequest)

		if sched.direction == 1 && req.PageNo >= sched.currentPos {
			distance := req.PageNo - sched.currentPos
			if distance < minDistance {
				minDistance = distance
				selectedElem = e
			}
		} else if sched.direction == -1 && req.PageNo <= sched.currentPos {
			distance := sched.currentPos - req.PageNo
			if distance < minDistance {
				minDistance = distance
				selectedElem = e
			}
		}
	}

	// 如果当前方向没有请求，改变方向
	if selectedElem == nil {
		sched.direction = -sched.direction
		return
	}

	// 处理请求
	req := selectedElem.Value.(*IORequest)
	sched.queue.Remove(selectedElem)
	sched.currentPos = req.PageNo

	// 实际执行IO（这里简化处理）
	go func() {
		var result IOResult
		if req.Type == "read" {
			result.Data = make([]byte, 16384)
		}
		req.ResultChan <- &result
	}()
}

// NewIOCache 创建IO缓存
func NewIOCache(maxSize int) *IOCache {
	return &IOCache{
		cache:   make(map[uint32][]byte),
		lruList: list.New(),
		lruMap:  make(map[uint32]*list.Element),
		maxSize: maxSize,
	}
}

// Get 获取缓存
func (c *IOCache) Get(key uint32) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if elem, exists := c.lruMap[key]; exists {
		c.lruList.MoveToFront(elem)
		return c.cache[key]
	}
	return nil
}

// Put 放入缓存
func (c *IOCache) Put(key uint32, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 如果已存在，更新并移到前面
	if elem, exists := c.lruMap[key]; exists {
		c.lruList.MoveToFront(elem)
		c.cache[key] = value
		return
	}

	// 检查容量
	if len(c.cache) >= c.maxSize {
		// 淘汰最久未使用的
		oldest := c.lruList.Back()
		if oldest != nil {
			oldKey := oldest.Value.(uint32)
			delete(c.cache, oldKey)
			delete(c.lruMap, oldKey)
			c.lruList.Remove(oldest)
		}
	}

	// 添加新项
	elem := c.lruList.PushFront(key)
	c.lruMap[key] = elem
	c.cache[key] = value
}

// GetStats 获取统计信息
func (ioo *IOOptimizer) GetStats() *IOStats {
	stats := &IOStats{}

	stats.totalReads = atomic.LoadUint64(&ioo.stats.totalReads)
	stats.sequentialReads = atomic.LoadUint64(&ioo.stats.sequentialReads)
	stats.randomReads = atomic.LoadUint64(&ioo.stats.randomReads)
	stats.readAheadHits = atomic.LoadUint64(&ioo.stats.readAheadHits)
	stats.readAheadMisses = atomic.LoadUint64(&ioo.stats.readAheadMisses)
	stats.totalWrites = atomic.LoadUint64(&ioo.stats.totalWrites)
	stats.batchWrites = atomic.LoadUint64(&ioo.stats.batchWrites)
	stats.singleWrites = atomic.LoadUint64(&ioo.stats.singleWrites)
	stats.readCacheHits = atomic.LoadUint64(&ioo.stats.readCacheHits)
	stats.readCacheMisses = atomic.LoadUint64(&ioo.stats.readCacheMisses)
	stats.avgReadLatency = atomic.LoadUint64(&ioo.stats.avgReadLatency)
	stats.avgWriteLatency = atomic.LoadUint64(&ioo.stats.avgWriteLatency)

	return stats
}

// Stop 停止优化器
func (ioo *IOOptimizer) Stop() {
	close(ioo.stopChan)

	// 刷盘所有待写数据
	if ioo.batchWriter != nil {
		ioo.batchWriter.bufferMu.Lock()
		ioo.batchWriter.flush()
		ioo.batchWriter.bufferMu.Unlock()
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
