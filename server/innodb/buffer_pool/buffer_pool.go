package buffer_pool

import (
	"container/list"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"sync"
	"sync/atomic"
)

// BufferPool represents the InnoDB buffer pool
type BufferPool struct {
	mu sync.RWMutex

	// Configuration
	config        *BufferPoolConfig // 配置信息
	totalPages    uint32            // 总页面数
	pageSize      uint32            // 页面大小
	checkpointLSN uint64            // 检查点LSN

	// Statistics
	dirtyPages uint32 // 脏页数量
	hitCount   uint64 // 缓存命中次数
	missCount  uint64 // 缓存未命中次数
	readCount  uint64 // 读取次数
	writeCount uint64 // 写入次数

	// Cache management
	lruCache     LRUCache      // LRU缓存
	freePages    []*BufferPage // 空闲页面列表
	freePageLock sync.Mutex    // 空闲页面锁

	// Storage manager
	storageManager basic.SpaceManager // 存储管理器

	// Buffer pool configuration
	bufferPoolSize uint64 // 缓冲池大小(字节)

	// Flush list for dirty pages
	flushList *list.List   // 脏页列表
	flushLock sync.RWMutex // 脏页列表锁

	// Auto-tuning parameters
	flushThreshold float64 // 脏页刷新阈值
	oldBlockTime   int     // old块存活时间

	// Prefetch manager
	prefetchManager *PrefetchManager // 预读管理器

	flushBlockList *FlushBlockList // 脏页列表

	FreeBlockList *FreeBlockList
}

// NewBufferPool creates a new buffer pool
func NewBufferPool(config *BufferPoolConfig) *BufferPool {
	bp := &BufferPool{
		totalPages:     config.TotalPages,
		pageSize:       config.PageSize,
		bufferPoolSize: config.BufferPoolSize,
		storageManager: config.StorageManager,
		lruCache: NewLRUCacheImpl(
			int(config.BufferPoolSize/uint64(config.PageSize)),
			config.YoungListPercent,
			config.OldListPercent,
			config.OldBlocksTime,
		),
		freePages: make([]*BufferPage, config.TotalPages),
		flushList: list.New(),
	}

	// Initialize free pages
	for i := uint32(0); i < config.TotalPages; i++ {
		bp.freePages[i] = NewBufferPage(0, 0)
	}

	// Initialize prefetch manager
	bp.config = config
	//bp.prefetchManager = NewPrefetchManager(&PrefetchConfig{
	//	BufferPool:   bp,
	//	PrefetchSize: config.PrefetchSize,
	//	MaxQueueSize: config.MaxQueueSize,
	//	Workers:      config.PrefetchWorkers,
	//})

	return bp
}

// BufferPoolConfig contains configuration for buffer pool
type BufferPoolConfig struct {
	// Basic configuration
	TotalPages     uint32
	PageSize       uint32
	BufferPoolSize uint64

	// LRU configuration
	YoungListPercent float64
	OldListPercent   float64
	OldBlocksTime    int

	// Prefetch configuration
	PrefetchSize    uint32
	MaxQueueSize    uint32
	PrefetchWorkers uint32

	// Storage manager
	StorageManager  basic.SpaceManager
	StorageProvider basic.StorageProvider
}

// Statistics methods

// GetHitRatio returns the cache hit ratio
func (bp *BufferPool) GetHitRatio() float64 {
	total := atomic.LoadUint64(&bp.hitCount) + atomic.LoadUint64(&bp.missCount)
	if total == 0 {
		return 0
	}
	return float64(atomic.LoadUint64(&bp.hitCount)) / float64(total)
}

// GetDirtyPageRatio returns the ratio of dirty pages
func (bp *BufferPool) GetDirtyPageRatio() float64 {
	return float64(atomic.LoadUint32(&bp.dirtyPages)) / float64(bp.totalPages)
}

// GetReadWriteRatio returns the ratio of reads to writes
func (bp *BufferPool) GetReadWriteRatio() float64 {
	writes := atomic.LoadUint64(&bp.writeCount)
	if writes == 0 {
		return 0
	}
	return float64(atomic.LoadUint64(&bp.readCount)) / float64(writes)
}

// GetCheckpointLSN returns the current checkpoint LSN
func (bp *BufferPool) GetCheckpointLSN() uint64 {
	return atomic.LoadUint64(&bp.checkpointLSN)
}

// UpdateCheckpointLSN updates the checkpoint LSN
func (bp *BufferPool) UpdateCheckpointLSN(lsn uint64) {
	atomic.StoreUint64(&bp.checkpointLSN, lsn)
}

// UpdateDirtyPageCount updates the dirty page count
func (bp *BufferPool) UpdateDirtyPageCount(delta int32) {
	atomic.AddUint32(&bp.dirtyPages, uint32(delta))
}

// RecordPageHit records a page cache hit
func (bp *BufferPool) RecordPageHit() {
	atomic.AddUint64(&bp.hitCount, 1)
	atomic.AddUint64(&bp.readCount, 1)
}

// RecordPageMiss records a page cache miss
func (bp *BufferPool) RecordPageMiss() {
	atomic.AddUint64(&bp.missCount, 1)
	atomic.AddUint64(&bp.readCount, 1)
}

// RecordPageWrite records a page write
func (bp *BufferPool) RecordPageWrite() {
	atomic.AddUint64(&bp.writeCount, 1)
}

// readFromDisk reads a page from disk
func (bp *BufferPool) readFromDisk(space basic.Space, pageNo uint32) (*BufferPage, error) {
	// Get a free page from pool
	page := bp.getFreePage()

	// Read page content from disk
	content, err := space.LoadPageByPageNumber(pageNo)
	if err != nil {
		return nil, err
	}

	// Initialize buffer page
	page.Init(space.ID(), pageNo, content)
	return page, nil
}

// getFreePage gets a free page from the pool
func (bp *BufferPool) getFreePage() *BufferPage {
	bp.freePageLock.Lock()
	defer bp.freePageLock.Unlock()

	// Try to get a free page
	for i := range bp.freePages {
		if bp.freePages[i].IsFree() {
			return bp.freePages[i]
		}
	}

	// No free pages, evict one from LRU cache
	return bp.evictPage()
}

// evictPage evicts a page from LRU cache
func (bp *BufferPool) evictPage() *BufferPage {
	// Get victim from LRU cache
	victim := bp.lruCache.Evict()
	if victim == nil {
		// Should never happen as we always have pages in LRU
		panic("no pages to evict from LRU cache")
	}

	// If dirty, write back to disk
	if victim.IsDirty() {
		if err := bp.writeToDisk(victim); err != nil {
			// Log error but continue, as we need to evict the page
			logger.Debugf("failed to write dirty page to disk: %v\n", err)
		}
	}

	// Reset page state
	victim.Reset()

	return victim
}

// writeToDisk writes a page back to disk
func (bp *BufferPool) writeToDisk(page *BufferPage) error {
	logger.Debugf("buffer pool affected writing page: %v\n", page)
	// Get space from storage manager
	space, err := bp.storageManager.GetSpace(page.GetSpaceID())
	if err != nil {
		return fmt.Errorf("failed to get space %d: %v", page.GetSpaceID(), err)
	}

	// Write page content to disk
	if err := space.FlushToDisk(page.GetPageNo(), page.GetContent()); err != nil {
		return err
	}

	// Update statistics
	bp.RecordPageWrite()

	return nil
}

// GetPage gets a page from buffer pool
func (bp *BufferPool) GetPage(spaceID uint32, pageNo uint32) (*BufferPage, error) {

	// Try to get from cache first
	block, err := bp.lruCache.Get(spaceID, pageNo)
	if err == nil && block != nil {
		bp.RecordPageHit()
		return block.BufferPage, nil
	}

	// Cache miss, need to read from disk
	bp.RecordPageMiss()

	// Get space from storage manager
	space, err := bp.storageManager.GetSpace(spaceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get space %d: %v", spaceID, err)
	}

	// Read page from disk
	page, err := bp.readFromDisk(space, pageNo)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d from disk: %v", pageNo, err)
	}

	// Record IO latency
	bp.RecordPageWrite()

	return page, nil
}

// PutPage puts a page into the buffer pool
func (bp *BufferPool) PutPage(page *BufferPage) error {
	// Create new buffer block with the page
	block := NewBufferBlock(page)

	// Add to LRU cache
	err := bp.lruCache.Set(page.GetSpaceID(), page.GetPageNo(), block)
	if err != nil {
		return fmt.Errorf("failed to add page to cache: %v", err)
	}

	// Update dirty page count if needed
	if page.IsDirty() {
		bp.UpdateDirtyPageCount(1)
	}

	return nil
}

// GetPageBlock gets a page block from the buffer pool
func (bp *BufferPool) GetPageBlock(spaceID uint32, pageNo uint32) (*BufferBlock, error) {
	// Try to get from cache first
	block, err := bp.lruCache.Get(spaceID, pageNo)
	if err == nil && block != nil {
		bp.RecordPageHit()
		return block, nil
	}

	// Cache miss, need to read from disk
	bp.RecordPageMiss()

	// Get a free block from the free list
	block = bp.FreeBlockList.GetPage(spaceID, pageNo)
	if block == nil {
		return nil, fmt.Errorf("failed to get free block")
	}

	// Add to LRU cache
	err = bp.lruCache.Set(spaceID, pageNo, block)
	if err != nil {
		return nil, fmt.Errorf("failed to add block to cache: %v", err)
	}

	return block, nil
}

// RangePageLoad loads a range of pages into the buffer pool
func (bp *BufferPool) RangePageLoad(spaceID uint32, startPageNo, endPageNo uint32) error {
	if startPageNo >= endPageNo {
		return fmt.Errorf("invalid page range: start %d >= end %d", startPageNo, endPageNo)
	}

	// Load pages in the specified range
	for pageNo := startPageNo; pageNo < endPageNo; pageNo++ {
		_, err := bp.GetPageBlock(spaceID, pageNo)
		if err != nil {
			return fmt.Errorf("failed to load page %d: %v", pageNo, err)
		}
	}

	// Trigger prefetch for subsequent pages
	bp.prefetchManager.TriggerPrefetch(spaceID, endPageNo)

	return nil
}

// GetDirtyPages returns all dirty pages in the buffer pool
func (bp *BufferPool) GetDirtyPages() []*BufferPage {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	dirtyPages := make([]*BufferPage, 0)

	// 遍历 young list
	youngSize := uint32(float64(bp.totalPages) * bp.config.YoungListPercent)
	for i := uint32(0); i < youngSize; i++ {
		if block, err := bp.lruCache.GetYoung(0, i); err == nil && block != nil && block.BufferPage.IsDirty() {
			dirtyPages = append(dirtyPages, block.BufferPage)
		}
	}

	// 遍历 old list
	oldSize := uint32(float64(bp.totalPages) * bp.config.OldListPercent)
	for i := uint32(0); i < oldSize; i++ {
		if block, err := bp.lruCache.GetOld(0, i); err == nil && block != nil && block.BufferPage.IsDirty() {
			dirtyPages = append(dirtyPages, block.BufferPage)
		}
	}

	return dirtyPages
}

// FlushPage flushes a specific page to disk
func (bp *BufferPool) FlushPage(page *BufferPage) error {
	if page == nil {
		return fmt.Errorf("cannot flush nil page")
	}

	// Get page data
	data := page.GetData()
	if data == nil {
		return fmt.Errorf("page data is nil")
	}

	// Write to storage
	if err := bp.config.StorageProvider.WritePage(page.GetSpaceID(), page.GetPageNo(), data); err != nil {
		return fmt.Errorf("failed to write page to storage: %v", err)
	}

	// Clear dirty flag
	page.SetDirty(false)

	// Update statistics
	bp.RecordPageWrite()

	return nil
}

// GetDirtyPageBlock gets a page block and marks it as dirty
func (bp *BufferPool) GetDirtyPageBlock(spaceID uint32, pageNo uint32) (*BufferBlock, error) {
	// Get the page block
	block, err := bp.GetPageBlock(spaceID, pageNo)
	if err != nil {
		return nil, err
	}

	// Mark as dirty
	block.MarkDirty()

	// Add to flush list
	bp.flushBlockList.AddBlock(block)

	return block, nil
}

// GetFlushDiskList returns the list of blocks to be flushed to disk
func (bp *BufferPool) GetFlushDiskList() *FlushBlockList {
	return bp.flushBlockList
}

// GetFreeBlockList returns the list of free blocks
func (bp *BufferPool) GetFreeBlockList() *FreeBlockList {
	return bp.FreeBlockList
}

// FlushDirtyPages flushes all dirty pages to disk
func (bp *BufferPool) FlushDirtyPages() error {
	// Get all blocks from flush list
	flushList := bp.GetFlushDiskList()
	if flushList.IsEmpty() {
		return nil
	}

	// Flush each block
	for !flushList.IsEmpty() {
		block := flushList.GetLastBlock()
		if block == nil {
			continue
		}

		// Write page to disk
		if err := bp.writeToDisk(block.BufferPage); err != nil {
			return fmt.Errorf("failed to flush page %d: %v", block.GetPageNo(), err)
		}

		// Update dirty page count
		bp.UpdateDirtyPageCount(-1)
	}

	return nil
}

// UpdateBlock updates a block in the buffer pool
func (bp *BufferPool) UpdateBlock(spaceID uint32, pageNo uint32, block *BufferBlock) error {
	// Remove from LRU cache
	if bp.lruCache.Has(spaceID, pageNo) {
		bp.lruCache.Remove(spaceID, pageNo)
	}

	// Add to LRU cache
	err := bp.lruCache.Set(spaceID, pageNo, block)
	if err != nil {
		return fmt.Errorf("failed to update block in cache: %v", err)
	}

	// If dirty, add to flush list
	if block.IsDirty() {
		bp.flushBlockList.AddBlock(block)
		bp.UpdateDirtyPageCount(1)
	}

	return nil
}

type FreeBlockList struct {
	storageManager basic.SpaceManager
	list           *list.List
	mu             sync.RWMutex
	freePageItems  map[uint64]*list.Element
}

// NewFreeBlockList creates a new free block list
func NewFreeBlockList(storageManager basic.SpaceManager) *FreeBlockList {
	return &FreeBlockList{
		storageManager: storageManager,
		list:           list.New(),
		freePageItems:  make(map[uint64]*list.Element),
	}
}

// GetPage gets a page from the free list
func (fbl *FreeBlockList) GetPage(spaceID uint32, pageNo uint32) *BufferBlock {
	fbl.mu.Lock()
	defer fbl.mu.Unlock()

	// Try to get from free list
	key := uint64(spaceID)<<32 | uint64(pageNo)
	if elem, ok := fbl.freePageItems[key]; ok {
		block := elem.Value.(*BufferBlock)
		fbl.list.Remove(elem)
		delete(fbl.freePageItems, key)
		return block
	}

	// Create new block
	page := NewBufferPage(spaceID, pageNo)
	block := NewBufferBlock(page)

	// Get space from storage manager
	space, err := fbl.storageManager.GetSpace(spaceID)
	if err != nil {
		logger.Debugf("failed to get space %d: %v\n", spaceID, err)
		return nil
	}

	// Load page content through space
	content, err := space.LoadPageByPageNumber(pageNo)
	if err != nil {
		logger.Debugf("failed to read page %d: %v\n", pageNo, err)
		return nil
	}

	// Initialize page
	page.Init(spaceID, pageNo, content)

	// Create buffer block
	block = NewBufferBlock(page)

	// Add to free list
	elem := fbl.list.PushBack(block)
	fbl.freePageItems[key] = elem

	return nil
}

// RemoveBlock removes a block from the free list
func (fbl *FreeBlockList) RemoveBlock(spaceID uint32, pageNo uint32) *BufferBlock {
	fbl.mu.Lock()
	defer fbl.mu.Unlock()

	// Generate key for the page
	key := uint64(spaceID)<<32 | uint64(pageNo)

	// Try to get from free list
	if elem, ok := fbl.freePageItems[key]; ok {
		block := elem.Value.(*BufferBlock)
		fbl.list.Remove(elem)
		delete(fbl.freePageItems, key)
		return block
	}

	return nil
}

// IsEmpty checks if the free list is empty
func (fbl *FreeBlockList) IsEmpty() bool {
	fbl.mu.RLock()
	defer fbl.mu.RUnlock()
	return fbl.list.Len() == 0
}

// Size returns the number of blocks in the free list
func (fbl *FreeBlockList) Size() int {
	fbl.mu.RLock()
	defer fbl.mu.RUnlock()
	return fbl.list.Len()
}

// Clear removes all blocks from the free list
func (fbl *FreeBlockList) Clear() {
	fbl.mu.Lock()
	defer fbl.mu.Unlock()

	fbl.list.Init()
	fbl.freePageItems = make(map[uint64]*list.Element)
}

// 脏页
type FlushBlockList struct {
	list list.List

	mu sync.RWMutex
}

func NewFlushBlockList() *FlushBlockList {
	var flushBlockList = new(FlushBlockList)
	return flushBlockList
}

func (flb *FlushBlockList) AddBlock(block *BufferBlock) {
	flb.mu.Lock()
	defer flb.mu.Unlock()
	flb.list.PushFront(block)
}

func (flb *FlushBlockList) IsEmpty() bool {
	return flb.list.Len() == 0
}

func (flb *FlushBlockList) GetLastBlock() *BufferBlock {
	flb.mu.Lock()
	defer flb.mu.Unlock()
	if flb.IsEmpty() {
		return nil
	}
	lastElement := flb.list.Back()
	flb.list.Remove(lastElement)
	return lastElement.Value.(*BufferBlock)
}
