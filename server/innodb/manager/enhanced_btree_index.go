package manager

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// EnhancedBTreeIndex 增强版B+树索引实例
type EnhancedBTreeIndex struct {
	// 基本信息
	metadata       *IndexMetadata  // 索引元信息
	storageManager *StorageManager // 存储管理器
	config         *BTreeConfig    // 配置

	// 页面缓存
	mu            sync.RWMutex          // 读写锁
	pageCache     map[uint32]*BTreePage // 页面缓存
	pageLoadOrder []uint32              // 页面访问顺序（LRU）

	// 统计信息
	statistics *EnhancedIndexStatistics // 索引统计

	// 引用计数
	refCount atomic.Int32 // 引用计数

	// 状态管理
	isLoaded  atomic.Bool // 是否已加载
	lastFlush time.Time   // 最后刷新时间
}

// NewEnhancedBTreeIndex 创建增强版B+树索引实例
func NewEnhancedBTreeIndex(metadata *IndexMetadata, storageManager *StorageManager, config *BTreeConfig) *EnhancedBTreeIndex {
	index := &EnhancedBTreeIndex{
		metadata:       metadata,
		storageManager: storageManager,
		config:         config,
		pageCache:      make(map[uint32]*BTreePage),
		pageLoadOrder:  make([]uint32, 0),
		statistics: &EnhancedIndexStatistics{
			Cardinality:  0,
			NullCount:    0,
			AvgKeyLength: 0,
			LeafPages:    0,
			NonLeafPages: 0,
			SplitCount:   0,
			MergeCount:   0,
			LastAnalyze:  time.Now(),
		},
		lastFlush: time.Now(),
	}

	return index
}

// 实现 BTreeIndex 接口

// GetIndexID 获取索引ID
func (idx *EnhancedBTreeIndex) GetIndexID() uint64 {
	return idx.metadata.IndexID
}

// GetTableID 获取表ID
func (idx *EnhancedBTreeIndex) GetTableID() uint64 {
	return idx.metadata.TableID
}

// GetSpaceID 获取表空间ID
func (idx *EnhancedBTreeIndex) GetSpaceID() uint32 {
	return idx.metadata.SpaceID
}

// GetRootPageNo 获取根页号
func (idx *EnhancedBTreeIndex) GetRootPageNo() uint32 {
	return idx.metadata.RootPageNo
}

// GetMetadata 获取元信息
func (idx *EnhancedBTreeIndex) GetMetadata() *IndexMetadata {
	return idx.metadata
}

// Insert 插入记录
func (idx *EnhancedBTreeIndex) Insert(ctx context.Context, key []byte, value []byte) error {
	if !idx.isLoaded.Load() {
		return fmt.Errorf("index %d is not loaded", idx.metadata.IndexID)
	}

	// 增加引用计数
	idx.AddRef()
	defer idx.Release()

	// 获取根页面
	rootPage, err := idx.GetPage(ctx, idx.metadata.RootPageNo)
	if err != nil {
		return fmt.Errorf("failed to get root page: %v", err)
	}

	// 执行插入操作
	err = idx.insertIntoPage(ctx, rootPage, key, value)
	if err != nil {
		return fmt.Errorf("failed to insert into page: %v", err)
	}

	// 更新统计信息
	idx.metadata.RecordCount++
	idx.metadata.UpdateTime = time.Now()

	return nil
}

// Delete 删除记录
func (idx *EnhancedBTreeIndex) Delete(ctx context.Context, key []byte) error {
	if !idx.isLoaded.Load() {
		return fmt.Errorf("index %d is not loaded", idx.metadata.IndexID)
	}

	// 增加引用计数
	idx.AddRef()
	defer idx.Release()

	// 查找记录
	record, err := idx.Search(ctx, key)
	if err != nil {
		return fmt.Errorf("record not found: %v", err)
	}

	// 获取页面
	page, err := idx.GetPage(ctx, record.PageNo)
	if err != nil {
		return fmt.Errorf("failed to get page: %v", err)
	}

	// 执行删除操作
	err = idx.deleteFromPage(ctx, page, key)
	if err != nil {
		return fmt.Errorf("failed to delete from page: %v", err)
	}

	// 更新统计信息
	if idx.metadata.RecordCount > 0 {
		idx.metadata.RecordCount--
	}
	idx.metadata.UpdateTime = time.Now()

	return nil
}

// Search 搜索记录
func (idx *EnhancedBTreeIndex) Search(ctx context.Context, key []byte) (*IndexRecord, error) {
	if !idx.isLoaded.Load() {
		return nil, fmt.Errorf("index %d is not loaded", idx.metadata.IndexID)
	}

	// 增加引用计数
	idx.AddRef()
	defer idx.Release()

	// 从根页面开始搜索
	currentPageNo := idx.metadata.RootPageNo

	for {
		page, err := idx.GetPage(ctx, currentPageNo)
		if err != nil {
			return nil, fmt.Errorf("failed to get page %d: %v", currentPageNo, err)
		}

		// 在页面中搜索
		record, nextPageNo, err := idx.searchInPage(page, key)
		if err != nil {
			return nil, err
		}

		// 如果找到记录，返回
		if record != nil {
			return record, nil
		}

		// 如果是叶子页面但没找到，说明记录不存在
		if page.PageType == BTreePageTypeLeaf {
			return nil, fmt.Errorf("record not found")
		}

		// 继续在子页面中搜索
		if nextPageNo == 0 {
			return nil, fmt.Errorf("invalid next page number")
		}
		currentPageNo = nextPageNo
	}
}

// RangeSearch 范围搜索
func (idx *EnhancedBTreeIndex) RangeSearch(ctx context.Context, startKey, endKey []byte) ([]IndexRecord, error) {
	if !idx.isLoaded.Load() {
		return nil, fmt.Errorf("index %d is not loaded", idx.metadata.IndexID)
	}

	// 增加引用计数
	idx.AddRef()
	defer idx.Release()

	var results []IndexRecord

	// 找到第一个叶子页面
	firstLeafPageNo, err := idx.GetFirstLeafPage(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get first leaf page: %v", err)
	}

	// 遍历叶子页面链表
	currentPageNo := firstLeafPageNo
	for currentPageNo != 0 {
		page, err := idx.GetPage(ctx, currentPageNo)
		if err != nil {
			return nil, fmt.Errorf("failed to get page %d: %v", currentPageNo, err)
		}

		// 在页面中搜索范围
		pageResults := idx.rangeSearchInPage(page, startKey, endKey)
		results = append(results, pageResults...)

		// 如果找到的记录键值已经超过结束键，停止搜索
		if len(pageResults) > 0 {
			lastKey := pageResults[len(pageResults)-1].Key
			if idx.compareKeys(lastKey, endKey) > 0 {
				break
			}
		}

		// 移动到下一个叶子页面
		currentPageNo = page.NextPage
	}

	return results, nil
}

// GetFirstLeafPage 获取第一个叶子页面
func (idx *EnhancedBTreeIndex) GetFirstLeafPage(ctx context.Context) (uint32, error) {
	currentPageNo := idx.metadata.RootPageNo

	for {
		page, err := idx.GetPage(ctx, currentPageNo)
		if err != nil {
			return 0, fmt.Errorf("failed to get page %d: %v", currentPageNo, err)
		}

		// 如果是叶子页面，返回页号
		if page.PageType == BTreePageTypeLeaf {
			return currentPageNo, nil
		}

		// 继续向左子页面搜索
		if len(page.Records) == 0 {
			return 0, fmt.Errorf("empty non-leaf page %d", currentPageNo)
		}

		// 获取第一个子页面（简化实现）
		// 实际应该从page内容中解析子页面号
		currentPageNo = idx.getFirstChildPageNo(page)
		if currentPageNo == 0 {
			return 0, fmt.Errorf("invalid child page number")
		}
	}
}

// GetAllLeafPages 获取所有叶子页面
func (idx *EnhancedBTreeIndex) GetAllLeafPages(ctx context.Context) ([]uint32, error) {
	var leafPages []uint32

	// 获取第一个叶子页面
	firstLeafPageNo, err := idx.GetFirstLeafPage(ctx)
	if err != nil {
		return nil, err
	}

	// 遍历叶子页面链表
	currentPageNo := firstLeafPageNo
	for currentPageNo != 0 {
		leafPages = append(leafPages, currentPageNo)

		page, err := idx.GetPage(ctx, currentPageNo)
		if err != nil {
			return nil, fmt.Errorf("failed to get page %d: %v", currentPageNo, err)
		}

		currentPageNo = page.NextPage
	}

	return leafPages, nil
}

// Iterator 创建索引迭代器
func (idx *EnhancedBTreeIndex) Iterator(ctx context.Context) (IndexIterator, error) {
	return NewIndexIterator(idx, ctx)
}

// GetPage 获取页面
func (idx *EnhancedBTreeIndex) GetPage(ctx context.Context, pageNo uint32) (*BTreePage, error) {
	// 先检查缓存
	idx.mu.RLock()
	page, exists := idx.pageCache[pageNo]
	if exists {
		page.LastAccess = time.Now()
		page.PinCount++
		idx.mu.RUnlock()
		return page, nil
	}
	idx.mu.RUnlock()

	// 从存储加载页面
	bufferPage, err := idx.storageManager.GetBufferPoolManager().GetPage(idx.metadata.SpaceID, pageNo)
	if err != nil {
		return nil, fmt.Errorf("failed to get page from buffer pool: %v", err)
	}

	// 解析页面内容
	page, err = idx.parsePageContent(bufferPage)
	if err != nil {
		return nil, fmt.Errorf("failed to parse page content: %v", err)
	}

	// 加入缓存
	idx.mu.Lock()
	// 再次检查缓存（双重检查锁定）
	if cachedPage, exists := idx.pageCache[pageNo]; exists {
		idx.mu.Unlock()
		return cachedPage, nil
	}

	// 检查缓存大小限制
	if err := idx.enforcePageCacheLimits(); err != nil {
		idx.mu.Unlock()
		return nil, err
	}

	idx.pageCache[pageNo] = page
	idx.pageLoadOrder = append(idx.pageLoadOrder, pageNo)
	idx.mu.Unlock()

	return page, nil
}

// AllocatePage 分配新页面
func (idx *EnhancedBTreeIndex) AllocatePage(ctx context.Context) (uint32, error) {
	// 简化实现：使用时间戳生成页号
	// 实际应该从存储管理器分配页面
	newPageNo := uint32(time.Now().UnixNano())%1000000 + 10000

	// TODO: 真正的页面分配逻辑
	return newPageNo, nil
}

// DeallocatePage 释放页面
func (idx *EnhancedBTreeIndex) DeallocatePage(ctx context.Context, pageNo uint32) error {
	// 从缓存中移除
	idx.mu.Lock()
	delete(idx.pageCache, pageNo)
	for i, no := range idx.pageLoadOrder {
		if no == pageNo {
			idx.pageLoadOrder = append(idx.pageLoadOrder[:i], idx.pageLoadOrder[i+1:]...)
			break
		}
	}
	idx.mu.Unlock()

	// TODO: 真正的页面释放逻辑
	return nil
}

// GetStatistics 获取统计信息
func (idx *EnhancedBTreeIndex) GetStatistics() *EnhancedIndexStatistics {
	return idx.statistics
}

// UpdateStatistics 更新统计信息
func (idx *EnhancedBTreeIndex) UpdateStatistics(ctx context.Context) error {
	// 更新基本统计
	idx.statistics.Cardinality = idx.metadata.RecordCount
	idx.statistics.LeafPages = 0
	idx.statistics.NonLeafPages = 0

	// 遍历所有页面统计信息（简化实现）
	leafPages, err := idx.GetAllLeafPages(ctx)
	if err != nil {
		return err
	}

	idx.statistics.LeafPages = uint32(len(leafPages))
	idx.statistics.LastAnalyze = time.Now()

	return nil
}

// CheckConsistency 检查一致性
func (idx *EnhancedBTreeIndex) CheckConsistency(ctx context.Context) error {
	// TODO: 实现一致性检查逻辑
	return nil
}

// Flush 刷新索引
func (idx *EnhancedBTreeIndex) Flush(ctx context.Context) error {
	idx.mu.RLock()
	var dirtyPages []*BTreePage
	for _, page := range idx.pageCache {
		if page.IsDirty {
			dirtyPages = append(dirtyPages, page)
		}
	}
	idx.mu.RUnlock()

	// 刷新所有脏页
	for _, page := range dirtyPages {
		if err := idx.flushPage(ctx, page); err != nil {
			return fmt.Errorf("failed to flush page %d: %v", page.PageNo, err)
		}
	}

	idx.lastFlush = time.Now()
	return nil
}

// IsLoaded 检查是否已加载
func (idx *EnhancedBTreeIndex) IsLoaded() bool {
	return idx.isLoaded.Load()
}

// GetRefCount 获取引用计数
func (idx *EnhancedBTreeIndex) GetRefCount() int32 {
	return idx.refCount.Load()
}

// AddRef 增加引用计数
func (idx *EnhancedBTreeIndex) AddRef() int32 {
	return idx.refCount.Add(1)
}

// Release 释放引用
func (idx *EnhancedBTreeIndex) Release() int32 {
	return idx.refCount.Add(-1)
}

// 索引生命周期方法

// InitializeEmptyIndex 初始化空索引
func (idx *EnhancedBTreeIndex) InitializeEmptyIndex(ctx context.Context) error {
	// 创建根页面
	rootPage := &BTreePage{
		PageNo:      idx.metadata.RootPageNo,
		PageType:    BTreePageTypeLeaf, // 初始时根页面也是叶子页面
		Level:       0,
		RecordCount: 0,
		FreeSpace:   uint16(idx.config.PageSize - 100), // 保留100字节用于页面头
		NextPage:    0,
		PrevPage:    0,
		Records:     make([]IndexRecord, 0),
		IsLoaded:    true,
		IsDirty:     true,
		LastAccess:  time.Now(),
		PinCount:    0,
	}

	// 保存根页面
	if err := idx.flushPage(ctx, rootPage); err != nil {
		return fmt.Errorf("failed to save root page: %v", err)
	}

	// 加入缓存
	idx.mu.Lock()
	idx.pageCache[rootPage.PageNo] = rootPage
	idx.pageLoadOrder = append(idx.pageLoadOrder, rootPage.PageNo)
	idx.mu.Unlock()

	// 更新元信息
	idx.metadata.Height = 1
	idx.metadata.PageCount = 1
	idx.metadata.RecordCount = 0

	idx.isLoaded.Store(true)
	return nil
}

// LoadFromStorage 从存储加载索引
func (idx *EnhancedBTreeIndex) LoadFromStorage(ctx context.Context) error {
	// 加载根页面来验证索引是否存在
	_, err := idx.GetPage(ctx, idx.metadata.RootPageNo)
	if err != nil {
		return fmt.Errorf("failed to load root page: %v", err)
	}

	idx.isLoaded.Store(true)
	return nil
}

// 内部方法

// insertIntoPage 向页面插入记录
func (idx *EnhancedBTreeIndex) insertIntoPage(ctx context.Context, page *BTreePage, key []byte, value []byte) error {
	// 简化实现：直接插入到页面
	record := IndexRecord{
		Key:        make([]byte, len(key)),
		Value:      make([]byte, len(value)),
		PageNo:     page.PageNo,
		SlotNo:     page.RecordCount,
		TxnID:      0, // TODO: 获取事务ID
		DeleteMark: false,
	}

	copy(record.Key, key)
	copy(record.Value, value)

	page.Records = append(page.Records, record)
	page.RecordCount++
	page.IsDirty = true
	page.LastAccess = time.Now()

	return nil
}

// deleteFromPage 从页面删除记录
func (idx *EnhancedBTreeIndex) deleteFromPage(ctx context.Context, page *BTreePage, key []byte) error {
	// 查找要删除的记录
	for i, record := range page.Records {
		if idx.compareKeys(record.Key, key) == 0 {
			// 标记删除（简化实现）
			page.Records[i].DeleteMark = true
			page.IsDirty = true
			page.LastAccess = time.Now()
			return nil
		}
	}

	return fmt.Errorf("record not found in page")
}

// searchInPage 在页面中搜索
func (idx *EnhancedBTreeIndex) searchInPage(page *BTreePage, key []byte) (*IndexRecord, uint32, error) {
	// 在叶子页面中搜索记录
	if page.PageType == BTreePageTypeLeaf {
		for _, record := range page.Records {
			if !record.DeleteMark && idx.compareKeys(record.Key, key) == 0 {
				return &record, 0, nil
			}
		}
		return nil, 0, nil
	}

	// 在内部页面中搜索子页面
	// 简化实现：返回第一个子页面
	if len(page.Records) > 0 {
		// TODO: 实现正确的子页面查找逻辑
		return nil, idx.getFirstChildPageNo(page), nil
	}

	return nil, 0, fmt.Errorf("empty internal page")
}

// rangeSearchInPage 在页面中进行范围搜索
func (idx *EnhancedBTreeIndex) rangeSearchInPage(page *BTreePage, startKey, endKey []byte) []IndexRecord {
	var results []IndexRecord

	for _, record := range page.Records {
		if record.DeleteMark {
			continue
		}

		keyCompareStart := idx.compareKeys(record.Key, startKey)
		keyCompareEnd := idx.compareKeys(record.Key, endKey)

		if keyCompareStart >= 0 && keyCompareEnd <= 0 {
			results = append(results, record)
		}
	}

	return results
}

// parsePageContent 解析页面内容
func (idx *EnhancedBTreeIndex) parsePageContent(bufferPage interface{}) (*BTreePage, error) {
	// 简化实现：创建一个基本的页面结构
	page := &BTreePage{
		PageNo:      1,                 // bufferPage.GetPageNo(), // 简化实现
		PageType:    BTreePageTypeLeaf, // 简化实现
		Level:       0,
		RecordCount: 0,
		FreeSpace:   uint16(idx.config.PageSize),
		NextPage:    0,
		PrevPage:    0,
		Records:     make([]IndexRecord, 0),
		IsLoaded:    true,
		IsDirty:     false,
		LastAccess:  time.Now(),
		PinCount:    1,
	}

	// TODO: 实现真正的页面内容解析

	return page, nil
}

// flushPage 刷新页面到存储
func (idx *EnhancedBTreeIndex) flushPage(ctx context.Context, page *BTreePage) error {
	// 获取缓冲池页面
	bufferPage, err := idx.storageManager.GetBufferPoolManager().GetPage(idx.metadata.SpaceID, page.PageNo)
	if err != nil {
		return err
	}

	// TODO: 将page内容序列化到bufferPage

	// 标记为脏页并刷新
	bufferPage.MarkDirty()
	err = idx.storageManager.GetBufferPoolManager().FlushPage(idx.metadata.SpaceID, page.PageNo)
	if err != nil {
		return err
	}

	page.IsDirty = false
	return nil
}

// enforcePageCacheLimits 强制执行页面缓存限制
func (idx *EnhancedBTreeIndex) enforcePageCacheLimits() error {
	maxPages := int(idx.config.MaxCacheSize / 100) // 简化计算
	if maxPages < 10 {
		maxPages = 10
	}

	if len(idx.pageCache) >= maxPages {
		// 移除最久未访问的页面（LRU）
		oldestPageNo := idx.pageLoadOrder[0]

		// 如果页面是脏的，先刷新
		if page, exists := idx.pageCache[oldestPageNo]; exists && page.IsDirty {
			if err := idx.flushPage(context.Background(), page); err != nil {
				return err
			}
		}

		// 从缓存中移除
		delete(idx.pageCache, oldestPageNo)
		idx.pageLoadOrder = idx.pageLoadOrder[1:]
	}

	return nil
}

// compareKeys 比较键值
func (idx *EnhancedBTreeIndex) compareKeys(a, b []byte) int {
	// 简化实现：字节比较
	if len(a) < len(b) {
		return -1
	} else if len(a) > len(b) {
		return 1
	}

	for i := 0; i < len(a); i++ {
		if a[i] < b[i] {
			return -1
		} else if a[i] > b[i] {
			return 1
		}
	}

	return 0
}

// getFirstChildPageNo 获取第一个子页面号
func (idx *EnhancedBTreeIndex) getFirstChildPageNo(page *BTreePage) uint32 {
	// 简化实现：返回固定值
	// 实际应该从页面内容中解析
	if len(page.Records) > 0 {
		return page.PageNo + 1 // 简化逻辑
	}
	return 0
}
