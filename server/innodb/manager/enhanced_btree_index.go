package manager

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/logger"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/page"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/record"
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
	// 使用存储管理器通过缓冲池管理器分配页面
	bufferPage, err := idx.storageManager.GetBufferPoolManager().AllocatePage(idx.metadata.SpaceID)
	if err != nil {
		return 0, err
	}

	return bufferPage.GetPageNo(), nil
}

// DeallocatePage 释放页面
func (idx *EnhancedBTreeIndex) DeallocatePage(ctx context.Context, pageNo uint32) error {
	// 先通过存储管理器释放页面
	if err := idx.storageManager.GetBufferPoolManager().FreePage(idx.metadata.SpaceID, pageNo); err != nil {
		return err
	}

	// 再从缓存中移除
	idx.mu.Lock()
	delete(idx.pageCache, pageNo)
	for i, no := range idx.pageLoadOrder {
		if no == pageNo {
			idx.pageLoadOrder = append(idx.pageLoadOrder[:i], idx.pageLoadOrder[i+1:]...)
			break
		}
	}
	idx.mu.Unlock()

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
	leafPages, err := idx.GetAllLeafPages(ctx)
	if err != nil {
		return err
	}

	for _, pageNo := range leafPages {
		page, err := idx.GetPage(ctx, pageNo)
		if err != nil {
			return err
		}

		bufferPage, err := idx.storageManager.GetBufferPoolManager().GetPage(idx.metadata.SpaceID, pageNo)
		if err != nil {
			return err
		}

		if idx.getRecordCountFromPage(bufferPage.GetContent()) != page.RecordCount {
			return fmt.Errorf("page %d record count mismatch", pageNo)
		}
	}

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

// insertIntoPage 向页面插入记录 (使用简化的页面初始化)
func (idx *EnhancedBTreeIndex) insertIntoPage(ctx context.Context, page *BTreePage, key []byte, value []byte) error {
	logger.Debugf(" Inserting record into page %d (key: %s, value size: %d bytes)\n", page.PageNo, string(key), len(value))

	// 1. 确保页面有有效内容
	bufferPage, err := idx.EnsurePageHasValidContent(ctx, idx.metadata.SpaceID, page.PageNo)
	if err != nil {
		return fmt.Errorf("failed to ensure page has valid content: %v", err)
	}

	// 2. 验证页面内容
	if err := idx.VerifyPageContent(idx.metadata.SpaceID, page.PageNo); err != nil {
		logger.Debugf("  Page verification warning: %v\n", err)
	}

	// 3. 创建标准的InnoDB记录
	record, err := idx.createInnoDBRecord(key, value)
	if err != nil {
		return fmt.Errorf("failed to create InnoDB record: %v", err)
	}

	logger.Debugf(" Created InnoDB record: %d bytes\n", len(record.ToByte()))

	// 4. 使用页面包装器插入记录
	err = idx.insertRecordToPage(bufferPage, record)
	if err != nil {
		return fmt.Errorf("failed to insert record to page: %v", err)
	}

	// 5. 更新页面状态
	bufferPage.MarkDirty()
	page.RecordCount++
	page.IsDirty = true
	page.LastAccess = time.Now()

	logger.Debugf(" Record inserted successfully, page now has %d records\n", page.RecordCount)

	// 6. 强制刷新到磁盘以确保持久化
	if err := idx.storageManager.GetBufferPoolManager().FlushPage(idx.metadata.SpaceID, page.PageNo); err != nil {
		logger.Debugf("  Warning: Failed to flush page to disk: %v\n", err)
	} else {
		logger.Debugf("💾 Page %d flushed to disk successfully\n", page.PageNo)
	}

	// 7. 更新内存中的记录信息（为了兼容现有逻辑）
	indexRecord := IndexRecord{
		Key:        make([]byte, len(key)),
		Value:      make([]byte, len(value)),
		PageNo:     page.PageNo,
		SlotNo:     page.RecordCount - 1, // 使用真实的记录数量
		TxnID:      0,                    // TODO: 获取事务ID
		DeleteMark: false,
	}
	copy(indexRecord.Key, key)
	copy(indexRecord.Value, value)
	page.Records = append(page.Records, indexRecord)

	// 8. 再次验证页面内容以确保插入成功
	content := bufferPage.GetContent()
	logger.Debugf(" Post-insertion page stats:\n")
	logger.Debugf("   - Page size: %d bytes\n", len(content))
	logger.Debugf("   - Non-zero bytes in first 200: %d\n", idx.countNonZeroBytes(content[:200]))
	logger.Debugf("   - Record count in page header: %d\n", idx.getRecordCountFromPage(content))

	return nil
}

// createInnoDBRecord 创建标准的InnoDB记录
func (idx *EnhancedBTreeIndex) createInnoDBRecord(key []byte, value []byte) (basic.Row, error) {
	// 创建表元组（TableRowTuple）
	tableTuple := idx.createTableRowTuple()
	if tableTuple == nil {
		return nil, fmt.Errorf("failed to create table row tuple")
	}

	// 准备记录数据：[头部信息] + [key] + [value]
	recordData := idx.serializeRecordData(key, value)

	// 创建聚簇索引叶子节点记录
	record := record.NewClusterLeafRow(recordData, tableTuple)

	return record, nil
}

// createTableRowTuple 创建表行元组
func (idx *EnhancedBTreeIndex) createTableRowTuple() metadata.RecordTableRowTuple {
	// 根据索引元信息创建表行元组
	tableMeta := metadata.CreateTableMeta(fmt.Sprintf("index_%d", idx.metadata.IndexID))

	// 添加索引键列
	for i, col := range idx.metadata.Columns {
		columnMeta := &metadata.ColumnMeta{
			Name:       col.ColumnName,
			Type:       "VARCHAR", // 简化处理，实际应该根据列类型设置
			Length:     int(col.KeyLength),
			IsNullable: false,
			IsPrimary:  i == 0, // 第一列作为主键
		}
		tableMeta.AddColumn(columnMeta)
	}

	// 添加值列
	valueColumnMeta := &metadata.ColumnMeta{
		Name:       "record_value",
		Type:       "BLOB",
		Length:     0,
		IsNullable: true,
		IsPrimary:  false,
	}
	tableMeta.AddColumn(valueColumnMeta)

	// 创建适配器来解决接口不匹配问题
	defaultTableRow := metadata.NewDefaultTableRow(tableMeta)
	adapter := &RecordTableRowTupleAdapter{TableRowTuple: defaultTableRow}
	return adapter
}

// serializeRecordData 序列化记录数据
func (idx *EnhancedBTreeIndex) serializeRecordData(key []byte, value []byte) []byte {
	// 计算需要的缓冲区大小
	keyLength := len(key)
	valueLength := len(value)

	// 记录格式：[变长字段长度列表] + [NULL标志位] + [记录头] + [实际数据]
	var buffer []byte

	// 1. 变长字段长度列表（2字节，倒序）
	// key长度
	buffer = append(buffer, byte(keyLength), byte(keyLength>>8))
	// value长度
	buffer = append(buffer, byte(valueLength), byte(valueLength>>8))

	// 2. NULL标志位（1字节，假设都不为空）
	buffer = append(buffer, 0x00)

	// 3. 记录头（5字节）
	recordHeader := make([]byte, 5)
	// 简化的记录头：[删除标志+最小记录标志+拥有记录数+堆序号+记录类型+下一记录偏移]
	recordHeader[0] = 0x00 // 删除标志=0, 最小记录标志=0, 拥有记录数=0
	recordHeader[1] = 0x00 // 堆序号低8位
	recordHeader[2] = 0x00 // 堆序号高5位 + 记录类型（0=普通记录）
	recordHeader[3] = 0x00 // 下一记录偏移低8位
	recordHeader[4] = 0x00 // 下一记录偏移高8位
	buffer = append(buffer, recordHeader...)

	// 4. 实际数据
	buffer = append(buffer, key...)
	buffer = append(buffer, value...)

	return buffer
}

// insertRecordToPage 将记录插入到页面
func (idx *EnhancedBTreeIndex) insertRecordToPage(bufferPage interface{}, record basic.Row) error {
	// 获取页面内容
	var pageContent []byte
	switch bp := bufferPage.(type) {
	case interface{ GetContent() []byte }:
		pageContent = bp.GetContent()
	case interface{ GetPageData() []byte }:
		pageContent = bp.GetPageData()
	default:
		return fmt.Errorf("unsupported buffer page type: %T", bufferPage)
	}

	// 如果页面为空，初始化为标准InnoDB页面格式
	if len(pageContent) == 0 {
		pageContent = idx.initializeEmptyPage()
	}

	// 使用页面包装器来插入记录
	indexPage, err := idx.parseOrCreateIndexPage(pageContent)
	if err != nil {
		return fmt.Errorf("failed to parse index page: %v", err)
	}

	// 获取记录的序列化数据
	recordBytes := record.ToByte()

	// 将记录添加到页面
	err = idx.addRecordToIndexPage(indexPage, recordBytes)
	if err != nil {
		return fmt.Errorf("failed to add record to index page: %v", err)
	}

	// 序列化更新后的页面
	var updatedPageContent []byte
	if serializable, ok := indexPage.(interface{ ToByte() []byte }); ok {
		updatedPageContent = serializable.ToByte()
	} else if serializable, ok := indexPage.(interface{ GetSerializeBytes() []byte }); ok {
		updatedPageContent = serializable.GetSerializeBytes()
	} else {
		return fmt.Errorf("index page does not support serialization")
	}

	// 更新缓冲页面内容
	switch bp := bufferPage.(type) {
	case interface{ SetContent([]byte) }:
		bp.SetContent(updatedPageContent)
	case interface{ SetPageData([]byte) error }:
		if err := bp.SetPageData(updatedPageContent); err != nil {
			return fmt.Errorf("failed to set page data: %v", err)
		}
	default:
		return fmt.Errorf("buffer page does not support content update")
	}

	return nil
}

// initializeEmptyPage 初始化空页面
func (idx *EnhancedBTreeIndex) initializeEmptyPage() []byte {
	pageSize := 16384 // 标准InnoDB页面大小
	pageContent := make([]byte, pageSize)

	// 文件头（38字节）
	// [4字节校验和] + [4字节页号] + [4字节前一页] + [4字节后一页] + [8字节LSN] + [2字节页类型] + ...
	binary.LittleEndian.PutUint32(pageContent[4:8], idx.metadata.RootPageNo) // 页号
	binary.LittleEndian.PutUint16(pageContent[24:26], 17855)                 // 页面类型：INDEX页面
	binary.LittleEndian.PutUint32(pageContent[34:38], idx.metadata.SpaceID)  // 表空间ID

	// 页面头（56字节，从偏移38开始）
	pageHeaderOffset := 38
	binary.LittleEndian.PutUint16(pageContent[pageHeaderOffset+2:pageHeaderOffset+4], 2)                      // 记录数（infimum+supremum）
	binary.LittleEndian.PutUint16(pageContent[pageHeaderOffset+4:pageHeaderOffset+6], 112)                    // 堆顶指针
	binary.LittleEndian.PutUint16(pageContent[pageHeaderOffset+6:pageHeaderOffset+8], 2)                      // 堆中记录数
	binary.LittleEndian.PutUint16(pageContent[pageHeaderOffset+20:pageHeaderOffset+22], 0)                    // 页面级别
	binary.LittleEndian.PutUint64(pageContent[pageHeaderOffset+22:pageHeaderOffset+30], idx.metadata.IndexID) // 索引ID

	// Infimum和Supremum记录（26字节，从偏移94开始）
	infimumSupremumOffset := 94
	// Infimum记录（13字节）
	copy(pageContent[infimumSupremumOffset:infimumSupremumOffset+8], []byte("infimum\x00"))
	pageContent[infimumSupremumOffset+8] = 0x01                                                       // 记录头信息
	pageContent[infimumSupremumOffset+9] = 0x00                                                       // 记录头信息
	pageContent[infimumSupremumOffset+10] = 0x02                                                      // 记录类型：infimum
	binary.LittleEndian.PutUint16(pageContent[infimumSupremumOffset+11:infimumSupremumOffset+13], 13) // 下一记录偏移

	// Supremum记录（13字节）
	supremumOffset := infimumSupremumOffset + 13
	copy(pageContent[supremumOffset:supremumOffset+8], []byte("supremum"))
	pageContent[supremumOffset+8] = 0x01                                               // 记录头信息
	pageContent[supremumOffset+9] = 0x00                                               // 记录头信息
	pageContent[supremumOffset+10] = 0x03                                              // 记录类型：supremum
	binary.LittleEndian.PutUint16(pageContent[supremumOffset+11:supremumOffset+13], 0) // 下一记录偏移（最后一条）

	// 页面目录（最后8字节保留给文件尾）
	directoryOffset := pageSize - 8 - 4                                                                          // 页面目录在文件尾之前
	binary.LittleEndian.PutUint16(pageContent[directoryOffset:directoryOffset+2], uint16(infimumSupremumOffset)) // infimum位置
	binary.LittleEndian.PutUint16(pageContent[directoryOffset+2:directoryOffset+4], uint16(supremumOffset))      // supremum位置

	// 文件尾（8字节）
	trailerOffset := pageSize - 8
	binary.LittleEndian.PutUint32(pageContent[trailerOffset+4:trailerOffset+8], 0) // LSN低32位

	return pageContent
}

// parseOrCreateIndexPage 解析或创建索引页面
func (idx *EnhancedBTreeIndex) parseOrCreateIndexPage(pageContent []byte) (basic.IIndexPage, error) {
	// 使用现有的页面包装器解析页面
	if len(pageContent) < 100 {
		// 页面太小，重新初始化
		pageContent = idx.initializeEmptyPage()
	}

	// 尝试使用标准的页面包装器
	indexPage := page.NewPageIndexByLoadBytes(pageContent)
	if indexPage == nil {
		return nil, fmt.Errorf("failed to create index page from content")
	}

	return indexPage, nil
}

// addRecordToIndexPage 向索引页面添加记录
func (idx *EnhancedBTreeIndex) addRecordToIndexPage(indexPage basic.IIndexPage, recordBytes []byte) error {
	// 这里需要调用页面的插入方法
	// 由于IIndexPage接口可能不包含插入方法，我们需要类型断言到具体实现

	switch page := indexPage.(type) {
	case interface{ InsertRecord([]byte) error }:
		return page.InsertRecord(recordBytes)
	case interface{ InsertRow(basic.Row) error }:
		// 创建一个临时的Row实现
		row := &SimpleRow{data: recordBytes}
		return page.InsertRow(row)
	case interface{ AddUserRecord([]byte) error }:
		return page.AddUserRecord(recordBytes)
	default:
		// 如果页面没有提供插入方法，我们直接操作页面内容
		return idx.insertRecordDirectly(indexPage, recordBytes)
	}
}

// insertRecordDirectly 直接插入记录到页面
func (idx *EnhancedBTreeIndex) insertRecordDirectly(indexPage basic.IIndexPage, recordBytes []byte) error {
	// 获取页面的字节表示
	var pageBytes []byte
	if serializable, ok := indexPage.(interface{ ToByte() []byte }); ok {
		pageBytes = serializable.ToByte()
	} else if serializable, ok := indexPage.(interface{ GetSerializeBytes() []byte }); ok {
		pageBytes = serializable.GetSerializeBytes()
	} else {
		return fmt.Errorf("index page does not support byte serialization")
	}

	// 找到用户记录区域的位置（在infimum/supremum之后）
	userRecordOffset := 120 // infimum(13) + supremum(13) + 页面头(94) = 120

	// 在用户记录区域插入新记录
	// 这是一个简化的实现，实际的InnoDB会维护更复杂的记录链表和页面目录

	// 计算新记录应该插入的位置
	insertOffset := userRecordOffset

	// 检查是否有足够的空间
	freeSpaceStart := insertOffset + len(recordBytes)
	directoryStart := len(pageBytes) - 8 - 4 // 文件尾(8) + 页面目录起始

	if freeSpaceStart >= directoryStart {
		return fmt.Errorf("not enough space in page for new record")
	}

	// 插入记录（简化实现）
	copy(pageBytes[insertOffset:insertOffset+len(recordBytes)], recordBytes)

	// 更新页面头中的记录数
	pageHeaderOffset := 38
	currentRecordCount := binary.LittleEndian.Uint16(pageBytes[pageHeaderOffset+2 : pageHeaderOffset+4])
	binary.LittleEndian.PutUint16(pageBytes[pageHeaderOffset+2:pageHeaderOffset+4], currentRecordCount+1)

	// 更新堆顶指针
	newHeapTop := insertOffset + len(recordBytes)
	binary.LittleEndian.PutUint16(pageBytes[pageHeaderOffset+4:pageHeaderOffset+6], uint16(newHeapTop))

	return nil
}

// SimpleRow 简单的Row实现，用于接口适配
type SimpleRow struct {
	data []byte
}

func (r *SimpleRow) Less(than basic.Row) bool                              { return false }
func (r *SimpleRow) ToByte() []byte                                        { return r.data }
func (r *SimpleRow) IsInfimumRow() bool                                    { return false }
func (r *SimpleRow) IsSupremumRow() bool                                   { return false }
func (r *SimpleRow) GetPageNumber() uint32                                 { return 0 }
func (r *SimpleRow) WriteWithNull(content []byte)                          {}
func (r *SimpleRow) GetRowLength() uint16                                  { return uint16(len(r.data)) }
func (r *SimpleRow) GetHeaderLength() uint16                               { return 5 } // 简化的头部长度
func (r *SimpleRow) GetPrimaryKey() basic.Value                            { return basic.NewStringValue("") }
func (r *SimpleRow) ReadValueByIndex(index int) basic.Value                { return basic.NewStringValue("") }
func (r *SimpleRow) GetFieldLength() int                                   { return 1 }                        // 简化实现
func (r *SimpleRow) GetHeapNo() uint16                                     { return 0 }                        // 简化实现
func (r *SimpleRow) GetNOwned() byte                                       { return 0 }                        // 简化实现
func (r *SimpleRow) GetNextRowOffset() uint16                              { return 0 }                        // 简化实现
func (r *SimpleRow) SetNextRowOffset(offset uint16)                        {}                                  // 简化实现
func (r *SimpleRow) SetHeapNo(heapNo uint16)                               {}                                  // 简化实现
func (r *SimpleRow) SetTransactionId(trxId uint64)                         {}                                  // 简化实现
func (r *SimpleRow) GetValueByColName(colName string) basic.Value          { return basic.NewStringValue("") } // 简化实现
func (r *SimpleRow) WriteBytesWithNullWithsPos(content []byte, index byte) {}                                  // 简化实现
func (r *SimpleRow) SetNOwned(cnt byte)                                    {}                                  // 简化实现
func (r *SimpleRow) ToString() string                                      { return "SimpleRow{}" }            // 简化实现

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
	if len(page.Records) > 0 {
		for i, record := range page.Records {
			if idx.compareKeys(key, record.Key) < 0 {
				if i == 0 {
					return nil, binary.LittleEndian.Uint32(record.Value), nil
				}
				return nil, binary.LittleEndian.Uint32(page.Records[i-1].Value), nil
			}
		}
		last := page.Records[len(page.Records)-1]
		return nil, binary.LittleEndian.Uint32(last.Value), nil
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
	p, ok := bufferPage.(basic.IPage)
	if !ok {
		return nil, fmt.Errorf("invalid buffer page")
	}

	data := p.GetData()
	if len(data) < 42 {
		return nil, fmt.Errorf("invalid page data")
	}

	pageType := BTreePageTypeLeaf
	if !p.IsLeafPage() {
		pageType = BTreePageTypeInternal
	}

	recordCount := binary.LittleEndian.Uint16(data[40:42])
	prev := binary.LittleEndian.Uint32(data[8:12])
	next := binary.LittleEndian.Uint32(data[12:16])

	page := &BTreePage{
		PageNo:      p.GetPageNo(),
		PageType:    pageType,
		Level:       0,
		RecordCount: recordCount,
		FreeSpace:   0,
		NextPage:    next,
		PrevPage:    prev,
		Records:     make([]IndexRecord, 0),
		IsLoaded:    true,
		IsDirty:     p.IsDirty(),
		LastAccess:  time.Now(),
		PinCount:    1,
	}

	return page, nil
}

// flushPage 刷新页面到存储
func (idx *EnhancedBTreeIndex) flushPage(ctx context.Context, page *BTreePage) error {
	// 获取缓冲池页面
	bufferPage, err := idx.storageManager.GetBufferPoolManager().GetPage(idx.metadata.SpaceID, page.PageNo)
	if err != nil {
		return err
	}

	data := bufferPage.GetContent()
	if len(data) >= 42 {
		binary.LittleEndian.PutUint32(data[8:12], page.PrevPage)
		binary.LittleEndian.PutUint32(data[12:16], page.NextPage)
		binary.LittleEndian.PutUint16(data[40:42], page.RecordCount)
		bufferPage.SetContent(data)
	}

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

// countNonZeroBytes 统计非零字节数量
func (idx *EnhancedBTreeIndex) countNonZeroBytes(data []byte) int {
	count := 0
	for _, b := range data {
		if b != 0 {
			count++
		}
	}
	return count
}

// getRecordCountFromPage 从页面头部获取记录数量
func (idx *EnhancedBTreeIndex) getRecordCountFromPage(content []byte) uint16 {
	if len(content) < 42 {
		return 0
	}
	// 页面头部偏移38，记录数量在偏移40-42
	return binary.LittleEndian.Uint16(content[40:42])
}
