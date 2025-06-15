package manager

import (
	"context"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/logger"
	"sync"
	"sync/atomic"
	"time"
)

// EnhancedBTreeManager 增强版B+树管理器
type EnhancedBTreeManager struct {
	// 核心组件
	storageManager  *StorageManager       // 存储管理器
	metadataManager *IndexMetadataManager // 索引元信息管理器
	config          *BTreeConfig          // 配置

	// 索引实例管理
	mu             sync.RWMutex                   // 读写锁
	loadedIndexes  map[uint64]*EnhancedBTreeIndex // 已加载的索引实例
	indexLoadOrder []uint64                       // 索引加载顺序（用于LRU）

	// 统计信息
	stats *BTreeManagerStats // 管理器统计

	// 后台任务
	stopChan     chan struct{}  // 停止信号
	backgroundWG sync.WaitGroup // 后台任务等待组

	// 资源管理
	isShutdown atomic.Bool // 是否已关闭
}

// BTreeManagerStats B+树管理器统计信息
type BTreeManagerStats struct {
	// 索引管理统计
	IndexesLoaded    uint64 // 已加载索引数
	IndexLoadCount   uint64 // 索引加载次数
	IndexUnloadCount uint64 // 索引卸载次数
	IndexCacheHits   uint64 // 索引缓存命中
	IndexCacheMisses uint64 // 索引缓存未命中

	// 操作统计
	SearchOperations uint64 // 搜索操作数
	InsertOperations uint64 // 插入操作数
	DeleteOperations uint64 // 删除操作数
	RangeOperations  uint64 // 范围查询操作数

	// 性能统计
	AvgSearchTime time.Duration // 平均搜索时间
	AvgInsertTime time.Duration // 平均插入时间
	AvgDeleteTime time.Duration // 平均删除时间

	// 最后更新时间
	LastUpdate time.Time
}

// NewEnhancedBTreeManager 创建增强版B+树管理器
func NewEnhancedBTreeManager(storageManager *StorageManager, config *BTreeConfig) *EnhancedBTreeManager {
	if config == nil {
		config = DefaultBTreeConfig
	}

	manager := &EnhancedBTreeManager{
		storageManager:  storageManager,
		metadataManager: NewIndexMetadataManager(),
		config:          config,
		loadedIndexes:   make(map[uint64]*EnhancedBTreeIndex),
		indexLoadOrder:  make([]uint64, 0),
		stats: &BTreeManagerStats{
			LastUpdate: time.Now(),
		},
		stopChan: make(chan struct{}),
	}

	// 启动后台任务
	manager.startBackgroundTasks()

	return manager
}

// CreateIndex 创建新索引
func (m *EnhancedBTreeManager) CreateIndex(ctx context.Context, metadata *IndexMetadata) (BTreeIndex, error) {
	if m.isShutdown.Load() {
		return nil, fmt.Errorf("btree manager is shutdown")
	}

	// 验证索引元信息
	if err := m.validateIndexMetadata(metadata); err != nil {
		return nil, fmt.Errorf("invalid index metadata: %v", err)
	}

	// 分配根页面
	rootPageNo, err := m.allocateRootPage(ctx, metadata.SpaceID)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate root page: %v", err)
	}

	// 设置根页号
	metadata.RootPageNo = rootPageNo
	metadata.CreateTime = time.Now()
	metadata.UpdateTime = time.Now()
	metadata.IndexState = EnhancedIndexStateBuilding

	// 注册索引元信息
	if err := m.metadataManager.RegisterIndex(metadata); err != nil {
		return nil, fmt.Errorf("failed to register index metadata: %v", err)
	}

	// 创建索引实例
	index := NewEnhancedBTreeIndex(metadata, m.storageManager, m.config)

	// 初始化索引结构
	if err := index.InitializeEmptyIndex(ctx); err != nil {
		// 清理资源
		m.metadataManager.RemoveIndex(metadata.IndexID)
		return nil, fmt.Errorf("failed to initialize index: %v", err)
	}

	// 加载索引到内存
	m.mu.Lock()
	m.loadedIndexes[metadata.IndexID] = index
	m.indexLoadOrder = append(m.indexLoadOrder, metadata.IndexID)
	m.mu.Unlock()

	// 更新索引状态
	metadata.IndexState = EnhancedIndexStateActive
	metadata.IsLoaded = true

	atomic.AddUint64(&m.stats.IndexLoadCount, 1)
	atomic.AddUint64(&m.stats.IndexesLoaded, 1)

	logger.Debugf(" Created index %d '%s' for table %d\n",
		metadata.IndexID, metadata.IndexName, metadata.TableID)

	return index, nil
}

// GetIndex 获取索引实例
func (m *EnhancedBTreeManager) GetIndex(indexID uint64) (BTreeIndex, error) {
	if m.isShutdown.Load() {
		return nil, fmt.Errorf("btree manager is shutdown")
	}

	// 先检查内存缓存
	m.mu.RLock()
	index, exists := m.loadedIndexes[indexID]
	if exists {
		m.mu.RUnlock()
		atomic.AddUint64(&m.stats.IndexCacheHits, 1)
		// 更新访问顺序
		m.updateIndexAccessOrder(indexID)
		return index, nil
	}
	m.mu.RUnlock()

	atomic.AddUint64(&m.stats.IndexCacheMisses, 1)

	// 从元信息管理器获取索引元信息
	metadata, err := m.metadataManager.GetIndexMetadata(indexID)
	if err != nil {
		return nil, fmt.Errorf("index %d not found: %v", indexID, err)
	}

	// 按需加载索引
	return m.loadIndex(context.Background(), metadata)
}

// GetIndexByName 根据名称获取索引
func (m *EnhancedBTreeManager) GetIndexByName(tableID uint64, indexName string) (BTreeIndex, error) {
	metadata, err := m.metadataManager.GetIndexByName(tableID, indexName)
	if err != nil {
		return nil, err
	}

	return m.GetIndex(metadata.IndexID)
}

// Insert 插入记录
func (m *EnhancedBTreeManager) Insert(ctx context.Context, indexID uint64, key []byte, value []byte) error {
	start := time.Now()
	defer func() {
		atomic.AddUint64(&m.stats.InsertOperations, 1)
		// 更新平均插入时间（简化计算）
		m.stats.AvgInsertTime = (m.stats.AvgInsertTime + time.Since(start)) / 2
	}()

	index, err := m.GetIndex(indexID)
	if err != nil {
		return err
	}

	enhancedIndex, ok := index.(*EnhancedBTreeIndex)
	if !ok {
		return fmt.Errorf("invalid index type")
	}

	return enhancedIndex.Insert(ctx, key, value)
}

// Search 搜索记录
func (m *EnhancedBTreeManager) Search(ctx context.Context, indexID uint64, key []byte) (*IndexRecord, error) {
	start := time.Now()
	defer func() {
		atomic.AddUint64(&m.stats.SearchOperations, 1)
		// 更新平均搜索时间（简化计算）
		m.stats.AvgSearchTime = (m.stats.AvgSearchTime + time.Since(start)) / 2
	}()

	index, err := m.GetIndex(indexID)
	if err != nil {
		return nil, err
	}

	enhancedIndex, ok := index.(*EnhancedBTreeIndex)
	if !ok {
		return nil, fmt.Errorf("invalid index type")
	}

	return enhancedIndex.Search(ctx, key)
}

// Delete 删除记录
func (m *EnhancedBTreeManager) Delete(ctx context.Context, indexID uint64, key []byte) error {
	start := time.Now()
	defer func() {
		atomic.AddUint64(&m.stats.DeleteOperations, 1)
		// 更新平均删除时间（简化计算）
		m.stats.AvgDeleteTime = (m.stats.AvgDeleteTime + time.Since(start)) / 2
	}()

	index, err := m.GetIndex(indexID)
	if err != nil {
		return err
	}

	enhancedIndex, ok := index.(*EnhancedBTreeIndex)
	if !ok {
		return fmt.Errorf("invalid index type")
	}

	return enhancedIndex.Delete(ctx, key)
}

// RangeSearch 范围搜索
func (m *EnhancedBTreeManager) RangeSearch(ctx context.Context, indexID uint64, startKey, endKey []byte) ([]IndexRecord, error) {
	defer func() {
		atomic.AddUint64(&m.stats.RangeOperations, 1)
	}()

	index, err := m.GetIndex(indexID)
	if err != nil {
		return nil, err
	}

	enhancedIndex, ok := index.(*EnhancedBTreeIndex)
	if !ok {
		return nil, fmt.Errorf("invalid index type")
	}

	return enhancedIndex.RangeSearch(ctx, startKey, endKey)
}

// LoadIndex 加载索引
func (m *EnhancedBTreeManager) LoadIndex(ctx context.Context, indexID uint64) error {
	metadata, err := m.metadataManager.GetIndexMetadata(indexID)
	if err != nil {
		return err
	}

	_, err = m.loadIndex(ctx, metadata)
	return err
}

// UnloadIndex 卸载索引
func (m *EnhancedBTreeManager) UnloadIndex(indexID uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	index, exists := m.loadedIndexes[indexID]
	if !exists {
		return nil // 索引本来就没有加载
	}

	// 检查引用计数
	if index.GetRefCount() > 0 {
		return fmt.Errorf("index %d is still in use (ref count: %d)", indexID, index.GetRefCount())
	}

	// 刷新索引
	if err := index.Flush(context.Background()); err != nil {
		return fmt.Errorf("failed to flush index %d: %v", indexID, err)
	}

	// 从内存中移除
	delete(m.loadedIndexes, indexID)

	// 更新加载顺序
	for i, id := range m.indexLoadOrder {
		if id == indexID {
			m.indexLoadOrder = append(m.indexLoadOrder[:i], m.indexLoadOrder[i+1:]...)
			break
		}
	}

	// 更新元信息
	metadata, _ := m.metadataManager.GetIndexMetadata(indexID)
	if metadata != nil {
		metadata.IsLoaded = false
	}

	atomic.AddUint64(&m.stats.IndexUnloadCount, 1)
	atomic.AddUint64(&m.stats.IndexesLoaded, ^uint64(0)) // 原子减1

	logger.Debugf(" Unloaded index %d from memory\n", indexID)
	return nil
}

// FlushIndex 刷新索引
func (m *EnhancedBTreeManager) FlushIndex(ctx context.Context, indexID uint64) error {
	index, err := m.GetIndex(indexID)
	if err != nil {
		return err
	}

	return index.Flush(ctx)
}

// AnalyzeIndex 分析索引
func (m *EnhancedBTreeManager) AnalyzeIndex(ctx context.Context, indexID uint64) (*EnhancedIndexStatistics, error) {
	index, err := m.GetIndex(indexID)
	if err != nil {
		return nil, err
	}

	enhancedIndex, ok := index.(*EnhancedBTreeIndex)
	if !ok {
		return nil, fmt.Errorf("invalid index type")
	}

	if err := enhancedIndex.UpdateStatistics(ctx); err != nil {
		return nil, err
	}

	return enhancedIndex.GetStatistics(), nil
}

// RebuildIndex 重建索引
func (m *EnhancedBTreeManager) RebuildIndex(ctx context.Context, indexID uint64) error {
	// TODO: 实现索引重建逻辑
	return fmt.Errorf("index rebuild not implemented yet")
}

// DropIndex 删除索引
func (m *EnhancedBTreeManager) DropIndex(ctx context.Context, indexID uint64) error {
	// 卸载索引
	if err := m.UnloadIndex(indexID); err != nil {
		return fmt.Errorf("failed to unload index: %v", err)
	}

	// 删除索引文件/页面
	metadata, err := m.metadataManager.GetIndexMetadata(indexID)
	if err != nil {
		return err
	}

	// TODO: 实现删除索引页面的逻辑

	// 移除元信息
	if err := m.metadataManager.RemoveIndex(indexID); err != nil {
		return fmt.Errorf("failed to remove index metadata: %v", err)
	}

	logger.Debugf("🗑️  Dropped index %d '%s'\n", indexID, metadata.IndexName)
	return nil
}

// Close 关闭管理器
func (m *EnhancedBTreeManager) Close() error {
	if m.isShutdown.Load() {
		return nil
	}

	m.isShutdown.Store(true)

	// 停止后台任务
	close(m.stopChan)
	m.backgroundWG.Wait()

	// 卸载所有索引
	m.mu.Lock()
	indexIDs := make([]uint64, 0, len(m.loadedIndexes))
	for indexID := range m.loadedIndexes {
		indexIDs = append(indexIDs, indexID)
	}
	m.mu.Unlock()

	for _, indexID := range indexIDs {
		if err := m.UnloadIndex(indexID); err != nil {
			logger.Debugf("  Failed to unload index %d: %v\n", indexID, err)
		}
	}

	logger.Debug("🔒 BTree Manager closed")
	return nil
}

// 内部方法

// loadIndex 加载索引到内存
func (m *EnhancedBTreeManager) loadIndex(ctx context.Context, metadata *IndexMetadata) (BTreeIndex, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 再次检查是否已加载（双重检查锁定）
	if index, exists := m.loadedIndexes[metadata.IndexID]; exists {
		return index, nil
	}

	// 检查内存限制
	if err := m.enforceMemoryLimits(); err != nil {
		return nil, err
	}

	// 创建索引实例
	index := NewEnhancedBTreeIndex(metadata, m.storageManager, m.config)

	// 从磁盘加载索引结构
	if err := index.LoadFromStorage(ctx); err != nil {
		return nil, fmt.Errorf("failed to load index from storage: %v", err)
	}

	// 加载到内存
	m.loadedIndexes[metadata.IndexID] = index
	m.indexLoadOrder = append(m.indexLoadOrder, metadata.IndexID)

	// 更新元信息
	metadata.IsLoaded = true
	metadata.LastAccess = time.Now()

	atomic.AddUint64(&m.stats.IndexLoadCount, 1)
	atomic.AddUint64(&m.stats.IndexesLoaded, 1)

	logger.Debugf(" Loaded index %d '%s' into memory\n", metadata.IndexID, metadata.IndexName)

	return index, nil
}

// updateIndexAccessOrder 更新索引访问顺序
func (m *EnhancedBTreeManager) updateIndexAccessOrder(indexID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 将索引移到访问顺序的末尾（最近访问）
	for i, id := range m.indexLoadOrder {
		if id == indexID {
			// 移除当前位置
			m.indexLoadOrder = append(m.indexLoadOrder[:i], m.indexLoadOrder[i+1:]...)
			// 添加到末尾
			m.indexLoadOrder = append(m.indexLoadOrder, indexID)
			break
		}
	}
}

// enforceMemoryLimits 强制执行内存限制
func (m *EnhancedBTreeManager) enforceMemoryLimits() error {
	// 如果加载的索引数量超过限制，卸载最久未访问的索引
	maxLoadedIndexes := int(m.config.MaxCacheSize / 10) // 简化计算
	if maxLoadedIndexes < 10 {
		maxLoadedIndexes = 10
	}

	if len(m.loadedIndexes) >= maxLoadedIndexes {
		// 卸载最久未访问的索引（LRU）
		oldestIndexID := m.indexLoadOrder[0]

		// 临时释放锁来卸载索引
		m.mu.Unlock()
		err := m.UnloadIndex(oldestIndexID)
		m.mu.Lock()

		if err != nil {
			return fmt.Errorf("failed to unload index %d to free memory: %v", oldestIndexID, err)
		}
	}

	return nil
}

// validateIndexMetadata 验证索引元信息
func (m *EnhancedBTreeManager) validateIndexMetadata(metadata *IndexMetadata) error {
	if metadata == nil {
		return fmt.Errorf("metadata is nil")
	}

	if metadata.IndexName == "" {
		return fmt.Errorf("index name is empty")
	}

	if metadata.TableID == 0 {
		return fmt.Errorf("table ID is zero")
	}

	if metadata.SpaceID == 0 {
		return fmt.Errorf("space ID is zero")
	}

	if len(metadata.Columns) == 0 {
		return fmt.Errorf("no columns specified")
	}

	return nil
}

// allocateRootPage 分配根页面
func (m *EnhancedBTreeManager) allocateRootPage(ctx context.Context, spaceID uint32) (uint32, error) {
	// 简化实现：使用时间戳作为页号
	// 实际应该从存储管理器分配页面
	rootPageNo := uint32(time.Now().Unix())%100000 + 1000

	// TODO: 真正的页面分配逻辑
	// bufferPage, err := m.storageManager.GetBufferPoolManager().AllocatePage(spaceID)
	// return bufferPage.GetPageNo(), err

	return rootPageNo, nil
}

// startBackgroundTasks 启动后台任务
func (m *EnhancedBTreeManager) startBackgroundTasks() {
	// 统计更新任务
	m.backgroundWG.Add(1)
	go m.statisticsUpdateTask()

	// 缓存清理任务
	m.backgroundWG.Add(1)
	go m.cacheCleanupTask()
}

// statisticsUpdateTask 统计更新任务
func (m *EnhancedBTreeManager) statisticsUpdateTask() {
	defer m.backgroundWG.Done()

	ticker := time.NewTicker(m.config.StatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.updateStatistics()
		case <-m.stopChan:
			return
		}
	}
}

// cacheCleanupTask 缓存清理任务
func (m *EnhancedBTreeManager) cacheCleanupTask() {
	defer m.backgroundWG.Done()

	ticker := time.NewTicker(time.Minute * 5) // 每5分钟清理一次
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanupUnusedIndexes()
		case <-m.stopChan:
			return
		}
	}
}

// updateStatistics 更新统计信息
func (m *EnhancedBTreeManager) updateStatistics() {
	m.mu.RLock()
	loadedCount := uint64(len(m.loadedIndexes))
	m.mu.RUnlock()

	atomic.StoreUint64(&m.stats.IndexesLoaded, loadedCount)
	m.stats.LastUpdate = time.Now()
}

// cleanupUnusedIndexes 清理未使用的索引
func (m *EnhancedBTreeManager) cleanupUnusedIndexes() {
	m.mu.RLock()
	var candidatesForUnload []uint64

	for indexID, index := range m.loadedIndexes {
		// 如果索引没有引用且超过一段时间未访问，标记为卸载候选
		if index.GetRefCount() == 0 {
			metadata := index.GetMetadata()
			if time.Since(metadata.LastAccess) > time.Hour {
				candidatesForUnload = append(candidatesForUnload, indexID)
			}
		}
	}
	m.mu.RUnlock()

	// 卸载候选索引
	for _, indexID := range candidatesForUnload {
		if err := m.UnloadIndex(indexID); err != nil {
			logger.Debugf("  Failed to unload unused index %d: %v\n", indexID, err)
		}
	}

	if len(candidatesForUnload) > 0 {
		logger.Debugf("🧹 Cleaned up %d unused indexes\n", len(candidatesForUnload))
	}
}

// GetStats 获取管理器统计信息
func (m *EnhancedBTreeManager) GetStats() *BTreeManagerStats {
	return m.stats
}

// GetLoadedIndexCount 获取已加载索引数量
func (m *EnhancedBTreeManager) GetLoadedIndexCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.loadedIndexes)
}
