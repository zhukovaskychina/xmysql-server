package manager

import (
	"context"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"sync"
)

// TableManager 表管理器
type TableManager struct {
	mu sync.RWMutex
	// schema管理器
	schemaManager metadata.InfoSchemaManager
	// 存储管理器
	storageManager *StorageManager
	// 表存储映射管理器
	tableStorageManager *TableStorageManager
	// 缓存表的元数据
	tableMetaCache map[string]*metadata.TableMeta
	// 缓存表的统计信息
	tableStatsCache map[string]*metadata.InfoTableStats
	// 缓存表的索引
	tableIndexCache map[string][]*Index
}

// NewTableManager 创建表管理器
func NewTableManager(schemaManager metadata.InfoSchemaManager) *TableManager {
	return &TableManager{
		schemaManager:   schemaManager,
		tableMetaCache:  make(map[string]*metadata.TableMeta),
		tableStatsCache: make(map[string]*metadata.InfoTableStats),
		tableIndexCache: make(map[string][]*Index),
	}
}

// NewTableManagerWithStorage 创建带存储管理器的表管理器
func NewTableManagerWithStorage(schemaManager metadata.InfoSchemaManager, storageManager *StorageManager) *TableManager {
	tm := &TableManager{
		schemaManager:   schemaManager,
		storageManager:  storageManager,
		tableMetaCache:  make(map[string]*metadata.TableMeta),
		tableStatsCache: make(map[string]*metadata.InfoTableStats),
		tableIndexCache: make(map[string][]*Index),
	}

	// 创建表存储映射管理器
	if storageManager != nil {
		tm.tableStorageManager = NewTableStorageManager(storageManager)
	}

	return tm
}

// CreateTable 创建表
func (tm *TableManager) CreateTable(ctx context.Context, schemaName string, table *metadata.Table) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 检查schema是否存在
	if !tm.schemaManager.HasSchema(ctx, schemaName) {
		return fmt.Errorf("schema %s not exists", schemaName)
	}

	// 检查表是否已存在
	if tm.schemaManager.HasTable(ctx, schemaName, "table_name") {
		return fmt.Errorf("table %s already exists in schema %s", "table_name", schemaName)
	}

	// 创建表
	if err := tm.schemaManager.CreateTable(ctx, schemaName, table); err != nil {
		return fmt.Errorf("create table failed: %v", err)
	}

	// 初始化表的元数据
	tableMeta := &metadata.TableMeta{
		Name:    "table_name",
		Columns: make([]*metadata.ColumnMeta, 0),
		Indices: make([]metadata.IndexMeta, 0),
	}

	// 缓存表的元数据
	cacheKey := tm.getCacheKey(schemaName, "table_name")
	tm.tableMetaCache[cacheKey] = tableMeta
	tm.tableIndexCache[cacheKey] = make([]*Index, 0)

	// 初始化表的统计信息
	stats := &metadata.InfoTableStats{
		RowCount:    0,
		AvgRowSize:  0,
		DataSize:    0,
		IndexSize:   0,
		ColumnStats: make(map[string]metadata.Stats),
		IndexStats:  make(map[string]metadata.Stats),
	}
	tm.tableStatsCache[cacheKey] = stats

	return nil
}

// DropTable 删除表
func (tm *TableManager) DropTable(ctx context.Context, schemaName, tableName string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 检查schema和表是否存在
	if !tm.schemaManager.HasSchema(ctx, schemaName) {
		return fmt.Errorf("schema %s not exists", schemaName)
	}
	if !tm.schemaManager.HasTable(ctx, schemaName, tableName) {
		return fmt.Errorf("table %s not exists in schema %s", tableName, schemaName)
	}

	// 删除表
	if err := tm.schemaManager.DropTable(ctx, schemaName, tableName); err != nil {
		return fmt.Errorf("drop table failed: %v", err)
	}

	// 清除缓存
	cacheKey := tm.getCacheKey(schemaName, tableName)
	delete(tm.tableMetaCache, cacheKey)
	delete(tm.tableStatsCache, cacheKey)
	delete(tm.tableIndexCache, cacheKey)

	return nil
}

// GetTableMetadata 获取表的元数据
func (tm *TableManager) GetTableMetadata(ctx context.Context, schemaName, tableName string) (*metadata.TableMeta, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	cacheKey := tm.getCacheKey(schemaName, tableName)
	if metadata, ok := tm.tableMetaCache[cacheKey]; ok {
		return metadata, nil
	}

	// 从schema管理器获取
	metadata, err := tm.schemaManager.GetTableMetadata(ctx, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("get table metadata failed: %v", err)
	}

	// 更新缓存
	tm.tableMetaCache[cacheKey] = metadata
	return metadata, nil
}

// UpdateTableStats 更新表的统计信息
func (tm *TableManager) UpdateTableStats(ctx context.Context, schemaName, tableName string, stats *metadata.InfoTableStats) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 更新schema管理器中的统计信息
	if err := tm.schemaManager.UpdateTableStats(ctx, schemaName, tableName, stats); err != nil {
		return fmt.Errorf("update table stats failed: %v", err)
	}

	// 更新缓存
	cacheKey := tm.getCacheKey(schemaName, tableName)
	tm.tableStatsCache[cacheKey] = stats
	return nil
}

// GetTableStats 获取表的统计信息
func (tm *TableManager) GetTableStats(ctx context.Context, schemaName, tableName string) (*metadata.InfoTableStats, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	cacheKey := tm.getCacheKey(schemaName, tableName)
	if stats, ok := tm.tableStatsCache[cacheKey]; ok {
		return stats, nil
	}

	// 从schema管理器获取
	stats, err := tm.schemaManager.GetTableStats(ctx, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("get table stats failed: %v", err)
	}

	// 更新缓存
	tm.tableStatsCache[cacheKey] = stats
	return stats, nil
}

// GetTableIndices 获取表的索引
func (tm *TableManager) GetTableIndices(ctx context.Context, schemaName, tableName string) ([]*Index, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	cacheKey := tm.getCacheKey(schemaName, tableName)
	if indices, ok := tm.tableIndexCache[cacheKey]; ok {
		return indices, nil
	}

	// 从schema管理器获取表元数据
	metadata, err := tm.schemaManager.GetTableMetadata(ctx, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("get table metadata failed: %v", err)
	}

	// 转换IndexMeta到Index类型
	var indices []*Index
	for _, indexMeta := range metadata.Indices {
		idx := &Index{
			Name:     indexMeta.Name,
			Columns:  convertColumnsFromMeta(indexMeta.Columns),
			IsUnique: indexMeta.Unique,
		}
		indices = append(indices, idx)
	}

	// 更新缓存
	tm.tableIndexCache[cacheKey] = indices
	return indices, nil
}

// convertColumnsFromMeta 转换列元数据到索引列
func convertColumnsFromMeta(columnNames []string) []Column {
	var columns []Column
	for _, name := range columnNames {
		columns = append(columns, Column{
			Name: name,
		})
	}
	return columns
}

// RefreshTableMetadata 刷新表的元数据
func (tm *TableManager) RefreshTableMetadata(ctx context.Context, schemaName, tableName string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 从schema管理器刷新元数据
	if err := tm.schemaManager.RefreshMetadata(ctx, schemaName); err != nil {
		return fmt.Errorf("refresh metadata failed: %v", err)
	}

	// 获取最新的元数据
	metadata, err := tm.schemaManager.GetTableMetadata(ctx, schemaName, tableName)
	if err != nil {
		return fmt.Errorf("get table metadata failed: %v", err)
	}

	// 更新缓存
	cacheKey := tm.getCacheKey(schemaName, tableName)
	tm.tableMetaCache[cacheKey] = metadata

	// 转换并更新索引缓存
	var indices []*Index
	for _, indexMeta := range metadata.Indices {
		idx := &Index{
			Name:     indexMeta.Name,
			Columns:  convertColumnsFromMeta(indexMeta.Columns),
			IsUnique: indexMeta.Unique,
		}
		indices = append(indices, idx)
	}
	tm.tableIndexCache[cacheKey] = indices

	return nil
}

// getCacheKey 生成缓存key
func (tm *TableManager) getCacheKey(schemaName, tableName string) string {
	return fmt.Sprintf("%s.%s", schemaName, tableName)
}

// GetTableBTreeManager 为指定表获取B+树管理器
func (tm *TableManager) GetTableBTreeManager(ctx context.Context, schemaName, tableName string) (*DefaultBPlusTreeManager, error) {
	if tm.tableStorageManager == nil {
		return nil, fmt.Errorf("table storage manager not available")
	}

	return tm.tableStorageManager.CreateBTreeManagerForTable(ctx, schemaName, tableName)
}

// GetTableStorageInfo 获取表的存储信息
func (tm *TableManager) GetTableStorageInfo(schemaName, tableName string) (*TableStorageInfo, error) {
	if tm.tableStorageManager == nil {
		return nil, fmt.Errorf("table storage manager not available")
	}

	return tm.tableStorageManager.GetTableStorageInfo(schemaName, tableName)
}
