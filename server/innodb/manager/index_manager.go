package manager

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// IndexManager 管理表的索引
type IndexManager struct {
	mu sync.RWMutex

	// 索引映射: index_id -> index
	indexes map[uint64]*Index

	// 段管理器
	segmentManager *SegmentManager

	// B+树管理器 - 使用接口类型以支持增强版
	btreeManager basic.BPlusTreeManager

	// 缓冲池管理器
	bufferPoolManager *OptimizedBufferPoolManager
	storageManager    *StorageManager

	// 索引统计信息
	stats *IndexManagerStats

	// 配置
	config *IndexManagerConfig
}

// Index 表示一个索引
type Index struct {
	IndexID    uint64   // 索引ID
	TableID    uint64   // 表ID
	SpaceID    uint32   // 表空间ID
	Name       string   // 索引名称
	Type       uint8    // 索引类型
	Columns    []Column // 索引列
	IsUnique   bool     // 是否唯一索引
	IsPrimary  bool     // 是否主键索引
	SegmentID  uint32   // 关联的段ID
	RootPageNo uint32   // B+树根页号
	Height     uint8    // B+树高度
	PageCount  uint32   // 索引页数

	// 索引状态
	State      IndexState // 索引状态
	CreateTime time.Time  // 创建时间
	UpdateTime time.Time  // 更新时间

	// 索引统计
	KeyCount     uint64 // 键数量
	LeafPages    uint32 // 叶子页数
	NonLeafPages uint32 // 非叶子页数
}

// Column 表示索引列
type Column struct {
	Name      string // 列名
	Type      uint8  // 数据类型
	Length    uint16 // 长度
	Nullable  bool   // 是否可空
	Ascending bool   // 是否升序
	Position  uint8  // 在索引中的位置
}

// SecondaryIndexEntry is one durable secondary-index key/value pair rebuilt
// from a clustered row.
type SecondaryIndexEntry struct {
	Key   []byte
	Value []byte
}

type secondaryIndexRepairRow struct {
	Values     map[string]interface{}
	PrimaryKey []byte
}

// IndexState 表示索引状态
type IndexState uint8

const (
	IndexStateBuilding IndexState = iota // 构建中
	IndexStateActive                     // 活跃状态
	IndexStateDisabled                   // 已禁用
	IndexStateDropping                   // 删除中
)

// 索引类型常量
const (
	INDEX_TYPE_BTREE    uint8 = iota // B+树索引
	INDEX_TYPE_HASH                  // 哈希索引
	INDEX_TYPE_FULLTEXT              // 全文索引
)

// IndexManagerStats 索引管理器统计信息
type IndexManagerStats struct {
	TotalIndexes   uint64
	ActiveIndexes  uint64
	PrimaryIndexes uint64
	UniqueIndexes  uint64
	SearchCount    uint64
	InsertCount    uint64
	DeleteCount    uint64
	UpdateCount    uint64
}

// IndexManagerConfig 索引管理器配置
type IndexManagerConfig struct {
	MaxIndexes       uint64        // 最大索引数
	CacheSize        uint32        // 缓存大小
	FlushInterval    time.Duration // 刷新间隔
	StatsInterval    time.Duration // 统计间隔
	EnableStatistics bool          // 是否启用统计
}

// NewIndexManager 创建索引管理器
func NewIndexManager(segmentManager *SegmentManager, bufferPoolManager *OptimizedBufferPoolManager, config *IndexManagerConfig) *IndexManager {
	if config == nil {
		config = &IndexManagerConfig{
			MaxIndexes:       10000,
			CacheSize:        1000,
			FlushInterval:    time.Second * 30,
			StatsInterval:    time.Minute * 5,
			EnableStatistics: true,
		}
	}

	im := &IndexManager{
		indexes:           make(map[uint64]*Index),
		segmentManager:    segmentManager,
		bufferPoolManager: bufferPoolManager,
		config:            config,
		stats:             &IndexManagerStats{},
	}

	return im
}

// NewIndexManagerWithStorage 创建带存储管理器的索引管理器
func NewIndexManagerWithStorage(segmentManager *SegmentManager, bufferPoolManager *OptimizedBufferPoolManager, storageManager *StorageManager, config *IndexManagerConfig) *IndexManager {
	if config == nil {
		config = &IndexManagerConfig{
			MaxIndexes:       10000,
			CacheSize:        1000,
			FlushInterval:    time.Second * 30,
			StatsInterval:    time.Minute * 5,
			EnableStatistics: true,
		}
	}

	im := &IndexManager{
		indexes:           make(map[uint64]*Index),
		segmentManager:    segmentManager,
		bufferPoolManager: bufferPoolManager,
		storageManager:    storageManager,
		config:            config,
		stats:             &IndexManagerStats{},
	}

	// 创建增强版B+树管理器适配器
	btreeConfig := &BTreeConfig{
		MaxCacheSize:   config.CacheSize,
		CachePolicy:    "LRU",
		PrefetchSize:   4,
		PageSize:       16384,
		FillFactor:     0.8,
		MinFillFactor:  0.4,
		SplitThreshold: 0.9,
		MergeThreshold: 0.3,
		AsyncIO:        true,
		EnableStats:    config.EnableStatistics,
		StatsInterval:  config.StatsInterval,
		EnableLogging:  true,
		LogLevel:       "INFO",
	}

	// 使用增强版B+树管理器适配器
	im.btreeManager = NewEnhancedBTreeAdapter(storageManager, btreeConfig)

	return im
}

// EnsureSecondaryIndexes makes durable secondary-index metadata available for a table.
// The index entries themselves are stored in the table's durable B+Tree, so the metadata
// can be reconstructed from the persisted table definition after restart.
func (im *IndexManager) EnsureSecondaryIndexes(tableInfo *TableStorageInfo, tableMeta *metadata.TableMeta) error {
	if tableInfo == nil || tableMeta == nil {
		return fmt.Errorf("table storage info and metadata are required")
	}

	im.mu.Lock()
	defer im.mu.Unlock()
	tableID := SecondaryIndexTableID(tableInfo.SchemaName, tableInfo.TableName)
	for _, indexMeta := range tableMeta.Indices {
		if strings.EqualFold(indexMeta.Name, "PRIMARY") || len(indexMeta.Columns) == 0 {
			continue
		}
		indexID := SecondaryIndexID(tableID, indexMeta.Name)
		if existing := im.indexes[indexID]; existing != nil {
			continue
		}
		columns := make([]Column, len(indexMeta.Columns))
		for columnIndex, columnName := range indexMeta.Columns {
			columns[columnIndex] = Column{Name: columnName, Position: uint8(columnIndex)}
		}
		im.indexes[indexID] = &Index{
			IndexID:    indexID,
			TableID:    tableID,
			SpaceID:    tableInfo.SpaceID,
			Name:       indexMeta.Name,
			Type:       INDEX_TYPE_BTREE,
			Columns:    columns,
			IsUnique:   indexMeta.Unique,
			State:      IndexStateActive,
			CreateTime: time.Now(),
			UpdateTime: time.Now(),
			RootPageNo: tableInfo.RootPageNo,
			Height:     1,
			PageCount:  1,
			LeafPages:  1,
		}
	}
	return nil
}

func (im *IndexManager) secondaryIndexManager(index *Index) (basic.BPlusTreeManager, error) {
	if index == nil {
		return nil, ErrIndexNotFound
	}
	if im.storageManager == nil || im.storageManager.GetTableStorageManager() == nil {
		return im.requireBTreeManager()
	}
	tableStorageManager := im.storageManager.GetTableStorageManager()
	tableInfo, err := tableStorageManager.GetTableBySpaceID(index.SpaceID)
	if err != nil {
		return nil, err
	}
	return tableStorageManager.CreateBTreeManagerForTable(context.Background(), tableInfo.SchemaName, tableInfo.TableName)
}

// CreateIndex 创建新索引
func (im *IndexManager) CreateIndex(tableID uint64, spaceID uint32, name string, cols []Column, unique bool, primary bool) (*Index, error) {
	im.mu.Lock()
	defer im.mu.Unlock()

	// 检查索引数量限制
	if uint64(len(im.indexes)) >= im.config.MaxIndexes {
		return nil, fmt.Errorf("maximum number of indexes reached: %d", im.config.MaxIndexes)
	}
	btreeManager, err := im.requireBTreeManager()
	if err != nil {
		return nil, err
	}

	// 生成新的索引ID
	indexID := uint64(len(im.indexes) + 1)

	// 为索引创建段
	seg, err := im.segmentManager.CreateSegment(spaceID, SEGMENT_TYPE_INDEX, false)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment for index: %v", err)
	}

	// 从段中获取SegmentID
	segmentImpl, ok := seg.(*SegmentImpl)
	if !ok {
		return nil, fmt.Errorf("invalid segment type")
	}

	// 分配根页面
	rootPage, err := im.segmentManager.AllocatePage(segmentImpl.SegmentID)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate root page: %v", err)
	}

	// 初始化B+树
	ctx := context.Background()
	if err := btreeManager.Init(ctx, spaceID, rootPage); err != nil {
		return nil, fmt.Errorf("failed to initialize B+tree: %v", err)
	}

	// 创建索引对象
	idx := &Index{
		IndexID:      indexID,
		TableID:      tableID,
		SpaceID:      spaceID,
		Name:         name,
		Type:         INDEX_TYPE_BTREE,
		Columns:      cols,
		IsUnique:     unique,
		IsPrimary:    primary,
		SegmentID:    segmentImpl.SegmentID,
		RootPageNo:   rootPage,
		Height:       1,
		PageCount:    1,
		State:        IndexStateBuilding,
		CreateTime:   time.Now(),
		UpdateTime:   time.Now(),
		KeyCount:     0,
		LeafPages:    1,
		NonLeafPages: 0,
	}

	// 保存索引
	im.indexes[indexID] = idx

	// 更新统计
	im.updateStats(idx, true)

	// 标记为活跃状态
	idx.State = IndexStateActive
	idx.UpdateTime = time.Now()

	return idx, nil
}

// GetIndex 获取索引
func (im *IndexManager) GetIndex(indexID uint64) *Index {
	im.mu.RLock()
	defer im.mu.RUnlock()
	return im.indexes[indexID]
}

// GetIndexByName 根据名称获取索引
func (im *IndexManager) GetIndexByName(tableID uint64, name string) *Index {
	im.mu.RLock()
	defer im.mu.RUnlock()

	for _, idx := range im.indexes {
		if idx.TableID == tableID && idx.Name == name {
			return idx
		}
	}
	return nil
}

// ListIndexes 列出表的所有索引
func (im *IndexManager) ListIndexes(tableID uint64) []*Index {
	im.mu.RLock()
	defer im.mu.RUnlock()

	var indexes []*Index
	for _, idx := range im.indexes {
		if idx.TableID == tableID {
			indexes = append(indexes, idx)
		}
	}
	return indexes
}

// InsertKey 插入索引项
func (im *IndexManager) InsertKey(indexID uint64, key interface{}, value []byte) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	idx := im.indexes[indexID]
	if idx == nil {
		return ErrIndexNotFound
	}

	if idx.State != IndexStateActive {
		return fmt.Errorf("index %d is not active", indexID)
	}
	btreeManager, err := im.secondaryIndexManager(idx)
	if err != nil {
		return err
	}

	// 检查唯一性约束
	if idx.IsUnique {
		ctx := context.Background()
		_, _, err := btreeManager.Search(ctx, key)
		if err == nil {
			return basic.ErrDuplicateKey
		}
	}

	// 插入到B+树
	ctx := context.Background()
	if err := btreeManager.Insert(ctx, key, value); err != nil {
		return fmt.Errorf("failed to insert key: %v", err)
	}

	// 更新索引统计
	idx.KeyCount++
	idx.UpdateTime = time.Now()
	im.stats.InsertCount++

	return nil
}

// UpdateKey 更新索引项（先删除旧值，再插入新值）
func (im *IndexManager) UpdateKey(indexID uint64, oldKey, newKey interface{}, newValue []byte) error {
	// 先删除旧键
	if err := im.DeleteKey(indexID, oldKey); err != nil {
		return fmt.Errorf("delete old key failed: %v", err)
	}

	// 再插入新键
	if err := im.InsertKey(indexID, newKey, newValue); err != nil {
		return fmt.Errorf("insert new key failed: %v", err)
	}

	return nil
}

// DeleteKey 删除索引项
func (im *IndexManager) DeleteKey(indexID uint64, key interface{}) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	idx := im.indexes[indexID]
	if idx == nil {
		return ErrIndexNotFound
	}

	if idx.State != IndexStateActive {
		return fmt.Errorf("index %d is not active", indexID)
	}
	btreeManager, err := im.secondaryIndexManager(idx)
	if err != nil {
		return err
	}

	// 从B+树删除
	ctx := context.Background()

	// 如果底层B+Tree实现支持删除接口，则直接调用
	if deleter, ok := btreeManager.(interface {
		Delete(ctx context.Context, key interface{}) error
	}); ok {
		if err := deleter.Delete(ctx, key); err != nil {
			return fmt.Errorf("failed to delete key: %v", err)
		}
	} else {
		// 旧的B+Tree实现没有提供删除接口，退化为查找并忽略操作
		if _, _, err := btreeManager.Search(ctx, key); err != nil {
			return fmt.Errorf("key not found: %v", err)
		}
		// 无直接删除能力，只更新统计信息
	}

	// 更新索引统计
	if idx.KeyCount > 0 {
		idx.KeyCount--
	}
	idx.UpdateTime = time.Now()
	im.stats.DeleteCount++

	return nil
}

// SearchKey 查找索引项
func (im *IndexManager) SearchKey(indexID uint64, key interface{}) (pageNo uint32, slot int, err error) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	idx := im.indexes[indexID]
	if idx == nil {
		return 0, 0, ErrIndexNotFound
	}

	if idx.State != IndexStateActive {
		return 0, 0, fmt.Errorf("index %d is not active", indexID)
	}
	btreeManager, err := im.secondaryIndexManager(idx)
	if err != nil {
		return 0, 0, err
	}

	// 在B+树中查找
	ctx := context.Background()
	pageNo, slot, err = btreeManager.Search(ctx, key)
	if err != nil {
		return 0, 0, fmt.Errorf("search failed: %v", err)
	}

	// 更新统计
	im.stats.SearchCount++

	return pageNo, slot, nil
}

// RangeSearch 范围搜索
func (im *IndexManager) RangeSearch(indexID uint64, startKey, endKey interface{}) ([]basic.Row, error) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	idx := im.indexes[indexID]
	if idx == nil {
		return nil, ErrIndexNotFound
	}

	if idx.State != IndexStateActive {
		return nil, fmt.Errorf("index %d is not active", indexID)
	}
	btreeManager, err := im.secondaryIndexManager(idx)
	if err != nil {
		return nil, err
	}

	// 执行范围查询
	ctx := context.Background()
	rows, err := btreeManager.RangeSearch(ctx, startKey, endKey)
	if err != nil {
		return nil, fmt.Errorf("range search failed: %v", err)
	}

	// 更新统计
	im.stats.SearchCount++

	return rows, nil
}

// InitializeSecondaryIndex loads the durable leaf-page chain before its first
// query after metadata reconstruction. This avoids treating a not-yet-loaded
// reopened B+Tree as an empty secondary index.
func (im *IndexManager) InitializeSecondaryIndex(indexID uint64) error {
	im.mu.RLock()
	idx := im.indexes[indexID]
	if idx == nil {
		im.mu.RUnlock()
		return ErrIndexNotFound
	}
	if idx.State != IndexStateActive {
		im.mu.RUnlock()
		return fmt.Errorf("index %d is not active", indexID)
	}
	btreeManager, err := im.secondaryIndexManager(idx)
	im.mu.RUnlock()
	if err != nil {
		return err
	}
	if _, err := btreeManager.GetAllLeafPages(context.Background()); err != nil {
		return fmt.Errorf("load secondary index pages: %w", err)
	}
	return nil
}

// DropIndex 删除索引
func (im *IndexManager) DropIndex(indexID uint64) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	idx := im.indexes[indexID]
	if idx == nil {
		return ErrIndexNotFound
	}
	btreeManager, err := im.requireBTreeManager()
	if err != nil {
		return err
	}

	// 标记为删除状态
	idx.State = IndexStateDropping
	idx.UpdateTime = time.Now()

	// 清理B+树中的所有页面
	ctx := context.Background()

	leafPages, err := btreeManager.GetAllLeafPages(ctx)
	if err == nil {
		for _, pageNo := range leafPages {
			// 释放缓冲池及存储中的页面
			if err := im.bufferPoolManager.FreePage(idx.SpaceID, pageNo); err != nil {
				logger.Debugf("Warning: failed to free buffer page %d: %v", pageNo, err)
			}
			if err := im.segmentManager.FreePage(idx.SegmentID, pageNo); err != nil {
				logger.Debugf("Warning: failed to free segment page %d: %v", pageNo, err)
			}
		}
	}

	// 释放段中的所有页面
	for pageNo := uint32(0); pageNo < idx.PageCount; pageNo++ {
		if err := im.bufferPoolManager.FreePage(idx.SpaceID, pageNo); err != nil {
			logger.Debugf("Warning: failed to free buffer page %d: %v", pageNo, err)
		}

		if err := im.segmentManager.FreePage(idx.SegmentID, pageNo); err != nil {
			// 记录错误但继续处理
			logger.Debugf("Warning: failed to free page %d in segment %d: %v", pageNo, idx.SegmentID, err)
		}
	}

	// 更新统计
	im.updateStats(idx, false)

	// 从映射中删除
	delete(im.indexes, indexID)

	return nil
}

// RebuildIndex 重建索引
func (im *IndexManager) RebuildIndex(indexID uint64) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	idx := im.indexes[indexID]
	if idx == nil {
		return ErrIndexNotFound
	}
	if im.canRebuildDurableSecondaryIndex(idx) {
		return im.rebuildDurableSecondaryIndexLocked(context.Background(), idx)
	}
	btreeManager, err := im.requireBTreeManager()
	if err != nil {
		return err
	}

	// 标记为构建状态
	idx.State = IndexStateBuilding
	idx.UpdateTime = time.Now()

	// 清理旧索引的所有页面
	for pageNo := uint32(0); pageNo < idx.PageCount; pageNo++ {
		if err := im.bufferPoolManager.FreePage(idx.SpaceID, pageNo); err != nil {
			logger.Debugf("Warning: failed to free buffer page %d: %v", pageNo, err)
		}
		if err := im.segmentManager.FreePage(idx.SegmentID, pageNo); err != nil {
			logger.Debugf("Warning: failed to free page %d in segment %d: %v", pageNo, idx.SegmentID, err)
		}
	}

	// 重新分配根页并初始化B+树
	rootPage, err := im.segmentManager.AllocatePage(idx.SegmentID)
	if err != nil {
		return fmt.Errorf("failed to allocate root page: %v", err)
	}

	ctx := context.Background()
	if err := btreeManager.Init(ctx, idx.SpaceID, rootPage); err != nil {
		return fmt.Errorf("failed to init btree: %v", err)
	}

	// 重置索引元数据
	idx.RootPageNo = rootPage
	idx.Height = 1
	idx.PageCount = 1
	idx.KeyCount = 0
	idx.LeafPages = 1
	idx.NonLeafPages = 0

	// 标记为活跃状态
	idx.State = IndexStateActive
	idx.UpdateTime = time.Now()

	return nil
}

func (im *IndexManager) RepairIndex(indexID uint64) error {
	if err := im.RebuildIndex(indexID); err != nil {
		return fmt.Errorf("rebuild index %d failed during repair: %w", indexID, err)
	}
	if err := im.ValidateIndex(indexID); err != nil {
		return fmt.Errorf("validate index %d failed after repair: %w", indexID, err)
	}
	return nil
}

// GetIndexStats 获取索引统计信息
func (im *IndexManager) GetIndexStats(indexID uint64) (*IndexStatistics, error) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	idx := im.indexes[indexID]
	if idx == nil {
		return nil, ErrIndexNotFound
	}

	return &IndexStatistics{
		IndexID:      idx.IndexID,
		KeyCount:     idx.KeyCount,
		PageCount:    idx.PageCount,
		Height:       idx.Height,
		LeafPages:    idx.LeafPages,
		NonLeafPages: idx.NonLeafPages,
		State:        idx.State,
		CreateTime:   idx.CreateTime,
		UpdateTime:   idx.UpdateTime,
	}, nil
}

// IndexStatistics 索引统计信息
type IndexStatistics struct {
	IndexID      uint64
	KeyCount     uint64
	PageCount    uint32
	Height       uint8
	LeafPages    uint32
	NonLeafPages uint32
	State        IndexState
	CreateTime   time.Time
	UpdateTime   time.Time
}

// SyncSecondaryIndexesOnInsert 在INSERT时同步所有二级索引
func (im *IndexManager) SyncSecondaryIndexesOnInsert(tableID uint64, rowData map[string]interface{}, primaryKeyValue []byte) error {
	// 先获取二级索引列表（需要读锁）
	im.mu.RLock()
	secondaryIndexes := im.getSecondaryIndexesByTable(tableID)
	im.mu.RUnlock()

	if len(secondaryIndexes) == 0 {
		return nil // 没有二级索引，直接返回
	}

	// 为每个二级索引插入条目（InsertKey内部会加写锁）
	for _, idx := range secondaryIndexes {
		indexKey, err := EncodeSecondaryIndexKey(tableID, metadata.IndexMeta{
			Name:    idx.Name,
			Columns: indexColumnNames(idx),
			Unique:  idx.IsUnique,
		}, rowData, primaryKeyValue)
		if err != nil {
			return fmt.Errorf("encode index key for index %d failed: %v", idx.IndexID, err)
		}

		// 插入到二级索引（值为主键）
		if err := im.InsertKey(idx.IndexID, indexKey, EncodeSecondaryIndexValue(primaryKeyValue)); err != nil {
			return fmt.Errorf("insert to secondary index %d failed: %v", idx.IndexID, err)
		}
	}

	return nil
}

func indexColumnNames(index *Index) []string {
	columns := make([]string, len(index.Columns))
	for i, column := range index.Columns {
		columns[i] = column.Name
	}
	return columns
}

// SyncSecondaryIndexesOnUpdate 在UPDATE时同步所有二级索引
func (im *IndexManager) SyncSecondaryIndexesOnUpdate(tableID uint64, oldRowData, newRowData map[string]interface{}, primaryKeyValue []byte) error {
	// 先获取二级索引列表（需要读锁）
	im.mu.RLock()
	secondaryIndexes := im.getSecondaryIndexesByTable(tableID)
	im.mu.RUnlock()

	if len(secondaryIndexes) == 0 {
		return nil // 没有二级索引，直接返回
	}

	// 为每个二级索引更新条目（UpdateKey内部会加写锁）
	for _, idx := range secondaryIndexes {
		// 检查索引列是否被更新
		if !im.isIndexAffected(idx, oldRowData, newRowData) {
			continue // 索引列未变化，跳过
		}

		indexMeta := metadata.IndexMeta{
			Name:    idx.Name,
			Columns: indexColumnNames(idx),
			Unique:  idx.IsUnique,
		}
		oldIndexKey, err := EncodeSecondaryIndexKey(tableID, indexMeta, oldRowData, primaryKeyValue)
		if err != nil {
			return fmt.Errorf("encode old index key for index %d failed: %v", idx.IndexID, err)
		}

		newIndexKey, err := EncodeSecondaryIndexKey(tableID, indexMeta, newRowData, primaryKeyValue)
		if err != nil {
			return fmt.Errorf("encode new index key for index %d failed: %v", idx.IndexID, err)
		}

		// 更新二级索引
		if err := im.UpdateKey(idx.IndexID, oldIndexKey, newIndexKey, EncodeSecondaryIndexValue(primaryKeyValue)); err != nil {
			return fmt.Errorf("update secondary index %d failed: %v", idx.IndexID, err)
		}
	}

	return nil
}

// SyncSecondaryIndexesOnDelete 在DELETE时同步所有二级索引
func (im *IndexManager) SyncSecondaryIndexesOnDelete(tableID uint64, rowData map[string]interface{}, primaryKeyValue []byte) error {
	// 先获取二级索引列表（需要读锁）
	im.mu.RLock()
	secondaryIndexes := im.getSecondaryIndexesByTable(tableID)
	im.mu.RUnlock()

	if len(secondaryIndexes) == 0 {
		return nil // 没有二级索引，直接返回
	}

	// 为每个二级索引删除条目（DeleteKey内部会加写锁）
	for _, idx := range secondaryIndexes {
		indexKey, err := EncodeSecondaryIndexKey(tableID, metadata.IndexMeta{
			Name:    idx.Name,
			Columns: indexColumnNames(idx),
			Unique:  idx.IsUnique,
		}, rowData, primaryKeyValue)
		if err != nil {
			return fmt.Errorf("encode index key for index %d failed: %v", idx.IndexID, err)
		}

		// 从二级索引删除
		if err := im.DeleteKey(idx.IndexID, indexKey); err != nil {
			return fmt.Errorf("delete from secondary index %d failed: %v", idx.IndexID, err)
		}
	}

	return nil
}

// getSecondaryIndexesByTable 获取表的所有二级索引（非主键索引）
func (im *IndexManager) getSecondaryIndexesByTable(tableID uint64) []*Index {
	var secondaryIndexes []*Index

	for _, idx := range im.indexes {
		if idx.TableID == tableID && !idx.IsPrimary && idx.State == IndexStateActive {
			secondaryIndexes = append(secondaryIndexes, idx)
		}
	}

	return secondaryIndexes
}

// extractIndexKey 从行数据中提取索引键值
func (im *IndexManager) extractIndexKey(idx *Index, rowData map[string]interface{}) (interface{}, error) {
	if len(idx.Columns) == 0 {
		return nil, fmt.Errorf("index %d has no columns", idx.IndexID)
	}

	// 单列索引：直接返回列值
	if len(idx.Columns) == 1 {
		colName := idx.Columns[0].Name
		value, exists := rowData[colName]
		if !exists {
			return nil, fmt.Errorf("column %s not found in row data", colName)
		}
		return value, nil
	}

	// 复合索引：拼接多列值
	var keyParts []interface{}
	for _, col := range idx.Columns {
		value, exists := rowData[col.Name]
		if !exists {
			return nil, fmt.Errorf("column %s not found in row data", col.Name)
		}
		keyParts = append(keyParts, value)
	}

	return keyParts, nil
}

// isIndexAffected 检查索引列是否被更新
func (im *IndexManager) isIndexAffected(idx *Index, oldRowData, newRowData map[string]interface{}) bool {
	for _, col := range idx.Columns {
		oldValue, oldExists := oldRowData[col.Name]
		newValue, newExists := newRowData[col.Name]

		// 如果列存在性变化，或值变化，则索引受影响
		if oldExists != newExists {
			return true
		}

		if oldExists && newExists && oldValue != newValue {
			return true
		}
	}

	return false
}

// GetManagerStats 获取管理器统计信息
func (im *IndexManager) GetManagerStats() *IndexManagerStats {
	im.mu.RLock()
	defer im.mu.RUnlock()

	// 复制统计信息
	stats := *im.stats
	return &stats
}

// updateStats 更新统计信息
func (im *IndexManager) updateStats(idx *Index, isCreate bool) {
	if isCreate {
		im.stats.TotalIndexes++
		if idx.State == IndexStateActive {
			im.stats.ActiveIndexes++
		}
		if idx.IsPrimary {
			im.stats.PrimaryIndexes++
		}
		if idx.IsUnique {
			im.stats.UniqueIndexes++
		}
	} else {
		if im.stats.TotalIndexes > 0 {
			im.stats.TotalIndexes--
		}
		if idx.State == IndexStateActive && im.stats.ActiveIndexes > 0 {
			im.stats.ActiveIndexes--
		}
		if idx.IsPrimary && im.stats.PrimaryIndexes > 0 {
			im.stats.PrimaryIndexes--
		}
		if idx.IsUnique && im.stats.UniqueIndexes > 0 {
			im.stats.UniqueIndexes--
		}
	}
}

func (im *IndexManager) requireBTreeManager() (basic.BPlusTreeManager, error) {
	if im == nil || im.btreeManager == nil {
		return nil, ErrBTreeManagerUnavailable
	}
	return im.btreeManager, nil
}

// FlushIndexes 刷新所有索引到磁盘
func (im *IndexManager) FlushIndexes() error {
	im.mu.RLock()
	defer im.mu.RUnlock()

	// 刷新缓冲池中的脏页
	if err := im.bufferPoolManager.FlushAllPages(); err != nil {
		return fmt.Errorf("failed to flush dirty pages: %v", err)
	}

	return nil
}

// Close 关闭索引管理器
func (im *IndexManager) Close() error {
	im.mu.Lock()
	defer im.mu.Unlock()

	// 刷新所有索引
	if err := im.FlushIndexes(); err != nil {
		return fmt.Errorf("failed to flush indexes during close: %v", err)
	}

	// 清理资源
	im.indexes = nil
	im.btreeManager = nil

	return nil
}

// ValidateIndex 验证索引的完整性
func (im *IndexManager) ValidateIndex(indexID uint64) error {
	im.mu.RLock()
	defer im.mu.RUnlock()

	idx := im.indexes[indexID]
	if idx == nil {
		return ErrIndexNotFound
	}
	if idx.Type != INDEX_TYPE_BTREE {
		return fmt.Errorf("index %d has unsupported type %d", indexID, idx.Type)
	}
	if idx.State != IndexStateActive {
		return fmt.Errorf("index %d is not active", indexID)
	}
	if idx.RootPageNo == 0 {
		return fmt.Errorf("index %d has no root page", indexID)
	}
	if idx.PageCount == 0 {
		return fmt.Errorf("index %d has zero pages", indexID)
	}
	if len(idx.Columns) == 0 {
		return fmt.Errorf("index %d has no columns", indexID)
	}
	btreeManager, err := im.btreeManagerForIndexValidation(idx)
	if err != nil {
		return err
	}

	ctx := context.Background()
	leafPages, err := btreeManager.GetAllLeafPages(ctx)
	if err != nil {
		return fmt.Errorf("get leaf pages failed: %v", err)
	}
	if len(leafPages) == 0 {
		return fmt.Errorf("index %d has no leaf pages", indexID)
	}
	seen := make(map[uint32]struct{}, len(leafPages))
	for _, pageNo := range leafPages {
		if pageNo == 0 {
			return fmt.Errorf("index %d has invalid leaf page 0", indexID)
		}
		if _, exists := seen[pageNo]; exists {
			return fmt.Errorf("index %d has duplicate leaf page %d", indexID, pageNo)
		}
		seen[pageNo] = struct{}{}
	}
	firstLeaf, err := btreeManager.GetFirstLeafPage(ctx)
	if err != nil {
		return fmt.Errorf("get first leaf page failed: %v", err)
	}
	if _, exists := seen[firstLeaf]; !exists {
		return fmt.Errorf("index %d first leaf page %d is not in leaf chain", indexID, firstLeaf)
	}
	if idx.LeafPages > 0 && idx.LeafPages != uint32(len(leafPages)) {
		return fmt.Errorf("index %d leaf page count mismatch: metadata=%d actual=%d", indexID, idx.LeafPages, len(leafPages))
	}
	if idx.PageCount < uint32(len(leafPages)) {
		return fmt.Errorf("index %d page count %d is smaller than leaf page count %d", indexID, idx.PageCount, len(leafPages))
	}
	if im.canRebuildDurableSecondaryIndex(idx) {
		if err := im.validateDurableSecondaryIndexLocked(ctx, idx, btreeManager); err != nil {
			return err
		}
	}

	return nil
}

func (im *IndexManager) btreeManagerForIndexValidation(index *Index) (basic.BPlusTreeManager, error) {
	if im.canRebuildDurableSecondaryIndex(index) {
		return im.secondaryIndexManager(index)
	}
	return im.requireBTreeManager()
}

func (im *IndexManager) canRebuildDurableSecondaryIndex(index *Index) bool {
	return im != nil &&
		index != nil &&
		!index.IsPrimary &&
		im.storageManager != nil &&
		im.storageManager.GetTableStorageManager() != nil &&
		im.storageManager.GetTableManager() != nil
}

func (im *IndexManager) rebuildDurableSecondaryIndexLocked(ctx context.Context, index *Index) error {
	btreeManager, expectedEntries, actualEntries, tableInfo, err := im.secondaryIndexRepairSnapshot(ctx, index)
	if err != nil {
		return err
	}

	index.State = IndexStateBuilding
	index.UpdateTime = time.Now()

	for _, entry := range actualEntries {
		if err := btreeManager.Delete(ctx, entry.Key); err != nil {
			return fmt.Errorf("delete stale secondary index key failed: %v", err)
		}
	}
	for _, entry := range expectedEntries {
		if err := btreeManager.Insert(ctx, entry.Key, entry.Value); err != nil {
			return fmt.Errorf("insert rebuilt secondary index key failed: %v", err)
		}
	}

	leafPages, err := btreeManager.GetAllLeafPages(ctx)
	if err != nil {
		return fmt.Errorf("get rebuilt leaf pages failed: %v", err)
	}
	if len(leafPages) == 0 {
		return fmt.Errorf("rebuilt index %d has no leaf pages", index.IndexID)
	}

	index.RootPageNo = tableInfo.RootPageNo
	index.Height = 1
	index.KeyCount = uint64(len(expectedEntries))
	index.LeafPages = uint32(len(leafPages))
	index.NonLeafPages = 0
	index.PageCount = index.LeafPages
	index.State = IndexStateActive
	index.UpdateTime = time.Now()
	return nil
}

func (im *IndexManager) validateDurableSecondaryIndexLocked(ctx context.Context, index *Index, btreeManager basic.BPlusTreeManager) error {
	expectedEntries, err := im.expectedSecondaryIndexEntries(ctx, index)
	if err != nil {
		return err
	}
	actualEntries, err := im.actualSecondaryIndexEntries(ctx, index, btreeManager)
	if err != nil {
		return err
	}

	expected := secondaryIndexEntryMap(expectedEntries)
	actual := secondaryIndexEntryMap(actualEntries)

	for key, expectedEntry := range expected {
		actualEntry, exists := actual[key]
		if !exists {
			return fmt.Errorf("index %d missing secondary entry for key %x", index.IndexID, expectedEntry.Key)
		}
		if !bytes.Equal(actualEntry.Value, expectedEntry.Value) {
			return fmt.Errorf("index %d secondary entry value mismatch for key %x", index.IndexID, expectedEntry.Key)
		}
		if _, err := DecodeSecondaryIndexValue(actualEntry.Value); err != nil {
			return fmt.Errorf("index %d has invalid secondary value for key %x: %v", index.IndexID, actualEntry.Key, err)
		}
	}
	for key, actualEntry := range actual {
		if _, exists := expected[key]; !exists {
			return fmt.Errorf("index %d has stale secondary entry for key %x", index.IndexID, actualEntry.Key)
		}
		if _, err := DecodeSecondaryIndexValue(actualEntry.Value); err != nil {
			return fmt.Errorf("index %d has invalid stale secondary value for key %x: %v", index.IndexID, actualEntry.Key, err)
		}
	}
	if index.KeyCount != uint64(len(expectedEntries)) {
		return fmt.Errorf("index %d key count mismatch: metadata=%d actual=%d", index.IndexID, index.KeyCount, len(expectedEntries))
	}
	return nil
}

func (im *IndexManager) secondaryIndexRepairSnapshot(ctx context.Context, index *Index) (basic.BPlusTreeManager, []SecondaryIndexEntry, []SecondaryIndexEntry, *TableStorageInfo, error) {
	tableInfo, tableMeta, btreeManager, err := im.secondaryIndexTableContext(ctx, index)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	rows, err := scanClusteredRowsForSecondaryIndexRepair(ctx, btreeManager, tableMeta)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	expectedEntries, err := buildSecondaryIndexEntriesFromRepairRows(index.TableID, tableMeta, rows, index)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	actualEntries, err := im.actualSecondaryIndexEntries(ctx, index, btreeManager)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return btreeManager, expectedEntries, actualEntries, tableInfo, nil
}

func (im *IndexManager) expectedSecondaryIndexEntries(ctx context.Context, index *Index) ([]SecondaryIndexEntry, error) {
	_, tableMeta, btreeManager, err := im.secondaryIndexTableContext(ctx, index)
	if err != nil {
		return nil, err
	}
	rows, err := scanClusteredRowsForSecondaryIndexRepair(ctx, btreeManager, tableMeta)
	if err != nil {
		return nil, err
	}
	return buildSecondaryIndexEntriesFromRepairRows(index.TableID, tableMeta, rows, index)
}

func (im *IndexManager) secondaryIndexTableContext(ctx context.Context, index *Index) (*TableStorageInfo, *metadata.TableMeta, basic.BPlusTreeManager, error) {
	if !im.canRebuildDurableSecondaryIndex(index) {
		return nil, nil, nil, fmt.Errorf("index %d does not have durable secondary index storage context", index.IndexID)
	}

	tableStorageManager := im.storageManager.GetTableStorageManager()
	tableInfo, err := tableStorageManager.GetTableBySpaceID(index.SpaceID)
	if err != nil {
		return nil, nil, nil, err
	}
	tableMeta, err := im.storageManager.GetTableManager().GetTableMetadata(ctx, tableInfo.SchemaName, tableInfo.TableName)
	if err != nil {
		return nil, nil, nil, err
	}
	btreeManager, err := tableStorageManager.CreateBTreeManagerForTable(ctx, tableInfo.SchemaName, tableInfo.TableName)
	if err != nil {
		return nil, nil, nil, err
	}
	return tableInfo, tableMeta, btreeManager, nil
}

func (im *IndexManager) actualSecondaryIndexEntries(ctx context.Context, index *Index, btreeManager basic.BPlusTreeManager) ([]SecondaryIndexEntry, error) {
	startKey, endKey, err := SecondaryIndexFullRange(index.TableID, metadata.IndexMeta{
		Name:    index.Name,
		Columns: indexColumnNames(index),
		Unique:  index.IsUnique,
	})
	if err != nil {
		return nil, err
	}
	rows, err := btreeManager.RangeSearch(ctx, startKey, endKey)
	if err != nil {
		return nil, fmt.Errorf("scan secondary index range failed: %v", err)
	}

	entries := make([]SecondaryIndexEntry, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		key, err := secondaryIndexKeyFromRow(row)
		if err != nil {
			return nil, err
		}
		entries = append(entries, SecondaryIndexEntry{
			Key:   key,
			Value: append([]byte(nil), row.ToByte()...),
		})
	}
	return entries, nil
}

func secondaryIndexKeyFromRow(row basic.Row) ([]byte, error) {
	primaryKey := row.GetPrimaryKey()
	if primaryKey == nil || primaryKey.IsNull() {
		return nil, fmt.Errorf("secondary index row has no key")
	}
	switch raw := primaryKey.Raw().(type) {
	case []byte:
		return append([]byte(nil), raw...), nil
	case string:
		return []byte(raw), nil
	default:
		return []byte(fmt.Sprintf("%v", raw)), nil
	}
}

func secondaryIndexEntryMap(entries []SecondaryIndexEntry) map[string]SecondaryIndexEntry {
	result := make(map[string]SecondaryIndexEntry, len(entries))
	for _, entry := range entries {
		result[string(entry.Key)] = entry
	}
	return result
}

func BuildSecondaryIndexEntries(tableID uint64, tableMeta *metadata.TableMeta, rows []map[string]interface{}, index *Index) ([]SecondaryIndexEntry, error) {
	repairRows := make([]secondaryIndexRepairRow, 0, len(rows))
	for _, row := range rows {
		repairRows = append(repairRows, secondaryIndexRepairRow{Values: row})
	}
	return buildSecondaryIndexEntriesFromRepairRows(tableID, tableMeta, repairRows, index)
}

func buildSecondaryIndexEntriesFromRepairRows(tableID uint64, tableMeta *metadata.TableMeta, rows []secondaryIndexRepairRow, index *Index) ([]SecondaryIndexEntry, error) {
	if tableMeta == nil {
		return nil, fmt.Errorf("table metadata is nil")
	}
	if index == nil {
		return nil, fmt.Errorf("index is nil")
	}
	indexMeta := metadata.IndexMeta{
		Name:    index.Name,
		Columns: indexColumnNames(index),
		Unique:  index.IsUnique,
	}
	entries := make([]SecondaryIndexEntry, 0, len(rows))
	for rowIndex, row := range rows {
		primaryKey, err := buildSecondaryPrimaryKey(row.Values, tableMeta)
		if err != nil {
			if len(row.PrimaryKey) == 0 {
				return nil, fmt.Errorf("build primary key for row %d failed: %v", rowIndex, err)
			}
			primaryKey = append([]byte(nil), row.PrimaryKey...)
		}
		key, err := EncodeSecondaryIndexKey(tableID, indexMeta, row.Values, primaryKey)
		if err != nil {
			return nil, fmt.Errorf("encode secondary key for row %d failed: %v", rowIndex, err)
		}
		entries = append(entries, SecondaryIndexEntry{
			Key:   key,
			Value: EncodeSecondaryIndexValue(primaryKey),
		})
	}
	return entries, nil
}

func buildSecondaryPrimaryKey(row map[string]interface{}, tableMeta *metadata.TableMeta) ([]byte, error) {
	if tableMeta == nil {
		return nil, fmt.Errorf("table metadata is nil")
	}
	if len(tableMeta.PrimaryKey) == 0 {
		return nil, fmt.Errorf("table %s has no primary key for secondary index rebuild", tableMeta.Name)
	}
	parts := make([][]byte, 0, len(tableMeta.PrimaryKey))
	for _, columnName := range tableMeta.PrimaryKey {
		value, exists := row[columnName]
		if !exists || value == nil {
			return nil, fmt.Errorf("missing primary key column '%s'", columnName)
		}
		part := []byte(fmt.Sprintf("%v", value))
		prefixed := make([]byte, 4, 4+len(part))
		binary.BigEndian.PutUint32(prefixed, uint32(len(part)))
		prefixed = append(prefixed, part...)
		parts = append(parts, prefixed)
	}
	return bytes.Join(parts, nil), nil
}

type indexRepairFullScanner interface {
	FullScan(ctx context.Context) ([]basic.Row, error)
}

func scanClusteredRowsForSecondaryIndexRepair(ctx context.Context, btreeManager basic.BPlusTreeManager, tableMeta *metadata.TableMeta) ([]secondaryIndexRepairRow, error) {
	fullScanner, ok := btreeManager.(indexRepairFullScanner)
	if !ok {
		return nil, fmt.Errorf("B+Tree manager does not support clustered full scan for secondary index rebuild")
	}
	storedRows, err := fullScanner.FullScan(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan clustered index failed: %v", err)
	}
	rows := make([]secondaryIndexRepairRow, 0, len(storedRows))
	for _, storedRow := range storedRows {
		if storedRow == nil {
			continue
		}
		rowData, ok, err := decodeClusteredRecordForIndexRepair(storedRow.ToByte(), tableMeta)
		if err != nil {
			return nil, err
		}
		if ok {
			repairRow := secondaryIndexRepairRow{Values: rowData}
			if keyBytes, ok := secondaryRepairStorageKey(storedRow); ok {
				repairRow.PrimaryKey = keyBytes
			}
			rows = append(rows, repairRow)
		}
	}
	return rows, nil
}

func secondaryRepairStorageKey(row basic.Row) ([]byte, bool) {
	if row == nil || row.GetPrimaryKey() == nil || row.GetPrimaryKey().IsNull() {
		return nil, false
	}
	switch raw := row.GetPrimaryKey().Raw().(type) {
	case []byte:
		if len(raw) == 0 {
			return nil, false
		}
		return append([]byte(nil), raw...), true
	case string:
		if raw == "" {
			return nil, false
		}
		return []byte(raw), true
	default:
		text := fmt.Sprintf("%v", raw)
		if text == "" {
			return nil, false
		}
		return []byte(text), true
	}
}

func decodeClusteredRecordForIndexRepair(data []byte, tableMeta *metadata.TableMeta) (map[string]interface{}, bool, error) {
	const magic = "XIR1"
	if tableMeta == nil {
		return nil, false, fmt.Errorf("table metadata is nil")
	}
	if !strings.HasPrefix(string(data), magic) {
		return nil, false, nil
	}
	if len(data) < len(magic)+1+2 {
		return nil, false, fmt.Errorf("clustered record payload too short")
	}

	offset := len(magic)
	if data[offset] != 1 {
		return nil, false, fmt.Errorf("unsupported clustered record version: %d", data[offset])
	}
	offset++
	columnCount := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2
	if int(columnCount) != len(tableMeta.Columns) {
		return nil, false, fmt.Errorf("clustered record column count %d does not match metadata column count %d", columnCount, len(tableMeta.Columns))
	}

	row := make(map[string]interface{}, len(tableMeta.Columns))
	for idx, col := range tableMeta.Columns {
		if col == nil {
			return nil, false, fmt.Errorf("column %d metadata is nil", idx)
		}
		if offset+1+4 > len(data) {
			return nil, false, fmt.Errorf("column %s header truncated", col.Name)
		}
		valueType := data[offset]
		offset++
		valueLen := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		if valueLen > uint32(len(data)-offset) {
			return nil, false, fmt.Errorf("column %s value truncated", col.Name)
		}
		value, err := decodeClusteredRepairValue(valueType, data[offset:offset+int(valueLen)])
		if err != nil {
			return nil, false, fmt.Errorf("decode column %s: %v", col.Name, err)
		}
		row[col.Name] = value
		offset += int(valueLen)
	}
	if offset != len(data) {
		return nil, false, fmt.Errorf("clustered record has %d trailing bytes", len(data)-offset)
	}
	return row, true, nil
}

func decodeClusteredRepairValue(valueType byte, data []byte) (interface{}, error) {
	switch valueType {
	case 0:
		return nil, nil
	case 1:
		if len(data) != 8 {
			return nil, fmt.Errorf("integer payload length = %d", len(data))
		}
		return int64(binary.BigEndian.Uint64(data)), nil
	case 2:
		if len(data) != 8 {
			return nil, fmt.Errorf("unsigned integer payload length = %d", len(data))
		}
		return binary.BigEndian.Uint64(data), nil
	case 3:
		if len(data) != 8 {
			return nil, fmt.Errorf("float payload length = %d", len(data))
		}
		return math.Float64frombits(binary.BigEndian.Uint64(data)), nil
	case 4:
		if len(data) != 1 {
			return nil, fmt.Errorf("boolean payload length = %d", len(data))
		}
		switch data[0] {
		case 0:
			return false, nil
		case 1:
			return true, nil
		default:
			return nil, fmt.Errorf("invalid boolean value: %d", data[0])
		}
	case 5:
		return string(data), nil
	case 6:
		return append([]byte(nil), data...), nil
	default:
		return nil, fmt.Errorf("unknown clustered value type %d", valueType)
	}
}

// CompactIndex 压缩索引，减少碎片
func (im *IndexManager) CompactIndex(indexID uint64) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	idx := im.indexes[indexID]
	if idx == nil {
		return ErrIndexNotFound
	}
	btreeManager, err := im.requireBTreeManager()
	if err != nil {
		return err
	}
	if idx.State != IndexStateActive {
		return fmt.Errorf("index %d is not active", indexID)
	}
	if idx.RootPageNo == 0 {
		return fmt.Errorf("index %d has no root page", indexID)
	}

	leafPages, err := btreeManager.GetAllLeafPages(context.Background())
	if err != nil {
		return fmt.Errorf("get leaf pages failed: %v", err)
	}
	if len(leafPages) == 0 {
		return fmt.Errorf("index %d has no leaf pages", indexID)
	}

	idx.LeafPages = uint32(len(leafPages))
	if idx.Height <= 1 {
		idx.NonLeafPages = 0
	} else {
		idx.NonLeafPages = uint32(idx.Height - 1)
	}
	idx.PageCount = idx.LeafPages + idx.NonLeafPages

	idx.UpdateTime = time.Now()
	return nil
}
