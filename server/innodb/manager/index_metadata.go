package manager

import (
	"fmt"
	"sync"
	"time"
)

// IndexType 索引类型
type IndexType uint8

const (
	IndexTypePrimary   IndexType = iota // 主键索引
	IndexTypeUnique                     // 唯一索引
	IndexTypeSecondary                  // 二级索引
	IndexTypeFullText                   // 全文索引
	IndexTypeSpatial                    // 空间索引
)

// EnhancedIndexState 增强版索引状态
type EnhancedIndexState uint8

const (
	EnhancedIndexStateBuilding  EnhancedIndexState = iota // 构建中
	EnhancedIndexStateActive                              // 活跃状态
	EnhancedIndexStateDisabled                            // 已禁用
	EnhancedIndexStateDropping                            // 删除中
	EnhancedIndexStateCorrupted                           // 损坏状态
)

// IndexColumn 索引列信息
type IndexColumn struct {
	ColumnName string // 列名
	ColumnPos  uint16 // 列在表中的位置
	KeyLength  uint16 // 键长度
	IsDesc     bool   // 是否降序
}

// IndexMetadata 索引元信息
type IndexMetadata struct {
	// 基本信息
	IndexID    uint64             // 索引ID
	TableID    uint64             // 表ID
	SpaceID    uint32             // 表空间ID
	IndexName  string             // 索引名称
	IndexType  IndexType          // 索引类型
	IndexState EnhancedIndexState // 索引状态

	// B+Tree结构信息
	RootPageNo  uint32 // 根页号
	Height      uint8  // 树高度
	PageCount   uint64 // 页面数量
	RecordCount uint64 // 记录数量

	// 索引列信息
	Columns   []IndexColumn // 索引列
	KeyLength uint16        // 索引键总长度

	// 统计信息
	CreateTime  time.Time // 创建时间
	UpdateTime  time.Time // 更新时间
	LastAccess  time.Time // 最后访问时间
	AccessCount uint64    // 访问次数

	// 内存管理
	IsLoaded  bool      // 是否已加载到内存
	RefCount  int32     // 引用计数
	LastFlush time.Time // 最后刷新时间
}

// EnhancedIndexStatistics 增强版索引统计信息
type EnhancedIndexStatistics struct {
	Cardinality  uint64    // 基数（不同值的数量）
	NullCount    uint64    // NULL值数量
	AvgKeyLength float64   // 平均键长度
	LeafPages    uint32    // 叶子页数
	NonLeafPages uint32    // 非叶子页数
	SplitCount   uint64    // 页分裂次数
	MergeCount   uint64    // 页合并次数
	LastAnalyze  time.Time // 最后分析时间
}

// IndexMetadataManager 索引元信息管理器
type IndexMetadataManager struct {
	mu sync.RWMutex

	// 索引元信息映射
	// key: indexID, value: IndexMetadata
	indexMetadata map[uint64]*IndexMetadata

	// 表到索引的映射
	// key: tableID, value: []indexID
	tableIndexes map[uint64][]uint64

	// 索引名称到ID的映射
	// key: "tableID.indexName", value: indexID
	nameToID map[string]uint64

	// 索引统计信息
	// key: indexID, value: EnhancedIndexStatistics
	statistics map[uint64]*EnhancedIndexStatistics

	// 下一个可用的索引ID
	nextIndexID uint64
}

// NewIndexMetadataManager 创建索引元信息管理器
func NewIndexMetadataManager() *IndexMetadataManager {
	return &IndexMetadataManager{
		indexMetadata: make(map[uint64]*IndexMetadata),
		tableIndexes:  make(map[uint64][]uint64),
		nameToID:      make(map[string]uint64),
		statistics:    make(map[uint64]*EnhancedIndexStatistics),
		nextIndexID:   1,
	}
}

// RegisterIndex 注册索引元信息
func (imm *IndexMetadataManager) RegisterIndex(metadata *IndexMetadata) error {
	imm.mu.Lock()
	defer imm.mu.Unlock()

	// 分配索引ID
	if metadata.IndexID == 0 {
		metadata.IndexID = imm.nextIndexID
		imm.nextIndexID++
	}

	// 检查索引名称是否冲突
	nameKey := fmt.Sprintf("%d.%s", metadata.TableID, metadata.IndexName)
	if _, exists := imm.nameToID[nameKey]; exists {
		return fmt.Errorf("index name '%s' already exists in table %d", metadata.IndexName, metadata.TableID)
	}

	// 注册索引元信息
	imm.indexMetadata[metadata.IndexID] = metadata
	imm.nameToID[nameKey] = metadata.IndexID

	// 更新表到索引的映射
	if indexes, exists := imm.tableIndexes[metadata.TableID]; exists {
		imm.tableIndexes[metadata.TableID] = append(indexes, metadata.IndexID)
	} else {
		imm.tableIndexes[metadata.TableID] = []uint64{metadata.IndexID}
	}

	// 初始化统计信息
	imm.statistics[metadata.IndexID] = &EnhancedIndexStatistics{
		LastAnalyze: time.Now(),
	}

	return nil
}

// GetIndexMetadata 获取索引元信息
func (imm *IndexMetadataManager) GetIndexMetadata(indexID uint64) (*IndexMetadata, error) {
	imm.mu.RLock()
	defer imm.mu.RUnlock()

	metadata, exists := imm.indexMetadata[indexID]
	if !exists {
		return nil, fmt.Errorf("index %d not found", indexID)
	}

	// 更新访问统计
	metadata.LastAccess = time.Now()
	metadata.AccessCount++

	return metadata, nil
}

// GetIndexByName 根据名称获取索引元信息
func (imm *IndexMetadataManager) GetIndexByName(tableID uint64, indexName string) (*IndexMetadata, error) {
	imm.mu.RLock()
	defer imm.mu.RUnlock()

	nameKey := fmt.Sprintf("%d.%s", tableID, indexName)
	indexID, exists := imm.nameToID[nameKey]
	if !exists {
		return nil, fmt.Errorf("index '%s' not found in table %d", indexName, tableID)
	}

	return imm.indexMetadata[indexID], nil
}

// GetTableIndexes 获取表的所有索引
func (imm *IndexMetadataManager) GetTableIndexes(tableID uint64) ([]*IndexMetadata, error) {
	imm.mu.RLock()
	defer imm.mu.RUnlock()

	indexIDs, exists := imm.tableIndexes[tableID]
	if !exists {
		return []*IndexMetadata{}, nil
	}

	var indexes []*IndexMetadata
	for _, indexID := range indexIDs {
		if metadata, exists := imm.indexMetadata[indexID]; exists {
			indexes = append(indexes, metadata)
		}
	}

	return indexes, nil
}

// UpdateIndexStatistics 更新索引统计信息
func (imm *IndexMetadataManager) UpdateIndexStatistics(indexID uint64, stats *EnhancedIndexStatistics) error {
	imm.mu.Lock()
	defer imm.mu.Unlock()

	if _, exists := imm.indexMetadata[indexID]; !exists {
		return fmt.Errorf("index %d not found", indexID)
	}

	stats.LastAnalyze = time.Now()
	imm.statistics[indexID] = stats

	return nil
}

// GetIndexStatistics 获取索引统计信息
func (imm *IndexMetadataManager) GetIndexStatistics(indexID uint64) (*EnhancedIndexStatistics, error) {
	imm.mu.RLock()
	defer imm.mu.RUnlock()

	stats, exists := imm.statistics[indexID]
	if !exists {
		return nil, fmt.Errorf("statistics for index %d not found", indexID)
	}

	return stats, nil
}

// RemoveIndex 移除索引元信息
func (imm *IndexMetadataManager) RemoveIndex(indexID uint64) error {
	imm.mu.Lock()
	defer imm.mu.Unlock()

	metadata, exists := imm.indexMetadata[indexID]
	if !exists {
		return fmt.Errorf("index %d not found", indexID)
	}

	// 移除索引元信息
	delete(imm.indexMetadata, indexID)
	delete(imm.statistics, indexID)

	// 移除名称映射
	nameKey := fmt.Sprintf("%d.%s", metadata.TableID, metadata.IndexName)
	delete(imm.nameToID, nameKey)

	// 更新表到索引的映射
	if indexes, exists := imm.tableIndexes[metadata.TableID]; exists {
		var newIndexes []uint64
		for _, id := range indexes {
			if id != indexID {
				newIndexes = append(newIndexes, id)
			}
		}
		if len(newIndexes) > 0 {
			imm.tableIndexes[metadata.TableID] = newIndexes
		} else {
			delete(imm.tableIndexes, metadata.TableID)
		}
	}

	return nil
}

// ListAllIndexes 列出所有索引
func (imm *IndexMetadataManager) ListAllIndexes() []*IndexMetadata {
	imm.mu.RLock()
	defer imm.mu.RUnlock()

	var indexes []*IndexMetadata
	for _, metadata := range imm.indexMetadata {
		indexes = append(indexes, metadata)
	}

	return indexes
}
