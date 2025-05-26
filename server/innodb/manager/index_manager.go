package manager

import (
	"context"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"sync"
	"time"
)

// IndexManager 管理表的索引
type IndexManager struct {
	mu sync.RWMutex

	// 索引映射: index_id -> index
	indexes map[uint64]*Index

	// 段管理器
	segmentManager *SegmentManager

	// B+树管理器
	btreeManager *DefaultBPlusTreeManager

	// 缓冲池管理器
	bufferPoolManager *OptimizedBufferPoolManager

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

	// 创建B+树管理器
	im.btreeManager = NewBPlusTreeManager(bufferPoolManager, nil)

	return im
}

// CreateIndex 创建新索引
func (im *IndexManager) CreateIndex(tableID uint64, spaceID uint32, name string, cols []Column, unique bool, primary bool) (*Index, error) {
	im.mu.Lock()
	defer im.mu.Unlock()

	// 检查索引数量限制
	if uint64(len(im.indexes)) >= im.config.MaxIndexes {
		return nil, fmt.Errorf("maximum number of indexes reached: %d", im.config.MaxIndexes)
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
	if err := im.btreeManager.Init(ctx, spaceID, rootPage); err != nil {
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

	// 检查唯一性约束
	if idx.IsUnique {
		ctx := context.Background()
		_, _, err := im.btreeManager.Search(ctx, key)
		if err == nil {
			return basic.ErrDuplicateKey
		}
	}

	// 插入到B+树
	ctx := context.Background()
	if err := im.btreeManager.Insert(ctx, key, value); err != nil {
		return fmt.Errorf("failed to insert key: %v", err)
	}

	// 更新索引统计
	idx.KeyCount++
	idx.UpdateTime = time.Now()
	im.stats.InsertCount++

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

	// 从B+树删除
	ctx := context.Background()
	_, _, err := im.btreeManager.Search(ctx, key)
	if err != nil {
		return fmt.Errorf("key not found: %v", err)
	}

	// TODO: 实现B+树删除操作
	// 目前B+树管理器没有Delete方法，需要补充

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

	// 在B+树中查找
	ctx := context.Background()
	pageNo, slot, err = im.btreeManager.Search(ctx, key)
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

	// 执行范围查询
	ctx := context.Background()
	rows, err := im.btreeManager.RangeSearch(ctx, startKey, endKey)
	if err != nil {
		return nil, fmt.Errorf("range search failed: %v", err)
	}

	// 更新统计
	im.stats.SearchCount++

	return rows, nil
}

// DropIndex 删除索引
func (im *IndexManager) DropIndex(indexID uint64) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	idx := im.indexes[indexID]
	if idx == nil {
		return ErrIndexNotFound
	}

	// 标记为删除状态
	idx.State = IndexStateDropping
	idx.UpdateTime = time.Now()

	// TODO: 清理B+树中的所有页面

	// 释放段中的所有页面
	for pageNo := uint32(0); pageNo < idx.PageCount; pageNo++ {
		if err := im.segmentManager.FreePage(idx.SegmentID, pageNo); err != nil {
			// 记录错误但继续处理
			fmt.Printf("Warning: failed to free page %d in segment %d: %v\n", pageNo, idx.SegmentID, err)
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

	// 标记为构建状态
	idx.State = IndexStateBuilding
	idx.UpdateTime = time.Now()

	// TODO: 实现索引重建逻辑
	// 1. 创建新的B+树
	// 2. 重新扫描表数据
	// 3. 重新插入所有键值
	// 4. 原子替换旧的索引结构

	// 标记为活跃状态
	idx.State = IndexStateActive
	idx.UpdateTime = time.Now()

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

	// TODO: 实现索引完整性检查
	// 1. 验证B+树结构
	// 2. 检查键值顺序
	// 3. 验证页面链接
	// 4. 检查统计信息

	return nil
}

// CompactIndex 压缩索引，减少碎片
func (im *IndexManager) CompactIndex(indexID uint64) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	idx := im.indexes[indexID]
	if idx == nil {
		return ErrIndexNotFound
	}

	// TODO: 实现索引压缩逻辑
	// 1. 重新组织页面
	// 2. 合并空闲空间
	// 3. 优化B+树结构

	idx.UpdateTime = time.Now()
	return nil
}
