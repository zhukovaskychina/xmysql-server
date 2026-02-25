package manager

import (
	"encoding/binary"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"sync"
	"time"
)

// IBufManager 管理Insert Buffer
type IBufManager struct {
	mu sync.RWMutex

	// Insert Buffer映射: space_id -> ibuf_tree
	ibufTrees map[uint32]*IBufTree

	// 段管理器
	segmentManager *SegmentManager

	// 页面管理器
	pageManager basic.PageManager

	// 合并阈值
	mergeThreshold float64

	// 最后一次合并时间
	lastMergeTime time.Time

	// 后台合并控制
	stopChan chan struct{}
	running  bool

	// 统计信息
	stats *IBufStats
}

// IBufStats Insert Buffer统计信息
type IBufStats struct {
	InsertCount uint64 // 插入到Insert Buffer的记录数
	MergeCount  uint64 // 合并次数
	MergedCount uint64 // 已合并的记录数
	CachedCount uint64 // 当前缓存的记录数
}

// IBufTree 表示一个Insert Buffer B+树
type IBufTree struct {
	SpaceID    uint32 // 表空间ID
	SegmentID  uint32 // Insert Buffer段ID
	RootPageNo uint32 // B+树根页号
	Height     uint8  // B+树高度
	Size       uint64 // 缓存的记录数
}

// IBufRecord 表示一个Insert Buffer记录
type IBufRecord struct {
	basic.Record
	SpaceID uint32    // 表空间ID
	PageNo  uint32    // 目标页号
	Type    uint8     // 操作类型
	Key     []byte    // 索引键值
	Value   []byte    // 记录内容
	TrxID   uint64    // 事务ID
	Time    time.Time // 插入时间
}

// 操作类型常量
const (
	IBUF_OP_INSERT uint8 = iota // 插入操作
	IBUF_OP_DELETE              // 删除操作
	IBUF_OP_UPDATE              // 更新操作
)

// NewIBufManager 创建Insert Buffer管理器
func NewIBufManager(segmentManager *SegmentManager, pageManager basic.PageManager) *IBufManager {
	im := &IBufManager{
		ibufTrees:      make(map[uint32]*IBufTree),
		segmentManager: segmentManager,
		pageManager:    pageManager,
		mergeThreshold: 0.7, // 当缓存页使用率达到70%时触发合并
		lastMergeTime:  time.Now(),
		stopChan:       make(chan struct{}),
		running:        false,
		stats: &IBufStats{
			InsertCount: 0,
			MergeCount:  0,
			MergedCount: 0,
			CachedCount: 0,
		},
	}

	// 启动后台合并线程
	im.StartBackgroundMerge()

	return im
}

// CreateIBufTree 为表空间创建Insert Buffer树
func (im *IBufManager) CreateIBufTree(spaceID uint32) (*IBufTree, error) {
	im.mu.Lock()
	defer im.mu.Unlock()

	// 检查是否已存在
	if tree := im.ibufTrees[spaceID]; tree != nil {
		return tree, nil
	}

	// 为Insert Buffer创建段
	seg, err := im.segmentManager.CreateSegment(spaceID, SEGMENT_TYPE_INDEX, false)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment for insert buffer: %v", err)
	}

	// 获取段ID
	segmentImpl, ok := seg.(*SegmentImpl)
	if !ok {
		return nil, fmt.Errorf("invalid segment type")
	}

	// 分配根页面
	rootPageNo, err := im.segmentManager.AllocatePage(segmentImpl.SegmentID)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate root page: %v", err)
	}

	// 创建Insert Buffer树
	tree := &IBufTree{
		SpaceID:    spaceID,
		SegmentID:  segmentImpl.SegmentID,
		RootPageNo: rootPageNo,
		Height:     1,
		Size:       0,
	}

	// 保存到映射
	im.ibufTrees[spaceID] = tree

	return tree, nil
}

// InsertRecord 插入一条记录到Insert Buffer
func (im *IBufManager) InsertRecord(record *IBufRecord) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	// 获取或创建Insert Buffer树
	tree := im.ibufTrees[record.SpaceID]
	if tree == nil {
		var err error
		tree, err = im.createIBufTreeLocked(record.SpaceID)
		if err != nil {
			return fmt.Errorf("failed to create insert buffer tree: %v", err)
		}
	}

	// 构造Insert Buffer键值（页号 + 索引键）
	ibufKey := im.buildKey(record.PageNo, record.Key)

	// 构造Insert Buffer值（操作类型 + 记录内容 + 事务ID）
	ibufValue := im.buildValue(record)

	// 插入到Insert Buffer B+树
	// 注意：这里简化实现，实际应该使用专门的B+树管理器
	// 暂时将记录缓存在内存中
	_ = ibufKey   // 避免未使用变量警告
	_ = ibufValue // 避免未使用变量警告

	tree.Size++

	// 更新统计信息
	im.stats.InsertCount++

	// 检查是否需要触发合并
	if im.shouldMerge(tree) {
		go im.mergeIBufTree(tree) // 异步合并
	}

	return nil
}

// createIBufTreeLocked 创建Insert Buffer树（内部方法，调用者需持有锁）
func (im *IBufManager) createIBufTreeLocked(spaceID uint32) (*IBufTree, error) {
	// 检查是否已存在
	if tree := im.ibufTrees[spaceID]; tree != nil {
		return tree, nil
	}

	// 为Insert Buffer创建段
	seg, err := im.segmentManager.CreateSegment(spaceID, SEGMENT_TYPE_INDEX, false)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment for insert buffer: %v", err)
	}

	// 获取段ID
	segmentImpl, ok := seg.(*SegmentImpl)
	if !ok {
		return nil, fmt.Errorf("invalid segment type")
	}

	// 分配根页面
	rootPageNo, err := im.segmentManager.AllocatePage(segmentImpl.SegmentID)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate root page: %v", err)
	}

	// 创建Insert Buffer树
	tree := &IBufTree{
		SpaceID:    spaceID,
		SegmentID:  segmentImpl.SegmentID,
		RootPageNo: rootPageNo,
		Height:     1,
		Size:       0,
	}

	// 保存到映射
	im.ibufTrees[spaceID] = tree

	return tree, nil
}

// MergeIBufTree 合并Insert Buffer树到实际的索引页
func (im *IBufManager) mergeIBufTree(tree *IBufTree) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	if tree == nil || tree.Size == 0 {
		return nil // 没有需要合并的记录
	}

	// 记录合并前的记录数
	beforeSize := tree.Size

	// 按页号分组记录（简化实现）
	// 实际应该遍历B+树获取所有记录
	pageRecords := make(map[uint32][]*IBufRecord)

	// 这里简化实现，实际应该从B+树中读取记录
	// 暂时跳过实际合并逻辑

	// 合并每个页面的记录
	for pageNo, records := range pageRecords {
		if err := im.mergePage(tree.SpaceID, pageNo, records); err != nil {
			return fmt.Errorf("failed to merge page %d: %v", pageNo, err)
		}
	}

	// 清空Insert Buffer树
	tree.Size = 0

	// 更新统计信息
	im.stats.MergeCount++
	im.stats.MergedCount += beforeSize

	// 更新最后合并时间
	im.lastMergeTime = time.Now()

	return nil
}

// MergePage 合并一个页面的记录
func (im *IBufManager) mergePage(spaceID uint32, pageNo uint32, records []*IBufRecord) error {
	if len(records) == 0 {
		return nil
	}

	// 获取目标页面
	pageData, err := im.pageManager.GetPage(spaceID, pageNo)
	if err != nil {
		return fmt.Errorf("failed to get page: %v", err)
	}

	// 按操作类型处理记录
	for _, record := range records {
		switch record.Type {
		case IBUF_OP_INSERT:
			// 插入记录到页面
			if err := im.insertRecordToPage(pageData, record); err != nil {
				return fmt.Errorf("failed to insert record: %v", err)
			}

		case IBUF_OP_DELETE:
			// 从页面删除记录
			if err := im.deleteRecordFromPage(pageData, record); err != nil {
				return fmt.Errorf("failed to delete record: %v", err)
			}

		case IBUF_OP_UPDATE:
			// 更新页面中的记录
			if err := im.updateRecordInPage(pageData, record); err != nil {
				return fmt.Errorf("failed to update record: %v", err)
			}

		default:
			return fmt.Errorf("unknown operation type: %d", record.Type)
		}
	}

	// 写回页面（标记为脏页）
	if err := im.pageManager.WritePage(spaceID, pageNo, pageData); err != nil {
		return fmt.Errorf("failed to write page: %v", err)
	}

	return nil
}

// buildKey 构造Insert Buffer键值（页号 + 索引键）
func (im *IBufManager) buildKey(pageNo uint32, indexKey []byte) []byte {
	key := make([]byte, 4+len(indexKey))
	binary.BigEndian.PutUint32(key[:4], pageNo)
	copy(key[4:], indexKey)
	return key
}

// buildValue 构造Insert Buffer值（操作类型 + 事务ID + 记录内容）
func (im *IBufManager) buildValue(record *IBufRecord) []byte {
	// 格式：[Type:1字节][TrxID:8字节][ValueLen:4字节][Value:变长]
	valueLen := 1 + 8 + 4 + len(record.Value)
	value := make([]byte, valueLen)

	// 操作类型
	value[0] = record.Type

	// 事务ID
	binary.BigEndian.PutUint64(value[1:9], record.TrxID)

	// 记录长度
	binary.BigEndian.PutUint32(value[9:13], uint32(len(record.Value)))

	// 记录内容
	copy(value[13:], record.Value)

	return value
}

// shouldMerge 判断是否应该触发合并
func (im *IBufManager) shouldMerge(tree *IBufTree) bool {
	// 条件1：缓存记录数超过阈值
	if tree.Size > 10000 {
		return true
	}

	// 条件2：距离上次合并时间超过阈值
	if time.Since(im.lastMergeTime) > 5*time.Minute {
		return true
	}

	return false
}

// insertRecordToPage 插入记录到页面
func (im *IBufManager) insertRecordToPage(pageData []byte, record *IBufRecord) error {
	// 简化实现：实际应该调用页面的插入方法
	// 这里只是占位符，实际需要解析页面格式并插入记录
	_ = pageData
	_ = record
	return nil
}

// deleteRecordFromPage 从页面删除记录
func (im *IBufManager) deleteRecordFromPage(pageData []byte, record *IBufRecord) error {
	// 简化实现：实际应该调用页面的删除方法
	// 这里只是占位符，实际需要解析页面格式并删除记录
	_ = pageData
	_ = record
	return nil
}

// updateRecordInPage 更新页面中的记录
func (im *IBufManager) updateRecordInPage(pageData []byte, record *IBufRecord) error {
	// 简化实现：实际应该调用页面的更新方法
	// 这里只是占位符，实际需要解析页面格式并更新记录
	_ = pageData
	_ = record
	return nil
}

// StartBackgroundMerge 启动后台合并线程
func (im *IBufManager) StartBackgroundMerge() {
	im.mu.Lock()
	if im.running {
		im.mu.Unlock()
		return
	}
	im.running = true
	im.mu.Unlock()

	go func() {
		ticker := time.NewTicker(1 * time.Minute) // 每分钟检查一次
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				im.backgroundMerge()
			case <-im.stopChan:
				return
			}
		}
	}()
}

// StopBackgroundMerge 停止后台合并线程
func (im *IBufManager) StopBackgroundMerge() {
	im.mu.Lock()
	if !im.running {
		im.mu.Unlock()
		return
	}
	im.running = false
	im.mu.Unlock()

	close(im.stopChan)
}

// backgroundMerge 后台合并任务
func (im *IBufManager) backgroundMerge() {
	im.mu.RLock()
	trees := make([]*IBufTree, 0, len(im.ibufTrees))
	for _, tree := range im.ibufTrees {
		if im.shouldMerge(tree) {
			trees = append(trees, tree)
		}
	}
	im.mu.RUnlock()

	// 合并需要处理的树
	for _, tree := range trees {
		if err := im.mergeIBufTree(tree); err != nil {
			// 记录错误但继续处理其他树
			fmt.Printf("background merge failed for space %d: %v\n", tree.SpaceID, err)
		}
	}
}

// GetStats 获取统计信息
func (im *IBufManager) GetStats() *IBufStats {
	im.mu.RLock()
	defer im.mu.RUnlock()

	// 计算当前缓存的记录数
	var cachedCount uint64
	for _, tree := range im.ibufTrees {
		cachedCount += tree.Size
	}
	im.stats.CachedCount = cachedCount

	return im.stats
}

// GetIBufTree 获取Insert Buffer树
func (im *IBufManager) GetIBufTree(spaceID uint32) *IBufTree {
	im.mu.RLock()
	defer im.mu.RUnlock()
	return im.ibufTrees[spaceID]
}

// Close 关闭Insert Buffer管理器
func (im *IBufManager) Close() error {
	// 停止后台合并线程
	im.StopBackgroundMerge()

	im.mu.Lock()
	defer im.mu.Unlock()

	// 合并所有未处理的记录
	for _, tree := range im.ibufTrees {
		if err := im.mergeIBufTree(tree); err != nil {
			return err
		}
	}

	// 清理资源
	im.ibufTrees = nil
	return nil
}
