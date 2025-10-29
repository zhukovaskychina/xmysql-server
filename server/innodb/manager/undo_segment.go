package manager

import (
	"fmt"
	"sync"
	"time"
)

// ============ LOG-007.1: Undo段分配和管理 ============

// UndoSegment Undo段
// Undo段是Undo日志的基本管理单元，每个段包含一个事务或多个事务的Undo日志
type UndoSegment struct {
	mu sync.RWMutex

	// 段标识
	segmentID    uint32       // 段ID
	tablespaceID uint32       // 表空间ID
	pageID       uint64       // 段头页面ID
	state        SegmentState // 段状态

	// 容量信息
	totalSize uint64 // 总大小（字节）
	usedSize  uint64 // 已使用大小
	freeSize  uint64 // 剩余空间
	maxSize   uint64 // 最大大小

	// 事务信息
	txID        int64     // 当前使用的事务ID (0表示空闲)
	txStartTime time.Time // 事务开始时间
	txEndTime   time.Time // 事务结束时间

	// Undo日志
	undoLogs    []UndoLogEntry // Undo日志列表
	firstRecLSN uint64         // 第一条记录的LSN
	lastRecLSN  uint64         // 最后一条记录的LSN

	// 段元数据
	createTime   time.Time // 创建时间
	lastUsedTime time.Time // 最后使用时间
	reuseCount   uint32    // 重用次数

	// History List相关
	historyNode *HistoryNode // History List节点
	purged      bool         // 是否已清理
}

// SegmentState 段状态
type SegmentState int

const (
	SEGMENT_FREE     SegmentState = iota // 空闲
	SEGMENT_ACTIVE                       // 活跃（正在使用）
	SEGMENT_PREPARED                     // 准备状态（事务已提交，待清理）
	SEGMENT_CACHED                       // 缓存状态（可重用）
	SEGMENT_PURGING                      // 清理中
)

// NewUndoSegment 创建Undo段
func NewUndoSegment(segmentID, tablespaceID uint32, pageID uint64, maxSize uint64) *UndoSegment {
	return &UndoSegment{
		segmentID:    segmentID,
		tablespaceID: tablespaceID,
		pageID:       pageID,
		state:        SEGMENT_FREE,
		maxSize:      maxSize,
		totalSize:    maxSize,
		freeSize:     maxSize,
		undoLogs:     make([]UndoLogEntry, 0, 100),
		createTime:   time.Now(),
	}
}

// Allocate 分配段给事务
func (us *UndoSegment) Allocate(txID int64) error {
	us.mu.Lock()
	defer us.mu.Unlock()

	if us.state != SEGMENT_FREE && us.state != SEGMENT_CACHED {
		return fmt.Errorf("segment is not available, state: %d", us.state)
	}

	us.txID = txID
	us.txStartTime = time.Now()
	us.state = SEGMENT_ACTIVE
	us.reuseCount++

	return nil
}

// AddUndoLog 添加Undo日志到段
func (us *UndoSegment) AddUndoLog(entry *UndoLogEntry) error {
	us.mu.Lock()
	defer us.mu.Unlock()

	if us.state != SEGMENT_ACTIVE {
		return fmt.Errorf("segment is not active")
	}

	// 检查空间
	entrySize := uint64(entry.Size())
	if us.freeSize < entrySize {
		return fmt.Errorf("segment is full")
	}

	// 添加日志
	us.undoLogs = append(us.undoLogs, *entry)

	// 更新大小
	us.usedSize += entrySize
	us.freeSize -= entrySize

	// 更新LSN
	if us.firstRecLSN == 0 {
		us.firstRecLSN = entry.LSN
	}
	us.lastRecLSN = entry.LSN

	return nil
}

// Prepare 准备段（事务已提交）
func (us *UndoSegment) Prepare() error {
	us.mu.Lock()
	defer us.mu.Unlock()

	if us.state != SEGMENT_ACTIVE {
		return fmt.Errorf("segment is not active")
	}

	us.state = SEGMENT_PREPARED
	us.txEndTime = time.Now()

	return nil
}

// MarkForPurge 标记为待清理
func (us *UndoSegment) MarkForPurge() {
	us.mu.Lock()
	defer us.mu.Unlock()

	us.state = SEGMENT_PURGING
}

// Purge 清理段
func (us *UndoSegment) Purge() error {
	us.mu.Lock()
	defer us.mu.Unlock()

	// 清空Undo日志
	us.undoLogs = us.undoLogs[:0]

	// 重置大小
	us.usedSize = 0
	us.freeSize = us.totalSize

	// 重置LSN
	us.firstRecLSN = 0
	us.lastRecLSN = 0

	// 重置事务信息
	us.txID = 0
	us.txStartTime = time.Time{}
	us.txEndTime = time.Time{}

	// 标记为已清理
	us.purged = true
	us.state = SEGMENT_CACHED
	us.lastUsedTime = time.Now()

	return nil
}

// Release 释放段
func (us *UndoSegment) Release() {
	us.mu.Lock()
	defer us.mu.Unlock()

	us.state = SEGMENT_FREE
}

// GetUtilization 获取利用率
func (us *UndoSegment) GetUtilization() float64 {
	us.mu.RLock()
	defer us.mu.RUnlock()

	return float64(us.usedSize) / float64(us.totalSize)
}

// GetStats 获取段统计信息
func (us *UndoSegment) GetStats() *UndoSegmentStats {
	us.mu.RLock()
	defer us.mu.RUnlock()

	return &UndoSegmentStats{
		SegmentID:   us.segmentID,
		State:       us.state,
		TxID:        us.txID,
		TotalSize:   us.totalSize,
		UsedSize:    us.usedSize,
		FreeSize:    us.freeSize,
		LogCount:    len(us.undoLogs),
		Utilization: us.GetUtilization(),
		ReuseCount:  us.reuseCount,
	}
}

// UndoSegmentStats Undo段统计信息
type UndoSegmentStats struct {
	SegmentID   uint32       `json:"segment_id"`
	State       SegmentState `json:"state"`
	TxID        int64        `json:"tx_id"`
	TotalSize   uint64       `json:"total_size"`
	UsedSize    uint64       `json:"used_size"`
	FreeSize    uint64       `json:"free_size"`
	LogCount    int          `json:"log_count"`
	Utilization float64      `json:"utilization"`
	ReuseCount  uint32       `json:"reuse_count"`
}

// ============ Undo段管理器 ============

// UndoSegmentManager Undo段管理器
type UndoSegmentManager struct {
	mu sync.RWMutex

	// 段池
	segments       map[uint32]*UndoSegment // 所有段
	freeSegments   []*UndoSegment          // 空闲段列表
	activeSegments map[int64]*UndoSegment  // 活跃段（事务ID -> 段）
	cachedSegments []*UndoSegment          // 缓存段列表

	// 段分配
	nextSegmentID uint32 // 下一个段ID
	tablespaceID  uint32 // 表空间ID
	segmentSize   uint64 // 段大小

	// 配置
	maxSegments     int           // 最大段数量
	minFreeSegments int           // 最小空闲段数量
	cacheTimeout    time.Duration // 缓存超时时间

	// 统计信息
	stats *UndoSegmentManagerStats
}

// UndoSegmentManagerStats Undo段管理器统计
type UndoSegmentManagerStats struct {
	TotalSegments  int     `json:"total_segments"`
	FreeSegments   int     `json:"free_segments"`
	ActiveSegments int     `json:"active_segments"`
	CachedSegments int     `json:"cached_segments"`
	AvgUtilization float64 `json:"avg_utilization"`
}

// NewUndoSegmentManager 创建Undo段管理器
func NewUndoSegmentManager(tablespaceID uint32, segmentSize uint64, maxSegments int) *UndoSegmentManager {
	usm := &UndoSegmentManager{
		segments:        make(map[uint32]*UndoSegment),
		freeSegments:    make([]*UndoSegment, 0, maxSegments),
		activeSegments:  make(map[int64]*UndoSegment),
		cachedSegments:  make([]*UndoSegment, 0, maxSegments/10),
		nextSegmentID:   1,
		tablespaceID:    tablespaceID,
		segmentSize:     segmentSize,
		maxSegments:     maxSegments,
		minFreeSegments: 10,
		cacheTimeout:    5 * time.Minute,
		stats:           &UndoSegmentManagerStats{},
	}

	// 预分配一些空闲段
	usm.preallocateSegments(usm.minFreeSegments)

	return usm
}

// AllocateSegment 为事务分配Undo段
func (usm *UndoSegmentManager) AllocateSegment(txID int64) (*UndoSegment, error) {
	usm.mu.Lock()
	defer usm.mu.Unlock()

	// 1. 尝试从缓存段中分配
	if len(usm.cachedSegments) > 0 {
		segment := usm.cachedSegments[0]
		usm.cachedSegments = usm.cachedSegments[1:]

		if err := segment.Allocate(txID); err == nil {
			usm.activeSegments[txID] = segment
			return segment, nil
		}
	}

	// 2. 尝试从空闲段中分配
	if len(usm.freeSegments) > 0 {
		segment := usm.freeSegments[0]
		usm.freeSegments = usm.freeSegments[1:]

		if err := segment.Allocate(txID); err == nil {
			usm.activeSegments[txID] = segment
			return segment, nil
		}
	}

	// 3. 创建新段
	if len(usm.segments) < usm.maxSegments {
		segment := usm.createSegment()
		if err := segment.Allocate(txID); err == nil {
			usm.activeSegments[txID] = segment
			return segment, nil
		}
	}

	return nil, fmt.Errorf("no available undo segment")
}

// ReleaseSegment 释放Undo段
func (usm *UndoSegmentManager) ReleaseSegment(txID int64) error {
	usm.mu.Lock()
	defer usm.mu.Unlock()

	segment, exists := usm.activeSegments[txID]
	if !exists {
		return fmt.Errorf("segment not found for transaction %d", txID)
	}

	// 标记为准备状态
	if err := segment.Prepare(); err != nil {
		return err
	}

	// 从活跃段中移除
	delete(usm.activeSegments, txID)

	// 加入缓存段列表
	usm.cachedSegments = append(usm.cachedSegments, segment)

	return nil
}

// preallocateSegments 预分配段
func (usm *UndoSegmentManager) preallocateSegments(count int) {
	for i := 0; i < count; i++ {
		segment := usm.createSegment()
		usm.freeSegments = append(usm.freeSegments, segment)
	}
}

// createSegment 创建新段
func (usm *UndoSegmentManager) createSegment() *UndoSegment {
	segmentID := usm.nextSegmentID
	usm.nextSegmentID++

	// 计算页面ID (简化版本)
	pageID := uint64(segmentID) * 100

	segment := NewUndoSegment(segmentID, usm.tablespaceID, pageID, usm.segmentSize)
	usm.segments[segmentID] = segment

	return segment
}

// GetStats 获取统计信息
func (usm *UndoSegmentManager) GetStats() *UndoSegmentManagerStats {
	usm.mu.RLock()
	defer usm.mu.RUnlock()

	// 计算平均利用率
	var totalUtilization float64
	activeCount := 0
	for _, segment := range usm.activeSegments {
		totalUtilization += segment.GetUtilization()
		activeCount++
	}

	avgUtilization := 0.0
	if activeCount > 0 {
		avgUtilization = totalUtilization / float64(activeCount)
	}

	return &UndoSegmentManagerStats{
		TotalSegments:  len(usm.segments),
		FreeSegments:   len(usm.freeSegments),
		ActiveSegments: len(usm.activeSegments),
		CachedSegments: len(usm.cachedSegments),
		AvgUtilization: avgUtilization,
	}
}

// Size 计算Undo日志条目的大小
func (e *UndoLogEntry) Size() int {
	// LSN(8) + TrxID(8) + TableID(8) + Type(1) + DataLen(2) + Data(n)
	return 8 + 8 + 8 + 1 + 2 + len(e.Data)
}
