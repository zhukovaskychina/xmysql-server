package manager

import (
	"sync"
	"time"
)

// ============ LOG-007.2: Undo日志空间回收机制 ============

// UndoPurger Undo日志清理器
// 负责清理已提交事务的Undo日志，回收空间
type UndoPurger struct {
	mu sync.RWMutex

	// 段管理器
	segmentManager *UndoSegmentManager

	// 清理队列
	purgeQueue   []*UndoSegment // 待清理段队列
	purgeHistory []*HistoryNode // History List

	// 配置
	purgeInterval time.Duration // 清理间隔
	batchSize     int           // 批量清理数量
	retentionTime time.Duration // 保留时间（事务提交后多久可清理）
	maxPurgeTime  time.Duration // 单次清理最大时间

	// 运行控制
	running  bool          // 是否运行中
	stopChan chan struct{} // 停止信号

	// 统计信息
	stats *UndoPurgerStats

	// 活跃快照。只要快照仍可能访问某事务版本，就不能 purge 对应 Undo。
	nextReadViewToken uint64
	activeReadViews   map[uint64]purgeReadView
}

type purgeReadView interface {
	GetLowWaterMark() uint64
	GetHighWaterMark() uint64
}

// UndoPurgerStats Undo清理器统计
type UndoPurgerStats struct {
	TotalPurges     uint64        `json:"total_purges"`       // 总清理次数
	TotalSegments   uint64        `json:"total_segments"`     // 总清理段数
	TotalRecords    uint64        `json:"total_records"`      // 总清理记录数
	BytesFreed      uint64        `json:"bytes_freed"`        // 释放字节数
	AvgPurgeTime    time.Duration `json:"avg_purge_time"`     // 平均清理时间
	LastPurgeTime   time.Time     `json:"last_purge_time"`    // 最后清理时间
	PendingSegments int           `json:"pending_segments"`   // 待清理段数
	OldestTxEndTime time.Time     `json:"oldest_tx_end_time"` // 最老事务结束时间
}

// NewUndoPurger 创建Undo清理器
func NewUndoPurger(segmentManager *UndoSegmentManager) *UndoPurger {
	return &UndoPurger{
		segmentManager:  segmentManager,
		purgeQueue:      make([]*UndoSegment, 0, 1000),
		purgeHistory:    make([]*HistoryNode, 0, 1000),
		purgeInterval:   1 * time.Second,
		batchSize:       100,
		retentionTime:   5 * time.Second,
		maxPurgeTime:    100 * time.Millisecond,
		stopChan:        make(chan struct{}),
		stats:           &UndoPurgerStats{},
		activeReadViews: make(map[uint64]purgeReadView),
	}
}

// Start 启动清理器
func (up *UndoPurger) Start() {
	up.mu.Lock()
	if up.running {
		up.mu.Unlock()
		return
	}
	up.running = true
	up.mu.Unlock()

	go up.purgeWorker()
}

// Stop 停止清理器
func (up *UndoPurger) Stop() {
	up.mu.Lock()
	if !up.running {
		up.mu.Unlock()
		return
	}
	up.running = false
	up.mu.Unlock()

	close(up.stopChan)
}

// SchedulePurge 计划清理段
func (up *UndoPurger) SchedulePurge(segment *UndoSegment) {
	up.mu.Lock()
	defer up.mu.Unlock()

	up.purgeQueue = append(up.purgeQueue, segment)
}

func (up *UndoPurger) RegisterActiveReadView(readView purgeReadView) uint64 {
	up.mu.Lock()
	defer up.mu.Unlock()

	if readView == nil {
		return 0
	}
	up.nextReadViewToken++
	token := up.nextReadViewToken
	up.activeReadViews[token] = readView
	return token
}

func (up *UndoPurger) UnregisterActiveReadView(token uint64) {
	up.mu.Lock()
	defer up.mu.Unlock()
	delete(up.activeReadViews, token)
}

// purgeWorker 清理工作协程
func (up *UndoPurger) purgeWorker() {
	ticker := time.NewTicker(up.purgeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			up.purgeOldSegments()

		case <-up.stopChan:
			return
		}
	}
}

// purgeOldSegments 清理旧段
func (up *UndoPurger) purgeOldSegments() {
	up.mu.Lock()
	defer up.mu.Unlock()

	if len(up.purgeQueue) == 0 {
		return
	}

	startTime := time.Now()
	purgedCount := 0
	bytesFreed := uint64(0)

	remaining := up.purgeQueue[:0]

	// 批量清理
	for i := 0; i < len(up.purgeQueue) && i < up.batchSize; i++ {
		segment := up.purgeQueue[i]

		// 检查是否可以清理
		if !up.canPurge(segment) {
			remaining = append(remaining, segment)
			continue
		}

		// 标记为清理中
		segment.MarkForPurge()

		// 清理段
		bytesFreed += segment.usedSize
		if err := segment.Purge(); err == nil {
			purgedCount++
		} else {
			remaining = append(remaining, segment)
		}

		// 检查时间限制
		if time.Since(startTime) > up.maxPurgeTime {
			remaining = append(remaining, up.purgeQueue[i+1:]...)
			break
		}
	}
	if len(up.purgeQueue) > up.batchSize {
		remaining = append(remaining, up.purgeQueue[up.batchSize:]...)
	}
	up.purgeQueue = remaining

	// 更新统计
	if purgedCount > 0 {
		up.stats.TotalPurges++
		up.stats.TotalSegments += uint64(purgedCount)
		up.stats.BytesFreed += bytesFreed
		up.stats.AvgPurgeTime = time.Since(startTime) / time.Duration(purgedCount)
		up.stats.LastPurgeTime = time.Now()
	}

	up.stats.PendingSegments = len(up.purgeQueue)
}

// canPurge 检查段是否可以清理
func (up *UndoPurger) canPurge(segment *UndoSegment) bool {
	// 检查状态
	if segment.state != SEGMENT_PREPARED {
		return false
	}

	// 检查保留时间
	if time.Since(segment.txEndTime) < up.retentionTime {
		return false
	}

	if up.segmentNeededByActiveSnapshot(segment) {
		return false
	}

	return true
}

func (up *UndoPurger) segmentNeededByActiveSnapshot(segment *UndoSegment) bool {
	if segment == nil || segment.txID <= 0 {
		return false
	}
	txID := uint64(segment.txID)
	for _, readView := range up.activeReadViews {
		if readView == nil {
			continue
		}
		if txID < readView.GetHighWaterMark() {
			return true
		}
	}
	return false
}

// GetStats 获取统计信息
func (up *UndoPurger) GetStats() *UndoPurgerStats {
	up.mu.RLock()
	defer up.mu.RUnlock()

	stats := *up.stats
	stats.PendingSegments = len(up.purgeQueue)

	return &stats
}

// SetPurgeInterval 设置清理间隔
func (up *UndoPurger) SetPurgeInterval(interval time.Duration) {
	up.mu.Lock()
	defer up.mu.Unlock()
	up.purgeInterval = interval
}

// SetRetentionTime 设置保留时间
func (up *UndoPurger) SetRetentionTime(retention time.Duration) {
	up.mu.Lock()
	defer up.mu.Unlock()
	up.retentionTime = retention
}

// ============ 空间回收优化 ============

// UndoSpaceReclaimer Undo空间回收器
// 实现更激进的空间回收策略
type UndoSpaceReclaimer struct {
	mu sync.RWMutex

	purger         *UndoPurger
	segmentManager *UndoSegmentManager

	// 回收策略
	targetUtilization float64       // 目标利用率
	compactionTrigger float64       // 压缩触发阈值
	minFreeSpace      uint64        // 最小空闲空间
	reclaimInterval   time.Duration // 回收间隔

	// 统计
	stats *ReclaimerStats
}

// ReclaimerStats 回收器统计
type ReclaimerStats struct {
	TotalReclaims   uint64 `json:"total_reclaims"`
	SpaceReclaimed  uint64 `json:"space_reclaimed"`
	SegmentsCompact uint64 `json:"segments_compacted"`
}

// NewUndoSpaceReclaimer 创建空间回收器
func NewUndoSpaceReclaimer(purger *UndoPurger, segmentManager *UndoSegmentManager) *UndoSpaceReclaimer {
	return &UndoSpaceReclaimer{
		purger:            purger,
		segmentManager:    segmentManager,
		targetUtilization: 0.7,
		compactionTrigger: 0.3,
		minFreeSpace:      1024 * 1024 * 10, // 10MB
		reclaimInterval:   10 * time.Second,
		stats:             &ReclaimerStats{},
	}
}

// Start 启动回收器
func (usr *UndoSpaceReclaimer) Start() {
	go usr.reclaimWorker()
}

// reclaimWorker 回收工作协程
func (usr *UndoSpaceReclaimer) reclaimWorker() {
	ticker := time.NewTicker(usr.reclaimInterval)
	defer ticker.Stop()

	for range ticker.C {
		usr.reclaimSpace()
	}
}

// reclaimSpace 回收空间
func (usr *UndoSpaceReclaimer) reclaimSpace() {
	usr.mu.Lock()
	defer usr.mu.Unlock()

	if usr.segmentManager == nil {
		usr.stats.TotalReclaims++
		return
	}

	usr.segmentManager.mu.Lock()
	defer usr.segmentManager.mu.Unlock()

	var reclaimed uint64
	var compacted uint64

	for _, segment := range usr.segmentManager.cachedSegments {
		if segment == nil {
			continue
		}

		segment.mu.RLock()
		state := segment.state
		usedSize := segment.usedSize
		segment.mu.RUnlock()

		if state != SEGMENT_PREPARED {
			continue
		}

		if usr.purger != nil {
			usr.purger.mu.RLock()
			canPurge := usr.purger.canPurge(segment)
			usr.purger.mu.RUnlock()
			if !canPurge {
				continue
			}
		}

		segment.MarkForPurge()
		if err := segment.Purge(); err != nil {
			continue
		}
		reclaimed += usedSize
		compacted++
	}

	usr.stats.TotalReclaims++
	usr.stats.SpaceReclaimed += reclaimed
	usr.stats.SegmentsCompact += compacted
}

// GetStats 获取统计信息
func (usr *UndoSpaceReclaimer) GetStats() *ReclaimerStats {
	usr.mu.RLock()
	defer usr.mu.RUnlock()

	stats := *usr.stats
	return &stats
}
