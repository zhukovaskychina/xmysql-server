package manager

import (
	"sync"
	"time"
)

// ============ LOG-007.3: History List管理 ============

// HistoryNode History List节点
// 表示一个已提交事务的Undo日志信息
type HistoryNode struct {
	// 事务信息
	TxID       int64     // 事务ID
	CommitTime time.Time // 提交时间
	CommitLSN  uint64    // 提交LSN

	// Undo段信息
	SegmentID   uint32 // Undo段ID
	FirstLSN    uint64 // 第一条Undo日志LSN
	LastLSN     uint64 // 最后一条Undo日志LSN
	RecordCount int    // Undo记录数量

	// 链表指针
	Next *HistoryNode // 下一个节点
	Prev *HistoryNode // 上一个节点

	// 清理状态
	Purged    bool      // 是否已清理
	PurgeTime time.Time // 清理时间
}

// HistoryList History List管理器
// 维护已提交事务的Undo日志历史，用于MVCC和Purge
type HistoryList struct {
	mu sync.RWMutex

	// 链表头尾
	head *HistoryNode // 链表头（最新的事务）
	tail *HistoryNode // 链表尾（最老的事务）

	// 统计信息
	length     int       // 链表长度
	oldestTxID int64     // 最老事务ID
	oldestTime time.Time // 最老事务提交时间

	// 索引（用于快速查找）
	txIndex map[int64]*HistoryNode // 事务ID -> 节点

	// 配置
	maxLength int           // 最大长度
	maxAge    time.Duration // 最大保留时间
}

// NewHistoryList 创建History List
func NewHistoryList() *HistoryList {
	return &HistoryList{
		txIndex:   make(map[int64]*HistoryNode),
		maxLength: 10000,
		maxAge:    1 * time.Hour,
	}
}

// Add 添加节点到History List
func (hl *HistoryList) Add(node *HistoryNode) {
	hl.mu.Lock()
	defer hl.mu.Unlock()

	// 添加到头部
	node.Next = hl.head
	node.Prev = nil

	if hl.head != nil {
		hl.head.Prev = node
	}
	hl.head = node

	// 如果是第一个节点，也设置为尾部
	if hl.tail == nil {
		hl.tail = node
	}

	// 更新索引
	hl.txIndex[node.TxID] = node

	// 更新统计
	hl.length++
	if hl.oldestTxID == 0 || node.TxID < hl.oldestTxID {
		hl.oldestTxID = node.TxID
		hl.oldestTime = node.CommitTime
	}

	// 检查长度限制
	if hl.length > hl.maxLength {
		hl.removeOldest()
	}
}

// Remove 从History List移除节点
func (hl *HistoryList) Remove(txID int64) *HistoryNode {
	hl.mu.Lock()
	defer hl.mu.Unlock()

	node, exists := hl.txIndex[txID]
	if !exists {
		return nil
	}

	// 从链表中移除
	if node.Prev != nil {
		node.Prev.Next = node.Next
	} else {
		hl.head = node.Next
	}

	if node.Next != nil {
		node.Next.Prev = node.Prev
	} else {
		hl.tail = node.Prev
	}

	// 从索引中移除
	delete(hl.txIndex, txID)

	// 更新统计
	hl.length--

	// 更新最老事务信息
	if hl.tail != nil {
		hl.oldestTxID = hl.tail.TxID
		hl.oldestTime = hl.tail.CommitTime
	} else {
		hl.oldestTxID = 0
		hl.oldestTime = time.Time{}
	}

	return node
}

// removeOldest 移除最老的节点
func (hl *HistoryList) removeOldest() {
	if hl.tail == nil {
		return
	}

	hl.Remove(hl.tail.TxID)
}

// Get 获取指定事务的节点
func (hl *HistoryList) Get(txID int64) *HistoryNode {
	hl.mu.RLock()
	defer hl.mu.RUnlock()

	return hl.txIndex[txID]
}

// GetOldestNode 获取最老的节点
func (hl *HistoryList) GetOldestNode() *HistoryNode {
	hl.mu.RLock()
	defer hl.mu.RUnlock()

	return hl.tail
}

// GetLength 获取链表长度
func (hl *HistoryList) GetLength() int {
	hl.mu.RLock()
	defer hl.mu.RUnlock()

	return hl.length
}

// IterateOldToNew 从老到新遍历
func (hl *HistoryList) IterateOldToNew(callback func(*HistoryNode) bool) {
	hl.mu.RLock()
	defer hl.mu.RUnlock()

	for node := hl.tail; node != nil; node = node.Prev {
		if !callback(node) {
			break
		}
	}
}

// IterateNewToOld 从新到老遍历
func (hl *HistoryList) IterateNewToOld(callback func(*HistoryNode) bool) {
	hl.mu.RLock()
	defer hl.mu.RUnlock()

	for node := hl.head; node != nil; node = node.Next {
		if !callback(node) {
			break
		}
	}
}

// Purge 清理已过期的节点
func (hl *HistoryList) Purge(beforeTime time.Time) []*HistoryNode {
	hl.mu.Lock()
	defer hl.mu.Unlock()

	purged := make([]*HistoryNode, 0)

	// 从尾部开始清理
	for hl.tail != nil && hl.tail.CommitTime.Before(beforeTime) {
		node := hl.tail
		hl.Remove(node.TxID)
		node.Purged = true
		node.PurgeTime = time.Now()
		purged = append(purged, node)
	}

	return purged
}

// GetStats 获取统计信息
func (hl *HistoryList) GetStats() *HistoryListStats {
	hl.mu.RLock()
	defer hl.mu.RUnlock()

	stats := &HistoryListStats{
		Length:     hl.length,
		OldestTxID: hl.oldestTxID,
		OldestTime: hl.oldestTime,
	}

	if hl.head != nil {
		stats.NewestTxID = hl.head.TxID
		stats.NewestTime = hl.head.CommitTime
	}

	return stats
}

// HistoryListStats History List统计信息
type HistoryListStats struct {
	Length     int       `json:"length"`
	OldestTxID int64     `json:"oldest_tx_id"`
	OldestTime time.Time `json:"oldest_time"`
	NewestTxID int64     `json:"newest_tx_id"`
	NewestTime time.Time `json:"newest_time"`
}

// ============ History List Purger ============

// HistoryListPurger History List清理器
type HistoryListPurger struct {
	mu sync.RWMutex

	historyList *HistoryList
	purger      *UndoPurger

	// 配置
	purgeInterval time.Duration // 清理间隔
	purgeAge      time.Duration // 清理年龄阈值

	// 运行控制
	running  bool
	stopChan chan struct{}

	// 统计
	stats *HistoryPurgerStats
}

// HistoryPurgerStats History清理器统计
type HistoryPurgerStats struct {
	TotalPurges    uint64    `json:"total_purges"`
	TotalNodes     uint64    `json:"total_nodes"`
	LastPurgeTime  time.Time `json:"last_purge_time"`
	LastPurgeCount int       `json:"last_purge_count"`
}

// NewHistoryListPurger 创建History List清理器
func NewHistoryListPurger(historyList *HistoryList, purger *UndoPurger) *HistoryListPurger {
	return &HistoryListPurger{
		historyList:   historyList,
		purger:        purger,
		purgeInterval: 10 * time.Second,
		purgeAge:      5 * time.Minute,
		stopChan:      make(chan struct{}),
		stats:         &HistoryPurgerStats{},
	}
}

// Start 启动清理器
func (hlp *HistoryListPurger) Start() {
	hlp.mu.Lock()
	if hlp.running {
		hlp.mu.Unlock()
		return
	}
	hlp.running = true
	hlp.mu.Unlock()

	go hlp.purgeWorker()
}

// Stop 停止清理器
func (hlp *HistoryListPurger) Stop() {
	hlp.mu.Lock()
	if !hlp.running {
		hlp.mu.Unlock()
		return
	}
	hlp.running = false
	hlp.mu.Unlock()

	close(hlp.stopChan)
}

// purgeWorker 清理工作协程
func (hlp *HistoryListPurger) purgeWorker() {
	ticker := time.NewTicker(hlp.purgeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hlp.purgeOldNodes()

		case <-hlp.stopChan:
			return
		}
	}
}

// purgeOldNodes 清理旧节点
func (hlp *HistoryListPurger) purgeOldNodes() {
	hlp.mu.Lock()
	defer hlp.mu.Unlock()

	// 计算清理阈值时间
	beforeTime := time.Now().Add(-hlp.purgeAge)

	// 清理
	purged := hlp.historyList.Purge(beforeTime)

	// 更新统计
	if len(purged) > 0 {
		hlp.stats.TotalPurges++
		hlp.stats.TotalNodes += uint64(len(purged))
		hlp.stats.LastPurgeTime = time.Now()
		hlp.stats.LastPurgeCount = len(purged)
	}
}

// GetStats 获取统计信息
func (hlp *HistoryListPurger) GetStats() *HistoryPurgerStats {
	hlp.mu.RLock()
	defer hlp.mu.RUnlock()

	stats := *hlp.stats
	return &stats
}
