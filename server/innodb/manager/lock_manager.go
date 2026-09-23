package manager

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// LockType 锁类型
type LockType int

const (
	LOCK_S LockType = iota // 共享锁
	LOCK_X                 // 排他锁
)

// LockMode 锁模式
type LockMode int

const (
	LOCK_MODE_RECORD LockMode = iota // 行锁
	LOCK_MODE_TABLE                  // 表锁
)

// LockRequest 锁请求
type LockRequest struct {
	TxID     uint64    // 事务ID
	LockType LockType  // 锁类型
	Mode     LockMode  // 锁模式
	Granted  bool      // 是否已授予
	WaitChan chan bool // 等待通道
	Created  time.Time // 创建时间
}

// LockInfo 锁信息
type LockInfo struct {
	ResourceID string         // 资源ID(表ID_页ID_行ID)
	Requests   []*LockRequest // 锁请求队列
}

type WaitEdge struct {
	WaitingTxID  uint64
	BlockingTxID uint64
	ResourceID   string
	Since        time.Time
	LockType     LockType
	Mode         LockMode
	WaitDuration time.Duration
}

// LockManager 锁管理器
type LockManager struct {
	mu          sync.RWMutex
	closeOnce   sync.Once
	lockTable   map[string]*LockInfo // 锁表
	waitGraph   map[uint64][]uint64  // 等待图
	waitHistory []WaitEdge           // recently completed lock waits
	txnLocks    map[uint64][]string  // 事务持有的锁
	stopChan    chan struct{}        // 停止信号
	// 回滚回调：用于在死锁检测中通知事务管理器回滚事务
	onAbortTransaction func(uint64)
	lastDeadlock       *DeadlockInfo

	// TXN-012: Gap锁和Next-Key锁支持
	gapLocks        map[string][]*GapLockInfo             // Gap锁表 (key: tableID_indexID)
	nextKeyLocks    map[string][]*NextKeyLockInfo         // Next-Key锁表 (key: tableID_indexID)
	insertIntLocks  map[string][]*InsertIntentionLockInfo // 插入意向锁表
	txnGapLocks     map[uint64][]string                   // 事务持有的Gap锁
	txnNextKeyLocks map[uint64][]string                   // 事务持有的Next-Key锁
}

// NewLockManager 创建锁管理器
func NewLockManager() *LockManager {
	lm := &LockManager{
		lockTable:   make(map[string]*LockInfo),
		waitGraph:   make(map[uint64][]uint64),
		waitHistory: make([]WaitEdge, 0, 128),
		txnLocks:    make(map[uint64][]string),
		stopChan:    make(chan struct{}),
		// TXN-012: 初始化Gap锁和Next-Key锁相关映射
		gapLocks:        make(map[string][]*GapLockInfo),
		nextKeyLocks:    make(map[string][]*NextKeyLockInfo),
		insertIntLocks:  make(map[string][]*InsertIntentionLockInfo),
		txnGapLocks:     make(map[uint64][]string),
		txnNextKeyLocks: make(map[uint64][]string),
	}
	// 启动死锁检测
	go lm.deadlockDetection()
	return lm
}

// Close 关闭锁管理器
func (lm *LockManager) Close() {
	if lm == nil {
		return
	}
	lm.closeOnce.Do(func() {
		close(lm.stopChan)
	})
}

// SetAbortTransactionHandler 设置死锁回滚回调
func (lm *LockManager) SetAbortTransactionHandler(handler func(uint64)) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.onAbortTransaction = handler
}

// WaitGraphSnapshot exposes a read-only diagnostic view for PROCESSLIST and
// PERFORMANCE_SCHEMA compatibility queries without exposing mutable lock state.
func (lm *LockManager) WaitGraphSnapshot() []WaitEdge {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	result := make([]WaitEdge, 0)
	for waiting, blockers := range lm.waitGraph {
		for _, blocking := range blockers {
			edge := WaitEdge{WaitingTxID: waiting, BlockingTxID: blocking}
			for resourceID, info := range lm.lockTable {
				if info == nil {
					continue
				}
				waitingFound, blockingFound := false, false
				var waitingRequest *LockRequest
				for _, request := range info.Requests {
					if request.TxID == waiting && !request.Granted {
						waitingFound = true
						waitingRequest = request
					}
					if request.TxID == blocking && request.Granted {
						blockingFound = true
					}
				}
				if waitingFound && blockingFound {
					edge.ResourceID = resourceID
					edge.Since = waitingRequest.Created
					edge.LockType = waitingRequest.LockType
					edge.Mode = waitingRequest.Mode
					edge.WaitDuration = time.Since(waitingRequest.Created)
					if edge.WaitDuration < 0 {
						edge.WaitDuration = 0
					}
					break
				}
			}
			result = append(result, edge)
		}
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].WaitingTxID != result[right].WaitingTxID {
			return result[left].WaitingTxID < result[right].WaitingTxID
		}
		if result[left].BlockingTxID != result[right].BlockingTxID {
			return result[left].BlockingTxID < result[right].BlockingTxID
		}
		return result[left].ResourceID < result[right].ResourceID
	})
	return result
}

// WaitHistorySnapshot returns completed record-lock waits retained for the
// bounded Performance Schema history views. The live wait graph remains the
// source for events_waits_current.
func (lm *LockManager) WaitHistorySnapshot() []WaitEdge {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	result := make([]WaitEdge, len(lm.waitHistory))
	copy(result, lm.waitHistory)
	return result
}

// LastDeadlockSnapshot returns a detached copy of the most recently detected
// deadlock, including its cycle, selected victim, and maximum wait duration.
func (lm *LockManager) LastDeadlockSnapshot() *DeadlockInfo {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	if lm.lastDeadlock == nil {
		return nil
	}
	snapshot := *lm.lastDeadlock
	snapshot.WaitingTxns = append([]uint64(nil), lm.lastDeadlock.WaitingTxns...)
	snapshot.Cycle = append([]uint64(nil), lm.lastDeadlock.Cycle...)
	return &snapshot
}

// makeResourceID 生成资源ID
func makeResourceID(tableID, pageID uint32, rowID uint64) string {
	return fmt.Sprintf("%d_%d_%d", tableID, pageID, rowID)
}

// isLockCompatible 检查锁兼容性
func isLockCompatible(existing, requested LockType) bool {
	if existing == LOCK_X || requested == LOCK_X {
		return false
	}
	return true
}

// checkDeadlock 检查死锁
func (lm *LockManager) checkDeadlock(txID uint64, visited map[uint64]bool) bool {
	if visited[txID] {
		return true // 发现环
	}

	visited[txID] = true
	for _, waitTxID := range lm.waitGraph[txID] {
		if lm.checkDeadlock(waitTxID, visited) {
			return true
		}
	}
	delete(visited, txID)
	return false
}

// updateWaitGraph 更新等待图
func (lm *LockManager) updateWaitGraph(waitingTxID uint64, holdingTxIDs []uint64) {
	lm.waitGraph[waitingTxID] = holdingTxIDs
}

// removeFromWaitGraph 从等待图中移除事务
func (lm *LockManager) removeFromWaitGraph(txID uint64) {
	delete(lm.waitGraph, txID)
	// 移除其他事务对该事务的等待
	for tid, waitList := range lm.waitGraph {
		newWaitList := make([]uint64, 0)
		for _, wid := range waitList {
			if wid != txID {
				newWaitList = append(newWaitList, wid)
			}
		}
		if len(newWaitList) == 0 {
			delete(lm.waitGraph, tid)
		} else {
			lm.waitGraph[tid] = newWaitList
		}
	}
}

// deadlockDetection 死锁检测循环
func (lm *LockManager) deadlockDetection() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			lm.mu.Lock()
			victimTxID := lm.detectDeadlockLocked()
			lm.mu.Unlock()

			if victimTxID != 0 {
				lm.abortTransaction(victimTxID)
			}
		case <-lm.stopChan:
			return
		}
	}
}

func (lm *LockManager) detectDeadlockLocked() uint64 {
	starts := make([]uint64, 0, len(lm.waitGraph))
	for txID := range lm.waitGraph {
		starts = append(starts, txID)
	}
	sort.Slice(starts, func(left, right int) bool { return starts[left] < starts[right] })
	for _, txID := range starts {
		cycle := lm.findDeadlockCycleLocked(txID)
		if len(cycle) == 0 {
			continue
		}
		victimTxID := lm.findOldestWaitingTxInCycle(cycle)
		waitDuration := time.Duration(0)
		waiting := make([]uint64, 0, len(cycle))
		for _, cycleTxID := range cycle {
			waiting = append(waiting, cycleTxID)
			for _, info := range lm.lockTable {
				if info == nil {
					continue
				}
				for _, request := range info.Requests {
					if request.TxID == cycleTxID && !request.Granted {
						age := time.Since(request.Created)
						if age > waitDuration {
							waitDuration = age
						}
					}
				}
			}
		}
		if waitDuration < 0 {
			waitDuration = 0
		}
		lm.lastDeadlock = &DeadlockInfo{
			DetectedAt:   time.Now(),
			WaitingTxns:  waiting,
			Cycle:        append([]uint64(nil), cycle...),
			VictimTxID:   victimTxID,
			WaitDuration: waitDuration,
		}
		return victimTxID
	}
	return 0
}

func (lm *LockManager) findDeadlockCycleLocked(start uint64) []uint64 {
	var visit func(uint64, map[uint64]int, []uint64) []uint64
	visit = func(current uint64, positions map[uint64]int, path []uint64) []uint64 {
		if position, exists := positions[current]; exists {
			return append(append([]uint64(nil), path[position:]...), current)
		}
		positions[current] = len(path)
		path = append(path, current)
		for _, next := range lm.waitGraph[current] {
			if cycle := visit(next, positions, path); len(cycle) > 0 {
				return cycle
			}
		}
		delete(positions, current)
		return nil
	}
	return visit(start, make(map[uint64]int), nil)
}

// findOldestWaitingTx 找到等待时间最长的事务
func (lm *LockManager) findOldestWaitingTx() uint64 {
	var oldestTxID uint64
	var oldestTime time.Time

	for _, lockInfo := range lm.lockTable {
		for _, req := range lockInfo.Requests {
			if !req.Granted && (oldestTime.IsZero() || req.Created.Before(oldestTime) ||
				(req.Created.Equal(oldestTime) && (oldestTxID == 0 || req.TxID < oldestTxID))) {
				oldestTime = req.Created
				oldestTxID = req.TxID
			}
		}
	}

	return oldestTxID
}

func (lm *LockManager) findOldestWaitingTxInCycle(cycle []uint64) uint64 {
	if len(cycle) == 0 {
		return 0
	}
	cycleTxns := make(map[uint64]struct{}, len(cycle))
	for _, txID := range cycle {
		cycleTxns[txID] = struct{}{}
	}
	var oldestTxID uint64
	var oldestTime time.Time
	for _, lockInfo := range lm.lockTable {
		for _, req := range lockInfo.Requests {
			if req.Granted {
				continue
			}
			if _, ok := cycleTxns[req.TxID]; !ok {
				continue
			}
			if oldestTime.IsZero() || req.Created.Before(oldestTime) ||
				(req.Created.Equal(oldestTime) && (oldestTxID == 0 || req.TxID < oldestTxID)) {
				oldestTime = req.Created
				oldestTxID = req.TxID
			}
		}
	}
	return oldestTxID
}

// abortTransaction 中止事务
func (lm *LockManager) abortTransaction(txID uint64) {
	if txID == 0 {
		return
	}

	// 释放该事务持有的所有锁
	lm.ReleaseLocks(txID)

	lm.mu.Lock()
	callback := lm.onAbortTransaction
	lm.mu.Unlock()

	if callback != nil {
		callback(txID)
	}
}

// AcquireLock 获取锁
func (lm *LockManager) AcquireLock(txID uint64, tableID, pageID uint32, rowID uint64, lockType LockType) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	resourceID := makeResourceID(tableID, pageID, rowID)
	info, exists := lm.lockTable[resourceID]
	if !exists {
		info = &LockInfo{
			ResourceID: resourceID,
			Requests:   make([]*LockRequest, 0),
		}
		lm.lockTable[resourceID] = info
	}

	// 检查是否已持有锁
	for _, req := range info.Requests {
		if req.TxID == txID {
			if req.LockType == lockType {
				if req.Granted {
					return nil // 已持有相同类型的锁
				}
				return ErrLockConflict
			}
			// 升级锁
			if req.LockType == LOCK_S && lockType == LOCK_X {
				// 检查是否有其他事务持有共享锁
				var holdingTxIDs []uint64
				for _, r := range info.Requests {
					if r.TxID != txID && r.Granted {
						holdingTxIDs = append(holdingTxIDs, r.TxID)
					}
				}
				if len(holdingTxIDs) > 0 {
					lm.updateWaitGraph(txID, holdingTxIDs)
					visited := make(map[uint64]bool)
					if lm.checkDeadlock(txID, visited) {
						lm.removeFromWaitGraph(txID)
						return ErrDeadlockDetected
					}
					return ErrLockConflict
				}
				req.LockType = LOCK_X
				return nil
			}
		}
	}

	// 检查锁兼容性
	var holdingTxIDs []uint64
	for _, req := range info.Requests {
		if req.Granted && !isLockCompatible(req.LockType, lockType) {
			holdingTxIDs = append(holdingTxIDs, req.TxID)
		}
	}

	// 创建新的锁请求
	newReq := &LockRequest{
		TxID:     txID,
		LockType: lockType,
		Mode:     LOCK_MODE_RECORD,
		Granted:  len(holdingTxIDs) == 0,
		WaitChan: make(chan bool, 1),
		Created:  time.Now(),
	}

	// 添加到请求队列
	info.Requests = append(info.Requests, newReq)

	// 如果需要等待，更新等待图
	if len(holdingTxIDs) > 0 {
		lm.updateWaitGraph(txID, holdingTxIDs)
		// 检查死锁
		visited := make(map[uint64]bool)
		if lm.checkDeadlock(txID, visited) {
			// 移除请求
			info.Requests = info.Requests[:len(info.Requests)-1]
			lm.removeFromWaitGraph(txID)
			return ErrDeadlockDetected
		}
	}

	// 记录事务持有的锁
	lm.txnLocks[txID] = append(lm.txnLocks[txID], resourceID)

	if len(holdingTxIDs) > 0 {
		return ErrLockConflict
	}

	return nil
}

// ReleaseLocks 释放事务持有的所有锁
func (lm *LockManager) ReleaseLocks(txID uint64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.releaseLocksLocked(txID)
}

// ReleaseLock 释放事务在指定资源上的锁
func (lm *LockManager) ReleaseLock(txID uint64, resourceID string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if resourceID == "" {
		return ErrInvalidParam
	}
	return lm.releaseSingleLockLocked(txID, resourceID)
}

// releaseLocksLocked 释放事务持有的所有锁（调用者已持有锁）
func (lm *LockManager) releaseLocksLocked(txID uint64) {
	// 1. 释放Record Lock
	// 获取事务持有的所有资源ID
	resourceIDs := lm.txnLocks[txID]
	delete(lm.txnLocks, txID)

	// 释放每个资源上的锁
	for _, resourceID := range resourceIDs {
		_ = lm.releaseSingleLockLocked(txID, resourceID)
	}

	// 2. TXN-012: 释放Gap锁
	gapKeys := lm.txnGapLocks[txID]
	delete(lm.txnGapLocks, txID)
	for _, key := range gapKeys {
		locks := lm.gapLocks[key]
		if locks == nil {
			continue
		}
		var newLocks []*GapLockInfo
		for _, lock := range locks {
			if lock.TxID != txID {
				newLocks = append(newLocks, lock)
			}
		}
		if len(newLocks) == 0 {
			delete(lm.gapLocks, key)
		} else {
			lm.gapLocks[key] = newLocks
		}
		// 尝试授予等待的插入意向锁
		lm.grantWaitingInsertIntentionLocks(key)
	}

	// 3. TXN-013: 释放Next-Key锁
	nextKeyKeys := lm.txnNextKeyLocks[txID]
	delete(lm.txnNextKeyLocks, txID)
	for _, key := range nextKeyKeys {
		locks := lm.nextKeyLocks[key]
		if locks == nil {
			continue
		}
		var newLocks []*NextKeyLockInfo
		for _, lock := range locks {
			if lock.TxID != txID {
				newLocks = append(newLocks, lock)
			}
		}
		if len(newLocks) == 0 {
			delete(lm.nextKeyLocks, key)
		} else {
			lm.nextKeyLocks[key] = newLocks
		}
		lm.grantWaitingNextKeyLocks(key)
	}

	// 从等待图中移除事务
	lm.removeFromWaitGraph(txID)
}

// releaseSingleLockLocked 释放指定资源上的锁（调用者已持有锁）
func (lm *LockManager) releaseSingleLockLocked(txID uint64, resourceID string) error {
	info := lm.lockTable[resourceID]
	if info == nil {
		return ErrLockNotFound
	}

	found := false
	var newRequests []*LockRequest
	var releasedRequests []*LockRequest
	for _, req := range info.Requests {
		if req.TxID == txID {
			found = true
			releasedRequests = append(releasedRequests, req)
			continue
		}
		newRequests = append(newRequests, req)
	}

	if !found {
		return ErrLockNotFound
	}
	for _, released := range releasedRequests {
		if released == nil || !released.Granted {
			continue
		}
		for _, waiting := range newRequests {
			if waiting == nil || waiting.Granted || isLockCompatible(released.LockType, waiting.LockType) {
				continue
			}
			lm.recordWaitHistoryLocked(info.ResourceID, released, waiting)
		}
	}

	// 更新或删除锁信息
	if len(newRequests) == 0 {
		delete(lm.lockTable, resourceID)
	} else {
		info.Requests = newRequests
		// 尝试授予等待的锁
		lm.grantWaitingLocks(info)
	}

	// 更新事务持有锁列表
	resourceIDs := lm.txnLocks[txID]
	newResourceIDs := make([]string, 0, len(resourceIDs))
	for _, id := range resourceIDs {
		if id != resourceID {
			newResourceIDs = append(newResourceIDs, id)
		}
	}
	if len(newResourceIDs) == 0 {
		delete(lm.txnLocks, txID)
	} else {
		lm.txnLocks[txID] = newResourceIDs
	}

	return nil
}

// grantWaitingLocks 尝试授予等待的锁
func (lm *LockManager) grantWaitingLocks(info *LockInfo) {
	var grantedLocks []*LockRequest
	var waitingLocks []*LockRequest

	// 分离已授予和等待的锁
	for _, req := range info.Requests {
		if req.Granted {
			grantedLocks = append(grantedLocks, req)
		} else {
			waitingLocks = append(waitingLocks, req)
		}
	}

	// 尝试授予等待的锁
	for _, waiting := range waitingLocks {
		canGrant := true
		for _, granted := range grantedLocks {
			if !isLockCompatible(granted.LockType, waiting.LockType) {
				canGrant = false
				break
			}
		}

		if canGrant {
			for _, blocking := range grantedLocks {
				if isLockCompatible(blocking.LockType, waiting.LockType) {
					continue
				}
				lm.recordWaitHistoryLocked(info.ResourceID, blocking, waiting)
			}
			waiting.Granted = true
			grantedLocks = append(grantedLocks, waiting)
			// 通知等待的事务
			select {
			case waiting.WaitChan <- true:
			default:
			}
		}
	}
	lm.rebuildWaitGraphLocked()
}

func (lm *LockManager) recordWaitHistoryLocked(resourceID string, blocking, waiting *LockRequest) {
	if blocking == nil || waiting == nil {
		return
	}
	duration := time.Since(waiting.Created)
	if duration < 0 {
		duration = 0
	}
	lm.waitHistory = append(lm.waitHistory, WaitEdge{
		WaitingTxID: waiting.TxID, BlockingTxID: blocking.TxID,
		ResourceID: resourceID, Since: waiting.Created,
		LockType: waiting.LockType, Mode: waiting.Mode,
		WaitDuration: duration,
	})
	if len(lm.waitHistory) > 128 {
		lm.waitHistory = lm.waitHistory[len(lm.waitHistory)-128:]
	}
}

// rebuildWaitGraphLocked derives the graph from the current lock table. A
// waiter can become granted when another transaction releases a lock; keeping
// the graph derived prevents stale diagnostic edges and stale deadlock cycles.
// The caller must hold lm.mu.
func (lm *LockManager) rebuildWaitGraphLocked() {
	graph := make(map[uint64][]uint64)
	for _, info := range lm.lockTable {
		if info == nil {
			continue
		}
		for _, waiting := range info.Requests {
			if waiting == nil || waiting.Granted {
				continue
			}
			seen := make(map[uint64]struct{})
			for _, blocking := range info.Requests {
				if blocking == nil || !blocking.Granted || blocking.TxID == waiting.TxID || isLockCompatible(blocking.LockType, waiting.LockType) {
					continue
				}
				seen[blocking.TxID] = struct{}{}
			}
			for txID := range seen {
				graph[waiting.TxID] = append(graph[waiting.TxID], txID)
			}
		}
	}
	for txID := range graph {
		sort.Slice(graph[txID], func(left, right int) bool { return graph[txID][left] < graph[txID][right] })
	}
	lm.waitGraph = graph
}
