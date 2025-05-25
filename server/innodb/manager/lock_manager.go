package manager

import (
	"fmt"
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

// LockManager 锁管理器
type LockManager struct {
	mu        sync.RWMutex
	lockTable map[string]*LockInfo // 锁表
	waitGraph map[uint64][]uint64  // 等待图
	txnLocks  map[uint64][]string  // 事务持有的锁
	stopChan  chan struct{}        // 停止信号
}

// NewLockManager 创建锁管理器
func NewLockManager() *LockManager {
	lm := &LockManager{
		lockTable: make(map[string]*LockInfo),
		waitGraph: make(map[uint64][]uint64),
		txnLocks:  make(map[uint64][]string),
		stopChan:  make(chan struct{}),
	}
	// 启动死锁检测
	go lm.deadlockDetection()
	return lm
}

// Close 关闭锁管理器
func (lm *LockManager) Close() {
	close(lm.stopChan)
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
			// 检查每个事务是否存在死锁
			for txID := range lm.waitGraph {
				visited := make(map[uint64]bool)
				if lm.checkDeadlock(txID, visited) {
					// 找到最老的等待事务进行回滚
					oldestTxID := lm.findOldestWaitingTx()
					lm.abortTransaction(oldestTxID)
				}
			}
			lm.mu.Unlock()
		case <-lm.stopChan:
			return
		}
	}
}

// findOldestWaitingTx 找到等待时间最长的事务
func (lm *LockManager) findOldestWaitingTx() uint64 {
	var oldestTxID uint64
	var oldestTime time.Time

	for _, lockInfo := range lm.lockTable {
		for _, req := range lockInfo.Requests {
			if !req.Granted && (oldestTime.IsZero() || req.Created.Before(oldestTime)) {
				oldestTime = req.Created
				oldestTxID = req.TxID
			}
		}
	}

	return oldestTxID
}

// abortTransaction 中止事务
func (lm *LockManager) abortTransaction(txID uint64) {
	// 释放该事务持有的所有锁
	lm.ReleaseLocks(txID)
	// 从等待图中移除
	lm.removeFromWaitGraph(txID)
	// TODO: 通知事务管理器回滚事务
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
				return nil // 已持有相同类型的锁
			}
			// 升级锁
			if req.LockType == LOCK_S && lockType == LOCK_X {
				// 检查是否有其他事务持有共享锁
				for _, r := range info.Requests {
					if r.TxID != txID && r.Granted {
						return fmt.Errorf("cannot upgrade lock: other transactions hold shared lock")
					}
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
			return fmt.Errorf("deadlock detected")
		}
	}

	// 记录事务持有的锁
	lm.txnLocks[txID] = append(lm.txnLocks[txID], resourceID)

	return nil
}

// ReleaseLocks 释放事务持有的所有锁
func (lm *LockManager) ReleaseLocks(txID uint64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// 获取事务持有的所有资源ID
	resourceIDs := lm.txnLocks[txID]
	delete(lm.txnLocks, txID)

	// 释放每个资源上的锁
	for _, resourceID := range resourceIDs {
		info := lm.lockTable[resourceID]
		if info == nil {
			continue
		}

		// 移除该事务的锁请求
		var newRequests []*LockRequest
		for _, req := range info.Requests {
			if req.TxID != txID {
				newRequests = append(newRequests, req)
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
	}

	// 从等待图中移除事务
	lm.removeFromWaitGraph(txID)
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
			waiting.Granted = true
			grantedLocks = append(grantedLocks, waiting)
			// 通知等待的事务
			select {
			case waiting.WaitChan <- true:
			default:
			}
		}
	}
}
