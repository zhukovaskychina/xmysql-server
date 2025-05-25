package mvcc

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"sync"
	"time"
)

// IsolationLevel 事务隔离级别
type IsolationLevel int

const (
	ReadUncommitted IsolationLevel = iota
	ReadCommitted
	RepeatableRead
	Serializable
)

// TransactionManager 事务管理器
type TransactionManager struct {
	mu                 sync.RWMutex
	activeTransactions sync.Map // txnID -> *Transaction
	isolationLevel     IsolationLevel
	deadlockDetector   *DeadlockDetector
	nextTxnID          uint64
}

// Transaction 事务信息
type Transaction struct {
	ID              uint64
	IsolationLevel  IsolationLevel
	StartTime       time.Time
	Status          TransactionStatus
	SnapshotVersion uint64 // MVCC快照版本
	Locks           map[string]LockType
	UndoLog         []UndoLogEntry
}

// TransactionStatus 事务状态
type TransactionStatus int

const (
	Active TransactionStatus = iota
	Committed
	Aborted
)

// LockType 锁类型
type LockType int

const (
	SharedLock LockType = iota
	ExclusiveLock
)

// UndoLogEntry 回滚日志条目
type UndoLogEntry struct {
	TableID   uint64
	RowID     []byte
	Operation string // "INSERT", "UPDATE", "DELETE"
	OldValue  []byte
	UndoNext  uint64 // 链接到下一个undo log条目
}

// NewTransactionManager 创建事务管理器
func NewTransactionManager(level IsolationLevel) *TransactionManager {
	return &TransactionManager{
		isolationLevel:   level,
		deadlockDetector: NewDeadlockDetector(),
	}
}

// BeginTransaction 开始新事务
func (tm *TransactionManager) BeginTransaction(level IsolationLevel) (*Transaction, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tm.nextTxnID++
	txn := &Transaction{
		ID:             tm.nextTxnID,
		IsolationLevel: level,
		StartTime:      time.Now(),
		Status:         Active,
		Locks:          make(map[string]LockType),
	}

	// 设置快照版本(用于MVCC)
	if level >= RepeatableRead {
		txn.SnapshotVersion = getCurrentVersion()
	}

	tm.activeTransactions.Store(txn.ID, txn)
	return txn, nil
}

// CommitTransaction 提交事务
func (tm *TransactionManager) CommitTransaction(txn *Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 检查事务状态
	if txn.Status != Active {
		return basic.ErrInvalidTransactionState
	}

	// 释放所有锁
	tm.releaseAllLocks(txn)

	// 更新事务状态
	txn.Status = Committed
	tm.activeTransactions.Delete(txn.ID)

	return nil
}

// RollbackTransaction 回滚事务
func (tm *TransactionManager) RollbackTransaction(txn *Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 检查事务状态
	if txn.Status != Active {
		return basic.ErrInvalidTransactionState
	}

	// 执行undo log
	for i := len(txn.UndoLog) - 1; i >= 0; i-- {
		entry := txn.UndoLog[i]
		if err := tm.applyUndoLogEntry(&entry); err != nil {
			return err
		}
	}

	// 释放所有锁
	tm.releaseAllLocks(txn)

	// 更新事务状态
	txn.Status = Aborted
	tm.activeTransactions.Delete(txn.ID)

	return nil
}

// AcquireLock 获取锁
func (tm *TransactionManager) AcquireLock(txn *Transaction, resourceID string, lockType LockType) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 检查死锁
	if tm.deadlockDetector.WouldCauseCycle(txn.ID, resourceID) {
		return basic.ErrDeadlockDetected
	}

	// 检查锁兼容性
	if !tm.isLockCompatible(resourceID, lockType) {
		// 添加等待关系
		tm.deadlockDetector.AddWaitFor(txn.ID, tm.getLockHolder(resourceID))
		return basic.ErrLockConflict
	}

	// 授予锁
	txn.Locks[resourceID] = lockType
	return nil
}

// ReleaseLock 释放锁
func (tm *TransactionManager) ReleaseLock(txn *Transaction, resourceID string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	delete(txn.Locks, resourceID)
	// 获取资源的锁持有者ID，然后移除等待关系
	holderID := tm.getLockHolder(resourceID)
	if holderID != 0 {
		tm.deadlockDetector.RemoveWaitFor(txn.ID, holderID)
	}
}

// 释放事务的所有锁
func (tm *TransactionManager) releaseAllLocks(txn *Transaction) {
	for resourceID := range txn.Locks {
		tm.ReleaseLock(txn, resourceID)
	}
}

// 检查锁兼容性
func (tm *TransactionManager) isLockCompatible(resourceID string, lockType LockType) bool {
	var hasExclusive bool
	tm.activeTransactions.Range(func(_, v interface{}) bool {
		t := v.(*Transaction)
		if l, ok := t.Locks[resourceID]; ok {
			if l == ExclusiveLock {
				hasExclusive = true
				return false
			}
		}
		return true
	})

	if hasExclusive {
		return false
	}

	if lockType == ExclusiveLock {
		// 检查是否有其他事务持有任何类型的锁
		var hasOtherLocks bool
		tm.activeTransactions.Range(func(_, v interface{}) bool {
			t := v.(*Transaction)
			if _, ok := t.Locks[resourceID]; ok {
				hasOtherLocks = true
				return false
			}
			return true
		})
		return !hasOtherLocks
	}

	return true
}

// 获取资源的锁持有者
func (tm *TransactionManager) getLockHolder(resourceID string) uint64 {
	var holderID uint64
	tm.activeTransactions.Range(func(_, v interface{}) bool {
		t := v.(*Transaction)
		if _, ok := t.Locks[resourceID]; ok {
			holderID = t.ID
			return false
		}
		return true
	})
	return holderID
}

// getCurrentVersion 获取当前版本号
func getCurrentVersion() uint64 {
	// TODO: 实现版本号生成逻辑
	return uint64(time.Now().UnixNano())
}

// 应用undo log条目
func (tm *TransactionManager) applyUndoLogEntry(entry *UndoLogEntry) error {
	// TODO: 实现undo log应用逻辑
	return nil
}
