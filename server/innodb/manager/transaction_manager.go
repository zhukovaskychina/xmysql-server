package manager

import (
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/mvcc"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrInvalidTrxState = errors.New("invalid transaction state")
)

// 事务状态
const (
	TRX_STATE_NOT_STARTED uint8 = iota
	TRX_STATE_ACTIVE
	TRX_STATE_PREPARED
	TRX_STATE_COMMITTED
	TRX_STATE_ROLLED_BACK
)

// 事务隔离级别
const (
	TRX_ISO_READ_UNCOMMITTED uint8 = iota
	TRX_ISO_READ_COMMITTED
	TRX_ISO_REPEATABLE_READ
	TRX_ISO_SERIALIZABLE
)

// Transaction 表示一个事务
type Transaction struct {
	ID             int64          // 事务ID
	State          uint8          // 事务状态
	IsolationLevel uint8          // 隔离级别
	StartTime      time.Time      // 开始时间
	LastActiveTime time.Time      // 最后活跃时间
	ReadView       *mvcc.ReadView // MVCC读视图
	UndoLogs       []UndoLogEntry // Undo日志
	RedoLogs       []RedoLogEntry // Redo日志
	IsReadOnly     bool           // 是否只读事务
}

// TransactionManager 事务管理器
type TransactionManager struct {
	mu                 sync.RWMutex
	nextTrxID          int64                  // 下一个事务ID
	activeTransactions map[int64]*Transaction // 活跃事务

	// 日志管理器
	redoManager *RedoLogManager
	undoManager *UndoLogManager

	// 默认配置
	defaultIsolationLevel uint8
	defaultTimeout        time.Duration
}

// NewTransactionManager 创建事务管理器
func NewTransactionManager(redoDir, undoDir string) (*TransactionManager, error) {
	redoManager, err := NewRedoLogManager(redoDir, 1000)
	if err != nil {
		return nil, err
	}

	undoManager, err := NewUndoLogManager(undoDir)
	if err != nil {
		return nil, err
	}

	return &TransactionManager{
		nextTrxID:             1,
		activeTransactions:    make(map[int64]*Transaction),
		redoManager:           redoManager,
		undoManager:           undoManager,
		defaultIsolationLevel: TRX_ISO_REPEATABLE_READ,
		defaultTimeout:        time.Hour,
	}, nil
}

// Begin 开始新事务
func (tm *TransactionManager) Begin(isReadOnly bool, isolationLevel uint8) (*Transaction, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 分配事务ID
	trxID := atomic.AddInt64(&tm.nextTrxID, 1)

	// 创建事务对象
	trx := &Transaction{
		ID:             trxID,
		State:          TRX_STATE_ACTIVE,
		IsolationLevel: isolationLevel,
		StartTime:      time.Now(),
		LastActiveTime: time.Now(),
		IsReadOnly:     isReadOnly,
	}

	// 创建ReadView（对于RR和RC隔离级别）
	if isolationLevel >= TRX_ISO_READ_COMMITTED {
		trx.ReadView = tm.createReadView(trxID)
	}

	// 记录活跃事务
	tm.activeTransactions[trxID] = trx

	return trx, nil
}

// Commit 提交事务
func (tm *TransactionManager) Commit(trx *Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 检查事务状态
	if trx.State != TRX_STATE_ACTIVE {
		return ErrInvalidTrxState
	}

	// 写入Redo日志
	for _, redoLog := range trx.RedoLogs {
		if _, err := tm.redoManager.Append(&redoLog); err != nil {
			return err
		}
	}

	// 确保Redo日志持久化
	if err := tm.redoManager.Flush(0); err != nil {
		return err
	}

	// 更新事务状态
	trx.State = TRX_STATE_COMMITTED
	trx.LastActiveTime = time.Now()

	// 清理Undo日志
	tm.undoManager.Cleanup(trx.ID)

	// 移除活跃事务记录
	delete(tm.activeTransactions, trx.ID)

	return nil
}

// Rollback 回滚事务
func (tm *TransactionManager) Rollback(trx *Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 检查事务状态
	if trx.State != TRX_STATE_ACTIVE {
		return ErrInvalidTrxState
	}

	// 执行回滚操作
	if err := tm.undoManager.Rollback(trx.ID); err != nil {
		return err
	}

	// 更新事务状态
	trx.State = TRX_STATE_ROLLED_BACK
	trx.LastActiveTime = time.Now()

	// 清理事务记录
	delete(tm.activeTransactions, trx.ID)

	return nil
}

// createReadView 创建MVCC读视图
func (tm *TransactionManager) createReadView(trxID int64) *mvcc.ReadView {
	// 获取当前活跃事务列表
	activeIDs := make([]int64, 0, len(tm.activeTransactions))
	minTrxID := int64(^uint64(0) >> 1)

	for id, trx := range tm.activeTransactions {
		if trx.State == TRX_STATE_ACTIVE && id != trxID {
			activeIDs = append(activeIDs, id)
			if id < minTrxID {
				minTrxID = id
			}
		}
	}

	return mvcc.NewReadView(activeIDs, minTrxID, tm.nextTrxID, trxID)
}

// GetTransaction 获取事务对象
func (tm *TransactionManager) GetTransaction(trxID int64) *Transaction {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.activeTransactions[trxID]
}

// IsVisible 判断数据版本是否对事务可见
func (tm *TransactionManager) IsVisible(trx *Transaction, version int64) bool {
	if trx.IsolationLevel == TRX_ISO_READ_UNCOMMITTED {
		return true
	}

	if trx.ReadView == nil {
		return true
	}

	return trx.ReadView.IsVisible(version)
}

// Cleanup 清理超时事务
func (tm *TransactionManager) Cleanup() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	timeout := tm.defaultTimeout
	now := time.Now()

	for id, trx := range tm.activeTransactions {
		if now.Sub(trx.LastActiveTime) > timeout {
			// 回滚超时事务
			tm.Rollback(trx)
			delete(tm.activeTransactions, id)
		}
	}
}

// Close 关闭事务管理器
func (tm *TransactionManager) Close() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 回滚所有未完成事务
	for _, trx := range tm.activeTransactions {
		if trx.State == TRX_STATE_ACTIVE {
			tm.Rollback(trx)
		}
	}

	// 关闭日志管理器
	if err := tm.redoManager.Close(); err != nil {
		return err
	}
	if err := tm.undoManager.Close(); err != nil {
		return err
	}

	return nil
}
