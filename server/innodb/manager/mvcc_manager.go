package manager

import (
	"errors"
	mvcc2 "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/mvcc"
	"sync"
	"time"
)

var (
	ErrTooManyTransactions = errors.New("too many active transactions")
	ErrTransactionNotFound = errors.New("transaction not found")
)

// MVCCManager MVCC管理器
type MVCCManager struct {
	sync.RWMutex

	// MVCC控制
	mvcc *mvcc2.Mvcc

	// 活跃事务
	activeTxs map[uint64]*TransactionInfo

	// 事务ID生成器
	nextTxID uint64

	// 配置
	config *MVCCConfig
}

// MVCCConfig MVCC配置
type MVCCConfig struct {
	// 事务超时时间
	TxTimeout time.Duration

	// 最大活跃事务数
	MaxActiveTxs int

	// 快照保留时间
	SnapshotRetention time.Duration
}

// TransactionInfo 事务信息
type TransactionInfo struct {
	ID        uint64
	StartTime time.Time
	ReadView  *mvcc2.ReadView
	State     TxState
}

// TxState 事务状态
type TxState int

const (
	TxStateActive TxState = iota
	TxStateCommitting
	TxStateCommitted
	TxStateRollback
)

// NewMVCCManager 创建MVCC管理器
func NewMVCCManager(config *MVCCConfig) *MVCCManager {
	return &MVCCManager{
		mvcc:      &mvcc2.Mvcc{},
		activeTxs: make(map[uint64]*TransactionInfo),
		config:    config,
	}
}

// BeginTransaction 开始事务
func (m *MVCCManager) BeginTransaction() (uint64, error) {
	m.Lock()
	defer m.Unlock()

	// 检查活跃事务数
	if len(m.activeTxs) >= m.config.MaxActiveTxs {
		return 0, ErrTooManyTransactions
	}

	// 生成事务ID
	m.nextTxID++
	txID := m.nextTxID

	// 创建ReadView
	view, _ := m.mvcc.CreateView()

	// 记录事务信息
	m.activeTxs[txID] = &TransactionInfo{
		ID:        txID,
		StartTime: time.Now(),
		ReadView:  view,
		State:     TxStateActive,
	}

	return txID, nil
}

// CommitTransaction 提交事务
func (m *MVCCManager) CommitTransaction(txID uint64) error {
	m.Lock()
	defer m.Unlock()

	tx, ok := m.activeTxs[txID]
	if !ok {
		return ErrTransactionNotFound
	}

	// 更新事务状态
	tx.State = TxStateCommitting

	// 关闭ReadView
	m.mvcc.CloseView(tx.ReadView, false)

	// 移除事务记录
	delete(m.activeTxs, txID)

	return nil
}

// RollbackTransaction 回滚事务
func (m *MVCCManager) RollbackTransaction(txID uint64) error {
	m.Lock()
	defer m.Unlock()

	tx, ok := m.activeTxs[txID]
	if !ok {
		return ErrTransactionNotFound
	}

	// 更新事务状态
	tx.State = TxStateRollback

	// 关闭ReadView
	m.mvcc.CloseView(tx.ReadView, false)

	// 移除事务记录
	delete(m.activeTxs, txID)

	return nil
}

// GetTransactionReadView 获取事务的ReadView
func (m *MVCCManager) GetTransactionReadView(txID uint64) (*mvcc2.ReadView, error) {
	m.RLock()
	defer m.RUnlock()

	tx, ok := m.activeTxs[txID]
	if !ok {
		return nil, ErrTransactionNotFound
	}

	return tx.ReadView, nil
}

// IsVisible 判断某个版本是否对事务可见
func (m *MVCCManager) IsVisible(txID uint64, version uint64) (bool, error) {
	_, err := m.GetTransactionReadView(txID)
	if err != nil {
		return false, err
	}

	// 使用ReadView判断可见性
	// TODO: 实现ReadView的可见性判断逻辑

	return true, nil
}

// CleanupExpiredTransactions 清理过期事务
func (m *MVCCManager) CleanupExpiredTransactions() {
	m.Lock()
	defer m.Unlock()

	now := time.Now()
	for txID, tx := range m.activeTxs {
		if now.Sub(tx.StartTime) > m.config.TxTimeout {
			// 回滚超时事务
			_ = m.RollbackTransaction(txID)
		}
	}
}

// GetActiveTransactionCount 获取活跃事务数
func (m *MVCCManager) GetActiveTransactionCount() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.activeTxs)
}

// GetTransactionState 获取事务状态
func (m *MVCCManager) GetTransactionState(txID uint64) (TxState, error) {
	m.RLock()
	defer m.RUnlock()

	tx, ok := m.activeTxs[txID]
	if !ok {
		return TxStateRollback, ErrTransactionNotFound
	}

	return tx.State, nil
}
