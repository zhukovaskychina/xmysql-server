package manager

import (
	"errors"
	"sync"
	"time"

	formatmvcc "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/format/mvcc"
)

var (
	ErrTooManyTransactions = errors.New("too many active transactions")
	ErrTransactionNotFound = errors.New("transaction not found")
)

// MVCCManager MVCC管理器
type MVCCManager struct {
	sync.RWMutex

	// 活跃事务
	activeTxs map[uint64]*MVCCTransactionInfo

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

// MVCCTransactionInfo MVCC事务信息（避免与crash_recovery.go中的TransactionInfo冲突）
type MVCCTransactionInfo struct {
	ID        uint64
	StartTime time.Time
	ReadView  *formatmvcc.ReadView
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
		activeTxs: make(map[uint64]*MVCCTransactionInfo),
		config:    config,
	}
}

// BeginTransaction 开始事务
// 修复MVCC-001: 确保ReadView创建时正确捕获所有活跃事务ID
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

	// 【修复1】先将新事务加入activeTxs，确保并发事务能看到它
	// 此时ReadView为nil，稍后创建
	m.activeTxs[txID] = &MVCCTransactionInfo{
		ID:        txID,
		StartTime: time.Now(),
		ReadView:  nil, // 稍后创建
		State:     TxStateActive,
	}

	// 【修复2】基于当前所有活跃事务创建ReadView（原子快照）
	// 注意：此时activeTxs已包含当前事务，需要排除自己
	activeIDs := make([]uint64, 0, len(m.activeTxs)-1)

	for id, tx := range m.activeTxs {
		// 排除当前事务自己，只记录其他活跃事务
		if tx.State == TxStateActive && id != txID {
			activeIDs = append(activeIDs, id)
		}
	}

	// 创建ReadView
	// format/mvcc的NewReadView会自动计算lowWaterMark和highWaterMark
	view := formatmvcc.NewReadView(activeIDs, txID, m.nextTxID+1)

	// 【修复6】更新事务的ReadView
	m.activeTxs[txID].ReadView = view

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

	// 移除事务记录
	delete(m.activeTxs, txID)

	return nil
}

// GetTransactionReadView 获取事务的ReadView
func (m *MVCCManager) GetTransactionReadView(txID uint64) (*formatmvcc.ReadView, error) {
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
	rv, err := m.GetTransactionReadView(txID)
	if err != nil {
		return false, err
	}
	if rv == nil {
		// 没有ReadView则默认为可见（例如RU或管理器尚未初始化）
		return true, nil
	}
	return rv.IsVisible(version), nil
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
