package mvcc

import (
	"time"
	"xmysql-server/server/innodb/basic"
)

// MVCCPage MVCC页面接口
type MVCCPage interface {
	basic.IPage

	// 版本控制
	GetVersion() uint64
	SetVersion(version uint64)

	// 事务管理
	GetTxID() uint64
	SetTxID(txID uint64)

	// 快照管理
	CreateSnapshot() (*PageSnapshot, error)
	RestoreSnapshot(snap *PageSnapshot) error

	// 锁管理
	AcquireLock(txID uint64, mode LockMode) error
	ReleaseLock(txID uint64) error
}

// RecordVersionManager 记录版本管理器接口
type RecordVersionManager interface {
	// 版本链管理
	AddVersion(record *RecordVersion) error
	GetLatestVersion(key basic.Value) (*RecordVersion, error)
	GetVersionByTxID(key basic.Value, txID uint64) (*RecordVersion, error)
	GetVisibleVersion(key basic.Value, txID uint64, readTS time.Time) (*RecordVersion, error)

	// 垃圾回收
	PurgeOldVersions(beforeTS time.Time) error
	GetVersionChainLength(key basic.Value) int
}

// SnapshotManager 快照管理器接口
type SnapshotManager interface {
	// 快照创建与恢复
	CreateSnapshot(pageID uint32, txID uint64) (*PageSnapshot, error)
	RestoreSnapshot(snap *PageSnapshot) error
	DeleteSnapshot(pageID uint32, version uint64) error

	// 快照查询
	GetSnapshot(pageID uint32, version uint64) (*PageSnapshot, error)
	GetLatestSnapshot(pageID uint32) (*PageSnapshot, error)
	ListSnapshots(pageID uint32) ([]*PageSnapshot, error)

	// 快照清理
	CleanupExpiredSnapshots(maxAge time.Duration) error
	GetSnapshotCount() int
	GetTotalSnapshotSize() int64
}

// LockManager 锁管理器接口
type LockManager interface {
	// 锁获取与释放
	AcquireLock(txID uint64, resource string, mode LockMode) error
	ReleaseLock(txID uint64, resource string) error
	ReleaseAllLocks(txID uint64) error

	// 锁查询
	GetLockMode(txID uint64, resource string) (LockMode, error)
	IsLocked(resource string) bool
	GetLockHolders(resource string) ([]uint64, error)

	// 死锁检测
	DetectDeadlock() ([]uint64, error)
	SetDeadlockTimeout(timeout time.Duration)
}

// TransactionVisibility 事务可见性接口
type TransactionVisibility interface {
	// 可见性检查
	IsVisible(record *RecordVersion, txID uint64, readTS time.Time) bool
	GetReadView(txID uint64) (*ReadView, error)
	CreateReadView(txID uint64) (*ReadView, error)

	// 隔离级别支持
	SetIsolationLevel(level IsolationLevel)
	GetIsolationLevel() IsolationLevel
}

// ReadView 读取视图，用于MVCC可见性判断
type ReadView struct {
	TxID          uint64    // 当前事务ID
	CreateTS      time.Time // 创建时间
	LowWaterMark  uint64    // 最小活跃事务ID
	HighWaterMark uint64    // 最大事务ID
	ActiveTxIDs   []uint64  // 活跃事务ID列表
}

// IsolationLevel 事务隔离级别
type IsolationLevel int

const (
	// IsolationReadUncommitted 读未提交
	IsolationReadUncommitted IsolationLevel = iota

	// IsolationReadCommitted 读已提交
	IsolationReadCommitted

	// IsolationRepeatableRead 可重复读
	IsolationRepeatableRead

	// IsolationSerializable 串行化
	IsolationSerializable
)

// String 返回隔离级别的字符串表示
func (il IsolationLevel) String() string {
	switch il {
	case IsolationReadUncommitted:
		return "READ_UNCOMMITTED"
	case IsolationReadCommitted:
		return "READ_COMMITTED"
	case IsolationRepeatableRead:
		return "REPEATABLE_READ"
	case IsolationSerializable:
		return "SERIALIZABLE"
	default:
		return "UNKNOWN"
	}
}

// NewReadView 创建新的读取视图
func NewReadView(txID uint64, activeTxIDs []uint64) *ReadView {
	rv := &ReadView{
		TxID:        txID,
		CreateTS:    time.Now(),
		ActiveTxIDs: make([]uint64, len(activeTxIDs)),
	}

	copy(rv.ActiveTxIDs, activeTxIDs)

	// 计算水位线
	if len(activeTxIDs) > 0 {
		rv.LowWaterMark = activeTxIDs[0]
		rv.HighWaterMark = activeTxIDs[len(activeTxIDs)-1]

		for _, txID := range activeTxIDs {
			if txID < rv.LowWaterMark {
				rv.LowWaterMark = txID
			}
			if txID > rv.HighWaterMark {
				rv.HighWaterMark = txID
			}
		}
	}

	return rv
}

// IsVisible 检查事务对当前读取视图是否可见
func (rv *ReadView) IsVisible(txID uint64) bool {
	// 自己的事务总是可见
	if txID == rv.TxID {
		return true
	}

	// 小于低水位线的事务已提交，可见
	if txID < rv.LowWaterMark {
		return true
	}

	// 大于高水位线的事务还未开始，不可见
	if txID > rv.HighWaterMark {
		return false
	}

	// 在活跃事务列表中的不可见
	for _, activeTxID := range rv.ActiveTxIDs {
		if txID == activeTxID {
			return false
		}
	}

	// 其他情况可见
	return true
}
