package mvcc

import (
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	formatmvcc "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/format/mvcc"
)

// IMVCCPage MVCC页面接口
// 统一了wrapper/page/MVCCPage和wrapper/mvcc/MVCCPage的定义
type IMVCCPage interface {
	basic.IPage

	// 版本控制
	GetVersion() uint64
	SetVersion(version uint64)

	// 事务ID管理
	GetTxID() uint64
	SetTxID(txID uint64)

	// 回滚指针管理
	GetRollPtr() []byte
	SetRollPtr(ptr []byte)

	// 快照管理
	CreateSnapshot() (*PageSnapshot, error)
	RestoreSnapshot(snap *PageSnapshot) error

	// 锁管理
	AcquireLock(txID uint64, mode LockMode) error
	ReleaseLock(txID uint64) error

	// 序列化支持
	ParseFromBytes(data []byte) error
	ToBytes() ([]byte, error)
}

// RecordVersionManager 记录版本管理器接口
type RecordVersionManager interface {
	// 版本链管理
	AddVersion(record *formatmvcc.RecordVersion) error
	GetLatestVersion(key basic.Value) (*formatmvcc.RecordVersion, error)
	GetVersionByTxID(key basic.Value, txID uint64) (*formatmvcc.RecordVersion, error)
	GetVisibleVersion(key basic.Value, readView *formatmvcc.ReadView) (*formatmvcc.RecordVersion, error)

	// 垃圾回收
	PurgeOldVersions(minTxID uint64) error
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
	IsVisible(record *formatmvcc.RecordVersion, readView *formatmvcc.ReadView) bool
	GetReadView(txID uint64) (*formatmvcc.ReadView, error)
	CreateReadView(txID uint64, activeTxIDs []uint64, nextTxID uint64) (*formatmvcc.ReadView, error)

	// 隔离级别支持
	SetIsolationLevel(level IsolationLevel)
	GetIsolationLevel() IsolationLevel
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
