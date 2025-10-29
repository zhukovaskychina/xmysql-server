package manager

import "time"

// LockGranularity 锁粒度类型 (TXN-012 新增)
type LockGranularity int

const (
	LOCK_RECORD           LockGranularity = iota // 记录锁(Record Lock)
	LOCK_GAP                                     // 间隙锁(Gap Lock)
	LOCK_NEXT_KEY                                // Next-Key锁(Record + Gap)
	LOCK_INSERT_INTENTION                        // 插入意向锁
)

// LockStats 锁统计信息
type LockStats struct {
	TotalLocks     uint64        // 总锁数
	GrantedLocks   uint64        // 已授予锁数
	WaitingLocks   uint64        // 等待中锁数
	Deadlocks      uint64        // 死锁次数
	AvgWaitTime    time.Duration // 平均等待时间
	MaxWaitTime    time.Duration // 最长等待时间
	LockTimeouts   uint64        // 锁超时次数
	LockConflicts  uint64        // 锁冲突次数
	RecordLocks    uint64        // 行锁数
	TableLocks     uint64        // 表锁数
	SharedLocks    uint64        // 共享锁数
	ExclusiveLocks uint64        // 排他锁数
}

// LockConfig 锁配置
type LockConfig struct {
	DeadlockInterval  time.Duration // 死锁检测间隔
	LockTimeout       time.Duration // 锁超时时间
	MaxLockWaitTime   time.Duration // 最大等待时间
	MaxDeadlockDepth  int           // 最大死锁检测深度
	MaxLocksPerTxn    uint32        // 每个事务最大锁数
	EnableTableLocks  bool          // 是否启用表锁
	EnableRecordLocks bool          // 是否启用行锁
}

// LockWaiter 锁等待者
type LockWaiter struct {
	TxID       uint64    // 事务ID
	ResourceID string    // 资源ID
	LockType   LockType  // 锁类型
	Mode       LockMode  // 锁模式
	WaitStart  time.Time // 等待开始时间
	Timeout    bool      // 是否超时
}

// LockOwner 锁持有者
type LockOwner struct {
	TxID       uint64    // 事务ID
	ResourceID string    // 资源ID
	LockType   LockType  // 锁类型
	Mode       LockMode  // 锁模式
	GrantTime  time.Time // 授予时间
}

// DeadlockInfo 死锁信息
type DeadlockInfo struct {
	DetectedAt   time.Time     // 检测时间
	WaitingTxns  []uint64      // 等待事务列表
	Cycle        []uint64      // 死锁环
	VictimTxID   uint64        // 牺牲事务
	WaitDuration time.Duration // 等待时长
}

// 锁优先级
const (
	LOCK_PRIORITY_LOW    uint8 = iota // 低优先级
	LOCK_PRIORITY_NORMAL              // 普通优先级
	LOCK_PRIORITY_HIGH                // 高优先级
)

// 锁状态
const (
	LOCK_STATUS_WAITING  uint8 = iota // 等待中
	LOCK_STATUS_GRANTED               // 已授予
	LOCK_STATUS_TIMEOUT               // 超时
	LOCK_STATUS_DEADLOCK              // 死锁
)

// LockResult 锁操作结果
type LockResult struct {
	Success    bool          // 是否成功
	Status     uint8         // 状态
	WaitTime   time.Duration // 等待时间
	Error      error         // 错误信息
	Deadlock   *DeadlockInfo // 死锁信息
	ResourceID string        // 资源ID
}

// ============ TXN-012: Gap锁和Next-Key锁相关类型 ============

// GapRange 间隙范围
type GapRange struct {
	LowerBound interface{} // 下界值 (不包含)
	UpperBound interface{} // 上界值 (不包含)
	TableID    uint32      // 表ID
	IndexID    uint32      // 索引ID
}

// LockRange 锁范围信息
type LockRange struct {
	Granularity LockGranularity // 锁粒度
	RecordKey   interface{}     // 记录键值 (Record Lock)
	GapRange    *GapRange       // 间隙范围 (Gap Lock/Next-Key Lock)
	TableID     uint32          // 表ID
	PageID      uint32          // 页ID
	RowID       uint64          // 行ID
}

// GapLockInfo Gap锁信息
type GapLockInfo struct {
	TxID       uint64    // 事务ID
	LockType   LockType  // 锁类型 (S/X)
	GapRange   *GapRange // 间隙范围
	Granted    bool      // 是否已授予
	WaitChan   chan bool // 等待通道
	CreateTime time.Time // 创建时间
}

// NextKeyLockInfo Next-Key锁信息
type NextKeyLockInfo struct {
	TxID       uint64      // 事务ID
	LockType   LockType    // 锁类型 (S/X)
	RecordKey  interface{} // 记录键值
	GapRange   *GapRange   // 间隙范围 (记录之前的gap)
	Granted    bool        // 是否已授予
	WaitChan   chan bool   // 等待通道
	CreateTime time.Time   // 创建时间
}

// InsertIntentionLockInfo 插入意向锁信息
type InsertIntentionLockInfo struct {
	TxID       uint64      // 事务ID
	InsertKey  interface{} // 待插入的键值
	GapRange   *GapRange   // 目标间隙
	Granted    bool        // 是否已授予
	WaitChan   chan bool   // 等待通道
	CreateTime time.Time   // 创建时间
}
