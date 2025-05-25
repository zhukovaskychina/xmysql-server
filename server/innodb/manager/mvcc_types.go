package manager

import "time"

// MVCCStats MVCC统计信息
type MVCCStats struct {
	TotalVersions    uint64        // 总版本数
	ActiveVersions   uint64        // 活跃版本数
	GarbageVersions  uint64        // 垃圾版本数
	OldestVersion    uint64        // 最老版本
	NewestVersion    uint64        // 最新版本
	AvgVersionsPerTx float64       // 每个事务平均版本数
	GCLatency        time.Duration // GC延迟
	ConflictRate     float64       // 冲突率
}

// MVCCVersion 版本信息
type MVCCVersion struct {
	TxID       uint64    // 事务ID
	CreateTime time.Time // 创建时间
	Data       []byte    // 数据
	Next       uint64    // 下一个版本
	Prev       uint64    // 上一个版本
}

// MVCCSnapshot 快照信息
type MVCCSnapshot struct {
	ID         uint64    // 快照ID
	CreateTime time.Time // 创建时间
	MinTxID    uint64    // 最小事务ID
	MaxTxID    uint64    // 最大事务ID
	TxMap      []uint64  // 活跃事务列表
}

// MVCCConflict 冲突信息
type MVCCConflict struct {
	TxID1     uint64    // 事务1 ID
	TxID2     uint64    // 事务2 ID
	Resource  string    // 冲突资源
	Time      time.Time // 冲突时间
	Operation string    // 冲突操作
}

// MVCCGCStats GC统计信息
type MVCCGCStats struct {
	LastGCTime        time.Time     // 最后GC时间
	VersionsCollected uint64        // 收集的版本数
	BytesFreed        uint64        // 释放的字节数
	Duration          time.Duration // GC持续时间
	ErrorCount        uint64        // 错误次数
}

// MVCCIsolationLevel 隔离级别
type MVCCIsolationLevel uint8

const (
	MVCC_READ_UNCOMMITTED MVCCIsolationLevel = iota
	MVCC_READ_COMMITTED
	MVCC_REPEATABLE_READ
	MVCC_SERIALIZABLE
)

// MVCCVisibility 可见性
type MVCCVisibility uint8

const (
	MVCC_VISIBLE MVCCVisibility = iota
	MVCC_INVISIBLE
	MVCC_DELETED
)

// MVCCOperation 操作类型
type MVCCOperation uint8

const (
	MVCC_INSERT MVCCOperation = iota
	MVCC_UPDATE
	MVCC_DELETE
)

// MVCCResult 操作结果
type MVCCResult struct {
	Success   bool          // 是否成功
	Version   uint64        // 版本号
	Conflict  *MVCCConflict // 冲突信息
	Error     error         // 错误信息
	Duration  time.Duration // 操作耗时
	Operation MVCCOperation // 操作类型
}
