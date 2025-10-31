package manager

import "time"

// UndoLogEntry Undo日志条目
type UndoLogEntry struct {
	LSN       uint64    // 日志序列号
	TrxID     int64     // 事务ID
	TableID   uint64    // 表ID
	RecordID  uint64    // 记录ID
	Type      uint8     // 操作类型
	Data      []byte    // 操作数据
	Timestamp time.Time // 时间戳
}

// RedoLogEntry Redo日志条目
type RedoLogEntry struct {
	LSN       uint64    // 日志序列号
	TrxID     int64     // 事务ID
	PageID    uint64    // 页面ID
	Type      uint8     // 操作类型
	Data      []byte    // 操作数据
	Timestamp time.Time // 时间戳
}

// 日志操作类型
const (
	LOG_TYPE_INSERT     uint8 = iota + 1 // INSERT操作
	LOG_TYPE_UPDATE                      // UPDATE操作
	LOG_TYPE_DELETE                      // DELETE操作
	LOG_TYPE_COMPENSATE                  // 补偿日志（CLR）

	// 页面操作类型
	LOG_TYPE_PAGE_CREATE // 创建页面
	LOG_TYPE_PAGE_DELETE // 删除页面
	LOG_TYPE_PAGE_MODIFY // 修改页面
	LOG_TYPE_PAGE_SPLIT  // 页面分裂
	LOG_TYPE_PAGE_MERGE  // 页面合并

	// 索引操作类型
	LOG_TYPE_INDEX_INSERT // 索引插入
	LOG_TYPE_INDEX_DELETE // 索引删除
	LOG_TYPE_INDEX_UPDATE // 索引更新

	// 事务操作类型
	LOG_TYPE_TXN_BEGIN     // 事务开始
	LOG_TYPE_TXN_COMMIT    // 事务提交
	LOG_TYPE_TXN_ROLLBACK  // 事务回滚
	LOG_TYPE_TXN_SAVEPOINT // 保存点

	// 系统操作类型
	LOG_TYPE_CHECKPOINT  // 检查点
	LOG_TYPE_FILE_CREATE // 文件创建
	LOG_TYPE_FILE_DELETE // 文件删除
	LOG_TYPE_FILE_EXTEND // 文件扩展
)

// LogStats 日志统计信息
type LogStats struct {
	TotalLogs     uint64        // 总日志数
	TotalSize     uint64        // 总大小
	AvgLogSize    uint64        // 平均日志大小
	WriteLatency  time.Duration // 写入延迟
	FlushLatency  time.Duration // 刷新延迟
	LogsPerSecond float64       // 每秒日志数
}

// LogConfig 日志配置
type LogConfig struct {
	LogDir          string        // 日志目录
	MaxFileSize     uint64        // 单个文件最大大小
	FlushInterval   time.Duration // 刷新间隔
	RetentionPeriod time.Duration // 保留期限
	SyncMode        string        // 同步模式
	Compression     bool          // 是否压缩
	BufferSize      uint32        // 缓冲区大小
}
