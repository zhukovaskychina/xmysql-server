package manager

import "time"

// SchemaStats Schema统计信息
type SchemaStats struct {
	TotalDatabases uint32        // 数据库总数
	TotalTables    uint32        // 表总数
	TotalIndexes   uint32        // 索引总数
	TotalColumns   uint32        // 列总数
	AvgTableSize   uint64        // 平均表大小
	LastUpdate     time.Time     // 最后更新时间
	CacheHitRate   float64       // 缓存命中率
	LoadTime       time.Duration // 加载时间
}

// TableStats 表统计信息
type TableStats struct {
	TableID      uint64    // 表ID
	RowCount     uint64    // 行数
	DataSize     uint64    // 数据大小
	IndexSize    uint64    // 索引大小
	LastAnalyze  time.Time // 最后分析时间
	ModifyCount  uint64    // 修改次数
	ReadCount    uint64    // 读取次数
	CacheHitRate float64   // 缓存命中率
}

// IndexStats 索引统计信息
type IndexStats struct {
	IndexID          uint64    // 索引ID
	CardinalityCount uint64    // 基数
	LeafPages        uint32    // 叶子页数
	TreeHeight       uint8     // 树高
	LastAnalyze      time.Time // 最后分析时间
	ScanCount        uint64    // 扫描次数
	HitCount         uint64    // 命中次数
}

// ColumnStats 列统计信息
type ColumnStats struct {
	ColumnID       uint32    // 列ID
	DistinctValues uint64    // 不同值数量
	NullCount      uint64    // NULL值数量
	AvgLength      float64   // 平均长度
	MaxValue       []byte    // 最大值
	MinValue       []byte    // 最小值
	LastAnalyze    time.Time // 最后分析时间
}

// SchemaObject Schema对象
type SchemaObject struct {
	ID         uint64    // 对象ID
	Name       string    // 对象名
	Type       uint8     // 对象类型
	CreateTime time.Time // 创建时间
	UpdateTime time.Time // 更新时间
	Version    uint32    // 版本号
}

// SchemaConfig Schema配置
type SchemaConfig struct {
	CacheSize       uint32        // 缓存大小
	AutoAnalyze     bool          // 自动分析
	AnalyzeInterval time.Duration // 分析间隔
	StatsRetention  time.Duration // 统计信息保留时间
	MaxTableSize    uint64        // 最大表大小
	MaxIndexCount   uint32        // 最大索引数
	MaxColumnCount  uint32        // 最大列数
}

// 对象类型
const (
	SCHEMA_OBJECT_DATABASE uint8 = iota
	SCHEMA_OBJECT_TABLE
	SCHEMA_OBJECT_INDEX
	SCHEMA_OBJECT_VIEW
	SCHEMA_OBJECT_TRIGGER
	SCHEMA_OBJECT_PROCEDURE
)

// SchemaError Schema错误
type SchemaError struct {
	Code    uint32 // 错误码
	Message string // 错误信息
	Object  string // 对象名
}

// SchemaEvent Schema事件
type SchemaEvent struct {
	Time     time.Time     // 事件时间
	Type     uint8         // 事件类型
	Object   SchemaObject  // 对象信息
	User     string        // 用户
	Duration time.Duration // 持续时间
}

// 事件类型
const (
	SCHEMA_EVENT_CREATE uint8 = iota
	SCHEMA_EVENT_ALTER
	SCHEMA_EVENT_DROP
	SCHEMA_EVENT_TRUNCATE
	SCHEMA_EVENT_ANALYZE
)
