package metadata

import (
	"context"
)

// InfoSchemaManager 定义MySQL的information schema管理器
type InfoSchemaManager interface {
	// Schema相关操作
	// GetSchemaByName 获取指定名称的schema
	GetSchemaByName(ctx context.Context, name string) (Schema, error)

	// HasSchema 检查schema是否存在
	HasSchema(ctx context.Context, name string) bool

	// GetAllSchemaNames 获取所有schema名称
	GetAllSchemaNames(ctx context.Context) ([]string, error)

	// GetAllSchemas 获取所有schema对象
	GetAllSchemas(ctx context.Context) ([]Schema, error)

	// CreateSchema 创建新的schema
	CreateSchema(ctx context.Context, schema Schema) error

	// DropSchema 删除指定schema
	DropSchema(ctx context.Context, name string) error

	// Table相关操作
	// GetTableByName 获取指定schema下的表
	GetTableByName(ctx context.Context, schemaName, tableName string) (*Table, error)

	// HasTable 检查表是否存在
	HasTable(ctx context.Context, schemaName, tableName string) bool

	// GetAllTables 获取指定schema下的所有表
	GetAllTables(ctx context.Context, schemaName string) ([]*Table, error)

	// CreateTable 在指定schema下创建表
	CreateTable(ctx context.Context, schemaName string, table *Table) error

	// DropTable 删除指定schema下的表
	DropTable(ctx context.Context, schemaName, tableName string) error

	// 元数据操作
	// RefreshMetadata 刷新指定schema的元数据
	RefreshMetadata(ctx context.Context, schemaName string) error

	// GetTableMetadata 获取表的元数据（统一返回 *metadata.TableMeta）
	GetTableMetadata(ctx context.Context, schemaName, tableName string) (*TableMeta, error)

	// 统计信息
	// GetTableStats 获取表的统计信息
	GetTableStats(ctx context.Context, schemaName, tableName string) (*InfoTableStats, error)

	// UpdateTableStats 更新表的统计信息
	UpdateTableStats(ctx context.Context, schemaName, tableName string, stats *InfoTableStats) error
	DatabaseExists(name string) (bool, error)
	DropDatabase(name string) bool
}

// Schema 数据库schema定义
type Schema interface {
	// GetName 获取schema名称
	GetName() string

	// GetCharset 获取字符集
	GetCharset() string

	// GetCollation 获取排序规则
	GetCollation() string

	// GetTables 获取所有表
	GetTables() []*Table
}

// InfoTableStats 表统计信息
type InfoTableStats struct {
	RowCount    uint64           // 总行数
	AvgRowSize  uint32           // 平均行大小
	DataSize    uint64           // 数据大小
	IndexSize   uint64           // 索引大小
	ColumnStats map[string]Stats // 列统计信息
	IndexStats  map[string]Stats // 索引统计信息
}

// Stats 统计信息
type Stats struct {
	DistinctCount uint64      // 不同值数量
	NullCount     uint64      // 空值数量
	AvgLength     float64     // 平均长度
	MinValue      interface{} // 最小值
	MaxValue      interface{} // 最大值
}
