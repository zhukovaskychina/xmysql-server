package metadata

// QuerySchema 表示查询结果的schema信息
// 用于火山模型执行器中的算子输出schema
type QuerySchema struct {
	// Columns 输出列的元数据
	Columns []*QueryColumn

	// TableName 源表名（如果是单表查询）
	TableName string

	// SchemaName 源schema名（如果是单表查询）
	SchemaName string
}

// QueryColumn 表示查询结果中的一列
type QueryColumn struct {
	// Name 列名
	Name string

	// DataType 数据类型
	DataType DataType

	// IsNullable 是否可为NULL
	IsNullable bool

	// TableName 源表名
	TableName string

	// SchemaName 源schema名
	SchemaName string

	// OrdinalPosition 列的位置（从1开始）
	OrdinalPosition int

	// CharMaxLength 字符类型的最大长度
	CharMaxLength int

	// Comment 列注释
	Comment string
}

// NewQuerySchema 创建新的QuerySchema
func NewQuerySchema() *QuerySchema {
	return &QuerySchema{
		Columns: make([]*QueryColumn, 0),
	}
}

// AddColumn 添加列到schema
func (qs *QuerySchema) AddColumn(col *QueryColumn) {
	col.OrdinalPosition = len(qs.Columns) + 1
	qs.Columns = append(qs.Columns, col)
}

// GetColumn 根据名称获取列
func (qs *QuerySchema) GetColumn(name string) (*QueryColumn, bool) {
	for _, col := range qs.Columns {
		if col.Name == name {
			return col, true
		}
	}
	return nil, false
}

// GetColumnByIndex 根据索引获取列（0-based）
func (qs *QuerySchema) GetColumnByIndex(idx int) (*QueryColumn, bool) {
	if idx < 0 || idx >= len(qs.Columns) {
		return nil, false
	}
	return qs.Columns[idx], true
}

// ColumnCount 返回列数
func (qs *QuerySchema) ColumnCount() int {
	return len(qs.Columns)
}

// NewQueryColumn 创建新的QueryColumn
func NewQueryColumn(name string, dataType DataType) *QueryColumn {
	return &QueryColumn{
		Name:       name,
		DataType:   dataType,
		IsNullable: true, // 默认可为NULL
	}
}

// FromTable 从Table创建QuerySchema
func FromTable(table *Table) *QuerySchema {
	schema := NewQuerySchema()
	schema.TableName = table.Name
	if table.Schema != nil {
		schema.SchemaName = table.Schema.Name
	}

	for _, col := range table.Columns {
		queryCol := &QueryColumn{
			Name:            col.Name,
			DataType:        col.DataType,
			IsNullable:      col.IsNullable,
			TableName:       table.Name,
			SchemaName:      schema.SchemaName,
			OrdinalPosition: col.OrdinalPosition,
			CharMaxLength:   col.CharMaxLength,
			Comment:         col.Comment,
		}
		schema.Columns = append(schema.Columns, queryCol)
	}

	return schema
}

// MergeSchemas 合并多个schema（用于JOIN操作）
func MergeSchemas(schemas ...*QuerySchema) *QuerySchema {
	merged := NewQuerySchema()

	for _, schema := range schemas {
		for _, col := range schema.Columns {
			// 创建新的列副本
			newCol := &QueryColumn{
				Name:          col.Name,
				DataType:      col.DataType,
				IsNullable:    col.IsNullable,
				TableName:     col.TableName,
				SchemaName:    col.SchemaName,
				CharMaxLength: col.CharMaxLength,
				Comment:       col.Comment,
			}
			merged.AddColumn(newCol)
		}
	}

	return merged
}

// ProjectSchema 投影schema（选择部分列）
func ProjectSchema(source *QuerySchema, columnIndices []int) *QuerySchema {
	projected := NewQuerySchema()
	projected.TableName = source.TableName
	projected.SchemaName = source.SchemaName

	for _, idx := range columnIndices {
		if idx >= 0 && idx < len(source.Columns) {
			col := source.Columns[idx]
			// 创建新的列副本
			newCol := &QueryColumn{
				Name:          col.Name,
				DataType:      col.DataType,
				IsNullable:    col.IsNullable,
				TableName:     col.TableName,
				SchemaName:    col.SchemaName,
				CharMaxLength: col.CharMaxLength,
				Comment:       col.Comment,
			}
			projected.AddColumn(newCol)
		}
	}

	return projected
}

// Clone 克隆schema
func (qs *QuerySchema) Clone() *QuerySchema {
	cloned := NewQuerySchema()
	cloned.TableName = qs.TableName
	cloned.SchemaName = qs.SchemaName

	for _, col := range qs.Columns {
		newCol := &QueryColumn{
			Name:            col.Name,
			DataType:        col.DataType,
			IsNullable:      col.IsNullable,
			TableName:       col.TableName,
			SchemaName:      col.SchemaName,
			OrdinalPosition: col.OrdinalPosition,
			CharMaxLength:   col.CharMaxLength,
			Comment:         col.Comment,
		}
		cloned.Columns = append(cloned.Columns, newCol)
	}

	return cloned
}
