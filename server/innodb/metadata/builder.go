package metadata

// TableBuilder is a builder for creating Table objects
// 用于构建 Table 对象的构建器
type TableBuilder struct {
	table *Table
}

// NewTableBuilder creates a new TableBuilder
func NewTableBuilder(name string) *TableBuilder {
	return &TableBuilder{
		table: NewTable(name),
	}
}

// WithEngine sets the storage engine
func (b *TableBuilder) WithEngine(engine string) *TableBuilder {
	b.table.Engine = engine
	return b
}

// WithCharset sets the character set
func (b *TableBuilder) WithCharset(charset string) *TableBuilder {
	b.table.Charset = charset
	return b
}

// WithCollation sets the collation
func (b *TableBuilder) WithCollation(collation string) *TableBuilder {
	b.table.Collation = collation
	return b
}

// WithComment sets the table comment
func (b *TableBuilder) WithComment(comment string) *TableBuilder {
	b.table.Comment = comment
	return b
}

// AddColumn adds a column to the table
func (b *TableBuilder) AddColumn(name string, dataType DataType, options ...ColumnOption) *TableBuilder {
	col := &Column{
		Name:     name,
		DataType: dataType,
	}

	// Apply column options
	for _, opt := range options {
		opt(col)
	}

	b.table.AddColumn(col)
	return b
}

// AddPrimaryKey adds a primary key constraint
func (b *TableBuilder) AddPrimaryKey(columns ...string) *TableBuilder {
	idx := &Index{
		Name:      "PRIMARY",
		Columns:   columns,
		IsPrimary: true,
		IsUnique:  true,
	}
	b.table.AddIndex(idx)
	return b
}

// AddIndex adds an index
func (b *TableBuilder) AddIndex(name string, unique bool, columns ...string) *TableBuilder {
	idx := &Index{
		Name:     name,
		Columns:  columns,
		IsUnique: unique,
	}
	b.table.AddIndex(idx)
	return b
}

// Build validates and returns the built Table
func (b *TableBuilder) Build() (*Table, error) {
	if err := b.table.Validate(); err != nil {
		return nil, err
	}
	return b.table, nil
}

// ColumnOption is a function that modifies a Column
// 用于修改 Column 的函数类型
type ColumnOption func(*Column)

// WithLength sets the maximum length for string/binary types
func WithLength(length int) ColumnOption {
	return func(c *Column) {
		c.CharMaxLength = length
	}
}

// Nullable marks the column as nullable
func Nullable() ColumnOption {
	return func(c *Column) {
		c.IsNullable = true
	}
}

// WithDefault sets the default value
func WithDefault(value interface{}) ColumnOption {
	return func(c *Column) {
		c.DefaultValue = value
	}
}

// AutoIncrement marks the column as auto-increment
func AutoIncrement() ColumnOption {
	return func(c *Column) {
		c.IsAutoIncrement = true
	}
}

// Unsigned marks the column as unsigned
func Unsigned() ColumnOption {
	return func(c *Column) {
		c.IsUnsigned = true
	}
}

// WithCharset sets the character set
func WithCharset(charset string) ColumnOption {
	return func(c *Column) {
		c.Charset = charset
	}
}

// WithCollation sets the collation
func WithCollation(collation string) ColumnOption {
	return func(c *Column) {
		c.Collation = collation
	}
}

// WithComment sets the column comment
func WithComment(comment string) ColumnOption {
	return func(c *Column) {
		c.Comment = comment
	}
}

// SchemaBuilder is a builder for creating DatabaseSchema objects
// 用于构建 DatabaseSchema 对象的构建器
type SchemaBuilder struct {
	schema *DatabaseSchema
}

// NewSchemaBuilder creates a new SchemaBuilder
func NewSchemaBuilder(name string) *SchemaBuilder {
	return &SchemaBuilder{
		schema: NewSchema(name),
	}
}

// WithCharset sets the default character set for the schema
func (b *SchemaBuilder) WithCharset(charset string) *SchemaBuilder {
	b.schema.Charset = charset
	return b
}

// AddTable adds a table to the schema
func (b *SchemaBuilder) AddTable(table *Table) *SchemaBuilder {
	_ = b.schema.AddTable(table) // Ignore error for builder pattern
	return b
}

// Build returns the built DatabaseSchema
func (b *SchemaBuilder) Build() *DatabaseSchema {
	return b.schema
}
