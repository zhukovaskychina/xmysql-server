package metadata

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// ColumnMeta contains metadata for a single column
type ColumnMeta struct {
	Name            string
	Type            DataType // 使用column_def.go中定义的DataType
	Length          int
	IsNullable      bool
	IsPrimary       bool
	IsUnique        bool
	IsAutoIncrement bool
	DefaultValue    interface{}
	Charset         string
	Collation       string
	Comment         string
}

// TableMeta contains metadata for a table
type TableMeta struct {
	Name       string
	Columns    []*ColumnMeta
	PrimaryKey []string
	Indices    []IndexMeta
	Engine     string
	Charset    string
	Collation  string
	RowFormat  string
	Comment    string
}

type TableStats struct {
}

// IndexMeta contains metadata for an index
type IndexMeta struct {
	Name    string
	Columns []string
	Unique  bool
}

// ColumnInfo represents column information for compatibility
type ColumnInfo struct {
	FieldType   string
	FieldLength int
}

// TableRowTuple defines the interface for table row metadata
type TableRowTuple interface {
	// GetColumnDescInfo gets column description information by name
	GetColumnDescInfo(colName string) (ColumnMeta, int)
	// GetTableMeta gets the table metadata
	GetTableMeta() *TableMeta
	// GetColumnCount gets the number of columns
	GetColumnCount() int
	// GetColumnMeta gets column metadata by index
	GetColumnMeta(index int) *ColumnMeta

	// Additional methods needed by record wrapper code
	GetColumnLength() int
	GetColumnInfos(index byte) ColumnInfo
	GetVarColumns() []ColumnInfo
}

// DefaultTableRow is a default implementation of TableRowTuple
type DefaultTableRow struct {
	tableMeta *TableMeta
}

// NewDefaultTableRow creates a new DefaultTableRow
func NewDefaultTableRow(meta *TableMeta) *DefaultTableRow {
	return &DefaultTableRow{
		tableMeta: meta,
	}
}

// GetColumnDescInfo implements TableRowTuple interface
func (d *DefaultTableRow) GetColumnDescInfo(colName string) (ColumnMeta, int) {
	for i, col := range d.tableMeta.Columns {
		if col.Name == colName {
			return *col, i
		}
	}
	return ColumnMeta{}, -1
}

// GetTableMeta implements TableRowTuple interface
func (d *DefaultTableRow) GetTableMeta() *TableMeta {
	return d.tableMeta
}

// GetColumnCount implements TableRowTuple interface
func (d *DefaultTableRow) GetColumnCount() int {
	if d.tableMeta == nil {
		return 0
	}
	return len(d.tableMeta.Columns)
}

// GetColumnMeta implements TableRowTuple interface
func (d *DefaultTableRow) GetColumnMeta(index int) *ColumnMeta {
	if index < 0 || index >= len(d.tableMeta.Columns) {
		return nil
	}
	return d.tableMeta.Columns[index]
}

// GetColumnLength implements TableRowTuple interface
func (d *DefaultTableRow) GetColumnLength() int {
	return d.GetColumnCount()
}

// GetColumnInfos implements TableRowTuple interface
func (d *DefaultTableRow) GetColumnInfos(index byte) ColumnInfo {
	if int(index) >= len(d.tableMeta.Columns) {
		return ColumnInfo{}
	}
	col := d.tableMeta.Columns[index]
	return ColumnInfo{
		FieldType:   string(col.Type),
		FieldLength: col.Length,
	}
}

// GetVarColumns implements TableRowTuple interface
func (d *DefaultTableRow) GetVarColumns() []ColumnInfo {
	var varCols []ColumnInfo
	for _, col := range d.tableMeta.Columns {
		if col.Type == TypeVarchar || col.Type == TypeVarBinary ||
			col.Type == TypeText || col.Type == TypeBlob {
			varCols = append(varCols, ColumnInfo{
				FieldType:   string(col.Type),
				FieldLength: col.Length,
			})
		}
	}
	return varCols
}

// ConvertToBasicValue converts a value to the basic.Value type based on column type
func (c *ColumnMeta) ConvertToBasicValue(val interface{}) (basic.Value, error) {
	// Implementation depends on your basic.Value type
	// This is a placeholder - you'll need to implement the actual conversion
	// based on your basic.Value implementation
	return nil, nil
}

// ValidateValue validates if the value matches the column type
func (c *ColumnMeta) ValidateValue(val interface{}) bool {
	// Implement validation logic based on column type
	return true
}
