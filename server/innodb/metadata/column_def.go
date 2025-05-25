package metadata

import (
	"fmt"
	"strings"
)

// Column represents a database column
// 表示数据库中的列
type Column struct {
	Table           *Table
	Name            string
	OrdinalPosition int
	DataType        DataType
	CharMaxLength   int
	IsNullable      bool
	DefaultValue    interface{}
	IsAutoIncrement bool
	IsUnsigned      bool
	IsZerofill      bool
	Charset         string
	Collation       string
	Comment         string
}

// DataType represents the SQL data type of a column
type DataType string

// Common SQL data types
const (
	TypeTinyInt    DataType = "TINYINT"
	TypeSmallInt   DataType = "SMALLINT"
	TypeMediumInt  DataType = "MEDIUMINT"
	TypeInt        DataType = "INT"
	TypeBigInt     DataType = "BIGINT"
	TypeFloat      DataType = "FLOAT"
	TypeDouble     DataType = "DOUBLE"
	TypeDecimal    DataType = "DECIMAL"
	TypeDate       DataType = "DATE"
	TypeTime       DataType = "TIME"
	TypeDateTime   DataType = "DATETIME"
	TypeTimestamp  DataType = "TIMESTAMP"
	TypeYear       DataType = "YEAR"
	TypeChar       DataType = "CHAR"
	TypeVarchar    DataType = "VARCHAR"
	TypeBinary     DataType = "BINARY"
	TypeVarBinary  DataType = "VARBINARY"
	TypeTinyBlob   DataType = "TINYBLOB"
	TypeBlob       DataType = "BLOB"
	TypeMediumBlob DataType = "MEDIUMBLOB"
	TypeLongBlob   DataType = "LONGBLOB"
	TypeTinyText   DataType = "TINYTEXT"
	TypeText       DataType = "TEXT"
	TypeMediumText DataType = "MEDIUMTEXT"
	TypeLongText   DataType = "LONGTEXT"
	TypeEnum       DataType = "ENUM"
	TypeSet        DataType = "SET"
	TypeJSON       DataType = "JSON"
	TypeBool       DataType = "BOOL"
	TypeBoolean    DataType = "BOOLEAN"
)

// Validate checks if the column definition is valid
func (c *Column) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("column name cannot be empty")
	}

	// Validate data type specific constraints
	switch c.DataType {
	case TypeChar, TypeVarchar, TypeBinary, TypeVarBinary:
		if c.CharMaxLength <= 0 {
			return fmt.Errorf("column %s: length must be positive for type %s", c.Name, c.DataType)
		}
	case TypeTinyInt, TypeSmallInt, TypeMediumInt, TypeInt, TypeBigInt,
		TypeFloat, TypeDouble, TypeDecimal, TypeDate, TypeTime,
		TypeDateTime, TypeTimestamp, TypeYear, TypeJSON,
		TypeTinyBlob, TypeBlob, TypeMediumBlob, TypeLongBlob,
		TypeTinyText, TypeText, TypeMediumText, TypeLongText,
		TypeEnum, TypeSet:
		// These types don't require additional validation here
	default:
		return fmt.Errorf("column %s: unknown data type %s", c.Name, c.DataType)
	}

	return nil
}

// SQL returns the SQL definition of the column
func (c *Column) SQL() string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("`%s` %s", c.Name, c.DataType))

	// Add length for types that support it
	switch c.DataType {
	case TypeChar, TypeVarchar, TypeBinary, TypeVarBinary:
		builder.WriteString(fmt.Sprintf("(%d)", c.CharMaxLength))
	}

	// Add UNSIGNED/ZEROFILL for numeric types
	if c.IsUnsigned {
		builder.WriteString(" UNSIGNED")
	}
	if c.IsZerofill {
		builder.WriteString(" ZEROFILL")
	}

	// Add NULL/NOT NULL
	if c.IsNullable {
		builder.WriteString(" NULL")
	} else {
		builder.WriteString(" NOT NULL")
	}

	// Add default value if specified
	if c.DefaultValue != nil {
		builder.WriteString(fmt.Sprintf(" DEFAULT %v", c.DefaultValue))
	}

	// Add AUTO_INCREMENT
	if c.IsAutoIncrement {
		builder.WriteString(" AUTO_INCREMENT")
	}

	// Add character set and collation if specified
	if c.Charset != "" {
		builder.WriteString(fmt.Sprintf(" CHARACTER SET %s", c.Charset))
	}
	if c.Collation != "" {
		builder.WriteString(fmt.Sprintf(" COLLATE %s", c.Collation))
	}

	// Add comment if specified
	if c.Comment != "" {
		builder.WriteString(fmt.Sprintf(" COMMENT '%s'", strings.ReplaceAll(c.Comment, "'", "''")))
	}

	return builder.String()
}

// IsNumeric returns true if the column has a numeric data type
func (c *Column) IsNumeric() bool {
	switch c.DataType {
	case TypeTinyInt, TypeSmallInt, TypeMediumInt, TypeInt, TypeBigInt,
		TypeFloat, TypeDouble, TypeDecimal:
		return true
	default:
		return false
	}
}

// IsString returns true if the column has a string data type
func (c *Column) IsString() bool {
	switch c.DataType {
	case TypeChar, TypeVarchar, TypeTinyText, TypeText,
		TypeMediumText, TypeLongText, TypeEnum, TypeSet:
		return true
	default:
		return false
	}
}

// IsBinary returns true if the column has a binary data type
func (c *Column) IsBinary() bool {
	switch c.DataType {
	case TypeBinary, TypeVarBinary, TypeTinyBlob, TypeBlob,
		TypeMediumBlob, TypeLongBlob:
		return true
	default:
		return false
	}
}

// IsTemporal returns true if the column has a temporal data type
func (c *Column) IsTemporal() bool {
	switch c.DataType {
	case TypeDate, TypeTime, TypeDateTime, TypeTimestamp, TypeYear:
		return true
	default:
		return false
	}
}

// IsJSON returns true if the column has a JSON data type
func (c *Column) IsJSON() bool {
	return c.DataType == TypeJSON
}

// Index represents a database index
// 表示数据库中的索引
type Index struct {
	Table     *Table
	Name      string
	Columns   []string
	IsUnique  bool
	IsPrimary bool
	IndexType string // BTREE, HASH, etc.
	Comment   string
	Stats     *IndexStatistics // 添加索引统计信息
}

// IndexStatistics contains statistical information about an index
type IndexStatistics struct {
	Cardinality  int64 // 基数
	DataLength   int64 // 数据长度
	LeafPages    int64 // 叶子页数量
	NonLeafPages int64 // 非叶子页数量
	Selectivity  int64
}

// SQL returns the SQL definition of the index
func (idx *Index) SQL() string {
	var builder strings.Builder

	if idx.IsPrimary {
		builder.WriteString("PRIMARY KEY")
	} else if idx.IsUnique {
		builder.WriteString("UNIQUE KEY")
	} else {
		builder.WriteString("KEY")
	}

	if !idx.IsPrimary {
		builder.WriteString(fmt.Sprintf(" `%s`", idx.Name))
	}

	// Add columns
	builder.WriteString(" (")
	for i, col := range idx.Columns {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(fmt.Sprintf("`%s`", col))
	}
	builder.WriteString(")")

	// Add index type if specified
	if idx.IndexType != "" {
		builder.WriteString(fmt.Sprintf(" USING %s", idx.IndexType))
	}

	// Add comment if specified
	if idx.Comment != "" {
		builder.WriteString(fmt.Sprintf(" COMMENT '%s'", strings.ReplaceAll(idx.Comment, "'", "''")))
	}

	return builder.String()
}

// ForeignKey represents a foreign key constraint
// 表示外键约束
type ForeignKey struct {
	Name            string
	Columns         []string
	ReferencedTable string
	ReferencedCols  []string
	OnDelete        string
	OnUpdate        string
}
