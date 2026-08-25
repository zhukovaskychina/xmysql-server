package metadata

import (
	"fmt"
	"strconv"
	"strings"
	"time"

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
	if c == nil {
		return nil, fmt.Errorf("column metadata is nil")
	}

	if val == nil {
		if c.IsNullable {
			return basic.NewNull(), nil
		}
		return nil, fmt.Errorf("column %s does not allow NULL", c.Name)
	}

	switch c.Type {
	case TypeTinyInt, TypeSmallInt, TypeMediumInt, TypeInt, TypeBigInt, TypeBool, TypeBoolean:
		if parsed, err := parseIntValue(val); err == nil {
			return basic.NewInt64(parsed), nil
		}
	case TypeFloat, TypeDouble:
		if parsed, err := parseFloatValue(val); err == nil {
			return basic.NewFloat(parsed), nil
		}
	case TypeDecimal:
		if parsed, err := parseFloatValue(val); err == nil {
			return basic.NewFloat(parsed), nil
		}
	case TypeDate, TypeTime, TypeDateTime, TypeTimestamp, TypeYear:
		if parsed, err := parseTimeString(val); err == nil {
			return basic.NewTime(parsed), nil
		}
	case TypeChar, TypeVarchar, TypeText, TypeTinyText, TypeMediumText, TypeLongText,
		TypeBinary, TypeVarBinary, TypeBlob, TypeTinyBlob, TypeMediumBlob, TypeLongBlob,
		TypeEnum, TypeSet, TypeJSON:
		if s, ok := parseStringValue(val); ok {
			if c.Type == TypeBinary || c.Type == TypeVarBinary || strings.Contains(string(c.Type), "BLOB") {
				return basic.NewBytes([]byte(s)), nil
			}
			return basic.NewString(s), nil
		}
	default:
		if s, ok := parseStringValue(val); ok {
			return basic.NewString(s), nil
		}
	}

	return nil, fmt.Errorf("column %s: unsupported value type %T for %s", c.Name, val, c.Type)
}

// ValidateValue validates if the value matches the column type
func (c *ColumnMeta) ValidateValue(val interface{}) bool {
	if c == nil {
		return false
	}
	if val == nil {
		return c.IsNullable
	}

	_, err := c.ConvertToBasicValue(val)
	return err == nil
}

func parseIntValue(val interface{}) (int64, error) {
	switch v := val.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > uint64(^uint(0)>>1) {
			return 0, fmt.Errorf("uint64 too large: %d", v)
		}
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case string:
		return strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	case basic.Value:
		return v.Int(), nil
	default:
		return 0, fmt.Errorf("unsupported int value type %T", val)
	}
}

func parseFloatValue(val interface{}) (float64, error) {
	switch v := val.(type) {
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		return strconv.ParseFloat(strings.TrimSpace(v), 64)
	case basic.Value:
		return v.Float64(), nil
	default:
		return 0, fmt.Errorf("unsupported float value type %T", val)
	}
}

func parseStringValue(val interface{}) (string, bool) {
	switch v := val.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	case fmt.Stringer:
		return v.String(), true
	case basic.Value:
		return v.ToString(), true
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool:
		i, err := parseIntValue(v)
		if err == nil {
			return strconv.FormatInt(i, 10), true
		}
		f, err := parseFloatValue(v)
		if err == nil {
			return strconv.FormatFloat(f, 'f', -1, 64), true
		}
		return "", false
	default:
		return "", false
	}
}

func parseTimeString(val interface{}) (string, error) {
	switch v := val.(type) {
	case time.Time:
		return v.Format("2006-01-02 15:04:05"), nil
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return "", fmt.Errorf("empty time string")
		}
		if _, err := time.Parse("2006-01-02", s); err == nil {
			return s, nil
		}
		if _, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
			return s, nil
		}
		if _, err := time.Parse(time.RFC3339, s); err == nil {
			return s, nil
		}
		return "", fmt.Errorf("invalid time format: %s", s)
	default:
		if bv, ok := val.(basic.Value); ok {
			t, ok := bv.Raw().(string)
			if ok {
				return parseTimeString(t)
			}
		}
	}

	return "", fmt.Errorf("unsupported temporal value %T", val)
}
