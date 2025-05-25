package metadata

import (
	"fmt"
	"strings"
)

// CreateTableMeta creates a new table metadata with the given name
func CreateTableMeta(name string) *TableMeta {
	return &TableMeta{
		Name:    name,
		Columns: make([]*ColumnMeta, 0),
		Indices: make([]IndexMeta, 0),
	}
}

// AddColumn adds a new column to the table metadata
func (t *TableMeta) AddColumn(col *ColumnMeta) {
	t.Columns = append(t.Columns, col)
}

// RemoveColumn removes a column by name
func (t *TableMeta) RemoveColumn(name string) error {
	for i, col := range t.Columns {
		if col.Name == name {
			t.Columns = append(t.Columns[:i], t.Columns[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("column %s not found", name)
}

// AddUniqueIndex adds a unique index
func (t *TableMeta) AddUniqueIndex(name string, columns []string) {
	t.Indices = append(t.Indices, IndexMeta{
		Name:    name,
		Columns: columns,
		Unique:  true,
	})
}

// RemoveIndex removes an index by name
func (t *TableMeta) RemoveIndex(name string) error {
	for i, idx := range t.Indices {
		if idx.Name == name {
			t.Indices = append(t.Indices[:i], t.Indices[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("index %s not found", name)
}

// SetOption sets a table option (engine, charset, collation, row format)
func (t *TableMeta) SetOption(option, value string) {
	switch strings.ToLower(option) {
	case "engine":
		t.Engine = value
	case "charset":
		t.Charset = value
	case "collation":
		t.Collation = value
	case "rowformat":
		t.RowFormat = value
	}
}

// AddComment sets the table comment
func (t *TableMeta) AddComment(comment string) {
	t.Comment = comment
}

// AddIndex adds a new index to the table metadata
func (t *TableMeta) AddIndex(name string, columns []string, unique bool) {
	t.Indices = append(t.Indices, IndexMeta{
		Name:    name,
		Columns: columns,
		Unique:  unique,
	})
}

// SetPrimaryKey sets the primary key columns
func (t *TableMeta) SetPrimaryKey(columns ...string) error {
	for _, colName := range columns {
		found := false
		for _, col := range t.Columns {
			if col.Name == colName {
				col.IsPrimary = true
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("column %s not found in table %s", colName, t.Name)
		}
	}
	t.PrimaryKey = columns
	return nil
}

// GetColumn returns the column metadata by name
func (t *TableMeta) GetColumn(name string) (*ColumnMeta, error) {
	for _, col := range t.Columns {
		if strings.EqualFold(col.Name, name) {
			return col, nil
		}
	}
	return nil, fmt.Errorf("column %s not found", name)
}

// Validate validates the table metadata
func (t *TableMeta) Validate() error {
	// Check for duplicate column names
	seen := make(map[string]bool)
	for _, col := range t.Columns {
		if seen[col.Name] {
			return fmt.Errorf("duplicate column name: %s", col.Name)
		}
		seen[col.Name] = true

		// Validate column type and length
		if err := col.Validate(); err != nil {
			return fmt.Errorf("invalid column %s: %v", col.Name, err)
		}
	}

	// Validate primary key
	if len(t.PrimaryKey) > 0 {
		for _, pkCol := range t.PrimaryKey {
			found := false
			for _, col := range t.Columns {
				if col.Name == pkCol {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("primary key column %s not found in table %s", pkCol, t.Name)
			}
		}
	}

	// Validate indices
	for _, idx := range t.Indices {
		for _, colName := range idx.Columns {
			found := false
			for _, col := range t.Columns {
				if col.Name == colName {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("index %s references unknown column %s", idx.Name, colName)
			}
		}
	}

	return nil
}

// Validate validates the column metadata
func (c *ColumnMeta) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("column name cannot be empty")
	}

	switch c.Type {
	case TypeChar, TypeVarchar, TypeBinary, TypeVarBinary:
		if c.Length <= 0 {
			return fmt.Errorf("column %s: length must be positive for type %v", c.Name, c.Type)
		}
	case TypeTinyInt, TypeSmallInt, TypeMediumInt, TypeInt, TypeBigInt,
		TypeFloat, TypeDouble, TypeDecimal, TypeDate, TypeTime,
		TypeDateTime, TypeTimestamp, TypeYear, TypeJSON:
		// These types don't require length validation
	case TypeTinyBlob, TypeBlob, TypeMediumBlob, TypeLongBlob,
		TypeTinyText, TypeText, TypeMediumText, TypeLongText:
		// BLOB and TEXT types don't require length
	case TypeEnum, TypeSet:
		// TODO: Validate enum/set values
	default:
		return fmt.Errorf("column %s: unknown type %v", c.Name, c.Type)
	}

	return nil
}

// SQLType returns the SQL type string for the column
func (c *ColumnMeta) SQLType() string {
	switch c.Type {
	case TypeTinyInt:
		return "TINYINT"
	case TypeSmallInt:
		return "SMALLINT"
	case TypeMediumInt:
		return "MEDIUMINT"
	case TypeInt:
		return "INT"
	case TypeBigInt:
		return "BIGINT"
	case TypeFloat:
		return "FLOAT"
	case TypeDouble:
		return "DOUBLE"
	case TypeDecimal:
		return "DECIMAL"
	case TypeDate:
		return "DATE"
	case TypeTime:
		return "TIME"
	case TypeDateTime:
		return "DATETIME"
	case TypeTimestamp:
		return "TIMESTAMP"
	case TypeYear:
		return "YEAR"
	case TypeChar:
		return fmt.Sprintf("CHAR(%d)", c.Length)
	case TypeVarchar:
		return fmt.Sprintf("VARCHAR(%d)", c.Length)
	case TypeBinary:
		return fmt.Sprintf("BINARY(%d)", c.Length)
	case TypeVarBinary:
		return fmt.Sprintf("VARBINARY(%d)", c.Length)
	case TypeTinyBlob:
		return "TINYBLOB"
	case TypeBlob:
		return "BLOB"
	case TypeMediumBlob:
		return "MEDIUMBLOB"
	case TypeLongBlob:
		return "LONGBLOB"
	case TypeTinyText:
		return "TINYTEXT"
	case TypeText:
		return "TEXT"
	case TypeMediumText:
		return "MEDIUMTEXT"
	case TypeLongText:
		return "LONGTEXT"
	case TypeEnum:
		// TODO: Handle enum values
		return "ENUM"
	case TypeSet:
		// TODO: Handle set values
		return "SET"
	case TypeJSON:
		return "JSON"
	default:
		return "UNKNOWN"
	}
}
