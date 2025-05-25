package metadata

import (
	"fmt"
	"strings"
)

// DatabaseSchema represents a database schema
// 表示数据库中的模式（schema）
type DatabaseSchema struct {
	Name    string
	Tables  map[string]*Table
	Charset string
}

// NewSchema creates a new schema
func NewSchema(name string) *DatabaseSchema {
	return &DatabaseSchema{
		Name:   name,
		Tables: make(map[string]*Table),
	}
}

// AddTable adds a table to the schema
func (s *DatabaseSchema) AddTable(table *Table) error {
	if _, exists := s.Tables[table.Name]; exists {
		return fmt.Errorf("table %s already exists in schema %s", table.Name, s.Name)
	}
	table.Schema = s
	s.Tables[table.Name] = table
	return nil
}

// GetTable retrieves a table by name (case-insensitive)
func (s *DatabaseSchema) GetTable(name string) (*Table, bool) {
	// First try exact match
	if table, exists := s.Tables[name]; exists {
		return table, true
	}

	// Fall back to case-insensitive search
	name = strings.ToLower(name)
	for tableName, table := range s.Tables {
		if strings.ToLower(tableName) == name {
			return table, true
		}
	}
	return nil, false
}

// Table represents a database table
// 表示数据库中的表
type Table struct {
	Schema      *DatabaseSchema
	Name        string
	Columns     []*Column
	Indices     []*Index
	PrimaryKey  *Index
	ForeignKeys []*ForeignKey
	Engine      string
	Charset     string
	Collation   string
	RowFormat   string
	Comment     string
	Stats       *TableStatistics
}

// NewTable creates a new table
func NewTable(name string) *Table {
	return &Table{
		Name:    name,
		Columns: make([]*Column, 0),
		Indices: make([]*Index, 0),
		Stats:   &TableStatistics{},
	}
}

// AddColumn adds a column to the table
func (t *Table) AddColumn(col *Column) {
	col.Table = t
	col.OrdinalPosition = len(t.Columns) + 1
	t.Columns = append(t.Columns, col)
}

// GetColumn returns a column by name (case-insensitive)
func (t *Table) GetColumn(name string) (*Column, bool) {
	for _, col := range t.Columns {
		if strings.EqualFold(col.Name, name) {
			return col, true
		}
	}
	return nil, false
}

// GetColumnByIndex returns a column by its ordinal position (1-based)
func (t *Table) GetColumnByIndex(idx int) (*Column, bool) {
	if idx <= 0 || idx > len(t.Columns) {
		return nil, false
	}
	return t.Columns[idx-1], true
}

// AddIndex adds an index to the table
func (t *Table) AddIndex(idx *Index) error {
	// Validate columns exist
	for _, colName := range idx.Columns {
		if _, exists := t.GetColumn(colName); !exists {
			return fmt.Errorf("column %s not found in table %s", colName, t.Name)
		}
	}

	idx.Table = t
	t.Indices = append(t.Indices, idx)

	// If this is a primary key, set it
	if idx.IsPrimary {
		t.PrimaryKey = idx
	}

	return nil
}

// GetIndex returns an index by name (case-insensitive)
func (t *Table) GetIndex(name string) (*Index, bool) {
	for _, idx := range t.Indices {
		if strings.EqualFold(idx.Name, name) {
			return idx, true
		}
	}
	return nil, false
}

// Validate checks if the table definition is valid
func (t *Table) Validate() error {
	if t.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	// Check for duplicate column names
	seenCols := make(map[string]bool)
	for _, col := range t.Columns {
		if seenCols[col.Name] {
			return fmt.Errorf("duplicate column name: %s", col.Name)
		}
		seenCols[col.Name] = true

		if err := col.Validate(); err != nil {
			return fmt.Errorf("invalid column %s: %v", col.Name, err)
		}
	}

	// Validate primary key
	if t.PrimaryKey != nil {
		for _, colName := range t.PrimaryKey.Columns {
			if _, exists := t.GetColumn(colName); !exists {
				return fmt.Errorf("primary key column %s not found", colName)
			}
		}
	}

	// Validate indices
	seenIndices := make(map[string]bool)
	for _, idx := range t.Indices {
		if seenIndices[idx.Name] {
			return fmt.Errorf("duplicate index name: %s", idx.Name)
		}
		seenIndices[idx.Name] = true

		for _, colName := range idx.Columns {
			if _, exists := t.GetColumn(colName); !exists {
				return fmt.Errorf("index %s references unknown column %s", idx.Name, colName)
			}
		}
	}

	return nil
}

// TableStatistics contains statistical information about a table
// 包含表的统计信息
type TableStatistics struct {
	RowCount      int64
	DataLength    int64
	IndexLength   int64
	TotalSize     int64
	AutoIncrement int64
	CreateTime    string
	UpdateTime    string
	CheckTime     string
}
