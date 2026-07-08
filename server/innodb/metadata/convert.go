package metadata

import (
	"fmt"
	"strings"
)

// ConvertTable converts a Table to a TableMeta
func ConvertTable(oldTable *Table) (*TableMeta, error) {
	if oldTable == nil {
		return nil, fmt.Errorf("table is nil")
	}

	meta := &TableMeta{
		Name:      oldTable.Name,
		Engine:    oldTable.Engine,
		Charset:   oldTable.Charset,
		Collation: oldTable.Collation,
		RowFormat: oldTable.RowFormat,
		Comment:   oldTable.Comment,
		Columns:   make([]*ColumnMeta, 0, len(oldTable.Columns)),
		Indices:   make([]IndexMeta, 0, len(oldTable.Indices)),
	}

	for _, oldCol := range oldTable.Columns {
		col, err := convertColumn(oldCol)
		if err != nil {
			return nil, fmt.Errorf("convert column %s failed: %w", oldCol.Name, err)
		}
		meta.AddColumn(col)
	}

	for _, oldIdx := range oldTable.Indices {
		if oldIdx == nil {
			continue
		}

		idxCols := extractIndexColumns(oldIdx.Columns)
		if len(idxCols) == 0 {
			continue
		}

		meta.Indices = append(meta.Indices, IndexMeta{
			Name:    oldIdx.Name,
			Columns: append([]string(nil), idxCols...),
			Unique:  oldIdx.IsUnique || oldIdx.IsPrimary,
		})

		if oldIdx.IsPrimary {
			meta.PrimaryKey = append([]string(nil), idxCols...)
		}

		if oldIdx.IsUnique {
			for _, colName := range idxCols {
				if col, err := meta.GetColumn(colName); err == nil {
					col.IsUnique = true
				}
			}
		}

		for _, colName := range idxCols {
			if col, err := meta.GetColumn(colName); err == nil {
				if oldIdx.IsPrimary {
					col.IsPrimary = true
				}
			}
		}
	}

	if oldTable.PrimaryKey != nil && len(oldTable.PrimaryKey.Columns) > 0 {
		meta.PrimaryKey = append([]string(nil), oldTable.PrimaryKey.Columns...)
		for _, colName := range oldTable.PrimaryKey.Columns {
			if col, err := meta.GetColumn(colName); err == nil {
				col.IsPrimary = true
			}
		}
	}

	if err := meta.Validate(); err != nil {
		return nil, fmt.Errorf("validate table %s failed: %w", oldTable.Name, err)
	}

	return meta, nil
}

// ConvertSchema converts a Schema to a DatabaseSchema
func ConvertSchema(oldSchema Schema) (*DatabaseSchema, error) {
	if oldSchema == nil {
		return nil, fmt.Errorf("schema is nil")
	}

	databaseSchema := &DatabaseSchema{
		Name:    oldSchema.GetName(),
		Charset: oldSchema.GetCharset(),
		Tables:  make(map[string]*Table),
	}

	for _, oldTable := range oldSchema.GetTables() {
		meta, err := ConvertTable(oldTable)
		if err != nil {
			return nil, fmt.Errorf("failed to convert table %s: %w", oldTable.Name, err)
		}

		converted := NewTable(meta.Name)
		converted.Engine = meta.Engine
		converted.Charset = meta.Charset
		converted.Collation = meta.Collation
		converted.RowFormat = meta.RowFormat
		converted.Comment = meta.Comment

		for _, col := range meta.Columns {
			converted.AddColumn(&Column{
				Name:            col.Name,
				DataType:        col.Type,
				CharMaxLength:   col.Length,
				IsNullable:      col.IsNullable,
				DefaultValue:    col.DefaultValue,
				IsAutoIncrement: col.IsAutoIncrement,
				Charset:         col.Charset,
				Collation:       col.Collation,
				Comment:         col.Comment,
			})
		}

		for _, idx := range meta.Indices {
			if _, exists := converted.GetIndex(idx.Name); exists {
				continue
			}

			_ = converted.AddIndex(&Index{
				Name:     idx.Name,
				Columns:  idx.Columns,
				IsUnique: idx.Unique,
			})
		}

		if len(meta.PrimaryKey) > 0 {
			if err := converted.AddIndex(&Index{
				Name:     "PRIMARY",
				Columns:  meta.PrimaryKey,
				IsUnique: true,
				IsPrimary: true,
			}); err != nil {
				return nil, fmt.Errorf("set primary key for %s failed: %w", meta.Name, err)
			}
		}

		if err := databaseSchema.AddTable(converted); err != nil {
			return nil, fmt.Errorf("add table %s to schema %s failed: %w", meta.Name, oldSchema.GetName(), err)
		}
	}

	return databaseSchema, nil
}

func convertColumn(oldCol *Column) (*ColumnMeta, error) {
	if oldCol == nil {
		return nil, fmt.Errorf("old column is nil")
	}

	meta := &ColumnMeta{
		Name:            oldCol.Name,
		Type:            oldCol.DataType,
		Length:          oldCol.CharMaxLength,
		IsNullable:      oldCol.IsNullable,
		IsAutoIncrement: oldCol.IsAutoIncrement,
		DefaultValue:    oldCol.DefaultValue,
		Charset:         oldCol.Charset,
		Collation:       oldCol.Collation,
		Comment:         oldCol.Comment,
	}

	if err := meta.Validate(); err != nil {
		return nil, err
	}

	return meta, nil
}

func extractIndexColumns(cols []string) []string {
	result := make([]string, 0, len(cols))
	for _, col := range cols {
		if strings.TrimSpace(col) == "" {
			continue
		}
		result = append(result, col)
	}
	return result
}
