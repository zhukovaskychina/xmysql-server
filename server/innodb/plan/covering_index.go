package plan

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// IsCoveringIndex 判断给定索引是否覆盖查询所需列（无需回表）。
// 若 requiredColumns 包含 "*" 或为空，返回 false。
// 二级索引隐式包含主键列，因此 requiredColumns 可包含主键列名。
func IsCoveringIndex(table *metadata.Table, index *metadata.Index, requiredColumns []string) bool {
	if len(requiredColumns) == 0 {
		return false
	}

	indexCols := make(map[string]bool)
	if index.IsPrimary && index.Table != nil {
		for _, col := range index.Table.Columns {
			indexCols[col.Name] = true
		}
	} else {
		for _, col := range index.Columns {
			indexCols[col] = true
		}
	}

	if !index.IsPrimary && index.Table != nil && index.Table.PrimaryKey != nil {
		for _, pkCol := range index.Table.PrimaryKey.Columns {
			indexCols[pkCol] = true
		}
	}

	for _, col := range requiredColumns {
		if col == "*" {
			return false
		}
		if !indexCols[col] {
			return false
		}
	}
	return true
}
