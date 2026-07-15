package engine

import (
	"fmt"
	"strings"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

var memoryDMLStore = &dmlMemoryStore{
	tables: make(map[string][]map[string]interface{}),
}

type dmlMemoryStore struct {
	mu     sync.RWMutex
	tables map[string][]map[string]interface{}
}

func dmlMemoryKey(schemaName, tableName string) string {
	return strings.ToLower(strings.TrimSpace(schemaName)) + "." + strings.ToLower(strings.TrimSpace(tableName))
}

func cloneRowMap(row map[string]interface{}) map[string]interface{} {
	cloned := make(map[string]interface{}, len(row))
	for k, v := range row {
		cloned[k] = v
	}
	return cloned
}

func memoryInsertRows(schemaName, tableName string, rows []*InsertRowData) {
	memoryDMLStore.mu.Lock()
	defer memoryDMLStore.mu.Unlock()

	key := dmlMemoryKey(schemaName, tableName)
	for _, row := range rows {
		if row == nil {
			continue
		}
		memoryDMLStore.tables[key] = append(memoryDMLStore.tables[key], cloneRowMap(row.ColumnValues))
	}
}

func memorySelectRows(schemaName, tableName string) ([]map[string]interface{}, bool) {
	memoryDMLStore.mu.RLock()
	defer memoryDMLStore.mu.RUnlock()

	rows, exists := memoryDMLStore.tables[dmlMemoryKey(schemaName, tableName)]
	if !exists {
		return nil, false
	}
	cloned := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		cloned = append(cloned, cloneRowMap(row))
	}
	return cloned, true
}

func memoryUpdateRows(schemaName, tableName string, whereConditions []string, updateExprs []*UpdateExpression) (int, bool, error) {
	memoryDMLStore.mu.Lock()
	defer memoryDMLStore.mu.Unlock()

	key := dmlMemoryKey(schemaName, tableName)
	rows, exists := memoryDMLStore.tables[key]
	if !exists {
		return 0, false, nil
	}

	affected := 0
	for i, row := range rows {
		matches, err := rowMatchesWhereConditions(row, whereConditions)
		if err != nil {
			return 0, true, err
		}
		if !matches {
			continue
		}
		for _, expr := range updateExprs {
			newValue := expr.NewValue
			if expr.Expr != nil {
				value, err := evaluateExpressionWithRow(expr.Expr, row)
				if err != nil {
					return 0, true, err
				}
				newValue = value
			}
			row[expr.ColumnName] = newValue
		}
		rows[i] = row
		affected++
	}
	memoryDMLStore.tables[key] = rows
	return affected, true, nil
}

func memoryDeleteRows(schemaName, tableName string, whereConditions []string) (int, bool, error) {
	memoryDMLStore.mu.Lock()
	defer memoryDMLStore.mu.Unlock()

	key := dmlMemoryKey(schemaName, tableName)
	rows, exists := memoryDMLStore.tables[key]
	if !exists {
		return 0, false, nil
	}

	kept := make([]map[string]interface{}, 0, len(rows))
	affected := 0
	for _, row := range rows {
		matches, err := rowMatchesWhereConditions(row, whereConditions)
		if err != nil {
			return 0, true, err
		}
		if matches {
			affected++
			continue
		}
		kept = append(kept, row)
	}
	memoryDMLStore.tables[key] = kept
	return affected, true, nil
}

func memoryClearTable(schemaName, tableName string) {
	memoryDMLStore.mu.Lock()
	defer memoryDMLStore.mu.Unlock()
	delete(memoryDMLStore.tables, dmlMemoryKey(schemaName, tableName))
}

func memoryClearDatabase(schemaName string) {
	memoryDMLStore.mu.Lock()
	defer memoryDMLStore.mu.Unlock()

	prefix := strings.ToLower(strings.TrimSpace(schemaName)) + "."
	for key := range memoryDMLStore.tables {
		if strings.HasPrefix(key, prefix) {
			delete(memoryDMLStore.tables, key)
		}
	}
}

func memoryValidateUnique(schemaName, tableName string, rows []*InsertRowData, tableMeta *metadata.TableMeta) error {
	uniqueColumns := make([]string, 0)
	for _, col := range tableMeta.Columns {
		if col != nil && col.IsUnique {
			uniqueColumns = append(uniqueColumns, col.Name)
		}
	}
	if len(uniqueColumns) == 0 {
		return nil
	}

	existingRows, _ := memorySelectRows(schemaName, tableName)
	for _, colName := range uniqueColumns {
		seen := make(map[string]struct{})
		for _, existing := range existingRows {
			value, exists := existing[colName]
			if !exists || value == nil {
				continue
			}
			seen[fmt.Sprintf("%v", value)] = struct{}{}
		}
		for _, row := range rows {
			if row == nil {
				continue
			}
			value, exists := row.ColumnValues[colName]
			if !exists || value == nil {
				continue
			}
			key := fmt.Sprintf("%v", value)
			if _, exists := seen[key]; exists {
				return fmt.Errorf("Duplicate entry '%v' for key '%s'", value, colName)
			}
			seen[key] = struct{}{}
		}
	}

	return nil
}
