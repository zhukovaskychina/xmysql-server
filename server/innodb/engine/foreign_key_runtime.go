package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

type foreignKeyRuntimeMeta struct {
	Columns    []string `json:"columns"`
	RefSchema  string   `json:"ref_schema,omitempty"`
	RefTable   string   `json:"ref_table"`
	RefColumns []string `json:"ref_columns"`
	OnDelete   string   `json:"on_delete"`
	OnUpdate   string   `json:"on_update"`
}

type foreignKeyTableInfo struct {
	TableName  string                  `json:"table_name"`
	ForeignKey []foreignKeyRuntimeMeta `json:"foreign_keys"`
}

func (dml *StorageIntegratedDMLExecutor) loadTableForeignKeys(schemaName, tableName string) ([]foreignKeyRuntimeMeta, error) {
	if dml == nil || dml.dataDir == "" || schemaName == "" || tableName == "" {
		return nil, nil
	}
	data, err := os.ReadFile(filepath.Join(dml.dataDir, schemaName, tableName+".frm"))
	if err != nil {
		return nil, err
	}
	var info foreignKeyTableInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	return info.ForeignKey, nil
}

func (dml *StorageIntegratedDMLExecutor) loadReferencingForeignKeys(schemaName, parentTable string) ([]struct {
	SchemaName string
	TableName  string
	FK         foreignKeyRuntimeMeta
}, error) {
	if dml == nil || dml.dataDir == "" || schemaName == "" {
		return nil, nil
	}
	schemas, err := os.ReadDir(dml.dataDir)
	if err != nil {
		return nil, err
	}
	var refs []struct {
		SchemaName string
		TableName  string
		FK         foreignKeyRuntimeMeta
	}
	for _, schemaEntry := range schemas {
		if !schemaEntry.IsDir() {
			continue
		}
		childSchema := schemaEntry.Name()
		entries, readErr := os.ReadDir(filepath.Join(dml.dataDir, childSchema))
		if readErr != nil {
			return nil, readErr
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".frm") {
				continue
			}
			tableName := strings.TrimSuffix(entry.Name(), ".frm")
			fks, loadErr := dml.loadTableForeignKeys(childSchema, tableName)
			if loadErr != nil {
				return nil, loadErr
			}
			for _, fk := range fks {
				refSchema := fk.RefSchema
				if strings.TrimSpace(refSchema) == "" {
					refSchema = childSchema
				}
				if strings.EqualFold(refSchema, schemaName) && strings.EqualFold(fk.RefTable, parentTable) {
					refs = append(refs, struct {
						SchemaName string
						TableName  string
						FK         foreignKeyRuntimeMeta
					}{SchemaName: childSchema, TableName: tableName, FK: fk})
				}
			}
		}
	}
	return refs, nil
}

func (dml *StorageIntegratedDMLExecutor) validateForeignKeyConstraints(
	ctx context.Context,
	rows []*InsertRowData,
	schemaName string,
	tableMeta *metadata.TableMeta,
) error {
	if dml != nil && !dml.foreignKeyChecks {
		return nil
	}
	fks, err := dml.loadTableForeignKeys(schemaName, dml.tableName)
	if err != nil || len(fks) == 0 {
		return err
	}
	return dml.validateRowsAgainstForeignKeys(ctx, rows, schemaName, dml.tableName, tableMeta, fks)
}

func (dml *StorageIntegratedDMLExecutor) validateUpdatedForeignKeyConstraints(
	ctx context.Context,
	rows []*InsertRowData,
	schemaName string,
	tableName string,
	tableMeta *metadata.TableMeta,
) error {
	if dml != nil && !dml.foreignKeyChecks {
		return nil
	}
	fks, err := dml.loadTableForeignKeys(schemaName, tableName)
	if err != nil || len(fks) == 0 {
		return err
	}
	return dml.validateRowsAgainstForeignKeys(ctx, rows, schemaName, tableName, tableMeta, fks)
}

func (dml *StorageIntegratedDMLExecutor) validateRowsAgainstForeignKeys(
	ctx context.Context,
	rows []*InsertRowData,
	schemaName string,
	tableName string,
	tableMeta *metadata.TableMeta,
	fks []foreignKeyRuntimeMeta,
) error {
	for _, fk := range fks {
		parentSchema := schemaName
		if strings.TrimSpace(fk.RefSchema) != "" {
			parentSchema = fk.RefSchema
		}
		parentMeta, parentStorage, parentBTree, err := dml.tableAccess(ctx, parentSchema, fk.RefTable)
		if err != nil {
			return err
		}
		_ = parentMeta
		for _, row := range rows {
			conditions, enforce := foreignKeyWhereConditions(fk.RefColumns, fk.Columns, row.ColumnValues)
			if !enforce {
				continue
			}
			matches, err := dml.scanRowsForTableConditions(ctx, parentSchema, fk.RefTable, []string{strings.Join(conditions, " and ")}, parentMeta, parentStorage, parentBTree)
			if err != nil {
				return err
			}
			if len(matches) == 0 {
				return fmt.Errorf("Cannot add or update a child row: a foreign key constraint fails (%s.%s)", schemaName, tableName)
			}
		}
	}
	_ = tableMeta
	return nil
}

func (dml *StorageIntegratedDMLExecutor) tableAccess(ctx context.Context, schemaName, tableName string) (*metadata.TableMeta, *manager.TableStorageInfo, basic.BPlusTreeManager, error) {
	storageInfo, err := dml.tableStorageManager.GetTableStorageInfo(schemaName, tableName)
	if err != nil {
		return nil, nil, nil, err
	}
	meta, err := (&SelectExecutor{}).loadTableMetaFromFrm(dml.dataDir, schemaName, tableName)
	if err != nil {
		return nil, nil, nil, err
	}
	btreeManager, err := dml.tableStorageManager.CreateBTreeManagerForTable(ctx, schemaName, tableName)
	if err != nil {
		return nil, nil, nil, err
	}
	return meta, storageInfo, btreeManager, nil
}

func sqlLiteral(value interface{}) string {
	switch v := value.(type) {
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (dml *StorageIntegratedDMLExecutor) applyOnDeleteCascade(
	ctx context.Context,
	txn interface{},
	schemaName string,
	parentTable string,
	parentRows []*RowUpdateInfo,
) ([]transactionDMLChange, error) {
	return dml.applyOnDeleteCascadeWithPath(ctx, txn, schemaName, parentTable, parentRows, make(map[string]bool))
}

func (dml *StorageIntegratedDMLExecutor) applyOnDeleteCascadeWithPath(
	ctx context.Context,
	txn interface{},
	schemaName string,
	parentTable string,
	parentRows []*RowUpdateInfo,
	path map[string]bool,
) ([]transactionDMLChange, error) {
	pathKey := strings.ToLower(schemaName + "." + parentTable)
	if path[pathKey] {
		return nil, nil
	}
	path[pathKey] = true
	defer delete(path, pathKey)

	refs, err := dml.loadReferencingForeignKeys(schemaName, parentTable)
	if err != nil || len(refs) == 0 {
		return nil, err
	}
	changes := make([]transactionDMLChange, 0)
	for _, ref := range refs {
		childSchema := ref.SchemaName
		action := normalizeReferentialAction(ref.FK.OnDelete)
		childMeta, childStorage, childBTree, err := dml.tableAccess(ctx, childSchema, ref.TableName)
		if err != nil {
			return nil, err
		}
		for _, parentRow := range parentRows {
			conditions, enforce := foreignKeyWhereConditions(ref.FK.Columns, ref.FK.RefColumns, parentRow.OldValues)
			if !enforce {
				continue
			}
			childRows, err := dml.scanRowsForTableConditions(ctx, childSchema, ref.TableName, []string{strings.Join(conditions, " and ")}, childMeta, childStorage, childBTree)
			if err != nil {
				return nil, err
			}
			if action == "restrict" {
				if len(childRows) > 0 {
					return nil, fmt.Errorf("Cannot delete or update a parent row: a foreign key constraint fails (%s.%s)", schemaName, parentTable)
				}
				continue
			}
			for _, childRow := range childRows {
				if action == "cascade" {
					descendantChanges, err := dml.applyOnDeleteCascadeWithPath(
						ctx, txn, childSchema, ref.TableName, []*RowUpdateInfo{childRow}, path,
					)
					if err != nil {
						return nil, err
					}
					changes = append(changes, descendantChanges...)
					if err := dml.deleteRowFromStorage(ctx, txn, childRow, childMeta, childStorage, childBTree); err != nil {
						return nil, err
					}
					if err := dml.updateIndexesForDelete(ctx, txn, []*RowUpdateInfo{childRow}, childMeta, childStorage); err != nil {
						return nil, err
					}
					changes = append(changes, transactionDMLChange{
						tableName:  childSchema + "." + ref.TableName,
						kind:       "delete",
						rowID:      childRow.RowId,
						storageKey: childRow.StorageKey,
						before:     cloneTransactionRow(childRow.OldValues),
					})
					continue
				}
				if action != "set null" {
					return nil, fmt.Errorf("unsupported foreign key ON DELETE action %q", ref.FK.OnDelete)
				}
				updateExprs, err := foreignKeySetNullExpressions(childMeta, ref.FK.Columns)
				if err != nil {
					return nil, err
				}
				updatedChildRow, err := dml.applyUpdateExpressions(&InsertRowData{
					ColumnValues: cloneTransactionRow(childRow.OldValues),
					ColumnTypes:  make(map[string]metadata.DataType),
				}, updateExprs, childMeta)
				if err != nil {
					return nil, err
				}
				if err := dml.updateRowInStorage(ctx, txn, childRow, updateExprs, childMeta, childStorage, childBTree); err != nil {
					return nil, err
				}
				if err := dml.updateIndexesForUpdate(ctx, txn, []*RowUpdateInfo{childRow}, updateExprs, childMeta, childStorage); err != nil {
					return nil, err
				}
				newStorageKey, err := dml.storageKeyForUpdatedRow(childRow, updatedChildRow, childMeta)
				if err != nil {
					return nil, err
				}
				changes = append(changes, transactionDMLChange{
					tableName:     childSchema + "." + ref.TableName,
					kind:          "update",
					rowID:         childRow.RowId,
					storageKey:    childRow.StorageKey,
					newStorageKey: newStorageKey,
					before:        cloneTransactionRow(childRow.OldValues),
					after:         cloneTransactionRow(updatedChildRow.ColumnValues),
				})
			}
		}
	}
	return changes, nil
}

func normalizeReferentialAction(action string) string {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "cascade", "set null":
		return strings.ToLower(strings.TrimSpace(action))
	case "restrict", "no action", "":
		return "restrict"
	default:
		return "restrict"
	}
}

func foreignKeySetNullExpressions(tableMeta *metadata.TableMeta, columns []string) ([]*UpdateExpression, error) {
	updates := make([]*UpdateExpression, 0, len(columns))
	for _, column := range columns {
		meta := findColumnMeta(tableMeta, column)
		if meta == nil || !meta.IsNullable {
			return nil, fmt.Errorf("Cannot set NULL for non-nullable foreign key column %s", column)
		}
		updates = append(updates, &UpdateExpression{
			ColumnName: column,
			NewValue:   nil,
			ColumnType: meta.Type,
		})
	}
	return updates, nil
}

func (dml *StorageIntegratedDMLExecutor) applyOnUpdateCascade(
	ctx context.Context,
	txn interface{},
	schemaName string,
	parentTable string,
	parentRows []*RowUpdateInfo,
	updatedRows []*InsertRowData,
) ([]transactionDMLChange, error) {
	return dml.applyOnUpdateCascadeWithPath(ctx, txn, schemaName, parentTable, parentRows, updatedRows, make(map[string]bool))
}

func (dml *StorageIntegratedDMLExecutor) applyOnUpdateCascadeWithPath(
	ctx context.Context,
	txn interface{},
	schemaName string,
	parentTable string,
	parentRows []*RowUpdateInfo,
	updatedRows []*InsertRowData,
	path map[string]bool,
) ([]transactionDMLChange, error) {
	pathKey := strings.ToLower(schemaName + "." + parentTable)
	if path[pathKey] {
		return nil, nil
	}
	path[pathKey] = true
	defer delete(path, pathKey)

	refs, err := dml.loadReferencingForeignKeys(schemaName, parentTable)
	if err != nil || len(refs) == 0 {
		return nil, err
	}
	changes := make([]transactionDMLChange, 0)
	for _, ref := range refs {
		childSchema := ref.SchemaName
		action := normalizeReferentialAction(ref.FK.OnUpdate)
		childMeta, childStorage, childBTree, err := dml.tableAccess(ctx, childSchema, ref.TableName)
		if err != nil {
			return nil, err
		}
		for i, parentRow := range parentRows {
			if i >= len(updatedRows) {
				continue
			}
			conditions, enforce := foreignKeyWhereConditions(ref.FK.Columns, ref.FK.RefColumns, parentRow.OldValues)
			if !enforce {
				continue
			}
			childRows, err := dml.scanRowsForTableConditions(ctx, childSchema, ref.TableName, []string{strings.Join(conditions, " and ")}, childMeta, childStorage, childBTree)
			if err != nil {
				return nil, err
			}
			updateExprs := make([]*UpdateExpression, 0, len(ref.FK.Columns))
			changed := false
			for index, childColumn := range ref.FK.Columns {
				parentColumn := ref.FK.RefColumns[index]
				oldValue := parentRow.OldValues[parentColumn]
				newValue := updatedRows[i].ColumnValues[parentColumn]
				childColumnType := foreignKeyColumnType(childMeta, childColumn)
				if newValue != nil {
					newValue = normalizeDefaultValue(newValue, childColumnType)
				}
				if compareScalarValues(oldValue, newValue) != 0 {
					changed = true
				}
				updateExprs = append(updateExprs, &UpdateExpression{
					ColumnName: childColumn,
					NewValue:   newValue,
					ColumnType: childColumnType,
				})
			}
			if !changed {
				continue
			}
			if action == "restrict" {
				if len(childRows) > 0 {
					return nil, fmt.Errorf("Cannot delete or update a parent row: a foreign key constraint fails (%s.%s)", schemaName, parentTable)
				}
				continue
			}
			if action == "set null" {
				updateExprs, err = foreignKeySetNullExpressions(childMeta, ref.FK.Columns)
				if err != nil {
					return nil, err
				}
			} else if action != "cascade" {
				return nil, fmt.Errorf("unsupported foreign key ON UPDATE action %q", ref.FK.OnUpdate)
			}
			for _, childRow := range childRows {
				updatedChildRow, err := dml.applyUpdateExpressions(&InsertRowData{
					ColumnValues: cloneTransactionRow(childRow.OldValues),
					ColumnTypes:  make(map[string]metadata.DataType),
				}, updateExprs, childMeta)
				if err != nil {
					return nil, err
				}
				if err := dml.updateRowInStorage(ctx, txn, childRow, updateExprs, childMeta, childStorage, childBTree); err != nil {
					return nil, err
				}
				if err := dml.updateIndexesForUpdate(ctx, txn, []*RowUpdateInfo{childRow}, updateExprs, childMeta, childStorage); err != nil {
					return nil, err
				}
				descendantChanges, err := dml.applyOnUpdateCascadeWithPath(
					ctx, txn, childSchema, ref.TableName, []*RowUpdateInfo{childRow}, []*InsertRowData{updatedChildRow}, path,
				)
				if err != nil {
					return nil, err
				}
				changes = append(changes, descendantChanges...)
				changes = append(changes, transactionDMLChange{
					tableName:  childSchema + "." + ref.TableName,
					kind:       "update",
					rowID:      childRow.RowId,
					storageKey: childRow.StorageKey,
					before:     cloneTransactionRow(childRow.OldValues),
					after:      cloneTransactionRow(updatedChildRow.ColumnValues),
				})
			}
		}
	}
	return changes, nil
}

func foreignKeyWhereConditions(localColumns, referencedColumns []string, values map[string]interface{}) ([]string, bool) {
	if len(localColumns) == 0 || len(localColumns) != len(referencedColumns) {
		return nil, false
	}
	conditions := make([]string, 0, len(localColumns))
	for index, localColumn := range localColumns {
		value, exists := values[referencedColumns[index]]
		if !exists || value == nil {
			return nil, false
		}
		conditions = append(conditions, fmt.Sprintf("%s = %s", localColumn, sqlLiteral(value)))
	}
	return conditions, true
}

func foreignKeyColumnType(tableMeta *metadata.TableMeta, columnName string) metadata.DataType {
	if col := findColumnMeta(tableMeta, columnName); col != nil {
		return col.Type
	}
	return metadata.TypeVarchar
}

func (dml *StorageIntegratedDMLExecutor) hasOnUpdateCascade(schemaName, parentTable string) bool {
	refs, err := dml.loadReferencingForeignKeys(schemaName, parentTable)
	if err != nil {
		return false
	}
	for _, ref := range refs {
		if strings.EqualFold(ref.FK.OnUpdate, "cascade") {
			return true
		}
	}
	return false
}

func (dml *StorageIntegratedDMLExecutor) buildUpdatedRowsForCascade(
	rowsToUpdate []*RowUpdateInfo,
	updateExprs []*UpdateExpression,
	tableMeta *metadata.TableMeta,
) ([]*InsertRowData, error) {
	updatedRows := make([]*InsertRowData, 0, len(rowsToUpdate))
	for _, rowInfo := range rowsToUpdate {
		existing := &InsertRowData{
			ColumnValues: make(map[string]interface{}, len(rowInfo.OldValues)),
			ColumnTypes:  make(map[string]metadata.DataType, len(rowInfo.OldValues)),
		}
		for name, value := range rowInfo.OldValues {
			existing.ColumnValues[name] = value
			if col := findColumnMeta(tableMeta, name); col != nil {
				existing.ColumnTypes[name] = col.Type
			}
		}
		updated, err := dml.applyUpdateExpressions(existing, updateExprs, tableMeta)
		if err != nil {
			return nil, err
		}
		updatedRows = append(updatedRows, updated)
	}
	return updatedRows, nil
}
