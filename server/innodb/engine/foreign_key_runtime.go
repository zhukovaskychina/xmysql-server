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
	TableName string
	FK        foreignKeyRuntimeMeta
}, error) {
	if dml == nil || dml.dataDir == "" || schemaName == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(filepath.Join(dml.dataDir, schemaName))
	if err != nil {
		return nil, err
	}
	var refs []struct {
		TableName string
		FK        foreignKeyRuntimeMeta
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".frm") {
			continue
		}
		tableName := strings.TrimSuffix(entry.Name(), ".frm")
		fks, err := dml.loadTableForeignKeys(schemaName, tableName)
		if err != nil {
			return nil, err
		}
		for _, fk := range fks {
			if strings.EqualFold(fk.RefTable, parentTable) {
				refs = append(refs, struct {
					TableName string
					FK        foreignKeyRuntimeMeta
				}{TableName: tableName, FK: fk})
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
	fks, err := dml.loadTableForeignKeys(schemaName, dml.tableName)
	if err != nil || len(fks) == 0 {
		return err
	}
	for _, fk := range fks {
		if len(fk.Columns) != 1 || len(fk.RefColumns) != 1 {
			continue
		}
		parentMeta, parentStorage, parentBTree, err := dml.tableAccess(ctx, schemaName, fk.RefTable)
		if err != nil {
			return err
		}
		_ = parentMeta
		for _, row := range rows {
			value := row.ColumnValues[fk.Columns[0]]
			if value == nil {
				continue
			}
			where := fmt.Sprintf("%s = %s", fk.RefColumns[0], sqlLiteral(value))
			matches, err := dml.scanRowsForTableConditions(ctx, schemaName, fk.RefTable, []string{where}, parentMeta, parentStorage, parentBTree)
			if err != nil {
				return err
			}
			if len(matches) == 0 {
				return fmt.Errorf("Cannot add or update a child row: a foreign key constraint fails (%s.%s)", schemaName, dml.tableName)
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
	refs, err := dml.loadReferencingForeignKeys(schemaName, parentTable)
	if err != nil || len(refs) == 0 {
		return nil, err
	}
	changes := make([]transactionDMLChange, 0)
	for _, ref := range refs {
		if !strings.EqualFold(ref.FK.OnDelete, "cascade") || len(ref.FK.Columns) != 1 || len(ref.FK.RefColumns) != 1 {
			continue
		}
		childMeta, childStorage, childBTree, err := dml.tableAccess(ctx, schemaName, ref.TableName)
		if err != nil {
			return nil, err
		}
		for _, parentRow := range parentRows {
			value := parentRow.OldValues[ref.FK.RefColumns[0]]
			where := fmt.Sprintf("%s = %s", ref.FK.Columns[0], sqlLiteral(value))
			childRows, err := dml.scanRowsForTableConditions(ctx, schemaName, ref.TableName, []string{where}, childMeta, childStorage, childBTree)
			if err != nil {
				return nil, err
			}
			for _, childRow := range childRows {
				if err := dml.deleteRowFromStorage(ctx, txn, childRow, childMeta, childStorage, childBTree); err != nil {
					return nil, err
				}
				if err := dml.updateIndexesForDelete(ctx, txn, []*RowUpdateInfo{childRow}, childMeta, childStorage); err != nil {
					return nil, err
				}
				changes = append(changes, transactionDMLChange{
					tableName:  schemaName + "." + ref.TableName,
					kind:       "delete",
					rowID:      childRow.RowId,
					storageKey: childRow.StorageKey,
					before:     cloneTransactionRow(childRow.OldValues),
				})
			}
		}
	}
	return changes, nil
}

func (dml *StorageIntegratedDMLExecutor) applyOnUpdateCascade(
	ctx context.Context,
	txn interface{},
	schemaName string,
	parentTable string,
	parentRows []*RowUpdateInfo,
	updatedRows []*InsertRowData,
) ([]transactionDMLChange, error) {
	refs, err := dml.loadReferencingForeignKeys(schemaName, parentTable)
	if err != nil || len(refs) == 0 {
		return nil, err
	}
	changes := make([]transactionDMLChange, 0)
	for _, ref := range refs {
		if !strings.EqualFold(ref.FK.OnUpdate, "cascade") || len(ref.FK.Columns) != 1 || len(ref.FK.RefColumns) != 1 {
			continue
		}
		childMeta, childStorage, childBTree, err := dml.tableAccess(ctx, schemaName, ref.TableName)
		if err != nil {
			return nil, err
		}
		childColumn := ref.FK.Columns[0]
		parentColumn := ref.FK.RefColumns[0]
		for i, parentRow := range parentRows {
			if i >= len(updatedRows) {
				continue
			}
			oldValue := parentRow.OldValues[parentColumn]
			newValue := updatedRows[i].ColumnValues[parentColumn]
			if compareScalarValues(oldValue, newValue) == 0 {
				continue
			}
			where := fmt.Sprintf("%s = %s", childColumn, sqlLiteral(oldValue))
			childRows, err := dml.scanRowsForTableConditions(ctx, schemaName, ref.TableName, []string{where}, childMeta, childStorage, childBTree)
			if err != nil {
				return nil, err
			}
			updateExprs := []*UpdateExpression{{
				ColumnName: childColumn,
				NewValue:   newValue,
				ColumnType: foreignKeyColumnType(childMeta, childColumn),
				Expr:       nil,
			}}
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
				changes = append(changes, transactionDMLChange{
					tableName:  schemaName + "." + ref.TableName,
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
