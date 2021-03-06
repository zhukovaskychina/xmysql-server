// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	types "github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/meta"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/schemas"

	"github.com/juju/errors"
	_ "github.com/sirupsen/logrus"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/ast"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/context"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/model"
	_ "github.com/zhukovaskychina/xmysql-server/server/mysql"
)

// adjustColumnInfoInAddColumn is used to set the correct position of column info when adding column.
// 1. The added column was append at the end of tblInfo.Columns, due to ddl state was not public then.
//    It should be moved to the correct position when the ddl state to be changed to public.
// 2. The offset of column should also to be set to the right value.
func (d *ddl) adjustColumnInfoInAddColumn(tblInfo *model.TableInfo, offset int) {
	oldCols := tblInfo.Columns
	newCols := make([]*model.ColumnInfo, 0, len(oldCols))
	newCols = append(newCols, oldCols[:offset]...)
	newCols = append(newCols, oldCols[len(oldCols)-1])
	newCols = append(newCols, oldCols[offset:len(oldCols)-1]...)
	// Adjust column offset.
	offsetChanged := make(map[int]int)
	for i := offset + 1; i < len(newCols); i++ {
		offsetChanged[newCols[i].Offset] = i
		newCols[i].Offset = i
	}
	newCols[offset].Offset = offset
	// Update index column offset info.
	// TODO: There may be some corner cases for index column offsets, we may check this later.
	for _, idx := range tblInfo.Indices {
		for _, col := range idx.Columns {
			newOffset, ok := offsetChanged[col.Offset]
			if ok {
				col.Offset = newOffset
			}
		}
	}
	tblInfo.Columns = newCols
}

// adjustColumnInfoInDropColumn is used to set the correct position of column info when dropping column.
// 1. The offset of column should to be set to the last of the columns.
// 2. The dropped column is moved to the end of tblInfo.Columns, due to it was not public any more.
func (d *ddl) adjustColumnInfoInDropColumn(tblInfo *model.TableInfo, offset int) {
	oldCols := tblInfo.Columns
	// Adjust column offset.
	offsetChanged := make(map[int]int)
	for i := offset + 1; i < len(oldCols); i++ {
		offsetChanged[oldCols[i].Offset] = i - 1
		oldCols[i].Offset = i - 1
	}
	oldCols[offset].Offset = len(oldCols) - 1
	// Update index column offset info.
	// TODO: There may be some corner cases for index column offsets, we may check this later.
	for _, idx := range tblInfo.Indices {
		for _, col := range idx.Columns {
			newOffset, ok := offsetChanged[col.Offset]
			if ok {
				col.Offset = newOffset
			}
		}
	}
	newCols := make([]*model.ColumnInfo, 0, len(oldCols))
	newCols = append(newCols, oldCols[:offset]...)
	newCols = append(newCols, oldCols[offset+1:]...)
	newCols = append(newCols, oldCols[offset])
	tblInfo.Columns = newCols
}

func (d *ddl) createColumnInfo(tblInfo *model.TableInfo, colInfo *model.ColumnInfo, pos *ast.ColumnPosition) (*model.ColumnInfo, int, error) {
	// Check column name duplicate.
	cols := tblInfo.Columns
	position := len(cols)

	// Get column position.
	if pos.Tp == ast.ColumnPositionFirst {
		position = 0
	} else if pos.Tp == ast.ColumnPositionAfter {
		c := findCol(cols, pos.RelativeColumn.Name.L)
		if c == nil {
			return nil, 0, schemas.ErrColumnNotExists.GenByArgs(pos.RelativeColumn, tblInfo.Name)
		}

		// Insert position is after the mentioned column.
		position = c.Offset + 1
	}
	colInfo.ID = allocateColumnID(tblInfo)
	colInfo.State = model.StateNone
	// To support add column asynchronous, we should mark its offset as the last column.
	// So that we can use origin column offset to get value from row.
	colInfo.Offset = len(cols)

	// Append the column info to the end of the tblInfo.Columns.
	// It will reorder to the right position in "Columns" when it state change to public.
	newCols := make([]*model.ColumnInfo, 0, len(cols)+1)
	newCols = append(newCols, cols...)
	newCols = append(newCols, colInfo)

	tblInfo.Columns = newCols
	return colInfo, position, nil
}

func (d *ddl) onAddColumn(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	col := &model.ColumnInfo{}
	pos := &ast.ColumnPosition{}
	offset := 0
	err = job.DecodeArgs(col, pos, &offset)
	if err != nil {
		job.State = model.JobCancelled
		return ver, errors.Trace(err)
	}

	columnInfo := findCol(tblInfo.Columns, col.Name.L)
	if columnInfo != nil {
		if columnInfo.State == model.StatePublic {
			// We already have a column with the same column name.
			job.State = model.JobCancelled
			return ver, schemas.ErrColumnExists.GenByArgs(col.Name)
		}
	} else {
		columnInfo, offset, err = d.createColumnInfo(tblInfo, col, pos)
		if err != nil {
			job.State = model.JobCancelled
			return ver, errors.Trace(err)
		}
		// Set offset arg to job.
		if offset != 0 {
			job.Args = []interface{}{columnInfo, pos, offset}
		}
	}

	originalState := columnInfo.State
	switch columnInfo.State {
	case model.StateNone:
		// none -> delete only
		job.SchemaState = model.StateDeleteOnly
		columnInfo.State = model.StateDeleteOnly
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
	case model.StateDeleteOnly:
		// delete only -> write only
		job.SchemaState = model.StateWriteOnly
		columnInfo.State = model.StateWriteOnly
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
	case model.StateWriteOnly:
		// write only -> reorganization
		job.SchemaState = model.StateWriteReorganization
		columnInfo.State = model.StateWriteReorganization
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
	case model.StateWriteReorganization:
		// reorganization -> public
		// Adjust table column offset.
		d.adjustColumnInfoInAddColumn(tblInfo, offset)
		columnInfo.State = model.StatePublic
		job.SchemaState = model.StatePublic
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		job.State = model.JobDone
		job.BinlogInfo.AddTableInfo(ver, tblInfo)
		d.asyncNotifyEvent(&Event{Tp: model.ActionAddColumn, TableInfo: tblInfo, ColumnInfo: columnInfo})
	default:
		err = ErrInvalidColumnState.Gen("invalid column state %v", columnInfo.State)
	}

	return ver, errors.Trace(err)
}

func (d *ddl) onDropColumn(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var colName model.CIStr
	err = job.DecodeArgs(&colName)
	if err != nil {
		job.State = model.JobCancelled
		return ver, errors.Trace(err)
	}

	colInfo := findCol(tblInfo.Columns, colName.L)
	if colInfo == nil {
		job.State = model.JobCancelled
		return ver, ErrCantDropFieldOrKey.Gen("column %s doesn't exist", colName)
	}
	if err = isDroppableColumn(tblInfo, colName); err != nil {
		job.State = model.JobCancelled
		return ver, errors.Trace(err)
	}

	originalState := colInfo.State
	switch colInfo.State {
	case model.StatePublic:
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		colInfo.State = model.StateWriteOnly
		// Set this column's offset to the last and reset all following columns' offsets.
		d.adjustColumnInfoInDropColumn(tblInfo, colInfo.Offset)
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		colInfo.State = model.StateDeleteOnly
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
	case model.StateDeleteOnly:
		// delete only -> reorganization
		job.SchemaState = model.StateDeleteReorganization
		colInfo.State = model.StateDeleteReorganization
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
	case model.StateDeleteReorganization:
		// reorganization -> absent
		// All reorganization jobs are done, drop this column.
		tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-1]
		colInfo.State = model.StateNone
		job.SchemaState = model.StateNone
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		job.State = model.JobDone
		job.BinlogInfo.AddTableInfo(ver, tblInfo)
		d.asyncNotifyEvent(&Event{Tp: model.ActionDropColumn, TableInfo: tblInfo, ColumnInfo: colInfo})
	default:
		err = ErrInvalidTableState.Gen("invalid table state %v", tblInfo.State)
	}
	return ver, errors.Trace(err)
}

const (
	defaultBatchCnt      = 1024
	defaultSmallBatchCnt = 128
)

// addTableColumn adds a column to the table.
// TODO: Use it when updating the column type or remove it.
// How to backfill column data in reorganization state?
//  1. Generate a snapshot with special version.
//  2. Traverse the snapshot, get every row in the table.
//  3. For one row, if the row has been already deleted, skip to next row.
//  4. If not deleted, check whether column data has existed, if existed, skip to next row.
//  5. If column data doesn't exist, backfill the column with default value and then continue to handle next row.
func (d *ddl) addTableColumn(t schemas.Table, columnInfo *model.ColumnInfo, reorgInfo *reorgInfo, job *model.Job) error {
	//seekHandle := reorgInfo.Handle
	//version := reorgInfo.SnapshotVer
	//count := job.GetRowCount()
	//ctx := d.newContext()
	//
	//colMeta := &columnMeta{
	//	colID:     columnInfo.ID,
	//	oldColMap: make(map[int64]*types.FieldType)}
	//handles := make([]int64, 0, defaultBatchCnt)
	//// Get column default value.
	//var err error
	//if columnInfo.DefaultValue != nil {
	//	colMeta.defaultVal, err = table.GetColDefaultValue(ctx, columnInfo)
	//	if err != nil {
	//		job.State = model.JobCancelled
	//		log.Errorf("[ddl] fatal: this case shouldn't happen, column %v err %v", columnInfo, err)
	//		return errors.Trace(err)
	//	}
	//} else if mysql.HasNotNullFlag(columnInfo.Flag) {
	//	colMeta.defaultVal = table.GetZeroValue(columnInfo)
	//}
	//for _, col := range t.Meta().Columns {
	//	colMeta.oldColMap[col.ID] = &col.FieldType
	//}
	//
	//for {
	//	startTime := time.Now()
	//	handles = handles[:0]
	//	err = iterateSnapshotRows(d.store, t, version, seekHandle,
	//		func(h int64, rowKey kv.Key, rawRecord []byte) (bool, error) {
	//			handles = append(handles, h)
	//			if len(handles) == defaultBatchCnt {
	//				return false, nil
	//			}
	//			return true, nil
	//		})
	//	if err != nil {
	//		return errors.Trace(err)
	//	} else if len(handles) == 0 {
	//		return nil
	//	}
	//
	//	count += int64(len(handles))
	//	seekHandle = handles[len(handles)-1] + 1
	//	sub := time.Since(startTime).Seconds()
	//	err = d.backfillColumn(ctx, t, colMeta, handles, reorgInfo)
	//	if err != nil {
	//		log.Warnf("[ddl] added column for %v rows failed, take time %v", count, sub)
	//		return errors.Trace(err)
	//	}
	//
	//	d.reorgCtx.setRowCountAndHandle(count, seekHandle)
	//	batchHandleDataHistogram.WithLabelValues(batchAddCol).Observe(sub)
	//	log.Infof("[ddl] added column for %v rows, take time %v", count, sub)
	//}
	return nil
}

// backfillColumnInTxn deals with a part of backfilling column data in a Transaction.
// This part of the column data rows is defaultSmallBatchCnt.
//func (d *ddl) backfillColumnInTxn(t table.Table, colMeta *columnMeta, handles []int64, txn kv.Transaction) (int64, error) {
//	//nextHandle := handles[0]
//	//for _, handle := range handles {
//	//	log.Debug("[ddl] backfill column...", handle)
//	//	rowKey := t.RecordKey(handle)
//	//	rowVal, err := txn.Get(rowKey)
//	//	if err != nil {
//	//		if kv.ErrNotExist.Equal(err) {
//	//			// If row doesn't exist, skip it.
//	//			continue
//	//		}
//	//		return 0, errors.Trace(err)
//	//	}
//	//
//	//	rowColumns, err := tablecodec.DecodeRow(rowVal, colMeta.oldColMap, time.UTC)
//	//	if err != nil {
//	//		return 0, errors.Trace(err)
//	//	}
//	//	if _, ok := rowColumns[colMeta.colID]; ok {
//	//		// The column is already added by update or insert statement, skip it.
//	//		continue
//	//	}
//	//
//	//	newColumnIDs := make([]int64, 0, len(rowColumns)+1)
//	//	newRow := make([]types.Datum, 0, len(rowColumns)+1)
//	//	for colID, val := range rowColumns {
//	//		newColumnIDs = append(newColumnIDs, colID)
//	//		newRow = append(newRow, val)
//	//	}
//	//	newColumnIDs = append(newColumnIDs, colMeta.colID)
//	//	newRow = append(newRow, colMeta.defaultVal)
//	//	newRowVal, err := tablecodec.EncodeRow(newRow, newColumnIDs, time.UTC)
//	//	if err != nil {
//	//		return 0, errors.Trace(err)
//	//	}
//	//	err = txn.Set(rowKey, newRowVal)
//	//	if err != nil {
//	//		return 0, errors.Trace(err)
//	//	}
//	//}
//	//
//	//return nextHandle, nil
//}

type columnMeta struct {
	colID      int64
	defaultVal types.Datum
	oldColMap  map[int64]*types.FieldType
}

func (d *ddl) backfillColumn(ctx context.Context, t schemas.Table, colMeta *columnMeta, handles []int64, reorgInfo *reorgInfo) error {
	//var endIdx int
	//for len(handles) > 0 {
	//	if len(handles) >= defaultSmallBatchCnt {
	//		endIdx = defaultSmallBatchCnt
	//	} else {
	//		endIdx = len(handles)
	//	}
	//
	//	err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
	//		if err := d.isReorgRunnable(); err != nil {
	//			return errors.Trace(err)
	//		}
	//
	//		nextHandle, err1 := d.backfillColumnInTxn(t, colMeta, handles[:endIdx], txn)
	//		if err1 != nil {
	//			return errors.Trace(err1)
	//		}
	//		return errors.Trace(reorgInfo.UpdateHandle(txn, nextHandle))
	//	})
	//
	//	if err != nil {
	//		return errors.Trace(err)
	//	}
	//	handles = handles[endIdx:]
	//}

	return nil
}

func (d *ddl) onSetDefaultValue(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	newCol := &model.ColumnInfo{}
	err := job.DecodeArgs(newCol)
	if err != nil {
		job.State = model.JobCancelled
		return ver, errors.Trace(err)
	}

	return d.updateColumn(t, job, newCol, &newCol.Name)
}

func (d *ddl) onModifyColumn(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	newCol := &model.ColumnInfo{}
	oldColName := &model.CIStr{}
	pos := &ast.ColumnPosition{}
	err := job.DecodeArgs(newCol, oldColName, pos)
	if err != nil {
		job.State = model.JobCancelled
		return ver, errors.Trace(err)
	}

	return d.doModifyColumn(t, job, newCol, oldColName, pos)
}

// doModifyColumn updates the column information and reorders all columns.
func (d *ddl) doModifyColumn(t *meta.Meta, job *model.Job, newCol *model.ColumnInfo, oldName *model.CIStr, pos *ast.ColumnPosition) (ver int64, _ error) {
	tblInfo, err := getTableInfo(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	oldCol := findCol(tblInfo.Columns, oldName.L)
	if oldCol == nil || oldCol.State != model.StatePublic {
		job.State = model.JobCancelled
		return ver, schemas.ErrColumnNotExists.GenByArgs(oldName, tblInfo.Name)
	}

	// We need the latest column's offset and state. This information can be obtained from the storebytes.
	newCol.Offset = oldCol.Offset
	newCol.State = oldCol.State
	// Calculate column's new position.
	oldPos, newPos := oldCol.Offset, oldCol.Offset
	if pos.Tp == ast.ColumnPositionAfter {
		if oldName.L == pos.RelativeColumn.Name.L {
			// `alter table tableName modify column b int after b` will return ver,ErrColumnNotExists.
			job.State = model.JobCancelled
			return ver, schemas.ErrColumnNotExists.GenByArgs(oldName, tblInfo.Name)
		}

		relative := findCol(tblInfo.Columns, pos.RelativeColumn.Name.L)
		if relative == nil || relative.State != model.StatePublic {
			job.State = model.JobCancelled
			return ver, schemas.ErrColumnNotExists.GenByArgs(pos.RelativeColumn, tblInfo.Name)
		}

		if relative.Offset < oldPos {
			newPos = relative.Offset + 1
		} else {
			newPos = relative.Offset
		}
	} else if pos.Tp == ast.ColumnPositionFirst {
		newPos = 0
	}

	columnChanged := make(map[string]*model.ColumnInfo)
	columnChanged[oldName.L] = newCol

	if newPos == oldPos {
		tblInfo.Columns[newPos] = newCol
	} else {
		cols := tblInfo.Columns

		// Reorder columns in place.
		if newPos < oldPos {
			copy(cols[newPos+1:], cols[newPos:oldPos])
		} else {
			copy(cols[oldPos:], cols[oldPos+1:newPos+1])
		}
		cols[newPos] = newCol

		for i, col := range tblInfo.Columns {
			if col.Offset != i {
				columnChanged[col.Name.L] = col
				col.Offset = i
			}
		}
	}

	// Change offset and name in indices.
	for _, idx := range tblInfo.Indices {
		for _, c := range idx.Columns {
			if newCol, ok := columnChanged[c.Name.L]; ok {
				c.Name = newCol.Name
				c.Offset = newCol.Offset
			}
		}
	}

	originalState := job.SchemaState
	job.SchemaState = model.StatePublic
	ver, err = updateTableInfo(t, job, tblInfo, originalState)
	if err != nil {
		job.State = model.JobCancelled
		return ver, errors.Trace(err)
	}

	job.State = model.JobDone
	job.BinlogInfo.AddTableInfo(ver, tblInfo)
	return ver, nil
}

func (d *ddl) updateColumn(t *meta.Meta, job *model.Job, newCol *model.ColumnInfo, oldColName *model.CIStr) (ver int64, _ error) {
	tblInfo, err := getTableInfo(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	oldCol := findCol(tblInfo.Columns, oldColName.L)
	if oldCol == nil || oldCol.State != model.StatePublic {
		job.State = model.JobCancelled
		return ver, schemas.ErrColumnNotExists.GenByArgs(newCol.Name, tblInfo.Name)
	}
	*oldCol = *newCol

	originalState := job.SchemaState
	job.SchemaState = model.StatePublic
	ver, err = updateTableInfo(t, job, tblInfo, originalState)
	if err != nil {
		job.State = model.JobCancelled
		return ver, errors.Trace(err)
	}

	job.State = model.JobDone
	job.BinlogInfo.AddTableInfo(ver, tblInfo)
	return ver, nil
}

func isColumnWithIndex(colName string, indices []*model.IndexInfo) bool {
	for _, indexInfo := range indices {
		for _, col := range indexInfo.Columns {
			if col.Name.L == colName {
				return true
			}
		}
	}
	return false
}

func allocateColumnID(tblInfo *model.TableInfo) int64 {
	tblInfo.MaxColumnID++
	return tblInfo.MaxColumnID
}
