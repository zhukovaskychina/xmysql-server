package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// ========================================
// InsertOperator - 插入算子
// ========================================

// InsertOperator 插入算子，执行INSERT操作
type InsertOperator struct {
	BaseOperator
	schemaName string
	tableName  string
	stmt       *sqlparser.Insert

	// 适配器
	storageAdapter     *StorageAdapter
	indexAdapter       *IndexAdapter
	transactionAdapter *TransactionAdapter

	// 执行状态
	executed     bool
	affectedRows int64
}

// NewInsertOperator 创建插入算子
func NewInsertOperator(
	schemaName, tableName string,
	stmt *sqlparser.Insert,
	storageAdapter *StorageAdapter,
	indexAdapter *IndexAdapter,
	transactionAdapter *TransactionAdapter,
) *InsertOperator {
	return &InsertOperator{
		BaseOperator:       BaseOperator{children: nil},
		schemaName:         schemaName,
		tableName:          tableName,
		stmt:               stmt,
		storageAdapter:     storageAdapter,
		indexAdapter:       indexAdapter,
		transactionAdapter: transactionAdapter,
		executed:           false,
		affectedRows:       0,
	}
}

// Open 初始化插入算子
func (i *InsertOperator) Open(ctx context.Context) error {
	if err := i.BaseOperator.Open(ctx); err != nil {
		return err
	}

	// 获取表元数据
	tableMetadata, err := i.storageAdapter.GetTableMetadata(ctx, i.schemaName, i.tableName)
	if err != nil {
		return fmt.Errorf("failed to get table metadata: %w", err)
	}
	if tableMetadata == nil || tableMetadata.Schema == nil {
		return fmt.Errorf("table metadata schema is nil for %s.%s", i.schemaName, i.tableName)
	}

	// 统一使用QuerySchema统一对外输出
	i.schema = metadata.FromTable(tableMetadata.Schema)

	logger.Debugf("InsertOperator opened for table %s.%s", i.schemaName, i.tableName)
	return nil
}

// Next 执行插入操作（一次性执行所有插入）
func (i *InsertOperator) Next(ctx context.Context) (Record, error) {
	if !i.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	// 插入操作只执行一次
	if i.executed {
		return nil, nil // EOF
	}

	// 开始事务
	txn, err := i.transactionAdapter.BeginTransaction(ctx, false, "READ COMMITTED")
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// 执行插入
	affectedRows, err := i.executeInsert(ctx, txn)
	if err != nil {
		// 回滚事务
		if rollbackErr := i.transactionAdapter.RollbackTransaction(ctx, txn); rollbackErr != nil {
			logger.Errorf("insert rollback failed: schema=%s table=%s txn=%d err=%v rollbackErr=%v", i.schemaName, i.tableName, txn.TxnID, err, rollbackErr)
			return nil, fmt.Errorf("insert failed in %s.%s: %w; rollback failed: %v", i.schemaName, i.tableName, err, rollbackErr)
		}

		logger.Debugf("insert rolled back: schema=%s table=%s txn=%d", i.schemaName, i.tableName, txn.TxnID)
		return nil, fmt.Errorf("insert failed in %s.%s: %w", i.schemaName, i.tableName, err)
	}

	// 提交事务
	if err := i.transactionAdapter.CommitTransaction(ctx, txn); err != nil {
		logger.Errorf("insert commit failed: schema=%s table=%s txn=%d err=%v", i.schemaName, i.tableName, txn.TxnID, err)
		return nil, fmt.Errorf("failed to commit transaction for insert on %s.%s: %w", i.schemaName, i.tableName, err)
	}

	i.affectedRows = affectedRows
	i.executed = true

	// 返回结果记录（包含影响行数）
	values := []basic.Value{
		basic.NewInt64Value(affectedRows),
	}

	return NewExecutorRecordFromValues(values, nil), nil
}

// executeInsert 执行实际的插入逻辑
func (i *InsertOperator) executeInsert(ctx context.Context, txn *Transaction) (int64, error) {
	logger.Debugf("Executing INSERT on table %s.%s", i.schemaName, i.tableName)

	// 1. 获取表元数据
	tableSchema, err := i.getTableSchema()
	if err != nil {
		return 0, fmt.Errorf("failed to get table schema: %v", err)
	}

	// 2. 解析INSERT语句中的值
	rows, err := i.parseInsertRows(tableSchema)
	if err != nil {
		return 0, fmt.Errorf("failed to parse insert rows: %v", err)
	}

	affectedRows := int64(0)

	// 3. 逐行插入
	for _, row := range rows {
		// 检查是否有ON DUPLICATE KEY UPDATE子句
		if i.stmt.OnDup != nil && len(i.stmt.OnDup) > 0 {
			// 执行INSERT ... ON DUPLICATE KEY UPDATE
			inserted, err := i.insertOrUpdate(ctx, txn, row, tableSchema)
			if err != nil {
				return 0, fmt.Errorf("failed to insert or update: %v", err)
			}
			if inserted {
				affectedRows++
			} else {
				affectedRows += 2 // MySQL convention: UPDATE counts as 2 affected rows
			}
		} else {
			// 普通INSERT
			err := i.insertRow(ctx, txn, row, tableSchema)
			if err != nil {
				return 0, fmt.Errorf("failed to insert row: %v", err)
			}
			affectedRows++
		}
	}

	return affectedRows, nil
}

// insertOrUpdate 执行INSERT ... ON DUPLICATE KEY UPDATE逻辑
// 返回true表示插入成功，false表示更新成功
func (i *InsertOperator) insertOrUpdate(ctx context.Context, txn *Transaction, row map[string]interface{}, schema *metadata.Table) (bool, error) {
	// 1. 尝试插入
	err := i.insertRow(ctx, txn, row, schema)
	if err == nil {
		// 插入成功
		return true, nil
	}

	// 2. 检查是否是主键冲突或唯一键冲突
	if !i.isDuplicateKeyError(err) {
		// 其他错误，直接返回
		return false, err
	}

	logger.Debugf("Duplicate key detected, executing UPDATE clause")

	// 3. 执行UPDATE操作
	err = i.updateOnDuplicate(ctx, txn, row, schema)
	if err != nil {
		return false, fmt.Errorf("failed to update on duplicate: %v", err)
	}

	// 更新成功
	return false, nil
}

// updateOnDuplicate 执行ON DUPLICATE KEY UPDATE子句
func (i *InsertOperator) updateOnDuplicate(ctx context.Context, txn *Transaction, row map[string]interface{}, schema *metadata.Table) error {
	// 1. 查找冲突的记录
	existingRecord, err := i.findDuplicateRecord(ctx, txn, row, schema)
	if err != nil {
		return fmt.Errorf("failed to find duplicate record: %v", err)
	}
	if existingRecord == nil {
		return fmt.Errorf("duplicate record not found")
	}

	// 2. 应用ON DUPLICATE KEY UPDATE子句
	updatedRecord, err := i.applyOnDupUpdate(existingRecord, row, schema)
	if err != nil {
		return fmt.Errorf("failed to apply ON DUPLICATE KEY UPDATE: %v", err)
	}

	// 3. 更新记录
	err = i.updateRecord(ctx, txn, existingRecord, updatedRecord, schema)
	if err != nil {
		return fmt.Errorf("failed to update record: %v", err)
	}

	return nil
}

// applyOnDupUpdate 应用ON DUPLICATE KEY UPDATE子句
func (i *InsertOperator) applyOnDupUpdate(existingRecord Record, insertRow map[string]interface{}, schema *metadata.Table) (map[string]interface{}, error) {
	// 创建更新后的记录（从现有记录开始）
	updatedRecord := make(map[string]interface{})

	// 复制现有记录的值
	existingValues := existingRecord.GetValues()
	for idx, col := range schema.Columns {
		if idx < len(existingValues) {
			updatedRecord[col.Name] = i.valueToInterface(existingValues[idx])
		}
	}

	// 应用ON DUPLICATE KEY UPDATE表达式
	for _, updateExpr := range i.stmt.OnDup {
		colName := updateExpr.Name.Name.String()

		// 计算更新表达式的值
		newValue, err := i.evaluateOnDupExpr(updateExpr.Expr, existingRecord, insertRow, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate ON DUPLICATE KEY UPDATE expression for column %s: %v", colName, err)
		}

		updatedRecord[colName] = newValue
	}

	return updatedRecord, nil
}

// evaluateOnDupExpr 计算ON DUPLICATE KEY UPDATE表达式
func (i *InsertOperator) evaluateOnDupExpr(expr sqlparser.Expr, existingRecord Record, insertRow map[string]interface{}, schema *metadata.Table) (interface{}, error) {
	switch e := expr.(type) {
	case *sqlparser.SQLVal:
		// 字面值
		return i.sqlValToInterface(e), nil

	case *sqlparser.ColName:
		// 列引用
		colName := e.Name.String()
		// 优先使用插入行的值
		if val, ok := insertRow[colName]; ok {
			return val, nil
		}
		// 否则使用现有记录的值
		existingValues := existingRecord.GetValues()
		for idx, col := range schema.Columns {
			if col.Name == colName && idx < len(existingValues) {
				return i.valueToInterface(existingValues[idx]), nil
			}
		}
		return nil, fmt.Errorf("column %s not found", colName)

	case *sqlparser.FuncExpr:
		// 函数调用（如VALUES()）
		if e.Name.Lowered() == "values" {
			// VALUES(col_name) 返回INSERT语句中的值
			if len(e.Exprs) > 0 {
				if aliasedExpr, ok := e.Exprs[0].(*sqlparser.AliasedExpr); ok {
					if colName, ok := aliasedExpr.Expr.(*sqlparser.ColName); ok {
						colNameStr := colName.Name.String()
						if val, ok := insertRow[colNameStr]; ok {
							return val, nil
						}
					}
				}
			}
			return nil, fmt.Errorf("VALUES() function requires column name")
		}
		// 其他函数暂不支持
		return nil, fmt.Errorf("function %s not supported in ON DUPLICATE KEY UPDATE", e.Name.String())

	case *sqlparser.BinaryExpr:
		// 二元表达式（如 col + 1）
		left, err := i.evaluateOnDupExpr(e.Left, existingRecord, insertRow, schema)
		if err != nil {
			return nil, err
		}
		right, err := i.evaluateOnDupExpr(e.Right, existingRecord, insertRow, schema)
		if err != nil {
			return nil, err
		}
		return i.evaluateBinaryOp(e.Operator, left, right)

	default:
		return nil, fmt.Errorf("unsupported expression type in ON DUPLICATE KEY UPDATE: %T", expr)
	}
}

// evaluateBinaryOp 计算二元操作
func (i *InsertOperator) evaluateBinaryOp(operator string, left, right interface{}) (interface{}, error) {
	// 简化实现：只支持数值运算
	leftInt, leftOk := i.toInt64(left)
	rightInt, rightOk := i.toInt64(right)

	if !leftOk || !rightOk {
		return nil, fmt.Errorf("binary operation requires numeric operands")
	}

	switch operator {
	case "+":
		return leftInt + rightInt, nil
	case "-":
		return leftInt - rightInt, nil
	case "*":
		return leftInt * rightInt, nil
	case "/":
		if rightInt == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return leftInt / rightInt, nil
	default:
		return nil, fmt.Errorf("unsupported binary operator: %s", operator)
	}
}

// toInt64 尝试将值转换为int64
func (i *InsertOperator) toInt64(val interface{}) (int64, bool) {
	switch v := val.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		return int64(v), true
	default:
		return 0, false
	}
}

// isDuplicateKeyError 检查是否是主键/唯一键冲突错误
func (i *InsertOperator) isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, basic.ErrDuplicateKey) {
		return true
	}

	var sqlErr *common.SQLError
	if errors.As(err, &sqlErr) {
		switch sqlErr.Code {
		case common.ErrDupEntry, common.ErrDupEntryWithKeyName, common.ErrDupEntryAutoincrementCase:
			return true
		}
	}

	return false
}

// findDuplicateRecord 查找冲突的记录
func (i *InsertOperator) findDuplicateRecord(ctx context.Context, txn *Transaction, row map[string]interface{}, schema *metadata.Table) (Record, error) {
	if i.storageAdapter == nil {
		return nil, fmt.Errorf("storage adapter is nil")
	}
	record, err := i.storageAdapter.FindDuplicateRecord(ctx, i.schemaName, i.tableName, row, schema, txn)
	if err != nil {
		if errors.Is(err, ErrStorageAdapterRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return record, nil
}

// insertRow 插入单行记录
func (i *InsertOperator) insertRow(ctx context.Context, txn *Transaction, row map[string]interface{}, schema *metadata.Table) error {
	if i.storageAdapter == nil {
		return fmt.Errorf("storage adapter is nil")
	}
	return i.storageAdapter.InsertRecord(ctx, i.schemaName, i.tableName, row, schema, txn)
}

// updateRecord 更新记录
func (i *InsertOperator) updateRecord(ctx context.Context, txn *Transaction, oldRecord Record, newRecord map[string]interface{}, schema *metadata.Table) error {
	if i.storageAdapter == nil {
		return fmt.Errorf("storage adapter is nil")
	}
	newExecutorRecord, err := mapToExecutorRecord(newRecord, schema)
	if err != nil {
		return err
	}
	return i.storageAdapter.UpdateRecord(ctx, i.schemaName, i.tableName, oldRecord, newExecutorRecord, schema, txn)
}

// getTableSchema 获取表Schema
func (i *InsertOperator) getTableSchema() (*metadata.Table, error) {
	if i.storageAdapter == nil {
		return nil, fmt.Errorf("storage adapter is nil")
	}

	tableMetadata, err := i.storageAdapter.GetTableMetadata(context.Background(), i.schemaName, i.tableName)
	if err != nil {
		return nil, err
	}
	if tableMetadata == nil || tableMetadata.Schema == nil {
		return nil, fmt.Errorf("table metadata schema is nil for %s.%s", i.schemaName, i.tableName)
	}

	return tableMetadata.Schema, nil
}

// parseInsertRows 解析INSERT语句中的行数据
func (i *InsertOperator) parseInsertRows(schema *metadata.Table) ([]map[string]interface{}, error) {
	if i.stmt == nil {
		return nil, fmt.Errorf("insert statement is nil")
	}
	if schema == nil {
		return nil, fmt.Errorf("table schema is nil")
	}

	values, ok := i.stmt.Rows.(sqlparser.Values)
	if !ok {
		return nil, fmt.Errorf("unsupported INSERT rows type: %T", i.stmt.Rows)
	}

	columnNames, err := i.resolveInsertColumnNames(schema)
	if err != nil {
		return nil, err
	}

	rows := make([]map[string]interface{}, 0, len(values))
	for rowIdx, tuple := range values {
		if len(tuple) != len(columnNames) {
			return nil, fmt.Errorf("insert row %d has %d values, expected %d", rowIdx, len(tuple), len(columnNames))
		}

		row := make(map[string]interface{}, len(columnNames))
		for colIdx, expr := range tuple {
			value, err := i.exprToInterface(expr)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate insert value at row %d column %s: %w", rowIdx, columnNames[colIdx], err)
			}
			row[columnNames[colIdx]] = value
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// valueToInterface 将basic.Value转换为interface{}
func (i *InsertOperator) valueToInterface(val basic.Value) interface{} {
	if val == nil {
		return nil
	}
	return val.Raw()
}

// sqlValToInterface 将SQLVal转换为interface{}
func (i *InsertOperator) sqlValToInterface(val *sqlparser.SQLVal) interface{} {
	if val == nil {
		return nil
	}
	value, err := i.sqlValToTypedInterface(val)
	if err != nil {
		return string(val.Val)
	}
	return value
}

func (i *InsertOperator) resolveInsertColumnNames(schema *metadata.Table) ([]string, error) {
	if len(i.stmt.Columns) > 0 {
		columnNames := make([]string, 0, len(i.stmt.Columns))
		for _, col := range i.stmt.Columns {
			colName := col.String()
			if colName == "" {
				return nil, fmt.Errorf("insert column name cannot be empty")
			}
			if _, ok := schema.GetColumn(colName); !ok {
				return nil, fmt.Errorf("column %s not found in table %s", colName, schema.Name)
			}
			columnNames = append(columnNames, colName)
		}
		return columnNames, nil
	}

	if len(schema.Columns) == 0 {
		return nil, fmt.Errorf("insert column list omitted but table %s has no columns", schema.Name)
	}

	columnNames := make([]string, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		if col == nil || col.Name == "" {
			return nil, fmt.Errorf("table %s contains invalid column metadata", schema.Name)
		}
		columnNames = append(columnNames, col.Name)
	}
	return columnNames, nil
}

func (i *InsertOperator) exprToInterface(expr sqlparser.Expr) (interface{}, error) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		return i.sqlValToTypedInterface(v)
	case *sqlparser.NullVal:
		return nil, nil
	case sqlparser.BoolVal:
		return bool(v), nil
	default:
		return nil, fmt.Errorf("unsupported insert expression type: %T", expr)
	}
}

func (i *InsertOperator) sqlValToTypedInterface(val *sqlparser.SQLVal) (interface{}, error) {
	switch val.Type {
	case sqlparser.StrVal:
		return string(val.Val), nil
	case sqlparser.IntVal:
		return strconv.ParseInt(string(val.Val), 10, 64)
	case sqlparser.FloatVal:
		return strconv.ParseFloat(string(val.Val), 64)
	case sqlparser.HexVal:
		return val.Val, nil
	default:
		return string(val.Val), nil
	}
}

// ========================================
// UpdateOperator - 更新算子
// ========================================

// UpdateOperator 更新算子，执行UPDATE操作
type UpdateOperator struct {
	BaseOperator
	schemaName string
	tableName  string
	stmt       *sqlparser.Update

	// 适配器
	storageAdapter     *StorageAdapter
	indexAdapter       *IndexAdapter
	transactionAdapter *TransactionAdapter

	// 子算子（用于扫描需要更新的记录）
	scanOperator Operator

	// 执行状态
	executed     bool
	affectedRows int64
}

// NewUpdateOperator 创建更新算子
func NewUpdateOperator(
	schemaName, tableName string,
	stmt *sqlparser.Update,
	storageAdapter *StorageAdapter,
	indexAdapter *IndexAdapter,
	transactionAdapter *TransactionAdapter,
	scanOperator Operator,
) *UpdateOperator {
	return &UpdateOperator{
		BaseOperator:       BaseOperator{children: []Operator{scanOperator}},
		schemaName:         schemaName,
		tableName:          tableName,
		stmt:               stmt,
		storageAdapter:     storageAdapter,
		indexAdapter:       indexAdapter,
		transactionAdapter: transactionAdapter,
		scanOperator:       scanOperator,
		executed:           false,
		affectedRows:       0,
	}
}

// Open 初始化更新算子
func (u *UpdateOperator) Open(ctx context.Context) error {
	if err := u.BaseOperator.Open(ctx); err != nil {
		return err
	}

	// 获取表元数据
	tableMetadata, err := u.storageAdapter.GetTableMetadata(ctx, u.schemaName, u.tableName)
	if err != nil {
		return fmt.Errorf("failed to get table metadata: %w", err)
	}
	if tableMetadata == nil || tableMetadata.Schema == nil {
		return fmt.Errorf("table metadata schema is nil for %s.%s", u.schemaName, u.tableName)
	}

	// 统一使用QuerySchema
	u.schema = metadata.FromTable(tableMetadata.Schema)

	logger.Debugf("UpdateOperator opened for table %s.%s", u.schemaName, u.tableName)
	return nil
}

// Next 执行更新操作
func (u *UpdateOperator) Next(ctx context.Context) (Record, error) {
	if !u.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	// 更新操作只执行一次
	if u.executed {
		return nil, nil // EOF
	}

	// 开始事务
	txn, err := u.transactionAdapter.BeginTransaction(ctx, false, "READ COMMITTED")
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// 执行更新
	affectedRows, err := u.executeUpdate(ctx, txn)
	if err != nil {
		// 回滚事务
		if rollbackErr := u.transactionAdapter.RollbackTransaction(ctx, txn); rollbackErr != nil {
			logger.Errorf("update rollback failed: schema=%s table=%s txn=%d err=%v rollbackErr=%v", u.schemaName, u.tableName, txn.TxnID, err, rollbackErr)
			return nil, fmt.Errorf("update failed in %s.%s: %w; rollback failed: %v", u.schemaName, u.tableName, err, rollbackErr)
		}

		logger.Debugf("update rolled back: schema=%s table=%s txn=%d", u.schemaName, u.tableName, txn.TxnID)
		return nil, fmt.Errorf("update failed in %s.%s: %w", u.schemaName, u.tableName, err)
	}

	// 提交事务
	if err := u.transactionAdapter.CommitTransaction(ctx, txn); err != nil {
		logger.Errorf("update commit failed: schema=%s table=%s txn=%d err=%v", u.schemaName, u.tableName, txn.TxnID, err)
		return nil, fmt.Errorf("failed to commit transaction for update on %s.%s: %w", u.schemaName, u.tableName, err)
	}

	u.affectedRows = affectedRows
	u.executed = true

	// 返回结果记录（包含影响行数）
	values := []basic.Value{
		basic.NewInt64Value(affectedRows),
	}

	return NewExecutorRecordFromValues(values, nil), nil
}

// executeUpdate 执行实际的更新逻辑
func (u *UpdateOperator) executeUpdate(ctx context.Context, txn *Transaction) (int64, error) {
	logger.Debugf("Executing UPDATE on table %s.%s", u.schemaName, u.tableName)

	affectedRows := int64(0)

	// 获取表的元数据
	tableSchema, err := u.getTableSchema()
	if err != nil {
		return 0, fmt.Errorf("failed to get table schema: %v", err)
	}

	// 扫描需要更新的记录
	for {
		record, err := u.scanOperator.Next(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to scan record: %v", err)
		}
		if record == nil {
			break // EOF
		}

		// 应用SET子句，生成新记录
		newRecord, err := u.applySetClause(record, tableSchema)
		if err != nil {
			return 0, fmt.Errorf("failed to apply SET clause: %v", err)
		}

		// 检查索引列是否变更
		indexColumnsChanged := u.checkIndexColumnsChanged(record, newRecord, tableSchema)

		if indexColumnsChanged {
			// 索引列变更：删除旧记录 + 插入新记录
			err = u.updateWithIndexChange(ctx, txn, record, newRecord, tableSchema)
		} else {
			// 索引列未变更：就地更新
			err = u.updateInPlace(ctx, txn, record, newRecord, tableSchema)
		}

		if err != nil {
			return 0, fmt.Errorf("failed to update record: %v", err)
		}

		affectedRows++
	}

	logger.Debugf("✅ UPDATE completed: affected %d rows", affectedRows)
	return affectedRows, nil
}

// getTableSchema 获取表的元数据
func (u *UpdateOperator) getTableSchema() (*metadata.Table, error) {
	if u.storageAdapter == nil {
		return nil, fmt.Errorf("storage adapter is nil")
	}

	tableMetadata, err := u.storageAdapter.GetTableMetadata(context.Background(), u.schemaName, u.tableName)
	if err != nil {
		return nil, err
	}
	if tableMetadata == nil || tableMetadata.Schema == nil {
		return nil, fmt.Errorf("table metadata schema is nil for %s.%s", u.schemaName, u.tableName)
	}
	return tableMetadata.Schema, nil
}

// applySetClause 应用SET子句到记录
func (u *UpdateOperator) applySetClause(oldRecord Record, schema *metadata.Table) (Record, error) {
	// 获取旧记录的值
	oldValues := oldRecord.GetValues()

	// 创建新值数组（复制旧值）
	newValues := make([]basic.Value, len(oldValues))
	copy(newValues, oldValues)

	if u.stmt == nil {
		return nil, fmt.Errorf("update statement is nil")
	}
	if schema == nil {
		return nil, fmt.Errorf("table schema is nil")
	}

	columnIndex := make(map[string]int, len(schema.Columns))
	for idx, col := range schema.Columns {
		if col == nil {
			continue
		}
		columnIndex[col.Name] = idx
	}

	for _, expr := range u.stmt.Exprs {
		if expr == nil || expr.Name == nil {
			return nil, fmt.Errorf("invalid SET expression")
		}

		colName := expr.Name.Name.String()
		idx, ok := columnIndex[colName]
		if !ok {
			return nil, fmt.Errorf("column %s not found in table %s", colName, schema.Name)
		}
		if idx >= len(newValues) {
			return nil, fmt.Errorf("column %s index %d out of record range %d", colName, idx, len(newValues))
		}

		value, err := updateExprToBasicValue(expr.Expr)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate SET expression for %s: %w", colName, err)
		}
		newValues[idx] = value
	}

	// 创建新记录
	newRecord := NewExecutorRecordFromValues(newValues, nil)
	return newRecord, nil
}

func updateExprToBasicValue(expr sqlparser.Expr) (basic.Value, error) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.StrVal:
			return basic.NewStringValue(string(v.Val)), nil
		case sqlparser.IntVal:
			parsed, err := strconv.ParseInt(string(v.Val), 10, 64)
			if err != nil {
				return nil, err
			}
			return basic.NewInt64Value(parsed), nil
		case sqlparser.FloatVal:
			parsed, err := strconv.ParseFloat(string(v.Val), 64)
			if err != nil {
				return nil, err
			}
			return basic.NewFloatValue(parsed), nil
		default:
			return basic.NewStringValue(string(v.Val)), nil
		}
	case *sqlparser.NullVal:
		return basic.NewNull(), nil
	case sqlparser.BoolVal:
		return basic.NewBool(bool(v)), nil
	default:
		return nil, fmt.Errorf("unsupported expression type %T", expr)
	}
}

// checkIndexColumnsChanged 检查索引列是否变更
func (u *UpdateOperator) checkIndexColumnsChanged(oldRecord, newRecord Record, schema *metadata.Table) bool {
	// 简化实现：假设主键列是第一列
	// 实际应该检查所有索引列

	oldValues := oldRecord.GetValues()
	newValues := newRecord.GetValues()

	if len(oldValues) == 0 || len(newValues) == 0 {
		return false
	}

	// 比较第一列（假设为主键）
	if len(oldValues) > 0 && len(newValues) > 0 {
		// 简化比较：检查值是否相等
		// 实际应该使用Value的比较方法
		return !valuesEqual(oldValues[0], newValues[0])
	}

	return false
}

// updateInPlace 就地更新记录（索引列未变更）
func (u *UpdateOperator) updateInPlace(ctx context.Context, txn *Transaction, oldRecord, newRecord Record, schema *metadata.Table) error {
	logger.Debugf("Performing in-place update")

	if u.storageAdapter == nil {
		return fmt.Errorf("storage adapter is nil")
	}

	return u.storageAdapter.UpdateRecord(ctx, u.schemaName, u.tableName, oldRecord, newRecord, schema, txn)
}

// updateWithIndexChange 更新记录（索引列变更，需要删除+插入）
func (u *UpdateOperator) updateWithIndexChange(ctx context.Context, txn *Transaction, oldRecord, newRecord Record, schema *metadata.Table) error {
	logger.Debugf("Performing update with index change (delete + insert)")

	// 1. 删除旧记录
	err := u.deleteOldRecord(ctx, txn, oldRecord, schema)
	if err != nil {
		return fmt.Errorf("failed to delete old record: %v", err)
	}

	// 2. 插入新记录
	err = u.insertNewRecord(ctx, txn, newRecord, schema)
	if err != nil {
		return fmt.Errorf("failed to insert new record: %v", err)
	}

	logger.Debugf("✅ Update with index change completed")
	return nil
}

// deleteOldRecord 删除旧记录
func (u *UpdateOperator) deleteOldRecord(ctx context.Context, txn *Transaction, record Record, schema *metadata.Table) error {
	if u.storageAdapter == nil {
		return fmt.Errorf("storage adapter is nil")
	}

	return u.storageAdapter.DeleteRecord(ctx, u.schemaName, u.tableName, record, schema, txn)
}

// insertNewRecord 插入新记录
func (u *UpdateOperator) insertNewRecord(ctx context.Context, txn *Transaction, record Record, schema *metadata.Table) error {
	if u.storageAdapter == nil {
		return fmt.Errorf("storage adapter is nil")
	}

	return u.storageAdapter.InsertRecord(ctx, u.schemaName, u.tableName, recordToInsertRow(record, schema), schema, txn)
}

// valuesEqual 比较两个Value是否相等
func valuesEqual(v1, v2 basic.Value) bool {
	// 简化实现：比较字符串表示
	// 实际应该使用Value的Compare方法
	return fmt.Sprintf("%v", v1) == fmt.Sprintf("%v", v2)
}

// ========================================
// DeleteOperator - 删除算子
// ========================================

// DeleteOperator 删除算子，执行DELETE操作
type DeleteOperator struct {
	BaseOperator
	schemaName string
	tableName  string
	stmt       *sqlparser.Delete

	// 适配器
	storageAdapter     *StorageAdapter
	indexAdapter       *IndexAdapter
	transactionAdapter *TransactionAdapter

	// 子算子（用于扫描需要删除的记录）
	scanOperator Operator

	// 执行状态
	executed     bool
	affectedRows int64
}

// NewDeleteOperator 创建删除算子
func NewDeleteOperator(
	schemaName, tableName string,
	stmt *sqlparser.Delete,
	storageAdapter *StorageAdapter,
	indexAdapter *IndexAdapter,
	transactionAdapter *TransactionAdapter,
	scanOperator Operator,
) *DeleteOperator {
	return &DeleteOperator{
		BaseOperator:       BaseOperator{children: []Operator{scanOperator}},
		schemaName:         schemaName,
		tableName:          tableName,
		stmt:               stmt,
		storageAdapter:     storageAdapter,
		indexAdapter:       indexAdapter,
		transactionAdapter: transactionAdapter,
		scanOperator:       scanOperator,
		executed:           false,
		affectedRows:       0,
	}
}

// Open 初始化删除算子
func (d *DeleteOperator) Open(ctx context.Context) error {
	if err := d.BaseOperator.Open(ctx); err != nil {
		return err
	}

	// 获取表元数据
	tableMetadata, err := d.storageAdapter.GetTableMetadata(ctx, d.schemaName, d.tableName)
	if err != nil {
		return fmt.Errorf("failed to get table metadata: %w", err)
	}
	if tableMetadata == nil || tableMetadata.Schema == nil {
		return fmt.Errorf("table metadata schema is nil for %s.%s", d.schemaName, d.tableName)
	}

	// 统一使用QuerySchema
	d.schema = metadata.FromTable(tableMetadata.Schema)

	logger.Debugf("DeleteOperator opened for table %s.%s", d.schemaName, d.tableName)
	return nil
}

// Next 执行删除操作
func (d *DeleteOperator) Next(ctx context.Context) (Record, error) {
	if !d.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	// 删除操作只执行一次
	if d.executed {
		return nil, nil // EOF
	}

	// 开始事务
	txn, err := d.transactionAdapter.BeginTransaction(ctx, false, "READ COMMITTED")
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// 执行删除
	affectedRows, err := d.executeDelete(ctx, txn)
	if err != nil {
		// 回滚事务
		if rollbackErr := d.transactionAdapter.RollbackTransaction(ctx, txn); rollbackErr != nil {
			logger.Errorf("delete rollback failed: schema=%s table=%s txn=%d err=%v rollbackErr=%v", d.schemaName, d.tableName, txn.TxnID, err, rollbackErr)
			return nil, fmt.Errorf("delete failed in %s.%s: %w; rollback failed: %v", d.schemaName, d.tableName, err, rollbackErr)
		}

		logger.Debugf("delete rolled back: schema=%s table=%s txn=%d", d.schemaName, d.tableName, txn.TxnID)
		return nil, fmt.Errorf("delete failed in %s.%s: %w", d.schemaName, d.tableName, err)
	}

	// 提交事务
	if err := d.transactionAdapter.CommitTransaction(ctx, txn); err != nil {
		logger.Errorf("delete commit failed: schema=%s table=%s txn=%d err=%v", d.schemaName, d.tableName, txn.TxnID, err)
		return nil, fmt.Errorf("failed to commit transaction for delete on %s.%s: %w", d.schemaName, d.tableName, err)
	}

	d.affectedRows = affectedRows
	d.executed = true

	// 返回结果记录（包含影响行数）
	values := []basic.Value{
		basic.NewInt64Value(affectedRows),
	}

	return NewExecutorRecordFromValues(values, nil), nil
}

// executeDelete 执行实际的删除逻辑
func (d *DeleteOperator) executeDelete(ctx context.Context, txn *Transaction) (int64, error) {
	if d.scanOperator == nil {
		return 0, fmt.Errorf("scan operator is nil")
	}
	if d.storageAdapter == nil {
		return 0, fmt.Errorf("storage adapter is nil")
	}

	logger.Debugf("Executing DELETE on table %s.%s", d.schemaName, d.tableName)

	affectedRows := int64(0)

	for {
		record, err := d.scanOperator.Next(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to scan record: %v", err)
		}
		if record == nil {
			break
		}

		if err := d.deleteRecord(ctx, txn, record); err != nil {
			return 0, fmt.Errorf("failed to delete record: %v", err)
		}

		affectedRows++
	}

	logger.Debugf("✅ DELETE completed: affected %d rows", affectedRows)
	return affectedRows, nil
}

func (d *DeleteOperator) deleteRecord(ctx context.Context, txn *Transaction, record Record) error {
	if d.storageAdapter == nil {
		return fmt.Errorf("storage adapter is nil")
	}
	if d.storageAdapter.deleteRecordFunc != nil {
		return d.storageAdapter.DeleteRecord(ctx, d.schemaName, d.tableName, record, nil, txn)
	}

	tableSchema, err := d.storageAdapter.GetTableMetadata(ctx, d.schemaName, d.tableName)
	if err != nil {
		return err
	}
	if tableSchema == nil || tableSchema.Schema == nil {
		return fmt.Errorf("table metadata schema is nil for %s.%s", d.schemaName, d.tableName)
	}

	return d.storageAdapter.DeleteRecord(ctx, d.schemaName, d.tableName, record, tableSchema.Schema, txn)
}

func mapToExecutorRecord(row map[string]interface{}, schema *metadata.Table) (Record, error) {
	if schema == nil {
		return nil, fmt.Errorf("table schema is nil")
	}
	values := make([]basic.Value, len(schema.Columns))
	for idx, col := range schema.Columns {
		if col == nil {
			continue
		}
		raw, ok := row[col.Name]
		if !ok {
			if col.DefaultValue != nil {
				raw = col.DefaultValue
			} else if col.IsNullable {
				raw = nil
			} else {
				return nil, fmt.Errorf("column %s has no value and no default", col.Name)
			}
		}
		value, err := interfaceToBasicValue(raw)
		if err != nil {
			return nil, fmt.Errorf("convert column %s: %w", col.Name, err)
		}
		values[idx] = value
	}
	return NewExecutorRecordFromValues(values, metadata.FromTable(schema)), nil
}

func recordToInsertRow(record Record, schema *metadata.Table) map[string]interface{} {
	row := make(map[string]interface{})
	if record == nil || schema == nil {
		return row
	}
	values := record.GetValues()
	for idx, col := range schema.Columns {
		if col == nil || idx >= len(values) || values[idx] == nil {
			continue
		}
		row[col.Name] = basicValueToInterface(values[idx])
	}
	return row
}

func basicValueToInterface(value basic.Value) interface{} {
	if value == nil || value.IsNull() {
		return nil
	}
	switch value.Type() {
	case basic.ValueTypeInt, basic.ValueTypeTinyInt, basic.ValueTypeSmallInt, basic.ValueTypeMediumInt, basic.ValueTypeBigInt:
		return value.Int()
	case basic.ValueTypeFloat, basic.ValueTypeDouble, basic.ValueTypeDecimal:
		return value.Float64()
	case basic.ValueTypeBool, basic.ValueTypeBoolean:
		return value.Bool()
	default:
		return value.ToString()
	}
}

func interfaceToBasicValue(raw interface{}) (basic.Value, error) {
	switch v := raw.(type) {
	case nil:
		return basic.NewNull(), nil
	case basic.Value:
		return v, nil
	case int:
		return basic.NewInt64Value(int64(v)), nil
	case int32:
		return basic.NewInt64Value(int64(v)), nil
	case int64:
		return basic.NewInt64Value(v), nil
	case uint:
		return basic.NewInt64Value(int64(v)), nil
	case uint32:
		return basic.NewInt64Value(int64(v)), nil
	case uint64:
		if v > uint64(^uint64(0)>>1) {
			return nil, fmt.Errorf("integer overflows int64")
		}
		return basic.NewInt64Value(int64(v)), nil
	case float32:
		return basic.NewFloatValue(float64(v)), nil
	case float64:
		return basic.NewFloatValue(v), nil
	case bool:
		return basic.NewBool(v), nil
	case string:
		return basic.NewStringValue(v), nil
	case []byte:
		return basic.NewStringValue(string(v)), nil
	default:
		return basic.NewStringValue(fmt.Sprintf("%v", v)), nil
	}
}
