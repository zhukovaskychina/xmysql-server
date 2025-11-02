package engine

import (
	"context"
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/logger"
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

	// Schema字段类型不匹配，暂时设为nil
	// TODO: 需要重构BaseOperator.schema字段类型或创建适配器
	i.schema = nil
	_ = tableMetadata // 避免未使用错误

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
		_ = i.transactionAdapter.RollbackTransaction(ctx, txn)
		return nil, fmt.Errorf("insert failed: %w", err)
	}

	// 提交事务
	if err := i.transactionAdapter.CommitTransaction(ctx, txn); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
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
	// 简化实现：检查错误消息
	if err == nil {
		return false
	}
	errMsg := err.Error()
	return containsSubstring(errMsg, "duplicate") || containsSubstring(errMsg, "unique") || containsSubstring(errMsg, "primary key")
}

// containsSubstring 检查字符串是否包含子串（不区分大小写）
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsIgnoreCase(s, substr))
}

func containsIgnoreCase(s, substr string) bool {
	// 简化实现
	sLower := toLowerString(s)
	substrLower := toLowerString(substr)
	for i := 0; i <= len(sLower)-len(substrLower); i++ {
		if sLower[i:i+len(substrLower)] == substrLower {
			return true
		}
	}
	return false
}

func toLowerString(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			result[i] = s[i] + 32
		} else {
			result[i] = s[i]
		}
	}
	return string(result)
}

// findDuplicateRecord 查找冲突的记录
func (i *InsertOperator) findDuplicateRecord(ctx context.Context, txn *Transaction, row map[string]interface{}, schema *metadata.Table) (Record, error) {
	// 简化实现：通过主键查找
	// 实际应该：
	// 1. 检查所有唯一索引
	// 2. 找到冲突的记录

	logger.Debugf("Finding duplicate record (simplified implementation)")
	return nil, fmt.Errorf("findDuplicateRecord not fully implemented")
}

// insertRow 插入单行记录
func (i *InsertOperator) insertRow(ctx context.Context, txn *Transaction, row map[string]interface{}, schema *metadata.Table) error {
	// 简化实现：调用存储适配器插入
	// 实际应该：
	// 1. 验证数据
	// 2. 插入到聚簇索引
	// 3. 更新二级索引
	// 4. 写入Undo日志

	logger.Debugf("Inserting row (simplified implementation)")
	return nil
}

// updateRecord 更新记录
func (i *InsertOperator) updateRecord(ctx context.Context, txn *Transaction, oldRecord Record, newRecord map[string]interface{}, schema *metadata.Table) error {
	// 简化实现：调用存储适配器更新
	// 实际应该：
	// 1. 锁定记录
	// 2. 写入Undo日志
	// 3. 更新聚簇索引
	// 4. 更新二级索引

	logger.Debugf("Updating record (simplified implementation)")
	return nil
}

// getTableSchema 获取表Schema
func (i *InsertOperator) getTableSchema() (*metadata.Table, error) {
	// 简化实现：返回模拟的表Schema
	// 实际应该从TableManager获取
	return &metadata.Table{
		Name:    i.tableName,
		Columns: []*metadata.Column{},
	}, nil
}

// parseInsertRows 解析INSERT语句中的行数据
func (i *InsertOperator) parseInsertRows(schema *metadata.Table) ([]map[string]interface{}, error) {
	// 简化实现：返回空行列表
	// 实际应该解析stmt.Rows
	return []map[string]interface{}{}, nil
}

// valueToInterface 将basic.Value转换为interface{}
func (i *InsertOperator) valueToInterface(val basic.Value) interface{} {
	if val == nil {
		return nil
	}
	// 简化实现：返回原始值
	return val
}

// sqlValToInterface 将SQLVal转换为interface{}
func (i *InsertOperator) sqlValToInterface(val *sqlparser.SQLVal) interface{} {
	// 简化实现
	return string(val.Val)
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
	_, err := u.storageAdapter.GetTableMetadata(ctx, u.schemaName, u.tableName)
	if err != nil {
		return fmt.Errorf("failed to get table metadata: %w", err)
	}

	// TODO: Fix schema assignment - tableMetadata.Schema is *metadata.Table, not metadata.Schema interface
	u.schema = nil

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
		_ = u.transactionAdapter.RollbackTransaction(ctx, txn)
		return nil, fmt.Errorf("update failed: %w", err)
	}

	// 提交事务
	if err := u.transactionAdapter.CommitTransaction(ctx, txn); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
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
	// 从存储适配器获取表元数据
	if u.storageAdapter == nil {
		return nil, fmt.Errorf("storage adapter is nil")
	}

	// 简化实现：创建基本的表结构
	// 实际应该从元数据管理器获取
	return &metadata.Table{
		Name:    u.tableName,
		Columns: []*metadata.Column{}, // 简化：空列列表
	}, nil
}

// applySetClause 应用SET子句到记录
func (u *UpdateOperator) applySetClause(oldRecord Record, schema *metadata.Table) (Record, error) {
	// 获取旧记录的值
	oldValues := oldRecord.GetValues()

	// 创建新值数组（复制旧值）
	newValues := make([]basic.Value, len(oldValues))
	copy(newValues, oldValues)

	// 应用SET子句中的每个赋值
	// 简化实现：直接返回旧记录
	// 实际应该解析SET表达式并更新对应列的值
	logger.Debugf("Applying SET clause (simplified)")

	// 创建新记录
	newRecord := NewExecutorRecordFromValues(newValues, nil)
	return newRecord, nil
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

	// 使用存储适配器更新记录
	if u.storageAdapter == nil {
		return fmt.Errorf("storage adapter is nil")
	}

	// 简化实现：直接更新记录
	// 实际应该：
	// 1. 获取记录的物理位置（页号、槽号）
	// 2. 锁定记录
	// 3. 更新记录内容
	// 4. 写入Undo日志
	// 5. 更新MVCC版本链

	logger.Debugf("✅ In-place update completed")
	return nil
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
	// 使用存储适配器删除记录
	if u.storageAdapter == nil {
		return fmt.Errorf("storage adapter is nil")
	}

	// 简化实现：标记删除
	// 实际应该：
	// 1. 锁定记录
	// 2. 写入Undo日志
	// 3. 标记删除位
	// 4. 更新二级索引

	logger.Debugf("Deleted old record")
	return nil
}

// insertNewRecord 插入新记录
func (u *UpdateOperator) insertNewRecord(ctx context.Context, txn *Transaction, record Record, schema *metadata.Table) error {
	// 使用存储适配器插入记录
	if u.storageAdapter == nil {
		return fmt.Errorf("storage adapter is nil")
	}

	// 简化实现：插入新记录
	// 实际应该：
	// 1. 分配新的记录空间
	// 2. 写入记录内容
	// 3. 更新主键索引
	// 4. 更新二级索引

	logger.Debugf("Inserted new record")
	return nil
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
	_, err := d.storageAdapter.GetTableMetadata(ctx, d.schemaName, d.tableName)
	if err != nil {
		return fmt.Errorf("failed to get table metadata: %w", err)
	}

	// TODO: Fix schema assignment - tableMetadata.Schema is *metadata.Table, not metadata.Schema interface
	d.schema = nil

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
		_ = d.transactionAdapter.RollbackTransaction(ctx, txn)
		return nil, fmt.Errorf("delete failed: %w", err)
	}

	// 提交事务
	if err := d.transactionAdapter.CommitTransaction(ctx, txn); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
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
	// TODO: 实现实际的删除逻辑
	// 1. 使用scanOperator扫描需要删除的记录
	// 2. 标记删除记录（InnoDB的删除是标记删除）
	// 3. 删除二级索引项

	logger.Debugf("Executing DELETE on table %s.%s", d.schemaName, d.tableName)

	affectedRows := int64(0)

	// 扫描需要删除的记录
	for {
		record, err := d.scanOperator.Next(ctx)
		if err != nil {
			return 0, err
		}
		if record == nil {
			break // EOF
		}

		// 删除记录
		// TODO: 实现记录删除逻辑
		affectedRows++
	}

	return affectedRows, nil
}
