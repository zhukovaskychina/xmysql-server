package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// SecondaryIndexSyncer 二级索引同步器接口
// 用于在DML操作时同步二级索引
type SecondaryIndexSyncer interface {
	SyncSecondaryIndexesOnInsert(tableID uint64, rowData map[string]interface{}, primaryKeyValue []byte) error
	SyncSecondaryIndexesOnUpdate(tableID uint64, oldRowData, newRowData map[string]interface{}, primaryKeyValue []byte) error
	SyncSecondaryIndexesOnDelete(tableID uint64, rowData map[string]interface{}) error
}

// DMLExecutor DML操作执行器
// 注意：此执行器是DML操作协调器，不是火山模型的Operator
// 实际的算子执行使用volcano_executor.go中的Operator接口
type DMLExecutor struct {
	// 管理器组件
	optimizerManager  *manager.OptimizerManager
	bufferPoolManager *manager.OptimizedBufferPoolManager
	btreeManager      basic.BPlusTreeManager
	tableManager      *manager.TableManager
	txManager         *manager.TransactionManager
	indexSyncer       SecondaryIndexSyncer // 二级索引同步器接口
	tableIDResolver   func(schemaName, tableName string) (uint64, error)

	// 执行状态
	schemaName    string
	tableName     string
	isInitialized bool
}

// NewDMLExecutor 创建DML执行器
func NewDMLExecutor(
	optimizerManager *manager.OptimizerManager,
	bufferPoolManager *manager.OptimizedBufferPoolManager,
	btreeManager basic.BPlusTreeManager,
	tableManager *manager.TableManager,
	txManager *manager.TransactionManager,
	indexSyncer SecondaryIndexSyncer,
) *DMLExecutor {
	return &DMLExecutor{
		optimizerManager:  optimizerManager,
		bufferPoolManager: bufferPoolManager,
		btreeManager:      btreeManager,
		tableManager:      tableManager,
		txManager:         txManager,
		indexSyncer:       indexSyncer,
		isInitialized:     false,
	}
}

// ExecuteInsert 执行INSERT语句
func (dml *DMLExecutor) ExecuteInsert(ctx context.Context, stmt *sqlparser.Insert, schemaName string) (*DMLResult, error) {
	logger.Infof(" 开始执行INSERT语句: %s", sqlparser.String(stmt))

	resolvedSchema := strings.TrimSpace(schemaName)
	if qualifier := strings.TrimSpace(stmt.Table.Qualifier.String()); qualifier != "" {
		resolvedSchema = qualifier
	}

	dml.schemaName = resolvedSchema
	dml.tableName = stmt.Table.Name.String()

	// 1. 验证表存在
	tableMeta, err := dml.getTableMetadata()
	if err != nil {
		return nil, fmt.Errorf("表不存在或无法访问: %v", err)
	}

	// 2. 解析INSERT的列和值
	insertRows, err := dml.parseInsertData(stmt, tableMeta)
	if err != nil {
		return nil, fmt.Errorf("解析INSERT数据失败: %v", err)
	}

	// 3. 验证数据完整性
	if err := dml.validateInsertData(insertRows, tableMeta); err != nil {
		return nil, fmt.Errorf("数据验证失败: %v", err)
	}

	// 4. 开始事务
	txn, err := dml.beginTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("开始事务失败: %v", err)
	}
	txnID := extractTxnIDFromAny(txn)

	affectedRows := 0
	var lastInsertId uint64 = 0

	// 5. 逐行插入数据
	for _, row := range insertRows {
		insertId, err := dml.insertRow(ctx, txn, row, tableMeta)
		if err != nil {
			// 回滚事务
			dml.rollbackTransaction(ctx, txn)
			return nil, fmt.Errorf("插入行失败: %v", err)
		}
		affectedRows++
		if insertId > 0 {
			lastInsertId = insertId
		}
	}

	// 6. 提交事务
	if err := dml.commitTransaction(ctx, txn); err != nil {
		return nil, fmt.Errorf("提交事务失败: %v", err)
	}

	logger.Infof(" INSERT执行成功，影响行数: %d, LastInsertID: %d", affectedRows, lastInsertId)

	return &DMLResult{
		AffectedRows: affectedRows,
		LastInsertId: lastInsertId,
		ResultType:   "INSERT",
		Message:      fmt.Sprintf("INSERT执行成功，影响行数: %d", affectedRows),
		TxnID:        txnID,
	}, nil
}

// ExecuteUpdate 执行UPDATE语句
func (dml *DMLExecutor) ExecuteUpdate(ctx context.Context, stmt *sqlparser.Update, schemaName string) (*DMLResult, error) {
	logger.Infof(" 开始执行UPDATE语句: %s", sqlparser.String(stmt))

	dml.schemaName = schemaName

	// 1. 解析表名（简化处理，假设只更新一个表）
	if len(stmt.TableExprs) == 0 {
		return nil, fmt.Errorf("UPDATE语句缺少表名")
	}
	resolvedSchema := strings.TrimSpace(schemaName)

	tableName, err := dml.parseTableName(stmt.TableExprs[0])
	if err != nil {
		return nil, fmt.Errorf("解析表名失败: %v", err)
	}
	if tableSchema, err := dml.parseTableSchema(stmt.TableExprs[0]); err == nil && tableSchema != "" {
		resolvedSchema = tableSchema
	}
	dml.tableName = tableName
	dml.schemaName = resolvedSchema

	// 2. 验证表存在
	tableMeta, err := dml.getTableMetadata()
	if err != nil {
		return nil, fmt.Errorf("表不存在或无法访问: %v", err)
	}

	// 3. 解析WHERE条件
	whereConditions := dml.parseWhereConditions(stmt.Where)

	// 4. 解析SET表达式
	updateExprs, err := dml.parseUpdateExpressions(stmt.Exprs, tableMeta)
	if err != nil {
		return nil, fmt.Errorf("解析UPDATE表达式失败: %v", err)
	}

	// 5. 开始事务
	txn, err := dml.beginTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("开始事务失败: %v", err)
	}
	txnID := extractTxnIDFromAny(txn)

	// 6. 查找需要更新的行
	rowsToUpdate, err := dml.findRowsToUpdate(ctx, txn, whereConditions, tableMeta)
	if err != nil {
		dml.rollbackTransaction(ctx, txn)
		return nil, fmt.Errorf("查找待更新行失败: %v", err)
	}

	affectedRows := 0

	// 7. 逐行更新数据
	for _, rowInfo := range rowsToUpdate {
		err := dml.updateRow(ctx, txn, rowInfo, updateExprs, tableMeta)
		if err != nil {
			dml.rollbackTransaction(ctx, txn)
			return nil, fmt.Errorf("更新行失败: %v", err)
		}
		affectedRows++
	}

	// 8. 提交事务
	if err := dml.commitTransaction(ctx, txn); err != nil {
		return nil, fmt.Errorf("提交事务失败: %v", err)
	}

	logger.Infof(" UPDATE执行成功，影响行数: %d", affectedRows)

	return &DMLResult{
		AffectedRows: affectedRows,
		LastInsertId: 0,
		ResultType:   "UPDATE",
		Message:      fmt.Sprintf("UPDATE执行成功，影响行数: %d", affectedRows),
		TxnID:        txnID,
	}, nil
}

// ExecuteDelete 执行DELETE语句
func (dml *DMLExecutor) ExecuteDelete(ctx context.Context, stmt *sqlparser.Delete, schemaName string) (*DMLResult, error) {
	logger.Infof(" 开始执行DELETE语句: %s", sqlparser.String(stmt))

	dml.schemaName = schemaName

	// 1. 解析表名
	if len(stmt.TableExprs) == 0 {
		return nil, fmt.Errorf("DELETE语句缺少表名")
	}
	resolvedSchema := strings.TrimSpace(schemaName)

	tableName, err := dml.parseTableName(stmt.TableExprs[0])
	if err != nil {
		return nil, fmt.Errorf("解析表名失败: %v", err)
	}
	if tableSchema, err := dml.parseTableSchema(stmt.TableExprs[0]); err == nil && tableSchema != "" {
		resolvedSchema = tableSchema
	}
	dml.tableName = tableName
	dml.schemaName = resolvedSchema

	// 2. 验证表存在
	tableMeta, err := dml.getTableMetadata()
	if err != nil {
		return nil, fmt.Errorf("表不存在或无法访问: %v", err)
	}

	// 3. 解析WHERE条件
	whereConditions := dml.parseWhereConditions(stmt.Where)

	// 4. 开始事务
	txn, err := dml.beginTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("开始事务失败: %v", err)
	}
	txnID := extractTxnIDFromAny(txn)

	// 5. 查找需要删除的行
	rowsToDelete, err := dml.findRowsToDelete(ctx, txn, whereConditions, tableMeta)
	if err != nil {
		dml.rollbackTransaction(ctx, txn)
		return nil, fmt.Errorf("查找待删除行失败: %v", err)
	}

	affectedRows := 0

	// 6. 逐行删除数据
	for _, rowInfo := range rowsToDelete {
		err := dml.deleteRow(ctx, txn, rowInfo, tableMeta)
		if err != nil {
			dml.rollbackTransaction(ctx, txn)
			return nil, fmt.Errorf("删除行失败: %v", err)
		}
		affectedRows++
	}

	// 7. 提交事务
	if err := dml.commitTransaction(ctx, txn); err != nil {
		return nil, fmt.Errorf("提交事务失败: %v", err)
	}

	logger.Infof(" DELETE执行成功，影响行数: %d", affectedRows)

	return &DMLResult{
		AffectedRows: affectedRows,
		LastInsertId: 0,
		ResultType:   "DELETE",
		Message:      fmt.Sprintf("DELETE执行成功，影响行数: %d", affectedRows),
		TxnID:        txnID,
	}, nil
}

// DMLResult DML操作结果
type DMLResult struct {
	AffectedRows int
	LastInsertId uint64
	ResultType   string
	Message      string
	TxnID        uint64
}

func extractTxnIDFromAny(txn interface{}) uint64 {
	if txn == nil {
		return 0
	}

	switch t := txn.(type) {
	case *manager.Transaction:
		return uint64(t.ID)
	default:
		return 0
	}
}

// InsertRowData 插入行数据结构
type InsertRowData struct {
	ColumnValues map[string]interface{}
	ColumnTypes  map[string]metadata.DataType
}

// RowUpdateInfo 行更新信息
type RowUpdateInfo struct {
	RowId     uint64
	PageNum   uint32
	SlotIndex int
	OldValues map[string]interface{}
}

// UpdateExpression 更新表达式
type UpdateExpression struct {
	ColumnName string
	NewValue   interface{}
	ColumnType metadata.DataType
	Expr       sqlparser.Expr
}

// getTableMetadata 获取表元数据
func (dml *DMLExecutor) getTableMetadata() (*metadata.TableMeta, error) {
	if dml.tableManager == nil {
		return nil, fmt.Errorf("表管理器未初始化")
	}

	tableMeta, err := dml.tableManager.GetTableMetadata(context.Background(), dml.schemaName, dml.tableName)
	if err != nil {
		return nil, fmt.Errorf("获取表元数据失败: %v", err)
	}

	return tableMeta, nil
}

// parseInsertData 解析INSERT数据
func (dml *DMLExecutor) parseInsertData(stmt *sqlparser.Insert, tableMeta *metadata.TableMeta) ([]*InsertRowData, error) {
	var insertRows []*InsertRowData

	// 解析列名列表
	var columnNames []string
	if len(stmt.Columns) > 0 {
		for _, col := range stmt.Columns {
			columnNames = append(columnNames, col.String())
		}
	} else {
		// 如果没有指定列名，使用表的所有列
		for _, col := range tableMeta.Columns {
			columnNames = append(columnNames, col.Name)
		}
	}

	// 解析VALUES子句
	switch valuesClause := stmt.Rows.(type) {
	case sqlparser.Values:
		for _, valTuple := range valuesClause {
			rowData := &InsertRowData{
				ColumnValues: make(map[string]interface{}),
				ColumnTypes:  make(map[string]metadata.DataType),
			}

			if len(valTuple) != len(columnNames) {
				return nil, fmt.Errorf("列数量不匹配: 期望 %d，实际 %d", len(columnNames), len(valTuple))
			}

			for i, expr := range valTuple {
				columnName := columnNames[i]
				value, err := dml.evaluateExpression(expr)
				if err != nil {
					return nil, fmt.Errorf("计算表达式值失败: %v", err)
				}

				rowData.ColumnValues[columnName] = value
				// 根据列定义设置列类型
				if colMeta := dml.getColumnMetadataByName(tableMeta, columnName); colMeta != nil {
					rowData.ColumnTypes[columnName] = colMeta.Type
				} else {
					rowData.ColumnTypes[columnName] = metadata.TypeVarchar
				}
			}

			insertRows = append(insertRows, rowData)
		}
	default:
		return nil, fmt.Errorf("不支持的INSERT语法: %T", stmt.Rows)
	}

	return insertRows, nil
}

// evaluateExpression 计算表达式值
func (dml *DMLExecutor) evaluateExpression(expr sqlparser.Expr) (interface{}, error) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		return dml.parseSQLVal(v)
	case *sqlparser.NullVal:
		return nil, nil
	case sqlparser.BoolVal:
		return bool(v), nil
	default:
		return nil, fmt.Errorf("不支持的表达式类型: %T", expr)
	}
}

// parseSQLVal 解析SQL值
func (dml *DMLExecutor) parseSQLVal(val *sqlparser.SQLVal) (interface{}, error) {
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

// validateInsertData 验证插入数据
func (dml *DMLExecutor) validateInsertData(rows []*InsertRowData, tableMeta *metadata.TableMeta) error {
	logger.Debugf(" 验证插入数据，行数: %d", len(rows))
	if tableMeta == nil {
		return fmt.Errorf("表元数据为空")
	}

	columnByName := map[string]*metadata.ColumnMeta{}
	for _, col := range tableMeta.Columns {
		if col != nil && col.Name != "" {
			columnByName[col.Name] = col
		}
	}

	for _, row := range rows {
		if row == nil {
			return fmt.Errorf("存在空行数据")
		}

		for _, colMeta := range columnByName {
			_, hasValue := row.ColumnValues[colMeta.Name]
			if !hasValue {
				if colMeta.IsAutoIncrement {
					continue
				}
				if !colMeta.IsNullable && colMeta.DefaultValue == nil {
					return fmt.Errorf("列 %s 不允许为 NULL", colMeta.Name)
				}
				if colMeta.DefaultValue != nil {
					row.ColumnValues[colMeta.Name] = colMeta.DefaultValue
					row.ColumnTypes[colMeta.Name] = colMeta.Type
				}
				continue
			}
			if err := dml.validateColumnValue(colMeta, row.ColumnValues[colMeta.Name]); err != nil {
				return err
			}
		}

		for colName, value := range row.ColumnValues {
			if colMeta, exists := columnByName[colName]; exists {
				if err := dml.validateColumnValue(colMeta, value); err != nil {
					return fmt.Errorf("列 %s 校验失败: %v", colName, err)
				}
				continue
			}
			// 对于元数据未知列，保守回退到原逻辑
			if value != nil {
				continue
			}
			return fmt.Errorf("未知列: %s", colName)
		}
	}

	return nil
}

// parseTableName 解析表名
func (dml *DMLExecutor) parseTableName(tableExpr sqlparser.TableExpr) (string, error) {
	switch v := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch tableExpr := v.Expr.(type) {
		case sqlparser.TableName:
			return tableExpr.Name.String(), nil
		default:
			return "", fmt.Errorf("不支持的表表达式类型: %T", tableExpr)
		}
	default:
		return "", fmt.Errorf("不支持的FROM表达式类型: %T", v)
	}
}

// parseTableSchema 解析表schema（限定符）
func (dml *DMLExecutor) parseTableSchema(tableExpr sqlparser.TableExpr) (string, error) {
	switch v := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch tableExpr := v.Expr.(type) {
		case sqlparser.TableName:
			return tableExpr.Qualifier.String(), nil
		default:
			return "", fmt.Errorf("不支持的表表达式类型: %T", tableExpr)
		}
	default:
		return "", fmt.Errorf("不支持的FROM表达式类型: %T", v)
	}
}

// parseWhereConditions 解析WHERE条件
func (dml *DMLExecutor) parseWhereConditions(where *sqlparser.Where) []string {
	if where == nil {
		return []string{}
	}

	// 简化实现，将WHERE条件转换为字符串
	conditions := []string{sqlparser.String(where.Expr)}
	return conditions
}

// parseUpdateExpressions 解析UPDATE表达式
func (dml *DMLExecutor) parseUpdateExpressions(exprs sqlparser.UpdateExprs, tableMeta *metadata.TableMeta) ([]*UpdateExpression, error) {
	var updateExprs []*UpdateExpression

	for _, expr := range exprs {
		columnName := expr.Name.Name.String()
		value, err := dml.evaluateExpression(expr.Expr)
		if err != nil {
			return nil, fmt.Errorf("计算更新表达式值失败: %v", err)
		}
		colMeta := dml.getColumnMetadataByName(tableMeta, columnName)
		if colMeta == nil {
			return nil, fmt.Errorf("列 %s 不存在", columnName)
		}

		updateExpr := &UpdateExpression{
			ColumnName: columnName,
			NewValue:   value,
			ColumnType: colMeta.Type,
		}

		updateExprs = append(updateExprs, updateExpr)
	}

	return updateExprs, nil
}

// 事务相关方法 - 简化实现
func (dml *DMLExecutor) beginTransaction(ctx context.Context) (interface{}, error) {
	logger.Debugf("🔄 开始事务")

	if dml.txManager == nil {
		return nil, fmt.Errorf("transaction manager not initialized")
	}

	tx, err := dml.txManager.Begin(false, manager.TRX_ISO_REPEATABLE_READ)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func (dml *DMLExecutor) commitTransaction(ctx context.Context, txn interface{}) error {
	logger.Debugf(" 提交事务")

	if txn == nil {
		return fmt.Errorf("nil transaction")
	}

	t, ok := txn.(*manager.Transaction)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}

	if dml.txManager == nil {
		return fmt.Errorf("transaction manager not initialized")
	}

	return dml.txManager.Commit(t)
}

func (dml *DMLExecutor) rollbackTransaction(ctx context.Context, txn interface{}) error {
	logger.Debugf("🔄 回滚事务")

	if txn == nil {
		return fmt.Errorf("nil transaction")
	}

	t, ok := txn.(*manager.Transaction)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}

	if dml.txManager == nil {
		return fmt.Errorf("transaction manager not initialized")
	}

	return dml.txManager.Rollback(t)
}

// 数据操作方法 - 简化实现
func (dml *DMLExecutor) insertRow(ctx context.Context, txn interface{}, row *InsertRowData, tableMeta *metadata.TableMeta) (uint64, error) {
	logger.Debugf(" 插入行数据: %+v", row.ColumnValues)

	if tableMeta == nil {
		return 0, fmt.Errorf("table metadata is nil")
	}

	// 确定主键列
	pkCol := "id"
	if len(tableMeta.PrimaryKey) > 0 {
		pkCol = tableMeta.PrimaryKey[0]
	}

	pkVal, ok := row.ColumnValues[pkCol]
	if !ok {
		pkVal = time.Now().UnixNano()
		row.ColumnValues[pkCol] = pkVal
	}

	// 类型验证
	for _, col := range tableMeta.Columns {
		if val, exists := row.ColumnValues[col.Name]; exists {
			if !dml.validateValueType(val, col.Type) {
				return 0, fmt.Errorf("column %s type mismatch", col.Name)
			}
		}
	}

	// 序列化行数据（使用简单JSON表示）
	bytes, err := json.Marshal(row.ColumnValues)
	if err != nil {
		return 0, fmt.Errorf("serialize row failed: %v", err)
	}

	// 写入B+树主键索引
	if dml.btreeManager != nil {
		if err := dml.btreeManager.Insert(ctx, pkVal, bytes); err != nil {
			return 0, err
		}
	}

	// 同步二级索引
	if dml.indexSyncer != nil {
		tableID, err := dml.getTableIDFromName(tableMeta.Name)
		if err != nil {
			logger.Errorf("❌ 解析二级索引表ID失败: %v", err)
			return 0, fmt.Errorf("二级索引同步不可执行: %v", err)
		}

		if err := dml.indexSyncer.SyncSecondaryIndexesOnInsert(
			tableID,
			row.ColumnValues,
			bytes, // 主键值（序列化后的行数据）
		); err != nil {
			// 二级索引同步失败，需要回滚主键插入
			logger.Errorf("❌ 二级索引同步失败，回滚主键插入: %v", err)

			// 尝试删除已插入的主键索引
			if dml.btreeManager != nil {
				if deleter, ok := interface{}(dml.btreeManager).(interface {
					Delete(ctx context.Context, key interface{}) error
				}); ok {
					if delErr := deleter.Delete(ctx, pkVal); delErr != nil {
						logger.Errorf("❌ 回滚主键插入失败: %v", delErr)
					}
				}
			}

			return 0, fmt.Errorf("同步二级索引失败: %v", err)
		}
		logger.Debugf("✅ 二级索引同步成功")
	}

	return dml.convertPrimaryKeyToUint64(pkVal), nil
}

func (dml *DMLExecutor) findRowsToUpdate(ctx context.Context, txn interface{}, whereConditions []string, tableMeta *metadata.TableMeta) ([]*RowUpdateInfo, error) {
	logger.Debugf(" 查找待更新行，条件: %v", whereConditions)

	var rows []*RowUpdateInfo

	for _, cond := range whereConditions {
		key := dml.extractPrimaryKeyFromCondition(cond)
		if key == nil {
			continue
		}

		if dml.btreeManager != nil {
			pageNo, slot, err := dml.btreeManager.Search(ctx, key)
			if err != nil {
				logger.Debugf(" search failed: %v", err)
				continue
			}

			rows = append(rows, &RowUpdateInfo{
				RowId:     dml.convertPrimaryKeyToUint64(key),
				PageNum:   pageNo,
				SlotIndex: slot,
				OldValues: map[string]interface{}{},
			})
		}
	}

	return rows, nil
}

func (dml *DMLExecutor) updateRow(ctx context.Context, txn interface{}, rowInfo *RowUpdateInfo, updateExprs []*UpdateExpression, tableMeta *metadata.TableMeta) error {
	logger.Debugf(" 更新行数据: RowID=%d", rowInfo.RowId)

	// 构建新的行数据
	newData := make(map[string]interface{})
	for k, v := range rowInfo.OldValues {
		newData[k] = v
	}

	for _, expr := range updateExprs {
		colMeta := dml.getColumnMetadataByName(tableMeta, expr.ColumnName)
		if colMeta == nil {
			return fmt.Errorf("列 %s 不存在", expr.ColumnName)
		}

		if err := dml.validateColumnValue(colMeta, expr.NewValue); err != nil {
			return err
		}
		newData[expr.ColumnName] = expr.NewValue
	}

	bytes, err := json.Marshal(newData)
	if err != nil {
		return fmt.Errorf("serialize row failed: %v", err)
	}

	// 更新主键索引
	if dml.btreeManager != nil {
		if err := dml.btreeManager.Insert(ctx, rowInfo.RowId, bytes); err != nil {
			return err
		}
	}

	// 同步二级索引
	if dml.indexSyncer != nil {
		tableID, err := dml.getTableIDFromName(tableMeta.Name)
		if err != nil {
			return fmt.Errorf("二级索引同步不可执行: %v", err)
		}

		if err := dml.indexSyncer.SyncSecondaryIndexesOnUpdate(
			tableID,
			rowInfo.OldValues, // 旧数据
			newData,           // 新数据
			bytes,             // 主键值（序列化后的行数据）
		); err != nil {
			logger.Errorf("❌ 二级索引更新失败: %v", err)
			return fmt.Errorf("同步二级索引失败: %v", err)
		}
		logger.Debugf("✅ 二级索引更新成功")
	}

	return nil
}

func (dml *DMLExecutor) findRowsToDelete(ctx context.Context, txn interface{}, whereConditions []string, tableMeta *metadata.TableMeta) ([]*RowUpdateInfo, error) {
	logger.Debugf(" 查找待删除行，条件: %v", whereConditions)

	var rows []*RowUpdateInfo

	for _, cond := range whereConditions {
		key := dml.extractPrimaryKeyFromCondition(cond)
		if key == nil {
			continue
		}

		if dml.btreeManager != nil {
			pageNo, slot, err := dml.btreeManager.Search(ctx, key)
			if err != nil {
				logger.Debugf(" search failed: %v", err)
				continue
			}

			rows = append(rows, &RowUpdateInfo{
				RowId:     dml.convertPrimaryKeyToUint64(key),
				PageNum:   pageNo,
				SlotIndex: slot,
				OldValues: map[string]interface{}{},
			})
		}
	}

	return rows, nil
}

func (dml *DMLExecutor) deleteRow(ctx context.Context, txn interface{}, rowInfo *RowUpdateInfo, tableMeta *metadata.TableMeta) error {
	logger.Debugf("🗑️ 删除行数据: RowID=%d", rowInfo.RowId)

	if dml.btreeManager == nil {
		return fmt.Errorf("btree manager not initialized")
	}

	// 先同步删除二级索引（在删除主键之前）
	if dml.indexSyncer != nil {
		tableID, err := dml.getTableIDFromName(tableMeta.Name)
		if err != nil {
			return fmt.Errorf("二级索引同步不可执行: %v", err)
		}

		if err := dml.indexSyncer.SyncSecondaryIndexesOnDelete(
			tableID,
			rowInfo.OldValues, // 行数据
		); err != nil {
			logger.Errorf("❌ 二级索引删除失败: %v", err)
			return fmt.Errorf("同步二级索引删除失败: %v", err)
		}
		logger.Debugf("✅ 二级索引删除成功")
	}

	// 删除主键索引
	if deleter, ok := interface{}(dml.btreeManager).(interface {
		Delete(ctx context.Context, key interface{}) error
	}); ok {
		return deleter.Delete(ctx, rowInfo.RowId)
	}

	// 如果不支持删除，尝试插入空值标记覆盖
	empty := []byte("DELETED")
	return dml.btreeManager.Insert(ctx, rowInfo.RowId, empty)
}

// ===== 辅助方法 =====

// getTableIDFromName 从表名获取 TableID
func (dml *DMLExecutor) getTableIDFromName(tableName string) (uint64, error) {
	if dml.tableIDResolver != nil {
		return dml.tableIDResolver(dml.schemaName, tableName)
	}

	if dml.tableManager == nil {
		return 0, fmt.Errorf("表管理器未初始化")
	}

	if dml.schemaName == "" || strings.TrimSpace(tableName) == "" {
		return 0, fmt.Errorf("表名或schema为空")
	}

	info, err := dml.tableManager.GetTableStorageInfo(dml.schemaName, tableName)
	if err != nil {
		return 0, fmt.Errorf("获取表存储信息失败: %v", err)
	}

	logger.Debugf("从TableStorageInfo获取TableID: %s.%s => %d", dml.schemaName, tableName, info.SpaceID)
	return uint64(info.SpaceID), nil
}

func (dml *DMLExecutor) getColumnMetadataByName(tableMeta *metadata.TableMeta, columnName string) *metadata.ColumnMeta {
	if tableMeta == nil {
		return nil
	}

	for _, col := range tableMeta.Columns {
		if col != nil && strings.EqualFold(strings.TrimSpace(col.Name), strings.TrimSpace(columnName)) {
			return col
		}
	}
	return nil
}

func (dml *DMLExecutor) validateColumnValue(colMeta *metadata.ColumnMeta, value interface{}) error {
	if colMeta == nil {
		return fmt.Errorf("列定义为空")
	}

	if value == nil {
		if !colMeta.IsNullable && !colMeta.IsAutoIncrement {
			return fmt.Errorf("列 %s 不允许为 NULL", colMeta.Name)
		}
		return nil
	}

	if !dml.validateValueType(value, colMeta.Type) {
		return fmt.Errorf("列 %s 类型不匹配，期望 %s", colMeta.Name, colMeta.Type)
	}

	if err := dml.validateValueLength(colMeta, value); err != nil {
		return fmt.Errorf("列 %s %v", colMeta.Name, err)
	}

	return nil
}

func (dml *DMLExecutor) validateValueLength(colMeta *metadata.ColumnMeta, value interface{}) error {
	if colMeta == nil {
		return fmt.Errorf("列定义为空")
	}

	if colMeta.Length <= 0 {
		return nil
	}

	maxLen := colMeta.Length
	switch v := value.(type) {
	case string:
		if len(v) > maxLen {
			return fmt.Errorf("长度超限，最大允许 %d", maxLen)
		}
	case []byte:
		if len(v) > maxLen {
			return fmt.Errorf("长度超限，最大允许 %d", maxLen)
		}
	}
	return nil
}

func (dml *DMLExecutor) convertPrimaryKeyToUint64(key interface{}) uint64 {
	switch v := key.(type) {
	case int64:
		return uint64(v)
	case uint64:
		return v
	case int:
		return uint64(v)
	case string:
		if id, err := strconv.ParseUint(v, 10, 64); err == nil {
			return id
		}
		return 0
	default:
		return 0
	}
}

func (dml *DMLExecutor) extractPrimaryKeyFromCondition(condition string) interface{} {
	condition = strings.TrimSpace(condition)
	if condition == "" {
		return nil
	}

	stmt, err := sqlparser.Parse("SELECT 1 FROM dual WHERE " + condition)
	if err != nil {
		logger.Debugf(" 解析WHERE条件失败: %v, condition=%q", err, condition)
		return nil
	}

	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok || selectStmt.Where == nil || selectStmt.Where.Expr == nil {
		return nil
	}

	return dml.extractPrimaryKeyFromExpr(selectStmt.Where.Expr)
}

func (dml *DMLExecutor) extractPrimaryKeyFromExpr(expr sqlparser.Expr) interface{} {
	switch v := expr.(type) {
	case *sqlparser.AndExpr:
		if key := dml.extractPrimaryKeyFromExpr(v.Left); key != nil {
			return key
		}
		return dml.extractPrimaryKeyFromExpr(v.Right)
	case *sqlparser.OrExpr:
		if key := dml.extractPrimaryKeyFromExpr(v.Left); key != nil {
			return key
		}
		return dml.extractPrimaryKeyFromExpr(v.Right)
	case *sqlparser.ComparisonExpr:
		if v.Operator != sqlparser.EqualStr {
			return nil
		}
		colName, ok := v.Left.(*sqlparser.ColName)
		if !ok || !dml.isPrimaryKeyColumnName(colName.Name.String()) {
			return nil
		}
		return dml.parsePrimaryKeyValue(v.Right)
	case *sqlparser.ParenExpr:
		return dml.extractPrimaryKeyFromExpr(v.Expr)
	default:
		return nil
	}
}

func (dml *DMLExecutor) parsePrimaryKeyValue(expr sqlparser.Expr) interface{} {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.IntVal:
			if val, err := strconv.ParseInt(string(v.Val), 10, 64); err == nil {
				return val
			}
		case sqlparser.StrVal:
			return string(v.Val)
		case sqlparser.HexVal:
			return v.Val
		case sqlparser.FloatVal:
			if val, err := strconv.ParseFloat(string(v.Val), 64); err == nil {
				return val
			}
		default:
			return string(v.Val)
		}
		return nil
	case *sqlparser.ParenExpr:
		return dml.parsePrimaryKeyValue(v.Expr)
	case *sqlparser.UnaryExpr:
		if v.Operator == sqlparser.MinusStr && v.Expr != nil {
			switch val := dml.parsePrimaryKeyValue(v.Expr).(type) {
			case int64:
				return -val
			case float64:
				return -val
			default:
				return nil
			}
		}
		if v.Operator == sqlparser.PlusStr {
			return dml.parsePrimaryKeyValue(v.Expr)
		}
		if v.Expr != nil {
			return dml.parsePrimaryKeyValue(v.Expr)
		}
		return nil
	default:
		return nil
	}
}

func (dml *DMLExecutor) isPrimaryKeyColumnName(raw string) bool {
	colName := strings.Trim(raw, "` ")
	if colName == "" {
		return false
	}
	if idx := strings.LastIndex(colName, "."); idx >= 0 && idx < len(colName)-1 {
		colName = colName[idx+1:]
	}
	lowerName := strings.ToLower(strings.TrimSpace(colName))
	return lowerName == "id" || strings.HasSuffix(lowerName, "_id")
}

func (dml *DMLExecutor) validateValueType(val interface{}, colType metadata.DataType) bool {
	switch colType {
	case metadata.TypeInt, metadata.TypeBigInt, metadata.TypeMediumInt, metadata.TypeSmallInt, metadata.TypeTinyInt:
		switch val.(type) {
		case int, int32, int64, uint, uint32, uint64:
			return true
		case string:
			_, err := strconv.ParseInt(strings.TrimSpace(val.(string)), 10, 64)
			return err == nil
		case []byte:
			_, err := strconv.ParseInt(strings.TrimSpace(string(val.([]byte))), 10, 64)
			return err == nil
		default:
			return false
		}
	case metadata.TypeFloat, metadata.TypeDouble, metadata.TypeDecimal:
		switch val.(type) {
		case int, int32, int64, uint, uint32, uint64, float64, float32:
			return true
		case string:
			_, err := strconv.ParseFloat(strings.TrimSpace(val.(string)), 64)
			return err == nil
		case []byte:
			_, err := strconv.ParseFloat(strings.TrimSpace(string(val.([]byte))), 64)
			return err == nil
		default:
			return false
		}
	case metadata.TypeBool, metadata.TypeBoolean:
		_, ok := val.(bool)
		return ok
	case metadata.TypeChar, metadata.TypeVarchar, metadata.TypeText, metadata.TypeLongText, metadata.TypeMediumText, metadata.TypeTinyText:
		_, ok := val.(string)
		if ok {
			return true
		}
		_, ok = val.([]byte)
		return ok
	case metadata.TypeBinary, metadata.TypeVarBinary, metadata.TypeBlob, metadata.TypeTinyBlob, metadata.TypeMediumBlob, metadata.TypeLongBlob:
		_, ok := val.(string)
		if ok {
			return true
		}
		_, ok = val.([]byte)
		return ok
	case metadata.TypeDate, metadata.TypeTime, metadata.TypeDateTime, metadata.TypeTimestamp, metadata.TypeYear:
		_, ok := val.(string)
		return ok
	case metadata.TypeJSON:
		switch val.(type) {
		case string, []byte:
			return true
		default:
			return false
		}
	default:
		return true
	}
}
