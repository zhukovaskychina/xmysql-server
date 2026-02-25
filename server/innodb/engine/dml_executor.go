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

	dml.schemaName = schemaName
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

	tableName, err := dml.parseTableName(stmt.TableExprs[0])
	if err != nil {
		return nil, fmt.Errorf("解析表名失败: %v", err)
	}
	dml.tableName = tableName

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

	tableName, err := dml.parseTableName(stmt.TableExprs[0])
	if err != nil {
		return nil, fmt.Errorf("解析表名失败: %v", err)
	}
	dml.tableName = tableName

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
	}, nil
}

// DMLResult DML操作结果
type DMLResult struct {
	AffectedRows int
	LastInsertId uint64
	ResultType   string
	Message      string
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
}

// getTableMetadata 获取表元数据
func (dml *DMLExecutor) getTableMetadata() (*metadata.TableMeta, error) {
	if dml.tableManager == nil {
		return nil, fmt.Errorf("表管理器未初始化")
	}

	// 这里需要实现根据表名获取表元数据的逻辑
	// 暂时返回一个默认的表元数据结构
	tableMeta := &metadata.TableMeta{
		Name:       dml.tableName,
		Columns:    []*metadata.ColumnMeta{},
		PrimaryKey: []string{},             // 使用正确的字段名
		Indices:    []metadata.IndexMeta{}, // 使用正确的字段名
	}

	// TODO: 从实际的数据字典中获取表元数据
	logger.Debugf(" 获取表元数据: %s.%s", dml.schemaName, dml.tableName)

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
				// TODO: 根据表元数据设置正确的列类型
				rowData.ColumnTypes[columnName] = metadata.TypeVarchar
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
	// TODO: 实现数据类型验证、约束检查等
	logger.Debugf(" 验证插入数据，行数: %d", len(rows))
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

		updateExpr := &UpdateExpression{
			ColumnName: columnName,
			NewValue:   value,
			ColumnType: metadata.TypeVarchar, // TODO: 根据表元数据设置正确的类型
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
	// 注意：由于 TableMeta 没有 TableID 字段，我们需要通过其他方式获取
	// 这里暂时使用表名的哈希值作为 TableID（临时方案）
	// TODO: 需要在 TableMeta 中添加 TableID 字段，或从 TableManager 获取
	if dml.indexSyncer != nil {
		tableID := dml.getTableIDFromName(tableMeta.Name)
		if tableID > 0 {
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
		if !dml.validateValueType(expr.NewValue, expr.ColumnType) {
			return fmt.Errorf("column %s type mismatch", expr.ColumnName)
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
		tableID := dml.getTableIDFromName(tableMeta.Name)
		if tableID > 0 {
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
		tableID := dml.getTableIDFromName(tableMeta.Name)
		if tableID > 0 {
			if err := dml.indexSyncer.SyncSecondaryIndexesOnDelete(
				tableID,
				rowInfo.OldValues, // 行数据
			); err != nil {
				logger.Errorf("❌ 二级索引删除失败: %v", err)
				return fmt.Errorf("同步二级索引删除失败: %v", err)
			}
			logger.Debugf("✅ 二级索引删除成功")
		}
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
// TODO: 这是一个临时实现，应该从 TableManager 或数据字典中获取真实的 TableID
func (dml *DMLExecutor) getTableIDFromName(tableName string) uint64 {
	if tableName == "" {
		return 0
	}

	// 临时方案：使用表名的哈希值作为 TableID
	// 在生产环境中，应该从数据字典或 TableManager 中获取真实的 TableID
	var hash uint64
	for i := 0; i < len(tableName); i++ {
		hash = hash*31 + uint64(tableName[i])
	}

	// 确保返回一个非零的正数
	if hash == 0 {
		hash = 1
	}

	logger.Debugf("📋 表名 '%s' 映射到 TableID: %d (临时哈希值)", tableName, hash)
	return hash
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
	if strings.Contains(condition, "=") {
		parts := strings.Split(condition, "=")
		if len(parts) == 2 {
			left := strings.TrimSpace(parts[0])
			right := strings.TrimSpace(parts[1])
			if strings.Contains(strings.ToLower(left), "id") {
				if id, err := strconv.ParseInt(right, 10, 64); err == nil {
					return id
				}
				if strings.HasPrefix(right, "'") && strings.HasSuffix(right, "'") {
					return right[1 : len(right)-1]
				}
				return right
			}
		}
	}
	return nil
}

func (dml *DMLExecutor) validateValueType(val interface{}, colType metadata.DataType) bool {
	switch colType {
	case metadata.TypeInt, metadata.TypeBigInt, metadata.TypeMediumInt, metadata.TypeSmallInt, metadata.TypeTinyInt:
		switch val.(type) {
		case int, int32, int64, uint, uint32, uint64:
			return true
		default:
			return false
		}
	case metadata.TypeBool, metadata.TypeBoolean:
		_, ok := val.(bool)
		return ok
	case metadata.TypeChar, metadata.TypeVarchar, metadata.TypeText, metadata.TypeLongText, metadata.TypeMediumText, metadata.TypeTinyText:
		_, ok := val.(string)
		return ok
	default:
		return true
	}
}
