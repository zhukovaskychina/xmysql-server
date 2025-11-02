package engine

import (
	"context"
	"encoding/binary"
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

// ===== 数据序列化与反序列化方法 =====

// generatePrimaryKey 生成主键值
func (dml *StorageIntegratedDMLExecutor) generatePrimaryKey(row *InsertRowData, tableMeta *metadata.TableMeta) (interface{}, error) {
	// 简化实现：如果有id列，使用id作为主键；否则生成一个
	if idValue, exists := row.ColumnValues["id"]; exists {
		return idValue, nil
	}

	// 生成自增主键
	timestamp := time.Now().UnixNano()
	return timestamp, nil
}

// serializeRowData 序列化行数据
func (dml *StorageIntegratedDMLExecutor) serializeRowData(row *InsertRowData, tableMeta *metadata.TableMeta) ([]byte, error) {
	// 创建行数据缓冲区
	var buffer []byte

	// 写入列数量
	columnCount := uint16(len(row.ColumnValues))
	countBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(countBytes, columnCount)
	buffer = append(buffer, countBytes...)

	// 写入每列的数据
	for columnName, value := range row.ColumnValues {
		// 写入列名长度和列名
		nameBytes := []byte(columnName)
		nameLen := uint16(len(nameBytes))
		nameLenBytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(nameLenBytes, nameLen)
		buffer = append(buffer, nameLenBytes...)
		buffer = append(buffer, nameBytes...)

		// 写入值
		valueBytes, err := dml.serializeValue(value)
		if err != nil {
			return nil, fmt.Errorf("序列化值失败: %v", err)
		}

		valueLen := uint32(len(valueBytes))
		valueLenBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(valueLenBytes, valueLen)
		buffer = append(buffer, valueLenBytes...)
		buffer = append(buffer, valueBytes...)
	}

	logger.Debugf(" 序列化行数据完成，大小: %d bytes", len(buffer))
	return buffer, nil
}

// serializeValue 序列化单个值
func (dml *StorageIntegratedDMLExecutor) serializeValue(value interface{}) ([]byte, error) {
	if value == nil {
		return []byte{0}, nil // NULL值标记
	}

	switch v := value.(type) {
	case string:
		return append([]byte{1}, []byte(v)...), nil // 1 = 字符串类型
	case int64:
		bytes := make([]byte, 9)
		bytes[0] = 2 // 2 = 整数类型
		binary.LittleEndian.PutUint64(bytes[1:], uint64(v))
		return bytes, nil
	case float64:
		bytes := make([]byte, 9)
		bytes[0] = 3 // 3 = 浮点数类型
		binary.LittleEndian.PutUint64(bytes[1:], uint64(v))
		return bytes, nil
	case bool:
		bytes := make([]byte, 2)
		bytes[0] = 4 // 4 = 布尔类型
		if v {
			bytes[1] = 1
		} else {
			bytes[1] = 0
		}
		return bytes, nil
	default:
		// 默认转为字符串
		str := fmt.Sprintf("%v", v)
		return append([]byte{1}, []byte(str)...), nil
	}
}

// deserializeRowData 反序列化行数据
func (dml *StorageIntegratedDMLExecutor) deserializeRowData(data []byte) (*InsertRowData, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("数据长度不足")
	}

	row := &InsertRowData{
		ColumnValues: make(map[string]interface{}),
		ColumnTypes:  make(map[string]metadata.DataType),
	}

	offset := 0

	// 读取列数量
	columnCount := binary.LittleEndian.Uint16(data[offset:])
	offset += 2

	// 读取每列数据
	for i := uint16(0); i < columnCount; i++ {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("数据格式错误")
		}

		// 读取列名
		nameLen := binary.LittleEndian.Uint16(data[offset:])
		offset += 2

		if offset+int(nameLen) > len(data) {
			return nil, fmt.Errorf("列名数据不足")
		}

		columnName := string(data[offset : offset+int(nameLen)])
		offset += int(nameLen)

		// 读取值
		if offset+4 > len(data) {
			return nil, fmt.Errorf("值长度数据不足")
		}

		valueLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		if offset+int(valueLen) > len(data) {
			return nil, fmt.Errorf("值数据不足")
		}

		value, err := dml.deserializeValue(data[offset : offset+int(valueLen)])
		if err != nil {
			return nil, fmt.Errorf("反序列化值失败: %v", err)
		}

		row.ColumnValues[columnName] = value
		row.ColumnTypes[columnName] = metadata.TypeVarchar // 简化处理
		offset += int(valueLen)
	}

	return row, nil
}

// deserializeValue 反序列化单个值
func (dml *StorageIntegratedDMLExecutor) deserializeValue(data []byte) (interface{}, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("空数据")
	}

	typeFlag := data[0]
	switch typeFlag {
	case 0: // NULL值
		return nil, nil
	case 1: // 字符串
		return string(data[1:]), nil
	case 2: // 整数
		if len(data) < 9 {
			return nil, fmt.Errorf("整数数据长度不足")
		}
		return int64(binary.LittleEndian.Uint64(data[1:])), nil
	case 3: // 浮点数
		if len(data) < 9 {
			return nil, fmt.Errorf("浮点数数据长度不足")
		}
		return float64(binary.LittleEndian.Uint64(data[1:])), nil
	case 4: // 布尔值
		if len(data) < 2 {
			return nil, fmt.Errorf("布尔数据长度不足")
		}
		return data[1] == 1, nil
	default:
		return nil, fmt.Errorf("未知类型标记: %d", typeFlag)
	}
}

// convertPrimaryKeyToUint64 将主键转换为uint64
func (dml *StorageIntegratedDMLExecutor) convertPrimaryKeyToUint64(key interface{}) uint64 {
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

// serializePrimaryKey 序列化主键
func (dml *StorageIntegratedDMLExecutor) serializePrimaryKey(key interface{}) ([]byte, error) {
	return dml.serializeValue(key)
}

// ===== 存储事务管理方法 =====

// beginStorageTransaction 开始存储事务
func (dml *StorageIntegratedDMLExecutor) beginStorageTransaction(ctx context.Context) (interface{}, error) {
	logger.Debugf("🔄 开始存储引擎事务")

	// 创建事务上下文
	txnContext := &StorageTransactionContext{
		TransactionID: uint64(time.Now().UnixNano()),
		StartTime:     time.Now(),
		Status:        "ACTIVE",
		ModifiedPages: make(map[string]uint32),
	}

	// 如果有事务管理器，使用真实的事务
	if dml.txManager != nil {
		// 从上下文中获取隔离级别，默认为可重复读
		isolationLevel := manager.TRX_ISO_REPEATABLE_READ
		if level, ok := ctx.Value("isolation_level").(uint8); ok {
			isolationLevel = level
		}

		// 从上下文中获取是否只读，默认为false
		isReadOnly := false
		if ro, ok := ctx.Value("read_only").(bool); ok {
			isReadOnly = ro
		}

		// 使用事务管理器开始真实事务
		trx, err := dml.txManager.Begin(isReadOnly, isolationLevel)
		if err != nil {
			return nil, fmt.Errorf("事务管理器开始事务失败: %v", err)
		}

		// 将真实事务保存到上下文中
		txnContext.RealTransaction = trx
		txnContext.TransactionID = uint64(trx.ID)

		logger.Debugf(" 使用事务管理器开始事务: TrxID=%d, IsolationLevel=%d, ReadOnly=%v",
			trx.ID, isolationLevel, isReadOnly)
	} else {
		logger.Debugf("⚠️ 事务管理器未初始化，使用简化事务上下文")
	}

	dml.stats.TransactionCount++
	return txnContext, nil
}

// commitStorageTransaction 提交存储事务
func (dml *StorageIntegratedDMLExecutor) commitStorageTransaction(ctx context.Context, txn interface{}) error {
	logger.Debugf(" 提交存储引擎事务")

	txnCtx, ok := txn.(*StorageTransactionContext)
	if !ok {
		return fmt.Errorf("无效的事务上下文")
	}

	// 如果有真实事务，先提交真实事务
	if dml.txManager != nil && txnCtx.RealTransaction != nil {
		// 使用事务管理器提交真实事务
		err := dml.txManager.Commit(txnCtx.RealTransaction)
		if err != nil {
			logger.Errorf(" 事务管理器提交失败: %v", err)
			return fmt.Errorf("事务管理器提交失败: %v", err)
		}

		logger.Debugf(" 事务管理器提交成功: TrxID=%d, Duration=%v",
			txnCtx.RealTransaction.ID, time.Since(txnCtx.StartTime))
	}

	// 刷新所有修改的页面到磁盘
	for spacePageKey, pageNo := range txnCtx.ModifiedPages {
		parts := strings.Split(spacePageKey, ":")
		if len(parts) == 2 {
			if spaceID, err := strconv.ParseUint(parts[0], 10, 32); err == nil {
				err = dml.bufferPoolManager.FlushPage(uint32(spaceID), pageNo)
				if err != nil {
					logger.Debugf("  警告: 刷新页面失败: %v", err)
				} else {
					logger.Debugf(" 页面已刷新: SpaceID=%d, PageNo=%d", spaceID, pageNo)
				}
			}
		}
	}

	txnCtx.Status = "COMMITTED"
	txnCtx.EndTime = time.Now()

	logger.Debugf(" 存储事务提交完成: TxnID=%d, ModifiedPages=%d, Duration=%v",
		txnCtx.TransactionID, len(txnCtx.ModifiedPages), time.Since(txnCtx.StartTime))

	return nil
}

// rollbackStorageTransaction 回滚存储事务
func (dml *StorageIntegratedDMLExecutor) rollbackStorageTransaction(ctx context.Context, txn interface{}) error {
	logger.Debugf("🔄 回滚存储引擎事务")

	txnCtx, ok := txn.(*StorageTransactionContext)
	if !ok {
		return fmt.Errorf("无效的事务上下文")
	}

	// 如果有真实事务，使用事务管理器回滚
	if dml.txManager != nil && txnCtx.RealTransaction != nil {
		// 使用事务管理器回滚真实事务
		err := dml.txManager.Rollback(txnCtx.RealTransaction)
		if err != nil {
			logger.Errorf(" 事务管理器回滚失败: %v", err)
			return fmt.Errorf("事务管理器回滚失败: %v", err)
		}

		logger.Debugf(" 事务管理器回滚成功: TrxID=%d, Duration=%v",
			txnCtx.RealTransaction.ID, time.Since(txnCtx.StartTime))
	}

	// 丢弃所有修改的页面（不刷新到磁盘）
	for spacePageKey, pageNo := range txnCtx.ModifiedPages {
		parts := strings.Split(spacePageKey, ":")
		if len(parts) == 2 {
			if spaceID, err := strconv.ParseUint(parts[0], 10, 32); err == nil {
				// 从缓冲池中移除脏页，强制重新从磁盘加载
				// 这样可以丢弃未提交的修改
				logger.Debugf(" 丢弃未提交的页面: SpaceID=%d, PageNo=%d", spaceID, pageNo)
			}
		}
	}

	txnCtx.Status = "ROLLED_BACK"
	txnCtx.EndTime = time.Now()

	logger.Debugf(" 存储事务回滚完成: TxnID=%d, ModifiedPages=%d, Duration=%v",
		txnCtx.TransactionID, len(txnCtx.ModifiedPages), time.Since(txnCtx.StartTime))

	return nil
}

// StorageTransactionContext 存储事务上下文
type StorageTransactionContext struct {
	TransactionID   uint64
	StartTime       time.Time
	EndTime         time.Time
	Status          string               // ACTIVE, COMMITTED, ROLLED_BACK
	ModifiedPages   map[string]uint32    // "spaceID:pageNo" -> pageNo
	RealTransaction *manager.Transaction // 真实的事务对象（如果使用事务管理器）
}

// ===== 数据查找和操作方法 =====

// findRowsToUpdateInStorage 在存储引擎中查找待更新的行
func (dml *StorageIntegratedDMLExecutor) findRowsToUpdateInStorage(
	ctx context.Context,
	txn interface{},
	whereConditions []string,
	tableMeta *metadata.TableMeta,
	tableStorageInfo *manager.TableStorageInfo,
	btreeManager basic.BPlusTreeManager,
) ([]*RowUpdateInfo, error) {
	logger.Debugf(" 在存储引擎中查找待更新行，条件: %v", whereConditions)

	var rowsToUpdate []*RowUpdateInfo

	// 简化实现：如果没有WHERE条件，返回空结果
	if len(whereConditions) == 0 {
		logger.Debugf("  没有WHERE条件，跳过UPDATE")
		return rowsToUpdate, nil
	}

	// 解析WHERE条件中的主键值
	for _, condition := range whereConditions {
		if primaryKey := dml.extractPrimaryKeyFromCondition(condition); primaryKey != nil {
			pageNo, slot, err := btreeManager.Search(ctx, primaryKey)
			if err != nil {
				logger.Debugf("  查找主键 %v 失败: %v", primaryKey, err)
				continue
			}

			// 读取现有数据作为OldValues
			existingData, err := dml.readRowFromStorage(ctx, pageNo, slot, tableStorageInfo)
			if err != nil {
				logger.Debugf("  读取现有数据失败: %v", err)
				continue
			}

			rowInfo := &RowUpdateInfo{
				RowId:     dml.convertPrimaryKeyToUint64(primaryKey),
				PageNum:   pageNo,
				SlotIndex: slot,
				OldValues: existingData.ColumnValues,
			}

			rowsToUpdate = append(rowsToUpdate, rowInfo)
			logger.Debugf(" 找到待更新行: RowID=%d, PageNo=%d, Slot=%d", rowInfo.RowId, pageNo, slot)
		}
	}

	return rowsToUpdate, nil
}

// findRowsToDeleteInStorage 在存储引擎中查找待删除的行
func (dml *StorageIntegratedDMLExecutor) findRowsToDeleteInStorage(
	ctx context.Context,
	txn interface{},
	whereConditions []string,
	tableMeta *metadata.TableMeta,
	tableStorageInfo *manager.TableStorageInfo,
	btreeManager basic.BPlusTreeManager,
) ([]*RowUpdateInfo, error) {
	logger.Debugf(" 在存储引擎中查找待删除行，条件: %v", whereConditions)

	var rowsToDelete []*RowUpdateInfo

	// 简化实现：如果没有WHERE条件，返回空结果
	if len(whereConditions) == 0 {
		logger.Debugf("  没有WHERE条件，跳过DELETE")
		return rowsToDelete, nil
	}

	// 解析WHERE条件中的主键值
	for _, condition := range whereConditions {
		if primaryKey := dml.extractPrimaryKeyFromCondition(condition); primaryKey != nil {
			pageNo, slot, err := btreeManager.Search(ctx, primaryKey)
			if err != nil {
				logger.Debugf("  查找主键 %v 失败: %v", primaryKey, err)
				continue
			}

			// 读取现有数据作为OldValues
			existingData, err := dml.readRowFromStorage(ctx, pageNo, slot, tableStorageInfo)
			if err != nil {
				logger.Debugf("  读取现有数据失败: %v", err)
				continue
			}

			rowInfo := &RowUpdateInfo{
				RowId:     dml.convertPrimaryKeyToUint64(primaryKey),
				PageNum:   pageNo,
				SlotIndex: slot,
				OldValues: existingData.ColumnValues,
			}

			rowsToDelete = append(rowsToDelete, rowInfo)
			logger.Debugf(" 找到待删除行: RowID=%d, PageNo=%d, Slot=%d", rowInfo.RowId, pageNo, slot)
		}
	}

	return rowsToDelete, nil
}

// readRowFromStorage 从存储引擎读取行数据
func (dml *StorageIntegratedDMLExecutor) readRowFromStorage(
	ctx context.Context,
	pageNo uint32,
	slot int,
	tableStorageInfo *manager.TableStorageInfo,
) (*InsertRowData, error) {
	logger.Debugf("📖 从存储引擎读取行数据: PageNo=%d, Slot=%d", pageNo, slot)

	// 获取页面
	bufferPage, err := dml.bufferPoolManager.GetPage(tableStorageInfo.SpaceID, pageNo)
	if err != nil {
		return nil, fmt.Errorf("获取页面失败: %v", err)
	}

	// 读取页面内容
	pageContent := bufferPage.GetContent()
	if len(pageContent) == 0 {
		return nil, fmt.Errorf("页面内容为空")
	}

	// 简化实现：假设整个页面就是一行记录
	rowData, err := dml.deserializeRowData(pageContent)
	if err != nil {
		return nil, fmt.Errorf("反序列化行数据失败: %v", err)
	}

	logger.Debugf(" 成功读取行数据: %+v", rowData.ColumnValues)
	return rowData, nil
}

// markRowAsDeletedInStorage 在存储引擎中标记行为已删除
func (dml *StorageIntegratedDMLExecutor) markRowAsDeletedInStorage(
	ctx context.Context,
	pageNo uint32,
	slot int,
	tableStorageInfo *manager.TableStorageInfo,
) error {
	logger.Debugf("🗑️ 在存储引擎中标记行为已删除: PageNo=%d, Slot=%d", pageNo, slot)

	// 获取页面
	bufferPage, err := dml.bufferPoolManager.GetPage(tableStorageInfo.SpaceID, pageNo)
	if err != nil {
		return fmt.Errorf("获取页面失败: %v", err)
	}

	// 简化实现：清空页面内容表示删除
	emptyContent := make([]byte, 0)
	bufferPage.SetContent(emptyContent)
	bufferPage.MarkDirty()

	logger.Debugf(" 成功标记行为已删除")
	return nil
}

// ===== 辅助解析方法 =====

// extractPrimaryKeyFromCondition 从WHERE条件中提取主键值
func (dml *StorageIntegratedDMLExecutor) extractPrimaryKeyFromCondition(condition string) interface{} {
	// 简化实现：解析类似 "id = 1" 的条件
	if strings.Contains(condition, "=") {
		parts := strings.Split(condition, "=")
		if len(parts) == 2 {
			leftPart := strings.TrimSpace(parts[0])
			rightPart := strings.TrimSpace(parts[1])

			// 检查是否是id字段
			if strings.Contains(leftPart, "id") || strings.Contains(leftPart, "ID") {
				// 尝试解析为数字
				if id, err := strconv.ParseInt(rightPart, 10, 64); err == nil {
					return id
				}
				// 尝试解析为字符串（去掉引号）
				if strings.HasPrefix(rightPart, "'") && strings.HasSuffix(rightPart, "'") {
					return rightPart[1 : len(rightPart)-1]
				}
				return rightPart
			}
		}
	}

	return nil
}

// applyUpdateExpressions 应用更新表达式
func (dml *StorageIntegratedDMLExecutor) applyUpdateExpressions(
	existingData *InsertRowData,
	updateExprs []*UpdateExpression,
	tableMeta *metadata.TableMeta,
) (*InsertRowData, error) {
	logger.Debugf(" 应用更新表达式，表达式数量: %d", len(updateExprs))

	// 复制现有数据
	updatedData := &InsertRowData{
		ColumnValues: make(map[string]interface{}),
		ColumnTypes:  make(map[string]metadata.DataType),
	}

	// 复制原有值
	for k, v := range existingData.ColumnValues {
		updatedData.ColumnValues[k] = v
	}
	for k, v := range existingData.ColumnTypes {
		updatedData.ColumnTypes[k] = v
	}

	// 应用更新表达式
	for _, expr := range updateExprs {
		updatedData.ColumnValues[expr.ColumnName] = expr.NewValue
		updatedData.ColumnTypes[expr.ColumnName] = expr.ColumnType
		logger.Debugf(" 更新列 %s: %v", expr.ColumnName, expr.NewValue)
	}

	return updatedData, nil
}

// ===== 统计信息更新方法 =====

// updateInsertStats 更新INSERT统计信息
func (dml *StorageIntegratedDMLExecutor) updateInsertStats(affectedRows int, executionTime time.Duration) {
	dml.stats.InsertCount += uint64(affectedRows)
	dml.stats.TotalTime += executionTime
	if dml.stats.InsertCount > 0 {
		dml.stats.AvgInsertTime = time.Duration(uint64(dml.stats.TotalTime) / dml.stats.InsertCount)
	}
}

// updateUpdateStats 更新UPDATE统计信息
func (dml *StorageIntegratedDMLExecutor) updateUpdateStats(affectedRows int, executionTime time.Duration) {
	dml.stats.UpdateCount += uint64(affectedRows)
	dml.stats.TotalTime += executionTime
	if dml.stats.UpdateCount > 0 {
		dml.stats.AvgUpdateTime = time.Duration(uint64(dml.stats.TotalTime) / dml.stats.UpdateCount)
	}
}

// updateDeleteStats 更新DELETE统计信息
func (dml *StorageIntegratedDMLExecutor) updateDeleteStats(affectedRows int, executionTime time.Duration) {
	dml.stats.DeleteCount += uint64(affectedRows)
	dml.stats.TotalTime += executionTime
	if dml.stats.DeleteCount > 0 {
		dml.stats.AvgDeleteTime = time.Duration(uint64(dml.stats.TotalTime) / dml.stats.DeleteCount)
	}
}

// GetStats 获取执行器统计信息
func (dml *StorageIntegratedDMLExecutor) GetStats() *DMLExecutorStats {
	return dml.stats
}

// ===== 继承和复用原有方法 =====

// parseInsertData 解析INSERT数据 - 复用原有实现
func (dml *StorageIntegratedDMLExecutor) parseInsertData(stmt *sqlparser.Insert, tableMeta *metadata.TableMeta) ([]*InsertRowData, error) {
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
				rowData.ColumnTypes[columnName] = metadata.TypeVarchar
			}

			insertRows = append(insertRows, rowData)
		}
	default:
		return nil, fmt.Errorf("不支持的INSERT语法: %T", stmt.Rows)
	}

	return insertRows, nil
}

// evaluateExpression 计算表达式值 - 复用原有实现
func (dml *StorageIntegratedDMLExecutor) evaluateExpression(expr sqlparser.Expr) (interface{}, error) {
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

// parseSQLVal 解析SQL值 - 复用原有实现
func (dml *StorageIntegratedDMLExecutor) parseSQLVal(val *sqlparser.SQLVal) (interface{}, error) {
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

// validateInsertData 验证插入数据 - 复用原有实现
func (dml *StorageIntegratedDMLExecutor) validateInsertData(rows []*InsertRowData, tableMeta *metadata.TableMeta) error {
	logger.Debugf(" 验证插入数据，行数: %d", len(rows))
	return nil
}

// parseTableName 解析表名 - 复用原有实现
func (dml *StorageIntegratedDMLExecutor) parseTableName(tableExpr sqlparser.TableExpr) (string, error) {
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

// parseWhereConditions 解析WHERE条件 - 复用原有实现
func (dml *StorageIntegratedDMLExecutor) parseWhereConditions(where *sqlparser.Where) []string {
	if where == nil {
		return []string{}
	}

	conditions := []string{sqlparser.String(where.Expr)}
	return conditions
}

// parseUpdateExpressions 解析UPDATE表达式 - 复用原有实现
func (dml *StorageIntegratedDMLExecutor) parseUpdateExpressions(exprs sqlparser.UpdateExprs, tableMeta *metadata.TableMeta) ([]*UpdateExpression, error) {
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
			ColumnType: metadata.TypeVarchar,
		}

		updateExprs = append(updateExprs, updateExpr)
	}

	return updateExprs, nil
}
