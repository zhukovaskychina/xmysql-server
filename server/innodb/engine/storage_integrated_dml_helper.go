package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"regexp"
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
	if tableMeta == nil {
		if idValue, exists := row.ColumnValues["id"]; exists {
			return idValue, nil
		}
	}

	if tableMeta == nil {
		value := time.Now().UnixNano()
		row.ColumnValues["id"] = value
		row.ColumnTypes["id"] = metadata.TypeInt
		return value, nil
	}

	primaryKeyColumns := effectivePrimaryKeyColumns(tableMeta)
	if len(primaryKeyColumns) > 1 {
		return buildCompositeKey(row.ColumnValues, primaryKeyColumns)
	}

	if len(primaryKeyColumns) == 0 {
		hiddenID, err := dml.ensureHiddenRowID(row.ColumnValues)
		if err != nil {
			return nil, err
		}
		return string(hiddenID), nil
	}

	for _, col := range tableMeta.Columns {
		if col == nil || !col.IsPrimary {
			continue
		}
		if value, exists := row.ColumnValues[col.Name]; exists && value != nil {
			if col.IsAutoIncrement {
				if numericValue, ok := autoIncrementValueAsUint64(value); ok {
					if err := observeAutoIncrementValue(dml.dataDir, dml.schemaName, dml.tableName, col.Name, numericValue); err != nil {
						return nil, err
					}
				}
			}
			return value, nil
		}
		if col.IsAutoIncrement {
			next, err := allocateAutoIncrementValue(dml.dataDir, dml.schemaName, dml.tableName, col.Name)
			if err != nil {
				return nil, err
			}
			value := int64(next)
			row.ColumnValues[col.Name] = value
			row.ColumnTypes[col.Name] = col.Type
			return value, nil
		}
	}

	value := time.Now().UnixNano()
	row.ColumnValues["id"] = value
	row.ColumnTypes["id"] = metadata.TypeInt
	return value, nil
}

func (dml *StorageIntegratedDMLExecutor) ensureHiddenRowID(rowData map[string]interface{}) ([]byte, error) {
	if rowData == nil {
		return nil, fmt.Errorf("row data is nil")
	}
	if hiddenID, ok := hiddenRowIDBytesFromValue(rowData[hiddenRowIDColumnName]); ok {
		return hiddenID, nil
	}
	next, err := allocateAutoIncrementValue(dml.dataDir, dml.schemaName, dml.tableName, hiddenRowIDColumnName)
	if err != nil {
		return nil, err
	}
	hiddenID := []byte(fmt.Sprintf("__xmysql_hidden_pk_%020d", next))
	rowData[hiddenRowIDColumnName] = string(hiddenID)
	return hiddenID, nil
}

func autoIncrementValueAsUint64(value interface{}) (uint64, bool) {
	switch v := value.(type) {
	case int:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int8:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int16:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int32:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case uint:
		return uint64(v), true
	case uint8:
		return uint64(v), true
	case uint16:
		return uint64(v), true
	case uint32:
		return uint64(v), true
	case uint64:
		return v, true
	default:
		return 0, false
	}
}

// serializeRowData 序列化行数据
func (dml *StorageIntegratedDMLExecutor) serializeRowData(row *InsertRowData, tableMeta *metadata.TableMeta) ([]byte, error) {
	if tableMeta != nil {
		encoded, err := EncodeClusteredRecord(row, tableMeta)
		if err != nil {
			return nil, err
		}
		logger.Debugf(" 序列化行数据完成，大小: %d bytes", len(encoded))
		return encoded, nil
	}

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
func (dml *StorageIntegratedDMLExecutor) deserializeRowData(data []byte, tableMetaOpt ...*metadata.TableMeta) (*InsertRowData, error) {
	if len(tableMetaOpt) > 0 && tableMetaOpt[0] != nil {
		return DecodeClusteredRecord(data, tableMetaOpt[0])
	}
	if strings.HasPrefix(string(data), clusteredRecordMagic) {
		return nil, fmt.Errorf("clustered record metadata is required")
	}

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

	if dml.txManager == nil {
		return nil, fmt.Errorf("transaction manager is not initialized for storage-integrated DML")
	}

	// 创建事务上下文
	txnContext := &StorageTransactionContext{
		StartTime:     time.Now(),
		Status:        "ACTIVE",
		ModifiedPages: make(map[string]uint32),
	}

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

	if dml.stats != nil {
		dml.stats.TransactionCount++
	}
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
	if dml.bufferPoolManager != nil {
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
	} else if len(txnCtx.ModifiedPages) > 0 {
		logger.Debugf("⚠️ bufferPoolManager 未初始化，跳过 %d 个脏页刷新", len(txnCtx.ModifiedPages))
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
		hasUndoLogs := len(txnCtx.RealTransaction.UndoLogs) > 0
		// 使用事务管理器回滚真实事务
		err := dml.txManager.Rollback(txnCtx.RealTransaction)
		if err != nil {
			logger.Errorf("事务管理器回滚失败: tx_id=%d err=%T %v", txnCtx.RealTransaction.ID, err, err)
			return fmt.Errorf("事务管理器回滚失败: tx_id=%d: %w", txnCtx.RealTransaction.ID, err)
		}

		if !hasUndoLogs {
			logger.Debugf("⚠️ 事务 %d 未检测到 Undo 日志，按空回滚处理", txnCtx.RealTransaction.ID)
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

	if !hasEffectiveWhereConditions(whereConditions) {
		return nil, fmt.Errorf("UPDATE without WHERE is not supported by storage integrated DML helper")
	}

	rowsToUpdate, err := dml.scanRowsForConditions(ctx, whereConditions, tableMeta, tableStorageInfo, btreeManager)
	if err != nil {
		return nil, err
	}
	return rowsToUpdate, nil
}

func (dml *StorageIntegratedDMLExecutor) scanRowsForConditions(
	ctx context.Context,
	whereConditions []string,
	tableMeta *metadata.TableMeta,
	tableStorageInfo *manager.TableStorageInfo,
	btreeManager basic.BPlusTreeManager,
) ([]*RowUpdateInfo, error) {
	return dml.scanRowsForTableConditions(ctx, dml.schemaName, dml.tableName, whereConditions, tableMeta, tableStorageInfo, btreeManager)
}

func (dml *StorageIntegratedDMLExecutor) scanRowsForTableConditions(
	ctx context.Context,
	schemaName string,
	tableName string,
	whereConditions []string,
	tableMeta *metadata.TableMeta,
	tableStorageInfo *manager.TableStorageInfo,
	btreeManager basic.BPlusTreeManager,
) ([]*RowUpdateInfo, error) {
	if btreeManager == nil {
		return nil, fmt.Errorf("B+树管理器未初始化")
	}

	scanner := NewClusteredIndexScanner(btreeManager, tableMeta)
	rows, err := scanner.ScanWithStorageKeys(ctx, whereConditions)
	if err != nil {
		return nil, err
	}

	matched := make([]*RowUpdateInfo, 0, len(rows))
	for slot, scannedRow := range rows {
		rowData := scannedRow.data
		rowID := dml.rowIDFromRowData(rowData, tableMeta)
		pageNum := tableStorageInfo.RootPageNo
		if scannedRow.pageNumber != 0 {
			pageNum = scannedRow.pageNumber
		}
		matched = append(matched, &RowUpdateInfo{
			RowId:      rowID,
			StorageKey: scannedRow.storageKey,
			PageNum:    pageNum,
			SlotIndex:  slot,
			SchemaName: schemaName,
			TableName:  tableName,
			OldValues:  rowData.ColumnValues,
		})
	}

	return matched, nil
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

	if !hasEffectiveWhereConditions(whereConditions) {
		return dml.scanRowsForConditions(ctx, nil, tableMeta, tableStorageInfo, btreeManager)
	}

	rowsToDelete, err := dml.scanRowsForConditions(ctx, whereConditions, tableMeta, tableStorageInfo, btreeManager)
	if err != nil {
		return nil, err
	}
	return rowsToDelete, nil
}

func hasEffectiveWhereConditions(whereConditions []string) bool {
	for _, condition := range whereConditions {
		if strings.TrimSpace(condition) != "" {
			return true
		}
	}
	return false
}

// readRowFromStorage 从存储引擎读取行数据
func (dml *StorageIntegratedDMLExecutor) readRowFromStorage(
	ctx context.Context,
	pageNo uint32,
	slot int,
	tableStorageInfo *manager.TableStorageInfo,
	tableMetaOpt ...*metadata.TableMeta,
) (*InsertRowData, error) {
	logger.Debugf("📖 从存储引擎读取行数据: PageNo=%d, Slot=%d", pageNo, slot)
	return nil, fmt.Errorf("B+Tree record reader is not wired for storage-integrated DML yet")
}

// ===== 辅助解析方法 =====

// extractPrimaryKeyFromCondition 从WHERE条件中提取主键值
func (dml *StorageIntegratedDMLExecutor) extractPrimaryKeyFromCondition(condition string) interface{} {
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

func (dml *StorageIntegratedDMLExecutor) extractPrimaryKeyFromExpr(expr sqlparser.Expr) interface{} {
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
		if !ok || !isPrimaryKeyColumn(colName.Name.String()) {
			return nil
		}
		return dml.parsePrimaryKeyValue(v.Right)
	case *sqlparser.ParenExpr:
		return dml.extractPrimaryKeyFromExpr(v.Expr)
	default:
		return nil
	}
}

func (dml *StorageIntegratedDMLExecutor) parsePrimaryKeyValue(expr sqlparser.Expr) interface{} {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		pkVal, err := dml.parseSQLVal(v)
		if err != nil {
			logger.Debugf(" 解析主键值失败: %v", err)
			return nil
		}
		return pkVal
	case *sqlparser.ParenExpr:
		return dml.parsePrimaryKeyValue(v.Expr)
	case *sqlparser.UnaryExpr:
		if v.Operator == sqlparser.MinusStr && v.Expr != nil {
			if intVal, ok := dml.parsePrimaryKeyValue(v.Expr).(int64); ok {
				return -intVal
			}
			return nil
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

func isPrimaryKeyColumn(raw string) bool {
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
		newValue := expr.NewValue
		if expr.Expr != nil {
			value, err := evaluateExpressionWithRow(expr.Expr, updatedData.ColumnValues)
			if err != nil {
				return nil, err
			}
			newValue = value
		}
		updatedData.ColumnValues[expr.ColumnName] = newValue
		updatedData.ColumnTypes[expr.ColumnName] = expr.ColumnType
		logger.Debugf(" 更新列 %s: %v", expr.ColumnName, newValue)
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
func (dml *StorageIntegratedDMLExecutor) parseInsertData(ctx context.Context, stmt *sqlparser.Insert, tableMeta *metadata.TableMeta, schemaName string) ([]*InsertRowData, error) {
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
	case *sqlparser.Select:
		return dml.parseInsertSelectData(ctx, valuesClause, tableMeta, schemaName, columnNames)
	default:
		return nil, fmt.Errorf("不支持的INSERT语法: %T", stmt.Rows)
	}

	return insertRows, nil
}

func (dml *StorageIntegratedDMLExecutor) parseInsertSelectData(
	ctx context.Context,
	selectStmt *sqlparser.Select,
	targetMeta *metadata.TableMeta,
	targetSchema string,
	targetColumnNames []string,
) ([]*InsertRowData, error) {
	if selectStmt == nil {
		return nil, fmt.Errorf("INSERT SELECT语句为空")
	}
	if len(selectStmt.From) != 1 {
		return nil, fmt.Errorf("INSERT SELECT仅支持单表来源")
	}

	sourceTableName, err := dml.parseTableName(selectStmt.From[0])
	if err != nil {
		return nil, err
	}
	sourceSchemaName, err := dml.parseTableSchema(selectStmt.From[0])
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(sourceSchemaName) == "" {
		sourceSchemaName = targetSchema
	}

	sourceMeta, err := dml.getTableMetadataFor(sourceSchemaName, sourceTableName)
	if err != nil {
		return nil, err
	}
	sourceStorageInfo, err := dml.tableStorageManager.GetTableStorageInfo(sourceSchemaName, sourceTableName)
	if err != nil {
		return nil, fmt.Errorf("获取INSERT SELECT来源表存储信息失败: %v", err)
	}
	sourceBtreeManager, err := dml.tableStorageManager.CreateBTreeManagerForTable(ctx, sourceSchemaName, sourceTableName)
	if err != nil {
		return nil, fmt.Errorf("创建INSERT SELECT来源表B+树管理器失败: %v", err)
	}

	whereConditions := dml.parseWhereConditions(selectStmt.Where)
	sourceRows, err := dml.scanRowsForTableConditions(ctx, sourceSchemaName, sourceTableName, whereConditions, sourceMeta, sourceStorageInfo, sourceBtreeManager)
	if err != nil {
		return nil, err
	}

	sourceColumnNames, err := dml.selectProjectionColumnNames(selectStmt, sourceMeta)
	if err != nil {
		return nil, err
	}
	if len(sourceColumnNames) != len(targetColumnNames) {
		return nil, fmt.Errorf("列数量不匹配: 期望 %d，实际 %d", len(targetColumnNames), len(sourceColumnNames))
	}

	insertRows := make([]*InsertRowData, 0, len(sourceRows))
	for _, sourceRow := range sourceRows {
		rowData := &InsertRowData{
			ColumnValues: make(map[string]interface{}),
			ColumnTypes:  make(map[string]metadata.DataType),
		}
		for i, sourceColumnName := range sourceColumnNames {
			targetColumnName := targetColumnNames[i]
			value, exists := sourceRow.OldValues[sourceColumnName]
			if !exists {
				return nil, fmt.Errorf("INSERT SELECT来源列不存在: %s", sourceColumnName)
			}
			rowData.ColumnValues[targetColumnName] = value
			if col := findColumnMeta(targetMeta, targetColumnName); col != nil {
				rowData.ColumnTypes[targetColumnName] = col.Type
			} else {
				rowData.ColumnTypes[targetColumnName] = metadata.TypeVarchar
			}
		}
		insertRows = append(insertRows, rowData)
	}

	return insertRows, nil
}

func (dml *StorageIntegratedDMLExecutor) selectProjectionColumnNames(selectStmt *sqlparser.Select, sourceMeta *metadata.TableMeta) ([]string, error) {
	if len(selectStmt.SelectExprs) == 1 {
		if _, ok := selectStmt.SelectExprs[0].(*sqlparser.StarExpr); ok {
			names := make([]string, 0, len(sourceMeta.Columns))
			for _, col := range sourceMeta.Columns {
				if col != nil {
					names = append(names, col.Name)
				}
			}
			return names, nil
		}
	}

	columnNames := make([]string, 0, len(selectStmt.SelectExprs))
	for _, expr := range selectStmt.SelectExprs {
		aliasedExpr, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("INSERT SELECT仅支持列投影")
		}
		colName, ok := aliasedExpr.Expr.(*sqlparser.ColName)
		if !ok {
			return nil, fmt.Errorf("INSERT SELECT仅支持列投影")
		}
		columnNames = append(columnNames, colName.Name.String())
	}
	return columnNames, nil
}

func (dml *StorageIntegratedDMLExecutor) getTableMetadataFor(schemaName string, tableName string) (*metadata.TableMeta, error) {
	originalSchema, originalTable := dml.schemaName, dml.tableName
	dml.schemaName, dml.tableName = schemaName, tableName
	defer func() {
		dml.schemaName, dml.tableName = originalSchema, originalTable
	}()
	return dml.getTableMetadata()
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

// parseTableSchema 解析表schema（限定符）
func (dml *StorageIntegratedDMLExecutor) parseTableSchema(tableExpr sqlparser.TableExpr) (string, error) {
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
			if _, ok := expr.Expr.(*sqlparser.BinaryExpr); !ok {
				return nil, fmt.Errorf("计算更新表达式值失败: %v", err)
			}
		}

		updateExpr := &UpdateExpression{
			ColumnName: columnName,
			NewValue:   value,
			ColumnType: metadata.TypeVarchar,
			Expr:       expr.Expr,
		}

		updateExprs = append(updateExprs, updateExpr)
	}

	return updateExprs, nil
}

func (dml *StorageIntegratedDMLExecutor) validateInsertData(rows []*InsertRowData, tableMeta *metadata.TableMeta) error {
	logger.Debugf(" 验证插入数据，行数: %d", len(rows))
	if tableMeta == nil {
		return fmt.Errorf("表元数据为空")
	}

	for _, row := range rows {
		if row == nil {
			return fmt.Errorf("存在空行数据")
		}
		for _, col := range tableMeta.Columns {
			if col == nil || col.Name == "" {
				continue
			}
			if _, exists := row.ColumnValues[col.Name]; exists {
				if row.ColumnValues[col.Name] == nil && !col.IsNullable && !col.IsAutoIncrement {
					return fmt.Errorf("Column '%s' cannot be null: not null constraint failed", col.Name)
				}
				continue
			}
			if col.IsAutoIncrement {
				continue
			}
			if col.DefaultValue != nil {
				row.ColumnValues[col.Name] = normalizeDefaultValue(col.DefaultValue, col.Type)
				row.ColumnTypes[col.Name] = col.Type
				continue
			}
			if !col.IsNullable {
				return fmt.Errorf("Column '%s' cannot be null: not null constraint failed", col.Name)
			}
		}
	}

	return nil
}

func normalizeDefaultValue(raw interface{}, dataType metadata.DataType) interface{} {
	s := strings.TrimSpace(fmt.Sprintf("%v", raw))
	if len(s) >= 2 {
		if (s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"') {
			s = s[1 : len(s)-1]
		}
	}
	if strings.EqualFold(s, "current_timestamp") || strings.EqualFold(s, "current_timestamp()") {
		return time.Now().Format("2006-01-02 15:04:05")
	}
	switch strings.ToUpper(string(dataType)) {
	case "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT":
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n
		}
	case "DECIMAL", "FLOAT", "DOUBLE":
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
	}
	return s
}

func (dml *StorageIntegratedDMLExecutor) validateUniqueConstraints(
	ctx context.Context,
	insertRows []*InsertRowData,
	tableMeta *metadata.TableMeta,
	tableStorageInfo *manager.TableStorageInfo,
	btreeManager basic.BPlusTreeManager,
) error {
	if len(tableMeta.PrimaryKey) > 0 {
		seenPrimaryKeys := make(map[string]struct{})
		for _, row := range insertRows {
			keyBytes, ok, err := buildPrimaryKeyIfAvailable(row.ColumnValues, tableMeta)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			key := string(keyBytes)
			if _, exists := seenPrimaryKeys[key]; exists {
				return fmt.Errorf("Duplicate entry '%s' for key 'PRIMARY'", formatCompositeKeyValues(row.ColumnValues, tableMeta.PrimaryKey))
			}
			seenPrimaryKeys[key] = struct{}{}
		}
	}

	uniqueColumns := make([]string, 0)
	for _, col := range tableMeta.Columns {
		if col != nil && col.IsUnique && !isCompositePrimaryKeyColumn(col.Name, tableMeta.PrimaryKey) {
			uniqueColumns = append(uniqueColumns, col.Name)
		}
	}
	if len(uniqueColumns) == 0 && len(tableMeta.PrimaryKey) == 0 {
		return nil
	}

	seen := make(map[string]struct{})
	for _, row := range insertRows {
		for _, colName := range uniqueColumns {
			value, exists := row.ColumnValues[colName]
			if !exists || value == nil {
				continue
			}
			key := strings.ToLower(colName) + "=" + fmt.Sprintf("%v", value)
			if _, exists := seen[key]; exists {
				return fmt.Errorf("Duplicate entry '%v' for key '%s'", value, colName)
			}
			seen[key] = struct{}{}
		}
	}

	existingRows, err := dml.scanRowsForConditions(ctx, nil, tableMeta, tableStorageInfo, btreeManager)
	if err != nil {
		return err
	}
	for _, rowInfo := range existingRows {
		if len(tableMeta.PrimaryKey) > 0 {
			existingKey, ok, err := buildPrimaryKeyIfAvailable(rowInfo.OldValues, tableMeta)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			for _, row := range insertRows {
				incomingKey, ok, err := buildPrimaryKeyIfAvailable(row.ColumnValues, tableMeta)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
				if string(existingKey) == string(incomingKey) {
					return fmt.Errorf("Duplicate entry '%s' for key 'PRIMARY'", formatCompositeKeyValues(row.ColumnValues, tableMeta.PrimaryKey))
				}
			}
		}
		for _, colName := range uniqueColumns {
			existing, exists := rowInfo.OldValues[colName]
			if !exists || existing == nil {
				continue
			}
			for _, row := range insertRows {
				incoming, exists := row.ColumnValues[colName]
				if !exists || incoming == nil {
					continue
				}
				if compareScalarValues(existing, incoming) == 0 {
					return fmt.Errorf("Duplicate entry '%v' for key '%s'", incoming, colName)
				}
			}
		}
	}

	return nil
}

func (dml *StorageIntegratedDMLExecutor) findDuplicateRowsForInsert(
	ctx context.Context,
	insertRows []*InsertRowData,
	tableMeta *metadata.TableMeta,
	tableStorageInfo *manager.TableStorageInfo,
	btreeManager basic.BPlusTreeManager,
) ([]*RowUpdateInfo, error) {
	duplicateRowsByInsert, err := dml.findDuplicateRowsByInsert(ctx, insertRows, tableMeta, tableStorageInfo, btreeManager)
	if err != nil {
		return nil, err
	}
	return flattenDuplicateRowsByInsert(duplicateRowsByInsert, tableMeta), nil
}

func (dml *StorageIntegratedDMLExecutor) findDuplicateRowsByInsert(
	ctx context.Context,
	insertRows []*InsertRowData,
	tableMeta *metadata.TableMeta,
	tableStorageInfo *manager.TableStorageInfo,
	btreeManager basic.BPlusTreeManager,
) (map[int][]*RowUpdateInfo, error) {
	duplicateRowsByInsert := make(map[int][]*RowUpdateInfo)
	if tableMeta == nil || len(insertRows) == 0 {
		return duplicateRowsByInsert, nil
	}
	if len(tableMeta.PrimaryKey) == 0 && len(uniqueConstraintColumns(tableMeta)) == 0 {
		return duplicateRowsByInsert, nil
	}

	existingRows, err := dml.scanRowsForConditions(ctx, nil, tableMeta, tableStorageInfo, btreeManager)
	if err != nil {
		return nil, err
	}

	uniqueColumns := uniqueConstraintColumns(tableMeta)
	for _, existing := range existingRows {
		if existing == nil {
			continue
		}
		for incomingIndex, incoming := range insertRows {
			if incoming == nil {
				continue
			}
			matched, err := rowConflictsWithInsert(existing.OldValues, incoming.ColumnValues, tableMeta, uniqueColumns)
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}
			if !rowInfoListContains(duplicateRowsByInsert[incomingIndex], existing, tableMeta) {
				duplicateRowsByInsert[incomingIndex] = append(duplicateRowsByInsert[incomingIndex], existing)
			}
		}
	}
	return duplicateRowsByInsert, nil
}

func rowConflictsWithInsert(
	existingValues map[string]interface{},
	incomingValues map[string]interface{},
	tableMeta *metadata.TableMeta,
	uniqueColumns []string,
) (bool, error) {
	if len(tableMeta.PrimaryKey) > 0 {
		existingKey, existingOK, err := buildPrimaryKeyIfAvailable(existingValues, tableMeta)
		if err != nil {
			return false, err
		}
		incomingKey, incomingOK, err := buildPrimaryKeyIfAvailable(incomingValues, tableMeta)
		if err != nil {
			return false, err
		}
		if existingOK && incomingOK && string(existingKey) == string(incomingKey) {
			return true, nil
		}
	}
	for _, colName := range uniqueColumns {
		existing, existingOK := existingValues[colName]
		incoming, incomingOK := incomingValues[colName]
		if !existingOK || !incomingOK || existing == nil || incoming == nil {
			continue
		}
		if compareScalarValues(existing, incoming) == 0 {
			return true, nil
		}
	}
	return false, nil
}

func rowInfoListContains(rows []*RowUpdateInfo, candidate *RowUpdateInfo, tableMeta *metadata.TableMeta) bool {
	candidateKey := duplicateRowIdentity(candidate, tableMeta)
	for _, row := range rows {
		if duplicateRowIdentity(row, tableMeta) == candidateKey {
			return true
		}
	}
	return false
}

func duplicateRowIdentity(rowInfo *RowUpdateInfo, tableMeta *metadata.TableMeta) string {
	if rowInfo == nil {
		return ""
	}
	if key, ok, err := buildPrimaryKeyIfAvailable(rowInfo.OldValues, tableMeta); err == nil && ok {
		return string(key)
	}
	return fmt.Sprintf("%d:%d", rowInfo.PageNum, rowInfo.SlotIndex)
}

func (dml *StorageIntegratedDMLExecutor) validateUpdateConstraints(
	ctx context.Context,
	rowsToUpdate []*RowUpdateInfo,
	updateExprs []*UpdateExpression,
	tableMeta *metadata.TableMeta,
	tableStorageInfo *manager.TableStorageInfo,
	btreeManager basic.BPlusTreeManager,
) error {
	if len(rowsToUpdate) == 0 {
		return nil
	}

	updatedRows := make([]*InsertRowData, 0, len(rowsToUpdate))
	oldRows := make([]map[string]interface{}, 0, len(rowsToUpdate))
	for _, rowInfo := range rowsToUpdate {
		if rowInfo == nil {
			return fmt.Errorf("待更新行信息不能为空")
		}
		if len(rowInfo.OldValues) == 0 {
			return fmt.Errorf("B+Tree record reader is not wired for UPDATE old values")
		}
		existingRowData := &InsertRowData{
			ColumnValues: make(map[string]interface{}, len(rowInfo.OldValues)),
			ColumnTypes:  make(map[string]metadata.DataType, len(rowInfo.OldValues)),
		}
		for columnName, value := range rowInfo.OldValues {
			existingRowData.ColumnValues[columnName] = value
		}
		updatedRow, err := dml.applyUpdateExpressions(existingRowData, updateExprs, tableMeta)
		if err != nil {
			return fmt.Errorf("应用更新表达式失败: %v", err)
		}
		updatedRows = append(updatedRows, updatedRow)
		oldRows = append(oldRows, rowInfo.OldValues)
	}

	if err := dml.validateInsertData(updatedRows, tableMeta); err != nil {
		return err
	}
	if err := validateUpdatedRowsUniqueWithinBatch(updatedRows, tableMeta); err != nil {
		return err
	}

	existingRows, err := dml.scanRowsForConditions(ctx, nil, tableMeta, tableStorageInfo, btreeManager)
	if err != nil {
		return err
	}
	uniqueColumns := uniqueConstraintColumns(tableMeta)
	for _, existing := range existingRows {
		if existing == nil {
			continue
		}
		for i, updated := range updatedRows {
			if sameRowIdentity(existing, rowsToUpdate[i], oldRows[i], tableMeta) {
				continue
			}
			if len(tableMeta.PrimaryKey) > 0 {
				existingKey, ok, err := buildPrimaryKeyIfAvailable(existing.OldValues, tableMeta)
				if err != nil {
					return err
				}
				if ok {
					incomingKey, ok, err := buildPrimaryKeyIfAvailable(updated.ColumnValues, tableMeta)
					if err != nil {
						return err
					}
					if ok && string(existingKey) == string(incomingKey) {
						return fmt.Errorf("Duplicate entry '%s' for key 'PRIMARY'", formatCompositeKeyValues(updated.ColumnValues, tableMeta.PrimaryKey))
					}
				}
			}
			for _, colName := range uniqueColumns {
				existingValue, exists := existing.OldValues[colName]
				if !exists || existingValue == nil {
					continue
				}
				incomingValue, exists := updated.ColumnValues[colName]
				if !exists || incomingValue == nil {
					continue
				}
				if compareScalarValues(existingValue, incomingValue) == 0 {
					return fmt.Errorf("Duplicate entry '%v' for key '%s'", incomingValue, colName)
				}
			}
		}
	}

	return nil
}

func validateUpdatedRowsUniqueWithinBatch(rows []*InsertRowData, tableMeta *metadata.TableMeta) error {
	if len(tableMeta.PrimaryKey) > 0 {
		seenPrimaryKeys := make(map[string]struct{})
		for _, row := range rows {
			keyBytes, ok, err := buildPrimaryKeyIfAvailable(row.ColumnValues, tableMeta)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			key := string(keyBytes)
			if _, exists := seenPrimaryKeys[key]; exists {
				return fmt.Errorf("Duplicate entry '%s' for key 'PRIMARY'", formatCompositeKeyValues(row.ColumnValues, tableMeta.PrimaryKey))
			}
			seenPrimaryKeys[key] = struct{}{}
		}
	}

	for _, colName := range uniqueConstraintColumns(tableMeta) {
		seen := make(map[string]interface{})
		for _, row := range rows {
			value, exists := row.ColumnValues[colName]
			if !exists || value == nil {
				continue
			}
			key := fmt.Sprintf("%v", value)
			if previous, exists := seen[key]; exists && compareScalarValues(previous, value) == 0 {
				return fmt.Errorf("Duplicate entry '%v' for key '%s'", value, colName)
			}
			seen[key] = value
		}
	}

	return nil
}

func uniqueConstraintColumns(tableMeta *metadata.TableMeta) []string {
	if tableMeta == nil {
		return nil
	}
	uniqueColumns := make([]string, 0)
	for _, col := range tableMeta.Columns {
		if col != nil && col.IsUnique && !isCompositePrimaryKeyColumn(col.Name, tableMeta.PrimaryKey) {
			uniqueColumns = append(uniqueColumns, col.Name)
		}
	}
	return uniqueColumns
}

func samePrimaryKey(left, right map[string]interface{}, tableMeta *metadata.TableMeta) bool {
	if tableMeta == nil || len(tableMeta.PrimaryKey) == 0 {
		return false
	}
	leftKey, leftOK, err := buildPrimaryKeyIfAvailable(left, tableMeta)
	if err != nil || !leftOK {
		return false
	}
	rightKey, rightOK, err := buildPrimaryKeyIfAvailable(right, tableMeta)
	if err != nil || !rightOK {
		return false
	}
	return string(leftKey) == string(rightKey)
}

func sameRowIdentity(existing, updating *RowUpdateInfo, updatingOldValues map[string]interface{}, tableMeta *metadata.TableMeta) bool {
	if existing == nil || updating == nil {
		return false
	}
	if samePrimaryKey(existing.OldValues, updatingOldValues, tableMeta) {
		return true
	}
	if tableMeta != nil && len(tableMeta.PrimaryKey) > 0 {
		return false
	}
	return existing.PageNum == updating.PageNum && existing.SlotIndex == updating.SlotIndex
}

func hasCompositePrimaryKey(tableMeta *metadata.TableMeta) bool {
	return len(effectivePrimaryKeyColumns(tableMeta)) > 1
}

func hasAnyIndexMetadata(tableMeta *metadata.TableMeta) bool {
	if tableMeta == nil {
		return false
	}
	if len(tableMeta.Indices) > 0 {
		return true
	}
	for _, col := range tableMeta.Columns {
		if col != nil && col.IsUnique {
			return true
		}
	}
	return false
}

func updateTouchesPrimaryKey(updateExprs []*UpdateExpression, tableMeta *metadata.TableMeta) bool {
	primaryKeyColumns := effectivePrimaryKeyColumns(tableMeta)
	if len(primaryKeyColumns) == 0 {
		return false
	}
	for _, expr := range updateExprs {
		if expr == nil {
			continue
		}
		for _, pkColumn := range primaryKeyColumns {
			if strings.EqualFold(expr.ColumnName, pkColumn) {
				return true
			}
		}
	}
	return false
}

func updateTouchesAnyIndex(updateExprs []*UpdateExpression, tableMeta *metadata.TableMeta) bool {
	if tableMeta == nil || len(tableMeta.Indices) == 0 {
		return false
	}
	for _, expr := range updateExprs {
		if expr == nil {
			continue
		}
		for _, idx := range tableMeta.Indices {
			for _, col := range idx.Columns {
				if strings.EqualFold(expr.ColumnName, col) {
					return true
				}
			}
		}
	}
	return false
}

func buildPrimaryKeyIfAvailable(row map[string]interface{}, tableMeta *metadata.TableMeta) ([]byte, bool, error) {
	if tableMeta == nil {
		return nil, false, nil
	}
	primaryKeyColumns := effectivePrimaryKeyColumns(tableMeta)
	if len(primaryKeyColumns) == 0 {
		return nil, false, nil
	}
	for _, columnName := range primaryKeyColumns {
		value, exists := row[columnName]
		if exists && value != nil {
			continue
		}
		if col := findColumnMeta(tableMeta, columnName); col != nil && col.IsAutoIncrement {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("missing primary key column '%s'", columnName)
	}
	key, err := buildCompositeKey(row, primaryKeyColumns)
	return key, true, err
}

func effectivePrimaryKeyColumns(tableMeta *metadata.TableMeta) []string {
	if tableMeta == nil {
		return nil
	}
	if len(tableMeta.PrimaryKey) > 0 {
		return tableMeta.PrimaryKey
	}
	columns := make([]string, 0)
	for _, col := range tableMeta.Columns {
		if col != nil && col.IsPrimary {
			columns = append(columns, col.Name)
		}
	}
	return columns
}

func findColumnMeta(tableMeta *metadata.TableMeta, columnName string) *metadata.ColumnMeta {
	if tableMeta == nil {
		return nil
	}
	for _, col := range tableMeta.Columns {
		if col != nil && strings.EqualFold(col.Name, columnName) {
			return col
		}
	}
	return nil
}

func isCompositePrimaryKeyColumn(columnName string, primaryKey []string) bool {
	if len(primaryKey) <= 1 {
		return false
	}
	for _, pkCol := range primaryKey {
		if strings.EqualFold(pkCol, columnName) {
			return true
		}
	}
	return false
}

func formatCompositeKeyValues(row map[string]interface{}, columns []string) string {
	values := make([]string, 0, len(columns))
	for _, col := range columns {
		values = append(values, fmt.Sprintf("%v", row[col]))
	}
	return strings.Join(values, "-")
}

func (dml *StorageIntegratedDMLExecutor) rowIDFromRowData(row *InsertRowData, tableMeta *metadata.TableMeta) uint64 {
	if row == nil {
		return 0
	}
	for _, col := range tableMeta.Columns {
		if col == nil || !col.IsPrimary {
			continue
		}
		if value, exists := row.ColumnValues[col.Name]; exists {
			return dml.convertPrimaryKeyToUint64(value)
		}
	}
	if value, exists := row.ColumnValues["id"]; exists {
		return dml.convertPrimaryKeyToUint64(value)
	}
	return 0
}

func rowMatchesWhereConditions(values map[string]interface{}, whereConditions []string) (bool, error) {
	if !hasEffectiveWhereConditions(whereConditions) {
		return true, nil
	}
	for _, condition := range whereConditions {
		condition = strings.TrimSpace(condition)
		if condition == "" {
			continue
		}
		stmt, err := sqlparser.Parse("SELECT 1 FROM dual WHERE " + condition)
		if err != nil {
			return false, err
		}
		selectStmt, ok := stmt.(*sqlparser.Select)
		if !ok || selectStmt.Where == nil {
			return false, nil
		}
		return evalPredicate(selectStmt.Where.Expr, values)
	}
	return true, nil
}

func evalPredicate(expr sqlparser.Expr, values map[string]interface{}) (bool, error) {
	switch v := expr.(type) {
	case *sqlparser.AndExpr:
		left, err := evalPredicate(v.Left, values)
		if err != nil || !left {
			return left, err
		}
		return evalPredicate(v.Right, values)
	case *sqlparser.OrExpr:
		left, err := evalPredicate(v.Left, values)
		if err != nil || left {
			return left, err
		}
		return evalPredicate(v.Right, values)
	case *sqlparser.ParenExpr:
		return evalPredicate(v.Expr, values)
	case *sqlparser.RangeCond:
		left, err := evaluateExpressionWithRow(v.Left, values)
		if err != nil {
			return false, err
		}
		from, err := evaluateExpressionWithRow(v.From, values)
		if err != nil {
			return false, err
		}
		to, err := evaluateExpressionWithRow(v.To, values)
		if err != nil {
			return false, err
		}
		matched := compareScalarValues(left, from) >= 0 && compareScalarValues(left, to) <= 0
		if v.Operator == sqlparser.NotBetweenStr {
			return !matched, nil
		}
		return matched, nil
	case *sqlparser.ComparisonExpr:
		left, err := evaluateExpressionWithRow(v.Left, values)
		if err != nil {
			return false, err
		}
		switch v.Operator {
		case sqlparser.InStr, sqlparser.NotInStr:
			matched, err := evalInPredicate(left, v.Right, values)
			if err != nil {
				return false, err
			}
			if v.Operator == sqlparser.NotInStr {
				return !matched, nil
			}
			return matched, nil
		case sqlparser.LikeStr, sqlparser.NotLikeStr:
			right, err := evaluateExpressionWithRow(v.Right, values)
			if err != nil {
				return false, err
			}
			matched := sqlLikePatternMatch(fmt.Sprintf("%v", left), fmt.Sprintf("%v", right))
			if v.Operator == sqlparser.NotLikeStr {
				return !matched, nil
			}
			return matched, nil
		}
		right, err := evaluateExpressionWithRow(v.Right, values)
		if err != nil {
			return false, err
		}
		cmp := compareScalarValues(left, right)
		switch v.Operator {
		case sqlparser.EqualStr:
			return cmp == 0, nil
		case sqlparser.NotEqualStr:
			return cmp != 0, nil
		case sqlparser.LessThanStr:
			return cmp < 0, nil
		case sqlparser.LessEqualStr:
			return cmp <= 0, nil
		case sqlparser.GreaterThanStr:
			return cmp > 0, nil
		case sqlparser.GreaterEqualStr:
			return cmp >= 0, nil
		default:
			return false, fmt.Errorf("不支持的比较操作符: %s", v.Operator)
		}
	default:
		return false, fmt.Errorf("不支持的WHERE表达式类型: %T", expr)
	}
}

func evalInPredicate(left interface{}, right sqlparser.Expr, values map[string]interface{}) (bool, error) {
	tuple, ok := right.(sqlparser.ValTuple)
	if !ok {
		value, err := evaluateExpressionWithRow(right, values)
		if err != nil {
			return false, err
		}
		return compareScalarValues(left, value) == 0, nil
	}
	for _, expr := range tuple {
		value, err := evaluateExpressionWithRow(expr, values)
		if err != nil {
			return false, err
		}
		if compareScalarValues(left, value) == 0 {
			return true, nil
		}
	}
	return false, nil
}

func sqlLikePatternMatch(value, pattern string) bool {
	var b strings.Builder
	b.WriteString("(?i)^")
	for _, r := range pattern {
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	re, err := regexp.Compile(b.String())
	if err != nil {
		return false
	}
	return re.MatchString(value)
}

func evaluateExpressionWithRow(expr sqlparser.Expr, values map[string]interface{}) (interface{}, error) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		return (&StorageIntegratedDMLExecutor{}).parseSQLVal(v)
	case *sqlparser.NullVal:
		return nil, nil
	case sqlparser.BoolVal:
		return bool(v), nil
	case *sqlparser.ColName:
		name := strings.Trim(v.Name.String(), "` ")
		if value, exists := values[name]; exists {
			return value, nil
		}
		if value, exists := values[strings.ToLower(name)]; exists {
			return value, nil
		}
		return nil, fmt.Errorf("列 %s 不存在", name)
	case *sqlparser.FuncExpr:
		name := sqlparser.String(v)
		if value, exists := values[name]; exists {
			return value, nil
		}
		if value, exists := values[strings.ToLower(name)]; exists {
			return value, nil
		}
		return nil, fmt.Errorf("表达式 %s 不存在", name)
	case *sqlparser.ParenExpr:
		return evaluateExpressionWithRow(v.Expr, values)
	case *sqlparser.UnaryExpr:
		value, err := evaluateExpressionWithRow(v.Expr, values)
		if err != nil {
			return nil, err
		}
		if v.Operator == sqlparser.UMinusStr {
			if n, ok := toFloat64(value); ok {
				return -n, nil
			}
		}
		return value, nil
	case *sqlparser.BinaryExpr:
		left, err := evaluateExpressionWithRow(v.Left, values)
		if err != nil {
			return nil, err
		}
		right, err := evaluateExpressionWithRow(v.Right, values)
		if err != nil {
			return nil, err
		}
		leftNum, leftOK := toFloat64(left)
		rightNum, rightOK := toFloat64(right)
		if !leftOK || !rightOK {
			return nil, fmt.Errorf("二元表达式只支持数字: %s", sqlparser.String(expr))
		}
		switch v.Operator {
		case sqlparser.PlusStr:
			return normalizeNumericResult(leftNum + rightNum), nil
		case sqlparser.MinusStr:
			return normalizeNumericResult(leftNum - rightNum), nil
		case sqlparser.MultStr:
			return normalizeNumericResult(leftNum * rightNum), nil
		case sqlparser.DivStr:
			if rightNum == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return leftNum / rightNum, nil
		default:
			return nil, fmt.Errorf("不支持的二元操作符: %s", v.Operator)
		}
	default:
		return nil, fmt.Errorf("不支持的表达式类型: %T", expr)
	}
}

func normalizeNumericResult(v float64) interface{} {
	if v == float64(int64(v)) {
		return int64(v)
	}
	return v
}

func compareScalarValues(left, right interface{}) int {
	if leftNum, ok := toFloat64(left); ok {
		if rightNum, ok := toFloat64(right); ok {
			switch {
			case leftNum < rightNum:
				return -1
			case leftNum > rightNum:
				return 1
			default:
				return 0
			}
		}
	}
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)
	switch {
	case leftStr < rightStr:
		return -1
	case leftStr > rightStr:
		return 1
	default:
		return 0
	}
}

func toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case string:
		n, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		return n, err == nil
	case []byte:
		n, err := strconv.ParseFloat(strings.TrimSpace(string(v)), 64)
		return n, err == nil
	default:
		return 0, false
	}
}
