package engine

import "github.com/zhukovaskychina/xmysql-server/server/common"

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/vector"
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
	return dml.serializeRowDataWithContext(context.Background(), row, tableMeta)
}

func (dml *StorageIntegratedDMLExecutor) serializeRowDataWithContext(ctx context.Context, row *InsertRowData, tableMeta *metadata.TableMeta) ([]byte, error) {
	if tableMeta != nil {
		storageRow, err := dml.externalizeLargeContentValues(ctx, row, tableMeta)
		if err != nil {
			return nil, err
		}
		encoded, err := EncodeClusteredRecord(storageRow, tableMeta)
		if err != nil {
			dml.deleteCreatedContentObjects(storageRow.CreatedContentObjects)
			return nil, err
		}
		row.ColumnValues = storageRow.ColumnValues
		row.CreatedContentObjects = append(row.CreatedContentObjects, storageRow.CreatedContentObjects...)
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

func (dml *StorageIntegratedDMLExecutor) externalizeLargeContentValues(ctx context.Context, row *InsertRowData, tableMeta *metadata.TableMeta) (result *InsertRowData, resultErr error) {
	if row == nil || tableMeta == nil {
		return row, nil
	}
	storageRow := &InsertRowData{ColumnValues: make(map[string]interface{}, len(row.ColumnValues)), ColumnTypes: make(map[string]metadata.DataType, len(row.ColumnTypes))}
	defer func() {
		if resultErr != nil {
			dml.deleteCreatedContentObjects(storageRow.CreatedContentObjects)
		}
	}()
	for name, value := range row.ColumnValues {
		storageRow.ColumnValues[name] = value
	}
	for name, dataType := range row.ColumnTypes {
		storageRow.ColumnTypes[name] = dataType
	}
	var store *content.SegmentStore
	createdRefs := make(map[string]struct{})
	for _, column := range tableMeta.Columns {
		if !columnHasDataType(column, metadata.TypeContent) {
			continue
		}
		value, ok := lookupDMLRowValue(storageRow.ColumnValues, column.Name)
		if !ok || value == nil {
			continue
		}
		var typed content.Value
		switch current := value.(type) {
		case content.Value:
			typed = current
		case *content.Value:
			if current == nil {
				continue
			}
			typed = *current
		case []byte:
			var err error
			typed, err = content.NewValue("application/octet-stream", 1, current)
			if err != nil {
				return nil, fmt.Errorf("prepare CONTENT column %s: %w", column.Name, err)
			}
		case string:
			var err error
			typed, err = content.NewValue("application/octet-stream", 1, []byte(current))
			if err != nil {
				return nil, fmt.Errorf("prepare CONTENT column %s: %w", column.Name, err)
			}
		default:
			return nil, fmt.Errorf("unsupported CONTENT value type %T for column %s", value, column.Name)
		}
		if err := typed.Validate(); err != nil {
			return nil, fmt.Errorf("invalid CONTENT column %s: %w", column.Name, err)
		}
		if typed.ExternalRef != nil {
			stored := typed
			if typed.TransientRef {
				if _, exists := createdRefs[typed.ExternalRef.ID]; !exists {
					storageRow.CreatedContentObjects = append(storageRow.CreatedContentObjects, *typed.ExternalRef)
					createdRefs[typed.ExternalRef.ID] = struct{}{}
				}
				if sessionValues, ok := storageRow.ColumnValues[sessionExpressionValuesKey].(map[string]interface{}); ok {
					if adoptedRefs, ok := sessionValues[PreparedContentAdoptedRefsSessionParam].(map[string]bool); ok {
						adoptedRefs[typed.ExternalRef.ID] = true
					}
				}
				var err error
				stored, err = content.NewSegmentedValue(*typed.ExternalRef)
				if err != nil {
					return nil, fmt.Errorf("bind CONTENT column %s to stored object: %w", column.Name, err)
				}
				stored.Revision = typed.Revision
			}
			for existingName := range storageRow.ColumnValues {
				if strings.EqualFold(existingName, column.Name) && existingName != column.Name {
					delete(storageRow.ColumnValues, existingName)
				}
			}
			storageRow.ColumnValues[column.Name] = stored
			continue
		}
		if typed.Size() <= content.DefaultInlineThreshold || typed.MIMEType == content.BlockMIMEType {
			continue
		}
		if store == nil {
			var err error
			store, err = contentSegmentStoreForDataDir(dml.dataDir)
			if err != nil {
				return nil, fmt.Errorf("open database CONTENT store: %w", err)
			}
		}
		ref, err := store.Put(ctx, typed.MIMEType, typed.Revision, bytes.NewReader(typed.Bytes))
		if err != nil {
			return nil, fmt.Errorf("externalize CONTENT column %s: %w", column.Name, err)
		}
		stored, err := content.NewSegmentedValue(ref)
		if err != nil {
			_ = store.Delete(ref)
			return nil, fmt.Errorf("bind CONTENT column %s to stored object: %w", column.Name, err)
		}
		storageRow.CreatedContentObjects = append(storageRow.CreatedContentObjects, ref)
		for existingName := range storageRow.ColumnValues {
			if strings.EqualFold(existingName, column.Name) && existingName != column.Name {
				delete(storageRow.ColumnValues, existingName)
			}
		}
		storageRow.ColumnValues[column.Name] = stored
	}
	return storageRow, nil
}

func (dml *StorageIntegratedDMLExecutor) deleteCreatedContentObjects(refs []content.ObjectRef) {
	if len(refs) == 0 {
		return
	}
	store, err := contentSegmentStoreForDataDir(dml.dataDir)
	if err != nil {
		logger.Warnf("open CONTENT store to clean failed DML objects: %v", err)
		return
	}
	for _, ref := range refs {
		if err := store.Delete(ref); err != nil && !errors.Is(err, content.ErrObjectNotFound) {
			logger.Warnf("clean failed DML CONTENT object %s: %v", ref.ID, err)
		}
	}
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
	if ctx != nil {
		if shared, ok := ctx.Value(replicationStorageTransactionContextKey).(*StorageTransactionContext); ok && shared != nil {
			// Replication replay supplies one physical transaction for the
			// complete source transaction. Individual replayed statements must
			// not open a second storage transaction.
			return shared, nil
		}
		if shared, ok := ctx.Value(clientStorageTransactionContextKey).(*StorageTransactionContext); ok && shared != nil {
			// A normal client transaction supplies one physical transaction for
			// every DML statement until COMMIT or ROLLBACK.
			return shared, nil
		}
	}

	if dml.txManager == nil {
		return nil, fmt.Errorf("transaction manager is not initialized for storage-integrated DML")
	}

	// 创建事务上下文
	txnContext := &StorageTransactionContext{
		StartTime:     time.Now(),
		Status:        "ACTIVE",
		ModifiedPages: make(map[string]uint32),
		DataDir:       dml.dataDir,
	}

	// 从上下文中获取隔离级别，默认为可重复读
	isolationLevel := common.TRX_ISO_REPEATABLE_READ
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
	if ctx != nil {
		if session, ok := ctx.Value(storageTransactionSessionContextKey).(server.MySQLServerSession); ok && session != nil && sessionTransactionActive(session) {
			txnContext.Session = session
			// Replication replay owns its storage transaction through the
			// replication-specific context. Do not also register it as a client
			// transaction merely because replay uses autocommit=0; otherwise the
			// replication commit path can return early without committing/flushing
			// the shared storage transaction.
			if !sessionBoolParam(session, "replication_replay") {
				session.SetParamByName(clientStorageTransactionContextKey, txnContext)
			}
		}
	}

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
	if ctx != nil {
		if shared, ok := ctx.Value(replicationStorageTransactionContextKey).(*StorageTransactionContext); ok && shared != nil && txn == shared {
			// The replay coordinator commits the shared transaction once, after
			// all source statements have succeeded.
			return nil
		}
		if shared, ok := ctx.Value(clientStorageTransactionContextKey).(*StorageTransactionContext); ok && shared != nil && txn == shared {
			return nil
		}
		if session, ok := ctx.Value(storageTransactionSessionContextKey).(server.MySQLServerSession); ok && session != nil {
			if shared, ok := session.GetParamByName(clientStorageTransactionContextKey).(*StorageTransactionContext); ok && shared != nil && txn == shared {
				return nil
			}
		}
	}

	txnCtx, ok := txn.(*StorageTransactionContext)
	if !ok {
		return fmt.Errorf("无效的事务上下文")
	}
	if txnCtx.Session != nil && ctx != nil && !storageTransactionForceCommit(ctx) {
		if _, ok := ctx.Value(storageTransactionSessionContextKey).(server.MySQLServerSession); ok {
			if shared, ok := txnCtx.Session.GetParamByName(clientStorageTransactionContextKey).(*StorageTransactionContext); ok && shared == txnCtx {
				return nil
			}
		}
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

		// The storage commit is authoritative as soon as TransactionManager.Commit
		// returns. Publish the local durable commit marker before flushing dirty
		// pages so a crash cannot leave committed redo state looking like an
		// uncommitted transaction during recovery.
		txnCtx.Status = "COMMITTED"
		txnCtx.EndTime = time.Now()
		txnCtx.CommitSequence = txnCtx.RealTransaction.CommitLSN
		postCommit := txnCtx.AfterRealCommit
		txnCtx.AfterRealCommit = nil
		if postCommit != nil {
			if err := postCommit(txnCtx); err != nil {
				logger.Errorf("真实存储提交后的 durable barrier 失败: %v", err)
				txnCtx.PostCommitError = err
			}
		}
	}

	// 刷新所有修改的页面到磁盘
	var flushErr error
	if dml.bufferPoolManager != nil {
		for spacePageKey, pageNo := range txnCtx.ModifiedPages {
			parts := strings.Split(spacePageKey, ":")
			if len(parts) == 2 {
				if spaceID, err := strconv.ParseUint(parts[0], 10, 32); err == nil {
					err = dml.bufferPoolManager.FlushPage(uint32(spaceID), pageNo)
					if err != nil {
						logger.Debugf("  警告: 刷新页面失败: %v", err)
						if flushErr == nil {
							flushErr = fmt.Errorf("flush page %s: %w", spacePageKey, err)
						}
					} else {
						logger.Debugf(" 页面已刷新: SpaceID=%d, PageNo=%d", spaceID, pageNo)
					}
				}
			}
		}
	} else if len(txnCtx.ModifiedPages) > 0 {
		logger.Debugf("⚠️ bufferPoolManager 未初始化，跳过 %d 个脏页刷新", len(txnCtx.ModifiedPages))
	}

	if txnCtx.Status != "COMMITTED" {
		txnCtx.Status = "COMMITTED"
		txnCtx.EndTime = time.Now()
	}
	// The row changes now own these immutable objects. Keep the on-disk values
	// and drop only the transaction's temporary cleanup list.
	txnCtx.CreatedContentObjects = nil

	logger.Debugf(" 存储事务提交完成: TxnID=%d, ModifiedPages=%d, Duration=%v",
		txnCtx.TransactionID, len(txnCtx.ModifiedPages), time.Since(txnCtx.StartTime))
	if flushErr != nil && txnCtx.PostCommitError != nil {
		return fmt.Errorf("storage transaction committed but page flush failed: %v; post-commit barrier failed: %w", flushErr, txnCtx.PostCommitError)
	}
	if txnCtx.PostCommitError != nil {
		return fmt.Errorf("storage transaction committed but post-commit barrier failed: %w", txnCtx.PostCommitError)
	}
	if flushErr != nil {
		// The redo transaction is already committed. Preserve that state so a
		// caller can retry the publication boundary without attempting a second
		// TransactionManager.Commit, while still surfacing the durability error.
		return fmt.Errorf("storage transaction committed but page flush failed: %w", flushErr)
	}

	return nil
}

// rollbackStorageTransaction 回滚存储事务
func (dml *StorageIntegratedDMLExecutor) rollbackStorageTransaction(ctx context.Context, txn interface{}) error {
	logger.Debugf("🔄 回滚存储引擎事务")
	if ctx != nil {
		if shared, ok := ctx.Value(replicationStorageTransactionContextKey).(*StorageTransactionContext); ok && shared != nil && txn == shared {
			// A replay statement failure is resolved by the outer replication
			// rollback boundary, which owns the shared physical transaction.
			return nil
		}
		if shared, ok := ctx.Value(clientStorageTransactionContextKey).(*StorageTransactionContext); ok && shared != nil && txn == shared {
			return nil
		}
		if session, ok := ctx.Value(storageTransactionSessionContextKey).(server.MySQLServerSession); ok && session != nil {
			if shared, ok := session.GetParamByName(clientStorageTransactionContextKey).(*StorageTransactionContext); ok && shared != nil && txn == shared {
				return nil
			}
		}
	}

	txnCtx, ok := txn.(*StorageTransactionContext)
	if !ok {
		return fmt.Errorf("无效的事务上下文")
	}
	if txnCtx.Session != nil && ctx != nil && !storageTransactionForceCommit(ctx) {
		if _, ok := ctx.Value(storageTransactionSessionContextKey).(server.MySQLServerSession); ok {
			if shared, ok := txnCtx.Session.GetParamByName(clientStorageTransactionContextKey).(*StorageTransactionContext); ok && shared == txnCtx {
				return nil
			}
		}
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
	dml.deleteCreatedContentObjects(txnCtx.CreatedContentObjects)
	txnCtx.CreatedContentObjects = nil

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
	txnCtx.AfterRealCommit = nil
	txnCtx.PostCommitError = nil

	logger.Debugf(" 存储事务回滚完成: TxnID=%d, ModifiedPages=%d, Duration=%v",
		txnCtx.TransactionID, len(txnCtx.ModifiedPages), time.Since(txnCtx.StartTime))

	return nil
}

// StorageTransactionContext 存储事务上下文
type StorageTransactionContext struct {
	TransactionID         uint64
	CommitSequence        uint64 // durable WAL commit LSN, assigned only after the commit marker is flushed
	StartTime             time.Time
	EndTime               time.Time
	Status                string               // ACTIVE, COMMITTED, ROLLED_BACK
	ModifiedPages         map[string]uint32    // "spaceID:pageNo" -> pageNo
	RealTransaction       *manager.Transaction // 真实的事务对象（如果使用事务管理器）
	Session               server.MySQLServerSession
	DataDir               string
	CreatedContentObjects []content.ObjectRef
	// AfterRealCommit runs immediately after the real transaction manager has
	// accepted the commit, before dirty pages are flushed. It is the narrow
	// durable publication barrier used by client and replication transactions.
	AfterRealCommit func(*StorageTransactionContext) error
	PostCommitError error
}

func (dml *StorageIntegratedDMLExecutor) registerTransactionContentObjects(txn interface{}, refs []content.ObjectRef) {
	txnCtx, ok := txn.(*StorageTransactionContext)
	if !ok || txnCtx == nil || len(refs) == 0 {
		return
	}
	txnCtx.CreatedContentObjects = append(txnCtx.CreatedContentObjects, refs...)
}

func storageTransactionForceCommit(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	force, _ := ctx.Value(storageTransactionForceCommitContextKey).(bool)
	return force
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
	incomingValues ...map[string]interface{},
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
		if defaultExpr, isDefault := expr.Expr.(*sqlparser.Default); isDefault {
			columnName := expr.ColumnName
			if defaultExpr != nil && defaultExpr.ColName != "" {
				columnName = defaultExpr.ColName
			}
			var err error
			newValue, err = normalizedDefaultValueForColumn(tableMeta, columnName)
			if err != nil {
				return nil, err
			}
		} else if expr.Expr != nil {
			evalValues := updatedData.ColumnValues
			if len(incomingValues) > 0 && incomingValues[0] != nil {
				evalValues = cloneTransactionRow(updatedData.ColumnValues)
				evalValues[insertValuesContextKey] = incomingValues[0]
			}
			value, err := evaluateExpressionWithRow(expr.Expr, evalValues)
			if err != nil {
				return nil, err
			}
			newValue = value
		}
		if column := findColumnMeta(tableMeta, expr.ColumnName); column != nil && newValue != nil {
			switch metadata.DataType(strings.ToUpper(string(column.Type))) {
			case metadata.TypeContent:
				previous, _ := lookupDMLRowValue(updatedData.ColumnValues, expr.ColumnName)
				mimeType := "application/octet-stream"
				revision := uint64(1)
				switch oldValue := previous.(type) {
				case content.Value:
					mimeType = oldValue.MIMEType
					if oldValue.Revision >= revision {
						revision = oldValue.Revision + 1
					}
				case *content.Value:
					if oldValue != nil {
						mimeType = oldValue.MIMEType
						if oldValue.Revision >= revision {
							revision = oldValue.Revision + 1
						}
					}
				}
				var source []byte
				preservedExternalRef := false
				switch typed := newValue.(type) {
				case content.Value:
					if typed.ExternalRef != nil {
						// Preserve immutable object identity without copying bytes.
						// SQL assignment advances the row revision; internal restore
						// retains the saved revision.
						newValue = typed.Clone()
						if typed.TransientRef || expr.Expr != nil {
							updated := newValue.(content.Value)
							updated.Revision = revision
							newValue = updated
						}
						preservedExternalRef = true
						break
					}
					mimeType = typed.MIMEType
					source = typed.Bytes
				case *content.Value:
					if typed != nil {
						if typed.ExternalRef != nil {
							newValue = typed.Clone()
							if typed.TransientRef || expr.Expr != nil {
								updated := newValue.(content.Value)
								updated.Revision = revision
								newValue = updated
							}
							preservedExternalRef = true
							break
						}
						mimeType = typed.MIMEType
						source = typed.Bytes
					} else {
						return nil, fmt.Errorf("CONTENT update for column '%s' is nil", expr.ColumnName)
					}
				case []byte:
					source = typed
				case string:
					source = []byte(typed)
				default:
					return nil, fmt.Errorf("unsupported CONTENT value type %T for column '%s'", newValue, expr.ColumnName)
				}
				if !preservedExternalRef {
					updatedContent, err := content.NewValue(mimeType, revision, source)
					if err != nil {
						return nil, fmt.Errorf("update CONTENT column '%s': %w", expr.ColumnName, err)
					}
					newValue = updatedContent
				}
			case metadata.TypeVector:
				typed, ok := vectorArgument(newValue)
				if !ok {
					return nil, fmt.Errorf("VECTOR update for column '%s' requires a typed VECTOR value, got %T", expr.ColumnName, newValue)
				}
				if column.Length > 0 && typed.Dimension() != column.Length {
					return nil, fmt.Errorf("VECTOR update for column '%s' has dimension %d, want %d", expr.ColumnName, typed.Dimension(), column.Length)
				}
				if err := typed.Validate(); err != nil {
					return nil, err
				}
				newValue = typed
			case metadata.TypeTinyInt, metadata.TypeSmallInt, metadata.TypeMediumInt, metadata.TypeInt, metadata.TypeBigInt:
				parsed, ok, overflow := boundedIntegerColumnValue(newValue, metadata.DataType(strings.ToUpper(string(column.Type))), column.IsUnsigned)
				if ok {
					newValue = parsed
				} else if overflow {
					if !dml.ignoreMode && dml.strictMode {
						return nil, fmt.Errorf("Out of range value: '%v' for column '%s'", newValue, expr.ColumnName)
					}
					newValue = integerColumnRangeLimit(metadata.DataType(strings.ToUpper(string(column.Type))), column.IsUnsigned, newValue)
					dml.warnings = append(dml.warnings, Warning{Level: "Warning", Code: 1264, Message: fmt.Sprintf("Out of range value for column '%s'", expr.ColumnName)})
				} else if dml.ignoreMode {
					text := fmt.Sprint(newValue)
					newValue = int64(0)
					dml.warnings = append(dml.warnings, Warning{Level: "Warning", Code: 1366, Message: fmt.Sprintf("Incorrect integer value: '%s' for column '%s'", text, expr.ColumnName)})
				} else {
					return nil, fmt.Errorf("Incorrect integer value: '%v' for column '%s'", newValue, expr.ColumnName)
				}
			case metadata.TypeFloat, metadata.TypeDouble, metadata.TypeDecimal:
				parsed, ok, overflow, rounded := boundedDecimalColumnValue(newValue, column)
				if ok {
					newValue = parsed
					if rounded {
						dml.warnings = append(dml.warnings, Warning{Level: "Warning", Code: 1265, Message: fmt.Sprintf("Data truncated for column '%s'", expr.ColumnName)})
					}
				} else if overflow {
					if !dml.ignoreMode && dml.strictMode {
						return nil, fmt.Errorf("Out of range value: '%v' for column '%s'", newValue, expr.ColumnName)
					}
					newValue = decimalColumnRangeLimit(column)
					dml.warnings = append(dml.warnings, Warning{Level: "Warning", Code: 1264, Message: fmt.Sprintf("Out of range value for column '%s'", expr.ColumnName)})
				} else if dml.ignoreMode {
					text := fmt.Sprint(newValue)
					newValue = float64(0)
					dml.warnings = append(dml.warnings, Warning{Level: "Warning", Code: 1366, Message: fmt.Sprintf("Incorrect decimal value: '%s' for column '%s'", text, expr.ColumnName)})
				} else {
					return nil, fmt.Errorf("Incorrect decimal value: '%v' for column '%s'", newValue, expr.ColumnName)
				}
			case metadata.TypeDate, metadata.TypeTime, metadata.TypeDateTime, metadata.TypeTimestamp, metadata.TypeYear:
				dataType := metadata.DataType(strings.ToUpper(string(column.Type)))
				if parsed, ok := dml.zeroTemporalColumnValue(newValue, dataType); ok {
					newValue = parsed
				} else if parsed, ok := temporalColumnValue(newValue, dataType); ok {
					newValue = parsed
				} else if dml.ignoreMode {
					text := fmt.Sprint(newValue)
					newValue = zeroTemporalValue(dataType)
					dml.warnings = append(dml.warnings, Warning{Level: "Warning", Code: 1292, Message: fmt.Sprintf("Incorrect date value: '%s' for column '%s'", text, expr.ColumnName)})
				} else {
					return nil, fmt.Errorf("Incorrect date value: '%v' for column '%s'", newValue, expr.ColumnName)
				}
			case metadata.TypeEnum, metadata.TypeSet:
				dataType := metadata.DataType(strings.ToUpper(string(column.Type)))
				if parsed, ok := enumSetColumnValue(newValue, dataType, column.EnumValues); ok {
					newValue = parsed
				} else if dml.ignoreMode {
					if dataType == metadata.TypeSet {
						newValue = enumSetIgnoreValue(newValue, column.EnumValues)
					} else {
						newValue = ""
					}
					dml.warnings = append(dml.warnings, Warning{Level: "Warning", Code: 1265, Message: fmt.Sprintf("Data truncated for column '%s'", expr.ColumnName)})
				} else {
					return nil, fmt.Errorf("Invalid %s value: '%v' for column '%s'", dataType, newValue, expr.ColumnName)
				}
			}
		}
		updatedData.ColumnValues[expr.ColumnName] = newValue
		updatedData.ColumnTypes[expr.ColumnName] = expr.ColumnType
		logger.Debugf(" 更新列 %s: %v", expr.ColumnName, newValue)
	}
	if err := applyGeneratedColumnsToRow(updatedData.ColumnValues, tableMeta); err != nil {
		return nil, err
	}
	if err := dml.invalidateChangedSourceBlocks(existingData, updatedData, tableMeta); err != nil {
		return nil, err
	}

	return updatedData, nil
}

// invalidateChangedSourceBlocks marks same-row derived blocks stale when the
// exact CONTENT value they reference changes. The source binding must match
// the old row identity first, so editing an unrelated block row cannot stale a
// reference to a different table or primary key.
func (dml *StorageIntegratedDMLExecutor) invalidateChangedSourceBlocks(existingData, updatedData *InsertRowData, tableMeta *metadata.TableMeta) error {
	if dml == nil || existingData == nil || updatedData == nil || tableMeta == nil {
		return nil
	}
	oldPrimaryKey, oldKeyExists, err := contentSourcePrimaryKey(existingData.ColumnValues, tableMeta)
	if err != nil || !oldKeyExists {
		return err
	}
	newPrimaryKey, newKeyExists, err := contentSourcePrimaryKey(updatedData.ColumnValues, tableMeta)
	if err != nil {
		return err
	}
	for _, blockColumn := range tableMeta.Columns {
		if !columnHasDataType(blockColumn, metadata.TypeContent) {
			continue
		}
		blockValue, ok := dmlContentValue(updatedData.ColumnValues, blockColumn.Name)
		if !ok || blockValue.MIMEType != content.BlockMIMEType {
			continue
		}
		block, err := content.BlockFromValue(blockValue)
		if err != nil {
			return fmt.Errorf("decode derived CONTENT block in column %s: %w", blockColumn.Name, err)
		}
		if block.State == content.BlockStateStale {
			continue
		}
		oldSourceValue, ok := dmlContentValue(existingData.ColumnValues, block.Source.Column)
		if !ok {
			continue
		}
		oldSource, err := content.NewSourceRef(dml.schemaName, dml.tableName, block.Source.Column, oldPrimaryKey, oldSourceValue)
		if err != nil || !block.Source.Matches(oldSource) {
			continue
		}
		if newKeyExists {
			if newSourceValue, exists := dmlContentValue(updatedData.ColumnValues, block.Source.Column); exists {
				newSource, sourceErr := content.NewSourceRef(dml.schemaName, dml.tableName, block.Source.Column, newPrimaryKey, newSourceValue)
				if sourceErr == nil && block.Source.Matches(newSource) {
					continue
				}
			}
		}

		block.State = content.BlockStateStale
		encodedBlock, err := block.MarshalBinary()
		if err != nil {
			return fmt.Errorf("encode stale derived CONTENT block in column %s: %w", blockColumn.Name, err)
		}
		if blockValue.Revision == ^uint64(0) {
			return fmt.Errorf("derived CONTENT block revision overflow in column %s", blockColumn.Name)
		}
		staleValue, err := content.NewValue(content.BlockMIMEType, blockValue.Revision+1, encodedBlock)
		if err != nil {
			return fmt.Errorf("store stale derived CONTENT block in column %s: %w", blockColumn.Name, err)
		}
		updatedData.ColumnValues[blockColumn.Name] = staleValue
	}
	return nil
}

func dmlContentValue(row map[string]interface{}, column string) (content.Value, bool) {
	value, exists := lookupDMLRowValue(row, column)
	if !exists {
		return content.Value{}, false
	}
	switch typed := value.(type) {
	case content.Value:
		return typed, true
	case *content.Value:
		if typed != nil {
			return *typed, true
		}
	}
	return content.Value{}, false
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
			if ctx != nil {
				if session, ok := ctx.Value(storageTransactionSessionContextKey).(server.MySQLServerSession); ok && session != nil {
					rowData.ColumnValues[sessionExpressionValuesKey] = newSessionExpressionValues(session)
				}
			}

			if len(valTuple) != len(columnNames) {
				return nil, fmt.Errorf("列数量不匹配: 期望 %d，实际 %d", len(columnNames), len(valTuple))
			}

			for i, expr := range valTuple {
				columnName := columnNames[i]
				if insertExpressionNeedsSourceRowContext(expr) {
					rowData.pendingSourceExpressions = append(rowData.pendingSourceExpressions, pendingInsertSourceExpression{column: columnName, expr: expr})
					if column := findColumnMeta(tableMeta, columnName); column != nil {
						rowData.ColumnTypes[columnName] = column.Type
					} else {
						rowData.ColumnTypes[columnName] = metadata.TypeVarchar
					}
					continue
				}
				var value interface{}
				var err error
				if _, isDefault := expr.(*sqlparser.Default); isDefault {
					value, err = normalizedDefaultValueForColumn(tableMeta, columnName)
				} else {
					value, err = evaluateExpressionWithRow(expr, rowData.ColumnValues)
				}
				if err != nil {
					return nil, fmt.Errorf("计算表达式值失败: %v", err)
				}

				rowData.ColumnValues[columnName] = value
				if column := findColumnMeta(tableMeta, columnName); column != nil {
					rowData.ColumnTypes[columnName] = column.Type
				} else {
					rowData.ColumnTypes[columnName] = metadata.TypeVarchar
				}
			}
			insertRows = append(insertRows, rowData)
		}
	case *sqlparser.Select:
		return dml.parseInsertSelectData(ctx, valuesClause, tableMeta, schemaName, columnNames)
	case *sqlparser.Union:
		if dml.unionExecutor == nil {
			return nil, fmt.Errorf("INSERT SELECT UNION来源执行器未配置")
		}
		result, err := dml.unionExecutor(ctx, valuesClause, schemaName)
		if err != nil {
			return nil, fmt.Errorf("执行INSERT SELECT UNION来源查询失败: %w", err)
		}
		return dml.insertRowsFromSelectResult(result, tableMeta, columnNames)
	default:
		return nil, fmt.Errorf("不支持的INSERT语法: %T", stmt.Rows)
	}

	return insertRows, nil
}

func (dml *StorageIntegratedDMLExecutor) resolvePendingInsertSourceExpressions(rows []*InsertRowData, tableMeta *metadata.TableMeta, schemaName string) error {
	for _, row := range rows {
		if row == nil || len(row.pendingSourceExpressions) == 0 {
			continue
		}
		if err := dml.assignPendingInsertAutoIncrementPrimaryKey(row, tableMeta); err != nil {
			return err
		}
		if err := bindPendingInsertSourceContext(row, tableMeta, schemaName); err != nil {
			return err
		}
		for _, pending := range row.pendingSourceExpressions {
			value, err := evaluateExpressionWithRow(pending.expr, row.ColumnValues)
			if err != nil {
				delete(row.ColumnValues, contentSourceRowContextKey)
				return fmt.Errorf("计算来源绑定表达式 %s 失败: %w", pending.column, err)
			}
			row.ColumnValues[pending.column] = value
		}
		delete(row.ColumnValues, contentSourceRowContextKey)
		row.pendingSourceExpressions = nil
	}
	return nil
}

func (dml *StorageIntegratedDMLExecutor) assignPendingInsertAutoIncrementPrimaryKey(row *InsertRowData, tableMeta *metadata.TableMeta) error {
	if row == nil || tableMeta == nil {
		return fmt.Errorf("cannot assign source-bound INSERT primary key without row metadata")
	}
	for _, primaryKeyColumn := range effectivePrimaryKeyColumns(tableMeta) {
		column := findColumnMeta(tableMeta, primaryKeyColumn)
		if column == nil || !column.IsAutoIncrement {
			continue
		}
		if value, exists := resolveExpressionRowValue(row.ColumnValues, primaryKeyColumn); exists && value != nil {
			return nil
		}
		if _, err := dml.generatePrimaryKey(row, tableMeta); err != nil {
			return fmt.Errorf("assign auto-increment primary key for source-bound INSERT: %w", err)
		}
		if value, exists := resolveExpressionRowValue(row.ColumnValues, primaryKeyColumn); !exists || value == nil {
			return fmt.Errorf("auto-increment primary key %s was not assigned", primaryKeyColumn)
		}
		return nil
	}
	return nil
}

func insertExpressionNeedsSourceRowContext(expression sqlparser.Expr) bool {
	needsContext := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		function, ok := node.(*sqlparser.FuncExpr)
		if !ok {
			return true, nil
		}
		switch strings.ToUpper(strings.Trim(function.Name.String(), "` ")) {
		case "CONTENT_BLOCK_FROM_SOURCE", "VECTOR_FROM_SOURCE", "VECTOR_FROM_BLOCK":
			needsContext = true
			return false, nil
		default:
			return true, nil
		}
	}, expression)
	return needsContext
}

func bindPendingInsertSourceContext(row *InsertRowData, tableMeta *metadata.TableMeta, schemaName string) error {
	if row == nil || tableMeta == nil {
		return fmt.Errorf("cannot bind insert source context without a row and table metadata")
	}
	primaryKeyColumns := effectivePrimaryKeyColumns(tableMeta)
	if len(primaryKeyColumns) == 0 {
		return fmt.Errorf("source-bound INSERT expressions require an explicit, complete primary key")
	}
	var primaryKey []byte
	if len(primaryKeyColumns) == 1 {
		value, exists := resolveExpressionRowValue(row.ColumnValues, primaryKeyColumns[0])
		if !exists || value == nil {
			return fmt.Errorf("source-bound INSERT expressions require an explicit, complete primary key")
		}
		var ok bool
		primaryKey, ok = storageKeyToBytes(value)
		if !ok {
			return fmt.Errorf("source-bound INSERT primary key cannot be encoded")
		}
	} else {
		var err error
		primaryKey, err = buildCompositeKey(row.ColumnValues, primaryKeyColumns)
		if err != nil {
			return err
		}
	}
	if strings.TrimSpace(schemaName) == "" || strings.TrimSpace(tableMeta.Name) == "" {
		return fmt.Errorf("source-bound INSERT expressions require a resolved schema and table")
	}
	row.ColumnValues[contentSourceRowContextKey] = contentSourceRowContext{
		Schema: strings.TrimSpace(schemaName), Table: strings.TrimSpace(tableMeta.Name), PrimaryKey: append([]byte(nil), primaryKey...),
	}
	return nil
}

func (dml *StorageIntegratedDMLExecutor) insertRowsFromSelectResult(result *SelectResult, targetMeta *metadata.TableMeta, targetColumnNames []string) ([]*InsertRowData, error) {
	if result == nil {
		return nil, nil
	}
	insertRows := make([]*InsertRowData, 0, len(result.Records))
	for _, record := range result.Records {
		if record == nil {
			return nil, fmt.Errorf("INSERT SELECT来源返回空记录")
		}
		values := record.GetValues()
		if len(values) != len(targetColumnNames) {
			return nil, fmt.Errorf("列数量不匹配: 期望 %d，实际 %d", len(targetColumnNames), len(values))
		}
		rowData := &InsertRowData{ColumnValues: make(map[string]interface{}, len(values)), ColumnTypes: make(map[string]metadata.DataType, len(values))}
		for index, targetColumnName := range targetColumnNames {
			value := basicValueToInterface(values[index])
			if typedRecord, ok := record.(*EngineExecutorRecord); ok {
				if contentValue, exists := typedRecord.ContentValueByIndex(index); exists {
					value = contentValue
				} else if vectorValue, exists := typedRecord.VectorValueByIndex(index); exists {
					value = vectorValue
				}
			}
			rowData.ColumnValues[targetColumnName] = value
			if column := findColumnMeta(targetMeta, targetColumnName); column != nil {
				rowData.ColumnTypes[targetColumnName] = column.Type
			} else {
				rowData.ColumnTypes[targetColumnName] = metadata.TypeVarchar
			}
		}
		insertRows = append(insertRows, rowData)
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
	if dml.selectExecutor != nil && insertSelectNeedsGeneralExecutor(selectStmt) {
		return dml.parseInsertSelectWithExecutor(ctx, selectStmt, targetMeta, targetSchema, targetColumnNames)
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
	sourceBtreeManager, err := dml.createBTreeManagerForDML(ctx, sourceSchemaName, sourceTableName, sourceMeta)
	if err != nil {
		return nil, fmt.Errorf("创建INSERT SELECT来源表B+树管理器失败: %v", err)
	}

	whereConditions := dml.parseWhereConditions(selectStmt.Where)
	sourceRows, err := dml.scanRowsForTableConditions(ctx, sourceSchemaName, sourceTableName, whereConditions, sourceMeta, sourceStorageInfo, sourceBtreeManager)
	if err != nil {
		return nil, err
	}

	projectionExpressions, err := dml.selectProjectionExpressions(selectStmt, sourceMeta)
	if err != nil {
		return nil, err
	}
	if len(projectionExpressions) != len(targetColumnNames) {
		return nil, fmt.Errorf("列数量不匹配: 期望 %d，实际 %d", len(targetColumnNames), len(projectionExpressions))
	}

	insertRows := make([]*InsertRowData, 0, len(sourceRows))
	for _, sourceRow := range sourceRows {
		rowData := &InsertRowData{
			ColumnValues: make(map[string]interface{}),
			ColumnTypes:  make(map[string]metadata.DataType),
		}
		expressionValues := insertSelectExpressionValues(selectStmt, sourceTableName, sourceRow.OldValues)
		for i, projection := range projectionExpressions {
			targetColumnName := targetColumnNames[i]
			value, evalErr := evaluateExpressionWithRow(projection, expressionValues)
			if evalErr != nil {
				return nil, fmt.Errorf("计算INSERT SELECT投影表达式失败: %w", evalErr)
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

func insertSelectNeedsGeneralExecutor(selectStmt *sqlparser.Select) bool {
	if selectStmt == nil || len(selectStmt.From) != 1 {
		return true
	}
	// Native content/model projections need the SELECT executor's source-row
	// identity, segment-store context, authorization and statement budgets.
	// The simple DML projection evaluator does not carry that context.
	needsNativeContext := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if function, ok := node.(*sqlparser.FuncExpr); ok {
			name := strings.ToUpper(function.Name.String())
			if strings.HasPrefix(name, "CONTENT_") || strings.HasPrefix(name, "VECTOR_") || strings.HasPrefix(name, "AI_") {
				needsNativeContext = true
				return false, nil
			}
		}
		return true, nil
	}, selectStmt)
	if needsNativeContext {
		return true
	}
	aliased, simpleSource := selectStmt.From[0].(*sqlparser.AliasedTableExpr)
	if !simpleSource {
		return true
	}
	if _, tableSource := aliased.Expr.(sqlparser.TableName); !tableSource {
		return true
	}
	return len(selectStmt.GroupBy) > 0 || selectStmt.Having != nil || len(selectStmt.OrderBy) > 0 ||
		selectStmt.Limit != nil || strings.TrimSpace(selectStmt.Distinct) != "" || len(selectStmt.Windows) > 0
}

func (dml *StorageIntegratedDMLExecutor) parseInsertSelectWithExecutor(
	ctx context.Context,
	selectStmt *sqlparser.Select,
	targetMeta *metadata.TableMeta,
	targetSchema string,
	targetColumnNames []string,
) ([]*InsertRowData, error) {
	result, err := dml.selectExecutor(ctx, selectStmt, targetSchema)
	if err != nil {
		return nil, fmt.Errorf("执行INSERT SELECT来源查询失败: %w", err)
	}
	if result == nil {
		return nil, nil
	}
	return dml.insertRowsFromSelectResult(result, targetMeta, targetColumnNames)
}

func insertSelectExpressionValues(selectStmt *sqlparser.Select, sourceTableName string, rowValues map[string]interface{}) map[string]interface{} {
	values := cloneTransactionRow(rowValues)
	if selectStmt == nil || len(selectStmt.From) != 1 {
		return values
	}
	aliased, ok := selectStmt.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return values
	}
	qualifiers := []string{sourceTableName}
	if !aliased.As.IsEmpty() {
		qualifiers = append(qualifiers, aliased.As.String())
	}
	for column, value := range rowValues {
		for _, qualifier := range qualifiers {
			if strings.TrimSpace(qualifier) != "" {
				values[qualifier+"."+column] = value
			}
		}
	}
	return values
}

func (dml *StorageIntegratedDMLExecutor) selectProjectionExpressions(selectStmt *sqlparser.Select, sourceMeta *metadata.TableMeta) ([]sqlparser.Expr, error) {
	if len(selectStmt.SelectExprs) == 1 {
		if _, ok := selectStmt.SelectExprs[0].(*sqlparser.StarExpr); ok {
			expressions := make([]sqlparser.Expr, 0, len(sourceMeta.Columns))
			for _, col := range sourceMeta.Columns {
				if col != nil {
					expressions = append(expressions, &sqlparser.ColName{Name: sqlparser.NewColIdent(col.Name)})
				}
			}
			return expressions, nil
		}
	}

	projectionExpressions := make([]sqlparser.Expr, 0, len(selectStmt.SelectExprs))
	for _, expr := range selectStmt.SelectExprs {
		aliasedExpr, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("INSERT SELECT包含不支持的投影类型 %T", expr)
		}
		projectionExpressions = append(projectionExpressions, aliasedExpr.Expr)
	}
	return projectionExpressions, nil
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
		return decodeSQLHexLiteral(val.Val)
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
		if col := findColumnMeta(tableMeta, columnName); col != nil && col.IsGenerated {
			return nil, fmt.Errorf("generated column '%s' cannot be updated", columnName)
		}
		value, err := dml.evaluateExpression(expr.Expr)
		if err != nil {
			switch expr.Expr.(type) {
			case *sqlparser.BinaryExpr, *sqlparser.CaseExpr, *sqlparser.ColName, *sqlparser.FuncExpr, *sqlparser.ValuesFuncExpr, *sqlparser.Default, *sqlparser.ParenExpr, *sqlparser.ConvertExpr, *sqlparser.ConvertUsingExpr, *sqlparser.CollateExpr:
				// These expressions are evaluated against each existing row after
				// the UPDATE target set has been selected.
			default:
				return nil, fmt.Errorf("计算更新表达式值失败: %v", err)
			}
		}

		columnType := metadata.TypeVarchar
		if column := findColumnMeta(tableMeta, columnName); column != nil {
			columnType = column.Type
		}
		updateExpr := &UpdateExpression{
			ColumnName: columnName,
			NewValue:   value,
			ColumnType: columnType,
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
			if col.IsGenerated {
				if value, exists := row.ColumnValues[col.Name]; exists && value != nil {
					return fmt.Errorf("generated column '%s' cannot be assigned", col.Name)
				}
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
		if err := applyGeneratedColumnsToRow(row.ColumnValues, tableMeta); err != nil {
			return err
		}
	}

	return nil
}

func applyGeneratedColumnsToRow(values map[string]interface{}, tableMeta *metadata.TableMeta) error {
	if values == nil || tableMeta == nil {
		return nil
	}
	for _, col := range tableMeta.Columns {
		if col == nil || !col.IsGenerated || strings.TrimSpace(col.GeneratedExpression) == "" {
			continue
		}
		value, err := evaluateGeneratedExpression(col.GeneratedExpression, values)
		if err != nil {
			return fmt.Errorf("compute generated column '%s': %v", col.Name, err)
		}
		values[col.Name] = value
	}
	return nil
}

func evaluateGeneratedExpression(expression string, values map[string]interface{}) (interface{}, error) {
	stmt, err := sqlparser.Parse("select " + expression)
	if err != nil {
		return nil, err
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok || len(selectStmt.SelectExprs) != 1 {
		return nil, fmt.Errorf("generated expression is not a scalar expression")
	}
	aliased, ok := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("generated expression is not aliased expression")
	}
	return evaluateExpressionWithRow(aliased.Expr, values)
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
	case "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BIT":
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
	if dml != nil && !dml.uniqueChecks {
		return nil
	}
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
	uniqueSets := uniqueConstraintColumnSets(tableMeta, uniqueColumns)
	if len(uniqueSets) == 0 && len(tableMeta.PrimaryKey) == 0 {
		return nil
	}

	seen := make(map[string]struct{})
	for _, row := range insertRows {
		for _, colName := range uniqueColumns {
			value, exists := lookupDMLRowValue(row.ColumnValues, colName)
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
	for _, columns := range uniqueSets {
		if len(columns) <= 1 {
			continue
		}
		for leftIndex := 0; leftIndex < len(insertRows); leftIndex++ {
			for rightIndex := leftIndex + 1; rightIndex < len(insertRows); rightIndex++ {
				if uniqueColumnValuesConflict(insertRows[leftIndex].ColumnValues, insertRows[rightIndex].ColumnValues, columns) {
					return fmt.Errorf("Duplicate entry '%s' for key '%s'", formatCompositeKeyValues(insertRows[rightIndex].ColumnValues, columns), strings.Join(columns, ","))
				}
			}
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
			existing, exists := lookupDMLRowValue(rowInfo.OldValues, colName)
			if !exists || existing == nil {
				continue
			}
			for _, row := range insertRows {
				incoming, exists := lookupDMLRowValue(row.ColumnValues, colName)
				if !exists || incoming == nil {
					continue
				}
				if compareScalarValues(existing, incoming) == 0 {
					return fmt.Errorf("Duplicate entry '%v' for key '%s'", incoming, colName)
				}
			}
		}
		for _, row := range insertRows {
			for _, columns := range uniqueSets {
				if len(columns) > 1 && uniqueColumnValuesConflict(rowInfo.OldValues, row.ColumnValues, columns) {
					return fmt.Errorf("Duplicate entry '%s' for key '%s'", formatCompositeKeyValues(row.ColumnValues, columns), strings.Join(columns, ","))
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
	for _, columns := range uniqueConstraintColumnSets(tableMeta, uniqueColumns) {
		if uniqueColumnValuesConflict(existingValues, incomingValues, columns) {
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
	incomingByRow ...map[*RowUpdateInfo]map[string]interface{},
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
		var incoming map[string]interface{}
		if len(incomingByRow) > 0 {
			incoming = incomingByRow[0][rowInfo]
		}
		updatedRow, err := dml.applyUpdateExpressions(existingRowData, updateExprs, tableMeta, incoming)
		if err != nil {
			return fmt.Errorf("应用更新表达式失败: %v", err)
		}
		updatedRows = append(updatedRows, updatedRow)
		oldRows = append(oldRows, rowInfo.OldValues)
	}

	if err := dml.validateInsertData(updatedRows, tableMeta); err != nil {
		return err
	}
	if !dml.uniqueChecks {
		return nil
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
				existingValue, exists := lookupDMLRowValue(existing.OldValues, colName)
				if !exists || existingValue == nil {
					continue
				}
				incomingValue, exists := lookupDMLRowValue(updated.ColumnValues, colName)
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
			value, exists := lookupDMLRowValue(row.ColumnValues, colName)
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
	for _, columns := range uniqueConstraintColumnSets(tableMeta, nil) {
		if len(columns) <= 1 {
			continue
		}
		for leftIndex := 0; leftIndex < len(rows); leftIndex++ {
			for rightIndex := leftIndex + 1; rightIndex < len(rows); rightIndex++ {
				if uniqueColumnValuesConflict(rows[leftIndex].ColumnValues, rows[rightIndex].ColumnValues, columns) {
					return fmt.Errorf("Duplicate entry '%s' for key '%s'", formatCompositeKeyValues(rows[rightIndex].ColumnValues, columns), strings.Join(columns, ","))
				}
			}
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

func lookupDMLRowValue(values map[string]interface{}, columnName string) (interface{}, bool) {
	if values == nil {
		return nil, false
	}
	if value, ok := values[columnName]; ok {
		return value, true
	}
	for name, value := range values {
		if strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(columnName)) {
			return value, true
		}
	}
	return nil, false
}

func uniqueConstraintColumnSets(tableMeta *metadata.TableMeta, fallback []string) [][]string {
	sets := make([][]string, 0)
	seen := make(map[string]struct{})
	add := func(columns []string) {
		if len(columns) == 0 {
			return
		}
		normalized := make([]string, len(columns))
		for i, column := range columns {
			normalized[i] = strings.ToLower(strings.TrimSpace(column))
		}
		key := strings.Join(normalized, "\x00")
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		sets = append(sets, append([]string(nil), columns...))
	}
	for _, column := range fallback {
		add([]string{column})
	}
	if tableMeta == nil {
		return sets
	}
	for _, index := range tableMeta.Indices {
		if index.Unique && !strings.EqualFold(strings.TrimSpace(index.Name), "PRIMARY") {
			add(index.Columns)
		}
	}
	for _, column := range tableMeta.Columns {
		if column != nil && column.IsUnique && !isCompositePrimaryKeyColumn(column.Name, tableMeta.PrimaryKey) {
			add([]string{column.Name})
		}
	}
	return sets
}

func uniqueColumnValuesConflict(left, right map[string]interface{}, columns []string) bool {
	if len(columns) == 0 {
		return false
	}
	for _, column := range columns {
		leftValue, leftOK := lookupDMLRowValue(left, column)
		rightValue, rightOK := lookupDMLRowValue(right, column)
		// MySQL UNIQUE indexes allow multiple rows with NULL in any indexed
		// column, so such a pair is never a duplicate-key conflict.
		if !leftOK || !rightOK || leftValue == nil || rightValue == nil {
			return false
		}
		if compareScalarValues(leftValue, rightValue) != 0 {
			return false
		}
	}
	return true
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
		value, exists := resolveExpressionRowValue(row, columnName)
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

func normalizedDefaultValueForColumn(tableMeta *metadata.TableMeta, columnName string) (interface{}, error) {
	column := findColumnMeta(tableMeta, strings.Trim(columnName, "` "))
	if column == nil {
		return nil, fmt.Errorf("column %s not found for DEFAULT expression", columnName)
	}
	if column.DefaultValue == nil {
		return nil, nil
	}
	return normalizeDefaultValue(column.DefaultValue, column.Type), nil
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
		value, _ := lookupDMLRowValue(row, col)
		values = append(values, fmt.Sprintf("%v", value))
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
		matched, err := evalPredicate(selectStmt.Where.Expr, values)
		if err != nil || !matched {
			return matched, err
		}
	}
	return true, nil
}

func evalPredicate(expr sqlparser.Expr, values map[string]interface{}) (bool, error) {
	truth, err := evalPredicateTruth(expr, values)
	return truth == sqlTruthTrue, err
}

const (
	sqlTruthFalse   = 0
	sqlTruthTrue    = 1
	sqlTruthUnknown = -1
)

// insertValuesContextKey carries the candidate INSERT row while evaluating
// ON DUPLICATE KEY UPDATE. It is deliberately kept out of ordinary column
// namespaces so VALUES(col) cannot accidentally resolve to a stored column.
const insertValuesContextKey = "__xmysql_insert_values"

type sqlIntervalValue struct {
	amount interface{}
	unit   string
}

// evalPredicateTruth evaluates a predicate using SQL's three-valued logic.
// The public WHERE contract remains bool (only TRUE selects a row), but the
// intermediate UNKNOWN state must survive NOT/AND/OR and comparisons.
func evalPredicateTruth(expr sqlparser.Expr, values map[string]interface{}) (int, error) {
	switch v := expr.(type) {
	case *sqlparser.AndExpr:
		left, err := evalPredicateTruth(v.Left, values)
		if err != nil || left == sqlTruthFalse {
			return left, err
		}
		right, err := evalPredicateTruth(v.Right, values)
		if err != nil {
			return sqlTruthFalse, err
		}
		if left == sqlTruthUnknown && right == sqlTruthFalse {
			return sqlTruthFalse, nil
		}
		if left == sqlTruthUnknown || right == sqlTruthUnknown {
			return sqlTruthUnknown, nil
		}
		return right, nil
	case *sqlparser.OrExpr:
		left, err := evalPredicateTruth(v.Left, values)
		if err != nil || left == sqlTruthTrue {
			return left, err
		}
		right, err := evalPredicateTruth(v.Right, values)
		if err != nil {
			return sqlTruthFalse, err
		}
		if left == sqlTruthUnknown && right == sqlTruthTrue {
			return sqlTruthTrue, nil
		}
		if left == sqlTruthUnknown || right == sqlTruthUnknown {
			return sqlTruthUnknown, nil
		}
		return right, nil
	case *sqlparser.ParenExpr:
		return evalPredicateTruth(v.Expr, values)
	case *sqlparser.NotExpr:
		truth, err := evalPredicateTruth(v.Expr, values)
		if err != nil {
			return sqlTruthFalse, err
		}
		if truth == sqlTruthUnknown {
			return sqlTruthUnknown, nil
		}
		if truth == sqlTruthTrue {
			return sqlTruthFalse, nil
		}
		return sqlTruthTrue, nil
	case *sqlparser.IsExpr:
		value, err := evaluateExpressionWithRow(v.Expr, values)
		if err != nil {
			return sqlTruthFalse, err
		}
		isNull := value == nil
		switch strings.ToLower(strings.TrimSpace(v.Operator)) {
		case sqlparser.IsNullStr:
			return truthValue(isNull), nil
		case sqlparser.IsNotNullStr:
			return truthValue(!isNull), nil
		case sqlparser.IsTrueStr:
			return truthValue(sqlTruthValue(value) == sqlTruthTrue), nil
		case sqlparser.IsFalseStr:
			return truthValue(sqlTruthValue(value) == sqlTruthFalse), nil
		case sqlparser.IsNotTrueStr:
			return truthValue(sqlTruthValue(value) != sqlTruthTrue), nil
		case sqlparser.IsNotFalseStr:
			return truthValue(sqlTruthValue(value) != sqlTruthFalse), nil
		default:
			return sqlTruthFalse, fmt.Errorf("不支持的IS操作符: %s", v.Operator)
		}
	case *sqlparser.RangeCond:
		left, err := evaluateExpressionWithRow(v.Left, values)
		if err != nil {
			return sqlTruthFalse, err
		}
		from, err := evaluateExpressionWithRow(v.From, values)
		if err != nil {
			return sqlTruthFalse, err
		}
		to, err := evaluateExpressionWithRow(v.To, values)
		if err != nil {
			return sqlTruthFalse, err
		}
		lowerCollation, err := resolveComparisonCollation(v.Left, v.From, values, "BETWEEN")
		if err != nil {
			return sqlTruthFalse, err
		}
		upperCollation, err := resolveComparisonCollation(v.Left, v.To, values, "BETWEEN")
		if err != nil {
			return sqlTruthFalse, err
		}
		if left == nil || from == nil || to == nil {
			return sqlTruthUnknown, nil
		}
		matched := compareScalarValuesWithCollation(left, from, lowerCollation) >= 0 && compareScalarValuesWithCollation(left, to, upperCollation) <= 0
		if v.Operator == sqlparser.NotBetweenStr {
			return truthValue(!matched), nil
		}
		return truthValue(matched), nil
	case *sqlparser.ComparisonExpr:
		left, err := evaluateExpressionWithRow(v.Left, values)
		if err != nil {
			return sqlTruthFalse, err
		}
		collation, err := resolveComparisonCollation(v.Left, v.Right, values, v.Operator)
		if err != nil {
			return sqlTruthFalse, err
		}
		switch v.Operator {
		case sqlparser.InStr, sqlparser.NotInStr:
			matched, unknown, err := evalInPredicateWithCollation(left, v.Right, values, collation)
			if err != nil {
				return sqlTruthFalse, err
			}
			if unknown {
				return sqlTruthUnknown, nil
			}
			if v.Operator == sqlparser.NotInStr {
				return truthValue(!matched), nil
			}
			return truthValue(matched), nil
		case sqlparser.LikeStr, sqlparser.NotLikeStr:
			right, err := evaluateExpressionWithRow(v.Right, values)
			if err != nil {
				return sqlTruthFalse, err
			}
			if left == nil || right == nil {
				return sqlTruthUnknown, nil
			}
			matched, err := sqlLikePatternMatchWithEscapeAndCollation(fmt.Sprintf("%v", left), fmt.Sprintf("%v", right), v.Escape, values, collation)
			if err != nil {
				return sqlTruthFalse, err
			}
			if v.Operator == sqlparser.NotLikeStr {
				return truthValue(!matched), nil
			}
			return truthValue(matched), nil
		case sqlparser.RegexpStr, sqlparser.NotRegexpStr:
			right, err := evaluateExpressionWithRow(v.Right, values)
			if err != nil {
				return sqlTruthFalse, err
			}
			if left == nil || right == nil {
				return sqlTruthUnknown, nil
			}
			matched, err := sqlRegexpMatchWithCollation(fmt.Sprint(right), fmt.Sprint(left), collation)
			if err != nil {
				return sqlTruthFalse, err
			}
			if v.Operator == sqlparser.NotRegexpStr {
				matched = !matched
			}
			return truthValue(matched), nil
		}
		right, err := evaluateExpressionWithRow(v.Right, values)
		if err != nil {
			return sqlTruthFalse, err
		}
		if v.Operator == sqlparser.NullSafeEqualStr {
			if left == nil || right == nil {
				return truthValue(left == nil && right == nil), nil
			}
			return truthValue(compareScalarValuesWithCollation(left, right, collation) == 0), nil
		}
		// A comparison involving NULL is UNKNOWN and therefore does not
		// satisfy a WHERE/EXISTS predicate. Do not let compareScalarValues
		// collapse NULL = NULL into a Go-style equality.
		if left == nil || right == nil {
			return sqlTruthUnknown, nil
		}
		cmp := compareScalarValuesWithCollation(left, right, collation)
		switch v.Operator {
		case sqlparser.EqualStr:
			return truthValue(cmp == 0), nil
		case sqlparser.NotEqualStr:
			return truthValue(cmp != 0), nil
		case sqlparser.LessThanStr:
			return truthValue(cmp < 0), nil
		case sqlparser.LessEqualStr:
			return truthValue(cmp <= 0), nil
		case sqlparser.GreaterThanStr:
			return truthValue(cmp > 0), nil
		case sqlparser.GreaterEqualStr:
			return truthValue(cmp >= 0), nil
		default:
			return sqlTruthFalse, fmt.Errorf("不支持的比较操作符: %s", v.Operator)
		}
	default:
		value, err := evaluateExpressionWithRow(expr, values)
		if err != nil {
			return sqlTruthFalse, err
		}
		return sqlTruthValue(value), nil
	}
}

func truthValue(value bool) int {
	if value {
		return sqlTruthTrue
	}
	return sqlTruthFalse
}

func evalInPredicate(left interface{}, right sqlparser.Expr, values map[string]interface{}) (bool, bool, error) {
	return evalInPredicateWithCollation(left, right, values, "")
}

func evalInPredicateWithCollation(left interface{}, right sqlparser.Expr, values map[string]interface{}, collation string) (bool, bool, error) {
	if left == nil {
		return false, true, nil
	}
	tuple, ok := right.(sqlparser.ValTuple)
	if !ok {
		value, err := evaluateExpressionWithRow(right, values)
		if err != nil {
			return false, false, err
		}
		if value == nil {
			return false, true, nil
		}
		return compareScalarValuesWithCollation(left, value, collation) == 0, false, nil
	}
	hasNull := false
	for _, expr := range tuple {
		value, err := evaluateExpressionWithRow(expr, values)
		if err != nil {
			return false, false, err
		}
		if value == nil {
			hasNull = true
			continue
		}
		if compareScalarValuesWithCollation(left, value, collation) == 0 {
			return true, false, nil
		}
	}
	return false, hasNull, nil
}

// sqlTruthValue returns SQL's three truth states as 1=true, 0=false and -1=unknown.
func sqlTruthValue(value interface{}) int {
	if value == nil {
		return -1
	}
	if n, ok := toFloat64(value); ok {
		if n == 0 {
			return 0
		}
		return 1
	}
	if strings.EqualFold(strings.TrimSpace(fmt.Sprintf("%v", value)), "true") {
		return 1
	}
	return 0
}

// regexp-value: retained. LIKE and ESCAPE are value patterns.
func sqlLikePatternMatch(value, pattern string) bool {
	matched, _ := sqlLikePatternMatchWithEscape(value, pattern, nil, nil)
	return matched
}

func sqlLikePatternMatchWithEscape(value, pattern string, escapeExpr sqlparser.Expr, values map[string]interface{}) (bool, error) {
	return sqlLikePatternMatchWithEscapeAndCollation(value, pattern, escapeExpr, values, "")
}

func sqlLikePatternMatchWithEscapeAndCollation(value, pattern string, escapeExpr sqlparser.Expr, values map[string]interface{}, collationName string) (bool, error) {
	escape := "\\"
	if escapeExpr != nil {
		escapeValue, err := evaluateExpressionWithRow(escapeExpr, values)
		if err != nil {
			return false, err
		}
		if escapeValue == nil {
			return false, fmt.Errorf("LIKE ESCAPE cannot be NULL")
		}
		escape = fmt.Sprintf("%v", escapeValue)
	}
	escapeRunes := []rune(escape)
	if len(escapeRunes) != 1 {
		return false, fmt.Errorf("LIKE ESCAPE must be a single character")
	}
	escapeRune := escapeRunes[0]

	caseInsensitive := true // Preserve the legacy fallback when no collation is available.
	accentInsensitive := false
	if collation, found := common.CollationMetadataByName(collationName); found {
		lowerName := strings.ToLower(collation.Name)
		caseInsensitive = !strings.EqualFold(collation.Charset, "binary") && strings.HasSuffix(lowerName, "_ci")
		accentInsensitive = !strings.EqualFold(collation.Charset, "binary") && strings.Contains(lowerName, "_ai_")
		if accentInsensitive {
			value = removeCollationDiacritics(value)
		}
	}
	var b strings.Builder
	b.WriteString("(?s)") // SQL LIKE wildcards also match line terminators.
	if caseInsensitive {
		b.WriteString("(?i)")
	}
	b.WriteString("^")
	escaped := false
	for _, r := range pattern {
		if escaped {
			literal := string(r)
			if accentInsensitive {
				literal = removeCollationDiacritics(literal)
			}
			b.WriteString(regexp.QuoteMeta(literal))
			escaped = false
			continue
		}
		if r == escapeRune {
			escaped = true
			continue
		}
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		default:
			literal := string(r)
			if accentInsensitive {
				literal = removeCollationDiacritics(literal)
			}
			b.WriteString(regexp.QuoteMeta(literal))
		}
	}
	if escaped {
		literal := string(escapeRune)
		if accentInsensitive {
			literal = removeCollationDiacritics(literal)
		}
		b.WriteString(regexp.QuoteMeta(literal))
	}
	b.WriteString("$")
	re, err := regexp.Compile(b.String())
	if err != nil {
		return false, err
	}
	return re.MatchString(value), nil
}

func preparedContentParam(values map[string]interface{}, index interface{}) (content.Value, error) {
	var paramID uint64
	switch typed := index.(type) {
	case int:
		if typed < 0 {
			return content.Value{}, fmt.Errorf("CONTENT_PARAM index must be non-negative")
		}
		paramID = uint64(typed)
	case int64:
		if typed < 0 {
			return content.Value{}, fmt.Errorf("CONTENT_PARAM index must be non-negative")
		}
		paramID = uint64(typed)
	case uint64:
		paramID = typed
	default:
		return content.Value{}, fmt.Errorf("CONTENT_PARAM index must be an integer; got %T", index)
	}
	if paramID > uint64(^uint16(0)) {
		return content.Value{}, fmt.Errorf("CONTENT_PARAM index is out of range")
	}
	sessionValues := values
	if nested, ok := values[sessionExpressionValuesKey].(map[string]interface{}); ok {
		sessionValues = nested
	}
	params, ok := sessionValues[PreparedContentParamsSessionParam].(map[uint16]content.Value)
	if !ok {
		return content.Value{}, fmt.Errorf("CONTENT_PARAM is only valid for the active prepared statement execution")
	}
	value, ok := params[uint16(paramID)]
	if !ok {
		return content.Value{}, fmt.Errorf("CONTENT_PARAM %d is not bound for this execution", paramID)
	}
	return value, nil
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
		if !v.Qualifier.IsEmpty() {
			qualified := strings.Trim(sqlparser.String(v.Qualifier), "` ") + "." + name
			if value, exists := resolveExpressionRowValue(values, qualified); exists {
				return value, nil
			}
		}
		if value, exists := resolveExpressionRowValue(values, name); exists {
			return value, nil
		}
		return nil, fmt.Errorf("列 %s 不存在", name)
	case *sqlparser.ValuesFuncExpr:
		if v == nil || v.Name == nil {
			return nil, fmt.Errorf("VALUES() function requires column name")
		}
		incoming, ok := values[insertValuesContextKey].(map[string]interface{})
		if !ok || incoming == nil {
			return nil, fmt.Errorf("VALUES() is only valid in an INSERT ... ON DUPLICATE KEY UPDATE expression")
		}
		columnName := strings.Trim(v.Name.Name.String(), "` ")
		if value, exists := incoming[columnName]; exists {
			return value, nil
		}
		for name, value := range incoming {
			if strings.EqualFold(strings.Trim(name, "` "), columnName) {
				return value, nil
			}
		}
		return nil, fmt.Errorf("column %s not found in INSERT values", columnName)
	case *sqlparser.FuncExpr:
		functionName := strings.ToUpper(strings.Trim(v.Name.String(), "` "))
		if functionName == "RRF_SCORE" {
			return evaluateReciprocalRankFusion(v, values)
		}
		if functionName == "CONTENT_PARAM" {
			if len(v.Exprs) != 1 {
				return nil, fmt.Errorf("CONTENT_PARAM requires one parameter index")
			}
			argument, ok := v.Exprs[0].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("CONTENT_PARAM index must be a scalar expression")
			}
			index, err := evaluateExpressionWithRow(argument.Expr, values)
			if err != nil {
				return nil, err
			}
			value, err := preparedContentParam(values, index)
			if err != nil {
				return nil, err
			}
			return value, nil
		}
		if functionName == "CONTENT_BLOCK_CONTEXT" {
			return evaluateContentBlockContext(v, values)
		}
		if functionName == "CONTENT_BLOCK_SOURCE_SLICE" {
			return evaluateContentBlockSourceSlice(v, values)
		}
		if functionName == "CONTENT_PARSE_CSV" {
			return evaluateContentParseCSV(v, values)
		}
		if functionName == "CONTENT_PARSE_JSON_ARRAY" {
			return evaluateContentParseJSONArray(v, values)
		}
		if functionName == "CONTENT_PARSE_IMAGE" {
			return evaluateContentParseImage(v, values)
		}
		if functionName == "CONTENT_PARSE_AUDIO" {
			return evaluateContentParseAudio(v, values)
		}
		if functionName == "CONTENT_PARSE_VIDEO" {
			return evaluateContentParseVideo(v, values)
		}
		if functionName == "CONTENT_BLOCK_RETRIEVAL_TEXT" {
			if len(v.Exprs) != 1 {
				return nil, fmt.Errorf("CONTENT_BLOCK_RETRIEVAL_TEXT requires exactly one CONTENT_BLOCK")
			}
			block, isNull, err := evaluateContentBlockArgument(v, values)
			if err != nil || isNull {
				return nil, err
			}
			text, available, err := content.BlockRetrievalText(block)
			if err != nil || !available {
				return nil, err
			}
			return text, nil
		}
		if functionName == "CONTENT_BLOCK_MATCHES_SOURCE" || functionName == "CONTENT_BLOCK_VERIFIED_TEXT" {
			if len(v.Exprs) != 2 {
				return nil, fmt.Errorf("%s requires a CONTENT_BLOCK and a source CONTENT column", functionName)
			}
			blockFunction := *v
			blockFunction.Exprs = v.Exprs[:1]
			block, isNull, err := evaluateContentBlockArgument(&blockFunction, values)
			if err != nil {
				return nil, err
			}
			if isNull {
				return nil, nil
			}
			sourceColumnExpr, ok := v.Exprs[1].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("%s source must be a CONTENT column", functionName)
			}
			sourceColumn, ok := sourceColumnExpr.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, fmt.Errorf("%s source must be a CONTENT column", functionName)
			}
			sourceValue, err := evaluateExpressionWithRow(sourceColumn, values)
			if err != nil {
				return nil, err
			}
			if sourceValue == nil {
				if functionName == "CONTENT_BLOCK_VERIFIED_TEXT" {
					return nil, nil
				}
				return false, nil
			}
			rowContext, ok := contentSourceContextForColumn(values, sourceColumn)
			if !ok || len(rowContext.PrimaryKey) == 0 {
				return nil, fmt.Errorf("%s requires stored-row context for the source column", functionName)
			}
			var typedSource content.Value
			switch typed := sourceValue.(type) {
			case content.Value:
				typedSource = typed
			case *content.Value:
				if typed == nil {
					return false, nil
				}
				typedSource = *typed
			default:
				return nil, fmt.Errorf("%s requires a typed CONTENT column; got %T", functionName, sourceValue)
			}
			actualSource, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, sourceColumn.Name.String(), rowContext.PrimaryKey, typedSource)
			if err != nil {
				return nil, fmt.Errorf("%s current source is invalid: %w", functionName, err)
			}
			matches := block.State == content.BlockStateReady && block.Source.Matches(actualSource)
			if functionName == "CONTENT_BLOCK_VERIFIED_TEXT" {
				// Matching source identity/version alone does not prove that a
				// caller-supplied block's text was extracted from that source.
				// This prototype only releases text when its byte locator points
				// to the exact same UTF-8 bytes in the current source value.
				if !matches || block.Kind != content.BlockText || block.Locator.ByteStart == nil || block.Locator.ByteEnd == nil || !utf8.ValidString(block.Text) {
					return nil, nil
				}
				if *block.Locator.ByteEnd < *block.Locator.ByteStart || *block.Locator.ByteEnd-*block.Locator.ByteStart > content.MaxReadRangeBytes {
					return nil, nil
				}
				store, err := contentSegmentStoreForValue(values, typedSource)
				if err != nil {
					return nil, err
				}
				sourceRange, err := typedSource.ReadRangeFromStore(storedFunctionContext(values), store, *block.Locator.ByteStart, *block.Locator.ByteEnd-*block.Locator.ByteStart)
				if err != nil {
					return nil, fmt.Errorf("%s source range read failed: %w", functionName, err)
				}
				if string(sourceRange) != block.Text {
					return nil, nil
				}
				return block.Text, nil
			}
			return matches, nil
		}
		if functionName == "CONTENT_BLOCK_FROM_SOURCE" {
			if len(v.Exprs) != 2 {
				return nil, fmt.Errorf("CONTENT_BLOCK_FROM_SOURCE requires a JSON definition and a source CONTENT column")
			}
			definition, ok := v.Exprs[0].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("CONTENT_BLOCK_FROM_SOURCE JSON definition must be a scalar expression")
			}
			rawDefinition, err := evaluateExpressionWithRow(definition.Expr, values)
			if err != nil {
				return nil, err
			}
			if rawDefinition == nil {
				return nil, nil
			}
			var definitionBytes []byte
			switch typed := rawDefinition.(type) {
			case []byte:
				definitionBytes = append([]byte(nil), typed...)
			case string:
				definitionBytes = []byte(typed)
			default:
				return nil, fmt.Errorf("CONTENT_BLOCK_FROM_SOURCE definition must be JSON text or bytes; got %T", rawDefinition)
			}
			sourceColumnExpr, ok := v.Exprs[1].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("CONTENT_BLOCK_FROM_SOURCE source must be a CONTENT column")
			}
			sourceColumn, ok := sourceColumnExpr.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, fmt.Errorf("CONTENT_BLOCK_FROM_SOURCE source must be a CONTENT column")
			}
			sourceValue, err := evaluateExpressionWithRow(sourceColumn, values)
			if err != nil {
				return nil, err
			}
			if sourceValue == nil {
				return nil, nil
			}
			rowContext, ok := contentSourceContextForColumn(values, sourceColumn)
			if !ok || len(rowContext.PrimaryKey) == 0 {
				return nil, fmt.Errorf("CONTENT_BLOCK_FROM_SOURCE requires stored-row context for the source column")
			}
			var typedSource content.Value
			switch typed := sourceValue.(type) {
			case content.Value:
				typedSource = typed
			case *content.Value:
				if typed == nil {
					return nil, nil
				}
				typedSource = *typed
			default:
				return nil, fmt.Errorf("CONTENT_BLOCK_FROM_SOURCE requires a typed CONTENT column; got %T", sourceValue)
			}
			source, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, sourceColumn.Name.String(), rowContext.PrimaryKey, typedSource)
			if err != nil {
				return nil, fmt.Errorf("CONTENT_BLOCK_FROM_SOURCE source verification failed: %w", err)
			}
			block, err := content.BlockFromSourceJSON(definitionBytes, source)
			if err != nil {
				return nil, err
			}
			return content.BlockValue(block)
		}
		if functionName == "CONTENT_TEXT_BLOCK" {
			return evaluateContentTextBlock(v, values)
		}
		if functionName == "CONTENT_TEXT_CHUNK" || functionName == "CONTENT_TEXT_CHUNK_COUNT" {
			return evaluateContentTextChunkSQL(v, values, functionName == "CONTENT_TEXT_CHUNK_COUNT")
		}
		if functionName == "CONTENT_MAINTENANCE_STATUS" {
			return evaluateContentMaintenanceStatusSQL(v, values)
		}
		if functionName == "CONTENT_BLOCK_FROM_JSON" {
			if len(v.Exprs) != 1 {
				return nil, fmt.Errorf("CONTENT_BLOCK_FROM_JSON requires exactly one JSON argument")
			}
			argument, ok := v.Exprs[0].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("CONTENT_BLOCK_FROM_JSON argument must be a scalar expression")
			}
			value, err := evaluateExpressionWithRow(argument.Expr, values)
			if err != nil {
				return nil, err
			}
			if value == nil {
				return nil, nil
			}
			var input []byte
			switch typed := value.(type) {
			case []byte:
				input = append([]byte(nil), typed...)
			case string:
				input = []byte(typed)
			default:
				return nil, fmt.Errorf("CONTENT_BLOCK_FROM_JSON requires a JSON string or binary parameter; got %T", value)
			}
			block, err := content.BlockFromJSON(input)
			if err != nil {
				return nil, err
			}
			return content.BlockValue(block)
		}
		if functionName == "VECTOR_FROM_ARRAY" {
			if len(v.Exprs) < 5 {
				return nil, fmt.Errorf("VECTOR_FROM_ARRAY requires model ID, model version, shared space ID, source modality and at least one element")
			}
			arguments := make([]interface{}, 0, len(v.Exprs))
			for _, rawArgument := range v.Exprs {
				argument, ok := rawArgument.(*sqlparser.AliasedExpr)
				if !ok {
					return nil, fmt.Errorf("VECTOR_FROM_ARRAY arguments must be scalar expressions")
				}
				value, err := evaluateExpressionWithRow(argument.Expr, values)
				if err != nil {
					return nil, err
				}
				arguments = append(arguments, value)
			}
			for _, argument := range arguments {
				if argument == nil {
					return nil, nil
				}
			}
			modelID, err := vectorStringArgument(arguments[0], "model ID")
			if err != nil {
				return nil, err
			}
			modelVersion, err := vectorStringArgument(arguments[1], "model version")
			if err != nil {
				return nil, err
			}
			spaceID, err := vectorStringArgument(arguments[2], "embedding space ID")
			if err != nil {
				return nil, err
			}
			modality, err := vectorStringArgument(arguments[3], "source modality")
			if err != nil {
				return nil, err
			}
			elements := make([]float32, 0, len(arguments)-4)
			for index, raw := range arguments[4:] {
				value, err := codecToFloat64(raw)
				if err != nil {
					return nil, fmt.Errorf("VECTOR_FROM_ARRAY element %d: %w", index, err)
				}
				elements = append(elements, float32(value))
			}
			return vector.NewValue(vector.Space{ModelID: modelID, ModelVersion: modelVersion, ID: spaceID}, modality, elements)
		}
		if functionName == "VECTOR_FROM_SOURCE" {
			if len(v.Exprs) != 2 {
				return nil, fmt.Errorf("VECTOR_FROM_SOURCE requires a vector JSON document and a source CONTENT column")
			}
			definition, ok := v.Exprs[0].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("VECTOR_FROM_SOURCE definition must be a scalar expression")
			}
			rawDefinition, err := evaluateExpressionWithRow(definition.Expr, values)
			if err != nil || rawDefinition == nil {
				return nil, err
			}
			var definitionBytes []byte
			switch typed := rawDefinition.(type) {
			case []byte:
				definitionBytes = append([]byte(nil), typed...)
			case string:
				definitionBytes = []byte(typed)
			default:
				return nil, fmt.Errorf("VECTOR_FROM_SOURCE definition must be JSON text or bytes; got %T", rawDefinition)
			}
			sourceColumnExpr, ok := v.Exprs[1].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("VECTOR_FROM_SOURCE source must be a CONTENT column")
			}
			sourceColumn, ok := sourceColumnExpr.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, fmt.Errorf("VECTOR_FROM_SOURCE source must be a CONTENT column")
			}
			sourceValue, err := evaluateExpressionWithRow(sourceColumn, values)
			if err != nil || sourceValue == nil {
				return nil, err
			}
			rowContext, ok := contentSourceContextForColumn(values, sourceColumn)
			if !ok || len(rowContext.PrimaryKey) == 0 {
				return nil, fmt.Errorf("VECTOR_FROM_SOURCE requires stored-row context for the source column")
			}
			var typedSource content.Value
			switch typed := sourceValue.(type) {
			case content.Value:
				typedSource = typed
			case *content.Value:
				if typed == nil {
					return nil, nil
				}
				typedSource = *typed
			default:
				return nil, fmt.Errorf("VECTOR_FROM_SOURCE requires a typed CONTENT column; got %T", sourceValue)
			}
			source, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, sourceColumn.Name.String(), rowContext.PrimaryKey, typedSource)
			if err != nil {
				return nil, fmt.Errorf("VECTOR_FROM_SOURCE source verification failed: %w", err)
			}
			return vector.FromSourceJSON(definitionBytes, source)
		}
		if functionName == "VECTOR_FROM_BLOCK" {
			if len(v.Exprs) != 3 {
				return nil, fmt.Errorf("VECTOR_FROM_BLOCK requires vector JSON, a CONTENT_BLOCK column and its source CONTENT column")
			}
			definitionExpr, ok := v.Exprs[0].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("VECTOR_FROM_BLOCK definition must be a scalar expression")
			}
			rawDefinition, err := evaluateExpressionWithRow(definitionExpr.Expr, values)
			if err != nil || rawDefinition == nil {
				return nil, err
			}
			var definitionBytes []byte
			switch typed := rawDefinition.(type) {
			case []byte:
				definitionBytes = append([]byte(nil), typed...)
			case string:
				definitionBytes = []byte(typed)
			default:
				return nil, fmt.Errorf("VECTOR_FROM_BLOCK definition must be JSON text or bytes; got %T", rawDefinition)
			}
			blockFunction, ok := v.Exprs[1].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("VECTOR_FROM_BLOCK block must be a CONTENT_BLOCK column")
			}
			blockColumn, ok := blockFunction.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, fmt.Errorf("VECTOR_FROM_BLOCK block must be a CONTENT_BLOCK column")
			}
			block, isNull, err := evaluateContentBlockArgument(&sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("CONTENT_BLOCK_FROM_JSON"), Exprs: sqlparser.SelectExprs{&sqlparser.AliasedExpr{Expr: blockColumn}},
			}, values)
			if err != nil {
				return nil, err
			}
			if isNull || block.State != content.BlockStateReady {
				return nil, nil
			}
			sourceExpr, ok := v.Exprs[2].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("VECTOR_FROM_BLOCK source must be a CONTENT column")
			}
			sourceColumn, ok := sourceExpr.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, fmt.Errorf("VECTOR_FROM_BLOCK source must be a CONTENT column")
			}
			sourceValue, err := evaluateExpressionWithRow(sourceColumn, values)
			if err != nil || sourceValue == nil {
				return nil, err
			}
			var typedSource content.Value
			switch typed := sourceValue.(type) {
			case content.Value:
				typedSource = typed
			case *content.Value:
				if typed == nil {
					return nil, nil
				}
				typedSource = *typed
			default:
				return nil, fmt.Errorf("VECTOR_FROM_BLOCK source must be typed CONTENT; got %T", sourceValue)
			}
			rowContext, ok := contentSourceContextForColumn(values, sourceColumn)
			if !ok || len(rowContext.PrimaryKey) == 0 {
				return nil, fmt.Errorf("VECTOR_FROM_BLOCK requires stored-row context for the source column")
			}
			actualSource, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, sourceColumn.Name.String(), rowContext.PrimaryKey, typedSource)
			if err != nil {
				return nil, fmt.Errorf("VECTOR_FROM_BLOCK source verification failed: %w", err)
			}
			return vector.FromBlockJSON(definitionBytes, actualSource, block)
		}
		if functionName == "VECTOR_MATCHES_BLOCK" {
			return evaluateVectorBlockBinding(v, values, false)
		}
		if functionName == "VECTOR_VERIFIED_BLOCK_DISTANCE" {
			return evaluateVectorBlockBinding(v, values, true)
		}
		if functionName == "VECTOR_MATCHES_SOURCE" || functionName == "VECTOR_VERIFIED_DISTANCE" {
			minimum := 2
			maximum := 2
			if functionName == "VECTOR_VERIFIED_DISTANCE" {
				minimum, maximum = 3, 4
			}
			if len(v.Exprs) < minimum || len(v.Exprs) > maximum {
				return nil, fmt.Errorf("%s received an invalid argument count", functionName)
			}
			arguments := make([]interface{}, 0, len(v.Exprs))
			for _, rawArgument := range v.Exprs {
				argument, ok := rawArgument.(*sqlparser.AliasedExpr)
				if !ok {
					return nil, fmt.Errorf("%s arguments must be scalar expressions", functionName)
				}
				value, err := evaluateExpressionWithRow(argument.Expr, values)
				if err != nil {
					return nil, err
				}
				arguments = append(arguments, value)
			}
			if arguments[0] == nil || arguments[minimum-1] == nil {
				if functionName == "VECTOR_MATCHES_SOURCE" {
					return false, nil
				}
				return nil, nil
			}
			stored, ok := vectorArgument(arguments[0])
			if !ok {
				return nil, fmt.Errorf("%s first argument must be VECTOR; got %T", functionName, arguments[0])
			}
			sourceIndex := 1
			var query vector.Value
			if functionName == "VECTOR_VERIFIED_DISTANCE" {
				query, ok = vectorArgument(arguments[1])
				if !ok {
					return nil, fmt.Errorf("VECTOR_VERIFIED_DISTANCE second argument must be VECTOR; got %T", arguments[1])
				}
				sourceIndex = 2
			}
			sourceColumnExpr, ok := v.Exprs[sourceIndex].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("%s source must be a CONTENT column", functionName)
			}
			sourceColumn, ok := sourceColumnExpr.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, fmt.Errorf("%s source must be a CONTENT column", functionName)
			}
			sourceValue := arguments[sourceIndex]
			var typedSource content.Value
			switch typed := sourceValue.(type) {
			case content.Value:
				typedSource = typed
			case *content.Value:
				if typed == nil {
					if functionName == "VECTOR_MATCHES_SOURCE" {
						return false, nil
					}
					return nil, nil
				}
				typedSource = *typed
			default:
				return nil, fmt.Errorf("%s requires a typed CONTENT column; got %T", functionName, sourceValue)
			}
			rowContext, ok := contentSourceContextForColumn(values, sourceColumn)
			if !ok || len(rowContext.PrimaryKey) == 0 {
				return nil, fmt.Errorf("%s requires stored-row context for the source column", functionName)
			}
			actualSource, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, sourceColumn.Name.String(), rowContext.PrimaryKey, typedSource)
			if err != nil {
				return nil, fmt.Errorf("%s current source is invalid: %w", functionName, err)
			}
			matches := stored.State == vector.StateReady && stored.Source != nil && stored.Source.Matches(actualSource)
			if functionName == "VECTOR_MATCHES_SOURCE" {
				return matches, nil
			}
			if !matches {
				return nil, nil
			}
			metric := vector.MetricCosine
			if len(arguments) == 4 {
				metricName, err := vectorStringArgument(arguments[3], "metric")
				if err != nil {
					return nil, err
				}
				metric = vector.Metric(strings.ToUpper(strings.TrimSpace(metricName)))
			}
			return vector.Distance(stored, query, metric)
		}
		if functionName == "VECTOR_DISTANCE" {
			if len(v.Exprs) < 2 || len(v.Exprs) > 3 {
				return nil, fmt.Errorf("VECTOR_DISTANCE requires two vectors and an optional metric")
			}
			arguments := make([]interface{}, 0, len(v.Exprs))
			for _, rawArgument := range v.Exprs {
				argument, ok := rawArgument.(*sqlparser.AliasedExpr)
				if !ok {
					return nil, fmt.Errorf("VECTOR_DISTANCE arguments must be scalar expressions")
				}
				value, err := evaluateExpressionWithRow(argument.Expr, values)
				if err != nil {
					return nil, err
				}
				arguments = append(arguments, value)
			}
			if arguments[0] == nil || arguments[1] == nil {
				return nil, nil
			}
			left, ok := vectorArgument(arguments[0])
			if !ok {
				return nil, fmt.Errorf("VECTOR_DISTANCE first argument must be VECTOR, got %T", arguments[0])
			}
			right, ok := vectorArgument(arguments[1])
			if !ok {
				return nil, fmt.Errorf("VECTOR_DISTANCE second argument must be VECTOR, got %T", arguments[1])
			}
			metric := vector.MetricCosine
			if len(arguments) == 3 {
				metricName, err := vectorStringArgument(arguments[2], "metric")
				if err != nil {
					return nil, err
				}
				metric = vector.Metric(strings.ToUpper(strings.TrimSpace(metricName)))
			}
			return vector.Distance(left, right, metric)
		}
		switch functionName {
		case "VECTOR_DIMENSION", "VECTOR_MODEL_ID", "VECTOR_MODEL_VERSION", "VECTOR_SPACE", "VECTOR_MODALITY", "VECTOR_STATE", "VECTOR_BLOCK_ID",
			"VECTOR_SOURCE_SCHEMA", "VECTOR_SOURCE_TABLE", "VECTOR_SOURCE_COLUMN", "VECTOR_SOURCE_PRIMARY_KEY", "VECTOR_SOURCE_REVISION", "VECTOR_SOURCE_SHA256":
			if len(v.Exprs) != 1 {
				return nil, fmt.Errorf("%s requires exactly one VECTOR argument", functionName)
			}
			argument, ok := v.Exprs[0].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("%s argument must be a scalar expression", functionName)
			}
			value, err := evaluateExpressionWithRow(argument.Expr, values)
			if err != nil {
				return nil, err
			}
			typed, ok := vectorArgument(value)
			if !ok {
				if value == nil {
					return nil, nil
				}
				return nil, fmt.Errorf("%s requires a typed VECTOR value; got %T", functionName, value)
			}
			switch functionName {
			case "VECTOR_DIMENSION":
				return int64(typed.Dimension()), nil
			case "VECTOR_MODEL_ID":
				return typed.Space.ModelID, nil
			case "VECTOR_MODEL_VERSION":
				return typed.Space.ModelVersion, nil
			case "VECTOR_SPACE":
				return typed.Space.ID, nil
			case "VECTOR_MODALITY":
				return typed.SourceModality, nil
			case "VECTOR_STATE":
				return string(typed.State), nil
			case "VECTOR_BLOCK_ID":
				if typed.SourceBlockID != "" {
					return typed.SourceBlockID, nil
				}
			case "VECTOR_SOURCE_SCHEMA":
				if typed.Source != nil {
					return typed.Source.Schema, nil
				}
			case "VECTOR_SOURCE_TABLE":
				if typed.Source != nil {
					return typed.Source.Table, nil
				}
			case "VECTOR_SOURCE_COLUMN":
				if typed.Source != nil {
					return typed.Source.Column, nil
				}
			case "VECTOR_SOURCE_PRIMARY_KEY":
				if typed.Source != nil {
					return append([]byte(nil), typed.Source.PrimaryKey...), nil
				}
			case "VECTOR_SOURCE_REVISION":
				if typed.Source != nil {
					return typed.Source.Revision, nil
				}
			case "VECTOR_SOURCE_SHA256":
				if typed.Source != nil {
					return fmt.Sprintf("%x", typed.Source.Digest[:]), nil
				}
			}
		}
		if functionName == "CONTENT_FROM_BYTES" {
			if len(v.Exprs) != 2 {
				return nil, fmt.Errorf("CONTENT_FROM_BYTES requires exactly two arguments: bytes and MIME type")
			}
			arguments := make([]interface{}, 0, 2)
			for _, rawArgument := range v.Exprs {
				argument, ok := rawArgument.(*sqlparser.AliasedExpr)
				if !ok {
					return nil, fmt.Errorf("CONTENT_FROM_BYTES arguments must be scalar expressions")
				}
				value, err := evaluateExpressionWithRow(argument.Expr, values)
				if err != nil {
					return nil, err
				}
				arguments = append(arguments, value)
			}
			mimeType, ok := arguments[1].(string)
			if !ok {
				if mimeBytes, bytesOK := arguments[1].([]byte); bytesOK {
					mimeType = string(mimeBytes)
				} else {
					return nil, fmt.Errorf("CONTENT_FROM_BYTES MIME argument has unsupported type %T", arguments[1])
				}
			}
			var source []byte
			switch typed := arguments[0].(type) {
			case content.Value:
				if err := typed.Validate(); err != nil {
					return nil, fmt.Errorf("CONTENT_FROM_BYTES received invalid CONTENT value: %w", err)
				}
				if !strings.EqualFold(typed.MIMEType, strings.TrimSpace(mimeType)) {
					return nil, fmt.Errorf("CONTENT_FROM_BYTES MIME %q does not match streamed CONTENT MIME %q", mimeType, typed.MIMEType)
				}
				return typed, nil
			case *content.Value:
				if typed == nil {
					return nil, nil
				}
				if err := typed.Validate(); err != nil {
					return nil, fmt.Errorf("CONTENT_FROM_BYTES received invalid CONTENT value: %w", err)
				}
				if !strings.EqualFold(typed.MIMEType, strings.TrimSpace(mimeType)) {
					return nil, fmt.Errorf("CONTENT_FROM_BYTES MIME %q does not match streamed CONTENT MIME %q", mimeType, typed.MIMEType)
				}
				return *typed, nil
			case []byte:
				source = append([]byte(nil), typed...)
			case string:
				source = []byte(typed)
			default:
				return nil, fmt.Errorf("CONTENT_FROM_BYTES bytes argument has unsupported type %T", arguments[0])
			}
			return content.NewValue(mimeType, 1, source)
		}
		if functionName == "CONTENT_SLICE" {
			if len(v.Exprs) != 3 {
				return nil, fmt.Errorf("CONTENT_SLICE requires exactly three arguments: CONTENT value, zero-based byte offset and length")
			}
			arguments := make([]interface{}, 0, 3)
			for _, rawArgument := range v.Exprs {
				argument, ok := rawArgument.(*sqlparser.AliasedExpr)
				if !ok {
					return nil, fmt.Errorf("CONTENT_SLICE arguments must be scalar expressions")
				}
				value, err := evaluateExpressionWithRow(argument.Expr, values)
				if err != nil {
					return nil, err
				}
				arguments = append(arguments, value)
			}
			if arguments[0] == nil || arguments[1] == nil || arguments[2] == nil {
				return nil, nil
			}
			var typed content.Value
			switch value := arguments[0].(type) {
			case content.Value:
				typed = value
			case *content.Value:
				if value == nil {
					return nil, nil
				}
				typed = *value
			default:
				return nil, fmt.Errorf("CONTENT_SLICE requires a typed CONTENT value; got %T", arguments[0])
			}
			offset, err := contentRangeArgument(arguments[1], "offset")
			if err != nil {
				return nil, err
			}
			length, err := contentRangeArgument(arguments[2], "length")
			if err != nil {
				return nil, err
			}
			store, err := contentSegmentStoreForValue(values, typed)
			if err != nil {
				return nil, err
			}
			return typed.ReadRangeFromStore(storedFunctionContext(values), store, offset, length)
		}
		if functionName == "BM25_TERM_SCORE" {
			return evaluateBM25TermScore(v, values)
		}
		if functionName == "BM25_SCORE" {
			return evaluateBM25Score(v, values)
		}
		if functionName == "FULLTEXT_TOKEN_COUNT" {
			return evaluateFullTextTokenCount(v, values)
		}
		if functionName == "FULLTEXT_SCORE" {
			if len(v.Exprs) < 2 || len(v.Exprs) > 3 {
				return nil, fmt.Errorf("FULLTEXT_SCORE requires text, query text and an optional NATURAL or BOOLEAN mode")
			}
			arguments := make([]interface{}, 0, len(v.Exprs))
			for _, rawArgument := range v.Exprs {
				argument, ok := rawArgument.(*sqlparser.AliasedExpr)
				if !ok {
					return nil, fmt.Errorf("FULLTEXT_SCORE arguments must be scalar expressions")
				}
				value, err := evaluateExpressionWithRow(argument.Expr, values)
				if err != nil {
					return nil, err
				}
				arguments = append(arguments, value)
			}
			if arguments[0] == nil || arguments[1] == nil {
				return nil, nil
			}
			text, err := fullTextStringArgument(arguments[0], "content", values)
			if err != nil {
				return nil, err
			}
			query, err := fullTextStringArgument(arguments[1], "query", values)
			if err != nil {
				return nil, err
			}
			booleanMode := false
			if len(arguments) == 3 {
				mode, err := fullTextStringArgument(arguments[2], "mode", values)
				if err != nil {
					return nil, err
				}
				switch strings.ToUpper(strings.TrimSpace(mode)) {
				case "NATURAL", "NATURAL LANGUAGE":
				case "BOOLEAN", "BOOLEAN MODE":
					booleanMode = true
				default:
					return nil, fmt.Errorf("FULLTEXT_SCORE mode must be NATURAL or BOOLEAN; got %q", mode)
				}
			}
			score, _ := fullTextMatches(text, query, booleanMode)
			return int64(score), nil
		}
		switch functionName {
		case "CONTENT_TEXT", "CONTENT_BYTES", "CONTENT_MIME", "CONTENT_KIND", "CONTENT_REVISION", "CONTENT_SHA256", "CONTENT_LENGTH":
			if len(v.Exprs) != 1 {
				return nil, fmt.Errorf("%s requires exactly one CONTENT argument", functionName)
			}
			argument, ok := v.Exprs[0].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("%s argument must be a scalar expression", functionName)
			}
			value, err := evaluateExpressionWithRow(argument.Expr, values)
			if err != nil {
				return nil, err
			}
			if value == nil {
				return nil, nil
			}
			var typed content.Value
			switch contentValue := value.(type) {
			case content.Value:
				typed = contentValue
			case *content.Value:
				if contentValue == nil {
					return nil, nil
				}
				typed = *contentValue
			default:
				return nil, fmt.Errorf("%s requires a typed CONTENT column; got %T", functionName, value)
			}
			if err := typed.Validate(); err != nil {
				return nil, fmt.Errorf("%s received invalid CONTENT metadata: %w", functionName, err)
			}
			switch functionName {
			case "CONTENT_TEXT":
				store, err := contentSegmentStoreForValue(values, typed)
				if err != nil {
					return nil, err
				}
				return content.ExtractTextFromStore(storedFunctionContext(values), store, typed)
			case "CONTENT_BYTES":
				store, err := contentSegmentStoreForValue(values, typed)
				if err != nil {
					return nil, err
				}
				return typed.ReadAllFromStore(storedFunctionContext(values), store, content.MaxReadRangeBytes)
			case "CONTENT_MIME":
				return typed.MIMEType, nil
			case "CONTENT_KIND":
				return string(typed.Kind), nil
			case "CONTENT_REVISION":
				return typed.Revision, nil
			case "CONTENT_SHA256":
				return typed.DigestHex(), nil
			case "CONTENT_LENGTH":
				return typed.Size(), nil
			}
		}
		switch functionName {
		case "CONTENT_BLOCK_ID", "CONTENT_BLOCK_IDENTITY_VERSION", "CONTENT_BLOCK_STATE", "CONTENT_BLOCK_KIND", "CONTENT_BLOCK_TEXT", "CONTENT_BLOCK_PAYLOAD", "CONTENT_BLOCK_PAYLOAD_SCHEMA", "CONTENT_BLOCK_PAYLOAD_VERSION", "CONTENT_BLOCK_PAYLOAD_MEDIA_TYPE", "CONTENT_BLOCK_PAYLOAD_SHA256", "CONTENT_BLOCK_ATTRIBUTES", "CONTENT_BLOCK_ORDINAL", "CONTENT_BLOCK_PARENT_ID", "CONTENT_BLOCK_RELATION",
			"CONTENT_BLOCK_SOURCE_SCHEMA", "CONTENT_BLOCK_SOURCE_TABLE", "CONTENT_BLOCK_SOURCE_COLUMN",
			"CONTENT_BLOCK_SOURCE_PRIMARY_KEY", "CONTENT_BLOCK_SOURCE_REVISION", "CONTENT_BLOCK_SOURCE_SHA256",
			"CONTENT_BLOCK_PAGE", "CONTENT_BLOCK_BYTE_START", "CONTENT_BLOCK_BYTE_END",
			"CONTENT_BLOCK_START_MS", "CONTENT_BLOCK_END_MS", "CONTENT_BLOCK_TRACK",
			"CONTENT_BLOCK_REGION_X", "CONTENT_BLOCK_REGION_Y", "CONTENT_BLOCK_REGION_WIDTH", "CONTENT_BLOCK_REGION_HEIGHT",
			"CONTENT_BLOCK_PROCESSOR_ID", "CONTENT_BLOCK_PROCESSOR_VERSION",
			"CONTENT_BLOCK_MODEL_ID", "CONTENT_BLOCK_MODEL_VERSION":
			block, isNull, err := evaluateContentBlockArgument(v, values)
			if err != nil || isNull {
				return nil, err
			}
			switch functionName {
			case "CONTENT_BLOCK_ID":
				return block.ID, nil
			case "CONTENT_BLOCK_IDENTITY_VERSION":
				return uint64(block.IdentityVersion), nil
			case "CONTENT_BLOCK_STATE":
				return string(block.State), nil
			case "CONTENT_BLOCK_KIND":
				return string(block.Kind), nil
			case "CONTENT_BLOCK_TEXT":
				return block.Text, nil
			case "CONTENT_BLOCK_PAYLOAD":
				if block.Payload != nil {
					return string(block.Payload.JSON), nil
				}
			case "CONTENT_BLOCK_PAYLOAD_SCHEMA":
				if block.Payload != nil {
					return block.Payload.SchemaID, nil
				}
			case "CONTENT_BLOCK_PAYLOAD_VERSION":
				if block.Payload != nil {
					return uint64(block.Payload.SchemaVersion), nil
				}
			case "CONTENT_BLOCK_PAYLOAD_MEDIA_TYPE":
				if block.Payload != nil {
					return block.Payload.MediaType, nil
				}
			case "CONTENT_BLOCK_PAYLOAD_SHA256":
				if block.Payload != nil {
					return block.Payload.SHA256, nil
				}
			case "CONTENT_BLOCK_PARENT_ID":
				if block.ParentID != "" {
					return block.ParentID, nil
				}
			case "CONTENT_BLOCK_RELATION":
				if block.Relation != "" {
					return block.Relation, nil
				}
			case "CONTENT_BLOCK_ATTRIBUTES":
				attributes, err := json.Marshal(block.Attributes)
				if err != nil {
					return nil, fmt.Errorf("encode CONTENT_BLOCK attributes: %w", err)
				}
				return string(attributes), nil
			case "CONTENT_BLOCK_ORDINAL":
				return uint64(block.Ordinal), nil
			case "CONTENT_BLOCK_SOURCE_SCHEMA":
				return block.Source.Schema, nil
			case "CONTENT_BLOCK_SOURCE_TABLE":
				return block.Source.Table, nil
			case "CONTENT_BLOCK_SOURCE_COLUMN":
				return block.Source.Column, nil
			case "CONTENT_BLOCK_SOURCE_PRIMARY_KEY":
				return append([]byte(nil), block.Source.PrimaryKey...), nil
			case "CONTENT_BLOCK_SOURCE_REVISION":
				return block.Source.Revision, nil
			case "CONTENT_BLOCK_SOURCE_SHA256":
				return fmt.Sprintf("%x", block.Source.Digest[:]), nil
			case "CONTENT_BLOCK_PAGE":
				if block.Locator.Page != nil {
					return uint64(*block.Locator.Page), nil
				}
			case "CONTENT_BLOCK_BYTE_START":
				if block.Locator.ByteStart != nil {
					return *block.Locator.ByteStart, nil
				}
			case "CONTENT_BLOCK_BYTE_END":
				if block.Locator.ByteEnd != nil {
					return *block.Locator.ByteEnd, nil
				}
			case "CONTENT_BLOCK_START_MS":
				if block.Locator.StartMS != nil {
					return *block.Locator.StartMS, nil
				}
			case "CONTENT_BLOCK_END_MS":
				if block.Locator.EndMS != nil {
					return *block.Locator.EndMS, nil
				}
			case "CONTENT_BLOCK_TRACK":
				if block.Locator.Track != "" {
					return block.Locator.Track, nil
				}
			case "CONTENT_BLOCK_REGION_X":
				if block.Locator.Region != nil {
					return block.Locator.Region.X, nil
				}
			case "CONTENT_BLOCK_REGION_Y":
				if block.Locator.Region != nil {
					return block.Locator.Region.Y, nil
				}
			case "CONTENT_BLOCK_REGION_WIDTH":
				if block.Locator.Region != nil {
					return block.Locator.Region.Width, nil
				}
			case "CONTENT_BLOCK_REGION_HEIGHT":
				if block.Locator.Region != nil {
					return block.Locator.Region.Height, nil
				}
			case "CONTENT_BLOCK_PROCESSOR_ID":
				return block.ProcessorID, nil
			case "CONTENT_BLOCK_PROCESSOR_VERSION":
				return block.ProcessorVersion, nil
			case "CONTENT_BLOCK_MODEL_ID":
				if block.ModelID != "" {
					return block.ModelID, nil
				}
			case "CONTENT_BLOCK_MODEL_VERSION":
				if block.ModelVersion != "" {
					return block.ModelVersion, nil
				}
			}
			return nil, nil
		}
		if dataDir, ok := values[storedFunctionDataDirKey].(string); ok && strings.TrimSpace(dataDir) != "" {
			schemaName, _ := values[storedFunctionSchemaKey].(string)
			if value, handled, err := evaluatePersistedFunctionProjectionWithContext(v, values, dataDir, schemaName); handled {
				return value, err
			}
		}
		name := sqlparser.String(v)
		if value, exists := values[name]; exists {
			return value, nil
		}
		if value, exists := values[strings.ToLower(name)]; exists {
			return value, nil
		}
		args := make([]interface{}, 0, len(v.Exprs))
		functionName = strings.ToUpper(v.Name.String())
		for index, selectExpr := range v.Exprs {
			if functionName == "COUNT" {
				if _, ok := selectExpr.(*sqlparser.StarExpr); ok {
					// COUNT(*) is represented by the parser as a StarExpr,
					// not an AliasedExpr. The aggregate executor counts rows
					// directly, while row-level expression evaluation still
					// needs one non-NULL marker for the current row (for
					// example when evaluating HAVING COUNT(*) > 0).
					args = append(args, int64(1))
					continue
				}
			}
			aliased, ok := selectExpr.(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("函数 %s 包含不支持的参数 %T", v.Name.String(), selectExpr)
			}
			if interval, ok := aliased.Expr.(*sqlparser.IntervalExpr); ok {
				args = append(args, sqlparser.String(interval))
				continue
			}
			if (functionName == "TIMESTAMPDIFF" || functionName == "TIMESTAMPADD") && index == 0 {
				if unit, ok := aliased.Expr.(*sqlparser.ColName); ok {
					args = append(args, unit.Name.String())
					continue
				}
			}
			value, err := evaluateExpressionWithRow(aliased.Expr, values)
			if err != nil {
				return nil, err
			}
			args = append(args, value)
		}
		planArgs := make([]plan.Expression, 0, len(args))
		for _, arg := range args {
			planArgs = append(planArgs, &plan.Constant{Value: arg})
		}
		return (&plan.Function{FuncName: v.Name.String(), FuncArgs: planArgs}).Eval(planEvalContextWithRow(values))
	case *sqlparser.ConvertExpr:
		if v.Type == nil {
			return nil, fmt.Errorf("转换目标类型为空")
		}
		expression := plan.BuildExpression(v)
		if expression == nil {
			return nil, fmt.Errorf("不支持的转换表达式")
		}
		return expression.Eval(planEvalContextWithRow(values))
	case *sqlparser.ConvertUsingExpr:
		expression := plan.BuildExpression(v)
		if expression == nil {
			return nil, fmt.Errorf("不支持的字符集转换表达式")
		}
		return expression.Eval(planEvalContextWithRow(values))
	case *sqlparser.CollateExpr:
		return evaluateExpressionWithRow(v.Expr, values)
	case *sqlparser.ParenExpr:
		return evaluateExpressionWithRow(v.Expr, values)
	case *sqlparser.AndExpr, *sqlparser.OrExpr, *sqlparser.NotExpr,
		*sqlparser.ComparisonExpr, *sqlparser.RangeCond, *sqlparser.IsExpr:
		truth, err := evalPredicateTruth(v, values)
		if err != nil {
			return nil, err
		}
		if truth == sqlTruthUnknown {
			return nil, nil
		}
		return truth == sqlTruthTrue, nil
	case sqlparser.ValTuple:
		result := make([]interface{}, len(v))
		for i, item := range v {
			value, err := evaluateExpressionWithRow(item, values)
			if err != nil {
				return nil, err
			}
			result[i] = value
		}
		return result, nil
	case *sqlparser.SubstrExpr:
		name, err := evaluateExpressionWithRow(v.Name, values)
		if err != nil {
			return nil, err
		}
		from, err := evaluateExpressionWithRow(v.From, values)
		if err != nil {
			return nil, err
		}
		args := []plan.Expression{&plan.Constant{Value: name}, &plan.Constant{Value: from}}
		if v.To != nil {
			to, err := evaluateExpressionWithRow(v.To, values)
			if err != nil {
				return nil, err
			}
			args = append(args, &plan.Constant{Value: to})
		}
		return (&plan.Function{FuncName: "SUBSTRING", FuncArgs: args}).Eval(planEvalContextWithRow(values))
	case *sqlparser.CaseExpr:
		var caseValue interface{}
		var err error
		if v.Expr != nil {
			caseValue, err = evaluateExpressionWithRow(v.Expr, values)
			if err != nil {
				return nil, err
			}
		}
		for _, when := range v.Whens {
			if when == nil {
				continue
			}
			matched := false
			if v.Expr == nil {
				matched, err = evalPredicate(when.Cond, values)
			} else {
				conditionValue, conditionErr := evaluateExpressionWithRow(when.Cond, values)
				if conditionErr != nil {
					return nil, conditionErr
				}
				collation, collationErr := resolveComparisonCollation(v.Expr, when.Cond, values, "CASE")
				if collationErr != nil {
					return nil, collationErr
				}
				if caseValue != nil && conditionValue != nil {
					matched = compareScalarValuesWithCollation(caseValue, conditionValue, collation) == 0
				}
			}
			if err != nil {
				return nil, err
			}
			if matched {
				return evaluateExpressionWithRow(when.Val, values)
			}
		}
		if v.Else != nil {
			return evaluateExpressionWithRow(v.Else, values)
		}
		return nil, nil
	case *sqlparser.UnaryExpr:
		value, err := evaluateExpressionWithRow(v.Expr, values)
		if err != nil {
			return nil, err
		}
		return (&plan.UnaryOperation{
			Operator: v.Operator,
			Operand:  &plan.Constant{Value: value},
		}).Eval(planEvalContextWithRow(values))
	case *sqlparser.IntervalExpr:
		amount, err := evaluateExpressionWithRow(v.Expr, values)
		if err != nil {
			return nil, err
		}
		if amount == nil {
			return nil, nil
		}
		return sqlIntervalValue{amount: amount, unit: v.Unit}, nil
	case *sqlparser.BinaryExpr:
		left, err := evaluateExpressionWithRow(v.Left, values)
		if err != nil {
			return nil, err
		}
		right, err := evaluateExpressionWithRow(v.Right, values)
		if err != nil {
			return nil, err
		}
		if interval, ok := right.(sqlIntervalValue); ok {
			functionName := "DATE_ADD"
			if v.Operator == sqlparser.MinusStr {
				functionName = "DATE_SUB"
			} else if v.Operator != sqlparser.PlusStr {
				return nil, fmt.Errorf("interval only supports date addition/subtraction")
			}
			intervalText := fmt.Sprintf("%v %s", interval.amount, interval.unit)
			return (&plan.Function{FuncName: functionName, FuncArgs: []plan.Expression{
				&plan.Constant{Value: left}, &plan.Constant{Value: intervalText},
			}}).Eval(planEvalContextWithRow(values))
		}
		var op plan.BinaryOp
		switch v.Operator {
		case sqlparser.PlusStr:
			op = plan.OpAdd
		case sqlparser.MinusStr:
			op = plan.OpSub
		case sqlparser.MultStr:
			op = plan.OpMul
		case sqlparser.DivStr:
			op = plan.OpDiv
		case sqlparser.ModStr:
			op = plan.OpMod
		case sqlparser.IntDivStr:
			op = plan.OpIntDiv
		case sqlparser.BitAndStr:
			op = plan.OpBitAnd
		case sqlparser.BitOrStr:
			op = plan.OpBitOr
		case sqlparser.BitXorStr:
			op = plan.OpBitXor
		case sqlparser.ShiftLeftStr:
			op = plan.OpShiftLeft
		case sqlparser.ShiftRightStr:
			op = plan.OpShiftRight
		default:
			return nil, fmt.Errorf("不支持的二元操作符: %s", v.Operator)
		}
		return (&plan.BinaryOperation{
			Op:    op,
			Left:  &plan.Constant{Value: left},
			Right: &plan.Constant{Value: right},
		}).Eval(planEvalContextWithRow(values))
	default:
		return nil, fmt.Errorf("不支持的表达式类型: %T", expr)
	}
}

func contentRangeArgument(value interface{}, name string) (uint64, error) {
	if parsed, ok := autoIncrementValueAsUint64(value); ok {
		return parsed, nil
	}
	var text string
	switch typed := value.(type) {
	case string:
		text = typed
	case []byte:
		text = string(typed)
	default:
		return 0, fmt.Errorf("CONTENT_SLICE %s must be a non-negative integer; got %T", name, value)
	}
	parsed, err := strconv.ParseUint(strings.TrimSpace(text), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("CONTENT_SLICE %s must be a non-negative integer: %w", name, err)
	}
	return parsed, nil
}

func fullTextStringArgument(value interface{}, name string, rowValues map[string]interface{}) (string, error) {
	switch typed := value.(type) {
	case string:
		return typed, nil
	case []byte:
		return string(typed), nil
	case basic.Value:
		return typed.ToString(), nil
	case content.Value:
		store, err := contentSegmentStoreForValue(rowValues, typed)
		if err != nil {
			return "", fmt.Errorf("FULLTEXT_SCORE %s CONTENT store is unavailable: %w", name, err)
		}
		text, err := content.ExtractTextFromStore(storedFunctionContext(rowValues), store, typed)
		if err != nil {
			return "", fmt.Errorf("FULLTEXT_SCORE %s CONTENT cannot be decoded as text: %w", name, err)
		}
		return text, nil
	case *content.Value:
		if typed == nil {
			return "", nil
		}
		return fullTextStringArgument(*typed, name, rowValues)
	default:
		return "", fmt.Errorf("FULLTEXT_SCORE %s must be text; got %T", name, value)
	}
}

func evaluateContentBlockArgument(function *sqlparser.FuncExpr, values map[string]interface{}) (content.Block, bool, error) {
	if function == nil || len(function.Exprs) != 1 {
		name := "CONTENT_BLOCK accessor"
		if function != nil {
			name = function.Name.String()
		}
		return content.Block{}, false, fmt.Errorf("%s requires exactly one CONTENT_BLOCK value", name)
	}
	argument, ok := function.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return content.Block{}, false, fmt.Errorf("%s argument must be a scalar expression", function.Name.String())
	}
	value, err := evaluateExpressionWithRow(argument.Expr, values)
	if err != nil {
		return content.Block{}, false, err
	}
	if value == nil {
		return content.Block{}, true, nil
	}
	var typed content.Value
	switch blockValue := value.(type) {
	case content.Value:
		typed = blockValue
	case *content.Value:
		if blockValue == nil {
			return content.Block{}, true, nil
		}
		typed = *blockValue
	default:
		return content.Block{}, false, fmt.Errorf("%s requires a CONTENT value created with MIME type %s; got %T", function.Name.String(), content.BlockMIMEType, value)
	}
	block, err := content.BlockFromValue(typed)
	if err != nil {
		return content.Block{}, false, fmt.Errorf("%s: %w", function.Name.String(), err)
	}
	return block, false, nil
}

func vectorArgument(value interface{}) (vector.Value, bool) {
	switch typed := value.(type) {
	case vector.Value:
		return typed, true
	case *vector.Value:
		if typed != nil {
			return *typed, true
		}
	}
	return vector.Value{}, false
}

func vectorStringArgument(value interface{}, name string) (string, error) {
	switch typed := value.(type) {
	case string:
		return typed, nil
	case []byte:
		return string(typed), nil
	default:
		return "", fmt.Errorf("VECTOR argument %s must be text, got %T", name, value)
	}
}

func planEvalContextWithRow(values map[string]interface{}) *plan.EvalContext {
	ctx := &plan.EvalContext{Row: values}
	if values != nil {
		ctx.SessionValues, _ = values[sessionExpressionValuesKey].(map[string]interface{})
	}
	return ctx
}

func resolveExpressionRowValue(values map[string]interface{}, requested string) (interface{}, bool) {
	normalized := strings.Trim(strings.TrimSpace(requested), "` ")
	lowerNormalized := strings.ToLower(normalized)
	for _, prefix := range []string{"@@global.", "@@session.", "@@local.", "global.", "session.", "local."} {
		if strings.HasPrefix(lowerNormalized, prefix) {
			normalized = normalized[len(prefix):]
			break
		}
	}
	if value, exists := values[normalized]; exists {
		return value, true
	}
	if value, exists := values[strings.ToLower(normalized)]; exists {
		return value, true
	}
	if value, exists := values[requested]; exists {
		return value, true
	}
	if value, exists := values[strings.ToLower(requested)]; exists {
		return value, true
	}
	for name, value := range values {
		if strings.EqualFold(strings.Trim(name, "` "), requested) {
			return value, true
		}
	}
	return nil, false
}

func normalizeNumericResult(v float64) interface{} {
	if v == float64(int64(v)) {
		return int64(v)
	}
	return v
}

func compareScalarValues(left, right interface{}) int {
	// MySQL sorts NULL before every non-NULL value in ascending order.
	if left == nil {
		if right == nil {
			return 0
		}
		return -1
	}
	if right == nil {
		return 1
	}
	// MySQL compares a numeric column against a numeric string numerically,
	// while two character operands retain collation/lexical comparison.
	if _, leftString := left.(string); leftString {
		if _, rightString := right.(string); rightString {
			return compareTextValues(left.(string), right.(string))
		}
	}
	if _, leftString := left.(string); !leftString {
		if _, rightString := right.(string); rightString {
			if rightNum, ok := numericStringValue(right); ok {
				if leftNum, ok := toFloat64(left); ok {
					return compareFloatValues(leftNum, rightNum)
				}
			}
		}
	}
	if _, rightString := right.(string); !rightString {
		if _, leftString := left.(string); leftString {
			if leftNum, ok := numericStringValue(left); ok {
				if rightNum, ok := toFloat64(right); ok {
					return compareFloatValues(leftNum, rightNum)
				}
			}
		}
	}
	if leftNum, ok := toFloat64(left); ok {
		if rightNum, ok := toFloat64(right); ok {
			return compareFloatValues(leftNum, rightNum)
		}
	}
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)
	return compareTextValues(leftStr, rightStr)
}

func compareTextValues(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func compareFloatValues(left, right float64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func numericStringValue(value interface{}) (float64, bool) {
	text, ok := value.(string)
	if !ok {
		return 0, false
	}
	number, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
	return number, err == nil
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
