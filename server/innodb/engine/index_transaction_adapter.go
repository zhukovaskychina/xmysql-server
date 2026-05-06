package engine

import (
	"context"
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

// IndexAdapter 索引适配器，提供索引访问接口
type IndexAdapter struct {
	indexManager        *manager.IndexManager
	btreeManager        interface{} // B+树管理器（可能是DefaultBPlusTreeManager）
	storageAdapter      *StorageAdapter
	tableStorageManager *manager.TableStorageManager
}

// NewIndexAdapter 创建索引适配器
func NewIndexAdapter(
	indexManager *manager.IndexManager,
	btreeManager interface{},
	storageAdapter *StorageAdapter,
) *IndexAdapter {
	var tableStorageManager *manager.TableStorageManager
	if storageAdapter != nil {
		tableStorageManager = storageAdapter.tableStorageManager
	}

	return &IndexAdapter{
		indexManager:        indexManager,
		btreeManager:        btreeManager,
		storageAdapter:      storageAdapter,
		tableStorageManager: tableStorageManager,
	}
}

// RangeScan 索引范围扫描
// 返回满足条件的主键列表（用于回表）
func (ia *IndexAdapter) RangeScan(ctx context.Context, indexID uint64, startKey, endKey []byte) ([][]byte, error) {
	logger.Debugf("Index range scan: indexID=%d, startKey=%v, endKey=%v", indexID, startKey, endKey)

	// 如果没有B+树管理器，返回空列表
	if ia.btreeManager == nil {
		logger.Debugf("No btree manager available, returning empty result")
		return [][]byte{}, nil
	}

	// 尝试将btreeManager转换为BTreeManager接口
	btreeManager, ok := ia.btreeManager.(manager.BTreeManager)
	if !ok {
		logger.Debugf("btreeManager is not manager.BTreeManager type, returning empty result")
		return [][]byte{}, nil
	}

	// 调用B+树管理器的RangeSearch方法
	records, err := btreeManager.RangeSearch(ctx, indexID, startKey, endKey)
	if err != nil {
		logger.Debugf("RangeSearch failed: %v, returning empty result", err)
		return [][]byte{}, nil
	}

	// 从IndexRecord中提取主键
	primaryKeys := make([][]byte, 0, len(records))
	for _, record := range records {
		// IndexRecord的Key字段就是主键
		primaryKeys = append(primaryKeys, record.Key)
	}

	logger.Debugf("RangeScan found %d records", len(primaryKeys))
	return primaryKeys, nil
}

// PointLookup 索引点查询
// 返回单个主键（用于回表）
func (ia *IndexAdapter) PointLookup(ctx context.Context, indexID uint64, key []byte) ([]byte, error) {
	logger.Debugf("Index point lookup: indexID=%d, key=%v", indexID, key)

	// 如果没有B+树管理器，返回错误
	if ia.btreeManager == nil {
		return nil, fmt.Errorf("no btree manager available")
	}

	// 尝试将btreeManager转换为BTreeManager接口
	btreeManager, ok := ia.btreeManager.(manager.BTreeManager)
	if !ok {
		return nil, fmt.Errorf("btreeManager is not manager.BTreeManager type")
	}

	// 调用B+树管理器的Search方法
	record, err := btreeManager.Search(ctx, indexID, key)
	if err != nil {
		return nil, fmt.Errorf("search failed: %w", err)
	}

	// 返回主键
	return record.Key, nil
}

// InsertEntry 插入索引项
func (ia *IndexAdapter) InsertEntry(ctx context.Context, indexID uint64, key, value []byte) error {
	logger.Debugf("Insert index entry: indexID=%d, keyLen=%d, valueLen=%d", indexID, len(key), len(value))

	// 验证参数
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	// 如果没有B+树管理器，返回错误
	if ia.btreeManager == nil {
		return fmt.Errorf("no btree manager available")
	}

	// 尝试将btreeManager转换为BTreeManager接口
	btreeManager, ok := ia.btreeManager.(manager.BTreeManager)
	if !ok {
		return fmt.Errorf("btreeManager is not manager.BTreeManager type")
	}

	// 调用B+树管理器的Insert方法
	err := btreeManager.Insert(ctx, indexID, key, value)
	if err != nil {
		return fmt.Errorf("failed to insert index entry: %w", err)
	}

	logger.Debugf("✅ Successfully inserted index entry: indexID=%d", indexID)
	return nil
}

// DeleteEntry 删除索引项
func (ia *IndexAdapter) DeleteEntry(ctx context.Context, indexID uint64, key []byte) error {
	logger.Debugf("Delete index entry: indexID=%d, keyLen=%d", indexID, len(key))

	// 验证参数
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	// 如果没有B+树管理器，返回错误
	if ia.btreeManager == nil {
		return fmt.Errorf("no btree manager available")
	}

	// 尝试将btreeManager转换为BTreeManager接口
	btreeManager, ok := ia.btreeManager.(manager.BTreeManager)
	if !ok {
		return fmt.Errorf("btreeManager is not manager.BTreeManager type")
	}

	// 调用B+树管理器的Delete方法
	err := btreeManager.Delete(ctx, indexID, key)
	if err != nil {
		return fmt.Errorf("failed to delete index entry: %w", err)
	}

	logger.Debugf("✅ Successfully deleted index entry: indexID=%d", indexID)
	return nil
}

// GetIndexMetadata 获取索引元数据
func (ia *IndexAdapter) GetIndexMetadata(ctx context.Context, schemaName, tableName, indexName string) (*IndexMetadata, error) {
	logger.Debugf("GetIndexMetadata: schema=%s, table=%s, index=%s", schemaName, tableName, indexName)

	// 如果没有索引管理器，返回默认元数据
	if ia.indexManager == nil {
		logger.Debugf("⚠️ No index manager available, returning default metadata")
		return &IndexMetadata{
			IndexID:     1,
			IndexName:   indexName,
			IsPrimary:   indexName == "PRIMARY",
			IsUnique:    indexName == "PRIMARY",
			Columns:     []string{},
			IsClustered: indexName == "PRIMARY",
		}, nil
	}

	// 从表存储管理器获取表信息
	if ia.tableStorageManager == nil {
		return nil, fmt.Errorf("table storage manager not available")
	}

	tableInfo, err := ia.tableStorageManager.GetTableStorageInfo(schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table storage info: %w", err)
	}

	// 从索引管理器获取表ID（使用SpaceID作为TableID）
	tableID := uint64(tableInfo.SpaceID)

	// 根据索引名称查找索引
	var foundIndex *manager.Index
	if indexName == "PRIMARY" {
		// 查找主键索引
		foundIndex = ia.indexManager.GetIndexByName(tableID, "PRIMARY")
	} else {
		// 查找普通索引
		foundIndex = ia.indexManager.GetIndexByName(tableID, indexName)
	}

	if foundIndex == nil {
		return nil, fmt.Errorf("index %s not found in table %s.%s", indexName, schemaName, tableName)
	}

	// 提取列名
	columns := make([]string, len(foundIndex.Columns))
	for i, col := range foundIndex.Columns {
		columns[i] = col.Name
	}

	// 构建索引元数据
	metadata := &IndexMetadata{
		IndexID:     foundIndex.IndexID,
		IndexName:   foundIndex.Name,
		IsPrimary:   foundIndex.IsPrimary,
		IsUnique:    foundIndex.IsUnique,
		Columns:     columns,
		IsClustered: foundIndex.IsPrimary, // 主键索引是聚簇索引
	}

	logger.Debugf("✅ Found index metadata: indexID=%d, columns=%v, unique=%v",
		metadata.IndexID, metadata.Columns, metadata.IsUnique)

	return metadata, nil
}

// IndexMetadata 索引元数据
type IndexMetadata struct {
	IndexID     uint64
	IndexName   string
	IsPrimary   bool
	IsUnique    bool
	Columns     []string
	IsClustered bool // 是否聚簇索引
}

// IsCoveringIndex 判断是否为覆盖索引
// 覆盖索引：索引列+主键列包含所有查询列
func (im *IndexMetadata) IsCoveringIndex(requiredColumns []string) bool {
	// 主键索引（聚簇索引）包含所有列
	if im.IsPrimary {
		logger.Debugf("✅ Primary index covers all columns")
		return true
	}

	// 如果没有要求的列，认为是覆盖的
	if len(requiredColumns) == 0 {
		return true
	}

	// 构建索引列的集合（包括索引列和主键列）
	indexColumnSet := make(map[string]bool)
	for _, col := range im.Columns {
		indexColumnSet[col] = true
	}

	// 对于非聚簇索引，索引记录中总是包含主键列
	// 这里假设主键列名为 "id" 或者在索引列中已经包含
	// 实际实现中应该从表元数据中获取主键列名
	indexColumnSet["id"] = true // 添加默认主键列

	// 检查所有要求的列是否都在索引列集合中
	for _, reqCol := range requiredColumns {
		if !indexColumnSet[reqCol] {
			logger.Debugf("❌ Column %s not in index %s, not a covering index", reqCol, im.IndexName)
			return false
		}
	}

	logger.Debugf("✅ Index %s covers all required columns: %v", im.IndexName, requiredColumns)
	return true
}

// ReadIndexRecord 从索引直接读取记录（覆盖索引优化）
// 当索引包含所有查询需要的列时，无需回表
func (ia *IndexAdapter) ReadIndexRecord(ctx context.Context, indexID uint64, key []byte) ([]byte, error) {
	logger.Debugf("ReadIndexRecord: indexID=%d, key=%v", indexID, key)

	// 如果没有B+树管理器，返回错误
	if ia.btreeManager == nil {
		return nil, fmt.Errorf("no btree manager available")
	}

	// 尝试将btreeManager转换为BTreeManager接口
	btreeManager, ok := ia.btreeManager.(manager.BTreeManager)
	if !ok {
		return nil, fmt.Errorf("btreeManager is not manager.BTreeManager type")
	}

	// 1. 在B+树索引中查找key
	record, err := btreeManager.Search(ctx, indexID, key)
	if err != nil {
		return nil, fmt.Errorf("search in index failed: %w", err)
	}

	// 2. 读取索引记录数据
	// IndexRecord包含Key和Value
	// 对于覆盖索引，Value包含索引列的值
	// 这里返回完整的记录数据（Key + Value）
	recordData := make([]byte, 0, len(record.Key)+len(record.Value))
	recordData = append(recordData, record.Key...)
	recordData = append(recordData, record.Value...)

	logger.Debugf("ReadIndexRecord found record, size=%d bytes", len(recordData))
	return recordData, nil
}

// TransactionAdapter 事务适配器，提供事务管理接口
type TransactionAdapter struct {
	storageManager *manager.StorageManager
	lockManager    *manager.LockManager
}

// NewTransactionAdapter 创建事务适配器
func NewTransactionAdapter(storageManager *manager.StorageManager, lockManager *manager.LockManager) *TransactionAdapter {
	return &TransactionAdapter{
		storageManager: storageManager,
		lockManager:    lockManager,
	}
}

// Transaction 事务对象
type Transaction struct {
	TxnID          uint64
	ReadOnly       bool
	IsolationLevel string
	StartTime      int64
}

// BeginTransaction 开始事务
func (ta *TransactionAdapter) BeginTransaction(ctx context.Context, readOnly bool, isolationLevel string) (*Transaction, error) {
	logger.Debugf("Begin transaction: readOnly=%v, isolationLevel=%s", readOnly, isolationLevel)

	// 调用存储管理器的事务管理接口
	txnID, err := ta.storageManager.BeginTransaction()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return &Transaction{
		TxnID:          txnID,
		ReadOnly:       readOnly,
		IsolationLevel: isolationLevel,
		StartTime:      0,
	}, nil
}

// CommitTransaction 提交事务
func (ta *TransactionAdapter) CommitTransaction(ctx context.Context, txn *Transaction) error {
	// 验证事务对象
	if txn == nil {
		return fmt.Errorf("transaction is nil")
	}

	logger.Debugf("Commit transaction: txnID=%d", txn.TxnID)

	// 如果有存储管理器，调用其提交方法
	if ta.storageManager != nil {
		err := ta.storageManager.CommitTransaction(txn.TxnID)
		if err != nil {
			logger.Errorf("❌ Failed to commit transaction %d: %v", txn.TxnID, err)
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
	}

	// 如果有锁管理器，释放所有锁
	if ta.lockManager != nil {
		ta.lockManager.ReleaseLocks(txn.TxnID)
		logger.Debugf("✅ Released all locks for transaction %d", txn.TxnID)
	}

	logger.Debugf("✅ Transaction %d committed successfully", txn.TxnID)
	return nil
}

// RollbackTransaction 回滚事务
func (ta *TransactionAdapter) RollbackTransaction(ctx context.Context, txn *Transaction) error {
	// 验证事务对象
	if txn == nil {
		return fmt.Errorf("transaction is nil")
	}

	logger.Debugf("Rollback transaction: txnID=%d", txn.TxnID)

	// 如果有存储管理器，调用其回滚方法
	if ta.storageManager != nil {
		err := ta.storageManager.RollbackTransaction(txn.TxnID)
		if err != nil {
			logger.Errorf("❌ Failed to rollback transaction %d: %v", txn.TxnID, err)
			return fmt.Errorf("failed to rollback transaction: %w", err)
		}
	}

	// 如果有锁管理器，释放所有锁
	if ta.lockManager != nil {
		ta.lockManager.ReleaseLocks(txn.TxnID)
		logger.Debugf("✅ Released all locks for transaction %d", txn.TxnID)
	}

	logger.Debugf("✅ Transaction %d rolled back successfully", txn.TxnID)
	return nil
}

// AcquireLock 获取锁
func (ta *TransactionAdapter) AcquireLock(ctx context.Context, txn *Transaction, lockType string, resource string) error {
	logger.Debugf("Acquire lock: txnID=%d, lockType=%s, resource=%s", txn.TxnID, lockType, resource)

	// 验证参数
	if txn == nil {
		return fmt.Errorf("transaction is nil")
	}

	if resource == "" {
		return fmt.Errorf("resource cannot be empty")
	}

	// 如果没有锁管理器，直接返回成功（简化模式）
	if ta.lockManager == nil {
		logger.Debugf("⚠️ No lock manager available, skipping lock acquisition")
		return nil
	}

	// 解析资源ID格式: "tableID:pageID:rowID"
	// 例如: "1:100:5" 表示表1，页100，行5
	var tableID, pageID uint32
	var rowID uint64
	_, err := fmt.Sscanf(resource, "%d:%d:%d", &tableID, &pageID, &rowID)
	if err != nil {
		// 如果解析失败，尝试简化格式 "tableID:rowID"
		_, err = fmt.Sscanf(resource, "%d:%d", &tableID, &rowID)
		if err != nil {
			return fmt.Errorf("invalid resource format: %s", resource)
		}
		pageID = 0 // 默认页ID为0
	}

	// 将字符串锁类型转换为LockType
	var lt manager.LockType
	switch lockType {
	case "S", "SHARED", "shared":
		lt = manager.LOCK_S
	case "X", "EXCLUSIVE", "exclusive":
		lt = manager.LOCK_X
	default:
		return fmt.Errorf("unknown lock type: %s", lockType)
	}

	// 调用锁管理器获取锁
	err = ta.lockManager.AcquireLock(txn.TxnID, tableID, pageID, rowID, lt)
	if err != nil {
		logger.Debugf("❌ Failed to acquire %s lock on %s for transaction %d: %v",
			lockType, resource, txn.TxnID, err)
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	logger.Debugf("✅ Acquired %s lock on %s for transaction %d", lockType, resource, txn.TxnID)
	return nil
}

// ReleaseLock 释放锁
// 注意：当前LockManager实现只支持释放事务的所有锁（ReleaseLocks）
// 单个锁的释放功能需要在LockManager中实现
func (ta *TransactionAdapter) ReleaseLock(ctx context.Context, txn *Transaction, resource string) error {
	logger.Debugf("Release lock: txnID=%d, resource=%s", txn.TxnID, resource)

	// 验证参数
	if txn == nil {
		return fmt.Errorf("transaction is nil")
	}

	if resource == "" {
		return fmt.Errorf("resource cannot be empty")
	}

	// 如果没有锁管理器，直接返回成功（简化模式）
	if ta.lockManager == nil {
		logger.Debugf("⚠️ No lock manager available, skipping lock release")
		return nil
	}

	// 当前实现：释放事务的所有锁
	// TODO: 在LockManager中实现单个锁的释放功能
	logger.Debugf("⚠️ ReleaseLock currently releases all locks for transaction %d", txn.TxnID)
	ta.lockManager.ReleaseLocks(txn.TxnID)

	logger.Debugf("✅ Released locks for transaction %d", txn.TxnID)
	return nil
}
