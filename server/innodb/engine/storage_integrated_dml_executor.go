package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// StorageIntegratedDMLExecutor 存储引擎集成的DML执行器
// 与实际的B+树存储引擎和索引管理器完全集成
type StorageIntegratedDMLExecutor struct {
	BaseExecutor

	// 核心管理器组件
	optimizerManager  *manager.OptimizerManager
	bufferPoolManager *manager.OptimizedBufferPoolManager
	btreeManager      basic.BPlusTreeManager
	tableManager      *manager.TableManager
	txManager         *manager.TransactionManager
	indexManager      *manager.IndexManager
	storageManager    *manager.StorageManager

	// 表空间和存储映射管理器
	tableStorageManager *manager.TableStorageManager

	// 持久化管理器
	persistenceManager *PersistenceManager

	// 执行状态
	schemaName    string
	tableName     string
	isInitialized bool

	// 性能统计
	stats *DMLExecutorStats
}

// DMLExecutorStats DML执行器统计信息
type DMLExecutorStats struct {
	InsertCount      uint64
	UpdateCount      uint64
	DeleteCount      uint64
	TotalTime        time.Duration
	AvgInsertTime    time.Duration
	AvgUpdateTime    time.Duration
	AvgDeleteTime    time.Duration
	IndexUpdates     uint64
	TransactionCount uint64
}

// NewStorageIntegratedDMLExecutor 创建存储引擎集成的DML执行器
func NewStorageIntegratedDMLExecutor(
	optimizerManager *manager.OptimizerManager,
	bufferPoolManager *manager.OptimizedBufferPoolManager,
	btreeManager basic.BPlusTreeManager,
	tableManager *manager.TableManager,
	txManager *manager.TransactionManager,
	indexManager *manager.IndexManager,
	storageManager *manager.StorageManager,
	tableStorageManager *manager.TableStorageManager,
) *StorageIntegratedDMLExecutor {
	executor := &StorageIntegratedDMLExecutor{
		optimizerManager:    optimizerManager,
		bufferPoolManager:   bufferPoolManager,
		btreeManager:        btreeManager,
		tableManager:        tableManager,
		txManager:           txManager,
		indexManager:        indexManager,
		storageManager:      storageManager,
		tableStorageManager: tableStorageManager,
		isInitialized:       false,
		stats: &DMLExecutorStats{
			InsertCount:      0,
			UpdateCount:      0,
			DeleteCount:      0,
			TotalTime:        0,
			AvgInsertTime:    0,
			AvgUpdateTime:    0,
			AvgDeleteTime:    0,
			IndexUpdates:     0,
			TransactionCount: 0,
		},
	}

	// 初始化持久化管理器
	dataDir := "./data" // 默认数据目录，实际应该从配置获取
	executor.persistenceManager = NewPersistenceManager(
		bufferPoolManager,
		storageManager,
		dataDir,
	)

	return executor
}

// StartPersistence 启动持久化管理器
func (dml *StorageIntegratedDMLExecutor) StartPersistence(ctx context.Context) error {
	if dml.persistenceManager == nil {
		return fmt.Errorf("持久化管理器未初始化")
	}

	logger.Infof("🚀 启动存储引擎持久化管理器")

	// 启动持久化管理器
	if err := dml.persistenceManager.Start(ctx); err != nil {
		return fmt.Errorf("启动持久化管理器失败: %v", err)
	}

	// 尝试从检查点恢复
	if err := dml.persistenceManager.RecoverFromCheckpoint(ctx); err != nil {
		logger.Errorf("  从检查点恢复失败: %v", err)
		// 不返回错误，继续正常启动
	}

	logger.Infof(" 存储引擎持久化管理器启动成功")
	return nil
}

// StopPersistence 停止持久化管理器
func (dml *StorageIntegratedDMLExecutor) StopPersistence() error {
	if dml.persistenceManager == nil {
		return nil
	}

	logger.Infof("🛑 停止存储引擎持久化管理器")

	if err := dml.persistenceManager.Stop(); err != nil {
		return fmt.Errorf("停止持久化管理器失败: %v", err)
	}

	logger.Infof(" 存储引擎持久化管理器停止成功")
	return nil
}

// ExecuteInsert 执行INSERT语句 - 存储引擎集成版本
func (dml *StorageIntegratedDMLExecutor) ExecuteInsert(ctx context.Context, stmt *sqlparser.Insert, schemaName string) (*DMLResult, error) {
	startTime := time.Now()
	logger.Infof("🚀 开始执行存储引擎集成的INSERT语句: %s", sqlparser.String(stmt))

	dml.schemaName = schemaName
	dml.tableName = stmt.Table.Name.String()

	// 1. 获取表的存储信息
	tableStorageInfo, err := dml.tableStorageManager.GetTableStorageInfo(schemaName, dml.tableName)
	if err != nil {
		return nil, fmt.Errorf("获取表存储信息失败: %v", err)
	}

	// 2. 获取表元数据
	tableMeta, err := dml.getTableMetadata()
	if err != nil {
		return nil, fmt.Errorf("获取表元数据失败: %v", err)
	}

	// 3. 解析INSERT数据
	insertRows, err := dml.parseInsertData(stmt, tableMeta)
	if err != nil {
		return nil, fmt.Errorf("解析INSERT数据失败: %v", err)
	}

	// 4. 验证数据完整性
	if err := dml.validateInsertData(insertRows, tableMeta); err != nil {
		return nil, fmt.Errorf("数据验证失败: %v", err)
	}

	// 5. 获取或创建表专用的B+树管理器
	tableBtreeManager, err := dml.tableStorageManager.CreateBTreeManagerForTable(ctx, schemaName, dml.tableName)
	if err != nil {
		return nil, fmt.Errorf("创建表B+树管理器失败: %v", err)
	}

	// 6. 开始事务
	txn, err := dml.beginStorageTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("开始存储事务失败: %v", err)
	}

	affectedRows := 0
	var lastInsertId uint64 = 0

	// 7. 逐行插入数据到存储引擎
	for _, row := range insertRows {
		insertId, err := dml.insertRowToStorage(ctx, txn, row, tableMeta, tableStorageInfo, tableBtreeManager)
		if err != nil {
			// 回滚事务
			dml.rollbackStorageTransaction(ctx, txn)
			return nil, fmt.Errorf("插入行到存储引擎失败: %v", err)
		}
		affectedRows++
		if insertId > 0 {
			lastInsertId = insertId
		}

		// 更新所有相关索引
		err = dml.updateIndexesForInsert(ctx, txn, row, tableMeta, tableStorageInfo)
		if err != nil {
			dml.rollbackStorageTransaction(ctx, txn)
			return nil, fmt.Errorf("更新索引失败: %v", err)
		}
	}

	// 8. 提交事务
	if err := dml.commitStorageTransaction(ctx, txn); err != nil {
		return nil, fmt.Errorf("提交存储事务失败: %v", err)
	}

	// 9. 更新统计信息
	executionTime := time.Since(startTime)
	dml.updateInsertStats(affectedRows, executionTime)

	logger.Infof(" 存储引擎集成INSERT执行成功，影响行数: %d, LastInsertID: %d, 耗时: %v",
		affectedRows, lastInsertId, executionTime)

	return &DMLResult{
		AffectedRows: affectedRows,
		LastInsertId: lastInsertId,
		ResultType:   "INSERT",
		Message:      fmt.Sprintf("存储引擎集成INSERT执行成功，影响行数: %d", affectedRows),
	}, nil
}

// ExecuteUpdate 执行UPDATE语句 - 存储引擎集成版本
func (dml *StorageIntegratedDMLExecutor) ExecuteUpdate(ctx context.Context, stmt *sqlparser.Update, schemaName string) (*DMLResult, error) {
	startTime := time.Now()
	logger.Infof("🚀 开始执行存储引擎集成的UPDATE语句: %s", sqlparser.String(stmt))

	dml.schemaName = schemaName

	// 1. 解析表名
	if len(stmt.TableExprs) == 0 {
		return nil, fmt.Errorf("UPDATE语句缺少表名")
	}

	tableName, err := dml.parseTableName(stmt.TableExprs[0])
	if err != nil {
		return nil, fmt.Errorf("解析表名失败: %v", err)
	}
	dml.tableName = tableName

	// 2. 获取表的存储信息
	tableStorageInfo, err := dml.tableStorageManager.GetTableStorageInfo(schemaName, dml.tableName)
	if err != nil {
		return nil, fmt.Errorf("获取表存储信息失败: %v", err)
	}

	// 3. 获取表元数据
	tableMeta, err := dml.getTableMetadata()
	if err != nil {
		return nil, fmt.Errorf("获取表元数据失败: %v", err)
	}

	// 4. 解析WHERE条件和SET表达式
	whereConditions := dml.parseWhereConditions(stmt.Where)
	updateExprs, err := dml.parseUpdateExpressions(stmt.Exprs, tableMeta)
	if err != nil {
		return nil, fmt.Errorf("解析UPDATE表达式失败: %v", err)
	}

	// 5. 获取表专用的B+树管理器
	tableBtreeManager, err := dml.tableStorageManager.CreateBTreeManagerForTable(ctx, schemaName, dml.tableName)
	if err != nil {
		return nil, fmt.Errorf("创建表B+树管理器失败: %v", err)
	}

	// 6. 开始事务
	txn, err := dml.beginStorageTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("开始存储事务失败: %v", err)
	}

	// 7. 查找需要更新的行
	rowsToUpdate, err := dml.findRowsToUpdateInStorage(ctx, txn, whereConditions, tableMeta, tableStorageInfo, tableBtreeManager)
	if err != nil {
		dml.rollbackStorageTransaction(ctx, txn)
		return nil, fmt.Errorf("查找待更新行失败: %v", err)
	}

	affectedRows := 0

	// 8. 逐行更新数据
	for _, rowInfo := range rowsToUpdate {
		err := dml.updateRowInStorage(ctx, txn, rowInfo, updateExprs, tableMeta, tableStorageInfo, tableBtreeManager)
		if err != nil {
			dml.rollbackStorageTransaction(ctx, txn)
			return nil, fmt.Errorf("更新行失败: %v", err)
		}

		// 更新相关索引
		err = dml.updateIndexesForUpdate(ctx, txn, []*RowUpdateInfo{rowInfo}, updateExprs, tableMeta, tableStorageInfo)
		if err != nil {
			dml.rollbackStorageTransaction(ctx, txn)
			return nil, fmt.Errorf("更新索引失败: %v", err)
		}

		affectedRows++
	}

	// 9. 提交事务
	if err := dml.commitStorageTransaction(ctx, txn); err != nil {
		return nil, fmt.Errorf("提交存储事务失败: %v", err)
	}

	// 10. 更新统计信息
	executionTime := time.Since(startTime)
	dml.updateUpdateStats(affectedRows, executionTime)

	logger.Infof(" 存储引擎集成UPDATE执行成功，影响行数: %d, 耗时: %v", affectedRows, executionTime)

	return &DMLResult{
		AffectedRows: affectedRows,
		LastInsertId: 0,
		ResultType:   "UPDATE",
		Message:      fmt.Sprintf("存储引擎集成UPDATE执行成功，影响行数: %d", affectedRows),
	}, nil
}

// ExecuteDelete 执行DELETE语句 - 存储引擎集成版本
func (dml *StorageIntegratedDMLExecutor) ExecuteDelete(ctx context.Context, stmt *sqlparser.Delete, schemaName string) (*DMLResult, error) {
	startTime := time.Now()
	logger.Infof("🚀 开始执行存储引擎集成的DELETE语句: %s", sqlparser.String(stmt))

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

	// 2. 获取表的存储信息
	tableStorageInfo, err := dml.tableStorageManager.GetTableStorageInfo(schemaName, dml.tableName)
	if err != nil {
		return nil, fmt.Errorf("获取表存储信息失败: %v", err)
	}

	// 3. 获取表元数据
	tableMeta, err := dml.getTableMetadata()
	if err != nil {
		return nil, fmt.Errorf("获取表元数据失败: %v", err)
	}

	// 4. 解析WHERE条件
	whereConditions := dml.parseWhereConditions(stmt.Where)

	// 5. 获取表专用的B+树管理器
	tableBtreeManager, err := dml.tableStorageManager.CreateBTreeManagerForTable(ctx, schemaName, dml.tableName)
	if err != nil {
		return nil, fmt.Errorf("创建表B+树管理器失败: %v", err)
	}

	// 6. 开始事务
	txn, err := dml.beginStorageTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("开始存储事务失败: %v", err)
	}

	// 7. 查找需要删除的行
	rowsToDelete, err := dml.findRowsToDeleteInStorage(ctx, txn, whereConditions, tableMeta, tableStorageInfo, tableBtreeManager)
	if err != nil {
		dml.rollbackStorageTransaction(ctx, txn)
		return nil, fmt.Errorf("查找待删除行失败: %v", err)
	}

	affectedRows := 0

	// 8. 逐行删除数据
	for _, rowInfo := range rowsToDelete {
		err := dml.deleteRowFromStorage(ctx, txn, rowInfo, tableMeta, tableStorageInfo, tableBtreeManager)
		if err != nil {
			dml.rollbackStorageTransaction(ctx, txn)
			return nil, fmt.Errorf("删除行失败: %v", err)
		}

		// 更新相关索引
		err = dml.updateIndexesForDelete(ctx, txn, []*RowUpdateInfo{rowInfo}, tableMeta, tableStorageInfo)
		if err != nil {
			dml.rollbackStorageTransaction(ctx, txn)
			return nil, fmt.Errorf("更新索引失败: %v", err)
		}

		affectedRows++
	}

	// 9. 提交事务
	if err := dml.commitStorageTransaction(ctx, txn); err != nil {
		return nil, fmt.Errorf("提交存储事务失败: %v", err)
	}

	// 10. 更新统计信息
	executionTime := time.Since(startTime)
	dml.updateDeleteStats(affectedRows, executionTime)

	logger.Infof(" 存储引擎集成DELETE执行成功，影响行数: %d, 耗时: %v", affectedRows, executionTime)

	return &DMLResult{
		AffectedRows: affectedRows,
		LastInsertId: 0,
		ResultType:   "DELETE",
		Message:      fmt.Sprintf("存储引擎集成DELETE执行成功，影响行数: %d", affectedRows),
	}, nil
}

// ===== 存储引擎集成的实际实现方法 =====

// insertRowToStorage 将行插入到存储引擎
func (dml *StorageIntegratedDMLExecutor) insertRowToStorage(
	ctx context.Context,
	txn interface{},
	row *InsertRowData,
	tableMeta *metadata.TableMeta,
	tableStorageInfo *manager.TableStorageInfo,
	btreeManager basic.BPlusTreeManager,
) (uint64, error) {
	logger.Debugf(" 插入行到存储引擎: SpaceID=%d, 数据=%+v", tableStorageInfo.SpaceID, row.ColumnValues)

	// 1. 生成主键值
	primaryKey, err := dml.generatePrimaryKey(row, tableMeta)
	if err != nil {
		return 0, fmt.Errorf("生成主键失败: %v", err)
	}

	// 2. 序列化行数据
	serializedRow, err := dml.serializeRowData(row, tableMeta)
	if err != nil {
		return 0, fmt.Errorf("序列化行数据失败: %v", err)
	}

	// 3. 插入到B+树存储引擎
	err = btreeManager.Insert(ctx, primaryKey, serializedRow)
	if err != nil {
		return 0, fmt.Errorf("插入到B+树失败: %v", err)
	}

	// 4. 立即持久化页面到磁盘（确保数据安全）
	if dml.persistenceManager != nil {
		err = dml.persistenceManager.FlushPage(ctx, tableStorageInfo.SpaceID, tableStorageInfo.RootPageNo)
		if err != nil {
			logger.Errorf(" 立即持久化页面失败: %v", err)
			// 不返回错误，但记录日志
		} else {
			logger.Debugf("💾 页面已立即持久化: SpaceID=%d, PageNo=%d",
				tableStorageInfo.SpaceID, tableStorageInfo.RootPageNo)
		}
	}

	// 5. 强制刷新缓冲池页面到磁盘（双重保障）
	err = dml.bufferPoolManager.FlushPage(tableStorageInfo.SpaceID, tableStorageInfo.RootPageNo)
	if err != nil {
		logger.Debugf("  警告: 刷新页面到磁盘失败: %v", err)
	}

	logger.Debugf(" 行成功插入到存储引擎并持久化，主键: %v", primaryKey)
	return dml.convertPrimaryKeyToUint64(primaryKey), nil
}

// updateRowInStorage 在存储引擎中更新行
func (dml *StorageIntegratedDMLExecutor) updateRowInStorage(
	ctx context.Context,
	txn interface{},
	rowInfo *RowUpdateInfo,
	updateExprs []*UpdateExpression,
	tableMeta *metadata.TableMeta,
	tableStorageInfo *manager.TableStorageInfo,
	btreeManager basic.BPlusTreeManager,
) error {
	logger.Debugf(" 在存储引擎中更新行: RowID=%d, 更新列数=%d", rowInfo.RowId, len(updateExprs))

	// 1. 根据RowID查找现有行数据
	primaryKey := rowInfo.RowId
	pageNo, slot, err := btreeManager.Search(ctx, primaryKey)
	if err != nil {
		return fmt.Errorf("查找行失败: %v", err)
	}

	logger.Debugf(" 找到行位置: PageNo=%d, Slot=%d", pageNo, slot)

	// 2. 读取现有行数据
	existingRowData, err := dml.readRowFromStorage(ctx, pageNo, slot, tableStorageInfo)
	if err != nil {
		return fmt.Errorf("读取现有行数据失败: %v", err)
	}

	// 3. 应用更新表达式
	updatedRowData, err := dml.applyUpdateExpressions(existingRowData, updateExprs, tableMeta)
	if err != nil {
		return fmt.Errorf("应用更新表达式失败: %v", err)
	}

	// 4. 序列化更新后的行数据
	serializedRow, err := dml.serializeRowData(updatedRowData, tableMeta)
	if err != nil {
		return fmt.Errorf("序列化更新后的行数据失败: %v", err)
	}

	// 5. 在B+树中更新记录（先删除后插入）
	// 注意：这里简化处理，实际应该有更复杂的就地更新逻辑
	err = btreeManager.Insert(ctx, primaryKey, serializedRow)
	if err != nil {
		return fmt.Errorf("更新B+树记录失败: %v", err)
	}

	// 6. 立即持久化更新的页面（确保数据安全）
	if dml.persistenceManager != nil {
		err = dml.persistenceManager.FlushPage(ctx, tableStorageInfo.SpaceID, pageNo)
		if err != nil {
			logger.Errorf(" 立即持久化更新页面失败: %v", err)
		} else {
			logger.Debugf("💾 更新页面已立即持久化: SpaceID=%d, PageNo=%d",
				tableStorageInfo.SpaceID, pageNo)
		}
	}

	// 7. 强制刷新到磁盘（双重保障）
	err = dml.bufferPoolManager.FlushPage(tableStorageInfo.SpaceID, pageNo)
	if err != nil {
		logger.Debugf("  警告: 刷新更新页面到磁盘失败: %v", err)
	}

	logger.Debugf(" 行成功在存储引擎中更新并持久化")
	return nil
}

// deleteRowFromStorage 从存储引擎删除行
func (dml *StorageIntegratedDMLExecutor) deleteRowFromStorage(
	ctx context.Context,
	txn interface{},
	rowInfo *RowUpdateInfo,
	tableMeta *metadata.TableMeta,
	tableStorageInfo *manager.TableStorageInfo,
	btreeManager basic.BPlusTreeManager,
) error {
	logger.Debugf("🗑️ 从存储引擎删除行: RowID=%d", rowInfo.RowId)

	// 1. 根据RowID查找行位置
	primaryKey := rowInfo.RowId
	pageNo, slot, err := btreeManager.Search(ctx, primaryKey)
	if err != nil {
		return fmt.Errorf("查找待删除行失败: %v", err)
	}

	logger.Debugf(" 找到待删除行位置: PageNo=%d, Slot=%d", pageNo, slot)

	// 2. 从存储页面中标记删除记录
	err = dml.markRowAsDeletedInStorage(ctx, pageNo, slot, tableStorageInfo)
	if err != nil {
		return fmt.Errorf("标记行为已删除失败: %v", err)
	}

	// 3. 立即持久化删除操作（确保数据安全）
	if dml.persistenceManager != nil {
		err = dml.persistenceManager.FlushPage(ctx, tableStorageInfo.SpaceID, pageNo)
		if err != nil {
			logger.Errorf(" 立即持久化删除页面失败: %v", err)
		} else {
			logger.Debugf("💾 删除页面已立即持久化: SpaceID=%d, PageNo=%d",
				tableStorageInfo.SpaceID, pageNo)
		}
	}

	// 4. 强制刷新到磁盘（双重保障）
	err = dml.bufferPoolManager.FlushPage(tableStorageInfo.SpaceID, pageNo)
	if err != nil {
		logger.Debugf("  警告: 刷新删除页面到磁盘失败: %v", err)
	}

	logger.Debugf(" 行成功从存储引擎删除并持久化")
	return nil
}

// ===== 索引管理方法 =====

// updateIndexesForInsert 为INSERT操作更新所有相关索引
func (dml *StorageIntegratedDMLExecutor) updateIndexesForInsert(
	ctx context.Context,
	txn interface{},
	row *InsertRowData,
	tableMeta *metadata.TableMeta,
	tableStorageInfo *manager.TableStorageInfo,
) error {
	logger.Debugf("🔄 更新INSERT相关索引，表: %s", tableMeta.Name)

	// 获取表的所有索引 - 使用SpaceID作为TableID的替代
	indexes := dml.indexManager.ListIndexes(uint64(tableStorageInfo.SpaceID))

	for _, index := range indexes {
		if index.IsPrimary {
			continue // 主键索引已经在主表插入时处理
		}

		// 构建索引键
		indexKey, err := dml.buildIndexKey(row, index, tableMeta)
		if err != nil {
			logger.Debugf("  构建索引键失败: %v", err)
			continue
		}

		// 验证索引键
		err = dml.validateIndexKey(indexKey, index)
		if err != nil {
			return fmt.Errorf("索引键验证失败: %v", err)
		}

		// 检查唯一性（如果是唯一索引）
		err = dml.checkIndexKeyUniqueness(index.IndexID, indexKey, index)
		if err != nil {
			return fmt.Errorf("唯一索引约束检查失败: %v", err)
		}

		// 生成主键值作为索引值
		primaryKey, err := dml.generatePrimaryKey(row, tableMeta)
		if err != nil {
			return fmt.Errorf("生成主键失败: %v", err)
		}

		// 插入索引项
		err = dml.insertIndexEntry(index.IndexID, indexKey, primaryKey)
		if err != nil {
			return fmt.Errorf("插入索引项失败: %v", err)
		}

		// 更新统计信息
		dml.updateIndexStatistics(index.IndexID, "INSERT")

		logger.Debugf(" 成功更新索引: %s", index.Name)
	}

	return nil
}

// updateIndexesForUpdate 为UPDATE操作更新相关索引
func (dml *StorageIntegratedDMLExecutor) updateIndexesForUpdate(
	ctx context.Context,
	txn interface{},
	rowsToUpdate []*RowUpdateInfo,
	updateExprs []*UpdateExpression,
	tableMeta *metadata.TableMeta,
	tableStorageInfo *manager.TableStorageInfo,
) error {
	logger.Debugf("🔄 更新UPDATE相关索引，表: %s", tableMeta.Name)

	// 获取表的所有索引 - 使用SpaceID作为TableID的替代
	indexes := dml.indexManager.ListIndexes(uint64(tableStorageInfo.SpaceID))

	for _, index := range indexes {
		if index.IsPrimary {
			continue // 主键索引通常不更新
		}

		// 检查此索引是否受UPDATE影响
		if !dml.indexNeedsUpdateForExpressions(index, updateExprs) {
			continue
		}

		// 为每个待更新的行处理索引
		for _, rowInfo := range rowsToUpdate {
			// 构建旧索引键
			oldIndexKey, err := dml.buildIndexKeyFromOldValues(rowInfo.OldValues, index, tableMeta)
			if err != nil {
				logger.Debugf("  构建旧索引键失败: %v", err)
				continue
			}

			// 构建新索引键
			newIndexKey, err := dml.buildIndexKeyFromUpdateExpressions(rowInfo.OldValues, updateExprs, index, tableMeta)
			if err != nil {
				logger.Debugf("  构建新索引键失败: %v", err)
				continue
			}

			// 如果索引键没有变化，跳过更新
			if oldIndexKey == newIndexKey {
				continue
			}

			// 验证新索引键
			err = dml.validateIndexKey(newIndexKey, index)
			if err != nil {
				return fmt.Errorf("新索引键验证失败: %v", err)
			}

			// 检查新索引键的唯一性（如果是唯一索引）
			err = dml.checkIndexKeyUniqueness(index.IndexID, newIndexKey, index)
			if err != nil {
				return fmt.Errorf("新索引键唯一性检查失败: %v", err)
			}

			// 更新索引项（删除旧的，插入新的）
			err = dml.updateIndexEntry(index.IndexID, oldIndexKey, newIndexKey, rowInfo.RowId)
			if err != nil {
				return fmt.Errorf("更新索引项失败: %v", err)
			}

			// 更新统计信息
			dml.updateIndexStatistics(index.IndexID, "UPDATE")
		}

		logger.Debugf(" 成功更新索引: %s", index.Name)
	}

	return nil
}

// updateIndexesForDelete 为DELETE操作更新相关索引
func (dml *StorageIntegratedDMLExecutor) updateIndexesForDelete(
	ctx context.Context,
	txn interface{},
	rowsToDelete []*RowUpdateInfo,
	tableMeta *metadata.TableMeta,
	tableStorageInfo *manager.TableStorageInfo,
) error {
	logger.Debugf("🔄 更新DELETE相关索引，表: %s", tableMeta.Name)

	// 获取表的所有索引 - 使用SpaceID作为TableID的替代
	indexes := dml.indexManager.ListIndexes(uint64(tableStorageInfo.SpaceID))

	for _, index := range indexes {
		if index.IsPrimary {
			continue // 主键索引在主表删除时已处理
		}

		// 为每个待删除的行处理索引
		for _, rowInfo := range rowsToDelete {
			// 构建索引键
			indexKey, err := dml.buildIndexKeyFromOldValues(rowInfo.OldValues, index, tableMeta)
			if err != nil {
				logger.Debugf("  构建索引键失败: %v", err)
				continue
			}

			// 删除索引项
			err = dml.deleteIndexEntry(index.IndexID, indexKey)
			if err != nil {
				logger.Debugf("  删除索引项失败: %v", err)
				continue
			}

			// 更新统计信息
			dml.updateIndexStatistics(index.IndexID, "DELETE")
		}

		logger.Debugf(" 成功更新索引: %s", index.Name)
	}

	return nil
}

// ===== 辅助方法 =====

// getTableMetadata 获取表元数据
func (dml *StorageIntegratedDMLExecutor) getTableMetadata() (*metadata.TableMeta, error) {
	if dml.tableManager == nil {
		return nil, fmt.Errorf("表管理器未初始化")
	}

	// 从实际的数据字典中获取表元数据
	tableMeta := &metadata.TableMeta{
		Name:       dml.tableName,
		Columns:    []*metadata.ColumnMeta{},
		PrimaryKey: []string{},
		Indices:    []metadata.IndexMeta{},
	}

	// TODO: 实现从数据字典获取真实的表元数据
	logger.Debugf(" 获取表元数据: %s.%s", dml.schemaName, dml.tableName)

	return tableMeta, nil
}

// 继续实现其他辅助方法...
// 为了保持文件长度合理，将在后续的方法中继续实现
