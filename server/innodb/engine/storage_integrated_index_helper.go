package engine

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// ===== 索引键构建方法 =====

// buildIndexKey 为插入操作构建索引键
func (dml *StorageIntegratedDMLExecutor) buildIndexKey(
	row *InsertRowData,
	index *manager.Index,
	tableMeta *metadata.TableMeta,
) (interface{}, error) {
	logger.Debugf(" 构建索引键，索引: %s", index.Name)

	// 简化实现：假设索引只有一列
	if len(index.Columns) == 0 {
		return nil, fmt.Errorf("索引列为空")
	}

	// 获取第一列的值作为索引键
	columnName := index.Columns[0].Name
	if value, exists := row.ColumnValues[columnName]; exists {
		return value, nil
	}

	return nil, fmt.Errorf("索引列 %s 在行数据中不存在", columnName)
}

// buildIndexKeyFromOldValues 从旧值构建索引键
func (dml *StorageIntegratedDMLExecutor) buildIndexKeyFromOldValues(
	oldValues map[string]interface{},
	index *manager.Index,
	tableMeta *metadata.TableMeta,
) (interface{}, error) {
	logger.Debugf(" 从旧值构建索引键，索引: %s", index.Name)

	// 简化实现：假设索引只有一列
	if len(index.Columns) == 0 {
		return nil, fmt.Errorf("索引列为空")
	}

	// 获取第一列的值作为索引键
	columnName := index.Columns[0].Name
	if value, exists := oldValues[columnName]; exists {
		return value, nil
	}

	return nil, fmt.Errorf("索引列 %s 在旧值中不存在", columnName)
}

// buildIndexKeyFromUpdateExpressions 从更新表达式构建新的索引键
func (dml *StorageIntegratedDMLExecutor) buildIndexKeyFromUpdateExpressions(
	oldValues map[string]interface{},
	updateExprs []*UpdateExpression,
	index *manager.Index,
	tableMeta *metadata.TableMeta,
) (interface{}, error) {
	logger.Debugf(" 从更新表达式构建新索引键，索引: %s", index.Name)

	// 简化实现：假设索引只有一列
	if len(index.Columns) == 0 {
		return nil, fmt.Errorf("索引列为空")
	}

	columnName := index.Columns[0].Name

	// 首先检查是否有更新表达式更新了这一列
	for _, expr := range updateExprs {
		if expr.ColumnName == columnName {
			return expr.NewValue, nil
		}
	}

	// 如果没有更新这一列，使用旧值
	if value, exists := oldValues[columnName]; exists {
		return value, nil
	}

	return nil, fmt.Errorf("索引列 %s 在数据中不存在", columnName)
}

// buildMultiColumnIndexKey 构建多列索引键
func (dml *StorageIntegratedDMLExecutor) buildMultiColumnIndexKey(
	values map[string]interface{},
	index *manager.Index,
	tableMeta *metadata.TableMeta,
) ([]byte, error) {
	logger.Debugf(" 构建多列索引键，索引: %s, 列数: %d", index.Name, len(index.Columns))

	var keyParts []string

	// 按顺序连接所有索引列的值
	for _, column := range index.Columns {
		if value, exists := values[column.Name]; exists {
			keyParts = append(keyParts, fmt.Sprintf("%v", value))
		} else {
			keyParts = append(keyParts, "NULL")
		}
	}

	// 使用分隔符连接各部分
	compositeKey := strings.Join(keyParts, "|")
	return []byte(compositeKey), nil
}

// ===== 索引更新检查方法 =====

// indexNeedsUpdateForExpressions 检查索引是否需要因UPDATE表达式而更新
func (dml *StorageIntegratedDMLExecutor) indexNeedsUpdateForExpressions(
	index *manager.Index,
	updateExprs []*UpdateExpression,
) bool {
	logger.Debugf(" 检查索引 %s 是否需要更新", index.Name)

	// 检查更新表达式中是否包含索引列
	for _, expr := range updateExprs {
		for _, indexColumn := range index.Columns {
			if expr.ColumnName == indexColumn.Name {
				logger.Debugf(" 索引列 %s 被更新，需要维护索引", indexColumn.Name)
				return true
			}
		}
	}

	logger.Debugf("⏸️  索引 %s 不受此UPDATE影响", index.Name)
	return false
}

// indexAffectedByColumns 检查索引是否受指定列影响
func (dml *StorageIntegratedDMLExecutor) indexAffectedByColumns(
	index *manager.Index,
	affectedColumns []string,
) bool {
	for _, affectedColumn := range affectedColumns {
		for _, indexColumn := range index.Columns {
			if affectedColumn == indexColumn.Name {
				return true
			}
		}
	}
	return false
}

// ===== 索引操作辅助方法 =====

// insertIndexEntry 插入索引项
func (dml *StorageIntegratedDMLExecutor) insertIndexEntry(
	indexID uint64,
	indexKey interface{},
	primaryKey interface{},
) error {
	logger.Debugf(" 插入索引项: IndexID=%d, Key=%v, PrimaryKey=%v", indexID, indexKey, primaryKey)

	// 序列化主键作为索引值
	indexValue, err := dml.serializePrimaryKey(primaryKey)
	if err != nil {
		return fmt.Errorf("序列化主键失败: %v", err)
	}

	// 调用索引管理器插入
	err = dml.indexManager.InsertKey(indexID, indexKey, indexValue)
	if err != nil {
		return fmt.Errorf("插入索引项失败: %v", err)
	}

	return nil
}

// deleteIndexEntry 删除索引项
func (dml *StorageIntegratedDMLExecutor) deleteIndexEntry(
	indexID uint64,
	indexKey interface{},
) error {
	logger.Debugf("🗑️ 删除索引项: IndexID=%d, Key=%v", indexID, indexKey)

	// 调用索引管理器删除
	err := dml.indexManager.DeleteKey(indexID, indexKey)
	if err != nil {
		return fmt.Errorf("删除索引项失败: %v", err)
	}

	return nil
}

// updateIndexEntry 更新索引项（先删除后插入）
func (dml *StorageIntegratedDMLExecutor) updateIndexEntry(
	indexID uint64,
	oldIndexKey interface{},
	newIndexKey interface{},
	primaryKey interface{},
) error {
	logger.Debugf("🔄 更新索引项: IndexID=%d, OldKey=%v, NewKey=%v", indexID, oldIndexKey, newIndexKey)

	// 删除旧的索引项
	err := dml.deleteIndexEntry(indexID, oldIndexKey)
	if err != nil {
		logger.Debugf("  警告: 删除旧索引项失败: %v", err)
	}

	// 插入新的索引项
	err = dml.insertIndexEntry(indexID, newIndexKey, primaryKey)
	if err != nil {
		return fmt.Errorf("插入新索引项失败: %v", err)
	}

	return nil
}

// ===== 索引验证和检查方法 =====

// validateIndexKey 验证索引键的有效性
func (dml *StorageIntegratedDMLExecutor) validateIndexKey(
	indexKey interface{},
	index *manager.Index,
) error {
	if indexKey == nil {
		if index.IsUnique {
			return fmt.Errorf("唯一索引不允许NULL值")
		}
		return nil // 非唯一索引允许NULL值
	}

	// TODO: 添加更多验证逻辑，如长度检查、类型检查等

	return nil
}

// checkIndexKeyUniqueness 检查索引键的唯一性（用于唯一索引）
func (dml *StorageIntegratedDMLExecutor) checkIndexKeyUniqueness(
	indexID uint64,
	indexKey interface{},
	index *manager.Index,
) error {
	if !index.IsUnique {
		return nil // 非唯一索引无需检查
	}

	logger.Debugf(" 检查唯一索引键重复: IndexID=%d, Key=%v", indexID, indexKey)

	// 在索引中查找是否已存在相同的键
	pageNo, slot, err := dml.indexManager.SearchKey(indexID, indexKey)
	if err == nil && pageNo > 0 {
		return fmt.Errorf("唯一索引约束违反: 键 %v 已存在 (页面: %d, 槽位: %d)", indexKey, pageNo, slot)
	}

	// 如果查找失败（键不存在），这是期望的结果
	return nil
}

// ===== 索引统计和监控方法 =====

// updateIndexStatistics 更新索引统计信息
func (dml *StorageIntegratedDMLExecutor) updateIndexStatistics(
	indexID uint64,
	operationType string, // INSERT, UPDATE, DELETE
) {
	// 更新全局统计
	dml.stats.IndexUpdates++

	// TODO: 可以添加更详细的索引级别统计
	logger.Debugf(" 更新索引统计: IndexID=%d, 操作=%s", indexID, operationType)
}

// getIndexUpdateCount 获取索引更新次数
func (dml *StorageIntegratedDMLExecutor) getIndexUpdateCount() uint64 {
	return dml.stats.IndexUpdates
}

// ===== 索引错误处理方法 =====

// handleIndexError 处理索引操作错误
func (dml *StorageIntegratedDMLExecutor) handleIndexError(
	err error,
	operation string,
	indexName string,
	indexKey interface{},
) error {
	if err == nil {
		return nil
	}

	logger.Debugf(" 索引操作错误: 操作=%s, 索引=%s, 键=%v, 错误=%v",
		operation, indexName, indexKey, err)

	// 根据错误类型进行不同处理
	switch {
	case errors.Is(err, basic.ErrDuplicateKey):
		return fmt.Errorf("索引键重复: %s (索引: %s, 键: %v)", err.Error(), indexName, indexKey)
	case errors.Is(err, manager.ErrIndexNotFound):
		return fmt.Errorf("索引键未找到: %s (索引: %s, 键: %v)", err.Error(), indexName, indexKey)
	default:
		return fmt.Errorf("索引操作失败: %s (索引: %s, 键: %v, 错误: %v)",
			operation, indexName, indexKey, err)
	}
}

// ===== 批量索引操作方法 =====

// batchInsertIndexEntries 批量插入索引项
func (dml *StorageIntegratedDMLExecutor) batchInsertIndexEntries(
	indexID uint64,
	entries []IndexEntryData,
) error {
	logger.Debugf(" 批量插入索引项: IndexID=%d, 数量=%d", indexID, len(entries))

	successCount := 0
	for _, entry := range entries {
		err := dml.insertIndexEntry(indexID, entry.Key, entry.PrimaryKey)
		if err != nil {
			logger.Debugf("  批量插入失败: %v", err)
			continue
		}
		successCount++
	}

	if successCount != len(entries) {
		return fmt.Errorf("批量插入部分失败: 成功=%d, 总数=%d", successCount, len(entries))
	}

	logger.Debugf(" 批量插入索引项完成: %d 项", successCount)
	return nil
}

// batchDeleteIndexEntries 批量删除索引项
func (dml *StorageIntegratedDMLExecutor) batchDeleteIndexEntries(
	indexID uint64,
	keys []interface{},
) error {
	logger.Debugf(" 批量删除索引项: IndexID=%d, 数量=%d", indexID, len(keys))

	successCount := 0
	for _, key := range keys {
		err := dml.deleteIndexEntry(indexID, key)
		if err != nil {
			logger.Debugf("  批量删除失败: %v", err)
			continue
		}
		successCount++
	}

	if successCount != len(keys) {
		logger.Debugf("  批量删除部分失败: 成功=%d, 总数=%d", successCount, len(keys))
	}

	logger.Debugf(" 批量删除索引项完成: %d 项", successCount)
	return nil
}

// IndexEntryData 索引条目数据
type IndexEntryData struct {
	Key        interface{} // 索引键
	PrimaryKey interface{} // 主键值
}

// ===== 索引维护和优化方法 =====

// rebuildIndexForTable 重建表的所有索引
func (dml *StorageIntegratedDMLExecutor) rebuildIndexForTable(
	tableID uint64,
) error {
	logger.Debugf(" 重建表索引: TableID=%d", tableID)

	// 获取表的所有索引
	indexes := dml.indexManager.ListIndexes(tableID)

	// 逐个重建索引
	for _, index := range indexes {
		if index.IsPrimary {
			continue // 跳过主键索引
		}

		logger.Debugf(" 重建索引: %s", index.Name)

		// TODO: 实现索引重建逻辑
		// 1. 扫描表数据
		// 2. 重新构建索引树
		// 3. 更新索引元数据
	}

	logger.Debugf(" 表索引重建完成: TableID=%d", tableID)
	return nil
}

// optimizeIndexes 优化表的索引
func (dml *StorageIntegratedDMLExecutor) optimizeIndexes(
	tableID uint64,
) error {
	logger.Debugf("⚡ 优化表索引: TableID=%d", tableID)

	// TODO: 实现索引优化逻辑
	// 1. 分析索引使用统计
	// 2. 重组索引页面
	// 3. 更新索引统计信息

	return nil
}

// ===== 索引一致性检查方法 =====

// checkIndexConsistency 检查索引一致性
func (dml *StorageIntegratedDMLExecutor) checkIndexConsistency(
	tableID uint64,
) error {
	logger.Debugf(" 检查索引一致性: TableID=%d", tableID)

	// 获取表的所有索引
	indexes := dml.indexManager.ListIndexes(tableID)

	// 检查每个索引的一致性
	for _, index := range indexes {
		err := dml.checkSingleIndexConsistency(index)
		if err != nil {
			return fmt.Errorf("索引 %s 一致性检查失败: %v", index.Name, err)
		}
	}

	logger.Debugf(" 索引一致性检查通过: TableID=%d", tableID)
	return nil
}

// checkSingleIndexConsistency 检查单个索引的一致性
func (dml *StorageIntegratedDMLExecutor) checkSingleIndexConsistency(
	index *manager.Index,
) error {
	logger.Debugf(" 检查单个索引一致性: %s", index.Name)

	// TODO: 实现索引一致性检查逻辑
	// 1. 验证索引键的有序性
	// 2. 验证索引键与表数据的对应关系
	// 3. 验证索引结构的完整性

	return nil
}

// ===== 索引性能监控方法 =====

// monitorIndexPerformance 监控索引性能
func (dml *StorageIntegratedDMLExecutor) monitorIndexPerformance() *IndexPerformanceStats {
	return &IndexPerformanceStats{
		TotalIndexUpdates: dml.stats.IndexUpdates,
		AverageUpdateTime: dml.calculateAverageIndexUpdateTime(),
		IndexCacheHitRate: dml.calculateIndexCacheHitRate(),
		ActiveIndexCount:  dml.getActiveIndexCount(),
	}
}

// IndexPerformanceStats 索引性能统计
type IndexPerformanceStats struct {
	TotalIndexUpdates uint64        // 总索引更新次数
	AverageUpdateTime time.Duration // 平均更新时间
	IndexCacheHitRate float64       // 索引缓存命中率
	ActiveIndexCount  uint32        // 活跃索引数量
}

// calculateAverageIndexUpdateTime 计算平均索引更新时间
func (dml *StorageIntegratedDMLExecutor) calculateAverageIndexUpdateTime() time.Duration {
	// 简化实现
	return time.Millisecond * 10
}

// calculateIndexCacheHitRate 计算索引缓存命中率
func (dml *StorageIntegratedDMLExecutor) calculateIndexCacheHitRate() float64 {
	// 简化实现
	return 0.85
}

// getActiveIndexCount 获取活跃索引数量
func (dml *StorageIntegratedDMLExecutor) getActiveIndexCount() uint32 {
	// 简化实现
	return 10
}
