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

	i.schema = &metadata.Schema{
		Columns: tableMetadata.Schema.Columns,
	}

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
		basic.NewInt64(affectedRows),
	}

	return NewExecutorRecordFromValues(values, nil), nil
}

// executeInsert 执行实际的插入逻辑
func (i *InsertOperator) executeInsert(ctx context.Context, txn *Transaction) (int64, error) {
	// TODO: 实现实际的插入逻辑
	// 1. 解析INSERT语句中的值
	// 2. 验证数据
	// 3. 插入到聚簇索引（主键索引）
	// 4. 更新二级索引

	logger.Debugf("Executing INSERT on table %s.%s", i.schemaName, i.tableName)

	// 临时实现：返回模拟的影响行数
	return 1, nil
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

	u.schema = &metadata.Schema{
		Columns: tableMetadata.Schema.Columns,
	}

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
		basic.NewInt64(affectedRows),
	}

	return NewExecutorRecordFromValues(values, nil), nil
}

// executeUpdate 执行实际的更新逻辑
func (u *UpdateOperator) executeUpdate(ctx context.Context, txn *Transaction) (int64, error) {
	// TODO: 实现实际的更新逻辑
	// 1. 使用scanOperator扫描需要更新的记录
	// 2. 对每条记录应用UPDATE的SET子句
	// 3. 检查索引列是否变更
	//    - 如果索引列未变更：就地更新
	//    - 如果索引列变更：删除旧记录+插入新记录
	// 4. 更新二级索引

	logger.Debugf("Executing UPDATE on table %s.%s", u.schemaName, u.tableName)

	affectedRows := int64(0)

	// 扫描需要更新的记录
	for {
		record, err := u.scanOperator.Next(ctx)
		if err != nil {
			return 0, err
		}
		if record == nil {
			break // EOF
		}

		// 更新记录
		// TODO: 实现记录更新逻辑
		affectedRows++
	}

	return affectedRows, nil
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

	d.schema = &metadata.Schema{
		Columns: tableMetadata.Schema.Columns,
	}

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
		basic.NewInt64(affectedRows),
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
