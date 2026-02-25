package engine

import (
	"context"
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// UnifiedExecutor 统一执行器，整合所有SQL执行入口
// 采用火山模型架构，支持SELECT、INSERT、UPDATE、DELETE
type UnifiedExecutor struct {
	// 适配器
	storageAdapter     *StorageAdapter
	indexAdapter       *IndexAdapter
	transactionAdapter *TransactionAdapter

	// 管理器（用于兼容性）
	tableManager        *manager.TableManager
	indexManager        *manager.IndexManager
	bufferPoolManager   *manager.OptimizedBufferPoolManager
	storageManager      *manager.StorageManager
	tableStorageManager *manager.TableStorageManager

	// 查询优化器
	optimizer interface{} // 可以是*plan.Optimizer或其他优化器实现
}

// NewUnifiedExecutor 创建统一执行器
func NewUnifiedExecutor(
	tableManager *manager.TableManager,
	indexManager *manager.IndexManager,
	bufferPoolManager *manager.OptimizedBufferPoolManager,
	storageManager *manager.StorageManager,
	tableStorageManager *manager.TableStorageManager,
) *UnifiedExecutor {
	// 创建适配器
	storageAdapter := NewStorageAdapter(
		tableManager,
		bufferPoolManager,
		storageManager,
		tableStorageManager,
	)

	indexAdapter := NewIndexAdapter(
		indexManager,
		nil, // btreeManager
		storageAdapter,
	)

	// 创建锁管理器（如果需要）
	lockManager := manager.NewLockManager()

	transactionAdapter := NewTransactionAdapter(storageManager, lockManager)

	return &UnifiedExecutor{
		storageAdapter:      storageAdapter,
		indexAdapter:        indexAdapter,
		transactionAdapter:  transactionAdapter,
		tableManager:        tableManager,
		indexManager:        indexManager,
		bufferPoolManager:   bufferPoolManager,
		storageManager:      storageManager,
		tableStorageManager: tableStorageManager,
	}
}

// ExecuteSelect 执行SELECT查询
func (ue *UnifiedExecutor) ExecuteSelect(ctx context.Context, stmt *sqlparser.Select, schemaName string) (*SelectResult, error) {
	logger.Debugf("UnifiedExecutor: executing SELECT query on schema %s", schemaName)

	// 1. 生成逻辑计划（简化版本，直接构建算子）
	// TODO: 集成查询优化器生成物理计划

	// 2. 构建算子树
	rootOperator, err := ue.buildSelectOperatorTree(ctx, stmt, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to build operator tree: %w", err)
	}

	// 3. 执行算子树
	if err := rootOperator.Open(ctx); err != nil {
		return nil, fmt.Errorf("failed to open operator: %w", err)
	}
	defer rootOperator.Close()

	// 4. 收集结果
	rows := [][]interface{}{}
	for {
		record, err := rootOperator.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch record: %w", err)
		}
		if record == nil {
			break // EOF
		}

		// 转换记录为结果行
		row := ue.recordToRow(record)
		rows = append(rows, row)
	}

	// 5. 构造结果
	return &SelectResult{
		Records:    []Record{}, // TODO: should be records from rootOperator
		RowCount:   len(rows),
		Columns:    []string{}, // TODO: Fix - need to get columns from schema, but schema type issue
		ResultType: "SELECT",
		Message:    "Success",
	}, nil
}

// ExecuteInsert 执行INSERT语句
func (ue *UnifiedExecutor) ExecuteInsert(ctx context.Context, stmt *sqlparser.Insert, schemaName string) (*DMLResult, error) {
	logger.Debugf("UnifiedExecutor: executing INSERT on schema %s", schemaName)

	// 获取表名
	tableName := stmt.Table.Name.String()

	// 创建插入算子
	insertOp := NewInsertOperator(
		schemaName,
		tableName,
		stmt,
		ue.storageAdapter,
		ue.indexAdapter,
		ue.transactionAdapter,
	)

	// 执行插入
	if err := insertOp.Open(ctx); err != nil {
		return nil, fmt.Errorf("failed to open insert operator: %w", err)
	}
	defer insertOp.Close()

	// 获取结果
	record, err := insertOp.Next(ctx)
	if err != nil {
		return nil, fmt.Errorf("insert failed: %w", err)
	}

	affectedRows := int(0)
	if record != nil {
		values := record.GetValues()
		if len(values) > 0 {
			affectedRows = int(values[0].Int())
		}
	}

	return &DMLResult{
		AffectedRows: affectedRows,
		Message:      fmt.Sprintf("INSERT executed, %d rows affected", affectedRows),
	}, nil
}

// ExecuteUpdate 执行UPDATE语句
func (ue *UnifiedExecutor) ExecuteUpdate(ctx context.Context, stmt *sqlparser.Update, schemaName string) (*DMLResult, error) {
	logger.Debugf("UnifiedExecutor: executing UPDATE on schema %s", schemaName)

	// 获取表名
	tableName := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()

	// 创建扫描算子（用于定位需要更新的记录）
	scanOp := NewTableScanOperator(schemaName, tableName, ue.storageAdapter)

	// TODO: 添加WHERE条件过滤算子

	// 创建更新算子
	updateOp := NewUpdateOperator(
		schemaName,
		tableName,
		stmt,
		ue.storageAdapter,
		ue.indexAdapter,
		ue.transactionAdapter,
		scanOp,
	)

	// 执行更新
	if err := updateOp.Open(ctx); err != nil {
		return nil, fmt.Errorf("failed to open update operator: %w", err)
	}
	defer updateOp.Close()

	// 获取结果
	record, err := updateOp.Next(ctx)
	if err != nil {
		return nil, fmt.Errorf("update failed: %w", err)
	}

	affectedRows := int(0)
	if record != nil {
		values := record.GetValues()
		if len(values) > 0 {
			affectedRows = int(values[0].Int())
		}
	}

	return &DMLResult{
		AffectedRows: affectedRows,
		Message:      fmt.Sprintf("UPDATE executed, %d rows affected", affectedRows),
	}, nil
}

// ExecuteDelete 执行DELETE语句
func (ue *UnifiedExecutor) ExecuteDelete(ctx context.Context, stmt *sqlparser.Delete, schemaName string) (*DMLResult, error) {
	logger.Debugf("UnifiedExecutor: executing DELETE on schema %s", schemaName)

	// 获取表名
	tableName := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()

	// 创建扫描算子（用于定位需要删除的记录）
	scanOp := NewTableScanOperator(schemaName, tableName, ue.storageAdapter)

	// TODO: 添加WHERE条件过滤算子

	// 创建删除算子
	deleteOp := NewDeleteOperator(
		schemaName,
		tableName,
		stmt,
		ue.storageAdapter,
		ue.indexAdapter,
		ue.transactionAdapter,
		scanOp,
	)

	// 执行删除
	if err := deleteOp.Open(ctx); err != nil {
		return nil, fmt.Errorf("failed to open delete operator: %w", err)
	}
	defer deleteOp.Close()

	// 获取结果
	record, err := deleteOp.Next(ctx)
	if err != nil {
		return nil, fmt.Errorf("delete failed: %w", err)
	}

	affectedRows := int(0)
	if record != nil {
		values := record.GetValues()
		if len(values) > 0 {
			affectedRows = int(values[0].Int())
		}
	}

	return &DMLResult{
		AffectedRows: affectedRows,
		Message:      fmt.Sprintf("DELETE executed, %d rows affected", affectedRows),
	}, nil
}

// BuildOperatorTree 构建算子树（通用接口）
func (ue *UnifiedExecutor) BuildOperatorTree(ctx context.Context, physicalPlan *plan.PhysicalPlan) (Operator, error) {
	// TODO: 根据物理计划构建算子树
	// 这个方法用于集成查询优化器
	return nil, fmt.Errorf("not implemented")
}

// buildSelectOperatorTree 构建SELECT查询的算子树
func (ue *UnifiedExecutor) buildSelectOperatorTree(ctx context.Context, stmt *sqlparser.Select, schemaName string) (Operator, error) {
	// 1. 解析FROM子句，确定表名
	if len(stmt.From) == 0 {
		return nil, fmt.Errorf("no table specified in FROM clause")
	}

	tableExpr := stmt.From[0].(*sqlparser.AliasedTableExpr)
	tableName := tableExpr.Expr.(sqlparser.TableName).Name.String()

	// 2. 创建基础扫描算子
	var scanOp Operator = NewTableScanOperator(schemaName, tableName, ue.storageAdapter)

	// 3. 添加WHERE过滤算子
	if stmt.Where != nil {
		// TODO: 解析WHERE条件并创建FilterOperator
		// predicate := ue.buildPredicate(stmt.Where)
		// scanOp = NewFilterOperator(scanOp, predicate)
	}

	// 4. 添加投影算子（SELECT子句）
	if len(stmt.SelectExprs) > 0 {
		// TODO: 解析SELECT表达式并创建ProjectionOperator
		// projections := ue.buildProjections(stmt.SelectExprs)
		// scanOp = NewProjectionOperator(scanOp, projections)
	}

	// 5. 添加ORDER BY排序算子
	if len(stmt.OrderBy) > 0 {
		// TODO: 创建SortOperator
	}

	// 6. 添加LIMIT算子
	if stmt.Limit != nil {
		// TODO: 创建LimitOperator
	}

	return scanOp, nil
}

// recordToRow 将Record转换为结果行
func (ue *UnifiedExecutor) recordToRow(record Record) []interface{} {
	values := record.GetValues()
	row := make([]interface{}, len(values))
	for i, val := range values {
		row[i] = val.Raw()
	}
	return row
}

// getColumnNames 获取列名
func (ue *UnifiedExecutor) getColumnNames(schema *metadata.Table) []string {
	if schema == nil || len(schema.Columns) == 0 {
		return []string{}
	}

	names := make([]string, len(schema.Columns))
	for i, col := range schema.Columns {
		names[i] = col.Name
	}
	return names
}

// Note: SelectResult is defined in select_executor.go
// Note: DMLResult is defined in dml_executor.go
