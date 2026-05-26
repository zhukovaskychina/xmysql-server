package engine

import (
	"context"
	"fmt"
	"strconv"
	"strings"

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

	if ue.optimizer != nil && ue.tableManager != nil {
		logicalPlan, err := plan.BuildLogicalPlan(stmt, &InfoSchemaAdapter{manager: ue.tableManager})
		if err != nil {
			return nil, fmt.Errorf("unified select optimizer path failed: build logical plan: %w", err)
		}

		var physicalPlan plan.PhysicalPlan

		switch optimizer := ue.optimizer.(type) {
		case interface {
			OptimizeQuery(context.Context, plan.LogicalPlan) (*plan.OptimizedQueryPlan, error)
		}:
			optimizedPlan, err := optimizer.OptimizeQuery(ctx, logicalPlan)
			if err != nil {
				return nil, fmt.Errorf("unified select optimizer path failed: optimize query: %w", err)
			}
			if optimizedPlan == nil {
				return nil, fmt.Errorf("unified select optimizer path failed: optimizer returned nil result")
			}
			physicalPlan = optimizedPlan.PhysicalPlan
		case interface {
			Optimize(plan.LogicalPlan) (plan.PhysicalPlan, error)
		}:
			optimizedPhysicalPlan, err := optimizer.Optimize(logicalPlan)
			if err != nil {
				return nil, fmt.Errorf("unified select optimizer path failed: optimizer failed: %w", err)
			}
			physicalPlan = optimizedPhysicalPlan
		default:
			return nil, fmt.Errorf("unified select optimizer path failed: unsupported optimizer interface %T", ue.optimizer)
		}

		if physicalPlan == nil {
			return nil, fmt.Errorf("unified select optimizer path failed: optimizer returned nil physical plan")
		}

		rootOperator, err := ue.BuildOperatorTree(ctx, physicalPlan)
		if err != nil {
			return nil, fmt.Errorf("unified select optimizer path failed: build operator tree: %w", err)
		}

		return ue.collectSelectResult(ctx, rootOperator)
	}

	logger.Debugf("UnifiedExecutor: optimizer not configured, using manual operator path for schema %s", schemaName)

	if stmt.Where != nil && ue.tableManager != nil {
		logger.Debugf("UnifiedExecutor: fallback to logical/physical plan path for SELECT with WHERE")
		rootOperator, err := ue.buildSelectOperatorTreeFromPlans(ctx, stmt, schemaName)
		if err != nil {
			return nil, fmt.Errorf("failed to build operator tree with WHERE: %w", err)
		}
		return ue.collectSelectResult(ctx, rootOperator)
	}

	if stmt.Where != nil {
		return nil, fmt.Errorf("WHERE predicates in SELECT require optimizer-backed plan execution, not available in unified manual mode")
	}

	// 2. 构建算子树
	rootOperator, err := ue.buildSelectOperatorTree(ctx, stmt, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to build operator tree: %w", err)
	}

	return ue.collectSelectResult(ctx, rootOperator)
}

// ExecuteInsert 执行INSERT语句
func (ue *UnifiedExecutor) ExecuteInsert(ctx context.Context, stmt *sqlparser.Insert, schemaName string) (*DMLResult, error) {
	logger.Debugf("UnifiedExecutor: executing INSERT on schema %s", schemaName)

	// 获取表名
	targetSchema := strings.TrimSpace(schemaName)
	if qualifier := strings.TrimSpace(stmt.Table.Qualifier.String()); qualifier != "" {
		targetSchema = qualifier
	}

	tableName := stmt.Table.Name.String()

	// 创建插入算子
	insertOp := NewInsertOperator(
		targetSchema,
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
	if len(stmt.TableExprs) == 0 {
		return nil, fmt.Errorf("no table specified in UPDATE statement")
	}

	firstTableExpr, ok := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, fmt.Errorf("unsupported UPDATE FROM table expression type: %T", stmt.TableExprs[0])
	}

	tableNameExpr, ok := firstTableExpr.Expr.(sqlparser.TableName)
	if !ok {
		return nil, fmt.Errorf("unsupported UPDATE table expression type: %T", firstTableExpr.Expr)
	}

	targetSchema := strings.TrimSpace(schemaName)
	if qualifier := strings.TrimSpace(tableNameExpr.Qualifier.String()); qualifier != "" {
		targetSchema = qualifier
	}
	tableName := tableNameExpr.Name.String()

	if stmt.Where != nil {
		return ue.executeUpdateWithWhereFallback(ctx, stmt, schemaName)
	}

	// 创建扫描算子（用于定位需要更新的记录）
	scanOp := NewTableScanOperator(targetSchema, tableName, ue.storageAdapter)

	// 创建更新算子
	updateOp := NewUpdateOperator(
		targetSchema,
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
	if len(stmt.TableExprs) == 0 {
		return nil, fmt.Errorf("no table specified in DELETE statement")
	}

	firstTableExpr, ok := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, fmt.Errorf("unsupported DELETE FROM table expression type: %T", stmt.TableExprs[0])
	}

	tableNameExpr, ok := firstTableExpr.Expr.(sqlparser.TableName)
	if !ok {
		return nil, fmt.Errorf("unsupported DELETE table expression type: %T", firstTableExpr.Expr)
	}

	targetSchema := strings.TrimSpace(schemaName)
	if qualifier := strings.TrimSpace(tableNameExpr.Qualifier.String()); qualifier != "" {
		targetSchema = qualifier
	}
	tableName := tableNameExpr.Name.String()

	if stmt.Where != nil {
		return ue.executeDeleteWithWhereFallback(ctx, stmt, schemaName)
	}

	// 创建扫描算子（用于定位需要删除的记录）
	scanOp := NewTableScanOperator(targetSchema, tableName, ue.storageAdapter)

	// 创建删除算子
	deleteOp := NewDeleteOperator(
		targetSchema,
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
func (ue *UnifiedExecutor) BuildOperatorTree(ctx context.Context, physicalPlan plan.PhysicalPlan) (Operator, error) {
	if physicalPlan == nil {
		return nil, fmt.Errorf("physical plan is nil")
	}

	volcanoExecutor := NewVolcanoExecutor(
		ue.tableManager,
		ue.bufferPoolManager,
		ue.storageManager,
		ue.indexManager,
	)
	return volcanoExecutor.buildOperatorTree(ctx, physicalPlan)
}

func (ue *UnifiedExecutor) collectSelectResult(ctx context.Context, rootOperator Operator) (*SelectResult, error) {
	if rootOperator == nil {
		return nil, fmt.Errorf("root operator is nil")
	}

	if err := rootOperator.Open(ctx); err != nil {
		return nil, fmt.Errorf("failed to open operator: %w", err)
	}
	defer rootOperator.Close()

	records := make([]Record, 0)
	for {
		record, err := rootOperator.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch record: %w", err)
		}
		if record == nil {
			break
		}
		records = append(records, record)
	}

	return &SelectResult{
		Records:    records,
		RowCount:   len(records),
		Columns:    ue.getQueryColumnNames(rootOperator.Schema()),
		ResultType: "SELECT",
		Message:    "Success",
	}, nil
}

// buildSelectOperatorTree 构建SELECT查询的算子树
func (ue *UnifiedExecutor) buildSelectOperatorTree(ctx context.Context, stmt *sqlparser.Select, schemaName string) (Operator, error) {
	// 1. 解析FROM子句，确定表名
	if len(stmt.From) == 0 {
		return nil, fmt.Errorf("no table specified in FROM clause")
	}

	fromExpr, ok := stmt.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, fmt.Errorf("unsupported FROM table expression type: %T", stmt.From[0])
	}

	tableNameExpr, ok := fromExpr.Expr.(sqlparser.TableName)
	if !ok {
		return nil, fmt.Errorf("unsupported table name expression type: %T", fromExpr.Expr)
	}

	tableName := tableNameExpr.Name.String()

	// 2. 创建基础扫描算子
	var scanOp Operator = NewTableScanOperator(schemaName, tableName, ue.storageAdapter)

	// 3. 添加WHERE过滤算子
	if stmt.Where != nil {
		return nil, fmt.Errorf("WHERE conditions are not supported in unified executor select yet")
	}

	// 4. 添加投影算子（SELECT子句）
	if len(stmt.SelectExprs) > 0 {
		projectionExprs, requiresProjection, err := ue.buildProjectionExprs(stmt.SelectExprs)
		if err != nil {
			return nil, fmt.Errorf("failed to build projection expressions: %w", err)
		}
		if requiresProjection {
			scanOp = NewProjectionOperatorWithExprs(scanOp, projectionExprs)
		}
	}

	// 5. 添加ORDER BY排序算子
	if len(stmt.OrderBy) > 0 {
		querySchema, err := ue.getQuerySchemaFromTable(ctx, schemaName, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve schema for ORDER BY: %w", err)
		}

		sortKeys, err := ue.buildSortKeysFromOrderBy(stmt.OrderBy, querySchema)
		if err != nil {
			return nil, fmt.Errorf("failed to build ORDER BY operator: %w", err)
		}
		scanOp = NewSortOperator(scanOp, sortKeys)
	}

	// 6. 添加LIMIT算子
	if stmt.Limit != nil {
		offset, limit, err := ue.parseLimit(stmt.Limit)
		if err != nil {
			return nil, fmt.Errorf("failed to parse limit clause: %w", err)
		}
		scanOp = NewLimitOperator(scanOp, offset, limit)
	}

	return scanOp, nil
}

// buildSelectOperatorTreeFromPlans 通过逻辑计划/物理计划构建选择算子树（用于WHERE等需要谓词处理的场景）
func (ue *UnifiedExecutor) buildSelectOperatorTreeFromPlans(ctx context.Context, stmt *sqlparser.Select, schemaName string) (Operator, error) {
	_ = schemaName
	logicalPlan, err := plan.BuildLogicalPlan(stmt, &InfoSchemaAdapter{manager: ue.tableManager})
	if err != nil {
		return nil, fmt.Errorf("failed to build logical plan: %w", err)
	}

	logicalPlan = plan.OptimizeLogicalPlan(logicalPlan)
	physicalPlan := plan.ConvertToPhysicalPlan(logicalPlan)
	if physicalPlan == nil {
		return nil, fmt.Errorf("failed to convert logical plan to physical plan")
	}

	rootOperator, err := ue.BuildOperatorTree(ctx, physicalPlan)
	if err != nil {
		return nil, fmt.Errorf("failed to build operator tree from physical plan: %w", err)
	}
	return rootOperator, nil
}

func (ue *UnifiedExecutor) supportsStorageIntegratedDML() bool {
	return ue.tableManager != nil &&
		ue.storageManager != nil &&
		ue.tableStorageManager != nil &&
		ue.indexManager != nil
}

func (ue *UnifiedExecutor) executeUpdateWithWhereFallback(ctx context.Context, stmt *sqlparser.Update, schemaName string) (*DMLResult, error) {
	if !ue.supportsStorageIntegratedDML() {
		return nil, fmt.Errorf("WHERE predicate in UPDATE is only supported by storage-integrated DML executor path")
	}

	executor := NewStorageIntegratedDMLExecutor(
		nil,
		ue.bufferPoolManager,
		nil,
		ue.tableManager,
		nil,
		ue.indexManager,
		ue.storageManager,
		ue.tableStorageManager,
	)

	return executor.ExecuteUpdate(ctx, stmt, schemaName)
}

func (ue *UnifiedExecutor) executeDeleteWithWhereFallback(ctx context.Context, stmt *sqlparser.Delete, schemaName string) (*DMLResult, error) {
	if !ue.supportsStorageIntegratedDML() {
		return nil, fmt.Errorf("WHERE predicate in DELETE is only supported by storage-integrated DML executor path")
	}

	executor := NewStorageIntegratedDMLExecutor(
		nil,
		ue.bufferPoolManager,
		nil,
		ue.tableManager,
		nil,
		ue.indexManager,
		ue.storageManager,
		ue.tableStorageManager,
	)

	return executor.ExecuteDelete(ctx, stmt, schemaName)
}

func (ue *UnifiedExecutor) parseLimit(limitClause *sqlparser.Limit) (int64, int64, error) {
	if limitClause == nil {
		return 0, 0, nil
	}

	offset := int64(0)
	if limitClause.Offset != nil {
		parsedOffset, err := ue.parseIntExpr(limitClause.Offset)
		if err != nil {
			return 0, 0, err
		}
		offset = parsedOffset
	}

	limit, err := ue.parseIntExpr(limitClause.Rowcount)
	if err != nil {
		return 0, 0, err
	}

	return offset, limit, nil
}

func (ue *UnifiedExecutor) buildSortKeysFromOrderBy(orderBy sqlparser.OrderBy, querySchema *metadata.QuerySchema) ([]SortKey, error) {
	if len(orderBy) == 0 {
		return nil, nil
	}

	if querySchema == nil {
		return nil, fmt.Errorf("query schema is nil")
	}

	sortKeys := make([]SortKey, 0, len(orderBy))
	for _, order := range orderBy {
		colExpr, ok := order.Expr.(*sqlparser.ColName)
		if !ok {
			return nil, fmt.Errorf("ORDER BY only supports simple column names in unified select path, got %T", order.Expr)
		}

		columnName := colExpr.Name.String()
		if strings.TrimSpace(columnName) == "" {
			return nil, fmt.Errorf("ORDER BY column name is empty")
		}

		columnIdx := ue.findColumnIndex(querySchema, columnName)
		if columnIdx < 0 {
			return nil, fmt.Errorf("ORDER BY column %q not found in source schema", columnName)
		}

		sortKeys = append(sortKeys, SortKey{
			ColumnIdx: columnIdx,
			Ascending: order.Direction != sqlparser.DescScr,
		})
	}

	if len(sortKeys) == 0 {
		return nil, fmt.Errorf("ORDER BY has no valid sort keys")
	}

	return sortKeys, nil
}

func (ue *UnifiedExecutor) findColumnIndex(querySchema *metadata.QuerySchema, columnName string) int {
	if querySchema == nil {
		return -1
	}

	for i := 0; i < querySchema.ColumnCount(); i++ {
		col, ok := querySchema.GetColumnByIndex(i)
		if !ok || col == nil {
			continue
		}
		if strings.EqualFold(col.Name, columnName) {
			return i
		}
	}

	return -1
}

func (ue *UnifiedExecutor) getQuerySchemaFromTable(ctx context.Context, schemaName, tableName string) (*metadata.QuerySchema, error) {
	if ue.storageAdapter == nil {
		return nil, fmt.Errorf("storage adapter is not initialized")
	}
	tableMetadata, err := ue.storageAdapter.GetTableMetadata(ctx, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("get table metadata failed: %w", err)
	}
	if tableMetadata == nil || tableMetadata.Schema == nil {
		return nil, fmt.Errorf("table metadata or schema missing")
	}

	return metadata.FromTable(tableMetadata.Schema), nil
}

func (ue *UnifiedExecutor) buildProjectionExprs(selectExprs sqlparser.SelectExprs) ([]plan.Expression, bool, error) {
	if len(selectExprs) == 0 {
		return nil, false, nil
	}

	exprs := make([]plan.Expression, 0, len(selectExprs))
	for _, selectExpr := range selectExprs {
		switch expr := selectExpr.(type) {
		case *sqlparser.StarExpr:
			return nil, false, nil
		case *sqlparser.AliasedExpr:
			planExpr, err := ue.buildPlanExpression(expr.Expr)
			if err != nil {
				return nil, false, err
			}
			exprs = append(exprs, planExpr)
		default:
			return nil, false, fmt.Errorf("unsupported select expression type: %T", selectExpr)
		}
	}

	return exprs, true, nil
}

func (ue *UnifiedExecutor) buildPlanExpression(expr sqlparser.Expr) (plan.Expression, error) {
	switch v := expr.(type) {
	case *sqlparser.ColName:
		return &plan.Column{Name: v.Name.String()}, nil
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.IntVal:
			intVal, err := strconv.ParseInt(string(v.Val), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid int literal %q: %w", string(v.Val), err)
			}
			return &plan.Constant{Value: intVal}, nil
		case sqlparser.StrVal:
			return &plan.Constant{Value: string(v.Val)}, nil
		default:
			return nil, fmt.Errorf("unsupported SQL value type: %v", v.Type)
		}
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func (ue *UnifiedExecutor) parseIntExpr(expr sqlparser.Expr) (int64, error) {
	sqlVal, ok := expr.(*sqlparser.SQLVal)
	if !ok {
		return 0, fmt.Errorf("unsupported limit expression type: %T", expr)
	}
	if sqlVal.Type != sqlparser.IntVal {
		return 0, fmt.Errorf("unsupported limit value type: %v", sqlVal.Type)
	}

	value, err := strconv.ParseInt(string(sqlVal.Val), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid limit integer %q: %w", string(sqlVal.Val), err)
	}
	return value, nil
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

func (ue *UnifiedExecutor) getQueryColumnNames(schema *metadata.QuerySchema) []string {
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
