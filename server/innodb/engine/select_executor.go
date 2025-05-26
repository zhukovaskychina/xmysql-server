package engine

import (
	"context"
	"fmt"
	"io"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// SelectExecutor SELECT查询执行器
type SelectExecutor struct {
	BaseExecutor

	// 管理器组件
	optimizerManager  *manager.OptimizerManager
	bufferPoolManager *manager.OptimizedBufferPoolManager
	btreeManager      basic.BPlusTreeManager
	tableManager      *manager.TableManager

	// 查询相关
	logicalPlan     plan.LogicalPlan
	physicalPlan    *manager.PlanNode
	tableName       string
	schemaName      string
	whereConditions []string
	selectExprs     []string
	orderByColumns  []string
	limit           int
	offset          int

	// 执行状态
	currentRowIndex int
	resultSet       []*Record
	isInitialized   bool
}

// NewSelectExecutor 创建SELECT执行器
func NewSelectExecutor(
	optimizerManager *manager.OptimizerManager,
	bufferPoolManager *manager.OptimizedBufferPoolManager,
	btreeManager basic.BPlusTreeManager,
	tableManager *manager.TableManager,
) *SelectExecutor {
	return &SelectExecutor{
		optimizerManager:  optimizerManager,
		bufferPoolManager: bufferPoolManager,
		btreeManager:      btreeManager,
		tableManager:      tableManager,
		currentRowIndex:   0,
		resultSet:         make([]*Record, 0),
		isInitialized:     false,
		limit:             -1, // -1表示无限制
		offset:            0,
	}
}

// ExecuteSelect 执行SELECT查询的主入口
func (se *SelectExecutor) ExecuteSelect(ctx context.Context, stmt *sqlparser.Select, schemaName string) (*SelectResult, error) {
	// 1. 解析SELECT语句
	if err := se.parseSelectStatement(stmt, schemaName); err != nil {
		return nil, fmt.Errorf("parse SELECT statement failed: %v", err)
	}

	// 2. 构建逻辑计划
	if err := se.buildLogicalPlan(ctx, stmt); err != nil {
		return nil, fmt.Errorf("build logical plan failed: %v", err)
	}

	// 3. 优化查询计划
	if err := se.optimizeQuery(ctx); err != nil {
		return nil, fmt.Errorf("optimize query failed: %v", err)
	}

	// 4. 生成物理执行计划
	if err := se.generatePhysicalPlan(ctx); err != nil {
		return nil, fmt.Errorf("generate physical plan failed: %v", err)
	}

	// 5. 执行查询
	if err := se.executeQuery(ctx); err != nil {
		return nil, fmt.Errorf("execute query failed: %v", err)
	}

	// 6. 构建结果
	result := se.buildSelectResult()
	return result, nil
}

// parseSelectStatement 解析SELECT语句
func (se *SelectExecutor) parseSelectStatement(stmt *sqlparser.Select, schemaName string) error {
	se.schemaName = schemaName

	// 解析FROM子句
	if len(stmt.From) == 0 {
		return fmt.Errorf("missing FROM clause")
	}

	// 简化处理，假设只有一个表
	for _, fromExpr := range stmt.From {
		switch v := fromExpr.(type) {
		case *sqlparser.AliasedTableExpr:
			switch tableExpr := v.Expr.(type) {
			case sqlparser.TableName:
				se.tableName = tableExpr.Name.String()
			default:
				return fmt.Errorf("unsupported table expression type: %T", tableExpr)
			}
		default:
			return fmt.Errorf("unsupported FROM expression type: %T", v)
		}
	}

	// 解析SELECT表达式
	if err := se.parseSelectExprs(stmt.SelectExprs); err != nil {
		return err
	}

	// 解析WHERE条件
	if stmt.Where != nil {
		se.whereConditions = se.parseWhereConditions(stmt.Where.Expr)
	}

	// 解析ORDER BY
	if err := se.parseOrderBy(stmt.OrderBy); err != nil {
		return err
	}

	// 解析LIMIT
	if err := se.parseLimit(stmt.Limit); err != nil {
		return err
	}

	return nil
}

// parseSelectExprs 解析SELECT表达式
func (se *SelectExecutor) parseSelectExprs(selectExprs sqlparser.SelectExprs) error {
	for _, expr := range selectExprs {
		switch v := expr.(type) {
		case *sqlparser.StarExpr:
			// SELECT *
			se.selectExprs = append(se.selectExprs, "*")
		case *sqlparser.AliasedExpr:
			// SELECT column_name [AS alias]
			se.selectExprs = append(se.selectExprs, sqlparser.String(v.Expr))
		default:
			return fmt.Errorf("unsupported SELECT expression type: %T", v)
		}
	}
	return nil
}

// parseWhereConditions 解析WHERE条件
func (se *SelectExecutor) parseWhereConditions(expr sqlparser.Expr) []string {
	// 简化实现，将WHERE条件转换为字符串
	conditions := []string{sqlparser.String(expr)}
	return conditions
}

// parseOrderBy 解析ORDER BY子句
func (se *SelectExecutor) parseOrderBy(orderBy sqlparser.OrderBy) error {
	for _, order := range orderBy {
		column := sqlparser.String(order.Expr)
		if order.Direction == sqlparser.DescScr {
			column += " DESC"
		} else {
			column += " ASC"
		}
		se.orderByColumns = append(se.orderByColumns, column)
	}
	return nil
}

// parseLimit 解析LIMIT子句
func (se *SelectExecutor) parseLimit(limitClause *sqlparser.Limit) error {
	if limitClause == nil {
		return nil
	}

	if limitClause.Rowcount != nil {
		switch v := limitClause.Rowcount.(type) {
		case *sqlparser.SQLVal:
			if v.Type == sqlparser.IntVal {
				se.limit = int(v.Val[0]) // 简化处理
			}
		}
	}

	if limitClause.Offset != nil {
		switch v := limitClause.Offset.(type) {
		case *sqlparser.SQLVal:
			if v.Type == sqlparser.IntVal {
				se.offset = int(v.Val[0]) // 简化处理
			}
		}
	}

	return nil
}

// buildLogicalPlan 构建逻辑计划
func (se *SelectExecutor) buildLogicalPlan(ctx context.Context, stmt *sqlparser.Select) error {
	// 使用计划构建器构建逻辑计划
	infoSchema := &InfoSchemaAdapter{
		manager: se.tableManager,
	}

	logicalPlan, err := plan.BuildLogicalPlan(stmt, infoSchema)
	if err != nil {
		return fmt.Errorf("build logical plan failed: %v", err)
	}

	se.logicalPlan = logicalPlan
	return nil
}

// optimizeQuery 优化查询
func (se *SelectExecutor) optimizeQuery(ctx context.Context) error {
	// 使用优化器管理器优化查询
	physicalPlan, err := se.optimizerManager.GeneratePhysicalPlan(ctx, se.tableName, se.whereConditions)
	if err != nil {
		return fmt.Errorf("generate physical plan failed: %v", err)
	}

	se.physicalPlan = physicalPlan
	return nil
}

// generatePhysicalPlan 生成物理执行计划
func (se *SelectExecutor) generatePhysicalPlan(ctx context.Context) error {
	// 基于逻辑计划和优化结果生成物理执行计划
	// 这里我们已经有了物理计划，可以进行进一步的优化

	// 检查是否可以使用索引
	if err := se.chooseAccessMethod(ctx); err != nil {
		return fmt.Errorf("choose access method failed: %v", err)
	}

	return nil
}

// chooseAccessMethod 选择访问方法
func (se *SelectExecutor) chooseAccessMethod(ctx context.Context) error {
	// 获取表的索引信息
	indices, err := se.tableManager.GetTableIndices(ctx, se.schemaName, se.tableName)
	if err != nil {
		// 如果获取索引失败，使用全表扫描
		se.physicalPlan.PlanType = manager.PLAN_TYPE_SEQUENTIAL_SCAN
		return nil
	}

	// 简化实现：如果有索引且WHERE条件中有索引列，使用索引扫描
	if len(indices) > 0 && len(se.whereConditions) > 0 {
		se.physicalPlan.PlanType = manager.PLAN_TYPE_INDEX_SCAN
		se.physicalPlan.IndexName = indices[0].Name
	} else {
		se.physicalPlan.PlanType = manager.PLAN_TYPE_SEQUENTIAL_SCAN
	}

	return nil
}

// executeQuery 执行查询
func (se *SelectExecutor) executeQuery(ctx context.Context) error {
	switch se.physicalPlan.PlanType {
	case manager.PLAN_TYPE_SEQUENTIAL_SCAN:
		return se.executeTableScan(ctx)
	case manager.PLAN_TYPE_INDEX_SCAN:
		return se.executeIndexScan(ctx)
	default:
		return fmt.Errorf("unsupported plan type: %v", se.physicalPlan.PlanType)
	}
}

// executeTableScan 执行全表扫描
func (se *SelectExecutor) executeTableScan(ctx context.Context) error {
	fmt.Printf("执行全表扫描，表名: %s.%s\n", se.schemaName, se.tableName)

	// 获取表特定的B+树管理器
	tableBTreeManager, err := se.tableManager.GetTableBTreeManager(ctx, se.schemaName, se.tableName)
	if err != nil {
		return fmt.Errorf("failed to get table btree manager for %s.%s: %v", se.schemaName, se.tableName, err)
	}

	// 获取表的存储信息
	storageInfo, err := se.tableManager.GetTableStorageInfo(se.schemaName, se.tableName)
	if err != nil {
		return fmt.Errorf("failed to get table storage info for %s.%s: %v", se.schemaName, se.tableName, err)
	}

	fmt.Printf("表存储信息: SpaceID=%d, RootPage=%d\n", storageInfo.SpaceID, storageInfo.RootPageNo)

	// 获取表的所有叶子页面
	leafPages, err := tableBTreeManager.GetAllLeafPages(ctx)
	if err != nil {
		return fmt.Errorf("get all leaf pages failed for table %s.%s: %v", se.schemaName, se.tableName, err)
	}

	fmt.Printf("找到表 %s.%s 的 %d 个叶子页面\n", se.schemaName, se.tableName, len(leafPages))

	// 扫描所有叶子页面
	for _, pageNum := range leafPages {
		if err := se.scanPageWithSpaceID(ctx, storageInfo.SpaceID, pageNum); err != nil {
			return fmt.Errorf("scan page %d in space %d failed: %v", pageNum, storageInfo.SpaceID, err)
		}
	}

	return nil
}

// executeIndexScan 执行索引扫描
func (se *SelectExecutor) executeIndexScan(ctx context.Context) error {
	// 简化实现：如果有WHERE条件，使用B+树搜索
	if len(se.whereConditions) > 0 {
		// 这里应该解析WHERE条件中的键值
		// 简化处理，假设第一个条件是等值查询
		key := "dummy_key" // 实际应该从WHERE条件中解析

		// 使用B+树搜索
		pageNum, slot, err := se.btreeManager.Search(ctx, key)
		if err != nil {
			return fmt.Errorf("B+tree search failed: %v", err)
		}

		// 扫描找到的页面
		if err := se.scanPageFromSlot(ctx, pageNum, slot); err != nil {
			return fmt.Errorf("scan page from slot failed: %v", err)
		}
	} else {
		// 没有WHERE条件，回退到全表扫描
		return se.executeTableScan(ctx)
	}

	return nil
}

// scanPage 扫描页面
func (se *SelectExecutor) scanPage(ctx context.Context, pageNum uint32) error {
	// 从缓冲池获取页面
	bufferPage, err := se.bufferPoolManager.GetPage(0, pageNum) // 假设spaceID为0
	if err != nil {
		return fmt.Errorf("get page from buffer pool failed: %v", err)
	}

	// 解析页面中的记录
	records, err := se.parsePageRecords(bufferPage)
	if err != nil {
		return fmt.Errorf("parse page records failed: %v", err)
	}

	// 应用WHERE条件过滤
	filteredRecords := se.applyWhereFilter(records)

	// 添加到结果集
	se.resultSet = append(se.resultSet, filteredRecords...)

	return nil
}

// scanPageWithSpaceID 使用指定表空间ID扫描页面
func (se *SelectExecutor) scanPageWithSpaceID(ctx context.Context, spaceID, pageNum uint32) error {
	fmt.Printf("扫描页面: SpaceID=%d, PageNum=%d\n", spaceID, pageNum)

	// 从缓冲池获取页面
	bufferPage, err := se.bufferPoolManager.GetPage(spaceID, pageNum)
	if err != nil {
		return fmt.Errorf("get page from buffer pool failed (space=%d, page=%d): %v", spaceID, pageNum, err)
	}

	// 解析页面中的记录
	records, err := se.parsePageRecords(bufferPage)
	if err != nil {
		return fmt.Errorf("parse page records failed (space=%d, page=%d): %v", spaceID, pageNum, err)
	}

	fmt.Printf("从页面 %d 解析出 %d 条记录\n", pageNum, len(records))

	// 应用WHERE条件过滤
	filteredRecords := se.applyWhereFilter(records)

	fmt.Printf("过滤后剩余 %d 条记录\n", len(filteredRecords))

	// 添加到结果集
	se.resultSet = append(se.resultSet, filteredRecords...)

	return nil
}

// scanPageFromSlot 从指定槽位开始扫描页面
func (se *SelectExecutor) scanPageFromSlot(ctx context.Context, pageNum uint32, slot int) error {
	// 从缓冲池获取页面
	bufferPage, err := se.bufferPoolManager.GetPage(0, pageNum)
	if err != nil {
		return fmt.Errorf("get page from buffer pool failed: %v", err)
	}

	// 解析页面中从指定槽位开始的记录
	records, err := se.parsePageRecordsFromSlot(bufferPage, slot)
	if err != nil {
		return fmt.Errorf("parse page records from slot failed: %v", err)
	}

	// 应用WHERE条件过滤
	filteredRecords := se.applyWhereFilter(records)

	// 添加到结果集
	se.resultSet = append(se.resultSet, filteredRecords...)

	return nil
}

// parsePageRecords 解析页面中的记录
func (se *SelectExecutor) parsePageRecords(bufferPage interface{}) ([]*Record, error) {
	// 简化实现：创建模拟记录
	var records []*Record

	// 获取表的元数据
	tableMeta, err := se.tableManager.GetTableMetadata(context.Background(), se.schemaName, se.tableName)
	if err != nil {
		// 如果获取元数据失败，创建默认记录
		for i := 0; i < 10; i++ { // 模拟10条记录
			record := &Record{
				Values: []interface{}{i, fmt.Sprintf("value_%d", i)},
				Schema: &metadata.TableMeta{
					Name: se.tableName,
					Columns: []*metadata.ColumnMeta{
						{Name: "id", Type: "INT"},
						{Name: "name", Type: "VARCHAR"},
					},
				},
			}
			records = append(records, record)
		}
	} else {
		// 基于表元数据创建记录
		for i := 0; i < 10; i++ {
			values := make([]interface{}, len(tableMeta.Columns))
			for j, col := range tableMeta.Columns {
				switch col.Type {
				case "INT":
					values[j] = i
				case "VARCHAR", "TEXT":
					values[j] = fmt.Sprintf("%s_%d", col.Name, i)
				default:
					values[j] = nil
				}
			}
			record := &Record{
				Values: values,
				Schema: tableMeta,
			}
			records = append(records, record)
		}
	}

	return records, nil
}

// parsePageRecordsFromSlot 从指定槽位解析记录
func (se *SelectExecutor) parsePageRecordsFromSlot(bufferPage interface{}, slot int) ([]*Record, error) {
	// 先获取所有记录，然后从指定槽位开始
	allRecords, err := se.parsePageRecords(bufferPage)
	if err != nil {
		return nil, err
	}

	if slot >= len(allRecords) {
		return []*Record{}, nil
	}

	return allRecords[slot:], nil
}

// applyWhereFilter 应用WHERE条件过滤
func (se *SelectExecutor) applyWhereFilter(records []*Record) []*Record {
	if len(se.whereConditions) == 0 {
		return records
	}

	var filteredRecords []*Record

	// 简化实现：只是返回一部分记录来模拟过滤效果
	for i, record := range records {
		if i%2 == 0 { // 简单的过滤逻辑：只返回偶数索引的记录
			filteredRecords = append(filteredRecords, record)
		}
	}

	return filteredRecords
}

// buildSelectResult 构建SELECT结果
func (se *SelectExecutor) buildSelectResult() *SelectResult {
	// 应用投影
	projectedRecords := se.applyProjection(se.resultSet)

	// 应用排序
	sortedRecords := se.applyOrderBy(projectedRecords)

	// 应用LIMIT和OFFSET
	limitedRecords := se.applyLimitOffset(sortedRecords)

	return &SelectResult{
		Records:    limitedRecords,
		RowCount:   len(limitedRecords),
		Columns:    se.getColumnNames(),
		ResultType: common.RESULT_TYPE_QUERY,
	}
}

// applyProjection 应用投影
func (se *SelectExecutor) applyProjection(records []*Record) []*Record {
	if len(se.selectExprs) == 0 || (len(se.selectExprs) == 1 && se.selectExprs[0] == "*") {
		// SELECT * 返回所有列
		return records
	}

	// 简化实现：返回原记录（实际应该根据selectExprs投影）
	return records
}

// applyOrderBy 应用排序
func (se *SelectExecutor) applyOrderBy(records []*Record) []*Record {
	if len(se.orderByColumns) == 0 {
		return records
	}

	// 简化实现：不进行实际排序
	return records
}

// applyLimitOffset 应用LIMIT和OFFSET
func (se *SelectExecutor) applyLimitOffset(records []*Record) []*Record {
	start := se.offset
	if start >= len(records) {
		return []*Record{}
	}

	end := len(records)
	if se.limit > 0 && start+se.limit < len(records) {
		end = start + se.limit
	}

	return records[start:end]
}

// getColumnNames 获取列名
func (se *SelectExecutor) getColumnNames() []string {
	if len(se.selectExprs) == 1 && se.selectExprs[0] == "*" {
		// 如果是SELECT *，需要从表元数据获取所有列名
		tableMeta, err := se.tableManager.GetTableMetadata(context.Background(), se.schemaName, se.tableName)
		if err != nil {
			return []string{"id", "name"} // 默认列名
		}

		var columnNames []string
		for _, col := range tableMeta.Columns {
			columnNames = append(columnNames, col.Name)
		}
		return columnNames
	}

	return se.selectExprs
}

// SelectResult SELECT查询结果
type SelectResult struct {
	Records    []*Record
	RowCount   int
	Columns    []string
	ResultType string
	Message    string
}

// InfoSchemaAdapter 信息模式适配器实现
type InfoSchemaAdapter struct {
	manager *manager.TableManager
}

// TableByName 根据表名查找表元信息
func (a *InfoSchemaAdapter) TableByName(name string) (*metadata.Table, error) {
	// 简化实现：创建一个基本的表结构
	return &metadata.Table{
		Name: name,
		Columns: []*metadata.Column{
			{Name: "id", DataType: metadata.TypeInt},
			{Name: "name", DataType: metadata.TypeVarchar},
		},
	}, nil
}

// 实现Iterator接口的方法
func (se *SelectExecutor) Init() error {
	se.isInitialized = true
	se.currentRowIndex = 0
	return nil
}

func (se *SelectExecutor) Next() error {
	if !se.isInitialized {
		return fmt.Errorf("executor not initialized")
	}

	if se.currentRowIndex >= len(se.resultSet) {
		return io.EOF
	}

	se.currentRowIndex++
	return nil
}

func (se *SelectExecutor) GetRow() []interface{} {
	if se.currentRowIndex == 0 || se.currentRowIndex > len(se.resultSet) {
		return nil
	}

	record := se.resultSet[se.currentRowIndex-1]
	return record.Values
}

func (se *SelectExecutor) Close() error {
	se.closed = true
	se.currentRowIndex = 0
	se.resultSet = nil
	return nil
}

// 实现Executor接口的方法
func (se *SelectExecutor) Schema() *metadata.Schema {
	return se.schema
}

func (se *SelectExecutor) Children() []Executor {
	return se.children
}

func (se *SelectExecutor) SetChildren(children []Executor) {
	se.children = children
}
