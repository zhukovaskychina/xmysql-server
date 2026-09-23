package engine

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/logger"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/page"
)

var (
	userWhereConditionRe = regexp.MustCompile("(?i)\\b(?:`?user`?)\\s*=\\s*(?:'([^']*)'|\"([^\"]*)\"|([^\\s]+))")
	hostWhereConditionRe = regexp.MustCompile("(?i)\\b(?:`?host`?)\\s*=\\s*(?:'([^']*)'|\"([^\"]*)\"|([^\\s]+))")
)

// SelectExecutor SELECT查询执行器
// 注意：此执行器是查询协调器，不是火山模型的Operator
// 实际的算子执行使用volcano_executor.go中的Operator接口
type SelectExecutor struct {
	// 管理器组件
	optimizerManager  *manager.OptimizerManager
	bufferPoolManager *manager.OptimizedBufferPoolManager
	btreeManager      basic.BPlusTreeManager
	tableManager      *manager.TableManager
	storageManager    *manager.StorageManager

	// dataDir 用于在无 tableManager 时从 .frm 文件加载表定义（与 executor 写入的 CREATE TABLE 一致）
	dataDir string

	// 查询相关
	logicalPlan     plan.LogicalPlan
	physicalPlan    *manager.PlanNode
	tableName       string
	schemaName      string
	whereConditions []string
	whereExpression sqlparser.Expr
	selectExprs     []string
	selectExprASTs  []sqlparser.Expr
	selectAliases   []string
	distinct        bool
	groupByColumns  []string
	havingCondition string
	orderByColumns  []string
	joinOrderTypes  map[string]metadata.DataType
	limit           int
	offset          int

	// 执行状态
	currentRowIndex           int
	resultSet                 []Record
	isInitialized             bool
	lastAccessPath            string
	storedFunctionAuthorizer  func(persistedStoredObject) error
	joinSubqueryEvaluator     func(*sqlparser.Subquery, joinedMapRow) (interface{}, error)
	joinSetSubqueryEvaluator  func(*sqlparser.Subquery, joinedMapRow) ([]interface{}, error)
	expressionError           error
	projectionExpressionCache map[string]projectionExpressionCacheEntry
	candidateStorageKeys      map[string]struct{}
	sessionValues             map[string]interface{}
	rowsExamined              int64
}

type projectionExpressionCacheEntry struct {
	ast      sqlparser.Expr
	compiled plan.CompiledExpression
}

func cloneStringSet(values map[string]struct{}) map[string]struct{} {
	if values == nil {
		return nil
	}
	clone := make(map[string]struct{}, len(values))
	for value := range values {
		clone[value] = struct{}{}
	}
	return clone
}

// NewSelectExecutor 创建SELECT执行器。dataDir 可选，非空时在无 tableManager 时从 dataDir/schema/table.frm 加载表定义。
func NewSelectExecutor(
	optimizerManager *manager.OptimizerManager,
	bufferPoolManager *manager.OptimizedBufferPoolManager,
	btreeManager basic.BPlusTreeManager,
	storageManager *manager.StorageManager,
	tableManager *manager.TableManager,
	dataDir string,
) *SelectExecutor {
	return &SelectExecutor{
		optimizerManager:          optimizerManager,
		bufferPoolManager:         bufferPoolManager,
		btreeManager:              btreeManager,
		storageManager:            storageManager,
		tableManager:              tableManager,
		dataDir:                   dataDir,
		currentRowIndex:           0,
		resultSet:                 make([]Record, 0),
		projectionExpressionCache: make(map[string]projectionExpressionCacheEntry),
		isInitialized:             false,
		limit:                     -1, // -1表示无限制
		offset:                    0,
	}
}

// ExecuteSelect 执行SELECT查询的主入口
func (se *SelectExecutor) ExecuteSelect(ctx context.Context, stmt *sqlparser.Select, schemaName string) (*SelectResult, error) {
	// 重置执行态，避免复用实例时污染上一次查询状态
	se.resetExecutionState()
	se.schemaName = schemaName

	if selectHasJoin(stmt) {
		return se.executeJoinSelect(ctx, stmt, schemaName)
	}

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
	if se.expressionError != nil {
		return nil, se.expressionError
	}
	return result, nil
}

func (se *SelectExecutor) resetExecutionState() {
	se.schemaName = ""
	se.tableName = ""

	se.whereConditions = nil
	se.whereExpression = nil
	se.selectExprs = nil
	se.selectExprASTs = nil
	se.selectAliases = nil
	se.distinct = false
	se.groupByColumns = nil
	se.havingCondition = ""
	se.orderByColumns = nil
	se.joinOrderTypes = nil
	se.limit = -1
	se.offset = 0

	se.currentRowIndex = 0
	se.resultSet = nil
	se.isInitialized = false
	se.lastAccessPath = ""
	se.expressionError = nil
	se.rowsExamined = 0
}

func (se *SelectExecutor) setStoredFunctionAuthorizer(authorizer func(persistedStoredObject) error) {
	se.storedFunctionAuthorizer = authorizer
}

// parseSelectStatement 解析SELECT语句
func (se *SelectExecutor) parseSelectStatement(stmt *sqlparser.Select, schemaName string) error {
	se.resetExecutionState()
	se.schemaName = schemaName
	se.distinct = strings.EqualFold(strings.TrimSpace(stmt.Distinct), strings.TrimSpace(sqlparser.DistinctStr))

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
				if qualifier := strings.TrimSpace(tableExpr.Qualifier.String()); qualifier != "" {
					se.schemaName = qualifier
				}
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
		se.whereExpression = stmt.Where.Expr
		se.whereConditions = se.parseWhereConditions(stmt.Where.Expr)
	}

	for _, expr := range stmt.GroupBy {
		se.groupByColumns = append(se.groupByColumns, sqlparser.String(expr))
	}
	if stmt.Having != nil {
		se.havingCondition = sqlparser.String(stmt.Having.Expr)
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
			se.selectExprASTs = append(se.selectExprASTs, nil)
			se.selectAliases = append(se.selectAliases, "")
		case *sqlparser.AliasedExpr:
			// SELECT column_name [AS alias]
			se.selectExprs = append(se.selectExprs, sqlparser.String(v.Expr))
			se.selectExprASTs = append(se.selectExprASTs, v.Expr)
			if !v.As.IsEmpty() {
				se.selectAliases = append(se.selectAliases, v.As.String())
			} else {
				se.selectAliases = append(se.selectAliases, "")
			}
		default:
			return fmt.Errorf("unsupported SELECT expression type: %T", v)
		}
	}
	return nil
}

// parseWhereConditions 解析WHERE条件
func (se *SelectExecutor) parseWhereConditions(expr sqlparser.Expr) []string {
	if expr == nil {
		return []string{}
	}

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
				limit, err := strconv.Atoi(string(v.Val))
				if err != nil {
					return fmt.Errorf("invalid LIMIT value: %s", string(v.Val))
				}
				se.limit = limit
			}
		}
	}

	if limitClause.Offset != nil {
		switch v := limitClause.Offset.(type) {
		case *sqlparser.SQLVal:
			if v.Type == sqlparser.IntVal {
				offset, err := strconv.Atoi(string(v.Val))
				if err != nil {
					return fmt.Errorf("invalid OFFSET value: %s", string(v.Val))
				}
				se.offset = offset
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
	se.lastAccessPath = "table_scan"

	// 获取表的索引信息
	indices, err := se.tableManager.GetTableIndices(ctx, se.schemaName, se.tableName)
	if err != nil {
		// 如果获取索引失败，使用全表扫描
		se.physicalPlan.PlanType = manager.PLAN_TYPE_SEQUENTIAL_SCAN
		return nil
	}

	// 简化实现：如果有索引且WHERE条件中有索引列，使用索引扫描
	visibleIndex := false
	var visibleIndexName string
	for _, index := range indices {
		if secondaryIndexVisible(index) {
			visibleIndex = true
			visibleIndexName = index.Name
			break
		}
	}
	if visibleIndex && len(se.whereConditions) > 0 {
		se.physicalPlan.PlanType = manager.PLAN_TYPE_INDEX_SCAN
		se.physicalPlan.IndexName = visibleIndexName
		se.lastAccessPath = "secondary_index:" + visibleIndexName
	} else {
		se.physicalPlan.PlanType = manager.PLAN_TYPE_SEQUENTIAL_SCAN
	}

	return nil
}

// executeQuery 执行查询
func (se *SelectExecutor) executeQuery(ctx context.Context) error {
	logger.Debugf(" [SelectExecutor] 开始执行查询: table=%s, schema=%s", se.tableName, se.schemaName)

	//  特殊处理 mysql.user 表查询
	if se.schemaName == "mysql" && se.tableName == "user" {
		logger.Debugf(" [SelectExecutor] 检测到 mysql.user 表查询，使用特殊处理逻辑")
		return se.executeUserTableQuery(ctx)
	}

	// 通用查询处理逻辑
	logger.Debugf(" [SelectExecutor] 执行通用表查询")

	// 优先从表管理器获取表元数据（列与表结构准确）
	if se.tableManager != nil {
		meta, err := se.tableManager.GetTableMetadata(ctx, se.schemaName, se.tableName)
		if err == nil && meta != nil && len(meta.Columns) > 0 {
			if frmMeta, frmErr := se.loadTableMetaFromFrm(se.dataDir, se.schemaName, se.tableName); frmErr == nil && frmMeta != nil {
				meta = frmMeta
			}
			logger.Debugf(" [SelectExecutor] 使用表管理器元数据: %s.%s, 列数=%d", se.schemaName, se.tableName, len(meta.Columns))
			if err := se.scanStorageRows(ctx, meta); err != nil {
				return err
			}
			logger.Debugf(" [SelectExecutor] 查询执行完成，返回 %d 行数据", len(se.resultSet))
			return nil
		}
		if err != nil {
			logger.Debugf(" [SelectExecutor] 表管理器未找到表 %s.%s: %v，尝试 .frm 或示例数据", se.schemaName, se.tableName, err)
		}
	}

	// 无表管理器或表中未在表管理器注册时：从 .frm 加载表定义（与 CREATE TABLE 写入一致）
	if se.dataDir != "" {
		if frmMeta, err := se.loadTableMetaFromFrm(se.dataDir, se.schemaName, se.tableName); err == nil && frmMeta != nil {
			logger.Debugf(" [SelectExecutor] 从 .frm 使用表定义并扫描数据页，列: %v", frmMeta.Columns)
			if err := se.scanStorageRows(ctx, frmMeta); err != nil {
				return err
			}
			logger.Debugf(" [SelectExecutor] 查询执行完成，返回 %d 行数据", len(se.resultSet))
			return nil
		}
	}

	// With a real table manager, reaching this point means the table is not
	// present in either the metadata cache or durable .frm files.  Do not mask
	// that state with the legacy sample-row fallback; callers must observe the
	// normal missing-table error (for example after RENAME TABLE moves it to a
	// different schema).
	if se.tableManager != nil {
		return fmt.Errorf("table '%s.%s' not found", se.schemaName, se.tableName)
	}

	// 无表管理器且无 .frm 时，退回示例数据
	logger.Debugf("  [SelectExecutor] 没有表管理器且无 .frm，创建示例数据")
	se.resultSet = []Record{
		NewExecutorRecordFromInterface([]interface{}{1, "sample_user", "sample_value"}, se.getDefaultTableMeta()),
		NewExecutorRecordFromInterface([]interface{}{2, "test_user", "test_value"}, se.getDefaultTableMeta()),
	}

	logger.Debugf(" [SelectExecutor] 查询执行完成，返回 %d 行数据", len(se.resultSet))
	return nil
}

func (se *SelectExecutor) scanStorageRows(ctx context.Context, tableMeta *metadata.TableMeta) error {
	if se.storageManager == nil || se.storageManager.GetTableStorageManager() == nil || se.bufferPoolManager == nil {
		se.resultSet = []Record{}
		return nil
	}
	if err := se.ensureTableStorageMapping(ctx); err != nil {
		return err
	}
	if se.candidateStorageKeys == nil {
		if used, err := se.scanSecondaryIndexRows(ctx, tableMeta); err != nil || used {
			return err
		}
	} else {
		se.lastAccessPath = "spatial_mbr_candidate_scan"
	}

	btreeManager := se.btreeManager
	if storageTableManager := se.storageManager.GetTableStorageManager(); storageTableManager != nil {
		if partitioned, isPartitioned, partitionErr := newPartitionedBTreeManager(ctx, storageTableManager, se.schemaName, se.tableName, tableMeta); partitionErr != nil {
			return fmt.Errorf("create partitioned clustered index manager failed: %v", partitionErr)
		} else if isPartitioned {
			if router, ok := partitioned.(*partitionedBTreeManager); ok {
				router.RestrictToWherePartitions(se.whereConditions)
			}
			if err := persistPartitionStorageRoots(se.dataDir, se.schemaName, se.tableName, storageTableManager); err != nil {
				return fmt.Errorf("persist partition storage mapping failed: %v", err)
			}
			btreeManager = partitioned
		} else {
			tableBTreeManager, err := storageTableManager.CreateBTreeManagerForTable(ctx, se.schemaName, se.tableName)
			if err == nil && tableBTreeManager != nil {
				if info, infoErr := storageTableManager.GetTableStorageInfo(se.schemaName, se.tableName); infoErr == nil {
					if persistErr := persistTableStorageIdentityIfPresent(se.dataDir, info); persistErr != nil {
						return fmt.Errorf("persist table storage mapping failed: %v", persistErr)
					}
				}
				btreeManager = tableBTreeManager
			}
		}
	}
	if btreeManager == nil && se.tableManager != nil {
		tableBTreeManager, err := se.tableManager.GetTableBTreeManager(ctx, se.schemaName, se.tableName)
		if err == nil && tableBTreeManager != nil {
			btreeManager = tableBTreeManager
		}
	}
	if btreeManager == nil {
		se.resultSet = []Record{}
		return nil
	}

	scanner := NewClusteredIndexScanner(btreeManager, tableMeta)
	scanConditions := se.whereConditions
	deferApplyWhere := se.whereRequiresRowExpressionEvaluation()
	if deferApplyWhere {
		scanConditions = nil
	}
	projectionColumns, useProjection, err := se.determineStorageProjectionColumns(tableMeta)
	if err != nil {
		return fmt.Errorf("determine storage projection columns failed: %v", err)
	}
	if useProjection {
		// The projected row contains every identifier found in WHERE, so apply
		// the predicate after the storage decoder has produced that reduced row.
		// This lets the decoder skip unrelated payload values safely.
		scanConditions = nil
		deferApplyWhere = true
	}
	var rows []*InsertRowData
	if len(se.candidateStorageKeys) > 0 {
		var scannedRows []clusteredScannedRow
		if useProjection {
			scannedRows, err = scanner.scanWithProjection(ctx, scanConditions, projectionColumns)
		} else {
			scannedRows, err = scanner.ScanWithStorageKeys(ctx, scanConditions)
		}
		if err == nil {
			rows = filterClusteredScannedRowsByStorageKeys(scannedRows, se.candidateStorageKeys)
		}
	} else if useProjection {
		rows, err = scanner.ScanProjected(ctx, scanConditions, projectionColumns)
	} else {
		rows, err = scanner.Scan(ctx, scanConditions)
	}
	if err != nil {
		return fmt.Errorf("scan clustered index failed: %v", err)
	}
	se.rowsExamined = scanner.RowsExamined()
	rows = se.applyPartitionPruning(rows)

	records := make([]Record, 0, len(rows))
	for _, row := range rows {
		records = append(records, recordFromInsertRowData(row, tableMeta))
	}
	if deferApplyWhere {
		records = se.applyWhereFilter(records)
	}
	se.resultSet = records
	return nil
}

func filterClusteredScannedRowsByStorageKeys(scannedRows []clusteredScannedRow, candidateKeys map[string]struct{}) []*InsertRowData {
	rows := make([]*InsertRowData, 0, len(scannedRows))
	for _, scannedRow := range scannedRows {
		if scannedRow.storageKey == nil {
			continue
		}
		key, keyOK := scannedRow.storageKey.(string)
		if !keyOK {
			continue
		}
		if _, exists := candidateKeys[key]; exists {
			rows = append(rows, scannedRow.data)
		}
	}
	return rows
}

// determineStorageProjectionColumns returns the source columns needed by a
// simple single-table projection and its WHERE predicates. Complex clauses
// conservatively keep the full-row scan path because they may need columns
// that are not represented by a plain identifier in the query text.
func (se *SelectExecutor) determineStorageProjectionColumns(tableMeta *metadata.TableMeta) ([]string, bool, error) {
	if tableMeta == nil {
		return nil, false, fmt.Errorf("table metadata is nil")
	}
	if len(se.selectExprs) == 0 || (len(se.selectExprs) == 1 && se.selectExprs[0] == "*") {
		return nil, false, nil
	}
	if len(se.groupByColumns) > 0 || strings.TrimSpace(se.havingCondition) != "" || len(se.orderByColumns) > 0 {
		return nil, false, nil
	}
	plainIdentifier := regexp.MustCompile(`(?i)^[a-z_][a-z0-9_$]*(?:\.[a-z_][a-z0-9_$]*)?$`)
	for _, expression := range se.selectExprs {
		if !plainIdentifier.MatchString(strings.Trim(strings.TrimSpace(expression), "`")) {
			return nil, false, nil
		}
	}
	requiredIndexes, _, err := se.determineRequiredColumns(tableMeta)
	if err != nil {
		return nil, false, err
	}
	if len(requiredIndexes) == 0 {
		return nil, false, nil
	}
	columns := make([]string, 0, len(requiredIndexes))
	for _, index := range requiredIndexes {
		if index < 0 || index >= len(tableMeta.Columns) || tableMeta.Columns[index] == nil {
			return nil, false, fmt.Errorf("required storage column index %d is invalid", index)
		}
		columns = append(columns, tableMeta.Columns[index].Name)
	}
	return columns, true, nil
}

func (se *SelectExecutor) whereRequiresRowExpressionEvaluation() bool {
	if len(se.whereConditions) == 0 {
		return false
	}
	for _, condition := range se.whereConditions {
		if strings.Contains(condition, "(") {
			return true
		}
	}
	return false
}

// applyPartitionPruning narrows the row materialization set for a simple
// constant predicate on the partition expression. The clustered scanner
// still owns correctness filtering; this helper only removes rows from
// partitions that the planner proves cannot match, and conservatively falls
// back to the full result for any unsupported predicate shape.
func (se *SelectExecutor) applyPartitionPruning(rows []*InsertRowData) []*InsertRowData {
	if len(rows) == 0 || len(se.whereConditions) == 0 || se.dataDir == "" {
		return rows
	}
	tableInfo, err := readTableMetadataMap(filepath.Join(se.dataDir, se.schemaName, se.tableName+".frm"))
	if err != nil {
		return rows
	}
	descriptor, ok := tableInfo["partitioning"].(map[string]interface{})
	if !ok {
		return rows
	}
	method, _ := descriptor["method"].(string)
	expression, _ := descriptor["expression"].(string)
	rules := partitionPlannerRules(descriptor)
	if len(rules) == 0 {
		return rows
	}
	allowedSet := make(map[int]struct{}, len(rules))
	for ordinal := range rules {
		allowedSet[ordinal] = struct{}{}
	}
	for _, condition := range se.whereConditions {
		conditionSet, ok := partitionAllowedOrdinals(condition, strings.ToUpper(method), expression, rules)
		if !ok {
			return rows
		}
		for ordinal := range allowedSet {
			if _, keep := conditionSet[ordinal]; !keep {
				delete(allowedSet, ordinal)
			}
		}
	}
	if len(allowedSet) == len(rules) {
		return rows
	}
	filtered := make([]*InsertRowData, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		value, exists := partitionRowValue(row.ColumnValues, expression)
		if !exists {
			return rows
		}
		ordinal, err := partitionForValue(strings.ToUpper(method), value, descriptorPartitions(descriptor))
		if err != nil {
			return rows
		}
		if _, keep := allowedSet[ordinal]; keep {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func partitionPlannerRules(descriptor map[string]interface{}) []plan.PartitionRule {
	definitions := descriptorPartitions(descriptor)
	rules := make([]plan.PartitionRule, 0, len(definitions))
	for _, raw := range definitions {
		definition, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		rule := plan.PartitionRule{}
		rule.Name, _ = definition["name"].(string)
		values, _ := definition["values"].(string)
		lowerValues := strings.ToLower(values)
		if strings.Contains(lowerValues, "maxvalue") {
			rule.MaxValue = true
		} else if tuples := partitionListTupleBounds(values); len(tuples) > 0 {
			rule.ListTuple = tuples
		} else if tuple, ok := partitionTupleBound(values); ok && len(tuple) > 1 {
			rule.LessThanTuple = tuple
		} else if bound, ok := firstPartitionNumber(values); ok {
			rule.LessThan = &bound
		} else if bound, ok := firstPartitionText(values); ok {
			rule.LessThanText = &bound
		}
		if at := strings.Index(lowerValues, "in"); at >= 0 {
			listText := strings.Trim(strings.TrimSpace(values[at+2:]), "()")
			for _, item := range splitTopLevelComma(listText) {
				if number, err := strconv.ParseInt(strings.TrimSpace(item), 10, 64); err == nil {
					rule.ListValue = append(rule.ListValue, number)
				} else if text, ok := partitionListTextLiteral(item); ok {
					rule.ListText = append(rule.ListText, text)
				}
			}
		}
		rules = append(rules, rule)
	}
	return rules
}

func partitionListTextLiteral(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 2 || (raw[0] != '\'' && raw[0] != '"') || raw[len(raw)-1] != raw[0] {
		return "", false
	}
	quote := raw[0]
	value := raw[1 : len(raw)-1]
	value = strings.ReplaceAll(value, string([]byte{quote, quote}), string(quote))
	return value, true
}

func descriptorPartitions(descriptor map[string]interface{}) []interface{} {
	definitions, _ := descriptor["partitions"].([]interface{})
	return definitions
}

func partitionConstantPredicate(condition, expression string) (string, interface{}, bool) {
	identifier := strings.Trim(strings.TrimSpace(expression), "`")
	pattern := regexp.MustCompile(`(?is)^\s*(?:[a-zA-Z0-9_$]+\s*\.\s*)?` + regexp.QuoteMeta(identifier) + `\s*(<=|>=|=|<|>)\s*(.+?)\s*$`)
	match := pattern.FindStringSubmatch(strings.TrimSpace(condition))
	if len(match) != 3 {
		return "", 0, false
	}
	value, ok := partitionPredicateLiteral(match[2])
	if !ok {
		return "", 0, false
	}
	return match[1], value, true
}

func partitionRowValue(values map[string]interface{}, expression string) (interface{}, bool) {
	identifier := strings.Trim(strings.TrimSpace(expression), "`")
	for name, value := range values {
		if strings.EqualFold(strings.Trim(name, "`"), identifier) {
			return value, true
		}
	}
	return nil, false
}

// secondaryIndexVisible treats metadata written before index visibility was
// persisted as visible. MySQL's primary key is not represented in the
// secondary index manager, so this gate only controls optimizer candidates.
func secondaryIndexVisible(index *manager.Index) bool {
	return index != nil && (!index.VisibilitySet || index.IsVisible)
}

func (se *SelectExecutor) scanSecondaryIndexRows(ctx context.Context, tableMeta *metadata.TableMeta) (bool, error) {
	if se != nil && se.storageManager != nil {
		if tableStorageManager := se.storageManager.GetTableStorageManager(); tableStorageManager != nil {
			if tableInfo, err := tableStorageManager.GetTableStorageInfo(se.schemaName, se.tableName); err == nil && len(tableInfo.Partitions) > 0 {
				// Secondary-index values currently carry only the clustered key.
				// For RANGE/LIST tables that key does not identify the physical
				// partition, so force the partition-aware clustered scan until
				// secondary entries gain a durable partition locator.
				se.lastAccessPath = "partitioned_table_scan"
				return false, nil
			}
		}
	}
	if branches, ok := secondaryIndexCompositeOrEqualityPredicates(se.whereConditions); ok {
		if used, err := se.scanSecondaryIndexCompositeOrRows(ctx, tableMeta, branches); used || err != nil {
			return used, err
		}
	}
	if predicates, ok := secondaryIndexOrPredicates(se.whereConditions); ok {
		if used, err := se.scanSecondaryIndexOrRows(ctx, tableMeta, predicates); used || err != nil {
			return used, err
		}
	}
	if equalityPredicates, inColumn, inValues, ok := secondaryIndexPrefixInPredicates(se.whereConditions); ok {
		if used, err := se.scanSecondaryIndexPrefixInRows(ctx, tableMeta, equalityPredicates, inColumn, inValues); used || err != nil {
			return used, err
		}
	}
	if equalityPredicates, nullColumn, ok := secondaryIndexPrefixNullPredicates(se.whereConditions); ok {
		if used, err := se.scanSecondaryIndexPrefixInRows(ctx, tableMeta, equalityPredicates, nullColumn, []interface{}{nil}); used || err != nil {
			return used, err
		}
	}
	if predicates, ok := secondaryIndexInPredicates(se.whereConditions); ok {
		if used, err := se.scanSecondaryIndexOrRows(ctx, tableMeta, predicates); used || err != nil {
			return used, err
		}
	}
	column, operator, value, ok := secondaryIndexPredicate(se.whereConditions)
	equalityPredicates, equalityOK := secondaryIndexEqualityPredicates(se.whereConditions)
	prefixRangePredicates, rangeColumn, rangeOperator, rangeValue, prefixRangeOK := secondaryIndexPrefixRangePredicates(se.whereConditions)
	rangeAndPredicates, rangeAndOK := secondaryIndexRangeAndPredicates(se.whereConditions)
	if !ok && !equalityOK && !prefixRangeOK && !rangeAndOK {
		se.lastAccessPath = "table_scan"
		return false, nil
	}
	if equalityOK && len(equalityPredicates) > 1 {
		if used, err := se.scanSecondaryIndexAndRows(ctx, tableMeta, equalityPredicates); used || err != nil {
			return used, err
		}
	}
	if rangeAndOK {
		if used, err := se.scanSecondaryIndexRangeAndRows(ctx, tableMeta, rangeAndPredicates); used || err != nil {
			return used, err
		}
	}
	tableStorageManager := se.storageManager.GetTableStorageManager()
	tableInfo, err := tableStorageManager.GetTableStorageInfo(se.schemaName, se.tableName)
	if err != nil {
		return false, err
	}
	indexManager := se.storageManager.GetIndexManager()
	if indexManager == nil {
		return false, nil
	}
	if err := indexManager.EnsureSecondaryIndexesWithRebuild(tableInfo, tableMeta); err != nil {
		return false, err
	}
	var selected *manager.Index
	selectedPrefix := 0
	tableID := manager.SecondaryIndexTableID(se.schemaName, se.tableName)
	for _, index := range indexManager.ListIndexes(tableID) {
		if !secondaryIndexVisible(index) || index.State != manager.IndexStateActive || len(index.Columns) == 0 {
			continue
		}
		if equalityOK {
			prefix := 0
			for prefix < len(index.Columns) {
				if _, exists := equalityPredicates[strings.ToLower(index.Columns[prefix].Name)]; !exists {
					break
				}
				prefix++
			}
			if prefix > selectedPrefix {
				selected, selectedPrefix = index, prefix
			}
			continue
		}
		if prefixRangeOK {
			prefix := 0
			for prefix < len(index.Columns) {
				if _, exists := prefixRangePredicates[strings.ToLower(index.Columns[prefix].Name)]; !exists {
					break
				}
				prefix++
			}
			if prefix > 0 && prefix < len(index.Columns) &&
				strings.EqualFold(index.Columns[prefix].Name, rangeColumn) && prefix > selectedPrefix {
				selected, selectedPrefix = index, prefix
			}
			continue
		}
		if strings.EqualFold(index.Columns[0].Name, column) {
			selected = index
			selectedPrefix = 1
			break
		}
	}
	if selected == nil || selectedPrefix == 0 {
		if ok && isSecondaryIndexSkipScanOperator(operator) {
			if used, err := se.scanSecondaryIndexSkipScan(ctx, tableMeta, column, operator, value); used || err != nil {
				return used, err
			}
		}
		se.lastAccessPath = "table_scan"
		return false, nil
	}
	if err := indexManager.InitializeSecondaryIndex(selected.IndexID); err != nil {
		return false, fmt.Errorf("initialize secondary index %d: %w", selected.IndexID, err)
	}
	indexMeta := metadata.IndexMeta{Name: selected.Name, Columns: indexColumnNames(selected), Unique: selected.IsUnique}
	var startKey, endKey []byte
	if equalityOK {
		row := make(map[string]interface{}, selectedPrefix)
		for index := 0; index < selectedPrefix; index++ {
			name := selected.Columns[index].Name
			row[name] = equalityPredicates[strings.ToLower(name)]
		}
		startKey, endKey, err = manager.SecondaryIndexPrefixEqualityRange(tableID, indexMeta, row, selectedPrefix)
	} else if prefixRangeOK {
		row := make(map[string]interface{}, selectedPrefix)
		for index := 0; index < selectedPrefix; index++ {
			name := selected.Columns[index].Name
			row[name] = prefixRangePredicates[strings.ToLower(name)]
		}
		startKey, endKey, err = manager.SecondaryIndexPrefixEqualityRange(tableID, indexMeta, row, selectedPrefix)
	} else if operator == "=" {
		startKey, endKey, err = manager.SecondaryIndexEqualityRange(tableID, indexMeta, map[string]interface{}{selected.Columns[0].Name: value})
	} else if operator == "is_null" {
		startKey, endKey, err = manager.SecondaryIndexEqualityRange(tableID, indexMeta, map[string]interface{}{selected.Columns[0].Name: nil})
	} else {
		startKey, endKey, err = manager.SecondaryIndexFullRange(tableID, indexMeta)
	}
	if err != nil {
		return false, err
	}
	storageAdapter := NewStorageAdapter(se.tableManager, se.bufferPoolManager, se.storageManager, tableStorageManager)
	tableSchema := tableFromMetadata(tableMeta)
	primaryKeys := make([][]byte, 0)
	if entries, entriesErr := indexManager.RangeSearchEntries(selected.IndexID, startKey, endKey); entriesErr == nil {
		primaryKeys = make([][]byte, 0, len(entries))
		for _, entry := range entries {
			if prefixRangeOK && !secondaryIndexEntryMatchesPredicate(entry.Key, selected, tableMeta, secondaryIndexPredicateBranch{
				column: rangeColumn, operator: rangeOperator, value: rangeValue,
			}) {
				continue
			}
			primaryKey, decodeErr := manager.DecodeSecondaryIndexValue(entry.Value)
			if decodeErr != nil {
				return false, fmt.Errorf("decode secondary index value for index %d: %w", selected.IndexID, decodeErr)
			}
			primaryKeys = append(primaryKeys, primaryKey)
		}
	} else {
		indexRows, rangeErr := indexManager.RangeSearch(selected.IndexID, startKey, endKey)
		if rangeErr != nil {
			return false, fmt.Errorf("secondary index range search failed: %w", rangeErr)
		}
		primaryKeys = make([][]byte, 0, len(indexRows))
		for _, indexRow := range indexRows {
			primaryKey, decodeErr := manager.DecodeSecondaryIndexValue(indexRow.ToByte())
			if decodeErr != nil {
				return false, fmt.Errorf("decode secondary index value for index %d: %w", selected.IndexID, decodeErr)
			}
			primaryKeys = append(primaryKeys, primaryKey)
		}
	}
	records := make([]Record, 0, len(primaryKeys))
	for _, primaryKey := range primaryKeys {
		record, err := storageAdapter.GetRecordByPrimaryKey(ctx, tableInfo.SpaceID, primaryKey, tableSchema)
		if err != nil {
			return false, err
		}
		records = append(records, record)
	}
	se.resultSet = se.applyWhereFilter(records)
	se.rowsExamined = int64(len(primaryKeys))
	se.lastAccessPath = "secondary_index:" + selected.Name
	return true, nil
}

func (se *SelectExecutor) scanSecondaryIndexSkipScan(ctx context.Context, tableMeta *metadata.TableMeta, column, operator, value string) (bool, error) {
	tableStorageManager := se.storageManager.GetTableStorageManager()
	if tableStorageManager == nil {
		return false, nil
	}
	tableInfo, err := tableStorageManager.GetTableStorageInfo(se.schemaName, se.tableName)
	if err != nil {
		return false, err
	}
	indexManager := se.storageManager.GetIndexManager()
	if indexManager == nil {
		return false, nil
	}
	if err := indexManager.EnsureSecondaryIndexesWithRebuild(tableInfo, tableMeta); err != nil {
		return false, err
	}
	tableID := manager.SecondaryIndexTableID(se.schemaName, se.tableName)
	for _, index := range indexManager.ListIndexes(tableID) {
		if !secondaryIndexVisible(index) || index.State != manager.IndexStateActive || len(index.Columns) < 2 {
			continue
		}
		predicatePosition := -1
		for position, indexColumn := range index.Columns {
			if strings.EqualFold(indexColumn.Name, column) {
				predicatePosition = position
				break
			}
		}
		if predicatePosition <= 0 {
			continue
		}
		if err := indexManager.InitializeSecondaryIndex(index.IndexID); err != nil {
			return false, fmt.Errorf("initialize secondary index %d: %w", index.IndexID, err)
		}
		indexMeta := metadata.IndexMeta{Name: index.Name, Columns: indexColumnNames(index), Unique: index.IsUnique}
		startKey, endKey, rangeErr := manager.SecondaryIndexFullRange(tableID, indexMeta)
		if rangeErr != nil {
			return false, rangeErr
		}
		entries, searchErr := indexManager.RangeSearchEntries(index.IndexID, startKey, endKey)
		if searchErr != nil {
			// Older table-level B+Tree facades do not expose key records. Keep
			// the normal table scan as the safe fallback for those stores.
			continue
		}
		prefixValues := make(map[string][]string)
		for _, entry := range entries {
			_, _, _, columns, values, decodeErr := manager.DecodeSecondaryIndexKey(entry.Key)
			if decodeErr != nil || len(columns) <= predicatePosition || len(values) <= predicatePosition {
				continue
			}
			validColumns := true
			for position := 0; position <= predicatePosition; position++ {
				if !strings.EqualFold(columns[position], index.Columns[position].Name) {
					validColumns = false
					break
				}
			}
			if !validColumns {
				continue
			}
			prefix := values[:predicatePosition]
			prefixKey := encodeSkipScanPrefix(prefix)
			if _, exists := prefixValues[prefixKey]; !exists {
				prefixValues[prefixKey] = append([]string(nil), prefix...)
			}
		}
		if len(prefixValues) == 0 {
			continue
		}
		// Enumerating every distinct leading combination is not selective when
		// each index entry contributes its own prefix. In that case the extra
		// prefix probes cannot beat a normal table scan, so keep the safe
		// residual-filtering fallback.
		if len(prefixValues) >= len(entries) {
			continue
		}

		primaryKeys := make(map[string][]byte)
		for _, leadingValues := range prefixValues {
			prefixRow := make(map[string]interface{}, predicatePosition+1)
			for position, leadingValue := range leadingValues {
				prefixRow[index.Columns[position].Name] = leadingValue
			}
			prefixColumns := predicatePosition
			if operator == "=" || operator == "is_null" {
				if operator == "is_null" {
					prefixRow[index.Columns[predicatePosition].Name] = nil
				} else {
					prefixRow[index.Columns[predicatePosition].Name] = value
				}
				prefixColumns++
			}
			prefixStart, prefixEnd, prefixErr := manager.SecondaryIndexPrefixEqualityRange(tableID, indexMeta, prefixRow, prefixColumns)
			if prefixErr != nil {
				return false, prefixErr
			}
			indexRows, prefixSearchErr := indexManager.RangeSearch(index.IndexID, prefixStart, prefixEnd)
			if prefixSearchErr != nil {
				return false, fmt.Errorf("secondary index skip scan failed: %w", prefixSearchErr)
			}
			for _, indexRow := range indexRows {
				primaryKey, decodeErr := manager.DecodeSecondaryIndexValue(indexRow.ToByte())
				if decodeErr != nil {
					return false, fmt.Errorf("decode secondary index value for index %d: %w", index.IndexID, decodeErr)
				}
				primaryKeys[string(primaryKey)] = primaryKey
			}
		}

		storageAdapter := NewStorageAdapter(se.tableManager, se.bufferPoolManager, se.storageManager, tableStorageManager)
		tableSchema := tableFromMetadata(tableMeta)
		records := make([]Record, 0, len(primaryKeys))
		for _, primaryKey := range primaryKeys {
			record, lookupErr := storageAdapter.GetRecordByPrimaryKey(ctx, tableInfo.SpaceID, primaryKey, tableSchema)
			if lookupErr != nil {
				return false, lookupErr
			}
			records = append(records, record)
		}
		se.resultSet = se.applyWhereFilter(records)
		se.rowsExamined = int64(len(records))
		se.lastAccessPath = "secondary_index:" + index.Name + "_skip_scan"
		return true, nil
	}
	return false, nil
}

func isSecondaryIndexSkipScanOperator(operator string) bool {
	switch strings.ToLower(strings.TrimSpace(operator)) {
	case "=", "is_null", "is_not_null", ">", ">=", "<", "<=":
		return true
	default:
		return false
	}
}

func encodeSkipScanPrefix(values []string) string {
	var builder strings.Builder
	for _, value := range values {
		fmt.Fprintf(&builder, "%d:", len(value))
		builder.WriteString(value)
	}
	return builder.String()
}

// secondaryIndexCompositeOrBranch is one DNF branch made entirely of
// equality predicates. It is intentionally narrower than a general boolean
// expression: only branches that can form a leading composite-index prefix
// are eligible for this access path.
type secondaryIndexCompositeOrBranch struct {
	equalities    map[string]string
	rangeColumn   string
	rangeOperator string
	rangeValue    string
}

func secondaryIndexCompositeOrEqualityPredicates(conditions []string) ([]secondaryIndexCompositeOrBranch, bool) {
	parts := make([]string, 0, len(conditions))
	for _, condition := range conditions {
		parts = append(parts, splitTopLevelDisjunctions(condition)...)
	}
	if len(parts) < 2 {
		return nil, false
	}
	branches := make([]secondaryIndexCompositeOrBranch, 0, len(parts))
	for _, part := range parts {
		part = trimSecondaryIndexOuterParentheses(part)
		conjunctions := splitSecondaryIndexConjunctions(part)
		if len(conjunctions) < 2 {
			return nil, false
		}
		equalities := make(map[string]string, len(conjunctions))
		rangeColumn, rangeOperator, rangeValue := "", "", ""
		for _, conjunction := range conjunctions {
			column, value, ok := parseSecondaryIndexEquality(conjunction)
			if ok {
				key := strings.ToLower(column)
				if _, duplicate := equalities[key]; duplicate {
					return nil, false
				}
				equalities[key] = value
				continue
			}
			var operator string
			column, operator, value, ok = parseSecondaryIndexNullSafeComparison(conjunction)
			if !ok {
				column, operator, value, ok = parseSecondaryIndexComparison(conjunction)
			}
			if !ok {
				column, operator, value, ok = parseSecondaryIndexBetween(conjunction)
			}
			if !ok {
				column, operator, value, ok = parseSecondaryIndexNullComparison(conjunction)
			}
			if !ok {
				column, operator, value, ok = parseSecondaryIndexLikeComparison(conjunction)
			}
			if !ok {
				column, operator, value, ok = parseSecondaryIndexNotInComparison(conjunction)
			}
			if !ok {
				column, operator, value, ok = parseSecondaryIndexInComparison(conjunction)
			}
			if !ok || !secondaryIndexCompositeOrBranchPredicate(operator) || rangeColumn != "" {
				return nil, false
			}
			rangeColumn, rangeOperator, rangeValue = column, operator, value
		}
		if len(equalities) == 0 || len(equalities) < 2 && rangeColumn == "" {
			return nil, false
		}
		branches = append(branches, secondaryIndexCompositeOrBranch{
			equalities:    equalities,
			rangeColumn:   rangeColumn,
			rangeOperator: rangeOperator,
			rangeValue:    rangeValue,
		})
	}
	return branches, true
}

func secondaryIndexCompositeOrBranchPredicate(operator string) bool {
	return isSecondaryIndexRangeOperator(operator) || operator == "between" || operator == "not_between" ||
		operator == "is_null" || operator == "is_not_null" || operator == "like" || operator == "not_like" || operator == "in" || operator == "not_in" || operator == "<>" || operator == "!=" || operator == "<=>"
}

func trimSecondaryIndexOuterParentheses(condition string) string {
	condition = strings.TrimSpace(condition)
	for len(condition) >= 2 && condition[0] == '(' {
		close := matchingParenIndex(condition, 0)
		if close != len(condition)-1 {
			break
		}
		condition = strings.TrimSpace(condition[1 : len(condition)-1])
	}
	return condition
}

func (se *SelectExecutor) scanSecondaryIndexCompositeOrRows(ctx context.Context, tableMeta *metadata.TableMeta, branches []secondaryIndexCompositeOrBranch) (bool, error) {
	tableStorageManager := se.storageManager.GetTableStorageManager()
	if tableStorageManager == nil {
		return false, nil
	}
	tableInfo, err := tableStorageManager.GetTableStorageInfo(se.schemaName, se.tableName)
	if err != nil {
		return false, err
	}
	indexManager := se.storageManager.GetIndexManager()
	if indexManager == nil {
		return false, nil
	}
	if err := indexManager.EnsureSecondaryIndexesWithRebuild(tableInfo, tableMeta); err != nil {
		return false, err
	}

	tableID := manager.SecondaryIndexTableID(se.schemaName, se.tableName)
	var selected *manager.Index
	selectedPrefix := 0
	for _, index := range indexManager.ListIndexes(tableID) {
		if !secondaryIndexVisible(index) || index.State != manager.IndexStateActive || len(index.Columns) < 2 {
			continue
		}
		prefix := len(index.Columns)
		for _, branch := range branches {
			branchPrefix := 0
			for branchPrefix < len(index.Columns) {
				if _, exists := branch.equalities[strings.ToLower(index.Columns[branchPrefix].Name)]; !exists {
					break
				}
				branchPrefix++
			}
			if branch.rangeColumn != "" {
				if branchPrefix == 0 || branchPrefix >= len(index.Columns) ||
					!strings.EqualFold(index.Columns[branchPrefix].Name, branch.rangeColumn) {
					branchPrefix = 0
				}
			} else if branchPrefix < 2 {
				branchPrefix = 0
			}
			if branchPrefix < prefix {
				prefix = branchPrefix
			}
		}
		if prefix > selectedPrefix {
			selected, selectedPrefix = index, prefix
		}
	}
	if selected == nil || selectedPrefix == 0 {
		return false, nil
	}
	if err := indexManager.InitializeSecondaryIndex(selected.IndexID); err != nil {
		return false, fmt.Errorf("initialize secondary index %d: %w", selected.IndexID, err)
	}

	indexMeta := metadata.IndexMeta{Name: selected.Name, Columns: indexColumnNames(selected), Unique: selected.IsUnique}
	primaryKeys := make(map[string][]byte)
	for _, branch := range branches {
		row := make(map[string]interface{}, selectedPrefix)
		for index := 0; index < selectedPrefix; index++ {
			column := selected.Columns[index].Name
			row[column] = branch.equalities[strings.ToLower(column)]
		}
		startKey, endKey, rangeErr := manager.SecondaryIndexPrefixEqualityRange(tableID, indexMeta, row, selectedPrefix)
		if rangeErr != nil {
			return false, rangeErr
		}
		indexRows, searchErr := indexManager.RangeSearch(selected.IndexID, startKey, endKey)
		if searchErr != nil {
			return false, fmt.Errorf("secondary index range search failed: %w", searchErr)
		}
		for _, indexRow := range indexRows {
			primaryKey, decodeErr := manager.DecodeSecondaryIndexValue(indexRow.ToByte())
			if decodeErr != nil {
				return false, fmt.Errorf("decode secondary index value for index %d: %w", selected.IndexID, decodeErr)
			}
			primaryKeys[string(primaryKey)] = primaryKey
		}
	}

	storageAdapter := NewStorageAdapter(se.tableManager, se.bufferPoolManager, se.storageManager, tableStorageManager)
	tableSchema := tableFromMetadata(tableMeta)
	records := make([]Record, 0, len(primaryKeys))
	for _, primaryKey := range primaryKeys {
		record, lookupErr := storageAdapter.GetRecordByPrimaryKey(ctx, tableInfo.SpaceID, primaryKey, tableSchema)
		if lookupErr != nil {
			return false, lookupErr
		}
		records = append(records, record)
	}
	se.resultSet = se.applyWhereFilter(records)
	se.rowsExamined = int64(len(records))
	se.lastAccessPath = "secondary_index:" + selected.Name
	return true, nil
}

func (se *SelectExecutor) scanSecondaryIndexPrefixInRows(ctx context.Context, tableMeta *metadata.TableMeta, equalityPredicates map[string]string, inColumn string, inValues []interface{}) (bool, error) {
	tableStorageManager := se.storageManager.GetTableStorageManager()
	if tableStorageManager == nil {
		return false, nil
	}
	tableInfo, err := tableStorageManager.GetTableStorageInfo(se.schemaName, se.tableName)
	if err != nil {
		return false, err
	}
	indexManager := se.storageManager.GetIndexManager()
	if indexManager == nil {
		return false, nil
	}
	if err := indexManager.EnsureSecondaryIndexesWithRebuild(tableInfo, tableMeta); err != nil {
		return false, err
	}

	tableID := manager.SecondaryIndexTableID(se.schemaName, se.tableName)
	var selected *manager.Index
	selectedPrefix := 0
	for _, index := range indexManager.ListIndexes(tableID) {
		if !secondaryIndexVisible(index) || index.State != manager.IndexStateActive || len(index.Columns) == 0 {
			continue
		}
		prefix := 0
		for prefix < len(index.Columns) {
			if _, exists := equalityPredicates[strings.ToLower(index.Columns[prefix].Name)]; !exists {
				break
			}
			prefix++
		}
		if prefix == 0 || prefix >= len(index.Columns) || !strings.EqualFold(index.Columns[prefix].Name, inColumn) {
			continue
		}
		if prefix > selectedPrefix {
			selected, selectedPrefix = index, prefix
		}
	}
	if selected == nil {
		return false, nil
	}
	if err := indexManager.InitializeSecondaryIndex(selected.IndexID); err != nil {
		return false, fmt.Errorf("initialize secondary index %d: %w", selected.IndexID, err)
	}
	indexMeta := metadata.IndexMeta{Name: selected.Name, Columns: indexColumnNames(selected), Unique: selected.IsUnique}
	storageAdapter := NewStorageAdapter(se.tableManager, se.bufferPoolManager, se.storageManager, tableStorageManager)
	tableSchema := tableFromMetadata(tableMeta)
	primaryKeys := make(map[string][]byte)
	for _, inValue := range inValues {
		row := make(map[string]interface{}, selectedPrefix+1)
		for index := 0; index < selectedPrefix; index++ {
			name := selected.Columns[index].Name
			row[name] = equalityPredicates[strings.ToLower(name)]
		}
		row[selected.Columns[selectedPrefix].Name] = inValue
		startKey, endKey, err := manager.SecondaryIndexPrefixEqualityRange(tableID, indexMeta, row, selectedPrefix+1)
		if err != nil {
			return false, err
		}
		indexRows, err := indexManager.RangeSearch(selected.IndexID, startKey, endKey)
		if err != nil {
			return false, fmt.Errorf("secondary index range search failed: %w", err)
		}
		for _, indexRow := range indexRows {
			primaryKey, err := manager.DecodeSecondaryIndexValue(indexRow.ToByte())
			if err != nil {
				return false, fmt.Errorf("decode secondary index value for index %d: %w", selected.IndexID, err)
			}
			primaryKeys[string(primaryKey)] = primaryKey
		}
	}
	records := make([]Record, 0, len(primaryKeys))
	for _, primaryKey := range primaryKeys {
		record, err := storageAdapter.GetRecordByPrimaryKey(ctx, tableInfo.SpaceID, primaryKey, tableSchema)
		if err != nil {
			return false, err
		}
		records = append(records, record)
	}
	se.resultSet = se.applyWhereFilter(records)
	se.lastAccessPath = "secondary_index:" + selected.Name
	return true, nil
}

func (se *SelectExecutor) scanSecondaryIndexAndRows(ctx context.Context, tableMeta *metadata.TableMeta, predicates map[string]string) (bool, error) {
	tableStorageManager := se.storageManager.GetTableStorageManager()
	if tableStorageManager == nil {
		return false, nil
	}
	tableInfo, err := tableStorageManager.GetTableStorageInfo(se.schemaName, se.tableName)
	if err != nil {
		return false, err
	}
	indexManager := se.storageManager.GetIndexManager()
	if indexManager == nil {
		return false, nil
	}
	if err := indexManager.EnsureSecondaryIndexesWithRebuild(tableInfo, tableMeta); err != nil {
		return false, err
	}

	columns := make([]string, 0, len(predicates))
	for column := range predicates {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	tableID := manager.SecondaryIndexTableID(se.schemaName, se.tableName)
	indexes := indexManager.ListIndexes(tableID)
	selected := make([]*manager.Index, 0, len(columns))
	usedIDs := make(map[uint64]struct{}, len(columns))
	for _, column := range columns {
		var match *manager.Index
		for _, index := range indexes {
			if !secondaryIndexVisible(index) || index.State != manager.IndexStateActive || len(index.Columns) == 0 ||
				!strings.EqualFold(index.Columns[0].Name, column) {
				continue
			}
			if _, used := usedIDs[index.IndexID]; used {
				continue
			}
			match = index
			break
		}
		if match == nil {
			return false, nil
		}
		selected = append(selected, match)
		usedIDs[match.IndexID] = struct{}{}
	}
	if len(selected) < 2 {
		return false, nil
	}

	branches := make([][]string, 0, len(selected))
	primaryKeys := make(map[string][]byte)
	for position, index := range selected {
		if err := indexManager.InitializeSecondaryIndex(index.IndexID); err != nil {
			return false, fmt.Errorf("initialize secondary index %d: %w", index.IndexID, err)
		}
		indexMeta := metadata.IndexMeta{Name: index.Name, Columns: indexColumnNames(index), Unique: index.IsUnique}
		startKey, endKey, err := manager.SecondaryIndexPrefixEqualityRange(tableID, indexMeta, map[string]interface{}{
			index.Columns[0].Name: predicates[columns[position]],
		}, 1)
		if err != nil {
			return false, err
		}
		indexRows, err := indexManager.RangeSearch(index.IndexID, startKey, endKey)
		if err != nil {
			return false, fmt.Errorf("secondary index range search failed: %w", err)
		}
		branch := make([]string, 0, len(indexRows))
		seen := make(map[string]struct{}, len(indexRows))
		for _, indexRow := range indexRows {
			primaryKey, err := manager.DecodeSecondaryIndexValue(indexRow.ToByte())
			if err != nil {
				return false, fmt.Errorf("decode secondary index value for index %d: %w", index.IndexID, err)
			}
			key := string(primaryKey)
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			branch = append(branch, key)
			primaryKeys[key] = primaryKey
		}
		branches = append(branches, branch)
	}

	intersected := intersectSecondaryIndexPrimaryKeys(branches)
	storageAdapter := NewStorageAdapter(se.tableManager, se.bufferPoolManager, se.storageManager, tableStorageManager)
	tableSchema := tableFromMetadata(tableMeta)
	records := make([]Record, 0, len(intersected))
	for _, key := range intersected {
		record, err := storageAdapter.GetRecordByPrimaryKey(ctx, tableInfo.SpaceID, primaryKeys[key], tableSchema)
		if err != nil {
			return false, err
		}
		records = append(records, record)
	}
	se.resultSet = se.applyWhereFilter(records)
	se.rowsExamined = int64(len(records))
	paths := make([]string, 0, len(selected))
	for _, index := range selected {
		paths = append(paths, index.Name)
	}
	se.lastAccessPath = "index_merge_and:" + strings.Join(paths, ",")
	return true, nil
}

func (se *SelectExecutor) scanSecondaryIndexRangeAndRows(ctx context.Context, tableMeta *metadata.TableMeta, predicates []secondaryIndexPredicateBranch) (bool, error) {
	tableStorageManager := se.storageManager.GetTableStorageManager()
	if tableStorageManager == nil {
		return false, nil
	}
	tableInfo, err := tableStorageManager.GetTableStorageInfo(se.schemaName, se.tableName)
	if err != nil {
		return false, err
	}
	indexManager := se.storageManager.GetIndexManager()
	if indexManager == nil {
		return false, nil
	}
	if err := indexManager.EnsureSecondaryIndexesWithRebuild(tableInfo, tableMeta); err != nil {
		return false, err
	}

	ordered := append([]secondaryIndexPredicateBranch(nil), predicates...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return strings.ToLower(ordered[i].column) < strings.ToLower(ordered[j].column)
	})
	tableID := manager.SecondaryIndexTableID(se.schemaName, se.tableName)
	indexes := indexManager.ListIndexes(tableID)
	selected := make([]*manager.Index, 0, len(ordered))
	for _, predicate := range ordered {
		var match *manager.Index
		for _, index := range indexes {
			if !secondaryIndexVisible(index) || index.State != manager.IndexStateActive || len(index.Columns) == 0 ||
				!strings.EqualFold(index.Columns[0].Name, predicate.column) {
				continue
			}
			match = index
			break
		}
		if match == nil {
			return false, nil
		}
		selected = append(selected, match)
	}
	if len(selected) < 2 {
		return false, nil
	}

	branches := make([][]string, 0, len(selected))
	primaryKeys := make(map[string][]byte)
	for position, index := range selected {
		if err := indexManager.InitializeSecondaryIndex(index.IndexID); err != nil {
			return false, fmt.Errorf("initialize secondary index %d: %w", index.IndexID, err)
		}
		indexMeta := metadata.IndexMeta{Name: index.Name, Columns: indexColumnNames(index), Unique: index.IsUnique}
		var startKey, endKey []byte
		predicate := ordered[position]
		if predicate.operator == "=" {
			startKey, endKey, err = manager.SecondaryIndexPrefixEqualityRange(tableID, indexMeta, map[string]interface{}{
				index.Columns[0].Name: predicate.value,
			}, 1)
		} else {
			// Current durable keys are not type-sortable. A range branch scans
			// this index namespace and relies on the exact residual predicate.
			startKey, endKey, err = manager.SecondaryIndexFullRange(tableID, indexMeta)
		}
		if err != nil {
			return false, err
		}
		branch := make([]string, 0)
		seen := make(map[string]struct{})
		if entries, entriesErr := indexManager.RangeSearchEntries(index.IndexID, startKey, endKey); entriesErr == nil {
			branch = make([]string, 0, len(entries))
			seen = make(map[string]struct{}, len(entries))
			for _, entry := range entries {
				if predicate.operator != "=" && !secondaryIndexEntryMatchesPredicate(entry.Key, index, tableMeta, predicate) {
					continue
				}
				primaryKey, decodeErr := manager.DecodeSecondaryIndexValue(entry.Value)
				if decodeErr != nil {
					return false, fmt.Errorf("decode secondary index value for index %d: %w", index.IndexID, decodeErr)
				}
				key := string(primaryKey)
				if _, duplicate := seen[key]; duplicate {
					continue
				}
				seen[key] = struct{}{}
				branch = append(branch, key)
				primaryKeys[key] = primaryKey
			}
		} else {
			indexRows, rangeErr := indexManager.RangeSearch(index.IndexID, startKey, endKey)
			if rangeErr != nil {
				return false, fmt.Errorf("secondary index range search failed: %w", rangeErr)
			}
			branch = make([]string, 0, len(indexRows))
			seen = make(map[string]struct{}, len(indexRows))
			for _, indexRow := range indexRows {
				primaryKey, decodeErr := manager.DecodeSecondaryIndexValue(indexRow.ToByte())
				if decodeErr != nil {
					return false, fmt.Errorf("decode secondary index value for index %d: %w", index.IndexID, decodeErr)
				}
				key := string(primaryKey)
				if _, duplicate := seen[key]; duplicate {
					continue
				}
				seen[key] = struct{}{}
				branch = append(branch, key)
				primaryKeys[key] = primaryKey
			}
		}
		branches = append(branches, branch)
	}

	intersected := intersectSecondaryIndexPrimaryKeys(branches)
	storageAdapter := NewStorageAdapter(se.tableManager, se.bufferPoolManager, se.storageManager, tableStorageManager)
	tableSchema := tableFromMetadata(tableMeta)
	records := make([]Record, 0, len(intersected))
	for _, key := range intersected {
		record, err := storageAdapter.GetRecordByPrimaryKey(ctx, tableInfo.SpaceID, primaryKeys[key], tableSchema)
		if err != nil {
			return false, err
		}
		records = append(records, record)
	}
	se.resultSet = se.applyWhereFilter(records)
	se.rowsExamined = int64(len(records))
	paths := make([]string, 0, len(selected))
	for _, index := range selected {
		paths = append(paths, index.Name)
	}
	se.lastAccessPath = "index_merge_and:" + strings.Join(paths, ",")
	return true, nil
}

func intersectSecondaryIndexPrimaryKeys(branches [][]string) []string {
	if len(branches) == 0 {
		return nil
	}
	counts := make(map[string]int)
	for _, branch := range branches {
		seen := make(map[string]struct{}, len(branch))
		for _, key := range branch {
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			counts[key]++
		}
	}
	result := make([]string, 0)
	for _, key := range branches[0] {
		if counts[key] == len(branches) {
			result = append(result, key)
			delete(counts, key)
		}
	}
	return result
}

type secondaryIndexPredicateBranch struct {
	column   string
	operator string
	value    string
}

func (se *SelectExecutor) scanSecondaryIndexOrRows(ctx context.Context, tableMeta *metadata.TableMeta, predicates []secondaryIndexPredicateBranch) (bool, error) {
	tableStorageManager := se.storageManager.GetTableStorageManager()
	tableInfo, err := tableStorageManager.GetTableStorageInfo(se.schemaName, se.tableName)
	if err != nil {
		return false, err
	}
	indexManager := se.storageManager.GetIndexManager()
	if indexManager == nil {
		return false, nil
	}
	if err := indexManager.EnsureSecondaryIndexesWithRebuild(tableInfo, tableMeta); err != nil {
		return false, err
	}
	tableID := manager.SecondaryIndexTableID(se.schemaName, se.tableName)
	selectedIndexes := make([]*manager.Index, 0, len(predicates))
	selectedPredicates := make([]secondaryIndexPredicateBranch, 0, len(predicates))
	for _, predicate := range predicates {
		var selected *manager.Index
		for _, index := range indexManager.ListIndexes(tableID) {
			if secondaryIndexVisible(index) && index.State == manager.IndexStateActive && len(index.Columns) == 1 && strings.EqualFold(index.Columns[0].Name, predicate.column) {
				selected = index
				break
			}
		}
		if selected == nil {
			continue
		}
		selectedIndexes = append(selectedIndexes, selected)
		selectedPredicates = append(selectedPredicates, predicate)
	}
	if len(selectedIndexes) == 0 {
		return false, nil
	}
	storageAdapter := NewStorageAdapter(se.tableManager, se.bufferPoolManager, se.storageManager, tableStorageManager)
	tableSchema := tableFromMetadata(tableMeta)
	records := make([]Record, 0)
	seenPrimaryKeys := make(map[string]struct{})
	accessPaths := make([]string, 0, len(selectedIndexes))
	for indexPosition, selected := range selectedIndexes {
		if err := indexManager.InitializeSecondaryIndex(selected.IndexID); err != nil {
			return false, fmt.Errorf("initialize secondary index %d: %w", selected.IndexID, err)
		}
		predicate := selectedPredicates[indexPosition]
		indexMeta := metadata.IndexMeta{Name: selected.Name, Columns: indexColumnNames(selected), Unique: selected.IsUnique}
		var startKey, endKey []byte
		if predicate.operator == "=" {
			startKey, endKey, err = manager.SecondaryIndexEqualityRange(tableID, indexMeta, map[string]interface{}{selected.Columns[0].Name: predicate.value})
		} else {
			// Secondary keys use a durable string representation rather than a
			// type-aware sortable encoding. Keep range branches correct by
			// scanning this index's bounded namespace and applying the original
			// predicate after clustered lookup.
			startKey, endKey, err = manager.SecondaryIndexFullRange(tableID, indexMeta)
		}
		if err != nil {
			return false, err
		}
		accessPaths = append(accessPaths, selected.Name)
		if entries, entriesErr := indexManager.RangeSearchEntries(selected.IndexID, startKey, endKey); entriesErr == nil {
			for _, entry := range entries {
				if predicate.operator != "=" && !secondaryIndexEntryMatchesPredicate(entry.Key, selected, tableMeta, predicate) {
					continue
				}
				primaryKey, decodeErr := manager.DecodeSecondaryIndexValue(entry.Value)
				if decodeErr != nil {
					return false, fmt.Errorf("decode secondary index value for index %d: %w", selected.IndexID, decodeErr)
				}
				primaryKeyKey := string(primaryKey)
				if _, exists := seenPrimaryKeys[primaryKeyKey]; exists {
					continue
				}
				seenPrimaryKeys[primaryKeyKey] = struct{}{}
				record, recordErr := storageAdapter.GetRecordByPrimaryKey(ctx, tableInfo.SpaceID, primaryKey, tableSchema)
				if recordErr != nil {
					return false, recordErr
				}
				records = append(records, record)
			}
			continue
		}
		indexRows, err := indexManager.RangeSearch(selected.IndexID, startKey, endKey)
		if err != nil {
			return false, fmt.Errorf("secondary index range search failed: %w", err)
		}
		for _, indexRow := range indexRows {
			primaryKey, err := manager.DecodeSecondaryIndexValue(indexRow.ToByte())
			if err != nil {
				return false, fmt.Errorf("decode secondary index value for index %d: %w", selected.IndexID, err)
			}
			primaryKeyKey := string(primaryKey)
			if _, exists := seenPrimaryKeys[primaryKeyKey]; exists {
				continue
			}
			seenPrimaryKeys[primaryKeyKey] = struct{}{}
			record, err := storageAdapter.GetRecordByPrimaryKey(ctx, tableInfo.SpaceID, primaryKey, tableSchema)
			if err != nil {
				return false, err
			}
			records = append(records, record)
		}
	}
	se.resultSet = se.applyWhereFilter(records)
	se.rowsExamined = int64(len(records))
	se.lastAccessPath = "index_merge_or:" + strings.Join(accessPaths, ",")
	return true, nil
}

// secondaryIndexEntryMatchesPredicate applies a typed residual comparison to
// the indexed value before the executor performs a clustered lookup. Durable
// secondary keys intentionally retain a stable textual payload, so numeric
// values such as "10" and "2" cannot be compared bytewise. Enhanced index
// readers expose the key and let range branches avoid fetching non-matching
// rows while preserving the legacy RangeSearch fallback.
func secondaryIndexEntryMatchesPredicate(key []byte, index *manager.Index, tableMeta *metadata.TableMeta, predicate secondaryIndexPredicateBranch) bool {
	if index == nil || len(index.Columns) == 0 || tableMeta == nil {
		return true
	}
	_, _, _, columns, values, err := manager.DecodeSecondaryIndexKey(key)
	if err != nil || len(columns) == 0 || len(values) == 0 {
		return true
	}
	valuePosition := -1
	for position, column := range columns {
		if strings.EqualFold(column, predicate.column) {
			valuePosition = position
			break
		}
	}
	if valuePosition < 0 || valuePosition >= len(values) {
		return true
	}
	var columnMeta *metadata.ColumnMeta
	for _, column := range tableMeta.Columns {
		if column != nil && strings.EqualFold(column.Name, predicate.column) {
			columnMeta = column
			break
		}
	}
	if columnMeta == nil {
		return true
	}
	return compareSecondaryIndexValues(values[valuePosition], predicate.value, columnMeta, predicate.operator)
}

func compareSecondaryIndexValues(actual, expected string, column *metadata.ColumnMeta, operator string) bool {
	if column == nil {
		return true
	}
	if actual == "<nil>" || expected == "<nil>" {
		return false
	}
	compare := 0
	switch column.Type {
	case metadata.TypeTinyInt, metadata.TypeSmallInt, metadata.TypeMediumInt, metadata.TypeInt,
		metadata.TypeBigInt, metadata.TypeBit, metadata.TypeYear:
		actualInt, actualErr := strconv.ParseInt(strings.TrimSpace(actual), 10, 64)
		expectedInt, expectedErr := strconv.ParseInt(strings.TrimSpace(expected), 10, 64)
		if actualErr != nil || expectedErr != nil {
			return true
		}
		switch {
		case actualInt < expectedInt:
			compare = -1
		case actualInt > expectedInt:
			compare = 1
		}
	case metadata.TypeFloat, metadata.TypeDouble, metadata.TypeDecimal:
		actualFloat, actualErr := strconv.ParseFloat(strings.TrimSpace(actual), 64)
		expectedFloat, expectedErr := strconv.ParseFloat(strings.TrimSpace(expected), 64)
		if actualErr != nil || expectedErr != nil {
			return true
		}
		switch {
		case actualFloat < expectedFloat:
			compare = -1
		case actualFloat > expectedFloat:
			compare = 1
		}
	default:
		compare = strings.Compare(actual, expected)
	}
	switch operator {
	case "<":
		return compare < 0
	case "<=":
		return compare <= 0
	case ">":
		return compare > 0
	case ">=":
		return compare >= 0
	case "=":
		return compare == 0
	default:
		return true
	}
}

func tableFromMetadata(tableMeta *metadata.TableMeta) *metadata.Table {
	table := metadata.NewTable(tableMeta.Name)
	for _, columnMeta := range tableMeta.Columns {
		if columnMeta == nil {
			continue
		}
		table.AddColumn(&metadata.Column{
			Name:            columnMeta.Name,
			DataType:        columnMeta.Type,
			CharMaxLength:   columnMeta.Length,
			IsNullable:      columnMeta.IsNullable,
			DefaultValue:    columnMeta.DefaultValue,
			IsAutoIncrement: columnMeta.IsAutoIncrement,
			Charset:         columnMeta.Charset,
			Collation:       columnMeta.Collation,
			Comment:         columnMeta.Comment,
		})
	}
	if len(tableMeta.PrimaryKey) > 0 {
		table.PrimaryKey = &metadata.Index{Name: "PRIMARY", Columns: append([]string(nil), tableMeta.PrimaryKey...), IsPrimary: true}
	} else {
		primaryColumns := effectivePrimaryKeyColumns(tableMeta)
		if len(primaryColumns) > 0 {
			table.PrimaryKey = &metadata.Index{Name: "PRIMARY", Columns: append([]string(nil), primaryColumns...), IsPrimary: true}
		}
	}
	return table
}

func secondaryIndexPredicate(conditions []string) (column, operator, value string, ok bool) {
	if len(conditions) != 1 {
		return "", "", "", false
	}
	condition := strings.TrimSpace(conditions[0])
	lowerCondition := strings.ToLower(condition)
	if notInAt := strings.Index(lowerCondition, " not in "); notInAt > 0 {
		column = strings.Trim(strings.TrimSpace(condition[:notInAt]), "` \t\r\n")
		list := strings.TrimSpace(condition[notInAt+len(" not in "):])
		if column == "" || len(list) < 2 || list[0] != '(' || list[len(list)-1] != ')' || strings.Contains(strings.ToLower(list), "select") {
			return "", "", "", false
		}
		items := splitTopLevelComma(strings.TrimSpace(list[1 : len(list)-1]))
		if len(items) == 0 {
			return "", "", "", false
		}
		for _, item := range items {
			if strings.TrimSpace(item) == "" || strings.ContainsAny(item, "()") {
				return "", "", "", false
			}
		}
		return column, "not_in", list, true
	}
	if notBetweenAt := strings.Index(lowerCondition, " not between "); notBetweenAt > 0 {
		rangeValues := lowerCondition[notBetweenAt+len(" not between "):]
		if strings.Count(rangeValues, " and ") == 1 {
			column = strings.Trim(strings.TrimSpace(condition[:notBetweenAt]), "`")
			if column != "" {
				return column, "not_between", "", true
			}
		}
		return "", "", "", false
	}
	if betweenAt := strings.Index(lowerCondition, " between "); betweenAt > 0 {
		rangeValues := lowerCondition[betweenAt+len(" between "):]
		if strings.Count(rangeValues, " and ") == 1 {
			column = strings.Trim(strings.TrimSpace(condition[:betweenAt]), "`")
			if column != "" {
				return column, "between", "", true
			}
		}
		return "", "", "", false
	}
	if notLikeAt := strings.Index(lowerCondition, " not like "); notLikeAt > 0 {
		column = strings.Trim(strings.TrimSpace(condition[:notLikeAt]), "`")
		pattern := strings.TrimSpace(condition[notLikeAt+len(" not like "):])
		value = strings.Trim(pattern, "'\"")
		if column == "" || pattern == "" || strings.Contains(strings.ToLower(pattern), "select") || strings.ContainsAny(pattern, "()") {
			return "", "", "", false
		}
		return column, "not_like", value, true
	}
	if likeAt := strings.Index(lowerCondition, " like "); likeAt > 0 {
		column = strings.Trim(strings.TrimSpace(condition[:likeAt]), "`")
		value = strings.Trim(strings.TrimSpace(condition[likeAt+len(" like "):]), "'\"")
		// Only a simple prefix pattern is eligible for this access path. The
		// durable compatibility index encoding is not type-aware sortable, so
		// the executor scans the index namespace and reapplies LIKE exactly.
		if column == "" || value == "" || !strings.HasSuffix(value, "%") {
			return "", "", "", false
		}
		prefix := strings.TrimSuffix(value, "%")
		if strings.ContainsAny(prefix, "%_") {
			return "", "", "", false
		}
		return column, "like", value, true
	}
	if strings.HasSuffix(lowerCondition, " is not null") {
		column = strings.Trim(strings.TrimSpace(condition[:len(condition)-len(" is not null")]), "`")
		if column != "" {
			return column, "is_not_null", "", true
		}
		return "", "", "", false
	}
	if strings.HasSuffix(lowerCondition, " is null") {
		column = strings.Trim(strings.TrimSpace(condition[:len(condition)-len(" is null")]), "`")
		if column != "" {
			return column, "is_null", "", true
		}
		return "", "", "", false
	}
	if strings.Contains(lowerCondition, " and ") {
		return "", "", "", false
	}
	for _, candidate := range []string{">=", "<=", "="} {
		parts := strings.SplitN(condition, candidate, 2)
		if len(parts) != 2 || strings.ContainsAny(parts[0], "<>") {
			continue
		}
		column = strings.Trim(strings.TrimSpace(parts[0]), "`")
		value = strings.Trim(strings.TrimSpace(parts[1]), "'\"")
		if column != "" && value != "" {
			return column, candidate, value, true
		}
	}
	return "", "", "", false
}

func secondaryIndexEqualityPredicates(conditions []string) (map[string]string, bool) {
	if len(conditions) == 0 {
		return nil, false
	}
	parts := make([]string, 0, len(conditions))
	for _, condition := range conditions {
		parts = append(parts, splitTopLevelConjunctions(condition)...)
	}
	if len(parts) == 0 {
		return nil, false
	}
	predicates := make(map[string]string, len(parts))
	for _, part := range parts {
		column, value, ok := parseSecondaryIndexEquality(part)
		if !ok {
			return nil, false
		}
		predicates[strings.ToLower(column)] = value
	}
	return predicates, true
}

func secondaryIndexRangeAndPredicates(conditions []string) ([]secondaryIndexPredicateBranch, bool) {
	parts := make([]string, 0, len(conditions))
	for _, condition := range conditions {
		parts = append(parts, splitTopLevelConjunctions(condition)...)
	}
	if len(parts) < 2 {
		return nil, false
	}
	seenColumns := make(map[string]struct{}, len(parts))
	predicates := make([]secondaryIndexPredicateBranch, 0, len(parts))
	for _, part := range parts {
		column, operator, value, ok := parseSecondaryIndexComparison(part)
		if !ok || (operator != "=" && !isSecondaryIndexRangeOperator(operator)) {
			return nil, false
		}
		key := strings.ToLower(column)
		if _, duplicate := seenColumns[key]; duplicate {
			return nil, false
		}
		seenColumns[key] = struct{}{}
		predicates = append(predicates, secondaryIndexPredicateBranch{column: column, operator: operator, value: value})
	}
	if len(seenColumns) < 2 {
		return nil, false
	}
	return predicates, true
}

// secondaryIndexPrefixInPredicates extracts equality predicates followed by a
// constant IN list on the next index column. The executor expands the list
// into bounded prefix ranges and reapplies the full WHERE clause afterwards.
func secondaryIndexPrefixInPredicates(conditions []string) (map[string]string, string, []interface{}, bool) {
	parts := make([]string, 0, len(conditions))
	for _, condition := range conditions {
		parts = append(parts, splitTopLevelConjunctions(condition)...)
	}
	if len(parts) < 2 {
		return nil, "", nil, false
	}
	equalities := make(map[string]string)
	inColumn, inValues := "", []interface{}(nil)
	for _, part := range parts {
		condition := strings.TrimSpace(part)
		lowerCondition := strings.ToLower(condition)
		inAt := strings.Index(lowerCondition, " in ")
		if inAt > 0 {
			if inColumn != "" || strings.Contains(lowerCondition[inAt+len(" in "):], "select") {
				return nil, "", nil, false
			}
			column := strings.Trim(strings.TrimSpace(condition[:inAt]), " `\t\r\n")
			list := strings.TrimSpace(condition[inAt+len(" in "):])
			if column == "" || len(list) < 2 || list[0] != '(' || list[len(list)-1] != ')' {
				return nil, "", nil, false
			}
			for _, item := range splitTopLevelComma(strings.TrimSpace(list[1 : len(list)-1])) {
				value := strings.Trim(strings.TrimSpace(item), "'\"")
				if value == "" || strings.ContainsAny(value, "()") {
					return nil, "", nil, false
				}
				inValues = append(inValues, value)
			}
			inColumn = column
			continue
		}
		column, value, ok := parseSecondaryIndexEquality(condition)
		if !ok || inColumn != "" && strings.EqualFold(column, inColumn) {
			return nil, "", nil, false
		}
		equalities[strings.ToLower(column)] = value
	}
	if len(equalities) == 0 || inColumn == "" || len(inValues) == 0 {
		return nil, "", nil, false
	}
	return equalities, inColumn, inValues, true
}

func secondaryIndexPrefixNullPredicates(conditions []string) (map[string]string, string, bool) {
	parts := make([]string, 0, len(conditions))
	for _, condition := range conditions {
		parts = append(parts, splitSecondaryIndexConjunctions(condition)...)
	}
	if len(parts) < 2 {
		return nil, "", false
	}
	equalities := make(map[string]string)
	nullColumn := ""
	for _, part := range parts {
		condition := strings.TrimSpace(part)
		lowerCondition := strings.ToLower(condition)
		if strings.HasSuffix(lowerCondition, " is null") {
			if nullColumn != "" {
				return nil, "", false
			}
			nullColumn = strings.Trim(strings.TrimSpace(condition[:len(condition)-len(" is null")]), " `\t\r\n")
			if nullColumn == "" {
				return nil, "", false
			}
			continue
		}
		column, value, ok := parseSecondaryIndexEquality(condition)
		if !ok || nullColumn != "" && strings.EqualFold(column, nullColumn) {
			return nil, "", false
		}
		equalities[strings.ToLower(column)] = value
	}
	if len(equalities) == 0 || nullColumn == "" {
		return nil, "", false
	}
	return equalities, nullColumn, true
}

// secondaryIndexPrefixRangePredicates extracts a contiguous equality prefix
// followed by one range predicate. The range value is intentionally left to
// residual filtering because secondary-index keys encode values in a durable
// compatibility format rather than a type-aware sortable representation.
func secondaryIndexPrefixRangePredicates(conditions []string) (map[string]string, string, string, string, bool) {
	parts := make([]string, 0, len(conditions))
	for _, condition := range conditions {
		parts = append(parts, splitSecondaryIndexConjunctions(condition)...)
	}
	if len(parts) < 2 {
		return nil, "", "", "", false
	}
	equalities := make(map[string]string, len(parts)-1)
	rangeColumn, rangeOperator, rangeValue := "", "", ""
	for _, part := range parts {
		column, operator, value, ok := parseSecondaryIndexComparison(part)
		if !ok {
			column, operator, value, ok = parseSecondaryIndexBetween(part)
		}
		if !ok {
			column, operator, value, ok = parseSecondaryIndexNullComparison(part)
		}
		if !ok {
			column, operator, value, ok = parseSecondaryIndexLikeComparison(part)
		}
		if !ok {
			column, operator, value, ok = parseSecondaryIndexNotInComparison(part)
		}
		if !ok {
			return nil, "", "", "", false
		}
		if operator == "=" {
			if _, exists := equalities[strings.ToLower(column)]; exists {
				return nil, "", "", "", false
			}
			equalities[strings.ToLower(column)] = value
			continue
		}
		if rangeColumn != "" || operator != "between" && operator != "not_between" && operator != "is_not_null" && operator != "like" && operator != "not_like" && operator != "not_in" && !isSecondaryIndexRangeOperator(operator) {
			return nil, "", "", "", false
		}
		rangeColumn, rangeOperator, rangeValue = column, operator, value
	}
	if len(equalities) == 0 || rangeColumn == "" {
		return nil, "", "", "", false
	}
	return equalities, rangeColumn, rangeOperator, rangeValue, true
}

func parseSecondaryIndexNullComparison(part string) (string, string, string, bool) {
	condition := strings.TrimSpace(part)
	lowerCondition := strings.ToLower(condition)
	if strings.HasSuffix(lowerCondition, " is not null") {
		column := strings.Trim(strings.TrimSpace(condition[:len(condition)-len(" is not null")]), " `\t\r\n")
		return column, "is_not_null", "", column != ""
	}
	if strings.HasSuffix(lowerCondition, " is null") {
		column := strings.Trim(strings.TrimSpace(condition[:len(condition)-len(" is null")]), " `\t\r\n")
		return column, "is_null", "", column != ""
	}
	return "", "", "", false
}

func parseSecondaryIndexLikeComparison(part string) (string, string, string, bool) {
	condition := strings.TrimSpace(part)
	lowerCondition := strings.ToLower(condition)
	operatorText, operator := " like ", "like"
	if notLikeAt := strings.Index(lowerCondition, " not like "); notLikeAt > 0 {
		operatorText, operator = " not like ", "not_like"
	}
	operatorAt := strings.Index(lowerCondition, operatorText)
	if operatorAt <= 0 {
		return "", "", "", false
	}
	column := strings.Trim(strings.TrimSpace(condition[:operatorAt]), " `\t\r\n")
	pattern := strings.Trim(strings.TrimSpace(condition[operatorAt+len(operatorText):]), "'\"")
	if column == "" || pattern == "" || !strings.HasSuffix(pattern, "%") {
		return "", "", "", false
	}
	prefix := strings.TrimSuffix(pattern, "%")
	if strings.ContainsAny(prefix, "%_") {
		return "", "", "", false
	}
	return column, operator, pattern, true
}

func parseSecondaryIndexNullSafeComparison(part string) (string, string, string, bool) {
	condition := strings.TrimSpace(part)
	operatorAt := strings.Index(condition, "<=>")
	if operatorAt <= 0 {
		return "", "", "", false
	}
	column := strings.Trim(strings.TrimSpace(condition[:operatorAt]), " `\t\r\n")
	value := strings.Trim(strings.TrimSpace(condition[operatorAt+len("<=>"):]), "'\"")
	if column == "" || value == "" {
		return "", "", "", false
	}
	return column, "<=>", value, true
}

func parseSecondaryIndexNotInComparison(part string) (string, string, string, bool) {
	condition := strings.TrimSpace(part)
	lowerCondition := strings.ToLower(condition)
	notInAt := strings.Index(lowerCondition, " not in ")
	if notInAt <= 0 {
		return "", "", "", false
	}
	column := strings.Trim(strings.TrimSpace(condition[:notInAt]), " `\t\r\n")
	list := strings.TrimSpace(condition[notInAt+len(" not in "):])
	if column == "" || len(list) < 2 || list[0] != '(' || list[len(list)-1] != ')' || strings.Contains(strings.ToLower(list), "select") {
		return "", "", "", false
	}
	items := splitTopLevelComma(strings.TrimSpace(list[1 : len(list)-1]))
	if len(items) == 0 {
		return "", "", "", false
	}
	for _, item := range items {
		if strings.Trim(strings.TrimSpace(item), "'\"") == "" || strings.ContainsAny(item, "()") {
			return "", "", "", false
		}
	}
	return column, "not_in", list, true
}

func parseSecondaryIndexInComparison(part string) (string, string, string, bool) {
	condition := strings.TrimSpace(part)
	lowerCondition := strings.ToLower(condition)
	inAt := strings.Index(lowerCondition, " in ")
	if inAt <= 0 {
		return "", "", "", false
	}
	column := strings.Trim(strings.TrimSpace(condition[:inAt]), " `\t\r\n")
	list := strings.TrimSpace(condition[inAt+len(" in "):])
	if column == "" || len(list) < 2 || list[0] != '(' || list[len(list)-1] != ')' || strings.Contains(strings.ToLower(list), "select") {
		return "", "", "", false
	}
	items := splitTopLevelComma(strings.TrimSpace(list[1 : len(list)-1]))
	if len(items) == 0 {
		return "", "", "", false
	}
	for _, item := range items {
		if strings.Trim(strings.TrimSpace(item), "'\"") == "" || strings.ContainsAny(item, "()") {
			return "", "", "", false
		}
	}
	return column, "in", list, true
}

func parseSecondaryIndexBetween(part string) (string, string, string, bool) {
	condition := strings.TrimSpace(part)
	lowerCondition := strings.ToLower(condition)
	if notBetweenAt := strings.Index(lowerCondition, " not between "); notBetweenAt > 0 {
		column := strings.Trim(strings.TrimSpace(condition[:notBetweenAt]), " `\t\r\n")
		rangeValues := strings.TrimSpace(condition[notBetweenAt+len(" not between "):])
		if column != "" && strings.Count(strings.ToLower(rangeValues), " and ") == 1 {
			return column, "not_between", rangeValues, true
		}
		return "", "", "", false
	}
	betweenAt := strings.Index(lowerCondition, " between ")
	if betweenAt <= 0 {
		return "", "", "", false
	}
	column := strings.Trim(strings.TrimSpace(condition[:betweenAt]), " `\t\r\n")
	rangeValues := strings.TrimSpace(condition[betweenAt+len(" between "):])
	if column == "" || strings.Count(strings.ToLower(rangeValues), " and ") != 1 {
		return "", "", "", false
	}
	return column, "between", rangeValues, true
}

// splitSecondaryIndexConjunctions is the index-parser variant of the generic
// boolean splitter. It keeps the AND inside BETWEEN ... AND ... as part of
// the range predicate, while still splitting surrounding predicates.
func splitSecondaryIndexConjunctions(condition string) []string {
	parts := splitTopLevelConjunctions(condition)
	merged := make([]string, 0, len(parts))
	for index := 0; index < len(parts); index++ {
		part := parts[index]
		lowerPart := strings.ToLower(part)
		if (strings.Contains(lowerPart, " between ") || strings.Contains(lowerPart, " not between ")) && index+1 < len(parts) {
			part += " and " + parts[index+1]
			index++
		}
		merged = append(merged, part)
	}
	return merged
}

func parseSecondaryIndexComparison(part string) (string, string, string, bool) {
	condition := strings.TrimSpace(part)
	for _, operator := range []string{"<=", ">=", "<>", "!=", "<", ">", "="} {
		pieces := strings.SplitN(condition, operator, 2)
		if len(pieces) != 2 {
			continue
		}
		column := strings.TrimSpace(pieces[0])
		if strings.ContainsAny(column, "()") {
			return "", "", "", false
		}
		if dot := strings.LastIndex(column, "."); dot >= 0 {
			column = column[dot+1:]
		}
		column = strings.Trim(column, " `\t\r\n")
		value := strings.Trim(strings.TrimSpace(pieces[1]), "'\"")
		if column == "" || value == "" {
			return "", "", "", false
		}
		return column, operator, value, true
	}
	return "", "", "", false
}

func isSecondaryIndexRangeOperator(operator string) bool {
	switch operator {
	case "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
}

func secondaryIndexOrPredicates(conditions []string) ([]secondaryIndexPredicateBranch, bool) {
	parts := make([]string, 0, len(conditions))
	for _, condition := range conditions {
		parts = append(parts, splitTopLevelDisjunctions(condition)...)
	}
	if len(parts) < 2 {
		return nil, false
	}
	predicates := make([]secondaryIndexPredicateBranch, 0, len(parts))
	for _, part := range parts {
		column, operator, value, ok := parseSecondaryIndexComparison(part)
		if !ok {
			return nil, false
		}
		if operator != "=" && !isSecondaryIndexRangeOperator(operator) {
			return nil, false
		}
		predicates = append(predicates, secondaryIndexPredicateBranch{column: column, operator: operator, value: value})
	}
	return predicates, true
}

// secondaryIndexInPredicates expands a constant IN list into equality
// branches. Each branch can use the same single-column secondary index; the
// existing OR executor deduplicates primary keys and reapplies the original
// IN predicate after clustered lookup. Subqueries and NOT IN are deliberately
// excluded because they need different NULL and materialization semantics.
func secondaryIndexInPredicates(conditions []string) ([]secondaryIndexPredicateBranch, bool) {
	if len(conditions) != 1 {
		return nil, false
	}
	condition := strings.TrimSpace(conditions[0])
	lowerCondition := strings.ToLower(condition)
	inAt := strings.Index(lowerCondition, " in ")
	if inAt <= 0 || strings.Contains(lowerCondition[inAt+len(" in "):], "select") {
		return nil, false
	}
	column := strings.Trim(strings.TrimSpace(condition[:inAt]), " `\t\r\n")
	list := strings.TrimSpace(condition[inAt+len(" in "):])
	if column == "" || len(list) < 2 || list[0] != '(' || list[len(list)-1] != ')' {
		return nil, false
	}
	items := splitTopLevelComma(strings.TrimSpace(list[1 : len(list)-1]))
	if len(items) < 2 {
		return nil, false
	}
	predicates := make([]secondaryIndexPredicateBranch, 0, len(items))
	for _, item := range items {
		value := strings.TrimSpace(item)
		if value == "" || strings.ContainsAny(value, "()") {
			return nil, false
		}
		value = strings.Trim(value, "'\"")
		if value == "" {
			return nil, false
		}
		predicates = append(predicates, secondaryIndexPredicateBranch{column: column, operator: "=", value: value})
	}
	return predicates, true
}

func parseSecondaryIndexEquality(part string) (string, string, bool) {
	pieces := strings.SplitN(part, "=", 2)
	if len(pieces) != 2 || strings.ContainsAny(pieces[0], "<>!") {
		return "", "", false
	}
	column := strings.TrimSpace(pieces[0])
	if dot := strings.LastIndex(column, "."); dot >= 0 {
		column = column[dot+1:]
	}
	column = strings.Trim(column, " `\t\r\n")
	value := strings.Trim(strings.TrimSpace(pieces[1]), "'\"")
	return column, value, column != "" && value != ""
}

func splitTopLevelConjunctions(condition string) []string {
	return splitTopLevelBoolean(condition, " and ")
}

func splitTopLevelDisjunctions(condition string) []string {
	return splitTopLevelBoolean(condition, " or ")
}

func splitTopLevelBoolean(condition, keyword string) []string {
	parts := make([]string, 0, 1)
	start, depth := 0, 0
	var quote byte
	for index := 0; index < len(condition); index++ {
		ch := condition[index]
		if quote != 0 {
			if ch == quote && (index == 0 || condition[index-1] != '\\') {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && index+len(keyword) <= len(condition) && strings.EqualFold(condition[index:index+len(keyword)], keyword) {
			parts = append(parts, strings.TrimSpace(condition[start:index]))
			start = index + len(keyword)
			index += len(keyword) - 1
		}
	}
	parts = append(parts, strings.TrimSpace(condition[start:]))
	return parts
}

func indexColumnNames(index *manager.Index) []string {
	columns := make([]string, len(index.Columns))
	for i, column := range index.Columns {
		columns[i] = column.Name
	}
	return columns
}

func (se *SelectExecutor) ensureTableStorageMapping(ctx context.Context) error {
	tableStorageManager := se.storageManager.GetTableStorageManager()
	rootPageNo, err := se.loadPersistedTableRootPage()
	if err != nil {
		return err
	}
	partitioning, partitionErr := se.loadPersistedPartitioning()
	if partitionErr != nil {
		return partitionErr
	}
	if existing, err := tableStorageManager.GetTableStorageInfo(se.schemaName, se.tableName); err == nil && existing.RootPageNo == rootPageNo {
		if partitioning != nil {
			return tableStorageManager.SetPartitioning(se.schemaName, se.tableName, partitioning)
		}
		return nil
	}
	spaceName := fmt.Sprintf("%s/%s", se.schemaName, se.tableName)
	ownsTablespace := true
	if raw, readErr := os.ReadFile(filepath.Join(se.dataDir, se.schemaName, se.tableName+".frm")); readErr == nil {
		var attachment struct {
			TablespaceName string `json:"tablespace_name"`
			OwnsTablespace *bool  `json:"owns_tablespace"`
			Discarded      bool   `json:"tablespace_discarded"`
		}
		if json.Unmarshal(raw, &attachment) == nil {
			if attachment.Discarded {
				return fmt.Errorf("tablespace for %s.%s was discarded; IMPORT TABLESPACE is required", se.schemaName, se.tableName)
			}
			if strings.TrimSpace(attachment.TablespaceName) != "" {
				spaceName = attachment.TablespaceName
			}
			if attachment.OwnsTablespace != nil {
				ownsTablespace = *attachment.OwnsTablespace
			}
		}
	}
	handle, err := se.storageManager.GetTablespace(spaceName)
	if err != nil && ownsTablespace {
		handle, err = se.storageManager.CreateTablespace(spaceName)
	}
	if err != nil {
		return fmt.Errorf("recover table storage mapping for %s: %w", spaceName, err)
	}
	info := &manager.TableStorageInfo{
		SchemaName:     se.schemaName,
		TableName:      se.tableName,
		SpaceID:        handle.SpaceID,
		RootPageNo:     rootPageNo,
		IndexPageNo:    rootPageNo,
		DataSegmentID:  handle.DataSegmentID,
		Type:           manager.TableTypeUser,
		Partitioning:   partitioning,
		TablespaceName: spaceName,
		OwnsTablespace: ownsTablespace,
	}
	if _, err := tableStorageManager.GetTableStorageInfo(se.schemaName, se.tableName); err == nil {
		if err := tableStorageManager.ReplaceTableStorage(ctx, info); err != nil {
			return err
		}
		if partitioning != nil {
			return tableStorageManager.SetPartitioning(se.schemaName, se.tableName, partitioning)
		}
		return nil
	}
	if err := tableStorageManager.RegisterTable(ctx, info); err != nil {
		return err
	}
	if partitioning != nil {
		return tableStorageManager.SetPartitioning(se.schemaName, se.tableName, partitioning)
	}
	return nil
}

func (se *SelectExecutor) loadPersistedTableRootPage() (uint32, error) {
	path := filepath.Join(se.dataDir, se.schemaName, se.tableName+".frm")
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read table metadata for storage recovery: %w", err)
	}
	var definition struct {
		StorageRootPage uint32 `json:"storage_root_page"`
	}
	if err := json.Unmarshal(raw, &definition); err != nil {
		return 0, fmt.Errorf("decode table metadata for storage recovery: %w", err)
	}
	if definition.StorageRootPage == 0 {
		var attachment struct {
			TablespaceName string `json:"tablespace_name"`
		}
		if json.Unmarshal(raw, &attachment) == nil && strings.TrimSpace(attachment.TablespaceName) != "" {
			return 0, nil
		}
		return 0, fmt.Errorf("table storage root page is missing for %s.%s", se.schemaName, se.tableName)
	}
	return definition.StorageRootPage, nil
}

func (se *SelectExecutor) loadPersistedPartitioning() (map[string]interface{}, error) {
	path := filepath.Join(se.dataDir, se.schemaName, se.tableName+".frm")
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read table metadata for partition recovery: %w", err)
	}
	var definition struct {
		Partitioning map[string]interface{} `json:"partitioning"`
	}
	if err := json.Unmarshal(raw, &definition); err != nil {
		return nil, fmt.Errorf("decode table partition metadata: %w", err)
	}
	return definition.Partitioning, nil
}

func recordFromInsertRowData(row *InsertRowData, tableMeta *metadata.TableMeta) Record {
	return recordFromRowMap(row.ColumnValues, tableMeta)
}

func recordFromRowMap(row map[string]interface{}, tableMeta *metadata.TableMeta) Record {
	values := make([]interface{}, 0, len(tableMeta.Columns))
	for _, col := range tableMeta.Columns {
		if col == nil {
			continue
		}
		value, exists := row[col.Name]
		if !exists || value == nil {
			value = defaultValueForColumn(col)
		}
		values = append(values, coerceValueForColumn(value, col.Type))
	}
	return NewExecutorRecordFromInterface(values, tableMeta)
}

func defaultValueForColumn(column *metadata.ColumnMeta) interface{} {
	if column == nil || column.DefaultValue == nil {
		return nil
	}
	if text, ok := column.DefaultValue.(string); ok {
		trimmed := strings.TrimSpace(text)
		if strings.EqualFold(trimmed, "null") {
			return nil
		}
		trimmed = strings.Trim(trimmed, "'")
		if number, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return number
		}
		return trimmed
	}
	return column.DefaultValue
}

func coerceValueForColumn(value interface{}, dataType metadata.DataType) interface{} {
	if value == nil {
		return nil
	}
	text := strings.TrimSpace(fmt.Sprint(value))
	switch dataType {
	case metadata.TypeTinyInt, metadata.TypeSmallInt, metadata.TypeMediumInt, metadata.TypeInt, metadata.TypeBigInt, metadata.TypeYear:
		if number, err := strconv.ParseInt(text, 10, 64); err == nil {
			return number
		}
	case metadata.TypeFloat, metadata.TypeDouble, metadata.TypeDecimal:
		if number, err := strconv.ParseFloat(text, 64); err == nil {
			return number
		}
	case metadata.TypeVarchar, metadata.TypeChar, metadata.TypeText, metadata.TypeTinyText, metadata.TypeMediumText, metadata.TypeLongText:
		return text
	}
	return value
}

// executeUserTableQuery 执行 mysql.user 表的特殊查询逻辑
func (se *SelectExecutor) executeUserTableQuery(ctx context.Context) error {
	logger.Debugf(" [SelectExecutor] 执行 mysql.user 表查询")

	// 优先使用真实存储管理器获取 mysql.user 信息
	storageManager := se.storageManager
	if storageManager == nil {
		return fmt.Errorf("storage manager is required for mysql.user query")
	}

	if se.storageManager != nil {
		logger.Debugf(" [SelectExecutor] 使用存储管理器查询用户数据")

		// 解析WHERE条件以获取用户名和主机
		username, host := se.parseUserQueryConditions()

		hostCandidates := []string{host}
		if host == "127.0.0.1" || host == "::1" {
			hostCandidates = append(hostCandidates, "localhost", "%")
		}
		if host != "localhost" {
			hostCandidates = append(hostCandidates, "localhost")
		}
		if host != "%" {
			hostCandidates = append(hostCandidates, "%")
		}

		seen := make(map[string]struct{})
		for _, candidateHost := range hostCandidates {
			if candidateHost == "" {
				continue
			}
			if _, exists := seen[candidateHost]; exists {
				continue
			}
			seen[candidateHost] = struct{}{}

			user, err := storageManager.QueryMySQLUser(username, candidateHost)
			if err != nil {
				logger.Warnf("  [SelectExecutor] 查询用户数据失败: user=%s host=%s -> %v", username, candidateHost, err)
				continue
			}

			// 将用户数据转换为记录
			if err := se.convertMySQLUserToRecord(user); err != nil {
				logger.Warnf("  [SelectExecutor] 转换用户数据失败: %v", err)
				continue
			}

			logger.Debugf(" [SelectExecutor] 成功从存储管理器获取用户数据: user=%s host=%s", user.User, user.Host)
			return nil
		}

		return fmt.Errorf("mysql.user record not found for %s@%s", username, host)
	}

	return fmt.Errorf("mysql.user query failed")
}

// parseUserQueryConditions 解析WHERE条件中的用户名和主机
func (se *SelectExecutor) parseUserQueryConditions() (username, host string) {
	logger.Debugf(" [SelectExecutor] 解析WHERE条件: %v", se.whereConditions)

	// 默认值
	username = "root"
	host = "localhost"

	if len(se.whereConditions) == 0 {
		logger.Debugf(" [SelectExecutor] WHERE条件为空，使用默认值 user=%s, host=%s", username, host)
		logger.Debugf(" [SelectExecutor] 最终解析结果: user=%s, host=%s", username, host)
		return username, host
	}

	whereClause := strings.Join(se.whereConditions, " AND ")
	userValue := se.extractWhereValue(userWhereConditionRe, whereClause)
	hostValue := se.extractWhereValue(hostWhereConditionRe, whereClause)

	if userValue != "" {
		username = userValue
		logger.Debugf(" [SelectExecutor] 解析到用户名: %s", username)
	} else {
		logger.Warnf(" [SelectExecutor] 未解析到User条件，使用默认值: %s", username)
	}
	if hostValue != "" {
		host = hostValue
		logger.Debugf(" [SelectExecutor] 解析到主机: %s", host)
	} else {
		logger.Warnf(" [SelectExecutor] 未解析到Host条件，使用默认值: %s", host)
	}

	logger.Debugf(" [SelectExecutor] 最终解析结果: user=%s, host=%s", username, host)
	return username, host
}

func (se *SelectExecutor) extractWhereValue(re *regexp.Regexp, whereClause string) string {
	matches := re.FindStringSubmatch(whereClause)
	if len(matches) < 2 {
		return ""
	}

	value := ""
	for _, candidate := range matches[1:] {
		if candidate != "" {
			value = candidate
			break
		}
	}
	value = strings.TrimSpace(strings.Trim(value, `"'`))
	return value
}

// convertUserToRecord 将用户数据转换为记录
func (se *SelectExecutor) convertMySQLUserToRecord(user *manager.MySQLUser) error {
	if user == nil {
		return fmt.Errorf("mysql user is nil")
	}

	logger.Debugf(" [SelectExecutor] 转换用户数据为记录: user=%s host=%s", user.User, user.Host)

	var passwordLifetime interface{}
	if user.PasswordLifetime != nil {
		passwordLifetime = int(*user.PasswordLifetime)
	}

	var passwordReuseCount interface{}
	if user.PasswordReuseCcount != nil {
		passwordReuseCount = int(*user.PasswordReuseCcount)
	}

	var passwordReuseTime interface{}
	if user.PasswordReuseTime != nil {
		passwordReuseTime = int(*user.PasswordReuseTime)
	}

	tableMeta := se.getMySQLUserTableMeta()
	rawValues := []interface{}{
		user.Host,
		user.User,
		user.SelectPriv,
		user.InsertPriv,
		user.UpdatePriv,
		user.DeletePriv,
		user.CreatePriv,
		user.DropPriv,
		user.ReloadPriv,
		user.ShutdownPriv,
		user.ProcessPriv,
		user.FilePriv,
		user.GrantPriv,
		user.ReferencesPriv,
		user.IndexPriv,
		user.AlterPriv,
		user.ShowDbPriv,
		user.SuperPriv,
		user.CreateTmpTablePriv,
		user.LockTablesPriv,
		user.ExecutePriv,
		user.ReplSlavePriv,
		user.ReplClientPriv,
		user.CreateViewPriv,
		user.ShowViewPriv,
		user.CreateRoutinePriv,
		user.AlterRoutinePriv,
		user.CreateUserPriv,
		user.EventPriv,
		user.TriggerPriv,
		user.CreateTablespacePriv,
		user.AuthenticationString,
		user.PasswordExpired,
		passwordLifetime,
		user.AccountLocked,
		func() interface{} {
			if user.PasswordLastChanged.IsZero() {
				return nil
			}
			return user.PasswordLastChanged.Format("2006-01-02 15:04:05")
		}(),
		passwordReuseCount,
		passwordReuseTime,
		user.PasswordRequireCurrent,
		user.UserAttributes,
	}
	record := NewExecutorRecordFromInterface(rawValues, tableMeta)
	se.resultSet = []Record{record}

	logger.Debugf(" [SelectExecutor] 用户数据转换完成，生成 1 条记录")
	return nil
}

// createDefaultUserData 创建默认的 mysql.user 表数据
func (se *SelectExecutor) createDefaultUserData() error {
	logger.Debugf("  [SelectExecutor] 创建默认 mysql.user 表数据")

	tableMeta := se.getMySQLUserTableMeta()
	defaultPasswordHash := se.getDefaultRootUserPasswordHash()

	// 创建默认的root用户记录
	// 字段顺序：Host, User, 29个权限字段, authentication_string, password_expired,
	//          max_questions, max_updates, max_connections, max_user_connections,
	//          account_locked, password_last_changed, password_require_current, user_attributes
	rootUsers := [][]interface{}{
		{
			"localhost", "root", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",
			"Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",
			"Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",
			"Y", defaultPasswordHash, "N",
			"0", "0", "0", "0", "N", "2024-01-01 00:00:00", "Y", "{}",
		},
		{
			"%", "root", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",
			"Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",
			"Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",
			"Y", defaultPasswordHash, "N",
			"0", "0", "0", "0", "N", "2024-01-01 00:00:00", "Y", "{}",
		},
	}

	se.resultSet = make([]Record, len(rootUsers))
	for i, userData := range rootUsers {
		se.resultSet[i] = NewExecutorRecordFromInterface(userData, tableMeta)
	}

	logger.Debugf(" [SelectExecutor] 创建了 %d 条默认用户记录", len(se.resultSet))
	return nil
}

func (se *SelectExecutor) getDefaultRootUserPasswordHash() string {
	defaultPassword := "root@1234"
	stage1 := sha1.Sum([]byte(defaultPassword))
	stage2 := sha1.Sum(stage1[:])
	return fmt.Sprintf("*%X", stage2)
}

// getMySQLUserTableMeta 获取 mysql.user 表的元数据
func (se *SelectExecutor) getMySQLUserTableMeta() *metadata.TableMeta {
	// 创建列元数据指针数组
	columns := make([]*metadata.ColumnMeta, 0, 40)

	// 添加所有列
	columnDefs := []metadata.ColumnMeta{
		{Name: "Host", Type: metadata.TypeChar, Length: 60},
		{Name: "User", Type: metadata.TypeChar, Length: 32},
		{Name: "Select_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Insert_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Update_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Delete_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Create_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Drop_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Reload_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Shutdown_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Process_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "File_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Grant_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "References_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Index_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Alter_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Show_db_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Super_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Create_tmp_table_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Lock_tables_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Execute_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Repl_slave_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Repl_client_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Create_view_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Show_view_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Create_routine_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Alter_routine_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Create_user_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Event_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Trigger_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Create_tablespace_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "authentication_string", Type: metadata.TypeText, Length: 65535},
		{Name: "password_expired", Type: metadata.TypeEnum, Length: 1},
		{Name: "max_questions", Type: metadata.TypeInt, Length: 11},
		{Name: "max_updates", Type: metadata.TypeInt, Length: 11},
		{Name: "max_connections", Type: metadata.TypeInt, Length: 11},
		{Name: "max_user_connections", Type: metadata.TypeInt, Length: 11},
		{Name: "account_locked", Type: metadata.TypeEnum, Length: 1},
		{Name: "password_last_changed", Type: metadata.TypeTimestamp, Length: 19},
		{Name: "password_require_current", Type: metadata.TypeEnum, Length: 1},
		{Name: "user_attributes", Type: metadata.TypeJSON, Length: 65535},
	}

	// 转换为指针数组
	for i := range columnDefs {
		columns = append(columns, &columnDefs[i])
	}

	tableMeta := &metadata.TableMeta{
		Name:    "user",
		Columns: columns,
	}

	return tableMeta
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
	logger.Debugf("扫描页面: SpaceID=%d, PageNum=%d\n", spaceID, pageNum)

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

	logger.Debugf("从页面 %d 解析出 %d 条记录\n", pageNum, len(records))

	// 应用WHERE条件过滤
	filteredRecords := se.applyWhereFilter(records)

	logger.Debugf("过滤后剩余 %d 条记录\n", len(filteredRecords))

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

// parsePageRecords 解析页面中的记录，结合元信息和投影需求
func (se *SelectExecutor) parsePageRecords(bufferPage interface{}) ([]Record, error) {
	// 获取表的元数据
	tableMeta, err := se.getTableMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %v", err)
	}

	// 确定需要解析的列（基于投影需求）
	requiredColumns, columnMapping, err := se.determineRequiredColumns(tableMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to determine required columns: %v", err)
	}

	// 解析页面数据，只提取需要的列
	records, err := se.parsePageWithProjection(bufferPage, tableMeta, requiredColumns, columnMapping)
	if err != nil {
		return nil, fmt.Errorf("failed to parse page with projection: %v", err)
	}

	return records, nil
}

// getTableMetadata 获取表元数据。优先 tableManager；若无或失败则从 dataDir 下的 .frm 加载（与 CREATE TABLE 写入格式一致）；否则返回默认表结构。
func (se *SelectExecutor) getTableMetadata() (*metadata.TableMeta, error) {
	if se.tableManager != nil {
		tableMeta, err := se.tableManager.GetTableMetadata(context.Background(), se.schemaName, se.tableName)
		if err == nil && tableMeta != nil && len(tableMeta.Columns) > 0 {
			return tableMeta, nil
		}
	}

	if se.dataDir != "" {
		if meta, err := se.loadTableMetaFromFrm(se.dataDir, se.schemaName, se.tableName); err == nil && meta != nil {
			return meta, nil
		}
	}

	return se.getDefaultTableMeta(), nil
}

// frmTableInfo 与 executor.createTableStructureFile 写入的 JSON 结构一致
type frmTableInfo struct {
	TableName    string                   `json:"table_name"`
	TableComment string                   `json:"table_comment"`
	Columns      []map[string]interface{} `json:"columns"`
	Indexes      []map[string]interface{} `json:"indexes"`
}

// loadTableMetaFromFrm 从 dataDir/schema/table.frm（JSON）加载表定义，与 executor 写入格式一致。
func (se *SelectExecutor) loadTableMetaFromFrm(dataDir, schemaName, tableName string) (*metadata.TableMeta, error) {
	if dataDir == "" || schemaName == "" || tableName == "" {
		return nil, fmt.Errorf("missing dataDir, schema or table name")
	}
	frmPath := filepath.Join(dataDir, schemaName, tableName+".frm")
	data, err := os.ReadFile(frmPath)
	if err != nil {
		return nil, err
	}
	var info frmTableInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	if len(info.Columns) == 0 {
		return nil, fmt.Errorf("no columns in .frm")
	}
	meta := &metadata.TableMeta{
		Name:    tableName,
		Comment: info.TableComment,
		Columns: make([]*metadata.ColumnMeta, 0, len(info.Columns)),
	}
	for _, col := range info.Columns {
		name, _ := col["name"].(string)
		if name == "" {
			continue
		}
		typeStr, _ := col["type"].(string)
		if typeStr == "" {
			typeStr = "VARCHAR"
		}
		if strings.EqualFold(strings.TrimSpace(typeStr), string(metadata.TypeGeometry)) {
			typeStr = string(metadata.TypeGeometry)
		}
		length := 0
		if n, ok := col["length"].(float64); ok {
			length = int(n)
		}
		scale := 0
		if n, ok := col["scale"].(float64); ok {
			scale = int(n)
		}
		isUnsigned, _ := col["unsigned"].(bool)
		nullable := true
		if b, ok := col["nullable"].(bool); ok {
			nullable = b
		}
		isPrimary, _ := col["primary"].(bool)
		isUnique, _ := col["unique"].(bool)
		isAutoIncrement, _ := col["auto_increment"].(bool)
		isGenerated, _ := col["generated"].(bool)
		generatedExpression, _ := col["generated_expression"].(string)
		columnMeta := &metadata.ColumnMeta{
			Name:                name,
			Type:                metadata.DataType(typeStr),
			Length:              length,
			Scale:               scale,
			IsNullable:          nullable,
			IsPrimary:           isPrimary,
			IsUnique:            isUnique,
			IsGenerated:         isGenerated,
			GeneratedExpression: generatedExpression,
		}
		columnMeta.IsUnsigned = isUnsigned
		if rawValues, ok := col["enum_values"].([]interface{}); ok {
			columnMeta.EnumValues = make([]string, 0, len(rawValues))
			for _, rawValue := range rawValues {
				columnMeta.EnumValues = append(columnMeta.EnumValues, strings.Trim(strings.TrimSpace(fmt.Sprint(rawValue)), "'\""))
			}
		}
		columnMeta.IsAutoIncrement = isAutoIncrement
		columnMeta.Comment = persistedString(col["comment"])
		if defaultValue, ok := col["default"]; ok {
			columnMeta.DefaultValue = defaultValue
		}
		meta.Columns = append(meta.Columns, columnMeta)
		if isPrimary {
			meta.PrimaryKey = append(meta.PrimaryKey, name)
		}
	}
	for _, idx := range info.Indexes {
		name, _ := idx["name"].(string)
		if name == "" {
			continue
		}
		rawColumns, _ := idx["columns"].([]interface{})
		columns := make([]string, 0, len(rawColumns))
		for _, rawColumn := range rawColumns {
			columnName, _ := rawColumn.(string)
			if columnName != "" {
				columns = append(columns, columnName)
			}
		}
		if len(columns) == 0 {
			continue
		}
		unique, _ := idx["unique"].(bool)
		visible := true
		visibilitySet := false
		if persistedVisible, ok := idx["visible"].(bool); ok {
			visible = persistedVisible
			visibilitySet = true
		}
		meta.Indices = append(meta.Indices, metadata.IndexMeta{
			Name:          name,
			Columns:       columns,
			Unique:        unique,
			IsVisible:     visible,
			VisibilitySet: visibilitySet,
		})
	}
	logger.Debugf(" [SelectExecutor] 从 .frm 加载表定义: %s.%s, 列数=%d", schemaName, tableName, len(meta.Columns))
	return meta, nil
}

// determineRequiredColumns 确定需要解析的列
func (se *SelectExecutor) determineRequiredColumns(tableMeta *metadata.TableMeta) ([]int, map[int]int, error) {
	// 如果是 SELECT *，需要所有列
	if len(se.selectExprs) == 0 || (len(se.selectExprs) == 1 && se.selectExprs[0] == "*") {
		requiredColumns := make([]int, len(tableMeta.Columns))
		columnMapping := make(map[int]int)
		for i := range tableMeta.Columns {
			requiredColumns[i] = i
			columnMapping[i] = i // 源索引 -> 目标索引
		}
		return requiredColumns, columnMapping, nil
	}

	// 创建列名到索引的映射（大小写不敏感）
	columnIndexMap := se.buildColumnIndexMap(tableMeta)

	var requiredColumns []int
	columnMapping := make(map[int]int) // 源索引 -> 目标索引

	for targetIndex, expr := range se.selectExprs {
		columnName := se.cleanColumnExpression(expr)
		sourceIndex, exists := se.findColumnIndex(columnName, columnIndexMap)

		if exists {
			// 检查是否已经在需要的列中
			found := false
			for _, reqCol := range requiredColumns {
				if reqCol == sourceIndex {
					found = true
					break
				}
			}

			if !found {
				requiredColumns = append(requiredColumns, sourceIndex)
			}
			columnMapping[sourceIndex] = targetIndex
		}
		// 如果列不存在，稍后在构造记录时处理为 NULL
	}

	// Predicates are evaluated after the projected page record is built, so
	// every column referenced only by WHERE must also be decoded from storage.
	// Without this dependency, SELECT id FROM t WHERE active = 1 can project
	// id correctly but lose active before filtering. Keep these columns out of
	// the result mapping while retaining them in the source value set.
	identifierPattern := regexp.MustCompile(`(?i)(?:[a-zA-Z0-9_$]+\s*\.\s*)?([a-zA-Z0-9_$]+)`)
	ignoredIdentifiers := map[string]struct{}{
		"and": {}, "or": {}, "not": {}, "is": {}, "null": {}, "true": {}, "false": {},
		"in": {}, "like": {}, "between": {}, "regexp": {}, "escape": {},
	}
	whereTargetIndex := len(se.selectExprs)
	for _, condition := range se.whereConditions {
		for _, match := range identifierPattern.FindAllStringSubmatch(condition, -1) {
			if len(match) != 2 {
				continue
			}
			columnName := strings.ToLower(strings.TrimSpace(match[1]))
			if _, ignored := ignoredIdentifiers[columnName]; ignored {
				continue
			}
			if sourceIndex, exists := columnIndexMap[columnName]; exists {
				found := false
				for _, required := range requiredColumns {
					if required == sourceIndex {
						found = true
						break
					}
				}
				if !found {
					requiredColumns = append(requiredColumns, sourceIndex)
					columnMapping[sourceIndex] = whereTargetIndex
					whereTargetIndex++
				}
			}
		}
	}

	return requiredColumns, columnMapping, nil
}

// parsePageWithProjection 解析页面数据并应用投影
func (se *SelectExecutor) parsePageWithProjection(bufferPage interface{}, tableMeta *metadata.TableMeta, requiredColumns []int, columnMapping map[int]int) ([]Record, error) {
	var records []Record

	// 从 bufferPage 获取页面内容
	pageContent, err := se.getPageContent(bufferPage)
	if err != nil {
		return nil, fmt.Errorf("failed to get page content: %v", err)
	}

	// 使用现有的页面解析器解析 InnoDB 页面格式
	indexPage, err := se.parseInnoDBPage(pageContent, tableMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to parse InnoDB page: %v", err)
	}

	// 获取页面中的实际记录
	rows := indexPage.GetRows()
	logger.Debugf("从页面解析出 %d 条实际记录\n", len(rows))

	// 将每条记录转换为投影后的 Record
	for recordIndex, row := range rows {
		if row == nil {
			continue
		}

		// 跳过 infimum 和 supremum 记录
		if row.IsInfimumRow() || row.IsSupremumRow() {
			continue
		}

		// 从实际记录中提取需要的列数据
		sourceValues, err := se.extractValuesFromRow(row, tableMeta, requiredColumns)
		if err != nil {
			logger.Debugf("Warning: Failed to extract values from record %d: %v\n", recordIndex, err)
			continue
		}

		// 构造投影后的记录
		projectedRecord, err := se.buildProjectedRecord(tableMeta, sourceValues, columnMapping)
		if err != nil {
			logger.Debugf("Warning: Failed to build projected record %d: %v\n", recordIndex, err)
			continue
		}

		records = append(records, projectedRecord)
	}

	return records, nil
}

// getPageContent 从 bufferPage 获取页面内容
func (se *SelectExecutor) getPageContent(bufferPage interface{}) ([]byte, error) {
	// 根据不同的缓冲页面类型获取内容
	switch page := bufferPage.(type) {
	case *buffer_pool.BufferPage:
		return page.GetContent(), nil
	case interface{ GetContent() []byte }:
		return page.GetContent(), nil
	default:
		return nil, fmt.Errorf("unsupported buffer page type: %T", bufferPage)
	}
}

// parseInnoDBPage 解析 InnoDB 页面格式
func (se *SelectExecutor) parseInnoDBPage(pageContent []byte, tableMeta *metadata.TableMeta) (basic.IIndexPage, error) {
	// 创建表行元组适配器（如果需要的话）
	tableRowTuple := se.createTableRowTuple(tableMeta)

	// 使用现有的页面解析器解析页面
	if tableRowTuple != nil {
		// 使用带元数据的解析器
		indexPage := page.NewPageIndexByLoadBytesWithTuple(pageContent, tableRowTuple)
		return indexPage, nil
	} else {
		// 使用基础解析器
		indexPage := page.NewPageIndexByLoadBytes(pageContent)
		return indexPage, nil
	}
}

// createTableRowTuple 创建表行元组（如果需要的话）
func (se *SelectExecutor) createTableRowTuple(tableMeta *metadata.TableMeta) metadata.TableRowTuple {
	// 尝试从 tableManager 获取（如果有此方法的话）
	// 但目前 TableManager 没有 GetTableRowTuple 方法，所以直接返回 nil
	// 让解析器使用默认逻辑

	// 如果获取失败，根据 tableMeta 创建一个基础的 TableRowTuple
	// 这里返回 nil，让解析器使用默认逻辑
	return nil
}

// extractValuesFromRow 从实际记录中提取需要的列数据
func (se *SelectExecutor) extractValuesFromRow(row basic.Row, tableMeta *metadata.TableMeta, requiredColumns []int) (map[int]basic.Value, error) {
	sourceValues := make(map[int]basic.Value)

	// 检查记录是否支持按索引读取值
	switch r := row.(type) {
	case interface{ ReadValueByIndex(int) basic.Value }:
		// 使用专门的方法读取值
		for _, columnIndex := range requiredColumns {
			if columnIndex < len(tableMeta.Columns) {
				value := r.ReadValueByIndex(columnIndex)
				if value != nil {
					sourceValues[columnIndex] = value
				} else {
					sourceValues[columnIndex] = basic.NewNull()
				}
			}
		}
	case interface{ GetPrimaryKey() basic.Value }:
		// 如果只能获取主键，则尝试其他方法
		primaryKey := r.GetPrimaryKey()
		if primaryKey != nil && len(requiredColumns) > 0 {
			// 假设第一个需要的列是主键
			sourceValues[requiredColumns[0]] = primaryKey
		}

		// 对于其他列，生成合理的默认值
		for i := 1; i < len(requiredColumns); i++ {
			columnIndex := requiredColumns[i]
			if columnIndex < len(tableMeta.Columns) {
				col := tableMeta.Columns[columnIndex]
				value := se.generateDefaultValueForColumn(col)
				sourceValues[columnIndex] = value
			}
		}
	default:
		// 如果记录类型不支持，生成默认值
		for _, columnIndex := range requiredColumns {
			if columnIndex < len(tableMeta.Columns) {
				col := tableMeta.Columns[columnIndex]
				value := se.generateDefaultValueForColumn(col)
				sourceValues[columnIndex] = value
			}
		}
	}

	return sourceValues, nil
}

// generateDefaultValueForColumn 为列生成默认值
func (se *SelectExecutor) generateDefaultValueForColumn(col *metadata.ColumnMeta) basic.Value {
	switch col.Type {
	case "INT", "BIGINT":
		return basic.NewInt64Value(0)
	case "VARCHAR", "CHAR", "TEXT":
		return basic.NewStringValue(fmt.Sprintf("default_%s", col.Name))
	case "TIMESTAMP", "DATETIME":
		return basic.NewStringValue("1970-01-01 00:00:00")
	case "ENUM":
		return basic.NewStringValue("N")
	case "JSON":
		return basic.NewStringValue("{}")
	default:
		return basic.NewStringValue("null")
	}
}

// buildProjectedRecord 构造投影后的记录
func (se *SelectExecutor) buildProjectedRecord(tableMeta *metadata.TableMeta, sourceValues map[int]basic.Value, columnMapping map[int]int) (Record, error) {
	var projectedValues []basic.Value
	var projectedColumns []*metadata.ColumnMeta

	// 如果是 SELECT *，直接使用所有列
	if len(se.selectExprs) == 0 || (len(se.selectExprs) == 1 && se.selectExprs[0] == "*") {
		// 为 SELECT * 创建合适大小的数组
		projectedValues = make([]basic.Value, len(tableMeta.Columns))
		projectedColumns = make([]*metadata.ColumnMeta, len(tableMeta.Columns))

		for i, col := range tableMeta.Columns {
			if value, exists := sourceValues[i]; exists {
				projectedValues[i] = value
			} else {
				projectedValues[i] = basic.NewNull()
			}
			projectedColumns[i] = col
		}
	} else {
		// 根据 selectExprs 构造投影
		projectedWidth := len(se.selectExprs)
		for _, targetIndex := range columnMapping {
			if targetIndex+1 > projectedWidth {
				projectedWidth = targetIndex + 1
			}
		}
		projectedValues = make([]basic.Value, projectedWidth)
		projectedColumns = make([]*metadata.ColumnMeta, projectedWidth)

		columnIndexMap := se.buildColumnIndexMap(tableMeta)

		for targetIndex, expr := range se.selectExprs {
			columnName := se.cleanColumnExpression(expr)
			sourceIndex, exists := se.findColumnIndex(columnName, columnIndexMap)

			if exists {
				if value, hasValue := sourceValues[sourceIndex]; hasValue {
					projectedValues[targetIndex] = value
				} else {
					projectedValues[targetIndex] = basic.NewNull()
				}

				if sourceIndex < len(tableMeta.Columns) {
					projectedColumns[targetIndex] = tableMeta.Columns[sourceIndex]
				} else {
					projectedColumns[targetIndex] = &metadata.ColumnMeta{
						Name: columnName,
						Type: "UNKNOWN",
					}
				}
			} else {
				// 列不存在，设置为 NULL
				projectedValues[targetIndex] = basic.NewNull()
				projectedColumns[targetIndex] = &metadata.ColumnMeta{
					Name: columnName,
					Type: "UNKNOWN",
				}
			}
		}
		for sourceIndex, targetIndex := range columnMapping {
			if targetIndex < len(se.selectExprs) || targetIndex >= projectedWidth || targetIndex >= len(projectedColumns) || sourceIndex >= len(tableMeta.Columns) {
				continue
			}
			if value, exists := sourceValues[sourceIndex]; exists {
				projectedValues[targetIndex] = value
			} else {
				projectedValues[targetIndex] = basic.NewNull()
			}
			projectedColumns[targetIndex] = tableMeta.Columns[sourceIndex]
		}
	}

	// 创建投影后的表元数据
	projectedTableMeta := &metadata.TableMeta{
		Name:    tableMeta.Name,
		Columns: projectedColumns,
	}

	// 创建记录
	return NewExecutorRecord(projectedValues, projectedTableMeta), nil
}

// parsePageRecordsFromSlot 从指定槽位解析记录
func (se *SelectExecutor) parsePageRecordsFromSlot(bufferPage interface{}, slot int) ([]Record, error) {
	// 先获取所有记录，然后从指定槽位开始
	allRecords, err := se.parsePageRecords(bufferPage)
	if err != nil {
		return nil, err
	}

	if slot >= len(allRecords) {
		return []Record{}, nil
	}

	return allRecords[slot:], nil
}

// applyWhereFilter 应用WHERE条件过滤
func (se *SelectExecutor) applyWhereFilter(records []Record) []Record {
	if len(se.whereConditions) == 0 {
		return records
	}

	filteredRecords := make([]Record, 0, len(records))
	for _, record := range records {
		values, err := se.recordValuesForWhere(record)
		if err != nil {
			logger.Warnf(" [applyWhereFilter] failed to read record values: %v", err)
			continue
		}
		matched := false
		if se.whereExpression != nil {
			matched, err = evalPredicate(se.whereExpression, values)
		} else {
			matched, err = rowMatchesWhereConditions(values, se.whereConditions)
		}
		if err != nil {
			if isStoredFunctionAuthorizationError(err) {
				se.expressionError = err
			}
			logger.Warnf(" [applyWhereFilter] failed to evaluate WHERE %v: %v", se.whereConditions, err)
			continue
		}
		if matched {
			filteredRecords = append(filteredRecords, record)
		}
	}

	return filteredRecords
}

func (se *SelectExecutor) recordValuesForWhere(record Record) (map[string]interface{}, error) {
	if record == nil {
		return nil, fmt.Errorf("record is nil")
	}
	tableMeta, err := se.getRecordTableMeta(record)
	if err != nil {
		return nil, err
	}
	values := make(map[string]interface{}, len(tableMeta.Columns))
	for idx, column := range tableMeta.Columns {
		if column == nil {
			continue
		}
		value := record.GetValueByIndex(idx)
		if value == nil {
			values[column.Name] = nil
			continue
		}
		values[column.Name] = recordValueForPredicate(value, column.Type)
	}
	if se.dataDir != "" {
		values[storedFunctionDataDirKey] = se.dataDir
		values[storedFunctionSchemaKey] = se.schemaName
		if se.storedFunctionAuthorizer != nil {
			values[storedFunctionAuthorizerKey] = se.storedFunctionAuthorizer
		}
	}
	if se.sessionValues != nil {
		values[sessionExpressionValuesKey] = se.sessionValues
	}
	return values, nil
}

func recordValueForPredicate(value basic.Value, dataType metadata.DataType) interface{} {
	if value == nil || value.IsNull() {
		return nil
	}
	switch metadata.DataType(strings.ToUpper(strings.TrimSpace(string(dataType)))) {
	case metadata.TypeTinyInt, metadata.TypeSmallInt, metadata.TypeMediumInt, metadata.TypeInt,
		metadata.TypeBigInt, metadata.TypeBit, metadata.TypeYear:
		return value.Int()
	case metadata.TypeFloat, metadata.TypeDouble, metadata.TypeDecimal:
		return value.Float64()
	case metadata.TypeBool, metadata.TypeBoolean:
		return value.Bool()
	default:
		return value.String()
	}
}

// buildSelectResult 构建SELECT结果
func (se *SelectExecutor) buildSelectResult() *SelectResult {
	if se.hasAggregateQuery() || len(se.groupByColumns) > 0 {
		return se.buildAggregateSelectResult()
	}

	// 应用投影
	projectedRecords := se.applyProjection(se.resultSet)

	if se.isCountStarQuery() {
		columns := se.getColumnNames()
		if len(columns) == 0 {
			columns = []string{"COUNT(*)"}
		}
		record := NewExecutorRecordFromInterface([]interface{}{int64(len(se.resultSet))}, &metadata.TableMeta{
			Name: se.tableName,
			Columns: []*metadata.ColumnMeta{
				{Name: columns[0], Type: metadata.TypeInt},
			},
		})
		return &SelectResult{
			Records:     []Record{record},
			RowCount:    1,
			Columns:     columns,
			ColumnTypes: []string{string(metadata.TypeBigInt)},
			ResultType:  common.RESULT_TYPE_QUERY,
			Message:     "Query OK, 1 row in set",
		}
	}

	// 应用排序
	sortedRecords := se.applyOrderBy(projectedRecords)

	// 应用DISTINCT
	distinctRecords := se.applyDistinct(sortedRecords)

	// 应用LIMIT和OFFSET
	limitedRecords := se.applyLimitOffset(distinctRecords)

	// 获取列信息
	columns := se.getColumnNames()

	//  特殊处理 mysql.user 表，确保列信息正确
	if se.schemaName == "mysql" && se.tableName == "user" {
		logger.Debugf(" [buildSelectResult] 为 mysql.user 表构建结果")

		if len(se.selectExprs) == 1 && se.selectExprs[0] == "*" {
			// SELECT * 查询，使用完整的user表列名
			tableMeta := se.getMySQLUserTableMeta()
			columns = make([]string, len(tableMeta.Columns))
			for i, col := range tableMeta.Columns {
				columns[i] = col.Name
			}
			logger.Debugf(" [buildSelectResult] SELECT * 列信息: %d 列", len(columns))
		} else {
			// 指定列查询，使用解析的表达式
			columns = se.selectExprs
			logger.Debugf(" [buildSelectResult] 指定列查询: %v", columns)
		}
	}

	result := &SelectResult{
		Records:     limitedRecords,
		RowCount:    len(limitedRecords),
		Columns:     columns,
		ColumnTypes: se.getColumnTypes(columns),
		ResultType:  common.RESULT_TYPE_QUERY,
		Message:     fmt.Sprintf("Query OK, %d rows in set", len(limitedRecords)),
	}

	logger.Debugf(" [buildSelectResult] 构建完成: %d行, %d列", result.RowCount, len(result.Columns))
	logger.Debugf(" [buildSelectResult] 列名: %v", result.Columns)

	return result
}

// applyProjection 应用投影
func (se *SelectExecutor) applyProjection(records []Record) []Record {
	// 如果是 SELECT *，不需要投影
	if len(se.selectExprs) == 1 && se.selectExprs[0] == "*" {
		return records
	}

	// 对每条记录进行投影
	projectedRecords := make([]Record, 0, len(records))
	for _, record := range records {
		projectedRecord, err := se.projectRecord(record, se.selectExprs)
		if err != nil {
			if isStoredFunctionAuthorizationError(err) {
				se.expressionError = err
			}
			logger.Warnf(" [applyProjection] 投影记录失败: %v，跳过该记录", err)
			continue
		}
		projectedRecords = append(projectedRecords, projectedRecord)
	}

	logger.Debugf(" [applyProjection] 投影完成: %d 条记录, %d 个字段", len(projectedRecords), len(se.selectExprs))
	return projectedRecords
}

// projectRecord 对单条记录进行投影
func (se *SelectExecutor) projectRecord(record Record, selectExprs []string) (Record, error) {
	// 获取表的元数据，用于列名映射
	tableMeta, err := se.getRecordTableMeta(record)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %v", err)
	}

	// 创建列名到索引的映射（大小写不敏感）
	columnIndexMap := se.buildColumnIndexMap(tableMeta)
	rowValues := make(map[string]interface{}, len(tableMeta.Columns))
	for index, column := range tableMeta.Columns {
		if column == nil || index >= len(record.GetValues()) {
			continue
		}
		rowValues[column.Name] = record.GetValues()[index].Raw()
		rowValues[strings.ToLower(column.Name)] = record.GetValues()[index].Raw()
	}
	if se.dataDir != "" {
		rowValues[storedFunctionDataDirKey] = se.dataDir
		rowValues[storedFunctionSchemaKey] = se.schemaName
		if se.storedFunctionAuthorizer != nil {
			rowValues[storedFunctionAuthorizerKey] = se.storedFunctionAuthorizer
		}
	}
	if se.sessionValues != nil {
		rowValues[sessionExpressionValuesKey] = se.sessionValues
	}

	// 解析选择的列并获取对应的值
	projectedValues := make([]basic.Value, 0, len(selectExprs))
	projectedColumns := make([]*metadata.ColumnMeta, 0, len(selectExprs))

	for exprIdx, expr := range selectExprs {
		// 清理表达式（移除空格、别名等）
		columnName := se.cleanColumnExpression(expr)
		outputName := columnName
		if exprIdx < len(se.selectAliases) && strings.TrimSpace(se.selectAliases[exprIdx]) != "" {
			outputName = strings.TrimSpace(se.selectAliases[exprIdx])
		}

		// 大小写不敏感查找列索引
		columnIndex, exists := se.findColumnIndex(columnName, columnIndexMap)
		if !exists {
			// Projection expressions (literals, arithmetic and common scalar
			// functions) are not physical columns. Evaluate them against the
			// current row before falling back to the historical NULL behavior.
			if value, evalErr := se.evaluateProjectionExpression(expr, rowValues); evalErr == nil {
				if value == nil {
					projectedValues = append(projectedValues, basic.NewNull())
				} else {
					projectedValues = append(projectedValues, basic.NewStringValue(projectionValueString(value)))
				}
				projectedColumns = append(projectedColumns, &metadata.ColumnMeta{Name: outputName, Type: projectionValueType(value)})
				continue
			} else if isStoredFunctionAuthorizationError(evalErr) {
				return nil, evalErr
			}
			projectedValues = append(projectedValues, basic.NewNull())
			projectedColumns = append(projectedColumns, &metadata.ColumnMeta{Name: outputName, Type: "UNKNOWN"})
			continue
		}

		// 获取列值
		value := record.GetValueByIndex(columnIndex)
		projectedValues = append(projectedValues, value)

		// 添加列元数据
		if columnIndex < len(tableMeta.Columns) {
			colMeta := *tableMeta.Columns[columnIndex]
			colMeta.Name = outputName
			projectedColumns = append(projectedColumns, &colMeta)
		} else {
			projectedColumns = append(projectedColumns, &metadata.ColumnMeta{
				Name: outputName,
				Type: "UNKNOWN",
			})
		}
	}

	// 创建新的表元数据
	projectedTableMeta := &metadata.TableMeta{
		Name:    tableMeta.Name + "_projected",
		Columns: projectedColumns,
	}

	// 创建投影后的记录
	projectedRecord := NewExecutorRecord(projectedValues, projectedTableMeta)

	return projectedRecord, nil
}

func evaluateProjectionExpression(expression string, values map[string]interface{}) (interface{}, error) {
	stmt, err := sqlparser.Parse("select " + expression)
	if err != nil {
		return nil, err
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok || len(selectStmt.SelectExprs) != 1 {
		return nil, fmt.Errorf("projection expression is not scalar")
	}
	aliased, ok := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("projection expression is not evaluable")
	}
	return evaluateExpressionWithRow(aliased.Expr, values)
}

// evaluateProjectionExpression first resolves a persisted stored function,
// then falls back to the normal scalar expression evaluator. The old
// executeStoredFunctionCall hook only handled a whole SELECT f(...); this
// path is needed for ordinary row projections such as SELECT id, f(id).
func (se *SelectExecutor) evaluateProjectionExpression(expression string, values map[string]interface{}) (interface{}, error) {
	if se.projectionExpressionCache == nil {
		se.projectionExpressionCache = make(map[string]projectionExpressionCacheEntry)
	}
	entry, ok := se.projectionExpressionCache[expression]
	if !ok {
		stmt, err := sqlparser.Parse("select " + expression)
		if err != nil {
			return nil, err
		}
		selectStmt, selectOK := stmt.(*sqlparser.Select)
		if !selectOK || len(selectStmt.SelectExprs) != 1 {
			return nil, fmt.Errorf("projection expression is not scalar")
		}
		aliased, aliasedOK := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr)
		if !aliasedOK {
			return nil, fmt.Errorf("projection expression is not evaluable")
		}
		entry.ast = aliased.Expr
		if built := plan.BuildExpression(entry.ast); built != nil {
			// The legacy executor applies additional SQL value coercions while
			// materializing rowValues. Keep CASE on its established AST evaluator
			// until those coercions are represented by the plan expression layer;
			// arithmetic and scalar predicates can use the equivalent compiled path.
			if _, isCase := built.(*plan.CaseExpression); !isCase {
				entry.compiled, _ = plan.CompileExpression(built)
			}
		}
		se.projectionExpressionCache[expression] = entry
	}
	if function, ok := entry.ast.(*sqlparser.FuncExpr); ok {
		if value, handled, functionErr := se.evaluatePersistedFunctionProjection(function, values); handled {
			return value, functionErr
		}
	}
	if entry.compiled != nil {
		return entry.compiled(planEvalContextWithRow(values))
	}
	return evaluateExpressionWithRow(entry.ast, values)
}

func (se *SelectExecutor) evaluatePersistedFunctionProjection(function *sqlparser.FuncExpr, values map[string]interface{}) (interface{}, bool, error) {
	if se == nil {
		return nil, false, nil
	}
	return evaluatePersistedFunctionProjectionWithContext(function, values, se.dataDir, se.schemaName)
}

const (
	storedFunctionDataDirKey    = "__xmysql_stored_function_data_dir"
	storedFunctionSchemaKey     = "__xmysql_stored_function_schema"
	storedFunctionAuthorizerKey = "__xmysql_stored_function_authorizer"
)

type storedFunctionAuthorizationError struct{ err error }

func (e *storedFunctionAuthorizationError) Error() string { return e.err.Error() }
func (e *storedFunctionAuthorizationError) Unwrap() error { return e.err }

func isStoredFunctionAuthorizationError(err error) bool {
	var target *storedFunctionAuthorizationError
	return errors.As(err, &target)
}

func evaluatePersistedFunctionProjectionWithContext(function *sqlparser.FuncExpr, values map[string]interface{}, dataDir, schemaName string) (interface{}, bool, error) {
	if function == nil || strings.TrimSpace(dataDir) == "" {
		return nil, false, nil
	}
	functionName := strings.Trim(function.Name.String(), "` ")
	functionSchema, bareName := schemaName, functionName
	if strings.Contains(functionName, ".") {
		parts := strings.SplitN(functionName, ".", 2)
		functionSchema, bareName = strings.Trim(parts[0], "` "), strings.Trim(parts[1], "` ")
	}
	path := filepath.Join(dataDir, functionSchema, bareName+".routine.json")
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	var object persistedStoredObject
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, true, err
	}
	if !strings.EqualFold(object.ObjectType, "function") {
		return nil, false, nil
	}
	if authorizer, ok := values[storedFunctionAuthorizerKey].(func(persistedStoredObject) error); ok && authorizer != nil {
		if err := authorizer(object); err != nil {
			return nil, true, &storedFunctionAuthorizationError{err: err}
		}
	}
	arguments := make([]string, 0, len(function.Exprs))
	for _, rawExpr := range function.Exprs {
		aliasedArg, ok := rawExpr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, true, fmt.Errorf("stored function argument is not scalar")
		}
		value, err := evaluateExpressionWithRow(aliasedArg.Expr, values)
		if err != nil {
			return nil, true, err
		}
		arguments = append(arguments, sessionUserVariableSQLLiteral(value))
	}
	expression, err := resolveStoredFunctionReturnExpression(object, arguments)
	if err != nil {
		return nil, true, err
	}
	returnValue, err := evaluateStoredRoutineScalarExpressionWithRow(expression, values)
	return returnValue, true, err
}

func evaluateStoredRoutineScalarExpressionWithRow(expression string, values map[string]interface{}) (interface{}, error) {
	statement, err := sqlparser.Parse("select " + strings.TrimSpace(expression))
	if err != nil {
		return nil, err
	}
	selectStatement, ok := statement.(*sqlparser.Select)
	if !ok || len(selectStatement.SelectExprs) != 1 {
		return nil, fmt.Errorf("stored function RETURN expression is not scalar")
	}
	aliased, ok := selectStatement.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("stored function RETURN expression is not evaluable")
	}
	return evaluateExpressionWithRow(aliased.Expr, values)
}

func projectionValueType(value interface{}) metadata.DataType {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return metadata.TypeBigInt
	case float32, float64:
		return metadata.TypeDouble
	case bool:
		return metadata.TypeTinyInt
	default:
		return metadata.TypeVarchar
	}
}

func projectionValueString(value interface{}) string {
	if bytes, ok := value.([]byte); ok {
		return string(bytes)
	}
	switch typed := value.(type) {
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	}
	return fmt.Sprint(value)
}

// getRecordTableMeta 获取记录的表元数据
func (se *SelectExecutor) getRecordTableMeta(record Record) (*metadata.TableMeta, error) {
	// 尝试从 EngineExecutorRecord 获取表元数据
	if engineRecord, ok := record.(*EngineExecutorRecord); ok {
		if meta := engineRecord.GetTableMeta(); meta != nil {
			return meta, nil
		}
	}

	// 如果无法获取元数据，尝试从 TableManager 获取
	if se.tableManager != nil {
		tableMeta, err := se.tableManager.GetTableMetadata(context.Background(), se.schemaName, se.tableName)
		if err == nil && tableMeta != nil {
			return tableMeta, nil
		}
	}

	// 最后使用默认的表结构
	return se.getDefaultTableMeta(), nil
}

// buildColumnIndexMap 构建列名到索引的映射（大小写不敏感）
func (se *SelectExecutor) buildColumnIndexMap(tableMeta *metadata.TableMeta) map[string]int {
	columnMap := make(map[string]int)

	for i, column := range tableMeta.Columns {
		// 使用小写作为键，实现大小写不敏感
		columnMap[strings.ToLower(column.Name)] = i
	}

	return columnMap
}

// findColumnIndex 查找列索引（大小写不敏感）
func (se *SelectExecutor) findColumnIndex(columnName string, columnMap map[string]int) (int, bool) {
	// 转换为小写进行查找
	lowerColumnName := strings.ToLower(columnName)
	index, exists := columnMap[lowerColumnName]
	return index, exists
}

// cleanColumnExpression 清理列表达式，移除空格和处理别名
func (se *SelectExecutor) cleanColumnExpression(expr string) string {
	// 移除前后空格
	expr = strings.TrimSpace(expr)

	// 处理别名（如果包含 AS 关键字）
	if strings.Contains(strings.ToUpper(expr), " AS ") {
		parts := strings.Split(expr, " ")
		if len(parts) > 0 {
			expr = strings.TrimSpace(parts[0])
		}
	}

	// 移除表前缀（如果存在）
	if strings.Contains(expr, ".") {
		parts := strings.Split(expr, ".")
		if len(parts) > 1 {
			expr = strings.TrimSpace(parts[len(parts)-1])
		}
	}

	// 移除引号
	expr = strings.Trim(expr, "`\"'")

	return expr
}

// getDefaultTableMeta 获取默认的表元数据
func (se *SelectExecutor) getDefaultTableMeta() *metadata.TableMeta {
	return &metadata.TableMeta{
		Name: se.tableName,
		Columns: []*metadata.ColumnMeta{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "VARCHAR"},
			{Name: "value", Type: "TEXT"},
		},
	}
}

// applyOrderBy 应用排序
func (se *SelectExecutor) applyOrderBy(records []Record) []Record {
	if len(se.orderByColumns) == 0 {
		return records
	}

	sortedRecords := append([]Record(nil), records...)
	sort.SliceStable(sortedRecords, func(i, j int) bool {
		for _, orderSpec := range se.orderByColumns {
			columnName, desc := parseOrderBySpec(orderSpec)
			left, leftOK := se.recordValueByColumnName(sortedRecords[i], columnName)
			right, rightOK := se.recordValueByColumnName(sortedRecords[j], columnName)
			if !leftOK || !rightOK {
				continue
			}
			cmp := compareScalarValues(left, right)
			if cmp == 0 {
				continue
			}
			if desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
	return sortedRecords
}

func parseOrderBySpec(orderSpec string) (string, bool) {
	trimmed := strings.TrimSpace(orderSpec)
	upper := strings.ToUpper(trimmed)
	switch {
	case strings.HasSuffix(upper, " DESC"):
		return strings.TrimSpace(trimmed[:len(trimmed)-5]), true
	case strings.HasSuffix(upper, " ASC"):
		return strings.TrimSpace(trimmed[:len(trimmed)-4]), false
	default:
		return trimmed, false
	}
}

func (se *SelectExecutor) recordValueByColumnName(record Record, columnName string) (interface{}, bool) {
	if record == nil {
		return nil, false
	}
	cleanName := strings.ToLower(se.cleanColumnExpression(columnName))
	tableMeta, err := se.getRecordTableMeta(record)
	if err != nil || tableMeta == nil {
		return nil, false
	}
	for idx, col := range tableMeta.Columns {
		if col == nil {
			continue
		}
		if strings.ToLower(col.Name) != cleanName {
			continue
		}
		value := recordValueForPredicate(record.GetValueByIndex(idx), col.Type)
		// A legacy or partially reconstructed .frm may leave the type
		// unknown. Numeric-looking values still need numeric ORDER BY behavior
		// in that case (for example after partition metadata maintenance).
		if strings.TrimSpace(string(col.Type)) == "" {
			if number, ok := numericStringValue(value); ok {
				return number, true
			}
		}
		return value, true
	}
	return nil, false
}

func (se *SelectExecutor) applyDistinct(records []Record) []Record {
	if !se.distinct {
		return records
	}
	seen := make(map[string]struct{}, len(records))
	distinctRecords := make([]Record, 0, len(records))
	for _, record := range records {
		key := recordDistinctKey(record)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		distinctRecords = append(distinctRecords, record)
	}
	return distinctRecords
}

func recordDistinctKey(record Record) string {
	if record == nil {
		return "<nil>"
	}
	values := record.GetValues()
	parts := make([]string, 0, len(values))
	for _, value := range values {
		if value == nil || value.IsNull() {
			parts = append(parts, "<null>")
			continue
		}
		parts = append(parts, fmt.Sprintf("%s:%v", value.Type().String(), value.Raw()))
	}
	return strings.Join(parts, "\x00")
}

func (se *SelectExecutor) hasAggregateQuery() bool {
	for _, expr := range se.selectExprs {
		if parseAggregateExpression(expr).funcName != "" {
			return true
		}
	}
	return false
}

type aggregateExpression struct {
	funcName    string
	column      string
	valueColumn string
	distinct    bool
	separator   string
	orderBy     string
	orderDesc   bool
}

func parseAggregateExpression(expr string) aggregateExpression {
	trimmed := strings.TrimSpace(expr)
	open := strings.Index(trimmed, "(")
	closeIdx := strings.LastIndex(trimmed, ")")
	if open <= 0 || closeIdx <= open {
		return aggregateExpression{}
	}
	fn := strings.ToUpper(strings.TrimSpace(trimmed[:open]))
	switch fn {
	case "COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT",
		"STD", "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VARIANCE", "VAR_POP", "VAR_SAMP",
		"BIT_AND", "BIT_OR", "BIT_XOR", "JSON_ARRAYAGG", "JSON_OBJECTAGG", "ANY_VALUE":
	default:
		return aggregateExpression{}
	}
	column := strings.TrimSpace(trimmed[open+1 : closeIdx])
	if fn == "JSON_OBJECTAGG" {
		arguments := splitTopLevelComma(column)
		if len(arguments) != 2 {
			return aggregateExpression{}
		}
		return aggregateExpression{funcName: fn, column: strings.TrimSpace(arguments[0]), valueColumn: strings.TrimSpace(arguments[1])}
	}
	distinct := false
	if strings.HasPrefix(strings.ToLower(column), "distinct ") {
		distinct = true
		column = strings.TrimSpace(column[len("distinct "):])
	}
	separator := ","
	if fn == "GROUP_CONCAT" {
		lowerColumn := strings.ToLower(column)
		if separatorAt := strings.Index(lowerColumn, " separator "); separatorAt >= 0 {
			separatorText := strings.TrimSpace(column[separatorAt+len(" separator "):])
			if len(separatorText) >= 2 && (separatorText[0] == '\'' || separatorText[0] == '"') {
				quote := separatorText[0]
				if end := strings.LastIndexByte(separatorText[1:], quote); end >= 0 {
					separator = separatorText[1 : end+1]
				}
			}
			column = strings.TrimSpace(column[:separatorAt])
		}
		if orderAt := strings.Index(strings.ToLower(column), " order by "); orderAt >= 0 {
			orderText := strings.TrimSpace(column[orderAt+len(" order by "):])
			orderFields := strings.Fields(orderText)
			orderBy := ""
			orderDesc := false
			if len(orderFields) > 0 {
				orderBy = strings.Trim(orderFields[0], "` ")
				orderDesc = len(orderFields) > 1 && strings.EqualFold(orderFields[1], "DESC")
			}
			column = strings.TrimSpace(column[:orderAt])
			return aggregateExpression{funcName: fn, column: strings.Trim(column, "` "), distinct: distinct, separator: separator, orderBy: orderBy, orderDesc: orderDesc}
		}
	}
	return aggregateExpression{funcName: fn, column: strings.Trim(column, "` "), distinct: distinct, separator: separator}
}

type aggregateAccumulator struct {
	fn         string
	countStar  bool
	count      int64
	sum        float64
	min        interface{}
	max        interface{}
	hasData    bool
	values     []string
	jsonValues []interface{}
	jsonObject map[string]interface{}
	anyValue   interface{}
	anySet     bool
	concatKeys []interface{}
	separator  string
	orderBy    string
	orderDesc  bool
	seen       map[string]struct{}
	sumSq      float64
	bits       uint64
	bitInit    bool
}

func (a *aggregateAccumulator) add(value interface{}) {
	a.addWithOrder(value, value)
}

func (a *aggregateAccumulator) addWithOrder(value, orderValue interface{}) {
	if a.seen != nil {
		if value == nil {
			return
		}
		text := fmt.Sprint(value)
		if _, exists := a.seen[text]; exists {
			return
		}
		a.seen[text] = struct{}{}
	}
	if strings.EqualFold(a.fn, "COUNT") {
		if value == nil && !a.countStar {
			return
		}
		a.count++
		return
	}
	if strings.EqualFold(a.fn, "JSON_ARRAYAGG") {
		a.jsonValues = append(a.jsonValues, value)
		a.hasData = true
		return
	}
	if strings.EqualFold(a.fn, "ANY_VALUE") {
		if !a.anySet && value != nil {
			a.anyValue = value
			a.anySet = true
		}
		return
	}
	if value == nil {
		return
	}
	if strings.EqualFold(a.fn, "GROUP_CONCAT") {
		text := fmt.Sprint(value)
		a.values = append(a.values, text)
		a.concatKeys = append(a.concatKeys, orderValue)
		a.hasData = true
		return
	}
	if strings.HasPrefix(a.fn, "BIT_") {
		if number, ok := toFloat64(value); ok {
			bits := uint64(number)
			switch a.fn {
			case "BIT_AND":
				if !a.bitInit {
					a.bits = ^uint64(0)
					a.bitInit = true
				}
				a.bits &= bits
			case "BIT_OR":
				a.bits |= bits
				a.bitInit = true
			case "BIT_XOR":
				a.bits ^= bits
				a.bitInit = true
			}
			a.count++
		}
		return
	}
	switch a.fn {
	case "SUM", "AVG":
		if num, ok := toFloat64(value); ok {
			a.sum += num
			a.sumSq += num * num
			a.count++
			a.hasData = true
		}
	case "STD", "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VARIANCE", "VAR_POP", "VAR_SAMP":
		if num, ok := toFloat64(value); ok {
			a.sum += num
			a.sumSq += num * num
			a.count++
			a.hasData = true
		}
	case "MIN":
		if !a.hasData || compareScalarValues(value, a.min) < 0 {
			a.min = value
			a.hasData = true
		}
	case "MAX":
		if !a.hasData || compareScalarValues(value, a.max) > 0 {
			a.max = value
			a.hasData = true
		}
	}
}

func (a *aggregateAccumulator) addJSONObject(key, value interface{}) {
	if key == nil {
		return
	}
	if a.jsonObject == nil {
		a.jsonObject = make(map[string]interface{})
	}
	a.jsonObject[fmt.Sprint(key)] = value
	a.hasData = true
}

func (a *aggregateAccumulator) value() interface{} {
	switch a.fn {
	case "COUNT":
		return a.count
	case "SUM":
		if !a.hasData {
			return nil
		}
		return normalizeNumericResult(a.sum)
	case "AVG":
		if a.count == 0 {
			return nil
		}
		return a.sum / float64(a.count)
	case "GROUP_CONCAT":
		if !a.hasData {
			return nil
		}
		separator := a.separator
		if separator == "" {
			separator = ","
		}
		if a.orderBy != "" && len(a.values) > 1 {
			indices := make([]int, len(a.values))
			for index := range indices {
				indices[index] = index
			}
			sort.SliceStable(indices, func(i, j int) bool {
				comparison := compareScalarValues(a.concatKeys[indices[i]], a.concatKeys[indices[j]])
				if a.orderDesc {
					return comparison > 0
				}
				return comparison < 0
			})
			ordered := make([]string, 0, len(a.values))
			for _, index := range indices {
				ordered = append(ordered, a.values[index])
			}
			return strings.Join(ordered, separator)
		}
		return strings.Join(a.values, separator)
	case "JSON_ARRAYAGG":
		if !a.hasData {
			return nil
		}
		encoded, err := json.Marshal(a.jsonValues)
		if err != nil {
			return nil
		}
		return string(encoded)
	case "JSON_OBJECTAGG":
		if !a.hasData {
			return nil
		}
		encoded, err := json.Marshal(a.jsonObject)
		if err != nil {
			return nil
		}
		return string(encoded)
	case "ANY_VALUE":
		return a.anyValue
	case "BIT_AND", "BIT_OR", "BIT_XOR":
		if !a.bitInit {
			return nil
		}
		// Keep the textual representation used by the legacy SELECT result
		// adapter; returning an int64 here would expose the internal big-endian
		// storage bytes through Result.Raw().
		return strconv.FormatUint(a.bits, 10)
	case "VARIANCE", "VAR_POP":
		if a.count == 0 {
			return nil
		}
		variance := (a.sumSq - a.sum*a.sum/float64(a.count)) / float64(a.count)
		if variance < 0 && variance > -1e-12 {
			variance = 0
		}
		return variance
	case "VAR_SAMP":
		if a.count < 2 {
			return nil
		}
		variance := (a.sumSq - a.sum*a.sum/float64(a.count)) / float64(a.count-1)
		if variance < 0 && variance > -1e-12 {
			variance = 0
		}
		return variance
	case "STD", "STDDEV", "STDDEV_POP":
		if a.count == 0 {
			return nil
		}
		variance := (a.sumSq - a.sum*a.sum/float64(a.count)) / float64(a.count)
		if variance < 0 && variance > -1e-12 {
			variance = 0
		}
		return math.Sqrt(variance)
	case "STDDEV_SAMP":
		if a.count < 2 {
			return nil
		}
		variance := (a.sumSq - a.sum*a.sum/float64(a.count)) / float64(a.count-1)
		if variance < 0 && variance > -1e-12 {
			variance = 0
		}
		return math.Sqrt(variance)
	case "MIN":
		return a.min
	case "MAX":
		return a.max
	default:
		return nil
	}
}

type aggregateGroupState struct {
	groupValues []interface{}
	accs        []aggregateAccumulator
}

func (se *SelectExecutor) buildAggregateSelectResult() *SelectResult {
	groupColumns := se.cleanColumnNames(se.groupByColumns)
	aggregateExprs := make([]aggregateExpression, len(se.selectExprs))
	for i, expr := range se.selectExprs {
		aggregateExprs[i] = parseAggregateExpression(expr)
	}

	groups := make(map[string]*aggregateGroupState)
	groupOrder := make([]string, 0)
	for _, record := range se.resultSet {
		groupValues := make([]interface{}, 0, len(groupColumns))
		for _, groupColumn := range groupColumns {
			value, _ := se.recordValueByColumnName(record, groupColumn)
			groupValues = append(groupValues, value)
		}
		groupKey := aggregateGroupKey(groupValues)
		if len(groupColumns) == 0 {
			groupKey = "__all__"
		}
		state := groups[groupKey]
		if state == nil {
			state = &aggregateGroupState{
				groupValues: groupValues,
				accs:        make([]aggregateAccumulator, len(se.selectExprs)),
			}
			for i, agg := range aggregateExprs {
				if agg.funcName != "" {
					state.accs[i] = aggregateAccumulator{fn: agg.funcName, countStar: agg.column == "*", separator: agg.separator, orderBy: agg.orderBy, orderDesc: agg.orderDesc}
					if agg.distinct {
						state.accs[i].seen = make(map[string]struct{})
					}
				}
			}
			groups[groupKey] = state
			groupOrder = append(groupOrder, groupKey)
		}

		for i, agg := range aggregateExprs {
			if agg.funcName == "" {
				continue
			}
			var value interface{}
			if agg.funcName == "JSON_OBJECTAGG" {
				key, _ := se.recordValueByColumnName(record, agg.column)
				value, _ = se.recordValueByColumnName(record, agg.valueColumn)
				state.accs[i].addJSONObject(key, value)
				continue
			}
			if agg.column != "*" {
				value, _ = se.recordValueByColumnName(record, agg.column)
			}
			orderValue := value
			if agg.orderBy != "" {
				orderValue, _ = se.recordValueByColumnName(record, agg.orderBy)
			}
			state.accs[i].addWithOrder(value, orderValue)
		}
	}
	if len(groupColumns) == 0 && len(groupOrder) == 0 {
		state := &aggregateGroupState{
			groupValues: nil,
			accs:        make([]aggregateAccumulator, len(se.selectExprs)),
		}
		for i, agg := range aggregateExprs {
			if agg.funcName == "" {
				continue
			}
			state.accs[i] = aggregateAccumulator{fn: agg.funcName, countStar: agg.column == "*", separator: agg.separator, orderBy: agg.orderBy, orderDesc: agg.orderDesc}
			if agg.distinct {
				state.accs[i].seen = make(map[string]struct{})
			}
		}
		groups["__all__"] = state
		groupOrder = append(groupOrder, "__all__")
	}

	columns := se.getColumnNames()
	columnTypes := make([]string, len(columns))
	records := make([]Record, 0, len(groupOrder))
	for _, key := range groupOrder {
		state := groups[key]
		if state == nil {
			continue
		}
		rowValues := make([]interface{}, 0, len(se.selectExprs))
		projectedColumns := make([]*metadata.ColumnMeta, 0, len(se.selectExprs))
		for i, expr := range se.selectExprs {
			agg := aggregateExprs[i]
			if agg.funcName != "" {
				value := state.accs[i].value()
				rowValues = append(rowValues, value)
				colType := metadata.TypeDecimal
				if agg.funcName == "COUNT" || strings.HasPrefix(agg.funcName, "BIT_") {
					colType = metadata.TypeBigInt
				} else if agg.funcName == "GROUP_CONCAT" || agg.funcName == "JSON_ARRAYAGG" || agg.funcName == "JSON_OBJECTAGG" {
					colType = metadata.TypeVarchar
				} else if agg.funcName == "ANY_VALUE" {
					colType = se.columnTypeForExpression(agg.column)
				}
				columnTypes[i] = strings.ToLower(string(colType))
				projectedColumns = append(projectedColumns, &metadata.ColumnMeta{Name: columns[i], Type: colType})
				continue
			}
			value := se.groupValueForExpression(expr, groupColumns, state.groupValues)
			rowValues = append(rowValues, value)
			colType := se.columnTypeForExpression(expr)
			columnTypes[i] = strings.ToLower(string(colType))
			projectedColumns = append(projectedColumns, &metadata.ColumnMeta{Name: columns[i], Type: colType})
		}

		rowMap := make(map[string]interface{}, len(columns)*2)
		for i, col := range columns {
			rowMap[col] = rowValues[i]
			rowMap[strings.ToLower(col)] = rowValues[i]
		}
		for i, expr := range se.selectExprs {
			rowMap[expr] = rowValues[i]
			rowMap[strings.ToLower(expr)] = rowValues[i]
		}
		if se.havingCondition != "" {
			matches, err := rowMatchesWhereConditions(rowMap, []string{se.havingCondition})
			if err != nil || !matches {
				continue
			}
		}

		records = append(records, NewExecutorRecordFromInterface(rowValues, &metadata.TableMeta{
			Name:    se.tableName + "_aggregate",
			Columns: projectedColumns,
		}))
	}

	sortedRecords := se.applyOrderBy(records)
	distinctRecords := se.applyDistinct(sortedRecords)
	limitedRecords := se.applyLimitOffset(distinctRecords)
	return &SelectResult{
		Records:     limitedRecords,
		RowCount:    len(limitedRecords),
		Columns:     columns,
		ColumnTypes: columnTypes,
		ResultType:  common.RESULT_TYPE_QUERY,
		Message:     fmt.Sprintf("Query OK, %d rows in set", len(limitedRecords)),
	}
}

func aggregateGroupKey(values []interface{}) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprintf("%T:%v", value, value))
	}
	return strings.Join(parts, "\x00")
}

func (se *SelectExecutor) cleanColumnNames(columns []string) []string {
	cleaned := make([]string, 0, len(columns))
	for _, column := range columns {
		cleaned = append(cleaned, se.cleanColumnExpression(column))
	}
	return cleaned
}

func (se *SelectExecutor) groupValueForExpression(expr string, groupColumns []string, groupValues []interface{}) interface{} {
	cleanExpr := strings.ToLower(se.cleanColumnExpression(expr))
	for i, column := range groupColumns {
		if strings.ToLower(column) == cleanExpr && i < len(groupValues) {
			return groupValues[i]
		}
	}
	return nil
}

func (se *SelectExecutor) columnTypeForExpression(expr string) metadata.DataType {
	tableMeta, err := se.getTableMetadata()
	if err != nil || tableMeta == nil {
		return metadata.TypeVarchar
	}
	cleanExpr := strings.ToLower(se.cleanColumnExpression(expr))
	for _, col := range tableMeta.Columns {
		if col != nil && strings.ToLower(col.Name) == cleanExpr {
			return col.Type
		}
	}
	return metadata.TypeVarchar
}

// applyLimitOffset 应用LIMIT和OFFSET
func (se *SelectExecutor) applyLimitOffset(records []Record) []Record {
	start := se.offset
	if start >= len(records) {
		return []Record{}
	}

	end := len(records)
	if se.limit > 0 && start+se.limit < len(records) {
		end = start + se.limit
	}

	return records[start:end]
}

// getColumnNames 获取列名。SELECT * 时从 getTableMetadata 取列（含从 .frm 加载）；否则用解析出的 select 表达式。
func (se *SelectExecutor) getColumnNames() []string {
	if se.isCountStarQuery() {
		if len(se.selectAliases) > 0 && strings.TrimSpace(se.selectAliases[0]) != "" {
			return []string{strings.TrimSpace(se.selectAliases[0])}
		}
		return []string{se.selectExprs[0]}
	}
	if len(se.selectExprs) == 1 && se.selectExprs[0] == "*" {
		tableMeta, err := se.getTableMetadata()
		if err != nil || tableMeta == nil {
			return []string{"id", "name"}
		}
		names := make([]string, 0, len(tableMeta.Columns))
		for _, col := range tableMeta.Columns {
			names = append(names, col.Name)
		}
		return names
	}
	names := make([]string, len(se.selectExprs))
	for i, expr := range se.selectExprs {
		if i < len(se.selectAliases) && strings.TrimSpace(se.selectAliases[i]) != "" {
			names[i] = strings.TrimSpace(se.selectAliases[i])
		} else {
			names[i] = se.cleanColumnExpression(expr)
		}
	}
	return names
}

func (se *SelectExecutor) getColumnTypes(columns []string) []string {
	if len(columns) == 0 {
		return nil
	}
	tableMeta, err := se.getTableMetadata()
	if err != nil || tableMeta == nil {
		return nil
	}
	columnTypeByName := make(map[string]string, len(tableMeta.Columns))
	for _, col := range tableMeta.Columns {
		if col == nil {
			continue
		}
		columnTypeByName[strings.ToLower(col.Name)] = strings.ToLower(string(col.Type))
	}
	types := make([]string, len(columns))
	for i, column := range columns {
		cleanName := strings.ToLower(se.cleanColumnExpression(column))
		if typ := columnTypeByName[cleanName]; typ != "" {
			types[i] = typ
		}
	}
	return types
}

func (se *SelectExecutor) isCountStarQuery() bool {
	if len(se.selectExprs) != 1 {
		return false
	}
	normalized := strings.ToLower(strings.ReplaceAll(se.selectExprs[0], " ", ""))
	return normalized == "count(*)"
}

// SelectResult SELECT查询结果
type SelectResult struct {
	Records     []Record
	RowCount    int
	Columns     []string
	ColumnTypes []string
	ResultType  string
	Message     string
}

// InfoSchemaAdapter 信息模式适配器实现
type InfoSchemaAdapter struct {
	manager *manager.TableManager
}

// TableByName 根据表名查找表元信息
func (a *InfoSchemaAdapter) TableByName(name string) (*metadata.Table, error) {
	// The plan builder may pass a schema-qualified lookup key. Preserve the
	// schema on the synthetic compatibility metadata so the physical adapter
	// does not fall back to the caller's default database.
	tableName := strings.TrimSpace(name)
	schemaName := ""
	if parts := strings.SplitN(tableName, ".", 2); len(parts) == 2 {
		schemaName, tableName = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}
	table := &metadata.Table{
		Name: tableName,
		Columns: []*metadata.Column{
			{Name: "id", DataType: metadata.TypeInt},
			{Name: "name", DataType: metadata.TypeVarchar},
		},
	}
	if schemaName != "" {
		schema := metadata.NewSchema(schemaName)
		if err := schema.AddTable(table); err != nil {
			return nil, err
		}
	}
	return table, nil
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
	// 转换 basic.Value 数组为 interface{} 数组
	values := record.GetValues()
	result := make([]interface{}, len(values))
	for i, v := range values {
		result[i] = v.Raw()
	}
	return result
}

func (se *SelectExecutor) Close() error {
	se.currentRowIndex = 0
	se.resultSet = nil
	return nil
}
