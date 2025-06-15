package integration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// SQLParserIntegrator SQL解析器集成器
// 负责查询优化器与SQL解析器的无缝对接
type SQLParserIntegrator struct {
	sync.RWMutex

	// 解析器组件
	parser *sqlparser.Parser

	// 优化器组件
	optimizerManager    *manager.OptimizerManager
	storageIntegrator   *StorageEngineIntegrator
	statisticsCollector *plan.StatisticsCollector
	indexOptimizer      *plan.IndexPushdownOptimizer

	// 元数据管理
	schemaManager metadata.InfoSchemaManager
	tableManager  *manager.TableManager

	// 集成状态
	isInitialized bool
	parserStats   *ParserIntegrationStats
}

// ParserIntegrationStats 解析器集成统计信息
type ParserIntegrationStats struct {
	ParsedQueries         uint64
	OptimizedQueries      uint64
	ParseErrors           uint64
	OptimizationErrors    uint64
	AvgParseTime          time.Duration
	AvgOptimizationTime   time.Duration
	TotalParseTime        time.Duration
	TotalOptimizationTime time.Duration
}

// NewSQLParserIntegrator 创建SQL解析器集成器
func NewSQLParserIntegrator(
	optimizerManager *manager.OptimizerManager,
	storageIntegrator *StorageEngineIntegrator,
	schemaManager metadata.InfoSchemaManager,
	tableManager *manager.TableManager,
) *SQLParserIntegrator {
	integrator := &SQLParserIntegrator{
		optimizerManager:  optimizerManager,
		storageIntegrator: storageIntegrator,
		schemaManager:     schemaManager,
		tableManager:      tableManager,
		isInitialized:     false,
		parserStats:       &ParserIntegrationStats{},
	}

	// 初始化集成组件
	integrator.initializeIntegration()

	return integrator
}

// initializeIntegration 初始化集成组件
func (spi *SQLParserIntegrator) initializeIntegration() {
	// 初始化解析器
	spi.parser = &sqlparser.Parser{}

	// 获取优化器组件
	spi.statisticsCollector = spi.storageIntegrator.statisticsCollector
	spi.indexOptimizer = spi.storageIntegrator.indexOptimizer

	spi.isInitialized = true
	logger.Info("SQL解析器集成器初始化完成")
}

// ParseAndOptimize 解析并优化SQL查询
func (spi *SQLParserIntegrator) ParseAndOptimize(
	ctx context.Context,
	query string,
	databaseName string,
) (*OptimizedSQLPlan, error) {
	startTime := time.Now()
	defer func() {
		spi.updateParserStats(time.Since(startTime))
	}()

	if !spi.isInitialized {
		return nil, fmt.Errorf("SQL解析器集成器未初始化")
	}

	// 1. SQL语法解析
	parseStartTime := time.Now()
	stmt, err := spi.parseSQL(query)
	if err != nil {
		spi.parserStats.ParseErrors++
		return nil, fmt.Errorf("SQL解析失败: %v", err)
	}
	parseTime := time.Since(parseStartTime)

	// 2. 语义分析
	semanticInfo, err := spi.analyzeSemantics(ctx, stmt, databaseName)
	if err != nil {
		return nil, fmt.Errorf("语义分析失败: %v", err)
	}

	// 3. 查询优化
	optimizeStartTime := time.Now()
	optimizedPlan, err := spi.optimizeQuery(ctx, stmt, semanticInfo)
	if err != nil {
		spi.parserStats.OptimizationErrors++
		return nil, fmt.Errorf("查询优化失败: %v", err)
	}
	optimizeTime := time.Since(optimizeStartTime)

	// 4. 构建最终计划
	sqlPlan := &OptimizedSQLPlan{
		OriginalQuery:    query,
		ParsedStatement:  stmt,
		SemanticInfo:     semanticInfo,
		OptimizedPlan:    optimizedPlan,
		ParseTime:        parseTime,
		OptimizationTime: optimizeTime,
		TotalTime:        time.Since(startTime),
	}

	spi.parserStats.ParsedQueries++
	spi.parserStats.OptimizedQueries++

	return sqlPlan, nil
}

// parseSQL 解析SQL语句
func (spi *SQLParserIntegrator) parseSQL(query string) (sqlparser.Statement, error) {
	// 预处理SQL语句
	normalizedQuery := spi.normalizeSQL(query)

	// 解析SQL语句
	stmt, err := sqlparser.Parse(normalizedQuery)
	if err != nil {
		return nil, fmt.Errorf("解析SQL语句失败: %v", err)
	}

	return stmt, nil
}

// normalizeSQL 规范化SQL语句
func (spi *SQLParserIntegrator) normalizeSQL(query string) string {
	// 移除多余的空白字符
	query = strings.TrimSpace(query)

	// 统一关键字大小写
	query = strings.ReplaceAll(query, " select ", " SELECT ")
	query = strings.ReplaceAll(query, " from ", " FROM ")
	query = strings.ReplaceAll(query, " where ", " WHERE ")
	query = strings.ReplaceAll(query, " order by ", " ORDER BY ")
	query = strings.ReplaceAll(query, " group by ", " GROUP BY ")
	query = strings.ReplaceAll(query, " having ", " HAVING ")
	query = strings.ReplaceAll(query, " limit ", " LIMIT ")

	return query
}

// analyzeSemantics 语义分析
func (spi *SQLParserIntegrator) analyzeSemantics(
	ctx context.Context,
	stmt sqlparser.Statement,
	databaseName string,
) (*SemanticInfo, error) {
	semanticInfo := &SemanticInfo{
		DatabaseName: databaseName,
		QueryType:    spi.getQueryType(stmt),
		Tables:       make([]*metadata.Table, 0),
		Columns:      make([]*metadata.Column, 0),
		Conditions:   make([]plan.Expression, 0),
	}

	// 根据语句类型进行语义分析
	switch v := stmt.(type) {
	case *sqlparser.Select:
		return spi.analyzeSelectSemantics(ctx, v, semanticInfo)
	case *sqlparser.Insert:
		return spi.analyzeInsertSemantics(ctx, v, semanticInfo)
	case *sqlparser.Update:
		return spi.analyzeUpdateSemantics(ctx, v, semanticInfo)
	case *sqlparser.Delete:
		return spi.analyzeDeleteSemantics(ctx, v, semanticInfo)
	default:
		return semanticInfo, nil
	}
}

// analyzeSelectSemantics 分析SELECT语句语义
func (spi *SQLParserIntegrator) analyzeSelectSemantics(
	ctx context.Context,
	stmt *sqlparser.Select,
	semanticInfo *SemanticInfo,
) (*SemanticInfo, error) {
	// 1. 分析FROM子句
	if err := spi.analyzeFromClause(ctx, stmt.From, semanticInfo); err != nil {
		return nil, fmt.Errorf("分析FROM子句失败: %v", err)
	}

	// 2. 分析SELECT表达式
	if err := spi.analyzeSelectExpressions(ctx, stmt.SelectExprs, semanticInfo); err != nil {
		return nil, fmt.Errorf("分析SELECT表达式失败: %v", err)
	}

	// 3. 分析WHERE条件
	if stmt.Where != nil {
		conditions, err := spi.analyzeWhereClause(ctx, stmt.Where, semanticInfo)
		if err != nil {
			return nil, fmt.Errorf("分析WHERE子句失败: %v", err)
		}
		semanticInfo.Conditions = conditions
	}

	// 4. 分析ORDER BY子句
	if len(stmt.OrderBy) > 0 {
		orderColumns, err := spi.analyzeOrderByClause(ctx, stmt.OrderBy, semanticInfo)
		if err != nil {
			return nil, fmt.Errorf("分析ORDER BY子句失败: %v", err)
		}
		semanticInfo.OrderByColumns = orderColumns
	}

	// 5. 分析GROUP BY子句
	if len(stmt.GroupBy) > 0 {
		groupColumns, err := spi.analyzeGroupByClause(ctx, stmt.GroupBy, semanticInfo)
		if err != nil {
			return nil, fmt.Errorf("分析GROUP BY子句失败: %v", err)
		}
		semanticInfo.GroupByColumns = groupColumns
	}

	return semanticInfo, nil
}

// analyzeFromClause 分析FROM子句
func (spi *SQLParserIntegrator) analyzeFromClause(
	ctx context.Context,
	fromExprs sqlparser.TableExprs,
	semanticInfo *SemanticInfo,
) error {
	for _, fromExpr := range fromExprs {
		switch v := fromExpr.(type) {
		case *sqlparser.AliasedTableExpr:
			tableName := spi.extractTableName(v.Expr)
			if tableName != "" {
				table, err := spi.getTableMetadata(ctx, semanticInfo.DatabaseName, tableName)
				if err != nil {
					return fmt.Errorf("获取表元数据失败: %v", err)
				}
				semanticInfo.Tables = append(semanticInfo.Tables, table)
			}
		case *sqlparser.JoinTableExpr:
			// 处理JOIN表达式
			if err := spi.analyzeJoinExpression(ctx, v, semanticInfo); err != nil {
				return fmt.Errorf("分析JOIN表达式失败: %v", err)
			}
		}
	}
	return nil
}

// analyzeSelectExpressions 分析SELECT表达式
func (spi *SQLParserIntegrator) analyzeSelectExpressions(
	ctx context.Context,
	selectExprs sqlparser.SelectExprs,
	semanticInfo *SemanticInfo,
) error {
	for _, expr := range selectExprs {
		switch v := expr.(type) {
		case *sqlparser.StarExpr:
			// SELECT * - 添加所有列
			for _, table := range semanticInfo.Tables {
				semanticInfo.Columns = append(semanticInfo.Columns, table.Columns...)
			}
		case *sqlparser.AliasedExpr:
			// 分析具体的列表达式
			columns, err := spi.extractColumnsFromExpression(ctx, v.Expr, semanticInfo)
			if err != nil {
				return fmt.Errorf("提取列信息失败: %v", err)
			}
			semanticInfo.Columns = append(semanticInfo.Columns, columns...)
		}
	}
	return nil
}

// analyzeWhereClause 分析WHERE子句
func (spi *SQLParserIntegrator) analyzeWhereClause(
	ctx context.Context,
	where *sqlparser.Where,
	semanticInfo *SemanticInfo,
) ([]plan.Expression, error) {
	return spi.convertExpressionToPlanExpression(ctx, where.Expr, semanticInfo)
}

// convertExpressionToPlanExpression 将SQL表达式转换为计划表达式
func (spi *SQLParserIntegrator) convertExpressionToPlanExpression(
	ctx context.Context,
	expr sqlparser.Expr,
	semanticInfo *SemanticInfo,
) ([]plan.Expression, error) {
	var expressions []plan.Expression

	switch v := expr.(type) {
	case *sqlparser.ComparisonExpr:
		planExpr, err := spi.convertComparisonExpression(ctx, v, semanticInfo)
		if err != nil {
			return nil, err
		}
		expressions = append(expressions, planExpr)

	case *sqlparser.AndExpr:
		leftExprs, err := spi.convertExpressionToPlanExpression(ctx, v.Left, semanticInfo)
		if err != nil {
			return nil, err
		}
		rightExprs, err := spi.convertExpressionToPlanExpression(ctx, v.Right, semanticInfo)
		if err != nil {
			return nil, err
		}
		expressions = append(expressions, leftExprs...)
		expressions = append(expressions, rightExprs...)

	case *sqlparser.OrExpr:
		// OR表达式需要特殊处理
		leftExprs, err := spi.convertExpressionToPlanExpression(ctx, v.Left, semanticInfo)
		if err != nil {
			return nil, err
		}
		rightExprs, err := spi.convertExpressionToPlanExpression(ctx, v.Right, semanticInfo)
		if err != nil {
			return nil, err
		}

		// 创建OR表达式
		orExpr := &plan.BinaryOperation{
			Op:    plan.OpOR,
			Left:  &plan.ExpressionList{Expressions: leftExprs},
			Right: &plan.ExpressionList{Expressions: rightExprs},
		}
		expressions = append(expressions, orExpr)
	}

	return expressions, nil
}

// convertComparisonExpression 转换比较表达式
func (spi *SQLParserIntegrator) convertComparisonExpression(
	ctx context.Context,
	expr *sqlparser.ComparisonExpr,
	semanticInfo *SemanticInfo,
) (plan.Expression, error) {
	// 提取左操作数
	leftExpr, err := spi.convertValueExpression(ctx, expr.Left, semanticInfo)
	if err != nil {
		return nil, fmt.Errorf("转换左操作数失败: %v", err)
	}

	// 提取右操作数
	rightExpr, err := spi.convertValueExpression(ctx, expr.Right, semanticInfo)
	if err != nil {
		return nil, fmt.Errorf("转换右操作数失败: %v", err)
	}

	// 转换操作符
	op, err := spi.convertOperator(expr.Operator)
	if err != nil {
		return nil, fmt.Errorf("转换操作符失败: %v", err)
	}

	return &plan.BinaryOperation{
		Op:    op,
		Left:  leftExpr,
		Right: rightExpr,
	}, nil
}

// convertValueExpression 转换值表达式
func (spi *SQLParserIntegrator) convertValueExpression(
	ctx context.Context,
	expr sqlparser.Expr,
	semanticInfo *SemanticInfo,
) (plan.Expression, error) {
	switch v := expr.(type) {
	case *sqlparser.ColName:
		// 列引用
		columnName := v.Name.String()
		return &plan.Column{Name: columnName}, nil

	case *sqlparser.SQLVal:
		// 常量值
		value, err := spi.convertSQLValue(v)
		if err != nil {
			return nil, err
		}
		return &plan.Constant{Value: value}, nil

	default:
		return nil, fmt.Errorf("不支持的表达式类型: %T", v)
	}
}

// convertOperator 转换操作符
func (spi *SQLParserIntegrator) convertOperator(operator string) (plan.Operator, error) {
	switch operator {
	case "=":
		return plan.OpEQ, nil
	case "!=", "<>":
		return plan.OpNE, nil
	case "<":
		return plan.OpLT, nil
	case "<=":
		return plan.OpLE, nil
	case ">":
		return plan.OpGT, nil
	case ">=":
		return plan.OpGE, nil
	case "like":
		return plan.OpLike, nil
	case "in":
		return plan.OpIN, nil
	default:
		return plan.OpEQ, fmt.Errorf("不支持的操作符: %s", operator)
	}
}

// convertSQLValue 转换SQL值
func (spi *SQLParserIntegrator) convertSQLValue(val *sqlparser.SQLVal) (interface{}, error) {
	switch val.Type {
	case sqlparser.StrVal:
		return string(val.Val), nil
	case sqlparser.IntVal:
		return string(val.Val), nil // 需要进一步解析为数字
	case sqlparser.FloatVal:
		return string(val.Val), nil // 需要进一步解析为浮点数
	default:
		return string(val.Val), nil
	}
}

// optimizeQuery 优化查询
func (spi *SQLParserIntegrator) optimizeQuery(
	ctx context.Context,
	stmt sqlparser.Statement,
	semanticInfo *SemanticInfo,
) (*OptimizedQueryPlan, error) {
	// 只处理SELECT语句的优化
	if selectStmt, ok := stmt.(*sqlparser.Select); ok {
		return spi.optimizeSelectQuery(ctx, selectStmt, semanticInfo)
	}

	// 其他类型的语句暂不优化
	return &OptimizedQueryPlan{
		Table:        nil,
		AccessMethod: AccessMethodTableScan,
		StorageHints: &StorageHints{},
	}, nil
}

// optimizeSelectQuery 优化SELECT查询
func (spi *SQLParserIntegrator) optimizeSelectQuery(
	ctx context.Context,
	stmt *sqlparser.Select,
	semanticInfo *SemanticInfo,
) (*OptimizedQueryPlan, error) {
	// 目前只支持单表查询优化
	if len(semanticInfo.Tables) != 1 {
		return &OptimizedQueryPlan{
			Table:        nil,
			AccessMethod: AccessMethodTableScan,
			StorageHints: &StorageHints{},
		}, nil
	}

	table := semanticInfo.Tables[0]
	selectColumns := spi.extractSelectColumnNames(semanticInfo.Columns)

	// 使用存储引擎集成器进行优化
	optimizedPlan, err := spi.storageIntegrator.OptimizeQuery(
		ctx, table, semanticInfo.Conditions, selectColumns)
	if err != nil {
		return nil, fmt.Errorf("存储引擎优化失败: %v", err)
	}

	return optimizedPlan, nil
}

// 辅助方法
func (spi *SQLParserIntegrator) getQueryType(stmt sqlparser.Statement) QueryType {
	switch stmt.(type) {
	case *sqlparser.Select:
		return QueryTypeSelect
	case *sqlparser.Insert:
		return QueryTypeInsert
	case *sqlparser.Update:
		return QueryTypeUpdate
	case *sqlparser.Delete:
		return QueryTypeDelete
	default:
		return QueryTypeOther
	}
}

func (spi *SQLParserIntegrator) extractTableName(expr sqlparser.TableExpr) string {
	switch v := expr.(type) {
	case sqlparser.TableName:
		return v.Name.String()
	default:
		return ""
	}
}

func (spi *SQLParserIntegrator) getTableMetadata(
	ctx context.Context,
	databaseName, tableName string,
) (*metadata.Table, error) {
	// 从表管理器获取表元数据
	return spi.tableManager.GetTable(ctx, databaseName, tableName)
}

func (spi *SQLParserIntegrator) extractColumnsFromExpression(
	ctx context.Context,
	expr sqlparser.Expr,
	semanticInfo *SemanticInfo,
) ([]*metadata.Column, error) {
	var columns []*metadata.Column

	switch v := expr.(type) {
	case *sqlparser.ColName:
		columnName := v.Name.String()
		// 在表中查找列
		for _, table := range semanticInfo.Tables {
			for _, column := range table.Columns {
				if column.Name == columnName {
					columns = append(columns, column)
					break
				}
			}
		}
	}

	return columns, nil
}

func (spi *SQLParserIntegrator) extractSelectColumnNames(columns []*metadata.Column) []string {
	names := make([]string, len(columns))
	for i, column := range columns {
		names[i] = column.Name
	}
	return names
}

func (spi *SQLParserIntegrator) analyzeInsertSemantics(
	ctx context.Context,
	stmt *sqlparser.Insert,
	semanticInfo *SemanticInfo,
) (*SemanticInfo, error) {
	// INSERT语句语义分析的简化实现
	tableName := stmt.Table.Name.String()
	table, err := spi.getTableMetadata(ctx, semanticInfo.DatabaseName, tableName)
	if err != nil {
		return nil, err
	}
	semanticInfo.Tables = append(semanticInfo.Tables, table)
	return semanticInfo, nil
}

func (spi *SQLParserIntegrator) analyzeUpdateSemantics(
	ctx context.Context,
	stmt *sqlparser.Update,
	semanticInfo *SemanticInfo,
) (*SemanticInfo, error) {
	// UPDATE语句语义分析的简化实现
	return semanticInfo, nil
}

func (spi *SQLParserIntegrator) analyzeDeleteSemantics(
	ctx context.Context,
	stmt *sqlparser.Delete,
	semanticInfo *SemanticInfo,
) (*SemanticInfo, error) {
	// DELETE语句语义分析的简化实现
	return semanticInfo, nil
}

func (spi *SQLParserIntegrator) analyzeJoinExpression(
	ctx context.Context,
	joinExpr *sqlparser.JoinTableExpr,
	semanticInfo *SemanticInfo,
) error {
	// JOIN表达式分析的简化实现
	return nil
}

func (spi *SQLParserIntegrator) analyzeOrderByClause(
	ctx context.Context,
	orderBy sqlparser.OrderBy,
	semanticInfo *SemanticInfo,
) ([]string, error) {
	var columns []string
	for _, order := range orderBy {
		if colName, ok := order.Expr.(*sqlparser.ColName); ok {
			columns = append(columns, colName.Name.String())
		}
	}
	return columns, nil
}

func (spi *SQLParserIntegrator) analyzeGroupByClause(
	ctx context.Context,
	groupBy sqlparser.GroupBy,
	semanticInfo *SemanticInfo,
) ([]string, error) {
	var columns []string
	for _, expr := range groupBy {
		if colName, ok := expr.(*sqlparser.ColName); ok {
			columns = append(columns, colName.Name.String())
		}
	}
	return columns, nil
}

func (spi *SQLParserIntegrator) updateParserStats(totalTime time.Duration) {
	spi.Lock()
	defer spi.Unlock()

	spi.parserStats.TotalParseTime += totalTime
	if spi.parserStats.ParsedQueries > 0 {
		spi.parserStats.AvgParseTime = spi.parserStats.TotalParseTime / time.Duration(spi.parserStats.ParsedQueries)
	}
}

// GetParserStats 获取解析器统计信息
func (spi *SQLParserIntegrator) GetParserStats() *ParserIntegrationStats {
	spi.RLock()
	defer spi.RUnlock()

	stats := *spi.parserStats
	return &stats
}

// Close 关闭集成器
func (spi *SQLParserIntegrator) Close() error {
	spi.Lock()
	defer spi.Unlock()

	spi.isInitialized = false
	logger.Info("SQL解析器集成器已关闭")
	return nil
}

// OptimizedSQLPlan 优化后的SQL计划
type OptimizedSQLPlan struct {
	OriginalQuery    string
	ParsedStatement  sqlparser.Statement
	SemanticInfo     *SemanticInfo
	OptimizedPlan    *OptimizedQueryPlan
	ParseTime        time.Duration
	OptimizationTime time.Duration
	TotalTime        time.Duration
}

// SemanticInfo 语义信息
type SemanticInfo struct {
	DatabaseName   string
	QueryType      QueryType
	Tables         []*metadata.Table
	Columns        []*metadata.Column
	Conditions     []plan.Expression
	OrderByColumns []string
	GroupByColumns []string
}

// QueryType 查询类型
type QueryType int

const (
	QueryTypeSelect QueryType = iota
	QueryTypeInsert
	QueryTypeUpdate
	QueryTypeDelete
	QueryTypeOther
)

func (qt QueryType) String() string {
	switch qt {
	case QueryTypeSelect:
		return "SELECT"
	case QueryTypeInsert:
		return "INSERT"
	case QueryTypeUpdate:
		return "UPDATE"
	case QueryTypeDelete:
		return "DELETE"
	default:
		return "OTHER"
	}
}
