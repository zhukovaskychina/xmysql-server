package plan

import (
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// IndexPushdownOptimizer 索引下推优化器
type IndexPushdownOptimizer struct {
	tableStats  map[string]*TableStats
	indexStats  map[string]*IndexStats
	columnStats map[string]*ColumnStats
}

// NewIndexPushdownOptimizer 创建索引下推优化器
func NewIndexPushdownOptimizer() *IndexPushdownOptimizer {
	return &IndexPushdownOptimizer{
		tableStats:  make(map[string]*TableStats),
		indexStats:  make(map[string]*IndexStats),
		columnStats: make(map[string]*ColumnStats),
	}
}

// IndexCondition 索引条件
type IndexCondition struct {
	Column      string      // 列名
	Operator    string      // 操作符 (=, <, >, <=, >=, IN, LIKE)
	Value       interface{} // 值
	CanPush     bool        // 是否可以下推
	Selectivity float64     // 选择性
}

// IndexCandidate 索引候选
type IndexCandidate struct {
	Index       *metadata.Index
	Conditions  []*IndexCondition
	CoverIndex  bool    // 是否覆盖索引
	Cost        float64 // 代价
	Selectivity float64 // 选择性
	KeyLength   int     // 使用的键长度
}

// OptimizeIndexAccess 优化索引访问
func (opt *IndexPushdownOptimizer) OptimizeIndexAccess(
	table *metadata.Table,
	whereConditions []Expression,
	selectColumns []string,
) (*IndexCandidate, error) {

	// 1. 分析WHERE条件
	conditions, err := opt.analyzeWhereConditions(whereConditions)
	if err != nil {
		return nil, fmt.Errorf("分析WHERE条件失败: %v", err)
	}

	// 2. 获取所有可用索引
	candidates := opt.generateIndexCandidates(table, conditions, selectColumns)

	// 3. 选择最优索引
	bestCandidate := opt.selectBestIndex(candidates)

	return bestCandidate, nil
}

// analyzeWhereConditions 分析WHERE条件
func (opt *IndexPushdownOptimizer) analyzeWhereConditions(conditions []Expression) ([]*IndexCondition, error) {
	var indexConditions []*IndexCondition

	for _, expr := range conditions {
		conds, err := opt.extractIndexConditions(expr)
		if err != nil {
			return nil, err
		}
		indexConditions = append(indexConditions, conds...)
	}

	return indexConditions, nil
}

// extractIndexConditions 从表达式中提取索引条件
func (opt *IndexPushdownOptimizer) extractIndexConditions(expr Expression) ([]*IndexCondition, error) {
	var conditions []*IndexCondition

	switch e := expr.(type) {
	case *BinaryOperation:
		cond, err := opt.extractBinaryCondition(e)
		if err != nil {
			return nil, err
		}
		if cond != nil {
			conditions = append(conditions, cond)
		}

	case *Function:
		// 处理函数表达式，如 IN, LIKE 等
		conds, err := opt.extractFunctionConditions(e)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, conds...)
	}

	return conditions, nil
}

// extractBinaryCondition 提取二元条件
func (opt *IndexPushdownOptimizer) extractBinaryCondition(expr *BinaryOperation) (*IndexCondition, error) {
	// 检查左侧是否为列引用
	leftCol, ok := expr.Left.(*Column)
	if !ok {
		return nil, nil
	}

	// 检查右侧是否为常量
	rightConst, ok := expr.Right.(*Constant)
	if !ok {
		return nil, nil
	}

	// 转换操作符
	operator := opt.convertOperator(expr.Op)
	if operator == "" {
		return nil, nil
	}

	// 计算选择性
	selectivity := opt.estimateSelectivity(leftCol.Name, operator, rightConst.Value)

	return &IndexCondition{
		Column:      leftCol.Name,
		Operator:    operator,
		Value:       rightConst.Value,
		CanPush:     opt.canPushCondition(operator),
		Selectivity: selectivity,
	}, nil
}

// extractFunctionConditions 提取函数条件
func (opt *IndexPushdownOptimizer) extractFunctionConditions(expr *Function) ([]*IndexCondition, error) {
	var conditions []*IndexCondition

	switch strings.ToUpper(expr.Name) {
	case "IN":
		// 处理 IN 条件
		if len(expr.Args) >= 2 {
			if col, ok := expr.Args[0].(*Column); ok {
				selectivity := opt.estimateInSelectivity(col.Name, expr.Args[1:])
				conditions = append(conditions, &IndexCondition{
					Column:      col.Name,
					Operator:    "IN",
					Value:       expr.Args[1:],
					CanPush:     true,
					Selectivity: selectivity,
				})
			}
		}

	case "LIKE":
		// 处理 LIKE 条件
		if len(expr.Args) == 2 {
			if col, ok := expr.Args[0].(*Column); ok {
				if pattern, ok := expr.Args[1].(*Constant); ok {
					canPush := opt.canPushLikeCondition(pattern.Value)
					selectivity := opt.estimateLikeSelectivity(col.Name, pattern.Value)
					conditions = append(conditions, &IndexCondition{
						Column:      col.Name,
						Operator:    "LIKE",
						Value:       pattern.Value,
						CanPush:     canPush,
						Selectivity: selectivity,
					})
				}
			}
		}
	}

	return conditions, nil
}

// generateIndexCandidates 生成索引候选
func (opt *IndexPushdownOptimizer) generateIndexCandidates(
	table *metadata.Table,
	conditions []*IndexCondition,
	selectColumns []string,
) []*IndexCandidate {

	var candidates []*IndexCandidate

	// 遍历所有索引
	for _, index := range table.Indices {
		candidate := opt.evaluateIndex(index, conditions, selectColumns)
		if candidate != nil {
			candidates = append(candidates, candidate)
		}
	}

	return candidates
}

// evaluateIndex 评估索引
func (opt *IndexPushdownOptimizer) evaluateIndex(
	index *metadata.Index,
	conditions []*IndexCondition,
	selectColumns []string,
) *IndexCandidate {

	candidate := &IndexCandidate{
		Index:      index,
		Conditions: make([]*IndexCondition, 0),
	}

	// 1. 匹配索引列与条件
	usedKeyLength := 0
	totalSelectivity := 1.0

	for i, indexCol := range index.Columns {
		found := false
		for _, cond := range conditions {
			if cond.Column == indexCol && cond.CanPush {
				candidate.Conditions = append(candidate.Conditions, cond)
				totalSelectivity *= cond.Selectivity
				usedKeyLength = i + 1
				found = true
				break
			}
		}

		// 如果某一列没有匹配的条件，后续列无法使用
		if !found {
			break
		}
	}

	// 如果没有可用条件，跳过此索引
	if len(candidate.Conditions) == 0 {
		return nil
	}

	candidate.KeyLength = usedKeyLength
	candidate.Selectivity = totalSelectivity

	// 2. 检查是否为覆盖索引
	candidate.CoverIndex = opt.isCoveringIndex(index, selectColumns)

	// 3. 计算代价
	candidate.Cost = opt.calculateIndexCost(index, candidate)

	return candidate
}

// isCoveringIndex 检查是否为覆盖索引
func (opt *IndexPushdownOptimizer) isCoveringIndex(index *metadata.Index, selectColumns []string) bool {
	if len(selectColumns) == 0 {
		return false
	}

	// 创建索引列集合
	indexCols := make(map[string]bool)
	for _, col := range index.Columns {
		indexCols[col] = true
	}

	// 检查所有选择列是否都在索引中
	for _, col := range selectColumns {
		if col == "*" {
			return false // SELECT * 不能被覆盖
		}
		if !indexCols[col] {
			return false
		}
	}

	return true
}

// selectBestIndex 选择最优索引
func (opt *IndexPushdownOptimizer) selectBestIndex(candidates []*IndexCandidate) *IndexCandidate {
	if len(candidates) == 0 {
		return nil
	}

	var best *IndexCandidate
	bestScore := float64(-1)

	for _, candidate := range candidates {
		score := opt.calculateIndexScore(candidate)
		if score > bestScore {
			bestScore = score
			best = candidate
		}
	}

	return best
}

// calculateIndexScore 计算索引评分
func (opt *IndexPushdownOptimizer) calculateIndexScore(candidate *IndexCandidate) float64 {
	score := 0.0

	// 1. 选择性越高越好
	score += candidate.Selectivity * 100

	// 2. 使用的键长度越长越好
	score += float64(candidate.KeyLength) * 10

	// 3. 覆盖索引加分
	if candidate.CoverIndex {
		score += 50
	}

	// 4. 唯一索引加分
	if candidate.Index.IsUnique {
		score += 20
	}

	// 5. 主键索引加分
	if candidate.Index.IsPrimary {
		score += 30
	}

	// 6. 代价越低越好
	score -= candidate.Cost / 100

	return score
}

// 辅助方法

// convertOperator 转换操作符
func (opt *IndexPushdownOptimizer) convertOperator(op BinaryOp) string {
	switch op {
	case OpEQ:
		return "="
	case OpNE:
		return "!="
	case OpLT:
		return "<"
	case OpLE:
		return "<="
	case OpGT:
		return ">"
	case OpGE:
		return ">="
	default:
		return ""
	}
}

// canPushCondition 检查条件是否可以下推
func (opt *IndexPushdownOptimizer) canPushCondition(operator string) bool {
	switch operator {
	case "=", "<", "<=", ">", ">=", "IN":
		return true
	case "LIKE":
		return true // 需要进一步检查模式
	default:
		return false
	}
}

// canPushLikeCondition 检查LIKE条件是否可以下推
func (opt *IndexPushdownOptimizer) canPushLikeCondition(pattern interface{}) bool {
	if str, ok := pattern.(string); ok {
		// 只有前缀匹配可以使用索引
		return !strings.HasPrefix(str, "%") && !strings.HasPrefix(str, "_")
	}
	return false
}

// estimateSelectivity 估算选择性
func (opt *IndexPushdownOptimizer) estimateSelectivity(column, operator string, value interface{}) float64 {
	// 获取列统计信息
	colStats, exists := opt.columnStats[column]
	if !exists {
		// 默认选择性
		switch operator {
		case "=":
			return 0.1
		case "<", "<=", ">", ">=":
			return 0.3
		default:
			return 0.5
		}
	}

	// 基于统计信息计算选择性
	switch operator {
	case "=":
		if colStats.DistinctCount > 0 {
			return 1.0 / float64(colStats.DistinctCount)
		}
		return 0.1
	case "<", "<=", ">", ">=":
		return opt.estimateRangeSelectivity(colStats, operator, value)
	default:
		return 0.5
	}
}

// estimateInSelectivity 估算IN条件选择性
func (opt *IndexPushdownOptimizer) estimateInSelectivity(column string, values []Expression) float64 {
	baseSelectivity := opt.estimateSelectivity(column, "=", nil)
	return baseSelectivity * float64(len(values))
}

// estimateLikeSelectivity 估算LIKE条件选择性
func (opt *IndexPushdownOptimizer) estimateLikeSelectivity(column string, pattern interface{}) float64 {
	if str, ok := pattern.(string); ok {
		// 前缀匹配的选择性估算
		if !strings.Contains(str, "%") && !strings.Contains(str, "_") {
			// 精确匹配
			return opt.estimateSelectivity(column, "=", pattern)
		}
		// 模糊匹配
		return 0.3
	}
	return 0.5
}

// estimateRangeSelectivity 估算范围条件选择性
func (opt *IndexPushdownOptimizer) estimateRangeSelectivity(
	colStats *ColumnStats,
	operator string,
	value interface{},
) float64 {
	// 简化实现：基于直方图估算
	if colStats.Histogram != nil {
		return opt.estimateFromHistogram(colStats.Histogram, operator, value)
	}

	// 默认范围选择性
	switch operator {
	case "<", "<=":
		return 0.3
	case ">", ">=":
		return 0.3
	default:
		return 0.5
	}
}

// estimateFromHistogram 基于直方图估算选择性
func (opt *IndexPushdownOptimizer) estimateFromHistogram(
	hist *Histogram,
	operator string,
	value interface{},
) float64 {
	// 简化实现：遍历桶估算
	totalCount := float64(hist.TotalCount)
	matchCount := 0.0

	for _, bucket := range hist.Buckets {
		switch operator {
		case "<", "<=":
			if opt.compareValues(bucket.UpperBound, value) <= 0 {
				matchCount += float64(bucket.Count)
			}
		case ">", ">=":
			if opt.compareValues(bucket.LowerBound, value) >= 0 {
				matchCount += float64(bucket.Count)
			}
		}
	}

	if totalCount > 0 {
		return matchCount / totalCount
	}
	return 0.3
}

// compareValues 比较值
func (opt *IndexPushdownOptimizer) compareValues(a, b interface{}) int {
	// 简化实现：只处理数值和字符串
	switch va := a.(type) {
	case int64:
		if vb, ok := b.(int64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case string:
		if vb, ok := b.(string); ok {
			return strings.Compare(va, vb)
		}
	}
	return 0
}

// calculateIndexCost 计算索引代价
func (opt *IndexPushdownOptimizer) calculateIndexCost(
	index *metadata.Index,
	candidate *IndexCandidate,
) float64 {
	// 基础代价
	baseCost := 1.0

	// 索引扫描代价
	indexScanCost := baseCost * (1.0 - candidate.Selectivity) * 0.1

	// 回表代价（如果不是覆盖索引）
	lookupCost := 0.0
	if !candidate.CoverIndex {
		lookupCost = baseCost * candidate.Selectivity * 0.5
	}

	return indexScanCost + lookupCost
}

// SetStatistics 设置统计信息
func (opt *IndexPushdownOptimizer) SetStatistics(
	tableStats map[string]*TableStats,
	indexStats map[string]*IndexStats,
	columnStats map[string]*ColumnStats,
) {
	opt.tableStats = tableStats
	opt.indexStats = indexStats
	opt.columnStats = columnStats
}
