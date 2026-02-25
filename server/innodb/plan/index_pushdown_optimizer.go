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
	Column        string      // 列名
	Operator      string      // 操作符 (=, <, >, <=, >=, IN, LIKE)
	Value         interface{} // 值
	CanPush       bool        // 是否可以下推
	Selectivity   float64     // 选择性
	Priority      int         // 下推优先级（新增）
	IndexPosition int         // 在索引中的位置（新增）
}

// IndexCandidate 索引候选
type IndexCandidate struct {
	Index       *metadata.Index
	Conditions  []*IndexCondition
	CoverIndex  bool    // 是否覆盖索引
	Cost        float64 // 代价
	Selectivity float64 // 选择性
	KeyLength   int     // 使用的键长度
	Score       float64 // 综合评分（新增）
	Reason      string  // 选择原因（新增）
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

	// 2.1 合并索引候选
	merged := opt.mergeCandidates(candidates)
	candidates = append(candidates, merged...)

	// 3. 选择最优索引或索引合并方案
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

	switch strings.ToUpper(expr.Name()) {
	case "IN":
		// 处理 IN 条件
		if len(expr.Args()) >= 2 {
			if col, ok := expr.Args()[0].(*Column); ok {
				selectivity := opt.estimateInSelectivity(col.Name, expr.Args()[1:])
				conditions = append(conditions, &IndexCondition{
					Column:      col.Name,
					Operator:    "IN",
					Value:       expr.Args()[1:],
					CanPush:     true,
					Selectivity: selectivity,
				})
			}
		}

	case "LIKE":
		// 处理 LIKE 条件
		if len(expr.Args()) == 2 {
			if col, ok := expr.Args()[0].(*Column); ok {
				if pattern, ok := expr.Args()[1].(*Constant); ok {
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

	// 1. 匹配索引列与条件（最左前缀原则）
	usedKeyLength := 0
	totalSelectivity := 1.0
	hasRangeCondition := false

	for i, indexCol := range index.Columns {
		found := false
		for _, cond := range conditions {
			if cond.Column == indexCol && cond.CanPush {
				// 检查范围查询边界：范围查询后的列不可用
				if hasRangeCondition {
					break
				}

				// 添加条件并设置其位置
				condCopy := *cond
				condCopy.IndexPosition = i
				condCopy.Priority = opt.calculateConditionPriority(&condCopy)
				candidate.Conditions = append(candidate.Conditions, &condCopy)
				totalSelectivity *= cond.Selectivity
				usedKeyLength = i + 1

				// 检测是否为范围条件
				if opt.isRangeCondition(cond.Operator) {
					hasRangeCondition = true
				}

				found = true
				break
			}
		}

		// 如果某一列没有匹配的条件，后续列无法使用（最左前缀原则）
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

	// 4. 计算综合评分
	candidate.Score = opt.calculateIndexScore(candidate)

	// 5. 生成选择原因
	candidate.Reason = opt.generateSelectionReason(candidate)

	return candidate
}

// mergeCandidates 生成合并索引候选
func (opt *IndexPushdownOptimizer) mergeCandidates(candidates []*IndexCandidate) []*IndexCandidate {
	var merged []*IndexCandidate

	// 仅当有多个候选时才考虑合并
	if len(candidates) < 2 {
		return merged
	}

	for i := 0; i < len(candidates); i++ {
		for j := i + 1; j < len(candidates); j++ {
			c1 := candidates[i]
			c2 := candidates[j]
			if c1 == nil || c2 == nil {
				continue
			}

			// 检查是否可以合并（条件不重叠）
			if !opt.canMergeIndexes(c1, c2) {
				continue
			}

			// 创建合并候选
			conds := append([]*IndexCondition{}, c1.Conditions...)
			conds = append(conds, c2.Conditions...)

			// 计算合并后的选择性（OR语义）
			// 选择性 = sel1 + sel2 - sel1 * sel2
			mergedSel := c1.Selectivity + c2.Selectivity - c1.Selectivity*c2.Selectivity

			// 计算合并代价
			mergeCost := opt.calculateMergeCost(c1, c2)
			totalCost := c1.Cost + c2.Cost + mergeCost

			mergedCandidate := &IndexCandidate{
				Index: &metadata.Index{
					Name: c1.Index.Name + "+" + c2.Index.Name,
				},
				Conditions:  conds,
				CoverIndex:  c1.CoverIndex && c2.CoverIndex,
				Cost:        totalCost,
				Selectivity: mergedSel,
				KeyLength:   c1.KeyLength + c2.KeyLength,
				Reason:      "索引合并",
			}

			mergedCandidate.Score = opt.calculateIndexScore(mergedCandidate)

			merged = append(merged, mergedCandidate)
		}
	}

	return merged
}

// canMergeIndexes 检查是否可以合并索引
func (opt *IndexPushdownOptimizer) canMergeIndexes(c1, c2 *IndexCandidate) bool {
	// 检查条件是否重叠
	cols1 := make(map[string]bool)
	for _, cond := range c1.Conditions {
		cols1[cond.Column] = true
	}

	for _, cond := range c2.Conditions {
		if cols1[cond.Column] {
			return false // 有重叠列，不能合并
		}
	}

	return true
}

// calculateMergeCost 计算合并代价
func (opt *IndexPushdownOptimizer) calculateMergeCost(c1, c2 *IndexCandidate) float64 {
	const (
		sortMergeCostPerRow = 0.05 // 排序归并代价
		deduplicationCost   = 0.02 // 去重代价
	)

	// 估算结果集大小
	resultSize := (c1.Selectivity + c2.Selectivity) * 1000 // 假设表有1000行

	// 合并代价 = 排序归并 + 去重
	mergeCost := resultSize * (sortMergeCostPerRow + deduplicationCost)

	return mergeCost
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

	// 如果是二级索引，需要添加隐式的主键列
	if !index.IsPrimary && index.Table != nil && index.Table.PrimaryKey != nil {
		for _, pkCol := range index.Table.PrimaryKey.Columns {
			indexCols[pkCol] = true
		}
	}

	// 检查所有选择列是否都在索引中
	for _, col := range selectColumns {
		// SELECT * 不能被覆盖
		if col == "*" {
			return false
		}

		// 处理聚合函数场景（如 COUNT(col), SUM(col)）
		colName := opt.extractColumnFromExpression(col)
		if colName == "" {
			continue // 跳过常量或复杂表达式
		}

		if !indexCols[colName] {
			return false
		}
	}

	return true
}

// extractColumnFromExpression 从表达式中提取列名（支持聚合函数）
func (opt *IndexPushdownOptimizer) extractColumnFromExpression(expr string) string {
	// 简化实现：处理 COUNT(col), SUM(col) 等情况
	expr = strings.TrimSpace(expr)

	// 处理聚合函数
	if strings.Contains(expr, "(") && strings.Contains(expr, ")") {
		start := strings.Index(expr, "(")
		end := strings.LastIndex(expr, ")")
		if start >= 0 && end > start {
			inner := strings.TrimSpace(expr[start+1 : end])
			// 如果是 * 或者常量，返回空
			if inner == "*" || inner == "1" {
				return ""
			}
			return inner
		}
	}

	return expr
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
		// 等值选择性 = 1 / NDV
		if colStats.DistinctCount > 0 {
			return 1.0 / float64(colStats.DistinctCount)
		}
		return 0.1

	case "<", "<=", ">", ">=":
		// 范围选择性：优先使用直方图
		if colStats.Histogram != nil {
			return opt.estimateFromHistogram(colStats.Histogram, operator, value)
		}
		// 回退到基于最大最小值的估算
		return opt.estimateRangeByMinMax(colStats, operator, value)

	default:
		return 0.5
	}
}

// estimateRangeByMinMax 基于最大最小值估算范围选择性
func (opt *IndexPushdownOptimizer) estimateRangeByMinMax(
	colStats *ColumnStats,
	operator string,
	value interface{},
) float64 {
	if colStats.MaxValue == nil || colStats.MinValue == nil {
		return 0.3 // 默认范围选择性
	}

	// 简化实现：仅处理数值类型
	switch operator {
	case "<", "<=":
		// 估算 (value - min) / (max - min)
		return opt.estimateRangeFraction(colStats.MinValue, value, colStats.MaxValue)
	case ">", ">=":
		// 估算 (max - value) / (max - min)
		return opt.estimateRangeFraction(value, colStats.MaxValue, colStats.MaxValue)
	default:
		return 0.3
	}
}

// estimateRangeFraction 计算范围分数
func (opt *IndexPushdownOptimizer) estimateRangeFraction(min, value, max interface{}) float64 {
	// 尝试转换为数值类型
	minVal, minOk := toFloat64Safe(min)
	val, valOk := toFloat64Safe(value)
	maxVal, maxOk := toFloat64Safe(max)

	if !minOk || !valOk || !maxOk {
		return 0.3 // 无法转换，返回默认值
	}

	if maxVal-minVal <= 0 {
		return 0.5
	}

	fraction := (val - minVal) / (maxVal - minVal)

	// 限制在 [0, 1] 范围内
	if fraction < 0 {
		return 0.01
	}
	if fraction > 1 {
		return 1.0
	}

	return fraction
}

// toFloat64Safe 安全地转换为float64
func toFloat64Safe(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
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

// estimateFromHistogram 基于直方图估算选择性
func (opt *IndexPushdownOptimizer) estimateFromHistogram(
	hist *Histogram,
	operator string,
	value interface{},
) float64 {
	if hist == nil || len(hist.Buckets) == 0 {
		return 0.3
	}

	// 简化实现：遍历桶估算
	totalCount := float64(hist.TotalCount)
	if totalCount == 0 {
		return 0.3
	}

	matchCount := 0.0

	for _, bucket := range hist.Buckets {
		switch operator {
		case "<", "<=":
			// 如果桶的上界 <= value，全部匹配
			if opt.compareValues(bucket.UpperBound, value) <= 0 {
				matchCount += float64(bucket.Count)
			} else if opt.compareValues(bucket.LowerBound, value) < 0 {
				// 部分匹配：线性插值
				fraction := opt.estimateRangeFraction(bucket.LowerBound, value, bucket.UpperBound)
				matchCount += float64(bucket.Count) * fraction
			}

		case ">", ">=":
			// 如果桶的下界 >= value，全部匹配
			if opt.compareValues(bucket.LowerBound, value) >= 0 {
				matchCount += float64(bucket.Count)
			} else if opt.compareValues(bucket.UpperBound, value) > 0 {
				// 部分匹配
				fraction := opt.estimateRangeFraction(value, bucket.UpperBound, bucket.UpperBound)
				matchCount += float64(bucket.Count) * fraction
			}
		}
	}

	selectivity := matchCount / totalCount

	// 限制在 [0.01, 1.0] 范围内
	if selectivity < 0.01 {
		return 0.01
	}
	if selectivity > 1.0 {
		return 1.0
	}

	return selectivity
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
	// 获取统计信息
	indexStats, hasIndexStats := opt.indexStats[index.Name]
	var tableStats *TableStats
	if index.Table != nil {
		tableStats = opt.tableStats[index.Table.Name]
	}

	// 基础参数
	const (
		pageReadCost         = 1.0  // 单页读取代价
		indexRecordReadCost  = 0.1  // 索引记录读取代价
		cacheHitRatio        = 0.8  // 缓冲池命中率
		defaultIndexHeight   = 3    // 默认索引高度
		defaultTableRowCount = 1000 // 默认表行数
	)

	// 1. 索引扫描代价
	indexHeight := float64(defaultIndexHeight)
	if hasIndexStats && indexStats != nil {
		// 如果有统计信息，使用实际高度
		// 注：这里假设 IndexStats 中有 Height 字段，如果没有需要添加
	}

	tableRowCount := float64(defaultTableRowCount)
	if tableStats != nil && tableStats.RowCount > 0 {
		tableRowCount = float64(tableStats.RowCount)
	}

	// 估算扫描行数 = 总行数 × 选择性
	estimatedRows := tableRowCount * candidate.Selectivity

	// 索引扫描代价 = 索引高度 × 页读取代价 + 估算行数 × 记录读取代价
	indexScanCost := indexHeight*pageReadCost + estimatedRows*indexRecordReadCost

	// 2. 回表代价（如果不是覆盖索引）
	lookupCost := 0.0
	if !candidate.CoverIndex {
		// 回表代价 = 估算行数 × (1 - 缓冲命中率) × 页读取代价
		lookupCost = estimatedRows * (1 - cacheHitRatio) * pageReadCost
	}

	// 3. CPU处理代价（谓词计算次数）
	cpuCost := estimatedRows * 0.01 * float64(len(candidate.Conditions))

	// 总代价
	totalCost := indexScanCost + lookupCost + cpuCost

	return totalCost
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

// isRangeCondition 检查是否为范围条件
func (opt *IndexPushdownOptimizer) isRangeCondition(operator string) bool {
	switch operator {
	case "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
}

// calculateConditionPriority 计算条件下推优先级
func (opt *IndexPushdownOptimizer) calculateConditionPriority(cond *IndexCondition) int {
	// 优先级：等值 > IN > 范围 > LIKE
	switch cond.Operator {
	case "=":
		return 100
	case "IN":
		return 80
	case "<", "<=", ">", ">=":
		return 60
	case "LIKE":
		return 40
	default:
		return 20
	}
}

// generateSelectionReason 生成选择原因
func (opt *IndexPushdownOptimizer) generateSelectionReason(candidate *IndexCandidate) string {
	reasons := make([]string, 0)

	// 覆盖索引
	if candidate.CoverIndex {
		reasons = append(reasons, "覆盖索引")
	}

	// 使用的键长度
	if candidate.KeyLength > 0 {
		reasons = append(reasons, fmt.Sprintf("使用%d个索引列", candidate.KeyLength))
	}

	// 选择性
	if candidate.Selectivity < 0.1 {
		reasons = append(reasons, "高选择性")
	}

	// 索引类型
	if candidate.Index.IsPrimary {
		reasons = append(reasons, "主键索引")
	} else if candidate.Index.IsUnique {
		reasons = append(reasons, "唯一索引")
	}

	if len(reasons) == 0 {
		return "默认选择"
	}

	return strings.Join(reasons, ", ")
}
