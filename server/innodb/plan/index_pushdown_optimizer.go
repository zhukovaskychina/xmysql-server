package plan

import (
	"fmt"
	"math"
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

func lookupCaseInsensitiveStats[T any](stats map[string]*T, key string) (*T, bool) {
	if value, ok := stats[key]; ok {
		return value, true
	}
	for candidate, value := range stats {
		if strings.EqualFold(candidate, key) {
			return value, true
		}
	}
	return nil, false
}

// IndexCondition 索引条件
type IndexCondition struct {
	Column        string      // 列名
	Operator      string      // 操作符 (=, <, >, <=, >=, IN, LIKE)
	Value         interface{} // 值
	Escape        interface{} // explicit LIKE escape character, when present
	CanPush       bool        // 是否可以下推
	Selectivity   float64     // 选择性
	Priority      int         // 下推优先级（新增）
	IndexPosition int         // 在索引中的位置（新增）
}

// IndexCandidate 索引候选
type IndexCandidate struct {
	Index      *metadata.Index
	Conditions []*IndexCondition
	// IndexMergeBranches contains the independently scanned branches of an
	// OR predicate. A non-empty value means this candidate is a union plan and
	// its row identity must be de-duplicated before projection.
	IndexMergeBranches []*IndexCandidate
	DeduplicateRows    bool
	MergeMode          string  // INDEX_MERGE_UNION or INDEX_MERGE_INTERSECTION
	CoverIndex         bool    // 是否覆盖索引
	Cost               float64 // 代价
	Selectivity        float64 // 选择性
	KeyLength          int     // 使用的键长度
	Score              float64 // 综合评分（新增）
	Reason             string  // 选择原因（新增）
}

const (
	IndexMergeUnion        = "UNION"
	IndexMergeIntersection = "INTERSECTION"
)

// OptimizeIndexAccess 优化索引访问
func (opt *IndexPushdownOptimizer) OptimizeIndexAccess(
	table *metadata.Table,
	whereConditions []Expression,
	selectColumns []string,
) (*IndexCandidate, error) {
	if branches := splitTopLevelDisjunction(whereConditions); len(branches) > 1 {
		return opt.optimizeDisjunction(table, branches, selectColumns)
	}

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

const maxIndexMergeDisjunctionBranches = 16

// splitTopLevelDisjunction extracts a bounded DNF for OR predicates.  In
// addition to A OR B, this handles a shared conjunct such as
// common AND (A OR B) by copying the common predicate into both index-merge
// branches.  The bound keeps pathological boolean expressions from causing
// an optimizer-time cartesian-product explosion; callers then fall back to a
// regular scan when the expression is too complex.
func splitTopLevelDisjunction(expressions []Expression) [][]Expression {
	branches := [][]Expression{{}}
	hasDisjunction := false
	for _, expr := range expressions {
		alternatives, containsDisjunction, ok := expandDisjunction(expr)
		if !ok {
			return nil
		}
		hasDisjunction = hasDisjunction || containsDisjunction
		if len(branches)*len(alternatives) > maxIndexMergeDisjunctionBranches {
			return nil
		}
		combined := make([][]Expression, 0, len(branches)*len(alternatives))
		for _, prefix := range branches {
			for _, alternative := range alternatives {
				branch := append([]Expression{}, prefix...)
				branch = append(branch, alternative...)
				combined = append(combined, branch)
			}
		}
		branches = combined
	}
	if !hasDisjunction || len(branches) < 2 {
		return nil
	}
	return branches
}

func expandDisjunction(expr Expression) ([][]Expression, bool, bool) {
	if between, ok := expr.(*BetweenExpression); ok && between != nil && between.Not {
		lower, lowerOK := indexConstantBound(between.LowerExpr, between.Lower)
		upper, upperOK := indexConstantBound(between.UpperExpr, between.Upper)
		if !lowerOK || !upperOK || between.Column == nil {
			return [][]Expression{{expr}}, false, true
		}
		return [][]Expression{
			[]Expression{&BinaryOperation{Op: OpLT, Left: between.Column, Right: lower}},
			[]Expression{&BinaryOperation{Op: OpGT, Left: between.Column, Right: upper}},
		}, true, true
	}
	op, ok := expr.(*BinaryOperation)
	if !ok {
		return [][]Expression{{expr}}, false, true
	}
	switch op.Op {
	case OpOr:
		left, _, ok := expandDisjunction(op.Left)
		if !ok {
			return nil, false, false
		}
		right, _, ok := expandDisjunction(op.Right)
		if !ok {
			return nil, false, false
		}
		return append(left, right...), true, true
	case OpAnd:
		left, leftHasDisjunction, ok := expandDisjunction(op.Left)
		if !ok {
			return nil, false, false
		}
		right, rightHasDisjunction, ok := expandDisjunction(op.Right)
		if !ok {
			return nil, false, false
		}
		if len(left)*len(right) > maxIndexMergeDisjunctionBranches {
			return nil, false, false
		}
		result := make([][]Expression, 0, len(left)*len(right))
		for _, leftBranch := range left {
			for _, rightBranch := range right {
				branch := append([]Expression{}, leftBranch...)
				branch = append(branch, rightBranch...)
				result = append(result, branch)
			}
		}
		return result, leftHasDisjunction || rightHasDisjunction, true
	default:
		return [][]Expression{{expr}}, false, true
	}
}

func (opt *IndexPushdownOptimizer) optimizeDisjunction(
	table *metadata.Table,
	branches [][]Expression,
	selectColumns []string,
) (*IndexCandidate, error) {
	plans := make([]*IndexCandidate, 0, len(branches))
	for _, branch := range branches {
		conditions, err := opt.analyzeWhereConditions(branch)
		if err != nil {
			return nil, err
		}
		candidates := opt.generateIndexCandidates(table, conditions, selectColumns)
		plan := opt.selectBestIndex(candidates)
		if plan == nil {
			// A union of index scans cannot represent an unindexed branch.
			return nil, nil
		}
		plans = append(plans, plan)
	}
	if len(plans) < 2 {
		return nil, nil
	}

	selectivity := 0.0
	cost := 0.0
	covering := true
	for _, plan := range plans {
		selectivity = selectivity + plan.Selectivity - selectivity*plan.Selectivity
		cost += plan.Cost
		covering = covering && plan.CoverIndex
	}
	cost += float64(len(plans)) * 0.02 * 1000 // union/dedup work estimate
	return &IndexCandidate{
		Index:              &metadata.Index{Name: "INDEX_MERGE_OR"},
		IndexMergeBranches: plans,
		DeduplicateRows:    true,
		MergeMode:          IndexMergeUnion,
		CoverIndex:         covering,
		Cost:               cost,
		Selectivity:        selectivity,
		KeyLength:          len(plans),
		Reason:             "OR 条件索引合并（去重）",
	}, nil
}

// DeduplicateIndexMergeRows removes duplicate row identities produced by
// overlapping OR branches. A NULL key is represented explicitly and is not
// confused with an absent key.
func DeduplicateIndexMergeRows(rows []map[string]interface{}, keyColumn string) []map[string]interface{} {
	seen := make(map[string]struct{}, len(rows))
	result := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		value, ok := row[keyColumn]
		key := "<missing>"
		if ok {
			key = fmt.Sprintf("%T:%v", value, value)
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, row)
	}
	return result
}

// IntersectIndexMergeRows returns rows whose identity is present in every
// branch. The first branch determines output order and row payload, matching
// the stable assembly contract used by the executor.
func IntersectIndexMergeRows(branches [][]map[string]interface{}, keyColumn string) []map[string]interface{} {
	if len(branches) == 0 {
		return nil
	}
	counts := make(map[string]int)
	for _, branch := range branches {
		seen := make(map[string]struct{}, len(branch))
		for _, row := range branch {
			key := indexMergeRowKey(row, keyColumn)
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			counts[key]++
		}
	}
	result := make([]map[string]interface{}, 0)
	for _, row := range branches[0] {
		if counts[indexMergeRowKey(row, keyColumn)] == len(branches) {
			result = append(result, row)
			delete(counts, indexMergeRowKey(row, keyColumn))
		}
	}
	return result
}

func indexMergeRowKey(row map[string]interface{}, keyColumn string) string {
	value, ok := row[keyColumn]
	if !ok {
		return "<missing>"
	}
	return fmt.Sprintf("%T:%v", value, value)
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
		if e.Op == OpAnd {
			left, err := opt.extractIndexConditions(e.Left)
			if err != nil {
				return nil, err
			}
			right, err := opt.extractIndexConditions(e.Right)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, left...)
			conditions = append(conditions, right...)
			break
		}
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

	case *InExpression:
		if col, ok := e.Column.(*Column); ok && len(e.Values) > 0 {
			values := make([]Expression, 0, len(e.Values))
			for _, value := range e.Values {
				values = append(values, &Constant{Value: value})
			}
			conditions = append(conditions, &IndexCondition{
				Column: col.Name, Operator: "IN", Value: e.Values,
				CanPush: true, Selectivity: opt.estimateInSelectivity(col.Name, values),
			})
		}

	case *LikeExpression:
		if col, ok := e.Column.(*Column); ok {
			canPush := opt.canPushLikeCondition(e.Pattern)
			var escape interface{}
			if e.Escape != nil {
				if escapeConstant, escapeOK := e.Escape.(*Constant); escapeOK {
					escape = escapeConstant.Value
					canPush = opt.canPushLikeConditionWithEscape(e.Pattern, escape)
				} else {
					canPush = false
				}
			}
			conditions = append(conditions, &IndexCondition{
				Column: col.Name, Operator: "LIKE", Value: e.Pattern, Escape: escape,
				CanPush: canPush, Selectivity: opt.estimateLikeSelectivity(col.Name, e.Pattern),
			})
		}

	case *IsNullExpression:
		if col, ok := e.Column.(*Column); ok {
			operator := "is_not_null"
			if e.IsNull {
				operator = "is_null"
			}
			conditions = append(conditions, &IndexCondition{
				Column: col.Name, Operator: operator, Value: nil,
				CanPush: true, Selectivity: opt.estimateSelectivity(col.Name, operator, nil),
			})
		}

	case *BetweenExpression:
		if e.Not {
			break
		}
		if col, ok := e.Column.(*Column); ok {
			lower, lowerOK := indexConstantBound(e.LowerExpr, e.Lower)
			upper, upperOK := indexConstantBound(e.UpperExpr, e.Upper)
			if !lowerOK || !upperOK {
				break
			}
			conditions = append(conditions,
				&IndexCondition{Column: col.Name, Operator: ">=", Value: lower.Value, CanPush: true, Selectivity: opt.estimateSelectivity(col.Name, ">=", lower.Value)},
				&IndexCondition{Column: col.Name, Operator: "<=", Value: upper.Value, CanPush: true, Selectivity: opt.estimateSelectivity(col.Name, "<=", upper.Value)},
			)
		}
	}

	return conditions, nil
}

func indexConstantBound(expression Expression, value interface{}) (*Constant, bool) {
	if expression != nil {
		return indexConstantExpression(expression)
	}
	if value == nil {
		return nil, false
	}
	return &Constant{Value: value}, true
}

// extractBinaryCondition 提取二元条件
func (opt *IndexPushdownOptimizer) extractBinaryCondition(expr *BinaryOperation) (*IndexCondition, error) {
	var column *Column
	var constant *Constant
	op := expr.Op
	if leftCol, ok := expr.Left.(*Column); ok {
		if rightConst, ok := indexConstantExpression(expr.Right); ok {
			column, constant = leftCol, rightConst
		}
	} else if leftConst, ok := indexConstantExpression(expr.Left); ok {
		if rightCol, ok := expr.Right.(*Column); ok {
			if reversed, canReverse := reverseIndexComparison(op); canReverse {
				column, constant, op = rightCol, leftConst, reversed
			}
		}
	}
	if column == nil || constant == nil {
		return nil, nil
	}

	// 转换操作符
	operator := opt.convertOperator(op)
	if operator == "" {
		return nil, nil
	}
	if (operator == "IN" || operator == "NOT IN") && !isConstantList(constant.Value) {
		return nil, nil
	}

	// 计算选择性
	selectivity := opt.estimateSelectivity(column.Name, operator, constant.Value)
	canPush := opt.canPushCondition(operator)
	var escape interface{}
	if operator == "LIKE" || operator == "NOT LIKE" {
		if expr.Escape == nil {
			canPush = opt.canPushLikeCondition(constant.Value)
		} else if escapeConstant, escapeOK := expr.Escape.(*Constant); escapeOK {
			escape = escapeConstant.Value
			canPush = opt.canPushLikeConditionWithEscape(constant.Value, escape)
		} else {
			canPush = false
		}
	}

	return &IndexCondition{
		Column:      column.Name,
		Operator:    operator,
		Value:       constant.Value,
		Escape:      escape,
		CanPush:     canPush,
		Selectivity: selectivity,
	}, nil
}

// indexConstantExpression folds only side-effect-free expressions that do not
// reference a row column.  Runtime/session functions are excluded by the
// scalar-function whitelist; evaluation errors (including divide-by-zero) also
// reject the bound instead of manufacturing an index range.
func indexConstantExpression(expr Expression) (*Constant, bool) {
	if expr == nil {
		return nil, false
	}
	if constant, ok := expr.(*Constant); ok {
		return constant, true
	}

	switch value := expr.(type) {
	case *UnaryOperation:
		switch strings.TrimSpace(strings.ToLower(value.Operator)) {
		case "+", "-", "~":
		default:
			return nil, false
		}
		if _, ok := indexConstantExpression(value.Operand); !ok {
			return nil, false
		}
	case *BinaryOperation:
		switch value.Op {
		case OpAdd, OpSub, OpMul, OpDiv, OpIntDiv, OpMod,
			OpBitAnd, OpBitOr, OpBitXor, OpShiftLeft, OpShiftRight,
			OpEQ, OpNE, OpLT, OpLE, OpGT, OpGE, OpAnd, OpOr,
			OpLike, OpNotLike, OpIn, OpNotIn, OpNullSafeEQ, OpRegexp, OpNotRegexp:
		default:
			return nil, false
		}
		if _, ok := indexConstantExpression(value.Left); !ok {
			return nil, false
		}
		if _, ok := indexConstantExpression(value.Right); !ok {
			return nil, false
		}
		if value.Escape != nil {
			if _, ok := indexConstantExpression(value.Escape); !ok {
				return nil, false
			}
		}
	case *Function:
		if value.Distinct || !isDeterministicScalarFunction(value.FuncName) || len(value.FuncArgs) == 0 {
			return nil, false
		}
		for _, arg := range value.FuncArgs {
			if _, ok := indexConstantExpression(arg); !ok {
				return nil, false
			}
		}
	case *TupleExpression:
		if len(value.Exprs) == 0 {
			return nil, false
		}
		for _, item := range value.Exprs {
			if _, ok := indexConstantExpression(item); !ok {
				return nil, false
			}
		}
	case *CaseExpression:
		if len(value.Whens) == 0 {
			return nil, false
		}
		if value.Operand != nil {
			if _, ok := indexConstantExpression(value.Operand); !ok {
				return nil, false
			}
		}
		for _, when := range value.Whens {
			if _, ok := indexConstantExpression(when.Condition); !ok {
				return nil, false
			}
			if _, ok := indexConstantExpression(when.Value); !ok {
				return nil, false
			}
		}
		if value.Else != nil {
			if _, ok := indexConstantExpression(value.Else); !ok {
				return nil, false
			}
		}
	default:
		return nil, false
	}

	result, err := expr.Eval(&EvalContext{Row: map[string]interface{}{}})
	if err != nil {
		return nil, false
	}
	return &Constant{Value: result}, true
}

func reverseIndexComparison(op BinaryOp) (BinaryOp, bool) {
	switch op {
	case OpEQ, OpNE, OpNullSafeEQ:
		return op, true
	case OpLT:
		return OpGT, true
	case OpLE:
		return OpGE, true
	case OpGT:
		return OpLT, true
	case OpGE:
		return OpLE, true
	default:
		return op, false
	}
}

func isConstantList(value interface{}) bool {
	values, ok := value.([]interface{})
	return ok && len(values) > 0
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
	rangeColumnIndex := -1 // 已有范围条件的列下标，用于同列双范围（如 age>18 AND age<60）与禁止后续列范围

	for i, indexCol := range index.Columns {
		found := false
		for _, cond := range conditions {
			if !indexColumnMatchesCondition(indexCol, cond.Column) || !cond.CanPush {
				continue
			}
			// 已有范围条件时：仅禁止在「前一列」已有范围后再在本列加范围；同列可再加范围（双范围）
			if hasRangeCondition && opt.isRangeCondition(cond.Operator) && rangeColumnIndex >= 0 && rangeColumnIndex != i {
				continue
			}

			// 添加条件并设置其位置
			condCopy := *cond
			condCopy.IndexPosition = i
			condCopy.Priority = opt.calculateConditionPriority(&condCopy)
			candidate.Conditions = append(candidate.Conditions, &condCopy)
			totalSelectivity *= cond.Selectivity
			usedKeyLength = i + 1

			if opt.isRangeCondition(cond.Operator) {
				hasRangeCondition = true
				rangeColumnIndex = i
			}

			found = true
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

func indexColumnMatchesCondition(indexColumn, conditionColumn string) bool {
	_, bareConditionColumn := splitQualifiedColumnName(conditionColumn)
	return strings.EqualFold(strings.Trim(indexColumn, "` "), strings.Trim(bareConditionColumn, "` "))
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

			// Conditions passed to this method are conjunctive. This is an
			// index-intersection candidate, so selectivity is multiplicative;
			// OR candidates are built separately by optimizeDisjunction.
			mergedSel := c1.Selectivity * c2.Selectivity

			// 计算合并代价
			mergeCost := opt.calculateIntersectionMergeCost(c1, c2)
			totalCost := c1.Cost + c2.Cost + mergeCost

			mergedCandidate := &IndexCandidate{
				Index:              &metadata.Index{Name: "INDEX_MERGE_AND"},
				Conditions:         conds,
				IndexMergeBranches: []*IndexCandidate{c1, c2},
				MergeMode:          IndexMergeIntersection,
				CoverIndex:         c1.CoverIndex && c2.CoverIndex,
				Cost:               totalCost,
				Selectivity:        mergedSel,
				KeyLength:          c1.KeyLength + c2.KeyLength,
				Reason:             "索引合并",
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

func (opt *IndexPushdownOptimizer) calculateIntersectionMergeCost(c1, c2 *IndexCandidate) float64 {
	const (
		sortMergeCostPerRow = 0.05
		intersectionCost    = 0.02
	)
	resultSize := (c1.Selectivity * c2.Selectivity) * 1000
	return resultSize * (sortMergeCostPerRow + intersectionCost)
}

// isCoveringIndex 检查是否为覆盖索引，委托给包级 IsCoveringIndex；selectColumns 可为列名或表达式（如 COUNT(col)），由 extractColumnFromExpression 解析。
func (opt *IndexPushdownOptimizer) isCoveringIndex(index *metadata.Index, selectColumns []string) bool {
	if len(selectColumns) == 0 || index.Table == nil {
		return false
	}
	var requiredNames []string
	for _, col := range selectColumns {
		if col == "*" {
			return false
		}
		name := opt.extractColumnFromExpression(col)
		if name != "" {
			requiredNames = append(requiredNames, name)
		}
	}
	if len(requiredNames) == 0 {
		return false
	}
	return IsCoveringIndex(index.Table, index, requiredNames)
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
	bestScore := math.Inf(-1) // 允许负分（非覆盖索引代价高时 score 可能为负），保证有候选时必选其一

	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
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
	case OpNullSafeEQ:
		return "<=>"
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
	case OpLike:
		return "LIKE"
	case OpNotLike:
		return "NOT LIKE"
	case OpIn:
		return "IN"
	case OpNotIn:
		return "NOT IN"
	default:
		return ""
	}
}

// canPushCondition 检查条件是否可以下推
func (opt *IndexPushdownOptimizer) canPushCondition(operator string) bool {
	switch operator {
	case "=", "!=", "<=>", "<", "<=", ">", ">=", "IN", "NOT IN":
		return true
	case "is_null", "is_not_null":
		return true
	case "LIKE", "NOT LIKE":
		return true // 需要进一步检查模式
	default:
		return false
	}
}

// canPushLikeCondition 检查LIKE条件是否可以下推
func (opt *IndexPushdownOptimizer) canPushLikeCondition(pattern interface{}) bool {
	str, ok := pattern.(string)
	if !ok {
		return false
	}
	return isSimpleLikePrefixPatternWithEscape(str, '\\')
}

func (opt *IndexPushdownOptimizer) canPushLikeConditionWithEscape(pattern, escape interface{}) bool {
	patternString, patternOK := pattern.(string)
	escapeString, escapeOK := escape.(string)
	if !patternOK || !escapeOK {
		return false
	}
	escapeRunes := []rune(escapeString)
	if len(escapeRunes) != 1 {
		return false
	}
	return isSimpleLikePrefixPatternWithEscape(patternString, escapeRunes[0])
}

// isSimpleLikePrefixPattern accepts only a literal prefix followed by one or
// more unescaped '%' wildcards. A pattern such as "abc%def" must retain exact
// residual filtering: treating it as a prefix range would admit values that
// do not satisfy the suffix predicate and defeats the intended access-path
// contract. Escaped wildcards are literals, not prefix markers.
func isSimpleLikePrefixPattern(pattern string) bool {
	return isSimpleLikePrefixPatternWithEscape(pattern, '\\')
}

func isSimpleLikePrefixPatternWithEscape(pattern string, escapeCharacter rune) bool {
	sawWildcard := false
	literalLength := 0
	escaped := false
	for _, ch := range pattern {
		if escaped {
			if sawWildcard {
				return false
			}
			literalLength++
			escaped = false
			continue
		}
		if ch == escapeCharacter {
			escaped = true
			if sawWildcard {
				return false
			}
			continue
		}
		switch ch {
		case '_':
			return false
		case '%':
			sawWildcard = true
		default:
			if sawWildcard {
				return false
			}
			literalLength++
		}
	}
	return sawWildcard && literalLength > 0 && !escaped
}

// estimateSelectivity 估算选择性
func (opt *IndexPushdownOptimizer) estimateSelectivity(column, operator string, value interface{}) float64 {
	// 获取列统计信息
	colStats, exists := lookupCaseInsensitiveStats(opt.columnStats, column)
	if !exists {
		_, bareColumn := splitQualifiedColumnName(column)
		colStats, exists = lookupCaseInsensitiveStats(opt.columnStats, bareColumn)
	}
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
	case "=", "<=>":
		// 等值选择性 = 1 / NDV
		if operator == "<=>" && value == nil {
			total := colStats.NotNullCount + colStats.NullCount
			if total > 0 {
				return float64(colStats.NullCount) / float64(total)
			}
		}
		if colStats.DistinctCount > 0 {
			return 1.0 / float64(colStats.DistinctCount)
		}
		return 0.1
	case "is_null", "is_not_null":
		total := colStats.NotNullCount + colStats.NullCount
		if total > 0 {
			if operator == "is_null" {
				return float64(colStats.NullCount) / float64(total)
			}
			return float64(colStats.NotNullCount) / float64(total)
		}
		return 0.5

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

	minVal, minOk := toFloat64Safe(colStats.MinValue)
	val, valOk := toFloat64Safe(value)
	maxVal, maxOk := toFloat64Safe(colStats.MaxValue)
	if !minOk || !valOk || !maxOk || maxVal <= minVal {
		return 0.3
	}

	span := maxVal - minVal
	switch operator {
	case "<":
		// 比例 (value - min) / (max - min)，value 以下
		f := (val - minVal) / span
		if f <= 0 {
			return 0.01
		}
		if f >= 1 {
			return 1.0
		}
		return f
	case "<=":
		f := (val - minVal) / span
		if f < 0 {
			return 0.01
		}
		if f >= 1 {
			return 1.0
		}
		return f
	case ">":
		// 比例 (max - value) / (max - min)，value 以上
		f := (maxVal - val) / span
		if f <= 0 {
			return 0.01
		}
		if f >= 1 {
			return 1.0
		}
		return f
	case ">=":
		f := (maxVal - val) / span
		if f <= 0 {
			return 0.01
		}
		if f > 1 {
			return 1.0
		}
		return f
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
	indexStats, hasIndexStats := lookupCaseInsensitiveStats(opt.indexStats, index.Name)
	var tableStats *TableStats
	if index.Table != nil {
		tableStats, _ = lookupCaseInsensitiveStats(opt.tableStats, index.Table.Name)
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
	case "=", "<=>":
		return 100
	case "is_null", "is_not_null":
		return 90
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
