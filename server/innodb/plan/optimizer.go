package plan

import (
	"fmt"
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// LogicalOptimizationRule is one ordered, single-purpose rewrite pass.
// Keeping the rule boundary explicit makes ordering and conflicts testable
// instead of relying on the order of statements inside OptimizeLogicalPlan.
type LogicalOptimizationRule struct {
	Name  string
	Apply func(LogicalPlan) LogicalPlan
}

// LogicalOptimizationPipeline applies rules exactly once, in declaration
// order. Duplicate names and nil callbacks are rejected at construction time
// so a future rule cannot silently shadow or repeat another rule.
type LogicalOptimizationPipeline struct {
	rules []LogicalOptimizationRule
}

func NewLogicalOptimizationPipeline(rules []LogicalOptimizationRule) (*LogicalOptimizationPipeline, error) {
	seen := make(map[string]struct{}, len(rules))
	validated := make([]LogicalOptimizationRule, len(rules))
	for i, rule := range rules {
		name := strings.TrimSpace(rule.Name)
		if name == "" {
			return nil, fmt.Errorf("logical optimization rule %d has no name", i)
		}
		if rule.Apply == nil {
			return nil, fmt.Errorf("logical optimization rule %q has no callback", name)
		}
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("logical optimization rule %q is duplicated", name)
		}
		seen[name] = struct{}{}
		rule.Name = name
		validated[i] = rule
	}
	return &LogicalOptimizationPipeline{rules: validated}, nil
}

func (p *LogicalOptimizationPipeline) Apply(plan LogicalPlan) (LogicalPlan, []string, error) {
	if p == nil {
		return plan, nil, fmt.Errorf("logical optimization pipeline is nil")
	}
	trace := make([]string, 0, len(p.rules))
	for _, rule := range p.rules {
		if plan == nil {
			return nil, trace, fmt.Errorf("logical optimization rule %q received nil plan", rule.Name)
		}
		plan = rule.Apply(plan)
		if plan == nil {
			return nil, trace, fmt.Errorf("logical optimization rule %q returned nil plan", rule.Name)
		}
		trace = append(trace, rule.Name)
	}
	return plan, trace, nil
}

func defaultLogicalOptimizationPipeline() *LogicalOptimizationPipeline {
	pipeline, err := NewLogicalOptimizationPipeline([]LogicalOptimizationRule{
		{Name: "normalize-expressions", Apply: normalizeExpressions},
		{Name: "predicate-pushdown", Apply: pushDownPredicates},
		{Name: "column-pruning", Apply: func(plan LogicalPlan) LogicalPlan { return columnPruning(plan, nil) }},
		{Name: "aggregation-elimination", Apply: eliminateAggregation},
		{Name: "projection-minmax-simplification", Apply: simplifyProjMinMaxRoot},
		{Name: "subquery-optimization", Apply: optimizeSubquery},
		{Name: "index-access-optimization", Apply: func(plan LogicalPlan) LogicalPlan {
			return optimizeIndexAccess(plan, NewIndexPushdownOptimizer())
		}},
	})
	if err != nil {
		// The built-in list is static and should always validate. Keep the
		// optimizer entrypoint total even if a future edit violates that
		// invariant: an invalid optional rewrite pipeline must not crash a
		// server process while handling a query. The identity rule preserves
		// the original plan and lets callers continue safely.
		return &LogicalOptimizationPipeline{rules: []LogicalOptimizationRule{{
			Name: "identity-fallback",
			Apply: func(plan LogicalPlan) LogicalPlan {
				return plan
			},
		}}}
	}
	return pipeline
}

// OptimizeLogicalPlan 优化逻辑计划
func OptimizeLogicalPlan(plan LogicalPlan) LogicalPlan {
	optimized, _, err := defaultLogicalOptimizationPipeline().Apply(plan)
	if err != nil {
		return plan
	}
	return optimized
}

// OptimizeLogicalPlanWithTrace is the diagnostic form used by EXPLAIN and
// compatibility tests. The normal entrypoint remains source-compatible while
// callers that need evidence can assert the exact rule order.
func OptimizeLogicalPlanWithTrace(plan LogicalPlan) (LogicalPlan, []string, error) {
	return defaultLogicalOptimizationPipeline().Apply(plan)
}

// normalizeExpressions 规范化表达式（新增）
func normalizeExpressions(plan LogicalPlan) LogicalPlan {
	normalizer := NewExpressionNormalizer()

	switch v := plan.(type) {
	case *LogicalSubquery:
		if v.Subplan != nil {
			v.Subplan = normalizeExpressions(v.Subplan)
		}
		return v

	case *LogicalApply:
		for i, cond := range v.JoinConds {
			v.JoinConds[i] = normalizer.Normalize(cond)
		}
		for i, child := range v.Children() {
			children := v.Children()
			children[i] = normalizeExpressions(child)
			v.SetChildren(children)
		}
		return v

	case *LogicalSelection:
		// 规范化过滤条件
		for i, cond := range v.Conditions {
			v.Conditions[i] = normalizer.Normalize(cond)
		}
		// 递归处理子节点
		for i, child := range v.Children() {
			children := v.Children()
			children[i] = normalizeExpressions(child)
			v.SetChildren(children)
		}
		return v

	case *LogicalProjection:
		// 规范化投影表达式
		for i, expr := range v.Exprs {
			v.Exprs[i] = normalizer.Normalize(expr)
		}
		// 递归处理子节点
		for i, child := range v.Children() {
			children := v.Children()
			children[i] = normalizeExpressions(child)
			v.SetChildren(children)
		}
		return v

	case *LogicalJoin:
		// 规范化连接条件
		for i, cond := range v.Conditions {
			v.Conditions[i] = normalizer.Normalize(cond)
		}
		// 递归处理子节点
		for i, child := range v.Children() {
			children := v.Children()
			children[i] = normalizeExpressions(child)
			v.SetChildren(children)
		}
		return v

	case *LogicalAggregation:
		// 规范化分组表达式
		for i, expr := range v.GroupByItems {
			v.GroupByItems[i] = normalizer.Normalize(expr)
		}
		// 递归处理子节点
		for i, child := range v.Children() {
			children := v.Children()
			children[i] = normalizeExpressions(child)
			v.SetChildren(children)
		}
		return v

	case *LogicalValues:
		for i, expr := range v.Exprs {
			v.Exprs[i] = normalizer.Normalize(expr)
		}
		return v

	case *LogicalCTE:
		if v.Query != nil {
			v.Query = normalizeExpressions(v.Query)
		}
		for i, child := range v.Children() {
			children := v.Children()
			children[i] = normalizeExpressions(child)
			v.SetChildren(children)
		}
		return v

	case *LogicalRecursiveCTE:
		if v.Anchor != nil {
			v.Anchor = normalizeExpressions(v.Anchor)
		}
		if v.Recursive != nil {
			v.Recursive = normalizeExpressions(v.Recursive)
		}
		for i, child := range v.Children() {
			children := v.Children()
			children[i] = normalizeExpressions(child)
			v.SetChildren(children)
		}
		return v

	case *LogicalCTEStatement:
		for i, definition := range v.Definitions {
			v.Definitions[i] = normalizeExpressions(definition)
		}
		if v.Body != nil {
			v.Body = normalizeExpressions(v.Body)
		}
		for i, child := range v.Children() {
			children := v.Children()
			children[i] = normalizeExpressions(child)
			v.SetChildren(children)
		}
		return v

	default:
		// 递归处理子节点
		for i, child := range plan.Children() {
			children := plan.Children()
			children[i] = normalizeExpressions(child)
			plan.SetChildren(children)
		}
		return plan
	}
}

// pushDownPredicates 谓词下推优化
func pushDownPredicates(plan LogicalPlan) LogicalPlan {
	switch v := plan.(type) {
	case *LogicalSubquery:
		// LogicalSubquery keeps its inner plan in Subplan rather than in
		// Children().  The inner query still needs the same predicate-pushdown
		// rules as a top-level plan, while outer-query predicates remain outside
		// this boundary.
		if v.Subplan != nil {
			v.Subplan = pushDownPredicates(v.Subplan)
		}
		return v

	case *LogicalApply:
		children := v.Children()
		if len(children) != 2 {
			return v
		}

		// A correlated subquery often arrives as an Apply whose inner child
		// already has a constant filter. For INNER/SEMI/ANTI semantics, an
		// equality (including NULL-safe equality) between the two sides lets us
		// safely infer that filter on the other side. This is deliberately
		// limited to a direct LogicalSelection or a projection-only wrapper
		// without DISTINCT/ORDER/LIMIT barriers and constant-shaped predicates;
		// outer joins and cross-side expressions stay conservative.
		applyType := strings.ToUpper(strings.TrimSpace(v.ApplyType))
		if applyType == "" || applyType == "INNER" || applyType == "SEMI" || applyType == "ANTI" {
			leftInferred := inferApplyChildPredicates(v, children, 1, 0)
			rightInferred := inferApplyChildPredicates(v, children, 0, 1)
			leftChild := pushDownPredicates(children[0])
			rightChild := pushDownPredicates(children[1])
			if len(leftInferred) > 0 {
				leftChild = &LogicalSelection{
					BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{leftChild}},
					Conditions:      leftInferred,
				}
			}
			if len(rightInferred) > 0 {
				rightChild = &LogicalSelection{
					BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{rightChild}},
					Conditions:      rightInferred,
				}
			}
			v.SetChildren([]LogicalPlan{leftChild, rightChild})
			return v
		}

		v.SetChildren([]LogicalPlan{pushDownPredicates(children[0]), pushDownPredicates(children[1])})
		return v

	case *LogicalProjection:
		// 投影算子不能下推谓词
		child := pushDownPredicates(v.Children()[0])
		v.SetChildren([]LogicalPlan{child})
		return v

	case *LogicalCTE:
		if v.Query != nil {
			v.Query = pushDownPredicates(v.Query)
		}
		children := v.Children()
		if len(children) > 0 {
			newChildren := make([]LogicalPlan, len(children))
			for i, child := range children {
				newChildren[i] = pushDownPredicates(child)
			}
			v.SetChildren(newChildren)
		}
		return v

	case *LogicalRecursiveCTE:
		if v.Anchor != nil {
			v.Anchor = pushDownPredicates(v.Anchor)
		}
		if v.Recursive != nil {
			v.Recursive = pushDownPredicates(v.Recursive)
		}
		children := v.Children()
		if len(children) > 0 {
			newChildren := make([]LogicalPlan, len(children))
			for i, child := range children {
				newChildren[i] = pushDownPredicates(child)
			}
			v.SetChildren(newChildren)
		}
		return v

	case *LogicalCTEStatement:
		for i, definition := range v.Definitions {
			v.Definitions[i] = pushDownPredicates(definition)
		}
		if v.Body != nil {
			v.Body = pushDownPredicates(v.Body)
		}
		children := v.Children()
		if len(children) > 0 {
			newChildren := make([]LogicalPlan, len(children))
			for i, child := range children {
				newChildren[i] = pushDownPredicates(child)
			}
			v.SetChildren(newChildren)
		}
		return v

	case *LogicalSelection:
		// 创建CNF转换器
		cnfConverter := NewCNFConverter()

		// 将过滤条件转换为CNF形式
		normalizedConds := make([]Expression, len(v.Conditions))
		for i, cond := range v.Conditions {
			normalizedConds[i] = cnfConverter.ConvertToCNF(cond)
		}

		// 提取CNF中的合取子句
		var allConjuncts []Expression
		for _, cond := range normalizedConds {
			conjuncts := cnfConverter.ExtractConjuncts(cond)
			allConjuncts = append(allConjuncts, conjuncts...)
		}

		// 使用CNF子句进行谓词下推
		v.Conditions = allConjuncts

		// 尝试将选择条件下推到子节点
		child := v.Children()[0]
		switch childPlan := child.(type) {
		case *LogicalTableScan, *LogicalIndexScan:
			// 可以直接下推到表扫描
			return mergePredicate(childPlan, v.Conditions)

		case *LogicalProjection:
			// A projection made solely of direct columns is safe to push through
			// when its output names are a one-to-one mapping to the child
			// columns. This also covers the common derived-table shape
			// `SELECT id AS user_id ... WHERE user_id > ...`; computed
			// projections, ambiguous aliases, and projections with LIMIT or
			// DISTINCT stay above the projection.
			if rewritten, ok := rewritePredicatesThroughDirectProjection(childPlan, v.Conditions); ok && len(childPlan.Children()) == 1 {
				pushedChild := pushDownPredicates(&LogicalSelection{
					BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{childPlan.Children()[0]}},
					Conditions:      rewritten,
				})
				childPlan.SetChildren([]LogicalPlan{pushedChild})
				return childPlan
			}
			childPlan.SetChildren([]LogicalPlan{pushDownPredicates(childPlan.Children()[0])})
			return v

		case *LogicalJoin:
			// 判断连接类型，外连接需要特殊处理
			if !isSafeForPredicatePushdown(childPlan) {
				// 不安全下推，保持原有结构
				v.SetChildren([]LogicalPlan{pushDownPredicates(childPlan)})
				return v
			}

			// 将连接条件分解为左右表的过滤条件
			leftConds, rightConds, otherConds := splitJoinCondition(v.Conditions, childPlan)
			if strings.EqualFold(strings.TrimSpace(childPlan.JoinType), "INNER") || strings.TrimSpace(childPlan.JoinType) == "" {
				childPlan.Conditions, otherConds = moveInnerJoinEqualities(childPlan, otherConds)
				leftConds, rightConds = inferJoinConstantPredicates(childPlan, leftConds, rightConds)
			}
			// A predicate on the null-extended side of an outer join cannot be
			// removed from the post-join selection. Pushing it into that input
			// would make unmatched rows survive as NULL-extended rows.
			switch strings.ToUpper(strings.TrimSpace(childPlan.JoinType)) {
			case "LEFT":
				otherConds = append(otherConds, rightConds...)
				rightConds = nil
			case "RIGHT":
				otherConds = append(otherConds, leftConds...)
				leftConds = nil
			}
			leftConds, rightConds = inferOuterJoinPreservedPredicates(childPlan, leftConds, rightConds)

			// 递归下推左右表的过滤条件
			var newLeft, newRight LogicalPlan
			if len(leftConds) > 0 {
				newLeft = pushDownPredicates(&LogicalSelection{
					BaseLogicalPlan: BaseLogicalPlan{
						children: []LogicalPlan{childPlan.Children()[0]},
					},
					Conditions: leftConds,
				})
			} else {
				newLeft = pushDownPredicates(childPlan.Children()[0])
			}

			if len(rightConds) > 0 {
				newRight = pushDownPredicates(&LogicalSelection{
					BaseLogicalPlan: BaseLogicalPlan{
						children: []LogicalPlan{childPlan.Children()[1]},
					},
					Conditions: rightConds,
				})
			} else {
				newRight = pushDownPredicates(childPlan.Children()[1])
			}

			// 重建连接节点
			childPlan.SetChildren([]LogicalPlan{newLeft, newRight})

			if len(otherConds) > 0 {
				// 剩余条件保留在选择算子中
				v.Conditions = otherConds
				v.SetChildren([]LogicalPlan{childPlan})
				return v
			}
			return childPlan

		case *LogicalUnion:
			children := childPlan.Children()
			if len(children) == 0 {
				return v
			}
			for _, child := range children {
				if !canPushThroughUnionBranch(v.Conditions, child) {
					newChildren := make([]LogicalPlan, len(children))
					for i, branch := range children {
						newChildren[i] = pushDownPredicates(branch)
					}
					childPlan.SetChildren(newChildren)
					return v
				}
			}
			newChildren := make([]LogicalPlan, len(children))
			for i, child := range children {
				newChildren[i] = pushDownPredicates(&LogicalSelection{
					BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{child}},
					Conditions:      append([]Expression(nil), v.Conditions...),
				})
			}
			childPlan.SetChildren(newChildren)
			return childPlan

		case *LogicalApply:
			// Apply has JoinConds for correlation, while predicates above it may
			// still reference only one child.  Push only the sides whose Apply
			// semantics preserve the predicate: the left side is safe for every
			// supported Apply type, and the right side is safe for INNER Apply.
			// Keeping right-side predicates above LEFT/SEMI/ANTI avoids changing
			// null-extension or existence semantics.
			children := childPlan.Children()
			if len(children) != 2 {
				v.SetChildren([]LogicalPlan{pushDownPredicates(childPlan)})
				return v
			}
			joinShape := &LogicalJoin{BaseLogicalPlan: BaseLogicalPlan{children: children}}
			leftConds, rightConds, otherConds := splitJoinCondition(v.Conditions, joinShape)
			// Correlated Apply carries its equality predicates in JoinConds rather
			// than LogicalJoin.Conditions. For INNER/SEMI/ANTI Apply, a predicate
			// already restricted to one side can therefore be inferred safely on
			// the correlated side. Keep LEFT Apply conservative because its
			// nullable-side semantics are not equivalent to an inner filter.
			applyType := strings.ToUpper(strings.TrimSpace(childPlan.ApplyType))
			if applyType == "" || applyType == "INNER" || applyType == "SEMI" || applyType == "ANTI" {
				joinShape.JoinType = childPlan.ApplyType
				joinShape.Conditions = append([]Expression(nil), childPlan.JoinConds...)
				leftConds, rightConds = inferJoinConstantPredicates(joinShape, leftConds, rightConds)
			}
			switch strings.ToUpper(strings.TrimSpace(childPlan.ApplyType)) {
			case "LEFT", "SEMI", "ANTI":
				otherConds = append(otherConds, rightConds...)
				rightConds = nil
			}
			var newLeft, newRight LogicalPlan
			if len(leftConds) > 0 {
				newLeft = pushDownPredicates(&LogicalSelection{
					BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{children[0]}},
					Conditions:      leftConds,
				})
			} else {
				newLeft = pushDownPredicates(children[0])
			}
			if len(rightConds) > 0 {
				newRight = pushDownPredicates(&LogicalSelection{
					BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{children[1]}},
					Conditions:      rightConds,
				})
			} else {
				newRight = pushDownPredicates(children[1])
			}
			childPlan.SetChildren([]LogicalPlan{newLeft, newRight})
			if len(otherConds) > 0 {
				v.Conditions = otherConds
				v.SetChildren([]LogicalPlan{childPlan})
				return v
			}
			return childPlan

		case *LogicalAggregation:
			// 检查条件是否可以下推到聚合之前
			pushable, nonPushable := splitAggregatePredicate(v.Conditions, childPlan)

			var newChild LogicalPlan
			if len(pushable) > 0 {
				// 下推可下推的条件
				newChild = pushDownPredicates(&LogicalSelection{
					BaseLogicalPlan: BaseLogicalPlan{
						children: []LogicalPlan{childPlan.Children()[0]},
					},
					Conditions: pushable,
				})
			} else {
				newChild = pushDownPredicates(childPlan.Children()[0])
			}

			childPlan.SetChildren([]LogicalPlan{newChild})

			if len(nonPushable) > 0 {
				// 不可下推的条件保留在HAVING中
				v.Conditions = nonPushable
				v.SetChildren([]LogicalPlan{childPlan})
				return v
			}
			return childPlan
		}

	case *LogicalJoin:
		// 递归优化左右子树
		newLeft := pushDownPredicates(v.Children()[0])
		newRight := pushDownPredicates(v.Children()[1])
		v.SetChildren([]LogicalPlan{newLeft, newRight})
		return v

	case *LogicalAggregation:
		// 聚合前的过滤条件可以下推
		child := pushDownPredicates(v.Children()[0])
		v.SetChildren([]LogicalPlan{child})
		return v
	}

	return plan
}

func inferApplyChildPredicates(apply *LogicalApply, children []LogicalPlan, sourceIndex, targetIndex int) []Expression {
	if apply == nil || len(children) != 2 || sourceIndex == targetIndex || sourceIndex < 0 || sourceIndex > 1 || targetIndex < 0 || targetIndex > 1 {
		return nil
	}
	sourceConditions, sourceAliases, ok := applyChildSelectionConditions(children[sourceIndex])
	if !ok || len(sourceConditions) == 0 {
		return nil
	}
	joinShape := &LogicalJoin{BaseLogicalPlan: BaseLogicalPlan{children: children}}
	leftSet, rightSet := getJoinChildColumnSets(joinShape)
	inferred := make([]Expression, 0)
	for _, condition := range splitConjunctiveConditions(apply.JoinConds) {
		pairs, ok := joinEqualityColumnPairs(condition, leftSet, rightSet)
		if ok {
			for _, pair := range pairs {
				sourceColumn, targetColumn := pair.left, pair.right
				if sourceIndex == 1 {
					sourceColumn, targetColumn = pair.right, pair.left
				}
				sourceName := sourceColumn.Name
				if mapped, exists := sourceAliases[optimizerColumnKey(sourceName)]; exists {
					sourceName = mapped
				}
				for _, predicate := range transitivePredicatesForColumn(sourceConditions, sourceName) {
					inferred = appendJoinConditionIfMissing(inferred, replacePredicateColumn(predicate, targetColumn.Name))
				}
			}
			continue
		}

		expressionPairs, expressionOK := joinEqualityExpressionPairs(condition, leftSet, rightSet)
		if !expressionOK {
			continue
		}
		for _, pair := range expressionPairs {
			sourceExpression, targetExpression := pair.left, pair.right
			sourceSet, targetSet := leftSet, rightSet
			if sourceIndex == 1 {
				sourceExpression, targetExpression = pair.right, pair.left
				sourceSet, targetSet = rightSet, leftSet
			}
			for _, predicate := range predicatesForJoinExpression(sourceConditions, sourceExpression, sourceSet, targetSet) {
				if rewritten, changed := replaceExpressionInPredicate(predicate, sourceExpression, targetExpression); changed {
					inferred = appendJoinConditionIfMissing(inferred, rewritten)
				}
			}
		}
	}
	return inferred
}

func applyChildSelectionConditions(plan LogicalPlan) ([]Expression, map[string]string, bool) {
	switch node := plan.(type) {
	case *LogicalSelection:
		if len(node.Children()) != 1 {
			return node.Conditions, nil, true
		}
		childConditions, childAliases, childOK := applyChildSelectionConditions(node.Children()[0])
		if !childOK {
			return node.Conditions, nil, true
		}
		// Keep every selection in the wrapper chain. If the current
		// selection is above a direct-column projection, rewrite its visible
		// aliases back to the source columns before transitive inference.
		ownConditions := rewriteConditionsThroughAliases(node.Conditions, childAliases)
		conditions := make([]Expression, 0, len(ownConditions)+len(childConditions))
		conditions = append(conditions, ownConditions...)
		conditions = append(conditions, childConditions...)
		return conditions, childAliases, true
	case *LogicalSubquery:
		// Predicate inference through a subquery is safe for semi-join
		// semantics: IN/EXISTS only test whether a matching row exists. A
		// scalar subquery can change its value or NULL result when filtered,
		// so keep that shape outside this transitivity rule.
		subqueryType := strings.ToUpper(strings.TrimSpace(node.SubqueryType))
		if node.Subplan == nil || (subqueryType != "IN" && subqueryType != "EXISTS" && subqueryType != "SEMI" && subqueryType != "ANTI") {
			return nil, nil, false
		}
		return applyChildSelectionConditions(node.Subplan)
	case *LogicalProjection:
		if len(node.Children()) != 1 || node.Distinct || node.HasLimit || node.Offset != 0 || len(node.OrderBy) != 0 {
			return nil, nil, false
		}
		conditions, childAliases, ok := applyChildSelectionConditions(node.Children()[0])
		if !ok {
			return nil, nil, false
		}
		aliases := make(map[string]string, len(node.Exprs))
		for index, expression := range node.Exprs {
			column, ok := expression.(*Column)
			if !ok || strings.TrimSpace(column.Name) == "" {
				return nil, nil, false
			}
			outputName := column.Name
			if index < len(node.OutputNames) && strings.TrimSpace(node.OutputNames[index]) != "" {
				outputName = node.OutputNames[index]
			}
			sourceName := column.Name
			if mapped, exists := childAliases[optimizerColumnKey(sourceName)]; exists {
				sourceName = mapped
			}
			aliases[optimizerColumnKey(outputName)] = sourceName
		}
		return conditions, aliases, true
	default:
		return nil, nil, false
	}
}

func rewriteConditionsThroughAliases(conditions []Expression, aliases map[string]string) []Expression {
	if len(conditions) == 0 || len(aliases) == 0 {
		return conditions
	}
	rewritten := make([]Expression, len(conditions))
	copy(rewritten, conditions)
	for index, condition := range rewritten {
		for alias, source := range aliases {
			if updated, changed := replaceColumnInExpression(condition, alias, source); changed {
				condition = updated
			}
		}
		rewritten[index] = condition
	}
	return rewritten
}

// columnPruning 列裁剪：parentRequired 为上层算子仍需要的列名（已排序去重可调用 unionSortedCols）。
func columnPruning(plan LogicalPlan, parentRequired []string) LogicalPlan {
	if plan == nil {
		return nil
	}

	switch v := plan.(type) {
	case *LogicalProjection:
		if len(parentRequired) > 0 {
			pruneProjectionOutputs(v, parentRequired)
		}
		self := collectUsedColumns(v.Exprs)
		translated := parentRequired
		if mapped, ok := translateDirectProjectionRequirements(v, parentRequired); ok {
			translated = mapped
		}
		req := unionSortedCols(self, translated)
		child := columnPruning(v.Children()[0], req)
		v.SetChildren([]LogicalPlan{child})
		return v

	case *LogicalSelection:
		self := collectUsedColumns(v.Conditions)
		req := unionSortedCols(self, parentRequired)
		child := columnPruning(v.Children()[0], req)
		v.SetChildren([]LogicalPlan{child})
		return v

	case *LogicalJoin:
		condCols := collectUsedColumns(v.Conditions)
		all := unionSortedCols(parentRequired, condCols)
		leftReq, rightReq := splitColsForJoin(v, all)
		newLeft := columnPruning(v.Children()[0], leftReq)
		newRight := columnPruning(v.Children()[1], rightReq)
		v.SetChildren([]LogicalPlan{newLeft, newRight})
		return v

	case *LogicalAggregation:
		self := collectUsedColumns(append(v.GroupByItems, collectAggFuncCols(v.AggFuncs)...))
		req := unionSortedCols(self, parentRequired)
		child := columnPruning(v.Children()[0], req)
		v.SetChildren([]LogicalPlan{child})
		return v

	case *LogicalSubquery:
		// Subquery.Subplan is deliberately kept separate from Children() because
		// it is an expression-like plan node. It still participates in column
		// pruning, and correlated outer references must remain available at the
		// correlation boundary. When the subquery is also used as a logical child
		// (for example the right side of Apply), parentRequired describes the
		// subquery's visible output and must be retained alongside OuterRefs.
		if v.Subplan != nil {
			v.Subplan = columnPruning(v.Subplan, unionSortedCols(v.OuterRefs, parentRequired))
		}
		return v

	case *LogicalCTE:
		if v.Query != nil {
			v.Query = columnPruning(v.Query, parentRequired)
		}
		children := v.Children()
		if len(children) > 0 {
			newChildren := make([]LogicalPlan, len(children))
			for i, child := range children {
				newChildren[i] = columnPruning(child, parentRequired)
			}
			v.SetChildren(newChildren)
		}
		return v

	case *LogicalRecursiveCTE:
		if v.Anchor != nil {
			v.Anchor = columnPruning(v.Anchor, parentRequired)
		}
		if v.Recursive != nil {
			v.Recursive = columnPruning(v.Recursive, parentRequired)
		}
		children := v.Children()
		if len(children) > 0 {
			newChildren := make([]LogicalPlan, len(children))
			for i, child := range children {
				newChildren[i] = columnPruning(child, parentRequired)
			}
			v.SetChildren(newChildren)
		}
		return v

	case *LogicalCTEStatement:
		for i, definition := range v.Definitions {
			v.Definitions[i] = columnPruning(definition, parentRequired)
		}
		if v.Body != nil {
			v.Body = columnPruning(v.Body, parentRequired)
		}
		children := v.Children()
		if len(children) > 0 {
			newChildren := make([]LogicalPlan, len(children))
			for i, child := range children {
				newChildren[i] = columnPruning(child, parentRequired)
			}
			v.SetChildren(newChildren)
		}
		return v

	case *LogicalApply:
		children := v.Children()
		if len(children) == 2 {
			condCols := collectUsedColumns(v.JoinConds)
			all := unionSortedCols(parentRequired, condCols)
			// Apply has the same two-child column ownership rules as Join, but
			// carries its predicates in JoinConds instead of Conditions.
			joinShape := &LogicalJoin{BaseLogicalPlan: BaseLogicalPlan{children: children}}
			leftReq, rightReq := splitColsForJoin(joinShape, all)
			v.SetChildren([]LogicalPlan{
				columnPruning(children[0], leftReq),
				columnPruning(children[1], rightReq),
			})
			return v
		}
		return v

	case *LogicalTableScan:
		applyPrunedSchemaToTableScan(v, parentRequired)
		return v

	case *LogicalIndexScan:
		applyPrunedSchemaToIndexScan(v, parentRequired)
		return v

	case *LogicalCTEScan:
		applyPrunedSchemaToCTEScan(v, parentRequired)
		return v

	case *LogicalUnion:
		children := v.Children()
		if len(children) == 0 {
			return v
		}
		outputNames := getPlanOutputColumnNames(v)
		requiredPositions := make(map[int]struct{}, len(parentRequired))
		for position, name := range outputNames {
			for _, required := range parentRequired {
				if strings.EqualFold(strings.TrimSpace(name), physicalRequiredColumnName(required)) {
					requiredPositions[position] = struct{}{}
					break
				}
			}
		}
		positions := make([]int, 0, len(requiredPositions))
		for position := range requiredPositions {
			positions = append(positions, position)
		}
		sort.Ints(positions)
		newChildren := make([]LogicalPlan, len(children))
		for childIndex, child := range children {
			childNames := getPlanOutputColumnNames(child)
			childRequired := make([]string, 0, len(requiredPositions))
			for _, position := range positions {
				if position < len(childNames) {
					childRequired = append(childRequired, childNames[position])
				}
			}
			newChildren[childIndex] = columnPruning(child, childRequired)
		}
		v.SetChildren(newChildren)
		return v

	default:
		children := plan.Children()
		if len(children) == 0 {
			return plan
		}
		newCh := make([]LogicalPlan, len(children))
		for i, c := range children {
			newCh[i] = columnPruning(c, parentRequired)
		}
		plan.SetChildren(newCh)
		return plan
	}
}

// pruneProjectionOutputs removes projection expressions that are not visible
// to the parent row source.  It is deliberately disabled across DISTINCT,
// ORDER BY, LIMIT and OFFSET because those clauses can depend on the complete
// projection shape even when a parent only consumes one output column.
func pruneProjectionOutputs(projection *LogicalProjection, required []string) bool {
	if projection == nil || len(projection.Exprs) == 0 || len(required) == 0 || projection.Distinct || projection.HasLimit || projection.Offset != 0 || len(projection.OrderBy) != 0 {
		return false
	}
	if len(projection.OutputNames) != 0 && len(projection.OutputNames) != len(projection.Exprs) {
		return false
	}
	hasOutputNames := len(projection.OutputNames) == len(projection.Exprs) && len(projection.OutputNames) > 0

	outputIndexes := make(map[string]int, len(projection.Exprs))
	for index, expression := range projection.Exprs {
		if expression == nil {
			return false
		}
		name := strings.TrimSpace(expression.String())
		if index < len(projection.OutputNames) && strings.TrimSpace(projection.OutputNames[index]) != "" {
			name = strings.TrimSpace(projection.OutputNames[index])
		}
		key := optimizerColumnKey(name)
		if key == "" {
			return false
		}
		if _, duplicate := outputIndexes[key]; duplicate {
			return false
		}
		outputIndexes[key] = index
	}

	keep := make(map[int]struct{}, len(required))
	for _, name := range required {
		index, found := outputIndexes[optimizerColumnKey(name)]
		if !found {
			return false
		}
		keep[index] = struct{}{}
	}
	if len(keep) == len(projection.Exprs) {
		return false
	}

	exprs := make([]Expression, 0, len(keep))
	outputNames := make([]string, 0, len(keep))
	for index, expression := range projection.Exprs {
		if _, ok := keep[index]; !ok {
			continue
		}
		exprs = append(exprs, expression)
		if len(projection.OutputNames) == len(projection.Exprs) {
			outputNames = append(outputNames, projection.OutputNames[index])
		}
	}
	projection.Exprs = exprs
	if hasOutputNames {
		projection.OutputNames = outputNames
	} else {
		projection.OutputNames = nil
	}
	return true
}

func unionSortedCols(a, b []string) []string {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	set := make(map[string]struct{})
	for _, s := range a {
		set[s] = struct{}{}
	}
	for _, s := range b {
		set[s] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// translateDirectProjectionRequirements maps columns required from a
// projection's output back to the direct child columns that produce them.
// Unknown names are retained for conservative compatibility with plans whose
// output lineage is not explicit.
func translateDirectProjectionRequirements(projection *LogicalProjection, required []string) ([]string, bool) {
	if projection == nil || len(projection.Exprs) == 0 {
		return nil, false
	}
	aliases := make(map[string]string, len(projection.Exprs))
	for index, expression := range projection.Exprs {
		column, ok := expression.(*Column)
		if !ok || strings.TrimSpace(column.Name) == "" {
			return nil, false
		}
		outputName := column.Name
		if index < len(projection.OutputNames) && strings.TrimSpace(projection.OutputNames[index]) != "" {
			outputName = projection.OutputNames[index]
		}
		key := optimizerColumnKey(outputName)
		if key == "" {
			return nil, false
		}
		if _, duplicate := aliases[key]; duplicate {
			return nil, false
		}
		aliases[key] = column.Name
	}
	translated := make([]string, 0, len(required))
	for _, name := range required {
		if source, ok := aliases[optimizerColumnKey(name)]; ok {
			translated = append(translated, source)
		} else {
			translated = append(translated, name)
		}
	}
	return translated, true
}

// splitColsForJoin 将列名划分到 Join 左右子树；无法唯一归属时保守地同时下推（避免丢列）。
func splitColsForJoin(j *LogicalJoin, cols []string) (left []string, right []string) {
	if len(cols) == 0 {
		return nil, nil
	}
	leftSet, rightSet := getJoinChildColumnSets(j)
	var onlyLeft, onlyRight, both, unknown []string
	for _, c := range cols {
		inL := leftSet[c]
		inR := rightSet[c]
		switch {
		case inL && inR:
			both = append(both, c)
		case inL:
			onlyLeft = append(onlyLeft, c)
		case inR:
			onlyRight = append(onlyRight, c)
		default:
			unknown = append(unknown, c)
		}
	}
	// 双边列与未知列：两侧都保留，避免错误裁剪
	for _, c := range both {
		onlyLeft = append(onlyLeft, c)
		onlyRight = append(onlyRight, c)
	}
	for _, c := range unknown {
		onlyLeft = append(onlyLeft, c)
		onlyRight = append(onlyRight, c)
	}
	sort.Strings(onlyLeft)
	sort.Strings(onlyRight)
	return onlyLeft, onlyRight
}

func applyPrunedSchemaToTableScan(ts *LogicalTableScan, cols []string) {
	if ts == nil || ts.Table == nil {
		return
	}
	schema := ensureLogicalTableSchema(ts.Table)
	if len(cols) == 0 {
		ts.BaseLogicalPlan.schema = schema
		return
	}
	ts.BaseLogicalPlan.schema = buildPrunedSchema(schema, cols)
}

func applyPrunedSchemaToIndexScan(ix *LogicalIndexScan, cols []string) {
	if ix == nil || ix.Table == nil {
		return
	}
	schema := ensureLogicalTableSchema(ix.Table)
	if len(cols) == 0 {
		ix.BaseLogicalPlan.schema = schema
		return
	}
	ix.BaseLogicalPlan.schema = buildPrunedSchema(schema, cols)
}

func ensureLogicalTableSchema(table *metadata.Table) *metadata.DatabaseSchema {
	if table == nil {
		return nil
	}
	if table.Schema != nil {
		return table.Schema
	}
	schema := metadata.NewSchema("")
	if err := schema.AddTable(table); err != nil {
		return nil
	}
	return schema
}

// eliminateAggregation 聚合消除优化
func eliminateAggregation(plan LogicalPlan) LogicalPlan {
	switch v := plan.(type) {
	case *LogicalSubquery:
		if v.Subplan != nil {
			v.Subplan = eliminateAggregation(v.Subplan)
		}
		return v

	case *LogicalCTE:
		if v.Query != nil {
			v.Query = eliminateAggregation(v.Query)
		}
		children := v.Children()
		if len(children) > 0 {
			newChildren := make([]LogicalPlan, len(children))
			for i, child := range children {
				newChildren[i] = eliminateAggregation(child)
			}
			v.SetChildren(newChildren)
		}
		return v

	case *LogicalRecursiveCTE:
		if v.Anchor != nil {
			v.Anchor = eliminateAggregation(v.Anchor)
		}
		if v.Recursive != nil {
			v.Recursive = eliminateAggregation(v.Recursive)
		}
		children := v.Children()
		if len(children) > 0 {
			newChildren := make([]LogicalPlan, len(children))
			for i, child := range children {
				newChildren[i] = eliminateAggregation(child)
			}
			v.SetChildren(newChildren)
		}
		return v

	case *LogicalCTEStatement:
		for i, definition := range v.Definitions {
			v.Definitions[i] = eliminateAggregation(definition)
		}
		if v.Body != nil {
			v.Body = eliminateAggregation(v.Body)
		}
		children := v.Children()
		if len(children) > 0 {
			newChildren := make([]LogicalPlan, len(children))
			for i, child := range children {
				newChildren[i] = eliminateAggregation(child)
			}
			v.SetChildren(newChildren)
		}
		return v

	case *LogicalAggregation:
		child := v.Children()[0]

		// 检查是否可以消除聚合
		if canEliminateAggregation(v, child) {
			// 将聚合转换为投影
			return &LogicalProjection{
				BaseLogicalPlan: BaseLogicalPlan{
					children: []LogicalPlan{child},
				},
				Exprs: convertAggToProj(v),
			}
		}
	}

	// 递归优化子节点
	for i, child := range plan.Children() {
		newChild := eliminateAggregation(child)
		children := plan.Children()
		children[i] = newChild
		plan.SetChildren(children)
	}

	return plan
}

// optimizeSubquery 子查询优化
func optimizeSubquery(plan LogicalPlan) LogicalPlan {
	// 使用子查询优化器进行优化
	optimizer := NewSubqueryOptimizer()
	optimized := optimizer.Optimize(plan)

	// 打印优化统计信息（可选，用于调试）
	stats := optimizer.GetStats()
	if stats.TotalSubqueries > 0 {
		// 可以在这里记录日志或统计信息
		_ = stats // 避免未使用变量警告
	}

	return optimized
}

// optimizeIndexAccess 使用索引下推优化器选择索引
func optimizeIndexAccess(plan LogicalPlan, optimizer *IndexPushdownOptimizer) LogicalPlan {
	switch v := plan.(type) {
	case *LogicalSubquery:
		if v.Subplan != nil {
			v.Subplan = optimizeIndexAccess(v.Subplan, optimizer)
		}
		return v

	case *LogicalCTE:
		if v.Query != nil {
			v.Query = optimizeIndexAccess(v.Query, optimizer)
		}
		children := v.Children()
		if len(children) > 0 {
			newChildren := make([]LogicalPlan, len(children))
			for i, child := range children {
				newChildren[i] = optimizeIndexAccess(child, optimizer)
			}
			v.SetChildren(newChildren)
		}
		return v

	case *LogicalRecursiveCTE:
		if v.Anchor != nil {
			v.Anchor = optimizeIndexAccess(v.Anchor, optimizer)
		}
		if v.Recursive != nil {
			v.Recursive = optimizeIndexAccess(v.Recursive, optimizer)
		}
		children := v.Children()
		if len(children) > 0 {
			newChildren := make([]LogicalPlan, len(children))
			for i, child := range children {
				newChildren[i] = optimizeIndexAccess(child, optimizer)
			}
			v.SetChildren(newChildren)
		}
		return v

	case *LogicalCTEStatement:
		for i, definition := range v.Definitions {
			v.Definitions[i] = optimizeIndexAccess(definition, optimizer)
		}
		if v.Body != nil {
			v.Body = optimizeIndexAccess(v.Body, optimizer)
		}
		children := v.Children()
		if len(children) > 0 {
			newChildren := make([]LogicalPlan, len(children))
			for i, child := range children {
				newChildren[i] = optimizeIndexAccess(child, optimizer)
			}
			v.SetChildren(newChildren)
		}
		return v

	case *LogicalSelection:
		if len(v.Children()) == 0 {
			return v
		}
		child := v.Children()[0]
		if ts, ok := child.(*LogicalTableScan); ok {
			cand, err := optimizer.OptimizeIndexAccess(ts.Table, v.Conditions, []string{})
			if err == nil && cand != nil {
				newScan := &LogicalIndexScan{
					BaseLogicalPlan: BaseLogicalPlan{schema: ts.Schema()},
					Table:           ts.Table,
					Index: &Index{
						Name:    cand.Index.Name,
						Columns: cand.Index.Columns,
						Unique:  cand.Index.IsUnique,
					},
				}
				// Keep the selection above the access path.  Index selection is an
				// access-path choice, not proof that every original predicate was
				// evaluated by the storage layer; dropping it changes query results.
				v.SetChildren([]LogicalPlan{newScan})
				return v
			}
		}
	}

	for i, child := range plan.Children() {
		newChild := optimizeIndexAccess(child, optimizer)
		children := plan.Children()
		children[i] = newChild
		plan.SetChildren(children)
	}
	return plan
}

// 辅助函数

// mergePredicate merges predicate conditions into an existing selection node or
// creates a new one on top of the given plan. It is used by predicate push down
// to combine multiple filters.
func mergePredicate(plan LogicalPlan, conditions []Expression) LogicalPlan {
	if sel, ok := plan.(*LogicalSelection); ok {
		sel.Conditions = append(sel.Conditions, conditions...)
		return sel
	}

	return &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{plan}},
		Conditions:      conditions,
	}
}

func canPushThroughIdentityProjection(projection *LogicalProjection, conditions []Expression) bool {
	_, ok := rewritePredicatesThroughDirectProjection(projection, conditions)
	return ok
}

// rewritePredicatesThroughDirectProjection returns predicates rewritten from
// projection output aliases to their direct child columns. Only a plain
// projection is eligible: LIMIT and DISTINCT can change the rows selected by
// an outer predicate, while computed expressions and duplicate aliases do not
// have a single safe child-column interpretation.
func rewritePredicatesThroughDirectProjection(projection *LogicalProjection, conditions []Expression) ([]Expression, bool) {
	if projection == nil || len(projection.Exprs) == 0 || len(conditions) == 0 || projection.Distinct || projection.HasLimit || len(projection.OrderBy) > 0 {
		return nil, false
	}

	aliases := make(map[string]string, len(projection.Exprs))
	for index, expression := range projection.Exprs {
		column, ok := expression.(*Column)
		if !ok || strings.TrimSpace(column.Name) == "" {
			return nil, false
		}
		outputName := column.Name
		if index < len(projection.OutputNames) && strings.TrimSpace(projection.OutputNames[index]) != "" {
			outputName = projection.OutputNames[index]
		}
		key := optimizerColumnKey(outputName)
		if key == "" {
			return nil, false
		}
		if _, duplicate := aliases[key]; duplicate {
			return nil, false
		}
		aliases[key] = column.Name
	}

	rewritten := make([]Expression, len(conditions))
	for index, condition := range conditions {
		updated := condition
		for _, used := range collectUsedColumns([]Expression{condition}) {
			replacement, exists := aliases[optimizerColumnKey(used)]
			if !exists {
				return nil, false
			}
			var changed bool
			updated, changed = replaceColumnInExpression(updated, used, replacement)
			if !changed {
				return nil, false
			}
		}
		rewritten[index] = updated
	}
	return rewritten, true
}

func canPushThroughUnionBranch(conditions []Expression, branch LogicalPlan) bool {
	if branch == nil {
		return false
	}
	available := make(map[string]struct{})
	for _, name := range getPlanOutputColumnNames(branch) {
		available[optimizerColumnKey(name)] = struct{}{}
	}
	for _, used := range collectUsedColumns(conditions) {
		if _, exists := available[optimizerColumnKey(used)]; !exists {
			return false
		}
	}
	return true
}

func optimizerColumnKey(name string) string {
	_, column := splitQualifiedColumnName(name)
	return strings.ToLower(strings.TrimSpace(column))
}

func splitJoinCondition(conditions []Expression, join *LogicalJoin) ([]Expression, []Expression, []Expression) {
	var leftConds, rightConds, otherConds []Expression

	// 若 Join 未设置 LeftSchema/RightSchema，从左右子计划推导列集（谓词下推 OPT-001）
	leftColSet, rightColSet := getJoinChildColumnSets(join)

	for _, cond := range conditions {
		cols := collectUsedColumns([]Expression{cond})
		if len(cols) == 0 {
			otherConds = append(otherConds, cond)
			continue
		}

		allLeft := true
		allRight := true
		anyLeft := false
		anyRight := false
		for _, c := range cols {
			inLeft := leftColSet[c] || leftColSet[strings.ToLower(c)]
			inRight := rightColSet[c] || rightColSet[strings.ToLower(c)]
			if inLeft {
				anyLeft = true
			}
			if inRight {
				anyRight = true
			}
			if !inLeft {
				allLeft = false
			}
			if !inRight {
				allRight = false
			}
		}

		// 仅属于左表 -> 下推左子；仅属于右表 -> 下推右子；跨表或歧义 -> 保留在 Join 上
		switch {
		case allLeft && !anyRight:
			leftConds = append(leftConds, cond)
		case allRight && !anyLeft:
			rightConds = append(rightConds, cond)
		default:
			otherConds = append(otherConds, cond)
		}
	}

	return leftConds, rightConds, otherConds
}

func inferJoinConstantPredicates(join *LogicalJoin, leftConds, rightConds []Expression) ([]Expression, []Expression) {
	if join == nil {
		return leftConds, rightConds
	}
	leftSet, rightSet := getJoinChildColumnSets(join)
	for _, condition := range splitConjunctiveConditions(join.Conditions) {
		pairs, ok := joinEqualityColumnPairs(condition, leftSet, rightSet)
		if ok {
			for _, pair := range pairs {
				for _, predicate := range transitivePredicatesForColumn(leftConds, pair.left.Name) {
					rightConds = appendJoinConditionIfMissing(rightConds, replacePredicateColumn(predicate, pair.right.Name))
				}
				for _, predicate := range transitivePredicatesForColumn(rightConds, pair.right.Name) {
					leftConds = appendJoinConditionIfMissing(leftConds, replacePredicateColumn(predicate, pair.left.Name))
				}
			}
			continue
		}

		expressionPairs, expressionOK := joinEqualityExpressionPairs(condition, leftSet, rightSet)
		if !expressionOK {
			continue
		}
		for _, pair := range expressionPairs {
			for _, predicate := range predicatesForJoinExpression(leftConds, pair.left, leftSet, rightSet) {
				if rewritten, changed := replaceExpressionInPredicate(predicate, pair.left, pair.right); changed {
					rightConds = appendJoinConditionIfMissing(rightConds, rewritten)
				}
			}
			for _, predicate := range predicatesForJoinExpression(rightConds, pair.right, rightSet, leftSet) {
				if rewritten, changed := replaceExpressionInPredicate(predicate, pair.right, pair.left); changed {
					leftConds = appendJoinConditionIfMissing(leftConds, rewritten)
				}
			}
		}
	}
	return leftConds, rightConds
}

// inferOuterJoinPreservedPredicates copies predicates from the preserved side
// of an outer join to the nullable side when a join equality proves that the
// referenced columns have the same value. The original predicate remains on
// the preserved side, so unmatched rows keep their outer-join semantics.
func inferOuterJoinPreservedPredicates(join *LogicalJoin, leftConds, rightConds []Expression) ([]Expression, []Expression) {
	if join == nil {
		return leftConds, rightConds
	}
	joinType := strings.ToUpper(strings.TrimSpace(join.JoinType))
	if joinType != "LEFT" && joinType != "RIGHT" {
		return leftConds, rightConds
	}
	leftSet, rightSet := getJoinChildColumnSets(join)
	for _, condition := range splitConjunctiveConditions(join.Conditions) {
		pairs, ok := joinEqualityColumnPairs(condition, leftSet, rightSet)
		if ok {
			for _, pair := range pairs {
				if joinType == "LEFT" {
					for _, predicate := range transitivePredicatesForColumn(leftConds, pair.left.Name) {
						rightConds = appendJoinConditionIfMissing(rightConds, replacePredicateColumn(predicate, pair.right.Name))
					}
				} else {
					for _, predicate := range transitivePredicatesForColumn(rightConds, pair.right.Name) {
						leftConds = appendJoinConditionIfMissing(leftConds, replacePredicateColumn(predicate, pair.left.Name))
					}
				}
			}
			continue
		}

		expressionPairs, expressionOK := joinEqualityExpressionPairs(condition, leftSet, rightSet)
		if !expressionOK {
			continue
		}
		for _, pair := range expressionPairs {
			if joinType == "LEFT" {
				for _, predicate := range predicatesForJoinExpression(leftConds, pair.left, leftSet, rightSet) {
					if rewritten, changed := replaceExpressionInPredicate(predicate, pair.left, pair.right); changed {
						rightConds = appendJoinConditionIfMissing(rightConds, rewritten)
					}
				}
			} else {
				for _, predicate := range predicatesForJoinExpression(rightConds, pair.right, rightSet, leftSet) {
					if rewritten, changed := replaceExpressionInPredicate(predicate, pair.right, pair.left); changed {
						leftConds = appendJoinConditionIfMissing(leftConds, rewritten)
					}
				}
			}
		}
	}
	return leftConds, rightConds
}

func predicatesForJoinExpression(conditions []Expression, source Expression, ownSet, otherSet map[string]bool) []Expression {
	predicates := make([]Expression, 0, len(conditions))
	for _, condition := range conditions {
		columns := collectUsedColumns([]Expression{condition})
		if len(columns) == 0 {
			continue
		}
		allOnSourceSide := true
		for _, column := range columns {
			if !joinColumnBelongsExclusivelyToSide(column, ownSet, otherSet) {
				allOnSourceSide = false
				break
			}
		}
		if allOnSourceSide {
			if _, changed := replaceExpressionInPredicate(condition, source, source); changed {
				predicates = append(predicates, condition)
			}
		}
	}
	return predicates
}

// splitConjunctiveConditions exposes equality keys hidden inside a parser
// generated AND expression. Correlated Apply and JOIN builders may store a
// whole ON/JoinConds expression as one item; splitting only for key analysis
// is semantics-preserving because each conjunct must hold for the original
// join condition to be true.
func splitConjunctiveConditions(conditions []Expression) []Expression {
	result := make([]Expression, 0, len(conditions))
	var appendCondition func(Expression)
	appendCondition = func(condition Expression) {
		if binary, ok := condition.(*BinaryOperation); ok && binary.Op == OpAnd {
			appendCondition(binary.Left)
			appendCondition(binary.Right)
			return
		}
		result = append(result, condition)
	}
	for _, condition := range conditions {
		appendCondition(condition)
	}
	return result
}

type joinEqualityColumnPair struct {
	left  *Column
	right *Column
}

// joinEqualityColumnPairs recognizes single-column and tuple equi-join keys.
// Tuple equality is decomposed only when every item is a column and all items
// map consistently from the left child to the right child. This keeps
// transitive inference away from expressions whose NULL or comparison
// semantics cannot be proven from positional equality alone.
func joinEqualityColumnPairs(condition Expression, leftSet, rightSet map[string]bool) ([]joinEqualityColumnPair, bool) {
	comparison, ok := condition.(*BinaryOperation)
	if !ok || !isJoinEqualityOperator(comparison.Op) {
		return nil, false
	}
	if leftColumn, leftOK := comparison.Left.(*Column); leftOK {
		if rightColumn, rightOK := comparison.Right.(*Column); rightOK {
			if columnInJoinSet(leftSet, leftColumn.Name) && columnInJoinSet(rightSet, rightColumn.Name) {
				return []joinEqualityColumnPair{{left: leftColumn, right: rightColumn}}, true
			}
			if columnInJoinSet(leftSet, rightColumn.Name) && columnInJoinSet(rightSet, leftColumn.Name) {
				return []joinEqualityColumnPair{{left: rightColumn, right: leftColumn}}, true
			}
		}
	}

	leftTuple, leftOK := comparison.Left.(*TupleExpression)
	rightTuple, rightOK := comparison.Right.(*TupleExpression)
	if !leftOK || !rightOK || len(leftTuple.Exprs) == 0 || len(leftTuple.Exprs) != len(rightTuple.Exprs) {
		return nil, false
	}

	pairs := make([]joinEqualityColumnPair, len(leftTuple.Exprs))
	for index := range leftTuple.Exprs {
		leftColumn, leftColumnOK := leftTuple.Exprs[index].(*Column)
		rightColumn, rightColumnOK := rightTuple.Exprs[index].(*Column)
		if !leftColumnOK || !rightColumnOK {
			return nil, false
		}
		pairs[index] = joinEqualityColumnPair{left: leftColumn, right: rightColumn}
	}
	if tupleColumnsMatchJoinSides(pairs, leftSet, rightSet) {
		return pairs, true
	}
	for index := range pairs {
		pairs[index].left, pairs[index].right = pairs[index].right, pairs[index].left
	}
	if tupleColumnsMatchJoinSides(pairs, leftSet, rightSet) {
		return pairs, true
	}
	return nil, false
}

type joinEqualityExpressionPair struct {
	left  Expression
	right Expression
}

// joinEqualityExpressionPairs recognizes deterministic arithmetic expressions
// on opposite join sides.  Keeping this separate from
// joinEqualityColumnPairs is intentional: the latter is used by broader
// transitivity rules and must remain conservative for NULL-sensitive
// expression semantics.
func joinEqualityExpressionPairs(condition Expression, leftSet, rightSet map[string]bool) ([]joinEqualityExpressionPair, bool) {
	comparison, ok := condition.(*BinaryOperation)
	if !ok || !isJoinEqualityOperator(comparison.Op) {
		return nil, false
	}

	if leftTuple, leftOK := comparison.Left.(*TupleExpression); leftOK {
		rightTuple, rightOK := comparison.Right.(*TupleExpression)
		if !rightOK || len(leftTuple.Exprs) == 0 || len(leftTuple.Exprs) != len(rightTuple.Exprs) {
			return nil, false
		}
		pairs := make([]joinEqualityExpressionPair, 0, len(leftTuple.Exprs))
		for index := range leftTuple.Exprs {
			if pair, pairOK := joinEqualityExpressionPairForSides(leftTuple.Exprs[index], rightTuple.Exprs[index], leftSet, rightSet); pairOK {
				pairs = append(pairs, pair)
			}
		}
		if len(pairs) > 0 {
			return pairs, true
		}
		for index := range leftTuple.Exprs {
			if pair, pairOK := joinEqualityExpressionPairForSides(rightTuple.Exprs[index], leftTuple.Exprs[index], leftSet, rightSet); pairOK {
				pairs = append(pairs, pair)
			}
		}
		return pairs, len(pairs) > 0
	}

	if pair, pairOK := joinEqualityExpressionPairForSides(comparison.Left, comparison.Right, leftSet, rightSet); pairOK {
		return []joinEqualityExpressionPair{pair}, true
	}
	if pair, pairOK := joinEqualityExpressionPairForSides(comparison.Right, comparison.Left, leftSet, rightSet); pairOK {
		return []joinEqualityExpressionPair{pair}, true
	}
	return nil, false
}

func joinEqualityExpressionPairForSides(left, right Expression, leftSet, rightSet map[string]bool) (joinEqualityExpressionPair, bool) {
	if !joinExpressionBelongsExclusivelyToSide(left, leftSet, rightSet) || !joinExpressionBelongsExclusivelyToSide(right, rightSet, leftSet) {
		return joinEqualityExpressionPair{}, false
	}
	if len(collectUsedColumns([]Expression{left})) == 0 || len(collectUsedColumns([]Expression{right})) == 0 {
		return joinEqualityExpressionPair{}, false
	}
	return joinEqualityExpressionPair{left: left, right: right}, true
}

func joinExpressionBelongsExclusivelyToSide(expr Expression, ownSet, otherSet map[string]bool) bool {
	if expr == nil || !joinExpressionIsDeterministic(expr) {
		return false
	}
	columns := collectUsedColumns([]Expression{expr})
	if len(columns) == 0 {
		return true
	}
	for _, column := range columns {
		if !joinColumnBelongsExclusivelyToSide(column, ownSet, otherSet) {
			return false
		}
	}
	return true
}

func joinExpressionIsDeterministic(expr Expression) bool {
	switch value := expr.(type) {
	case *Column, *Constant:
		return true
	case *UnaryOperation:
		switch strings.TrimSpace(strings.ToLower(value.Operator)) {
		case "+", "-", "~":
			return joinExpressionIsDeterministic(value.Operand)
		default:
			return false
		}
	case *BinaryOperation:
		switch value.Op {
		case OpAdd, OpSub, OpMul, OpDiv, OpIntDiv, OpMod,
			OpBitAnd, OpBitOr, OpBitXor, OpShiftLeft, OpShiftRight:
			return joinExpressionIsDeterministic(value.Left) && joinExpressionIsDeterministic(value.Right)
		default:
			return false
		}
	case *Function:
		if value.Distinct || !isDeterministicScalarFunction(value.FuncName) || len(value.FuncArgs) == 0 {
			return false
		}
		for _, arg := range value.FuncArgs {
			if !joinExpressionIsDeterministic(arg) {
				return false
			}
		}
		return true
	case *TupleExpression:
		if len(value.Exprs) == 0 {
			return false
		}
		for _, item := range value.Exprs {
			if !joinExpressionIsDeterministic(item) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func isDeterministicScalarFunction(name string) bool {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "ABS", "CEIL", "CEILING", "FLOOR", "ROUND", "TRUNCATE",
		"LOWER", "LCASE", "UPPER", "UCASE", "LENGTH", "CHAR_LENGTH",
		"OCTET_LENGTH", "LEFT", "RIGHT", "REPLACE", "TRIM", "LTRIM", "RTRIM",
		"CONCAT", "CONCAT_WS", "IFNULL", "COALESCE", "NULLIF",
		"CAST", "CONVERT",
		"YEAR", "MONTH", "DAY", "DAYOFMONTH", "DAYOFYEAR", "WEEK", "YEARWEEK",
		"WEEKOFYEAR", "WEEKDAY", "DAYOFWEEK", "QUARTER", "HOUR", "MINUTE", "SECOND",
		"MICROSECOND", "DATE", "TIME", "DAYNAME", "MONTHNAME", "DATE_FORMAT", "TIME_FORMAT",
		"STR_TO_DATE",
		"MD5", "SHA", "SHA1", "SHA2", "CRC32", "INET_ATON", "INET_NTOA", "INET6_ATON",
		"INET6_NTOA", "IS_IPV4", "IS_IPV6", "IS_IPV4_COMPAT", "IS_IPV4_MAPPED", "IS_UUID",
		"UUID_TO_BIN", "BIN_TO_UUID", "TO_BASE64", "FROM_BASE64", "HEX", "UNHEX", "BIN", "OCT":
		return true
	default:
		return false
	}
}

func joinColumnBelongsExclusivelyToSide(name string, ownSet, otherSet map[string]bool) bool {
	name = strings.Trim(strings.TrimSpace(name), "`")
	ownExact := ownSet[name] || ownSet[strings.ToLower(name)]
	otherExact := otherSet[name] || otherSet[strings.ToLower(name)]
	if strings.Contains(name, ".") && (ownExact || otherExact) {
		return ownExact && !otherExact
	}
	if ownExact || otherExact {
		return ownExact && !otherExact
	}
	if separator := strings.LastIndex(name, "."); separator >= 0 && separator+1 < len(name) {
		visibleName := name[separator+1:]
		ownVisible := ownSet[visibleName] || ownSet[strings.ToLower(visibleName)]
		otherVisible := otherSet[visibleName] || otherSet[strings.ToLower(visibleName)]
		return ownVisible && !otherVisible
	}
	return false
}

func isJoinEqualityOperator(op BinaryOp) bool {
	return op == OpEQ || op == OpNullSafeEQ
}

func tupleColumnsMatchJoinSides(pairs []joinEqualityColumnPair, leftSet, rightSet map[string]bool) bool {
	for _, pair := range pairs {
		if !columnInJoinSet(leftSet, pair.left.Name) || !columnInJoinSet(rightSet, pair.right.Name) {
			return false
		}
	}
	return true
}

func moveInnerJoinEqualities(join *LogicalJoin, conditions []Expression) ([]Expression, []Expression) {
	if join == nil {
		return nil, conditions
	}
	leftSet, rightSet := getJoinChildColumnSets(join)
	joinConditions := append([]Expression(nil), join.Conditions...)
	remaining := make([]Expression, 0, len(conditions))
	for _, condition := range splitConjunctiveConditions(conditions) {
		if _, ok := joinEqualityColumnPairs(condition, leftSet, rightSet); !ok {
			remaining = append(remaining, condition)
			continue
		}
		joinConditions = appendJoinConditionIfMissing(joinConditions, condition)
	}
	return joinConditions, remaining
}

func columnInJoinSet(columns map[string]bool, name string) bool {
	if columns[name] || columns[strings.ToLower(name)] {
		return true
	}
	// Derived/CTE output contracts commonly expose an unqualified alias,
	// while a surrounding join condition still carries the source qualifier.
	// Match that qualifier to the visible output suffix without reintroducing
	// hidden physical child columns into the ownership set.
	if separator := strings.LastIndex(name, "."); separator >= 0 && separator+1 < len(name) {
		visibleName := name[separator+1:]
		return columns[visibleName] || columns[strings.ToLower(visibleName)]
	}
	return false
}

func equalityConstantForColumn(conditions []Expression, columnName string) (*Constant, bool) {
	for _, condition := range conditions {
		comparison, ok := condition.(*BinaryOperation)
		if !ok || comparison.Op != OpEQ {
			continue
		}
		column, columnOK := comparison.Left.(*Column)
		constant, constantOK := comparison.Right.(*Constant)
		if columnOK && constantOK && strings.EqualFold(column.Name, columnName) {
			return constant, true
		}
	}
	return nil, false
}

// constantPredicateForColumn returns a comparison between a column and a
// constant, normalizing the less common constant-on-the-left form. Inner-join
// equality makes the predicate safe to infer on the other join side for the
// ordinary comparison operators, while outer joins are excluded by the caller.
func constantPredicateForColumn(conditions []Expression, columnName string) (*BinaryOperation, bool) {
	for _, condition := range conditions {
		comparison, ok := condition.(*BinaryOperation)
		if !ok || (!isTransitiveComparisonOp(comparison.Op) && !isTransitivePatternOp(comparison.Op)) {
			continue
		}
		if _, constantOK := comparison.Right.(*Constant); constantOK {
			if candidate, replaced := replaceSingleColumnExpression(comparison.Left, columnName, columnName); replaced {
				return &BinaryOperation{Op: comparison.Op, Operator: comparison.Operator, Left: candidate, Right: comparison.Right, Escape: comparison.Escape}, true
			}
		}
		if !isTransitiveComparisonOp(comparison.Op) {
			continue
		}
		if constant, constantOK := comparison.Left.(*Constant); constantOK {
			if candidate, replaced := replaceSingleColumnExpression(comparison.Right, columnName, columnName); replaced {
				return &BinaryOperation{
					Op:       reverseTransitiveComparisonOp(comparison.Op),
					Operator: comparison.Operator,
					Left:     candidate,
					Right:    constant,
					Escape:   comparison.Escape,
				}, true
			}
		}
	}
	return nil, false
}

// replaceSingleColumnExpression clones an expression when it references only
// one occurrence of the requested column. This lets equality transitivity
// safely carry deterministic wrappers such as (a.id + 1) > 7 across a join
// equality, without guessing how to rewrite expressions that also reference
// unrelated columns.
func replaceSingleColumnExpression(expr Expression, columnName, replacementName string) (Expression, bool) {
	if expr == nil {
		return nil, false
	}
	columns := collectUsedColumns([]Expression{expr})
	if len(columns) != 1 || !strings.EqualFold(columns[0], columnName) {
		return nil, false
	}
	return replaceColumnInExpression(expr, columnName, replacementName)
}

// replaceExpressionInPredicate rewrites an exact deterministic join-key
// expression inside a predicate.  Matching the complete subtree is important:
// replacing individual columns would also rewrite expressions that are not
// proven equivalent by the join condition.
func replaceExpressionInPredicate(expr, source, replacement Expression) (Expression, bool) {
	if expr == nil || source == nil {
		return expr, false
	}
	if expressionsEquivalent(expr, source) {
		return replacement, true
	}

	switch value := expr.(type) {
	case *BinaryOperation:
		left, leftChanged := replaceExpressionInPredicate(value.Left, source, replacement)
		right, rightChanged := replaceExpressionInPredicate(value.Right, source, replacement)
		escape, escapeChanged := replaceExpressionInPredicate(value.Escape, source, replacement)
		if !leftChanged && !rightChanged && !escapeChanged {
			return expr, false
		}
		clone := *value
		clone.Left, clone.Right, clone.Escape = left, right, escape
		return &clone, true
	case *UnaryOperation:
		operand, changed := replaceExpressionInPredicate(value.Operand, source, replacement)
		if !changed {
			return expr, false
		}
		clone := *value
		clone.Operand = operand
		return &clone, true
	case *Function:
		clone := *value
		clone.FuncArgs = append([]Expression(nil), value.FuncArgs...)
		changedAny := false
		for index, arg := range clone.FuncArgs {
			updated, changed := replaceExpressionInPredicate(arg, source, replacement)
			if changed {
				clone.FuncArgs[index] = updated
				changedAny = true
			}
		}
		if !changedAny {
			return expr, false
		}
		return &clone, true
	case *TupleExpression:
		clone := *value
		clone.Exprs = append([]Expression(nil), value.Exprs...)
		changedAny := false
		for index, item := range clone.Exprs {
			updated, changed := replaceExpressionInPredicate(item, source, replacement)
			if changed {
				clone.Exprs[index] = updated
				changedAny = true
			}
		}
		if !changedAny {
			return expr, false
		}
		return &clone, true
	case *CaseExpression:
		clone := *value
		clone.Whens = append([]CaseWhen(nil), value.Whens...)
		changedAny := false
		if value.Operand != nil {
			if updated, changed := replaceExpressionInPredicate(value.Operand, source, replacement); changed {
				clone.Operand, changedAny = updated, true
			}
		}
		for index, when := range clone.Whens {
			if updated, changed := replaceExpressionInPredicate(when.Condition, source, replacement); changed {
				clone.Whens[index].Condition, changedAny = updated, true
			}
			if updated, changed := replaceExpressionInPredicate(when.Value, source, replacement); changed {
				clone.Whens[index].Value, changedAny = updated, true
			}
		}
		if value.Else != nil {
			if updated, changed := replaceExpressionInPredicate(value.Else, source, replacement); changed {
				clone.Else, changedAny = updated, true
			}
		}
		if !changedAny {
			return expr, false
		}
		return &clone, true
	case *NotExpression:
		operand, changed := replaceExpressionInPredicate(value.Operand, source, replacement)
		if !changed {
			return expr, false
		}
		clone := *value
		clone.Operand = operand
		return &clone, true
	case *InExpression:
		column, changed := replaceExpressionInPredicate(value.Column, source, replacement)
		if !changed {
			return expr, false
		}
		clone := *value
		clone.Column = column
		clone.Values = append([]interface{}(nil), value.Values...)
		return &clone, true
	case *LikeExpression:
		column, columnChanged := replaceExpressionInPredicate(value.Column, source, replacement)
		escape, escapeChanged := replaceExpressionInPredicate(value.Escape, source, replacement)
		if !columnChanged && !escapeChanged {
			return expr, false
		}
		clone := *value
		clone.Column, clone.Escape = column, escape
		return &clone, true
	case *IsNullExpression:
		column, changed := replaceExpressionInPredicate(value.Column, source, replacement)
		if !changed {
			return expr, false
		}
		clone := *value
		clone.Column = column
		return &clone, true
	case *IsTruthExpression:
		inner, changed := replaceExpressionInPredicate(value.Expr, source, replacement)
		if !changed {
			return expr, false
		}
		clone := *value
		clone.Expr = inner
		return &clone, true
	case *BetweenExpression:
		clone := *value
		changedAny := false
		if updated, changed := replaceExpressionInPredicate(value.Column, source, replacement); changed {
			clone.Column, changedAny = updated, true
		}
		if updated, changed := replaceExpressionInPredicate(value.LowerExpr, source, replacement); changed {
			clone.LowerExpr, changedAny = updated, true
		}
		if updated, changed := replaceExpressionInPredicate(value.UpperExpr, source, replacement); changed {
			clone.UpperExpr, changedAny = updated, true
		}
		if !changedAny {
			return expr, false
		}
		return &clone, true
	default:
		return expr, false
	}
}

func replaceColumnInExpression(expr Expression, columnName, replacementName string) (Expression, bool) {
	switch value := expr.(type) {
	case *Column:
		if !strings.EqualFold(value.Name, columnName) {
			return expr, false
		}
		clone := *value
		clone.Name = replacementName
		return &clone, true
	case *BinaryOperation:
		left, leftReplaced := replaceColumnInExpression(value.Left, columnName, replacementName)
		right, rightReplaced := replaceColumnInExpression(value.Right, columnName, replacementName)
		escape, escapeReplaced := replaceColumnInExpression(value.Escape, columnName, replacementName)
		if !leftReplaced && !rightReplaced && !escapeReplaced {
			return expr, false
		}
		clone := *value
		clone.Left, clone.Right = left, right
		clone.Escape = escape
		return &clone, true
	case *UnaryOperation:
		operand, replaced := replaceColumnInExpression(value.Operand, columnName, replacementName)
		if !replaced {
			return expr, false
		}
		clone := *value
		clone.Operand = operand
		return &clone, true
	case *Function:
		clone := *value
		clone.FuncArgs = append([]Expression(nil), value.FuncArgs...)
		replaced := false
		for index, arg := range clone.FuncArgs {
			updated, changed := replaceColumnInExpression(arg, columnName, replacementName)
			if changed {
				clone.FuncArgs[index] = updated
				replaced = true
			}
		}
		if !replaced {
			return expr, false
		}
		return &clone, true
	case *TupleExpression:
		clone := *value
		clone.Exprs = append([]Expression(nil), value.Exprs...)
		replaced := false
		for index, item := range clone.Exprs {
			updated, changed := replaceColumnInExpression(item, columnName, replacementName)
			if changed {
				clone.Exprs[index] = updated
				replaced = true
			}
		}
		if !replaced {
			return expr, false
		}
		return &clone, true
	case *CaseExpression:
		clone := *value
		clone.Whens = append([]CaseWhen(nil), value.Whens...)
		replaced := false
		if clone.Operand != nil {
			updated, changed := replaceColumnInExpression(clone.Operand, columnName, replacementName)
			if changed {
				clone.Operand, replaced = updated, true
			}
		}
		for index, when := range clone.Whens {
			if updated, changed := replaceColumnInExpression(when.Condition, columnName, replacementName); changed {
				clone.Whens[index].Condition, replaced = updated, true
			}
			if updated, changed := replaceColumnInExpression(when.Value, columnName, replacementName); changed {
				clone.Whens[index].Value, replaced = updated, true
			}
		}
		if clone.Else != nil {
			if updated, changed := replaceColumnInExpression(clone.Else, columnName, replacementName); changed {
				clone.Else, replaced = updated, true
			}
		}
		if !replaced {
			return expr, false
		}
		return &clone, true
	case *NotExpression:
		operand, replaced := replaceColumnInExpression(value.Operand, columnName, replacementName)
		if !replaced {
			return expr, false
		}
		clone := *value
		clone.Operand = operand
		return &clone, true
	case *InExpression:
		column, replaced := replaceColumnInExpression(value.Column, columnName, replacementName)
		if !replaced {
			return expr, false
		}
		clone := *value
		clone.Column = column
		clone.Values = append([]interface{}(nil), value.Values...)
		return &clone, true
	case *LikeExpression:
		column, replaced := replaceColumnInExpression(value.Column, columnName, replacementName)
		escape, escapeReplaced := replaceColumnInExpression(value.Escape, columnName, replacementName)
		if !replaced && !escapeReplaced {
			return expr, false
		}
		clone := *value
		clone.Column = column
		clone.Escape = escape
		return &clone, true
	case *IsNullExpression:
		column, replaced := replaceColumnInExpression(value.Column, columnName, replacementName)
		if !replaced {
			return expr, false
		}
		clone := *value
		clone.Column = column
		return &clone, true
	case *IsTruthExpression:
		inner, replaced := replaceColumnInExpression(value.Expr, columnName, replacementName)
		if !replaced {
			return expr, false
		}
		clone := *value
		clone.Expr = inner
		return &clone, true
	case *BetweenExpression:
		clone := *value
		replaced := false
		if updated, changed := replaceColumnInExpression(value.Column, columnName, replacementName); changed {
			clone.Column, replaced = updated, true
		}
		if updated, changed := replaceColumnInExpression(value.LowerExpr, columnName, replacementName); changed {
			clone.LowerExpr, replaced = updated, true
		}
		if updated, changed := replaceColumnInExpression(value.UpperExpr, columnName, replacementName); changed {
			clone.UpperExpr, replaced = updated, true
		}
		if !replaced {
			return expr, false
		}
		return &clone, true
	default:
		return expr, false
	}
}

func transitivePredicateForColumn(conditions []Expression, columnName string) (Expression, bool) {
	predicates := transitivePredicatesForColumn(conditions, columnName)
	if len(predicates) != 0 {
		return predicates[0], true
	}
	return nil, false
}

// transitivePredicatesForColumn returns every predicate that can be safely
// rewritten through an equality join key. Keeping all predicates matters for
// range intersections such as column > 7 AND column < 10; carrying only the
// first predicate leaves the nullable side with a weaker access filter.
func transitivePredicatesForColumn(conditions []Expression, columnName string) []Expression {
	predicates := make([]Expression, 0, len(conditions))
	for _, condition := range conditions {
		if predicate, found := constantPredicateForColumn([]Expression{condition}, columnName); found {
			predicates = append(predicates, predicate)
			continue
		}
		if predicate, found := nullPredicateForColumn([]Expression{condition}, columnName); found {
			predicates = append(predicates, predicate)
			continue
		}
		if predicate, found := inPredicateForColumn([]Expression{condition}, columnName); found {
			predicates = append(predicates, predicate)
			continue
		}
		if predicate, found := likePredicateForColumn([]Expression{condition}, columnName); found {
			predicates = append(predicates, predicate)
			continue
		}
		if predicate, found := betweenPredicateForColumn([]Expression{condition}, columnName); found {
			predicates = append(predicates, predicate)
			continue
		}
		switch condition.(type) {
		case *NotExpression, *IsTruthExpression:
			if _, replaced := replaceSingleColumnExpression(condition, columnName, columnName); replaced {
				predicates = append(predicates, condition)
			}
		}
		if isSingleColumnBooleanCombination(condition, columnName) {
			predicates = append(predicates, condition)
		}
	}
	return predicates
}

// isSingleColumnBooleanCombination recognizes a boolean OR tree whose leaves
// all depend on the same column. Under an equality join, replacing that one
// column with the other join key is safe for both inner joins and the
// preserved-side inference used by outer joins. Cross-column expressions and
// non-boolean binary arithmetic are deliberately excluded.
func isSingleColumnBooleanCombination(expression Expression, columnName string) bool {
	binary, ok := expression.(*BinaryOperation)
	if !ok || binary.Op != OpOr {
		return false
	}
	columns := collectUsedColumns([]Expression{expression})
	if len(columns) != 1 || !strings.EqualFold(columns[0], columnName) {
		return false
	}
	var valid func(Expression) bool
	valid = func(candidate Expression) bool {
		if nested, ok := candidate.(*BinaryOperation); ok && (nested.Op == OpOr || nested.Op == OpAnd) {
			return valid(nested.Left) && valid(nested.Right)
		}
		if _, ok := constantPredicateForColumn([]Expression{candidate}, columnName); ok {
			return true
		}
		if _, ok := nullPredicateForColumn([]Expression{candidate}, columnName); ok {
			return true
		}
		if _, ok := inPredicateForColumn([]Expression{candidate}, columnName); ok {
			return true
		}
		if _, ok := likePredicateForColumn([]Expression{candidate}, columnName); ok {
			return true
		}
		if _, ok := betweenPredicateForColumn([]Expression{candidate}, columnName); ok {
			return true
		}
		return false
	}
	return valid(binary.Left) && valid(binary.Right)
}

func nullPredicateForColumn(conditions []Expression, columnName string) (*IsNullExpression, bool) {
	for _, condition := range conditions {
		predicate, ok := condition.(*IsNullExpression)
		if !ok {
			continue
		}
		if _, ok := replacePredicateExpressionColumn(predicate.Column, columnName); ok {
			return predicate, true
		}
	}
	return nil, false
}

func inPredicateForColumn(conditions []Expression, columnName string) (*InExpression, bool) {
	for _, condition := range conditions {
		predicate, ok := condition.(*InExpression)
		if !ok {
			continue
		}
		if _, ok := replacePredicateExpressionColumn(predicate.Column, columnName); ok {
			return predicate, true
		}
	}
	return nil, false
}

func likePredicateForColumn(conditions []Expression, columnName string) (*LikeExpression, bool) {
	for _, condition := range conditions {
		predicate, ok := condition.(*LikeExpression)
		if !ok {
			continue
		}
		if _, ok := replacePredicateExpressionColumn(predicate.Column, columnName); ok {
			return predicate, true
		}
	}
	return nil, false
}

func betweenPredicateForColumn(conditions []Expression, columnName string) (*BetweenExpression, bool) {
	for _, condition := range conditions {
		predicate, ok := condition.(*BetweenExpression)
		if !ok {
			continue
		}
		if _, ok := replacePredicateExpressionColumn(predicate.Column, columnName); !ok {
			continue
		}
		if len(collectUsedColumns([]Expression{predicate.LowerExpr, predicate.UpperExpr})) != 0 {
			continue
		}
		return predicate, true
	}
	return nil, false
}

func replacePredicateColumn(predicate Expression, columnName string) Expression {
	switch p := predicate.(type) {
	case *BinaryOperation:
		if (p.Op == OpAnd || p.Op == OpOr) && len(collectUsedColumns([]Expression{p})) == 1 {
			columns := collectUsedColumns([]Expression{p})
			if replaced, changed := replaceColumnInExpression(p, columns[0], columnName); changed {
				return replaced
			}
		}
		if columns := collectUsedColumns([]Expression{p.Left}); len(columns) == 1 {
			if left, replaced := replaceColumnInExpression(p.Left, columns[0], columnName); replaced {
				return &BinaryOperation{Op: p.Op, Operator: p.Operator, Left: left, Right: p.Right, Escape: p.Escape}
			}
		}
		return &BinaryOperation{Op: p.Op, Operator: p.Operator, Left: &Column{Name: columnName}, Right: p.Right, Escape: p.Escape}
	case *IsNullExpression:
		if column, replaced := replacePredicateExpressionColumn(p.Column, columnName); replaced {
			clone := *p
			clone.Column = column
			return &clone
		}
		return p
	case *InExpression:
		if column, replaced := replacePredicateExpressionColumn(p.Column, columnName); replaced {
			clone := *p
			clone.Column = column
			clone.Values = append([]interface{}(nil), p.Values...)
			return &clone
		}
		return p
	case *LikeExpression:
		if column, replaced := replacePredicateExpressionColumn(p.Column, columnName); replaced {
			clone := *p
			clone.Column = column
			return &clone
		}
		return p
	case *BetweenExpression:
		if column, replaced := replacePredicateExpressionColumn(p.Column, columnName); replaced {
			clone := *p
			clone.Column = column
			return &clone
		}
		return p
	case *NotExpression, *IsTruthExpression:
		if columns := collectUsedColumns([]Expression{predicate}); len(columns) == 1 {
			if updated, changed := replaceColumnInExpression(predicate, columns[0], columnName); changed {
				return updated
			}
		}
		return predicate
	default:
		return predicate
	}
}

func replacePredicateExpressionColumn(expr Expression, columnName string) (Expression, bool) {
	columns := collectUsedColumns([]Expression{expr})
	if len(columns) != 1 {
		return nil, false
	}
	return replaceColumnInExpression(expr, columns[0], columnName)
}

func isTransitiveComparisonOp(op BinaryOp) bool {
	switch op {
	case OpEQ, OpNE, OpLT, OpLE, OpGT, OpGE, OpNullSafeEQ:
		return true
	default:
		return false
	}
}

func isTransitivePatternOp(op BinaryOp) bool {
	switch op {
	case OpLike, OpNotLike, OpIn, OpNotIn:
		return true
	default:
		return false
	}
}

func reverseTransitiveComparisonOp(op BinaryOp) BinaryOp {
	switch op {
	case OpLT:
		return OpGT
	case OpLE:
		return OpGE
	case OpGT:
		return OpLT
	case OpGE:
		return OpLE
	default:
		return op
	}
}

func appendJoinConditionIfMissing(conditions []Expression, candidate Expression) []Expression {
	for _, condition := range conditions {
		if expressionsEquivalent(condition, candidate) {
			return conditions
		}
	}
	return append(conditions, candidate)
}

// getJoinChildColumnSets 从 Join 的左右子计划得到左/右输出列名集合；用于谓词下推时划分条件。
func getJoinChildColumnSets(join *LogicalJoin) (map[string]bool, map[string]bool) {
	leftSet := make(map[string]bool)
	rightSet := make(map[string]bool)

	if join.LeftSchema != nil {
		for _, tbl := range join.LeftSchema.Tables {
			for _, col := range tbl.Columns {
				addJoinColumnName(leftSet, tbl.Name, col.Name)
			}
		}
	}
	if join.RightSchema != nil {
		for _, tbl := range join.RightSchema.Tables {
			for _, col := range tbl.Columns {
				addJoinColumnName(rightSet, tbl.Name, col.Name)
			}
		}
	}

	if len(leftSet) > 0 && len(rightSet) > 0 {
		return leftSet, rightSet
	}

	// 未设置 Schema 时从子计划推导
	if children := join.Children(); len(children) >= 2 {
		addPlanJoinColumnNames(leftSet, children[0])
		addPlanJoinColumnNames(rightSet, children[1])
	}
	return leftSet, rightSet
}

func addJoinColumnName(set map[string]bool, tableName, columnName string) {
	if set == nil || strings.TrimSpace(columnName) == "" {
		return
	}
	set[columnName] = true
	set[strings.ToLower(columnName)] = true
	if strings.TrimSpace(tableName) == "" {
		return
	}
	qualified := tableName + "." + columnName
	set[qualified] = true
	set[strings.ToLower(qualified)] = true
}

func addPlanJoinColumnNames(set map[string]bool, plan LogicalPlan) {
	if set == nil || plan == nil {
		return
	}
	switch p := plan.(type) {
	case *LogicalTableScan:
		if p.Table == nil {
			return
		}
		for _, col := range p.Table.Columns {
			if col != nil {
				addJoinColumnName(set, p.Table.Name, col.Name)
			}
		}
	case *LogicalIndexScan:
		if p.Table == nil {
			return
		}
		for _, col := range p.Table.Columns {
			if col != nil {
				addJoinColumnName(set, p.Table.Name, col.Name)
			}
		}
	case *LogicalCTEScan:
		if p.schema == nil {
			return
		}
		for _, table := range p.schema.Tables {
			if table == nil {
				continue
			}
			for _, col := range table.Columns {
				if col != nil {
					addJoinColumnName(set, table.Name, col.Name)
				}
			}
		}
	case *LogicalCTE:
		// A CTE definition is a named row-source boundary. Its declared
		// columns, rather than the physical columns of Query, are visible to
		// an enclosing join or Apply.
		addVisiblePlanOutputNames(set, p)
	case *LogicalRecursiveCTE, *LogicalCTEStatement:
		// Recursive CTEs and WITH statements likewise expose only their
		// declared/body output contract; do not leak hidden definition
		// columns into ownership inference.
		addVisiblePlanOutputNames(set, plan)
	case *LogicalProjection:
		// A projection defines the visible row-source boundary. Do not expose
		// hidden child columns here; ownership checks must use aliases such as
		// `right_key` rather than the physical `right.id` expression.
		addVisiblePlanOutputNames(set, p)
	case *LogicalSubquery, *LogicalValues, *LogicalAggregation, *LogicalUnion, *LogicalApply:
		// These nodes define their visible output outside Children() or through
		// a branch boundary. Ownership analysis must use that output contract
		// instead of recursively exposing hidden physical columns.
		addVisiblePlanOutputNames(set, plan)
	default:
		for _, child := range plan.Children() {
			addPlanJoinColumnNames(set, child)
		}
	}
}

func addVisiblePlanOutputNames(set map[string]bool, plan LogicalPlan) {
	for _, name := range getPlanOutputColumnNames(plan) {
		qualifier, column := splitQualifiedColumnName(name)
		addJoinColumnName(set, qualifier, column)
	}
}

// getPlanOutputColumnNames 返回该逻辑计划输出中的列名（用于谓词下推时判断条件归属）。
func getPlanOutputColumnNames(plan LogicalPlan) []string {
	if plan == nil {
		return nil
	}
	switch p := plan.(type) {
	case *LogicalTableScan:
		if p.Table == nil {
			return nil
		}
		names := make([]string, 0, len(p.Table.Columns))
		for _, col := range p.Table.Columns {
			names = append(names, col.Name)
		}
		return names
	case *LogicalIndexScan:
		if p.Table == nil {
			return nil
		}
		names := make([]string, 0, len(p.Table.Columns))
		for _, col := range p.Table.Columns {
			names = append(names, col.Name)
		}
		return names
	case *LogicalSelection:
		if len(p.Children()) > 0 {
			return getPlanOutputColumnNames(p.Children()[0])
		}
		return nil
	case *LogicalProjection:
		if len(p.OutputNames) == len(p.Exprs) && len(p.OutputNames) > 0 {
			return append([]string(nil), p.OutputNames...)
		}
		// A projection changes both the output arity and ordinal mapping. Using
		// the child schema here makes SELECT name, id look like [id, name],
		// which breaks ordinal/column ownership checks in later optimizer rules.
		names := make([]string, 0, len(p.Exprs))
		for _, expr := range p.Exprs {
			if expr == nil {
				continue
			}
			name := strings.TrimSpace(expr.String())
			if name != "" {
				names = append(names, name)
			}
		}
		if len(names) > 0 {
			return names
		}
		if len(p.Children()) > 0 {
			return getPlanOutputColumnNames(p.Children()[0])
		}
		return nil
	case *LogicalJoin:
		ch := p.Children()
		if len(ch) < 2 {
			return nil
		}
		left := getPlanOutputColumnNames(ch[0])
		right := getPlanOutputColumnNames(ch[1])
		return append(append([]string{}, left...), right...)
	case *LogicalApply:
		ch := p.Children()
		if len(ch) < 2 {
			return nil
		}
		left := getPlanOutputColumnNames(ch[0])
		// SEMI and ANTI APPLY return only the preserved/outer row source;
		// exposing right-side names here would make column ownership and
		// pruning treat hidden subquery columns as part of the result.
		applyType := strings.ToUpper(strings.TrimSpace(p.ApplyType))
		if applyType == "SEMI" || applyType == "ANTI" {
			return left
		}
		right := getPlanOutputColumnNames(ch[1])
		return append(append([]string{}, left...), right...)
	case *LogicalSubquery:
		// The subplan is stored outside Children because the node is also used
		// as an expression-like boundary. Its output columns still define the
		// names visible to an enclosing Apply/Union or ownership check.
		if p.Subplan != nil {
			return getPlanOutputColumnNames(p.Subplan)
		}
		if children := p.Children(); len(children) > 0 {
			return getPlanOutputColumnNames(children[0])
		}
		return nil
	case *LogicalAggregation:
		if names := logicalSchemaColumnNames(p.Schema()); len(names) > 0 {
			return names
		}
		names := make([]string, 0, len(p.GroupByItems)+len(p.AggFuncs))
		for _, item := range p.GroupByItems {
			if item != nil && strings.TrimSpace(item.String()) != "" {
				names = append(names, item.String())
			}
		}
		for _, aggregate := range p.AggFuncs {
			if aggregate == nil {
				continue
			}
			if expression, ok := aggregate.(Expression); ok && expression != nil {
				names = append(names, expression.String())
				continue
			}
			if name := strings.TrimSpace(aggregate.Name()); name != "" {
				names = append(names, name)
			}
		}
		return names
	case *LogicalValues:
		if names := logicalSchemaColumnNames(p.Schema()); len(names) > 0 {
			return names
		}
		names := make([]string, 0, len(p.Exprs))
		for _, expression := range p.Exprs {
			if expression != nil && strings.TrimSpace(expression.String()) != "" {
				names = append(names, expression.String())
			}
		}
		return names
	case *LogicalUnion:
		children := p.Children()
		if len(children) > 0 {
			// UNION output names and arity are defined by the first query
			// block; later branches contribute values by ordinal position.
			return getPlanOutputColumnNames(children[0])
		}
		return nil
	case *LogicalCTEScan:
		if len(p.Columns) > 0 {
			return append([]string(nil), p.Columns...)
		}
		if p.schema == nil {
			return nil
		}
		var names []string
		for _, table := range p.schema.Tables {
			if table == nil {
				continue
			}
			for _, col := range table.Columns {
				if col != nil {
					names = append(names, col.Name)
				}
			}
		}
		return names
	case *LogicalCTE:
		if len(p.Columns) > 0 {
			return append([]string(nil), p.Columns...)
		}
		if p.Query != nil {
			return getPlanOutputColumnNames(p.Query)
		}
		return nil
	case *LogicalRecursiveCTE:
		if len(p.Columns) > 0 {
			return append([]string(nil), p.Columns...)
		}
		if p.Anchor != nil {
			return getPlanOutputColumnNames(p.Anchor)
		}
		return nil
	case *LogicalCTEStatement:
		return getPlanOutputColumnNames(p.Body)
	default:
		return nil
	}
}

func logicalSchemaColumnNames(schema *metadata.DatabaseSchema) []string {
	if schema == nil {
		return nil
	}
	var names []string
	for _, table := range schema.Tables {
		if table == nil {
			continue
		}
		for _, column := range table.Columns {
			if column != nil && strings.TrimSpace(column.Name) != "" {
				names = append(names, column.Name)
			}
		}
	}
	return names
}

// isSafeForPredicatePushdown 检查是否可以安全地下推谓词
func isSafeForPredicatePushdown(join *LogicalJoin) bool {
	if join == nil {
		return false
	}
	// This function is used only for predicates above the join. Predicates
	// referencing one child are safe to push through LEFT/RIGHT JOIN: the
	// null-extended side is still produced by the join and the outer WHERE
	// predicate is evaluated afterward. Cross-child predicates remain in
	// otherConds in splitJoinCondition and are never pushed here.
	switch strings.ToUpper(strings.TrimSpace(join.JoinType)) {
	case "", "INNER", "LEFT", "RIGHT":
		return true
	default:
		return false
	}
}

// splitAggregatePredicate 分离聚合条件：可下推和不可下推
func splitAggregatePredicate(conditions []Expression, agg *LogicalAggregation) ([]Expression, []Expression) {
	var pushable []Expression
	var nonPushable []Expression

	for _, cond := range conditions {
		if canPushThroughAggregate(cond, agg) {
			pushable = append(pushable, cond)
		} else {
			nonPushable = append(nonPushable, cond)
		}
	}

	return pushable, nonPushable
}

// canPushThroughAggregate 检查条件是否可以下推到聚合之前
func canPushThroughAggregate(cond Expression, agg *LogicalAggregation) bool {
	// 检查条件中是否包含聚合函数
	if containsAggregateFunction(cond) {
		return false // HAVING条件，不可下推
	}

	// 检查条件中的列是否都在GROUP BY中
	// 如果条件只涉及GROUP BY列，可以下推
	cols := collectUsedColumns([]Expression{cond})
	groupByCols := collectUsedColumns(agg.GroupByItems)

	groupBySet := make(map[string]bool)
	for _, col := range groupByCols {
		groupBySet[col] = true
	}

	for _, col := range cols {
		if !groupBySet[col] {
			return false // 条件涉及非GROUP BY列，不可下推
		}
	}

	return true
}

// containsAggregateFunction 检查表达式中是否包含聚合函数
func containsAggregateFunction(expr Expression) bool {
	switch e := expr.(type) {
	case *Function:
		// 检查是否为聚合函数
		funcName := strings.ToUpper(e.FuncName)
		if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" ||
			funcName == "MAX" || funcName == "MIN" {
			return true
		}
		// 递归检查参数
		for _, arg := range e.FuncArgs {
			if containsAggregateFunction(arg) {
				return true
			}
		}
		return false

	case *BinaryOperation:
		return containsAggregateFunction(e.Left) || containsAggregateFunction(e.Right) || containsAggregateFunction(e.Escape)

	case *NotExpression:
		return containsAggregateFunction(e.Operand)

	case *UnaryOperation:
		return containsAggregateFunction(e.Operand)

	case *TupleExpression:
		for _, item := range e.Exprs {
			if containsAggregateFunction(item) {
				return true
			}
		}
		return false

	case *CaseExpression:
		if containsAggregateFunction(e.Operand) || containsAggregateFunction(e.Else) {
			return true
		}
		for _, when := range e.Whens {
			if containsAggregateFunction(when.Condition) || containsAggregateFunction(when.Value) {
				return true
			}
		}
		return false

	case *BetweenExpression:
		return containsAggregateFunction(e.Column) || containsAggregateFunction(e.LowerExpr) || containsAggregateFunction(e.UpperExpr)

	case *IsNullExpression:
		return containsAggregateFunction(e.Column)

	case *IsTruthExpression:
		return containsAggregateFunction(e.Expr)

	default:
		return false
	}
}

func collectUsedColumns(exprs []Expression) []string {
	colSet := make(map[string]struct{})
	var collect func(Expression)

	collect = func(e Expression) {
		if e == nil {
			return
		}
		switch v := e.(type) {
		case *Column:
			colSet[v.Name] = struct{}{}
		case *BinaryOperation:
			collect(v.Left)
			collect(v.Right)
			collect(v.Escape)
		case *Function:
			for _, arg := range v.Args() {
				collect(arg)
			}
		case *InExpression:
			collect(v.Column)
		case *LikeExpression:
			collect(v.Column)
			collect(v.Escape)
		case *IsNullExpression:
			collect(v.Column)
		case *BetweenExpression:
			collect(v.Column)
			collect(v.LowerExpr)
			collect(v.UpperExpr)
		case *UnaryOperation:
			collect(v.Operand)
		case *NotExpression:
			collect(v.Operand)
		case *TupleExpression:
			for _, item := range v.Exprs {
				collect(item)
			}
		case *CaseExpression:
			collect(v.Operand)
			for _, when := range v.Whens {
				collect(when.Condition)
				collect(when.Value)
			}
			collect(v.Else)
		case *IsTruthExpression:
			collect(v.Expr)
		default:
			for _, c := range e.Children() {
				collect(c)
			}
		}
	}

	for _, expr := range exprs {
		collect(expr)
	}

	cols := make([]string, 0, len(colSet))
	for c := range colSet {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return cols
}

func collectAggFuncCols(funcs []AggregateFunc) []Expression {
	var exprs []Expression
	for _, f := range funcs {
		exprs = append(exprs, f.Args()...)
	}
	return exprs
}

// columnInSchema checks whether the given column exists in any table of the schema.
func columnInSchema(schema *metadata.DatabaseSchema, col string) bool {
	if schema == nil {
		return false
	}
	for _, tbl := range schema.Tables {
		if _, ok := tbl.GetColumn(col); ok {
			return true
		}
	}
	return false
}

// buildPrunedSchema creates a new schema containing only the specified columns.
// Columns that do not exist in the original schema are ignored.
func buildPrunedSchema(schema *metadata.DatabaseSchema, cols []string) *metadata.DatabaseSchema {
	if schema == nil {
		return nil
	}
	newSchema := metadata.NewSchema(schema.Name)
	for _, tbl := range schema.Tables {
		newTbl := &metadata.Table{Name: tbl.Name, Indices: tbl.Indices, Stats: tbl.Stats}
		for _, required := range cols {
			colName := physicalRequiredColumnName(required)
			if col, ok := tbl.GetColumn(colName); ok {
				cp := *col
				newTbl.Columns = append(newTbl.Columns, &cp)
			}
		}
		_ = newSchema.AddTable(newTbl)
	}
	return newSchema
}

// applyPrunedSchemaToCTEScan maps visible CTE column aliases back to the
// underlying query columns by ordinal position. A declaration such as
// WITH recent (recent_id) AS (SELECT id ...) exposes recent_id to the outer
// plan, but the child schema still stores the physical column as id.
func applyPrunedSchemaToCTEScan(scan *LogicalCTEScan, required []string) {
	if scan == nil || len(required) == 0 || scan.schema == nil {
		return
	}
	if len(scan.Columns) == 0 {
		scan.schema = buildPrunedSchema(scan.schema, required)
		return
	}

	visiblePositions := make(map[string]int, len(scan.Columns))
	for position, name := range scan.Columns {
		key := physicalRequiredColumnName(name)
		if key != "" {
			visiblePositions[strings.ToLower(key)] = position
		}
	}
	underlying := make([]string, 0)
	for _, table := range scan.schema.Tables {
		if table == nil {
			continue
		}
		for _, column := range table.Columns {
			if column != nil {
				underlying = append(underlying, column.Name)
			}
		}
	}

	mapped := make([]string, 0, len(required))
	seen := make(map[string]struct{}, len(required))
	for _, name := range required {
		physical := physicalRequiredColumnName(name)
		if position, ok := visiblePositions[strings.ToLower(physical)]; ok && position < len(underlying) {
			physical = underlying[position]
		}
		key := strings.ToLower(strings.TrimSpace(physical))
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		mapped = append(mapped, physical)
	}
	scan.schema = buildPrunedSchema(scan.schema, mapped)
}

func canEliminateAggregation(agg *LogicalAggregation, child LogicalPlan) bool {
	// Only consider MIN/MAX without GROUP BY
	if len(agg.GroupByItems) > 0 || len(agg.AggFuncs) != 1 {
		return false
	}

	fn, ok := agg.AggFuncs[0].(*Function)
	if !ok {
		return false
	}

	name := strings.ToUpper(fn.Name())
	if name != "MIN" && name != "MAX" {
		return false
	}

	if len(fn.Args()) != 1 {
		return false
	}

	if _, ok := fn.Args()[0].(*Column); !ok {
		return false
	}

	switch child.(type) {
	case *LogicalTableScan, *LogicalIndexScan:
		return true
	}

	return false
}

func convertAggToProj(agg *LogicalAggregation) []Expression {
	if len(agg.AggFuncs) != 1 {
		return nil
	}
	fn, ok := agg.AggFuncs[0].(*Function)
	if !ok || len(fn.Args()) != 1 {
		return nil
	}
	return []Expression{fn.Args()[0]}
}

// simplifyProjMinMaxRoot 当根为 Proj(MAX(col)/MIN(col)) 且子为 Proj(col)（聚合消除产生）时，用子节点作为新根
func simplifyProjMinMaxRoot(plan LogicalPlan) LogicalPlan {
	switch nested := plan.(type) {
	case *LogicalSubquery:
		if nested.Subplan != nil {
			nested.Subplan = simplifyProjMinMaxRoot(nested.Subplan)
		}
		return nested
	case *LogicalCTE:
		if nested.Query != nil {
			nested.Query = simplifyProjMinMaxRoot(nested.Query)
		}
		return nested
	case *LogicalRecursiveCTE:
		if nested.Anchor != nil {
			nested.Anchor = simplifyProjMinMaxRoot(nested.Anchor)
		}
		if nested.Recursive != nil {
			nested.Recursive = simplifyProjMinMaxRoot(nested.Recursive)
		}
		return nested
	case *LogicalCTEStatement:
		for i, definition := range nested.Definitions {
			nested.Definitions[i] = simplifyProjMinMaxRoot(definition)
		}
		if nested.Body != nil {
			nested.Body = simplifyProjMinMaxRoot(nested.Body)
		}
		return nested
	case *LogicalUnion:
		children := nested.Children()
		newChildren := make([]LogicalPlan, len(children))
		for i, child := range children {
			newChildren[i] = simplifyProjMinMaxRoot(child)
		}
		nested.SetChildren(newChildren)
		return nested
	}
	proj, ok := plan.(*LogicalProjection)
	if !ok || len(proj.Exprs) != 1 {
		return plan
	}
	fn, ok := proj.Exprs[0].(*Function)
	if !ok || len(fn.Args()) != 1 {
		return plan
	}
	name := strings.ToUpper(fn.Name())
	if name != "MIN" && name != "MAX" {
		return plan
	}
	col, ok := fn.Args()[0].(*Column)
	if !ok {
		return plan
	}
	child := proj.Children()[0]
	childProj, ok := child.(*LogicalProjection)
	if !ok || len(childProj.Exprs) != 1 {
		return plan
	}
	childCol, ok := childProj.Exprs[0].(*Column)
	if !ok || childCol.Name != col.Name {
		return plan
	}
	return child
}
