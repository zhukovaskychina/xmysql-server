package plan

import (
	"strings"
)

// SubqueryOptimizer 子查询优化器
// 负责优化子查询，包括去关联、上拉、下推等优化
type SubqueryOptimizer struct {
	// 优化统计信息
	stats SubqueryOptimizationStats
}

// SubqueryOptimizationStats 子查询优化统计信息
type SubqueryOptimizationStats struct {
	TotalSubqueries        int     // 总子查询数
	DecorrelatedSubqueries int     // 去关联的子查询数
	PulledUpSubqueries     int     // 上拉的子查询数
	InToSemiJoin           int     // IN转SEMI JOIN数
	ExistsToSemiJoin       int     // EXISTS转SEMI JOIN数
	EstimatedSpeedup       float64 // 预估加速比
}

// NewSubqueryOptimizer 创建子查询优化器
func NewSubqueryOptimizer() *SubqueryOptimizer {
	return &SubqueryOptimizer{
		stats: SubqueryOptimizationStats{},
	}
}

// Optimize 优化子查询
func (opt *SubqueryOptimizer) Optimize(plan LogicalPlan) LogicalPlan {
	return opt.optimizeSubqueryRecursive(plan)
}

// optimizeSubqueryRecursive 递归优化子查询
func (opt *SubqueryOptimizer) optimizeSubqueryRecursive(plan LogicalPlan) LogicalPlan {
	if plan == nil {
		return nil
	}

	switch v := plan.(type) {
	case *LogicalSubquery:
		opt.stats.TotalSubqueries++
		// A standalone subquery has no outer row source to become the left
		// child of an Apply/Join.  Flattening it here would create a one-child
		// Apply that cannot be converted into a valid physical plan.  Keep the
		// boundary intact and optimize only the inner query; callers that own
		// both row sources can still invoke the explicit IN/EXISTS rewrites.
		if v.Subplan != nil {
			v.Subplan = opt.optimizeSubqueryRecursive(v.Subplan)
		}
		return v

	case *LogicalCTE:
		// CTE.Query is a named plan field rather than a guaranteed child. Walk
		// it explicitly so subqueries inside a CTE definition are not skipped
		// when the builder keeps the definition separate from Children().
		if v.Query != nil {
			v.Query = opt.optimizeSubqueryRecursive(v.Query)
		}
		return opt.optimizeSubqueryChildren(v, v.Query)

	case *LogicalRecursiveCTE:
		// Anchor and Recursive are likewise independent plan fields. Both must
		// be optimized, but a child reference that points at either field must
		// not be visited a second time.
		if v.Anchor != nil {
			v.Anchor = opt.optimizeSubqueryRecursive(v.Anchor)
		}
		if v.Recursive != nil {
			v.Recursive = opt.optimizeSubqueryRecursive(v.Recursive)
		}
		return opt.optimizeSubqueryChildren(v, v.Anchor, v.Recursive)

	case *LogicalCTEStatement:
		// Definitions and Body are the authoritative CTE statement fields. A
		// few callers also mirror them into Children(), so deduplicate those
		// references to keep optimizer statistics stable.
		for i, definition := range v.Definitions {
			v.Definitions[i] = opt.optimizeSubqueryRecursive(definition)
		}
		if v.Body != nil {
			v.Body = opt.optimizeSubqueryRecursive(v.Body)
		}
		return opt.optimizeSubqueryChildren(v, append(append([]LogicalPlan{}, v.Definitions...), v.Body)...)

	case *LogicalApply:
		return opt.optimizeApplyNode(v)

	case *LogicalSelection:
		// 检查条件中是否包含子查询
		newConditions := make([]Expression, 0, len(v.Conditions))
		var subqueryPlans []LogicalPlan

		for _, cond := range v.Conditions {
			if subq := opt.extractSubqueryFromExpression(cond); subq != nil {
				// 提取子查询并优化
				optimizedSubq := opt.optimizeSubqueryNode(subq)
				subqueryPlans = append(subqueryPlans, optimizedSubq)
			} else {
				newConditions = append(newConditions, cond)
			}
		}

		// 如果有子查询，尝试转换为JOIN
		if len(subqueryPlans) > 0 {
			return opt.convertSubqueriesToJoins(v, subqueryPlans, newConditions)
		}

		v.Conditions = newConditions
		// 递归处理子节点
		for i, child := range v.Children() {
			children := v.Children()
			children[i] = opt.optimizeSubqueryRecursive(child)
			v.SetChildren(children)
		}
		return v

	default:
		// 递归处理子节点
		for i, child := range plan.Children() {
			children := plan.Children()
			children[i] = opt.optimizeSubqueryRecursive(child)
			plan.SetChildren(children)
		}
		return plan
	}
}

// optimizeSubqueryChildren recursively visits only children that are not
// already represented by an owning plan field. Logical CTE nodes historically
// used both representations, and visiting the same pointer twice would make
// optimization counters depend on how a plan was assembled.
func (opt *SubqueryOptimizer) optimizeSubqueryChildren(plan LogicalPlan, owned ...LogicalPlan) LogicalPlan {
	if plan == nil {
		return nil
	}
	children := plan.Children()
	if len(children) == 0 {
		return plan
	}
	optimized := make([]LogicalPlan, len(children))
	for i, child := range children {
		alreadyOwned := false
		for _, field := range owned {
			if field != nil && child == field {
				alreadyOwned = true
				break
			}
		}
		if alreadyOwned {
			optimized[i] = child
			continue
		}
		optimized[i] = opt.optimizeSubqueryRecursive(child)
	}
	plan.SetChildren(optimized)
	return plan
}

// optimizeSubqueryNode 优化子查询节点
func (opt *SubqueryOptimizer) optimizeSubqueryNode(subquery *LogicalSubquery) LogicalPlan {
	// 1. 如果是关联子查询，尝试去关联
	if subquery.Correlated {
		if decorrelated := opt.decorrelateSubquery(subquery); decorrelated != nil {
			if decorrelated != subquery {
				opt.stats.DecorrelatedSubqueries++
			}
			return decorrelated
		}
	}

	// 2. 根据子查询类型进行优化
	switch subquery.SubqueryType {
	case "IN":
		return opt.optimizeInSubquery(subquery)
	case "EXISTS":
		return opt.optimizeExistsSubquery(subquery)
	case "SCALAR":
		return opt.optimizeScalarSubquery(subquery)
	case "ANY", "ALL":
		return opt.optimizeQuantifiedSubquery(subquery)
	default:
		return subquery
	}
}

// decorrelateSubquery 去关联子查询。
//
// This helper only receives the subquery node, not the enclosing query's row
// source. It therefore cannot safely construct an Apply/Join: doing so would
// create a one-child plan and lose the outer-row binding required by the
// correlation. The enclosing-plan rewrites are responsible for supplying both
// row sources; until then the correlated boundary must remain intact.
func (opt *SubqueryOptimizer) decorrelateSubquery(subquery *LogicalSubquery) LogicalPlan {
	if !subquery.Correlated || len(subquery.OuterRefs) == 0 {
		return nil
	}

	return subquery
}

// optimizeInSubquery 优化IN子查询
// 将 IN 子查询转换为 SEMI JOIN
func (opt *SubqueryOptimizer) optimizeInSubquery(subquery *LogicalSubquery) LogicalPlan {
	// IN子查询可以转换为SEMI JOIN
	// SELECT * FROM t1 WHERE t1.id IN (SELECT t2.id FROM t2)
	// 转换为:
	// SELECT * FROM t1 SEMI JOIN t2 ON t1.id = t2.id

	// This helper receives only the subquery node. Without the enclosing
	// outer-row source it cannot construct a valid two-child Apply, and a
	// one-child Apply would lose the IN predicate's outer binding. The caller
	// that owns both sides must perform the SEMI rewrite explicitly.
	return subquery
}

// optimizeExistsSubquery 优化EXISTS子查询
// 将 EXISTS 子查询转换为 SEMI JOIN
func (opt *SubqueryOptimizer) optimizeExistsSubquery(subquery *LogicalSubquery) LogicalPlan {
	// EXISTS子查询可以转换为SEMI JOIN
	// SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t1.id = t2.id)
	// 转换为:
	// SELECT * FROM t1 SEMI JOIN t2 ON t1.id = t2.id

	// As with IN, this node-only helper has no outer child to bind. Preserve
	// the boundary; an enclosing-plan rewrite is required before a SEMI Apply
	// can be materialized safely.
	return subquery
}

// optimizeScalarSubquery 优化标量子查询
// 标量子查询返回单个值
func (opt *SubqueryOptimizer) optimizeScalarSubquery(subquery *LogicalSubquery) LogicalPlan {
	// 标量子查询通常需要保持原样，但可以优化子查询内部
	if subquery.Subplan != nil {
		subquery.Subplan = opt.optimizeSubqueryRecursive(subquery.Subplan)
	}
	return subquery
}

// optimizeQuantifiedSubquery 优化量化子查询（ANY/ALL）
func (opt *SubqueryOptimizer) optimizeQuantifiedSubquery(subquery *LogicalSubquery) LogicalPlan {
	// ANY/ALL子查询可以转换为聚合 + 比较
	// 例如: WHERE col > ANY (SELECT ...) 可以转换为 WHERE col > (SELECT MIN(...))

	if subquery.Subplan != nil {
		subquery.Subplan = opt.optimizeSubqueryRecursive(subquery.Subplan)
	}
	return subquery
}

// optimizeApplyNode 优化Apply节点
func (opt *SubqueryOptimizer) optimizeApplyNode(apply *LogicalApply) LogicalPlan {
	// 如果Apply不是关联的，可以转换为普通JOIN
	if !apply.Correlated {
		// A Join/Apply rewrite requires both row sources. Preserve malformed or
		// standalone boundaries instead of manufacturing a one-child Join that
		// later physical planning cannot execute safely.
		if len(apply.Children()) != 2 {
			for i, child := range apply.Children() {
				children := apply.Children()
				children[i] = opt.optimizeSubqueryRecursive(child)
				apply.SetChildren(children)
			}
			return apply
		}
		joinType := apply.ApplyType
		if joinType == "SEMI" || joinType == "ANTI" {
			// SEMI/ANTI only return the outer side and must not be lowered to
			// INNER, which would duplicate rows and expose inner columns. Keep
			// the two-child Apply so the executor can preserve its semantics.
			for i, child := range apply.Children() {
				children := apply.Children()
				children[i] = opt.optimizeSubqueryRecursive(child)
				apply.SetChildren(children)
			}
			return apply
		}

		join := &LogicalJoin{
			BaseLogicalPlan: BaseLogicalPlan{
				children: apply.Children(),
			},
			JoinType:   joinType,
			Conditions: splitConjunctiveConditions(apply.JoinConds),
		}

		opt.stats.PulledUpSubqueries++
		return join
	}

	// 递归优化子节点
	for i, child := range apply.Children() {
		children := apply.Children()
		children[i] = opt.optimizeSubqueryRecursive(child)
		apply.SetChildren(children)
	}

	return apply
}

// extractSubqueryFromExpression 从表达式中提取子查询
func (opt *SubqueryOptimizer) extractSubqueryFromExpression(expr Expression) *LogicalSubquery {
	// 检查表达式是否包含子查询
	// 这里需要根据实际的表达式类型来判断

	switch e := expr.(type) {
	case *Function:
		// 检查是否为IN/EXISTS等子查询函数
		funcName := strings.ToUpper(e.FuncName)
		if funcName == "IN" || funcName == "EXISTS" {
			// 提取子查询
			// 这里需要根据实际的函数参数来构建LogicalSubquery
			// 简化处理，返回nil
			return nil
		}
	}

	return nil
}

// convertSubqueriesToJoins 将子查询转换为JOIN
func (opt *SubqueryOptimizer) convertSubqueriesToJoins(
	selection *LogicalSelection,
	subqueries []LogicalPlan,
	remainingConds []Expression,
) LogicalPlan {
	// 将子查询转换为JOIN
	result := selection.Children()[0]

	for _, subq := range subqueries {
		// 创建JOIN
		join := &LogicalJoin{
			BaseLogicalPlan: BaseLogicalPlan{
				children: []LogicalPlan{result, subq},
			},
			JoinType:   "INNER",
			Conditions: []Expression{}, // 需要从子查询条件中提取
		}
		result = join
	}

	// 如果还有剩余条件，添加Selection
	if len(remainingConds) > 0 {
		result = &LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{
				children: []LogicalPlan{result},
			},
			Conditions: remainingConds,
		}
	}

	return result
}

// GetStats 获取优化统计信息
func (opt *SubqueryOptimizer) GetStats() SubqueryOptimizationStats {
	// 计算预估加速比
	if opt.stats.TotalSubqueries > 0 {
		optimizedCount := opt.stats.DecorrelatedSubqueries +
			opt.stats.PulledUpSubqueries +
			opt.stats.InToSemiJoin +
			opt.stats.ExistsToSemiJoin

		// 假设每个优化的子查询平均加速10倍
		opt.stats.EstimatedSpeedup = float64(optimizedCount) * 10.0
	}

	return opt.stats
}
