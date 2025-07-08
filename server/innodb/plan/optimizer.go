package plan

import (
	"sort"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// OptimizeLogicalPlan 优化逻辑计划
func OptimizeLogicalPlan(plan LogicalPlan) LogicalPlan {
	// 1. 谓词下推
	plan = pushDownPredicates(plan)

	// 2. 列裁剪
	plan = columnPruning(plan)

	// 3. 聚合消除
	plan = eliminateAggregation(plan)

	// 4. 子查询优化
	plan = optimizeSubquery(plan)

	// 5. 索引访问优化
	opt := NewIndexPushdownOptimizer()
	plan = optimizeIndexAccess(plan, opt)

	return plan
}

// pushDownPredicates 谓词下推优化
func pushDownPredicates(plan LogicalPlan) LogicalPlan {
	switch v := plan.(type) {
	case *LogicalProjection:
		// 投影算子不能下推谓词
		child := pushDownPredicates(v.Children()[0])
		v.SetChildren([]LogicalPlan{child})
		return v

	case *LogicalSelection:
		// 尝试将选择条件下推到子节点
		child := v.Children()[0]
		switch childPlan := child.(type) {
		case *LogicalTableScan, *LogicalIndexScan:
			// 可以直接下推到表扫描
			return mergePredicate(childPlan, v.Conditions)

		case *LogicalJoin:
			// 将连接条件分解为左右表的过滤条件
			leftConds, rightConds, otherConds := splitJoinCondition(v.Conditions, childPlan)

			// 递归下推左右表的过滤条件
			newLeft := pushDownPredicates(&LogicalSelection{
				BaseLogicalPlan: BaseLogicalPlan{
					children: []LogicalPlan{childPlan.Children()[0]},
				},
				Conditions: leftConds,
			})

			newRight := pushDownPredicates(&LogicalSelection{
				BaseLogicalPlan: BaseLogicalPlan{
					children: []LogicalPlan{childPlan.Children()[1]},
				},
				Conditions: rightConds,
			})

			// 重建连接节点
			childPlan.SetChildren([]LogicalPlan{newLeft, newRight})

			if len(otherConds) > 0 {
				// 剩余条件保留在选择算子中
				v.Conditions = otherConds
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

// columnPruning 列裁剪优化
func columnPruning(plan LogicalPlan) LogicalPlan {
	switch v := plan.(type) {
	case *LogicalProjection:
		// 收集投影中使用的列
		usedCols := collectUsedColumns(v.Exprs)

		// 递归优化子节点
		child := columnPruning(v.Children()[0])

		// 更新子节点的输出列
		updateOutputColumns(child, usedCols)

		v.SetChildren([]LogicalPlan{child})
		return v

	case *LogicalSelection:
		// 收集过滤条件中使用的列
		usedCols := collectUsedColumns(v.Conditions)

		// 递归优化子节点
		child := columnPruning(v.Children()[0])

		// 更新子节点的输出列
		updateOutputColumns(child, usedCols)

		v.SetChildren([]LogicalPlan{child})
		return v

	case *LogicalJoin:
		// 递归优化左右子树
		newLeft := columnPruning(v.Children()[0])
		newRight := columnPruning(v.Children()[1])

		// 收集连接条件中使用的列
		usedCols := collectUsedColumns(v.Conditions)

		// 更新左右子节点的输出列
		updateOutputColumns(newLeft, usedCols)
		updateOutputColumns(newRight, usedCols)

		v.SetChildren([]LogicalPlan{newLeft, newRight})
		return v

	case *LogicalAggregation:
		// 收集分组和聚合函数中使用的列
		usedCols := collectUsedColumns(append(v.GroupByItems, collectAggFuncCols(v.AggFuncs)...))

		// 递归优化子节点
		child := columnPruning(v.Children()[0])

		// 更新子节点的输出列
		updateOutputColumns(child, usedCols)

		v.SetChildren([]LogicalPlan{child})
		return v
	}

	return plan
}

// eliminateAggregation 聚合消除优化
func eliminateAggregation(plan LogicalPlan) LogicalPlan {
	switch v := plan.(type) {
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
	// 当前代码库尚未实现完整的子查询算子，这里仅递归处理子计划。
	// 若将来增加了子查询相关的逻辑计划节点，可在此处实现去关联、
	// 展开以及上拉等优化。

	for i, child := range plan.Children() {
		newChild := optimizeSubquery(child)
		children := plan.Children()
		children[i] = newChild
		plan.SetChildren(children)
	}

	return plan
}

// optimizeIndexAccess 使用索引下推优化器选择索引
func optimizeIndexAccess(plan LogicalPlan, optimizer *IndexPushdownOptimizer) LogicalPlan {
	switch v := plan.(type) {
	case *LogicalSelection:
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
				return newScan
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

func splitJoinCondition(conditions []Expression, join *LogicalJoin) ([]Expression, []Expression, []Expression) {
	var leftConds, rightConds, otherConds []Expression

	for _, cond := range conditions {
		cols := collectUsedColumns([]Expression{cond})
		if len(cols) == 0 {
			otherConds = append(otherConds, cond)
			continue
		}

		allLeft := true
		allRight := true
		for _, c := range cols {
			if join.LeftSchema == nil || !columnInSchema(join.LeftSchema, c) {
				allLeft = false
			}
			if join.RightSchema == nil || !columnInSchema(join.RightSchema, c) {
				allRight = false
			}
		}

		switch {
		case allLeft && !allRight:
			leftConds = append(leftConds, cond)
		case allRight && !allLeft:
			rightConds = append(rightConds, cond)
		default:
			otherConds = append(otherConds, cond)
		}
	}

	return leftConds, rightConds, otherConds
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
		case *Function:
			for _, arg := range v.Args {
				collect(arg)
			}
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

func updateOutputColumns(plan LogicalPlan, usedCols []string) {
	if len(usedCols) == 0 {
		return
	}

	switch p := plan.(type) {
	case *LogicalTableScan:
		p.BaseLogicalPlan.schema = buildPrunedSchema(p.Table.Schema, usedCols)
	case *LogicalIndexScan:
		p.BaseLogicalPlan.schema = buildPrunedSchema(p.Table.Schema, usedCols)
	case *LogicalSelection, *LogicalProjection, *LogicalAggregation:
		if len(p.Children()) > 0 {
			updateOutputColumns(p.Children()[0], usedCols)
		}
	case *LogicalJoin:
		if len(p.Children()) >= 2 {
			updateOutputColumns(p.Children()[0], usedCols)
			updateOutputColumns(p.Children()[1], usedCols)
		}
	}
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
		for _, colName := range cols {
			if col, ok := tbl.GetColumn(colName); ok {
				cp := *col
				newTbl.Columns = append(newTbl.Columns, &cp)
			}
		}
		_ = newSchema.AddTable(newTbl)
	}
	return newSchema
}

func canEliminateAggregation(agg *LogicalAggregation, child LogicalPlan) bool {
	// TODO: 判断是否可以消除聚合
	return false
}

func convertAggToProj(agg *LogicalAggregation) []Expression {
	// TODO: 将聚合转换为投影表达式
	return nil
}
