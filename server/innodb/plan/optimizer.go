package plan

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
	// TODO: 实现子查询优化
	// 1. 子查询去关联
	// 2. 子查询展开
	// 3. 子查询上拉
	return plan
}

// 辅助函数

func mergePredicate(plan LogicalPlan, conditions []Expression) LogicalPlan {
	// TODO: 合并谓词条件
	return plan
}

func splitJoinCondition(conditions []Expression, join *LogicalJoin) ([]Expression, []Expression, []Expression) {
	// TODO: 分解连接条件
	return nil, nil, conditions
}

func collectUsedColumns(exprs []Expression) []string {
	// TODO: 收集表达式中使用的列
	return nil
}

func updateOutputColumns(plan LogicalPlan, usedCols []string) {
	// TODO: 更新计划节点的输出列
}

func collectAggFuncCols(funcs []AggregateFunc) []Expression {
	// TODO: 收集聚合函数中使用的列
	return nil
}

func canEliminateAggregation(agg *LogicalAggregation, child LogicalPlan) bool {
	// TODO: 判断是否可以消除聚合
	return false
}

func convertAggToProj(agg *LogicalAggregation) []Expression {
	// TODO: 将聚合转换为投影表达式
	return nil
}
