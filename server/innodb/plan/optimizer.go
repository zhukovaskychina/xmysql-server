package plan

import (
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// OptimizeLogicalPlan 优化逻辑计划
func OptimizeLogicalPlan(plan LogicalPlan) LogicalPlan {
	// 0. 表达式规范化（新增）
	plan = normalizeExpressions(plan)

	// 1. 谓词下推
	plan = pushDownPredicates(plan)

	// 2. 列裁剪（自顶向下合并「上层算子所需列」，避免 Proj 与 Sel 各自裁剪导致丢失列）
	plan = columnPruning(plan, nil)

	// 3. 聚合消除
	plan = eliminateAggregation(plan)
	// 3.1 若根为 Proj(MAX/MIN(col)) 且子节点为 Proj(col)（聚合消除结果），则用子节点作为根
	plan = simplifyProjMinMaxRoot(plan)

	// 4. 子查询优化
	plan = optimizeSubquery(plan)

	// 5. 索引访问优化
	opt := NewIndexPushdownOptimizer()
	plan = optimizeIndexAccess(plan, opt)

	return plan
}

// normalizeExpressions 规范化表达式（新增）
func normalizeExpressions(plan LogicalPlan) LogicalPlan {
	normalizer := NewExpressionNormalizer()

	switch v := plan.(type) {
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
	case *LogicalProjection:
		// 投影算子不能下推谓词
		child := pushDownPredicates(v.Children()[0])
		v.SetChildren([]LogicalPlan{child})
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

		case *LogicalJoin:
			// 判断连接类型，外连接需要特殊处理
			if !isSafeForPredicatePushdown(childPlan) {
				// 不安全下推，保持原有结构
				v.SetChildren([]LogicalPlan{pushDownPredicates(childPlan)})
				return v
			}

			// 将连接条件分解为左右表的过滤条件
			leftConds, rightConds, otherConds := splitJoinCondition(v.Conditions, childPlan)

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

// columnPruning 列裁剪：parentRequired 为上层算子仍需要的列名（已排序去重可调用 unionSortedCols）。
func columnPruning(plan LogicalPlan, parentRequired []string) LogicalPlan {
	if plan == nil {
		return nil
	}

	switch v := plan.(type) {
	case *LogicalProjection:
		self := collectUsedColumns(v.Exprs)
		req := unionSortedCols(self, parentRequired)
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

	case *LogicalTableScan:
		applyPrunedSchemaToTableScan(v, parentRequired)
		return v

	case *LogicalIndexScan:
		applyPrunedSchemaToIndexScan(v, parentRequired)
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
	if len(cols) == 0 {
		ts.BaseLogicalPlan.schema = ts.Table.Schema
		return
	}
	ts.BaseLogicalPlan.schema = buildPrunedSchema(ts.Table.Schema, cols)
}

func applyPrunedSchemaToIndexScan(ix *LogicalIndexScan, cols []string) {
	if ix == nil || ix.Table == nil {
		return
	}
	if len(cols) == 0 {
		ix.BaseLogicalPlan.schema = ix.Table.Schema
		return
	}
	ix.BaseLogicalPlan.schema = buildPrunedSchema(ix.Table.Schema, cols)
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
			inLeft := leftColSet[c]
			inRight := rightColSet[c]
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

// getJoinChildColumnSets 从 Join 的左右子计划得到左/右输出列名集合；用于谓词下推时划分条件。
func getJoinChildColumnSets(join *LogicalJoin) (map[string]bool, map[string]bool) {
	leftSet := make(map[string]bool)
	rightSet := make(map[string]bool)

	if join.LeftSchema != nil {
		for _, tbl := range join.LeftSchema.Tables {
			for _, col := range tbl.Columns {
				leftSet[col.Name] = true
			}
		}
	}
	if join.RightSchema != nil {
		for _, tbl := range join.RightSchema.Tables {
			for _, col := range tbl.Columns {
				rightSet[col.Name] = true
			}
		}
	}

	if len(leftSet) > 0 && len(rightSet) > 0 {
		return leftSet, rightSet
	}

	// 未设置 Schema 时从子计划推导
	leftCols := getPlanOutputColumnNames(join.Children()[0])
	rightCols := getPlanOutputColumnNames(join.Children()[1])
	for _, c := range leftCols {
		leftSet[c] = true
	}
	for _, c := range rightCols {
		rightSet[c] = true
	}
	return leftSet, rightSet
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
	default:
		return nil
	}
}

// isSafeForPredicatePushdown 检查是否可以安全地下推谓词
func isSafeForPredicatePushdown(join *LogicalJoin) bool {
	// 外连接的ON条件不能下推，因为会影响连接语义
	// 只有INNER JOIN可以安全下推
	if join.JoinType == "INNER" || join.JoinType == "" {
		return true
	}
	return false
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
		return containsAggregateFunction(e.Left) || containsAggregateFunction(e.Right)

	case *NotExpression:
		return containsAggregateFunction(e.Operand)

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
		case *Function:
			for _, arg := range v.Args() {
				collect(arg)
			}
		case *InExpression:
			collect(v.Column)
		case *LikeExpression:
			collect(v.Column)
		case *IsNullExpression:
			collect(v.Column)
		case *BetweenExpression:
			collect(v.Column)
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
