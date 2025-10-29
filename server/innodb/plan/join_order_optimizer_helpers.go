package plan

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// ============ 辅助方法 ============

// getTableRowCount 获取表行数
func (joo *JoinOrderOptimizer) getTableRowCount(table *metadata.Table) int64 {
	if joo.statsCollector != nil {
		if stats, exists := joo.statsCollector.GetTableStatistics(table.Name); exists {
			return stats.RowCount
		}
	}

	// 降级估算
	return 1000
}

// estimateTableScanCost 估算表扫描成本
func (joo *JoinOrderOptimizer) estimateTableScanCost(
	table *metadata.Table,
	whereConditions []Expression,
) *QueryCost {
	rowCount := joo.getTableRowCount(table)

	// 估算WHERE条件的选择率
	selectivity := 1.0
	if joo.selectivityEstimator != nil {
		for _, cond := range whereConditions {
			if joo.involvesTable(cond, table) {
				sel := joo.selectivityEstimator.EstimateSelectivity(table, cond)
				selectivity *= sel
			}
		}
	}

	// 假设每页100行，16KB
	pageSize := int64(100)
	numPages := (rowCount + pageSize - 1) / pageSize

	cost := joo.costModel.EstimateSeqScanCost(numPages, rowCount)

	// 应用选择率
	cost.Cardinality = int64(float64(rowCount) * selectivity)

	return cost
}

// hasJoinCondition 检查两个表集合之间是否有连接条件
func (joo *JoinOrderOptimizer) hasJoinCondition(
	left, right uint64,
	joinConditions []Expression,
	numTables int,
) bool {
	for _, cond := range joinConditions {
		if joo.connectsSets(cond, left, right, numTables) {
			return true
		}
	}
	return false
}

// connectsSets 检查条件是否连接两个表集合
func (joo *JoinOrderOptimizer) connectsSets(
	cond Expression,
	left, right uint64,
	numTables int,
) bool {
	// 提取条件中涉及的列
	columns := joo.extractColumns(cond)

	leftHas := false
	rightHas := false

	for _, col := range columns {
		// 简化实现：假设列名就是表名（实际应该解析表名）
		// TODO: 实现正确的表名解析
		_ = col
		for i := 0; i < numTables; i++ {
			if (left & (1 << i)) != 0 {
				leftHas = true
			}
			if (right & (1 << i)) != 0 {
				rightHas = true
			}
		}
	}

	return leftHas && rightHas
}

// extractJoinConditions 提取连接两个表集合的条件
func (joo *JoinOrderOptimizer) extractJoinConditions(
	left, right uint64,
	joinConditions []Expression,
	numTables int,
) []Expression {
	var result []Expression

	for _, cond := range joinConditions {
		if joo.connectsSets(cond, left, right, numTables) {
			result = append(result, cond)
		}
	}

	return result
}

// estimateJoinSelectivity 估算连接选择率
func (joo *JoinOrderOptimizer) estimateJoinSelectivity(
	conditions []Expression,
	tables []*metadata.Table,
) float64 {
	if len(conditions) == 0 {
		// 笛卡尔积，选择率为1
		return 1.0
	}

	selectivity := 1.0

	if joo.selectivityEstimator != nil {
		for _, cond := range conditions {
			// 简化实现：假设每个条件的选择率为0.1
			// TODO: 实现更精确的连接选择率估算
			_ = cond
			selectivity *= 0.1
		}
	} else {
		// 默认选择率
		selectivity = 0.1
	}

	return selectivity
}

// involvesTable 检查表达式是否涉及指定表
func (joo *JoinOrderOptimizer) involvesTable(expr Expression, table *metadata.Table) bool {
	columns := joo.extractColumns(expr)

	for _, col := range columns {
		// 简化实现：假设列名包含表名前缀
		// TODO: 实现更精确的表名匹配
		_ = col
		return true // 暂时返回true
	}

	return false
}

// involvesJoinedTables 检查表达式是否涉及已连接的表
func (joo *JoinOrderOptimizer) involvesJoinedTables(expr Expression, joinNode *JoinNode) bool {
	columns := joo.extractColumns(expr)

	for _, col := range columns {
		// 简化实现
		_ = col
		return true // 暂时返回true
	}

	return false
}

// extractColumns 从表达式中提取列
func (joo *JoinOrderOptimizer) extractColumns(expr Expression) []*Column {
	var columns []*Column

	switch e := expr.(type) {
	case *Column:
		columns = append(columns, e)
	case *BinaryOperation:
		columns = append(columns, joo.extractColumns(e.Left)...)
		columns = append(columns, joo.extractColumns(e.Right)...)
	case *Function:
		for _, arg := range e.Args() {
			columns = append(columns, joo.extractColumns(arg)...)
		}
	case *NotExpression:
		columns = append(columns, joo.extractColumns(e.Operand)...)
	case *InExpression:
		columns = append(columns, joo.extractColumns(e.Column)...)
	case *LikeExpression:
		columns = append(columns, joo.extractColumns(e.Column)...)
	case *IsNullExpression:
		columns = append(columns, joo.extractColumns(e.Column)...)
	case *BetweenExpression:
		columns = append(columns, joo.extractColumns(e.Column)...)
	}

	return columns
}

// ============ 成本上界剪枝 (OPT-018.3) ============

// pruneByCostUpperBound 成本上界剪枝
func (joo *JoinOrderOptimizer) pruneByCostUpperBound(
	currentCost *QueryCost,
	bestCost *QueryCost,
) bool {
	if !joo.config.EnableCostPruning {
		return false
	}

	if bestCost == nil {
		return false
	}

	// 如果当前成本已经超过已知最优成本，剪枝
	return currentCost.CompareTo(bestCost) > 0
}
