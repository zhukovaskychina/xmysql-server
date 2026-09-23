package plan

import (
	"strings"

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
	tables []*metadata.Table,
) bool {
	for _, cond := range joinConditions {
		if joo.connectsSets(cond, left, right, tables) {
			return true
		}
	}
	return false
}

// connectsSets 检查条件是否连接两个表集合
func (joo *JoinOrderOptimizer) connectsSets(
	cond Expression,
	left, right uint64,
	tables []*metadata.Table,
) bool {
	mentioned := joo.tableBitsForExpression(cond, tables)
	return mentioned&left != 0 && mentioned&right != 0
}

// extractJoinConditions 提取连接两个表集合的条件
func (joo *JoinOrderOptimizer) extractJoinConditions(
	left, right uint64,
	joinConditions []Expression,
	tables []*metadata.Table,
) []Expression {
	var result []Expression

	for _, cond := range joinConditions {
		if joo.connectsSets(cond, left, right, tables) {
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

	for _, cond := range conditions {
		if equi, ok := cond.(*BinaryOperation); ok && equi.Op == OpEQ {
			leftColumn, leftOK := equi.Left.(*Column)
			rightColumn, rightOK := equi.Right.(*Column)
			if leftOK && rightOK && joo.statsCollector != nil {
				leftNDV := joo.columnDistinctCount(leftColumn)
				rightNDV := joo.columnDistinctCount(rightColumn)
				if leftNDV > 0 && rightNDV > 0 {
					maxNDV := leftNDV
					if rightNDV > maxNDV {
						maxNDV = rightNDV
					}
					selectivity *= 1.0 / float64(maxNDV)
					continue
				}
			}
		}
		selectivity *= 0.1
	}

	return selectivity
}

// involvesTable 检查表达式是否涉及指定表
func (joo *JoinOrderOptimizer) involvesTable(expr Expression, table *metadata.Table) bool {
	for _, col := range joo.extractColumns(expr) {
		if joo.columnBelongsToTable(col, table) {
			return true
		}
	}
	return false
}

// involvesJoinedTables 检查表达式是否涉及已连接的表
func (joo *JoinOrderOptimizer) involvesJoinedTables(expr Expression, joinNode *JoinNode) bool {
	if joinNode == nil {
		return false
	}
	for _, col := range joo.extractColumns(expr) {
		if joo.columnBelongsToJoinTree(col, joinNode) {
			return true
		}
	}
	return false
}

func (joo *JoinOrderOptimizer) columnDistinctCount(column *Column) int64 {
	if column == nil || joo.statsCollector == nil {
		return 0
	}
	tableName, columnName := splitQualifiedColumnName(column.Name)
	if tableName == "" {
		return 0
	}
	if stats, ok := joo.statsCollector.GetColumnStatistics(tableName, columnName); ok && stats != nil {
		return stats.DistinctCount
	}
	return 0
}

func (joo *JoinOrderOptimizer) tableBitsForExpression(expr Expression, tables []*metadata.Table) uint64 {
	var bits uint64
	for _, column := range joo.extractColumns(expr) {
		for index, table := range tables {
			if joo.columnBelongsToTable(column, table) {
				bits |= 1 << index
			}
		}
	}
	return bits
}

func (joo *JoinOrderOptimizer) columnBelongsToTable(column *Column, table *metadata.Table) bool {
	if column == nil || table == nil {
		return false
	}
	qualifier, columnName := splitQualifiedColumnName(column.Name)
	if qualifier != "" {
		if !strings.EqualFold(qualifier, table.Name) {
			return false
		}
		// Some planner callers provide only table identity and load column
		// metadata later. A qualified reference still identifies the table in
		// that case; once columns are present, validate the column name too.
		return len(table.Columns) == 0 || tableHasColumn(table, columnName)
	}
	return tableHasColumn(table, columnName)
}

func (joo *JoinOrderOptimizer) columnBelongsToJoinTree(column *Column, node *JoinNode) bool {
	if node == nil {
		return false
	}
	if node.NodeType == "TABLE" {
		return joo.columnBelongsToTable(column, node.Table)
	}
	return joo.columnBelongsToJoinTree(column, node.LeftChild) || joo.columnBelongsToJoinTree(column, node.RightChild)
}

func tableHasColumn(table *metadata.Table, columnName string) bool {
	if table == nil {
		return false
	}
	for _, column := range table.Columns {
		if column != nil && strings.EqualFold(column.Name, columnName) {
			return true
		}
	}
	return false
}

func splitQualifiedColumnName(name string) (qualifier, column string) {
	name = strings.Trim(strings.TrimSpace(name), "`")
	if dot := strings.LastIndex(name, "."); dot >= 0 {
		qualifier = strings.Trim(strings.TrimSpace(name[:dot]), "`")
		column = strings.Trim(strings.TrimSpace(name[dot+1:]), "`")
		return qualifier, column
	}
	return "", name
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
