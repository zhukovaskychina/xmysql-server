package engine

import (
	"fmt"
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// executeAdvancedWindowQuery exposes the common single-table window shape
// through the SQL entrypoint: ROW_NUMBER, RANK and DENSE_RANK with PARTITION.
func (e *XMySQLExecutor) executeAdvancedWindowQuery(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	parsed, err := sqlparser.Parse(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	if err != nil || parsed == nil {
		return false, nil
	}
	selectStmt, ok := parsed.(*sqlparser.Select)
	if !ok || len(selectStmt.SelectExprs) != 2 || len(selectStmt.From) != 1 {
		return false, nil
	}
	valueExpr, valueOK := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr)
	windowExpr, windowOK := selectStmt.SelectExprs[1].(*sqlparser.AliasedExpr)
	windowFunction, functionOK := (*sqlparser.FuncExpr)(nil), false
	if windowOK {
		windowFunction, functionOK = windowExpr.Expr.(*sqlparser.FuncExpr)
	}
	if !valueOK || !windowOK || !functionOK || windowFunction.Over == nil || !windowExpr.As.IsEmpty() && windowExpr.As.String() == "" {
		return false, nil
	}
	functionName := strings.ToLower(windowFunction.Name.String())
	if functionName != "row_number" && functionName != "rank" && functionName != "dense_rank" {
		return false, nil
	}
	valueColumn, valueColumnOK := windowColumnExpressionName(valueExpr.Expr)
	if !valueColumnOK || windowExpr.As.IsEmpty() || len(windowFunction.Over.OrderBy) != 1 {
		return false, nil
	}
	orderColumn, orderColumnOK := windowColumnExpressionName(windowFunction.Over.OrderBy[0].Expr)
	if !orderColumnOK {
		return false, nil
	}
	partitionColumn := ""
	if len(windowFunction.Over.PartitionBy) > 0 {
		partitionColumn, _ = windowColumnExpressionName(windowFunction.Over.PartitionBy[0])
	}
	orderDirection := ""
	if windowFunction.Over.OrderBy[0].Direction == sqlparser.DescScr {
		orderDirection = "desc"
	}
	from, fromOK := selectStmt.From[0].(*sqlparser.AliasedTableExpr)
	fromName, tableOK := sqlparser.TableName{}, false
	if fromOK {
		fromName, tableOK = from.Expr.(sqlparser.TableName)
	}
	if !tableOK || fromName.Name.String() == "" {
		return false, nil
	}
	tableName := fromName.Name.String()
	if fromName.Qualifier.String() != "" {
		databaseName = fromName.Qualifier.String()
	}
	sourceStmt, err := sqlparser.Parse(fmt.Sprintf("select * from %s", tableName))
	if err != nil {
		return true, err
	}
	sourceSelect, ok := sourceStmt.(*sqlparser.Select)
	if !ok {
		return true, fmt.Errorf("window source is not a SELECT")
	}
	result, err := e.executeSelectStatement(ctx, sourceSelect, databaseName)
	if err != nil {
		return true, err
	}
	valueIndex, orderIndex, partitionIndex := -1, -1, -1
	for index, column := range result.Columns {
		clean := strings.Trim(column, "`")
		if strings.EqualFold(clean, valueColumn) {
			valueIndex = index
		}
		if strings.EqualFold(clean, orderColumn) {
			orderIndex = index
		}
		if partitionColumn != "" && strings.EqualFold(clean, partitionColumn) {
			partitionIndex = index
		}
	}
	if valueIndex < 0 || orderIndex < 0 || (partitionColumn != "" && partitionIndex < 0) {
		return true, fmt.Errorf("window column not found")
	}
	type windowRow struct {
		values []interface{}
		order  string
	}
	groups := map[string][]windowRow{}
	groupOrder := []string{}
	for _, record := range result.Records {
		values := make([]interface{}, 0, len(record.GetValues()))
		for _, value := range record.GetValues() {
			values = append(values, value.Raw())
		}
		group := ""
		if partitionIndex >= 0 {
			group = fmt.Sprintf("%v", values[partitionIndex])
		}
		if _, exists := groups[group]; !exists {
			groupOrder = append(groupOrder, group)
		}
		groups[group] = append(groups[group], windowRow{values: values, order: fmt.Sprintf("%v", values[orderIndex])})
	}
	output := make([][]interface{}, 0, len(result.Records))
	for _, group := range groupOrder {
		rows := groups[group]
		sort.SliceStable(rows, func(i, j int) bool { return compareWindowValue(rows[i].order, rows[j].order, orderDirection) < 0 })
		lastOrder, rank, denseRank := "", 0, 0
		for index, row := range rows {
			if index == 0 || row.order != lastOrder {
				rank = index + 1
				denseRank++
				lastOrder = row.order
			}
			windowValue := int64(index + 1)
			switch functionName {
			case "rank":
				windowValue = int64(rank)
			case "dense_rank":
				windowValue = int64(denseRank)
			}
			output = append(output, []interface{}{row.values[valueIndex], windowValue})
		}
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: newInformationSchemaSelectResult("window", []string{valueColumn, windowExpr.As.String()}, output), Message: fmt.Sprintf("SELECT query executed successfully, %d rows returned", len(output))}
	return true, nil
}

func windowColumnExpressionName(expression sqlparser.Expr) (string, bool) {
	column, ok := expression.(*sqlparser.ColName)
	if !ok {
		return "", false
	}
	name := strings.Trim(column.Name.String(), "`")
	return name, name != ""
}

func compareWindowValue(left, right, direction string) int {
	if left == right {
		return 0
	}
	result := -1
	if left > right {
		result = 1
	}
	if strings.EqualFold(direction, "desc") {
		return -result
	}
	return result
}
