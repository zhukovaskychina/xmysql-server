package engine

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// executeAdvancedWindowQuery exposes the common single-table window shape
// through the SQL entrypoint: ROW_NUMBER, RANK and DENSE_RANK with PARTITION.
func (e *XMySQLExecutor) executeAdvancedWindowQuery(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	pattern := regexp.MustCompile(`(?is)^\s*select\s+([a-zA-Z0-9_$.` + "`" + `]+)\s*,\s*(row_number|rank|dense_rank)\s*\(\s*\)\s+over\s*\(\s*(?:partition\s+by\s+([a-zA-Z0-9_$.` + "`" + `]+)\s+)?order\s+by\s+([a-zA-Z0-9_$.` + "`" + `]+)\s*(asc|desc)?\s*\)\s+as\s+([a-zA-Z0-9_` + "`" + `]+)\s+from\s+([a-zA-Z0-9_$.` + "`" + `]+)\s*;?\s*$`)
	match := pattern.FindStringSubmatch(query)
	if len(match) == 0 {
		return false, nil
	}
	valueColumn := strings.Trim(match[1], "`")
	functionName := strings.ToLower(match[2])
	partitionColumn := strings.Trim(match[3], "`")
	orderColumn := strings.Trim(match[4], "`")
	alias := strings.Trim(match[6], "`")
	tableName := strings.Trim(match[7], "`")
	if idx := strings.LastIndex(tableName, "."); idx >= 0 {
		databaseName, tableName = tableName[:idx], tableName[idx+1:]
	}
	stmt, err := sqlparser.Parse(fmt.Sprintf("select * from %s", tableName))
	if err != nil {
		return true, err
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return true, fmt.Errorf("window source is not a SELECT")
	}
	result, err := e.executeSelectStatement(ctx, selectStmt, databaseName)
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
		sort.SliceStable(rows, func(i, j int) bool { return compareWindowValue(rows[i].order, rows[j].order, match[5]) < 0 })
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
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: newInformationSchemaSelectResult("window", []string{valueColumn, alias}, output), Message: fmt.Sprintf("SELECT query executed successfully, %d rows returned", len(output))}
	return true, nil
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
