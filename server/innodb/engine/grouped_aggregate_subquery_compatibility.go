package engine

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func (se *SelectExecutor) evaluateGroupedProjectionWithAggregateSubqueries(expression sqlparser.Expr, row joinedMapRow, groupRows []map[string]interface{}) (interface{}, error) {
	if !aggregateExpressionContainsAggregate(expression) {
		return se.evaluateGroupedProjectionWithSubqueries(expression, row)
	}
	rewritten, err := replaceGroupedAggregateCalls(sqlparser.String(expression), groupRows)
	if err != nil {
		return nil, err
	}
	statement, err := parseSelectSQL("select " + rewritten)
	if err != nil {
		return nil, err
	}
	if len(statement.SelectExprs) != 1 {
		return nil, fmt.Errorf("grouped aggregate subquery projection must contain one expression")
	}
	projection, ok := statement.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("grouped aggregate subquery projection is not an expression")
	}
	return se.evaluateGroupedProjectionWithSubqueries(projection.Expr, row)
}

func (se *SelectExecutor) matchesGroupedHavingWithAggregateSubqueries(row joinedMapRow, groupRows []map[string]interface{}) (bool, error) {
	if se == nil || se.havingExpression == nil || !expressionHasSubquery(se.havingExpression) || !aggregateExpressionContainsAggregate(se.havingExpression) {
		return se.matchesHavingRow(row)
	}
	rewritten, err := replaceGroupedAggregateCalls(sqlparser.String(se.havingExpression), groupRows)
	if err != nil {
		return false, err
	}
	statement, err := parseSelectSQL("select 1 where " + rewritten)
	if err != nil {
		return false, err
	}
	if statement.Where == nil {
		return false, fmt.Errorf("grouped HAVING expression has no predicate")
	}
	return se.evalJoinPredicateWithError(statement.Where.Expr, row)
}

func aggregateRecordRowValues(se *SelectExecutor, record Record) map[string]interface{} {
	values := make(map[string]interface{})
	if se == nil || record == nil {
		return values
	}
	meta, err := se.getRecordTableMeta(record)
	if err != nil || meta == nil {
		return values
	}
	for index, column := range meta.Columns {
		if column == nil || index >= len(record.GetValues()) {
			continue
		}
		name := strings.Trim(column.Name, "` ")
		value := recordValueForPredicate(record.GetValueByIndex(index), column.Type)
		values[name] = value
		values[strings.ToLower(name)] = value
		for _, qualifier := range []string{se.tableAlias, se.tableName, se.schemaName + "." + se.tableName} {
			qualifier = strings.Trim(qualifier, "` ")
			if qualifier == "" {
				continue
			}
			key := qualifier + "." + name
			values[key] = value
			values[strings.ToLower(key)] = value
		}
	}
	return values
}

func replaceGroupedAggregateCalls(expression string, rows []map[string]interface{}) (string, error) {
	var rewritten strings.Builder
	for index := 0; index < len(expression); {
		current := expression[index]
		if current == '\'' || current == '"' || current == '`' {
			end := sqlQuotedEnd(expression, index, current)
			rewritten.WriteString(expression[index:end])
			index = end
			continue
		}
		if current == '(' {
			close := matchingParenIndex(expression, index)
			if close < 0 {
				return "", fmt.Errorf("unbalanced expression while evaluating grouped aggregate")
			}
			if strings.HasPrefix(strings.ToLower(strings.TrimSpace(expression[index+1:close])), "select ") {
				rewritten.WriteString(expression[index : close+1])
				index = close + 1
				continue
			}
		}
		if isAggregateIdentifierStart(current) {
			nameEnd := index + 1
			for nameEnd < len(expression) && isAggregateIdentifierPart(expression[nameEnd]) {
				nameEnd++
			}
			open := nameEnd
			for open < len(expression) && (expression[open] == ' ' || expression[open] == '\t' || expression[open] == '\r' || expression[open] == '\n') {
				open++
			}
			if open < len(expression) && expression[open] == '(' {
				close := matchingParenIndex(expression, open)
				if close < 0 {
					return "", fmt.Errorf("unbalanced aggregate call in grouped projection")
				}
				call := expression[index : close+1]
				aggregate := parseAggregateExpression(call)
				if aggregate.funcName != "" {
					value, err := aggregateValueForGroupedRows(aggregate, rows)
					if err != nil {
						return "", err
					}
					rewritten.WriteString(groupedAggregateSQLLiteral(value))
					index = close + 1
					continue
				}
			}
			rewritten.WriteString(expression[index:nameEnd])
			index = nameEnd
			continue
		}
		rewritten.WriteByte(current)
		index++
	}
	return rewritten.String(), nil
}

func groupedAggregateSQLLiteral(value interface{}) string {
	switch value.(type) {
	case []interface{}, map[string]interface{}:
		encoded, err := json.Marshal(value)
		if err == nil {
			literal := strings.ReplaceAll(string(encoded), "\\", "\\\\")
			return "'" + strings.ReplaceAll(literal, "'", "''") + "'"
		}
	}
	return correlatedInterfaceSQLLiteral(value)
}

func aggregateValueForGroupedRows(aggregate aggregateExpression, rows []map[string]interface{}) (interface{}, error) {
	accumulator := aggregateAccumulator{
		fn:        aggregate.funcName,
		countStar: aggregate.column == "*",
		separator: aggregate.separator,
		orderBy:   aggregate.orderBy,
		orderDesc: aggregate.orderDesc,
	}
	if aggregate.distinct {
		accumulator.seen = make(map[string]struct{})
	}
	for _, row := range rows {
		if aggregate.funcName == "JSON_OBJECTAGG" {
			key, err := evaluateProjectionExpression(aggregate.column, row)
			if err != nil {
				return nil, err
			}
			value, err := evaluateProjectionExpression(aggregate.valueColumn, row)
			if err != nil {
				return nil, err
			}
			accumulator.addJSONObject(key, value)
			continue
		}
		if aggregate.funcName == "COUNT" && aggregate.column == "*" {
			accumulator.add(int64(1))
			continue
		}
		value, err := evaluateProjectionExpression(aggregate.column, row)
		if err != nil {
			return nil, err
		}
		if aggregate.funcName == "COUNT" && value == nil {
			continue
		}
		if collation := collationFromValueContext(row, aggregate.column); collation != "" {
			accumulator.valueCollation = collation
		}
		orderValue := value
		if aggregate.orderBy != "" {
			orderValue, err = evaluateProjectionExpression(aggregate.orderBy, row)
			if err != nil {
				return nil, err
			}
			if collation := collationFromValueContext(row, aggregate.orderBy); collation != "" {
				accumulator.orderCollation = collation
			}
		}
		accumulator.addWithOrder(value, orderValue)
	}
	return accumulator.value(), nil
}

func sqlQuotedEnd(text string, start int, quote byte) int {
	for index := start + 1; index < len(text); index++ {
		if text[index] == '\\' && quote != '`' && index+1 < len(text) {
			index++
			continue
		}
		if text[index] != quote {
			continue
		}
		if index+1 < len(text) && text[index+1] == quote {
			index++
			continue
		}
		return index + 1
	}
	return len(text)
}

func isAggregateIdentifierStart(value byte) bool {
	return value == '_' || value == '$' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
}

func isAggregateIdentifierPart(value byte) bool {
	return isAggregateIdentifierStart(value) || value >= '0' && value <= '9'
}
