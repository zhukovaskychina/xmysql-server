package engine

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// executeCorrelatedDerivedOuterCompatibility handles correlated EXISTS where
// the outer row source is a derived table. The normal correlated handlers use
// the structured SELECT executor for base-table/JOIN sources, but a derived
// source cannot be represented by their compact source parser. Materialize the
// derived source once, then retain the same per-outer-row inner binding and
// NULL-aware EXISTS semantics.
func (e *XMySQLExecutor) executeCorrelatedDerivedOuterCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "select ") {
		return false, nil
	}
	existsAt, notExists, existsFound := findExistsCall(trimmed)
	if !existsFound {
		return false, nil
	}
	outerPrefix := strings.TrimSpace(trimmed[:existsAt])
	fromParts := splitTopLevelKeyword(outerPrefix, "from")
	if len(fromParts) != 2 || !strings.HasPrefix(strings.ToLower(strings.TrimSpace(fromParts[0])), "select ") {
		return false, nil
	}
	projectionText := strings.TrimSpace(strings.TrimSpace(fromParts[0])[len("select "):])
	sourceParts := splitTopLevelKeyword(strings.TrimSpace(fromParts[1]), "where")
	outerSource := strings.TrimSpace(sourceParts[0])
	outerWhere := ""
	if len(sourceParts) == 2 {
		outerWhere = strings.TrimSpace(sourceParts[1])
		outerWhere = trimCorrelatedConditionConnectorSuffix(outerWhere)
	}
	if !strings.HasPrefix(outerSource, "(") {
		return false, nil
	}
	closeSource := matchingParenIndex(outerSource, 0)
	if closeSource < 0 {
		return true, fmt.Errorf("unterminated derived outer source")
	}
	aliasText := strings.TrimSpace(outerSource[closeSource+1:])
	if strings.HasPrefix(strings.ToLower(aliasText), "as ") {
		aliasText = strings.TrimSpace(aliasText[len("as "):])
	}
	outerAlias := strings.Trim(strings.TrimSpace(aliasText), "`")
	if outerAlias == "" || strings.ContainsAny(outerAlias, " ()") {
		return false, nil
	}
	innerSourceSQL := strings.TrimSpace(outerSource[1:closeSource])
	if !strings.HasPrefix(strings.ToLower(innerSourceSQL), "select ") {
		return false, nil
	}
	open := strings.Index(trimmed[existsAt:], "(")
	if open < 0 {
		return true, fmt.Errorf("correlated derived EXISTS is missing parentheses")
	}
	open += existsAt
	close := matchingParenIndex(trimmed, open)
	if close < 0 {
		return true, fmt.Errorf("unterminated correlated derived EXISTS subquery")
	}
	tail := strings.TrimSpace(trimmed[close+1:])
	if tail != "" && !strings.HasPrefix(strings.ToLower(tail), "order by ") && !strings.HasPrefix(strings.ToLower(tail), "limit ") {
		return false, nil
	}
	innerSQL := strings.TrimSpace(trimmed[open+1 : close])
	if !strings.HasPrefix(strings.ToLower(innerSQL), "select ") || !correlatedDerivedSQLReferencesAlias(innerSQL, outerAlias) {
		return false, nil
	}

	outerStmt, err := sqlparser.Parse("select * from " + outerSource)
	if err != nil {
		return true, err
	}
	outerSelect, ok := outerStmt.(*sqlparser.Select)
	if !ok {
		return true, fmt.Errorf("derived outer source is not a SELECT")
	}
	outerResult, err := e.executeDerivedTableSelect(ctx, outerSelect, databaseName)
	if err != nil {
		return true, err
	}
	whereExpr, err := correlatedDerivedWhereExpr(outerWhere)
	if err != nil {
		return true, err
	}
	selectedColumns := splitTopLevelComma(projectionText)
	if len(selectedColumns) == 1 && strings.TrimSpace(selectedColumns[0]) == "*" {
		selectedColumns = append([]string(nil), outerResult.Columns...)
	}
	type selectedRow struct {
		values []basic.Value
		order  []interface{}
	}
	selectedRows := make([]selectedRow, 0, len(outerResult.Records))
	orderTerms, orderErr := correlatedDerivedOrderExpressions(tail)
	if orderErr != nil {
		return true, orderErr
	}
	metaColumns := make([]*metadata.ColumnMeta, 0, len(selectedColumns))
	for _, record := range outerResult.Records {
		rowValues := correlatedOuterRowValues(record, outerResult.Columns, outerResult.ColumnTypes, outerAlias, outerAlias)
		if whereExpr != nil {
			matched, evalErr := evalPredicate(whereExpr, rowValues)
			if evalErr != nil {
				return true, evalErr
			}
			if !matched {
				continue
			}
		}
		boundInnerSQL := innerSQL
		for index, column := range outerResult.Columns {
			if index >= len(record.GetValues()) {
				continue
			}
			literal := correlatedValueSQLLiteral(record.GetValues()[index])
			boundInnerSQL = replaceQualifiedName(boundInnerSQL, outerAlias, strings.Trim(column, "`"), literal)
		}
		innerStmt, parseErr := sqlparser.Parse(boundInnerSQL)
		if parseErr != nil {
			return true, parseErr
		}
		innerSelect, selectOK := innerStmt.(*sqlparser.Select)
		if !selectOK {
			return true, fmt.Errorf("derived correlated inner query is not SELECT")
		}
		innerResult, execErr := e.executeSelectStatement(ctx, innerSelect, databaseName)
		if execErr != nil {
			return true, execErr
		}
		matched := innerResult.RowCount > 0
		if (matched && notExists) || (!matched && !notExists) {
			continue
		}
		values := make([]basic.Value, 0, len(selectedColumns))
		for index, expression := range selectedColumns {
			expressionText, name, parseOK := correlatedDerivedProjection(expression)
			if !parseOK {
				return true, fmt.Errorf("unsupported derived correlated projection %s", expression)
			}
			value, evalErr := evaluateProjectionExpression(expressionText, rowValues)
			if evalErr != nil {
				return true, evalErr
			}
			values = append(values, correlatedExpressionValue(value))
			if len(metaColumns) <= index {
				columnType := metadata.TypeVarchar
				for outerIndex, outerColumn := range outerResult.Columns {
					if strings.EqualFold(strings.Trim(outerColumn, "`"), strings.Trim(name, "`")) && outerIndex < len(outerResult.ColumnTypes) {
						columnType = metadata.DataType(strings.ToUpper(outerResult.ColumnTypes[outerIndex]))
						break
					}
				}
				metaColumns = append(metaColumns, &metadata.ColumnMeta{Name: name, Type: columnType})
			}
		}
		orderValues := make([]interface{}, len(orderTerms))
		for index, term := range orderTerms {
			orderValues[index], err = evaluateProjectionExpression(term.expression, rowValues)
			if err != nil {
				return true, fmt.Errorf("evaluate correlated derived ORDER BY expression %q: %v", term.expression, err)
			}
		}
		selectedRows = append(selectedRows, selectedRow{values: values, order: orderValues})
	}
	if len(orderTerms) > 0 {
		sort.SliceStable(selectedRows, func(left, right int) bool {
			for termIndex, term := range orderTerms {
				comparison := compareScalarValues(selectedRows[left].order[termIndex], selectedRows[right].order[termIndex])
				if comparison == 0 {
					continue
				}
				if term.descending {
					return comparison > 0
				}
				return comparison < 0
			}
			return false
		})
	}
	limitOffset, limitCount, hasLimit, limitErr := correlatedDerivedLimit(tail)
	if limitErr != nil {
		return true, limitErr
	}
	if hasLimit {
		if limitOffset > len(selectedRows) {
			limitOffset = len(selectedRows)
		}
		end := limitOffset + limitCount
		if end > len(selectedRows) {
			end = len(selectedRows)
		}
		selectedRows = selectedRows[limitOffset:end]
	}
	meta := &metadata.TableMeta{Name: "correlated_derived", Columns: metaColumns}
	records := make([]Record, 0, len(selectedRows))
	for _, row := range selectedRows {
		records = append(records, NewExecutorRecord(row.values, meta))
	}
	columns := make([]string, 0, len(metaColumns))
	for _, column := range metaColumns {
		columns = append(columns, column.Name)
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: &SelectResult{Records: records, RowCount: len(records), Columns: columns, ColumnTypes: correlatedProjectionTypes(records, meta), ResultType: common.RESULT_TYPE_QUERY}, Message: fmt.Sprintf("correlated derived query returned %d rows", len(records))}
	return true, nil
}

func correlatedDerivedSQLReferencesAlias(query, alias string) bool {
	parsed, err := sqlparser.Parse(query)
	if err != nil || parsed == nil {
		return false
	}
	found := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		column, ok := node.(*sqlparser.ColName)
		if ok && !column.Qualifier.IsEmpty() && strings.EqualFold(strings.Trim(column.Qualifier.Name.String(), "`"), strings.Trim(alias, "`")) {
			found = true
			return false, nil
		}
		return true, nil
	}, parsed)
	return found
}

func trimCorrelatedConditionConnectorSuffix(condition string) string {
	trimmed := strings.TrimSpace(condition)
	lower := strings.ToLower(trimmed)
	for _, connector := range []string{"and", "or"} {
		if strings.HasSuffix(lower, " "+connector) {
			return strings.TrimSpace(trimmed[:len(trimmed)-len(connector)-1])
		}
	}
	return trimmed
}

type correlatedDerivedOrderTerm struct {
	expression string
	descending bool
}

func correlatedDerivedOrderExpressions(tail string) ([]correlatedDerivedOrderTerm, error) {
	trimmed := strings.TrimSpace(tail)
	if trimmed == "" || !strings.HasPrefix(strings.ToLower(trimmed), "order by ") {
		return nil, nil
	}
	expression := strings.TrimSpace(trimmed[len("order by "):])
	if limitParts := splitTopLevelKeyword(expression, "limit"); len(limitParts) == 2 {
		expression = strings.TrimSpace(limitParts[0])
	}
	terms := make([]correlatedDerivedOrderTerm, 0, 2)
	for _, rawTerm := range splitTopLevelSetOrderTerms(expression) {
		rawTerm = strings.TrimSpace(rawTerm)
		if rawTerm == "" {
			continue
		}
		stmt, err := sqlparser.Parse("select 1 order by " + rawTerm)
		if err != nil {
			return nil, fmt.Errorf("invalid correlated derived ORDER BY expression %q: %v", rawTerm, err)
		}
		selectStmt, ok := stmt.(*sqlparser.Select)
		if !ok || len(selectStmt.OrderBy) != 1 || selectStmt.OrderBy[0] == nil {
			return nil, fmt.Errorf("invalid correlated derived ORDER BY expression %q", rawTerm)
		}
		order := selectStmt.OrderBy[0]
		terms = append(terms, correlatedDerivedOrderTerm{
			expression: sqlparser.String(order.Expr),
			descending: order.Direction == sqlparser.DescScr,
		})
	}
	return terms, nil
}

func correlatedDerivedLimit(tail string) (offset, count int, ok bool, err error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(tail), ";"))
	if trimmed == "" {
		return 0, 0, false, nil
	}
	stmt, parseErr := sqlparser.Parse("select 1 " + trimmed)
	if parseErr != nil {
		return 0, 0, false, nil
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok || selectStmt.Limit == nil {
		return 0, 0, false, nil
	}
	parseLimitValue := func(expression sqlparser.Expr) (int, error) {
		value := strings.TrimSpace(sqlparser.String(expression))
		parsed, parseErr := strconv.Atoi(value)
		if parseErr != nil || parsed < 0 {
			return 0, fmt.Errorf("invalid correlated derived LIMIT value %q", value)
		}
		return parsed, nil
	}
	count, err = parseLimitValue(selectStmt.Limit.Rowcount)
	if err != nil {
		return 0, 0, true, err
	}
	if selectStmt.Limit.Offset != nil {
		offset, err = parseLimitValue(selectStmt.Limit.Offset)
		if err != nil {
			return 0, 0, true, err
		}
	}
	return offset, count, true, nil
}

func correlatedDerivedWhereExpr(where string) (sqlparser.Expr, error) {
	if strings.TrimSpace(where) == "" {
		return nil, nil
	}
	stmt, err := sqlparser.Parse("select * from correlated_source where " + where)
	if err != nil {
		return nil, err
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok || selectStmt.Where == nil {
		return nil, fmt.Errorf("invalid derived correlated outer WHERE")
	}
	return selectStmt.Where.Expr, nil
}

func correlatedDerivedProjection(expression string) (string, string, bool) {
	stmt, err := sqlparser.Parse("select " + strings.TrimSpace(expression))
	if err != nil || stmt == nil {
		return "", "", false
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok || len(selectStmt.SelectExprs) != 1 {
		return "", "", false
	}
	aliased, ok := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return "", "", false
	}
	expressionText := sqlparser.String(aliased.Expr)
	name := strings.TrimSpace(expressionText)
	if !aliased.As.IsEmpty() {
		name = aliased.As.String()
	} else if dot := strings.LastIndex(name, "."); dot >= 0 {
		name = name[dot+1:]
	}
	return expressionText, strings.Trim(name, "`"), true
}
