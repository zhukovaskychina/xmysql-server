package engine

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// executeCorrelatedPredicateSubqueryCompatibility evaluates the common
// correlated predicate shape one outer row at a time:
//
//	SELECT p.id FROM parents p
//	WHERE p.id IN (SELECT c.parent_id FROM children c WHERE c.parent_id = p.id)
//
// The normal statement-boundary IN/ANY/ALL rewrites deliberately materialize
// an uncorrelated subquery once. This handler is kept separate so a correlated
// inner WHERE is rebound for every outer row and SQL NULL semantics are not
// lost during that materialization.
func (e *XMySQLExecutor) executeCorrelatedPredicateSubqueryCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	if handled, err := e.executeMultipleCorrelatedPredicateSubqueriesCompatibility(ctx, query, databaseName); handled {
		return handled, err
	}
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "select ") {
		return false, nil
	}
	body := strings.TrimSpace(trimmed[len("select"):])
	fromParts := splitTopLevelKeyword(body, "from")
	if len(fromParts) != 2 {
		return false, nil
	}
	projectionText := strings.TrimSpace(fromParts[0])
	fromTail := strings.TrimSpace(fromParts[1])
	outerOrder := ""
	if orderParts := splitTopLevelKeyword(fromTail, "order by"); len(orderParts) == 2 {
		fromTail = strings.TrimSpace(orderParts[0])
		outerOrder = strings.TrimSpace(orderParts[1])
	}
	whereParts := splitTopLevelKeyword(fromTail, "where")
	if len(whereParts) != 2 {
		return false, nil
	}
	outerSource := strings.TrimSpace(whereParts[0])
	whereText := strings.TrimSpace(whereParts[1])
	outerSourceMatch := regexp.MustCompile("(?is)^([a-zA-Z0-9_$]+(?:\\.[a-zA-Z0-9_$]+)?)(?:\\s+(?:as\\s+)?([a-zA-Z0-9_$]+))?").FindStringSubmatch(outerSource)
	if len(outerSourceMatch) == 0 {
		return false, nil
	}
	outerTable := strings.TrimSpace(outerSourceMatch[1])
	outerAlias := strings.Trim(strings.TrimSpace(outerSourceMatch[2]), "`")
	if strings.EqualFold(outerAlias, "join") || strings.EqualFold(outerAlias, "left") || strings.EqualFold(outerAlias, "right") || strings.EqualFold(outerAlias, "inner") {
		outerAlias = ""
	}
	if outerAlias == "" {
		outerAlias = strings.Trim(strings.TrimSpace(outerTable), "`")
		if dot := strings.LastIndex(outerAlias, "."); dot >= 0 {
			outerAlias = strings.Trim(outerAlias[dot+1:], "`")
		}
	}

	predicatePattern := regexp.MustCompile(`(?is)([a-zA-Z0-9_$\.]+)\s*(not\s+in|in|(?:<=>|<>|!=|<=|>=|=|<|>)\s*(?:any|some|all))\s*\(`)
	predicateMatch := predicatePattern.FindStringSubmatchIndex(whereText)
	if len(predicateMatch) == 0 {
		return false, nil
	}
	open := strings.Index(whereText[predicateMatch[0]:predicateMatch[1]], "(")
	if open < 0 {
		return true, fmt.Errorf("correlated predicate subquery is missing parentheses")
	}
	open += predicateMatch[0]
	close := matchingParenIndex(whereText, open)
	if close < 0 {
		return true, fmt.Errorf("unterminated correlated predicate subquery")
	}
	outerConditions := make([]string, 0, 2)
	prefixConnector, suffixConnector := "", ""
	outerOrCondition := ""
	outerTrailingOrCondition := ""
	if prefix := strings.TrimSpace(whereText[:predicateMatch[0]]); prefix != "" {
		prefix, prefixConnector = splitCorrelatedConditionConnector(prefix, false)
		if prefix != "" {
			outerConditions = append(outerConditions, prefix)
			if prefixConnector == "OR" {
				outerOrCondition = prefix
			}
		}
	}
	if suffix := strings.TrimSpace(whereText[close+1:]); suffix != "" {
		suffix, suffixConnector = splitCorrelatedConditionConnector(suffix, true)
		if suffix != "" {
			if suffixConnector == "OR" {
				outerTrailingOrCondition = suffix
			} else {
				outerConditions = append(outerConditions, suffix)
			}
		}
	}
	// The compatibility handler removes the subquery predicate from the
	// candidate-row SELECT and evaluates it one outer row at a time. It can
	// safely prefilter only predicates joined to the subquery by AND. Applying
	// an OR-side predicate as an outer WHERE would change the truth table, so
	// leave those shapes to the general executor instead of silently rewriting
	// OR into AND.
	innerSQL := strings.TrimSpace(whereText[open+1 : close])
	if !strings.HasPrefix(strings.ToLower(innerSQL), "select ") || !strings.Contains(strings.ToLower(innerSQL), strings.ToLower(outerAlias)+".") {
		return false, nil
	}

	outerConditionText := strings.Join(outerConditions, " and ")
	outerDependencies := strings.TrimSpace(strings.Join([]string{outerConditionText, outerOrCondition, outerTrailingOrCondition}, " "))
	outerSQL := correlatedOuterSourceSQL(projectionText, outerDependencies, innerSQL, outerSource)
	if len(outerConditions) > 0 && outerOrCondition == "" && outerTrailingOrCondition == "" {
		outerSQL += " where " + strings.Join(outerConditions, " and ")
	}
	if outerOrder != "" {
		outerSQL += " order by " + outerOrder
	}
	outerStmt, err := parseSelectSQL(outerSQL)
	if err != nil {
		return true, err
	}
	outerResult, err := e.executeSelectStatement(ctx, outerStmt, databaseName)
	if err != nil {
		return true, err
	}
	var outerOrExpr sqlparser.Expr
	if outerOrCondition != "" {
		whereStmt, parseErr := sqlparser.Parse("select 1 from correlated_source where " + outerOrCondition)
		if parseErr != nil {
			return true, parseErr
		}
		whereSelect, ok := whereStmt.(*sqlparser.Select)
		if !ok || whereSelect.Where == nil {
			return true, fmt.Errorf("outer correlated WHERE is not a predicate")
		}
		outerOrExpr = whereSelect.Where.Expr
	}
	var outerTrailingOrExpr sqlparser.Expr
	if outerTrailingOrCondition != "" {
		whereStmt, parseErr := sqlparser.Parse("select 1 from correlated_source where " + outerTrailingOrCondition)
		if parseErr != nil {
			return true, parseErr
		}
		whereSelect, ok := whereStmt.(*sqlparser.Select)
		if !ok || whereSelect.Where == nil {
			return true, fmt.Errorf("trailing outer correlated WHERE is not a predicate")
		}
		outerTrailingOrExpr = whereSelect.Where.Expr
	}
	selectedColumns := splitTopLevelComma(projectionText)
	if len(selectedColumns) == 1 && strings.TrimSpace(selectedColumns[0]) == "*" {
		selectedColumns = append([]string(nil), outerResult.Columns...)
	}
	selectedIndexes, err := correlatedPredicateSelectedIndexes(selectedColumns, outerResult.Columns)
	if err != nil {
		return true, err
	}

	leftText := strings.TrimSpace(whereText[predicateMatch[2]:predicateMatch[3]])
	operator := strings.ToUpper(strings.TrimSpace(whereText[predicateMatch[4]:predicateMatch[5]]))
	meta := &metadata.TableMeta{Name: "correlated_predicate", Columns: make([]*metadata.ColumnMeta, 0, len(selectedColumns))}
	for index, column := range selectedColumns {
		name := strings.TrimSpace(column)
		if dot := strings.LastIndex(name, "."); dot >= 0 {
			name = name[dot+1:]
		}
		name = strings.Trim(name, "` ")
		columnType := metadata.TypeVarchar
		if index < len(selectedIndexes) && selectedIndexes[index] < len(outerResult.ColumnTypes) {
			columnType = metadata.DataType(strings.ToUpper(outerResult.ColumnTypes[selectedIndexes[index]]))
		}
		meta.Columns = append(meta.Columns, &metadata.ColumnMeta{Name: name, Type: columnType})
	}

	selectedValues := make([][]basic.Value, 0, len(outerResult.Records))
	for _, outerRecord := range outerResult.Records {
		rowValues := correlatedOuterRowValues(outerRecord, outerResult.Columns, outerResult.ColumnTypes, outerAlias, outerTable)
		if outerTrailingOrExpr != nil {
			outerMatches, evalErr := evalPredicate(outerTrailingOrExpr, rowValues)
			if evalErr != nil {
				return true, evalErr
			}
			if outerMatches {
				values := make([]basic.Value, 0, len(selectedIndexes))
				for _, index := range selectedIndexes {
					values = append(values, outerRecord.GetValues()[index])
				}
				selectedValues = append(selectedValues, values)
				continue
			}
		}
		if outerOrExpr != nil {
			outerMatches, evalErr := evalPredicate(outerOrExpr, rowValues)
			if evalErr != nil {
				return true, evalErr
			}
			if outerMatches {
				values := make([]basic.Value, 0, len(selectedIndexes))
				for _, index := range selectedIndexes {
					values = append(values, outerRecord.GetValues()[index])
				}
				selectedValues = append(selectedValues, values)
				continue
			}
		}
		leftValue, err := correlatedPredicateValue(leftText, rowValues, outerResult.Columns, outerRecord)
		if err != nil {
			return true, err
		}
		boundInnerSQL := innerSQL
		for index, column := range outerResult.Columns {
			if index >= len(outerRecord.GetValues()) {
				continue
			}
			pattern := regexp.MustCompile(`(?i)` + regexp.QuoteMeta(outerAlias) + `\s*\.\s*` + regexp.QuoteMeta(strings.Trim(column, "`")))
			boundInnerSQL = pattern.ReplaceAllString(boundInnerSQL, basicValueSQLLiteral(outerRecord.GetValues()[index]))
		}
		innerStmt, err := parseSelectSQL(boundInnerSQL)
		if err != nil {
			return true, err
		}
		innerResult, err := e.executeSelectStatement(ctx, innerStmt, databaseName)
		if err != nil {
			return true, err
		}
		if len(innerResult.Columns) != 1 {
			return true, fmt.Errorf("correlated predicate subquery returns %d columns", len(innerResult.Columns))
		}
		innerValues := make([]interface{}, 0, len(innerResult.Records))
		for _, record := range innerResult.Records {
			if len(record.GetValues()) == 0 || record.GetValues()[0].IsNull() {
				innerValues = append(innerValues, nil)
			} else {
				innerValues = append(innerValues, record.GetValues()[0].Raw())
			}
		}
		if !correlatedPredicateMatches(leftValue, operator, innerValues) {
			continue
		}
		values := make([]basic.Value, 0, len(selectedIndexes))
		for _, index := range selectedIndexes {
			values = append(values, outerRecord.GetValues()[index])
		}
		selectedValues = append(selectedValues, values)
	}

	records := make([]Record, 0, len(selectedValues))
	for _, values := range selectedValues {
		records = append(records, NewExecutorRecord(values, meta))
	}
	columns := make([]string, 0, len(meta.Columns))
	for _, column := range meta.Columns {
		columns = append(columns, column.Name)
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: &SelectResult{
		Records: records, RowCount: len(records), Columns: columns,
		ColumnTypes: correlatedProjectionTypes(records, meta), ResultType: common.RESULT_TYPE_QUERY,
	}, Message: fmt.Sprintf("correlated predicate query returned %d rows", len(records))}
	return true, nil
}

func splitCorrelatedConditionConnector(text string, leading bool) (string, string) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", ""
	}
	if leading {
		pattern := regexp.MustCompile(`(?is)^\s*(and|or)\s+`)
		match := pattern.FindStringSubmatch(text)
		if len(match) == 2 {
			return strings.TrimSpace(pattern.ReplaceAllString(text, "")), strings.ToUpper(match[1])
		}
		return text, ""
	}
	pattern := regexp.MustCompile(`(?is)\s+(and|or)\s*$`)
	match := pattern.FindStringSubmatch(text)
	if len(match) == 2 {
		return strings.TrimSpace(pattern.ReplaceAllString(text, "")), strings.ToUpper(match[1])
	}
	return text, ""
}

func correlatedPredicateSelectedIndexes(columns, resultColumns []string) ([]int, error) {
	indexes := make([]int, 0, len(columns))
	for _, column := range columns {
		clean := strings.Trim(strings.TrimSpace(column), "`")
		if dot := strings.LastIndex(clean, "."); dot >= 0 {
			clean = clean[dot+1:]
		}
		found := -1
		for index, resultColumn := range resultColumns {
			if strings.EqualFold(strings.Trim(resultColumn, "`"), clean) {
				found = index
				break
			}
		}
		if found < 0 {
			return nil, fmt.Errorf("outer correlated column %s does not exist", column)
		}
		indexes = append(indexes, found)
	}
	return indexes, nil
}

func correlatedPredicateValue(expression string, values map[string]interface{}, columns []string, record Record) (interface{}, error) {
	clean := strings.Trim(strings.TrimSpace(expression), "`")
	if dot := strings.LastIndex(clean, "."); dot >= 0 {
		clean = clean[dot+1:]
	}
	for index, column := range columns {
		if strings.EqualFold(strings.Trim(column, "`"), clean) && index < len(record.GetValues()) {
			value := record.GetValues()[index]
			if value == nil || value.IsNull() {
				return nil, nil
			}
			return value.Raw(), nil
		}
	}
	return evaluateProjectionExpression(expression, values)
}

func correlatedPredicateMatches(left interface{}, operator string, values []interface{}) bool {
	upper := strings.ToUpper(strings.TrimSpace(operator))
	if upper == "IN" || upper == "NOT IN" {
		if left == nil {
			return false
		}
		matched, hasNull := false, false
		for _, right := range values {
			if right == nil {
				hasNull = true
				continue
			}
			if compareScalarValues(left, right) == 0 {
				matched = true
				break
			}
		}
		if upper == "IN" {
			return matched
		}
		return !matched && !hasNull
	}
	parts := strings.Fields(upper)
	if len(parts) != 2 {
		return false
	}
	truth, _ := evaluateQuantifiedPredicate(left, parts[0], parts[1], values)
	return truth
}
