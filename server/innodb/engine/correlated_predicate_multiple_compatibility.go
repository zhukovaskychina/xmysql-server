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

type correlatedPredicateClause struct {
	leftText  string
	operator  string
	innerSQL  string
	kind      string
	outerExpr sqlparser.Expr
}

// executeMultipleCorrelatedPredicateSubqueriesCompatibility handles the
// common conjunction of two or more correlated predicate subqueries. Each
// subquery is rebound to the current outer row and all predicates must be
// true, preserving SQL's AND semantics without materializing any correlated
// inner query once for the whole statement.
func (e *XMySQLExecutor) executeMultipleCorrelatedPredicateSubqueriesCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	if !strings.HasPrefix(strings.ToLower(trimmed), "select ") {
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
	if outerAlias == "" || strings.EqualFold(outerAlias, "join") || strings.EqualFold(outerAlias, "left") || strings.EqualFold(outerAlias, "right") || strings.EqualFold(outerAlias, "inner") {
		outerAlias = strings.Trim(strings.TrimSpace(outerTable), "`")
		if dot := strings.LastIndex(outerAlias, "."); dot >= 0 {
			outerAlias = strings.Trim(outerAlias[dot+1:], "`")
		}
	}

	terms := splitTopLevelConjunctions(whereText)
	booleanOperator := "AND"
	if len(terms) < 2 {
		terms = splitTopLevelDisjunctions(whereText)
		booleanOperator = "OR"
		if len(terms) < 2 {
			return false, nil
		}
	}
	pattern := regexp.MustCompile(`(?is)^\s*([a-zA-Z0-9_$\.]+)\s*(not\s+in|in|(?:<=>|<>|!=|<=|>=|=|<|>)\s*(?:any|some|all))\s*\(`)
	existsPattern := regexp.MustCompile(`(?is)^\s*(not\s+)?exists\s*\(`)
	clauses := make([]correlatedPredicateClause, 0, len(terms))
	outerConditions := make([]string, 0, len(terms))
	for _, rawTerm := range terms {
		term := trimEnclosingPredicateParens(strings.TrimSpace(rawTerm))
		match := pattern.FindStringSubmatchIndex(term)
		clauseKind := "predicate"
		operator := ""
		leftText := ""
		if len(match) > 0 {
			leftText = strings.TrimSpace(term[match[2]:match[3]])
			operator = strings.ToUpper(strings.TrimSpace(term[match[4]:match[5]]))
		} else if existsMatch := existsPattern.FindStringSubmatchIndex(term); len(existsMatch) > 0 {
			match = existsMatch
			clauseKind = "exists"
			operator = "EXISTS"
			if existsMatch[2] >= 0 {
				operator = "NOT EXISTS"
			}
		} else {
			if booleanOperator == "OR" {
				outerStmt, parseErr := parseSelectSQL("select 1 from correlated_source where " + term)
				if parseErr != nil || outerStmt.Where == nil {
					if parseErr != nil {
						return true, parseErr
					}
					return true, fmt.Errorf("outer correlated predicate is not a WHERE expression")
				}
				clauses = append(clauses, correlatedPredicateClause{kind: "outer", leftText: term, outerExpr: outerStmt.Where.Expr})
				outerConditions = append(outerConditions, term)
				continue
			}
			outerConditions = append(outerConditions, term)
			continue
		}
		open := strings.Index(term[match[0]:match[1]], "(")
		if open < 0 {
			return true, fmt.Errorf("correlated predicate subquery is missing parentheses")
		}
		open += match[0]
		close := matchingParenIndex(term, open)
		if close < 0 || strings.TrimSpace(term[close+1:]) != "" {
			return false, nil
		}
		innerSQL := strings.TrimSpace(term[open+1 : close])
		if !strings.HasPrefix(strings.ToLower(innerSQL), "select ") || !strings.Contains(strings.ToLower(innerSQL), strings.ToLower(outerAlias)+".") {
			return false, nil
		}
		clauses = append(clauses, correlatedPredicateClause{
			leftText: leftText,
			operator: operator,
			innerSQL: innerSQL,
			kind:     clauseKind,
		})
	}
	if len(clauses) < 2 {
		return false, nil
	}

	innerDependencies := make([]string, 0, len(clauses))
	for _, clause := range clauses {
		innerDependencies = append(innerDependencies, clause.innerSQL)
	}
	outerDependencies := strings.TrimSpace(strings.Join(append(outerConditions, innerDependencies...), " "))
	outerSQL := correlatedOuterSourceSQL(projectionText, outerDependencies, strings.Join(innerDependencies, " "), outerSource)
	if booleanOperator == "AND" && len(outerConditions) > 0 {
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
	selectedColumns := splitTopLevelComma(projectionText)
	if len(selectedColumns) == 1 && strings.TrimSpace(selectedColumns[0]) == "*" {
		selectedColumns = append([]string(nil), outerResult.Columns...)
	}
	selectedIndexes, err := correlatedPredicateSelectedIndexes(selectedColumns, outerResult.Columns)
	if err != nil {
		return true, err
	}

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
		rowMatches := booleanOperator == "AND"
		for _, clause := range clauses {
			if clause.kind == "outer" {
				matched, evalErr := evalPredicate(clause.outerExpr, rowValues)
				if evalErr != nil {
					return true, evalErr
				}
				if booleanOperator == "AND" {
					if !matched {
						rowMatches = false
						break
					}
				} else if matched {
					rowMatches = true
					break
				}
				continue
			}
			var leftValue interface{}
			if clause.kind != "exists" {
				var valueErr error
				leftValue, valueErr = correlatedPredicateValue(clause.leftText, rowValues, outerResult.Columns, outerRecord)
				if valueErr != nil {
					return true, valueErr
				}
			}
			boundInnerSQL := clause.innerSQL
			for index, column := range outerResult.Columns {
				if index >= len(outerRecord.GetValues()) {
					continue
				}
				pattern := regexp.MustCompile(`(?i)` + regexp.QuoteMeta(outerAlias) + `\s*\.\s*` + regexp.QuoteMeta(strings.Trim(column, "`")))
				boundInnerSQL = pattern.ReplaceAllString(boundInnerSQL, basicValueSQLLiteral(outerRecord.GetValues()[index]))
			}
			innerStmt, parseErr := parseSelectSQL(boundInnerSQL)
			if parseErr != nil {
				return true, parseErr
			}
			innerResult, execErr := e.executeSelectStatement(ctx, innerStmt, databaseName)
			if execErr != nil {
				return true, execErr
			}
			matched := false
			if clause.kind == "exists" {
				matched = innerResult.RowCount > 0
				if clause.operator == "NOT EXISTS" {
					matched = !matched
				}
			} else {
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
				matched = correlatedPredicateMatches(leftValue, clause.operator, innerValues)
			}
			if booleanOperator == "AND" {
				if !matched {
					rowMatches = false
					break
				}
			} else if matched {
				rowMatches = true
				break
			}
		}
		if !rowMatches {
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
	}, Message: fmt.Sprintf("multiple correlated predicate query returned %d rows", len(records))}
	return true, nil
}

func trimEnclosingPredicateParens(input string) string {
	for len(input) >= 2 && input[0] == '(' {
		close := matchingParenIndex(input, 0)
		if close != len(input)-1 {
			break
		}
		input = strings.TrimSpace(input[1:close])
	}
	return input
}
