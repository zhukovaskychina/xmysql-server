package engine

import (
	"encoding/binary"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// executeCorrelatedScalarSubqueryCompatibility handles the common correlated
// projection shape that cannot be represented by the legacy select executor:
//
//	SELECT p.id, (SELECT MAX(c.score) FROM children c
//	              WHERE c.parent_id = p.id) AS max_score
//	FROM parents p
//
// The inner statement is parsed and executed independently for every outer
// row, so aggregates, empty results (NULL), and row-specific outer bindings
// do not accidentally reuse the previous outer row.
func (e *XMySQLExecutor) executeCorrelatedScalarSubqueryCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "select ") || !strings.Contains(lower, "(select") {
		return false, nil
	}
	body := strings.TrimSpace(trimmed[len("select"):])
	fromParts := splitTopLevelKeyword(body, "from")
	if len(fromParts) != 2 {
		return false, nil
	}
	projectionText := strings.TrimSpace(fromParts[0])
	outerDistinct := false
	if strings.HasPrefix(strings.ToLower(projectionText), "distinct ") {
		outerDistinct = true
		projectionText = strings.TrimSpace(projectionText[len("distinct "):])
	}
	fromTail := strings.TrimSpace(fromParts[1])
	projectionExpressions := splitTopLevelComma(projectionText)
	if len(projectionExpressions) == 0 {
		return false, nil
	}
	correlatedScalarAliases := make(map[string]struct{})
	hasScalar := false
	for _, expression := range projectionExpressions {
		if _, alias, _, ok := parseCorrelatedScalarProjectionParts(expression); ok {
			hasScalar = true
			if alias != "" {
				correlatedScalarAliases[strings.ToLower(strings.Trim(alias, "`"))] = struct{}{}
			}
		}
	}
	outerOrder := ""
	outerLimit := ""
	deferredOuterOrder := ""
	deferredOuterHaving := ""
	hiddenOrderAliases := make([]string, 0, 2)
	if orderParts := splitTopLevelKeyword(fromTail, "order by"); len(orderParts) == 2 {
		fromTail = strings.TrimSpace(orderParts[0])
		outerOrder = strings.TrimSpace(orderParts[1])
		if limitParts := splitTopLevelKeyword(outerOrder, "limit"); len(limitParts) == 2 {
			outerOrder = strings.TrimSpace(limitParts[0])
			outerLimit = strings.TrimSpace(limitParts[1])
		}
		if correlatedOrderContainsScalarAlias(outerOrder, correlatedScalarAliases) {
			deferredOuterOrder = outerOrder
			outerOrder = ""
		}
	} else if limitParts := splitTopLevelKeyword(fromTail, "limit"); len(limitParts) == 2 {
		fromTail = strings.TrimSpace(limitParts[0])
		outerLimit = strings.TrimSpace(limitParts[1])
	}
	outerTail := fromTail
	outerHaving := ""
	if havingParts := splitTopLevelKeyword(outerTail, "having"); len(havingParts) == 2 {
		outerTail = strings.TrimSpace(havingParts[0])
		outerHaving = strings.TrimSpace(havingParts[1])
	}
	outerGroupBy := ""
	if groupParts := splitTopLevelKeyword(outerTail, "group by"); len(groupParts) == 2 {
		outerTail = strings.TrimSpace(groupParts[0])
		outerGroupBy = strings.TrimSpace(groupParts[1])
	}
	outerFromParts := splitTopLevelKeyword(outerTail, "where")
	outerSource := strings.TrimSpace(outerFromParts[0])
	outerWhere := ""
	if len(outerFromParts) == 2 {
		outerWhere = strings.TrimSpace(outerFromParts[1])
	}
	if correlatedClauseContainsScalarAlias(outerHaving, correlatedScalarAliases) {
		deferredOuterHaving = outerHaving
		outerHaving = ""
	}
	deferredOuterGroupBy := ""
	if outerGroupBy != "" && (correlatedClauseContainsScalarAlias(outerGroupBy, correlatedScalarAliases) || strings.Contains(strings.ToLower(outerGroupBy), "(select")) {
		// The legacy aggregate path cannot group before a correlated scalar has
		// been evaluated. Materialize the outer rows first, then apply the
		// scalar projection and perform the grouping below.
		deferredOuterGroupBy = outerGroupBy
		outerGroupBy = ""
		if outerHaving != "" {
			deferredOuterHaving = outerHaving
			outerHaving = ""
		}
		if strings.Contains(strings.ToLower(deferredOuterHaving), "(select") {
			outerHaving = ""
		}
		if outerOrder != "" && deferredOuterOrder == "" {
			deferredOuterOrder = outerOrder
			outerOrder = ""
		}
	}
	outerTable, outerAlias, ok := parseCorrelatedOuterSource(outerSource)
	if !ok {
		return false, nil
	}
	if strings.EqualFold(outerAlias, "join") || strings.EqualFold(outerAlias, "left") || strings.EqualFold(outerAlias, "right") || strings.EqualFold(outerAlias, "inner") {
		outerAlias = ""
	}
	if outerAlias == "" {
		outerAlias = strings.Trim(strings.TrimSpace(outerTable), "`")
		if dot := strings.LastIndex(outerAlias, "."); dot >= 0 {
			outerAlias = strings.Trim(outerAlias[dot+1:], "`")
		}
	}
	// MySQL permits a correlated scalar subquery directly in HAVING without
	// exposing it in the SELECT list. Materialize each such scalar as a hidden
	// projection so the grouped outer rows can be filtered after per-row
	// correlation has been evaluated.
	if deferredOuterHaving == "" && strings.Contains(strings.ToLower(outerHaving), "(select") {
		ranges := correlatedScalarProjectionRanges(outerHaving)
		if len(ranges) == 0 {
			return false, nil
		}
		for index := len(ranges) - 1; index >= 0; index-- {
			innerSQL := ranges[index].inner
			if !strings.Contains(strings.ToLower(innerSQL), strings.ToLower(outerAlias)+".") {
				return false, nil
			}
			alias := fmt.Sprintf("__correlated_having_%d__", index)
			projectionExpressions = append(projectionExpressions, "("+innerSQL+") AS "+alias)
			hiddenOrderAliases = append(hiddenOrderAliases, alias)
		}
		deferredOuterHaving = outerHaving
		outerHaving = ""
		hasScalar = true
	}
	// A correlated scalar may be used directly in ORDER BY without being
	// projected. Materialize it as a hidden per-row scalar, sort on its alias,
	// then remove that helper column before returning the result set. DISTINCT
	// is intentionally left on the existing path because its duplicate
	// semantics depend on the visible projection only.
	if deferredOuterOrder == "" && !outerDistinct && strings.Contains(strings.ToLower(outerOrder), "(select") {
		ranges := correlatedScalarProjectionRanges(outerOrder)
		transformedOrder := outerOrder
		for index := len(ranges) - 1; index >= 0; index-- {
			innerSQL := ranges[index].inner
			if !strings.Contains(strings.ToLower(innerSQL), strings.ToLower(outerAlias)+".") {
				return false, nil
			}
			alias := fmt.Sprintf("__correlated_order_%d__", index)
			projectionExpressions = append(projectionExpressions, "("+innerSQL+") AS "+alias)
			transformedOrder = transformedOrder[:ranges[index].start] + alias + transformedOrder[ranges[index].end+1:]
			hiddenOrderAliases = append(hiddenOrderAliases, alias)
		}
		if len(ranges) > 0 && len(hiddenOrderAliases) > 0 {
			deferredOuterOrder = transformedOrder
			outerOrder = ""
			hasScalar = true
		}
	}
	deferredOuterWherePredicates := make([]correlatedScalarWherePredicate, 0, 2)
	outerWhereForSource := outerWhere
	if residual, predicates, found := splitCorrelatedScalarWhere(outerWhere, outerAlias); found {
		deferredOuterWherePredicates = predicates
		outerWhereForSource = residual
	}
	if !hasScalar && len(deferredOuterWherePredicates) == 0 {
		return false, nil
	}
	directOuterProjection := make([]string, 0, len(projectionExpressions))
	innerSources := make([]string, 0, len(projectionExpressions))
	for _, expression := range projectionExpressions {
		innerSQLs, _, directExpression, scalar := parseCorrelatedScalarProjectionParts(expression)
		if scalar {
			innerSources = append(innerSources, innerSQLs...)
			continue
		}
		if strings.TrimSpace(directExpression) != "" {
			directOuterProjection = append(directOuterProjection, directExpression)
		}
	}
	outerProjectionForSource := strings.Join(directOuterProjection, ", ")
	if deferredOuterGroupBy != "" {
		outerProjectionForSource = "*"
	}
	if outerProjectionForSource == "" {
		outerProjectionForSource = "*"
	}
	outerDependencies := strings.Join([]string{outerWhere, outerGroupBy, outerHaving, deferredOuterHaving}, " ")
	outerSQL := correlatedOuterSourceSQL(outerProjectionForSource, outerDependencies, strings.Join(innerSources, " "), outerSource)
	if outerWhereForSource != "" {
		outerSQL += " where " + outerWhereForSource
	}
	if outerGroupBy != "" {
		outerSQL += " group by " + outerGroupBy
	}
	if outerHaving != "" {
		outerSQL += " having " + outerHaving
	}
	if outerOrder != "" {
		outerSQL += " order by " + outerOrder
	}
	if outerLimit != "" && !outerDistinct && deferredOuterOrder == "" && len(deferredOuterWherePredicates) == 0 {
		outerSQL += " limit " + outerLimit
	}
	outerStmt, err := parseSelectSQL(outerSQL)
	if err != nil {
		return true, err
	}
	var outerResult *SelectResult
	if strings.HasPrefix(strings.TrimSpace(outerSource), "(") {
		outerResult, err = e.executeDerivedTableSelect(ctx, outerStmt, databaseName)
	} else {
		outerResult, err = e.executeSelectStatement(ctx, outerStmt, databaseName)
	}
	if err != nil {
		return true, err
	}

	parsedProjections := make([]correlatedScalarProjection, 0, len(projectionExpressions))
	for _, expression := range projectionExpressions {
		innerSQLs, alias, directExpression, scalar := parseCorrelatedScalarProjectionParts(expression)
		if scalar {
			for _, innerSQL := range innerSQLs {
				innerFrom := splitTopLevelKeyword(innerSQL, "from")
				if len(innerFrom) != 2 {
					return true, fmt.Errorf("correlated scalar subquery requires a FROM clause")
				}
				innerWhere := splitTopLevelKeyword(innerFrom[1], "where")
				if len(innerWhere) != 2 || !strings.Contains(strings.ToLower(innerWhere[1]), strings.ToLower(outerAlias)+".") {
					// Leave uncorrelated scalars to rewriteSimpleScalarSubqueries so
					// this handler does not change its normal projection metadata.
					return false, nil
				}
			}
			parsedProjections = append(parsedProjections, correlatedScalarProjection{innerSQLs: innerSQLs, alias: alias, directExpression: directExpression, scalar: true})
			continue
		}
		parsedProjections = append(parsedProjections, correlatedScalarProjection{directExpression: directExpression, alias: alias})
	}

	meta := &metadata.TableMeta{Name: "correlated_scalar", Columns: make([]*metadata.ColumnMeta, 0, len(parsedProjections))}
	for _, projection := range parsedProjections {
		name := projection.alias
		if name == "" {
			name = projection.directExpression
		}
		meta.Columns = append(meta.Columns, &metadata.ColumnMeta{Name: name, Type: metadata.TypeVarchar})
	}
	records := make([]Record, 0, len(outerResult.Records))
	rawRows := make([]map[string]interface{}, 0, len(outerResult.Records))
	for _, outerRecord := range outerResult.Records {
		rowValues := correlatedOuterRowValues(outerRecord, outerResult.Columns, outerResult.ColumnTypes, outerAlias, outerTable)
		if len(deferredOuterWherePredicates) > 0 {
			matchedAll := true
			for _, predicate := range deferredOuterWherePredicates {
				matched, predicateErr := e.evaluateCorrelatedScalarPredicate(ctx, predicate.predicate, predicate.innerSQLs, outerRecord, rowValues, outerResult, outerAlias, databaseName)
				if predicateErr != nil {
					return true, predicateErr
				}
				if !matched {
					matchedAll = false
					break
				}
			}
			if !matchedAll {
				continue
			}
		}
		values := make([]basic.Value, 0, len(parsedProjections))
		for _, projection := range parsedProjections {
			if !projection.scalar {
				value, err := evaluateProjectionExpression(projection.directExpression, rowValues)
				if err != nil {
					return true, err
				}
				values = append(values, correlatedExpressionValue(value))
				continue
			}
			expressionValues := make(map[string]interface{}, len(rowValues)+len(projection.innerSQLs)*2)
			for key, value := range rowValues {
				expressionValues[key] = value
			}
			for scalarIndex, innerSQLSource := range projection.innerSQLs {
				innerSQL := innerSQLSource
				for index, column := range outerResult.Columns {
					value, exists := correlatedOuterColumnValue(rowValues, column)
					if !exists && index < len(outerRecord.GetValues()) {
						value = correlatedBasicValueInterface(outerRecord.GetValues()[index], index, outerResult.ColumnTypes)
						exists = true
					}
					if !exists {
						continue
					}
					literal := correlatedInterfaceSQLLiteral(value)
					pattern := regexp.MustCompile(`(?i)` + regexp.QuoteMeta(outerAlias) + `\s*\.\s*` + regexp.QuoteMeta(strings.Trim(column, "`")))
					innerSQL = pattern.ReplaceAllString(innerSQL, literal)
				}
				innerSQL = normalizeCorrelatedInnerAlias(innerSQL)
				innerStmt, err := parseSelectSQL(innerSQL)
				if err != nil {
					return true, err
				}
				innerResult, err := e.executeSelectStatement(ctx, innerStmt, databaseName)
				if err != nil {
					return true, err
				}
				if len(innerResult.Columns) != 1 {
					return true, fmt.Errorf("correlated scalar subquery returns %d columns", len(innerResult.Columns))
				}
				if len(innerResult.Records) > 1 {
					return true, fmt.Errorf("subquery returns more than 1 row")
				}
				var scalarValue basic.Value
				if len(innerResult.Records) == 0 || len(innerResult.Records[0].GetValues()) == 0 {
					if strings.Contains(strings.ToLower(innerSQL), "count(") {
						scalarValue = basic.NewInt64(0)
					} else {
						scalarValue = basic.NewNull()
					}
				} else {
					scalarValue = innerResult.Records[0].GetValues()[0]
				}
				var raw interface{}
				if scalarValue != nil {
					raw = basicValueToInterface(scalarValue)
				}
				expressionValues[fmt.Sprintf("__correlated_scalar_%d__", scalarIndex)] = raw
			}
			if projection.directExpression == "" {
				values = append(values, correlatedExpressionValue(expressionValues["__correlated_scalar_0__"]))
				continue
			}
			value, evalErr := evaluateProjectionExpression(projection.directExpression, expressionValues)
			if evalErr != nil {
				return true, evalErr
			}
			value = derivedNormalizeValue(value, "BIGINT")
			values = append(values, correlatedExpressionValue(value))
		}
		records = append(records, NewExecutorRecord(values, meta))
		rawRows = append(rawRows, rowValues)
	}
	if deferredOuterGroupBy != "" {
		records, rawRows, err = groupCorrelatedScalarRecords(records, rawRows, deferredOuterGroupBy, parsedProjections, meta)
		if err != nil {
			return true, err
		}
	}
	if deferredOuterHaving != "" {
		records, err = filterCorrelatedRecordsByHaving(records, deferredOuterHaving, meta, parsedProjections)
		if err != nil {
			return true, err
		}
	}
	if outerDistinct {
		records = correlatedDistinctRecords(records)
	}
	if deferredOuterOrder != "" {
		if err := sortCorrelatedRecords(records, deferredOuterOrder, meta); err != nil {
			return true, err
		}
	}
	if outerLimit != "" && (outerDistinct || deferredOuterOrder != "" || len(deferredOuterWherePredicates) > 0) {
		records, err = correlatedLimitRecords(records, outerLimit)
		if err != nil {
			return true, err
		}
	}
	if len(hiddenOrderAliases) > 0 {
		hidden := make(map[string]struct{}, len(hiddenOrderAliases))
		for _, alias := range hiddenOrderAliases {
			hidden[strings.ToLower(alias)] = struct{}{}
		}
		visibleColumns := make([]*metadata.ColumnMeta, 0, len(meta.Columns)-len(hidden))
		visibleIndexes := make([]int, 0, len(meta.Columns)-len(hidden))
		for index, column := range meta.Columns {
			if column == nil {
				continue
			}
			if _, exists := hidden[strings.ToLower(strings.Trim(column.Name, "`"))]; exists {
				continue
			}
			visibleColumns = append(visibleColumns, column)
			visibleIndexes = append(visibleIndexes, index)
		}
		visibleMeta := &metadata.TableMeta{Name: meta.Name, Columns: visibleColumns}
		visibleRecords := make([]Record, 0, len(records))
		for _, record := range records {
			values := record.GetValues()
			projected := make([]basic.Value, 0, len(visibleIndexes))
			for _, index := range visibleIndexes {
				if index < len(values) {
					projected = append(projected, values[index])
				}
			}
			visibleRecords = append(visibleRecords, NewExecutorRecord(projected, visibleMeta))
		}
		meta = visibleMeta
		records = visibleRecords
	}
	columns := make([]string, 0, len(meta.Columns))
	for _, column := range meta.Columns {
		columns = append(columns, column.Name)
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: &SelectResult{Records: records, RowCount: len(records), Columns: columns, ColumnTypes: correlatedProjectionTypes(records, meta), ResultType: common.RESULT_TYPE_QUERY}, Message: fmt.Sprintf("correlated scalar query returned %d rows", len(records))}
	return true, nil
}

// correlatedScalarWherePredicate is one scalar subquery comparison deferred
// until the outer row is available.
type correlatedScalarWherePredicate struct {
	predicate string
	innerSQLs []string
}

// splitCorrelatedScalarWhere extracts correlated scalar predicates from an
// outer WHERE clause. AND terms without a scalar remain in the outer
// execution; a term containing a scalar keeps its complete boolean expression
// so the normal SQL three-valued evaluator can preserve OR semantics.
func splitCorrelatedScalarWhere(where, outerAlias string) (residual string, predicates []correlatedScalarWherePredicate, found bool) {
	where = strings.TrimSpace(where)
	if where == "" {
		return "", nil, false
	}
	terms := make([]string, 0, 2)
	if len(splitTopLevelKeyword(where, "or")) == 2 {
		// Keep top-level OR with the scalar term intact; splitting an AND first
		// would change (a AND scalar) OR b into a AND (scalar OR b).
		terms = append(terms, where)
	} else {
		remaining := where
		for {
			parts := splitTopLevelKeyword(remaining, "and")
			terms = append(terms, strings.TrimSpace(parts[0]))
			if len(parts) != 2 {
				break
			}
			remaining = strings.TrimSpace(parts[1])
		}
	}
	residualTerms := make([]string, 0, len(terms))
	for _, term := range terms {
		if !strings.Contains(strings.ToLower(term), "(select") {
			if term != "" {
				residualTerms = append(residualTerms, term)
			}
			continue
		}
		ranges := correlatedScalarProjectionRanges(term)
		if len(ranges) == 0 {
			return "", nil, false
		}
		innerSQLs := make([]string, 0, len(ranges))
		for _, value := range ranges {
			prefix := strings.ToLower(strings.TrimSpace(term[:value.start]))
			if strings.HasSuffix(prefix, "exists") || strings.HasSuffix(prefix, "in") || strings.HasSuffix(prefix, "any") || strings.HasSuffix(prefix, "some") || strings.HasSuffix(prefix, "all") {
				return "", nil, false
			}
			innerSQL := strings.TrimSpace(value.inner)
			if !strings.Contains(strings.ToLower(innerSQL), strings.ToLower(outerAlias)+".") {
				return "", nil, false
			}
			innerSQLs = append(innerSQLs, innerSQL)
		}
		predicates = append(predicates, correlatedScalarWherePredicate{predicate: term, innerSQLs: innerSQLs})
	}
	if len(predicates) == 0 {
		return "", nil, false
	}
	return strings.Join(residualTerms, " and "), predicates, true
}

func (e *XMySQLExecutor) evaluateCorrelatedScalarPredicate(ctx *ExecutionContext, predicate string, innerSQLs []string, outerRecord Record, rowValues map[string]interface{}, outerResult *SelectResult, outerAlias, databaseName string) (bool, error) {
	ranges := correlatedScalarProjectionRanges(predicate)
	if len(ranges) != len(innerSQLs) {
		return false, fmt.Errorf("correlated scalar predicate subquery count mismatch")
	}
	literals := make([]string, len(innerSQLs))
	for scalarIndex, innerSQL := range innerSQLs {
		boundInnerSQL := innerSQL
		for index, column := range outerResult.Columns {
			if index >= len(outerRecord.GetValues()) {
				continue
			}
			value, exists := correlatedOuterColumnValue(rowValues, column)
			if !exists {
				value = correlatedBasicValueInterface(outerRecord.GetValues()[index], index, outerResult.ColumnTypes)
			}
			pattern := regexp.MustCompile(`(?i)` + regexp.QuoteMeta(outerAlias) + `\s*\.\s*` + regexp.QuoteMeta(strings.Trim(column, "`")))
			boundInnerSQL = pattern.ReplaceAllString(boundInnerSQL, correlatedInterfaceSQLLiteral(value))
		}
		boundInnerSQL = normalizeCorrelatedInnerAlias(boundInnerSQL)
		innerStmt, err := parseSelectSQL(boundInnerSQL)
		if err != nil {
			return false, err
		}
		innerResult, err := e.executeSelectStatement(ctx, innerStmt, databaseName)
		if err != nil {
			return false, err
		}
		if len(innerResult.Columns) != 1 {
			return false, fmt.Errorf("correlated scalar predicate returns %d columns", len(innerResult.Columns))
		}
		if len(innerResult.Records) > 1 {
			return false, fmt.Errorf("subquery returns more than 1 row")
		}
		var scalar interface{}
		if len(innerResult.Records) == 0 || len(innerResult.Records[0].GetValues()) == 0 || innerResult.Records[0].GetValues()[0] == nil || innerResult.Records[0].GetValues()[0].IsNull() {
			if strings.Contains(strings.ToLower(innerSQL), "count(") {
				scalar = int64(0)
			}
		} else {
			scalar = basicValueToInterface(innerResult.Records[0].GetValues()[0])
		}
		literals[scalarIndex] = correlatedInterfaceSQLLiteral(scalar)
	}
	rewritten := predicate
	for index := len(ranges) - 1; index >= 0; index-- {
		rangeValue := ranges[index]
		rewritten = rewritten[:rangeValue.start] + literals[index] + rewritten[rangeValue.end+1:]
	}
	stmt, err := parseSelectSQL("select 1 where " + rewritten)
	if err != nil {
		return false, err
	}
	if stmt.Where == nil {
		return false, fmt.Errorf("correlated scalar predicate has no WHERE expression")
	}
	return evalPredicate(stmt.Where.Expr, rowValues)
}

func correlatedLimitRecords(records []Record, limitClause string) ([]Record, error) {
	stmt, err := parseSelectSQL("select 1 limit " + strings.TrimSpace(limitClause))
	if err != nil {
		return nil, err
	}
	start, end := derivedLimitBounds(stmt.Limit, len(records))
	return records[start:end], nil
}

func correlatedDistinctRecords(records []Record) []Record {
	seen := make(map[string]struct{}, len(records))
	unique := make([]Record, 0, len(records))
	for _, record := range records {
		parts := make([]string, 0, len(record.GetValues()))
		for _, value := range record.GetValues() {
			if value == nil || value.IsNull() {
				parts = append(parts, "<null>")
				continue
			}
			parts = append(parts, fmt.Sprintf("%T:%v", basicValueToInterface(value), basicValueToInterface(value)))
		}
		key := strings.Join(parts, "\x00")
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, record)
	}
	return unique
}

func correlatedOrderContainsScalarAlias(orderClause string, aliases map[string]struct{}) bool {
	return correlatedClauseContainsScalarAlias(orderClause, aliases)
}

func correlatedClauseContainsScalarAlias(clause string, aliases map[string]struct{}) bool {
	if strings.TrimSpace(clause) == "" || len(aliases) == 0 {
		return false
	}
	for alias := range aliases {
		pattern := regexp.MustCompile(`(?i)(^|[^a-zA-Z0-9_$])` + regexp.QuoteMeta(alias) + `([^a-zA-Z0-9_$]|$)`)
		if pattern.MatchString(clause) {
			return true
		}
	}
	return false
}

func sortCorrelatedRecords(records []Record, orderClause string, meta *metadata.TableMeta) error {
	type orderTerm struct {
		index      int
		expression string
		desc       bool
	}
	terms := splitTopLevelComma(orderClause)
	orderTerms := make([]orderTerm, 0, len(terms))
	for _, term := range terms {
		term = strings.TrimSpace(term)
		desc := regexp.MustCompile(`(?i)\s+desc\s*$`).MatchString(term)
		term = regexp.MustCompile(`(?i)\s+(?:asc|desc)\s*$`).ReplaceAllString(term, "")
		term = strings.TrimSpace(term)
		columnName := strings.Trim(term, "`")
		if dot := strings.LastIndex(columnName, "."); dot >= 0 {
			columnName = strings.Trim(columnName[dot+1:], "`")
		}
		index := -1
		for candidate, column := range meta.Columns {
			if column != nil && strings.EqualFold(correlatedOrderColumnName(column.Name), columnName) {
				index = candidate
				break
			}
		}
		if index < 0 {
			if !regexp.MustCompile(`(?i)^[a-zA-Z0-9_$]+(?:\.[a-zA-Z0-9_$]+)?$`).MatchString(columnName) {
				orderTerms = append(orderTerms, orderTerm{index: -1, expression: term, desc: desc})
				continue
			}
			return fmt.Errorf("correlated scalar ORDER BY column %s not found", term)
		}
		orderTerms = append(orderTerms, orderTerm{index: index, desc: desc})
	}
	orderValues := make([][]interface{}, len(records))
	for recordIndex, record := range records {
		rowValues := correlatedRecordValueMap(record, meta)
		orderValues[recordIndex] = make([]interface{}, len(orderTerms))
		for termIndex, term := range orderTerms {
			if term.index >= 0 {
				if term.index < len(record.GetValues()) && record.GetValues()[term.index] != nil && !record.GetValues()[term.index].IsNull() {
					orderValues[recordIndex][termIndex] = basicValueToInterface(record.GetValues()[term.index])
				}
				continue
			}
			value, err := evaluateProjectionExpression(term.expression, rowValues)
			if err != nil {
				return err
			}
			orderValues[recordIndex][termIndex] = value
		}
	}
	type correlatedOrderRecord struct {
		record Record
		values []interface{}
	}
	ordered := make([]correlatedOrderRecord, len(records))
	for index, record := range records {
		ordered[index] = correlatedOrderRecord{record: record, values: orderValues[index]}
	}
	sort.SliceStable(ordered, func(left, right int) bool {
		for termIndex, term := range orderTerms {
			comparison := compareCorrelatedOrderInterfaces(ordered[left].values[termIndex], ordered[right].values[termIndex])
			if comparison == 0 {
				continue
			}
			if term.desc {
				return comparison > 0
			}
			return comparison < 0
		}
		return false
	})
	for index, item := range ordered {
		records[index] = item.record
	}
	return nil
}

func correlatedOrderColumnName(name string) string {
	name = strings.Trim(strings.TrimSpace(name), "`")
	if dot := strings.LastIndex(name, "."); dot >= 0 {
		name = strings.Trim(name[dot+1:], "`")
	}
	return name
}

func compareCorrelatedOrderValues(left, right basic.Value) int {
	var leftValue, rightValue interface{}
	if left != nil && !left.IsNull() {
		leftValue = basicValueToInterface(left)
	}
	if right != nil && !right.IsNull() {
		rightValue = basicValueToInterface(right)
	}
	return compareCorrelatedOrderInterfaces(leftValue, rightValue)
}

func compareCorrelatedOrderInterfaces(leftValue, rightValue interface{}) int {
	if leftValue == nil && rightValue == nil {
		return 0
	}
	if leftValue == nil {
		return -1
	}
	if rightValue == nil {
		return 1
	}
	if leftText, ok := leftValue.(string); ok {
		if rightText, ok := rightValue.(string); ok {
			if leftNumber, leftErr := strconv.ParseFloat(strings.TrimSpace(leftText), 64); leftErr == nil {
				if rightNumber, rightErr := strconv.ParseFloat(strings.TrimSpace(rightText), 64); rightErr == nil {
					return compareFloatValues(leftNumber, rightNumber)
				}
			}
		}
	}
	return compareScalarValues(leftValue, rightValue)
}

func correlatedRecordValueMap(record Record, meta *metadata.TableMeta) map[string]interface{} {
	values := make(map[string]interface{}, len(meta.Columns)*3)
	for index, column := range meta.Columns {
		if column == nil || index >= len(record.GetValues()) {
			continue
		}
		value := record.GetValues()[index]
		var raw interface{}
		if value != nil && !value.IsNull() {
			raw = basicValueToInterface(value)
		}
		name := strings.Trim(column.Name, "`")
		values[name] = raw
		values[strings.ToLower(name)] = raw
		shortName := correlatedOrderColumnName(name)
		values[shortName] = raw
		values[strings.ToLower(shortName)] = raw
	}
	return values
}

func filterCorrelatedRecordsByHaving(records []Record, having string, meta *metadata.TableMeta, projections []correlatedScalarProjection) ([]Record, error) {
	filtered := make([]Record, 0, len(records))
	for _, record := range records {
		values := correlatedRecordValueMap(record, meta)
		rewrittenHaving := having
		if ranges := correlatedScalarProjectionRanges(having); len(ranges) > 0 {
			for index := len(ranges) - 1; index >= 0; index-- {
				value, ok := correlatedScalarProjectionValueForInner(record, ranges[index].inner, projections)
				if !ok {
					return nil, fmt.Errorf("correlated scalar HAVING expression is not projected")
				}
				rewrittenHaving = rewrittenHaving[:ranges[index].start] + correlatedInterfaceSQLLiteral(value) + rewrittenHaving[ranges[index].end+1:]
			}
		}
		stmt, err := parseSelectSQL("select * from correlated_scalar where " + strings.TrimSpace(rewrittenHaving))
		if err != nil {
			return nil, err
		}
		if stmt.Where == nil {
			return nil, fmt.Errorf("invalid correlated scalar HAVING clause")
		}
		matched, evalErr := evalPredicate(stmt.Where.Expr, values)
		if evalErr != nil {
			return nil, evalErr
		}
		if matched {
			filtered = append(filtered, record)
		}
	}
	return filtered, nil
}

func correlatedScalarProjectionValueForInner(record Record, innerSQL string, projections []correlatedScalarProjection) (interface{}, bool) {
	for projectionIndex, projection := range projections {
		for _, projectedInnerSQL := range projection.innerSQLs {
			if normalizeCorrelatedGroupingExpression(projectedInnerSQL) != normalizeCorrelatedGroupingExpression(innerSQL) {
				continue
			}
			if projectionIndex >= len(record.GetValues()) {
				return nil, false
			}
			return basicValueToInterface(record.GetValues()[projectionIndex]), true
		}
	}
	return nil, false
}

// groupCorrelatedScalarRecords applies the grouping stage after correlated
// scalar values have been materialized. This is needed for queries such as
// SELECT (SELECT MAX(... WHERE ... = p.id)) AS max_score, COUNT(*)
// FROM parents p GROUP BY max_score.
func groupCorrelatedScalarRecords(records []Record, rawRows []map[string]interface{}, groupBy string, projections []correlatedScalarProjection, meta *metadata.TableMeta) ([]Record, []map[string]interface{}, error) {
	terms := splitTopLevelComma(groupBy)
	if len(terms) == 0 || len(records) != len(rawRows) {
		return nil, nil, fmt.Errorf("invalid correlated scalar GROUP BY input")
	}
	type group struct {
		key      string
		indexes  []int
		firstRow map[string]interface{}
	}
	groups := make([]group, 0, len(records))
	byKey := make(map[string]int, len(records))
	for index, record := range records {
		values := correlatedRecordValueMap(record, meta)
		parts := make([]string, 0, len(terms))
		for _, term := range terms {
			term = strings.TrimSpace(term)
			value, exists := correlatedOuterColumnValue(values, term)
			if !exists && strings.Contains(strings.ToLower(term), "(select") {
				value, exists = correlatedScalarGroupingValue(term, record, projections)
			}
			if !exists {
				var err error
				value, err = evaluateProjectionExpression(term, values)
				if err != nil {
					return nil, nil, err
				}
			}
			parts = append(parts, fmt.Sprintf("%T:%v", value, value))
		}
		key := strings.Join(parts, "\x00")
		groupIndex, exists := byKey[key]
		if !exists {
			groupIndex = len(groups)
			byKey[key] = groupIndex
			groups = append(groups, group{key: key, firstRow: rawRows[index]})
		}
		groups[groupIndex].indexes = append(groups[groupIndex].indexes, index)
	}

	grouped := make([]Record, 0, len(groups))
	groupedRawRows := make([]map[string]interface{}, 0, len(groups))
	for _, current := range groups {
		if len(current.indexes) == 0 {
			continue
		}
		values := make([]basic.Value, 0, len(projections))
		first := current.indexes[0]
		for projectionIndex, projection := range projections {
			var value interface{}
			if projection.scalar {
				if projectionIndex < len(records[first].GetValues()) {
					value = basicValueToInterface(records[first].GetValues()[projectionIndex])
				}
			} else if functionName, argument, ok := correlatedAggregateExpression(projection.directExpression); ok {
				accumulator := aggregateAccumulator{fn: functionName}
				for _, rowIndex := range current.indexes {
					if functionName == "COUNT" && strings.TrimSpace(argument) == "*" {
						accumulator.add(int64(1))
						continue
					}
					rowValue, evalErr := evaluateProjectionExpression(argument, rawRows[rowIndex])
					if evalErr != nil {
						return nil, nil, evalErr
					}
					accumulator.add(rowValue)
				}
				value = accumulator.value()
			} else if projectionIndex < len(records[first].GetValues()) {
				value = basicValueToInterface(records[first].GetValues()[projectionIndex])
			}
			values = append(values, correlatedExpressionValue(value))
		}
		grouped = append(grouped, NewExecutorRecord(values, meta))
		groupedRawRows = append(groupedRawRows, current.firstRow)
	}
	return grouped, groupedRawRows, nil
}

func correlatedScalarGroupingValue(term string, record Record, projections []correlatedScalarProjection) (interface{}, bool) {
	if record == nil {
		return nil, false
	}
	normalizedTerm := normalizeCorrelatedGroupingExpression(term)
	for projectionIndex, projection := range projections {
		if len(projection.innerSQLs) == 0 {
			continue
		}
		expression := projection.directExpression
		if expression == "" && len(projection.innerSQLs) == 1 {
			expression = "__correlated_scalar_0__"
		}
		for index, innerSQL := range projection.innerSQLs {
			placeholder := fmt.Sprintf("__correlated_scalar_%d__", index)
			expression = strings.ReplaceAll(expression, placeholder, "("+innerSQL+")")
		}
		if normalizeCorrelatedGroupingExpression(expression) != normalizedTerm {
			continue
		}
		if projectionIndex >= len(record.GetValues()) {
			return nil, false
		}
		return basicValueToInterface(record.GetValues()[projectionIndex]), true
	}
	return nil, false
}

func normalizeCorrelatedGroupingExpression(expression string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(expression))), "")
}

func correlatedAggregateExpression(expression string) (functionName, argument string, ok bool) {
	match := regexp.MustCompile(`(?is)^\s*(count|sum|avg|min|max)\s*\((.*)\)\s*$`).FindStringSubmatch(strings.TrimSpace(expression))
	if len(match) != 3 {
		return "", "", false
	}
	return strings.ToUpper(match[1]), strings.TrimSpace(match[2]), true
}

// parseCorrelatedOuterSource accepts the ordinary table source plus the
// bounded derived-table shape used by the compatibility executor.  The
// derived query is still parsed and executed by the normal SELECT path; this
// helper only extracts its correlation alias.
func parseCorrelatedOuterSource(source string) (table, alias string, ok bool) {
	source = strings.TrimSpace(source)
	if source == "" {
		return "", "", false
	}
	if strings.HasPrefix(source, "(") {
		close := matchingParenIndex(source, 0)
		if close < 0 {
			return "", "", false
		}
		tail := strings.TrimSpace(source[close+1:])
		aliasMatch := regexp.MustCompile(`(?is)^(?:as\s+)?([a-zA-Z0-9_$]+)$`).FindStringSubmatch(tail)
		if len(aliasMatch) != 2 {
			return "", "", false
		}
		alias = strings.Trim(aliasMatch[1], "`")
		return alias, alias, alias != ""
	}
	match := regexp.MustCompile("(?is)^([a-zA-Z0-9_$]+(?:\\.[a-zA-Z0-9_$]+)?)(?:\\s+(?:as\\s+)?([a-zA-Z0-9_$]+))?").FindStringSubmatch(source)
	if len(match) == 0 {
		return "", "", false
	}
	table = strings.TrimSpace(match[1])
	alias = strings.Trim(strings.TrimSpace(match[2]), "`")
	if alias == "" {
		alias = strings.Trim(table, "`")
		if dot := strings.LastIndex(alias, "."); dot >= 0 {
			alias = strings.Trim(alias[dot+1:], "`")
		}
	}
	return table, alias, alias != ""
}

func normalizeCorrelatedInnerAlias(query string) string {
	match := regexp.MustCompile(`(?is)\bfrom\s+` + "`?[a-zA-Z0-9_$]+`?" + `\s+` + "`?([a-zA-Z0-9_$]+)`?" + `\s+where\b`).FindStringSubmatch(query)
	if len(match) != 2 {
		return query
	}
	return regexp.MustCompile(`(?i)`+regexp.QuoteMeta(strings.Trim(match[1], "`"))+`\s*\.\s*`).ReplaceAllString(query, "")
}

type correlatedScalarProjection struct {
	innerSQLs        []string
	directExpression string
	alias            string
	scalar           bool
}

func parseCorrelatedScalarProjection(expression string) (innerSQL, alias, direct string, scalar bool) {
	innerSQLs, alias, direct, scalar := parseCorrelatedScalarProjectionParts(expression)
	if len(innerSQLs) == 0 {
		return "", alias, direct, scalar
	}
	return innerSQLs[0], alias, direct, scalar
}

func parseCorrelatedScalarProjectionParts(expression string) (innerSQLs []string, alias, direct string, scalar bool) {
	trimmed := strings.TrimSpace(expression)
	ranges := correlatedScalarProjectionRanges(trimmed)
	if len(ranges) > 0 {
		base := trimmed
		upper := strings.ToUpper(trimmed)
		if index := strings.LastIndex(upper, " AS "); index >= 0 {
			alias = strings.Trim(strings.TrimSpace(trimmed[index+4:]), "`")
			base = strings.TrimSpace(trimmed[:index])
		}
		for index := len(ranges) - 1; index >= 0; index-- {
			rangeValue := ranges[index]
			placeholder := fmt.Sprintf("__correlated_scalar_%d__", index)
			base = base[:rangeValue.start] + placeholder + base[rangeValue.end+1:]
		}
		innerSQLs = make([]string, len(ranges))
		for index, rangeValue := range ranges {
			innerSQLs[index] = rangeValue.inner
		}
		if len(ranges) == 1 && strings.TrimSpace(base) == "__correlated_scalar_0__" {
			return innerSQLs, alias, "", true
		}
		return innerSQLs, alias, base, true
	}
	upper := strings.ToUpper(trimmed)
	if index := strings.LastIndex(upper, " AS "); index >= 0 {
		return nil, strings.Trim(strings.TrimSpace(trimmed[index+4:]), "`"), strings.TrimSpace(trimmed[:index]), false
	}
	return nil, "", trimmed, false
}

type correlatedScalarProjectionRangeValue struct {
	start int
	end   int
	inner string
}

func correlatedScalarProjectionRanges(expression string) []correlatedScalarProjectionRangeValue {
	ranges := make([]correlatedScalarProjectionRangeValue, 0, 2)
	for index := 0; index < len(expression); index++ {
		if expression[index] != '(' {
			continue
		}
		close := matchingParenIndex(expression, index)
		if close < 0 {
			break
		}
		inner := strings.TrimSpace(expression[index+1 : close])
		if strings.HasPrefix(strings.ToLower(inner), "select ") {
			ranges = append(ranges, correlatedScalarProjectionRangeValue{start: index, end: close, inner: inner})
			index = close
		}
	}
	return ranges
}

func parseSelectSQL(query string) (*sqlparser.Select, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("expected SELECT, got %T", stmt)
	}
	return selectStmt, nil
}

func correlatedOuterRowValues(record Record, columns, columnTypes []string, alias, table string) map[string]interface{} {
	values := make(map[string]interface{}, len(columns)*3)
	for index, column := range columns {
		if index >= len(record.GetValues()) {
			continue
		}
		value := record.GetValues()[index]
		var raw interface{}
		if value != nil && !value.IsNull() {
			// Raw() exposes the encoded storage bytes for integer basic.Values.
			// Correlated substitution needs the SQL scalar value so grouped or
			// aggregated outer rows bind numeric columns correctly.
			raw = correlatedBasicValueInterface(value, index, columnTypes)
		}
		clean := strings.Trim(column, "`")
		values[clean] = raw
		values[strings.ToLower(clean)] = raw
		values[alias+"."+clean] = raw
		values[strings.ToLower(alias+"."+clean)] = raw
		values[table+"."+clean] = raw
	}
	return values
}

func correlatedOuterColumnValue(values map[string]interface{}, column string) (interface{}, bool) {
	clean := strings.Trim(column, "`")
	if value, ok := values[clean]; ok {
		return value, true
	}
	if value, ok := values[strings.ToLower(clean)]; ok {
		return value, true
	}
	if dot := strings.LastIndex(clean, "."); dot >= 0 {
		return correlatedOuterColumnValue(values, clean[dot+1:])
	}
	return nil, false
}

func correlatedBasicValueInterface(value basic.Value, index int, columnTypes []string) interface{} {
	if value == nil || value.IsNull() {
		return nil
	}
	if index >= 0 && index < len(columnTypes) {
		typeName := strings.ToUpper(strings.TrimSpace(columnTypes[index]))
		switch typeName {
		case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "YEAR":
			if raw, ok := value.Raw().([]byte); ok && len(raw) == 8 && value.Type() == basic.ValueTypeVarchar {
				return int64(binary.BigEndian.Uint64(raw))
			}
			if parsed, err := strconv.ParseInt(strings.TrimSpace(value.String()), 10, 64); err == nil {
				return parsed
			}
		}
	}
	return basicValueToInterface(value)
}

func correlatedInterfaceSQLLiteral(value interface{}) string {
	switch typed := value.(type) {
	case nil:
		return "NULL"
	case int:
		return strconv.Itoa(typed)
	case int8:
		return strconv.FormatInt(int64(typed), 10)
	case int16:
		return strconv.FormatInt(int64(typed), 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case int64:
		return strconv.FormatInt(typed, 10)
	case uint:
		return strconv.FormatUint(uint64(typed), 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	case float32:
		return strconv.FormatFloat(float64(typed), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(typed, 'g', -1, 64)
	case []byte:
		return "'" + strings.ReplaceAll(string(typed), "'", "''") + "'"
	case string:
		if _, err := strconv.ParseInt(strings.TrimSpace(typed), 10, 64); err == nil {
			return strings.TrimSpace(typed)
		}
		return "'" + strings.ReplaceAll(typed, "'", "''") + "'"
	default:
		return "'" + strings.ReplaceAll(fmt.Sprint(value), "'", "''") + "'"
	}
}

func correlatedInterfaceValue(value interface{}) basic.Value {
	if value == nil {
		return basic.NewNull()
	}
	if existing, ok := value.(basic.Value); ok {
		return correlatedInterfaceValue(basicValueToInterface(existing))
	}
	switch typed := value.(type) {
	case int:
		return basic.NewInt64Value(int64(typed))
	case int8:
		return basic.NewInt64Value(int64(typed))
	case int16:
		return basic.NewInt64Value(int64(typed))
	case int32:
		return basic.NewInt64Value(int64(typed))
	case int64:
		return basic.NewInt64Value(typed)
	case uint:
		return basic.NewInt64Value(int64(typed))
	case uint64:
		return basic.NewInt64Value(int64(typed))
	case float32:
		return basic.NewFloatValue(float64(typed))
	case float64:
		return basic.NewFloatValue(typed)
	case []byte:
		return basic.NewStringValue(string(typed))
	default:
		return basic.NewStringValue(fmt.Sprint(value))
	}
}

func correlatedExpressionValue(value interface{}) basic.Value {
	if value == nil {
		return basic.NewNull()
	}
	if existing, ok := value.(basic.Value); ok {
		if existing.IsNull() {
			return basic.NewNull()
		}
		return basic.NewStringValue(existing.String())
	}
	return basic.NewStringValue(fmt.Sprint(value))
}

func correlatedProjectionTypes(records []Record, meta *metadata.TableMeta) []string {
	types := make([]string, len(meta.Columns))
	for index := range types {
		types[index] = string(meta.Columns[index].Type)
		if len(records) > 0 && index < len(records[0].GetValues()) && records[0].GetValues()[index] != nil {
			types[index] = strings.ToLower(records[0].GetValues()[index].Type().String())
		}
	}
	return types
}
