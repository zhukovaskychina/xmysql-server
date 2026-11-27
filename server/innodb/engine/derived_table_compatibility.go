package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// executeDerivedTableCompatibility handles one-level derived tables through
// the same SelectResult contract as ordinary scans. It deliberately keeps the
// scope narrow and correct: one derived source, row-wise projection/filter,
// DISTINCT, ORDER BY and LIMIT/OFFSET. Mixed set-expression sources have a
// separate raw compatibility path that can feed the existing derived-join
// executor; broader arbitrary set-expression AST combinations remain outside
// this path.
func (e *XMySQLExecutor) executeDerivedTableCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	if handled, err := e.executeRawSetDerivedJoinCompatibility(ctx, query, databaseName); handled {
		return handled, err
	}
	if handled, err := e.executeRawSetDerivedTableCompatibility(ctx, query, databaseName); handled {
		return handled, err
	}
	if handled, err := e.executeRawNestedSetDerivedTableCompatibility(ctx, query, databaseName); handled {
		return handled, err
	}
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return false, nil
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return false, nil
	}
	if !selectHasDerivedTable(selectStmt) && selectHasDerivedSource(selectStmt) {
		result, err := e.executeDerivedJoinSelect(ctx, selectStmt, databaseName)
		if err != nil {
			return true, err
		}
		if ctx != nil && ctx.Results != nil {
			ctx.Results <- &Result{
				ResultType: common.RESULT_TYPE_QUERY,
				Data:       result,
				Message:    fmt.Sprintf("derived join query executed successfully, %d rows returned", result.RowCount),
			}
		}
		return true, nil
	}
	if !selectHasDerivedTable(selectStmt) {
		return false, nil
	}
	result, err := e.executeDerivedTableSelect(ctx, selectStmt, databaseName)
	if err != nil {
		return true, err
	}
	if ctx != nil && ctx.Results != nil {
		ctx.Results <- &Result{
			ResultType: common.RESULT_TYPE_QUERY,
			Data:       result,
			Message:    fmt.Sprintf("derived table query executed successfully, %d rows returned", result.RowCount),
		}
	}
	return true, nil
}

// executeRawNestedSetDerivedTableCompatibility handles a mixed set expression
// nested inside another derived table. The legacy parser can parse the outer
// SELECT and ordinary UNION-derived sources, but it cannot represent a
// parenthesized INTERSECT/EXCEPT source below that level. Materialize the
// innermost set expression first, evaluate its immediate SELECT wrapper, then
// feed that result into the outer derived-table evaluator.
func (e *XMySQLExecutor) executeRawNestedSetDerivedTableCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	result, handled, err := e.materializeRawNestedSetDerivedQuery(ctx, query, databaseName)
	if !handled {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if ctx != nil && ctx.Results != nil {
		ctx.Results <- &Result{
			ResultType: common.RESULT_TYPE_QUERY,
			Data:       result,
			Message:    fmt.Sprintf("nested derived set query executed successfully, %d rows returned", result.RowCount),
		}
	}
	return true, nil
}

func (e *XMySQLExecutor) materializeRawNestedSetDerivedQuery(ctx *ExecutionContext, query, databaseName string) (*SelectResult, bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "select ") {
		return nil, false, nil
	}
	outerParts := splitTopLevelKeyword(strings.TrimSpace(trimmed[len("select"):]), "from")
	if len(outerParts) != 2 {
		return nil, false, nil
	}
	outerProjection := strings.TrimSpace(outerParts[0])
	outerFromTail := strings.TrimSpace(outerParts[1])
	if outerProjection == "" || outerFromTail == "" || outerFromTail[0] != '(' {
		return nil, false, nil
	}
	outerClose := matchingParenIndex(outerFromTail, 0)
	if outerClose <= 1 {
		return nil, false, nil
	}
	innerQuery := strings.TrimSpace(outerFromTail[1:outerClose])
	if !strings.HasPrefix(strings.ToLower(innerQuery), "select ") {
		return nil, false, nil
	}
	outerAlias, outerSuffix, ok := parseRawDerivedAliasAndSuffix(outerFromTail[outerClose+1:])
	if !ok {
		return nil, false, nil
	}

	innerParts := splitTopLevelKeyword(strings.TrimSpace(innerQuery[len("select"):]), "from")
	if len(innerParts) != 2 {
		return nil, false, nil
	}
	innerProjection := strings.TrimSpace(innerParts[0])
	innerFromTail := strings.TrimSpace(innerParts[1])
	if innerProjection == "" || innerFromTail == "" || innerFromTail[0] != '(' {
		return nil, false, nil
	}
	innerClose := matchingParenIndex(innerFromTail, 0)
	if innerClose <= 1 {
		return nil, false, nil
	}
	setText := strings.TrimSpace(innerFromTail[1:innerClose])
	innerAlias, innerSuffix, ok := parseRawDerivedAliasAndSuffix(innerFromTail[innerClose+1:])
	if !ok {
		return nil, false, nil
	}

	var setResult *SelectResult
	var err error
	if _, operators, setOK := splitSetOperationQuery(setText); setOK && hasNonUnionSetOperator(operators) {
		setResult, err = e.executeSetOperationBranch(ctx, setText, databaseName)
		if err != nil {
			return nil, true, err
		}
	} else {
		var handled bool
		setResult, handled, err = e.materializeRawNestedSetDerivedQuery(ctx, innerQuery, databaseName)
		if err != nil {
			return nil, true, err
		}
		if !handled {
			return nil, false, nil
		}
	}
	innerSQL := "select " + innerProjection + " from materialized_set as " + innerAlias
	if innerSuffix != "" {
		innerSQL += " " + innerSuffix
	}
	innerStmt, err := sqlparser.Parse(innerSQL)
	if err != nil {
		return nil, true, err
	}
	innerSelect, ok := innerStmt.(*sqlparser.Select)
	if !ok {
		return nil, true, fmt.Errorf("nested derived set inner query is not SELECT: %T", innerStmt)
	}
	var innerResult *SelectResult
	if rawDerivedSuffixHasJoin(innerSuffix) {
		innerResult, err = e.executeDerivedJoinSelectWithMaterialized(ctx, innerSelect, databaseName, map[string]*SelectResult{strings.ToLower(innerAlias): setResult})
	} else {
		innerResult, err = e.evaluateMaterializedDerivedSelect(ctx, innerSelect, innerAlias, setResult, databaseName)
	}
	if err != nil {
		return nil, true, err
	}

	outerSQL := "select " + outerProjection + " from materialized_set as " + outerAlias
	if outerSuffix != "" {
		outerSQL += " " + outerSuffix
	}
	outerStmt, err := sqlparser.Parse(outerSQL)
	if err != nil {
		return nil, true, err
	}
	outerSelect, ok := outerStmt.(*sqlparser.Select)
	if !ok {
		return nil, true, fmt.Errorf("nested derived set outer query is not SELECT: %T", outerStmt)
	}
	var result *SelectResult
	if rawDerivedSuffixHasJoin(outerSuffix) {
		result, err = e.executeDerivedJoinSelectWithMaterialized(ctx, outerSelect, databaseName, map[string]*SelectResult{strings.ToLower(outerAlias): innerResult})
	} else {
		result, err = e.evaluateMaterializedDerivedSelect(ctx, outerSelect, outerAlias, innerResult, databaseName)
	}
	if err != nil {
		return nil, true, err
	}
	return result, true, nil
}

// executeRawSetDerivedJoinCompatibility handles a mixed set expression used as
// the right side of an ordinary INNER JOIN. The SQL parser cannot represent
// that parenthesized set expression as a normal derived subquery, so the set
// branch is materialized first and then routed through the existing derived
// join executor.
func (e *XMySQLExecutor) executeRawSetDerivedJoinCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "select ") {
		return false, nil
	}
	parts := splitTopLevelKeyword(strings.TrimSpace(trimmed[len("select"):]), "from")
	if len(parts) != 2 {
		return false, nil
	}
	projectionText := strings.TrimSpace(parts[0])
	fromTail := strings.TrimSpace(parts[1])
	joinType, joinParts := splitRawDerivedJoin(fromTail)
	if len(joinParts) != 2 || strings.TrimSpace(joinParts[0]) == "" {
		return false, nil
	}
	rightTail := strings.TrimSpace(joinParts[1])
	if rightTail == "" || rightTail[0] != '(' {
		return false, nil
	}
	close := matchingParenIndex(rightTail, 0)
	if close <= 1 {
		return false, nil
	}
	innerText := strings.TrimSpace(rightTail[1:close])
	if _, operators, ok := splitSetOperationQuery(innerText); !ok || !hasNonUnionSetOperator(operators) {
		return false, nil
	}
	alias, suffix, ok := parseRawDerivedAliasAndSuffix(strings.TrimSpace(rightTail[close+1:]))
	if !ok || suffix == "" {
		return false, nil
	}
	innerResult, err := e.executeSetOperationBranch(ctx, innerText, databaseName)
	if err != nil {
		return true, err
	}
	outerSQL := "select " + projectionText + " from " + strings.TrimSpace(joinParts[0]) + " " + joinType + " materialized_set as " + alias + " " + suffix
	parsed, err := sqlparser.Parse(outerSQL)
	if err != nil {
		return true, err
	}
	outer, ok := parsed.(*sqlparser.Select)
	if !ok {
		return true, fmt.Errorf("derived set join outer query is not SELECT: %T", parsed)
	}
	result, err := e.executeDerivedJoinSelectWithMaterialized(ctx, outer, databaseName, map[string]*SelectResult{strings.ToLower(alias): innerResult})
	if err != nil {
		return true, err
	}
	if ctx != nil && ctx.Results != nil {
		ctx.Results <- &Result{
			ResultType: common.RESULT_TYPE_QUERY,
			Data:       result,
			Message:    fmt.Sprintf("derived set right-join query executed successfully, %d rows returned", result.RowCount),
		}
	}
	return true, nil
}

func splitRawDerivedJoin(fromTail string) (string, []string) {
	for _, joinType := range []string{"left outer join", "right outer join", "left join", "right join", "inner join", "cross join", "join"} {
		if parts := splitTopLevelKeyword(fromTail, joinType); len(parts) == 2 {
			return joinType, parts
		}
	}
	return "", nil
}

// executeRawSetDerivedTableCompatibility handles the set-expression form that
// the legacy SQL parser cannot represent as a derived-table SelectStatement:
//
//	SELECT d.n FROM ((SELECT ... UNION ALL SELECT ...) INTERSECT SELECT ...) AS d
//	WHERE ... ORDER BY ... LIMIT ...
//
// The inner set expression is still executed by the normal set-operation
// materializer. Only the outer, row-oriented query is evaluated from the
// materialized result, keeping duplicate, NULL, ordering and limit semantics
// in one place without teaching the legacy parser new set-operation grammar.
func (e *XMySQLExecutor) executeRawSetDerivedTableCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "select ") {
		return false, nil
	}
	parts := splitTopLevelKeyword(strings.TrimSpace(trimmed[len("select"):]), "from")
	if len(parts) != 2 {
		return false, nil
	}
	projectionText := strings.TrimSpace(parts[0])
	fromTail := strings.TrimSpace(parts[1])
	if projectionText == "" || len(fromTail) == 0 || fromTail[0] != '(' {
		return false, nil
	}
	close := matchingParenIndex(fromTail, 0)
	if close <= 1 {
		return false, nil
	}
	innerText := strings.TrimSpace(fromTail[1:close])
	if _, operators, ok := splitSetOperationQuery(innerText); !ok || !hasNonUnionSetOperator(operators) {
		return false, nil
	}

	afterSource := strings.TrimSpace(fromTail[close+1:])
	alias, suffix, ok := parseRawDerivedAliasAndSuffix(afterSource)
	if !ok {
		return false, nil
	}
	innerResult, err := e.executeSetOperationBranch(ctx, innerText, databaseName)
	if err != nil {
		return true, err
	}
	outerSQL := "select " + projectionText + " from materialized_set as " + alias
	if suffix != "" {
		outerSQL += " " + suffix
	}
	parsed, err := sqlparser.Parse(outerSQL)
	if err != nil {
		return true, err
	}
	outer, ok := parsed.(*sqlparser.Select)
	if !ok {
		return true, fmt.Errorf("derived set outer query is not SELECT: %T", parsed)
	}
	if rawDerivedSuffixHasJoin(suffix) {
		result, joinErr := e.executeDerivedJoinSelectWithMaterialized(ctx, outer, databaseName, map[string]*SelectResult{strings.ToLower(alias): innerResult})
		if joinErr != nil {
			return true, joinErr
		}
		if ctx != nil && ctx.Results != nil {
			ctx.Results <- &Result{
				ResultType: common.RESULT_TYPE_QUERY,
				Data:       result,
				Message:    fmt.Sprintf("derived set join query executed successfully, %d rows returned", result.RowCount),
			}
		}
		return true, nil
	}
	result, err := e.evaluateMaterializedDerivedSelect(ctx, outer, alias, innerResult, databaseName)
	if err != nil {
		return true, err
	}
	if ctx != nil && ctx.Results != nil {
		ctx.Results <- &Result{
			ResultType: common.RESULT_TYPE_QUERY,
			Data:       result,
			Message:    fmt.Sprintf("derived set query executed successfully, %d rows returned", result.RowCount),
		}
	}
	return true, nil
}

func rawDerivedSuffixHasJoin(suffix string) bool {
	lower := strings.ToLower(strings.TrimSpace(suffix))
	for _, prefix := range []string{"join ", "inner join ", "left join ", "right join ", "cross join ", "straight_join "} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

func parseRawDerivedAliasAndSuffix(text string) (string, string, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", "", false
	}
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return "", "", false
	}
	alias := ""
	consumed := 0
	if strings.EqualFold(fields[0], "as") {
		if len(fields) < 2 {
			return "", "", false
		}
		alias = strings.Trim(fields[1], "`")
		consumed = 2
	} else {
		alias = strings.Trim(fields[0], "`")
		consumed = 1
	}
	if alias == "" || strings.ContainsAny(alias, "(),") {
		return "", "", false
	}
	remaining := text
	for index := 0; index < consumed; index++ {
		space := strings.IndexAny(remaining, " \t\r\n")
		if space < 0 {
			remaining = ""
			break
		}
		remaining = strings.TrimSpace(remaining[space:])
	}
	return alias, remaining, true
}

func (e *XMySQLExecutor) evaluateMaterializedDerivedSelect(ctx *ExecutionContext, stmt *sqlparser.Select, alias string, input *SelectResult, databaseName string) (*SelectResult, error) {
	if stmt == nil || input == nil {
		return nil, fmt.Errorf("derived set query has no materialized input")
	}
	rows := make([]derivedRow, 0, len(input.Records))
	for _, record := range input.Records {
		row := make(map[string]interface{}, len(input.Columns)*4)
		for index, column := range input.Columns {
			if index >= len(record.GetValues()) {
				continue
			}
			value := derivedNormalizeValue(derivedBasicValueInterface(record.GetValues()[index]), derivedColumnType(input, index))
			clean := strings.Trim(column, "`")
			row[clean] = value
			row[strings.ToLower(clean)] = value
			row[alias+"."+clean] = value
			row[strings.ToLower(alias+"."+clean)] = value
		}
		e.addStoredFunctionEvaluationContext(row, ctx, databaseName)
		rows = append(rows, derivedRow{values: row})
	}
	filtered := make([]derivedRow, 0, len(rows))
	for _, row := range rows {
		if stmt.Where != nil {
			matched, err := evalPredicate(stmt.Where.Expr, row.values)
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}
		}
		filtered = append(filtered, row)
	}
	if containsAggregateSelectExpr(stmt.SelectExprs) || len(stmt.GroupBy) > 0 {
		return buildDerivedAggregateResult(stmt, filtered, alias)
	}

	columns := make([]string, 0, len(stmt.SelectExprs))
	metaColumns := make([]*metadata.ColumnMeta, 0, len(stmt.SelectExprs))
	projected := make([]derivedRow, 0, len(filtered))
	for _, row := range filtered {
		values := make([]interface{}, 0, len(stmt.SelectExprs))
		outputMap := make(map[string]interface{})
		if len(stmt.SelectExprs) == 1 {
			if star, ok := stmt.SelectExprs[0].(*sqlparser.StarExpr); ok {
				if !star.TableName.IsEmpty() && !strings.EqualFold(star.TableName.Name.String(), alias) {
					return nil, fmt.Errorf("unknown derived table alias %s", star.TableName.Name.String())
				}
				for _, column := range input.Columns {
					clean := strings.Trim(column, "`")
					value := row.values[clean]
					values = append(values, value)
					if len(projected) == 0 {
						columns = append(columns, clean)
						metaColumns = append(metaColumns, &metadata.ColumnMeta{Name: clean, Type: projectionValueType(value)})
					}
					outputMap[clean] = value
					outputMap[strings.ToLower(clean)] = value
				}
			} else {
				aliased, ok := stmt.SelectExprs[0].(*sqlparser.AliasedExpr)
				if !ok {
					return nil, fmt.Errorf("unsupported derived set SELECT expression %T", stmt.SelectExprs[0])
				}
				value, err := evaluateExpressionWithRow(aliased.Expr, row.values)
				if err != nil {
					return nil, err
				}
				name := derivedProjectionName(sqlparser.String(aliased.Expr))
				if !aliased.As.IsEmpty() {
					name = aliased.As.String()
				}
				values = append(values, value)
				outputMap[name] = value
				outputMap[strings.ToLower(name)] = value
				if len(projected) == 0 {
					columns = append(columns, name)
					metaColumns = append(metaColumns, &metadata.ColumnMeta{Name: name, Type: projectionValueType(value)})
				}
			}
		} else {
			for _, expression := range stmt.SelectExprs {
				aliased, ok := expression.(*sqlparser.AliasedExpr)
				if !ok {
					return nil, fmt.Errorf("unsupported derived set SELECT expression %T", expression)
				}
				value, err := evaluateExpressionWithRow(aliased.Expr, row.values)
				if err != nil {
					return nil, err
				}
				name := derivedProjectionName(sqlparser.String(aliased.Expr))
				if !aliased.As.IsEmpty() {
					name = aliased.As.String()
				}
				values = append(values, value)
				outputMap[name] = value
				outputMap[strings.ToLower(name)] = value
				if len(projected) == 0 {
					columns = append(columns, name)
					metaColumns = append(metaColumns, &metadata.ColumnMeta{Name: name, Type: projectionValueType(value)})
				}
			}
		}
		projected = append(projected, derivedRow{values: outputMap, order: values})
	}
	if strings.EqualFold(strings.TrimSpace(stmt.Distinct), strings.TrimSpace(sqlparser.DistinctStr)) {
		seen := make(map[string]struct{}, len(projected))
		unique := make([]derivedRow, 0, len(projected))
		for _, row := range projected {
			key := derivedValuesKey(row.order)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			unique = append(unique, row)
		}
		projected = unique
	}
	if len(stmt.OrderBy) > 0 {
		sort.SliceStable(projected, func(left, right int) bool {
			for _, order := range stmt.OrderBy {
				leftValue, leftOK := derivedOrderValue(order, projected[left], columns)
				rightValue, rightOK := derivedOrderValue(order, projected[right], columns)
				if !leftOK || !rightOK {
					continue
				}
				cmp := compareScalarValues(leftValue, rightValue)
				if cmp == 0 {
					continue
				}
				if order.Direction == sqlparser.DescScr {
					return cmp > 0
				}
				return cmp < 0
			}
			return false
		})
	}
	start, end := derivedLimitBounds(stmt.Limit, len(projected))
	projected = projected[start:end]
	records := make([]Record, 0, len(projected))
	for _, row := range projected {
		records = append(records, NewExecutorRecordFromInterface(derivedOutputValues(row.order), &metadata.TableMeta{Name: alias + "_derived", Columns: metaColumns}))
	}
	return &SelectResult{Records: records, RowCount: len(records), Columns: columns, ColumnTypes: derivedColumnTypes(metaColumns), ResultType: common.RESULT_TYPE_QUERY}, nil
}

func selectHasDerivedTable(stmt *sqlparser.Select) bool {
	if stmt == nil || len(stmt.From) != 1 {
		return false
	}
	from, ok := stmt.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return false
	}
	_, ok = from.Expr.(*sqlparser.Subquery)
	return ok
}

type derivedRow struct {
	values map[string]interface{}
	order  []interface{}
}

func (e *XMySQLExecutor) executeDerivedTableSelect(ctx *ExecutionContext, stmt *sqlparser.Select, databaseName string) (*SelectResult, error) {
	if stmt == nil || !selectHasDerivedTable(stmt) {
		return nil, fmt.Errorf("derived table query requires one derived source")
	}
	from := stmt.From[0].(*sqlparser.AliasedTableExpr)
	derived := from.Expr.(*sqlparser.Subquery)
	innerResult, err := e.executeDerivedInnerSelect(ctx, derived.Select, databaseName)
	if err != nil {
		return nil, err
	}
	alias := strings.TrimSpace(from.As.String())
	if alias == "" {
		alias = "derived"
	}

	rows := make([]derivedRow, 0, len(innerResult.Records))
	for _, record := range innerResult.Records {
		row := make(map[string]interface{}, len(innerResult.Columns)*4)
		for index, column := range innerResult.Columns {
			if index >= len(record.GetValues()) {
				continue
			}
			raw := derivedNormalizeValue(derivedBasicValueInterface(record.GetValues()[index]), derivedColumnType(innerResult, index))
			clean := strings.Trim(column, "`")
			row[clean] = raw
			row[strings.ToLower(clean)] = raw
			row[alias+"."+clean] = raw
			row[strings.ToLower(alias+"."+clean)] = raw
		}
		e.addStoredFunctionEvaluationContext(row, ctx, databaseName)
		rows = append(rows, derivedRow{values: row})
	}

	filtered := make([]derivedRow, 0, len(rows))
	for _, row := range rows {
		if stmt.Where != nil {
			matched, err := evalPredicate(stmt.Where.Expr, row.values)
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}
		}
		filtered = append(filtered, row)
	}
	if containsAggregateSelectExpr(stmt.SelectExprs) || len(stmt.GroupBy) > 0 {
		return buildDerivedAggregateResult(stmt, filtered, alias)
	}

	columns := make([]string, 0)
	metaColumns := make([]*metadata.ColumnMeta, 0)
	projected := make([]derivedRow, 0, len(filtered))
	for _, row := range filtered {
		values := make([]interface{}, 0)
		outputMap := make(map[string]interface{})
		e.addStoredFunctionEvaluationContext(outputMap, ctx, databaseName)
		if len(stmt.SelectExprs) == 1 {
			if star, ok := stmt.SelectExprs[0].(*sqlparser.StarExpr); ok {
				if !star.TableName.IsEmpty() && !strings.EqualFold(star.TableName.Name.String(), alias) {
					return nil, fmt.Errorf("unknown derived table alias %s", star.TableName.Name.String())
				}
				for _, column := range innerResult.Columns {
					clean := strings.Trim(column, "`")
					value := row.values[clean]
					values = append(values, value)
					if len(projected) == 0 {
						columns = append(columns, clean)
						metaColumns = append(metaColumns, &metadata.ColumnMeta{Name: clean, Type: projectionValueType(value)})
					}
					outputMap[clean] = value
					outputMap[strings.ToLower(clean)] = value
				}
			} else {
				aliased, ok := stmt.SelectExprs[0].(*sqlparser.AliasedExpr)
				if !ok {
					return nil, fmt.Errorf("unsupported derived table SELECT expression %T", stmt.SelectExprs[0])
				}
				value, err := evaluateExpressionWithRow(aliased.Expr, row.values)
				if err != nil {
					return nil, err
				}
				name := derivedProjectionName(sqlparser.String(aliased.Expr))
				if !aliased.As.IsEmpty() {
					name = aliased.As.String()
				}
				values = append(values, value)
				outputMap[name] = value
				outputMap[strings.ToLower(name)] = value
				if len(projected) == 0 {
					columns = append(columns, name)
					metaColumns = append(metaColumns, &metadata.ColumnMeta{Name: name, Type: projectionValueType(value)})
				}
			}
		} else {
			for _, expression := range stmt.SelectExprs {
				aliased, ok := expression.(*sqlparser.AliasedExpr)
				if !ok {
					return nil, fmt.Errorf("unsupported derived table SELECT expression %T", expression)
				}
				value, err := evaluateExpressionWithRow(aliased.Expr, row.values)
				if err != nil {
					return nil, err
				}
				name := derivedProjectionName(sqlparser.String(aliased.Expr))
				if !aliased.As.IsEmpty() {
					name = aliased.As.String()
				}
				values = append(values, value)
				outputMap[name] = value
				outputMap[strings.ToLower(name)] = value
				if len(projected) == 0 {
					columns = append(columns, name)
					metaColumns = append(metaColumns, &metadata.ColumnMeta{Name: name, Type: projectionValueType(value)})
				}
			}
		}
		row.values = outputMap
		row.order = values
		projected = append(projected, row)
	}

	if len(stmt.OrderBy) > 0 {
		sort.SliceStable(projected, func(i, j int) bool {
			return derivedRowsLess(projected[i], projected[j], stmt.OrderBy, columns)
		})
	}

	if strings.EqualFold(strings.TrimSpace(stmt.Distinct), strings.TrimSpace(sqlparser.DistinctStr)) {
		seen := make(map[string]struct{}, len(projected))
		unique := make([]derivedRow, 0, len(projected))
		for _, row := range projected {
			key := derivedValuesKey(row.order)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			unique = append(unique, row)
		}
		projected = unique
	}

	start, end := derivedLimitBounds(stmt.Limit, len(projected))
	projected = projected[start:end]
	tableMeta := &metadata.TableMeta{Name: alias + "_derived", Columns: metaColumns}
	records := make([]Record, 0, len(projected))
	for _, row := range projected {
		values := make([]interface{}, len(row.order))
		for index, value := range row.order {
			values[index] = derivedOutputValue(value)
		}
		records = append(records, NewExecutorRecordFromInterface(values, tableMeta))
	}
	columnTypes := make([]string, 0, len(metaColumns))
	for _, column := range metaColumns {
		columnTypes = append(columnTypes, string(column.Type))
	}
	return &SelectResult{Records: records, RowCount: len(records), Columns: columns, ColumnTypes: columnTypes, ResultType: common.RESULT_TYPE_QUERY}, nil
}

func (e *XMySQLExecutor) addStoredFunctionEvaluationContext(values map[string]interface{}, ctx *ExecutionContext, databaseName string) {
	if values == nil {
		return
	}
	if ctx != nil {
		sessionValues := newSessionExpressionValues(ctx.Session)
		if sessionValues["database"] == nil && strings.TrimSpace(databaseName) != "" {
			sessionValues["database"] = databaseName
		}
		values[sessionExpressionValuesKey] = sessionValues
	}
	values[storedFunctionDataDirKey] = e.getDataDir()
	values[storedFunctionSchemaKey] = databaseName
	values[storedFunctionAuthorizerKey] = func(object persistedStoredObject) error {
		if object.Definer != "" {
			if err := e.validateStoredObjectAccount(object.Definer); err != nil {
				return err
			}
		}
		return e.checkStoredRoutineExecute(ctx, object.Schema, object.Name)
	}
}

func buildDerivedAggregateResult(stmt *sqlparser.Select, rows []derivedRow, alias string) (*SelectResult, error) {
	if len(stmt.GroupBy) > 0 {
		return buildDerivedGroupedAggregateResult(stmt, rows, alias)
	}
	if stmt.Having != nil {
		// HAVING aliases can be evaluated after aggregate projection below.
	}
	columns := make([]string, 0, len(stmt.SelectExprs))
	metaColumns := make([]*metadata.ColumnMeta, 0, len(stmt.SelectExprs))
	values := make([]interface{}, 0, len(stmt.SelectExprs))
	rowMap := make(map[string]interface{}, len(stmt.SelectExprs)*2)
	for _, expression := range stmt.SelectExprs {
		aliased, ok := expression.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("unsupported aggregate derived expression %T", expression)
		}
		agg := parseAggregateExpression(sqlparser.String(aliased.Expr))
		if agg.funcName == "" {
			return nil, fmt.Errorf("derived aggregate projection requires aggregate expressions")
		}
		accumulator := &aggregateAccumulator{fn: agg.funcName, separator: agg.separator, orderBy: agg.orderBy, orderDesc: agg.orderDesc}
		if agg.distinct {
			accumulator.seen = make(map[string]struct{})
		}
		for _, row := range rows {
			if agg.column == "*" {
				accumulator.add(1)
				continue
			}
			value, exists := derivedValueByName(row.values, agg.column)
			if !exists {
				return nil, fmt.Errorf("unknown derived aggregate column %s", agg.column)
			}
			accumulator.add(value)
		}
		value := accumulator.value()
		name := derivedProjectionName(sqlparser.String(aliased.Expr))
		if !aliased.As.IsEmpty() {
			name = aliased.As.String()
		}
		columns = append(columns, name)
		values = append(values, value)
		rowMap[name] = value
		rowMap[strings.ToLower(name)] = value
		columnType := metadata.TypeDecimal
		if agg.funcName == "COUNT" || strings.HasPrefix(agg.funcName, "BIT_") {
			columnType = metadata.TypeBigInt
		} else if agg.funcName == "GROUP_CONCAT" {
			columnType = metadata.TypeVarchar
		}
		metaColumns = append(metaColumns, &metadata.ColumnMeta{Name: name, Type: columnType})
	}
	if stmt.Having != nil {
		matched, err := evalPredicate(stmt.Having.Expr, rowMap)
		if err != nil {
			return nil, err
		}
		if !matched {
			return &SelectResult{Columns: columns, ColumnTypes: derivedColumnTypes(metaColumns), ResultType: common.RESULT_TYPE_QUERY}, nil
		}
	}
	start, end := derivedLimitBounds(stmt.Limit, 1)
	if start >= end {
		return &SelectResult{Columns: columns, ColumnTypes: derivedColumnTypes(metaColumns), ResultType: common.RESULT_TYPE_QUERY}, nil
	}
	record := NewExecutorRecordFromInterface(derivedOutputValues(values), &metadata.TableMeta{Name: alias + "_derived", Columns: metaColumns})
	return &SelectResult{Records: []Record{record}, RowCount: 1, Columns: columns, ColumnTypes: derivedColumnTypes(metaColumns), ResultType: common.RESULT_TYPE_QUERY}, nil
}

func buildDerivedGroupedAggregateResult(stmt *sqlparser.Select, rows []derivedRow, alias string) (*SelectResult, error) {
	type derivedGroup struct {
		key  string
		rows []derivedRow
	}
	groups := make([]*derivedGroup, 0)
	byKey := make(map[string]*derivedGroup)
	for _, row := range rows {
		keyValues := make([]interface{}, 0, len(stmt.GroupBy))
		for _, expression := range stmt.GroupBy {
			value, err := evaluateExpressionWithRow(expression, row.values)
			if err != nil {
				return nil, err
			}
			keyValues = append(keyValues, value)
		}
		key := derivedValuesKey(keyValues)
		group := byKey[key]
		if group == nil {
			group = &derivedGroup{key: key}
			byKey[key] = group
			groups = append(groups, group)
		}
		group.rows = append(group.rows, row)
	}
	if len(groups) == 0 {
		columns := make([]string, 0, len(stmt.SelectExprs))
		for _, expression := range stmt.SelectExprs {
			if aliased, ok := expression.(*sqlparser.AliasedExpr); ok {
				name := derivedProjectionName(sqlparser.String(aliased.Expr))
				if !aliased.As.IsEmpty() {
					name = aliased.As.String()
				}
				columns = append(columns, name)
			}
		}
		return &SelectResult{Columns: columns, ColumnTypes: make([]string, len(columns)), ResultType: common.RESULT_TYPE_QUERY}, nil
	}

	columns := make([]string, 0, len(stmt.SelectExprs))
	allRows := make([]derivedRow, 0, len(groups))
	var metaColumns []*metadata.ColumnMeta
	for _, group := range groups {
		values := make([]interface{}, 0, len(stmt.SelectExprs))
		rowMap := make(map[string]interface{}, len(stmt.SelectExprs)*2)
		currentMeta := make([]*metadata.ColumnMeta, 0, len(stmt.SelectExprs))
		for _, expression := range stmt.SelectExprs {
			aliased, ok := expression.(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("unsupported grouped derived expression %T", expression)
			}
			expressionText := sqlparser.String(aliased.Expr)
			agg := parseAggregateExpression(expressionText)
			var value interface{}
			columnType := metadata.TypeVarchar
			if agg.funcName != "" {
				accumulator := &aggregateAccumulator{fn: agg.funcName, separator: agg.separator, orderBy: agg.orderBy, orderDesc: agg.orderDesc}
				if agg.distinct {
					accumulator.seen = make(map[string]struct{})
				}
				for _, row := range group.rows {
					if agg.column == "*" {
						accumulator.add(1)
						continue
					}
					candidate, exists := derivedValueByName(row.values, agg.column)
					if !exists {
						return nil, fmt.Errorf("unknown derived aggregate column %s", agg.column)
					}
					accumulator.add(candidate)
				}
				value = accumulator.value()
				columnType = metadata.TypeDecimal
				if agg.funcName == "COUNT" || strings.HasPrefix(agg.funcName, "BIT_") {
					columnType = metadata.TypeBigInt
				} else if agg.funcName == "GROUP_CONCAT" {
					columnType = metadata.TypeVarchar
				}
			} else if len(group.rows) > 0 {
				var err error
				value, err = evaluateExpressionWithRow(aliased.Expr, group.rows[0].values)
				if err != nil {
					return nil, err
				}
				columnType = projectionValueType(value)
			}
			name := derivedProjectionName(expressionText)
			if !aliased.As.IsEmpty() {
				name = aliased.As.String()
			}
			if len(columns) < len(stmt.SelectExprs) {
				columns = append(columns, name)
			}
			values = append(values, value)
			rowMap[name] = value
			rowMap[strings.ToLower(name)] = value
			currentMeta = append(currentMeta, &metadata.ColumnMeta{Name: name, Type: columnType})
		}
		if stmt.Having != nil {
			matched, err := evalPredicate(stmt.Having.Expr, rowMap)
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}
		}
		if metaColumns == nil {
			metaColumns = currentMeta
		}
		allRows = append(allRows, derivedRow{values: rowMap, order: values})
	}
	if len(metaColumns) == 0 {
		metaColumns = make([]*metadata.ColumnMeta, len(columns))
	}
	if len(stmt.OrderBy) > 0 {
		sort.SliceStable(allRows, func(left, right int) bool {
			return derivedRowsLess(allRows[left], allRows[right], stmt.OrderBy, columns)
		})
	}
	start, end := derivedLimitBounds(stmt.Limit, len(allRows))
	allRows = allRows[start:end]
	records := make([]Record, 0, len(allRows))
	for _, row := range allRows {
		records = append(records, NewExecutorRecordFromInterface(derivedOutputValues(row.order), &metadata.TableMeta{Name: alias + "_derived", Columns: metaColumns}))
	}
	return &SelectResult{Records: records, RowCount: len(records), Columns: columns, ColumnTypes: derivedColumnTypes(metaColumns), ResultType: common.RESULT_TYPE_QUERY}, nil
}

func derivedValueByName(values map[string]interface{}, name string) (interface{}, bool) {
	clean := strings.Trim(strings.TrimSpace(name), "`")
	if value, ok := values[clean]; ok {
		return value, true
	}
	if value, ok := values[strings.ToLower(clean)]; ok {
		return value, true
	}
	if dot := strings.LastIndex(clean, "."); dot >= 0 {
		return derivedValueByName(values, clean[dot+1:])
	}
	return nil, false
}

func derivedProjectionName(expression string) string {
	name := strings.Trim(strings.TrimSpace(expression), "`")
	if dot := strings.LastIndex(name, "."); dot >= 0 {
		candidate := strings.Trim(strings.TrimSpace(name[dot+1:]), "`")
		if candidate != "" && !strings.ContainsAny(candidate, " +-*/()") {
			return candidate
		}
	}
	return name
}

func derivedColumnTypes(columns []*metadata.ColumnMeta) []string {
	result := make([]string, 0, len(columns))
	for _, column := range columns {
		if column == nil {
			result = append(result, "")
			continue
		}
		result = append(result, strings.ToLower(string(column.Type)))
	}
	return result
}

func derivedOutputValues(values []interface{}) []interface{} {
	result := make([]interface{}, len(values))
	for index, value := range values {
		result[index] = derivedOutputValue(value)
	}
	return result
}

func derivedOutputValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	default:
		return value
	}
}

func (e *XMySQLExecutor) executeDerivedInnerSelect(ctx *ExecutionContext, statement sqlparser.SelectStatement, databaseName string) (*SelectResult, error) {
	if statement == nil {
		return nil, fmt.Errorf("derived table subquery is empty")
	}
	switch inner := statement.(type) {
	case *sqlparser.Select:
		if selectHasDerivedTable(inner) {
			return e.executeDerivedTableSelect(ctx, inner, databaseName)
		}
		if ctx == nil {
			return e.executeSelectStatement(&ExecutionContext{Context: context.Background()}, inner, databaseName)
		}
		previous := ctx.RawQuery
		ctx.RawQuery = sqlparser.String(inner)
		result, err := e.executeSelectStatement(ctx, inner, databaseName)
		ctx.RawQuery = previous
		return result, err
	case *sqlparser.ParenSelect:
		return e.executeDerivedInnerSelect(ctx, inner.Select, databaseName)
	case *sqlparser.Union:
		raw := sqlparser.String(inner)
		if branches, unionAll, ok := splitUnionQuery(raw); ok {
			return e.executeUnionQuery(ctx, branches, unionAll, databaseName)
		}
		return nil, fmt.Errorf("unsupported derived table set operation")
	default:
		return nil, fmt.Errorf("unsupported derived table subquery %T", statement)
	}
}

func containsAggregateSelectExpr(exprs sqlparser.SelectExprs) bool {
	for _, expression := range exprs {
		aliased, ok := expression.(*sqlparser.AliasedExpr)
		if ok && containsAggregateSQLExpr(aliased.Expr) {
			return true
		}
	}
	return false
}

func containsAggregateSQLExpr(expr sqlparser.Expr) bool {
	switch value := expr.(type) {
	case *sqlparser.FuncExpr:
		switch strings.ToUpper(value.Name.String()) {
		case "COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT", "JSON_ARRAYAGG", "JSON_OBJECTAGG":
			return true
		}
		for _, child := range value.Exprs {
			if aliased, ok := child.(*sqlparser.AliasedExpr); ok && containsAggregateSQLExpr(aliased.Expr) {
				return true
			}
		}
	case *sqlparser.BinaryExpr:
		return containsAggregateSQLExpr(value.Left) || containsAggregateSQLExpr(value.Right)
	case *sqlparser.ParenExpr:
		return containsAggregateSQLExpr(value.Expr)
	case *sqlparser.UnaryExpr:
		return containsAggregateSQLExpr(value.Expr)
	}
	return false
}

func derivedBasicValueInterface(value basic.Value) interface{} {
	if value == nil || value.IsNull() {
		return nil
	}
	return value.Raw()
}

func derivedColumnType(result *SelectResult, index int) string {
	if result == nil || index < 0 || index >= len(result.ColumnTypes) {
		return ""
	}
	return result.ColumnTypes[index]
}

// Storage-backed aggregate results can arrive as fixed-width encoded numeric
// bytes. Decode them before binding the derived row; otherwise an outer WHERE
// or ORDER BY compares a binary payload instead of the SQL number.
func derivedNormalizeValue(value interface{}, columnType string) interface{} {
	bytes, ok := value.([]byte)
	if !ok {
		if text, isText := value.(string); isText && len(text) == 8 && strings.ContainsRune(text, '\x00') {
			bytes = []byte(text)
		} else {
			return value
		}
	}
	if len(bytes) == 0 {
		return value
	}
	lowerType := strings.ToLower(columnType)
	binaryPayload := false
	for _, current := range bytes {
		if current == 0 {
			binaryPayload = true
			break
		}
	}
	if len(bytes) == 8 && binaryPayload && !strings.Contains(lowerType, "float") && !strings.Contains(lowerType, "double") && !strings.Contains(lowerType, "real") {
		return int64(binary.BigEndian.Uint64(bytes))
	}
	switch {
	case strings.Contains(lowerType, "float"), strings.Contains(lowerType, "double"), strings.Contains(lowerType, "real"):
		if len(bytes) == 8 {
			return math.Float64frombits(binary.BigEndian.Uint64(bytes))
		}
	case strings.Contains(lowerType, "int"), strings.Contains(lowerType, "decimal"), strings.Contains(lowerType, "numeric"):
		// Textual storage values such as []byte("2") are already SQL
		// values. The aggregate path currently emits eight-byte payloads.
		if len(bytes) == 8 {
			return int64(binary.BigEndian.Uint64(bytes))
		}
	}
	// Literal set branches may expose numeric values as textual bytes without
	// a useful ColumnTypes entry. Convert only unambiguously numeric payloads so
	// outer arithmetic and predicates receive SQL numeric operands while text
	// values remain untouched.
	if parsed, err := strconv.ParseInt(strings.TrimSpace(string(bytes)), 10, 64); err == nil {
		return parsed
	}
	if parsed, err := strconv.ParseFloat(strings.TrimSpace(string(bytes)), 64); err == nil {
		return parsed
	}
	return value
}

func derivedOrderValue(order *sqlparser.Order, row derivedRow, columns []string) (interface{}, bool) {
	if order == nil {
		return nil, false
	}
	if literal, ok := order.Expr.(*sqlparser.SQLVal); ok && literal.Type == sqlparser.IntVal {
		index, err := strconv.Atoi(string(literal.Val))
		if err == nil && index > 0 && index <= len(row.order) {
			return row.order[index-1], true
		}
	}
	value, err := evaluateExpressionWithRow(order.Expr, row.values)
	return value, err == nil
}

// derivedRowsLess applies every ORDER BY term in sequence. The compatibility
// paths materialize rows before applying LIMIT, so comparing only the first
// term can select the wrong rows whenever that term ties. Keep the comparison
// stable and match the existing scalar comparison/NULL ordering behavior.
func derivedRowsLess(left, right derivedRow, orders sqlparser.OrderBy, columns []string) bool {
	for _, order := range orders {
		leftValue, leftOK := derivedOrderValue(order, left, columns)
		rightValue, rightOK := derivedOrderValue(order, right, columns)
		if !leftOK || !rightOK {
			return false
		}
		cmp := compareScalarValues(leftValue, rightValue)
		if cmp == 0 {
			continue
		}
		if order.Direction == sqlparser.DescScr {
			return cmp > 0
		}
		return cmp < 0
	}
	return false
}

func derivedValuesKey(values []interface{}) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprintf("%T:%v", value, value))
	}
	return strings.Join(parts, "|")
}

func derivedLimitBounds(limit *sqlparser.Limit, length int) (int, int) {
	if limit == nil {
		return 0, length
	}
	start, count := 0, length
	if limit.Offset != nil {
		if value, ok := limit.Offset.(*sqlparser.SQLVal); ok {
			start, _ = strconv.Atoi(string(value.Val))
		}
	}
	if limit.Rowcount != nil {
		if value, ok := limit.Rowcount.(*sqlparser.SQLVal); ok {
			count, _ = strconv.Atoi(string(value.Val))
		}
	}
	if start < 0 {
		start = 0
	}
	if start > length {
		start = length
	}
	end := length
	if count >= 0 && start+count < end {
		end = start + count
	}
	return start, end
}
