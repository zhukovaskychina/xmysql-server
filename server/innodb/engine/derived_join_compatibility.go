package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

type derivedJoinSource struct {
	rows          []derivedRow
	columns       []string
	qualifiedCols map[string][]string
}

func selectHasDerivedSource(stmt *sqlparser.Select) bool {
	if stmt == nil {
		return false
	}
	for _, expression := range stmt.From {
		if tableExprHasDerivedSource(expression) {
			return true
		}
	}
	return false
}

func tableExprHasDerivedSource(expression sqlparser.TableExpr) bool {
	switch table := expression.(type) {
	case *sqlparser.AliasedTableExpr:
		_, ok := table.Expr.(*sqlparser.Subquery)
		return ok
	case *sqlparser.JoinTableExpr:
		return tableExprHasDerivedSource(table.LeftExpr) || tableExprHasDerivedSource(table.RightExpr)
	case *sqlparser.ParenTableExpr:
		for _, nested := range table.Exprs {
			if tableExprHasDerivedSource(nested) {
				return true
			}
		}
	}
	return false
}

func (e *XMySQLExecutor) executeDerivedJoinSelect(ctx *ExecutionContext, stmt *sqlparser.Select, databaseName string) (*SelectResult, error) {
	return e.executeDerivedJoinSelectWithMaterialized(ctx, stmt, databaseName, nil)
}

func (e *XMySQLExecutor) executeDerivedJoinSelectWithMaterialized(ctx *ExecutionContext, stmt *sqlparser.Select, databaseName string, materialized map[string]*SelectResult) (*SelectResult, error) {
	if stmt == nil || (!selectHasDerivedSource(stmt) && len(materialized) == 0) {
		return nil, fmt.Errorf("derived join query requires a derived source")
	}
	var subqueryEvaluator joinSubqueryEvaluator
	var setSubqueryEvaluator joinSetSubqueryEvaluator
	if e != nil {
		subqueryEvaluator = func(subquery *sqlparser.Subquery, row joinedMapRow) (interface{}, error) {
			return e.evaluateJoinScalarSubquery(ctx, subquery, row, databaseName)
		}
		setSubqueryEvaluator = func(subquery *sqlparser.Subquery, row joinedMapRow) ([]interface{}, error) {
			values, _, err := e.evaluateJoinSubqueryValues(ctx, subquery, row, databaseName)
			return values, err
		}
	}
	var source *derivedJoinSource
	for _, expression := range stmt.From {
		current, err := e.executeDerivedJoinTableExprWithMaterialized(ctx, expression, databaseName, materialized, subqueryEvaluator, setSubqueryEvaluator)
		if err != nil {
			return nil, err
		}
		if source == nil {
			source = current
			continue
		}
		joined, err := joinDerivedSources(source, current, sqlparser.JoinStr, sqlparser.JoinCondition{}, subqueryEvaluator, setSubqueryEvaluator)
		if err != nil {
			return nil, err
		}
		source = joined
	}
	if source == nil {
		return nil, fmt.Errorf("derived join query has no FROM source")
	}
	for index := range source.rows {
		e.addStoredFunctionEvaluationContext(source.rows[index].values, ctx, databaseName)
	}

	filtered := make([]derivedRow, 0, len(source.rows))
	for _, row := range source.rows {
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
		return buildDerivedAggregateResult(stmt, filtered, "derived_join")
	}

	projected := make([]derivedRow, 0, len(filtered))
	columns := make([]string, 0)
	metaColumns := make([]*metadata.ColumnMeta, 0)
	for _, row := range filtered {
		values := make([]interface{}, 0)
		outputMap := cloneDerivedValues(row.values)
		for _, expression := range stmt.SelectExprs {
			if star, ok := expression.(*sqlparser.StarExpr); ok {
				starColumns := source.columns
				if !star.TableName.IsEmpty() {
					starColumns = source.qualifiedCols[strings.ToLower(star.TableName.Name.String())]
					if starColumns == nil {
						return nil, fmt.Errorf("unknown derived join alias %s", star.TableName.Name.String())
					}
				}
				for _, column := range starColumns {
					value, exists := derivedValueByName(row.values, column)
					if !exists {
						value = nil
					}
					values = append(values, value)
					if len(projected) == 0 {
						columns = append(columns, column)
						metaColumns = append(metaColumns, &metadata.ColumnMeta{Name: column, Type: projectionValueType(value)})
					}
				}
				continue
			}
			aliased, ok := expression.(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("unsupported derived join projection %T", expression)
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
		row.values = outputMap
		row.order = values
		projected = append(projected, row)
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
	records := make([]Record, 0, len(projected))
	tableMeta := &metadata.TableMeta{Name: "derived_join", Columns: metaColumns}
	for _, row := range projected {
		records = append(records, NewExecutorRecordFromInterface(derivedOutputValues(row.order), tableMeta))
	}
	columnTypes := make([]string, 0, len(metaColumns))
	for _, column := range metaColumns {
		columnTypes = append(columnTypes, string(column.Type))
	}
	return &SelectResult{Records: records, RowCount: len(records), Columns: columns, ColumnTypes: columnTypes, ResultType: common.RESULT_TYPE_QUERY}, nil
}

func (e *XMySQLExecutor) executeDerivedJoinTableExpr(ctx *ExecutionContext, expression sqlparser.TableExpr, databaseName string) (*derivedJoinSource, error) {
	return e.executeDerivedJoinTableExprWithMaterialized(ctx, expression, databaseName, nil, nil, nil)
}

func (e *XMySQLExecutor) executeDerivedJoinTableExprWithMaterialized(ctx *ExecutionContext, expression sqlparser.TableExpr, databaseName string, materialized map[string]*SelectResult, subqueryEvaluator joinSubqueryEvaluator, setSubqueryEvaluator joinSetSubqueryEvaluator) (*derivedJoinSource, error) {
	switch table := expression.(type) {
	case *sqlparser.AliasedTableExpr:
		return e.executeDerivedJoinAliasedTableWithMaterialized(ctx, table, databaseName, materialized)
	case *sqlparser.JoinTableExpr:
		left, err := e.executeDerivedJoinTableExprWithMaterialized(ctx, table.LeftExpr, databaseName, materialized, subqueryEvaluator, setSubqueryEvaluator)
		if err != nil {
			return nil, err
		}
		right, err := e.executeDerivedJoinTableExprWithMaterialized(ctx, table.RightExpr, databaseName, materialized, subqueryEvaluator, setSubqueryEvaluator)
		if err != nil {
			return nil, err
		}
		return joinDerivedSources(left, right, table.Join, table.Condition, subqueryEvaluator, setSubqueryEvaluator)
	case *sqlparser.ParenTableExpr:
		if len(table.Exprs) != 1 {
			return nil, fmt.Errorf("parenthesized derived join source must contain one table expression")
		}
		return e.executeDerivedJoinTableExprWithMaterialized(ctx, table.Exprs[0], databaseName, materialized, subqueryEvaluator, setSubqueryEvaluator)
	default:
		return nil, fmt.Errorf("unsupported derived join table expression %T", expression)
	}
}

func (e *XMySQLExecutor) executeDerivedJoinAliasedTable(ctx *ExecutionContext, table *sqlparser.AliasedTableExpr, databaseName string) (*derivedJoinSource, error) {
	return e.executeDerivedJoinAliasedTableWithMaterialized(ctx, table, databaseName, nil)
}

func (e *XMySQLExecutor) executeDerivedJoinAliasedTableWithMaterialized(ctx *ExecutionContext, table *sqlparser.AliasedTableExpr, databaseName string, materialized map[string]*SelectResult) (*derivedJoinSource, error) {
	if table == nil {
		return nil, fmt.Errorf("derived join table is nil")
	}
	alias := strings.TrimSpace(table.As.String())
	switch expression := table.Expr.(type) {
	case *sqlparser.Subquery:
		if alias == "" {
			alias = "derived"
		}
		result, err := e.executeDerivedInnerSelect(ctx, expression.Select, databaseName)
		if err != nil {
			return nil, err
		}
		return derivedJoinSourceFromResult(result, alias), nil
	case sqlparser.TableName:
		if alias == "" {
			alias = expression.Name.String()
		}
		if result := materialized[strings.ToLower(alias)]; result != nil {
			return derivedJoinSourceFromResult(result, alias), nil
		}
		selectStmt := &sqlparser.Select{
			SelectExprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
			From:        sqlparser.TableExprs{&sqlparser.AliasedTableExpr{Expr: expression, As: table.As}},
		}
		if ctx == nil {
			ctx = &ExecutionContext{Context: context.Background()}
		}
		result, err := e.executeSelectStatement(ctx, selectStmt, databaseName)
		if err != nil {
			return nil, err
		}
		return derivedJoinSourceFromResult(result, alias), nil
	default:
		return nil, fmt.Errorf("unsupported derived join source %T", table.Expr)
	}
}

func derivedJoinSourceFromResult(result *SelectResult, alias string) *derivedJoinSource {
	source := &derivedJoinSource{rows: make([]derivedRow, 0), columns: make([]string, 0), qualifiedCols: map[string][]string{}}
	if result == nil {
		return source
	}
	for _, column := range result.Columns {
		clean := strings.Trim(column, "`")
		source.columns = append(source.columns, clean)
		source.qualifiedCols[strings.ToLower(alias)] = append(source.qualifiedCols[strings.ToLower(alias)], clean)
	}
	for _, record := range result.Records {
		row := make(map[string]interface{}, len(result.Columns)*4)
		for index, column := range result.Columns {
			if index >= len(record.GetValues()) {
				continue
			}
			value := derivedNormalizeValue(derivedBasicValueInterface(record.GetValues()[index]), derivedColumnType(result, index))
			clean := strings.Trim(column, "`")
			row[clean] = value
			row[strings.ToLower(clean)] = value
			row[alias+"."+clean] = value
			row[strings.ToLower(alias+"."+clean)] = value
		}
		source.rows = append(source.rows, derivedRow{values: row})
	}
	return source
}

func joinDerivedSources(left, right *derivedJoinSource, join string, condition sqlparser.JoinCondition, subqueryEvaluator joinSubqueryEvaluator, setSubqueryEvaluator joinSetSubqueryEvaluator) (*derivedJoinSource, error) {
	if left == nil || right == nil {
		return nil, fmt.Errorf("derived join source is nil")
	}
	result := &derivedJoinSource{
		rows:          make([]derivedRow, 0),
		columns:       append(append([]string(nil), left.columns...), right.columns...),
		qualifiedCols: cloneDerivedQualifiedColumns(left.qualifiedCols),
	}
	for alias, columns := range right.qualifiedCols {
		result.qualifiedCols[alias] = append(result.qualifiedCols[alias], columns...)
	}
	joinLower := strings.ToLower(strings.TrimSpace(join))
	leftJoin := strings.Contains(joinLower, "left")
	rightJoin := strings.Contains(joinLower, "right")
	rightNull := derivedJoinNullRow(right)
	leftNull := derivedJoinNullRow(left)
	if rightJoin {
		for _, rightRow := range right.rows {
			matched := false
			for _, leftRow := range left.rows {
				ok, err := derivedJoinRowsMatch(leftRow, rightRow, condition, subqueryEvaluator, setSubqueryEvaluator)
				if err != nil {
					return nil, err
				}
				if ok {
					result.rows = append(result.rows, mergeDerivedRows(leftRow, rightRow))
					matched = true
				}
			}
			if !matched {
				result.rows = append(result.rows, mergeDerivedRows(leftNull, rightRow))
			}
		}
		return result, nil
	}
	for _, leftRow := range left.rows {
		matched := false
		for _, rightRow := range right.rows {
			ok, err := derivedJoinRowsMatch(leftRow, rightRow, condition, subqueryEvaluator, setSubqueryEvaluator)
			if err != nil {
				return nil, err
			}
			if ok {
				result.rows = append(result.rows, mergeDerivedRows(leftRow, rightRow))
				matched = true
			}
		}
		if leftJoin && !matched {
			result.rows = append(result.rows, mergeDerivedRows(leftRow, rightNull))
		}
	}
	return result, nil
}

func derivedJoinRowsMatch(left, right derivedRow, condition sqlparser.JoinCondition, subqueryEvaluator joinSubqueryEvaluator, setSubqueryEvaluator joinSetSubqueryEvaluator) (bool, error) {
	if condition.On != nil {
		return derivedJoinConditionMatches(mergeDerivedRows(left, right), condition, subqueryEvaluator, setSubqueryEvaluator)
	}
	for _, column := range condition.Using {
		name := strings.Trim(column.String(), "`")
		leftValue, leftOK := derivedValueByName(left.values, name)
		rightValue, rightOK := derivedValueByName(right.values, name)
		if !leftOK || !rightOK || compareScalarValues(leftValue, rightValue) != 0 {
			return false, nil
		}
	}
	return true, nil
}

func derivedJoinConditionMatches(row derivedRow, condition sqlparser.JoinCondition, subqueryEvaluator joinSubqueryEvaluator, setSubqueryEvaluator joinSetSubqueryEvaluator) (bool, error) {
	if condition.On != nil {
		if subqueryEvaluator != nil {
			var evaluationErr error
			evaluator := func(subquery *sqlparser.Subquery, candidate joinedMapRow) (interface{}, error) {
				value, err := subqueryEvaluator(subquery, candidate)
				if err != nil && evaluationErr == nil {
					evaluationErr = err
				}
				return value, err
			}
			var setEvaluator joinSetSubqueryEvaluator
			if setSubqueryEvaluator != nil {
				setEvaluator = func(subquery *sqlparser.Subquery, candidate joinedMapRow) ([]interface{}, error) {
					values, err := setSubqueryEvaluator(subquery, candidate)
					if err != nil && evaluationErr == nil {
						evaluationErr = err
					}
					return values, err
				}
			}
			truth := evalJoinPredicateTruthWithEvaluator(condition.On, joinedMapRow(row.values), evaluator, setEvaluator)
			if evaluationErr != nil {
				return false, evaluationErr
			}
			return truth == sqlTruthTrue, nil
		}
		return evalPredicate(condition.On, row.values)
	}
	for _, column := range condition.Using {
		name := strings.Trim(column.String(), "`")
		value, ok := derivedValueByName(row.values, name)
		if !ok || value == nil {
			return false, nil
		}
	}
	return true, nil
}

func mergeDerivedRows(left, right derivedRow) derivedRow {
	values := cloneDerivedValues(left.values)
	for key, value := range right.values {
		values[key] = value
	}
	return derivedRow{values: values}
}

func derivedJoinNullRow(source *derivedJoinSource) derivedRow {
	values := make(map[string]interface{})
	for alias, columns := range source.qualifiedCols {
		for _, column := range columns {
			values[alias+"."+column] = nil
			values[strings.ToLower(alias+"."+column)] = nil
		}
	}
	for _, column := range source.columns {
		if _, exists := values[column]; !exists {
			values[column] = nil
		}
		values[strings.ToLower(column)] = nil
	}
	return derivedRow{values: values}
}

func cloneDerivedValues(values map[string]interface{}) map[string]interface{} {
	cloned := make(map[string]interface{}, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneDerivedQualifiedColumns(values map[string][]string) map[string][]string {
	cloned := make(map[string][]string, len(values))
	for key, columns := range values {
		cloned[key] = append([]string(nil), columns...)
	}
	return cloned
}
