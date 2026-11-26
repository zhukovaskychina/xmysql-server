package engine

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func queryHasJoinedOuterSource(query string) bool {
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return false
	}
	selectStatement, ok := statement.(*sqlparser.Select)
	return ok && selectHasJoin(selectStatement)
}

// executeCorrelatedSetSubqueryCompatibility evaluates correlated IN/NOT IN
// and EXISTS predicates in a simple single-table SELECT. The inner query is
// executed for each outer row through the normal SELECT executor so SQL NULL,
// type conversion, permissions, and storage visibility stay on the ordinary
// query path.
func (e *XMySQLExecutor) executeCorrelatedSetSubqueryCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return false, nil
	}
	selectStatement, ok := statement.(*sqlparser.Select)
	if !ok || len(selectStatement.From) != 1 || selectStatement.Where == nil && len(selectStatement.SelectExprs) == 0 {
		return false, nil
	}
	if len(selectStatement.GroupBy) != 0 || selectStatement.Having != nil || len(selectStatement.Windows) != 0 || selectStatement.Lock != "" ||
		selectStatement.Cache != "" || selectStatement.Hints != "" || len(selectStatement.Comments) != 0 {
		return false, nil
	}
	if correlatedSelectHasAggregateOrWindow(selectStatement) {
		return false, nil
	}
	var orderTerms []string
	if len(selectStatement.OrderBy) > 0 {
		var supported bool
		orderTerms, supported = correlatedProjectionOrderTerms(selectStatement.OrderBy, selectStatement.SelectExprs)
		if !supported || sqlNodeHasSubquery(selectStatement.OrderBy) {
			return false, nil
		}
	}
	aliased, ok := selectStatement.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return false, nil
	}
	tableName, ok := aliased.Expr.(sqlparser.TableName)
	if !ok {
		return false, nil
	}
	outerAlias := strings.TrimSpace(aliased.As.String())
	if outerAlias == "" {
		outerAlias = tableName.Name.String()
	}

	hasCorrelatedSetSubquery := false
	deferOuterWhere := selectStatement.Where != nil && expressionHasSubquery(selectStatement.Where.Expr)
	if deferOuterWhere {
		if !booleanSQLExpression(selectStatement.Where.Expr) {
			return false, nil
		}
		valid, correlated := correlatedSetPredicateShape(selectStatement.Where.Expr, outerAlias)
		if !valid {
			return false, nil
		}
		hasCorrelatedSetSubquery = correlated
	}
	for _, selectExpression := range selectStatement.SelectExprs {
		projection, ok := selectExpression.(*sqlparser.AliasedExpr)
		if !ok || !booleanSQLExpression(projection.Expr) || !expressionHasSubquery(projection.Expr) {
			continue
		}
		valid, correlated := correlatedSetPredicateShape(projection.Expr, outerAlias)
		if !valid {
			return false, nil
		}
		hasCorrelatedSetSubquery = hasCorrelatedSetSubquery || correlated
	}
	if !hasCorrelatedSetSubquery {
		return false, nil
	}

	outerColumns := correlatedOuterProjectionColumns(selectStatement, outerAlias)
	outerSelectExprs := make(sqlparser.SelectExprs, 0, len(outerColumns))
	if correlatedProjectionHasStar(selectStatement.SelectExprs) {
		for _, expression := range selectStatement.SelectExprs {
			if star, ok := expression.(*sqlparser.StarExpr); ok {
				outerSelectExprs = append(outerSelectExprs, star)
				break
			}
		}
	} else {
		for _, column := range outerColumns {
			outerSelectExprs = append(outerSelectExprs, &sqlparser.AliasedExpr{Expr: column})
		}
	}
	if len(outerSelectExprs) == 0 {
		return false, nil
	}
	outerWhere := selectStatement.Where
	if deferOuterWhere {
		outerWhere = nil
	}
	outerStatement := &sqlparser.Select{SelectExprs: outerSelectExprs, From: selectStatement.From, Where: outerWhere}
	outerResult, err := e.executeSelectStatement(ctx, outerStatement, databaseName)
	if err != nil {
		return true, err
	}
	type projectedColumn struct {
		expression  sqlparser.Expr
		name        string
		typ         metadata.DataType
		sourceIndex int
	}
	projected := make([]projectedColumn, 0, len(selectStatement.SelectExprs))
	for _, selectExpression := range selectStatement.SelectExprs {
		if star, ok := selectExpression.(*sqlparser.StarExpr); ok {
			if !star.TableName.IsEmpty() && !strings.EqualFold(star.TableName.Name.String(), outerAlias) && !strings.EqualFold(star.TableName.Name.String(), tableName.Name.String()) {
				return false, nil
			}
			for sourceIndex, name := range outerResult.Columns {
				typ := metadata.TypeVarchar
				if sourceIndex < len(outerResult.ColumnTypes) {
					typ = metadata.DataType(strings.ToUpper(outerResult.ColumnTypes[sourceIndex]))
				}
				projected = append(projected, projectedColumn{name: name, typ: typ, sourceIndex: sourceIndex})
			}
			continue
		}
		projection, ok := selectExpression.(*sqlparser.AliasedExpr)
		if !ok {
			return false, nil
		}
		name := projection.As.String()
		if name == "" {
			if column, ok := projection.Expr.(*sqlparser.ColName); ok {
				name = column.Name.String()
			} else {
				name = sqlparser.String(projection.Expr)
			}
		}
		typ := metadata.TypeVarchar
		if booleanSQLExpression(projection.Expr) {
			typ = metadata.TypeBigInt
		} else if column, ok := projection.Expr.(*sqlparser.ColName); ok {
			if sourceIndex := correlatedProjectionColumnIndex(outerResult.Columns, column.Name.String()); sourceIndex >= 0 && sourceIndex < len(outerResult.ColumnTypes) {
				typ = metadata.DataType(strings.ToUpper(outerResult.ColumnTypes[sourceIndex]))
			}
		}
		projected = append(projected, projectedColumn{expression: projection.Expr, name: name, typ: typ, sourceIndex: -1})
	}

	meta := &metadata.TableMeta{Name: "correlated_set_projection", Columns: make([]*metadata.ColumnMeta, len(projected))}
	for index, projection := range projected {
		meta.Columns[index] = &metadata.ColumnMeta{Name: projection.name, Type: projection.typ}
	}
	rows := make([]Record, 0, len(outerResult.Records))
	for _, outerRecord := range outerResult.Records {
		rowValues := correlatedOuterRowValues(outerRecord, outerResult.Columns, outerResult.ColumnTypes, outerAlias, tableName.Name.String())
		joinedRow := joinedMapRow(rowValues)
		evaluateSetPredicate := func(expression sqlparser.Expr) (int, error) {
			var evaluationErr error
			setEvaluator := joinSetSubqueryEvaluator(func(subquery *sqlparser.Subquery, candidate joinedMapRow) ([]interface{}, error) {
				resultValues, _, subqueryErr := e.evaluateJoinSubqueryValues(ctx, subquery, candidate, databaseName)
				if subqueryErr != nil && evaluationErr == nil {
					evaluationErr = subqueryErr
				}
				return resultValues, subqueryErr
			})
			truth := evalJoinPredicateTruthWithEvaluator(expression, joinedRow, nil, setEvaluator)
			return truth, evaluationErr
		}
		if deferOuterWhere {
			truth, evalErr := evaluateSetPredicate(selectStatement.Where.Expr)
			if evalErr != nil {
				return true, evalErr
			}
			if truth != sqlTruthTrue {
				continue
			}
		}
		values := make([]interface{}, len(projected))
		for index, projection := range projected {
			if projection.sourceIndex >= 0 {
				if projection.sourceIndex < len(outerRecord.GetValues()) {
					values[index] = correlatedProjectionSourceValue(outerRecord.GetValues()[projection.sourceIndex], projection.typ)
				}
				continue
			}
			if booleanSQLExpression(projection.expression) && expressionHasSubquery(projection.expression) {
				truth, evalErr := evaluateSetPredicate(projection.expression)
				if evalErr != nil {
					return true, evalErr
				}
				switch truth {
				case sqlTruthUnknown:
					values[index] = nil
				case sqlTruthTrue:
					values[index] = "1"
				default:
					values[index] = "0"
				}
				continue
			}
			value, evalErr := evaluateExpressionWithRow(projection.expression, rowValues)
			if evalErr != nil {
				return true, evalErr
			}
			values[index] = correlatedProjectionOutputValue(value, projection.typ)
		}
		rows = append(rows, NewExecutorRecordFromInterface(values, meta))
	}
	if selectStatement.Distinct != "" {
		rows = correlatedDistinctRecords(rows)
	}
	if len(selectStatement.OrderBy) > 0 {
		if err := sortCorrelatedRecords(rows, strings.Join(orderTerms, ", "), meta); err != nil {
			return true, err
		}
	}
	if selectStatement.Limit != nil {
		limitClause := sqlparser.String(selectStatement.Limit.Rowcount)
		if selectStatement.Limit.Offset != nil {
			limitClause = sqlparser.String(selectStatement.Limit.Offset) + ", " + limitClause
		}
		rows, err = correlatedLimitRecords(rows, limitClause)
		if err != nil {
			return true, err
		}
	}
	columns := make([]string, len(projected))
	columnTypes := make([]string, len(projected))
	for index, projection := range projected {
		columns[index] = projection.name
		columnTypes[index] = strings.ToLower(string(projection.typ))
	}
	result := &SelectResult{Records: rows, RowCount: len(rows), Columns: columns, ColumnTypes: columnTypes, ResultType: common.RESULT_TYPE_QUERY, Message: fmt.Sprintf("Query OK, %d rows in set", len(rows))}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: result.Message}
	return true, nil
}

func correlatedProjectionSourceValue(value basic.Value, declaredType metadata.DataType) interface{} {
	if value == nil || value.IsNull() {
		return nil
	}
	switch metadata.DataType(strings.ToUpper(strings.TrimSpace(string(declaredType)))) {
	case metadata.TypeTinyInt, metadata.TypeSmallInt, metadata.TypeMediumInt, metadata.TypeInt, metadata.TypeBigInt, metadata.TypeYear:
		if raw, ok := value.Raw().([]byte); ok {
			if parsed, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64); err == nil {
				return strconv.FormatInt(parsed, 10)
			}
			if len(raw) == 8 {
				return strconv.FormatInt(int64(binary.BigEndian.Uint64(raw)), 10)
			}
		}
		return strconv.FormatInt(value.Int(), 10)
	}
	return basicValueToInterface(value)
}

func correlatedProjectionOutputValue(value interface{}, declaredType metadata.DataType) interface{} {
	switch metadata.DataType(strings.ToUpper(strings.TrimSpace(string(declaredType)))) {
	case metadata.TypeTinyInt, metadata.TypeSmallInt, metadata.TypeMediumInt, metadata.TypeInt, metadata.TypeBigInt, metadata.TypeYear:
		switch typed := value.(type) {
		case int:
			return strconv.FormatInt(int64(typed), 10)
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
		case uint8:
			return strconv.FormatUint(uint64(typed), 10)
		case uint16:
			return strconv.FormatUint(uint64(typed), 10)
		case uint32:
			return strconv.FormatUint(uint64(typed), 10)
		case uint64:
			return strconv.FormatUint(typed, 10)
		case []byte:
			if parsed, err := strconv.ParseInt(strings.TrimSpace(string(typed)), 10, 64); err == nil {
				return strconv.FormatInt(parsed, 10)
			}
			if len(typed) == 8 {
				return strconv.FormatInt(int64(binary.BigEndian.Uint64(typed)), 10)
			}
		}
	}
	return value
}

func correlatedProjectionOutputRecord(values []basic.Value, sourceIndexes []int, sourceTypes []string, meta *metadata.TableMeta) Record {
	projected := make([]interface{}, len(values))
	for index, value := range values {
		declaredType := metadata.DataType("")
		if index < len(sourceIndexes) && sourceIndexes[index] >= 0 && sourceIndexes[index] < len(sourceTypes) {
			declaredType = metadata.DataType(strings.ToUpper(sourceTypes[sourceIndexes[index]]))
		} else if meta != nil && index < len(meta.Columns) && meta.Columns[index] != nil {
			declaredType = meta.Columns[index].Type
		}
		projected[index] = correlatedProjectionSourceValue(value, declaredType)
	}
	return NewExecutorRecordFromInterface(projected, meta)
}

func correlatedProjectionHasStar(expressions sqlparser.SelectExprs) bool {
	for _, expression := range expressions {
		if _, ok := expression.(*sqlparser.StarExpr); ok {
			return true
		}
	}
	return false
}

func sqlNodeHasSubquery(node sqlparser.SQLNode) bool {
	found := false
	_ = sqlparser.Walk(func(part sqlparser.SQLNode) (bool, error) {
		if _, ok := part.(*sqlparser.Subquery); ok {
			found = true
			return false, nil
		}
		return !found, nil
	}, node)
	return found
}

func correlatedSelectHasAggregateOrWindow(statement *sqlparser.Select) bool {
	if statement == nil {
		return false
	}
	contains := false
	aggregateFunctions := map[string]bool{
		"any_value": true, "avg": true, "bit_and": true, "bit_or": true, "bit_xor": true, "count": true,
		"group_concat": true, "max": true, "min": true, "sum": true, "std": true, "stddev": true,
		"stddev_pop": true, "stddev_samp": true, "var_pop": true, "var_samp": true, "variance": true,
		"json_arrayagg": true, "json_objectagg": true,
	}
	inspect := func(root sqlparser.SQLNode) {
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			if _, subquery := node.(*sqlparser.Subquery); subquery {
				return false, nil
			}
			if function, ok := node.(*sqlparser.FuncExpr); ok {
				if function.Over != nil || aggregateFunctions[strings.ToLower(function.Name.String())] {
					contains = true
					return false, nil
				}
			}
			return !contains, nil
		}, root)
	}
	for _, projection := range statement.SelectExprs {
		inspect(projection)
	}
	if statement.Where != nil {
		inspect(statement.Where.Expr)
	}
	return contains
}

func correlatedProjectionOrderTerms(orderBy sqlparser.OrderBy, selectExprs sqlparser.SelectExprs) ([]string, bool) {
	terms := make([]string, 0, len(orderBy))
	for _, order := range orderBy {
		var outputName string
		if ordinal, ok := order.Expr.(*sqlparser.SQLVal); ok && ordinal.Type == sqlparser.IntVal {
			position, err := strconv.Atoi(string(ordinal.Val))
			if err != nil || position < 1 || position > len(selectExprs) {
				return nil, false
			}
			projection, ok := selectExprs[position-1].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, false
			}
			outputName = correlatedProjectionOutputName(projection)
		} else {
			requested := canonicalProjectionExpression(sqlparser.String(order.Expr))
			for _, selectExpression := range selectExprs {
				projection, ok := selectExpression.(*sqlparser.AliasedExpr)
				if !ok {
					continue
				}
				name := correlatedProjectionOutputName(projection)
				if strings.EqualFold(name, strings.Trim(sqlparser.String(order.Expr), "`")) ||
					canonicalProjectionExpression(sqlparser.String(projection.Expr)) == requested {
					outputName = name
					break
				}
			}
		}
		if strings.TrimSpace(outputName) == "" {
			return nil, false
		}
		term := sqlparser.String(&sqlparser.ColName{Name: sqlparser.NewColIdent(outputName)})
		if order.Direction == sqlparser.DescScr {
			term += " desc"
		}
		terms = append(terms, term)
	}
	return terms, len(terms) == len(orderBy)
}

func correlatedProjectionOutputName(projection *sqlparser.AliasedExpr) string {
	if projection == nil {
		return ""
	}
	if !projection.As.IsEmpty() {
		return projection.As.String()
	}
	if column, ok := projection.Expr.(*sqlparser.ColName); ok {
		return column.Name.String()
	}
	return sqlparser.String(projection.Expr)
}

func canonicalProjectionExpression(expression string) string {
	expression = strings.ToLower(strings.TrimSpace(expression))
	return strings.ReplaceAll(expression, "`", "")
}

func correlatedSetPredicateShape(expression sqlparser.Expr, outerAlias string) (valid, correlated bool) {
	valid = true
	allowed := make(map[*sqlparser.Subquery]bool)
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch value := node.(type) {
		case *sqlparser.ExistsExpr:
			if value.Subquery != nil {
				allowed[value.Subquery] = true
			}
		case *sqlparser.ComparisonExpr:
			if value.Operator != sqlparser.InStr && value.Operator != sqlparser.NotInStr {
				if expressionHasSubquery(value) {
					valid = false
				}
				return true, nil
			}
			if subquery := correlatedProjectionSubquery(value.Right); subquery != nil {
				allowed[subquery] = true
			}
		}
		return true, nil
	}, expression)
	if !valid {
		return false, false
	}
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if subquery, ok := node.(*sqlparser.Subquery); ok {
			if !allowed[subquery] {
				valid = false
				return false, nil
			}
			correlated = correlated || sqlHasQualifierDot(sqlparser.String(subquery.Select), outerAlias)
		}
		return valid, nil
	}, expression)
	return valid, correlated
}

func correlatedProjectionSubquery(expression sqlparser.Expr) *sqlparser.Subquery {
	for {
		switch value := expression.(type) {
		case *sqlparser.Subquery:
			return value
		case *sqlparser.ParenExpr:
			expression = value.Expr
		default:
			return nil
		}
	}
}

func correlatedOuterProjectionColumns(statement *sqlparser.Select, outerAlias string) []*sqlparser.ColName {
	columns := make([]*sqlparser.ColName, 0)
	seen := make(map[string]struct{})
	add := func(column *sqlparser.ColName, requireOuterQualifier bool) {
		if column == nil {
			return
		}
		qualifier := strings.TrimSpace(column.Qualifier.Name.String())
		if requireOuterQualifier && !strings.EqualFold(qualifier, outerAlias) {
			return
		}
		if !requireOuterQualifier && qualifier != "" && !strings.EqualFold(qualifier, outerAlias) {
			return
		}
		key := strings.ToLower(strings.TrimSpace(sqlparser.String(column)))
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		copy := *column
		columns = append(columns, &copy)
	}
	collect := func(root sqlparser.SQLNode) {}
	collect = func(root sqlparser.SQLNode) {
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			switch value := node.(type) {
			case *sqlparser.Subquery:
				_ = sqlparser.Walk(func(inner sqlparser.SQLNode) (bool, error) {
					if nested, ok := inner.(*sqlparser.Subquery); ok {
						// Visit nested subqueries separately so only references to
						// this outer scope are projected into the source query.
						_ = sqlparser.Walk(func(nestedNode sqlparser.SQLNode) (bool, error) {
							if col, ok := nestedNode.(*sqlparser.ColName); ok {
								add(col, true)
							}
							return true, nil
						}, nested.Select)
						return false, nil
					}
					if col, ok := inner.(*sqlparser.ColName); ok {
						add(col, true)
					}
					return true, nil
				}, value.Select)
				return false, nil
			case *sqlparser.ColName:
				add(value, false)
			}
			return true, nil
		}, root)
	}
	for _, projection := range statement.SelectExprs {
		collect(projection)
	}
	if statement.Where != nil {
		collect(statement.Where.Expr)
	}
	return columns
}

func correlatedProjectionColumnIndex(columns []string, requested string) int {
	requested = strings.Trim(strings.TrimSpace(requested), "`")
	if dot := strings.LastIndex(requested, "."); dot >= 0 {
		requested = strings.Trim(requested[dot+1:], "`")
	}
	for index, column := range columns {
		if strings.EqualFold(strings.Trim(column, "`"), requested) {
			return index
		}
	}
	return -1
}
