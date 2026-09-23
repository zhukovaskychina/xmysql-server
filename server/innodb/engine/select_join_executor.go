package engine

import (
	"context"
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

type joinTableSource struct {
	tableName string
	alias     string
	meta      *metadata.TableMeta
	rows      []map[string]interface{}
}

type joinedMapRow map[string]interface{}

func selectHasJoin(stmt *sqlparser.Select) bool {
	if stmt == nil {
		return false
	}
	if len(stmt.From) > 1 {
		return true
	}
	for _, from := range stmt.From {
		if _, ok := from.(*sqlparser.JoinTableExpr); ok {
			return true
		}
	}
	return false
}

func (se *SelectExecutor) executeJoinSelect(ctx context.Context, stmt *sqlparser.Select, schemaName string) (*SelectResult, error) {
	se.resetExecutionState()
	se.schemaName = schemaName
	se.distinct = strings.EqualFold(strings.TrimSpace(stmt.Distinct), strings.TrimSpace(sqlparser.DistinctStr))
	if err := se.parseSelectExprs(stmt.SelectExprs); err != nil {
		return nil, err
	}
	if stmt.Where != nil {
		se.whereConditions = se.parseWhereConditions(stmt.Where.Expr)
	}
	for _, expr := range stmt.GroupBy {
		se.groupByColumns = append(se.groupByColumns, sqlparser.String(expr))
	}
	if stmt.Having != nil {
		se.havingCondition = sqlparser.String(stmt.Having.Expr)
	}
	if err := se.parseOrderBy(stmt.OrderBy); err != nil {
		return nil, err
	}
	if err := se.parseLimit(stmt.Limit); err != nil {
		return nil, err
	}

	rows, err := se.evaluateJoinFrom(ctx, stmt.From[0])
	if err != nil {
		return nil, err
	}
	for _, from := range stmt.From[1:] {
		rightRows, rightErr := se.evaluateJoinFrom(ctx, from)
		if rightErr != nil {
			return nil, rightErr
		}
		rows, err = crossJoinRows(rows, rightRows)
		if err != nil {
			return nil, err
		}
	}
	if hasEffectiveWhereConditions(se.whereConditions) {
		filtered := make([]joinedMapRow, 0, len(rows))
		for _, row := range rows {
			matched, err := rowMatchesWhereConditions(map[string]interface{}(row), se.whereConditions)
			if err != nil {
				return nil, err
			}
			if matched {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}
	if se.hasAggregateQuery() || len(se.groupByColumns) > 0 {
		return se.buildJoinAggregateResult(rows), nil
	}
	se.sortJoinedRows(rows)
	records, columns, columnTypes := se.projectJoinedRows(rows)
	records = se.applyDistinct(records)
	records = se.applyLimitOffset(records)
	return &SelectResult{
		Records:     records,
		RowCount:    len(records),
		Columns:     columns,
		ColumnTypes: columnTypes,
		ResultType:  common.RESULT_TYPE_QUERY,
		Message:     fmt.Sprintf("Query OK, %d rows in set", len(records)),
	}, nil
}

func crossJoinRows(leftRows, rightRows []joinedMapRow) ([]joinedMapRow, error) {
	joined := make([]joinedMapRow, 0, len(leftRows)*len(rightRows))
	for _, left := range leftRows {
		for _, right := range rightRows {
			joined = append(joined, mergeJoinedRows(left, right))
		}
	}
	return joined, nil
}

func (se *SelectExecutor) evaluateJoinFrom(ctx context.Context, expr sqlparser.TableExpr) ([]joinedMapRow, error) {
	switch v := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		source, err := se.loadJoinTableSource(ctx, v)
		if err != nil {
			return nil, err
		}
		rows := make([]joinedMapRow, 0, len(source.rows))
		for _, row := range source.rows {
			rows = append(rows, joinedMapRow(row))
		}
		return rows, nil
	case *sqlparser.JoinTableExpr:
		leftRows, err := se.evaluateJoinFrom(ctx, v.LeftExpr)
		if err != nil {
			return nil, err
		}
		rightSource, err := se.loadJoinRightSource(ctx, v.RightExpr)
		if err != nil {
			return nil, err
		}
		return se.joinRows(leftRows, rightSource, v.Join, v.Condition)
	default:
		return nil, fmt.Errorf("unsupported JOIN FROM expression type: %T", expr)
	}
}

func (se *SelectExecutor) loadJoinRightSource(ctx context.Context, expr sqlparser.TableExpr) (*joinTableSource, error) {
	aliased, ok := expr.(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, fmt.Errorf("unsupported JOIN right expression type: %T", expr)
	}
	return se.loadJoinTableSource(ctx, aliased)
}

func (se *SelectExecutor) loadJoinTableSource(ctx context.Context, expr *sqlparser.AliasedTableExpr) (*joinTableSource, error) {
	tableName, ok := expr.Expr.(sqlparser.TableName)
	if !ok {
		return nil, fmt.Errorf("unsupported JOIN table expression type: %T", expr.Expr)
	}
	tableSchema := se.schemaName
	if qualifier := strings.TrimSpace(tableName.Qualifier.String()); qualifier != "" {
		tableSchema = qualifier
	}
	alias := expr.As.String()
	if strings.TrimSpace(alias) == "" {
		alias = tableName.Name.String()
	}
	meta, err := se.loadTableMetaFromFrm(se.dataDir, tableSchema, tableName.Name.String())
	if err != nil {
		return nil, err
	}
	btreeManager := se.btreeManager
	if se.storageManager != nil && se.storageManager.GetTableStorageManager() != nil {
		if tableBTreeManager, err := se.storageManager.GetTableStorageManager().CreateBTreeManagerForTable(ctx, tableSchema, tableName.Name.String()); err == nil && tableBTreeManager != nil {
			btreeManager = tableBTreeManager
		}
	}
	if btreeManager == nil && se.tableManager != nil {
		if tableBTreeManager, err := se.tableManager.GetTableBTreeManager(ctx, tableSchema, tableName.Name.String()); err == nil && tableBTreeManager != nil {
			btreeManager = tableBTreeManager
		}
	}
	if btreeManager == nil {
		return nil, fmt.Errorf("missing B+Tree manager for JOIN table %s", tableName.Name.String())
	}
	scanner := NewClusteredIndexScanner(btreeManager, meta)
	rowData, err := scanner.Scan(ctx, nil)
	if err != nil {
		return nil, err
	}
	source := &joinTableSource{tableName: tableName.Name.String(), alias: alias, meta: meta}
	if se.joinOrderTypes == nil {
		se.joinOrderTypes = make(map[string]metadata.DataType)
	}
	for _, column := range meta.Columns {
		if column == nil {
			continue
		}
		for _, qualified := range []string{
			alias + "." + column.Name,
			tableName.Name.String() + "." + column.Name,
		} {
			se.joinOrderTypes[strings.ToLower(qualified)] = column.Type
		}
	}
	for _, row := range rowData {
		source.rows = append(source.rows, qualifiedJoinRow(source, row.ColumnValues))
	}
	return source, nil
}

func qualifiedJoinRow(source *joinTableSource, values map[string]interface{}) map[string]interface{} {
	row := make(map[string]interface{}, len(values)*3)
	for _, col := range source.meta.Columns {
		if col == nil {
			continue
		}
		value := values[col.Name]
		row[col.Name] = value
		row[strings.ToLower(col.Name)] = value
		row[source.alias+"."+col.Name] = value
		row[strings.ToLower(source.alias+"."+col.Name)] = value
		row[source.tableName+"."+col.Name] = value
		row[strings.ToLower(source.tableName+"."+col.Name)] = value
	}
	return row
}

func nullJoinRow(source *joinTableSource) map[string]interface{} {
	row := make(map[string]interface{}, len(source.meta.Columns)*3)
	for _, col := range source.meta.Columns {
		if col == nil {
			continue
		}
		row[col.Name] = nil
		row[strings.ToLower(col.Name)] = nil
		row[source.alias+"."+col.Name] = nil
		row[strings.ToLower(source.alias+"."+col.Name)] = nil
		row[source.tableName+"."+col.Name] = nil
		row[strings.ToLower(source.tableName+"."+col.Name)] = nil
	}
	return row
}

func (se *SelectExecutor) joinRows(leftRows []joinedMapRow, right *joinTableSource, joinType string, condition sqlparser.JoinCondition) ([]joinedMapRow, error) {
	joined := make([]joinedMapRow, 0)
	rightMatched := make([]bool, len(right.rows))
	for _, left := range leftRows {
		matched := false
		for i, rightRow := range right.rows {
			merged := mergeJoinedRows(left, joinedMapRow(rightRow))
			matchedRow := true
			if condition.On != nil {
				var err error
				matchedRow, err = se.evalJoinPredicateWithError(condition.On, merged)
				if err != nil {
					return nil, err
				}
			} else if len(condition.Using) > 0 {
				matchedRow = joinUsingRowsMatch(left, joinedMapRow(rightRow), condition.Using)
			}
			if matchedRow {
				joined = append(joined, merged)
				rightMatched[i] = true
				matched = true
			}
		}
		if !matched && strings.EqualFold(joinType, sqlparser.LeftJoinStr) {
			joined = append(joined, mergeJoinedRows(left, joinedMapRow(nullJoinRow(right))))
		}
	}
	if strings.EqualFold(joinType, sqlparser.RightJoinStr) {
		for i, rightRow := range right.rows {
			if rightMatched[i] {
				continue
			}
			joined = append(joined, joinedMapRow(rightRow))
		}
	}
	return joined, nil
}

func joinUsingRowsMatch(left, right joinedMapRow, columns sqlparser.Columns) bool {
	for _, column := range columns {
		name := strings.Trim(column.String(), "` ")
		leftValue, leftOK := resolveExpressionRowValue(map[string]interface{}(left), name)
		rightValue, rightOK := resolveExpressionRowValue(map[string]interface{}(right), name)
		if !leftOK || !rightOK || leftValue == nil || rightValue == nil || compareScalarValues(leftValue, rightValue) != 0 {
			return false
		}
	}
	return true
}

func mergeJoinedRows(left, right joinedMapRow) joinedMapRow {
	merged := make(joinedMapRow, len(left)+len(right))
	for k, v := range left {
		merged[k] = v
	}
	for k, v := range right {
		merged[k] = v
	}
	return merged
}

func evalJoinPredicate(expr sqlparser.Expr, row joinedMapRow) bool {
	return evalJoinPredicateTruthWithEvaluator(expr, row, nil, nil) == sqlTruthTrue
}

func evalJoinPredicateTruth(expr sqlparser.Expr, row joinedMapRow) int {
	return evalJoinPredicateTruthWithEvaluator(expr, row, nil, nil)
}

func (se *SelectExecutor) evalJoinPredicate(expr sqlparser.Expr, row joinedMapRow) bool {
	return se.evalJoinPredicateTruth(expr, row) == sqlTruthTrue
}

func (se *SelectExecutor) evalJoinPredicateWithError(expr sqlparser.Expr, row joinedMapRow) (bool, error) {
	if se.joinSubqueryEvaluator == nil && se.joinSetSubqueryEvaluator == nil {
		return se.evalJoinPredicate(expr, row), nil
	}
	var evaluationErr error
	var evaluator joinSubqueryEvaluator
	if se.joinSubqueryEvaluator != nil {
		evaluator = func(subquery *sqlparser.Subquery, candidate joinedMapRow) (interface{}, error) {
			value, err := se.joinSubqueryEvaluator(subquery, candidate)
			if err != nil && evaluationErr == nil {
				evaluationErr = err
			}
			return value, err
		}
	}
	var setEvaluator joinSetSubqueryEvaluator
	if se.joinSetSubqueryEvaluator != nil {
		setEvaluator = func(subquery *sqlparser.Subquery, candidate joinedMapRow) ([]interface{}, error) {
			values, err := se.joinSetSubqueryEvaluator(subquery, candidate)
			if err != nil && evaluationErr == nil {
				evaluationErr = err
			}
			return values, err
		}
	}
	truth := evalJoinPredicateTruthWithEvaluator(expr, row, evaluator, setEvaluator)
	if evaluationErr != nil {
		return false, evaluationErr
	}
	return truth == sqlTruthTrue, nil
}

func (se *SelectExecutor) evalJoinPredicateTruth(expr sqlparser.Expr, row joinedMapRow) int {
	return evalJoinPredicateTruthWithEvaluator(expr, row, se.joinSubqueryEvaluator, se.joinSetSubqueryEvaluator)
}

type joinSubqueryEvaluator func(*sqlparser.Subquery, joinedMapRow) (interface{}, error)
type joinSetSubqueryEvaluator func(*sqlparser.Subquery, joinedMapRow) ([]interface{}, error)

func evalJoinPredicateTruthWithEvaluator(expr sqlparser.Expr, row joinedMapRow, evaluator joinSubqueryEvaluator, setEvaluator joinSetSubqueryEvaluator) int {
	if expr == nil {
		return sqlTruthTrue
	}
	switch v := expr.(type) {
	case *sqlparser.AndExpr:
		left := evalJoinPredicateTruthWithEvaluator(v.Left, row, evaluator, setEvaluator)
		if left == sqlTruthFalse {
			return sqlTruthFalse
		}
		right := evalJoinPredicateTruthWithEvaluator(v.Right, row, evaluator, setEvaluator)
		if right == sqlTruthFalse {
			return sqlTruthFalse
		}
		if left == sqlTruthUnknown || right == sqlTruthUnknown {
			return sqlTruthUnknown
		}
		return sqlTruthTrue
	case *sqlparser.OrExpr:
		left := evalJoinPredicateTruthWithEvaluator(v.Left, row, evaluator, setEvaluator)
		if left == sqlTruthTrue {
			return sqlTruthTrue
		}
		right := evalJoinPredicateTruthWithEvaluator(v.Right, row, evaluator, setEvaluator)
		if right == sqlTruthTrue {
			return sqlTruthTrue
		}
		if left == sqlTruthUnknown || right == sqlTruthUnknown {
			return sqlTruthUnknown
		}
		return sqlTruthFalse
	case *sqlparser.NotExpr:
		truth := evalJoinPredicateTruthWithEvaluator(v.Expr, row, evaluator, setEvaluator)
		if truth == sqlTruthUnknown {
			return sqlTruthUnknown
		}
		if truth == sqlTruthTrue {
			return sqlTruthFalse
		}
		return sqlTruthTrue
	case *sqlparser.ComparisonExpr:
		left, leftOK := evalJoinValueWithEvaluator(v.Left, row, evaluator)
		if !leftOK {
			return sqlTruthFalse
		}
		if v.Operator == sqlparser.InStr || v.Operator == sqlparser.NotInStr {
			matched, unknown, err := evalJoinInPredicate(left, v.Right, row, setEvaluator)
			if err != nil {
				return sqlTruthFalse
			}
			if unknown {
				return sqlTruthUnknown
			}
			if v.Operator == sqlparser.NotInStr {
				return truthValue(!matched)
			}
			return truthValue(matched)
		}
		right, rightOK := evalJoinValueWithEvaluator(v.Right, row, evaluator)
		if !rightOK {
			return sqlTruthFalse
		}
		return evalJoinComparisonTruth(v.Operator, left, right)
	case *sqlparser.RangeCond:
		left, leftOK := evalJoinValueWithEvaluator(v.Left, row, evaluator)
		from, fromOK := evalJoinValueWithEvaluator(v.From, row, evaluator)
		to, toOK := evalJoinValueWithEvaluator(v.To, row, evaluator)
		if !leftOK || !fromOK || !toOK {
			return sqlTruthFalse
		}
		if left == nil || from == nil || to == nil {
			return sqlTruthUnknown
		}
		matched := compareScalarValues(left, from) >= 0 && compareScalarValues(left, to) <= 0
		if v.Operator == sqlparser.NotBetweenStr {
			return truthValue(!matched)
		}
		return truthValue(matched)
	case *sqlparser.IsExpr:
		value, ok := evalJoinValueWithEvaluator(v.Expr, row, evaluator)
		if !ok {
			return sqlTruthFalse
		}
		switch v.Operator {
		case sqlparser.IsNullStr:
			return truthValue(value == nil)
		case sqlparser.IsNotNullStr:
			return truthValue(value != nil)
		default:
			return sqlTruthFalse
		}
	case *sqlparser.ExistsExpr:
		if setEvaluator == nil || v.Subquery == nil {
			return sqlTruthFalse
		}
		values, err := setEvaluator(v.Subquery, row)
		if err != nil {
			return sqlTruthFalse
		}
		return truthValue(len(values) > 0)
	case *sqlparser.ParenExpr:
		return evalJoinPredicateTruthWithEvaluator(v.Expr, row, evaluator, setEvaluator)
	default:
		return sqlTruthFalse
	}
}

func evalJoinInPredicate(left interface{}, right sqlparser.Expr, row joinedMapRow, setEvaluator joinSetSubqueryEvaluator) (bool, bool, error) {
	if paren, ok := right.(*sqlparser.ParenExpr); ok {
		return evalJoinInPredicate(left, paren.Expr, row, setEvaluator)
	}
	if subquery, ok := right.(*sqlparser.Subquery); ok && setEvaluator != nil {
		values, err := setEvaluator(subquery, row)
		if err != nil {
			return false, false, err
		}
		hasNull := false
		for _, value := range values {
			if value == nil {
				hasNull = true
				continue
			}
			if compareScalarValues(left, value) == 0 {
				return true, false, nil
			}
		}
		return false, hasNull, nil
	}
	return evalInPredicate(left, right, map[string]interface{}(row))
}

func evalJoinComparison(operator string, left, right interface{}) bool {
	return evalJoinComparisonTruth(operator, left, right) == sqlTruthTrue
}

func evalJoinComparisonTruth(operator string, left, right interface{}) int {
	if operator == sqlparser.NullSafeEqualStr {
		if left == nil || right == nil {
			return truthValue(left == nil && right == nil)
		}
	}
	if left == nil || right == nil {
		return sqlTruthUnknown
	}
	if operator == sqlparser.RegexpStr || operator == sqlparser.NotRegexpStr {
		matched, err := regexp.MatchString(fmt.Sprint(right), fmt.Sprint(left))
		if err != nil {
			return sqlTruthFalse
		}
		if operator == sqlparser.NotRegexpStr {
			return truthValue(!matched)
		}
		return truthValue(matched)
	}
	if operator == sqlparser.LikeStr || operator == sqlparser.NotLikeStr {
		matched := sqlLikePatternMatch(fmt.Sprint(left), fmt.Sprint(right))
		if operator == sqlparser.NotLikeStr {
			return truthValue(!matched)
		}
		return truthValue(matched)
	}
	cmp := compareScalarValues(left, right)
	switch operator {
	case sqlparser.EqualStr:
		return truthValue(cmp == 0)
	case sqlparser.NotEqualStr:
		return truthValue(cmp != 0)
	case sqlparser.LessThanStr:
		return truthValue(cmp < 0)
	case sqlparser.LessEqualStr:
		return truthValue(cmp <= 0)
	case sqlparser.GreaterThanStr:
		return truthValue(cmp > 0)
	case sqlparser.GreaterEqualStr:
		return truthValue(cmp >= 0)
	default:
		return sqlTruthFalse
	}
}

func evalJoinValue(expr sqlparser.Expr, row joinedMapRow) (interface{}, bool) {
	return evalJoinValueWithEvaluator(expr, row, nil)
}

func evalJoinValueWithEvaluator(expr sqlparser.Expr, row joinedMapRow, evaluator joinSubqueryEvaluator) (interface{}, bool) {
	switch v := expr.(type) {
	case *sqlparser.ColName:
		key := joinColumnKey(v)
		value, ok := row[key]
		if !ok {
			value, ok = row[strings.ToLower(key)]
		}
		return value, ok
	case *sqlparser.SQLVal:
		value, err := (&StorageIntegratedDMLExecutor{}).parseSQLVal(v)
		return value, err == nil
	case *sqlparser.Subquery:
		if evaluator == nil {
			return nil, false
		}
		value, err := evaluator(v, row)
		return value, err == nil
	case *sqlparser.ParenExpr:
		return evalJoinValueWithEvaluator(v.Expr, row, evaluator)
	default:
		if evaluator != nil && strings.Contains(strings.ToLower(sqlparser.String(expr)), "select") {
			if value, ok := evalJoinExpressionWithSubqueries(expr, row, evaluator); ok {
				return value, true
			}
		}
		value, err := evaluateExpressionWithRow(expr, map[string]interface{}(row))
		if err != nil {
			coerced := make(map[string]interface{}, len(row))
			for key, raw := range row {
				coerced[key] = coerceJoinNumericString(raw)
			}
			value, err = evaluateExpressionWithRow(expr, coerced)
		}
		return value, err == nil
	}
}

func evalJoinExpressionWithSubqueries(expr sqlparser.Expr, row joinedMapRow, evaluator joinSubqueryEvaluator) (interface{}, bool) {
	resolved := sqlparser.String(expr)
	changed := false
	for index := 0; index < len(resolved); index++ {
		if resolved[index] != '(' {
			continue
		}
		close := matchingParenIndex(resolved, index)
		if close < 0 {
			return nil, false
		}
		inner := strings.TrimSpace(resolved[index+1 : close])
		if !strings.HasPrefix(strings.ToLower(inner), "select ") {
			continue
		}
		statement, err := sqlparser.Parse(inner)
		if err != nil {
			return nil, false
		}
		selectStmt, ok := statement.(*sqlparser.Select)
		if !ok {
			return nil, false
		}
		value, err := evaluator(&sqlparser.Subquery{Select: selectStmt}, row)
		if err != nil {
			return nil, false
		}
		literal := correlatedInterfaceSQLLiteral(value)
		resolved = resolved[:index] + literal + resolved[close+1:]
		index += len(literal) - 1
		changed = true
	}
	if !changed {
		return nil, false
	}
	statement, err := sqlparser.Parse("select " + resolved)
	if err != nil {
		return nil, false
	}
	selectStmt, ok := statement.(*sqlparser.Select)
	if !ok || len(selectStmt.SelectExprs) != 1 {
		return nil, false
	}
	aliased, ok := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, false
	}
	value, err := evaluateExpressionWithRow(aliased.Expr, map[string]interface{}(row))
	if err != nil {
		coerced := make(map[string]interface{}, len(row))
		for key, raw := range row {
			coerced[key] = coerceJoinNumericString(raw)
		}
		value, err = evaluateExpressionWithRow(aliased.Expr, coerced)
	}
	return value, err == nil
}

func coerceJoinNumericString(value interface{}) interface{} {
	text, ok := value.(string)
	if !ok {
		return value
	}
	text = strings.TrimSpace(text)
	if integer, err := strconv.ParseInt(text, 10, 64); err == nil {
		return integer
	}
	if decimal, err := strconv.ParseFloat(text, 64); err == nil {
		return decimal
	}
	return value
}

func joinColumnKey(col *sqlparser.ColName) string {
	if col == nil {
		return ""
	}
	if !col.Qualifier.IsEmpty() {
		return col.Qualifier.Name.String() + "." + col.Name.String()
	}
	return col.Name.String()
}

func (se *SelectExecutor) projectJoinedRows(rows []joinedMapRow) ([]Record, []string, []string) {
	columns := se.getColumnNames()
	columnTypes := make([]string, len(columns))
	projectedColumns := make([]*metadata.ColumnMeta, 0, len(columns))
	for i, col := range columns {
		columnTypes[i] = strings.ToLower(string(metadata.TypeVarchar))
		projectedColumns = append(projectedColumns, &metadata.ColumnMeta{Name: col, Type: metadata.TypeVarchar})
	}
	records := make([]Record, 0, len(rows))
	for _, row := range rows {
		values := make([]interface{}, 0, len(se.selectExprs))
		for index, expr := range se.selectExprs {
			values = append(values, se.valueForJoinProjection(row, index, expr))
		}
		records = append(records, NewExecutorRecordFromInterface(values, &metadata.TableMeta{
			Name:    se.tableName + "_join",
			Columns: projectedColumns,
		}))
	}
	return records, columns, columnTypes
}

func (se *SelectExecutor) valueForJoinProjection(row joinedMapRow, index int, expr string) interface{} {
	if index < len(se.selectExprASTs) && se.selectExprASTs[index] != nil {
		values := map[string]interface{}(row)
		value, err := evaluateExpressionWithRow(se.selectExprASTs[index], values)
		if err == nil {
			return value
		}
		coerced := make(map[string]interface{}, len(row))
		for key, raw := range row {
			coerced[key] = coerceJoinNumericString(raw)
		}
		if value, err = evaluateExpressionWithRow(se.selectExprASTs[index], coerced); err == nil {
			return value
		}
	}
	return rowValueForSelectExpr(row, expr)
}

func rowValueForSelectExpr(row joinedMapRow, expr string) interface{} {
	key := cleanJoinExpr(expr)
	if value, ok := row[key]; ok {
		return value
	}
	return row[strings.ToLower(key)]
}

func cleanJoinExpr(expr string) string {
	trimmed := strings.Trim(strings.TrimSpace(expr), "`")
	return strings.ReplaceAll(trimmed, "`", "")
}

func (se *SelectExecutor) sortJoinedRows(rows []joinedMapRow) {
	if len(se.orderByColumns) == 0 {
		return
	}
	sort.SliceStable(rows, func(i, j int) bool {
		for _, orderSpec := range se.orderByColumns {
			columnName, desc := parseOrderBySpec(orderSpec)
			left := rowValueForSelectExpr(rows[i], columnName)
			right := rowValueForSelectExpr(rows[j], columnName)
			cmp := se.compareJoinedOrderValues(columnName, left, right)
			if cmp == 0 {
				continue
			}
			if desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
}

func (se *SelectExecutor) compareJoinedOrderValues(columnName string, left, right interface{}) int {
	if left == nil {
		if right == nil {
			return 0
		}
		return -1
	}
	if right == nil {
		return 1
	}
	if se != nil && se.joinOrderTypes != nil {
		dataType, ok := se.joinOrderTypes[strings.ToLower(strings.TrimSpace(columnName))]
		if ok {
			dataType = metadata.DataType(strings.ToUpper(strings.TrimSpace(string(dataType))))
			switch dataType {
			case metadata.TypeTinyInt, metadata.TypeSmallInt, metadata.TypeMediumInt,
				metadata.TypeInt, metadata.TypeBigInt, metadata.TypeFloat, metadata.TypeDouble,
				metadata.TypeDecimal, metadata.TypeYear:
				leftNumber, leftOK := toFloat64(left)
				rightNumber, rightOK := toFloat64(right)
				if leftOK && rightOK {
					return compareFloatValues(leftNumber, rightNumber)
				}
			}
		}
	}
	return compareScalarValues(left, right)
}

func (se *SelectExecutor) buildJoinAggregateResult(rows []joinedMapRow) *SelectResult {
	groupColumns := se.cleanColumnNames(se.groupByColumns)
	aggregateExprs := make([]aggregateExpression, len(se.selectExprs))
	for i, expr := range se.selectExprs {
		aggregateExprs[i] = parseAggregateExpression(expr)
	}
	groups := make(map[string][]joinedMapRow)
	groupOrder := make([]string, 0)
	for _, row := range rows {
		values := make([]interface{}, 0, len(groupColumns))
		for _, column := range groupColumns {
			values = append(values, rowValueForSelectExpr(row, column))
		}
		key := aggregateGroupKey(values)
		if len(groupColumns) == 0 {
			key = "__all__"
		}
		if _, exists := groups[key]; !exists {
			groupOrder = append(groupOrder, key)
		}
		groups[key] = append(groups[key], row)
	}
	columns := se.getColumnNames()
	projectedColumns := make([]*metadata.ColumnMeta, 0, len(columns))
	columnTypes := make([]string, len(columns))
	for i, column := range columns {
		colType := metadata.TypeVarchar
		if aggregateExprs[i].funcName == "COUNT" {
			colType = metadata.TypeBigInt
		} else if aggregateExprs[i].funcName != "" {
			colType = metadata.TypeDecimal
		}
		projectedColumns = append(projectedColumns, &metadata.ColumnMeta{Name: column, Type: colType})
		columnTypes[i] = strings.ToLower(string(colType))
	}
	resultRows := make([]joinedMapRow, 0, len(groupOrder))
	for _, key := range groupOrder {
		groupRows := groups[key]
		out := make(joinedMapRow)
		for i, expr := range se.selectExprs {
			agg := aggregateExprs[i]
			var value interface{}
			if agg.funcName == "" {
				value = rowValueForSelectExpr(groupRows[0], expr)
			} else {
				value = aggregateJoinedRows(groupRows, agg)
			}
			out[columns[i]] = value
			out[strings.ToLower(columns[i])] = value
			out[expr] = value
			out[strings.ToLower(expr)] = value
		}
		if se.havingCondition != "" {
			matched, err := rowMatchesWhereConditions(map[string]interface{}(out), []string{se.havingCondition})
			if err != nil || !matched {
				continue
			}
		}
		resultRows = append(resultRows, out)
	}
	se.sortJoinedRows(resultRows)
	records := make([]Record, 0, len(resultRows))
	for _, row := range resultRows {
		values := make([]interface{}, 0, len(columns))
		for _, column := range columns {
			values = append(values, row[column])
		}
		records = append(records, NewExecutorRecordFromInterface(values, &metadata.TableMeta{
			Name:    se.tableName + "_join_aggregate",
			Columns: projectedColumns,
		}))
	}
	records = se.applyLimitOffset(records)
	return &SelectResult{
		Records:     records,
		RowCount:    len(records),
		Columns:     columns,
		ColumnTypes: columnTypes,
		ResultType:  common.RESULT_TYPE_QUERY,
		Message:     fmt.Sprintf("Query OK, %d rows in set", len(records)),
	}
}

func aggregateJoinedRows(rows []joinedMapRow, agg aggregateExpression) interface{} {
	acc := aggregateAccumulator{fn: agg.funcName, separator: agg.separator, orderBy: agg.orderBy, orderDesc: agg.orderDesc}
	if agg.distinct {
		acc.seen = make(map[string]struct{})
	}
	for _, row := range rows {
		if agg.funcName == "COUNT" && agg.column != "*" && rowValueForSelectExpr(row, agg.column) == nil {
			continue
		}
		var value interface{}
		if agg.column != "*" {
			value = rowValueForSelectExpr(row, agg.column)
		}
		orderValue := value
		if agg.orderBy != "" {
			orderValue = rowValueForSelectExpr(row, agg.orderBy)
		}
		acc.addWithOrder(value, orderValue)
	}
	return acc.value()
}

func basicValueToJoinInterface(value basic.Value) interface{} {
	if value == nil || value.IsNull() {
		return nil
	}
	return value.Raw()
}
