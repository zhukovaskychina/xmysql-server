package engine

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

type joinTableSource struct {
	tableName       string
	schemaName      string
	alias           string
	meta            *metadata.TableMeta
	columnSemantics map[string]metadata.ColumnSemanticDefinition
	tableSemantics  *metadata.TableSemanticDefinition
	rows            []map[string]interface{}
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
	// The first table in this compatibility join path is always read by a
	// clustered full scan. MySQL counts that first-table scan separately from
	// SELECT_FULL_JOIN, which describes the subsequent unindexed join lookup.
	if len(stmt.From) > 0 {
		se.selectScan = 1
	}
	se.distinct = strings.EqualFold(strings.TrimSpace(stmt.Distinct), strings.TrimSpace(sqlparser.DistinctStr))
	expressions, err := se.expandJoinSelectExpressions(stmt)
	if err != nil {
		return nil, err
	}
	if err := se.parseSelectExprs(expressions); err != nil {
		return nil, err
	}
	if stmt.Where != nil {
		se.whereConditions = se.parseWhereConditions(stmt.Where.Expr)
	}
	for _, expr := range stmt.GroupBy {
		se.groupByColumns = append(se.groupByColumns, sqlparser.String(expr))
		se.groupByExpressions = append(se.groupByExpressions, expr)
	}
	if stmt.Having != nil {
		se.havingCondition = sqlparser.String(stmt.Having.Expr)
		se.havingExpression = stmt.Having.Expr
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
		// This compatibility path materializes both sides and performs a
		// Cartesian product. MySQL exposes each such unindexed join as a
		// SELECT_FULL_JOIN event; range-join and range-check paths remain zero
		// until the executor has a real indexed join implementation.
		se.selectFullJoin++
	}
	if hasEffectiveWhereConditions(se.whereConditions) {
		filtered := make([]joinedMapRow, 0, len(rows))
		for _, row := range rows {
			var matched bool
			var err error
			if stmt.Where != nil && expressionHasSubquery(stmt.Where.Expr) {
				matched, err = se.evalJoinPredicateWithError(stmt.Where.Expr, row)
			} else {
				matched, err = rowMatchesWhereConditions(row, se.whereConditions)
			}
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
		result := se.buildJoinAggregateResult(rows)
		if se.expressionError != nil {
			return nil, se.expressionError
		}
		return result, nil
	}
	se.sortJoinedRows(rows)
	records, columns, columnTypes, err := se.projectJoinedRows(stmt, rows)
	if err != nil {
		return nil, err
	}
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
	// JOIN reads use a separate materialization path from ordinary SELECTs.
	// Apply the same transaction visibility fence before rows are qualified
	// and joined, otherwise a REPEATABLE READ session can observe later
	// committed rows through JOIN even though a single-table scan hides them.
	rowData = applyPendingClientTransactionVisibility(ctx, tableSchema, tableName.Name.String(), rowData, meta)
	se.rowsExamined += scanner.RowsExamined()
	source := &joinTableSource{
		tableName: tableName.Name.String(), schemaName: tableSchema, alias: alias, meta: meta,
		columnSemantics: se.columnSemanticDefinitions(meta),
		tableSemantics:  se.tableSemanticDefinition(meta, tableSchema),
	}
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
		joinedRow := qualifiedJoinRow(source, row.ColumnValues)
		joinedRow[storedFunctionContextKey] = ctxOrBackground(ctx)
		joinedRow[storedFunctionDataDirKey] = se.dataDir
		joinedRow[storedFunctionSchemaKey] = tableSchema
		if se.sessionValues != nil {
			joinedRow[sessionExpressionValuesKey] = se.sessionValues
		}
		if se.storedFunctionAuthorizer != nil {
			joinedRow[storedFunctionAuthorizerKey] = se.storedFunctionAuthorizer
		}
		source.rows = append(source.rows, joinedRow)
	}
	return source, nil
}

func qualifiedJoinRow(source *joinTableSource, values map[string]interface{}) map[string]interface{} {
	row := make(map[string]interface{}, len(values)*3)
	for _, col := range source.meta.Columns {
		if col == nil {
			continue
		}
		collation := strings.TrimSpace(col.Collation)
		if collation == "" {
			collation = strings.TrimSpace(source.meta.Collation)
		}
		if collation != "" {
			row[columnCollationContextKey(col.Name)] = collation
			row[columnCollationContextKey(source.alias+"."+col.Name)] = collation
			row[columnCollationContextKey(source.tableName+"."+col.Name)] = collation
		}
		value := values[col.Name]
		row[col.Name] = value
		row[strings.ToLower(col.Name)] = value
		row[source.alias+"."+col.Name] = value
		row[strings.ToLower(source.alias+"."+col.Name)] = value
		row[source.tableName+"."+col.Name] = value
		row[strings.ToLower(source.tableName+"."+col.Name)] = value
	}
	if primaryKey, exists, err := contentSourcePrimaryKey(values, source.meta); err == nil && exists {
		context := contentSourceRowContext{
			Schema: source.schemaName, Table: source.tableName, PrimaryKey: append([]byte(nil), primaryKey...),
			ColumnSemantics: source.columnSemantics,
			TableSemantics:  source.tableSemantics,
		}
		for _, qualifier := range []string{source.alias, source.tableName, source.schemaName + "." + source.tableName} {
			if strings.TrimSpace(qualifier) != "" {
				row[contentSourceRowContextKeyForAlias(qualifier)] = context
			}
		}
	}
	return row
}

func contentSourceRowContextKeyForAlias(alias string) string {
	return contentSourceRowContextKey + ":" + strings.ToLower(strings.Trim(strings.TrimSpace(alias), "` "))
}

func contentSourceRowContextKeyForColumn(alias, column string) string {
	return contentSourceRowContextKeyForAlias(alias) + ":" + strings.ToLower(strings.Trim(strings.TrimSpace(column), "` "))
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
	// loadJoinTableSource always performs a clustered full scan today. Keep
	// this counter tied to the actual join implementation rather than merely
	// matching the SQL text, so derived/unsupported paths do not fabricate P_S
	// counters.
	se.selectFullJoin++
	joined := make([]joinedMapRow, 0)
	rightMatched := make([]bool, len(right.rows))
	for _, left := range leftRows {
		matched := false
		for i, rightRow := range right.rows {
			merged := mergeJoinedRowsUsing(left, joinedMapRow(rightRow), condition.Using)
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
		if !matched && strings.Contains(strings.ToLower(joinType), "left") {
			joined = append(joined, mergeJoinedRowsUsing(left, joinedMapRow(nullJoinRow(right)), condition.Using))
		}
	}
	if strings.Contains(strings.ToLower(joinType), "right") {
		for i, rightRow := range right.rows {
			if rightMatched[i] {
				continue
			}
			// Bind the absent side explicitly, including when its input is
			// empty. Otherwise qualified references fall back to the other
			// table's unqualified column of the same name.
			leftNull := make(joinedMapRow, len(se.joinOrderTypes))
			for name := range se.joinOrderTypes {
				leftNull[name] = nil
			}
			joined = append(joined, mergeJoinedRowsUsing(leftNull, joinedMapRow(rightRow), condition.Using))
		}
	}
	return joined, nil
}

func joinUsingRowsMatch(left, right joinedMapRow, columns sqlparser.Columns) bool {
	for _, column := range columns {
		name := strings.Trim(column.String(), "` ")
		leftValue, leftOK := resolveExpressionRowValue(map[string]interface{}(left), name)
		rightValue, rightOK := resolveExpressionRowValue(map[string]interface{}(right), name)
		collation, _ := left[columnCollationContextKey(name)].(string)
		if collation == "" {
			collation, _ = right[columnCollationContextKey(name)].(string)
		}
		if !leftOK || !rightOK || leftValue == nil || rightValue == nil || compareScalarValuesWithCollation(leftValue, rightValue, collation) != 0 {
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

func mergeJoinedRowsUsing(left, right joinedMapRow, columns sqlparser.Columns) joinedMapRow {
	merged := mergeJoinedRows(left, right)
	for _, column := range columns {
		name := strings.Trim(column.String(), "` ")
		value, _ := resolveExpressionRowValue(map[string]interface{}(left), name)
		if value == nil {
			value, _ = resolveExpressionRowValue(map[string]interface{}(right), name)
		}
		merged[name] = value
		merged[strings.ToLower(name)] = value
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
	truth, err := se.evalJoinPredicateTruthWithError(expr, row)
	return truth == sqlTruthTrue, err
}

func (se *SelectExecutor) evalJoinPredicateTruthWithError(expr sqlparser.Expr, row joinedMapRow) (int, error) {
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
	truth := evalJoinPredicateTruthWithEvaluatorAndError(expr, row, evaluator, setEvaluator, &evaluationErr)
	if evaluationErr != nil {
		return sqlTruthFalse, evaluationErr
	}
	return truth, nil
}

func (se *SelectExecutor) evaluateGroupedProjectionWithSubqueries(expression sqlparser.Expr, row joinedMapRow) (interface{}, error) {
	if se == nil || expression == nil || !expressionHasSubquery(expression) {
		return nil, fmt.Errorf("grouped subquery projection is not applicable")
	}
	if booleanSQLExpression(expression) {
		truth, err := se.evalJoinPredicateTruthWithError(expression, row)
		if err != nil {
			return nil, err
		}
		switch truth {
		case sqlTruthUnknown:
			return nil, nil
		case sqlTruthTrue:
			return int64(1), nil
		default:
			return int64(0), nil
		}
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
	value, ok := evalJoinValueWithEvaluator(expression, row, evaluator)
	if evaluationErr != nil {
		return nil, evaluationErr
	}
	if !ok {
		return nil, fmt.Errorf("cannot evaluate grouped subquery projection %s", sqlparser.String(expression))
	}
	return value, nil
}

func (se *SelectExecutor) evalJoinPredicateTruth(expr sqlparser.Expr, row joinedMapRow) int {
	return evalJoinPredicateTruthWithEvaluator(expr, row, se.joinSubqueryEvaluator, se.joinSetSubqueryEvaluator)
}

type joinSubqueryEvaluator func(*sqlparser.Subquery, joinedMapRow) (interface{}, error)
type joinSetSubqueryEvaluator func(*sqlparser.Subquery, joinedMapRow) ([]interface{}, error)

func evalJoinPredicateTruthWithEvaluator(expr sqlparser.Expr, row joinedMapRow, evaluator joinSubqueryEvaluator, setEvaluator joinSetSubqueryEvaluator) int {
	return evalJoinPredicateTruthWithEvaluatorAndError(expr, row, evaluator, setEvaluator, nil)
}

func evalJoinPredicateTruthWithEvaluatorAndError(expr sqlparser.Expr, row joinedMapRow, evaluator joinSubqueryEvaluator, setEvaluator joinSetSubqueryEvaluator, evaluationErr *error) int {
	if expr == nil {
		return sqlTruthTrue
	}
	switch v := expr.(type) {
	case *sqlparser.AndExpr:
		left := evalJoinPredicateTruthWithEvaluatorAndError(v.Left, row, evaluator, setEvaluator, evaluationErr)
		if left == sqlTruthFalse {
			return sqlTruthFalse
		}
		right := evalJoinPredicateTruthWithEvaluatorAndError(v.Right, row, evaluator, setEvaluator, evaluationErr)
		if right == sqlTruthFalse {
			return sqlTruthFalse
		}
		if left == sqlTruthUnknown || right == sqlTruthUnknown {
			return sqlTruthUnknown
		}
		return sqlTruthTrue
	case *sqlparser.OrExpr:
		left := evalJoinPredicateTruthWithEvaluatorAndError(v.Left, row, evaluator, setEvaluator, evaluationErr)
		if left == sqlTruthTrue {
			return sqlTruthTrue
		}
		right := evalJoinPredicateTruthWithEvaluatorAndError(v.Right, row, evaluator, setEvaluator, evaluationErr)
		if right == sqlTruthTrue {
			return sqlTruthTrue
		}
		if left == sqlTruthUnknown || right == sqlTruthUnknown {
			return sqlTruthUnknown
		}
		return sqlTruthFalse
	case *sqlparser.NotExpr:
		truth := evalJoinPredicateTruthWithEvaluatorAndError(v.Expr, row, evaluator, setEvaluator, evaluationErr)
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
		collation, collationErr := resolveComparisonCollation(v.Left, v.Right, map[string]interface{}(row), v.Operator)
		if collationErr != nil {
			if evaluationErr != nil && *evaluationErr == nil {
				*evaluationErr = collationErr
			}
			return sqlTruthFalse
		}
		if v.Operator == sqlparser.InStr || v.Operator == sqlparser.NotInStr {
			matched, unknown, err := evalJoinInPredicate(left, v.Right, row, setEvaluator, collation)
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
		if (v.Operator == sqlparser.RegexpStr || v.Operator == sqlparser.NotRegexpStr) && left != nil && right != nil {
			matched, err := sqlRegexpMatchWithCollation(fmt.Sprint(right), fmt.Sprint(left), collation)
			if err != nil {
				if evaluationErr != nil && *evaluationErr == nil {
					*evaluationErr = err
				}
				return sqlTruthFalse
			}
			if v.Operator == sqlparser.NotRegexpStr {
				matched = !matched
			}
			return truthValue(matched)
		}
		if (v.Operator == sqlparser.LikeStr || v.Operator == sqlparser.NotLikeStr) && v.Escape != nil && left != nil && right != nil {
			matched, err := sqlLikePatternMatchWithEscapeAndCollation(fmt.Sprint(left), fmt.Sprint(right), v.Escape, map[string]interface{}(row), collation)
			if err != nil {
				if evaluationErr != nil && *evaluationErr == nil {
					*evaluationErr = err
				}
				return sqlTruthFalse
			}
			if v.Operator == sqlparser.NotLikeStr {
				matched = !matched
			}
			return truthValue(matched)
		}
		return evalJoinComparisonTruthWithEscapeAndCollation(v.Operator, left, right, v.Escape, map[string]interface{}(row), collation)
	case *sqlparser.RangeCond:
		left, leftOK := evalJoinValueWithEvaluator(v.Left, row, evaluator)
		from, fromOK := evalJoinValueWithEvaluator(v.From, row, evaluator)
		to, toOK := evalJoinValueWithEvaluator(v.To, row, evaluator)
		if !leftOK || !fromOK || !toOK {
			return sqlTruthFalse
		}
		lowerCollation, err := resolveComparisonCollation(v.Left, v.From, map[string]interface{}(row), "BETWEEN")
		if err != nil {
			if evaluationErr != nil && *evaluationErr == nil {
				*evaluationErr = err
			}
			return sqlTruthFalse
		}
		upperCollation, err := resolveComparisonCollation(v.Left, v.To, map[string]interface{}(row), "BETWEEN")
		if err != nil {
			if evaluationErr != nil && *evaluationErr == nil {
				*evaluationErr = err
			}
			return sqlTruthFalse
		}
		if left == nil || from == nil || to == nil {
			return sqlTruthUnknown
		}
		matched := compareScalarValuesWithCollation(left, from, lowerCollation) >= 0 && compareScalarValuesWithCollation(left, to, upperCollation) <= 0
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
		case sqlparser.IsTrueStr:
			return truthValue(sqlTruthValue(value) == sqlTruthTrue)
		case sqlparser.IsFalseStr:
			return truthValue(sqlTruthValue(value) == sqlTruthFalse)
		case sqlparser.IsNotTrueStr:
			return truthValue(sqlTruthValue(value) != sqlTruthTrue)
		case sqlparser.IsNotFalseStr:
			return truthValue(sqlTruthValue(value) != sqlTruthFalse)
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
		return evalJoinPredicateTruthWithEvaluatorAndError(v.Expr, row, evaluator, setEvaluator, evaluationErr)
	default:
		value, ok := evalJoinValueWithEvaluator(expr, row, evaluator)
		if !ok {
			return sqlTruthFalse
		}
		return sqlTruthValue(value)
	}
}

func evalJoinInPredicate(left interface{}, right sqlparser.Expr, row joinedMapRow, setEvaluator joinSetSubqueryEvaluator, collation string) (bool, bool, error) {
	if paren, ok := right.(*sqlparser.ParenExpr); ok {
		return evalJoinInPredicate(left, paren.Expr, row, setEvaluator, collation)
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
			if compareScalarValuesWithCollation(left, value, collation) == 0 {
				return true, false, nil
			}
		}
		return false, hasNull, nil
	}
	return evalInPredicateWithCollation(left, right, map[string]interface{}(row), collation)
}

func evalJoinComparison(operator string, left, right interface{}) bool {
	return evalJoinComparisonTruth(operator, left, right) == sqlTruthTrue
}

func evalJoinComparisonTruth(operator string, left, right interface{}) int {
	return evalJoinComparisonTruthWithCollation(operator, left, right, "")
}

func evalJoinComparisonTruthWithCollation(operator string, left, right interface{}, collation string) int {
	return evalJoinComparisonTruthWithEscapeAndCollation(operator, left, right, nil, nil, collation)
}

func evalJoinComparisonTruthWithEscapeAndCollation(operator string, left, right interface{}, escapeExpr sqlparser.Expr, values map[string]interface{}, collation string) int {
	if operator == sqlparser.NullSafeEqualStr {
		if left == nil || right == nil {
			return truthValue(left == nil && right == nil)
		}
	}
	if left == nil || right == nil {
		return sqlTruthUnknown
	}
	if operator == sqlparser.RegexpStr || operator == sqlparser.NotRegexpStr {
		matched, err := sqlRegexpMatchWithCollation(fmt.Sprint(right), fmt.Sprint(left), collation)
		if err != nil {
			return sqlTruthFalse
		}
		if operator == sqlparser.NotRegexpStr {
			return truthValue(!matched)
		}
		return truthValue(matched)
	}
	if operator == sqlparser.LikeStr || operator == sqlparser.NotLikeStr {
		matched, _ := sqlLikePatternMatchWithEscapeAndCollation(fmt.Sprint(left), fmt.Sprint(right), escapeExpr, values, collation)
		if operator == sqlparser.NotLikeStr {
			return truthValue(!matched)
		}
		return truthValue(matched)
	}
	cmp := compareScalarValuesWithCollation(left, right, collation)
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

func (se *SelectExecutor) projectJoinedRows(stmt *sqlparser.Select, rows []joinedMapRow) ([]Record, []string, []string, error) {
	columns := se.getColumnNames()
	columnTypes := make([]string, len(columns))
	projectedColumns := make([]*metadata.ColumnMeta, 0, len(columns))
	for i, col := range columns {
		typ := metadata.TypeVarchar
		collation := ""
		if i < len(se.selectExprASTs) {
			if booleanSQLExpression(se.selectExprASTs[i]) {
				typ = metadata.TypeBigInt
			} else if modelType, known := modelProjectionResultType(se.selectExprASTs[i]); known {
				typ = modelType
			} else if name, ok := se.selectExprASTs[i].(*sqlparser.ColName); ok {
				if len(rows) > 0 {
					collation, _ = rows[0][columnCollationContextKey(joinColumnKey(name))].(string)
				}
				if resolved, found := se.joinOrderTypes[strings.ToLower(joinColumnKey(name))]; found {
					typ = resolved
				}
			} else if len(rows) > 0 {
				value, err := se.valueForJoinProjection(rows[0], i, se.selectExprs[i])
				if err != nil {
					return nil, nil, nil, err
				}
				typ = projectionValueType(value)
			}
			typ = mysqlSortedJoinMaterializationType(stmt, se.selectExprASTs[i], typ)
		}
		columnTypes[i] = strings.ToLower(string(typ))
		projectedColumns = append(projectedColumns, &metadata.ColumnMeta{Name: col, Type: typ, Collation: collation})
	}
	records := make([]Record, 0, len(rows))
	for _, row := range rows {
		values := make([]interface{}, 0, len(se.selectExprs))
		for index, expr := range se.selectExprs {
			value, err := se.valueForJoinProjection(row, index, expr)
			if err != nil {
				return nil, nil, nil, err
			}
			values = append(values, value)
		}
		record := NewExecutorRecordFromInterface(values, &metadata.TableMeta{
			Name:    se.tableName + "_join",
			Columns: projectedColumns,
		})
		if typedRecord, ok := record.(*EngineExecutorRecord); ok {
			for index, expression := range se.selectExprASTs {
				column, isColumn := expression.(*sqlparser.ColName)
				if !isColumn {
					continue
				}
				if rowContext, exists := contentSourceContextForColumn(map[string]interface{}(row), column); exists {
					if typedRecord.contentSourceByColumn == nil {
						typedRecord.contentSourceByColumn = make(map[int]contentSourceRowContext)
					}
					typedRecord.contentSourceByColumn[index] = rowContext
				}
			}
			for key, value := range row {
				if rowContext, exists := value.(contentSourceRowContext); exists && strings.HasPrefix(key, contentSourceRowContextKey) {
					if typedRecord.contentSourceContexts == nil {
						typedRecord.contentSourceContexts = make(map[string]contentSourceRowContext)
					}
					typedRecord.contentSourceContexts[key] = rowContext
				}
			}
		}
		records = append(records, record)
	}
	return records, columns, columnTypes, nil
}

func (se *SelectExecutor) valueForJoinProjection(row joinedMapRow, index int, expr string) (interface{}, error) {
	if index < len(se.selectExprASTs) && se.selectExprASTs[index] != nil {
		if function, ok := se.selectExprASTs[index].(*sqlparser.FuncExpr); ok {
			name := strings.ToUpper(strings.Trim(function.Name.String(), "` "))
			switch name {
			case "AI_EMBED":
				return evaluateAIEmbedSQL(se.embeddingRuntime, se.modelContext, function, map[string]interface{}(row))
			case "AI_EMBED_BLOCK":
				return se.evaluateAIEmbedBlockSQL(function, map[string]interface{}(row))
			case "AI_DESCRIBE_BLOCK", "AI_DESCRIBE_BLOCK_RESULT":
				return se.evaluateAIDescribeBlockSQL(function, map[string]interface{}(row))
			case "AI_GENERATE", "AI_GENERATE_RESULT", "AI_SUMMARIZE":
				return se.evaluateAIGenerateSQL(function, map[string]interface{}(row))
			}
			if strings.HasPrefix(name, "CONTENT_") || strings.HasPrefix(name, "VECTOR_") || strings.HasPrefix(name, "AI_") {
				// Source read/validation errors must not become a successful NULL.
				return evaluateExpressionWithRow(function, map[string]interface{}(row))
			}
		}
		if booleanSQLExpression(se.selectExprASTs[index]) && expressionHasSubquery(se.selectExprASTs[index]) {
			var evaluationErr error
			evaluator := joinSubqueryEvaluator(nil)
			if se.joinSubqueryEvaluator != nil {
				evaluator = func(subquery *sqlparser.Subquery, candidate joinedMapRow) (interface{}, error) {
					value, err := se.joinSubqueryEvaluator(subquery, candidate)
					if err != nil && evaluationErr == nil {
						evaluationErr = err
					}
					return value, err
				}
			}
			setEvaluator := joinSetSubqueryEvaluator(nil)
			if se.joinSetSubqueryEvaluator != nil {
				setEvaluator = func(subquery *sqlparser.Subquery, candidate joinedMapRow) ([]interface{}, error) {
					values, err := se.joinSetSubqueryEvaluator(subquery, candidate)
					if err != nil && evaluationErr == nil {
						evaluationErr = err
					}
					return values, err
				}
			}
			truth := evalJoinPredicateTruthWithEvaluator(se.selectExprASTs[index], row, evaluator, setEvaluator)
			if evaluationErr != nil {
				return nil, evaluationErr
			}
			switch truth {
			case sqlTruthUnknown:
				return nil, nil
			case sqlTruthTrue:
				return int64(1), nil
			default:
				return int64(0), nil
			}
		}
		if expressionHasSubquery(se.selectExprASTs[index]) {
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
			value, ok := evalJoinValueWithEvaluator(se.selectExprASTs[index], row, evaluator)
			if evaluationErr != nil {
				return nil, evaluationErr
			}
			if !ok {
				return nil, fmt.Errorf("cannot evaluate JOIN subquery projection %s", se.selectExprASTs[index])
			}
			return value, nil
		}
		values := map[string]interface{}(row)
		value, err := evaluateExpressionWithRow(se.selectExprASTs[index], values)
		if err == nil {
			return value, nil
		}
		coerced := make(map[string]interface{}, len(row))
		for key, raw := range row {
			coerced[key] = coerceJoinNumericString(raw)
		}
		if value, err = evaluateExpressionWithRow(se.selectExprASTs[index], coerced); err == nil {
			return value, nil
		}
	}
	return rowValueForSelectExpr(row, expr), nil
}

func expressionHasSubquery(expression sqlparser.Expr) bool {
	found := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if _, ok := node.(*sqlparser.Subquery); ok {
			found = true
			return false, nil
		}
		return !found, nil
	}, expression)
	return found
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
			if ordinal, err := strconv.Atoi(columnName); err == nil && ordinal > 0 && ordinal <= len(se.selectExprs) {
				columnName = se.selectExprs[ordinal-1]
			} else {
				for index, alias := range se.selectAliases {
					if strings.EqualFold(alias, columnName) {
						columnName = se.selectExprs[index]
						break
					}
				}
			}
			left, _ := evaluateProjectionExpression(columnName, map[string]interface{}(rows[i]))
			right, _ := evaluateProjectionExpression(columnName, map[string]interface{}(rows[j]))
			collation, _ := rows[i][columnCollationContextKey(columnName)].(string)
			cmp := se.compareJoinedOrderValues(columnName, left, right, collation)
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

func (se *SelectExecutor) compareJoinedOrderValues(columnName string, left, right interface{}, collation string) int {
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
	return compareScalarValuesWithCollation(left, right, collation)
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
		collations := make([]string, 0, len(groupColumns))
		for _, column := range groupColumns {
			values = append(values, rowValueForSelectExpr(row, column))
			collation, _ := row[columnCollationContextKey(column)].(string)
			collations = append(collations, collation)
		}
		key := aggregateGroupKeyWithCollations(values, collations)
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
		if i < len(se.selectExprASTs) && booleanSQLExpression(se.selectExprASTs[i]) {
			colType = metadata.TypeBigInt
		} else if aggregateExprs[i].funcName == "COUNT" {
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
		out[storedFunctionContextKey] = ctxOrBackground(se.modelContext)
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
		if len(groupRows) > 0 {
			addJoinedAggregateGroupBindings(out, se.groupByExpressions, groupRows[0])
			for index, expression := range se.selectExprASTs {
				column, isColumn := expression.(*sqlparser.ColName)
				if !isColumn || index >= len(columns) {
					continue
				}
				if rowContext, exists := contentSourceContextForColumn(map[string]interface{}(groupRows[0]), column); exists {
					out[derivedOutputSourceContextKey(columns[index])] = rowContext
				}
			}
			for key, value := range groupRows[0] {
				if rowContext, exists := value.(contentSourceRowContext); exists && strings.HasPrefix(key, contentSourceRowContextKey) {
					out[key] = rowContext
				}
			}
		}
		var groupRowsAsValues []map[string]interface{}
		for _, expression := range se.selectExprASTs {
			if expressionHasSubquery(expression) && aggregateExpressionContainsAggregate(expression) {
				groupRowsAsValues = make([]map[string]interface{}, len(groupRows))
				for rowIndex, groupRow := range groupRows {
					groupRowsAsValues[rowIndex] = map[string]interface{}(groupRow)
				}
				break
			}
		}
		for index, expression := range se.selectExprASTs {
			if index >= len(columns) || !expressionHasSubquery(expression) {
				continue
			}
			value, err := se.evaluateGroupedProjectionWithAggregateSubqueries(expression, out, groupRowsAsValues)
			if err != nil {
				if se.expressionError == nil {
					se.expressionError = err
				}
				continue
			}
			out[columns[index]] = value
			out[strings.ToLower(columns[index])] = value
			out[se.selectExprs[index]] = value
			out[strings.ToLower(se.selectExprs[index])] = value
		}
		if se.havingCondition != "" {
			matched, err := se.matchesHavingRow(out)
			if err != nil || !matched {
				if err != nil && se.expressionError == nil {
					se.expressionError = err
				}
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
		record := NewExecutorRecordFromInterface(values, &metadata.TableMeta{
			Name:    se.tableName + "_join_aggregate",
			Columns: projectedColumns,
		})
		if typedRecord, ok := record.(*EngineExecutorRecord); ok {
			for index, column := range columns {
				if rowContext, exists := row[derivedOutputSourceContextKey(column)].(contentSourceRowContext); exists {
					if typedRecord.contentSourceByColumn == nil {
						typedRecord.contentSourceByColumn = make(map[int]contentSourceRowContext)
					}
					typedRecord.contentSourceByColumn[index] = rowContext
				}
			}
			for key, value := range row {
				if rowContext, exists := value.(contentSourceRowContext); exists && strings.HasPrefix(key, contentSourceRowContextKey) {
					if typedRecord.contentSourceContexts == nil {
						typedRecord.contentSourceContexts = make(map[string]contentSourceRowContext)
					}
					typedRecord.contentSourceContexts[key] = rowContext
				}
			}
		}
		records = append(records, record)
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

func addJoinedAggregateGroupBindings(target joinedMapRow, expressions []sqlparser.Expr, source joinedMapRow) {
	if target == nil || source == nil {
		return
	}
	for _, expression := range expressions {
		column, ok := expression.(*sqlparser.ColName)
		if !ok {
			continue
		}
		value := rowValueForSelectExpr(source, sqlparser.String(column))
		name := strings.Trim(column.Name.String(), "` ")
		qualifier := strings.Trim(strings.TrimSpace(sqlparser.String(column.Qualifier)), "` ")
		if qualifier != "" {
			key := qualifier + "." + name
			target[key] = value
			target[strings.ToLower(key)] = value
		} else {
			target[name] = value
			target[strings.ToLower(name)] = value
		}
	}
}

func aggregateJoinedRows(rows []joinedMapRow, agg aggregateExpression) interface{} {
	acc := aggregateAccumulator{
		fn:        agg.funcName,
		countStar: agg.column == "*",
		separator: agg.separator,
		orderBy:   agg.orderBy,
		orderDesc: agg.orderDesc,
	}
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
			if collation := joinedRowColumnCollation(row, agg.column); collation != "" {
				acc.valueCollation = collation
			}
		}
		orderValue := value
		if agg.orderBy != "" {
			orderValue = rowValueForSelectExpr(row, agg.orderBy)
			if collation := joinedRowColumnCollation(row, agg.orderBy); collation != "" {
				acc.orderCollation = collation
			}
		}
		acc.addWithOrder(value, orderValue)
	}
	return acc.value()
}

func joinedRowColumnCollation(row joinedMapRow, column string) string {
	return collationFromValueContext(map[string]interface{}(row), column)
}

func basicValueToJoinInterface(value basic.Value) interface{} {
	if value == nil || value.IsNull() {
		return nil
	}
	return value.Raw()
}
