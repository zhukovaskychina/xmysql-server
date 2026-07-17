package engine

import (
	"context"
	"fmt"
	"sort"
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
		return se.joinRows(leftRows, rightSource, v.Join, v.Condition.On), nil
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
	alias := expr.As.String()
	if strings.TrimSpace(alias) == "" {
		alias = tableName.Name.String()
	}
	meta, err := se.loadTableMetaFromFrm(se.dataDir, se.schemaName, tableName.Name.String())
	if err != nil {
		return nil, err
	}
	btreeManager := se.btreeManager
	if se.storageManager != nil && se.storageManager.GetTableStorageManager() != nil {
		if tableBTreeManager, err := se.storageManager.GetTableStorageManager().CreateBTreeManagerForTable(ctx, se.schemaName, tableName.Name.String()); err == nil && tableBTreeManager != nil {
			btreeManager = tableBTreeManager
		}
	}
	if btreeManager == nil && se.tableManager != nil {
		if tableBTreeManager, err := se.tableManager.GetTableBTreeManager(ctx, se.schemaName, tableName.Name.String()); err == nil && tableBTreeManager != nil {
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

func (se *SelectExecutor) joinRows(leftRows []joinedMapRow, right *joinTableSource, joinType string, on sqlparser.Expr) []joinedMapRow {
	joined := make([]joinedMapRow, 0)
	rightMatched := make([]bool, len(right.rows))
	for _, left := range leftRows {
		matched := false
		for i, rightRow := range right.rows {
			merged := mergeJoinedRows(left, joinedMapRow(rightRow))
			if evalJoinPredicate(on, merged) {
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
	return joined
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
	if expr == nil {
		return true
	}
	switch v := expr.(type) {
	case *sqlparser.AndExpr:
		return evalJoinPredicate(v.Left, row) && evalJoinPredicate(v.Right, row)
	case *sqlparser.ComparisonExpr:
		left, leftOK := evalJoinValue(v.Left, row)
		right, rightOK := evalJoinValue(v.Right, row)
		if !leftOK || !rightOK {
			return false
		}
		cmp := compareScalarValues(left, right)
		switch v.Operator {
		case sqlparser.EqualStr:
			return cmp == 0
		case sqlparser.NotEqualStr:
			return cmp != 0
		default:
			return false
		}
	case *sqlparser.ParenExpr:
		return evalJoinPredicate(v.Expr, row)
	default:
		return false
	}
}

func evalJoinValue(expr sqlparser.Expr, row joinedMapRow) (interface{}, bool) {
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
	default:
		return nil, false
	}
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
		for _, expr := range se.selectExprs {
			values = append(values, rowValueForSelectExpr(row, expr))
		}
		records = append(records, NewExecutorRecordFromInterface(values, &metadata.TableMeta{
			Name:    se.tableName + "_join",
			Columns: projectedColumns,
		}))
	}
	return records, columns, columnTypes
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
			cmp := compareScalarValues(left, right)
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
	acc := aggregateAccumulator{fn: agg.funcName}
	for _, row := range rows {
		if agg.funcName == "COUNT" && agg.column != "*" && rowValueForSelectExpr(row, agg.column) == nil {
			continue
		}
		var value interface{}
		if agg.column != "*" {
			value = rowValueForSelectExpr(row, agg.column)
		}
		acc.add(value)
	}
	return acc.value()
}

func basicValueToJoinInterface(value basic.Value) interface{} {
	if value == nil || value.IsNull() {
		return nil
	}
	return value.Raw()
}
