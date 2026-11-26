package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

type derivedJoinSource struct {
	rows          []derivedRow
	visitRows     func(func(derivedRow) error) error
	columns       []string
	qualifiedCols map[string][]string
}

var errDerivedRowLimitReached = errors.New("derived row limit reached")

func visitDerivedSourceRows(source *derivedJoinSource, visit func(derivedRow) error) error {
	if source == nil || visit == nil {
		return fmt.Errorf("derived row source and visitor are required")
	}
	if source.visitRows != nil {
		return source.visitRows(visit)
	}
	for _, row := range source.rows {
		if err := visit(row); err != nil {
			return err
		}
	}
	return nil
}

func materializeDerivedSourceRows(source *derivedJoinSource) error {
	if source == nil || source.visitRows == nil {
		return nil
	}
	rows := make([]derivedRow, 0)
	err := source.visitRows(func(row derivedRow) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		return err
	}
	source.rows = rows
	source.visitRows = nil
	return nil
}

func derivedStreamingLimitBounds(stmt *sqlparser.Select) (int, int, bool) {
	if stmt == nil || stmt.Limit == nil || stmt.Where != nil || len(stmt.OrderBy) != 0 ||
		stmt.Distinct != "" || len(stmt.GroupBy) != 0 || stmt.Having != nil ||
		stmt.Lock != "" || containsAggregateSelectExpr(stmt.SelectExprs) {
		return 0, 0, false
	}
	parseLiteral := func(expression sqlparser.Expr) (int, bool) {
		value, ok := expression.(*sqlparser.SQLVal)
		if !ok || value.Type != sqlparser.IntVal {
			return 0, false
		}
		parsed, err := strconv.ParseUint(string(value.Val), 10, 64)
		maxInt := uint64(^uint(0) >> 1)
		if err != nil || parsed > maxInt {
			return 0, false
		}
		return int(parsed), true
	}
	count, ok := parseLiteral(stmt.Limit.Rowcount)
	if !ok {
		return 0, 0, false
	}
	offset := 0
	if stmt.Limit.Offset != nil {
		offset, ok = parseLiteral(stmt.Limit.Offset)
		if !ok {
			return 0, 0, false
		}
	}
	return offset, count, true
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
		switch table.Expr.(type) {
		case *sqlparser.Subquery, *sqlparser.TableFunction:
			return true
		default:
			return false
		}
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
	streamedLimitApplied := false
	var source *derivedJoinSource
	for _, expression := range stmt.From {
		if table, ok := contentDOCXBlocksFunctionInExpr(expression); ok {
			if source == nil {
				return nil, fmt.Errorf("CONTENT_PARSE_DOCX requires a preceding row source")
			}
			joined, err := lateralContentDOCXBlocks(executionContextBase(ctx), source, table, e)
			if err != nil {
				return nil, err
			}
			source = joined
			continue
		}
		if table, ok := contentPDFPagesFunctionInExpr(expression); ok {
			if source == nil {
				return nil, fmt.Errorf("CONTENT_PARSE_PDF requires a preceding row source")
			}
			joined, err := lateralContentPDFPages(executionContextBase(ctx), source, table, e)
			if err != nil {
				return nil, err
			}
			source = joined
			continue
		}
		if table, ok := contentTextChunksFunctionInExpr(expression); ok {
			if source == nil {
				return nil, fmt.Errorf("CONTENT_TEXT_CHUNKS requires a preceding row source")
			}
			joined, err := lateralContentTextChunks(executionContextBase(ctx), source, table, e.getDataDir())
			if err != nil {
				return nil, err
			}
			source = joined
			continue
		}
		if table, ok := fullTextTokensFunctionInExpr(expression); ok {
			joined, err := lateralFullTextTokens(source, table, e.getDataDir())
			if err != nil {
				return nil, err
			}
			source = joined
			continue
		}
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
	if source.visitRows != nil {
		offset, limitRows, canStreamLimit := derivedStreamingLimitBounds(stmt)
		if canStreamLimit {
			capacity := limitRows
			if capacity > 1024 {
				capacity = 1024
			}
			rows := make([]derivedRow, 0, capacity)
			skipped := 0
			if limitRows > 0 {
				err := visitDerivedSourceRows(source, func(row derivedRow) error {
					if skipped < offset {
						skipped++
						return nil
					}
					rows = append(rows, row)
					if len(rows) >= limitRows {
						return errDerivedRowLimitReached
					}
					return nil
				})
				if err != nil && !errors.Is(err, errDerivedRowLimitReached) {
					return nil, err
				}
			}
			source.rows = rows
			source.visitRows = nil
			streamedLimitApplied = true
		} else if err := materializeDerivedSourceRows(source); err != nil {
			return nil, err
		}
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
	// Prefer SQL/catalog types over the Go representation of the first row.
	// This keeps INT columns as INT and gives empty native function results
	// the same shape as populated results without evaluating a synthetic row.
	ctes := make(map[string][]joinOutputColumn, len(materialized))
	for name, result := range materialized {
		if result == nil {
			continue
		}
		for index, column := range result.Columns {
			ctes[strings.ToLower(name)] = append(ctes[strings.ToLower(name)], joinOutputColumn{name: column, typ: metadata.DataType(derivedColumnType(result, index))})
		}
	}
	describer := preparedSelectDescriber{executor: e, ctx: ctx, database: databaseName}
	if describer.ctx == nil {
		describer.ctx = &ExecutionContext{}
	}
	declaredColumns, describeErr := describer.describe(stmt, ctes, 0)
	if len(filtered) == 0 && describeErr == nil {
		return emptySelectResultFromShape(declaredColumns), nil
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
					qualifier := sqlparser.TableName{}
					if !star.TableName.IsEmpty() {
						qualifier = star.TableName
					}
					if sourceContext, found := contentSourceContextForColumn(row.values, &sqlparser.ColName{Name: sqlparser.NewColIdent(column), Qualifier: qualifier}); found {
						outputMap[derivedOutputSourceContextKey(column)] = sourceContext
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
			var value interface{}
			var err error
			if function, ok := aliased.Expr.(*sqlparser.FuncExpr); ok && strings.EqualFold(strings.Trim(function.Name.String(), "` "), "AI_EMBED_BLOCK") {
				var modelContext context.Context
				if ctx != nil {
					modelContext = ctx.Context
				}
				modelExecutor := &SelectExecutor{
					dataDir: e.getDataDir(), embeddingRuntime: e.embeddingRuntimeSnapshot(),
					generationRuntime: e.generationRuntimeSnapshot(), modelContext: modelContext,
				}
				value, err = modelExecutor.evaluateAIEmbedBlockSQL(function, row.values)
			} else if function, ok := aliased.Expr.(*sqlparser.FuncExpr); ok &&
				(strings.EqualFold(strings.Trim(function.Name.String(), "` "), "AI_GENERATE") || strings.EqualFold(strings.Trim(function.Name.String(), "` "), "AI_GENERATE_RESULT") || strings.EqualFold(strings.Trim(function.Name.String(), "` "), "AI_SUMMARIZE") || strings.EqualFold(strings.Trim(function.Name.String(), "` "), "AI_DESCRIBE_BLOCK") || strings.EqualFold(strings.Trim(function.Name.String(), "` "), "AI_DESCRIBE_BLOCK_RESULT")) {
				var modelContext context.Context
				if ctx != nil {
					modelContext = ctx.Context
				}
				modelExecutor := &SelectExecutor{generationRuntime: e.generationRuntimeSnapshot(), modelContext: modelContext}
				if strings.EqualFold(strings.Trim(function.Name.String(), "` "), "AI_DESCRIBE_BLOCK") || strings.EqualFold(strings.Trim(function.Name.String(), "` "), "AI_DESCRIBE_BLOCK_RESULT") {
					value, err = modelExecutor.evaluateAIDescribeBlockSQL(function, row.values)
				} else {
					value, err = modelExecutor.evaluateAIGenerateSQL(function, row.values)
				}
			} else {
				value, err = evaluateExpressionWithRow(aliased.Expr, row.values)
			}
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
			if collation := collationForExpression(aliased.Expr, row.values); collation != "" {
				outputMap[columnCollationContextKey(name)] = collation
			}
			if column, ok := aliased.Expr.(*sqlparser.ColName); ok {
				if sourceContext, found := contentSourceContextForColumn(row.values, column); found {
					outputMap[derivedOutputSourceContextKey(name)] = sourceContext
				}
			}
			if len(projected) == 0 {
				columns = append(columns, name)
				metaColumns = append(metaColumns, &metadata.ColumnMeta{Name: name, Type: projectionValueType(value), Collation: collationForExpression(aliased.Expr, row.values)})
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
				collation := collationForExpression(order.Expr, projected[left].values)
				if collation == "" {
					if literal, ok := order.Expr.(*sqlparser.SQLVal); ok && literal.Type == sqlparser.IntVal {
						if index, err := strconv.Atoi(string(literal.Val)); err == nil && index > 0 && index <= len(columns) {
							collation = collationFromValueContext(projected[left].values, columns[index-1])
						}
					}
				}
				cmp := compareScalarValuesWithCollation(leftValue, rightValue, collation)
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
			collations := make([]string, len(columns))
			for index, column := range columns {
				collations[index] = collationFromValueContext(row.values, column)
			}
			key := aggregateGroupKeyWithCollations(row.order, collations)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			unique = append(unique, row)
		}
		projected = unique
	}
	start, end := 0, len(projected)
	if !streamedLimitApplied {
		start, end = derivedLimitBounds(stmt.Limit, len(projected))
	}
	projected = projected[start:end]
	records := make([]Record, 0, len(projected))
	if describeErr == nil && len(declaredColumns) == len(metaColumns) {
		for index, column := range declaredColumns {
			// Preserve the evaluated row's collation and source context.
			metaColumns[index].Type = column.typ
		}
	}
	tableMeta := &metadata.TableMeta{Name: "derived_join", Columns: metaColumns}
	for _, row := range projected {
		record := NewExecutorRecordFromInterface(derivedOutputValues(row.order), tableMeta)
		if typedRecord, ok := record.(*EngineExecutorRecord); ok {
			for index, column := range columns {
				if rowContext, exists := row.values[derivedOutputSourceContextKey(column)].(contentSourceRowContext); exists {
					if typedRecord.contentSourceByColumn == nil {
						typedRecord.contentSourceByColumn = make(map[int]contentSourceRowContext)
					}
					typedRecord.contentSourceByColumn[index] = rowContext
				}
			}
			for key, value := range row.values {
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
	columnTypes := make([]string, 0, len(metaColumns))
	for _, column := range metaColumns {
		columnTypes = append(columnTypes, strings.ToLower(string(column.Type)))
	}
	return &SelectResult{Records: records, RowCount: len(records), Columns: columns, ColumnTypes: columnTypes, ResultType: common.RESULT_TYPE_QUERY}, nil
}

func derivedOutputSourceContextKey(column string) string {
	return contentSourceRowContextKeyForColumn("__xmysql_derived_output__", column)
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
		if functionTable, ok := contentTextChunksFunctionInExpr(table.RightExpr); ok {
			if !supportsCrossTableFunctionJoin(table) {
				return nil, fmt.Errorf("CONTENT_TEXT_CHUNKS supports CROSS JOIN or comma join only")
			}
			return lateralContentTextChunks(executionContextBase(ctx), left, functionTable, e.getDataDir())
		}
		if functionTable, ok := contentPDFPagesFunctionInExpr(table.RightExpr); ok {
			if !supportsCrossTableFunctionJoin(table) {
				return nil, fmt.Errorf("CONTENT_PARSE_PDF supports CROSS JOIN or comma join only")
			}
			return lateralContentPDFPages(executionContextBase(ctx), left, functionTable, e)
		}
		if functionTable, ok := contentDOCXBlocksFunctionInExpr(table.RightExpr); ok {
			if !supportsCrossTableFunctionJoin(table) {
				return nil, fmt.Errorf("CONTENT_PARSE_DOCX supports CROSS JOIN or comma join only")
			}
			return lateralContentDOCXBlocks(executionContextBase(ctx), left, functionTable, e)
		}
		if functionTable, ok := fullTextTokensFunctionInExpr(table.RightExpr); ok {
			if !supportsCrossTableFunctionJoin(table) {
				return nil, fmt.Errorf("FULLTEXT_TOKENS supports CROSS JOIN or comma join only")
			}
			return lateralFullTextTokens(left, functionTable, e.getDataDir())
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

// MySQL grammar and the local parser normalize CROSS JOIN to the generic join
// token. Treat an unqualified inner join as a cross join only when it has no
// ON/USING clause; lateral table functions remain unavailable to filtered or
// outer joins.
func supportsCrossTableFunctionJoin(join *sqlparser.JoinTableExpr) bool {
	return join != nil && strings.EqualFold(strings.TrimSpace(join.Join), sqlparser.JoinStr) &&
		join.Condition.On == nil && len(join.Condition.Using) == 0
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
	case *sqlparser.TableFunction:
		if expression.Function == nil {
			return nil, fmt.Errorf("derived table function is nil")
		}
		if alias == "" {
			alias = strings.ToLower(expression.Function.Name.String())
		}
		if strings.EqualFold(expression.Function.Name.String(), "VECTOR_SEARCH") {
			source, err := e.evaluateVectorSearchTable(vectorSearchExecutionContext(ctx), expression.Function)
			if err != nil {
				return nil, err
			}
			source.qualifiedCols[strings.ToLower(alias)] = append([]string(nil), source.columns...)
			return source, nil
		}
		if strings.EqualFold(expression.Function.Name.String(), "FULLTEXT_TOKENS") {
			source, err := evaluateFullTextTokensTable(expression.Function, map[string]interface{}{storedFunctionDataDirKey: e.getDataDir()})
			if err != nil {
				return nil, err
			}
			source.qualifiedCols[strings.ToLower(alias)] = append([]string(nil), source.columns...)
			return source, nil
		}
		return nil, fmt.Errorf("unsupported table-valued function %s", expression.Function.Name.String())
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
			value := interface{}(derivedBasicValueInterface(record.GetValues()[index]))
			if typedRecord, ok := record.(*EngineExecutorRecord); ok {
				if contentValue, exists := typedRecord.ContentValueByIndex(index); exists {
					value = contentValue
				} else if vectorValue, exists := typedRecord.VectorValueByIndex(index); exists {
					value = vectorValue
				}
			}
			value = derivedNormalizeValue(value, derivedColumnType(result, index))
			clean := strings.Trim(column, "`")
			row[clean] = value
			row[strings.ToLower(clean)] = value
			row[alias+"."+clean] = value
			row[strings.ToLower(alias+"."+clean)] = value
			if typedRecord, ok := record.(*EngineExecutorRecord); ok {
				if tableMeta := typedRecord.GetTableMeta(); tableMeta != nil {
					collation := ""
					if index < len(tableMeta.Columns) && tableMeta.Columns[index] != nil {
						collation = strings.TrimSpace(tableMeta.Columns[index].Collation)
					}
					if collation == "" {
						collation = strings.TrimSpace(tableMeta.Collation)
					}
					if collation != "" {
						row[columnCollationContextKey(clean)] = collation
						row[columnCollationContextKey(alias+"."+clean)] = collation
					}
				}
				if rowContext, exists := typedRecord.ContentSourceContextByIndex(index); exists {
					row[contentSourceRowContextKeyForColumn(alias, clean)] = rowContext
				}
			}
		}
		if typedRecord, ok := record.(*EngineExecutorRecord); ok {
			for key, rowContext := range typedRecord.ContentSourceContexts() {
				row[key] = rowContext
			}
		}
		source.rows = append(source.rows, derivedRow{values: row})
	}
	return source
}

func joinDerivedSources(left, right *derivedJoinSource, join string, condition sqlparser.JoinCondition, subqueryEvaluator joinSubqueryEvaluator, setSubqueryEvaluator joinSetSubqueryEvaluator) (*derivedJoinSource, error) {
	if left == nil || right == nil {
		return nil, fmt.Errorf("derived join source is nil")
	}
	if err := materializeDerivedSourceRows(left); err != nil {
		return nil, err
	}
	if err := materializeDerivedSourceRows(right); err != nil {
		return nil, err
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
		collation := collationFromValueContext(left.values, name)
		if collation == "" {
			collation = collationFromValueContext(right.values, name)
		}
		if !leftOK || !rightOK || compareScalarValuesWithCollation(leftValue, rightValue, collation) != 0 {
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
