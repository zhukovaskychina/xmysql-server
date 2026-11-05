package engine

import (
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// executeCorrelatedSubqueryCompatibility evaluates a correlated EXISTS query
// with an isolated inner execution for every outer row. This prevents the
// common but incorrect optimization of materializing the inner query once.
func (e *XMySQLExecutor) executeCorrelatedSubqueryCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	if strings.HasPrefix(strings.ToLower(trimmed), "with ") {
		return false, nil
	}
	existsAt, notExists, existsFound := findExistsCall(trimmed)
	if !existsFound {
		return false, nil
	}
	outerPrefix := strings.TrimSpace(trimmed[:existsAt])
	outerWhere := ""
	outerWhereConnector := ""
	trailingWhere := ""
	trailingWhereConnector := ""
	if whereStart, whereEnd, whereFound := findSpacedKeyword(outerPrefix, "where"); whereFound {
		outerWhere = strings.TrimSpace(outerPrefix[whereEnd:])
		outerPrefix = strings.TrimSpace(outerPrefix[:whereStart])
		// The compatibility path removes EXISTS from the outer WHERE and
		// evaluates it separately for each outer row. Keep the connector so
		// an OR expression can be evaluated against the same outer row instead
		// of silently becoming an AND filter.
		outerWhere, outerWhereConnector = splitCorrelatedConditionConnector(outerWhere, false)
	}
	outerFromParts := splitTopLevelKeyword(outerPrefix, "from")
	if len(outerFromParts) != 2 || !strings.HasPrefix(strings.ToLower(strings.TrimSpace(outerFromParts[0])), "select ") {
		return false, nil
	}
	outerProjection := strings.TrimSpace(strings.TrimSpace(outerFromParts[0])[len("select "):])
	outerSource := strings.TrimSpace(outerFromParts[1])
	outerTableName, outerAliasName, sourceOK := parseLeadingTableRef(outerSource, false, false)
	if !sourceOK {
		return false, nil
	}
	open := strings.Index(trimmed[existsAt:], "(")
	if open < 0 {
		return true, fmt.Errorf("EXISTS subquery is missing parentheses")
	}
	open += existsAt
	if scalarSubqueryAppearsInJoinOn(trimmed, open) {
		// EXISTS in JOIN ... ON must be evaluated against each candidate
		// joined row. The outer-WHERE compatibility rewriter cannot preserve
		// that scope, so leave the statement for the structured JOIN path.
		return false, nil
	}
	close := matchingParenIndex(trimmed, open)
	if close < 0 {
		return true, fmt.Errorf("unterminated correlated subquery")
	}
	tail := strings.TrimSpace(trimmed[close+1:])
	outerOrder := ""
	if tail != "" {
		if orderParts := splitTopLevelKeyword(tail, "order by"); len(orderParts) == 2 {
			if strings.TrimSpace(orderParts[0]) != "" {
				trailingWhere, trailingWhereConnector = splitCorrelatedConditionConnector(strings.TrimSpace(orderParts[0]), true)
				if trailingWhere == "" || trailingWhereConnector == "" {
					return false, nil
				}
			}
			outerOrder = strings.TrimSpace(orderParts[1])
		} else {
			trailingWhere, trailingWhereConnector = splitCorrelatedConditionConnector(tail, true)
			if trailingWhere == "" || trailingWhereConnector == "" {
				return false, nil
			}
		}
	}
	innerSQL := strings.TrimSpace(trimmed[open+1 : close])
	if !strings.HasPrefix(strings.ToLower(innerSQL), "select ") {
		return true, fmt.Errorf("correlated EXISTS requires an inner SELECT")
	}
	outerAlias := outerAliasName
	if strings.EqualFold(outerAlias, "join") || strings.EqualFold(outerAlias, "left") || strings.EqualFold(outerAlias, "right") || strings.EqualFold(outerAlias, "inner") {
		outerAlias = ""
	}
	if outerAlias == "" {
		outerAlias = outerTableName
	}
	if outerAlias == "" {
		outerAlias = strings.Trim(strings.TrimSpace(outerTableName), "`")
	}
	if !sqlHasQualifierDot(innerSQL, outerAlias) {
		return false, nil
	}
	outerDependencies := strings.TrimSpace(strings.Join([]string{outerWhere, trailingWhere}, " "))
	outerSQL := correlatedOuterSourceSQL(outerProjection, outerDependencies, innerSQL, outerSource)
	if outerWhere != "" && outerWhereConnector != "OR" {
		outerSQL += " where " + outerWhere
	}
	if outerOrder != "" {
		outerSQL += " order by " + outerOrder
	}
	outerStmt, err := sqlparser.Parse(outerSQL)
	if err != nil {
		return true, err
	}
	outerSelect, ok := outerStmt.(*sqlparser.Select)
	if !ok {
		return true, fmt.Errorf("outer correlated query is not SELECT")
	}
	outerResult, err := e.executeSelectStatement(ctx, outerSelect, databaseName)
	if err != nil {
		return true, err
	}
	var outerWhereExpr sqlparser.Expr
	if outerWhere != "" && outerWhereConnector == "OR" {
		whereStmt, parseErr := sqlparser.Parse("select 1 from correlated_source where " + outerWhere)
		if parseErr != nil {
			return true, parseErr
		}
		whereSelect, ok := whereStmt.(*sqlparser.Select)
		if !ok || whereSelect.Where == nil {
			return true, fmt.Errorf("outer correlated WHERE is not a predicate")
		}
		outerWhereExpr = whereSelect.Where.Expr
	}
	var trailingWhereExpr sqlparser.Expr
	if trailingWhere != "" {
		whereStmt, parseErr := sqlparser.Parse("select 1 from correlated_source where " + trailingWhere)
		if parseErr != nil {
			return true, parseErr
		}
		whereSelect, ok := whereStmt.(*sqlparser.Select)
		if !ok || whereSelect.Where == nil {
			return true, fmt.Errorf("trailing correlated WHERE is not a predicate")
		}
		trailingWhereExpr = whereSelect.Where.Expr
	}
	selectedColumns := splitTopLevelComma(outerProjection)
	if len(selectedColumns) == 1 && strings.TrimSpace(selectedColumns[0]) == "*" {
		selectedColumns = append([]string(nil), outerResult.Columns...)
	}
	selectedIndexes := make([]int, 0, len(selectedColumns))
	for _, column := range selectedColumns {
		clean := strings.Trim(strings.TrimSpace(column), "`")
		if dot := strings.LastIndex(clean, "."); dot >= 0 {
			clean = clean[dot+1:]
		}
		index := -1
		for i, resultColumn := range outerResult.Columns {
			if strings.EqualFold(strings.Trim(resultColumn, "`"), clean) {
				index = i
				break
			}
		}
		if index < 0 {
			return true, fmt.Errorf("outer correlated column %s does not exist", clean)
		}
		selectedIndexes = append(selectedIndexes, index)
	}

	selectedValues := make([][]basic.Value, 0, len(outerResult.Records))
	for _, outerRecord := range outerResult.Records {
		rowValues := correlatedOuterRowValues(outerRecord, outerResult.Columns, outerResult.ColumnTypes, outerAlias, outerAlias)
		trailingMatches := false
		if trailingWhereExpr != nil {
			trailingMatches, err = evalPredicate(trailingWhereExpr, rowValues)
			if err != nil {
				return true, err
			}
		}
		outerPredicateMatches := false
		if outerWhereExpr != nil {
			outerPredicateMatches, err = evalPredicate(outerWhereExpr, rowValues)
			if err != nil {
				return true, err
			}
		}
		if outerPredicateMatches {
			if trailingWhereExpr != nil && trailingWhereConnector == "AND" && !trailingMatches {
				continue
			}
			values := make([]basic.Value, 0, len(selectedIndexes))
			for _, index := range selectedIndexes {
				values = append(values, outerRecord.GetValues()[index])
			}
			selectedValues = append(selectedValues, values)
			continue
		}
		boundInnerSQL := innerSQL
		for index, outerColumn := range outerResult.Columns {
			if index >= len(outerRecord.GetValues()) {
				continue
			}
			literal := correlatedValueSQLLiteral(outerRecord.GetValues()[index])
			boundInnerSQL = replaceQualifiedName(boundInnerSQL, outerAlias, strings.Trim(outerColumn, "`"), literal)
		}
		innerStmt, parseErr := sqlparser.Parse(boundInnerSQL)
		if parseErr != nil {
			return true, parseErr
		}
		innerSelect, ok := innerStmt.(*sqlparser.Select)
		if !ok {
			return true, fmt.Errorf("inner correlated query is not SELECT")
		}
		innerResult, execErr := e.executeSelectStatement(ctx, innerSelect, databaseName)
		if execErr != nil {
			return true, execErr
		}
		matched := (innerResult.RowCount > 0 && !notExists) || (innerResult.RowCount == 0 && notExists)
		if trailingWhereExpr != nil {
			if trailingWhereConnector == "AND" {
				matched = matched && trailingMatches
			} else {
				matched = matched || trailingMatches
			}
		}
		if matched {
			values := make([]basic.Value, 0, len(selectedIndexes))
			for _, index := range selectedIndexes {
				values = append(values, outerRecord.GetValues()[index])
			}
			selectedValues = append(selectedValues, values)
		}
	}

	meta := &metadata.TableMeta{Name: "correlated", Columns: make([]*metadata.ColumnMeta, 0, len(selectedColumns))}
	for i, column := range selectedColumns {
		columnName := strings.TrimSpace(column)
		if dot := strings.LastIndex(columnName, "."); dot >= 0 {
			columnName = columnName[dot+1:]
		}
		columnType := metadata.TypeVarchar
		if i < len(outerResult.ColumnTypes) {
			columnType = metadata.DataType(strings.ToUpper(outerResult.ColumnTypes[selectedIndexes[i]]))
		}
		meta.Columns = append(meta.Columns, &metadata.ColumnMeta{Name: strings.Trim(columnName, "`"), Type: columnType})
	}
	records := make([]Record, 0, len(selectedValues))
	for _, values := range selectedValues {
		records = append(records, NewExecutorRecord(values, meta))
	}
	columns := make([]string, len(meta.Columns))
	for i, column := range meta.Columns {
		columns[i] = column.Name
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: &SelectResult{Records: records, RowCount: len(records), Columns: columns, ColumnTypes: outerResult.ColumnTypes, ResultType: common.RESULT_TYPE_QUERY}, Message: fmt.Sprintf("correlated subquery returned %d rows", len(records))}
	return true, nil
}

func correlatedValueSQLLiteral(value basic.Value) string {
	if value.IsNull() {
		return "NULL"
	}
	switch value.Type() {
	case basic.ValueTypeTinyInt, basic.ValueTypeSmallInt, basic.ValueTypeMediumInt, basic.ValueTypeInt, basic.ValueTypeBigInt:
		return fmt.Sprintf("%d", value.Int())
	case basic.ValueTypeFloat, basic.ValueTypeDouble, basic.ValueTypeDecimal:
		return fmt.Sprintf("%v", value.Float64())
	default:
		return "'" + strings.ReplaceAll(value.String(), "'", "''") + "'"
	}
}

func correlatedOuterSourceSQL(projection, outerWhere, innerSQL, source string) string {
	aliases := correlatedOuterSourceAliases(source)
	known := make(map[string]struct{}, len(aliases))
	for _, alias := range aliases {
		known[strings.ToLower(alias)] = struct{}{}
	}
	items := make([]string, 0, 8)
	seen := make(map[string]struct{})
	for _, item := range splitTopLevelComma(projection) {
		item = strings.TrimSpace(item)
		if item == "" || item == "*" || strings.HasSuffix(item, ".*") {
			continue
		}
		key := strings.ToLower(item)
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			items = append(items, item)
		}
	}
	qualified := findQualifiedPairs(projection + " " + outerWhere + " " + innerSQL)
	for _, match := range qualified {
		if _, exists := known[strings.ToLower(match[0])]; !exists {
			continue
		}
		item := match[0] + "." + match[1]
		key := strings.ToLower(item)
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			items = append(items, item)
		}
	}
	if len(items) == 0 {
		items = append(items, "*")
	}
	return "select " + strings.Join(items, ", ") + " from " + source
}

func correlatedOuterSourceAliases(source string) []string {
	aliases := make([]string, 0, 4)
	add := func(alias string) {
		alias = strings.TrimSpace(alias)
		switch strings.ToLower(alias) {
		case "", "as", "join", "left", "right", "inner", "outer", "cross", "on":
			return
		}
		for _, existing := range aliases {
			if strings.EqualFold(existing, alias) {
				return
			}
		}
		aliases = append(aliases, alias)
	}
	stmt, err := sqlparser.Parse("select * from " + source)
	if err != nil {
		return aliases
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return aliases
	}
	var collect func(sqlparser.TableExpr)
	collect = func(expr sqlparser.TableExpr) {
		switch node := expr.(type) {
		case *sqlparser.AliasedTableExpr:
			if !node.As.IsEmpty() {
				add(node.As.String())
			} else if table, tableOK := node.Expr.(sqlparser.TableName); tableOK {
				add(table.Name.String())
			}
		case *sqlparser.JoinTableExpr:
			collect(node.LeftExpr)
			collect(node.RightExpr)
		case *sqlparser.ParenTableExpr:
			for _, nested := range node.Exprs {
				collect(nested)
			}
		}
	}
	for _, expr := range selectStmt.From {
		collect(expr)
	}
	return aliases
}
