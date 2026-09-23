package engine

import (
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

// validateCTEQuerySyntax validates the structured parser scope before the
// legacy execution rewrite runs. This keeps compatibility execution available
// while ensuring duplicate names, duplicate columns, forward references and
// malformed recursive definitions fail at the SQL boundary.
func validateCTEQuerySyntax(query string) error {
	trimmed := strings.TrimSpace(query)
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "with ") && !strings.HasPrefix(lower, "with\n") && !strings.HasPrefix(lower, "with\t") {
		return nil
	}
	stmt, err := sqlparser.Parse(trimmed)
	if err != nil {
		if isRawSetCTECompatibilityQuery(trimmed) {
			return nil
		}
		return err
	}
	withStmt, ok := stmt.(*sqlparser.With)
	if !ok {
		return fmt.Errorf("WITH statement did not produce structured AST")
	}
	definitions := make([]*CTEDefinition, 0, len(withStmt.CTEs))
	for _, cte := range withStmt.CTEs {
		if cte == nil {
			return fmt.Errorf("CTE definition is nil")
		}
		definitions = append(definitions, &CTEDefinition{
			Name:    cte.Name.String(),
			Columns: cteColumnsToStrings(cte.Columns),
			Query:   cte.Query,
			// WITH RECURSIVE is a statement-level modifier. A later CTE
			// may still be an ordinary definition that consumes an earlier
			// recursive CTE, so only a self-reference makes this definition
			// recursive for validation purposes.
			Recursive: withStmt.Recursive && cteQueryReferencesName(cte.Query, cte.Name.String()),
		})
	}
	ctx, err := BuildCTEContext(definitions)
	if err != nil {
		return err
	}
	return ResolveCTEReferences(withStmt.Body, ctx)
}

func cteQueryReferencesName(query sqlparser.Statement, name string) bool {
	for _, reference := range collectCTETableRefs(query) {
		if strings.EqualFold(strings.Trim(reference, "`"), strings.Trim(name, "`")) {
			return true
		}
	}
	return false
}

func isRawSetCTECompatibilityQuery(query string) bool {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "with ") || strings.HasPrefix(lower, "with recursive ") {
		return false
	}
	position := len("with")
	for position < len(trimmed) && strings.ContainsRune(" \t\r\n", rune(trimmed[position])) {
		position++
	}
	header := regexp.MustCompile(`(?is)^([a-zA-Z0-9_$]+)(?:\s*\([^)]*\))?\s+as\s*\(`)
	match := header.FindStringSubmatchIndex(trimmed[position:])
	if len(match) == 0 {
		return false
	}
	open := position + match[1] - 1
	close := matchingParenIndex(trimmed, open)
	if close < 0 {
		return false
	}
	definition := strings.TrimSpace(trimmed[open+1 : close])
	_, operators, ok := splitSetOperationQuery(definition)
	if !ok || !hasNonUnionSetOperator(operators) {
		return false
	}
	main := strings.TrimSpace(trimmed[close+1:])
	if strings.HasPrefix(main, ",") {
		return false
	}
	return strings.HasPrefix(strings.ToLower(main), "select ")
}

func cteColumnsToStrings(columns sqlparser.Columns) []string {
	result := make([]string, 0, len(columns))
	for _, column := range columns {
		result = append(result, column.String())
	}
	return result
}

// executeCTECompatibility handles the query forms that need a materialized
// CTE scope rather than a textual table-name substitution. It deliberately
// keeps the scope per statement, so a CTE can never leak into a later query.
func (e *XMySQLExecutor) executeCTECompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "with recursive ") {
		return false, nil
	}

	definitions, mainQuery, err := parseCTECompatibilityQuery(trimmed)
	if err != nil {
		return true, err
	}
	for index := range definitions {
		definitions[index].sessionValues = newCTESessionExpressionValues(ctx, databaseName)
	}
	if len(definitions) > 1 {
		return e.executeMultipleRecursiveCTECompatibility(ctx, databaseName, mainQuery, definitions)
	}
	if len(definitions) != 1 {
		return true, fmt.Errorf("recursive CTE compatibility currently requires one definition")
	}
	definition := definitions[0]
	parts, distinct, ok := splitRecursiveCTEQuery(definition.query)
	if !ok {
		return true, fmt.Errorf("recursive CTE must use UNION ALL or UNION DISTINCT")
	}
	if result, handled, err := e.executeRecursiveCTEBaseTableJoin(ctx, databaseName, mainQuery, definition, parts, distinct); handled {
		if err != nil {
			return true, err
		}
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: fmt.Sprintf("CTE query executed successfully, %d rows returned", result.RowCount)}
		return true, nil
	}
	if result, handled, err := e.executeRecursiveCTEDualAnchorJoin(ctx, databaseName, mainQuery, definition, parts, distinct); handled {
		if err != nil {
			return true, err
		}
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: fmt.Sprintf("CTE query executed successfully, %d rows returned", result.RowCount)}
		return true, nil
	}
	if len(definition.columns) > 1 {
		rows, err := materializeRecursiveCTEMultiColumnValues(definition, parts, distinct)
		if err != nil {
			return true, err
		}
		if joinedResult, handled, joinErr := e.executeRecursiveCTEMultiColumnMainJoinWithBaseTable(ctx, mainQuery, definition, rows, databaseName); handled {
			if joinErr != nil {
				return true, joinErr
			}
			ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: joinedResult, Message: fmt.Sprintf("CTE query executed successfully, %d rows returned", joinedResult.RowCount)}
			return true, nil
		}
		result, err := executeRecursiveCTEMultiColumnMainQuery(mainQuery, definition, rows)
		if err != nil {
			return true, err
		}
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: fmt.Sprintf("CTE query executed successfully, %d rows returned", result.RowCount)}
		return true, nil
	}
	if values, handled, err := materializeRecursiveCTESingleColumnGeneric(definition, parts, distinct); handled {
		if err != nil {
			return true, err
		}
		return true, e.finishRecursiveCTECompatibility(ctx, mainQuery, definition, values, databaseName)
	}

	anchorMatch := regexp.MustCompile(`(?is)^select\s+(-?\d+)\s*(?:as\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `)?\s*$`).FindStringSubmatch(strings.TrimSpace(parts[0]))
	recursiveMatch := regexp.MustCompile(`(?is)^select\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `\s*\+\s*(-?\d+)\s*(?:as\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `)?\s+from\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `\s+where\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `\s*(<|<=|=|>|>=)\s*(-?\d+)\s*$`).FindStringSubmatch(strings.TrimSpace(parts[1]))
	if len(anchorMatch) == 0 || len(recursiveMatch) == 0 {
		return true, fmt.Errorf("unsupported recursive CTE shape")
	}
	if !strings.EqualFold(recursiveMatch[4], definition.name) || !strings.EqualFold(recursiveMatch[1], definition.column) || !strings.EqualFold(recursiveMatch[5], definition.column) {
		return true, fmt.Errorf("recursive CTE column binding does not match definition")
	}
	start, err := strconv.ParseInt(anchorMatch[1], 10, 64)
	if err != nil {
		return true, err
	}
	step, err := strconv.ParseInt(recursiveMatch[2], 10, 64)
	if err != nil || (step == 0 && !distinct) {
		return true, fmt.Errorf("recursive CTE step must be a non-zero integer")
	}
	limit, err := strconv.ParseInt(recursiveMatch[7], 10, 64)
	if err != nil {
		return true, err
	}
	values := []interface{}{start}
	seen := map[int64]struct{}{start: {}}
	for iteration := 0; iteration < 1000; iteration++ {
		current := values[len(values)-1].(int64)
		if !recursiveCondition(current, recursiveMatch[6], limit) {
			break
		}
		next := current + step
		if _, exists := seen[next]; exists {
			if distinct {
				break
			}
			return true, fmt.Errorf("cycle detected in recursive CTE %s", definition.name)
		}
		seen[next] = struct{}{}
		values = append(values, next)
	}
	if len(values) >= 1000 {
		return true, fmt.Errorf("recursive CTE %s exceeded maximum recursion depth 1000", definition.name)
	}

	return true, e.finishRecursiveCTECompatibility(ctx, mainQuery, definition, values, databaseName)
}

func newCTESessionExpressionValues(ctx *ExecutionContext, databaseName string) map[string]interface{} {
	var sessionValues map[string]interface{}
	if ctx != nil {
		sessionValues = newSessionExpressionValues(ctx.Session)
	} else {
		sessionValues = newSessionExpressionValues(nil)
	}
	if sessionValues["database"] == nil && strings.TrimSpace(databaseName) != "" {
		sessionValues["database"] = databaseName
	}
	return sessionValues
}

func recursiveCTEExpressionValues(definition cteCompatibilityDefinition) map[string]interface{} {
	return map[string]interface{}{sessionExpressionValuesKey: definition.sessionValues}
}

// executeMultipleRecursiveCTECompatibility supports independent recursive
// definitions and the common two-CTE inner-join main query. It deliberately
// keeps the join shape bounded: arbitrary recursive multi-source members and
// correlated recursive execution remain outside this compatibility path.
func (e *XMySQLExecutor) executeMultipleRecursiveCTECompatibility(ctx *ExecutionContext, databaseName, mainQuery string, definitions []cteCompatibilityDefinition) (bool, error) {
	statement, err := sqlparser.Parse(strings.TrimSpace(mainQuery))
	if err != nil {
		return true, err
	}
	selectStmt, ok := statement.(*sqlparser.Select)
	if !ok || len(selectStmt.From) != 1 {
		return true, fmt.Errorf("multiple recursive CTE compatibility currently requires a single SELECT source or inner join")
	}

	type materializedDefinition struct {
		definition cteCompatibilityDefinition
		rows       [][]interface{}
	}
	materialized := make(map[string]materializedDefinition, len(definitions))
	for index := range definitions {
		definition := definitions[index]
		parts, distinct, splitOK := splitRecursiveCTEQuery(definition.query)
		var rows [][]interface{}
		if !splitOK {
			// WITH RECURSIVE also permits ordinary CTE definitions after a
			// recursive definition. Evaluate those definitions against the
			// already materialized CTE scope instead of requiring every item
			// to contain its own UNION ALL.
			statement, parseErr := parseSelectSQL(strings.TrimSpace(definition.query))
			if parseErr != nil {
				return true, fmt.Errorf("parse dependent CTE %s: %w", definition.name, parseErr)
			}
			if len(materialized) == 0 {
				return true, fmt.Errorf("dependent CTE %s has no materialized source", definition.name)
			}
			available := make(map[string]*SelectResult, len(materialized))
			for name, selected := range materialized {
				columnTypes := make([]string, len(selected.definition.columns))
				for columnIndex := range columnTypes {
					columnTypes[columnIndex] = "varchar"
					for _, row := range selected.rows {
						if columnIndex >= len(row) || row[columnIndex] == nil {
							continue
						}
						switch row[columnIndex].(type) {
						case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
							columnTypes[columnIndex] = "bigint"
						}
						break
					}
				}
				available[name] = newCompatibilityTypedResult(name, selected.definition.columns, columnTypes, selected.rows)
			}
			selected, execErr := e.executeDerivedJoinSelectWithMaterialized(ctx, statement, databaseName, available)
			if execErr != nil {
				return true, fmt.Errorf("execute dependent CTE %s: %w", definition.name, execErr)
			}
			if len(definition.columns) != len(selected.Columns) {
				return true, fmt.Errorf("dependent CTE %s returned %d columns, definition declares %d", definition.name, len(selected.Columns), len(definition.columns))
			}
			for _, record := range selected.Records {
				values := make([]interface{}, 0, len(record.GetValues()))
				for _, value := range record.GetValues() {
					values = append(values, derivedBasicValueInterface(value))
				}
				rows = append(rows, values)
			}
		} else if len(definition.columns) == 1 {
			values, handled, materializeErr := materializeRecursiveCTESingleColumnGeneric(definition, parts, distinct)
			if materializeErr != nil {
				return true, materializeErr
			}
			if !handled {
				values, materializeErr = materializeRecursiveCTESingleColumnLegacy(definition, parts, distinct)
				if materializeErr != nil {
					return true, materializeErr
				}
			}
			rows = make([][]interface{}, 0, len(values))
			for _, value := range values {
				rows = append(rows, []interface{}{value})
			}
		} else {
			rows, err = materializeRecursiveCTEMultiColumnValues(definition, parts, distinct)
			if err != nil {
				return true, err
			}
		}
		materialized[strings.ToLower(definition.name)] = materializedDefinition{definition: definition, rows: rows}
	}

	// Once every source in a joined FROM tree is a materialized recursive CTE,
	// reuse the general derived-join executor. This covers three or more
	// independent CTEs and arbitrarily nested inner joins while keeping base
	// table joins on their existing compatibility path.
	if recursiveCTEContainsJoin(selectStmt.From[0]) {
		aliases := make(map[string]string)
		unsupportedSource := false
		var collectRecursiveSources func(sqlparser.TableExpr)
		collectRecursiveSources = func(expression sqlparser.TableExpr) {
			switch table := expression.(type) {
			case *sqlparser.AliasedTableExpr:
				tableName, ok := table.Expr.(sqlparser.TableName)
				if !ok {
					unsupportedSource = true
					return
				}
				name := strings.ToLower(tableName.Name.String())
				if _, exists := materialized[name]; exists {
					alias := strings.TrimSpace(table.As.String())
					if alias == "" {
						alias = tableName.Name.String()
					}
					aliases[strings.ToLower(alias)] = name
				}
			case *sqlparser.JoinTableExpr:
				collectRecursiveSources(table.LeftExpr)
				collectRecursiveSources(table.RightExpr)
			case *sqlparser.ParenTableExpr:
				for _, nested := range table.Exprs {
					collectRecursiveSources(nested)
				}
			default:
				unsupportedSource = true
			}
		}
		collectRecursiveSources(selectStmt.From[0])
		if !unsupportedSource && len(aliases) > 0 {
			materializedSources := make(map[string]*SelectResult, len(aliases))
			for alias, name := range aliases {
				selected := materialized[name]
				columnTypes := make([]string, len(selected.definition.columns))
				for index := range columnTypes {
					columnTypes[index] = "varchar"
					for _, row := range selected.rows {
						if index >= len(row) || row[index] == nil {
							continue
						}
						switch row[index].(type) {
						case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
							columnTypes[index] = "bigint"
						}
						break
					}
				}
				materializedSources[alias] = newCompatibilityTypedResult("cte", selected.definition.columns, columnTypes, selected.rows)
			}
			result, err := e.executeDerivedJoinSelectWithMaterialized(ctx, selectStmt, databaseName, materializedSources)
			if err != nil {
				return true, err
			}
			ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: fmt.Sprintf("CTE query executed successfully, %d rows returned", result.RowCount)}
			return true, nil
		}
	}

	if from, single := selectStmt.From[0].(*sqlparser.AliasedTableExpr); single {
		tableName, tableOK := from.Expr.(sqlparser.TableName)
		if !tableOK {
			return true, fmt.Errorf("multiple recursive CTE main query must read one CTE")
		}
		selected, exists := materialized[strings.ToLower(tableName.Name.String())]
		if !exists {
			return true, fmt.Errorf("multiple recursive CTE main query must read one declared CTE")
		}
		var result *SelectResult
		if len(selected.definition.columns) == 1 {
			result, err = executeRecursiveCTEMainQuery(mainQuery, selected.definition, flattenRecursiveCTERows(selected.rows))
		} else {
			result, err = executeRecursiveCTEMultiColumnMainQuery(mainQuery, selected.definition, selected.rows)
		}
		if err != nil {
			return true, err
		}
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: fmt.Sprintf("CTE query executed successfully, %d rows returned", result.RowCount)}
		return true, nil
	}

	join, joinOK := selectStmt.From[0].(*sqlparser.JoinTableExpr)
	if !joinOK || !strings.EqualFold(join.Join, sqlparser.JoinStr) || join.Condition.On == nil {
		return true, fmt.Errorf("multiple recursive CTE main query supports only inner JOIN ... ON")
	}
	leftName, leftAlias, leftOK := recursiveCTETableExprInfo(join.LeftExpr)
	rightName, rightAlias, rightOK := recursiveCTETableExprInfo(join.RightExpr)
	if !leftOK || !rightOK {
		return true, fmt.Errorf("multiple recursive CTE join source is unsupported")
	}
	left, leftExists := materialized[strings.ToLower(leftName)]
	right, rightExists := materialized[strings.ToLower(rightName)]
	if !leftExists || !rightExists {
		return true, fmt.Errorf("multiple recursive CTE join must read declared CTEs")
	}
	result, err := executeRecursiveCTEJoinedMainQuery(selectStmt, left.definition, leftAlias, left.rows, right.definition, rightAlias, right.rows)
	if err != nil {
		return true, err
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: fmt.Sprintf("CTE query executed successfully, %d rows returned", result.RowCount)}
	return true, nil
}

func materializeRecursiveCTESingleColumnLegacy(definition cteCompatibilityDefinition, parts []string, distinct bool) ([]interface{}, error) {
	anchorMatch := regexp.MustCompile(`(?is)^select\s+(-?\d+)\s*(?:as\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `)?\s*$`).FindStringSubmatch(strings.TrimSpace(parts[0]))
	recursiveMatch := regexp.MustCompile(`(?is)^select\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `\s*\+\s*(-?\d+)\s*(?:as\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `)?\s+from\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `\s+where\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `\s*(<|<=|=|>|>=)\s*(-?\d+)\s*$`).FindStringSubmatch(strings.TrimSpace(parts[1]))
	if len(anchorMatch) == 0 || len(recursiveMatch) == 0 {
		return nil, fmt.Errorf("unsupported recursive CTE shape")
	}
	if !strings.EqualFold(recursiveMatch[4], definition.name) || !strings.EqualFold(recursiveMatch[1], definition.column) || !strings.EqualFold(recursiveMatch[5], definition.column) {
		return nil, fmt.Errorf("recursive CTE column binding does not match definition")
	}
	start, err := strconv.ParseInt(anchorMatch[1], 10, 64)
	if err != nil {
		return nil, err
	}
	step, err := strconv.ParseInt(recursiveMatch[2], 10, 64)
	if err != nil || (step == 0 && !distinct) {
		return nil, fmt.Errorf("recursive CTE step must be a non-zero integer")
	}
	limit, err := strconv.ParseInt(recursiveMatch[7], 10, 64)
	if err != nil {
		return nil, err
	}
	values := []interface{}{start}
	seen := map[int64]struct{}{start: {}}
	for iteration := 0; iteration < 1000; iteration++ {
		current := values[len(values)-1].(int64)
		if !recursiveCondition(current, recursiveMatch[6], limit) {
			break
		}
		next := current + step
		if _, exists := seen[next]; exists {
			if distinct {
				break
			}
			return nil, fmt.Errorf("cycle detected in recursive CTE %s", definition.name)
		}
		seen[next] = struct{}{}
		values = append(values, next)
	}
	if len(values) >= 1000 {
		return nil, fmt.Errorf("recursive CTE %s exceeded maximum recursion depth 1000", definition.name)
	}
	return values, nil
}

// executeRecursiveCTEBaseTableJoin materializes the common hierarchy shape:
// an anchor SELECT from a real table followed by a recursive member joining
// the CTE to that same (or another) base table. It deliberately uses the
// normal SELECT executor for both base-table scans, so storage, visibility,
// type conversion and predicates remain shared with ordinary queries.
func (e *XMySQLExecutor) executeRecursiveCTEBaseTableJoin(ctx *ExecutionContext, databaseName, mainQuery string, definition cteCompatibilityDefinition, parts []string, distinct bool) (*SelectResult, bool, error) {
	anchor, err := parseSelectSQL(strings.TrimSpace(parts[0]))
	if err != nil || len(anchor.From) != 1 {
		return nil, false, nil
	}
	anchorTable, _, ok := recursiveCTETableExprInfo(anchor.From[0])
	if !ok || strings.EqualFold(anchorTable, definition.name) || strings.EqualFold(anchorTable, "dual") {
		return nil, false, nil
	}
	recursive, err := parseSelectSQL(strings.TrimSpace(parts[1]))
	if err != nil || len(recursive.From) != 1 || len(anchor.SelectExprs) != len(definition.columns) || len(recursive.SelectExprs) != len(definition.columns) {
		return nil, false, nil
	}
	join, ok := recursive.From[0].(*sqlparser.JoinTableExpr)
	if !ok || join.Condition.On == nil {
		return nil, false, nil
	}
	leftTable, leftAlias, leftOK := recursiveCTETableExprInfo(join.LeftExpr)
	rightTable, rightAlias, rightOK := recursiveCTETableExprInfo(join.RightExpr)
	if !leftOK || !rightOK || !strings.EqualFold(leftTable, definition.name) || strings.EqualFold(rightTable, definition.name) {
		return nil, false, nil
	}
	if leftAlias == "" {
		leftAlias = definition.name
	}
	if rightAlias == "" {
		rightAlias = rightTable
	}

	anchorResult, err := e.executeSelectStatement(ctx, anchor, databaseName)
	if err != nil {
		return nil, true, err
	}
	baseStmt, err := sqlparser.Parse("select * from " + rightTable)
	if err != nil {
		return nil, true, err
	}
	baseSelect, ok := baseStmt.(*sqlparser.Select)
	if !ok {
		return nil, true, fmt.Errorf("recursive CTE base table is not SELECT")
	}
	baseResult, err := e.executeSelectStatement(ctx, baseSelect, databaseName)
	if err != nil {
		return nil, true, err
	}
	baseRows := recursiveCTEResultRows(baseResult, rightAlias, rightTable)

	rows := make([][]interface{}, 0, anchorResult.RowCount)
	seen := make(map[string]struct{}, anchorResult.RowCount)
	frontier := make([][]interface{}, 0, anchorResult.RowCount)
	for _, row := range recursiveCTEResultValueRows(anchorResult) {
		key := derivedValuesKey(row)
		if _, exists := seen[key]; exists {
			if distinct {
				continue
			}
			return nil, true, fmt.Errorf("cycle detected in recursive CTE %s", definition.name)
		}
		seen[key] = struct{}{}
		rows = append(rows, row)
		frontier = append(frontier, row)
	}

	for depth := 0; depth < 1000 && len(frontier) > 0; depth++ {
		nextFrontier := make([][]interface{}, 0)
		for _, current := range frontier {
			cteValues := recursiveCTERowValues(definition, leftAlias, current)
			for _, baseRow := range baseRows {
				values := make(map[string]interface{}, len(cteValues)+len(baseRow))
				for key, value := range cteValues {
					values[key] = value
				}
				for key, value := range baseRow {
					if _, exists := values[key]; !exists || strings.Contains(key, ".") {
						values[key] = value
					}
				}
				matched, err := evalPredicate(join.Condition.On, values)
				if err != nil {
					return nil, true, err
				}
				if !matched {
					continue
				}
				if recursive.Where != nil {
					matched, err = evalPredicate(recursive.Where.Expr, values)
					if err != nil {
						return nil, true, err
					}
					if !matched {
						continue
					}
				}
				next := make([]interface{}, 0, len(definition.columns))
				for _, expression := range recursive.SelectExprs {
					aliased, ok := expression.(*sqlparser.AliasedExpr)
					if !ok {
						return nil, true, fmt.Errorf("recursive CTE member has unsupported expression %T", expression)
					}
					value, err := evaluateExpressionWithRow(aliased.Expr, values)
					if err != nil {
						return nil, true, err
					}
					next = append(next, value)
				}
				key := derivedValuesKey(next)
				if _, exists := seen[key]; exists {
					if distinct {
						continue
					}
					return nil, true, fmt.Errorf("cycle detected in recursive CTE %s", definition.name)
				}
				seen[key] = struct{}{}
				rows = append(rows, next)
				nextFrontier = append(nextFrontier, next)
			}
		}
		frontier = nextFrontier
	}
	if len(frontier) > 0 {
		return nil, true, fmt.Errorf("recursive CTE %s exceeded maximum recursion depth 1000", definition.name)
	}
	var result *SelectResult
	if len(definition.columns) == 1 {
		result, err = executeRecursiveCTEMainQuery(mainQuery, definition, flattenRecursiveCTERows(rows))
	} else {
		result, err = executeRecursiveCTEMultiColumnMainQuery(mainQuery, definition, rows)
	}
	return result, true, err
}

// executeRecursiveCTEDualAnchorJoin handles the common recursive hierarchy
// shape with a literal/DUAL anchor and an inner-join recursive member that
// combines the CTE frontier with persisted base tables. The join graph is
// intentionally limited to inner joins; outer/correlated recursive AST paths
// remain outside this compatibility path.
func (e *XMySQLExecutor) executeRecursiveCTEDualAnchorJoin(ctx *ExecutionContext, databaseName, mainQuery string, definition cteCompatibilityDefinition, parts []string, distinct bool) (*SelectResult, bool, error) {
	anchor, err := parseSelectSQL(strings.TrimSpace(parts[0]))
	if err != nil || !recursiveCTEAnchorHasOnlyDual(anchor) || len(anchor.SelectExprs) != len(definition.columns) {
		return nil, false, nil
	}
	recursive, err := parseSelectSQL(strings.TrimSpace(parts[1]))
	if err != nil || len(recursive.SelectExprs) != len(definition.columns) || len(recursive.From) != 1 {
		return nil, false, nil
	}
	if _, ok := recursive.From[0].(*sqlparser.JoinTableExpr); !ok || !recursiveCTEJoinContainsBaseTable(recursive.From[0], definition.name) {
		return nil, false, nil
	}

	anchorValues := make([]interface{}, 0, len(definition.columns))
	for _, expression := range anchor.SelectExprs {
		aliased, ok := expression.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, true, fmt.Errorf("recursive CTE anchor has unsupported expression %T", expression)
		}
		value, err := evaluateExpressionWithRow(aliased.Expr, recursiveCTEExpressionValues(definition))
		if err != nil {
			return nil, true, err
		}
		anchorValues = append(anchorValues, value)
	}
	rows := [][]interface{}{anchorValues}
	frontier := [][]interface{}{anchorValues}
	seen := map[string]struct{}{derivedValuesKey(anchorValues): {}}
	baseCache := make(map[string][]map[string]interface{})
	for depth := 0; depth < 1000 && len(frontier) > 0; depth++ {
		nextFrontier := make([][]interface{}, 0)
		for _, current := range frontier {
			valuesFromCTE := recursiveCTERowValues(definition, definition.name, current)
			memberRows, err := e.recursiveCTEMemberJoinRows(ctx, databaseName, recursive.From[0], definition.name, valuesFromCTE, baseCache)
			if err != nil {
				return nil, true, err
			}
			for _, values := range memberRows {
				if recursive.Where != nil {
					matched, err := evalPredicate(recursive.Where.Expr, values)
					if err != nil {
						return nil, true, err
					}
					if !matched {
						continue
					}
				}
				next := make([]interface{}, 0, len(definition.columns))
				for _, expression := range recursive.SelectExprs {
					aliased, ok := expression.(*sqlparser.AliasedExpr)
					if !ok {
						return nil, true, fmt.Errorf("recursive CTE member has unsupported expression %T", expression)
					}
					value, err := evaluateExpressionWithRow(aliased.Expr, values)
					if err != nil {
						return nil, true, err
					}
					next = append(next, value)
				}
				key := derivedValuesKey(next)
				if _, exists := seen[key]; exists {
					if distinct {
						continue
					}
					return nil, true, fmt.Errorf("cycle detected in recursive CTE %s", definition.name)
				}
				seen[key] = struct{}{}
				rows = append(rows, next)
				nextFrontier = append(nextFrontier, next)
			}
		}
		frontier = nextFrontier
	}
	if len(frontier) > 0 {
		return nil, true, fmt.Errorf("recursive CTE %s exceeded maximum recursion depth 1000", definition.name)
	}
	if len(definition.columns) == 1 {
		result, err := executeRecursiveCTEMainQuery(mainQuery, definition, flattenRecursiveCTERows(rows))
		return result, true, err
	}
	result, err := executeRecursiveCTEMultiColumnMainQuery(mainQuery, definition, rows)
	return result, true, err
}

func recursiveCTEJoinContainsBaseTable(expr sqlparser.TableExpr, cteName string) bool {
	if table, _, ok := recursiveCTETableExprInfo(expr); ok {
		return !strings.EqualFold(table, cteName) && !strings.EqualFold(table, "dual")
	}
	join, ok := expr.(*sqlparser.JoinTableExpr)
	if !ok {
		return false
	}
	return recursiveCTEJoinContainsBaseTable(join.LeftExpr, cteName) || recursiveCTEJoinContainsBaseTable(join.RightExpr, cteName)
}

func (e *XMySQLExecutor) recursiveCTEMemberJoinRows(ctx *ExecutionContext, databaseName string, expr sqlparser.TableExpr, cteName string, cteValues map[string]interface{}, baseCache map[string][]map[string]interface{}) ([]map[string]interface{}, error) {
	if table, alias, ok := recursiveCTETableExprInfo(expr); ok {
		if strings.EqualFold(table, cteName) {
			return []map[string]interface{}{cteValues}, nil
		}
		if strings.EqualFold(table, "dual") {
			return nil, fmt.Errorf("recursive CTE member cannot join DUAL")
		}
		if alias == "" {
			alias = table
		}
		cacheKey := strings.ToLower(table + "\x00" + alias)
		if rows, exists := baseCache[cacheKey]; exists {
			return rows, nil
		}
		statement, err := sqlparser.Parse("select * from " + table)
		if err != nil {
			return nil, err
		}
		selectStmt, ok := statement.(*sqlparser.Select)
		if !ok {
			return nil, fmt.Errorf("recursive CTE base table is not SELECT")
		}
		result, err := e.executeSelectStatement(ctx, selectStmt, databaseName)
		if err != nil {
			return nil, err
		}
		rows := recursiveCTEResultRows(result, alias, table)
		baseCache[cacheKey] = rows
		return rows, nil
	}
	join, ok := expr.(*sqlparser.JoinTableExpr)
	if !ok || join.Condition.On == nil {
		return nil, fmt.Errorf("recursive CTE member supports JOIN ... ON")
	}
	leftRows, err := e.recursiveCTEMemberJoinRows(ctx, databaseName, join.LeftExpr, cteName, cteValues, baseCache)
	if err != nil {
		return nil, err
	}
	rightRows, err := e.recursiveCTEMemberJoinRows(ctx, databaseName, join.RightExpr, cteName, cteValues, baseCache)
	if err != nil {
		return nil, err
	}
	joinType := strings.ToLower(strings.TrimSpace(join.Join))
	if joinType != sqlparser.JoinStr && joinType != sqlparser.LeftJoinStr && joinType != sqlparser.RightJoinStr {
		return nil, fmt.Errorf("recursive CTE member supports only INNER/LEFT/RIGHT JOIN ... ON")
	}
	combined := make([]map[string]interface{}, 0, len(leftRows)*len(rightRows))
	matchedRight := make([]bool, len(rightRows))
	for _, left := range leftRows {
		matchedLeft := false
		for rightIndex, right := range rightRows {
			values := recursiveCTEMergeJoinRows(left, right)
			matched, err := evalPredicate(join.Condition.On, values)
			if err != nil {
				return nil, err
			}
			if matched {
				matchedLeft = true
				matchedRight[rightIndex] = true
				combined = append(combined, values)
			}
		}
		if joinType == sqlparser.LeftJoinStr && !matchedLeft && len(rightRows) > 0 {
			combined = append(combined, recursiveCTEMergeJoinRows(left, recursiveCTENullJoinRow(rightRows[0])))
		}
	}
	if joinType == sqlparser.RightJoinStr {
		for rightIndex, right := range rightRows {
			if matchedRight[rightIndex] || len(leftRows) == 0 {
				continue
			}
			combined = append(combined, recursiveCTEMergeJoinRows(recursiveCTENullJoinRow(leftRows[0]), right))
		}
	}
	return combined, nil
}

func recursiveCTEMergeJoinRows(left, right map[string]interface{}) map[string]interface{} {
	values := make(map[string]interface{}, len(left)+len(right))
	for key, value := range left {
		values[key] = value
	}
	for key, value := range right {
		if _, exists := values[key]; !exists || strings.Contains(key, ".") {
			values[key] = value
		}
	}
	return values
}

func recursiveCTENullJoinRow(template map[string]interface{}) map[string]interface{} {
	nullRow := make(map[string]interface{}, len(template))
	for key := range template {
		nullRow[key] = nil
	}
	return nullRow
}

func flattenRecursiveCTERows(rows [][]interface{}) []interface{} {
	values := make([]interface{}, 0, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			values = append(values, nil)
			continue
		}
		values = append(values, row[0])
	}
	return values
}

func recursiveCTETableExprInfo(expr sqlparser.TableExpr) (string, string, bool) {
	aliased, ok := expr.(*sqlparser.AliasedTableExpr)
	if !ok {
		return "", "", false
	}
	table, ok := aliased.Expr.(sqlparser.TableName)
	if !ok {
		return "", "", false
	}
	return table.Name.String(), strings.TrimSpace(aliased.As.String()), true
}

func recursiveCTEResultValueRows(result *SelectResult) [][]interface{} {
	if result == nil {
		return nil
	}
	rows := make([][]interface{}, 0, len(result.Records))
	for _, record := range result.Records {
		values := record.GetValues()
		row := make([]interface{}, 0, len(values))
		for _, value := range values {
			row = append(row, basicValueInterface(value))
		}
		rows = append(rows, row)
	}
	return rows
}

func recursiveCTEResultRows(result *SelectResult, alias, table string) []map[string]interface{} {
	rows := recursiveCTEResultValueRows(result)
	resultRows := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		values := make(map[string]interface{}, len(row)*3)
		for index, value := range row {
			if result == nil || index >= len(result.Columns) {
				continue
			}
			column := strings.Trim(result.Columns[index], "` ")
			values[column] = value
			values[strings.ToLower(column)] = value
			values[alias+"."+column] = value
			values[strings.ToLower(alias+"."+column)] = value
			values[table+"."+column] = value
			values[strings.ToLower(table+"."+column)] = value
		}
		resultRows = append(resultRows, values)
	}
	return resultRows
}

func (e *XMySQLExecutor) finishRecursiveCTECompatibility(ctx *ExecutionContext, mainQuery string, definition cteCompatibilityDefinition, values []interface{}, databaseName string) error {
	if handled, err := e.executeRecursiveCTEDML(ctx, mainQuery, definition, values, databaseName); handled {
		return err
	}
	if handled, err := e.executeRecursiveCTEInsert(ctx, mainQuery, definition, values, databaseName); handled {
		return err
	}
	if result, handled, err := e.executeRecursiveCTEMainJoinWithBaseTable(ctx, mainQuery, definition, values, databaseName); handled {
		if err != nil {
			return err
		}
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: fmt.Sprintf("CTE query executed successfully, %d rows returned", result.RowCount)}
		return nil
	}
	result, err := executeRecursiveCTEMainQuery(mainQuery, definition, values)
	if err != nil {
		return err
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: fmt.Sprintf("CTE query executed successfully, %d rows returned", result.RowCount)}
	return nil
}

// executeRecursiveCTEMainJoinWithBaseTable handles a materialized recursive
// CTE used as one side of the final SELECT JOIN. Recursive members already
// share the normal base-table executor, but the final-query compatibility path
// historically accepted only a bare CTE source (or two CTE sources). Reuse
// the existing materialized derived-join evaluator so ON/WHERE/projection,
// outer-join behavior, ordering and pagination stay consistent with ordinary
// derived joins.
func (e *XMySQLExecutor) executeRecursiveCTEMainJoinWithBaseTable(ctx *ExecutionContext, mainQuery string, definition cteCompatibilityDefinition, values []interface{}, databaseName string) (*SelectResult, bool, error) {
	statement, err := sqlparser.Parse(strings.TrimSpace(mainQuery))
	if err != nil {
		return nil, false, nil
	}
	selectStmt, ok := statement.(*sqlparser.Select)
	if !ok || len(selectStmt.From) != 1 {
		return nil, false, nil
	}
	if !recursiveCTEContainsJoin(selectStmt.From[0]) {
		return nil, false, nil
	}
	cteAliases := recursiveCTEMaterializedAliases(selectStmt.From[0], definition.name)
	if len(cteAliases) == 0 {
		return nil, false, nil
	}
	columnType := "varchar"
	if len(values) > 0 {
		switch values[0].(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			columnType = "bigint"
		}
	}
	materialized := newCompatibilityTypedResult("cte", []string{definition.column}, []string{columnType}, func() [][]interface{} {
		rows := make([][]interface{}, 0, len(values))
		for _, value := range values {
			rows = append(rows, []interface{}{value})
		}
		return rows
	}())
	materializedSources := make(map[string]*SelectResult, len(cteAliases))
	for _, alias := range cteAliases {
		materializedSources[strings.ToLower(alias)] = materialized
	}
	result, err := e.executeDerivedJoinSelectWithMaterialized(ctx, selectStmt, databaseName, materializedSources)
	return result, true, err
}

func (e *XMySQLExecutor) executeRecursiveCTEMultiColumnMainJoinWithBaseTable(ctx *ExecutionContext, mainQuery string, definition cteCompatibilityDefinition, rows [][]interface{}, databaseName string) (*SelectResult, bool, error) {
	statement, err := sqlparser.Parse(strings.TrimSpace(mainQuery))
	if err != nil {
		return nil, false, nil
	}
	selectStmt, ok := statement.(*sqlparser.Select)
	if !ok || len(selectStmt.From) != 1 {
		return nil, false, nil
	}
	if !recursiveCTEContainsJoin(selectStmt.From[0]) {
		return nil, false, nil
	}
	cteAliases := recursiveCTEMaterializedAliases(selectStmt.From[0], definition.name)
	if len(cteAliases) == 0 {
		return nil, false, nil
	}
	columnTypes := make([]string, len(definition.columns))
	for index := range columnTypes {
		columnTypes[index] = "varchar"
		for _, row := range rows {
			if index >= len(row) || row[index] == nil {
				continue
			}
			switch row[index].(type) {
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				columnTypes[index] = "bigint"
			}
			break
		}
	}
	materialized := newCompatibilityTypedResult("cte", definition.columns, columnTypes, rows)
	materializedSources := make(map[string]*SelectResult, len(cteAliases))
	for _, alias := range cteAliases {
		materializedSources[strings.ToLower(alias)] = materialized
	}
	result, err := e.executeDerivedJoinSelectWithMaterialized(ctx, selectStmt, databaseName, materializedSources)
	return result, true, err
}

func recursiveCTEContainsJoin(expression sqlparser.TableExpr) bool {
	switch table := expression.(type) {
	case *sqlparser.JoinTableExpr:
		return true
	case *sqlparser.ParenTableExpr:
		for _, nested := range table.Exprs {
			if recursiveCTEContainsJoin(nested) {
				return true
			}
		}
	}
	return false
}

func recursiveCTEMaterializedAlias(expression sqlparser.TableExpr, name string) (string, int) {
	switch table := expression.(type) {
	case *sqlparser.AliasedTableExpr:
		tableName, ok := table.Expr.(sqlparser.TableName)
		if !ok || !strings.EqualFold(tableName.Name.String(), name) {
			return "", 0
		}
		return strings.TrimSpace(table.As.String()), 1
	case *sqlparser.JoinTableExpr:
		leftAlias, leftCount := recursiveCTEMaterializedAlias(table.LeftExpr, name)
		rightAlias, rightCount := recursiveCTEMaterializedAlias(table.RightExpr, name)
		if leftCount > 0 {
			return leftAlias, leftCount + rightCount
		}
		return rightAlias, rightCount
	case *sqlparser.ParenTableExpr:
		var alias string
		count := 0
		for _, nested := range table.Exprs {
			nestedAlias, nestedCount := recursiveCTEMaterializedAlias(nested, name)
			if alias == "" {
				alias = nestedAlias
			}
			count += nestedCount
		}
		return alias, count
	default:
		return "", 0
	}
}

func recursiveCTEMaterializedAliases(expression sqlparser.TableExpr, name string) []string {
	aliases := make([]string, 0, 1)
	var collect func(sqlparser.TableExpr)
	collect = func(current sqlparser.TableExpr) {
		switch table := current.(type) {
		case *sqlparser.AliasedTableExpr:
			tableName, ok := table.Expr.(sqlparser.TableName)
			if !ok || !strings.EqualFold(tableName.Name.String(), name) {
				return
			}
			alias := strings.TrimSpace(table.As.String())
			if alias == "" {
				alias = tableName.Name.String()
			}
			aliases = append(aliases, alias)
		case *sqlparser.JoinTableExpr:
			collect(table.LeftExpr)
			collect(table.RightExpr)
		case *sqlparser.ParenTableExpr:
			for _, nested := range table.Exprs {
				collect(nested)
			}
		}
	}
	collect(expression)
	return aliases
}

func splitRecursiveCTEQuery(query string) (parts []string, distinct, ok bool) {
	if split := splitTopLevelKeyword(query, "union all"); len(split) == 2 {
		return split, false, true
	}
	if split := splitTopLevelKeyword(query, "union distinct"); len(split) == 2 {
		return split, true, true
	}
	if split := splitTopLevelKeyword(query, "union"); len(split) == 2 {
		return split, true, true
	}
	return nil, false, false
}

// materializeRecursiveCTESingleColumnGeneric evaluates the common recursive
// AST shape before the legacy fast path. It supports arbitrary expressions
// already understood by evaluateExpressionWithRow, while keeping unsupported
// shapes available to the older compatibility matcher.
func materializeRecursiveCTESingleColumnGeneric(definition cteCompatibilityDefinition, parts []string, distinct bool) ([]interface{}, bool, error) {
	anchor, err := parseSelectSQL(strings.TrimSpace(parts[0]))
	if err != nil {
		return nil, false, nil
	}
	recursive, err := parseSelectSQL(strings.TrimSpace(parts[1]))
	if err != nil {
		return nil, false, nil
	}
	if !recursiveCTEAnchorHasOnlyDual(anchor) || len(anchor.SelectExprs) != 1 || len(recursive.SelectExprs) != 1 || len(recursive.From) != 1 {
		return nil, false, nil
	}
	from, ok := recursive.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, false, nil
	}
	table, ok := from.Expr.(sqlparser.TableName)
	if !ok || !strings.EqualFold(table.Name.String(), definition.name) {
		return nil, false, nil
	}
	anchorExpression, ok := anchor.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, false, nil
	}
	anchorValue, err := evaluateExpressionWithRow(anchorExpression.Expr, recursiveCTEExpressionValues(definition))
	if err != nil {
		return nil, true, err
	}
	values := []interface{}{anchorValue}
	seen := map[string]struct{}{derivedValuesKey([]interface{}{anchorValue}): {}}
	alias := strings.TrimSpace(from.As.String())
	if alias == "" {
		alias = definition.name
	}
	recursiveExpression, ok := recursive.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, false, nil
	}
	for iteration := 0; iteration < 1000; iteration++ {
		current := values[len(values)-1]
		rowValues := recursiveCTERowValues(definition, alias, []interface{}{current})
		if recursive.Where != nil {
			matched, err := evalPredicate(recursive.Where.Expr, rowValues)
			if err != nil {
				return nil, true, err
			}
			if !matched {
				break
			}
		}
		next, err := evaluateExpressionWithRow(recursiveExpression.Expr, rowValues)
		if err != nil {
			return nil, true, err
		}
		key := derivedValuesKey([]interface{}{next})
		if _, exists := seen[key]; exists {
			if distinct {
				break
			}
			return nil, true, fmt.Errorf("recursive CTE step must be non-zero or cycle detected in %s", definition.name)
		}
		seen[key] = struct{}{}
		values = append(values, next)
	}
	if len(values) >= 1000 {
		return nil, true, fmt.Errorf("recursive CTE %s exceeded maximum recursion depth 1000", definition.name)
	}
	return values, true, nil
}

func (e *XMySQLExecutor) executeRecursiveCTEInsert(ctx *ExecutionContext, mainQuery string, definition cteCompatibilityDefinition, values []interface{}, databaseName string) (bool, error) {
	match := regexp.MustCompile(`(?is)^insert\s+(ignore\s+)?into\s+((?:[a-zA-Z0-9_$]+\s*\.\s*)?[a-zA-Z0-9_$]+)(?:\s*\(([^)]*)\))?\s+(.+)$`).FindStringSubmatch(strings.TrimSpace(mainQuery))
	if len(match) == 0 || !strings.HasPrefix(strings.ToLower(strings.TrimSpace(match[4])), "select ") {
		return false, nil
	}
	selectResult, err := executeRecursiveCTEMainQuery(strings.TrimSpace(match[4]), definition, values)
	if err != nil {
		return true, err
	}
	if selectResult.RowCount == 0 {
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "recursive CTE insert affected 0 rows"}
		return true, nil
	}
	columns := strings.TrimSpace(match[3])
	insertPrefix := "insert into " + strings.TrimSpace(match[2])
	if strings.TrimSpace(match[1]) != "" {
		insertPrefix = "insert ignore into " + strings.TrimSpace(match[2])
	}
	if columns != "" {
		insertPrefix += " (" + columns + ")"
	}
	rows := make([]string, 0, selectResult.RowCount)
	for _, record := range selectResult.Records {
		literals := make([]string, 0, len(record.GetValues()))
		for _, value := range record.GetValues() {
			literals = append(literals, recursiveCTEValueSQLLiteral(value))
		}
		rows = append(rows, "("+strings.Join(literals, ", ")+")")
	}
	insertQuery := insertPrefix + " values " + strings.Join(rows, ", ")
	nestedResults := make(chan *Result, 8)
	nestedCtx := &ExecutionContext{Context: ctx.Context, Results: nestedResults, Cfg: ctx.Cfg, DatabaseName: databaseName, RawQuery: insertQuery, Session: ctx.Session}
	go e.executeQuery(nestedCtx, ctx.Session, insertQuery, databaseName, nestedResults)
	for result := range nestedResults {
		if result != nil {
			ctx.Results <- result
		}
	}
	return true, nil
}

// executeRecursiveCTEDML handles the useful single-column DML form where a
// recursive result selects target rows through IN/NOT IN. The recursive CTE is
// still materialized per statement; only the materialized predicate is handed
// to the ordinary UPDATE/DELETE executor, preserving its transaction and
// constraint behavior.
func (e *XMySQLExecutor) executeRecursiveCTEDML(ctx *ExecutionContext, mainQuery string, definition cteCompatibilityDefinition, values []interface{}, databaseName string) (bool, error) {
	trimmed := strings.TrimSpace(mainQuery)
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "update ") && !strings.HasPrefix(lower, "delete from ") {
		return false, nil
	}
	if len(definition.columns) != 1 {
		return false, nil
	}
	column := regexp.QuoteMeta(definition.column)
	table := regexp.QuoteMeta(definition.name)
	pattern := regexp.MustCompile(`(?is)(` + "`?" + column + "`?" + `)\s+(not\s+in|in)\s*\(\s*select\s+` + "`?" + column + "`?" + `\s+from\s+` + "`?" + table + "`?" + `\s*\)`)
	literals := make([]string, 0, len(values))
	for _, value := range values {
		literals = append(literals, recursiveCTEInterfaceSQLLiteral(value))
	}
	rewritten := pattern.ReplaceAllString(trimmed, `$1 $2 (`+strings.Join(literals, ", ")+`)`)
	if rewritten == trimmed {
		return false, nil
	}
	nestedResults := make(chan *Result, 8)
	nestedCtx := &ExecutionContext{Context: ctx.Context, Results: nestedResults, Cfg: ctx.Cfg, DatabaseName: databaseName, RawQuery: rewritten, Session: ctx.Session}
	go e.executeQuery(nestedCtx, ctx.Session, rewritten, databaseName, nestedResults)
	for result := range nestedResults {
		if result != nil {
			ctx.Results <- result
		}
	}
	return true, nil
}

func recursiveCTEInterfaceSQLLiteral(value interface{}) string {
	if value == nil {
		return "NULL"
	}
	switch typed := value.(type) {
	case int:
		return strconv.Itoa(typed)
	case int8, int16, int32, int64:
		return strconv.FormatInt(reflectValueInt64(typed), 10)
	case uint, uint8, uint16, uint32, uint64:
		return strconv.FormatUint(reflectValueUint64(typed), 10)
	case float32:
		return strconv.FormatFloat(float64(typed), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(typed, 'g', -1, 64)
	case bool:
		if typed {
			return "1"
		}
		return "0"
	default:
		return "'" + strings.ReplaceAll(fmt.Sprint(value), "'", "''") + "'"
	}
}

func reflectValueInt64(value interface{}) int64 {
	switch typed := value.(type) {
	case int8:
		return int64(typed)
	case int16:
		return int64(typed)
	case int32:
		return int64(typed)
	case int64:
		return typed
	default:
		return 0
	}
}

func reflectValueUint64(value interface{}) uint64 {
	switch typed := value.(type) {
	case uint:
		return uint64(typed)
	case uint8:
		return uint64(typed)
	case uint16:
		return uint64(typed)
	case uint32:
		return uint64(typed)
	case uint64:
		return typed
	default:
		return 0
	}
}

func recursiveCTEValueSQLLiteral(value basic.Value) string {
	if value == nil || value.IsNull() {
		return "NULL"
	}
	switch value.Type() {
	case basic.ValueTypeTinyInt, basic.ValueTypeSmallInt, basic.ValueTypeMediumInt, basic.ValueTypeInt, basic.ValueTypeBigInt:
		return strconv.FormatInt(value.Int(), 10)
	case basic.ValueTypeFloat, basic.ValueTypeDouble, basic.ValueTypeDecimal:
		return strconv.FormatFloat(value.Float64(), 'g', -1, 64)
	default:
		return "'" + strings.ReplaceAll(value.String(), "'", "''") + "'"
	}
}

func executeRecursiveCTEMainQuery(mainQuery string, definition cteCompatibilityDefinition, values []interface{}) (*SelectResult, error) {
	statement, err := sqlparser.Parse(strings.TrimSpace(mainQuery))
	if err != nil {
		return nil, err
	}
	selectStmt, ok := statement.(*sqlparser.Select)
	if !ok || len(selectStmt.From) != 1 {
		return nil, fmt.Errorf("recursive CTE main query must be a single SELECT over %s", definition.name)
	}
	from, ok := selectStmt.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, fmt.Errorf("recursive CTE main query has unsupported FROM expression")
	}
	tableName, ok := from.Expr.(sqlparser.TableName)
	if !ok || !strings.EqualFold(tableName.Name.String(), definition.name) {
		return nil, fmt.Errorf("recursive CTE main query must read from %s", definition.name)
	}
	alias := strings.TrimSpace(from.As.String())
	if alias == "" {
		alias = definition.name
	}

	rows := make([]derivedRow, 0, len(values))
	for _, value := range values {
		rowValues := map[string]interface{}{
			definition.column:                                value,
			strings.ToLower(definition.column):               value,
			alias + "." + definition.column:                  value,
			strings.ToLower(alias + "." + definition.column): value,
		}
		rows = append(rows, derivedRow{values: rowValues})
	}
	filtered := make([]derivedRow, 0, len(rows))
	for _, row := range rows {
		if selectStmt.Where != nil {
			matched, err := evalPredicate(selectStmt.Where.Expr, row.values)
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}
		}
		filtered = append(filtered, row)
	}
	if containsAggregateSelectExpr(selectStmt.SelectExprs) || len(selectStmt.GroupBy) > 0 {
		return buildDerivedAggregateResult(selectStmt, filtered, alias)
	}

	projected := make([]derivedRow, 0, len(filtered))
	columns := make([]string, 0, len(selectStmt.SelectExprs))
	metaColumns := make([]*metadata.ColumnMeta, 0, len(selectStmt.SelectExprs))
	for _, sourceRow := range filtered {
		outputValues := make([]interface{}, 0, len(selectStmt.SelectExprs))
		outputMap := make(map[string]interface{}, len(sourceRow.values)+len(selectStmt.SelectExprs)*2)
		for key, value := range sourceRow.values {
			outputMap[key] = value
		}
		if len(selectStmt.SelectExprs) == 1 {
			if _, star := selectStmt.SelectExprs[0].(*sqlparser.StarExpr); star {
				outputValues = append(outputValues, sourceRow.values[definition.column])
				columns = append(columns, definition.column)
				metaColumns = append(metaColumns, &metadata.ColumnMeta{Name: definition.column, Type: projectionValueType(sourceRow.values[definition.column])})
				projected = append(projected, derivedRow{values: outputMap, order: outputValues})
				continue
			}
		}
		for _, expression := range selectStmt.SelectExprs {
			aliased, ok := expression.(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("recursive CTE main projection has unsupported expression %T", expression)
			}
			value, err := evaluateExpressionWithRow(aliased.Expr, sourceRow.values)
			if err != nil {
				return nil, err
			}
			name := derivedProjectionName(sqlparser.String(aliased.Expr))
			if !aliased.As.IsEmpty() {
				name = aliased.As.String()
			}
			outputValues = append(outputValues, value)
			outputMap[name] = value
			outputMap[strings.ToLower(name)] = value
			if len(projected) == 0 {
				columns = append(columns, name)
				metaColumns = append(metaColumns, &metadata.ColumnMeta{Name: name, Type: projectionValueType(value)})
			}
		}
		projected = append(projected, derivedRow{values: outputMap, order: outputValues})
	}

	if len(selectStmt.OrderBy) > 0 {
		sort.SliceStable(projected, func(left, right int) bool {
			return derivedRowsLess(projected[left], projected[right], selectStmt.OrderBy, columns)
		})
	}
	if strings.EqualFold(strings.TrimSpace(selectStmt.Distinct), strings.TrimSpace(sqlparser.DistinctStr)) {
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
	start, end := derivedLimitBounds(selectStmt.Limit, len(projected))
	projected = projected[start:end]
	resultRows := make([][]interface{}, 0, len(projected))
	for _, row := range projected {
		resultRows = append(resultRows, row.order)
	}
	columnTypes := make([]string, 0, len(metaColumns))
	for _, column := range metaColumns {
		columnTypes = append(columnTypes, string(column.Type))
	}
	return newCompatibilityTypedResult("cte", columns, columnTypes, resultRows), nil
}

func materializeRecursiveCTEMultiColumnValues(definition cteCompatibilityDefinition, parts []string, distinct bool) ([][]interface{}, error) {
	anchor, err := parseSelectSQL(strings.TrimSpace(parts[0]))
	if err != nil {
		return nil, err
	}
	recursive, err := parseSelectSQL(strings.TrimSpace(parts[1]))
	if err != nil {
		return nil, err
	}
	if !recursiveCTEAnchorHasOnlyDual(anchor) || len(anchor.SelectExprs) != len(definition.columns) {
		return nil, fmt.Errorf("recursive CTE anchor must project exactly %d columns without a source table", len(definition.columns))
	}
	if len(recursive.From) != 1 || len(recursive.SelectExprs) != len(definition.columns) {
		return nil, fmt.Errorf("recursive CTE member must project exactly %d columns from %s", len(definition.columns), definition.name)
	}
	from, ok := recursive.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, fmt.Errorf("recursive CTE member has unsupported FROM expression")
	}
	tableName, ok := from.Expr.(sqlparser.TableName)
	if !ok || !strings.EqualFold(tableName.Name.String(), definition.name) {
		return nil, fmt.Errorf("recursive CTE member must read from %s", definition.name)
	}
	alias := strings.TrimSpace(from.As.String())
	if alias == "" {
		alias = definition.name
	}
	anchorValues := make([]interface{}, 0, len(definition.columns))
	for _, expression := range anchor.SelectExprs {
		aliased, ok := expression.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("recursive CTE anchor has unsupported expression %T", expression)
		}
		value, err := evaluateExpressionWithRow(aliased.Expr, recursiveCTEExpressionValues(definition))
		if err != nil {
			return nil, err
		}
		anchorValues = append(anchorValues, value)
	}
	rows := [][]interface{}{anchorValues}
	seen := map[string]struct{}{derivedValuesKey(anchorValues): {}}
	for iteration := 0; iteration < 1000; iteration++ {
		current := rows[len(rows)-1]
		rowValues := recursiveCTERowValues(definition, alias, current)
		if recursive.Where != nil {
			matched, err := evalPredicate(recursive.Where.Expr, rowValues)
			if err != nil {
				return nil, err
			}
			if !matched {
				break
			}
		}
		next := make([]interface{}, 0, len(definition.columns))
		for _, expression := range recursive.SelectExprs {
			aliased, ok := expression.(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("recursive CTE member has unsupported expression %T", expression)
			}
			value, err := evaluateExpressionWithRow(aliased.Expr, rowValues)
			if err != nil {
				return nil, err
			}
			next = append(next, value)
		}
		key := derivedValuesKey(next)
		if _, exists := seen[key]; exists {
			if distinct {
				break
			}
			return nil, fmt.Errorf("cycle detected in recursive CTE %s", definition.name)
		}
		seen[key] = struct{}{}
		rows = append(rows, next)
	}
	if len(rows) >= 1000 {
		return nil, fmt.Errorf("recursive CTE %s exceeded maximum recursion depth 1000", definition.name)
	}
	return rows, nil
}

// executeRecursiveCTEMultiColumnQuery covers the common materialized recursive
// CTE shape where the anchor and recursive member both project the declared
// CTE columns. It intentionally shares the existing expression and predicate
// evaluators so arithmetic, column references and supported scalar functions
// have the same behavior as ordinary SELECT projections.
func executeRecursiveCTEMultiColumnQuery(mainQuery string, definition cteCompatibilityDefinition, parts []string, distinct bool) (*SelectResult, error) {
	anchor, err := parseSelectSQL(strings.TrimSpace(parts[0]))
	if err != nil {
		return nil, err
	}
	recursive, err := parseSelectSQL(strings.TrimSpace(parts[1]))
	if err != nil {
		return nil, err
	}
	if !recursiveCTEAnchorHasOnlyDual(anchor) || len(anchor.SelectExprs) != len(definition.columns) {
		return nil, fmt.Errorf("recursive CTE anchor must project exactly %d columns without a source table", len(definition.columns))
	}
	if len(recursive.From) != 1 || len(recursive.SelectExprs) != len(definition.columns) {
		return nil, fmt.Errorf("recursive CTE member must project exactly %d columns from %s", len(definition.columns), definition.name)
	}
	from, ok := recursive.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, fmt.Errorf("recursive CTE member has unsupported FROM expression")
	}
	tableName, ok := from.Expr.(sqlparser.TableName)
	if !ok || !strings.EqualFold(tableName.Name.String(), definition.name) {
		return nil, fmt.Errorf("recursive CTE member must read from %s", definition.name)
	}
	alias := strings.TrimSpace(from.As.String())
	if alias == "" {
		alias = definition.name
	}

	anchorValues := make([]interface{}, 0, len(definition.columns))
	for _, expression := range anchor.SelectExprs {
		aliased, ok := expression.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("recursive CTE anchor has unsupported expression %T", expression)
		}
		value, err := evaluateExpressionWithRow(aliased.Expr, recursiveCTEExpressionValues(definition))
		if err != nil {
			return nil, err
		}
		anchorValues = append(anchorValues, value)
	}
	rows := [][]interface{}{anchorValues}
	seen := map[string]struct{}{derivedValuesKey(anchorValues): {}}
	for iteration := 0; iteration < 1000; iteration++ {
		current := rows[len(rows)-1]
		rowValues := recursiveCTERowValues(definition, alias, current)
		if recursive.Where != nil {
			matched, err := evalPredicate(recursive.Where.Expr, rowValues)
			if err != nil {
				return nil, err
			}
			if !matched {
				break
			}
		}
		next := make([]interface{}, 0, len(definition.columns))
		for _, expression := range recursive.SelectExprs {
			aliased, ok := expression.(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("recursive CTE member has unsupported expression %T", expression)
			}
			value, err := evaluateExpressionWithRow(aliased.Expr, rowValues)
			if err != nil {
				return nil, err
			}
			next = append(next, value)
		}
		key := derivedValuesKey(next)
		if _, exists := seen[key]; exists {
			if distinct {
				break
			}
			return nil, fmt.Errorf("cycle detected in recursive CTE %s", definition.name)
		}
		seen[key] = struct{}{}
		rows = append(rows, next)
	}
	if len(rows) >= 1000 {
		return nil, fmt.Errorf("recursive CTE %s exceeded maximum recursion depth 1000", definition.name)
	}
	return executeRecursiveCTEMultiColumnMainQuery(mainQuery, definition, rows)
}

func recursiveCTEAnchorHasOnlyDual(anchor *sqlparser.Select) bool {
	if len(anchor.From) == 0 {
		return true
	}
	if len(anchor.From) != 1 {
		return false
	}
	from, ok := anchor.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return false
	}
	table, ok := from.Expr.(sqlparser.TableName)
	return ok && strings.EqualFold(table.Name.String(), "dual")
}

func recursiveCTERowValues(definition cteCompatibilityDefinition, alias string, row []interface{}) map[string]interface{} {
	values := make(map[string]interface{}, len(definition.columns)*4+len(definition.sessionValues))
	values[sessionExpressionValuesKey] = definition.sessionValues
	for index, column := range definition.columns {
		if index >= len(row) {
			break
		}
		value := row[index]
		values[column] = value
		values[strings.ToLower(column)] = value
		qualified := definition.name + "." + column
		values[qualified] = value
		values[strings.ToLower(qualified)] = value
		qualified = alias + "." + column
		values[qualified] = value
		values[strings.ToLower(qualified)] = value
	}
	return values
}

func executeRecursiveCTEMultiColumnMainQuery(mainQuery string, definition cteCompatibilityDefinition, rows [][]interface{}) (*SelectResult, error) {
	statement, err := sqlparser.Parse(strings.TrimSpace(mainQuery))
	if err != nil {
		return nil, err
	}
	selectStmt, ok := statement.(*sqlparser.Select)
	if !ok || len(selectStmt.From) != 1 {
		return nil, fmt.Errorf("recursive CTE main query must be a single SELECT over %s", definition.name)
	}
	from, ok := selectStmt.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, fmt.Errorf("recursive CTE main query has unsupported FROM expression")
	}
	tableName, ok := from.Expr.(sqlparser.TableName)
	if !ok || !strings.EqualFold(tableName.Name.String(), definition.name) {
		return nil, fmt.Errorf("recursive CTE main query must read from %s", definition.name)
	}
	alias := strings.TrimSpace(from.As.String())
	if alias == "" {
		alias = definition.name
	}
	filtered := make([]derivedRow, 0, len(rows))
	for _, row := range rows {
		values := recursiveCTERowValues(definition, alias, row)
		if selectStmt.Where != nil {
			matched, err := evalPredicate(selectStmt.Where.Expr, values)
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}
		}
		filtered = append(filtered, derivedRow{values: values})
	}
	if containsAggregateSelectExpr(selectStmt.SelectExprs) || len(selectStmt.GroupBy) > 0 {
		return buildDerivedAggregateResult(selectStmt, filtered, alias)
	}

	projected := make([]derivedRow, 0, len(filtered))
	columns := make([]string, 0, len(selectStmt.SelectExprs))
	metaColumns := make([]*metadata.ColumnMeta, 0, len(selectStmt.SelectExprs))
	for _, sourceRow := range filtered {
		outputValues := make([]interface{}, 0, len(selectStmt.SelectExprs))
		outputMap := make(map[string]interface{}, len(sourceRow.values)+len(selectStmt.SelectExprs)*2)
		for key, value := range sourceRow.values {
			outputMap[key] = value
		}
		for _, expression := range selectStmt.SelectExprs {
			if star, ok := expression.(*sqlparser.StarExpr); ok {
				_ = star
				for _, column := range definition.columns {
					value := sourceRow.values[column]
					outputValues = append(outputValues, value)
					if len(projected) == 0 {
						columns = append(columns, column)
						metaColumns = append(metaColumns, &metadata.ColumnMeta{Name: column, Type: projectionValueType(value)})
					}
				}
				continue
			}
			aliased, ok := expression.(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("recursive CTE main projection has unsupported expression %T", expression)
			}
			value, err := evaluateExpressionWithRow(aliased.Expr, sourceRow.values)
			if err != nil {
				return nil, err
			}
			name := derivedProjectionName(sqlparser.String(aliased.Expr))
			if !aliased.As.IsEmpty() {
				name = aliased.As.String()
			}
			outputValues = append(outputValues, value)
			outputMap[name] = value
			outputMap[strings.ToLower(name)] = value
			if len(projected) == 0 {
				columns = append(columns, name)
				metaColumns = append(metaColumns, &metadata.ColumnMeta{Name: name, Type: projectionValueType(value)})
			}
		}
		projected = append(projected, derivedRow{values: outputMap, order: outputValues})
	}
	if len(selectStmt.OrderBy) > 0 {
		sort.SliceStable(projected, func(left, right int) bool {
			return derivedRowsLess(projected[left], projected[right], selectStmt.OrderBy, columns)
		})
	}
	start, end := derivedLimitBounds(selectStmt.Limit, len(projected))
	projected = projected[start:end]
	resultRows := make([][]interface{}, 0, len(projected))
	for _, row := range projected {
		resultRows = append(resultRows, row.order)
	}
	columnTypes := make([]string, 0, len(metaColumns))
	for _, column := range metaColumns {
		columnTypes = append(columnTypes, string(column.Type))
	}
	return newCompatibilityTypedResult("cte", columns, columnTypes, resultRows), nil
}

func executeRecursiveCTEJoinedMainQuery(selectStmt *sqlparser.Select, leftDefinition cteCompatibilityDefinition, leftAlias string, leftRows [][]interface{}, rightDefinition cteCompatibilityDefinition, rightAlias string, rightRows [][]interface{}) (*SelectResult, error) {
	join, ok := selectStmt.From[0].(*sqlparser.JoinTableExpr)
	if !ok || join.Condition.On == nil {
		return nil, fmt.Errorf("recursive CTE JOIN requires an ON predicate")
	}
	if leftAlias == "" {
		leftAlias = leftDefinition.name
	}
	if rightAlias == "" {
		rightAlias = rightDefinition.name
	}
	filtered := make([]derivedRow, 0, len(leftRows)*len(rightRows))
	for _, leftRow := range leftRows {
		leftValues := recursiveCTERowValues(leftDefinition, leftAlias, leftRow)
		for _, rightRow := range rightRows {
			values := make(map[string]interface{}, len(leftValues)+len(rightRow)*4)
			for key, value := range leftValues {
				values[key] = value
			}
			rightValues := recursiveCTERowValues(rightDefinition, rightAlias, rightRow)
			for key, value := range rightValues {
				if strings.Contains(key, ".") || values[key] == nil {
					values[key] = value
				}
			}
			matched, err := evalPredicate(join.Condition.On, values)
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}
			if selectStmt.Where != nil {
				matched, err = evalPredicate(selectStmt.Where.Expr, values)
				if err != nil {
					return nil, err
				}
				if !matched {
					continue
				}
			}
			filtered = append(filtered, derivedRow{values: values})
		}
	}
	if containsAggregateSelectExpr(selectStmt.SelectExprs) || len(selectStmt.GroupBy) > 0 {
		return buildDerivedAggregateResult(selectStmt, filtered, leftAlias+"_"+rightAlias)
	}

	projected := make([]derivedRow, 0, len(filtered))
	columns := make([]string, 0, len(selectStmt.SelectExprs))
	metaColumns := make([]*metadata.ColumnMeta, 0, len(selectStmt.SelectExprs))
	for _, sourceRow := range filtered {
		outputValues := make([]interface{}, 0, len(selectStmt.SelectExprs))
		outputMap := make(map[string]interface{}, len(sourceRow.values)+len(selectStmt.SelectExprs)*2)
		for key, value := range sourceRow.values {
			outputMap[key] = value
		}
		for _, expression := range selectStmt.SelectExprs {
			if _, star := expression.(*sqlparser.StarExpr); star {
				for _, definition := range []cteCompatibilityDefinition{leftDefinition, rightDefinition} {
					alias := leftAlias
					if !strings.EqualFold(definition.name, leftDefinition.name) {
						alias = rightAlias
					}
					for _, column := range definition.columns {
						value := sourceRow.values[alias+"."+column]
						outputValues = append(outputValues, value)
						if len(projected) == 0 {
							columns = append(columns, column)
							metaColumns = append(metaColumns, &metadata.ColumnMeta{Name: column, Type: projectionValueType(value)})
						}
					}
				}
				continue
			}
			aliased, ok := expression.(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("recursive CTE JOIN projection has unsupported expression %T", expression)
			}
			value, err := evaluateExpressionWithRow(aliased.Expr, sourceRow.values)
			if err != nil {
				return nil, err
			}
			name := derivedProjectionName(sqlparser.String(aliased.Expr))
			if !aliased.As.IsEmpty() {
				name = aliased.As.String()
			}
			outputValues = append(outputValues, value)
			outputMap[name] = value
			outputMap[strings.ToLower(name)] = value
			if len(projected) == 0 {
				columns = append(columns, name)
				metaColumns = append(metaColumns, &metadata.ColumnMeta{Name: name, Type: projectionValueType(value)})
			}
		}
		projected = append(projected, derivedRow{values: outputMap, order: outputValues})
	}
	if len(selectStmt.OrderBy) > 0 {
		sort.SliceStable(projected, func(left, right int) bool {
			return derivedRowsLess(projected[left], projected[right], selectStmt.OrderBy, columns)
		})
	}
	if strings.EqualFold(strings.TrimSpace(selectStmt.Distinct), strings.TrimSpace(sqlparser.DistinctStr)) {
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
	start, end := derivedLimitBounds(selectStmt.Limit, len(projected))
	projected = projected[start:end]
	resultRows := make([][]interface{}, 0, len(projected))
	for _, row := range projected {
		resultRows = append(resultRows, row.order)
	}
	columnTypes := make([]string, 0, len(metaColumns))
	for _, column := range metaColumns {
		columnTypes = append(columnTypes, string(column.Type))
	}
	return newCompatibilityTypedResult("cte", columns, columnTypes, resultRows), nil
}

func newCompatibilityTypedResult(name string, columns, columnTypes []string, rows [][]interface{}) *SelectResult {
	tableMeta := &metadata.TableMeta{Name: name, Columns: make([]*metadata.ColumnMeta, 0, len(columns))}
	for index, column := range columns {
		typeName := metadata.TypeVarchar
		if index < len(columnTypes) && strings.EqualFold(columnTypes[index], "bigint") {
			typeName = metadata.TypeBigInt
		}
		tableMeta.Columns = append(tableMeta.Columns, &metadata.ColumnMeta{Name: column, Type: typeName})
	}
	records := make([]Record, 0, len(rows))
	for _, row := range rows {
		records = append(records, NewExecutorRecordFromInterface(row, tableMeta))
	}
	return &SelectResult{Records: records, RowCount: len(records), Columns: columns, ColumnTypes: columnTypes, ResultType: common.RESULT_TYPE_QUERY}
}

type cteCompatibilityDefinition struct {
	name          string
	column        string
	columns       []string
	query         string
	sessionValues map[string]interface{}
}

func parseCTECompatibilityQuery(query string) ([]cteCompatibilityDefinition, string, error) {
	pos := len("with recursive")
	for pos < len(query) && (query[pos] == ' ' || query[pos] == '\t' || query[pos] == '\r' || query[pos] == '\n') {
		pos++
	}
	header := regexp.MustCompile(`(?is)^` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `\s*\(\s*([^)]*?)\s*\)\s+as\s*\(`)
	definitions := make([]cteCompatibilityDefinition, 0, 2)
	for {
		match := header.FindStringSubmatchIndex(query[pos:])
		if len(match) == 0 {
			return nil, "", fmt.Errorf("invalid recursive CTE definition")
		}
		name := query[pos+match[2] : pos+match[3]]
		rawColumns := query[pos+match[4] : pos+match[5]]
		columns := make([]string, 0, 2)
		for _, rawColumn := range strings.Split(rawColumns, ",") {
			column := strings.Trim(strings.TrimSpace(rawColumn), "`")
			if column == "" {
				return nil, "", fmt.Errorf("recursive CTE column name is empty")
			}
			columns = append(columns, column)
		}
		if len(columns) == 0 {
			return nil, "", fmt.Errorf("recursive CTE requires at least one column")
		}
		open := pos + match[1] - 1
		close := matchingParenIndex(query, open)
		if close < 0 {
			return nil, "", fmt.Errorf("unterminated recursive CTE definition")
		}
		definitions = append(definitions, cteCompatibilityDefinition{name: name, column: columns[0], columns: columns, query: query[open+1 : close]})

		restStart := close + 1
		for restStart < len(query) && (query[restStart] == ' ' || query[restStart] == '\t' || query[restStart] == '\r' || query[restStart] == '\n') {
			restStart++
		}
		if restStart >= len(query) || query[restStart] != ',' {
			return definitions, strings.TrimSpace(query[close+1:]), nil
		}
		next := restStart + 1
		for next < len(query) && (query[next] == ' ' || query[next] == '\t' || query[next] == '\r' || query[next] == '\n') {
			next++
		}
		if len(header.FindStringSubmatchIndex(query[next:])) == 0 {
			return definitions, strings.TrimSpace(query[next:]), nil
		}
		pos = next
	}
}

func recursiveCondition(value int64, operator string, limit int64) bool {
	switch operator {
	case "<":
		return value < limit
	case "<=":
		return value <= limit
	case "=":
		return value == limit
	case ">":
		return value > limit
	case ">=":
		return value >= limit
	default:
		return false
	}
}

func splitTopLevelKeyword(input, keyword string) []string {
	lower := strings.ToLower(input)
	keyword = strings.ToLower(keyword)
	depth := 0
	quote := byte(0)
	for i := 0; i <= len(input)-len(keyword); i++ {
		if quote != 0 {
			if input[i] == quote && (i == 0 || input[i-1] != '\\') {
				quote = 0
			}
			continue
		}
		if input[i] == '\'' || input[i] == '"' || input[i] == '`' {
			quote = input[i]
			continue
		}
		switch input[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && strings.HasPrefix(lower[i:], keyword) && (i == 0 || lower[i-1] == ' ') && (i+len(keyword) == len(input) || lower[i+len(keyword)] == ' ') {
			return []string{strings.TrimSpace(input[:i]), strings.TrimSpace(input[i+len(keyword):])}
		}
	}
	return []string{input}
}
