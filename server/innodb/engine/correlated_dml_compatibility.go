package engine

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// rewriteCorrelatedDMLSubquery materializes the target primary keys for the
// common single-table UPDATE/DELETE ... WHERE [NOT] EXISTS form. The legacy
// parser cannot represent a correlated subquery in a DML predicate, while the
// SELECT compatibility path already has the row-by-row binding semantics we
// need. Reusing that path keeps the DML executor responsible for locks,
// triggers, constraints, transaction journaling, and affected-row counts.
//
// The rewrite is deliberately bounded to one-column primary keys and a WHERE
// clause consisting solely of the correlated EXISTS predicate. More complex
// DML predicates remain on the normal parser path instead of being rewritten
// with potentially different boolean semantics.
func (e *XMySQLExecutor) rewriteCorrelatedDMLSubquery(ctx *ExecutionContext, query, databaseName string) (string, bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	lower := strings.ToLower(trimmed)
	kind := ""
	rest := ""
	switch {
	case strings.HasPrefix(lower, "update "):
		kind = "update"
		rest = strings.TrimSpace(trimmed[len("update"):])
	case strings.HasPrefix(lower, "delete from "):
		kind = "delete"
		rest = strings.TrimSpace(trimmed[len("delete from"):])
	default:
		return "", false, nil
	}

	whereParts := splitTopLevelKeyword(rest, "where")
	if len(whereParts) != 2 {
		if kind == "update" {
			setParts := splitTopLevelKeyword(rest, "set")
			if len(setParts) == 2 {
				targetClause := strings.TrimSpace(setParts[0])
				setText := strings.TrimSpace(setParts[1])
				if target, ok := parseCorrelatedDMLTarget(targetClause); ok {
					if rewritten, handled, err := e.rewriteCorrelatedDMLScalarAssignment(ctx, databaseName, kind, target, targetClause, setText, ""); handled {
						return rewritten, true, err
					}
				}
			}
		}
		return "", false, nil
	}
	fromOrTarget := strings.TrimSpace(whereParts[0])
	whereText := strings.TrimSpace(whereParts[1])
	if whereText == "" {
		return "", false, nil
	}

	targetClause := fromOrTarget
	setText := ""
	if kind == "update" {
		setParts := splitTopLevelKeyword(fromOrTarget, "set")
		if len(setParts) != 2 || strings.TrimSpace(setParts[1]) == "" {
			return "", false, nil
		}
		targetClause = strings.TrimSpace(setParts[0])
		setText = strings.TrimSpace(setParts[1])
	}
	target, ok := parseCorrelatedDMLTarget(targetClause)
	if !ok {
		return "", false, nil
	}
	if rewritten, handled, err := e.rewriteCorrelatedDMLScalarAssignment(ctx, databaseName, kind, target, targetClause, setText, whereText); handled {
		return rewritten, true, err
	}
	if rewritten, handled, err := e.rewriteCorrelatedDMLPredicate(ctx, databaseName, kind, target, targetClause, setText, whereText); handled {
		return rewritten, true, err
	}

	whereLower := strings.ToLower(whereText)
	notExists := strings.HasPrefix(whereLower, "not exists")
	existsAt := strings.Index(whereLower, "exists")
	if existsAt < 0 || (!notExists && strings.TrimSpace(whereText[:existsAt]) != "") || (notExists && strings.TrimSpace(whereText[:existsAt]) != "not") {
		return "", false, nil
	}
	open := strings.Index(whereLower[existsAt:], "(")
	if open < 0 {
		return "", true, fmt.Errorf("correlated DML EXISTS is missing parentheses")
	}
	open += existsAt
	close := matchingParenIndex(whereText, open)
	if close < 0 {
		return "", true, fmt.Errorf("unterminated correlated DML EXISTS subquery")
	}
	if strings.TrimSpace(whereText[close+1:]) != "" {
		return "", false, nil
	}
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(whereText[existsAt:])), "exists") {
		return "", false, nil
	}

	schemaName := databaseName
	tableName := target.tableName
	if target.schemaName != "" {
		schemaName = target.schemaName
	}
	meta, err := e.correlatedDMLTableMetadata(contextOrBackground(ctx), schemaName, tableName)
	if err != nil {
		return "", true, fmt.Errorf("load correlated DML target metadata: %w", err)
	}
	if meta == nil || len(meta.PrimaryKey) == 0 {
		return "", false, nil
	}
	primaryKeys := correlatedDMLPrimaryKeys(meta.PrimaryKey)
	if len(primaryKeys) == 0 {
		return "", false, nil
	}

	outerSQL := fmt.Sprintf("select %s from %s where %s", correlatedDMLKeyProjection(target.alias, primaryKeys), targetClause, whereText)
	childResults := make(chan *Result, 1)
	childCtx := &ExecutionContext{Context: contextOrBackground(ctx), Results: childResults, Cfg: ctxConfig(ctx), DatabaseName: schemaName, RawQuery: outerSQL, Session: sessionFromContext(ctx)}
	handled, err := e.executeCorrelatedSubqueryCompatibility(childCtx, outerSQL, schemaName)
	if err != nil {
		return "", true, err
	}
	if !handled {
		return "", false, nil
	}
	var selected *SelectResult
	select {
	case result := <-childResults:
		if result == nil {
			return "", true, fmt.Errorf("correlated DML materialization returned no result")
		}
		if result.Err != nil {
			return "", true, result.Err
		}
		var ok bool
		selected, ok = result.Data.(*SelectResult)
		if !ok || selected == nil {
			return "", true, fmt.Errorf("correlated DML materialization returned invalid result")
		}
	default:
		return "", true, fmt.Errorf("correlated DML materialization returned no rows result")
	}

	predicate := correlatedDMLKeyPredicate(primaryKeys, selected)
	if kind == "update" {
		return fmt.Sprintf("update %s set %s where %s", targetClause, setText, predicate), true, nil
	}
	return fmt.Sprintf("delete from %s where %s", targetClause, predicate), true, nil
}

func (e *XMySQLExecutor) rewriteCorrelatedDMLScalarAssignment(ctx *ExecutionContext, databaseName, kind string, target correlatedDMLTarget, targetClause, setText, whereText string) (string, bool, error) {
	if kind != "update" {
		return "", false, nil
	}
	if assignments := splitTopLevelComma(setText); len(assignments) > 1 {
		return e.rewriteCorrelatedDMLMultiScalarAssignment(ctx, databaseName, target, targetClause, assignments, whereText)
	}
	assignment := strings.TrimSpace(setText)
	equals := topLevelAssignmentIndex(assignment)
	if equals < 0 {
		return "", false, nil
	}
	column := strings.Trim(strings.TrimSpace(assignment[:equals]), "`")
	right := strings.TrimSpace(assignment[equals+1:])
	if !regexp.MustCompile(`(?is)^[a-zA-Z0-9_$]+$`).MatchString(column) {
		return "", false, nil
	}
	ranges := correlatedDMLScalarSubqueryRanges(right, target.alias)
	if len(ranges) == 0 {
		return "", false, nil
	}
	meta, err := e.correlatedDMLTableMetadata(contextOrBackground(ctx), correlatedDMLSchema(databaseName, target), target.tableName)
	if err != nil {
		return "", true, fmt.Errorf("load correlated DML target metadata: %w", err)
	}
	primaryKeys := correlatedDMLPrimaryKeys(meta.PrimaryKey)
	if len(primaryKeys) == 0 {
		return "", false, nil
	}
	projection := correlatedDMLKeyProjection(target.alias, primaryKeys)
	for index, scalarRange := range ranges {
		projection += fmt.Sprintf(", %s as __correlated_value_%d", right[scalarRange.start:scalarRange.end+1], index)
	}
	outerSQL := fmt.Sprintf("select %s from %s", projection, targetClause)
	if strings.TrimSpace(whereText) != "" {
		outerSQL += " where " + whereText
	}
	childResults := make(chan *Result, 1)
	childCtx := &ExecutionContext{Context: contextOrBackground(ctx), Results: childResults, Cfg: ctxConfig(ctx), DatabaseName: correlatedDMLSchema(databaseName, target), RawQuery: outerSQL, Session: sessionFromContext(ctx)}
	handled, err := e.executeCorrelatedScalarSubqueryCompatibility(childCtx, outerSQL, correlatedDMLSchema(databaseName, target))
	if err != nil {
		return "", true, err
	}
	if !handled {
		return "", false, nil
	}
	result := <-childResults
	if result == nil {
		return "", true, fmt.Errorf("correlated scalar DML materialization returned no result")
	}
	if result.Err != nil {
		return "", true, result.Err
	}
	selected, ok := result.Data.(*SelectResult)
	if !ok || selected == nil {
		return "", true, fmt.Errorf("correlated scalar DML materialization returned invalid result")
	}
	whenClauses := make([][]string, len(ranges))
	keyPredicates := make([]string, 0, len(selected.Records))
	for _, record := range selected.Records {
		values := record.GetValues()
		if len(values) < len(primaryKeys)+len(ranges) {
			continue
		}
		terms := make([]string, 0, len(primaryKeys))
		valid := true
		for index, key := range primaryKeys {
			if values[index].IsNull() {
				valid = false
				break
			}
			terms = append(terms, fmt.Sprintf("%s = %s", key, correlatedValueSQLLiteral(values[index])))
		}
		if !valid {
			continue
		}
		condition := strings.Join(terms, " and ")
		for scalarIndex := range ranges {
			whenClauses[scalarIndex] = append(whenClauses[scalarIndex], fmt.Sprintf("when %s then %s", condition, correlatedValueSQLLiteral(values[len(primaryKeys)+scalarIndex])))
		}
		keyPredicates = append(keyPredicates, "("+condition+")")
	}
	predicate := "1 = 0"
	if len(keyPredicates) > 0 {
		predicate = strings.Join(keyPredicates, " or ")
	}
	rewrittenRight := right
	for scalarIndex := len(ranges) - 1; scalarIndex >= 0; scalarIndex-- {
		scalarRange := ranges[scalarIndex]
		caseExpression := "case " + strings.Join(whenClauses[scalarIndex], " ") + " else " + column + " end"
		rewrittenRight = rewrittenRight[:scalarRange.start] + caseExpression + rewrittenRight[scalarRange.end+1:]
	}
	return fmt.Sprintf("update %s set %s = %s where %s", targetClause, column, rewrittenRight, predicate), true, nil
}

func correlatedDMLScalarSubqueryRange(expression, alias string) (int, int, string, bool) {
	ranges := correlatedDMLScalarSubqueryRanges(expression, alias)
	if len(ranges) > 0 {
		return ranges[0].start, ranges[0].end, ranges[0].inner, true
	}
	return 0, 0, "", false
}

type correlatedDMLScalarRange struct {
	start int
	end   int
	inner string
}

func correlatedDMLScalarSubqueryRanges(expression, alias string) []correlatedDMLScalarRange {
	ranges := make([]correlatedDMLScalarRange, 0, 2)
	correlation := regexp.MustCompile(`(?i)` + regexp.QuoteMeta(alias) + `\s*\.`)
	for index := 0; index < len(expression); index++ {
		if expression[index] != '(' {
			continue
		}
		close := matchingParenIndex(expression, index)
		if close < 0 {
			return nil
		}
		inner := strings.TrimSpace(expression[index+1 : close])
		if strings.HasPrefix(strings.ToLower(inner), "select ") && correlation.MatchString(inner) {
			ranges = append(ranges, correlatedDMLScalarRange{start: index, end: close, inner: inner})
			index = close
		}
	}
	return ranges
}

func (e *XMySQLExecutor) rewriteCorrelatedDMLMultiScalarAssignment(ctx *ExecutionContext, databaseName string, target correlatedDMLTarget, targetClause string, assignments []string, whereText string) (string, bool, error) {
	type scalarAssignment struct {
		column      string
		right       string
		ranges      []correlatedDMLScalarRange
		valueOffset int
	}
	parsed := make([]scalarAssignment, 0, len(assignments))
	totalScalars := 0
	for _, raw := range assignments {
		assignment := strings.TrimSpace(raw)
		equals := topLevelAssignmentIndex(assignment)
		if equals < 0 {
			return "", false, nil
		}
		column := strings.Trim(strings.TrimSpace(assignment[:equals]), "`")
		right := strings.TrimSpace(assignment[equals+1:])
		if !regexp.MustCompile(`(?is)^[a-zA-Z0-9_$]+$`).MatchString(column) {
			return "", false, nil
		}
		ranges := correlatedDMLScalarSubqueryRanges(right, target.alias)
		if len(ranges) == 0 {
			return "", false, nil
		}
		parsed = append(parsed, scalarAssignment{column: column, right: right, ranges: ranges, valueOffset: totalScalars})
		totalScalars += len(ranges)
	}
	if len(parsed) <= 1 {
		return "", false, nil
	}
	meta, err := e.correlatedDMLTableMetadata(contextOrBackground(ctx), correlatedDMLSchema(databaseName, target), target.tableName)
	if err != nil {
		return "", true, fmt.Errorf("load correlated DML target metadata: %w", err)
	}
	primaryKeys := correlatedDMLPrimaryKeys(meta.PrimaryKey)
	if len(primaryKeys) == 0 {
		return "", false, nil
	}
	projection := correlatedDMLKeyProjection(target.alias, primaryKeys)
	valueIndex := 0
	for _, assignment := range parsed {
		for _, scalarRange := range assignment.ranges {
			projection += fmt.Sprintf(", %s as __correlated_value_%d", assignment.right[scalarRange.start:scalarRange.end+1], valueIndex)
			valueIndex++
		}
	}
	outerSQL := fmt.Sprintf("select %s from %s", projection, targetClause)
	if strings.TrimSpace(whereText) != "" {
		outerSQL += " where " + whereText
	}
	childResults := make(chan *Result, 1)
	childCtx := &ExecutionContext{Context: contextOrBackground(ctx), Results: childResults, Cfg: ctxConfig(ctx), DatabaseName: correlatedDMLSchema(databaseName, target), RawQuery: outerSQL, Session: sessionFromContext(ctx)}
	handled, err := e.executeCorrelatedScalarSubqueryCompatibility(childCtx, outerSQL, correlatedDMLSchema(databaseName, target))
	if err != nil {
		return "", true, err
	}
	if !handled {
		return "", false, nil
	}
	result := <-childResults
	if result == nil {
		return "", true, fmt.Errorf("correlated scalar DML materialization returned no result")
	}
	if result.Err != nil {
		return "", true, result.Err
	}
	selected, ok := result.Data.(*SelectResult)
	if !ok || selected == nil {
		return "", true, fmt.Errorf("correlated scalar DML materialization returned invalid result")
	}
	setClauses := make([]string, 0, len(parsed))
	for _, assignment := range parsed {
		whenClauses := make([][]string, len(assignment.ranges))
		for _, record := range selected.Records {
			values := record.GetValues()
			if len(values) < len(primaryKeys)+totalScalars {
				continue
			}
			terms := make([]string, 0, len(primaryKeys))
			valid := true
			for keyIndex, key := range primaryKeys {
				if values[keyIndex].IsNull() {
					valid = false
					break
				}
				terms = append(terms, fmt.Sprintf("%s = %s", key, correlatedValueSQLLiteral(values[keyIndex])))
			}
			if valid {
				for rangeIndex := range assignment.ranges {
					value := values[len(primaryKeys)+assignment.valueOffset+rangeIndex]
					whenClauses[rangeIndex] = append(whenClauses[rangeIndex], fmt.Sprintf("when %s then %s", strings.Join(terms, " and "), correlatedValueSQLLiteral(value)))
				}
			}
		}
		rewrittenRight := assignment.right
		for rangeIndex := len(assignment.ranges) - 1; rangeIndex >= 0; rangeIndex-- {
			scalarRange := assignment.ranges[rangeIndex]
			caseExpression := fmt.Sprintf("case %s else %s end", strings.Join(whenClauses[rangeIndex], " "), assignment.column)
			rewrittenRight = rewrittenRight[:scalarRange.start] + caseExpression + rewrittenRight[scalarRange.end+1:]
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", assignment.column, rewrittenRight))
	}
	predicate := correlatedDMLKeyPredicate(primaryKeys, selected)
	return fmt.Sprintf("update %s set %s where %s", targetClause, strings.Join(setClauses, ", "), predicate), true, nil
}

func topLevelAssignmentIndex(input string) int {
	depth := 0
	var quote byte
	for index := 0; index < len(input); index++ {
		ch := input[index]
		if quote != 0 {
			if ch == quote && (index == 0 || input[index-1] != '\\') {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case '=':
			if depth == 0 {
				return index
			}
		}
	}
	return -1
}

func (e *XMySQLExecutor) rewriteCorrelatedDMLPredicate(ctx *ExecutionContext, databaseName, kind string, target correlatedDMLTarget, targetClause, setText, whereText string) (string, bool, error) {
	match := regexp.MustCompile(`(?is)^(.+?)\s+(not\s+in|in)\s*\(`).FindStringSubmatchIndex(strings.TrimSpace(whereText))
	if len(match) != 6 {
		return "", false, nil
	}
	whereText = strings.TrimSpace(whereText)
	open := strings.Index(whereText[match[0]:match[1]], "(")
	if open < 0 {
		return "", true, fmt.Errorf("correlated DML predicate is missing parentheses")
	}
	open += match[0]
	close := matchingParenIndex(whereText, open)
	if close < 0 || strings.TrimSpace(whereText[close+1:]) != "" {
		return "", false, nil
	}
	innerSQL := strings.TrimSpace(whereText[open+1 : close])
	if !strings.HasPrefix(strings.ToLower(innerSQL), "select ") || !regexp.MustCompile(`(?i)`+regexp.QuoteMeta(target.alias)+`\s*\.`).MatchString(innerSQL) {
		return "", false, nil
	}
	meta, err := e.correlatedDMLTableMetadata(contextOrBackground(ctx), correlatedDMLSchema(databaseName, target), target.tableName)
	if err != nil {
		return "", true, fmt.Errorf("load correlated DML target metadata: %w", err)
	}
	if meta == nil || len(meta.PrimaryKey) == 0 {
		return "", false, nil
	}
	primaryKeys := correlatedDMLPrimaryKeys(meta.PrimaryKey)
	if len(primaryKeys) == 0 {
		return "", false, nil
	}
	outerSQL := fmt.Sprintf("select %s from %s where %s", correlatedDMLKeyProjection(target.alias, primaryKeys), targetClause, whereText)
	childResults := make(chan *Result, 1)
	childCtx := &ExecutionContext{Context: contextOrBackground(ctx), Results: childResults, Cfg: ctxConfig(ctx), DatabaseName: correlatedDMLSchema(databaseName, target), RawQuery: outerSQL, Session: sessionFromContext(ctx)}
	handled, err := e.executeCorrelatedPredicateSubqueryCompatibility(childCtx, outerSQL, correlatedDMLSchema(databaseName, target))
	if err != nil {
		return "", true, err
	}
	if !handled {
		return "", false, nil
	}
	result := <-childResults
	if result == nil {
		return "", true, fmt.Errorf("correlated DML materialization returned no result")
	}
	if result.Err != nil {
		return "", true, result.Err
	}
	selected, ok := result.Data.(*SelectResult)
	if !ok || selected == nil {
		return "", true, fmt.Errorf("correlated DML materialization returned invalid result")
	}
	predicate := correlatedDMLKeyPredicate(primaryKeys, selected)
	if kind == "update" {
		return fmt.Sprintf("update %s set %s where %s", targetClause, setText, predicate), true, nil
	}
	return fmt.Sprintf("delete from %s where %s", targetClause, predicate), true, nil
}

func correlatedDMLPrimaryKeys(columns []string) []string {
	keys := make([]string, 0, len(columns))
	for _, column := range columns {
		if clean := strings.Trim(strings.TrimSpace(column), "`"); clean != "" {
			keys = append(keys, clean)
		}
	}
	return keys
}

func correlatedDMLKeyProjection(alias string, keys []string) string {
	projection := make([]string, 0, len(keys))
	for _, key := range keys {
		projection = append(projection, alias+"."+key)
	}
	return strings.Join(projection, ", ")
}

func correlatedDMLKeyPredicate(keys []string, selected *SelectResult) string {
	if len(keys) == 0 || selected == nil {
		return "1 = 0"
	}
	rows := make([]string, 0, len(selected.Records))
	for _, record := range selected.Records {
		values := record.GetValues()
		if len(values) < len(keys) {
			continue
		}
		terms := make([]string, 0, len(keys))
		valid := true
		for index, key := range keys {
			if values[index].IsNull() {
				valid = false
				break
			}
			terms = append(terms, fmt.Sprintf("%s = %s", key, correlatedValueSQLLiteral(values[index])))
		}
		if valid {
			rows = append(rows, "("+strings.Join(terms, " and ")+")")
		}
	}
	if len(rows) == 0 {
		return "1 = 0"
	}
	return strings.Join(rows, " or ")
}

func correlatedDMLSchema(databaseName string, target correlatedDMLTarget) string {
	if target.schemaName != "" {
		return target.schemaName
	}
	return databaseName
}

func (e *XMySQLExecutor) correlatedDMLTableMetadata(ctx context.Context, schemaName, tableName string) (*metadata.TableMeta, error) {
	if e != nil {
		if manager, ok := e.tableManager.(interface {
			GetTableMetadata(context.Context, string, string) (*metadata.TableMeta, error)
		}); ok {
			if meta, err := manager.GetTableMetadata(ctx, schemaName, tableName); err == nil && meta != nil {
				return meta, nil
			}
		}
		if e.infosSchemaManager != nil {
			if meta, err := e.infosSchemaManager.GetTableMetadata(ctx, schemaName, tableName); err == nil && meta != nil {
				return meta, nil
			}
		}
		if meta, err := (&SelectExecutor{}).loadTableMetaFromFrm(e.getDataDir(), schemaName, tableName); err == nil && meta != nil {
			return meta, nil
		}
	}
	return nil, fmt.Errorf("metadata manager is unavailable")
}

type correlatedDMLTarget struct {
	schemaName string
	tableName  string
	alias      string
}

func parseCorrelatedDMLTarget(text string) (correlatedDMLTarget, bool) {
	pattern := regexp.MustCompile("(?is)^(`?[a-zA-Z0-9_$]+`?(?:\\s*\\.\\s*`?[a-zA-Z0-9_$]+`?)?)(?:\\s+(?:as\\s+)?(`?[a-zA-Z0-9_$]+`?))?$")
	match := pattern.FindStringSubmatch(strings.TrimSpace(text))
	if len(match) != 3 {
		return correlatedDMLTarget{}, false
	}
	qualified := strings.TrimSpace(match[1])
	parts := strings.Split(qualified, ".")
	if len(parts) > 2 {
		return correlatedDMLTarget{}, false
	}
	tableName := strings.Trim(strings.TrimSpace(parts[len(parts)-1]), "`")
	if tableName == "" {
		return correlatedDMLTarget{}, false
	}
	schemaName := ""
	if len(parts) == 2 {
		schemaName = strings.Trim(strings.TrimSpace(parts[0]), "`")
	}
	alias := strings.Trim(strings.TrimSpace(match[2]), "`")
	if alias == "" {
		alias = tableName
	}
	return correlatedDMLTarget{schemaName: schemaName, tableName: tableName, alias: alias}, true
}

func contextOrBackground(ctx *ExecutionContext) context.Context {
	if ctx != nil && ctx.Context != nil {
		return ctx.Context
	}
	return context.Background()
}

func ctxConfig(ctx *ExecutionContext) *conf.Cfg {
	if ctx == nil {
		return nil
	}
	return ctx.Cfg
}

func sessionFromContext(ctx *ExecutionContext) server.MySQLServerSession {
	if ctx == nil {
		return nil
	}
	return ctx.Session
}
