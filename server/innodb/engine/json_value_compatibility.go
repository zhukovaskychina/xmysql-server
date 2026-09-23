package engine

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

var jsonValueReturningPattern = regexp.MustCompile(`(?is)^(.*?)\s+returning\s+(signed|unsigned|decimal(?:\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\))?|char(?:\s*\(\s*\d+\s*\))?|date|datetime|time|json)(.*)$`)

var jsonValuePolicyPattern = regexp.MustCompile(`(?is)^(null|error|default\s+(.+?))\s+on\s+(empty|error)(?:\s+(.*))?$`)

// rewriteJSONValueReturningQuery lowers every JSON_VALUE RETURNING call in a
// query to the internal seven-argument function understood by the regular
// expression executor. Keeping this as a lexical rewrite lets JSON_VALUE be
// used in projections, predicates, ordering, and joins without teaching the
// legacy yacc grammar the full RETURNING policy syntax.
func rewriteJSONValueReturningQuery(query string) (string, error) {
	var rewritten strings.Builder
	cursor := 0
	for {
		start := findJSONValueCall(query, cursor)
		if start < 0 {
			rewritten.WriteString(query[cursor:])
			return rewritten.String(), nil
		}
		open := start + len("json_value")
		close := matchingParenthesis(query, open)
		if close < 0 {
			return "", fmt.Errorf("invalid JSON_VALUE expression")
		}
		rewritten.WriteString(query[cursor:start])
		call := query[start : close+1]
		arguments := splitTopLevelComma(query[open+1 : close])
		if len(arguments) != 2 {
			rewritten.WriteString(call)
			cursor = close + 1
			continue
		}
		returning := jsonValueReturningPattern.FindStringSubmatch(strings.TrimSpace(arguments[1]))
		if len(returning) != 4 {
			rewritten.WriteString(call)
			cursor = close + 1
			continue
		}
		compatCall, err := buildJSONValueCompatibilityCall(arguments[0], returning)
		if err != nil {
			return "", err
		}
		rewritten.WriteString(compatCall)
		cursor = close + 1
	}
}

func findJSONValueCall(query string, start int) int {
	lower := strings.ToLower(query)
	for i := start; i+len("json_value(") <= len(query); i++ {
		if query[i] == '\'' || query[i] == '"' || query[i] == '`' {
			quote := query[i]
			i++
			for i < len(query) {
				if query[i] == quote {
					if i+1 < len(query) && query[i+1] == quote {
						i += 2
						continue
					}
					break
				}
				i++
			}
			continue
		}
		if strings.HasPrefix(lower[i:], "--") || query[i] == '#' {
			for i < len(query) && query[i] != '\n' {
				i++
			}
			continue
		}
		if strings.HasPrefix(lower[i:], "/*") {
			if end := strings.Index(query[i+2:], "*/"); end >= 0 {
				i += end + 3
				continue
			}
			return -1
		}
		if strings.HasPrefix(lower[i:], "json_value(") && (i == 0 || !isSQLIdentifierByte(query[i-1])) {
			return i
		}
	}
	return -1
}

func isSQLIdentifierByte(value byte) bool {
	return value == '_' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '$'
}

func buildJSONValueCompatibilityCall(document string, returning []string) (string, error) {
	if len(returning) != 4 {
		return "", fmt.Errorf("unsupported JSON_VALUE RETURNING clause")
	}
	emptyMode, emptyDefault, errorMode, errorDefault, err := parseJSONValuePolicies(returning[3])
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"json_value_compat(%s, %s, %s, %s, %s, %s, %s)",
		document, returning[1], quoteJSONValueSQLLiteral(strings.TrimSpace(returning[2])),
		quoteJSONValueSQLLiteral(emptyMode), jsonValueDefaultExpression(emptyDefault),
		quoteJSONValueSQLLiteral(errorMode), jsonValueDefaultExpression(errorDefault),
	), nil
}

// executeRawJSONValueCompatibility handles the JSON_VALUE RETURNING forms
// that the generic parser does not yet accept. It deliberately stays narrow:
// the rewritten expression must be a standalone SELECT so arbitrary query
// tails are left for the regular parser and reported consistently.
func (e *XMySQLExecutor) executeRawJSONValueCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	lower := strings.ToLower(trimmed)
	const prefix = "select json_value("
	if !strings.HasPrefix(lower, prefix) {
		return false, nil
	}
	open := strings.Index(trimmed, "(")
	if open < 0 {
		return false, nil
	}
	close := matchingParenthesis(trimmed, open)
	if close != len(trimmed)-1 {
		return false, nil
	}
	arguments := splitTopLevelComma(trimmed[open+1 : close])
	if len(arguments) != 2 {
		return true, fmt.Errorf("JSON_VALUE RETURNING compatibility requires a document, path, and RETURNING type")
	}
	returning := jsonValueReturningPattern.FindStringSubmatch(strings.TrimSpace(arguments[1]))
	if len(returning) != 4 {
		return true, fmt.Errorf("unsupported JSON_VALUE RETURNING clause")
	}
	compatCall, err := buildJSONValueCompatibilityCall(arguments[0], returning)
	if err != nil {
		return true, err
	}
	rewritten := "select " + compatCall
	stmt, err := sqlparser.Parse(rewritten)
	if err != nil {
		return true, err
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok || !selectHasNoFrom(selectStmt) {
		return true, fmt.Errorf("JSON_VALUE RETURNING compatibility only supports a standalone SELECT")
	}
	selectResult, err := executeConstantSelectStatement(selectStmt)
	if err != nil {
		return true, err
	}
	ctx.Results <- &Result{
		ResultType: common.RESULT_TYPE_QUERY,
		Data:       selectResult,
		Message:    fmt.Sprintf("SELECT query executed successfully, %d rows returned", selectResult.RowCount),
	}
	return true, nil
}

func parseJSONValuePolicies(suffix string) (string, string, string, string, error) {
	emptyMode, emptyDefault := "NULL", "NULL"
	errorMode, errorDefault := "NULL", "NULL"
	remaining := strings.TrimSpace(suffix)
	for remaining != "" {
		match := jsonValuePolicyPattern.FindStringSubmatch(remaining)
		if len(match) != 5 {
			return "", "", "", "", fmt.Errorf("unsupported JSON_VALUE policy clause %q", remaining)
		}
		mode := strings.ToUpper(strings.TrimSpace(match[1]))
		defaultValue := "NULL"
		if strings.HasPrefix(mode, "DEFAULT ") {
			mode = "DEFAULT"
			defaultValue = strings.TrimSpace(match[2])
			if defaultValue == "" {
				return "", "", "", "", fmt.Errorf("JSON_VALUE DEFAULT requires a value")
			}
		}
		target := strings.ToLower(strings.TrimSpace(match[3]))
		if target == "empty" {
			if emptyMode != "NULL" || emptyDefault != "NULL" {
				return "", "", "", "", fmt.Errorf("duplicate JSON_VALUE ON EMPTY clause")
			}
			emptyMode, emptyDefault = mode, defaultValue
		} else {
			if errorMode != "NULL" || errorDefault != "NULL" {
				return "", "", "", "", fmt.Errorf("duplicate JSON_VALUE ON ERROR clause")
			}
			errorMode, errorDefault = mode, defaultValue
		}
		remaining = strings.TrimSpace(match[4])
	}
	return emptyMode, emptyDefault, errorMode, errorDefault, nil
}

func quoteJSONValueSQLLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func jsonValueDefaultExpression(value string) string {
	if strings.TrimSpace(value) == "" {
		return "NULL"
	}
	return strings.TrimSpace(value)
}
