package engine

import (
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// rewriteStandaloneTableStatement routes MySQL's TABLE query form through the
// existing SELECT executor. Keep the accepted clauses narrow: TABLE has no
// WHERE or projection list, and unsupported forms must remain parser errors.
func rewriteStandaloneTableStatement(query string) (string, bool) {
	trimmed := strings.TrimSpace(query)
	hasTerminator := strings.HasSuffix(trimmed, ";")
	if hasTerminator {
		trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
	}
	if len(trimmed) < len("TABLE ") || !strings.EqualFold(trimmed[:len("TABLE")], "TABLE") || !isSQLWhitespace(trimmed[len("TABLE")]) {
		return "", false
	}
	position := len("TABLE")
	for position < len(trimmed) && isSQLWhitespace(trimmed[position]) {
		position++
	}
	nameStart := position
	position, ok := scanTableStatementIdentifier(trimmed, position)
	if !ok {
		return "", false
	}
	if position < len(trimmed) && trimmed[position] == '.' {
		position++
		position, ok = scanTableStatementIdentifier(trimmed, position)
		if !ok || position < len(trimmed) && trimmed[position] == '.' {
			return "", false
		}
	}
	tableName := trimmed[nameStart:position]
	tail := strings.TrimSpace(trimmed[position:])

	statement := "SELECT * FROM " + tableName
	if tail != "" {
		statement += " " + tail
	}
	if hasTerminator {
		statement += ";"
	}
	parsed, err := sqlparser.ParseForExecution(statement)
	if err != nil {
		return "", false
	}
	selectStatement, ok := parsed.AST.(*sqlparser.Select)
	if !ok || len(selectStatement.From) != 1 || selectStatement.Where != nil || selectStatement.Having != nil || len(selectStatement.GroupBy) != 0 || len(selectStatement.Windows) != 0 || selectStatement.Lock != "" || selectStatement.Distinct != "" || selectStatement.Cache != "" || selectStatement.Hints != "" {
		return "", false
	}
	return statement, true
}

func scanTableStatementIdentifier(query string, position int) (int, bool) {
	if position >= len(query) {
		return position, false
	}
	if query[position] == '`' {
		position++
		for position < len(query) {
			if query[position] != '`' {
				position++
				continue
			}
			if position+1 < len(query) && query[position+1] == '`' {
				position += 2
				continue
			}
			return position + 1, position > 1
		}
		return position, false
	}
	start := position
	for position < len(query) {
		ch := query[position]
		if isSQLIdentifierPart(ch) || ch >= 0x80 {
			position++
			continue
		}
		break
	}
	return position, position > start
}
