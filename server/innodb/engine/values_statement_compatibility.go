package engine

import (
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// rewriteStandaloneValuesStatement routes MySQL's VALUES ROW constructor
// through the existing SELECT and set-operation executors.
func rewriteStandaloneValuesStatement(query string) (string, bool) {
	trimmed := strings.TrimSpace(query)
	if len(trimmed) <= len("VALUES") || !strings.EqualFold(trimmed[:len("VALUES")], "VALUES") || isSQLIdentifierPart(trimmed[len("VALUES")]) {
		return "", false
	}

	position := skipSQLTrivia(trimmed, len("VALUES"))
	rows := make([][]string, 0, 1)
	columnCount := -1
	for {
		if position+len("ROW") > len(trimmed) || !strings.EqualFold(trimmed[position:position+len("ROW")], "ROW") {
			return "", false
		}
		position += len("ROW")
		if position < len(trimmed) && isSQLIdentifierPart(trimmed[position]) {
			return "", false
		}
		position = skipSQLTrivia(trimmed, position)
		if position >= len(trimmed) || trimmed[position] != '(' {
			return "", false
		}
		close := findSQLClosingParenthesis(trimmed, position)
		if close < 0 {
			return "", false
		}
		values := splitTopLevelSQLValues(trimmed[position+1 : close])
		if len(values) == 0 || columnCount >= 0 && len(values) != columnCount {
			return "", false
		}
		columnCount = len(values)
		rows = append(rows, values)
		position = skipSQLTrivia(trimmed, close+1)
		if position >= len(trimmed) || trimmed[position] != ',' {
			break
		}
		position = skipSQLTrivia(trimmed, position+1)
	}

	tail := strings.TrimSpace(trimmed[position:])
	hasTerminator := strings.HasSuffix(tail, ";")
	if hasTerminator {
		tail = strings.TrimSpace(strings.TrimSuffix(tail, ";"))
	}
	if tail != "" && !startsWithSQLKeyword(tail, "ORDER") && !startsWithSQLKeyword(tail, "LIMIT") {
		return "", false
	}

	var statement strings.Builder
	for rowIndex, values := range rows {
		if rowIndex > 0 {
			statement.WriteString(" UNION ALL ")
		}
		statement.WriteString("SELECT ")
		for columnIndex, value := range values {
			if columnIndex > 0 {
				statement.WriteString(", ")
			}
			statement.WriteString(value)
			if rowIndex == 0 {
				statement.WriteString(" AS column_")
				statement.WriteString(strconv.Itoa(columnIndex))
			}
		}
	}
	if tail != "" {
		statement.WriteByte(' ')
		statement.WriteString(tail)
	}
	if hasTerminator {
		statement.WriteByte(';')
	}

	rewritten := statement.String()
	parsed, err := sqlparser.ParseForExecution(rewritten)
	if err != nil || !isStandaloneValuesSelect(parsed.AST) {
		return "", false
	}
	return rewritten, true
}

func isStandaloneValuesSelect(statement sqlparser.Statement) bool {
	switch node := statement.(type) {
	case *sqlparser.Select:
		return len(node.From) == 0 && node.Where == nil && len(node.GroupBy) == 0 && node.Having == nil && len(node.Windows) == 0 && node.Lock == "" && node.Distinct == "" && node.Cache == "" && node.Hints == ""
	case *sqlparser.Union:
		return isStandaloneValuesSelect(node.Left) && isStandaloneValuesSelect(node.Right) && node.Lock == ""
	default:
		return false
	}
}

func startsWithSQLKeyword(input, keyword string) bool {
	position := skipSQLTrivia(input, 0)
	if position+len(keyword) > len(input) || !strings.EqualFold(input[position:position+len(keyword)], keyword) {
		return false
	}
	return position+len(keyword) == len(input) || !isSQLIdentifierPart(input[position+len(keyword)])
}

func skipSQLTrivia(input string, position int) int {
	for position < len(input) {
		if isSQLWhitespace(input[position]) {
			position++
			continue
		}
		if position+1 < len(input) && input[position] == '/' && input[position+1] == '*' {
			close := strings.Index(input[position+2:], "*/")
			if close < 0 {
				return len(input)
			}
			position += close + 4
			continue
		}
		if input[position] == '#' || position+2 < len(input) && input[position:position+2] == "--" && (input[position+2] == ' ' || input[position+2] == '\t' || input[position+2] == '\r' || input[position+2] == '\n') {
			for position < len(input) && input[position] != '\n' && input[position] != '\r' {
				position++
			}
			continue
		}
		break
	}
	return position
}

func findSQLClosingParenthesis(input string, open int) int {
	depth := 0
	for position := open; position < len(input); {
		if next := skipSQLTrivia(input, position); next != position {
			position = next
			continue
		}
		ch := input[position]
		if ch == '\'' || ch == '"' || ch == '`' {
			quote := ch
			position++
			for position < len(input) {
				if input[position] == '\\' && quote != '`' && position+1 < len(input) {
					position += 2
					continue
				}
				if input[position] == quote {
					if position+1 < len(input) && input[position+1] == quote {
						position += 2
						continue
					}
					position++
					break
				}
				position++
			}
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return position
			}
		}
		position++
	}
	return -1
}

func splitTopLevelSQLValues(input string) []string {
	values := make([]string, 0, 1)
	start := 0
	depth := 0
	for position := 0; position < len(input); {
		if next := skipSQLTrivia(input, position); next != position {
			position = next
			continue
		}
		ch := input[position]
		if ch == '\'' || ch == '"' || ch == '`' {
			quote := ch
			position++
			for position < len(input) {
				if input[position] == '\\' && quote != '`' && position+1 < len(input) {
					position += 2
					continue
				}
				if input[position] == quote {
					if position+1 < len(input) && input[position+1] == quote {
						position += 2
						continue
					}
					position++
					break
				}
				position++
			}
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				if value := strings.TrimSpace(input[start:position]); value != "" {
					values = append(values, value)
				}
				start = position + 1
			}
		}
		position++
	}
	if value := strings.TrimSpace(input[start:]); value != "" {
		values = append(values, value)
	}
	return values
}
