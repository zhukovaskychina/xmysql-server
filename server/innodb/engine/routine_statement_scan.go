package engine

import (
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

// Routine statement forms below are read from the stored-program AST boundary
// with a tokenizer. They replace structural regular expressions. A statement
// that does not match is left for the caller to reject or dispatch.

func scanDeclareCondition(statement string) (name, sqlState, errno string, ok bool) {
	rest, ok := trimStatementPrefix(statement, "declare")
	if !ok {
		return "", "", "", false
	}
	name, rest = takeSQLIdent(rest)
	if name == "" {
		return "", "", "", false
	}
	rest, ok = trimStatementPrefix(rest, "condition")
	if !ok {
		return "", "", "", false
	}
	rest, ok = trimStatementPrefix(rest, "for")
	if !ok {
		return "", "", "", false
	}
	if next, matched := trimStatementPrefix(rest, "sqlstate"); matched {
		literal, tail, quoted := takeSingleQuoted(next)
		if !quoted || strings.TrimSpace(tail) != "" {
			return "", "", "", false
		}
		return name, literal, "", true
	}
	number, tail := takeDigits(rest)
	if number == "" || strings.TrimSpace(tail) != "" {
		return "", "", "", false
	}
	return name, "", number, true
}

func scanDiagnosticsCondition(statement string) (number, assignments string, ok bool) {
	rest, ok := trimStatementPrefix(statement, "get")
	if !ok {
		return "", "", false
	}
	if next, matched := trimStatementPrefix(rest, "current"); matched {
		rest = next
	}
	rest, ok = trimStatementPrefix(rest, "diagnostics")
	if !ok {
		return "", "", false
	}
	rest, ok = trimStatementPrefix(rest, "condition")
	if !ok {
		return "", "", false
	}
	number, rest = takeDigits(rest)
	if number == "" || strings.TrimSpace(rest) == "" {
		return "", "", false
	}
	return number, strings.TrimSpace(rest), true
}

func scanDiagnosticAssignment(statement string) (target, item string, ok bool) {
	statement = strings.TrimSpace(statement)
	parts := strings.SplitN(statement, "=", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	target = strings.TrimSpace(parts[0])
	item = strings.ToLower(strings.TrimSpace(parts[1]))
	if target == "" {
		return "", "", false
	}
	switch item {
	case common.RoutineDiagnosticReturnedSQLState, common.RoutineDiagnosticMessageText, common.RoutineDiagnosticMySQLErrno:
		return target, item, true
	default:
		return "", "", false
	}
}

func scanDeclareCursor(statement string) (name, query string, ok bool) {
	rest, ok := trimStatementPrefix(statement, "declare")
	if !ok {
		return "", "", false
	}
	name, rest = takeSQLIdent(rest)
	rest, ok = trimStatementPrefix(rest, "cursor")
	if !ok || name == "" {
		return "", "", false
	}
	rest, ok = trimStatementPrefix(rest, "for")
	if !ok || strings.TrimSpace(rest) == "" {
		return "", "", false
	}
	return name, strings.TrimSpace(rest), true
}

func scanSelectInto(statement string) (projection, targets, fromClause string, ok bool) {
	rest, ok := trimStatementPrefix(statement, "select")
	if !ok {
		return "", "", "", false
	}
	intoAt := topLevelKeyword(rest, "into")
	if intoAt < 0 {
		return "", "", "", false
	}
	fromAt := topLevelKeyword(rest[intoAt+len("into"):], "from")
	if fromAt < 0 {
		return "", "", "", false
	}
	fromAt += intoAt + len("into")
	projection = strings.TrimSpace(rest[:intoAt])
	targets = strings.TrimSpace(rest[intoAt+len("into") : fromAt])
	fromClause = strings.TrimSpace(rest[fromAt+len("from"):])
	if projection == "" || targets == "" || fromClause == "" {
		return "", "", "", false
	}
	return projection, targets, fromClause, true
}

func scanRoutineControl(statement string) (kind, label string, ok bool) {
	for _, candidate := range []string{routineControlLeave, routineControlIterate} {
		rest, matched := trimStatementPrefix(statement, candidate)
		if !matched {
			continue
		}
		label, tail := takeSQLIdent(rest)
		if label == "" || strings.TrimSpace(tail) != "" {
			return "", "", false
		}
		return candidate, label, true
	}
	return "", "", false
}

func scanLoopStatement(statement string) (label, body string, ok bool) {
	raw := strings.TrimSpace(statement)
	lower := strings.ToLower(raw)
	label = ""
	if colon := strings.Index(lower, ":"); colon > 0 {
		candidate := strings.TrimSpace(raw[:colon])
		ident, tail := takeSQLIdent(candidate)
		if ident != "" && strings.TrimSpace(tail) == "" {
			label = ident
			raw = strings.TrimSpace(raw[colon+1:])
			lower = strings.ToLower(raw)
		}
	}
	rest, matched := trimStatementPrefix(raw, "loop")
	if !matched {
		return "", "", false
	}
	return label, strings.TrimSpace(rest), true
}

func trimRepeatEnd(condition string) string {
	lower := strings.ToLower(condition)
	marker := "end repeat"
	for {
		at := strings.LastIndex(lower, marker)
		if at < 0 {
			return strings.TrimSpace(condition)
		}
		tail := strings.TrimSpace(lower[at+len(marker):])
		if tail != "" {
			return strings.TrimSpace(condition)
		}
		return strings.TrimSpace(condition[:at])
	}
}

func scanResignal(statement string) (assignments string, ok bool) {
	rest, matched := trimStatementPrefix(statement, "resignal")
	if !matched {
		return "", false
	}
	if strings.TrimSpace(rest) == "" {
		return "", true
	}
	rest, matched = trimStatementPrefix(rest, "set")
	if !matched {
		return "", false
	}
	return strings.TrimSpace(rest), true
}

func scanResignalMessage(assignment string) (message string, ok bool) {
	rest, matched := trimStatementPrefix(assignment, common.RoutineDiagnosticMessageText)
	if !matched {
		return "", false
	}
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "=") {
		return "", false
	}
	literal, tail, quoted := takeSingleQuoted(strings.TrimSpace(rest[1:]))
	if !quoted || strings.TrimSpace(tail) != "" {
		return "", false
	}
	return literal, true
}

func scanResignalErrno(assignment string) (errno string, ok bool) {
	rest, matched := trimStatementPrefix(assignment, "mysql_errno")
	if !matched {
		return "", false
	}
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "=") {
		return "", false
	}
	number, tail := takeDigits(strings.TrimSpace(rest[1:]))
	if number == "" || strings.TrimSpace(tail) != "" {
		return "", false
	}
	return number, true
}

func scanDeclareHandler(statement string) (action, conditions string, ok bool) {
	rest, matched := trimStatementPrefix(statement, "declare")
	if !matched {
		return "", "", false
	}
	action, rest = takeSQLIdent(rest)
	if !strings.EqualFold(action, common.RoutineHandlerContinue) && !strings.EqualFold(action, common.RoutineHandlerExit) {
		return "", "", false
	}
	rest, matched = trimStatementPrefix(rest, "handler")
	if !matched {
		return "", "", false
	}
	rest, matched = trimStatementPrefix(rest, "for")
	if !matched || strings.TrimSpace(rest) == "" {
		return "", "", false
	}
	return strings.ToLower(action), strings.TrimSpace(rest), true
}

func scanRoutineSet(statement string) (name, expression string, ok bool) {
	rest, matched := trimStatementPrefix(statement, "set")
	if !matched {
		return "", "", false
	}
	name, rest = takeSQLIdent(rest)
	rest = strings.TrimSpace(rest)
	if name == "" || !strings.HasPrefix(rest, "=") {
		return "", "", false
	}
	expression = strings.TrimSpace(rest[1:])
	if expression == "" {
		return "", "", false
	}
	return name, expression, true
}

func scanRoutineReturn(statement string) (expression string, ok bool) {
	rest, matched := trimStatementPrefix(statement, "return")
	if !matched || strings.TrimSpace(rest) == "" {
		return "", false
	}
	return strings.TrimSpace(rest), true
}

func identifierMentioned(sql, name string) bool {
	if name == "" {
		return false
	}
	lowerSQL := strings.ToLower(sql)
	lowerName := strings.ToLower(name)
	for start := 0; start+len(lowerName) <= len(lowerSQL); {
		at := strings.Index(lowerSQL[start:], lowerName)
		if at < 0 {
			return false
		}
		at += start
		beforeOK := at == 0 || !isSQLIdentByte(lowerSQL[at-1])
		after := at + len(lowerName)
		afterOK := after == len(lowerSQL) || !isSQLIdentByte(lowerSQL[after])
		if beforeOK && afterOK {
			return true
		}
		start = at + len(lowerName)
	}
	return false
}

func trimStatementPrefix(statement, keyword string) (string, bool) {
	statement = strings.TrimSpace(statement)
	if len(statement) < len(keyword) || !strings.EqualFold(statement[:len(keyword)], keyword) {
		return statement, false
	}
	if len(statement) == len(keyword) {
		return "", true
	}
	if !isSQLSpaceByte(statement[len(keyword)]) && statement[len(keyword)] != '(' {
		return statement, false
	}
	return strings.TrimSpace(statement[len(keyword):]), true
}

func takeSQLIdent(input string) (string, string) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", input
	}
	if input[0] == '`' {
		end := strings.IndexByte(input[1:], '`')
		if end < 0 {
			return "", input
		}
		return input[1 : 1+end], strings.TrimSpace(input[2+end:])
	}
	if input[0] == '@' {
		end := 1
		for end < len(input) && isSQLIdentByte(input[end]) {
			end++
		}
		return input[:end], strings.TrimSpace(input[end:])
	}
	end := 0
	for end < len(input) && isSQLIdentByte(input[end]) {
		end++
	}
	if end == 0 {
		return "", input
	}
	return input[:end], strings.TrimSpace(input[end:])
}

func takeDigits(input string) (string, string) {
	input = strings.TrimSpace(input)
	end := 0
	for end < len(input) && input[end] >= '0' && input[end] <= '9' {
		end++
	}
	if end == 0 {
		return "", input
	}
	return input[:end], strings.TrimSpace(input[end:])
}

func takeSingleQuoted(input string) (string, string, bool) {
	input = strings.TrimSpace(input)
	if input == "" || input[0] != '\'' {
		return "", input, false
	}
	for i := 1; i < len(input); i++ {
		if input[i] == '\'' {
			return input[1:i], input[i+1:], true
		}
	}
	return "", input, false
}

func topLevelKeyword(input, keyword string) int {
	lower := strings.ToLower(input)
	keyword = strings.ToLower(keyword)
	depth := 0
	var quote byte
	for i := 0; i+len(keyword) <= len(lower); i++ {
		ch := lower[i]
		if quote != 0 {
			if ch == quote {
				quote = 0
			}
			continue
		}
		switch ch {
		case '\'', '"', '`':
			quote = ch
			continue
		case '(':
			depth++
			continue
		case ')':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth != 0 || !strings.HasPrefix(lower[i:], keyword) {
			continue
		}
		if i > 0 && isSQLIdentByte(lower[i-1]) {
			continue
		}
		after := i + len(keyword)
		if after < len(lower) && isSQLIdentByte(lower[after]) {
			continue
		}
		return i
	}
	return -1
}

func isSQLIdentByte(value byte) bool {
	return value == '_' || value == '$' || (value >= '0' && value <= '9') || (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z')
}

func isSQLSpaceByte(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n'
}

func scanAccountQuotes(query string) [][2]string {
	var result [][2]string
	for i := 0; i < len(query); i++ {
		if query[i] != '\'' {
			continue
		}
		user, next, ok := takeSingleQuoted(query[i:])
		if !ok {
			continue
		}
		rest := strings.TrimSpace(next)
		if rest == "" || (rest[0] != '@' && !strings.HasPrefix(strings.ToLower(rest), "@")) {
			i += len(user) + 1
			continue
		}
		rest = strings.TrimSpace(rest[1:])
		host, _, quoted := takeSingleQuoted(rest)
		if !quoted {
			continue
		}
		result = append(result, [2]string{user, host})
		i += len(user) + 1
	}
	return result
}

func scanTablespaceName(query string) string {
	lower := strings.ToLower(query)
	for start := 0; start < len(lower); {
		at := strings.Index(lower[start:], "tablespace")
		if at < 0 {
			return ""
		}
		at += start
		if at > 0 && isSQLIdentByte(lower[at-1]) {
			start = at + len("tablespace")
			continue
		}
		rest := strings.TrimSpace(query[at+len("tablespace"):])
		if strings.HasPrefix(rest, "=") {
			rest = strings.TrimSpace(rest[1:])
		}
		if rest == "" {
			return ""
		}
		if rest[0] == '`' {
			end := strings.IndexByte(rest[1:], '`')
			if end < 0 {
				return ""
			}
			return rest[1 : 1+end]
		}
		ident, _ := takeSQLIdent(rest)
		return ident
	}
	return ""
}

func validTablespaceOptionText(options string) bool {
	rest := strings.TrimSpace(options)
	if rest == "" {
		return true
	}
	lower := strings.ToLower(rest)
	if strings.HasPrefix(lower, "add datafile ") {
		rest = strings.TrimSpace(rest[len("add datafile "):])
		_, tail, single := takeSingleQuoted(rest)
		if !single {
			if rest == "" || (rest[0] != '"') {
				return false
			}
			end := strings.IndexByte(rest[1:], '"')
			if end < 0 {
				return false
			}
			tail = rest[2+end:]
		}
		rest = strings.TrimSpace(tail)
		lower = strings.ToLower(rest)
	}
	if strings.HasPrefix(lower, "file_block_size") {
		rest = strings.TrimSpace(rest[len("file_block_size"):])
		if !strings.HasPrefix(rest, "=") {
			return false
		}
		number, tail := takeDigits(strings.TrimSpace(rest[1:]))
		if number == "" {
			return false
		}
		rest = tail
		lower = strings.ToLower(rest)
	}
	if strings.HasPrefix(lower, "engine") {
		rest = strings.TrimSpace(rest[len("engine"):])
		if strings.HasPrefix(rest, "=") {
			rest = strings.TrimSpace(rest[1:])
		}
		ident, tail := takeSQLIdent(rest)
		if ident == "" || strings.TrimSpace(tail) != "" {
			return false
		}
		return true
	}
	return rest == ""
}
