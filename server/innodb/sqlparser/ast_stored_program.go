package sqlparser

import (
	"fmt"
	"strconv"
	"strings"
)

// StoredSignalStatement is the structured form of SIGNAL/RESIGNAL used by
// stored-program execution. Diagnostic names are kept as SQL names while
// values are decoded from their literal forms.
type StoredSignalStatement struct {
	Resignal       bool
	SQLState       string
	MessageText    string
	HasMessageText bool
	MysqlErrno     int
	HasMysqlErrno  bool
}

// ParseStoredProgramSignal parses SIGNAL/RESIGNAL without treating the
// statement as an opaque regular-expression match.
func ParseStoredProgramSignal(query string) (*StoredSignalStatement, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	fields := strings.Fields(trimmed)
	if len(fields) == 0 || (!strings.EqualFold(fields[0], "signal") && !strings.EqualFold(fields[0], "resignal")) {
		return nil, fmt.Errorf("stored signal statement expected")
	}
	statement := &StoredSignalStatement{Resignal: strings.EqualFold(fields[0], "resignal"), MysqlErrno: 1644}
	rest := strings.TrimSpace(trimmed[len(fields[0]):])
	if !statement.Resignal {
		lower := strings.ToLower(rest)
		if !strings.HasPrefix(lower, "sqlstate") {
			return nil, fmt.Errorf("SIGNAL requires SQLSTATE")
		}
		rest = strings.TrimSpace(rest[len("sqlstate"):])
		value, remaining, ok := consumeStoredSignalLiteral(rest)
		if !ok || len(value) == 0 {
			return nil, fmt.Errorf("SIGNAL SQLSTATE value is invalid")
		}
		statement.SQLState, rest = value, remaining
	}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return statement, nil
	}
	if !strings.HasPrefix(strings.ToLower(rest), "set") {
		return nil, fmt.Errorf("unsupported stored signal suffix")
	}
	assignments := splitStoredSignalAssignments(strings.TrimSpace(rest[len("set"):]))
	for _, assignment := range assignments {
		equal := strings.Index(assignment, "=")
		if equal <= 0 {
			return nil, fmt.Errorf("invalid stored signal diagnostic %q", assignment)
		}
		name := strings.ToLower(strings.Trim(strings.TrimSpace(assignment[:equal]), "`"))
		value := strings.TrimSpace(assignment[equal+1:])
		switch name {
		case "message_text", "mysql_errno":
		default:
			return nil, fmt.Errorf("unsupported stored signal diagnostic %q", name)
		}
		if name == "message_text" {
			decoded, ok := decodeStoredSignalLiteral(value)
			if !ok {
				return nil, fmt.Errorf("invalid MESSAGE_TEXT value")
			}
			statement.MessageText, statement.HasMessageText = decoded, true
			continue
		}
		number, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return nil, fmt.Errorf("invalid MYSQL_ERRNO value")
		}
		statement.MysqlErrno, statement.HasMysqlErrno = number, true
	}
	return statement, nil
}

func consumeStoredSignalLiteral(input string) (value, remainder string, ok bool) {
	input = strings.TrimSpace(input)
	if len(input) < 2 || (input[0] != '\'' && input[0] != '"') {
		return "", input, false
	}
	quote := input[0]
	for index := 1; index < len(input); index++ {
		if input[index] != quote {
			continue
		}
		if index+1 < len(input) && input[index+1] == quote {
			index++
			continue
		}
		return strings.ReplaceAll(strings.ReplaceAll(input[1:index], string(quote)+string(quote), string(quote)), "\\'", "'"), input[index+1:], true
	}
	return "", input, false
}

func decodeStoredSignalLiteral(value string) (string, bool) {
	decoded, remainder, ok := consumeStoredSignalLiteral(value)
	return decoded, ok && strings.TrimSpace(remainder) == ""
}

func splitStoredSignalAssignments(input string) []string {
	parts := make([]string, 0, 2)
	start, depth := 0, 0
	var quote byte
	for index := 0; index < len(input); index++ {
		ch := input[index]
		if quote != 0 {
			if ch == quote {
				if index+1 < len(input) && input[index+1] == quote {
					index++
				} else {
					quote = 0
				}
			}
			continue
		}
		switch ch {
		case '\'', '"':
			quote = ch
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(input[start:index]))
				start = index + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(input[start:]))
	return parts
}

type StoredProgramStatement struct {
	Kind             string
	Raw              string
	Name             string
	Label            string
	Condition        Expr
	AST              Statement
	Children         *StoredProgramBlock
	ElseBranch       *StoredProgramBlock
	HandlerAction    string
	HandlerCondition string
	ReturnExpr       string
}

type StoredProgramBlock struct {
	Statements []StoredProgramStatement
}

func (node *StoredProgramBlock) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	for index, statement := range node.Statements {
		if index > 0 {
			buf.Myprintf("; ")
		}
		buf.Myprintf("%s", statement.Raw)
	}
}

// ParseStoredProgramBlock performs SQL-aware statement splitting for routine
// bodies. It respects quotes and parenthesis nesting and leaves control-flow
// semantics to the existing routine executor.
func ParseStoredProgramBlock(body string) (*StoredProgramBlock, error) {
	parts := mergeStoredProgramBlocks(splitStoredProgramStatements(body))
	block := &StoredProgramBlock{Statements: make([]StoredProgramStatement, 0, len(parts))}
	for _, raw := range parts {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		statement, err := parseStoredProgramStatement(trimmed)
		if err != nil {
			return nil, err
		}
		block.Statements = append(block.Statements, statement)
	}
	return block, nil
}

// SplitStoredProgramStatements exposes the quote/parenthesis-aware leaf
// splitter for the legacy routine state machine. It deliberately does not
// merge control-flow END markers; that executor still needs the marker as a
// boundary while ParseStoredProgramBlock provides the nested AST view.
func SplitStoredProgramStatements(body string) []string { return splitStoredProgramStatements(body) }

func parseStoredProgramStatement(raw string) (StoredProgramStatement, error) {
	fields := strings.Fields(raw)
	if len(fields) == 0 {
		return StoredProgramStatement{}, nil
	}
	statement := StoredProgramStatement{Kind: strings.ToLower(fields[0]), Raw: raw}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(raw)), "for each row ") {
		// This is a trigger body marker, not a stored-program FOR loop. Do not
		// recursively parse the same body as a loop without a terminating END.
		statement.Kind = "trigger_body"
		return statement, nil
	}
	if len(fields) > 1 {
		statement.Name = strings.Trim(fields[1], "`:")
	}
	if statement.Kind == "begin" {
		inner := strings.TrimSpace(raw[len(fields[0]):])
		lowerInner := strings.ToLower(inner)
		if strings.HasSuffix(lowerInner, "end") {
			inner = strings.TrimSpace(inner[:len(inner)-len("end")])
		}
		if strings.TrimSpace(inner) != "" && strings.TrimSpace(inner) != strings.TrimSpace(raw) {
			children, err := ParseStoredProgramBlock(inner)
			if err != nil {
				return statement, err
			}
			statement.Children = children
		}
	}
	if statement.Kind == "if" || statement.Kind == "case" || statement.Kind == "while" || statement.Kind == "repeat" || statement.Kind == "loop" || statement.Kind == "for" {
		body := raw
		if marker := strings.Index(strings.ToLower(body), " then "); marker >= 0 {
			body = body[marker+6:]
		}
		if marker := strings.Index(strings.ToLower(body), " do "); marker >= 0 {
			body = body[marker+4:]
		}
		body = trimStoredProgramEnd(body, statement.Kind)
		if strings.TrimSpace(body) != strings.TrimSpace(raw) {
			if children, err := ParseStoredProgramBlock(body); err == nil && len(children.Statements) > 0 {
				statement.Children = children
			}
		}
	}
	if statement.Kind == "declare" && strings.Contains(strings.ToLower(raw), " handler ") {
		lower := strings.ToLower(raw)
		fields := strings.Fields(raw)
		if len(fields) > 1 {
			statement.HandlerAction = strings.ToUpper(fields[1])
		}
		if marker := strings.Index(lower, " for "); marker >= 0 {
			conditionFields := strings.Fields(strings.TrimSpace(raw[marker+5:]))
			if len(conditionFields) >= 2 && (strings.EqualFold(conditionFields[0], "not") || strings.EqualFold(conditionFields[0], "sqlstate")) {
				statement.HandlerCondition = strings.ToUpper(strings.Join(conditionFields[:2], " "))
			} else if len(conditionFields) > 0 {
				statement.HandlerCondition = strings.ToUpper(conditionFields[0])
			}
		}
	}
	if statement.Kind == "return" {
		statement.ReturnExpr = strings.TrimSpace(raw[len(fields[0]):])
	}
	if statement.Kind == "set" || statement.Kind == "return" || statement.Kind == "select" || statement.Kind == "insert" || statement.Kind == "update" || statement.Kind == "delete" {
		if parsed, err := ParseForExecution(raw); err == nil {
			statement.AST = parsed.AST
		}
	}
	return statement, nil
}

func trimStoredProgramEnd(body, kind string) string {
	lower := strings.ToLower(strings.TrimSpace(body))
	for _, suffix := range []string{"end if", "end case", "end while", "end repeat", "end loop", "end for"} {
		if strings.HasSuffix(lower, suffix) {
			return strings.TrimSpace(body[:len(body)-len(suffix)])
		}
	}
	return body
}

func mergeStoredProgramBlocks(parts []string) []string {
	merged := make([]string, 0, len(parts))
	var current []string
	depth := 0
	for _, part := range parts {
		lower := strings.ToLower(strings.TrimSpace(part))
		if depth == 0 && !storedProgramBlockStart(lower) {
			merged = append(merged, part)
			continue
		}
		current = append(current, part)
		if storedProgramBlockStart(lower) {
			depth++
		}
		if storedProgramBlockEnd(lower) {
			depth--
		}
		if depth == 0 {
			merged = append(merged, strings.Join(current, "; "))
			current = nil
		}
	}
	if len(current) > 0 {
		merged = append(merged, strings.Join(current, "; "))
	}
	return merged
}

func storedProgramBlockStart(lower string) bool {
	for _, prefix := range []string{"begin", "if ", "case ", "while ", "repeat", "loop", "for "} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

func storedProgramBlockEnd(lower string) bool {
	return lower == "end" || strings.HasPrefix(lower, "end ")
}

func splitStoredProgramStatements(body string) []string {
	statements := make([]string, 0, 2)
	start, depth := 0, 0
	var quote byte
	for index := 0; index < len(body); index++ {
		ch := body[index]
		if quote != 0 {
			if ch == quote && (index == 0 || body[index-1] != '\\') {
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
		case ';':
			if depth == 0 {
				if value := strings.TrimSpace(body[start:index]); value != "" {
					statements = append(statements, value)
				}
				start = index + 1
			}
		}
	}
	if value := strings.TrimSpace(body[start:]); value != "" {
		statements = append(statements, value)
	}
	return statements
}
