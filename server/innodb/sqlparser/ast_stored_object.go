package sqlparser

import (
	"fmt"
	"strconv"
	"strings"
)

type StoredObjectStatement struct {
	Kind                 string
	ObjectType           string
	Name                 TableName
	Parameters           []string
	Arguments            []Expr
	ReturnType           string
	Definer              AccountName
	Security             string
	Deterministic        bool
	DeterministicSet     bool
	DataAccess           string
	Body                 string
	TriggerTiming        string
	TriggerEvent         string
	TriggerOrderKind     string
	TriggerOrderName     string
	TargetTable          TableName
	Schedule             string
	ScheduleKind         string
	IntervalValue        int64
	IntervalUnit         string
	Starts               string
	Ends                 string
	OnCompletionPreserve bool
	Comment              string
	CommentSet           bool
	EventRenameTo        TableName
	EventEnabled         bool
	EventEnabledSet      bool
	IfExists             bool
	IfNotExists          bool
	OrReplace            bool
	Raw                  string
	Options              string
}

func (node *StoredObjectStatement) iStatement() {}

func (node *StoredObjectStatement) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	buf.Myprintf("%s %s %v", node.Kind, node.ObjectType, node.Name)
	if node.Body != "" {
		buf.Myprintf(" %s", node.Body)
	}
}

func (node *StoredObjectStatement) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	if err := Walk(visit, node.Name); err != nil {
		return err
	}
	for _, argument := range node.Arguments {
		if err := Walk(visit, argument); err != nil {
			return err
		}
	}
	return nil
}

func tryParseStoredObjectForExecution(query string) (*StoredObjectStatement, bool, error) {
	tokens, body, err := scanStoredObjectHeader(query)
	if err != nil || len(tokens) < 2 {
		return nil, false, err
	}
	kind := strings.ToLower(tokens[0].text)
	index := 1
	orReplace := false
	if kind == "create" && index+1 < len(tokens) && strings.EqualFold(tokens[index].text, "or") && strings.EqualFold(tokens[index+1].text, "replace") {
		orReplace = true
		index += 2
	}
	definer := AccountName{}
	if index+1 < len(tokens) && strings.EqualFold(tokens[index].text, "definer") && tokens[index+1].text == "=" {
		account, next, ok := parseAccountNameTokens(tokens, index+2)
		if !ok {
			return nil, false, nil
		}
		definer, index = account, next
	}
	if index >= len(tokens) {
		return nil, false, nil
	}
	objectType := strings.ToLower(tokens[index].text)
	if kind == "call" {
		objectType = "call"
	} else if kind != "create" && kind != "alter" && kind != "drop" {
		return nil, false, nil
	} else if objectType != "procedure" && objectType != "function" && objectType != "trigger" && objectType != "event" {
		return nil, false, nil
	}
	nameIndex := index + 1
	if kind == "call" {
		nameIndex = 1
	}
	ifExists, ifNotExists := false, false
	if nameIndex < len(tokens) && strings.EqualFold(tokens[nameIndex].text, "if") {
		nameIndex++
		if nameIndex < len(tokens) && strings.EqualFold(tokens[nameIndex].text, "not") {
			ifNotExists = true
			nameIndex++
		} else {
			ifExists = true
		}
		if nameIndex >= len(tokens) || !strings.EqualFold(tokens[nameIndex].text, "exists") {
			return nil, true, fmt.Errorf("%w: EXISTS expected", ErrInvalidStatement)
		}
		nameIndex++
	}
	name, next, ok := parseShowQualifiedName(tokens, nameIndex)
	if !ok {
		return nil, true, fmt.Errorf("%w: stored object name missing", ErrInvalidStatement)
	}
	statement := &StoredObjectStatement{Kind: kind, ObjectType: objectType, Name: name, Raw: query, Body: body, Definer: definer, OrReplace: orReplace, IfExists: ifExists, IfNotExists: ifNotExists}
	if kind == "alter" && next < len(tokens) && tokens[next].start >= 0 && tokens[next].start < len(query) {
		statement.Options = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query[tokens[next].start:]), ";"))
	}
	if objectType == "trigger" && strings.TrimSpace(body) != "" {
		bodyFields := strings.Fields(body)
		for tokenIndex := 0; tokenIndex+1 < len(bodyFields); tokenIndex++ {
			if strings.EqualFold(bodyFields[tokenIndex], "follows") || strings.EqualFold(bodyFields[tokenIndex], "precedes") {
				statement.TriggerOrderKind = strings.ToLower(bodyFields[tokenIndex])
				statement.TriggerOrderName = strings.Trim(bodyFields[tokenIndex+1], "` ;")
				break
			}
		}
	}
	if kind == "call" {
		if next == len(tokens) {
			statement.Body = query
			return statement, true, nil
		}
		if tokens[next].text != "(" || tokens[len(tokens)-1].text != ")" {
			return nil, true, fmt.Errorf("%w: invalid CALL arguments", ErrInvalidStatement)
		}
		argumentText := strings.TrimSpace(query[tokens[next].end:tokens[len(tokens)-1].start])
		if argumentText != "" {
			arguments, err := ParseForExecution("select " + argumentText + " from dual")
			if err != nil {
				return nil, true, fmt.Errorf("%w: CALL arguments: %v", ErrInvalidStatement, err)
			}
			selectStmt, ok := arguments.AST.(*Select)
			if !ok {
				return nil, true, fmt.Errorf("%w: CALL arguments require scalar expressions", ErrInvalidStatement)
			}
			for _, item := range selectStmt.SelectExprs {
				aliased, ok := item.(*AliasedExpr)
				if !ok || !aliased.As.IsEmpty() {
					return nil, true, fmt.Errorf("%w: invalid CALL argument", ErrInvalidStatement)
				}
				statement.Arguments = append(statement.Arguments, aliased.Expr)
			}
		}
		statement.Body = query
		return statement, true, nil
	}
	if kind == "drop" && next != len(tokens) {
		return nil, true, fmt.Errorf("%w: unexpected stored object DROP suffix", ErrInvalidStatement)
	}
	if next < len(tokens) && tokens[next].text == "(" {
		depth := 1
		parameterTokens := make([]string, 0, 4)
		for next+1 < len(tokens) && depth > 0 {
			next++
			switch tokens[next].text {
			case "(":
				depth++
				if depth > 1 {
					parameterTokens = append(parameterTokens, tokens[next].text)
				}
			case ")":
				depth--
				if depth > 0 {
					parameterTokens = append(parameterTokens, tokens[next].text)
				}
				if depth == 0 && len(parameterTokens) > 0 {
					statement.Parameters = append(statement.Parameters, strings.Join(parameterTokens, " "))
					parameterTokens = parameterTokens[:0]
				}
			case ",":
				if depth == 1 && len(parameterTokens) > 0 {
					statement.Parameters = append(statement.Parameters, strings.Join(parameterTokens, " "))
					parameterTokens = parameterTokens[:0]
				} else if depth > 1 {
					parameterTokens = append(parameterTokens, tokens[next].text)
				}
			default:
				parameterTokens = append(parameterTokens, tokens[next].text)
			}
		}
	}
	if objectType == "trigger" {
		for tokenIndex := next; tokenIndex+2 < len(tokens); tokenIndex++ {
			if strings.EqualFold(tokens[tokenIndex].text, "before") || strings.EqualFold(tokens[tokenIndex].text, "after") {
				statement.TriggerTiming = strings.ToUpper(tokens[tokenIndex].text)
				statement.TriggerEvent = strings.ToUpper(tokens[tokenIndex+1].text)
				if strings.EqualFold(tokens[tokenIndex+2].text, "on") {
					if table, _, ok := parseShowQualifiedName(tokens, tokenIndex+3); ok {
						statement.TargetTable = table
					}
				}
				break
			}
		}
		for tokenIndex := next; tokenIndex+1 < len(tokens); tokenIndex++ {
			if strings.EqualFold(tokens[tokenIndex].text, "follows") || strings.EqualFold(tokens[tokenIndex].text, "precedes") {
				statement.TriggerOrderKind = strings.ToLower(tokens[tokenIndex].text)
				statement.TriggerOrderName = strings.Trim(tokens[tokenIndex+1].text, "` ")
				break
			}
		}
	}
	for tokenIndex := 0; tokenIndex < len(tokens); tokenIndex++ {
		if tokens[tokenIndex].kind == STRING {
			continue
		}
		value := strings.ToLower(tokens[tokenIndex].text)
		if value == "comment" && tokenIndex+1 < len(tokens) && tokens[tokenIndex+1].kind == STRING {
			statement.Comment = tokens[tokenIndex+1].text
			statement.CommentSet = true
			tokenIndex++
			continue
		}
		if value == "definer" && tokenIndex+2 < len(tokens) && tokens[tokenIndex+1].text == "=" {
			if account, nextAccount, ok := parseAccountNameTokens(tokens, tokenIndex+2); ok {
				statement.Definer = account
				tokenIndex = nextAccount - 1
			}
		}
		if value == "security" && tokenIndex+1 < len(tokens) {
			statement.Security = strings.ToUpper(tokens[tokenIndex+1].text)
			tokenIndex++
		}
		if value == "deterministic" {
			statement.Deterministic = tokenIndex == 0 || !strings.EqualFold(tokens[tokenIndex-1].text, "not")
			statement.DeterministicSet = true
		}
		if value == "contains" || value == "reads" || value == "modifies" || value == "no" {
			if tokenIndex+1 < len(tokens) && (strings.EqualFold(tokens[tokenIndex+1].text, "sql") || strings.EqualFold(tokens[tokenIndex+1].text, "sql data")) {
				statement.DataAccess = strings.ToUpper(value + " " + tokens[tokenIndex+1].text)
				if tokenIndex+2 < len(tokens) && strings.EqualFold(tokens[tokenIndex+2].text, "data") {
					statement.DataAccess += " DATA"
				}
			}
		}
		if value == "returns" && tokenIndex+1 < len(tokens) {
			parts := make([]string, 0, 4)
			depth := 0
			for nextIndex := tokenIndex + 1; nextIndex < len(tokens); nextIndex++ {
				nextValue := strings.ToLower(tokens[nextIndex].text)
				if depth == 0 && (nextValue == "not" || nextValue == "comment" || nextValue == "deterministic" || nextValue == "contains" || nextValue == "reads" || nextValue == "modifies" || nextValue == "no" || nextValue == "sql" || nextValue == "security" || nextValue == "begin" || nextValue == "return") {
					break
				}
				parts = append(parts, tokens[nextIndex].text)
				switch tokens[nextIndex].text {
				case "(":
					depth++
				case ")":
					if depth > 0 {
						depth--
					}
				}
				tokenIndex = nextIndex
			}
			statement.ReturnType = normalizeStoredTypeTokens(parts)
		}
	}
	if objectType == "event" {
		for tokenIndex := 0; tokenIndex+1 < len(tokens); tokenIndex++ {
			if tokens[tokenIndex].kind == STRING {
				continue
			}
			switch strings.ToLower(tokens[tokenIndex].text) {
			case "at":
				statement.ScheduleKind = "AT"
				statement.Starts = tokens[tokenIndex+1].text
			case "every":
				statement.ScheduleKind = "EVERY"
				if value, parseErr := strconv.ParseInt(tokens[tokenIndex+1].text, 10, 64); parseErr == nil {
					statement.IntervalValue = value
				}
				if tokenIndex+2 < len(tokens) {
					statement.IntervalUnit = strings.ToUpper(tokens[tokenIndex+2].text)
				}
			case "starts":
				statement.Starts = tokens[tokenIndex+1].text
			case "ends":
				statement.Ends = tokens[tokenIndex+1].text
			case "completion":
				statement.OnCompletionPreserve = strings.EqualFold(tokens[tokenIndex+1].text, "preserve")
			}
		}
		for tokenIndex := next; tokenIndex < len(tokens); tokenIndex++ {
			value := strings.ToLower(tokens[tokenIndex].text)
			if value == "rename" && tokenIndex+1 < len(tokens) && strings.EqualFold(tokens[tokenIndex+1].text, "to") {
				if target, _, ok := parseShowQualifiedName(tokens, tokenIndex+2); ok {
					statement.EventRenameTo = target
				}
			}
			if value == "enable" || value == "disable" {
				statement.EventEnabledSet = true
				statement.EventEnabled = value == "enable"
			}
		}
	}
	if kind == "call" && statement.Body == "" {
		statement.Body = strings.TrimSpace(query)
	}
	return statement, true, nil
}

func normalizeStoredTypeTokens(tokens []string) string {
	value := strings.TrimSpace(strings.Join(tokens, " "))
	value = strings.ReplaceAll(value, " (", "(")
	value = strings.ReplaceAll(value, "( ", "(")
	value = strings.ReplaceAll(value, " ,", ",")
	value = strings.ReplaceAll(value, ", ", ",")
	value = strings.ReplaceAll(value, " )", ")")
	return value
}

// scanStoredObjectHeader uses token kinds and byte offsets so keywords inside
// quoted attributes cannot terminate a definition header. The executable body
// stays verbatim, including its internal semicolons, for the stored-program AST.
func scanStoredObjectHeader(query string) ([]showExecutionToken, string, error) {
	tokenizer := NewStringTokenizer(query)
	tokens := make([]showExecutionToken, 0, 32)
	starts := make([]int, 0, 32)
	depth := 0
	terminated := false
	for {
		if tokenizer.lastChar == 0 {
			tokenizer.next()
		}
		tokenizer.skipBlank()
		start := tokenizer.Position - 1
		kind, value := tokenizer.Scan()
		if kind == 0 {
			break
		}
		if kind == COMMENT {
			continue
		}
		if kind == ';' {
			terminated = true
			continue
		}
		if terminated {
			return nil, "", fmt.Errorf("%w: token after statement terminator", ErrInvalidStatement)
		}
		text := string(value)
		if text == "" {
			text = KeywordString(kind)
		}
		if text == "" && kind < 256 {
			text = string(rune(kind))
		}
		keyword := kind != STRING && kind != ID
		// Some stored-program keywords are returned as ID by the legacy
		// lexer. A quoted identifier remains distinguishable in the source.
		if kind == ID && start >= 0 && start < len(query) && query[start] != '`' {
			keyword = true
		}
		if depth == 0 && keyword {
			switch strings.ToLower(text) {
			case "begin", "return", "do":
				return tokens, strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query[start:]), ";")), nil
			}
			if strings.EqualFold(text, "row") && len(tokens) >= 2 && strings.EqualFold(tokens[len(tokens)-2].text, "for") && strings.EqualFold(tokens[len(tokens)-1].text, "each") {
				bodyStart := starts[len(tokens)-2]
				return tokens[:len(tokens)-2], strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query[bodyStart:]), ";")), nil
			}
		}
		if kind == '(' {
			depth++
		}
		if kind == ')' {
			depth--
		}
		end := tokenizer.Position - 1
		if end > len(query) {
			end = len(query)
		}
		tokens = append(tokens, showExecutionToken{kind: kind, text: text, start: start, end: end})
		starts = append(starts, start)
	}
	if tokenizer.LastError != nil {
		return nil, "", fmt.Errorf("%w: %v", ErrInvalidStatement, tokenizer.LastError)
	}
	return tokens, "", nil
}
