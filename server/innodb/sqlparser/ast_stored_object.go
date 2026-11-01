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
	ReturnType           string
	Definer              AccountName
	Security             string
	Deterministic        bool
	DataAccess           string
	Body                 string
	TriggerTiming        string
	TriggerEvent         string
	TargetTable          TableName
	Schedule             string
	ScheduleKind         string
	IntervalValue        int64
	IntervalUnit         string
	Starts               string
	Ends                 string
	OnCompletionPreserve bool
	Raw                  string
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
	return Walk(visit, node.Name)
}

func tryParseStoredObjectForExecution(query string) (*StoredObjectStatement, bool, error) {
	scanInput := query
	queryLower := strings.ToLower(query)
	cut := -1
	for _, marker := range []string{" begin ", " do ", " for each row ", " return "} {
		if position := strings.Index(queryLower, marker); position >= 0 && (cut < 0 || position < cut) {
			cut = position
		}
	}
	if cut >= 0 {
		scanInput = query[:cut]
	}
	tokens, err := scanShowExecutionTokens(scanInput)
	if err != nil || len(tokens) < 2 {
		return nil, false, err
	}
	kind := strings.ToLower(tokens[0].text)
	objectType := strings.ToLower(tokens[1].text)
	if kind == "call" {
		objectType = "call"
	} else if kind != "create" && kind != "alter" && kind != "drop" {
		return nil, false, nil
	} else if objectType != "procedure" && objectType != "function" && objectType != "trigger" && objectType != "event" {
		return nil, false, nil
	}
	nameIndex := 2
	if kind == "call" {
		nameIndex = 1
	}
	name, next, ok := parseShowQualifiedName(tokens, nameIndex)
	if !ok {
		return nil, true, fmt.Errorf("%w: stored object name missing", ErrInvalidStatement)
	}
	statement := &StoredObjectStatement{Kind: kind, ObjectType: objectType, Name: name, Raw: query}
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
	lower := strings.ToLower(query)
	for _, marker := range []string{" begin ", " return ", " do ", " for each row "} {
		if position := strings.Index(lower, marker); position >= 0 {
			statement.Body = strings.TrimSpace(query[position+1:])
			break
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
	}
	for tokenIndex := 0; tokenIndex < len(tokens); tokenIndex++ {
		value := strings.ToLower(tokens[tokenIndex].text)
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
			statement.Deterministic = true
		}
		if value == "contains" || value == "reads" || value == "modifies" || value == "no" {
			if tokenIndex+1 < len(tokens) && (strings.EqualFold(tokens[tokenIndex+1].text, "sql") || strings.EqualFold(tokens[tokenIndex+1].text, "sql data")) {
				statement.DataAccess = strings.ToUpper(value + " " + tokens[tokenIndex+1].text)
			}
		}
		if value == "returns" && tokenIndex+1 < len(tokens) {
			parts := make([]string, 0, 4)
			depth := 0
			for nextIndex := tokenIndex + 1; nextIndex < len(tokens); nextIndex++ {
				nextValue := strings.ToLower(tokens[nextIndex].text)
				if depth == 0 && (nextValue == "deterministic" || nextValue == "contains" || nextValue == "reads" || nextValue == "modifies" || nextValue == "no" || nextValue == "sql" || nextValue == "security" || nextValue == "begin" || nextValue == "return") {
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
		lower := strings.ToLower(query)
		if strings.Contains(lower, "on completion preserve") {
			statement.OnCompletionPreserve = true
		}
		if strings.Contains(lower, "on completion not preserve") {
			statement.OnCompletionPreserve = false
		}
		if start := strings.Index(lower, " on schedule "); start >= 0 {
			statement.Schedule = strings.TrimSpace(query[start+1:])
		}
		for tokenIndex := 0; tokenIndex+1 < len(tokens); tokenIndex++ {
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
			case "preserve":
				statement.OnCompletionPreserve = true
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
