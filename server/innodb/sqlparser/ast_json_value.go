package sqlparser

import (
	"fmt"
	"strings"
)

// JSONValueReturningClause is the structured tail of a JSON_VALUE call that
// the base yacc grammar does not currently accept. Path is kept as SQL text so
// it can be inserted back into the surrounding expression AST rewrite.
type JSONValueReturningClause struct {
	Path          string
	ReturningType string
	Policies      string
}

// JSONValuePolicy is one ON EMPTY or ON ERROR policy attached to JSON_VALUE.
type JSONValuePolicy struct {
	Mode    string
	Default string
	Target  string
}

// ParseJSONValueReturningClause parses the structured RETURNING tail without
// using a regular expression. It recognizes the MySQL JSON_VALUE return types
// supported by the compatibility executor and preserves the policy suffix for
// the policy parser below.
func ParseJSONValueReturningClause(raw string) (JSONValueReturningClause, error) {
	index := jsonValueTopLevelKeyword(raw, "returning")
	if index < 0 {
		return JSONValueReturningClause{}, fmt.Errorf("JSON_VALUE RETURNING clause is missing RETURNING")
	}
	path := strings.TrimSpace(raw[:index])
	if path == "" {
		return JSONValueReturningClause{}, fmt.Errorf("JSON_VALUE RETURNING clause requires a path")
	}
	remainder := strings.TrimSpace(raw[index+len("returning"):])
	returnType, end, err := jsonValueReturnType(remainder)
	if err != nil {
		return JSONValueReturningClause{}, err
	}
	return JSONValueReturningClause{
		Path:          path,
		ReturningType: returnType,
		Policies:      strings.TrimSpace(remainder[end:]),
	}, nil
}

// ParseJSONValuePolicies parses a sequence of ON EMPTY/ON ERROR clauses. The
// default expression ends at the next top-level ON keyword, so quoted strings
// and function arguments remain intact.
func ParseJSONValuePolicies(raw string) ([]JSONValuePolicy, error) {
	remaining := strings.TrimSpace(raw)
	policies := make([]JSONValuePolicy, 0, 2)
	for remaining != "" {
		onIndex := jsonValueTopLevelKeyword(remaining, "on")
		if onIndex < 0 {
			return nil, fmt.Errorf("JSON_VALUE policy clause %q is missing ON", remaining)
		}
		modeText := strings.TrimSpace(remaining[:onIndex])
		targetText := strings.TrimSpace(remaining[onIndex+len("on"):])
		target, targetEnd := jsonValueLeadingWord(targetText)
		if targetEnd == 0 || (target != "empty" && target != "error") {
			return nil, fmt.Errorf("unsupported JSON_VALUE policy clause %q", remaining)
		}
		mode, defaultValue, err := jsonValuePolicyMode(modeText)
		if err != nil {
			return nil, err
		}
		policies = append(policies, JSONValuePolicy{Mode: mode, Default: defaultValue, Target: target})
		remaining = strings.TrimSpace(targetText[targetEnd:])
	}
	return policies, nil
}

func jsonValueReturnType(raw string) (string, int, error) {
	word, end := jsonValueLeadingWord(raw)
	if end == 0 {
		return "", 0, fmt.Errorf("JSON_VALUE RETURNING type is missing")
	}
	lower := strings.ToLower(word)
	if lower != "signed" && lower != "unsigned" && lower != "decimal" && lower != "char" && lower != "date" && lower != "datetime" && lower != "time" && lower != "json" {
		return "", 0, fmt.Errorf("unsupported JSON_VALUE RETURNING type %q", word)
	}
	if lower == "decimal" || lower == "char" {
		space := end
		for space < len(raw) && (raw[space] == ' ' || raw[space] == '\t' || raw[space] == '\r' || raw[space] == '\n') {
			space++
		}
		if space < len(raw) && raw[space] == '(' {
			close := jsonValueMatchingParen(raw, space)
			if close < 0 {
				return "", 0, fmt.Errorf("unterminated JSON_VALUE RETURNING type %q", word)
			}
			end = close + 1
		}
	}
	return strings.TrimSpace(raw[:end]), end, nil
}

func jsonValuePolicyMode(raw string) (string, string, error) {
	trimmed := strings.TrimSpace(raw)
	word, end := jsonValueLeadingWord(trimmed)
	if end == 0 {
		return "", "", fmt.Errorf("JSON_VALUE policy mode is missing")
	}
	switch strings.ToLower(word) {
	case "null", "error":
		if strings.TrimSpace(trimmed[end:]) != "" {
			return "", "", fmt.Errorf("invalid JSON_VALUE policy mode %q", raw)
		}
		return strings.ToUpper(word), "NULL", nil
	case "default":
		value := strings.TrimSpace(trimmed[end:])
		if value == "" {
			return "", "", fmt.Errorf("JSON_VALUE DEFAULT requires a value")
		}
		return "DEFAULT", value, nil
	default:
		return "", "", fmt.Errorf("unsupported JSON_VALUE policy mode %q", word)
	}
}

func jsonValueLeadingWord(raw string) (string, int) {
	start := 0
	for start < len(raw) && (raw[start] == ' ' || raw[start] == '\t' || raw[start] == '\r' || raw[start] == '\n') {
		start++
	}
	end := start
	for end < len(raw) && jsonValueIdentifierByte(raw[end]) {
		end++
	}
	return raw[start:end], end
}

func jsonValueTopLevelKeyword(raw, keyword string) int {
	depth := 0
	var quote byte
	for index := 0; index+len(keyword) <= len(raw); index++ {
		ch := raw[index]
		if quote != 0 {
			if ch == quote {
				if index+1 < len(raw) && raw[index+1] == quote {
					index++
					continue
				}
				quote = 0
			}
			if ch == '\\' && quote == '\'' {
				index++
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
		}
		if depth == 0 && strings.EqualFold(raw[index:index+len(keyword)], keyword) &&
			(index == 0 || !jsonValueIdentifierByte(raw[index-1])) &&
			(index+len(keyword) == len(raw) || !jsonValueIdentifierByte(raw[index+len(keyword)])) {
			return index
		}
	}
	return -1
}

func jsonValueMatchingParen(raw string, open int) int {
	depth := 0
	var quote byte
	for index := open; index < len(raw); index++ {
		ch := raw[index]
		if quote != 0 {
			if ch == quote {
				if index+1 < len(raw) && raw[index+1] == quote {
					index++
					continue
				}
				quote = 0
			}
			continue
		}
		switch ch {
		case '\'', '"':
			quote = ch
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return index
			}
		}
	}
	return -1
}

func jsonValueIdentifierByte(value byte) bool {
	return value == '_' || value == '$' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
}
