package sqlparser

import (
	"fmt"
	"strings"
)

func parseExplainForExecution(query string) (*ExplainStatement, error) {
	trimmed := strings.TrimSpace(StripLeadingComments(query))
	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return nil, fmt.Errorf("%w: EXPLAIN statement is empty", ErrInvalidStatement)
	}
	kind := strings.ToLower(fields[0])
	if kind == "desc" {
		kind = "describe"
	}
	if kind != "describe" && kind != "explain" {
		return nil, fmt.Errorf("%w: %s", ErrInvalidStatement, kind)
	}
	statement := &ExplainStatement{Kind: kind}
	remainder := strings.TrimSpace(trimmed[len(fields[0]):])
	for {
		lowerRemainder := strings.ToLower(remainder)
		if kind == "explain" && strings.HasPrefix(lowerRemainder, "analyze ") {
			statement.Analyze = true
			remainder = strings.TrimSpace(remainder[len("analyze "):])
			continue
		}
		if kind == "explain" && strings.HasPrefix(lowerRemainder, "format=") {
			value := remainder[len("format="):]
			if space := strings.IndexAny(value, " \t\r\n"); space >= 0 {
				statement.ExplainFormat = strings.ToLower(value[:space])
				remainder = strings.TrimSpace(value[space:])
			} else {
				statement.ExplainFormat = strings.ToLower(value)
				remainder = ""
			}
			break
		}
		if kind == "explain" && strings.HasPrefix(lowerRemainder, "format ") {
			value := strings.TrimSpace(remainder[len("format "):])
			if space := strings.IndexAny(value, " \t\r\n"); space >= 0 {
				statement.ExplainFormat = strings.ToLower(value[:space])
				remainder = strings.TrimSpace(value[space:])
			} else {
				statement.ExplainFormat = strings.ToLower(value)
				remainder = ""
			}
		}
		break
	}
	if remainder == "" {
		return nil, fmt.Errorf("%w: EXPLAIN target is missing", ErrInvalidStatement)
	}
	if kind == "explain" && startsWithExplainQuery(remainder) {
		inner, err := ParseForExecution(remainder)
		if err != nil {
			return nil, err
		}
		statement.Statement = inner.AST
		return statement, nil
	}
	tokens, err := scanShowExecutionTokens(trimmed)
	if err != nil || len(tokens) < 2 {
		return nil, fmt.Errorf("%w: EXPLAIN target is invalid", ErrInvalidStatement)
	}
	table, next, ok := parseShowQualifiedName(tokens, 1)
	if !ok || next != len(tokens) {
		return nil, fmt.Errorf("%w: EXPLAIN target is invalid", ErrInvalidStatement)
	}
	statement.Table = table
	return statement, nil
}

func startsWithExplainQuery(query string) bool {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return false
	}
	first := strings.Fields(trimmed)[0]
	switch strings.ToLower(first) {
	case "select", "with", "insert", "update", "delete":
		return true
	default:
		return false
	}
}
