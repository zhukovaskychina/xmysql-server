package engine

import "strings"

// rewriteParenthesizedTableConstructors converts TABLE and VALUES statements
// used as parenthesized query expressions before the regular SQL parser runs.
func rewriteParenthesizedTableConstructors(query string) string {
	for {
		rewritten, changed := rewriteFirstParenthesizedTableConstructor(query)
		if !changed {
			return query
		}
		query = rewritten
	}
}

func rewriteFirstParenthesizedTableConstructor(query string) (string, bool) {
	for position := 0; position < len(query); {
		if next := skipSQLTrivia(query, position); next != position {
			position = next
			continue
		}
		if query[position] == '\'' || query[position] == '"' || query[position] == '`' {
			position = skipSQLQuotedValue(query, position)
			continue
		}
		if query[position] != '(' {
			position++
			continue
		}

		close := findSQLClosingParenthesis(query, position)
		if close < 0 {
			position++
			continue
		}
		body := query[position+1 : close]
		leading := skipSQLTrivia(body, 0)
		candidate := body[leading:]
		rewritten, ok := rewriteStandaloneValuesStatement(candidate)
		if !ok {
			rewritten, ok = rewriteStandaloneTableStatement(candidate)
		}
		if !ok {
			position++
			continue
		}

		var result strings.Builder
		result.Grow(len(query) + len(rewritten) - len(candidate))
		result.WriteString(query[:position+1])
		result.WriteString(body[:leading])
		result.WriteString(rewritten)
		result.WriteString(query[close:])
		return result.String(), true
	}
	return query, false
}

func skipSQLQuotedValue(input string, position int) int {
	quote := input[position]
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
			return position + 1
		}
		position++
	}
	return len(input)
}
