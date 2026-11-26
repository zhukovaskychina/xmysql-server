package engine

import "strings"

// rewriteCreateTableConstructorSource adapts TABLE and VALUES sources in
// CREATE TABLE/VIEW ... AS statements to the existing SELECT source paths.
func rewriteCreateTableConstructorSource(query string) string {
	position := skipSQLTrivia(query, 0)
	first, position := scanSQLWord(query, position)
	if !strings.EqualFold(first, "CREATE") {
		return query
	}

	depth := 0
	hasTableOrView := false
	for position < len(query) {
		if next := skipSQLTrivia(query, position); next != position {
			position = next
			continue
		}
		ch := query[position]
		if ch == '\'' || ch == '"' || ch == '`' {
			position = skipSQLQuotedValue(query, position)
			continue
		}
		switch ch {
		case '(':
			depth++
			position++
			continue
		case ')':
			if depth > 0 {
				depth--
			}
			position++
			continue
		case ';':
			return query
		}
		if depth != 0 || !isSQLWordStart(ch) {
			position++
			continue
		}

		word, end := scanSQLWord(query, position)
		if strings.EqualFold(word, "TABLE") || strings.EqualFold(word, "VIEW") {
			hasTableOrView = true
		}
		if strings.EqualFold(word, "AS") && hasTableOrView {
			contentStart := skipSQLTrivia(query, end)
			if rewritten, ok := rewriteStandaloneValuesStatement(query[contentStart:]); ok {
				return query[:contentStart] + rewritten
			}
			if rewritten, ok := rewriteStandaloneTableStatement(query[contentStart:]); ok {
				return query[:contentStart] + rewritten
			}
			if rewritten, ok := rewriteTableConstructorsInSetQuery(query[contentStart:]); ok {
				return query[:contentStart] + rewritten
			}
			return query
		}
		position = end
	}
	return query
}

func scanSQLWord(input string, position int) (string, int) {
	start := position
	for position < len(input) && isSQLIdentifierPart(input[position]) {
		position++
	}
	return input[start:position], position
}

func isSQLWordStart(ch byte) bool {
	return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= 0x80
}
