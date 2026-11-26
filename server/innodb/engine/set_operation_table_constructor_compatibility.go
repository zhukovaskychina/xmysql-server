package engine

import "strings"

// rewriteTableConstructorsInSetQuery lets the native set-operation executor
// consume MySQL TABLE and VALUES operands as ordinary SELECT branches.
func rewriteTableConstructorsInSetQuery(query string) (string, bool) {
	branches, operators, ok := splitSetOperationQuery(query)
	if !ok || len(branches) != len(operators)+1 {
		return "", false
	}

	changed := false
	for index, branch := range branches {
		if rewritten, ok := rewriteTableConstructorSetBranch(branch); ok {
			branches[index] = rewritten
			changed = true
		}
	}
	if !changed {
		return "", false
	}

	var rewritten strings.Builder
	for index, branch := range branches {
		if index > 0 {
			rewritten.WriteByte(' ')
			operator := operators[index-1]
			rewritten.WriteString(operator.kind)
			if operator.all {
				rewritten.WriteString(" ALL")
			}
			rewritten.WriteByte(' ')
		}
		rewritten.WriteString(branch)
	}
	return rewritten.String(), true
}

func rewriteTableConstructorSetBranch(branch string) (string, bool) {
	trimmed := strings.TrimSpace(branch)
	leading := skipSQLTrivia(trimmed, 0)
	prefix := trimmed[:leading]
	candidate := trimmed[leading:]
	wrappers := 0
	suffix := ""
	for len(candidate) > 1 && candidate[0] == '(' {
		close := findSQLClosingParenthesis(candidate, 0)
		if close < 0 || skipSQLTrivia(candidate, close+1) != len(candidate) {
			break
		}
		suffix = candidate[close+1:] + suffix
		candidate = candidate[1:close]
		wrappers++
	}

	rewritten, ok := rewriteStandaloneValuesStatement(candidate)
	if !ok {
		rewritten, ok = rewriteStandaloneTableStatement(candidate)
	}
	if !ok {
		return "", false
	}
	if wrappers > 0 {
		rewritten = strings.Repeat("(", wrappers) + rewritten + strings.Repeat(")", wrappers) + suffix
	}
	return prefix + rewritten, true
}
