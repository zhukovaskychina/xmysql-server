package engine

import "strings"

// SQL structure scanners. These replace regular expressions that used to
// decide statement shape. Quoted text is not special-cased beyond identifier
// bytes, matching the previous word-boundary behavior.

func findExistsCall(sql string) (at int, notExists, found bool) {
	lower := strings.ToLower(sql)
	for start := 0; start < len(lower); {
		index := strings.Index(lower[start:], "exists")
		if index < 0 {
			return 0, false, false
		}
		index += start
		if index > 0 && isSQLIdentByte(lower[index-1]) {
			start = index + len("exists")
			continue
		}
		cursor := index + len("exists")
		for cursor < len(lower) && isSQLSpaceByte(lower[cursor]) {
			cursor++
		}
		if cursor >= len(lower) || lower[cursor] != '(' {
			start = index + len("exists")
			continue
		}
		before := strings.TrimRightFunc(lower[:index], isSQLSpaceRune)
		if strings.HasSuffix(before, "not") {
			notStart := len(before) - len("not")
			if (notStart == 0 || !isSQLIdentByte(lower[notStart-1])) && len(before) < index {
				return notStart, true, true
			}
		}
		return index, false, true
	}
	return 0, false, false
}

func findSpacedKeyword(sql, keyword string) (start, end int, found bool) {
	lower := strings.ToLower(sql)
	keyword = strings.ToLower(keyword)
	for i := 0; i < len(lower); i++ {
		if !isSQLSpaceByte(lower[i]) {
			continue
		}
		j := i
		for j < len(lower) && isSQLSpaceByte(lower[j]) {
			j++
		}
		if j+len(keyword) > len(lower) || lower[j:j+len(keyword)] != keyword {
			continue
		}
		after := j + len(keyword)
		if after < len(lower) && isSQLIdentByte(lower[after]) {
			continue
		}
		if after < len(lower) && !isSQLSpaceByte(lower[after]) {
			continue
		}
		if after < len(lower) && isSQLSpaceByte(lower[after]) {
			after++
		}
		return i, after, true
	}
	return 0, 0, false
}

func parseLeadingTableRef(source string, allowQualifier, allowAs bool) (table, alias string, ok bool) {
	source = strings.TrimSpace(source)
	table, rest := takeSQLIdent(source)
	if table == "" {
		return "", "", false
	}
	if allowQualifier {
		for {
			trimmed := strings.TrimSpace(rest)
			if !strings.HasPrefix(trimmed, ".") {
				rest = trimmed
				break
			}
			next, tail := takeSQLIdent(strings.TrimSpace(trimmed[1:]))
			if next == "" {
				break
			}
			table += "." + next
			rest = tail
		}
	}
	rest = strings.TrimSpace(rest)
	if allowAs {
		if next, matched := trimStatementPrefix(rest, "as"); matched {
			rest = next
		}
	}
	alias, _ = takeSQLIdent(rest)
	return table, alias, true
}

type predicateCall struct {
	start    int
	end      int
	left     string
	operator string
}

func findPredicateCall(sql string, anchored bool) (predicateCall, bool) {
	lower := strings.ToLower(sql)
	begin := 0
	if anchored {
		begin = len(sql) - len(strings.TrimLeft(sql, " \t\r\n"))
	}
	for i := begin; i < len(lower); i++ {
		if anchored && i > begin {
			break
		}
		if !isSQLIdentByte(lower[i]) && lower[i] != '.' && lower[i] != '`' {
			if anchored {
				break
			}
			continue
		}
		if i > 0 && (isSQLIdentByte(lower[i-1]) || lower[i-1] == '.' || lower[i-1] == '`') {
			continue
		}
		j := i
		for j < len(lower) && (isSQLIdentByte(lower[j]) || lower[j] == '.' || lower[j] == '`') {
			j++
		}
		if j == i {
			continue
		}
		op, opEnd, matched := matchPredicateOperator(sql[j:])
		if !matched {
			continue
		}
		after := strings.TrimLeft(sql[j+opEnd:], " \t\r\n")
		gap := len(sql[j+opEnd:]) - len(after)
		if !strings.HasPrefix(after, "(") {
			continue
		}
		return predicateCall{start: i, end: j + opEnd + gap + 1, left: strings.Trim(sql[i:j], "`"), operator: op}, true
	}
	return predicateCall{}, false
}

func matchPredicateOperator(rest string) (string, int, bool) {
	lower := strings.ToLower(rest)
	if strings.HasPrefix(lower, "not") && (len(lower) == 3 || !isSQLIdentByte(lower[3])) {
		tail := strings.TrimLeft(rest[3:], " \t\r\n")
		tailLower := strings.ToLower(tail)
		pad := 3 + (len(rest[3:]) - len(tail))
		if strings.HasPrefix(tailLower, "in") && (len(tailLower) == 2 || !isSQLIdentByte(tailLower[2])) {
			return rest[:pad+2], pad + 2, true
		}
	}
	if strings.HasPrefix(lower, "in") && (len(lower) == 2 || !isSQLIdentByte(lower[2])) {
		return rest[:2], 2, true
	}
	for _, op := range []string{"<=>", "<>", "!=", "<=", ">=", "=", "<", ">"} {
		if !strings.HasPrefix(rest, op) {
			continue
		}
		tail := strings.TrimLeft(rest[len(op):], " \t\r\n")
		tailLower := strings.ToLower(tail)
		pad := len(op) + (len(rest[len(op):]) - len(tail))
		for _, quant := range []string{"any", "some", "all"} {
			if strings.HasPrefix(tailLower, quant) && (len(tailLower) == len(quant) || !isSQLIdentByte(tailLower[len(quant)])) {
				return strings.TrimSpace(rest[:pad+len(quant)]), pad + len(quant), true
			}
		}
	}
	return "", 0, false
}

func findLeadingInCall(sql string) (start, end int, ok bool) {
	trimmed := strings.TrimSpace(sql)
	offset := len(sql) - len(trimmed)
	lower := strings.ToLower(trimmed)
	for i := 1; i < len(lower); i++ {
		if !isSQLSpaceByte(lower[i]) {
			continue
		}
		j := i
		for j < len(lower) && isSQLSpaceByte(lower[j]) {
			j++
		}
		_, opLen, matched := matchInOperator(lower[j:])
		if !matched {
			continue
		}
		after := strings.TrimLeft(lower[j+opLen:], " \t\r\n")
		if !strings.HasPrefix(after, "(") {
			continue
		}
		gap := len(lower[j+opLen:]) - len(after)
		return offset, offset + j + opLen + gap + 1, true
	}
	return 0, 0, false
}

func matchInOperator(rest string) (string, int, bool) {
	if strings.HasPrefix(rest, "not") && (len(rest) == 3 || !isSQLIdentByte(rest[3])) {
		tail := strings.TrimLeft(rest[3:], " \t\r\n")
		pad := 3 + len(rest[3:]) - len(tail)
		if strings.HasPrefix(tail, "in") && (len(tail) == 2 || !isSQLIdentByte(tail[2])) {
			return "not in", pad + 2, true
		}
	}
	if strings.HasPrefix(rest, "in") && (len(rest) == 2 || !isSQLIdentByte(rest[2])) {
		return "in", 2, true
	}
	return "", 0, false
}

func replaceQualifiedName(sql, qualifier, column, replacement string) string {
	if qualifier == "" || column == "" {
		return sql
	}
	lower := strings.ToLower(sql)
	q := strings.ToLower(qualifier)
	c := strings.ToLower(strings.Trim(column, "`"))
	var builder strings.Builder
	start := 0
	for start < len(lower) {
		at := strings.Index(lower[start:], q)
		if at < 0 {
			builder.WriteString(sql[start:])
			break
		}
		at += start
		if (at > 0 && isSQLIdentByte(lower[at-1])) || at+len(q) > len(lower) {
			builder.WriteString(sql[start : at+1])
			start = at + 1
			continue
		}
		cursor := at + len(q)
		if cursor < len(lower) && isSQLIdentByte(lower[cursor]) {
			builder.WriteString(sql[start : at+1])
			start = at + 1
			continue
		}
		for cursor < len(lower) && isSQLSpaceByte(lower[cursor]) {
			cursor++
		}
		if cursor >= len(lower) || lower[cursor] != '.' {
			builder.WriteString(sql[start : at+1])
			start = at + 1
			continue
		}
		cursor++
		for cursor < len(lower) && isSQLSpaceByte(lower[cursor]) {
			cursor++
		}
		if c != "" {
			if cursor+len(c) > len(lower) || lower[cursor:cursor+len(c)] != c || (cursor+len(c) < len(lower) && isSQLIdentByte(lower[cursor+len(c)])) {
				builder.WriteString(sql[start : at+1])
				start = at + 1
				continue
			}
			cursor += len(c)
		}
		builder.WriteString(sql[start:at])
		builder.WriteString(replacement)
		start = cursor
	}
	return builder.String()
}

func sqlHasQualifierDot(sql, qualifier string) bool {
	if qualifier == "" {
		return false
	}
	return replaceQualifiedName(sql, qualifier, "", "\x00") != sql
}

func findQualifiedPairs(sql string) [][2]string {
	lower := strings.ToLower(sql)
	pairs := make([][2]string, 0)
	for i := 0; i < len(lower); i++ {
		if !isSQLIdentByte(lower[i]) {
			continue
		}
		if i > 0 && isSQLIdentByte(lower[i-1]) {
			continue
		}
		j := i
		for j < len(lower) && isSQLIdentByte(lower[j]) {
			j++
		}
		cursor := j
		for cursor < len(lower) && isSQLSpaceByte(lower[cursor]) {
			cursor++
		}
		if cursor >= len(lower) || lower[cursor] != '.' {
			continue
		}
		cursor++
		for cursor < len(lower) && isSQLSpaceByte(lower[cursor]) {
			cursor++
		}
		end := cursor
		for end < len(lower) && isSQLIdentByte(lower[end]) {
			end++
		}
		if end == cursor {
			continue
		}
		pairs = append(pairs, [2]string{sql[i:j], sql[cursor:end]})
		i = end - 1
	}
	return pairs
}

func replaceSQLIdent(expression, oldName, newName string) string {
	oldName = strings.Trim(oldName, "`")
	if oldName == "" {
		return expression
	}
	lower := strings.ToLower(expression)
	needle := strings.ToLower(oldName)
	var builder strings.Builder
	start := 0
	for start < len(lower) {
		at := strings.Index(lower[start:], needle)
		if at < 0 {
			builder.WriteString(expression[start:])
			break
		}
		at += start
		after := at + len(needle)
		if (at > 0 && isSQLIdentByte(lower[at-1])) || (after < len(lower) && isSQLIdentByte(lower[after])) {
			builder.WriteString(expression[start : at+1])
			start = at + 1
			continue
		}
		builder.WriteString(expression[start:at])
		builder.WriteString(newName)
		start = after
	}
	return builder.String()
}

func isSQLSpaceRune(value rune) bool {
	return value == ' ' || value == '\t' || value == '\n' || value == '\r'
}

func scanAutoIncrementValue(sql string) (string, bool) {
	lower := strings.ToLower(sql)
	for start := 0; start < len(lower); {
		at := strings.Index(lower[start:], "auto_increment")
		if at < 0 {
			return "", false
		}
		at += start
		if at > 0 && isSQLIdentByte(lower[at-1]) {
			start = at + len("auto_increment")
			continue
		}
		rest := strings.TrimLeft(sql[at+len("auto_increment"):], " \t\r\n")
		if !strings.HasPrefix(rest, "=") {
			start = at + len("auto_increment")
			continue
		}
		number, tail := takeDigits(strings.TrimSpace(rest[1:]))
		if number == "" || (tail != "" && isSQLIdentByte(tail[0])) {
			start = at + len("auto_increment")
			continue
		}
		return number, true
	}
	return "", false
}

func scanGeneratedExpression(definition string) (string, bool) {
	definition = strings.TrimSpace(definition)
	lower := strings.ToLower(definition)
	at := strings.LastIndex(lower, " as")
	if at < 0 && strings.HasPrefix(lower, "as") {
		at = 0
	}
	if at < 0 {
		return "", false
	}
	if at > 0 && !isSQLSpaceByte(lower[at]) && !strings.HasPrefix(lower[at:], "as") {
		return "", false
	}
	rest := strings.TrimSpace(definition[at:])
	restLower := strings.ToLower(rest)
	if strings.HasPrefix(restLower, "as") && (len(restLower) == 2 || !isSQLIdentByte(restLower[2])) {
		rest = strings.TrimSpace(rest[2:])
	} else {
		return "", false
	}
	if !strings.HasPrefix(rest, "(") {
		return "", false
	}
	close := strings.LastIndex(rest, ")")
	if close < 0 {
		return "", false
	}
	tail := strings.TrimSpace(rest[close+1:])
	if tail != "" && !strings.EqualFold(tail, "stored") && !strings.EqualFold(tail, "virtual") {
		return "", false
	}
	return strings.TrimSpace(rest[1:close]), true
}

func scanTableOptionValue(options, keyPattern string) (string, bool) {
	lower := strings.ToLower(options)
	keys := tableOptionKeys(keyPattern)
	bestAt := -1
	bestKey := ""
	for _, key := range keys {
		for start := 0; start < len(lower); {
			at := strings.Index(lower[start:], key)
			if at < 0 {
				break
			}
			at += start
			if at > 0 && isSQLIdentByte(lower[at-1]) {
				start = at + 1
				continue
			}
			after := at + len(key)
			if after < len(lower) && isSQLIdentByte(lower[after]) {
				start = at + 1
				continue
			}
			if bestAt < 0 || at < bestAt {
				bestAt = at
				bestKey = key
			}
			break
		}
	}
	if bestAt < 0 {
		return "", false
	}
	rest := strings.TrimSpace(options[bestAt+len(bestKey):])
	if strings.HasPrefix(rest, "=") {
		rest = strings.TrimSpace(rest[1:])
	}
	value, _ := takeSQLIdent(rest)
	if value == "" {
		return "", false
	}
	return value, true
}

func tableOptionKeys(keyPattern string) []string {
	switch keyPattern {
	case `(?:default\s+)?(?:character\s+set|charset)`:
		return []string{"default character set", "default charset", "character set", "charset"}
	default:
		return []string{strings.ToLower(keyPattern)}
	}
}

func scanTableComment(options string) (string, bool) {
	lower := strings.ToLower(options)
	for start := 0; start < len(lower); {
		at := strings.Index(lower[start:], "comment")
		if at < 0 {
			return "", false
		}
		at += start
		if at > 0 && isSQLIdentByte(lower[at-1]) {
			start = at + len("comment")
			continue
		}
		rest := strings.TrimSpace(options[at+len("comment"):])
		if strings.HasPrefix(rest, "=") {
			rest = strings.TrimSpace(rest[1:])
		}
		if literal, _, quoted := takeSingleQuoted(rest); quoted {
			return strings.ReplaceAll(literal, "''", "'"), true
		}
		if strings.HasPrefix(rest, `"`) {
			end := strings.Index(rest[1:], `"`)
			if end >= 0 {
				return strings.ReplaceAll(rest[1:1+end], `""`, `"`), true
			}
		}
		start = at + len("comment")
	}
	return "", false
}
