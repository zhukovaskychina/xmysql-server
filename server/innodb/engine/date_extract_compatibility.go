package engine

import (
	"fmt"
	"regexp"
	"strings"
)

var supportedExtractDateUnits = map[string]struct{}{
	"YEAR":        {},
	"QUARTER":     {},
	"MONTH":       {},
	"WEEK":        {},
	"DAY":         {},
	"HOUR":        {},
	"MINUTE":      {},
	"SECOND":      {},
	"MICROSECOND": {},
}

var supportedExtractCompositeUnits = map[string]struct{}{
	"YEAR_MONTH":         {},
	"DAY_HOUR":           {},
	"DAY_MINUTE":         {},
	"DAY_SECOND":         {},
	"DAY_MICROSECOND":    {},
	"HOUR_MINUTE":        {},
	"HOUR_SECOND":        {},
	"HOUR_MICROSECOND":   {},
	"MINUTE_SECOND":      {},
	"MINUTE_MICROSECOND": {},
	"SECOND_MICROSECOND": {},
}

// rewriteExtractDateFunctions translates the common MySQL EXTRACT syntax to
// the scalar date-part functions already supported by the expression engine.
// Compound units are rewritten to dedicated scalar functions so their packed
// numeric semantics remain explicit instead of being mistaken for a single
// date part.
func rewriteExtractDateFunctions(query string) (string, error) {
	rewritten := query
	for {
		start, open, ok := findExtractFunction(rewritten)
		if !ok {
			return rewritten, nil
		}
		close := matchingParenthesis(rewritten, open)
		if close < 0 {
			return "", fmt.Errorf("EXTRACT has an unterminated argument list")
		}
		body := strings.TrimSpace(rewritten[open+1 : close])
		parts := extractDateUnitPattern.FindStringSubmatch(body)
		if len(parts) != 3 {
			return "", fmt.Errorf("EXTRACT requires a date unit and FROM expression")
		}
		unit := strings.ToUpper(strings.TrimSpace(parts[1]))
		_, simpleUnit := supportedExtractDateUnits[unit]
		_, compositeUnit := supportedExtractCompositeUnits[unit]
		if !simpleUnit && !compositeUnit {
			return "", fmt.Errorf("unsupported EXTRACT unit %s", unit)
		}
		expression, err := rewriteExtractDateFunctions(strings.TrimSpace(parts[2]))
		if err != nil {
			return "", err
		}
		functionName := unit
		if compositeUnit {
			functionName = "EXTRACT_" + unit
		}
		replacement := functionName + "(" + expression + ")"
		rewritten = rewritten[:start] + replacement + rewritten[close+1:]
	}
}

var extractDateUnitPattern = regexp.MustCompile(`(?is)^\s*([a-zA-Z_]+)\s+from\s+(.+?)\s*$`)

func findExtractFunction(input string) (start, open int, ok bool) {
	var quote byte
	for index := 0; index < len(input); index++ {
		character := input[index]
		if quote != 0 {
			if character == quote {
				if index+1 < len(input) && input[index+1] == quote {
					index++
					continue
				}
				quote = 0
			}
			continue
		}
		if character == '\'' || character == '"' || character == '`' {
			quote = character
			continue
		}
		if index+7 > len(input) || !strings.EqualFold(input[index:index+7], "extract") {
			continue
		}
		if index > 0 && isExtractIdentifierCharacter(input[index-1]) {
			continue
		}
		cursor := index + 7
		for cursor < len(input) && (input[cursor] == ' ' || input[cursor] == '\t' || input[cursor] == '\r' || input[cursor] == '\n') {
			cursor++
		}
		if cursor < len(input) && input[cursor] == '(' {
			return index, cursor, true
		}
		index += 6
	}
	return 0, 0, false
}

func isExtractIdentifierCharacter(character byte) bool {
	return character == '_' || character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9'
}

func matchingParenthesis(input string, open int) int {
	if open < 0 || open >= len(input) || input[open] != '(' {
		return -1
	}
	depth := 0
	var quote byte
	for i := open; i < len(input); i++ {
		ch := input[i]
		if quote != 0 {
			if ch == quote {
				if i+1 < len(input) && input[i+1] == quote {
					i++
					continue
				}
				quote = 0
			}
			continue
		}
		switch ch {
		case '\'', '"', '`':
			quote = ch
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}
