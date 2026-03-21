package engine

import (
	"regexp"
	"strings"
)

func filterShowRowsByLike(rows [][]interface{}, like string) [][]interface{} {
	if strings.TrimSpace(like) == "" {
		return rows
	}
	filtered := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		name, ok := row[0].(string)
		if !ok {
			continue
		}
		if sqlLikeMatch(name, like) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func FilterShowRowsByLike(rows [][]interface{}, like string) [][]interface{} {
	return filterShowRowsByLike(rows, like)
}

func sqlLikeMatch(value, like string) bool {
	var b strings.Builder
	b.WriteString("(?i)^")
	for _, r := range like {
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	re, err := regexp.Compile(b.String())
	if err != nil {
		return false
	}
	return re.MatchString(value)
}
