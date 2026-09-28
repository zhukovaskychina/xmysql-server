package metrics

import (
	"regexp"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	querypb "github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser/dependency/querypb"
)

var statementDigestBindVariablePattern = regexp.MustCompile(`::digest\d+|:digest\d+`)

// NormalizeStatementDigest produces the stable digest text shared by the
// runtime recorder and Performance Schema projections. Parser normalization
// handles SQL literals; the lexical fallback keeps malformed or unsupported
// statements observable instead of dropping their summary row.
func NormalizeStatementDigest(sql string) string {
	raw := strings.TrimSpace(sql)
	if raw == "" {
		return ""
	}
	stmt, err := sqlparser.Parse(raw)
	if err != nil {
		return lexicalStatementDigestText(raw)
	}
	bindVars := make(map[string]*querypb.BindVariable)
	sqlparser.Normalize(stmt, bindVars, "digest")
	text := statementDigestBindVariablePattern.ReplaceAllString(sqlparser.String(stmt), "?")
	if strings.Contains(text, ":digest") || strings.Contains(text, "::digest") || text == strings.Join(strings.Fields(raw), " ") {
		return lexicalStatementDigestText(raw)
	}
	return strings.Join(strings.Fields(text), " ")
}

func lexicalStatementDigestText(raw string) string {
	var builder strings.Builder
	spacePending := false
	flushSpace := func() {
		if spacePending && builder.Len() > 0 {
			builder.WriteByte(' ')
		}
		spacePending = false
	}
	for index := 0; index < len(raw); {
		ch := raw[index]
		if ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' {
			spacePending = true
			index++
			continue
		}
		if ch == '#' || (ch == '-' && index+2 < len(raw) && raw[index+1] == '-' && (raw[index+2] == ' ' || raw[index+2] == '\t')) {
			for index < len(raw) && raw[index] != '\n' {
				index++
			}
			spacePending = true
			continue
		}
		if ch == '/' && index+1 < len(raw) && raw[index+1] == '*' {
			index += 2
			for index+1 < len(raw) && !(raw[index] == '*' && raw[index+1] == '/') {
				index++
			}
			if index+1 < len(raw) {
				index += 2
			}
			spacePending = true
			continue
		}
		if ch == '\'' || ch == '"' {
			flushSpace()
			quote := ch
			index++
			for index < len(raw) {
				if raw[index] == '\\' && index+1 < len(raw) {
					index += 2
					continue
				}
				if raw[index] == quote {
					if index+1 < len(raw) && raw[index+1] == quote {
						index += 2
						continue
					}
					index++
					break
				}
				index++
			}
			builder.WriteByte('?')
			continue
		}
		if ch >= '0' && ch <= '9' && (index == 0 || !isStatementDigestIdentifierChar(raw[index-1])) {
			flushSpace()
			index++
			for index < len(raw) {
				next := raw[index]
				if (next >= '0' && next <= '9') || next == '.' || next == 'x' || next == 'X' || (next >= 'a' && next <= 'f') || (next >= 'A' && next <= 'F') || next == '+' || next == '-' {
					index++
					continue
				}
				break
			}
			builder.WriteByte('?')
			continue
		}
		flushSpace()
		builder.WriteByte(ch)
		index++
	}
	return strings.TrimSpace(builder.String())
}

func isStatementDigestIdentifierChar(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '$' || ch == '.'
}
