package engine

import "strings"

// rewriteCharsetIntroducers removes MySQL character-set introducers from
// string literals before the embedded SQL parser sees them. The literal value
// remains unchanged; charset conversion is already represented by the
// connection/session charset in this compatibility layer. Keeping the
// rewrite lexical avoids changing identifiers such as column names that merely
// start with an underscore.
func rewriteCharsetIntroducers(query string) string {
	var rewritten strings.Builder
	rewritten.Grow(len(query))
	for index := 0; index < len(query); {
		switch query[index] {
		case '\'', '"', '`':
			quote := query[index]
			rewritten.WriteByte(query[index])
			index++
			for index < len(query) {
				rewritten.WriteByte(query[index])
				if query[index] == '\\' && index+1 < len(query) {
					index++
					rewritten.WriteByte(query[index])
				} else if query[index] == quote {
					index++
					if index < len(query) && query[index] == quote {
						rewritten.WriteByte(query[index])
					} else {
						break
					}
				}
				index++
			}
			continue
		case '#':
			for index < len(query) {
				rewritten.WriteByte(query[index])
				if query[index] == '\n' {
					index++
					break
				}
				index++
			}
			continue
		case '/':
			if index+1 < len(query) && query[index+1] == '*' {
				for index < len(query) {
					rewritten.WriteByte(query[index])
					if query[index] == '*' && index+1 < len(query) && query[index+1] == '/' {
						index++
						rewritten.WriteByte(query[index])
						index++
						break
					}
					index++
				}
				continue
			}
		}
		if query[index] == '_' && (index == 0 || !charsetIdentifierByte(query[index-1])) {
			end := index + 1
			for end < len(query) && charsetIdentifierByte(query[end]) {
				end++
			}
			literal := end
			for literal < len(query) && (query[literal] == ' ' || query[literal] == '\t' || query[literal] == '\r' || query[literal] == '\n') {
				literal++
			}
			if literal < len(query) && (query[literal] == '\'' || query[literal] == '"') {
				index = literal
				continue
			}
		}
		rewritten.WriteByte(query[index])
		index++
	}
	return rewritten.String()
}

func charsetIdentifierByte(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '_'
}
