package sqlparser

import "strings"

func enrichSetOptions(stmt *Set, query string) {
	if stmt == nil {
		return
	}
	tokens, err := scanShowExecutionTokens(query)
	if err != nil || len(tokens) < 2 || !strings.EqualFold(tokens[0].text, "set") {
		return
	}
	if len(tokens) < 3 || !strings.EqualFold(tokens[1].text, "names") {
		return
	}
	stmt.NamesCharset = tokens[2].text
	for index := 3; index+1 < len(tokens); index++ {
		if strings.EqualFold(tokens[index].text, "collate") {
			stmt.NamesCollation = tokens[index+1].text
			break
		}
	}
}
