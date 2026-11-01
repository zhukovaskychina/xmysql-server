package sqlparser

import (
	"fmt"
	"strings"
)

type IndexDDLStatement struct {
	Action      string
	Name        string
	Table       TableName
	Columns     []string
	Unique      bool
	FullText    bool
	IfExists    bool
	IfNotExists bool
	Visibility  string
	Raw         string
}

func (node *IndexDDLStatement) iStatement() {}
func (node *IndexDDLStatement) Format(buf *TrackedBuffer) {
	if node != nil {
		buf.Myprintf("%s index %s on %v", node.Action, node.Name, node.Table)
	}
}
func (node *IndexDDLStatement) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Table)
}

func tryParseIndexDDLForExecution(query string) (*IndexDDLStatement, bool, error) {
	tokens, err := scanShowExecutionTokens(query)
	if err != nil || len(tokens) < 4 {
		return nil, false, err
	}
	action := strings.ToLower(tokens[0].text)
	if action != "create" && action != "drop" {
		return nil, false, nil
	}
	index := 1
	statement := &IndexDDLStatement{Action: action, Raw: query}
	if action == "create" {
		if index < len(tokens) && strings.EqualFold(tokens[index].text, "unique") {
			statement.Unique = true
			index++
		}
		if index < len(tokens) && strings.EqualFold(tokens[index].text, "fulltext") {
			statement.FullText = true
			index++
		}
		if index >= len(tokens) || (!strings.EqualFold(tokens[index].text, "index") && !strings.EqualFold(tokens[index].text, "key")) {
			return nil, false, nil
		}
		index++
		if index+2 < len(tokens) && strings.EqualFold(tokens[index].text, "if") && strings.EqualFold(tokens[index+1].text, "not") && strings.EqualFold(tokens[index+2].text, "exists") {
			statement.IfNotExists = true
			index += 3
		}
	} else {
		if index >= len(tokens) || !strings.EqualFold(tokens[index].text, "index") {
			return nil, false, nil
		}
		index++
		if index+1 < len(tokens) && strings.EqualFold(tokens[index].text, "if") && strings.EqualFold(tokens[index+1].text, "exists") {
			statement.IfExists = true
			index += 2
		}
	}
	name, next, ok := parseShowQualifiedName(tokens, index)
	if !ok || name.Name.String() == "" || name.Qualifier.String() != "" {
		return nil, true, fmt.Errorf("invalid index name")
	}
	statement.Name = strings.Trim(name.Name.String(), "`")
	index = next
	if index >= len(tokens) || !strings.EqualFold(tokens[index].text, "on") {
		return nil, true, fmt.Errorf("index ON target missing")
	}
	table, next, ok := parseShowQualifiedName(tokens, index+1)
	if !ok || table.Name.String() == "" {
		return nil, true, fmt.Errorf("index table target missing")
	}
	statement.Table = table
	index = next
	if action == "drop" {
		return statement, true, nil
	}
	if index >= len(tokens) || tokens[index].text != "(" {
		return nil, true, fmt.Errorf("index column list missing")
	}
	depth := 1
	index++
	for index < len(tokens) && depth > 0 {
		if tokens[index].text == ")" {
			depth--
			index++
			continue
		}
		if tokens[index].text == "(" {
			depth++
		}
		if depth == 1 && tokens[index].text != "," {
			statement.Columns = append(statement.Columns, strings.Trim(tokens[index].text, "`"))
		}
		index++
	}
	if depth != 0 || len(statement.Columns) == 0 {
		return nil, true, fmt.Errorf("index column list invalid")
	}
	if index < len(tokens) && (strings.EqualFold(tokens[index].text, "visible") || strings.EqualFold(tokens[index].text, "invisible")) {
		statement.Visibility = strings.ToLower(tokens[index].text)
	}
	return statement, true, nil
}
