package sqlparser

import (
	"fmt"
	"strings"
)

// TablespaceStatement represents CREATE/ALTER/DROP TABLESPACE management
// statements. Options are kept verbatim because the storage layer validates
// only the options it can honor.
type TablespaceStatement struct {
	Action      string
	Name        string
	RenameTo    string
	IfNotExists bool
	Options     string
	Raw         string
}

func (node *TablespaceStatement) iStatement() {}

func (node *TablespaceStatement) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	buf.Myprintf("%s tablespace %s", node.Action, node.Name)
}

func (node *TablespaceStatement) walkSubtree(visit Visit) error { return nil }

func tryParseTablespaceForExecution(query string) (*TablespaceStatement, bool, error) {
	tokens, err := scanShowExecutionTokens(query)
	if err != nil || len(tokens) < 3 {
		return nil, false, err
	}
	action := strings.ToLower(tokens[0].text)
	if (action != "create" && action != "alter" && action != "drop") || !strings.EqualFold(tokens[1].text, "tablespace") {
		return nil, false, nil
	}
	index := 2
	statement := &TablespaceStatement{Action: action, Raw: query}
	if action == "create" && index+2 < len(tokens) && strings.EqualFold(tokens[index].text, "if") && strings.EqualFold(tokens[index+1].text, "not") && strings.EqualFold(tokens[index+2].text, "exists") {
		statement.IfNotExists = true
		index += 3
	}
	name, next, ok := parseShowQualifiedName(tokens, index)
	if !ok || name.Name.String() == "" || name.Qualifier.String() != "" {
		return nil, true, fmt.Errorf("invalid TABLESPACE name")
	}
	statement.Name = strings.Trim(name.Name.String(), "`")
	if action == "alter" {
		if next+1 >= len(tokens) || !strings.EqualFold(tokens[next].text, "rename") || !strings.EqualFold(tokens[next+1].text, "to") {
			return nil, true, fmt.Errorf("ALTER TABLESPACE requires RENAME TO")
		}
		target, end, targetOK := parseShowQualifiedName(tokens, next+2)
		if !targetOK || target.Name.String() == "" || target.Qualifier.String() != "" || end != len(tokens) {
			return nil, true, fmt.Errorf("invalid TABLESPACE rename target")
		}
		statement.RenameTo = strings.Trim(target.Name.String(), "`")
		return statement, true, nil
	}
	if next < len(tokens) {
		statement.Options = strings.TrimSpace(query[tokens[next].start:])
	}
	return statement, true, nil
}
