package sqlparser

import (
	"fmt"
	"strings"
)

type PreparedStatement struct {
	Kind             string
	Name             string
	Source           string
	SourceExpression string
	Parameters       []string
	Target           TableName
	Raw              string
}

// HandlerStatement describes the session cursor form of HANDLER.  The
// engine keeps the cursor lifecycle; this node owns only SQL syntax values.
type HandlerStatement struct {
	Kind      string
	Name      string
	Alias     string
	Table     TableName
	Index     string
	Direction string
	Condition Expr
	Limit     *Limit
	Suffix    string
	Raw       string
}

func (node *HandlerStatement) iStatement() {}

func (node *HandlerStatement) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	buf.Myprintf("handler %s", node.Kind)
	if node.Table.Name.String() != "" {
		buf.Myprintf(" %v", node.Table)
	}
	if node.Name != "" {
		buf.Myprintf(" %s", node.Name)
	}
	if node.Direction != "" {
		buf.Myprintf(" %s", node.Direction)
	}
	if node.Limit != nil {
		buf.Myprintf(" %v", node.Limit)
	}
}

func (node *HandlerStatement) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Table, node.Condition, node.Limit)
}

// ComponentStatement represents INSTALL/UNINSTALL COMPONENT with a literal
// URN.  The URN is lexed as one value and is never rediscovered by text regex.
type ComponentStatement struct {
	Kind    string
	URN     string
	URNs    []string
	Options map[string]string
	Raw     string
}

func (node *ComponentStatement) iStatement() {}
func (node *ComponentStatement) Format(buf *TrackedBuffer) {
	if node != nil {
		buf.Myprintf("%s component '%s'", node.Kind, node.URN)
	}
}
func (node *ComponentStatement) walkSubtree(visit Visit) error { return nil }

func (node *PreparedStatement) iStatement() {}

func (node *PreparedStatement) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	buf.Myprintf("%s %s", node.Kind, node.Name)
	if node.Source != "" {
		buf.Myprintf(" from '%s'", node.Source)
	}
}

func (node *PreparedStatement) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Target)
}

func parsePreparedForExecution(query string) (*PreparedStatement, error) {
	tokens, err := scanShowExecutionTokens(query)
	if err != nil || len(tokens) < 2 {
		return nil, fmt.Errorf("%w: prepared statement is incomplete", ErrInvalidStatement)
	}
	kind := strings.ToLower(tokens[0].text)
	statement := &PreparedStatement{Kind: kind, Raw: query}
	index := 1
	if kind == "deallocate" && index < len(tokens) && strings.EqualFold(tokens[index].text, "prepare") {
		index++
	}
	if index >= len(tokens) {
		return nil, fmt.Errorf("%w: prepared statement name missing", ErrInvalidStatement)
	}
	statement.Name = tokens[index].text
	index++
	if kind == "prepare" {
		if index >= len(tokens) || !strings.EqualFold(tokens[index].text, "from") {
			return nil, fmt.Errorf("%w: PREPARE source missing", ErrInvalidStatement)
		}
		index++
		if index >= len(tokens) {
			return nil, fmt.Errorf("%w: PREPARE source missing", ErrInvalidStatement)
		}
		statement.Source = tokens[index].text
		index++
		// Tokenizer values intentionally omit quote delimiters. PREPARE's
		// source expression needs the original literal so the existing scalar
		// evaluator can distinguish a SQL string from an identifier.
		if from := strings.Index(strings.ToLower(query), " from "); from >= 0 {
			statement.SourceExpression = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query[from+6:]), ";"))
		}
	} else if kind == "execute" && index < len(tokens) {
		if !strings.EqualFold(tokens[index].text, "using") {
			return nil, fmt.Errorf("%w: unexpected EXECUTE token %q", ErrInvalidStatement, tokens[index].text)
		}
		index++
		for index < len(tokens) {
			statement.Parameters = append(statement.Parameters, tokens[index].text)
			index++
			if index < len(tokens) && tokens[index].text == "," {
				index++
			}
		}
	}
	if index != len(tokens) {
		return nil, fmt.Errorf("%w: unexpected prepared token %q", ErrInvalidStatement, tokens[index].text)
	}
	return statement, nil
}

func parseHandlerForExecution(query string) (*HandlerStatement, error) {
	tokens, err := scanShowExecutionTokens(query)
	if err != nil || len(tokens) < 2 {
		return nil, fmt.Errorf("%w: HANDLER statement is incomplete", ErrInvalidStatement)
	}
	if !strings.EqualFold(tokens[0].text, "handler") {
		return nil, fmt.Errorf("%w: HANDLER expected", ErrInvalidStatement)
	}
	node := &HandlerStatement{Raw: query}
	index := 1
	action := ""
	if strings.EqualFold(tokens[index].text, "open") || strings.EqualFold(tokens[index].text, "close") || strings.EqualFold(tokens[index].text, "read") {
		action = strings.ToLower(tokens[index].text)
		index++
	} else {
		table, next, ok := parseShowQualifiedName(tokens, index)
		if !ok {
			return nil, fmt.Errorf("%w: HANDLER table missing", ErrInvalidStatement)
		}
		node.Table, index = table, next
		if index >= len(tokens) {
			return nil, fmt.Errorf("%w: HANDLER operation missing", ErrInvalidStatement)
		}
		action = strings.ToLower(tokens[index].text)
		index++
	}
	node.Kind = action
	switch node.Kind {
	case "open":
		if node.Table.Name.String() == "" {
			name, next, ok := parseShowQualifiedName(tokens, index)
			if !ok {
				return nil, fmt.Errorf("%w: HANDLER OPEN table missing", ErrInvalidStatement)
			}
			node.Table, index = name, next
		}
		if index+1 < len(tokens) && strings.EqualFold(tokens[index].text, "as") {
			node.Alias = tokens[index+1].text
			index += 2
		}
		if index < len(tokens) && strings.EqualFold(tokens[index].text, "read") {
			node.Kind = "open read"
			index++
		}
	case "close":
		if node.Table.Name.String() == "" {
			if index >= len(tokens) {
				return nil, fmt.Errorf("%w: HANDLER CLOSE name missing", ErrInvalidStatement)
			}
			node.Name, index = tokens[index].text, index+1
		}
	case "read":
		if index >= len(tokens) {
			return nil, fmt.Errorf("%w: HANDLER READ name missing", ErrInvalidStatement)
		}
		node.Name, index = tokens[index].text, index+1
		if index < len(tokens) {
			candidate := strings.ToLower(tokens[index].text)
			if candidate == "first" || candidate == "next" || candidate == "last" {
				node.Direction, index = candidate, index+1
			}
		}
	default:
		return nil, fmt.Errorf("%w: HANDLER %s", ErrUnsupportedStatement, node.Kind)
	}
	for index < len(tokens) {
		if tokens[index].text != "limit" && tokens[index].text != "where" && tokens[index].start >= 0 && tokens[index].start < len(query) {
			node.Suffix = strings.TrimSpace(query[tokens[index].start:])
			return node, nil
		}
		if strings.EqualFold(tokens[index].text, "limit") && index+1 < len(tokens) {
			node.Limit = &Limit{Rowcount: NewIntVal([]byte(tokens[index+1].text))}
			index += 2
			continue
		}
		if strings.EqualFold(tokens[index].text, "where") && tokens[index].end < len(query) {
			whereSQL := strings.TrimSpace(query[tokens[index].end:])
			whereParsed, whereErr := Parse("select 1 where " + whereSQL)
			if whereErr != nil {
				return nil, whereErr
			}
			whereSelect, whereOK := whereParsed.(*Select)
			if !whereOK || whereSelect.Where == nil {
				return nil, fmt.Errorf("%w: HANDLER WHERE expression invalid", ErrInvalidStatement)
			}
			node.Condition = whereSelect.Where.Expr
			break
		}
		return nil, fmt.Errorf("%w: unexpected HANDLER token %q", ErrInvalidStatement, tokens[index].text)
	}
	return node, nil
}

// ParseHandlerForExecution exposes the complete HANDLER parser to the
// compatibility executor without changing the generic execution-family
// contract, where HANDLER remains an unsupported top-level family.
func ParseHandlerForExecution(query string) (*HandlerStatement, error) {
	return parseHandlerForExecution(query)
}

func parseComponentForExecution(query string) (*ComponentStatement, error) {
	tokens, err := scanShowExecutionTokens(query)
	if err != nil || len(tokens) < 3 || !strings.EqualFold(tokens[1].text, "component") {
		return nil, fmt.Errorf("%w: component statement is incomplete", ErrInvalidStatement)
	}
	kind := strings.ToLower(tokens[0].text)
	if kind != "install" && kind != "uninstall" {
		return nil, fmt.Errorf("%w: component action %s", ErrUnsupportedStatement, kind)
	}
	node := &ComponentStatement{Kind: kind, URN: tokens[2].text, URNs: []string{tokens[2].text}, Options: make(map[string]string), Raw: query}
	for index := 3; index < len(tokens); index++ {
		if tokens[index].text == "," {
			continue
		}
		if strings.EqualFold(tokens[index].text, "set") {
			continue
		}
		if index+2 < len(tokens) && tokens[index+1].text == "=" {
			node.Options[strings.ToLower(tokens[index].text)] = tokens[index+2].text
			index += 2
			continue
		}
		node.URNs = append(node.URNs, tokens[index].text)
	}
	return node, nil
}
