package sqlparser

import (
	"fmt"
	"strings"
)

type ViewStatement struct {
	Kind        string
	Name        TableName
	Columns     []string
	Definition  Statement
	Algorithm   string
	Definer     AccountName
	Security    string
	CheckOption string
	Raw         string
}

func (node *ViewStatement) iStatement() {}

func (node *ViewStatement) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	buf.Myprintf("%s view %v", node.Kind, node.Name)
	if node.Definition != nil {
		buf.Myprintf(" as %v", node.Definition)
	}
}

func (node *ViewStatement) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Name, node.Definition)
}

func tryParseViewForExecution(query string) (*ViewStatement, bool, error) {
	tokens, err := scanShowExecutionTokens(query)
	if err != nil || len(tokens) < 3 {
		return nil, false, err
	}
	kind := strings.ToLower(tokens[0].text)
	if kind == "drop" && len(tokens) >= 3 && strings.EqualFold(tokens[1].text, "view") {
		name, _, ok := parseShowQualifiedName(tokens, 2)
		if !ok {
			return nil, true, fmt.Errorf("%w: VIEW name missing", ErrInvalidStatement)
		}
		return &ViewStatement{Kind: kind, Name: name, Raw: query}, true, nil
	}
	if kind != "create" && kind != "alter" {
		return nil, false, nil
	}
	index := 1
	if index < len(tokens) && strings.EqualFold(tokens[index].text, "or") {
		index++
		if index < len(tokens) && strings.EqualFold(tokens[index].text, "replace") {
			index++
		}
	}
	optionStart := index
	for index < len(tokens) && !strings.EqualFold(tokens[index].text, "view") {
		// CREATE/ALTER view options occur before VIEW. Their values are
		// captured below; this scan only finds the grammar anchor.
		index++
	}
	if index >= len(tokens) || !strings.EqualFold(tokens[index].text, "view") {
		return nil, false, nil
	}
	viewIndex := index
	index++
	name, next, ok := parseShowQualifiedName(tokens, index)
	if !ok {
		return nil, true, fmt.Errorf("%w: VIEW name missing", ErrInvalidStatement)
	}
	index = next
	statement := &ViewStatement{Kind: kind, Name: name, Raw: query}
	for optionIndex := optionStart; optionIndex < viewIndex; optionIndex++ {
		if strings.EqualFold(tokens[optionIndex].text, "algorithm") && optionIndex+2 < viewIndex && tokens[optionIndex+1].text == "=" {
			statement.Algorithm = strings.ToLower(tokens[optionIndex+2].text)
		}
		if strings.EqualFold(tokens[optionIndex].text, "security") && optionIndex+1 < viewIndex {
			statement.Security = strings.ToLower(tokens[optionIndex+1].text)
		}
		if strings.EqualFold(tokens[optionIndex].text, "definer") && optionIndex+3 < viewIndex && tokens[optionIndex+1].text == "=" {
			if account, _, ok := parseAccountNameTokens(tokens, optionIndex+2); ok {
				statement.Definer = account
			}
		}
	}
	if index < len(tokens) && tokens[index].text == "(" {
		index++
		for index < len(tokens) && tokens[index].text != ")" {
			if tokens[index].text != "," {
				statement.Columns = append(statement.Columns, strings.Trim(tokens[index].text, "`"))
			}
			index++
		}
		if index >= len(tokens) {
			return nil, true, fmt.Errorf("%w: VIEW column list is unclosed", ErrInvalidStatement)
		}
		index++
	}
	for index < len(tokens) && !strings.EqualFold(tokens[index].text, "as") {
		if strings.EqualFold(tokens[index].text, "algorithm") && index+2 < len(tokens) && tokens[index+1].text == "=" {
			statement.Algorithm = strings.ToLower(tokens[index+2].text)
			index += 3
			continue
		}
		if strings.EqualFold(tokens[index].text, "sql") && index+2 < len(tokens) && strings.EqualFold(tokens[index+1].text, "security") {
			statement.Security = strings.ToLower(tokens[index+2].text)
			index += 3
			continue
		}
		if strings.EqualFold(tokens[index].text, "definer") && index+3 < len(tokens) && tokens[index+1].text == "=" {
			if account, nextAccount, ok := parseAccountNameTokens(tokens, index+2); ok {
				statement.Definer = account
				index = nextAccount
				continue
			}
		}
		index++
	}
	if index >= len(tokens) || !strings.EqualFold(tokens[index].text, "as") {
		return nil, true, fmt.Errorf("%w: VIEW definition missing", ErrInvalidStatement)
	}
	// Reparse the definition from the first AS in the original SQL. The
	// ordinary parser owns nested expressions, comments and string literals.
	asIndex := indexKeywordOutsideQuotes(query, " as ")
	if asIndex < 0 {
		return nil, true, fmt.Errorf("%w: VIEW definition missing", ErrInvalidStatement)
	}
	bodyText := strings.TrimSpace(query[asIndex+4:])
	bodyLower := strings.ToLower(bodyText)
	if check := strings.LastIndex(bodyLower, " with local check option"); check >= 0 && strings.TrimSpace(bodyText[check+len(" with local check option"):]) == "" {
		statement.CheckOption = "LOCAL"
		bodyText = strings.TrimSpace(bodyText[:check])
	} else if check := strings.LastIndex(bodyLower, " with cascaded check option"); check >= 0 && strings.TrimSpace(bodyText[check+len(" with cascaded check option"):]) == "" {
		statement.CheckOption = "CASCADED"
		bodyText = strings.TrimSpace(bodyText[:check])
	} else if check := strings.LastIndex(bodyLower, " with check option"); check >= 0 && strings.TrimSpace(bodyText[check+len(" with check option"):]) == "" {
		statement.CheckOption = "CASCADED"
		bodyText = strings.TrimSpace(bodyText[:check])
	}
	definition, parseErr := ParseForExecution(bodyText)
	if parseErr != nil {
		return nil, true, parseErr
	}
	statement.Definition = definition.AST
	return statement, true, nil
}
