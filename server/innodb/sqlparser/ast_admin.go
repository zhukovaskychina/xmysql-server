package sqlparser

import (
	"fmt"
	"strings"
)

type AdminStatement struct {
	Kind        string
	Targets     []TableName
	TargetModes []string
	ID          string
	Scope       string
	Channel     string
	Options     map[string]string
	Raw         string
}

func (node *AdminStatement) iStatement() {}

func (node *AdminStatement) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	buf.Myprintf("%s", node.Kind)
	if node.ID != "" {
		buf.Myprintf(" %s", node.ID)
	}
	for _, target := range node.Targets {
		buf.Myprintf(" %v", target)
	}
}

func (node *AdminStatement) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	for _, target := range node.Targets {
		if err := Walk(visit, target); err != nil {
			return err
		}
	}
	return nil
}

func parseAdminForExecution(query string) (*AdminStatement, error) {
	tokens, err := scanShowExecutionTokens(query)
	if err != nil || len(tokens) == 0 {
		return nil, fmt.Errorf("%w: invalid admin statement", ErrInvalidStatement)
	}
	kind := strings.ToLower(tokens[0].text)
	if !adminKindSupported(kind) {
		return nil, fmt.Errorf("%w: admin statement %s", ErrUnsupportedStatement, kind)
	}
	statement := &AdminStatement{Kind: kind, Raw: query, Options: make(map[string]string)}
	index := 1
	if kind == "kill" {
		if index < len(tokens) && (strings.EqualFold(tokens[index].text, "query") || strings.EqualFold(tokens[index].text, "connection")) {
			statement.Scope = strings.ToLower(tokens[index].text)
			index++
		}
		if index >= len(tokens) {
			return nil, fmt.Errorf("%w: KILL id missing", ErrInvalidStatement)
		}
		statement.ID = tokens[index].text
		index++
	} else if kind == "repair" || kind == "optimize" || kind == "analyze" || kind == "check" {
		if index < len(tokens) && strings.EqualFold(tokens[index].text, "table") {
			index++
		}
		for index < len(tokens) {
			if strings.EqualFold(tokens[index].text, "quick") || strings.EqualFold(tokens[index].text, "extended") {
				statement.Options[strings.ToLower(tokens[index].text)] = "true"
				index++
				continue
			}
			table, next, ok := parseShowQualifiedName(tokens, index)
			if !ok {
				return nil, fmt.Errorf("%w: admin target invalid", ErrInvalidStatement)
			}
			statement.Targets = append(statement.Targets, table)
			index = next
			if index < len(tokens) && tokens[index].text == "," {
				index++
			} else if index < len(tokens) && (strings.EqualFold(tokens[index].text, "quick") || strings.EqualFold(tokens[index].text, "extended")) {
				continue
			} else if index < len(tokens) {
				return nil, fmt.Errorf("%w: expected comma between admin targets", ErrInvalidStatement)
			}
		}
	} else if kind == "lock" || kind == "unlock" {
		if index < len(tokens) && strings.EqualFold(tokens[index].text, "tables") {
			index++
		}
		for index < len(tokens) {
			table, next, ok := parseShowQualifiedName(tokens, index)
			if !ok {
				return nil, fmt.Errorf("%w: table lock target invalid", ErrInvalidStatement)
			}
			statement.Targets = append(statement.Targets, table)
			index = next
			if index < len(tokens) && (strings.EqualFold(tokens[index].text, "read") || strings.EqualFold(tokens[index].text, "write")) {
				statement.Options["lock"] = strings.ToLower(tokens[index].text)
				statement.TargetModes = append(statement.TargetModes, strings.ToLower(tokens[index].text))
				index++
			}
			if index < len(tokens) && tokens[index].text == "," {
				index++
			}
		}
	} else if kind == "start" || kind == "stop" {
		if index < len(tokens) && (strings.EqualFold(tokens[index].text, "replica") || strings.EqualFold(tokens[index].text, "slave")) {
			statement.Options["replica"] = strings.ToLower(tokens[index].text)
			index++
		}
		for index < len(tokens) {
			if strings.EqualFold(tokens[index].text, "for") && index+2 < len(tokens) && strings.EqualFold(tokens[index+1].text, "channel") {
				statement.Channel = tokens[index+2].text
				index += 3
				continue
			}
			statement.Options[strings.ToLower(tokens[index].text)] = "true"
			index++
		}
	} else if kind == "reset" {
		for index < len(tokens) {
			value := strings.ToLower(tokens[index].text)
			if value == "for" && index+2 < len(tokens) && strings.EqualFold(tokens[index+1].text, "channel") {
				statement.Channel = tokens[index+2].text
				index += 3
				continue
			}
			statement.Options[value] = "true"
			index++
		}
	} else if kind == "change" {
		// CHANGE MASTER/SOURCE/FILTER options are comma-separated key/value
		// pairs. Store each value without interpreting credentials or emitting
		// them in the AST formatter.
		for index < len(tokens) {
			if strings.EqualFold(tokens[index].text, "for") && index+2 < len(tokens) && strings.EqualFold(tokens[index+1].text, "channel") {
				statement.Channel = tokens[index+2].text
				index += 3
				continue
			}
			if tokens[index].text == "," {
				index++
				continue
			}
			key := strings.ToLower(tokens[index].text)
			index++
			if index < len(tokens) && tokens[index].text == "=" {
				index++
				if index >= len(tokens) {
					return nil, fmt.Errorf("%w: option %s value missing", ErrInvalidStatement, key)
				}
				if _, exists := statement.Options[key]; exists {
					return nil, fmt.Errorf("%w: option %s specified more than once", ErrInvalidStatement, key)
				}
				if tokens[index].text == "(" {
					depth := 0
					parts := make([]showExecutionToken, 0)
					for index < len(tokens) {
						switch tokens[index].text {
						case "(":
							depth++
						case ")":
							depth--
						}
						parts = append(parts, tokens[index])
						index++
						if depth == 0 {
							break
						}
					}
					if depth != 0 {
						return nil, fmt.Errorf("%w: option %s value is unclosed", ErrInvalidStatement, key)
					}
					statement.Options[key] = formatAdminTokenSequence(parts)
				} else {
					statement.Options[key] = tokens[index].text
					index++
				}
			} else {
				if _, exists := statement.Options[key]; exists {
					return nil, fmt.Errorf("%w: option %s specified more than once", ErrInvalidStatement, key)
				}
				statement.Options[key] = "true"
			}
		}
	} else if kind == "purge" {
		if index < len(tokens) {
			statement.Scope = strings.ToLower(tokens[index].text)
			index++
		}
		if index < len(tokens) && strings.EqualFold(tokens[index].text, "logs") {
			index++
		}
		if index < len(tokens) && (strings.EqualFold(tokens[index].text, "to") || strings.EqualFold(tokens[index].text, "before")) {
			statement.Options[strings.ToLower(tokens[index].text)] = "true"
			index++
			if index < len(tokens) {
				statement.ID = tokens[index].text
				index++
			}
		}
	} else if kind == "flush" {
		for index < len(tokens) {
			if tokens[index].text == "," {
				index++
				continue
			}
			statement.Options[strings.ToLower(tokens[index].text)] = "true"
			index++
		}
	}
	if index != len(tokens) {
		return nil, fmt.Errorf("%w: unexpected admin token %q", ErrInvalidStatement, tokens[index].text)
	}
	return statement, nil
}

func formatAdminTokenSequence(tokens []showExecutionToken) string {
	var builder strings.Builder
	for _, token := range tokens {
		text := strings.TrimSpace(token.text)
		if text == "" {
			continue
		}
		if builder.Len() > 0 && text != "," && text != ")" && text != "(" {
			builder.WriteByte(' ')
		}
		builder.WriteString(text)
	}
	return strings.TrimSpace(builder.String())
}

func adminKindSupported(kind string) bool {
	switch kind {
	case "analyze", "check", "repair", "optimize", "kill", "flush", "lock", "unlock", "reset", "start", "stop", "change", "purge":
		return true
	default:
		return false
	}
}
