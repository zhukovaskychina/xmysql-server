package sqlparser

import (
	"fmt"
	"strings"
)

type AdminStatement struct {
	Kind             string
	Targets          []TableName
	TargetModes      []string
	ID               string
	Scope            string
	Channel          string
	ChannelSpecified bool
	Options          map[string]string
	OptionValues     map[string]AdminOptionValue
	Histogram        *AdminHistogram
	Raw              string
}

type AdminHistogram struct {
	Operation string
	Columns   []ColIdent
	Buckets   string
}

// AdminOptionValue retains list nesting and decoded lexer values. In particular,
// punctuation inside a quoted value cannot become a list separator or keyword.
type AdminOptionValue struct {
	Text   string
	List   []AdminOptionValue
	IsList bool
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
		if index < len(tokens) && (strings.EqualFold(tokens[index].text, "local") || strings.EqualFold(tokens[index].text, "no_write_to_binlog")) {
			statement.Options[strings.ToLower(tokens[index].text)] = "true"
			index++
		}
		if index >= len(tokens) || !strings.EqualFold(tokens[index].text, "table") {
			return nil, fmt.Errorf("%w: TABLE expected", ErrInvalidStatement)
		}
		index++
		for {
			table, next, ok := parseAdminTableName(tokens, index)
			if !ok {
				return nil, fmt.Errorf("%w: admin target invalid", ErrInvalidStatement)
			}
			statement.Targets = append(statement.Targets, table)
			index = next
			if index == len(tokens) || tokens[index].kind != ',' {
				break
			}
			index++
		}
		if kind == "analyze" && index < len(tokens) && (tokens[index].kind == UPDATE || tokens[index].kind == DROP) {
			histogram := &AdminHistogram{Operation: strings.ToLower(tokens[index].text)}
			index++
			if len(statement.Targets) != 1 || index+1 >= len(tokens) || !strings.EqualFold(tokens[index].text, "histogram") || !strings.EqualFold(tokens[index+1].text, "on") {
				return nil, fmt.Errorf("%w: HISTOGRAM ON requires exactly one table", ErrInvalidStatement)
			}
			index += 2
			for {
				if index >= len(tokens) || !isAdminNameToken(tokens[index]) {
					return nil, fmt.Errorf("%w: histogram column missing", ErrInvalidStatement)
				}
				histogram.Columns = append(histogram.Columns, NewColIdent(tokens[index].text))
				index++
				if index == len(tokens) || tokens[index].kind != ',' {
					break
				}
				index++
			}
			if index < len(tokens) && tokens[index].kind == WITH {
				index++
				if histogram.Operation != "update" || index+1 >= len(tokens) || tokens[index].kind != INTEGRAL || (!strings.EqualFold(tokens[index+1].text, "buckets") && !strings.EqualFold(tokens[index+1].text, "bucket")) {
					return nil, fmt.Errorf("%w: UPDATE HISTOGRAM WITH count BUCKETS expected", ErrInvalidStatement)
				}
				histogram.Buckets = tokens[index].text
				index += 2
			}
			statement.Histogram = histogram
		} else {
			for index < len(tokens) {
				if tokens[index].kind == FOR && index+2 == len(tokens) && strings.EqualFold(tokens[index+1].text, "upgrade") {
					statement.Options["for_upgrade"] = "true"
					index += 2
					break
				}
				option := strings.ToLower(tokens[index].text)
				switch option {
				case "quick", "extended", "fast", "medium", "changed", "use_frm":
					statement.Options[option] = "true"
					index++
				default:
					return nil, fmt.Errorf("%w: unsupported table maintenance option", ErrInvalidStatement)
				}
			}
		}
	} else if kind == "lock" || kind == "unlock" {
		if index >= len(tokens) || !strings.EqualFold(tokens[index].text, "tables") {
			return nil, fmt.Errorf("%w: TABLES expected", ErrInvalidStatement)
		}
		index++
		if kind == "lock" && index == len(tokens) {
			return nil, fmt.Errorf("%w: table lock target missing", ErrInvalidStatement)
		}
		if kind == "unlock" && index < len(tokens) {
			return nil, fmt.Errorf("%w: unexpected UNLOCK target", ErrInvalidStatement)
		}
		for index < len(tokens) {
			table, next, ok := parseAdminTableName(tokens, index)
			if !ok {
				return nil, fmt.Errorf("%w: table lock target invalid", ErrInvalidStatement)
			}
			statement.Targets = append(statement.Targets, table)
			index = next
			if index < len(tokens) && (strings.EqualFold(tokens[index].text, "read") || strings.EqualFold(tokens[index].text, "write")) {
				statement.Options["lock"] = strings.ToLower(tokens[index].text)
				statement.TargetModes = append(statement.TargetModes, strings.ToLower(tokens[index].text))
				index++
				if index < len(tokens) && strings.EqualFold(tokens[index].text, "local") && statement.TargetModes[len(statement.TargetModes)-1] == "read" {
					index++
				}
			} else {
				return nil, fmt.Errorf("%w: READ or WRITE required for LOCK TABLES", ErrInvalidStatement)
			}
			if index < len(tokens) && tokens[index].text == "," {
				index++
				if index == len(tokens) {
					return nil, fmt.Errorf("%w: table lock target missing", ErrInvalidStatement)
				}
			} else if index < len(tokens) {
				return nil, fmt.Errorf("%w: expected comma between lock targets", ErrInvalidStatement)
			}
		}
	} else if kind == "start" || kind == "stop" {
		if index >= len(tokens) || (!strings.EqualFold(tokens[index].text, "replica") && !strings.EqualFold(tokens[index].text, "slave")) {
			return nil, fmt.Errorf("%w: REPLICA or SLAVE expected", ErrInvalidStatement)
		}
		statement.Options["replica"] = strings.ToLower(tokens[index].text)
		index++
		if index < len(tokens) {
			if index+3 != len(tokens) || tokens[index].kind != FOR || !strings.EqualFold(tokens[index+1].text, "channel") || tokens[index+2].kind != STRING {
				return nil, fmt.Errorf("%w: replication thread options are not supported", ErrInvalidStatement)
			}
			statement.ChannelSpecified = true
			statement.Channel = tokens[index+2].text
			index += 3
		}
	} else if kind == "reset" {
		if index >= len(tokens) {
			return nil, fmt.Errorf("%w: RESET target missing", ErrInvalidStatement)
		}
		target := strings.ToLower(tokens[index].text)
		index++
		switch target {
		case "master":
			statement.Options["master"] = "true"
		case "binary":
			if index+2 >= len(tokens) || !strings.EqualFold(tokens[index].text, "logs") || !strings.EqualFold(tokens[index+1].text, "and") || !strings.EqualFold(tokens[index+2].text, "gtids") {
				return nil, fmt.Errorf("%w: RESET BINARY LOGS AND GTIDS expected", ErrInvalidStatement)
			}
			statement.Options["binary_gtids"] = "true"
			index += 3
			if index < len(tokens) && strings.EqualFold(tokens[index].text, "to") {
				index++
				if index >= len(tokens) || tokens[index].kind != INTEGRAL {
					return nil, fmt.Errorf("%w: RESET file index expected", ErrInvalidStatement)
				}
				statement.ID = tokens[index].text
				index++
			}
		case "replica", "slave":
			statement.Options["replica"] = target
			if index < len(tokens) && strings.EqualFold(tokens[index].text, "all") {
				statement.Options["all"] = "true"
				index++
			}
			if index+3 == len(tokens) && strings.EqualFold(tokens[index].text, "for") && strings.EqualFold(tokens[index+1].text, "channel") && tokens[index+2].kind == STRING {
				statement.ChannelSpecified = true
				statement.Channel = tokens[index+2].text
				index += 3
			}
		default:
			return nil, fmt.Errorf("%w: unsupported RESET target", ErrInvalidStatement)
		}
	} else if kind == "change" {
		statement.OptionValues = make(map[string]AdminOptionValue)
		if index+1 < len(tokens) && strings.EqualFold(tokens[index].text, "replication") && (strings.EqualFold(tokens[index+1].text, "source") || strings.EqualFold(tokens[index+1].text, "filter")) {
			statement.Scope = strings.ToLower(tokens[index+1].text)
			index += 2
		} else if index < len(tokens) && strings.EqualFold(tokens[index].text, "master") {
			statement.Scope = "master"
			index++
		} else {
			return nil, fmt.Errorf("%w: CHANGE REPLICATION SOURCE/FILTER or MASTER expected", ErrInvalidStatement)
		}
		if statement.Scope != "filter" {
			if index >= len(tokens) || !strings.EqualFold(tokens[index].text, "to") {
				return nil, fmt.Errorf("%w: TO expected", ErrInvalidStatement)
			}
			index++
		}
		if index == len(tokens) {
			return nil, fmt.Errorf("%w: CHANGE option missing", ErrInvalidStatement)
		}
		for index < len(tokens) {
			key := strings.ToLower(tokens[index].text)
			index++
			if _, exists := statement.Options[key]; exists {
				return nil, fmt.Errorf("%w: option %s specified more than once", ErrInvalidStatement, key)
			}
			if index >= len(tokens) || tokens[index].kind != '=' {
				return nil, fmt.Errorf("%w: option %s requires =", ErrInvalidStatement, key)
			}
			index++
			start := index
			value, next, valueErr := parseAdminOptionValue(tokens, index)
			if valueErr != nil {
				return nil, valueErr
			}
			statement.OptionValues[key] = value
			index = next
			statement.Options[key] = value.Text
			if value.IsList {
				statement.Options[key] = formatAdminTokenSequence(tokens[start:index])
			}
			if index == len(tokens) {
				break
			}
			if tokens[index].kind == FOR && index+3 == len(tokens) && strings.EqualFold(tokens[index+1].text, "channel") && tokens[index+2].kind == STRING {
				statement.Channel = tokens[index+2].text
				statement.ChannelSpecified = true
				index += 3
				break
			}
			if tokens[index].kind != ',' || index+1 == len(tokens) {
				return nil, fmt.Errorf("%w: expected comma or FOR CHANNEL after option", ErrInvalidStatement)
			}
			index++
		}
	} else if kind == "purge" {
		if index+3 >= len(tokens) || (!strings.EqualFold(tokens[index].text, "binary") && !strings.EqualFold(tokens[index].text, "master")) || !strings.EqualFold(tokens[index+1].text, "logs") || (!strings.EqualFold(tokens[index+2].text, "to") && !strings.EqualFold(tokens[index+2].text, "before")) || tokens[index+3].kind != STRING || tokens[index+3].text == "" {
			return nil, fmt.Errorf("%w: PURGE BINARY LOGS TO/BEFORE requires a quoted target", ErrInvalidStatement)
		}
		statement.Scope = strings.ToLower(tokens[index].text)
		statement.Options[strings.ToLower(tokens[index+2].text)] = "true"
		statement.ID = tokens[index+3].text
		index += 4
	} else if kind == "flush" {
		if index < len(tokens) && (strings.EqualFold(tokens[index].text, "local") || strings.EqualFold(tokens[index].text, "no_write_to_binlog")) {
			statement.Options[strings.ToLower(tokens[index].text)] = "true"
			index++
		}
		if index >= len(tokens) {
			return nil, fmt.Errorf("%w: FLUSH target missing", ErrInvalidStatement)
		}
		if strings.EqualFold(tokens[index].text, "tables") {
			statement.Options["tables"] = "true"
			index++
			for index < len(tokens) {
				if tokens[index].kind == WITH {
					if index+3 != len(tokens) || !strings.EqualFold(tokens[index+1].text, "read") || !strings.EqualFold(tokens[index+2].text, "lock") {
						return nil, fmt.Errorf("%w: WITH READ LOCK expected", ErrInvalidStatement)
					}
					statement.Options["with_read_lock"] = "true"
					index += 3
					break
				}
				table, next, ok := parseAdminTableName(tokens, index)
				if !ok {
					return nil, fmt.Errorf("%w: FLUSH TABLES target invalid", ErrInvalidStatement)
				}
				statement.Targets = append(statement.Targets, table)
				index = next
				if index == len(tokens) {
					break
				}
				if tokens[index].kind == WITH {
					continue
				}
				if tokens[index].kind != ',' || index+1 == len(tokens) || tokens[index+1].kind == WITH {
					return nil, fmt.Errorf("%w: FLUSH table list invalid", ErrInvalidStatement)
				}
				index++
			}
		} else {
			for index < len(tokens) {
				if tokens[index].kind == ',' {
					index++
					continue
				}
				statement.Options[strings.ToLower(tokens[index].text)] = "true"
				index++
			}
		}
	}

	if index != len(tokens) {
		return nil, fmt.Errorf("%w: unexpected admin token %q", ErrInvalidStatement, tokens[index].text)
	}
	return statement, nil
}

func isAdminNameToken(token showExecutionToken) bool {
	return token.kind >= 256 && token.kind != STRING && token.kind != INTEGRAL && token.kind != FLOAT && token.kind != HEX && token.kind != HEXNUM && token.kind != LEX_ERROR
}

func parseAdminTableName(tokens []showExecutionToken, index int) (TableName, int, bool) {
	if index >= len(tokens) || !isAdminNameToken(tokens[index]) {
		return TableName{}, index, false
	}
	name := NewTableIdent(tokens[index].text)
	index++
	if index < len(tokens) && tokens[index].kind == '.' {
		index++
		if index >= len(tokens) || !isAdminNameToken(tokens[index]) {
			return TableName{}, index, false
		}
		return TableName{Qualifier: name, Name: NewTableIdent(tokens[index].text)}, index + 1, true
	}
	return TableName{Name: name}, index, true
}

func parseAdminOptionValue(tokens []showExecutionToken, index int) (AdminOptionValue, int, error) {
	return parseAdminOptionValueDepth(tokens, index, 0)
}

func parseAdminOptionValueDepth(tokens []showExecutionToken, index, depth int) (AdminOptionValue, int, error) {
	invalid := func() (AdminOptionValue, int, error) {
		return AdminOptionValue{}, index, fmt.Errorf("%w: invalid admin option value", ErrInvalidStatement)
	}
	if index >= len(tokens) || depth > 64 {
		return invalid()
	}
	if tokens[index].kind == '(' {
		value := AdminOptionValue{IsList: true, List: make([]AdminOptionValue, 0)}
		index++
		if index < len(tokens) && tokens[index].kind == ')' {
			return value, index + 1, nil
		}
		for index < len(tokens) {
			item, next, err := parseAdminOptionValueDepth(tokens, index, depth+1)
			if err != nil {
				return AdminOptionValue{}, next, err
			}
			value.List = append(value.List, item)
			index = next
			if index < len(tokens) && tokens[index].kind == ')' {
				return value, index + 1, nil
			}
			if index >= len(tokens) || tokens[index].kind != ',' {
				return invalid()
			}
			index++
		}
		return invalid()
	}
	if tokens[index].kind < 256 || tokens[index].kind == LEX_ERROR {
		return invalid()
	}
	value := AdminOptionValue{Text: tokens[index].text}
	quoted := tokens[index].kind == STRING
	index++
	if !quoted && index+1 < len(tokens) && tokens[index].kind == '.' && tokens[index+1].kind >= 256 {
		value.Text += "." + tokens[index+1].text
		index += 2
	}
	return value, index, nil
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
