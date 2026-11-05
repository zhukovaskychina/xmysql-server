package sqlparser

import (
	"fmt"
	"strings"
)

type showExecutionToken struct {
	kind  int
	text  string
	start int
	end   int
}

func parseShowForExecution(query string) (*Show, error) {
	tokens, err := scanShowExecutionTokens(query)
	if err != nil {
		return nil, err
	}
	if len(tokens) < 2 || !strings.EqualFold(tokens[0].text, "show") {
		return nil, fmt.Errorf("%w: SHOW statement expected", ErrInvalidStatement)
	}
	show := &Show{}
	index := 1
	if index < len(tokens) && strings.EqualFold(tokens[index].text, "full") {
		index++
		if index >= len(tokens) {
			return nil, fmt.Errorf("%w: FULL must be followed by TABLES", ErrInvalidStatement)
		}
		if strings.EqualFold(tokens[index].text, "tables") {
			show.Type = "tables"
			show.ShowTablesOpt = &ShowTablesOpt{Full: "full "}
			index++
		} else if strings.EqualFold(tokens[index].text, "columns") || strings.EqualFold(tokens[index].text, "fields") {
			show.Type = strings.ToLower(tokens[index].text)
			index++
		} else {
			return nil, fmt.Errorf("%w: FULL must be followed by TABLES or COLUMNS", ErrInvalidStatement)
		}
	} else if index < len(tokens) && strings.EqualFold(tokens[index].text, "create") {
		index++
		if index >= len(tokens) {
			return nil, fmt.Errorf("%w: SHOW CREATE target missing", ErrInvalidStatement)
		}
		objectType := strings.ToLower(tokens[index].text)
		if !showCreateObjectType(objectType) {
			return nil, fmt.Errorf("%w: unsupported SHOW CREATE target %s", ErrUnsupportedStatement, objectType)
		}
		show.Type = "create " + objectType
		index++
		var ok bool
		show.OnTable, index, ok = parseShowQualifiedName(tokens, index)
		if !ok {
			return nil, fmt.Errorf("%w: SHOW CREATE object missing", ErrInvalidStatement)
		}
	} else {
		if index < len(tokens) && (strings.EqualFold(tokens[index].text, "session") || strings.EqualFold(tokens[index].text, "global") || strings.EqualFold(tokens[index].text, "local")) {
			show.Scope = strings.ToLower(tokens[index].text)
			index++
		}
		if index >= len(tokens) {
			return nil, fmt.Errorf("%w: SHOW type missing", ErrInvalidStatement)
		}
		show.Type = strings.ToLower(tokens[index].text)
		index++
		if (show.Type == "binary" || show.Type == "binlog" || show.Type == "relaylog" || show.Type == "master" || show.Type == "source" || show.Type == "slave") && index < len(tokens) && showTypeSuffix(tokens[index].text) {
			show.Type += " " + strings.ToLower(tokens[index].text)
			index++
		} else if show.Type == "table" && index < len(tokens) && strings.EqualFold(tokens[index].text, "status") {
			show.Type = "table status"
			index++
		} else if show.Type == "replica" && index < len(tokens) && strings.EqualFold(tokens[index].text, "status") {
			index++
		} else if show.Type == "procedure" && index < len(tokens) && strings.EqualFold(tokens[index].text, "status") {
			index++
		}
		if !showExecutionTypeSupported(show.Type) {
			return nil, fmt.Errorf("%w: SHOW %s", ErrUnsupportedStatement, show.Type)
		}
	}

	if show.Type == "tables" && show.ShowTablesOpt == nil {
		show.ShowTablesOpt = &ShowTablesOpt{}
	}
	if show.ShowTablesOpt != nil {
		if index < len(tokens) && (strings.EqualFold(tokens[index].text, "from") || strings.EqualFold(tokens[index].text, "in")) {
			index++
			if index >= len(tokens) {
				return nil, fmt.Errorf("%w: SHOW TABLES database missing", ErrInvalidStatement)
			}
			show.ShowTablesOpt.DbName = tokens[index].text
			index++
		}
	} else if show.Type == "table status" || show.Type == "triggers" || show.Type == "events" || show.Type == "procedure" {
		if index < len(tokens) && (strings.EqualFold(tokens[index].text, "from") || strings.EqualFold(tokens[index].text, "in")) {
			index++
			if index >= len(tokens) || !isShowIdentifier(tokens[index]) {
				return nil, fmt.Errorf("%w: SHOW %s database missing", ErrInvalidStatement, show.Type)
			}
			show.ShowTablesOpt = &ShowTablesOpt{DbName: tokens[index].text}
			index++
		}
	} else if show.Type == "index" || show.Type == "indexes" || show.Type == "keys" || show.Type == "columns" || show.Type == "fields" {
		if index >= len(tokens) || (!strings.EqualFold(tokens[index].text, "from") && !strings.EqualFold(tokens[index].text, "in")) {
			return nil, fmt.Errorf("%w: SHOW %s target missing", ErrInvalidStatement, show.Type)
		}
		index++
		var ok bool
		show.OnTable, index, ok = parseShowQualifiedName(tokens, index)
		if !ok {
			return nil, fmt.Errorf("%w: SHOW %s target missing", ErrInvalidStatement, show.Type)
		}
	}

	if show.Type == "binlog events" || show.Type == "binary logs" || show.Type == "relaylog events" {
		if index < len(tokens) && strings.EqualFold(tokens[index].text, "in") {
			index++
			if index >= len(tokens) {
				return nil, fmt.Errorf("%w: log file missing", ErrInvalidStatement)
			}
			show.LogFile = tokens[index].text
			index++
		}
		if index < len(tokens) && strings.EqualFold(tokens[index].text, "from") {
			index++
			if index >= len(tokens) {
				return nil, fmt.Errorf("%w: log position missing", ErrInvalidStatement)
			}
			show.LogPosition = NewIntVal([]byte(tokens[index].text))
			index++
		}
	}
	if index < len(tokens) && strings.EqualFold(tokens[index].text, "for") {
		index++
		if index < len(tokens) && strings.EqualFold(tokens[index].text, "channel") {
			index++
			if index >= len(tokens) {
				return nil, fmt.Errorf("%w: channel name missing", ErrInvalidStatement)
			}
			show.Channel = tokens[index].text
			index++
		}
	}

	if index < len(tokens) && (strings.EqualFold(tokens[index].text, "like") || strings.EqualFold(tokens[index].text, "where")) {
		clause := strings.ToLower(tokens[index].text)
		index++
		if clause == "like" {
			if index >= len(tokens) {
				return nil, fmt.Errorf("%w: LIKE value missing", ErrInvalidStatement)
			}
			show.Filter = &ShowFilter{Like: tokens[index].text}
			index++
		} else {
			parsed, parseErr := Parse(query)
			if parseErr != nil {
				return nil, fmt.Errorf("%w: SHOW WHERE: %v", ErrInvalidStatement, parseErr)
			}
			if parsedShow, ok := parsed.(*Show); ok {
				show.Filter = parsedShow.Filter
				show.Limit = parsedShow.Limit
				if show.ShowTablesOpt == nil {
					show.ShowTablesOpt = parsedShow.ShowTablesOpt
				}
				if show.OnTable.IsEmpty() {
					show.OnTable = parsedShow.OnTable
				}
			}
			index = len(tokens)
		}
	}
	if index < len(tokens) && strings.EqualFold(tokens[index].text, "limit") {
		index++
		if index >= len(tokens) {
			return nil, fmt.Errorf("%w: LIMIT value missing", ErrInvalidStatement)
		}
		first := NewIntVal([]byte(tokens[index].text))
		index++
		limit := &Limit{Rowcount: first}
		if index < len(tokens) && tokens[index].text == "," {
			index++
			if index >= len(tokens) {
				return nil, fmt.Errorf("%w: LIMIT row count missing", ErrInvalidStatement)
			}
			limit.Offset = first
			limit.Rowcount = NewIntVal([]byte(tokens[index].text))
			index++
		} else if index < len(tokens) && strings.EqualFold(tokens[index].text, "offset") {
			index++
			if index >= len(tokens) {
				return nil, fmt.Errorf("%w: LIMIT offset missing", ErrInvalidStatement)
			}
			limit.Offset = NewIntVal([]byte(tokens[index].text))
			index++
		}
		show.Limit = limit
	}
	if index != len(tokens) {
		return nil, fmt.Errorf("%w: unexpected SHOW token %q", ErrInvalidStatement, tokens[index].text)
	}
	return show, nil
}

func showTypeSuffix(value string) bool {
	switch strings.ToLower(value) {
	case "logs", "log", "events", "status", "hosts":
		return true
	default:
		return false
	}
}

func scanShowExecutionTokens(query string) ([]showExecutionToken, error) {
	tokenizer := NewStringTokenizer(query)
	tokens := make([]showExecutionToken, 0, 16)
	semicolon := false
	for {
		if tokenizer.lastChar == 0 {
			tokenizer.next()
		}
		tokenizer.skipBlank()
		start := tokenizer.Position - 1
		kind, value := tokenizer.Scan()
		if kind == 0 {
			break
		}
		if kind == COMMENT {
			continue
		}
		if kind == ';' {
			semicolon = true
			continue
		}
		if semicolon {
			return nil, fmt.Errorf("%w: token after statement terminator", ErrInvalidStatement)
		}
		text := string(value)
		if text == "" {
			text = KeywordString(kind)
		}
		if text == "" && kind < 256 {
			text = string(rune(kind))
		}
		end := tokenizer.Position - 1
		if end > len(query) {
			end = len(query)
		}
		tokens = append(tokens, showExecutionToken{kind: kind, text: text, start: start, end: end})
	}
	if tokenizer.LastError != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidStatement, tokenizer.LastError)
	}
	return tokens, nil
}

func parseShowQualifiedName(tokens []showExecutionToken, index int) (TableName, int, bool) {
	if index >= len(tokens) || !isShowIdentifier(tokens[index]) {
		return TableName{}, index, false
	}
	first := NewTableIdent(tokens[index].text)
	index++
	if index < len(tokens) && tokens[index].text == "." {
		index++
		if index >= len(tokens) || !isShowIdentifier(tokens[index]) {
			return TableName{}, index, false
		}
		return TableName{Qualifier: first, Name: NewTableIdent(tokens[index].text)}, index + 1, true
	}
	return TableName{Name: first}, index, true
}

func isShowIdentifier(token showExecutionToken) bool {
	return token.text != "" && token.text != "." && token.text != "," && token.text != ";" && !strings.EqualFold(token.text, "like") && !strings.EqualFold(token.text, "where") && !strings.EqualFold(token.text, "limit")
}

func showCreateObjectType(value string) bool {
	switch value {
	case "database", "event", "function", "procedure", "table", "trigger", "view":
		return true
	default:
		return false
	}
}

func showExecutionTypeSupported(value string) bool {
	switch value {
	case "databases", "tables", "columns", "fields", "index", "indexes", "keys", "variables", "status", "processlist", "replica", "replicas", "slave", "replica hosts", "slave hosts", "master status", "source status", "engines", "storage", "open", "plugins", "engine", "warnings", "errors", "triggers", "events", "procedure", "table status", "binlog events", "binary logs", "relaylog events":
		return true
	default:
		return false
	}
}
