package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// handlerCursorState is the connection-local state for the small but useful
// HANDLER protocol.  MySQL exposes HANDLER as a session cursor rather than a
// persistent catalog object, so keeping it in session parameters also makes
// COM_RESET_CONNECTION cleanup natural.
type handlerCursorState struct {
	Database   string
	Table      string
	Alias      string
	Index      string
	Position   int
	Result     *SelectResult
	Where      string
	Limit      string
	Lookup     string
	LookupNext string
}

var handlerTablePattern = regexp.MustCompile(`(?is)^handler\s+(` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `(?:\s*\.\s*` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `)?)\s+(open|close|read)\b(.*)$`)

// executeHandlerCompatibility implements the session-cursor forms most
// clients use for indexed/table scans: OPEN, READ FIRST/NEXT and CLOSE.  It
// deliberately reuses the ordinary SELECT/storage path so visibility,
// transactions and result conversion stay identical to SELECT *.
func (e *XMySQLExecutor) executeHandlerCompatibility(ctx *ExecutionContext, session server.MySQLServerSession, query, databaseName string, results chan *Result) (bool, error) {
	match := handlerTablePattern.FindStringSubmatch(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	if len(match) != 4 {
		return false, nil
	}
	if session == nil {
		return true, fmt.Errorf("HANDLER requires a session")
	}
	db, table := handlerQualifiedName(match[1], databaseName)
	if db == "" || table == "" {
		return true, fmt.Errorf("HANDLER table name is incomplete")
	}
	key := strings.ToLower(db + "." + table)
	cursors := handlerCursors(session)
	action := strings.ToLower(strings.TrimSpace(match[2]))
	suffix := strings.TrimSpace(match[3])
	switch action {
	case "open":
		alias, aliasErr := handlerOpenAlias(suffix)
		if aliasErr != nil {
			return true, aliasErr
		}
		cursorKey := key
		if alias != "" {
			cursorKey = strings.ToLower(db + "." + alias)
		}
		if existing := cursors[cursorKey]; existing != nil {
			return true, fmt.Errorf("HANDLER cursor '%s' is already open", strings.TrimPrefix(cursorKey, strings.ToLower(db+".")))
		}
		for _, existing := range cursors {
			if existing != nil && strings.EqualFold(existing.Database, db) && strings.EqualFold(existing.Table, table) {
				return true, fmt.Errorf("HANDLER cursor '%s' is already open", table)
			}
		}
		cursors[cursorKey] = &handlerCursorState{Database: db, Table: table, Alias: alias}
		session.SetParamByName("handler_cursors", cursors)
		results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "HANDLER opened"}
		return true, nil
	case "close":
		closeKey := key
		if _, ok := cursors[closeKey]; !ok {
			for candidateKey, cursor := range cursors {
				if cursor != nil && strings.EqualFold(cursor.Database, db) && strings.EqualFold(cursor.Table, table) {
					closeKey = candidateKey
					break
				}
			}
		}
		if _, ok := cursors[closeKey]; !ok {
			return true, fmt.Errorf("Unknown table '%s' in HANDLER CLOSE", table)
		}
		delete(cursors, closeKey)
		session.SetParamByName("handler_cursors", cursors)
		results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "HANDLER closed"}
		return true, nil
	case "read":
		cursor := cursors[key]
		if cursor == nil {
			return true, fmt.Errorf("Table '%s' was not opened with HANDLER", table)
		}
		result, err := e.readHandlerCursor(ctx, session, cursor, suffix)
		if err != nil {
			return true, err
		}
		results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: fmt.Sprintf("HANDLER returned %d row(s)", result.RowCount)}
		return true, nil
	default:
		return true, fmt.Errorf("unsupported HANDLER operation %q", match[2])
	}
}

func handlerOpenAlias(suffix string) (string, error) {
	suffix = strings.TrimSpace(suffix)
	if suffix == "" {
		return "", nil
	}
	fields := strings.Fields(suffix)
	if len(fields) == 2 && strings.EqualFold(fields[0], "as") {
		suffix = fields[1]
	} else if len(fields) == 1 {
		suffix = fields[0]
	} else {
		return "", fmt.Errorf("invalid HANDLER OPEN alias %q", suffix)
	}
	suffix = strings.Trim(strings.TrimSpace(suffix), "`")
	if !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`).MatchString(suffix) {
		return "", fmt.Errorf("invalid HANDLER OPEN alias %q", suffix)
	}
	return suffix, nil
}

func handlerCursors(session server.MySQLServerSession) map[string]*handlerCursorState {
	if session == nil {
		return map[string]*handlerCursorState{}
	}
	if raw := session.GetParamByName("handler_cursors"); raw != nil {
		if cursors, ok := raw.(map[string]*handlerCursorState); ok && cursors != nil {
			return cursors
		}
	}
	return map[string]*handlerCursorState{}
}

func handlerQualifiedName(raw, defaultDatabase string) (string, string) {
	parts := strings.Split(raw, ".")
	for index := range parts {
		parts[index] = strings.Trim(strings.TrimSpace(parts[index]), "`")
	}
	if len(parts) == 1 {
		return strings.TrimSpace(defaultDatabase), parts[0]
	}
	return parts[len(parts)-2], parts[len(parts)-1]
}

func (e *XMySQLExecutor) readHandlerCursor(ctx *ExecutionContext, session server.MySQLServerSession, cursor *handlerCursorState, suffix string) (*SelectResult, error) {
	if cursor == nil {
		return nil, fmt.Errorf("HANDLER cursor is not open")
	}
	readSpec := strings.TrimSpace(suffix)
	indexName, command, remainder := handlerReadCommand(readSpec)
	if indexName == "" {
		indexName = "PRIMARY"
	}
	indexColumns, err := e.handlerIndexColumns(ctx, cursor.Database, cursor.Table, indexName)
	if err != nil {
		return nil, err
	}
	trimmedRemainder, limit := handlerReadLimit(remainder)
	where := ""
	if whereIndex := strings.Index(strings.ToLower(trimmedRemainder), "where"); whereIndex >= 0 {
		where = strings.TrimSpace(trimmedRemainder[whereIndex:])
	}
	lookup := ""
	nextLookup := ""
	requestedCommand := command
	if command == "match" {
		var lookupErr error
		lookup, nextLookup, lookupErr = handlerLookupPredicate(indexColumns, remainder)
		if lookupErr != nil {
			return nil, lookupErr
		}
		command = "first"
	} else if requestedCommand != "first" && cursor.Result != nil && cursor.Index == indexName && cursor.Lookup != "" {
		// MySQL keeps the active key range for READ index NEXT/PREV after a
		// keyed FIRST lookup. A subsequent unqualified NEXT must not reopen the
		// whole table and reset the cursor to its first row.
		lookup = cursor.Lookup
		if cursor.LookupNext != "" {
			lookup = cursor.LookupNext
		}
	}
	if cursor.Result == nil || cursor.Where != where || cursor.Limit != limit || cursor.Index != indexName || cursor.Lookup != lookup || command == "first" {
		query := fmt.Sprintf("select * from `%s`.`%s`", strings.ReplaceAll(cursor.Database, "`", "``"), strings.ReplaceAll(cursor.Table, "`", "``"))
		predicates := make([]string, 0, 2)
		if lookup != "" {
			predicates = append(predicates, lookup)
		}
		if where != "" {
			wherePredicate := strings.TrimSpace(where)
			if len(wherePredicate) >= len("where") && strings.EqualFold(wherePredicate[:len("where")], "where") {
				wherePredicate = strings.TrimSpace(wherePredicate[len("where"):])
			}
			predicates = append(predicates, wherePredicate)
		}
		if len(predicates) > 0 {
			query += " where " + strings.Join(predicates, " and ")
		}
		if len(indexColumns) > 0 {
			quoted := make([]string, 0, len(indexColumns))
			for _, column := range indexColumns {
				quoted = append(quoted, "`"+strings.ReplaceAll(column, "`", "``")+"`")
			}
			query += " order by " + strings.Join(quoted, ", ")
		}
		if limit != "" {
			query += " " + limit
		}
		if err := queryHandlerSource(e, ctx, session, query, cursor); err != nil {
			return nil, err
		}
		cursor.Where = where
		cursor.Limit = limit
		cursor.Index = indexName
		cursor.Lookup = lookup
		cursor.LookupNext = nextLookup
		if requestedCommand != "match" && cursor.LookupNext == lookup {
			cursor.LookupNext = ""
		}
		cursor.Position = 0
	}
	if command == "last" {
		cursor.Position = len(cursor.Result.Records) - 1
	} else if command == "prev" || command == "previous" {
		cursor.Position--
	}
	if cursor.Position < 0 {
		cursor.Position = 0
	}
	if cursor.Position >= len(cursor.Result.Records) {
		return &SelectResult{Columns: append([]string(nil), cursor.Result.Columns...), ColumnTypes: append([]string(nil), cursor.Result.ColumnTypes...), ResultType: common.RESULT_TYPE_QUERY}, nil
	}
	row := cursor.Result.Records[cursor.Position]
	cursor.Position++
	return &SelectResult{Records: []Record{row}, RowCount: 1, Columns: append([]string(nil), cursor.Result.Columns...), ColumnTypes: append([]string(nil), cursor.Result.ColumnTypes...), ResultType: common.RESULT_TYPE_QUERY}, nil
}

func handlerReadLimit(spec string) (string, string) {
	spec = strings.TrimSpace(spec)
	patterns := []struct {
		pattern *regexp.Regexp
		format  func([]string) string
	}{
		{regexp.MustCompile(`(?is)\s+limit\s+([0-9]+)\s*,\s*([0-9]+)\s*$`), func(match []string) string {
			return "limit " + match[1] + "," + match[2]
		}},
		{regexp.MustCompile(`(?is)\s+limit\s+([0-9]+)\s+offset\s+([0-9]+)\s*$`), func(match []string) string {
			return "limit " + match[1] + " offset " + match[2]
		}},
		{regexp.MustCompile(`(?is)\s+limit\s+([0-9]+)\s*$`), func(match []string) string {
			return "limit " + match[1]
		}},
	}
	for _, candidate := range patterns {
		match := candidate.pattern.FindStringSubmatchIndex(spec)
		if len(match) == 0 {
			continue
		}
		values := candidate.pattern.FindStringSubmatch(spec)
		return strings.TrimSpace(spec[:match[0]]), candidate.format(values)
	}
	return spec, ""
}

func handlerReadCommand(spec string) (string, string, string) {
	consume := func(raw string) (string, string) {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return "", ""
		}
		fields := strings.Fields(raw)
		if len(fields) == 0 {
			return "", ""
		}
		return fields[0], strings.TrimSpace(raw[len(fields[0]):])
	}
	first, remainder := consume(spec)
	lowerFirst := strings.ToLower(first)
	if lowerFirst == "first" || lowerFirst == "next" || lowerFirst == "last" || lowerFirst == "prev" || lowerFirst == "previous" {
		return "PRIMARY", lowerFirst, remainder
	}
	indexName := strings.Trim(first, "` ")
	second, remainder := consume(remainder)
	command := strings.ToLower(second)
	if command == "first" || command == "next" || command == "last" || command == "prev" || command == "previous" {
		return indexName, command, remainder
	}
	if command == "=" || command == "<" || command == "<=" || command == ">" || command == ">=" {
		return indexName, "match", command + " " + strings.TrimSpace(remainder)
	}
	return "PRIMARY", "next", strings.TrimSpace(spec)
}

func (e *XMySQLExecutor) handlerIndexColumns(ctx *ExecutionContext, databaseName, tableName, indexName string) ([]string, error) {
	if e == nil {
		return nil, fmt.Errorf("HANDLER metadata manager is unavailable")
	}
	var (
		meta *metadata.TableMeta
		err  error
	)
	if provider, ok := e.tableManager.(interface {
		GetTableMetadata(context.Context, string, string) (*metadata.TableMeta, error)
	}); ok {
		meta, err = provider.GetTableMetadata(ctxContext(ctx), databaseName, tableName)
	} else if e.infosSchemaManager != nil {
		meta, err = e.infosSchemaManager.GetTableMetadata(ctxContext(ctx), databaseName, tableName)
	} else {
		err = fmt.Errorf("metadata manager is unavailable")
	}
	if err == nil && meta != nil {
		for _, index := range meta.Indices {
			if strings.EqualFold(index.Name, indexName) || (strings.EqualFold(indexName, "PRIMARY") && strings.EqualFold(index.Name, "PRIMARY")) {
				if len(index.Columns) == 0 {
					return nil, fmt.Errorf("HANDLER index %q has no columns", indexName)
				}
				return append([]string(nil), index.Columns...), nil
			}
		}
	}
	// The integrated test/DDL path persists the authoritative definition in
	// .frm before the information-schema cache is refreshed. HANDLER must be
	// usable in that small window too, just like ordinary SELECT.
	frmPath := filepath.Join(e.getDataDir(), databaseName, tableName+".frm")
	raw, readErr := os.ReadFile(frmPath)
	if readErr == nil {
		var definition struct {
			Indexes []struct {
				Name    string   `json:"name"`
				Columns []string `json:"columns"`
			} `json:"indexes"`
		}
		if json.Unmarshal(raw, &definition) == nil {
			for _, index := range definition.Indexes {
				if strings.EqualFold(index.Name, indexName) {
					if len(index.Columns) == 0 {
						return nil, fmt.Errorf("HANDLER index %q has no columns", indexName)
					}
					return append([]string(nil), index.Columns...), nil
				}
			}
		}
	}
	if err == nil {
		err = fmt.Errorf("table metadata is empty")
	}
	if readErr != nil {
		err = fmt.Errorf("metadata and table definition unavailable: %v; %v", err, readErr)
	}
	return nil, fmt.Errorf("Unknown index '%s' in HANDLER READ", indexName)
}

func handlerLookupPredicate(indexColumns []string, spec string) (string, string, error) {
	match := regexp.MustCompile(`(?is)^\s*(=|<=|>=|<|>)\s*\((.*)\)\s*(?:where\s+.*)?$`).FindStringSubmatch(strings.TrimSpace(spec))
	if len(match) != 3 {
		return "", "", fmt.Errorf("invalid HANDLER index lookup %q", strings.TrimSpace(spec))
	}
	values := splitTopLevelComma(match[2])
	if len(values) == 0 || len(values) > len(indexColumns) {
		return "", "", fmt.Errorf("HANDLER index lookup expects between 1 and %d value(s), got %d", len(indexColumns), len(values))
	}
	for index, value := range values {
		values[index] = strings.TrimSpace(value)
		if values[index] == "" {
			return "", "", fmt.Errorf("HANDLER index lookup value %d is empty", index+1)
		}
	}
	operator := match[1]
	keyColumns := indexColumns[:len(values)]
	quoteColumn := func(column string) string {
		return "`" + strings.ReplaceAll(column, "`", "``") + "`"
	}
	equality := make([]string, 0, len(indexColumns))
	for index, column := range keyColumns {
		equality = append(equality, quoteColumn(column)+" = "+values[index])
	}
	if operator == "=" {
		nextLookup := ""
		if len(values) == len(indexColumns) {
			nextLookup = comparisonPredicate(keyColumns, values, ">")
		}
		return strings.Join(equality, " and "), nextLookup, nil
	}
	comparison := func(comparisonOperator string) string {
		terms := make([]string, 0, len(indexColumns))
		for index, column := range keyColumns {
			parts := append([]string(nil), equality[:index]...)
			parts = append(parts, quoteColumn(column)+" "+comparisonOperator+" "+values[index])
			terms = append(terms, "("+strings.Join(parts, " and ")+")")
		}
		return strings.Join(terms, " or ")
	}
	switch operator {
	case ">", "<":
		return comparison(operator), comparison(operator), nil
	case ">=", "<=":
		predicate := "(" + comparison(operator[:1]) + " or (" + strings.Join(equality, " and ") + "))"
		return predicate, predicate, nil
	default:
		return "", "", fmt.Errorf("unsupported HANDLER index operator %q", operator)
	}
}

func comparisonPredicate(indexColumns []string, values []string, operator string) string {
	terms := make([]string, 0, len(indexColumns))
	for index, column := range indexColumns {
		parts := make([]string, 0, index+1)
		for equalIndex := 0; equalIndex < index; equalIndex++ {
			parts = append(parts, "`"+strings.ReplaceAll(indexColumns[equalIndex], "`", "``")+"` = "+values[equalIndex])
		}
		parts = append(parts, "`"+strings.ReplaceAll(column, "`", "``")+"` "+operator+" "+values[index])
		terms = append(terms, "("+strings.Join(parts, " and ")+")")
	}
	return strings.Join(terms, " or ")
}

func queryHandlerSource(e *XMySQLExecutor, parent *ExecutionContext, session server.MySQLServerSession, query string, cursor *handlerCursorState) error {
	if e == nil || cursor == nil {
		return fmt.Errorf("HANDLER executor is unavailable")
	}
	queryContext := context.Background()
	if parent != nil && parent.Context != nil {
		queryContext = parent.Context
	}
	resultChannel := make(chan *Result, 1)
	nested := &ExecutionContext{Context: queryContext, Session: session, DatabaseName: cursor.Database, RawQuery: query, Results: resultChannel}
	e.executeQuery(nested, session, query, cursor.Database, resultChannel)
	for result := range resultChannel {
		if result == nil {
			continue
		}
		if result.Err != nil {
			return result.Err
		}
		if selected, ok := result.Data.(*SelectResult); ok {
			cursor.Result = selected
			return nil
		}
	}
	return fmt.Errorf("HANDLER source query returned no result")
}
