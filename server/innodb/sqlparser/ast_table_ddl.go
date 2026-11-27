package sqlparser

import (
	"fmt"
	"strings"
)

// TableDDLStatement is the execution-facing wrapper for the table/database
// DDL nodes that predate the staged execution parser.  Keeping the original
// node in Primary preserves all existing column, index, partition and table
// option information while Targets records multi-object forms explicitly.
type TableDDLStatement struct {
	Action        string
	Targets       []TableName
	RenameTo      []TableName
	Primary       Statement
	TableSpec     *TableSpec
	PartitionSpec *PartitionSpec
	IfExists      bool
	IfNotExists   bool
	Definition    string
	TableOptions  string
	PartitionSQL  string
	Like          TableName
	AsSelect      Statement
	AsSelectSQL   string
	SelectMode    string
	Temporary     bool
	Raw           string
}

func (node *TableDDLStatement) iStatement() {}

func (node *TableDDLStatement) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	if node.Primary != nil {
		node.Primary.Format(buf)
		return
	}
	if node.AsSelect != nil && len(node.Targets) == 1 {
		buf.Myprintf("create ")
		if node.Temporary {
			buf.Myprintf("temporary ")
		}
		buf.Myprintf("table ")
		if node.IfNotExists {
			buf.Myprintf("if not exists ")
		}
		buf.Myprintf("%v", node.Targets[0])
		if node.Definition != "" {
			buf.Myprintf(" (%s)", node.Definition)
		}
		if node.TableOptions != "" {
			buf.Myprintf(" %s", node.TableOptions)
		}
		if node.SelectMode != "" {
			buf.Myprintf(" %s", node.SelectMode)
		}
		buf.Myprintf(" as %v", node.AsSelect)
		return
	}
	buf.Myprintf("%s", node.Action)
	for i, target := range node.Targets {
		if i > 0 {
			buf.Myprintf(", ")
		}
		buf.Myprintf("%v", target)
	}
}

func (node *TableDDLStatement) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	if err := Walk(visit, node.Primary, node.AsSelect, node.Like); err != nil {
		return err
	}
	for _, target := range node.Targets {
		if err := Walk(visit, target); err != nil {
			return err
		}
	}
	for _, target := range node.RenameTo {
		if err := Walk(visit, target); err != nil {
			return err
		}
	}
	return nil
}

func parseTableDDLForExecution(query string) (*TableDDLStatement, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	if statement, matched, err := ParseCreateTableAsSelect(trimmed); matched || err != nil {
		return statement, err
	}
	if validationErr := validateTableDDLText(trimmed); validationErr != nil {
		return nil, validationErr
	}
	parsed, err := ParseStrictDDL(trimmed)
	if err == nil && parsed != nil {
		statement := &TableDDLStatement{Raw: query, Primary: parsed}
		switch node := parsed.(type) {
		case *DDL:
			statement.Action = strings.ToLower(node.Action)
			statement.TableSpec = node.TableSpec
			statement.PartitionSpec = node.PartitionSpec
			statement.IfExists = node.IfExists
			statement.Definition, statement.TableOptions, statement.PartitionSQL = tableDDLFragments(trimmed)
			if node.Action == CreateStr {
				statement.Targets = []TableName{node.NewName}
				statement.Temporary = strings.Contains(strings.ToLower(trimmed), "create temporary table")
			} else {
				statement.Targets = []TableName{node.Table}
			}
			// LIKE is retained in the raw definition by the legacy grammar;
			// expose its source when it is a simple CREATE TABLE LIKE form.
			if like := extractCreateLikeTarget(trimmed); like.Name.String() != "" {
				statement.Like = like
			}
		case *DBDDL:
			statement.Action = strings.ToLower(node.Action) + " database"
			statement.Targets = []TableName{{Name: NewTableIdent(node.DBName)}}
		default:
			return nil, fmt.Errorf("%w: unsupported DDL node %T", ErrUnsupportedStatement, parsed)
		}
		if statement.Action == RenameStr {
			if tokens, tokenErr := scanShowExecutionTokens(trimmed); tokenErr == nil {
				if targets, destinations, ok := parseRenameTableTargets(tokens); ok {
					statement.Targets = targets
					statement.RenameTo = destinations
				}
			}
		}
		attachCreateSelect(statement, trimmed)
		return statement, nil
	}

	// The legacy grammar represents only one DROP target.  Parse the target
	// list here so the execution layer can keep ordering and per-target scope.
	tokens, tokenErr := scanShowExecutionTokens(trimmed)
	if tokenErr != nil || len(tokens) < 3 {
		if err == nil {
			err = tokenErr
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidStatement, err)
	}
	action := strings.ToLower(tokens[0].text)
	if action == "create" {
		index := 1
		if index < len(tokens) && strings.EqualFold(tokens[index].text, "temporary") {
			index++
		}
		if index < len(tokens) && strings.EqualFold(tokens[index].text, "table") {
			index++
		}
		if index < len(tokens) && strings.EqualFold(tokens[index].text, "if") {
			index += 3
		}
		name, next, ok := parseShowQualifiedName(tokens, index)
		if ok && next < len(tokens) && strings.EqualFold(tokens[next].text, "like") {
			like, end, likeOK := parseShowQualifiedName(tokens, next+1)
			if likeOK && end == len(tokens) {
				statement := &TableDDLStatement{Action: action, Targets: []TableName{name}, Like: like, Temporary: strings.Contains(strings.ToLower(trimmed), "temporary"), Raw: query}
				statement.Definition, statement.TableOptions, statement.PartitionSQL = tableDDLFragments(trimmed)
				return statement, nil
			}
		}
		if ok {
			statement := &TableDDLStatement{Action: action, Targets: []TableName{name}, Temporary: strings.Contains(strings.ToLower(trimmed), "temporary"), Raw: query}
			statement.Definition, statement.TableOptions, statement.PartitionSQL = tableDDLFragments(trimmed)
			attachCreateSelect(statement, trimmed)
			return statement, nil
		}
	}
	if action != "drop" && action != "rename" {
		return nil, fmt.Errorf("%w: %v", ErrInvalidStatement, err)
	}
	statement := &TableDDLStatement{Action: action, Raw: query}
	index := 1
	if index < len(tokens) && (strings.EqualFold(tokens[index].text, "temporary") || strings.EqualFold(tokens[index].text, "table")) {
		if strings.EqualFold(tokens[index].text, "temporary") {
			statement.Temporary = true
			index++
		}
		if index < len(tokens) && strings.EqualFold(tokens[index].text, "table") {
			index++
		}
	}
	if action == "drop" && index+2 < len(tokens) && strings.EqualFold(tokens[index].text, "if") && strings.EqualFold(tokens[index+1].text, "exists") {
		statement.IfExists = true
		index += 2
	}
	for index < len(tokens) {
		name, next, ok := parseShowQualifiedName(tokens, index)
		if !ok {
			return nil, fmt.Errorf("%w: invalid %s target", ErrInvalidStatement, action)
		}
		statement.Targets = append(statement.Targets, name)
		index = next
		if action == "rename" {
			if index >= len(tokens) || !strings.EqualFold(tokens[index].text, "to") {
				return nil, fmt.Errorf("%w: RENAME target must use TO", ErrInvalidStatement)
			}
			index++
			to, nextTo, ok := parseShowQualifiedName(tokens, index)
			if !ok {
				return nil, fmt.Errorf("%w: invalid RENAME destination", ErrInvalidStatement)
			}
			statement.RenameTo = append(statement.RenameTo, to)
			index = nextTo
		}
		if index == len(tokens) {
			break
		}
		if tokens[index].text != "," {
			return nil, fmt.Errorf("%w: unexpected %s token %q", ErrInvalidStatement, action, tokens[index].text)
		}
		index++
	}
	if len(statement.Targets) == 0 {
		return nil, fmt.Errorf("%w: %s target missing", ErrInvalidStatement, action)
	}
	return statement, nil
}

func parseRenameTableTargets(tokens []showExecutionToken) ([]TableName, []TableName, bool) {
	if len(tokens) < 5 || !strings.EqualFold(tokens[0].text, "rename") || !strings.EqualFold(tokens[1].text, "table") {
		return nil, nil, false
	}
	targets := make([]TableName, 0, 2)
	destinations := make([]TableName, 0, 2)
	index := 2
	for index < len(tokens) {
		source, next, ok := parseShowQualifiedName(tokens, index)
		if !ok || next >= len(tokens) || !strings.EqualFold(tokens[next].text, "to") {
			return nil, nil, false
		}
		destination, end, ok := parseShowQualifiedName(tokens, next+1)
		if !ok {
			return nil, nil, false
		}
		targets = append(targets, source)
		destinations = append(destinations, destination)
		index = end
		if index == len(tokens) {
			return targets, destinations, true
		}
		if tokens[index].text != "," {
			return nil, nil, false
		}
		index++
	}
	return nil, nil, false
}

// ParseCreateTableAsSelect separates the CREATE header and SELECT using
// lexer spans. Parentheses and keywords inside strings, comments or the source
// query never become CREATE column definitions or table options.
func ParseCreateTableAsSelect(query string) (*TableDDLStatement, bool, error) {
	tokens, err := scanShowExecutionTokens(query)
	if err != nil {
		return nil, false, err
	}
	if len(tokens) < 4 || tokens[0].kind != CREATE {
		return nil, false, nil
	}
	index := 1
	temporary := false
	if strings.EqualFold(tokens[index].text, "temporary") {
		temporary = true
		index++
	}
	if index >= len(tokens) || tokens[index].kind != TABLE {
		return nil, false, nil
	}
	index++
	ifNotExists := false
	if index+2 < len(tokens) && tokens[index].kind == IF && tokens[index+1].kind == NOT && tokens[index+2].kind == EXISTS {
		ifNotExists = true
		index += 3
	}
	target, next, ok := parseAdminTableName(tokens, index)
	if !ok || next >= len(tokens) {
		return nil, false, nil
	}
	index = next
	definition := ""
	optionsStart := tokens[index].start
	if tokens[index].kind == '(' {
		open := index
		depth := 1
		index++
		for index < len(tokens) && depth > 0 {
			if tokens[index].kind == '(' {
				depth++
			}
			if tokens[index].kind == ')' {
				depth--
			}
			index++
		}
		if depth != 0 {
			return nil, false, fmt.Errorf("%w: unclosed CREATE TABLE definition", ErrInvalidStatement)
		}
		definition = strings.TrimSpace(query[tokens[open].end:tokens[index-1].start])
		optionsStart = tokens[index-1].end
	}
	sourceIndex := -1
	depth := 0
	for pos := index; pos < len(tokens); pos++ {
		switch tokens[pos].kind {
		case '(':
			depth++
		case ')':
			depth--
		case SELECT, WITH:
			if depth == 0 {
				sourceIndex = pos
			}
		}
		if sourceIndex >= 0 {
			break
		}
	}
	if sourceIndex < 0 {
		return nil, false, nil
	}
	optionsEnd := tokens[sourceIndex].start
	mode := ""
	previous := sourceIndex - 1
	if previous >= index && tokens[previous].kind == AS {
		optionsEnd = tokens[previous].start
		previous--
	}
	if previous >= index && (tokens[previous].kind == IGNORE || tokens[previous].kind == REPLACE) {
		mode = strings.ToLower(tokens[previous].text)
		optionsEnd = tokens[previous].start
	}
	sourceSQL := strings.TrimSpace(query[tokens[sourceIndex].start:])
	source, err := ParseForExecution(sourceSQL)
	if err != nil {
		return nil, true, fmt.Errorf("%w: CREATE TABLE SELECT source: %v", ErrInvalidStatement, err)
	}
	if source.Family != StatementFamilySelect {
		return nil, true, fmt.Errorf("%w: SELECT source expected", ErrInvalidStatement)
	}
	return &TableDDLStatement{Action: CreateStr, Targets: []TableName{target}, IfNotExists: ifNotExists, Definition: definition, TableOptions: strings.TrimSpace(query[optionsStart:optionsEnd]), AsSelect: source.AST, AsSelectSQL: sourceSQL, SelectMode: mode, Temporary: temporary, Raw: query}, true, nil
}

func tableDDLFragments(query string) (definition, options, partition string) {
	open := strings.Index(query, "(")
	if open < 0 {
		return "", strings.TrimSpace(query), extractPartitionSQL(query)
	}
	close := matchingParenthesis(query, open)
	if close < 0 {
		return "", strings.TrimSpace(query[open+1:]), extractPartitionSQL(query)
	}
	return strings.TrimSpace(query[open+1 : close]), strings.TrimSpace(query[close+1:]), extractPartitionSQL(query)
}

func extractPartitionSQL(query string) string {
	lower := strings.ToLower(query)
	if index := strings.Index(lower, "partition by"); index >= 0 {
		return strings.TrimSpace(query[index:])
	}
	return ""
}

func attachCreateSelect(statement *TableDDLStatement, query string) {
	if statement == nil || statement.Action != CreateStr {
		return
	}
	lower := strings.ToLower(query)
	if pos := indexKeywordOutsideQuotes(query, " as "); pos >= 0 {
		candidate := strings.TrimSpace(query[pos+4:])
		if parsed, err := ParseForExecution(candidate); err == nil && (parsed.Family == StatementFamilySelect) {
			statement.AsSelect = parsed.AST
			return
		}
	}
	if pos := indexKeywordOutsideQuotes(lower, " select "); pos >= 0 {
		candidate := strings.TrimSpace(query[pos+1:])
		if parsed, err := ParseForExecution(candidate); err == nil {
			statement.AsSelect = parsed.AST
		}
	}
}

func validateTableDDLText(query string) error {
	lower := strings.ToLower(strings.TrimSpace(query))
	if !strings.HasPrefix(lower, "create ") || !strings.Contains(lower, " table ") {
		return nil
	}
	open := strings.Index(query, "(")
	if open < 0 {
		return nil
	}
	depth := 0
	var quote byte
	closeAt := -1
	for i := open; i < len(query); i++ {
		ch := query[i]
		if quote != 0 {
			if ch == quote && (i == 0 || query[i-1] != '\\') {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			continue
		}
		if ch == '(' {
			depth++
		}
		if ch == ')' {
			depth--
			if depth == 0 {
				closeAt = i
				break
			}
		}
	}
	if closeAt < 0 {
		return fmt.Errorf("%w: unclosed CREATE TABLE definition", ErrInvalidStatement)
	}
	tail := strings.TrimSpace(query[closeAt+1:])
	if tail == "" {
		return nil
	}
	first := strings.ToLower(strings.Fields(tail)[0])
	for _, allowed := range []string{"engine", "default", "character", "charset", "collate", "comment", "auto_increment", "row_format", "key_block_size", "partition", "with", "as"} {
		if first == allowed {
			return nil
		}
	}
	return fmt.Errorf("%w: unexpected CREATE TABLE token %q", ErrInvalidStatement, first)
}

func extractCreateLikeTarget(query string) TableName {
	tokens, err := scanShowExecutionTokens(query)
	if err != nil {
		return TableName{}
	}
	for i := 0; i+1 < len(tokens); i++ {
		if strings.EqualFold(tokens[i].text, "like") {
			if target, _, ok := parseShowQualifiedName(tokens, i+1); ok {
				return target
			}
		}
	}
	return TableName{}
}

func indexKeywordOutsideQuotes(input, keyword string) int {
	var quote byte
	for i := 0; i+len(keyword) <= len(input); i++ {
		if quote != 0 {
			if input[i] == quote && (i == 0 || input[i-1] != '\\') {
				quote = 0
			}
			continue
		}
		if input[i] == '\'' || input[i] == '"' || input[i] == '`' {
			quote = input[i]
			continue
		}
		if strings.EqualFold(input[i:i+len(keyword)], keyword) {
			return i
		}
	}
	return -1
}
