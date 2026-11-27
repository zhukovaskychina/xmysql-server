package sqlparser

import (
	"errors"
	"fmt"
	"strings"
)

// StatementFamily identifies the top-level SQL family understood by the
// execution parser. It is deliberately independent from the concrete AST
// node, because execution support is enabled family by family.
type StatementFamily string

const (
	StatementFamilyUnknown      StatementFamily = "unknown"
	StatementFamilySelect       StatementFamily = "select"
	StatementFamilyInsert       StatementFamily = "insert"
	StatementFamilyUpdate       StatementFamily = "update"
	StatementFamilyDelete       StatementFamily = "delete"
	StatementFamilySet          StatementFamily = "set"
	StatementFamilyUse          StatementFamily = "use"
	StatementFamilyTransaction  StatementFamily = "transaction"
	StatementFamilyShow         StatementFamily = "show"
	StatementFamilyExplain      StatementFamily = "explain"
	StatementFamilyPrepared     StatementFamily = "prepared"
	StatementFamilyHandler      StatementFamily = "handler"
	StatementFamilyComponent    StatementFamily = "component"
	StatementFamilyAccount      StatementFamily = "account"
	StatementFamilyView         StatementFamily = "view"
	StatementFamilyStoredObject StatementFamily = "stored_object"
	StatementFamilyAlter        StatementFamily = "alter"
	StatementFamilyAdmin        StatementFamily = "admin"
	StatementFamilyDDL          StatementFamily = "ddl"
)

// ErrUnsupportedStatement means the parser can identify the SQL family but
// that family is not enabled by the execution parser yet.
var ErrUnsupportedStatement = errors.New("unsupported SQL statement")

// ErrInvalidStatement means an enabled SQL family failed complete parsing.
var ErrInvalidStatement = errors.New("invalid SQL statement")

// ParsedStatement is the immutable parse result shared by authorization,
// locking, planning and execution. RawSQL is retained for diagnostics and
// persistence compatibility; AST is the source of structured semantics.
type ParsedStatement struct {
	AST      Statement
	RawSQL   string
	Family   StatementFamily
	Complete bool
}

// ClassifyStatement reads the first meaningful token without using a regular
// expression or a raw string prefix. Classification alone does not validate
// the remainder of the statement.
func ClassifyStatement(sql string) (StatementFamily, error) {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return StatementFamilyUnknown, fmt.Errorf("%w: empty input", ErrInvalidStatement)
	}
	tokenizer := NewStringTokenizer(trimmed)
	for {
		token, value := tokenizer.Scan()
		switch token {
		case COMMENT, COMMENT_KEYWORD:
			continue
		case 0, eofChar:
			return StatementFamilyUnknown, fmt.Errorf("%w: empty input", ErrInvalidStatement)
		case SELECT, WITH:
			return StatementFamilySelect, nil
		case INSERT:
			return StatementFamilyInsert, nil
		case UPDATE:
			return StatementFamilyUpdate, nil
		case DELETE:
			return StatementFamilyDelete, nil
		case SET:
			return StatementFamilySet, nil
		case USE:
			return StatementFamilyUse, nil
		case BEGIN, COMMIT, ROLLBACK:
			return StatementFamilyTransaction, nil
		case SHOW:
			return StatementFamilyShow, nil
		case DESC, DESCRIBE, EXPLAIN:
			return StatementFamilyExplain, nil
		case ALTER:
			return StatementFamilyAlter, nil
		case CREATE, DROP, RENAME, TRUNCATE:
			return StatementFamilyDDL, nil
		case ANALYZE, REPAIR, OPTIMIZE, LOCK:
			return StatementFamilyAdmin, nil
		default:
			switch strings.ToLower(string(value)) {
			case "flush", "kill", "unlock", "check":
				return StatementFamilyAdmin, nil
			case "reset", "start", "stop", "change", "purge":
				return StatementFamilyAdmin, nil
			case "prepare", "execute", "deallocate":
				return StatementFamilyPrepared, nil
			case "handler":
				return StatementFamilyHandler, nil
			case "install", "uninstall":
				return StatementFamilyComponent, nil
			}
			return StatementFamilyUnknown, fmt.Errorf("%w: token %s", ErrUnsupportedStatement, KeywordString(token))
		}
	}
}

// ParseForExecution parses only families whose AST is currently complete
// enough to be shared with execution. Other families fail explicitly instead
// of falling back to a partial DDL or OtherAdmin node.
func ParseForExecution(sql string) (*ParsedStatement, error) {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return nil, fmt.Errorf("%w: empty input", ErrInvalidStatement)
	}
	if tablespace, matched, tablespaceErr := tryParseTablespaceForExecution(trimmed); matched {
		if tablespaceErr != nil {
			return nil, tablespaceErr
		}
		return &ParsedStatement{AST: tablespace, RawSQL: sql, Family: StatementFamilyDDL, Complete: true}, nil
	}
	if indexDDL, matched, indexErr := tryParseIndexDDLForExecution(trimmed); matched {
		if indexErr != nil {
			return nil, indexErr
		}
		return &ParsedStatement{AST: indexDDL, RawSQL: sql, Family: StatementFamilyDDL, Complete: true}, nil
	}
	if view, matched, viewErr := tryParseViewForExecution(trimmed); matched {
		if viewErr != nil {
			return nil, viewErr
		}
		return &ParsedStatement{AST: view, RawSQL: sql, Family: StatementFamilyView, Complete: true}, nil
	}
	if stored, matched, storedErr := tryParseStoredObjectForExecution(trimmed); matched {
		if storedErr != nil {
			return nil, storedErr
		}
		return &ParsedStatement{AST: stored, RawSQL: sql, Family: StatementFamilyStoredObject, Complete: true}, nil
	}
	if account, matched, accountErr := tryParseAccountForExecution(trimmed); matched {
		if accountErr != nil {
			return nil, accountErr
		}
		return &ParsedStatement{AST: account, RawSQL: sql, Family: StatementFamilyAccount, Complete: true}, nil
	}
	family, err := ClassifyStatement(trimmed)
	if err != nil {
		return nil, err
	}
	switch family {
	case StatementFamilySelect, StatementFamilyInsert, StatementFamilyUpdate,
		StatementFamilyDelete, StatementFamilySet, StatementFamilyUse,
		StatementFamilyTransaction:
		// These are the parser families with complete top-level ASTs in the
		// current migration step. CTE and window SELECTs are handled by Parse.
	case StatementFamilyShow:
		show, showErr := parseShowForExecution(trimmed)
		if showErr != nil {
			return nil, showErr
		}
		return &ParsedStatement{AST: show, RawSQL: sql, Family: family, Complete: true}, nil
	case StatementFamilyExplain:
		explain, explainErr := parseExplainForExecution(trimmed)
		if explainErr != nil {
			return nil, explainErr
		}
		return &ParsedStatement{AST: explain, RawSQL: sql, Family: family, Complete: true}, nil
	case StatementFamilyAlter:
		alter, alterErr := parseAlterForExecution(trimmed)
		if alterErr != nil {
			return nil, alterErr
		}
		return &ParsedStatement{AST: alter, RawSQL: sql, Family: family, Complete: true}, nil
	case StatementFamilyAdmin:
		admin, adminErr := parseAdminForExecution(trimmed)
		if adminErr != nil {
			return nil, adminErr
		}
		return &ParsedStatement{AST: admin, RawSQL: sql, Family: family, Complete: true}, nil
	case StatementFamilyPrepared:
		prepared, preparedErr := parsePreparedForExecution(trimmed)
		if preparedErr != nil {
			return nil, preparedErr
		}
		return &ParsedStatement{AST: prepared, RawSQL: sql, Family: family, Complete: true}, nil
	case StatementFamilyHandler:
		// The generic execution contract keeps the legacy HANDLER family
		// unsupported. The engine compatibility path calls ParseHandlerForExecution
		// directly for MySQL's table-first syntax.
		fields := strings.Fields(trimmed)
		if len(fields) > 1 && !strings.EqualFold(fields[1], "open") && !strings.EqualFold(fields[1], "close") && !strings.EqualFold(fields[1], "read") {
			return nil, fmt.Errorf("%w: handler", ErrUnsupportedStatement)
		}
		handler, handlerErr := parseHandlerForExecution(trimmed)
		if handlerErr != nil {
			return nil, handlerErr
		}
		return &ParsedStatement{AST: handler, RawSQL: sql, Family: family, Complete: true}, nil
	case StatementFamilyComponent:
		component, componentErr := parseComponentForExecution(trimmed)
		if componentErr != nil {
			return nil, componentErr
		}
		return &ParsedStatement{AST: component, RawSQL: sql, Family: family, Complete: true}, nil
	case StatementFamilyDDL:
		ddl, ddlErr := parseTableDDLForExecution(trimmed)
		if ddlErr != nil {
			return nil, ddlErr
		}
		return &ParsedStatement{AST: ddl, RawSQL: sql, Family: family, Complete: true}, nil
	case StatementFamilyAccount:
		return nil, fmt.Errorf("%w: account statement classification mismatch", ErrInvalidStatement)
	default:
		return nil, fmt.Errorf("%w: family %s", ErrUnsupportedStatement, family)
	}

	stmt, parseErr := Parse(trimmed)
	if family == StatementFamilySet && parseErr != nil {
		if setStmt, setErr := parseSetCompatibilityForExecution(trimmed); setErr == nil {
			return &ParsedStatement{AST: setStmt, RawSQL: sql, Family: family, Complete: true}, nil
		}
	}
	if parseErr != nil || stmt == nil {
		if parseErr == nil {
			parseErr = errors.New("parser returned no statement")
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidStatement, parseErr)
	}
	if !isExecutionFamilyAST(family, stmt) {
		return nil, fmt.Errorf("%w: family %s produced AST %T", ErrInvalidStatement, family, stmt)
	}
	if family == StatementFamilySet {
		enrichSetOptions(stmt.(*Set), trimmed)
	}
	return &ParsedStatement{AST: stmt, RawSQL: sql, Family: family, Complete: true}, nil
}

func parseSetCompatibilityForExecution(query string) (*Set, error) {
	tokens, err := scanShowExecutionTokens(query)
	if err != nil || len(tokens) < 4 {
		return nil, fmt.Errorf("%w: SET syntax", ErrInvalidStatement)
	}
	if len(tokens) >= 6 && strings.EqualFold(tokens[0].text, "set") && strings.EqualFold(tokens[1].text, "new") && tokens[2].text == "." && tokens[4].text == "=" {
		column := strings.Trim(tokens[3].text, "`")
		if column == "" {
			return nil, fmt.Errorf("%w: SET NEW column missing", ErrInvalidStatement)
		}
		expressionStart := tokens[5].start
		if expressionStart < 0 || expressionStart >= len(query) {
			return nil, fmt.Errorf("%w: SET NEW expression missing", ErrInvalidStatement)
		}
		expressionSQL := strings.TrimSpace(query[expressionStart:])
		parsed, parseErr := Parse("select " + expressionSQL)
		if parseErr != nil {
			return nil, parseErr
		}
		selectStmt, ok := parsed.(*Select)
		if !ok || len(selectStmt.SelectExprs) != 1 {
			return nil, fmt.Errorf("%w: SET NEW expression", ErrInvalidStatement)
		}
		aliased, ok := selectStmt.SelectExprs[0].(*AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("%w: SET NEW expression", ErrInvalidStatement)
		}
		return &Set{Exprs: SetExprs{&SetExpr{Name: NewColIdent("new." + column), Expr: aliased.Expr}}}, nil
	}
	modifier := strings.ToLower(tokens[1].text)
	if tokens[1].kind != ID || (modifier != "persist" && modifier != "persist_only") {
		return nil, fmt.Errorf("%w: SET syntax", ErrInvalidStatement)
	}
	start, end := tokens[1].start, tokens[1].end
	if start < 0 || end > len(query) || start >= end || query[start] == '`' {
		return nil, fmt.Errorf("%w: SET syntax", ErrInvalidStatement)
	}
	inner, parseErr := Parse(query[:start] + " " + query[end:])
	if parseErr != nil {
		return nil, parseErr
	}
	setStmt, ok := inner.(*Set)
	if !ok {
		return nil, fmt.Errorf("%w: SET AST %T", ErrInvalidStatement, inner)
	}
	setStmt.Persist = true
	setStmt.PersistOnly = modifier == "persist_only"
	return setStmt, nil
}

func isExecutionFamilyAST(family StatementFamily, stmt Statement) bool {
	switch family {
	case StatementFamilySelect:
		switch stmt.(type) {
		case *Select, *Union, *With:
			return true
		default:
			return false
		}
	case StatementFamilyInsert:
		_, ok := stmt.(*Insert)
		return ok
	case StatementFamilyUpdate:
		_, ok := stmt.(*Update)
		return ok
	case StatementFamilyDelete:
		_, ok := stmt.(*Delete)
		return ok
	case StatementFamilySet:
		_, ok := stmt.(*Set)
		return ok
	case StatementFamilyUse:
		_, ok := stmt.(*Use)
		return ok
	case StatementFamilyTransaction:
		switch stmt.(type) {
		case *Begin, *Commit, *Rollback:
			return true
		default:
			return false
		}
	case StatementFamilyShow:
		_, ok := stmt.(*Show)
		return ok
	case StatementFamilyExplain:
		_, ok := stmt.(*ExplainStatement)
		return ok
	case StatementFamilyAlter:
		_, ok := stmt.(*AlterTable)
		return ok
	case StatementFamilyAdmin:
		_, ok := stmt.(*AdminStatement)
		return ok
	case StatementFamilyPrepared:
		_, ok := stmt.(*PreparedStatement)
		return ok
	case StatementFamilyHandler:
		_, ok := stmt.(*HandlerStatement)
		return ok
	case StatementFamilyComponent:
		_, ok := stmt.(*ComponentStatement)
		return ok
	case StatementFamilyAccount:
		_, ok := stmt.(*AccountStatement)
		return ok
	case StatementFamilyView:
		_, ok := stmt.(*ViewStatement)
		return ok
	case StatementFamilyStoredObject:
		_, ok := stmt.(*StoredObjectStatement)
		return ok
	case StatementFamilyDDL:
		switch stmt.(type) {
		case *TableDDLStatement, *TablespaceStatement, *IndexDDLStatement:
			return true
		default:
			return false
		}
	default:
		return false
	}
}
