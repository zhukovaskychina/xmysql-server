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
			case "flush", "kill", "unlock":
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
	modifier := strings.ToLower(tokens[1].text)
	if modifier != "persist" && modifier != "persist_only" {
		return nil, fmt.Errorf("%w: SET syntax", ErrInvalidStatement)
	}
	marker := " " + modifier + " "
	lower := strings.ToLower(query)
	position := strings.Index(lower, marker)
	if position < 0 {
		return nil, fmt.Errorf("%w: SET syntax", ErrInvalidStatement)
	}
	inner, parseErr := Parse(strings.TrimSpace(query[:position] + " " + query[position+len(marker):]))
	if parseErr != nil {
		return nil, parseErr
	}
	setStmt, ok := inner.(*Set)
	if !ok {
		return nil, fmt.Errorf("%w: SET AST %T", ErrInvalidStatement, inner)
	}
	setStmt.Persist = true
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
		_, ok := stmt.(*TableDDLStatement)
		return ok
	default:
		return false
	}
}
