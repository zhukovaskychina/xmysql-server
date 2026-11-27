package engine

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// evaluateMetadataPredicate evaluates a parsed metadata WHERE expression
// using the same SQL three-valued evaluator as table constraints and row
// filtering. Only TRUE keeps a metadata row; FALSE and UNKNOWN are filtered.
func evaluateMetadataPredicate(expr sqlparser.Expr, values map[string]interface{}) (bool, error) {
	if expr == nil {
		return true, nil
	}
	truth, err := evalPredicateTruth(expr, values)
	if err != nil {
		return false, err
	}
	return truth == sqlTruthTrue, nil
}

func metadataPredicateFromQuery(query string) (sqlparser.Expr, error) {
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		return nil, err
	}
	switch stmt := parsed.AST.(type) {
	case *sqlparser.Select:
		if stmt.Where == nil {
			return nil, nil
		}
		return stmt.Where.Expr, nil
	case *sqlparser.Union:
		return nil, fmt.Errorf("metadata predicate on UNION requires a single branch")
	default:
		return nil, fmt.Errorf("metadata predicate requires SELECT, got %T", parsed.AST)
	}
}
