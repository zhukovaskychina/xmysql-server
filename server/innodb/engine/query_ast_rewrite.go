package engine

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// rewriteStatementAST returns an independent AST copy.  Parsing the formatted
// node is intentional: it gives callers a fresh tree and keeps aliases,
// literals, CTEs and nested expressions in parser ownership rather than doing
// textual replacement in the execution layer.
func rewriteStatementAST(stmt sqlparser.Statement) (sqlparser.Statement, error) {
	if stmt == nil {
		return nil, fmt.Errorf("cannot rewrite nil SQL statement")
	}
	copy, err := sqlparser.Parse(sqlparser.String(stmt))
	if err != nil {
		return nil, err
	}
	if copy == nil {
		return nil, fmt.Errorf("parser returned nil rewritten statement")
	}
	return copy, nil
}
