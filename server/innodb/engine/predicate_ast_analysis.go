package engine

import (
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// predicateASTColumns collects real column references from an expression. The
// parser Walk implementation skips string literals, so dependency analysis
// cannot mistake text containing a column name for a dependency.
func predicateASTColumns(expr sqlparser.Expr) []string {
	seen := make(map[string]struct{})
	var columns []string
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if column, ok := node.(*sqlparser.ColName); ok {
			name := strings.ToLower(column.Name.String())
			if name != "" {
				if _, exists := seen[name]; !exists {
					seen[name] = struct{}{}
					columns = append(columns, name)
				}
			}
		}
		return true, nil
	}, expr)
	return columns
}

// predicateASTAllowsPruning is deliberately conservative. It only permits a
// caller to narrow a scan when every branch is composed of known column/value
// comparisons; unknown functions, subqueries and parameter markers retain the
// full scan and are evaluated by the residual predicate.
func predicateASTAllowsPruning(expr sqlparser.Expr) bool {
	if expr == nil {
		return false
	}
	allowed := true
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch value := node.(type) {
		case *sqlparser.FuncExpr, *sqlparser.Subquery, *sqlparser.ListArg:
			allowed = false
		case *sqlparser.SQLVal:
			if len(value.Val) == 0 || string(value.Val) == "?" {
				allowed = false
			}
		}
		return allowed, nil
	}, expr)
	return allowed
}
