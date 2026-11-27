package engine

import (
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// ensureParsedStatement parses one statement once for an execution context.
// It is intentionally not wired into the global executor path yet; staged
// statement families opt in after their AST and execution adapters are ready.
func (e *XMySQLExecutor) ensureParsedStatement(ctx *ExecutionContext, query string) (*sqlparser.ParsedStatement, error) {
	if ctx != nil && ctx.Parsed != nil && ctx.RawQuery == query {
		return ctx.Parsed, nil
	}
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		return nil, err
	}
	if stored, ok := parsed.AST.(*sqlparser.StoredObjectStatement); ok && strings.EqualFold(stored.ObjectType, "call") {
		return nil, fmt.Errorf("%w: CALL execution remains on the routine compatibility path", sqlparser.ErrUnsupportedStatement)
	}
	if ctx != nil {
		ctx.RawQuery = query
		ctx.Parsed = parsed
	}
	return parsed, nil
}
