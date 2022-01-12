package engine

import (
	"github.com/juju/errors"
	"xmysql-server/server/innodb/ast"
	"xmysql-server/server/innodb/context"
	"xmysql-server/server/innodb/plan"
	"xmysql-server/server/innodb/resolver"
	"xmysql-server/server/innodb/schemas"
)

// Compile is safe for concurrent use by multiple goroutines.
func Compile(ctx context.Context, rawStmt ast.StmtNode) (plan.Plan, error) {
	info := ctx.GetSessionVars().TxnCtx.InfoSchema.(schemas.InfoSchema)

	node := rawStmt
	err := resolver.ResolveName(node, info, ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p, err := plan.Optimize(ctx, node, info)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return p, nil
}

type PreparedStatement struct {
}

// runStmt executes the ast.Statement and commit or rollback the current transaction.
func runStmt(ctx context.Context, s ast.Statement) (ast.RecordSet, error) {
	var err error
	var rs ast.RecordSet
	rs, err = s.Exec(ctx)
	return rs, errors.Trace(err)
}
