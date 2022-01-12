package engine

import (
	"xmysql-server/server/innodb/basic"
	"xmysql-server/server/innodb/context"
	"xmysql-server/server/innodb/plan"
	"xmysql-server/server/innodb/schemas"
)

type cursorBuilder struct {
	infoSchema schemas.InfoSchema
	ctx        context.Context
}

func NewCursorBuilder(ctx context.Context, schema schemas.InfoSchema) *cursorBuilder {
	return &cursorBuilder{ctx: ctx, infoSchema: schema}
}

func (b *cursorBuilder) build(currentPlan plan.Plan) basic.Cursor {

	switch v := currentPlan.(type) {

	case *plan.Selection:
		{
			return b.buildSelection(v)
		}
	case *plan.Projection:
		{
			return b.buildProjection(v)
		}
	default:

		return nil
	}
	return nil
}

func (b *cursorBuilder) buildSelection(v *plan.Selection) basic.Cursor {
	return &SelectionExec{
		baseCursor: NewBaseCursor(b.ctx, b.build(v.Children()[0])),
		Conditions: v.Conditions,
	}
}

func (b *cursorBuilder) buildProjection(v *plan.Projection) basic.Cursor {
	return &ProjectionExec{
		baseCursor: NewBaseCursor(b.ctx, b.build(v.Children()[0])),
		exprs:      v.Exprs,
	}
}
