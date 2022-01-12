package engine

import (
	"github.com/juju/errors"
	"xmysql-server/server/innodb/basic"
	"xmysql-server/server/innodb/context"
	"xmysql-server/server/innodb/expression"
)

type baseCursor struct {
	children []basic.Cursor
	ctx      context.Context
}

func (b baseCursor) Open() error {
	for _, child := range b.children {
		err := child.Open()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (b *baseCursor) Close() error {
	for _, child := range b.children {
		err := child.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func NewBaseCursor(ctx context.Context, children ...basic.Cursor) baseCursor {
	return baseCursor{
		children: children,
		ctx:      ctx,
	}
}

type SelectionExec struct {
	baseCursor
	Conditions []expression.Expression
}

func (s SelectionExec) Open() error {
	if err := s.baseCursor.Open(); err != nil {
		return err
	}
	return s.open(s.ctx)
}

func (s SelectionExec) open(ctx context.Context) error {

	return nil
}

func (s SelectionExec) GetRow() basic.Row {
	return s.children[0].GetRow()
}

func (s SelectionExec) Next() bool {
	for {
		hasNext := s.children[0].Next()
		if !hasNext {
			return hasNext
		}
		match, err := expression.EvalBool(s.Conditions, s.GetRow().ToDatum(), s.ctx)
		if err != nil {
			return false
		}
		if match {
			return true
		}
	}
}

func (s SelectionExec) Type() string {
	panic("implement me")
}

func (s SelectionExec) CursorName() string {
	panic("implement me")
}

type ProjectionExec struct {
	baseCursor
	exprs []expression.Expression
}

func (p ProjectionExec) GetRow() basic.Row {
	return p.children[0].GetRow()
}

func (p ProjectionExec) Next() bool {
	hasNext := p.children[0].Next()
	if !hasNext {
		return hasNext
	}

	return hasNext
}

func (p ProjectionExec) Type() string {
	panic("implement me")
}

func (p ProjectionExec) CursorName() string {
	panic("implement me")
}

//
//func (e *ProjectionExec) Next() (retRow Row, err error) {
//	srcRow, err := e.children[0].Next()
//	if err != nil {
//		return nil, errors.Trace(err)
//	}
//	if srcRow == nil {
//		return nil, nil
//	}
//	row := make([]basic.Datum, 0, len(e.exprs))
//	for _, expr := range e.exprs {
//		val, err := expr.Eval(srcRow)
//		if err != nil {
//			return nil, errors.Trace(err)
//		}
//		row = append(row, val)
//	}
//	return row, nil
//}
