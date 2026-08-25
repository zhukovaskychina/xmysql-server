package plan

import "github.com/zhukovaskychina/xmysql-server/server/innodb/basic"

type noopPlanCursor struct{}

func (n *noopPlanCursor) Open() error {
	return nil
}

func (n *noopPlanCursor) GetRow() basic.Row {
	return nil
}

func (n *noopPlanCursor) Next() bool {
	return false
}

func (n *noopPlanCursor) Close() error {
	return nil
}

func (n *noopPlanCursor) Type() string {
	return "noop"
}

func (n *noopPlanCursor) CursorName() string {
	return "noop_plan_cursor"
}
