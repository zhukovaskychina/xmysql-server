package plan

import (
	"xmysql-server/server"
	"xmysql-server/server/innodb/basic"
)

type JoinPlan struct {
	JoinType string
	LeftPlan Plan

	RightPlan Plan
}

func (j JoinPlan) GetPlanId() int {
	panic("implement me")
}

func (j JoinPlan) GetExtraInfo() string {
	panic("implement me")
}

func (j JoinPlan) GetPlanAccessType() string {
	panic("implement me")
}

func NewJoinPlan() Plan {
	var joinPlan = new(JoinPlan)

	return joinPlan
}

func (j JoinPlan) GetEstimateBlocks() int64 {
	panic("implement me")
}

func (j JoinPlan) GetEstimateRows() int64 {
	panic("implement me")
}

func (j JoinPlan) Scan(session server.MySQLServerSession) basic.Cursor {
	panic("implement me")
}

func (j JoinPlan) ToString() string {
	panic("implement me")
}
