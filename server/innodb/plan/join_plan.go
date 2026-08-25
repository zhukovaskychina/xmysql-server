package plan

import (
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

type JoinPlan struct {
	JoinType string
	LeftPlan Plan

	RightPlan Plan
}

func (j JoinPlan) GetPlanId() int {
	if j.LeftPlan != nil && j.RightPlan != nil {
		return j.LeftPlan.GetPlanId()*31 + j.RightPlan.GetPlanId()
	}
	return 0
}

func (j JoinPlan) GetExtraInfo() string {
	if j.LeftPlan == nil || j.RightPlan == nil {
		return "unsupported join plan with missing child plans"
	}
	return "joinType=" + j.JoinType + ", left=" + j.LeftPlan.ToString() + ", right=" + j.RightPlan.ToString()
}

func (j JoinPlan) GetPlanAccessType() string {
	return "JOIN"
}

func NewJoinPlan() Plan {
	var joinPlan = new(JoinPlan)

	return joinPlan
}

func (j JoinPlan) GetEstimateBlocks() int64 {
	leftBlocks := int64(0)
	rightBlocks := int64(0)
	if j.LeftPlan != nil {
		leftBlocks = j.LeftPlan.GetEstimateBlocks()
	}
	if j.RightPlan != nil {
		rightBlocks = j.RightPlan.GetEstimateBlocks()
	}
	return leftBlocks + rightBlocks + 1
}

func (j JoinPlan) GetEstimateRows() int64 {
	leftRows := int64(0)
	rightRows := int64(0)
	if j.LeftPlan != nil {
		leftRows = j.LeftPlan.GetEstimateRows()
	}
	if j.RightPlan != nil {
		rightRows = j.RightPlan.GetEstimateRows()
	}
	if leftRows == 0 {
		return rightRows
	}
	if rightRows == 0 {
		return leftRows
	}
	return leftRows * rightRows
}

func (j JoinPlan) Scan(session server.MySQLServerSession) basic.Cursor {
	return &noopPlanCursor{}
}

func (j JoinPlan) ToString() string {
	return "JoinPlan(" + j.JoinType + ")"
}
