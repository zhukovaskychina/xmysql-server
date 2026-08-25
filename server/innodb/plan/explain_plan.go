package plan

import (
	"bytes"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

type ExplainPlan struct {
	bufferString bytes.Buffer
	plan         Plan
}

func (e *ExplainPlan) GetPlanId() int {
	if e.plan == nil {
		return 0
	}
	return e.plan.GetPlanId()
}

func (e *ExplainPlan) GetExtraInfo() string {
	if e.plan == nil {
		return "unsupported explain plan: missing child plan"
	}
	return "explain of " + e.plan.ToString()
}

func (e *ExplainPlan) GetPlanAccessType() string {
	if e.plan == nil {
		return "EXPLAIN"
	}
	return e.plan.GetPlanAccessType()
}

func NewExplainPlan(plan Plan) Plan {
	var explainPlan = new(ExplainPlan)
	explainPlan.plan = plan
	return explainPlan
}

func (e *ExplainPlan) GetEstimateBlocks() int64 {
	if e.plan == nil {
		return 0
	}
	return e.plan.GetEstimateBlocks()
}

func (e *ExplainPlan) GetEstimateRows() int64 {
	if e.plan == nil {
		return 0
	}
	return e.plan.GetEstimateRows()
}

func (e *ExplainPlan) Scan(session server.MySQLServerSession) basic.Cursor {
	return &noopPlanCursor{}
	//return scan.NewExplainScan(e.Scan(), e.ToString())
}

func (e *ExplainPlan) ToString() string {
	if e.plan == nil {
		return "EXPLAIN <nil>"
	}
	return "EXPLAIN " + e.plan.ToString()
}
