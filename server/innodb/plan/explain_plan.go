package plan

import (
	"bytes"
	"xmysql-server/server"
	"xmysql-server/server/innodb/basic"
)

type ExplainPlan struct {
	bufferString bytes.Buffer
	plan         Plan
}

func (e *ExplainPlan) GetPlanId() int {
	panic("implement me")
}

func (e *ExplainPlan) GetExtraInfo() string {
	panic("implement me")
}

func (e *ExplainPlan) GetPlanAccessType() string {
	panic("implement me")
}

func NewExplainPlan(plan Plan) Plan {
	var explainPlan = new(ExplainPlan)
	explainPlan.plan = plan
	return explainPlan
}

func (e *ExplainPlan) GetEstimateBlocks() int64 {
	panic("implement me")
}

func (e *ExplainPlan) GetEstimateRows() int64 {
	panic("implement me")
}

func (e *ExplainPlan) Scan(session server.MySQLServerSession) basic.Cursor {
	panic("implement me")
	//return scan.NewExplainScan(e.Scan(), e.ToString())
}

func (e *ExplainPlan) ToString() string {
	panic("implement me")
}
