package plan

import (
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

type Plan interface {
	GetPlanId() int

	//此次计划读取的物理块数
	GetEstimateBlocks() int64

	//获取读取的预估行数
	GetEstimateRows() int64

	//开始扫描
	Scan(session server.MySQLServerSession) basic.Cursor

	ToString() string

	GetExtraInfo() string

	//ALL, index,  range, ref, eq_ref, const, system, NULL
	GetPlanAccessType() string
}
