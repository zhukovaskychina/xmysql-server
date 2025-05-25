package engine

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

type Result struct {
	StatementID int64
	Rows        basic.Rows
	Data        interface{}
	Err         error
	ResultType  string
	Message     string // Status or informational message
}

func NewResult() *Result {
	var rows = make([]basic.Row, 0)
	return &Result{
		StatementID: 0,
		Rows:        rows,
		Data:        nil,
		Err:         nil,
		Message:     "",
	}
}

func (result *Result) AddRows(row basic.Row) {
	result.Rows.AddRow(row)
}
