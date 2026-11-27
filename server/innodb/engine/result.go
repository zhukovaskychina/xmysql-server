package engine

import (
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

type Result struct {
	StatementID  int64
	Rows         basic.Rows
	Data         interface{}
	Err          error
	ResultType   string
	Message      string // Status or informational message
	AffectedRows int
	LastInsertID uint64
	Warnings     []Warning
}

type statementResultAccounting struct {
	rowsAffected int64
	rowsSent     int64
	warnings     int64
}

type statementExecutionSummary struct {
	threadID            int64
	user                string
	host                string
	statementType       string
	status              string
	latency             time.Duration
	rowsExamined        int64
	selectScan          int64
	selectRange         int64
	selectFullJoin      int64
	selectFullRangeJoin int64
	selectRangeCheck    int64
	noIndexUsed         int64
	noGoodIndexUsed     int64
	sortRows            int64
	sortScan            int64
	sortRange           int64
	cpuTime             int64
	cpuTimeCaptured     bool
	indexNames          []string
}

func statementResultAccountingFor(result *Result) statementResultAccounting {
	if result == nil {
		return statementResultAccounting{}
	}
	accounting := statementResultAccounting{
		rowsAffected: int64(result.AffectedRows),
		warnings:     int64(len(result.Warnings)),
	}
	switch data := result.Data.(type) {
	case *SelectResult:
		if data != nil {
			accounting.rowsSent = int64(data.RowCount)
		}
	case SelectResult:
		accounting.rowsSent = int64(data.RowCount)
	}
	return accounting
}

type Warning struct {
	Level   string
	Code    uint16
	Message string
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
