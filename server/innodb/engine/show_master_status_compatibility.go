package engine

import (
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

func isShowMasterStatusQuery(query string) bool {
	q := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	return q == "show master status" || q == "show source status"
}

// executeShowMasterStatus exposes the local source position and executed GTID
// set using the column contract expected by replication tooling.
func (e *XMySQLExecutor) executeShowMasterStatus(ctx *ExecutionContext) {
	columns := []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}
	rows := [][]interface{}{}
	if e != nil && e.replicationSource != nil {
		if source := e.replicationSource(); source != nil {
			file, position := source.NativeCurrentFilePosition()
			if file == "" {
				file = "binlog.000001"
			}
			rows = append(rows, []interface{}{file, position, "", "", source.Executed.String()})
		}
	}
	result := newInformationSchemaSelectResult("master_status", columns, rows)
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: "Master status returned"}
}
