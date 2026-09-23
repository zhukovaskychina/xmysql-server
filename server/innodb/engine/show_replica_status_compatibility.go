package engine

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func isShowReplicaStatusQuery(query string) bool {
	q := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	return q == "show replica status" || q == "show slave status"
}

func (e *XMySQLExecutor) executeShowReplicaStatus(ctx *ExecutionContext) {
	columns := []string{
		"Replica_IO_State", "Source_Host", "Source_Port", "Replica_IO_Running", "Replica_SQL_Running",
		"Seconds_Behind_Source", "Last_IO_Error", "Last_SQL_Error", "Retrieved_Gtid_Set", "Executed_Gtid_Set",
		"Source_Log_Pos", "Read_Source_Log_Pos", "Auto_Position",
	}
	if e == nil || e.replicationStatus == nil {
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: newInformationSchemaSelectResult("replica_status", columns, nil), Message: "No replication status available"}
		return
	}
	status := e.replicationStatus()
	if status.Role != replication.RoleReplica {
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: newInformationSchemaSelectResult("replica_status", columns, nil), Message: "No replication status available"}
		return
	}

	host, port := replicationSourceHostPort(status.SourceURL)
	running := "Yes"
	if status.ReplicaRunningKnown && !status.ReplicaRunning {
		running = "No"
	}
	if strings.TrimSpace(status.LastError) != "" {
		running = "No"
	}
	lag := fmt.Sprintf("%g", status.ReplicationLagSec)
	row := []interface{}{
		mapReplicaState(running, status.LastError), host, port, running, running,
		lag, status.LastError, status.LastError, status.ExecutedGTIDs, status.ExecutedGTIDs,
		status.SourcePosition, status.SourcePosition, boolToMySQL(status.ExecutedGTIDs != ""),
	}
	result := newInformationSchemaSelectResult("replica_status", columns, [][]interface{}{row})
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: "Replication status returned"}
}

func replicationSourceHostPort(raw string) (string, int) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Hostname() == "" {
		return "", 0
	}
	port := 0
	if parsed.Port() != "" {
		port, _ = strconv.Atoi(parsed.Port())
	}
	return parsed.Hostname(), port
}

func mapReplicaState(running, lastError string) string {
	if running == "Yes" {
		return "Waiting for source to send event"
	}
	return lastError
}

func boolToMySQL(value bool) int {
	if value {
		return 1
	}
	return 0
}
