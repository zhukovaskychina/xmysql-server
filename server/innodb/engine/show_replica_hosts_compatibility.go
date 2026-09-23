package engine

import (
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

func isShowReplicaHostsQuery(query string) bool {
	q := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	return q == "show replicas" || q == "show slave hosts" || q == "show replica hosts"
}

// executeShowReplicaHosts exposes the non-sensitive registration metadata
// collected by COM_REGISTER_SLAVE/COM_REGISTER_REPLICA. MySQL's result shape
// is intentionally kept to the legacy five-column SHOW SLAVE HOSTS contract.
func (e *XMySQLExecutor) executeShowReplicaHosts(ctx *ExecutionContext) {
	columns := []string{"Server_id", "Host", "Port", "Master_id", "Slave_UUID"}
	rows := make([][]interface{}, 0)
	if e != nil && e.replicaRegistrations != nil {
		for _, registration := range e.replicaRegistrations() {
			rows = append(rows, []interface{}{
				registration.ServerID,
				registration.ReportHost,
				registration.ReportPort,
				registration.MasterID,
				"",
			})
		}
	}
	result := newInformationSchemaSelectResult("replica_hosts", columns, rows)
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: "Replica hosts returned"}
}
