package engine

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func isShowReplicaStatusQuery(query string) bool {
	q := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	if q == "show replica status" || q == "show slave status" {
		return true
	}
	_, matched := parseShowReplicaStatusChannel(query)
	return matched
}

var showReplicaStatusChannelPattern = regexp.MustCompile(`(?is)^\s*show\s+(?:replica|slave)\s+status\s+for\s+channel\s+(?:"((?:""|[^"])*)"|'((?:''|[^'])*)'|([^\s]+))\s*;?\s*$`)

func parseShowReplicaStatusChannel(query string) (channel string, matched bool) {
	match := showReplicaStatusChannelPattern.FindStringSubmatch(query)
	if len(match) != 4 {
		return "", false
	}
	channel = match[1]
	if channel == "" {
		channel = match[2]
	}
	if channel == "" {
		channel = match[3]
	}
	channel = strings.ReplaceAll(channel, `""`, `"`)
	channel = strings.ReplaceAll(channel, "''", "'")
	return strings.TrimSpace(channel), true
}

func (e *XMySQLExecutor) checkReplicationPrivilege(ctx *ExecutionContext, required string) error {
	if e == nil || ctx == nil || ctx.Session == nil {
		return nil
	}
	user, _ := ctx.Session.GetParamByName("user").(string)
	user = strings.TrimSpace(user)
	if user == "" || strings.EqualFold(user, "root") {
		return nil
	}
	if privileges, ok := ctx.Session.GetParamByName("global_privileges").([]common.PrivilegeType); ok {
		requiredPrivilege := common.ReplicationClientPriv
		if strings.EqualFold(required, "REPLICATION SLAVE") {
			requiredPrivilege = common.ReplicationSlavePriv
		}
		for _, privilege := range privileges {
			if privilege == requiredPrivilege || privilege == common.SuperPriv || privilege == common.AllPriv {
				return nil
			}
		}
	}

	host, _ := ctx.Session.GetParamByName("host").(string)
	host = strings.TrimSpace(host)
	if host == "" {
		host = "localhost"
	}
	file, err := e.accountFileForSession(ctx)
	if err != nil {
		return err
	}
	account := sessionAccount(file, ctx.Session)
	if account == nil {
		return fmt.Errorf("Access denied; user '%s'@'%s' does not exist", user, host)
	}
	grants := effectiveAccountGrants(file, *account, ctx.Session)
	if grantsContain(grants, "*.*", required) || grantsContain(grants, "*.*", "SUPER") {
		return nil
	}
	return fmt.Errorf("Access denied; you need the %s or SUPER privilege for this operation", required)
}

func (e *XMySQLExecutor) checkShowReplicaStatusPrivilege(ctx *ExecutionContext) error {
	return e.checkReplicationPrivilege(ctx, "REPLICATION CLIENT")
}

func (e *XMySQLExecutor) executeShowReplicaStatus(ctx *ExecutionContext) {
	if channel, matched := parseShowReplicaStatusChannel(ctx.RawQuery); matched && channel != "" {
		ctx.Results <- &Result{Err: fmt.Errorf("only the default replication channel is supported"), ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	if err := e.checkShowReplicaStatusPrivilege(ctx); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	columns := []string{
		"Replica_IO_State", "Source_Host", "Source_User", "Source_Port", "Connect_Retry",
		"Source_Log_File", "Read_Source_Log_Pos", "Relay_Log_File", "Relay_Log_Pos",
		"Relay_Source_Log_File", "Replica_IO_Running", "Replica_SQL_Running", "Replicate_Do_DB",
		"Replicate_Ignore_DB", "Replicate_Do_Table", "Replicate_Ignore_Table", "Replicate_Wild_Do_Table",
		"Replicate_Wild_Ignore_Table", "Last_Errno", "Last_Error", "Skip_Counter", "Exec_Source_Log_Pos",
		"Relay_Log_Space", "Until_Condition", "Until_Log_File", "Until_Log_Pos", "Source_SSL_Allowed",
		"Source_SSL_CA_File", "Source_SSL_CA_Path", "Source_SSL_Cert", "Source_SSL_Cipher", "Source_SSL_Key",
		"Seconds_Behind_Source", "Source_SSL_Verify_Server_Cert", "Last_IO_Errno", "Last_IO_Error",
		"Last_SQL_Errno", "Last_SQL_Error", "Replicate_Ignore_Server_Ids", "Source_Server_Id", "Source_UUID",
		"Source_Info_File", "SQL_Delay", "SQL_Remaining_Delay", "Replica_SQL_Running_State", "Source_Retry_Count",
		"Source_Bind", "Last_IO_Error_Timestamp", "Last_SQL_Error_Timestamp", "Source_SSL_Crl", "Source_SSL_Crlpath",
		"Retrieved_Gtid_Set", "Executed_Gtid_Set", "Auto_Position", "Replicate_Rewrite_DB", "Channel_Name",
		"Source_TLS_Version", "Source_public_key_path", "Get_Source_public_key", "Network_Namespace",
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
	parsedSource, _ := url.Parse(strings.TrimSpace(status.SourceURL))
	nativeSource := strings.EqualFold(parsedSource.Scheme, "mysql")
	running := "Yes"
	if status.ReplicaRunningKnown && !status.ReplicaRunning {
		running = "No"
	}
	if strings.TrimSpace(status.LastError) != "" {
		running = "No"
	}
	lastErrNo := interface{}(int64(0))
	lastError := interface{}(nil)
	if strings.TrimSpace(status.LastError) != "" {
		lastErrNo = status.LastErrorNumber
		lastError = status.LastError
	}
	lastIOErrNo := interface{}(int64(0))
	lastIOError := interface{}(nil)
	lastIOErrorTimestamp := interface{}(nil)
	if strings.TrimSpace(status.LastIOError) != "" || status.LastIOErrorNumber != 0 {
		lastIOErrNo = status.LastIOErrorNumber
		if strings.TrimSpace(status.LastIOError) != "" {
			lastIOError = status.LastIOError
		}
		if !status.LastIOErrorAt.IsZero() {
			lastIOErrorTimestamp = status.LastIOErrorAt
		}
	}
	lastSQLErrNo := interface{}(int64(0))
	lastSQLError := interface{}(nil)
	lastSQLErrorTimestamp := interface{}(nil)
	if strings.TrimSpace(status.LastSQLError) != "" || status.LastSQLErrorNumber != 0 {
		lastSQLErrNo = status.LastSQLErrorNumber
		if strings.TrimSpace(status.LastSQLError) != "" {
			lastSQLError = status.LastSQLError
		}
		if !status.LastSQLErrorAt.IsZero() {
			lastSQLErrorTimestamp = status.LastSQLErrorAt
		}
	}
	lag := interface{}(fmt.Sprintf("%g", status.ReplicationLagSec))
	if running != "Yes" {
		lag = nil
	}
	autoPosition := "0"
	if status.SourceAutoPosition || (!nativeSource && status.ExecutedGTIDs != "") {
		autoPosition = "1"
	}
	row := []interface{}{
		mapReplicaState(running, status.LastError), host, status.SourceUser, port, nil,
		status.SourceFile, status.SourcePosition, nil, nil, status.SourceFile,
		running, running, nil, nil, nil, nil, nil, nil,
		lastErrNo, lastError, int64(0), status.SourcePosition, nil, "None", nil, int64(0),
		"No", nil, nil, nil, nil, nil, lag, "No", lastIOErrNo, lastIOError,
		lastSQLErrNo, lastSQLError, nil, nil, status.SourceUUID, nil, int64(0), nil,
		mapReplicaState(running, status.LastError), nil, nil, lastIOErrorTimestamp, lastSQLErrorTimestamp, nil, nil,
		status.ExecutedGTIDs, status.ExecutedGTIDs, autoPosition, nil, "",
		nil, nil, int64(0), nil,
	}
	row[54] = replicationRewriteDBStatusValue(status)
	result := newInformationSchemaSelectResult("replica_status", columns, [][]interface{}{row})
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: "Replication status returned"}
}

func replicationRewriteDBStatusValue(status replication.StatusSnapshot) interface{} {
	rules := make([]string, 0)
	for _, filter := range status.ReplicationFilters {
		if !strings.EqualFold(strings.TrimSpace(filter.Name), "REPLICATE_REWRITE_DB") {
			continue
		}
		parts := strings.SplitN(filter.Rule, "->", 2)
		if len(parts) != 2 {
			continue
		}
		from := strings.TrimSpace(parts[0])
		to := strings.TrimSpace(parts[1])
		if from == "" || to == "" {
			continue
		}
		rules = append(rules, fmt.Sprintf("(%s,%s)", from, to))
	}
	if len(rules) == 0 {
		return nil
	}
	return strings.Join(rules, ",")
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
