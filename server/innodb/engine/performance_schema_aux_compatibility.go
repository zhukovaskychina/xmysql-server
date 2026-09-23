package engine

import (
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
)

func (e *XMySQLExecutor) executePerformanceSchemaHostCacheSelect(query string, current server.MySQLServerSession) *SelectResult {
	defaults := []string{
		"IP", "HOST", "HOST_VALIDATED", "SUM_CONNECT_ERRORS", "COUNT_HOST_BLOCKED_ERRORS",
		"COUNT_NAMEINFO_TRANSIENT_ERRORS", "COUNT_NAMEINFO_PERMANENT_ERRORS", "COUNT_FORMAT_ERRORS",
		"COUNT_ADDRINFO_TRANSIENT_ERRORS", "COUNT_ADDRINFO_PERMANENT_ERRORS", "COUNT_FCRDNS_ERRORS",
		"COUNT_HOST_ACL_ERRORS", "COUNT_NO_AUTH_PLUGIN_ERRORS", "COUNT_AUTH_PLUGIN_ERRORS",
		"COUNT_HANDSHAKE_ERRORS", "COUNT_PROXY_USER_ERRORS", "COUNT_PROXY_USER_ACL_ERRORS",
		"COUNT_AUTHENTICATION_ERRORS", "COUNT_SSL_ERRORS", "COUNT_MAX_USER_CONNECTIONS_ERRORS",
		"COUNT_MAX_USER_CONNECTIONS_PER_HOUR_ERRORS", "COUNT_DEFAULT_DATABASE_ERRORS", "COUNT_INIT_CONNECT_ERRORS",
		"COUNT_LOCAL_ERRORS", "COUNT_UNKNOWN_ERRORS", "FIRST_SEEN", "LAST_SEEN", "FIRST_ERROR_SEEN", "LAST_ERROR_SEEN",
	}
	columns := requestedInformationSchemaColumns(query, defaults)
	rows := make([][]interface{}, 0)
	seen := make(map[string]struct{})
	failedByHost := make(map[string]int64)
	if e != nil && e.metricsRecorder != nil {
		for _, summary := range e.metricsRecorder.AuthenticationFailureSummaryByHost() {
			failedByHost[summary.Host] = summary.Count
		}
	}
	appendRow := func(ip, host string) {
		key := ip + "\x00" + host
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		if !performanceSchemaSummaryFilterMatches(query, "IP", ip) || !performanceSchemaSummaryFilterMatches(query, "HOST", host) {
			return
		}
		count := failedByHost[host]
		if ip != host {
			count += failedByHost[ip]
		}
		values := map[string]interface{}{
			"IP": ip, "HOST": host, "HOST_VALIDATED": "YES", "SUM_CONNECT_ERRORS": count,
			"COUNT_HOST_BLOCKED_ERRORS": int64(0), "COUNT_NAMEINFO_TRANSIENT_ERRORS": int64(0), "COUNT_NAMEINFO_PERMANENT_ERRORS": int64(0),
			"COUNT_FORMAT_ERRORS": int64(0), "COUNT_ADDRINFO_TRANSIENT_ERRORS": int64(0), "COUNT_ADDRINFO_PERMANENT_ERRORS": int64(0),
			"COUNT_FCRDNS_ERRORS": int64(0), "COUNT_HOST_ACL_ERRORS": int64(0), "COUNT_NO_AUTH_PLUGIN_ERRORS": int64(0),
			"COUNT_AUTH_PLUGIN_ERRORS": int64(0), "COUNT_HANDSHAKE_ERRORS": int64(0), "COUNT_PROXY_USER_ERRORS": int64(0),
			"COUNT_PROXY_USER_ACL_ERRORS": int64(0), "COUNT_AUTHENTICATION_ERRORS": count, "COUNT_SSL_ERRORS": int64(0),
			"COUNT_MAX_USER_CONNECTIONS_ERRORS": int64(0), "COUNT_MAX_USER_CONNECTIONS_PER_HOUR_ERRORS": int64(0),
			"COUNT_DEFAULT_DATABASE_ERRORS": int64(0), "COUNT_INIT_CONNECT_ERRORS": int64(0), "COUNT_LOCAL_ERRORS": int64(0),
			"COUNT_UNKNOWN_ERRORS": int64(0), "FIRST_SEEN": nil, "LAST_SEEN": nil, "FIRST_ERROR_SEEN": nil, "LAST_ERROR_SEEN": nil,
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			return
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	for _, listed := range e.performanceSchemaSessions(current) {
		if listed == nil {
			continue
		}
		ip, _ := performanceSchemaSocketEndpoint(listed)
		host, _ := listed.GetParamByName("host").(string)
		if strings.TrimSpace(host) == "" {
			host = ip
		}
		appendRow(ip, host)
	}
	if e != nil && e.metricsRecorder != nil {
		for _, summary := range e.metricsRecorder.AuthenticationFailureSummaryByHost() {
			appendRow(summary.Host, summary.Host)
		}
	}
	return newInformationSchemaSelectResult("performance_schema.host_cache", columns, rows)
}

func (e *XMySQLExecutor) executePerformanceSchemaSyncInstancesSelect(query, name string) *SelectResult {
	defaults := []string{"NAME", "OBJECT_INSTANCE_BEGIN", "LOCKED_BY_THREAD_ID"}
	if name == "performance_schema.rwlock_instances" {
		defaults = []string{"NAME", "OBJECT_INSTANCE_BEGIN", "WRITE_LOCKED_BY_THREAD_ID", "READ_LOCKED_BY_COUNT"}
	}
	columns := requestedInformationSchemaColumns(query, defaults)
	return newInformationSchemaSelectResult(name, columns, nil)
}

func (e *XMySQLExecutor) executePerformanceSchemaObjectsSummarySelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"})
	rows := make([][]interface{}, 0)
	for _, edge := range e.performanceSchemaWaitEdges() {
		values := map[string]interface{}{
			"OBJECT_TYPE": "TABLE", "OBJECT_SCHEMA": nil, "OBJECT_NAME": edge.ResourceID,
			"COUNT_STAR": int64(1), "SUM_TIMER_WAIT": edge.WaitDuration.Nanoseconds() * 1000,
			"MIN_TIMER_WAIT": edge.WaitDuration.Nanoseconds() * 1000, "AVG_TIMER_WAIT": edge.WaitDuration.Nanoseconds() * 1000,
			"MAX_TIMER_WAIT": edge.WaitDuration.Nanoseconds() * 1000,
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("performance_schema.objects_summary_global_by_type", columns, rows)
}
