package engine

import (
	"sort"
	"strings"
	"time"

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
	firstErrorByHost := make(map[string]time.Time)
	lastErrorByHost := make(map[string]time.Time)
	if e != nil && e.metricsRecorder != nil {
		for _, summary := range e.metricsRecorder.AuthenticationFailureSummaryByHost() {
			failedByHost[summary.Host] = summary.Count
			firstErrorByHost[summary.Host] = summary.FirstSeen
			lastErrorByHost[summary.Host] = summary.LastSeen
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
		// Authentication failures are the authoritative host-cache observation
		// for hosts that have no separate connection-lifecycle timestamp yet.
		// Use those timestamps for FIRST_SEEN/LAST_SEEN rather than returning a
		// NULL pair on an otherwise source-backed host row.
		firstSeen := firstErrorByHost[host]
		lastSeen := lastErrorByHost[host]
		if firstSeen.IsZero() && ip != host {
			firstSeen = firstErrorByHost[ip]
		}
		if lastSeen.IsZero() && ip != host {
			lastSeen = lastErrorByHost[ip]
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
			"COUNT_UNKNOWN_ERRORS": int64(0), "FIRST_SEEN": nullableHostCacheTime(firstSeen), "LAST_SEEN": nullableHostCacheTime(lastSeen),
			"FIRST_ERROR_SEEN": firstErrorByHost[host], "LAST_ERROR_SEEN": lastErrorByHost[host],
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

func nullableHostCacheTime(value time.Time) interface{} {
	if value.IsZero() {
		return nil
	}
	return value
}

func (e *XMySQLExecutor) executePerformanceSchemaSyncInstancesSelect(query, name string) *SelectResult {
	defaults := []string{"NAME", "OBJECT_INSTANCE_BEGIN", "LOCKED_BY_THREAD_ID"}
	if name == "performance_schema.cond_instances" {
		defaults = []string{"NAME", "OBJECT_INSTANCE_BEGIN"}
	}
	if name == "performance_schema.rwlock_instances" {
		defaults = []string{"NAME", "OBJECT_INSTANCE_BEGIN", "WRITE_LOCKED_BY_THREAD_ID", "READ_LOCKED_BY_COUNT"}
	}
	columns := requestedInformationSchemaColumns(query, defaults)
	if name == "performance_schema.cond_instances" {
		if e == nil {
			return newInformationSchemaSelectResult(name, columns, nil)
		}
		rows := make([][]interface{}, 0)
		for _, instance := range e.performanceSchemaConditionInstancesForQuery() {
			values := map[string]interface{}{
				"NAME":                  instance.name,
				"OBJECT_INSTANCE_BEGIN": instance.objectInstanceBegin,
			}
			if !performanceSchemaLockValuesMatch(query, values) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
		return newInformationSchemaSelectResult(name, columns, rows)
	}
	if name == "performance_schema.mutex_instances" {
		if e == nil {
			return newInformationSchemaSelectResult(name, columns, nil)
		}
		rows := make([][]interface{}, 0)
		for _, instance := range e.performanceSchemaMutexInstancesForQuery() {
			values := map[string]interface{}{
				"NAME":                  instance.name,
				"OBJECT_INSTANCE_BEGIN": instance.objectInstanceBegin,
				"LOCKED_BY_THREAD_ID":   instance.lockedByThreadID,
			}
			if !performanceSchemaLockValuesMatch(query, values) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
		return newInformationSchemaSelectResult(name, columns, rows)
	}
	if name != "performance_schema.rwlock_instances" || e == nil || e.ddlCoordinator == nil {
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	enabled, _ := e.performanceSchemaInstrumentSetting(performanceSchemaDDLCoordinatorRWLockInstrument)
	if !enabled {
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	rows := make([][]interface{}, 0)
	for _, instance := range e.performanceSchemaRWLockInstancesForQuery() {
		values := map[string]interface{}{
			"NAME":                      instance.name,
			"OBJECT_INSTANCE_BEGIN":     instance.objectInstanceBegin,
			"WRITE_LOCKED_BY_THREAD_ID": instance.writeLockedByThreadID,
			"READ_LOCKED_BY_COUNT":      instance.readLockedByCount,
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult(name, columns, rows)
}

func (e *XMySQLExecutor) executePerformanceSchemaObjectsSummarySelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"})
	type objectSummary struct {
		objectType   string
		objectSchema interface{}
		objectName   string
		count        int64
		sum          int64
		min          int64
		max          int64
	}
	byKey := make(map[string]*objectSummary)
	addSummary := func(objectType string, objectSchema interface{}, objectName string, count, timer int64) {
		key := objectType + "\x00" + objectName
		summary := byKey[key]
		if summary == nil {
			summary = &objectSummary{objectType: objectType, objectSchema: objectSchema, objectName: objectName, min: timer, max: timer}
			byKey[key] = summary
		}
		summary.count += count
		summary.sum += timer
		if summary.count == count || timer < summary.min {
			summary.min = timer
		}
		if timer > summary.max {
			summary.max = timer
		}
	}
	if e != nil && e.lockManager != nil {
		for _, edge := range e.lockManager.ObjectSummarySnapshot() {
			timer := edge.WaitDuration.Nanoseconds() * 1000
			addSummary("TABLE", nil, edge.ResourceID, 1, timer)
		}
	}
	for _, edge := range e.performanceSchemaWaitEdges() {
		timer := edge.WaitDuration.Nanoseconds() * 1000
		addSummary("TABLE", nil, edge.ResourceID, 1, timer)
	}
	if e != nil {
		e.performanceSchemaMu.RLock()
		resetRows := append([]performanceSchemaObjectSummaryResetEvent(nil), e.performanceSchemaObjectSummaryReset...)
		e.performanceSchemaMu.RUnlock()
		for _, reset := range resetRows {
			key := reset.ObjectType + "\x00" + reset.ObjectName
			if _, exists := byKey[key]; !exists {
				byKey[key] = &objectSummary{objectType: reset.ObjectType, objectSchema: reset.ObjectSchema, objectName: reset.ObjectName}
			}
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([][]interface{}, 0, len(keys))
	for _, key := range keys {
		summary := byKey[key]
		avg := int64(0)
		if summary.count > 0 {
			avg = summary.sum / summary.count
		}
		values := map[string]interface{}{
			"OBJECT_TYPE": summary.objectType, "OBJECT_SCHEMA": summary.objectSchema, "OBJECT_NAME": summary.objectName,
			"COUNT_STAR": summary.count, "SUM_TIMER_WAIT": summary.sum, "MIN_TIMER_WAIT": summary.min,
			"AVG_TIMER_WAIT": avg, "MAX_TIMER_WAIT": summary.max,
		}
		if performanceSchemaLockValuesMatch(query, values) {
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("performance_schema.objects_summary_global_by_type", columns, rows)
}
