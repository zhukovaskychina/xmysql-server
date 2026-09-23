package engine

import (
	"regexp"
	"sort"
	"strings"
)

// executeInformationSchemaInnoDBMetricsSelect exposes only metrics backed by
// live engine state. MySQL has a much larger metric registry; returning rows
// with invented zero counters would be less compatible than omitting metrics
// for which this engine has no authoritative source.
func (e *XMySQLExecutor) executeInformationSchemaInnoDBMetricsSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{
		"NAME", "SUBSYSTEM", "COUNT", "MAX_COUNT", "MIN_COUNT", "AVG_COUNT",
		"COUNT_RESET", "MAX_COUNT_RESET", "MIN_COUNT_RESET", "AVG_COUNT_RESET",
		"TIME_ENABLED", "TIME_DISABLED", "TIME_ELAPSED", "TIME_RESET", "STATUS", "TYPE", "COMMENT",
	})
	rows := make([][]interface{}, 0, 3)
	activeTransactions := 0
	if e != nil && e.txManager != nil {
		activeTransactions = len(e.txManager.GetActiveTransactionSnapshots())
	}
	waitEdges := make([]managerWaitEdgeSnapshot, 0)
	if e != nil {
		for _, edge := range e.performanceSchemaWaitEdges() {
			waitEdges = append(waitEdges, managerWaitEdgeSnapshot{durationMilliseconds: edge.WaitDuration.Milliseconds()})
		}
	}
	waitDurationMs := int64(0)
	for _, edge := range waitEdges {
		if edge.durationMilliseconds > 0 {
			waitDurationMs += edge.durationMilliseconds
		}
	}
	metrics := []innodbMetricRow{
		{name: "trx_active_transactions", subsystem: "transaction", count: int64(activeTransactions), metricType: "gauge", comment: "Active transactions currently registered in TransactionManager"},
		{name: "lock_waits_current", subsystem: "lock", count: int64(len(waitEdges)), metricType: "gauge", comment: "Current row-lock wait edges maintained by LockManager"},
		{name: "lock_wait_time_ms_current", subsystem: "lock", count: waitDurationMs, metricType: "gauge", comment: "Sum of current row-lock wait durations in milliseconds"},
	}
	nameFilter, filterOperator, hasNameFilter := informationSchemaMetricNameFilter(query)
	if hasNameFilter {
		filtered := metrics[:0]
		for _, metric := range metrics {
			if metricNameMatches(metric.name, filterOperator, nameFilter) {
				filtered = append(filtered, metric)
			}
		}
		metrics = filtered
	}
	filtered := metrics[:0]
	for _, metric := range metrics {
		if !performanceSchemaSummaryFilterMatches(query, "subsystem", metric.subsystem) ||
			!performanceSchemaSummaryFilterMatches(query, "status", "enabled") ||
			!performanceSchemaSummaryFilterMatches(query, "type", metric.metricType) {
			continue
		}
		filtered = append(filtered, metric)
	}
	metrics = filtered
	sort.Slice(metrics, func(i, j int) bool { return metrics[i].name < metrics[j].name })
	for _, metric := range metrics {
		values := map[string]interface{}{
			"NAME": metric.name, "SUBSYSTEM": metric.subsystem, "COUNT": metric.count,
			"MAX_COUNT": metric.count, "MIN_COUNT": metric.count, "AVG_COUNT": metric.count,
			"COUNT_RESET": nil, "MAX_COUNT_RESET": nil, "MIN_COUNT_RESET": nil, "AVG_COUNT_RESET": nil,
			"TIME_ENABLED": nil, "TIME_DISABLED": nil, "TIME_ELAPSED": nil, "TIME_RESET": nil,
			"STATUS": "enabled", "TYPE": metric.metricType, "COMMENT": metric.comment,
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("information_schema.innodb_metrics", columns, rows)
}

type innodbMetricRow struct {
	name       string
	subsystem  string
	count      int64
	metricType string
	comment    string
}

type managerWaitEdgeSnapshot struct {
	durationMilliseconds int64
}

func informationSchemaMetricNameFilter(query string) (string, string, bool) {
	match := regexp.MustCompile(`(?is)\bname\b\s*(=|like)\s*('([^']*)'|"([^"]*)")`).FindStringSubmatch(query)
	if len(match) != 5 {
		return "", "", false
	}
	value := match[3]
	if value == "" {
		value = match[4]
	}
	return value, strings.ToLower(match[1]), true
}

func metricNameMatches(name, operator, pattern string) bool {
	if operator == "=" {
		return strings.EqualFold(name, pattern)
	}
	regex := "^" + regexp.QuoteMeta(strings.ToLower(pattern)) + "$"
	regex = strings.ReplaceAll(regex, "%", ".*")
	regex = strings.ReplaceAll(regex, "_", ".")
	matched, _ := regexp.MatchString(regex, strings.ToLower(name))
	return matched
}
