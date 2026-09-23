package engine

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type performanceSchemaTelemetryLogger struct {
	name        string
	level       string
	description string
}

type performanceSchemaTelemetryMeter struct {
	name        string
	frequency   int64
	enabled     string
	description string
}

type performanceSchemaTelemetryMetric struct {
	name        string
	meter       string
	metricType  string
	numType     string
	unit        string
	description string
}

// These are the telemetry definitions that have an authoritative xmysql
// runtime source. The setup tables describe instrumentation, not metric
// values; rows are therefore safe to expose even when the current value is
// zero or no event has been recorded yet.
var performanceSchemaTelemetryLoggers = []performanceSchemaTelemetryLogger{
	{name: "logger/sql/error_log", level: "info", description: "MySQL error logger"},
	{name: "logger/sql/slow_log", level: "info", description: "MySQL slow query logger"},
	{name: "logger/sql/general_log", level: "info", description: "MySQL general logger"},
}

var performanceSchemaTelemetryMeters = []performanceSchemaTelemetryMeter{
	{name: "mysql.inno", frequency: 10, enabled: "YES", description: "MySql InnoDB metrics"},
	{name: "mysql.inno.buffer_pool", frequency: 10, enabled: "YES", description: "MySql InnoDB buffer pool metrics"},
	{name: "mysql.inno.data", frequency: 10, enabled: "YES", description: "MySql InnoDB data metrics"},
	{name: "mysql.stats", frequency: 10, enabled: "YES", description: "MySql core metrics"},
	{name: "mysql.stats.connection", frequency: 10, enabled: "YES", description: "MySql connection stats"},
	{name: "mysql.stats.handler", frequency: 10, enabled: "YES", description: "MySql handler stats"},
	{name: "mysql.stats.ssl", frequency: 10, enabled: "YES", description: "MySql TLS related stats"},
	{name: "mysql.perf_schema", frequency: 10, enabled: "YES", description: "MySql performance_schema lost instruments"},
}

var performanceSchemaTelemetryMetrics = []performanceSchemaTelemetryMetric{
	{
		name: "trx_active_transactions", meter: "mysql.inno", metricType: "ASYNC GAUGE", numType: "INTEGER",
		description: "Active transactions currently registered in TransactionManager",
	},
	{
		name: "lock_waits_current", meter: "mysql.inno", metricType: "ASYNC GAUGE", numType: "INTEGER",
		description: "Current row-lock wait edges maintained by LockManager",
	},
	{
		name: "lock_wait_time_ms_current", meter: "mysql.inno", metricType: "ASYNC GAUGE", numType: "INTEGER",
		description: "Sum of current row-lock wait durations in milliseconds",
	},
}

func defaultPerformanceSchemaLoggers() map[string]performanceSchemaTelemetryLogger {
	result := make(map[string]performanceSchemaTelemetryLogger, len(performanceSchemaTelemetryLoggers))
	for _, logger := range performanceSchemaTelemetryLoggers {
		result[logger.name] = logger
	}
	return result
}

func defaultPerformanceSchemaMeters() map[string]performanceSchemaTelemetryMeter {
	result := make(map[string]performanceSchemaTelemetryMeter, len(performanceSchemaTelemetryMeters))
	for _, meter := range performanceSchemaTelemetryMeters {
		result[meter.name] = meter
	}
	return result
}

func (e *XMySQLExecutor) executePerformanceSchemaTelemetrySetupSelect(query, table string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, performanceSchemaTableColumns(table))
	rows := make([][]interface{}, 0)
	switch table {
	case "setup_loggers":
		loggers := defaultPerformanceSchemaLoggers()
		if e != nil {
			e.performanceSchemaMu.RLock()
			loggers = clonePerformanceSchemaLoggers(e.performanceSchemaLoggers)
			e.performanceSchemaMu.RUnlock()
		}
		for _, logger := range sortedPerformanceSchemaLoggers(loggers) {
			if !performanceSchemaTelemetryRowMatches(query, map[string]string{
				"name": logger.name, "level": logger.level, "description": logger.description,
			}) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, map[string]interface{}{
				"NAME": logger.name, "LEVEL": logger.level, "DESCRIPTION": logger.description,
			}))
		}
	case "setup_meters":
		meters := defaultPerformanceSchemaMeters()
		if e != nil {
			e.performanceSchemaMu.RLock()
			meters = clonePerformanceSchemaMeters(e.performanceSchemaMeters)
			e.performanceSchemaMu.RUnlock()
		}
		for _, meter := range sortedPerformanceSchemaMeters(meters) {
			if !performanceSchemaTelemetryRowMatches(query, map[string]string{
				"name": meter.name, "enabled": meter.enabled, "frequency": formatInt64(meter.frequency), "description": meter.description,
			}) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, map[string]interface{}{
				"NAME": meter.name, "FREQUENCY": meter.frequency, "ENABLED": meter.enabled, "DESCRIPTION": meter.description,
			}))
		}
	case "setup_metrics":
		for _, metric := range performanceSchemaTelemetryMetrics {
			if !performanceSchemaTelemetryRowMatches(query, map[string]string{
				"name": metric.name, "meter": metric.meter, "metric_type": metric.metricType, "num_type": metric.numType,
				"unit": metric.unit, "description": metric.description,
			}) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, map[string]interface{}{
				"NAME": metric.name, "METER": metric.meter, "METRIC_TYPE": metric.metricType,
				"NUM_TYPE": metric.numType, "UNIT": metric.unit, "DESCRIPTION": metric.description,
			}))
		}
	}
	return newInformationSchemaSelectResult("performance_schema."+table, columns, rows)
}

func clonePerformanceSchemaLoggers(source map[string]performanceSchemaTelemetryLogger) map[string]performanceSchemaTelemetryLogger {
	result := make(map[string]performanceSchemaTelemetryLogger, len(source))
	for name, logger := range source {
		result[name] = logger
	}
	return result
}

func clonePerformanceSchemaMeters(source map[string]performanceSchemaTelemetryMeter) map[string]performanceSchemaTelemetryMeter {
	result := make(map[string]performanceSchemaTelemetryMeter, len(source))
	for name, meter := range source {
		result[name] = meter
	}
	return result
}

func sortedPerformanceSchemaLoggers(source map[string]performanceSchemaTelemetryLogger) []performanceSchemaTelemetryLogger {
	rows := make([]performanceSchemaTelemetryLogger, 0, len(source))
	for _, logger := range source {
		rows = append(rows, logger)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })
	return rows
}

func sortedPerformanceSchemaMeters(source map[string]performanceSchemaTelemetryMeter) []performanceSchemaTelemetryMeter {
	rows := make([]performanceSchemaTelemetryMeter, 0, len(source))
	for _, meter := range source {
		rows = append(rows, meter)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })
	return rows
}

func (e *XMySQLExecutor) executePerformanceSchemaTelemetrySetupUpdate(table, assignments, where string) (*Result, bool, error) {
	nameMatches, ok := performanceSchemaSetupNameMatcher(where)
	if !ok {
		return nil, true, fmt.Errorf("unsupported Performance Schema telemetry WHERE clause %q", strings.TrimSpace(where))
	}

	if strings.EqualFold(table, "setup_loggers") {
		level := ""
		for _, assignment := range splitTopLevelComma(assignments) {
			parts := strings.SplitN(assignment, "=", 2)
			if len(parts) != 2 || !strings.EqualFold(strings.Trim(strings.TrimSpace(parts[0]), "`"), "level") {
				return nil, true, fmt.Errorf("unsupported Performance Schema logger assignment %q", strings.TrimSpace(assignment))
			}
			level = strings.ToLower(strings.Trim(strings.TrimSpace(parts[1]), "'\"`"))
			if level != "none" && level != "error" && level != "warn" && level != "info" && level != "debug" {
				return nil, true, fmt.Errorf("unsupported Performance Schema logger level %q", level)
			}
		}
		e.performanceSchemaMu.Lock()
		defer e.performanceSchemaMu.Unlock()
		affected := 0
		for name, logger := range e.performanceSchemaLoggers {
			if !nameMatches(name) {
				continue
			}
			logger.level = level
			e.performanceSchemaLoggers[name] = logger
			affected++
		}
		return &Result{AffectedRows: affected, ResultType: "QUERY", Message: fmt.Sprintf("Performance Schema setup updated, %d rows affected", affected)}, true, nil
	}

	frequency := int64(0)
	hasFrequency := false
	hasEnabled := false
	enabled := false
	for _, assignment := range splitTopLevelComma(assignments) {
		parts := strings.SplitN(assignment, "=", 2)
		if len(parts) != 2 {
			return nil, true, fmt.Errorf("unsupported Performance Schema meter assignment %q", strings.TrimSpace(assignment))
		}
		column := strings.ToLower(strings.Trim(strings.TrimSpace(parts[0]), "`"))
		switch column {
		case "frequency":
			parsed, err := strconv.ParseInt(strings.Trim(strings.TrimSpace(parts[1]), "'\"`"), 10, 64)
			if err != nil || parsed <= 0 {
				return nil, true, fmt.Errorf("invalid Performance Schema meter frequency %q", strings.TrimSpace(parts[1]))
			}
			frequency, hasFrequency = parsed, true
		case "enabled":
			parsed, valid := performanceSchemaSetupBoolean(parts[1])
			if !valid {
				return nil, true, fmt.Errorf("invalid Performance Schema meter enabled value %q", strings.TrimSpace(parts[1]))
			}
			enabled, hasEnabled = parsed, true
		default:
			return nil, true, fmt.Errorf("unsupported Performance Schema meter assignment %q", strings.TrimSpace(assignment))
		}
	}
	e.performanceSchemaMu.Lock()
	defer e.performanceSchemaMu.Unlock()
	affected := 0
	for name, meter := range e.performanceSchemaMeters {
		if !nameMatches(name) {
			continue
		}
		if hasFrequency {
			meter.frequency = frequency
		}
		if hasEnabled {
			if enabled {
				meter.enabled = "YES"
			} else {
				meter.enabled = "NO"
			}
		}
		e.performanceSchemaMeters[name] = meter
		affected++
	}
	return &Result{AffectedRows: affected, ResultType: "QUERY", Message: fmt.Sprintf("Performance Schema setup updated, %d rows affected", affected)}, true, nil
}

func performanceSchemaTelemetryRowMatches(query string, values map[string]string) bool {
	for column, value := range values {
		if !performanceSchemaTLSStatusQueryMatches(query, column, value) {
			return false
		}
	}
	return true
}

func formatInt64(value int64) string {
	return strconv.FormatInt(value, 10)
}
