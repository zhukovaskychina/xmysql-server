package engine

import (
	"regexp"
	"sort"
	"strings"
)

type performanceSchemaTableIOCounters struct {
	count int64
	sum   int64
	min   int64
	max   int64
}

type performanceSchemaTableIOSummary struct {
	objectType   string
	objectSchema string
	objectName   string
	indexName    string
	all          performanceSchemaTableIOCounters
	read         performanceSchemaTableIOCounters
	write        performanceSchemaTableIOCounters
	fetch        performanceSchemaTableIOCounters
	insert       performanceSchemaTableIOCounters
	update       performanceSchemaTableIOCounters
	delete       performanceSchemaTableIOCounters
}

var performanceSchemaTableReferencePattern = regexp.MustCompile("(?i)\\b(?:from|join|into|update)\\s+([[:alnum:]_$`.-]+)")

func performanceSchemaTableReferences(query, defaultSchema string) [][2]string {
	seen := make(map[string]struct{})
	refs := make([][2]string, 0)
	for _, match := range performanceSchemaTableReferencePattern.FindAllStringSubmatch(query, -1) {
		if len(match) < 2 {
			continue
		}
		raw := strings.Trim(strings.TrimSpace(match[1]), "`")
		if raw == "" || strings.HasPrefix(raw, "(") {
			continue
		}
		schema, table := compatibilityQualifiedTable(raw, defaultSchema)
		key := strings.ToLower(schema + "\x00" + table)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		refs = append(refs, [2]string{schema, table})
	}
	return refs
}

func (c *performanceSchemaTableIOCounters) add(timer int64) {
	if c.count == 0 || timer < c.min {
		c.min = timer
	}
	if c.count == 0 || timer > c.max {
		c.max = timer
	}
	c.count++
	c.sum += timer
}

func (c performanceSchemaTableIOCounters) project(prefix string) map[string]interface{} {
	return map[string]interface{}{
		"COUNT_" + prefix:     c.count,
		"SUM_TIMER_" + prefix: c.sum,
		"MIN_TIMER_" + prefix: c.min,
		"AVG_TIMER_" + prefix: averagePerformanceSchemaTimer(c.sum, c.count),
		"MAX_TIMER_" + prefix: c.max,
	}
}

func (e *XMySQLExecutor) executePerformanceSchemaTableIOSummarySelect(query string, byIndex bool) *SelectResult {
	base := []string{"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME"}
	if byIndex {
		base = append(base, "INDEX_NAME")
	}
	base = append(base,
		"COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT",
		"COUNT_READ", "SUM_TIMER_READ", "MIN_TIMER_READ", "AVG_TIMER_READ", "MAX_TIMER_READ",
		"COUNT_WRITE", "SUM_TIMER_WRITE", "MIN_TIMER_WRITE", "AVG_TIMER_WRITE", "MAX_TIMER_WRITE",
		"COUNT_FETCH", "SUM_TIMER_FETCH", "MIN_TIMER_FETCH", "AVG_TIMER_FETCH", "MAX_TIMER_FETCH",
		"COUNT_INSERT", "SUM_TIMER_INSERT", "MIN_TIMER_INSERT", "AVG_TIMER_INSERT", "MAX_TIMER_INSERT",
		"COUNT_UPDATE", "SUM_TIMER_UPDATE", "MIN_TIMER_UPDATE", "AVG_TIMER_UPDATE", "MAX_TIMER_UPDATE",
		"COUNT_DELETE", "SUM_TIMER_DELETE", "MIN_TIMER_DELETE", "AVG_TIMER_DELETE", "MAX_TIMER_DELETE",
	)
	columns := requestedInformationSchemaColumns(query, base)
	byKey := make(map[string]*performanceSchemaTableIOSummary)
	if e == nil || e.metricsRecorder == nil {
		name := "performance_schema.table_io_waits_summary_by_table"
		if byIndex {
			name = "performance_schema.table_io_waits_summary_by_index_usage"
		}
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	for _, event := range e.metricsRecorder.StatementHistory() {
		if strings.EqualFold(event.Status, "error") {
			continue
		}
		refs := performanceSchemaTableReferences(event.SQL, event.Schema)
		if len(refs) == 0 {
			continue
		}
		timer := event.Latency.Nanoseconds() * 1000
		if timer < 0 {
			timer = 0
		}
		kind := strings.ToUpper(strings.TrimSpace(event.StatementType))
		for _, ref := range refs {
			if strings.EqualFold(ref[0], "performance_schema") || strings.EqualFold(ref[0], "information_schema") {
				continue
			}
			if !performanceSchemaSummaryFilterMatches(query, "OBJECT_TYPE", "TABLE") ||
				!performanceSchemaSummaryFilterMatches(query, "OBJECT_SCHEMA", ref[0]) ||
				!performanceSchemaSummaryFilterMatches(query, "OBJECT_NAME", ref[1]) {
				continue
			}
			if byIndex && !performanceSchemaSummaryFilterMatches(query, "INDEX_NAME", "") {
				continue
			}
			indexName := ""
			key := ref[0] + "\x00" + ref[1]
			if byIndex {
				key += "\x00" + indexName
			}
			summary := byKey[key]
			if summary == nil {
				summary = &performanceSchemaTableIOSummary{objectType: "TABLE", objectSchema: ref[0], objectName: ref[1], indexName: indexName}
				byKey[key] = summary
			}
			summary.all.add(timer)
			switch kind {
			case "SELECT", "SHOW", "EXPLAIN":
				summary.read.add(timer)
				summary.fetch.add(timer)
			case "INSERT", "REPLACE":
				summary.write.add(timer)
				summary.insert.add(timer)
			case "UPDATE":
				summary.read.add(timer)
				summary.write.add(timer)
				summary.update.add(timer)
			case "DELETE":
				summary.read.add(timer)
				summary.write.add(timer)
				summary.delete.add(timer)
			default:
				summary.read.add(timer)
			}
		}
	}
	values := make([]performanceSchemaTableIOSummary, 0, len(byKey))
	for _, summary := range byKey {
		values = append(values, *summary)
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].objectSchema != values[j].objectSchema {
			return values[i].objectSchema < values[j].objectSchema
		}
		return values[i].objectName < values[j].objectName
	})
	rows := make([][]interface{}, 0, len(values))
	for _, summary := range values {
		row := map[string]interface{}{
			"OBJECT_TYPE": summary.objectType, "OBJECT_SCHEMA": summary.objectSchema, "OBJECT_NAME": summary.objectName,
			"COUNT_STAR": summary.all.count, "SUM_TIMER_WAIT": summary.all.sum, "MIN_TIMER_WAIT": summary.all.min,
			"AVG_TIMER_WAIT": averagePerformanceSchemaTimer(summary.all.sum, summary.all.count), "MAX_TIMER_WAIT": summary.all.max,
		}
		for key, value := range summary.read.project("READ") {
			row[key] = value
		}
		for key, value := range summary.write.project("WRITE") {
			row[key] = value
		}
		for key, value := range summary.fetch.project("FETCH") {
			row[key] = value
		}
		for key, value := range summary.insert.project("INSERT") {
			row[key] = value
		}
		for key, value := range summary.update.project("UPDATE") {
			row[key] = value
		}
		for key, value := range summary.delete.project("DELETE") {
			row[key] = value
		}
		if byIndex {
			row["INDEX_NAME"] = summary.indexName
		}
		if !performanceSchemaLockValuesMatch(query, row) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, row))
	}
	name := "performance_schema.table_io_waits_summary_by_table"
	if byIndex {
		name = "performance_schema.table_io_waits_summary_by_index_usage"
	}
	return newInformationSchemaSelectResult(name, columns, rows)
}
