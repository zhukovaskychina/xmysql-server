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

// refreshPerformanceSchemaTableInstancesLost derives table-instance capacity
// accounting from the runtime recorder's table-I/O summaries. These summaries
// are the authoritative table objects currently instrumented by this server;
// the counter is cumulative while the identity set tracks only currently
// overflowing objects so a table that becomes visible again can be counted if
// it overflows later.
func (e *XMySQLExecutor) refreshPerformanceSchemaTableInstancesLost() {
	if e == nil || e.metricsRecorder == nil {
		return
	}
	keys := make([]string, 0)
	seen := make(map[string]struct{})
	for _, summary := range e.metricsRecorder.TableIOSummary() {
		if strings.TrimSpace(summary.ObjectSchema) == "" || strings.TrimSpace(summary.ObjectName) == "" {
			continue
		}
		setting, configured := e.performanceSchemaObjectSettingFor("TABLE", summary.ObjectSchema, summary.ObjectName)
		if configured && !setting.Enabled {
			continue
		}
		key := summary.ObjectType + "\x00" + summary.ObjectSchema + "\x00" + summary.ObjectName
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	capacity := e.performanceSchemaCapacity("performance_schema_max_table_instances")
	overflow := make(map[string]struct{})
	if capacity < len(keys) {
		for _, key := range keys[capacity:] {
			overflow[key] = struct{}{}
		}
	}
	e.performanceSchemaMu.Lock()
	if e.performanceSchemaTableInstanceLostKeys == nil {
		e.performanceSchemaTableInstanceLostKeys = make(map[string]struct{})
	}
	for key := range overflow {
		if _, exists := e.performanceSchemaTableInstanceLostKeys[key]; exists {
			continue
		}
		e.performanceSchemaTableInstanceLostKeys[key] = struct{}{}
		e.performanceSchemaTableInstancesLost.Add(1)
	}
	for key := range e.performanceSchemaTableInstanceLostKeys {
		if _, exists := overflow[key]; !exists {
			delete(e.performanceSchemaTableInstanceLostKeys, key)
		}
	}
	e.performanceSchemaMu.Unlock()
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

func (c *performanceSchemaTableIOCounters) addAggregate(count, sum, min, max int64) {
	if count <= 0 {
		return
	}
	if c.count == 0 || min < c.min {
		c.min = min
	}
	if c.count == 0 || max > c.max {
		c.max = max
	}
	c.count += count
	c.sum += sum
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
	if e != nil && e.metricsRecorder != nil {
		return e.executePerformanceSchemaTableIOSummaryFromRecorder(query, byIndex, columns)
	}
	byKey := make(map[string]*performanceSchemaTableIOSummary)
	if e == nil || e.metricsRecorder == nil {
		name := "performance_schema.table_io_waits_summary_by_table"
		if byIndex {
			name = "performance_schema.table_io_waits_summary_by_index_usage"
		}
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	for _, event := range e.metricsRecorder.StatementSummary() {
		if event.SuccessCount == 0 {
			continue
		}
		refs := performanceSchemaTableReferences(event.SQL, event.Schema)
		if len(refs) == 0 {
			continue
		}
		kind := strings.ToUpper(strings.TrimSpace(event.StatementType))
		for _, ref := range refs {
			if strings.EqualFold(ref[0], "performance_schema") || strings.EqualFold(ref[0], "information_schema") {
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
			summary.all.addAggregate(event.SuccessCount, event.SuccessSumTimerWait, event.SuccessMinTimerWait, event.SuccessMaxTimerWait)
			switch kind {
			case "SELECT", "SHOW", "EXPLAIN":
				summary.read.addAggregate(event.SuccessCount, event.SuccessSumTimerWait, event.SuccessMinTimerWait, event.SuccessMaxTimerWait)
				summary.fetch.addAggregate(event.SuccessCount, event.SuccessSumTimerWait, event.SuccessMinTimerWait, event.SuccessMaxTimerWait)
			case "INSERT", "REPLACE":
				summary.write.addAggregate(event.SuccessCount, event.SuccessSumTimerWait, event.SuccessMinTimerWait, event.SuccessMaxTimerWait)
				summary.insert.addAggregate(event.SuccessCount, event.SuccessSumTimerWait, event.SuccessMinTimerWait, event.SuccessMaxTimerWait)
			case "UPDATE":
				summary.read.addAggregate(event.SuccessCount, event.SuccessSumTimerWait, event.SuccessMinTimerWait, event.SuccessMaxTimerWait)
				summary.write.addAggregate(event.SuccessCount, event.SuccessSumTimerWait, event.SuccessMinTimerWait, event.SuccessMaxTimerWait)
				summary.update.addAggregate(event.SuccessCount, event.SuccessSumTimerWait, event.SuccessMinTimerWait, event.SuccessMaxTimerWait)
			case "DELETE":
				summary.read.addAggregate(event.SuccessCount, event.SuccessSumTimerWait, event.SuccessMinTimerWait, event.SuccessMaxTimerWait)
				summary.write.addAggregate(event.SuccessCount, event.SuccessSumTimerWait, event.SuccessMinTimerWait, event.SuccessMaxTimerWait)
				summary.delete.addAggregate(event.SuccessCount, event.SuccessSumTimerWait, event.SuccessMinTimerWait, event.SuccessMaxTimerWait)
			default:
				summary.read.addAggregate(event.SuccessCount, event.SuccessSumTimerWait, event.SuccessMinTimerWait, event.SuccessMaxTimerWait)
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
	if byIndex {
		values = e.applyPerformanceSchemaIndexStatCapacity(values)
	}
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

func (e *XMySQLExecutor) executePerformanceSchemaTableIOSummaryFromRecorder(query string, byIndex bool, columns []string) *SelectResult {
	e.refreshPerformanceSchemaTableInstancesLost()
	source := e.metricsRecorder.TableIOSummary()
	if byIndex {
		source = e.metricsRecorder.TableIOIndexSummary()
	}
	byKey := make(map[string]*performanceSchemaTableIOSummary)
	toCounters := func(count, sum, min, max int64) performanceSchemaTableIOCounters {
		return performanceSchemaTableIOCounters{count: count, sum: sum, min: min, max: max}
	}
	merge := func(target *performanceSchemaTableIOCounters, source performanceSchemaTableIOCounters) {
		if source.count <= 0 {
			return
		}
		if target.count == 0 || source.min < target.min {
			target.min = source.min
		}
		if target.count == 0 || source.max > target.max {
			target.max = source.max
		}
		target.count += source.count
		target.sum += source.sum
	}
	for _, event := range source {
		objectSetting, objectConfigured := e.performanceSchemaObjectSettingFor("TABLE", event.ObjectSchema, event.ObjectName)
		if !objectConfigured || !objectSetting.Enabled {
			continue
		}
		current := performanceSchemaTableIOSummary{
			objectType: event.ObjectType, objectSchema: event.ObjectSchema,
			objectName: event.ObjectName, indexName: event.IndexName,
			all:    toCounters(event.All.Count, event.All.SumTimerWait, event.All.MinTimerWait, event.All.MaxTimerWait),
			read:   toCounters(event.Read.Count, event.Read.SumTimerWait, event.Read.MinTimerWait, event.Read.MaxTimerWait),
			write:  toCounters(event.Write.Count, event.Write.SumTimerWait, event.Write.MinTimerWait, event.Write.MaxTimerWait),
			fetch:  toCounters(event.Fetch.Count, event.Fetch.SumTimerWait, event.Fetch.MinTimerWait, event.Fetch.MaxTimerWait),
			insert: toCounters(event.Insert.Count, event.Insert.SumTimerWait, event.Insert.MinTimerWait, event.Insert.MaxTimerWait),
			update: toCounters(event.Update.Count, event.Update.SumTimerWait, event.Update.MinTimerWait, event.Update.MaxTimerWait),
			delete: toCounters(event.Delete.Count, event.Delete.SumTimerWait, event.Delete.MinTimerWait, event.Delete.MaxTimerWait),
		}
		if !objectSetting.Timed {
			untimed := func(c *performanceSchemaTableIOCounters) {
				c.sum, c.min, c.max = 0, 0, 0
			}
			untimed(&current.all)
			untimed(&current.read)
			untimed(&current.write)
			untimed(&current.fetch)
			untimed(&current.insert)
			untimed(&current.update)
			untimed(&current.delete)
		}
		key := current.objectSchema + "\x00" + current.objectName + "\x00" + current.indexName
		summary := byKey[key]
		if summary == nil {
			copy := current
			byKey[key] = &copy
			continue
		}
		merge(&summary.all, current.all)
		merge(&summary.read, current.read)
		merge(&summary.write, current.write)
		merge(&summary.fetch, current.fetch)
		merge(&summary.insert, current.insert)
		merge(&summary.update, current.update)
		merge(&summary.delete, current.delete)
	}
	values := make([]performanceSchemaTableIOSummary, 0, len(byKey))
	for _, summary := range byKey {
		values = append(values, *summary)
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].objectSchema != values[j].objectSchema {
			return values[i].objectSchema < values[j].objectSchema
		}
		if values[i].objectName != values[j].objectName {
			return values[i].objectName < values[j].objectName
		}
		return values[i].indexName < values[j].indexName
	})
	if byIndex {
		values = e.applyPerformanceSchemaIndexStatCapacity(values)
	}
	values = e.applyPerformanceSchemaTableInstanceCapacity(values)
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

// applyPerformanceSchemaTableInstanceCapacity projects max_table_instances
// over the table identities represented by the already aggregated I/O rows.
// Values are sorted by table identity before this helper is called, making the
// retained set deterministic for compatibility tests and diagnostics.
func (e *XMySQLExecutor) applyPerformanceSchemaTableInstanceCapacity(values []performanceSchemaTableIOSummary) []performanceSchemaTableIOSummary {
	if e == nil || len(values) == 0 {
		return values
	}
	capacity := e.performanceSchemaCapacity("performance_schema_max_table_instances")
	retained := make(map[string]struct{})
	for _, value := range values {
		key := value.objectType + "\x00" + value.objectSchema + "\x00" + value.objectName
		if _, exists := retained[key]; exists {
			continue
		}
		if len(retained) >= capacity {
			break
		}
		retained[key] = struct{}{}
	}
	if len(retained) >= len(values) {
		return values
	}
	filtered := make([]performanceSchemaTableIOSummary, 0, len(values))
	for _, value := range values {
		key := value.objectType + "\x00" + value.objectSchema + "\x00" + value.objectName
		if _, exists := retained[key]; exists {
			filtered = append(filtered, value)
		}
	}
	return filtered
}

// applyPerformanceSchemaIndexStatCapacity projects the configured MySQL
// index-statistics table capacity and records each distinct row that could not
// be retained. Callers apply query predicates only after this helper so an
// evicted index row cannot be resurrected by a selective read.
func (e *XMySQLExecutor) applyPerformanceSchemaIndexStatCapacity(values []performanceSchemaTableIOSummary) []performanceSchemaTableIOSummary {
	if e == nil {
		return values
	}
	capacity := e.performanceSchemaIndexStatCapacity()
	if capacity < len(values) {
		e.performanceSchemaMu.Lock()
		for _, summary := range values[capacity:] {
			key := summary.objectSchema + "\x00" + summary.objectName + "\x00" + summary.indexName
			if _, exists := e.performanceSchemaIndexStatLostKeys[key]; exists {
				continue
			}
			e.performanceSchemaIndexStatLostKeys[key] = struct{}{}
			e.performanceSchemaIndexStatLost.Add(1)
		}
		e.performanceSchemaMu.Unlock()
		values = values[:capacity]
	}
	return values
}
