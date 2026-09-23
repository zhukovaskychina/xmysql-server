package engine

import (
	"fmt"
	"sort"

	"github.com/zhukovaskychina/xmysql-server/server"
)

func (e *XMySQLExecutor) executePerformanceSchemaMemorySummaryByIdentitySelect(query, dimension string) *SelectResult {
	var defaults []string
	var name string
	switch dimension {
	case "account":
		defaults, name = performanceSchemaMemorySummaryAccountColumns, "performance_schema.memory_summary_by_account_by_event_name"
	case "host":
		defaults, name = performanceSchemaMemorySummaryHostColumns, "performance_schema.memory_summary_by_host_by_event_name"
	default:
		defaults, name = performanceSchemaMemorySummaryUserColumns, "performance_schema.memory_summary_by_user_by_event_name"
	}
	columns := requestedInformationSchemaColumns(query, defaults)
	if e == nil || e.metricsRecorder == nil {
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	type summary struct {
		user, host, eventName                                                                                    string
		alloc, free, bytesAlloc, bytesFree, lowCount, currentCount, highCount, lowBytes, currentBytes, highBytes int64
	}
	byKey := make(map[string]*summary)
	for _, row := range e.metricsRecorder.MemorySummary() {
		user, host := e.performanceSchemaThreadIdentity(row.ThreadID)
		key := fmt.Sprintf("%s\x00%s\x00%s", user, host, row.EventName)
		if dimension == "host" {
			key = fmt.Sprintf("%s\x00%s", host, row.EventName)
		} else if dimension == "user" {
			key = fmt.Sprintf("%s\x00%s", user, row.EventName)
		}
		item := byKey[key]
		if item == nil {
			item = &summary{user: user, host: host, eventName: row.EventName}
			byKey[key] = item
		}
		item.alloc += row.CountAlloc
		item.free += row.CountFree
		item.bytesAlloc += row.BytesAlloc
		item.bytesFree += row.BytesFree
		item.lowCount += row.LowCountUsed
		item.currentCount += row.CurrentCountUsed
		item.highCount += row.HighCountUsed
		item.lowBytes += row.LowBytesUsed
		item.currentBytes += row.CurrentBytesUsed
		item.highBytes += row.HighBytesUsed
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([][]interface{}, 0, len(keys))
	for _, key := range keys {
		item := byKey[key]
		if !performanceSchemaSummaryFilterMatches(query, "user", item.user) ||
			!performanceSchemaSummaryFilterMatches(query, "host", item.host) ||
			!performanceSchemaSummaryFilterMatches(query, "event_name", item.eventName) {
			continue
		}
		values := map[string]interface{}{
			"USER": item.user, "HOST": item.host, "EVENT_NAME": item.eventName,
			"COUNT_ALLOC": item.alloc, "COUNT_FREE": item.free, "SUM_NUMBER_OF_BYTES_ALLOC": item.bytesAlloc, "SUM_NUMBER_OF_BYTES_FREE": item.bytesFree,
			"LOW_COUNT_USED": item.lowCount, "CURRENT_COUNT_USED": item.currentCount, "HIGH_COUNT_USED": item.highCount,
			"LOW_NUMBER_OF_BYTES_USED": item.lowBytes, "CURRENT_NUMBER_OF_BYTES_USED": item.currentBytes, "HIGH_NUMBER_OF_BYTES_USED": item.highBytes,
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult(name, columns, rows)
}

func (e *XMySQLExecutor) performanceSchemaSessions(current server.MySQLServerSession) []server.MySQLServerSession {
	sessions := make([]server.MySQLServerSession, 0)
	if e != nil && e.processlistProvider != nil {
		sessions = append(sessions, e.processlistProvider()...)
	}
	if current != nil {
		seen := false
		for _, listed := range sessions {
			if listed == current {
				seen = true
				break
			}
		}
		if !seen {
			sessions = append(sessions, current)
		}
	}
	return sessions
}

func (e *XMySQLExecutor) executePerformanceSchemaMemorySummaryByThreadSelect(query string, current server.MySQLServerSession) *SelectResult {
	defaults := []string{"THREAD_ID", "EVENT_NAME", "COUNT_ALLOC", "COUNT_FREE", "SUM_NUMBER_OF_BYTES_ALLOC", "SUM_NUMBER_OF_BYTES_FREE", "LOW_COUNT_USED", "CURRENT_COUNT_USED", "HIGH_COUNT_USED", "LOW_NUMBER_OF_BYTES_USED", "CURRENT_NUMBER_OF_BYTES_USED", "HIGH_NUMBER_OF_BYTES_USED"}
	columns := requestedInformationSchemaColumns(query, defaults)
	type memoryRow struct {
		threadID int64
		values   []interface{}
	}
	memoryRows := make([]memoryRow, 0)
	seenThreads := make(map[int64]struct{})
	threadFilter, hasThreadFilter := informationSchemaUint64Filter(query, "thread_id")
	for _, session := range e.performanceSchemaSessions(current) {
		if session == nil {
			continue
		}
		threadID := int64Param(session.GetParamByName("connection_id"))
		if hasThreadFilter && (threadID < 0 || uint64(threadID) != threadFilter) {
			continue
		}
		seenThreads[threadID] = struct{}{}
		if e != nil && e.metricsRecorder != nil {
			matched := false
			for _, summary := range e.metricsRecorder.MemorySummary() {
				if summary.ThreadID != threadID || !performanceSchemaSummaryFilterMatches(query, "EVENT_NAME", summary.EventName) {
					continue
				}
				row := map[string]interface{}{
					"THREAD_ID": summary.ThreadID, "EVENT_NAME": summary.EventName,
					"COUNT_ALLOC": summary.CountAlloc, "COUNT_FREE": summary.CountFree,
					"SUM_NUMBER_OF_BYTES_ALLOC": summary.BytesAlloc, "SUM_NUMBER_OF_BYTES_FREE": summary.BytesFree,
					"LOW_COUNT_USED": summary.LowCountUsed, "CURRENT_COUNT_USED": summary.CurrentCountUsed, "HIGH_COUNT_USED": summary.HighCountUsed,
					"LOW_NUMBER_OF_BYTES_USED": summary.LowBytesUsed, "CURRENT_NUMBER_OF_BYTES_USED": summary.CurrentBytesUsed, "HIGH_NUMBER_OF_BYTES_USED": summary.HighBytesUsed,
				}
				if !performanceSchemaLockValuesMatch(query, row) {
					continue
				}
				memoryRows = append(memoryRows, memoryRow{threadID: threadID, values: projectInformationSchemaRow(columns, row)})
				matched = true
			}
			if matched {
				continue
			}
		}
		row := map[string]interface{}{
			"THREAD_ID": threadID, "EVENT_NAME": "memory/sql/THD::main_mem_root",
			"COUNT_ALLOC": int64(0), "COUNT_FREE": int64(0), "SUM_NUMBER_OF_BYTES_ALLOC": int64(0), "SUM_NUMBER_OF_BYTES_FREE": int64(0),
			"LOW_COUNT_USED": int64(0), "CURRENT_COUNT_USED": int64(0), "HIGH_COUNT_USED": int64(0),
			"LOW_NUMBER_OF_BYTES_USED": int64(0), "CURRENT_NUMBER_OF_BYTES_USED": int64(0), "HIGH_NUMBER_OF_BYTES_USED": int64(0),
		}
		memoryRows = append(memoryRows, memoryRow{threadID: threadID, values: projectInformationSchemaRow(columns, row)})
	}
	if e != nil && e.metricsRecorder != nil && !hasThreadFilter {
		for _, summary := range e.metricsRecorder.MemorySummary() {
			if _, exists := seenThreads[summary.ThreadID]; exists {
				continue
			}
			row := map[string]interface{}{
				"THREAD_ID": summary.ThreadID, "EVENT_NAME": summary.EventName,
				"COUNT_ALLOC": summary.CountAlloc, "COUNT_FREE": summary.CountFree,
				"SUM_NUMBER_OF_BYTES_ALLOC": summary.BytesAlloc, "SUM_NUMBER_OF_BYTES_FREE": summary.BytesFree,
				"LOW_COUNT_USED": summary.LowCountUsed, "CURRENT_COUNT_USED": summary.CurrentCountUsed, "HIGH_COUNT_USED": summary.HighCountUsed,
				"LOW_NUMBER_OF_BYTES_USED": summary.LowBytesUsed, "CURRENT_NUMBER_OF_BYTES_USED": summary.CurrentBytesUsed, "HIGH_NUMBER_OF_BYTES_USED": summary.HighBytesUsed,
			}
			if !performanceSchemaLockValuesMatch(query, row) {
				continue
			}
			memoryRows = append(memoryRows, memoryRow{threadID: summary.ThreadID, values: projectInformationSchemaRow(columns, row)})
		}
	}
	sort.Slice(memoryRows, func(i, j int) bool { return memoryRows[i].threadID < memoryRows[j].threadID })
	rows := make([][]interface{}, 0, len(memoryRows))
	for _, memoryRow := range memoryRows {
		rows = append(rows, memoryRow.values)
	}
	return newInformationSchemaSelectResult("performance_schema.memory_summary_by_thread_by_event_name", columns, rows)
}

func (e *XMySQLExecutor) executePerformanceSchemaPerformanceTimersSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{"TIMER_NAME", "TIMER_FREQUENCY", "TIMER_RESOLUTION", "TIMER_OVERHEAD"})
	rows := make([][]interface{}, 0, 5)
	for _, timer := range []struct {
		name       string
		frequency  string
		resolution string
		overhead   string
	}{
		{name: "CYCLE", frequency: "1000000000", resolution: "1", overhead: "1"},
		{name: "NANOSECOND", frequency: "1000000000", resolution: "1", overhead: "1"},
		{name: "MICROSECOND", frequency: "1000000", resolution: "1", overhead: "1"},
		{name: "MILLISECOND", frequency: "1000", resolution: "1", overhead: "1"},
		{name: "TICK", frequency: "100", resolution: "1", overhead: "1"},
	} {
		if !performanceSchemaSummaryFilterMatches(query, "TIMER_NAME", timer.name) {
			continue
		}
		values := map[string]interface{}{"TIMER_NAME": timer.name, "TIMER_FREQUENCY": timer.frequency, "TIMER_RESOLUTION": timer.resolution, "TIMER_OVERHEAD": timer.overhead}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("performance_schema.performance_timers", columns, rows)
}
