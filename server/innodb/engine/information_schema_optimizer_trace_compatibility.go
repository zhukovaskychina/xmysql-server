package engine

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server"
)

const optimizerTraceHistoryLimit = 128

type optimizerTraceEntry struct {
	ThreadID   int64
	RecordedAt time.Time
	Query      string
	Trace      string
}

func (e *XMySQLExecutor) recordOptimizerTrace(ctx *ExecutionContext, selectExecutor *SelectExecutor) {
	if e == nil || selectExecutor == nil || selectExecutor.physicalPlan == nil {
		return
	}
	query := ""
	threadID := int64(0)
	if ctx != nil {
		query = strings.TrimSpace(ctx.RawQuery)
		if ctx.Session != nil {
			threadID = int64(sessionConnectionID(ctx.Session))
		}
	}
	traceDocument := map[string]interface{}{
		"access_path":  selectExecutor.lastAccessPath,
		"plan_type":    int64(selectExecutor.physicalPlan.PlanType),
		"cost":         selectExecutor.physicalPlan.Cost,
		"row_count":    selectExecutor.physicalPlan.RowCount,
		"table_name":   selectExecutor.physicalPlan.TableName,
		"index_name":   selectExecutor.physicalPlan.IndexName,
		"conditions":   selectExecutor.physicalPlan.Conditions,
		"join_type":    selectExecutor.physicalPlan.JoinType,
		"sort_columns": selectExecutor.physicalPlan.SortColumns,
		"group_by":     selectExecutor.physicalPlan.GroupByCols,
	}
	encoded, err := json.Marshal(traceDocument)
	if err != nil {
		return
	}
	entry := optimizerTraceEntry{
		ThreadID:   threadID,
		RecordedAt: time.Now(),
		Query:      query,
		Trace:      string(encoded),
	}
	e.optimizerTraceMu.Lock()
	e.optimizerTraces = append(e.optimizerTraces, entry)
	if len(e.optimizerTraces) > optimizerTraceHistoryLimit {
		e.optimizerTraces = append([]optimizerTraceEntry(nil), e.optimizerTraces[len(e.optimizerTraces)-optimizerTraceHistoryLimit:]...)
	}
	e.optimizerTraceMu.Unlock()
}

func (e *XMySQLExecutor) optimizerTraceSnapshot(session server.MySQLServerSession) []optimizerTraceEntry {
	if e == nil {
		return nil
	}
	threadID := int64(0)
	if session != nil {
		threadID = int64(sessionConnectionID(session))
	}
	e.optimizerTraceMu.RLock()
	entries := make([]optimizerTraceEntry, 0, len(e.optimizerTraces))
	for _, entry := range e.optimizerTraces {
		if threadID != 0 && entry.ThreadID != 0 && entry.ThreadID != threadID {
			continue
		}
		entries = append(entries, entry)
	}
	e.optimizerTraceMu.RUnlock()
	return entries
}

func (e *XMySQLExecutor) executeInformationSchemaOptimizerTraceSelect(query string, session server.MySQLServerSession) *SelectResult {
	const table = "optimizer_trace"
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry[table])
	entries := e.optimizerTraceSnapshot(session)
	rows := make([][]interface{}, 0, len(entries))
	for _, entry := range entries {
		values := map[string]interface{}{
			"QUERY":                             entry.Query,
			"TRACE":                             entry.Trace,
			"MISSING_BYTES_BEYOND_MAX_MEM_SIZE": int64(0),
			"INSUFFICIENT_PRIVILEGES":           "NO",
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("information_schema."+table, columns, rows)
}
