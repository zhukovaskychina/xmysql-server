package engine

import (
	"hash/fnv"
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
)

// executePerformanceSchemaTableHandlesSelect exposes the explicit LOCK TABLES
// handles held by the live sessions.  The executor already keeps the
// qualified table and lock mode in locked_tables; projecting that state here
// makes table_handles a runtime view instead of a shape-only registry table.
func (e *XMySQLExecutor) executePerformanceSchemaTableHandlesSelect(query string, current server.MySQLServerSession) *SelectResult {
	defaults := performanceSchemaTableColumns("table_handles")
	columns := requestedInformationSchemaColumns(query, defaults)
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

	type tableHandle struct {
		schema, table, internal, external string
		threadID, eventID, instance       int64
	}
	handles := make([]tableHandle, 0)
	for _, session := range sessions {
		if session == nil {
			continue
		}
		entries := performanceSchemaTableHandleEntries(session)
		if len(entries) == 0 {
			continue
		}
		threadID := int64Param(session.GetParamByName("connection_id"))
		eventID := int64Param(session.GetParamByName("event_id"))
		for _, entry := range entries {
			schema, table := splitPerformanceSchemaTableHandleName(entry.name)
			if schema == "" || table == "" {
				continue
			}
			internal, external := performanceSchemaTableHandleLocks(entry.mode)
			handles = append(handles, tableHandle{
				schema: schema, table: table, internal: internal, external: external,
				threadID: threadID, eventID: eventID,
				instance: performanceSchemaTableHandleInstance(schema, table, threadID),
			})
		}
	}

	sort.Slice(handles, func(i, j int) bool {
		if handles[i].schema != handles[j].schema {
			return handles[i].schema < handles[j].schema
		}
		if handles[i].table != handles[j].table {
			return handles[i].table < handles[j].table
		}
		return handles[i].threadID < handles[j].threadID
	})
	rows := make([][]interface{}, 0, len(handles))
	for _, handle := range handles {
		values := map[string]interface{}{
			"OBJECT_TYPE":           "TABLE",
			"OBJECT_SCHEMA":         handle.schema,
			"OBJECT_NAME":           handle.table,
			"OBJECT_INSTANCE_BEGIN": handle.instance,
			"OWNER_THREAD_ID":       handle.threadID,
			"OWNER_EVENT_ID":        handle.eventID,
			"INTERNAL_LOCK":         handle.internal,
			"EXTERNAL_LOCK":         handle.external,
		}
		if performanceSchemaLockValuesMatch(query, values) {
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("performance_schema.table_handles", columns, rows)
}

type performanceSchemaTableHandleEntry struct {
	name string
	mode string
}

func performanceSchemaTableHandleEntries(session server.MySQLServerSession) []performanceSchemaTableHandleEntry {
	entries := make([]performanceSchemaTableHandleEntry, 0)
	appendLease := func(value interface{}) {
		lease, ok := value.(*sessionTableLockLease)
		if !ok || lease == nil {
			return
		}
		for _, entry := range lease.entries {
			if strings.TrimSpace(entry.table) == "" {
				continue
			}
			entries = append(entries, performanceSchemaTableHandleEntry{name: entry.table, mode: entry.mode})
		}
	}
	appendLease(session.GetParamByName("__table_lock_lease"))
	appendLease(session.GetParamByName("__transaction_table_lock_lease"))
	if len(entries) > 0 {
		return entries
	}

	locked, ok := session.GetParamByName("locked_tables").(map[string]string)
	if !ok {
		return nil
	}
	schema, _ := session.GetParamByName("database").(string)
	for table, mode := range locked {
		name := strings.TrimSpace(table)
		if !strings.Contains(name, ".") && strings.TrimSpace(schema) != "" {
			name = strings.TrimSpace(schema) + "." + name
		}
		entries = append(entries, performanceSchemaTableHandleEntry{name: name, mode: mode})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
	return entries
}

func splitPerformanceSchemaTableHandleName(name string) (schema, table string) {
	parts := strings.SplitN(strings.TrimSpace(name), ".", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
}

func performanceSchemaTableHandleLocks(mode string) (internal, external string) {
	if strings.EqualFold(strings.TrimSpace(mode), "write") {
		return "WRITE", "WRITE"
	}
	return "READ", "READ"
}

func performanceSchemaTableHandleInstance(schema, table string, threadID int64) int64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(schema + "\x00" + table))
	value := int64(hash.Sum64() &^ (uint64(1) << 63))
	if value == 0 {
		value = 1
	}
	return value ^ threadID
}
