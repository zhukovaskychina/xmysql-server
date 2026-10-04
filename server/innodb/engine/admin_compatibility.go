package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func metricStatementType(query string) string {
	normalized := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";")))
	if normalized == "" {
		return "empty"
	}
	hasPrefix := func(prefix string) bool {
		return normalized == prefix || strings.HasPrefix(normalized, prefix+" ")
	}
	switch {
	case hasPrefix("create database"):
		return "create_db"
	case hasPrefix("create table") || hasPrefix("create temporary table"):
		return "create_table"
	case hasPrefix("create view"):
		return "create_view"
	case hasPrefix("create procedure"):
		return "create_procedure"
	case hasPrefix("create function"):
		return "create_function"
	case hasPrefix("create trigger"):
		return "create_trigger"
	case hasPrefix("create event"):
		return "create_event"
	case hasPrefix("create role"):
		return "create_role"
	case hasPrefix("create index") || hasPrefix("create unique index"):
		return "create_index"
	case hasPrefix("create user"):
		return "create_user"
	case hasPrefix("alter database"):
		return "alter_db"
	case hasPrefix("alter table"):
		return "alter_table"
	case hasPrefix("alter view"):
		return "alter_view"
	case hasPrefix("alter procedure"):
		return "alter_procedure"
	case hasPrefix("alter function"):
		return "alter_function"
	case hasPrefix("alter event"):
		return "alter_event"
	case hasPrefix("alter tablespace"):
		return "alter_tablespace"
	case hasPrefix("alter user"):
		return "alter_user"
	case hasPrefix("drop database"):
		return "drop_db"
	case hasPrefix("drop table"):
		return "drop_table"
	case hasPrefix("drop temporary table"):
		return "drop_table"
	case hasPrefix("drop index"):
		return "drop_index"
	case hasPrefix("drop view"):
		return "drop_view"
	case hasPrefix("drop procedure"):
		return "drop_procedure"
	case hasPrefix("drop function"):
		return "drop_function"
	case hasPrefix("drop trigger"):
		return "drop_trigger"
	case hasPrefix("drop event"):
		return "drop_event"
	case hasPrefix("drop role"):
		return "drop_role"
	case hasPrefix("drop user"):
		return "drop_user"
	case hasPrefix("rename table"):
		return "rename_table"
	case hasPrefix("rename user"):
		return "rename_user"
	case hasPrefix("truncate table"):
		return "truncate"
	case hasPrefix("insert"):
		if strings.Contains(normalized, " select ") {
			return "insert_select"
		}
		return "insert"
	case hasPrefix("replace"):
		if strings.Contains(normalized, " select ") {
			return "replace_select"
		}
		return "replace"
	case hasPrefix("update"):
		return "update"
	case hasPrefix("delete"):
		return "delete"
	case hasPrefix("prepare"):
		return "prepare_sql"
	case hasPrefix("execute"):
		return "execute_sql"
	case hasPrefix("deallocate prepare"):
		return "dealloc_sql"
	case hasPrefix("call"):
		return "call_procedure"
	case hasPrefix("show create table"):
		return "show_create_table"
	case hasPrefix("show create database"):
		return "show_create_db"
	case hasPrefix("show create view"):
		return "show_create_table"
	case hasPrefix("show create procedure"):
		return "show_create_proc"
	case hasPrefix("show create function"):
		return "show_create_func"
	case hasPrefix("show create trigger"):
		return "show_create_trigger"
	case hasPrefix("show create event"):
		return "show_create_event"
	case hasPrefix("show create user"):
		return "show_create_user"
	case hasPrefix("show databases"):
		return "show_databases"
	case hasPrefix("show tables") || hasPrefix("show full tables"):
		return "show_tables"
	case hasPrefix("show table status"):
		return "show_table_status"
	case hasPrefix("show triggers"):
		return "show_triggers"
	case hasPrefix("show events"):
		return "show_events"
	case hasPrefix("show procedure status"):
		return "show_procedure_status"
	case hasPrefix("show function status"):
		return "show_function_status"
	case hasPrefix("show columns") || hasPrefix("show fields") || hasPrefix("show full columns") || hasPrefix("show full fields"):
		return "show_fields"
	case hasPrefix("show index") || hasPrefix("show indexes") || hasPrefix("show keys"):
		return "show_keys"
	case hasPrefix("show charsets"):
		return "show_charsets"
	case hasPrefix("show collations") || hasPrefix("show collation"):
		return "show_collations"
	case hasPrefix("show plugins"):
		return "show_plugins"
	case hasPrefix("show privileges"):
		return "show_privileges"
	case hasPrefix("show storage engines"):
		return "show_storage_engines"
	case hasPrefix("show open tables"):
		return "show_open_tables"
	case hasPrefix("show binlogs"):
		return "show_binlogs"
	case hasPrefix("show binary logs"):
		return "show_binlogs"
	case hasPrefix("show binlog events"):
		return "show_binlog_events"
	case hasPrefix("show relaylog events"):
		return "show_relaylog_events"
	case hasPrefix("show relay log events"):
		return "show_relaylog_events"
	case hasPrefix("show master status") || hasPrefix("show source status"):
		return "show_master_status"
	case hasPrefix("show slave status") || hasPrefix("show replica status"):
		return "show_slave_status"
	case hasPrefix("show slave hosts") || hasPrefix("show replica hosts"):
		return "show_slave_hosts"
	case hasPrefix("show engine") && strings.Contains(normalized, " logs"):
		return "show_engine_logs"
	case hasPrefix("show engine") && strings.Contains(normalized, " status"):
		return "show_engine_status"
	case hasPrefix("show engine") && strings.Contains(normalized, " mutex"):
		return "show_engine_mutex"
	case hasPrefix("show profiles"):
		return "show_profiles"
	case hasPrefix("show profile"):
		return "show_profile"
	case hasPrefix("show variables"):
		return "show_variables"
	case hasPrefix("show status"):
		return "show_status"
	case hasPrefix("show processlist"):
		return "show_processlist"
	case hasPrefix("show grants"):
		return "show_grants"
	case hasPrefix("show warnings"):
		return "show_warnings"
	case hasPrefix("show errors"):
		return "show_errors"
	case hasPrefix("load data"):
		return "load"
	case hasPrefix("lock tables"):
		return "lock_tables"
	case hasPrefix("unlock tables"):
		return "unlock_tables"
	case hasPrefix("flush"):
		return "flush"
	case hasPrefix("kill"):
		return "kill"
	case hasPrefix("analyze table"):
		return "analyze"
	case hasPrefix("optimize table"):
		return "optimize"
	case hasPrefix("check table"):
		return "check"
	case hasPrefix("checksum table"):
		return "checksum"
	case hasPrefix("reset"):
		return "reset"
	case hasPrefix("purge binary logs") || hasPrefix("purge master logs"):
		return "purge"
	case hasPrefix("change replication filter"):
		return "change_repl_filter"
	case hasPrefix("change replication source") || hasPrefix("change master"):
		return "change_master"
	case hasPrefix("start replica") || hasPrefix("start slave"):
		return "slave_start"
	case hasPrefix("stop replica") || hasPrefix("stop slave"):
		return "slave_stop"
	case hasPrefix("use"):
		return "change_db"
	case hasPrefix("do"):
		return "do"
	case hasPrefix("empty query"):
		return "empty_query"
	case hasPrefix("set"):
		return "set_option"
	case hasPrefix("rollback to savepoint"):
		return "rollback_to_savepoint"
	case hasPrefix("rollback to"):
		return "rollback_to_savepoint"
	case hasPrefix("rollback"):
		return "rollback"
	case hasPrefix("release savepoint"):
		return "release_savepoint"
	case hasPrefix("savepoint"):
		return "savepoint"
	case hasPrefix("start transaction") || hasPrefix("begin"):
		return "begin"
	case hasPrefix("commit"):
		return "commit"
	case strings.HasPrefix(normalized, "grant '"):
		return "grant_role"
	case hasPrefix("grant"):
		return "grant"
	case hasPrefix("revoke all"):
		return "revoke_all"
	case strings.HasPrefix(normalized, "revoke '"):
		return "revoke_role"
	case hasPrefix("revoke"):
		return "revoke"
	case hasPrefix("xa start"):
		return "xa_start"
	case hasPrefix("xa end"):
		return "xa_end"
	case hasPrefix("xa prepare"):
		return "xa_prepare"
	case hasPrefix("xa commit"):
		return "xa_commit"
	case hasPrefix("xa rollback"):
		return "xa_rollback"
	case hasPrefix("xa recover"):
		return "xa_recover"
	}
	fields := strings.Fields(normalized)
	if len(fields) == 0 {
		return "empty"
	}
	return fields[0]
}

func metricStatementTypeForContext(ctx *ExecutionContext, query string) string {
	if ctx != nil && ctx.statementParseError {
		return "error"
	}
	return metricStatementType(query)
}

// performanceSchemaStatementEventName preserves fully-qualified command
// instruments while keeping the historical shorthand for SQL statements.
// SQL execution records use values such as "select"; protocol commands use
// MySQL's statement/com/* names, for example "statement/com/Close stmt".
func performanceSchemaStatementEventName(statementType string) string {
	statementType = strings.TrimSpace(statementType)
	if strings.HasPrefix(strings.ToLower(statementType), "statement/") {
		return statementType
	}
	return "statement/sql/" + strings.ToLower(statementType)
}

// executeAdminCompatibility handles session-scoped table locks and gives
// explicitly unsupported file-import/export commands a stable error. This is
// intentionally before the SQL parser so unsupported variants cannot be
// mistaken for a successful generic statement.
func (e *XMySQLExecutor) executeAdminCompatibility(ctx *ExecutionContext, session server.MySQLServerSession, query string) (bool, error) {
	q := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	lower := strings.ToLower(q)
	if handled, err := e.executeSessionKill(session, q, lower); handled {
		return true, err
	}
	requiredPrivilege := replicationControlRequiredPrivilege(lower)
	if requiredPrivilege == "" {
		requiredPrivilege = sourceBinlogRequiredPrivilege(lower)
	}
	if requiredPrivilege != "" {
		privilegeContext := &ExecutionContext{}
		if ctx != nil {
			contextCopy := *ctx
			privilegeContext = &contextCopy
		}
		if session != nil {
			privilegeContext.Session = session
		}
		if err := e.checkGlobalPrivilege(privilegeContext, requiredPrivilege); err != nil {
			return true, err
		}
	}
	if handled, err := e.executeReplicationControl(q, lower); handled {
		return true, err
	}
	if handled, err := e.executeFlushOptimizerCosts(ctx, q); handled {
		return true, err
	}
	if handled, err := e.executeComponentLifecycle(ctx, session, q); handled {
		return true, err
	}
	if handled, err := e.executeFlushTables(ctx, q); handled {
		return true, err
	}
	if strings.HasPrefix(lower, "load data") || strings.Contains(lower, " into outfile ") || strings.Contains(lower, " into dumpfile ") {
		return true, fmt.Errorf("file import/export is not supported")
	}
	if handled, err := e.executePersistedVariableStatement(ctx, session, q); handled {
		return true, err
	}
	if isTableMaintenanceStatement(lower) {
		if err := e.prepareDDLImplicitCommit(ctx.Session); err != nil {
			return true, err
		}
		var targets []tableMaintenanceTarget
		if histogram, matched, histogramErr := parseHistogramMaintenance(q, ctxDatabaseName(ctx)); matched {
			if histogramErr != nil {
				return true, histogramErr
			}
			targets = []tableMaintenanceTarget{histogram.target}
		} else {
			var targetErr error
			targets, targetErr = parseTableMaintenanceTargets(tableMaintenancePattern.FindStringSubmatch(q)[2], ctxDatabaseName(ctx))
			if targetErr != nil {
				return true, targetErr
			}
		}
		metadataTables := make([]string, 0, len(targets))
		for _, target := range targets {
			metadataTables = append(metadataTables, strings.ToLower(strings.TrimSpace(target.schema))+"."+strings.ToLower(strings.TrimSpace(target.table)))
		}
		if len(metadataTables) > 0 {
			releaseTableLocks, lockErr := e.acquireStatementTableLocks(ctx, ctx.Session, "select * from "+strings.Join(metadataTables, " join "), ctxDatabaseName(ctx))
			if lockErr != nil {
				return true, tableLockWaitError(lockErr)
			}
			defer releaseTableLocks()
		}
		result, err := e.executeTableMaintenance(ctx, q)
		if err != nil {
			return true, err
		}
		if ctx != nil {
			ctx.AdminResult = result
		}
		return true, nil
	}
	if strings.EqualFold(lower, "unlock tables") {
		e.releaseGlobalReadLock(session)
		e.releaseSessionTableLocks(session)
		return true, nil
	}
	if strings.HasPrefix(lower, "lock tables ") {
		if session == nil {
			return true, fmt.Errorf("LOCK TABLES requires a session")
		}
		// MySQL commits the active transaction before acquiring explicit table
		// locks. This also releases a transaction-scoped metadata lease created
		// by autocommit=0 reads before the new LOCK TABLES lease is installed.
		if err := e.prepareDDLImplicitCommit(session); err != nil {
			return true, err
		}
		schemaName := ""
		if ctx != nil {
			schemaName = strings.TrimSpace(ctx.DatabaseName)
		}
		if schemaName == "" {
			return true, fmt.Errorf("no database selected")
		}
		requests, err := e.parseSessionTableLockRequests(ctx, q, schemaName)
		if err != nil {
			return true, err
		}
		lockContext := context.Background()
		if ctx != nil && ctx.Context != nil {
			lockContext = ctx.Context
		}
		lockContext, cancel := tableLockContext(lockContext, session)
		defer cancel()
		if err := e.acquireSessionTableLocks(lockContext, session, requests); err != nil {
			return true, tableLockWaitError(err)
		}
		return true, nil
	}
	if err := checkSessionTableLock(session, q, ctxDatabaseName(ctx)); err != nil {
		return true, err
	}
	return false, nil
}

var flushTablesPattern = regexp.MustCompile(`(?is)^\s*flush\s+(?:(?:no_write_to_binlog|local)\s+)?tables(?:\s+(.+?))?\s*$`)
var flushOptimizerCostsPattern = regexp.MustCompile(`(?is)^\s*flush\s+(?:(?:no_write_to_binlog|local)\s+)?optimizer_costs\s*$`)

func (e *XMySQLExecutor) executeFlushOptimizerCosts(ctx *ExecutionContext, query string) (bool, error) {
	if !flushOptimizerCostsPattern.MatchString(query) {
		return false, nil
	}
	if err := e.prepareDDLImplicitCommit(ctx.Session); err != nil {
		return true, err
	}
	if err := e.reloadOptimizerCostModel(); err != nil {
		return true, err
	}
	return true, nil
}

// executeFlushTables makes FLUSH TABLES a real storage barrier. The current
// engine has no independent open-table cache, so named-table forms flush the
// same durable page set as the unqualified form after validating their names.
// WITH READ LOCK additionally holds the instance read barrier until UNLOCK
// TABLES, allowing SELECTs while blocking recognized table writers.
func (e *XMySQLExecutor) executeFlushTables(ctx *ExecutionContext, query string) (bool, error) {
	match := flushTablesPattern.FindStringSubmatch(query)
	if len(match) == 0 {
		return false, nil
	}
	if err := e.prepareDDLImplicitCommit(ctx.Session); err != nil {
		return true, err
	}
	rest := strings.TrimSpace(match[1])
	withReadLock := false
	if strings.EqualFold(rest, "with read lock") {
		withReadLock = true
		rest = ""
	} else if strings.HasSuffix(strings.ToLower(rest), " with read lock") {
		withReadLock = true
		rest = strings.TrimSpace(rest[:len(rest)-len(" with read lock")])
	}
	if withReadLock && (e == nil || ctx == nil || ctx.Session == nil) {
		return true, fmt.Errorf("FLUSH TABLES WITH READ LOCK requires a session")
	}
	if rest != "" {
		if _, err := parseTableMaintenanceTargets(rest, ctxDatabaseName(ctx)); err != nil {
			return true, fmt.Errorf("invalid FLUSH TABLES target: %w", err)
		}
	}
	if e == nil || e.storageManager == nil {
		return true, fmt.Errorf("storage manager is not configured")
	}
	if err := e.storageManager.Flush(); err != nil {
		return true, fmt.Errorf("FLUSH TABLES failed: %w", err)
	}
	if withReadLock {
		queryContext := context.Background()
		if ctx != nil && ctx.Context != nil {
			queryContext = ctx.Context
		}
		if err := e.holdGlobalReadLock(queryContext, ctx.Session); err != nil {
			return true, err
		}
	}
	return true, nil
}

func (e *XMySQLExecutor) holdGlobalReadLock(ctx context.Context, session server.MySQLServerSession) error {
	if e == nil || session == nil {
		return fmt.Errorf("FLUSH TABLES WITH READ LOCK requires a session")
	}
	e.globalReadLockStateMu.Lock()
	if e.globalReadLockHeld {
		e.globalReadLockStateMu.Unlock()
		return fmt.Errorf("FLUSH TABLES WITH READ LOCK is already held")
	}
	e.globalReadLockStateMu.Unlock()
	if err := e.globalReadLock.LockWithObserver(ctx, func(started time.Time) func() {
		return e.beginPerformanceSchemaGlobalReadLockWait(session, started)
	}); err != nil {
		return err
	}
	e.globalReadLockStateMu.Lock()
	if e.globalReadLockHeld {
		e.globalReadLockStateMu.Unlock()
		e.globalReadLock.Unlock()
		return fmt.Errorf("FLUSH TABLES WITH READ LOCK is already held")
	}
	e.globalReadLockHeld = true
	e.globalReadLockOwner = session
	e.globalReadLockStateMu.Unlock()
	return nil
}

func (e *XMySQLExecutor) releaseGlobalReadLock(session server.MySQLServerSession) {
	if e == nil || session == nil {
		return
	}
	e.globalReadLockStateMu.Lock()
	if !e.globalReadLockHeld || e.globalReadLockOwner != session {
		e.globalReadLockStateMu.Unlock()
		return
	}
	e.globalReadLockHeld = false
	e.globalReadLockOwner = nil
	e.globalReadLock.Unlock()
	e.globalReadLockStateMu.Unlock()
}

func (e *XMySQLExecutor) acquireGlobalReadLockForStatement(ctx context.Context, session server.MySQLServerSession, query string) (func(), error) {
	if e == nil || !globalReadLockWriteStatement(query) {
		return func() {}, nil
	}
	if err := e.globalReadLock.RLockWithObserver(ctx, func(started time.Time) func() {
		return e.beginPerformanceSchemaGlobalReadLockWait(session, started)
	}); err != nil {
		return nil, err
	}
	return e.globalReadLock.RUnlock, nil
}

func globalReadLockWriteStatement(query string) bool {
	lower := strings.ToLower(strings.TrimSpace(query))
	for _, prefix := range []string{
		"insert ", "insert\t", "update ", "update\t", "delete ", "delete\t", "replace ", "replace\t",
		"create table", "create\ttable", "drop table", "drop\ttable", "alter table", "alter\ttable",
		"truncate table", "truncate\ttable", "rename table", "rename\ttable", "create database", "drop database",
	} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

var lockTableEntryPattern = regexp.MustCompile("(?is)^\\s*((?:`?[a-zA-Z0-9_$]+`?\\s*\\.\\s*)?`?[a-zA-Z0-9_$]+`?)\\s+(read(?:\\s+local)?|write)\\s*$")

func (e *XMySQLExecutor) parseSessionTableLockRequests(ctx *ExecutionContext, query, defaultSchema string) ([]sessionTableLockRequest, error) {
	trimmed := strings.TrimSpace(query)
	if len(trimmed) < len("lock tables") || !strings.EqualFold(trimmed[:len("lock tables")], "lock tables") {
		return nil, fmt.Errorf("invalid LOCK TABLES syntax")
	}
	parts := strings.Split(strings.TrimSpace(trimmed[len("lock tables"):]), ",")
	if len(parts) == 0 {
		return nil, fmt.Errorf("invalid LOCK TABLES syntax")
	}
	requests := make([]sessionTableLockRequest, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		match := lockTableEntryPattern.FindStringSubmatch(part)
		if len(match) != 3 {
			return nil, fmt.Errorf("invalid LOCK TABLES syntax near %q", strings.TrimSpace(part))
		}
		database, table := compatibilityQualifiedTable(match[1], defaultSchema)
		if database == "" || table == "" {
			return nil, fmt.Errorf("invalid LOCK TABLES table %q", match[1])
		}
		key := strings.ToLower(strings.TrimSpace(database)) + "." + strings.ToLower(strings.TrimSpace(table))
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate table in LOCK TABLES: %s", match[1])
		}
		seen[key] = struct{}{}
		if err := e.checkTablePrivilege(ctx, database, table, "LOCK TABLES"); err != nil {
			return nil, err
		}
		if err := e.checkTablePrivilege(ctx, database, table, "SELECT"); err != nil {
			return nil, err
		}
		mode := "read"
		if strings.EqualFold(strings.TrimSpace(match[2]), "write") {
			mode = "write"
		}
		requests = append(requests, sessionTableLockRequest{table: key, mode: mode})
	}
	return requests, nil
}

func ctxDatabaseName(ctx *ExecutionContext) string {
	if ctx == nil {
		return ""
	}
	return strings.TrimSpace(ctx.DatabaseName)
}

var killConnectionPattern = regexp.MustCompile(`(?is)^kill\s+(?:(connection)\s+)?([0-9]+)$`)

func (e *XMySQLExecutor) executeSessionKill(session server.MySQLServerSession, query, lower string) (bool, error) {
	if !strings.HasPrefix(lower, "kill ") {
		return false, nil
	}
	isQuery := strings.HasPrefix(lower, "kill query ")
	matchPattern := killConnectionPattern
	if isQuery {
		matchPattern = regexp.MustCompile(`(?is)^kill\s+query\s+([0-9]+)$`)
	}
	match := matchPattern.FindStringSubmatch(query)
	if len(match) == 0 {
		return true, fmt.Errorf("invalid KILL syntax")
	}
	idIndex := 2
	if isQuery {
		idIndex = 1
	}
	targetID, err := strconv.ParseUint(match[idIndex], 10, 32)
	if err != nil || targetID == 0 {
		return true, fmt.Errorf("invalid KILL thread id")
	}
	if isQuery {
		if e == nil || e.sessionQueryKill == nil {
			return true, fmt.Errorf("session query kill control is not configured")
		}
		return true, e.sessionQueryKill(uint32(targetID), session)
	}
	if e == nil || e.sessionKill == nil {
		return true, fmt.Errorf("session kill control is not configured")
	}
	return true, e.sessionKill(uint32(targetID), session)
}

func replicationControlRequiredPrivilege(lower string) string {
	if lower == "reset replica" || lower == "reset slave" ||
		strings.HasPrefix(lower, "reset replica ") || strings.HasPrefix(lower, "reset slave ") {
		return "RELOAD"
	}
	if strings.HasPrefix(lower, "change replication source to ") ||
		strings.HasPrefix(lower, "change master to ") ||
		strings.HasPrefix(lower, "change replication filter ") ||
		lower == "start replica" || lower == "start slave" ||
		strings.HasPrefix(lower, "start replica ") || strings.HasPrefix(lower, "start slave ") ||
		lower == "stop replica" || lower == "stop slave" ||
		strings.HasPrefix(lower, "stop replica ") || strings.HasPrefix(lower, "stop slave ") {
		return "REPLICATION_SLAVE_ADMIN"
	}
	return ""
}

func sourceBinlogRequiredPrivilege(lower string) string {
	if flushOptimizerCostsPattern.MatchString(lower) {
		return "FLUSH_OPTIMIZER_COSTS"
	}
	if lower == "flush binary logs" || lower == "reset master" ||
		lower == "reset binary logs and gtids" || strings.HasPrefix(lower, "reset binary logs and gtids ") {
		return "RELOAD"
	}
	if strings.HasPrefix(lower, "purge binary logs to ") ||
		strings.HasPrefix(lower, "purge master logs to ") ||
		strings.HasPrefix(lower, "purge binary logs before ") ||
		strings.HasPrefix(lower, "purge master logs before ") {
		return "BINLOG_ADMIN"
	}
	return ""
}

func (e *XMySQLExecutor) executeReplicationControl(query, lower string) (bool, error) {
	if strings.HasPrefix(lower, "purge binary logs before ") || strings.HasPrefix(lower, "purge master logs before ") {
		if e == nil || e.replicationPurgeBinaryLogsBefore == nil {
			return true, fmt.Errorf("replication source is not configured")
		}
		match := purgeBinaryLogsBeforePattern.FindStringSubmatch(query)
		if len(match) != 2 || strings.TrimSpace(match[1]) == "" {
			return true, fmt.Errorf("invalid PURGE BINARY LOGS syntax")
		}
		cutoff, err := parseBinlogPurgeTime(strings.TrimSpace(match[1]))
		if err != nil {
			return true, err
		}
		return true, e.replicationPurgeBinaryLogsBefore(cutoff)
	}
	if strings.HasPrefix(lower, "purge binary logs to ") || strings.HasPrefix(lower, "purge master logs to ") {
		if e == nil || e.replicationPurgeBinaryLogsTo == nil {
			return true, fmt.Errorf("replication source is not configured")
		}
		match := purgeBinaryLogsPattern.FindStringSubmatch(query)
		if len(match) != 2 || strings.TrimSpace(match[1]) == "" {
			return true, fmt.Errorf("invalid PURGE BINARY LOGS syntax")
		}
		return true, e.replicationPurgeBinaryLogsTo(strings.TrimSpace(match[1]))
	}
	if lower == "flush binary logs" {
		if e == nil || e.replicationFlushLogs == nil {
			return true, fmt.Errorf("replication source is not configured")
		}
		return true, e.replicationFlushLogs()
	}
	if lower == "reset master" {
		if e == nil || e.replicationResetMaster == nil {
			return true, fmt.Errorf("replication source is not configured")
		}
		return true, e.replicationResetMaster()
	}
	if lower == "reset binary logs and gtids" || strings.HasPrefix(lower, "reset binary logs and gtids ") {
		if e == nil || e.replicationResetBinaryLogsAndGTIDs == nil {
			return true, fmt.Errorf("replication source is not configured")
		}
		match := resetBinaryLogsAndGTIDsPattern.FindStringSubmatch(query)
		if len(match) != 2 {
			return true, fmt.Errorf("invalid RESET BINARY LOGS AND GTIDS syntax")
		}
		index := uint32(1)
		if strings.TrimSpace(match[1]) != "" {
			parsed, err := strconv.ParseUint(strings.TrimSpace(match[1]), 10, 32)
			if err != nil || parsed == 0 {
				return true, fmt.Errorf("invalid RESET BINARY LOGS AND GTIDS file index %q", strings.TrimSpace(match[1]))
			}
			index = uint32(parsed)
		}
		return true, e.replicationResetBinaryLogsAndGTIDs(index)
	}
	if strings.HasPrefix(lower, "change replication source to ") || strings.HasPrefix(lower, "change master to ") {
		if e == nil {
			return true, fmt.Errorf("replication runtime is not configured")
		}
		sourceURL, err := parseReplicationSourceURL(query)
		if err != nil {
			return true, err
		}
		_, channel, _ := splitReplicationChannelSuffix(query)
		if channel != "" {
			if e.replicationChangeSourceForChannel == nil {
				return true, fmt.Errorf("only the default replication channel is supported")
			}
			return true, e.replicationChangeSourceForChannel(channel, sourceURL)
		}
		if e.replicationChangeSource == nil {
			return true, fmt.Errorf("replication runtime is not configured")
		}
		return true, e.replicationChangeSource(sourceURL)
	}
	if strings.HasPrefix(lower, "change replication filter ") {
		if e == nil {
			return true, fmt.Errorf("replication runtime is not configured")
		}
		filters, err := parseReplicationFilter(query)
		if err != nil {
			return true, err
		}
		_, channel, _ := splitReplicationChannelSuffix(query)
		if channel != "" {
			if e.replicationChangeFilterForChannel == nil {
				return true, fmt.Errorf("only the default replication channel is supported")
			}
			return true, e.replicationChangeFilterForChannel(channel, filters)
		}
		if e.replicationChangeFilter == nil {
			return true, fmt.Errorf("replication runtime is not configured")
		}
		return true, e.replicationChangeFilter(filters)
	}
	reset := lower == "reset replica" || lower == "reset slave" || strings.HasPrefix(lower, "reset replica ") || strings.HasPrefix(lower, "reset slave ")
	if reset {
		resetAll, channel, valid := parseResetReplicaStatement(query)
		if !valid {
			return true, fmt.Errorf("invalid RESET REPLICA syntax")
		}
		if resetAll {
			if channel != "" {
				if e == nil || e.replicationResetAllForChannel == nil {
					return true, fmt.Errorf("only the default replication channel is supported")
				}
				return true, e.replicationResetAllForChannel(channel)
			}
			if e == nil || e.replicationResetAll == nil {
				return true, fmt.Errorf("replication runtime is not configured")
			}
			return true, e.replicationResetAll()
		}
		if channel != "" {
			if e == nil || e.replicationResetForChannel == nil {
				return true, fmt.Errorf("only the default replication channel is supported")
			}
			return true, e.replicationResetForChannel(channel)
		}
		if e == nil || e.replicationReset == nil {
			return true, fmt.Errorf("replication runtime is not configured")
		}
		return true, e.replicationReset()
	}
	start := lower == "start replica" || lower == "start slave" || strings.HasPrefix(lower, "start replica ") || strings.HasPrefix(lower, "start slave ")
	stop := lower == "stop replica" || lower == "stop slave" || strings.HasPrefix(lower, "stop replica ") || strings.HasPrefix(lower, "stop slave ")
	if !start && !stop {
		return false, nil
	}
	channel, channelSyntax := parseReplicationChannelControl(query)
	if !channelSyntax {
		fields := strings.Fields(lower)
		if len(fields) > 2 {
			return true, fmt.Errorf("replication thread options are not supported")
		}
		if len(fields) < 2 {
			return true, fmt.Errorf("invalid replication control statement")
		}
	}
	if start {
		if channel != "" {
			if e == nil || e.replicationStartForChannel == nil {
				return true, fmt.Errorf("only the default replication channel is supported")
			}
			return true, e.replicationStartForChannel(channel)
		}
		if e == nil || e.replicationStart == nil {
			return true, fmt.Errorf("replication runtime is not configured")
		}
		return true, e.replicationStart()
	}
	if channel != "" {
		if e == nil || e.replicationStopForChannel == nil {
			return true, fmt.Errorf("only the default replication channel is supported")
		}
		return true, e.replicationStopForChannel(channel)
	}
	if e == nil || e.replicationStop == nil {
		return true, fmt.Errorf("replication runtime is not configured")
	}
	return true, e.replicationStop()
}

var resetReplicaPattern = regexp.MustCompile(`(?is)^\s*reset\s+(?:replica|slave)(?:\s+(all))?(?:\s+for\s+channel\s+(?:"((?:""|[^"])*)"|'((?:''|[^'])*)'|([^\s]+)))?\s*$`)

var replicationChannelPattern = regexp.MustCompile(`(?is)^\s*(?:start|stop)\s+(?:replica|slave)\s+for\s+channel\s+(?:"((?:""|[^"])*)"|'((?:''|[^'])*)'|([^\s]+))\s*$`)

func parseReplicationChannelControl(query string) (channel string, matched bool) {
	match := replicationChannelPattern.FindStringSubmatch(query)
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

func parseResetReplicaStatement(query string) (resetAll bool, channel string, valid bool) {
	match := resetReplicaPattern.FindStringSubmatch(query)
	if len(match) != 5 {
		return false, "", false
	}
	resetAll = strings.EqualFold(strings.TrimSpace(match[1]), "all")
	channel = match[2]
	if channel == "" {
		channel = match[3]
	}
	if channel == "" {
		channel = match[4]
	}
	channel = strings.ReplaceAll(channel, `""`, `"`)
	channel = strings.ReplaceAll(channel, "''", "'")
	return resetAll, strings.TrimSpace(channel), true
}

var replicationFilterOptionPattern = regexp.MustCompile(`(?is)^\s*(replicate_do_db|replicate_ignore_db|replicate_do_table|replicate_ignore_table|replicate_wild_do_table|replicate_wild_ignore_table|replicate_rewrite_db)\s*=\s*\((.*)\)\s*$`)

var replicationChannelSuffixPattern = regexp.MustCompile(`(?is)^(.+)\s+for\s+channel\s+(?:"((?:""|[^"])*)"|'((?:''|[^'])*)'|([^\s]+))\s*$`)

func splitReplicationChannelSuffix(query string) (base, channel string, matched bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	match := replicationChannelSuffixPattern.FindStringSubmatch(trimmed)
	if len(match) != 5 {
		return trimmed, "", false
	}
	channel = match[2]
	if channel == "" {
		channel = match[3]
	}
	if channel == "" {
		channel = match[4]
	}
	channel = strings.ReplaceAll(channel, `""`, `"`)
	channel = strings.ReplaceAll(channel, "''", "'")
	return strings.TrimSpace(match[1]), strings.TrimSpace(channel), true
}

func parseReplicationFilter(query string) (replication.ReplicationFilterConfig, error) {
	var config replication.ReplicationFilterConfig
	trimmed, _, _ := splitReplicationChannelSuffix(query)
	prefix := "change replication filter"
	if len(trimmed) <= len(prefix) || !strings.EqualFold(trimmed[:len(prefix)], prefix) {
		return config, fmt.Errorf("invalid CHANGE REPLICATION FILTER syntax")
	}
	options := strings.TrimSpace(trimmed[len(prefix):])
	if options == "" {
		return config, fmt.Errorf("CHANGE REPLICATION FILTER requires a filter option")
	}
	seen := make(map[string]struct{})
	for _, rawOption := range splitTopLevelComma(options) {
		match := replicationFilterOptionPattern.FindStringSubmatch(rawOption)
		if len(match) != 3 {
			return config, fmt.Errorf("unsupported CHANGE REPLICATION FILTER option %q", strings.TrimSpace(rawOption))
		}
		name := strings.ToLower(match[1])
		if _, exists := seen[name]; exists {
			return config, fmt.Errorf("CHANGE REPLICATION FILTER option %s specified more than once", strings.ToUpper(name))
		}
		seen[name] = struct{}{}
		switch name {
		case "replicate_do_db":
			values, err := parseReplicationFilterValues(match[2])
			if err != nil {
				return config, fmt.Errorf("%s: %w", strings.ToUpper(name), err)
			}
			config.ReplicateDoDB = values
		case "replicate_ignore_db":
			values, err := parseReplicationFilterValues(match[2])
			if err != nil {
				return config, fmt.Errorf("%s: %w", strings.ToUpper(name), err)
			}
			config.ReplicateIgnoreDB = values
		case "replicate_do_table":
			values, err := parseReplicationFilterValues(match[2])
			if err != nil {
				return config, fmt.Errorf("%s: %w", strings.ToUpper(name), err)
			}
			config.ReplicateDoTable = values
		case "replicate_ignore_table":
			values, err := parseReplicationFilterValues(match[2])
			if err != nil {
				return config, fmt.Errorf("%s: %w", strings.ToUpper(name), err)
			}
			config.ReplicateIgnoreTable = values
		case "replicate_wild_do_table":
			values, err := parseReplicationFilterValues(match[2])
			if err != nil {
				return config, fmt.Errorf("%s: %w", strings.ToUpper(name), err)
			}
			config.ReplicateWildDoTable = values
		case "replicate_wild_ignore_table":
			values, err := parseReplicationFilterValues(match[2])
			if err != nil {
				return config, fmt.Errorf("%s: %w", strings.ToUpper(name), err)
			}
			config.ReplicateWildIgnoreTable = values
		case "replicate_rewrite_db":
			rewrites, err := parseReplicationRewriteDBValues(match[2])
			if err != nil {
				return config, fmt.Errorf("%s: %w", strings.ToUpper(name), err)
			}
			config.ReplicateRewriteDB = rewrites
		}
	}
	return config, nil
}

func parseReplicationRewriteDBValues(raw string) ([]replication.ReplicationDBRewrite, error) {
	values := make([]replication.ReplicationDBRewrite, 0)
	for _, token := range splitTopLevelComma(strings.TrimSpace(raw)) {
		pair := strings.TrimSpace(token)
		if len(pair) < 2 || pair[0] != '(' || pair[len(pair)-1] != ')' {
			return nil, fmt.Errorf("rewrite rule must be a (from_db, to_db) pair")
		}
		parts := splitTopLevelComma(pair[1 : len(pair)-1])
		if len(parts) != 2 {
			return nil, fmt.Errorf("rewrite rule must contain exactly two database names")
		}
		from := unquoteReplicationFilterValue(parts[0])
		to := unquoteReplicationFilterValue(parts[1])
		if from == "" || to == "" {
			return nil, fmt.Errorf("rewrite database names cannot be empty")
		}
		values = append(values, replication.ReplicationDBRewrite{From: from, To: to})
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("rewrite rule list cannot be empty")
	}
	return values, nil
}

func unquoteReplicationFilterValue(raw string) string {
	value := strings.TrimSpace(raw)
	if len(value) >= 2 && ((value[0] == '\'' && value[len(value)-1] == '\'') || (value[0] == '"' && value[len(value)-1] == '"')) {
		value = value[1 : len(value)-1]
		value = strings.ReplaceAll(value, "''", "'")
		value = strings.ReplaceAll(value, `""`, `"`)
	}
	return strings.TrimSpace(value)
}

func parseReplicationFilterValues(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []string{}, nil
	}
	values := make([]string, 0)
	for _, token := range splitTopLevelComma(raw) {
		value := strings.TrimSpace(token)
		if len(value) >= 2 && ((value[0] == '\'' && value[len(value)-1] == '\'') || (value[0] == '"' && value[len(value)-1] == '"')) {
			value = value[1 : len(value)-1]
			value = strings.ReplaceAll(value, "''", "'")
			value = strings.ReplaceAll(value, `""`, `"`)
		}
		if strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("filter rule cannot be empty")
		}
		values = append(values, value)
	}
	return values, nil
}

var purgeBinaryLogsPattern = regexp.MustCompile(`(?is)^purge\s+(?:binary|master)\s+logs\s+to\s+['"]([^'"]+)['"]$`)
var purgeBinaryLogsBeforePattern = regexp.MustCompile(`(?is)^purge\s+(?:binary|master)\s+logs\s+before\s+['"]([^'"]+)['"]$`)
var resetBinaryLogsAndGTIDsPattern = regexp.MustCompile(`(?is)^reset\s+binary\s+logs\s+and\s+gtids(?:\s+to\s+([0-9]+))?$`)

func parseBinlogPurgeTime(value string) (time.Time, error) {
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05", time.RFC3339} {
		if parsed, err := time.ParseInLocation(layout, value, time.UTC); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid PURGE BINARY LOGS timestamp %q", value)
}

var replicationSourceOptionPattern = regexp.MustCompile(`(?is)^\s*(source_host|master_host|source_port|master_port|source_user|master_user|source_password|master_password|source_log_file|master_log_file|source_log_pos|master_log_pos|source_auto_position|master_auto_position|source_ssl|master_ssl|source_ssl_verify_server_cert|master_ssl_verify_server_cert|source_ssl_ca|master_ssl_ca|source_ssl_cert|master_ssl_cert|source_ssl_key|master_ssl_key|source_connect_retry|master_connect_retry|source_retry_count|master_retry_count|source_heartbeat_period|master_heartbeat_period|source_compression_algorithms|master_compression_algorithms|source_zstd_compression_level|master_zstd_compression_level)\s*=\s*(?:'((?:''|[^'])*)'|"((?:""|[^"])*)"|([^\s]+))\s*$`)

func parseReplicationSourceURL(query string) (string, error) {
	query, _, _ = splitReplicationChannelSuffix(query)
	lower := strings.ToLower(strings.TrimSpace(query))
	prefix := "change replication source to"
	if strings.HasPrefix(lower, "change master to") {
		prefix = "change master to"
	}
	options := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query)[len(prefix):], ";"))
	if options == "" {
		return "", fmt.Errorf("CHANGE REPLICATION SOURCE requires SOURCE_HOST")
	}
	host := ""
	port := 0
	user := ""
	password := ""
	passwordSet := false
	logFile := ""
	logPosition := ""
	autoPosition := ""
	ssl := ""
	sslVerify := ""
	sslCA := ""
	sslCert := ""
	sslKey := ""
	connectRetry := ""
	retryCount := ""
	heartbeatPeriod := ""
	compressionAlgorithms := ""
	zstdCompressionLevel := ""
	for _, rawOption := range splitTopLevelComma(options) {
		match := replicationSourceOptionPattern.FindStringSubmatch(rawOption)
		if len(match) == 0 {
			return "", fmt.Errorf("unsupported CHANGE REPLICATION SOURCE option %q", strings.TrimSpace(rawOption))
		}
		value := match[2]
		if value == "" {
			value = match[3]
		}
		if value == "" {
			value = match[4]
		}
		value = strings.ReplaceAll(value, "''", "'")
		value = strings.ReplaceAll(value, `""`, `"`)
		switch strings.ToLower(match[1]) {
		case "source_host", "master_host":
			if host != "" {
				return "", fmt.Errorf("SOURCE_HOST specified more than once")
			}
			host = strings.TrimSpace(value)
		case "source_port", "master_port":
			if port != 0 {
				return "", fmt.Errorf("SOURCE_PORT specified more than once")
			}
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed < 1 || parsed > 65535 {
				return "", fmt.Errorf("invalid SOURCE_PORT %q", value)
			}
			port = parsed
		case "source_user", "master_user":
			if user != "" {
				return "", fmt.Errorf("SOURCE_USER specified more than once")
			}
			user = strings.TrimSpace(value)
		case "source_password", "master_password":
			if passwordSet {
				return "", fmt.Errorf("SOURCE_PASSWORD specified more than once")
			}
			password = value
			passwordSet = true
		case "source_log_file", "master_log_file":
			if logFile != "" {
				return "", fmt.Errorf("SOURCE_LOG_FILE specified more than once")
			}
			logFile = strings.TrimSpace(value)
		case "source_log_pos", "master_log_pos":
			if logPosition != "" {
				return "", fmt.Errorf("SOURCE_LOG_POS specified more than once")
			}
			logPosition = strings.TrimSpace(value)
		case "source_auto_position", "master_auto_position":
			if autoPosition != "" {
				return "", fmt.Errorf("SOURCE_AUTO_POSITION specified more than once")
			}
			autoPosition = strings.TrimSpace(value)
		case "source_ssl", "master_ssl":
			if ssl != "" {
				return "", fmt.Errorf("SOURCE_SSL specified more than once")
			}
			ssl = strings.TrimSpace(value)
		case "source_ssl_verify_server_cert", "master_ssl_verify_server_cert":
			if sslVerify != "" {
				return "", fmt.Errorf("SOURCE_SSL_VERIFY_SERVER_CERT specified more than once")
			}
			sslVerify = strings.TrimSpace(value)
		case "source_ssl_ca", "master_ssl_ca":
			if sslCA != "" {
				return "", fmt.Errorf("SOURCE_SSL_CA specified more than once")
			}
			sslCA = strings.TrimSpace(value)
		case "source_ssl_cert", "master_ssl_cert":
			if sslCert != "" {
				return "", fmt.Errorf("SOURCE_SSL_CERT specified more than once")
			}
			sslCert = strings.TrimSpace(value)
		case "source_ssl_key", "master_ssl_key":
			if sslKey != "" {
				return "", fmt.Errorf("SOURCE_SSL_KEY specified more than once")
			}
			sslKey = strings.TrimSpace(value)
		case "source_connect_retry", "master_connect_retry":
			if connectRetry != "" {
				return "", fmt.Errorf("SOURCE_CONNECT_RETRY specified more than once")
			}
			connectRetry = strings.TrimSpace(value)
		case "source_retry_count", "master_retry_count":
			if retryCount != "" {
				return "", fmt.Errorf("SOURCE_RETRY_COUNT specified more than once")
			}
			retryCount = strings.TrimSpace(value)
		case "source_heartbeat_period", "master_heartbeat_period":
			if heartbeatPeriod != "" {
				return "", fmt.Errorf("SOURCE_HEARTBEAT_PERIOD specified more than once")
			}
			heartbeatPeriod = strings.TrimSpace(value)
		case "source_compression_algorithms", "master_compression_algorithms":
			if compressionAlgorithms != "" {
				return "", fmt.Errorf("SOURCE_COMPRESSION_ALGORITHMS specified more than once")
			}
			compressionAlgorithms = strings.TrimSpace(value)
		case "source_zstd_compression_level", "master_zstd_compression_level":
			if zstdCompressionLevel != "" {
				return "", fmt.Errorf("SOURCE_ZSTD_COMPRESSION_LEVEL specified more than once")
			}
			zstdCompressionLevel = strings.TrimSpace(value)
		}
	}
	if host == "" {
		return "", fmt.Errorf("CHANGE REPLICATION SOURCE requires SOURCE_HOST")
	}
	scheme := "http"
	if strings.Contains(host, "://") {
		parsedScheme, err := url.ParseRequestURI(host)
		if err != nil || parsedScheme.Host == "" {
			return "", fmt.Errorf("invalid SOURCE_HOST %q", host)
		}
		scheme = strings.ToLower(parsedScheme.Scheme)
	} else if user != "" {
		scheme = "mysql"
		host = scheme + "://" + host
	} else {
		host = "http://" + host
	}
	parsed, err := url.ParseRequestURI(host)
	if err != nil || ((scheme != "http" && scheme != "https" && scheme != "mysql") || parsed.Scheme != scheme) || parsed.Host == "" {
		return "", fmt.Errorf("invalid SOURCE_HOST %q", host)
	}
	if port != 0 {
		parsed.Host = net.JoinHostPort(parsed.Hostname(), strconv.Itoa(port))
	}
	if scheme == "mysql" {
		if user == "" {
			return "", fmt.Errorf("native MySQL SOURCE_HOST requires SOURCE_USER")
		}
		if passwordSet {
			parsed.User = url.UserPassword(user, password)
		} else {
			parsed.User = url.User(user)
		}
		values := parsed.Query()
		if logFile != "" {
			values.Set("binlog_file", logFile)
		}
		if logPosition != "" {
			position, parseErr := strconv.ParseUint(logPosition, 10, 64)
			if parseErr != nil || position < 4 {
				return "", fmt.Errorf("invalid SOURCE_LOG_POS %q", logPosition)
			}
			values.Set("binlog_pos", strconv.FormatUint(position, 10))
		}
		if autoPosition != "" {
			parsedAuto, parseErr := parseReplicationBool(autoPosition)
			if parseErr != nil {
				return "", parseErr
			}
			values.Set("gtid_auto_position", strconv.FormatBool(parsedAuto))
		} else {
			values.Set("gtid_auto_position", "true")
		}
		if ssl != "" {
			parsedSSL, parseErr := parseReplicationBool(ssl)
			if parseErr != nil {
				return "", fmt.Errorf("invalid SOURCE_SSL %q", ssl)
			}
			values.Set("ssl", strconv.FormatBool(parsedSSL))
		}
		if sslVerify != "" {
			parsedVerify, parseErr := parseReplicationBool(sslVerify)
			if parseErr != nil {
				return "", fmt.Errorf("invalid SOURCE_SSL_VERIFY_SERVER_CERT %q", sslVerify)
			}
			values.Set("ssl_verify_server_cert", strconv.FormatBool(parsedVerify))
		}
		if sslCA != "" {
			values.Set("ssl_ca", sslCA)
		}
		if sslCert != "" {
			values.Set("ssl_cert", sslCert)
		}
		if sslKey != "" {
			values.Set("ssl_key", sslKey)
		}
		if connectRetry != "" {
			if _, parseErr := strconv.ParseUint(connectRetry, 10, 64); parseErr != nil {
				return "", fmt.Errorf("invalid SOURCE_CONNECT_RETRY %q", connectRetry)
			}
			values.Set("connect_retry", connectRetry)
		}
		if retryCount != "" {
			if _, parseErr := strconv.ParseUint(retryCount, 10, 64); parseErr != nil {
				return "", fmt.Errorf("invalid SOURCE_RETRY_COUNT %q", retryCount)
			}
			values.Set("connect_retry_count", retryCount)
		}
		if heartbeatPeriod != "" {
			if parsedHeartbeat, parseErr := strconv.ParseFloat(heartbeatPeriod, 64); parseErr != nil || parsedHeartbeat < 0 {
				return "", fmt.Errorf("invalid SOURCE_HEARTBEAT_PERIOD %q", heartbeatPeriod)
			}
			values.Set("heartbeat_interval", heartbeatPeriod)
		}
		if compressionAlgorithms != "" {
			values.Set("compression_algorithm", compressionAlgorithms)
		}
		if zstdCompressionLevel != "" {
			if parsedLevel, parseErr := strconv.ParseInt(zstdCompressionLevel, 10, 64); parseErr != nil || parsedLevel < 0 {
				return "", fmt.Errorf("invalid SOURCE_ZSTD_COMPRESSION_LEVEL %q", zstdCompressionLevel)
			}
			values.Set("zstd_compression_level", zstdCompressionLevel)
		}
		parsed.RawQuery = values.Encode()
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

func parseReplicationBool(value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "on", "true":
		return true, nil
	case "0", "off", "false":
		return false, nil
	default:
		return false, fmt.Errorf("invalid SOURCE_AUTO_POSITION %q", value)
	}
}

var tableMaintenancePattern = regexp.MustCompile(`(?is)^\s*(check|analyze|optimize)\s+(?:(?:no_write_to_binlog|local)\s+)?table\s+(.+?)\s*$`)
var tableMaintenanceTargetPattern = regexp.MustCompile("(?is)^\\s*(`?[a-zA-Z0-9_$]+`?)(?:\\s*\\.\\s*(`?[a-zA-Z0-9_$]+`?))?\\s*$")
var tableMaintenanceUpgradeSuffixPattern = regexp.MustCompile(`(?is)\s+for\s+upgrade\s*$`)
var histogramMaintenancePattern = regexp.MustCompile(`(?is)^\s*analyze\s+(?:(?:no_write_to_binlog|local)\s+)?table\s+(.+?)\s+(update|drop)\s+histogram\s+on\s+(.+?)(?:\s+with\s+([0-9]+)\s+buckets?)?\s*;?\s*$`)

func isTableMaintenanceStatement(query string) bool {
	return tableMaintenancePattern.MatchString(strings.TrimSpace(query))
}

func (e *XMySQLExecutor) executeTableMaintenance(ctx *ExecutionContext, query string) (*Result, error) {
	defaultSchema := ""
	if ctx != nil {
		defaultSchema = strings.TrimSpace(ctx.DatabaseName)
	}
	if histogram, matched, err := parseHistogramMaintenance(query, defaultSchema); matched || err != nil {
		if err != nil {
			return nil, err
		}
		return e.executeHistogramMaintenance(ctx, histogram)
	}
	match := tableMaintenancePattern.FindStringSubmatch(query)
	if len(match) != 3 {
		return nil, fmt.Errorf("invalid table maintenance statement")
	}
	targets, err := parseTableMaintenanceTargets(match[2], defaultSchema)
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("table maintenance requires at least one table")
	}
	operation := strings.ToUpper(match[1])
	rows := make([][]interface{}, 0, len(targets))
	for _, target := range targets {
		if operation == "CHECK" {
			if err := e.checkTableHasAnyPrivilege(ctx, target.schema, target.table); err != nil {
				return nil, err
			}
		} else {
			if err := e.checkTablePrivilege(ctx, target.schema, target.table, "SELECT"); err != nil {
				return nil, err
			}
			if err := e.checkTablePrivilege(ctx, target.schema, target.table, "INSERT"); err != nil {
				return nil, err
			}
		}
		info, err := e.readPersistedTableInfo(target.schema, target.table)
		if err != nil {
			return nil, err
		}
		if len(info.Columns) == 0 {
			return nil, fmt.Errorf("table '%s.%s' has no columns", target.schema, target.table)
		}
		rowCount, scanErr := e.physicalTableRowCountStrict(target.schema, target.table)
		if scanErr != nil {
			return nil, scanErr
		}
		if operation == "CHECK" && e.tableStorageManager != nil {
			btree, btreeErr := e.tableStorageManager.CreateBTreeManagerForTable(context.Background(), target.schema, target.table)
			if btreeErr != nil {
				return nil, btreeErr
			}
			if checker, ok := btree.(interface{ CheckConsistency(context.Context) error }); ok {
				if err := checker.CheckConsistency(context.Background()); err != nil {
					return nil, err
				}
			}
		}
		if operation == "ANALYZE" || operation == "OPTIMIZE" {
			stats, statsErr := e.collectTableStatistics(ctx, target.schema, target.table, info, rowCount)
			if statsErr != nil {
				return nil, statsErr
			}
			e.preserveColumnHistogramMetadata(target.schema, target.table, info, stats)
			if e.infosSchemaManager != nil {
				if err := e.infosSchemaManager.UpdateTableStats(context.Background(), target.schema, target.table, stats); err != nil {
					return nil, err
				}
			} else if err := e.persistTableStatistics(target.schema, target.table, info, stats); err != nil {
				return nil, err
			}
		}
		rows = append(rows, []interface{}{target.table, strings.ToLower(match[1]), "status", "OK"})
	}
	return &Result{
		ResultType: common.RESULT_TYPE_QUERY,
		Data: map[string]interface{}{
			"columns": []string{"Table", "Op", "Msg_type", "Msg_text"},
			"rows":    rows,
		},
		Message: fmt.Sprintf("%s TABLE completed for %d table(s)", operation, len(targets)),
	}, nil
}

type histogramMaintenance struct {
	target      tableMaintenanceTarget
	operation   string
	columns     []string
	bucketCount int
}

func parseHistogramMaintenance(query, defaultSchema string) (*histogramMaintenance, bool, error) {
	match := histogramMaintenancePattern.FindStringSubmatch(strings.TrimSpace(query))
	if len(match) == 0 {
		return nil, false, nil
	}
	targets, err := parseTableMaintenanceTargets(match[1], defaultSchema)
	if err != nil {
		return nil, true, err
	}
	if len(targets) != 1 {
		return nil, true, fmt.Errorf("histogram maintenance requires exactly one table")
	}
	operation := strings.ToUpper(strings.TrimSpace(match[2]))
	if operation == "DROP" && strings.TrimSpace(match[4]) != "" {
		return nil, true, fmt.Errorf("DROP HISTOGRAM does not accept a bucket count")
	}
	columns := make([]string, 0)
	for _, rawColumn := range splitTopLevelComma(strings.TrimSpace(match[3])) {
		column := strings.TrimSpace(rawColumn)
		if column == "" {
			return nil, true, fmt.Errorf("histogram column name cannot be empty")
		}
		if strings.HasPrefix(column, "`") && strings.HasSuffix(column, "`") {
			column = strings.Trim(column, "`")
			column = strings.ReplaceAll(column, "``", "`")
		}
		if !regexp.MustCompile(`^[a-zA-Z0-9_$]+$`).MatchString(column) {
			return nil, true, fmt.Errorf("invalid histogram column %q", column)
		}
		columns = append(columns, column)
	}
	if len(columns) == 0 {
		return nil, true, fmt.Errorf("histogram maintenance requires at least one column")
	}
	bucketCount := 100
	if match[4] != "" {
		bucketCount, err = strconv.Atoi(match[4])
		if err != nil || bucketCount < 1 || bucketCount > 1024 {
			return nil, true, fmt.Errorf("histogram bucket count must be between 1 and 1024")
		}
	}
	return &histogramMaintenance{
		target:      targets[0],
		operation:   operation,
		columns:     columns,
		bucketCount: bucketCount,
	}, true, nil
}

func (e *XMySQLExecutor) executeHistogramMaintenance(ctx *ExecutionContext, maintenance *histogramMaintenance) (*Result, error) {
	if maintenance == nil {
		return nil, fmt.Errorf("histogram maintenance is nil")
	}
	if err := e.checkTablePrivilege(ctx, maintenance.target.schema, maintenance.target.table, "SELECT"); err != nil {
		return nil, err
	}
	if err := e.checkTablePrivilege(ctx, maintenance.target.schema, maintenance.target.table, "INSERT"); err != nil {
		return nil, err
	}
	info, err := e.readPersistedTableInfo(maintenance.target.schema, maintenance.target.table)
	if err != nil {
		return nil, err
	}
	if len(info.Columns) == 0 {
		return nil, fmt.Errorf("table '%s.%s' has no columns", maintenance.target.schema, maintenance.target.table)
	}
	rowCount, err := e.physicalTableRowCountStrict(maintenance.target.schema, maintenance.target.table)
	if err != nil {
		return nil, err
	}
	stats, err := e.collectTableStatistics(ctx, maintenance.target.schema, maintenance.target.table, info, rowCount)
	if err != nil {
		return nil, err
	}
	e.preserveColumnHistogramMetadata(maintenance.target.schema, maintenance.target.table, info, stats)

	columnPositions := make(map[string]int, len(info.Columns))
	columnNames := make([]string, 0, len(info.Columns))
	for position, column := range info.Columns {
		name, _ := column["name"].(string)
		columnNames = append(columnNames, name)
		columnPositions[strings.ToLower(name)] = position
	}
	for _, columnName := range maintenance.columns {
		if _, ok := columnPositions[strings.ToLower(columnName)]; !ok {
			return nil, fmt.Errorf("unknown column '%s' in histogram", columnName)
		}
	}
	if maintenance.operation == "UPDATE" {
		for _, columnName := range maintenance.columns {
			histogram, histogramErr := e.collectColumnHistogram(ctx, maintenance.target.schema, maintenance.target.table, columnNames, columnName, maintenance.bucketCount)
			if histogramErr != nil {
				return nil, histogramErr
			}
			actualColumnName := columnNames[columnPositions[strings.ToLower(columnName)]]
			stat := stats.ColumnStats[actualColumnName]
			if existing, ok := informationSchemaColumnStat(stats.ColumnStats, actualColumnName); ok {
				stat = existing
			}
			stat.Histogram = histogram
			stat.HistogramDropped = false
			stats.ColumnStats[actualColumnName] = stat
		}
	} else {
		for _, columnName := range maintenance.columns {
			actualColumnName := columnNames[columnPositions[strings.ToLower(columnName)]]
			stat := stats.ColumnStats[actualColumnName]
			if existing, ok := informationSchemaColumnStat(stats.ColumnStats, actualColumnName); ok {
				stat = existing
			}
			stat.Histogram = nil
			stat.HistogramDropped = true
			stats.ColumnStats[actualColumnName] = stat
		}
	}
	if e.infosSchemaManager != nil {
		if err := e.infosSchemaManager.UpdateTableStats(context.Background(), maintenance.target.schema, maintenance.target.table, stats); err != nil {
			return nil, err
		}
	} else if err := e.persistTableStatistics(maintenance.target.schema, maintenance.target.table, info, stats); err != nil {
		return nil, err
	}
	return &Result{
		ResultType: common.RESULT_TYPE_QUERY,
		Data: map[string]interface{}{
			"columns": []string{"Table", "Op", "Msg_type", "Msg_text"},
			"rows":    [][]interface{}{{maintenance.target.table, "analyze", "status", "OK"}},
		},
		Message: fmt.Sprintf("%s HISTOGRAM completed for %s column(s)", maintenance.operation, strings.Join(maintenance.columns, ",")),
	}, nil
}

// preserveColumnHistogramMetadata keeps explicitly managed histograms across
// a normal ANALYZE/OPTIMIZE refresh. MySQL's scalar statistics refresh does
// not implicitly DROP HISTOGRAM; only an explicit DROP HISTOGRAM should clear
// the I_S row.
func (e *XMySQLExecutor) preserveColumnHistogramMetadata(schemaName, tableName string, info *persistedTableInfo, stats *metadata.InfoTableStats) {
	if stats == nil {
		return
	}
	var existing *metadata.InfoTableStats
	if e != nil && e.infosSchemaManager != nil {
		if loaded, err := e.infosSchemaManager.GetTableStats(context.Background(), schemaName, tableName); err == nil {
			existing = loaded
		}
	} else if info != nil {
		existing = info.Stats
	}
	if existing == nil {
		return
	}
	for name, existingStat := range existing.ColumnStats {
		if current, ok := stats.ColumnStats[name]; ok {
			current.Histogram = existingStat.Histogram
			current.HistogramDropped = existingStat.HistogramDropped
			stats.ColumnStats[name] = current
		}
	}
}

func (e *XMySQLExecutor) collectColumnHistogram(ctx *ExecutionContext, schemaName, tableName string, columnNames []string, columnName string, bucketCount int) (*metadata.ColumnHistogram, error) {
	stmt, err := sqlparser.Parse("select * from " + quoteMaintenanceIdentifier(tableName))
	if err != nil {
		return nil, err
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("histogram source is not SELECT")
	}
	result, err := e.executeSelectStatement(ctx, selectStmt, schemaName)
	if err != nil {
		return nil, err
	}
	position := indexOfFold(columnNames, columnName)
	if position < 0 {
		return nil, fmt.Errorf("unknown column '%s' in histogram", columnName)
	}
	values := make([]interface{}, 0, len(result.Records))
	for _, record := range result.Records {
		rawValues := record.GetValues()
		if position >= len(rawValues) || rawValues[position] == nil || rawValues[position].IsNull() {
			continue
		}
		values = append(values, normalizeMaintenanceStatsValue(rawValues[position].Raw(), result.ColumnTypes, position))
	}
	histogram := &metadata.ColumnHistogram{
		NullValues:               float64(len(result.Records)-len(values)) / float64(maxInt(1, len(result.Records))),
		SamplingRate:             1.0,
		HistogramType:            "equi-height",
		NumberOfBucketsSpecified: bucketCount,
	}
	if len(values) == 0 {
		return histogram, nil
	}
	sort.SliceStable(values, func(i, j int) bool { return compareScalarValues(values[i], values[j]) < 0 })
	if bucketCount > len(values) {
		bucketCount = len(values)
	}
	for bucket := 0; bucket < bucketCount; bucket++ {
		start := bucket * len(values) / bucketCount
		end := (bucket + 1) * len(values) / bucketCount
		if end <= start {
			end = start + 1
		}
		if end > len(values) {
			end = len(values)
		}
		distinct := make(map[string]struct{}, end-start)
		for _, value := range values[start:end] {
			distinct[fmt.Sprintf("%T:%v", value, value)] = struct{}{}
		}
		histogram.Buckets = append(histogram.Buckets, metadata.ColumnHistogramBucket{
			LowerBound:          values[start],
			UpperBound:          values[end-1],
			CumulativeFrequency: float64(end) / float64(len(values)),
			DistinctCount:       uint64(len(distinct)),
		})
	}
	return histogram, nil
}

func maxInt(value, minimum int) int {
	if value < minimum {
		return minimum
	}
	return value
}

type tableMaintenanceTarget struct {
	schema string
	table  string
}

func parseTableMaintenanceTargets(spec, defaultSchema string) ([]tableMaintenanceTarget, error) {
	spec = tableMaintenanceUpgradeSuffixPattern.ReplaceAllString(strings.TrimSpace(spec), "")
	parts := splitTopLevelComma(spec)
	if len(parts) == 0 {
		return nil, fmt.Errorf("table maintenance requires at least one table")
	}
	targets := make([]tableMaintenanceTarget, 0, len(parts))
	for _, part := range parts {
		match := tableMaintenanceTargetPattern.FindStringSubmatch(strings.TrimSpace(part))
		if len(match) == 0 {
			return nil, fmt.Errorf("invalid table maintenance target %q", strings.TrimSpace(part))
		}
		schema, table := strings.Trim(match[1], "`"), ""
		if strings.TrimSpace(match[2]) == "" {
			table = schema
			schema = strings.TrimSpace(defaultSchema)
		} else {
			table = strings.Trim(match[2], "`")
		}
		if schema == "" || table == "" {
			return nil, fmt.Errorf("no database selected")
		}
		targets = append(targets, tableMaintenanceTarget{schema: schema, table: table})
	}
	return targets, nil
}

func (e *XMySQLExecutor) collectTableStatistics(ctx *ExecutionContext, schemaName, tableName string, info *persistedTableInfo, rowCount int64) (*metadata.InfoTableStats, error) {
	stats := &metadata.InfoTableStats{
		RowCount:    uint64(maxInt64(rowCount, 0)),
		ColumnStats: make(map[string]metadata.Stats, len(info.Columns)),
		IndexStats:  make(map[string]metadata.Stats, len(info.Indexes)),
	}
	if rowCount == 0 {
		for _, column := range info.Columns {
			if name, ok := column["name"].(string); ok && name != "" {
				stats.ColumnStats[name] = metadata.Stats{}
			}
		}
		for _, index := range info.Indexes {
			if name, ok := index["name"].(string); ok && name != "" {
				stats.IndexStats[name] = metadata.Stats{}
			}
		}
		return stats, nil
	}

	stmt, err := sqlparser.Parse("select * from " + quoteMaintenanceIdentifier(tableName))
	if err != nil {
		return nil, err
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("ANALYZE source is not SELECT")
	}
	result, err := e.executeSelectStatement(ctx, selectStmt, schemaName)
	if err != nil {
		return nil, err
	}

	columnNames := make([]string, 0, len(info.Columns))
	for _, column := range info.Columns {
		name, _ := column["name"].(string)
		columnNames = append(columnNames, name)
	}
	columnValues := make(map[string]map[string]struct{}, len(columnNames))
	for _, name := range columnNames {
		columnValues[name] = make(map[string]struct{})
	}
	rowSize := uint64(0)
	for _, record := range result.Records {
		values := record.GetValues()
		for index, name := range columnNames {
			if index >= len(values) || values[index] == nil {
				continue
			}
			value := values[index]
			columnStat := stats.ColumnStats[name]
			if value.IsNull() {
				columnStat.NullCount++
				stats.ColumnStats[name] = columnStat
				continue
			}
			raw := normalizeMaintenanceStatsValue(value.Raw(), result.ColumnTypes, index)
			key := fmt.Sprintf("%T:%v", raw, raw)
			columnValues[name][key] = struct{}{}
			text := value.ToString()
			rowSize += uint64(len(text))
			// Compare values using the same MySQL-compatible numeric/string
			// coercion used by execution. Lexical fmt.Sprint ordering makes
			// numeric stats wrong for values such as 2 and 10.
			if columnStat.MinValue == nil || compareScalarValues(raw, columnStat.MinValue) < 0 {
				columnStat.MinValue = raw
			}
			if columnStat.MaxValue == nil || compareScalarValues(raw, columnStat.MaxValue) > 0 {
				columnStat.MaxValue = raw
			}
			columnStat.AvgLength += float64(len(text))
			stats.ColumnStats[name] = columnStat
		}
	}
	for name, values := range columnValues {
		columnStat := stats.ColumnStats[name]
		columnStat.DistinctCount = uint64(len(values))
		nonNull := int64(stats.RowCount) - int64(columnStat.NullCount)
		if nonNull > 0 {
			columnStat.AvgLength /= float64(nonNull)
		}
		stats.ColumnStats[name] = columnStat
	}
	if stats.RowCount > 0 {
		// The compatibility executor does not expose the physical tablespace
		// byte counters to ANALYZE. Persist the measured logical payload size so
		// INFORMATION_SCHEMA/optimizer consumers do not observe a misleading
		// zero; physical page and index allocation sampling remains separate.
		stats.DataSize = rowSize
		stats.AvgRowSize = uint32(rowSize / stats.RowCount)
	}
	for _, index := range info.Indexes {
		name, _ := index["name"].(string)
		columns := persistedIndexColumns(index["columns"])
		if name == "" || len(columns) == 0 {
			continue
		}
		keys := make(map[string]struct{})
		nullCount := uint64(0)
		for _, record := range result.Records {
			values := record.GetValues()
			parts := make([]string, 0, len(columns))
			nullKey := false
			for _, column := range columns {
				columnName := fmt.Sprint(column)
				position := indexOfFold(columnNames, columnName)
				if position < 0 || position >= len(values) || values[position] == nil || values[position].IsNull() {
					nullKey = true
					break
				}
				normalized := normalizeMaintenanceStatsValue(values[position].Raw(), result.ColumnTypes, position)
				parts = append(parts, fmt.Sprintf("%T:%v", normalized, normalized))
			}
			if nullKey {
				nullCount++
				continue
			}
			keys[strings.Join(parts, "\x00")] = struct{}{}
		}
		stats.IndexStats[name] = metadata.Stats{DistinctCount: uint64(len(keys)), NullCount: nullCount}
	}
	// Persist a durable compatibility estimate for index bytes alongside the
	// logical row payload.  The page counters are maintained by the clustered
	// B+Tree and secondary-index managers; multiplying by the configured page
	// size keeps this value useful to INFORMATION_SCHEMA/optimizer consumers
	// without pretending to provide upstream's full physical sampling detail.
	for _, index := range info.Indexes {
		name, _ := index["name"].(string)
		if strings.TrimSpace(name) == "" {
			continue
		}
		pages := e.informationSchemaIndexPages(context.Background(), schemaName, tableName, name)
		if pages > 0 {
			stats.IndexSize += uint64(pages) * uint64(manager.PAGE_SIZE)
		}
	}
	return stats, nil
}

func persistedIndexColumns(raw interface{}) []interface{} {
	switch values := raw.(type) {
	case []interface{}:
		return values
	case []string:
		columns := make([]interface{}, len(values))
		for index, value := range values {
			columns[index] = value
		}
		return columns
	default:
		return nil
	}
}

func (e *XMySQLExecutor) persistTableStatistics(schemaName, tableName string, info *persistedTableInfo, stats *metadata.InfoTableStats) error {
	info.Stats = stats
	path := filepath.Join(e.getDataDir(), schemaName, tableName+".frm")
	raw, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal table statistics: %w", err)
	}
	if err := writeMetadataFileAtomic(path, raw); err != nil {
		return fmt.Errorf("persist table statistics: %w", err)
	}
	if err := e.persistTableStatisticsSidecar(schemaName, tableName, info, stats); err != nil {
		return fmt.Errorf("persist table statistics sidecar: %w", err)
	}
	return nil
}

func quoteMaintenanceIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

func normalizeMaintenanceStatsValue(value interface{}, columnTypes []string, index int) interface{} {
	bytes, ok := value.([]byte)
	if !ok || index < 0 || index >= len(columnTypes) {
		return value
	}
	typeName := strings.ToUpper(strings.TrimSpace(columnTypes[index]))
	if typeName == "" {
		return value
	}
	if strings.Contains(typeName, "DECIMAL") || strings.Contains(typeName, "NUMERIC") ||
		strings.Contains(typeName, "FLOAT") || strings.Contains(typeName, "DOUBLE") {
		if number, err := strconv.ParseFloat(strings.TrimSpace(string(bytes)), 64); err == nil {
			return number
		}
		return value
	}
	if strings.Contains(typeName, "INT") || typeName == "YEAR" || typeName == "BIT" {
		if strings.Contains(typeName, "UNSIGNED") {
			if number, err := strconv.ParseUint(strings.TrimSpace(string(bytes)), 10, 64); err == nil {
				return number
			}
		} else if number, err := strconv.ParseInt(strings.TrimSpace(string(bytes)), 10, 64); err == nil {
			return number
		}
	}
	return value
}

func indexOfFold(values []string, target string) int {
	for index, value := range values {
		if strings.EqualFold(value, target) {
			return index
		}
	}
	return -1
}

func maxInt64(value, minimum int64) int64 {
	if value < minimum {
		return minimum
	}
	return value
}

func (e *XMySQLExecutor) physicalTableRowCount(schemaName, tableName string) int64 {
	count, _ := e.physicalTableRowCountStrict(schemaName, tableName)
	return count
}

func (e *XMySQLExecutor) physicalTableRowCountStrict(schemaName, tableName string) (int64, error) {
	if e.tableStorageManager == nil {
		return 0, nil
	}
	btree, err := e.tableStorageManager.CreateBTreeManagerForTable(context.Background(), schemaName, tableName)
	if err != nil {
		return 0, nil
	}
	scanner, ok := btree.(interface {
		FullScan(context.Context) ([]basic.Row, error)
	})
	if !ok {
		return 0, nil
	}
	rows, err := scanner.FullScan(context.Background())
	if err != nil {
		return 0, fmt.Errorf("scan table for maintenance failed: %v", err)
	}
	return int64(len(rows)), nil
}

func checkSessionTableLock(session server.MySQLServerSession, query, databaseName string) error {
	if session == nil {
		return nil
	}
	tables := statementTableAccesses(query, databaseName)
	if len(tables) == 0 {
		return nil
	}
	for _, qualifiedTable := range tables {
		_, table := compatibilityQualifiedTable(qualifiedTable, "")
		mode, exists := sessionTableLockMode(session, table)
		if !exists {
			value := session.GetParamByName("locked_tables")
			if value == nil {
				return nil
			}
			if locks, ok := value.(map[string]string); !ok || len(locks) == 0 {
				return nil
			}
			return fmt.Errorf("table '%s' was not locked with LOCK TABLES", table)
		}
		lower := strings.ToLower(strings.TrimSpace(query))
		write := strings.HasPrefix(lower, "insert") || strings.HasPrefix(lower, "update") || strings.HasPrefix(lower, "delete") || strings.HasPrefix(lower, "replace") || strings.HasPrefix(lower, "alter") || strings.HasPrefix(lower, "drop") || strings.HasPrefix(lower, "truncate")
		if write && mode != "write" {
			return fmt.Errorf("table '%s' was locked with a READ lock", table)
		}
	}
	return nil
}

func referencedTableForLockCheck(query string) string {
	pattern := regexp.MustCompile("(?is)^\\s*(?:select\\s+.+?\\s+from|insert\\s+(?:ignore\\s+)?into|replace\\s+into|update|delete\\s+from|alter\\s+table|drop\\s+table|truncate\\s+table)\\s+((?:`?[a-zA-Z0-9_$]+`?\\s*\\.\\s*)?`?[a-zA-Z0-9_$]+`?)")
	match := pattern.FindStringSubmatch(query)
	if len(match) > 1 {
		return match[1]
	}
	return ""
}
