package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
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
			targets, targetErr = tableMaintenanceTargets(q, ctxDatabaseName(ctx))
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

func parsedFlushStatement(query string) *sqlparser.AdminStatement {
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		return nil
	}
	stmt, ok := parsed.AST.(*sqlparser.AdminStatement)
	if !ok || stmt.Kind != "flush" {
		return nil
	}
	return stmt
}

func flushOptimizerCostsStatement(query string) bool {
	stmt := parsedFlushStatement(query)
	if stmt == nil || stmt.Options["optimizer_costs"] != "true" {
		return false
	}
	for key := range stmt.Options {
		if key != "optimizer_costs" && key != "local" && key != "no_write_to_binlog" {
			return false
		}
	}
	return true
}

func (e *XMySQLExecutor) executeFlushOptimizerCosts(ctx *ExecutionContext, query string) (bool, error) {
	if !flushOptimizerCostsStatement(query) {
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
	stmt := parsedFlushStatement(query)
	if stmt == nil || stmt.Options["tables"] != "true" {
		return false, nil
	}
	if err := e.prepareDDLImplicitCommit(ctx.Session); err != nil {
		return true, err
	}
	withReadLock := stmt.Options["with_read_lock"] == "true"
	if withReadLock && (e == nil || ctx == nil || ctx.Session == nil) {
		return true, fmt.Errorf("FLUSH TABLES WITH READ LOCK requires a session")
	}
	for _, target := range stmt.Targets {
		if target.Qualifier.IsEmpty() && ctxDatabaseName(ctx) == "" {
			return true, fmt.Errorf("invalid FLUSH TABLES target: no database selected")
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

func (e *XMySQLExecutor) parseSessionTableLockRequests(ctx *ExecutionContext, query, defaultSchema string) ([]sessionTableLockRequest, error) {
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		return nil, fmt.Errorf("invalid LOCK TABLES syntax")
	}
	statement, ok := parsed.AST.(*sqlparser.AdminStatement)
	if !ok || statement.Kind != "lock" || len(statement.Targets) == 0 || len(statement.TargetModes) != len(statement.Targets) {
		return nil, fmt.Errorf("invalid LOCK TABLES syntax")
	}
	requests := make([]sessionTableLockRequest, 0, len(statement.Targets))
	seen := make(map[string]struct{}, len(statement.Targets))
	for index, target := range statement.Targets {
		database, table := target.Qualifier.String(), target.Name.String()
		if database == "" {
			database = defaultSchema
		}
		if database == "" || table == "" {
			return nil, fmt.Errorf("invalid LOCK TABLES table %q", table)
		}
		key := strings.ToLower(strings.TrimSpace(database)) + "." + strings.ToLower(strings.TrimSpace(table))
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate table in LOCK TABLES: %s", table)
		}
		seen[key] = struct{}{}
		if err := e.checkTablePrivilege(ctx, database, table, "LOCK TABLES"); err != nil {
			return nil, err
		}
		if err := e.checkTablePrivilege(ctx, database, table, "SELECT"); err != nil {
			return nil, err
		}
		mode := "read"
		if strings.EqualFold(statement.TargetModes[index], "write") {
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

func (e *XMySQLExecutor) executeSessionKill(session server.MySQLServerSession, query, _ string) (bool, error) {
	tokenizer := sqlparser.NewStringTokenizer(query)
	_, first := tokenizer.Scan()
	if !strings.EqualFold(string(first), "kill") {
		return false, nil
	}
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		return true, fmt.Errorf("invalid KILL syntax")
	}
	statement, ok := parsed.AST.(*sqlparser.AdminStatement)
	if !ok || statement.Kind != "kill" {
		return false, nil
	}
	_, err = e.executeParsedAdmin(&ExecutionContext{Session: session}, statement, session)
	return true, err
}

func replicationControlRequiredPrivilege(query string) string {
	stmt := parsedAdminStatement(query)
	if stmt == nil {
		return ""
	}
	if stmt.Kind == "reset" && stmt.Options["replica"] != "" {
		return "RELOAD"
	}
	if stmt.Kind == "change" || ((stmt.Kind == "start" || stmt.Kind == "stop") && stmt.Options["replica"] != "") {
		return "REPLICATION_SLAVE_ADMIN"
	}
	return ""
}

func sourceBinlogRequiredPrivilege(query string) string {
	stmt := parsedAdminStatement(query)
	if stmt == nil {
		return ""
	}
	return sourceBinlogPrivilegeFromAST(stmt)
}

func sourceBinlogPrivilegeFromAST(stmt *sqlparser.AdminStatement) string {
	switch stmt.Kind {
	case "flush":
		if stmt.Options["optimizer_costs"] == "true" {
			return "FLUSH_OPTIMIZER_COSTS"
		}
		if stmt.Options["binary"] == "true" && stmt.Options["logs"] == "true" {
			return "RELOAD"
		}
	case "reset":
		if stmt.Options["master"] == "true" || stmt.Options["binary_gtids"] == "true" {
			return "RELOAD"
		}
	case "purge":
		return "BINLOG_ADMIN"
	}
	return ""
}

func parsedAdminStatement(query string) *sqlparser.AdminStatement {
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		return nil
	}
	stmt, _ := parsed.AST.(*sqlparser.AdminStatement)
	return stmt
}

func (e *XMySQLExecutor) executeReplicationControl(query, _ string) (bool, error) {
	// Select only replication commands here: RESET PERSIST and transaction START
	// are owned by other handlers. All executable parameters come from the AST.
	tokenizer := sqlparser.NewStringTokenizer(query)
	_, first := tokenizer.Scan()
	_, second := tokenizer.Scan()
	kind, target := strings.ToLower(string(first)), strings.ToLower(string(second))
	recognized := false
	switch kind {
	case "start", "stop":
		recognized = target == "replica" || target == "slave"
	case "reset":
		recognized = target == "replica" || target == "slave" || target == "master" || target == "binary"
	case "change":
		recognized = target == "replication" || target == "master"
	case "purge":
		recognized = true
	case "flush":
		recognized = target == "binary"
	}
	if !recognized {
		return false, nil
	}
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		return true, err
	}
	stmt, ok := parsed.AST.(*sqlparser.AdminStatement)
	if !ok {
		return true, fmt.Errorf("invalid replication admin statement")
	}
	if e == nil {
		return true, fmt.Errorf("replication runtime is not configured")
	}
	if stmt.Kind == "change" {
		_, err = e.executeParsedAdmin(nil, stmt, nil)
		return true, err
	}
	return e.executeParsedReplicationAdmin(stmt)
}

func parseReplicationFilter(query string) (replication.ReplicationFilterConfig, error) {
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		return replication.ReplicationFilterConfig{}, err
	}
	stmt, ok := parsed.AST.(*sqlparser.AdminStatement)
	if !ok {
		return replication.ReplicationFilterConfig{}, fmt.Errorf("invalid CHANGE REPLICATION FILTER syntax")
	}
	return typedReplicationFilterConfig(stmt)
}

func parseBinlogPurgeTime(value string) (time.Time, error) {
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05", time.RFC3339} {
		if parsed, err := time.ParseInLocation(layout, value, time.UTC); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid PURGE BINARY LOGS timestamp %q", value)
}

func replicationSourceURLFromOptions(options map[string]string) (string, error) {
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
	keys := make([]string, 0, len(options))
	for key := range options {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	seen := make(map[string]bool)
	for _, key := range keys {
		canonical := strings.TrimPrefix(strings.TrimPrefix(key, "source_"), "master_")
		if seen[canonical] {
			return "", fmt.Errorf("SOURCE_%s specified more than once", strings.ToUpper(canonical))
		}
		seen[canonical] = true
		value := options[key]
		switch key {
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
		default:
			return "", fmt.Errorf("unsupported CHANGE REPLICATION SOURCE option %q", key)
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

func isTableMaintenanceStatement(query string) bool {
	tokenizer := sqlparser.NewStringTokenizer(query)
	_, text := tokenizer.Scan()
	switch strings.ToLower(string(text)) {
	case "check", "analyze", "optimize":
		return true
	default:
		return false
	}
}

func parsedTableMaintenance(query string) (*sqlparser.AdminStatement, error) {
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		return nil, err
	}
	stmt, ok := parsed.AST.(*sqlparser.AdminStatement)
	if !ok || (stmt.Kind != "check" && stmt.Kind != "analyze" && stmt.Kind != "optimize") {
		return nil, fmt.Errorf("invalid table maintenance statement")
	}
	return stmt, nil
}

func maintenanceTargetsFromAST(names []sqlparser.TableName, defaultSchema string) ([]tableMaintenanceTarget, error) {
	targets := make([]tableMaintenanceTarget, 0, len(names))
	for _, name := range names {
		schema, table := name.Qualifier.String(), name.Name.String()
		if schema == "" {
			schema = strings.TrimSpace(defaultSchema)
		}
		if schema == "" || table == "" {
			return nil, fmt.Errorf("no database selected")
		}
		targets = append(targets, tableMaintenanceTarget{schema: schema, table: table})
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("table maintenance requires at least one table")
	}
	return targets, nil
}

func tableMaintenanceTargets(query, defaultSchema string) ([]tableMaintenanceTarget, error) {
	stmt, err := parsedTableMaintenance(query)
	if err != nil {
		return nil, err
	}
	return maintenanceTargetsFromAST(stmt.Targets, defaultSchema)
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
	stmt, err := parsedTableMaintenance(query)
	if err != nil {
		return nil, err
	}
	targets, err := maintenanceTargetsFromAST(stmt.Targets, defaultSchema)
	if err != nil {
		return nil, err
	}
	operation := strings.ToUpper(stmt.Kind)
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
		rows = append(rows, []interface{}{target.table, stmt.Kind, "status", "OK"})
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
	stmt, err := parsedTableMaintenance(query)
	if err != nil {
		return nil, isTableMaintenanceStatement(query), err
	}
	if stmt.Histogram == nil {
		return nil, false, nil
	}
	targets, err := maintenanceTargetsFromAST(stmt.Targets, defaultSchema)
	if err != nil {
		return nil, true, err
	}
	if len(targets) != 1 {
		return nil, true, fmt.Errorf("histogram maintenance requires exactly one table")
	}
	columns := make([]string, 0, len(stmt.Histogram.Columns))
	for _, column := range stmt.Histogram.Columns {
		columns = append(columns, column.String())
	}
	count := 100
	if stmt.Histogram.Buckets != "" {
		count, err = strconv.Atoi(stmt.Histogram.Buckets)
		if err != nil || count < 1 || count > 1024 {
			return nil, true, fmt.Errorf("histogram bucket count must be between 1 and 1024")
		}
	}
	return &histogramMaintenance{target: targets[0], operation: strings.ToUpper(stmt.Histogram.Operation), columns: columns, bucketCount: count}, true, nil
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
