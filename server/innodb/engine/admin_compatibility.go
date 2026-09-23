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

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func metricStatementType(query string) string {
	fields := strings.Fields(strings.TrimSpace(query))
	if len(fields) == 0 {
		return "empty"
	}
	return strings.ToLower(fields[0])
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
	if handled, err := e.executeReplicationControl(q, lower); handled {
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
	if err := e.globalReadLock.Lock(ctx); err != nil {
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
	if err := e.globalReadLock.RLock(ctx); err != nil {
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

func (e *XMySQLExecutor) executeReplicationControl(query, lower string) (bool, error) {
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
	if strings.HasPrefix(lower, "change replication source to ") || strings.HasPrefix(lower, "change master to ") {
		if e == nil || e.replicationChangeSource == nil {
			return true, fmt.Errorf("replication runtime is not configured")
		}
		sourceURL, err := parseReplicationSourceURL(query)
		if err != nil {
			return true, err
		}
		return true, e.replicationChangeSource(sourceURL)
	}
	reset := lower == "reset replica" || lower == "reset slave" || strings.HasPrefix(lower, "reset replica ") || strings.HasPrefix(lower, "reset slave ")
	if reset {
		fields := strings.Fields(lower)
		resetAll := len(fields) == 3 && fields[2] == "all"
		if len(fields) > 2 && !resetAll {
			return true, fmt.Errorf("replication reset options are not supported")
		}
		if resetAll {
			if e == nil || e.replicationResetAll == nil {
				return true, fmt.Errorf("replication runtime is not configured")
			}
			return true, e.replicationResetAll()
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
	fields := strings.Fields(lower)
	if len(fields) > 2 {
		return true, fmt.Errorf("replication thread options are not supported")
	}
	if len(fields) < 2 {
		return true, fmt.Errorf("invalid replication control statement")
	}
	if start {
		if e == nil || e.replicationStart == nil {
			return true, fmt.Errorf("replication runtime is not configured")
		}
		return true, e.replicationStart()
	}
	if e == nil || e.replicationStop == nil {
		return true, fmt.Errorf("replication runtime is not configured")
	}
	return true, e.replicationStop()
}

var replicationSourceOptionPattern = regexp.MustCompile(`(?is)^\s*(source_host|master_host|source_port|master_port)\s*=\s*(?:'((?:''|[^'])*)'|"((?:""|[^"])*)"|([^\s]+))\s*$`)

func parseReplicationSourceURL(query string) (string, error) {
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
		}
	}
	if host == "" {
		return "", fmt.Errorf("CHANGE REPLICATION SOURCE requires SOURCE_HOST")
	}
	if !strings.Contains(host, "://") {
		host = "http://" + host
	}
	parsed, err := url.ParseRequestURI(host)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", fmt.Errorf("invalid SOURCE_HOST %q", host)
	}
	if port != 0 {
		parsed.Host = net.JoinHostPort(parsed.Hostname(), strconv.Itoa(port))
	}
	return strings.TrimRight(parsed.String(), "/"), nil
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
