package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const statementHistoryLimit = 256
const errorLogLimit = 256

// StatementEvent is the execution record exposed by Performance Schema
// statement history compatibility views.
type StatementEvent struct {
	Time          time.Time
	ThreadID      int64
	Instrumented  bool
	History       bool
	User          string
	Host          string
	Schema        string
	SQL           string
	StatementType string
	Status        string
	Latency       time.Duration
	RowsAffected  int64
	RowsSent      int64
	RowsExamined  int64
	SelectScan    int64
	Warnings      int64
}

// MemorySummaryRow is the bounded memory lifecycle snapshot exposed to
// Performance Schema compatibility views. Bytes are tracked at instrument
// boundaries, rather than inferred from Go heap statistics.
type MemorySummaryRow struct {
	ThreadID         int64
	EventName        string
	CountAlloc       int64
	CountFree        int64
	BytesAlloc       int64
	BytesFree        int64
	LowCountUsed     int64
	CurrentCountUsed int64
	HighCountUsed    int64
	LowBytesUsed     int64
	CurrentBytesUsed int64
	HighBytesUsed    int64
}

// ErrorSummaryRow is the bounded error aggregate consumed by the global
// Performance Schema error summary view. Identity-specific dimensions are
// intentionally added by higher-level callers only when they have a reliable
// session identity.
type ErrorSummaryRow struct {
	ErrorName    string
	ErrorCount   int64
	WarningCount int64
	FirstSeen    time.Time
	LastSeen     time.Time
}

// ErrorIdentitySummaryRow is the same aggregate keyed by a captured client
// identity for the account/host/user/thread Performance Schema views.
type ErrorIdentitySummaryRow struct {
	ThreadID     int64
	User         string
	Host         string
	ErrorName    string
	ErrorCount   int64
	WarningCount int64
	FirstSeen    time.Time
	LastSeen     time.Time
}

// ErrorLogEvent is the bounded execution-error event stream used by
// Performance Schema error_log compatibility. It intentionally records only
// fields available at the query boundary; server-internal log metadata is not
// inferred from aggregate counters.
type ErrorLogEvent struct {
	Logged     time.Time
	Priority   int64
	ErrorCode  string
	Subsystem  string
	Database   string
	ErrorClass string
}

// FailedLoginAttempt is the bounded account-level authentication failure
// state exposed through INFORMATION_SCHEMA connection-control metadata.
type FailedLoginAttempt struct {
	UserHost       string
	FailedAttempts int64
}

// AuthenticationFailureSummary is the host-level projection used by
// Performance Schema host_cache. It is derived from the same account-level
// counters exposed by INFORMATION_SCHEMA connection-control metadata.
type AuthenticationFailureSummary struct {
	Host  string
	Count int64
}

// ConnectionSummaryRow is the durable-in-process connection count used by
// Performance Schema accounts/hosts/users views after a client disconnects.
type ConnectionSummaryRow struct {
	User             string
	Host             string
	TotalConnections int64
}

type memorySummaryKey struct {
	threadID  int64
	eventName string
}

// RuntimeRecorder converts runtime events into P0 metric updates.
//
// It is intentionally small and dependency-free so execution, transaction,
// lock, recovery, and checkpoint paths can adopt it incrementally.
type RuntimeRecorder struct {
	registry         *Registry
	statementMu      sync.RWMutex
	statementEvents  []StatementEvent
	memoryMu         sync.RWMutex
	memoryRows       map[memorySummaryKey]*MemorySummaryRow
	errorMu          sync.RWMutex
	errorRows        map[string]*ErrorSummaryRow
	errorByIdentity  map[string]*ErrorIdentitySummaryRow
	errorLog         []ErrorLogEvent
	authMu           sync.RWMutex
	failedLogins     map[string]int64
	connectionMu     sync.RWMutex
	connectionTotals map[string]int64
	fileIOMu         sync.Mutex
	fileIO           *fileSummaryRecorder
}

// NewRuntimeRecorder creates a recorder backed by registry.
func NewRuntimeRecorder(registry *Registry) *RuntimeRecorder {
	return &RuntimeRecorder{
		registry:         registry,
		statementEvents:  make([]StatementEvent, 0, statementHistoryLimit),
		memoryRows:       make(map[memorySummaryKey]*MemorySummaryRow),
		errorRows:        make(map[string]*ErrorSummaryRow),
		errorByIdentity:  make(map[string]*ErrorIdentitySummaryRow),
		errorLog:         make([]ErrorLogEvent, 0, errorLogLimit),
		failedLogins:     make(map[string]int64),
		connectionTotals: make(map[string]int64),
		fileIO:           newFileSummaryRecorder(),
	}
}

// RecordConnection increments the server-lifetime connection count for an
// authenticated account. The count intentionally survives session removal
// until the recorder is recreated, matching the runtime scope of P_S totals.
func (r *RuntimeRecorder) RecordConnection(user, host string) {
	if r == nil || user == "" {
		return
	}
	if host == "" {
		host = "localhost"
	}
	key := user + "\x00" + host
	r.connectionMu.Lock()
	if r.connectionTotals == nil {
		r.connectionTotals = make(map[string]int64)
	}
	r.connectionTotals[key]++
	r.connectionMu.Unlock()
}

// ConnectionTotals returns a stable snapshot of authenticated connection
// totals grouped by account identity.
func (r *RuntimeRecorder) ConnectionTotals() []ConnectionSummaryRow {
	if r == nil {
		return nil
	}
	r.connectionMu.RLock()
	rows := make([]ConnectionSummaryRow, 0, len(r.connectionTotals))
	for key, total := range r.connectionTotals {
		parts := strings.SplitN(key, "\x00", 2)
		if len(parts) != 2 {
			continue
		}
		rows = append(rows, ConnectionSummaryRow{User: parts[0], Host: parts[1], TotalConnections: total})
	}
	r.connectionMu.RUnlock()
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].User != rows[j].User {
			return rows[i].User < rows[j].User
		}
		return rows[i].Host < rows[j].Host
	})
	return rows
}

// RecordAuthenticationFailure increments the consecutive failed-login count
// for one user/host account. The account key uses the same SQL display shape
// as MySQL's CONNECTION_CONTROL_FAILED_LOGIN_ATTEMPTS view.
func (r *RuntimeRecorder) RecordAuthenticationFailure(user, host string) {
	if r == nil || user == "" {
		return
	}
	key := fmt.Sprintf("'%s'@'%s'", user, host)
	r.authMu.Lock()
	if r.failedLogins == nil {
		r.failedLogins = make(map[string]int64)
	}
	r.failedLogins[key]++
	r.authMu.Unlock()
}

// RecordAuthenticationSuccess resets the account's consecutive failed-login
// count, matching MySQL's connection-control semantics.
func (r *RuntimeRecorder) RecordAuthenticationSuccess(user, host string) {
	if r == nil || user == "" {
		return
	}
	key := fmt.Sprintf("'%s'@'%s'", user, host)
	r.authMu.Lock()
	delete(r.failedLogins, key)
	r.authMu.Unlock()
}

// FailedLoginAttempts returns a deterministic snapshot of active account
// counters. Accounts with a successful subsequent login are omitted.
func (r *RuntimeRecorder) FailedLoginAttempts() []FailedLoginAttempt {
	if r == nil {
		return nil
	}
	r.authMu.RLock()
	rows := make([]FailedLoginAttempt, 0, len(r.failedLogins))
	for userHost, count := range r.failedLogins {
		if count > 0 {
			rows = append(rows, FailedLoginAttempt{UserHost: userHost, FailedAttempts: count})
		}
	}
	r.authMu.RUnlock()
	sort.Slice(rows, func(i, j int) bool { return rows[i].UserHost < rows[j].UserHost })
	return rows
}

// AuthenticationFailureSummaryByHost returns active authentication failures
// grouped by host. Successful authentication removes the corresponding
// account counter, so the host projection follows the same reset semantics.
func (r *RuntimeRecorder) AuthenticationFailureSummaryByHost() []AuthenticationFailureSummary {
	if r == nil {
		return nil
	}
	r.authMu.RLock()
	counts := make(map[string]int64)
	for userHost, count := range r.failedLogins {
		if count <= 0 {
			continue
		}
		marker := strings.LastIndex(userHost, "'@'")
		if marker < 0 || marker+3 >= len(userHost) {
			continue
		}
		host := strings.Trim(userHost[marker+3:], "'")
		if host != "" {
			counts[host] += count
		}
	}
	r.authMu.RUnlock()
	rows := make([]AuthenticationFailureSummary, 0, len(counts))
	for host, count := range counts {
		rows = append(rows, AuthenticationFailureSummary{Host: host, Count: count})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Host < rows[j].Host })
	return rows
}

// RecordStatement stores a bounded, newest-first-independent statement
// history. The returned snapshots preserve execution order.
func (r *RuntimeRecorder) RecordStatement(database, sql, statementType, status string, latency time.Duration) {
	r.RecordStatementWithThreadID(0, database, sql, statementType, status, latency)
}

// RecordStatementWithThreadID stores a completed statement together with the
// connection/thread that executed it. A zero thread ID remains valid for
// internal callers that do not have a client session.
func (r *RuntimeRecorder) RecordStatementWithThreadID(threadID int64, database, sql, statementType, status string, latency time.Duration) {
	r.RecordStatementWithThreadIDAndIdentity(threadID, "", "", database, sql, statementType, status, latency)
}

// RecordStatementWithThreadIDAndIdentity stores the client identity captured
// at execution time.  Performance Schema summary tables group by account,
// host, and user; retaining the identity on the bounded event is safer than
// reconstructing it later from a potentially disconnected session.
func (r *RuntimeRecorder) RecordStatementWithThreadIDAndIdentity(threadID int64, user, host, database, sql, statementType, status string, latency time.Duration) {
	r.RecordStatementWithThreadIDAndIdentityAndAccounting(threadID, user, host, database, sql, statementType, status, latency, 0, 0, 0)
}

// RecordStatementWithThreadIDAndIdentityAndAccounting stores a completed
// statement together with the result counters that MySQL exposes through
// Performance Schema statement summaries. The legacy recording methods keep
// zero-valued counters for callers that only have timing information.
func (r *RuntimeRecorder) RecordStatementWithThreadIDAndIdentityAndAccounting(threadID int64, user, host, database, sql, statementType, status string, latency time.Duration, rowsAffected, rowsSent, warnings int64) {
	r.RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExamined(threadID, user, host, database, sql, statementType, status, latency, rowsAffected, rowsSent, 0, warnings, true, true)
}

// RecordStatementWithThreadIDAndIdentityAndAccountingWithSettings stores a
// completed statement together with the Performance Schema actor settings
// captured for the foreground thread. Instrumented=false means the event is
// not collected; History=false keeps it available to summary consumers while
// excluding it from statement history views.
func (r *RuntimeRecorder) RecordStatementWithThreadIDAndIdentityAndAccountingWithSettings(threadID int64, user, host, database, sql, statementType, status string, latency time.Duration, rowsAffected, rowsSent, warnings int64, instrumented, history bool) {
	r.RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExamined(threadID, user, host, database, sql, statementType, status, latency, rowsAffected, rowsSent, 0, warnings, instrumented, history)
}

// RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExamined stores
// result counters plus the number of rows inspected by a real execution scan.
func (r *RuntimeRecorder) RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExamined(threadID int64, user, host, database, sql, statementType, status string, latency time.Duration, rowsAffected, rowsSent, rowsExamined, warnings int64, instrumented, history bool) {
	r.RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScan(threadID, user, host, database, sql, statementType, status, latency, rowsAffected, rowsSent, rowsExamined, 0, warnings, instrumented, history)
}

// RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScan
// stores result counters plus scan-level execution counters.
func (r *RuntimeRecorder) RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScan(threadID int64, user, host, database, sql, statementType, status string, latency time.Duration, rowsAffected, rowsSent, rowsExamined, selectScan, warnings int64, instrumented, history bool) {
	if r == nil {
		return
	}
	if !instrumented {
		return
	}
	if rowsAffected < 0 {
		rowsAffected = 0
	}
	if rowsSent < 0 {
		rowsSent = 0
	}
	if rowsExamined < 0 {
		rowsExamined = 0
	}
	if selectScan < 0 {
		selectScan = 0
	}
	if warnings < 0 {
		warnings = 0
	}
	event := StatementEvent{
		Time:          time.Now(),
		ThreadID:      threadID,
		Instrumented:  instrumented,
		History:       history,
		User:          user,
		Host:          host,
		Schema:        database,
		SQL:           sql,
		StatementType: statementType,
		Status:        status,
		Latency:       latency,
		RowsAffected:  rowsAffected,
		RowsSent:      rowsSent,
		RowsExamined:  rowsExamined,
		SelectScan:    selectScan,
		Warnings:      warnings,
	}
	r.statementMu.Lock()
	defer r.statementMu.Unlock()
	if len(r.statementEvents) >= statementHistoryLimit {
		copy(r.statementEvents, r.statementEvents[1:])
		r.statementEvents = r.statementEvents[:statementHistoryLimit-1]
	}
	r.statementEvents = append(r.statementEvents, event)
}

// StatementHistory returns a stable copy of the bounded statement history.
func (r *RuntimeRecorder) StatementHistory() []StatementEvent {
	if r == nil {
		return nil
	}
	r.statementMu.RLock()
	defer r.statementMu.RUnlock()
	return append([]StatementEvent(nil), r.statementEvents...)
}

// RecordMemoryAllocation records one instrumented allocation and updates its
// current/high-water lifecycle counters.
func (r *RuntimeRecorder) RecordMemoryAllocation(threadID int64, eventName string, bytes int64) {
	if r == nil || bytes < 0 {
		return
	}
	if eventName == "" {
		eventName = "memory/sql/THD::main_mem_root"
	}
	r.memoryMu.Lock()
	defer r.memoryMu.Unlock()
	row := r.memoryRowLocked(threadID, eventName)
	row.CountAlloc++
	row.BytesAlloc += bytes
	row.CurrentCountUsed++
	row.CurrentBytesUsed += bytes
	if row.CurrentCountUsed > row.HighCountUsed {
		row.HighCountUsed = row.CurrentCountUsed
	}
	if row.CurrentBytesUsed > row.HighBytesUsed {
		row.HighBytesUsed = row.CurrentBytesUsed
	}
}

// RecordMemoryFree records one instrumented release. Counters are clamped at
// zero so an incomplete cleanup path cannot expose negative Performance
// Schema usage.
func (r *RuntimeRecorder) RecordMemoryFree(threadID int64, eventName string, bytes int64) {
	if r == nil || bytes < 0 {
		return
	}
	if eventName == "" {
		eventName = "memory/sql/THD::main_mem_root"
	}
	r.memoryMu.Lock()
	defer r.memoryMu.Unlock()
	row := r.memoryRowLocked(threadID, eventName)
	row.CountFree++
	row.BytesFree += bytes
	if row.CurrentCountUsed > 0 {
		row.CurrentCountUsed--
	}
	if bytes >= row.CurrentBytesUsed {
		row.CurrentBytesUsed = 0
	} else {
		row.CurrentBytesUsed -= bytes
	}
	if row.LowCountUsed == 0 || row.CurrentCountUsed < row.LowCountUsed {
		row.LowCountUsed = row.CurrentCountUsed
	}
	if row.LowBytesUsed == 0 || row.CurrentBytesUsed < row.LowBytesUsed {
		row.LowBytesUsed = row.CurrentBytesUsed
	}
}

func (r *RuntimeRecorder) memoryRowLocked(threadID int64, eventName string) *MemorySummaryRow {
	if r.memoryRows == nil {
		r.memoryRows = make(map[memorySummaryKey]*MemorySummaryRow)
	}
	key := memorySummaryKey{threadID: threadID, eventName: eventName}
	if row := r.memoryRows[key]; row != nil {
		return row
	}
	row := &MemorySummaryRow{ThreadID: threadID, EventName: eventName}
	r.memoryRows[key] = row
	return row
}

// MemorySummary returns a stable, deterministic copy of all instrumented
// memory rows, including rows whose current usage has returned to zero.
func (r *RuntimeRecorder) MemorySummary() []MemorySummaryRow {
	if r == nil {
		return nil
	}
	r.memoryMu.RLock()
	rows := make([]MemorySummaryRow, 0, len(r.memoryRows))
	for _, row := range r.memoryRows {
		if row != nil {
			rows = append(rows, *row)
		}
	}
	r.memoryMu.RUnlock()
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].ThreadID != rows[j].ThreadID {
			return rows[i].ThreadID < rows[j].ThreadID
		}
		return rows[i].EventName < rows[j].EventName
	})
	return rows
}

// RecordFileOpen records one physical file handle becoming active.
func (r *RuntimeRecorder) RecordFileOpen(fileName string) {
	if r == nil {
		return
	}
	r.fileSummaryRecorder().open(fileName, "")
}

// RecordFileClose records one physical file handle becoming inactive.
func (r *RuntimeRecorder) RecordFileClose(fileName string) {
	if r == nil {
		return
	}
	r.fileSummaryRecorder().close(fileName, "")
}

// RecordFileRead records a completed physical file read.
func (r *RuntimeRecorder) RecordFileRead(fileName string, latency time.Duration) {
	if r == nil {
		return
	}
	r.fileSummaryRecorder().read(fileName, "", latency)
}

// RecordFileWrite records a completed physical file write.
func (r *RuntimeRecorder) RecordFileWrite(fileName string, latency time.Duration) {
	if r == nil {
		return
	}
	r.fileSummaryRecorder().write(fileName, "", latency)
}

func (r *RuntimeRecorder) fileSummaryRecorder() *fileSummaryRecorder {
	r.fileIOMu.Lock()
	defer r.fileIOMu.Unlock()
	if r.fileIO == nil {
		r.fileIO = newFileSummaryRecorder()
	}
	return r.fileIO
}

// FileSummary returns a stable copy of physical file-I/O observations.
func (r *RuntimeRecorder) FileSummary() []FileSummaryRow {
	if r == nil {
		return nil
	}
	r.fileIOMu.Lock()
	fileIO := r.fileIO
	r.fileIOMu.Unlock()
	if fileIO == nil {
		return nil
	}
	return fileIO.snapshot()
}

// RecordQuery records one completed query.
func (r *RuntimeRecorder) RecordQuery(database, statementType, status string, latency time.Duration) {
	labels := Labels{
		"database": database,
		"status":   status,
	}
	_ = r.registry.IncCounter("xmysql_queries_total", 1, labels)
	_ = r.registry.ObserveHistogram("xmysql_query_latency_ms", float64(latency.Milliseconds()), Labels{
		"database":       database,
		"statement_type": statementType,
		"status":         status,
	})
}

// PrometheusText returns the live registry snapshot for compatibility views
// and diagnostics that need current values rather than just counters.
func (r *RuntimeRecorder) PrometheusText() string {
	if r == nil || r.registry == nil {
		return ""
	}
	return r.registry.WritePrometheusText()
}

// RecordQueryError records a query error.
func (r *RuntimeRecorder) RecordQueryError(database, errorClass, errorCode string) {
	r.recordQueryError(database, errorClass, errorCode, 0, "", "")
	if r == nil {
		return
	}
	_ = r.registry.IncCounter("xmysql_query_errors_total", 1, Labels{
		"database":    database,
		"error_class": errorClass,
		"error_code":  errorCode,
	})
}

// RecordQueryErrorWithIdentity records both global and client-identity error
// aggregates. The identity is optional; callers that lack it still retain a
// correct global row.
func (r *RuntimeRecorder) RecordQueryErrorWithIdentity(database, errorClass, errorCode string, threadID int64, user, host string) {
	r.recordQueryError(database, errorClass, errorCode, threadID, user, host)
	if r == nil {
		return
	}
	_ = r.registry.IncCounter("xmysql_query_errors_total", 1, Labels{
		"database":    database,
		"error_class": errorClass,
		"error_code":  errorCode,
	})
}

func (r *RuntimeRecorder) recordQueryError(database, errorClass, errorCode string, threadID int64, user, host string) {
	if r == nil {
		return
	}
	if errorCode == "" {
		errorCode = "UNKNOWN"
	}
	now := time.Now()
	r.errorMu.Lock()
	r.errorLog = append(r.errorLog, ErrorLogEvent{
		Logged: now, Priority: 3, ErrorCode: errorCode, Subsystem: "query",
		Database: database, ErrorClass: errorClass,
	})
	if len(r.errorLog) > errorLogLimit {
		r.errorLog = append([]ErrorLogEvent(nil), r.errorLog[len(r.errorLog)-errorLogLimit:]...)
	}
	row := r.errorRows[errorCode]
	if row == nil {
		row = &ErrorSummaryRow{ErrorName: errorCode, FirstSeen: now}
		r.errorRows[errorCode] = row
	}
	row.ErrorCount++
	row.LastSeen = now
	if threadID != 0 || user != "" || host != "" {
		key := fmt.Sprintf("%d\x00%s\x00%s\x00%s", threadID, user, host, errorCode)
		identity := r.errorByIdentity[key]
		if identity == nil {
			identity = &ErrorIdentitySummaryRow{ThreadID: threadID, User: user, Host: host, ErrorName: errorCode, FirstSeen: now}
			r.errorByIdentity[key] = identity
		}
		identity.ErrorCount++
		identity.LastSeen = now
	}
	r.errorMu.Unlock()
}

// ErrorLog returns a bounded chronological copy of query error events.
func (r *RuntimeRecorder) ErrorLog() []ErrorLogEvent {
	if r == nil {
		return nil
	}
	r.errorMu.RLock()
	rows := append([]ErrorLogEvent(nil), r.errorLog...)
	r.errorMu.RUnlock()
	return rows
}

// ErrorSummary returns a deterministic copy of global query-error aggregates.
func (r *RuntimeRecorder) ErrorSummary() []ErrorSummaryRow {
	if r == nil {
		return nil
	}
	r.errorMu.RLock()
	rows := make([]ErrorSummaryRow, 0, len(r.errorRows))
	for _, row := range r.errorRows {
		if row != nil {
			rows = append(rows, *row)
		}
	}
	r.errorMu.RUnlock()
	sort.Slice(rows, func(i, j int) bool { return rows[i].ErrorName < rows[j].ErrorName })
	return rows
}

// ErrorSummaryByIdentity returns a deterministic copy of client-identity
// error aggregates.
func (r *RuntimeRecorder) ErrorSummaryByIdentity() []ErrorIdentitySummaryRow {
	if r == nil {
		return nil
	}
	r.errorMu.RLock()
	rows := make([]ErrorIdentitySummaryRow, 0, len(r.errorByIdentity))
	for _, row := range r.errorByIdentity {
		if row != nil {
			rows = append(rows, *row)
		}
	}
	r.errorMu.RUnlock()
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].ThreadID != rows[j].ThreadID {
			return rows[i].ThreadID < rows[j].ThreadID
		}
		if rows[i].User != rows[j].User {
			return rows[i].User < rows[j].User
		}
		if rows[i].Host != rows[j].Host {
			return rows[i].Host < rows[j].Host
		}
		return rows[i].ErrorName < rows[j].ErrorName
	})
	return rows
}

// SetActiveConnections records the current active connection count.
func (r *RuntimeRecorder) SetActiveConnections(listener string, count int) {
	_ = r.registry.SetGauge("xmysql_connections_active", float64(count), Labels{
		"listener": listener,
	})
}

// SetActiveTransactions records the current active transaction count.
func (r *RuntimeRecorder) SetActiveTransactions(isolationLevel string, count int) {
	_ = r.registry.SetGauge("xmysql_transactions_active", float64(count), Labels{
		"isolation_level": isolationLevel,
	})
}

// RecordTransactionCommit records one committed transaction.
func (r *RuntimeRecorder) RecordTransactionCommit(isolationLevel string) {
	_ = r.registry.IncCounter("xmysql_transactions_committed_total", 1, Labels{
		"isolation_level": isolationLevel,
	})
}

// RecordTransactionRollback records one rolled-back transaction.
func (r *RuntimeRecorder) RecordTransactionRollback(isolationLevel, reason string) {
	_ = r.registry.IncCounter("xmysql_transactions_rolled_back_total", 1, Labels{
		"isolation_level": isolationLevel,
		"reason":          reason,
	})
}

// RecordLockWait records one lock wait and its duration.
func (r *RuntimeRecorder) RecordLockWait(lockType, resourceType string, duration time.Duration) {
	labels := Labels{
		"lock_type":     lockType,
		"resource_type": resourceType,
	}
	_ = r.registry.IncCounter("xmysql_lock_waits_total", 1, labels)
	_ = r.registry.ObserveHistogram("xmysql_lock_wait_duration_ms", float64(duration.Milliseconds()), labels)
}

// SetLongTransactions records active long transaction count.
func (r *RuntimeRecorder) SetLongTransactions(level string, count int) {
	_ = r.registry.SetGauge("xmysql_long_transactions_active", float64(count), Labels{
		"level": level,
	})
}

// RecordRecoveryRun records one recovery attempt.
func (r *RuntimeRecorder) RecordRecoveryRun(result string) {
	_ = r.registry.IncCounter("xmysql_recovery_runs_total", 1, Labels{
		"result": result,
	})
}

// RecordRecoveryFailure records one recovery failure at stage.
func (r *RuntimeRecorder) RecordRecoveryFailure(stage string) {
	_ = r.registry.IncCounter("xmysql_recovery_failures_total", 1, Labels{
		"stage": stage,
	})
}

// SetCheckpointDirtyPages records current checkpoint dirty page count.
func (r *RuntimeRecorder) SetCheckpointDirtyPages(space string, count int) {
	_ = r.registry.SetGauge("xmysql_checkpoint_dirty_pages", float64(count), Labels{
		"space": space,
	})
}

// RecordCheckpointRun records one checkpoint run.
func (r *RuntimeRecorder) RecordCheckpointRun(result string) {
	_ = r.registry.IncCounter("xmysql_checkpoint_runs_total", 1, Labels{
		"result": result,
	})
}
