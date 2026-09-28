package metrics

import (
	"fmt"
	"regexp"
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
	Time                time.Time
	ThreadID            int64
	Instrumented        bool
	History             bool
	Timed               bool
	StageTimed          bool
	User                string
	Host                string
	Schema              string
	SQL                 string
	StatementType       string
	Status              string
	Latency             time.Duration
	RowsAffected        int64
	RowsSent            int64
	RowsExamined        int64
	SelectScan          int64
	SelectRange         int64
	SelectFullJoin      int64
	SelectFullRangeJoin int64
	SelectRangeCheck    int64
	SortRows            int64
	SortScan            int64
	SortRange           int64
	NoIndexUsed         int64
	NoGoodIndexUsed     int64
	ControlledMemory    int64
	MaxControlledMemory int64
	TotalMemory         int64
	MaxTotalMemory      int64
	Warnings            int64
}

// StatementSummaryRow is an instance-lifetime aggregate for one statement
// type and execution identity. It is separate from StatementEvent because
// Performance Schema history is intentionally bounded while summary tables
// continue accumulating until the recorder is recreated or reset.
type StatementSummaryRow struct {
	ThreadID            int64
	User                string
	Host                string
	Schema              string
	StatementType       string
	SQL                 string
	Count               int64
	SumTimerWait        int64
	MinTimerWait        int64
	MaxTimerWait        int64
	Errors              int64
	SuccessCount        int64
	SuccessSumTimerWait int64
	SuccessMinTimerWait int64
	SuccessMaxTimerWait int64
	Warnings            int64
	RowsAffected        int64
	RowsSent            int64
	RowsExamined        int64
	SelectScan          int64
	SelectRange         int64
	SelectFullJoin      int64
	SelectFullRangeJoin int64
	SelectRangeCheck    int64
	SortRows            int64
	SortScan            int64
	SortRange           int64
	NoIndexUsed         int64
	NoGoodIndexUsed     int64
	MaxControlledMemory int64
	MaxTotalMemory      int64
	FirstSeen           time.Time
	LastSeen            time.Time
	SampleSeen          time.Time
	SampleTimerWait     int64
	LatencySamples      []int64
}

// TableIOCounters is one operation-class aggregate used by the Performance
// Schema table I/O summary views.
type TableIOCounters struct {
	Count        int64
	SumTimerWait int64
	MinTimerWait int64
	MaxTimerWait int64
}

// TableIOSummaryRow is an independent table I/O aggregate. It is deliberately
// separate from StatementSummaryRow so TRUNCATE of table I/O summaries does
// not change statement or digest summaries.
type TableIOSummaryRow struct {
	ObjectType    string
	ObjectSchema  string
	ObjectName    string
	IndexName     string
	StatementType string
	All           TableIOCounters
	Read          TableIOCounters
	Write         TableIOCounters
	Fetch         TableIOCounters
	Insert        TableIOCounters
	Update        TableIOCounters
	Delete        TableIOCounters
}

type tableIOStatementKey struct {
	objectSchema  string
	objectName    string
	statementType string
}

type statementSummaryKey struct {
	threadID      int64
	user          string
	host          string
	schema        string
	statementType string
	sql           string
}

type statementDigestKey struct {
	schema     string
	digestText string
}

var tableIOReferencePattern = regexp.MustCompile(`(?i)\b(?:from|join|into|update)\s+([[:alnum:]_$` + "`" + `.-]+)`)

type statementStageKey struct {
	threadID int64
	user     string
	host     string
}

// MemorySummaryRow is the bounded memory lifecycle snapshot exposed to
// Performance Schema compatibility views. Bytes are tracked at instrument
// boundaries, rather than inferred from Go heap statistics.
type MemorySummaryRow struct {
	ThreadID         int64
	User             string
	Host             string
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
	HandledCount int64
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
	HandledCount int64
	FirstSeen    time.Time
	LastSeen     time.Time
}

// ErrorLogEvent is the bounded execution-error event stream used by
// Performance Schema error_log compatibility. It intentionally records only
// fields available at the query boundary; server-internal log metadata is not
// inferred from aggregate counters.
type ErrorLogEvent struct {
	Logged     time.Time
	ThreadID   int64
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
	Host      string
	Count     int64
	FirstSeen time.Time
	LastSeen  time.Time
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
	registry                         *Registry
	instrumentMu                     sync.RWMutex
	instrumentSettings               map[string]runtimeInstrumentSetting
	statementMu                      sync.RWMutex
	statementEvents                  []StatementEvent
	statementByThread                map[int64][]StatementEvent
	stageEvents                      []StatementEvent
	stageByThread                    map[int64][]StatementEvent
	statementSummary                 map[statementSummaryKey]*StatementSummaryRow
	statementSummaryByDimension      map[string]map[string]*StatementSummaryRow
	statementDigestSummary           map[statementDigestKey]*StatementSummaryRow
	statementStageSummary            map[statementStageKey]*StatementSummaryRow
	statementStageSummaryByDimension map[string]map[string]*StatementSummaryRow
	tableIOSummary                   map[tableIOStatementKey]*TableIOSummaryRow
	tableIOIndexSummary              map[tableIOStatementKey]*TableIOSummaryRow
	statementHistogramSamples        []int64
	memoryMu                         sync.RWMutex
	memoryRows                       map[memorySummaryKey]*MemorySummaryRow
	memorySummaryByDimension         map[string]map[string]*MemorySummaryRow
	statementMemoryHighBaseline      map[int64]int64
	errorMu                          sync.RWMutex
	errorRows                        map[string]*ErrorSummaryRow
	errorByIdentity                  map[string]*ErrorIdentitySummaryRow
	errorByAccount                   map[string]*ErrorIdentitySummaryRow
	errorByHost                      map[string]*ErrorIdentitySummaryRow
	errorByUser                      map[string]*ErrorIdentitySummaryRow
	errorByThread                    map[string]*ErrorIdentitySummaryRow
	errorLog                         []ErrorLogEvent
	authMu                           sync.RWMutex
	failedLogins                     map[string]int64
	failedLoginFirstSeen             map[string]time.Time
	failedLoginLastSeen              map[string]time.Time
	authFailuresTotal                int64
	connectionMu                     sync.RWMutex
	connectionTotals                 map[string]int64
	transactionMu                    sync.RWMutex
	transactionCommit                int64
	transactionRWCommit              int64
	transactionROCommit              int64
	transactionNLROCommit            int64
	transactionDMLCommit             int64
	transactionRollbackSavepoint     int64
	transactionRollback              int64
	tableHandleMu                    sync.RWMutex
	tableHandlesOpened               int64
	tableHandlesClosed               int64
	tableHandleRefs                  int64
	lockTimeoutMu                    sync.RWMutex
	lockTimeouts                     int64
	queryMu                          sync.RWMutex
	queryTotals                      map[string]int64
	fileIOMu                         sync.Mutex
	fileIO                           *fileSummaryRecorder
	socketMu                         sync.RWMutex
	socketRows                       map[int64]*SocketSummaryRow
	socketSummaryReset               bool
}

type runtimeInstrumentSetting struct {
	enabled bool
	timed   bool
}

// NewRuntimeRecorder creates a recorder backed by registry.
func NewRuntimeRecorder(registry *Registry) *RuntimeRecorder {
	return &RuntimeRecorder{
		registry:                         registry,
		instrumentSettings:               make(map[string]runtimeInstrumentSetting),
		statementEvents:                  make([]StatementEvent, 0, statementHistoryLimit),
		statementByThread:                make(map[int64][]StatementEvent),
		stageEvents:                      make([]StatementEvent, 0, statementHistoryLimit),
		stageByThread:                    make(map[int64][]StatementEvent),
		statementSummary:                 make(map[statementSummaryKey]*StatementSummaryRow),
		statementSummaryByDimension:      make(map[string]map[string]*StatementSummaryRow),
		statementDigestSummary:           make(map[statementDigestKey]*StatementSummaryRow),
		statementStageSummary:            make(map[statementStageKey]*StatementSummaryRow),
		statementStageSummaryByDimension: make(map[string]map[string]*StatementSummaryRow),
		tableIOSummary:                   make(map[tableIOStatementKey]*TableIOSummaryRow),
		tableIOIndexSummary:              make(map[tableIOStatementKey]*TableIOSummaryRow),
		statementHistogramSamples:        make([]int64, 0),
		memoryRows:                       make(map[memorySummaryKey]*MemorySummaryRow),
		memorySummaryByDimension:         make(map[string]map[string]*MemorySummaryRow),
		statementMemoryHighBaseline:      make(map[int64]int64),
		errorRows:                        make(map[string]*ErrorSummaryRow),
		errorByIdentity:                  make(map[string]*ErrorIdentitySummaryRow),
		errorByAccount:                   make(map[string]*ErrorIdentitySummaryRow),
		errorByHost:                      make(map[string]*ErrorIdentitySummaryRow),
		errorByUser:                      make(map[string]*ErrorIdentitySummaryRow),
		errorByThread:                    make(map[string]*ErrorIdentitySummaryRow),
		errorLog:                         make([]ErrorLogEvent, 0, errorLogLimit),
		failedLogins:                     make(map[string]int64),
		failedLoginFirstSeen:             make(map[string]time.Time),
		failedLoginLastSeen:              make(map[string]time.Time),
		connectionTotals:                 make(map[string]int64),
		queryTotals:                      make(map[string]int64),
		fileIO:                           newFileSummaryRecorder(),
		socketRows:                       make(map[int64]*SocketSummaryRow),
	}
}

// SetInstrument updates the runtime gate for a Performance Schema instrument.
// The SQL executor owns the setup_instruments rows; the recorder only needs a
// small process-local copy so low-level file and socket producers can honor
// ENABLED/TIMED without depending on the SQL package.
func (r *RuntimeRecorder) SetInstrument(name string, enabled, timed bool) {
	if r == nil || strings.TrimSpace(name) == "" {
		return
	}
	r.instrumentMu.Lock()
	if r.instrumentSettings == nil {
		r.instrumentSettings = make(map[string]runtimeInstrumentSetting)
	}
	r.instrumentSettings[strings.ToLower(strings.TrimSpace(name))] = runtimeInstrumentSetting{enabled: enabled, timed: timed}
	r.instrumentMu.Unlock()
}

func (r *RuntimeRecorder) instrumentSetting(name string) (bool, bool) {
	if r == nil {
		return true, true
	}
	r.instrumentMu.RLock()
	setting, ok := r.instrumentSettings[strings.ToLower(strings.TrimSpace(name))]
	r.instrumentMu.RUnlock()
	if !ok {
		return true, true
	}
	return setting.enabled, setting.timed
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

// ResetConnectionTotals applies the Performance Schema connection-table
// TRUNCATE rule: retain only currently connected accounts and reset their
// TOTAL_CONNECTIONS to the current count. The caller supplies a snapshot of
// live authenticated sessions so the recorder does not need to own session
// lifecycle state.
func (r *RuntimeRecorder) ResetConnectionTotals(current []ConnectionSummaryRow) {
	if r == nil {
		return
	}
	reset := make(map[string]int64, len(current))
	for _, row := range current {
		if strings.TrimSpace(row.User) == "" || row.TotalConnections <= 0 {
			continue
		}
		host := row.Host
		if strings.TrimSpace(host) == "" {
			host = "localhost"
		}
		key := row.User + "\x00" + host
		reset[key] += row.TotalConnections
	}
	r.connectionMu.Lock()
	r.connectionTotals = reset
	r.connectionMu.Unlock()
}

// RecordAuthenticationFailure increments the consecutive failed-login count
// for one user/host account. The account key uses the same SQL display shape
// as MySQL's CONNECTION_CONTROL_FAILED_LOGIN_ATTEMPTS view.
func (r *RuntimeRecorder) RecordAuthenticationFailure(user, host string) {
	if r == nil || user == "" {
		return
	}
	key := fmt.Sprintf("'%s'@'%s'", user, host)
	now := time.Now().UTC()
	r.authMu.Lock()
	if r.failedLogins == nil {
		r.failedLogins = make(map[string]int64)
	}
	if r.failedLoginFirstSeen == nil {
		r.failedLoginFirstSeen = make(map[string]time.Time)
	}
	if r.failedLoginLastSeen == nil {
		r.failedLoginLastSeen = make(map[string]time.Time)
	}
	if r.failedLoginFirstSeen[key].IsZero() {
		r.failedLoginFirstSeen[key] = now
	}
	r.failedLoginLastSeen[key] = now
	r.failedLogins[key]++
	r.authFailuresTotal++
	r.authMu.Unlock()
}

// AuthenticationFailuresTotal returns the server-lifetime count of failed
// authentication attempts. It intentionally does not reset when a later
// successful login clears the active connection-control counter.
func (r *RuntimeRecorder) AuthenticationFailuresTotal() int64 {
	if r == nil {
		return 0
	}
	r.authMu.RLock()
	total := r.authFailuresTotal
	r.authMu.RUnlock()
	return total
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
	type hostFailureState struct {
		count     int64
		firstSeen time.Time
		lastSeen  time.Time
	}
	counts := make(map[string]hostFailureState)
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
			state := counts[host]
			state.count += count
			if firstSeen := r.failedLoginFirstSeen[userHost]; !firstSeen.IsZero() && (state.firstSeen.IsZero() || firstSeen.Before(state.firstSeen)) {
				state.firstSeen = firstSeen
			}
			if lastSeen := r.failedLoginLastSeen[userHost]; !lastSeen.IsZero() && (state.lastSeen.IsZero() || lastSeen.After(state.lastSeen)) {
				state.lastSeen = lastSeen
			}
			counts[host] = state
		}
	}
	r.authMu.RUnlock()
	rows := make([]AuthenticationFailureSummary, 0, len(counts))
	for host, state := range counts {
		rows = append(rows, AuthenticationFailureSummary{Host: host, Count: state.count, FirstSeen: state.firstSeen, LastSeen: state.lastSeen})
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
	r.RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScanAndIndexUsage(threadID, user, host, database, sql, statementType, status, latency, rowsAffected, rowsSent, rowsExamined, selectScan, 0, 0, warnings, instrumented, history)
}

// RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScanAndIndexUsage
// stores scan counters plus the authoritative no-index flags reported by the
// execution path. The older method remains source-compatible for callers that
// do not have access-path information.
func (r *RuntimeRecorder) RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScanAndIndexUsage(threadID int64, user, host, database, sql, statementType, status string, latency time.Duration, rowsAffected, rowsSent, rowsExamined, selectScan, noIndexUsed, noGoodIndexUsed, warnings int64, instrumented, history bool) {
	r.RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScanAndIndexUsageAndSortRows(threadID, user, host, database, sql, statementType, status, latency, rowsAffected, rowsSent, rowsExamined, selectScan, noIndexUsed, noGoodIndexUsed, 0, warnings, instrumented, history)
}

// RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScanAndIndexUsageAndSortRows
// stores the access-path counters produced by the real SELECT executor.
func (r *RuntimeRecorder) RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScanAndIndexUsageAndSortRows(threadID int64, user, host, database, sql, statementType, status string, latency time.Duration, rowsAffected, rowsSent, rowsExamined, selectScan, noIndexUsed, noGoodIndexUsed, sortRows, warnings int64, instrumented, history bool) {
	r.RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScanAndIndexUsageAndSortRowsAndScan(threadID, user, host, database, sql, statementType, status, latency, rowsAffected, rowsSent, rowsExamined, selectScan, noIndexUsed, noGoodIndexUsed, sortRows, 0, warnings, instrumented, history)
}

// RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScanAndIndexUsageAndSortRowsAndScan
// stores sort rows and the table-scan sort counter from the real executor.
func (r *RuntimeRecorder) RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScanAndIndexUsageAndSortRowsAndScan(threadID int64, user, host, database, sql, statementType, status string, latency time.Duration, rowsAffected, rowsSent, rowsExamined, selectScan, noIndexUsed, noGoodIndexUsed, sortRows, sortScan, warnings int64, instrumented, history bool) {
	r.RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScanAndIndexUsageAndSortRowsAndScanAndRange(threadID, user, host, database, sql, statementType, status, latency, rowsAffected, rowsSent, rowsExamined, selectScan, 0, noIndexUsed, noGoodIndexUsed, sortRows, sortScan, 0, warnings, instrumented, history)
}

// RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScanAndIndexUsageAndSortRowsAndScanAndRange
// stores the range counters produced by the real SELECT executor. The older
// method remains source-compatible for callers that do not have range
// classification available.
func (r *RuntimeRecorder) RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScanAndIndexUsageAndSortRowsAndScanAndRange(threadID int64, user, host, database, sql, statementType, status string, latency time.Duration, rowsAffected, rowsSent, rowsExamined, selectScan, selectRange, noIndexUsed, noGoodIndexUsed, sortRows, sortScan, sortRange, warnings int64, instrumented, history bool) {
	r.RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScanAndIndexUsageAndSortRowsAndScanAndRangeAndJoin(threadID, user, host, database, sql, statementType, status, latency, rowsAffected, rowsSent, rowsExamined, selectScan, selectRange, 0, 0, 0, noIndexUsed, noGoodIndexUsed, sortRows, sortScan, sortRange, warnings, instrumented, history)
}

// RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScanAndIndexUsageAndSortRowsAndScanAndRangeAndJoin
// stores join-access counters in addition to the scan and range counters
// produced by the real SELECT executor.
func statementTimerValue(latency time.Duration, timed bool) int64 {
	if !timed {
		return 0
	}
	timer := latency.Nanoseconds() * 1000
	if timer <= 0 {
		return 1
	}
	return timer
}

func (r *RuntimeRecorder) RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScanAndIndexUsageAndSortRowsAndScanAndRangeAndJoin(threadID int64, user, host, database, sql, statementType, status string, latency time.Duration, rowsAffected, rowsSent, rowsExamined, selectScan, selectRange, selectFullJoin, selectFullRangeJoin, selectRangeCheck, noIndexUsed, noGoodIndexUsed, sortRows, sortScan, sortRange, warnings int64, instrumented, history bool) {
	r.RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScanAndIndexUsageAndSortRowsAndScanAndRangeAndJoinWithTimer(
		threadID, user, host, database, sql, statementType, status, latency, rowsAffected, rowsSent, rowsExamined, selectScan, selectRange, selectFullJoin, selectFullRangeJoin, selectRangeCheck, noIndexUsed, noGoodIndexUsed, sortRows, sortScan, sortRange, warnings, instrumented, history, true, true, true,
	)
}

// RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScanAndIndexUsageAndSortRowsAndScanAndRangeAndJoinWithTimer
// records the statement while allowing the caller to disable timer collection
// without dropping the event from statement summaries. stageInstrumented
// independently controls creation of stage summary/history rows.
func (r *RuntimeRecorder) RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScanAndIndexUsageAndSortRowsAndScanAndRangeAndJoinWithTimer(threadID int64, user, host, database, sql, statementType, status string, latency time.Duration, rowsAffected, rowsSent, rowsExamined, selectScan, selectRange, selectFullJoin, selectFullRangeJoin, selectRangeCheck, noIndexUsed, noGoodIndexUsed, sortRows, sortScan, sortRange, warnings int64, instrumented, history, timed, stageInstrumented, stageTimed bool) {
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
	if selectRange < 0 {
		selectRange = 0
	}
	if selectFullJoin < 0 {
		selectFullJoin = 0
	}
	if selectFullRangeJoin < 0 {
		selectFullRangeJoin = 0
	}
	if selectRangeCheck < 0 {
		selectRangeCheck = 0
	}
	if noIndexUsed < 0 {
		noIndexUsed = 0
	}
	if noGoodIndexUsed < 0 {
		noGoodIndexUsed = 0
	}
	if sortRows < 0 {
		sortRows = 0
	}
	if sortScan < 0 {
		sortScan = 0
	}
	if sortRange < 0 {
		sortRange = 0
	}
	if warnings < 0 {
		warnings = 0
	}
	currentMemory, maxMemory := r.statementMemorySnapshot(threadID)
	event := StatementEvent{
		Time:                time.Now(),
		ThreadID:            threadID,
		Instrumented:        instrumented,
		History:             history,
		Timed:               timed,
		StageTimed:          stageTimed,
		User:                user,
		Host:                host,
		Schema:              database,
		SQL:                 sql,
		StatementType:       statementType,
		Status:              status,
		Latency:             latency,
		RowsAffected:        rowsAffected,
		RowsSent:            rowsSent,
		RowsExamined:        rowsExamined,
		SelectScan:          selectScan,
		SelectRange:         selectRange,
		SelectFullJoin:      selectFullJoin,
		SelectFullRangeJoin: selectFullRangeJoin,
		SelectRangeCheck:    selectRangeCheck,
		SortRows:            sortRows,
		SortScan:            sortScan,
		SortRange:           sortRange,
		NoIndexUsed:         noIndexUsed,
		NoGoodIndexUsed:     noGoodIndexUsed,
		ControlledMemory:    currentMemory,
		MaxControlledMemory: maxMemory,
		TotalMemory:         currentMemory,
		MaxTotalMemory:      maxMemory,
		Warnings:            warnings,
	}
	r.statementMu.Lock()
	defer r.statementMu.Unlock()
	key := statementSummaryKey{
		threadID: threadID, user: user, host: host, schema: database, statementType: statementType, sql: sql,
	}
	summary := r.statementSummary[key]
	if summary == nil {
		summary = &StatementSummaryRow{
			ThreadID: threadID, User: user, Host: host, Schema: database, StatementType: statementType, SQL: sql,
		}
		r.statementSummary[key] = summary
	}
	timer := statementTimerValue(latency, timed)
	stageTimer := statementTimerValue(latency, stageTimed)
	if timed && timer <= 0 {
		// Performance Schema reports a positive timer for a completed
		// instrumented event even when the host clock rounds a very short
		// execution down to zero.
		timer = 1
	}
	accumulateStatementSummary(summary, event, timer)
	r.recordStatementSummaryDimensionsLocked(event, timer)
	digestKey := statementDigestKey{schema: database, digestText: NormalizeStatementDigest(sql)}
	digestSummary := r.statementDigestSummary[digestKey]
	if digestSummary == nil {
		digestSummary = &StatementSummaryRow{Schema: database, SQL: sql}
		r.statementDigestSummary[digestKey] = digestSummary
	}
	accumulateStatementSummary(digestSummary, event, timer)
	if stageInstrumented {
		stageKey := statementStageKey{threadID: threadID, user: user, host: host}
		stageSummary := r.statementStageSummary[stageKey]
		if stageSummary == nil {
			stageSummary = &StatementSummaryRow{ThreadID: threadID, User: user, Host: host, StatementType: "stage/sql/execute"}
			r.statementStageSummary[stageKey] = stageSummary
		}
		accumulateStatementSummary(stageSummary, event, stageTimer)
		r.recordStatementStageSummaryDimensionsLocked(event, stageTimer)
	}
	r.recordTableIOSummariesLocked(event, timer)
	r.statementHistogramSamples = append(r.statementHistogramSamples, timer)
	if len(r.statementEvents) >= statementHistoryLimit {
		copy(r.statementEvents, r.statementEvents[1:])
		r.statementEvents = r.statementEvents[:statementHistoryLimit-1]
	}
	r.statementEvents = append(r.statementEvents, event)
	if r.statementByThread == nil {
		r.statementByThread = make(map[int64][]StatementEvent)
	}
	threadEvents := r.statementByThread[threadID]
	if len(threadEvents) >= statementHistoryLimit {
		copy(threadEvents, threadEvents[1:])
		threadEvents = threadEvents[:statementHistoryLimit-1]
	}
	r.statementByThread[threadID] = append(threadEvents, event)
	if stageInstrumented {
		if len(r.stageEvents) >= statementHistoryLimit {
			copy(r.stageEvents, r.stageEvents[1:])
			r.stageEvents = r.stageEvents[:statementHistoryLimit-1]
		}
		r.stageEvents = append(r.stageEvents, event)
		if r.stageByThread == nil {
			r.stageByThread = make(map[int64][]StatementEvent)
		}
		stageThreadEvents := r.stageByThread[threadID]
		if len(stageThreadEvents) >= statementHistoryLimit {
			copy(stageThreadEvents, stageThreadEvents[1:])
			stageThreadEvents = stageThreadEvents[:statementHistoryLimit-1]
		}
		r.stageByThread[threadID] = append(stageThreadEvents, event)
	}
}

// BeginStatementMemory marks the current thread memory high-water mark before
// the executor adds the statement allocation. This prevents a previous
// statement's lifetime high-water mark from leaking into the next event.
func (r *RuntimeRecorder) BeginStatementMemory(threadID int64) {
	if r == nil {
		return
	}
	r.memoryMu.Lock()
	if r.statementMemoryHighBaseline == nil {
		r.statementMemoryHighBaseline = make(map[int64]int64)
	}
	row := r.memoryRows[memorySummaryKey{threadID: threadID, eventName: "memory/sql/THD::main_mem_root"}]
	baseline := int64(0)
	if row != nil {
		baseline = row.HighBytesUsed
	}
	r.statementMemoryHighBaseline[threadID] = baseline
	r.memoryMu.Unlock()
}

// EndStatementMemory drops the per-thread statement-memory baseline after the
// event has been recorded. It does not alter the public memory summary.
func (r *RuntimeRecorder) EndStatementMemory(threadID int64) {
	if r == nil {
		return
	}
	r.memoryMu.Lock()
	delete(r.statementMemoryHighBaseline, threadID)
	r.memoryMu.Unlock()
}

// statementMemorySnapshot captures the authoritative statement-scoped memory
// instrument while the statement's allocation is still live. The executor
// records a baseline before allocation and releases the allocation afterwards,
// so the values describe this statement rather than an idle connection.
func (r *RuntimeRecorder) statementMemorySnapshot(threadID int64) (current, high int64) {
	if r == nil {
		return 0, 0
	}
	r.memoryMu.RLock()
	row := r.memoryRows[memorySummaryKey{threadID: threadID, eventName: "memory/sql/THD::main_mem_root"}]
	if row != nil {
		current = row.CurrentBytesUsed
		high = row.HighBytesUsed
		if baseline, ok := r.statementMemoryHighBaseline[threadID]; ok {
			high -= baseline
			if high < 0 {
				high = 0
			}
		} else {
			// Direct recorder callers have no statement lifecycle hook; the
			// live current value is the only non-stale statement observation.
			high = current
		}
		if high < current {
			high = current
		}
	}
	r.memoryMu.RUnlock()
	return current, high
}

func (r *RuntimeRecorder) recordStatementStageSummaryDimensionsLocked(event StatementEvent, timer int64) {
	if r.statementStageSummaryByDimension == nil {
		r.statementStageSummaryByDimension = make(map[string]map[string]*StatementSummaryRow)
	}
	for _, dimension := range []string{"thread", "account", "host", "user"} {
		rows := r.statementStageSummaryByDimension[dimension]
		if rows == nil {
			rows = make(map[string]*StatementSummaryRow)
			r.statementStageSummaryByDimension[dimension] = rows
		}
		var key string
		summary := &StatementSummaryRow{StatementType: "stage/sql/execute"}
		switch dimension {
		case "thread":
			key = fmt.Sprint(event.ThreadID)
			summary.ThreadID = event.ThreadID
		case "account":
			key = event.User + "\x00" + event.Host
			summary.User, summary.Host = event.User, event.Host
		case "host":
			key = event.Host
			summary.Host = event.Host
		case "user":
			key = event.User
			summary.User = event.User
		}
		if existing := rows[key]; existing != nil {
			summary = existing
		} else {
			rows[key] = summary
		}
		accumulateStatementSummary(summary, event, timer)
	}
}

func accumulateStatementSummary(summary *StatementSummaryRow, event StatementEvent, timer int64) {
	if summary == nil {
		return
	}
	summary.Count++
	summary.SumTimerWait += timer
	if summary.Count == 1 {
		summary.FirstSeen = event.Time
		summary.SampleSeen = event.Time
		summary.SampleTimerWait = timer
	}
	if summary.FirstSeen.IsZero() || event.Time.Before(summary.FirstSeen) {
		summary.FirstSeen = event.Time
	}
	if summary.LastSeen.IsZero() || event.Time.After(summary.LastSeen) {
		summary.LastSeen = event.Time
	}
	if summary.Count == 1 || timer < summary.MinTimerWait {
		summary.MinTimerWait = timer
	}
	if timer > summary.MaxTimerWait {
		summary.MaxTimerWait = timer
	}
	if strings.EqualFold(event.Status, "error") {
		summary.Errors++
	} else {
		summary.SuccessCount++
		summary.SuccessSumTimerWait += timer
		if summary.SuccessCount == 1 || timer < summary.SuccessMinTimerWait {
			summary.SuccessMinTimerWait = timer
		}
		if timer > summary.SuccessMaxTimerWait {
			summary.SuccessMaxTimerWait = timer
		}
	}
	summary.Warnings += event.Warnings
	summary.RowsAffected += event.RowsAffected
	summary.RowsSent += event.RowsSent
	summary.RowsExamined += event.RowsExamined
	summary.SelectScan += event.SelectScan
	summary.SelectRange += event.SelectRange
	summary.SelectFullJoin += event.SelectFullJoin
	summary.SelectFullRangeJoin += event.SelectFullRangeJoin
	summary.SelectRangeCheck += event.SelectRangeCheck
	summary.SortRows += event.SortRows
	summary.SortScan += event.SortScan
	summary.SortRange += event.SortRange
	summary.NoIndexUsed += event.NoIndexUsed
	summary.NoGoodIndexUsed += event.NoGoodIndexUsed
	if event.MaxControlledMemory > summary.MaxControlledMemory {
		summary.MaxControlledMemory = event.MaxControlledMemory
	}
	if event.MaxTotalMemory > summary.MaxTotalMemory {
		summary.MaxTotalMemory = event.MaxTotalMemory
	}
	summary.LatencySamples = append(summary.LatencySamples, timer)
}

func (c *TableIOCounters) add(timer int64) {
	if c.Count == 0 || timer < c.MinTimerWait {
		c.MinTimerWait = timer
	}
	if c.Count == 0 || timer > c.MaxTimerWait {
		c.MaxTimerWait = timer
	}
	c.Count++
	c.SumTimerWait += timer
}

func tableIOReferences(sql, defaultSchema string) [][2]string {
	seen := make(map[string]struct{})
	refs := make([][2]string, 0)
	for _, match := range tableIOReferencePattern.FindAllStringSubmatch(sql, -1) {
		if len(match) < 2 {
			continue
		}
		raw := strings.Trim(strings.TrimSpace(match[1]), "`")
		if raw == "" || strings.HasPrefix(raw, "(") {
			continue
		}
		schema, table := defaultSchema, raw
		if parts := strings.SplitN(raw, ".", 2); len(parts) == 2 {
			schema, table = strings.Trim(parts[0], "`"), strings.Trim(parts[1], "`")
		}
		if schema == "" {
			schema = defaultSchema
		}
		if table == "" || strings.EqualFold(schema, "performance_schema") || strings.EqualFold(schema, "information_schema") {
			continue
		}
		key := strings.ToLower(schema + "\x00" + table)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		refs = append(refs, [2]string{schema, table})
	}
	return refs
}

func (r *RuntimeRecorder) recordTableIOSummariesLocked(event StatementEvent, timer int64) {
	if r == nil || strings.EqualFold(event.Status, "error") {
		return
	}
	refs := tableIOReferences(event.SQL, event.Schema)
	if len(refs) == 0 {
		return
	}
	statementType := strings.ToUpper(strings.TrimSpace(event.StatementType))
	if statementType == "" {
		statementType = "SELECT"
	}
	for _, ref := range refs {
		key := tableIOStatementKey{objectSchema: ref[0], objectName: ref[1], statementType: statementType}
		for _, summaries := range []map[tableIOStatementKey]*TableIOSummaryRow{r.tableIOSummary, r.tableIOIndexSummary} {
			row := summaries[key]
			if row == nil {
				row = &TableIOSummaryRow{
					ObjectType: "TABLE", ObjectSchema: ref[0], ObjectName: ref[1],
					StatementType: statementType,
				}
				summaries[key] = row
			}
			row.All.add(timer)
			switch statementType {
			case "SELECT", "SHOW", "EXPLAIN":
				row.Read.add(timer)
				row.Fetch.add(timer)
			case "INSERT", "REPLACE":
				row.Write.add(timer)
				row.Insert.add(timer)
			case "UPDATE":
				row.Read.add(timer)
				row.Write.add(timer)
				row.Update.add(timer)
			case "DELETE":
				row.Read.add(timer)
				row.Write.add(timer)
				row.Delete.add(timer)
			default:
				row.Read.add(timer)
			}
		}
	}
}

func copyTableIOSummaries(source map[tableIOStatementKey]*TableIOSummaryRow) []TableIOSummaryRow {
	rows := make([]TableIOSummaryRow, 0, len(source))
	for _, row := range source {
		if row != nil {
			rows = append(rows, *row)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].ObjectSchema != rows[j].ObjectSchema {
			return rows[i].ObjectSchema < rows[j].ObjectSchema
		}
		if rows[i].ObjectName != rows[j].ObjectName {
			return rows[i].ObjectName < rows[j].ObjectName
		}
		return rows[i].StatementType < rows[j].StatementType
	})
	return rows
}

// TableIOSummary returns the independent table-level table I/O aggregates.
func (r *RuntimeRecorder) TableIOSummary() []TableIOSummaryRow {
	if r == nil {
		return nil
	}
	r.statementMu.RLock()
	defer r.statementMu.RUnlock()
	return copyTableIOSummaries(r.tableIOSummary)
}

// TableIOIndexSummary returns the independent index-usage projection. The
// current executor has no physical index identity, so INDEX_NAME is empty.
func (r *RuntimeRecorder) TableIOIndexSummary() []TableIOSummaryRow {
	if r == nil {
		return nil
	}
	r.statementMu.RLock()
	defer r.statementMu.RUnlock()
	return copyTableIOSummaries(r.tableIOIndexSummary)
}

// ResetTableIOSummary implements TRUNCATE of the table-level summary and its
// implicitly dependent index-usage summary.
func (r *RuntimeRecorder) ResetTableIOSummary() {
	if r == nil {
		return
	}
	r.statementMu.Lock()
	for _, summaries := range []map[tableIOStatementKey]*TableIOSummaryRow{r.tableIOSummary, r.tableIOIndexSummary} {
		for key, row := range summaries {
			if row == nil {
				continue
			}
			summaries[key] = &TableIOSummaryRow{
				ObjectType: row.ObjectType, ObjectSchema: row.ObjectSchema,
				ObjectName: row.ObjectName, IndexName: row.IndexName,
				StatementType: row.StatementType,
			}
		}
	}
	r.statementMu.Unlock()
}

// ResetTableIOIndexSummary implements TRUNCATE of the index-usage summary
// without changing the table-level summary.
func (r *RuntimeRecorder) ResetTableIOIndexSummary() {
	if r == nil {
		return
	}
	r.statementMu.Lock()
	for key, row := range r.tableIOIndexSummary {
		if row == nil {
			continue
		}
		r.tableIOIndexSummary[key] = &TableIOSummaryRow{
			ObjectType: row.ObjectType, ObjectSchema: row.ObjectSchema,
			ObjectName: row.ObjectName, IndexName: row.IndexName,
			StatementType: row.StatementType,
		}
	}
	r.statementMu.Unlock()
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

// StatementHistoryForThread returns the bounded statement history for one
// thread. Performance Schema's events_statements_history is per-thread,
// while events_statements_history_long is the instance-wide history exposed
// by StatementHistory.
func (r *RuntimeRecorder) StatementHistoryForThread(threadID int64) []StatementEvent {
	if r == nil {
		return nil
	}
	r.statementMu.RLock()
	defer r.statementMu.RUnlock()
	return append([]StatementEvent(nil), r.statementByThread[threadID]...)
}

// StageHistory returns the independent bounded stage-event history used by
// Performance Schema stage history tables.
func (r *RuntimeRecorder) StageHistory() []StatementEvent {
	if r == nil {
		return nil
	}
	r.statementMu.RLock()
	defer r.statementMu.RUnlock()
	return append([]StatementEvent(nil), r.stageEvents...)
}

// StageHistoryForThread returns the stage history for one thread.
func (r *RuntimeRecorder) StageHistoryForThread(threadID int64) []StatementEvent {
	if r == nil {
		return nil
	}
	r.statementMu.RLock()
	defer r.statementMu.RUnlock()
	return append([]StatementEvent(nil), r.stageByThread[threadID]...)
}

// ResetStatementHistory implements TRUNCATE for the per-thread statement
// history without changing long history, stage history, or summaries.
func (r *RuntimeRecorder) ResetStatementHistory() {
	if r == nil {
		return
	}
	r.statementMu.Lock()
	r.statementByThread = make(map[int64][]StatementEvent)
	r.statementMu.Unlock()
}

// ResetStatementHistoryLong implements TRUNCATE for the instance-wide
// statement history without changing per-thread history or summaries.
func (r *RuntimeRecorder) ResetStatementHistoryLong() {
	if r == nil {
		return
	}
	r.statementMu.Lock()
	r.statementEvents = r.statementEvents[:0]
	r.statementMu.Unlock()
}

// ResetStageHistory implements TRUNCATE for stage history tables without
// changing statement history or stage summaries.
func (r *RuntimeRecorder) ResetStageHistory() {
	if r == nil {
		return
	}
	r.statementMu.Lock()
	r.stageEvents = r.stageEvents[:0]
	r.stageByThread = make(map[int64][]StatementEvent)
	r.statementMu.Unlock()
}

// ResetStageHistoryLong implements TRUNCATE for the instance-wide stage
// history without changing per-thread stage history.
func (r *RuntimeRecorder) ResetStageHistoryLong() {
	if r == nil {
		return
	}
	r.statementMu.Lock()
	r.stageEvents = r.stageEvents[:0]
	r.statementMu.Unlock()
}

// StatementSummary returns a deterministic copy of instance-lifetime
// statement aggregates. Unlike StatementHistory, this snapshot is not
// truncated at statementHistoryLimit.
func (r *RuntimeRecorder) StatementSummary() []StatementSummaryRow {
	if r == nil {
		return nil
	}
	r.statementMu.RLock()
	rows := make([]StatementSummaryRow, 0, len(r.statementSummary))
	for _, row := range r.statementSummary {
		if row != nil {
			copyRow := *row
			copyRow.LatencySamples = append([]int64(nil), row.LatencySamples...)
			rows = append(rows, copyRow)
		}
	}
	r.statementMu.RUnlock()
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
		if rows[i].Schema != rows[j].Schema {
			return rows[i].Schema < rows[j].Schema
		}
		if rows[i].StatementType != rows[j].StatementType {
			return rows[i].StatementType < rows[j].StatementType
		}
		return rows[i].SQL < rows[j].SQL
	})
	return rows
}

// statementSummaryDimensionNames are independent Performance Schema summary
// sources. They cannot be reconstructed from one shared identity map after a
// dimension-specific TRUNCATE, because MySQL permits account/host/user/thread
// resets without resetting the global summary.
var statementSummaryDimensionNames = []string{"global", "thread", "account", "host", "user"}

func statementSummaryDimensionKey(event StatementEvent, dimension string) string {
	eventName := "statement/sql/" + strings.ToLower(event.StatementType)
	switch dimension {
	case "thread":
		return fmt.Sprintf("%d\x00%s", event.ThreadID, eventName)
	case "account":
		return fmt.Sprintf("%s\x00%s\x00%s", event.User, event.Host, eventName)
	case "host":
		return fmt.Sprintf("%s\x00%s", event.Host, eventName)
	case "user":
		return fmt.Sprintf("%s\x00%s", event.User, eventName)
	default:
		return eventName
	}
}

func (r *RuntimeRecorder) recordStatementSummaryDimensionsLocked(event StatementEvent, timer int64) {
	if r.statementSummaryByDimension == nil {
		r.statementSummaryByDimension = make(map[string]map[string]*StatementSummaryRow)
	}
	for _, dimension := range statementSummaryDimensionNames {
		rows := r.statementSummaryByDimension[dimension]
		if rows == nil {
			rows = make(map[string]*StatementSummaryRow)
			r.statementSummaryByDimension[dimension] = rows
		}
		key := statementSummaryDimensionKey(event, dimension)
		row := rows[key]
		if row == nil {
			row = &StatementSummaryRow{
				ThreadID: event.ThreadID, User: event.User, Host: event.Host,
				Schema: event.Schema, StatementType: event.StatementType, SQL: event.SQL,
			}
			rows[key] = row
		}
		accumulateStatementSummary(row, event, timer)
	}
}

// StatementSummaryForDimension returns a stable copy of one native summary
// table's independent aggregate.
func (r *RuntimeRecorder) StatementSummaryForDimension(dimension string) []StatementSummaryRow {
	if r == nil {
		return nil
	}
	dimension = strings.ToLower(strings.TrimSpace(dimension))
	r.statementMu.RLock()
	rowsByKey := r.statementSummaryByDimension[dimension]
	rows := make([]StatementSummaryRow, 0, len(rowsByKey))
	for _, row := range rowsByKey {
		if row == nil {
			continue
		}
		copyRow := *row
		copyRow.LatencySamples = append([]int64(nil), row.LatencySamples...)
		rows = append(rows, copyRow)
	}
	r.statementMu.RUnlock()
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
		return rows[i].StatementType < rows[j].StatementType
	})
	return rows
}

func resetStatementSummaryRow(row *StatementSummaryRow) {
	if row == nil {
		return
	}
	identity := *row
	identity.Count = 0
	identity.SumTimerWait = 0
	identity.MinTimerWait = 0
	identity.MaxTimerWait = 0
	identity.Errors = 0
	identity.Warnings = 0
	identity.RowsAffected = 0
	identity.RowsSent = 0
	identity.RowsExamined = 0
	identity.SelectScan = 0
	identity.SelectRange = 0
	identity.SelectFullJoin = 0
	identity.SelectFullRangeJoin = 0
	identity.SelectRangeCheck = 0
	identity.SortRows = 0
	identity.SortScan = 0
	identity.SortRange = 0
	identity.NoIndexUsed = 0
	identity.NoGoodIndexUsed = 0
	identity.MaxControlledMemory = 0
	identity.MaxTotalMemory = 0
	identity.FirstSeen = time.Time{}
	identity.LastSeen = time.Time{}
	identity.LatencySamples = nil
	*row = identity
}

// ResetStatementSummaryDimension resets only one derived dimension. The
// global table uses ResetStatementSummaryGlobal because MySQL's global
// truncation also resets all derived statement dimensions.
func (r *RuntimeRecorder) ResetStatementSummaryDimension(dimension string) {
	if r == nil {
		return
	}
	dimension = strings.ToLower(strings.TrimSpace(dimension))
	r.statementMu.Lock()
	defer r.statementMu.Unlock()
	for _, row := range r.statementSummaryByDimension[dimension] {
		resetStatementSummaryRow(row)
	}
}

// StatementDigestSummary returns independent schema/digest aggregates used by
// Performance Schema's events_statements_summary_by_digest. It is separate
// from the identity-level statement summary so either summary family can be
// truncated without changing the other.
func (r *RuntimeRecorder) StatementDigestSummary() []StatementSummaryRow {
	if r == nil {
		return nil
	}
	r.statementMu.RLock()
	rows := make([]StatementSummaryRow, 0, len(r.statementDigestSummary))
	for _, row := range r.statementDigestSummary {
		if row != nil {
			copyRow := *row
			copyRow.LatencySamples = append([]int64(nil), row.LatencySamples...)
			rows = append(rows, copyRow)
		}
	}
	r.statementMu.RUnlock()
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Schema != rows[j].Schema {
			return rows[i].Schema < rows[j].Schema
		}
		return NormalizeStatementDigest(rows[i].SQL) < NormalizeStatementDigest(rows[j].SQL)
	})
	return rows
}

// StatementStageSummary returns independent stage/sql/execute aggregates.
// Stage summaries intentionally have their own lifecycle from statement and
// digest summaries so Performance Schema TRUNCATE operations remain isolated.
func (r *RuntimeRecorder) StatementStageSummary() []StatementSummaryRow {
	if r == nil {
		return nil
	}
	r.statementMu.RLock()
	rows := make([]StatementSummaryRow, 0, len(r.statementStageSummary))
	for _, row := range r.statementStageSummary {
		if row != nil {
			copyRow := *row
			copyRow.LatencySamples = append([]int64(nil), row.LatencySamples...)
			rows = append(rows, copyRow)
		}
	}
	r.statementMu.RUnlock()
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].ThreadID != rows[j].ThreadID {
			return rows[i].ThreadID < rows[j].ThreadID
		}
		if rows[i].User != rows[j].User {
			return rows[i].User < rows[j].User
		}
		return rows[i].Host < rows[j].Host
	})
	return rows
}

// StatementStageSummaryForDimension returns the independent stage/sql/execute
// aggregate for a Performance Schema dimension. The global view retains the
// identity-keyed source used by the global and historical projections; the
// other dimensions have their own lifecycle so TRUNCATE is isolated.
func (r *RuntimeRecorder) StatementStageSummaryForDimension(dimension string) []StatementSummaryRow {
	if r == nil {
		return nil
	}
	if dimension == "global" {
		return r.StatementStageSummary()
	}
	r.statementMu.RLock()
	rowsByKey := r.statementStageSummaryByDimension[dimension]
	rows := make([]StatementSummaryRow, 0, len(rowsByKey))
	for _, row := range rowsByKey {
		if row != nil {
			copyRow := *row
			copyRow.LatencySamples = append([]int64(nil), row.LatencySamples...)
			rows = append(rows, copyRow)
		}
	}
	r.statementMu.RUnlock()
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].ThreadID != rows[j].ThreadID {
			return rows[i].ThreadID < rows[j].ThreadID
		}
		if rows[i].User != rows[j].User {
			return rows[i].User < rows[j].User
		}
		return rows[i].Host < rows[j].Host
	})
	return rows
}

// StatementHistogramSamples returns the instance-wide statement latency
// samples used by EVENTS_STATEMENTS_HISTOGRAM_GLOBAL. It is separate from
// per-digest samples so the two MySQL histogram tables can be truncated
// independently.
func (r *RuntimeRecorder) StatementHistogramSamples() []int64 {
	if r == nil {
		return nil
	}
	r.statementMu.RLock()
	defer r.statementMu.RUnlock()
	return append([]int64(nil), r.statementHistogramSamples...)
}

// ResetStatementHistogramGlobal implements TRUNCATE of the global statement
// histogram without discarding statement summary rows.
func (r *RuntimeRecorder) ResetStatementHistogramGlobal() {
	if r == nil {
		return
	}
	r.statementMu.Lock()
	r.statementHistogramSamples = r.statementHistogramSamples[:0]
	r.statementMu.Unlock()
}

// ResetStatementHistogramByDigest implements TRUNCATE of the digest
// histogram without discarding the lifetime digest summary aggregates.
func (r *RuntimeRecorder) ResetStatementHistogramByDigest() {
	if r == nil {
		return
	}
	r.statementMu.Lock()
	for _, summary := range r.statementDigestSummary {
		if summary != nil {
			summary.LatencySamples = summary.LatencySamples[:0]
		}
	}
	r.statementMu.Unlock()
}

// ResetStatementSummaryByDigest implements TRUNCATE of the digest summary
// table. The global/event-name summary remains intact.
func (r *RuntimeRecorder) ResetStatementSummaryByDigest() {
	if r == nil {
		return
	}
	r.statementMu.Lock()
	r.statementDigestSummary = make(map[statementDigestKey]*StatementSummaryRow)
	r.statementMu.Unlock()
}

// ResetStatementSummaryGlobal resets the identity/event-name statement
// summaries while retaining the digest summary rows. Existing keys remain so
// the event-name projections can continue to expose zeroed summary rows.
func (r *RuntimeRecorder) ResetStatementSummaryGlobal() {
	if r == nil {
		return
	}
	r.statementMu.Lock()
	for _, rows := range r.statementSummaryByDimension {
		for _, summary := range rows {
			resetStatementSummaryRow(summary)
		}
	}
	for _, summary := range r.statementSummary {
		resetStatementSummaryRow(summary)
	}
	r.statementMu.Unlock()
}

// ResetStatementStageSummary implements TRUNCATE of the global stage summary
// while retaining identity rows for subsequent accumulation.
func (r *RuntimeRecorder) ResetStatementStageSummary() {
	if r == nil {
		return
	}
	r.statementMu.Lock()
	for _, summary := range r.statementStageSummary {
		if summary == nil {
			continue
		}
		*summary = StatementSummaryRow{
			ThreadID: summary.ThreadID, User: summary.User, Host: summary.Host,
			StatementType: "stage/sql/execute",
		}
	}
	r.statementMu.Unlock()
}

// ResetStatementStageSummaryDimension implements an isolated TRUNCATE for a
// non-global stage summary dimension.
func (r *RuntimeRecorder) ResetStatementStageSummaryDimension(dimension string) {
	if r == nil || dimension == "global" {
		return
	}
	r.statementMu.Lock()
	for _, summary := range r.statementStageSummaryByDimension[dimension] {
		if summary == nil {
			continue
		}
		identity := StatementSummaryRow{
			ThreadID: summary.ThreadID, User: summary.User, Host: summary.Host,
			StatementType: "stage/sql/execute",
		}
		*summary = identity
	}
	r.statementMu.Unlock()
}

// RecordMemoryAllocation records one instrumented allocation and updates its
// current/high-water lifecycle counters. The legacy entry point has no client
// identity, so it updates only the global and thread aggregates.
func (r *RuntimeRecorder) RecordMemoryAllocation(threadID int64, eventName string, bytes int64) {
	r.recordMemoryAllocation(threadID, "", "", eventName, bytes)
}

// RecordMemoryAllocationWithIdentity records one allocation in all independent
// Performance Schema memory-summary dimensions. Keeping those dimensions
// separate is required because MySQL permits account, host, user, and thread
// summaries to be truncated independently.
func (r *RuntimeRecorder) RecordMemoryAllocationWithIdentity(threadID int64, user, host, eventName string, bytes int64) {
	r.recordMemoryAllocation(threadID, user, host, eventName, bytes)
}

func (r *RuntimeRecorder) recordMemoryAllocation(threadID int64, user, host, eventName string, bytes int64) {
	if r == nil || bytes < 0 {
		return
	}
	if eventName == "" {
		eventName = "memory/sql/THD::main_mem_root"
	}
	host = normalizeMemoryHost(host)
	r.memoryMu.Lock()
	defer r.memoryMu.Unlock()
	for dimension, key := range memorySummaryDimensionKeys(threadID, user, host, eventName) {
		row := r.memorySummaryRowForDimensionLocked(dimension, key, threadID, user, host, eventName)
		recordMemoryAllocationLocked(row, bytes)
	}
}

// RecordMemoryFree records one instrumented release. Counters are clamped at
// zero so an incomplete cleanup path cannot expose negative Performance Schema
// usage. The legacy entry point has no client identity.
func (r *RuntimeRecorder) RecordMemoryFree(threadID int64, eventName string, bytes int64) {
	r.recordMemoryFree(threadID, "", "", eventName, bytes)
}

// RecordMemoryFreeWithIdentity records one release in all independent memory
// summary dimensions.
func (r *RuntimeRecorder) RecordMemoryFreeWithIdentity(threadID int64, user, host, eventName string, bytes int64) {
	r.recordMemoryFree(threadID, user, host, eventName, bytes)
}

func (r *RuntimeRecorder) recordMemoryFree(threadID int64, user, host, eventName string, bytes int64) {
	if r == nil || bytes < 0 {
		return
	}
	if eventName == "" {
		eventName = "memory/sql/THD::main_mem_root"
	}
	host = normalizeMemoryHost(host)
	r.memoryMu.Lock()
	defer r.memoryMu.Unlock()
	for dimension, key := range memorySummaryDimensionKeys(threadID, user, host, eventName) {
		row := r.memorySummaryRowForDimensionLocked(dimension, key, threadID, user, host, eventName)
		recordMemoryFreeLocked(row, bytes)
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

var memorySummaryDimensionNames = []string{"global", "thread", "account", "host", "user"}

func normalizeMemoryHost(host string) string {
	if strings.TrimSpace(host) == "" {
		return "localhost"
	}
	return host
}

func memorySummaryDimensionKeys(threadID int64, user, host, eventName string) map[string]string {
	return map[string]string{
		"global":  eventName,
		"thread":  fmt.Sprintf("%d\x00%s", threadID, eventName),
		"account": fmt.Sprintf("%s\x00%s\x00%s", user, host, eventName),
		"host":    fmt.Sprintf("%s\x00%s", host, eventName),
		"user":    fmt.Sprintf("%s\x00%s", user, eventName),
	}
}

func (r *RuntimeRecorder) memorySummaryRowForDimensionLocked(dimension, key string, threadID int64, user, host, eventName string) *MemorySummaryRow {
	if r.memorySummaryByDimension == nil {
		r.memorySummaryByDimension = make(map[string]map[string]*MemorySummaryRow)
	}
	rows := r.memorySummaryByDimension[dimension]
	if rows == nil {
		rows = make(map[string]*MemorySummaryRow)
		r.memorySummaryByDimension[dimension] = rows
	}
	if dimension == "thread" {
		if r.memoryRows == nil {
			r.memoryRows = make(map[memorySummaryKey]*MemorySummaryRow)
		}
		legacyKey := memorySummaryKey{threadID: threadID, eventName: eventName}
		if row := r.memoryRows[legacyKey]; row != nil {
			rows[key] = row
			return row
		}
	}
	if row := rows[key]; row != nil {
		return row
	}
	row := &MemorySummaryRow{ThreadID: threadID, User: user, Host: host, EventName: eventName}
	rows[key] = row
	if dimension == "thread" {
		r.memoryRows[memorySummaryKey{threadID: threadID, eventName: eventName}] = row
	}
	return row
}

func recordMemoryAllocationLocked(row *MemorySummaryRow, bytes int64) {
	if row == nil {
		return
	}
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

func recordMemoryFreeLocked(row *MemorySummaryRow, bytes int64) {
	if row == nil {
		return
	}
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

// MemorySummaryForDimension returns a stable copy of one independent memory
// summary source. The global and derived dimensions are intentionally not
// reconstructed from thread rows after a dimension-specific TRUNCATE.
func (r *RuntimeRecorder) MemorySummaryForDimension(dimension string) []MemorySummaryRow {
	if r == nil {
		return nil
	}
	dimension = strings.ToLower(strings.TrimSpace(dimension))
	r.memoryMu.RLock()
	rowsByKey := r.memorySummaryByDimension[dimension]
	rows := make([]MemorySummaryRow, 0, len(rowsByKey))
	for _, row := range rowsByKey {
		if row != nil {
			rows = append(rows, *row)
		}
	}
	r.memoryMu.RUnlock()
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].ThreadID != rows[j].ThreadID {
			return rows[i].ThreadID < rows[j].ThreadID
		}
		if rows[i].EventName != rows[j].EventName {
			return rows[i].EventName < rows[j].EventName
		}
		if rows[i].User != rows[j].User {
			return rows[i].User < rows[j].User
		}
		return rows[i].Host < rows[j].Host
	})
	return rows
}

func resetMemorySummaryRow(row *MemorySummaryRow) {
	if row == nil {
		return
	}
	identity := *row
	identity.CountAlloc = 0
	identity.CountFree = 0
	identity.BytesAlloc = 0
	identity.BytesFree = 0
	identity.LowCountUsed = identity.CurrentCountUsed
	identity.HighCountUsed = identity.CurrentCountUsed
	identity.LowBytesUsed = identity.CurrentBytesUsed
	identity.HighBytesUsed = identity.CurrentBytesUsed
	*row = identity
}

// ResetMemorySummaryDimension implements MySQL's independent TRUNCATE
// semantics for one memory summary dimension.
func (r *RuntimeRecorder) ResetMemorySummaryDimension(dimension string) {
	if r == nil {
		return
	}
	dimension = strings.ToLower(strings.TrimSpace(dimension))
	if dimension == "global" {
		r.ResetMemorySummary()
		return
	}
	r.memoryMu.Lock()
	defer r.memoryMu.Unlock()
	for _, row := range r.memorySummaryByDimension[dimension] {
		resetMemorySummaryRow(row)
	}
}

// ResetMemorySummary establishes a new baseline for memory counters while
// preserving current ownership and summary identities. This mirrors MySQL's
// memory-summary TRUNCATE semantics: paired allocation/free counters and byte
// totals are reduced by the same completed baseline, and watermarks become
// the current usage.
func (r *RuntimeRecorder) ResetMemorySummary() {
	if r == nil {
		return
	}
	r.memoryMu.Lock()
	defer r.memoryMu.Unlock()
	for _, rows := range r.memorySummaryByDimension {
		for _, row := range rows {
			resetMemorySummaryRow(row)
		}
	}
}

// RecordFileOpen records one physical file handle becoming active.
func (r *RuntimeRecorder) RecordFileOpen(fileName string) {
	if r == nil {
		return
	}
	eventName := fileEventName(fileName)
	enabled, _ := r.instrumentSetting(eventName)
	if enabled {
		r.fileSummaryRecorder().open(fileName, eventName)
	}
}

// RecordFileClose records one physical file handle becoming inactive.
func (r *RuntimeRecorder) RecordFileClose(fileName string) {
	if r == nil {
		return
	}
	r.fileSummaryRecorder().close(fileName, fileEventName(fileName))
}

// RecordFileRead records a completed physical file read.
func (r *RuntimeRecorder) RecordFileRead(fileName string, latency time.Duration) {
	r.RecordFileReadBytes(fileName, 0, latency)
}

// RecordFileReadBytes records a completed physical file read and its byte
// count.  The byte-aware form backs Performance Schema's
// SUM_NUMBER_OF_BYTES_READ column; the legacy method above remains useful for
// callers that only have timing information.
func (r *RuntimeRecorder) RecordFileReadBytes(fileName string, bytes int64, latency time.Duration) {
	if r == nil {
		return
	}
	eventName := fileEventName(fileName)
	enabled, timed := r.instrumentSetting(eventName)
	if enabled {
		r.fileSummaryRecorder().read(fileName, eventName, bytes, latency, timed)
	}
}

// RecordFileWrite records a completed physical file write.
func (r *RuntimeRecorder) RecordFileWrite(fileName string, latency time.Duration) {
	r.RecordFileWriteBytes(fileName, 0, latency)
}

// RecordFileWriteBytes records a completed physical file write and its byte
// count.  The byte-aware form backs Performance Schema's
// SUM_NUMBER_OF_BYTES_WRITE column.
func (r *RuntimeRecorder) RecordFileWriteBytes(fileName string, bytes int64, latency time.Duration) {
	if r == nil {
		return
	}
	eventName := fileEventName(fileName)
	enabled, timed := r.instrumentSetting(eventName)
	if enabled {
		r.fileSummaryRecorder().write(fileName, eventName, bytes, latency, timed)
	}
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

// ResetFileSummary implements Performance Schema file-summary truncation
// without discarding file identities or currently open handles.
func (r *RuntimeRecorder) ResetFileSummary() {
	if r == nil {
		return
	}
	r.fileIOMu.Lock()
	fileIO := r.fileIO
	r.fileIOMu.Unlock()
	if fileIO != nil {
		fileIO.reset()
	}
}

// RecordQuery records one completed query.
func (r *RuntimeRecorder) RecordQuery(database, statementType, status string, latency time.Duration) {
	if r == nil {
		return
	}
	statementType = strings.ToLower(strings.TrimSpace(statementType))
	r.queryMu.Lock()
	if r.queryTotals == nil {
		r.queryTotals = make(map[string]int64)
	}
	r.queryTotals[statementType]++
	r.queryMu.Unlock()
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

// QueryTotals returns server-lifetime query counts grouped by normalized
// statement type. Unlike StatementHistory, these counters are not bounded by
// the Performance Schema history size and are suitable for global status
// counters such as Com_select and Questions.
func (r *RuntimeRecorder) QueryTotals() map[string]int64 {
	if r == nil {
		return nil
	}
	r.queryMu.RLock()
	totals := make(map[string]int64, len(r.queryTotals))
	for statementType, count := range r.queryTotals {
		totals[statementType] = count
	}
	r.queryMu.RUnlock()
	return totals
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

// RecordQueryErrorHandledWithIdentity records that an already-raised query
// error was consumed by a SQL exception handler. MySQL exposes this as
// SUM_ERROR_HANDLED; it is deliberately separate from WarningCount because
// warnings and handled exceptions are different Performance Schema metrics.
func (r *RuntimeRecorder) RecordQueryErrorHandledWithIdentity(errorCode string, threadID int64, user, host string) {
	if r == nil {
		return
	}
	if errorCode == "" {
		errorCode = "UNKNOWN"
	}
	r.errorMu.Lock()
	if row := r.errorRows[errorCode]; row != nil {
		row.HandledCount++
	}
	if threadID != 0 || user != "" || host != "" {
		key := fmt.Sprintf("%d\x00%s\x00%s\x00%s", threadID, user, host, errorCode)
		if identity := r.errorByIdentity[key]; identity != nil {
			identity.HandledCount++
		}
		if threadID != 0 {
			if identity := r.errorByThread[fmt.Sprintf("%d\x00%s", threadID, errorCode)]; identity != nil {
				identity.HandledCount++
			}
		}
		if user != "" && host != "" {
			if identity := r.errorByAccount[user+"\x00"+host+"\x00"+errorCode]; identity != nil {
				identity.HandledCount++
			}
		}
		if host != "" {
			if identity := r.errorByHost[host+"\x00"+errorCode]; identity != nil {
				identity.HandledCount++
			}
		}
		if user != "" {
			if identity := r.errorByUser[user+"\x00"+errorCode]; identity != nil {
				identity.HandledCount++
			}
		}
	}
	r.errorMu.Unlock()
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
		Logged: now, ThreadID: threadID, Priority: 3, ErrorCode: errorCode, Subsystem: "query",
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
	if row.FirstSeen.IsZero() {
		row.FirstSeen = now
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
		if identity.FirstSeen.IsZero() {
			identity.FirstSeen = now
		}
		identity.ErrorCount++
		identity.LastSeen = now
		if threadID != 0 {
			recordErrorIdentitySummaryLocked(r.errorByThread, fmt.Sprintf("%d\x00%s", threadID, errorCode), threadID, user, host, errorCode, now)
		}
		if user != "" && host != "" {
			recordErrorIdentitySummaryLocked(r.errorByAccount, user+"\x00"+host+"\x00"+errorCode, 0, user, host, errorCode, now)
		}
		if host != "" {
			recordErrorIdentitySummaryLocked(r.errorByHost, host+"\x00"+errorCode, 0, "", host, errorCode, now)
		}
		if user != "" {
			recordErrorIdentitySummaryLocked(r.errorByUser, user+"\x00"+errorCode, 0, user, "", errorCode, now)
		}
	}
	r.errorMu.Unlock()
}

func recordErrorIdentitySummaryLocked(rows map[string]*ErrorIdentitySummaryRow, key string, threadID int64, user, host, errorCode string, now time.Time) {
	row := rows[key]
	if row == nil {
		row = &ErrorIdentitySummaryRow{ThreadID: threadID, User: user, Host: host, ErrorName: errorCode, FirstSeen: now}
		rows[key] = row
	}
	if row.FirstSeen.IsZero() {
		row.FirstSeen = now
	}
	row.ErrorCount++
	row.LastSeen = now
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

// ErrorSummaryByDimension returns the independent aggregate used by one
// Performance Schema error-summary dimension. Global truncation may reset all
// dimensions together, but account/host/user/thread truncation must not erase
// the other dimensions.
func (r *RuntimeRecorder) ErrorSummaryByDimension(dimension string) []ErrorIdentitySummaryRow {
	if r == nil {
		return nil
	}
	r.errorMu.RLock()
	var source map[string]*ErrorIdentitySummaryRow
	switch strings.ToLower(strings.TrimSpace(dimension)) {
	case "account":
		source = r.errorByAccount
	case "host":
		source = r.errorByHost
	case "user":
		source = r.errorByUser
	case "thread":
		source = r.errorByThread
	default:
		source = r.errorByIdentity
	}
	rows := make([]ErrorIdentitySummaryRow, 0, len(source))
	for _, row := range source {
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

// ResetErrorSummaries resets the in-process aggregates used by the
// Performance Schema error-summary views. Existing rows are retained with
// zero counters and cleared timestamps, matching MySQL's global/thread
// summary-table TRUNCATE behavior. It intentionally leaves the bounded
// error log untouched; MySQL exposes error_log as a separate table with
// different truncation semantics.
func (r *RuntimeRecorder) ResetErrorSummaries() {
	if r == nil {
		return
	}
	r.errorMu.Lock()
	r.resetErrorSummaryDimensionLocked("global")
	r.errorMu.Unlock()
}

// ResetErrorSummaryDimension resets only one error-summary dimension. The
// global table follows MySQL's implicit-truncate rule and resets all derived
// dimensions; a derived table reset remains isolated.
func (r *RuntimeRecorder) ResetErrorSummaryDimension(dimension string) {
	if r == nil {
		return
	}
	r.errorMu.Lock()
	r.resetErrorSummaryDimensionLocked(dimension)
	r.errorMu.Unlock()
}

func (r *RuntimeRecorder) resetErrorSummaryDimensionLocked(dimension string) {
	resetGlobal := strings.EqualFold(strings.TrimSpace(dimension), "global")
	if resetGlobal {
		resetErrorSummaryRowsLocked(r.errorRows)
		resetErrorIdentityRowsLocked(r.errorByIdentity)
		resetErrorIdentityRowsLocked(r.errorByAccount)
		resetErrorIdentityRowsLocked(r.errorByHost)
		resetErrorIdentityRowsLocked(r.errorByUser)
		resetErrorIdentityRowsLocked(r.errorByThread)
		return
	}
	switch strings.ToLower(strings.TrimSpace(dimension)) {
	case "account":
		resetErrorIdentityRowsLocked(r.errorByAccount)
	case "host":
		resetErrorIdentityRowsLocked(r.errorByHost)
	case "user":
		resetErrorIdentityRowsLocked(r.errorByUser)
	case "thread":
		resetErrorIdentityRowsLocked(r.errorByThread)
	}
}

func resetErrorSummaryRowsLocked(rows map[string]*ErrorSummaryRow) {
	for _, row := range rows {
		if row == nil {
			continue
		}
		row.ErrorCount = 0
		row.WarningCount = 0
		row.HandledCount = 0
		row.FirstSeen = time.Time{}
		row.LastSeen = time.Time{}
	}
}

func resetErrorIdentityRowsLocked(rows map[string]*ErrorIdentitySummaryRow) {
	for _, row := range rows {
		if row == nil {
			continue
		}
		row.ErrorCount = 0
		row.WarningCount = 0
		row.HandledCount = 0
		row.FirstSeen = time.Time{}
		row.LastSeen = time.Time{}
	}
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
	r.RecordTransactionCommitWithMode(isolationLevel, false)
}

// RecordTransactionCommitWithMode records a commit and its read-only mode.
// The mode is captured at the transaction completion boundary, where the
// executor still has the session's transaction characteristics.
func (r *RuntimeRecorder) RecordTransactionCommitWithMode(isolationLevel string, readOnly bool) {
	r.RecordTransactionCommitWithModeAndDML(isolationLevel, readOnly, false)
}

// RecordTransactionCommitWithModeAndDML records a transaction commit together
// with the transaction's access mode and whether it applied INSERT or UPDATE
// changes before the completion boundary.
func (r *RuntimeRecorder) RecordTransactionCommitWithModeAndDML(isolationLevel string, readOnly, hadInsertUpdate bool) {
	if r == nil {
		return
	}
	_ = r.registry.IncCounter("xmysql_transactions_committed_total", 1, Labels{
		"isolation_level": isolationLevel,
	})
	r.transactionMu.Lock()
	r.transactionCommit++
	if readOnly {
		r.transactionROCommit++
	} else {
		r.transactionRWCommit++
	}
	if hadInsertUpdate {
		r.transactionDMLCommit++
	}
	r.transactionMu.Unlock()
}

// RecordNonLockingAutocommitReadOnlyCommit records an InnoDB table read that
// completed as a non-locking auto-commit read-only transaction. MySQL keeps
// this counter separate from explicit read-only transaction commits.
func (r *RuntimeRecorder) RecordNonLockingAutocommitReadOnlyCommit() {
	if r == nil {
		return
	}
	r.transactionMu.Lock()
	r.transactionNLROCommit++
	r.transactionMu.Unlock()
}

// RecordTransactionRollback records one rolled-back transaction.
func (r *RuntimeRecorder) RecordTransactionRollback(isolationLevel, reason string) {
	if r == nil {
		return
	}
	_ = r.registry.IncCounter("xmysql_transactions_rolled_back_total", 1, Labels{
		"isolation_level": isolationLevel,
		"reason":          reason,
	})
	r.transactionMu.Lock()
	r.transactionRollback++
	r.transactionMu.Unlock()
}

// RecordTransactionRollbackToSavepoint records a successful rollback to a
// savepoint at the transaction statement boundary.
func (r *RuntimeRecorder) RecordTransactionRollbackToSavepoint() {
	if r == nil {
		return
	}
	r.transactionMu.Lock()
	r.transactionRollbackSavepoint++
	r.transactionMu.Unlock()
}

// TransactionTotals returns the instance-lifetime commit and rollback totals
// recorded at the transaction completion boundary. The counters are kept
// separately from the Prometheus registry so compatibility views can project
// them without parsing rendered metric text.
func (r *RuntimeRecorder) TransactionTotals() (committed, rolledBack int64) {
	if r == nil {
		return 0, 0
	}
	r.transactionMu.RLock()
	committed = r.transactionCommit
	rolledBack = r.transactionRollback
	r.transactionMu.RUnlock()
	return committed, rolledBack
}

// TransactionCommitModeTotals returns the instance-lifetime read-write and
// read-only commit totals.
func (r *RuntimeRecorder) TransactionCommitModeTotals() (readWrite, readOnly int64) {
	if r == nil {
		return 0, 0
	}
	r.transactionMu.RLock()
	readWrite = r.transactionRWCommit
	readOnly = r.transactionROCommit
	r.transactionMu.RUnlock()
	return readWrite, readOnly
}

// TransactionDMLCommitTotal returns the instance-lifetime number of committed
// transactions that contained at least one recorded DML change.
func (r *RuntimeRecorder) TransactionDMLCommitTotal() int64 {
	if r == nil {
		return 0
	}
	r.transactionMu.RLock()
	count := r.transactionDMLCommit
	r.transactionMu.RUnlock()
	return count
}

// TransactionNonLockingReadOnlyCommitTotal returns the instance-lifetime
// count of non-locking auto-commit read-only transactions.
func (r *RuntimeRecorder) TransactionNonLockingReadOnlyCommitTotal() int64 {
	if r == nil {
		return 0
	}
	r.transactionMu.RLock()
	count := r.transactionNLROCommit
	r.transactionMu.RUnlock()
	return count
}

// TransactionRollbackSavepointTotal returns the instance-lifetime number of
// successful rollbacks to savepoints.
func (r *RuntimeRecorder) TransactionRollbackSavepointTotal() int64 {
	if r == nil {
		return 0
	}
	r.transactionMu.RLock()
	count := r.transactionRollbackSavepoint
	r.transactionMu.RUnlock()
	return count
}

// RecordTableHandleOpen records table-handle lease acquisition at the
// executor boundary. The reference count represents currently held leases;
// opened and closed are instance-lifetime counters.
func (r *RuntimeRecorder) RecordTableHandleOpen(count int) {
	if r == nil || count <= 0 {
		return
	}
	r.tableHandleMu.Lock()
	r.tableHandlesOpened += int64(count)
	r.tableHandleRefs += int64(count)
	r.tableHandleMu.Unlock()
}

// RecordTableHandleClose records table-handle lease release. The reference
// count is clamped so a duplicate cleanup cannot create a negative runtime
// value.
func (r *RuntimeRecorder) RecordTableHandleClose(count int) {
	if r == nil || count <= 0 {
		return
	}
	r.tableHandleMu.Lock()
	closed := int64(count)
	if closed > r.tableHandleRefs {
		closed = r.tableHandleRefs
	}
	r.tableHandlesClosed += closed
	r.tableHandleRefs -= closed
	r.tableHandleMu.Unlock()
}

// TableHandleTotals returns the table-handle lifecycle counters used by
// INFORMATION_SCHEMA.INNODB_METRICS.
func (r *RuntimeRecorder) TableHandleTotals() (opened, closed, references int64) {
	if r == nil {
		return 0, 0, 0
	}
	r.tableHandleMu.RLock()
	opened = r.tableHandlesOpened
	closed = r.tableHandlesClosed
	references = r.tableHandleRefs
	r.tableHandleMu.RUnlock()
	return opened, closed, references
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

// RecordLockTimeout records a real lock wait that ended because its deadline
// elapsed. Callers must not use this for immediate lock conflicts.
func (r *RuntimeRecorder) RecordLockTimeout() {
	if r == nil {
		return
	}
	r.lockTimeoutMu.Lock()
	r.lockTimeouts++
	r.lockTimeoutMu.Unlock()
}

// LockTimeouts returns the instance-lifetime lock timeout count.
func (r *RuntimeRecorder) LockTimeouts() int64 {
	if r == nil {
		return 0
	}
	r.lockTimeoutMu.RLock()
	count := r.lockTimeouts
	r.lockTimeoutMu.RUnlock()
	return count
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
