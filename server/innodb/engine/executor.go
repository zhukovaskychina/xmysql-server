package engine

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	innodbcommon "github.com/zhukovaskychina/xmysql-server/server/innodb/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	querypb "github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser/dependency/querypb"
	observabilitymetrics "github.com/zhukovaskychina/xmysql-server/server/observability/metrics"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

var jdbcTableCatIdentifierPattern = regexp.MustCompile(`(?i)(^|[^a-z0-9_])table_cat([^a-z0-9_]|$)`)

// XMySQLExecutor 是 SQL 执行器的核心结构，负责整个 SQL 的解析与执行
// 支持解析 SELECT、DDL、SHOW 等语句，并调用相应执行逻辑
// 注意：实际的算子执行使用volcano_executor.go中的Operator接口和火山模型
// 执行流程：解析 -> 生成逻辑计划 -> 转物理计划 -> 构造执行器 -> 流式迭代执行
// DML 统一通过 StorageIntegratedDMLExecutor 写入真实存储；仅保留少量
// 无存储上下文的 parser/routing helper 供单元测试使用。

// XMySQLExecutor SQL执行器结构体
type XMySQLExecutor struct {
	infosSchemaManager metadata.InfoSchemaManager // 信息模式管理器
	conf               *conf.Cfg                  // 配置项
	ctx                *ExecutionContext          // 执行上下文
	results            chan *Result               // 结果通道

	// 管理器组件 - 添加这些字段来访问各个管理器
	optimizerManager  interface{} // 查询优化器管理器
	bufferPoolManager interface{} // 缓冲池管理器
	btreeManager      interface{} // B+树管理器
	tableManager      interface{} // 表管理器

	// 存储引擎相关管理器 - 新增字段
	indexManager                            *manager.IndexManager        // 索引管理器
	storageManager                          *manager.StorageManager      // 存储管理器
	tableStorageManager                     *manager.TableStorageManager // 表存储映射管理器
	txManager                               *manager.TransactionManager  // 事务管理器
	lockManager                             *manager.LockManager         // 锁等待诊断
	metricsRecorder                         *observabilitymetrics.RuntimeRecorder
	replicationCommitHook                   func([]replication.Statement) error
	replicationCommitTransactionHook        func([]replication.RowChange, []replication.Statement) error
	replicationCommitTransactionHookWithID  func(string, []replication.RowChange, []replication.Statement) error
	replicationStatus                       func() replication.StatusSnapshot
	replicationSource                       func() *replication.Source
	replicationReplica                      func() *replication.Replica
	replicaRegistrations                    func() []replication.ReplicaRegistration
	replicationStart                        func() error
	replicationStop                         func() error
	replicationChangeSource                 func(string) error
	replicationReset                        func() error
	replicationResetAll                     func() error
	replicationFlushLogs                    func() error
	replicationResetMaster                  func() error
	sessionKill                             func(uint32, server.MySQLServerSession) error
	sessionQueryKill                        func(uint32, server.MySQLServerSession) error
	processlistProvider                     func() []server.MySQLServerSession
	eventScheduler                          *EventScheduler
	accountMu                               sync.Mutex
	ddlCoordinatorMu                        sync.Mutex
	ddlCoordinator                          *tableDDLCoordinator
	globalReadLock                          globalReadLockGate
	globalReadLockStateMu                   sync.Mutex
	globalReadLockOwner                     server.MySQLServerSession
	globalReadLockHeld                      bool
	performanceSchemaMu                     sync.RWMutex
	performanceSchemaConsumers              map[string]performanceSchemaSetupSetting
	performanceSchemaInstruments            map[string]performanceSchemaSetupSetting
	performanceSchemaObjects                map[string]performanceSchemaSetupSetting
	performanceSchemaLoggers                map[string]performanceSchemaTelemetryLogger
	performanceSchemaMeters                 map[string]performanceSchemaTelemetryMeter
	performanceSchemaActors                 performanceSchemaActorSetting
	performanceSchemaActorSessions          map[server.MySQLServerSession]performanceSchemaActorSetting
	performanceSchemaTransactionHistoryLong []performanceSchemaTransactionEvent
	performanceSchemaTransactionSummaries   []performanceSchemaTransactionEvent
	performanceSchemaProgramHistoryLong     []performanceSchemaProgramEvent
	optimizerTraceMu                        sync.RWMutex
	optimizerTraces                         []optimizerTraceEntry
	activeTransactions                      atomic.Int64
	activeQueryMu                           sync.Mutex
	activeQueries                           map[server.MySQLServerSession]activeQueryHandle
	activeQuerySequence                     atomic.Uint64
	activeStatementMu                       sync.RWMutex
	activeStatements                        map[server.MySQLServerSession]activeStatement
	activeStatementSequence                 atomic.Uint64
	xaMu                                    sync.Mutex
	xaPrepared                              map[string]*xaPreparedTransaction
	xaSuspended                             map[string]*xaSuspendedTransaction
}

type activeQueryHandle struct {
	token  uint64
	cancel context.CancelFunc
}

type activeStatement struct {
	token uint64
	event observabilitymetrics.StatementEvent
}

type definerExecutionSession struct {
	server.MySQLServerSession
	user string
	host string
}

func (s *definerExecutionSession) GetParamByName(name string) interface{} {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "user":
		return s.user
	case "host":
		return s.host
	default:
		return s.MySQLServerSession.GetParamByName(name)
	}
}

func (s *definerExecutionSession) SetParamByName(name string, value interface{}) {
	s.MySQLServerSession.SetParamByName(name, value)
}

func (s *definerExecutionSession) SessionContext() *server.SessionContext {
	return s.MySQLServerSession.SessionContext()
}

type performanceSchemaSetupSetting struct {
	Enabled bool
	Timed   bool
}

type performanceSchemaActorSetting struct {
	Enabled bool
	History bool
}

type sessionTransactionState struct {
	Changes        []transactionDMLChange
	Statements     []replication.Statement
	Savepoints     []sessionSavepoint
	AccessMode     string
	IsolationLevel string
}

type sessionSavepoint struct {
	name   string
	offset int
}

// NewXMySQLExecutor 构造 SQL 执行器实例
func NewXMySQLExecutor(infosSchemaManager metadata.InfoSchemaManager, conf *conf.Cfg) *XMySQLExecutor {
	if conf != nil {
		statePath := ""
		if strings.TrimSpace(conf.DataDir) != "" {
			statePath = filepath.Join(conf.DataDir, "uuid_short.state")
		}
		if err := plan.ConfigureDefaultUUIDShort(conf.ReplicationServerID, statePath); err != nil {
			logger.Warnf("failed to configure UUID_SHORT generator: %v", err)
		}
	}
	return &XMySQLExecutor{
		infosSchemaManager:             infosSchemaManager,
		conf:                           conf,
		metricsRecorder:                observabilitymetrics.DefaultRuntimeRecorder(),
		eventScheduler:                 NewEventScheduler(false),
		ddlCoordinator:                 newTableDDLCoordinator(),
		performanceSchemaConsumers:     defaultPerformanceSchemaConsumers(),
		performanceSchemaInstruments:   defaultPerformanceSchemaInstruments(),
		performanceSchemaObjects:       defaultPerformanceSchemaObjects(),
		performanceSchemaLoggers:       defaultPerformanceSchemaLoggers(),
		performanceSchemaMeters:        defaultPerformanceSchemaMeters(),
		performanceSchemaActors:        performanceSchemaActorSetting{Enabled: true, History: true},
		performanceSchemaActorSessions: make(map[server.MySQLServerSession]performanceSchemaActorSetting),
		optimizerTraces:                make([]optimizerTraceEntry, 0, optimizerTraceHistoryLimit),
		activeQueries:                  make(map[server.MySQLServerSession]activeQueryHandle),
		activeStatements:               make(map[server.MySQLServerSession]activeStatement),
		xaPrepared:                     make(map[string]*xaPreparedTransaction),
		xaSuspended:                    make(map[string]*xaSuspendedTransaction),
	}
}

func (e *XMySQLExecutor) beginActiveStatement(session server.MySQLServerSession, query, database string) func() {
	if e == nil || session == nil {
		return func() {}
	}
	token := e.activeStatementSequence.Add(1)
	event := activeStatement{
		token: token,
		event: observabilitymetrics.StatementEvent{
			Time:          time.Now(),
			ThreadID:      int64(sessionConnectionID(session)),
			Schema:        database,
			SQL:           strings.TrimSpace(query),
			StatementType: metricStatementType(query),
			Status:        "running",
		},
	}
	e.activeStatementMu.Lock()
	if e.activeStatements == nil {
		e.activeStatements = make(map[server.MySQLServerSession]activeStatement)
	}
	e.activeStatements[session] = event
	e.activeStatementMu.Unlock()
	return func() {
		e.activeStatementMu.Lock()
		if current, ok := e.activeStatements[session]; ok && current.token == token {
			delete(e.activeStatements, session)
		}
		e.activeStatementMu.Unlock()
	}
}

func (e *XMySQLExecutor) activeStatementEvents() []observabilitymetrics.StatementEvent {
	if e == nil {
		return nil
	}
	e.activeStatementMu.RLock()
	events := make([]observabilitymetrics.StatementEvent, 0, len(e.activeStatements))
	for _, statement := range e.activeStatements {
		events = append(events, statement.event)
	}
	e.activeStatementMu.RUnlock()
	sort.Slice(events, func(i, j int) bool {
		if events[i].ThreadID != events[j].ThreadID {
			return events[i].ThreadID < events[j].ThreadID
		}
		return events[i].Time.Before(events[j].Time)
	})
	return events
}

func (e *XMySQLExecutor) beginActiveQuery(session server.MySQLServerSession) (context.Context, func()) {
	queryContext, cancel := context.WithCancel(context.Background())
	if e == nil || session == nil {
		return queryContext, cancel
	}
	token := e.activeQuerySequence.Add(1)
	e.activeQueryMu.Lock()
	if e.activeQueries == nil {
		e.activeQueries = make(map[server.MySQLServerSession]activeQueryHandle)
	}
	e.activeQueries[session] = activeQueryHandle{token: token, cancel: cancel}
	e.activeQueryMu.Unlock()
	return queryContext, func() {
		e.activeQueryMu.Lock()
		if current, ok := e.activeQueries[session]; ok && current.token == token {
			delete(e.activeQueries, session)
		}
		e.activeQueryMu.Unlock()
		cancel()
	}
}

// CancelActiveQuery interrupts the current query for the connection id. An
// idle target is a successful no-op, matching MySQL KILL QUERY semantics.
func (e *XMySQLExecutor) CancelActiveQuery(targetID uint32) error {
	if e == nil || targetID == 0 {
		return fmt.Errorf("invalid KILL QUERY thread id")
	}
	var cancel context.CancelFunc
	e.activeQueryMu.Lock()
	for session, handle := range e.activeQueries {
		if sessionConnectionID(session) == targetID {
			cancel = handle.cancel
			break
		}
	}
	e.activeQueryMu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

func sessionConnectionID(session server.MySQLServerSession) uint32 {
	if session == nil {
		return 0
	}
	if ctx := session.SessionContext(); ctx != nil {
		if id := ctx.GetConnectionID(); id != 0 {
			return id
		}
	}
	for _, name := range []string{"connection_id", "session_id"} {
		switch value := session.GetParamByName(name).(type) {
		case uint32:
			return value
		case uint64:
			return uint32(value)
		case int:
			return uint32(value)
		case int64:
			return uint32(value)
		case string:
			if parsed, err := strconv.ParseUint(value, 10, 32); err == nil {
				return uint32(parsed)
			}
		}
	}
	return 0
}

func newExecutorErrorf(stage string, code ExecutionErrorCode, schema, table, sql string, err error, message string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	var executionErr *ExecutionError
	if errors.As(err, &executionErr) && executionErr != nil {
		return err
	}
	if strings.TrimSpace(message) == "" {
		return NewExecutionErrorWithCause("engine", stage, code, schema, table, sql, 0, err, "")
	}
	return NewExecutionErrorf("engine", stage, code, schema, table, sql, 0, err, message, args...)
}

const triggerAtomicJournalStateParam = "__xmysql_trigger_atomic_journal"

type triggerAtomicJournalState struct {
	rootOffset         int
	priorJournalActive bool
	frames             []int
}

func (e *XMySQLExecutor) beginAfterTriggerAtomicJournal(session server.MySQLServerSession) (func(bool), error) {
	if e == nil || session == nil {
		return func(bool) {}, nil
	}
	state := e.sessionTransactionState(session)
	journal, _ := session.GetParamByName(triggerAtomicJournalStateParam).(*triggerAtomicJournalState)
	if journal == nil {
		prior, _ := session.GetParamByName("transaction_journal_active").(bool)
		journal = &triggerAtomicJournalState{rootOffset: len(state.Changes), priorJournalActive: prior}
		session.SetParamByName(triggerAtomicJournalStateParam, journal)
		session.SetParamByName("transaction_journal_active", true)
	}
	frameOffset := len(state.Changes)
	journal.frames = append(journal.frames, frameOffset)
	return func(success bool) {
		if len(journal.frames) == 0 {
			return
		}
		journal.frames = journal.frames[:len(journal.frames)-1]
		if success {
			return
		}
		if err := e.rollbackSessionTransaction(session, frameOffset); err != nil {
			logger.Warnf("rollback AFTER trigger side effects failed: %v", err)
		}
	}, nil
}

func (e *XMySQLExecutor) finishAfterTriggerAtomicJournal(parent *ExecutionContext, session server.MySQLServerSession, success bool) {
	if e == nil || session == nil || (parent != nil && len(parent.triggerStack) > 0) {
		return
	}
	journal, _ := session.GetParamByName(triggerAtomicJournalStateParam).(*triggerAtomicJournalState)
	if journal == nil {
		return
	}
	state := e.sessionTransactionState(session)
	if !success {
		if err := e.rollbackSessionTransaction(session, journal.rootOffset); err != nil {
			logger.Warnf("rollback failed after AFTER trigger statement failure: %v", err)
		}
	} else if !journal.priorJournalActive && journal.rootOffset <= len(state.Changes) {
		state.Changes = state.Changes[:journal.rootOffset]
		session.SetParamByName("transaction_dml_state", state)
	}
	session.SetParamByName("transaction_journal_active", journal.priorJournalActive)
	session.SetParamByName(triggerAtomicJournalStateParam, nil)
}

func (e *XMySQLExecutor) executeAfterTriggerStatement(parent *ExecutionContext, session server.MySQLServerSession, schema, statement, sourceTable, triggerName string) error {
	if e == nil {
		return fmt.Errorf("nil executor for AFTER trigger")
	}
	startedAt := time.Now()
	frame := strings.ToLower(strings.TrimSpace(schema) + "." + strings.TrimSpace(sourceTable) + "." + strings.TrimSpace(triggerName))
	if strings.Trim(frame, ".") == "" {
		frame = strings.ToLower(strings.TrimSpace(schema) + "." + strings.TrimSpace(sourceTable))
	}
	if parent != nil {
		for _, active := range parent.triggerStack {
			if active == frame {
				return fmt.Errorf("trigger recursion detected for %s", frame)
			}
		}
		if len(parent.triggerStack) >= 64 {
			return fmt.Errorf("trigger nesting depth exceeded while executing %s", frame)
		}
	}
	results := make(chan *Result, 8)
	nested := &ExecutionContext{Context: context.Background(), Results: results, Cfg: e.conf, DatabaseName: schema, RawQuery: statement, Session: session}
	if parent != nil && parent.Context != nil {
		nested.Context = parent.Context
	}
	if parent != nil {
		nested.triggerStack = append(append([]string(nil), parent.triggerStack...), frame)
	}
	go e.executeQuery(nested, session, statement, schema, results)
	accounting := statementResultAccounting{}
	var triggerErr error
	for result := range results {
		if result == nil {
			continue
		}
		resultAccounting := statementResultAccountingFor(result)
		accounting.rowsAffected += resultAccounting.rowsAffected
		accounting.rowsSent += resultAccounting.rowsSent
		accounting.warnings += resultAccounting.warnings
		if result.Err != nil && triggerErr == nil {
			triggerErr = result.Err
		}
	}
	timerWait := time.Since(startedAt).Nanoseconds() * 1000
	if timerWait <= 0 {
		timerWait = 1000
	}
	e.recordPerformanceSchemaProgramExecution("TRIGGER", schema, triggerName, timerWait, 1, timerWait, timerWait, timerWait,
		boolToInt64(triggerErr != nil), accounting.warnings, accounting.rowsAffected, accounting.rowsSent,
	)
	return triggerErr
}

func normalizedTransactionCommand(query string) (cmd string, name string, ok bool) {
	q := strings.TrimSpace(query)
	q = strings.TrimSpace(strings.TrimRight(q, ";"))
	lower := strings.ToLower(q)
	switch {
	case lower == "begin" || lower == "begin work" || lower == "start transaction":
		return "begin", "", true
	case strings.HasPrefix(lower, "start transaction "):
		return normalizeStartTransactionOptions(strings.TrimSpace(lower[len("start transaction "):]))
	case lower == "commit" || strings.HasPrefix(lower, "commit work") || strings.HasPrefix(lower, "commit and") || strings.HasPrefix(lower, "commit release") || strings.HasPrefix(lower, "commit no release"):
		if option, ok := normalizedTransactionCompletionOptions(lower, "commit"); ok {
			return "commit", option, true
		}
		return "", "", false
	case lower == "rollback" || strings.HasPrefix(lower, "rollback work") || strings.HasPrefix(lower, "rollback and") || strings.HasPrefix(lower, "rollback release") || strings.HasPrefix(lower, "rollback no release"):
		if option, ok := normalizedTransactionCompletionOptions(lower, "rollback"); ok {
			return "rollback", option, true
		}
		return "", "", false
	case strings.HasPrefix(lower, "savepoint "):
		name := strings.TrimSpace(q[len("savepoint "):])
		return "savepoint", name, name != ""
	case strings.HasPrefix(lower, "rollback to savepoint "):
		name := strings.TrimSpace(q[len("rollback to savepoint "):])
		return "rollback_to_savepoint", name, name != ""
	case strings.HasPrefix(lower, "rollback to "):
		name := strings.TrimSpace(q[len("rollback to "):])
		return "rollback_to_savepoint", name, name != "" && !strings.EqualFold(name, "savepoint")
	case strings.HasPrefix(lower, "release savepoint "):
		name := strings.TrimSpace(q[len("release savepoint "):])
		return "release_savepoint", name, name != ""
	default:
		return "", "", false
	}
}

// normalizeStartTransactionOptions accepts MySQL's comma-separated
// transaction characteristics in either order. WITH CONSISTENT SNAPSHOT is
// independent of the access mode, while READ ONLY and READ WRITE are
// mutually exclusive and may each appear at most once.
func normalizeStartTransactionOptions(options string) (string, string, bool) {
	tokens := strings.Fields(strings.ReplaceAll(options, ",", " "))
	if len(tokens) == 0 {
		return "", "", false
	}
	consistentSnapshot := false
	accessMode := ""
	for index := 0; index < len(tokens); {
		switch tokens[index] {
		case "with":
			if index+2 >= len(tokens) || tokens[index+1] != "consistent" || tokens[index+2] != "snapshot" || consistentSnapshot {
				return "", "", false
			}
			consistentSnapshot = true
			index += 3
		case "read":
			if index+1 >= len(tokens) || (tokens[index+1] != "only" && tokens[index+1] != "write") || accessMode != "" {
				return "", "", false
			}
			if tokens[index+1] == "only" {
				accessMode = "READ ONLY"
			} else {
				accessMode = "READ WRITE"
			}
			index += 2
		default:
			return "", "", false
		}
	}
	switch accessMode {
	case "READ ONLY":
		return "begin_read_only", "", true
	case "READ WRITE":
		return "begin_read_write", "", true
	default:
		return "begin", "", true
	}
}

// normalizedTransactionCompletionOptions accepts the optional WORK, CHAIN,
// and RELEASE clauses shared by COMMIT and ROLLBACK. The option string is
// retained so execution can both start a chained transaction and request
// connection release after the completion has been acknowledged.
func normalizedTransactionCompletionOptions(query, verb string) (string, bool) {
	rest := strings.TrimSpace(strings.TrimPrefix(query, verb))
	if rest == "" {
		return "", true
	}
	if rest == "work" {
		return "", true
	}
	if strings.HasPrefix(rest, "work ") {
		rest = strings.TrimSpace(strings.TrimPrefix(rest, "work"))
	}
	if rest == "release" || rest == "no release" {
		return rest, true
	}
	if !strings.HasPrefix(rest, "and ") {
		return "", false
	}
	rest = strings.TrimSpace(strings.TrimPrefix(rest, "and "))
	chain := ""
	switch {
	case rest == "chain":
		chain = "chain"
		rest = ""
	case rest == "no chain":
		chain = "no chain"
		rest = ""
	case strings.HasPrefix(rest, "chain "):
		chain = "chain"
		rest = strings.TrimSpace(strings.TrimPrefix(rest, "chain"))
	case strings.HasPrefix(rest, "no chain "):
		chain = "no chain"
		rest = strings.TrimSpace(strings.TrimPrefix(rest, "no chain"))
	default:
		return "", false
	}
	if rest == "" {
		return chain, true
	}
	if rest != "release" && rest != "no release" {
		return "", false
	}
	return chain + " " + rest, true
}

func IsTransactionCommand(query string) bool {
	_, _, ok := normalizedTransactionCommand(query)
	return ok
}

func (e *XMySQLExecutor) executeTransactionCommand(ctx *ExecutionContext, cmd string, name string, session server.MySQLServerSession) {
	if session != nil {
		xaState := strings.ToUpper(strings.TrimSpace(fmt.Sprint(session.GetParamByName("xa_state"))))
		if xaState != "" && xaState != "<NIL>" {
			err := fmt.Errorf("XAER_PROTO: regular transaction command %s cannot be executed while an XA transaction is %s", strings.ToUpper(cmd), xaState)
			e.recordQueryError(ctx.DatabaseName, err)
			ctx.Results <- &Result{Err: err, ResultType: innodbcommon.RESULT_TYPE_ERROR, Message: err.Error()}
			return
		}
		switch cmd {
		case "begin", "begin_read_only", "begin_read_write":
			e.syncTransactionDefaults(session)
			wasInTransaction, _ := session.GetParamByName("in_transaction").(bool)
			pendingIsolation, _ := session.GetParamByName("next_transaction_isolation").(string)
			pendingReadOnly := session.GetParamByName("next_transaction_read_only")
			pendingReadOnlySet := pendingReadOnly != nil
			if wasInTransaction {
				// START TRANSACTION implicitly commits the previous transaction.
				// Do this before replacing its state so account changes,
				// replication statements, history, and active-transaction metrics
				// observe the same boundary as ordinary COMMIT.
				if err := e.commitSessionAccountChanges(session); err != nil {
					e.recordQueryError(ctx.DatabaseName, err)
					ctx.Results <- &Result{Err: err, ResultType: innodbcommon.RESULT_TYPE_ERROR, Message: err.Error()}
					return
				}
				if err := e.commitReplicationStatements(session); err != nil {
					e.recordQueryError(ctx.DatabaseName, err)
					ctx.Results <- &Result{Err: err, ResultType: innodbcommon.RESULT_TYPE_ERROR, Message: err.Error()}
					return
				}
				e.recordPerformanceSchemaTransactionHistory(session, "COMMITTED")
				e.recordActiveTransactionDelta(session, -1)
			}
			e.clearSessionTransactionState(session)
			if cmd == "begin_read_only" {
				session.SetParamByName("tx_read_only", int64(1))
				session.SetParamByName("transaction_read_only", int64(1))
			} else if cmd == "begin_read_write" {
				session.SetParamByName("tx_read_only", int64(0))
				session.SetParamByName("transaction_read_only", int64(0))
			}
			state := e.sessionTransactionState(session)
			state.AccessMode = transactionAccessMode(session)
			state.IsolationLevel = transactionIsolation(session)
			if strings.TrimSpace(pendingIsolation) != "" {
				state.IsolationLevel = pendingIsolation
			}
			if cmd == "begin" && pendingReadOnlySet {
				if sessionBoolValue(pendingReadOnly) {
					state.AccessMode = "READ ONLY"
				} else {
					state.AccessMode = "READ WRITE"
				}
			}
			session.SetParamByName("transaction_dml_state", state)
			session.SetParamByName("next_transaction_isolation", nil)
			session.SetParamByName("next_transaction_read_only", nil)
			session.SetParamByName("in_transaction", true)
			session.SetParamByName("transaction_journal_active", true)
			session.SessionContext().SetInTransaction(true)
			e.markPerformanceSchemaTransactionStart(session)
			e.recordActiveTransactionDelta(session, 1)
		case "commit", "rollback":
			wasInTransaction, _ := session.GetParamByName("in_transaction").(bool)
			completionOptions := strings.ToLower(strings.TrimSpace(name))
			chain := completionOptions == "chain" || strings.HasPrefix(completionOptions, "chain ")
			release := completionOptions == "release" || strings.HasSuffix(completionOptions, " release")
			chainedIsolation := transactionIsolationForMetadata(session)
			chainedAccessMode := transactionAccessModeForMetadata(session)
			if cmd == "rollback" {
				if err := e.rollbackSessionTransaction(session, 0); err != nil {
					e.recordQueryError(ctx.DatabaseName, err)
					ctx.Results <- &Result{Err: err, ResultType: innodbcommon.RESULT_TYPE_ERROR, Message: err.Error()}
					return
				}
				e.discardSessionAccountChanges(session)
			} else if err := e.commitSessionAccountChanges(session); err != nil {
				e.recordQueryError(ctx.DatabaseName, err)
				ctx.Results <- &Result{Err: err, ResultType: innodbcommon.RESULT_TYPE_ERROR, Message: err.Error()}
				return
			} else if cmd == "commit" {
				if err := e.commitReplicationStatements(session); err != nil {
					e.recordQueryError(ctx.DatabaseName, err)
					ctx.Results <- &Result{Err: err, ResultType: innodbcommon.RESULT_TYPE_ERROR, Message: err.Error()}
					return
				}
			}
			if wasInTransaction {
				state := "COMMITTED"
				if cmd == "rollback" {
					state = "ROLLED BACK"
				}
				e.recordPerformanceSchemaTransactionHistory(session, state)
			}
			e.clearSessionTransactionState(session)
			session.SetParamByName("transaction_journal_active", false)
			session.SetParamByName("in_transaction", false)
			session.SessionContext().SetInTransaction(false)
			if wasInTransaction && e.metricsRecorder != nil {
				e.recordActiveTransactionDelta(session, -1)
				if cmd == "commit" {
					e.metricsRecorder.RecordTransactionCommit(transactionIsolation(session))
				} else {
					e.metricsRecorder.RecordTransactionRollback(transactionIsolation(session), "explicit")
				}
			} else if wasInTransaction {
				e.recordActiveTransactionDelta(session, -1)
			}
			if chain {
				e.startChainedTransaction(session, chainedIsolation, chainedAccessMode)
			}
			if release {
				// The network handler observes this marker after sending the OK
				// packet and closes the connection, matching COMMIT/ROLLBACK
				// RELEASE without dropping the completion response.
				session.SetParamByName("should_close", true)
			}
		case "savepoint":
			if err := e.captureTransactionSavepoint(session, name); err != nil {
				ctx.Results <- &Result{Err: err, ResultType: innodbcommon.RESULT_TYPE_ERROR, Message: err.Error()}
				return
			}
		case "rollback_to_savepoint":
			if err := e.restoreTransactionSavepoint(session, name); err != nil {
				ctx.Results <- &Result{Err: err, ResultType: innodbcommon.RESULT_TYPE_ERROR, Message: err.Error()}
				return
			}
		case "release_savepoint":
			if err := e.releaseTransactionSavepoint(session, name); err != nil {
				ctx.Results <- &Result{Err: err, ResultType: innodbcommon.RESULT_TYPE_ERROR, Message: err.Error()}
				return
			}
		}
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: strings.ToUpper(cmd)}
}

// startChainedTransaction starts the transaction promised by COMMIT/ROLLBACK
// AND CHAIN. MySQL carries the just-finished transaction's characteristics
// into the new transaction instead of re-reading the session defaults.
func (e *XMySQLExecutor) startChainedTransaction(session server.MySQLServerSession, isolation, accessMode string) {
	if session == nil {
		return
	}
	isolation = strings.ToUpper(strings.TrimSpace(isolation))
	if isolation == "" {
		isolation = "REPEATABLE-READ"
	}
	if accessMode != "READ ONLY" {
		accessMode = "READ WRITE"
	}
	readOnly := int64(0)
	if accessMode == "READ ONLY" {
		readOnly = 1
	}
	session.SetParamByName("transaction_isolation", isolation)
	session.SetParamByName("tx_isolation", isolation)
	session.SetParamByName("tx_read_only", readOnly)
	session.SetParamByName("transaction_read_only", readOnly)
	session.SetParamByName("next_transaction_isolation", nil)
	session.SetParamByName("next_transaction_read_only", nil)
	state := e.sessionTransactionState(session)
	state.AccessMode = accessMode
	state.IsolationLevel = isolation
	session.SetParamByName("transaction_dml_state", state)
	session.SetParamByName("transaction_journal_active", true)
	session.SetParamByName("in_transaction", true)
	session.SessionContext().SetInTransaction(true)
	e.markPerformanceSchemaTransactionStart(session)
	e.recordActiveTransactionDelta(session, 1)
}

// beginImplicitTransactionIfNeeded materializes the transaction state that
// MySQL exposes after the first DML in an autocommit=0 session. The storage
// journal can already be active before that first statement, but clients and
// transaction metadata must see an active transaction as well.
func (e *XMySQLExecutor) beginImplicitTransactionIfNeeded(session server.MySQLServerSession) {
	if e == nil || session == nil || sessionBoolParam(session, "in_transaction") || !sessionTransactionActive(session) {
		return
	}
	e.syncTransactionDefaults(session)
	state := e.sessionTransactionState(session)
	state.AccessMode = transactionAccessMode(session)
	state.IsolationLevel = transactionIsolation(session)
	session.SetParamByName("transaction_dml_state", state)
	session.SetParamByName("transaction_journal_active", true)
	session.SetParamByName("in_transaction", true)
	session.SessionContext().SetInTransaction(true)
	e.markPerformanceSchemaTransactionStart(session)
	e.recordActiveTransactionDelta(session, 1)
}

// commitAutocommitTransaction applies the implicit COMMIT performed by
// SET autocommit=1 when an autocommit=0 transaction is active.
func (e *XMySQLExecutor) commitAutocommitTransaction(session server.MySQLServerSession) error {
	if e == nil || session == nil {
		return nil
	}
	if !sessionBoolParam(session, "in_transaction") {
		e.releaseSessionTransactionTableLocks(session)
		return nil
	}
	if err := e.commitSessionAccountChanges(session); err != nil {
		return err
	}
	if err := e.commitReplicationStatements(session); err != nil {
		return err
	}
	isolation := transactionIsolation(session)
	e.recordPerformanceSchemaTransactionHistory(session, "COMMITTED")
	e.clearSessionTransactionState(session)
	session.SetParamByName("transaction_journal_active", false)
	session.SetParamByName("in_transaction", false)
	session.SessionContext().SetInTransaction(false)
	e.recordActiveTransactionDelta(session, -1)
	if e.metricsRecorder != nil {
		e.metricsRecorder.RecordTransactionCommit(isolation)
	}
	return nil
}

func transactionIsolation(session server.MySQLServerSession) string {
	if session != nil {
		if value, ok := session.GetParamByName("transaction_isolation").(string); ok && strings.TrimSpace(value) != "" {
			return strings.ToUpper(value)
		}
		if value, ok := session.GetParamByName("tx_isolation").(string); ok && strings.TrimSpace(value) != "" {
			return strings.ToUpper(value)
		}
	}
	return "REPEATABLE-READ"
}

// syncTransactionDefaults materializes the manager's global transaction
// defaults for sessions that have not overridden them locally. The network
// handshake normally initializes these values, but direct engine users and
// newly-created sessions must observe SET GLOBAL TRANSACTION changes too.
func (e *XMySQLExecutor) syncTransactionDefaults(session server.MySQLServerSession) {
	if e == nil || session == nil || e.storageManager == nil {
		return
	}
	sysVars := e.storageManager.GetSystemVariablesManager()
	if sysVars == nil {
		return
	}
	sessionID := ""
	if value := session.GetParamByName("session_id"); value != nil {
		sessionID = strings.TrimSpace(fmt.Sprint(value))
	}
	isolation := session.GetParamByName("transaction_isolation")
	legacyIsolation := session.GetParamByName("tx_isolation")
	if isolation == nil && legacyIsolation == nil {
		if value, err := sysVars.GetVariable(sessionID, "transaction_isolation", manager.SessionScope); err == nil {
			if normalized, normalizeErr := normalizeTransactionIsolationValue(value); normalizeErr == nil {
				if strings.EqualFold(strings.TrimSpace(fmt.Sprint(value)), "REPEATABLE-READ") {
					normalized = "REPEATABLE-READ"
				}
				session.SetParamByName("transaction_isolation", normalized)
				session.SetParamByName("tx_isolation", normalized)
			}
		}
	} else if isolation == nil {
		session.SetParamByName("transaction_isolation", legacyIsolation)
	} else if legacyIsolation == nil {
		session.SetParamByName("tx_isolation", isolation)
	}
	readOnly := session.GetParamByName("transaction_read_only")
	legacyReadOnly := session.GetParamByName("tx_read_only")
	if readOnly == nil && legacyReadOnly == nil {
		if value, err := sysVars.GetVariable(sessionID, "transaction_read_only", manager.SessionScope); err == nil {
			readOnlyValue := int64(boolishToInt(value))
			session.SetParamByName("transaction_read_only", readOnlyValue)
			session.SetParamByName("tx_read_only", readOnlyValue)
		}
	} else if readOnly == nil {
		session.SetParamByName("transaction_read_only", legacyReadOnly)
	} else if legacyReadOnly == nil {
		session.SetParamByName("tx_read_only", readOnly)
	}
}

func transactionContextForSession(ctx context.Context, session server.MySQLServerSession) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if session == nil {
		return ctx
	}
	isolation := transactionIsolation(session)
	inTransaction := sessionBoolParam(session, "in_transaction")
	readOnly := false
	if inTransaction {
		readOnly = sessionBoolParam(session, "tx_read_only") || sessionBoolParam(session, "transaction_read_only")
		if raw := session.GetParamByName("transaction_dml_state"); raw != nil {
			if state, ok := raw.(*sessionTransactionState); ok && state != nil {
				if strings.TrimSpace(state.IsolationLevel) != "" {
					isolation = state.IsolationLevel
				}
				if state.AccessMode != "" {
					readOnly = strings.EqualFold(state.AccessMode, "READ ONLY")
				}
			}
		}
	} else {
		if pending, ok := session.GetParamByName("next_transaction_isolation").(string); ok && strings.TrimSpace(pending) != "" {
			isolation = pending
		}
		if pending := session.GetParamByName("next_transaction_read_only"); pending != nil {
			readOnly = sessionBoolValue(pending)
		}
	}
	isolation = strings.ToUpper(strings.ReplaceAll(isolation, "_", "-"))
	var isolationLevel uint8
	switch strings.ReplaceAll(isolation, "-", " ") {
	case "READ UNCOMMITTED":
		isolationLevel = manager.TRX_ISO_READ_UNCOMMITTED
	case "READ COMMITTED":
		isolationLevel = manager.TRX_ISO_READ_COMMITTED
	case "SERIALIZABLE":
		isolationLevel = manager.TRX_ISO_SERIALIZABLE
	default:
		isolationLevel = manager.TRX_ISO_REPEATABLE_READ
	}
	ctx = context.WithValue(ctx, "isolation_level", isolationLevel)
	return context.WithValue(ctx, "read_only", readOnly)
}

func (e *XMySQLExecutor) recordActiveTransactionDelta(session server.MySQLServerSession, delta int64) {
	if e == nil || e.metricsRecorder == nil {
		return
	}
	count := e.activeTransactions.Add(delta)
	if count < 0 {
		e.activeTransactions.Store(0)
		count = 0
	}
	e.metricsRecorder.SetActiveTransactions(transactionIsolation(session), int(count))
}

func (e *XMySQLExecutor) recordQueryError(database string, err error) {
	if e == nil || e.metricsRecorder == nil || err == nil {
		return
	}
	code := string(ExecutionErrorCodeUnknown)
	var executionErr *ExecutionError
	if errors.As(err, &executionErr) && executionErr != nil && executionErr.ErrorCode != "" {
		code = string(executionErr.ErrorCode)
	}
	e.metricsRecorder.RecordQueryError(database, "execution", code)
}

func (e *XMySQLExecutor) recordQueryErrorForSession(session server.MySQLServerSession, database string, err error) {
	if e == nil || e.metricsRecorder == nil || err == nil {
		return
	}
	code := string(ExecutionErrorCodeUnknown)
	var executionErr *ExecutionError
	if errors.As(err, &executionErr) && executionErr != nil && executionErr.ErrorCode != "" {
		code = string(executionErr.ErrorCode)
	}
	threadID := int64(0)
	user, host := "", ""
	if session != nil {
		threadID = int64(sessionConnectionID(session))
		user, _ = session.GetParamByName("user").(string)
		host, _ = session.GetParamByName("host").(string)
	}
	e.metricsRecorder.RecordQueryErrorWithIdentity(database, "execution", code, threadID, user, host)
}

func closeOwnedBTreeManager(btree basic.BPlusTreeManager) {
	if closer, ok := btree.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
}

func (e *XMySQLExecutor) prepareTransactionalDML(session server.MySQLServerSession) error {
	return nil
}

func (e *XMySQLExecutor) sessionTransactionState(session server.MySQLServerSession) *sessionTransactionState {
	if session == nil {
		return nil
	}
	if raw := session.GetParamByName("transaction_dml_state"); raw != nil {
		if state, ok := raw.(*sessionTransactionState); ok {
			return state
		}
	}
	state := &sessionTransactionState{}
	session.SetParamByName("transaction_dml_state", state)
	return state
}

func sessionTransactionActive(session server.MySQLServerSession) bool {
	if session == nil {
		return false
	}
	if inTxn, ok := session.GetParamByName("in_transaction").(bool); ok && inTxn {
		return true
	}
	switch v := session.GetParamByName("autocommit").(type) {
	case string:
		return boolishToInt(v) == 0
	case int64:
		return v == 0
	case int:
		return v == 0
	case bool:
		return !v
	default:
		return false
	}
}

func (e *XMySQLExecutor) captureTransactionSavepoint(session server.MySQLServerSession, name string) error {
	if session == nil {
		return nil
	}
	state := e.sessionTransactionState(session)
	points := state.Savepoints[:0]
	for _, point := range state.Savepoints {
		if strings.EqualFold(point.name, name) {
			continue
		}
		points = append(points, point)
	}
	state.Savepoints = append(points, sessionSavepoint{name: name, offset: len(state.Changes)})
	session.SetParamByName("transaction_dml_state", state)
	session.SetParamByName("savepoints", transactionSavepointNames(state.Savepoints))
	session.SetParamByName("performance_schema_savepoint_count", int64Param(session.GetParamByName("performance_schema_savepoint_count"))+1)
	return nil
}

func (e *XMySQLExecutor) restoreTransactionSavepoint(session server.MySQLServerSession, name string) error {
	state := e.sessionTransactionState(session)
	if state == nil {
		return fmt.Errorf("savepoint %s does not exist", name)
	}
	for i := len(state.Savepoints) - 1; i >= 0; i-- {
		point := state.Savepoints[i]
		if !strings.EqualFold(point.name, name) {
			continue
		}
		if err := e.rollbackSessionTransaction(session, point.offset); err != nil {
			return err
		}
		state.Savepoints = state.Savepoints[:i+1]
		session.SetParamByName("transaction_dml_state", state)
		session.SetParamByName("savepoints", transactionSavepointNames(state.Savepoints))
		session.SetParamByName("performance_schema_rollback_to_savepoint_count", int64Param(session.GetParamByName("performance_schema_rollback_to_savepoint_count"))+1)
		return nil
	}
	return fmt.Errorf("savepoint %s does not exist", name)
}

func (e *XMySQLExecutor) releaseTransactionSavepoint(session server.MySQLServerSession, name string) error {
	state := e.sessionTransactionState(session)
	if state == nil {
		return nil
	}
	previousCount := len(state.Savepoints)
	next := state.Savepoints[:0]
	for _, point := range state.Savepoints {
		if strings.EqualFold(point.name, name) {
			continue
		}
		next = append(next, point)
	}
	state.Savepoints = next
	session.SetParamByName("transaction_dml_state", state)
	session.SetParamByName("savepoints", transactionSavepointNames(state.Savepoints))
	if len(next) != previousCount {
		session.SetParamByName("performance_schema_release_savepoint_count", int64Param(session.GetParamByName("performance_schema_release_savepoint_count"))+1)
	}
	return nil
}

func (e *XMySQLExecutor) clearSessionTransactionState(session server.MySQLServerSession) {
	e.releaseSessionTransactionTableLocks(session)
	state := e.sessionTransactionState(session)
	if state == nil {
		return
	}
	state.Changes = nil
	state.Statements = nil
	state.Savepoints = nil
	state.AccessMode = ""
	state.IsolationLevel = ""
	session.SetParamByName("transaction_dml_state", state)
	session.SetParamByName("savepoints", []string{})
	session.SetParamByName("performance_schema_savepoint_count", int64(0))
	session.SetParamByName("performance_schema_rollback_to_savepoint_count", int64(0))
	session.SetParamByName("performance_schema_release_savepoint_count", int64(0))
	e.clearTransactionJournal(session)
}

// ResetSession restores the session state required by COM_RESET_CONNECTION.
// The reset is a rollback boundary: any uncommitted DML is undone before the
// transaction journal and session-local variables are discarded.
func (e *XMySQLExecutor) ResetSession(session server.MySQLServerSession) error {
	if session == nil {
		return nil
	}
	if strings.EqualFold(strings.TrimSpace(fmt.Sprint(session.GetParamByName("xa_state"))), "PREPARED") {
		return fmt.Errorf("cannot reset a session with a prepared XA transaction")
	}
	e.releaseSessionTableLocks(session)
	if err := e.CleanupTemporaryTables(session); err != nil {
		return err
	}
	wasInTransaction, _ := session.GetParamByName("in_transaction").(bool)
	if err := e.rollbackSessionTransaction(session, 0); err != nil {
		return err
	}
	e.discardSessionAccountChanges(session)
	e.clearSessionTransactionState(session)
	session.SetParamByName("autocommit", "1")
	session.SetParamByName("in_transaction", false)
	session.SetParamByName("transaction_journal_active", false)
	session.SetParamByName("database", "")
	session.SetParamByName("last_insert_id", uint64(0))
	session.SetParamByName("row_count", int64(0))
	session.SetParamByName("locked_tables", map[string]string{})
	session.SetParamByName("warnings", []Warning{})
	session.SetParamByName("user_variables", map[string]interface{}{})
	session.SetParamByName("session_variables", map[string]interface{}{})
	session.SetParamByName("handler_cursors", map[string]*handlerCursorState{})
	session.SessionContext().SetInTransaction(false)
	if wasInTransaction {
		e.recordActiveTransactionDelta(session, -1)
	}
	return nil
}

func transactionSavepointNames(points []sessionSavepoint) []string {
	names := make([]string, 0, len(points))
	for _, point := range points {
		names = append(names, point.name)
	}
	return names
}

func (e *XMySQLExecutor) recordTransactionDMLChanges(session server.MySQLServerSession, changes []transactionDMLChange) {
	if session == nil || len(changes) == 0 {
		return
	}
	e.beginImplicitTransactionIfNeeded(session)
	journalActive, _ := session.GetParamByName("transaction_journal_active").(bool)
	replaying, _ := session.GetParamByName("replication_replay").(bool)
	if !sessionTransactionActive(session) && !journalActive && e.replicationCommitHook == nil && e.replicationCommitTransactionHook == nil && e.replicationCommitTransactionHookWithID == nil {
		return
	}
	state := e.sessionTransactionState(session)
	state.Changes = append(state.Changes, changes...)
	if !replaying {
		if statement, ok := session.GetParamByName("replication_current_statement").(replication.Statement); ok && strings.TrimSpace(statement.SQL) != "" {
			e.recordReplicationStatement(session, statement)
			state = e.sessionTransactionState(session)
		}
	}
	session.SetParamByName("transaction_dml_state", state)
	if err := e.persistTransactionJournal(session, changes); err != nil {
		logger.Warnf("persist transaction journal failed: %v", err)
	}
	if !sessionTransactionActive(session) && !journalActive && !replaying {
		if err := e.commitReplicationStatements(session); err != nil {
			logger.Errorf("replication append failed after autocommit DML: %v", err)
		}
	}
}

func (e *XMySQLExecutor) commitReplicationStatements(session server.MySQLServerSession) error {
	return e.commitReplicationStatementsWithID(session, "")
}

func (e *XMySQLExecutor) commitReplicationStatementsWithID(session server.MySQLServerSession, transactionID string) error {
	if e == nil || (e.replicationCommitHook == nil && e.replicationCommitTransactionHook == nil && e.replicationCommitTransactionHookWithID == nil) || session == nil {
		return nil
	}
	replaying, _ := session.GetParamByName("replication_replay").(bool)
	if replaying {
		return nil
	}
	state := e.sessionTransactionState(session)
	if state == nil || len(state.Statements) == 0 {
		return nil
	}
	statements := append([]replication.Statement(nil), state.Statements...)
	if strings.TrimSpace(transactionID) != "" && e.replicationCommitTransactionHookWithID != nil {
		if err := e.replicationCommitTransactionHookWithID(transactionID, replicationRowsFromTransactionChanges(state.Changes), statements); err != nil {
			return err
		}
	} else if e.replicationCommitTransactionHook != nil {
		if err := e.replicationCommitTransactionHook(replicationRowsFromTransactionChanges(state.Changes), statements); err != nil {
			return err
		}
	} else if e.replicationCommitHook != nil {
		if err := e.replicationCommitHook(statements); err != nil {
			return err
		}
	}
	state.Statements = nil
	session.SetParamByName("transaction_dml_state", state)
	return nil
}

func (e *XMySQLExecutor) rollbackSessionTransaction(session server.MySQLServerSession, offset int) error {
	state := e.sessionTransactionState(session)
	if state == nil || offset < 0 || offset > len(state.Changes) {
		return fmt.Errorf("invalid transaction rollback offset %d", offset)
	}
	if len(state.Changes) == offset {
		return nil
	}
	dml, err := e.newStorageIntegratedDMLExecutor()
	if err != nil {
		return err
	}
	for i := len(state.Changes) - 1; i >= offset; i-- {
		if err := rollbackDMLChange(dml, state.Changes[i]); err != nil {
			return err
		}
	}
	state.Changes = state.Changes[:offset]
	session.SetParamByName("transaction_dml_state", state)
	return nil
}

func (e *XMySQLExecutor) newStorageIntegratedDMLExecutor() (*StorageIntegratedDMLExecutor, error) {
	txManager, err := e.getTransactionManager()
	if err != nil {
		return nil, err
	}
	var optimizerManager *manager.OptimizerManager
	var bufferPoolManager *manager.OptimizedBufferPoolManager
	var btreeManager basic.BPlusTreeManager
	var tableManager *manager.TableManager
	if managerValue, ok := e.optimizerManager.(*manager.OptimizerManager); ok {
		optimizerManager = managerValue
	}
	if managerValue, ok := e.bufferPoolManager.(*manager.OptimizedBufferPoolManager); ok {
		bufferPoolManager = managerValue
	}
	if managerValue, ok := e.btreeManager.(basic.BPlusTreeManager); ok {
		btreeManager = managerValue
	}
	if managerValue, ok := e.tableManager.(*manager.TableManager); ok {
		tableManager = managerValue
	}
	dml := NewStorageIntegratedDMLExecutor(
		optimizerManager,
		bufferPoolManager,
		btreeManager,
		tableManager,
		txManager,
		e.indexManager,
		e.storageManager,
		e.tableStorageManager,
	)
	dml.SetDataDir(e.getDataDir())
	return dml, nil
}

func (e *XMySQLExecutor) missingStorageIntegratedDMLManagersError(
	stage string,
	schema string,
	table string,
	indexManager *manager.IndexManager,
	storageManager *manager.StorageManager,
	tableStorageManager *manager.TableStorageManager,
) error {
	missing := make([]string, 0, 3)
	if indexManager == nil {
		missing = append(missing, "indexManager")
	}
	if storageManager == nil {
		missing = append(missing, "storageManager")
	}
	if tableStorageManager == nil {
		missing = append(missing, "tableStorageManager")
	}
	err := fmt.Errorf("storage-integrated DML requires managers: missing %s", strings.Join(missing, ", "))
	return NewExecutionErrorWithCause(
		"engine",
		stage,
		ExecutionErrorCodeStorageMissing,
		schema,
		table,
		"",
		0,
		err,
		"storage-integrated DML requires managers",
	)
}

// SetManagers 设置管理器组件
func (e *XMySQLExecutor) SetManagers(
	optimizerManager interface{},
	bufferPoolManager interface{},
	btreeManager interface{},
	tableManager interface{},
) {
	e.optimizerManager = optimizerManager
	e.bufferPoolManager = bufferPoolManager
	e.btreeManager = btreeManager
	e.tableManager = tableManager
}

// SetReplicationCommitHook receives successfully executed, committed logical
// DML/DDL statements. The hook is intentionally injected from the engine so
// the executor remains usable without clustering.
func (e *XMySQLExecutor) SetReplicationCommitHook(hook func([]replication.Statement) error) {
	if e != nil {
		e.replicationCommitHook = hook
	}
}

// SetReplicationCommitTransactionHook receives one committed transaction's
// row images and statements together, allowing native row-event consumers and
// statement-based replicas to share one GTID boundary.
func (e *XMySQLExecutor) SetReplicationCommitTransactionHook(hook func([]replication.RowChange, []replication.Statement) error) {
	if e != nil {
		e.replicationCommitTransactionHook = hook
	}
}

// SetReplicationCommitTransactionHookWithID is used by XA-aware replication
// coordinators. The transaction ID remains stable when the client retries a
// commit after the downstream append has already succeeded.
func (e *XMySQLExecutor) SetReplicationCommitTransactionHookWithID(hook func(string, []replication.RowChange, []replication.Statement) error) {
	if e != nil {
		e.replicationCommitTransactionHookWithID = hook
	}
}

func replicationRowsFromTransactionChanges(changes []transactionDMLChange) []replication.RowChange {
	if len(changes) == 0 {
		return nil
	}
	rows := make([]replication.RowChange, 0, len(changes))
	for _, change := range changes {
		columns := make(map[string]struct{}, len(change.before)+len(change.after))
		for column := range change.before {
			columns[column] = struct{}{}
		}
		for column := range change.after {
			columns[column] = struct{}{}
		}
		orderedColumns := make([]string, 0, len(columns))
		for column := range columns {
			orderedColumns = append(orderedColumns, column)
		}
		sort.Strings(orderedColumns)
		rows = append(rows, replication.RowChange{
			Table:       change.tableName,
			Action:      change.kind,
			Columns:     orderedColumns,
			ColumnTypes: cloneStringMap(change.columnTypes),
			Before:      cloneTransactionRow(change.before),
			After:       cloneTransactionRow(change.after),
		})
	}
	return rows
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func (e *XMySQLExecutor) recordReplicationStatement(session server.MySQLServerSession, statement replication.Statement) {
	if e == nil || session == nil || (e.replicationCommitHook == nil && e.replicationCommitTransactionHook == nil && e.replicationCommitTransactionHookWithID == nil) || strings.TrimSpace(statement.SQL) == "" {
		return
	}
	if replaying, _ := session.GetParamByName("replication_replay").(bool); replaying {
		return
	}
	state := e.sessionTransactionState(session)
	for _, existing := range state.Statements {
		if existing.Database == statement.Database && existing.SQL == statement.SQL {
			return
		}
	}
	state.Statements = append(state.Statements, statement)
	session.SetParamByName("transaction_dml_state", state)
	journalActive, _ := session.GetParamByName("transaction_journal_active").(bool)
	if !sessionTransactionActive(session) && !journalActive {
		if err := e.commitReplicationStatements(session); err != nil {
			logger.Errorf("replication append failed after autocommit statement: %v", err)
		}
	}
}

// ExecuteWithQuery 接收原始 SQL 查询，异步执行并返回结果通道
func (e *XMySQLExecutor) ExecuteWithQuery(mysqlSession server.MySQLServerSession, query string, databaseName string) <-chan *Result {
	results := make(chan *Result)
	workerResults := make(chan *Result)
	queryContext, cleanup := e.beginActiveQuery(mysqlSession)
	executionContext := &ExecutionContext{
		Context:     queryContext,
		statementId: 0,
		QueryId:     0,
		Results:     workerResults,
		Cfg:         nil,
		Session:     mysqlSession,
	}
	// Keep the legacy field populated for callers that inspect it, but capture
	// a query-local context in the worker so concurrent queries cannot execute
	// against whichever context was assigned most recently.
	e.ctx = executionContext
	go func() {
		defer cleanup()
		if mysqlSession != nil {
			previousProcesslistQuery := mysqlSession.GetParamByName("processlist_query")
			previousProcesslistStart := mysqlSession.GetParamByName("processlist_start_time")
			mysqlSession.SetParamByName("processlist_query", query)
			mysqlSession.SetParamByName("processlist_start_time", time.Now())
			defer mysqlSession.SetParamByName("processlist_query", previousProcesslistQuery)
			defer mysqlSession.SetParamByName("processlist_start_time", previousProcesslistStart)
		}
		e.executeQuery(executionContext, mysqlSession, query, databaseName, workerResults)
	}()
	go func() {
		defer close(results)
		for result := range workerResults {
			if result != nil && result.Err != nil {
				recordSessionError(mysqlSession, result.Err)
			}
			executionContext.recordStatementResult(result)
			results <- result
		}
	}()
	return results
}

// executeQuery 是实际的 SQL 执行过程，包括解析和语义分派
func (e *XMySQLExecutor) executeQuery(ctx *ExecutionContext, mysqlSession server.MySQLServerSession, query string, databaseName string, results chan *Result) {
	query = rewriteCharsetIntroducers(query)
	// Publish the result channel only after all query lifecycle defers have
	// completed. Callers that range until channel close must be able to observe
	// the matching metrics and statement-history records immediately afterward.
	defer close(results)
	defer e.beginActiveStatement(mysqlSession, query, databaseName)()
	if ctx != nil && ctx.Session == nil {
		ctx.Session = mysqlSession
	}
	startedAt := time.Now()
	metricStatus := "success"
	if e.metricsRecorder != nil {
		threadID := int64(0)
		if mysqlSession != nil {
			threadID = int64(sessionConnectionID(mysqlSession))
		}
		e.metricsRecorder.RecordMemoryAllocation(threadID, "memory/sql/THD::main_mem_root", int64(len(query)))
		defer e.metricsRecorder.RecordMemoryFree(threadID, "memory/sql/THD::main_mem_root", int64(len(query)))
		defer func() {
			latency := time.Since(startedAt)
			e.metricsRecorder.RecordQuery(databaseName, metricStatementType(query), metricStatus, latency)
			user, host := "", ""
			if mysqlSession != nil {
				user, _ = mysqlSession.GetParamByName("user").(string)
				host, _ = mysqlSession.GetParamByName("host").(string)
			}
			actorSetting := e.performanceSchemaStatementSettingForSession(mysqlSession)
			e.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScan(
				threadID, user, host, databaseName, strings.TrimSpace(query), metricStatementType(query), metricStatus, latency,
				ctx.statementRowsAffected.Load(), ctx.statementRowsSent.Load(), ctx.statementRowsExamined.Load(), ctx.statementSelectScan.Load(), ctx.statementWarnings.Load(), actorSetting.Enabled, actorSetting.History,
			)
			if metricStatus == "error" {
				e.metricsRecorder.RecordQueryError(databaseName, "execution", string(ExecutionErrorCodeUnknown))
			}
		}()
	}
	defer func() {
		if mysqlSession == nil {
			return
		}
		journalActive, _ := mysqlSession.GetParamByName("transaction_journal_active").(bool)
		if !sessionTransactionActive(mysqlSession) && !journalActive {
			e.clearTransactionJournal(mysqlSession)
		}
	}()
	if ctx != nil {
		ctx.DatabaseName = databaseName
		ctx.RawQuery = query
		ctx.Session = mysqlSession
	}
	if effectiveSession, viewSecurityRequired, err := e.applyViewExecutionSecurity(mysqlSession, query, databaseName); err != nil {
		results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		return
	} else if effectiveSession != mysqlSession {
		mysqlSession = effectiveSession
		ctx.Session = effectiveSession
		ctx.ViewSecurityRequired = viewSecurityRequired
	} else {
		ctx.ViewSecurityRequired = viewSecurityRequired
	}
	if rewritten, err := rewriteJSONValueReturningQuery(query); err != nil {
		results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		return
	} else if rewritten != query {
		query = rewritten
		if ctx != nil {
			ctx.RawQuery = rewritten
		}
	}
	if handled, err := e.executeRawJSONValueCompatibility(ctx, query, databaseName); handled {
		if err != nil {
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		}
		return
	}
	if handled, err := e.executeRawTemporaryTableCompatibility(ctx, query, databaseName); handled {
		if err != nil {
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: err.Error()}
		}
		return
	}
	if handled, err := e.executeTablespaceCompatibility(ctx, query); handled {
		if err != nil {
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: err.Error()}
		}
		return
	}
	if handled, err := e.executeHandlerCompatibility(ctx, mysqlSession, query, databaseName, results); handled {
		if err != nil {
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		}
		return
	}
	if rewritten := e.rewriteTemporaryTableReferences(mysqlSession, query); rewritten != query {
		query = rewritten
		ctx.RawQuery = rewritten
	}
	if rewritten, err := rewriteExtractDateFunctions(query); err != nil {
		metricStatus = "error"
		results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		return
	} else if rewritten != query {
		query = rewritten
		ctx.RawQuery = rewritten
	}
	if rewritten, err := e.rewriteNestedCTESubqueries(query); err != nil {
		metricStatus = "error"
		results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		return
	} else if rewritten != "" {
		query = rewritten
		ctx.RawQuery = rewritten
	}
	if unlock, err := e.acquireStatementTableLocks(ctx, mysqlSession, query, databaseName); err != nil {
		metricStatus = "error"
		results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		return
	} else {
		defer unlock()
	}
	globalUnlock, globalErr := e.acquireGlobalReadLockForStatement(ctx.Context, mysqlSession, query)
	if globalErr != nil {
		metricStatus = "error"
		results <- &Result{Err: globalErr, ResultType: common.RESULT_TYPE_QUERY, Message: globalErr.Error()}
		return
	}
	defer globalUnlock()
	if err := validateCTEQuerySyntax(query); err != nil {
		metricStatus = "error"
		results <- &Result{Err: newExecutorErrorf("cte-parse", ExecutionErrorCodeValidation, databaseName, "", query, err, "invalid CTE statement"), ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		return
	}
	if shouldClearSessionWarnings(query) {
		clearSessionWarnings(mysqlSession)
	}
	if e.executeUserVariableAssignment(ctx, query, mysqlSession) {
		return
	}

	if cmd, name, ok := normalizedTransactionCommand(query); ok {
		e.executeTransactionCommand(ctx, cmd, name, mysqlSession)
		return
	}
	if handled, err := e.executeXACompatibility(ctx, mysqlSession, query); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		} else if !isXARecoverQuery(query) {
			results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "XA statement executed successfully"}
		}
		return
	}
	if handled, err := e.executeAdminCompatibility(ctx, mysqlSession, query); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
			return
		}
		if ctx.AdminResult != nil {
			results <- ctx.AdminResult
			return
		}
		results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "statement executed successfully"}
		return
	}

	if err := rejectUnsupportedCreateTableConstraintsSQL(query); err != nil {
		metricStatus = "error"
		results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_QUERY,
			Message:    err.Error(),
		}
		return
	}
	if handled, err := e.executeShowCreateDatabaseCompatibility(ctx, query); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		}
		return
	}

	if isShowFullTablesQuery(query) {
		e.executeShowFullTablesRaw(ctx, mysqlSession, query, databaseName)
		return
	}
	if handled := e.executeAccountStatement(ctx, query); handled {
		return
	}
	if handled, err := e.executeRawCreateTableLikeCompatibility(ctx, query, databaseName); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: err.Error()}
		}
		return
	}
	if handled := e.executeStoredObjectCall(ctx, query, databaseName); handled {
		return
	}
	if handled := e.executeStoredFunctionCall(ctx, query, databaseName); handled {
		return
	}
	if handled := e.executeShowCreateStoredObject(ctx, query, databaseName); handled {
		return
	}
	if handled := e.executeStoredObjectAlter(ctx, query, databaseName); handled {
		return
	}
	if handled := e.executeStoredObjectDDL(ctx, query, databaseName); handled {
		return
	}
	if handled, err := e.executeRawCreateTableIndexVisibility(ctx, query, databaseName); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: err.Error()}
		}
		return
	}
	if handled, err := e.executeRawFullTextSpatialCreate(ctx, query, databaseName); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: err.Error()}
		}
		return
	}
	if handled, err := e.executeRawStandaloneIndexCompatibility(ctx, query, databaseName); handled {
		if err != nil {
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: err.Error()}
		} else {
			results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: "index executed successfully"}
		}
		return
	}
	if handled, err := e.executeRawCreatePartitionCompatibility(ctx, query, databaseName); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: err.Error()}
		}
		return
	}
	if handled, err := e.executeRawPartitionMaintenance(ctx, query, databaseName); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: err.Error()}
		}
		return
	}
	if handled, err := e.executeRawTableTablespaceCompatibility(ctx, query, databaseName); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: err.Error()}
		}
		return
	}
	if handled, err := e.executeFullTextQuery(ctx, query, databaseName); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		}
		return
	}
	if handled, err := e.executeSpatialQuery(ctx, query, databaseName); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		}
		return
	}
	if handled, err := e.executeRawAlterCompatibility(ctx, query, databaseName); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: err.Error()}
			return
		}
		results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: "ALTER TABLE executed successfully"}
		return
	}
	if handled, dropped, err := e.executeRawDropTableCompatibility(ctx, query, databaseName, mysqlSession); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: err.Error()}
			return
		}
		results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("%d table(s) dropped successfully", dropped)}
		return
	}
	if handled, err := e.executeGeneralWindowQuery(ctx, query, databaseName); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		}
		return
	}
	if handled, err := e.executeAdvancedWindowQuery(ctx, query, databaseName); handled {
		if err != nil {
			metricStatus = "error"
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		}
		return
	}
	if handled, err := e.executeCTECompatibility(ctx, query, databaseName); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		}
		return
	}
	if handled, err := e.executeCorrelatedScalarSubqueryCompatibility(ctx, query, databaseName); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		}
		return
	}
	if handled, err := e.executeCorrelatedPredicateSubqueryCompatibility(ctx, query, databaseName); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		}
		return
	}
	if handled, err := e.executeCorrelatedDerivedOuterCompatibility(ctx, query, databaseName); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		}
		return
	}
	if handled, err := e.executeCorrelatedSubqueryCompatibility(ctx, query, databaseName); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		}
		return
	}
	if rewritten, handled, err := e.rewriteCorrelatedDMLSubquery(ctx, query, databaseName); handled {
		if err != nil {
			metricStatus = "error"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
			return
		}
		query = rewritten
		ctx.RawQuery = rewritten
	}
	if handled, err := e.executeSimpleWindowQuery(ctx, query, databaseName); handled {
		if err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		}
		return
	}
	if handled := e.executeViewDDL(ctx, query, databaseName); handled {
		return
	}
	if handled := e.executeShowCreateView(ctx, query, databaseName); handled {
		return
	}
	if rewritten, err := e.rewriteSimpleCTEQuery(query); err != nil {
		results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return
	} else if rewritten != "" {
		query = rewritten
		ctx.RawQuery = rewritten
	}
	if rewritten, err := e.rewriteViewQuery(query, databaseName); err != nil {
		results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return
	} else if rewritten != "" {
		query = rewritten
		ctx.RawQuery = rewritten
	}

	if handled := e.executeAdminReadQuery(ctx, query, databaseName); handled {
		return
	}
	if result, handled, err := e.executePerformanceSchemaSetupUpdate(query); handled {
		if err != nil {
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
			return
		}
		results <- result
		return
	}

	if selectResult, handled, err := e.executeInformationSchemaMetadataSelect(query, mysqlSession); handled {
		if err != nil {
			results <- &Result{
				Err:        newExecutorErrorf("execute-metadata-select", ExecutionErrorCodeUnknown, databaseName, "", query, err, "execute metadata SELECT failed"),
				ResultType: common.RESULT_TYPE_QUERY,
				Message:    "metadata SELECT query failed",
			}
			return
		}
		selectResult = normalizeInformationSchemaAggregate(query, selectResult)
		results <- &Result{
			ResultType: common.RESULT_TYPE_QUERY,
			Data:       selectResult,
			Message:    fmt.Sprintf("SELECT query executed successfully, %d rows returned", selectResult.RowCount),
		}
		return
	}
	if handled, err := e.executeDerivedTableCompatibility(ctx, query, databaseName); handled {
		if err != nil {
			results <- &Result{Err: newExecutorErrorf("execute-derived-table", ExecutionErrorCodeUnknown, databaseName, "", query, err, "execute derived table failed"), ResultType: common.RESULT_TYPE_QUERY, Message: "derived table query failed"}
		}
		return
	}
	if !isInsertOrReplaceQuery(query) {
		if branches, operators, ok := splitSetOperationQuery(query); ok && hasNonUnionSetOperator(operators) {
			selectResult, err := e.executeMixedSetOperationQuery(ctx, branches, operators, databaseName)
			if err != nil {
				results <- &Result{Err: newExecutorErrorf("execute-set-operation", ExecutionErrorCodeUnknown, databaseName, "", query, err, "execute set operation failed"), ResultType: common.RESULT_TYPE_QUERY, Message: "set operation failed"}
				return
			}
			results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: selectResult, Message: fmt.Sprintf("set operation executed successfully, %d rows returned", selectResult.RowCount)}
			return
		}

		if branches, unionAll, ok := splitUnionQuery(query); ok {
			selectResult, err := e.executeUnionQuery(ctx, branches, unionAll, databaseName)
			if err != nil {
				results <- &Result{Err: newExecutorErrorf("execute-union", ExecutionErrorCodeUnknown, databaseName, "", query, err, "execute UNION failed"), ResultType: common.RESULT_TYPE_QUERY, Message: "UNION query failed"}
				return
			}
			results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: selectResult, Message: fmt.Sprintf("UNION query executed successfully, %d rows returned", selectResult.RowCount)}
			return
		}
		if branches, operators, ok := splitIntersectExceptQuery(query); ok {
			selectResult, err := e.executeIntersectExceptQuery(ctx, branches, operators, databaseName)
			if err != nil {
				results <- &Result{Err: newExecutorErrorf("execute-set-operation", ExecutionErrorCodeUnknown, databaseName, "", query, err, "execute set operation failed"), ResultType: common.RESULT_TYPE_QUERY, Message: "set operation failed"}
				return
			}
			results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: selectResult, Message: fmt.Sprintf("set operation executed successfully, %d rows returned", selectResult.RowCount)}
			return
		}
	}
	if rewritten, err := e.rewriteSimpleInSubquery(ctx, query, databaseName); err != nil {
		results <- &Result{Err: newExecutorErrorf("execute-subquery", ExecutionErrorCodeUnknown, databaseName, "", query, err, "execute subquery failed"), ResultType: common.RESULT_TYPE_QUERY, Message: "subquery failed"}
		return
	} else if rewritten != "" {
		query = rewritten
		ctx.RawQuery = rewritten
	}
	if rewritten, err := e.rewriteSimpleQuantifiedSubqueries(ctx, query, databaseName); err != nil {
		results <- &Result{Err: newExecutorErrorf("execute-quantified-subquery", ExecutionErrorCodeUnknown, databaseName, "", query, err, "execute quantified subquery failed"), ResultType: common.RESULT_TYPE_QUERY, Message: "quantified subquery failed"}
		return
	} else if rewritten != "" {
		query = rewritten
		ctx.RawQuery = rewritten
	}
	if rewritten, err := e.rewriteSimpleScalarSubqueries(ctx, query, databaseName); err != nil {
		results <- &Result{Err: newExecutorErrorf("execute-scalar-subquery", ExecutionErrorCodeUnknown, databaseName, "", query, err, "execute scalar subquery failed"), ResultType: common.RESULT_TYPE_QUERY, Message: "scalar subquery failed"}
		return
	} else if rewritten != "" {
		query = rewritten
		ctx.RawQuery = rewritten
	}
	if rewritten := e.rewriteSessionUserVariables(query, mysqlSession); rewritten != query {
		query = rewritten
		ctx.RawQuery = rewritten
	}
	if rewritten := rewriteSessionMetadataFunctions(query); rewritten != query {
		query = rewritten
		ctx.RawQuery = rewritten
	}
	if handled, err := e.executeXACompatibility(ctx, mysqlSession, query); handled {
		if err != nil {
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		} else if !isXARecoverQuery(query) {
			results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "XA statement executed successfully"}
		}
		return
	}
	// SQL语法解析
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		results <- &Result{
			Err:        newExecutorErrorf("sql-parse", ExecutionErrorCodeValidation, "", "", query, err, "SQL parse error"),
			ResultType: common.RESULT_TYPE_QUERY,
			Message:    "Failed to parse SQL statement",
		}
		return
	}

	// 根据不同语句类型分派执行
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		// 执行SELECT查询
		selectResult, err := e.executeSelectStatement(ctx, stmt, databaseName)
		if err != nil {
			results <- &Result{
				Err:        newExecutorErrorf("execute-select", ExecutionErrorCodeUnknown, databaseName, "", query, err, "execute SELECT failed"),
				ResultType: common.RESULT_TYPE_QUERY,
				Message:    "SELECT query failed",
			}
		} else {
			// 将SelectResult转换为Result
			result := &Result{
				ResultType: common.RESULT_TYPE_QUERY,
				Data:       selectResult,
				Message:    fmt.Sprintf("SELECT query executed successfully, %d rows returned", selectResult.RowCount),
			}
			results <- result
		}
	case *sqlparser.Union:
		if selectResult, handled, err := e.executeInformationSchemaMetadataSelect(query, mysqlSession); handled {
			if err != nil {
				results <- &Result{
					Err:        newExecutorErrorf("execute-metadata-union", ExecutionErrorCodeUnknown, databaseName, "", query, err, "execute metadata UNION failed"),
					ResultType: common.RESULT_TYPE_QUERY,
					Message:    "metadata UNION query failed",
				}
				return
			}
			selectResult = normalizeInformationSchemaAggregate(query, selectResult)
			results <- &Result{
				ResultType: common.RESULT_TYPE_QUERY,
				Data:       selectResult,
				Message:    fmt.Sprintf("SELECT query executed successfully, %d rows returned", selectResult.RowCount),
			}
			return
		}
		results <- &Result{
			Err:        newExecutorErrorf("statement-dispatch", ExecutionErrorCodeValidation, databaseName, "", query, fmt.Errorf("unsupported statement type: %T", stmt), "unsupported statement type"),
			ResultType: common.RESULT_TYPE_QUERY,
			Message:    "Unsupported statement type",
		}
	case *sqlparser.Insert:
		// 执行INSERT语句
		if err := e.prepareTransactionalDML(mysqlSession); err != nil {
			results <- &Result{
				Err:        err,
				ResultType: innodbcommon.RESULT_TYPE_ERROR,
				Message:    err.Error(),
			}
			return
		}
		dmlResult, err := e.executeInsertStatement(ctx, stmt, databaseName, mysqlSession)
		if err != nil {
			results <- &Result{
				Err:        newExecutorErrorf("execute-insert", ExecutionErrorCodeUnknown, databaseName, "", query, err, "execute INSERT failed"),
				ResultType: common.RESULT_TYPE_QUERY,
				Message:    "INSERT failed",
			}
		} else {
			if mysqlSession != nil {
				mysqlSession.SetParamByName("row_count", int64(dmlResult.AffectedRows))
			}
			result := &Result{
				ResultType:   common.RESULT_TYPE_QUERY,
				Data:         dmlResult,
				AffectedRows: dmlResult.AffectedRows,
				LastInsertID: dmlResult.LastInsertId,
				Warnings:     dmlResult.Warnings,
				Message:      dmlResult.Message,
			}
			results <- result
		}
	case *sqlparser.Update:
		// 执行UPDATE语句
		if err := e.prepareTransactionalDML(mysqlSession); err != nil {
			results <- &Result{
				Err:        err,
				ResultType: innodbcommon.RESULT_TYPE_ERROR,
				Message:    err.Error(),
			}
			return
		}
		dmlResult, err := e.executeUpdateStatement(ctx, stmt, databaseName, mysqlSession)
		if err != nil {
			results <- &Result{
				Err:        newExecutorErrorf("execute-update", ExecutionErrorCodeUnknown, databaseName, "", query, err, "execute UPDATE failed"),
				ResultType: common.RESULT_TYPE_QUERY,
				Message:    "UPDATE failed",
			}
		} else {
			if mysqlSession != nil {
				mysqlSession.SetParamByName("row_count", int64(dmlResult.AffectedRows))
			}
			result := &Result{
				ResultType:   common.RESULT_TYPE_QUERY,
				Data:         dmlResult,
				AffectedRows: dmlResult.AffectedRows,
				LastInsertID: dmlResult.LastInsertId,
				Warnings:     dmlResult.Warnings,
				Message:      dmlResult.Message,
			}
			results <- result
		}
	case *sqlparser.Delete:
		// 执行DELETE语句
		if err := e.prepareTransactionalDML(mysqlSession); err != nil {
			results <- &Result{
				Err:        err,
				ResultType: innodbcommon.RESULT_TYPE_ERROR,
				Message:    err.Error(),
			}
			return
		}
		dmlResult, err := e.executeDeleteStatement(ctx, stmt, databaseName, mysqlSession)
		if err != nil {
			results <- &Result{
				Err:        newExecutorErrorf("execute-delete", ExecutionErrorCodeUnknown, databaseName, "", query, err, "execute DELETE failed"),
				ResultType: common.RESULT_TYPE_QUERY,
				Message:    "DELETE failed",
			}
		} else {
			if mysqlSession != nil {
				mysqlSession.SetParamByName("row_count", int64(dmlResult.AffectedRows))
			}
			result := &Result{
				ResultType:   common.RESULT_TYPE_QUERY,
				Data:         dmlResult,
				AffectedRows: dmlResult.AffectedRows,
				LastInsertID: dmlResult.LastInsertId,
				Warnings:     dmlResult.Warnings,
				Message:      dmlResult.Message,
			}
			results <- result
		}
	case *sqlparser.DDL:
		e.executeDDL(stmt, mysqlSession, databaseName, results, query)
	case *sqlparser.DBDDL:
		e.normalizeDBDDLOptions(stmt)
		e.executeDBDDL(stmt, mysqlSession, results)
	case *sqlparser.Show:
		e.executeShowStatementWithQuery(ctx, stmt, mysqlSession, query)
	case *sqlparser.Set:
		e.executeSetStatement(ctx, stmt, mysqlSession)
	case *sqlparser.Use:
		// 处理USE语句
		dbName := stmt.DBName.String()
		logger.Debugf(" 处理USE语句: %s", dbName)

		// 设置会话的数据库上下文（如果有会话的话）
		// 注意：这里的mysqlSession可能为nil，需要检查
		if mysqlSession != nil {
			mysqlSession.SetParamByName("database", dbName)
			logger.Debugf(" 会话数据库上下文已设置为: %s", dbName)
		}

		results <- &Result{
			ResultType: common.RESULT_TYPE_QUERY,
			Message:    fmt.Sprintf("Database changed to '%s'", dbName),
		}
	default:
		results <- &Result{
			Err:        newExecutorErrorf("statement-dispatch", ExecutionErrorCodeValidation, databaseName, "", query, fmt.Errorf("unsupported statement type: %T", stmt), "unsupported statement type"),
			ResultType: common.RESULT_TYPE_QUERY,
			Message:    "Unsupported statement type",
		}
	}
}

// executeRawAlterCompatibility covers ALTER variants that the legacy yacc
// grammar does not expose as a structured AST yet. It still updates the
// persisted dictionary atomically, so the statement cannot report success
// while leaving a half-written .frm file behind.
func (e *XMySQLExecutor) executeRawAlterCompatibility(ctx *ExecutionContext, query, currentDB string) (bool, error) {
	q := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	lowerQuery := strings.ToLower(q)
	if !alterTablePattern.MatchString(q) && !strings.HasPrefix(lowerQuery, "rename table ") {
		return false, nil
	}
	if ctx != nil && (sessionBoolParam(ctx.Session, "in_transaction") || sessionHasTransactionTableLocks(ctx.Session)) {
		if err := e.commitAutocommitTransaction(ctx.Session); err != nil {
			return true, err
		}
	}
	if !strings.Contains(strings.ToLower(q), " rename to ") {
		normalized, targetDB, _, qualified, err := normalizeQualifiedAlterTableTarget(q, currentDB)
		if err != nil {
			return true, err
		}
		if qualified {
			q = normalized
			currentDB = targetDB
		}
	}
	if table, ok := alterTableAccess(q, currentDB); ok {
		parts := strings.SplitN(table, ".", 2)
		if len(parts) == 2 {
			if err := e.checkAlterTablePrivileges(ctx, parts[0], parts[1]); err != nil {
				return true, err
			}
		}
	}
	if err := e.checkAlterForeignKeyPrivileges(ctx, currentDB, q); err != nil {
		return true, err
	}
	if table, ok := alterTableAccess(q, currentDB); ok && alterRequiresWriteLock(q) && !sessionHoldsTableLock(ctx.Session, table) {
		release, err := e.acquireDDLWriteLock(ctx, table, alterUsesSharedLock(q))
		if err != nil {
			return true, tableLockWaitError(err)
		}
		defer release()
	}
	tableCommentPattern := regexp.MustCompile("(?is)^alter\\s+table\\s+((?:`?[a-zA-Z0-9_$]+`?\\s*\\.\\s*)?`?[a-zA-Z0-9_$]+`?)\\s+comment\\s*(?:=\\s*)?(?:'((?:''|[^'])*)'|\"((?:\"\"|[^\"])*)\")$")
	if match := tableCommentPattern.FindStringSubmatch(q); len(match) == 4 {
		tableComment := match[2]
		if tableComment != "" {
			tableComment = strings.ReplaceAll(tableComment, "''", "'")
		} else {
			tableComment = strings.ReplaceAll(match[3], `""`, `"`)
		}
		targetDB, targetTable := compatibilityQualifiedTable(match[1], currentDB)
		if targetDB == "" || targetTable == "" {
			return true, fmt.Errorf("ALTER TABLE requires a database and table name")
		}
		frmPath := filepath.Join(e.getDataDir(), targetDB, targetTable+".frm")
		tableInfo, err := readTableMetadataMap(frmPath)
		if err != nil {
			return true, err
		}
		tableInfo["table_comment"] = tableComment
		return true, writeTableMetadataMapAtomic(frmPath, tableInfo)
	}
	tableCharsetPattern := regexp.MustCompile("(?is)^alter\\s+table\\s+((?:`?[a-zA-Z0-9_$]+`?\\s*\\.\\s*)?`?[a-zA-Z0-9_$]+`?)\\s+(?:default\\s+)?(?:character\\s+set|charset)\\s*(?:=\\s*)?([a-zA-Z0-9_]+)(?:\\s+collate\\s*(?:=\\s*)?([a-zA-Z0-9_]+))?$")
	if match := tableCharsetPattern.FindStringSubmatch(q); len(match) == 4 {
		targetDB, targetTable := compatibilityQualifiedTable(match[1], currentDB)
		return true, e.persistAlterTableCharacterOptions(targetDB, targetTable, match[2], match[3], "default charset="+match[2])
	}
	tableConvertCharsetPattern := regexp.MustCompile("(?is)^alter\\s+table\\s+((?:`?[a-zA-Z0-9_$]+`?\\s*\\.\\s*)?`?[a-zA-Z0-9_$]+`?)\\s+convert\\s+to\\s+(?:character\\s+set|charset)\\s*(?:=\\s*)?([a-zA-Z0-9_]+)(?:\\s+collate\\s*(?:=\\s*)?([a-zA-Z0-9_]+))?$")
	if match := tableConvertCharsetPattern.FindStringSubmatch(q); len(match) == 4 {
		targetDB, targetTable := compatibilityQualifiedTable(match[1], currentDB)
		return true, e.persistAlterTableCharacterOptions(targetDB, targetTable, match[2], match[3], "convert to charset="+match[2])
	}
	tableCollationPattern := regexp.MustCompile("(?is)^alter\\s+table\\s+((?:`?[a-zA-Z0-9_$]+`?\\s*\\.\\s*)?`?[a-zA-Z0-9_$]+`?)\\s+(?:default\\s+)?collate\\s*(?:=\\s*)?([a-zA-Z0-9_]+)$")
	if match := tableCollationPattern.FindStringSubmatch(q); len(match) == 3 {
		targetDB, targetTable := compatibilityQualifiedTable(match[1], currentDB)
		return true, e.persistAlterTableCharacterOptions(targetDB, targetTable, "", match[2], "default collate="+match[2])
	}
	tableRowFormatPattern := regexp.MustCompile("(?is)^alter\\s+table\\s+((?:`?[a-zA-Z0-9_$]+`?\\s*\\.\\s*)?`?[a-zA-Z0-9_$]+`?)\\s+row_format\\s*=\\s*(dynamic|compact|compressed|redundant|default)$")
	if match := tableRowFormatPattern.FindStringSubmatch(q); len(match) == 3 {
		targetDB, targetTable := compatibilityQualifiedTable(match[1], currentDB)
		return true, e.persistAlterTableRowFormat(targetDB, targetTable, match[2])
	}
	autoIncrementPattern := regexp.MustCompile("(?is)^alter\\s+table\\s+((?:`?[a-zA-Z0-9_$]+`?\\s*\\.\\s*)?`?[a-zA-Z0-9_$]+`?)\\s+auto_increment\\s*=\\s*([0-9]+)$")
	if match := autoIncrementPattern.FindStringSubmatch(q); len(match) == 3 {
		requested, err := strconv.ParseUint(match[2], 10, 64)
		if err != nil || requested == 0 {
			return true, fmt.Errorf("invalid AUTO_INCREMENT value %q", match[2])
		}
		targetDB, targetTable := compatibilityQualifiedTable(match[1], currentDB)
		frmPath := filepath.Join(e.getDataDir(), targetDB, targetTable+".frm")
		tableInfo, err := readTableMetadataMap(frmPath)
		if err != nil {
			return true, err
		}
		columns, _ := tableInfo["columns"].([]interface{})
		for _, rawColumn := range columns {
			column, ok := rawColumn.(map[string]interface{})
			if !ok {
				continue
			}
			autoIncrement, _ := column["auto_increment"].(bool)
			if !autoIncrement {
				continue
			}
			if err := observeAutoIncrementValue(e.getDataDir(), targetDB, targetTable, fmt.Sprint(column["name"]), requested-1); err != nil {
				return true, err
			}
			return true, nil
		}
		return true, fmt.Errorf("table '%s.%s' does not have an AUTO_INCREMENT column", targetDB, targetTable)
	}
	if handled, err := e.executeCompoundAlterCompatibility(ctx, q, currentDB); handled {
		return true, err
	}
	foreignKeyChecksEnabled := sessionForeignKeyChecksEnabled(ctx.Session)
	if handled, err := e.executeAlterOperationsCompatibility(q, currentDB, foreignKeyChecksEnabled); handled {
		return true, err
	}
	addUniqueConstraintPattern := regexp.MustCompile("(?is)^alter\\s+table\\s+([a-zA-Z0-9_$]+)\\s+add\\s+constraint\\s+`?([a-zA-Z0-9_$]+)`?\\s+unique(?:\\s+(?:index|key))?\\s*\\(([^)]+)\\)$")
	if match := addUniqueConstraintPattern.FindStringSubmatch(q); len(match) == 4 {
		translated := fmt.Sprintf("alter table %s add unique index %s (%s)", match[1], match[2], match[3])
		return e.alterTableIndexDDL(currentDB, match[1], translated, foreignKeyChecksEnabled)
	}
	addForeignKeyPattern := regexp.MustCompile("(?is)^alter\\s+table\\s+([a-zA-Z0-9_$]+)\\s+add\\s+(?:constraint\\s+`?([a-zA-Z0-9_$]+)`?\\s+)?foreign\\s+key\\s*\\(([^)]+)\\)\\s+references\\s+(?:(?:`?([a-zA-Z0-9_$]+)`?)\\.)?`?([a-zA-Z0-9_$]+)`?\\s*\\(([^)]+)\\)(.*)$")
	dropForeignKeyPattern := regexp.MustCompile("(?is)^alter\\s+table\\s+([a-zA-Z0-9_$]+)\\s+drop\\s+foreign\\s+key\\s+`?([a-zA-Z0-9_$]+)`?$")
	if match := addForeignKeyPattern.FindStringSubmatch(q); len(match) == 8 {
		return true, e.alterForeignKeyMetadata(currentDB, match[1], match[2], parseIdentifierList(match[3]), match[4], match[5], parseIdentifierList(match[6]), match[7], true, foreignKeyChecksEnabled)
	}
	if match := dropForeignKeyPattern.FindStringSubmatch(q); len(match) == 3 {
		return true, e.alterForeignKeyMetadata(currentDB, match[1], match[2], nil, "", "", nil, "", false, foreignKeyChecksEnabled)
	}
	addCheckPattern := regexp.MustCompile("(?is)^alter\\s+table\\s+([a-zA-Z0-9_$]+)\\s+add\\s+(?:constraint\\s+`?([a-zA-Z0-9_$]+)`?\\s+)?check\\s*\\(([^)]*)\\)(?:\\s+(enforced|not\\s+enforced))?$")
	dropCheckPattern := regexp.MustCompile("(?is)^alter\\s+table\\s+([a-zA-Z0-9_$]+)\\s+drop\\s+check\\s+`?([a-zA-Z0-9_$]+)`?$")
	if match := addCheckPattern.FindStringSubmatch(q); len(match) == 5 {
		enforced := !strings.EqualFold(strings.TrimSpace(match[4]), "not enforced")
		return true, e.alterCheckMetadata(currentDB, match[1], match[2], strings.TrimSpace(match[3]), enforced, true)
	}
	if match := dropCheckPattern.FindStringSubmatch(q); len(match) == 3 {
		return true, e.alterCheckMetadata(currentDB, match[1], match[2], "", true, false)
	}
	alterCheckPattern := regexp.MustCompile("(?is)^alter\\s+table\\s+([a-zA-Z0-9_$]+)\\s+alter\\s+check\\s+`?([a-zA-Z0-9_$]+)`?\\s+(enforced|not\\s+enforced)$")
	if match := alterCheckPattern.FindStringSubmatch(q); len(match) == 4 {
		enforced := strings.EqualFold(strings.TrimSpace(match[3]), "enforced")
		return true, e.alterCheckEnforcement(currentDB, match[1], match[2], enforced)
	}
	// MySQL accepts DROP CONSTRAINT for named CHECK constraints and, for
	// compatibility with schemas that use the same spelling for a foreign-key
	// symbol, we also try the foreign-key metadata path after CHECK lookup.
	dropConstraintPattern := regexp.MustCompile("(?is)^alter\\s+table\\s+([a-zA-Z0-9_$]+)\\s+drop\\s+constraint\\s+`?([a-zA-Z0-9_$]+)`?$")
	if match := dropConstraintPattern.FindStringSubmatch(q); len(match) == 3 {
		checkErr := e.alterCheckMetadata(currentDB, match[1], match[2], "", true, false)
		if checkErr == nil {
			return true, nil
		}
		foreignKeyErr := e.alterForeignKeyMetadata(currentDB, match[1], match[2], nil, "", "", nil, "", false, foreignKeyChecksEnabled)
		if foreignKeyErr == nil {
			return true, nil
		}
		return true, checkErr
	}
	addPrimaryPattern := regexp.MustCompile("(?is)^alter\\s+table\\s+([a-zA-Z0-9_$]+)\\s+add\\s+(?:constraint\\s+`?[a-zA-Z0-9_$]+`?\\s+)?primary\\s+key\\s*\\(([^)]+)\\)$")
	dropPrimaryPattern := regexp.MustCompile(`(?is)^alter\s+table\s+([a-zA-Z0-9_$]+)\s+drop\s+primary\s+key$`)
	if match := addPrimaryPattern.FindStringSubmatch(q); len(match) == 3 {
		return true, e.alterPrimaryKeyMetadata(currentDB, match[1], parseIdentifierList(match[2]), true, foreignKeyChecksEnabled)
	}
	if match := dropPrimaryPattern.FindStringSubmatch(q); len(match) == 2 {
		return true, e.alterPrimaryKeyMetadata(currentDB, match[1], nil, false, foreignKeyChecksEnabled)
	}
	setDefaultPattern := regexp.MustCompile("(?is)^alter\\s+table\\s+([a-zA-Z0-9_$]+)\\s+alter\\s+column\\s+`?([a-zA-Z0-9_$]+)`?\\s+set\\s+default\\s+(.+)$")
	dropDefaultPattern := regexp.MustCompile("(?is)^alter\\s+table\\s+([a-zA-Z0-9_$]+)\\s+alter\\s+column\\s+`?([a-zA-Z0-9_$]+)`?\\s+drop\\s+default$")
	if match := setDefaultPattern.FindStringSubmatch(q); len(match) == 4 {
		return true, e.alterColumnDefaultMetadata(currentDB, match[1], match[2], strings.TrimSpace(match[3]), true)
	}
	if match := dropDefaultPattern.FindStringSubmatch(q); len(match) == 3 {
		return true, e.alterColumnDefaultMetadata(currentDB, match[1], match[2], "", false)
	}
	modifyPattern := regexp.MustCompile(`(?is)^alter\s+table\s+([a-zA-Z0-9_$]+)\s+modify\s+(?:column\s+)?([a-zA-Z0-9_$]+)\s+(.+)$`)
	changePattern := regexp.MustCompile(`(?is)^alter\s+table\s+([a-zA-Z0-9_$]+)\s+change\s+(?:column\s+)?([a-zA-Z0-9_$]+)\s+([a-zA-Z0-9_$]+)\s+(.+)$`)
	renameColumnPattern := regexp.MustCompile(`(?is)^alter\s+table\s+([a-zA-Z0-9_$]+)\s+rename\s+column\s+([a-zA-Z0-9_$]+)\s+to\s+([a-zA-Z0-9_$]+)$`)
	if renamePairs, matched, err := parseRenameTablePairs(q, currentDB); matched {
		if err != nil {
			return true, err
		}
		if err := e.checkRenameTablePrivileges(ctx, renamePairs); err != nil {
			return true, err
		}
		keys := make([]string, 0, len(renamePairs)*2)
		for _, pair := range renamePairs {
			keys = append(keys,
				strings.ToLower(pair.sourceDB+"."+pair.sourceTable),
				strings.ToLower(pair.destinationDB+"."+pair.destinationTbl))
		}
		release, lockErr := e.acquireDDLTableLocks(ctx, ctx.Session, keys)
		if lockErr != nil {
			return true, lockErr
		}
		defer release()
		return true, e.renameTablesAtomically(renamePairs)
	}
	alterRenamePattern := regexp.MustCompile(`(?is)^alter\s+table\s+(?:\x60?([a-zA-Z0-9_$]+)\x60?\.)?\x60?([a-zA-Z0-9_$]+)\x60?\s+rename\s+to\s+(?:\x60?([a-zA-Z0-9_$]+)\x60?\.)?\x60?([a-zA-Z0-9_$]+)\x60?$`)

	if match := modifyPattern.FindStringSubmatch(q); len(match) > 0 {
		return true, e.replaceAlterColumnMetadata(currentDB, match[1], match[2], match[2], match[3])
	}
	if match := changePattern.FindStringSubmatch(q); len(match) > 0 {
		return true, e.replaceAlterColumnMetadata(currentDB, match[1], match[2], match[3], match[4])
	}
	if match := renameColumnPattern.FindStringSubmatch(q); len(match) > 0 {
		return true, e.renameAlterColumnMetadata(currentDB, match[1], match[2], match[3])
	}
	if match := alterRenamePattern.FindStringSubmatch(q); len(match) == 5 {
		sourceDB, destinationDB := match[1], match[3]
		if sourceDB == "" {
			sourceDB = currentDB
		}
		if destinationDB == "" {
			destinationDB = currentDB
		}
		return true, e.renameTableMetadataAcrossSchemas(sourceDB, match[2], destinationDB, match[4])
	}
	return false, nil
}

func (e *XMySQLExecutor) persistAlterTableCharacterOptions(databaseName, tableName, charset, collation, rawOptions string) error {
	frmPath := filepath.Join(e.getDataDir(), databaseName, tableName+".frm")
	tableInfo, err := readTableMetadataMap(frmPath)
	if err != nil {
		return err
	}
	options, _ := tableInfo["options"].(map[string]interface{})
	if options == nil {
		options = make(map[string]interface{})
	}
	if strings.TrimSpace(charset) != "" {
		options["charset"] = charset
	}
	if strings.TrimSpace(collation) != "" {
		options["collation"] = collation
	}
	if strings.TrimSpace(collation) != "" {
		rawOptions += " collate=" + collation
	}
	options["raw_options"] = strings.TrimSpace(rawOptions)
	tableInfo["options"] = options
	return writeTableMetadataMapAtomic(frmPath, tableInfo)
}

func (e *XMySQLExecutor) persistAlterTableRowFormat(databaseName, tableName, rowFormat string) error {
	frmPath := filepath.Join(e.getDataDir(), databaseName, tableName+".frm")
	tableInfo, err := readTableMetadataMap(frmPath)
	if err != nil {
		return err
	}
	options, _ := tableInfo["options"].(map[string]interface{})
	if options == nil {
		options = make(map[string]interface{})
	}
	normalized := normalizeTableRowFormat(rowFormat)
	options["row_format"] = normalized
	rawOptions, _ := options["raw_options"].(string)
	rowFormatOption := "row_format=" + strings.ToLower(normalized)
	rowFormatPattern := regexp.MustCompile(`(?is)\brow_format\s*=\s*(?:dynamic|compact|compressed|redundant|default)`)
	if rowFormatPattern.MatchString(rawOptions) {
		rawOptions = rowFormatPattern.ReplaceAllString(rawOptions, rowFormatOption)
	} else if strings.TrimSpace(rawOptions) == "" {
		rawOptions = rowFormatOption
	} else {
		rawOptions = strings.TrimSpace(rawOptions) + " " + rowFormatOption
	}
	options["raw_options"] = strings.TrimSpace(rawOptions)
	tableInfo["options"] = options
	return writeTableMetadataMapAtomic(frmPath, tableInfo)
}

// executeCompoundAlterCompatibility covers ALTER TABLE statements that mix
// the metadata-only column/index path with constraints handled by the raw
// compatibility path. The legacy parser exposes these as one TableSpec, so a
// compound statement such as ADD COLUMN ..., ADD CONSTRAINT ... cannot be
// executed as a single structured AST operation. Each top-level clause is
// still executed through the same single-operation implementation used by
// MySQL-compatible statements. The original .frm is restored if a later
// clause fails, preventing a reported DDL error from leaving partial metadata.
func (e *XMySQLExecutor) executeCompoundAlterCompatibility(ctx *ExecutionContext, query, currentDB string) (bool, error) {
	match := regexp.MustCompile("(?is)^alter\\s+table\\s+((?:`?[a-zA-Z0-9_$]+`?\\s*\\.\\s*)?`?[a-zA-Z0-9_$]+`?)\\s+(.+)$").FindStringSubmatch(query)
	if len(match) != 3 {
		return false, nil
	}
	parts := splitTopLevelComma(match[2])
	if len(parts) < 2 || !alterPartsContainConstraint(parts) {
		return false, nil
	}
	targetDB, targetTable := compatibilityQualifiedTable(match[1], currentDB)
	if targetDB == "" || targetTable == "" {
		return true, fmt.Errorf("no database selected")
	}
	frmPath := filepath.Join(e.getDataDir(), targetDB, targetTable+".frm")
	original, err := os.ReadFile(frmPath)
	if err != nil {
		return true, err
	}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || regexp.MustCompile(`(?is)^(?:algorithm|lock)\s*=`).MatchString(part) {
			continue
		}
		singleQuery := "alter table " + targetTable + " " + part
		// The structured compatibility parser intentionally declines ordinary
		// single-operation ALTERs so the normal AST path can handle them. The
		// synthetic no-op modifier keeps this clause on that same metadata path
		// while we are decomposing a compound statement.
		handled, partErr := e.executeAlterOperationsCompatibility(singleQuery+", algorithm=instant", targetDB, sessionForeignKeyChecksEnabled(ctx.Session))
		if !handled || partErr != nil {
			rawHandled, rawErr := e.executeRawAlterCompatibility(ctx, singleQuery, targetDB)
			if rawHandled {
				handled, partErr = rawHandled, rawErr
			}
		}
		if partErr != nil {
			_ = writeMetadataFileAtomic(frmPath, original)
			return true, partErr
		}
		if !handled {
			_ = writeMetadataFileAtomic(frmPath, original)
			return true, fmt.Errorf("unsupported ALTER TABLE operation %q", part)
		}
	}
	return true, nil
}

func alterPartsContainConstraint(parts []string) bool {
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if regexp.MustCompile(`(?is)^add\s+(?:constraint\b.*\b(?:unique|foreign\s+key|primary\s+key|check)\b|(?:unique|foreign\s+key|primary\s+key|check)\b)|^drop\s+(?:foreign\s+key|check|primary\s+key)\b`).MatchString(trimmed) {
			return true
		}
	}
	return false
}

func (e *XMySQLExecutor) alterPrimaryKeyMetadata(dbName, tableName string, columns []string, add, foreignKeyChecksEnabled bool) error {
	if dbName == "" || tableName == "" {
		return fmt.Errorf("no database selected")
	}
	frmPath := filepath.Join(e.getDataDir(), dbName, tableName+".frm")
	tableInfo, err := readTableMetadataMap(frmPath)
	if err != nil {
		return err
	}
	indexes, _ := tableInfo["indexes"].([]interface{})
	primaryIndex := -1
	for index, raw := range indexes {
		if value, ok := raw.(map[string]interface{}); ok {
			primary, _ := value["primary"].(bool)
			if primary || strings.EqualFold(fmt.Sprint(value["name"]), "PRIMARY") {
				primaryIndex = index
				break
			}
		}
	}
	metadataColumns, _ := tableInfo["columns"].([]interface{})
	columnByName := make(map[string]map[string]interface{}, len(metadataColumns))
	for _, raw := range metadataColumns {
		if value, ok := raw.(map[string]interface{}); ok {
			columnByName[strings.ToLower(fmt.Sprint(value["name"]))] = value
		}
	}
	if add {
		if primaryIndex >= 0 {
			return fmt.Errorf("multiple primary keys defined")
		}
		if len(columns) == 0 {
			return fmt.Errorf("primary key must contain at least one column")
		}
		if rowCount, scanErr := e.physicalTableRowCountStrict(dbName, tableName); scanErr != nil {
			return scanErr
		} else if rowCount > 0 {
			stmt, parseErr := sqlparser.Parse("select * from " + quoteMaintenanceIdentifier(tableName))
			if parseErr != nil {
				return parseErr
			}
			selectStmt, ok := stmt.(*sqlparser.Select)
			if !ok {
				return fmt.Errorf("primary key validation source is not SELECT")
			}
			result, selectErr := e.executeSelectStatement(&ExecutionContext{Context: context.Background(), DatabaseName: dbName}, selectStmt, dbName)
			if selectErr != nil {
				return selectErr
			}
			positions := make([]int, len(columns))
			for index, name := range columns {
				positions[index] = findResultColumn(result.Columns, name)
				if positions[index] < 0 {
					return fmt.Errorf("column '%s' does not exist", name)
				}
			}
			seen := make(map[string]struct{}, len(result.Records))
			for _, record := range result.Records {
				values := record.GetValues()
				parts := make([]string, 0, len(positions))
				for _, position := range positions {
					if position >= len(values) || values[position] == nil || values[position].IsNull() {
						return fmt.Errorf("primary key column cannot contain NULL")
					}
					parts = append(parts, windowValueKey(values[position]))
				}
				key := strings.Join(parts, "\x00")
				if _, exists := seen[key]; exists {
					return fmt.Errorf("duplicate entry for primary key")
				}
				seen[key] = struct{}{}
			}
		}
		for _, name := range columns {
			column, exists := columnByName[strings.ToLower(strings.Trim(name, "` "))]
			if !exists {
				return fmt.Errorf("column '%s' does not exist", name)
			}
			column["primary"] = true
			column["nullable"] = false
		}
		indexes = append(indexes, map[string]interface{}{"name": "PRIMARY", "type": "PRIMARY", "unique": true, "primary": true, "columns": columns})
	} else {
		if primaryIndex < 0 {
			return fmt.Errorf("can't drop primary key; no primary key exists")
		}
		if foreignKeyChecksEnabled {
			if err := e.validateReferencedForeignKeyIndexDrop(dbName, tableName, "PRIMARY", tableInfo); err != nil {
				return err
			}
		}
		filtered := make([]interface{}, 0, len(indexes)-1)
		for index, raw := range indexes {
			if index == primaryIndex {
				continue
			}
			filtered = append(filtered, raw)
		}
		indexes = filtered
		for _, column := range columnByName {
			delete(column, "primary")
		}
	}
	tableInfo["columns"] = metadataColumns
	tableInfo["indexes"] = indexes
	return writeTableMetadataMapAtomic(frmPath, tableInfo)
}

func (e *XMySQLExecutor) alterForeignKeyMetadata(dbName, tableName, name string, columns []string, refSchema string, refTable string, refColumns []string, tail string, add, foreignKeyChecksEnabled bool) error {
	if dbName == "" || tableName == "" {
		return fmt.Errorf("no database selected")
	}
	frmPath := filepath.Join(e.getDataDir(), dbName, tableName+".frm")
	tableInfo, err := readTableMetadataMap(frmPath)
	if err != nil {
		return err
	}
	foreignKeys, _ := tableInfo["foreign_keys"].([]interface{})
	indexes, _ := tableInfo["indexes"].([]interface{})
	if add {
		if err := validateForeignKeyReferentialActions(tail); err != nil {
			return err
		}
		if strings.TrimSpace(refSchema) == "" {
			refSchema = dbName
		}
		if name == "" {
			name = fmt.Sprintf("fk_%d", len(foreignKeys)+1)
		}
		for _, raw := range foreignKeys {
			if value, ok := raw.(map[string]interface{}); ok && strings.EqualFold(fmt.Sprint(value["name"]), name) {
				return fmt.Errorf("duplicate foreign key constraint '%s'", name)
			}
		}
		constraintExists, err := e.foreignKeyConstraintNameExists(dbName, tableName, name)
		if err != nil {
			return fmt.Errorf("check foreign key constraint '%s': %w", name, err)
		}
		if constraintExists {
			return fmt.Errorf("duplicate foreign key constraint '%s' in schema '%s'", name, dbName)
		}
		if len(columns) == 0 || len(columns) != len(refColumns) {
			return fmt.Errorf("foreign key column count does not match referenced column count")
		}
		childColumns, _ := tableInfo["columns"].([]interface{})
		for _, requested := range columns {
			found := false
			for _, raw := range childColumns {
				if column, ok := raw.(map[string]interface{}); ok && strings.EqualFold(fmt.Sprint(column["name"]), requested) {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("column '%s' does not exist", requested)
			}
		}
		if _, err := os.Stat(filepath.Join(e.getDataDir(), refSchema, refTable+".frm")); err != nil {
			return fmt.Errorf("referenced table '%s' does not exist", refTable)
		}
		if err := e.validateReferencedForeignKeyIndex(refSchema, refTable, refColumns); err != nil {
			return fmt.Errorf("cannot add foreign key '%s': %w", name, err)
		}
		if err := e.validateForeignKeyColumnTypes(tableInfo, columns, refSchema, refTable, refColumns); err != nil {
			return fmt.Errorf("cannot add foreign key '%s': %w", name, err)
		}
		if err := validateForeignKeySetNullColumns(tableInfo, columns, cascadeActionFromTail(tail, "delete"), cascadeActionFromTail(tail, "update")); err != nil {
			return fmt.Errorf("cannot add foreign key '%s': %w", name, err)
		}
		if foreignKeyChecksEnabled {
			if err := e.validateExistingRowsAgainstForeignKey(dbName, tableName, name, columns, refSchema, refTable, refColumns); err != nil {
				return err
			}
		}
		foreignKeys = append(foreignKeys, map[string]interface{}{
			"name": name, "columns": columns, "ref_schema": refSchema, "ref_table": refTable, "ref_columns": refColumns,
			"on_delete":      cascadeActionFromTail(strings.ToLower(tail), "delete"),
			"on_update":      cascadeActionFromTail(strings.ToLower(tail), "update"),
			"raw_definition": strings.TrimSpace("foreign key (" + strings.Join(columns, ",") + ") references " + refSchema + "." + refTable + "(" + strings.Join(refColumns, ",") + ")" + tail),
		})
		covered := false
		for _, raw := range indexes {
			index, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			indexColumns, _ := index["columns"].([]interface{})
			if len(indexColumns) < len(columns) {
				continue
			}
			covered = true
			for index, column := range columns {
				if !strings.EqualFold(fmt.Sprint(indexColumns[index]), column) {
					covered = false
					break
				}
			}
			if covered {
				break
			}
		}
		if !covered {
			indexColumns := make([]interface{}, 0, len(columns))
			for _, column := range columns {
				indexColumns = append(indexColumns, column)
			}
			indexes = append(indexes, map[string]interface{}{"name": name, "type": "INDEX", "unique": false, "primary": false, "auto_foreign_key": true, "columns": indexColumns})
			tableInfo["indexes"] = indexes
		}
	} else {
		filtered := make([]interface{}, 0, len(foreignKeys))
		found := false
		for _, raw := range foreignKeys {
			value, ok := raw.(map[string]interface{})
			if ok && strings.EqualFold(fmt.Sprint(value["name"]), name) {
				found = true
				continue
			}
			filtered = append(filtered, raw)
		}
		if !found {
			return fmt.Errorf("foreign key constraint '%s' does not exist", name)
		}
		foreignKeys = filtered
		remainingForeignKeysNeedIndex := func(indexColumns []string) bool {
			for _, rawForeignKey := range foreignKeys {
				foreignKey, ok := rawForeignKey.(map[string]interface{})
				if !ok {
					continue
				}
				if identifierPrefixMatches(indexColumns, metadataIdentifierList(foreignKey["columns"])) {
					return true
				}
			}
			return false
		}
		filteredIndexes := make([]interface{}, 0, len(indexes))
		for _, rawIndex := range indexes {
			index, ok := rawIndex.(map[string]interface{})
			if !ok {
				filteredIndexes = append(filteredIndexes, rawIndex)
				continue
			}
			autoForeignKey, _ := index["auto_foreign_key"].(bool)
			if !autoForeignKey || remainingForeignKeysNeedIndex(metadataIdentifierList(index["columns"])) {
				filteredIndexes = append(filteredIndexes, rawIndex)
			}
		}
		indexes = filteredIndexes
		tableInfo["indexes"] = indexes
	}
	tableInfo["foreign_keys"] = foreignKeys
	return writeTableMetadataMapAtomic(frmPath, tableInfo)
}

// validateReferencedForeignKeyIndex enforces the InnoDB requirement that the
// referenced columns are covered by an index in left-to-right order. A
// non-unique index remains valid for the compatibility path; MySQL 8.4 still
// permits this legacy InnoDB extension when restrict_fk_on_non_standard_key
// is not enabled.
func (e *XMySQLExecutor) validateReferencedForeignKeyIndex(schemaName, tableName string, referencedColumnsValue interface{}) error {
	if e == nil {
		return fmt.Errorf("executor is nil")
	}
	referencedColumns := metadataIdentifierList(referencedColumnsValue)
	if len(referencedColumns) == 0 {
		return fmt.Errorf("referenced key must contain at least one column")
	}
	info, err := e.readPersistedTableInfo(schemaName, tableName)
	if err != nil {
		return fmt.Errorf("load referenced table metadata: %w", err)
	}
	for _, column := range referencedColumns {
		found := false
		for _, rawColumn := range info.Columns {
			if strings.EqualFold(strings.TrimSpace(fmt.Sprint(rawColumn["name"])), column) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("referenced column '%s' does not exist", column)
		}
	}
	for _, index := range info.Indexes {
		indexColumns := metadataIdentifierList(index["columns"])
		if len(indexColumns) < len(referencedColumns) {
			continue
		}
		covered := true
		for position, column := range referencedColumns {
			if !strings.EqualFold(indexColumns[position], column) {
				covered = false
				break
			}
		}
		if covered {
			return nil
		}
	}
	return fmt.Errorf("there is no index in the referenced table where the referenced columns are the first columns")
}

func (e *XMySQLExecutor) validateForeignKeyColumnTypes(childTableInfo map[string]interface{}, childColumns []string, refSchema, refTable string, refColumns []string) error {
	if e == nil {
		return fmt.Errorf("executor is nil")
	}
	if len(childColumns) == 0 || len(childColumns) != len(refColumns) {
		return fmt.Errorf("foreign key column count does not match referenced column count")
	}
	parentInfo, err := e.readPersistedTableInfo(refSchema, refTable)
	if err != nil {
		return fmt.Errorf("load referenced table metadata: %w", err)
	}
	childInfoColumns, _ := childTableInfo["columns"].([]interface{})
	findColumn := func(columns interface{}, name string) map[string]interface{} {
		switch typedColumns := columns.(type) {
		case []interface{}:
			for _, rawColumn := range typedColumns {
				column, ok := rawColumn.(map[string]interface{})
				if ok && strings.EqualFold(strings.TrimSpace(fmt.Sprint(column["name"])), strings.TrimSpace(name)) {
					return column
				}
			}
		case []map[string]interface{}:
			for _, column := range typedColumns {
				if strings.EqualFold(strings.TrimSpace(fmt.Sprint(column["name"])), strings.TrimSpace(name)) {
					return column
				}
			}
		}
		return nil
	}
	for index, childName := range childColumns {
		childColumn := findColumn(childInfoColumns, childName)
		parentColumn := findColumn(parentInfo.Columns, refColumns[index])
		if childColumn == nil {
			return fmt.Errorf("foreign key column '%s' does not exist", childName)
		}
		if parentColumn == nil {
			return fmt.Errorf("referenced column '%s' does not exist", refColumns[index])
		}
		if !foreignKeyColumnTypesCompatible(childColumn, parentColumn) {
			return fmt.Errorf("foreign key column '%s' type %s is incompatible with referenced column '%s' type %s", childName, foreignKeyColumnTypeDescription(childColumn), refColumns[index], foreignKeyColumnTypeDescription(parentColumn))
		}
	}
	return nil
}

func validateForeignKeySetNullColumns(childTableInfo map[string]interface{}, childColumns []string, onDelete, onUpdate string) error {
	if normalizeReferentialAction(onDelete) != "set null" && normalizeReferentialAction(onUpdate) != "set null" {
		return nil
	}
	columns, _ := childTableInfo["columns"].([]interface{})
	for _, childName := range childColumns {
		var childColumn map[string]interface{}
		for _, rawColumn := range columns {
			column, ok := rawColumn.(map[string]interface{})
			if ok && strings.EqualFold(strings.TrimSpace(fmt.Sprint(column["name"])), strings.TrimSpace(childName)) {
				childColumn = column
				break
			}
		}
		if childColumn == nil {
			continue
		}
		if nullable, present := childColumn["nullable"].(bool); present && !nullable {
			return fmt.Errorf("foreign key column '%s' must be nullable for SET NULL referential action", childName)
		}
	}
	return nil
}

func (e *XMySQLExecutor) foreignKeyConstraintNameExists(schemaName, currentTable, name string) (bool, error) {
	if e == nil || strings.TrimSpace(schemaName) == "" || strings.TrimSpace(name) == "" || strings.EqualFold(strings.TrimSpace(name), "<nil>") {
		return false, nil
	}
	entries, err := os.ReadDir(filepath.Join(e.getDataDir(), schemaName))
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".frm") || strings.EqualFold(strings.TrimSuffix(entry.Name(), ".frm"), currentTable) {
			continue
		}
		info, err := readTableMetadataMap(filepath.Join(e.getDataDir(), schemaName, entry.Name()))
		if err != nil {
			return false, err
		}
		foreignKeys, _ := info["foreign_keys"].([]interface{})
		for _, rawForeignKey := range foreignKeys {
			foreignKey, ok := rawForeignKey.(map[string]interface{})
			if ok && strings.EqualFold(strings.TrimSpace(fmt.Sprint(foreignKey["name"])), strings.TrimSpace(name)) {
				return true, nil
			}
		}
	}
	return false, nil
}

func foreignKeyColumnTypeDescription(column map[string]interface{}) string {
	if column == nil {
		return "<unknown>"
	}
	base := strings.ToUpper(strings.TrimSpace(fmt.Sprint(column["type"])))
	if base == "" || base == "<NIL>" {
		base = "<unknown>"
	}
	if unsigned, _ := column["unsigned"].(bool); unsigned {
		base += " UNSIGNED"
	}
	return base
}

func foreignKeyColumnTypesCompatible(child, parent map[string]interface{}) bool {
	childType := normalizeForeignKeyColumnType(child)
	parentType := normalizeForeignKeyColumnType(parent)
	if childType == "" || parentType == "" {
		return true
	}
	if isForeignKeyIntegerType(childType) || isForeignKeyIntegerType(parentType) {
		return childType == parentType && foreignKeyColumnUnsigned(child) == foreignKeyColumnUnsigned(parent)
	}
	if isForeignKeyDecimalType(childType) || isForeignKeyDecimalType(parentType) {
		return childType == parentType && foreignKeyColumnUnsigned(child) == foreignKeyColumnUnsigned(parent) &&
			foreignKeyColumnNumber(child, "length") == foreignKeyColumnNumber(parent, "length") &&
			foreignKeyColumnNumber(child, "scale") == foreignKeyColumnNumber(parent, "scale")
	}
	if isForeignKeyStringType(childType) || isForeignKeyStringType(parentType) {
		if childType != parentType {
			return false
		}
		return foreignKeyOptionalStringEqual(child, parent, "charset") && foreignKeyOptionalStringEqual(child, parent, "collate")
	}
	return childType == parentType
}

func normalizeForeignKeyColumnType(column map[string]interface{}) string {
	if column == nil {
		return ""
	}
	typeName := strings.ToUpper(strings.TrimSpace(fmt.Sprint(column["type"])))
	if index := strings.Index(typeName, "("); index >= 0 {
		typeName = typeName[:index]
	}
	switch typeName {
	case "INTEGER":
		return "INT"
	case "DEC":
		return "DECIMAL"
	case "NUMERIC":
		return "DECIMAL"
	case "CHARACTER VARYING":
		return "VARCHAR"
	default:
		return typeName
	}
}

func isForeignKeyIntegerType(typeName string) bool {
	switch typeName {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT", "YEAR", "BIT":
		return true
	default:
		return false
	}
}

func isForeignKeyDecimalType(typeName string) bool {
	switch typeName {
	case "DECIMAL", "FLOAT", "DOUBLE":
		return true
	default:
		return false
	}
}

func isForeignKeyStringType(typeName string) bool {
	switch typeName {
	case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
		return true
	default:
		return false
	}
}

func foreignKeyColumnUnsigned(column map[string]interface{}) bool {
	unsigned, _ := column["unsigned"].(bool)
	return unsigned
}

func foreignKeyColumnNumber(column map[string]interface{}, key string) int64 {
	if column == nil {
		return 0
	}
	switch value := column[key].(type) {
	case int:
		return int64(value)
	case int64:
		return value
	case float64:
		return int64(value)
	case string:
		parsed, _ := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		return parsed
	default:
		parsed, _ := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(value)), 10, 64)
		return parsed
	}
}

func foreignKeyOptionalStringEqual(left, right map[string]interface{}, key string) bool {
	leftValue := strings.TrimSpace(fmt.Sprint(left[key]))
	rightValue := strings.TrimSpace(fmt.Sprint(right[key]))
	if leftValue == "" || leftValue == "<nil>" || rightValue == "" || rightValue == "<nil>" {
		return true
	}
	return strings.EqualFold(leftValue, rightValue)
}

func (e *XMySQLExecutor) validateExistingRowsAgainstForeignKey(dbName, tableName, name string, columns []string, refSchema string, refTable string, refColumns []string) error {
	if e == nil || e.tableStorageManager == nil {
		return fmt.Errorf("cannot validate foreign key '%s': table storage manager is not initialized", name)
	}
	childMeta, err := (&SelectExecutor{}).loadTableMetaFromFrm(e.getDataDir(), dbName, tableName)
	if err != nil {
		return fmt.Errorf("cannot validate foreign key '%s': load child table metadata: %w", name, err)
	}
	parentMeta, err := (&SelectExecutor{}).loadTableMetaFromFrm(e.getDataDir(), refSchema, refTable)
	if err != nil {
		return fmt.Errorf("cannot validate foreign key '%s': load referenced table metadata: %w", name, err)
	}
	for _, refColumn := range refColumns {
		found := false
		for _, column := range parentMeta.Columns {
			if column != nil && strings.EqualFold(column.Name, refColumn) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("referenced column '%s' does not exist", refColumn)
		}
	}
	childBTree, err := e.tableStorageManager.CreateBTreeManagerForTable(context.Background(), dbName, tableName)
	if err != nil {
		return fmt.Errorf("cannot validate foreign key '%s': create child scanner: %w", name, err)
	}
	rows, err := NewClusteredIndexScanner(childBTree, childMeta).Scan(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("cannot validate foreign key '%s': scan existing rows: %w", name, err)
	}
	dml := &StorageIntegratedDMLExecutor{
		dataDir:             e.getDataDir(),
		tableStorageManager: e.tableStorageManager,
		tableName:           tableName,
	}
	if err := dml.validateRowsAgainstForeignKeys(context.Background(), rows, dbName, tableName, childMeta, []foreignKeyRuntimeMeta{{
		Columns: columns, RefSchema: refSchema, RefTable: refTable, RefColumns: refColumns,
	}}); err != nil {
		return fmt.Errorf("foreign key '%s' is violated by an existing row: %w", name, err)
	}
	return nil
}

func (e *XMySQLExecutor) alterCheckMetadata(dbName, tableName, name, expression string, enforced, add bool) error {
	if dbName == "" || tableName == "" {
		return fmt.Errorf("no database selected")
	}
	frmPath := filepath.Join(e.getDataDir(), dbName, tableName+".frm")
	tableInfo, err := readTableMetadataMap(frmPath)
	if err != nil {
		return err
	}
	checks, _ := tableInfo["checks"].([]interface{})
	names, _ := tableInfo["check_names"].(map[string]interface{})
	if names == nil {
		names = make(map[string]interface{})
	}
	enforcedByName, _ := tableInfo["check_enforced"].(map[string]interface{})
	if enforcedByName == nil {
		enforcedByName = make(map[string]interface{})
	}
	if add {
		if name == "" {
			name = fmt.Sprintf("check_%d", len(checks)+1)
		}
		if _, exists := names[strings.ToLower(name)]; exists {
			return fmt.Errorf("duplicate check constraint '%s'", name)
		}
		if enforced {
			if err := e.validateExistingRowsAgainstCheck(dbName, tableName, name, expression); err != nil {
				return err
			}
		}
		checks = append(checks, expression)
		names[strings.ToLower(name)] = expression
		enforcedByName[strings.ToLower(name)] = enforced
	} else {
		value, exists := names[strings.ToLower(name)]
		if !exists {
			return fmt.Errorf("check constraint '%s' does not exist", name)
		}
		expression, _ = value.(string)
		filtered := make([]interface{}, 0, len(checks))
		removed := false
		for _, check := range checks {
			if !removed && strings.EqualFold(fmt.Sprint(check), expression) {
				removed = true
				continue
			}
			filtered = append(filtered, check)
		}
		if !removed {
			return fmt.Errorf("check constraint '%s' metadata is inconsistent", name)
		}
		checks = filtered
		delete(names, strings.ToLower(name))
		delete(enforcedByName, strings.ToLower(name))
	}
	tableInfo["checks"] = checks
	tableInfo["check_names"] = names
	tableInfo["check_enforced"] = enforcedByName
	return writeTableMetadataMapAtomic(frmPath, tableInfo)
}

func (e *XMySQLExecutor) validateExistingRowsAgainstCheck(dbName, tableName, name, expression string) error {
	if e == nil || e.tableStorageManager == nil {
		return fmt.Errorf("cannot validate check constraint '%s': table storage manager is not initialized", name)
	}
	tableMeta, err := (&SelectExecutor{}).loadTableMetaFromFrm(e.getDataDir(), dbName, tableName)
	if err != nil {
		return fmt.Errorf("cannot validate check constraint '%s': load table metadata: %w", name, err)
	}
	btreeManager, err := e.tableStorageManager.CreateBTreeManagerForTable(context.Background(), dbName, tableName)
	if err != nil {
		return fmt.Errorf("cannot validate check constraint '%s': create table scanner: %w", name, err)
	}
	rows, err := NewClusteredIndexScanner(btreeManager, tableMeta).Scan(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("cannot validate check constraint '%s': scan existing rows: %w", name, err)
	}
	for _, row := range rows {
		if row == nil {
			continue
		}
		truth, err := evaluateCheckConstraint(expression, row.ColumnValues)
		if err != nil {
			return fmt.Errorf("cannot validate check constraint '%s': evaluation failed: %w", name, err)
		}
		if truth != sqlTruthTrue && truth != sqlTruthUnknown {
			return fmt.Errorf("check constraint '%s' is violated by an existing row", name)
		}
	}
	return nil
}

func (e *XMySQLExecutor) alterCheckEnforcement(dbName, tableName, name string, enforced bool) error {
	if dbName == "" || tableName == "" {
		return fmt.Errorf("no database selected")
	}
	frmPath := filepath.Join(e.getDataDir(), dbName, tableName+".frm")
	tableInfo, err := readTableMetadataMap(frmPath)
	if err != nil {
		return err
	}
	names, _ := tableInfo["check_names"].(map[string]interface{})
	key := strings.ToLower(strings.TrimSpace(name))
	if names == nil {
		return fmt.Errorf("check constraint '%s' does not exist", name)
	}
	if _, exists := names[key]; !exists {
		return fmt.Errorf("check constraint '%s' does not exist", name)
	}
	if enforced {
		expression, ok := names[key].(string)
		if !ok || strings.TrimSpace(expression) == "" {
			return fmt.Errorf("check constraint '%s' metadata is invalid", name)
		}
		if err := e.validateExistingRowsAgainstCheck(dbName, tableName, name, expression); err != nil {
			return err
		}
	}
	enforcedByName, _ := tableInfo["check_enforced"].(map[string]interface{})
	if enforcedByName == nil {
		enforcedByName = make(map[string]interface{})
	}
	enforcedByName[key] = enforced
	tableInfo["check_enforced"] = enforcedByName
	return writeTableMetadataMapAtomic(frmPath, tableInfo)
}

func (e *XMySQLExecutor) alterColumnDefaultMetadata(dbName, tableName, columnName, defaultValue string, setDefault bool) error {
	if dbName == "" || tableName == "" {
		return fmt.Errorf("no database selected")
	}
	frmPath := filepath.Join(e.getDataDir(), dbName, tableName+".frm")
	tableInfo, err := readTableMetadataMap(frmPath)
	if err != nil {
		return err
	}
	columns, _ := tableInfo["columns"].([]interface{})
	found := false
	for _, raw := range columns {
		column, ok := raw.(map[string]interface{})
		if !ok || !strings.EqualFold(fmt.Sprint(column["name"]), columnName) {
			continue
		}
		found = true
		if setDefault {
			column["default"] = defaultValue
		} else {
			delete(column, "default")
		}
		break
	}
	if !found {
		return fmt.Errorf("column '%s' does not exist", columnName)
	}
	tableInfo["columns"] = columns
	return writeTableMetadataMapAtomic(frmPath, tableInfo)
}

func (e *XMySQLExecutor) renameAlterColumnMetadata(dbName, tableName, oldName, newName string) error {
	if dbName == "" || tableName == "" {
		return fmt.Errorf("no database selected")
	}
	frmPath := filepath.Join(e.getDataDir(), dbName, tableName+".frm")
	tableInfo, err := readTableMetadataMap(frmPath)
	if err != nil {
		return err
	}
	columns, _ := tableInfo["columns"].([]interface{})
	found := false
	for _, value := range columns {
		if column, ok := value.(map[string]interface{}); ok && strings.EqualFold(fmt.Sprint(column["name"]), oldName) {
			column["name"] = newName
			found = true
		}
	}
	if !found {
		return fmt.Errorf("column '%s' does not exist", oldName)
	}
	if err := renameColumnReferences(tableInfo, oldName, newName); err != nil {
		return err
	}
	if err := renameReferencedForeignKeyColumns(e.getDataDir(), dbName, tableName, oldName, newName); err != nil {
		return err
	}
	tableInfo["columns"] = columns
	return writeTableMetadataMapAtomic(frmPath, tableInfo)
}

func (e *XMySQLExecutor) replaceAlterColumnMetadata(dbName, tableName, oldName, newName, definition string) error {
	if dbName == "" || tableName == "" {
		return fmt.Errorf("no database selected")
	}
	frmPath := filepath.Join(e.getDataDir(), dbName, tableName+".frm")
	tableInfo, err := readTableMetadataMap(frmPath)
	if err != nil {
		return err
	}
	cleanDefinition, first, afterColumn := parseColumnPosition(definition)
	parsed := parseCreateTableColumnsFallback("create table alter_target (" + newName + " " + cleanDefinition + ")")
	if len(parsed) != 1 {
		return fmt.Errorf("invalid column definition for '%s'", newName)
	}
	if typeName, ok := parsed[0]["type"].(string); ok {
		parsed[0]["type"] = strings.ToLower(typeName)
	}
	columns, _ := tableInfo["columns"].([]interface{})
	found := false
	alteredIndex := -1
	for i, value := range columns {
		column, ok := value.(map[string]interface{})
		if !ok || !strings.EqualFold(fmt.Sprint(column["name"]), oldName) {
			continue
		}
		updated := parsed[0]
		// MODIFY keeps the name; CHANGE supplies a new one.
		updated["name"] = newName
		for key, value := range column {
			if _, exists := updated[key]; !exists && (key == "primary" || key == "unique" || key == "auto_increment" || key == "generated" || key == "generated_expression") {
				updated[key] = value
			}
		}
		columns[i] = updated
		found = true
		alteredIndex = i
		break
	}
	if !found {
		return fmt.Errorf("column '%s' does not exist", oldName)
	}
	if first || afterColumn != "" {
		column := columns[alteredIndex]
		remaining := append([]interface{}(nil), columns[:alteredIndex]...)
		remaining = append(remaining, columns[alteredIndex+1:]...)
		if first {
			columns = append([]interface{}{column}, remaining...)
		} else {
			position := -1
			for i, value := range remaining {
				if candidate, ok := value.(map[string]interface{}); ok && strings.EqualFold(fmt.Sprint(candidate["name"]), afterColumn) {
					position = i
					break
				}
			}
			if position < 0 {
				return fmt.Errorf("column '%s' does not exist", afterColumn)
			}
			remaining = append(remaining, nil)
			copy(remaining[position+2:], remaining[position+1:])
			remaining[position+1] = column
			columns = remaining
		}
	}
	if !strings.EqualFold(oldName, newName) {
		if err := renameColumnReferences(tableInfo, oldName, newName); err != nil {
			return err
		}
		if err := renameReferencedForeignKeyColumns(e.getDataDir(), dbName, tableName, oldName, newName); err != nil {
			return err
		}
	}
	tableInfo["columns"] = columns
	return writeTableMetadataMapAtomic(frmPath, tableInfo)
}

func renameColumnReferences(tableInfo map[string]interface{}, oldName, newName string) error {
	columns, _ := tableInfo["columns"].([]interface{})
	for _, value := range columns {
		column, ok := value.(map[string]interface{})
		if !ok {
			continue
		}
		if expression, ok := column["generated_expression"].(string); ok {
			column["generated_expression"] = renameSQLIdentifier(expression, oldName, newName)
		}
	}
	indexes, _ := tableInfo["indexes"].([]interface{})
	for _, value := range indexes {
		index, ok := value.(map[string]interface{})
		if !ok {
			continue
		}
		columns, _ := index["columns"].([]interface{})
		for i, column := range columns {
			if strings.EqualFold(fmt.Sprint(column), oldName) {
				columns[i] = newName
			}
		}
		index["columns"] = columns
	}
	foreignKeys, _ := tableInfo["foreign_keys"].([]interface{})
	for _, value := range foreignKeys {
		foreignKey, ok := value.(map[string]interface{})
		if !ok {
			continue
		}
		columns, _ := foreignKey["columns"].([]interface{})
		for index, column := range columns {
			if strings.EqualFold(fmt.Sprint(column), oldName) {
				columns[index] = newName
			}
		}
		foreignKey["columns"] = columns
		if rawDefinition, ok := foreignKey["raw_definition"].(string); ok {
			foreignKey["raw_definition"] = renameSQLIdentifier(rawDefinition, oldName, newName)
		}
	}
	checks, _ := tableInfo["checks"].([]interface{})
	for index, value := range checks {
		if expression, ok := value.(string); ok {
			checks[index] = renameSQLIdentifier(expression, oldName, newName)
		}
	}
	tableInfo["checks"] = checks
	checkNames, _ := tableInfo["check_names"].(map[string]interface{})
	for name, value := range checkNames {
		if expression, ok := value.(string); ok {
			checkNames[name] = renameSQLIdentifier(expression, oldName, newName)
		}
	}
	tableInfo["check_names"] = checkNames
	return nil
}

func renameSQLIdentifier(expression, oldName, newName string) string {
	oldName = strings.Trim(oldName, "`")
	if oldName == "" || strings.EqualFold(oldName, newName) {
		return expression
	}
	pattern := regexp.MustCompile(`(?i)(^|[^a-zA-Z0-9_$])` + regexp.QuoteMeta(oldName) + `([^a-zA-Z0-9_$]|$)`)
	return pattern.ReplaceAllStringFunc(expression, func(match string) string {
		index := strings.Index(strings.ToLower(match), strings.ToLower(oldName))
		if index < 0 {
			return match
		}
		return match[:index] + newName + match[index+len(oldName):]
	})
}

func foreignKeyReferencesParent(foreignKey map[string]interface{}, childDB, parentDB, parentTable string) bool {
	if !strings.EqualFold(strings.Trim(strings.TrimSpace(fmt.Sprint(foreignKey["ref_table"])), "`"), parentTable) {
		return false
	}
	refDB := strings.Trim(strings.TrimSpace(fmt.Sprint(foreignKey["ref_schema"])), "`")
	if refDB == "" {
		refDB = childDB
	}
	return strings.EqualFold(refDB, parentDB)
}

func renameReferencedForeignKeyColumns(dataDir, dbName, tableName, oldName, newName string) error {
	schemas, err := os.ReadDir(dataDir)
	if err != nil {
		return fmt.Errorf("read data directory metadata failed: %v", err)
	}
	for _, schemaEntry := range schemas {
		if !schemaEntry.IsDir() {
			continue
		}
		childDB := schemaEntry.Name()
		entries, readErr := os.ReadDir(filepath.Join(dataDir, childDB))
		if readErr != nil {
			return readErr
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.EqualFold(filepath.Ext(entry.Name()), ".frm") {
				continue
			}
			path := filepath.Join(dataDir, childDB, entry.Name())
			foreignTable, readErr := readTableMetadataMap(path)
			if readErr != nil {
				return readErr
			}
			foreignKeys, _ := foreignTable["foreign_keys"].([]interface{})
			changed := false
			for _, value := range foreignKeys {
				foreignKey, ok := value.(map[string]interface{})
				if !ok || !foreignKeyReferencesParent(foreignKey, childDB, dbName, tableName) {
					continue
				}
				columns, _ := foreignKey["ref_columns"].([]interface{})
				for index, column := range columns {
					if strings.EqualFold(fmt.Sprint(column), oldName) {
						columns[index] = newName
						changed = true
					}
				}
				foreignKey["ref_columns"] = columns
			}
			if changed {
				foreignTable["foreign_keys"] = foreignKeys
				if writeErr := writeTableMetadataMapAtomic(path, foreignTable); writeErr != nil {
					return writeErr
				}
			}
		}
	}
	return nil
}

func renameReferencedForeignKeyTable(dataDir, oldDB, oldTable, newDB, newTable string) error {
	schemas, err := os.ReadDir(dataDir)
	if err != nil {
		return fmt.Errorf("read data directory metadata failed: %v", err)
	}
	for _, schemaEntry := range schemas {
		if !schemaEntry.IsDir() {
			continue
		}
		childDB := schemaEntry.Name()
		entries, readErr := os.ReadDir(filepath.Join(dataDir, childDB))
		if readErr != nil {
			return readErr
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.EqualFold(filepath.Ext(entry.Name()), ".frm") {
				continue
			}
			path := filepath.Join(dataDir, childDB, entry.Name())
			childTable, readErr := readTableMetadataMap(path)
			if readErr != nil {
				return readErr
			}
			foreignKeys, _ := childTable["foreign_keys"].([]interface{})
			changed := false
			for _, value := range foreignKeys {
				foreignKey, ok := value.(map[string]interface{})
				if !ok || !foreignKeyReferencesParent(foreignKey, childDB, oldDB, oldTable) {
					continue
				}
				foreignKey["ref_schema"] = newDB
				foreignKey["ref_table"] = newTable
				if rawDefinition, ok := foreignKey["raw_definition"].(string); ok && strings.TrimSpace(rawDefinition) != "" {
					foreignKey["raw_definition"] = rewriteForeignKeyReferenceTable(rawDefinition, oldDB, oldTable, newDB, newTable)
				}
				changed = true
			}
			if changed {
				childTable["foreign_keys"] = foreignKeys
				if writeErr := writeTableMetadataMapAtomic(path, childTable); writeErr != nil {
					return writeErr
				}
			}
		}
	}
	return nil
}

func rewriteForeignKeyReferenceTable(definition, oldDB, oldTable, newDB, newTable string) string {
	qualifiedTarget := newDB + "." + newTable
	pattern := regexp.MustCompile(`(?i)(references\s+)(?:\x60?` + regexp.QuoteMeta(oldDB) + `\x60?\s*\.\s*)?\x60?` + regexp.QuoteMeta(oldTable) + `\x60?`)
	return pattern.ReplaceAllString(definition, "${1}"+qualifiedTarget)
}

func (e *XMySQLExecutor) validateReferencedForeignKeysAfterColumnDrop(dbName, tableName, droppedColumn string) error {
	if e == nil {
		return nil
	}
	schemas, err := os.ReadDir(e.getDataDir())
	if err != nil {
		return fmt.Errorf("read data directory metadata failed: %v", err)
	}
	for _, schemaEntry := range schemas {
		if !schemaEntry.IsDir() {
			continue
		}
		childDB := schemaEntry.Name()
		entries, readErr := os.ReadDir(filepath.Join(e.getDataDir(), childDB))
		if readErr != nil {
			return readErr
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.EqualFold(filepath.Ext(entry.Name()), ".frm") {
				continue
			}
			foreignTableName := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
			foreignTable, readErr := readTableMetadataMap(filepath.Join(e.getDataDir(), childDB, entry.Name()))
			if readErr != nil {
				return readErr
			}
			foreignKeys, _ := foreignTable["foreign_keys"].([]interface{})
			for _, rawForeignKey := range foreignKeys {
				foreignKey, ok := rawForeignKey.(map[string]interface{})
				if !ok || !foreignKeyReferencesParent(foreignKey, childDB, dbName, tableName) {
					continue
				}
				refColumns, _ := foreignKey["ref_columns"].([]interface{})
				for _, rawColumn := range refColumns {
					if strings.EqualFold(strings.Trim(strings.TrimSpace(fmt.Sprint(rawColumn)), "`"), droppedColumn) {
						return fmt.Errorf("cannot drop column '%s': foreign key constraint '%s' in table '%s.%s' references it", droppedColumn, foreignKey["name"], childDB, foreignTableName)
					}
				}
			}
		}
	}
	return nil
}

func (e *XMySQLExecutor) validateForeignKeyDependenciesBeforeTableDrop(dbName, tableName string) error {
	if e == nil {
		return nil
	}
	schemas, err := os.ReadDir(e.getDataDir())
	if err != nil {
		return fmt.Errorf("read data directory metadata failed: %v", err)
	}
	for _, schemaEntry := range schemas {
		if !schemaEntry.IsDir() {
			continue
		}
		childDB := schemaEntry.Name()
		entries, readErr := os.ReadDir(filepath.Join(e.getDataDir(), childDB))
		if readErr != nil {
			return readErr
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.EqualFold(filepath.Ext(entry.Name()), ".frm") {
				continue
			}
			foreignTableName := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
			if strings.EqualFold(childDB, dbName) && strings.EqualFold(foreignTableName, tableName) {
				continue
			}
			foreignTable, readErr := readTableMetadataMap(filepath.Join(e.getDataDir(), childDB, entry.Name()))
			if readErr != nil {
				return readErr
			}
			foreignKeys, _ := foreignTable["foreign_keys"].([]interface{})
			for _, rawForeignKey := range foreignKeys {
				foreignKey, ok := rawForeignKey.(map[string]interface{})
				if ok && foreignKeyReferencesParent(foreignKey, childDB, dbName, tableName) {
					return fmt.Errorf("cannot drop table '%s': foreign key constraint '%s' in table '%s.%s' references it", tableName, foreignKey["name"], childDB, foreignTableName)
				}
			}
		}
	}
	return nil
}

func (e *XMySQLExecutor) validateForeignKeyDependenciesBeforeTableTruncate(dbName, tableName string) error {
	if e == nil {
		return nil
	}
	schemas, err := os.ReadDir(e.getDataDir())
	if err != nil {
		return fmt.Errorf("read data directory metadata failed: %v", err)
	}
	for _, schemaEntry := range schemas {
		if !schemaEntry.IsDir() {
			continue
		}
		childDB := schemaEntry.Name()
		entries, readErr := os.ReadDir(filepath.Join(e.getDataDir(), childDB))
		if readErr != nil {
			return readErr
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.EqualFold(filepath.Ext(entry.Name()), ".frm") {
				continue
			}
			foreignTableName := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
			foreignTable, readErr := readTableMetadataMap(filepath.Join(e.getDataDir(), childDB, entry.Name()))
			if readErr != nil {
				return readErr
			}
			foreignKeys, _ := foreignTable["foreign_keys"].([]interface{})
			for _, rawForeignKey := range foreignKeys {
				foreignKey, ok := rawForeignKey.(map[string]interface{})
				if ok && foreignKeyReferencesParent(foreignKey, childDB, dbName, tableName) {
					return fmt.Errorf("cannot truncate table '%s.%s': foreign key constraint '%s' in table '%s.%s' references it", dbName, tableName, foreignKey["name"], childDB, foreignTableName)
				}
			}
		}
	}
	return nil
}

func (e *XMySQLExecutor) validateReferencedForeignKeyIndexDrop(dbName, tableName, indexName string, parentTableInfo map[string]interface{}) error {
	if e == nil {
		return nil
	}
	indexes, _ := parentTableInfo["indexes"].([]interface{})
	var targetColumns []string
	for _, rawIndex := range indexes {
		index, ok := rawIndex.(map[string]interface{})
		if ok && strings.EqualFold(fmt.Sprint(index["name"]), indexName) {
			targetColumns = metadataIdentifierList(index["columns"])
			break
		}
	}
	if len(targetColumns) == 0 {
		return nil
	}
	schemas, err := os.ReadDir(e.getDataDir())
	if err != nil {
		return fmt.Errorf("read data directory metadata failed: %v", err)
	}
	for _, schemaEntry := range schemas {
		if !schemaEntry.IsDir() {
			continue
		}
		childDB := schemaEntry.Name()
		entries, readErr := os.ReadDir(filepath.Join(e.getDataDir(), childDB))
		if readErr != nil {
			return readErr
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.EqualFold(filepath.Ext(entry.Name()), ".frm") {
				continue
			}
			foreignTableName := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
			foreignTable, readErr := readTableMetadataMap(filepath.Join(e.getDataDir(), childDB, entry.Name()))
			if readErr != nil {
				return readErr
			}
			foreignKeys, _ := foreignTable["foreign_keys"].([]interface{})
			for _, rawForeignKey := range foreignKeys {
				foreignKey, ok := rawForeignKey.(map[string]interface{})
				if !ok || !foreignKeyReferencesParent(foreignKey, childDB, dbName, tableName) {
					continue
				}
				refColumns := metadataIdentifierList(foreignKey["ref_columns"])
				if !identifierPrefixMatches(targetColumns, refColumns) {
					continue
				}
				coveredByOtherIndex := false
				for _, rawIndex := range indexes {
					index, ok := rawIndex.(map[string]interface{})
					if !ok || strings.EqualFold(fmt.Sprint(index["name"]), indexName) {
						continue
					}
					if identifierPrefixMatches(metadataIdentifierList(index["columns"]), refColumns) {
						coveredByOtherIndex = true
						break
					}
				}
				if !coveredByOtherIndex {
					return fmt.Errorf("cannot drop index '%s': foreign key constraint '%s' in table '%s.%s' references it", indexName, foreignKey["name"], childDB, foreignTableName)
				}
			}
		}
	}
	return nil
}

type renameTablePair struct {
	sourceDB       string
	sourceTable    string
	destinationDB  string
	destinationTbl string
}

type dropTableTarget struct {
	databaseName string
	tableName    string
}

// parseDropTableTargets handles the multi-target DROP TABLE form that the
// legacy parser does not represent in its DDL AST. Single-target statements
// deliberately fall through to executeDropTableStatement so their existing
// result and temporary-table behavior remains unchanged.
func parseDropTableTargets(query, currentDB string) ([]dropTableTarget, bool, bool, error) {
	trimmed := strings.TrimSpace(query)
	trimmed = strings.TrimSuffix(trimmed, ";")
	prefixPattern := regexp.MustCompile(`(?is)^drop\s+table\s+(?:(if\s+exists)\s+)?(.+)$`)
	match := prefixPattern.FindStringSubmatch(trimmed)
	if len(match) != 3 {
		return nil, false, false, nil
	}
	parts := splitTopLevelComma(match[2])
	if len(parts) <= 1 {
		return nil, false, false, nil
	}
	namePattern := regexp.MustCompile(`(?is)^(?:\x60?([a-zA-Z0-9_$]+)\x60?\.)?\x60?([a-zA-Z0-9_$]+)\x60?$`)
	targets := make([]dropTableTarget, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		nameMatch := namePattern.FindStringSubmatch(strings.TrimSpace(part))
		if len(nameMatch) != 3 {
			return nil, true, true, fmt.Errorf("invalid DROP TABLE target %q", strings.TrimSpace(part))
		}
		databaseName, tableName := nameMatch[1], nameMatch[2]
		if databaseName == "" {
			databaseName = currentDB
		}
		if databaseName == "" {
			return nil, true, true, fmt.Errorf("no database selected")
		}
		key := strings.ToLower(databaseName + "." + tableName)
		if _, exists := seen[key]; exists {
			return nil, true, true, fmt.Errorf("DROP TABLE target appears more than once: %s", key)
		}
		seen[key] = struct{}{}
		targets = append(targets, dropTableTarget{databaseName: databaseName, tableName: tableName})
	}
	return targets, strings.TrimSpace(match[1]) != "", true, nil
}

func parseDropViewTargets(query, currentDB string) ([]dropTableTarget, bool, bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	prefixPattern := regexp.MustCompile(`(?is)^drop\s+view\s+(?:(if\s+exists)\s+)?(.+)$`)
	match := prefixPattern.FindStringSubmatch(trimmed)
	if len(match) != 3 {
		return nil, false, false, nil
	}
	parts := splitTopLevelComma(match[2])
	if len(parts) == 0 {
		return nil, false, true, fmt.Errorf("DROP VIEW requires a view name")
	}
	targets := make([]dropTableTarget, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		databaseName, tableName := compatibilityQualifiedTable(part, currentDB)
		if databaseName == "" || tableName == "" {
			return nil, false, true, fmt.Errorf("DROP VIEW requires valid view names")
		}
		key := strings.ToLower(databaseName + "." + tableName)
		if _, exists := seen[key]; exists {
			return nil, false, true, fmt.Errorf("DROP VIEW target appears more than once: %s", key)
		}
		seen[key] = struct{}{}
		targets = append(targets, dropTableTarget{databaseName: databaseName, tableName: tableName})
	}
	return targets, strings.TrimSpace(match[1]) != "", true, nil
}

// executeRawDropTableCompatibility performs statement-wide validation before
// deleting any target. This gives multi-table DROP the important MySQL
// behavior that a missing target or dependency does not leave earlier targets
// already removed. A storage failure after validation is still surfaced; the
// underlying drop implementation is not transactional across independent
// tables.
func (e *XMySQLExecutor) executeRawDropTableCompatibility(ctx *ExecutionContext, query, currentDB string, mysqlSession server.MySQLServerSession) (bool, int, error) {
	targets, ifExists, matched, err := parseDropTableTargets(query, currentDB)
	if !matched {
		return false, 0, nil
	}
	if err != nil {
		return true, 0, err
	}
	for _, target := range targets {
		if err := e.checkTablePrivilege(ctx, target.databaseName, target.tableName, "DROP"); err != nil {
			return true, 0, err
		}
	}
	present := make([]dropTableTarget, 0, len(targets))
	for _, target := range targets {
		if err := e.validateDatabaseExists(target.databaseName); err != nil {
			return true, 0, err
		}
		exists, err := e.checkTableExists(target.databaseName, target.tableName)
		if err != nil {
			return true, 0, err
		}
		if !exists {
			if ifExists {
				continue
			}
			return true, 0, fmt.Errorf("table '%s' does not exist", target.tableName)
		}
		present = append(present, target)
	}
	for _, target := range present {
		if sessionForeignKeyChecksEnabled(mysqlSession) {
			if err := e.validateForeignKeyDependenciesBeforeTableDrop(target.databaseName, target.tableName); err != nil {
				return true, 0, err
			}
		}
		if dependency := e.findStoredObjectTableDependency(target.databaseName, target.tableName); dependency != "" {
			return true, 0, fmt.Errorf("cannot drop table '%s.%s': it is referenced by %s", target.databaseName, target.tableName, dependency)
		}
	}
	if sessionBoolParam(mysqlSession, "in_transaction") || sessionHasTransactionTableLocks(mysqlSession) {
		if err := e.commitAutocommitTransaction(mysqlSession); err != nil {
			return true, 0, err
		}
	}
	lockTargets := append([]dropTableTarget(nil), present...)
	sort.SliceStable(lockTargets, func(i, j int) bool {
		left := strings.ToLower(lockTargets[i].databaseName + "." + lockTargets[i].tableName)
		right := strings.ToLower(lockTargets[j].databaseName + "." + lockTargets[j].tableName)
		return left < right
	})
	releases := make([]func(), 0, len(lockTargets))
	for _, target := range lockTargets {
		tableKey := strings.ToLower(target.databaseName + "." + target.tableName)
		if sessionHoldsTableLock(mysqlSession, tableKey) {
			continue
		}
		release, lockErr := e.acquireDDLWriteLock(ctx, tableKey, false)
		if lockErr != nil {
			for i := len(releases) - 1; i >= 0; i-- {
				releases[i]()
			}
			return true, 0, tableLockWaitError(lockErr)
		}
		releases = append(releases, release)
	}
	defer func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}()
	for _, target := range present {
		if err := e.dropTableImpl(target.databaseName, target.tableName); err != nil {
			return true, 0, err
		}
	}
	return true, len(present), nil
}

func parseRenameTablePairs(query, currentDB string) ([]renameTablePair, bool, error) {
	trimmed := strings.TrimSpace(query)
	prefixPattern := regexp.MustCompile(`(?is)^rename\s+table(?:\s|$)`)
	if !prefixPattern.MatchString(trimmed) {
		return nil, false, nil
	}
	rest := strings.TrimSpace(prefixPattern.ReplaceAllString(trimmed, ""))
	if rest == "" {
		return nil, true, fmt.Errorf("RENAME TABLE requires at least one table pair")
	}
	pairPattern := regexp.MustCompile(`(?is)^(?:\x60?([a-zA-Z0-9_$]+)\x60?\.)?\x60?([a-zA-Z0-9_$]+)\x60?\s+to\s+(?:\x60?([a-zA-Z0-9_$]+)\x60?\.)?\x60?([a-zA-Z0-9_$]+)\x60?$`)
	parts := splitTopLevelComma(rest)
	pairs := make([]renameTablePair, 0, len(parts))
	seenSources := make(map[string]struct{}, len(parts))
	seenTargets := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		match := pairPattern.FindStringSubmatch(strings.TrimSpace(part))
		if len(match) != 5 {
			return nil, true, fmt.Errorf("invalid RENAME TABLE pair %q", strings.TrimSpace(part))
		}
		sourceDB, destinationDB := match[1], match[3]
		if sourceDB == "" {
			sourceDB = currentDB
		}
		if destinationDB == "" {
			destinationDB = currentDB
		}
		pair := renameTablePair{sourceDB: sourceDB, sourceTable: match[2], destinationDB: destinationDB, destinationTbl: match[4]}
		sourceKey := strings.ToLower(sourceDB + "." + match[2])
		targetKey := strings.ToLower(destinationDB + "." + match[4])
		if _, exists := seenSources[sourceKey]; exists {
			return nil, true, fmt.Errorf("RENAME TABLE source appears more than once: %s", sourceKey)
		}
		if _, exists := seenTargets[targetKey]; exists {
			return nil, true, fmt.Errorf("RENAME TABLE target appears more than once: %s", targetKey)
		}
		seenSources[sourceKey] = struct{}{}
		seenTargets[targetKey] = struct{}{}
		pairs = append(pairs, pair)
	}
	return pairs, true, nil
}

func (e *XMySQLExecutor) renameTablesAtomically(pairs []renameTablePair) error {
	if len(pairs) == 0 {
		return fmt.Errorf("RENAME TABLE requires at least one table pair")
	}
	for _, pair := range pairs {
		if err := e.validateDatabaseExists(pair.sourceDB); err != nil {
			return err
		}
		if err := e.validateDatabaseExists(pair.destinationDB); err != nil {
			return err
		}
		exists, err := e.checkTableOrViewExists(pair.sourceDB, pair.sourceTable)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("table '%s.%s' does not exist", pair.sourceDB, pair.sourceTable)
		}
		targetExists, err := e.checkTableOrViewExists(pair.destinationDB, pair.destinationTbl)
		if err != nil {
			return err
		}
		if targetExists && !(strings.EqualFold(pair.sourceDB, pair.destinationDB) && strings.EqualFold(pair.sourceTable, pair.destinationTbl)) {
			return fmt.Errorf("table '%s' already exists", pair.destinationTbl)
		}
	}
	done := make([]renameTablePair, 0, len(pairs))
	for _, pair := range pairs {
		if err := e.renameTableMetadataAcrossSchemas(pair.sourceDB, pair.sourceTable, pair.destinationDB, pair.destinationTbl); err != nil {
			for index := len(done) - 1; index >= 0; index-- {
				completed := done[index]
				if rollbackErr := e.renameTableMetadataAcrossSchemas(completed.destinationDB, completed.destinationTbl, completed.sourceDB, completed.sourceTable); rollbackErr != nil {
					return fmt.Errorf("rename table failed: %v; rollback failed: %v", err, rollbackErr)
				}
			}
			return err
		}
		done = append(done, pair)
	}
	return nil
}

func (e *XMySQLExecutor) renameTableMetadata(dbName, oldName, newName string) error {
	return e.renameTableMetadataAcrossSchemas(dbName, oldName, dbName, newName)
}

func (e *XMySQLExecutor) renameTableMetadataAcrossSchemas(sourceDB, oldName, destinationDB, newName string) error {
	if sourceDB == "" || destinationDB == "" || oldName == "" || newName == "" {
		return fmt.Errorf("rename table requires database and table names")
	}
	if strings.EqualFold(sourceDB, destinationDB) && strings.EqualFold(oldName, newName) {
		return nil
	}
	viewPath := filepath.Join(e.getDataDir(), sourceDB, oldName+".view.json")
	if _, err := os.Stat(viewPath); err == nil {
		if !strings.EqualFold(sourceDB, destinationDB) {
			return fmt.Errorf("RENAME TABLE cannot move view '%s.%s' across databases", sourceDB, oldName)
		}
		return e.renameViewDefinition(sourceDB, oldName, newName)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("check source view definition failed: %v", err)
	}
	sourcePath := filepath.Join(e.getDataDir(), sourceDB)
	destinationPath := filepath.Join(e.getDataDir(), destinationDB)
	oldFrm := filepath.Join(sourcePath, oldName+".frm")
	newFrm := filepath.Join(destinationPath, newName+".frm")
	oldIbd := filepath.Join(sourcePath, oldName+".ibd")
	newIbd := filepath.Join(destinationPath, newName+".ibd")
	if _, err := os.Stat(newFrm); err == nil {
		return fmt.Errorf("table '%s' already exists", newName)
	}
	if _, err := os.Stat(newIbd); err == nil {
		return fmt.Errorf("table '%s' already exists", newName)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("check destination table data file failed: %v", err)
	}
	oldIbdExists := false
	if _, err := os.Stat(oldIbd); err == nil {
		oldIbdExists = true
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("check source table data file failed: %v", err)
	}
	tableInfo, err := readTableMetadataMap(oldFrm)
	if err != nil {
		return err
	}
	originalData, err := json.MarshalIndent(tableInfo, "", "  ")
	if err != nil {
		return err
	}
	tableInfo["table_name"] = newName
	tableInfo["schema_name"] = destinationDB
	data, err := json.MarshalIndent(tableInfo, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(destinationPath, 0755); err != nil {
		return fmt.Errorf("create destination schema directory failed: %v", err)
	}
	// Write the new dictionary before removing the old one. If the rename
	// fails, the original table remains available and the temp file is removed.
	if err := writeMetadataFileAtomic(newFrm, data); err != nil {
		return fmt.Errorf("write renamed table metadata failed: %v", err)
	}
	if err := os.Remove(oldFrm); err != nil {
		_ = os.Remove(newFrm)
		return fmt.Errorf("remove old table metadata failed: %v", err)
	}
	ibdMoved := false
	managedTablespaceRenamed := false
	renameIbd := func() error {
		if !oldIbdExists {
			return nil
		}
		oldTablespaceName := filepath.ToSlash(filepath.Join(sourceDB, oldName))
		newTablespaceName := filepath.ToSlash(filepath.Join(destinationDB, newName))
		if e.storageManager != nil {
			if err := e.storageManager.RenameTablespace(oldTablespaceName, newTablespaceName); err == nil {
				managedTablespaceRenamed = true
				ibdMoved = true
				return nil
			} else if !strings.Contains(strings.ToLower(err.Error()), "not found") {
				return err
			}
		}
		if err := os.Rename(oldIbd, newIbd); err != nil {
			return err
		}
		ibdMoved = true
		return nil
	}
	restoreRenamedFiles := func() {
		if ibdMoved {
			if managedTablespaceRenamed && e.storageManager != nil {
				oldTablespaceName := filepath.ToSlash(filepath.Join(sourceDB, oldName))
				newTablespaceName := filepath.ToSlash(filepath.Join(destinationDB, newName))
				_ = e.storageManager.RenameTablespace(newTablespaceName, oldTablespaceName)
			} else {
				_ = os.Rename(newIbd, oldIbd)
			}
		}
		_ = os.Remove(newFrm)
		_ = os.WriteFile(oldFrm, originalData, 0644)
	}
	// The integrated mapping is a logical name map over the same space.
	if e.tableStorageManager != nil {
		if info, lookupErr := e.tableStorageManager.GetTableStorageInfo(sourceDB, oldName); lookupErr == nil {
			infoCopy := *info
			infoCopy.Partitions = append([]manager.PartitionStorageInfo(nil), info.Partitions...)
			infoCopy.SchemaName = destinationDB
			infoCopy.TableName = newName
			oldTablespaceName := filepath.ToSlash(filepath.Join(sourceDB, oldName))
			newTablespaceName := filepath.ToSlash(filepath.Join(destinationDB, newName))
			if infoCopy.OwnsTablespace && strings.EqualFold(strings.TrimSpace(infoCopy.TablespaceName), oldTablespaceName) {
				infoCopy.TablespaceName = newTablespaceName
			}
			type renamedPartitionTablespace struct {
				oldName string
				newName string
			}
			renamedPartitionSpaces := make([]renamedPartitionTablespace, 0, len(info.Partitions))
			for index := range infoCopy.Partitions {
				partition := &infoCopy.Partitions[index]
				originalPartition := info.Partitions[index]
				oldPartitionName := filepath.ToSlash(filepath.Join(sourceDB, oldName+"#"+partition.Name))
				newPartitionName := filepath.ToSlash(filepath.Join(destinationDB, newName+"#"+partition.Name))
				if partition.OwnsTablespace && (strings.TrimSpace(partition.TablespaceName) == "" || strings.EqualFold(strings.TrimSpace(partition.TablespaceName), oldPartitionName)) {
					partition.TablespaceName = newPartitionName
				}
				if originalPartition.OwnsTablespace && strings.EqualFold(strings.TrimSpace(originalPartition.TablespaceName), oldPartitionName) && !strings.EqualFold(oldPartitionName, newPartitionName) {
					renamedPartitionSpaces = append(renamedPartitionSpaces, renamedPartitionTablespace{oldName: oldPartitionName, newName: newPartitionName})
				}
			}
			restorePartitionSpaces := func() {
				if e.storageManager == nil {
					return
				}
				for index := len(renamedPartitionSpaces) - 1; index >= 0; index-- {
					move := renamedPartitionSpaces[index]
					_ = e.storageManager.RenameTablespace(move.newName, move.oldName)
				}
			}
			if e.storageManager != nil {
				for _, move := range renamedPartitionSpaces {
					if err := e.storageManager.RenameTablespace(move.oldName, move.newName); err != nil {
						restorePartitionSpaces()
						restoreRenamedFiles()
						return fmt.Errorf("move partition data file failed: %v", err)
					}
				}
			}
			if unregisterErr := e.tableStorageManager.UnregisterTable(sourceDB, oldName); unregisterErr != nil {
				restorePartitionSpaces()
				restoreRenamedFiles()
				return fmt.Errorf("rename storage mapping failed: %v", unregisterErr)
			}
			if err := renameIbd(); err != nil {
				restorePartitionSpaces()
				_ = e.tableStorageManager.RegisterTable(context.Background(), info)
				restoreRenamedFiles()
				return fmt.Errorf("move table data file failed: %v", err)
			}
			if registerErr := e.tableStorageManager.RegisterTable(context.Background(), &infoCopy); registerErr != nil {
				restorePartitionSpaces()
				_ = e.tableStorageManager.RegisterTable(context.Background(), info)
				restoreRenamedFiles()
				return fmt.Errorf("rename storage mapping failed: %v", registerErr)
			}
			if persistErr := persistTableStorageIdentity(e.getDataDir(), &infoCopy); persistErr != nil {
				restorePartitionSpaces()
				_ = e.tableStorageManager.UnregisterTable(destinationDB, newName)
				_ = e.tableStorageManager.RegisterTable(context.Background(), info)
				restoreRenamedFiles()
				return fmt.Errorf("persist renamed storage mapping failed: %v", persistErr)
			}
			if len(infoCopy.Partitions) > 0 {
				if persistErr := persistPartitionStorageRoots(e.getDataDir(), destinationDB, newName, e.tableStorageManager); persistErr != nil {
					restorePartitionSpaces()
					_ = e.tableStorageManager.UnregisterTable(destinationDB, newName)
					_ = e.tableStorageManager.RegisterTable(context.Background(), info)
					restoreRenamedFiles()
					return fmt.Errorf("persist renamed partition mapping failed: %v", persistErr)
				}
			}
		}
	}
	if e.tableStorageManager == nil {
		if err := renameIbd(); err != nil {
			restoreRenamedFiles()
			return fmt.Errorf("move table data file failed: %v", err)
		}
	}
	if err := renameReferencedForeignKeyTable(e.getDataDir(), sourceDB, oldName, destinationDB, newName); err != nil {
		return fmt.Errorf("update foreign-key references after rename failed: %v", err)
	}
	if e.infosSchemaManager != nil {
		dropCtx := context.Background()
		if invalidator, ok := e.tableManager.(interface {
			InvalidateTableMetadata(string, string) error
		}); ok {
			if err := invalidator.InvalidateTableMetadata(sourceDB, oldName); err != nil {
				return fmt.Errorf("invalidate source table-manager metadata after rename failed: %v", err)
			}
		}
		// A dictionary-backed table may also be present.  The .frm-backed
		// executor path normally has no dictionary entry, so a not-found result
		// is expected and harmless here.
		if err := e.infosSchemaManager.DropTable(dropCtx, sourceDB, oldName); err != nil && !strings.Contains(strings.ToLower(err.Error()), "not found") {
			return fmt.Errorf("remove source dictionary entry after rename failed: %v", err)
		}
		refreshCtx := context.Background()
		if err := e.infosSchemaManager.RefreshMetadata(refreshCtx, sourceDB); err != nil {
			return fmt.Errorf("refresh source schema metadata after rename failed: %v", err)
		}
		if !strings.EqualFold(sourceDB, destinationDB) {
			if err := e.infosSchemaManager.RefreshMetadata(refreshCtx, destinationDB); err != nil {
				return fmt.Errorf("refresh destination schema metadata after rename failed: %v", err)
			}
		}
	}
	return nil
}

func (e *XMySQLExecutor) renameViewDefinition(databaseName, oldName, newName string) error {
	sourcePath := filepath.Join(e.getDataDir(), databaseName, oldName+".view.json")
	targetPath := filepath.Join(e.getDataDir(), databaseName, newName+".view.json")
	if _, err := os.Stat(targetPath); err == nil {
		return fmt.Errorf("table '%s' already exists", newName)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("check destination view definition failed: %v", err)
	}
	raw, err := os.ReadFile(sourcePath)
	if err != nil {
		return fmt.Errorf("read view definition failed: %v", err)
	}
	var viewInfo map[string]interface{}
	if err := json.Unmarshal(raw, &viewInfo); err != nil {
		return fmt.Errorf("parse view definition failed: %v", err)
	}
	viewInfo["view_name"] = newName
	viewInfo["schema"] = databaseName
	data, err := json.MarshalIndent(viewInfo, "", "  ")
	if err != nil {
		return fmt.Errorf("serialize renamed view definition failed: %v", err)
	}
	if err := writeMetadataFileAtomic(targetPath, data); err != nil {
		return fmt.Errorf("write renamed view definition failed: %v", err)
	}
	if err := os.Remove(sourcePath); err != nil {
		_ = os.Remove(targetPath)
		return fmt.Errorf("remove old view definition failed: %v", err)
	}
	return nil
}

func readTableMetadataMap(path string) (map[string]interface{}, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read table metadata failed: %v", err)
	}
	var tableInfo map[string]interface{}
	if err := json.Unmarshal(raw, &tableInfo); err != nil {
		return nil, fmt.Errorf("parse table metadata failed: %v", err)
	}
	return tableInfo, nil
}

func writeTableMetadataMapAtomic(path string, tableInfo map[string]interface{}) error {
	data, err := json.MarshalIndent(tableInfo, "", "  ")
	if err != nil {
		return fmt.Errorf("serialize table metadata failed: %v", err)
	}
	if err := writeMetadataFileAtomic(path, data); err != nil {
		return fmt.Errorf("write table metadata failed: %v", err)
	}
	return nil
}

func (e *XMySQLExecutor) normalizeDBDDLOptions(stmt *sqlparser.DBDDL) {
	if stmt == nil {
		return
	}
}

// executeDDL 处理 DDL 类型语句，如 CREATE TABLE, DROP TABLE
func (e *XMySQLExecutor) executeDDL(stmt *sqlparser.DDL, mysqlSession server.MySQLServerSession, databaseName string, results chan *Result, rawQuery ...string) {
	query := ""
	if len(rawQuery) > 0 {
		query = rawQuery[0]
	}
	// 创建执行上下文
	ctx := &ExecutionContext{
		Context:     context.Background(),
		Session:     mysqlSession,
		statementId: 0,
		QueryId:     0,
		Results:     results,
		Cfg:         e.conf,
		RawQuery:    query,
	}
	// 从会话中获取当前数据库
	currentDB := databaseName
	if currentDB == "" && mysqlSession != nil {
		if dbParam := mysqlSession.GetParamByName("database"); dbParam != nil {
			if db, ok := dbParam.(string); ok {
				currentDB = db
			}
		}
	}
	if stmt != nil {
		release, err := e.acquireSimpleDDLTableLock(ctx, mysqlSession, stmt.Table, stmt.Action, currentDB)
		if err != nil {
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: err.Error()}
			return
		}
		defer release()
	}

	switch stmt.Action {
	case "create":
		logger.Debugf(" CREATE TABLE使用数据库: %s", currentDB)
		e.executeCreateTableStatement(ctx, currentDB, stmt)
	case "drop":
		logger.Debugf("🗑️ DROP TABLE使用数据库: %s", currentDB)
		e.executeDropTableStatement(ctx, currentDB, stmt, mysqlSession)
	case "truncate":
		logger.Debugf("TRUNCATE TABLE使用数据库: %s", currentDB)
		e.executeTruncateTableStatement(ctx, currentDB, stmt)
	case "alter":
		logger.Debugf("ALTER TABLE使用数据库: %s", currentDB)
		e.executeAlterTableStatement(ctx, currentDB, stmt, query)
	default:
		results <- &Result{
			Err:        newExecutorErrorf("ddl-action", ExecutionErrorCodeValidation, currentDB, "", "", fmt.Errorf("unsupported DDL action: %s", stmt.Action), "unsupported DDL action"),
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("Unsupported DDL action: %s", stmt.Action),
		}
	}
}

func (e *XMySQLExecutor) executeAlterTableStatement(ctx *ExecutionContext, currentDB string, stmt *sqlparser.DDL, rawQuery ...string) {
	tableName := stmt.Table.Name.String()
	databaseName := stmt.Table.Qualifier.String()
	if databaseName == "" {
		databaseName = currentDB
	}
	if databaseName == "" || tableName == "" {
		ctx.Results <- &Result{Err: fmt.Errorf("ALTER TABLE requires database and table"), ResultType: common.RESULT_TYPE_DDL}
		return
	}
	if err := e.checkAlterTablePrivileges(ctx, databaseName, tableName); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("ALTER TABLE failed: %v", err)}
		return
	}
	if len(rawQuery) > 0 {
		foreignKeyChecksEnabled := sessionForeignKeyChecksEnabled(ctx.Session)
		if err := e.checkAlterForeignKeyPrivileges(ctx, databaseName, rawQuery[0]); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("ALTER TABLE failed: %v", err)}
			return
		}
		handled, err := e.alterTableIndexDDL(databaseName, tableName, rawQuery[0], foreignKeyChecksEnabled)
		if handled {
			if err != nil {
				ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("ALTER TABLE failed: %v", err)}
				return
			}
			ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Table '%s' altered successfully", tableName)}
			return
		}
		handled, err = e.alterTableColumnDDL(databaseName, tableName, rawQuery[0], foreignKeyChecksEnabled)
		if handled {
			if err != nil {
				ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("ALTER TABLE failed: %v", err)}
				return
			}
			ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Table '%s' altered successfully", tableName)}
			return
		}
	}
	if stmt.TableSpec == nil || len(stmt.TableSpec.Columns) == 0 {
		ctx.Results <- &Result{Err: fmt.Errorf("unsupported ALTER TABLE action"), ResultType: common.RESULT_TYPE_DDL}
		return
	}
	if err := e.alterTableAddColumns(databaseName, tableName, stmt.TableSpec.Columns); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("ALTER TABLE failed: %v", err)}
		return
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Table '%s' altered successfully", tableName)}
}

func (e *XMySQLExecutor) alterTableIndexDDL(dbName, tableName, query string, foreignKeyChecksEnabled bool) (bool, error) {
	addPattern := regexp.MustCompile(`(?is)^\s*alter\s+table\s+.*?\s+add\s+(unique\s+)?(?:index|key)\s+` + "`?([a-zA-Z0-9_$]+)`?" + `\s*\(([^)]+)\)(?:\s+(visible|invisible))?\s*$`)
	spatialAddPattern := regexp.MustCompile(`(?is)^\s*alter\s+table\s+.*?\s+add\s+spatial\s+(?:index|key)\s+` + "`?([a-zA-Z0-9_$]+)`?" + `\s*\(([^)]+)\)\s*$`)
	dropPattern := regexp.MustCompile(`(?is)^\s*alter\s+table\s+.*?\s+drop\s+(?:index|key)\s+` + "`?([a-zA-Z0-9_$]+)`?")
	renamePattern := regexp.MustCompile(`(?is)^\s*alter\s+table\s+.*?\s+rename\s+(?:index|key)\s+` + "`?([a-zA-Z0-9_$]+)`?" + `\s+to\s+` + "`?([a-zA-Z0-9_$]+)`?")
	add := addPattern.FindStringSubmatch(query)
	spatialAdd := spatialAddPattern.FindStringSubmatch(query)
	drop := dropPattern.FindStringSubmatch(query)
	rename := renamePattern.FindStringSubmatch(query)
	if len(add) == 0 && len(spatialAdd) == 0 && len(drop) == 0 && len(rename) == 0 {
		return false, nil
	}
	frmPath := filepath.Join(e.getDataDir(), dbName, tableName+".frm")
	raw, err := os.ReadFile(frmPath)
	if err != nil {
		return true, fmt.Errorf("read table metadata failed: %v", err)
	}
	var tableInfo map[string]interface{}
	if err := json.Unmarshal(raw, &tableInfo); err != nil {
		return true, fmt.Errorf("parse table metadata failed: %v", err)
	}
	indexes, _ := tableInfo["indexes"].([]interface{})
	if len(add) > 0 || len(spatialAdd) > 0 {
		name := ""
		columnsText := ""
		indexType := "INDEX"
		unique := false
		advanced := false
		if len(spatialAdd) > 0 {
			name = spatialAdd[1]
			columnsText = spatialAdd[2]
			indexType = "SPATIAL"
			advanced = true
		} else {
			name = add[2]
			columnsText = add[3]
			if strings.TrimSpace(add[1]) != "" {
				indexType = "UNIQUE INDEX"
				unique = true
			}
		}
		for _, value := range indexes {
			if index, ok := value.(map[string]interface{}); ok && strings.EqualFold(fmt.Sprint(index["name"]), name) {
				return true, fmt.Errorf("duplicate index '%s'", name)
			}
		}
		columns := parseIdentifierList(columnsText)
		if len(columns) == 0 {
			return true, fmt.Errorf("index '%s' must contain at least one column", name)
		}
		indexes = append(indexes, map[string]interface{}{
			"name":     name,
			"type":     indexType,
			"unique":   unique,
			"primary":  false,
			"columns":  columns,
			"advanced": advanced,
		})
		if len(add) > 0 && strings.TrimSpace(add[4]) != "" {
			added := indexes[len(indexes)-1].(map[string]interface{})
			added["visible"] = !strings.EqualFold(strings.TrimSpace(add[4]), "invisible")
			added["visibility_set"] = true
		}
	} else if len(drop) > 0 {
		name := drop[1]
		filtered := make([]interface{}, 0, len(indexes))
		found := false
		for _, value := range indexes {
			index, ok := value.(map[string]interface{})
			if ok && strings.EqualFold(fmt.Sprint(index["name"]), name) {
				if primary, _ := index["primary"].(bool); primary {
					return true, fmt.Errorf("cannot drop primary key index")
				}
				found = true
				continue
			}
			filtered = append(filtered, value)
		}
		if !found {
			return true, fmt.Errorf("index '%s' does not exist", name)
		}
		if foreignKeyChecksEnabled {
			if err := validateForeignKeyIndexDrop(tableInfo, name); err != nil {
				return true, err
			}
			if err := e.validateReferencedForeignKeyIndexDrop(dbName, tableName, name, tableInfo); err != nil {
				return true, err
			}
		}
		indexes = filtered
		delete(tableInfo, "fulltext_segments_"+name)
		delete(tableInfo, "spatial_entries_"+name)
	} else {
		oldName, newName := rename[1], rename[2]
		if !strings.EqualFold(oldName, newName) {
			for _, value := range indexes {
				index, ok := value.(map[string]interface{})
				if ok && strings.EqualFold(fmt.Sprint(index["name"]), newName) {
					return true, fmt.Errorf("duplicate index '%s'", newName)
				}
			}
		}
		found := false
		for _, value := range indexes {
			if index, ok := value.(map[string]interface{}); ok && strings.EqualFold(fmt.Sprint(index["name"]), oldName) {
				index["name"] = newName
				found = true
			}
		}
		if !found {
			return true, fmt.Errorf("index '%s' does not exist", oldName)
		}
		if state, exists := tableInfo["fulltext_segments_"+oldName]; exists {
			delete(tableInfo, "fulltext_segments_"+oldName)
			tableInfo["fulltext_segments_"+newName] = state
		}
		if state, exists := tableInfo["spatial_entries_"+oldName]; exists {
			delete(tableInfo, "spatial_entries_"+oldName)
			tableInfo["spatial_entries_"+newName] = state
		}
	}
	tableInfo["indexes"] = indexes
	out, err := json.MarshalIndent(tableInfo, "", "  ")
	if err != nil {
		return true, fmt.Errorf("serialize table metadata failed: %v", err)
	}
	if err := writeMetadataFileAtomic(frmPath, out); err != nil {
		return true, fmt.Errorf("write table metadata failed: %v", err)
	}
	if err := e.refreshAlteredSecondaryIndexes(dbName, tableName); err != nil {
		return true, err
	}
	if len(spatialAdd) > 0 {
		if err := e.refreshSpatialIndexStateForTable(dbName, tableName); err != nil {
			return true, err
		}
	}
	return true, nil
}

// refreshAlteredSecondaryIndexes closes the ALTER->DML timing window. Newly
// added indexes must contain pre-existing clustered rows before the next DML
// appends its row; otherwise the first write after ALTER can leave a partial
// index that only a later read happens to repair.
func (e *XMySQLExecutor) refreshAlteredSecondaryIndexes(dbName, tableName string) error {
	if e == nil || e.indexManager == nil || e.tableStorageManager == nil || e.infosSchemaManager == nil {
		return nil
	}
	tableInfo, err := e.tableStorageManager.GetTableStorageInfo(dbName, tableName)
	if err != nil {
		return fmt.Errorf("load table storage for altered index: %w", err)
	}
	refreshCtx := context.Background()
	if err := e.infosSchemaManager.RefreshMetadata(refreshCtx, dbName); err != nil {
		return fmt.Errorf("refresh table metadata for altered index: %w", err)
	}
	tableMeta, err := e.infosSchemaManager.GetTableMetadata(refreshCtx, dbName, tableName)
	if err != nil {
		// ALTER writes the compatibility .frm before the dictionary cache is
		// refreshed. Use that authoritative file as the immediate fallback.
		tableMeta, err = (&SelectExecutor{}).loadTableMetaFromFrm(e.getDataDir(), dbName, tableName)
		if err != nil {
			return fmt.Errorf("load table metadata for altered index: %w", err)
		}
	}
	if err := e.indexManager.EnsureSecondaryIndexesWithRebuild(tableInfo, tableMeta); err != nil {
		return fmt.Errorf("rebuild altered secondary indexes: %w", err)
	}
	return nil
}

func (e *XMySQLExecutor) alterTableColumnDDL(dbName, tableName, query string, foreignKeyChecksEnabled bool) (bool, error) {
	renamePattern := regexp.MustCompile(`(?is)^\s*alter\s+table\s+.*?\s+rename\s+column\s+` + "`?([a-zA-Z0-9_$]+)`?" + `\s+to\s+` + "`?([a-zA-Z0-9_$]+)`?")
	if rename := renamePattern.FindStringSubmatch(query); len(rename) == 3 {
		frmPath := filepath.Join(e.getDataDir(), dbName, tableName+".frm")
		raw, err := os.ReadFile(frmPath)
		if err != nil {
			return true, fmt.Errorf("read table metadata failed: %v", err)
		}
		var tableInfo map[string]interface{}
		if err := json.Unmarshal(raw, &tableInfo); err != nil {
			return true, fmt.Errorf("parse table metadata failed: %v", err)
		}
		oldMeta, err := (&SelectExecutor{}).loadTableMetaFromFrm(e.getDataDir(), dbName, tableName)
		if err != nil {
			return true, fmt.Errorf("load table metadata before RENAME COLUMN: %v", err)
		}
		columns, _ := tableInfo["columns"].([]interface{})
		found := false
		for _, value := range columns {
			if column, ok := value.(map[string]interface{}); ok && strings.EqualFold(fmt.Sprint(column["name"]), rename[1]) {
				column["name"] = rename[2]
				found = true
			}
		}
		if !found {
			return true, fmt.Errorf("column '%s' does not exist", rename[1])
		}
		indexes, _ := tableInfo["indexes"].([]interface{})
		for _, value := range indexes {
			if index, ok := value.(map[string]interface{}); ok {
				indexColumns, _ := index["columns"].([]interface{})
				for i, column := range indexColumns {
					if strings.EqualFold(fmt.Sprint(column), rename[1]) {
						indexColumns[i] = rename[2]
					}
				}
			}
		}
		tableInfo["columns"], tableInfo["indexes"] = columns, indexes
		out, err := json.MarshalIndent(tableInfo, "", "  ")
		if err != nil {
			return true, fmt.Errorf("serialize table metadata failed: %v", err)
		}
		if err := writeMetadataFileAtomic(frmPath, out); err != nil {
			return true, fmt.Errorf("write table metadata failed: %v", err)
		}
		if err := e.rewriteRowsAfterSchemaChange(dbName, tableName, oldMeta, map[string]string{rename[1]: rename[2]}); err != nil {
			_ = writeMetadataFileAtomic(frmPath, raw)
			return true, err
		}
		if err := e.refreshAlteredSecondaryIndexes(dbName, tableName); err != nil {
			_ = writeMetadataFileAtomic(frmPath, raw)
			return true, err
		}
		return true, nil
	}
	setDefaultPattern := regexp.MustCompile(`(?is)^\s*alter\s+table\s+.*?\s+alter\s+column\s+` + "`?([a-zA-Z0-9_$]+)`?" + `\s+set\s+default\s+(.+?)\s*;?\s*$`)
	dropDefaultPattern := regexp.MustCompile(`(?is)^\s*alter\s+table\s+.*?\s+alter\s+column\s+` + "`?([a-zA-Z0-9_$]+)`?" + `\s+drop\s+default\s*;?\s*$`)
	defaultColumn, defaultValue, setDefault := "", "", false
	if match := setDefaultPattern.FindStringSubmatch(query); len(match) == 3 {
		defaultColumn, defaultValue, setDefault = match[1], strings.TrimSpace(match[2]), true
	} else if match := dropDefaultPattern.FindStringSubmatch(query); len(match) == 2 {
		defaultColumn = match[1]
	}
	if defaultColumn != "" {
		frmPath := filepath.Join(e.getDataDir(), dbName, tableName+".frm")
		raw, err := os.ReadFile(frmPath)
		if err != nil {
			return true, fmt.Errorf("read table metadata failed: %v", err)
		}
		var tableInfo map[string]interface{}
		if err := json.Unmarshal(raw, &tableInfo); err != nil {
			return true, fmt.Errorf("parse table metadata failed: %v", err)
		}
		columns, _ := tableInfo["columns"].([]interface{})
		found := false
		for _, value := range columns {
			column, ok := value.(map[string]interface{})
			if !ok || !strings.EqualFold(fmt.Sprint(column["name"]), defaultColumn) {
				continue
			}
			found = true
			if setDefault {
				column["default"] = defaultValue
			} else {
				delete(column, "default")
			}
			break
		}
		if !found {
			return true, fmt.Errorf("column '%s' does not exist", defaultColumn)
		}
		out, err := json.MarshalIndent(tableInfo, "", "  ")
		if err != nil {
			return true, fmt.Errorf("serialize table metadata failed: %v", err)
		}
		if err := writeMetadataFileAtomic(frmPath, out); err != nil {
			return true, fmt.Errorf("write table metadata failed: %v", err)
		}
		return true, nil
	}
	dropPattern := regexp.MustCompile(`(?is)^\s*alter\s+table\s+.*?\s+drop\s+column\s+` + "`?([a-zA-Z0-9_$]+)`?")
	drop := dropPattern.FindStringSubmatch(query)
	if len(drop) == 0 {
		return false, nil
	}
	columnName := drop[1]
	frmPath := filepath.Join(e.getDataDir(), dbName, tableName+".frm")
	raw, err := os.ReadFile(frmPath)
	if err != nil {
		return true, fmt.Errorf("read table metadata failed: %v", err)
	}
	var tableInfo map[string]interface{}
	if err := json.Unmarshal(raw, &tableInfo); err != nil {
		return true, fmt.Errorf("parse table metadata failed: %v", err)
	}
	oldMeta, err := (&SelectExecutor{}).loadTableMetaFromFrm(e.getDataDir(), dbName, tableName)
	if err != nil {
		return true, fmt.Errorf("load table metadata before DROP COLUMN: %v", err)
	}
	columns, _ := tableInfo["columns"].([]interface{})
	filteredColumns := make([]interface{}, 0, len(columns))
	found := false
	for _, value := range columns {
		column, ok := value.(map[string]interface{})
		if ok && strings.EqualFold(fmt.Sprint(column["name"]), columnName) {
			if primary, _ := column["primary"].(bool); primary {
				return true, fmt.Errorf("cannot drop primary key column '%s'", columnName)
			}
			found = true
			continue
		}
		filteredColumns = append(filteredColumns, value)
	}
	if !found {
		return true, fmt.Errorf("column '%s' does not exist", columnName)
	}
	if foreignKeyChecksEnabled {
		if err := e.validateReferencedForeignKeysAfterColumnDrop(dbName, tableName, columnName); err != nil {
			return true, err
		}
	}
	indexes, _ := tableInfo["indexes"].([]interface{})
	filteredIndexes := make([]interface{}, 0, len(indexes))
	for _, value := range indexes {
		index, ok := value.(map[string]interface{})
		if !ok {
			filteredIndexes = append(filteredIndexes, value)
			continue
		}
		indexColumns, _ := index["columns"].([]interface{})
		usesColumn := false
		for _, indexColumn := range indexColumns {
			if strings.EqualFold(fmt.Sprint(indexColumn), columnName) {
				usesColumn = true
				break
			}
		}
		if !usesColumn {
			filteredIndexes = append(filteredIndexes, value)
		}
	}
	tableInfo["columns"] = filteredColumns
	tableInfo["indexes"] = filteredIndexes
	if err := reconcileChecksAfterColumnDrop(tableInfo, columnName, filteredColumns); err != nil {
		return true, err
	}
	if err := reconcileForeignKeysAfterColumnDrop(tableInfo, columnName); err != nil {
		return true, err
	}
	out, err := json.MarshalIndent(tableInfo, "", "  ")
	if err != nil {
		return true, fmt.Errorf("serialize table metadata failed: %v", err)
	}
	if err := writeMetadataFileAtomic(frmPath, out); err != nil {
		return true, fmt.Errorf("write table metadata failed: %v", err)
	}
	if err := e.rewriteRowsAfterSchemaChange(dbName, tableName, oldMeta, nil); err != nil {
		_ = writeMetadataFileAtomic(frmPath, raw)
		return true, err
	}
	if err := e.refreshAlteredSecondaryIndexes(dbName, tableName); err != nil {
		_ = writeMetadataFileAtomic(frmPath, raw)
		return true, err
	}
	return true, nil
}

func (e *XMySQLExecutor) alterTableAddColumns(dbName, tableName string, cols []*sqlparser.ColumnDefinition) error {
	frmPath := filepath.Join(e.getDataDir(), dbName, tableName+".frm")
	raw, err := os.ReadFile(frmPath)
	if err != nil {
		return fmt.Errorf("read table metadata failed: %v", err)
	}

	var tableInfo map[string]interface{}
	if err := json.Unmarshal(raw, &tableInfo); err != nil {
		return fmt.Errorf("parse table metadata failed: %v", err)
	}
	oldMeta, err := (&SelectExecutor{}).loadTableMetaFromFrm(e.getDataDir(), dbName, tableName)
	if err != nil {
		return fmt.Errorf("load table metadata before ADD COLUMN: %v", err)
	}

	existing, _ := tableInfo["columns"].([]interface{})
	seen := map[string]struct{}{}
	for _, col := range existing {
		if m, ok := col.(map[string]interface{}); ok {
			if name, _ := m["name"].(string); name != "" {
				seen[strings.ToLower(name)] = struct{}{}
			}
		}
	}

	for _, col := range cols {
		name := col.Name.String()
		if _, ok := seen[strings.ToLower(name)]; ok {
			return fmt.Errorf("duplicate column '%s'", name)
		}

		column := map[string]interface{}{
			"name":     name,
			"type":     col.Type.Type,
			"length":   col.Type.Length,
			"scale":    col.Type.Scale,
			"unsigned": col.Type.Unsigned,
			"nullable": !col.Type.NotNull,
		}
		if col.Type.Default != nil {
			column["default"] = sqlparser.String(col.Type.Default)
		}
		existing = append(existing, column)
		seen[strings.ToLower(name)] = struct{}{}
	}
	tableInfo["columns"] = existing

	out, err := json.MarshalIndent(tableInfo, "", "  ")
	if err != nil {
		return fmt.Errorf("serialize table metadata failed: %v", err)
	}
	if err := writeMetadataFileAtomic(frmPath, out); err != nil {
		return fmt.Errorf("write table metadata failed: %v", err)
	}
	if err := e.rewriteRowsAfterSchemaChange(dbName, tableName, oldMeta, nil); err != nil {
		_ = writeMetadataFileAtomic(frmPath, raw)
		return err
	}
	if err := e.refreshAlteredSecondaryIndexes(dbName, tableName); err != nil {
		_ = writeMetadataFileAtomic(frmPath, raw)
		return err
	}
	return nil
}

func writeMetadataFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".frm-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0644); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// executeTruncateTableStatement 执行 TRUNCATE TABLE，保留表元数据并重建表空间。
func (e *XMySQLExecutor) executeTruncateTableStatement(ctx *ExecutionContext, currentDB string, stmt *sqlparser.DDL) {
	tableName := stmt.Table.Name.String()
	databaseName := stmt.Table.Qualifier.String()
	if databaseName == "" {
		databaseName = currentDB
	}

	if tableName == "" {
		ctx.Results <- &Result{
			Err:        fmt.Errorf("table name cannot be empty"),
			ResultType: common.RESULT_TYPE_DDL,
			Message:    "TRUNCATE TABLE failed: table name cannot be empty",
		}
		return
	}
	if databaseName == "" {
		ctx.Results <- &Result{
			Err:        fmt.Errorf("no database selected"),
			ResultType: common.RESULT_TYPE_DDL,
			Message:    "TRUNCATE TABLE failed: no database selected",
		}
		return
	}
	if err := e.checkTablePrivilege(ctx, databaseName, tableName, "DROP"); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("TRUNCATE TABLE failed: %v", err)}
		return
	}
	if sessionForeignKeyChecksEnabled(ctx.Session) {
		if err := e.validateForeignKeyDependenciesBeforeTableTruncate(databaseName, tableName); err != nil {
			truncateErr := common.NewErrf(
				common.ErrTruncateIllegalFk,
				"Cannot truncate a table referenced in a foreign key constraint (%s)",
				nil,
				err,
			)
			ctx.Results <- &Result{
				Err:        truncateErr,
				ResultType: common.RESULT_TYPE_DDL,
				Message:    fmt.Sprintf("TRUNCATE TABLE failed: %v", truncateErr),
			}
			return
		}
	}
	if err := e.truncateTableImpl(ctx, databaseName, tableName); err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("TRUNCATE TABLE failed: %v", err),
		}
		return
	}

	ctx.Results <- &Result{
		ResultType: common.RESULT_TYPE_DDL,
		Message:    fmt.Sprintf("Table '%s' truncated successfully", tableName),
	}
}

// executeDBDDL 处理数据库级的DDL语句，如 CREATE DATABASE
func (e *XMySQLExecutor) executeDBDDL(stmt *sqlparser.DBDDL, session server.MySQLServerSession, results chan *Result) {
	// 创建执行上下文
	ctx := &ExecutionContext{
		Context:     context.Background(),
		statementId: 0,
		QueryId:     0,
		Session:     session,
		Results:     results,
		Cfg:         e.conf,
	}
	if err := e.prepareDDLImplicitCommit(session); err != nil {
		results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: err.Error()}
		return
	}

	switch stmt.Action {
	case "create":
		e.executeCreateDatabaseStatement(ctx, stmt)
	case "drop":
		e.executeDropDatabaseStatement(ctx, stmt)
	default:
		results <- &Result{
			Err:        newExecutorErrorf("ddl-action", ExecutionErrorCodeValidation, "", "", "", fmt.Errorf("unsupported database DDL action: %s", stmt.Action), "unsupported database DDL action"),
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("Unsupported database DDL action: %s", stmt.Action),
		}
	}
}

// buildExecutorTree 从物理计划构建VolcanoExecutor
func (e *XMySQLExecutor) buildExecutorTree(ctx context.Context, physicalPlan plan.PhysicalPlan) (*VolcanoExecutor, error) {
	// 验证管理器实例有效性
	var tableManager *manager.TableManager
	var bufferPoolManager *manager.OptimizedBufferPoolManager
	var storageManager *manager.StorageManager
	var indexManager *manager.IndexManager

	// 类型断言获取管理器
	if e.tableManager != nil {
		if tm, ok := e.tableManager.(*manager.TableManager); ok {
			tableManager = tm
		}
	}
	if e.bufferPoolManager != nil {
		if bpm, ok := e.bufferPoolManager.(*manager.OptimizedBufferPoolManager); ok {
			bufferPoolManager = bpm
		}
	}
	storageManager = e.storageManager
	indexManager = e.indexManager

	// 验证必需的管理器
	if tableManager == nil {
		return nil, NewExecutionErrorWithCause(
			"engine",
			"build-executor-tree",
			ExecutionErrorCodeMetadataMissing,
			"",
			"",
			"",
			0,
			fmt.Errorf("tableManager is nil"),
			"cannot build executor tree",
		)
	}
	if bufferPoolManager == nil {
		return nil, NewExecutionErrorWithCause(
			"engine",
			"build-executor-tree",
			ExecutionErrorCodeStorageMissing,
			"",
			"",
			"",
			0,
			fmt.Errorf("bufferPoolManager is nil"),
			"cannot build executor tree",
		)
	}

	// 创建VolcanoExecutor实例
	volcanoExec := NewVolcanoExecutor(
		tableManager,
		bufferPoolManager,
		storageManager,
		indexManager,
	)

	// 构建算子树
	if err := volcanoExec.BuildFromPhysicalPlan(ctx, physicalPlan); err != nil {
		return nil, newExecutorErrorf(
			"build-executor-tree",
			ExecutionErrorCodeUnknown,
			"",
			"",
			"",
			err,
			"failed to build operator tree",
		)
	}

	return volcanoExec, nil
}

// executeSelectStatement 执行 SELECT 查询
func (e *XMySQLExecutor) executeSelectStatement(ctx *ExecutionContext, stmt *sqlparser.Select, databaseName string) (*SelectResult, error) {
	sessionValues := newSessionExpressionValues(nil)
	if ctx != nil {
		sessionValues = newSessionExpressionValues(ctx.Session)
		if sessionValues["database"] == nil && strings.TrimSpace(databaseName) != "" {
			sessionValues["database"] = databaseName
		}
	}
	if ctx != nil {
		if result, handled, err := e.executeInformationSchemaMetadataSelect(ctx.RawQuery, ctx.Session); handled {
			return normalizeInformationSchemaAggregate(ctx.RawQuery, result), err
		}
	}
	if stmt != nil && selectHasNoFrom(stmt) {
		result, err := executeConstantSelectStatement(stmt, sessionValues)
		if err == nil && ctx != nil {
			sessionValues["row_count"] = int64(-1)
			syncSessionExpressionValues(ctx.Session, sessionValues)
		}
		return result, err
	}
	if err := e.checkSelectColumnPrivileges(ctx, stmt, databaseName); err != nil {
		return nil, err
	}

	// 类型断言获取具体的管理器类型
	var optimizerManager *manager.OptimizerManager
	var bufferPoolManager *manager.OptimizedBufferPoolManager
	var btreeManager basic.BPlusTreeManager // 使用接口类型
	var tableManager *manager.TableManager

	if e.optimizerManager != nil {
		if om, ok := e.optimizerManager.(*manager.OptimizerManager); ok {
			optimizerManager = om
		}
	}
	if e.bufferPoolManager != nil {
		if bpm, ok := e.bufferPoolManager.(*manager.OptimizedBufferPoolManager); ok {
			bufferPoolManager = bpm
		}
	}
	if e.btreeManager != nil {
		// 尝试断言为 basic.BPlusTreeManager 接口
		if btm, ok := e.btreeManager.(basic.BPlusTreeManager); ok {
			btreeManager = btm
		}
	}
	if e.tableManager != nil {
		if tm, ok := e.tableManager.(*manager.TableManager); ok {
			tableManager = tm
		}
	}

	// 创建SELECT执行器（传入 dataDir：表未在表管理器注册时从 .frm 加载表定义）
	dataDir := ""
	if e.conf != nil {
		dataDir = e.conf.DataDir
		if dataDir == "" && e.conf.InnodbDataDir != "" {
			dataDir = e.conf.InnodbDataDir
		}
		if dataDir == "" {
			dataDir = "data"
		}
	}
	selectExecutor := NewSelectExecutor(
		optimizerManager,
		bufferPoolManager,
		btreeManager,
		e.storageManager,
		tableManager,
		dataDir,
	)
	selectExecutor.sessionValues = sessionValues
	if ctx != nil && ctx.SpatialCandidateKeys != nil {
		selectExecutor.candidateStorageKeys = cloneStringSet(ctx.SpatialCandidateKeys)
	}
	selectExecutor.setStoredFunctionAuthorizer(func(object persistedStoredObject) error {
		if object.Definer != "" {
			if err := e.validateStoredObjectAccount(object.Definer); err != nil {
				return err
			}
		}
		return e.checkStoredRoutineExecute(ctx, object.Schema, object.Name)
	})
	selectExecutor.joinSubqueryEvaluator = func(subquery *sqlparser.Subquery, row joinedMapRow) (interface{}, error) {
		return e.evaluateJoinScalarSubquery(ctx, subquery, row, databaseName)
	}
	selectExecutor.joinSetSubqueryEvaluator = func(subquery *sqlparser.Subquery, row joinedMapRow) ([]interface{}, error) {
		values, _, err := e.evaluateJoinSubqueryValues(ctx, subquery, row, databaseName)
		return values, err
	}

	// 执行SELECT查询
	result, err := selectExecutor.ExecuteSelect(ctx.Context, stmt, databaseName)
	if ctx != nil {
		ctx.recordRowsExamined(selectExecutor.rowsExamined)
		if selectExecutor.lastAccessPath == "table_scan" {
			ctx.recordSelectScan(1)
		}
	}
	e.recordOptimizerTrace(ctx, selectExecutor)
	if err != nil {
		return nil, newExecutorErrorf("execute-select", ExecutionErrorCodeUnknown, databaseName, "", "", err, "execute SELECT failed")
	}
	if ctx != nil {
		sessionValues["row_count"] = int64(-1)
		syncSessionExpressionValues(ctx.Session, sessionValues)
	}

	return result, nil
}

// evaluateJoinScalarSubquery binds qualified columns from the current joined
// row into a scalar subquery before executing it through the normal SELECT
// path. This keeps correlated JOIN ... ON predicates on the same storage,
// aggregate and NULL semantics as correlated projections and WHERE clauses.
func (e *XMySQLExecutor) evaluateJoinScalarSubquery(ctx *ExecutionContext, subquery *sqlparser.Subquery, row joinedMapRow, databaseName string) (interface{}, error) {
	values, query, err := e.evaluateJoinSubqueryValues(ctx, subquery, row, databaseName)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		if strings.Contains(strings.ToLower(query), "count(") {
			return int64(0), nil
		}
		return nil, nil
	}
	if len(values) > 1 {
		return nil, fmt.Errorf("subquery returns more than 1 row")
	}
	return values[0], nil
}

func (e *XMySQLExecutor) evaluateJoinSubqueryValues(ctx *ExecutionContext, subquery *sqlparser.Subquery, row joinedMapRow, databaseName string) ([]interface{}, string, error) {
	if subquery == nil || subquery.Select == nil {
		return nil, "", fmt.Errorf("JOIN subquery is empty")
	}
	query := bindJoinOuterReferences(sqlparser.String(subquery.Select), row)
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, query, fmt.Errorf("parse JOIN subquery: %w", err)
	}
	selectStmt, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, query, fmt.Errorf("JOIN subquery must be SELECT, got %T", statement)
	}
	childCtx := ctx
	if childCtx == nil {
		childCtx = &ExecutionContext{Context: context.Background(), DatabaseName: databaseName}
	}
	result, err := e.executeSelectStatement(&ExecutionContext{
		Context:      childCtx.Context,
		Session:      childCtx.Session,
		DatabaseName: databaseName,
		RawQuery:     query,
	}, selectStmt, databaseName)
	if err != nil {
		return nil, query, err
	}
	values := make([]interface{}, 0, len(result.Records))
	if result == nil {
		return values, query, nil
	}
	for _, record := range result.Records {
		cells := record.GetValues()
		if len(cells) == 0 {
			values = append(values, nil)
			continue
		}
		cell := cells[0]
		if cell == nil || cell.IsNull() {
			values = append(values, nil)
			continue
		}
		values = append(values, basicValueToInterface(cell))
	}
	return values, query, nil
}

func bindJoinOuterReferences(query string, row joinedMapRow) string {
	type reference struct {
		key       string
		qualifier string
		column    string
		value     interface{}
	}
	references := make([]reference, 0, len(row))
	seen := make(map[string]struct{}, len(row))
	for key, value := range row {
		parts := strings.SplitN(key, ".", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			continue
		}
		normalized := strings.ToLower(strings.TrimSpace(parts[0]) + "." + strings.TrimSpace(parts[1]))
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		references = append(references, reference{key: normalized, qualifier: strings.Trim(parts[0], "` "), column: strings.Trim(parts[1], "` "), value: value})
	}
	sort.SliceStable(references, func(left, right int) bool { return len(references[left].key) > len(references[right].key) })
	for _, reference := range references {
		pattern := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(reference.qualifier) + `\s*\.\s*` + regexp.QuoteMeta(reference.column) + `\b`)
		query = pattern.ReplaceAllString(query, correlatedInterfaceSQLLiteral(reference.value))
	}
	return query
}

func selectHasNoFrom(stmt *sqlparser.Select) bool {
	if stmt == nil || len(stmt.From) == 0 {
		return true
	}
	for _, from := range stmt.From {
		fromText := strings.TrimSpace(strings.ToLower(sqlparser.String(from)))
		if fromText != "" && fromText != "dual" {
			return false
		}
	}
	return true
}

func executeConstantSelectStatement(stmt *sqlparser.Select, sessionValues ...map[string]interface{}) (*SelectResult, error) {
	if stmt == nil || len(stmt.SelectExprs) == 0 {
		return nil, fmt.Errorf("SELECT requires at least one expression")
	}
	values := make([]basic.Value, 0, len(stmt.SelectExprs))
	columns := make([]string, 0, len(stmt.SelectExprs))
	types := make([]string, 0, len(stmt.SelectExprs))
	meta := &metadata.TableMeta{Name: "constant", Columns: make([]*metadata.ColumnMeta, 0, len(stmt.SelectExprs))}
	for _, expression := range stmt.SelectExprs {
		aliased, ok := expression.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("unsupported constant SELECT expression %T", expression)
		}
		row := make(map[string]interface{})
		if len(sessionValues) > 0 && sessionValues[0] != nil {
			row[sessionExpressionValuesKey] = sessionValues[0]
		}
		value, err := evaluateExpressionWithRow(aliased.Expr, row)
		if err != nil {
			return nil, err
		}
		if value == nil {
			values = append(values, basic.NewNull())
			types = append(types, "varchar")
		} else {
			values = append(values, basic.NewStringValue(projectionValueString(value)))
			types = append(types, string(projectionValueType(value)))
		}
		name := sqlparser.String(aliased.Expr)
		if !aliased.As.IsEmpty() {
			name = aliased.As.String()
		}
		columns = append(columns, name)
		meta.Columns = append(meta.Columns, &metadata.ColumnMeta{Name: name, Type: metadata.TypeVarchar})
	}
	return &SelectResult{
		Records:     []Record{NewExecutorRecord(values, meta)},
		RowCount:    1,
		Columns:     columns,
		ColumnTypes: types,
		ResultType:  common.RESULT_TYPE_QUERY,
	}, nil
}

func (e *XMySQLExecutor) executeUnionQuery(ctx *ExecutionContext, branches []string, unionAll []bool, databaseName string) (*SelectResult, error) {
	orderSpec, limit, offset, hasLimit := splitSetOperationTail(branches)
	var combined *SelectResult
	for branchIndex, branch := range branches {
		result, err := e.executeSetOperationBranch(ctx, branch, databaseName)
		if err != nil {
			return nil, err
		}
		if combined == nil {
			combined = &SelectResult{Columns: append([]string(nil), result.Columns...), ColumnTypes: append([]string(nil), result.ColumnTypes...), ResultType: common.RESULT_TYPE_QUERY}
		}
		if len(result.Columns) != len(combined.Columns) {
			return nil, fmt.Errorf("UNION branches have different column counts")
		}
		all := branchIndex > 0 && branchIndex-1 < len(unionAll) && unionAll[branchIndex-1]
		if !all {
			// UNION DISTINCT applies to the accumulated left side and the
			// current branch. Rebuilding the key set here also handles mixed
			// UNION ALL / UNION chains instead of treating the whole query as
			// one global UNION ALL.
			seen := make(map[string]struct{}, len(combined.Records))
			unique := make([]Record, 0, len(combined.Records)+len(result.Records))
			for _, record := range combined.Records {
				key := unionRecordKey(record)
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				unique = append(unique, record)
			}
			for _, record := range result.Records {
				key := unionRecordKey(record)
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				unique = append(unique, record)
			}
			combined.Records = unique
		} else {
			combined.Records = append(combined.Records, result.Records...)
		}
	}
	if combined == nil {
		return nil, fmt.Errorf("UNION requires at least one SELECT")
	}
	if err := sortSetOperationRecords(combined, orderSpec, "UNION"); err != nil {
		return nil, err
	}
	if hasLimit || offset > 0 {
		start := offset
		if start > len(combined.Records) {
			start = len(combined.Records)
		}
		end := len(combined.Records)
		if hasLimit && start+limit < end {
			end = start + limit
		}
		combined.Records = combined.Records[start:end]
	}
	combined.RowCount = len(combined.Records)
	combined.Message = fmt.Sprintf("UNION query executed successfully, %d rows returned", combined.RowCount)
	return combined, nil
}

func unionRecordKey(record Record) string {
	keyParts := make([]string, 0, len(record.GetValues()))
	for _, value := range record.GetValues() {
		keyParts = append(keyParts, fmt.Sprintf("%T:%v", value.Raw(), value.Raw()))
	}
	return strings.Join(keyParts, "|")
}

type intersectExceptOperator struct {
	kind string
	all  bool
}

// executeIntersectExceptQuery implements the common MySQL 8 set operations
// without materializing a second SQL AST. Values are compared using the same
// typed row identity as UNION DISTINCT; result order remains stable according
// to the left input branch.
func (e *XMySQLExecutor) executeIntersectExceptQuery(ctx *ExecutionContext, branches []string, operators []intersectExceptOperator, databaseName string) (*SelectResult, error) {
	if len(branches) == 0 || len(operators) != len(branches)-1 {
		return nil, fmt.Errorf("set operation requires at least two SELECT branches")
	}
	results := make([]*SelectResult, 0, len(branches))
	for _, branch := range branches {
		result, err := e.executeSetOperationBranch(ctx, branch, databaseName)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	combined := &SelectResult{
		Columns:     append([]string(nil), results[0].Columns...),
		ColumnTypes: append([]string(nil), results[0].ColumnTypes...),
		ResultType:  common.RESULT_TYPE_QUERY,
		Records:     append([]Record(nil), results[0].Records...),
	}
	for branchIndex, operator := range operators {
		right := results[branchIndex+1]
		if len(right.Columns) != len(combined.Columns) {
			return nil, fmt.Errorf("set operation branches have different column counts")
		}
		rightCounts := make(map[string]int, len(right.Records))
		for _, record := range right.Records {
			rightCounts[unionRecordKey(record)]++
		}
		leftCounts := make(map[string]int, len(combined.Records))
		for _, record := range combined.Records {
			leftCounts[unionRecordKey(record)]++
		}
		out := make([]Record, 0, len(combined.Records))
		seen := make(map[string]struct{}, len(leftCounts))
		for _, record := range combined.Records {
			key := unionRecordKey(record)
			if operator.kind == "INTERSECT" {
				if rightCounts[key] == 0 {
					continue
				}
				if operator.all {
					if leftCounts[key] <= 0 || rightCounts[key] <= 0 {
						continue
					}
					leftCounts[key]--
					rightCounts[key]--
					out = append(out, record)
					continue
				}
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				out = append(out, record)
				continue
			}
			if operator.kind == "EXCEPT" {
				if operator.all {
					if rightCounts[key] > 0 {
						rightCounts[key]--
						continue
					}
					out = append(out, record)
					continue
				}
				if rightCounts[key] > 0 {
					continue
				}
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				out = append(out, record)
			}
		}
		combined.Records = out
	}
	combined.RowCount = len(combined.Records)
	combined.Message = fmt.Sprintf("set operation executed successfully, %d rows returned", combined.RowCount)
	return combined, nil
}

// executeMixedSetOperationQuery evaluates INTERSECT before UNION/EXCEPT, which
// matches MySQL's set-operator precedence for the common unparenthesized form.
func (e *XMySQLExecutor) executeMixedSetOperationQuery(ctx *ExecutionContext, branches []string, operators []intersectExceptOperator, databaseName string) (*SelectResult, error) {
	if len(branches) == 0 || len(operators) != len(branches)-1 {
		return nil, fmt.Errorf("set operation requires at least two SELECT branches")
	}
	orderSpec, limit, offset, hasLimit := splitSetOperationTail(branches)
	results := make([]*SelectResult, 0, len(branches))
	for _, branch := range branches {
		result, err := e.executeSetOperationBranch(ctx, branch, databaseName)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	type setSegment struct {
		result *SelectResult
		next   int
	}
	segments := make([]setSegment, 0, len(branches))
	for index := 0; index < len(results); {
		current := results[index]
		next := index
		for next < len(operators) && operators[next].kind == "INTERSECT" {
			var err error
			current, err = combineSetOperationResults(current, results[next+1], operators[next])
			if err != nil {
				return nil, err
			}
			next++
		}
		segments = append(segments, setSegment{result: current, next: next})
		index = next + 1
	}
	if len(segments) == 0 {
		return nil, fmt.Errorf("set operation has no result segments")
	}
	combined := segments[0].result
	for segmentIndex := 1; segmentIndex < len(segments); segmentIndex++ {
		opIndex := segments[segmentIndex-1].next
		if opIndex < 0 || opIndex >= len(operators) {
			return nil, fmt.Errorf("set operation operator index out of range")
		}
		nextCombined, err := combineSetOperationResults(combined, segments[segmentIndex].result, operators[opIndex])
		if err != nil {
			return nil, err
		}
		combined = nextCombined
	}
	if err := sortSetOperationRecords(combined, orderSpec, "set operation"); err != nil {
		return nil, err
	}
	if hasLimit || offset > 0 {
		start := offset
		if start > len(combined.Records) {
			start = len(combined.Records)
		}
		end := len(combined.Records)
		if hasLimit && start+limit < end {
			end = start + limit
		}
		combined.Records = combined.Records[start:end]
	}
	combined.RowCount = len(combined.Records)
	combined.Message = fmt.Sprintf("set operation executed successfully, %d rows returned", combined.RowCount)
	return combined, nil
}

func parseSetSelectBranch(branch string) (*sqlparser.Select, error) {
	text := strings.TrimSpace(branch)
	for hasBalancedOuterParentheses(text) {
		text = strings.TrimSpace(text[1 : len(text)-1])
	}
	stmt, err := sqlparser.Parse(text)
	if err != nil {
		return nil, err
	}
	if paren, ok := stmt.(*sqlparser.ParenSelect); ok {
		if selectStmt, ok := paren.Select.(*sqlparser.Select); ok {
			return selectStmt, nil
		}
	}
	if selectStmt, ok := stmt.(*sqlparser.Select); ok {
		return selectStmt, nil
	}
	return nil, fmt.Errorf("set operation branch is not SELECT: %T", stmt)
}

// executeSetOperationBranch materializes a parenthesized set expression when
// it appears as one branch of a larger set operation. The top-level dispatcher
// already handles ordinary branches; this helper keeps nested UNION,
// INTERSECT and EXCEPT branches on the same execution path.
func (e *XMySQLExecutor) executeSetOperationBranch(ctx *ExecutionContext, branch, databaseName string) (*SelectResult, error) {
	text := strings.TrimSpace(branch)
	for hasBalancedOuterParentheses(text) {
		text = strings.TrimSpace(text[1 : len(text)-1])
	}
	if nestedBranches, nestedOperators, ok := splitSetOperationQuery(text); ok {
		if hasNonUnionSetOperator(nestedOperators) {
			return e.executeMixedSetOperationQuery(ctx, nestedBranches, nestedOperators, databaseName)
		}
		unionAll := make([]bool, 0, len(nestedOperators))
		for _, operator := range nestedOperators {
			unionAll = append(unionAll, operator.all)
		}
		return e.executeUnionQuery(ctx, nestedBranches, unionAll, databaseName)
	}
	selectStmt, err := parseSetSelectBranch(text)
	if err != nil {
		return nil, err
	}
	return e.executeSelectStatement(ctx, selectStmt, databaseName)
}

func hasBalancedOuterParentheses(text string) bool {
	text = strings.TrimSpace(text)
	if len(text) < 2 || text[0] != '(' || text[len(text)-1] != ')' {
		return false
	}
	depth := 0
	var quote byte
	for index := 0; index < len(text); index++ {
		ch := text[index]
		if quote != 0 {
			if ch == quote && (index == 0 || text[index-1] != '\\') {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && index != len(text)-1 {
				return false
			}
		}
	}
	return depth == 0
}

func splitSetOperationTail(branches []string) (string, int, int, bool) {
	if len(branches) == 0 {
		return "", 0, 0, false
	}
	last := strings.TrimSpace(branches[len(branches)-1])
	orderSpec := ""
	tail := last
	if parts := splitTopLevelKeyword(last, "order by"); len(parts) == 2 {
		last = strings.TrimSpace(parts[0])
		tail = strings.TrimSpace(parts[1])
		orderSpec = tail
	}
	if limitParts := splitTopLevelKeyword(tail, "limit"); len(limitParts) == 2 {
		if orderSpec != "" {
			orderSpec = strings.TrimSpace(limitParts[0])
		} else {
			last = strings.TrimSpace(limitParts[0])
		}
		limit, offset, ok := parseSetLimitSpec(limitParts[1])
		branches[len(branches)-1] = last
		return orderSpec, limit, offset, ok
	}
	branches[len(branches)-1] = strings.TrimSpace(last)
	return orderSpec, 0, 0, false
}

func parseSetLimitSpec(spec string) (int, int, bool) {
	spec = strings.TrimSpace(spec)
	if values := strings.Split(spec, ","); len(values) == 2 {
		offset, err1 := strconv.Atoi(strings.TrimSpace(values[0]))
		limit, err2 := strconv.Atoi(strings.TrimSpace(values[1]))
		return limit, offset, err1 == nil && err2 == nil && offset >= 0 && limit >= 0
	}
	if parts := splitTopLevelKeyword(spec, "offset"); len(parts) == 2 {
		limit, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
		offset, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
		return limit, offset, err1 == nil && err2 == nil && offset >= 0 && limit >= 0
	}
	limit, err := strconv.Atoi(spec)
	return limit, 0, err == nil && limit >= 0
}

type setOrderTerm struct {
	expr   sqlparser.Expr
	column string
	desc   bool
}

func sortSetOperationRecords(result *SelectResult, orderSpec, label string) error {
	if strings.TrimSpace(orderSpec) == "" {
		return nil
	}
	terms := make([]setOrderTerm, 0, 2)
	for _, rawTerm := range splitTopLevelSetOrderTerms(orderSpec) {
		rawTerm = strings.TrimSpace(rawTerm)
		if rawTerm == "" {
			continue
		}
		stmt, err := sqlparser.Parse("select 1 order by " + rawTerm)
		if err != nil {
			return fmt.Errorf("invalid %s ORDER BY expression %q: %v", label, rawTerm, err)
		}
		selectStmt, ok := stmt.(*sqlparser.Select)
		if !ok || len(selectStmt.OrderBy) != 1 || selectStmt.OrderBy[0] == nil {
			return fmt.Errorf("invalid %s ORDER BY expression %q", label, rawTerm)
		}
		order := selectStmt.OrderBy[0]
		term := setOrderTerm{expr: order.Expr, column: strings.Trim(sqlparser.String(order.Expr), "` ")}
		term.desc = order.Direction == sqlparser.DescScr
		terms = append(terms, term)
	}
	if len(terms) == 0 {
		return nil
	}
	indexes := make([]int, len(terms))
	for termIndex, term := range terms {
		columnIndex := -1
		if ordinal, err := strconv.Atoi(term.column); err == nil && ordinal > 0 && ordinal <= len(result.Columns) {
			columnIndex = ordinal - 1
		} else if _, ok := term.expr.(*sqlparser.ColName); ok {
			for index, column := range result.Columns {
				if strings.EqualFold(strings.Trim(column, "`"), term.column) {
					columnIndex = index
					break
				}
			}
		}
		if columnIndex < 0 {
			if _, isColumn := term.expr.(*sqlparser.ColName); !isColumn {
				indexes[termIndex] = -1
				continue
			}
			return fmt.Errorf("unknown %s ORDER BY column '%s'", label, term.column)
		}
		indexes[termIndex] = columnIndex
	}
	type orderedSetRecord struct {
		record Record
		order  []interface{}
	}
	ordered := make([]orderedSetRecord, len(result.Records))
	for rowIndex, record := range result.Records {
		values := record.GetValues()
		row := make(map[string]interface{}, len(result.Columns)*2)
		for columnIndex, column := range result.Columns {
			if columnIndex >= len(values) {
				continue
			}
			value := setOrderValue(values[columnIndex].Raw())
			row[column] = value
			row[strings.ToLower(strings.Trim(column, "`"))] = value
		}
		ordered[rowIndex] = orderedSetRecord{record: record, order: make([]interface{}, len(terms))}
		for termIndex, term := range terms {
			if indexes[termIndex] >= 0 {
				if indexes[termIndex] >= len(values) {
					return fmt.Errorf("%s ORDER BY column %q is not present in result", label, term.column)
				}
				ordered[rowIndex].order[termIndex] = setOrderValue(values[indexes[termIndex]].Raw())
				continue
			}
			value, err := evaluateExpressionWithRow(term.expr, row)
			if err != nil {
				return fmt.Errorf("evaluate %s ORDER BY expression %q: %v", label, term.column, err)
			}
			ordered[rowIndex].order[termIndex] = setOrderValue(value)
		}
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		for termIndex := range terms {
			cmp := compareScalarValues(ordered[i].order[termIndex], ordered[j].order[termIndex])
			if cmp == 0 {
				continue
			}
			if terms[termIndex].desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
	for index, item := range ordered {
		result.Records[index] = item.record
	}
	return nil
}

func setOrderValue(value interface{}) interface{} {
	if bytes, ok := value.([]byte); ok {
		return string(bytes)
	}
	return value
}

func splitTopLevelSetOrderTerms(spec string) []string {
	terms := make([]string, 0, 2)
	start, depth := 0, 0
	var quote byte
	for index := 0; index < len(spec); index++ {
		ch := spec[index]
		if quote != 0 {
			if ch == quote && (index == 0 || spec[index-1] != '\\') {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				terms = append(terms, strings.TrimSpace(spec[start:index]))
				start = index + 1
			}
		}
	}
	terms = append(terms, strings.TrimSpace(spec[start:]))
	return terms
}

func combineSetOperationResults(left, right *SelectResult, operator intersectExceptOperator) (*SelectResult, error) {
	if left == nil || right == nil || len(left.Columns) != len(right.Columns) {
		return nil, fmt.Errorf("set operation branches have different column counts")
	}
	combined := &SelectResult{
		Columns:     append([]string(nil), left.Columns...),
		ColumnTypes: append([]string(nil), left.ColumnTypes...),
		ResultType:  common.RESULT_TYPE_QUERY,
		Records:     make([]Record, 0),
	}
	rightCounts := make(map[string]int, len(right.Records))
	for _, record := range right.Records {
		rightCounts[unionRecordKey(record)]++
	}
	seen := make(map[string]struct{}, len(left.Records))
	for _, record := range left.Records {
		key := unionRecordKey(record)
		switch operator.kind {
		case "UNION":
			if operator.all {
				combined.Records = append(combined.Records, record)
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			combined.Records = append(combined.Records, record)
		case "INTERSECT":
			if rightCounts[key] == 0 {
				continue
			}
			if operator.all {
				if rightCounts[key] <= 0 {
					continue
				}
				rightCounts[key]--
				combined.Records = append(combined.Records, record)
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			combined.Records = append(combined.Records, record)
		case "EXCEPT":
			if rightCounts[key] > 0 {
				if operator.all {
					rightCounts[key]--
				}
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			combined.Records = append(combined.Records, record)
		default:
			return nil, fmt.Errorf("unsupported set operator %s", operator.kind)
		}
	}
	if operator.kind == "UNION" && operator.all {
		combined.Records = append(combined.Records, right.Records...)
	} else if operator.kind == "UNION" {
		for _, record := range right.Records {
			key := unionRecordKey(record)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			combined.Records = append(combined.Records, record)
		}
	}
	combined.RowCount = len(combined.Records)
	return combined, nil
}

func (e *XMySQLExecutor) rewriteSimpleInSubquery(ctx *ExecutionContext, query, databaseName string) (string, error) {
	re := regexp.MustCompile(`(?is)([a-zA-Z0-9_$\.]+)\s+(not\s+)?in\s*\(\s*(select\s+[^()]+)\s*\)`)
	matches := re.FindStringSubmatch(query)
	if len(matches) == 0 {
		return "", nil
	}
	matchStart := strings.Index(strings.ToLower(query), strings.ToLower(matches[0]))
	if matchStart >= 0 && scalarSubqueryAppearsInJoinOn(query, matchStart) {
		// JOIN ... ON subqueries need the current candidate row. Leave both
		// correlated and uncorrelated IN subqueries for the JOIN evaluator,
		// which can preserve NULL-aware set semantics per candidate row.
		return "", nil
	}
	innerStmt, err := sqlparser.Parse(strings.TrimSpace(matches[3]))
	if err != nil {
		return "", err
	}
	innerSelect, ok := innerStmt.(*sqlparser.Select)
	if !ok {
		return "", fmt.Errorf("IN subquery must be SELECT")
	}
	innerResult, err := e.executeSelectStatement(ctx, innerSelect, databaseName)
	if err != nil {
		return "", err
	}
	if len(innerResult.Columns) != 1 {
		return "", fmt.Errorf("subquery returns more than one column")
	}
	literals := make([]string, 0, len(innerResult.Records))
	hasNull := false
	for _, record := range innerResult.Records {
		values := record.GetValues()
		if len(values) == 0 || values[0].IsNull() {
			hasNull = true
			continue
		}
		literals = append(literals, basicValueSQLLiteral(values[0]))
	}
	if strings.TrimSpace(matches[2]) != "" && hasNull {
		return strings.Replace(query, matches[0], "1 = 0", 1), nil
	}
	if len(literals) == 0 {
		literals = append(literals, "NULL")
	}
	operator := "IN"
	if strings.TrimSpace(matches[2]) != "" {
		operator = "NOT IN"
	}
	replacement := fmt.Sprintf("%s %s (%s)", matches[1], operator, strings.Join(literals, ", "))
	return strings.Replace(query, matches[0], replacement, 1), nil
}

// rewriteSimpleQuantifiedSubqueries evaluates uncorrelated ANY/SOME/ALL
// predicates at the statement boundary. The legacy SQL AST path does not
// expose quantified subqueries as executable expressions, so this keeps the
// complete predicate semantics (including empty sets and NULL UNKNOWN) while
// leaving the normal row executor unchanged.
func (e *XMySQLExecutor) rewriteSimpleQuantifiedSubqueries(ctx *ExecutionContext, query, databaseName string) (string, error) {
	result := query
	matcher := regexp.MustCompile(`(?is)([a-zA-Z0-9_$\.]+)\s*(<=>|<>|!=|<=|>=|=|<|>)\s*(any|some|all)\s*\(`)
	for {
		indexes := matcher.FindStringSubmatchIndex(result)
		if len(indexes) == 0 {
			return resultIfChanged(query, result), nil
		}
		start, end := indexes[0], indexes[1]
		openRelative := strings.LastIndex(result[start:end], "(")
		if openRelative < 0 {
			return "", fmt.Errorf("quantified subquery has no opening parenthesis")
		}
		open := start + openRelative
		close := matchingParenIndex(result, open)
		if close < 0 {
			return "", fmt.Errorf("quantified subquery has unbalanced parentheses")
		}
		innerSQL := strings.TrimSpace(result[open+1 : close])
		if !strings.HasPrefix(strings.ToLower(innerSQL), "select") {
			return "", fmt.Errorf("quantified predicate requires a SELECT subquery")
		}
		innerStmt, err := sqlparser.Parse(innerSQL)
		if err != nil {
			return "", err
		}
		innerSelect, ok := innerStmt.(*sqlparser.Select)
		if !ok {
			return "", fmt.Errorf("quantified subquery must be SELECT")
		}
		innerResult, err := e.executeSelectStatement(ctx, innerSelect, databaseName)
		if err != nil {
			return "", err
		}
		if len(innerResult.Columns) != 1 {
			return "", fmt.Errorf("subquery returns more than one column")
		}
		leftText := result[indexes[2]:indexes[3]]
		op := result[indexes[4]:indexes[5]]
		quantifier := strings.ToUpper(result[indexes[6]:indexes[7]])
		operands := make([]string, 0, len(innerResult.Records))
		for _, record := range innerResult.Records {
			if len(record.GetValues()) == 0 || record.GetValues()[0].IsNull() {
				operands = append(operands, "NULL")
				continue
			}
			operands = append(operands, basicValueSQLLiteral(record.GetValues()[0]))
		}
		if len(operands) == 0 {
			if quantifier == "ALL" {
				result = result[:start] + "1 = 1" + result[close+1:]
			} else {
				result = result[:start] + "1 = 0" + result[close+1:]
			}
			continue
		}
		joiner := " OR "
		if quantifier == "ALL" {
			joiner = " AND "
		}
		predicates := make([]string, 0, len(operands))
		for _, operand := range operands {
			predicates = append(predicates, fmt.Sprintf("(%s %s %s)", leftText, op, operand))
		}
		replacement := strings.Join(predicates, joiner)
		result = result[:start] + replacement + result[close+1:]
	}
}

func evaluateQuantifiedPredicate(left interface{}, operator, quantifier string, values []interface{}) (truth bool, unknown bool) {
	if quantifier == "ANY" || quantifier == "SOME" {
		if len(values) == 0 {
			return false, false
		}
		for _, right := range values {
			matched, indeterminate := evaluateScalarComparison(left, operator, right)
			if matched {
				return true, false
			}
			if indeterminate {
				unknown = true
			}
		}
		return false, unknown
	}
	if len(values) == 0 {
		return true, false
	}
	for _, right := range values {
		matched, indeterminate := evaluateScalarComparison(left, operator, right)
		if !matched && !indeterminate {
			return false, false
		}
		if indeterminate {
			unknown = true
		}
	}
	return !unknown, unknown
}

func evaluateScalarComparison(left interface{}, operator string, right interface{}) (bool, bool) {
	if operator == "<=>" {
		if left == nil || right == nil {
			return left == nil && right == nil, false
		}
		return compareScalarValues(left, right) == 0, false
	}
	if left == nil || right == nil {
		return false, true
	}
	comparison := compareScalarValues(left, right)
	switch operator {
	case "=":
		return comparison == 0, false
	case "!=", "<>":
		return comparison != 0, false
	case "<":
		return comparison < 0, false
	case "<=":
		return comparison <= 0, false
	case ">":
		return comparison > 0, false
	case ">=":
		return comparison >= 0, false
	default:
		return false, true
	}
}

// rewriteSimpleScalarSubqueries materializes uncorrelated scalar subqueries
// before the normal expression executor runs.  This covers the common MySQL
// forms `(SELECT value FROM ...)` and `column = (SELECT value FROM ...)`, while
// preserving SQL NULL for an empty result and rejecting multi-row scalars.
// Correlated subqueries are handled by executeCorrelatedSubqueryCompatibility
// and are deliberately left untouched here.
func (e *XMySQLExecutor) rewriteSimpleScalarSubqueries(ctx *ExecutionContext, query, databaseName string) (string, error) {
	result := query
	searchFrom := 0
	for {
		lower := strings.ToLower(result)
		openRelative := strings.Index(lower[searchFrom:], "(")
		if openRelative < 0 {
			return resultIfChanged(query, result), nil
		}
		open := searchFrom + openRelative
		probe := open + 1
		for probe < len(lower) && (lower[probe] == ' ' || lower[probe] == '\t' || lower[probe] == '\r' || lower[probe] == '\n') {
			probe++
		}
		if !strings.HasPrefix(lower[probe:], "select") || (probe+len("select") < len(lower) && isSQLIdentifierPart(lower[probe+len("select")])) {
			searchFrom = open + 1
			continue
		}
		close := matchingParenIndex(result, open)
		if close < 0 {
			return "", fmt.Errorf("scalar subquery has unbalanced parentheses")
		}
		if scalarSubqueryAppearsInJoinOn(result, open) {
			// JOIN ... ON scalar subqueries need the current candidate row as
			// correlation context; leave them for the JOIN predicate evaluator
			// instead of materializing them once before the join starts.
			searchFrom = close + 1
			continue
		}
		if scalarSubqueryAppearsAsDerivedTable(result, open) {
			// FROM/JOIN (SELECT ...) is a derived table, not a scalar
			// expression. It must remain in the AST so the derived-table
			// executor can materialize all projected columns.
			searchFrom = close + 1
			continue
		}
		innerSQL := strings.TrimSpace(result[probe:close])
		innerStmt, err := sqlparser.Parse(innerSQL)
		if err != nil {
			return "", err
		}
		innerSelect, ok := innerStmt.(*sqlparser.Select)
		if !ok {
			return "", fmt.Errorf("scalar subquery must be SELECT")
		}
		innerResult, err := e.executeSelectStatement(ctx, innerSelect, databaseName)
		if err != nil {
			return "", err
		}
		if len(innerResult.Columns) != 1 {
			return "", fmt.Errorf("scalar subquery returns %d columns", len(innerResult.Columns))
		}
		if len(innerResult.Records) > 1 {
			return "", fmt.Errorf("subquery returns more than 1 row")
		}
		replacement := "NULL"
		if len(innerResult.Records) == 1 && len(innerResult.Records[0].GetValues()) > 0 && !innerResult.Records[0].GetValues()[0].IsNull() {
			replacement = basicValueSQLLiteral(innerResult.Records[0].GetValues()[0])
		}
		result = result[:open] + replacement + result[close+1:]
		searchFrom = open + len(replacement)
	}
}

func scalarSubqueryAppearsInJoinOn(query string, open int) bool {
	if open <= 0 || open > len(query) {
		return false
	}
	prefix := strings.ToLower(query[:open])
	lastOn := strings.LastIndex(prefix, " on ")
	lastJoin := strings.LastIndex(prefix, " join ")
	if lastOn <= lastJoin {
		return false
	}
	// A JOIN's ON clause ends before the outer WHERE/GROUP/HAVING/ORDER/LIMIT
	// tail. Without this check an EXISTS in `WHERE` after a JOIN would be
	// mistaken for a JOIN predicate merely because the earlier FROM clause
	// contains `JOIN ... ON`.
	for _, clause := range []string{" where ", " group by ", " having ", " order by ", " limit "} {
		if strings.LastIndex(prefix, clause) > lastOn {
			return false
		}
	}
	return true
}

func scalarSubqueryAppearsAsDerivedTable(query string, open int) bool {
	if open <= 0 || open > len(query) {
		return false
	}
	prefix := strings.ToLower(strings.TrimSpace(query[:open]))
	for _, marker := range []string{"from", "join", "left join", "right join", "inner join", "cross join"} {
		if strings.HasSuffix(prefix, marker) {
			return true
		}
	}
	return false
}

func isInsertOrReplaceQuery(query string) bool {
	lower := strings.ToLower(strings.TrimSpace(query))
	return strings.HasPrefix(lower, "insert ") || strings.HasPrefix(lower, "replace ")
}

func resultIfChanged(original, result string) string {
	if original == result {
		return ""
	}
	return result
}

func isSQLIdentifierPart(ch byte) bool {
	return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '$'
}

func (e *XMySQLExecutor) rewriteSessionUserVariables(query string, session server.MySQLServerSession) string {
	if session == nil || !strings.Contains(query, "@") {
		return query
	}
	var builder strings.Builder
	var quote byte
	for index := 0; index < len(query); {
		ch := query[index]
		if quote != 0 {
			builder.WriteByte(ch)
			if ch == quote && (index == 0 || query[index-1] != '\\') {
				quote = 0
			}
			index++
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			builder.WriteByte(ch)
			index++
			continue
		}
		if ch != '@' || index+1 >= len(query) || query[index+1] == '@' {
			builder.WriteByte(ch)
			index++
			continue
		}
		end := index + 1
		for end < len(query) && isSQLIdentifierPart(query[end]) {
			end++
		}
		if end == index+1 {
			builder.WriteByte(ch)
			index++
			continue
		}
		name := query[index:end]
		value := session.GetParamByName(name)
		if value == nil {
			value = session.GetParamByName(name[1:])
		}
		builder.WriteString(sessionUserVariableSQLLiteral(value))
		index = end
	}
	return builder.String()
}

func (e *XMySQLExecutor) executeUserVariableAssignment(ctx *ExecutionContext, query string, session server.MySQLServerSession) bool {
	match := regexp.MustCompile(`(?is)^set\s+(@[a-zA-Z0-9_$]+)\s*=\s*(.+?)\s*;?$`).FindStringSubmatch(strings.TrimSpace(query))
	if len(match) == 0 {
		return false
	}
	if session == nil {
		ctx.Results <- &Result{Err: fmt.Errorf("SET user variable requires a session"), ResultType: innodbcommon.RESULT_TYPE_ERROR}
		return true
	}
	value := interface{}(nil)
	expression := e.rewriteSessionUserVariables(strings.TrimSpace(match[2]), session)
	statement, err := sqlparser.Parse("select " + expression)
	if err == nil {
		if selectStmt, ok := statement.(*sqlparser.Select); ok && len(selectStmt.SelectExprs) == 1 {
			if aliased, ok := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr); ok {
				value, err = evaluateExpressionWithRow(aliased.Expr, map[string]interface{}{})
			}
		}
	}
	if err != nil {
		ctx.Results <- &Result{Err: err, ResultType: innodbcommon.RESULT_TYPE_ERROR}
		return true
	}
	session.SetParamByName(match[1], value)
	session.SetParamByName(strings.TrimPrefix(match[1], "@"), value)
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_SET, Message: "user variable set"}
	return true
}

func sessionUserVariableSQLLiteral(value interface{}) string {
	if value == nil {
		return "NULL"
	}
	switch typed := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprint(typed)
	case []byte:
		return "'" + strings.ReplaceAll(string(typed), "'", "''") + "'"
	case string:
		trimmed := strings.TrimSpace(typed)
		if _, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return trimmed
		}
		if _, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return trimmed
		}
		return "'" + strings.ReplaceAll(typed, "'", "''") + "'"
	default:
		return "'" + strings.ReplaceAll(fmt.Sprint(value), "'", "''") + "'"
	}
}

func basicValueSQLLiteral(value basic.Value) string {
	text := value.ToString()
	if _, err := strconv.ParseInt(text, 10, 64); err == nil {
		return text
	}
	if _, err := strconv.ParseFloat(text, 64); err == nil {
		return text
	}
	return "'" + strings.ReplaceAll(text, "'", "''") + "'"
}

func splitUnionQuery(query string) ([]string, []bool, bool) {
	lower := strings.ToLower(query)
	depth := 0
	quote := byte(0)
	start := 0
	branches := make([]string, 0, 2)
	unionAll := make([]bool, 0, 1)
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if quote != 0 {
			if ch == quote && (i == 0 || query[i-1] != '\\') {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth != 0 || !strings.HasPrefix(lower[i:], " union") {
			continue
		}
		end := i + len(" union")
		if end < len(lower) && lower[end] != ' ' && lower[end] != '\t' && lower[end] != '\r' && lower[end] != '\n' {
			continue
		}
		j := end
		for j < len(lower) && (lower[j] == ' ' || lower[j] == '\t' || lower[j] == '\r' || lower[j] == '\n') {
			j++
		}
		isAll := strings.HasPrefix(lower[j:], "all") && (j+3 == len(lower) || lower[j+3] == ' ' || lower[j+3] == '\t' || lower[j+3] == '\r' || lower[j+3] == '\n')
		if isAll {
			j += 3
		} else if strings.HasPrefix(lower[j:], "distinct") && (j+8 == len(lower) || !isSQLIdentifierPart(lower[j+8])) {
			// UNION DISTINCT is the explicit spelling of the default UNION
			// duplicate-elimination mode. Consume the modifier while retaining
			// the existing unionAll=false representation.
			j += 8
		}
		branches = append(branches, strings.TrimSpace(query[start:i]))
		start = j
		unionAll = append(unionAll, isAll)
		i = j - 1
	}
	if len(branches) == 0 {
		return nil, nil, false
	}
	branches = append(branches, strings.TrimSpace(query[start:]))
	return branches, unionAll, true
}

// splitIntersectExceptQuery finds top-level INTERSECT/EXCEPT operators. UNION
// is intentionally handled by splitUnionQuery so existing mixed UNION ALL /
// DISTINCT semantics remain unchanged until a precedence-aware set planner is
// introduced.
func splitIntersectExceptQuery(query string) ([]string, []intersectExceptOperator, bool) {
	lower := strings.ToLower(query)
	depth, start := 0, 0
	var quote byte
	branches := make([]string, 0, 2)
	operators := make([]intersectExceptOperator, 0, 1)
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if quote != 0 {
			if ch == quote && (i == 0 || query[i-1] != '\\') {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth != 0 || i == 0 || (query[i-1] != ' ' && query[i-1] != '\t' && query[i-1] != '\r' && query[i-1] != '\n') {
			continue
		}
		kind := ""
		for _, candidate := range []string{"intersect", "except"} {
			if strings.HasPrefix(lower[i:], candidate) {
				end := i + len(candidate)
				if end == len(lower) || lower[end] == ' ' || lower[end] == '\t' || lower[end] == '\r' || lower[end] == '\n' {
					kind = strings.ToUpper(candidate)
				}
			}
			if kind != "" {
				break
			}
		}
		if kind == "" {
			continue
		}
		j := i + len(strings.ToLower(kind))
		for j < len(lower) && (lower[j] == ' ' || lower[j] == '\t' || lower[j] == '\r' || lower[j] == '\n' || lower[j] == '(' || lower[j] == ')') {
			j++
		}
		all := false
		if strings.HasPrefix(lower[j:], "all") && (j+3 == len(lower) || !isSQLIdentifierPart(lower[j+3])) {
			all = true
			j += 3
		} else if strings.HasPrefix(lower[j:], "distinct") && (j+8 == len(lower) || !isSQLIdentifierPart(lower[j+8])) {
			j += 8
		}
		branches = append(branches, strings.TrimSpace(query[start:i]))
		operators = append(operators, intersectExceptOperator{kind: kind, all: all})
		start = j
		i = j - 1
	}
	if len(operators) == 0 {
		return nil, nil, false
	}
	branches = append(branches, strings.TrimSpace(query[start:]))
	return branches, operators, true
}

// splitSetOperationQuery recognizes top-level UNION, INTERSECT and EXCEPT.
// It is used only when a non-UNION operator is present; pure UNION keeps the
// established outer ORDER BY/LIMIT implementation above.
func splitSetOperationQuery(query string) ([]string, []intersectExceptOperator, bool) {
	lower := strings.ToLower(query)
	depth, start := 0, 0
	var quote byte
	branches := make([]string, 0, 2)
	operators := make([]intersectExceptOperator, 0, 1)
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if quote != 0 {
			if ch == quote && (i == 0 || query[i-1] != '\\') {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth != 0 || i == 0 || !isSQLWhitespace(query[i-1]) {
			continue
		}
		kind := ""
		for _, candidate := range []string{"union", "intersect", "except"} {
			if !strings.HasPrefix(lower[i:], candidate) {
				continue
			}
			end := i + len(candidate)
			if end == len(lower) || isSQLWhitespace(lower[end]) {
				kind = strings.ToUpper(candidate)
				break
			}
		}
		if kind == "" {
			continue
		}
		j := i + len(kind)
		for j < len(lower) && isSQLWhitespace(lower[j]) {
			j++
		}
		all := false
		if strings.HasPrefix(lower[j:], "all") && (j+3 == len(lower) || !isSQLIdentifierPart(lower[j+3])) {
			all = true
			j += 3
		} else if strings.HasPrefix(lower[j:], "distinct") && (j+8 == len(lower) || !isSQLIdentifierPart(lower[j+8])) {
			j += 8
		}
		branches = append(branches, strings.TrimSpace(query[start:i]))
		operators = append(operators, intersectExceptOperator{kind: kind, all: all})
		start = j
		i = j - 1
	}
	if len(operators) == 0 {
		return nil, nil, false
	}
	branches = append(branches, strings.TrimSpace(query[start:]))
	return branches, operators, true
}

func hasNonUnionSetOperator(operators []intersectExceptOperator) bool {
	for _, operator := range operators {
		if operator.kind != "UNION" {
			return true
		}
	}
	return false
}

func isSQLWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n'
}

func matchingParenIndex(input string, open int) int {
	if open < 0 || open >= len(input) || input[open] != '(' {
		return -1
	}
	depth := 0
	var quote byte
	for i := open; i < len(input); i++ {
		ch := input[i]
		if quote != 0 {
			if ch == quote && (i == 0 || input[i-1] != '\\') {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

var informationSchemaMetadataFilterPattern = regexp.MustCompile("(?i)`?\\b(table_schema|table_name|schema_name|column_name|constraint_name|constraint_type)\\b`?\\s*(?:=|like)\\s*(?:'([^']*)'|\"([^\"]*)\")")

var processlistFilterPattern = regexp.MustCompile(`(?is)^\s*(id|user|host|db|command|state|info)\s*(=|!=|<>|like|not\s+like)\s*(?:'((?:''|[^'])*)'|"((?:""|[^"])*)"|([0-9]+))\s*$`)
var processlistNullFilterPattern = regexp.MustCompile(`(?is)^\s*(id|user|host|db|command|state|info)\s+is\s+(not\s+)?null\s*$`)
var processlistInFilterPattern = regexp.MustCompile(`(?is)^\s*(id|user|host|db|command|state|info)\s+(not\s+)?in\s*\((.*)\)\s*$`)
var processlistFunctionFilterPattern = regexp.MustCompile(`(?is)^\s*(id|user|host|db|command|state|info)\s*(=|!=|<>)\s*(connection_id\(\))\s*$`)

func performanceSchemaTimerWait(latency time.Duration) int64 {
	timer := latency.Nanoseconds() * 1000
	if timer <= 0 {
		return 1
	}
	return timer
}

var performanceSchemaTimerOrigin = time.Now()

func performanceSchemaTimerTimestamp(at time.Time) int64 {
	if at.IsZero() {
		return 1
	}
	timer := at.Sub(performanceSchemaTimerOrigin).Nanoseconds() * 1000
	if timer <= 0 {
		return 1
	}
	return timer
}

func performanceSchemaWaitTimerValues(started time.Time, duration time.Duration, timed, current bool) (interface{}, interface{}, interface{}) {
	if !timed {
		return nil, nil, nil
	}
	wait := performanceSchemaTimerWait(duration)
	if current {
		end := performanceSchemaTimerTimestamp(time.Now())
		start := end - wait
		if start <= 0 {
			start = 1
		}
		return start, end, end - start
	}
	start := performanceSchemaTimerTimestamp(started)
	return start, start + wait, wait
}

func (e *XMySQLExecutor) executeInformationSchemaMetadataSelect(query string, sessions ...server.MySQLServerSession) (*SelectResult, bool, error) {
	if !isInformationSchemaMetadataQuery(query) && !isMySQLMetadataQuery(query) && !isPerformanceSchemaMetadataQuery(query) {
		return nil, false, nil
	}

	lower := strings.ToLower(query)
	var session server.MySQLServerSession
	if len(sessions) > 0 {
		session = sessions[0]
	}
	switch {
	case strings.Contains(lower, "information_schema.processlist"):
		return e.executeProcesslistSelect(session, query), true, nil
	case isInformationSchemaRegistryQuery(lower):
		return e.executeInformationSchemaRegistrySelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.enabled_roles"):
		return e.executeInformationSchemaEnabledRolesSelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.applicable_roles"):
		return e.executeInformationSchemaApplicableRolesSelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.administrable_role_authorizations"):
		return e.executeInformationSchemaAdministrableRoleAuthorizationsSelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.role_table_grants"):
		return e.executeInformationSchemaRoleTableGrantsSelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.role_column_grants"):
		return e.executeInformationSchemaRoleColumnGrantsSelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.role_routine_grants"):
		return e.executeInformationSchemaRoleRoutineGrantsSelect(session, query), true, nil
	case strings.Contains(lower, "information_schema.innodb_tablespaces"):
		return e.executeInformationSchemaInnoDBTablespacesSelect(query), true, nil
	case strings.Contains(lower, "information_schema.innodb_datafiles"):
		return e.executeInformationSchemaInnoDBDatafilesSelect(query), true, nil
	case strings.Contains(lower, "information_schema.innodb_tablestats"):
		return e.executeInformationSchemaInnoDBTableStatsSelect(query), true, nil
	case strings.Contains(lower, "information_schema.innodb_indexes"):
		return e.executeInformationSchemaInnoDBIndexesSelect(query), true, nil
	case strings.Contains(lower, "information_schema.innodb_columns"):
		return e.executeInformationSchemaInnoDBColumnsSelect(query), true, nil
	case strings.Contains(lower, "information_schema.innodb_metrics"):
		return e.executeInformationSchemaInnoDBMetricsSelect(query), true, nil
	case strings.Contains(lower, "information_schema.innodb_foreign_cols"):
		return e.executeInformationSchemaInnoDBForeignColsSelect(query), true, nil
	case strings.Contains(lower, "information_schema.innodb_foreign"):
		return e.executeInformationSchemaInnoDBForeignSelect(query), true, nil
	case strings.Contains(lower, "information_schema.innodb_trx"):
		return e.executeInformationSchemaInnoDBTrxSelect(query), true, nil
	case strings.Contains(lower, "information_schema.innodb_lock_waits"):
		return e.executeInformationSchemaInnoDBLockWaitsSelect(query), true, nil
	case strings.Contains(lower, "information_schema.innodb_locks"):
		return e.executeInformationSchemaInnoDBLocksSelect(query), true, nil
	case strings.Contains(lower, "performance_schema.events_statements_summary_by_digest"):
		return e.executePerformanceSchemaStatementsSelect(query), true, nil
	case strings.Contains(lower, "performance_schema.events_statements_summary_by_program"):
		return e.executePerformanceSchemaProgramSummarySelect(query), true, nil
	case strings.Contains(lower, "performance_schema.events_statements_current"):
		return e.executePerformanceSchemaStatementHistorySelect("performance_schema.events_statements_current", true, query), true, nil
	case strings.Contains(lower, "performance_schema.events_statements_history_long"):
		return e.executePerformanceSchemaStatementHistorySelect("performance_schema.events_statements_history_long", false, query), true, nil
	case strings.Contains(lower, "performance_schema.events_statements_history"):
		return e.executePerformanceSchemaStatementHistorySelect("performance_schema.events_statements_history", false, query), true, nil
	case strings.Contains(lower, "performance_schema.events_stages_current"):
		return e.executePerformanceSchemaStageHistorySelect("performance_schema.events_stages_current", true, query), true, nil
	case strings.Contains(lower, "performance_schema.events_stages_history_long"):
		return e.executePerformanceSchemaStageHistorySelect("performance_schema.events_stages_history_long", false, query), true, nil
	case strings.Contains(lower, "performance_schema.events_stages_history"):
		return e.executePerformanceSchemaStageHistorySelect("performance_schema.events_stages_history", false, query), true, nil
	case strings.Contains(lower, "performance_schema.events_transactions_current"):
		return e.executePerformanceSchemaTransactionsSelect(query, session, "performance_schema.events_transactions_current"), true, nil
	case strings.Contains(lower, "performance_schema.events_transactions_history_long"):
		return e.executePerformanceSchemaTransactionsSelect(query, session, "performance_schema.events_transactions_history_long"), true, nil
	case strings.Contains(lower, "performance_schema.events_transactions_history"):
		return e.executePerformanceSchemaTransactionsSelect(query, session, "performance_schema.events_transactions_history"), true, nil
	case strings.Contains(lower, "performance_schema.events_stages_summary_by_thread_by_event_name"):
		return e.executePerformanceSchemaStageSummarySelect(query, true), true, nil
	case strings.Contains(lower, "performance_schema.events_stages_summary_by_account_by_event_name"):
		return e.executePerformanceSchemaStageSummaryRegistrySelect(query, "account"), true, nil
	case strings.Contains(lower, "performance_schema.events_stages_summary_by_host_by_event_name"):
		return e.executePerformanceSchemaStageSummaryRegistrySelect(query, "host"), true, nil
	case strings.Contains(lower, "performance_schema.events_stages_summary_by_user_by_event_name"):
		return e.executePerformanceSchemaStageSummaryRegistrySelect(query, "user"), true, nil
	case strings.Contains(lower, "performance_schema.events_stages_summary_global_by_event_name"):
		return e.executePerformanceSchemaStageSummarySelect(query, false), true, nil
	case strings.Contains(lower, "performance_schema.events_statements_summary_by_thread_by_event_name"):
		return e.executePerformanceSchemaStatementSummaryRegistrySelect(query, "thread"), true, nil
	case strings.Contains(lower, "performance_schema.events_statements_summary_by_account_by_event_name"):
		return e.executePerformanceSchemaStatementSummaryRegistrySelect(query, "account"), true, nil
	case strings.Contains(lower, "performance_schema.events_statements_summary_by_host_by_event_name"):
		return e.executePerformanceSchemaStatementSummaryRegistrySelect(query, "host"), true, nil
	case strings.Contains(lower, "performance_schema.events_statements_summary_by_user_by_event_name"):
		return e.executePerformanceSchemaStatementSummaryRegistrySelect(query, "user"), true, nil
	case strings.Contains(lower, "performance_schema.events_statements_summary_global_by_event_name"):
		return e.executePerformanceSchemaStatementSummaryRegistrySelect(query, "global"), true, nil
	case strings.Contains(lower, "performance_schema.events_transactions_summary_by_thread_by_event_name"):
		return e.executePerformanceSchemaTransactionSummaryRegistrySelect(query, "thread"), true, nil
	case strings.Contains(lower, "performance_schema.events_transactions_summary_by_account_by_event_name"):
		return e.executePerformanceSchemaTransactionSummaryRegistrySelect(query, "account"), true, nil
	case strings.Contains(lower, "performance_schema.events_transactions_summary_by_host_by_event_name"):
		return e.executePerformanceSchemaTransactionSummaryRegistrySelect(query, "host"), true, nil
	case strings.Contains(lower, "performance_schema.events_transactions_summary_by_user_by_event_name"):
		return e.executePerformanceSchemaTransactionSummaryRegistrySelect(query, "user"), true, nil
	case strings.Contains(lower, "performance_schema.events_transactions_summary_global_by_event_name"):
		return e.executePerformanceSchemaTransactionSummaryRegistrySelect(query, "global"), true, nil
	case strings.Contains(lower, "performance_schema.data_lock_waits"):
		return e.executePerformanceSchemaDataLockWaitsSelect(query), true, nil
	case strings.Contains(lower, "performance_schema.data_locks"):
		return e.executePerformanceSchemaDataLocksSelect(query), true, nil
	case strings.Contains(lower, "performance_schema.metadata_locks"):
		return e.executePerformanceSchemaMetadataLocksSelect(query), true, nil
	case strings.Contains(lower, "performance_schema.events_waits_summary_by_thread_by_event_name"):
		return e.executePerformanceSchemaWaitSummarySelect(query, true), true, nil
	case strings.Contains(lower, "performance_schema.events_waits_summary_by_instance"):
		return e.executePerformanceSchemaWaitSummaryByInstanceSelect(query), true, nil
	case strings.Contains(lower, "performance_schema.events_waits_summary_by_account_by_event_name"):
		return e.executePerformanceSchemaWaitSummaryRegistrySelect(query, "account"), true, nil
	case strings.Contains(lower, "performance_schema.events_waits_summary_by_host_by_event_name"):
		return e.executePerformanceSchemaWaitSummaryRegistrySelect(query, "host"), true, nil
	case strings.Contains(lower, "performance_schema.events_waits_summary_by_user_by_event_name"):
		return e.executePerformanceSchemaWaitSummaryRegistrySelect(query, "user"), true, nil
	case strings.Contains(lower, "performance_schema.events_waits_summary_global_by_event_name"):
		return e.executePerformanceSchemaWaitSummarySelect(query, false), true, nil
	case strings.Contains(lower, "performance_schema.table_lock_waits_summary_by_table"):
		return e.executePerformanceSchemaTableLockWaitSummarySelect(query), true, nil
	case strings.Contains(lower, "performance_schema.table_io_waits_summary_by_index_usage"):
		return e.executePerformanceSchemaTableIOSummarySelect(query, true), true, nil
	case strings.Contains(lower, "performance_schema.table_io_waits_summary_by_table"):
		return e.executePerformanceSchemaTableIOSummarySelect(query, false), true, nil
	case strings.Contains(lower, "performance_schema.events_waits_current"):
		return e.executePerformanceSchemaEventsWaitsSelect(query), true, nil
	case strings.Contains(lower, "performance_schema.events_waits_history"):
		return e.executePerformanceSchemaEventsWaitsSelect(query), true, nil
	case strings.Contains(lower, "performance_schema.events_waits_history_long"):
		return e.executePerformanceSchemaEventsWaitsSelect(query), true, nil
	case strings.Contains(lower, "performance_schema.memory_summary_global_by_event_name"):
		return e.executePerformanceSchemaMemorySummarySelect(query), true, nil
	case strings.Contains(lower, "performance_schema.memory_summary_by_thread_by_event_name"):
		return e.executePerformanceSchemaMemorySummaryByThreadSelect(query, session), true, nil
	case strings.Contains(lower, "performance_schema.memory_summary_by_account_by_event_name"):
		return e.executePerformanceSchemaMemorySummaryByIdentitySelect(query, "account"), true, nil
	case strings.Contains(lower, "performance_schema.memory_summary_by_host_by_event_name"):
		return e.executePerformanceSchemaMemorySummaryByIdentitySelect(query, "host"), true, nil
	case strings.Contains(lower, "performance_schema.memory_summary_by_user_by_event_name"):
		return e.executePerformanceSchemaMemorySummaryByIdentitySelect(query, "user"), true, nil
	case strings.Contains(lower, "performance_schema.performance_timers"):
		return e.executePerformanceSchemaPerformanceTimersSelect(query), true, nil
	case strings.Contains(lower, "performance_schema.host_cache"):
		return e.executePerformanceSchemaHostCacheSelect(query, session), true, nil
	case strings.Contains(lower, "performance_schema.mutex_instances"):
		return e.executePerformanceSchemaSyncInstancesSelect(query, "performance_schema.mutex_instances"), true, nil
	case strings.Contains(lower, "performance_schema.rwlock_instances"):
		return e.executePerformanceSchemaSyncInstancesSelect(query, "performance_schema.rwlock_instances"), true, nil
	case strings.Contains(lower, "performance_schema.objects_summary_global_by_type"):
		return e.executePerformanceSchemaObjectsSummarySelect(query), true, nil
	case strings.Contains(lower, "performance_schema.threads"):
		return e.executePerformanceSchemaThreadsSelect(query, session), true, nil
	case strings.Contains(lower, "performance_schema.setup_threads"):
		return e.executePerformanceSchemaSetupThreadsSelect(query, session), true, nil
	case strings.Contains(lower, "performance_schema.session_connect_attrs"):
		return e.executePerformanceSchemaSessionConnectAttrsSelect(query, session), true, nil
	case strings.Contains(lower, "performance_schema.session_account_connect_attrs"):
		return e.executePerformanceSchemaSessionAccountConnectAttrsSelect(query, session), true, nil
	case strings.Contains(lower, "performance_schema.socket_instances"):
		return e.executePerformanceSchemaSocketInstancesSelect(query, session), true, nil
	case strings.Contains(lower, "performance_schema.socket_summary_by_instance"):
		return e.executePerformanceSchemaSocketSummarySelect(query, session, true), true, nil
	case strings.Contains(lower, "performance_schema.socket_summary_by_event_name"):
		return e.executePerformanceSchemaSocketSummarySelect(query, session, false), true, nil
	case strings.Contains(lower, "performance_schema.file_instances"):
		return e.executePerformanceSchemaFileInstancesSelect(query), true, nil
	case strings.Contains(lower, "performance_schema.file_summary_by_instance"):
		return e.executePerformanceSchemaFileSummarySelect(query, true), true, nil
	case strings.Contains(lower, "performance_schema.file_summary_by_event_name"):
		return e.executePerformanceSchemaFileSummarySelect(query, false), true, nil
	case strings.Contains(lower, "performance_schema.users"):
		return e.executePerformanceSchemaConnectionSummarySelect(query, "performance_schema.users", session), true, nil
	case strings.Contains(lower, "performance_schema.accounts"):
		return e.executePerformanceSchemaConnectionSummarySelect(query, "performance_schema.accounts", session), true, nil
	case strings.Contains(lower, "performance_schema.hosts"):
		return e.executePerformanceSchemaConnectionSummarySelect(query, "performance_schema.hosts", session), true, nil
	case strings.Contains(lower, "performance_schema.setup_consumers"):
		return e.executePerformanceSchemaSetupConsumersSelect(query), true, nil
	case strings.Contains(lower, "performance_schema.setup_instruments"):
		return e.executePerformanceSchemaSetupInstrumentsSelect(query), true, nil
	case strings.Contains(lower, "performance_schema.setup_actors"):
		return e.executePerformanceSchemaSetupActorsSelect(query), true, nil
	case strings.Contains(lower, "performance_schema.setup_objects"):
		return e.executePerformanceSchemaSetupObjectsSelect(query), true, nil
	case strings.Contains(lower, "performance_schema.setup_timers"):
		return e.executePerformanceSchemaSetupTimersSelect(query), true, nil
	case strings.Contains(lower, "performance_schema.global_variables"):
		return e.executePerformanceSchemaVariablesSelect("performance_schema.global_variables", query), true, nil
	case strings.Contains(lower, "performance_schema.session_variables"):
		return e.executePerformanceSchemaSessionVariablesSelect("performance_schema.session_variables", query, session), true, nil
	case strings.Contains(lower, "performance_schema.global_status"):
		return e.executePerformanceSchemaStatusSelect("performance_schema.global_status", query, session), true, nil
	case strings.Contains(lower, "performance_schema.session_status"):
		return e.executePerformanceSchemaStatusSelect("performance_schema.session_status", query, session), true, nil
	case strings.Contains(lower, "performance_schema.replication_"):
		return e.executePerformanceSchemaReplicationSelect(query, performanceSchemaTableName(query)), true, nil
	case strings.Contains(lower, "performance_schema.replication_connection_status"):
		return e.executePerformanceSchemaReplicationSelect(query, "replication_connection_status"), true, nil
	case strings.Contains(lower, "performance_schema.replication_applier_status"):
		return e.executePerformanceSchemaReplicationSelect(query, "replication_applier_status"), true, nil
	case strings.Contains(lower, "performance_schema.replication_connection_configuration"):
		return e.executePerformanceSchemaReplicationSelect(query, "replication_connection_configuration"), true, nil
	case strings.Contains(lower, "performance_schema.replication_applier_configuration"):
		return e.executePerformanceSchemaReplicationSelect(query, "replication_applier_configuration"), true, nil
	case strings.Contains(lower, "performance_schema.log_status"):
		return e.executePerformanceSchemaReplicationSelect(query, "log_status"), true, nil
	case strings.Contains(lower, "performance_schema.status_by_account"):
		return e.executePerformanceSchemaSessionRuntimeRegistrySelect(query, "status_by_account", session), true, nil
	case strings.Contains(lower, "performance_schema.status_by_host"):
		return e.executePerformanceSchemaSessionRuntimeRegistrySelect(query, "status_by_host", session), true, nil
	case strings.Contains(lower, "performance_schema.status_by_thread"):
		return e.executePerformanceSchemaSessionRuntimeRegistrySelect(query, "status_by_thread", session), true, nil
	case strings.Contains(lower, "performance_schema.status_by_user"):
		return e.executePerformanceSchemaSessionRuntimeRegistrySelect(query, "status_by_user", session), true, nil
	case strings.Contains(lower, "performance_schema.variables_by_thread"):
		return e.executePerformanceSchemaSessionRuntimeRegistrySelect(query, "variables_by_thread", session), true, nil
	case strings.Contains(lower, "performance_schema.user_variables_by_thread"):
		return e.executePerformanceSchemaSessionRuntimeRegistrySelect(query, "user_variables_by_thread", session), true, nil
	case strings.Contains(lower, "performance_schema.prepared_statements_instances"):
		return e.executePerformanceSchemaPreparedStatementsSelect(query, session), true, nil
	case strings.Contains(lower, "performance_schema.events_errors_summary_by_account_by_error"):
		return e.executePerformanceSchemaErrorSummarySelect(query, "events_errors_summary_by_account_by_error"), true, nil
	case strings.Contains(lower, "performance_schema.events_errors_summary_by_host_by_error"):
		return e.executePerformanceSchemaErrorSummarySelect(query, "events_errors_summary_by_host_by_error"), true, nil
	case strings.Contains(lower, "performance_schema.events_errors_summary_by_thread_by_error"):
		return e.executePerformanceSchemaErrorSummarySelect(query, "events_errors_summary_by_thread_by_error"), true, nil
	case strings.Contains(lower, "performance_schema.events_errors_summary_by_user_by_error"):
		return e.executePerformanceSchemaErrorSummarySelect(query, "events_errors_summary_by_user_by_error"), true, nil
	case strings.Contains(lower, "performance_schema.events_errors_summary_global_by_error"):
		return e.executePerformanceSchemaErrorSummarySelect(query, "events_errors_summary_global_by_error"), true, nil
	case strings.Contains(lower, "performance_schema.events_statements_histogram_by_digest"):
		return e.executePerformanceSchemaStatementHistogramSelect(query, true), true, nil
	case strings.Contains(lower, "performance_schema.events_statements_histogram_global"):
		return e.executePerformanceSchemaStatementHistogramSelect(query, false), true, nil
	case isPerformanceSchemaRegistryQuery(lower):
		return e.executePerformanceSchemaRegistrySelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.system_variables"):
		return e.executePerformanceSchemaVariablesSelect("information_schema.system_variables", query), true, nil
	case strings.Contains(lower, "information_schema.global_variables"):
		return e.executePerformanceSchemaVariablesSelect("information_schema.global_variables", query), true, nil
	case strings.Contains(lower, "information_schema.session_variables"):
		return e.executePerformanceSchemaSessionVariablesSelect("information_schema.session_variables", query, session), true, nil
	case strings.Contains(lower, "information_schema.column_privileges") &&
		strings.Contains(lower, "information_schema.table_privileges") &&
		strings.Contains(lower, " union all "):
		return newInformationSchemaSelectResult(
			"information_schema_privileges_union",
			[]string{"GRANTEE", "TABLE_NAME", "COLUMN_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE"},
			nil,
		), true, nil
	case strings.Contains(lower, "information_schema.tablespaces"):
		return e.executeInformationSchemaTablespacesSelect(query), true, nil
	case strings.Contains(lower, "information_schema.tables"):
		return e.executeInformationSchemaTablesSelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.schemata"):
		return e.executeInformationSchemaSchemataSelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.columns"):
		return e.executeInformationSchemaColumnsSelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.routines"):
		if err := e.checkStoredRoutineShow(&ExecutionContext{Session: session}); err != nil {
			return nil, true, err
		}
		result, err := e.executeInformationSchemaRoutinesSelect(query, session)
		return result, true, err
	case strings.Contains(lower, "information_schema.parameters"):
		if err := e.checkStoredRoutineShow(&ExecutionContext{Session: session}); err != nil {
			return nil, true, err
		}
		result, err := e.executeInformationSchemaParametersSelect(query, session)
		return result, true, err
	case strings.Contains(lower, "information_schema.statistics"):
		return e.executeInformationSchemaStatisticsSelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.key_column_usage"):
		return e.executeInformationSchemaKeyColumnUsageSelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.table_constraints"):
		return e.executeInformationSchemaTableConstraintsSelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.check_constraints"):
		return e.executeInformationSchemaCheckConstraintsSelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.referential_constraints"):
		return e.executeInformationSchemaReferentialConstraintsSelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.column_privileges"):
		return e.executeInformationSchemaColumnPrivilegesSelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.table_privileges"):
		return e.executeInformationSchemaTablePrivilegesSelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.views"):
		return e.executeInformationSchemaViewsSelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.partitions"):
		return e.executeInformationSchemaPartitionsSelect(query), true, nil
	case strings.Contains(lower, "information_schema.triggers"):
		result, err := e.executeInformationSchemaTriggersSelect(query, session)
		return result, true, err
	case strings.Contains(lower, "information_schema.events"):
		result, err := e.executeInformationSchemaEventsSelect(query, session)
		return result, true, err
	case strings.Contains(lower, "information_schema.collations"):
		return executeInformationSchemaCollationsSelect(query), true, nil
	case strings.Contains(lower, "information_schema.character_sets"):
		return executeInformationSchemaCharacterSetsSelect(query), true, nil
	case strings.Contains(lower, "information_schema.engines"):
		return executeInformationSchemaEnginesSelect(query), true, nil
	case strings.Contains(lower, "information_schema.plugins"):
		return executeInformationSchemaPluginsSelect(query), true, nil
	case strings.Contains(lower, "information_schema.column_statistics"):
		return e.executeInformationSchemaColumnStatisticsSelect(query), true, nil
	case strings.Contains(lower, "information_schema.files"):
		return e.executeInformationSchemaFilesSelect(query), true, nil
	case strings.Contains(lower, "information_schema.user_privileges"):
		return e.executeInformationSchemaUserPrivilegesSelect(query, session), true, nil
	case strings.Contains(lower, "information_schema.schema_privileges"):
		return e.executeInformationSchemaSchemaPrivilegesSelect(query, session), true, nil
	case strings.Contains(lower, "mysql.procs_priv"):
		return e.executeMySQLProcsPrivSelect(query), true, nil
	case strings.Contains(lower, "mysql.proxies_priv"):
		return e.executeMySQLProxiesPrivSelect(query), true, nil
	case strings.Contains(lower, "mysql.global_grants"):
		return e.executeMySQLGlobalGrantsSelect(query), true, nil
	case strings.Contains(lower, "mysql.user"):
		return e.executeMySQLUserSelect(query), true, nil
	case strings.Contains(lower, "mysql.db"):
		return e.executeMySQLDBSelect(query), true, nil
	case strings.Contains(lower, "mysql.tables_priv"):
		return e.executeMySQLTablesPrivSelect(query), true, nil
	case strings.Contains(lower, "mysql.columns_priv"):
		return e.executeMySQLColumnsPrivSelect(query), true, nil
	default:
		return nil, false, nil
	}
}

func (e *XMySQLExecutor) executePerformanceSchemaStatementsSelect(query string) *SelectResult {
	const table = "events_statements_summary_by_digest"
	columns := requestedInformationSchemaColumns(query, performanceSchemaTableColumns(table))
	if e == nil || e.metricsRecorder == nil {
		return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
	}
	type digestAggregate struct {
		schema, digest, digestText, sample                         string
		count, sum, min, max, errors                               int64
		warnings, rowsAffected, rowsSent, rowsExamined, selectScan int64
		firstSeen, lastSeen, sampleSeen                            time.Time
		sampleTimer                                                int64
		latencies                                                  []int64
	}
	aggregates := make(map[string]*digestAggregate)
	for _, event := range e.metricsRecorder.StatementHistory() {
		digestText := performanceSchemaDigestText(event.SQL)
		key := event.Schema + "\x00" + digestText
		aggregate := aggregates[key]
		if aggregate == nil {
			digest := sha256.Sum256([]byte(digestText))
			aggregate = &digestAggregate{
				schema: event.Schema, digest: fmt.Sprintf("%x", digest[:]), digestText: digestText,
				min: -1, sample: event.SQL, sampleSeen: event.Time,
			}
			aggregates[key] = aggregate
		}
		timer := event.Latency.Nanoseconds() * 1000
		if timer < 0 {
			timer = 0
		}
		if aggregate.count == 0 {
			aggregate.firstSeen = event.Time
			aggregate.sampleTimer = timer
		}
		if aggregate.firstSeen.IsZero() || (!event.Time.IsZero() && event.Time.Before(aggregate.firstSeen)) {
			aggregate.firstSeen = event.Time
		}
		if aggregate.lastSeen.IsZero() || event.Time.After(aggregate.lastSeen) {
			aggregate.lastSeen = event.Time
		}
		if aggregate.sampleSeen.IsZero() {
			aggregate.sampleSeen = event.Time
		}
		aggregate.count++
		aggregate.sum += timer
		if aggregate.min < 0 || timer < aggregate.min {
			aggregate.min = timer
		}
		if timer > aggregate.max {
			aggregate.max = timer
		}
		aggregate.latencies = append(aggregate.latencies, timer)
		if strings.EqualFold(event.Status, "error") {
			aggregate.errors++
		}
		aggregate.warnings += event.Warnings
		aggregate.rowsAffected += event.RowsAffected
		aggregate.rowsSent += event.RowsSent
		aggregate.rowsExamined += event.RowsExamined
		aggregate.selectScan += event.SelectScan
		if aggregate.sample == "" {
			aggregate.sample = event.SQL
			aggregate.sampleTimer = timer
		}
	}
	keys := make([]string, 0, len(aggregates))
	for key := range aggregates {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([][]interface{}, 0, len(keys))
	for _, key := range keys {
		aggregate := aggregates[key]
		if !performanceSchemaTLSStatusQueryMatches(query, "schema_name", aggregate.schema) ||
			!performanceSchemaTLSStatusQueryMatches(query, "digest", aggregate.digest) ||
			!performanceSchemaTLSStatusQueryMatches(query, "digest_text", aggregate.digestText) {
			continue
		}
		average := int64(0)
		if aggregate.count > 0 {
			average = aggregate.sum / aggregate.count
		}
		values := map[string]interface{}{
			"SCHEMA_NAME": aggregate.schema, "DIGEST": aggregate.digest, "DIGEST_TEXT": aggregate.digestText,
			"COUNT_STAR": aggregate.count, "SUM_TIMER_WAIT": aggregate.sum, "MIN_TIMER_WAIT": aggregate.min,
			"AVG_TIMER_WAIT": average, "MAX_TIMER_WAIT": aggregate.max, "SUM_LOCK_TIME": int64(0),
			"SUM_ERRORS": aggregate.errors, "SUM_WARNINGS": aggregate.warnings, "SUM_ROWS_AFFECTED": aggregate.rowsAffected,
			"SUM_ROWS_SENT": aggregate.rowsSent, "SUM_ROWS_EXAMINED": aggregate.rowsExamined, "SUM_CREATED_TMP_DISK_TABLES": int64(0),
			"SUM_CREATED_TMP_TABLES": int64(0), "SUM_SELECT_FULL_JOIN": int64(0), "SUM_SELECT_FULL_RANGE_JOIN": int64(0),
			"SUM_SELECT_RANGE": int64(0), "SUM_SELECT_RANGE_CHECK": int64(0), "SUM_SELECT_SCAN": aggregate.selectScan,
			"SUM_SORT_MERGE_PASSES": int64(0), "SUM_SORT_RANGE": int64(0), "SUM_SORT_ROWS": int64(0),
			"SUM_SORT_SCAN": int64(0), "SUM_NO_INDEX_USED": int64(0), "SUM_NO_GOOD_INDEX_USED": int64(0),
			"SUM_CPU_TIME": int64(0), "MAX_CONTROLLED_MEMORY": int64(0), "MAX_TOTAL_MEMORY": int64(0),
			"COUNT_SECONDARY": int64(0), "FIRST_SEEN": aggregate.firstSeen, "LAST_SEEN": aggregate.lastSeen,
			"QUANTILE_95":       performanceSchemaObservedQuantile(aggregate.latencies, 95, 100),
			"QUANTILE_99":       performanceSchemaObservedQuantile(aggregate.latencies, 99, 100),
			"QUANTILE_999":      performanceSchemaObservedQuantile(aggregate.latencies, 999, 1000),
			"QUERY_SAMPLE_TEXT": aggregate.sample, "QUERY_SAMPLE_SEEN": aggregate.sampleSeen,
			"QUERY_SAMPLE_TIMER_WAIT": aggregate.sampleTimer,
		}
		if performanceSchemaLockValuesMatch(query, values) {
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("performance_schema."+table, columns, rows)
}

// performanceSchemaObservedQuantile returns a nearest-rank quantile from the
// bounded statement observations.  The copy keeps the aggregate's insertion
// order stable for deterministic sample selection and preserves the recorder
// history as the source of truth.
func performanceSchemaObservedQuantile(values []int64, numerator, denominator int64) int64 {
	if len(values) == 0 || numerator <= 0 || denominator <= 0 {
		return 0
	}
	sortedValues := append([]int64(nil), values...)
	sort.Slice(sortedValues, func(i, j int) bool { return sortedValues[i] < sortedValues[j] })
	rank := (int64(len(sortedValues))*numerator + denominator - 1) / denominator
	if rank < 1 {
		rank = 1
	}
	if rank > int64(len(sortedValues)) {
		rank = int64(len(sortedValues))
	}
	return sortedValues[rank-1]
}

// performanceSchemaDigestText uses the SQL parser's value normalizer so
// literals share one digest while identifiers and statement structure remain
// visible. MySQL's digest algorithm has more server-specific token rules; a
// parse failure therefore falls back to a stable whitespace-normalized text
// instead of dropping the runtime observation.
func performanceSchemaDigestText(sql string) string {
	raw := strings.TrimSpace(sql)
	if raw == "" {
		return ""
	}
	stmt, err := sqlparser.Parse(raw)
	if err != nil {
		return performanceSchemaLexicalDigestText(raw)
	}
	bindVars := make(map[string]*querypb.BindVariable)
	sqlparser.Normalize(stmt, bindVars, "digest")
	text := sqlparser.String(stmt)
	text = regexp.MustCompile(`::digest\d+|:digest\d+`).ReplaceAllString(text, "?")
	if strings.Contains(text, ":digest") || strings.Contains(text, "::digest") || text == strings.Join(strings.Fields(raw), " ") {
		return performanceSchemaLexicalDigestText(raw)
	}
	return strings.Join(strings.Fields(text), " ")
}

func performanceSchemaLexicalDigestText(raw string) string {
	var builder strings.Builder
	spacePending := false
	flushSpace := func() {
		if spacePending && builder.Len() > 0 {
			builder.WriteByte(' ')
		}
		spacePending = false
	}
	for index := 0; index < len(raw); {
		ch := raw[index]
		if ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' {
			spacePending = true
			index++
			continue
		}
		if ch == '#' || (ch == '-' && index+2 < len(raw) && raw[index+1] == '-' && (raw[index+2] == ' ' || raw[index+2] == '\t')) {
			for index < len(raw) && raw[index] != '\n' {
				index++
			}
			spacePending = true
			continue
		}
		if ch == '/' && index+1 < len(raw) && raw[index+1] == '*' {
			index += 2
			for index+1 < len(raw) && !(raw[index] == '*' && raw[index+1] == '/') {
				index++
			}
			if index+1 < len(raw) {
				index += 2
			}
			spacePending = true
			continue
		}
		if ch == '\'' || ch == '"' {
			flushSpace()
			quote := ch
			index++
			for index < len(raw) {
				if raw[index] == '\\' && index+1 < len(raw) {
					index += 2
					continue
				}
				if raw[index] == quote {
					if index+1 < len(raw) && raw[index+1] == quote {
						index += 2
						continue
					}
					index++
					break
				}
				index++
			}
			builder.WriteByte('?')
			continue
		}
		if ch >= '0' && ch <= '9' && (index == 0 || !isPerformanceSchemaDigestIdentifierChar(raw[index-1])) {
			flushSpace()
			index++
			for index < len(raw) {
				next := raw[index]
				if (next >= '0' && next <= '9') || next == '.' || next == 'x' || next == 'X' || (next >= 'a' && next <= 'f') || (next >= 'A' && next <= 'F') || next == '+' || next == '-' {
					index++
					continue
				}
				break
			}
			builder.WriteByte('?')
			continue
		}
		flushSpace()
		builder.WriteByte(ch)
		index++
	}
	return strings.TrimSpace(builder.String())
}

func isPerformanceSchemaDigestIdentifierChar(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '$' || ch == '.'
}

func (e *XMySQLExecutor) executePerformanceSchemaStatementHistorySelect(name string, currentOnly bool, queries ...string) *SelectResult {
	query := ""
	if len(queries) > 0 {
		query = queries[0]
	}
	defaults := performanceSchemaStatementEventColumns
	columns := requestedInformationSchemaColumns(query, defaults)
	if e == nil || e.metricsRecorder == nil {
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	if !e.performanceSchemaConsumerEnabled(name[strings.LastIndex(name, ".")+1:]) {
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	events := e.metricsRecorder.StatementHistory()
	if !currentOnly {
		events = performanceSchemaHistoricalStatementEvents(events)
	}
	if currentOnly {
		events = e.activeStatementEvents()
		// Internal executor tests and server-side statements without a client
		// session have thread ID 0. Preserve one such event for compatibility;
		// completed client statements must never be reported as current.
		if len(events) == 0 && len(e.metricsRecorder.StatementHistory()) > 0 {
			history := e.metricsRecorder.StatementHistory()
			if history[len(history)-1].ThreadID == 0 {
				events = history[len(history)-1:]
			}
		}
	}
	rows := make([][]interface{}, 0, len(events))
	startID := int64(1)
	for index, event := range events {
		instrumentEnabled, instrumentTimed := e.performanceSchemaInstrumentSetting("statement/sql/" + strings.ToLower(event.StatementType))
		if !instrumentEnabled {
			continue
		}
		eventID := startID + int64(index)
		eventName := "statement/sql/" + strings.ToLower(event.StatementType)
		errno := int64(0)
		var state interface{}
		if event.Status == "error" {
			errno = 1105
			state = "HY000"
		}
		if !performanceSchemaSummaryFilterMatches(query, "event_id", fmt.Sprint(eventID)) ||
			!performanceSchemaSummaryFilterMatches(query, "thread_id", fmt.Sprint(event.ThreadID)) ||
			!performanceSchemaSummaryFilterMatches(query, "event_name", eventName) ||
			!performanceSchemaSummaryFilterMatches(query, "sql_text", event.SQL) ||
			!performanceSchemaSummaryFilterMatches(query, "current_schema", event.Schema) ||
			!performanceSchemaSummaryFilterMatches(query, "sql_command", strings.ToUpper(event.StatementType)) ||
			!performanceSchemaSummaryFilterMatches(query, "mysql_errno", fmt.Sprint(errno)) ||
			!performanceSchemaSummaryFilterMatches(query, "returned_sqlstate", fmt.Sprint(state)) {
			continue
		}
		timerWait := performanceSchemaTimerWait(event.Latency)
		if !instrumentTimed {
			timerWait = 0
		}
		endEventID := interface{}(eventID)
		if currentOnly {
			endEventID = nil
		}
		digestText := performanceSchemaDigestText(event.SQL)
		digest := sha256.Sum256([]byte(digestText))
		errors := int64(0)
		if event.Status == "error" {
			errors = 1
		}
		rows = append(rows, projectInformationSchemaRow(columns, map[string]interface{}{
			"THREAD_ID": event.ThreadID, "EVENT_ID": eventID, "END_EVENT_ID": endEventID,
			"EVENT_NAME": eventName, "SOURCE": nil, "TIMER_START": nil, "TIMER_END": nil,
			"TIMER_WAIT": timerWait, "LOCK_TIME": nil, "SQL_TEXT": event.SQL,
			"DIGEST": fmt.Sprintf("%x", digest[:]), "DIGEST_TEXT": digestText,
			"CURRENT_SCHEMA": event.Schema, "OBJECT_TYPE": nil, "OBJECT_SCHEMA": nil, "OBJECT_NAME": nil,
			"OBJECT_INSTANCE_BEGIN": nil, "MYSQL_ERRNO": errno, "RETURNED_SQLSTATE": state,
			"MESSAGE_TEXT": nil, "ERRORS": errors, "WARNINGS": event.Warnings, "ROWS_AFFECTED": event.RowsAffected,
			"ROWS_SENT": event.RowsSent, "ROWS_EXAMINED": event.RowsExamined, "CREATED_TMP_DISK_TABLES": nil,
			"CREATED_TMP_TABLES": nil, "SELECT_FULL_JOIN": nil, "SELECT_FULL_RANGE_JOIN": nil,
			"SELECT_RANGE": nil, "SELECT_RANGE_CHECK": nil, "SELECT_SCAN": event.SelectScan, "SORT_MERGE_PASSES": nil,
			"SORT_RANGE": nil, "SORT_ROWS": nil, "SORT_SCAN": nil, "NO_INDEX_USED": nil,
			"NO_GOOD_INDEX_USED": nil, "NESTING_EVENT_ID": nil, "NESTING_EVENT_TYPE": nil,
			"NESTING_LEVEL": int64(0), "STATEMENT_ID": eventID, "CPU_TIME": nil,
			"MAX_CONTROLLED_MEMORY": nil, "MAX_TOTAL_MEMORY": nil, "EXECUTION_ENGINE": "PRIMARY",
			// SQL_COMMAND is retained as a compatibility alias for existing xmysql
			// clients even though it is not part of the upstream event-table shape.
			"SQL_COMMAND": strings.ToUpper(event.StatementType),
		}))
	}
	return newInformationSchemaSelectResult(name, columns, rows)
}

func performanceSchemaHistoricalStatementEvents(events []observabilitymetrics.StatementEvent) []observabilitymetrics.StatementEvent {
	filtered := make([]observabilitymetrics.StatementEvent, 0, len(events))
	for _, event := range events {
		if event.History {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

func (e *XMySQLExecutor) executePerformanceSchemaStageHistorySelect(name string, currentOnly bool, queries ...string) *SelectResult {
	query := ""
	if len(queries) > 0 {
		query = queries[0]
	}
	defaults := []string{
		"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_START",
		"TIMER_END", "TIMER_WAIT", "WORK_COMPLETED", "WORK_ESTIMATED", "NESTING_EVENT_ID",
		"NESTING_EVENT_TYPE",
	}
	columns := requestedInformationSchemaColumns(query, defaults)
	if e == nil || e.metricsRecorder == nil {
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	consumer := "events_stages_history_long"
	if strings.Contains(name, "current") {
		consumer = "events_stages_current"
	} else if strings.HasSuffix(name, "events_stages_history") {
		consumer = "events_stages_history"
	}
	if !e.performanceSchemaConsumerEnabled(consumer) {
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	events := e.metricsRecorder.StatementHistory()
	if !currentOnly {
		events = performanceSchemaHistoricalStatementEvents(events)
	}
	if currentOnly {
		events = e.activeStatementEvents()
		if len(events) == 0 && len(e.metricsRecorder.StatementHistory()) > 0 {
			history := e.metricsRecorder.StatementHistory()
			if history[len(history)-1].ThreadID == 0 {
				events = history[len(history)-1:]
			}
		}
	}
	rows := make([][]interface{}, 0, len(events))
	for index, event := range events {
		instrumentEnabled, instrumentTimed := e.performanceSchemaInstrumentSetting("stage/sql/execute")
		if !instrumentEnabled {
			continue
		}
		threadID := event.ThreadID
		eventID := int64(index + 1)
		isCurrentEvent := currentOnly || strings.EqualFold(event.Status, "running")
		timerWait := performanceSchemaTimerWait(event.Latency)
		if !instrumentTimed || isCurrentEvent {
			timerWait = 0
		}
		if !performanceSchemaSummaryFilterMatches(query, "thread_id", fmt.Sprint(threadID)) ||
			!performanceSchemaSummaryFilterMatches(query, "event_id", fmt.Sprint(eventID)) ||
			!performanceSchemaSummaryFilterMatches(query, "end_event_id", func() string {
				if isCurrentEvent {
					return ""
				}
				return fmt.Sprint(eventID)
			}()) ||
			!performanceSchemaSummaryFilterMatches(query, "event_name", "stage/sql/execute") ||
			!performanceSchemaSummaryFilterMatches(query, "timer_wait", func() string {
				if isCurrentEvent {
					return ""
				}
				return fmt.Sprint(timerWait)
			}()) {
			continue
		}
		var timerStart interface{} = int64(0)
		var timerEnd interface{} = int64(0)
		var timerWaitValue interface{} = timerWait
		var endEventID interface{} = eventID
		if instrumentTimed {
			if isCurrentEvent {
				timerStart = performanceSchemaTimerTimestamp(event.Time)
				timerEnd = nil
				timerWaitValue = nil
				endEventID = nil
			} else {
				timerEndValue := performanceSchemaTimerTimestamp(event.Time)
				timerStartValue := timerEndValue - timerWait
				if timerStartValue <= 0 {
					timerStartValue = 1
				}
				timerStart = timerStartValue
				timerEnd = timerEndValue
			}
		} else if isCurrentEvent {
			timerStart = performanceSchemaTimerTimestamp(event.Time)
			timerEnd = nil
			timerWaitValue = nil
			endEventID = nil
		}
		rows = append(rows, projectInformationSchemaRow(columns, map[string]interface{}{
			"THREAD_ID": threadID, "EVENT_ID": eventID, "END_EVENT_ID": endEventID,
			"EVENT_NAME": "stage/sql/execute", "SOURCE": nil, "TIMER_START": timerStart,
			"TIMER_END": timerEnd, "TIMER_WAIT": timerWaitValue, "WORK_COMPLETED": nil,
			"WORK_ESTIMATED": nil, "NESTING_EVENT_ID": eventID, "NESTING_EVENT_TYPE": "STATEMENT",
		}))
	}
	return newInformationSchemaSelectResult(name, columns, rows)
}

func (e *XMySQLExecutor) executePerformanceSchemaStageSummarySelect(query string, byThread bool) *SelectResult {
	defaults := []string{"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"}
	name := "performance_schema.events_stages_summary_global_by_event_name"
	if byThread {
		defaults = append([]string{"THREAD_ID"}, defaults...)
		name = "performance_schema.events_stages_summary_by_thread_by_event_name"
	}
	columns := requestedInformationSchemaColumns(query, defaults)
	if e == nil || e.metricsRecorder == nil {
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	if !performanceSchemaSummaryFilterMatches(query, "event_name", "stage/sql/execute") {
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	instrumentEnabled, instrumentTimed := e.performanceSchemaInstrumentSetting("stage/sql/execute")
	if !instrumentEnabled {
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	byKey := make(map[string]*performanceSchemaWaitSummary)
	for _, event := range e.metricsRecorder.StatementHistory() {
		if byThread && !performanceSchemaSummaryFilterMatches(query, "thread_id", fmt.Sprint(event.ThreadID)) {
			continue
		}
		timer := performanceSchemaTimerWait(event.Latency)
		if !instrumentTimed {
			timer = 0
		}
		if timer < 0 {
			timer = 0
		}
		key := "stage/sql/execute"
		if byThread {
			key = fmt.Sprintf("%d\x00%s", event.ThreadID, key)
		}
		summary := byKey[key]
		if summary == nil {
			summary = &performanceSchemaWaitSummary{threadID: event.ThreadID, event: "stage/sql/execute", min: timer, max: timer}
			byKey[key] = summary
		}
		summary.count++
		summary.sum += timer
		if timer < summary.min {
			summary.min = timer
		}
		if timer > summary.max {
			summary.max = timer
		}
	}
	values := make([]performanceSchemaWaitSummary, 0, len(byKey))
	for _, summary := range byKey {
		values = append(values, *summary)
	}
	sort.Slice(values, func(i, j int) bool {
		if byThread && values[i].threadID != values[j].threadID {
			return values[i].threadID < values[j].threadID
		}
		return values[i].event < values[j].event
	})
	rows := make([][]interface{}, 0, len(values))
	for _, summary := range values {
		row := map[string]interface{}{
			"THREAD_ID": summary.threadID, "EVENT_NAME": summary.event, "COUNT_STAR": summary.count,
			"SUM_TIMER_WAIT": summary.sum, "MIN_TIMER_WAIT": summary.min, "AVG_TIMER_WAIT": summary.sum / summary.count,
			"MAX_TIMER_WAIT": summary.max,
		}
		if !performanceSchemaLockValuesMatch(query, row) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, row))
	}
	return newInformationSchemaSelectResult(name, columns, rows)
}

func (e *XMySQLExecutor) executePerformanceSchemaTransactionsSelect(query string, session server.MySQLServerSession, name string) *SelectResult {
	defaultColumns := []string{
		"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "STATE", "TRX_ID", "GTID",
		"XID_FORMAT", "XID_GTRID", "XID_BQUAL", "TIMER_START", "TIMER_END", "TIMER_WAIT",
		"ACCESS_MODE", "ISOLATION_LEVEL", "AUTOCOMMIT", "NUMBER_OF_SAVEPOINTS",
		"NUMBER_OF_ROLLBACK_TO_SAVEPOINT", "NUMBER_OF_RELEASE_SAVEPOINT", "OBJECT_INSTANCE_BEGIN",
		"NESTING_EVENT_ID", "NESTING_EVENT_TYPE",
	}
	consumer := "events_transactions_current"
	if strings.Contains(name, "history_long") {
		consumer = "events_transactions_history_long"
	} else if strings.Contains(name, "history") {
		consumer = "events_transactions_history"
	}
	columns := requestedInformationSchemaColumns(query, defaultColumns)
	if e == nil || !e.performanceSchemaInstrumentationConsumersEnabled() || !e.performanceSchemaConsumerEnabled(consumer) || (session == nil && consumer != "events_transactions_history_long") {
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	if consumer != "events_transactions_current" {
		var events []performanceSchemaTransactionEvent
		if consumer == "events_transactions_history_long" {
			e.performanceSchemaMu.RLock()
			events = append(events, e.performanceSchemaTransactionHistoryLong...)
			e.performanceSchemaMu.RUnlock()
		} else {
			events, _ = session.GetParamByName("performance_schema_transaction_history").([]performanceSchemaTransactionEvent)
		}
		rows := make([][]interface{}, 0, len(events))
		for _, event := range events {
			values := informationSchemaRowValues(defaultColumns, event.Row)
			if !performanceSchemaTransactionRowMatches(query, values) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
		return newInformationSchemaSelectResult(name, columns, rows)
	}
	if !sessionBoolParam(session, "in_transaction") {
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	threadID := int64(0)
	if raw := session.GetParamByName("connection_id"); raw != nil {
		threadID = int64Param(raw)
	}
	isolation := transactionIsolationForMetadata(session)
	autocommit := "YES"
	if raw := session.GetParamByName("autocommit"); raw != nil && !sessionBoolValue(raw) {
		autocommit = "NO"
	}
	accessMode := transactionAccessModeForMetadata(session)
	startedAt := int64Param(session.GetParamByName("performance_schema_transaction_started_at"))
	timerStart := int64(0)
	if startedAt > 0 {
		timerStart = performanceSchemaTimerTimestamp(time.Unix(0, startedAt))
	}
	row := []interface{}{
		threadID, int64(1), nil, "transaction", "ACTIVE", nil, nil,
		nil, nil, nil, timerStart, nil, nil, accessMode, isolation,
		autocommit, int64Param(session.GetParamByName("performance_schema_savepoint_count")),
		int64Param(session.GetParamByName("performance_schema_rollback_to_savepoint_count")),
		int64Param(session.GetParamByName("performance_schema_release_savepoint_count")), nil, nil, nil,
	}
	values := informationSchemaRowValues(defaultColumns, row)
	if !performanceSchemaTransactionRowMatches(query, values) {
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	return newInformationSchemaSelectResult(name, columns, [][]interface{}{projectInformationSchemaRow(columns, values)})
}

func performanceSchemaTransactionRowMatches(query string, values map[string]interface{}) bool {
	for _, column := range []string{"thread_id", "event_id", "end_event_id", "event_name", "state", "trx_id", "gtid", "access_mode", "isolation_level", "autocommit"} {
		key := strings.ToUpper(column)
		value := ""
		if raw, ok := values[key]; ok && raw != nil {
			value = fmt.Sprint(raw)
		}
		if !performanceSchemaSummaryFilterMatches(query, column, value) {
			return false
		}
	}
	return true
}

func informationSchemaRowValues(columns []string, row []interface{}) map[string]interface{} {
	values := make(map[string]interface{}, len(columns))
	for index, column := range columns {
		if index < len(row) {
			values[column] = row[index]
		}
	}
	return values
}

const performanceSchemaTransactionHistoryLimit = 64
const performanceSchemaTransactionHistoryLongLimit = 1024

type performanceSchemaTransactionEvent struct {
	Row       []interface{}
	User      string
	Host      string
	TimerWait int64
}

const performanceSchemaProgramHistoryLongLimit = 1024

type performanceSchemaProgramEvent struct {
	ObjectType        string
	ObjectSchema      string
	ObjectName        string
	TimerWait         int64
	StatementCount    int64
	StatementsWaitSum int64
	StatementsWaitMin int64
	StatementsWaitMax int64
	Errors            int64
	Warnings          int64
	RowsAffected      int64
	RowsSent          int64
}

func (e *XMySQLExecutor) recordPerformanceSchemaProgramExecution(objectType, schema, name string, timerWait, statementCount, statementsWaitSum, statementsWaitMin, statementsWaitMax, errors, warnings, rowsAffected, rowsSent int64) {
	if e == nil || strings.TrimSpace(name) == "" {
		return
	}
	if !e.performanceSchemaInstrumentationConsumersEnabled() {
		return
	}
	objectType = strings.ToUpper(strings.TrimSpace(objectType))
	timed := true
	e.performanceSchemaMu.RLock()
	objectSetting, objectConfigured := e.performanceSchemaObjects[objectType]
	e.performanceSchemaMu.RUnlock()
	if objectConfigured {
		if !objectSetting.Enabled {
			return
		}
		if !objectSetting.Timed {
			timed = false
			timerWait = 0
			statementsWaitSum = 0
			statementsWaitMin = 0
			statementsWaitMax = 0
		}
	}
	if timed {
		if timerWait <= 0 {
			timerWait = 1
		}
		if statementCount > 0 {
			if statementsWaitSum <= 0 {
				statementsWaitSum = timerWait
			}
			if statementsWaitMin <= 0 {
				statementsWaitMin = 1
			}
			if statementsWaitMax <= 0 {
				statementsWaitMax = statementsWaitMin
			}
		}
	}
	if timerWait < 0 {
		timerWait = 0
	}
	e.performanceSchemaMu.Lock()
	defer e.performanceSchemaMu.Unlock()
	if len(e.performanceSchemaProgramHistoryLong) >= performanceSchemaProgramHistoryLongLimit {
		e.performanceSchemaProgramHistoryLong = e.performanceSchemaProgramHistoryLong[1:]
	}
	e.performanceSchemaProgramHistoryLong = append(e.performanceSchemaProgramHistoryLong, performanceSchemaProgramEvent{
		ObjectType: objectType, ObjectSchema: schema, ObjectName: name,
		TimerWait: timerWait, StatementCount: statementCount, StatementsWaitSum: statementsWaitSum,
		StatementsWaitMin: statementsWaitMin, StatementsWaitMax: statementsWaitMax, Errors: errors,
		Warnings: warnings, RowsAffected: rowsAffected, RowsSent: rowsSent,
	})
}

func (e *XMySQLExecutor) markPerformanceSchemaTransactionStart(session server.MySQLServerSession) {
	if e == nil || session == nil {
		return
	}
	session.SetParamByName("performance_schema_transaction_started_at", time.Now().UnixNano())
}

func (e *XMySQLExecutor) recordPerformanceSchemaTransactionHistory(session server.MySQLServerSession, state string) {
	if e == nil || session == nil {
		return
	}
	if !e.performanceSchemaInstrumentationConsumersEnabled() {
		session.SetParamByName("performance_schema_transaction_started_at", nil)
		return
	}
	keepSessionHistory := e.performanceSchemaConsumerEnabled("events_transactions_history")
	keepLongHistory := e.performanceSchemaConsumerEnabled("events_transactions_history_long")
	threadID := int64(0)
	if raw := session.GetParamByName("connection_id"); raw != nil {
		threadID = int64Param(raw)
	}
	eventID := int64Param(session.GetParamByName("performance_schema_transaction_event_id")) + 1
	session.SetParamByName("performance_schema_transaction_event_id", eventID)
	isolation := transactionIsolationForMetadata(session)
	autocommit := "YES"
	if raw := session.GetParamByName("autocommit"); raw != nil && !sessionBoolValue(raw) {
		autocommit = "NO"
	}
	accessMode := transactionAccessModeForMetadata(session)
	startedAt := int64Param(session.GetParamByName("performance_schema_transaction_started_at"))
	timerStart := int64(0)
	if startedAt > 0 {
		timerStart = performanceSchemaTimerTimestamp(time.Unix(0, startedAt))
	}
	timerWait := int64(0)
	if startedAt > 0 {
		timerWait = time.Since(time.Unix(0, startedAt)).Nanoseconds() * 1000
		if timerWait <= 0 {
			timerWait = 1000
		}
	}
	timerEnd := timerStart + timerWait
	event := performanceSchemaTransactionEvent{Row: []interface{}{
		threadID, eventID, eventID, "transaction", state, nil, nil,
		nil, nil, nil, timerStart, timerEnd, timerWait, accessMode, isolation,
		autocommit, int64Param(session.GetParamByName("performance_schema_savepoint_count")),
		int64Param(session.GetParamByName("performance_schema_rollback_to_savepoint_count")),
		int64Param(session.GetParamByName("performance_schema_release_savepoint_count")), nil, nil, nil,
	}, User: sessionStringParam(session, "user"), Host: sessionStringParam(session, "host"), TimerWait: timerWait}
	session.SetParamByName("performance_schema_transaction_started_at", nil)
	e.performanceSchemaMu.Lock()
	if len(e.performanceSchemaTransactionSummaries) >= performanceSchemaTransactionHistoryLongLimit {
		e.performanceSchemaTransactionSummaries = e.performanceSchemaTransactionSummaries[1:]
	}
	e.performanceSchemaTransactionSummaries = append(e.performanceSchemaTransactionSummaries, event)
	if keepLongHistory {
		if len(e.performanceSchemaTransactionHistoryLong) >= performanceSchemaTransactionHistoryLongLimit {
			e.performanceSchemaTransactionHistoryLong = e.performanceSchemaTransactionHistoryLong[1:]
		}
		e.performanceSchemaTransactionHistoryLong = append(e.performanceSchemaTransactionHistoryLong, event)
	}
	e.performanceSchemaMu.Unlock()
	if keepSessionHistory {
		events, _ := session.GetParamByName("performance_schema_transaction_history").([]performanceSchemaTransactionEvent)
		if len(events) >= performanceSchemaTransactionHistoryLimit {
			events = events[1:]
		}
		events = append(events, event)
		session.SetParamByName("performance_schema_transaction_history", events)
	}
}

func transactionAccessMode(session server.MySQLServerSession) string {
	if sessionBoolParam(session, "tx_read_only") || sessionBoolParam(session, "transaction_read_only") {
		return "READ ONLY"
	}
	return "READ WRITE"
}

func transactionAccessModeForMetadata(session server.MySQLServerSession) string {
	if session != nil {
		if raw := session.GetParamByName("transaction_dml_state"); raw != nil {
			if state, ok := raw.(*sessionTransactionState); ok && state != nil && state.AccessMode != "" {
				return state.AccessMode
			}
		}
	}
	return transactionAccessMode(session)
}

func transactionIsolationForMetadata(session server.MySQLServerSession) string {
	if session != nil {
		if raw := session.GetParamByName("transaction_dml_state"); raw != nil {
			if state, ok := raw.(*sessionTransactionState); ok && state != nil && state.IsolationLevel != "" {
				return strings.ReplaceAll(strings.ToUpper(state.IsolationLevel), "-", " ")
			}
		}
	}
	isolation := transactionIsolation(session)
	return strings.ReplaceAll(strings.ToUpper(isolation), "-", " ")
}

func sessionBoolParam(session server.MySQLServerSession, name string) bool {
	if session == nil {
		return false
	}
	raw := session.GetParamByName(name)
	return raw != nil && sessionBoolValue(raw)
}

func sessionBoolValue(raw interface{}) bool {
	switch value := raw.(type) {
	case bool:
		return value
	case int:
		return value != 0
	case int64:
		return value != 0
	case uint64:
		return value != 0
	case string:
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "1", "true", "on", "yes":
			return true
		}
	}
	return false
}

// rejectReadOnlyDML enforces the session transaction read-only flags at the
// SQL execution boundary. Replication applies statements through the same
// executor, so those statements explicitly bypass the client-facing guard and
// retain the replica's own write policy.
func rejectReadOnlyDML(session server.MySQLServerSession, statementType string) error {
	return rejectReadOnlyDMLForQuery(session, statementType, "", "")
}

func rejectReadOnlyDMLForQuery(session server.MySQLServerSession, statementType, query, databaseName string) error {
	if session == nil || sessionBoolParam(session, "replication_replay") {
		return nil
	}
	if isTemporaryTableDML(session, query, databaseName) {
		return nil
	}
	xaState := strings.ToUpper(strings.TrimSpace(fmt.Sprint(session.GetParamByName("xa_state"))))
	if xaState != "" && xaState != "<NIL>" && xaState != "ACTIVE" {
		return fmt.Errorf("XAER_PROTO: cannot execute %s statement while an XA transaction is %s", strings.ToUpper(strings.TrimSpace(statementType)), xaState)
	}
	readOnly := false
	if sessionBoolParam(session, "in_transaction") {
		readOnly = sessionBoolParam(session, "tx_read_only") || sessionBoolParam(session, "transaction_read_only")
	} else {
		if pending := session.GetParamByName("next_transaction_read_only"); pending != nil {
			readOnly = sessionBoolValue(pending)
		}
	}
	if !readOnly {
		return nil
	}
	return fmt.Errorf("Cannot execute %s statement in a READ ONLY transaction", strings.ToUpper(strings.TrimSpace(statementType)))
}

// globalReadOnlyWriteBlockReason enforces the server-wide read_only variables
// for client DML and persistent DDL. Replication replay is deliberately
// exempted because source changes must still be applied on a read-only replica.
// MySQL permits operations on TEMPORARY tables and ANALYZE/OPTIMIZE even in
// read-only mode; those exceptions are kept here rather than in each DDL path.
func (e *XMySQLExecutor) globalReadOnlyWriteBlockReason(session server.MySQLServerSession, query, databaseName string) string {
	if e == nil || e.storageManager == nil {
		return ""
	}
	isDML := isReplicationDMLQuery(query)
	isDDL := isReadOnlyProtectedDDLQuery(query)
	if !isDML && !isDDL {
		return ""
	}
	if (isDML && isTemporaryTableDML(session, query, databaseName)) || (isDDL && isTemporaryTableDDL(session, query, databaseName)) {
		return ""
	}
	if session != nil && sessionBoolParam(session, "replication_replay") {
		return ""
	}
	sysVars := e.storageManager.GetSystemVariablesManager()
	if sysVars == nil {
		return ""
	}
	if value, err := sysVars.GetVariable("", "super_read_only", manager.GlobalScope); err == nil && sessionBoolValue(value) {
		return "The MySQL server is running with the --super-read-only option so it cannot execute this statement"
	}
	if sessionHasGlobalReadOnlyBypass(session) {
		return ""
	}
	value, err := sysVars.GetVariable("", "read_only", manager.GlobalScope)
	if err != nil || !sessionBoolValue(value) {
		return ""
	}
	return "The MySQL server is running with the --read-only option so it cannot execute this statement"
}

func isReadOnlyProtectedDDLQuery(query string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(query))
	trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
	switch {
	case strings.HasPrefix(trimmed, "create temporary "),
		strings.HasPrefix(trimmed, "drop temporary "),
		strings.HasPrefix(trimmed, "analyze table "),
		strings.HasPrefix(trimmed, "optimize table "),
		strings.HasPrefix(trimmed, "flush status"):
		return false
	case strings.HasPrefix(trimmed, "create "),
		strings.HasPrefix(trimmed, "alter "),
		strings.HasPrefix(trimmed, "drop "),
		strings.HasPrefix(trimmed, "truncate "),
		strings.HasPrefix(trimmed, "rename "),
		strings.HasPrefix(trimmed, "repair "),
		strings.HasPrefix(trimmed, "install "),
		strings.HasPrefix(trimmed, "uninstall "):
		return true
	default:
		return false
	}
}

func isTemporaryTableDDL(session server.MySQLServerSession, query, databaseName string) bool {
	if session == nil {
		return false
	}
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "create temporary ") || strings.HasPrefix(lower, "drop temporary ") {
		return true
	}
	// DROP TABLE accepts multiple targets. Allow the statement only when every
	// target belongs to this session's temporary-table namespace; a mixed
	// temporary/persistent statement must remain protected by read_only.
	if strings.HasPrefix(lower, "drop table ") && strings.Contains(trimmed[len("drop table"):], ",") {
		tail := strings.TrimSpace(trimmed[len("drop table"):])
		if strings.HasPrefix(strings.ToLower(tail), "if exists") {
			tail = strings.TrimSpace(tail[len("if exists"):])
		}
		identifier := regexp.MustCompile("(?is)^\\s*((?:" + compatibilityIdentifierPattern + ")(?:\\s*\\.\\s*(?:" + compatibilityIdentifierPattern + "))?)\\s*$")
		parts := strings.Split(tail, ",")
		if len(parts) < 2 {
			return false
		}
		for _, part := range parts {
			match := identifier.FindStringSubmatch(part)
			if len(match) != 2 {
				return false
			}
			targetDB, targetTable := compatibilityQualifiedTable(match[1], databaseName)
			if targetDB == "" || targetTable == "" || !isSessionTemporaryTable(session, targetDB, targetTable) {
				return false
			}
		}
		return true
	}
	pattern := "(?is)^\\s*(?:alter\\s+table|drop\\s+table|truncate\\s+table)\\s+(?:if\\s+exists\\s+)?((?:`?[a-zA-Z0-9_$]+`?\\s*\\.\\s*)?`?[a-zA-Z0-9_$]+`?)"
	match := regexp.MustCompile(pattern).FindStringSubmatch(trimmed)
	if len(match) != 2 || strings.Contains(match[1], ",") {
		return false
	}
	targetDB, targetTable := compatibilityQualifiedTable(match[1], databaseName)
	if targetDB == "" || targetTable == "" {
		return false
	}
	return isSessionTemporaryTable(session, targetDB, targetTable)
}

func isSessionTemporaryTable(session server.MySQLServerSession, databaseName, tableName string) bool {
	if session == nil || databaseName == "" || tableName == "" {
		return false
	}
	if _, temporary := temporaryPhysicalTableName(session, databaseName, tableName); temporary {
		return true
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(tableName)), "__xmysql_tmp_") {
		_, temporary := temporaryMetadataTableName(session, databaseName, tableName)
		return temporary
	}
	return false
}

func isTemporaryTableDML(session server.MySQLServerSession, query, databaseName string) bool {
	if session == nil {
		return false
	}
	match := regexp.MustCompile(`(?is)^\s*(?:insert\s+(?:ignore\s+)?into|replace\s+into|update|delete\s+from)\s+((?:` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `\s*\.\s*)?` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `)`).FindStringSubmatch(strings.TrimSpace(query))
	if len(match) != 2 {
		return false
	}
	targetDB, targetTable := compatibilityQualifiedTable(match[1], databaseName)
	if targetDB == "" || targetTable == "" {
		return false
	}
	if _, temporary := temporaryPhysicalTableName(session, targetDB, targetTable); temporary {
		return true
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(targetTable)), "__xmysql_tmp_") {
		_, temporary := temporaryMetadataTableName(session, targetDB, targetTable)
		return temporary
	}
	return false
}

func sessionHasGlobalReadOnlyBypass(session server.MySQLServerSession) bool {
	if session == nil {
		return false
	}
	if privileges, ok := session.GetParamByName("global_privileges").([]common.PrivilegeType); ok {
		for _, privilege := range privileges {
			if privilege == common.SuperPriv || privilege == common.AllPriv {
				return true
			}
		}
	}
	if dynamicPrivileges, ok := session.GetParamByName("dynamic_privileges").([]string); ok {
		for _, privilege := range dynamicPrivileges {
			if strings.EqualFold(strings.TrimSpace(privilege), "CONNECTION_ADMIN") {
				return true
			}
		}
	}
	return false
}

func int64Param(raw interface{}) int64 {
	switch value := raw.(type) {
	case int:
		return int64(value)
	case int64:
		return value
	case uint64:
		return int64(value)
	case string:
		parsed, _ := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		return parsed
	default:
		return 0
	}
}

func defaultPerformanceSchemaConsumers() map[string]performanceSchemaSetupSetting {
	return map[string]performanceSchemaSetupSetting{
		"global_instrumentation":           {Enabled: true, Timed: true},
		"thread_instrumentation":           {Enabled: true, Timed: true},
		"events_statements_current":        {Enabled: true, Timed: true},
		"events_statements_history":        {Enabled: true, Timed: true},
		"events_statements_history_long":   {Enabled: true, Timed: true},
		"events_stages_current":            {Enabled: true, Timed: true},
		"events_stages_history":            {Enabled: true, Timed: true},
		"events_stages_history_long":       {Enabled: true, Timed: true},
		"events_waits_current":             {Enabled: true, Timed: true},
		"events_waits_history":             {Enabled: true, Timed: true},
		"events_waits_history_long":        {Enabled: true, Timed: true},
		"events_transactions_current":      {Enabled: true, Timed: true},
		"events_transactions_history":      {Enabled: true, Timed: true},
		"events_transactions_history_long": {Enabled: true, Timed: true},
	}
}

func defaultPerformanceSchemaInstruments() map[string]performanceSchemaSetupSetting {
	return map[string]performanceSchemaSetupSetting{
		"statement/sql/select":        {Enabled: true, Timed: true},
		"statement/sql/insert":        {Enabled: true, Timed: true},
		"statement/sql/update":        {Enabled: true, Timed: true},
		"statement/sql/delete":        {Enabled: true, Timed: true},
		"stage/sql/execute":           {Enabled: true, Timed: true},
		"wait/lock/table/sql/handler": {Enabled: true, Timed: true},
		"wait/lock/metadata/sql/mdl":  {Enabled: true, Timed: true},
	}
}

func defaultPerformanceSchemaObjects() map[string]performanceSchemaSetupSetting {
	return map[string]performanceSchemaSetupSetting{
		"TABLE":     {Enabled: true, Timed: true},
		"EVENT":     {Enabled: true, Timed: true},
		"FUNCTION":  {Enabled: true, Timed: true},
		"PROCEDURE": {Enabled: true, Timed: true},
		"TRIGGER":   {Enabled: true, Timed: true},
	}
}

func (e *XMySQLExecutor) executePerformanceSchemaSetupConsumersSelect(query string) *SelectResult {
	columns := []string{"NAME", "ENABLED"}
	names := []string{"global_instrumentation", "thread_instrumentation", "events_statements_current", "events_statements_history", "events_statements_history_long", "events_stages_current", "events_stages_history", "events_stages_history_long", "events_waits_current", "events_waits_history", "events_waits_history_long", "events_transactions_current", "events_transactions_history", "events_transactions_history_long"}
	e.performanceSchemaMu.RLock()
	rows := make([][]interface{}, 0, len(names))
	for _, name := range names {
		setting := e.performanceSchemaConsumers[name]
		rows = append(rows, []interface{}{name, performanceSchemaYesNo(setting.Enabled)})
	}
	e.performanceSchemaMu.RUnlock()
	rows = filterPerformanceSchemaSetupRows(query, rows)
	return newInformationSchemaSelectResult("performance_schema.setup_consumers", columns, rows)
}

func (e *XMySQLExecutor) executePerformanceSchemaSetupInstrumentsSelect(query string) *SelectResult {
	columns := []string{"NAME", "ENABLED", "TIMED", "PROPERTIES", "VOLATILITY", "DOCUMENT"}
	names := []string{"statement/sql/select", "statement/sql/insert", "statement/sql/update", "statement/sql/delete", "stage/sql/execute", "wait/lock/table/sql/handler", "wait/lock/metadata/sql/mdl"}
	e.performanceSchemaMu.RLock()
	rows := make([][]interface{}, 0, len(names))
	for _, name := range names {
		setting := e.performanceSchemaInstruments[name]
		rows = append(rows, []interface{}{name, performanceSchemaYesNo(setting.Enabled), performanceSchemaYesNo(setting.Timed), "", int64(0), nil})
	}
	e.performanceSchemaMu.RUnlock()
	rows = filterPerformanceSchemaSetupRows(query, rows)
	return newInformationSchemaSelectResult("performance_schema.setup_instruments", columns, rows)
}

func (e *XMySQLExecutor) executePerformanceSchemaSetupActorsSelect(query string) *SelectResult {
	columns := []string{"HOST", "USER", "ROLE", "ENABLED", "HISTORY"}
	e.performanceSchemaMu.RLock()
	setting := e.performanceSchemaActors
	e.performanceSchemaMu.RUnlock()
	rows := [][]interface{}{{"%", "%", "%", performanceSchemaYesNo(setting.Enabled), performanceSchemaYesNo(setting.History)}}
	if matcher, ok := performanceSchemaSetupActorMatcher(query); ok && !matcher() {
		rows = nil
	}
	return newInformationSchemaSelectResult("performance_schema.setup_actors", columns, rows)
}

func (e *XMySQLExecutor) executePerformanceSchemaSetupObjectsSelect(query string) *SelectResult {
	columns := []string{"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "ENABLED", "TIMED"}
	names := []string{"TABLE", "EVENT", "FUNCTION", "PROCEDURE", "TRIGGER"}
	e.performanceSchemaMu.RLock()
	rows := make([][]interface{}, 0, len(names))
	for _, name := range names {
		setting, ok := e.performanceSchemaObjects[name]
		if !ok {
			setting = performanceSchemaSetupSetting{Enabled: true, Timed: true}
		}
		rows = append(rows, []interface{}{name, "%", "%", performanceSchemaYesNo(setting.Enabled), performanceSchemaYesNo(setting.Timed)})
	}
	e.performanceSchemaMu.RUnlock()
	return filterPerformanceSchemaSetupObjectRows(query, columns, rows)
}

func (e *XMySQLExecutor) executePerformanceSchemaSetupTimersSelect(query string) *SelectResult {
	columns := []string{"NAME", "TIMER_NAME", "TIMER_FREQUENCY", "TIMER_RESOLUTION", "TIMER_OVERHEAD"}
	rows := [][]interface{}{
		{"CYCLE", "CYCLE", int64(0), int64(0), int64(0)},
		{"NANOSECOND", "NANOSECOND", int64(1000000000), int64(1), int64(0)},
		{"MICROSECOND", "MICROSECOND", int64(1000000), int64(1), int64(0)},
		{"MILLISECOND", "MILLISECOND", int64(1000), int64(1), int64(0)},
		{"TICK", "TICK", int64(0), int64(0), int64(0)},
	}
	return newInformationSchemaSelectResult("performance_schema.setup_timers", columns, filterPerformanceSchemaSetupRows(query, rows))
}

func filterPerformanceSchemaSetupObjectRows(query string, columns []string, rows [][]interface{}) *SelectResult {
	patterns := regexp.MustCompile(`(?i)\b(object_type|object_schema|object_name)\s*(=|like)\s*(?:'([^']*)'|"([^"]*)")`)
	matches := patterns.FindAllStringSubmatch(query, -1)
	if len(matches) == 0 {
		return newInformationSchemaSelectResult("performance_schema.setup_objects", columns, rows)
	}
	filtered := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		matched := true
		for _, match := range matches {
			columnIndex := map[string]int{"object_type": 0, "object_schema": 1, "object_name": 2}[strings.ToLower(match[1])]
			value := fmt.Sprint(row[columnIndex])
			pattern := match[3]
			if pattern == "" {
				pattern = match[4]
			}
			if strings.EqualFold(match[2], "=") {
				matched = matched && strings.EqualFold(value, pattern)
			} else {
				matched = matched && performanceSchemaSQLLike(pattern, value)
			}
		}
		if matched {
			filtered = append(filtered, row)
		}
	}
	return newInformationSchemaSelectResult("performance_schema.setup_objects", columns, filtered)
}

func (e *XMySQLExecutor) performanceSchemaConsumerEnabled(name string) bool {
	e.performanceSchemaMu.RLock()
	setting, ok := e.performanceSchemaConsumers[strings.ToLower(name)]
	e.performanceSchemaMu.RUnlock()
	return !ok || setting.Enabled
}

// performanceSchemaActorSettingForSession snapshots setup_actors when a
// foreground session is first observed. MySQL applies setup_actors changes to
// foreground threads created after the change; keeping the snapshot per
// session preserves that boundary instead of retroactively changing an
// existing thread. Internal statements without a session use the background
// thread default and remain instrumented and historical.
func (e *XMySQLExecutor) performanceSchemaActorSettingForSession(session server.MySQLServerSession) performanceSchemaActorSetting {
	if e == nil || session == nil {
		return performanceSchemaActorSetting{Enabled: true, History: true}
	}
	e.performanceSchemaMu.Lock()
	defer e.performanceSchemaMu.Unlock()
	if e.performanceSchemaActorSessions == nil {
		e.performanceSchemaActorSessions = make(map[server.MySQLServerSession]performanceSchemaActorSetting)
	}
	if setting, ok := e.performanceSchemaActorSessions[session]; ok {
		return setting
	}
	setting := e.performanceSchemaActors
	e.performanceSchemaActorSessions[session] = setting
	return setting
}

// performanceSchemaStatementSettingForSession applies the two global
// Performance Schema consumer gates after the actor snapshot. The global gate
// suppresses all instrumented threads; the thread gate suppresses foreground
// and background thread instrumentation. Actor HISTORY remains independent so
// callers can still distinguish collection from historical retention.
func (e *XMySQLExecutor) performanceSchemaStatementSettingForSession(session server.MySQLServerSession) performanceSchemaActorSetting {
	setting := e.performanceSchemaActorSettingForSession(session)
	if !e.performanceSchemaInstrumentationConsumersEnabled() {
		setting.Enabled = false
	}
	return setting
}

func (e *XMySQLExecutor) performanceSchemaInstrumentationConsumersEnabled() bool {
	if e == nil {
		return true
	}
	return e.performanceSchemaConsumerEnabled("global_instrumentation") &&
		e.performanceSchemaConsumerEnabled("thread_instrumentation")
}

func (e *XMySQLExecutor) performanceSchemaInstrumentSetting(name string) (bool, bool) {
	e.performanceSchemaMu.RLock()
	setting, ok := e.performanceSchemaInstruments[strings.ToLower(name)]
	e.performanceSchemaMu.RUnlock()
	if !ok {
		return true, true
	}
	return setting.Enabled, setting.Timed
}

func (e *XMySQLExecutor) performanceSchemaObservedWait(event string, wait time.Duration) (bool, time.Duration) {
	if e == nil {
		return true, wait
	}
	if !e.performanceSchemaInstrumentationConsumersEnabled() {
		return false, 0
	}
	enabled, timed := e.performanceSchemaInstrumentSetting(event)
	if !enabled {
		return false, 0
	}
	if !timed {
		return true, 0
	}
	return true, wait
}

func performanceSchemaYesNo(value bool) string {
	if value {
		return "YES"
	}
	return "NO"
}

var performanceSchemaSetupUpdatePattern = regexp.MustCompile(`(?is)^\s*update\s+(?:` + "`?" + `performance_schema` + "`?" + `\.)?` + "`?" + `(setup_consumers|setup_instruments|setup_actors|setup_objects|setup_loggers|setup_meters)` + "`?" + `\s+set\s+(.+?)\s+where\s+(.+?)\s*;?\s*$`)

func (e *XMySQLExecutor) executePerformanceSchemaSetupUpdate(query string) (*Result, bool, error) {
	match := performanceSchemaSetupUpdatePattern.FindStringSubmatch(query)
	if len(match) != 4 {
		return nil, false, nil
	}
	if strings.EqualFold(match[1], "setup_loggers") || strings.EqualFold(match[1], "setup_meters") {
		return e.executePerformanceSchemaTelemetrySetupUpdate(match[1], match[2], match[3])
	}

	updates := make(map[string]bool)
	for _, assignment := range splitTopLevelComma(match[2]) {
		parts := strings.SplitN(assignment, "=", 2)
		if len(parts) != 2 {
			return nil, true, fmt.Errorf("unsupported Performance Schema setup assignment %q", strings.TrimSpace(assignment))
		}
		column := strings.ToLower(strings.Trim(strings.TrimSpace(parts[0]), "`"))
		value, ok := performanceSchemaSetupBoolean(parts[1])
		if !ok || (column != "enabled" && column != "timed" && column != "history") {
			return nil, true, fmt.Errorf("unsupported Performance Schema setup assignment %q", strings.TrimSpace(assignment))
		}
		updates[column] = value
	}

	var nameMatches func(string) bool
	var actorMatches func() bool
	var objectMatches func(string) bool
	var ok bool
	if strings.EqualFold(match[1], "setup_actors") {
		actorMatches, ok = performanceSchemaSetupActorMatcher(match[3])
	} else if strings.EqualFold(match[1], "setup_objects") {
		objectMatches, ok = performanceSchemaSetupObjectMatcher(match[3])
	} else {
		nameMatches, ok = performanceSchemaSetupNameMatcher(match[3])
	}
	if !ok {
		return nil, true, fmt.Errorf("unsupported Performance Schema setup WHERE clause %q", strings.TrimSpace(match[3]))
	}

	e.performanceSchemaMu.Lock()
	defer e.performanceSchemaMu.Unlock()
	affected := 0
	switch strings.ToLower(match[1]) {
	case "setup_consumers":
		for name, setting := range e.performanceSchemaConsumers {
			if !nameMatches(name) {
				continue
			}
			if value, exists := updates["enabled"]; exists {
				setting.Enabled = value
			}
			if value, exists := updates["timed"]; exists {
				setting.Timed = value
			}
			e.performanceSchemaConsumers[name] = setting
			affected++
		}
	case "setup_instruments":
		for name, setting := range e.performanceSchemaInstruments {
			if !nameMatches(name) {
				continue
			}
			if value, exists := updates["enabled"]; exists {
				setting.Enabled = value
			}
			if value, exists := updates["timed"]; exists {
				setting.Timed = value
			}
			e.performanceSchemaInstruments[name] = setting
			affected++
		}
	case "setup_objects":
		for name, setting := range e.performanceSchemaObjects {
			if !objectMatches(name) {
				continue
			}
			if value, exists := updates["enabled"]; exists {
				setting.Enabled = value
			}
			if value, exists := updates["timed"]; exists {
				setting.Timed = value
			}
			e.performanceSchemaObjects[name] = setting
			affected++
		}
	case "setup_actors":
		if actorMatches() {
			if value, exists := updates["enabled"]; exists {
				e.performanceSchemaActors.Enabled = value
			}
			if value, exists := updates["history"]; exists {
				e.performanceSchemaActors.History = value
			}
			affected = 1
		}
	}
	return &Result{AffectedRows: affected, ResultType: common.RESULT_TYPE_QUERY, Message: fmt.Sprintf("Performance Schema setup updated, %d rows affected", affected)}, true, nil
}

func performanceSchemaSetupActorMatcher(where string) (func() bool, bool) {
	whereLower := strings.ToLower(where)
	if !strings.Contains(whereLower, "host") && !strings.Contains(whereLower, "user") && !strings.Contains(whereLower, "role") {
		return nil, false
	}
	for _, column := range []string{"host", "user", "role"} {
		if !strings.Contains(whereLower, column) {
			continue
		}
		pattern := regexp.MustCompile(`(?i)\b` + column + `\s*=\s*'([^']*)'`).FindStringSubmatch(where)
		if len(pattern) == 2 && pattern[1] != "%" {
			return func() bool { return false }, true
		}
	}
	return func() bool { return true }, true
}

func performanceSchemaSetupBoolean(raw string) (bool, bool) {
	value := strings.ToUpper(strings.Trim(strings.TrimSpace(raw), "'\"`"))
	switch value {
	case "YES", "ON", "1":
		return true, true
	case "NO", "OFF", "0":
		return false, true
	default:
		return false, false
	}
}

func performanceSchemaSetupNameMatcher(where string) (func(string) bool, bool) {
	for _, expression := range []string{`(?i)\bname\s*=\s*'([^']*)'`, `(?i)\bname\s*=\s*"([^"]*)"`, `(?i)\bname\s+like\s+'([^']*)'`, `(?i)\bname\s+like\s+"([^"]*)"`} {
		match := regexp.MustCompile(expression).FindStringSubmatch(where)
		if len(match) != 2 {
			continue
		}
		pattern := match[1]
		if strings.Contains(strings.ToLower(expression), "like") {
			return func(name string) bool { return performanceSchemaSQLLike(pattern, name) }, true
		}
		return func(name string) bool { return strings.EqualFold(name, pattern) }, true
	}
	return nil, false
}

func performanceSchemaSetupObjectMatcher(where string) (func(string) bool, bool) {
	match := regexp.MustCompile(`(?i)\bobject_type\s*(=|like)\s*(?:'([^']*)'|"([^"]*)")`).FindStringSubmatch(where)
	if len(match) != 4 {
		return nil, false
	}
	pattern := match[2]
	if pattern == "" {
		pattern = match[3]
	}
	if strings.EqualFold(match[1], "=") {
		return func(name string) bool { return strings.EqualFold(name, pattern) }, true
	}
	return func(name string) bool { return performanceSchemaSQLLike(pattern, name) }, true
}

func performanceSchemaSQLLike(pattern, value string) bool {
	pattern = strings.ToLower(pattern)
	value = strings.ToLower(value)
	var match func(int, int) bool
	match = func(patternIndex, valueIndex int) bool {
		if patternIndex == len(pattern) {
			return valueIndex == len(value)
		}
		if pattern[patternIndex] == '%' {
			return match(patternIndex+1, valueIndex) || (valueIndex < len(value) && match(patternIndex, valueIndex+1))
		}
		if valueIndex == len(value) {
			return false
		}
		return (pattern[patternIndex] == '_' || pattern[patternIndex] == value[valueIndex]) && match(patternIndex+1, valueIndex+1)
	}
	return match(0, 0)
}

func filterPerformanceSchemaSetupRows(query string, rows [][]interface{}) [][]interface{} {
	matcher, ok := performanceSchemaSetupNameMatcher(query)
	if !ok {
		return rows
	}
	filtered := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		if len(row) > 0 && matcher(fmt.Sprint(row[0])) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func (e *XMySQLExecutor) executePerformanceSchemaVariablesSelect(name string, queries ...string) *SelectResult {
	defaults := []string{"VARIABLE_NAME", "VARIABLE_VALUE"}
	columns := defaults
	if len(queries) > 0 {
		columns = requestedInformationSchemaColumns(queries[0], defaults)
	}
	rows := make([][]interface{}, 0)
	if e != nil && e.storageManager != nil && e.storageManager.GetSystemVariablesManager() != nil && strings.Contains(strings.ToLower(name), "global_variables") {
		values := e.storageManager.GetSystemVariablesManager().ListVariables("", manager.GlobalScope)
		names := make([]string, 0, len(values))
		for variableName := range values {
			names = append(names, variableName)
		}
		sort.Strings(names)
		for _, variableName := range names {
			rows = append(rows, []interface{}{variableName, fmt.Sprint(values[variableName])})
		}
	} else {
		rows = [][]interface{}{
			{"max_connections", "151"},
			{"autocommit", "ON"},
			{"character_set_client", "utf8mb4"},
			{"character_set_connection", "utf8mb4"},
			{"character_set_results", "utf8mb4"},
			{"collation_connection", "utf8mb4_0900_ai_ci"},
			{"transaction_isolation", "REPEATABLE-READ"},
		}
	}
	if len(queries) > 0 {
		rows = filterPerformanceSchemaVariableRows(queries[0], rows)
	}
	projected := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		if len(row) < 2 {
			continue
		}
		projected = append(projected, projectInformationSchemaRow(columns, map[string]interface{}{
			"VARIABLE_NAME": row[0], "VARIABLE_VALUE": row[1],
		}))
	}
	return newInformationSchemaSelectResult(name, columns, projected)
}

func (e *XMySQLExecutor) executePerformanceSchemaSessionVariablesSelect(name, query string, session server.MySQLServerSession) *SelectResult {
	defaults := []string{"VARIABLE_NAME", "VARIABLE_VALUE"}
	columns := requestedInformationSchemaColumns(query, defaults)
	rows := filterPerformanceSchemaVariableRows(query, performanceSchemaSessionVariableRows(session))
	projected := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		if len(row) < 2 {
			continue
		}
		projected = append(projected, projectInformationSchemaRow(columns, map[string]interface{}{
			"VARIABLE_NAME": row[0], "VARIABLE_VALUE": row[1],
		}))
	}
	return newInformationSchemaSelectResult(name, columns, projected)
}

func filterPerformanceSchemaVariableRows(query string, rows [][]interface{}) [][]interface{} {
	filtered := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		if len(row) > 0 && performanceSchemaSummaryFilterMatches(query, "variable_name", fmt.Sprint(row[0])) {
			filtered = append(filtered, row)
		}
	}
	if len(filtered) == 0 && !regexp.MustCompile(`(?i)\bvariable_name\b\s*(?:=|like)`).MatchString(query) {
		return rows
	}
	return filtered
}

func (e *XMySQLExecutor) executePerformanceSchemaStatusSelect(name, query string, current ...server.MySQLServerSession) *SelectResult {
	queryCount := "0"
	if e != nil && e.metricsRecorder != nil {
		for _, line := range strings.Split(e.metricsRecorder.PrometheusText(), "\n") {
			if strings.HasPrefix(line, "xmysql_queries_total{") {
				queryCount = strings.TrimSpace(line[strings.LastIndex(line, "} ")+2:])
				break
			}
		}
	}
	columns := []string{"VARIABLE_NAME", "VARIABLE_VALUE"}
	if query != "" {
		columns = requestedInformationSchemaColumns(query, columns)
	}
	threadsConnected := "0"
	threadsRunning := "0"
	if strings.HasSuffix(name, "session_status") {
		if len(current) > 0 && current[0] != nil {
			threadsConnected = "1"
			threadID, _, _ := performanceSchemaSessionIdentity(current[0])
			if e != nil && e.metricsRecorder != nil {
				count := 0
				for _, event := range e.metricsRecorder.StatementHistory() {
					if event.ThreadID == threadID {
						count++
					}
				}
				queryCount = strconv.Itoa(count)
			}
			for _, event := range e.activeStatementEvents() {
				if event.ThreadID == threadID {
					threadsRunning = "1"
					break
				}
			}
		}
	} else {
		if e != nil {
			var currentSession server.MySQLServerSession
			if len(current) > 0 {
				currentSession = current[0]
			}
			threadsConnected = strconv.Itoa(len(e.performanceSchemaSessions(currentSession)))
			threadsRunning = strconv.Itoa(len(e.activeStatementEvents()))
		}
	}
	rows := [][]interface{}{
		{"Threads_connected", threadsConnected},
		{"Threads_running", threadsRunning},
		{"Queries", queryCount},
		{"Uptime", "0"},
	}
	if query != "" {
		rows = filterPerformanceSchemaVariableRows(query, rows)
	}
	projected := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		if len(row) < 2 {
			continue
		}
		projected = append(projected, projectInformationSchemaRow(columns, map[string]interface{}{
			"VARIABLE_NAME":  row[0],
			"VARIABLE_VALUE": row[1],
		}))
	}
	return newInformationSchemaSelectResult(name, columns, projected)
}

func (e *XMySQLExecutor) executePerformanceSchemaThreadsSelect(query string, current ...server.MySQLServerSession) *SelectResult {
	defaults := []string{
		"THREAD_ID", "NAME", "TYPE", "PROCESSLIST_ID", "PROCESSLIST_USER", "PROCESSLIST_HOST",
		"PROCESSLIST_DB", "PROCESSLIST_COMMAND", "PROCESSLIST_TIME", "PROCESSLIST_STATE", "PROCESSLIST_INFO",
		"PARENT_THREAD_ID", "ROLE", "INSTRUMENTED", "HISTORY", "CONNECTION_TYPE", "THREAD_OS_ID",
		"RESOURCE_GROUP", "EXECUTION_ENGINE", "CONTROLLED_MEMORY", "MAX_CONTROLLED_MEMORY", "TOTAL_MEMORY",
		"MAX_TOTAL_MEMORY", "TELEMETRY_ACTIVE",
	}
	columns := requestedInformationSchemaColumns(query, defaults)
	if e == nil || e.processlistProvider == nil {
		if len(current) > 0 && current[0] != nil {
			process := e.processlistRow(current[0], query, true)
			actorSetting := e.performanceSchemaStatementSettingForSession(current[0])
			values := map[string]interface{}{
				"THREAD_ID": process[0], "NAME": "thread/sql/one_connection", "TYPE": "FOREGROUND",
				"PROCESSLIST_ID": process[0], "PROCESSLIST_USER": nullableInformationSchemaString(process[1]),
				"PROCESSLIST_HOST": nullableInformationSchemaString(process[2]), "PROCESSLIST_DB": nullableInformationSchemaString(process[3]),
				"PROCESSLIST_COMMAND": nullableInformationSchemaString(process[4]), "PROCESSLIST_TIME": process[5],
				"PROCESSLIST_STATE": nullableInformationSchemaString(process[6]), "PROCESSLIST_INFO": nullableInformationSchemaString(process[7]),
				"PARENT_THREAD_ID": nil, "ROLE": nil, "INSTRUMENTED": performanceSchemaYesNo(actorSetting.Enabled), "HISTORY": performanceSchemaYesNo(actorSetting.History),
				"CONNECTION_TYPE": "TCP/IP", "THREAD_OS_ID": nil, "RESOURCE_GROUP": nil, "EXECUTION_ENGINE": "PRIMARY",
				"CONTROLLED_MEMORY": int64(0), "MAX_CONTROLLED_MEMORY": int64(0), "TOTAL_MEMORY": int64(0),
				"MAX_TOTAL_MEMORY": int64(0), "TELEMETRY_ACTIVE": "NO",
			}
			if !performanceSchemaLockValuesMatch(query, values) {
				return newInformationSchemaSelectResult("performance_schema.threads", columns, nil)
			}
			return newInformationSchemaSelectResult("performance_schema.threads", columns, [][]interface{}{projectInformationSchemaRow(columns, values)})
		}
		state := ""
		if e != nil && e.lockManager != nil && len(e.lockManager.WaitGraphSnapshot()) > 0 {
			state = "Waiting for lock"
		}
		values := map[string]interface{}{
			"THREAD_ID": int64(1), "NAME": "xmysql", "TYPE": "FOREGROUND", "PROCESSLIST_ID": nil,
			"PROCESSLIST_USER": nil, "PROCESSLIST_HOST": nil, "PROCESSLIST_DB": nil,
			"PROCESSLIST_COMMAND": "Sleep", "PROCESSLIST_TIME": int64(0), "PROCESSLIST_STATE": state,
			"PROCESSLIST_INFO": nil, "PARENT_THREAD_ID": nil, "ROLE": nil, "INSTRUMENTED": "YES",
			"HISTORY": "YES", "CONNECTION_TYPE": nil, "THREAD_OS_ID": nil, "RESOURCE_GROUP": nil,
			"EXECUTION_ENGINE": "PRIMARY", "CONTROLLED_MEMORY": int64(0), "MAX_CONTROLLED_MEMORY": int64(0),
			"TOTAL_MEMORY": int64(0), "MAX_TOTAL_MEMORY": int64(0), "TELEMETRY_ACTIVE": "NO",
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			return newInformationSchemaSelectResult("performance_schema.threads", columns, nil)
		}
		return newInformationSchemaSelectResult("performance_schema.threads", columns, [][]interface{}{projectInformationSchemaRow(columns, values)})
	}
	var session server.MySQLServerSession
	if len(current) > 0 {
		session = current[0]
	}
	canInspectAll := session == nil || sessionHasProcessPrivilege(session)
	currentUser := ""
	if session != nil {
		currentUser, _ = session.GetParamByName("user").(string)
	}
	rows := make([][]interface{}, 0)
	for _, listedSession := range e.processlistProvider() {
		if listedSession == nil {
			continue
		}
		listedUser, _ := listedSession.GetParamByName("user").(string)
		isCurrent := listedSession == session
		if !canInspectAll && !isCurrent && (currentUser == "" || !strings.EqualFold(currentUser, listedUser)) {
			continue
		}
		process := e.processlistRow(listedSession, query, isCurrent)
		actorSetting := e.performanceSchemaStatementSettingForSession(listedSession)
		values := map[string]interface{}{
			"THREAD_ID": process[0], "NAME": "thread/sql/one_connection", "TYPE": "FOREGROUND",
			"PROCESSLIST_ID": process[0], "PROCESSLIST_USER": nullableInformationSchemaString(process[1]),
			"PROCESSLIST_HOST": nullableInformationSchemaString(process[2]), "PROCESSLIST_DB": nullableInformationSchemaString(process[3]),
			"PROCESSLIST_COMMAND": nullableInformationSchemaString(process[4]), "PROCESSLIST_TIME": process[5],
			"PROCESSLIST_STATE": nullableInformationSchemaString(process[6]), "PROCESSLIST_INFO": nullableInformationSchemaString(process[7]),
			"PARENT_THREAD_ID": nil, "ROLE": nil, "INSTRUMENTED": performanceSchemaYesNo(actorSetting.Enabled), "HISTORY": performanceSchemaYesNo(actorSetting.History),
			"CONNECTION_TYPE": "TCP/IP", "THREAD_OS_ID": nil, "RESOURCE_GROUP": nil, "EXECUTION_ENGINE": "PRIMARY",
			"CONTROLLED_MEMORY": int64(0), "MAX_CONTROLLED_MEMORY": int64(0), "TOTAL_MEMORY": int64(0),
			"MAX_TOTAL_MEMORY": int64(0), "TELEMETRY_ACTIVE": "NO",
		}
		if performanceSchemaLockValuesMatch(query, values) {
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	if len(rows) == 0 && session == nil {
		values := map[string]interface{}{
			"THREAD_ID": int64(1), "NAME": "xmysql", "TYPE": "FOREGROUND", "PROCESSLIST_ID": nil,
			"PROCESSLIST_USER": nil, "PROCESSLIST_HOST": nil, "PROCESSLIST_DB": nil, "PROCESSLIST_COMMAND": "Sleep",
			"PROCESSLIST_TIME": int64(0), "PROCESSLIST_STATE": nil, "PROCESSLIST_INFO": nil, "PARENT_THREAD_ID": nil,
			"ROLE": nil, "INSTRUMENTED": "YES", "HISTORY": "YES", "CONNECTION_TYPE": nil, "THREAD_OS_ID": nil,
			"RESOURCE_GROUP": nil, "EXECUTION_ENGINE": "PRIMARY", "CONTROLLED_MEMORY": int64(0),
			"MAX_CONTROLLED_MEMORY": int64(0), "TOTAL_MEMORY": int64(0), "MAX_TOTAL_MEMORY": int64(0), "TELEMETRY_ACTIVE": "NO",
		}
		if performanceSchemaLockValuesMatch(query, values) {
			rows = [][]interface{}{projectInformationSchemaRow(columns, values)}
		}
	}
	return newInformationSchemaSelectResult("performance_schema.threads", columns, rows)
}

func nullableInformationSchemaString(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "" {
		return nil
	}
	return text
}

func (e *XMySQLExecutor) executePerformanceSchemaSetupThreadsSelect(query string, current server.MySQLServerSession) *SelectResult {
	defaults := []string{"NAME", "TYPE", "PROCESSLIST_ID", "PROCESSLIST_USER", "PROCESSLIST_HOST", "ENABLED", "HISTORY", "CONNECTION_TYPE", "THREAD_ID", "THREAD_OS_ID"}
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
	rows := make([][]interface{}, 0, len(sessions))
	for _, session := range sessions {
		if session == nil {
			continue
		}
		user, _ := session.GetParamByName("user").(string)
		host, _ := session.GetParamByName("host").(string)
		if strings.TrimSpace(host) == "" {
			host = "localhost"
		}
		threadID := int64Param(session.GetParamByName("connection_id"))
		actorSetting := e.performanceSchemaStatementSettingForSession(session)
		values := map[string]interface{}{
			"NAME": "thread/sql/one_connection", "TYPE": "FOREGROUND", "PROCESSLIST_ID": threadID,
			"PROCESSLIST_USER": user, "PROCESSLIST_HOST": host, "ENABLED": performanceSchemaYesNo(actorSetting.Enabled),
			"HISTORY": performanceSchemaYesNo(actorSetting.History), "CONNECTION_TYPE": "TCP/IP", "THREAD_ID": threadID, "THREAD_OS_ID": nil,
		}
		if performanceSchemaLockValuesMatch(query, values) {
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("performance_schema.setup_threads", columns, rows)
}

func (e *XMySQLExecutor) executePerformanceSchemaSessionConnectAttrsSelect(query string, current server.MySQLServerSession) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{"PROCESSLIST_ID", "ATTR_NAME", "ATTR_VALUE", "ORDINAL_POSITION"})
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
	processlistID, hasProcesslistID := informationSchemaUint64Filter(query, "processlist_id")
	type attrRow struct {
		processlistID int64
		name          string
		value         string
	}
	attrRows := make([]attrRow, 0)
	for _, listed := range sessions {
		if listed == nil {
			continue
		}
		id := int64Param(listed.GetParamByName("connection_id"))
		if hasProcesslistID && (id < 0 || uint64(id) != processlistID) {
			continue
		}
		attributes := performanceSchemaSessionAttributes(listed.GetParamByName("connection_attributes"))
		keys := make([]string, 0, len(attributes))
		for key := range attributes {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			attrRows = append(attrRows, attrRow{processlistID: id, name: key, value: attributes[key]})
		}
	}
	sort.Slice(attrRows, func(i, j int) bool {
		if attrRows[i].processlistID != attrRows[j].processlistID {
			return attrRows[i].processlistID < attrRows[j].processlistID
		}
		return attrRows[i].name < attrRows[j].name
	})
	rows := make([][]interface{}, 0, len(attrRows))
	lastID := int64(-1)
	ordinal := int64(0)
	for _, attr := range attrRows {
		if attr.processlistID != lastID {
			lastID = attr.processlistID
			ordinal = 0
		}
		values := map[string]interface{}{
			"PROCESSLIST_ID": attr.processlistID, "ATTR_NAME": attr.name, "ATTR_VALUE": attr.value, "ORDINAL_POSITION": ordinal,
		}
		if performanceSchemaLockValuesMatch(query, values) {
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
		ordinal++
	}
	return newInformationSchemaSelectResult("performance_schema.session_connect_attrs", columns, rows)
}

func (e *XMySQLExecutor) executePerformanceSchemaSessionAccountConnectAttrsSelect(query string, current server.MySQLServerSession) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{"PROCESSLIST_ID", "ATTR_NAME", "ATTR_VALUE", "ORDINAL_POSITION"})
	if current == nil {
		return newInformationSchemaSelectResult("performance_schema.session_account_connect_attrs", columns, nil)
	}
	attributes := performanceSchemaSessionAttributes(current.GetParamByName("connection_attributes"))
	keys := make([]string, 0, len(attributes))
	for key := range attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	processlistID := int64Param(current.GetParamByName("connection_id"))
	rows := make([][]interface{}, 0, len(keys))
	for ordinal, key := range keys {
		values := map[string]interface{}{
			"PROCESSLIST_ID": processlistID, "ATTR_NAME": key, "ATTR_VALUE": attributes[key], "ORDINAL_POSITION": int64(ordinal),
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("performance_schema.session_account_connect_attrs", columns, rows)
}

func performanceSchemaSessionAttributes(raw interface{}) map[string]string {
	switch attributes := raw.(type) {
	case map[string]string:
		return attributes
	case map[string]interface{}:
		result := make(map[string]string, len(attributes))
		for key, value := range attributes {
			result[key] = fmt.Sprint(value)
		}
		return result
	case interface{ GetAll() map[string]string }:
		return attributes.GetAll()
	default:
		return nil
	}
}

func (e *XMySQLExecutor) executePerformanceSchemaSocketInstancesSelect(query string, current server.MySQLServerSession) *SelectResult {
	defaults := []string{"EVENT_NAME", "OBJECT_INSTANCE_BEGIN", "THREAD_ID", "SOCKET_ID", "IP", "PORT", "STATE"}
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
	rows := make([][]interface{}, 0, len(sessions))
	for _, listed := range sessions {
		if listed == nil {
			continue
		}
		threadID := int64Param(listed.GetParamByName("connection_id"))
		ip, port := performanceSchemaSocketEndpoint(listed)
		if !performanceSchemaSummaryFilterMatches(query, "event_name", "wait/io/socket/sql/client_connection") ||
			!performanceSchemaSummaryFilterMatches(query, "thread_id", fmt.Sprint(threadID)) ||
			!performanceSchemaSummaryFilterMatches(query, "socket_id", fmt.Sprint(threadID)) ||
			!performanceSchemaSummaryFilterMatches(query, "ip", ip) ||
			!performanceSchemaSummaryFilterMatches(query, "port", fmt.Sprint(port)) {
			continue
		}
		values := map[string]interface{}{
			"EVENT_NAME": "wait/io/socket/sql/client_connection", "OBJECT_INSTANCE_BEGIN": threadID,
			"THREAD_ID": threadID, "SOCKET_ID": threadID, "IP": ip, "PORT": port, "STATE": "ACTIVE",
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("performance_schema.socket_instances", columns, rows)
}

func performanceSchemaSocketEndpoint(session server.MySQLServerSession) (string, int64) {
	host, _ := session.GetParamByName("host").(string)
	raw, _ := session.GetParamByName("remote_addr").(string)
	ip := strings.TrimSpace(host)
	var port int64
	if parsedHost, parsedPort, err := net.SplitHostPort(strings.TrimSpace(raw)); err == nil {
		if strings.TrimSpace(parsedHost) != "" {
			ip = strings.Trim(parsedHost, "[]")
		}
		if parsed, parseErr := strconv.ParseInt(parsedPort, 10, 64); parseErr == nil && parsed >= 0 {
			port = parsed
		}
	} else if parsedIP := net.ParseIP(strings.TrimSpace(raw)); parsedIP != nil {
		ip = parsedIP.String()
	}
	if ip == "" {
		ip = "localhost"
	}
	return ip, port
}

func (e *XMySQLExecutor) executePerformanceSchemaSocketSummarySelect(query string, current server.MySQLServerSession, byInstance bool) *SelectResult {
	instanceColumns := []string{
		"EVENT_NAME", "OBJECT_INSTANCE_BEGIN",
		"COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT",
		"COUNT_READ", "SUM_TIMER_READ", "MIN_TIMER_READ", "AVG_TIMER_READ", "MAX_TIMER_READ", "SUM_NUMBER_OF_BYTES_READ",
		"COUNT_WRITE", "SUM_TIMER_WRITE", "MIN_TIMER_WRITE", "AVG_TIMER_WRITE", "MAX_TIMER_WRITE", "SUM_NUMBER_OF_BYTES_WRITE",
		"COUNT_MISC", "SUM_TIMER_MISC", "MIN_TIMER_MISC", "AVG_TIMER_MISC", "MAX_TIMER_MISC",
	}
	eventColumns := []string{
		"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT",
		"COUNT_READ", "SUM_TIMER_READ", "MIN_TIMER_READ", "AVG_TIMER_READ", "MAX_TIMER_READ", "SUM_NUMBER_OF_BYTES_READ",
		"COUNT_WRITE", "SUM_TIMER_WRITE", "MIN_TIMER_WRITE", "AVG_TIMER_WRITE", "MAX_TIMER_WRITE", "SUM_NUMBER_OF_BYTES_WRITE",
		"COUNT_MISC", "SUM_TIMER_MISC", "MIN_TIMER_MISC", "AVG_TIMER_MISC", "MAX_TIMER_MISC",
	}
	name := "performance_schema.socket_summary_by_event_name"
	defaults := eventColumns
	if byInstance {
		name = "performance_schema.socket_summary_by_instance"
		defaults = instanceColumns
	}
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
	rows := make([][]interface{}, 0, len(sessions))
	if byInstance {
		for _, listed := range sessions {
			if listed == nil {
				continue
			}
			threadID := int64Param(listed.GetParamByName("connection_id"))
			ip, port := performanceSchemaSocketEndpoint(listed)
			if !performanceSchemaSummaryFilterMatches(query, "event_name", "wait/io/socket/sql/client_connection") ||
				!performanceSchemaSummaryFilterMatches(query, "thread_id", fmt.Sprint(threadID)) ||
				!performanceSchemaSummaryFilterMatches(query, "socket_id", fmt.Sprint(threadID)) ||
				!performanceSchemaSummaryFilterMatches(query, "ip", ip) ||
				!performanceSchemaSummaryFilterMatches(query, "port", fmt.Sprint(port)) {
				continue
			}
			values := map[string]interface{}{
				"EVENT_NAME": "wait/io/socket/sql/client_connection", "OBJECT_INSTANCE_BEGIN": threadID,
				"THREAD_ID": threadID, "SOCKET_ID": threadID, "IP": ip, "PORT": port,
				"COUNT_STAR": int64(0), "SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0), "AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0),
				"COUNT_READ": int64(0), "SUM_TIMER_READ": int64(0), "MIN_TIMER_READ": int64(0), "AVG_TIMER_READ": int64(0), "MAX_TIMER_READ": int64(0),
				"SUM_NUMBER_OF_BYTES_READ": int64(0),
				"COUNT_WRITE":              int64(0), "SUM_TIMER_WRITE": int64(0), "MIN_TIMER_WRITE": int64(0), "AVG_TIMER_WRITE": int64(0), "MAX_TIMER_WRITE": int64(0),
				"SUM_NUMBER_OF_BYTES_WRITE": int64(0),
				"COUNT_MISC":                int64(0), "SUM_TIMER_MISC": int64(0), "MIN_TIMER_MISC": int64(0), "AVG_TIMER_MISC": int64(0), "MAX_TIMER_MISC": int64(0),
			}
			if !performanceSchemaLockValuesMatch(query, values) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
		return newInformationSchemaSelectResult(name, columns, rows)
	}

	count := int64(0)
	for _, listed := range sessions {
		if listed != nil {
			count++
		}
	}
	if count > 0 && performanceSchemaSummaryFilterMatches(query, "event_name", "wait/io/socket/sql/client_connection") {
		values := map[string]interface{}{
			"EVENT_NAME": "wait/io/socket/sql/client_connection", "COUNT_STAR": count,
			"SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0), "AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0),
			"COUNT_READ": int64(0), "SUM_TIMER_READ": int64(0), "MIN_TIMER_READ": int64(0), "AVG_TIMER_READ": int64(0), "MAX_TIMER_READ": int64(0),
			"SUM_NUMBER_OF_BYTES_READ": int64(0),
			"COUNT_WRITE":              int64(0), "SUM_TIMER_WRITE": int64(0), "MIN_TIMER_WRITE": int64(0), "AVG_TIMER_WRITE": int64(0), "MAX_TIMER_WRITE": int64(0),
			"SUM_NUMBER_OF_BYTES_WRITE": int64(0),
			"COUNT_MISC":                int64(0), "SUM_TIMER_MISC": int64(0), "MIN_TIMER_MISC": int64(0), "AVG_TIMER_MISC": int64(0), "MAX_TIMER_MISC": int64(0),
		}
		if performanceSchemaLockValuesMatch(query, values) {
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult(name, columns, rows)
}

func (e *XMySQLExecutor) executePerformanceSchemaFileInstancesSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{"FILE_NAME", "EVENT_NAME", "OPEN_COUNT"})
	dataDir := "data"
	if e != nil {
		dataDir = e.getDataDir()
	}
	instances := performanceSchemaFileInstanceSnapshot(dataDir, nil)
	if e != nil {
		instances = performanceSchemaFileInstanceSnapshot(dataDir, e.metricsRecorder)
	}
	rows := make([][]interface{}, 0, len(instances))
	for _, instance := range instances {
		if !performanceSchemaSummaryFilterMatches(query, "file_name", instance.name) ||
			!performanceSchemaSummaryFilterMatches(query, "event_name", instance.event) {
			continue
		}
		values := map[string]interface{}{
			"FILE_NAME": instance.name, "EVENT_NAME": instance.event, "OPEN_COUNT": instance.openCount,
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("performance_schema.file_instances", columns, rows)
}

type performanceSchemaFileInstance struct {
	name      string
	event     string
	openCount int64
	summary   *observabilitymetrics.FileSummaryRow
}

func performanceSchemaFileInstances(dataDir string) []performanceSchemaFileInstance {
	instances := make([]performanceSchemaFileInstance, 0)
	if _, err := os.Stat(dataDir); err != nil {
		return instances
	}
	_ = filepath.WalkDir(dataDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if entry == nil || !entry.Type().IsRegular() {
			return nil
		}
		instances = append(instances, performanceSchemaFileInstance{name: path, event: performanceSchemaFileEventName(path)})
		return nil
	})
	sort.Slice(instances, func(i, j int) bool { return instances[i].name < instances[j].name })
	return instances
}

func performanceSchemaFileInstanceSnapshot(dataDir string, recorder *observabilitymetrics.RuntimeRecorder) []performanceSchemaFileInstance {
	physical := performanceSchemaFileInstances(dataDir)
	byName := make(map[string]int, len(physical))
	for index := range physical {
		byName[filepath.Clean(physical[index].name)] = index
	}
	if recorder != nil {
		for _, summary := range recorder.FileSummary() {
			if !performanceSchemaPathWithin(summary.FileName, dataDir) {
				continue
			}
			cleanName := filepath.Clean(summary.FileName)
			index, exists := byName[cleanName]
			if !exists {
				byName[cleanName] = len(physical)
				physical = append(physical, performanceSchemaFileInstance{name: cleanName, event: summary.EventName})
				index = len(physical) - 1
			}
			physical[index].openCount = summary.OpenCount
			copySummary := summary
			physical[index].summary = &copySummary
		}
	}
	sort.Slice(physical, func(i, j int) bool { return physical[i].name < physical[j].name })
	return physical
}

func performanceSchemaPathWithin(path, dataDir string) bool {
	path = filepath.Clean(path)
	dataDir = filepath.Clean(strings.TrimSpace(dataDir))
	if path == "" || dataDir == "" {
		return false
	}
	relative, err := filepath.Rel(dataDir, path)
	if err != nil {
		return false
	}
	return relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))
}

func (e *XMySQLExecutor) executePerformanceSchemaFileSummarySelect(query string, byInstance bool) *SelectResult {
	instanceColumns := []string{
		"FILE_NAME", "EVENT_NAME", "OBJECT_INSTANCE_BEGIN",
		"COUNT_READ", "SUM_TIMER_READ", "MIN_TIMER_READ", "AVG_TIMER_READ", "MAX_TIMER_READ",
		"COUNT_WRITE", "SUM_TIMER_WRITE", "MIN_TIMER_WRITE", "AVG_TIMER_WRITE", "MAX_TIMER_WRITE",
		"COUNT_MISC", "SUM_TIMER_MISC", "MIN_TIMER_MISC", "AVG_TIMER_MISC", "MAX_TIMER_MISC",
	}
	eventColumns := []string{
		"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT",
		"COUNT_READ", "SUM_TIMER_READ", "MIN_TIMER_READ", "AVG_TIMER_READ", "MAX_TIMER_READ",
		"COUNT_WRITE", "SUM_TIMER_WRITE", "MIN_TIMER_WRITE", "AVG_TIMER_WRITE", "MAX_TIMER_WRITE",
		"COUNT_MISC", "SUM_TIMER_MISC", "MIN_TIMER_MISC", "AVG_TIMER_MISC", "MAX_TIMER_MISC",
	}
	name := "performance_schema.file_summary_by_event_name"
	defaults := eventColumns
	if byInstance {
		name = "performance_schema.file_summary_by_instance"
		defaults = instanceColumns
	}
	columns := requestedInformationSchemaColumns(query, defaults)
	dataDir := "data"
	if e != nil {
		dataDir = e.getDataDir()
	}
	instances := performanceSchemaFileInstanceSnapshot(dataDir, nil)
	if e != nil {
		instances = performanceSchemaFileInstanceSnapshot(dataDir, e.metricsRecorder)
	}
	rows := make([][]interface{}, 0, len(instances))
	if byInstance {
		for index, instance := range instances {
			if !performanceSchemaSummaryFilterMatches(query, "file_name", instance.name) ||
				!performanceSchemaSummaryFilterMatches(query, "event_name", instance.event) {
				continue
			}
			values := performanceSchemaFileSummaryValues(instance, int64(index+1))
			if !performanceSchemaLockValuesMatch(query, values) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
		return newInformationSchemaSelectResult(name, columns, rows)
	}
	counts := make(map[string]performanceSchemaFileEventSummary)
	for _, instance := range instances {
		summary := counts[instance.event]
		if instance.summary == nil {
			// Preserve a discoverable event row for a physical file that has not
			// produced an observable page operation in this process.
			summary.instanceCount++
		} else {
			summary.countRead += instance.summary.CountRead
			summary.sumTimerRead += instance.summary.SumTimerRead
			summary.minTimerRead = mergePerformanceSchemaTimerMin(summary.minTimerRead, instance.summary.MinTimerRead)
			summary.maxTimerRead = maxInt64(summary.maxTimerRead, instance.summary.MaxTimerRead)
			summary.countWrite += instance.summary.CountWrite
			summary.sumTimerWrite += instance.summary.SumTimerWrite
			summary.minTimerWrite = mergePerformanceSchemaTimerMin(summary.minTimerWrite, instance.summary.MinTimerWrite)
			summary.maxTimerWrite = maxInt64(summary.maxTimerWrite, instance.summary.MaxTimerWrite)
			summary.countMisc += instance.summary.CountMisc
			summary.sumTimerMisc += instance.summary.SumTimerMisc
			summary.minTimerMisc = mergePerformanceSchemaTimerMin(summary.minTimerMisc, instance.summary.MinTimerMisc)
			summary.maxTimerMisc = maxInt64(summary.maxTimerMisc, instance.summary.MaxTimerMisc)
		}
		counts[instance.event] = summary
	}
	events := make([]string, 0, len(counts))
	for event := range counts {
		events = append(events, event)
	}
	sort.Strings(events)
	for _, event := range events {
		if !performanceSchemaSummaryFilterMatches(query, "event_name", event) {
			continue
		}
		summary := counts[event]
		countStar := summary.countRead + summary.countWrite + summary.countMisc
		if countStar == 0 {
			countStar = summary.instanceCount
		}
		values := map[string]interface{}{
			"EVENT_NAME": event, "COUNT_STAR": countStar,
			"SUM_TIMER_WAIT": summary.sumTimerRead + summary.sumTimerWrite + summary.sumTimerMisc,
			"MIN_TIMER_WAIT": performanceSchemaTimerMin(summary.minTimerRead, summary.minTimerWrite, summary.minTimerMisc),
			"AVG_TIMER_WAIT": performanceSchemaTimerAverage(summary.sumTimerRead+summary.sumTimerWrite+summary.sumTimerMisc, summary.countRead+summary.countWrite+summary.countMisc),
			"MAX_TIMER_WAIT": maxInt64(summary.maxTimerRead, maxInt64(summary.maxTimerWrite, summary.maxTimerMisc)),
			"COUNT_READ":     summary.countRead, "SUM_TIMER_READ": summary.sumTimerRead, "MIN_TIMER_READ": summary.minTimerRead, "AVG_TIMER_READ": performanceSchemaTimerAverage(summary.sumTimerRead, summary.countRead), "MAX_TIMER_READ": summary.maxTimerRead,
			"COUNT_WRITE": summary.countWrite, "SUM_TIMER_WRITE": summary.sumTimerWrite, "MIN_TIMER_WRITE": summary.minTimerWrite, "AVG_TIMER_WRITE": performanceSchemaTimerAverage(summary.sumTimerWrite, summary.countWrite), "MAX_TIMER_WRITE": summary.maxTimerWrite,
			"COUNT_MISC": summary.countMisc, "SUM_TIMER_MISC": summary.sumTimerMisc, "MIN_TIMER_MISC": summary.minTimerMisc, "AVG_TIMER_MISC": performanceSchemaTimerAverage(summary.sumTimerMisc, summary.countMisc), "MAX_TIMER_MISC": summary.maxTimerMisc,
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult(name, columns, rows)
}

type performanceSchemaFileEventSummary struct {
	instanceCount int64
	countRead     int64
	sumTimerRead  int64
	minTimerRead  int64
	maxTimerRead  int64
	countWrite    int64
	sumTimerWrite int64
	minTimerWrite int64
	maxTimerWrite int64
	countMisc     int64
	sumTimerMisc  int64
	minTimerMisc  int64
	maxTimerMisc  int64
}

func performanceSchemaFileSummaryValues(instance performanceSchemaFileInstance, fallbackInstance int64) map[string]interface{} {
	values := map[string]interface{}{
		"FILE_NAME": instance.name, "EVENT_NAME": instance.event, "OBJECT_INSTANCE_BEGIN": fallbackInstance,
		"COUNT_READ": int64(0), "SUM_TIMER_READ": int64(0), "MIN_TIMER_READ": int64(0), "AVG_TIMER_READ": int64(0), "MAX_TIMER_READ": int64(0),
		"COUNT_WRITE": int64(0), "SUM_TIMER_WRITE": int64(0), "MIN_TIMER_WRITE": int64(0), "AVG_TIMER_WRITE": int64(0), "MAX_TIMER_WRITE": int64(0),
		"COUNT_MISC": int64(0), "SUM_TIMER_MISC": int64(0), "MIN_TIMER_MISC": int64(0), "AVG_TIMER_MISC": int64(0), "MAX_TIMER_MISC": int64(0),
	}
	if instance.summary == nil {
		return values
	}
	if instance.summary.ObjectInstanceBegin != 0 {
		values["OBJECT_INSTANCE_BEGIN"] = instance.summary.ObjectInstanceBegin
	}
	values["COUNT_READ"] = instance.summary.CountRead
	values["SUM_TIMER_READ"] = instance.summary.SumTimerRead
	values["MIN_TIMER_READ"] = instance.summary.MinTimerRead
	values["AVG_TIMER_READ"] = performanceSchemaTimerAverage(instance.summary.SumTimerRead, instance.summary.CountRead)
	values["MAX_TIMER_READ"] = instance.summary.MaxTimerRead
	values["COUNT_WRITE"] = instance.summary.CountWrite
	values["SUM_TIMER_WRITE"] = instance.summary.SumTimerWrite
	values["MIN_TIMER_WRITE"] = instance.summary.MinTimerWrite
	values["AVG_TIMER_WRITE"] = performanceSchemaTimerAverage(instance.summary.SumTimerWrite, instance.summary.CountWrite)
	values["MAX_TIMER_WRITE"] = instance.summary.MaxTimerWrite
	values["COUNT_MISC"] = instance.summary.CountMisc
	values["SUM_TIMER_MISC"] = instance.summary.SumTimerMisc
	values["MIN_TIMER_MISC"] = instance.summary.MinTimerMisc
	values["AVG_TIMER_MISC"] = performanceSchemaTimerAverage(instance.summary.SumTimerMisc, instance.summary.CountMisc)
	values["MAX_TIMER_MISC"] = instance.summary.MaxTimerMisc
	return values
}

func performanceSchemaTimerAverage(sum, count int64) int64 {
	if count <= 0 {
		return 0
	}
	return sum / count
}

func performanceSchemaTimerMin(values ...int64) int64 {
	minimum := int64(0)
	for _, value := range values {
		minimum = mergePerformanceSchemaTimerMin(minimum, value)
	}
	return minimum
}

func mergePerformanceSchemaTimerMin(current, candidate int64) int64 {
	if candidate <= 0 {
		return current
	}
	if current <= 0 || candidate < current {
		return candidate
	}
	return current
}

func performanceSchemaFileEventName(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".ibd":
		return "wait/io/file/innodb/innodb_data_file"
	case ".log":
		return "wait/io/file/innodb/innodb_log_file"
	case ".frm":
		return "wait/io/file/sql/FRM"
	default:
		return "wait/io/file/sql/file"
	}
}

type performanceSchemaConnectionSummary struct {
	user, host            string
	current, total, hosts int64
	maxControlled, maxAll int64
}

func (e *XMySQLExecutor) executePerformanceSchemaConnectionSummarySelect(query, name string, current server.MySQLServerSession) *SelectResult {
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

	byKey := make(map[string]*performanceSchemaConnectionSummary)
	for _, session := range sessions {
		if session == nil {
			continue
		}
		user, _ := session.GetParamByName("user").(string)
		host, _ := session.GetParamByName("host").(string)
		if strings.TrimSpace(host) == "" {
			host = "localhost"
		}
		key := user + "\x00" + host
		summary := byKey[key]
		if summary == nil {
			summary = &performanceSchemaConnectionSummary{user: user, host: host}
			byKey[key] = summary
		}
		summary.current++
		summary.total++
	}
	if e != nil && e.metricsRecorder != nil {
		for _, total := range e.metricsRecorder.ConnectionTotals() {
			key := total.User + "\x00" + total.Host
			summary := byKey[key]
			if summary == nil {
				summary = &performanceSchemaConnectionSummary{user: total.User, host: total.Host}
				byKey[key] = summary
			}
			if total.TotalConnections > summary.total {
				summary.total = total.TotalConnections
			}
		}
	}

	values := make([]performanceSchemaConnectionSummary, 0, len(byKey))
	for _, summary := range byKey {
		values = append(values, *summary)
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].user != values[j].user {
			return values[i].user < values[j].user
		}
		return values[i].host < values[j].host
	})

	if name == "performance_schema.users" {
		columns := []string{"USER", "CURRENT_CONNECTIONS", "TOTAL_CONNECTIONS", "MAX_SESSION_CONTROLLED_MEMORY", "MAX_SESSION_TOTAL_MEMORY", "COUNT_HOSTS"}
		byUser := make(map[string]*performanceSchemaConnectionSummary)
		for _, summary := range values {
			combined := byUser[summary.user]
			if combined == nil {
				combined = &performanceSchemaConnectionSummary{user: summary.user}
				byUser[summary.user] = combined
			}
			combined.current += summary.current
			combined.total += summary.total
			combined.maxControlled = maxInt64(combined.maxControlled, summary.maxControlled)
			combined.maxAll = maxInt64(combined.maxAll, summary.maxAll)
			combined.hosts++
		}
		userValues := make([]performanceSchemaConnectionSummary, 0, len(byUser))
		for _, summary := range byUser {
			userValues = append(userValues, *summary)
		}
		sort.Slice(userValues, func(i, j int) bool { return userValues[i].user < userValues[j].user })
		rows := make([][]interface{}, 0, len(userValues))
		for _, summary := range userValues {
			if !performanceSchemaSummaryFilterMatches(query, "user", summary.user) {
				continue
			}
			row := map[string]interface{}{
				"USER": summary.user, "CURRENT_CONNECTIONS": summary.current, "TOTAL_CONNECTIONS": summary.total,
				"MAX_SESSION_CONTROLLED_MEMORY": summary.maxControlled, "MAX_SESSION_TOTAL_MEMORY": summary.maxAll, "COUNT_HOSTS": summary.hosts,
			}
			if performanceSchemaLockValuesMatch(query, row) {
				rows = append(rows, projectInformationSchemaRow(columns, row))
			}
		}
		return newInformationSchemaSelectResult(name, columns, rows)
	}

	var columns []string
	rows := make([][]interface{}, 0, len(values))
	switch name {
	case "performance_schema.accounts":
		columns = []string{"USER", "HOST", "CURRENT_CONNECTIONS", "TOTAL_CONNECTIONS", "MAX_SESSION_CONTROLLED_MEMORY", "MAX_SESSION_TOTAL_MEMORY"}
		for _, summary := range values {
			if !performanceSchemaSummaryFilterMatches(query, "user", summary.user) || !performanceSchemaSummaryFilterMatches(query, "host", summary.host) {
				continue
			}
			row := map[string]interface{}{
				"USER": summary.user, "HOST": summary.host, "CURRENT_CONNECTIONS": summary.current, "TOTAL_CONNECTIONS": summary.total,
				"MAX_SESSION_CONTROLLED_MEMORY": summary.maxControlled, "MAX_SESSION_TOTAL_MEMORY": summary.maxAll,
			}
			if performanceSchemaLockValuesMatch(query, row) {
				rows = append(rows, projectInformationSchemaRow(columns, row))
			}
		}
	case "performance_schema.hosts":
		columns = []string{"HOST", "CURRENT_CONNECTIONS", "TOTAL_CONNECTIONS", "MAX_SESSION_CONTROLLED_MEMORY", "MAX_SESSION_TOTAL_MEMORY", "SUM_CONNECTIONS"}
		byHost := make(map[string]*performanceSchemaConnectionSummary)
		for _, summary := range values {
			combined := byHost[summary.host]
			if combined == nil {
				combined = &performanceSchemaConnectionSummary{host: summary.host}
				byHost[summary.host] = combined
			}
			combined.current += summary.current
			combined.total += summary.total
			combined.maxControlled = maxInt64(combined.maxControlled, summary.maxControlled)
			combined.maxAll = maxInt64(combined.maxAll, summary.maxAll)
		}
		hostValues := make([]performanceSchemaConnectionSummary, 0, len(byHost))
		for _, summary := range byHost {
			hostValues = append(hostValues, *summary)
		}
		sort.Slice(hostValues, func(i, j int) bool { return hostValues[i].host < hostValues[j].host })
		for _, summary := range hostValues {
			if !performanceSchemaSummaryFilterMatches(query, "host", summary.host) {
				continue
			}
			row := map[string]interface{}{
				"HOST": summary.host, "CURRENT_CONNECTIONS": summary.current, "TOTAL_CONNECTIONS": summary.total,
				"MAX_SESSION_CONTROLLED_MEMORY": summary.maxControlled, "MAX_SESSION_TOTAL_MEMORY": summary.maxAll, "SUM_CONNECTIONS": summary.total,
			}
			if performanceSchemaLockValuesMatch(query, row) {
				if performanceSchemaLockValuesMatch(query, row) {
					rows = append(rows, projectInformationSchemaRow(columns, row))
				}
			}
		}
	}
	if columns == nil {
		switch name {
		case "performance_schema.users":
			columns = []string{"USER", "CURRENT_CONNECTIONS", "TOTAL_CONNECTIONS", "MAX_SESSION_CONTROLLED_MEMORY", "MAX_SESSION_TOTAL_MEMORY", "COUNT_HOSTS"}
		case "performance_schema.accounts":
			columns = []string{"USER", "HOST", "CURRENT_CONNECTIONS", "TOTAL_CONNECTIONS", "MAX_SESSION_CONTROLLED_MEMORY", "MAX_SESSION_TOTAL_MEMORY"}
		default:
			columns = []string{"HOST", "CURRENT_CONNECTIONS", "TOTAL_CONNECTIONS", "MAX_SESSION_CONTROLLED_MEMORY", "MAX_SESSION_TOTAL_MEMORY", "SUM_CONNECTIONS"}
		}
	}
	return newInformationSchemaSelectResult(name, columns, rows)
}

func performanceSchemaSummaryFilterMatches(query, column, value string) bool {
	inPattern := regexp.MustCompile(`(?is)\b` + regexp.QuoteMeta(column) + `\s+(not\s+)?in\s*\(([^)]*)\)`)
	for _, match := range inPattern.FindAllStringSubmatch(query, -1) {
		contains := false
		for _, raw := range splitTopLevelComma(match[2]) {
			candidate := strings.Trim(strings.TrimSpace(raw), "'\"")
			if strings.EqualFold(value, candidate) {
				contains = true
				break
			}
		}
		notIn := strings.TrimSpace(match[1]) != ""
		if (notIn && contains) || (!notIn && !contains) {
			return false
		}
	}
	equalityPattern := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(column) + `\s*(=|like)\s*(?:'([^']*)'|"([^"]*)"|([0-9]+))`)
	for _, pattern := range equalityPattern.FindAllStringSubmatch(query, -1) {
		filter := pattern[2]
		if filter == "" {
			filter = pattern[3]
		}
		if filter == "" {
			filter = pattern[4]
		}
		if strings.EqualFold(pattern[1], "=") {
			if !strings.EqualFold(value, filter) {
				return false
			}
			continue
		}
		if !performanceSchemaSQLLike(filter, value) {
			return false
		}
	}
	return true
}

func (e *XMySQLExecutor) performanceSchemaWaitEdges() []manager.WaitEdge {
	if e == nil || e.lockManager == nil {
		return nil
	}
	return e.lockManager.WaitGraphSnapshot()
}

func (e *XMySQLExecutor) performanceSchemaMetadataWaitEdges() []MetadataLockWaitEdge {
	if e == nil {
		return nil
	}
	return e.getDDLCoordinator().MetadataLockWaitEdges()
}

func (e *XMySQLExecutor) executePerformanceSchemaDataLockWaitsSelect(query string) *SelectResult {
	defaults := []string{"ENGINE", "REQUESTING_ENGINE_LOCK_ID", "REQUESTING_ENGINE_TRANSACTION_ID", "REQUESTING_THREAD_ID", "REQUESTING_EVENT_ID", "REQUESTING_OBJECT_INSTANCE_BEGIN", "BLOCKING_ENGINE_LOCK_ID", "BLOCKING_ENGINE_TRANSACTION_ID", "BLOCKING_THREAD_ID", "BLOCKING_EVENT_ID", "BLOCKING_OBJECT_INSTANCE_BEGIN"}
	columns := requestedInformationSchemaColumns(query, defaults)
	rows := make([][]interface{}, 0)
	for _, edge := range e.performanceSchemaWaitEdges() {
		included, _ := e.performanceSchemaObservedWait("wait/lock/table/sql/handler", edge.WaitDuration)
		if !included {
			continue
		}
		values := map[string]interface{}{
			"ENGINE": "INNODB", "REQUESTING_ENGINE_LOCK_ID": edge.ResourceID, "REQUESTING_ENGINE_TRANSACTION_ID": int64(edge.WaitingTxID),
			"REQUESTING_THREAD_ID": int64(edge.WaitingTxID), "REQUESTING_EVENT_ID": int64(0), "REQUESTING_OBJECT_INSTANCE_BEGIN": int64(0),
			"BLOCKING_ENGINE_LOCK_ID": edge.ResourceID, "BLOCKING_ENGINE_TRANSACTION_ID": int64(edge.BlockingTxID),
			"BLOCKING_THREAD_ID": int64(edge.BlockingTxID), "BLOCKING_EVENT_ID": int64(0), "BLOCKING_OBJECT_INSTANCE_BEGIN": int64(0),
			"REQUESTING_LOCK_ID": edge.ResourceID, "BLOCKING_LOCK_ID": edge.ResourceID,
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	for _, edge := range e.performanceSchemaMetadataWaitEdges() {
		included, _ := e.performanceSchemaObservedWait("wait/lock/metadata/sql/mdl", edge.WaitDuration)
		if !included {
			continue
		}
		waitingThread := metadataLockThreadID(edge.WaitingOwner)
		blockingThread := metadataLockThreadID(edge.BlockingOwner)
		if waitingThread == nil || blockingThread == nil {
			continue
		}
		resource := "metadata:" + edge.Table
		values := map[string]interface{}{
			"ENGINE": "INNODB", "REQUESTING_ENGINE_LOCK_ID": resource, "REQUESTING_ENGINE_TRANSACTION_ID": waitingThread,
			"REQUESTING_THREAD_ID": waitingThread, "REQUESTING_EVENT_ID": int64(0), "REQUESTING_OBJECT_INSTANCE_BEGIN": int64(0),
			"BLOCKING_ENGINE_LOCK_ID": resource, "BLOCKING_ENGINE_TRANSACTION_ID": blockingThread,
			"BLOCKING_THREAD_ID": blockingThread, "BLOCKING_EVENT_ID": int64(0), "BLOCKING_OBJECT_INSTANCE_BEGIN": int64(0),
			"REQUESTING_LOCK_ID": resource, "BLOCKING_LOCK_ID": resource,
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("performance_schema.data_lock_waits", columns, rows)
}

func (e *XMySQLExecutor) executePerformanceSchemaDataLocksSelect(query string) *SelectResult {
	defaults := []string{"ENGINE", "ENGINE_LOCK_ID", "ENGINE_TRANSACTION_ID", "THREAD_ID", "EVENT_ID", "OBJECT_SCHEMA", "OBJECT_NAME", "PARTITION_NAME", "SUBPARTITION_NAME", "INDEX_NAME", "OBJECT_INSTANCE_BEGIN", "LOCK_TYPE", "LOCK_MODE", "LOCK_STATUS", "LOCK_DATA"}
	columns := requestedInformationSchemaColumns(query, defaults)
	rows := make([][]interface{}, 0)
	for _, edge := range e.performanceSchemaWaitEdges() {
		included, _ := e.performanceSchemaObservedWait("wait/lock/table/sql/handler", edge.WaitDuration)
		if !included {
			continue
		}
		for _, lock := range []struct {
			txID  uint64
			state string
		}{
			{txID: edge.WaitingTxID, state: "WAITING"},
			{txID: edge.BlockingTxID, state: "GRANTED"},
		} {
			values := map[string]interface{}{
				"ENGINE": "XMYSQL", "ENGINE_LOCK_ID": fmt.Sprintf("%s:%d", edge.ResourceID, lock.txID), "ENGINE_TRANSACTION_ID": int64(lock.txID),
				"THREAD_ID": int64(lock.txID), "EVENT_ID": int64(0), "OBJECT_SCHEMA": nil, "OBJECT_NAME": edge.ResourceID, "PARTITION_NAME": nil, "SUBPARTITION_NAME": nil, "INDEX_NAME": nil,
				"OBJECT_INSTANCE_BEGIN": int64(0), "LOCK_TYPE": performanceSchemaLockType(edge.LockType), "LOCK_MODE": performanceSchemaLockMode(edge.Mode),
				"LOCK_STATUS": lock.state, "LOCK_DATA": nil,
			}
			if !performanceSchemaLockValuesMatch(query, values) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	for _, edge := range e.performanceSchemaMetadataWaitEdges() {
		included, _ := e.performanceSchemaObservedWait("wait/lock/metadata/sql/mdl", edge.WaitDuration)
		if !included {
			continue
		}
		for _, lock := range []struct {
			owner string
			state string
		}{
			{owner: edge.WaitingOwner, state: "WAITING"},
			{owner: edge.BlockingOwner, state: "GRANTED"},
		} {
			threadID := metadataLockThreadID(lock.owner)
			if threadID == nil {
				continue
			}
			values := map[string]interface{}{
				"ENGINE": "XMYSQL", "ENGINE_LOCK_ID": fmt.Sprintf("metadata:%s:%s", edge.Table, lock.owner), "ENGINE_TRANSACTION_ID": threadID,
				"THREAD_ID": threadID, "EVENT_ID": int64(0), "OBJECT_SCHEMA": nil, "OBJECT_NAME": edge.Table, "PARTITION_NAME": nil, "SUBPARTITION_NAME": nil, "INDEX_NAME": nil,
				"OBJECT_INSTANCE_BEGIN": int64(0), "LOCK_TYPE": "METADATA", "LOCK_MODE": performanceSchemaMetadataLockMode(edge.WaitingMode, lock.owner == edge.BlockingOwner, edge.BlockingMode),
				"LOCK_STATUS": lock.state, "LOCK_DATA": nil,
			}
			if !performanceSchemaLockValuesMatch(query, values) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("performance_schema.data_locks", columns, rows)
}

func performanceSchemaMetadataLockMode(waitingMode tableLockMode, blocking bool, blockingMode tableLockMode) string {
	mode := waitingMode
	if blocking {
		mode = blockingMode
	}
	switch mode {
	case tableLockWrite:
		return "EXCLUSIVE"
	case tableLockShared:
		return "SHARED"
	default:
		return "SHARED_READ"
	}
}

func (e *XMySQLExecutor) executePerformanceSchemaMetadataLocksSelect(query string) *SelectResult {
	defaults := []string{"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "OBJECT_INSTANCE_BEGIN", "LOCK_TYPE", "LOCK_DURATION", "LOCK_STATUS", "SOURCE", "OWNER_THREAD_ID", "OWNER_EVENT_ID"}
	columns := requestedInformationSchemaColumns(query, defaults)
	rows := make([][]interface{}, 0)
	if e == nil {
		return newInformationSchemaSelectResult("performance_schema.metadata_locks", columns, rows)
	}
	for _, lock := range e.getDDLCoordinator().MetadataLocks() {
		schema, table := compatibilityQualifiedTable(lock.Table, "")
		values := map[string]interface{}{
			"OBJECT_TYPE": "TABLE", "OBJECT_SCHEMA": schema, "OBJECT_NAME": table, "OBJECT_INSTANCE_BEGIN": nil,
			"LOCK_TYPE": "SHARED", "LOCK_DURATION": "TRANSACTION", "LOCK_STATUS": lock.Status,
			"SOURCE": "xmysql-server/ddl_lock_coordinator.go", "OWNER_THREAD_ID": metadataLockThreadID(lock.Owner),
			"OWNER_EVENT_ID": int64(0), "INSTRUMENTED": "YES",
		}
		if lock.Mode == tableLockDML {
			values["LOCK_TYPE"] = "SHARED_WRITE"
		} else if lock.Mode == tableLockWrite {
			values["LOCK_TYPE"] = "EXCLUSIVE"
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("performance_schema.metadata_locks", columns, rows)
}

func performanceSchemaLockValuesMatch(query string, values map[string]interface{}) bool {
	for column, value := range values {
		if value == nil {
			// SQL comparisons against NULL do not match.  Keep IS NULL/IS NOT
			// NULL predicates for the regular expression/expression path, but do
			// not let an explicit equality or LIKE predicate accidentally keep a
			// row whose projected value is NULL.
			if performanceSchemaSummaryFilterSpecified(query, strings.ToLower(column)) {
				return false
			}
			continue
		}
		if !performanceSchemaSummaryFilterMatches(query, strings.ToLower(column), fmt.Sprint(value)) {
			return false
		}
	}
	return true
}

func performanceSchemaSummaryFilterSpecified(query, column string) bool {
	pattern := regexp.MustCompile(`(?is)\b` + regexp.QuoteMeta(column) + `\s*(=|like)\s*(?:'[^']*'|"[^"]*"|[0-9]+)|\b` + regexp.QuoteMeta(column) + `\s+(?:not\s+)?in\s*\([^)]*\)`)
	return pattern.MatchString(query)
}

func metadataLockThreadID(owner string) interface{} {
	if strings.HasPrefix(owner, "thread/") {
		if id, err := strconv.ParseUint(strings.TrimPrefix(owner, "thread/"), 10, 32); err == nil {
			return int64(id)
		}
	}
	return nil
}

func (e *XMySQLExecutor) executePerformanceSchemaEventsWaitsSelect(query string) *SelectResult {
	defaults := []string{"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT", "SPINS", "OBJECT_SCHEMA", "OBJECT_NAME", "INDEX_NAME", "OBJECT_TYPE", "OPERATION", "NUMBER_OF_BYTES", "FLAGS"}
	columns := requestedInformationSchemaColumns(query, defaults)
	viewName := "performance_schema.events_waits_current"
	if strings.Contains(strings.ToLower(query), "events_waits_history_long") {
		viewName = "performance_schema.events_waits_history_long"
	} else if strings.Contains(strings.ToLower(query), "events_waits_history") {
		viewName = "performance_schema.events_waits_history"
	}
	consumer := viewName[strings.LastIndex(viewName, ".")+1:]
	if e != nil && !e.performanceSchemaConsumerEnabled(consumer) {
		return newInformationSchemaSelectResult(viewName, columns, nil)
	}
	rows := make([][]interface{}, 0)
	eventSequence := int64(0)
	buildWaitValues := func(threadID int64, eventName, source, objectName, operation string, started time.Time, wait time.Duration, timed, current bool) map[string]interface{} {
		eventSequence++
		timerStart, timerEnd, timerWait := performanceSchemaWaitTimerValues(started, wait, timed, current)
		var endEventID interface{} = eventSequence
		if current {
			endEventID = nil
		}
		return map[string]interface{}{
			"THREAD_ID": threadID, "EVENT_ID": eventSequence, "END_EVENT_ID": endEventID, "EVENT_NAME": eventName,
			"SOURCE": source, "TIMER_START": timerStart, "TIMER_END": timerEnd, "TIMER_WAIT": timerWait,
			"SPINS": int64(0), "OBJECT_SCHEMA": nil, "OBJECT_NAME": objectName, "INDEX_NAME": nil, "OBJECT_TYPE": "TABLE", "OPERATION": operation,
			"NUMBER_OF_BYTES": nil, "FLAGS": int64(0),
		}
	}
	for _, edge := range e.performanceSchemaWaitEdges() {
		included, wait := e.performanceSchemaObservedWait("wait/lock/table/sql/handler", edge.WaitDuration)
		if !included {
			continue
		}
		_, timed := e.performanceSchemaInstrumentSetting("wait/lock/table/sql/handler")
		values := buildWaitValues(int64(edge.WaitingTxID), "wait/lock/table/sql/handler", "xmysql/lock_manager.go", edge.ResourceID, "lock", edge.Since, wait, timed, true)
		if performanceSchemaLockValuesMatch(query, values) {
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	for _, edge := range e.performanceSchemaMetadataWaitEdges() {
		included, wait := e.performanceSchemaObservedWait("wait/lock/metadata/sql/mdl", edge.WaitDuration)
		if !included {
			continue
		}
		threadID := metadataLockThreadID(edge.WaitingOwner)
		if threadID == nil {
			continue
		}
		_, timed := e.performanceSchemaInstrumentSetting("wait/lock/metadata/sql/mdl")
		values := buildWaitValues(threadID.(int64), "wait/lock/metadata/sql/mdl", "xmysql/ddl_lock_coordinator.go", edge.Table, "metadata lock", edge.WaitStarted, wait, timed, true)
		if performanceSchemaLockValuesMatch(query, values) {
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	if viewName != "performance_schema.events_waits_current" {
		rows = rows[:0]
		if e != nil && e.lockManager != nil {
			for _, edge := range e.lockManager.WaitHistorySnapshot() {
				included, wait := e.performanceSchemaObservedWait("wait/lock/table/sql/handler", edge.WaitDuration)
				if !included {
					continue
				}
				_, timed := e.performanceSchemaInstrumentSetting("wait/lock/table/sql/handler")
				values := buildWaitValues(int64(edge.WaitingTxID), "wait/lock/table/sql/handler", "xmysql/lock_manager.go", edge.ResourceID, "lock", edge.Since, wait, timed, false)
				if performanceSchemaLockValuesMatch(query, values) {
					rows = append(rows, projectInformationSchemaRow(columns, values))
				}
			}
		}
		if e != nil {
			for _, edge := range e.getDDLCoordinator().MetadataLockWaitHistory() {
				included, wait := e.performanceSchemaObservedWait("wait/lock/metadata/sql/mdl", edge.WaitDuration)
				if !included {
					continue
				}
				threadID := metadataLockThreadID(edge.WaitingOwner)
				if threadID == nil {
					continue
				}
				_, timed := e.performanceSchemaInstrumentSetting("wait/lock/metadata/sql/mdl")
				values := buildWaitValues(threadID.(int64), "wait/lock/metadata/sql/mdl", "xmysql/ddl_lock_coordinator.go", edge.Table, "metadata lock", edge.WaitStarted, wait, timed, false)
				if performanceSchemaLockValuesMatch(query, values) {
					rows = append(rows, projectInformationSchemaRow(columns, values))
				}
			}
		}
	}
	return newInformationSchemaSelectResult(viewName, columns, rows)
}

type performanceSchemaWaitSummary struct {
	threadID int64
	event    string
	count    int64
	sum      int64
	min      int64
	max      int64
}

// executePerformanceSchemaWaitSummarySelect aggregates real bounded wait
// history together with the same live wait edges exposed by
// events_waits_current. Instrument settings control both visibility and
// whether the retained duration is reported.
func (e *XMySQLExecutor) executePerformanceSchemaWaitSummarySelect(query string, byThread bool) *SelectResult {
	defaults := []string{"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"}
	if byThread {
		defaults = append([]string{"THREAD_ID"}, defaults...)
	}
	columns := requestedInformationSchemaColumns(query, defaults)
	byKey := make(map[string]*performanceSchemaWaitSummary)
	add := func(threadID int64, event string, wait time.Duration) {
		if strings.TrimSpace(event) == "" {
			return
		}
		timer := wait.Nanoseconds() * 1000
		if timer < 0 {
			timer = 0
		}
		key := fmt.Sprintf("%d\x00%s", threadID, event)
		summary := byKey[key]
		if summary == nil {
			summary = &performanceSchemaWaitSummary{threadID: threadID, event: event, min: timer, max: timer}
			byKey[key] = summary
		}
		summary.count++
		summary.sum += timer
		if timer < summary.min {
			summary.min = timer
		}
		if timer > summary.max {
			summary.max = timer
		}
	}
	if e != nil && e.lockManager != nil {
		for _, edge := range e.lockManager.WaitHistorySnapshot() {
			included, wait := e.performanceSchemaObservedWait("wait/lock/table/sql/handler", edge.WaitDuration)
			if included {
				add(int64(edge.WaitingTxID), "wait/lock/table/sql/handler", wait)
			}
		}
	}
	if e != nil {
		for _, edge := range e.getDDLCoordinator().MetadataLockWaitHistory() {
			included, wait := e.performanceSchemaObservedWait("wait/lock/metadata/sql/mdl", edge.WaitDuration)
			if included {
				if id, ok := metadataLockThreadID(edge.WaitingOwner).(int64); ok {
					add(id, "wait/lock/metadata/sql/mdl", wait)
				}
			}
		}
	}
	for _, edge := range e.performanceSchemaWaitEdges() {
		included, wait := e.performanceSchemaObservedWait("wait/lock/table/sql/handler", edge.WaitDuration)
		if included {
			add(int64(edge.WaitingTxID), "wait/lock/table/sql/handler", wait)
		}
	}
	for _, edge := range e.performanceSchemaMetadataWaitEdges() {
		included, wait := e.performanceSchemaObservedWait("wait/lock/metadata/sql/mdl", edge.WaitDuration)
		if included {
			threadID := metadataLockThreadID(edge.WaitingOwner)
			if id, ok := threadID.(int64); ok {
				add(id, "wait/lock/metadata/sql/mdl", wait)
			}
		}
	}

	values := make([]performanceSchemaWaitSummary, 0, len(byKey))
	for _, summary := range byKey {
		values = append(values, *summary)
	}
	sort.Slice(values, func(i, j int) bool {
		if byThread && values[i].threadID != values[j].threadID {
			return values[i].threadID < values[j].threadID
		}
		return values[i].event < values[j].event
	})
	rows := make([][]interface{}, 0, len(values))
	if byThread {
		for _, summary := range values {
			if summary.count == 0 {
				continue
			}
			row := map[string]interface{}{
				"THREAD_ID": summary.threadID, "EVENT_NAME": summary.event, "COUNT_STAR": summary.count,
				"SUM_TIMER_WAIT": summary.sum, "MIN_TIMER_WAIT": summary.min, "AVG_TIMER_WAIT": summary.sum / summary.count,
				"MAX_TIMER_WAIT": summary.max,
			}
			if !performanceSchemaLockValuesMatch(query, row) {
				continue
			}
			if performanceSchemaLockValuesMatch(query, row) {
				rows = append(rows, projectInformationSchemaRow(columns, row))
			}
		}
		return newInformationSchemaSelectResult("performance_schema.events_waits_summary_by_thread_by_event_name", columns, rows)
	}

	global := make(map[string]*performanceSchemaWaitSummary)
	for _, summary := range values {
		combined := global[summary.event]
		if combined == nil {
			combined = &performanceSchemaWaitSummary{event: summary.event, min: summary.min, max: summary.max}
			global[summary.event] = combined
		}
		combined.count += summary.count
		combined.sum += summary.sum
		if summary.min < combined.min {
			combined.min = summary.min
		}
		if summary.max > combined.max {
			combined.max = summary.max
		}
	}
	globalValues := make([]performanceSchemaWaitSummary, 0, len(global))
	for _, summary := range global {
		globalValues = append(globalValues, *summary)
	}
	sort.Slice(globalValues, func(i, j int) bool { return globalValues[i].event < globalValues[j].event })
	for _, summary := range globalValues {
		row := map[string]interface{}{
			"EVENT_NAME": summary.event, "COUNT_STAR": summary.count, "SUM_TIMER_WAIT": summary.sum,
			"MIN_TIMER_WAIT": summary.min, "AVG_TIMER_WAIT": summary.sum / summary.count, "MAX_TIMER_WAIT": summary.max,
		}
		if !performanceSchemaLockValuesMatch(query, row) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, row))
	}
	return newInformationSchemaSelectResult("performance_schema.events_waits_summary_global_by_event_name", columns, rows)
}

type performanceSchemaTableLockWaitSummary struct {
	objectType   string
	objectSchema string
	objectName   string
	count        int64
	sum          int64
	min          int64
	max          int64
	readCount    int64
	readSum      int64
	readMin      int64
	readMax      int64
	writeCount   int64
	writeSum     int64
	writeMin     int64
	writeMax     int64
	miscCount    int64
	miscSum      int64
	miscMin      int64
	miscMax      int64
}

// executePerformanceSchemaTableLockWaitSummarySelect projects completed and
// currently observable table-lock waits into MySQL's table summary shape.
// Completed record-lock and owner-aware metadata-lock waits come from their
// bounded histories; canceled and legacy helper waits are not fabricated.
func (e *XMySQLExecutor) executePerformanceSchemaTableLockWaitSummarySelect(query string) *SelectResult {
	defaults := []string{
		"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT",
		"COUNT_READ", "SUM_TIMER_READ", "MIN_TIMER_READ", "AVG_TIMER_READ", "MAX_TIMER_READ",
		"COUNT_WRITE", "SUM_TIMER_WRITE", "MIN_TIMER_WRITE", "AVG_TIMER_WRITE", "MAX_TIMER_WRITE",
		"COUNT_MISC", "SUM_TIMER_MISC", "MIN_TIMER_MISC", "AVG_TIMER_MISC", "MAX_TIMER_MISC",
	}
	columns := requestedInformationSchemaColumns(query, defaults)
	byKey := make(map[string]*performanceSchemaTableLockWaitSummary)
	add := func(objectType, objectSchema, objectName string, wait time.Duration, mode manager.LockType, metadata bool) {
		if objectName == "" || !performanceSchemaSummaryFilterMatches(query, "OBJECT_TYPE", objectType) ||
			!performanceSchemaSummaryFilterMatches(query, "OBJECT_SCHEMA", objectSchema) ||
			!performanceSchemaSummaryFilterMatches(query, "OBJECT_NAME", objectName) {
			return
		}
		timer := wait.Nanoseconds() * 1000
		if timer < 0 {
			timer = 0
		}
		key := objectType + "\x00" + objectSchema + "\x00" + objectName
		summary := byKey[key]
		if summary == nil {
			summary = &performanceSchemaTableLockWaitSummary{
				objectType: objectType, objectSchema: objectSchema, objectName: objectName,
				min: timer, max: timer, readMin: timer, readMax: timer, writeMin: timer, writeMax: timer,
				miscMin: timer, miscMax: timer,
			}
			byKey[key] = summary
		}
		summary.count++
		summary.sum += timer
		if timer < summary.min {
			summary.min = timer
		}
		if timer > summary.max {
			summary.max = timer
		}
		if metadata {
			summary.miscCount++
			summary.miscSum += timer
			if timer < summary.miscMin {
				summary.miscMin = timer
			}
			if timer > summary.miscMax {
				summary.miscMax = timer
			}
			return
		}
		if mode == manager.LOCK_S {
			summary.readCount++
			summary.readSum += timer
			if timer < summary.readMin {
				summary.readMin = timer
			}
			if timer > summary.readMax {
				summary.readMax = timer
			}
			return
		}
		summary.writeCount++
		summary.writeSum += timer
		if timer < summary.writeMin {
			summary.writeMin = timer
		}
		if timer > summary.writeMax {
			summary.writeMax = timer
		}
	}
	if e != nil && e.lockManager != nil {
		for _, edge := range e.lockManager.WaitHistorySnapshot() {
			included, wait := e.performanceSchemaObservedWait("wait/lock/table/sql/handler", edge.WaitDuration)
			if included {
				add("TABLE", "", edge.ResourceID, wait, edge.LockType, false)
			}
		}
	}
	if e != nil {
		for _, edge := range e.getDDLCoordinator().MetadataLockWaitHistory() {
			included, wait := e.performanceSchemaObservedWait("wait/lock/metadata/sql/mdl", edge.WaitDuration)
			if included {
				schema, table := compatibilityQualifiedTable(edge.Table, "")
				add("TABLE", schema, table, wait, manager.LOCK_X, true)
			}
		}
	}
	for _, edge := range e.performanceSchemaWaitEdges() {
		included, wait := e.performanceSchemaObservedWait("wait/lock/table/sql/handler", edge.WaitDuration)
		if included {
			add("TABLE", "", edge.ResourceID, wait, edge.LockType, false)
		}
	}
	for _, edge := range e.performanceSchemaMetadataWaitEdges() {
		included, wait := e.performanceSchemaObservedWait("wait/lock/metadata/sql/mdl", edge.WaitDuration)
		if included {
			schema, table := compatibilityQualifiedTable(edge.Table, "")
			add("TABLE", schema, table, wait, manager.LOCK_X, true)
		}
	}

	values := make([]performanceSchemaTableLockWaitSummary, 0, len(byKey))
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
			"COUNT_STAR": summary.count, "SUM_TIMER_WAIT": summary.sum, "MIN_TIMER_WAIT": summary.min,
			"AVG_TIMER_WAIT": summary.sum / summary.count, "MAX_TIMER_WAIT": summary.max,
			"COUNT_READ": summary.readCount, "SUM_TIMER_READ": summary.readSum, "MIN_TIMER_READ": summary.readMin,
			"AVG_TIMER_READ": averagePerformanceSchemaTimer(summary.readSum, summary.readCount), "MAX_TIMER_READ": summary.readMax,
			"COUNT_WRITE": summary.writeCount, "SUM_TIMER_WRITE": summary.writeSum, "MIN_TIMER_WRITE": summary.writeMin,
			"AVG_TIMER_WRITE": averagePerformanceSchemaTimer(summary.writeSum, summary.writeCount), "MAX_TIMER_WRITE": summary.writeMax,
			"COUNT_MISC": summary.miscCount, "SUM_TIMER_MISC": summary.miscSum, "MIN_TIMER_MISC": summary.miscMin,
			"AVG_TIMER_MISC": averagePerformanceSchemaTimer(summary.miscSum, summary.miscCount), "MAX_TIMER_MISC": summary.miscMax,
		}
		if !performanceSchemaLockValuesMatch(query, row) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, row))
	}
	return newInformationSchemaSelectResult("performance_schema.table_lock_waits_summary_by_table", columns, rows)
}

func averagePerformanceSchemaTimer(sum, count int64) int64 {
	if count <= 0 {
		return 0
	}
	return sum / count
}

func (e *XMySQLExecutor) executePerformanceSchemaMemorySummarySelect(query string) *SelectResult {
	defaults := []string{"EVENT_NAME", "COUNT_ALLOC", "COUNT_FREE", "SUM_NUMBER_OF_BYTES_ALLOC", "SUM_NUMBER_OF_BYTES_FREE", "LOW_COUNT_USED", "CURRENT_COUNT_USED", "HIGH_COUNT_USED", "LOW_NUMBER_OF_BYTES_USED", "CURRENT_NUMBER_OF_BYTES_USED", "HIGH_NUMBER_OF_BYTES_USED"}
	columns := requestedInformationSchemaColumns(query, defaults)
	if e == nil || e.metricsRecorder == nil {
		values := map[string]interface{}{
			"EVENT_NAME": "memory/sql/THD::main_mem_root", "COUNT_ALLOC": int64(0), "COUNT_FREE": int64(0),
			"SUM_NUMBER_OF_BYTES_ALLOC": int64(0), "SUM_NUMBER_OF_BYTES_FREE": int64(0), "LOW_COUNT_USED": int64(0),
			"CURRENT_COUNT_USED": int64(0), "HIGH_COUNT_USED": int64(0), "LOW_NUMBER_OF_BYTES_USED": int64(0),
			"CURRENT_NUMBER_OF_BYTES_USED": int64(0), "HIGH_NUMBER_OF_BYTES_USED": int64(0),
		}
		return newInformationSchemaSelectResult("performance_schema.memory_summary_global_by_event_name", columns, [][]interface{}{projectInformationSchemaRow(columns, values)})
	}
	type aggregate struct{ alloc, free, bytesAlloc, bytesFree, lowCount, currentCount, highCount, lowBytes, currentBytes, highBytes int64 }
	byEvent := make(map[string]*aggregate)
	for _, summary := range e.metricsRecorder.MemorySummary() {
		if !performanceSchemaSummaryFilterMatches(query, "EVENT_NAME", summary.EventName) {
			continue
		}
		total := byEvent[summary.EventName]
		if total == nil {
			total = &aggregate{}
			byEvent[summary.EventName] = total
		}
		total.alloc += summary.CountAlloc
		total.free += summary.CountFree
		total.bytesAlloc += summary.BytesAlloc
		total.bytesFree += summary.BytesFree
		total.lowCount += summary.LowCountUsed
		total.currentCount += summary.CurrentCountUsed
		total.highCount += summary.HighCountUsed
		total.lowBytes += summary.LowBytesUsed
		total.currentBytes += summary.CurrentBytesUsed
		total.highBytes += summary.HighBytesUsed
	}
	if len(byEvent) == 0 {
		byEvent["memory/sql/THD::main_mem_root"] = &aggregate{}
	}
	eventNames := make([]string, 0, len(byEvent))
	for eventName := range byEvent {
		eventNames = append(eventNames, eventName)
	}
	sort.Strings(eventNames)
	rows := make([][]interface{}, 0, len(eventNames))
	for _, eventName := range eventNames {
		total := byEvent[eventName]
		values := map[string]interface{}{
			"EVENT_NAME": eventName, "COUNT_ALLOC": total.alloc, "COUNT_FREE": total.free,
			"SUM_NUMBER_OF_BYTES_ALLOC": total.bytesAlloc, "SUM_NUMBER_OF_BYTES_FREE": total.bytesFree,
			"LOW_COUNT_USED": total.lowCount, "CURRENT_COUNT_USED": total.currentCount, "HIGH_COUNT_USED": total.highCount,
			"LOW_NUMBER_OF_BYTES_USED": total.lowBytes, "CURRENT_NUMBER_OF_BYTES_USED": total.currentBytes, "HIGH_NUMBER_OF_BYTES_USED": total.highBytes,
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("performance_schema.memory_summary_global_by_event_name", columns, rows)
}

func performanceSchemaLockType(lockType manager.LockType) string {
	if lockType == manager.LOCK_S {
		return "RECORD"
	}
	return "RECORD"
}

func performanceSchemaLockMode(mode manager.LockMode) string {
	if mode == manager.LOCK_MODE_TABLE {
		return "EXCLUSIVE"
	}
	return "X"
}

func (e *XMySQLExecutor) executeShowProcesslist(ctx *ExecutionContext, session server.MySQLServerSession) {
	query := ""
	if ctx != nil {
		query = ctx.RawQuery
	}
	result := e.executeProcesslistSelect(session, query)
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: fmt.Sprintf("processlist returned %d rows", result.RowCount)}
}

func (e *XMySQLExecutor) executeProcesslistSelect(session server.MySQLServerSession, query string) *SelectResult {
	informationSchema := strings.Contains(strings.ToLower(query), "information_schema.processlist")
	informationSchemaColumns := []string{"ID", "USER", "HOST", "DB", "COMMAND", "TIME", "STATE", "INFO"}
	columns := []string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info", "Rows_sent", "Rows_examined"}
	if informationSchema {
		columns = requestedInformationSchemaColumns(query, informationSchemaColumns)
	}
	sessions := []server.MySQLServerSession{session}
	if e != nil && e.processlistProvider != nil {
		if provided := e.processlistProvider(); len(provided) > 0 {
			sessions = provided
		}
	}
	rows := make([][]interface{}, 0, len(sessions))
	canInspectAll := sessionHasProcessPrivilege(session)
	currentUser := ""
	if session != nil {
		currentUser, _ = session.GetParamByName("user").(string)
	}
	for _, listedSession := range sessions {
		if listedSession == nil {
			continue
		}
		listedUser, _ := listedSession.GetParamByName("user").(string)
		isCurrent := listedSession == session
		if !canInspectAll && !isCurrent && (currentUser == "" || !strings.EqualFold(currentUser, listedUser)) {
			continue
		}
		row := e.processlistRow(listedSession, query, isCurrent)
		if processlistRowMatchesQuery(row, query, int64(sessionConnectionID(session))) {
			if !informationSchema {
				rows = append(rows, row)
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, map[string]interface{}{
				"ID": row[0], "USER": row[1], "HOST": row[2], "DB": row[3], "COMMAND": row[4],
				"TIME": row[5], "STATE": row[6], "INFO": row[7], "TIME_MS": row[5].(int64) * 1000,
				"ROWS_SENT": row[8], "ROWS_EXAMINED": row[9],
			}))
		}
	}
	return newInformationSchemaSelectResult("processlist", columns, rows)
}

func processlistRowMatchesQuery(row []interface{}, query string, currentID ...int64) bool {
	lower := strings.ToLower(query)
	whereAt := strings.Index(lower, " where ")
	if whereAt < 0 {
		return true
	}
	where := strings.TrimSpace(query[whereAt+len(" where "):])
	whereLower := strings.ToLower(where)
	for _, suffix := range []string{" order by ", " limit "} {
		if at := strings.Index(whereLower, suffix); at >= 0 {
			where = strings.TrimSpace(where[:at])
			whereLower = strings.ToLower(where)
		}
	}
	groups := splitTopLevelBoolean(where, " or ")
	if len(groups) == 0 {
		return true
	}
	indexes := map[string]int{"id": 0, "user": 1, "host": 2, "db": 3, "command": 4, "state": 6, "info": 7}
	for _, group := range groups {
		conditions := splitTopLevelBoolean(group, " and ")
		if len(conditions) == 0 {
			continue
		}
		groupMatches := true
		for _, condition := range conditions {
			if !processlistConditionMatches(row, indexes, condition, currentID...) {
				groupMatches = false
				break
			}
		}
		if groupMatches {
			return true
		}
	}
	return false
}

func processlistConditionMatches(row []interface{}, indexes map[string]int, condition string, currentID ...int64) bool {
	condition = strings.TrimSpace(condition)
	if functionMatch := processlistFunctionFilterPattern.FindStringSubmatch(condition); len(functionMatch) > 0 {
		if len(currentID) == 0 {
			return true
		}
		actual := fmt.Sprint(row[indexes[strings.ToLower(functionMatch[1])]])
		want := strconv.FormatInt(currentID[0], 10)
		if strings.EqualFold(functionMatch[2], "=") {
			return actual == want
		}
		return actual != want
	}
	nullMatch := processlistNullFilterPattern.FindStringSubmatch(condition)
	if len(nullMatch) > 0 {
		actual := row[indexes[strings.ToLower(nullMatch[1])]]
		isNull := actual == nil
		if strings.TrimSpace(nullMatch[2]) != "" {
			return !isNull
		}
		return isNull
	}
	if inMatch := processlistInFilterPattern.FindStringSubmatch(condition); len(inMatch) > 0 {
		actual := fmt.Sprint(row[indexes[strings.ToLower(inMatch[1])]])
		if actual == "<nil>" {
			actual = ""
		}
		items := splitTopLevelComma(inMatch[3])
		contains := false
		for _, item := range items {
			value, ok := processlistFilterLiteral(item)
			if ok && strings.EqualFold(actual, value) {
				contains = true
				break
			}
		}
		if strings.TrimSpace(inMatch[2]) != "" {
			return !contains
		}
		return contains
	}
	match := processlistFilterPattern.FindStringSubmatch(condition)
	if len(match) == 0 {
		// Unknown predicates are deliberately retained rather than guessed; a
		// false negative here would make administrative inspection misleading.
		return true
	}
	value := match[3]
	if value == "" {
		value = match[4]
	}
	if value == "" {
		value = match[5]
	}
	value = strings.ReplaceAll(value, "''", "'")
	value = strings.ReplaceAll(value, `""`, `"`)
	if value == "<nil>" {
		value = ""
	}
	actual := fmt.Sprint(row[indexes[strings.ToLower(match[1])]])
	if actual == "<nil>" {
		actual = ""
	}
	operator := strings.ToLower(strings.TrimSpace(match[2]))
	switch operator {
	case "like", "not like":
		matched := metadataFilterMatches(actual, value)
		if operator == "not like" {
			return !matched
		}
		return matched
	case "!=", "<>":
		return !strings.EqualFold(actual, value)
	default:
		return strings.EqualFold(actual, value)
	}
}

func processlistFilterLiteral(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) >= 2 && raw[0] == '\'' && raw[len(raw)-1] == '\'' {
		return strings.ReplaceAll(raw[1:len(raw)-1], "''", "'"), true
	}
	if len(raw) >= 2 && raw[0] == '"' && raw[len(raw)-1] == '"' {
		return strings.ReplaceAll(raw[1:len(raw)-1], `""`, `"`), true
	}
	if _, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return raw, true
	}
	return "", false
}

func sessionHasProcessPrivilege(session server.MySQLServerSession) bool {
	if session == nil {
		return false
	}
	privileges, ok := session.GetParamByName("global_privileges").([]common.PrivilegeType)
	if !ok {
		return false
	}
	for _, privilege := range privileges {
		if privilege == common.ProcessPriv || privilege == common.SuperPriv || privilege == common.AllPriv {
			return true
		}
	}
	return false
}

func (e *XMySQLExecutor) processlistRow(session server.MySQLServerSession, query string, isCurrent bool) []interface{} {
	threadID := int64(sessionConnectionID(session))
	user, host, database, command, state, info := "", "", "", "Sleep", "", ""
	processTime, rowsSent, rowsExamined := int64(0), int64(0), int64(0)
	if session != nil {
		if value, ok := session.GetParamByName("user").(string); ok {
			user = value
		}
		if value, ok := session.GetParamByName("host").(string); ok {
			host = value
		}
		if value, ok := session.GetParamByName("database").(string); ok {
			database = value
		}
		if inTxn, ok := session.GetParamByName("in_transaction").(bool); ok && inTxn {
			state = "In transaction"
		}
		if value, ok := session.GetParamByName("processlist_query").(string); ok {
			info = value
		}
		if started, ok := session.GetParamByName("processlist_start_time").(time.Time); ok && !started.IsZero() {
			if elapsed := int64(time.Since(started).Seconds()); elapsed > 0 {
				processTime = elapsed
			}
		}
		rowsSent = processlistCounter(session.GetParamByName("processlist_rows_sent"))
		rowsExamined = processlistCounter(session.GetParamByName("processlist_rows_examined"))
	}
	if isCurrent {
		info = query
	}
	if strings.TrimSpace(info) != "" {
		command = "Query"
	}
	if e != nil && e.lockManager != nil {
		transactionID := threadID
		if session != nil {
			if value, ok := session.GetParamByName("transaction_id").(int64); ok {
				transactionID = value
			}
		}
		for _, edge := range e.lockManager.WaitGraphSnapshot() {
			if int64(edge.WaitingTxID) == transactionID {
				state = "Waiting for lock"
				break
			}
		}
	}
	return []interface{}{threadID, user, host, database, command, processTime, state, info, rowsSent, rowsExamined}
}

func processlistCounter(value interface{}) int64 {
	switch number := value.(type) {
	case int:
		return int64(number)
	case int64:
		return number
	case uint:
		return int64(number)
	case uint64:
		return int64(number)
	case uint32:
		return int64(number)
	case string:
		parsed, _ := strconv.ParseInt(number, 10, 64)
		return parsed
	default:
		return 0
	}
}

func (e *XMySQLExecutor) executeInformationSchemaPartitionsSelect(query string) *SelectResult {
	defaults := []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "PARTITION_NAME", "SUBPARTITION_NAME", "PARTITION_ORDINAL_POSITION", "SUBPARTITION_ORDINAL_POSITION", "PARTITION_METHOD", "SUBPARTITION_METHOD", "PARTITION_EXPRESSION", "SUBPARTITION_EXPRESSION", "PARTITION_DESCRIPTION", "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE", "CREATE_TIME", "UPDATE_TIME", "CHECK_TIME", "CHECKSUM", "PARTITION_COMMENT", "NODEGROUP", "TABLESPACE_NAME"}
	columns := requestedInformationSchemaColumns(query, defaults)
	filters := informationSchemaMetadataFilters(query)
	rows := make([][]interface{}, 0)
	for _, table := range e.scanFrmTables() {
		if !metadataPatternMatches(table.schemaName, filters["table_schema"]) || !metadataPatternMatches(table.tableName, filters["table_name"]) {
			continue
		}
		values := map[string]interface{}{
			"TABLE_CATALOG": "def", "TABLE_SCHEMA": table.schemaName, "TABLE_NAME": table.tableName,
			"PARTITION_NAME": nil, "SUBPARTITION_NAME": nil, "PARTITION_ORDINAL_POSITION": nil, "SUBPARTITION_ORDINAL_POSITION": nil,
			"PARTITION_METHOD": nil, "SUBPARTITION_METHOD": nil, "PARTITION_EXPRESSION": nil, "SUBPARTITION_EXPRESSION": nil,
			"PARTITION_DESCRIPTION": nil, "TABLE_ROWS": int64(0), "AVG_ROW_LENGTH": int64(0), "DATA_LENGTH": int64(0),
			"MAX_DATA_LENGTH": nil, "INDEX_LENGTH": int64(0), "DATA_FREE": int64(0), "CREATE_TIME": nil, "UPDATE_TIME": nil,
			"CHECK_TIME": nil, "CHECKSUM": nil, "PARTITION_COMMENT": "", "NODEGROUP": nil, "TABLESPACE_NAME": nil,
		}
		partitionRows := e.partitionRowsForTable(table.schemaName, table.tableName)
		if len(partitionRows) == 0 {
			if performanceSchemaLockValuesMatch(query, values) {
				rows = append(rows, projectInformationSchemaRow(columns, values))
			}
			continue
		}
		for _, partition := range partitionRows {
			rowValues := make(map[string]interface{}, len(values)+len(partition))
			for key, value := range values {
				rowValues[key] = value
			}
			for key, value := range partition {
				rowValues[key] = value
			}
			if performanceSchemaLockValuesMatch(query, rowValues) {
				rows = append(rows, projectInformationSchemaRow(columns, rowValues))
			}
		}
	}
	return newInformationSchemaSelectResult("information_schema_partitions", columns, rows)
}

func isInformationSchemaMetadataQuery(query string) bool {
	lower := strings.ToLower(strings.TrimSpace(query))
	lower = strings.ReplaceAll(lower, "`", "")
	lower = regexp.MustCompile(`\s*\.\s*`).ReplaceAllString(lower, ".")
	for _, tableName := range informationSchemaMetadataTableNames() {
		if strings.Contains(lower, "information_schema."+tableName) {
			return true
		}
	}
	return false
}

func isPerformanceSchemaMetadataQuery(query string) bool {
	lower := strings.ToLower(strings.TrimSpace(query))
	lower = strings.ReplaceAll(lower, "`", "")
	return strings.Contains(lower, "performance_schema.")
}

func isPerformanceSchemaRegistryQuery(query string) bool {
	name := performanceSchemaTableName(query)
	if name == "" {
		return false
	}
	_, registered := performanceSchemaTableRegistry[name]
	return registered
}

func isMySQLMetadataQuery(query string) bool {
	lower := strings.ToLower(strings.TrimSpace(query))
	lower = strings.ReplaceAll(lower, "`", "")
	lower = regexp.MustCompile(`\s*\.\s*`).ReplaceAllString(lower, ".")
	for _, tableName := range []string{"user", "procs_priv", "proxies_priv", "db", "tables_priv", "columns_priv", "global_grants"} {
		if strings.Contains(lower, "mysql."+tableName) {
			return true
		}
	}
	return false
}

func informationSchemaMetadataTableNames() []string {
	names := []string{
		"tables",
		"columns",
		"schemata",
		"routines",
		"parameters",
		"statistics",
		"key_column_usage",
		"table_constraints",
		"check_constraints",
		"referential_constraints",
		"column_privileges",
		"table_privileges",
		"views",
		"partitions",
		"triggers",
		"events",
		"collations",
		"character_sets",
		"engines",
		"plugins",
		"column_statistics",
		"tablespaces",
		"files",
		"processlist",
		"user_privileges",
		"schema_privileges",
		"enabled_roles",
		"applicable_roles",
		"administrable_role_authorizations",
		"role_table_grants",
		"role_column_grants",
		"role_routine_grants",
		"innodb_tablespaces",
		"innodb_datafiles",
		"innodb_tablestats",
		"innodb_indexes",
		"innodb_columns",
		"innodb_metrics",
		"innodb_foreign",
		"innodb_foreign_cols",
		"innodb_trx",
		"innodb_lock_waits",
		"innodb_locks",
		"system_variables",
		"global_variables",
		"session_variables",
	}
	for name := range informationSchemaTableRegistry {
		names = append(names, name)
	}
	return names
}

// executeInformationSchemaEnabledRolesSelect projects the roles currently
// active on this connection. MySQL exposes one row per active role in
// INFORMATION_SCHEMA.ENABLED_ROLES.
func (e *XMySQLExecutor) executeInformationSchemaEnabledRolesSelect(query string, session server.MySQLServerSession) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{"ROLE_NAME", "ROLE_HOST", "IS_DEFAULT", "IS_MANDATORY"})
	if session == nil {
		return newInformationSchemaSelectResult("information_schema.enabled_roles", columns, nil)
	}
	roles, ok := session.GetParamByName("active_roles").([]string)
	if !ok || len(roles) == 0 {
		return newInformationSchemaSelectResult("information_schema.enabled_roles", columns, nil)
	}
	file, _ := e.accountFileForSession(&ExecutionContext{Session: session})
	account := sessionAccount(file, session)
	mandatoryRoles := e.mandatoryRoles()
	rows := make([][]interface{}, 0, len(roles))
	for _, role := range roles {
		roleName, roleHost := role, "%"
		if at := strings.LastIndex(role, "@"); at > 0 && at < len(role)-1 {
			roleName, roleHost = role[:at], role[at+1:]
		}
		isDefault := "NO"
		if account != nil && containsRole(account.DefaultRoles, role) {
			isDefault = "YES"
		}
		isMandatory := "NO"
		if containsConfiguredRole(mandatoryRoles, role) {
			isMandatory = "YES"
		}
		values := map[string]interface{}{"ROLE_NAME": roleName, "ROLE_HOST": roleHost, "IS_DEFAULT": isDefault, "IS_MANDATORY": isMandatory}
		if !informationSchemaRoleQueryMatches(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("information_schema.enabled_roles", columns, rows)
}

var informationSchemaRoleFilterPattern = regexp.MustCompile(`(?i)\b(grantor|grantor_host|grantee|grantee_host|role_name|role_host|default_role|is_default|is_mandatory|is_grantable|table_catalog|table_schema|table_name|column_name|privilege_type|specific_catalog|specific_schema|specific_name|routine_catalog|routine_schema|routine_name)\b\s*(?:=|like)\s*(?:'([^']*)'|"([^"]*)")`)

func informationSchemaRoleQueryMatches(query string, values map[string]interface{}) bool {
	for _, match := range informationSchemaRoleFilterPattern.FindAllStringSubmatch(query, -1) {
		field := strings.ToUpper(match[1])
		pattern := match[2]
		if pattern == "" {
			pattern = match[3]
		}
		value := fmt.Sprint(values[field])
		if !metadataPatternMatches(value, pattern) {
			return false
		}
	}
	return true
}

// executeInformationSchemaApplicableRolesSelect exposes the direct role
// grants for the authenticated account. Role inheritance beyond the direct
// grant edge is intentionally kept bounded to the role graph used by the
// privilege checker.
func (e *XMySQLExecutor) executeInformationSchemaApplicableRolesSelect(query string, session server.MySQLServerSession) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{"USER", "HOST", "GRANTEE", "GRANTEE_HOST", "ROLE_NAME", "ROLE_HOST", "IS_GRANTABLE", "IS_DEFAULT", "IS_MANDATORY"})
	if e == nil || session == nil {
		return newInformationSchemaSelectResult("information_schema.applicable_roles", columns, nil)
	}
	user, _ := session.GetParamByName("user").(string)
	host, _ := session.GetParamByName("host").(string)
	if user == "" {
		return newInformationSchemaSelectResult("information_schema.applicable_roles", columns, nil)
	}
	file, err := e.accountFileForSession(&ExecutionContext{Session: session})
	if err != nil {
		return newInformationSchemaSelectResult("information_schema.applicable_roles", columns, nil)
	}
	var account *persistedAccount
	for index := range file.Accounts {
		candidate := &file.Accounts[index]
		if strings.EqualFold(candidate.User, user) && (host == "" || strings.EqualFold(candidate.Host, host)) {
			account = candidate
			break
		}
	}
	if account == nil {
		return newInformationSchemaSelectResult("information_schema.applicable_roles", columns, nil)
	}
	grantee := fmt.Sprintf("'%s'@'%s'", escapeAccountSQL(account.User), escapeAccountSQL(account.Host))
	roleAccounts := collectRoleAccounts(file, e.allGrantedRoles(file, *account))
	rows := make([][]interface{}, 0, len(roleAccounts))
	for _, roleAccount := range roleAccounts {
		role := roleAccount.User + "@" + roleAccount.Host
		roleName, roleHost := roleAccount.User, roleAccount.Host
		if roleHost == "" {
			roleHost = "%"
		}
		grantable := "NO"
		if containsRole(account.RoleAdminOptions, role) {
			grantable = "YES"
		}
		defaultRole := "NO"
		if containsRole(account.DefaultRoles, role) {
			defaultRole = "YES"
		}
		mandatoryRole := "NO"
		if e.isMandatoryRole(roleName, roleHost) {
			mandatoryRole = "YES"
		}
		values := map[string]interface{}{
			"USER": account.User, "HOST": account.Host, "GRANTEE": grantee, "GRANTEE_HOST": account.Host,
			"ROLE_NAME": roleName, "ROLE_HOST": roleHost, "IS_GRANTABLE": grantable,
			"IS_DEFAULT": defaultRole, "IS_MANDATORY": mandatoryRole, "DEFAULT_ROLE": defaultRole,
		}
		if informationSchemaRoleQueryMatches(query, values) {
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("information_schema.applicable_roles", columns, rows)
}

// executeInformationSchemaAdministrableRoleAuthorizationsSelect exposes the
// subset of applicable roles for which the current account has WITH ADMIN
// OPTION. The role graph is bounded by collectRoleAccounts, consistent with
// the privilege checker and the other role metadata projections.
func (e *XMySQLExecutor) executeInformationSchemaAdministrableRoleAuthorizationsSelect(query string, session server.MySQLServerSession) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{"USER", "HOST", "GRANTEE", "GRANTEE_HOST", "ROLE_NAME", "ROLE_HOST", "IS_GRANTABLE", "IS_DEFAULT", "IS_MANDATORY"})
	if e == nil || session == nil {
		return newInformationSchemaSelectResult("information_schema.administrable_role_authorizations", columns, nil)
	}
	user, _ := session.GetParamByName("user").(string)
	host, _ := session.GetParamByName("host").(string)
	if user == "" {
		return newInformationSchemaSelectResult("information_schema.administrable_role_authorizations", columns, nil)
	}
	file, err := e.accountFileForSession(&ExecutionContext{Session: session})
	if err != nil {
		return newInformationSchemaSelectResult("information_schema.administrable_role_authorizations", columns, nil)
	}
	var account *persistedAccount
	for index := range file.Accounts {
		candidate := &file.Accounts[index]
		if strings.EqualFold(candidate.User, user) && (host == "" || strings.EqualFold(candidate.Host, host)) {
			account = candidate
			break
		}
	}
	if account == nil {
		return newInformationSchemaSelectResult("information_schema.administrable_role_authorizations", columns, nil)
	}
	grantee := fmt.Sprintf("'%s'@'%s'", escapeAccountSQL(account.User), escapeAccountSQL(account.Host))
	rows := make([][]interface{}, 0)
	for _, roleAccount := range collectRoleAccounts(file, e.allGrantedRoles(file, *account)) {
		role := roleAccount.User + "@" + roleAccount.Host
		if !containsRole(account.RoleAdminOptions, role) {
			continue
		}
		values := map[string]interface{}{
			"USER": account.User, "HOST": account.Host, "GRANTEE": grantee, "GRANTEE_HOST": account.Host,
			"ROLE_NAME": roleAccount.User, "ROLE_HOST": roleAccount.Host, "IS_GRANTABLE": "YES",
			"IS_DEFAULT": func() string {
				if containsRole(account.DefaultRoles, role) {
					return "YES"
				}
				return "NO"
			}(),
			"IS_MANDATORY": func() string {
				if e.isMandatoryRole(roleAccount.User, roleAccount.Host) {
					return "YES"
				}
				return "NO"
			}(),
		}
		if informationSchemaRoleQueryMatches(query, values) {
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		for column := range rows[i] {
			left, right := fmt.Sprint(rows[i][column]), fmt.Sprint(rows[j][column])
			if left == right {
				continue
			}
			return left < right
		}
		return false
	})
	return newInformationSchemaSelectResult("information_schema.administrable_role_authorizations", columns, rows)
}

func containsRole(roles []string, target string) bool {
	for _, role := range roles {
		if strings.EqualFold(role, target) {
			return true
		}
	}
	return false
}

func collectRoleAccounts(file persistedAccountFile, roles []string) []persistedAccount {
	accountsByName := make(map[string]persistedAccount, len(file.Accounts))
	for _, account := range file.Accounts {
		accountsByName[strings.ToLower(account.User+"@"+account.Host)] = account
	}
	result := make([]persistedAccount, 0, len(roles))
	visited := make(map[string]bool)
	var visit func(string, int)
	visit = func(role string, depth int) {
		if depth > 16 {
			return
		}
		role = strings.TrimSpace(role)
		if role == "" {
			return
		}
		key := strings.ToLower(role)
		if visited[key] {
			return
		}
		visited[key] = true
		parts := strings.SplitN(role, "@", 2)
		var account persistedAccount
		var ok bool
		if len(parts) == 2 {
			account, ok = accountsByName[strings.ToLower(parts[0]+"@"+parts[1])]
		} else {
			for accountKey, candidate := range accountsByName {
				if strings.HasPrefix(accountKey, strings.ToLower(parts[0])+"@") {
					account, ok = candidate, true
					break
				}
			}
		}
		if !ok {
			return
		}
		result = append(result, account)
		for _, nested := range account.Roles {
			visit(nested, depth+1)
		}
	}
	for _, role := range roles {
		visit(role, 1)
	}
	return result
}

func enabledRoleReferences(account persistedAccount, session server.MySQLServerSession) []string {
	if session != nil {
		if active, ok := session.GetParamByName("active_roles").([]string); ok {
			return active
		}
	}
	return account.Roles
}

func (e *XMySQLExecutor) executeInformationSchemaRoleTableGrantsSelect(query string, session server.MySQLServerSession) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{"GRANTOR", "GRANTOR_HOST", "GRANTEE", "GRANTEE_HOST", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE"})
	if e == nil || session == nil {
		return newInformationSchemaSelectResult("information_schema.role_table_grants", columns, nil)
	}
	user, _ := session.GetParamByName("user").(string)
	host, _ := session.GetParamByName("host").(string)
	if user == "" {
		return newInformationSchemaSelectResult("information_schema.role_table_grants", columns, nil)
	}
	file, err := e.accountFileForSession(&ExecutionContext{Session: session})
	if err != nil {
		return newInformationSchemaSelectResult("information_schema.role_table_grants", columns, nil)
	}
	var account *persistedAccount
	for index := range file.Accounts {
		candidate := &file.Accounts[index]
		if strings.EqualFold(candidate.User, user) && (host == "" || strings.EqualFold(candidate.Host, host)) {
			account = candidate
			break
		}
	}
	if account == nil {
		return newInformationSchemaSelectResult("information_schema.role_table_grants", columns, nil)
	}
	rows := make([][]interface{}, 0)
	for _, roleAccount := range collectRoleAccounts(file, enabledRoleReferences(*account, session)) {
		grantee := fmt.Sprintf("'%s'@'%s'", escapeAccountSQL(roleAccount.User), escapeAccountSQL(roleAccount.Host))
		for scope, privileges := range roleAccount.Grants {
			parts := strings.Split(scope, ".")
			if len(parts) != 2 || parts[0] == "*" || parts[1] == "*" {
				continue
			}
			grantable := "NO"
			if hasGrantOption(privileges) {
				grantable = "YES"
			}
			for _, privilege := range displayGrantPrivileges(privileges) {
				values := map[string]interface{}{
					"GRANTOR": "root", "GRANTOR_HOST": "localhost", "GRANTEE": grantee, "GRANTEE_HOST": roleAccount.Host,
					"TABLE_CATALOG": "def", "TABLE_SCHEMA": parts[0], "TABLE_NAME": parts[1],
					"PRIVILEGE_TYPE": privilege, "IS_GRANTABLE": grantable,
				}
				if informationSchemaRoleQueryMatches(query, values) {
					rows = append(rows, projectInformationSchemaRow(columns, values))
				}
			}
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		for column := range rows[i] {
			left, right := fmt.Sprint(rows[i][column]), fmt.Sprint(rows[j][column])
			if left == right {
				continue
			}
			return left < right
		}
		return false
	})
	return newInformationSchemaSelectResult("information_schema.role_table_grants", columns, rows)
}

func (e *XMySQLExecutor) executeInformationSchemaRoleColumnGrantsSelect(query string, session server.MySQLServerSession) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{"GRANTOR", "GRANTOR_HOST", "GRANTEE", "GRANTEE_HOST", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE"})
	if e == nil || session == nil {
		return newInformationSchemaSelectResult("information_schema.role_column_grants", columns, nil)
	}
	user, _ := session.GetParamByName("user").(string)
	host, _ := session.GetParamByName("host").(string)
	if user == "" {
		return newInformationSchemaSelectResult("information_schema.role_column_grants", columns, nil)
	}
	file, err := e.accountFileForSession(&ExecutionContext{Session: session})
	if err != nil {
		return newInformationSchemaSelectResult("information_schema.role_column_grants", columns, nil)
	}
	var account *persistedAccount
	for index := range file.Accounts {
		candidate := &file.Accounts[index]
		if strings.EqualFold(candidate.User, user) && (host == "" || strings.EqualFold(candidate.Host, host)) {
			account = candidate
			break
		}
	}
	if account == nil {
		return newInformationSchemaSelectResult("information_schema.role_column_grants", columns, nil)
	}
	rows := make([][]interface{}, 0)
	for _, roleAccount := range collectRoleAccounts(file, enabledRoleReferences(*account, session)) {
		grantee := fmt.Sprintf("'%s'@'%s'", escapeAccountSQL(roleAccount.User), escapeAccountSQL(roleAccount.Host))
		for scope, privileges := range roleAccount.ColumnGrants {
			parts := strings.Split(scope, ".")
			if len(parts) != 3 {
				continue
			}
			grantable := "NO"
			if hasGrantOption(privileges) {
				grantable = "YES"
			}
			for _, privilege := range displayGrantPrivileges(privileges) {
				values := map[string]interface{}{
					"GRANTOR": "root", "GRANTOR_HOST": "localhost", "GRANTEE": grantee, "GRANTEE_HOST": roleAccount.Host,
					"TABLE_CATALOG": "def", "TABLE_SCHEMA": parts[0], "TABLE_NAME": parts[1], "COLUMN_NAME": parts[2],
					"PRIVILEGE_TYPE": privilege, "IS_GRANTABLE": grantable,
				}
				if informationSchemaRoleQueryMatches(query, values) {
					rows = append(rows, projectInformationSchemaRow(columns, values))
				}
			}
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		for column := range rows[i] {
			left, right := fmt.Sprint(rows[i][column]), fmt.Sprint(rows[j][column])
			if left == right {
				continue
			}
			return left < right
		}
		return false
	})
	return newInformationSchemaSelectResult("information_schema.role_column_grants", columns, rows)
}

// executeInformationSchemaRoleRoutineGrantsSelect exposes the routine-level
// privileges held by roles available to the authenticated account. The
// persisted grant file does not retain the original grantor edge, so the
// compatibility projection uses the server bootstrap account as GRANTOR and
// keeps the role account as GRANTEE, matching the useful routine identity and
// privilege fields exposed by MySQL.
func (e *XMySQLExecutor) executeInformationSchemaRoleRoutineGrantsSelect(session server.MySQLServerSession, query string) *SelectResult {
	allColumns := []string{
		"GRANTOR", "GRANTOR_HOST", "GRANTEE", "GRANTEE_HOST", "SPECIFIC_CATALOG",
		"SPECIFIC_SCHEMA", "SPECIFIC_NAME", "ROUTINE_CATALOG", "ROUTINE_SCHEMA",
		"ROUTINE_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE",
	}
	columns := requestedInformationSchemaColumns(query, allColumns)
	if e == nil || session == nil {
		return newInformationSchemaSelectResult("information_schema.role_routine_grants", columns, nil)
	}
	user, _ := session.GetParamByName("user").(string)
	host, _ := session.GetParamByName("host").(string)
	if user == "" {
		return newInformationSchemaSelectResult("information_schema.role_routine_grants", columns, nil)
	}
	file, err := e.accountFileForSession(&ExecutionContext{Session: session})
	if err != nil {
		return newInformationSchemaSelectResult("information_schema.role_routine_grants", columns, nil)
	}
	var account *persistedAccount
	for index := range file.Accounts {
		candidate := &file.Accounts[index]
		if strings.EqualFold(candidate.User, user) && (host == "" || strings.EqualFold(candidate.Host, host)) {
			account = candidate
			break
		}
	}
	if account == nil {
		return newInformationSchemaSelectResult("information_schema.role_routine_grants", columns, nil)
	}
	routines := append(e.scanStoredObjects("procedure"), e.scanStoredObjects("function")...)
	rows := make([][]interface{}, 0)
	for _, roleAccount := range collectRoleAccounts(file, enabledRoleReferences(*account, session)) {
		grantee := roleAccount.User
		granteeHost := roleAccount.Host
		for _, routine := range routines {
			privileges := routinePrivilegesForRole(roleAccount, routine.Schema, routine.Name)
			for _, privilege := range privileges {
				values := map[string]interface{}{
					"GRANTOR": "root", "GRANTOR_HOST": "localhost",
					"GRANTEE": grantee, "GRANTEE_HOST": granteeHost,
					"SPECIFIC_CATALOG": "def", "SPECIFIC_SCHEMA": routine.Schema, "SPECIFIC_NAME": routine.Name,
					"ROUTINE_CATALOG": "def", "ROUTINE_SCHEMA": routine.Schema, "ROUTINE_NAME": routine.Name,
					"PRIVILEGE_TYPE": privilege, "IS_GRANTABLE": boolToYesNo(routineGrantOptionForRole(roleAccount, routine.Schema, routine.Name)),
				}
				if informationSchemaRoleQueryMatches(query, values) {
					rows = append(rows, projectInformationSchemaRow(columns, values))
				}
			}
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		for column := range rows[i] {
			left, right := fmt.Sprint(rows[i][column]), fmt.Sprint(rows[j][column])
			if left == right {
				continue
			}
			return left < right
		}
		return false
	})
	return newInformationSchemaSelectResult("information_schema.role_routine_grants", columns, rows)
}

func routinePrivilegesForRole(role persistedAccount, schema, name string) []string {
	privileges := make([]string, 0)
	for scope, granted := range role.Grants {
		if !scopeCovers(schema+"."+name, scope) {
			continue
		}
		for _, privilege := range displayGrantPrivileges(granted) {
			normalized := strings.ToUpper(strings.TrimSpace(privilege))
			switch normalized {
			case "EXECUTE", "ALTER ROUTINE", "ALL", "ALL PRIVILEGES":
				privileges = appendUniqueStrings(privileges, normalized)
			}
		}
	}
	sort.Strings(privileges)
	return privileges
}

func routineGrantOptionForRole(role persistedAccount, schema, name string) bool {
	requested := schema + "." + name
	for scope, privileges := range role.Grants {
		if scopeCovers(requested, scope) && hasGrantOption(privileges) {
			return true
		}
	}
	return false
}

func (e *XMySQLExecutor) executeInformationSchemaTablesSelect(query string, session server.MySQLServerSession) *SelectResult {
	filters := informationSchemaMetadataFilters(query)
	schemaPattern := filters["table_schema"]
	tablePattern := filters["table_name"]
	columns := informationSchemaTablesRequestedColumns(query)

	rows := make([][]interface{}, 0)
	for _, table := range e.scanFrmTables() {
		visibleTableName, visible := temporaryMetadataTableName(session, table.schemaName, table.tableName)
		if !visible || !e.informationSchemaTableVisible(session, table.schemaName, table.tableName, false) || !informationSchemaMetadataQueryMatches(query, "table_schema", table.schemaName, schemaPattern) || !informationSchemaMetadataQueryMatches(query, "table_name", visibleTableName, tablePattern) {
			continue
		}
		rowCount, avgRowLength, dataLength, indexLength := e.informationSchemaTableSizes(table.schemaName, table.tableName)
		engineName, tableCollation, createOptions := persistedTableDisplayOptions(e, table.schemaName, table.tableName)
		rowFormat := persistedTableRowFormat(e, table.schemaName, table.tableName)
		autoIncrement := persistedAutoIncrementValue(e.getDataDir(), table.schemaName, table.tableName, table.columns)
		values := map[string]interface{}{
			"TABLE_CATALOG": "def", "TABLE_SCHEMA": table.schemaName, "TABLE_NAME": visibleTableName,
			"TABLE_TYPE": "TABLE", "ENGINE": engineName, "VERSION": int64(10),
			"ROW_FORMAT": rowFormat, "TABLE_ROWS": rowCount, "AVG_ROW_LENGTH": avgRowLength,
			"DATA_LENGTH": dataLength, "MAX_DATA_LENGTH": nil, "INDEX_LENGTH": indexLength,
			"DATA_FREE": int64(0), "AUTO_INCREMENT": autoIncrement, "CREATE_TIME": nil, "UPDATE_TIME": nil,
			"CHECK_TIME": nil, "TABLE_COLLATION": tableCollation, "CHECKSUM": nil,
			"CREATE_OPTIONS": createOptions, "TABLE_COMMENT": persistedTableComment(e, table.schemaName, table.tableName),
			// JDBC's historical aliases are kept as first-class projections.
			"TABLE_CAT": table.schemaName, "TABLE_SCHEM": nil, "REMARKS": "",
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	// INFORMATION_SCHEMA and PERFORMANCE_SCHEMA are virtual schemas.  Their
	// tables do not necessarily have user-table .frm metadata, but MySQL
	// exposes them through INFORMATION_SCHEMA.TABLES and clients use that
	// discovery path before querying a diagnostic table directly.
	persistedTables := make(map[string]struct{})
	for _, table := range e.scanFrmTables() {
		persistedTables[strings.ToLower(table.schemaName+"."+table.tableName)] = struct{}{}
	}
	virtualNames := make([]string, 0)
	for qualified := range informationSchemaVirtualTableDefinitions() {
		virtualNames = append(virtualNames, qualified)
	}
	sort.Strings(virtualNames)
	for _, qualified := range virtualNames {
		parts := strings.SplitN(qualified, ".", 2)
		if len(parts) != 2 || !informationSchemaMetadataQueryMatches(query, "table_schema", parts[0], schemaPattern) || !informationSchemaMetadataQueryMatches(query, "table_name", parts[1], tablePattern) {
			continue
		}
		// Virtual catalog rows have NULL AUTO_INCREMENT.  Honor the common
		// predicate used by JDBC metadata probes instead of returning a row
		// merely because the virtual table itself was discoverable.
		if strings.Contains(strings.ToLower(query), "auto_increment is not null") {
			continue
		}
		if _, exists := persistedTables[strings.ToLower(qualified)]; exists {
			continue
		}
		values := map[string]interface{}{
			"TABLE_CATALOG": "def", "TABLE_SCHEMA": parts[0], "TABLE_NAME": parts[1],
			"TABLE_TYPE": "SYSTEM VIEW", "ENGINE": nil, "VERSION": nil, "ROW_FORMAT": nil,
			"TABLE_ROWS": int64(0), "AVG_ROW_LENGTH": int64(0), "DATA_LENGTH": int64(0),
			"MAX_DATA_LENGTH": nil, "INDEX_LENGTH": int64(0), "DATA_FREE": int64(0), "AUTO_INCREMENT": nil,
			"CREATE_TIME": nil, "UPDATE_TIME": nil, "CHECK_TIME": nil, "TABLE_COLLATION": "utf8mb4_general_ci",
			"CHECKSUM": nil, "CREATE_OPTIONS": "", "TABLE_COMMENT": nil,
			"TABLE_CAT": parts[0], "TABLE_SCHEM": nil, "REMARKS": "",
		}
		if performanceSchemaLockValuesMatch(query, values) {
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	schemaEntries, _ := os.ReadDir(e.getDataDir())
	for _, schemaEntry := range schemaEntries {
		if !schemaEntry.IsDir() || !metadataPatternMatches(schemaEntry.Name(), schemaPattern) {
			continue
		}
		viewEntries, _ := os.ReadDir(filepath.Join(e.getDataDir(), schemaEntry.Name()))
		for _, viewEntry := range viewEntries {
			if viewEntry.IsDir() || !strings.HasSuffix(strings.ToLower(viewEntry.Name()), ".view.json") {
				continue
			}
			viewName := strings.TrimSuffix(viewEntry.Name(), ".view.json")
			visibleViewName, visible := temporaryMetadataTableName(session, schemaEntry.Name(), viewName)
			if !visible || !e.informationSchemaTableVisible(session, schemaEntry.Name(), viewName, true) || !informationSchemaMetadataQueryMatches(query, "table_name", visibleViewName, tablePattern) {
				continue
			}
			values := map[string]interface{}{
				"TABLE_CATALOG": "def", "TABLE_SCHEMA": schemaEntry.Name(), "TABLE_NAME": visibleViewName,
				"TABLE_TYPE": "VIEW", "ENGINE": nil, "VERSION": nil, "ROW_FORMAT": nil, "TABLE_ROWS": nil,
				"AVG_ROW_LENGTH": nil, "DATA_LENGTH": nil, "MAX_DATA_LENGTH": nil, "INDEX_LENGTH": nil,
				"DATA_FREE": nil, "AUTO_INCREMENT": nil, "CREATE_TIME": nil, "UPDATE_TIME": nil,
				"CHECK_TIME": nil, "TABLE_COLLATION": "utf8mb4_general_ci", "CHECKSUM": nil,
				"CREATE_OPTIONS": "", "TABLE_COMMENT": nil,
				"TABLE_CAT": schemaEntry.Name(), "TABLE_SCHEM": nil, "REMARKS": "",
			}
			if !performanceSchemaLockValuesMatch(query, values) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	schemaIndex, tableIndex := -1, -1
	for index, column := range columns {
		switch strings.ToUpper(column) {
		case "TABLE_SCHEMA":
			schemaIndex = index
		case "TABLE_NAME":
			tableIndex = index
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if tableIndex < 0 || tableIndex >= len(rows[i]) || tableIndex >= len(rows[j]) {
			return false
		}
		left, right := fmt.Sprint(rows[i][tableIndex]), fmt.Sprint(rows[j][tableIndex])
		if schemaIndex >= 0 && schemaIndex < len(rows[i]) && schemaIndex < len(rows[j]) {
			left = fmt.Sprintf("%s.%s", rows[i][schemaIndex], left)
			right = fmt.Sprintf("%s.%s", rows[j][schemaIndex], right)
		}
		return left < right
	})

	return newInformationSchemaSelectResult("information_schema_tables", columns, rows)
}

// informationSchemaTableSizes projects the durable ANALYZE statistics into
// INFORMATION_SCHEMA.TABLES.  The physical row counter is retained as a
// fallback for tables without analyzed statistics; index length is estimated
// from the page counters already exposed by the storage/index managers.
func (e *XMySQLExecutor) informationSchemaTableSizes(schemaName, tableName string) (rowCount, avgRowLength, dataLength, indexLength int64) {
	rowCount = e.physicalTableRowCount(schemaName, tableName)
	if e != nil && e.infosSchemaManager != nil {
		stats, err := e.infosSchemaManager.GetTableStats(context.Background(), schemaName, tableName)
		if err == nil && stats != nil {
			if stats.RowCount > 0 {
				rowCount = int64(stats.RowCount)
			}
			avgRowLength = int64(stats.AvgRowSize)
			dataLength = int64(stats.DataSize)
			indexLength = int64(stats.IndexSize)
		}
	}
	if indexLength == 0 {
		info, err := e.readPersistedTableInfo(schemaName, tableName)
		if err != nil {
			return rowCount, avgRowLength, dataLength, indexLength
		}
		for _, rawIndex := range info.Indexes {
			indexName, ok := rawIndex["name"].(string)
			if !ok || strings.TrimSpace(indexName) == "" {
				continue
			}
			pages := e.informationSchemaIndexPages(context.Background(), schemaName, tableName, indexName)
			if pages > 0 {
				indexLength += pages * int64(manager.PAGE_SIZE)
			}
		}
	}
	return rowCount, avgRowLength, dataLength, indexLength
}

func informationSchemaTablesRequestedColumns(query string) []string {
	defaults := []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "ENGINE", "VERSION", "ROW_FORMAT", "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE", "AUTO_INCREMENT", "CREATE_TIME", "UPDATE_TIME", "CHECK_TIME", "TABLE_COLLATION", "CHECKSUM", "CREATE_OPTIONS", "TABLE_COMMENT"}
	return requestedInformationSchemaColumns(query, defaults)
}

func (e *XMySQLExecutor) executeInformationSchemaSchemataSelect(query string, session server.MySQLServerSession) *SelectResult {
	filters := informationSchemaMetadataFilters(query)
	schemaPattern := filters["schema_name"]
	defaultColumns := []string{"CATALOG_NAME", "SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME", "SQL_PATH", "DEFAULT_ENCRYPTION"}
	columns := requestedInformationSchemaColumns(query, defaultColumns)

	schemaSet := map[string]struct{}{
		"information_schema": {},
		"mysql":              {},
		"performance_schema": {},
		"sys":                {},
	}
	for _, table := range e.scanFrmTables() {
		schemaSet[table.schemaName] = struct{}{}
	}

	schemas := make([]string, 0, len(schemaSet))
	for schemaName := range schemaSet {
		if metadataPatternMatches(schemaName, schemaPattern) {
			schemas = append(schemas, schemaName)
		}
	}
	sort.Strings(schemas)

	rows := make([][]interface{}, 0, len(schemas))
	for _, schemaName := range schemas {
		if !e.schemaMetadataVisibleToSession(session, schemaName) {
			continue
		}
		values := map[string]interface{}{
			"CATALOG_NAME": "def", "SCHEMA_NAME": schemaName, "DEFAULT_CHARACTER_SET_NAME": "utf8mb4",
			"DEFAULT_COLLATION_NAME": "utf8mb4_general_ci", "SQL_PATH": nil, "DEFAULT_ENCRYPTION": "N",
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult(
		"information_schema_schemata",
		columns,
		rows,
	)
}

func (e *XMySQLExecutor) executeInformationSchemaColumnsSelect(query string, session server.MySQLServerSession) *SelectResult {
	filters := informationSchemaMetadataFilters(query)
	schemaPattern := filters["table_schema"]
	tablePattern := filters["table_name"]
	columnPattern := filters["column_name"]

	defaultColumns := []string{
		"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_DEFAULT",
		"IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION",
		"NUMERIC_SCALE", "DATETIME_PRECISION", "CHARACTER_SET_NAME", "COLLATION_NAME", "COLUMN_TYPE", "COLUMN_KEY",
		"EXTRA", "PRIVILEGES", "COLUMN_COMMENT", "GENERATION_EXPRESSION", "SRS_ID",
	}
	columns := requestedInformationSchemaColumns(query, defaultColumns)
	rows := make([][]interface{}, 0)
	allTables := append(e.scanFrmTables(), e.scanViewTables(session)...)
	for _, table := range allTables {
		visibleTableName, visible := temporaryMetadataTableName(session, table.schemaName, table.tableName)
		if !visible || !e.informationSchemaTableVisible(session, table.schemaName, table.tableName, e.isViewMetadataTable(table)) || !informationSchemaMetadataQueryMatches(query, "table_schema", table.schemaName, schemaPattern) || !informationSchemaMetadataQueryMatches(query, "table_name", visibleTableName, tablePattern) {
			continue
		}
		for ordinal, column := range table.columns {
			if !metadataPatternMatches(column.name, columnPattern) {
				continue
			}
			nullable := int64(1)
			if !column.nullable {
				nullable = 0
			}
			columnType := formatInformationSchemaColumnType(column.typeName, column.length)
			dataType := int64(12)
			if strings.EqualFold(strings.TrimSpace(column.typeName), string(metadata.TypeBit)) {
				dataType = -7 // java.sql.Types.BIT
			}
			dataTypeValue := interface{}(strings.ToUpper(strings.TrimSpace(strings.SplitN(column.typeName, "(", 2)[0])))
			if jdbcTableCatIdentifierPattern.MatchString(query) || strings.Contains(strings.ToLower(query), "type_name") || strings.Contains(strings.ToLower(query), "column_size") {
				dataTypeValue = dataType
			}
			columnKey := ""
			if column.isPrimary {
				columnKey = "PRI"
			} else if column.isUnique {
				columnKey = "UNI"
			}
			extra := ""
			if column.autoIncrement {
				extra = "auto_increment"
			}
			values := map[string]interface{}{
				"TABLE_CATALOG": "def", "TABLE_SCHEMA": table.schemaName, "TABLE_NAME": visibleTableName,
				"TABLE_CAT": table.schemaName, "TABLE_SCHEM": nil,
				"COLUMN_NAME": column.name, "DATA_TYPE": dataTypeValue, "TYPE_NAME": strings.ToUpper(column.typeName), "COLUMN_TYPE": columnType,
				"CHARACTER_MAXIMUM_LENGTH": informationSchemaCharacterLength(column), "CHARACTER_OCTET_LENGTH": informationSchemaCharacterLength(column),
				"NUMERIC_PRECISION": nil, "NUMERIC_SCALE": nil, "DATETIME_PRECISION": nil, "CHARACTER_SET_NAME": informationSchemaCharacterSet(column.typeName), "COLLATION_NAME": informationSchemaCollation(column.typeName),
				"COLUMN_SIZE": int64(column.length), "NULLABLE": nullable, "IS_NULLABLE": map[bool]string{true: "YES", false: "NO"}[column.nullable], "REMARKS": column.comment,
				"ORDINAL_POSITION": int64(ordinal + 1), "COLUMN_KEY": columnKey, "EXTRA": extra, "PRIVILEGES": "select,insert,update,references", "COLUMN_COMMENT": column.comment,
				"COLUMN_DEFAULT": column.defaultValue, "GENERATION_EXPRESSION": column.generatedExpression, "SRS_ID": nil,
			}
			if performanceSchemaLockValuesMatch(query, values) {
				rows = append(rows, projectInformationSchemaRow(columns, values))
			}
		}
	}
	for qualified, virtualColumns := range informationSchemaVirtualTableDefinitions() {
		parts := strings.SplitN(qualified, ".", 2)
		if len(parts) != 2 || !informationSchemaMetadataQueryMatches(query, "table_schema", parts[0], schemaPattern) || !informationSchemaMetadataQueryMatches(query, "table_name", parts[1], tablePattern) {
			continue
		}
		for ordinal, columnName := range virtualColumns {
			if !metadataPatternMatches(columnName, columnPattern) {
				continue
			}
			typeName, length, nullable := informationSchemaVirtualColumnType(columnName)
			column := frmMetadataColumn{name: columnName, typeName: typeName, length: length, nullable: nullable}
			dataType := interface{}(strings.ToUpper(typeName))
			if strings.Contains(strings.ToLower(query), "type_name") || strings.Contains(strings.ToLower(query), "column_size") {
				dataType = int64(12)
			}
			values := map[string]interface{}{
				"TABLE_CATALOG": "def", "TABLE_SCHEMA": parts[0], "TABLE_NAME": parts[1],
				"TABLE_CAT": parts[0], "TABLE_SCHEM": nil, "COLUMN_NAME": columnName,
				"ORDINAL_POSITION": int64(ordinal + 1), "COLUMN_DEFAULT": nil, "IS_NULLABLE": map[bool]string{true: "YES", false: "NO"}[nullable],
				"DATA_TYPE": dataType, "TYPE_NAME": typeName, "COLUMN_TYPE": typeName,
				"CHARACTER_MAXIMUM_LENGTH": informationSchemaCharacterLength(column), "CHARACTER_OCTET_LENGTH": informationSchemaCharacterLength(column),
				"NUMERIC_PRECISION": nil, "NUMERIC_SCALE": nil, "DATETIME_PRECISION": nil,
				"CHARACTER_SET_NAME": informationSchemaCharacterSet(typeName), "COLLATION_NAME": informationSchemaCollation(typeName),
				"COLUMN_SIZE": int64(length), "NULLABLE": map[bool]int64{true: 1, false: 0}[nullable], "REMARKS": nil,
				"COLUMN_KEY": "", "EXTRA": "", "PRIVILEGES": "select", "COLUMN_COMMENT": nil,
				"GENERATION_EXPRESSION": nil, "SRS_ID": nil,
			}
			if performanceSchemaLockValuesMatch(query, values) {
				rows = append(rows, projectInformationSchemaRow(columns, values))
			}
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		left := informationSchemaRowSortKey(rows[i], columns)
		right := informationSchemaRowSortKey(rows[j], columns)
		return left < right
	})

	return newInformationSchemaSelectResult("information_schema_columns", columns, rows)
}

func formatInformationSchemaColumnType(typeName string, length int) string {
	upper := strings.ToUpper(strings.TrimSpace(typeName))
	if upper == "" {
		return upper
	}
	if length > 0 {
		switch upper {
		case "BIT", "CHAR", "VARCHAR", "BINARY", "VARBINARY":
			return fmt.Sprintf("%s(%d)", upper, length)
		}
	}
	return upper
}

func informationSchemaRowSortKey(row []interface{}, columns []string) string {
	value := func(name string) interface{} {
		for index, column := range columns {
			if strings.EqualFold(column, name) && index < len(row) {
				return row[index]
			}
		}
		return nil
	}
	return fmt.Sprintf("%v.%v.%03v", value("TABLE_CAT"), value("TABLE_NAME"), value("ORDINAL_POSITION"))
}

func (e *XMySQLExecutor) executeInformationSchemaKeyColumnUsageSelect(query string, session server.MySQLServerSession) *SelectResult {
	defaults := []string{"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "POSITION_IN_UNIQUE_CONSTRAINT", "REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME"}
	columns := requestedInformationSchemaColumns(query, defaults)
	filters := informationSchemaMetadataFilters(query)
	rows := make([][]interface{}, 0)
	for _, table := range e.scanFrmTables() {
		visibleTableName, visible := temporaryMetadataTableName(session, table.schemaName, table.tableName)
		if !visible || !e.informationSchemaTableVisible(session, table.schemaName, table.tableName, false) || !metadataPatternMatches(table.schemaName, filters["table_schema"]) || !metadataPatternMatches(visibleTableName, filters["table_name"]) {
			continue
		}
		info, err := e.readPersistedTableInfo(table.schemaName, table.tableName)
		if err != nil {
			continue
		}
		for _, index := range info.Indexes {
			if autoForeignKey, _ := index["auto_foreign_key"].(bool); autoForeignKey {
				continue
			}
			name, _ := index["name"].(string)
			constraintType := "UNIQUE"
			if primary, _ := index["primary"].(bool); primary || strings.EqualFold(name, "PRIMARY") {
				constraintType = "PRIMARY KEY"
			}
			indexColumns, _ := index["columns"].([]interface{})
			for ordinal, indexColumn := range indexColumns {
				constraintName := name
				if !metadataPatternMatches(constraintName, filters["constraint_name"]) {
					continue
				}
				row := map[string]interface{}{
					"CONSTRAINT_CATALOG": "def", "CONSTRAINT_SCHEMA": table.schemaName, "CONSTRAINT_NAME": constraintName,
					"TABLE_CATALOG": "def", "TABLE_SCHEMA": table.schemaName, "TABLE_NAME": visibleTableName,
					"COLUMN_NAME": fmt.Sprint(indexColumn), "ORDINAL_POSITION": int64(ordinal + 1),
					"POSITION_IN_UNIQUE_CONSTRAINT": nil, "REFERENCED_TABLE_SCHEMA": nil, "REFERENCED_TABLE_NAME": nil, "REFERENCED_COLUMN_NAME": nil,
					"CONSTRAINT_TYPE": constraintType,
				}
				if performanceSchemaLockValuesMatch(query, row) {
					rows = append(rows, projectInformationSchemaRow(columns, row))
				}
			}
		}
		for fkIndex, rawFK := range info.ForeignKeys {
			constraintName := fmt.Sprint(rawFK["name"])
			if constraintName == "" || constraintName == "<nil>" {
				constraintName = fmt.Sprintf("FOREIGN_KEY_%d", fkIndex+1)
			}
			if !metadataPatternMatches(constraintName, filters["constraint_name"]) {
				continue
			}
			localColumns, _ := rawFK["columns"].([]interface{})
			refColumns, _ := rawFK["ref_columns"].([]interface{})
			for ordinal, localColumn := range localColumns {
				var refColumn interface{}
				if ordinal < len(refColumns) {
					refColumn = refColumns[ordinal]
				}
				row := map[string]interface{}{
					"CONSTRAINT_CATALOG": "def", "CONSTRAINT_SCHEMA": table.schemaName, "CONSTRAINT_NAME": constraintName,
					"TABLE_CATALOG": "def", "TABLE_SCHEMA": table.schemaName, "TABLE_NAME": visibleTableName,
					"COLUMN_NAME": fmt.Sprint(localColumn), "ORDINAL_POSITION": int64(ordinal + 1),
					"POSITION_IN_UNIQUE_CONSTRAINT": int64(ordinal + 1), "REFERENCED_TABLE_SCHEMA": foreignKeyReferencedSchema(rawFK, table.schemaName),
					"REFERENCED_TABLE_NAME": rawFK["ref_table"], "REFERENCED_COLUMN_NAME": refColumn,
					"CONSTRAINT_TYPE": "FOREIGN KEY",
				}
				if performanceSchemaLockValuesMatch(query, row) {
					rows = append(rows, projectInformationSchemaRow(columns, row))
				}
			}
		}
	}
	return newInformationSchemaSelectResult("information_schema_key_column_usage", columns, rows)
}

func (e *XMySQLExecutor) executeInformationSchemaTableConstraintsSelect(query string, session server.MySQLServerSession) *SelectResult {
	defaults := []string{"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_TYPE", "ENFORCED"}
	columns := requestedInformationSchemaColumns(query, defaults)
	filters := informationSchemaMetadataFilters(query)
	rows := make([][]interface{}, 0)
	for _, table := range e.scanFrmTables() {
		visibleTableName, visible := temporaryMetadataTableName(session, table.schemaName, table.tableName)
		if !visible || !e.informationSchemaTableVisible(session, table.schemaName, table.tableName, false) || !metadataPatternMatches(table.schemaName, filters["table_schema"]) || !metadataPatternMatches(visibleTableName, filters["table_name"]) {
			continue
		}
		info, err := e.readPersistedTableInfo(table.schemaName, table.tableName)
		if err != nil {
			continue
		}
		for _, index := range info.Indexes {
			if autoForeignKey, _ := index["auto_foreign_key"].(bool); autoForeignKey {
				continue
			}
			name, _ := index["name"].(string)
			if name == "" {
				continue
			}
			unique, _ := index["unique"].(bool)
			primary, _ := index["primary"].(bool)
			if !unique && !primary && !strings.EqualFold(name, "PRIMARY") {
				continue
			}
			constraintType := "UNIQUE"
			if primary || strings.EqualFold(name, "PRIMARY") {
				constraintType = "PRIMARY KEY"
			}
			row := map[string]interface{}{"CONSTRAINT_CATALOG": "def", "CONSTRAINT_SCHEMA": table.schemaName, "CONSTRAINT_NAME": name, "TABLE_SCHEMA": table.schemaName, "TABLE_NAME": visibleTableName, "CONSTRAINT_TYPE": constraintType, "ENFORCED": "YES"}
			if metadataPatternMatches(name, filters["constraint_name"]) && metadataPatternMatches(constraintType, filters["constraint_type"]) {
				if performanceSchemaLockValuesMatch(query, row) {
					rows = append(rows, projectInformationSchemaRow(columns, row))
				}
			}
		}
		for fkIndex, rawFK := range info.ForeignKeys {
			name := fmt.Sprint(rawFK["name"])
			if name == "" || name == "<nil>" {
				name = fmt.Sprintf("FOREIGN_KEY_%d", fkIndex+1)
			}
			if metadataPatternMatches(name, filters["constraint_name"]) && metadataPatternMatches("FOREIGN KEY", filters["constraint_type"]) {
				row := map[string]interface{}{"CONSTRAINT_CATALOG": "def", "CONSTRAINT_SCHEMA": table.schemaName, "CONSTRAINT_NAME": name, "TABLE_SCHEMA": table.schemaName, "TABLE_NAME": visibleTableName, "CONSTRAINT_TYPE": "FOREIGN KEY", "ENFORCED": "YES"}
				if performanceSchemaLockValuesMatch(query, row) {
					rows = append(rows, projectInformationSchemaRow(columns, row))
				}
			}
		}
		checkNames := make(map[string]string, len(info.CheckNames))
		for name, expression := range info.CheckNames {
			checkNames[name] = expression
		}
		for checkIndex, expression := range info.Checks {
			name := ""
			for candidate, candidateExpression := range checkNames {
				if strings.EqualFold(candidateExpression, expression) {
					name = candidate
					break
				}
			}
			if name == "" {
				name = fmt.Sprintf("check_%d", checkIndex+1)
			}
			if !metadataPatternMatches(name, filters["constraint_name"]) || !metadataPatternMatches("CHECK", filters["constraint_type"]) {
				continue
			}
			enforced := "YES"
			if value, ok := info.CheckEnforced[strings.ToLower(name)]; ok && !value {
				enforced = "NO"
			}
			row := map[string]interface{}{
				"CONSTRAINT_CATALOG": "def", "CONSTRAINT_SCHEMA": table.schemaName, "CONSTRAINT_NAME": name,
				"TABLE_SCHEMA": table.schemaName, "TABLE_NAME": visibleTableName, "CONSTRAINT_TYPE": "CHECK", "ENFORCED": enforced,
			}
			if performanceSchemaLockValuesMatch(query, row) {
				rows = append(rows, projectInformationSchemaRow(columns, row))
			}
		}
	}
	return newInformationSchemaSelectResult("information_schema_table_constraints", columns, rows)
}

func (e *XMySQLExecutor) executeInformationSchemaCheckConstraintsSelect(query string, session server.MySQLServerSession) *SelectResult {
	defaults := []string{"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "CHECK_CLAUSE"}
	columns := requestedInformationSchemaColumns(query, defaults)
	filters := informationSchemaMetadataFilters(query)
	rows := make([][]interface{}, 0)
	for _, table := range e.scanFrmTables() {
		visibleTableName, visible := temporaryMetadataTableName(session, table.schemaName, table.tableName)
		if !visible || !e.informationSchemaTableVisible(session, table.schemaName, table.tableName, false) || !metadataPatternMatches(table.schemaName, filters["table_schema"]) || !metadataPatternMatches(visibleTableName, filters["table_name"]) {
			continue
		}
		info, err := e.readPersistedTableInfo(table.schemaName, table.tableName)
		if err != nil {
			continue
		}
		checkNames := make(map[string]string, len(info.CheckNames))
		for name, expression := range info.CheckNames {
			checkNames[name] = expression
		}
		for index, expression := range info.Checks {
			name := ""
			for candidate, candidateExpression := range checkNames {
				if strings.EqualFold(candidateExpression, expression) {
					name = candidate
					break
				}
			}
			if name == "" {
				name = fmt.Sprintf("check_%d", index+1)
			}
			if !metadataPatternMatches(name, filters["constraint_name"]) {
				continue
			}
			enforced := "YES"
			if value, ok := info.CheckEnforced[strings.ToLower(name)]; ok && !value {
				enforced = "NO"
			}
			row := map[string]interface{}{
				"CONSTRAINT_CATALOG": "def", "CONSTRAINT_SCHEMA": table.schemaName, "CONSTRAINT_NAME": name,
				"TABLE_SCHEMA": table.schemaName, "TABLE_NAME": visibleTableName, "CHECK_CLAUSE": expression, "ENFORCED": enforced,
			}
			if performanceSchemaLockValuesMatch(query, row) {
				rows = append(rows, projectInformationSchemaRow(columns, row))
			}
		}
	}
	return newInformationSchemaSelectResult("information_schema_check_constraints", columns, rows)
}

func (e *XMySQLExecutor) executeInformationSchemaReferentialConstraintsSelect(query string, session server.MySQLServerSession) *SelectResult {
	defaults := []string{"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "UNIQUE_CONSTRAINT_CATALOG", "UNIQUE_CONSTRAINT_SCHEMA", "UNIQUE_CONSTRAINT_NAME", "MATCH_OPTION", "UPDATE_RULE", "DELETE_RULE", "TABLE_NAME", "REFERENCED_TABLE_NAME"}
	columns := requestedInformationSchemaColumns(query, defaults)
	filters := informationSchemaMetadataFilters(query)
	rows := make([][]interface{}, 0)
	for _, table := range e.scanFrmTables() {
		visibleTableName, visible := temporaryMetadataTableName(session, table.schemaName, table.tableName)
		if !visible || !e.informationSchemaTableVisible(session, table.schemaName, table.tableName, false) || !metadataPatternMatches(table.schemaName, filters["table_schema"]) || !metadataPatternMatches(visibleTableName, filters["table_name"]) {
			continue
		}
		info, err := e.readPersistedTableInfo(table.schemaName, table.tableName)
		if err != nil {
			continue
		}
		for fkIndex, rawFK := range info.ForeignKeys {
			name := fmt.Sprint(rawFK["name"])
			if name == "" || name == "<nil>" {
				name = fmt.Sprintf("FOREIGN_KEY_%d", fkIndex+1)
			}
			if !metadataPatternMatches(name, filters["constraint_name"]) {
				continue
			}
			row := map[string]interface{}{
				"CONSTRAINT_CATALOG": "def", "CONSTRAINT_SCHEMA": table.schemaName, "CONSTRAINT_NAME": name,
				"UNIQUE_CONSTRAINT_CATALOG": "def", "UNIQUE_CONSTRAINT_SCHEMA": foreignKeyReferencedSchema(rawFK, table.schemaName), "UNIQUE_CONSTRAINT_NAME": e.referencedUniqueConstraintName(rawFK, table.schemaName),
				"MATCH_OPTION": "NONE", "UPDATE_RULE": foreignKeyReferentialRule(rawFK, "on_update"), "DELETE_RULE": foreignKeyReferentialRule(rawFK, "on_delete"),
				"TABLE_NAME": visibleTableName, "REFERENCED_TABLE_NAME": rawFK["ref_table"],
			}
			if performanceSchemaLockValuesMatch(query, row) {
				rows = append(rows, projectInformationSchemaRow(columns, row))
			}
		}
	}
	return newInformationSchemaSelectResult("information_schema_referential_constraints", columns, rows)
}

func foreignKeyReferencedSchema(foreignKey map[string]interface{}, fallback string) string {
	if schema := strings.Trim(strings.TrimSpace(fmt.Sprint(foreignKey["ref_schema"])), "`"); schema != "" && schema != "<nil>" {
		return schema
	}
	return fallback
}

func foreignKeyReferentialRule(foreignKey map[string]interface{}, key string) string {
	rule := strings.ToUpper(strings.TrimSpace(fmt.Sprint(foreignKey[key])))
	if rule == "" || rule == "<NIL>" {
		return "RESTRICT"
	}
	return rule
}

func (e *XMySQLExecutor) referencedUniqueConstraintName(foreignKey map[string]interface{}, fallbackSchema string) interface{} {
	if e == nil || foreignKey == nil {
		return nil
	}
	schema := foreignKeyReferencedSchema(foreignKey, fallbackSchema)
	table := strings.Trim(strings.TrimSpace(fmt.Sprint(foreignKey["ref_table"])), "`")
	referencedColumns := metadataIdentifierList(foreignKey["ref_columns"])
	if schema == "" || table == "" || table == "<nil>" || len(referencedColumns) == 0 {
		return nil
	}
	info, err := e.readPersistedTableInfo(schema, table)
	if err != nil {
		return nil
	}
	for _, index := range info.Indexes {
		name := strings.Trim(strings.TrimSpace(fmt.Sprint(index["name"])), "`")
		if name == "" || name == "<nil>" {
			continue
		}
		unique, _ := index["unique"].(bool)
		primary, _ := index["primary"].(bool)
		if !unique && !primary && !strings.EqualFold(name, "PRIMARY") {
			continue
		}
		columns := metadataIdentifierList(index["columns"])
		if len(columns) != len(referencedColumns) {
			continue
		}
		matches := true
		for i := range columns {
			if !strings.EqualFold(columns[i], referencedColumns[i]) {
				matches = false
				break
			}
		}
		if matches {
			if primary || strings.EqualFold(name, "PRIMARY") {
				return "PRIMARY"
			}
			return name
		}
	}
	return nil
}

func (e *XMySQLExecutor) executeInformationSchemaViewsSelect(query string, session server.MySQLServerSession) *SelectResult {
	defaults := []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "VIEW_DEFINITION", "CHECK_OPTION", "IS_UPDATABLE", "DEFINER", "SECURITY_TYPE", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION"}
	columns := requestedInformationSchemaColumns(query, defaults)
	filters := informationSchemaMetadataFilters(query)
	rows := make([][]interface{}, 0)
	entries, _ := os.ReadDir(e.getDataDir())
	for _, schemaEntry := range entries {
		if !schemaEntry.IsDir() || !metadataPatternMatches(schemaEntry.Name(), filters["table_schema"]) {
			continue
		}
		viewEntries, _ := os.ReadDir(filepath.Join(e.getDataDir(), schemaEntry.Name()))
		for _, viewEntry := range viewEntries {
			if viewEntry.IsDir() || !strings.HasSuffix(viewEntry.Name(), ".view.json") {
				continue
			}
			tableName := strings.TrimSuffix(viewEntry.Name(), ".view.json")
			if !metadataPatternMatches(tableName, filters["table_name"]) {
				continue
			}
			if err := e.checkViewMetadataPrivilege(&ExecutionContext{Session: session}, schemaEntry.Name()); err != nil {
				// INFORMATION_SCHEMA hides view definitions without SHOW VIEW.
				continue
			}
			raw, err := os.ReadFile(filepath.Join(e.getDataDir(), schemaEntry.Name(), viewEntry.Name()))
			if err != nil {
				continue
			}
			var info struct {
				Definition  string `json:"definition"`
				Definer     string `json:"definer"`
				SQLSecurity string `json:"sql_security"`
				CheckOption string `json:"check_option"`
			}
			if json.Unmarshal(raw, &info) != nil {
				continue
			}
			checkOption := strings.ToUpper(strings.TrimSpace(info.CheckOption))
			if checkOption == "" {
				checkOption = "NONE"
			}
			definer := strings.TrimSpace(info.Definer)
			if definer == "" {
				definer = "root@localhost"
			} else if definerMatch := regexp.MustCompile(`^'([^']*)'@'([^']*)'$`).FindStringSubmatch(definer); len(definerMatch) == 3 {
				definer = definerMatch[1] + "@" + definerMatch[2]
			}
			securityType := strings.ToUpper(strings.TrimSpace(info.SQLSecurity))
			if securityType == "" {
				securityType = "DEFINER"
			}
			row := map[string]interface{}{
				"TABLE_CATALOG": "def", "TABLE_SCHEMA": schemaEntry.Name(), "TABLE_NAME": tableName,
				"VIEW_DEFINITION": info.Definition, "CHECK_OPTION": checkOption, "IS_UPDATABLE": "NO",
				"DEFINER": definer, "SECURITY_TYPE": securityType, "CHARACTER_SET_CLIENT": "utf8mb4", "COLLATION_CONNECTION": "utf8mb4_general_ci",
			}
			if performanceSchemaLockValuesMatch(query, row) {
				rows = append(rows, projectInformationSchemaRow(columns, row))
			}
		}
	}
	return newInformationSchemaSelectResult("information_schema_views", columns, rows)
}

func (e *XMySQLExecutor) checkViewMetadataPrivilege(ctx *ExecutionContext, schema string) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	user, _ := ctx.Session.GetParamByName("user").(string)
	user = strings.TrimSpace(user)
	if user == "" || strings.EqualFold(user, "root") {
		return nil
	}
	host, _ := ctx.Session.GetParamByName("host").(string)
	if strings.TrimSpace(host) == "" {
		host = "localhost"
	}
	file, err := e.accountFileForSession(ctx)
	if err != nil {
		return err
	}
	account := sessionAccount(file, ctx.Session)
	if account == nil {
		return fmt.Errorf("access denied: user '%s'@'%s' does not exist", user, host)
	}
	if grantsContain(effectiveAccountGrants(file, *account, ctx.Session), strings.TrimSpace(schema)+".*", "SHOW VIEW") {
		return nil
	}
	return fmt.Errorf("access denied: user '%s'@'%s' lacks SHOW VIEW privilege on database '%s'", user, host, schema)
}

func (e *XMySQLExecutor) checkViewDefinerPrivilege(ctx *ExecutionContext, path string) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	user, _ := ctx.Session.GetParamByName("user").(string)
	user = strings.TrimSpace(user)
	if user == "" || strings.EqualFold(user, "root") {
		return nil
	}
	host, _ := ctx.Session.GetParamByName("host").(string)
	host = strings.TrimSpace(host)
	if host == "" {
		host = "localhost"
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var info struct {
		Definer string `json:"definer"`
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		return err
	}
	definer := strings.TrimSpace(info.Definer)
	if definer == "" {
		definer = "root@localhost"
	} else if match := regexp.MustCompile(`^'([^']*)'@'([^']*)'$`).FindStringSubmatch(definer); len(match) == 3 {
		definer = match[1] + "@" + match[2]
	}
	if strings.EqualFold(definer, user+"@"+host) {
		return nil
	}
	file, err := e.accountFileForSession(ctx)
	if err != nil {
		return err
	}
	account := sessionAccount(file, ctx.Session)
	if account == nil {
		return fmt.Errorf("access denied: user '%s'@'%s' does not exist", user, host)
	}
	grants := effectiveAccountGrants(file, *account, ctx.Session)
	if grantsContain(grants, "*.*", "SET_ANY_DEFINER") || grantsContain(grants, "*.*", "ALLOW_NONEXISTENT_DEFINER") {
		return nil
	}
	return fmt.Errorf("access denied: user '%s'@'%s' is not the view definer '%s'", user, host, definer)
}

func (e *XMySQLExecutor) checkViewDefinerClausePrivilege(ctx *ExecutionContext, query string) error {
	match := regexp.MustCompile(`(?is)\bdefiner\s*=\s*(?:current_user(?:\(\))?|'([^']*)'\s*@\s*'([^']*)'|"([^"]*)"\s*@\s*"([^"]*)")`).FindStringSubmatch(query)
	if len(match) == 0 {
		return nil
	}
	definerUser, definerHost := strings.TrimSpace(match[1]), strings.TrimSpace(match[2])
	if definerUser == "" && len(match) >= 5 {
		definerUser, definerHost = strings.TrimSpace(match[3]), strings.TrimSpace(match[4])
	}
	if definerUser == "" {
		return nil
	}
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	user, _ := ctx.Session.GetParamByName("user").(string)
	user = strings.TrimSpace(user)
	host, _ := ctx.Session.GetParamByName("host").(string)
	host = strings.TrimSpace(host)
	if host == "" {
		host = "localhost"
	}
	if user == "" || strings.EqualFold(user, "root") || (strings.EqualFold(user, definerUser) && strings.EqualFold(host, definerHost)) {
		return nil
	}
	file, err := e.accountFileForSession(ctx)
	if err != nil {
		return err
	}
	account := sessionAccount(file, ctx.Session)
	if account == nil {
		return fmt.Errorf("access denied: user '%s'@'%s' does not exist", user, host)
	}
	grants := effectiveAccountGrants(file, *account, ctx.Session)
	if grantsContain(grants, "*.*", "SET_ANY_DEFINER") || grantsContain(grants, "*.*", "ALLOW_NONEXISTENT_DEFINER") {
		return nil
	}
	return fmt.Errorf("access denied: user '%s'@'%s' cannot use definer '%s'@'%s'", user, host, definerUser, definerHost)
}

func (e *XMySQLExecutor) executeInformationSchemaStatisticsSelect(query string, session server.MySQLServerSession) *SelectResult {
	filters := informationSchemaMetadataFilters(query)
	defaultColumns := []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "NON_UNIQUE", "INDEX_SCHEMA", "INDEX_NAME", "SEQ_IN_INDEX", "COLUMN_NAME", "COLLATION", "CARDINALITY", "SUB_PART", "PACKED", "NULLABLE", "INDEX_TYPE", "COMMENT", "INDEX_COMMENT", "IS_VISIBLE", "EXPRESSION"}
	columns := requestedInformationSchemaColumns(query, defaultColumns)
	lower := strings.ToLower(query)
	if jdbcTableCatIdentifierPattern.MatchString(query) || strings.Contains(lower, "pk_name") || strings.Contains(lower, "index_qualifier") {
		columns = jdbcStatisticsMetadataColumns(query)
	}
	rows := make([][]interface{}, 0)
	indexNameFilter := ""
	if match := regexp.MustCompile(`(?is)\bindex_name\s*=\s*'([^']*)'`).FindStringSubmatch(query); len(match) == 2 {
		indexNameFilter = strings.TrimSpace(match[1])
	}
	for _, table := range e.scanFrmTables() {
		visibleTableName, visible := temporaryMetadataTableName(session, table.schemaName, table.tableName)
		if !visible || !e.informationSchemaTableVisible(session, table.schemaName, table.tableName, false) || !metadataPatternMatches(table.schemaName, filters["table_schema"]) || !metadataPatternMatches(visibleTableName, filters["table_name"]) {
			continue
		}
		info, err := e.readPersistedTableInfo(table.schemaName, table.tableName)
		if err != nil {
			continue
		}
		indexCardinality := make(map[string]interface{})
		if info.Stats != nil {
			for name, stat := range info.Stats.IndexStats {
				indexCardinality[name] = int64(stat.DistinctCount)
			}
		}
		if len(indexCardinality) == 0 {
			cardinalityCtx := &ExecutionContext{Context: context.Background(), DatabaseName: table.schemaName}
			if current, cardinalityErr := e.tableIndexCardinalities(cardinalityCtx, table.schemaName, table.tableName); cardinalityErr == nil {
				for name, value := range current {
					indexCardinality[name] = value
				}
			}
		}
		for _, index := range info.Indexes {
			indexName, _ := index["name"].(string)
			if indexNameFilter != "" && !strings.EqualFold(indexName, indexNameFilter) {
				continue
			}
			if strings.Contains(lower, "index_name='primary'") && !strings.EqualFold(indexName, "PRIMARY") {
				continue
			}
			unique, _ := index["unique"].(bool)
			visible := true
			if persistedVisible, ok := index["visible"].(bool); ok {
				visible = persistedVisible
			}
			visibleValue := "NO"
			if visible {
				visibleValue = "YES"
			}
			indexColumns, _ := index["columns"].([]interface{})
			for ordinal, indexColumn := range indexColumns {
				row := map[string]interface{}{
					"TABLE_CATALOG": "def", "TABLE_SCHEMA": table.schemaName, "TABLE_NAME": visibleTableName,
					"NON_UNIQUE": int64(0), "INDEX_SCHEMA": table.schemaName, "INDEX_NAME": indexName,
					"SEQ_IN_INDEX": int64(ordinal + 1), "COLUMN_NAME": fmt.Sprint(indexColumn), "COLLATION": "A", "CARDINALITY": indexCardinality[indexName],
					"SUB_PART": nil, "PACKED": nil, "NULLABLE": "", "INDEX_TYPE": "BTREE", "COMMENT": "", "INDEX_COMMENT": "", "IS_VISIBLE": visibleValue, "EXPRESSION": nil,
					"TABLE_CAT": table.schemaName, "TABLE_SCHEM": nil, "INDEX_QUALIFIER": nil, "TYPE": int64(3), "ORDINAL_POSITION": int64(ordinal + 1), "ASC_OR_DESC": "A", "PAGES": e.informationSchemaIndexPages(context.Background(), table.schemaName, table.tableName, indexName), "FILTER_CONDITION": nil,
					"KEY_SEQ": int64(ordinal + 1), "PK_NAME": indexName,
				}
				if !unique {
					row["NON_UNIQUE"] = int64(1)
				}
				if jdbcTableCatIdentifierPattern.MatchString(query) {
					row["TABLE_CAT"] = table.schemaName
					row["TABLE_SCHEM"] = nil
					row["INDEX_QUALIFIER"] = nil
					row["NON_UNIQUE"] = row["NON_UNIQUE"]
				}
				if performanceSchemaLockValuesMatch(query, row) {
					rows = append(rows, projectInformationSchemaRow(columns, row))
				}
			}
		}
	}
	return newInformationSchemaSelectResult("information_schema_statistics", columns, rows)
}

// informationSchemaIndexPages exposes the best durable page estimate already
// maintained by the storage/index managers.  The clustered index can be
// counted directly from its leaf chain; secondary indexes use their durable
// IndexManager page counter.  This is intentionally an estimate, not a claim
// of upstream InnoDB's full physical sampling semantics.
func (e *XMySQLExecutor) informationSchemaIndexPages(ctx context.Context, schemaName, tableName, indexName string) int64 {
	if e == nil {
		return 0
	}
	if e.indexManager != nil && !strings.EqualFold(indexName, "PRIMARY") {
		tableID := manager.SecondaryIndexTableID(schemaName, tableName)
		if index := e.indexManager.GetIndexByName(tableID, indexName); index != nil && index.PageCount > 0 {
			return int64(index.PageCount)
		}
	}
	if !strings.EqualFold(indexName, "PRIMARY") || e.tableStorageManager == nil {
		return 0
	}
	if ctx == nil {
		ctx = context.Background()
	}
	btree, err := e.tableStorageManager.CreateBTreeManagerForTable(ctx, schemaName, tableName)
	if err != nil || btree == nil {
		return 0
	}
	pages, err := btree.GetAllLeafPages(ctx)
	if err != nil {
		return 0
	}
	return int64(len(pages))
}

func requestedInformationSchemaColumns(query string, defaults []string) []string {
	match := regexp.MustCompile("(?is)^\\s*select\\s+(.+?)\\s+from\\s+").FindStringSubmatch(query)
	if len(match) == 0 || strings.TrimSpace(match[1]) == "*" {
		return defaults
	}
	columns := make([]string, 0)
	for _, item := range splitTopLevelComma(match[1]) {
		item = strings.TrimSpace(item)
		if idx := strings.LastIndex(strings.ToLower(item), " as "); idx >= 0 {
			item = item[idx+4:]
		} else if fields := strings.Fields(item); len(fields) > 1 {
			item = fields[len(fields)-1]
		}
		item = strings.Trim(strings.TrimSpace(item), "`")
		if item != "" {
			columns = append(columns, strings.ToUpper(item))
		}
	}
	if len(columns) == 0 {
		return defaults
	}
	return columns
}

// normalizeInformationSchemaAggregate applies the aggregate projection that
// ordinary table execution normally performs after scanning rows. Dedicated
// INFORMATION_SCHEMA handlers intentionally return catalog rows directly, so
// without this step SELECT COUNT(*) would expose one NULL per source row (the
// synthetic COUNT(*) column is not a catalog field).
func normalizeInformationSchemaAggregate(query string, result *SelectResult) *SelectResult {
	if result == nil {
		return result
	}
	match := regexp.MustCompile(`(?is)^\s*select\s+(.+?)\s+from\s+`).FindStringSubmatch(query)
	if len(match) != 2 {
		return result
	}
	items := splitTopLevelComma(match[1])
	if len(items) != 1 {
		return result
	}
	item := strings.TrimSpace(items[0])
	normalized := strings.ToLower(strings.ReplaceAll(item, " ", ""))
	if normalized != "count(*)" && !strings.HasPrefix(normalized, "count(*)as") {
		return result
	}
	column := "COUNT(*)"
	if aliasIndex := strings.Index(strings.ToLower(item), " as "); aliasIndex >= 0 {
		alias := strings.Trim(strings.TrimSpace(item[aliasIndex+4:]), "`")
		if alias != "" {
			column = alias
		}
	}
	return newInformationSchemaSelectResult("information_schema_aggregate", []string{column}, [][]interface{}{{int64(len(result.Records))}})
}

func informationSchemaSelectsAllColumns(query string) bool {
	match := regexp.MustCompile(`(?is)^\s*select\s+(.+?)\s+from\s+`).FindStringSubmatch(query)
	return len(match) == 2 && strings.TrimSpace(match[1]) == "*"
}

func informationSchemaSelectListContains(query, column string) bool {
	match := regexp.MustCompile(`(?is)^\s*select\s+(.+?)\s+from\s+`).FindStringSubmatch(query)
	if len(match) != 2 {
		return false
	}
	for _, item := range splitTopLevelComma(match[1]) {
		item = strings.TrimSpace(item)
		if index := strings.Index(strings.ToLower(item), " as "); index >= 0 {
			item = item[:index]
		}
		if strings.EqualFold(strings.Trim(strings.TrimSpace(item), "`"), column) {
			return true
		}
	}
	return false
}

func informationSchemaSelectListContainsAny(query string, columns ...string) bool {
	for _, column := range columns {
		if informationSchemaSelectListContains(query, column) {
			return true
		}
	}
	return false
}

func projectInformationSchemaRow(columns []string, values map[string]interface{}) []interface{} {
	row := make([]interface{}, 0, len(columns))
	for _, column := range columns {
		row = append(row, values[strings.ToUpper(column)])
	}
	return row
}

type frmMetadataTable struct {
	schemaName       string
	tableName        string
	columns          []frmMetadataColumn
	indexes          []frmMetadataIndex
	rowFormat        string
	totalRowVersions int
}

type frmMetadataIndex struct {
	name    string
	unique  bool
	primary bool
	columns []string
}

type frmMetadataColumn struct {
	name                string
	typeName            string
	length              int
	nullable            bool
	comment             string
	autoIncrement       bool
	defaultValue        interface{}
	generatedExpression interface{}
	isPrimary           bool
	isUnique            bool
	instantAdded        bool
}

func (e *XMySQLExecutor) scanFrmTables() []frmMetadataTable {
	dataDir := e.getDataDir()
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil
	}

	tables := make([]frmMetadataTable, 0)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		schemaName := entry.Name()
		tableEntries, err := os.ReadDir(filepath.Join(dataDir, schemaName))
		if err != nil {
			continue
		}
		for _, tableEntry := range tableEntries {
			if tableEntry.IsDir() || filepath.Ext(tableEntry.Name()) != ".frm" {
				continue
			}
			tableName := strings.TrimSuffix(tableEntry.Name(), ".frm")
			frmPath := filepath.Join(dataDir, schemaName, tableEntry.Name())
			tables = append(tables, frmMetadataTable{
				schemaName:       schemaName,
				tableName:        tableName,
				columns:          readFrmMetadataColumns(frmPath),
				indexes:          readFrmMetadataIndexes(frmPath),
				rowFormat:        readFrmMetadataRowFormat(frmPath),
				totalRowVersions: readFrmMetadataTotalRowVersions(frmPath),
			})
		}
	}
	return tables
}

func readFrmMetadataRowFormat(path string) string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	var tableInfo struct {
		Options map[string]interface{} `json:"options"`
	}
	if err := json.Unmarshal(raw, &tableInfo); err != nil {
		return ""
	}
	return persistedString(tableInfo.Options["row_format"])
}

func readFrmMetadataTotalRowVersions(path string) int {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	var tableInfo struct {
		TotalRowVersions interface{} `json:"total_row_versions"`
	}
	if err := json.Unmarshal(raw, &tableInfo); err != nil {
		return 0
	}
	value := int(persistedNumber(tableInfo.TotalRowVersions))
	if value < 0 {
		return 0
	}
	return value
}

func (e *XMySQLExecutor) scanViewTables(session server.MySQLServerSession) []frmMetadataTable {
	dataDir := e.getDataDir()
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil
	}

	tables := make([]frmMetadataTable, 0)
	for _, schemaEntry := range entries {
		if !schemaEntry.IsDir() {
			continue
		}
		viewEntries, err := os.ReadDir(filepath.Join(dataDir, schemaEntry.Name()))
		if err != nil {
			continue
		}
		for _, viewEntry := range viewEntries {
			if viewEntry.IsDir() || !strings.HasSuffix(strings.ToLower(viewEntry.Name()), ".view.json") {
				continue
			}
			viewName := strings.TrimSuffix(viewEntry.Name(), ".view.json")
			if _, visible := temporaryMetadataTableName(session, schemaEntry.Name(), viewName); !visible {
				continue
			}
			columns, err := e.viewColumnMetadata(schemaEntry.Name(), viewName)
			if err != nil {
				continue
			}
			frmColumns := make([]frmMetadataColumn, 0, len(columns))
			for _, column := range columns {
				if column == nil || strings.TrimSpace(column.Name) == "" {
					continue
				}
				frmColumns = append(frmColumns, frmMetadataColumn{
					name: column.Name, typeName: string(column.Type), length: column.Length,
					nullable: column.IsNullable, comment: column.Comment,
				})
			}
			tables = append(tables, frmMetadataTable{schemaName: schemaEntry.Name(), tableName: viewName, columns: frmColumns})
		}
	}
	return tables
}

func (e *XMySQLExecutor) viewColumnMetadata(schemaName, viewName string) ([]*metadata.ColumnMeta, error) {
	raw, err := os.ReadFile(filepath.Join(e.getDataDir(), schemaName, viewName+".view.json"))
	if err != nil {
		return nil, fmt.Errorf("view '%s.%s' does not exist: %w", schemaName, viewName, err)
	}
	var info struct {
		Definition string `json:"definition"`
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		return nil, fmt.Errorf("parse view metadata failed: %w", err)
	}
	definition := strings.TrimSpace(strings.TrimSuffix(info.Definition, ";"))
	if rewritten, rewriteErr := e.rewriteNestedCTESubqueries(definition); rewriteErr != nil {
		return nil, fmt.Errorf("rewrite nested view CTE failed: %w", rewriteErr)
	} else if strings.TrimSpace(rewritten) != "" {
		definition = rewritten
	}
	statement, parseErr := sqlparser.Parse(definition)
	if parseErr != nil {
		return nil, fmt.Errorf("parse view '%s.%s' definition failed: %w", schemaName, viewName, parseErr)
	}
	if _, ok := statement.(*sqlparser.Select); !ok {
		if withStmt, withOK := statement.(*sqlparser.With); !withOK || withStmt.Body == nil {
			return nil, fmt.Errorf("view '%s.%s' definition is not a SELECT", schemaName, viewName)
		}
	}
	results := make(chan *Result, 1)
	executionContext := &ExecutionContext{Context: context.Background(), Results: results, RawQuery: definition, DatabaseName: schemaName}
	e.executeQuery(executionContext, nil, definition, schemaName, results)
	executionResult := <-results
	if executionResult == nil {
		return nil, fmt.Errorf("load column metadata for %s.%s: empty execution result", schemaName, viewName)
	}
	if executionResult.Err != nil {
		return nil, fmt.Errorf("load column metadata for %s.%s: %w", schemaName, viewName, executionResult.Err)
	}
	result, ok := executionResult.Data.(*SelectResult)
	if !ok || result == nil {
		return nil, fmt.Errorf("load column metadata for %s.%s: unexpected result type %T", schemaName, viewName, executionResult.Data)
	}
	if result == nil || len(result.Columns) == 0 {
		return nil, fmt.Errorf("view '%s.%s' has no projected columns", schemaName, viewName)
	}
	var selectStmt *sqlparser.Select
	if statement, parseErr := sqlparser.Parse(definition); parseErr == nil {
		selectStmt, _ = statement.(*sqlparser.Select)
	}
	sourceColumns := viewSourceColumns(e, schemaName, selectStmt)
	columns := make([]*metadata.ColumnMeta, 0, len(result.Columns))
	for index, name := range result.Columns {
		columnName := strings.Trim(strings.TrimSpace(name), "`")
		if columnName == "" {
			columnName = fmt.Sprintf("column_%d", index+1)
		}
		typeName := string(metadata.TypeVarchar)
		if index < len(result.ColumnTypes) && strings.TrimSpace(result.ColumnTypes[index]) != "" {
			typeName = strings.ToUpper(strings.TrimSpace(result.ColumnTypes[index]))
		}
		var sourceColumn *metadata.ColumnMeta
		if index < len(sourceColumns) {
			sourceColumn = sourceColumns[index]
		}
		if sourceColumn != nil {
			typeName = string(sourceColumn.Type)
		}
		length := 0
		nullable := true
		comment := ""
		if sourceColumn != nil {
			length, nullable, comment = sourceColumn.Length, sourceColumn.IsNullable, sourceColumn.Comment
		}
		columns = append(columns, &metadata.ColumnMeta{
			Name: columnName, Type: metadata.DataType(typeName), Length: length, IsNullable: nullable, Comment: comment,
		})
	}
	return columns, nil
}

func viewSourceColumns(e *XMySQLExecutor, schemaName string, stmt *sqlparser.Select) []*metadata.ColumnMeta {
	if e == nil || stmt == nil || len(stmt.From) != 1 {
		return nil
	}
	tableExpr, ok := stmt.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil
	}
	tableName, ok := tableExpr.Expr.(sqlparser.TableName)
	if !ok {
		return nil
	}
	sourceSchema := tableName.Qualifier.String()
	if sourceSchema == "" {
		sourceSchema = schemaName
	}
	sourceName := tableName.Name.String()
	sourceMeta, err := e.getShowColumnsTableMetadata(sourceSchema, sourceName)
	if err != nil || sourceMeta == nil {
		return nil
	}
	sourceByName := make(map[string]*metadata.ColumnMeta, len(sourceMeta.Columns))
	for _, column := range sourceMeta.Columns {
		if column != nil {
			sourceByName[strings.ToLower(column.Name)] = column
		}
	}
	if len(stmt.SelectExprs) == 1 {
		if _, star := stmt.SelectExprs[0].(*sqlparser.StarExpr); star {
			return append([]*metadata.ColumnMeta(nil), sourceMeta.Columns...)
		}
	}
	columns := make([]*metadata.ColumnMeta, len(stmt.SelectExprs))
	for index, expression := range stmt.SelectExprs {
		aliased, ok := expression.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		column, ok := aliased.Expr.(*sqlparser.ColName)
		if ok {
			columns[index] = sourceByName[strings.ToLower(column.Name.String())]
		}
	}
	return columns
}

func (e *XMySQLExecutor) validateViewDefinitionSources(ctx *ExecutionContext, schemaName, viewName, definition string) error {
	validatedDefinition := definition
	if rewritten, rewriteErr := e.rewriteNestedCTESubqueries(definition); rewriteErr != nil {
		return fmt.Errorf("rewrite nested view CTE failed: %w", rewriteErr)
	} else if strings.TrimSpace(rewritten) != "" {
		validatedDefinition = rewritten
	}
	statement, err := sqlparser.Parse(validatedDefinition)
	if err != nil {
		return fmt.Errorf("parse view definition failed: %w", err)
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(validatedDefinition)), "with") {
		if err := validateCTEQuerySyntax(validatedDefinition); err != nil {
			return fmt.Errorf("invalid view CTE definition: %w", err)
		}
	}
	return e.validateViewStatementSources(ctx, schemaName, viewName, statement, nil)
}

func (e *XMySQLExecutor) validateViewStatementSources(ctx *ExecutionContext, schemaName, viewName string, statement sqlparser.Statement, cteNames map[string]struct{}) error {
	switch stmt := statement.(type) {
	case *sqlparser.Select:
		if err := e.checkSelectColumnPrivilegesForView(ctx, stmt, schemaName); err != nil {
			return err
		}
		for _, expression := range stmt.From {
			if err := e.validateViewTableExpr(ctx, schemaName, viewName, expression, cteNames); err != nil {
				return err
			}
		}
		return nil
	case *sqlparser.With:
		localCTEs := make(map[string]struct{}, len(cteNames)+len(stmt.CTEs))
		for name := range cteNames {
			localCTEs[name] = struct{}{}
		}
		for _, cte := range stmt.CTEs {
			if cte == nil || cte.Query == nil {
				return fmt.Errorf("view '%s.%s' contains an empty CTE definition", schemaName, viewName)
			}
			localCTEs[strings.ToLower(cte.Name.String())] = struct{}{}
		}
		for _, cte := range stmt.CTEs {
			if err := e.validateViewStatementSources(ctx, schemaName, viewName, cte.Query, localCTEs); err != nil {
				return err
			}
		}
		return e.validateViewStatementSources(ctx, schemaName, viewName, stmt.Body, localCTEs)
	default:
		return fmt.Errorf("view '%s.%s' definition is not a SELECT", schemaName, viewName)
	}
}

func (e *XMySQLExecutor) validateViewTableExpr(ctx *ExecutionContext, schemaName, viewName string, expression sqlparser.TableExpr, cteNames map[string]struct{}) error {
	switch table := expression.(type) {
	case *sqlparser.AliasedTableExpr:
		switch source := table.Expr.(type) {
		case sqlparser.TableName:
			sourceSchema := source.Qualifier.String()
			if sourceSchema == "" {
				sourceSchema = schemaName
			}
			sourceName := source.Name.String()
			if source.Qualifier.String() == "" {
				if _, isCTE := cteNames[strings.ToLower(sourceName)]; isCTE {
					return nil
				}
			}
			if strings.EqualFold(sourceSchema, schemaName) && strings.EqualFold(sourceName, viewName) {
				return fmt.Errorf("view '%s.%s' cannot reference itself", schemaName, viewName)
			}
			if strings.EqualFold(sourceName, "dual") {
				return nil
			}
			exists, err := e.checkTableOrViewExists(sourceSchema, sourceName)
			if err != nil {
				return err
			}
			if !exists {
				return fmt.Errorf("table or view '%s.%s' does not exist", sourceSchema, sourceName)
			}
			if ctx != nil {
				if err := e.checkTablePrivilege(ctx, sourceSchema, sourceName, "SELECT"); err != nil && !e.hasColumnSelectGrantForTable(ctx, sourceSchema, sourceName) {
					return err
				}
			}
		case *sqlparser.Subquery:
			nestedSelect, ok := source.Select.(*sqlparser.Select)
			if !ok || nestedSelect == nil {
				return fmt.Errorf("view '%s.%s' contains an empty derived source", schemaName, viewName)
			}
			if err := e.validateViewStatementSources(ctx, schemaName, viewName, nestedSelect, cteNames); err != nil {
				return err
			}
		}
	case *sqlparser.JoinTableExpr:
		if err := e.validateViewTableExpr(ctx, schemaName, viewName, table.LeftExpr, cteNames); err != nil {
			return err
		}
		return e.validateViewTableExpr(ctx, schemaName, viewName, table.RightExpr, cteNames)
	case *sqlparser.ParenTableExpr:
		for _, nested := range table.Exprs {
			if err := e.validateViewTableExpr(ctx, schemaName, viewName, nested, cteNames); err != nil {
				return err
			}
		}
	}
	return nil
}

func readFrmMetadataColumns(path string) []frmMetadataColumn {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var tableInfo struct {
		Columns []map[string]interface{} `json:"columns"`
	}
	if err := json.Unmarshal(raw, &tableInfo); err != nil {
		return nil
	}
	columns := make([]frmMetadataColumn, 0, len(tableInfo.Columns))
	for _, column := range tableInfo.Columns {
		name, _ := column["name"].(string)
		if name == "" {
			continue
		}
		typeName, _ := column["type"].(string)
		if typeName == "" {
			typeName = "varchar"
		}
		length := 0
		switch value := column["length"].(type) {
		case float64:
			length = int(value)
		case int:
			length = value
		}
		nullable := true
		if value, ok := column["nullable"].(bool); ok {
			nullable = value
		}
		columns = append(columns, frmMetadataColumn{
			name:                name,
			typeName:            typeName,
			length:              length,
			nullable:            nullable,
			comment:             persistedString(column["comment"]),
			autoIncrement:       persistedBool(column["auto_increment"]),
			defaultValue:        informationSchemaPersistedDefault(column["default"]),
			generatedExpression: informationSchemaPersistedNullableString(column["generated_expression"]),
			isPrimary:           persistedBool(column["primary"]),
			isUnique:            persistedBool(column["unique"]),
			instantAdded:        persistedBool(column["instant_added"]),
		})
	}
	return columns
}

func readFrmMetadataIndexes(path string) []frmMetadataIndex {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var tableInfo struct {
		Indexes []map[string]interface{} `json:"indexes"`
	}
	if err := json.Unmarshal(raw, &tableInfo); err != nil {
		return nil
	}
	indexes := make([]frmMetadataIndex, 0, len(tableInfo.Indexes))
	for _, index := range tableInfo.Indexes {
		if index == nil {
			continue
		}
		name := strings.TrimSpace(fmt.Sprint(index["name"]))
		if name == "" || name == "<nil>" {
			continue
		}
		columns := make([]string, 0)
		if rawColumns, ok := index["columns"].([]interface{}); ok {
			for _, rawColumn := range rawColumns {
				column := strings.Trim(strings.TrimSpace(fmt.Sprint(rawColumn)), "`")
				if column != "" && column != "<nil>" {
					columns = append(columns, column)
				}
			}
		}
		indexes = append(indexes, frmMetadataIndex{
			name: name, unique: persistedBool(index["unique"]),
			primary: persistedBool(index["primary"]), columns: columns,
		})
	}
	return indexes
}

func informationSchemaPersistedDefault(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	if text, ok := value.(string); ok && strings.EqualFold(strings.TrimSpace(text), "null") {
		return nil
	}
	return value
}

func informationSchemaPersistedNullableString(value interface{}) interface{} {
	text := strings.TrimSpace(fmt.Sprint(value))
	if value == nil || text == "" || text == "<nil>" {
		return nil
	}
	return text
}

func informationSchemaCharacterLength(column frmMetadataColumn) interface{} {
	if informationSchemaCharacterSet(column.typeName) == nil || column.length <= 0 {
		return nil
	}
	return int64(column.length)
}

func informationSchemaCharacterSet(typeName string) interface{} {
	base := strings.ToUpper(strings.TrimSpace(strings.SplitN(typeName, "(", 2)[0]))
	switch base {
	case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT":
		return "utf8mb4"
	default:
		return nil
	}
}

func informationSchemaCollation(typeName string) interface{} {
	if informationSchemaCharacterSet(typeName) == nil {
		return nil
	}
	return "utf8mb4_general_ci"
}

func persistedAutoIncrementValue(dataDir, schemaName, tableName string, columns []frmMetadataColumn) interface{} {
	for _, column := range columns {
		if !column.autoIncrement {
			continue
		}
		state, err := loadAutoIncrementState(dataDir)
		if err != nil {
			return nil
		}
		next := state[autoIncrementKey(schemaName, tableName, column.name)]
		if next == 0 {
			next = 1
		}
		return int64(next)
	}
	return nil
}

func persistedBool(value interface{}) bool {
	result, _ := value.(bool)
	return result
}

func informationSchemaMetadataFilters(query string) map[string]string {
	filters := make(map[string]string)
	for _, match := range informationSchemaMetadataFilterPattern.FindAllStringSubmatch(query, -1) {
		value := match[2]
		if value == "" {
			value = match[3]
		}
		filters[strings.ToLower(match[1])] = value
	}
	return filters
}

var informationSchemaMetadataInFilterPattern = regexp.MustCompile(`(?is)\b(table_schema|table_name|schema_name|column_name|constraint_name|constraint_type)\b\s+in\s*\(([^)]*)\)`)

func informationSchemaMetadataQueryMatches(query, column, value, equalityPattern string) bool {
	if strings.TrimSpace(equalityPattern) != "" && !metadataPatternMatches(value, equalityPattern) {
		return false
	}
	for _, match := range informationSchemaMetadataInFilterPattern.FindAllStringSubmatch(query, -1) {
		if !strings.EqualFold(match[1], column) {
			continue
		}
		matched := false
		for _, raw := range splitTopLevelComma(match[2]) {
			candidate := strings.Trim(strings.TrimSpace(raw), "'\"")
			if strings.EqualFold(candidate, value) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func metadataPatternMatches(value, pattern string) bool {
	if strings.TrimSpace(pattern) == "" {
		return true
	}
	quoted := regexp.QuoteMeta(pattern)
	quoted = strings.ReplaceAll(quoted, "%", ".*")
	quoted = strings.ReplaceAll(quoted, "_", ".")
	matched, err := regexp.MatchString("(?i)^"+quoted+"$", value)
	return err == nil && matched
}

func (e *XMySQLExecutor) executeInformationSchemaParametersSelect(query string, session server.MySQLServerSession) (*SelectResult, error) {
	lowerQuery := strings.ToLower(query)
	jdbcProjection := strings.Contains(lowerQuery, "procedure_cat") || strings.Contains(lowerQuery, "column_type") || strings.Contains(lowerQuery, "type_name") || strings.Contains(lowerQuery, "is_nullable")
	columns := requestedInformationSchemaColumns(query, []string{
		"SPECIFIC_CATALOG", "SPECIFIC_SCHEMA", "SPECIFIC_NAME", "ORDINAL_POSITION", "PARAMETER_MODE", "PARAMETER_NAME",
		"DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE",
		"DATETIME_PRECISION", "CHARACTER_SET_NAME", "COLLATION_NAME", "DTD_IDENTIFIER", "ROUTINE_TYPE",
	})
	filters := map[string]string{}
	filterPattern := regexp.MustCompile(`(?i)\b(?:specific_schema|specific_name|routine_schema|routine_name|specific_catalog)\b\s*(?:=|like)\s*(?:'([^']*)'|"([^"]*)")`)
	for _, match := range filterPattern.FindAllStringSubmatch(query, -1) {
		value := match[1]
		if value == "" {
			value = match[2]
		}
		key := strings.ToLower(strings.TrimSpace(strings.Fields(strings.ToLower(match[0]))[0]))
		filters[key] = value
	}
	rows := make([][]interface{}, 0)
	objects := append(e.scanStoredObjects("procedure"), e.scanStoredObjects("function")...)
	lockCtx := &ExecutionContext{Context: context.Background(), Session: session}
	releaseObjectLocks, lockErr := e.acquireStoredObjectReadLocks(lockCtx, session, objects, "")
	if lockErr != nil {
		return nil, lockErr
	}
	defer releaseObjectLocks()
	for _, object := range objects {
		if !metadataFilterMatches(object.Schema, filters["specific_schema"], filters["routine_schema"]) {
			continue
		}
		if !metadataFilterMatches(object.Name, filters["specific_name"], filters["routine_name"]) {
			continue
		}
		for index, parameter := range object.Parameters {
			mode, name, typeName := parseStoredRoutineParameterMetadata(parameter)
			dataType := interface{}(strings.ToLower(strings.SplitN(typeName, "(", 2)[0]))
			if jdbcProjection {
				dataType = fmt.Sprintf("%d", storedRoutineParameterDataType(typeName))
			}
			values := map[string]interface{}{
				"SPECIFIC_CATALOG": "def", "SPECIFIC_SCHEMA": object.Schema, "SPECIFIC_NAME": object.Name,
				"ORDINAL_POSITION": int64(index + 1), "PARAMETER_MODE": mode, "PARAMETER_NAME": name,
				"DATA_TYPE": dataType, "CHARACTER_MAXIMUM_LENGTH": nil,
				"CHARACTER_OCTET_LENGTH": nil, "NUMERIC_PRECISION": nil, "NUMERIC_SCALE": nil,
				"DATETIME_PRECISION": nil, "CHARACTER_SET_NAME": nil, "COLLATION_NAME": nil,
				"DTD_IDENTIFIER": typeName, "ROUTINE_TYPE": strings.ToUpper(object.ObjectType),
				"PROCEDURE_CAT": nil, "PROCEDURE_SCHEM": object.Schema, "PROCEDURE_NAME": object.Name,
				"COLUMN_NAME": name, "COLUMN_TYPE": fmt.Sprintf("%d", storedRoutineParameterColumnType(mode)),
				"TYPE_NAME": typeName,
				"PRECISION": nil, "LENGTH": nil, "SCALE": nil, "RADIX": int64(10),
				"NULLABLE": int64(1), "REMARKS": "", "COLUMN_DEF": nil,
				"SQL_DATA_TYPE": nil, "SQL_DATETIME_SUB": nil, "CHAR_OCTET_LENGTH": nil,
				"IS_NULLABLE": "YES",
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
		if strings.EqualFold(object.ObjectType, "function") {
			dataType := interface{}(strings.ToLower(strings.SplitN(object.ReturnType, "(", 2)[0]))
			if jdbcProjection {
				dataType = fmt.Sprintf("%d", storedRoutineParameterDataType(object.ReturnType))
			}
			values := map[string]interface{}{
				"SPECIFIC_CATALOG": "def", "SPECIFIC_SCHEMA": object.Schema, "SPECIFIC_NAME": object.Name,
				"ORDINAL_POSITION": int64(0), "PARAMETER_MODE": nil, "PARAMETER_NAME": "RETURN_VALUE",
				"DATA_TYPE": dataType, "CHARACTER_MAXIMUM_LENGTH": nil,
				"CHARACTER_OCTET_LENGTH": nil, "NUMERIC_PRECISION": nil, "NUMERIC_SCALE": nil,
				"DATETIME_PRECISION": nil, "CHARACTER_SET_NAME": nil, "COLLATION_NAME": nil,
				"DTD_IDENTIFIER": object.ReturnType, "ROUTINE_TYPE": "FUNCTION",
				"PROCEDURE_CAT": nil, "PROCEDURE_SCHEM": object.Schema, "PROCEDURE_NAME": object.Name,
				"COLUMN_NAME": "RETURN_VALUE", "COLUMN_TYPE": "5",
				"TYPE_NAME": strings.ToUpper(object.ReturnType),
				"PRECISION": nil, "LENGTH": nil, "SCALE": nil, "RADIX": int64(10),
				"NULLABLE": int64(1), "REMARKS": "", "COLUMN_DEF": nil,
				"SQL_DATA_TYPE": nil, "SQL_DATETIME_SUB": nil, "CHAR_OCTET_LENGTH": nil,
				"IS_NULLABLE": "YES",
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("information_schema_parameters", columns, rows), nil
}

func metadataFilterMatches(value string, patterns ...string) bool {
	for _, pattern := range patterns {
		if strings.TrimSpace(pattern) != "" {
			return metadataPatternMatches(value, pattern)
		}
	}
	return true
}

func parseStoredRoutineParameterMetadata(raw string) (mode, name, typeName string) {
	fields := strings.Fields(strings.TrimSpace(raw))
	if len(fields) == 0 {
		return "IN", "", ""
	}
	mode = "IN"
	start := 0
	if strings.EqualFold(fields[0], "IN") || strings.EqualFold(fields[0], "OUT") || strings.EqualFold(fields[0], "INOUT") {
		mode = strings.ToUpper(fields[0])
		start = 1
	}
	if start < len(fields) {
		name = strings.Trim(fields[start], "` ")
	}
	if start+1 < len(fields) {
		typeName = strings.ToUpper(strings.TrimSpace(fields[start+1]))
	}
	return mode, name, typeName
}

func storedRoutineParameterColumnType(mode string) int64 {
	switch strings.ToUpper(mode) {
	case "OUT":
		return 4
	case "INOUT":
		return 2
	default:
		return 1
	}
}

func storedRoutineParameterDataType(typeName string) int64 {
	base := strings.ToUpper(strings.TrimSpace(strings.SplitN(typeName, "(", 2)[0]))
	switch base {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER":
		return 4
	case "BIGINT":
		return -5
	case "DECIMAL", "NUMERIC":
		return 3
	case "FLOAT", "REAL":
		return 7
	case "DOUBLE":
		return 8
	case "DATE":
		return 91
	case "DATETIME", "TIMESTAMP":
		return 93
	case "TIME":
		return 92
	case "BINARY", "VARBINARY", "BLOB":
		return -3
	default:
		return 12
	}
}

func executeInformationSchemaCollationsSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{
		"COLLATION_NAME", "CHARACTER_SET_NAME", "ID", "IS_DEFAULT", "IS_COMPILED", "SORTLEN", "PAD_ATTRIBUTE",
	})
	rows := make([][]interface{}, 0, 4)
	filters := map[string]string{}
	filterPattern := regexp.MustCompile(`(?i)\b(collation_name|character_set_name|is_default)\b\s*(?:=|like)\s*(?:'([^']*)'|"([^"]*)")`)
	for _, match := range filterPattern.FindAllStringSubmatch(query, -1) {
		value := match[2]
		if value == "" {
			value = match[3]
		}
		filters[strings.ToLower(match[1])] = value
	}
	for _, collation := range []struct {
		name, charset, isDefault, isCompiled, padAttribute string
		id, sortLen                                        int64
	}{
		{"utf8mb4_0900_ai_ci", "utf8mb4", "YES", "YES", "PAD SPACE", 255, 1},
		{"utf8mb4_general_ci", "utf8mb4", "NO", "YES", "PAD SPACE", 45, 1},
		{"utf8mb4_bin", "utf8mb4", "NO", "YES", "PAD SPACE", 46, 1},
		{"latin1_swedish_ci", "latin1", "YES", "YES", "PAD SPACE", 8, 1},
	} {
		if !metadataFilterMatches(collation.name, filters["collation_name"]) ||
			!metadataFilterMatches(collation.charset, filters["character_set_name"]) ||
			(strings.TrimSpace(filters["is_default"]) != "" && !strings.EqualFold(collation.isDefault, filters["is_default"])) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, map[string]interface{}{
			"COLLATION_NAME": collation.name, "CHARACTER_SET_NAME": collation.charset, "ID": collation.id,
			"IS_DEFAULT": collation.isDefault, "IS_COMPILED": collation.isCompiled, "SORTLEN": collation.sortLen,
			"PAD_ATTRIBUTE": collation.padAttribute,
		}))
	}
	return newInformationSchemaSelectResult("information_schema_collations", columns, rows)
}

func executeInformationSchemaCharacterSetsSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{"CHARACTER_SET_NAME", "DEFAULT_COLLATE_NAME", "DESCRIPTION", "MAXLEN"})
	rows := make([][]interface{}, 0, 3)
	filter := regexp.MustCompile(`(?i)\bcharacter_set_name\b\s*(?:=|like)\s*(?:'([^']*)'|"([^"]*)")`).FindStringSubmatch(query)
	pattern := ""
	if len(filter) == 3 {
		pattern = filter[1]
		if pattern == "" {
			pattern = filter[2]
		}
	}
	for _, charset := range []struct {
		name, collation, description string
		maxLen                       int64
	}{
		{"utf8mb4", "utf8mb4_0900_ai_ci", "UTF-8 Unicode", 4},
		{"utf8", "utf8_general_ci", "UTF-8 Unicode", 3},
		{"latin1", "latin1_swedish_ci", "cp1252 West European", 1},
	} {
		if !metadataFilterMatches(charset.name, pattern) {
			continue
		}
		values := map[string]interface{}{
			"CHARACTER_SET_NAME": charset.name, "DEFAULT_COLLATE_NAME": charset.collation, "DESCRIPTION": charset.description, "MAXLEN": fmt.Sprintf("%d", charset.maxLen),
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("information_schema_character_sets", columns, rows)
}

func executeInformationSchemaEnginesSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{"ENGINE", "SUPPORT", "COMMENT", "TRANSACTIONS", "XA", "SAVEPOINTS"})
	values := map[string]interface{}{
		"ENGINE": "InnoDB", "SUPPORT": "DEFAULT", "COMMENT": "Supports transactions, row-level locking, and foreign keys",
		"TRANSACTIONS": "YES", "XA": "YES", "SAVEPOINTS": "YES",
	}
	filters := make(map[string]string)
	filterPattern := regexp.MustCompile(`(?i)\b(engine|support|comment|transactions|xa|savepoints)\b\s*(?:=|like)\s*(?:'([^']*)'|"([^"]*)")`)
	for _, match := range filterPattern.FindAllStringSubmatch(query, -1) {
		value := match[2]
		if value == "" {
			value = match[3]
		}
		filters[strings.ToUpper(match[1])] = value
	}
	for column, pattern := range filters {
		if !metadataFilterMatches(fmt.Sprint(values[column]), pattern) {
			return newInformationSchemaSelectResult("information_schema_engines", columns, nil)
		}
	}
	rows := [][]interface{}{projectInformationSchemaRow(columns, values)}
	return newInformationSchemaSelectResult("information_schema_engines", columns, rows)
}

func executeInformationSchemaPluginsSelect(query string) *SelectResult {
	defaults := []string{"PLUGIN_NAME", "PLUGIN_VERSION", "PLUGIN_STATUS", "PLUGIN_TYPE", "PLUGIN_TYPE_VERSION", "PLUGIN_LIBRARY", "PLUGIN_LIBRARY_VERSION", "PLUGIN_AUTHOR", "PLUGIN_DESCRIPTION", "PLUGIN_LICENSE", "LOAD_OPTION"}
	columns := requestedInformationSchemaColumns(query, defaults)
	plugins := []map[string]interface{}{
		{
			"PLUGIN_NAME": "InnoDB", "PLUGIN_VERSION": "1.0", "PLUGIN_STATUS": "ACTIVE", "PLUGIN_TYPE": "STORAGE ENGINE", "PLUGIN_TYPE_VERSION": "8.4",
			"PLUGIN_LIBRARY": nil, "PLUGIN_LIBRARY_VERSION": nil, "PLUGIN_AUTHOR": "Oracle Corporation", "PLUGIN_DESCRIPTION": "Supports transactions, row-level locking, and foreign keys", "PLUGIN_LICENSE": "GPL", "LOAD_OPTION": "DEFAULT", "PLUGIN_INITIALIZATION": nil, "PLUGIN_DEINITIALIZATION": nil, "PLUGIN_MATURITY": "GA",
		},
		{
			"PLUGIN_NAME": "mysql_native_password", "PLUGIN_VERSION": "1.1", "PLUGIN_STATUS": "ACTIVE", "PLUGIN_TYPE": "AUTHENTICATION", "PLUGIN_TYPE_VERSION": "8.4",
			"PLUGIN_LIBRARY": nil, "PLUGIN_LIBRARY_VERSION": nil, "PLUGIN_AUTHOR": "Oracle Corporation", "PLUGIN_DESCRIPTION": "Native MySQL authentication", "PLUGIN_LICENSE": "GPL", "LOAD_OPTION": "ON", "PLUGIN_INITIALIZATION": nil, "PLUGIN_DEINITIALIZATION": nil, "PLUGIN_MATURITY": "GA",
		},
		{
			"PLUGIN_NAME": "caching_sha2_password", "PLUGIN_VERSION": "1.0", "PLUGIN_STATUS": "ACTIVE", "PLUGIN_TYPE": "AUTHENTICATION", "PLUGIN_TYPE_VERSION": "8.4",
			"PLUGIN_LIBRARY": nil, "PLUGIN_LIBRARY_VERSION": nil, "PLUGIN_AUTHOR": "Oracle Corporation", "PLUGIN_DESCRIPTION": "Caching SHA-2 authentication", "PLUGIN_LICENSE": "GPL", "LOAD_OPTION": "ON", "PLUGIN_INITIALIZATION": nil, "PLUGIN_DEINITIALIZATION": nil, "PLUGIN_MATURITY": "GA",
		},
		{
			"PLUGIN_NAME": "sha256_password", "PLUGIN_VERSION": "1.1", "PLUGIN_STATUS": "ACTIVE", "PLUGIN_TYPE": "AUTHENTICATION", "PLUGIN_TYPE_VERSION": "8.4",
			"PLUGIN_LIBRARY": nil, "PLUGIN_LIBRARY_VERSION": nil, "PLUGIN_AUTHOR": "Oracle Corporation", "PLUGIN_DESCRIPTION": "SHA-256 authentication", "PLUGIN_LICENSE": "GPL", "LOAD_OPTION": "ON", "PLUGIN_INITIALIZATION": nil, "PLUGIN_DEINITIALIZATION": nil, "PLUGIN_MATURITY": "GA",
		},
	}
	rows := make([][]interface{}, 0, len(plugins))
	for _, plugin := range plugins {
		if !performanceSchemaLockValuesMatch(query, plugin) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, plugin))
	}
	return newInformationSchemaSelectResult("information_schema.plugins", columns, rows)
}

func jdbcRoutinesMetadataColumns(query string) []string {
	lower := strings.ToLower(query)
	if strings.Contains(lower, " as function_type") || strings.Contains(lower, "function_name") {
		return []string{"FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME"}
	}
	return []string{"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "RESERVED_1", "RESERVED_2", "RESERVED_3", "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME"}
}

func jdbcParametersMetadataColumns() []string {
	return []string{
		"PROCEDURE_CAT",
		"PROCEDURE_SCHEM",
		"PROCEDURE_NAME",
		"COLUMN_NAME",
		"COLUMN_TYPE",
		"DATA_TYPE",
		"TYPE_NAME",
		"PRECISION",
		"LENGTH",
		"SCALE",
		"RADIX",
		"NULLABLE",
		"REMARKS",
		"COLUMN_DEF",
		"SQL_DATA_TYPE",
		"SQL_DATETIME_SUB",
		"CHAR_OCTET_LENGTH",
		"ORDINAL_POSITION",
		"IS_NULLABLE",
		"SPECIFIC_NAME",
	}
}

func jdbcStatisticsMetadataColumns(query string) []string {
	lower := strings.ToLower(query)
	if strings.Contains(lower, " as pk_name") {
		return []string{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"}
	}
	return []string{
		"TABLE_CAT",
		"TABLE_SCHEM",
		"TABLE_NAME",
		"NON_UNIQUE",
		"INDEX_QUALIFIER",
		"INDEX_NAME",
		"TYPE",
		"ORDINAL_POSITION",
		"COLUMN_NAME",
		"ASC_OR_DESC",
		"CARDINALITY",
		"PAGES",
		"FILTER_CONDITION",
	}
}

func jdbcKeyColumnUsageMetadataColumns() []string {
	return []string{
		"PKTABLE_CAT",
		"PKTABLE_SCHEM",
		"PKTABLE_NAME",
		"PKCOLUMN_NAME",
		"FKTABLE_CAT",
		"FKTABLE_SCHEM",
		"FKTABLE_NAME",
		"FKCOLUMN_NAME",
		"KEY_SEQ",
		"UPDATE_RULE",
		"DELETE_RULE",
		"FK_NAME",
		"PK_NAME",
		"DEFERRABILITY",
	}
}

func jdbcTableConstraintsMetadataColumns() []string {
	return []string{"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_TYPE"}
}

func jdbcReferentialConstraintsMetadataColumns() []string {
	return []string{"CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_NAME", "REFERENCED_TABLE_NAME", "UPDATE_RULE", "DELETE_RULE"}
}

func jdbcColumnPrivilegesMetadataColumns() []string {
	return []string{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"}
}

func jdbcTablePrivilegesMetadataColumns() []string {
	return []string{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"}
}

func jdbcViewsMetadataColumns() []string {
	return []string{"TABLE_NAME", "VIEW_DEFINITION", "DEFINER"}
}

func jdbcPartitionsMetadataColumns() []string {
	return []string{
		"TABLE_NAME",
		"PARTITION_NAME",
		"SUBPARTITION_NAME",
		"PARTITION_ORDINAL_POSITION",
		"SUBPARTITION_ORDINAL_POSITION",
		"PARTITION_METHOD",
		"SUBPARTITION_METHOD",
		"PARTITION_EXPRESSION",
		"SUBPARTITION_EXPRESSION",
		"PARTITION_DESCRIPTION",
		"TABLE_ROWS",
		"AVG_ROW_LENGTH",
		"DATA_LENGTH",
		"MAX_DATA_LENGTH",
		"INDEX_LENGTH",
		"DATA_FREE",
		"CREATE_TIME",
		"UPDATE_TIME",
		"CHECK_TIME",
		"CHECKSUM",
		"PARTITION_COMMENT",
		"NODEGROUP",
		"TABLESPACE_NAME",
	}
}

func jdbcTriggersMetadataColumns() []string {
	return []string{"TRIGGER_NAME", "EVENT_MANIPULATION", "EVENT_OBJECT_TABLE", "ACTION_STATEMENT", "ACTION_TIMING", "DEFINER"}
}

func jdbcEventsMetadataColumns() []string {
	return []string{"EVENT_NAME", "EVENT_DEFINITION", "EVENT_TYPE", "EXECUTE_AT", "INTERVAL_VALUE", "INTERVAL_FIELD", "STATUS", "DEFINER"}
}

func jdbcCollationsMetadataColumns() []string {
	return []string{"COLLATION_NAME", "CHARACTER_SET_NAME", "IS_DEFAULT"}
}

func jdbcUserPrivilegesMetadataColumns() []string {
	return []string{"GRANTEE", "PRIVILEGE_TYPE", "IS_GRANTABLE"}
}

func jdbcSchemaPrivilegesMetadataColumns() []string {
	return []string{"GRANTEE", "TABLE_SCHEMA", "PRIVILEGE_TYPE", "IS_GRANTABLE"}
}

func jdbcMySQLProcsPrivMetadataColumns() []string {
	return []string{"HOST", "USER", "ROUTINE_NAME", "PROC_PRIV", "IS_PROC"}
}

func newInformationSchemaSelectResult(name string, columns []string, rows [][]interface{}) *SelectResult {
	tableMeta := &metadata.TableMeta{Name: name, Columns: make([]*metadata.ColumnMeta, 0, len(columns))}
	columnTypes := make([]string, 0, len(columns))
	for columnIndex, column := range columns {
		columnType := informationSchemaColumnType(rows, columnIndex)
		tableMeta.Columns = append(tableMeta.Columns, &metadata.ColumnMeta{Name: column, Type: columnType})
		columnTypes = append(columnTypes, string(columnType))
	}
	records := make([]Record, 0, len(rows))
	for _, row := range rows {
		records = append(records, NewExecutorRecordFromInterface(row, tableMeta))
	}
	return &SelectResult{
		Records:     records,
		RowCount:    len(records),
		Columns:     columns,
		ColumnTypes: columnTypes,
		ResultType:  common.RESULT_TYPE_QUERY,
		Message:     fmt.Sprintf("SELECT query executed successfully, %d rows returned", len(records)),
	}
}

func informationSchemaColumnType(rows [][]interface{}, columnIndex int) metadata.DataType {
	for _, row := range rows {
		if columnIndex < 0 || columnIndex >= len(row) || row[columnIndex] == nil {
			continue
		}
		switch row[columnIndex].(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return metadata.TypeBigInt
		case float32, float64:
			return metadata.TypeDouble
		case bool:
			return metadata.TypeTinyInt
		default:
			return metadata.TypeVarchar
		}
	}
	return metadata.TypeVarchar
}

// generateLogicalPlan 从SQL生成逻辑计划
func (e *XMySQLExecutor) generateLogicalPlan(stmt *sqlparser.Select, databaseName string) (plan.LogicalPlan, error) {
	if stmt == nil {
		return nil, NewExecutionErrorWithCause(
			"engine",
			"generate-logical-plan",
			ExecutionErrorCodeValidation,
			"",
			"",
			"",
			0,
			fmt.Errorf("select statement is nil"),
			"logical plan generation failed",
		)
	}

	// 简化回退：优先返回最小可执行的表扫描计划
	tableName := ""
	if stmt.From != nil && len(stmt.From) > 0 {
		if expr, ok := stmt.From[0].(*sqlparser.AliasedTableExpr); ok {
			if tableExpr, ok := expr.Expr.(sqlparser.TableName); ok {
				tableName = tableExpr.Name.String()
			}
		}
	}
	if tableName == "" {
		tableName = "unknown"
	}

	return &plan.LogicalTableScan{
		BaseLogicalPlan: plan.BaseLogicalPlan{},
		Table: &metadata.Table{
			Name: tableName,
		},
	}, nil
}

type showLogicalPlan struct {
	plan.BaseLogicalPlan
	showType string
}

func (s *showLogicalPlan) String() string {
	if s == nil {
		return "SHOW"
	}
	return fmt.Sprintf("SHOW %s", strings.ToUpper(s.showType))
}

// optimizeToPhysicalPlan 逻辑计划优化为物理计划
func (e *XMySQLExecutor) optimizeToPhysicalPlan(logicalPlan plan.LogicalPlan) (plan.PhysicalPlan, error) {
	// 获取优化器管理器
	var optimizerManager *manager.OptimizerManager
	if e.optimizerManager != nil {
		if om, ok := e.optimizerManager.(*manager.OptimizerManager); ok {
			optimizerManager = om
		}
	}

	if optimizerManager == nil {
		return nil, NewExecutionErrorWithCause(
			"engine",
			"optimize-physical-plan",
			ExecutionErrorCodeOptimizer,
			"",
			"",
			"",
			0,
			fmt.Errorf("optimizerManager is nil"),
			"cannot optimize to physical plan",
		)
	}

	logger.Debugf("🔧 开始物理计划优化...")

	// 使用优化器管理器生成物理计划
	physicalPlan, err := e.generatePhysicalPlan(logicalPlan, optimizerManager)
	if err != nil {
		return nil, newExecutorErrorf("optimize-physical-plan", ExecutionErrorCodeOptimizer, "", "", "", err, "failed to generate physical plan")
	}

	logger.Debugf("✅ 物理计划优化完成")
	return physicalPlan, nil
}

// generatePhysicalPlan 生成物理计划
func (e *XMySQLExecutor) generatePhysicalPlan(logicalPlan plan.LogicalPlan, optimizerManager *manager.OptimizerManager) (plan.PhysicalPlan, error) {
	if logicalPlan == nil {
		return nil, fmt.Errorf("cannot generate physical plan from nil logical plan")
	}

	// 根据逻辑计划类型生成对应的物理计划
	switch lp := logicalPlan.(type) {
	case *plan.LogicalTableScan:
		return e.generatePhysicalTableScan(lp, optimizerManager)

	case *plan.LogicalIndexScan:
		return e.generatePhysicalIndexScan(lp, optimizerManager)

	case *plan.LogicalJoin:
		return e.generatePhysicalJoin(lp, optimizerManager)

	case *plan.LogicalAggregation:
		return e.generatePhysicalAggregation(lp, optimizerManager)

	case *plan.LogicalProjection:
		return e.generatePhysicalProjection(lp, optimizerManager)

	case *plan.LogicalSelection:
		return e.generatePhysicalSelection(lp, optimizerManager)

	default:
		// Keep all supported logical nodes on the same conversion path and make
		// genuinely unknown nodes fail explicitly. Returning an empty table scan
		// here would silently change the query result.
		physical := plan.ConvertToPhysicalPlan(logicalPlan)
		if physical == nil {
			return nil, fmt.Errorf("unsupported logical plan type: %T", logicalPlan)
		}
		return physical, nil
	}
}

// generatePhysicalTableScan 生成物理表扫描计划
func (e *XMySQLExecutor) generatePhysicalTableScan(lp *plan.LogicalTableScan, om *manager.OptimizerManager) (plan.PhysicalPlan, error) {
	logger.Debugf("生成物理表扫描计划: table=%s", lp.Table.Name)

	physical := &plan.PhysicalTableScan{
		BasePhysicalPlan: plan.BasePhysicalPlan{},
		Table:            lp.Table,
		RequiredColumns:  plan.RequiredColumnNames(lp.Table, lp.Schema()),
	}
	physical.SetSchema(lp.Schema())
	return physical, nil
}

// generatePhysicalIndexScan 生成物理索引扫描计划
func (e *XMySQLExecutor) generatePhysicalIndexScan(lp *plan.LogicalIndexScan, om *manager.OptimizerManager) (plan.PhysicalPlan, error) {
	logger.Debugf("生成物理索引扫描计划: table=%s, index=%s", lp.Table.Name, lp.Index.Name)

	// 将plan.Index转换为metadata.Index
	metadataIndex := &metadata.Index{
		Name:     lp.Index.Name,
		Columns:  lp.Index.Columns,
		IsUnique: lp.Index.Unique,
	}

	physical := &plan.PhysicalIndexScan{
		BasePhysicalPlan: plan.BasePhysicalPlan{},
		Table:            lp.Table,
		Index:            metadataIndex,
		RequiredColumns:  plan.RequiredColumnNames(lp.Table, lp.Schema()),
	}
	physical.SetSchema(lp.Schema())
	return physical, nil
}

// generatePhysicalJoin 生成物理连接计划
func (e *XMySQLExecutor) generatePhysicalJoin(lp *plan.LogicalJoin, om *manager.OptimizerManager) (plan.PhysicalPlan, error) {
	logger.Debugf("生成物理连接计划: type=%s", lp.JoinType)

	// 递归生成左右子计划
	children := lp.Children()
	if len(children) < 2 {
		return nil, NewExecutionErrorWithCause("engine", "generate-physical-plan", ExecutionErrorCodeValidation, "", "", "", 0,
			fmt.Errorf("join plan needs at least 2 children"), "join plan invalid")
	}

	leftPlan, err := e.generatePhysicalPlan(children[0], om)
	if err != nil {
		return nil, newExecutorErrorf("generate-physical-plan", ExecutionErrorCodeOptimizer, "", "", "", err, "failed to generate left plan")
	}

	rightPlan, err := e.generatePhysicalPlan(children[1], om)
	if err != nil {
		return nil, newExecutorErrorf("generate-physical-plan", ExecutionErrorCodeOptimizer, "", "", "", err, "failed to generate right plan")
	}

	// 选择连接算法（Hash Join, Nested Loop Join, Sort-Merge Join）
	joinAlgorithm := e.chooseJoinAlgorithm(lp, leftPlan, rightPlan, om)

	switch joinAlgorithm {
	case "hash":
		physical := &plan.PhysicalHashJoin{
			BasePhysicalPlan: plan.BasePhysicalPlan{},
			JoinType:         lp.JoinType,
			Conditions:       lp.Conditions,
			LeftSchema:       lp.LeftSchema,
			RightSchema:      lp.RightSchema,
		}
		physical.SetChildren([]plan.PhysicalPlan{leftPlan, rightPlan})
		return physical, nil

	case "merge":
		physical := &plan.PhysicalMergeJoin{
			BasePhysicalPlan: plan.BasePhysicalPlan{},
			JoinType:         lp.JoinType,
			Conditions:       lp.Conditions,
			LeftSchema:       lp.LeftSchema,
			RightSchema:      lp.RightSchema,
		}
		physical.SetChildren([]plan.PhysicalPlan{leftPlan, rightPlan})
		return physical, nil

	default:
		// 默认使用Hash Join
		physical := &plan.PhysicalHashJoin{
			BasePhysicalPlan: plan.BasePhysicalPlan{},
			JoinType:         lp.JoinType,
			Conditions:       lp.Conditions,
			LeftSchema:       lp.LeftSchema,
			RightSchema:      lp.RightSchema,
		}
		physical.SetChildren([]plan.PhysicalPlan{leftPlan, rightPlan})
		return physical, nil
	}
}

// generatePhysicalAggregation 生成物理聚合计划
func (e *XMySQLExecutor) generatePhysicalAggregation(lp *plan.LogicalAggregation, om *manager.OptimizerManager) (plan.PhysicalPlan, error) {
	logger.Debugf("生成物理聚合计划")

	// 递归生成子计划
	children := lp.Children()
	if len(children) == 0 {
		return nil, NewExecutionErrorWithCause("engine", "generate-physical-plan", ExecutionErrorCodeValidation, "", "", "", 0,
			fmt.Errorf("aggregation plan has no child"), "aggregation plan has no child")
	}

	childPlan, err := e.generatePhysicalPlan(children[0], om)
	if err != nil {
		return nil, newExecutorErrorf("generate-physical-plan", ExecutionErrorCodeOptimizer, "", "", "", err, "failed to generate child plan")
	}

	// 选择聚合算法（Hash Aggregate, Sort Aggregate）
	aggAlgorithm := e.chooseAggregateAlgorithm(lp, childPlan, om)

	switch aggAlgorithm {
	case "hash":
		physical := &plan.PhysicalHashAgg{
			BasePhysicalPlan: plan.BasePhysicalPlan{},
			GroupByItems:     lp.GroupByItems,
			AggFuncs:         lp.AggFuncs,
		}
		physical.SetChildren([]plan.PhysicalPlan{childPlan})
		return physical, nil

	case "stream":
		physical := &plan.PhysicalStreamAgg{
			BasePhysicalPlan: plan.BasePhysicalPlan{},
			GroupByItems:     lp.GroupByItems,
			AggFuncs:         lp.AggFuncs,
		}
		physical.SetChildren([]plan.PhysicalPlan{childPlan})
		return physical, nil

	default:
		// 默认使用Hash Aggregate
		physical := &plan.PhysicalHashAgg{
			BasePhysicalPlan: plan.BasePhysicalPlan{},
			GroupByItems:     lp.GroupByItems,
			AggFuncs:         lp.AggFuncs,
		}
		physical.SetChildren([]plan.PhysicalPlan{childPlan})
		return physical, nil
	}
}

// generatePhysicalProjection 生成物理投影计划
func (e *XMySQLExecutor) generatePhysicalProjection(lp *plan.LogicalProjection, om *manager.OptimizerManager) (plan.PhysicalPlan, error) {
	logger.Debugf("生成物理投影计划")

	// 递归生成子计划
	children := lp.Children()
	if len(children) == 0 {
		return nil, NewExecutionErrorWithCause("engine", "generate-physical-plan", ExecutionErrorCodeValidation, "", "", "", 0,
			fmt.Errorf("projection plan has no child"), "projection plan has no child")
	}

	childPlan, err := e.generatePhysicalPlan(children[0], om)
	if err != nil {
		return nil, newExecutorErrorf("generate-physical-plan", ExecutionErrorCodeOptimizer, "", "", "", err, "failed to generate child plan")
	}

	physical := &plan.PhysicalProjection{
		BasePhysicalPlan: plan.BasePhysicalPlan{},
		Exprs:            lp.Exprs,
		OutputNames:      lp.OutputNames,
		Distinct:         lp.Distinct,
		OrderBy:          lp.OrderBy,
		Offset:           lp.Offset,
		Limit:            lp.Limit,
		HasLimit:         lp.HasLimit,
	}
	physical.SetChildren([]plan.PhysicalPlan{childPlan})
	return physical, nil
}

// generatePhysicalSelection 生成物理选择计划
func (e *XMySQLExecutor) generatePhysicalSelection(lp *plan.LogicalSelection, om *manager.OptimizerManager) (plan.PhysicalPlan, error) {
	logger.Debugf("生成物理选择计划")

	// 递归生成子计划
	children := lp.Children()
	if len(children) == 0 {
		return nil, NewExecutionErrorWithCause("engine", "generate-physical-plan", ExecutionErrorCodeValidation, "", "", "", 0,
			fmt.Errorf("selection plan has no child"), "selection plan has no child")
	}

	childPlan, err := e.generatePhysicalPlan(children[0], om)
	if err != nil {
		return nil, newExecutorErrorf("generate-physical-plan", ExecutionErrorCodeOptimizer, "", "", "", err, "failed to generate child plan")
	}

	physical := &plan.PhysicalSelection{
		BasePhysicalPlan: plan.BasePhysicalPlan{},
		Conditions:       lp.Conditions,
	}
	physical.SetChildren([]plan.PhysicalPlan{childPlan})
	return physical, nil
}

// generatePhysicalSort 生成物理排序计划
func (e *XMySQLExecutor) generatePhysicalSort(lp *plan.BaseLogicalPlan, om *manager.OptimizerManager) (plan.PhysicalPlan, error) {
	logger.Debugf("生成物理排序计划")

	// 递归生成子计划
	children := lp.Children()
	if len(children) == 0 {
		return nil, NewExecutionErrorWithCause("engine", "generate-physical-plan", ExecutionErrorCodeValidation, "", "", "", 0,
			fmt.Errorf("sort plan has no child"), "sort plan has no child")
	}

	childPlan, err := e.generatePhysicalPlan(children[0], om)
	if err != nil {
		return nil, newExecutorErrorf("generate-physical-plan", ExecutionErrorCodeOptimizer, "", "", "", err, "failed to generate child plan")
	}

	physical := &plan.PhysicalSort{
		BasePhysicalPlan: plan.BasePhysicalPlan{},
		ByItems:          []plan.ByItem{}, // 简化：空排序项
	}
	physical.SetChildren([]plan.PhysicalPlan{childPlan})
	return physical, nil
}

// generatePhysicalLimit 生成物理限制计划
func (e *XMySQLExecutor) generatePhysicalLimit(lp *plan.BaseLogicalPlan, om *manager.OptimizerManager) (plan.PhysicalPlan, error) {
	logger.Debugf("生成物理限制计划")

	// 递归生成子计划
	children := lp.Children()
	if len(children) == 0 {
		return nil, NewExecutionErrorWithCause("engine", "generate-physical-plan", ExecutionErrorCodeValidation, "", "", "", 0,
			fmt.Errorf("limit plan has no child"), "limit plan has no child")
	}

	childPlan, err := e.generatePhysicalPlan(children[0], om)
	if err != nil {
		return nil, newExecutorErrorf("generate-physical-plan", ExecutionErrorCodeOptimizer, "", "", "", err, "failed to generate child plan")
	}

	// 简化：直接返回子计划，limit逻辑在执行时处理
	return childPlan, nil
}

// chooseJoinAlgorithm 选择连接算法
func (e *XMySQLExecutor) chooseJoinAlgorithm(lp *plan.LogicalJoin, leftPlan, rightPlan plan.PhysicalPlan, om *manager.OptimizerManager) string {
	// 简化实现：基于表大小选择算法
	// 实际应该使用代价估算

	// 如果有等值连接条件，优先使用Hash Join
	if e.hasEquiJoinCondition(lp.Conditions) {
		return "hash"
	}

	// 否则使用Nested Loop Join
	return "nested_loop"
}

// chooseAggregateAlgorithm 选择聚合算法
func (e *XMySQLExecutor) chooseAggregateAlgorithm(lp *plan.LogicalAggregation, childPlan plan.PhysicalPlan, om *manager.OptimizerManager) string {
	// 简化实现：默认使用Hash Aggregate
	// 实际应该使用代价估算

	// 如果有GROUP BY，使用Hash Aggregate
	if len(lp.GroupByItems) > 0 {
		return "hash"
	}

	// 否则使用Stream Aggregate
	return "stream"
}

// hasEquiJoinCondition 检查是否有等值连接条件
func (e *XMySQLExecutor) hasEquiJoinCondition(conditions []plan.Expression) bool {
	for _, cond := range conditions {
		if e.hasEquiCondition(cond) {
			return true
		}
	}
	return false
}

func (e *XMySQLExecutor) hasEquiCondition(expr plan.Expression) bool {
	if expr == nil {
		return false
	}

	switch v := expr.(type) {
	case *plan.BinaryOperation:
		if v.Op == plan.OpEQ || v.Operator == "=" {
			return true
		}
		return e.hasEquiCondition(v.Left) || e.hasEquiCondition(v.Right)

	case *plan.Function:
		for _, arg := range v.Args() {
			if e.hasEquiCondition(arg) {
				return true
			}
		}
		return false

	case *plan.InExpression:
		return e.hasEquiCondition(v.Column)

	case *plan.BetweenExpression:
		return e.hasEquiCondition(v.Column)

	case *plan.LikeExpression:
		return e.hasEquiCondition(v.Column)

	case *plan.IsNullExpression:
		return e.hasEquiCondition(v.Column)

	default:
		return e.hasOrTraverseChildren(expr)
	}
}

func (e *XMySQLExecutor) hasOrTraverseChildren(expr plan.Expression) bool {
	if expr == nil {
		return false
	}
	for _, child := range expr.Children() {
		if e.hasEquiCondition(child) {
			return true
		}
	}
	return false
}

// convertToSelectResult 将Record数组转换为SelectResult
func (e *XMySQLExecutor) convertToSelectResult(records []Record, schema *metadata.Table) (*SelectResult, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema is nil")
	}

	// 构建列名和类型
	columnNames := make([]string, 0, len(schema.Columns))
	columnTypes := make([]string, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		columnNames = append(columnNames, col.Name)
		columnTypes = append(columnTypes, strings.ToLower(string(col.DataType)))
	}

	// 转换记录为行数据
	rows := make([][]interface{}, 0, len(records))
	for _, record := range records {
		values := record.GetValues()
		row := make([]interface{}, len(values))
		for i, v := range values {
			row[i] = e.convertValueToInterface(v)
		}
		rows = append(rows, row)
	}

	return &SelectResult{
		Records:     records,
		RowCount:    len(rows),
		Columns:     columnNames,
		ColumnTypes: columnTypes,
	}, nil
}

// convertValueToInterface 将basic.Value转换为interface{}
func (e *XMySQLExecutor) convertValueToInterface(value basic.Value) interface{} {
	if value.IsNull() {
		return nil
	}

	// 使用Value interface的方法
	switch value.Type() {
	case basic.ValueTypeBigInt, basic.ValueTypeInt, basic.ValueTypeMediumInt, basic.ValueTypeSmallInt, basic.ValueTypeTinyInt:
		return value.Int()
	case basic.ValueTypeFloat, basic.ValueTypeDouble:
		return value.Float64()
	case basic.ValueTypeChar, basic.ValueTypeVarchar, basic.ValueTypeText, basic.ValueTypeMediumText, basic.ValueTypeLongText:
		return value.String()
	case basic.ValueTypeBinary, basic.ValueTypeVarBinary, basic.ValueTypeBlob, basic.ValueTypeMediumBlob, basic.ValueTypeLongBlob:
		return value.Bytes()
	case basic.ValueTypeDate, basic.ValueTypeTime, basic.ValueTypeDateTime, basic.ValueTypeTimestamp:
		return value.Time()
	default:
		return value.Raw()
	}
}

// executeInsertStatement 执行 INSERT 语句
func (e *XMySQLExecutor) executeInsertStatement(ctx *ExecutionContext, stmt *sqlparser.Insert, databaseName string, session server.MySQLServerSession) (*DMLResult, error) {
	if err := rejectReadOnlyDMLForQuery(session, "INSERT", sqlparser.String(stmt), databaseName); err != nil {
		return nil, err
	}
	// 类型断言获取具体的管理器类型
	var optimizerManager *manager.OptimizerManager
	var bufferPoolManager *manager.OptimizedBufferPoolManager
	var btreeManager basic.BPlusTreeManager
	var tableManager *manager.TableManager

	if e.optimizerManager != nil {
		if om, ok := e.optimizerManager.(*manager.OptimizerManager); ok {
			optimizerManager = om
		}
	}
	if e.bufferPoolManager != nil {
		if bpm, ok := e.bufferPoolManager.(*manager.OptimizedBufferPoolManager); ok {
			bufferPoolManager = bpm
		}
	}
	if e.btreeManager != nil {
		if btm, ok := e.btreeManager.(basic.BPlusTreeManager); ok {
			btreeManager = btm
		}
	}
	if e.tableManager != nil {
		if tm, ok := e.tableManager.(*manager.TableManager); ok {
			tableManager = tm
		}
	}

	// 使用实际的存储管理器字段
	indexManager := e.indexManager
	storageManager := e.storageManager
	tableStorageManager := e.tableStorageManager
	txManager, err := e.getTransactionManager()
	if err != nil {
		return nil, err
	}

	// 根据配置选择DML执行器类型
	useStorageIntegrated := true // 可以从配置中读取
	targetSchema := e.resolveDmlSchema(ctx, databaseName, strings.TrimSpace(stmt.Table.Qualifier.String()))
	ctxSchema := ""
	if ctx != nil {
		ctxSchema = ctx.DatabaseName
	}
	tableName := strings.TrimSpace(stmt.Table.Name.String())
	rawSchema := strings.TrimSpace(stmt.Table.Qualifier.String())
	logger.Debugf("INSERT schema resolve: databaseName=%q ctx.DatabaseName=%q stmt.Table.Qualifier=%q table=%q => targetSchema=%q",
		databaseName, ctxSchema, rawSchema, tableName, targetSchema)
	if err := e.checkTableOrColumnPrivileges(ctx, targetSchema, tableName, "INSERT", insertPrivilegeColumns(e, targetSchema, tableName, stmt)); err != nil {
		return nil, err
	}
	if strings.EqualFold(stmt.Action, sqlparser.ReplaceStr) {
		if err := e.checkTablePrivilege(ctx, targetSchema, tableName, "DELETE"); err != nil {
			return nil, err
		}
	} else if len(stmt.OnDup) > 0 {
		if err := e.checkTableOrColumnPrivileges(ctx, targetSchema, tableName, "UPDATE", updatePrivilegeColumns(sqlparser.UpdateExprs(stmt.OnDup))); err != nil {
			return nil, err
		}
	}

	if useStorageIntegrated && (indexManager == nil || storageManager == nil || tableStorageManager == nil) {
		return nil, e.missingStorageIntegratedDMLManagersError("execute-insert", targetSchema, tableName, indexManager, storageManager, tableStorageManager)
	}

	logger.Debugf("🚀 Using storage-integrated DML executor for INSERT")
	storageIntegratedExecutor := NewStorageIntegratedDMLExecutor(
		optimizerManager,
		bufferPoolManager,
		btreeManager,
		tableManager,
		txManager,
		indexManager,
		storageManager,
		tableStorageManager,
	)
	storageIntegratedExecutor.SetDataDir(e.getDataDir())
	storageIntegratedExecutor.SetClientFoundRows(sessionFoundRowsEnabled(session))
	storageIntegratedExecutor.SetForeignKeyChecks(sessionForeignKeyChecksEnabled(session))
	storageIntegratedExecutor.SetCheckConstraintChecks(sessionCheckConstraintChecksEnabled(session))
	storageIntegratedExecutor.SetUniqueChecks(sessionUniqueChecksEnabled(session))
	storageIntegratedExecutor.SetSQLMode(sessionSQLMode(session))
	storageIntegratedExecutor.SetSelectExecutor(func(selectCtx context.Context, selectStmt *sqlparser.Select, schema string) (*SelectResult, error) {
		if selectStmt == nil {
			return nil, fmt.Errorf("INSERT SELECT来源查询为空")
		}
		queryText := sqlparser.String(selectStmt)
		if selectHasDerivedSource(selectStmt) {
			return e.executeDerivedJoinSelect(&ExecutionContext{
				Context:      selectCtx,
				Session:      session,
				DatabaseName: schema,
				RawQuery:     queryText,
			}, selectStmt, schema)
		}
		return e.executeSelectStatement(&ExecutionContext{
			Context:      selectCtx,
			Session:      session,
			DatabaseName: schema,
			RawQuery:     queryText,
		}, selectStmt, schema)
	})
	storageIntegratedExecutor.SetUnionExecutor(func(selectCtx context.Context, unionStmt *sqlparser.Union, schema string) (*SelectResult, error) {
		if unionStmt == nil {
			return nil, fmt.Errorf("INSERT SELECT UNION来源查询为空")
		}
		queryText := sqlparser.String(unionStmt)
		if branches, operators, ok := splitSetOperationQuery(queryText); ok && hasNonUnionSetOperator(operators) {
			return e.executeMixedSetOperationQuery(&ExecutionContext{Context: selectCtx, Session: session, DatabaseName: schema, RawQuery: queryText}, branches, operators, schema)
		}
		if branches, unionAll, ok := splitUnionQuery(queryText); ok {
			return e.executeUnionQuery(&ExecutionContext{Context: selectCtx, Session: session, DatabaseName: schema, RawQuery: queryText}, branches, unionAll, schema)
		}
		return nil, fmt.Errorf("INSERT SELECT UNION来源无法解析")
	})
	storageIntegratedExecutor.SetTransactionChangeRecorder(func(changes []transactionDMLChange) {
		e.recordTransactionDMLChanges(session, changes)
	})
	storageIntegratedExecutor.SetBeforeTriggerObserver(func(schema, tableName, triggerName string, timerWait int64, triggerErr error) {
		e.recordPerformanceSchemaProgramExecution("TRIGGER", schema, triggerName, timerWait, 1, timerWait, timerWait, timerWait,
			boolToInt64(triggerErr != nil), 0, 0, 0,
		)
	})
	storageIntegratedExecutor.SetAfterTriggerExecutor(func(schema, statement, sourceTable, triggerName string) error {
		return e.executeAfterTriggerStatement(ctx, session, schema, statement, sourceTable, triggerName)
	})
	storageIntegratedExecutor.SetAfterTriggerAtomicBegin(func(string, string, string) (func(bool), error) {
		return e.beginAfterTriggerAtomicJournal(session)
	})
	storageIntegratedExecutor.SetTriggerMetadataLockAcquirer(func(lockCtx context.Context, schema, table string) (func(), error) {
		return e.acquireTriggerExecutionLocks(lockCtx, session, schema, table)
	})

	result, err := storageIntegratedExecutor.ExecuteInsert(transactionContextForSession(ctx.Context, session), stmt, targetSchema)
	if err != nil {
		e.finishAfterTriggerAtomicJournal(ctx, session, false)
		return nil, newExecutorErrorf(
			"execute-insert",
			ExecutionErrorCodeUnknown,
			targetSchema,
			tableName,
			"",
			err,
			"execute storage-integrated INSERT failed",
		)
	}
	e.finishAfterTriggerAtomicJournal(ctx, session, true)
	if session != nil && result.LastInsertId > 0 {
		session.SetParamByName("last_insert_id", result.LastInsertId)
	}
	setSessionWarnings(ctx, result.Warnings)
	e.invalidateTableStatistics(targetSchema, tableName)

	return result, nil
}

// executeUpdateStatement 执行 UPDATE 语句
func (e *XMySQLExecutor) executeUpdateStatement(ctx *ExecutionContext, stmt *sqlparser.Update, databaseName string, session server.MySQLServerSession) (*DMLResult, error) {
	if err := rejectReadOnlyDMLForQuery(session, "UPDATE", sqlparser.String(stmt), databaseName); err != nil {
		return nil, err
	}
	// 类型断言获取具体的管理器类型
	var optimizerManager *manager.OptimizerManager
	var bufferPoolManager *manager.OptimizedBufferPoolManager
	var btreeManager basic.BPlusTreeManager
	var tableManager *manager.TableManager
	var indexManager *manager.IndexManager
	var storageManager *manager.StorageManager
	var tableStorageManager *manager.TableStorageManager

	if e.optimizerManager != nil {
		if om, ok := e.optimizerManager.(*manager.OptimizerManager); ok {
			optimizerManager = om
		}
	}
	if e.bufferPoolManager != nil {
		if bpm, ok := e.bufferPoolManager.(*manager.OptimizedBufferPoolManager); ok {
			bufferPoolManager = bpm
		}
	}
	if e.btreeManager != nil {
		if btm, ok := e.btreeManager.(basic.BPlusTreeManager); ok {
			btreeManager = btm
		}
	}
	if e.tableManager != nil {
		if tm, ok := e.tableManager.(*manager.TableManager); ok {
			tableManager = tm
		}
	}
	indexManager = e.indexManager
	storageManager = e.storageManager
	tableStorageManager = e.tableStorageManager
	txManager, err := e.getTransactionManager()
	if err != nil {
		return nil, err
	}

	// 根据配置选择DML执行器类型
	useStorageIntegrated := true // 可以从配置中读取
	targetSchema := ""
	if joinedDMLHasJoin(stmt.TableExprs) {
		return e.executeJoinedUpdateCompatibility(ctx, stmt, e.resolveDmlSchema(ctx, databaseName, ""), session)
	}
	if len(stmt.TableExprs) > 0 {
		tableSchema, err := e.resolveTableExprSchema(stmt.TableExprs[0])
		if err != nil {
			return nil, err
		}
		targetSchema = tableSchema
	}
	targetSchema = e.resolveDmlSchema(ctx, databaseName, targetSchema)
	tableName := tableNameFromExpr(stmt.TableExprs[0])
	if err := e.checkTableOrColumnPrivileges(ctx, targetSchema, tableName, "UPDATE", updatePrivilegeColumns(stmt.Exprs)); err != nil {
		return nil, err
	}
	if err := e.checkUpdateReadColumnPrivileges(ctx, stmt, targetSchema, tableName); err != nil {
		return nil, err
	}

	if useStorageIntegrated && (indexManager == nil || storageManager == nil || tableStorageManager == nil) {
		return nil, e.missingStorageIntegratedDMLManagersError("execute-update", targetSchema, "", indexManager, storageManager, tableStorageManager)
	}

	storageIntegratedExecutor := NewStorageIntegratedDMLExecutor(
		optimizerManager,
		bufferPoolManager,
		btreeManager,
		tableManager,
		txManager,
		indexManager,
		storageManager,
		tableStorageManager,
	)
	storageIntegratedExecutor.SetDataDir(e.getDataDir())
	storageIntegratedExecutor.SetClientFoundRows(sessionFoundRowsEnabled(session))
	storageIntegratedExecutor.SetForeignKeyChecks(sessionForeignKeyChecksEnabled(session))
	storageIntegratedExecutor.SetCheckConstraintChecks(sessionCheckConstraintChecksEnabled(session))
	storageIntegratedExecutor.SetUniqueChecks(sessionUniqueChecksEnabled(session))
	storageIntegratedExecutor.SetSQLMode(sessionSQLMode(session))
	storageIntegratedExecutor.SetTransactionChangeRecorder(func(changes []transactionDMLChange) {
		e.recordTransactionDMLChanges(session, changes)
	})
	storageIntegratedExecutor.SetBeforeTriggerObserver(func(schema, tableName, triggerName string, timerWait int64, triggerErr error) {
		e.recordPerformanceSchemaProgramExecution("TRIGGER", schema, triggerName, timerWait, 1, timerWait, timerWait, timerWait,
			boolToInt64(triggerErr != nil), 0, 0, 0,
		)
	})
	storageIntegratedExecutor.SetAfterTriggerExecutor(func(schema, statement, sourceTable, triggerName string) error {
		return e.executeAfterTriggerStatement(ctx, session, schema, statement, sourceTable, triggerName)
	})
	storageIntegratedExecutor.SetAfterTriggerAtomicBegin(func(string, string, string) (func(bool), error) {
		return e.beginAfterTriggerAtomicJournal(session)
	})
	storageIntegratedExecutor.SetTriggerMetadataLockAcquirer(func(lockCtx context.Context, schema, table string) (func(), error) {
		return e.acquireTriggerExecutionLocks(lockCtx, session, schema, table)
	})

	result, err := storageIntegratedExecutor.ExecuteUpdate(transactionContextForSession(ctx.Context, session), stmt, targetSchema)
	if err != nil {
		e.finishAfterTriggerAtomicJournal(ctx, session, false)
		return nil, newExecutorErrorf(
			"execute-update",
			ExecutionErrorCodeUnknown,
			targetSchema,
			"",
			"",
			err,
			"execute storage-integrated UPDATE failed",
		)
	}
	e.finishAfterTriggerAtomicJournal(ctx, session, true)
	setSessionWarnings(ctx, result.Warnings)
	e.invalidateTableStatistics(targetSchema, tableName)

	return result, nil
}

// executeDeleteStatement 执行 DELETE 语句
func (e *XMySQLExecutor) executeDeleteStatement(ctx *ExecutionContext, stmt *sqlparser.Delete, databaseName string, session server.MySQLServerSession) (*DMLResult, error) {
	if err := rejectReadOnlyDMLForQuery(session, "DELETE", sqlparser.String(stmt), databaseName); err != nil {
		return nil, err
	}
	// 类型断言获取具体的管理器类型
	var optimizerManager *manager.OptimizerManager
	var bufferPoolManager *manager.OptimizedBufferPoolManager
	var btreeManager basic.BPlusTreeManager
	var tableManager *manager.TableManager
	var indexManager *manager.IndexManager
	var storageManager *manager.StorageManager
	var tableStorageManager *manager.TableStorageManager

	if e.optimizerManager != nil {
		if om, ok := e.optimizerManager.(*manager.OptimizerManager); ok {
			optimizerManager = om
		}
	}
	if e.bufferPoolManager != nil {
		if bpm, ok := e.bufferPoolManager.(*manager.OptimizedBufferPoolManager); ok {
			bufferPoolManager = bpm
		}
	}
	if e.btreeManager != nil {
		if btm, ok := e.btreeManager.(basic.BPlusTreeManager); ok {
			btreeManager = btm
		}
	}
	if e.tableManager != nil {
		if tm, ok := e.tableManager.(*manager.TableManager); ok {
			tableManager = tm
		}
	}
	indexManager = e.indexManager
	storageManager = e.storageManager
	tableStorageManager = e.tableStorageManager
	txManager, err := e.getTransactionManager()
	if err != nil {
		return nil, err
	}

	// 根据配置选择DML执行器类型
	useStorageIntegrated := true // 可以从配置中读取
	targetSchema := ""
	if joinedDMLHasJoin(stmt.TableExprs) {
		return e.executeJoinedDeleteCompatibility(ctx, stmt, e.resolveDmlSchema(ctx, databaseName, ""), session)
	}
	if len(stmt.TableExprs) > 0 {
		tableSchema, err := e.resolveTableExprSchema(stmt.TableExprs[0])
		if err != nil {
			return nil, err
		}
		targetSchema = tableSchema
	}
	targetSchema = e.resolveDmlSchema(ctx, databaseName, targetSchema)
	tableName := tableNameFromExpr(stmt.TableExprs[0])
	if err := e.checkTablePrivilege(ctx, targetSchema, tableName, "DELETE"); err != nil {
		return nil, err
	}

	if useStorageIntegrated && (indexManager == nil || storageManager == nil || tableStorageManager == nil) {
		return nil, e.missingStorageIntegratedDMLManagersError("execute-delete", targetSchema, "", indexManager, storageManager, tableStorageManager)
	}

	storageIntegratedExecutor := NewStorageIntegratedDMLExecutor(
		optimizerManager,
		bufferPoolManager,
		btreeManager,
		tableManager,
		txManager,
		indexManager,
		storageManager,
		tableStorageManager,
	)
	storageIntegratedExecutor.SetDataDir(e.getDataDir())
	storageIntegratedExecutor.SetClientFoundRows(sessionFoundRowsEnabled(session))
	storageIntegratedExecutor.SetForeignKeyChecks(sessionForeignKeyChecksEnabled(session))
	storageIntegratedExecutor.SetCheckConstraintChecks(sessionCheckConstraintChecksEnabled(session))
	storageIntegratedExecutor.SetUniqueChecks(sessionUniqueChecksEnabled(session))
	storageIntegratedExecutor.SetSQLMode(sessionSQLMode(session))
	storageIntegratedExecutor.SetTransactionChangeRecorder(func(changes []transactionDMLChange) {
		e.recordTransactionDMLChanges(session, changes)
	})
	storageIntegratedExecutor.SetBeforeTriggerObserver(func(schema, tableName, triggerName string, timerWait int64, triggerErr error) {
		e.recordPerformanceSchemaProgramExecution("TRIGGER", schema, triggerName, timerWait, 1, timerWait, timerWait, timerWait,
			boolToInt64(triggerErr != nil), 0, 0, 0,
		)
	})
	storageIntegratedExecutor.SetAfterTriggerExecutor(func(schema, statement, sourceTable, triggerName string) error {
		return e.executeAfterTriggerStatement(ctx, session, schema, statement, sourceTable, triggerName)
	})
	storageIntegratedExecutor.SetAfterTriggerAtomicBegin(func(string, string, string) (func(bool), error) {
		return e.beginAfterTriggerAtomicJournal(session)
	})
	storageIntegratedExecutor.SetTriggerMetadataLockAcquirer(func(lockCtx context.Context, schema, table string) (func(), error) {
		return e.acquireTriggerExecutionLocks(lockCtx, session, schema, table)
	})

	result, err := storageIntegratedExecutor.ExecuteDelete(transactionContextForSession(ctx.Context, session), stmt, targetSchema)
	if err != nil {
		e.finishAfterTriggerAtomicJournal(ctx, session, false)
		return nil, newExecutorErrorf(
			"execute-delete",
			ExecutionErrorCodeUnknown,
			targetSchema,
			"",
			"",
			err,
			"execute storage-integrated DELETE failed",
		)
	}
	e.finishAfterTriggerAtomicJournal(ctx, session, true)
	setSessionWarnings(ctx, result.Warnings)
	e.invalidateTableStatistics(targetSchema, tableName)

	return result, nil
}

func (e *XMySQLExecutor) invalidateTableStatistics(schemaName, tableName string) {
	if e == nil || strings.TrimSpace(schemaName) == "" || strings.TrimSpace(tableName) == "" {
		return
	}
	statsPath := tableStatisticsSidecarPath(e.getDataDir(), schemaName, tableName)
	if err := os.Remove(statsPath); err != nil && !os.IsNotExist(err) {
		logger.Warnf("remove invalidated table statistics sidecar for %s.%s failed: %v", schemaName, tableName, err)
	}
	invalidatedThroughTableManager := false
	if invalidator, ok := e.tableManager.(interface {
		InvalidateTableStats(context.Context, string, string) error
	}); ok {
		invalidatedThroughTableManager = true
		if err := invalidator.InvalidateTableStats(context.Background(), schemaName, tableName); err != nil {
			logger.Warnf("invalidate table-manager statistics for %s.%s failed: %v", schemaName, tableName, err)
		}
	}
	if !invalidatedThroughTableManager {
		if invalidator, ok := e.infosSchemaManager.(interface {
			InvalidateTableStats(context.Context, string, string) error
		}); ok {
			if err := invalidator.InvalidateTableStats(context.Background(), schemaName, tableName); err != nil {
				logger.Warnf("invalidate table statistics for %s.%s failed: %v", schemaName, tableName, err)
			}
		}
	}
	path := filepath.Join(e.getDataDir(), schemaName, tableName+".frm")
	raw, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var metadataMap map[string]interface{}
	if err := json.Unmarshal(raw, &metadataMap); err != nil {
		logger.Warnf("parse table statistics metadata for %s.%s failed: %v", schemaName, tableName, err)
		return
	}
	if _, exists := metadataMap["stats"]; !exists {
		return
	}
	delete(metadataMap, "stats")
	updated, err := json.MarshalIndent(metadataMap, "", "  ")
	if err != nil {
		logger.Warnf("marshal invalidated table metadata for %s.%s failed: %v", schemaName, tableName, err)
		return
	}
	if err := writeMetadataFileAtomic(path, updated); err != nil {
		logger.Warnf("persist invalidated table metadata for %s.%s failed: %v", schemaName, tableName, err)
	}
}

// executeCreateDatabaseStatement 执行 CREATE DATABASE
func (e *XMySQLExecutor) executeCreateDatabaseStatement(ctx *ExecutionContext, stmt *sqlparser.DBDDL) {
	logger.Debugf(" Executing CREATE DATABASE: %s", stmt.DBName)
	if err := e.checkDatabasePrivilege(ctx, stmt.DBName, "CREATE"); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("CREATE DATABASE failed: %v", err)}
		return
	}

	// 获取SchemaManager (需要从引擎中获取)
	// 这里需要添加SchemaManager到XMySQLExecutor结构体中
	// 暂时使用简化的实现

	// 解析CREATE DATABASE语句的选项
	charset := stmt.Charset
	collation := stmt.Collate
	ifNotExists := stmt.IfExists

	// 设置默认值
	if charset == "" {
		charset = "utf8mb4"
	}
	if collation == "" {
		collation = "utf8mb4_general_ci"
	}

	// 创建数据库目录和元数据
	if err := e.createDatabaseImpl(stmt.DBName, charset, collation, ifNotExists); err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("CREATE DATABASE failed: %v", err),
		}
		return
	}

	// 返回成功结果
	ctx.Results <- &Result{
		ResultType: common.RESULT_TYPE_DDL,
		Message:    fmt.Sprintf("Database '%s' created successfully", stmt.DBName),
	}

	logger.Infof(" CREATE DATABASE '%s' executed successfully", stmt.DBName)
}

// createDatabaseImpl 实际的数据库创建实现
func (e *XMySQLExecutor) createDatabaseImpl(dbName, charset, collation string, ifNotExists bool) error {
	// 1. 验证数据库名称
	if err := validateDatabaseName(dbName); err != nil {
		return newExecutorErrorf(
			"create-database",
			ExecutionErrorCodeValidation,
			"",
			"",
			"",
			err,
			"invalid database name '%s'",
			dbName,
		)
	}

	// 2. 获取数据目录
	dataDir := e.getDataDir()

	// 3. 构建数据库路径
	dbPath := filepath.Join(dataDir, dbName)

	// 4. 检查数据库是否已存在
	if _, err := os.Stat(dbPath); err == nil {
		if ifNotExists {
			logger.Debugf("Database '%s' already exists, skipping creation due to IF NOT EXISTS", dbName)
			return nil
		}
		return NewExecutionErrorWithCause(
			"engine",
			"create-database",
			ExecutionErrorCodeValidation,
			"",
			"",
			"",
			0,
			fmt.Errorf("database '%s' already exists", dbName),
			"database already exists",
		)
	}

	// 5. 创建数据库目录
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return NewExecutionErrorWithCause(
			"engine",
			"create-database",
			ExecutionErrorCodeStorageWriteFailure,
			"",
			"",
			"",
			0,
			err,
			fmt.Sprintf("failed to create database directory '%s'", dbPath),
		)
	}

	// 6. 创建数据库元数据文件 (db.opt)
	if err := createDatabaseMetadataFile(dbPath, charset, collation); err != nil {
		// 回滚：删除已创建的目录
		os.RemoveAll(dbPath)
		return NewExecutionErrorWithCause(
			"engine",
			"create-database",
			ExecutionErrorCodeStorageWriteFailure,
			"",
			"",
			"",
			0,
			err,
			"failed to create database metadata",
		)
	}

	logger.Infof("📂 Created database directory: %s", dbPath)
	return nil
}

// validateDatabaseName 验证数据库名称
func validateDatabaseName(name string) error {
	// 1. 检查长度
	if len(name) == 0 {
		return NewExecutionErrorWithCause("engine", "validate-database-name", ExecutionErrorCodeValidation, "", "", "", 0,
			fmt.Errorf("database name cannot be empty"), "database name cannot be empty")
	}
	if len(name) > 64 {
		return NewExecutionErrorWithCause("engine", "validate-database-name", ExecutionErrorCodeValidation, "", "", "", 0,
			fmt.Errorf("database name too long (max 64 characters)"), "database name too long (max 64 characters)")
	}

	// 2. 检查字符合法性 (MySQL标准)
	for i, char := range name {
		if !((char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '_' || char == '$') {
			return NewExecutionErrorWithCause(
				"engine",
				"validate-database-name",
				ExecutionErrorCodeValidation,
				"",
				"",
				"",
				0,
				fmt.Errorf("database name contains invalid character at position %d: '%c'", i, char),
				"database name contains invalid character",
			)
		}
	}

	// 3. 检查是否以数字开头
	if name[0] >= '0' && name[0] <= '9' {
		return NewExecutionErrorWithCause("engine", "validate-database-name", ExecutionErrorCodeValidation, "", "", "", 0,
			fmt.Errorf("database name cannot start with a number"), "database name cannot start with a number")
	}

	// 4. 检查保留字
	reservedWords := []string{
		"information_schema", "mysql", "performance_schema", "sys",
	}
	lowerName := strings.ToLower(name)
	for _, reserved := range reservedWords {
		if lowerName == reserved {
			return NewExecutionErrorWithCause("engine", "validate-database-name", ExecutionErrorCodeValidation, "", "", "", 0,
				fmt.Errorf("'%s' is a reserved database name", name), "database name is reserved")
		}
	}

	return nil
}

// createDatabaseMetadataFile 创建数据库元数据文件
func createDatabaseMetadataFile(dbPath, charset, collation string) error {
	// 创建 db.opt 文件 (MySQL兼容格式)
	dbOptPath := filepath.Join(dbPath, "db.opt")
	dbOptContent := fmt.Sprintf("default-character-set=%s\ndefault-collation=%s\n", charset, collation)

	if err := ioutil.WriteFile(dbOptPath, []byte(dbOptContent), 0644); err != nil {
		return NewExecutionErrorWithCause(
			"engine",
			"create-database-metadata-file",
			ExecutionErrorCodeStorageWriteFailure,
			"",
			"",
			"",
			0,
			err,
			"failed to create db.opt file",
		)
	}

	logger.Debugf(" Created database metadata file: %s", dbOptPath)
	return nil
}

// buildWhereConditions 构建 WHERE 条件表达式（占位）
func (e *XMySQLExecutor) buildWhereConditions(where *sqlparser.Where) {}

// executeSetStatement 执行 SET 语句
func (e *XMySQLExecutor) executeSetStatement(ctx *ExecutionContext, stmt *sqlparser.Set, session server.MySQLServerSession) {
	if ctx == nil || ctx.Results == nil {
		logger.Errorf(" [executeSetStatement] execution context is not initialized")
		return
	}

	logger.Debugf(" [executeSetStatement] 执行SET语句，包含 %d 个表达式", len(stmt.Exprs))
	unscopedTransaction := isUnscopedSetTransaction(ctx.RawQuery)

	if session == nil {
		logger.Errorf(" [executeSetStatement] session is nil, 无法设置会话变量")
		ctx.Results <- &Result{
			Err:        fmt.Errorf("session is required for SET statements"),
			ResultType: innodbcommon.RESULT_TYPE_ERROR,
			Message:    "session unavailable for SET statement",
		}
		return
	}
	if sessionBoolParam(session, "in_transaction") && isSessionTransactionCharacteristics(ctx.RawQuery) {
		err := fmt.Errorf("Transaction characteristics can't be changed while a transaction is in progress")
		ctx.Results <- &Result{Err: err, ResultType: innodbcommon.RESULT_TYPE_ERROR, Message: err.Error()}
		return
	}
	if isSessionTransactionCharacteristics(ctx.RawQuery) {
		if err := validateTransactionCharacteristics(stmt); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: innodbcommon.RESULT_TYPE_ERROR, Message: err.Error()}
			return
		}
	}

	affectedVars := 0
	var errors []string

	for _, expr := range stmt.Exprs {
		varName := expr.Name.String()
		logger.Debugf(" [executeSetStatement] 处理变量: %s", varName)

		value, err := e.evaluateSetValue(expr.Expr)
		if err != nil {
			errMsg := fmt.Sprintf("failed to evaluate value for %s: %v", varName, err)
			logger.Errorf(" [executeSetStatement] %s", errMsg)
			errors = append(errors, errMsg)
			continue
		}
		cleanName := strings.ToLower(strings.Trim(strings.TrimSpace(varName), "`"))
		if unscopedTransaction && (cleanName == "tx_isolation" || cleanName == "transaction_isolation") {
			isolation, normalizeErr := normalizeTransactionIsolationValue(value)
			if normalizeErr != nil {
				errors = append(errors, fmt.Sprintf("failed to set %s: %v", varName, normalizeErr))
				continue
			}
			session.SetParamByName("next_transaction_isolation", isolation)
			affectedVars++
			continue
		}
		if unscopedTransaction && (cleanName == "tx_read_only" || cleanName == "transaction_read_only") {
			session.SetParamByName("next_transaction_read_only", int64(boolishToInt(value)))
			affectedVars++
			continue
		}
		if cleanName == "autocommit" && boolishToInt(value) != 0 && !sessionBoolValue(session.GetParamByName("autocommit")) && (sessionBoolParam(session, "in_transaction") || sessionHasTransactionTableLocks(session)) {
			if err := e.commitAutocommitTransaction(session); err != nil {
				errMsg := fmt.Sprintf("failed to commit transaction while enabling autocommit: %v", err)
				logger.Warnf(" [executeSetStatement] %s", errMsg)
				errors = append(errors, errMsg)
				continue
			}
		}

		var setErr error
		if setScope, ok := setStatementScope(stmt.Scope, varName); ok && setScope == manager.GlobalScope {
			setErr = e.setGlobalVariable(session, varName, value)
		} else {
			setErr = e.setSessionVariable(session, varName, value)
		}
		if setErr != nil {
			errMsg := fmt.Sprintf("failed to set %s: %v", varName, setErr)
			logger.Warnf(" [executeSetStatement] %s", errMsg)
			errors = append(errors, errMsg)
			continue
		}
		if cleanName == "names" {
			if collation := parseSetNamesCollation(ctx.RawQuery); collation != "" {
				session.SetParamByName("collation_connection", collation)
			}
		}

		affectedVars++
		logger.Debugf(" [executeSetStatement] 成功设置变量: %s = %v", varName, value)
	}

	if affectedVars == 0 && len(errors) > 0 {
		ctx.Results <- &Result{
			Err:        fmt.Errorf("SET statement failed: %s", strings.Join(errors, "; ")),
			ResultType: innodbcommon.RESULT_TYPE_ERROR,
			Message:    "SET statement failed",
		}
		return
	}

	message := fmt.Sprintf("SET statement executed, %d variables processed", affectedVars)
	if len(errors) > 0 {
		message = fmt.Sprintf("%s (%d warnings: %s)", message, len(errors), strings.Join(errors, "; "))
	}

	ctx.Results <- &Result{
		ResultType: innodbcommon.RESULT_TYPE_SET,
		Message:    message,
	}
	logger.Debugf(" [executeSetStatement] SET语句执行完成: %s", message)
}

func parseSetNamesCollation(query string) string {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	match := regexp.MustCompile(`(?is)^\s*set\s+names\s+(?:'[^']*'|"[^"]*"|[a-zA-Z0-9_]+)\s+collate\s+(?:'([^']*)'|"([^"]*)"|([a-zA-Z0-9_]+))`).FindStringSubmatch(trimmed)
	if len(match) == 0 {
		return ""
	}
	for _, value := range match[1:] {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// validateTransactionCharacteristics rejects duplicate or conflicting
// characteristics before executeSetStatement mutates the session/global
// variable state. MySQL permits one isolation level and one access mode per
// SET TRANSACTION statement; silently applying the last duplicate would make
// a malformed statement appear successful and could leave partial state.
func validateTransactionCharacteristics(stmt *sqlparser.Set) error {
	if stmt == nil {
		return fmt.Errorf("invalid SET TRANSACTION statement")
	}
	isolationCount := 0
	accessModeCount := 0
	for _, expr := range stmt.Exprs {
		name := strings.ToLower(strings.Trim(strings.TrimSpace(expr.Name.String()), "`"))
		switch name {
		case "transaction_isolation", "tx_isolation":
			isolationCount++
		case "transaction_read_only", "tx_read_only":
			accessModeCount++
		}
	}
	if isolationCount > 1 {
		return fmt.Errorf("Transaction isolation level specified more than once")
	}
	if accessModeCount > 1 {
		return fmt.Errorf("Transaction access mode specified more than once")
	}
	return nil
}

// evaluateSetValue 计算 SET 表达式的值
func (e *XMySQLExecutor) evaluateSetValue(expr sqlparser.Expr) (interface{}, error) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.IntVal:
			val, err := strconv.ParseInt(string(v.Val), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid integer value '%s'", string(v.Val))
			}
			return val, nil
		case sqlparser.FloatVal:
			val, err := strconv.ParseFloat(string(v.Val), 64)
			if err != nil {
				return nil, fmt.Errorf("invalid float value '%s'", string(v.Val))
			}
			// Numeric system variables should retain integer values when the
			// literal is mathematically integral. Otherwise a value such as
			// 33554432 can be stored as float64 and later exposed as
			// 3.3554432e+07 through Performance Schema.
			if val == math.Trunc(val) && val >= -9.0e18 && val <= 9.0e18 {
				return int64(val), nil
			}
			return val, nil
		case sqlparser.StrVal:
			return string(v.Val), nil
		default:
			return string(v.Val), nil
		}
	case sqlparser.BoolVal:
		if bool(v) {
			return int64(1), nil
		}
		return int64(0), nil
	case *sqlparser.NullVal:
		return nil, nil
	case *sqlparser.ColName:
		return v.Name.String(), nil
	case *sqlparser.ParenExpr:
		return e.evaluateSetValue(v.Expr)
	default:
		value, err := evaluateExpressionWithRow(expr, map[string]interface{}{})
		if err != nil {
			return sqlparser.String(expr), nil
		}
		return value, nil
	}
}

// setSessionVariable 设置会话变量
func (e *XMySQLExecutor) setSessionVariable(session server.MySQLServerSession, name string, value interface{}) error {
	if session == nil {
		return fmt.Errorf("session is nil")
	}

	cleanName := strings.TrimSpace(name)
	cleanName = strings.Trim(cleanName, "`")
	cleanName = strings.TrimPrefix(cleanName, "@@")
	cleanName = strings.TrimPrefix(cleanName, "session.")
	cleanName = strings.TrimPrefix(cleanName, "global.")
	cleanName = strings.ToLower(cleanName)

	if cleanName == "" {
		return fmt.Errorf("variable name cannot be empty")
	}

	logger.Debugf(" [setSessionVariable] 设置变量: %s = %v (type=%T)", cleanName, value, value)

	switch cleanName {
	case "autocommit":
		session.SetParamByName(cleanName, fmt.Sprintf("%d", boolishToInt(value)))
	case "tx_read_only", "transaction_read_only":
		readOnly := int64(boolishToInt(value))
		session.SetParamByName("tx_read_only", readOnly)
		session.SetParamByName("transaction_read_only", readOnly)
	case "transaction_isolation", "tx_isolation":
		isolation, err := normalizeTransactionIsolationValue(value)
		if err != nil {
			return err
		}
		session.SetParamByName("transaction_isolation", isolation)
		session.SetParamByName("tx_isolation", isolation)
	case "names":
		charset := fmt.Sprintf("%v", value)
		if charset == "" {
			charset = "utf8mb4"
		}
		session.SetParamByName("character_set_client", charset)
		session.SetParamByName("character_set_connection", charset)
		session.SetParamByName("character_set_results", charset)
	case "character_set_client", "character_set_connection", "character_set_results",
		"character_set_database", "character_set_server":
		session.SetParamByName(cleanName, fmt.Sprintf("%v", value))
	case "sql_mode", "time_zone",
		"net_write_timeout", "net_read_timeout", "max_allowed_packet":
		session.SetParamByName(cleanName, fmt.Sprintf("%v", value))
	default:
		session.SetParamByName(cleanName, fmt.Sprintf("%v", value))
	}

	return nil
}

func normalizeTransactionIsolationValue(value interface{}) (string, error) {
	text := strings.ToUpper(strings.TrimSpace(fmt.Sprint(value)))
	text = strings.ReplaceAll(text, "_", " ")
	text = strings.ReplaceAll(text, "-", " ")
	text = strings.Join(strings.Fields(text), " ")
	switch text {
	case "READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE":
		return text, nil
	default:
		return "", fmt.Errorf("invalid transaction isolation level %q", fmt.Sprint(value))
	}
}

func setStatementScope(statementScope, name string) (manager.SystemVariableScope, bool) {
	if strings.EqualFold(strings.TrimSpace(statementScope), sqlparser.GlobalStr) {
		return manager.GlobalScope, true
	}
	lowerName := strings.ToLower(strings.TrimSpace(name))
	if strings.HasPrefix(lowerName, "@@global.") || strings.HasPrefix(lowerName, "global.") {
		return manager.GlobalScope, true
	}
	if strings.EqualFold(strings.TrimSpace(statementScope), sqlparser.SessionStr) ||
		strings.HasPrefix(lowerName, "@@session.") || strings.HasPrefix(lowerName, "session.") {
		return manager.SessionScope, true
	}
	return manager.SessionScope, false
}

func isUnscopedSetTransaction(query string) bool {
	fields := strings.Fields(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";")))
	if len(fields) < 2 || !strings.EqualFold(fields[0], "set") {
		return false
	}
	return strings.EqualFold(fields[1], "transaction")
}

func isSessionTransactionCharacteristics(query string) bool {
	fields := strings.Fields(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";")))
	if len(fields) < 2 || !strings.EqualFold(fields[0], "set") {
		return false
	}
	if strings.EqualFold(fields[1], "transaction") {
		return true
	}
	return len(fields) >= 3 && strings.EqualFold(fields[1], "session") && strings.EqualFold(fields[2], "transaction")
}

func (e *XMySQLExecutor) setGlobalVariable(session server.MySQLServerSession, name string, value interface{}) error {
	if !sessionHasGlobalVariableAdmin(session) {
		return fmt.Errorf("Access denied; you need the SUPER or SYSTEM_VARIABLES_ADMIN privilege for this operation")
	}
	if e == nil || e.storageManager == nil || e.storageManager.GetSystemVariablesManager() == nil {
		return fmt.Errorf("system variable manager is unavailable")
	}
	cleanName := strings.ToLower(strings.TrimSpace(name))
	cleanName = strings.Trim(cleanName, "`")
	cleanName = strings.TrimPrefix(cleanName, "@@global.")
	cleanName = strings.TrimPrefix(cleanName, "global.")
	if cleanName == "" {
		return fmt.Errorf("variable name cannot be empty")
	}
	if cleanName == "mandatory_roles" && !sessionHasRoleAdmin(session) {
		return fmt.Errorf("Access denied; you need the ROLE_ADMIN privilege for this operation")
	}
	sysVars := e.storageManager.GetSystemVariablesManager()
	switch cleanName {
	case "super_read_only":
		superReadOnly := boolishToInt(value)
		if superReadOnly != 0 {
			if err := validateGlobalReadOnlyEnable(session); err != nil {
				return err
			}
			if err := sysVars.SetVariable("", "read_only", "ON", manager.GlobalScope); err != nil {
				return err
			}
		}
		if err := sysVars.SetVariable("", "super_read_only", superReadOnly, manager.GlobalScope); err != nil {
			return err
		}
	case "read_only":
		if boolishToInt(value) != 0 {
			if err := validateGlobalReadOnlyEnable(session); err != nil {
				return err
			}
		}
		if err := sysVars.SetVariable("", "read_only", value, manager.GlobalScope); err != nil {
			return err
		}
		if boolishToInt(value) == 0 {
			// MySQL implicitly disables super_read_only when read_only is
			// turned off, keeping the two global switches consistent.
			if err := sysVars.SetVariable("", "super_read_only", "OFF", manager.GlobalScope); err != nil {
				return err
			}
		}
	case "transaction_isolation", "tx_isolation":
		isolation, err := normalizeTransactionIsolationValue(value)
		if err != nil {
			return err
		}
		for _, alias := range []string{"transaction_isolation", "tx_isolation"} {
			if err := sysVars.SetVariable("", alias, isolation, manager.GlobalScope); err != nil {
				return err
			}
		}
	case "transaction_read_only", "tx_read_only":
		readOnly := int64(boolishToInt(value))
		for _, alias := range []string{"transaction_read_only", "tx_read_only"} {
			if err := sysVars.SetVariable("", alias, readOnly, manager.GlobalScope); err != nil {
				return err
			}
		}
	default:
		if err := sysVars.SetVariable("", cleanName, value, manager.GlobalScope); err != nil {
			return err
		}
	}
	return nil
}

// validateGlobalReadOnlyEnable mirrors MySQL's guard against enabling the
// global read-only switches while the changing session still owns an active
// transaction.  In that state the server must not leave a transaction open
// across the read-only transition.
func validateGlobalReadOnlyEnable(session server.MySQLServerSession) error {
	if session == nil {
		return nil
	}
	if sessionBoolParam(session, "in_transaction") || sessionBoolParam(session, "transaction_journal_active") {
		return fmt.Errorf("Cannot set read_only while a transaction is in progress")
	}
	if lockedTables, ok := session.GetParamByName("locked_tables").(map[string]string); ok && len(lockedTables) > 0 {
		return fmt.Errorf("Cannot set read_only while tables are explicitly locked")
	}
	return nil
}

func sessionHasGlobalVariableAdmin(session server.MySQLServerSession) bool {
	if session == nil {
		return false
	}
	if privileges, ok := session.GetParamByName("global_privileges").([]common.PrivilegeType); ok {
		for _, privilege := range privileges {
			if privilege == common.SuperPriv || privilege == common.AllPriv {
				return true
			}
		}
	}
	if dynamicPrivileges, ok := session.GetParamByName("dynamic_privileges").([]string); ok {
		for _, privilege := range dynamicPrivileges {
			if strings.EqualFold(strings.TrimSpace(privilege), "SYSTEM_VARIABLES_ADMIN") {
				return true
			}
		}
	}
	return false
}

func sessionHasRoleAdmin(session server.MySQLServerSession) bool {
	if session == nil {
		return false
	}
	if privileges, ok := session.GetParamByName("global_privileges").([]common.PrivilegeType); ok {
		for _, privilege := range privileges {
			if privilege == common.SuperPriv || privilege == common.AllPriv {
				return true
			}
		}
	}
	if dynamicPrivileges, ok := session.GetParamByName("dynamic_privileges").([]string); ok {
		for _, privilege := range dynamicPrivileges {
			if strings.EqualFold(strings.TrimSpace(privilege), "ROLE_ADMIN") {
				return true
			}
		}
	}
	return false
}

func boolishToInt(value interface{}) int64 {
	switch v := value.(type) {
	case int64:
		if v != 0 {
			return 1
		}
		return 0
	case int:
		if v != 0 {
			return 1
		}
		return 0
	case bool:
		if v {
			return 1
		}
		return 0
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "1", "on", "true", "enabled":
			return 1
		default:
			return 0
		}
	case nil:
		return 0
	default:
		return 0
	}
}

// getCreateTableName 返回 CREATE TABLE 解析出的表名与库名。parser 将 CREATE TABLE 的表名放在 NewName，不是 Table。
func getCreateTableName(stmt *sqlparser.DDL) (tableName, dbQualifier string) {
	if stmt.Action == sqlparser.CreateStr {
		return stmt.NewName.Name.String(), stmt.NewName.Qualifier.String()
	}
	return stmt.Table.Name.String(), stmt.Table.Qualifier.String()
}

// resolveDmlSchema 按优先级解析DML执行 schemaName: SQL 显式限定符 > 执行上下文 > 传入参数
func (e *XMySQLExecutor) resolveDmlSchema(ctx *ExecutionContext, fallbackSchema string, explicitSchema string) string {
	explicitSchema = strings.TrimSpace(explicitSchema)
	if explicitSchema != "" {
		return explicitSchema
	}

	if ctx != nil {
		ctxSchema := strings.TrimSpace(ctx.DatabaseName)
		if ctxSchema != "" {
			return ctxSchema
		}
	}

	return strings.TrimSpace(fallbackSchema)
}

// resolveTableExprSchema 从表表达式提取 schema 限定符（仅返回 qualifier，不做表名解析）
func (e *XMySQLExecutor) resolveTableExprSchema(tableExpr sqlparser.TableExpr) (string, error) {
	switch expr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch tableName := expr.Expr.(type) {
		case sqlparser.TableName:
			return strings.TrimSpace(tableName.Qualifier.String()), nil
		default:
			return "", fmt.Errorf("不支持的表表达式类型: %T", expr.Expr)
		}
	default:
		return "", fmt.Errorf("不支持的FROM表达式类型: %T", tableExpr)
	}
}

func tableNameFromExpr(tableExpr sqlparser.TableExpr) string {
	switch expr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		if tableName, ok := expr.Expr.(sqlparser.TableName); ok {
			return strings.TrimSpace(tableName.Name.String())
		}
	}
	return ""
}

// executeCreateTableStatement 执行 CREATE TABLE
func (e *XMySQLExecutor) executeCreateTableStatement(ctx *ExecutionContext, databaseName string, stmt *sqlparser.DDL) {
	tableName, qualifier := getCreateTableName(stmt)
	logger.Debugf(" Executing CREATE TABLE: %s", tableName)
	logger.Debugf(" [executeCreateTableStatement] DDL语句详细信息:")
	logger.Debugf("   - Action: %s", stmt.Action)
	logger.Debugf("   - Table.Name: '%s'", stmt.Table.Name.String())
	logger.Debugf("   - Table.Qualifier: '%s'", stmt.Table.Qualifier.String())
	logger.Debugf("   - NewName: '%s'", stmt.NewName.Name.String())
	logger.Debugf("   - IfExists: %v", stmt.IfExists)
	logger.Debugf("   - TableSpec: %v", stmt.TableSpec != nil)

	// 1. 获取当前数据库名称（会话库优先；若表名带库限定符则用限定符）
	currentDB := databaseName
	if qualifier != "" {
		currentDB = qualifier
	}
	if currentDB == "" {
		ctx.Results <- &Result{
			Err:        NewExecutionErrorWithCause("engine", "execute-create-table", ExecutionErrorCodeMetadataMissing, "", "", "", 0, fmt.Errorf("no database selected"), "no database selected"),
			ResultType: common.RESULT_TYPE_DDL,
			Message:    "CREATE TABLE failed: no database selected",
		}
		return
	}
	temporaryCreate := false
	if ctx != nil && ctx.Session != nil {
		temporaryCreate, _ = ctx.Session.GetParamByName(temporaryTableCreatePrivilegeBypassParam).(bool)
	}
	if !temporaryCreate {
		if err := e.checkDatabasePrivilege(ctx, currentDB, "CREATE"); err != nil {
			ctx.Results <- &Result{
				Err:        err,
				ResultType: common.RESULT_TYPE_DDL,
				Message:    fmt.Sprintf("CREATE TABLE failed: %v", err),
			}
			return
		}
	}

	// 2. 验证数据库是否存在
	if err := e.validateDatabaseExists(currentDB); err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("CREATE TABLE failed: %v", err),
		}
		return
	}

	// 3. 表名非空校验（CREATE 时表名在 NewName 中）
	if tableName == "" {
		ctx.Results <- &Result{
			Err:        NewExecutionErrorWithCause("engine", "execute-create-table", ExecutionErrorCodeValidation, currentDB, "", "", 0, fmt.Errorf("table name cannot be empty"), "table name cannot be empty"),
			ResultType: common.RESULT_TYPE_DDL,
			Message:    "CREATE TABLE failed: table name cannot be empty",
		}
		return
	}

	// 4. 检查表是否已存在
	if exists, err := e.checkTableExists(currentDB, tableName); err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("CREATE TABLE failed: %v", err),
		}
		return
	} else if exists {
		if stmt.IfExists {
			// IF NOT EXISTS 场景：表文件已存在时仅跳过建表，但需补齐存储映射，避免重播/重启后 miss。
			// createTableStorageMapping 已支持幂等：映射存在则成功返回，缺失则补建。
			if err := e.createTableStorageMapping(currentDB, tableName, ctx.RawQuery); err != nil {
				ctx.Results <- &Result{
					Err:        err,
					ResultType: common.RESULT_TYPE_DDL,
					Message:    fmt.Sprintf("CREATE TABLE failed: %v", err),
				}
				return
			}
			logger.Debugf("Table '%s.%s' already exists, skipping creation due to IF NOT EXISTS", currentDB, tableName)
			ctx.Results <- &Result{
				ResultType: common.RESULT_TYPE_DDL,
				Message:    fmt.Sprintf("Table '%s' already exists", tableName),
			}
			return
		} else {
			ctx.Results <- &Result{
				Err:        fmt.Errorf("table '%s' already exists", tableName),
				ResultType: common.RESULT_TYPE_DDL,
				Message:    fmt.Sprintf("CREATE TABLE failed: table '%s' already exists", tableName),
			}
			return
		}
	}

	// 5. 创建表实现
	rawQuery := ""
	if ctx != nil {
		rawQuery = ctx.RawQuery
	}
	if err := e.checkCreateForeignKeyPrivileges(ctx, currentDB, stmt, rawQuery); err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("CREATE TABLE failed: %v", err),
		}
		return
	}
	if sessionForeignKeyChecksEnabled(ctx.Session) {
		if err := e.validateCreateTableForeignKeys(currentDB, stmt, rawQuery); err != nil {
			ctx.Results <- &Result{
				Err:        err,
				ResultType: common.RESULT_TYPE_DDL,
				Message:    fmt.Sprintf("CREATE TABLE failed: %v", err),
			}
			return
		}
	}
	if err := e.createTableImpl(currentDB, tableName, stmt, rawQuery); err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("CREATE TABLE failed: %v", err),
		}
		return
	}

	// 6. 创建表存储映射
	if err := e.createTableStorageMapping(currentDB, tableName, rawQuery); err != nil {
		// 回滚：删除已创建的表文件
		e.dropTableImpl(currentDB, tableName)
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("CREATE TABLE failed: %v", err),
		}
		return
	}
	if err := e.initializeCreateTableAutoIncrement(currentDB, tableName, rawQuery); err != nil {
		e.dropTableImpl(currentDB, tableName)
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("CREATE TABLE failed: %v", err)}
		return
	}
	if descriptor, _, partitioned, err := parsePartitionedCreate(rawQuery); partitioned {
		if err != nil {
			e.dropTableImpl(currentDB, tableName)
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("CREATE TABLE failed: %v", err)}
			return
		}
		if err := e.persistPartitionMetadata(currentDB, tableName, descriptor); err != nil {
			e.dropTableImpl(currentDB, tableName)
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("CREATE TABLE failed: %v", err)}
			return
		}
	}

	// 7. 返回成功结果
	ctx.Results <- &Result{
		ResultType: common.RESULT_TYPE_DDL,
		Message:    fmt.Sprintf("Table '%s' created successfully", tableName),
	}

	logger.Infof(" CREATE TABLE '%s.%s' executed successfully", currentDB, tableName)
}

// checkCreateForeignKeyPrivileges enforces the MySQL REFERENCES privilege on
// every existing parent table. A self-reference is checked by CREATE's
// database-level CREATE privilege because the parent table does not exist yet.
func (e *XMySQLExecutor) checkCreateForeignKeyPrivileges(ctx *ExecutionContext, databaseName string, stmt *sqlparser.DDL, rawQuery string) error {
	if e == nil || stmt == nil || ctx == nil {
		return nil
	}
	foreignKeys := parseTableForeignKeys(stmt.TableSpec)
	if len(foreignKeys) == 0 && strings.TrimSpace(rawQuery) != "" {
		foreignKeys = parseCreateTableForeignKeysFallback(rawQuery)
	}
	newTableName := stmt.NewName.Name.String()
	for _, foreignKey := range foreignKeys {
		if foreignKey == nil {
			continue
		}
		refSchema := foreignKeyReferencedSchema(foreignKey, databaseName)
		refTable := strings.Trim(strings.TrimSpace(fmt.Sprint(foreignKey["ref_table"])), "`")
		if refTable == "" || refTable == "<nil>" {
			continue
		}
		if strings.EqualFold(refSchema, databaseName) && strings.EqualFold(refTable, newTableName) {
			continue
		}
		if err := e.checkTablePrivilege(ctx, refSchema, refTable, "REFERENCES"); err != nil {
			name := strings.Trim(strings.TrimSpace(fmt.Sprint(foreignKey["name"])), "`")
			if name == "" || name == "<nil>" {
				name = "foreign key"
			}
			return fmt.Errorf("cannot create %s: %w", name, err)
		}
	}
	return nil
}

// checkAlterForeignKeyPrivileges covers ALTER TABLE ... ADD FOREIGN KEY,
// including self-references where the parent table already exists.
func (e *XMySQLExecutor) checkAlterForeignKeyPrivileges(ctx *ExecutionContext, databaseName, query string) error {
	if e == nil || ctx == nil {
		return nil
	}
	pattern := regexp.MustCompile(`(?is)^\s*alter\s+table\s+` + "`?([a-zA-Z0-9_$]+)`?" + `\s+add\s+(?:constraint\s+` + "`?([a-zA-Z0-9_$]+)`?" + `\s+)?foreign\s+key\s*\(([^)]+)\)\s+references\s+(?:(?:` + "`?([a-zA-Z0-9_$]+)`?" + `)\.)?` + "`?([a-zA-Z0-9_$]+)`?" + `\s*\(([^)]+)\)`)
	match := pattern.FindStringSubmatch(query)
	if len(match) == 0 {
		return nil
	}
	refSchema := strings.Trim(strings.TrimSpace(match[4]), "`")
	if refSchema == "" {
		refSchema = databaseName
	}
	refTable := strings.Trim(strings.TrimSpace(match[5]), "`")
	if refTable == "" {
		return nil
	}
	if err := e.checkTablePrivilege(ctx, refSchema, refTable, "REFERENCES"); err != nil {
		constraint := strings.Trim(strings.TrimSpace(match[2]), "`")
		if constraint == "" {
			constraint = "foreign key"
		}
		return fmt.Errorf("cannot add %s: %w", constraint, err)
	}
	return nil
}

// validateCreateTableForeignKeys checks the parent-side index contract before
// CREATE TABLE publishes any metadata. The session switch intentionally skips
// this validation when FOREIGN_KEY_CHECKS=0, matching the import/rebuild use
// case supported by the runtime path.
func (e *XMySQLExecutor) validateCreateTableForeignKeys(databaseName string, stmt *sqlparser.DDL, rawQuery string) error {
	if e == nil || stmt == nil {
		return nil
	}
	if err := validateForeignKeyReferentialActions(rawQuery); err != nil {
		return err
	}
	foreignKeys := parseTableForeignKeys(stmt.TableSpec)
	if len(foreignKeys) == 0 && strings.TrimSpace(rawQuery) != "" {
		foreignKeys = parseCreateTableForeignKeysFallback(rawQuery)
	}
	childColumns := e.parseTableColumns(stmt.TableSpec)
	if len(childColumns) == 0 && strings.TrimSpace(rawQuery) != "" {
		childColumns = parseCreateTableColumnsFallback(rawQuery)
	}
	childColumnValues := make([]interface{}, 0, len(childColumns))
	for _, column := range childColumns {
		childColumnValues = append(childColumnValues, column)
	}
	childTableInfo := map[string]interface{}{"columns": childColumnValues}
	seenConstraintNames := make(map[string]struct{}, len(foreignKeys))
	for ordinal, foreignKey := range foreignKeys {
		if foreignKey == nil {
			continue
		}
		refSchema := foreignKeyReferencedSchema(foreignKey, databaseName)
		refTable := strings.Trim(strings.TrimSpace(fmt.Sprint(foreignKey["ref_table"])), "`")
		name := strings.Trim(strings.TrimSpace(fmt.Sprint(foreignKey["name"])), "`")
		if name == "" || name == "<nil>" {
			name = fmt.Sprintf("FOREIGN_KEY_%d", ordinal+1)
		}
		nameKey := strings.ToLower(strings.TrimSpace(name))
		if _, seen := seenConstraintNames[nameKey]; seen {
			return fmt.Errorf("duplicate foreign key constraint '%s' in schema '%s'", name, databaseName)
		}
		seenConstraintNames[nameKey] = struct{}{}
		constraintExists, err := e.foreignKeyConstraintNameExists(databaseName, stmt.NewName.Name.String(), name)
		if err != nil {
			return fmt.Errorf("check foreign key constraint '%s': %w", name, err)
		}
		if constraintExists {
			return fmt.Errorf("duplicate foreign key constraint '%s' in schema '%s'", name, databaseName)
		}
		if refTable == "" || refTable == "<nil>" {
			return fmt.Errorf("cannot add foreign key '%s': referenced table is empty", name)
		}
		if _, err := os.Stat(filepath.Join(e.getDataDir(), refSchema, refTable+".frm")); err != nil {
			if strings.EqualFold(refSchema, databaseName) && strings.EqualFold(refTable, stmt.NewName.Name.String()) {
				if err := e.validateSelfReferentialForeignKey(stmt, childTableInfo, foreignKey); err != nil {
					return fmt.Errorf("cannot add foreign key '%s': %w", name, err)
				}
				continue
			}
			return fmt.Errorf("cannot add foreign key '%s': referenced table '%s.%s' does not exist", name, refSchema, refTable)
		}
		if err := e.validateReferencedForeignKeyIndex(refSchema, refTable, foreignKey["ref_columns"]); err != nil {
			return fmt.Errorf("cannot add foreign key '%s': %w", name, err)
		}
		if err := e.validateForeignKeyColumnTypes(childTableInfo, metadataIdentifierList(foreignKey["columns"]), refSchema, refTable, metadataIdentifierList(foreignKey["ref_columns"])); err != nil {
			return fmt.Errorf("cannot add foreign key '%s': %w", name, err)
		}
		if err := validateForeignKeySetNullColumns(childTableInfo, metadataIdentifierList(foreignKey["columns"]), fmt.Sprint(foreignKey["on_delete"]), fmt.Sprint(foreignKey["on_update"])); err != nil {
			return fmt.Errorf("cannot add foreign key '%s': %w", name, err)
		}
	}
	return nil
}

func (e *XMySQLExecutor) validateSelfReferentialForeignKey(stmt *sqlparser.DDL, childTableInfo map[string]interface{}, foreignKey map[string]interface{}) error {
	if e == nil || stmt == nil {
		return fmt.Errorf("executor or table definition is nil")
	}
	localColumns := metadataIdentifierList(foreignKey["columns"])
	referencedColumns := metadataIdentifierList(foreignKey["ref_columns"])
	if len(localColumns) == 0 || len(localColumns) != len(referencedColumns) {
		return fmt.Errorf("foreign key column count does not match referenced column count")
	}
	columns, _ := childTableInfo["columns"].([]interface{})
	findColumn := func(name string) map[string]interface{} {
		for _, raw := range columns {
			column, ok := raw.(map[string]interface{})
			if ok && strings.EqualFold(strings.TrimSpace(fmt.Sprint(column["name"])), strings.TrimSpace(name)) {
				return column
			}
		}
		return nil
	}
	for index, localName := range localColumns {
		local := findColumn(localName)
		referenced := findColumn(referencedColumns[index])
		if local == nil {
			return fmt.Errorf("foreign key column '%s' does not exist", localName)
		}
		if referenced == nil {
			return fmt.Errorf("referenced column '%s' does not exist", referencedColumns[index])
		}
		if !strings.EqualFold(fmt.Sprint(local["type"]), fmt.Sprint(referenced["type"])) ||
			fmt.Sprint(local["unsigned"]) != fmt.Sprint(referenced["unsigned"]) {
			return fmt.Errorf("foreign key columns '%s' and '%s' have incompatible types", localName, referencedColumns[index])
		}
	}
	indexes := e.parseTableIndexes(stmt.TableSpec)
	for _, raw := range columns {
		column, ok := raw.(map[string]interface{})
		if !ok || (!boolValueFromMap(column, "primary") && !boolValueFromMap(column, "unique")) || len(referencedColumns) != 1 || !strings.EqualFold(fmt.Sprint(column["name"]), referencedColumns[0]) {
			continue
		}
		return nil
	}
	for _, index := range indexes {
		indexColumns := metadataIdentifierList(index["columns"])
		if len(indexColumns) < len(referencedColumns) {
			continue
		}
		covered := true
		for position, column := range referencedColumns {
			if !strings.EqualFold(indexColumns[position], column) {
				covered = false
				break
			}
		}
		if covered {
			return nil
		}
	}
	return fmt.Errorf("there is no index in the referenced table where the referenced columns are the first columns")
}

func (e *XMySQLExecutor) initializeCreateTableAutoIncrement(schemaName, tableName, rawQuery string) error {
	match := regexp.MustCompile(`(?is)\bauto_increment\s*=\s*([0-9]+)`).FindStringSubmatch(rawQuery)
	if len(match) != 2 {
		return nil
	}
	requested, err := strconv.ParseUint(match[1], 10, 64)
	if err != nil || requested == 0 {
		return fmt.Errorf("invalid AUTO_INCREMENT value %q", match[1])
	}
	tableInfo, err := readTableMetadataMap(filepath.Join(e.getDataDir(), schemaName, tableName+".frm"))
	if err != nil {
		return err
	}
	columns, _ := tableInfo["columns"].([]interface{})
	for _, rawColumn := range columns {
		column, ok := rawColumn.(map[string]interface{})
		if !ok {
			continue
		}
		autoIncrement, _ := column["auto_increment"].(bool)
		if !autoIncrement {
			continue
		}
		return observeAutoIncrementValue(e.getDataDir(), schemaName, tableName, fmt.Sprint(column["name"]), requested-1)
	}
	return fmt.Errorf("table '%s' does not have an AUTO_INCREMENT column", tableName)
}

// createTableStorageMapping 创建表存储映射
func (e *XMySQLExecutor) createTableStorageMapping(dbName, tableName string, rawQuery ...string) error {
	logger.Debugf("createTableStorageMapping start: dbName=%q tableName=%q", dbName, tableName)

	// 获取存储管理器
	storageManager := e.storageManager
	if storageManager == nil {
		return NewExecutionErrorWithCause("engine", "create-table-storage-mapping", ExecutionErrorCodeStorageMissing, dbName, tableName, "", 0,
			fmt.Errorf("storage manager not available"), "storage manager not available")
	}

	// A CREATE TABLE ... TABLESPACE name attaches to an existing general
	// tablespace. Without that option the engine keeps the historical
	// file-per-table mapping and owns the generated space.
	spaceName := fmt.Sprintf("%s/%s", dbName, tableName)
	ownsTablespace := true
	if len(rawQuery) > 0 {
		if requested := tableOptionTablespace(rawQuery[0]); requested != "" {
			spaceName = requested
			ownsTablespace = false
		}
	}
	var handle *manager.TablespaceHandle
	var err error
	if ownsTablespace {
		handle, err = storageManager.CreateTablespace(spaceName)
	} else {
		handle, err = storageManager.GetTablespace(spaceName)
		if err != nil {
			return NewExecutionErrorWithCause("engine", "create-table-storage-mapping", ExecutionErrorCodeStorageReadFailure,
				dbName, tableName, "", 0, err, fmt.Sprintf("general tablespace %s does not exist", spaceName))
		}
	}
	if err != nil {
		if isTablespaceAlreadyExistsError(err) {
			handle, err = storageManager.GetTablespace(spaceName)
		}
		if err != nil {
			return NewExecutionErrorWithCause("engine", "create-table-storage-mapping", ExecutionErrorCodeStorageWriteFailure,
				dbName, tableName, "", 0, err, "failed to create tablespace")
		}
	}

	// 获取表存储映射管理器
	tableStorageManager := e.tableStorageManager
	if tableStorageManager == nil {
		return NewExecutionErrorWithCause("engine", "create-table-storage-mapping", ExecutionErrorCodeMetadataMissing, dbName, tableName, "", 0,
			fmt.Errorf("table storage manager not available"), "table storage manager not available")
	}

	// 创建表存储信息
	info := &manager.TableStorageInfo{
		SchemaName:     dbName,
		TableName:      tableName,
		SpaceID:        handle.SpaceID,
		RootPageNo:     3, // 默认根页面号
		IndexPageNo:    3, // 默认索引页面号
		DataSegmentID:  handle.DataSegmentID,
		Type:           manager.TableTypeUser,
		TablespaceName: spaceName,
		OwnsTablespace: ownsTablespace,
	}
	if !ownsTablespace {
		// A general tablespace can hold multiple clustered trees. Page 3 is not
		// a safe shared default; the B+Tree manager must allocate a fresh root.
		info.RootPageNo = 0
		info.IndexPageNo = 0
	}

	logger.Debugf("createTableStorageMapping prepared info: key=%s spaceID=%d dataSegmentID=%d",
		spaceName, info.SpaceID, info.DataSegmentID)

	// 注册表存储信息（若已注册则视为成功，兼容 IF NOT EXISTS / 重试）
	if err := tableStorageManager.RegisterTable(context.Background(), info); err != nil {
		if isTableStorageAlreadyRegisteredError(err) {
			logger.Infof("Table storage already registered: %s.%s, refreshing mapping to current tablespace", dbName, tableName)
			if replaceErr := tableStorageManager.ReplaceTableStorage(context.Background(), info); replaceErr != nil {
				return NewExecutionErrorWithCause(
					"engine",
					"create-table-storage-mapping",
					ExecutionErrorCodeStorageWriteFailure,
					dbName,
					tableName,
					"",
					0,
					replaceErr,
					"failed to refresh table storage mapping",
				)
			}
			allTables := tableStorageManager.ListAllTables()
			if _, ok := allTables[fmt.Sprintf("%s.%s", dbName, tableName)]; ok {
				logger.Infof(" Existing mapping confirmed for key=%s.%s", dbName, tableName)
			} else {
				logger.Warnf(" Mapping check: key=%s.%s not found after register-already error", dbName, tableName)
			}
			if persistErr := persistTableStorageIdentityIfPresent(e.getDataDir(), info); persistErr != nil {
				return NewExecutionErrorWithCause(
					"engine",
					"create-table-storage-mapping",
					ExecutionErrorCodeStorageWriteFailure,
					dbName,
					tableName,
					"",
					0,
					persistErr,
					"failed to persist table storage identity",
				)
			}
			return nil
		}
		return NewExecutionErrorWithCause(
			"engine",
			"create-table-storage-mapping",
			ExecutionErrorCodeStorageWriteFailure,
			dbName,
			tableName,
			"",
			0,
			err,
			"failed to register table storage",
		)
	}
	if err := persistTableStorageIdentityIfPresent(e.getDataDir(), info); err != nil {
		return NewExecutionErrorWithCause(
			"engine",
			"create-table-storage-mapping",
			ExecutionErrorCodeStorageWriteFailure,
			dbName,
			tableName,
			"",
			0,
			err,
			"failed to persist table storage identity",
		)
	}

	logger.Debugf("createTableStorageMapping completed: db=%q table=%q spaceID=%d", dbName, tableName, handle.SpaceID)
	return nil
}

func isTablespaceAlreadyExistsError(err error) bool {
	return errors.Is(err, manager.ErrTablespaceExists)
}

func persistTableStorageIdentityIfPresent(dataDir string, info *manager.TableStorageInfo) error {
	if strings.TrimSpace(dataDir) == "" || info == nil {
		return nil
	}
	path := filepath.Join(dataDir, info.SchemaName, info.TableName+".frm")
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return persistTableStorageIdentity(dataDir, info)
}

func isTableStorageAlreadyRegisteredError(err error) bool {
	return errors.Is(err, manager.ErrTableStorageAlreadyRegistered)
}

func (e *XMySQLExecutor) truncateTableImpl(ctx *ExecutionContext, databaseName, tableName string) error {
	if err := clearAutoIncrementTable(e.getDataDir(), databaseName, tableName); err != nil {
		return fmt.Errorf("clear auto increment state failed: %v", err)
	}
	if err := validateDatabaseName(databaseName); err != nil {
		return newExecutorErrorf(
			"truncate-table",
			ExecutionErrorCodeValidation,
			databaseName,
			tableName,
			"",
			err,
			"invalid database name '%s'",
			databaseName,
		)
	}
	if err := e.validateDatabaseExists(databaseName); err != nil {
		return err
	}
	exists, err := e.checkTableExists(databaseName, tableName)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("table '%s.%s' does not exist", databaseName, tableName)
	}
	if e.storageManager == nil {
		return fmt.Errorf("storage manager not available")
	}
	if e.tableStorageManager == nil {
		return fmt.Errorf("table storage manager not available")
	}

	// A physical tablespace rebuild is not safe while a client query may still
	// hold a page/index handle. In particular, Windows rejects deleting the
	// .ibd file while those handles are live. Allocate a fresh empty clustered
	// index root inside the existing tablespace instead. This preserves the
	// table definition and tablespace identity, while clearing all rows in one
	// metadata operation. The auto-increment state was cleared above.
	info, err := e.tableStorageManager.GetTableStorageInfo(databaseName, tableName)
	if err != nil || info == nil {
		return fmt.Errorf("get table storage info during truncate failed: %v", err)
	}
	if err := clearBTreeSidecarForSpace(e.getDataDir(), info.SpaceID); err != nil {
		return fmt.Errorf("clear B+Tree sidecar records failed: %v", err)
	}
	resetContext := context.Background()
	if ctx != nil && ctx.Context != nil {
		resetContext = ctx.Context
	}
	rootPageNo, err := e.tableStorageManager.ResetTableBTree(resetContext, databaseName, tableName)
	if err != nil {
		return fmt.Errorf("reset btree during truncate failed: %v", err)
	}
	updatedInfo, infoErr := e.tableStorageManager.GetTableStorageInfo(databaseName, tableName)
	if infoErr != nil {
		return fmt.Errorf("reload truncated table storage info failed: %v", infoErr)
	}
	if err := persistTableStorageIdentity(e.getDataDir(), updatedInfo); err != nil {
		return fmt.Errorf("persist truncated table identity failed: %v", err)
	}

	logger.Infof("TRUNCATE TABLE '%s.%s' reset clustered-index root to page %d", databaseName, tableName, rootPageNo)
	return nil
}

func persistTableStorageIdentity(dataDir string, info *manager.TableStorageInfo) error {
	if strings.TrimSpace(dataDir) == "" || info == nil {
		return nil
	}
	path := filepath.Join(dataDir, info.SchemaName, info.TableName+".frm")
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read table metadata: %w", err)
	}
	var definition map[string]interface{}
	if err := json.Unmarshal(raw, &definition); err != nil {
		return fmt.Errorf("decode table metadata: %w", err)
	}
	definition["storage_root_page"] = info.RootPageNo
	definition["storage_space_id"] = info.SpaceID
	if strings.TrimSpace(info.TablespaceName) != "" {
		definition["tablespace_name"] = info.TablespaceName
	}
	definition["owns_tablespace"] = info.OwnsTablespace
	encoded, err := json.MarshalIndent(definition, "", "  ")
	if err != nil {
		return fmt.Errorf("encode table metadata: %w", err)
	}
	if err := os.WriteFile(path, encoded, 0644); err != nil {
		return fmt.Errorf("write table metadata: %w", err)
	}
	return nil
}

func cloneTablePartitioning(input map[string]interface{}) map[string]interface{} {
	if input == nil {
		return nil
	}
	result := make(map[string]interface{}, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

// executeDropTableStatement 执行 DROP TABLE
func (e *XMySQLExecutor) executeDropTableStatement(ctx *ExecutionContext, currentDB string, stmt *sqlparser.DDL, mysqlSession server.MySQLServerSession) {
	logger.Debugf("🗑️ Executing DROP TABLE: %s", stmt.Table.Name.String())

	// 1. 解析表名和数据库名
	tableName := stmt.Table.Name.String()
	databaseName := stmt.Table.Qualifier.String()
	if databaseName == "" {
		databaseName = currentDB
	}

	if tableName == "" {
		ctx.Results <- &Result{
			Err:        fmt.Errorf("table name cannot be empty"),
			ResultType: common.RESULT_TYPE_DDL,
			Message:    "DROP TABLE failed: table name cannot be empty",
		}
		return
	}

	// 2. 如果没有指定数据库，需要有当前数据库上下文
	if databaseName == "" {
		// 这里应该从会话中获取当前数据库，暂时使用默认逻辑
		ctx.Results <- &Result{
			Err:        fmt.Errorf("no database selected"),
			ResultType: common.RESULT_TYPE_DDL,
			Message:    "DROP TABLE failed: no database selected",
		}
		return
	}
	if err := e.checkTablePrivilege(ctx, databaseName, tableName, "DROP"); err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("DROP TABLE failed: %v", err),
		}
		return
	}

	// 3. 验证数据库是否存在
	if err := e.validateDatabaseExists(databaseName); err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("DROP TABLE failed: %v", err),
		}
		return
	}

	// 4. 检查表是否存在
	exists, err := e.checkTableExists(databaseName, tableName)
	if err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("DROP TABLE failed: %v", err),
		}
		return
	}

	if !exists {
		if stmt.IfExists {
			logger.Debugf("Table '%s.%s' does not exist, skipping drop due to IF EXISTS", databaseName, tableName)
			ctx.Results <- &Result{
				ResultType: common.RESULT_TYPE_DDL,
				Message:    fmt.Sprintf("Table '%s' does not exist", tableName),
			}
			return
		} else {
			ctx.Results <- &Result{
				Err:        fmt.Errorf("table '%s' does not exist", tableName),
				ResultType: common.RESULT_TYPE_DDL,
				Message:    fmt.Sprintf("DROP TABLE failed: table '%s' does not exist", tableName),
			}
			return
		}
	}
	if sessionForeignKeyChecksEnabled(mysqlSession) {
		if err := e.validateForeignKeyDependenciesBeforeTableDrop(databaseName, tableName); err != nil {
			ctx.Results <- &Result{
				Err:        err,
				ResultType: common.RESULT_TYPE_DDL,
				Message:    fmt.Sprintf("DROP TABLE failed: %v", err),
			}
			return
		}
	}
	if dependency := e.findStoredObjectTableDependency(databaseName, tableName); dependency != "" {
		ctx.Results <- &Result{
			Err:        fmt.Errorf("cannot drop table '%s.%s': it is referenced by %s", databaseName, tableName, dependency),
			ResultType: common.RESULT_TYPE_DDL,
			Message:    "DROP TABLE failed: stored object dependency exists",
		}
		return
	}

	// 5. 删除表实现
	if err := e.dropTableImpl(databaseName, tableName); err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("DROP TABLE failed: %v", err),
		}
		return
	}

	// 6. 返回成功结果
	ctx.Results <- &Result{
		ResultType: common.RESULT_TYPE_DDL,
		Message:    fmt.Sprintf("Table '%s' dropped successfully", tableName),
	}

	logger.Infof(" DROP TABLE '%s.%s' executed successfully", databaseName, tableName)
}

// PhysicalPlan 是逻辑计划转换后的物理执行计划（别名）
type PhysicalPlan = plan.PhysicalPlan

// InfoSchemaAdapter在select_executor.go中已定义

// OptimizeLogicalPlan delegates to the shared optimizer pipeline so callers
// entering through the engine do not silently bypass plan normalization,
// predicate pushdown, pruning, and index access selection.
func OptimizeLogicalPlan(logicalPlan plan.LogicalPlan) plan.LogicalPlan {
	return plan.OptimizeLogicalPlan(logicalPlan)
}

// BuildShowPlan 构建 SHOW 语句的逻辑计划（简化实现）
func BuildShowPlan(stmt *sqlparser.Show) (plan.LogicalPlan, error) {
	if stmt == nil {
		return nil, fmt.Errorf("show statement is nil")
	}
	return &showLogicalPlan{
		showType: stmt.Type,
	}, nil
}

// SetAdditionalManagers 设置额外的管理器组件（用于存储引擎集成）
func (e *XMySQLExecutor) SetAdditionalManagers(
	indexManager *manager.IndexManager,
	storageManager *manager.StorageManager,
	tableStorageManager *manager.TableStorageManager,
) {
	// 存储额外的管理器，以便DML执行器可以访问
	e.indexManager = indexManager
	e.storageManager = storageManager
	e.tableStorageManager = tableStorageManager

	logger.Debugf(" Additional managers set: IndexManager=%v, StorageManager=%v, TableStorageManager=%v",
		indexManager != nil, storageManager != nil, tableStorageManager != nil)
}

func (e *XMySQLExecutor) getTransactionManager() (*manager.TransactionManager, error) {
	if e == nil {
		return nil, NewExecutionErrorWithCause(
			"engine",
			"get-transaction-manager",
			ExecutionErrorCodeTxnContextInvalid,
			"",
			"",
			"",
			0,
			fmt.Errorf("executor is nil"),
			"executor is nil",
		)
	}

	if e.txManager != nil {
		return e.txManager, nil
	}

	if e.storageManager == nil {
		return nil, NewExecutionErrorWithCause(
			"engine",
			"get-transaction-manager",
			ExecutionErrorCodeStorageMissing,
			"",
			"",
			"",
			0,
			fmt.Errorf("storage manager is not initialized"),
			"storage manager is not initialized",
		)
	}

	txManager := e.storageManager.GetTransactionManager()
	if txManager == nil {
		return nil, NewExecutionErrorWithCause(
			"engine",
			"get-transaction-manager",
			ExecutionErrorCodeTxnContextInvalid,
			"",
			"",
			"",
			0,
			fmt.Errorf("transaction manager is not initialized"),
			"transaction manager is not initialized",
		)
	}

	return txManager, nil
}

func (e *XMySQLExecutor) SetTransactionManager(txManager *manager.TransactionManager) {
	e.txManager = txManager
}

// SetLockManager attaches the lock manager used by PROCESSLIST and
// PERFORMANCE_SCHEMA compatibility views for live wait diagnostics.
func (e *XMySQLExecutor) SetLockManager(lockManager *manager.LockManager) {
	e.lockManager = lockManager
}

// SetReplicationStatusProvider attaches the runtime status source used by
// SHOW REPLICA STATUS and SHOW SLAVE STATUS.
func (e *XMySQLExecutor) SetReplicationStatusProvider(provider func() replication.StatusSnapshot) {
	e.replicationStatus = provider
}

// SetReplicationSourceProvider attaches the source stream used by SHOW
// BINARY LOGS and SHOW BINLOG EVENTS.
func (e *XMySQLExecutor) SetReplicationSourceProvider(provider func() *replication.Source) {
	e.replicationSource = provider
}

// SetReplicationReplicaProvider attaches the local replica relay stream used
// by SHOW RELAYLOG EVENTS.
func (e *XMySQLExecutor) SetReplicationReplicaProvider(provider func() *replication.Replica) {
	e.replicationReplica = provider
}

// SetReplicationControl attaches the runtime operations used by START/STOP
// REPLICA (and their legacy SLAVE aliases). Keeping the callbacks at the
// executor boundary avoids coupling SQL execution to the replication package.
func (e *XMySQLExecutor) SetReplicationControl(start func() error, stop func() error) {
	if e == nil {
		return
	}
	e.replicationStart = start
	e.replicationStop = stop
}

// SetReplicationSourceControl attaches CHANGE REPLICATION SOURCE support.
func (e *XMySQLExecutor) SetReplicationSourceControl(change func(string) error) {
	if e == nil {
		return
	}
	e.replicationChangeSource = change
}

// SetReplicationResetControl attaches RESET REPLICA support.
func (e *XMySQLExecutor) SetReplicationResetControl(reset func() error) {
	if e == nil {
		return
	}
	e.replicationReset = reset
}

// SetReplicationResetAllControl attaches RESET REPLICA ALL support.
func (e *XMySQLExecutor) SetReplicationResetAllControl(resetAll func() error) {
	if e == nil {
		return
	}
	e.replicationResetAll = resetAll
}

// SetReplicationSourceAdminControl attaches source-side binlog maintenance
// operations used by FLUSH BINARY LOGS and RESET MASTER.
func (e *XMySQLExecutor) SetReplicationSourceAdminControl(flush func() error, resetMaster func() error) {
	if e == nil {
		return
	}
	e.replicationFlushLogs = flush
	e.replicationResetMaster = resetMaster
}

// SetSessionKillControl attaches the connection registry used by KILL
// CONNECTION. The network layer owns live sessions; the executor only
// validates and dispatches the SQL command through this callback.
func (e *XMySQLExecutor) SetSessionKillControl(kill func(uint32, server.MySQLServerSession) error) {
	if e == nil {
		return
	}
	e.sessionKill = kill
}

// SetSessionQueryKillControl attaches the connection registry used by KILL
// QUERY. The callback performs target ownership and privilege checks in the
// network layer before calling CancelActiveQuery.
func (e *XMySQLExecutor) SetSessionQueryKillControl(kill func(uint32, server.MySQLServerSession) error) {
	if e == nil {
		return
	}
	e.sessionQueryKill = kill
}

// SetProcesslistProvider attaches the live server session snapshot used by
// SHOW PROCESSLIST and INFORMATION_SCHEMA.PROCESSLIST.
func (e *XMySQLExecutor) SetProcesslistProvider(provider func() []server.MySQLServerSession) {
	if e == nil {
		return
	}
	e.processlistProvider = provider
}

// SetReplicaRegistrationProvider attaches the live native replication
// registrations used by SHOW REPLICAS and SHOW SLAVE HOSTS.
func (e *XMySQLExecutor) SetReplicaRegistrationProvider(provider func() []replication.ReplicaRegistration) {
	e.replicaRegistrations = provider
}

func (e *XMySQLExecutor) getDataDir() string {
	if e != nil && e.conf != nil {
		if e.conf.DataDir != "" {
			return e.conf.DataDir
		}
		if e.conf.InnodbDataDir != "" {
			return e.conf.InnodbDataDir
		}
	}
	return "data"
}

// executeDropDatabaseStatement 执行 DROP DATABASE
func (e *XMySQLExecutor) executeDropDatabaseStatement(ctx *ExecutionContext, stmt *sqlparser.DBDDL) {
	logger.Debugf("🗑️ Executing DROP DATABASE: %s", stmt.DBName)
	if err := e.checkDatabasePrivilege(ctx, stmt.DBName, "DROP"); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("DROP DATABASE failed: %v", err)}
		return
	}
	// DROP DATABASE is database-wide DDL. Acquire every currently managed
	// table in a deterministic order so another session's transaction-scoped
	// metadata lease cannot be bypassed by removing the database directory.
	releaseDatabaseLocks, err := e.acquireDatabaseDDLTableLocks(ctx, ctx.Session, stmt.DBName)
	if err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("DROP DATABASE failed: %v", err)}
		return
	}
	defer releaseDatabaseLocks()

	// 解析DROP DATABASE语句的选项
	ifExists := stmt.IfExists

	// 删除数据库
	if err := e.dropDatabaseImpl(stmt.DBName, ifExists); err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("DROP DATABASE failed: %v", err),
		}
		return
	}

	// 返回成功结果
	ctx.Results <- &Result{
		ResultType: common.RESULT_TYPE_DDL,
		Message:    fmt.Sprintf("Database '%s' dropped successfully", stmt.DBName),
	}

	logger.Infof(" DROP DATABASE '%s' executed successfully", stmt.DBName)
}

// dropDatabaseImpl 实际的数据库删除实现
func (e *XMySQLExecutor) dropDatabaseImpl(dbName string, ifExists bool) error {
	// 1. 检查是否为系统数据库
	if isSystemDatabase(dbName) {
		return fmt.Errorf("cannot drop system database '%s'", dbName)
	}

	// 2. 获取数据目录并检查数据库是否存在。
	dataDir := e.getDataDir()
	dbPath := filepath.Join(dataDir, dbName)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		if ifExists {
			logger.Debugf("Database '%s' does not exist, skipping drop due to IF EXISTS", dbName)
			return nil
		}
		return fmt.Errorf("database '%s' does not exist", dbName)
	} else if err != nil {
		return fmt.Errorf("failed to inspect database directory '%s': %w", dbPath, err)
	}

	if err := clearAutoIncrementDatabase(dataDir, dbName); err != nil {
		return fmt.Errorf("clear auto increment state failed: %v", err)
	}

	if e.tableStorageManager != nil {
		if err := e.tableStorageManager.CloseBTreeManagers(); err != nil {
			return fmt.Errorf("close table btree managers failed: %v", err)
		}
		for _, info := range e.tableStorageManager.ListAllTables() {
			if strings.EqualFold(info.SchemaName, dbName) {
				// Close and remove the managed tablespace before deleting the
				// database directory. On Windows an open IBD handle makes
				// DROP DATABASE fail even though the metadata mapping is gone.
				if e.storageManager != nil && info.SpaceID != 0 && info.OwnsTablespace {
					var deleteErr error
					for attempt := 0; attempt < 50; attempt++ {
						deleteErr = e.storageManager.DeleteSpace(info.SpaceID)
						if deleteErr == nil {
							break
						}
						// Windows can briefly retain an IBD handle after the last
						// statement releases its cursor. Match DROP TABLE's bounded
						// retry window before reporting a real cleanup failure.
						time.Sleep(20 * time.Millisecond)
					}
					if deleteErr != nil {
						return fmt.Errorf("delete tablespace %d failed: %v", info.SpaceID, deleteErr)
					}
				}
				if err := clearBTreeSidecarForSpace(dataDir, info.SpaceID); err != nil {
					return fmt.Errorf("clear B+Tree sidecar records failed: %v", err)
				}
				if err := e.tableStorageManager.UnregisterTable(info.SchemaName, info.TableName); err != nil {
					return fmt.Errorf("unregister table storage failed: %v", err)
				}
			}
		}
	}

	// 3. 删除数据库目录
	if err := os.RemoveAll(dbPath); err != nil {
		return fmt.Errorf("failed to remove database directory '%s': %v", dbPath, err)
	}

	logger.Infof("📂 Removed database directory: %s", dbPath)
	return nil
}

// isSystemDatabase 检查是否为系统数据库
func isSystemDatabase(name string) bool {
	systemDatabases := []string{
		"information_schema",
		"mysql",
		"performance_schema",
		"sys",
	}

	lowerName := strings.ToLower(name)
	for _, sysDB := range systemDatabases {
		if lowerName == sysDB {
			return true
		}
	}
	return false
}

// virtualUserDatabases 在 INFORMATION_SCHEMA.SCHEMATA 中展示、但可能尚未在磁盘创建的库名。
// 首次被引用时自动创建目录，避免 "database 'xxx' does not exist"。
var virtualUserDatabases = map[string]struct{ charset, collation string }{
	"demo_db": {"utf8mb4", "utf8mb4_general_ci"},
}

// validateDatabaseExists 验证数据库是否存在
func (e *XMySQLExecutor) validateDatabaseExists(dbName string) error {
	// 获取数据目录
	dataDir := e.getDataDir()

	// 构建数据库路径
	dbPath := filepath.Join(dataDir, dbName)

	// 检查数据库目录是否存在
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		// 若为“虚拟”用户库（在 SCHEMATA 中展示），首次使用时自动创建目录
		if opt, ok := virtualUserDatabases[dbName]; ok {
			if err := os.MkdirAll(dbPath, 0755); err != nil {
				return fmt.Errorf("database '%s' does not exist", dbName)
			}
			if err := createDatabaseMetadataFile(dbPath, opt.charset, opt.collation); err != nil {
				_ = os.RemoveAll(dbPath)
				return fmt.Errorf("database '%s' does not exist", dbName)
			}
			logger.Infof("Auto-created database directory for '%s' (advertised in SCHEMATA)", dbName)
			return nil
		}
		return fmt.Errorf("database '%s' does not exist", dbName)
	}

	return nil
}

// checkTableExists 检查表是否存在
func (e *XMySQLExecutor) checkTableExists(dbName, tableName string) (bool, error) {
	// 获取数据目录
	dataDir := e.getDataDir()

	// 构建表文件路径 (.frm文件或.ibd文件)
	dbPath := filepath.Join(dataDir, dbName)
	frmPath := filepath.Join(dbPath, tableName+".frm")
	ibdPath := filepath.Join(dbPath, tableName+".ibd")

	// 检查是否存在.frm文件或.ibd文件
	if _, err := os.Stat(frmPath); err == nil {
		return true, nil
	}
	if _, err := os.Stat(ibdPath); err == nil {
		return true, nil
	}

	return false, nil
}

func (e *XMySQLExecutor) checkTableOrViewExists(dbName, objectName string) (bool, error) {
	exists, err := e.checkTableExists(dbName, objectName)
	if err != nil || exists {
		return exists, err
	}
	viewPath := filepath.Join(e.getDataDir(), dbName, objectName+".view.json")
	if _, err := os.Stat(viewPath); err == nil {
		return true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return false, nil
}

// createTableImpl 实际的表创建实现
func (e *XMySQLExecutor) createTableImpl(dbName, tableName string, stmt *sqlparser.DDL, rawQuery ...string) error {
	logger.Debugf(" Creating table %s.%s", dbName, tableName)
	if err := rejectUnsupportedCreateTableConstraints(stmt); err != nil {
		return err
	}

	// 获取数据目录
	dataDir := e.getDataDir()

	dbPath := filepath.Join(dataDir, dbName)

	// 1. 创建表结构文件 (.frm)
	query := ""
	if len(rawQuery) > 0 {
		query = rawQuery[0]
	}
	if err := e.createTableStructureFile(dbPath, tableName, stmt, query); err != nil {
		return fmt.Errorf("failed to create table structure file: %v", err)
	}

	// 2. 创建表数据文件 (.ibd)
	if err := e.createTableDataFile(dbPath, tableName); err != nil {
		// 回滚：删除已创建的.frm文件
		os.Remove(filepath.Join(dbPath, tableName+".frm"))
		return fmt.Errorf("failed to create table data file: %v", err)
	}

	logger.Infof(" Created table files for %s.%s", dbName, tableName)
	return nil
}

// dropTableImpl 实际的表删除实现
func (e *XMySQLExecutor) dropTableImpl(dbName, tableName string) error {
	logger.Debugf("🗑️ Dropping table %s.%s", dbName, tableName)
	if err := clearAutoIncrementTable(e.getDataDir(), dbName, tableName); err != nil {
		return fmt.Errorf("clear auto increment state failed: %v", err)
	}

	// 获取数据目录
	dataDir := e.getDataDir()
	var tableInfo *manager.TableStorageInfo
	if e.tableStorageManager != nil {
		if err := e.tableStorageManager.CloseBTreeManagers(); err != nil {
			return fmt.Errorf("close table btree managers failed: %v", err)
		}
		if info, err := e.tableStorageManager.GetTableStorageInfo(dbName, tableName); err == nil {
			tableInfo = info
		}
	}
	spaceID := uint32(0)
	if tableInfo != nil {
		spaceID = tableInfo.SpaceID
	} else if raw, readErr := os.ReadFile(filepath.Join(dataDir, dbName, tableName+".frm")); readErr == nil {
		var definition map[string]interface{}
		if json.Unmarshal(raw, &definition) == nil {
			if value, ok := definition["storage_space_id"].(float64); ok && value > 0 {
				spaceID = uint32(value)
			}
		}
	}
	if spaceID > 0 && (tableInfo == nil || tableInfo.OwnsTablespace) {
		if err := clearBTreeSidecarForSpace(dataDir, spaceID); err != nil {
			return fmt.Errorf("clear B+Tree sidecar records failed: %v", err)
		}
		if e.storageManager != nil {
			var deleteErr error
			for attempt := 0; attempt < 50; attempt++ {
				deleteErr = e.storageManager.DeleteSpace(spaceID)
				if deleteErr == nil {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if deleteErr != nil {
				return fmt.Errorf("delete table space failed: %v", deleteErr)
			}
		}
	}
	if tableInfo != nil && e.tableStorageManager != nil {
		if err := e.tableStorageManager.UnregisterTable(dbName, tableName); err != nil {
			return fmt.Errorf("unregister table storage failed: %v", err)
		}
	}

	dbPath := filepath.Join(dataDir, dbName)

	// 删除表相关文件
	filesToDelete := []string{
		filepath.Join(dbPath, tableName+".frm"), // 表结构文件
		filepath.Join(dbPath, tableName+".ibd"), // 表数据文件
		filepath.Join(dbPath, tableName+".MYD"), // MyISAM数据文件
		filepath.Join(dbPath, tableName+".MYI"), // MyISAM索引文件
	}

	var errors []string
	for _, filePath := range filesToDelete {
		if _, err := os.Stat(filePath); err == nil {
			var removeErr error
			for attempt := 0; attempt < 50; attempt++ {
				removeErr = os.Remove(filePath)
				if removeErr == nil || os.IsNotExist(removeErr) {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if removeErr != nil && !os.IsNotExist(removeErr) {
				errors = append(errors, fmt.Sprintf("failed to remove %s: %v", filePath, removeErr))
			} else {
				logger.Debugf("🗑️ Removed file: %s", filePath)
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("errors during table drop: %s", strings.Join(errors, "; "))
	}

	logger.Infof("🗑️ Dropped table %s.%s", dbName, tableName)
	return nil
}

// createTableStructureFile 创建表结构文件 (.frm)
func (e *XMySQLExecutor) createTableStructureFile(dbPath, tableName string, stmt *sqlparser.DDL, rawQuery ...string) error {
	frmPath := filepath.Join(dbPath, tableName+".frm")
	columns := e.parseTableColumns(stmt.TableSpec)
	if len(columns) == 0 && len(rawQuery) > 0 {
		columns = parseCreateTableColumnsFallback(rawQuery[0])
	}
	if len(rawQuery) > 0 && len(columns) > 0 {
		mergeFallbackColumnMetadata(columns, parseCreateTableColumnsFallback(rawQuery[0]))
	}
	if len(columns) == 0 {
		return fmt.Errorf("no column definitions parsed for table %s", tableName)
	}
	indexes := e.parseTableIndexes(stmt.TableSpec)
	applyIndexMetadataToColumns(columns, indexes)
	indexes = synthesizePrimaryIndexMetadata(columns, indexes)
	indexes = synthesizeUniqueIndexMetadata(columns, indexes)
	foreignKeys := parseTableForeignKeys(stmt.TableSpec)
	if len(foreignKeys) == 0 && len(rawQuery) > 0 {
		foreignKeys = parseCreateTableForeignKeysFallback(rawQuery[0])
	}
	indexes = ensureForeignKeyIndexes(indexes, foreignKeys)
	checks := parseTableChecks(stmt.TableSpec)
	checkNames, checkEnforced := parseTableCheckMetadata(stmt.TableSpec)
	tableComment := ""
	if options := e.parseTableOptions(stmt.TableSpec); options != nil {
		if value, ok := options["comment"].(string); ok {
			tableComment = value
		}
	}

	// 构建表结构信息
	tableInfo := map[string]interface{}{
		"table_name":     tableName,
		"columns":        columns,
		"indexes":        indexes,
		"foreign_keys":   foreignKeys,
		"checks":         checks,
		"check_names":    checkNames,
		"check_enforced": checkEnforced,
		"options":        e.parseTableOptions(stmt.TableSpec),
		"table_comment":  tableComment,
		"created_at":     time.Now().Format(time.RFC3339),
	}

	// 序列化为JSON
	data, err := json.MarshalIndent(tableInfo, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize table structure: %v", err)
	}

	// 写入文件
	if err := ioutil.WriteFile(frmPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write .frm file: %v", err)
	}

	logger.Debugf(" Created table structure file: %s", frmPath)
	return nil
}

func parseTableChecks(spec *sqlparser.TableSpec) []string {
	if spec == nil || len(spec.Checks) == 0 {
		return nil
	}
	return append([]string(nil), spec.Checks...)
}

func parseTableCheckMetadata(spec *sqlparser.TableSpec) (map[string]string, map[string]bool) {
	names := make(map[string]string)
	enforced := make(map[string]bool)
	if spec == nil {
		return names, enforced
	}
	definitions := spec.CheckDefinitions
	if len(definitions) == 0 {
		for _, expression := range spec.Checks {
			definitions = append(definitions, sqlparser.CheckConstraintDefinition{Expression: expression, Enforced: true})
		}
	}
	for index, definition := range definitions {
		name := strings.TrimSpace(definition.Name)
		if name == "" {
			name = fmt.Sprintf("check_%d", index+1)
		}
		key := strings.ToLower(name)
		names[key] = definition.Expression
		enforced[key] = definition.Enforced
	}
	return names, enforced
}

func parseTableForeignKeys(spec *sqlparser.TableSpec) []map[string]interface{} {
	if spec == nil || len(spec.ForeignKeys) == 0 {
		return nil
	}
	foreignKeys := make([]map[string]interface{}, 0, len(spec.ForeignKeys))
	for _, fk := range spec.ForeignKeys {
		if fk == nil {
			continue
		}
		columns := make([]string, 0, len(fk.Columns))
		for _, column := range fk.Columns {
			columns = append(columns, column.String())
		}
		refColumns := make([]string, 0, len(fk.ReferencedColumns))
		for _, column := range fk.ReferencedColumns {
			refColumns = append(refColumns, column.String())
		}
		foreignKeys = append(foreignKeys, map[string]interface{}{
			"name":           fk.Name.String(),
			"columns":        columns,
			"ref_schema":     fk.ReferencedTable.Qualifier.String(),
			"ref_table":      fk.ReferencedTable.Name.String(),
			"ref_columns":    refColumns,
			"on_delete":      fk.OnDelete,
			"on_update":      fk.OnUpdate,
			"raw_definition": sqlparser.String(fk),
		})
	}
	return foreignKeys
}

func ensureForeignKeyIndexes(indexes []map[string]interface{}, foreignKeys []map[string]interface{}) []map[string]interface{} {
	if len(foreignKeys) == 0 {
		return indexes
	}
	result := append([]map[string]interface{}(nil), indexes...)
	for ordinal, foreignKey := range foreignKeys {
		columns, _ := foreignKey["columns"].([]string)
		if len(columns) == 0 {
			continue
		}
		covered := false
		for _, index := range result {
			indexColumns, _ := index["columns"].([]string)
			if len(indexColumns) < len(columns) {
				continue
			}
			covered = true
			for i := range columns {
				if !strings.EqualFold(indexColumns[i], columns[i]) {
					covered = false
					break
				}
			}
			if covered {
				break
			}
		}
		if covered {
			continue
		}
		name := fmt.Sprint(foreignKey["name"])
		if name == "" || name == "<nil>" {
			name = fmt.Sprintf("fk_%d", ordinal+1)
		}
		result = append(result, map[string]interface{}{"name": name, "type": "INDEX", "unique": false, "primary": false, "auto_foreign_key": true, "columns": append([]string(nil), columns...)})
	}
	return result
}

// createTableDataFile 创建表数据文件 (.ibd)
func (e *XMySQLExecutor) createTableDataFile(dbPath, tableName string) error {
	ibdPath := filepath.Join(dbPath, tableName+".ibd")

	// 创建空的数据文件
	file, err := os.Create(ibdPath)
	if err != nil {
		return fmt.Errorf("failed to create .ibd file: %v", err)
	}
	defer file.Close()

	// 写入基本的InnoDB页头信息（简化版本）
	header := make([]byte, 16384)     // 16KB页大小
	copy(header[0:4], []byte("IBDT")) // InnoDB数据文件标识

	if _, err := file.Write(header); err != nil {
		return fmt.Errorf("failed to write .ibd header: %v", err)
	}

	logger.Debugf(" Created table data file: %s", ibdPath)
	return nil
}

// parseTableColumns 解析表列定义
func (e *XMySQLExecutor) parseTableColumns(spec *sqlparser.TableSpec) []map[string]interface{} {
	if spec == nil {
		return nil
	}

	var columns []map[string]interface{}
	for _, col := range spec.Columns {
		length, scale := 0, 0
		if col.Type.Length != nil {
			length, _ = strconv.Atoi(strings.TrimSpace(sqlparser.String(col.Type.Length)))
		}
		if col.Type.Scale != nil {
			scale, _ = strconv.Atoi(strings.TrimSpace(sqlparser.String(col.Type.Scale)))
		}
		column := map[string]interface{}{
			"name":     col.Name.String(),
			"type":     col.Type.Type,
			"length":   length,
			"scale":    scale,
			"unsigned": bool(col.Type.Unsigned),
			"zerofill": bool(col.Type.Zerofill),
			"nullable": !bool(col.Type.NotNull),
			"charset":  col.Type.Charset,
			"collate":  col.Type.Collate,
		}

		// 解析列选项
		if col.Type.Autoincrement {
			column["auto_increment"] = true
		}
		typeText := strings.ToLower(sqlparser.String(&col.Type))
		if strings.Contains(typeText, "primary key") {
			column["primary"] = true
			column["unique"] = true
			column["nullable"] = false
		}
		if strings.Contains(typeText, "unique") {
			column["unique"] = true
		}

		// 解析默认值
		if col.Type.Default != nil {
			column["default"] = sqlparser.String(col.Type.Default)
		}

		// 解析ON UPDATE
		if col.Type.OnUpdate != nil {
			column["on_update"] = sqlparser.String(col.Type.OnUpdate)
		}

		// 解析注释
		if col.Type.Comment != nil {
			column["comment"] = persistedString(sqlparser.String(col.Type.Comment))
		}

		// 解析键选项
		switch col.Type.KeyOpt {
		case 1: // colKeyPrimary
			column["key"] = "PRIMARY"
		case 2: // colKeyUnique
			column["key"] = "UNIQUE"
		case 3: // colKeyUniqueKey
			column["key"] = "UNIQUE KEY"
		case 4: // colKeySpatialKey
			column["key"] = "SPATIAL KEY"
		case 5: // colKey
			column["key"] = "KEY"
		}

		// 解析枚举值
		if len(col.Type.EnumValues) > 0 {
			column["enum_values"] = col.Type.EnumValues
		}

		columns = append(columns, column)
	}

	return columns
}

func parseCreateTableColumnsFallback(query string) []map[string]interface{} {
	body := extractCreateTableBody(query)
	if body == "" {
		return nil
	}

	definitions := splitTopLevelComma(body)
	columns := make([]map[string]interface{}, 0, len(definitions))
	for _, definition := range definitions {
		column := parseCreateTableColumnDefinitionFallback(definition)
		if column != nil {
			columns = append(columns, column)
		}
	}
	return columns
}

func parseCreateTableForeignKeysFallback(query string) []map[string]interface{} {
	body := extractCreateTableBody(query)
	if body == "" {
		return nil
	}
	definitions := splitTopLevelComma(body)
	foreignKeys := make([]map[string]interface{}, 0)
	re := regexp.MustCompile("(?is)^\\s*(?:constraint\\s+`?([a-zA-Z0-9_$]+)`?\\s+)?foreign\\s+key\\s*\\(([^)]+)\\)\\s+references\\s+(?:(?:`?([a-zA-Z0-9_]+)`?)\\.)?`?([a-zA-Z0-9_]+)`?\\s*\\(([^)]+)\\)(.*)$")
	for _, definition := range definitions {
		matches := re.FindStringSubmatch(strings.TrimSpace(definition))
		if len(matches) == 0 {
			continue
		}
		tail := strings.ToLower(matches[6])
		foreignKeys = append(foreignKeys, map[string]interface{}{
			"name":           strings.Trim(matches[1], "` "),
			"columns":        parseIdentifierList(matches[2]),
			"ref_schema":     strings.Trim(matches[3], "` "),
			"ref_table":      strings.Trim(matches[4], "` "),
			"ref_columns":    parseIdentifierList(matches[5]),
			"on_delete":      cascadeActionFromTail(tail, "delete"),
			"on_update":      cascadeActionFromTail(tail, "update"),
			"raw_definition": strings.TrimSpace(definition),
		})
	}
	return foreignKeys
}

func parseIdentifierList(input string) []string {
	parts := strings.Split(input, ",")
	identifiers := make([]string, 0, len(parts))
	for _, part := range parts {
		identifier := strings.Trim(strings.TrimSpace(part), "`")
		if identifier != "" {
			identifiers = append(identifiers, identifier)
		}
	}
	return identifiers
}

func cascadeActionFromTail(tail, action string) string {
	pattern := regexp.MustCompile(`(?i)\bon\s+` + regexp.QuoteMeta(action) + `\s+(restrict|cascade|set\s+null|no\s+action)\b`)
	match := pattern.FindStringSubmatch(tail)
	if len(match) != 2 {
		return ""
	}
	return strings.Join(strings.Fields(strings.ToLower(match[1])), " ")
}

func validateForeignKeyReferentialActions(definition string) error {
	pattern := regexp.MustCompile(`(?i)\bon\s+(delete|update)\b`)
	matches := pattern.FindAllStringSubmatchIndex(definition, -1)
	for index, match := range matches {
		if len(match) != 4 {
			continue
		}
		end := len(definition)
		if index+1 < len(matches) {
			end = matches[index+1][0]
		}
		words := strings.Fields(strings.ToLower(strings.TrimSpace(definition[match[3]:end])))
		if len(words) == 0 {
			return fmt.Errorf("unsupported foreign key ON %s action %q", strings.ToUpper(definition[match[2]:match[3]]), "")
		}
		words[0] = strings.TrimRight(words[0], "(),;")
		action := words[0]
		if (action == "set" || action == "no") && len(words) > 1 {
			action += " " + strings.TrimRight(words[1], "(),;")
		}
		switch action {
		case "restrict", "cascade", "set null", "no action":
			continue
		default:
			return fmt.Errorf("unsupported foreign key ON %s action %q", strings.ToUpper(definition[match[2]:match[3]]), action)
		}
	}
	return nil
}

func extractCreateTableBody(query string) string {
	start := strings.Index(query, "(")
	if start < 0 {
		return ""
	}
	depth := 0
	for i := start; i < len(query); i++ {
		switch query[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return query[start+1 : i]
			}
		}
	}
	return ""
}

func splitTopLevelComma(input string) []string {
	var parts []string
	start := 0
	depth := 0
	quote := byte(0)
	for i := 0; i < len(input); i++ {
		ch := input[i]
		if quote != 0 {
			if ch == quote {
				quote = 0
			}
			continue
		}
		switch ch {
		case '\'', '"', '`':
			quote = ch
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				if part := strings.TrimSpace(input[start:i]); part != "" {
					parts = append(parts, part)
				}
				start = i + 1
			}
		}
	}
	if part := strings.TrimSpace(input[start:]); part != "" {
		parts = append(parts, part)
	}
	return parts
}

func parseCreateTableColumnDefinitionFallback(definition string) map[string]interface{} {
	tokens := strings.Fields(definition)
	if len(tokens) < 2 {
		return nil
	}

	first := strings.ToLower(strings.Trim(tokens[0], "`"))
	switch first {
	case "primary", "unique", "key", "index", "constraint", "foreign", "check", "fulltext":
		return nil
	}

	columnName := strings.Trim(tokens[0], "`")
	typeToken := strings.ToUpper(tokens[1])
	typeName := typeToken
	length := 0
	scale := 0
	if open := strings.Index(typeToken, "("); open >= 0 {
		typeName = typeToken[:open]
		if close := strings.LastIndex(typeToken, ")"); close > open {
			dimensions := strings.Split(typeToken[open+1:close], ",")
			if parsed, err := strconv.Atoi(strings.TrimSpace(dimensions[0])); err == nil {
				length = parsed
			}
			if len(dimensions) > 1 {
				if parsed, err := strconv.Atoi(strings.TrimSpace(dimensions[1])); err == nil {
					scale = parsed
				}
			}
		}
	}

	lowerDefinition := strings.ToLower(definition)
	column := map[string]interface{}{
		"name":     columnName,
		"type":     normalizeFallbackColumnType(typeName),
		"length":   length,
		"scale":    scale,
		"unsigned": strings.Contains(lowerDefinition, " unsigned"),
		"zerofill": strings.Contains(lowerDefinition, " zerofill"),
		"nullable": !strings.Contains(lowerDefinition, "not null"),
		"charset":  "",
		"collate":  "",
	}
	if strings.Contains(lowerDefinition, "auto_increment") {
		column["auto_increment"] = true
	}
	if strings.Contains(lowerDefinition, "primary key") {
		column["primary"] = true
		column["unique"] = true
		column["nullable"] = false
	}
	if strings.Contains(lowerDefinition, " unique") {
		column["unique"] = true
	}
	if defaultValue := extractFallbackDefaultValue(definition); defaultValue != "" {
		column["default"] = defaultValue
	}
	if generatedExpression := extractFallbackGeneratedExpression(definition); generatedExpression != "" {
		column["generated"] = true
		column["generated_expression"] = generatedExpression
		column["nullable"] = false
	}
	return column
}

func extractFallbackGeneratedExpression(definition string) string {
	re := regexp.MustCompile(`(?is)\b(?:generated\s+always\s+)?as\s*\((.+)\)\s*(?:stored|virtual)?\s*$`)
	matches := re.FindStringSubmatch(strings.TrimSpace(definition))
	if len(matches) != 2 {
		return ""
	}
	return strings.TrimSpace(matches[1])
}

func mergeFallbackColumnMetadata(columns, fallback []map[string]interface{}) {
	byName := make(map[string]map[string]interface{}, len(fallback))
	for _, col := range fallback {
		if name, _ := col["name"].(string); name != "" {
			byName[strings.ToLower(name)] = col
		}
	}
	for _, col := range columns {
		name, _ := col["name"].(string)
		fallbackCol, ok := byName[strings.ToLower(name)]
		if !ok {
			continue
		}
		for _, key := range []string{"generated", "generated_expression"} {
			if value, exists := fallbackCol[key]; exists {
				col[key] = value
			}
		}
	}
}

func normalizeFallbackColumnType(typeName string) string {
	switch strings.ToUpper(strings.TrimSpace(typeName)) {
	case "INTEGER":
		return string(metadata.TypeInt)
	case "BOOL":
		return string(metadata.TypeBool)
	case "BOOLEAN":
		return string(metadata.TypeBoolean)
	default:
		return strings.ToUpper(strings.TrimSpace(typeName))
	}
}

func extractFallbackDefaultValue(definition string) string {
	tokens := strings.Fields(definition)
	for i := 0; i < len(tokens)-1; i++ {
		if strings.EqualFold(tokens[i], "default") {
			return strings.Trim(tokens[i+1], "'\"")
		}
	}
	return ""
}

func applyIndexMetadataToColumns(columns []map[string]interface{}, indexes []map[string]interface{}) {
	byName := make(map[string]map[string]interface{}, len(columns))
	for _, col := range columns {
		if name, _ := col["name"].(string); name != "" {
			byName[name] = col
		}
	}
	for _, idx := range indexes {
		rawColumns, _ := idx["columns"].([]string)
		if len(rawColumns) == 0 {
			continue
		}
		isPrimary, _ := idx["primary"].(bool)
		isUnique, _ := idx["unique"].(bool)
		for _, name := range rawColumns {
			if col, ok := byName[name]; ok {
				if isPrimary {
					col["primary"] = true
					col["nullable"] = false
					if len(rawColumns) == 1 {
						col["unique"] = true
					}
				} else if isUnique && len(rawColumns) == 1 {
					col["unique"] = true
				}
			}
		}
	}
}

func synthesizePrimaryIndexMetadata(columns []map[string]interface{}, indexes []map[string]interface{}) []map[string]interface{} {
	for _, idx := range indexes {
		if isPrimary, _ := idx["primary"].(bool); isPrimary {
			return indexes
		}
	}

	primaryColumns := make([]string, 0)
	for _, col := range columns {
		isPrimary, _ := col["primary"].(bool)
		if !isPrimary {
			continue
		}
		name, _ := col["name"].(string)
		if name != "" {
			primaryColumns = append(primaryColumns, name)
		}
	}
	if len(primaryColumns) == 0 {
		return indexes
	}

	primaryIndex := map[string]interface{}{
		"name":    "PRIMARY",
		"type":    "PRIMARY KEY",
		"unique":  true,
		"primary": true,
		"columns": primaryColumns,
	}
	return append([]map[string]interface{}{primaryIndex}, indexes...)
}

// synthesizeUniqueIndexMetadata preserves the index created by a column-level
// UNIQUE attribute. The parser records the column constraint on the column,
// but the storage metadata and foreign-key validator both need the durable
// index entry as well. MySQL uses the column name as the implicit index name.
func synthesizeUniqueIndexMetadata(columns []map[string]interface{}, indexes []map[string]interface{}) []map[string]interface{} {
	result := append([]map[string]interface{}(nil), indexes...)
	for _, column := range columns {
		if column == nil {
			continue
		}
		unique, _ := column["unique"].(bool)
		primary, _ := column["primary"].(bool)
		name, _ := column["name"].(string)
		if !unique || primary || strings.TrimSpace(name) == "" {
			continue
		}
		covered := false
		for _, index := range result {
			if index == nil {
				continue
			}
			indexUnique, _ := index["unique"].(bool)
			indexColumns := metadataIdentifierList(index["columns"])
			if indexUnique && len(indexColumns) == 1 && strings.EqualFold(indexColumns[0], name) {
				covered = true
				break
			}
		}
		if covered {
			continue
		}
		result = append(result, map[string]interface{}{
			"name": name, "type": "UNIQUE", "unique": true, "primary": false,
			"columns": []string{name},
		})
	}
	return result
}

// parseTableIndexes 解析表索引定义
func (e *XMySQLExecutor) parseTableIndexes(spec *sqlparser.TableSpec) []map[string]interface{} {
	if spec == nil {
		return nil
	}

	var indexes []map[string]interface{}
	for _, idx := range spec.Indexes {
		index := map[string]interface{}{
			"name":    idx.Info.Name.String(),
			"type":    idx.Info.Type,
			"unique":  idx.Info.Unique,
			"primary": idx.Info.Primary,
			"columns": make([]string, 0),
		}

		// 解析索引列
		for _, col := range idx.Columns {
			index["columns"] = append(index["columns"].([]string), col.Column.String())
		}

		indexes = append(indexes, index)
	}

	return indexes
}

func rejectUnsupportedCreateTableConstraints(stmt *sqlparser.DDL) error {
	if stmt == nil || stmt.Action != "create" {
		return nil
	}
	return rejectUnsupportedCreateTableConstraintsSQL(sqlparser.String(stmt))
}

func rejectUnsupportedCreateTableConstraintsSQL(query string) error {
	ddl := strings.ToLower(query)
	if !strings.HasPrefix(strings.TrimSpace(ddl), "create table") {
		return nil
	}
	return nil
}

// parseTableOptions 解析表选项
func (e *XMySQLExecutor) parseTableOptions(spec *sqlparser.TableSpec) map[string]interface{} {
	options := make(map[string]interface{})

	if spec == nil {
		return options
	}

	// 设置默认选项
	options["engine"] = "InnoDB"
	options["charset"] = "utf8mb4"
	options["collation"] = "utf8mb4_general_ci"
	options["row_format"] = "Dynamic"

	// 解析表选项（如果有的话）
	if spec.Options != "" {
		// Preserve the original option text, but also expose the common MySQL
		// table options in their normalized metadata form.  The parser accepts
		// these options even when their values are not needed by the storage
		// implementation, and clients expect SHOW/INFORMATION_SCHEMA to retain
		// the requested charset and collation rather than the defaults.
		options["raw_options"] = spec.Options
		if value, ok := parseTableOptionValue(spec.Options, `engine`); ok {
			if strings.EqualFold(value, "innodb") {
				value = "InnoDB"
			}
			options["engine"] = value
		}
		if value, ok := parseTableOptionValue(spec.Options, `(?:default\s+)?(?:character\s+set|charset)`); ok {
			options["charset"] = value
		}
		if value, ok := parseTableOptionValue(spec.Options, `collate`); ok {
			options["collation"] = value
		}
		if value, ok := parseTableOptionValue(spec.Options, `row_format`); ok {
			options["row_format"] = normalizeTableRowFormat(value)
		}
		if comment, ok := parseTableCommentOption(spec.Options); ok {
			options["comment"] = comment
		}
	}

	return options
}

func normalizeTableRowFormat(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "compressed":
		return "Compressed"
	case "compact":
		return "Compact"
	case "redundant":
		return "Redundant"
	case "dynamic", "default":
		return "Dynamic"
	default:
		return value
	}
}

func parseTableOptionValue(options, keyPattern string) (string, bool) {
	pattern := regexp.MustCompile(`(?is)\b` + keyPattern + `\s*=\s*([a-zA-Z0-9_]+)`).FindStringSubmatch(options)
	if len(pattern) != 2 {
		return "", false
	}
	return strings.TrimSpace(pattern[1]), true
}

func parseTableCommentOption(options string) (string, bool) {
	match := regexp.MustCompile(`(?is)\bcomment\s*(?:=\s*)?(?:'((?:''|[^'])*)'|"((?:""|[^"])*)")`).FindStringSubmatch(options)
	if len(match) != 3 {
		return "", false
	}
	if match[1] != "" {
		return strings.ReplaceAll(match[1], "''", "'"), true
	}
	return strings.ReplaceAll(match[2], `""`, `"`), true
}

// executeShowStatement 执行 SHOW 语句
func (e *XMySQLExecutor) executeShowStatement(ctx *ExecutionContext, stmt *sqlparser.Show, session server.MySQLServerSession) {
	logger.Debugf(" [executeShowStatement] 处理SHOW语句: %s", stmt.Type)
	if isShowCharacterSetQuery(ctx.RawQuery) {
		e.executeShowCharacterSet(ctx, ctx.RawQuery)
		return
	}
	if isShowCollationQuery(ctx.RawQuery) {
		e.executeShowCollation(ctx, ctx.RawQuery)
		return
	}
	if isShowTableStatusQuery(ctx.RawQuery) {
		e.executeShowTableStatus(ctx, ctx.RawQuery, session)
		return
	}
	if isShowTriggersQuery(ctx.RawQuery) {
		e.executeShowTriggers(ctx, ctx.RawQuery, session)
		return
	}
	if isShowEventsQuery(ctx.RawQuery) {
		e.executeShowEvents(ctx, ctx.RawQuery, session)
		return
	}
	if routineType := showRoutineStatusType(ctx.RawQuery); routineType != "" {
		e.executeShowRoutineStatus(ctx, ctx.RawQuery, session, routineType)
		return
	}
	if isShowMasterStatusQuery(ctx.RawQuery) {
		e.executeShowMasterStatus(ctx)
		return
	}
	if isShowReplicaHostsQuery(ctx.RawQuery) {
		e.executeShowReplicaHosts(ctx)
		return
	}
	if isShowBinaryLogsQuery(ctx.RawQuery) {
		e.executeShowBinaryLogs(ctx)
		return
	}
	if isShowBinlogEventsQuery(ctx.RawQuery) {
		e.executeShowBinlogEvents(ctx)
		return
	}
	if isShowRelaylogEventsQuery(ctx.RawQuery) {
		e.executeShowRelaylogEvents(ctx)
		return
	}

	showType := strings.ToLower(stmt.Type)
	rawShow := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(ctx.RawQuery, ";")))
	if rawShow == "show count(*) warnings" || rawShow == "show count(*) errors" {
		e.executeShowWarningCount(ctx, strings.HasSuffix(rawShow, "errors"))
		return
	}

	switch showType {
	case "databases":
		e.executeShowDatabases(ctx)
	case "tables":
		e.executeShowTables(ctx, stmt, session, "")
	case "columns", "fields":
		e.executeShowColumns(ctx, stmt, "")
	case "index", "indexes", "keys":
		e.executeShowIndex(ctx, ctx.RawQuery)
	case "variables":
		e.executeShowVariables(ctx, stmt)
	case "status":
		e.executeShowStatus(ctx, stmt)
	case "processlist":
		e.executeShowProcesslist(ctx, session)
	case "replica", "slave":
		e.executeShowReplicaStatus(ctx)
	case "create table":
		e.executeShowCreateTable(ctx, stmt)
	case "engines":
		e.executeShowEngines(ctx)
	case "storage":
		e.executeShowEngines(ctx)
	case "open":
		e.executeShowOpenTables(ctx, stmt, session, ctx.RawQuery)
	case "plugins":
		e.executeShowPlugins(ctx, ctx.RawQuery)
	case "engine":
		e.executeShowEngineInnoDBStatus(ctx)
	case "warnings":
		e.executeShowWarnings(ctx)
	case "errors":
		e.executeShowErrors(ctx)
	default:
		logger.Warnf(" [executeShowStatement] 不支持的SHOW类型: %s", showType)
		ctx.Results <- &Result{
			Err:        fmt.Errorf("unsupported SHOW type: %s", showType),
			ResultType: "ERROR",
		}
	}
}

func (e *XMySQLExecutor) executeShowWarningCount(ctx *ExecutionContext, errorsOnly bool) {
	count := int64(0)
	for _, warning := range sessionWarnings(ctx) {
		isError := strings.EqualFold(warning.Level, "error")
		if isError == errorsOnly {
			count++
		}
	}
	column := "@@session.warning_count"
	if errorsOnly {
		column = "@@session.error_count"
	}
	ctx.Results <- &Result{
		ResultType: "QUERY",
		Data: map[string]interface{}{
			"columns": []string{column},
			"rows":    [][]interface{}{{count}},
		},
	}
}

func (e *XMySQLExecutor) executeShowPlugins(ctx *ExecutionContext, rawQuery string) {
	columns := []string{"Name", "Status", "Type", "Library", "License", "Load_option"}
	rows := [][]interface{}{
		{"InnoDB", "ACTIVE", "STORAGE ENGINE", nil, "GPL", "DEFAULT"},
		{"mysql_native_password", "ACTIVE", "AUTHENTICATION", nil, "GPL", "ON"},
		{"caching_sha2_password", "ACTIVE", "AUTHENTICATION", nil, "GPL", "ON"},
		{"sha256_password", "ACTIVE", "AUTHENTICATION", nil, "GPL", "ON"},
	}
	if match := regexp.MustCompile(`(?is)\blike\s+['"]([^'"]*)['"]`).FindStringSubmatch(rawQuery); len(match) == 2 {
		pattern := match[1]
		filtered := make([][]interface{}, 0, len(rows))
		for _, row := range rows {
			if sqlLikeMatch(fmt.Sprint(row[0]), pattern) {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}
	ctx.Results <- &Result{
		ResultType: common.RESULT_TYPE_QUERY,
		Data:       newCompatibilityTypedResult("plugins", columns, []string{"varchar", "varchar", "varchar", "varchar", "varchar", "varchar"}, rows),
	}
}

// executeShowOpenTables implements the catalog-visible form of SHOW OPEN
// TABLES (and the parser's SHOW OPEN SCHEMAS alias). The coordinator's live
// owner/status/mode snapshots provide a real current-use projection for
// In_use and Name_locked while durable table/view metadata remains
// discoverable. This is intentionally a lock-backed compatibility view, not a
// claim of byte-for-byte parity with MySQL's private table-cache internals.
func (e *XMySQLExecutor) executeShowOpenTables(ctx *ExecutionContext, stmt *sqlparser.Show, session server.MySQLServerSession, rawQuery string) {
	databaseFilter := ""
	if match := regexp.MustCompile(`(?is)\b(?:from|in)\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?").FindStringSubmatch(rawQuery); len(match) == 2 {
		databaseFilter = match[1]
	}
	if databaseFilter != "" {
		if err := e.validateDatabaseExists(databaseFilter); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return
		}
	}

	type openTable struct {
		database string
		table    string
		isView   bool
	}
	objects := make([]openTable, 0)
	seen := make(map[string]struct{})
	for _, table := range e.scanFrmTables() {
		if databaseFilter != "" && !strings.EqualFold(databaseFilter, table.schemaName) {
			continue
		}
		if !e.tableMetadataVisibleToSession(&ExecutionContext{Session: session}, table.schemaName, table.tableName, false) {
			continue
		}
		key := strings.ToLower(table.schemaName + "\x00" + table.tableName)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		objects = append(objects, openTable{database: table.schemaName, table: table.tableName})
	}
	for _, view := range e.scanViewTables(session) {
		if databaseFilter != "" && !strings.EqualFold(databaseFilter, view.schemaName) {
			continue
		}
		if !e.tableMetadataVisibleToSession(&ExecutionContext{Session: session}, view.schemaName, view.tableName, true) {
			continue
		}
		key := strings.ToLower(view.schemaName + "\x00" + view.tableName)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		objects = append(objects, openTable{database: view.schemaName, table: view.tableName, isView: true})
	}
	sort.Slice(objects, func(i, j int) bool {
		if !strings.EqualFold(objects[i].database, objects[j].database) {
			return strings.ToLower(objects[i].database) < strings.ToLower(objects[j].database)
		}
		return strings.ToLower(objects[i].table) < strings.ToLower(objects[j].table)
	})

	rows := make([][]interface{}, 0, len(objects))
	for _, object := range objects {
		inUse, nameLocked := e.showOpenTableCounters(object.database, object.table)
		rows = append(rows, []interface{}{object.database, object.table, inUse, nameLocked})
	}
	likePattern := ResolveShowLikePattern("open tables", stmt, rawQuery)
	if strings.TrimSpace(likePattern) == "" {
		if match := regexp.MustCompile(`(?is)\blike\s+['"]([^'"]*)['"]`).FindStringSubmatch(rawQuery); len(match) == 2 {
			likePattern = match[1]
		}
	}
	if strings.TrimSpace(likePattern) != "" {
		filtered := make([][]interface{}, 0, len(rows))
		for _, row := range rows {
			if len(row) > 1 && sqlLikeMatch(fmt.Sprint(row[1]), likePattern) {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}
	ctx.Results <- &Result{
		ResultType: common.RESULT_TYPE_QUERY,
		Data:       map[string]interface{}{"columns": []string{"Database", "Table", "In_use", "Name_locked"}, "rows": rows},
		Message:    fmt.Sprintf("Found %d open tables", len(rows)),
	}
}

func (e *XMySQLExecutor) showOpenTableCounters(databaseName, tableName string) (int64, int64) {
	if e == nil {
		return 0, 0
	}
	inUse := int64(0)
	nameLocked := int64(0)
	for _, snapshot := range e.getDDLCoordinator().MetadataLocks() {
		schema, table := compatibilityQualifiedTable(snapshot.Table, "")
		if !strings.EqualFold(schema, databaseName) || !strings.EqualFold(table, tableName) {
			continue
		}
		if strings.EqualFold(snapshot.Status, "GRANTED") {
			inUse++
		}
		if snapshot.Mode == tableLockWrite || strings.EqualFold(snapshot.Status, "PENDING") {
			nameLocked = 1
		}
	}
	return inUse, nameLocked
}

func (e *XMySQLExecutor) executeShowIndex(ctx *ExecutionContext, rawQuery string) {
	pattern := regexp.MustCompile("(?is)^\\s*show\\s+(?:index|indexes|keys)\\s+(?:from|in)\\s+`?([a-zA-Z0-9_$]+)`?(?:\\s+(?:from|in)\\s+`?([a-zA-Z0-9_$]+)`?)?")
	matches := pattern.FindStringSubmatch(rawQuery)
	if len(matches) == 0 {
		ctx.Results <- &Result{Err: fmt.Errorf("SHOW INDEX requires a table name"), ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	tableName, databaseName := matches[1], ctx.DatabaseName
	if matches[2] != "" {
		databaseName, tableName = matches[2], matches[1]
	}
	physicalTableName, temporary := temporaryPhysicalTableName(ctx.Session, databaseName, tableName)
	if !temporary && !e.tableMetadataVisibleToSession(ctx, databaseName, tableName, false) {
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: map[string]interface{}{
			"columns": []string{"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Comment", "Index_comment", "Visible", "Expression"},
			"rows":    [][]interface{}{},
		}}
		return
	}
	frmPath := filepath.Join(e.getDataDir(), databaseName, physicalTableName+".frm")
	raw, err := os.ReadFile(frmPath)
	if err != nil {
		ctx.Results <- &Result{Err: fmt.Errorf("read table metadata failed: %v", err), ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	var tableInfo struct {
		Indexes []map[string]interface{} `json:"indexes"`
	}
	if err := json.Unmarshal(raw, &tableInfo); err != nil {
		ctx.Results <- &Result{Err: fmt.Errorf("parse table metadata failed: %v", err), ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	columns := []string{"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Comment", "Index_comment", "Visible", "Expression"}
	rows := make([][]interface{}, 0)
	cardinality, _ := e.tableIndexCardinalities(ctx, databaseName, physicalTableName)
	for _, index := range tableInfo.Indexes {
		name := fmt.Sprint(index["name"])
		unique, _ := index["unique"].(bool)
		nonUnique := 1
		if unique {
			nonUnique = 0
		}
		visible := true
		if persistedVisible, ok := index["visible"].(bool); ok {
			visible = persistedVisible
		}
		visibleValue := "NO"
		if visible {
			visibleValue = "YES"
		}
		indexColumns, _ := index["columns"].([]interface{})
		for position, column := range indexColumns {
			cardinalityValue := cardinality[name]
			rows = append(rows, []interface{}{tableName, nonUnique, name, position + 1, fmt.Sprint(column), "A", cardinalityValue, nil, nil, "", "BTREE", "", "", visibleValue, nil})
		}
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: map[string]interface{}{"columns": columns, "rows": rows}, Message: fmt.Sprintf("Found %d indexes", len(rows))}
}

func (e *XMySQLExecutor) tableIndexCardinalities(ctx *ExecutionContext, databaseName, tableName string) (map[string]int64, error) {
	result := make(map[string]int64)
	if info, err := e.readPersistedTableInfo(databaseName, tableName); err == nil && info.Stats != nil {
		for name, stat := range info.Stats.IndexStats {
			result[name] = int64(stat.DistinctCount)
		}
		if len(result) > 0 {
			return result, nil
		}
	}
	stmt, err := sqlparser.Parse("select * from " + tableName)
	if err != nil {
		return result, err
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return result, fmt.Errorf("table cardinality source is not SELECT")
	}
	rows, err := e.executeSelectStatement(ctx, selectStmt, databaseName)
	if err != nil {
		return result, err
	}
	frmPath := filepath.Join(e.getDataDir(), databaseName, tableName+".frm")
	raw, err := os.ReadFile(frmPath)
	if err != nil {
		return result, err
	}
	var info struct {
		Indexes []map[string]interface{} `json:"indexes"`
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		return result, err
	}
	for _, index := range info.Indexes {
		name := fmt.Sprint(index["name"])
		columns, _ := index["columns"].([]interface{})
		seen := make(map[string]struct{})
		for _, record := range rows.Records {
			parts := make([]string, 0, len(columns))
			for _, column := range columns {
				columnIndex := findResultColumn(rows.Columns, fmt.Sprint(column))
				if columnIndex < 0 || columnIndex >= len(record.GetValues()) {
					continue
				}
				parts = append(parts, windowValueKey(record.GetValues()[columnIndex]))
			}
			seen[strings.Join(parts, "|")] = struct{}{}
		}
		result[name] = int64(len(seen))
	}
	return result, nil
}

// executeShowStatementWithQuery 执行SHOW语句（带原始SQL）
func (e *XMySQLExecutor) executeShowStatementWithQuery(ctx *ExecutionContext, stmt *sqlparser.Show, session server.MySQLServerSession, rawQuery string) {
	logger.Debugf(" [executeShowStatementWithQuery] 处理SHOW语句: %s, rawQuery=%s", stmt.Type, rawQuery)
	if isShowCharacterSetQuery(rawQuery) {
		e.executeShowCharacterSet(ctx, rawQuery)
		return
	}
	if isShowCollationQuery(rawQuery) {
		e.executeShowCollation(ctx, rawQuery)
		return
	}
	if isShowTableStatusQuery(rawQuery) {
		e.executeShowTableStatus(ctx, rawQuery, session)
		return
	}
	if isShowTriggersQuery(rawQuery) {
		e.executeShowTriggers(ctx, rawQuery, session)
		return
	}
	if isShowEventsQuery(rawQuery) {
		e.executeShowEvents(ctx, rawQuery, session)
		return
	}
	if routineType := showRoutineStatusType(rawQuery); routineType != "" {
		e.executeShowRoutineStatus(ctx, rawQuery, session, routineType)
		return
	}
	if isShowMasterStatusQuery(rawQuery) {
		e.executeShowMasterStatus(ctx)
		return
	}
	if isShowReplicaHostsQuery(rawQuery) {
		e.executeShowReplicaHosts(ctx)
		return
	}
	if isShowBinaryLogsQuery(rawQuery) {
		e.executeShowBinaryLogs(ctx)
		return
	}
	if isShowBinlogEventsQuery(rawQuery) {
		e.executeShowBinlogEvents(ctx)
		return
	}
	if isShowRelaylogEventsQuery(rawQuery) {
		e.executeShowRelaylogEvents(ctx)
		return
	}
	if isShowReplicaStatusQuery(rawQuery) {
		e.executeShowReplicaStatus(ctx)
		return
	}
	if isShowEngineInnoDBStatus(rawQuery) {
		e.executeShowEngineInnoDBStatus(ctx)
		return
	}
	rawShow := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(rawQuery, ";")))
	if rawShow == "show count(*) warnings" || rawShow == "show count(*) errors" {
		e.executeShowWarningCount(ctx, strings.HasSuffix(rawShow, "errors"))
		return
	}

	showType := strings.ToLower(stmt.Type)
	switch showType {
	case "databases":
		e.executeShowDatabasesWithQuery(ctx, stmt, rawQuery)
	case "tables":
		e.executeShowTables(ctx, stmt, session, rawQuery)
	case "columns", "fields":
		e.executeShowColumns(ctx, stmt, rawQuery)
	case "index", "indexes", "keys":
		e.executeShowIndex(ctx, rawQuery)
	case "variables":
		e.executeShowVariablesWithQuery(ctx, stmt, rawQuery)
	case "status":
		e.executeShowStatusWithQuery(ctx, stmt, rawQuery)
	case "processlist":
		e.executeShowProcesslist(ctx, session)
	case "replica", "slave":
		e.executeShowReplicaStatus(ctx)
	case "create table":
		e.executeShowCreateTable(ctx, stmt, rawQuery)
	case "engines":
		e.executeShowEngines(ctx)
	case "storage":
		e.executeShowEngines(ctx)
	case "open":
		e.executeShowOpenTables(ctx, stmt, session, rawQuery)
	case "plugins":
		e.executeShowPlugins(ctx, rawQuery)
	case "engine":
		e.executeShowEngineInnoDBStatus(ctx)
	case "warnings":
		e.executeShowWarnings(ctx)
	case "errors":
		e.executeShowErrors(ctx)
	default:
		logger.Warnf(" [executeShowStatementWithQuery] 不支持的SHOW类型: %s", showType)
		ctx.Results <- &Result{
			Err:        fmt.Errorf("unsupported SHOW type: %s", showType),
			ResultType: "ERROR",
		}
	}
}

// executeShowDatabases 执行 SHOW DATABASES
func (e *XMySQLExecutor) executeShowDatabases(ctx *ExecutionContext) {
	e.executeShowDatabasesWithQuery(ctx, nil, "")
}

// executeShowDatabasesWithQuery 执行 SHOW DATABASES，支持LIKE和WHERE
func (e *XMySQLExecutor) executeShowDatabasesWithQuery(ctx *ExecutionContext, stmt *sqlparser.Show, rawQuery string) {
	logger.Debugf(" [executeShowDatabases] 执行SHOW DATABASES")

	// 获取数据目录
	dataDir := e.getDataDir()

	// 读取数据目录下的所有子目录（每个子目录代表一个数据库）
	var databases []string

	// 添加系统数据库
	databases = append(databases, "information_schema", "mysql", "performance_schema", "sys")

	// 读取用户数据库
	if entries, err := os.ReadDir(dataDir); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				dbName := entry.Name()
				// 过滤掉隐藏目录和系统目录
				if !strings.HasPrefix(dbName, ".") && !strings.HasPrefix(dbName, "_") {
					databases = append(databases, dbName)
				}
			}
		}
	}
	visibleDatabases := databases[:0]
	for _, db := range databases {
		if e.schemaMetadataVisibleToSession(ctx.Session, db) {
			visibleDatabases = append(visibleDatabases, db)
		}
	}
	databases = visibleDatabases

	// 构造结果集
	rows := make([][]interface{}, len(databases))
	for i, db := range databases {
		rows[i] = []interface{}{db}
	}

	likePattern := ResolveShowLikePattern("databases", stmt, rawQuery)
	rows = filterShowRowsByLike(rows, likePattern)
	whereExpr := ResolveShowWhereExpr("databases", stmt, rawQuery)
	rows = filterShowRowsByWhere(rows, []string{"Database"}, whereExpr)

	// 使用 Data 字段存储结果，格式为 map
	resultData := map[string]interface{}{
		"columns": []string{"Database"},
		"rows":    rows,
	}

	ctx.Results <- &Result{
		ResultType: common.RESULT_TYPE_QUERY,
		Data:       resultData,
		Message:    fmt.Sprintf("Found %d databases", len(rows)),
	}

	logger.Debugf(" [executeShowDatabases] 返回 %d 个数据库", len(rows))
}

func isShowTableStatusQuery(query string) bool {
	normalized := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	return strings.HasPrefix(normalized, "show table status") &&
		(normalized == "show table status" || strings.HasPrefix(normalized, "show table status "))
}

func isShowTriggersQuery(query string) bool {
	normalized := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	return strings.HasPrefix(normalized, "show triggers") &&
		(normalized == "show triggers" || strings.HasPrefix(normalized, "show triggers "))
}

func isShowEventsQuery(query string) bool {
	normalized := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	return strings.HasPrefix(normalized, "show events") &&
		(normalized == "show events" || strings.HasPrefix(normalized, "show events "))
}

func showRoutineStatusType(query string) string {
	normalized := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	switch {
	case strings.HasPrefix(normalized, "show procedure status") && (normalized == "show procedure status" || strings.HasPrefix(normalized, "show procedure status ")):
		return "procedure"
	case strings.HasPrefix(normalized, "show function status") && (normalized == "show function status" || strings.HasPrefix(normalized, "show function status ")):
		return "function"
	default:
		return ""
	}
}

func isShowCharacterSetQuery(query string) bool {
	normalized := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	return strings.HasPrefix(normalized, "show character set") || strings.HasPrefix(normalized, "show charset")
}

func isShowCollationQuery(query string) bool {
	normalized := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	return strings.HasPrefix(normalized, "show collation")
}

func (e *XMySQLExecutor) executeShowCharacterSet(ctx *ExecutionContext, rawQuery string) {
	columns := []string{"Charset", "Description", "Default collation", "Maxlen"}
	rows := [][]interface{}{
		{"utf8mb4", "UTF-8 Unicode", "utf8mb4_0900_ai_ci", int64(4)},
		{"utf8mb3", "UTF-8 Unicode", "utf8mb3_general_ci", int64(3)},
		{"utf8", "UTF-8 Unicode", "utf8_general_ci", int64(3)},
		{"binary", "Binary pseudo charset", "binary", int64(1)},
	}
	rows = filterShowRowsByLike(rows, ResolveShowLikePattern("character set", nil, rawQuery))
	rows = filterShowRowsByWhere(rows, columns, ResolveShowWhereExpr("character set", nil, rawQuery))
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: map[string]interface{}{"columns": columns, "rows": rows}, Message: fmt.Sprintf("Found %d character sets", len(rows))}
}

func (e *XMySQLExecutor) executeShowCollation(ctx *ExecutionContext, rawQuery string) {
	columns := []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen", "Pad_attribute"}
	rows := [][]interface{}{
		{"utf8mb4_0900_ai_ci", "utf8mb4", int64(255), "Yes", "Yes", int64(0), "NO PAD"},
		{"utf8mb4_general_ci", "utf8mb4", int64(45), "", "Yes", int64(1), "PAD SPACE"},
		{"utf8mb3_general_ci", "utf8mb3", int64(33), "Yes", "Yes", int64(1), "PAD SPACE"},
		{"utf8_general_ci", "utf8", int64(33), "Yes", "Yes", int64(1), "PAD SPACE"},
		{"binary", "binary", int64(63), "Yes", "Yes", int64(1), "PAD SPACE"},
	}
	rows = filterShowRowsByLike(rows, ResolveShowLikePattern("collation", nil, rawQuery))
	rows = filterShowRowsByWhere(rows, columns, ResolveShowWhereExpr("collation", nil, rawQuery))
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: map[string]interface{}{"columns": columns, "rows": rows}, Message: fmt.Sprintf("Found %d collations", len(rows))}
}

func (e *XMySQLExecutor) executeShowTableStatus(ctx *ExecutionContext, rawQuery string, session server.MySQLServerSession) {
	databaseName := strings.TrimSpace(ctx.DatabaseName)
	if session != nil {
		if value, ok := session.GetParamByName("database").(string); ok && strings.TrimSpace(value) != "" {
			databaseName = strings.TrimSpace(value)
		}
	}
	targetPattern := regexp.MustCompile(`(?is)\b(?:from|in)\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?")
	if match := targetPattern.FindStringSubmatch(rawQuery); len(match) == 2 {
		databaseName = match[1]
	}
	if databaseName == "" {
		ctx.Results <- &Result{Err: fmt.Errorf("no database selected"), ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	if err := e.validateDatabaseExists(databaseName); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	statusTables := make([]string, 0)
	for _, table := range e.scanFrmTables() {
		if !strings.EqualFold(table.schemaName, databaseName) {
			continue
		}
		_, visible := temporaryMetadataTableName(session, databaseName, table.tableName)
		if !visible || !e.informationSchemaTableVisible(session, databaseName, table.tableName, false) {
			continue
		}
		statusTables = append(statusTables, databaseName+"."+table.tableName)
	}
	sort.Strings(statusTables)
	if len(statusTables) > 0 {
		lockQuery := "select * from " + strings.Join(statusTables, " join ")
		releaseTableLocks, lockErr := e.acquireStatementTableLocks(ctx, session, lockQuery, databaseName)
		if lockErr != nil {
			ctx.Results <- &Result{Err: tableLockWaitError(lockErr), ResultType: common.RESULT_TYPE_QUERY}
			return
		}
		defer releaseTableLocks()
	}

	columns := []string{"Name", "Engine", "Version", "Row_format", "Rows", "Avg_row_length", "Data_length", "Max_data_length", "Index_length", "Data_free", "Auto_increment", "Create_time", "Update_time", "Check_time", "Collation", "Checksum", "Create_options", "Comment"}
	rows := make([][]interface{}, 0)
	for _, table := range e.scanFrmTables() {
		if !strings.EqualFold(table.schemaName, databaseName) {
			continue
		}
		visibleName, visible := temporaryMetadataTableName(session, databaseName, table.tableName)
		if !visible || !e.informationSchemaTableVisible(session, databaseName, table.tableName, false) {
			continue
		}
		engineName, collation, createOptions := persistedTableDisplayOptions(e, databaseName, table.tableName)
		rowFormat := persistedTableRowFormat(e, databaseName, table.tableName)
		autoIncrement := persistedAutoIncrementValue(e.getDataDir(), databaseName, table.tableName, table.columns)
		rowCount, avgRowLength, dataLength, indexLength := e.informationSchemaTableSizes(databaseName, table.tableName)
		rows = append(rows, []interface{}{
			visibleName, engineName, int64(10), rowFormat,
			rowCount, avgRowLength, dataLength, nil, indexLength, int64(0),
			autoIncrement, nil, nil, nil, collation, nil, createOptions, persistedTableComment(e, databaseName, table.tableName),
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		return strings.ToLower(fmt.Sprint(rows[i][0])) < strings.ToLower(fmt.Sprint(rows[j][0]))
	})
	rows = filterShowRowsByLike(rows, ResolveShowLikePattern("table status", nil, rawQuery))
	rows = filterShowRowsByWhere(rows, columns, ResolveShowWhereExpr("table status", nil, rawQuery))
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: map[string]interface{}{"columns": columns, "rows": rows}, Message: fmt.Sprintf("Found %d tables", len(rows))}
}

func (e *XMySQLExecutor) executeShowTriggers(ctx *ExecutionContext, rawQuery string, session server.MySQLServerSession) {
	databaseName := strings.TrimSpace(ctx.DatabaseName)
	if session != nil {
		if value, ok := session.GetParamByName("database").(string); ok && strings.TrimSpace(value) != "" {
			databaseName = strings.TrimSpace(value)
		}
	}
	targetPattern := regexp.MustCompile(`(?is)\b(?:from|in)\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?")
	if match := targetPattern.FindStringSubmatch(rawQuery); len(match) == 2 {
		databaseName = match[1]
	}
	if databaseName == "" {
		ctx.Results <- &Result{Err: fmt.Errorf("no database selected"), ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	if err := e.validateDatabaseExists(databaseName); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	schemaPrivilegeErr := e.checkStoredTriggerPrivilege(ctx, databaseName)

	objects := e.scanStoredObjects("trigger")
	releaseObjectLocks, lockErr := e.acquireStoredObjectReadLocks(ctx, session, objects, databaseName)
	if lockErr != nil {
		ctx.Results <- &Result{Err: tableLockWaitError(lockErr), ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	defer releaseObjectLocks()

	columns := []string{"Trigger", "Event", "Table", "Statement", "Timing", "Created", "sql_mode", "Definer", "character_set_client", "collation_connection", "Database Collation"}
	rows := make([][]interface{}, 0)
	for _, object := range objects {
		if !strings.EqualFold(object.Schema, databaseName) {
			continue
		}
		timing, event, tableName, action := parseTriggerInformationSchemaMetadata(object.Definition)
		if schemaPrivilegeErr != nil {
			if err := e.checkStoredTriggerPrivilege(ctx, databaseName, tableName); err != nil {
				continue
			}
		}
		_, collation, _ := persistedTableDisplayOptions(e, databaseName, tableName)
		definer := object.Definer
		if strings.TrimSpace(definer) == "" {
			definer = "root@localhost"
		}
		rows = append(rows, []interface{}{object.Name, event, tableName, action, timing, object.CreatedAt, "", definer, "utf8mb4", "utf8mb4_0900_ai_ci", collation})
	}
	if schemaPrivilegeErr != nil && len(rows) == 0 {
		ctx.Results <- &Result{Err: schemaPrivilegeErr, ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	sort.Slice(rows, func(i, j int) bool {
		return strings.ToLower(fmt.Sprint(rows[i][0])) < strings.ToLower(fmt.Sprint(rows[j][0]))
	})
	rows = filterShowRowsByLike(rows, ResolveShowLikePattern("triggers", nil, rawQuery))
	rows = filterShowRowsByWhere(rows, columns, ResolveShowWhereExpr("triggers", nil, rawQuery))
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: map[string]interface{}{"columns": columns, "rows": rows}, Message: fmt.Sprintf("Found %d triggers", len(rows))}
}

func (e *XMySQLExecutor) executeShowEvents(ctx *ExecutionContext, rawQuery string, session server.MySQLServerSession) {
	databaseName := strings.TrimSpace(ctx.DatabaseName)
	if session != nil {
		if value, ok := session.GetParamByName("database").(string); ok && strings.TrimSpace(value) != "" {
			databaseName = strings.TrimSpace(value)
		}
	}
	targetPattern := regexp.MustCompile(`(?is)\b(?:from|in)\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?")
	if match := targetPattern.FindStringSubmatch(rawQuery); len(match) == 2 {
		databaseName = match[1]
	}
	if databaseName == "" {
		ctx.Results <- &Result{Err: fmt.Errorf("no database selected"), ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	if err := e.validateDatabaseExists(databaseName); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	if err := e.checkStoredEventPrivilege(ctx, databaseName); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return
	}

	objects := e.scanStoredObjects("event")
	releaseObjectLocks, lockErr := e.acquireStoredObjectReadLocks(ctx, session, objects, databaseName)
	if lockErr != nil {
		ctx.Results <- &Result{Err: tableLockWaitError(lockErr), ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	defer releaseObjectLocks()

	columns := []string{"Db", "Name", "Definer", "Time zone", "Event type", "Execute at", "Interval value", "Interval field", "Starts", "Ends", "Status", "Originator", "character_set_client", "collation_connection", "Database Collation"}
	rows := make([][]interface{}, 0)
	for _, object := range objects {
		if !strings.EqualFold(object.Schema, databaseName) {
			continue
		}
		eventType, executeAt, intervalValue, intervalField := parseEventInformationSchemaMetadata(object.Definition)
		starts := interface{}(nil)
		if match := regexp.MustCompile(`(?is)\bstarts\s+'([^']+)'`).FindStringSubmatch(object.Definition); len(match) == 2 {
			starts = match[1]
		}
		ends := interface{}(nil)
		if match := regexp.MustCompile(`(?is)\bends\s+'([^']+)'`).FindStringSubmatch(object.Definition); len(match) == 2 {
			ends = match[1]
		}
		definer := object.Definer
		if strings.TrimSpace(definer) == "" {
			definer = "root@localhost"
		}
		status := "ENABLED"
		if object.Disabled {
			status = "DISABLED"
		}
		rows = append(rows, []interface{}{databaseName, object.Name, definer, "SYSTEM", eventType, executeAt, intervalValue, intervalField, starts, ends, status, int64(0), "utf8mb4", "utf8mb4_0900_ai_ci", "utf8mb4_0900_ai_ci"})
	}
	sort.Slice(rows, func(i, j int) bool {
		return strings.ToLower(fmt.Sprint(rows[i][1])) < strings.ToLower(fmt.Sprint(rows[j][1]))
	})
	if like := ResolveShowLikePattern("events", nil, rawQuery); strings.TrimSpace(like) != "" {
		filtered := make([][]interface{}, 0, len(rows))
		for _, row := range rows {
			if len(row) > 1 && sqlLikeMatch(fmt.Sprint(row[1]), like) {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}
	rows = filterShowRowsByWhere(rows, columns, ResolveShowWhereExpr("events", nil, rawQuery))
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: map[string]interface{}{"columns": columns, "rows": rows}, Message: fmt.Sprintf("Found %d events", len(rows))}
}

func (e *XMySQLExecutor) executeShowRoutineStatus(ctx *ExecutionContext, rawQuery string, session server.MySQLServerSession, routineType string) {
	databaseName := strings.TrimSpace(ctx.DatabaseName)
	if session != nil {
		if value, ok := session.GetParamByName("database").(string); ok && strings.TrimSpace(value) != "" {
			databaseName = strings.TrimSpace(value)
		}
	}
	targetPattern := regexp.MustCompile(`(?is)\b(?:from|in)\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?")
	if match := targetPattern.FindStringSubmatch(rawQuery); len(match) == 2 {
		databaseName = match[1]
	}
	if databaseName == "" {
		ctx.Results <- &Result{Err: fmt.Errorf("no database selected"), ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	if err := e.validateDatabaseExists(databaseName); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	if err := e.checkStoredRoutineShow(ctx); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return
	}

	objects := e.scanStoredObjects(routineType)
	releaseObjectLocks, lockErr := e.acquireStoredObjectReadLocks(ctx, session, objects, databaseName)
	if lockErr != nil {
		ctx.Results <- &Result{Err: tableLockWaitError(lockErr), ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	defer releaseObjectLocks()

	columns := []string{"Db", "Name", "Type", "Definer", "Modified", "Created", "Security_type", "Comment", "character_set_client", "collation_connection", "Database Collation"}
	rows := make([][]interface{}, 0)
	for _, object := range objects {
		if !strings.EqualFold(object.Schema, databaseName) {
			continue
		}
		definer := object.Definer
		if strings.TrimSpace(definer) == "" {
			definer = "root@localhost"
		}
		security := object.SQLSecurity
		if strings.TrimSpace(security) == "" {
			security = "DEFINER"
		}
		rows = append(rows, []interface{}{databaseName, object.Name, strings.ToUpper(routineType), definer, object.CreatedAt, object.CreatedAt, strings.ToUpper(security), object.RoutineComment, "utf8mb4", "utf8mb4_0900_ai_ci", "utf8mb4_0900_ai_ci"})
	}
	sort.Slice(rows, func(i, j int) bool {
		return strings.ToLower(fmt.Sprint(rows[i][1])) < strings.ToLower(fmt.Sprint(rows[j][1]))
	})
	if like := ResolveShowLikePattern(routineType+" status", nil, rawQuery); strings.TrimSpace(like) != "" {
		filtered := make([][]interface{}, 0, len(rows))
		for _, row := range rows {
			if len(row) > 1 && sqlLikeMatch(fmt.Sprint(row[1]), like) {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}
	rows = filterShowRowsByWhere(rows, columns, ResolveShowWhereExpr(routineType+" status", nil, rawQuery))
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: map[string]interface{}{"columns": columns, "rows": rows}, Message: fmt.Sprintf("Found %d %s(s)", len(rows), routineType)}
}

// executeShowTables 执行 SHOW TABLES
func (e *XMySQLExecutor) executeShowTables(ctx *ExecutionContext, stmt *sqlparser.Show, session server.MySQLServerSession, rawQuery string) {
	logger.Debugf(" [executeShowTables] 执行SHOW TABLES")

	if session == nil {
		ctx.Results <- &Result{
			Err:        fmt.Errorf("session is required for SHOW TABLES"),
			ResultType: "ERROR",
		}
		return
	}

	// 获取当前数据库（优先使用 SHOW TABLES FROM db 显式指定）
	currentDB := ""
	if stmt != nil && stmt.ShowTablesOpt != nil {
		currentDB = strings.TrimSpace(stmt.ShowTablesOpt.DbName)
	}
	if currentDB == "" {
		if dbParam := session.GetParamByName("database"); dbParam != nil {
			if dbName, ok := dbParam.(string); ok {
				currentDB = dbName
			}
		}
	}
	if currentDB == "" {
		ctx.Results <- &Result{
			Err:        fmt.Errorf("no database selected"),
			ResultType: "ERROR",
		}
		return
	}
	if !e.hasTemporaryTableFilesInDirectory(currentDB) && !e.hasViewFilesInDirectory(currentDB) && e.tryExecuteShowExecutor(ctx, "TABLES", currentDB, "", stmt, rawQuery) {
		return
	}

	// 通过数据目录扫描实际表文件（.frm/.ibd），确保重启后可见
	dataDir := e.getDataDir()
	dbPath := filepath.Join(dataDir, currentDB)
	entries, err := os.ReadDir(dbPath)
	if err != nil {
		if os.IsNotExist(err) {
			ctx.Results <- &Result{
				Err:        fmt.Errorf("database '%s' does not exist", currentDB),
				ResultType: "ERROR",
			}
			return
		}
		ctx.Results <- &Result{
			Err:        fmt.Errorf("failed to read database '%s': %v", currentDB, err),
			ResultType: "ERROR",
		}
		return
	}
	tableSet := make(map[string]struct{})
	tableViews := make(map[string]bool)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		switch {
		case strings.HasSuffix(name, ".frm"):
			tableSet[strings.TrimSuffix(name, ".frm")] = struct{}{}
		case strings.HasSuffix(name, ".ibd"):
			tableSet[strings.TrimSuffix(name, ".ibd")] = struct{}{}
		case strings.HasSuffix(strings.ToLower(name), ".view.json"):
			viewName := name[:len(name)-len(".view.json")]
			tableSet[viewName] = struct{}{}
			tableViews[viewName] = true
		}
	}
	tables := make([]string, 0, len(tableSet))
	for table := range tableSet {
		tables = append(tables, table)
	}
	tables = e.visibleTemporaryTableNames(session, currentDB, tables)
	visibleTables := tables[:0]
	for _, table := range tables {
		if e.tableMetadataVisibleToSession(&ExecutionContext{Session: session}, currentDB, table, tableViews[table]) {
			visibleTables = append(visibleTables, table)
		}
	}
	tables = visibleTables
	sort.Strings(tables)
	logger.Debugf(" [executeShowTables] 当前数据库: %s", currentDB)

	// 构造结果集
	rows := make([][]interface{}, len(tables))
	for i, table := range tables {
		rows[i] = []interface{}{table}
	}
	likePattern := ResolveShowLikePattern("tables", stmt, rawQuery)
	rows = filterShowRowsByLike(rows, likePattern)

	columnName := fmt.Sprintf("Tables_in_%s", currentDB)
	whereExpr := ResolveShowWhereExpr("tables", stmt, rawQuery)
	rows = filterShowRowsByWhere(rows, []string{columnName}, whereExpr)
	resultData := map[string]interface{}{
		"columns": []string{columnName},
		"rows":    rows,
	}

	ctx.Results <- &Result{
		ResultType: "QUERY",
		Data:       resultData,
		Message:    fmt.Sprintf("Found %d tables", len(rows)),
	}

	logger.Debugf(" [executeShowTables] 返回 %d 个表", len(tables))
}

func (e *XMySQLExecutor) tableMetadataVisibleToSession(ctx *ExecutionContext, schema, table string, isView bool) bool {
	if ctx == nil || ctx.Session == nil {
		return true
	}
	user, _ := ctx.Session.GetParamByName("user").(string)
	user = strings.TrimSpace(user)
	if user == "" || strings.EqualFold(user, "root") {
		return true
	}
	file, err := e.accountFileForSession(ctx)
	if err != nil {
		return false
	}
	account := sessionAccount(file, ctx.Session)
	if account == nil {
		return false
	}
	grants := effectiveAccountGrants(file, *account, ctx.Session)
	requested := strings.TrimSpace(schema) + "." + strings.TrimSpace(table)
	for scope, privileges := range grants {
		if !scopeCovers(requested, scope) {
			continue
		}
		for _, privilege := range privileges {
			privilege = strings.TrimSpace(privilege)
			if privilege == "" || strings.EqualFold(privilege, "GRANT OPTION") || strings.EqualFold(privilege, "USAGE") {
				continue
			}
			if !isView || strings.EqualFold(privilege, "SHOW VIEW") || strings.EqualFold(privilege, "SELECT") || strings.EqualFold(privilege, "ALL") || strings.EqualFold(privilege, "ALL PRIVILEGES") {
				return true
			}
		}
	}
	// A column-level grant is also sufficient to reveal the table metadata.
	// Keep this separate from the table-scope loop because column grants are
	// persisted in mysql.columns_priv-equivalent state and may be inherited
	// through the session's active roles.
	for scope, privileges := range effectiveAccountColumnGrants(file, *account, ctx.Session) {
		parts := strings.Split(strings.ToLower(strings.TrimSpace(scope)), ".")
		if len(parts) != 3 ||
			(parts[0] != "*" && parts[0] != strings.ToLower(strings.TrimSpace(schema))) ||
			(parts[1] != "*" && parts[1] != strings.ToLower(strings.TrimSpace(table))) {
			continue
		}
		for _, privilege := range privileges {
			privilege = strings.TrimSpace(privilege)
			if privilege != "" && !strings.EqualFold(privilege, "GRANT OPTION") && !strings.EqualFold(privilege, "USAGE") {
				return true
			}
		}
	}
	return false
}

func (e *XMySQLExecutor) informationSchemaTableVisible(session server.MySQLServerSession, schema, table string, isView bool) bool {
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(table)), "__xmysql_tmp_") {
		return true
	}
	return e.tableMetadataVisibleToSession(&ExecutionContext{Session: session}, schema, table, isView)
}

func (e *XMySQLExecutor) schemaMetadataVisibleToSession(session server.MySQLServerSession, schema string) bool {
	if session == nil {
		return true
	}
	user, _ := session.GetParamByName("user").(string)
	user = strings.TrimSpace(user)
	if user == "" || strings.EqualFold(user, "root") || strings.EqualFold(schema, "information_schema") {
		return true
	}
	for _, table := range e.scanFrmTables() {
		if strings.EqualFold(table.schemaName, schema) && e.informationSchemaTableVisible(session, table.schemaName, table.tableName, false) {
			return true
		}
	}
	entries, _ := os.ReadDir(filepath.Join(e.getDataDir(), schema))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".view.json") {
			continue
		}
		viewName := strings.TrimSuffix(entry.Name(), ".view.json")
		if e.informationSchemaTableVisible(session, schema, viewName, true) {
			return true
		}
	}
	ctx := &ExecutionContext{Session: session}
	file, err := e.accountFileForSession(ctx)
	if err != nil {
		return false
	}
	account := sessionAccount(file, session)
	if account == nil {
		return false
	}
	for scope, privileges := range effectiveAccountGrants(file, *account, session) {
		if !scopeCovers(strings.TrimSpace(schema)+".*", scope) {
			continue
		}
		for _, privilege := range privileges {
			privilege = strings.TrimSpace(privilege)
			if privilege != "" && !strings.EqualFold(privilege, "USAGE") && !strings.EqualFold(privilege, "GRANT OPTION") {
				return true
			}
		}
	}
	return false
}

func (e *XMySQLExecutor) isViewMetadataTable(table frmMetadataTable) bool {
	_, err := os.Stat(filepath.Join(e.getDataDir(), table.schemaName, table.tableName+".view.json"))
	return err == nil
}

var showFullTablesPattern = regexp.MustCompile("(?is)^\\s*show\\s+full\\s+tables(?:\\s+(?:from|in)\\s+`?([A-Za-z0-9_$]+)`?)?(?:\\s+like\\s+'([^']*)')?(?:\\s+where\\s+[^;]+)?\\s*;?\\s*$")

func isShowFullTablesQuery(query string) bool {
	return showFullTablesPattern.MatchString(query)
}

func (e *XMySQLExecutor) executeShowFullTablesRaw(ctx *ExecutionContext, session server.MySQLServerSession, rawQuery string, databaseName string) {
	matches := showFullTablesPattern.FindStringSubmatch(rawQuery)
	currentDB := ""
	likePattern := ""
	if len(matches) > 1 {
		currentDB = strings.TrimSpace(matches[1])
	}
	if len(matches) > 2 {
		likePattern = strings.TrimSpace(matches[2])
	}
	if currentDB == "" {
		currentDB = databaseName
	}
	if currentDB == "" && session != nil {
		if dbParam := session.GetParamByName("database"); dbParam != nil {
			if dbName, ok := dbParam.(string); ok {
				currentDB = dbName
			}
		}
	}
	if currentDB == "" {
		ctx.Results <- &Result{
			Err:        fmt.Errorf("no database selected"),
			ResultType: "ERROR",
		}
		return
	}

	tables, err := e.showTableNames(currentDB)
	if err != nil {
		ctx.Results <- &Result{Err: err, ResultType: "ERROR"}
		return
	}
	tables = e.visibleTemporaryTableNames(session, currentDB, tables)
	visibleTables := tables[:0]
	for _, table := range tables {
		isView := false
		if _, viewErr := os.Stat(filepath.Join(e.getDataDir(), currentDB, table+".view.json")); viewErr == nil {
			isView = true
		}
		if e.tableMetadataVisibleToSession(&ExecutionContext{Session: session}, currentDB, table, isView) {
			visibleTables = append(visibleTables, table)
		}
	}
	tables = visibleTables

	rows := make([][]interface{}, 0, len(tables))
	for _, table := range tables {
		if !metadataPatternMatches(table, likePattern) {
			continue
		}
		tableType := "BASE TABLE"
		if _, viewErr := os.Stat(filepath.Join(e.getDataDir(), currentDB, table+".view.json")); viewErr == nil {
			tableType = "VIEW"
		}
		rows = append(rows, []interface{}{table, tableType})
	}

	ctx.Results <- &Result{
		ResultType: common.RESULT_TYPE_QUERY,
		Data: map[string]interface{}{
			"columns": []string{fmt.Sprintf("Tables_in_%s", currentDB), "Table_type"},
			"rows":    rows,
		},
		Message: fmt.Sprintf("Found %d tables", len(rows)),
	}
}

func (e *XMySQLExecutor) showTableNames(currentDB string) ([]string, error) {
	dataDir := e.getDataDir()
	dbPath := filepath.Join(dataDir, currentDB)
	entries, err := os.ReadDir(dbPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("database '%s' does not exist", currentDB)
		}
		return nil, fmt.Errorf("failed to read database '%s': %v", currentDB, err)
	}
	tableSet := make(map[string]struct{})
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		switch {
		case strings.HasSuffix(name, ".frm"):
			tableSet[strings.TrimSuffix(name, ".frm")] = struct{}{}
		case strings.HasSuffix(name, ".ibd"):
			tableSet[strings.TrimSuffix(name, ".ibd")] = struct{}{}
		case strings.HasSuffix(strings.ToLower(name), ".view.json"):
			tableSet[name[:len(name)-len(".view.json")]] = struct{}{}
		}
	}
	tables := make([]string, 0, len(tableSet))
	for table := range tableSet {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables, nil
}

func (e *XMySQLExecutor) hasViewFilesInDirectory(databaseName string) bool {
	entries, err := os.ReadDir(filepath.Join(e.getDataDir(), databaseName))
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(strings.ToLower(entry.Name()), ".view.json") {
			return true
		}
	}
	return false
}

func (e *XMySQLExecutor) executeShowFullColumns(ctx *ExecutionContext, query, databaseName string) {
	match := regexp.MustCompile(`(?is)^show\s+full\s+(?:columns|fields)\s+(?:from|in)\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `(?:\s+from\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `)?`).FindStringSubmatch(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	if len(match) == 0 {
		ctx.Results <- &Result{Err: fmt.Errorf("SHOW FULL COLUMNS requires a table name"), ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	schemaName, tableName := databaseName, match[1]
	if match[2] != "" {
		schemaName, tableName = match[1], match[2]
	}
	_, temporary := temporaryPhysicalTableName(ctx.Session, schemaName, tableName)
	if !temporary {
		releaseTableLock, lockErr := e.acquireStatementTableLocks(ctx, ctx.Session, "select * from "+schemaName+"."+tableName, schemaName)
		if lockErr != nil {
			ctx.Results <- &Result{Err: tableLockWaitError(lockErr), ResultType: common.RESULT_TYPE_QUERY}
			return
		}
		defer releaseTableLock()
	}
	if _, viewErr := os.Stat(filepath.Join(e.getDataDir(), schemaName, tableName+".view.json")); viewErr == nil {
		columns, err := e.viewColumnMetadata(schemaName, tableName)
		if err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return
		}
		rows := make([][]interface{}, 0, len(columns))
		for _, column := range columns {
			if column == nil {
				continue
			}
			collation := interface{}(nil)
			if isCharacterColumnType(string(column.Type)) {
				collation = column.Collation
				if collation == "" {
					collation = "utf8mb4_general_ci"
				}
			}
			rows = append(rows, []interface{}{column.Name, strings.ToLower(e.formatShowColumnType(column.Type, column.Length)), collation, "YES", "", column.DefaultValue, "", "select,insert,update,references", column.Comment})
		}
		rows = filterShowFullColumnRows(rows, query)
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: map[string]interface{}{
			"columns": []string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"},
			"rows":    rows,
		}, Message: fmt.Sprintf("Found %d columns", len(rows))}
		return
	}
	info, err := e.readPersistedTableInfo(schemaName, tableName)
	if err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	rows := make([][]interface{}, 0, len(info.Columns))
	for _, column := range info.Columns {
		name, _ := column["name"].(string)
		if name == "" {
			continue
		}
		key := ""
		if primary, _ := column["primary"].(bool); primary {
			key = "PRI"
		} else if unique, _ := column["unique"].(bool); unique {
			key = "UNI"
		}
		nullText := "YES"
		if nullable, ok := column["nullable"].(bool); ok && !nullable {
			nullText = "NO"
		}
		extra := ""
		if autoIncrement, _ := column["auto_increment"].(bool); autoIncrement {
			extra = "auto_increment"
		}
		var collation interface{}
		if isCharacterColumnType(fmt.Sprint(column["type"])) {
			collation = column["collate"]
			if collation == "" {
				collation = "utf8mb4_general_ci"
			}
		}
		rows = append(rows, []interface{}{name, formatPersistedColumnType(column), collation, nullText, key, persistedDefault(column), extra, "select,insert,update,references", persistedString(column["comment"])})
	}
	rows = filterShowFullColumnRows(rows, query)
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: map[string]interface{}{
		"columns": []string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"},
		"rows":    rows,
	}, Message: fmt.Sprintf("Found %d columns", len(rows))}
}

func filterShowFullColumnRows(rows [][]interface{}, query string) [][]interface{} {
	rows = filterShowRowsByLike(rows, ResolveShowLikePattern("columns", nil, query))
	return filterShowRowsByWhere(rows,
		[]string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"},
		ResolveShowWhereExpr("columns", nil, query))
}

func isCharacterColumnType(typeName string) bool {
	switch strings.ToLower(strings.TrimSpace(typeName)) {
	case "char", "varchar", "text", "tinytext", "mediumtext", "longtext", "enum", "set":
		return true
	default:
		return false
	}
}

// executeShowColumns 执行 SHOW COLUMNS
func (e *XMySQLExecutor) executeShowColumns(ctx *ExecutionContext, stmt *sqlparser.Show, rawQuery string) {
	logger.Debugf(" [executeShowColumns] 执行SHOW COLUMNS")

	schemaName, tableName, err := e.resolveShowColumnsTarget(ctx, stmt, rawQuery)
	if err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: "ERROR",
		}
		return
	}
	_, temporary := temporaryPhysicalTableName(ctx.Session, schemaName, tableName)
	if !temporary {
		releaseTableLock, lockErr := e.acquireStatementTableLocks(ctx, ctx.Session, "select * from "+schemaName+"."+tableName, schemaName)
		if lockErr != nil {
			ctx.Results <- &Result{Err: tableLockWaitError(lockErr), ResultType: common.RESULT_TYPE_QUERY}
			return
		}
		defer releaseTableLock()
	}
	viewPath := filepath.Join(e.getDataDir(), schemaName, tableName+".view.json")
	if _, viewErr := os.Stat(viewPath); viewErr == nil {
		if err := e.checkViewMetadataPrivilege(ctx, schemaName); err != nil {
			ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: map[string]interface{}{
				"columns": []string{"Field", "Type", "Null", "Key", "Default", "Extra"}, "rows": [][]interface{}{},
			}}
			return
		}
	} else if os.IsNotExist(viewErr) {
		_, temporary := temporaryPhysicalTableName(ctx.Session, schemaName, tableName)
		if !temporary && !e.tableMetadataVisibleToSession(ctx, schemaName, tableName, false) {
			ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: map[string]interface{}{
				"columns": []string{"Field", "Type", "Null", "Key", "Default", "Extra"}, "rows": [][]interface{}{},
			}}
			return
		}
	}
	if _, viewErr := os.Stat(filepath.Join(e.getDataDir(), schemaName, tableName+".view.json")); viewErr != nil && !os.IsNotExist(viewErr) {
		ctx.Results <- &Result{Err: viewErr, ResultType: "ERROR"}
		return
	}

	tableMeta, err := e.getShowColumnsTableMetadata(schemaName, tableName)
	if err != nil {
		if e.tryExecuteShowExecutor(ctx, "COLUMNS", schemaName, tableName, stmt, rawQuery) {
			return
		}
		ctx.Results <- &Result{
			Err:        fmt.Errorf("failed to get table metadata for %s.%s: %v", schemaName, tableName, err),
			ResultType: "ERROR",
		}
		return
	}

	if len(tableMeta.Columns) == 0 {
		ctx.Results <- &Result{
			Err:        fmt.Errorf("table '%s.%s' has no columns", schemaName, tableName),
			ResultType: "ERROR",
		}
		return
	}

	rows := make([][]interface{}, 0, len(tableMeta.Columns))
	for _, col := range tableMeta.Columns {
		if col == nil {
			continue
		}

		keyText := ""
		if col.IsPrimary {
			keyText = "PRI"
		} else if col.IsUnique {
			keyText = "UNI"
		}

		nullText := "YES"
		if !col.IsNullable {
			nullText = "NO"
		}

		extraText := ""
		if col.IsAutoIncrement {
			extraText = "auto_increment"
		}

		rows = append(rows, []interface{}{
			col.Name,
			e.formatShowColumnType(col.Type, col.Length),
			nullText,
			keyText,
			col.DefaultValue,
			extraText,
		})
	}
	likePattern := ResolveShowLikePattern("columns", stmt, rawQuery)
	rows = filterShowRowsByLike(rows, likePattern)
	whereExpr := ResolveShowWhereExpr("columns", stmt, rawQuery)
	rows = filterShowRowsByWhere(rows, []string{"Field", "Type", "Null", "Key", "Default", "Extra"}, whereExpr)

	resultData := map[string]interface{}{
		"columns": []string{"Field", "Type", "Null", "Key", "Default", "Extra"},
		"rows":    rows,
	}

	ctx.Results <- &Result{
		ResultType: "QUERY",
		Data:       resultData,
		Message:    fmt.Sprintf("Found %d columns", len(rows)),
	}

	logger.Debugf(" [executeShowColumns] 返回 %d 列定义", len(rows))
}

func (e *XMySQLExecutor) tryExecuteShowExecutor(ctx *ExecutionContext, showType, schemaName, tableName string, stmt *sqlparser.Show, rawQuery string) bool {
	if e == nil || e.infosSchemaManager == nil {
		return false
	}

	executor := &ShowExecutor{
		showType:          showType,
		schemaName:        schemaName,
		tableName:         tableName,
		infoSchemaManager: e.infosSchemaManager,
		rows:              make([][]interface{}, 0),
		current:           -1,
	}
	if err := executor.Init(); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: "ERROR"}
		return true
	}
	defer executor.Close()

	rows := make([][]interface{}, 0)
	for {
		if err := executor.Next(); err != nil {
			break
		}
		row := executor.GetRow()
		if row != nil {
			rows = append(rows, row)
		}
	}

	columns := showExecutorColumnNames(executor.Schema())
	normalizedType := strings.ToLower(strings.TrimSpace(showType))
	likePattern := ResolveShowLikePattern(normalizedType, stmt, rawQuery)
	rows = filterShowRowsByLike(rows, likePattern)
	whereExpr := ResolveShowWhereExpr(normalizedType, stmt, rawQuery)
	rows = filterShowRowsByWhere(rows, columns, whereExpr)
	if normalizedType == "tables" && len(rows) == 0 {
		return false
	}

	ctx.Results <- &Result{
		ResultType: "QUERY",
		Data: map[string]interface{}{
			"columns": columns,
			"rows":    rows,
		},
		Message: fmt.Sprintf("Found %d %s", len(rows), showExecutorResultNoun(normalizedType)),
	}
	return true
}

func showExecutorColumnNames(schema *metadata.Table) []string {
	if schema == nil {
		return []string{"Value"}
	}
	columns := make([]string, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		if col == nil {
			continue
		}
		columns = append(columns, col.Name)
	}
	return columns
}

func showExecutorResultNoun(showType string) string {
	switch showType {
	case "databases":
		return "databases"
	case "tables":
		return "tables"
	case "columns", "fields":
		return "columns"
	default:
		return "rows"
	}
}

func (e *XMySQLExecutor) resolveShowColumnsTarget(ctx *ExecutionContext, stmt *sqlparser.Show, rawQuery string) (string, string, error) {
	if stmt == nil {
		return "", "", fmt.Errorf("invalid SHOW COLUMNS statement")
	}

	tableName := strings.TrimSpace(stmt.OnTable.Name.String())
	if tableName == "" && rawQuery != "" {
		tableName = extractShowColumnsTableNameFromQuery(rawQuery)
	}
	if tableName == "" {
		return "", "", fmt.Errorf("table name is required for SHOW COLUMNS/FIELDS")
	}

	schemaName := strings.TrimSpace(stmt.OnTable.Qualifier.String())
	if schemaName == "" && ctx != nil {
		schemaName = strings.TrimSpace(ctx.DatabaseName)
	}
	if schemaName == "" {
		return "", tableName, fmt.Errorf("database name is required for SHOW COLUMNS/FIELDS")
	}

	return schemaName, tableName, nil
}

func (e *XMySQLExecutor) getShowColumnsTableMetadata(schemaName, tableName string) (*metadata.TableMeta, error) {
	if _, viewErr := os.Stat(filepath.Join(e.getDataDir(), schemaName, tableName+".view.json")); viewErr == nil {
		columns, err := e.viewColumnMetadata(schemaName, tableName)
		if err != nil {
			return nil, err
		}
		return &metadata.TableMeta{Name: tableName, Columns: columns}, nil
	}
	if info, infoErr := e.readPersistedTableInfo(schemaName, tableName); infoErr == nil && len(info.Columns) > 0 {
		return persistedTableMetaForShowColumns(info, tableName), nil
	}
	if e.tableManager != nil {
		if tableManager, ok := e.tableManager.(*manager.TableManager); ok {
			tableMeta, err := tableManager.GetTableMetadata(context.Background(), schemaName, tableName)
			if err == nil && tableMeta != nil {
				return tableMeta, nil
			}
		}
	}

	dataDir := e.getDataDir()
	if dataDir == "" {
		return nil, fmt.Errorf("no data directory configured")
	}

	se := &SelectExecutor{dataDir: dataDir}
	meta, err := se.loadTableMetaFromFrm(dataDir, schemaName, tableName)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

func persistedTableMetaForShowColumns(info *persistedTableInfo, tableName string) *metadata.TableMeta {
	meta := &metadata.TableMeta{Name: tableName, Comment: info.TableComment, Columns: make([]*metadata.ColumnMeta, 0, len(info.Columns))}
	for _, column := range info.Columns {
		name, _ := column["name"].(string)
		if strings.TrimSpace(name) == "" {
			continue
		}
		typeName, _ := column["type"].(string)
		if strings.TrimSpace(typeName) == "" {
			typeName = string(metadata.TypeVarchar)
		}
		length := 0
		if value, ok := column["length"].(float64); ok {
			length = int(value)
		}
		scale := 0
		if value, ok := column["scale"].(float64); ok {
			scale = int(value)
		}
		nullable := true
		if value, ok := column["nullable"].(bool); ok {
			nullable = value
		}
		defaultValue := column["default"]
		if defaultValue == nil {
			defaultValue = column["default_value"]
		}
		meta.Columns = append(meta.Columns, &metadata.ColumnMeta{
			Name:            name,
			Type:            metadata.DataType(typeName),
			Length:          length,
			Scale:           scale,
			IsNullable:      nullable,
			IsPrimary:       boolValueFromMap(column, "primary"),
			IsUnique:        boolValueFromMap(column, "unique"),
			IsAutoIncrement: boolValueFromMap(column, "auto_increment"),
			IsUnsigned:      boolValueFromMap(column, "unsigned"),
			IsGenerated:     boolValueFromMap(column, "generated"),
			DefaultValue:    defaultValue,
		})
	}
	if len(meta.PrimaryKey) == 0 {
		for _, index := range info.Indexes {
			name, _ := index["name"].(string)
			primary, _ := index["primary"].(bool)
			if !primary && !strings.EqualFold(name, "PRIMARY") {
				continue
			}
			rawColumns, _ := index["columns"].([]interface{})
			for _, rawColumn := range rawColumns {
				columnName := fmt.Sprint(rawColumn)
				if strings.TrimSpace(columnName) == "" {
					continue
				}
				meta.PrimaryKey = append(meta.PrimaryKey, columnName)
				for _, column := range meta.Columns {
					if column != nil && strings.EqualFold(column.Name, columnName) {
						column.IsPrimary = true
					}
				}
			}
			break
		}
	}
	return meta
}

func boolValueFromMap(values map[string]interface{}, key string) bool {
	value, _ := values[key].(bool)
	return value
}

func (e *XMySQLExecutor) formatShowColumnType(columnType metadata.DataType, length int) string {
	typeName := strings.TrimSpace(string(columnType))
	if typeName == "" {
		typeName = string(metadata.TypeVarchar)
	}

	if length > 0 {
		switch metadata.DataType(strings.ToUpper(typeName)) {
		case metadata.TypeChar, metadata.TypeVarchar, metadata.TypeBinary, metadata.TypeVarBinary:
			return fmt.Sprintf("%s(%d)", strings.ToUpper(typeName), length)
		}
	}

	return typeName
}

// executeShowVariables 执行 SHOW VARIABLES
func (e *XMySQLExecutor) executeShowVariables(ctx *ExecutionContext, stmt *sqlparser.Show) {
	e.executeShowVariablesWithQuery(ctx, stmt, "")
}

// executeShowVariablesWithQuery 执行 SHOW VARIABLES，支持LIKE和WHERE
func (e *XMySQLExecutor) executeShowVariablesWithQuery(ctx *ExecutionContext, stmt *sqlparser.Show, rawQuery string) {
	logger.Debugf(" [executeShowVariables] 执行SHOW VARIABLES")

	// 简化实现，返回一些常见变量
	rows := [][]interface{}{
		{"character_set_client", "utf8mb4"},
		{"character_set_connection", "utf8mb4"},
		{"character_set_database", "utf8mb4"},
		{"character_set_results", "utf8mb4"},
		{"character_set_server", "utf8mb4"},
		{"collation_connection", "utf8mb4_general_ci"},
		{"collation_database", "utf8mb4_general_ci"},
		{"collation_server", "utf8mb4_general_ci"},
		{"version", "8.0.0-xmysql"},
		{"version_comment", "XMySQL Server"},
	}
	if e.storageManager != nil && e.storageManager.GetSystemVariablesManager() != nil {
		scope := manager.SessionScope
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(rawQuery)), "show global variables") {
			scope = manager.GlobalScope
		}
		if value, err := e.storageManager.GetSystemVariablesManager().GetVariable("", "read_only", scope); err == nil {
			readOnlyValue := "OFF"
			if sessionBoolValue(value) {
				readOnlyValue = "ON"
			}
			rows = append(rows, []interface{}{"read_only", readOnlyValue})
		}
		if scope == manager.GlobalScope {
			if value, err := e.storageManager.GetSystemVariablesManager().GetVariable("", "super_read_only", scope); err == nil {
				superReadOnlyValue := "OFF"
				if sessionBoolValue(value) {
					superReadOnlyValue = "ON"
				}
				rows = append(rows, []interface{}{"super_read_only", superReadOnlyValue})
			}
		}
	}

	likePattern := ResolveShowLikePattern("variables", stmt, rawQuery)
	rows = filterShowRowsByLike(rows, likePattern)
	whereExpr := ResolveShowWhereExpr("variables", stmt, rawQuery)
	rows = filterShowRowsByWhere(rows, []string{"Variable_name", "Value"}, whereExpr)

	resultData := map[string]interface{}{
		"columns": []string{"Variable_name", "Value"},
		"rows":    rows,
	}

	ctx.Results <- &Result{
		ResultType: "QUERY",
		Data:       resultData,
		Message:    fmt.Sprintf("Found %d variables", len(rows)),
	}
}

// executeShowStatus 执行 SHOW STATUS
func (e *XMySQLExecutor) executeShowStatus(ctx *ExecutionContext, stmt *sqlparser.Show) {
	e.executeShowStatusWithQuery(ctx, stmt, "")
}

// executeShowStatusWithQuery 执行 SHOW STATUS，支持LIKE和WHERE
func (e *XMySQLExecutor) executeShowStatusWithQuery(ctx *ExecutionContext, stmt *sqlparser.Show, rawQuery string) {
	logger.Debugf(" [executeShowStatus] 执行SHOW STATUS")

	// 简化实现，返回一些状态变量
	rows := [][]interface{}{
		{"Threads_connected", "1"},
		{"Uptime", "3600"},
		{"Questions", "100"},
	}

	likePattern := ResolveShowLikePattern("status", stmt, rawQuery)
	rows = filterShowRowsByLike(rows, likePattern)
	whereExpr := ResolveShowWhereExpr("status", stmt, rawQuery)
	rows = filterShowRowsByWhere(rows, []string{"Variable_name", "Value"}, whereExpr)

	resultData := map[string]interface{}{
		"columns": []string{"Variable_name", "Value"},
		"rows":    rows,
	}

	ctx.Results <- &Result{
		ResultType: "QUERY",
		Data:       resultData,
		Message:    fmt.Sprintf("Found %d status rows", len(rows)),
	}
}

// executeShowCreateTable 执行 SHOW CREATE TABLE
func (e *XMySQLExecutor) executeShowCreateTable(ctx *ExecutionContext, stmt *sqlparser.Show, rawQuery ...string) {
	logger.Debugf(" [executeShowCreateTable] 执行SHOW CREATE TABLE")

	tableName := ""
	if stmt != nil {
		tableName = stmt.OnTable.Name.String()
		if tableName == "" && strings.TrimSpace(stmt.Type) != "" {
			tableName = extractShowCreateTableName(stmt.Type)
		}
	}
	if tableName == "" && len(rawQuery) > 0 {
		tableName = extractShowCreateTableNameFromQuery(rawQuery[0])
	}

	if tableName == "" {
		ctx.Results <- &Result{
			Err:        fmt.Errorf("table name not found in SHOW CREATE TABLE statement"),
			ResultType: "ERROR",
		}
		return
	}

	databaseName := ctx.DatabaseName
	if len(rawQuery) > 0 {
		if schema, qualifiedTable := extractQualifiedTableFromShowCreate(rawQuery[0]); qualifiedTable != "" {
			tableName = qualifiedTable
			if schema != "" {
				databaseName = schema
			}
		}
	}
	physicalTableName, temporary := temporaryPhysicalTableName(ctx.Session, databaseName, tableName)
	if !temporary && databaseName != "" {
		releaseTableLock, lockErr := e.acquireStatementTableLocks(ctx, ctx.Session, "select * from "+databaseName+"."+tableName, databaseName)
		if lockErr != nil {
			ctx.Results <- &Result{Err: tableLockWaitError(lockErr), ResultType: common.RESULT_TYPE_QUERY}
			return
		}
		defer releaseTableLock()
	}
	if !temporary && !e.tableMetadataVisibleToSession(ctx, databaseName, tableName, false) {
		ctx.Results <- &Result{
			Err:        fmt.Errorf("access denied: current user lacks privilege on table '%s.%s'", databaseName, tableName),
			ResultType: common.RESULT_TYPE_QUERY,
		}
		return
	}
	createDDL, err := e.loadCreateTableDefinition(databaseName, physicalTableName)
	if err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	if temporary {
		createDDL = strings.Replace(createDDL,
			fmt.Sprintf("CREATE TABLE `%s`", physicalTableName),
			fmt.Sprintf("CREATE TABLE `%s`", tableName),
			1,
		)
	}
	rows := [][]interface{}{
		{tableName, createDDL},
	}

	resultData := map[string]interface{}{
		"columns": []string{"Table", "Create Table"},
		"rows":    rows,
	}

	ctx.Results <- &Result{
		ResultType: "QUERY",
		Data:       resultData,
	}
}

// executeAdminReadQuery handles DESCRIBE/DESC and EXPLAIN before the legacy
// parser's OtherRead placeholder is dispatched. Both result sets are derived
// from the persisted .frm metadata rather than returning a fixed sample row.
func (e *XMySQLExecutor) executeAdminReadQuery(ctx *ExecutionContext, query, databaseName string) bool {
	lower := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	switch {
	case strings.HasPrefix(lower, "explain "), strings.HasPrefix(lower, "desc select "), strings.HasPrefix(lower, "describe select "):
		e.executeExplainQuery(ctx, query, databaseName)
		return true
	case strings.HasPrefix(lower, "repair table "):
		target := strings.TrimSpace(strings.TrimPrefix(lower, "repair table "))
		ctx.Results <- &Result{Err: fmt.Errorf("The InnoDB storage engine for table '%s' doesn't support repair", target), ResultType: common.RESULT_TYPE_QUERY}
		return true
	case strings.HasPrefix(lower, "check table "):
		e.executeMaintenanceTableQuery(ctx, query, databaseName, "check")
		return true
	case strings.HasPrefix(lower, "analyze table "):
		e.executeMaintenanceTableQuery(ctx, query, databaseName, "analyze")
		return true
	case strings.HasPrefix(lower, "optimize table "):
		e.executeMaintenanceTableQuery(ctx, query, databaseName, "optimize")
		return true
	case regexp.MustCompile(`^show\s+full\s+(?:columns|fields)\b`).MatchString(lower):
		e.executeShowFullColumns(ctx, query, databaseName)
		return true
	case strings.HasPrefix(lower, "describe "), strings.HasPrefix(lower, "desc "):
		schema, table := extractAdminTableTarget(query, databaseName)
		if table == "" {
			ctx.Results <- &Result{Err: fmt.Errorf("DESCRIBE requires a table name"), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		_, temporary := temporaryPhysicalTableName(ctx.Session, schema, table)
		if !temporary && schema != "" {
			releaseTableLock, lockErr := e.acquireStatementTableLocks(ctx, ctx.Session, "select * from "+schema+"."+table, schema)
			if lockErr != nil {
				ctx.Results <- &Result{Err: tableLockWaitError(lockErr), ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
			defer releaseTableLock()
		}
		info, err := e.readPersistedTableInfo(schema, table)
		if err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		rows := make([][]interface{}, 0, len(info.Columns))
		for _, column := range info.Columns {
			name, _ := column["name"].(string)
			if name == "" {
				continue
			}
			key := ""
			if primary, _ := column["primary"].(bool); primary {
				key = "PRI"
			} else if unique, _ := column["unique"].(bool); unique {
				key = "UNI"
			}
			nullText := "YES"
			if nullable, ok := column["nullable"].(bool); ok && !nullable {
				nullText = "NO"
			}
			extra := ""
			if autoIncrement, _ := column["auto_increment"].(bool); autoIncrement {
				extra = "auto_increment"
			}
			rows = append(rows, []interface{}{name, formatPersistedColumnType(column), nullText, key, persistedDefault(column), extra})
		}
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: map[string]interface{}{
			"columns": []string{"Field", "Type", "Null", "Key", "Default", "Extra"},
			"rows":    rows,
		}, Message: fmt.Sprintf("Found %d columns", len(rows))}
		return true
	default:
		return false
	}
}

func (e *XMySQLExecutor) executeViewDDL(ctx *ExecutionContext, query, databaseName string) bool {
	trimmedQuery := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	lower := strings.ToLower(trimmedQuery)
	viewOptions := `(?:(?:algorithm\s*=\s*(?:undefined|merge|temptable)|definer\s*=\s*(?:current_user(?:\(\))?|` + "'[^']*'@'[^']*'" + `)|sql\s+security\s+(?:definer|invoker))\s+)*`
	isCreateView := regexp.MustCompile(`(?is)^create\s+(?:or\s+replace\s+)?` + viewOptions + `view\s+`).MatchString(trimmedQuery)
	isAlterView := regexp.MustCompile(`(?is)^alter\s+` + viewOptions + `view\s+`).MatchString(trimmedQuery)
	if !isCreateView && !isAlterView && !strings.HasPrefix(lower, "drop view ") {
		return false
	}
	if err := e.prepareDDLImplicitCommit(ctx.Session); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL}
		return true
	}
	if strings.HasPrefix(lower, "drop view ") {
		targets, ifExists, _, err := parseDropViewTargets(trimmedQuery, databaseName)
		if err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL}
			return true
		}
		viewLocks := make([]string, 0, len(targets))
		for _, target := range targets {
			viewLocks = append(viewLocks, strings.ToLower(strings.TrimSpace(target.databaseName))+"."+strings.ToLower(strings.TrimSpace(target.tableName)))
		}
		releaseViewLocks, lockErr := e.acquireDDLTableLocks(ctx, ctx.Session, viewLocks)
		if lockErr != nil {
			ctx.Results <- &Result{Err: tableLockWaitError(lockErr), ResultType: common.RESULT_TYPE_DDL}
			return true
		}
		defer releaseViewLocks()
		present := make([]dropTableTarget, 0, len(targets))
		for _, target := range targets {
			path := filepath.Join(e.getDataDir(), target.databaseName, target.tableName+".view.json")
			if _, statErr := os.Stat(path); statErr != nil {
				if os.IsNotExist(statErr) && ifExists {
					continue
				}
				if os.IsNotExist(statErr) {
					statErr = fmt.Errorf("view '%s.%s' does not exist", target.databaseName, target.tableName)
				}
				ctx.Results <- &Result{Err: statErr, ResultType: common.RESULT_TYPE_DDL}
				return true
			}
			present = append(present, target)
		}
		for _, target := range present {
			if err := e.checkTablePrivilege(ctx, target.databaseName, target.tableName, "DROP"); err != nil {
				ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL}
				return true
			}
		}
		for _, target := range present {
			path := filepath.Join(e.getDataDir(), target.databaseName, target.tableName+".view.json")
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL}
				return true
			}
		}
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("%d view(s) dropped", len(present))}
		return true
	}
	isAlter := isAlterView
	isCreateOrReplace := regexp.MustCompile(`(?is)^create\s+or\s+replace\s+`).MatchString(trimmedQuery)
	match := regexp.MustCompile("(?is)^\\s*(?:create\\s+(?:or\\s+replace\\s+)?" + viewOptions + "|alter\\s+" + viewOptions + ")view\\s+`?([a-zA-Z0-9_$]+)`?(?:\\s*\\.\\s*`?([a-zA-Z0-9_$]+)`?)?(?:\\s*\\(([^)]*)\\))?\\s+as\\s+(.+?)\\s*;?\\s*$").FindStringSubmatch(trimmedQuery)
	if len(match) == 0 {
		ctx.Results <- &Result{Err: fmt.Errorf("%s VIEW requires an AS SELECT definition", map[bool]string{true: "ALTER", false: "CREATE"}[isAlter]), ResultType: common.RESULT_TYPE_DDL}
		return true
	}
	schemaName, viewName, definition := databaseName, match[1], strings.TrimSpace(match[4])
	if match[2] != "" {
		schemaName, viewName = match[1], match[2]
	}
	sourceTables, sourceErr := e.collectViewSourceTables(schemaName, viewName, definition)
	if sourceErr != nil {
		ctx.Results <- &Result{Err: sourceErr, ResultType: common.RESULT_TYPE_DDL}
		return true
	}
	viewLock := strings.ToLower(strings.TrimSpace(schemaName)) + "." + strings.ToLower(strings.TrimSpace(viewName))
	releaseViewLock, lockErr := e.acquireDDLTableLocks(ctx, ctx.Session, []string{viewLock})
	if lockErr != nil {
		ctx.Results <- &Result{Err: tableLockWaitError(lockErr), ResultType: common.RESULT_TYPE_DDL}
		return true
	}
	defer releaseViewLock()
	if len(sourceTables) > 0 {
		// CREATE/ALTER VIEW reads every referenced object while validating and
		// persisting the definition. Keep shared metadata leases on the full
		// nested dependency set so a concurrent DROP/ALTER of a base table
		// cannot race past a view definition that depends on it.
		syntheticQuery := "select * from " + strings.Join(sourceTables, " join ")
		releaseSourceLocks, sourceLockErr := e.acquireStatementTableLocks(ctx, ctx.Session, syntheticQuery, schemaName)
		if sourceLockErr != nil {
			ctx.Results <- &Result{Err: tableLockWaitError(sourceLockErr), ResultType: common.RESULT_TYPE_DDL}
			return true
		}
		defer releaseSourceLocks()
	}
	if columnList := strings.TrimSpace(match[3]); columnList != "" {
		columns := splitTopLevelComma(columnList)
		for _, column := range columns {
			if !regexp.MustCompile("^`?[a-zA-Z0-9_$]+`?$").MatchString(strings.TrimSpace(column)) {
				ctx.Results <- &Result{Err: fmt.Errorf("invalid CREATE VIEW column name %q", strings.TrimSpace(column)), ResultType: common.RESULT_TYPE_DDL}
				return true
			}
		}
		aliased := applyCTEColumnAliases(definition, columns)
		if aliased == definition {
			ctx.Results <- &Result{Err: fmt.Errorf("CREATE VIEW column list does not match SELECT projection"), ResultType: common.RESULT_TYPE_DDL}
			return true
		}
		definition = aliased
	}
	checkOption := ""
	if checkMatch := regexp.MustCompile(`(?is)\s+with\s+(?:(local|cascaded)\s+)?check\s+option\s*$`).FindStringSubmatch(definition); len(checkMatch) == 2 {
		checkOption = strings.ToUpper(strings.TrimSpace(checkMatch[1]))
		if checkOption == "" {
			checkOption = "CASCADED"
		}
		definition = strings.TrimSpace(definition[:len(definition)-len(checkMatch[0])])
	}
	if schemaName == "" {
		ctx.Results <- &Result{Err: fmt.Errorf("no database selected"), ResultType: common.RESULT_TYPE_DDL}
		return true
	}
	if err := e.checkViewDefinerClausePrivilege(ctx, trimmedQuery); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL}
		return true
	}
	if isAlter {
		if err := e.checkDatabasePrivilege(ctx, schemaName, "CREATE VIEW"); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL}
			return true
		}
		if err := e.checkTablePrivilege(ctx, schemaName, viewName, "DROP"); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL}
			return true
		}
	} else if err := e.checkDatabasePrivilege(ctx, schemaName, "CREATE VIEW"); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL}
		return true
	} else if isCreateOrReplace {
		if err := e.checkTablePrivilege(ctx, schemaName, viewName, "DROP"); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL}
			return true
		}
	}
	if err := e.validateViewDefinitionSources(ctx, schemaName, viewName, definition); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL}
		return true
	}
	dir := filepath.Join(e.getDataDir(), schemaName)
	viewPath := filepath.Join(dir, viewName+".view.json")
	if isAlter {
		if _, err := os.Stat(viewPath); err != nil {
			if os.IsNotExist(err) {
				err = fmt.Errorf("view '%s.%s' does not exist", schemaName, viewName)
			}
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL}
			return true
		}
		if err := e.checkViewDefinerPrivilege(ctx, viewPath); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL}
			return true
		}
	} else {
		if tableExists, err := e.checkTableExists(schemaName, viewName); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL}
			return true
		} else if tableExists {
			ctx.Results <- &Result{Err: fmt.Errorf("table '%s.%s' already exists", schemaName, viewName), ResultType: common.RESULT_TYPE_DDL}
			return true
		}
		if _, err := os.Stat(viewPath); err == nil && !isCreateOrReplace {
			ctx.Results <- &Result{Err: fmt.Errorf("view '%s.%s' already exists", schemaName, viewName), ResultType: common.RESULT_TYPE_DDL}
			return true
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL}
			return true
		}
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL}
		return true
	}
	viewInfo := map[string]interface{}{"view_name": viewName, "definition": definition, "schema": schemaName, "created_at": time.Now().UTC().Format(time.RFC3339)}
	if algorithmMatch := regexp.MustCompile(`(?is)\balgorithm\s*=\s*(undefined|merge|temptable)`).FindStringSubmatch(trimmedQuery); len(algorithmMatch) == 2 {
		viewInfo["algorithm"] = strings.ToUpper(algorithmMatch[1])
	}
	if definerMatch := regexp.MustCompile(`(?is)\bdefiner\s*=\s*([^\s]+)`).FindStringSubmatch(trimmedQuery); len(definerMatch) == 2 {
		viewInfo["definer"] = definerMatch[1]
	}
	if securityMatch := regexp.MustCompile(`(?is)\bsql\s+security\s+(definer|invoker)`).FindStringSubmatch(trimmedQuery); len(securityMatch) == 2 {
		viewInfo["sql_security"] = strings.ToUpper(securityMatch[1])
	}
	if checkOption != "" {
		viewInfo["check_option"] = checkOption
	}
	data, err := json.MarshalIndent(viewInfo, "", "  ")
	if err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL}
		return true
	}
	if err := writeMetadataFileAtomic(viewPath, data); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_DDL}
		return true
	}
	action := "created"
	if isAlter {
		action = "altered"
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("View '%s' %s", viewName, action)}
	return true
}

func (e *XMySQLExecutor) collectViewSourceTables(schemaName, viewName, definition string) ([]string, error) {
	seen := make(map[string]struct{})
	visitingViews := make(map[string]struct{})
	rootKey := strings.ToLower(strings.TrimSpace(schemaName)) + "." + strings.ToLower(strings.TrimSpace(viewName))
	if strings.Trim(rootKey, ".") != "" {
		visitingViews[rootKey] = struct{}{}
	}
	var collect func(string, string) error
	collect = func(currentSchema, currentDefinition string) error {
		for _, table := range viewSourceTableAccesses(currentDefinition, currentSchema) {
			if _, exists := seen[table]; exists {
				continue
			}
			seen[table] = struct{}{}
			parts := strings.SplitN(table, ".", 2)
			if len(parts) != 2 {
				continue
			}
			sourceSchema, sourceTable := parts[0], parts[1]
			viewKey := strings.ToLower(sourceSchema) + "." + strings.ToLower(sourceTable)
			if _, active := visitingViews[viewKey]; active {
				return fmt.Errorf("view dependency cycle detected involving '%s.%s'", sourceSchema, sourceTable)
			}
			raw, err := os.ReadFile(filepath.Join(e.getDataDir(), sourceSchema, sourceTable+".view.json"))
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return fmt.Errorf("read source view '%s.%s' failed: %w", sourceSchema, sourceTable, err)
			}
			var info struct {
				Definition string `json:"definition"`
			}
			if err := json.Unmarshal(raw, &info); err != nil {
				return fmt.Errorf("parse source view '%s.%s' failed: %w", sourceSchema, sourceTable, err)
			}
			if strings.TrimSpace(info.Definition) == "" {
				continue
			}
			visitingViews[viewKey] = struct{}{}
			if err := collect(sourceSchema, info.Definition); err != nil {
				return err
			}
			delete(visitingViews, viewKey)
		}
		return nil
	}
	if err := collect(schemaName, definition); err != nil {
		return nil, err
	}
	result := make([]string, 0, len(seen))
	for table := range seen {
		result = append(result, table)
	}
	sort.Strings(result)
	return result, nil
}

func (e *XMySQLExecutor) executeShowCreateView(ctx *ExecutionContext, query, databaseName string) bool {
	match := regexp.MustCompile("(?is)^\\s*show\\s+create\\s+view\\s+`?([a-zA-Z0-9_$]+)`?(?:\\s*\\.\\s*`?([a-zA-Z0-9_$]+)`?)?\\s*;?\\s*$").FindStringSubmatch(query)
	if len(match) == 0 {
		return false
	}
	schemaName, viewName := databaseName, match[1]
	if match[2] != "" {
		schemaName, viewName = match[1], match[2]
	}
	if schemaName != "" {
		releaseViewLock, lockErr := e.acquireStatementTableLocks(ctx, ctx.Session, "select * from "+schemaName+"."+viewName, schemaName)
		if lockErr != nil {
			ctx.Results <- &Result{Err: tableLockWaitError(lockErr), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		defer releaseViewLock()
	}
	if err := e.checkViewMetadataPrivilege(ctx, schemaName); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	raw, err := os.ReadFile(filepath.Join(e.getDataDir(), schemaName, viewName+".view.json"))
	if err != nil {
		ctx.Results <- &Result{Err: fmt.Errorf("view '%s.%s' does not exist: %w", schemaName, viewName, err), ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	var info struct {
		Definition  string `json:"definition"`
		Algorithm   string `json:"algorithm"`
		Definer     string `json:"definer"`
		SQLSecurity string `json:"sql_security"`
		CheckOption string `json:"check_option"`
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	algorithm := strings.ToUpper(strings.TrimSpace(info.Algorithm))
	if algorithm == "" {
		algorithm = "UNDEFINED"
	}
	createSQL := fmt.Sprintf("CREATE ALGORITHM=%s", algorithm)
	if strings.TrimSpace(info.Definer) != "" {
		createSQL += " DEFINER=" + strings.TrimSpace(info.Definer)
	}
	if security := strings.ToUpper(strings.TrimSpace(info.SQLSecurity)); security != "" {
		createSQL += " SQL SECURITY " + security
	}
	createSQL += fmt.Sprintf(" VIEW `%s` AS %s", viewName, info.Definition)
	if checkOption := strings.ToUpper(strings.TrimSpace(info.CheckOption)); checkOption != "" {
		createSQL += " WITH " + strings.ToLower(checkOption) + " CHECK OPTION"
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: map[string]interface{}{
		"columns": []string{"View", "Create View", "character_set_client", "collation_connection"},
		"rows":    [][]interface{}{{viewName, createSQL, "utf8mb4", "utf8mb4_general_ci"}},
	}, Message: fmt.Sprintf("View '%s' definition returned", viewName)}
	return true
}

func (e *XMySQLExecutor) applyViewExecutionSecurity(session server.MySQLServerSession, query, databaseName string) (server.MySQLServerSession, bool, error) {
	if session == nil {
		return session, false, nil
	}
	match := regexp.MustCompile("(?is)^\\s*select\\s+.+?\\s+from\\s+(?:`?([a-zA-Z0-9_$]+)`?\\s*\\.\\s*)?`?([a-zA-Z0-9_$]+)`?").FindStringSubmatch(query)
	if len(match) != 3 {
		return session, false, nil
	}
	schemaName, viewName := databaseName, match[2]
	if match[1] != "" {
		schemaName = match[1]
	}
	path := filepath.Join(e.getDataDir(), schemaName, viewName+".view.json")
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return session, false, nil
	}
	if err != nil {
		return session, false, err
	}
	var info struct {
		Definer     string `json:"definer"`
		SQLSecurity string `json:"sql_security"`
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		return session, false, err
	}
	if strings.EqualFold(strings.TrimSpace(info.SQLSecurity), "INVOKER") {
		return session, true, nil
	}
	definer := strings.TrimSpace(info.Definer)
	if definer == "" {
		definer = "root@localhost"
	} else if quoted := regexp.MustCompile(`^'([^']*)'@'([^']*)'$`).FindStringSubmatch(definer); len(quoted) == 3 {
		definer = quoted[1] + "@" + quoted[2]
	}
	parts := strings.SplitN(definer, "@", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return session, false, fmt.Errorf("invalid view definer %q", definer)
	}
	return &definerExecutionSession{MySQLServerSession: session, user: parts[0], host: parts[1]}, true, nil
}

func (e *XMySQLExecutor) rewriteViewQuery(query, databaseName string) (string, error) {
	match := regexp.MustCompile("(?is)^\\s*(select\\s+.+?\\s+from\\s+)(?:`?([a-zA-Z0-9_$]+)`?(?:\\s*\\.\\s*`?([a-zA-Z0-9_$]+)`?)?)(\\s+(?:as\\s+)?[a-zA-Z0-9_$]+)?(\\s+(?:where|group\\s+by|having|order\\s+by|limit|union|intersect|except)\\b.*)?\\s*;?\\s*$").FindStringSubmatch(query)
	if len(match) == 0 {
		return "", nil
	}
	schemaName, viewName := databaseName, match[2]
	if match[3] != "" {
		schemaName, viewName = match[2], match[3]
	}
	raw, err := os.ReadFile(filepath.Join(e.getDataDir(), schemaName, viewName+".view.json"))
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var info struct {
		Definition string `json:"definition"`
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		return "", err
	}
	definition := strings.TrimSpace(strings.TrimSuffix(info.Definition, ";"))
	if rewritten, rewriteErr := e.rewriteNestedCTESubqueries(definition); rewriteErr != nil {
		return "", fmt.Errorf("rewrite nested view CTE failed: %w", rewriteErr)
	} else if strings.TrimSpace(rewritten) != "" {
		definition = rewritten
	}
	statement, parseErr := sqlparser.Parse(definition)
	if parseErr != nil {
		return "", fmt.Errorf("parse view definition failed: %w", parseErr)
	}
	definition = bindViewDefinitionTables(statement, definition, schemaName)
	if _, ok := statement.(*sqlparser.Select); !ok {
		if withStmt, withOK := statement.(*sqlparser.With); !withOK || withStmt.Body == nil {
			return "", fmt.Errorf("view definition is not a SELECT")
		}
		if rewritten, rewriteErr := e.rewriteSimpleCTEQuery(definition); rewriteErr != nil {
			return "", rewriteErr
		} else if rewritten != "" {
			definition = rewritten
		}
	}
	alias := strings.TrimSpace(match[4])
	if alias == "" {
		alias = "AS " + quotePartitionIdentifier(viewName)
	}
	tail := strings.TrimSpace(match[5])
	if tail != "" {
		tail = " " + tail
	}
	return strings.TrimSpace(match[1]) + " (" + definition + ") " + alias + tail, nil
}

// bindViewDefinitionTables makes the database binding captured by MySQL when
// a view is created explicit in the rewritten derived query. The normal
// executor receives the caller's current database, but unqualified names in a
// view definition resolve against the view's owning database. CTE names and
// already-qualified names must remain untouched.
func bindViewDefinitionTables(statement sqlparser.Statement, definition, databaseName string) string {
	databaseName = strings.TrimSpace(databaseName)
	if statement == nil || databaseName == "" {
		return definition
	}
	cteNames := make(map[string]struct{})
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if cte, ok := node.(*sqlparser.CTEDefinition); ok && cte != nil {
			cteNames[strings.ToLower(strings.TrimSpace(cte.Name.String()))] = struct{}{}
		}
		return true, nil
	}, statement)
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		aliased, ok := node.(*sqlparser.AliasedTableExpr)
		if !ok || aliased == nil {
			return true, nil
		}
		table, ok := aliased.Expr.(sqlparser.TableName)
		if !ok || !table.Qualifier.IsEmpty() {
			return true, nil
		}
		name := strings.TrimSpace(table.Name.String())
		if name == "" || strings.EqualFold(name, "dual") {
			return true, nil
		}
		if _, isCTE := cteNames[strings.ToLower(name)]; isCTE {
			return true, nil
		}
		table.Qualifier = sqlparser.NewTableIdent(databaseName)
		aliased.Expr = table
		return true, nil
	}, statement)
	return sqlparser.String(statement)
}

// rewriteNestedCTESubqueries expands WITH queries that occur inside derived
// table parentheses before Vitess parses the surrounding view definition.
// The parser accepts a top-level WITH, but not every nested WITH shape used by
// MySQL clients. Reusing the existing simple CTE rewriter keeps the expanded
// query on the normal derived-table execution path.
func (e *XMySQLExecutor) rewriteNestedCTESubqueries(query string) (string, error) {
	rewritten := query
	for index := 0; index < len(rewritten); index++ {
		if rewritten[index] != '(' {
			continue
		}
		close := matchingParenIndex(rewritten, index)
		if close < 0 {
			continue
		}
		inner := strings.TrimSpace(rewritten[index+1 : close])
		if !strings.HasPrefix(strings.ToLower(inner), "with") {
			nestedRewritten, err := e.rewriteNestedCTESubqueries(inner)
			if err != nil {
				return "", err
			}
			if !strings.EqualFold(strings.TrimSpace(nestedRewritten), inner) {
				rewritten = rewritten[:index+1] + nestedRewritten + rewritten[close:]
				index += len(nestedRewritten)
				continue
			}
			index = close
			continue
		}
		innerRewritten, err := e.rewriteSimpleCTEQuery(inner)
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(innerRewritten) == "" || strings.EqualFold(strings.TrimSpace(innerRewritten), inner) {
			index = close
			continue
		}
		nestedRewritten, err := e.rewriteNestedCTESubqueries(innerRewritten)
		if err != nil {
			return "", err
		}
		rewritten = rewritten[:index+1] + nestedRewritten + rewritten[close:]
		index += len(nestedRewritten)
	}
	return rewritten, nil
}

// rewriteSimpleCTEQuery handles the common single-CTE shape used by clients
// for metadata and small staging queries. The CTE query is already a normal
// SELECT, so expanding SELECT * FROM cte preserves the existing planner path.
func (e *XMySQLExecutor) rewriteSimpleCTEQuery(query string) (string, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "with ") {
		return "", nil
	}
	pos := len("with")
	for pos < len(trimmed) && (trimmed[pos] == ' ' || trimmed[pos] == '\t' || trimmed[pos] == '\r' || trimmed[pos] == '\n') {
		pos++
	}
	if strings.HasPrefix(strings.ToLower(trimmed[pos:]), "recursive ") {
		pos += len("recursive ")
	}
	definitionHeader := regexp.MustCompile(`(?is)^([a-zA-Z0-9_$]+)(?:\s*\(([^)]*)\))?\s+as\s*\(`)
	definitions := map[string]string{}
	for {
		if pos >= len(trimmed) {
			return "", fmt.Errorf("CTE query has no main SELECT")
		}
		match := definitionHeader.FindStringSubmatchIndex(trimmed[pos:])
		if len(match) == 0 {
			return "", fmt.Errorf("unsupported CTE syntax")
		}
		name := strings.ToLower(trimmed[pos+match[2] : pos+match[3]])
		open := pos + match[1] - 1
		close := matchingParenIndex(trimmed, open)
		if close < 0 {
			return "", fmt.Errorf("unterminated CTE definition")
		}
		if _, exists := definitions[name]; exists {
			return "", fmt.Errorf("duplicate CTE name '%s'", name)
		}
		definition := strings.TrimSpace(trimmed[open+1 : close])
		definitionLower := strings.ToLower(definition)
		if !strings.HasPrefix(definitionLower, "select ") && !strings.HasPrefix(definitionLower, "(") {
			return "", fmt.Errorf("CTE definition must be a SELECT")
		}
		if strings.HasPrefix(definitionLower, "(") {
			if _, operators, ok := splitSetOperationQuery(definition); !ok || !hasNonUnionSetOperator(operators) {
				return "", fmt.Errorf("CTE definition must be a SELECT")
			}
		}
		if len(match) >= 6 && match[4] >= 0 && match[5] >= 0 {
			definition = applyCTEColumnAliases(definition, splitTopLevelComma(trimmed[pos+match[4]:pos+match[5]]))
		}
		definitions[name] = definition
		pos = close + 1
		for pos < len(trimmed) && (trimmed[pos] == ' ' || trimmed[pos] == '\t' || trimmed[pos] == '\r' || trimmed[pos] == '\n') {
			pos++
		}
		if pos < len(trimmed) && trimmed[pos] == ',' {
			pos++
			for pos < len(trimmed) && (trimmed[pos] == ' ' || trimmed[pos] == '\t' || trimmed[pos] == '\r' || trimmed[pos] == '\n') {
				pos++
			}
			continue
		}
		break
	}
	main := strings.TrimSpace(trimmed[pos:])
	if rewritten, ok := rewriteCTEInsertQuery(main, definitions); ok {
		return rewritten, nil
	}
	if rewritten, ok := rewriteCTESourcesAsDerived(main, definitions); ok {
		return rewritten, nil
	}
	if rewritten, ok := rewriteCTEDMLQuery(main, definitions); ok {
		return rewritten, nil
	}
	if rewritten, ok := rewriteCTEJoinQuery(main, definitions); ok {
		return rewritten, nil
	}
	mainMatch := regexp.MustCompile(`(?is)^select\s+\*\s+from\s+([a-zA-Z0-9_$]+)\s*$`).FindStringSubmatch(main)
	if len(mainMatch) == 0 {
		if rewritten, ok := rewriteMaterializedCTEQuery(main, definitions); ok {
			return rewritten, nil
		}
		return "", nil
	}
	name := strings.ToLower(mainMatch[1])
	definition, exists := definitions[name]
	if !exists {
		return "", fmt.Errorf("CTE '%s' is not defined", mainMatch[1])
	}
	// Resolve the useful client shape: a chain of SELECT * FROM CTE names.
	for i := 0; i < len(definitions); i++ {
		ref := regexp.MustCompile(`(?is)^select\s+\*\s+from\s+([a-zA-Z0-9_$]+)(.*)$`).FindStringSubmatch(definition)
		if len(ref) == 0 {
			break
		}
		referenced, ok := definitions[strings.ToLower(ref[1])]
		if !ok {
			break
		}
		definition = referenced + ref[2]
	}
	return definition, nil
}

func applyCTEColumnAliases(definition string, columns []string) string {
	if len(columns) == 0 {
		return definition
	}
	parts := splitTopLevelKeyword(definition, "from")
	if len(parts) != 2 {
		return definition
	}
	projectionText := strings.TrimSpace(parts[0])
	if len(projectionText) < len("select") || !strings.EqualFold(projectionText[:len("select")], "select") {
		return definition
	}
	projection := splitTopLevelComma(strings.TrimSpace(projectionText[len("select"):]))
	if len(projection) != len(columns) {
		return definition
	}
	aliased := make([]string, 0, len(projection))
	for index, expression := range projection {
		column := strings.Trim(strings.TrimSpace(columns[index]), "`")
		if column == "" {
			return definition
		}
		aliased = append(aliased, strings.TrimSpace(expression)+" AS "+column)
	}
	return "select " + strings.Join(aliased, ", ") + " from " + strings.TrimSpace(parts[1])
}

func rewriteCTEInsertQuery(main string, definitions map[string]string) (string, bool) {
	match := regexp.MustCompile(`(?is)^insert\s+into\s+(.+?)\s+select\s+(.+?)\s+from\s+([a-zA-Z0-9_$]+)(?:\s+where\s+(.+?))?\s*;?$`).FindStringSubmatch(strings.TrimSpace(main))
	if len(match) != 5 {
		return "", false
	}
	definition, ok := definitions[strings.ToLower(match[3])]
	if !ok {
		return "", false
	}
	definitionMatch := regexp.MustCompile(`(?is)^select\s+.+?\s+from\s+([a-zA-Z0-9_$]+)(?:\s+where\s+(.+?))?\s*$`).FindStringSubmatch(strings.TrimSpace(definition))
	if len(definitionMatch) != 3 {
		return "", false
	}
	whereParts := make([]string, 0, 2)
	if strings.TrimSpace(definitionMatch[2]) != "" {
		whereParts = append(whereParts, "("+strings.TrimSpace(definitionMatch[2])+")")
	}
	if strings.TrimSpace(match[4]) != "" {
		whereParts = append(whereParts, "("+strings.TrimSpace(match[4])+")")
	}
	rewritten := fmt.Sprintf("insert into %s select %s from %s", strings.TrimSpace(match[1]), strings.TrimSpace(match[2]), definitionMatch[1])
	if len(whereParts) > 0 {
		rewritten += " where " + strings.Join(whereParts, " and ")
	}
	return rewritten, true
}

// rewriteCTESourcesAsDerived materializes ordinary CTE table references as
// derived sources. This keeps the CTE scope statement-local while allowing
// the normal derived-table executor to handle multiple CTEs, CTE chains and
// joins without leaking temporary names into the catalog.
func rewriteCTESourcesAsDerived(main string, definitions map[string]string) (string, bool) {
	lowerMain := strings.ToLower(strings.TrimSpace(main))
	if (!strings.HasPrefix(lowerMain, "select ") && !strings.HasPrefix(lowerMain, "insert ")) || len(definitions) == 0 {
		return "", false
	}
	expanded := make(map[string]string, len(definitions))
	visiting := make(map[string]bool, len(definitions))
	var expand func(string) (string, bool)
	expand = func(name string) (string, bool) {
		name = strings.ToLower(name)
		if value, ok := expanded[name]; ok {
			return value, true
		}
		definition, ok := definitions[name]
		if !ok || visiting[name] {
			return "", false
		}
		visiting[name] = true
		value, changed := replaceCTESourceReferences(definition, definitions, func(reference string) (string, bool) {
			return expand(reference)
		}, name)
		delete(visiting, name)
		if !changed {
			value = definition
		}
		expanded[name] = value
		return value, true
	}

	rewritten, changed := replaceCTESourceReferences(main, definitions, func(reference string) (string, bool) {
		return expand(reference)
	}, "")
	if !changed {
		return "", false
	}
	return rewritten, true
}

func replaceCTESourceReferences(sql string, definitions map[string]string, resolve func(string) (string, bool), current string) (string, bool) {
	changed := false
	result := sql
	for name := range definitions {
		if strings.EqualFold(name, current) {
			continue
		}
		pattern := regexp.MustCompile(`(?is)(\b(?:from|join)\s+)` + regexp.QuoteMeta(name) + `\b(?:\s+(?:as\s+)?([a-zA-Z_][a-zA-Z0-9_$]*))?`)
		matches := pattern.FindAllStringSubmatchIndex(result, -1)
		if len(matches) == 0 {
			continue
		}
		definition, ok := resolve(name)
		if !ok {
			continue
		}
		var builder strings.Builder
		last := 0
		for _, match := range matches {
			alias := ""
			end := match[1]
			if len(match) >= 6 && match[4] >= 0 && match[5] >= 0 {
				candidate := result[match[4]:match[5]]
				if !isCTESourceKeyword(candidate) {
					alias = candidate
					end = match[5]
				} else {
					end = match[3] + len(name)
				}
			}
			builder.WriteString(result[last:match[2]])
			builder.WriteString(result[match[2]:match[3]])
			builder.WriteString("(")
			builder.WriteString(definition)
			builder.WriteString(") AS ")
			if alias == "" {
				builder.WriteString(name)
			} else {
				builder.WriteString(alias)
			}
			last = end
			changed = true
		}
		builder.WriteString(result[last:])
		result = builder.String()
	}
	return result, changed
}

func isCTESourceKeyword(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "as", "join", "left", "right", "inner", "outer", "cross", "where", "group", "order", "limit", "having", "union", "on", "using":
		return true
	default:
		return false
	}
}

// rewriteCTEDMLQuery supports the common MySQL form where a CTE is consumed
// by an IN subquery of UPDATE or DELETE. It only rewrites a simple single-table
// CTE, leaving joins, aggregates and recursive DML to the normal unsupported
// statement path with an explicit error.
func rewriteCTEDMLQuery(main string, definitions map[string]string) (string, bool) {
	lower := strings.ToLower(strings.TrimSpace(main))
	if !strings.HasPrefix(lower, "update ") && !strings.HasPrefix(lower, "delete ") {
		return "", false
	}
	result := main
	rewritten := false
	for name, definition := range definitions {
		definitionMatch := regexp.MustCompile(`(?is)^select\s+(.+?)\s+from\s+([a-zA-Z0-9_$]+)(?:\s+where\s+(.+?))?$`).FindStringSubmatch(strings.TrimSpace(definition))
		if len(definitionMatch) != 4 {
			continue
		}
		subquery := regexp.MustCompile(`(?is)\(\s*select\s+(.+?)\s+from\s+` + regexp.QuoteMeta(name) + `\s*\)`).FindStringSubmatchIndex(result)
		if len(subquery) == 0 {
			continue
		}
		projection := strings.TrimSpace(result[subquery[2]:subquery[3]])
		expanded := "(select " + projection + " from " + definitionMatch[2]
		if predicate := strings.TrimSpace(definitionMatch[3]); predicate != "" {
			expanded += " where " + predicate
		}
		expanded += ")"
		result = result[:subquery[0]] + expanded + result[subquery[1]:]
		rewritten = true
	}
	return result, rewritten
}

func rewriteMaterializedCTEQuery(main string, definitions map[string]string) (string, bool) {
	match := regexp.MustCompile(`(?is)^select\s+(.+?)\s+from\s+([a-zA-Z0-9_$]+)(?:\s+where\s+(.+?))?(?:\s+order\s+by\s+(.+?))?(?:\s+limit\s+(.+?))?$`).FindStringSubmatch(strings.TrimSpace(main))
	if len(match) == 0 {
		return "", false
	}
	definition, ok := definitions[strings.ToLower(match[2])]
	if !ok {
		return "", false
	}
	definitionMatch := regexp.MustCompile(`(?is)^select\s+(.+?)\s+from\s+([a-zA-Z0-9_$]+)(?:\s+where\s+(.+?))?$`).FindStringSubmatch(strings.TrimSpace(definition))
	if len(definitionMatch) == 0 {
		return "", false
	}
	projection := strings.TrimSpace(match[1])
	if projection == "*" {
		projection = strings.TrimSpace(definitionMatch[1])
	}
	whereParts := make([]string, 0, 2)
	if strings.TrimSpace(definitionMatch[3]) != "" {
		whereParts = append(whereParts, strings.TrimSpace(definitionMatch[3]))
	}
	if strings.TrimSpace(match[3]) != "" {
		whereParts = append(whereParts, strings.TrimSpace(match[3]))
	}
	rewritten := fmt.Sprintf("select %s from %s", projection, definitionMatch[2])
	if len(whereParts) > 0 {
		rewritten += " where " + strings.Join(whereParts, " and ")
	}
	if strings.TrimSpace(match[4]) != "" {
		rewritten += " order by " + strings.TrimSpace(match[4])
	}
	if strings.TrimSpace(match[5]) != "" {
		rewritten += " limit " + strings.TrimSpace(match[5])
	}
	return rewritten, true
}

// rewriteCTEJoinQuery expands the safe single-table CTE + JOIN shape without
// introducing a derived-table parser dependency. The CTE predicate is kept
// in the outer WHERE clause and the CTE name becomes a stable table alias.
func rewriteCTEJoinQuery(main string, definitions map[string]string) (string, bool) {
	match := regexp.MustCompile(`(?is)^select\s+(.+?)\s+from\s+([a-zA-Z0-9_$]+)(?:\s+(?:as\s+)?([a-zA-Z0-9_$]+))?\s+join\s+(.+)$`).FindStringSubmatch(strings.TrimSpace(main))
	if len(match) != 5 {
		return "", false
	}
	cteName := strings.ToLower(match[2])
	definition, ok := definitions[cteName]
	if !ok {
		return "", false
	}
	definitionMatch := regexp.MustCompile(`(?is)^select\s+(.+?)\s+from\s+([a-zA-Z0-9_$]+)(?:\s+where\s+(.+?))?$`).FindStringSubmatch(strings.TrimSpace(definition))
	if len(definitionMatch) != 4 {
		return "", false
	}
	alias := strings.TrimSpace(match[3])
	if alias == "" {
		alias = match[2]
	}
	joinTail := strings.TrimSpace(match[4])
	lowerTail := strings.ToLower(joinTail)
	whereAt := strings.Index(lowerTail, " where ")
	orderAt := strings.Index(lowerTail, " order by ")
	limitAt := strings.Index(lowerTail, " limit ")
	suffixAt := len(joinTail)
	for _, at := range []int{whereAt, orderAt, limitAt} {
		if at >= 0 && at < suffixAt {
			suffixAt = at
		}
	}
	joinPart := strings.TrimSpace(joinTail[:suffixAt])
	suffix := strings.TrimSpace(joinTail[suffixAt:])
	existingWhere := ""
	if whereAt >= 0 && whereAt == suffixAt {
		whereEnd := len(joinTail)
		for _, at := range []int{orderAt, limitAt} {
			if at > whereAt && at < whereEnd {
				whereEnd = at
			}
		}
		existingWhere = strings.TrimSpace(joinTail[whereAt+len(" where ") : whereEnd])
		suffix = strings.TrimSpace(joinTail[whereEnd:])
	}
	rewritten := fmt.Sprintf("select %s from %s as %s join %s", strings.TrimSpace(match[1]), definitionMatch[2], alias, joinPart)
	if predicate := strings.TrimSpace(definitionMatch[3]); predicate != "" {
		rewritten += " where (" + predicate + ")"
		if existingWhere != "" {
			rewritten += " and (" + existingWhere + ")"
		}
	} else if existingWhere != "" {
		rewritten += " where " + existingWhere
	}
	if suffix != "" {
		rewritten += " " + suffix
	}
	return rewritten, true
}

func (e *XMySQLExecutor) executeSimpleWindowQuery(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	match := regexp.MustCompile(`(?is)^\s*select\s+([a-zA-Z0-9_$.` + "`" + `]+)\s*,\s*row_number\s*\(\s*\)\s+over\s*\(\s*order\s+by\s+([a-zA-Z0-9_$.` + "`" + `]+)\s*(asc|desc)?\s*\)\s+as\s+([a-zA-Z0-9_` + "`" + `]+)\s+from\s+([a-zA-Z0-9_$.` + "`" + `]+)\s*;?\s*$`).FindStringSubmatch(query)
	if len(match) == 0 {
		return false, nil
	}
	columnName := strings.Trim(match[1], "`")
	orderColumn := strings.Trim(match[2], "`")
	tableName := strings.Trim(match[5], "`")
	if idx := strings.LastIndex(tableName, "."); idx >= 0 {
		databaseName, tableName = tableName[:idx], tableName[idx+1:]
	}
	stmt, err := sqlparser.Parse(fmt.Sprintf("select %s from %s", columnName, tableName))
	if err != nil {
		return true, err
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return true, fmt.Errorf("window source is not a SELECT")
	}
	result, err := e.executeSelectStatement(ctx, selectStmt, databaseName)
	if err != nil {
		return true, err
	}
	orderIndex := 0
	for index, column := range result.Columns {
		if strings.EqualFold(strings.Trim(column, "`"), orderColumn) {
			orderIndex = index
			break
		}
	}
	type indexedRow struct {
		values []interface{}
		key    string
	}
	rows := make([]indexedRow, 0, len(result.Records))
	for _, record := range result.Records {
		values := make([]interface{}, 0, len(record.GetValues()))
		for _, value := range record.GetValues() {
			values = append(values, value.Raw())
		}
		key := ""
		if orderIndex < len(values) {
			key = fmt.Sprintf("%v", values[orderIndex])
		}
		rows = append(rows, indexedRow{values: values, key: key})
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if strings.EqualFold(match[3], "desc") {
			return rows[i].key > rows[j].key
		}
		return rows[i].key < rows[j].key
	})
	output := make([][]interface{}, 0, len(rows))
	for index, row := range rows {
		output = append(output, append(row.values, int64(index+1)))
	}
	columns := append([]string{columnName}, match[4])
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: newInformationSchemaSelectResult("window", columns, output), Message: fmt.Sprintf("SELECT query executed successfully, %d rows returned", len(output))}
	return true, nil
}

func (e *XMySQLExecutor) executeMaintenanceTableQuery(ctx *ExecutionContext, query, databaseName, operation string) {
	if ctx == nil {
		ctx = &ExecutionContext{Context: context.Background(), DatabaseName: databaseName}
	}
	if strings.TrimSpace(ctx.DatabaseName) == "" {
		ctx.DatabaseName = databaseName
	}
	result, err := e.executeTableMaintenance(ctx, query)
	if err != nil {
		if ctx.Results != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
		}
		return
	}
	if ctx.Results != nil {
		ctx.Results <- result
	}
}

func (e *XMySQLExecutor) executeExplainQuery(ctx *ExecutionContext, query, databaseName string) {
	selectSQL := strings.TrimSpace(query)
	if len(selectSQL) >= len("explain") && strings.EqualFold(selectSQL[:len("explain")], "explain") {
		selectSQL = strings.TrimSpace(selectSQL[len("explain"):])
	}
	if strings.HasPrefix(strings.ToLower(selectSQL), "format=") {
		if idx := strings.Index(selectSQL, " "); idx >= 0 {
			selectSQL = strings.TrimSpace(selectSQL[idx:])
		}
	}

	tablePattern := regexp.MustCompile("(?is)\\bfrom\\s+`?([a-zA-Z0-9_$]+)`?(?:\\s*\\.\\s*`?([a-zA-Z0-9_$]+)`?)?")
	matches := tablePattern.FindStringSubmatch(selectSQL)
	tableName, schemaName := "", databaseName
	if len(matches) > 0 {
		tableName = matches[1]
		if matches[2] != "" {
			schemaName, tableName = matches[1], matches[2]
		}
	}
	columns := []string{"id", "select_type", "table", "partitions", "type", "possible_keys", "key", "key_len", "ref", "rows", "filtered", "Extra"}
	if tableName == "" {
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: map[string]interface{}{"columns": columns, "rows": [][]interface{}{}}, Message: "EXPLAIN returned no table plan"}
		return
	}
	info, err := e.readPersistedTableInfo(schemaName, tableName)
	if err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	possibleKeys, accessType := "", "ALL"
	var selectedKey interface{}
	where := strings.ToLower(selectSQL)
	for _, index := range info.Indexes {
		name, _ := index["name"].(string)
		indexColumns, _ := index["columns"].([]interface{})
		if name == "" || len(indexColumns) == 0 {
			continue
		}
		columnName, _ := indexColumns[0].(string)
		if strings.Contains(where, strings.ToLower(columnName)+" =") {
			if possibleKeys != "" {
				possibleKeys += ","
			}
			possibleKeys += name
			if selectedKey == nil {
				selectedKey = name
				accessType = "ref"
				if primary, _ := index["primary"].(bool); primary {
					accessType = "const"
				}
			}
		}
	}
	rowCount := int64(0)
	if info.Stats != nil {
		rowCount = int64(info.Stats.RowCount)
	} else if tableRows, countErr := e.tableRowCount(ctx, schemaName, tableName); countErr == nil {
		rowCount = tableRows
	}
	if selectedKey != nil {
		if rowCount > 1 {
			rowCount = 1
		}
	}
	var partitions interface{}
	if partitionRows := e.partitionRowsForTable(schemaName, tableName); len(partitionRows) > 0 {
		names := make([]string, 0, len(partitionRows))
		for _, partition := range partitionRows {
			names = append(names, fmt.Sprint(partition["PARTITION_NAME"]))
		}
		partitions = strings.Join(names, ",")
	}
	keyLength := int64(0)
	if selectedKey != nil {
		keyLength = 8
	}
	extra := ""
	if selectedKey != nil {
		extra = "Using index condition"
	}
	rows := [][]interface{}{{int64(1), "SIMPLE", tableName, partitions, accessType, nullIfEmpty(possibleKeys), selectedKey, keyLength, nil, rowCount, float64(100), extra}}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: map[string]interface{}{"columns": columns, "rows": rows}, Message: "EXPLAIN completed"}
}

func (e *XMySQLExecutor) tableRowCount(ctx *ExecutionContext, schemaName, tableName string) (int64, error) {
	stmt, err := sqlparser.Parse("select * from " + tableName)
	if err != nil {
		return 0, err
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return 0, fmt.Errorf("row count source is not SELECT")
	}
	result, err := e.executeSelectStatement(ctx, selectStmt, schemaName)
	if err != nil {
		return 0, err
	}
	return int64(result.RowCount), nil
}

type persistedTableInfo struct {
	TableName       string                   `json:"table_name"`
	TableComment    string                   `json:"table_comment,omitempty"`
	StorageRootPage uint32                   `json:"storage_root_page,omitempty"`
	Columns         []map[string]interface{} `json:"columns"`
	Indexes         []map[string]interface{} `json:"indexes"`
	ForeignKeys     []map[string]interface{} `json:"foreign_keys"`
	Checks          []string                 `json:"checks"`
	CheckNames      map[string]string        `json:"check_names"`
	CheckEnforced   map[string]bool          `json:"check_enforced"`
	Options         map[string]interface{}   `json:"options"`
	Stats           *metadata.InfoTableStats `json:"stats,omitempty"`
}

func (e *XMySQLExecutor) readPersistedTableInfo(schemaName, tableName string) (*persistedTableInfo, error) {
	if strings.TrimSpace(schemaName) == "" {
		return nil, fmt.Errorf("no database selected")
	}
	path := filepath.Join(e.getDataDir(), schemaName, tableName+".frm")
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("table '%s.%s' does not exist: %w", schemaName, tableName, err)
	}
	var info persistedTableInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return nil, fmt.Errorf("parse table metadata failed: %w", err)
	}
	return &info, nil
}

func (e *XMySQLExecutor) loadCreateTableDefinition(schemaName, tableName string) (string, error) {
	info, err := e.readPersistedTableInfo(schemaName, tableName)
	if err != nil {
		// Keep the legacy in-memory executor usable for parser/routing unit tests
		// that intentionally do not configure a data directory. Configured
		// engines still return the real missing-table error.
		if e.conf == nil {
			return fmt.Sprintf("CREATE TABLE `%s` (\n  `id` int NOT NULL\n)", tableName), nil
		}
		return "", err
	}
	lines := make([]string, 0, len(info.Columns)+len(info.Indexes)+len(info.ForeignKeys)+len(info.Checks))
	for _, column := range info.Columns {
		name, _ := column["name"].(string)
		if name == "" {
			continue
		}
		line := fmt.Sprintf("  `%s` %s", name, formatPersistedColumnType(column))
		if nullable, ok := column["nullable"].(bool); ok && !nullable {
			line += " NOT NULL"
		}
		if defaultValue := persistedDefault(column); defaultValue != nil {
			line += " DEFAULT " + fmt.Sprint(defaultValue)
		}
		if autoIncrement, _ := column["auto_increment"].(bool); autoIncrement {
			line += " AUTO_INCREMENT"
		}
		if comment := persistedString(column["comment"]); comment != "" {
			line += " COMMENT '" + strings.ReplaceAll(comment, "'", "''") + "'"
		}
		lines = append(lines, line)
	}
	for _, index := range info.Indexes {
		name, _ := index["name"].(string)
		columns, _ := index["columns"].([]interface{})
		if name == "" || len(columns) == 0 {
			continue
		}
		columnNames := make([]string, 0, len(columns))
		for _, column := range columns {
			columnNames = append(columnNames, fmt.Sprintf("`%s`", fmt.Sprint(column)))
		}
		joined := strings.Join(columnNames, ", ")
		if primary, _ := index["primary"].(bool); primary || strings.EqualFold(name, "PRIMARY") {
			lines = append(lines, "  PRIMARY KEY ("+joined+")")
			continue
		}
		unique, _ := index["unique"].(bool)
		prefix := "KEY"
		if unique {
			prefix = "UNIQUE KEY"
		}
		line := fmt.Sprintf("  %s `%s` (%s)", prefix, name, joined)
		if visible, ok := index["visible"].(bool); ok && !visible {
			line += " INVISIBLE"
		}
		lines = append(lines, line)
	}
	for _, foreignKey := range info.ForeignKeys {
		if rawDefinition, _ := foreignKey["raw_definition"].(string); strings.TrimSpace(rawDefinition) != "" {
			lines = append(lines, "  "+strings.TrimSpace(rawDefinition))
		}
	}
	for checkIndex, check := range info.Checks {
		check = strings.TrimSpace(check)
		if check == "" {
			continue
		}
		name := ""
		for candidate, expression := range info.CheckNames {
			if strings.EqualFold(strings.TrimSpace(expression), check) {
				name = strings.Trim(strings.TrimSpace(candidate), "`")
				break
			}
		}
		if name == "" {
			name = fmt.Sprintf("check_%d", checkIndex+1)
		}
		line := "CHECK (" + check + ")"
		if strings.HasPrefix(strings.ToLower(check), "check ") {
			line = check
		}
		if name != "" {
			line = "CONSTRAINT `" + name + "` " + line
		}
		if enforced, ok := info.CheckEnforced[strings.ToLower(name)]; ok && !enforced {
			line += " NOT ENFORCED"
		}
		lines = append(lines, "  "+line)
	}
	engineName := "InnoDB"
	if rawEngine, ok := info.Options["engine"].(string); ok && rawEngine != "" {
		engineName = rawEngine
		if strings.EqualFold(engineName, "innodb") {
			engineName = "InnoDB"
		}
	}
	definition := fmt.Sprintf("CREATE TABLE `%s` (\n%s\n) ENGINE=%s", tableName, strings.Join(lines, ",\n"), engineName)
	if rawOptions, ok := info.Options["raw_options"].(string); ok && strings.TrimSpace(rawOptions) != "" {
		if charset, found := parseTableOptionValue(rawOptions, `(?:default\s+)?(?:character\s+set|charset)`); found {
			definition += " DEFAULT CHARSET=" + charset
		}
		if collation, found := parseTableOptionValue(rawOptions, `collate`); found {
			definition += " COLLATE=" + collation
		}
		if rowFormat, found := parseTableOptionValue(rawOptions, `row_format`); found {
			definition += " ROW_FORMAT=" + strings.ToUpper(normalizeTableRowFormat(rowFormat))
		}
	}
	if info.TableComment != "" {
		definition += " COMMENT='" + strings.ReplaceAll(info.TableComment, "'", "''") + "'"
	}
	return definition, nil
}

func persistedTableComment(e *XMySQLExecutor, schemaName, tableName string) string {
	info, err := e.readPersistedTableInfo(schemaName, tableName)
	if err != nil || info == nil {
		return ""
	}
	if info.TableComment != "" {
		return info.TableComment
	}
	if value, ok := info.Options["comment"].(string); ok {
		return value
	}
	return ""
}

func persistedTableDisplayOptions(e *XMySQLExecutor, schemaName, tableName string) (string, string, string) {
	engineName := "InnoDB"
	tableCollation := "utf8mb4_0900_ai_ci"
	createOptions := ""
	info, err := e.readPersistedTableInfo(schemaName, tableName)
	if err != nil || info == nil {
		return engineName, tableCollation, createOptions
	}
	if value, ok := info.Options["engine"].(string); ok && strings.TrimSpace(value) != "" {
		engineName = value
	}
	if value, ok := info.Options["collation"].(string); ok && strings.TrimSpace(value) != "" {
		tableCollation = value
	}
	if value, ok := info.Options["raw_options"].(string); ok {
		createOptions = value
	}
	return engineName, tableCollation, createOptions
}

func persistedTableRowFormat(e *XMySQLExecutor, schemaName, tableName string) string {
	info, err := e.readPersistedTableInfo(schemaName, tableName)
	if err != nil || info == nil {
		return "Dynamic"
	}
	if value, ok := info.Options["row_format"].(string); ok && strings.TrimSpace(value) != "" {
		return normalizeTableRowFormat(value)
	}
	if rawOptions, ok := info.Options["raw_options"].(string); ok {
		if value, found := parseTableOptionValue(rawOptions, `row_format`); found {
			return normalizeTableRowFormat(value)
		}
	}
	return "Dynamic"
}

func persistedString(value interface{}) string {
	text := strings.TrimSpace(fmt.Sprint(value))
	if len(text) >= 2 && ((text[0] == '\'' && text[len(text)-1] == '\'') || (text[0] == '"' && text[len(text)-1] == '"')) {
		quote := text[0]
		text = text[1 : len(text)-1]
		if quote == '\'' {
			return strings.ReplaceAll(text, "''", "'")
		}
		return strings.ReplaceAll(text, `""`, `"`)
	}
	if text == "<nil>" {
		return ""
	}
	return text
}

func extractAdminTableTarget(query, databaseName string) (string, string) {
	fields := strings.Fields(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	if len(fields) < 2 {
		return databaseName, ""
	}
	targetIndex := 1
	if strings.EqualFold(fields[1], "table") || strings.EqualFold(fields[1], "view") {
		targetIndex = 2
	}
	if len(fields) <= targetIndex {
		return databaseName, ""
	}
	target := strings.Trim(fields[targetIndex], "`")
	parts := strings.Split(target, ".")
	if len(parts) == 2 {
		return strings.Trim(parts[0], "`"), strings.Trim(parts[1], "`")
	}
	return databaseName, target
}

func extractQualifiedTableFromShowCreate(query string) (string, string) {
	match := regexp.MustCompile("(?is)show\\s+create\\s+table\\s+`?([a-zA-Z0-9_$]+)`?(?:\\s*\\.\\s*`?([a-zA-Z0-9_$]+)`?)?").FindStringSubmatch(query)
	if len(match) == 0 {
		return "", ""
	}
	if match[2] != "" {
		return match[1], match[2]
	}
	return "", match[1]
}

func formatPersistedColumnType(column map[string]interface{}) string {
	typeName, _ := column["type"].(string)
	typeName = strings.ToLower(typeName)
	length := persistedNumber(column["length"])
	scale := persistedNumber(column["scale"])
	if length > 0 && (typeName == "varchar" || typeName == "char" || typeName == "varbinary" || typeName == "binary" || typeName == "decimal" || typeName == "numeric") {
		if scale > 0 && (typeName == "decimal" || typeName == "numeric") {
			typeName = fmt.Sprintf("%s(%d,%d)", typeName, int(length), int(scale))
		} else {
			typeName = fmt.Sprintf("%s(%d)", typeName, int(length))
		}
	}
	if unsigned, _ := column["unsigned"].(bool); unsigned {
		typeName += " unsigned"
	}
	return typeName
}

func persistedNumber(value interface{}) float64 {
	switch n := value.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case json.Number:
		parsed, _ := n.Float64()
		return parsed
	default:
		return 0
	}
}

func persistedDefault(column map[string]interface{}) interface{} {
	value, exists := column["default"]
	if !exists {
		return nil
	}
	return value
}

func nullIfEmpty(value string) interface{} {
	if value == "" {
		return nil
	}
	return value
}

// executeShowEngines 执行 SHOW ENGINES
func (e *XMySQLExecutor) executeShowEngines(ctx *ExecutionContext) {
	logger.Debugf(" [executeShowEngines] 执行SHOW ENGINES")

	rows := [][]interface{}{
		{"InnoDB", "DEFAULT", "Supports transactions, row-level locking, and foreign keys", "YES", "YES", "YES"},
	}

	resultData := map[string]interface{}{
		"columns": []string{"Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"},
		"rows":    rows,
	}

	ctx.Results <- &Result{
		ResultType: "QUERY",
		Data:       resultData,
	}
}

// executeShowWarnings 执行 SHOW WARNINGS
func (e *XMySQLExecutor) executeShowWarnings(ctx *ExecutionContext) {
	logger.Debugf(" [executeShowWarnings] 执行SHOW WARNINGS")
	warnings := sessionWarnings(ctx)
	start, end := showWarningsBounds(ctx.RawQuery, len(warnings))
	rows := make([][]interface{}, 0, end-start)
	for _, warning := range warnings[start:end] {
		rows = append(rows, []interface{}{warning.Level, warning.Code, warning.Message})
	}
	resultData := map[string]interface{}{
		"columns": []string{"Level", "Code", "Message"},
		"rows":    rows,
	}

	ctx.Results <- &Result{
		ResultType: "QUERY",
		Data:       resultData,
	}
}

// executeShowErrors 执行 SHOW ERRORS
func (e *XMySQLExecutor) executeShowErrors(ctx *ExecutionContext) {
	logger.Debugf(" [executeShowErrors] 执行SHOW ERRORS")
	errorWarnings := make([]Warning, 0)
	for _, warning := range sessionWarnings(ctx) {
		if strings.EqualFold(warning.Level, "error") {
			errorWarnings = append(errorWarnings, warning)
		}
	}
	start, end := showWarningsBounds(ctx.RawQuery, len(errorWarnings))
	rows := make([][]interface{}, 0, end-start)
	for _, warning := range errorWarnings[start:end] {
		rows = append(rows, []interface{}{warning.Level, warning.Code, warning.Message})
	}

	resultData := map[string]interface{}{
		"columns": []string{"Level", "Code", "Message"},
		"rows":    rows,
	}

	ctx.Results <- &Result{
		ResultType: "QUERY",
		Data:       resultData,
	}
}
