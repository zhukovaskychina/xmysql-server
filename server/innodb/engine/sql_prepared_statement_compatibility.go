package engine

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/observability/compatibility"
)

const sqlPreparedStatementSessionParam = "sql_prepared_stmt_mgr"

const sqlPreparedStatementIDBase uint32 = 1 << 30

type sqlPreparedStatement struct {
	compatibility.PreparedStatementSnapshot
}

type sqlPreparedStatementManager struct {
	mu         sync.RWMutex
	statements map[string]*sqlPreparedStatement
	nextID     uint32
}

func newSQLPreparedStatementManager() *sqlPreparedStatementManager {
	return &sqlPreparedStatementManager{
		statements: make(map[string]*sqlPreparedStatement),
		nextID:     sqlPreparedStatementIDBase,
	}
}

func sqlPreparedStatementManagerForSession(session server.MySQLServerSession) *sqlPreparedStatementManager {
	if session == nil {
		return nil
	}
	if manager, ok := session.GetParamByName(sqlPreparedStatementSessionParam).(*sqlPreparedStatementManager); ok && manager != nil {
		return manager
	}
	manager := newSQLPreparedStatementManager()
	session.SetParamByName(sqlPreparedStatementSessionParam, manager)
	return manager
}

func (m *sqlPreparedStatementManager) Prepare(name, query string) {
	if m == nil {
		return
	}
	name = strings.ToLower(strings.TrimSpace(name))
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statements[name] = &sqlPreparedStatement{PreparedStatementSnapshot: compatibility.PreparedStatementSnapshot{
		ID:         m.nextID,
		Name:       name,
		SQL:        strings.TrimSpace(query),
		CreatedAt:  time.Now(),
		LastUsedAt: time.Now(),
	}}
	m.nextID++
}

func (m *sqlPreparedStatementManager) Get(name string) (compatibility.PreparedStatementSnapshot, bool) {
	if m == nil {
		return compatibility.PreparedStatementSnapshot{}, false
	}
	name = strings.ToLower(strings.TrimSpace(name))
	m.mu.Lock()
	defer m.mu.Unlock()
	statement, ok := m.statements[name]
	if !ok || statement == nil {
		return compatibility.PreparedStatementSnapshot{}, false
	}
	statement.LastUsedAt = time.Now()
	return statement.PreparedStatementSnapshot, true
}

func (m *sqlPreparedStatementManager) RecordExecution(name string, stats compatibility.PreparedStatementExecutionStats) error {
	if m == nil {
		return fmt.Errorf("SQL prepared statement manager is unavailable")
	}
	name = strings.ToLower(strings.TrimSpace(name))
	m.mu.Lock()
	defer m.mu.Unlock()
	statement, ok := m.statements[name]
	if !ok || statement == nil {
		return fmt.Errorf("Unknown prepared statement handler (%s)", name)
	}
	statement.LastUsedAt = time.Now()
	duration := maxPreparedStatementDuration(stats.Duration)
	statement.ExecuteCount++
	statement.ExecuteTimeTotal += duration
	if statement.ExecuteCount == 1 || statement.ExecuteTimeMin == 0 || duration < statement.ExecuteTimeMin {
		statement.ExecuteTimeMin = duration
	}
	if duration > statement.ExecuteTimeMax {
		statement.ExecuteTimeMax = duration
	}
	if stats.Failed {
		statement.ErrorCount++
	}
	statement.WarningCount += stats.Warnings
	statement.RowsAffected += stats.RowsAffected
	statement.RowsSent += stats.RowsSent
	statement.RowsExamined += stats.RowsExamined
	if stats.CPUTimeCaptured {
		statement.CPUTimeTotal += stats.CPUTime
	}
	return nil
}

func maxPreparedStatementDuration(duration time.Duration) time.Duration {
	if duration < 0 {
		return 0
	}
	return duration
}

func (m *sqlPreparedStatementManager) Deallocate(name string) bool {
	if m == nil {
		return false
	}
	name = strings.ToLower(strings.TrimSpace(name))
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.statements[name]; !ok {
		return false
	}
	delete(m.statements, name)
	return true
}

func (m *sqlPreparedStatementManager) Snapshot() []compatibility.PreparedStatementSnapshot {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]compatibility.PreparedStatementSnapshot, 0, len(m.statements))
	for _, statement := range m.statements {
		if statement == nil {
			continue
		}
		result = append(result, statement.PreparedStatementSnapshot)
	}
	return result
}

var (
	sqlPreparePattern    = regexp.MustCompile(`(?is)^prepare\s+([a-zA-Z0-9_$]+)\s+from\s+(.+)$`)
	sqlExecutePattern    = regexp.MustCompile(`(?is)^execute\s+([a-zA-Z0-9_$]+)(?:\s+using\s+(.+))?$`)
	sqlDeallocatePattern = regexp.MustCompile(`(?is)^deallocate\s+(?:prepare\s+)?([a-zA-Z0-9_$]+)$`)
)

// executeSQLPreparedStatementCompatibility implements the SQL text-protocol
// PREPARE/EXECUTE/DEALLOCATE forms. Binary COM_STMT_* statements continue to
// use the protocol-owned manager; both inventories are projected by
// performance_schema.prepared_statements_instances.
func (e *XMySQLExecutor) executeSQLPreparedStatementCompatibility(ctx *ExecutionContext, session server.MySQLServerSession, query, databaseName string, results chan *Result) (bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	if match := sqlPreparePattern.FindStringSubmatch(trimmed); len(match) == 3 {
		if session == nil {
			return true, fmt.Errorf("PREPARE requires a session")
		}
		expression := e.rewriteSessionUserVariables(strings.TrimSpace(match[2]), session)
		value, err := evaluateStoredRoutineScalarExpression(expression)
		if err != nil {
			return true, err
		}
		statementSQL, ok := value.(string)
		if !ok || strings.TrimSpace(statementSQL) == "" {
			return true, fmt.Errorf("PREPARE source must evaluate to a non-empty string")
		}
		sqlPreparedStatementManagerForSession(session).Prepare(match[1], statementSQL)
		results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "statement prepared successfully"}
		return true, nil
	}
	if match := sqlDeallocatePattern.FindStringSubmatch(trimmed); len(match) == 2 {
		if session == nil {
			return true, fmt.Errorf("DEALLOCATE PREPARE requires a session")
		}
		if !sqlPreparedStatementManagerForSession(session).Deallocate(match[1]) {
			return true, fmt.Errorf("Unknown prepared statement handler (%s)", match[1])
		}
		results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "statement deallocated successfully"}
		return true, nil
	}
	if match := sqlExecutePattern.FindStringSubmatch(trimmed); len(match) >= 2 {
		if session == nil {
			return true, fmt.Errorf("EXECUTE requires a session")
		}
		manager := sqlPreparedStatementManagerForSession(session)
		statement, ok := manager.Get(match[1])
		if !ok {
			return true, fmt.Errorf("Unknown prepared statement handler (%s)", match[1])
		}
		values := make([]interface{}, 0)
		if len(match) == 3 && strings.TrimSpace(match[2]) != "" {
			for _, argument := range splitTopLevelComma(match[2]) {
				argument = strings.TrimSpace(argument)
				value := session.GetParamByName(argument)
				if value == nil && strings.HasPrefix(argument, "@") {
					value = session.GetParamByName(strings.TrimPrefix(argument, "@"))
				}
				values = append(values, value)
			}
		}
		boundSQL, err := bindRoutineParameterMarkers(statement.SQL, values)
		if err != nil {
			return true, err
		}
		startedAt := time.Now()
		nestedResults := make(chan *Result, 16)
		nestedContext := &ExecutionContext{Context: ctx.Context, Results: nestedResults, Cfg: e.conf, DatabaseName: databaseName, RawQuery: boundSQL, Session: session}
		e.executeQuery(nestedContext, session, boundSQL, databaseName, nestedResults)
		failed := false
		var warnings, rowsAffected, rowsSent int64
		for result := range nestedResults {
			if result != nil {
				if result.Err != nil {
					failed = true
				}
				accounting := statementResultAccountingFor(result)
				warnings += accounting.warnings
				rowsAffected += accounting.rowsAffected
				rowsSent += accounting.rowsSent
			}
			results <- result
		}
		_ = manager.RecordExecution(match[1], compatibility.PreparedStatementExecutionStats{
			Duration: time.Since(startedAt), Failed: failed, Warnings: uint64(maxInt64(0, warnings)),
			RowsAffected: uint64(maxInt64(0, rowsAffected)), RowsSent: uint64(maxInt64(0, rowsSent)),
			RowsExamined: uint64(maxInt64(0, nestedContext.statementRowsExamined.Load())),
			CPUTime:      uint64(maxInt64(0, nestedContext.statementCPUTime.Load())), CPUTimeCaptured: nestedContext.statementCPUTimeCaptured.Load(),
		})
		return true, nil
	}
	return false, nil
}
