package engine

import (
	"fmt"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/observability/compatibility"
)

// executeParsedPreparedStatement handles SQL-level prepared statements from
// their structured node. Binary protocol prepared statements continue to use
// the protocol-owned manager and do not enter this path.
func (e *XMySQLExecutor) executeParsedPreparedStatement(ctx *ExecutionContext, session server.MySQLServerSession, stmt *sqlparser.PreparedStatement, databaseName string, results chan *Result) (bool, error) {
	if stmt == nil {
		return false, nil
	}
	if session == nil {
		return true, fmt.Errorf("%s requires a session", strings.ToUpper(stmt.Kind))
	}
	manager := sqlPreparedStatementManagerForSession(session)
	switch strings.ToLower(stmt.Kind) {
	case "prepare":
		source := stmt.SourceExpression
		if strings.TrimSpace(source) == "" {
			source = stmt.Source
		}
		expression := e.rewriteSessionUserVariables(strings.TrimSpace(source), session)
		value, err := evaluateStoredRoutineScalarExpression(expression)
		if err != nil {
			return true, err
		}
		sqlText, ok := value.(string)
		if !ok || strings.TrimSpace(sqlText) == "" {
			return true, fmt.Errorf("PREPARE source must evaluate to a non-empty string")
		}
		manager.Prepare(stmt.Name, sqlText)
		results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "statement prepared successfully"}
		return true, nil
	case "deallocate":
		if !manager.Deallocate(stmt.Name) {
			return true, fmt.Errorf("Unknown prepared statement handler (%s)", stmt.Name)
		}
		results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "statement deallocated successfully"}
		return true, nil
	case "execute":
		prepared, ok := manager.Get(stmt.Name)
		if !ok {
			return true, fmt.Errorf("Unknown prepared statement handler (%s)", stmt.Name)
		}
		values := make([]interface{}, 0, len(stmt.Parameters))
		for _, argument := range stmt.Parameters {
			value := session.GetParamByName(argument)
			if value == nil && strings.HasPrefix(argument, "@") {
				value = session.GetParamByName(strings.TrimPrefix(argument, "@"))
			}
			values = append(values, value)
		}
		boundSQL, err := bindRoutineParameterMarkers(prepared.SQL, values)
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
		_ = manager.RecordExecution(stmt.Name, compatibility.PreparedStatementExecutionStats{Duration: time.Since(startedAt), Failed: failed, Warnings: uint64(maxInt64(0, warnings)), RowsAffected: uint64(maxInt64(0, rowsAffected)), RowsSent: uint64(maxInt64(0, rowsSent)), RowsExamined: uint64(maxInt64(0, nestedContext.statementRowsExamined.Load())), CPUTime: uint64(maxInt64(0, nestedContext.statementCPUTime.Load())), CPUTimeCaptured: nestedContext.statementCPUTimeCaptured.Load()})
		return true, nil
	default:
		return false, nil
	}
}
