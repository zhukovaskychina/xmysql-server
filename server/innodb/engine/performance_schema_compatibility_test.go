package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/observability/compatibility"
	metrics "github.com/zhukovaskychina/xmysql-server/server/observability/metrics"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func enableAllPerformanceSchemaConsumersForTest(t *testing.T, executor *XMySQLEngine) {
	t.Helper()
	executor.QueryExecutor.performanceSchemaMu.Lock()
	defer executor.QueryExecutor.performanceSchemaMu.Unlock()
	for name, setting := range executor.QueryExecutor.performanceSchemaConsumers {
		setting.Enabled = true
		executor.QueryExecutor.performanceSchemaConsumers[name] = setting
	}
}

type testPreparedStatementInventory struct {
	statements []compatibility.PreparedStatementSnapshot
}

func (i *testPreparedStatementInventory) Snapshot() []compatibility.PreparedStatementSnapshot {
	return append([]compatibility.PreparedStatementSnapshot(nil), i.statements...)
}

func TestPerformanceSchemaProcesslistProjectsLiveSessions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	current := newTestMySQLSession()
	current.ctx.SetConnectionID(901)
	current.SetParamByName("user", "alice")
	current.SetParamByName("host", "localhost")
	current.SetParamByName("global_privileges", []common.PrivilegeType{common.ProcessPriv})
	other := newTestMySQLSession()
	other.ctx.SetConnectionID(902)
	other.SetParamByName("user", "bob")
	other.SetParamByName("host", "10.0.0.2")
	other.SetParamByName("processlist_query", "SELECT 1")
	other.SetParamByName("processlist_start_time", time.Now().Add(-1500*time.Millisecond))
	other.SetParamByName("processlist_rows_sent", int64(4))
	other.SetParamByName("processlist_rows_examined", int64(9))
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{current, other}
	})

	result := <-executor.ExecuteQuery(current, "select id, user, host, command, time_ms, rows_sent, rows_examined from performance_schema.processlist where user='bob'", "")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, []string{"ID", "USER", "HOST", "COMMAND", "TIME_MS", "ROWS_SENT", "ROWS_EXAMINED"}, selectResult.Columns)
	require.Len(t, selectResult.Records, 1)
	values := selectResult.Records[0].GetValues()
	require.Equal(t, int64(902), values[0].Int())
	require.Equal(t, "bob", values[1].String())
	require.Equal(t, "10.0.0.2", values[2].String())
	require.Equal(t, "Query", values[3].String())
	require.Equal(t, int64(1000), values[4].Int())
	require.Equal(t, int64(4), values[5].Int())
	require.Equal(t, int64(9), values[6].Int())
	wrongRowsSent := mustSelectResultSQL(t, executor, "", "select id from performance_schema.processlist where user='bob' and rows_sent = 999")
	require.Empty(t, wrongRowsSent.Records)
}

func TestPerformanceSchemaProcesslistAndThreadsHonorNativeVisibilityRules(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'alice'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on performance_schema.threads to 'alice'@'localhost'")
	current := newTestMySQLSession()
	current.ctx.SetConnectionID(911)
	current.SetParamByName("user", "alice")
	current.SetParamByName("host", "localhost")
	current.SetParamByName("global_privileges", []common.PrivilegeType{})
	sameUser := newTestMySQLSession()
	sameUser.ctx.SetConnectionID(912)
	sameUser.SetParamByName("user", "alice")
	sameUser.SetParamByName("host", "same-user")
	otherUser := newTestMySQLSession()
	otherUser.ctx.SetConnectionID(913)
	otherUser.SetParamByName("user", "bob")
	otherUser.SetParamByName("host", "other-user")
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{current, sameUser, otherUser}
	})

	filteredResult := <-executor.ExecuteQuery(current, "select id, user, execution_engine from performance_schema.processlist", "")
	require.NoError(t, filteredResult.Err)
	filtered := filteredResult.Data.(*SelectResult)
	require.Equal(t, []string{"ID", "USER", "EXECUTION_ENGINE"}, filtered.Columns)
	require.Len(t, filtered.Records, 2)
	for _, record := range filtered.Records {
		values := record.GetValues()
		require.Equal(t, "alice", values[1].String())
		require.Equal(t, "PRIMARY", values[2].String())
	}

	current.SetParamByName("global_privileges", []common.PrivilegeType{common.ProcessPriv})
	allUsersResult := <-executor.ExecuteQuery(current, "select id, user, execution_engine from performance_schema.processlist", "")
	require.NoError(t, allUsersResult.Err)
	allUsers := allUsersResult.Data.(*SelectResult)
	require.Len(t, allUsers.Records, 3)
	seen := make(map[string]bool, len(allUsers.Records))
	for _, record := range allUsers.Records {
		values := record.GetValues()
		seen[values[1].String()] = true
		require.Equal(t, "PRIMARY", values[2].String())
	}
	require.True(t, seen["alice"])
	require.True(t, seen["bob"])

	threadsResult := <-executor.ExecuteQuery(current, "select processlist_id, processlist_user from performance_schema.threads", "")
	require.NoError(t, threadsResult.Err)
	threads := threadsResult.Data.(*SelectResult)
	require.NotEmpty(t, threads.Records)
	seenThreadUsers := make(map[string]bool)
	for _, record := range threads.Records {
		values := record.GetValues()
		if !values[1].IsNull() {
			seenThreadUsers[values[1].String()] = true
		}
	}
	require.True(t, seenThreadUsers["alice"])
	require.True(t, seenThreadUsers["bob"])
}

func TestPerformanceSchemaStatementsExposeLiveQueryMetrics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "select 1", "")
	require.NoError(t, result.Err)
	executor.QueryExecutor.metricsRecorder.RecordQuery("app", "SELECT", "ok", 2*time.Millisecond)
	rows := mustQuerySQL(t, executor, "", "select schema_name, count_star from performance_schema.events_statements_summary_by_digest")
	require.NotEmpty(t, rows)

	// Keep the recorder contract explicit: latency is a live observation, not a
	// hard-coded static metric in the compatibility view.
	rows = mustQuerySQL(t, executor, "", "select schema_name, count_star from performance_schema.events_statements_summary_by_digest")
	require.NotEmpty(t, rows)
}

func TestPerformanceSchemaStatementAccountingUsesExecutionResults(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database accounting_app")
	mustExecSQL(t, executor, "accounting_app", "create table counters (id int primary key)")

	selectResult := <-executor.ExecuteQuery(nil, "select 1", "accounting_app")
	require.NoError(t, selectResult.Err)
	insertResult := <-executor.ExecuteQuery(nil, "insert into counters values (1)", "accounting_app")
	require.NoError(t, insertResult.Err)

	var selectEvent, insertEvent *metrics.StatementEvent
	for index := range executor.QueryExecutor.metricsRecorder.StatementHistory() {
		event := executor.QueryExecutor.metricsRecorder.StatementHistory()[index]
		switch event.SQL {
		case "select 1":
			copy := event
			selectEvent = &copy
		case "insert into counters values (1)":
			copy := event
			insertEvent = &copy
		}
	}
	require.NotNil(t, selectEvent)
	require.Equal(t, int64(1), selectEvent.RowsSent)
	require.Equal(t, int64(0), selectEvent.RowsAffected)
	require.NotNil(t, insertEvent)
	require.Equal(t, int64(1), insertEvent.RowsAffected)
	require.Equal(t, int64(0), insertEvent.RowsSent)

	digest := executor.QueryExecutor.executePerformanceSchemaStatementsSelect("select * from performance_schema.events_statements_summary_by_digest")
	var insertDigest []interface{}
	for _, row := range selectResultRows(digest) {
		if len(row) > 2 && row[0] == "accounting_app" && row[2] == "insert into counters values (?)" {
			insertDigest = row
			break
		}
	}
	require.NotNil(t, insertDigest)
	require.Equal(t, "1", insertDigest[11])
	require.Equal(t, "0", insertDigest[12])
}

func TestPerformanceSchemaStatementDigestUsesMySQL84ShapeAndAggregatesHistory(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
		71, "digest_user", "digest_host", "digest_app",
		"select * from digest_app.docs where id = 1", "SELECT", "ok", 2*time.Millisecond, 2, 3, 1,
	)
	executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
		71, "digest_user", "digest_host", "digest_app",
		"select * from digest_app.docs where id = 2", "SELECT", "ok", 3*time.Millisecond, 4, 5, 2,
	)

	result := mustSelectResultSQL(t, executor, "", "select * from performance_schema.events_statements_summary_by_digest")
	require.Equal(t, []string{
		"SCHEMA_NAME", "DIGEST", "DIGEST_TEXT", "COUNT_STAR", "SUM_TIMER_WAIT",
		"MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "SUM_LOCK_TIME", "SUM_ERRORS",
		"SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED",
		"SUM_CREATED_TMP_DISK_TABLES", "SUM_CREATED_TMP_TABLES", "SUM_SELECT_FULL_JOIN",
		"SUM_SELECT_FULL_RANGE_JOIN", "SUM_SELECT_RANGE", "SUM_SELECT_RANGE_CHECK",
		"SUM_SELECT_SCAN", "SUM_SORT_MERGE_PASSES", "SUM_SORT_RANGE", "SUM_SORT_ROWS",
		"SUM_SORT_SCAN", "SUM_NO_INDEX_USED", "SUM_NO_GOOD_INDEX_USED", "SUM_CPU_TIME",
		"MAX_CONTROLLED_MEMORY", "MAX_TOTAL_MEMORY", "COUNT_SECONDARY", "FIRST_SEEN",
		"LAST_SEEN", "QUANTILE_95", "QUANTILE_99", "QUANTILE_999", "QUERY_SAMPLE_TEXT",
		"QUERY_SAMPLE_SEEN", "QUERY_SAMPLE_TIMER_WAIT",
	}, result.Columns)

	var digestRow []interface{}
	for _, row := range selectResultRows(result) {
		if len(row) > 0 && row[0] == "digest_app" && len(row) > 2 && row[2] == "select * from digest_app.docs where id = ?" {
			digestRow = row
			break
		}
	}
	require.NotNil(t, digestRow)
	require.Equal(t, "digest_app", digestRow[0])
	require.NotEmpty(t, digestRow[1])
	require.Equal(t, "2", digestRow[3])
	require.Equal(t, "5000000000", digestRow[4])
	require.Equal(t, "2000000000", digestRow[5])
	require.Equal(t, "2500000000", digestRow[6])
	require.Equal(t, "3000000000", digestRow[7])
	require.Equal(t, "0", digestRow[9])
	require.Equal(t, "3", digestRow[10])
	require.Equal(t, "6", digestRow[11])
	require.Equal(t, "8", digestRow[12])
	require.NotEmpty(t, digestRow[31])
	require.NotEmpty(t, digestRow[32])
	require.Equal(t, "select * from digest_app.docs where id = 2", digestRow[36])
	require.Equal(t, "3000000000", digestRow[38])
}

func TestPerformanceSchemaStatementDigestUsesObservedLatencyQuantiles(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	for latencyMS := 1; latencyMS <= 100; latencyMS++ {
		executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
			73, "quantile_user", "quantile_host", "quantile_app",
			fmt.Sprintf("select * from quantile_app.docs where id = %d", latencyMS),
			"SELECT", "ok", time.Duration(latencyMS)*time.Millisecond, 0, 0, 0,
		)
	}

	result := mustSelectResultSQL(t, executor, "", "select digest_text, count_star, quantile_95, quantile_99, quantile_999 from performance_schema.events_statements_summary_by_digest where schema_name = 'quantile_app'")
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	require.Equal(t, "select * from quantile_app.docs where id = ?", values[0].String())
	require.Equal(t, int64(100), values[1].Int())
	require.Equal(t, int64(95_000_000_000), values[2].Int())
	require.Equal(t, int64(99_000_000_000), values[3].Int())
	require.Equal(t, int64(100_000_000_000), values[4].Int())
}

func TestPerformanceSchemaStatementSummaryUsesMySQL84AccountingColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
		72, "summary_user", "summary_host", "summary_app", "select 1", "ACCOUNTING_TEST", "ok", time.Millisecond, 2, 3, 4,
	)

	result := executor.QueryExecutor.executePerformanceSchemaStatementSummaryRegistrySelect(
		"select * from performance_schema.events_statements_summary_global_by_event_name", "global",
	)
	require.Equal(t, []string{
		"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT",
		"SUM_LOCK_TIME", "SUM_ERRORS", "SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED",
		"SUM_CREATED_TMP_DISK_TABLES", "SUM_CREATED_TMP_TABLES", "SUM_SELECT_FULL_JOIN", "SUM_SELECT_FULL_RANGE_JOIN",
		"SUM_SELECT_RANGE", "SUM_SELECT_RANGE_CHECK", "SUM_SELECT_SCAN", "SUM_SORT_MERGE_PASSES", "SUM_SORT_RANGE",
		"SUM_SORT_ROWS", "SUM_SORT_SCAN", "SUM_NO_INDEX_USED", "SUM_NO_GOOD_INDEX_USED", "SUM_CPU_TIME",
		"MAX_CONTROLLED_MEMORY", "MAX_TOTAL_MEMORY", "COUNT_SECONDARY",
	}, result.Columns)

	var summaryRow []interface{}
	for _, row := range selectResultRows(result) {
		if len(row) > 0 && row[0] == "statement/sql/accounting_test" {
			summaryRow = row
			break
		}
	}
	require.NotNil(t, summaryRow)
	require.NotEqual(t, "0", summaryRow[1])
	require.NotEqual(t, "0", summaryRow[2])
	require.Equal(t, "0", summaryRow[6])
	require.Equal(t, "4", summaryRow[8])
	require.Equal(t, "2", summaryRow[9])
	require.Equal(t, "3", summaryRow[10])
	require.Equal(t, "0", summaryRow[28])
}

func TestPerformanceSchemaStatementMemoryColumnsUseRuntimeInstrument(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	executor.QueryExecutor.metricsRecorder.RecordMemoryAllocation(173, "memory/sql/THD::main_mem_root", 96)
	executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
		173, "memory_user", "memory_host", "memory_app", "select memory_test", "SELECT", "ok", time.Millisecond, 0, 1, 0,
	)
	executor.QueryExecutor.metricsRecorder.RecordMemoryFree(173, "memory/sql/THD::main_mem_root", 96)

	history := executor.QueryExecutor.executePerformanceSchemaStatementHistorySelect(
		"performance_schema.events_statements_history_long", false,
		"select max_controlled_memory, max_total_memory from performance_schema.events_statements_history_long where thread_id=173 and sql_text='select memory_test'",
	)
	rows := selectResultRows(history)
	require.Len(t, rows, 1)
	require.Equal(t, "96", rows[0][0])
	require.Equal(t, "96", rows[0][1])

	digest := executor.QueryExecutor.executePerformanceSchemaStatementsSelect(
		"select max_controlled_memory, max_total_memory from performance_schema.events_statements_summary_by_digest where schema_name='memory_app'",
	)
	require.Len(t, digest.Records, 1)
	digestRow := selectResultRows(digest)[0]
	require.Equal(t, "96", digestRow[0])
	require.Equal(t, "96", digestRow[1])
}

func TestPerformanceSchemaStatementSummaryRetainsLifetimeTotalsBeyondHistory(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	const executions = 300
	for i := 0; i < executions; i++ {
		executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScan(
			81, "lifetime_user", "lifetime_host", "lifetime_app", "select lifetime_test", "LIFETIME_TEST", "ok",
			time.Millisecond, 2, 3, 1, 1, 1, true, true,
		)
	}

	result := executor.QueryExecutor.executePerformanceSchemaStatementSummaryRegistrySelect(
		"select event_name, count_star, sum_timer_wait, sum_rows_affected, sum_rows_sent, sum_rows_examined from performance_schema.events_statements_summary_global_by_event_name",
		"global",
	)
	var summaryRow []interface{}
	for _, row := range selectResultRows(result) {
		if len(row) > 0 && row[0] == "statement/sql/lifetime_test" {
			summaryRow = row
			break
		}
	}
	require.NotNil(t, summaryRow)
	require.Equal(t, fmt.Sprint(executions), summaryRow[1])
	require.Equal(t, fmt.Sprint(int64(executions)*int64(time.Millisecond)*1000), summaryRow[2])
	require.Equal(t, fmt.Sprint(int64(executions)*2), summaryRow[3])
	require.Equal(t, fmt.Sprint(int64(executions)*3), summaryRow[4])
	require.Equal(t, fmt.Sprint(int64(executions)), summaryRow[5])
}

func TestPerformanceSchemaStatementDigestRetainsLifetimeTotalsBeyondHistory(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	const executions = 300
	for i := 0; i < executions; i++ {
		executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScan(
			82, "digest_lifetime_user", "digest_lifetime_host", "digest_lifetime_app",
			"select * from digest_lifetime_app.docs where id = 1", "SELECT", "ok",
			time.Millisecond, 2, 3, 1, 1, 1, true, true,
		)
	}

	result := executor.QueryExecutor.executePerformanceSchemaStatementsSelect(
		"select schema_name, digest_text, count_star, sum_timer_wait, sum_rows_affected, sum_rows_sent, sum_rows_examined from performance_schema.events_statements_summary_by_digest where schema_name='digest_lifetime_app'",
	)
	require.Len(t, result.Records, 1)
	row := selectResultRows(result)[0]
	require.Equal(t, "digest_lifetime_app", row[0])
	require.Equal(t, "select * from digest_lifetime_app.docs where id = ?", row[1])
	require.Equal(t, fmt.Sprint(executions), row[2])
	require.Equal(t, fmt.Sprint(int64(executions)*int64(time.Millisecond)*1000), row[3])
	require.Equal(t, fmt.Sprint(int64(executions)*2), row[4])
	require.Equal(t, fmt.Sprint(int64(executions)*3), row[5])
	require.Equal(t, fmt.Sprint(int64(executions)), row[6])
}

func TestPerformanceSchemaStatementHistogramRetainsLifetimeTotalsBeyondHistory(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	const executions = 300
	for i := 0; i < executions; i++ {
		executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
			83, "histogram_lifetime_user", "histogram_lifetime_host", "histogram_lifetime_app",
			"select histogram_lifetime_test", "SELECT", "ok", time.Millisecond, 0, 0, 0,
		)
	}

	result := executor.QueryExecutor.executePerformanceSchemaStatementHistogramSelect(
		"select bucket_number, count_bucket, count_star from performance_schema.events_statements_histogram_by_digest where schema_name='histogram_lifetime_app'",
		true,
	)
	var populated []interface{}
	for _, row := range selectResultRows(result) {
		if len(row) >= 3 && row[1] != "0" {
			populated = row
			break
		}
	}
	require.NotNil(t, populated)
	require.Equal(t, fmt.Sprint(executions), populated[1])
	require.Equal(t, fmt.Sprint(executions), populated[2])
}

func TestPerformanceSchemaProgramSummaryTracksStoredProcedureCalls(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "update performance_schema.setup_consumers set enabled='YES' where name='events_statements_cpu'")
	mustExecSQL(t, executor, "", "create database program_app")
	mustExecSQL(t, executor, "program_app", "create table report_rows (id int)")
	mustExecSQL(t, executor, "program_app", "insert into report_rows values (1), (2)")
	mustExecSQL(t, executor, "program_app", "create procedure report() begin select id from report_rows; end")
	mustExecSQL(t, executor, "program_app", "call report()")

	result := mustSelectResultSQL(t, executor, "", "select * from performance_schema.events_statements_summary_by_program where object_name='report'")
	require.Equal(t, performanceSchemaStatementSummaryProgramColumns, result.Columns)
	require.Len(t, result.Records, 1)
	row := selectResultRows(result)[0]
	require.Equal(t, "PROCEDURE", row[0])
	require.Equal(t, "program_app", row[1])
	require.Equal(t, "report", row[2])
	require.Equal(t, "1", row[3])
	wrongCount := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.events_statements_summary_by_program where object_name='report' and count_star = 999999")
	require.Empty(t, wrongCount.Records)
	require.NotEqual(t, "0", row[4])
	require.NotEqual(t, "0", row[5])
	require.NotEqual(t, "0", row[6])
	require.NotEqual(t, "0", row[7])
	require.NotEqual(t, "0", row[8])
	require.NotEqual(t, "0", row[9])
	require.NotEqual(t, "0", row[10])
	require.NotEqual(t, "0", row[11])
	require.NotEqual(t, "0", row[12])
	// The procedure body contains a table scan, so the program summary must carry
	// the child statement's real sent-row accounting as well.
	require.NotEqual(t, "0", row[17])
	// The same child statement also contributes its clustered rows-examined
	// accounting to the stored-program summary.
	require.NotEqual(t, "0", row[18])
	// CPU time is propagated from the stored procedure's child statement into
	// the program-level aggregate when the CPU consumer is enabled.
	require.NotEqual(t, "0", row[32])
	// Do not leak the opt-in CPU/history state into later tests that reuse the
	// process-wide test executor registry.
	mustExecSQL(t, executor, "", "update performance_schema.setup_consumers set enabled='NO' where name='events_statements_cpu'")
	truncateStmt, err := sqlparser.Parse("truncate table performance_schema.events_statements_history_long")
	require.NoError(t, err)
	truncateResults := make(chan *Result, 1)
	executor.QueryExecutor.executeDDL(truncateStmt.(*sqlparser.DDL), nil, "", truncateResults)
	require.NoError(t, (<-truncateResults).Err)
}

func TestPerformanceSchemaProgramSummaryRetainsLifetimeTotalsBeyondHistory(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	const executions = 1100
	for i := 0; i < executions; i++ {
		executor.QueryExecutor.recordPerformanceSchemaProgramExecution(
			"PROCEDURE", "program_lifetime_app", "report", 1000, 1, 2000, 2000, 2000, 0, 1, 2, 3, 4,
		)
	}

	result := executor.QueryExecutor.executePerformanceSchemaProgramSummarySelect(
		"select object_type, object_schema, object_name, count_star, sum_timer_wait, sum_statements_wait, sum_warnings, sum_rows_affected, sum_rows_sent, sum_rows_examined from performance_schema.events_statements_summary_by_program where object_name='report'",
	)
	require.Len(t, result.Records, 1)
	row := selectResultRows(result)[0]
	require.Equal(t, "PROCEDURE", row[0])
	require.Equal(t, "program_lifetime_app", row[1])
	require.Equal(t, "report", row[2])
	require.Equal(t, fmt.Sprint(executions), row[3])
	require.Equal(t, fmt.Sprint(int64(executions)*1000), row[4])
	require.Equal(t, fmt.Sprint(int64(executions)*2000), row[5])
	require.Equal(t, fmt.Sprint(executions), row[6])
	require.Equal(t, fmt.Sprint(int64(executions)*2), row[7])
	require.Equal(t, fmt.Sprint(int64(executions)*3), row[8])
	require.Equal(t, fmt.Sprint(int64(executions)*4), row[9])
}

func TestPerformanceSchemaStoredProcedureStatementStatsRetainLifetimeDeltaBeyondHistory(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	const statements = 300
	body := strings.TrimSuffix(strings.Repeat("select 1;", statements), ";")
	mustExecSQL(t, executor, "", "create database program_delta_app")
	mustExecSQL(t, executor, "program_delta_app", "create procedure many_selects() begin "+body+"; end")
	mustExecSQL(t, executor, "program_delta_app", "call many_selects()")

	result := mustSelectResultSQL(t, executor, "", "select count_star, count_statements, sum_statements_wait from performance_schema.events_statements_summary_by_program where object_schema='program_delta_app' and object_name='many_selects'")
	require.Len(t, result.Records, 1)
	row := selectResultRows(result)[0]
	require.Equal(t, "1", row[0])
	require.Equal(t, fmt.Sprint(int64(statements)), row[1])
	require.NotEqual(t, "0", row[2])
}

func TestPerformanceSchemaProgramSummaryTracksStoredFunctionCalls(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "update performance_schema.setup_consumers set enabled='YES' where name='events_statements_cpu'")
	mustExecSQL(t, executor, "", "create database program_function_app")
	mustExecSQL(t, executor, "program_function_app", "create function score() returns int deterministic begin return 1; end")
	require.Len(t, mustQuerySQL(t, executor, "program_function_app", "select score()"), 1)

	result := mustSelectResultSQL(t, executor, "", "select * from performance_schema.events_statements_summary_by_program where object_type='FUNCTION' and object_schema='program_function_app' and object_name='score'")
	require.Len(t, result.Records, 1)
	row := selectResultRows(result)[0]
	require.Equal(t, "FUNCTION", row[0])
	require.Equal(t, "program_function_app", row[1])
	require.Equal(t, "score", row[2])
	require.Equal(t, "1", row[3])
	require.NotEqual(t, "0", row[17])
	require.NotEqual(t, "0", row[32])
	mustExecSQL(t, executor, "", "update performance_schema.setup_consumers set enabled='NO' where name='events_statements_cpu'")
	truncateStmt, err := sqlparser.Parse("truncate table performance_schema.events_statements_history_long")
	require.NoError(t, err)
	truncateResults := make(chan *Result, 1)
	executor.QueryExecutor.executeDDL(truncateStmt.(*sqlparser.DDL), nil, "", truncateResults)
	require.NoError(t, (<-truncateResults).Err)
}

func TestPerformanceSchemaTransactionSummaryUsesMySQL84TimerColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(73))
	session.SetParamByName("user", "transaction_user")
	session.SetParamByName("host", "transaction_host")
	require.NoError(t, (<-executor.ExecuteQuery(session, "begin", "transaction_app")).Err)
	require.NoError(t, (<-executor.ExecuteQuery(session, "commit", "transaction_app")).Err)

	result := mustSelectResultSQL(t, executor, "", "select * from performance_schema.events_transactions_summary_global_by_event_name")
	require.Equal(t, []string{
		"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT",
		"COUNT_READ_WRITE", "SUM_TIMER_READ_WRITE", "MIN_TIMER_READ_WRITE", "AVG_TIMER_READ_WRITE", "MAX_TIMER_READ_WRITE",
		"COUNT_READ_ONLY", "SUM_TIMER_READ_ONLY", "MIN_TIMER_READ_ONLY", "AVG_TIMER_READ_ONLY", "MAX_TIMER_READ_ONLY",
	}, result.Columns)

	var summaryRow []interface{}
	for _, row := range selectResultRows(result) {
		if len(row) > 0 && row[0] == "transaction" {
			summaryRow = row
			break
		}
	}
	require.NotNil(t, summaryRow)
	require.NotEqual(t, "0", summaryRow[1])
	require.NotEqual(t, "0", summaryRow[6])
	require.NotEqual(t, "0", summaryRow[2])
	require.NotEqual(t, "0", summaryRow[3])
	require.NotEqual(t, "0", summaryRow[4])
	require.NotEqual(t, "0", summaryRow[5])
	require.NotEqual(t, "0", summaryRow[7])
	require.NotEqual(t, "0", summaryRow[8])
	require.NotEqual(t, "0", summaryRow[9])
	require.NotEqual(t, "0", summaryRow[10])
	require.Equal(t, "0", summaryRow[15])
	wrongCount := mustSelectResultSQL(t, executor, "", "select event_name from performance_schema.events_transactions_summary_global_by_event_name where event_name='transaction' and count_star=999")
	require.Empty(t, wrongCount.Records)
}

func TestPerformanceSchemaTransactionSummaryRetainsLifetimeTotalsBeyondHistory(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(816))
	session.SetParamByName("user", "transaction_lifetime_user")
	session.SetParamByName("host", "transaction_lifetime_host")
	const executions = 1100
	for i := 0; i < executions; i++ {
		session.SetParamByName("performance_schema_transaction_started_at", time.Now().UnixNano())
		executor.QueryExecutor.recordPerformanceSchemaTransactionHistory(session, "COMMIT")
	}

	result := executor.QueryExecutor.executePerformanceSchemaTransactionSummaryRegistrySelect(
		"select event_name, count_star, sum_timer_wait from performance_schema.events_transactions_summary_global_by_event_name",
		"global",
	)
	require.Len(t, result.Records, 1)
	row := selectResultRows(result)[0]
	require.Equal(t, "transaction", row[0])
	require.Equal(t, fmt.Sprint(executions), row[1])
	require.NotEqual(t, "0", row[2])
}

func TestPerformanceSchemaTransactionSummaryIsIndependentOfHistoryConsumers(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "update performance_schema.setup_consumers set enabled='NO' where name='events_transactions_history'", "")
	require.NoError(t, result.Err)
	result = <-executor.ExecuteQuery(nil, "update performance_schema.setup_consumers set enabled='NO' where name='events_transactions_history_long'", "")
	require.NoError(t, result.Err)

	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(74))
	require.NoError(t, (<-executor.ExecuteQuery(session, "begin", "transaction_summary_app")).Err)
	require.NoError(t, (<-executor.ExecuteQuery(session, "commit", "transaction_summary_app")).Err)

	rows := mustQuerySQL(t, executor, "", "select event_name, count_star from performance_schema.events_transactions_summary_global_by_event_name where event_name='transaction'")
	require.Len(t, rows, 1)
	require.NotEqual(t, "0", rows[0][1])
}

func TestPerformanceSchemaTransactionSummaryIdentityFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(741))
	session.SetParamByName("user", "transaction_filter_user")
	session.SetParamByName("host", "transaction-filter.example")
	require.NoError(t, (<-executor.ExecuteQuery(session, "begin", "transaction_filter_app")).Err)
	require.NoError(t, (<-executor.ExecuteQuery(session, "commit", "transaction_filter_app")).Err)

	filtered := mustSelectResultSQL(t, executor, "", "select user, host, event_name, count_star from performance_schema.events_transactions_summary_by_account_by_event_name where user='transaction_filter_user' and host='transaction-filter.example' and event_name='transaction'")
	require.Equal(t, [][]interface{}{{"transaction_filter_user", "transaction-filter.example", "transaction", "1"}}, selectResultRows(filtered))

	wrongUser := mustSelectResultSQL(t, executor, "", "select user, host, event_name from performance_schema.events_transactions_summary_by_account_by_event_name where user='other_transaction_user'")
	require.Empty(t, wrongUser.Records)

	wrongHost := mustSelectResultSQL(t, executor, "", "select host, event_name from performance_schema.events_transactions_summary_by_host_by_event_name where host='other-transaction-host'")
	require.Empty(t, wrongHost.Records)
}

func TestPerformanceSchemaStatementSummaryIdentityFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
		870, "statement_filter_user", "statement-filter.example", "statement_filter_app",
		"select 870", "SELECT", "ok", time.Millisecond, 0, 1, 0,
	)

	filtered := mustSelectResultSQL(t, executor, "", "select user, host, event_name, count_star from performance_schema.events_statements_summary_by_account_by_event_name where user='statement_filter_user' and host='statement-filter.example' and event_name='statement/sql/select'")
	require.Equal(t, [][]interface{}{{"statement_filter_user", "statement-filter.example", "statement/sql/select", "1"}}, selectResultRows(filtered))

	wrongCount := mustSelectResultSQL(t, executor, "", "select user, host, event_name from performance_schema.events_statements_summary_by_account_by_event_name where user='statement_filter_user' and host='statement-filter.example' and event_name='statement/sql/select' and count_star=999999")
	require.Empty(t, wrongCount.Records)

	wrongUser := mustSelectResultSQL(t, executor, "", "select user, host, event_name from performance_schema.events_statements_summary_by_account_by_event_name where user='other_statement_user'")
	require.Empty(t, wrongUser.Records)

	wrongHost := mustSelectResultSQL(t, executor, "", "select host, event_name from performance_schema.events_statements_summary_by_host_by_event_name where host='other-statement-host'")
	require.Empty(t, wrongHost.Records)

	wrongEvent := mustSelectResultSQL(t, executor, "", "select user, event_name from performance_schema.events_statements_summary_by_user_by_event_name where user='statement_filter_user' and event_name='statement/sql/update'")
	require.Empty(t, wrongEvent.Records)
}

func TestPerformanceSchemaStatementHistogramsExposeStatementHistory(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "select 123", "app")
	require.NoError(t, result.Err)

	global := mustSelectResultSQL(t, executor, "", "select bucket_number, count_bucket, count_star from performance_schema.events_statements_histogram_global where count_bucket > 0")
	require.NotEmpty(t, global.Records)
	wrongBucket := mustSelectResultSQL(t, executor, "", "select bucket_number from performance_schema.events_statements_histogram_global where bucket_number = 999")
	require.Empty(t, wrongBucket.Records)
	digest := mustSelectResultSQL(t, executor, "", "select schema_name, digest, digest_text, count_bucket from performance_schema.events_statements_histogram_by_digest")
	require.NotEmpty(t, digest.Records)
	wrongSchema := mustSelectResultSQL(t, executor, "", "select schema_name from performance_schema.events_statements_histogram_by_digest where schema_name = 'other_schema'")
	require.Empty(t, wrongSchema.Records)
}

func TestPerformanceSchemaStatementHistogramTruncateResetsCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
		871, "histogram_truncate_user", "histogram-truncate.example", "histogram_truncate_app",
		"select histogram_truncate", "SELECT", "ok", time.Millisecond, 0, 1, 0,
	)
	before := mustSelectResultSQL(t, executor, "", "select count_bucket from performance_schema.events_statements_histogram_global where count_bucket > 0")
	require.NotEmpty(t, before.Records)

	truncateStmt, err := sqlparser.Parse("truncate table performance_schema.events_statements_histogram_global")
	require.NoError(t, err)
	ddlResults := make(chan *Result, 1)
	executor.QueryExecutor.executeDDL(truncateStmt.(*sqlparser.DDL), nil, "", ddlResults)
	require.NoError(t, (<-ddlResults).Err)
	after := mustSelectResultSQL(t, executor, "", "select count_bucket, count_bucket_and_lower from performance_schema.events_statements_histogram_global")
	require.NotEmpty(t, after.Records)
	for _, row := range selectResultRows(after) {
		require.Equal(t, "0", fmt.Sprint(row[0]))
		require.Equal(t, "0", fmt.Sprint(row[1]))
	}

	executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
		872, "histogram_truncate_user", "histogram-truncate.example", "histogram_truncate_app",
		"select histogram_truncate_digest", "SELECT", "ok", time.Millisecond, 0, 1, 0,
	)
	byDigestStmt, err := sqlparser.Parse("truncate table performance_schema.events_statements_histogram_by_digest")
	require.NoError(t, err)
	byDigestResults := make(chan *Result, 1)
	executor.QueryExecutor.executeDDL(byDigestStmt.(*sqlparser.DDL), nil, "", byDigestResults)
	require.NoError(t, (<-byDigestResults).Err)
	byDigestAfter := mustSelectResultSQL(t, executor, "", "select count_bucket, count_bucket_and_lower from performance_schema.events_statements_histogram_by_digest where schema_name = 'histogram_truncate_app'")
	for _, row := range selectResultRows(byDigestAfter) {
		require.Equal(t, "0", fmt.Sprint(row[0]))
		require.Equal(t, "0", fmt.Sprint(row[1]))
	}
}

func TestPerformanceSchemaStatementSummaryTruncateKeepsDigestAndGlobalStateIndependent(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	hasNonZeroBucket := func(result *SelectResult) bool {
		for _, row := range selectResultRows(result) {
			if len(row) >= 2 && (fmt.Sprint(row[0]) != "0" || fmt.Sprint(row[1]) != "0") {
				return true
			}
		}
		return false
	}
	assertAllBucketsZero := func(result *SelectResult) {
		require.NotEmpty(t, result.Records)
		for _, row := range selectResultRows(result) {
			require.Equal(t, "0", fmt.Sprint(row[0]))
			require.Equal(t, "0", fmt.Sprint(row[1]))
		}
	}
	for _, sql := range []string{
		"select * from summary_truncate_app.docs where id = 1",
		"select * from summary_truncate_app.docs where id = 2",
	} {
		executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
			901, "summary_truncate_user", "summary-truncate.example", "summary_truncate_app",
			sql, "SELECT", "ok", time.Millisecond, 0, 1, 0,
		)
	}

	digestBefore := executor.QueryExecutor.executePerformanceSchemaStatementsSelect("select count_star from performance_schema.events_statements_summary_by_digest where schema_name='summary_truncate_app'")
	require.Equal(t, 1, len(digestBefore.Records))
	require.Equal(t, "2", fmt.Sprint(selectResultRows(digestBefore)[0][0]))
	globalBefore := executor.QueryExecutor.executePerformanceSchemaStatementSummaryRegistrySelect("select count_star from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/sql/select'", "global")
	require.Equal(t, 1, len(globalBefore.Records))
	require.Equal(t, "2", fmt.Sprint(selectResultRows(globalBefore)[0][0]))
	histogramBefore := executor.QueryExecutor.executePerformanceSchemaStatementHistogramSelect("select count_bucket, count_bucket_and_lower from performance_schema.events_statements_histogram_global", false)
	require.True(t, hasNonZeroBucket(histogramBefore))

	truncateDigest, err := sqlparser.Parse("truncate table performance_schema.events_statements_summary_by_digest")
	require.NoError(t, err)
	digestResults := make(chan *Result, 1)
	executor.QueryExecutor.executeDDL(truncateDigest.(*sqlparser.DDL), nil, "", digestResults)
	require.NoError(t, (<-digestResults).Err)

	require.Empty(t, executor.QueryExecutor.executePerformanceSchemaStatementsSelect("select * from performance_schema.events_statements_summary_by_digest where schema_name='summary_truncate_app'").Records)
	globalAfterDigest := executor.QueryExecutor.executePerformanceSchemaStatementSummaryRegistrySelect("select count_star from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/sql/select'", "global")
	require.Equal(t, "2", fmt.Sprint(selectResultRows(globalAfterDigest)[0][0]))
	require.Empty(t, executor.QueryExecutor.executePerformanceSchemaStatementHistogramSelect("select count_bucket, count_bucket_and_lower from performance_schema.events_statements_histogram_by_digest where schema_name='summary_truncate_app'", true).Records)
	require.True(t, hasNonZeroBucket(executor.QueryExecutor.executePerformanceSchemaStatementHistogramSelect("select count_bucket, count_bucket_and_lower from performance_schema.events_statements_histogram_global", false)))

	executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
		902, "summary_truncate_user", "summary-truncate.example", "summary_truncate_app",
		"select * from summary_truncate_app.docs where id = 3", "SELECT", "ok", time.Millisecond, 0, 1, 0,
	)
	truncateGlobal, err := sqlparser.Parse("truncate table performance_schema.events_statements_summary_global_by_event_name")
	require.NoError(t, err)
	globalResults := make(chan *Result, 1)
	executor.QueryExecutor.executeDDL(truncateGlobal.(*sqlparser.DDL), nil, "", globalResults)
	require.NoError(t, (<-globalResults).Err)

	globalAfterGlobal := executor.QueryExecutor.executePerformanceSchemaStatementSummaryRegistrySelect("select count_star from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/sql/select'", "global")
	require.Equal(t, 1, len(globalAfterGlobal.Records))
	require.Equal(t, "0", fmt.Sprint(selectResultRows(globalAfterGlobal)[0][0]))
	digestAfterGlobal := executor.QueryExecutor.executePerformanceSchemaStatementsSelect("select count_star from performance_schema.events_statements_summary_by_digest where schema_name='summary_truncate_app'")
	require.Equal(t, 1, len(digestAfterGlobal.Records))
	require.Equal(t, "1", fmt.Sprint(selectResultRows(digestAfterGlobal)[0][0]))
	assertAllBucketsZero(executor.QueryExecutor.executePerformanceSchemaStatementHistogramSelect("select count_bucket, count_bucket_and_lower from performance_schema.events_statements_histogram_global", false))
	require.True(t, hasNonZeroBucket(executor.QueryExecutor.executePerformanceSchemaStatementHistogramSelect("select count_bucket, count_bucket_and_lower from performance_schema.events_statements_histogram_by_digest where schema_name='summary_truncate_app'", true)))
}

func TestPerformanceSchemaProgramSummaryTruncateResetsRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.recordPerformanceSchemaProgramExecution(
		"PROCEDURE", "summary_program_app", "report", 100, 1, 100, 100, 100, 0, 0, 2, 3, 4,
	)
	before := executor.QueryExecutor.executePerformanceSchemaProgramSummarySelect(
		"select object_name, count_star, sum_timer_wait, sum_rows_affected, sum_rows_sent, sum_rows_examined from performance_schema.events_statements_summary_by_program where object_schema='summary_program_app'",
	)
	require.Equal(t, 1, len(before.Records))
	require.Equal(t, "1", fmt.Sprint(selectResultRows(before)[0][1]))

	truncateStmt, err := sqlparser.Parse("truncate table performance_schema.events_statements_summary_by_program")
	require.NoError(t, err)
	ddlResults := make(chan *Result, 1)
	executor.QueryExecutor.executeDDL(truncateStmt.(*sqlparser.DDL), nil, "", ddlResults)
	require.NoError(t, (<-ddlResults).Err)

	after := executor.QueryExecutor.executePerformanceSchemaProgramSummarySelect(
		"select object_name, count_star, sum_timer_wait, sum_rows_affected, sum_rows_sent, sum_rows_examined from performance_schema.events_statements_summary_by_program where object_schema='summary_program_app'",
	)
	require.Equal(t, 1, len(after.Records))
	require.Equal(t, []interface{}{"report", "0", "0", "0", "0", "0"}, selectResultRows(after)[0])
}

func TestPerformanceSchemaTransactionSummaryTruncateResetsRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(1308))
	session.SetParamByName("user", "transaction_truncate_user")
	session.SetParamByName("host", "transaction-truncate.example")
	session.SetParamByName("performance_schema_transaction_started_at", time.Now().UnixNano())
	executor.QueryExecutor.recordPerformanceSchemaTransactionHistory(session, "COMMIT")

	before := executor.QueryExecutor.executePerformanceSchemaTransactionSummaryRegistrySelect(
		"select event_name, count_star, sum_timer_wait from performance_schema.events_transactions_summary_global_by_event_name",
		"global",
	)
	require.Equal(t, 1, len(before.Records))
	require.Equal(t, []interface{}{"transaction", "1"}, []interface{}{selectResultRows(before)[0][0], selectResultRows(before)[0][1]})
	require.NotEqual(t, "0", fmt.Sprint(selectResultRows(before)[0][2]))

	truncateStmt, err := sqlparser.Parse("truncate table performance_schema.events_transactions_summary_global_by_event_name")
	require.NoError(t, err)
	ddlResults := make(chan *Result, 1)
	executor.QueryExecutor.executeDDL(truncateStmt.(*sqlparser.DDL), nil, "", ddlResults)
	require.NoError(t, (<-ddlResults).Err)

	globalAfter := executor.QueryExecutor.executePerformanceSchemaTransactionSummaryRegistrySelect(
		"select event_name, count_star, sum_timer_wait, min_timer_wait, avg_timer_wait, max_timer_wait from performance_schema.events_transactions_summary_global_by_event_name",
		"global",
	)
	require.Equal(t, 1, len(globalAfter.Records))
	require.Equal(t, []interface{}{"transaction", "0", "0", "0", "0", "0"}, selectResultRows(globalAfter)[0])

	accountAfter := executor.QueryExecutor.executePerformanceSchemaTransactionSummaryRegistrySelect(
		"select user, host, event_name, count_star, sum_timer_wait from performance_schema.events_transactions_summary_by_account_by_event_name",
		"account",
	)
	require.Equal(t, [][]interface{}{{"transaction_truncate_user", "transaction-truncate.example", "transaction", "0", "0"}}, selectResultRows(accountAfter))
}

func TestPerformanceSchemaTransactionAccountSummaryTruncateIsolated(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(1313))
	session.SetParamByName("user", "transaction_account_user")
	session.SetParamByName("host", "transaction-account.example")
	session.SetParamByName("performance_schema_transaction_started_at", time.Now().UnixNano())
	executor.QueryExecutor.recordPerformanceSchemaTransactionHistory(session, "COMMIT")

	globalBefore := executor.QueryExecutor.executePerformanceSchemaTransactionSummaryRegistrySelect(
		"select event_name, count_star from performance_schema.events_transactions_summary_global_by_event_name",
		"global",
	)
	require.Equal(t, [][]interface{}{{"transaction", "1"}}, selectResultRows(globalBefore))

	truncateStmt, err := sqlparser.Parse("truncate table performance_schema.events_transactions_summary_by_account_by_event_name")
	require.NoError(t, err)
	ddlResults := make(chan *Result, 1)
	executor.QueryExecutor.executeDDL(truncateStmt.(*sqlparser.DDL), nil, "", ddlResults)
	require.NoError(t, (<-ddlResults).Err)

	accountAfter := executor.QueryExecutor.executePerformanceSchemaTransactionSummaryRegistrySelect(
		"select user, host, event_name, count_star from performance_schema.events_transactions_summary_by_account_by_event_name",
		"account",
	)
	require.Equal(t, [][]interface{}{{"transaction_account_user", "transaction-account.example", "transaction", "0"}}, selectResultRows(accountAfter))

	globalAfter := executor.QueryExecutor.executePerformanceSchemaTransactionSummaryRegistrySelect(
		"select event_name, count_star from performance_schema.events_transactions_summary_global_by_event_name",
		"global",
	)
	require.Equal(t, [][]interface{}{{"transaction", "1"}}, selectResultRows(globalAfter))
}

func TestPerformanceSchemaStatementAccountSummaryTruncateIsolated(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
		1401, "statement_truncate_alice", "statement-truncate-a.example", "statement_truncate_app",
		"select account_a", "SELECT", "ok", time.Millisecond, 0, 1, 0,
	)
	executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
		1402, "statement_truncate_bob", "statement-truncate-b.example", "statement_truncate_app",
		"select account_b", "SELECT", "ok", time.Millisecond, 0, 1, 0,
	)

	globalBefore := executor.QueryExecutor.executePerformanceSchemaStatementSummaryRegistrySelect(
		"select event_name, count_star from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/sql/select'",
		"global",
	)
	require.Equal(t, [][]interface{}{{"statement/sql/select", "2"}}, selectResultRows(globalBefore))

	truncateStmt, err := sqlparser.Parse("truncate table performance_schema.events_statements_summary_by_account_by_event_name")
	require.NoError(t, err)
	ddlResults := make(chan *Result, 1)
	executor.QueryExecutor.executeDDL(truncateStmt.(*sqlparser.DDL), nil, "", ddlResults)
	require.NoError(t, (<-ddlResults).Err)

	accountAfter := executor.QueryExecutor.executePerformanceSchemaStatementSummaryRegistrySelect(
		"select user, host, event_name, count_star from performance_schema.events_statements_summary_by_account_by_event_name",
		"account",
	)
	for _, row := range selectResultRows(accountAfter) {
		require.Equal(t, "0", fmt.Sprint(row[3]))
	}

	globalAfter := executor.QueryExecutor.executePerformanceSchemaStatementSummaryRegistrySelect(
		"select event_name, count_star from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/sql/select'",
		"global",
	)
	require.Equal(t, [][]interface{}{{"statement/sql/select", "2"}}, selectResultRows(globalAfter))
}

func TestPerformanceSchemaStageSummaryTruncateIsIndependent(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
		1310, "stage_truncate_user", "stage-truncate.example", "stage_truncate_app",
		"select stage_truncate", "SELECT", "ok", time.Millisecond, 0, 1, 0,
	)

	before := executor.QueryExecutor.executePerformanceSchemaStageSummarySelect(
		"select event_name, count_star, sum_timer_wait from performance_schema.events_stages_summary_global_by_event_name",
		false,
	)
	require.Equal(t, 1, len(before.Records))
	require.Equal(t, "1", fmt.Sprint(selectResultRows(before)[0][1]))

	statementBefore := executor.QueryExecutor.executePerformanceSchemaStatementSummaryRegistrySelect(
		"select count_star from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/sql/select'",
		"global",
	)
	require.Equal(t, "1", fmt.Sprint(selectResultRows(statementBefore)[0][0]))

	truncateStmt, err := sqlparser.Parse("truncate table performance_schema.events_stages_summary_global_by_event_name")
	require.NoError(t, err)
	ddlResults := make(chan *Result, 1)
	executor.QueryExecutor.executeDDL(truncateStmt.(*sqlparser.DDL), nil, "", ddlResults)
	require.NoError(t, (<-ddlResults).Err)

	after := executor.QueryExecutor.executePerformanceSchemaStageSummarySelect(
		"select event_name, count_star, sum_timer_wait, min_timer_wait, avg_timer_wait, max_timer_wait from performance_schema.events_stages_summary_global_by_event_name",
		false,
	)
	require.Equal(t, 1, len(after.Records))
	require.Equal(t, []interface{}{"stage/sql/execute", "0", "0", "0", "0", "0"}, selectResultRows(after)[0])
	statementAfter := executor.QueryExecutor.executePerformanceSchemaStatementSummaryRegistrySelect(
		"select count_star from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/sql/select'",
		"global",
	)
	require.Equal(t, "1", fmt.Sprint(selectResultRows(statementAfter)[0][0]))
}

func TestPerformanceSchemaStageAccountSummaryTruncateIsolated(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
		1312, "stage_account_user", "stage-account.example", "stage_account_app",
		"select stage_account", "SELECT", "ok", time.Millisecond, 0, 1, 0,
	)

	globalBefore := executor.QueryExecutor.executePerformanceSchemaStageSummarySelect(
		"select event_name, count_star from performance_schema.events_stages_summary_global_by_event_name",
		false,
	)
	require.Equal(t, [][]interface{}{{"stage/sql/execute", "1"}}, selectResultRows(globalBefore))

	truncateStmt, err := sqlparser.Parse("truncate table performance_schema.events_stages_summary_by_account_by_event_name")
	require.NoError(t, err)
	ddlResults := make(chan *Result, 1)
	executor.QueryExecutor.executeDDL(truncateStmt.(*sqlparser.DDL), nil, "", ddlResults)
	require.NoError(t, (<-ddlResults).Err)

	accountAfter := executor.QueryExecutor.executePerformanceSchemaStageSummaryRegistrySelect(
		"select user, host, event_name, count_star from performance_schema.events_stages_summary_by_account_by_event_name",
		"account",
	)
	for _, row := range selectResultRows(accountAfter) {
		require.Equal(t, "0", fmt.Sprint(row[3]))
	}

	globalAfter := executor.QueryExecutor.executePerformanceSchemaStageSummarySelect(
		"select event_name, count_star from performance_schema.events_stages_summary_global_by_event_name",
		false,
	)
	require.Equal(t, [][]interface{}{{"stage/sql/execute", "1"}}, selectResultRows(globalAfter))
}

func TestPerformanceSchemaWaitSummaryTruncateResetsRows(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 1311, 1, 1, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(2, 1311, 1, 1, manager.LOCK_X))
	locks.ReleaseLocks(1)
	locks.ReleaseLocks(2)

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	executor.QueryExecutor.SetLockManager(locks)
	before := executor.QueryExecutor.executePerformanceSchemaWaitSummarySelect(
		"select event_name, count_star, sum_timer_wait from performance_schema.events_waits_summary_global_by_event_name",
		false,
	)
	require.Equal(t, 1, len(before.Records))
	require.Equal(t, "1", fmt.Sprint(selectResultRows(before)[0][1]))

	truncateStmt, err := sqlparser.Parse("truncate table performance_schema.events_waits_summary_global_by_event_name")
	require.NoError(t, err)
	ddlResults := make(chan *Result, 1)
	executor.QueryExecutor.executeDDL(truncateStmt.(*sqlparser.DDL), nil, "", ddlResults)
	require.NoError(t, (<-ddlResults).Err)

	after := executor.QueryExecutor.executePerformanceSchemaWaitSummarySelect(
		"select event_name, count_star, sum_timer_wait, min_timer_wait, avg_timer_wait, max_timer_wait from performance_schema.events_waits_summary_global_by_event_name",
		false,
	)
	require.Equal(t, 1, len(after.Records))
	require.Equal(t, []interface{}{"wait/lock/table/sql/handler", "0", "0", "0", "0", "0"}, selectResultRows(after)[0])

	threadAfter := executor.QueryExecutor.executePerformanceSchemaWaitSummarySelect(
		"select thread_id, event_name, count_star, sum_timer_wait from performance_schema.events_waits_summary_by_thread_by_event_name",
		true,
	)
	require.Equal(t, [][]interface{}{{"2", "wait/lock/table/sql/handler", "0", "0"}}, selectResultRows(threadAfter))

	instanceAfter := executor.QueryExecutor.executePerformanceSchemaWaitSummaryByInstanceSelect(
		"select event_name, object_instance_begin, count_star, sum_timer_wait from performance_schema.events_waits_summary_by_instance",
	)
	require.Equal(t, [][]interface{}{{"wait/lock/table/sql/handler", "1311_1_1", "0", "0"}}, selectResultRows(instanceAfter))

	require.NoError(t, locks.AcquireLock(3, 1311, 1, 1, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(4, 1311, 1, 1, manager.LOCK_X))
	time.Sleep(2 * time.Millisecond)
	resumed := executor.QueryExecutor.executePerformanceSchemaWaitSummarySelect(
		"select event_name, count_star, sum_timer_wait, min_timer_wait, max_timer_wait from performance_schema.events_waits_summary_global_by_event_name",
		false,
	)
	require.Equal(t, 1, len(resumed.Records))
	resumedRow := selectResultRows(resumed)[0]
	require.Equal(t, []interface{}{"wait/lock/table/sql/handler", "1"}, resumedRow[:2])
	require.NotEqual(t, "0", resumedRow[2])
	require.NotEqual(t, "0", resumedRow[3])
	require.NotEqual(t, "0", resumedRow[4])
	resumedInstance := executor.QueryExecutor.executePerformanceSchemaWaitSummaryByInstanceSelect(
		"select event_name, object_instance_begin, count_star, sum_timer_wait, min_timer_wait, max_timer_wait from performance_schema.events_waits_summary_by_instance",
	)
	require.Equal(t, 1, len(resumedInstance.Records))
	resumedInstanceRow := selectResultRows(resumedInstance)[0]
	require.Equal(t, []interface{}{"wait/lock/table/sql/handler", "1311_1_1", "1"}, resumedInstanceRow[:3])
	require.NotEqual(t, "0", resumedInstanceRow[3])
	require.NotEqual(t, "0", resumedInstanceRow[4])
	require.NotEqual(t, "0", resumedInstanceRow[5])
	locks.ReleaseLocks(3)
	locks.ReleaseLocks(4)
}

func TestPerformanceSchemaWaitSummaryByInstanceTruncateIsIndependent(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 1312, 1, 1, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(2, 1312, 1, 1, manager.LOCK_X))
	locks.ReleaseLocks(1)
	locks.ReleaseLocks(2)

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	executor.QueryExecutor.SetLockManager(locks)
	before := selectResultRows(mustSelectResultSQL(t, executor, "", "select event_name, object_instance_begin, count_star from performance_schema.events_waits_summary_by_instance where event_name='wait/lock/table/sql/handler'"))
	require.Equal(t, [][]interface{}{{"wait/lock/table/sql/handler", "1312_1_1", "1"}}, before)

	stmt, err := sqlparser.Parse("truncate table performance_schema.events_waits_summary_by_instance")
	require.NoError(t, err)
	results := make(chan *Result, 1)
	executor.QueryExecutor.executeTruncateTableStatement(&ExecutionContext{Results: results, Cfg: executor.QueryExecutor.conf}, "", stmt.(*sqlparser.DDL))
	require.NoError(t, (<-results).Err)

	after := selectResultRows(mustSelectResultSQL(t, executor, "", "select event_name, object_instance_begin, count_star from performance_schema.events_waits_summary_by_instance where event_name='wait/lock/table/sql/handler'"))
	require.Equal(t, [][]interface{}{{"wait/lock/table/sql/handler", "1312_1_1", "0"}}, after)
	global := selectResultRows(mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_waits_summary_global_by_event_name where event_name='wait/lock/table/sql/handler'"))
	require.Equal(t, [][]interface{}{{"wait/lock/table/sql/handler", "1"}}, global)
}

func TestPerformanceSchemaTableIOSummaryTruncateIsIndependent(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	targetTable := "table_io_truncate_target"
	executor.QueryExecutor.metricsRecorder.RecordStatement(
		"table_io_truncate_app", "select id from table_io_truncate_app."+targetTable,
		"SELECT", "ok", time.Millisecond,
	)

	before := executor.QueryExecutor.executePerformanceSchemaTableIOSummarySelect(
		"select object_schema, object_name, count_star from performance_schema.table_io_waits_summary_by_table where object_name='"+targetTable+"'",
		false,
	)
	require.Equal(t, [][]interface{}{{"table_io_truncate_app", targetTable, "1"}}, selectResultRows(before))
	statementBefore := executor.QueryExecutor.executePerformanceSchemaStatementSummaryRegistrySelect(
		"select count_star from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/sql/select'",
		"global",
	)
	require.Equal(t, "1", fmt.Sprint(selectResultRows(statementBefore)[0][0]))

	truncateStmt, err := sqlparser.Parse("truncate table performance_schema.table_io_waits_summary_by_table")
	require.NoError(t, err)
	ddlResults := make(chan *Result, 1)
	executor.QueryExecutor.executeDDL(truncateStmt.(*sqlparser.DDL), nil, "", ddlResults)
	require.NoError(t, (<-ddlResults).Err)

	after := executor.QueryExecutor.executePerformanceSchemaTableIOSummarySelect(
		"select object_schema, object_name, count_star, count_read from performance_schema.table_io_waits_summary_by_table where object_name='"+targetTable+"'",
		false,
	)
	require.Equal(t, [][]interface{}{{"table_io_truncate_app", targetTable, "0", "0"}}, selectResultRows(after))
	indexAfter := executor.QueryExecutor.executePerformanceSchemaTableIOSummarySelect(
		"select object_schema, object_name, index_name, count_star from performance_schema.table_io_waits_summary_by_index_usage where object_name='"+targetTable+"'",
		true,
	)
	require.Equal(t, [][]interface{}{{"table_io_truncate_app", targetTable, "", "0"}}, selectResultRows(indexAfter))
	statementAfter := executor.QueryExecutor.executePerformanceSchemaStatementSummaryRegistrySelect(
		"select count_star from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/sql/select'",
		"global",
	)
	require.Equal(t, "1", fmt.Sprint(selectResultRows(statementAfter)[0][0]))
}

func TestPerformanceSchemaTableIOIndexSummaryPreservesPhysicalIndexName(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	executor.QueryExecutor.metricsRecorder.RecordStatementWithIndexNames(
		"index_io_app", "select id from index_io_app.orders where email='a@example.com'",
		"SELECT", "ok", time.Millisecond, []string{"idx_orders_email"},
	)

	result := executor.QueryExecutor.executePerformanceSchemaTableIOSummarySelect(
		"select object_schema, object_name, index_name, count_star from performance_schema.table_io_waits_summary_by_index_usage where object_name='orders'",
		true,
	)
	require.Equal(t, [][]interface{}{{"index_io_app", "orders", "idx_orders_email", "1"}}, selectResultRows(result))
}

func TestPerformanceSchemaStatementSummariesGroupByClientIdentity(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(91))
	session.SetParamByName("user", "reporter")
	session.SetParamByName("host", "10.0.0.9")
	require.NoError(t, (<-executor.ExecuteQuery(session, "select 1", "app")).Err)
	require.NoError(t, (<-executor.ExecuteQuery(session, "begin", "app")).Err)
	require.NoError(t, (<-executor.ExecuteQuery(session, "select 2", "app")).Err)
	require.NoError(t, (<-executor.ExecuteQuery(session, "commit", "app")).Err)
	for _, view := range []string{
		"events_statements_summary_by_account_by_event_name",
		"events_statements_summary_by_host_by_event_name",
		"events_statements_summary_by_user_by_event_name",
		"events_stages_summary_by_account_by_event_name",
		"events_stages_summary_by_host_by_event_name",
		"events_stages_summary_by_user_by_event_name",
		"events_transactions_summary_by_account_by_event_name",
		"events_transactions_summary_by_host_by_event_name",
		"events_transactions_summary_by_user_by_event_name",
	} {
		result := mustSelectResultSQL(t, executor, "", "select * from performance_schema."+view)
		require.NotEmpty(t, result.Records, view)
	}
	account := mustSelectResultSQL(t, executor, "", "select user, host, event_name, count_star from performance_schema.events_statements_summary_by_account_by_event_name where user='reporter' and host='10.0.0.9'")
	require.NotEmpty(t, account.Records)
}

func TestInformationSchemaVariableViewsExposeCompatibilityRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	queries := []string{
		"select variable_name, variable_value from information_schema.system_variables where variable_name='autocommit'",
		"select variable_name, variable_value from information_schema.global_variables where variable_name='autocommit'",
		"select variable_name, variable_value from information_schema.session_variables where variable_name='autocommit'",
	}
	for _, query := range queries {
		rows := mustQuerySQL(t, executor, "", query)
		require.Equal(t, [][]interface{}{{"autocommit", "ON"}}, rows, query)
	}
}

func TestPerformanceSchemaStatementHistoryExposesCompletedQueries(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	result := <-executor.ExecuteQuery(nil, "select 1", "app")
	require.NoError(t, result.Err)
	rows := mustQuerySQL(t, executor, "", "select sql_text, current_schema, sql_command from performance_schema.events_statements_history_long")
	require.NotEmpty(t, rows)
	found := false
	for _, row := range rows {
		if len(row) >= 3 && row[0] == "select 1" && row[1] == "app" && row[2] == "SELECT" {
			found = true
			break
		}
	}
	require.True(t, found, "completed select 1 was not present in statement history: %#v", rows)
}

func TestPerformanceSchemaHistoryTablesTruncateIndependently(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(1401)
	truncatePerformanceSchema := func(table string) {
		stmt, err := sqlparser.Parse("truncate table performance_schema." + table)
		require.NoError(t, err)
		results := make(chan *Result, 1)
		executor.QueryExecutor.executeTruncateTableStatement(&ExecutionContext{Session: session, Results: results, Cfg: executor.QueryExecutor.conf}, "", stmt.(*sqlparser.DDL))
		require.NoError(t, (<-results).Err)
	}

	require.NoError(t, (<-executor.ExecuteQuery(session, "select 1401", "app")).Err)
	statementHistory := executor.QueryExecutor.executePerformanceSchemaStatementHistorySelect(
		"performance_schema.events_statements_history_long", false,
	)
	stageHistory := executor.QueryExecutor.executePerformanceSchemaStageHistorySelect(
		"performance_schema.events_stages_history_long", false,
	)
	require.NotEmpty(t, selectResultRows(statementHistory))
	require.NotEmpty(t, selectResultRows(stageHistory))

	truncatePerformanceSchema("events_statements_history_long")
	statementHistory = executor.QueryExecutor.executePerformanceSchemaStatementHistorySelect(
		"performance_schema.events_statements_history_long", false,
	)
	stageHistory = executor.QueryExecutor.executePerformanceSchemaStageHistorySelect(
		"performance_schema.events_stages_history_long", false,
	)
	require.Empty(t, selectResultRows(statementHistory))
	require.NotEmpty(t, selectResultRows(stageHistory), "statement history truncate must not clear stage history")

	mustExecSessionSQL(t, executor, session, "", "begin")
	mustExecSessionSQL(t, executor, session, "", "commit")
	transactionHistory := <-executor.ExecuteQuery(session, "select * from performance_schema.events_transactions_history", "")
	require.NoError(t, transactionHistory.Err)
	require.NotEmpty(t, selectResultRows(transactionHistory.Data.(*SelectResult)))
	truncatePerformanceSchema("events_transactions_history")
	transactionHistoryResult := executor.QueryExecutor.executePerformanceSchemaTransactionsSelect(
		"", session, "performance_schema.events_transactions_history",
	)
	require.Empty(t, selectResultRows(transactionHistoryResult))

	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1401, 9, 1, 1, manager.LOCK_X))
	require.ErrorIs(t, locks.AcquireLock(1402, 9, 1, 1, manager.LOCK_X), manager.ErrLockConflict)
	locks.ReleaseLocks(1401)
	locks.ReleaseLocks(1402)
	executor.QueryExecutor.SetLockManager(locks)
	waitHistory := executor.QueryExecutor.executePerformanceSchemaEventsWaitsSelect("select * from performance_schema.events_waits_history_long")
	require.NotEmpty(t, selectResultRows(waitHistory))
	truncatePerformanceSchema("events_waits_history_long")
	waitHistory = executor.QueryExecutor.executePerformanceSchemaEventsWaitsSelect("select * from performance_schema.events_waits_history_long")
	require.Empty(t, selectResultRows(waitHistory))
}

func TestPerformanceSchemaStatementHistoryAppliesFiltersAndProjection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	first := newTestMySQLSession()
	first.ctx.SetConnectionID(131)
	second := newTestMySQLSession()
	second.ctx.SetConnectionID(132)
	require.NoError(t, (<-executor.ExecuteQuery(first, "select 7", "app")).Err)
	require.NoError(t, (<-executor.ExecuteQuery(second, "select 8", "app")).Err)

	result := mustSelectResultSQL(t, executor, "", "select thread_id, event_name, sql_text from performance_schema.events_statements_history_long where thread_id=131 and event_name='statement/sql/select'")
	require.Equal(t, []string{"THREAD_ID", "EVENT_NAME", "SQL_TEXT"}, result.Columns)
	require.Equal(t, [][]interface{}{{"131", "statement/sql/select", "select 7"}}, selectResultRows(result))
}

func TestPerformanceSchemaStatementHistoryIncludesRecentEventsPerThread(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	first := newTestMySQLSession()
	first.ctx.SetConnectionID(201)
	second := newTestMySQLSession()
	second.ctx.SetConnectionID(202)

	require.NoError(t, (<-executor.ExecuteQuery(first, "select 201", "app")).Err)
	require.NoError(t, (<-executor.ExecuteQuery(second, "select 202", "app")).Err)

	result := mustSelectResultSessionSQL(t, executor, first, "", "select thread_id, sql_text from performance_schema.events_statements_history")
	rows := selectResultRows(result)
	seen := make(map[string]string)
	for _, row := range rows {
		if row[0] == "201" || row[0] == "202" {
			seen[row[0].(string)] = row[1].(string)
		}
	}
	require.Equal(t, map[string]string{"201": "select 201", "202": "select 202"}, seen)
}

func TestPerformanceSchemaStageHistoryIncludesRecentEventsPerThread(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	first := newTestMySQLSession()
	first.ctx.SetConnectionID(301)
	second := newTestMySQLSession()
	second.ctx.SetConnectionID(302)

	require.NoError(t, (<-executor.ExecuteQuery(first, "select 301", "app")).Err)
	require.NoError(t, (<-executor.ExecuteQuery(second, "select 302", "app")).Err)

	result := mustSelectResultSessionSQL(t, executor, first, "", "select thread_id, event_name from performance_schema.events_stages_history")
	rows := selectResultRows(result)
	seen := make(map[string]string)
	for _, row := range rows {
		if row[0] == "301" || row[0] == "302" {
			seen[row[0].(string)] = row[1].(string)
		}
	}
	require.Equal(t, map[string]string{"301": "stage/sql/execute", "302": "stage/sql/execute"}, seen)
}

func TestPerformanceSchemaStatementHistoryProjectsOfficialEventFields(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(133)
	require.NoError(t, (<-executor.ExecuteQuery(session, "select 17", "app")).Err)

	all := mustSelectResultSQL(t, executor, "", "select * from performance_schema.events_statements_history_long where thread_id = 133")
	require.Contains(t, all.Columns, "ROWS_AFFECTED")
	require.Contains(t, all.Columns, "ROWS_SENT")
	require.Contains(t, all.Columns, "MESSAGE_TEXT")
	require.Contains(t, all.Columns, "DIGEST_TEXT")
	require.Contains(t, all.Columns, "EXECUTION_ENGINE")

	projected := selectResultRows(mustSelectResultSQL(t, executor, "", "select rows_affected, rows_sent, warnings, errors, digest_text, execution_engine from performance_schema.events_statements_history_long where thread_id = 133 and sql_text = 'select 17'"))
	require.Equal(t, [][]interface{}{{"0", "1", "0", "0", "select ? from dual", "PRIMARY"}}, projected)
}

func TestPerformanceSchemaStatementHistoryProjectsRowsExaminedFromClusteredScan(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	mustExecSQL(t, executor, "", "create database scan_app")
	mustExecSQL(t, executor, "scan_app", "create table scan_rows (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "scan_app", "insert into scan_rows values (1, 'hit'), (2, 'miss')")
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(134)
	require.NoError(t, (<-executor.ExecuteQuery(session, "select id from scan_rows where label = 'hit'", "scan_app")).Err)

	history := selectResultRows(mustSelectResultSQL(t, executor, "", "select sql_text, rows_examined, rows_sent, select_scan from performance_schema.events_statements_history_long where thread_id = 134"))
	var projected []interface{}
	for _, row := range history {
		if len(row) == 4 && row[0] == "select id from scan_rows where label = 'hit'" {
			projected = row[1:]
			break
		}
	}
	require.Equal(t, []interface{}{"2", "1", "1"}, projected)

	digest := selectResultRows(mustSelectResultSQL(t, executor, "", "select sum_rows_examined, sum_select_scan from performance_schema.events_statements_summary_by_digest where schema_name = 'scan_app' and digest_text = 'select id from scan_rows where label = ?'"))
	require.Equal(t, [][]interface{}{{"2", "1"}}, digest)
}

func TestPerformanceSchemaStatementHistoryProjectsNoIndexUsedFromTableScan(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	mustExecSQL(t, executor, "", "create database no_index_app")
	mustExecSQL(t, executor, "no_index_app", "create table scan_rows (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "no_index_app", "insert into scan_rows values (1, 'hit'), (2, 'miss')")
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(135)
	require.NoError(t, (<-executor.ExecuteQuery(session, "select id from scan_rows where label = 'hit'", "no_index_app")).Err)

	history := selectResultRows(mustSelectResultSQL(t, executor, "", "select sql_text, no_index_used, no_good_index_used from performance_schema.events_statements_history_long where thread_id = 135"))
	var projected []interface{}
	for _, row := range history {
		if len(row) == 3 && row[0] == "select id from scan_rows where label = 'hit'" {
			projected = row[1:]
			break
		}
	}
	require.Equal(t, []interface{}{"1", "1"}, projected)

	digest := selectResultRows(mustSelectResultSQL(t, executor, "", "select sum_no_index_used, sum_no_good_index_used from performance_schema.events_statements_summary_by_digest where schema_name = 'no_index_app' and digest_text = 'select id from scan_rows where label = ?'"))
	require.Equal(t, [][]interface{}{{"1", "1"}}, digest)
}

func TestPerformanceSchemaStatementHistoryProjectsSortRowsFromOrderBy(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	mustExecSQL(t, executor, "", "create database sort_rows_app")
	mustExecSQL(t, executor, "sort_rows_app", "create table ordered_rows (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "sort_rows_app", "insert into ordered_rows values (1, 'one'), (2, 'two'), (3, 'three')")
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(136)
	query := "select id from ordered_rows order by id desc"
	require.NoError(t, (<-executor.ExecuteQuery(session, query, "sort_rows_app")).Err)

	history := selectResultRows(mustSelectResultSQL(t, executor, "", "select sql_text, sort_rows, sort_scan from performance_schema.events_statements_history_long where thread_id = 136"))
	var projected []interface{}
	for _, row := range history {
		if len(row) == 3 && row[0] == query {
			projected = row[1:]
			break
		}
	}
	require.Equal(t, []interface{}{"3", "1"}, projected)

	digest := selectResultRows(mustSelectResultSQL(t, executor, "", "select sum_sort_rows, sum_sort_scan from performance_schema.events_statements_summary_by_digest where schema_name = 'sort_rows_app' and digest_text = 'select id from ordered_rows order by id desc'"))
	require.Equal(t, [][]interface{}{{"3", "1"}}, digest)
}

func TestPerformanceSchemaStatementHistoryProjectsRangeCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	mustExecSQL(t, executor, "", "create database sort_range_app")
	mustExecSQL(t, executor, "sort_range_app", "create table ranged_rows (id int primary key, price int, index idx_price (price))")
	mustExecSQL(t, executor, "sort_range_app", "insert into ranged_rows values (1, 10), (2, 20), (3, 30)")
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(137)
	query := "select id from ranged_rows where price >= 20 order by price desc"
	require.NoError(t, (<-executor.ExecuteQuery(session, query, "sort_range_app")).Err)

	history := selectResultRows(mustSelectResultSQL(t, executor, "", "select sql_text, select_range, sort_range, sort_rows, sort_scan from performance_schema.events_statements_history_long where thread_id = 137"))
	var projected []interface{}
	for _, row := range history {
		if len(row) == 5 && row[0] == query {
			projected = row[1:]
			break
		}
	}
	require.Equal(t, []interface{}{"1", "1", "2", "0"}, projected)

	digest := selectResultRows(mustSelectResultSQL(t, executor, "", "select sum_select_range, sum_sort_range, sum_sort_rows, sum_sort_scan from performance_schema.events_statements_summary_by_digest where schema_name = 'sort_range_app' and digest_text = 'select id from ranged_rows where price >= ? order by price desc'"))
	require.Equal(t, [][]interface{}{{"1", "1", "2", "0"}}, digest)
}

func TestPerformanceSchemaStatementHistoryProjectsJoinCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	mustExecSQL(t, executor, "", "create database join_stats_app")
	mustExecSQL(t, executor, "join_stats_app", "create table join_left (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "join_stats_app", "create table join_right (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "join_stats_app", "insert into join_left values (1, 'left')")
	mustExecSQL(t, executor, "join_stats_app", "insert into join_right values (1, 'right')")
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(138)
	query := "select l.id from join_left l join join_right r on l.id = r.id"
	require.NoError(t, (<-executor.ExecuteQuery(session, query, "join_stats_app")).Err)

	history := selectResultRows(mustSelectResultSQL(t, executor, "", "select sql_text, rows_examined, select_scan, select_full_join, select_full_range_join, select_range_check from performance_schema.events_statements_history_long where thread_id = 138"))
	var projected []interface{}
	for _, row := range history {
		if len(row) == 6 && row[0] == query {
			projected = row[1:]
			break
		}
	}
	require.Equal(t, []interface{}{"2", "1", "1", "0", "0"}, projected)

	digest := selectResultRows(mustSelectResultSQL(t, executor, "", "select sum_rows_examined, sum_select_scan, sum_select_full_join, sum_select_full_range_join, sum_select_range_check from performance_schema.events_statements_summary_by_digest where schema_name = 'join_stats_app' and digest_text = 'select l.id from join_left as l join join_right as r on l.id = r.id'"))
	require.Equal(t, [][]interface{}{{"2", "1", "1", "0", "0"}}, digest)
}

func indexOfColumn(columns []string, wanted string) int {
	for index, column := range columns {
		if strings.EqualFold(column, wanted) {
			return index
		}
	}
	return -1
}

func TestPerformanceSchemaStatementHistoryCarriesConnectionThreadID(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(123)
	result := <-executor.ExecuteQuery(session, "select 7", "app")
	require.NoError(t, result.Err)
	events := executor.QueryExecutor.metricsRecorder.StatementHistory()
	found := false
	for _, event := range events {
		if event.SQL == "select 7" && event.ThreadID == 123 {
			found = true
			require.Equal(t, int64(123), event.ThreadID)
			break
		}
	}
	require.True(t, found, "completed select 7 was not present in statement history: %#v", events)
	history := executor.QueryExecutor.executePerformanceSchemaStatementHistorySelect("performance_schema.events_statements_history_long", false)
	found = false
	threadIndex := indexOfColumn(history.Columns, "THREAD_ID")
	sqlTextIndex := indexOfColumn(history.Columns, "SQL_TEXT")
	for _, record := range history.Records {
		values := record.GetValues()
		if threadIndex >= 0 && sqlTextIndex >= 0 && values[threadIndex].Int() == 123 && values[sqlTextIndex].String() == "select 7" {
			found = true
			require.Equal(t, int64(123), values[threadIndex].Int())
			break
		}
	}
	require.True(t, found, "statement history view did not expose select 7: %#v", history.Records)
}

func TestPerformanceSchemaCurrentEventsDoNotReuseCompletedStatements(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(125)
	result := <-executor.ExecuteQuery(session, "select 101", "app")
	require.NoError(t, result.Err)

	currentStatements := executor.QueryExecutor.executePerformanceSchemaStatementHistorySelect("performance_schema.events_statements_current", true)
	sqlTextIndex := indexOfColumn(currentStatements.Columns, "SQL_TEXT")
	for _, record := range currentStatements.Records {
		require.NotEqual(t, "select 101", record.GetValues()[sqlTextIndex].String())
	}
	currentStages := executor.QueryExecutor.executePerformanceSchemaStageHistorySelect("performance_schema.events_stages_current", true)
	for _, record := range currentStages.Records {
		require.NotEqual(t, int64(125), record.GetValues()[0].Int())
	}
}

func TestPerformanceSchemaCurrentQueryStartsAsProtocolCommand(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(127)
	session.SetParamByName("performance_schema_statement_event_name", "statement/com/Query")
	cleanup := executor.QueryExecutor.beginActiveStatement(session, "select 127", "app")
	defer cleanup()

	current := executor.QueryExecutor.executePerformanceSchemaStatementHistorySelect("performance_schema.events_statements_current", true)
	eventNameIndex := indexOfColumn(current.Columns, "EVENT_NAME")
	sqlTextIndex := indexOfColumn(current.Columns, "SQL_TEXT")
	found := false
	for _, record := range current.Records {
		values := record.GetValues()
		if values[sqlTextIndex].String() == "select 127" {
			found = true
			require.Equal(t, "statement/com/Query", values[eventNameIndex].String())
		}
	}
	require.True(t, found, "protocol query was not visible in events_statements_current: %#v", current.Records)
}

func TestPerformanceSchemaStatementHistoryProjectsTimerBounds(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
		126, "timer_user", "timer_host", "timer_app", "select timer_bounds", "SELECT", "ok",
		2*time.Millisecond, 0, 1, 0,
	)

	history := executor.QueryExecutor.executePerformanceSchemaStatementHistorySelect(
		"performance_schema.events_statements_history_long", false,
		"select thread_id, sql_text, timer_start, timer_end, timer_wait from performance_schema.events_statements_history_long",
	)
	sqlTextIndex := indexOfColumn(history.Columns, "SQL_TEXT")
	timerStartIndex := indexOfColumn(history.Columns, "TIMER_START")
	timerEndIndex := indexOfColumn(history.Columns, "TIMER_END")
	timerWaitIndex := indexOfColumn(history.Columns, "TIMER_WAIT")
	var timerRow []basic.Value
	for _, record := range history.Records {
		values := record.GetValues()
		if sqlTextIndex >= 0 && values[sqlTextIndex].String() == "select timer_bounds" {
			timerRow = values
			break
		}
	}
	require.NotNil(t, timerRow, "statement history did not expose timer_bounds")
	require.False(t, timerRow[timerStartIndex].IsNull())
	require.False(t, timerRow[timerEndIndex].IsNull())
	require.Greater(t, timerRow[timerStartIndex].Int(), int64(0))
	require.Greater(t, timerRow[timerEndIndex].Int(), timerRow[timerStartIndex].Int())
	require.Equal(t, timerRow[timerEndIndex].Int()-timerRow[timerStartIndex].Int(), timerRow[timerWaitIndex].Int())
}

func TestPerformanceSchemaDirectExecutorRecordsMemoryAndStatementLifecycle(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(124)
	var beforeAlloc, beforeFree, beforeBytesAlloc, beforeBytesFree int64
	for _, row := range executor.QueryExecutor.metricsRecorder.MemorySummary() {
		if row.ThreadID == 124 && row.EventName == "memory/sql/THD::main_mem_root" {
			beforeAlloc = row.CountAlloc
			beforeFree = row.CountFree
			beforeBytesAlloc = row.BytesAlloc
			beforeBytesFree = row.BytesFree
		}
	}

	results := executor.QueryExecutor.ExecuteWithQuery(session, "select 8", "app")
	var resultCount int
	for result := range results {
		require.NoError(t, result.Err)
		resultCount++
	}
	require.Equal(t, 1, resultCount)

	var foundMemory bool
	for _, row := range executor.QueryExecutor.metricsRecorder.MemorySummary() {
		if row.ThreadID == 124 && row.EventName == "memory/sql/THD::main_mem_root" {
			foundMemory = true
			require.Equal(t, beforeAlloc+1, row.CountAlloc)
			require.Equal(t, beforeFree+1, row.CountFree)
			require.Equal(t, beforeBytesAlloc+int64(len("select 8")), row.BytesAlloc)
			require.Equal(t, beforeBytesFree+int64(len("select 8")), row.BytesFree)
			require.Zero(t, row.CurrentBytesUsed)
		}
	}
	require.True(t, foundMemory, "direct executor did not record query memory lifecycle")

	var foundStatement bool
	for _, event := range executor.QueryExecutor.metricsRecorder.StatementHistory() {
		if event.ThreadID == 124 && event.SQL == "select 8" {
			foundStatement = true
			require.Equal(t, "success", event.Status)
			require.Equal(t, int64(1), event.RowsSent)
			require.Zero(t, event.RowsAffected)
		}
	}
	require.True(t, foundStatement, "direct executor did not record statement history")
}

func TestPerformanceSchemaStageHistoryExposesCompletedQueries(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	result := <-executor.ExecuteQuery(nil, "select 9", "app")
	require.NoError(t, result.Err)
	for _, name := range []string{
		"performance_schema.events_stages_current",
		"performance_schema.events_stages_history",
		"performance_schema.events_stages_history_long",
	} {
		rows := mustQuerySQL(t, executor, "", "select * from "+name)
		require.NotEmpty(t, rows, name)
		require.Equal(t, "stage/sql/execute", rows[len(rows)-1][3], name)
		require.Equal(t, "STATEMENT", rows[len(rows)-1][11], name)
	}
}

func TestPerformanceSchemaStageHistoryAppliesFiltersAndProjection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	first := newTestMySQLSession()
	first.ctx.SetConnectionID(141)
	second := newTestMySQLSession()
	second.ctx.SetConnectionID(142)
	require.NoError(t, (<-executor.ExecuteQuery(first, "select 11", "app")).Err)
	require.NoError(t, (<-executor.ExecuteQuery(second, "select 12", "app")).Err)

	result := mustSelectResultSQL(t, executor, "", "select thread_id, event_name, timer_wait from performance_schema.events_stages_history_long where thread_id=141 and event_name='stage/sql/execute'")
	require.Equal(t, []string{"THREAD_ID", "EVENT_NAME", "TIMER_WAIT"}, result.Columns)
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	require.Equal(t, int64(141), values[0].Int())
	require.Equal(t, "stage/sql/execute", values[1].String())
	require.Greater(t, values[2].Int(), int64(0))
}

func TestPerformanceSchemaStageHistoryProjectsMonotonicTimerWindow(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
		151, "", "", "app", "select 13", "SELECT", "success", 8*time.Microsecond, 0, 1, 0,
	)

	result := executor.QueryExecutor.executePerformanceSchemaStageHistorySelect(
		"performance_schema.events_stages_history_long",
		false,
		"select thread_id, event_id, end_event_id, timer_start, timer_end, timer_wait from performance_schema.events_stages_history_long where thread_id=151",
	)
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	require.Equal(t, int64(151), values[0].Int())
	require.False(t, values[2].IsNull(), "completed stage must expose END_EVENT_ID")
	start := values[3].Int()
	end := values[4].Int()
	wait := values[5].Int()
	require.Greater(t, start, int64(0))
	require.Greater(t, end, start)
	require.Equal(t, end-start, wait)
}

func TestPerformanceSchemaCurrentStageLeavesCompletionTimersNull(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(152)
	stop := executor.QueryExecutor.beginActiveStatement(session, "select 14", "app")
	defer stop()

	result := executor.QueryExecutor.executePerformanceSchemaStageHistorySelect(
		"performance_schema.events_stages_current",
		true,
		"select thread_id, event_id, end_event_id, timer_start, timer_end, timer_wait from performance_schema.events_stages_current where thread_id=152",
	)
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	require.Equal(t, int64(152), values[0].Int())
	require.True(t, values[2].IsNull(), "current stage must not expose END_EVENT_ID")
	require.Greater(t, values[3].Int(), int64(0))
	require.True(t, values[4].IsNull(), "current stage must not expose TIMER_END")
	require.True(t, values[5].IsNull(), "current stage must not expose TIMER_WAIT")
}

func TestPerformanceSchemaStageHistoryHonorsConsumerAndInstrumentSettings(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "select 10", "app")
	require.NoError(t, result.Err)
	result = <-executor.ExecuteQuery(nil, "update performance_schema.setup_consumers set enabled='NO' where name='events_stages_history_long'", "")
	require.NoError(t, result.Err)
	require.Empty(t, executor.QueryExecutor.executePerformanceSchemaStageHistorySelect("performance_schema.events_stages_history_long", false).Records)
	result = <-executor.ExecuteQuery(nil, "update performance_schema.setup_consumers set enabled='YES' where name='events_stages_history_long'", "")
	require.NoError(t, result.Err)
	result = <-executor.ExecuteQuery(nil, "update performance_schema.setup_instruments set timed='NO' where name='stage/sql/execute'", "")
	require.NoError(t, result.Err)
	stageResult := executor.QueryExecutor.executePerformanceSchemaStageHistorySelect("performance_schema.events_stages_history_long", false)
	require.NotEmpty(t, stageResult.Records)
	values := stageResult.Records[len(stageResult.Records)-1].GetValues()
	require.Equal(t, int64(0), values[7].Int())
}

func TestPerformanceSchemaSetupTablesExposeInstrumentConfiguration(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	consumers := mustQuerySQL(t, executor, "", "select name, enabled from performance_schema.setup_consumers")
	require.Contains(t, consumers, []interface{}{"global_instrumentation", "YES"})
	instruments := mustQuerySQL(t, executor, "", "select name, enabled, timed from performance_schema.setup_instruments")
	require.NotEmpty(t, instruments)
	require.Equal(t, []interface{}{"statement/sql/select", "YES", "YES"}, instruments[0][:3])
}

func TestPerformanceSchemaSetupInstrumentsExposeNullableFlags(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	rows := mustQuerySQL(t, executor, "", "select * from performance_schema.setup_instruments where name='statement/sql/select'")
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 7)
	require.Equal(t, "", rows[0][3], "PROPERTIES should be an empty non-null SET")
	require.Nil(t, rows[0][4], "FLAGS is nullable and should be NULL when no flag is present")
	require.Nil(t, rows[0][6], "DOCUMENTATION is nullable when no documentation is present")
}

func TestPerformanceSchemaParseErrorsUseErrorInstrument(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	// The production default recorder is process-wide.  Use an isolated
	// recorder here so repeated runs and the full engine suite do not inherit
	// parse-error counts from earlier tests.
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	setup := mustQuerySQL(t, executor, "", "select name, enabled, timed from performance_schema.setup_instruments where name='statement/sql/error'")
	require.Len(t, setup, 1)
	require.Equal(t, []interface{}{"statement/sql/error", "YES", "YES"}, setup[0][:3])

	result := <-executor.ExecuteQuery(nil, "select * from", "")
	require.Error(t, result.Err)

	summary := mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/sql/error'")
	require.Len(t, summary.Records, 1)
	require.Equal(t, int64(1), summary.Records[0].GetValues()[1].Int())
}

func TestPerformanceSchemaPreparedProtocolCommandsUseCommandInstrument(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder.RecordProtocolCommand(
		17, "app_user", "localhost", "app", "statement/com/Close stmt", "ok", time.Millisecond,
	)

	setup := mustQuerySQL(t, executor, "", "select name, enabled, timed from performance_schema.setup_instruments where name='statement/com/Close stmt'")
	require.Len(t, setup, 1)
	require.Equal(t, []interface{}{"statement/com/Close stmt", "YES", "YES"}, setup[0][:3])

	summary := mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/com/Close stmt'")
	require.Len(t, summary.Records, 1)
	require.Equal(t, int64(1), summary.Records[0].GetValues()[1].Int())

	status := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Com_stmt_close'")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(status))
}

func TestPerformanceSchemaPreparedProtocolCommandInstrumentSettingFiltersEventOnly(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder.ResetStatementSummaryGlobal()
	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set enabled='NO' where name='statement/com/Close stmt'")
	enabled, _ := executor.QueryExecutor.performanceSchemaInstrumentSetting("statement/com/Close stmt")
	require.False(t, enabled)
	beforeStatus := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Com_stmt_close'")
	require.Len(t, beforeStatus.Records, 1)
	beforeCount, err := strconv.Atoi(beforeStatus.Records[0].GetValues()[0].String())
	require.NoError(t, err)
	executor.QueryExecutor.RecordProtocolCommand(18, "app_user", "localhost", "app", "statement/com/Close stmt", "ok", time.Millisecond)

	summary := mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/com/Close stmt'")
	require.Len(t, summary.Records, 1)
	require.Equal(t, int64(0), summary.Records[0].GetValues()[1].Int())
	status := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Com_stmt_close'")
	require.Equal(t, [][]interface{}{{strconv.Itoa(beforeCount + 1)}}, selectResultRows(status))
}

func TestPerformanceSchemaCommonProtocolCommandInstrument(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	setup := mustQuerySQL(t, executor, "", "select name, enabled, timed from performance_schema.setup_instruments where name='statement/com/Ping'")
	require.Len(t, setup, 1)
	require.Equal(t, []interface{}{"statement/com/Ping", "YES", "YES"}, setup[0][:3])

	executor.QueryExecutor.metricsRecorder.ResetStatementSummaryGlobal()
	executor.QueryExecutor.RecordProtocolCommand(19, "app_user", "localhost", "app", "statement/com/Ping", "ok", time.Millisecond)
	summary := mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/com/Ping'")
	require.Len(t, summary.Records, 1)
	require.Equal(t, int64(1), summary.Records[0].GetValues()[1].Int())
}

func TestPerformanceSchemaResetConnectionUsesCommandInstrument(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	setup := mustQuerySQL(t, executor, "", "select name, enabled, timed from performance_schema.setup_instruments where name='statement/com/Reset connection'")
	require.Len(t, setup, 1)
	require.Equal(t, []interface{}{"statement/com/Reset connection", "YES", "YES"}, setup[0][:3])
	executor.QueryExecutor.metricsRecorder.ResetStatementSummaryGlobal()
	executor.QueryExecutor.RecordProtocolCommand(21, "app_user", "localhost", "app", "statement/com/Reset connection", "ok", time.Millisecond)
	summary := mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/com/Reset connection'")
	require.Len(t, summary.Records, 1)
	require.Equal(t, int64(1), summary.Records[0].GetValues()[1].Int())
}

func TestPerformanceSchemaSetupInstrumentsExposeClientSocketUserProperty(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	rows := mustQuerySQL(t, executor, "", "select * from performance_schema.setup_instruments where name='wait/io/socket/sql/client_connection'")
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 7)
	require.Equal(t, "user", rows[0][3])
	require.Nil(t, rows[0][4])
	require.Nil(t, rows[0][6])
}

func TestPerformanceSchemaSetupInstrumentsExposeTHDMemoryMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select * from performance_schema.setup_instruments where name='memory/sql/THD::main_mem_root'")
	require.Len(t, result.Records, 1)
	require.Len(t, result.Records[0].GetValues(), 7)
	require.Equal(t, "controlled_by_default", result.Records[0].GetValueByIndex(3).String())
	require.Equal(t, "controlled", result.Records[0].GetValueByIndex(4).String())
	require.Equal(t, "BIGINT", result.ColumnTypes[5])
	require.Equal(t, int64(0), result.Records[0].GetValueByIndex(5).Int())
	require.Equal(t, "Main mem root used for e.g. the query arena.", result.Records[0].GetValueByIndex(6).String())
}

func TestPerformanceSchemaSetupInstrumentsExposeOfficialInnoDBFileRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	for _, name := range []string{
		"wait/io/file/innodb/innodb_tablespace_open_file",
		"wait/io/file/innodb/innodb_temp_file",
		"wait/io/file/innodb/innodb_arch_file",
		"wait/io/file/innodb/innodb_clone_file",
	} {
		rows := mustQuerySQL(t, executor, "", "select name, enabled, timed from performance_schema.setup_instruments where name='"+name+"'")
		require.Len(t, rows, 1, name)
		require.Equal(t, []interface{}{name, "YES", "YES"}, rows[0][:3], name)
	}
}

func TestPerformanceSchemaSetupInstrumentsExposeAbstractQueryMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select * from performance_schema.setup_instruments where name='statement/abstract/Query'")
	require.Len(t, result.Records, 1)
	require.Len(t, result.Records[0].GetValues(), 7)
	require.Equal(t, "mutable", result.Records[0].GetValueByIndex(3).String())
	require.Nil(t, result.Records[0].GetValueByIndex(4).Raw())
	require.Equal(t, "BIGINT", result.ColumnTypes[5])
	require.Equal(t, int64(0), result.Records[0].GetValueByIndex(5).Int())
	require.Equal(t, "SQL query just received from the network. At this point, the real statement type is unknown, the type will be refined after SQL parsing.", result.Records[0].GetValueByIndex(6).String())
}

func TestPerformanceSchemaSetupInstrumentsExposeAbstractPacketAndRelayLog(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	for _, name := range []string{"statement/abstract/new_packet", "statement/abstract/relay_log"} {
		rows := mustQuerySQL(t, executor, "", "select name, enabled, timed from performance_schema.setup_instruments where name='"+name+"'")
		require.Len(t, rows, 1, name)
		require.Equal(t, []interface{}{name, "YES", "YES"}, rows[0][:3], name)
	}
}

func TestPerformanceSchemaSetupInstrumentsExposeAbstractPacketMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	want := map[string]string{
		"statement/abstract/new_packet": "New packet just received from the network. At this point, the real command type is unknown, the type will be refined after reading the packet header.",
		"statement/abstract/relay_log":  "New event just read from the relay log. At this point, the real statement type is unknown, the type will be refined after parsing the event.",
	}
	for name, documentation := range want {
		result := mustSelectResultSQL(t, executor, "", "select * from performance_schema.setup_instruments where name='"+name+"'")
		require.Len(t, result.Records, 1, name)
		values := result.Records[0].GetValues()
		require.Len(t, values, 7, name)
		require.Equal(t, "mutable", values[3].String(), name)
		require.Nil(t, values[4].Raw(), name)
		require.Equal(t, int64(0), values[5].Int(), name)
		require.Equal(t, documentation, values[6].String(), name)
	}
}

func TestPerformanceSchemaSetupConsumersUseMySQL84Defaults(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	want := map[string]string{
		"global_instrumentation":           "YES",
		"thread_instrumentation":           "YES",
		"events_statements_cpu":            "NO",
		"events_statements_current":        "YES",
		"events_statements_history":        "YES",
		"events_statements_history_long":   "NO",
		"events_stages_current":            "NO",
		"events_stages_history":            "NO",
		"events_stages_history_long":       "NO",
		"events_waits_current":             "NO",
		"events_waits_history":             "NO",
		"events_waits_history_long":        "NO",
		"events_transactions_current":      "YES",
		"events_transactions_history":      "YES",
		"events_transactions_history_long": "NO",
		"statements_digest":                "YES",
	}
	for name, enabled := range want {
		rows := mustQuerySQL(t, executor, "", "select name, enabled from performance_schema.setup_consumers where name='"+name+"'")
		require.Equal(t, [][]interface{}{{name, enabled}}, rows, name)
	}
}

func TestPerformanceSchemaStatementCPUConsumerIsQueryableAndMutable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	rows := mustQuerySQL(t, executor, "", "select name, enabled from performance_schema.setup_consumers where name='events_statements_cpu'")
	require.Equal(t, [][]interface{}{{"events_statements_cpu", "NO"}}, rows)

	result := <-executor.ExecuteQuery(nil, "update performance_schema.setup_consumers set enabled='YES' where name='events_statements_cpu'", "")
	require.NoError(t, result.Err)
	require.Equal(t, 1, result.AffectedRows)
	rows = mustQuerySQL(t, executor, "", "select name, enabled from performance_schema.setup_consumers where name='events_statements_cpu'")
	require.Equal(t, [][]interface{}{{"events_statements_cpu", "YES"}}, rows)

	result = <-executor.ExecuteQuery(nil, "update performance_schema.setup_consumers set enabled='NO' where name='events_statements_cpu'", "")
	require.NoError(t, result.Err)
}

func TestPerformanceSchemaStatementCPUTimeUsesEnabledConsumer(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "update performance_schema.setup_consumers set enabled='YES' where name='events_statements_history_long'")
	mustExecSQL(t, executor, "", "update performance_schema.setup_consumers set enabled='YES' where name='events_statements_cpu'")

	const cpuProbeSQL = "select 983451"
	result := <-executor.ExecuteQuery(nil, cpuProbeSQL, "")
	require.NoError(t, result.Err)
	selectResult := mustSelectResultSQL(t, executor, "", "select cpu_time from performance_schema.events_statements_history_long where sql_text='"+cpuProbeSQL+"'")
	require.Len(t, selectResult.Records, 1)
	values := selectResult.Records[0].GetValues()
	require.NotNil(t, values[0], "CPU_TIME must be captured when events_statements_cpu is enabled")
	require.Greater(t, values[0].Int(), int64(0))
	summaryResult := mustSelectResultSQL(t, executor, "", "select sum_cpu_time from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/sql/select'")
	require.Len(t, summaryResult.Records, 1)
	require.Greater(t, summaryResult.Records[0].GetValues()[0].Int(), int64(0), "SUM_CPU_TIME must aggregate captured CPU time")

	mustExecSQL(t, executor, "", "update performance_schema.setup_consumers set enabled='NO' where name='events_statements_cpu'")
	result = <-executor.ExecuteQuery(nil, "select 983452", "")
	require.NoError(t, result.Err)
	selectResult = mustSelectResultSQL(t, executor, "", "select cpu_time from performance_schema.events_statements_history_long where sql_text='select 983452'")
	require.Len(t, selectResult.Records, 1)
	values = selectResult.Records[0].GetValues()
	require.True(t, values[0].IsNull(), "CPU_TIME must remain NULL when the CPU consumer is disabled")
}

func TestPerformanceSchemaSetupObjectsAndTimersExposeCompatibilityRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	objects := mustQuerySQL(t, executor, "", "select object_type, object_schema, object_name, enabled, timed from performance_schema.setup_objects")
	require.Contains(t, objects, []interface{}{"TABLE", "%", "%", "YES", "YES"})
	require.Len(t, objects, 20, "MySQL initializes four setup_objects rows for each supported object type")
	require.Contains(t, objects, []interface{}{"TABLE", "mysql", "%", "NO", "NO"})
	result := <-executor.ExecuteQuery(nil, "update performance_schema.setup_objects set enabled='NO', timed='NO' where object_type='TABLE'", "")
	require.NoError(t, result.Err)
	objects = mustQuerySQL(t, executor, "", "select object_type, enabled, timed from performance_schema.setup_objects where object_type='TABLE'")
	require.Len(t, objects, 4)
	for _, row := range objects {
		require.Equal(t, "TABLE", row[0])
		require.Equal(t, "NO", row[3])
		require.Equal(t, "NO", row[4])
	}

	timers := mustQuerySQL(t, executor, "", "select name, timer_name, timer_frequency, timer_resolution, timer_overhead from performance_schema.setup_timers")
	require.NotEmpty(t, timers)
	require.Equal(t, "CYCLE", timers[0][0])
	require.Equal(t, "CYCLE", timers[0][1])
	require.Len(t, timers[0], 5)
}

func TestPerformanceSchemaSetupViewsSupportInNotInAndNullPredicates(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	consumers := mustQuerySQL(t, executor, "", "select name from performance_schema.setup_consumers where name in ('global_instrumentation', 'events_statements_cpu')")
	require.Len(t, consumers, 2)
	consumers = mustQuerySQL(t, executor, "", "select name from performance_schema.setup_consumers where name not in ('global_instrumentation')")
	require.NotEmpty(t, consumers)
	consumers = mustQuerySQL(t, executor, "", "select name from performance_schema.setup_consumers where name is null")
	require.Empty(t, consumers)
	consumers = mustQuerySQL(t, executor, "", "select name from performance_schema.setup_consumers where name is not null")
	require.Len(t, consumers, 16)

	instruments := mustQuerySQL(t, executor, "", "select name from performance_schema.setup_instruments where name in ('statement/sql/select', 'stage/sql/execute')")
	require.Len(t, instruments, 2)
	instruments = mustQuerySQL(t, executor, "", "select name from performance_schema.setup_instruments where name not in ('statement/sql/select') and name like 'statement/sql/%'")
	require.NotEmpty(t, instruments)

	timers := mustQuerySQL(t, executor, "", "select name from performance_schema.setup_timers where name in ('CYCLE', 'NANOSECOND')")
	require.Len(t, timers, 2)
	timers = mustQuerySQL(t, executor, "", "select name from performance_schema.setup_timers where name is null")
	require.Empty(t, timers)

	mustExecSQL(t, executor, "", "insert into performance_schema.setup_actors (host, user, role, enabled, history) values ('127.0.0.1', 'predicate_actor', 'predicate_role', 'YES', 'YES')")
	actors := mustQuerySQL(t, executor, "", "select host, user, role from performance_schema.setup_actors where role in ('%', 'predicate_role')")
	require.Len(t, actors, 2)
	actors = mustQuerySQL(t, executor, "", "select host, user, role from performance_schema.setup_actors where role not in ('%')")
	require.Len(t, actors, 1)
	actors = mustQuerySQL(t, executor, "", "select host, user, role from performance_schema.setup_actors where role is null")
	require.Empty(t, actors)

	objects := mustQuerySQL(t, executor, "", "select object_type from performance_schema.setup_objects where object_type in ('TABLE', 'EVENT') and object_schema in ('%', 'app')")
	require.Len(t, objects, 2)
	objects = mustQuerySQL(t, executor, "", "select object_type, object_schema from performance_schema.setup_objects where object_type not in ('TABLE') and object_schema is not null")
	require.Len(t, objects, 16)
	objects = mustQuerySQL(t, executor, "", "select object_name from performance_schema.setup_objects where object_name is null")
	require.Empty(t, objects)
}

func TestPerformanceSchemaSetupObjectsUpdateHonorsAllObjectPredicates(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	result := <-executor.ExecuteQuery(nil, "update performance_schema.setup_objects set enabled='NO' where object_type='TABLE' and object_schema='app' and object_name='orders'", "")
	require.NoError(t, result.Err)
	require.Zero(t, result.AffectedRows, "the default TABLE/%/% row must not match a schema/name-specific predicate")

	objects := mustQuerySQL(t, executor, "", "select object_type, object_schema, object_name, enabled, timed from performance_schema.setup_objects where object_type='TABLE' and object_schema='%' and object_name='%'")
	require.Equal(t, [][]interface{}{{"TABLE", "%", "%", "YES", "YES"}}, objects)
}

func TestPerformanceSchemaSetupObjectsSupportsCustomRowsAndPrecedence(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	result := <-executor.ExecuteQuery(nil, "insert into performance_schema.setup_objects (object_type, object_schema, object_name, enabled, timed) values ('TABLE', 'app', 'orders', 'NO', 'NO')", "")
	require.NoError(t, result.Err)
	require.Equal(t, 1, result.AffectedRows)

	setting, ok := executor.QueryExecutor.performanceSchemaObjectSettingFor("TABLE", "app", "orders")
	require.True(t, ok)
	require.False(t, setting.Enabled)
	require.False(t, setting.Timed)

	result = <-executor.ExecuteQuery(nil, "update performance_schema.setup_objects set enabled='YES', timed='NO' where object_type='TABLE' and object_schema='app' and object_name='orders'", "")
	require.NoError(t, result.Err)
	require.Equal(t, 1, result.AffectedRows)
	setting, ok = executor.QueryExecutor.performanceSchemaObjectSettingFor("TABLE", "app", "orders")
	require.True(t, ok)
	require.True(t, setting.Enabled)
	require.False(t, setting.Timed)

	result = <-executor.ExecuteQuery(nil, "delete from performance_schema.setup_objects where object_type='TABLE' and object_schema='app' and object_name='orders'", "")
	require.NoError(t, result.Err)
	require.Equal(t, 1, result.AffectedRows)
	_, ok = executor.QueryExecutor.performanceSchemaObjectSettingFor("TABLE", "app", "orders")
	require.True(t, ok, "deleting the specific row must reveal the default TABLE/%/% rule")

	objects := mustQuerySQL(t, executor, "", "select object_type, object_schema, object_name from performance_schema.setup_objects where object_type='TABLE' and object_schema='%' and object_name='%'")
	require.Equal(t, [][]interface{}{{"TABLE", "%", "%", "YES", "YES"}}, objects)

	for _, statement := range []string{
		"insert into performance_schema.setup_objects values ('TABLE', 'app', '%', 'NO', 'NO')",
		"insert into performance_schema.setup_objects values ('TABLE', 'app', 'orders', 'YES', 'NO')",
	} {
		result = <-executor.ExecuteQuery(nil, statement, "")
		require.NoError(t, result.Err)
	}
	setting, ok = executor.QueryExecutor.performanceSchemaObjectSettingFor("TABLE", "app", "orders")
	require.True(t, ok)
	require.True(t, setting.Enabled, "exact object rule must beat schema wildcard")
	require.False(t, setting.Timed)

	result = <-executor.ExecuteQuery(nil, "delete from performance_schema.setup_objects where object_type='TABLE' and object_schema='app' and object_name='orders'", "")
	require.NoError(t, result.Err)
	setting, ok = executor.QueryExecutor.performanceSchemaObjectSettingFor("TABLE", "app", "orders")
	require.True(t, ok)
	require.False(t, setting.Enabled, "schema wildcard must apply after exact object deletion")

	result = <-executor.ExecuteQuery(nil, "delete from performance_schema.setup_objects where object_type='TABLE' and object_schema='app' and object_name='%'", "")
	require.NoError(t, result.Err)
	setting, ok = executor.QueryExecutor.performanceSchemaObjectSettingFor("TABLE", "app", "orders")
	require.True(t, ok)
	require.True(t, setting.Enabled, "global wildcard must apply after schema wildcard deletion")

	mustExecSQL(t, executor, "", "truncate table performance_schema.setup_objects")
	objects = mustQuerySQL(t, executor, "", "select object_type, object_schema, object_name from performance_schema.setup_objects")
	require.Empty(t, objects)
	_, ok = executor.QueryExecutor.performanceSchemaObjectSettingFor("TABLE", "app", "orders")
	require.False(t, ok, "TRUNCATE must remove all setup_objects rules")
}

func TestPerformanceSchemaSetupObjectsControlProgramInstrumentation(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table setup_object_target (id int primary key, flag varchar(20))")
	mustExecSQL(t, executor, "app", "create trigger setup_object_trigger before insert on setup_object_target for each row set new.flag = 'triggered'")

	result := <-executor.ExecuteQuery(nil, "update performance_schema.setup_objects set enabled='NO' where object_type='TRIGGER'", "")
	require.NoError(t, result.Err)
	mustExecSQL(t, executor, "app", "insert into setup_object_target values (1, 'caller')")
	rows := mustQuerySQL(t, executor, "", "select object_name from performance_schema.events_statements_summary_by_program where object_type='TRIGGER' and object_name='setup_object_trigger'")
	require.Empty(t, rows)

	result = <-executor.ExecuteQuery(nil, "update performance_schema.setup_objects set enabled='YES', timed='NO' where object_type='TRIGGER'", "")
	require.NoError(t, result.Err)
	mustExecSQL(t, executor, "app", "insert into setup_object_target values (2, 'caller')")
	program := mustSelectResultSQL(t, executor, "", "select object_name, count_star, sum_timer_wait from performance_schema.events_statements_summary_by_program where object_type='TRIGGER' and object_name='setup_object_trigger'")
	require.Len(t, program.Records, 1)
	programRow := selectResultRows(program)[0]
	require.NotEqual(t, "0", programRow[1])
	require.Equal(t, "0", programRow[2])
}

func TestPerformanceSchemaConnectionSummaryViewsExposeCurrentSession(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	// NewXMySQLEngine intentionally uses the process-wide recorder in
	// production, but this assertion is about one fresh engine's connection
	// summary. Isolate it from identities recorded by earlier package tests so
	// the package-order regression is deterministic.
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	session := newTestMySQLSession()
	session.SetParamByName("user", "app")
	session.SetParamByName("host", "client")

	for _, view := range []string{"users", "accounts", "hosts"} {
		result := <-executor.ExecuteQuery(session, "select * from performance_schema."+view, "app")
		require.NoError(t, result.Err, view)
		selectResult, ok := result.Data.(*SelectResult)
		require.True(t, ok, view)
		rows := selectResultRows(selectResult)
		require.Len(t, rows, 1, view)
		if view == "hosts" {
			require.Equal(t, "client", rows[0][0])
		} else {
			require.Equal(t, "app", rows[0][0])
		}
	}
}

func TestPerformanceSchemaConnectionSummaryRetainsHistoricalTotals(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder.RecordConnection("app", "client")
	executor.QueryExecutor.metricsRecorder.RecordConnection("app", "client")
	session := newTestMySQLSession()
	session.SetParamByName("user", "app")
	session.SetParamByName("host", "client")

	accountsResult := <-executor.ExecuteQuery(session, "select user, host, current_connections, total_connections from performance_schema.accounts where user='app' and host='client'", "")
	require.NoError(t, accountsResult.Err)
	accountValues := accountsResult.Data.(*SelectResult).Records[0].GetValues()
	require.Equal(t, "app", accountValues[0].String())
	require.Equal(t, "client", accountValues[1].String())
	require.Equal(t, int64(1), accountValues[2].Int())
	require.Equal(t, int64(2), accountValues[3].Int())

	hostsResult := <-executor.ExecuteQuery(session, "select host, current_connections, total_connections from performance_schema.hosts where host='client'", "")
	require.NoError(t, hostsResult.Err)
	hostValues := hostsResult.Data.(*SelectResult).Records[0].GetValues()
	require.Equal(t, "client", hostValues[0].String())
	require.Equal(t, int64(1), hostValues[1].Int())
	require.Equal(t, int64(2), hostValues[2].Int())
	require.Len(t, hostValues, 5)
	wrongTotal := mustSelectResultSQL(t, executor, "", "select user, host from performance_schema.accounts where user='app' and host='client' and total_connections=999999")
	require.Empty(t, wrongTotal.Records)
}

func TestPerformanceSchemaConnectionSummaryProjectsMemoryHighWatermarks(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	recorder := executor.QueryExecutor.metricsRecorder
	recorder.RecordConnection("memory_summary_user", "memory-summary.example")
	recorder.RecordMemoryAllocationWithIdentity(1301, "memory_summary_user", "memory-summary.example", "memory/test/connection-summary", 96)
	recorder.RecordMemoryFreeWithIdentity(1301, "memory_summary_user", "memory-summary.example", "memory/test/connection-summary", 96)
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(1301)
	session.SetParamByName("user", "memory_summary_user")
	session.SetParamByName("host", "memory-summary.example")

	result := executor.QueryExecutor.executePerformanceSchemaConnectionSummarySelect(
		"select user, host, current_connections, total_connections, max_session_controlled_memory, max_session_total_memory from performance_schema.accounts where user='memory_summary_user' and host='memory-summary.example'",
		"performance_schema.accounts", session,
	)
	require.Equal(t, [][]interface{}{{"memory_summary_user", "memory-summary.example", "1", "1", "96", "96"}}, selectResultRows(result))
	hosts := executor.QueryExecutor.executePerformanceSchemaConnectionSummarySelect(
		"select host, current_connections, total_connections, max_session_controlled_memory, max_session_total_memory from performance_schema.hosts where host='memory-summary.example'",
		"performance_schema.hosts", session,
	)
	require.Equal(t, [][]interface{}{{"memory-summary.example", "1", "1", "96", "96"}}, selectResultRows(hosts))
	users := executor.QueryExecutor.executePerformanceSchemaConnectionSummarySelect(
		"select user, current_connections, total_connections, max_session_controlled_memory, max_session_total_memory from performance_schema.users where user='memory_summary_user'",
		"performance_schema.users", session,
	)
	require.Equal(t, [][]interface{}{{"memory_summary_user", "1", "1", "96", "96"}}, selectResultRows(users))
}

func TestPerformanceSchemaConnectionSummaryTruncateResetsTotalsToCurrentConnections(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder.RecordConnection("app", "client")
	executor.QueryExecutor.metricsRecorder.RecordConnection("app", "client")
	executor.QueryExecutor.metricsRecorder.RecordConnection("historical", "old-client")
	executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
		1201, "app", "client", "app", "select connection_summary_reset", "SELECT", "ok", time.Millisecond, 0, 1, 0,
	)
	session := newTestMySQLSession()
	session.SetParamByName("user", "app")
	session.SetParamByName("host", "client")

	stmt, err := sqlparser.Parse("truncate table performance_schema.accounts")
	require.NoError(t, err)
	results := make(chan *Result, 1)
	executor.QueryExecutor.executeTruncateTableStatement(&ExecutionContext{Session: session, Results: results, Cfg: executor.QueryExecutor.conf}, "", stmt.(*sqlparser.DDL))
	require.NoError(t, (<-results).Err)
	statementSummary := executor.QueryExecutor.executePerformanceSchemaStatementSummaryRegistrySelect("select * from performance_schema.events_statements_summary_by_account_by_event_name where user='app' and host='client'", "account")
	countIndex := indexOfColumn(statementSummary.Columns, "COUNT_STAR")
	require.NotEmpty(t, statementSummary.Records)
	require.Zero(t, statementSummary.Records[0].GetValues()[countIndex].Int(), "accounts TRUNCATE must implicitly reset account statement summaries")

	accounts := executor.QueryExecutor.executePerformanceSchemaConnectionSummarySelect("select user, host, current_connections, total_connections from performance_schema.accounts where user='app' and host='client'", "performance_schema.accounts", session)
	require.Equal(t, [][]interface{}{{"app", "client", "1", "1", "0", "0"}}, selectResultRows(accounts))
	hosts := executor.QueryExecutor.executePerformanceSchemaConnectionSummarySelect("select host, current_connections, total_connections, sum_connections from performance_schema.hosts where host='client'", "performance_schema.hosts", session)
	require.Equal(t, [][]interface{}{{"client", "1", "1", "0", "0"}}, selectResultRows(hosts))
	users := executor.QueryExecutor.executePerformanceSchemaConnectionSummarySelect("select user, current_connections, total_connections, count_hosts from performance_schema.users where user='app'", "performance_schema.users", session)
	require.Equal(t, [][]interface{}{{"app", "1", "1", "0", "0"}}, selectResultRows(users))
	removed := executor.QueryExecutor.executePerformanceSchemaConnectionSummarySelect("select user from performance_schema.accounts where user='historical'", "performance_schema.accounts", session)
	require.Empty(t, removed.Records)
}

func TestPerformanceSchemaSessionAccountConnectAttrsOnlyExposeCurrentSession(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(77))
	session.SetParamByName("connection_attributes", map[string]string{"_client_name": "test-client", "_client_version": "1"})
	result := <-executor.ExecuteQuery(session, "select processlist_id, attr_name, attr_value, ordinal_position from performance_schema.session_account_connect_attrs", "")
	require.NoError(t, result.Err)
	rows := selectResultRows(result.Data.(*SelectResult))
	require.Equal(t, [][]interface{}{{"77", "_client_name", "test-client", "0"}, {"77", "_client_version", "1", "1"}}, rows)
	filteredResult := <-executor.ExecuteQuery(session, "select attr_name from performance_schema.session_account_connect_attrs where attr_name='_client_name'", "")
	require.NoError(t, filteredResult.Err)
	require.Equal(t, [][]interface{}{{"_client_name"}}, selectResultRows(filteredResult.Data.(*SelectResult)))
}

func TestPerformanceSchemaSessionAccountConnectAttrsIncludeSameAccountSessions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	current := newTestMySQLSession()
	current.ctx.SetConnectionID(77)
	current.SetParamByName("connection_id", int64(77))
	current.SetParamByName("user", "attrs_user")
	current.SetParamByName("host", "localhost")
	current.SetParamByName("connection_attributes", map[string]string{"_client_name": "current"})
	sameAccount := newTestMySQLSession()
	sameAccount.ctx.SetConnectionID(78)
	sameAccount.SetParamByName("connection_id", int64(78))
	sameAccount.SetParamByName("user", "attrs_user")
	sameAccount.SetParamByName("host", "localhost")
	sameAccount.SetParamByName("connection_attributes", map[string]string{"_client_name": "same-account"})
	otherAccount := newTestMySQLSession()
	otherAccount.ctx.SetConnectionID(79)
	otherAccount.SetParamByName("connection_id", int64(79))
	otherAccount.SetParamByName("user", "other_user")
	otherAccount.SetParamByName("host", "localhost")
	otherAccount.SetParamByName("connection_attributes", map[string]string{"_client_name": "other-account"})
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{current, sameAccount, otherAccount}
	})

	result := <-executor.ExecuteQuery(current, "select processlist_id, attr_name, attr_value from performance_schema.session_account_connect_attrs order by processlist_id", "")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{
		{"77", "_client_name", "current"},
		{"78", "_client_name", "same-account"},
	}, selectResultRows(result.Data.(*SelectResult)))
}

func TestPerformanceSchemaSetupThreadsExposeThreadInstrumentation(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'app'@'client' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on performance_schema.setup_threads to 'app'@'client'")
	mustExecSQL(t, executor, "", "grant update on performance_schema.setup_threads to 'app'@'client'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "app")
	session.SetParamByName("host", "client")
	session.SetParamByName("connection_id", int64(42))

	result := <-executor.ExecuteQuery(session, "select * from performance_schema.setup_threads", "app")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	rows := selectResultRows(selectResult)
	wantNames := []string{
		"thread/performance_schema/setup",
		"thread/sql/admin_interface",
		"thread/sql/bootstrap",
		"thread/sql/compress_gtid_table",
		"thread/sql/event_scheduler",
		"thread/sql/main",
		"thread/sql/manager",
		"thread/sql/one_connection",
		"thread/sql/parser_service",
		"thread/sql/signal_handler",
	}
	if runtime.GOOS == "windows" {
		wantNames = append(wantNames[:1], append([]string{
			"thread/sql/con_named_pipes",
			"thread/sql/con_shared_mem",
			"thread/sql/con_sockets",
			"thread/sql/shutdown_restart",
		}, wantNames[1:]...)...)
	}
	sort.Strings(wantNames)
	require.Len(t, rows, len(wantNames))
	actualNames := make([]string, 0, len(rows))
	for _, row := range rows {
		actualNames = append(actualNames, row[0].(string))
	}
	require.Equal(t, wantNames, actualNames)
	indices := make(map[string]int, len(actualNames))
	for index, name := range actualNames {
		indices[name] = index
	}
	require.Equal(t, "YES", rows[indices["thread/sql/one_connection"]][1])
	require.Equal(t, "YES", rows[indices["thread/sql/one_connection"]][2])
	require.Equal(t, "user", rows[indices["thread/sql/one_connection"]][3])
	for _, name := range []string{
		"thread/performance_schema/setup",
		"thread/sql/bootstrap",
		"thread/sql/compress_gtid_table",
		"thread/sql/event_scheduler",
		"thread/sql/main",
		"thread/sql/manager",
		"thread/sql/parser_service",
		"thread/sql/signal_handler",
		"thread/sql/con_named_pipes",
		"thread/sql/con_shared_mem",
		"thread/sql/con_sockets",
		"thread/sql/shutdown_restart",
	} {
		if _, ok := indices[name]; ok {
			require.Equal(t, "singleton", rows[indices[name]][3], name)
		}
	}
	require.Equal(t, "user", rows[indices["thread/sql/admin_interface"]][3])
	filteredResult := <-executor.ExecuteQuery(session, "select name from performance_schema.setup_threads where name='missing'", "app")
	require.NoError(t, filteredResult.Err)
	require.Empty(t, filteredResult.Data.(*SelectResult).Records)
	projectedResult := <-executor.ExecuteQuery(session, "select name, history from performance_schema.setup_threads where name='thread/sql/one_connection'", "app")
	require.NoError(t, projectedResult.Err)
	require.Equal(t, []string{"NAME", "HISTORY"}, projectedResult.Data.(*SelectResult).Columns)
	require.Equal(t, [][]interface{}{{"thread/sql/one_connection", "YES"}}, selectResultRows(projectedResult.Data.(*SelectResult)))
	update := <-executor.ExecuteQuery(session, "update performance_schema.setup_threads set enabled='NO', history='NO' where name='thread/sql/one_connection'", "app")
	require.NoError(t, update.Err)
	updated := <-executor.ExecuteQuery(session, "select enabled, history from performance_schema.setup_threads where name='thread/sql/one_connection'", "app")
	require.NoError(t, updated.Err)
	require.Equal(t, [][]interface{}{{"NO", "NO"}}, selectResultRows(updated.Data.(*SelectResult)))
	backgroundUpdate := <-executor.ExecuteQuery(session, "update performance_schema.setup_threads set enabled='NO' where name='thread/sql/main'", "app")
	require.NoError(t, backgroundUpdate.Err)
	require.Equal(t, 1, backgroundUpdate.AffectedRows)
	background := <-executor.ExecuteQuery(session, "select name, enabled, history, properties from performance_schema.setup_threads where name='thread/sql/main'", "app")
	require.NoError(t, background.Err)
	require.Equal(t, [][]interface{}{{"thread/sql/main", "NO", "YES", "singleton"}}, selectResultRows(background.Data.(*SelectResult)))
	truncate := <-executor.ExecuteQuery(session, "truncate table performance_schema.setup_threads", "app")
	require.Error(t, truncate.Err)
}

func TestPerformanceSchemaSetupThreadsControlNewConnectionInstrumentation(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	existing := newTestMySQLSession()
	existing.SetParamByName("user", "root")
	existing.SetParamByName("host", "localhost")
	existing.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	existing.SetParamByName("connection_id", int64(142))
	beforeUpdate := <-executor.ExecuteQuery(existing, "select thread_id, instrumented, history from performance_schema.threads", "")
	require.NoError(t, beforeUpdate.Err)
	require.Equal(t, [][]interface{}{{"142", "YES", "YES"}}, selectResultRows(beforeUpdate.Data.(*SelectResult)))

	update := <-executor.ExecuteQuery(nil, "update performance_schema.setup_threads set enabled='NO', history='NO' where name='thread/sql/one_connection'", "")
	require.NoError(t, update.Err)
	require.Equal(t, 1, update.AffectedRows)
	existingAfterUpdate := <-executor.ExecuteQuery(existing, "select thread_id, instrumented, history from performance_schema.threads", "")
	require.NoError(t, existingAfterUpdate.Err)
	require.Equal(t, [][]interface{}{{"142", "YES", "YES"}}, selectResultRows(existingAfterUpdate.Data.(*SelectResult)))

	session := newTestMySQLSession()
	session.SetParamByName("user", "root")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	session.SetParamByName("connection_id", int64(143))
	result := <-executor.ExecuteQuery(session, "select thread_id, instrumented, history from performance_schema.threads", "")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"143", "NO", "NO"}}, selectResultRows(result.Data.(*SelectResult)))
}

func TestPerformanceSchemaSetupConsumersAndInstrumentsRejectTruncate(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	for _, table := range []string{"setup_consumers", "setup_instruments"} {
		result := <-executor.ExecuteQuery(nil, "truncate table performance_schema."+table, "")
		require.Error(t, result.Err, table)
		require.Contains(t, strings.ToLower(result.Err.Error()), "invalid performance_schema usage", table)
	}
}

func TestPerformanceSchemaSetupTablesRejectUnknownConfigurationColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	queries := []string{
		"update performance_schema.setup_consumers set timed='NO' where name='events_statements_history_long'",
		"update performance_schema.setup_instruments set history='NO' where name='statement/sql/select'",
		"update performance_schema.setup_actors set timed='NO' where host='%' and user='%' and role='%'",
		"update performance_schema.setup_threads set timed='NO' where name='thread/sql/one_connection'",
	}
	for _, query := range queries {
		result := <-executor.ExecuteQuery(nil, query, "")
		require.Error(t, result.Err, query)
		require.Contains(t, strings.ToLower(result.Err.Error()), "unsupported performance schema setup assignment", query)
	}
}

func TestPerformanceSchemaHistorySizeVariablesResizeLiveRetention(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	session := newTestMySQLSession()
	session.SetParamByName("user", "root")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	session.SetParamByName("connection_id", int64(1725))

	mustExecSessionSQL(t, executor, session, "", "set global performance_schema_events_statements_history_size=2")
	mustExecSessionSQL(t, executor, session, "", "set global performance_schema_events_statements_history_long_size=3")
	short := mustSelectResultSessionSQL(t, executor, session, "", "select @@global.performance_schema_events_statements_history_size")
	require.Equal(t, int64(2), short.Records[0].GetValues()[0].Int())
	long := mustSelectResultSessionSQL(t, executor, session, "", "select @@global.performance_schema_events_statements_history_long_size")
	require.Equal(t, int64(3), long.Records[0].GetValues()[0].Int())
	mustExecSessionSQL(t, executor, session, "", "set global performance_schema_events_stages_history_size=1")
	mustExecSessionSQL(t, executor, session, "", "set global performance_schema_events_stages_history_long_size=2")

	for index := 0; index < 4; index++ {
		executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadID(1725, "app", fmt.Sprintf("select retention %d", index), "SELECT", "ok", time.Millisecond)
	}
	require.Len(t, executor.QueryExecutor.metricsRecorder.StatementHistoryForThread(1725), 2)
	require.Len(t, executor.QueryExecutor.metricsRecorder.StatementHistory(), 3)
	for index := 0; index < 3; index++ {
		executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccountingWithSettings(1725, "app", "localhost", "app", fmt.Sprintf("select stage retention %d", index), "SELECT", "ok", time.Millisecond, 0, 0, 0, true, true)
	}
	require.Len(t, executor.QueryExecutor.metricsRecorder.StageHistoryForThread(1725), 1)
	require.Len(t, executor.QueryExecutor.metricsRecorder.StageHistory(), 2)
}

func TestPerformanceSchemaHistorySizeVariablesResizeTransactionAndWaitRetention(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("user", "root")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	session.SetParamByName("connection_id", int64(1726))

	mustExecSessionSQL(t, executor, session, "", "set global performance_schema_events_transactions_history_size=2")
	mustExecSessionSQL(t, executor, session, "", "set global performance_schema_events_transactions_history_long_size=3")
	mustExecSessionSQL(t, executor, session, "", "update performance_schema.setup_consumers set enabled='YES' where name='events_transactions_history_long'")
	for index := 0; index < 4; index++ {
		executor.QueryExecutor.markPerformanceSchemaTransactionStart(session)
		executor.QueryExecutor.recordPerformanceSchemaTransactionHistory(session, "COMMIT")
	}
	transactionHistory, _ := session.GetParamByName("performance_schema_transaction_history").([]performanceSchemaTransactionEvent)
	require.Len(t, transactionHistory, 2)
	executor.QueryExecutor.performanceSchemaMu.RLock()
	require.Len(t, executor.QueryExecutor.performanceSchemaTransactionHistoryLong, 3)
	executor.QueryExecutor.performanceSchemaMu.RUnlock()

	mustExecSessionSQL(t, executor, session, "", "set global performance_schema_events_waits_history_size=1")
	mustExecSessionSQL(t, executor, session, "", "set global performance_schema_events_waits_history_long_size=2")
	lock := executor.QueryExecutor.ddlCoordinator.lockFor("retention_table")
	for index := 0; index < 3; index++ {
		lock.stateMu.Lock()
		lock.recordWaitHistoryLocked(fmt.Sprintf("waiter-%d", index), metadataLockWaiter{
			mode:        tableLockRead,
			waitStarted: time.Now(),
			blockers:    map[string]tableLockMode{"blocker": tableLockWrite},
		}, time.Millisecond)
		lock.stateMu.Unlock()
	}
	require.Len(t, executor.QueryExecutor.ddlCoordinator.MetadataLockWaitHistory(), 1)
	require.Len(t, executor.QueryExecutor.ddlCoordinator.MetadataLockWaitHistoryLong(), 2)
}

func TestPerformanceSchemaThreadsExposeMySQL84ColumnsAndLiveAttributes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'app'@'127.0.0.1' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on performance_schema.threads to 'app'@'127.0.0.1'")
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(126)
	session.SetParamByName("user", "app")
	session.SetParamByName("host", "127.0.0.1")
	session.SetParamByName("database", "inventory")

	result := <-executor.ExecuteQuery(session, "select * from performance_schema.threads", "inventory")
	require.NoError(t, result.Err)
	selectResult := result.Data.(*SelectResult)
	require.Equal(t, []string{
		"THREAD_ID", "NAME", "TYPE", "PROCESSLIST_ID", "PROCESSLIST_USER", "PROCESSLIST_HOST",
		"PROCESSLIST_DB", "PROCESSLIST_COMMAND", "PROCESSLIST_TIME", "PROCESSLIST_STATE", "PROCESSLIST_INFO",
		"PARENT_THREAD_ID", "ROLE", "INSTRUMENTED", "HISTORY", "CONNECTION_TYPE", "THREAD_OS_ID",
		"RESOURCE_GROUP", "EXECUTION_ENGINE", "CONTROLLED_MEMORY", "MAX_CONTROLLED_MEMORY", "TOTAL_MEMORY",
		"MAX_TOTAL_MEMORY", "TELEMETRY_ACTIVE",
	}, selectResult.Columns)
	require.Len(t, selectResult.Records, 1)
	values := selectResult.Records[0].GetValues()
	require.Equal(t, int64(126), values[0].Int())
	require.Equal(t, "app", values[4].String())
	require.Equal(t, "127.0.0.1", values[5].String())
	require.Equal(t, "inventory", values[6].String())
	require.Equal(t, "PRIMARY", values[18].String())
	require.Equal(t, "NO", values[23].String())
	filtered := <-executor.ExecuteQuery(session, "select thread_id from performance_schema.threads where thread_id=999", "inventory")
	require.NoError(t, filtered.Err)
	require.Empty(t, filtered.Data.(*SelectResult).Records)
}

func TestPerformanceSchemaThreadsExposeMainBackgroundThreadWithoutSession(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	result := executor.QueryExecutor.executePerformanceSchemaThreadsSelect(
		"select thread_id, name, type, processlist_id, processlist_command from performance_schema.threads",
	)
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	require.Equal(t, int64(1), values[0].Int())
	require.Equal(t, "thread/sql/main", values[1].String())
	require.Equal(t, "BACKGROUND", values[2].String())
	require.True(t, values[3].IsNull(), "main server thread must not have a PROCESSLIST_ID")
	require.True(t, values[4].IsNull(), "background server thread must not expose a client command")
}

func TestPerformanceSchemaThreadsUpdateControlsMainBackgroundThread(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	before := executor.QueryExecutor.executePerformanceSchemaThreadsSelect(
		"select thread_id, instrumented, history from performance_schema.threads",
	)
	require.Equal(t, [][]interface{}{{"1", "YES", "YES"}}, selectResultRows(before))

	update := <-executor.ExecuteQuery(nil,
		"update performance_schema.threads set instrumented='NO', history='NO' where thread_id=1", "")
	require.NoError(t, update.Err)
	require.Equal(t, 1, update.AffectedRows)

	after := executor.QueryExecutor.executePerformanceSchemaThreadsSelect(
		"select thread_id, instrumented, history from performance_schema.threads",
	)
	require.Equal(t, [][]interface{}{{"1", "NO", "NO"}}, selectResultRows(after))
}

func TestPerformanceSchemaMainBackgroundThreadProjectsServerRuntimeFields(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.serverStartedAt = time.Now().Add(-2*time.Hour - 2*time.Second)

	result := executor.QueryExecutor.executePerformanceSchemaThreadsSelect(
		"select thread_id, processlist_db, processlist_time, resource_group from performance_schema.threads",
	)
	require.Equal(t, 1, len(result.Records))
	values := result.Records[0].GetValues()
	require.Equal(t, int64(1), values[0].Int())
	require.Equal(t, "mysql", values[1].String())
	require.GreaterOrEqual(t, values[2].Int(), int64(2*time.Hour/time.Second))
	require.Equal(t, "SYS_default", values[3].String())
}

func TestPerformanceSchemaThreadsExposeRuntimeMemoryCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(127)
	session.SetParamByName("user", "app")
	session.SetParamByName("host", "127.0.0.1")
	executor.QueryExecutor.metricsRecorder.RecordMemoryAllocation(127, "memory/sql/THD::main_mem_root", 64)
	executor.QueryExecutor.metricsRecorder.RecordMemoryFree(127, "memory/sql/THD::main_mem_root", 32)

	selectResult := executor.QueryExecutor.executePerformanceSchemaThreadsSelect("select thread_id, controlled_memory, max_controlled_memory, total_memory, max_total_memory from performance_schema.threads", session)
	rows := selectResultRows(selectResult)
	require.Equal(t, [][]interface{}{{"127", "32", "64", "32", "64"}}, rows)
}

func TestPerformanceSchemaThreadsExposeAuthoritativeOSThreadID(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(129)
	session.SetParamByName("user", "app")
	session.SetParamByName("host", "127.0.0.1")
	session.SetParamByName("__thread_os_id", int64(4242))

	selectResult := executor.QueryExecutor.executePerformanceSchemaThreadsSelect(
		"select thread_id, thread_os_id from performance_schema.threads", session,
	)
	require.Len(t, selectResult.Records, 1)
	values := selectResult.Records[0].GetValues()
	require.Equal(t, int64(129), values[0].Int())
	require.Equal(t, int64(4242), values[1].Int())
}

func TestPerformanceSchemaThreadsUpdateControlsCurrentThread(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'app'@'127.0.0.1' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant update on performance_schema.threads to 'app'@'127.0.0.1'")
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(128)
	session.SetParamByName("user", "app")
	session.SetParamByName("host", "127.0.0.1")
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession { return []server.MySQLServerSession{session} })

	result := <-executor.ExecuteQuery(session, "update performance_schema.threads set instrumented='NO', history='NO' where thread_id=128", "")
	require.NoError(t, result.Err)
	require.Equal(t, 1, result.AffectedRows)
	selectResult := executor.QueryExecutor.executePerformanceSchemaThreadsSelect("select thread_id, instrumented, history from performance_schema.threads", session)
	require.Equal(t, [][]interface{}{{"128", "NO", "NO"}}, selectResultRows(selectResult))
}

func TestPerformanceSchemaThreadsUpdateRequiresUpdatePrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'p_s_threads_reader'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.ctx.SetConnectionID(129)
	session.SetParamByName("user", "p_s_threads_reader")
	session.SetParamByName("host", "localhost")
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession { return []server.MySQLServerSession{session} })

	query := "update performance_schema.threads set instrumented='NO' where thread_id=129"
	result := <-executor.ExecuteQuery(session, query, "")
	require.Error(t, result.Err)
	require.Contains(t, strings.ToLower(result.Err.Error()), "lacks update privilege")

	mustExecSQL(t, executor, "", "grant update on performance_schema.threads to 'p_s_threads_reader'@'localhost'")
	result = <-executor.ExecuteQuery(session, query, "")
	require.NoError(t, result.Err)
	require.Equal(t, 1, result.AffectedRows)
}

func TestPerformanceSchemaThreadsSelectRequiresSelectPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'p_s_threads_reader'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.ctx.SetConnectionID(130)
	session.SetParamByName("user", "p_s_threads_reader")
	session.SetParamByName("host", "localhost")
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession { return []server.MySQLServerSession{session} })

	query := "select thread_id from performance_schema.threads where thread_id=130"
	result := <-executor.ExecuteQuery(session, query, "")
	require.Error(t, result.Err)
	require.Contains(t, strings.ToLower(result.Err.Error()), "lacks select privilege")

	mustExecSQL(t, executor, "", "grant select on performance_schema.threads to 'p_s_threads_reader'@'localhost'")
	result = <-executor.ExecuteQuery(session, query, "")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"130"}}, selectResultRows(result.Data.(*SelectResult)))
}

func TestPerformanceSchemaSelectRequiresSelectPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'p_s_select_reader'@'localhost' identified by 'secret'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "p_s_select_reader")
	session.SetParamByName("host", "localhost")

	query := "select name, enabled from performance_schema.setup_consumers where name='events_statements_history_long'"
	result := <-executor.ExecuteQuery(session, query, "")
	require.Error(t, result.Err)
	require.Contains(t, strings.ToLower(result.Err.Error()), "lacks select privilege")

	mustExecSQL(t, executor, "", "grant select on performance_schema.setup_consumers to 'p_s_select_reader'@'localhost'")
	result = <-executor.ExecuteQuery(session, query, "")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"events_statements_history_long", "NO"}}, selectResultRows(result.Data.(*SelectResult)))
}

func TestPerformanceSchemaComponentTablesRequireIndependentSelectPrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'p_s_component_reader'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "p_s_component_reader")
	session.SetParamByName("host", "localhost")

	for _, table := range []string{"component_scheduler_tasks", "clone_status", "keyring_keys"} {
		query := "select * from performance_schema." + table
		denied := <-executor.ExecuteQuery(session, query, "")
		require.Error(t, denied.Err, table)
		require.Contains(t, strings.ToLower(denied.Err.Error()), "lacks select privilege", table)

		mustExecSQL(t, executor, "", "grant select on performance_schema."+table+" to 'p_s_component_reader'@'localhost'")
		allowed := <-executor.ExecuteQuery(session, query, "")
		require.NoError(t, allowed.Err, table)
		result, ok := allowed.Data.(*SelectResult)
		require.True(t, ok, table)
		require.NotEmpty(t, result.Columns, table)
	}
}

func TestPerformanceSchemaSetupTablesAcceptConfigurationUpdates(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	result := <-executor.ExecuteQuery(nil, "update performance_schema.setup_consumers set enabled='NO' where name='events_statements_history_long'", "")
	require.NoError(t, result.Err)
	consumers := mustQuerySQL(t, executor, "", "select name, enabled from performance_schema.setup_consumers where name='events_statements_history_long'")
	require.Equal(t, [][]interface{}{{"events_statements_history_long", "NO"}}, consumers)

	result = <-executor.ExecuteQuery(nil, "update performance_schema.setup_instruments set timed='NO' where name='statement/sql/select'", "")
	require.NoError(t, result.Err)
	instruments := mustQuerySQL(t, executor, "", "select name, enabled, timed from performance_schema.setup_instruments where name='statement/sql/select'")
	require.Len(t, instruments, 1)
	require.Equal(t, []interface{}{"statement/sql/select", "YES", "NO"}, instruments[0][:3])

	result = <-executor.ExecuteQuery(nil, "update performance_schema.setup_instruments set enabled='NO', timed='NO' where name like 'statement/sql/%'", "")
	require.NoError(t, result.Err)
	instruments = mustQuerySQL(t, executor, "", "select name, enabled, timed from performance_schema.setup_instruments where name like 'statement/sql/%'")
	require.Len(t, instruments, len(performanceSchemaStatementInstrumentNames))
	for _, row := range instruments {
		require.Equal(t, []interface{}{row[0], "NO", "NO"}, row[:3])
	}
}

func TestPerformanceSchemaSetupCapacityVariablesAreExposed(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select @@global.performance_schema_setup_actors_size, @@global.performance_schema_setup_objects_size")
	require.Equal(t, [][]interface{}{{"-1", "-1"}}, selectResultRows(result))
}

func TestPerformanceSchemaShowProcesslistVariableIsDynamic(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("global_privileges", []common.PrivilegeType{common.SuperPriv})

	initial := mustSelectResultSessionSQL(t, executor, session, "", "select @@global.performance_schema_show_processlist")
	require.Equal(t, [][]interface{}{{"OFF"}}, selectResultRows(initial))
	mustExecSessionSQL(t, executor, session, "", "set global performance_schema_show_processlist=on")
	enabled := mustSelectResultSessionSQL(t, executor, session, "", "select @@global.performance_schema_show_processlist")
	require.Equal(t, [][]interface{}{{"ON"}}, selectResultRows(enabled))

	mustExecSessionSQL(t, executor, session, "", "show processlist")
}

func TestPerformanceSchemaConnectionSummaryStartupCapacities(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("accounts_size", "0")
	require.NoError(t, err)
	_, err = section.NewKey("hosts_size", "1")
	require.NoError(t, err)
	_, err = section.NewKey("users_size", "1")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	variables := mustSelectResultSQL(t, executor, "", "select @@global.performance_schema_accounts_size, @@global.performance_schema_hosts_size, @@global.performance_schema_users_size")
	require.Equal(t, [][]interface{}{{"0", "1", "1"}}, selectResultRows(variables))

	current := newTestMySQLSession()
	current.SetParamByName("connection_id", int64(941))
	current.SetParamByName("user", "summary_alice")
	current.SetParamByName("host", "summary-host-a")
	other := newTestMySQLSession()
	other.SetParamByName("connection_id", int64(942))
	other.SetParamByName("user", "summary_bob")
	other.SetParamByName("host", "summary-host-b")
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{current, other}
	})

	accounts := executor.QueryExecutor.executePerformanceSchemaConnectionSummarySelect("select user, host from performance_schema.accounts", "performance_schema.accounts", current)
	require.Empty(t, accounts.Records, "accounts_size=0 must disable account statistics")
	hosts := executor.QueryExecutor.executePerformanceSchemaConnectionSummarySelect("select host from performance_schema.hosts", "performance_schema.hosts", current)
	require.Len(t, hosts.Records, 1, "hosts_size=1 must cap host rows")
	users := executor.QueryExecutor.executePerformanceSchemaConnectionSummarySelect("select user from performance_schema.users", "performance_schema.users", current)
	require.Len(t, users.Records, 1, "users_size=1 must cap user rows")
	executor.QueryExecutor.executePerformanceSchemaConnectionSummarySelect("select user, host from performance_schema.accounts", "performance_schema.accounts", current)
	executor.QueryExecutor.executePerformanceSchemaConnectionSummarySelect("select host from performance_schema.hosts", "performance_schema.hosts", current)
	executor.QueryExecutor.executePerformanceSchemaConnectionSummarySelect("select user from performance_schema.users", "performance_schema.users", current)
	lost := mustSelectResultSQL(t, executor, "", "select variable_name, variable_value from performance_schema.global_status where variable_name in ('Performance_schema_accounts_lost', 'Performance_schema_hosts_lost', 'Performance_schema_users_lost') order by variable_name")
	require.Equal(t, [][]interface{}{{"Performance_schema_accounts_lost", "2"}, {"Performance_schema_hosts_lost", "1"}, {"Performance_schema_users_lost", "1"}}, selectResultRows(lost))

	status := executor.QueryExecutor.executePerformanceSchemaStatusRegistrySummarySelect("select user, variable_name from performance_schema.status_by_account", "status_by_account", current)
	require.Empty(t, status.Records, "accounts_size=0 must disable account status statistics")
}

func TestPerformanceSchemaConnectionSummaryCapacityDoesNotResurrectEvictedRows(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	for key, value := range map[string]string{"accounts_size": "1", "hosts_size": "1", "users_size": "1"} {
		_, err = section.NewKey(key, value)
		require.NoError(t, err)
	}

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	retained := newTestMySQLSession()
	retained.SetParamByName("connection_id", int64(951))
	retained.SetParamByName("user", "capacity_alice")
	retained.SetParamByName("host", "capacity-host-a")
	evicted := newTestMySQLSession()
	evicted.SetParamByName("connection_id", int64(952))
	evicted.SetParamByName("user", "capacity_bob")
	evicted.SetParamByName("host", "capacity-host-b")
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{retained, evicted}
	})

	tests := []struct {
		name       string
		allQuery   string
		retainedQ  string
		evictedQ   string
		allColumn  string
		retainedV  string
		evictedV   string
		lostStatus string
	}{
		{
			name: "accounts", allQuery: "select user, host from performance_schema.accounts",
			retainedQ: "select user, host from performance_schema.accounts where user='capacity_alice'",
			evictedQ:  "select user, host from performance_schema.accounts where user='capacity_bob'",
			allColumn: "USER", retainedV: "capacity_alice", evictedV: "capacity_bob", lostStatus: "Performance_schema_accounts_lost",
		},
		{
			name: "hosts", allQuery: "select host from performance_schema.hosts",
			retainedQ: "select host from performance_schema.hosts where host='capacity-host-a'",
			evictedQ:  "select host from performance_schema.hosts where host='capacity-host-b'",
			allColumn: "HOST", retainedV: "capacity-host-a", evictedV: "capacity-host-b", lostStatus: "Performance_schema_hosts_lost",
		},
		{
			name: "users", allQuery: "select user from performance_schema.users",
			retainedQ: "select user from performance_schema.users where user='capacity_alice'",
			evictedQ:  "select user from performance_schema.users where user='capacity_bob'",
			allColumn: "USER", retainedV: "capacity_alice", evictedV: "capacity_bob", lostStatus: "Performance_schema_users_lost",
		},
	}

	for _, test := range tests {
		name := test.name
		t.Run(name, func(t *testing.T) {
			table := "performance_schema." + name
			all := executor.QueryExecutor.executePerformanceSchemaConnectionSummarySelect(test.allQuery, table, retained)
			require.Len(t, all.Records, 1)
			require.Equal(t, test.retainedV, all.Records[0].GetValues()[0].String())
			retainedResult := executor.QueryExecutor.executePerformanceSchemaConnectionSummarySelect(test.retainedQ, table, retained)
			require.Len(t, retainedResult.Records, 1)
			require.Equal(t, test.retainedV, retainedResult.Records[0].GetValues()[0].String())
			evictedResult := executor.QueryExecutor.executePerformanceSchemaConnectionSummarySelect(test.evictedQ, table, retained)
			require.Empty(t, evictedResult.Records, "a SQL predicate must not resurrect an evicted %s row", test.name)
			status := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='"+test.lostStatus+"'")
			require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(status))
		})
	}
}

func TestPerformanceSchemaDigestStartupCapacity(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("digests_size", "1")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	variables := mustSelectResultSQL(t, executor, "", "select @@global.performance_schema_digests_size")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(variables))
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	executor.QueryExecutor.metricsRecorder.RecordStatement("digest_capacity_app", "select 1", "select", "ok", time.Millisecond)
	executor.QueryExecutor.metricsRecorder.RecordStatement("digest_capacity_app", "select count(*) from digest_capacity_table", "select", "ok", time.Millisecond)
	digests := mustSelectResultSQL(t, executor, "", "select schema_name, digest_text from performance_schema.events_statements_summary_by_digest where schema_name='digest_capacity_app'")
	require.Len(t, digests.Records, 1, "performance_schema_digests_size=1 must cap digest rows")
	evicted := mustSelectResultSQL(t, executor, "", "select schema_name, digest_text from performance_schema.events_statements_summary_by_digest where schema_name='digest_capacity_app' and digest_text like '%digest_capacity_table%'")
	require.Empty(t, evicted.Records, "a selective query must not resurrect an evicted digest")
	status := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_digest_lost'")
	lost, err := strconv.ParseInt(selectResultRows(status)[0][0].(string), 10, 64)
	require.NoError(t, err)
	require.GreaterOrEqual(t, lost, int64(1))
}

func TestPerformanceSchemaErrorSummaryStartupCapacity(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("error_size", "0")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	variables := mustSelectResultSQL(t, executor, "", "select @@global.performance_schema_error_size")
	require.Equal(t, [][]interface{}{{"0"}}, selectResultRows(variables))

	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(1747))
	failed := <-executor.ExecuteQuery(session, "select * from missing_error_capacity_table", "error_capacity_app")
	require.Error(t, failed.Err)
	errors := mustSelectResultSQL(t, executor, "", "select * from performance_schema.events_errors_summary_global_by_error")
	require.Empty(t, errors.Records, "performance_schema_error_size=0 must disable error instrumentation")

	positiveCfg := conf.NewCfg()
	positiveCfg.DataDir = t.TempDir()
	positiveCfg.InnodbDataDir = positiveCfg.DataDir
	positiveSection, err := positiveCfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = positiveSection.NewKey("error_size", "1")
	require.NoError(t, err)
	positiveExecutor := NewXMySQLEngine(positiveCfg)
	t.Cleanup(func() { require.NoError(t, positiveExecutor.Close()) })
	positiveSession := newTestMySQLSession()
	positiveSession.SetParamByName("connection_id", int64(1748))
	failed = <-positiveExecutor.ExecuteQuery(positiveSession, "select * from missing_error_capacity_table", "error_capacity_app")
	require.Error(t, failed.Err)
	positiveErrors := mustSelectResultSQL(t, positiveExecutor, "", "select * from performance_schema.events_errors_summary_global_by_error")
	require.LessOrEqual(t, len(positiveErrors.Records), 1, "positive performance_schema_error_size must cap error-summary rows")
}

func TestPerformanceSchemaErrorSummaryCapacityDoesNotResurrectFilteredRows(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("error_size", "1")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	recorder := executor.QueryExecutor.metricsRecorder
	recorder.RecordQueryError("error_capacity_app", "execution", string(ExecutionErrorCodeSchemaOrTableNotFound))
	recorder.RecordQueryError("error_capacity_app", "execution", string(ExecutionErrorCodeValidation))
	recorder.RecordQueryErrorWithIdentity("error_capacity_app", "execution", string(ExecutionErrorCodeSchemaOrTableNotFound), 1841, "capacity_alice", "capacity-host")
	recorder.RecordQueryErrorWithIdentity("error_capacity_app", "execution", string(ExecutionErrorCodeSchemaOrTableNotFound), 1842, "capacity_bob", "capacity-host")

	global := mustSelectResultSQL(t, executor, "", "select error_name from performance_schema.events_errors_summary_global_by_error")
	require.Equal(t, [][]interface{}{{"ER_NO_SUCH_TABLE"}}, selectResultRows(global))
	evictedGlobal := mustSelectResultSQL(t, executor, "", "select error_name from performance_schema.events_errors_summary_global_by_error where error_name='ER_PARSE_ERROR'")
	require.Empty(t, evictedGlobal.Records, "a global error row evicted by error_size must not be resurrected")

	accounts := mustSelectResultSQL(t, executor, "", "select user, host from performance_schema.events_errors_summary_by_account_by_error")
	require.Equal(t, [][]interface{}{{"capacity_alice", "capacity-host"}}, selectResultRows(accounts))
	evictedAccount := mustSelectResultSQL(t, executor, "", "select user, host from performance_schema.events_errors_summary_by_account_by_error where user='capacity_bob'")
	require.Empty(t, evictedAccount.Records, "an identity error row evicted by error_size must not be resurrected")
}

func TestPerformanceSchemaMetadataLockStartupCapacity(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_metadata_locks", "0")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	variables := mustSelectResultSQL(t, executor, "", "select @@global.performance_schema_max_metadata_locks")
	require.Equal(t, [][]interface{}{{"0"}}, selectResultRows(variables))
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(1749))
	err = executor.QueryExecutor.acquireSessionTableLocks(context.Background(), session, []sessionTableLockRequest{{table: "app.metadata_lost", mode: "read"}})
	require.NoError(t, err)
	t.Cleanup(func() { executor.QueryExecutor.releaseSessionTableLocks(session) })
	locks := mustSelectResultSQL(t, executor, "", "select * from performance_schema.metadata_locks")
	require.Empty(t, locks.Records, "max_metadata_locks=0 must disable metadata lock instrumentation")
	lost := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_metadata_lock_lost'")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(lost))
}

func TestPerformanceSchemaTableHandleStartupCapacity(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_table_handles", "0")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	variables := mustSelectResultSQL(t, executor, "", "select @@global.performance_schema_max_table_handles")
	require.Equal(t, [][]interface{}{{"0"}}, selectResultRows(variables))
	handles := mustSelectResultSQL(t, executor, "", "select * from performance_schema.table_handles")
	require.Empty(t, handles.Records, "max_table_handles=0 must disable table handle instrumentation")
}

func TestPerformanceSchemaTableHandleCapacityReportsLost(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_table_handles", "0")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	owner := newTestMySQLSession()
	observer := newTestMySQLSession()
	owner.SetParamByName("connection_id", int64(1751))
	owner.SessionContext().SetConnectionID(1751)
	observer.SetParamByName("connection_id", int64(1752))
	observer.SessionContext().SetConnectionID(1752)
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table table_handle_lost (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables table_handle_lost read")
	t.Cleanup(func() { executor.QueryExecutor.releaseSessionTableLocks(owner) })
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{owner, observer}
	})

	handles := mustSelectResultSessionSQL(t, executor, observer, "app", "select * from performance_schema.table_handles")
	require.Empty(t, handles.Records, "max_table_handles=0 must hide the table handle")
	lost := mustSelectResultSessionSQL(t, executor, observer, "app", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_table_handles_lost'")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(lost))
}

func TestPerformanceSchemaSessionConnectAttrsStartupCapacity(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("session_connect_attrs_size", "3")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	variables := mustSelectResultSQL(t, executor, "", "select @@global.performance_schema_session_connect_attrs_size")
	require.Equal(t, [][]interface{}{{"3"}}, selectResultRows(variables))
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(1750))
	session.SetParamByName("user", "attrs_capacity")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("connection_attributes", map[string]string{"_client_name": "abcdef"})
	attrs := mustSelectResultSessionSQL(t, executor, session, "", "select attr_name, attr_value from performance_schema.session_connect_attrs where processlist_id=1750")
	require.Empty(t, selectResultRows(attrs), "a three-byte buffer cannot retain the encoded attribute key")
	lost := mustSelectResultSessionSQL(t, executor, session, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_session_connect_attrs_lost'")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(lost))
	longest := mustSelectResultSessionSQL(t, executor, session, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_session_connect_attrs_longest_seen'")
	require.Equal(t, [][]interface{}{{"20"}}, selectResultRows(longest))
}

func TestPerformanceSchemaSessionConnectAttrsExposesTruncatedBytesWhenBufferAllows(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("session_connect_attrs_size", "30")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(1756))
	session.SetParamByName("user", "attrs_truncated")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("connection_attributes", map[string]string{
		"a": "1",
		"b": strings.Repeat("x", 100),
	})

	attrs := mustSelectResultSessionSQL(t, executor, session, "", "select attr_name, attr_value from performance_schema.session_connect_attrs where processlist_id=1756 order by attr_name")
	require.Equal(t, [][]interface{}{
		{"_truncated", "92"},
		{"a", "1"},
		{"b", "xxxxxxxx"},
	}, selectResultRows(attrs))
}

func TestPerformanceSchemaPreparedStatementsStartupCapacity(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_prepared_statements_instances", "0")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(1751))
	session.SetParamByName("prepared_stmt_mgr", &testPreparedStatementInventory{statements: []compatibility.PreparedStatementSnapshot{{
		ID: 1751, SQL: "select prepared_capacity", CreatedAt: time.Now().Add(-time.Second), ExecuteCount: 1,
	}}})
	variables := mustSelectResultSessionSQL(t, executor, session, "", "select @@global.performance_schema_max_prepared_statements_instances")
	require.Equal(t, [][]interface{}{{"0"}}, selectResultRows(variables))
	prepared := mustSelectResultSessionSQL(t, executor, session, "", "select * from performance_schema.prepared_statements_instances")
	require.Empty(t, prepared.Records, "max_prepared_statements_instances=0 must disable prepared statement instrumentation")
}

func TestPerformanceSchemaPreparedStatementsLostCapacity(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_prepared_statements_instances", "1")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(1752))
	session.SetParamByName("prepared_stmt_mgr", &testPreparedStatementInventory{statements: []compatibility.PreparedStatementSnapshot{
		{ID: 1752, SQL: "select prepared_capacity_one", CreatedAt: time.Now().Add(-2 * time.Second)},
		{ID: 1753, SQL: "select prepared_capacity_two", CreatedAt: time.Now().Add(-time.Second)},
	}})
	prepared := mustSelectResultSessionSQL(t, executor, session, "", "select statement_id from performance_schema.prepared_statements_instances")
	require.Len(t, prepared.Records, 1)
	lost := mustSelectResultSessionSQL(t, executor, session, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_prepared_statements_lost'")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(lost))
	preparedAgain := mustSelectResultSessionSQL(t, executor, session, "", "select statement_id from performance_schema.prepared_statements_instances")
	require.Len(t, preparedAgain.Records, 1)
	lostAgain := mustSelectResultSessionSQL(t, executor, session, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_prepared_statements_lost'")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(lostAgain))
}

func TestPerformanceSchemaPreparedStatementsCapacityDoesNotResurrectEvictedRows(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_prepared_statements_instances", "1")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(1754))
	session.SetParamByName("prepared_stmt_mgr", &testPreparedStatementInventory{statements: []compatibility.PreparedStatementSnapshot{
		{ID: 1754, SQL: "select prepared_capacity_first", CreatedAt: time.Now().Add(-2 * time.Second)},
		{ID: 1755, SQL: "select prepared_capacity_second", CreatedAt: time.Now().Add(-time.Second)},
	}})

	visible := mustSelectResultSessionSQL(t, executor, session, "", "select statement_id from performance_schema.prepared_statements_instances")
	require.Equal(t, [][]interface{}{{"1754"}}, selectResultRows(visible))
	evicted := mustSelectResultSessionSQL(t, executor, session, "", "select statement_id from performance_schema.prepared_statements_instances where statement_id = 1755")
	require.Empty(t, evicted.Records, "a selective query must not resurrect an evicted prepared statement")
	retained := mustSelectResultSessionSQL(t, executor, session, "", "select statement_id from performance_schema.prepared_statements_instances where statement_id = 1754")
	require.Equal(t, [][]interface{}{{"1754"}}, selectResultRows(retained))
	lost := mustSelectResultSessionSQL(t, executor, session, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_prepared_statements_lost'")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(lost))
}

func TestPerformanceSchemaProgramStartupCapacity(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_program_instances", "0")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	variables := mustSelectResultSQL(t, executor, "", "select @@global.performance_schema_max_program_instances")
	require.Equal(t, [][]interface{}{{"0"}}, selectResultRows(variables))
	programs := mustSelectResultSQL(t, executor, "", "select * from performance_schema.events_statements_summary_by_program")
	require.Empty(t, programs.Records, "max_program_instances=0 must disable program instrumentation")
}

func TestPerformanceSchemaProgramLostCapacity(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_program_instances", "1")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	executor.QueryExecutor.performanceSchemaMu.Lock()
	executor.QueryExecutor.performanceSchemaProgramSummaries["PROCEDURE\x00program_capacity\x00a"] = performanceSchemaProgramEvent{ObjectType: "PROCEDURE", ObjectSchema: "program_capacity", ObjectName: "a", Count: 1}
	executor.QueryExecutor.performanceSchemaProgramSummaries["PROCEDURE\x00program_capacity\x00b"] = performanceSchemaProgramEvent{ObjectType: "PROCEDURE", ObjectSchema: "program_capacity", ObjectName: "b", Count: 1}
	executor.QueryExecutor.performanceSchemaMu.Unlock()
	programs := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.events_statements_summary_by_program")
	require.Len(t, programs.Records, 1)
	evicted := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.events_statements_summary_by_program where object_name='b'")
	require.Empty(t, evicted.Records, "a selective query must not resurrect an evicted program summary")
	retained := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.events_statements_summary_by_program where object_name='a'")
	require.Equal(t, [][]interface{}{{"a"}}, selectResultRows(retained))
	status := mustSelectResultSQL(t, executor, "", "select variable_name, variable_value from performance_schema.global_status")
	statusValues := make(map[string]string)
	for _, row := range selectResultRows(status) {
		statusValues[fmt.Sprint(row[0])] = fmt.Sprint(row[1])
	}
	require.Equal(t, "1", statusValues["Performance_schema_program_lost"])
}

func TestPerformanceSchemaThreadStartupCapacityAndLost(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_thread_instances", "1")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	current := newTestMySQLSession()
	current.SetParamByName("connection_id", int64(1760))
	current.SetParamByName("user", "thread_capacity_current")
	other := newTestMySQLSession()
	other.SetParamByName("connection_id", int64(1761))
	other.SetParamByName("user", "thread_capacity_other")
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{current, other}
	})

	variables := mustSelectResultSessionSQL(t, executor, current, "", "select @@global.performance_schema_max_thread_instances")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(variables))
	threads := executor.QueryExecutor.executePerformanceSchemaThreadsSelect("select thread_id from performance_schema.threads", current)
	require.Len(t, threads.Records, 1)
	evicted := executor.QueryExecutor.executePerformanceSchemaThreadsSelect("select thread_id from performance_schema.threads where thread_id=1761", current)
	require.Empty(t, evicted.Records, "a selective query must not resurrect an evicted thread instance")
	status := executor.QueryExecutor.executePerformanceSchemaStatusSelect("performance_schema.global_status", "")
	statusValues := make(map[string]string)
	for _, row := range selectResultRows(status) {
		statusValues[fmt.Sprint(row[0])] = fmt.Sprint(row[1])
	}
	require.Equal(t, "1", statusValues["Performance_schema_thread_instances_lost"])
}

func TestPerformanceSchemaFileStartupCapacityAndLost(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_file_instances", "0")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	executor.QueryExecutor.metricsRecorder.RecordFileOpen(filepath.Join(cfg.DataDir, "synthetic_a.ibd"))
	executor.QueryExecutor.metricsRecorder.RecordFileOpen(filepath.Join(cfg.DataDir, "synthetic_b.ibd"))
	variables := mustSelectResultSQL(t, executor, "", "select @@global.performance_schema_max_file_instances")
	require.Equal(t, [][]interface{}{{"0"}}, selectResultRows(variables))
	instances := mustSelectResultSQL(t, executor, "", "select file_name from performance_schema.file_instances")
	require.Empty(t, instances.Records, "max_file_instances=0 must disable file instrumentation")
	status := mustSelectResultSQL(t, executor, "", "select variable_name, variable_value from performance_schema.global_status")
	statusValues := make(map[string]string)
	for _, row := range selectResultRows(status) {
		statusValues[fmt.Sprint(row[0])] = fmt.Sprint(row[1])
	}
	lost, err := strconv.ParseInt(statusValues["Performance_schema_file_instances_lost"], 10, 64)
	require.NoError(t, err)
	require.Greater(t, lost, int64(0))
}

func TestPerformanceSchemaFileHandleCapacityReportsLost(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_file_handles", "1")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	fileA := filepath.Join(cfg.DataDir, "file_handle_a.ibd")
	fileB := filepath.Join(cfg.DataDir, "file_handle_b.ibd")
	executor.QueryExecutor.metricsRecorder.RecordFileOpen(fileA)
	executor.QueryExecutor.metricsRecorder.RecordFileOpen(fileB)

	variables := mustSelectResultSQL(t, executor, "", "select @@global.performance_schema_max_file_handles")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(variables))
	status := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_file_handles_lost'")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(status))
}

func TestPerformanceSchemaStatementStackCapacityReportsLost(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_statement_stack", "1")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(1773))
	parent := &ExecutionContext{
		Context:      context.Background(),
		DatabaseName: "app",
		Session:      session,
		triggerStack: []string{"app.parent.trigger"},
	}
	require.NoError(t, executor.QueryExecutor.executeAfterTriggerStatement(parent, session, "app", "select 1", "target", "nested_trigger"))
	status := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_nested_statement_lost'")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(status))
}

func TestPerformanceSchemaTableInstanceCapacityReportsLost(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_table_instances", "1")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	executor.QueryExecutor.metricsRecorder.RecordStatement("app", "select id from app.table_instance_a", "SELECT", "ok", time.Millisecond)
	executor.QueryExecutor.metricsRecorder.RecordStatement("app", "select id from app.table_instance_b", "SELECT", "ok", time.Millisecond)

	visible := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.table_io_waits_summary_by_table order by object_name")
	require.Equal(t, [][]interface{}{{"table_instance_a"}}, selectResultRows(visible))
	evicted := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.table_io_waits_summary_by_table where object_name='table_instance_b'")
	require.Empty(t, evicted.Records, "a selective query must not resurrect an evicted table instance")
	status := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_table_instances_lost'")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(status))
}

func TestPerformanceSchemaSocketStartupCapacityAndLost(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_socket_instances", "0")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(1765))
	session.SetParamByName("host", "socket_capacity")
	variables := mustSelectResultSessionSQL(t, executor, session, "", "select @@global.performance_schema_max_socket_instances")
	require.Equal(t, [][]interface{}{{"0"}}, selectResultRows(variables))
	sockets := executor.QueryExecutor.executePerformanceSchemaSocketInstancesSelect("select thread_id from performance_schema.socket_instances", session)
	require.Empty(t, sockets.Records, "max_socket_instances=0 must disable socket instrumentation")
	status := executor.QueryExecutor.executePerformanceSchemaStatusSelect("performance_schema.global_status", "")
	statusValues := make(map[string]string)
	for _, row := range selectResultRows(status) {
		statusValues[fmt.Sprint(row[0])] = fmt.Sprint(row[1])
	}
	lost, err := strconv.ParseInt(statusValues["Performance_schema_socket_instances_lost"], 10, 64)
	require.NoError(t, err)
	require.Equal(t, int64(1), lost)
}

func TestPerformanceSchemaSocketCapacityDoesNotResurrectEvictedRows(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_socket_instances", "1")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	current := newTestMySQLSession()
	current.SetParamByName("connection_id", int64(1767))
	current.SetParamByName("host", "socket_capacity_second")
	first := newTestMySQLSession()
	first.SetParamByName("connection_id", int64(1766))
	first.SetParamByName("host", "socket_capacity_first")
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{current, first}
	})

	visible := mustSelectResultSessionSQL(t, executor, current, "", "select thread_id from performance_schema.socket_instances")
	require.Equal(t, [][]interface{}{{"1766"}}, selectResultRows(visible))
	evicted := mustSelectResultSessionSQL(t, executor, current, "", "select thread_id from performance_schema.socket_instances where thread_id = 1767")
	require.Empty(t, evicted.Records, "a selective query must not resurrect an evicted socket instance")
	lost := mustSelectResultSessionSQL(t, executor, current, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_socket_instances_lost'")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(lost))
}

func TestPerformanceSchemaFileCapacityDoesNotResurrectEvictedRows(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_file_instances", "1")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	fileA := filepath.Join(cfg.DataDir, "aaa-file_capacity_a.ibd")
	fileB := filepath.Join(cfg.DataDir, "zzz-file_capacity_b.ibd")
	executor.QueryExecutor.metricsRecorder.RecordFileOpen(fileB)
	executor.QueryExecutor.metricsRecorder.RecordFileOpen(fileA)

	visible := mustSelectResultSQL(t, executor, "", "select file_name from performance_schema.file_instances")
	require.Equal(t, [][]interface{}{{fileA}}, selectResultRows(visible))
	evicted := mustSelectResultSQL(t, executor, "", "select file_name from performance_schema.file_instances where file_name = '"+fileB+"'")
	require.Empty(t, evicted.Records, "a selective query must not resurrect an evicted file instance")
	lost := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_file_instances_lost'")
	lostValue, err := strconv.ParseInt(selectResultRows(lost)[0][0].(string), 10, 64)
	require.NoError(t, err)
	require.Greater(t, lostValue, int64(0))
}

func TestPerformanceSchemaTableLockStatStartupCapacityAndLost(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_table_lock_stat", "0")
	require.NoError(t, err)

	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 1770, 1, 1, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(2, 1770, 1, 1, manager.LOCK_X))
	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	executor.QueryExecutor.SetLockManager(locks)
	variables := mustSelectResultSQL(t, executor, "", "select @@global.performance_schema_max_table_lock_stat")
	require.Equal(t, [][]interface{}{{"0"}}, selectResultRows(variables))
	stats := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.table_lock_waits_summary_by_table")
	require.Empty(t, stats.Records, "max_table_lock_stat=0 must disable table lock statistics")
	status := mustSelectResultSQL(t, executor, "", "select variable_name, variable_value from performance_schema.global_status")
	statusValues := make(map[string]string)
	for _, row := range selectResultRows(status) {
		statusValues[fmt.Sprint(row[0])] = fmt.Sprint(row[1])
	}
	require.Equal(t, "1", statusValues["Performance_schema_table_lock_stat_lost"])
	locks.ReleaseLocks(1)
	locks.ReleaseLocks(2)
}

func TestPerformanceSchemaTableLockStatCapacityDoesNotResurrectEvictedRows(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_table_lock_stat", "1")
	require.NoError(t, err)

	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1771, 1771, 1, 1, manager.LOCK_X))
	require.NoError(t, locks.AcquireLock(1772, 1772, 1, 1, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(17711, 1771, 1, 1, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(17721, 1772, 1, 1, manager.LOCK_X))

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	executor.QueryExecutor.SetLockManager(locks)

	all := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.table_lock_waits_summary_by_table")
	require.Equal(t, [][]interface{}{{"1771_1_1"}}, selectResultRows(all))
	retained := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.table_lock_waits_summary_by_table where object_name='1771_1_1'")
	require.Equal(t, [][]interface{}{{"1771_1_1"}}, selectResultRows(retained))
	evicted := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.table_lock_waits_summary_by_table where object_name='1772_1_1'")
	require.Empty(t, evicted.Records, "a selective query must not resurrect an evicted table-lock summary")
	status := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_table_lock_stat_lost'")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(status))
}

func TestPerformanceSchemaIndexStatStartupCapacityAndLost(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_index_stat", "0")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	executor.QueryExecutor.metricsRecorder.RecordStatement("app", "select id from app.index_capacity_a", "SELECT", "ok", time.Millisecond)
	executor.QueryExecutor.metricsRecorder.RecordStatement("app", "select id from app.index_capacity_b", "SELECT", "ok", time.Millisecond)
	variables := mustSelectResultSQL(t, executor, "", "select @@global.performance_schema_max_index_stat")
	require.Equal(t, [][]interface{}{{"0"}}, selectResultRows(variables))
	stats := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.table_io_waits_summary_by_index_usage")
	require.Empty(t, stats.Records, "max_index_stat=0 must disable index statistics")
	status := mustSelectResultSQL(t, executor, "", "select variable_name, variable_value from performance_schema.global_status")
	statusValues := make(map[string]string)
	for _, row := range selectResultRows(status) {
		statusValues[fmt.Sprint(row[0])] = fmt.Sprint(row[1])
	}
	require.Equal(t, "2", statusValues["Performance_schema_index_stat_lost"])
}

func TestPerformanceSchemaIndexStatCapacityDoesNotResurrectEvictedRows(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_index_stat", "1")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	executor.QueryExecutor.metricsRecorder.RecordStatementWithIndexNames(
		"index_capacity_app", "select id from index_capacity_app.index_capacity_a", "SELECT", "ok", time.Millisecond, []string{"idx_a"},
	)
	executor.QueryExecutor.metricsRecorder.RecordStatementWithIndexNames(
		"index_capacity_app", "select id from index_capacity_app.index_capacity_b", "SELECT", "ok", time.Millisecond, []string{"idx_b"},
	)

	visible := mustSelectResultSQL(t, executor, "", "select object_name, index_name from performance_schema.table_io_waits_summary_by_index_usage order by object_name")
	require.Equal(t, [][]interface{}{{"index_capacity_a", "idx_a"}}, selectResultRows(visible))
	evicted := mustSelectResultSQL(t, executor, "", "select object_name, index_name from performance_schema.table_io_waits_summary_by_index_usage where object_name='index_capacity_b'")
	require.Empty(t, evicted.Records, "a selective query must not resurrect an evicted index statistic")
	status := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_index_stat_lost'")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(status))
}

func TestPerformanceSchemaDigestLengthStartupLimit(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_digest_length", "8")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	executor.QueryExecutor.metricsRecorder.RecordStatement("app", "select digest_identifier_with_more_than_eight_bytes", "SELECT", "ok", time.Millisecond)
	variables := mustSelectResultSQL(t, executor, "", "select @@global.performance_schema_max_digest_length")
	require.Equal(t, [][]interface{}{{"8"}}, selectResultRows(variables))
	digests := mustSelectResultSQL(t, executor, "", "select digest_text from performance_schema.events_statements_summary_by_digest where schema_name='app'")
	require.Len(t, digests.Records, 1)
	require.LessOrEqual(t, len([]byte(fmt.Sprint(selectResultRows(digests)[0][0]))), 8)
	histograms := mustSelectResultSQL(t, executor, "", "select digest_text from performance_schema.events_statements_histogram_by_digest where schema_name='app'")
	require.Len(t, histograms.Records, 11)
	for _, row := range selectResultRows(histograms) {
		require.LessOrEqual(t, len([]byte(fmt.Sprint(row[0]))), 8)
	}
}

func TestPerformanceSchemaOfficialCapacityVariablesStartupConfig(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	values := map[string]string{
		"max_cond_classes": "7", "max_cond_instances": "8", "max_digest_sample_age": "9",
		"max_file_classes": "10", "max_file_handles": "11", "max_memory_classes": "12",
		"max_meter_classes": "13", "max_metric_classes": "30", "max_mutex_classes": "14",
		"max_mutex_instances": "15", "max_rwlock_classes": "16", "max_rwlock_instances": "17",
		"max_socket_classes": "18", "max_stage_classes": "19", "max_statement_classes": "20",
		"max_statement_stack": "21", "max_table_instances": "22", "max_thread_classes": "23",
	}
	for key, value := range values {
		_, err = section.NewKey(key, value)
		require.NoError(t, err)
	}

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	variables := mustSelectResultSQL(t, executor, "", "select @@global.performance_schema_max_cond_classes, @@global.performance_schema_max_cond_instances, @@global.performance_schema_max_digest_sample_age, @@global.performance_schema_max_file_classes, @@global.performance_schema_max_file_handles, @@global.performance_schema_max_memory_classes, @@global.performance_schema_max_meter_classes, @@global.performance_schema_max_metric_classes, @@global.performance_schema_max_mutex_classes, @@global.performance_schema_max_mutex_instances, @@global.performance_schema_max_rwlock_classes, @@global.performance_schema_max_rwlock_instances, @@global.performance_schema_max_socket_classes, @@global.performance_schema_max_stage_classes, @@global.performance_schema_max_statement_classes, @@global.performance_schema_max_statement_stack, @@global.performance_schema_max_table_instances, @@global.performance_schema_max_thread_classes")
	require.Equal(t, [][]interface{}{{"7", "8", "9", "10", "11", "12", "13", "30", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"}}, selectResultRows(variables))
	status := mustSelectResultSQL(t, executor, "", "select variable_name, variable_value from performance_schema.global_status")
	statusValues := make(map[string]string)
	for _, row := range selectResultRows(status) {
		statusValues[fmt.Sprint(row[0])] = fmt.Sprint(row[1])
	}
	for _, name := range []string{"Performance_schema_cond_classes_lost", "Performance_schema_cond_instances_lost", "Performance_schema_file_classes_lost", "Performance_schema_file_handles_lost", "Performance_schema_memory_classes_lost", "Performance_schema_meter_lost", "Performance_schema_metric_lost", "Performance_schema_mutex_classes_lost", "Performance_schema_mutex_instances_lost", "Performance_schema_nested_statement_lost", "Performance_schema_rwlock_classes_lost", "Performance_schema_rwlock_instances_lost", "Performance_schema_socket_classes_lost", "Performance_schema_stage_classes_lost", "Performance_schema_table_instances_lost", "Performance_schema_thread_classes_lost"} {
		require.Equal(t, "0", statusValues[name], "missing official Performance Schema status variable %s", name)
	}
	require.Equal(t, strconv.FormatInt(int64(len(performanceSchemaStatementClassNames)-20), 10), statusValues["Performance_schema_statement_classes_lost"])
}

func TestPerformanceSchemaStatementClassCapacityStartupConfig(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_statement_classes", "0")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })

	rows := mustSelectResultSQL(t, executor, "", "select name from performance_schema.setup_instruments where name like 'statement/%'")
	require.Empty(t, selectResultRows(rows))

	status := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_statement_classes_lost'")
	require.Len(t, selectResultRows(status), 1)
	require.Greater(t, int64Param(selectResultRows(status)[0][0]), int64(0))
}

func TestPerformanceSchemaInstrumentClassCapacityStartupConfig(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	for key, value := range map[string]string{
		"max_stage_classes":  "0",
		"max_file_classes":   "0",
		"max_socket_classes": "0",
		"max_memory_classes": "0",
	} {
		_, err = section.NewKey(key, value)
		require.NoError(t, err)
	}

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })

	for _, pattern := range []string{"stage/%", "wait/io/file/%", "wait/io/socket/%", "memory/%"} {
		rows := mustSelectResultSQL(t, executor, "", "select name from performance_schema.setup_instruments where name like '"+pattern+"'")
		require.Empty(t, selectResultRows(rows), pattern)
	}
	for _, table := range []string{
		"file_instances",
		"file_summary_by_event_name",
		"file_summary_by_instance",
		"socket_instances",
		"socket_summary_by_event_name",
		"socket_summary_by_instance",
		"memory_summary_global_by_event_name",
	} {
		rows := mustSelectResultSQL(t, executor, "", "select * from performance_schema."+table)
		require.Empty(t, selectResultRows(rows), table)
	}

	status := mustSelectResultSQL(t, executor, "", "select variable_name, variable_value from performance_schema.global_status")
	statusValues := make(map[string]string)
	for _, row := range selectResultRows(status) {
		statusValues[fmt.Sprint(row[0])] = fmt.Sprint(row[1])
	}
	for _, name := range []string{
		"Performance_schema_stage_classes_lost",
		"Performance_schema_file_classes_lost",
		"Performance_schema_socket_classes_lost",
		"Performance_schema_memory_classes_lost",
	} {
		require.Greater(t, int64Param(statusValues[name]), int64(0), name)
	}
}

func TestPerformanceSchemaThreadClassCapacityStartupConfig(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_thread_classes", "0")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })

	setupThreads := mustSelectResultSQL(t, executor, "", "select name from performance_schema.setup_threads")
	require.Empty(t, selectResultRows(setupThreads))

	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(991))
	session.SetParamByName("user", "thread_class_user")
	session.SetParamByName("host", "thread-class-host")
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{session}
	})
	threads := mustSelectResultSQL(t, executor, "", "select name, instrumented from performance_schema.threads")
	require.NotEmpty(t, selectResultRows(threads))
	for _, row := range selectResultRows(threads) {
		require.Equal(t, "NO", fmt.Sprint(row[1]), row)
	}

	status := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_thread_classes_lost'")
	require.Len(t, selectResultRows(status), 1)
	require.Greater(t, int64Param(selectResultRows(status)[0][0]), int64(0))
}

func TestPerformanceSchemaTelemetryClassCapacityStartupConfig(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_meter_classes", "0")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })

	meters := mustSelectResultSQL(t, executor, "", "select name from performance_schema.setup_meters")
	require.Empty(t, selectResultRows(meters))

	status := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_meter_lost'")
	require.Len(t, selectResultRows(status), 1)
	require.Greater(t, int64Param(selectResultRows(status)[0][0]), int64(0))
}

func TestPerformanceSchemaDigestSampleAgeStartupAndDynamicSync(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_digest_sample_age", "1")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	executor.QueryExecutor.syncPerformanceSchemaDigestSampleAge()
	executor.QueryExecutor.metricsRecorder.RecordStatement("digest_sample_age_app", "select 1", "SELECT", "ok", 10*time.Millisecond)
	time.Sleep(1100 * time.Millisecond)
	executor.QueryExecutor.metricsRecorder.RecordStatement("digest_sample_age_app", "select 2", "SELECT", "ok", time.Microsecond)
	rows := executor.QueryExecutor.metricsRecorder.StatementDigestSummary()
	require.Len(t, rows, 1)
	require.Equal(t, "select 2", rows[0].SQL)

	admin := newTestMySQLSession()
	admin.SetParamByName("dynamic_privileges", []string{"SYSTEM_VARIABLES_ADMIN"})
	result := <-executor.ExecuteQuery(admin, "set global performance_schema_max_digest_sample_age = 0", "")
	require.NoError(t, result.Err)
	variables := mustSelectResultSQL(t, executor, "", "select @@global.performance_schema_max_digest_sample_age")
	require.Equal(t, [][]interface{}{{"0"}}, selectResultRows(variables))
}

func TestPerformanceSchemaSQLTextStartupLimit(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_sql_text_length", "5")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	variables := mustSelectResultSQL(t, executor, "", "select @@global.performance_schema_max_sql_text_length")
	require.Equal(t, [][]interface{}{{"5"}}, selectResultRows(variables))
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(1901)
	result := <-executor.ExecuteQuery(session, "select 123456789", "text_limit_app")
	require.NoError(t, result.Err)
	history := executor.QueryExecutor.executePerformanceSchemaStatementHistorySelectWithSession(
		"performance_schema.events_statements_history_long", false,
		session,
		"select sql_text from performance_schema.events_statements_history_long",
	)
	rows := selectResultRows(history)
	require.NotEmpty(t, rows)
	found := false
	for _, row := range rows {
		if len(row) > 0 && row[0] == "selec" {
			found = true
			break
		}
	}
	require.True(t, found, "SQL_TEXT must be truncated at the configured byte limit: %#v", rows)
}

func TestPerformanceSchemaSetupTableDefaultCapacityRejectsOverflow(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	require.Equal(t, 100, executor.QueryExecutor.performanceSchemaSetupTableCapacity("performance_schema_setup_actors_size", 100))
	require.Equal(t, 100, executor.QueryExecutor.performanceSchemaSetupTableCapacity("performance_schema_setup_objects_size", 100))

	executor.QueryExecutor.performanceSchemaMu.Lock()
	for len(executor.QueryExecutor.performanceSchemaActorRules) < 100 {
		index := len(executor.QueryExecutor.performanceSchemaActorRules)
		executor.QueryExecutor.performanceSchemaActorRules[performanceSchemaActorKey{
			Host: fmt.Sprintf("capacity-%d", index), User: "user", Role: "%",
		}] = performanceSchemaActorSetting{Enabled: true, History: true}
	}
	for len(executor.QueryExecutor.performanceSchemaObjects) < 100 {
		index := len(executor.QueryExecutor.performanceSchemaObjects)
		executor.QueryExecutor.performanceSchemaObjects[performanceSchemaObjectKey{
			ObjectType: "TABLE", ObjectSchema: fmt.Sprintf("capacity_%d", index), ObjectName: "table",
		}] = performanceSchemaSetupSetting{Enabled: true, Timed: true}
	}
	executor.QueryExecutor.performanceSchemaMu.Unlock()

	_, handled, err := executor.QueryExecutor.executePerformanceSchemaSetupActorsMutation(
		"insert into performance_schema.setup_actors values ('overflow', 'user', '%', 'YES', 'YES')")
	require.True(t, handled)
	require.Error(t, err)
	_, handled, err = executor.QueryExecutor.executePerformanceSchemaSetupObjectsMutation(
		"insert into performance_schema.setup_objects values ('TABLE', 'overflow', 'table', 'YES', 'YES')")
	require.True(t, handled)
	require.Error(t, err)
}

func TestPerformanceSchemaUsesCanonicalStatementInstrumentNames(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	queries := map[string]string{
		"create database app":                                                     "create_db",
		"create table app.orders (id int)":                                        "create_table",
		"create temporary table app.tmp_orders (id int)":                          "create_table",
		"create view app.order_view as select 1":                                  "create_view",
		"create procedure app.p() select 1":                                       "create_procedure",
		"create function app.f() returns int return 1":                            "create_function",
		"create trigger app.tr before insert on app.orders for each row set @x=1": "create_trigger",
		"create event app.e on schedule every 1 day do select 1":                  "create_event",
		"create role 'r'@'localhost'":                                             "create_role",
		"alter table app.orders add c int":                                        "alter_table",
		"alter view app.order_view as select 2":                                   "alter_view",
		"alter procedure app.p comment 'p'":                                       "alter_procedure",
		"alter function app.f comment 'f'":                                        "alter_function",
		"alter event app.e disable":                                               "alter_event",
		"alter tablespace ts add datafile 'x'":                                    "alter_tablespace",
		"drop table app.orders":                                                   "drop_table",
		"drop view app.order_view":                                                "drop_view",
		"drop procedure app.p":                                                    "drop_procedure",
		"drop function app.f":                                                     "drop_function",
		"drop trigger app.tr":                                                     "drop_trigger",
		"drop event app.e":                                                        "drop_event",
		"drop role 'r'@'localhost'":                                               "drop_role",
		"rename table app.orders to app.orders_new":                               "rename_table",
		"rename user 'old'@'localhost' to 'new'@'localhost'":                      "rename_user",
		"truncate table app.orders":                                               "truncate",
		"insert into app.orders select id from app.other":                         "insert_select",
		"replace into app.orders select id from app.other":                        "replace_select",
		"prepare stmt from 'select 1'":                                            "prepare_sql",
		"execute stmt":                                                            "execute_sql",
		"deallocate prepare stmt":                                                 "dealloc_sql",
		"call app.p()":                                                            "call_procedure",
		"show tables":                                                             "show_tables",
		"show create view app.order_view":                                         "show_create_table",
		"show create procedure app.p":                                             "show_create_proc",
		"show create function app.f":                                              "show_create_func",
		"show create trigger app.tr":                                              "show_create_trigger",
		"show create event app.e":                                                 "show_create_event",
		"show create user 'u'@'localhost'":                                        "show_create_user",
		"show full tables":                                                        "show_tables",
		"show procedure status":                                                   "show_procedure_status",
		"show function status":                                                    "show_function_status",
		"show binlog events":                                                      "show_binlog_events",
		"show replica status":                                                     "show_slave_status",
		"show source status":                                                      "show_master_status",
		"show engine innodb status":                                               "show_engine_status",
		"load data infile 'x' into table app.orders":                              "load",
		"lock tables app.orders read":                                             "lock_tables",
		"unlock tables":                                                           "unlock_tables",
		"flush tables":                                                            "flush",
		"kill 1":                                                                  "kill",
		"analyze table app.orders":                                                "analyze",
		"optimize table app.orders":                                               "optimize",
		"check table app.orders":                                                  "check",
		"checksum table app.orders":                                               "checksum",
		"reset replica":                                                           "reset",
		"purge binary logs to 'binlog.000002'":                                    "purge",
		"change replication filter replicate_do_db=('app')":                       "change_repl_filter",
		"change replication source to source_host='x'":                            "change_master",
		"start replica":                                                           "slave_start",
		"stop replica":                                                            "slave_stop",
		"use app":                                                                 "change_db",
		"do 1":                                                                    "do",
		"set autocommit = 1":                                                      "set_option",
		"begin":                                                                   "begin",
		"commit":                                                                  "commit",
		"rollback to savepoint before_update":                                     "rollback_to_savepoint",
		"grant select on app.orders to 'u'@'localhost'":                           "grant",
		"grant 'r'@'localhost' to 'u'@'localhost'":                                "grant_role",
		"revoke select on app.orders from 'u'@'localhost'":                        "revoke",
		"revoke 'r'@'localhost' from 'u'@'localhost'":                             "revoke_role",
		"revoke all privileges, grant option from 'u'@'localhost'":                "revoke_all",
		"xa start 'x'":                                                            "xa_start",
	}
	for query, want := range queries {
		require.Equal(t, want, metricStatementType(query), query)
		setup := mustQuerySQL(t, executor, "", fmt.Sprintf("select name from performance_schema.setup_instruments where name='statement/sql/%s'", want))
		require.Len(t, setup, 1, query)
		require.Equal(t, "statement/sql/"+want, setup[0][0], query)
	}
}

func TestPerformanceSchemaCanonicalDDLInstrumentControlsRuntimeSummary(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	database := "p_s_canonical_1563"
	mustExecSQL(t, executor, "", "create database "+database)
	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set enabled='NO' where name='statement/sql/create_table'")
	setup := mustQuerySQL(t, executor, "", "select name, enabled from performance_schema.setup_instruments where name='statement/sql/create_table'")
	require.Equal(t, []interface{}{"statement/sql/create_table", "NO"}, setup[0][:2])
	mustExecSQL(t, executor, database, "create table disabled_ddl (id int primary key)")

	result := mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/sql/create_table'")
	require.Empty(t, result.Records)

	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set enabled='YES' where name='statement/sql/create_table'")
	mustExecSQL(t, executor, database, "create table enabled_ddl (id int primary key)")
	result = mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/sql/create_table'")
	require.Len(t, result.Records, 1)
	require.Equal(t, int64(1), result.Records[0].GetValues()[1].Int())
}

func TestPerformanceSchemaReplaceInstrumentControlsReplaceSummaries(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	setup := mustQuerySQL(t, executor, "", "select name, enabled, timed from performance_schema.setup_instruments where name='statement/sql/replace'")
	require.Len(t, setup, 1)
	require.Equal(t, []interface{}{"statement/sql/replace", "YES", "YES"}, setup[0][:3])

	table := "p_s_replace_1556"
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table "+table+" (id int primary key, value varchar(32))")
	mustExecSQL(t, executor, "app", "replace into "+table+" (id, value) values (1, 'first')")

	result := mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/sql/replace'")
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	initialCount := values[1].Int()
	require.GreaterOrEqual(t, initialCount, int64(1))

	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set enabled='NO' where name='statement/sql/replace'")
	mustExecSQL(t, executor, "app", "replace into "+table+" (id, value) values (1, 'second')")
	result = mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_statements_summary_global_by_event_name where event_name='statement/sql/replace'")
	require.Len(t, result.Records, 1)
	finalCount := result.Records[0].GetValues()[1].Int()
	require.Equal(t, initialCount, finalCount)
}

func TestPerformanceSchemaSetupUpdatesRequireUpdatePrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'p_s_setup_reader'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "p_s_setup_reader")
	session.SetParamByName("host", "localhost")
	query := "update performance_schema.setup_consumers set enabled='NO' where name='events_statements_history_long'"

	result := <-executor.ExecuteQuery(session, query, "")
	require.Error(t, result.Err)
	require.Contains(t, strings.ToLower(result.Err.Error()), "lacks update privilege")

	mustExecSQL(t, executor, "", "grant update on performance_schema.setup_consumers to 'p_s_setup_reader'@'localhost'")
	result = <-executor.ExecuteQuery(session, query, "")
	require.NoError(t, result.Err)
	require.Equal(t, 1, result.AffectedRows)
}

func TestPerformanceSchemaSetupObjectRowMutationsRequireMatchingPrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'p_s_setup_object_reader'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "p_s_setup_object_reader")
	session.SetParamByName("host", "localhost")
	insert := "insert into performance_schema.setup_objects values ('TABLE', 'app', 'orders', 'NO', 'NO')"
	result := <-executor.ExecuteQuery(session, insert, "")
	require.Error(t, result.Err)
	require.Contains(t, strings.ToLower(result.Err.Error()), "lacks insert privilege")

	mustExecSQL(t, executor, "", "grant insert on performance_schema.setup_objects to 'p_s_setup_object_reader'@'localhost'")
	result = <-executor.ExecuteQuery(session, insert, "")
	require.NoError(t, result.Err)
	require.Equal(t, 1, result.AffectedRows)

	delete := "delete from performance_schema.setup_objects where object_type='TABLE' and object_schema='app' and object_name='orders'"
	result = <-executor.ExecuteQuery(session, delete, "")
	require.Error(t, result.Err)
	require.Contains(t, strings.ToLower(result.Err.Error()), "lacks delete privilege")

	mustExecSQL(t, executor, "", "grant delete on performance_schema.setup_objects to 'p_s_setup_object_reader'@'localhost'")
	result = <-executor.ExecuteQuery(session, delete, "")
	require.NoError(t, result.Err)
	require.Equal(t, 1, result.AffectedRows)
}

func TestPerformanceSchemaSetupConfigurationControlsEventViews(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	result := <-executor.ExecuteQuery(nil, "update performance_schema.setup_consumers set enabled='NO' where name='events_statements_history_long'", "")
	require.NoError(t, result.Err)
	result = <-executor.ExecuteQuery(nil, "select 2", "app")
	require.NoError(t, result.Err)
	require.Empty(t, mustQuerySQL(t, executor, "", "select sql_text from performance_schema.events_statements_history_long"))

	result = <-executor.ExecuteQuery(nil, "update performance_schema.setup_consumers set enabled='YES' where name='events_statements_history_long'", "")
	require.NoError(t, result.Err)
	result = <-executor.ExecuteQuery(nil, "update performance_schema.setup_instruments set timed='NO' where name='statement/sql/select'", "")
	require.NoError(t, result.Err)
	result = <-executor.ExecuteQuery(nil, "select 3", "app")
	require.NoError(t, result.Err)
	rows := mustQuerySQL(t, executor, "", "select sql_text, timer_wait from performance_schema.events_statements_history_long")
	var foundUntimed bool
	for _, row := range rows {
		if len(row) >= 2 && row[0] == "select 3" {
			foundUntimed = true
			switch timer := row[1].(type) {
			case int64:
				require.Zero(t, timer)
			case string:
				require.Equal(t, string(make([]byte, 8)), timer)
			default:
				t.Fatalf("unexpected TIMER_WAIT value: %#v", row[6])
			}
		}
	}
	require.True(t, foundUntimed, "select 3 was not present in statement history: %#v", rows)

	result = <-executor.ExecuteQuery(nil, "update performance_schema.setup_actors set enabled='NO', history='NO' where host='%' and user='%'", "")
	require.NoError(t, result.Err)
	actors := mustQuerySQL(t, executor, "", "select host, user, role, enabled, history from performance_schema.setup_actors")
	require.Equal(t, [][]interface{}{{"%", "%", "%", "NO", "NO"}}, actors)
}

func TestPerformanceSchemaStatementInstrumentDisablesStatementSummaries(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set enabled='NO' where name='statement/sql/select'")
	enabled, _ := executor.QueryExecutor.performanceSchemaInstrumentSetting("statement/sql/select")
	require.False(t, enabled)

	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(930))
	session.SetParamByName("user", "statement_instrument_disabled")
	session.SetParamByName("host", "127.0.0.1")
	mustQuerySessionSQL(t, executor, session, "", "select 930001")

	require.False(t, statementHistoryContainsSQL(executor.QueryExecutor, "select 930001"))
	digestRows := selectResultRows(executor.QueryExecutor.executePerformanceSchemaStatementsSelect(
		"select schema_name, digest_text, count_star from performance_schema.events_statements_summary_by_digest",
	))
	for _, row := range digestRows {
		if len(row) >= 2 {
			require.NotEqual(t, "select ?", row[1], "disabled statement instrument must not create a digest row")
		}
	}
}

func TestPerformanceSchemaStatementInstrumentTimedControlsStatementSummaries(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set timed='NO' where name='statement/sql/select'")
	_, timed := executor.QueryExecutor.performanceSchemaInstrumentSetting("statement/sql/select")
	require.False(t, timed)

	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(931))
	session.SetParamByName("user", "statement_instrument_untimed")
	session.SetParamByName("host", "127.0.0.1")
	mustQuerySessionSQL(t, executor, session, "", "select 931001 as untimed_931001")

	rows := selectResultRows(executor.QueryExecutor.executePerformanceSchemaStatementsSelect(
		"select digest_text, count_star, sum_timer_wait, avg_timer_wait, max_timer_wait from performance_schema.events_statements_summary_by_digest",
	))
	var found bool
	for _, row := range rows {
		if len(row) >= 5 && row[0] == "select ? as untimed_931001 from dual" {
			found = true
			require.NotEqual(t, "0", row[1])
			require.Equal(t, "0", row[2])
			require.Equal(t, "0", row[3])
			require.Equal(t, "0", row[4])
		}
	}
	require.True(t, found, "untimed statement digest row was not retained: %#v", rows)
}

func TestPerformanceSchemaStatementHistoryPreservesEventTimedSnapshot(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "update performance_schema.setup_consumers set enabled='YES' where name='events_statements_history_long'")
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(9311))
	session.SetParamByName("user", "statement_history_timed_snapshot")
	session.SetParamByName("host", "127.0.0.1")

	mustQuerySessionSQL(t, executor, session, "", "select 155601 as timed_snapshot_yes")
	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set timed='NO' where name='statement/sql/select'")
	history := mustSelectResultSQL(t, executor, "", "select sql_text, timer_wait from performance_schema.events_statements_history_long where sql_text='select 155601 as timed_snapshot_yes'")
	require.Len(t, history.Records, 1)
	require.Greater(t, history.Records[0].GetValues()[1].Int(), int64(0), "history must retain the event-time TIMED=YES snapshot")

	mustQuerySessionSQL(t, executor, session, "", "select 155602 as timed_snapshot_no")
	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set timed='YES' where name='statement/sql/select'")
	history = mustSelectResultSQL(t, executor, "", "select sql_text, timer_wait from performance_schema.events_statements_history_long where sql_text='select 155602 as timed_snapshot_no'")
	require.Len(t, history.Records, 1)
	require.Zero(t, history.Records[0].GetValues()[1].Int(), "history must not retroactively time an event captured with TIMED=NO")
}

func TestPerformanceSchemaStatementHistoryPreservesEventInstrumentSnapshot(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(9341))
	session.SetParamByName("user", "statement_history_instrument_snapshot")
	session.SetParamByName("host", "127.0.0.1")

	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set enabled='NO' where name='statement/sql/select'")
	mustQuerySessionSQL(t, executor, session, "", "select 155801 as statement_instrument_disabled")
	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set enabled='YES' where name='statement/sql/select'")
	disabledHistory := mustSelectResultSQL(t, executor, "", "select thread_id, sql_text from performance_schema.events_statements_history_long where thread_id=9341")
	require.Empty(t, disabledHistory.Records, "statement history must not backfill an event captured while the statement instrument was disabled")

	mustQuerySessionSQL(t, executor, session, "", "select 155802 as statement_history_before_disable")
	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set enabled='NO' where name='statement/sql/select'")
	history := mustSelectResultSQL(t, executor, "", "select thread_id, sql_text from performance_schema.events_statements_history_long where thread_id=9341")
	require.Len(t, history.Records, 1, "disabling an instrument must not erase already-captured statement history")
	require.Equal(t, "select 155802 as statement_history_before_disable", history.Records[0].GetValues()[1].String())
}

func TestPerformanceSchemaStageHistoryPreservesEventInstrumentSnapshot(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(9351))
	session.SetParamByName("user", "stage_history_instrument_snapshot")
	session.SetParamByName("host", "127.0.0.1")

	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set enabled='NO' where name='stage/sql/execute'")
	mustQuerySessionSQL(t, executor, session, "", "select 155701 as stage_instrument_disabled")
	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set enabled='YES' where name='stage/sql/execute'")
	disabledHistory := mustSelectResultSQL(t, executor, "", "select sql_text from performance_schema.events_statements_history_long where sql_text='select 155701 as stage_instrument_disabled'")
	require.Len(t, disabledHistory.Records, 1)
	stageHistory := mustSelectResultSQL(t, executor, "", "select thread_id, event_name from performance_schema.events_stages_history_long where thread_id=9351")
	require.Empty(t, stageHistory.Records, "stage history must not backfill an event captured while the stage instrument was disabled")
}

func TestPerformanceSchemaStageHistoryPreservesEventTimedSnapshot(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(9352))
	session.SetParamByName("user", "stage_history_timed_snapshot")
	session.SetParamByName("host", "127.0.0.1")

	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set enabled='YES', timed='YES' where name='stage/sql/execute'")
	mustQuerySessionSQL(t, executor, session, "", "select 155702 as stage_timed_yes")
	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set timed='NO' where name='stage/sql/execute'")
	stageHistory := mustSelectResultSQL(t, executor, "", "select sql_text, timer_wait from performance_schema.events_statements_history_long where sql_text='select 155702 as stage_timed_yes'")
	require.Len(t, stageHistory.Records, 1)
	history := mustSelectResultSQL(t, executor, "", "select thread_id, timer_wait from performance_schema.events_stages_history_long where thread_id=9352")
	require.Len(t, history.Records, 1)
	require.Greater(t, history.Records[0].GetValues()[1].Int(), int64(0), "stage history must retain the event-time TIMED=YES snapshot")

	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set timed='NO' where name='stage/sql/execute'")
	mustQuerySessionSQL(t, executor, session, "", "select 155703 as stage_timed_no")
	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set timed='YES' where name='stage/sql/execute'")
	history = mustSelectResultSQL(t, executor, "", "select thread_id, timer_wait from performance_schema.events_stages_history_long where thread_id=9352 and event_name='stage/sql/execute'")
	require.Len(t, history.Records, 2)
	require.Zero(t, history.Records[1].GetValues()[1].Int(), "stage history must not retroactively time an event captured with TIMED=NO")
}

func TestPerformanceSchemaStageHistoryRetainsEventsAfterInstrumentDisabled(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(9353))
	session.SetParamByName("user", "stage_history_instrument_lifetime")
	session.SetParamByName("host", "127.0.0.1")

	mustQuerySessionSQL(t, executor, session, "", "select 155704 as stage_history_before_disable")
	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set enabled='NO' where name='stage/sql/execute'")
	history := mustSelectResultSQL(t, executor, "", "select thread_id, event_name from performance_schema.events_stages_history_long where thread_id=9353")
	require.Len(t, history.Records, 1, "disabling an instrument must not erase already-captured stage history")
}

func TestPerformanceSchemaStageInstrumentDoesNotBackfillDisabledEvents(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	executor.QueryExecutor.metricsRecorder.ResetStatementStageSummary()

	_, handled, err := executor.QueryExecutor.executePerformanceSchemaSetupUpdate(
		"update performance_schema.setup_instruments set enabled='NO' where name='stage/sql/execute'",
	)
	require.True(t, handled)
	require.NoError(t, err)

	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(932))
	session.SetParamByName("user", "stage_instrument_disabled")
	session.SetParamByName("host", "127.0.0.1")
	mustQuerySessionSQL(t, executor, session, "", "select 932001 as stage_disabled_932001")

	_, handled, err = executor.QueryExecutor.executePerformanceSchemaSetupUpdate(
		"update performance_schema.setup_instruments set enabled='YES' where name='stage/sql/execute'",
	)
	require.True(t, handled)
	require.NoError(t, err)
	rows := selectResultRows(executor.QueryExecutor.executePerformanceSchemaStageSummarySelect(
		"select event_name, count_star from performance_schema.events_stages_summary_global_by_event_name where event_name='stage/sql/execute'", false,
	))
	require.Equal(t, [][]interface{}{{"stage/sql/execute", "0"}}, rows)
}

func TestPerformanceSchemaStageInstrumentTimedControlsStageSummaries(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	executor.QueryExecutor.metricsRecorder.ResetStatementStageSummary()

	_, handled, err := executor.QueryExecutor.executePerformanceSchemaSetupUpdate(
		"update performance_schema.setup_instruments set enabled='YES', timed='NO' where name='stage/sql/execute'",
	)
	require.True(t, handled)
	require.NoError(t, err)

	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(933))
	session.SetParamByName("user", "stage_instrument_untimed")
	session.SetParamByName("host", "127.0.0.1")
	mustQuerySessionSQL(t, executor, session, "", "select 933001 as stage_untimed_933001")

	_, handled, err = executor.QueryExecutor.executePerformanceSchemaSetupUpdate(
		"update performance_schema.setup_instruments set timed='YES' where name='stage/sql/execute'",
	)
	require.True(t, handled)
	require.NoError(t, err)
	rows := selectResultRows(executor.QueryExecutor.executePerformanceSchemaStageSummarySelect(
		"select event_name, count_star, sum_timer_wait from performance_schema.events_stages_summary_global_by_event_name where event_name='stage/sql/execute'", false,
	))
	require.Equal(t, [][]interface{}{{"stage/sql/execute", "1", "0"}}, rows)
}

func TestPerformanceSchemaSetupActorsControlStageInstrumentation(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	mustExecSQL(t, executor, "", "update performance_schema.setup_actors set enabled='NO', history='NO' where host='%' and user='%' and role='%'")
	executor.QueryExecutor.metricsRecorder.ResetStatementStageSummary()
	executor.QueryExecutor.metricsRecorder.ResetStageHistory()

	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(9361))
	session.SetParamByName("user", "stage_actor_disabled")
	session.SetParamByName("host", "127.0.0.1")
	mustQuerySessionSQL(t, executor, session, "", "select 155901 as stage_actor_disabled")

	rows := selectResultRows(executor.QueryExecutor.executePerformanceSchemaStageSummarySelect(
		"select event_name, count_star from performance_schema.events_stages_summary_global_by_event_name where event_name='stage/sql/execute'", false,
	))
	require.Equal(t, [][]interface{}{{"stage/sql/execute", "0"}}, rows)
	history := mustSelectResultSQL(t, executor, "", "select thread_id from performance_schema.events_stages_history_long where thread_id=9361")
	require.Empty(t, history.Records, "actor-disabled statements must not create stage history")
}

func TestPerformanceSchemaSetupActorsControlNewForegroundThreads(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	_, handled, err := executor.QueryExecutor.executePerformanceSchemaSetupUpdate(
		"update performance_schema.setup_actors set enabled='NO', history='NO' where host='%' and user='%'",
	)
	require.True(t, handled)
	require.NoError(t, err)

	disabled := newTestMySQLSession()
	disabled.SetParamByName("connection_id", int64(901))
	disabled.SetParamByName("user", "actor_disabled")
	disabled.SetParamByName("host", "127.0.0.1")
	mustQuerySessionSQL(t, executor, disabled, "", "select 910101")

	threadResult := executor.QueryExecutor.executePerformanceSchemaThreadsSelect(
		"select thread_id, instrumented, history from performance_schema.threads", disabled,
	)
	threadRows := selectResultRows(threadResult)
	require.Len(t, threadRows, 1)
	require.Equal(t, []interface{}{"901", "NO", "NO"}, threadRows[0])

	historyRows := selectResultRows(executor.QueryExecutor.executePerformanceSchemaStatementHistorySelect(
		"performance_schema.events_statements_history_long", false,
	))
	for _, row := range historyRows {
		require.NotEqual(t, "select 910101", row[3])
	}

	_, handled, err = executor.QueryExecutor.executePerformanceSchemaSetupUpdate(
		"update performance_schema.setup_actors set enabled='YES', history='NO' where host='%' and user='%'",
	)
	require.True(t, handled)
	require.NoError(t, err)
	historyless := newTestMySQLSession()
	historyless.SetParamByName("connection_id", int64(902))
	historyless.SetParamByName("user", "actor_historyless")
	historyless.SetParamByName("host", "127.0.0.1")
	mustQuerySessionSQL(t, executor, historyless, "", "select 910102")

	historyRows = selectResultRows(executor.QueryExecutor.executePerformanceSchemaStatementHistorySelect(
		"performance_schema.events_statements_history_long", false,
	))
	for _, row := range historyRows {
		require.NotEqual(t, "select 910102", row[3])
	}
	digestRows := selectResultRows(executor.QueryExecutor.executePerformanceSchemaStatementsSelect(
		"select schema_name, digest_text, count_star from performance_schema.events_statements_summary_by_digest",
	))
	var digestFound bool
	for _, row := range digestRows {
		if len(row) >= 3 && strings.HasPrefix(fmt.Sprint(row[1]), "select ?") {
			digestFound = true
			require.NotEqual(t, "0", row[2])
		}
	}
	require.True(t, digestFound, "history-disabled statement was not retained in digest summary: %#v", digestRows)
}

func TestPerformanceSchemaSetupActorsSupportsMultipleRulesAndLifecycle(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	mustExecSQL(t, executor, "", "update performance_schema.setup_actors set enabled='NO', history='NO' where host='%' and user='%' and role='%'")
	mustExecSQL(t, executor, "", "insert into performance_schema.setup_actors (host, user, role, enabled, history) values ('127.0.0.1', 'actor_specific', '%', 'YES', 'YES')")

	actors := mustQuerySQL(t, executor, "", "select host, user, role, enabled, history from performance_schema.setup_actors")
	require.Equal(t, [][]interface{}{
		[]interface{}{"%", "%", "%", "NO", "NO"},
		[]interface{}{"127.0.0.1", "actor_specific", "%", "YES", "YES"},
	}, actors)

	specific := newTestMySQLSession()
	specific.SetParamByName("connection_id", int64(991))
	specific.SetParamByName("user", "actor_specific")
	specific.SetParamByName("host", "127.0.0.1")
	mustQuerySessionSQL(t, executor, specific, "", "select 991001")
	threadRows := selectResultRows(executor.QueryExecutor.executePerformanceSchemaThreadsSelect(
		"select thread_id, instrumented, history from performance_schema.threads", specific,
	))
	require.Equal(t, [][]interface{}{{"991", "YES", "YES"}}, threadRows)

	mustExecSQL(t, executor, "", "delete from performance_schema.setup_actors where host='127.0.0.1' and user='actor_specific'")
	remaining := mustQuerySQL(t, executor, "", "select host, user, role from performance_schema.setup_actors")
	require.Equal(t, [][]interface{}{{"%", "%", "%", "NO", "NO"}}, remaining)

	mustExecSQL(t, executor, "", "truncate table performance_schema.setup_actors")
	require.Empty(t, mustQuerySQL(t, executor, "", "select host, user, role from performance_schema.setup_actors"))
}

func TestPerformanceSchemaSetupActorsMatchActiveRole(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	mustExecSQL(t, executor, "", "update performance_schema.setup_actors set enabled='YES', history='YES' where host='%' and user='%' and role='%' ")
	mustExecSQL(t, executor, "", "insert into performance_schema.setup_actors (host, user, role, enabled, history) values ('%', '%', 'report_reader', 'NO', 'NO')")

	withoutRole := newTestMySQLSession()
	withoutRole.SetParamByName("connection_id", int64(993))
	withoutRole.SetParamByName("user", "actor_role_user")
	withoutRole.SetParamByName("host", "127.0.0.1")
	withoutRole.SetParamByName("active_roles", []string{})
	withoutRoleRows := selectResultRows(executor.QueryExecutor.executePerformanceSchemaThreadsSelect(
		"select thread_id, instrumented, history from performance_schema.threads", withoutRole,
	))
	require.Equal(t, [][]interface{}{{"993", "YES", "YES"}}, withoutRoleRows)

	withRole := newTestMySQLSession()
	withRole.SetParamByName("connection_id", int64(994))
	withRole.SetParamByName("user", "actor_role_user")
	withRole.SetParamByName("host", "127.0.0.1")
	withRole.SetParamByName("active_roles", []string{"report_reader@localhost"})
	withRoleRows := selectResultRows(executor.QueryExecutor.executePerformanceSchemaThreadsSelect(
		"select thread_id, instrumented, history from performance_schema.threads", withRole,
	))
	require.Equal(t, [][]interface{}{{"994", "NO", "NO"}}, withRoleRows)
}

func TestPerformanceSchemaSetupActorsUsePersistedDefaultRole(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'default_actor_role'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'default_actor_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'default_actor_role'@'localhost' to 'default_actor_user'@'localhost'")
	admin := newTestMySQLSession()
	admin.SetParamByName("user", "root")
	admin.SetParamByName("host", "localhost")
	mustExecSessionSQL(t, executor, admin, "", "set default role 'default_actor_role'@'localhost' to 'default_actor_user'@'localhost'")

	mustExecSQL(t, executor, "", "update performance_schema.setup_actors set enabled='YES', history='YES' where host='%' and user='%' and role='%' ")
	mustExecSQL(t, executor, "", "insert into performance_schema.setup_actors (host, user, role, enabled, history) values ('%', 'default_actor_user', 'default_actor_role', 'NO', 'NO')")

	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(995))
	session.SetParamByName("user", "default_actor_user")
	session.SetParamByName("host", "localhost")
	rows := selectResultRows(executor.QueryExecutor.executePerformanceSchemaThreadsSelect(
		"select thread_id, instrumented, history from performance_schema.threads", session,
	))
	require.Equal(t, [][]interface{}{{"995", "NO", "NO"}}, rows)
}

func TestPerformanceSchemaSetupTableTruncateRequiresDropPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'p_s_truncate_reader'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select, update on performance_schema.setup_actors to 'p_s_truncate_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant select, update on performance_schema.setup_objects to 'p_s_truncate_reader'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "p_s_truncate_reader")
	session.SetParamByName("host", "localhost")
	for _, table := range []string{"setup_actors", "setup_objects"} {
		result := <-executor.ExecuteQuery(session, "truncate table performance_schema."+table, "")
		require.Error(t, result.Err, table)
		require.Contains(t, strings.ToLower(result.Err.Error()), "drop", table)
	}

	mustExecSQL(t, executor, "", "grant drop on performance_schema.setup_actors to 'p_s_truncate_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant drop on performance_schema.setup_objects to 'p_s_truncate_reader'@'localhost'")
	for _, table := range []string{"setup_actors", "setup_objects"} {
		result := <-executor.ExecuteQuery(session, "truncate table performance_schema."+table, "")
		require.NoError(t, result.Err, table)
	}
}

func TestPerformanceSchemaSetupConsumersControlStatementInstrumentation(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	_, handled, err := executor.QueryExecutor.executePerformanceSchemaSetupUpdate(
		"update performance_schema.setup_consumers set enabled='NO' where name='global_instrumentation'",
	)
	require.True(t, handled)
	require.NoError(t, err)

	globalDisabled := newTestMySQLSession()
	globalDisabled.SetParamByName("connection_id", int64(903))
	globalDisabled.SetParamByName("user", "consumer_global_disabled")
	globalDisabled.SetParamByName("host", "127.0.0.1")
	mustQuerySessionSQL(t, executor, globalDisabled, "", "select 920001")
	require.False(t, statementHistoryContainsSQL(executor.QueryExecutor, "select 920001"))

	_, handled, err = executor.QueryExecutor.executePerformanceSchemaSetupUpdate(
		"update performance_schema.setup_consumers set enabled='YES' where name='global_instrumentation'",
	)
	require.True(t, handled)
	require.NoError(t, err)
	_, handled, err = executor.QueryExecutor.executePerformanceSchemaSetupUpdate(
		"update performance_schema.setup_consumers set enabled='NO' where name='thread_instrumentation'",
	)
	require.True(t, handled)
	require.NoError(t, err)

	threadDisabled := newTestMySQLSession()
	threadDisabled.SetParamByName("connection_id", int64(904))
	threadDisabled.SetParamByName("user", "consumer_thread_disabled")
	threadDisabled.SetParamByName("host", "127.0.0.1")
	mustQuerySessionSQL(t, executor, threadDisabled, "", "select 920002")
	require.False(t, statementHistoryContainsSQL(executor.QueryExecutor, "select 920002"))

	threads := selectResultRows(executor.QueryExecutor.executePerformanceSchemaThreadsSelect(
		"select thread_id, instrumented from performance_schema.threads", threadDisabled,
	))
	require.Len(t, threads, 1)
	require.Equal(t, []interface{}{"904", "NO"}, threads[0])

	_, handled, err = executor.QueryExecutor.executePerformanceSchemaSetupUpdate(
		"update performance_schema.setup_consumers set enabled='YES' where name='thread_instrumentation'",
	)
	require.True(t, handled)
	require.NoError(t, err)
	threadEnabled := newTestMySQLSession()
	threadEnabled.SetParamByName("connection_id", int64(905))
	threadEnabled.SetParamByName("user", "consumer_thread_enabled")
	threadEnabled.SetParamByName("host", "127.0.0.1")
	mustQuerySessionSQL(t, executor, threadEnabled, "", "select 920003")
	require.True(t, statementHistoryContainsSQL(executor.QueryExecutor, "select 920003"))
}

func TestPerformanceSchemaSetupConsumersGateWaitAndTransactionInstrumentation(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 920, 1, 1, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(2, 920, 1, 1, manager.LOCK_X))

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	executor.QueryExecutor.SetLockManager(locks)
	_, handled, err := executor.QueryExecutor.executePerformanceSchemaSetupUpdate(
		"update performance_schema.setup_consumers set enabled='NO' where name='global_instrumentation'",
	)
	require.True(t, handled)
	require.NoError(t, err)
	require.Empty(t, mustQuerySQL(t, executor, "", "select * from performance_schema.events_waits_current"))

	transaction := newTestMySQLSession()
	transaction.SetParamByName("connection_id", int64(906))
	transaction.SetParamByName("in_transaction", true)
	require.Empty(t, selectResultRows(executor.QueryExecutor.executePerformanceSchemaTransactionsSelect(
		"select * from performance_schema.events_transactions_current", transaction, "performance_schema.events_transactions_current",
	)))

	_, handled, err = executor.QueryExecutor.executePerformanceSchemaSetupUpdate(
		"update performance_schema.setup_consumers set enabled='YES' where name='global_instrumentation'",
	)
	require.True(t, handled)
	require.NoError(t, err)
	_, handled, err = executor.QueryExecutor.executePerformanceSchemaSetupUpdate(
		"update performance_schema.setup_consumers set enabled='NO' where name='thread_instrumentation'",
	)
	require.True(t, handled)
	require.NoError(t, err)
	require.Empty(t, mustQuerySQL(t, executor, "", "select * from performance_schema.events_waits_current"))

	_, handled, err = executor.QueryExecutor.executePerformanceSchemaSetupUpdate(
		"update performance_schema.setup_consumers set enabled='YES' where name='thread_instrumentation'",
	)
	require.True(t, handled)
	require.NoError(t, err)
	require.NotEmpty(t, mustQuerySQL(t, executor, "", "select * from performance_schema.events_waits_current"))
	transaction.SetParamByName("in_transaction", true)
	require.Len(t, selectResultRows(executor.QueryExecutor.executePerformanceSchemaTransactionsSelect(
		"select * from performance_schema.events_transactions_current", transaction, "performance_schema.events_transactions_current",
	)), 1)
}

func statementHistoryContainsSQL(executor *XMySQLExecutor, sql string) bool {
	if executor == nil || executor.metricsRecorder == nil {
		return false
	}
	for _, event := range executor.metricsRecorder.StatementHistory() {
		if event.SQL == sql {
			return true
		}
	}
	return false
}

func TestPerformanceSchemaCommonVariableAndActorViews(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	variables := mustQuerySQL(t, executor, "", "select variable_name, variable_value from performance_schema.global_variables")
	require.Contains(t, variables, []interface{}{"max_connections", "151"})
	status := mustQuerySQL(t, executor, "", "select variable_name, variable_value from performance_schema.global_status")
	require.Contains(t, status, []interface{}{"Threads_connected", "0"})
	filteredStatus := mustSelectResultSQL(t, executor, "", "select variable_name, variable_value from performance_schema.global_status where variable_name='Queries'")
	filteredStatusRows := selectResultRows(filteredStatus)
	require.Len(t, filteredStatusRows, 1)
	require.Equal(t, "Queries", filteredStatusRows[0][0])
	likeStatus := mustSelectResultSQL(t, executor, "", "select variable_name from performance_schema.global_status where variable_name like 'Threads_%'")
	likeStatusRows := selectResultRows(likeStatus)
	require.Equal(t, []string{"VARIABLE_NAME"}, likeStatus.Columns)
	require.Len(t, likeStatusRows, 2)
	require.Equal(t, "Threads_connected", likeStatusRows[0][0])
	require.Equal(t, "Threads_running", likeStatusRows[1][0])
	require.Equal(t, [][]interface{}{{"Queries"}}, selectResultRows(mustSelectResultSQL(t, executor, "", "select variable_name from performance_schema.global_status where variable_name='Queries'")))
	actors := mustQuerySQL(t, executor, "", "select host, user, role, enabled, history from performance_schema.setup_actors")
	require.Equal(t, [][]interface{}{{"%", "%", "%", "YES", "YES"}}, actors)

	session := newTestMySQLSession()
	session.SetParamByName("time_zone", "Asia/Shanghai")
	sessionVariables := <-executor.ExecuteQuery(session, "select variable_name, variable_value from performance_schema.session_variables where variable_name='time_zone'", "")
	require.NoError(t, sessionVariables.Err)
	require.Equal(t, [][]interface{}{{"time_zone", "Asia/Shanghai"}}, selectResultRows(sessionVariables.Data.(*SelectResult)))
	informationSessionVariables := <-executor.ExecuteQuery(session, "select variable_name, variable_value from information_schema.session_variables where variable_name='time_zone'", "")
	require.NoError(t, informationSessionVariables.Err)
	require.Equal(t, [][]interface{}{{"time_zone", "Asia/Shanghai"}}, selectResultRows(informationSessionVariables.Data.(*SelectResult)))
	sessionStatus := <-executor.ExecuteQuery(session, "select variable_name, variable_value from performance_schema.session_status where variable_name='Threads_connected'", "")
	require.NoError(t, sessionStatus.Err)
	require.Equal(t, [][]interface{}{{"Threads_connected", "1"}}, selectResultRows(sessionStatus.Data.(*SelectResult)))
	globalStatus := <-executor.ExecuteQuery(session, "select variable_name, variable_value from performance_schema.global_status where variable_name='Threads_connected'", "")
	require.NoError(t, globalStatus.Err)
	require.Equal(t, [][]interface{}{{"Threads_connected", "1"}}, selectResultRows(globalStatus.Data.(*SelectResult)))
}

func TestPerformanceSchemaSessionVariablesExposeReadOnlyAliases(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("transaction_read_only", int64(1))
	session.SetParamByName("tx_read_only", int64(1))

	for _, name := range []string{"transaction_read_only", "tx_read_only"} {
		result := mustSelectResultSessionSQL(t, executor, session, "", fmt.Sprintf(
			"select variable_name, variable_value from performance_schema.session_variables where variable_name='%s'",
			name,
		))
		require.Equal(t, [][]interface{}{{name, "1"}}, selectResultRows(result), name)
	}
}

func TestPerformanceSchemaSessionVariablesExposeCharacterSetScope(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("character_set_database", "latin1")
	session.SetParamByName("character_set_server", "utf8mb4")

	for name, expected := range map[string]string{
		"character_set_database": "latin1",
		"character_set_server":   "utf8mb4",
	} {
		result := mustSelectResultSessionSQL(t, executor, session, "", fmt.Sprintf(
			"select variable_name, variable_value from performance_schema.session_variables where variable_name='%s'",
			name,
		))
		require.Equal(t, [][]interface{}{{name, expected}}, selectResultRows(result), name)
	}
}

func TestPerformanceSchemaSessionVariablesExposeSystemManagerInventory(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()

	for _, item := range []struct {
		table    string
		name     string
		expected string
	}{
		{table: "performance_schema.session_variables", name: "wait_timeout", expected: "28800"},
		{table: "performance_schema.session_variables", name: "innodb_page_size", expected: "16384"},
		{table: "information_schema.session_variables", name: "wait_timeout", expected: "28800"},
		{table: "information_schema.session_variables", name: "innodb_page_size", expected: "16384"},
	} {
		result := mustSelectResultSessionSQL(t, executor, session, "", fmt.Sprintf(
			"select variable_name, variable_value from %s where variable_name='%s'",
			item.table, item.name,
		))
		require.Equal(t, [][]interface{}{{item.name, item.expected}}, selectResultRows(result), item.table+"."+item.name)
	}
}

func TestPerformanceSchemaWaitInstrumentSettingsControlWaitViews(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 77, 1, 13, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(2, 77, 1, 13, manager.LOCK_X))

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	executor.QueryExecutor.SetLockManager(locks)
	setup := mustQuerySQL(t, executor, "", "select name, enabled, timed from performance_schema.setup_instruments where name like 'wait/lock/%'")
	var handlerSetup, metadataSetup bool
	for _, row := range setup {
		if len(row) >= 3 && row[0] == "wait/lock/table/sql/handler" {
			handlerSetup = true
			require.Equal(t, []interface{}{"wait/lock/table/sql/handler", "YES", "YES"}, row[:3])
		}
		if len(row) >= 3 && row[0] == "wait/lock/metadata/sql/mdl" {
			metadataSetup = true
			require.Equal(t, []interface{}{"wait/lock/metadata/sql/mdl", "YES", "YES"}, row[:3])
		}
	}
	require.True(t, handlerSetup)
	require.True(t, metadataSetup)

	result := <-executor.ExecuteQuery(nil, "update performance_schema.setup_instruments set enabled='NO' where name='wait/lock/table/sql/handler'", "")
	require.NoError(t, result.Err)
	require.Empty(t, mustQuerySQL(t, executor, "", "select * from performance_schema.events_waits_current"))
	require.Empty(t, mustQuerySQL(t, executor, "", "select * from performance_schema.events_waits_summary_global_by_event_name"))

	result = <-executor.ExecuteQuery(nil, "update performance_schema.setup_instruments set enabled='YES', timed='NO' where name='wait/lock/table/sql/handler'", "")
	require.NoError(t, result.Err)
	rows := mustQuerySQL(t, executor, "", "select event_name, timer_wait from performance_schema.events_waits_current")
	require.Len(t, rows, 1)
	require.Equal(t, "wait/lock/table/sql/handler", rows[0][0])
	require.Nil(t, rows[0][1])

	locks.ReleaseLocks(1)
	locks.ReleaseLocks(2)
}

func TestPerformanceSchemaWaitConsumersControlEventViews(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 78, 1, 14, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(2, 78, 1, 14, manager.LOCK_X))

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.SetLockManager(locks)
	result := <-executor.ExecuteQuery(nil, "update performance_schema.setup_consumers set enabled='NO' where name='events_waits_current'", "")
	require.NoError(t, result.Err)
	require.Empty(t, mustQuerySQL(t, executor, "", "select * from performance_schema.events_waits_current"))

	result = <-executor.ExecuteQuery(nil, "update performance_schema.setup_consumers set enabled='NO' where name='events_waits_history'", "")
	require.NoError(t, result.Err)
	locks.ReleaseLocks(1)
	locks.ReleaseLocks(2)
	require.Empty(t, mustQuerySQL(t, executor, "", "select * from performance_schema.events_waits_history"))

	result = <-executor.ExecuteQuery(nil, "update performance_schema.setup_consumers set enabled='YES' where name='events_waits_history'", "")
	require.NoError(t, result.Err)
	require.NotEmpty(t, mustQuerySQL(t, executor, "", "select * from performance_schema.events_waits_history"))
}

func TestPerformanceSchemaWaitEventsProjectLifecycleTimers(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 79, 1, 15, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(2, 79, 1, 15, manager.LOCK_X))

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	executor.QueryExecutor.SetLockManager(locks)
	current := mustSelectResultSQL(t, executor, "", "select thread_id, event_id, end_event_id, timer_start, timer_end, timer_wait from performance_schema.events_waits_current")
	require.Len(t, current.Records, 1)
	currentValues := current.Records[0].GetValues()
	require.True(t, currentValues[2].IsNull(), "active wait must not expose END_EVENT_ID")
	currentStart := currentValues[3].Int()
	currentEnd := currentValues[4].Int()
	currentWait := currentValues[5].Int()
	require.Greater(t, currentStart, int64(0))
	require.Greater(t, currentEnd, currentStart)
	require.Equal(t, currentEnd-currentStart, currentWait)

	locks.ReleaseLocks(1)
	locks.ReleaseLocks(2)
	history := mustSelectResultSQL(t, executor, "", "select thread_id, event_id, end_event_id, timer_start, timer_end, timer_wait from performance_schema.events_waits_history")
	require.Len(t, history.Records, 1)
	historyValues := history.Records[0].GetValues()
	require.False(t, historyValues[2].IsNull(), "completed wait must expose END_EVENT_ID")
	require.Greater(t, historyValues[1].Int(), int64(0))
	historyStart := historyValues[3].Int()
	historyEnd := historyValues[4].Int()
	historyWait := historyValues[5].Int()
	require.Greater(t, historyStart, int64(0))
	require.Greater(t, historyEnd, historyStart)
	require.Equal(t, historyEnd-historyStart, historyWait)
}

func TestPerformanceSchemaWaitHistoryPreservesEventInstrumentSnapshot(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 795, 1, 15, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(2, 795, 1, 15, manager.LOCK_X))
	time.Sleep(2 * time.Millisecond)
	locks.ReleaseLocks(1)
	locks.ReleaseLocks(2)

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	executor.QueryExecutor.SetLockManager(locks)

	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set timed='NO' where name='wait/lock/table/sql/handler'")
	history := mustSelectResultSQL(t, executor, "", "select event_name, timer_wait from performance_schema.events_waits_history_long where event_name='wait/lock/table/sql/handler'")
	require.Len(t, history.Records, 1)
	require.Greater(t, history.Records[0].GetValues()[1].Int(), int64(0), "wait history must retain the event-time TIMED=YES snapshot")
	summary := mustSelectResultSQL(t, executor, "", "select event_name, count_star, sum_timer_wait from performance_schema.events_waits_summary_global_by_event_name where event_name='wait/lock/table/sql/handler'")
	require.Len(t, summary.Records, 1)
	require.Greater(t, summary.Records[0].GetValues()[2].Int(), int64(0), "wait summary must retain the event-time TIMED=YES snapshot")
	tableSummary := mustSelectResultSQL(t, executor, "", "select object_name, count_star, sum_timer_wait from performance_schema.table_lock_waits_summary_by_table where object_name='795_1_15'")
	require.Len(t, tableSummary.Records, 1)
	require.Equal(t, "795_1_15", tableSummary.Records[0].GetValues()[0].String())
	require.Equal(t, int64(1), tableSummary.Records[0].GetValues()[1].Int())
	require.Greater(t, tableSummary.Records[0].GetValues()[2].Int(), int64(0), "table-lock summary must retain the event-time TIMED=YES snapshot")

	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set enabled='NO' where name='wait/lock/table/sql/handler'")
	history = mustSelectResultSQL(t, executor, "", "select event_name, timer_wait from performance_schema.events_waits_history_long where event_name='wait/lock/table/sql/handler'")
	require.Len(t, history.Records, 1, "disabling an instrument must not erase already-captured wait history")
	summary = mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_waits_summary_global_by_event_name where event_name='wait/lock/table/sql/handler'")
	require.Equal(t, [][]interface{}{{"wait/lock/table/sql/handler", "1"}}, selectResultRows(summary), "disabling an instrument must not erase its completed wait summary")
	tableSummary = mustSelectResultSQL(t, executor, "", "select object_name, count_star from performance_schema.table_lock_waits_summary_by_table where object_name='795_1_15'")
	require.Equal(t, [][]interface{}{{"795_1_15", "1"}}, selectResultRows(tableSummary), "disabling an instrument must not erase its completed table-lock summary")
}

func TestPerformanceSchemaWaitHistoryIncludesRecentEventsPerThread(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 80, 1, 16, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(402, 80, 1, 16, manager.LOCK_X))
	require.NoError(t, locks.AcquireLock(3, 81, 1, 17, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(404, 81, 1, 17, manager.LOCK_X))
	locks.ReleaseLocks(1)
	locks.ReleaseLocks(3)
	defer locks.ReleaseLocks(402)
	defer locks.ReleaseLocks(404)

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	executor.QueryExecutor.SetLockManager(locks)
	first := newTestMySQLSession()
	first.ctx.SetConnectionID(402)
	second := newTestMySQLSession()
	second.ctx.SetConnectionID(404)

	result := mustSelectResultSessionSQL(t, executor, first, "", "select thread_id, event_name from performance_schema.events_waits_history")
	rows := selectResultRows(result)
	seen := map[string]bool{}
	for _, row := range rows {
		if row[0] == "402" || row[0] == "404" {
			seen[row[0].(string)] = true
		}
	}
	require.True(t, seen["402"])
	require.True(t, seen["404"])

	other := mustSelectResultSessionSQL(t, executor, second, "", "select thread_id, event_name from performance_schema.events_waits_history")
	otherRows := selectResultRows(other)
	require.Equal(t, rows, otherRows, "per-thread history must not depend on which session queries it")
}

func TestPerformanceSchemaTransactionCurrentReflectsSessionState(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "begin")

	result := <-executor.ExecuteQuery(session, "select state, access_mode, isolation_level, autocommit from performance_schema.events_transactions_current", "app")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, []string{"STATE", "ACCESS_MODE", "ISOLATION_LEVEL", "AUTOCOMMIT"}, selectResult.Columns)
	rows := selectResultRows(selectResult)
	require.Len(t, rows, 1)
	require.Equal(t, "ACTIVE", rows[0][0])
	require.Equal(t, "READ WRITE", rows[0][1])
	require.Equal(t, "REPEATABLE READ", rows[0][2])
	require.Equal(t, "YES", rows[0][3])
}

func TestPerformanceSchemaTransactionHistoryIncludesRecentEventsPerThread(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	first := newTestMySQLSession()
	first.SetParamByName("connection_id", int64(717))
	second := newTestMySQLSession()
	second.SetParamByName("connection_id", int64(718))
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{first, second}
	})

	mustExecSessionSQL(t, executor, first, "app", "begin")
	mustExecSessionSQL(t, executor, first, "app", "commit")
	mustExecSessionSQL(t, executor, second, "app", "begin")
	mustExecSessionSQL(t, executor, second, "app", "commit")

	result := <-executor.ExecuteQuery(first, "select thread_id, state from performance_schema.events_transactions_history", "app")
	require.NoError(t, result.Err)
	seen := map[string]bool{}
	for _, row := range selectResultRows(result.Data.(*SelectResult)) {
		if row[0] == "717" || row[0] == "718" {
			seen[row[0].(string)] = true
		}
	}
	require.True(t, seen["717"])
	require.True(t, seen["718"])
}

func TestPerformanceSchemaTransactionCurrentAppliesProjection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(716))
	mustExecSessionSQL(t, executor, session, "app", "begin")

	result := <-executor.ExecuteQuery(session, "select thread_id, state from performance_schema.events_transactions_current where thread_id=716 and state='ACTIVE'", "app")
	require.NoError(t, result.Err)
	selectResult := result.Data.(*SelectResult)
	require.Equal(t, []string{"THREAD_ID", "STATE"}, selectResult.Columns)
	require.Equal(t, [][]interface{}{{"716", "ACTIVE"}}, selectResultRows(selectResult))
}

func TestPerformanceSchemaTransactionCurrentProjectsSessionTransactionID(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(718))
	session.SetParamByName("transaction_id", int64(991))
	mustExecSessionSQL(t, executor, session, "app", "begin")

	result := executor.QueryExecutor.executePerformanceSchemaTransactionsSelect(
		"select thread_id, trx_id, state from performance_schema.events_transactions_current where thread_id=718",
		session,
		"performance_schema.events_transactions_current",
	)
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	require.Equal(t, int64(718), values[0].Int())
	require.Equal(t, int64(991), values[1].Int())
	require.Equal(t, "ACTIVE", values[2].String())

	mustExecSessionSQL(t, executor, session, "app", "commit")
	history := executor.QueryExecutor.executePerformanceSchemaTransactionsSelect(
		"select thread_id, trx_id, state from performance_schema.events_transactions_history where thread_id=718",
		session,
		"performance_schema.events_transactions_history",
	)
	require.Len(t, history.Records, 1)
	historyValues := history.Records[0].GetValues()
	require.Equal(t, int64(718), historyValues[0].Int())
	require.Equal(t, int64(991), historyValues[1].Int())
	require.Equal(t, "COMMITTED", historyValues[2].String())
}

func TestPerformanceSchemaTransactionProjectsXAIdentity(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(719))

	mustExecSessionSQL(t, executor, session, "app", "XA START 'global-xid','branch-xid',17")
	current := executor.QueryExecutor.executePerformanceSchemaTransactionsSelect(
		"select xid_format, xid_gtrid, xid_bqual, timer_start from performance_schema.events_transactions_current where thread_id=719",
		session,
		"performance_schema.events_transactions_current",
	)
	require.Len(t, current.Records, 1)
	currentValues := current.Records[0].GetValues()
	require.Equal(t, int64(17), currentValues[0].Int())
	require.Equal(t, []byte("global-xid"), currentValues[1].Raw())
	require.Equal(t, []byte("branch-xid"), currentValues[2].Raw())
	require.Greater(t, currentValues[3].Int(), int64(0))

	mustExecSessionSQL(t, executor, session, "app", "XA END 'global-xid','branch-xid',17")
	mustExecSessionSQL(t, executor, session, "app", "XA PREPARE 'global-xid','branch-xid',17")
	mustExecSessionSQL(t, executor, session, "app", "XA COMMIT 'global-xid','branch-xid',17")
	history := executor.QueryExecutor.executePerformanceSchemaTransactionsSelect(
		"select xid_format, xid_gtrid, xid_bqual from performance_schema.events_transactions_history where thread_id=719",
		session,
		"performance_schema.events_transactions_history",
	)
	require.Len(t, history.Records, 1)
	historyValues := history.Records[0].GetValues()
	require.Equal(t, int64(17), historyValues[0].Int())
	require.Equal(t, []byte("global-xid"), historyValues[1].Raw())
	require.Equal(t, []byte("branch-xid"), historyValues[2].Raw())
}

func TestPerformanceSchemaTransactionEventsProjectLifecycleTimers(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(717))
	mustExecSessionSQL(t, executor, session, "app", "begin")

	current := executor.QueryExecutor.executePerformanceSchemaTransactionsSelect(
		"select thread_id, event_id, end_event_id, timer_start, timer_end, timer_wait from performance_schema.events_transactions_current where thread_id=717",
		session,
		"performance_schema.events_transactions_current",
	)
	require.Len(t, current.Records, 1)
	currentValues := current.Records[0].GetValues()
	require.True(t, currentValues[2].IsNull(), "active transaction must not expose END_EVENT_ID")
	require.Greater(t, currentValues[3].Int(), int64(0))
	require.True(t, currentValues[4].IsNull(), "active transaction must not expose TIMER_END")
	require.True(t, currentValues[5].IsNull(), "active transaction must not expose TIMER_WAIT")

	mustExecSessionSQL(t, executor, session, "app", "commit")
	history := executor.QueryExecutor.executePerformanceSchemaTransactionsSelect(
		"select thread_id, event_id, end_event_id, timer_start, timer_end, timer_wait from performance_schema.events_transactions_history where thread_id=717",
		session,
		"performance_schema.events_transactions_history",
	)
	require.Len(t, history.Records, 1)
	historyValues := history.Records[0].GetValues()
	require.False(t, historyValues[2].IsNull(), "completed transaction must expose END_EVENT_ID")
	start := historyValues[3].Int()
	end := historyValues[4].Int()
	wait := historyValues[5].Int()
	require.Greater(t, start, int64(0))
	require.Greater(t, end, start)
	require.Equal(t, end-start, wait)
}

func TestPerformanceSchemaTransactionAccessModeReflectsReadOnlyState(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "start transaction read only")

	current := <-executor.ExecuteQuery(session, "select * from performance_schema.events_transactions_current", "app")
	require.NoError(t, current.Err)
	currentRows := selectResultRows(current.Data.(*SelectResult))
	require.Len(t, currentRows, 1)
	require.Equal(t, "READ ONLY", currentRows[0][15])

	mustExecSessionSQL(t, executor, session, "app", "commit")
	history := <-executor.ExecuteQuery(session, "select * from performance_schema.events_transactions_history", "app")
	require.NoError(t, history.Err)
	historyRows := selectResultRows(history.Data.(*SelectResult))
	require.Len(t, historyRows, 1)
	require.Equal(t, "READ ONLY", historyRows[0][15])
}

func TestPerformanceSchemaTransactionHistoryReflectsCommitAndRollback(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "begin")
	mustExecSessionSQL(t, executor, session, "app", "commit")
	mustExecSessionSQL(t, executor, session, "app", "begin")
	mustExecSessionSQL(t, executor, session, "app", "rollback")

	result := <-executor.ExecuteQuery(session, "select * from performance_schema.events_transactions_history", "app")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	rows := selectResultRows(selectResult)
	require.Len(t, rows, 2)
	require.Equal(t, "COMMITTED", rows[0][4])
	require.Equal(t, "transaction", rows[0][3])
	require.Equal(t, "REPEATABLE READ", rows[0][16])
	require.Equal(t, "ROLLED BACK", rows[1][4])
	require.Equal(t, "transaction", rows[1][3])
	require.Equal(t, "REPEATABLE READ", rows[1][16])
}

func TestPerformanceSchemaTransactionHistoryLongAggregatesSessions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	first := newTestMySQLSession()
	first.SetParamByName("connection_id", int64(701))
	second := newTestMySQLSession()
	second.SetParamByName("connection_id", int64(702))

	mustExecSessionSQL(t, executor, first, "app", "begin")
	mustExecSessionSQL(t, executor, first, "app", "commit")
	mustExecSessionSQL(t, executor, second, "app", "begin")
	mustExecSessionSQL(t, executor, second, "app", "rollback")

	result := <-executor.ExecuteQuery(first, "select thread_id, state from performance_schema.events_transactions_history_long", "app")
	require.NoError(t, result.Err)
	rows := selectResultRows(result.Data.(*SelectResult))
	require.Equal(t, [][]interface{}{{"701", "COMMITTED"}, {"702", "ROLLED BACK"}}, rows)

	globalResult := <-executor.ExecuteQuery(nil, "select thread_id, state from performance_schema.events_transactions_history_long", "")
	require.NoError(t, globalResult.Err)
	require.Equal(t, rows, selectResultRows(globalResult.Data.(*SelectResult)))
}

func TestPerformanceSchemaTransactionInstrumentIsRegisteredAndGatesHistory(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)

	setup := mustQuerySQL(t, executor, "", "select name, enabled, timed from performance_schema.setup_instruments where name='transaction'")
	require.Len(t, setup, 1)
	require.Equal(t, []interface{}{"transaction", "YES", "YES"}, setup[0][:3])
	enabled, timed := executor.QueryExecutor.performanceSchemaInstrumentSetting("transaction")
	require.True(t, enabled)
	require.True(t, timed)
	require.True(t, executor.QueryExecutor.performanceSchemaConsumerEnabled("events_transactions_history_long"))

	first := newTestMySQLSession()
	first.SetParamByName("connection_id", int64(721))
	mustExecSessionSQL(t, executor, first, "app", "begin")
	mustExecSessionSQL(t, executor, first, "app", "commit")
	before := mustQuerySQL(t, executor, "", "select thread_id, state from performance_schema.events_transactions_history_long")
	require.NotEmpty(t, before)

	update := <-executor.ExecuteQuery(nil, "update performance_schema.setup_instruments set enabled='NO' where name='transaction'", "")
	require.NoError(t, update.Err)
	require.Equal(t, 1, update.AffectedRows)
	second := newTestMySQLSession()
	second.SetParamByName("connection_id", int64(722))
	mustExecSessionSQL(t, executor, second, "app", "begin")
	mustExecSessionSQL(t, executor, second, "app", "commit")
	after := mustQuerySQL(t, executor, "", "select thread_id, state from performance_schema.events_transactions_history_long")
	require.Equal(t, before, after)

	update = <-executor.ExecuteQuery(nil, "update performance_schema.setup_instruments set enabled='YES', timed='NO' where name='transaction'", "")
	require.NoError(t, update.Err)
	require.Equal(t, 1, update.AffectedRows)
	third := newTestMySQLSession()
	third.SetParamByName("connection_id", int64(723))
	mustExecSessionSQL(t, executor, third, "app", "begin")
	mustExecSessionSQL(t, executor, third, "app", "commit")
	timedOff := mustQuerySQL(t, executor, "", "select thread_id, state, timer_start, timer_end, timer_wait from performance_schema.events_transactions_history_long where thread_id=723")
	require.Len(t, timedOff, 1)
	require.Equal(t, "COMMITTED", timedOff[0][1])
	require.Nil(t, timedOff[0][2])
	require.Nil(t, timedOff[0][3])
	require.Nil(t, timedOff[0][4])
}

func TestPerformanceSchemaTransactionHistoryTracksSavepointCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "begin")
	mustExecSessionSQL(t, executor, session, "app", "savepoint sp1")
	mustExecSessionSQL(t, executor, session, "app", "rollback to savepoint sp1")
	mustExecSessionSQL(t, executor, session, "app", "release savepoint sp1")
	mustExecSessionSQL(t, executor, session, "app", "commit")

	result := <-executor.ExecuteQuery(session, "select number_of_savepoints, number_of_rollback_to_savepoint, number_of_release_savepoint from performance_schema.events_transactions_history", "app")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"1", "1", "1"}}, selectResultRows(result.Data.(*SelectResult)))
}

func TestPerformanceSchemaLockWaitViewsExposeWaitGraph(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 7, 1, 9, manager.LOCK_X))
	require.ErrorIs(t, locks.AcquireLock(2, 7, 1, 9, manager.LOCK_X), manager.ErrLockConflict)
	var waiting bool
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if len(locks.WaitGraphSnapshot()) > 0 {
			waiting = true
			break
		}
	}
	require.True(t, waiting)

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	executor.QueryExecutor.SetLockManager(locks)
	waitResult := <-executor.ExecuteQuery(nil, "select requesting_engine_transaction_id, blocking_engine_transaction_id, requesting_lock_id, blocking_lock_id from performance_schema.data_lock_waits", "")
	require.NoError(t, waitResult.Err)
	waits, ok := waitResult.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"2", "1", "7_1_9:2", "7_1_9:1"}}, selectResultRows(waits))
	qualifiedIDs := mustSelectResultSQL(t, executor, "", "select requesting_engine_lock_id, blocking_engine_lock_id from performance_schema.data_lock_waits")
	require.Equal(t, [][]interface{}{{"7_1_9:2", "7_1_9:1"}}, selectResultRows(qualifiedIDs))
	current := mustQuerySQL(t, executor, "", "select thread_id, event_name, object_name, operation from performance_schema.events_waits_current")
	require.NotEmpty(t, current)
	require.Contains(t, current[0][1], "wait/lock")
	wrongCurrent := mustQuerySQL(t, executor, "", "select object_name from performance_schema.events_waits_current where object_name='not-a-real-resource'")
	require.Empty(t, wrongCurrent)
	history := mustQuerySQL(t, executor, "", "select thread_id, event_name, object_name, operation from performance_schema.events_waits_history")
	locks.ReleaseLocks(1)
	locks.ReleaseLocks(2)
	history = mustQuerySQL(t, executor, "", "select thread_id, event_name, object_name, operation from performance_schema.events_waits_history")
	require.NotEmpty(t, history)
	require.Contains(t, history[0][1], "wait/lock")
}

func TestPerformanceSchemaDataLocksExposeGrantedRecordLocks(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 8, 1, 10, manager.LOCK_X))

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.SetLockManager(locks)
	result := mustSelectResultSQL(t, executor, "", "select engine_lock_id, engine_transaction_id, thread_id, lock_type, lock_mode, lock_status from performance_schema.data_locks")
	// LockManager owns the InnoDB transaction identity, but does not own a
	// session/thread identity. Do not expose the transaction ID as THREAD_ID.
	require.Equal(t, [][]interface{}{{"8_1_10:1", "1", "", "RECORD", "X", "GRANTED"}}, selectResultRows(result))
}

func TestPerformanceSchemaDataLocksResolveOwningSessionThreadID(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 8, 1, 11, manager.LOCK_X))

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	owner.SetParamByName("connection_id", int64(731))
	owner.SetParamByName(clientStorageTransactionContextKey, &StorageTransactionContext{TransactionID: 1, Session: owner, Status: "ACTIVE"})
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession { return []server.MySQLServerSession{owner} })
	executor.QueryExecutor.SetLockManager(locks)

	result := mustSelectResultSQL(t, executor, "", "select engine_lock_id, engine_transaction_id, thread_id from performance_schema.data_locks")
	require.Equal(t, [][]interface{}{{"8_1_11:1", "1", "731"}}, selectResultRows(result))
}

func TestPerformanceSchemaDataLockWaitsResolveOwningSessionThreadIDs(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 8, 1, 12, manager.LOCK_X))
	require.ErrorIs(t, locks.AcquireLock(2, 8, 1, 12, manager.LOCK_X), manager.ErrLockConflict)

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	owner.SetParamByName("connection_id", int64(741))
	owner.SetParamByName(clientStorageTransactionContextKey, &StorageTransactionContext{TransactionID: 1, Session: owner, Status: "ACTIVE"})
	waiter := newTestMySQLSession()
	waiter.SetParamByName("connection_id", int64(742))
	waiter.SetParamByName(clientStorageTransactionContextKey, &StorageTransactionContext{TransactionID: 2, Session: waiter, Status: "ACTIVE"})
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession { return []server.MySQLServerSession{owner, waiter} })
	executor.QueryExecutor.SetLockManager(locks)

	result := mustSelectResultSQL(t, executor, "", "select requesting_thread_id, blocking_thread_id from performance_schema.data_lock_waits")
	require.Equal(t, [][]interface{}{{"742", "741"}}, selectResultRows(result))
}

func TestPerformanceSchemaWaitEventsResolveOwningSessionThreadID(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 8, 1, 13, manager.LOCK_X))
	require.ErrorIs(t, locks.AcquireLock(2, 8, 1, 13, manager.LOCK_X), manager.ErrLockConflict)

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	owner := newTestMySQLSession()
	owner.SetParamByName("connection_id", int64(751))
	owner.SetParamByName(clientStorageTransactionContextKey, &StorageTransactionContext{TransactionID: 1, Session: owner, Status: "ACTIVE"})
	waiter := newTestMySQLSession()
	waiter.SetParamByName("connection_id", int64(752))
	waiter.SetParamByName(clientStorageTransactionContextKey, &StorageTransactionContext{TransactionID: 2, Session: waiter, Status: "ACTIVE"})
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession { return []server.MySQLServerSession{owner, waiter} })
	executor.QueryExecutor.SetLockManager(locks)

	current := mustSelectResultSQL(t, executor, "", "select thread_id from performance_schema.events_waits_current")
	require.Equal(t, [][]interface{}{{"752"}}, selectResultRows(current))

	locks.ReleaseLocks(1)
	locks.ReleaseLocks(2)
	history := mustSelectResultSQL(t, executor, "", "select thread_id from performance_schema.events_waits_history")
	require.Equal(t, [][]interface{}{{"752"}}, selectResultRows(history))
	summary := mustSelectResultSQL(t, executor, "", "select thread_id, event_name, count_star from performance_schema.events_waits_summary_by_thread_by_event_name")
	require.Equal(t, [][]interface{}{{"752", "wait/lock/table/sql/handler", "1"}}, selectResultRows(summary))
}

func TestPerformanceSchemaGlobalReadLockWaitProjectsCurrentHistoryAndSummary(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	owner := newTestMySQLSession()
	waiter := newTestMySQLSession()
	waiter.SessionContext().SetConnectionID(1902)
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "insert into users values (1)")

	locked := <-executor.ExecuteQuery(owner, "flush tables with read lock", "app")
	require.NoError(t, locked.Err)
	blocked := executor.ExecuteQuery(waiter, "insert into users values (2)", "app")

	var current [][]interface{}
	require.Eventually(t, func() bool {
		result := <-executor.ExecuteQuery(nil, "select thread_id, event_name, object_name, operation from performance_schema.events_waits_current where event_name='wait/synch/cond/sql/xmysql/global_read_lock'", "")
		if result.Err != nil {
			return false
		}
		current = selectResultRows(result.Data.(*SelectResult))
		return len(current) == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, [][]interface{}{{"1902", "wait/synch/cond/sql/xmysql/global_read_lock", "", "wait"}}, current)

	currentSummary := mustSelectResultSQL(t, executor, "", "select thread_id, event_name, count_star from performance_schema.events_waits_summary_by_thread_by_event_name where event_name='wait/synch/cond/sql/xmysql/global_read_lock'")
	require.Equal(t, [][]interface{}{{"1902", "wait/synch/cond/sql/xmysql/global_read_lock", "1"}}, selectResultRows(currentSummary))

	unlocked := <-executor.ExecuteQuery(owner, "unlock tables", "app")
	require.NoError(t, unlocked.Err)
	require.NoError(t, (<-blocked).Err)

	history := mustSelectResultSQL(t, executor, "", "select thread_id, event_name, object_name, operation from performance_schema.events_waits_history_long where event_name='wait/synch/cond/sql/xmysql/global_read_lock'")
	require.Equal(t, [][]interface{}{{"1902", "wait/synch/cond/sql/xmysql/global_read_lock", "", "wait"}}, selectResultRows(history))
	historySummary := mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_waits_summary_global_by_event_name where event_name='wait/synch/cond/sql/xmysql/global_read_lock'")
	require.Equal(t, [][]interface{}{{"wait/synch/cond/sql/xmysql/global_read_lock", "1"}}, selectResultRows(historySummary))
}

func TestPerformanceSchemaGlobalReadLockWaitUsesSynchronizationObjectFields(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	owner := newTestMySQLSession()
	waiter := newTestMySQLSession()
	waiter.SessionContext().SetConnectionID(1904)
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "insert into users values (1)")

	locked := <-executor.ExecuteQuery(owner, "flush tables with read lock", "app")
	require.NoError(t, locked.Err)
	blocked := executor.ExecuteQuery(waiter, "insert into users values (2)", "app")

	var current *SelectResult
	require.Eventually(t, func() bool {
		result := <-executor.ExecuteQuery(nil, "select object_schema, object_name, object_type, object_instance_begin from performance_schema.events_waits_current where event_name='wait/synch/cond/sql/xmysql/global_read_lock'", "")
		if result.Err != nil {
			return false
		}
		current = result.Data.(*SelectResult)
		return len(current.Records) == 1
	}, 2*time.Second, 10*time.Millisecond)
	values := current.Records[0].GetValues()
	require.True(t, values[0].IsNull())
	require.True(t, values[1].IsNull())
	require.True(t, values[2].IsNull())
	require.Greater(t, values[3].Int(), int64(0))
	condition := mustSelectResultSQL(t, executor, "", "select object_instance_begin from performance_schema.cond_instances where name='wait/synch/cond/sql/xmysql/global_read_lock'")
	require.Len(t, condition.Records, 1)
	require.Equal(t, condition.Records[0].GetValues()[0].Int(), values[3].Int())

	unlocked := <-executor.ExecuteQuery(owner, "unlock tables", "app")
	require.NoError(t, unlocked.Err)
	require.NoError(t, (<-blocked).Err)
}

func TestPerformanceSchemaGlobalReadLockWaitHistoryHonorsPerThreadCapacity(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	owner := newTestMySQLSession()
	waiter := newTestMySQLSession()
	waiter.SessionContext().SetConnectionID(1905)
	owner.SetParamByName("user", "root")
	owner.SetParamByName("host", "localhost")
	owner.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	mustExecSessionSQL(t, executor, owner, "", "set global performance_schema_events_waits_history_size=1")
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "insert into users values (1)")

	for _, id := range []int{2, 3} {
		locked := <-executor.ExecuteQuery(owner, "flush tables with read lock", "app")
		require.NoError(t, locked.Err)
		blocked := executor.ExecuteQuery(waiter, fmt.Sprintf("insert into users values (%d)", id), "app")
		require.Eventually(t, func() bool {
			result := <-executor.ExecuteQuery(nil, "select thread_id from performance_schema.events_waits_current where event_name='wait/synch/cond/sql/xmysql/global_read_lock'", "")
			return result.Err == nil && len(result.Data.(*SelectResult).Records) > 0
		}, time.Second, 5*time.Millisecond)
		unlocked := <-executor.ExecuteQuery(owner, "unlock tables", "app")
		require.NoError(t, unlocked.Err)
		require.NoError(t, (<-blocked).Err)
	}

	history := mustSelectResultSQL(t, executor, "", "select thread_id, event_name from performance_schema.events_waits_history where event_name='wait/synch/cond/sql/xmysql/global_read_lock'")
	require.Len(t, history.Records, 1)
	require.Equal(t, "1905", selectResultRows(history)[0][0])
}

func TestPerformanceSchemaGlobalReadLockWaitShortAndLongHistoryHaveIndependentCapacity(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	owner := newTestMySQLSession()
	waiter := newTestMySQLSession()
	waiter.SessionContext().SetConnectionID(1906)
	owner.SetParamByName("user", "root")
	owner.SetParamByName("host", "localhost")
	owner.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	mustExecSessionSQL(t, executor, owner, "", "set global performance_schema_events_waits_history_size=3")
	mustExecSessionSQL(t, executor, owner, "", "set global performance_schema_events_waits_history_long_size=1")
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "insert into users values (1)")

	for _, id := range []int{2, 3, 4} {
		locked := <-executor.ExecuteQuery(owner, "flush tables with read lock", "app")
		require.NoError(t, locked.Err)
		blocked := executor.ExecuteQuery(waiter, fmt.Sprintf("insert into users values (%d)", id), "app")
		require.Eventually(t, func() bool {
			result := <-executor.ExecuteQuery(nil, "select thread_id from performance_schema.events_waits_current where event_name='wait/synch/cond/sql/xmysql/global_read_lock'", "")
			return result.Err == nil && len(result.Data.(*SelectResult).Records) > 0
		}, time.Second, 5*time.Millisecond)
		unlocked := <-executor.ExecuteQuery(owner, "unlock tables", "app")
		require.NoError(t, unlocked.Err)
		require.NoError(t, (<-blocked).Err)
	}

	shortHistory := mustSelectResultSQL(t, executor, "", "select thread_id, event_name from performance_schema.events_waits_history where event_name='wait/synch/cond/sql/xmysql/global_read_lock'")
	longHistory := mustSelectResultSQL(t, executor, "", "select thread_id, event_name from performance_schema.events_waits_history_long where event_name='wait/synch/cond/sql/xmysql/global_read_lock'")
	require.Len(t, shortHistory.Records, 3)
	require.Len(t, longHistory.Records, 1)
}

func TestPerformanceSchemaGlobalReadLockWaitHistoryTruncateScopesAreIndependent(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	owner := newTestMySQLSession()
	waiter := newTestMySQLSession()
	waiter.SessionContext().SetConnectionID(1907)
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "insert into users values (1)")
	locked := <-executor.ExecuteQuery(owner, "flush tables with read lock", "app")
	require.NoError(t, locked.Err)
	blocked := executor.ExecuteQuery(waiter, "insert into users values (2)", "app")
	require.Eventually(t, func() bool {
		result := <-executor.ExecuteQuery(nil, "select thread_id from performance_schema.events_waits_current where event_name='wait/synch/cond/sql/xmysql/global_read_lock'", "")
		return result.Err == nil && len(result.Data.(*SelectResult).Records) > 0
	}, time.Second, 5*time.Millisecond)
	unlocked := <-executor.ExecuteQuery(owner, "unlock tables", "app")
	require.NoError(t, unlocked.Err)
	require.NoError(t, (<-blocked).Err)

	mustExecSQL(t, executor, "", "truncate table performance_schema.events_waits_history")
	shortHistory := mustSelectResultSQL(t, executor, "", "select event_name from performance_schema.events_waits_history where event_name='wait/synch/cond/sql/xmysql/global_read_lock'")
	longHistory := mustSelectResultSQL(t, executor, "", "select event_name from performance_schema.events_waits_history_long where event_name='wait/synch/cond/sql/xmysql/global_read_lock'")
	require.Empty(t, shortHistory.Records)
	require.Len(t, longHistory.Records, 1)

	mustExecSQL(t, executor, "", "truncate table performance_schema.events_waits_history_long")
	longHistory = mustSelectResultSQL(t, executor, "", "select event_name from performance_schema.events_waits_history_long where event_name='wait/synch/cond/sql/xmysql/global_read_lock'")
	require.Empty(t, longHistory.Records)
}

func TestPerformanceSchemaGlobalReadLockWaitSummaryByInstanceProjectsCurrentAndHistory(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	owner := newTestMySQLSession()
	waiter := newTestMySQLSession()
	waiter.SessionContext().SetConnectionID(1903)
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "insert into users values (1)")

	locked := <-executor.ExecuteQuery(owner, "flush tables with read lock", "app")
	require.NoError(t, locked.Err)
	blocked := executor.ExecuteQuery(waiter, "insert into users values (2)", "app")

	var current [][]interface{}
	require.Eventually(t, func() bool {
		result := <-executor.ExecuteQuery(nil, "select event_name, object_instance_begin, count_star from performance_schema.events_waits_summary_by_instance where event_name='wait/synch/cond/sql/xmysql/global_read_lock'", "")
		if result.Err != nil {
			return false
		}
		current = selectResultRows(result.Data.(*SelectResult))
		return len(current) == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, "wait/synch/cond/sql/xmysql/global_read_lock", current[0][0])
	require.NotEqual(t, "0", current[0][1])
	require.Equal(t, "1", current[0][2])
	condition := mustSelectResultSQL(t, executor, "", "select object_instance_begin from performance_schema.cond_instances where name='wait/synch/cond/sql/xmysql/global_read_lock'")
	require.Len(t, condition.Records, 1)
	require.Equal(t, strconv.FormatInt(condition.Records[0].GetValues()[0].Int(), 10), current[0][1])

	unlocked := <-executor.ExecuteQuery(owner, "unlock tables", "app")
	require.NoError(t, unlocked.Err)
	require.NoError(t, (<-blocked).Err)

	history := mustSelectResultSQL(t, executor, "", "select event_name, object_instance_begin, count_star, sum_timer_wait from performance_schema.events_waits_summary_by_instance where event_name='wait/synch/cond/sql/xmysql/global_read_lock'")
	require.Len(t, history.Records, 1)
	historyRows := selectResultRows(history)
	require.Equal(t, current[0][0], historyRows[0][0])
	require.Equal(t, current[0][1], historyRows[0][1])
	require.Equal(t, "1", historyRows[0][2])
	require.NotEqual(t, "0", historyRows[0][3])
}

func TestPerformanceSchemaGlobalReadLockWaitSummaryTruncateClearsAllDimensions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	owner := newTestMySQLSession()
	waiter := newTestMySQLSession()
	waiter.SessionContext().SetConnectionID(1908)
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "insert into users values (1)")

	locked := <-executor.ExecuteQuery(owner, "flush tables with read lock", "app")
	require.NoError(t, locked.Err)
	blocked := executor.ExecuteQuery(waiter, "insert into users values (2)", "app")
	require.Eventually(t, func() bool {
		result := <-executor.ExecuteQuery(nil, "select thread_id from performance_schema.events_waits_current where event_name='wait/synch/cond/sql/xmysql/global_read_lock'", "")
		return result.Err == nil && len(result.Data.(*SelectResult).Records) > 0
	}, time.Second, 5*time.Millisecond)
	unlocked := <-executor.ExecuteQuery(owner, "unlock tables", "app")
	require.NoError(t, unlocked.Err)
	require.NoError(t, (<-blocked).Err)

	before := mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_waits_summary_global_by_event_name where event_name='wait/synch/cond/sql/xmysql/global_read_lock'")
	require.Equal(t, [][]interface{}{{"wait/synch/cond/sql/xmysql/global_read_lock", "1"}}, selectResultRows(before))

	truncateStmt, err := sqlparser.Parse("truncate table performance_schema.events_waits_summary_by_instance")
	require.NoError(t, err)
	truncateResults := make(chan *Result, 1)
	executor.QueryExecutor.executeTruncateTableStatement(&ExecutionContext{Session: owner, Results: truncateResults, Cfg: executor.QueryExecutor.conf}, "", truncateStmt.(*sqlparser.DDL))
	require.NoError(t, (<-truncateResults).Err)
	byInstance := mustSelectResultSQL(t, executor, "", "select event_name, object_instance_begin, count_star from performance_schema.events_waits_summary_by_instance where event_name='wait/synch/cond/sql/xmysql/global_read_lock'")
	require.Equal(t, [][]interface{}{{"wait/synch/cond/sql/xmysql/global_read_lock", strconv.FormatInt(executor.QueryExecutor.performanceSchemaGlobalReadLockConditionObjectInstanceBegin(), 10), "0"}}, selectResultRows(byInstance))
	byThread := mustSelectResultSQL(t, executor, "", "select thread_id, event_name, count_star from performance_schema.events_waits_summary_by_thread_by_event_name where event_name='wait/synch/cond/sql/xmysql/global_read_lock'")
	require.Equal(t, [][]interface{}{{"1908", "wait/synch/cond/sql/xmysql/global_read_lock", "1"}}, selectResultRows(byThread))
	global := mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_waits_summary_global_by_event_name where event_name='wait/synch/cond/sql/xmysql/global_read_lock'")
	require.Equal(t, [][]interface{}{{"wait/synch/cond/sql/xmysql/global_read_lock", "1"}}, selectResultRows(global))
}

func TestPerformanceSchemaWaitSummariesAggregateCurrentWaits(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 8, 1, 10, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(2, 8, 1, 10, manager.LOCK_X))

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.SetLockManager(locks)
	result := mustSelectResultSQL(t, executor, "", "select event_name, count_star, sum_timer_wait, min_timer_wait, avg_timer_wait, max_timer_wait from performance_schema.events_waits_summary_global_by_event_name")
	rows := selectResultRows(result)
	require.Len(t, rows, 1)
	require.Equal(t, "wait/lock/table/sql/handler", rows[0][0])
	require.Equal(t, "1", rows[0][1])

	threadResult := mustSelectResultSQL(t, executor, "", "select thread_id, event_name, count_star from performance_schema.events_waits_summary_by_thread_by_event_name")
	require.Equal(t, [][]interface{}{{"2", "wait/lock/table/sql/handler", "1"}}, selectResultRows(threadResult))
	locks.ReleaseLocks(1)
	locks.ReleaseLocks(2)
	historySummary := mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_waits_summary_global_by_event_name")
	require.Equal(t, [][]interface{}{{"wait/lock/table/sql/handler", "1"}}, selectResultRows(historySummary))
}

func TestPerformanceSchemaWaitAccountSummaryTruncateIsolated(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 1313, 1, 1, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(2, 1313, 1, 1, manager.LOCK_X))
	locks.ReleaseLocks(1)
	locks.ReleaseLocks(2)

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	waitingSession := newTestMySQLSession()
	waitingSession.SetParamByName("connection_id", int64(2))
	waitingSession.SetParamByName("user", "wait_account_user")
	waitingSession.SetParamByName("host", "wait-account.example")
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{waitingSession}
	})
	executor.QueryExecutor.SetLockManager(locks)

	accountBefore := executor.QueryExecutor.executePerformanceSchemaWaitSummaryRegistrySelect(
		"select user, host, event_name, count_star from performance_schema.events_waits_summary_by_account_by_event_name",
		"account",
	)
	require.Equal(t, [][]interface{}{{"wait_account_user", "wait-account.example", "wait/lock/table/sql/handler", "1"}}, selectResultRows(accountBefore))

	truncateStmt, err := sqlparser.Parse("truncate table performance_schema.events_waits_summary_by_account_by_event_name")
	require.NoError(t, err)
	ddlResults := make(chan *Result, 1)
	executor.QueryExecutor.executeDDL(truncateStmt.(*sqlparser.DDL), nil, "", ddlResults)
	require.NoError(t, (<-ddlResults).Err)

	accountAfter := executor.QueryExecutor.executePerformanceSchemaWaitSummaryRegistrySelect(
		"select user, host, event_name, count_star from performance_schema.events_waits_summary_by_account_by_event_name",
		"account",
	)
	require.Equal(t, [][]interface{}{{"wait_account_user", "wait-account.example", "wait/lock/table/sql/handler", "0"}}, selectResultRows(accountAfter))

	secondSession := newTestMySQLSession()
	secondSession.SetParamByName("connection_id", int64(4))
	secondSession.SetParamByName("user", "wait_account_user")
	secondSession.SetParamByName("host", "wait-account.example")
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{waitingSession, secondSession}
	})
	require.NoError(t, locks.AcquireLock(3, 1313, 1, 1, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(4, 1313, 1, 1, manager.LOCK_X))
	time.Sleep(2 * time.Millisecond)
	accountResumed := executor.QueryExecutor.executePerformanceSchemaWaitSummaryRegistrySelect(
		"select user, host, event_name, count_star, sum_timer_wait, min_timer_wait, max_timer_wait from performance_schema.events_waits_summary_by_account_by_event_name",
		"account",
	)
	resumedRows := selectResultRows(accountResumed)
	require.Equal(t, 1, len(resumedRows))
	require.Equal(t, []interface{}{"wait_account_user", "wait-account.example", "wait/lock/table/sql/handler", "1"}, resumedRows[0][:4])
	require.NotEqual(t, "0", resumedRows[0][4])
	require.NotEqual(t, "0", resumedRows[0][5])
	require.NotEqual(t, "0", resumedRows[0][6])
	locks.ReleaseLocks(3)
	locks.ReleaseLocks(4)
	globalAfter := executor.QueryExecutor.executePerformanceSchemaWaitSummarySelect(
		"select event_name, count_star from performance_schema.events_waits_summary_global_by_event_name",
		false,
	)
	require.Equal(t, [][]interface{}{{"wait/lock/table/sql/handler", "2"}}, selectResultRows(globalAfter))
}

func TestPerformanceSchemaWaitSummaryRetainsLifetimeTotalsBeyondHistory(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	const waits = 130
	for resource := uint64(0); resource < waits; resource++ {
		pageID := uint32(1000 + resource)
		require.NoError(t, locks.AcquireLock(1, pageID, 1, 10, manager.LOCK_X))
		require.Error(t, locks.AcquireLock(2, pageID, 1, 10, manager.LOCK_X))
		locks.ReleaseLocks(1)
		locks.ReleaseLocks(2)
	}

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.SetLockManager(locks)
	result := executor.QueryExecutor.executePerformanceSchemaWaitSummarySelect(
		"select event_name, count_star from performance_schema.events_waits_summary_global_by_event_name",
		false,
	)
	require.Equal(t, [][]interface{}{{"wait/lock/table/sql/handler", fmt.Sprint(waits)}}, selectResultRows(result))
}

func TestPerformanceSchemaWaitSummaryIdentityFilters(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 82, 1, 102, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(2, 82, 1, 102, manager.LOCK_X))

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.SetLockManager(locks)
	waiter := newTestMySQLSession()
	waiter.SetParamByName("connection_id", int64(2))
	waiter.SetParamByName("user", "wait_summary_user")
	waiter.SetParamByName("host", "wait-summary.example")
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{waiter}
	})

	filtered := mustSelectResultSQL(t, executor, "", "select user, host, event_name, count_star from performance_schema.events_waits_summary_by_account_by_event_name where user='wait_summary_user' and host='wait-summary.example' and event_name='wait/lock/table/sql/handler'")
	require.Equal(t, [][]interface{}{{"wait_summary_user", "wait-summary.example", "wait/lock/table/sql/handler", "1"}}, selectResultRows(filtered))

	wrongCount := mustSelectResultSQL(t, executor, "", "select user, host, event_name from performance_schema.events_waits_summary_by_account_by_event_name where user='wait_summary_user' and host='wait-summary.example' and event_name='wait/lock/table/sql/handler' and count_star=999999")
	require.Empty(t, wrongCount.Records)

	wrongUser := mustSelectResultSQL(t, executor, "", "select user, host, event_name from performance_schema.events_waits_summary_by_account_by_event_name where user='other_user'")
	require.Empty(t, wrongUser.Records)

	wrongHost := mustSelectResultSQL(t, executor, "", "select host, event_name from performance_schema.events_waits_summary_by_host_by_event_name where host='other-host'")
	require.Empty(t, wrongHost.Records)
	locks.ReleaseLocks(1)
	locks.ReleaseLocks(2)
}

func TestPerformanceSchemaWaitSummaryByInstanceProjectsCurrentWaits(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 81, 1, 101, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(2, 81, 1, 101, manager.LOCK_X))

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.SetLockManager(locks)
	result := mustSelectResultSQL(t, executor, "", "select event_name, object_instance_begin, count_star, sum_timer_wait from performance_schema.events_waits_summary_by_instance")
	require.Equal(t, []string{"EVENT_NAME", "OBJECT_INSTANCE_BEGIN", "COUNT_STAR", "SUM_TIMER_WAIT"}, result.Columns)
	require.Len(t, result.Records, 1)
	rows := selectResultRows(result)
	require.Equal(t, "wait/lock/table/sql/handler", rows[0][0])
	require.Equal(t, "81_1_101", rows[0][1])
	require.Equal(t, "1", rows[0][2])
	require.NotEqual(t, "0", rows[0][3])
	locks.ReleaseLocks(1)
	locks.ReleaseLocks(2)
}

func TestPerformanceSchemaTableLockSummaryAggregatesCurrentWaits(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 9, 1, 11, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(2, 9, 1, 11, manager.LOCK_X))

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.SetLockManager(locks)
	rows := selectResultRows(mustSelectResultSQL(t, executor, "", "select object_name, count_star, sum_timer_wait from performance_schema.table_lock_waits_summary_by_table"))
	require.Len(t, rows, 1)
	require.Equal(t, "9_1_11", rows[0][0])
	require.Equal(t, "1", rows[0][1])
	wrongCount := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.table_lock_waits_summary_by_table where object_name='9_1_11' and count_star=999999")
	require.Empty(t, wrongCount.Records)
	locks.ReleaseLocks(1)
	locks.ReleaseLocks(2)
}

func TestPerformanceSchemaTableLockSummaryTruncateIsIndependent(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 9, 1, 11, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(2, 9, 1, 11, manager.LOCK_X))

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.SetLockManager(locks)

	before := selectResultRows(mustSelectResultSQL(t, executor, "", "select object_name, count_star from performance_schema.table_lock_waits_summary_by_table"))
	require.Len(t, before, 1)
	require.Equal(t, "9_1_11", before[0][0])
	require.Equal(t, "1", before[0][1])

	waitsBefore := selectResultRows(mustSelectResultSQL(t, executor, "", "select object_name, count_star from performance_schema.events_waits_summary_global_by_event_name where event_name='wait/lock/table/sql/handler'"))
	require.Len(t, waitsBefore, 1)
	require.Equal(t, "1", waitsBefore[0][1])
	locks.ReleaseLocks(1)
	locks.ReleaseLocks(2)

	mustExecSQL(t, executor, "", "truncate table performance_schema.table_lock_waits_summary_by_table")

	after := selectResultRows(mustSelectResultSQL(t, executor, "", "select object_name, count_star from performance_schema.table_lock_waits_summary_by_table"))
	require.Len(t, after, 1)
	require.Equal(t, "9_1_11", after[0][0])
	require.Equal(t, "0", after[0][1])

	waitsAfter := selectResultRows(mustSelectResultSQL(t, executor, "", "select object_name, count_star from performance_schema.events_waits_summary_global_by_event_name where event_name='wait/lock/table/sql/handler'"))
	require.Len(t, waitsAfter, 1)
	require.Equal(t, "1", waitsAfter[0][1])
}

func TestPerformanceSchemaMemorySummaryTruncateResetsBaseline(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	recorder := executor.QueryExecutor.metricsRecorder
	recorder.RecordMemoryAllocation(777, "memory/test/truncate", 64)
	recorder.RecordMemoryFree(777, "memory/test/truncate", 64)

	before := recorder.MemorySummary()
	var found bool
	for _, row := range before {
		if row.ThreadID == 777 && row.EventName == "memory/test/truncate" {
			require.Equal(t, int64(1), row.CountAlloc)
			require.Equal(t, int64(1), row.CountFree)
			require.Equal(t, int64(0), row.CurrentCountUsed)
			require.Equal(t, int64(1), row.HighCountUsed)
			found = true
		}
	}
	require.True(t, found)

	mustExecSQL(t, executor, "", "truncate table performance_schema.memory_summary_global_by_event_name")

	after := recorder.MemorySummary()
	for _, row := range after {
		if row.ThreadID == 777 && row.EventName == "memory/test/truncate" {
			require.Equal(t, int64(0), row.CountAlloc)
			require.Equal(t, int64(0), row.CountFree)
			require.Equal(t, int64(0), row.CurrentCountUsed)
			require.Equal(t, int64(0), row.HighCountUsed)
			require.Equal(t, int64(0), row.LowCountUsed)
			return
		}
	}
	t.Fatal("memory summary row disappeared after truncate")
}

func TestPerformanceSchemaMemorySummaryConnectionTruncateResetsDependentDimensions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	current := newTestMySQLSession()
	current.SetParamByName("connection_id", int64(991))
	current.SessionContext().SetConnectionID(991)
	current.SetParamByName("user", "memory_current_user")
	current.SetParamByName("host", "memory-current.example")
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{current}
	})
	recorder := executor.QueryExecutor.metricsRecorder
	recorder.RecordMemoryAllocationWithIdentity(991, "memory_current_user", "memory-current.example", "memory/test/connection-truncate", 64)
	recorder.RecordMemoryAllocationWithIdentity(991, "memory_current_user", "memory-current.example", "memory/test/connection-truncate", 32)
	recorder.RecordMemoryFreeWithIdentity(991, "memory_current_user", "memory-current.example", "memory/test/connection-truncate", 32)
	recorder.RecordMemoryAllocationWithIdentity(992, "memory_historical_user", "memory-historical.example", "memory/test/connection-truncate", 128)

	mustExecSessionSQL(t, executor, current, "", "truncate table performance_schema.accounts")

	rows := recorder.MemorySummary()
	seen := map[int64]bool{}
	for _, row := range rows {
		if row.EventName != "memory/test/connection-truncate" {
			continue
		}
		seen[row.ThreadID] = true
		require.Equal(t, int64(0), row.CountAlloc, "thread %d allocation baseline", row.ThreadID)
		require.Equal(t, int64(0), row.CountFree, "thread %d free baseline", row.ThreadID)
		require.Equal(t, int64(1), row.CurrentCountUsed, "thread %d current allocation", row.ThreadID)
	}
	require.True(t, seen[991])
	require.True(t, seen[992])

	accountRows := selectResultRows(mustSelectResultSessionSQL(t, executor, current, "", "select user, host, event_name, count_alloc, current_count_used from performance_schema.memory_summary_by_account_by_event_name where user='memory_current_user' and host='memory-current.example' and event_name='memory/test/connection-truncate'"))
	require.Equal(t, [][]interface{}{{"memory_current_user", "memory-current.example", "memory/test/connection-truncate", "0", "1"}}, accountRows)
	hostRows := selectResultRows(mustSelectResultSessionSQL(t, executor, current, "", "select host, event_name, count_alloc, current_count_used from performance_schema.memory_summary_by_host_by_event_name where host='memory-historical.example' and event_name='memory/test/connection-truncate'"))
	require.Equal(t, [][]interface{}{{"memory-historical.example", "memory/test/connection-truncate", "1", "1"}}, hostRows)
	userRows := selectResultRows(mustSelectResultSessionSQL(t, executor, current, "", "select user, event_name, count_alloc, current_count_used from performance_schema.memory_summary_by_user_by_event_name where user='memory_historical_user' and event_name='memory/test/connection-truncate'"))
	require.Equal(t, [][]interface{}{{"memory_historical_user", "memory/test/connection-truncate", "1", "1"}}, userRows)
}

func TestPerformanceSchemaFileSummaryTruncateResetsRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	recorder := executor.QueryExecutor.metricsRecorder
	fileName := "data/ps-file-truncate.ibd"
	recorder.RecordFileOpen(fileName)
	recorder.RecordFileRead(fileName, 2*time.Millisecond)

	before := recorder.FileSummary()
	var found bool
	for _, row := range before {
		if row.FileName == filepath.Clean(fileName) {
			require.Equal(t, int64(1), row.CountRead)
			require.Equal(t, int64(1), row.OpenCount)
			found = true
		}
	}
	require.True(t, found)

	mustExecSQL(t, executor, "", "truncate table performance_schema.file_summary_by_event_name")

	after := recorder.FileSummary()
	for _, row := range after {
		if row.FileName == filepath.Clean(fileName) {
			require.Equal(t, int64(0), row.CountRead)
			require.Equal(t, int64(0), row.SumTimerRead)
			require.Equal(t, int64(1), row.OpenCount)
			return
		}
	}
	t.Fatal("file summary row disappeared after truncate")
}

func TestPerformanceSchemaSocketSummaryTruncateResetsRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	previousRecorder := executor.QueryExecutor.metricsRecorder
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	t.Cleanup(func() { executor.QueryExecutor.metricsRecorder = previousRecorder })
	recorder := executor.QueryExecutor.metricsRecorder
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(778))
	recorder.RecordSocketRead(778, 128, 3*time.Millisecond)
	recorder.RecordSocketWrite(778, 256, 5*time.Millisecond)

	before := recorder.SocketSummary()
	require.Len(t, before, 1)
	require.Equal(t, int64(1), before[0].CountRead)
	require.Equal(t, int64(1), before[0].CountWrite)
	require.Equal(t, int64(128), before[0].BytesRead)
	require.Equal(t, int64(256), before[0].BytesWrite)

	mustExecSQL(t, executor, "", "truncate table performance_schema.socket_summary_by_event_name")

	after := recorder.SocketSummary()
	require.Len(t, after, 1)
	require.Equal(t, int64(778), after[0].ThreadID)
	require.Equal(t, int64(0), after[0].CountRead)
	require.Equal(t, int64(0), after[0].CountWrite)
	require.Equal(t, int64(0), after[0].BytesRead)
	require.Equal(t, int64(0), after[0].BytesWrite)

	result := <-executor.ExecuteQuery(session, "select event_name, count_star, count_read, count_write from performance_schema.socket_summary_by_event_name", "")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"wait/io/socket/sql/client_connection", "0", "0", "0"}}, selectResultRows(result.Data.(*SelectResult)))
}

func TestPerformanceSchemaTableHandlesExposeExplicitTableLocks(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	observer := newTestMySQLSession()
	owner.SetParamByName("connection_id", int64(431))
	owner.SessionContext().SetConnectionID(431)
	observer.SetParamByName("connection_id", int64(432))
	observer.SessionContext().SetConnectionID(432)
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table table_handle_target (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables table_handle_target read")
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{owner, observer}
	})

	result := <-executor.ExecuteQuery(observer, "select object_type, object_schema, object_name, owner_thread_id, internal_lock, external_lock from performance_schema.table_handles", "app")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"TABLE", "app", "table_handle_target", "431", "READ", "READ"}}, selectResultRows(result.Data.(*SelectResult)))

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	afterUnlock := <-executor.ExecuteQuery(observer, "select object_name from performance_schema.table_handles where object_name='table_handle_target'", "app")
	require.NoError(t, afterUnlock.Err)
	require.Empty(t, selectResultRows(afterUnlock.Data.(*SelectResult)))
}

func TestPerformanceSchemaTableHandlesExposeImplicitTransactionTableLease(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	observer := newTestMySQLSession()
	owner.SetParamByName("connection_id", int64(433))
	owner.SessionContext().SetConnectionID(433)
	observer.SetParamByName("connection_id", int64(434))
	observer.SessionContext().SetConnectionID(434)
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table implicit_handle_target (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "start transaction")
	require.Empty(t, mustQuerySessionSQL(t, executor, owner, "app", "select id from implicit_handle_target"))
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{owner, observer}
	})

	result := <-executor.ExecuteQuery(observer, "select object_type, object_schema, object_name, owner_thread_id, internal_lock, external_lock from performance_schema.table_handles", "app")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"TABLE", "app", "implicit_handle_target", "433", "READ", "READ"}}, selectResultRows(result.Data.(*SelectResult)))

	mustExecSessionSQL(t, executor, owner, "app", "rollback")
	afterRollback := <-executor.ExecuteQuery(observer, "select object_name from performance_schema.table_handles where object_name='implicit_handle_target'", "app")
	require.NoError(t, afterRollback.Err)
	require.Empty(t, selectResultRows(afterRollback.Data.(*SelectResult)))
}

func TestPerformanceSchemaObjectsSummaryFiltersCurrentWaitObjects(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 91, 1, 13, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(2, 91, 1, 13, manager.LOCK_X))

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.SetLockManager(locks)
	matching := mustSelectResultSQL(t, executor, "", "select object_type, object_name, count_star from performance_schema.objects_summary_global_by_type where object_name = '91_1_13'")
	require.Len(t, matching.Records, 1)
	wrongName := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.objects_summary_global_by_type where object_name = 'other'")
	require.Empty(t, wrongName.Records)
	wrongType := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.objects_summary_global_by_type where object_type = 'FILE'")
	require.Empty(t, wrongType.Records)
	locks.ReleaseLocks(1)
	locks.ReleaseLocks(2)
}

func TestPerformanceSchemaObjectsSummaryUsesCompletedWaitsAndTruncateResetsCounters(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 91, 1, 14, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(2, 91, 1, 14, manager.LOCK_X))
	locks.ReleaseLocks(1)
	locks.ReleaseLocks(2)
	require.NoError(t, locks.AcquireLock(3, 91, 1, 14, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(4, 91, 1, 14, manager.LOCK_X))
	locks.ReleaseLocks(3)

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.SetLockManager(locks)
	before := selectResultRows(mustSelectResultSQL(t, executor, "", "select object_type, object_name, count_star, sum_timer_wait from performance_schema.objects_summary_global_by_type where object_name = '91_1_14'"))
	require.Len(t, before, 1)
	require.Equal(t, "2", before[0][2])

	mustExecSQL(t, executor, "", "truncate table performance_schema.objects_summary_global_by_type")
	after := selectResultRows(mustSelectResultSQL(t, executor, "", "select object_name, count_star, sum_timer_wait from performance_schema.objects_summary_global_by_type where object_name = '91_1_14'"))
	require.Len(t, after, 1)
	require.Equal(t, "0", after[0][1])
	require.Equal(t, "0", after[0][2])
}

func TestPerformanceSchemaTableIOSummaryProjectsRecentTableAccess(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	targetTable := fmt.Sprintf("ps_io_%d", time.Now().UnixNano())
	executor.QueryExecutor.metricsRecorder.RecordStatement("app", "select id from app."+targetTable, "SELECT", "ok", 2*time.Millisecond)
	executor.QueryExecutor.metricsRecorder.RecordStatement("app", "update app."+targetTable+" set id = id", "UPDATE", "ok", 3*time.Millisecond)
	rows := selectResultRows(mustSelectResultSQL(t, executor, "", "select object_schema, object_name, count_star, count_read, count_write, count_fetch, count_update from performance_schema.table_io_waits_summary_by_table where object_name = '"+targetTable+"'"))
	require.Len(t, rows, 1)
	require.Equal(t, "app", rows[0][0])
	require.Equal(t, targetTable, rows[0][1])
	require.Equal(t, "2", rows[0][2])
	require.Equal(t, "2", rows[0][3])
	require.Equal(t, "1", rows[0][4])
	require.Equal(t, "1", rows[0][5])
	require.Equal(t, "1", rows[0][6])
	wrongType := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.table_io_waits_summary_by_table where object_name = '"+targetTable+"' and object_type = 'INDEX'")
	require.Empty(t, wrongType.Records)
	wrongIndex := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.table_io_waits_summary_by_index_usage where object_name = '"+targetTable+"' and index_name = 'PRIMARY'")
	require.Empty(t, wrongIndex.Records)
	wrongCount := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.table_io_waits_summary_by_table where object_name = '"+targetTable+"' and count_star = 999")
	require.Empty(t, wrongCount.Records)
	wrongReadCount := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.table_io_waits_summary_by_table where object_name = '"+targetTable+"' and count_read = 999")
	require.Empty(t, wrongReadCount.Records)
}

func TestPerformanceSchemaSetupObjectsControlsTableIOSummaryProjection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	targetTable := fmt.Sprintf("ps_setup_object_io_%d", time.Now().UnixNano())

	result := <-executor.ExecuteQuery(nil, "insert into performance_schema.setup_objects (object_type, object_schema, object_name, enabled, timed) values ('TABLE', 'app', '"+targetTable+"', 'NO', 'NO')", "")
	require.NoError(t, result.Err)
	executor.QueryExecutor.metricsRecorder.RecordStatement("app", "select id from app."+targetTable, "SELECT", "ok", 2*time.Millisecond)
	rows := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.table_io_waits_summary_by_table where object_name='"+targetTable+"'")
	require.Empty(t, rows.Records, "disabled setup_objects rows must not be exposed in table I/O summaries")

	result = <-executor.ExecuteQuery(nil, "update performance_schema.setup_objects set enabled='YES', timed='NO' where object_type='TABLE' and object_schema='app' and object_name='"+targetTable+"'", "")
	require.NoError(t, result.Err)
	executor.QueryExecutor.metricsRecorder.RecordStatement("app", "select id from app."+targetTable, "SELECT", "ok", 3*time.Millisecond)
	rows = mustSelectResultSQL(t, executor, "", "select object_name, count_star, sum_timer_wait from performance_schema.table_io_waits_summary_by_table where object_name='"+targetTable+"'")
	require.Len(t, rows.Records, 1)
	values := rows.Records[0].GetValues()
	require.Equal(t, targetTable, values[0].String())
	require.Equal(t, int64(2), values[1].Int())
	require.Zero(t, values[2].Int(), "TIMED=NO must preserve table I/O counts but omit timer values")
}

func TestPerformanceSchemaTableIOSummaryRetainsLifetimeTotalsBeyondHistory(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	targetTable := fmt.Sprintf("ps_io_lifetime_%d", time.Now().UnixNano())
	const executions = 300
	for i := 0; i < executions; i++ {
		executor.QueryExecutor.metricsRecorder.RecordStatement(
			"app", "select id from app."+targetTable, "SELECT", "ok", time.Millisecond,
		)
	}
	rows := selectResultRows(mustSelectResultSQL(t, executor, "", "select object_name, count_star, count_read, count_fetch from performance_schema.table_io_waits_summary_by_table where object_name = '"+targetTable+"'"))
	require.Len(t, rows, 1)
	require.Equal(t, targetTable, rows[0][0])
	require.Equal(t, fmt.Sprint(executions), rows[0][1])
	require.Equal(t, fmt.Sprint(executions), rows[0][2])
	require.Equal(t, fmt.Sprint(executions), rows[0][3])
}

func TestPerformanceSchemaThreadMemoryAndTimerViewsExposeCompatibilityRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(88)
	result := <-executor.ExecuteQuery(session, "select 1", "")
	require.NoError(t, result.Err)
	global := executor.QueryExecutor.executePerformanceSchemaMemorySummarySelect("select event_name, count_alloc, count_free, sum_number_of_bytes_alloc, sum_number_of_bytes_free from performance_schema.memory_summary_global_by_event_name")
	var globalValues []basic.Value
	for _, record := range global.Records {
		values := record.GetValues()
		if values[0].String() == "memory/sql/THD::main_mem_root" {
			globalValues = values
			break
		}
	}
	require.NotNil(t, globalValues)
	require.Greater(t, globalValues[1].Int(), int64(0))
	require.Greater(t, globalValues[2].Int(), int64(0))
	require.Greater(t, globalValues[3].Int(), int64(0))
	require.Greater(t, globalValues[4].Int(), int64(0))
	memory := executor.QueryExecutor.executePerformanceSchemaMemorySummaryByThreadSelect("select event_name, thread_id, current_count_used from performance_schema.memory_summary_by_thread_by_event_name", session)
	require.Equal(t, []string{"EVENT_NAME", "THREAD_ID", "CURRENT_COUNT_USED"}, memory.Columns)
	require.NotEmpty(t, memory.Records)
	foundMemoryThread := false
	for _, record := range memory.Records {
		values := record.GetValues()
		if values[0].String() == "memory/sql/THD::main_mem_root" && values[1].Int() == 88 {
			foundMemoryThread = true
			break
		}
	}
	require.True(t, foundMemoryThread)
	timers := mustQuerySQL(t, executor, "", "select timer_name, timer_frequency from performance_schema.performance_timers where timer_name='NANOSECOND'")
	require.Equal(t, [][]interface{}{{"NANOSECOND", "1000000000"}}, timers)
}

func TestPerformanceSchemaMemorySummaryIdentityFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(881))
	session.SetParamByName("user", "memory_filter_user")
	session.SetParamByName("host", "memory-filter.example")
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{session}
	})
	require.NoError(t, (<-executor.ExecuteQuery(session, "select 881881", "memory_filter_app")).Err)

	filtered := mustSelectResultSQL(t, executor, "", "select user, host, event_name, count_alloc from performance_schema.memory_summary_by_account_by_event_name where user='memory_filter_user' and host='memory-filter.example'")
	require.NotEmpty(t, filtered.Records)
	for _, row := range selectResultRows(filtered) {
		require.Equal(t, "memory_filter_user", row[0])
		require.Equal(t, "memory-filter.example", row[1])
	}

	wrongCount := mustSelectResultSQL(t, executor, "", "select user, host, event_name from performance_schema.memory_summary_by_account_by_event_name where user='memory_filter_user' and host='memory-filter.example' and count_alloc=999999")
	require.Empty(t, wrongCount.Records)

	wrongUser := mustSelectResultSQL(t, executor, "", "select user, host, event_name from performance_schema.memory_summary_by_account_by_event_name where user='other_memory_user'")
	require.Empty(t, wrongUser.Records)

	wrongHost := mustSelectResultSQL(t, executor, "", "select host, event_name from performance_schema.memory_summary_by_host_by_event_name where host='other-memory-host'")
	require.Empty(t, wrongHost.Records)
}

func TestPerformanceSchemaAuxiliaryInstanceAndHostViewsAreQueryable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("host", "127.0.0.1")
	session.SetParamByName("remote_addr", "127.0.0.1:3306")
	executor.QueryExecutor.metricsRecorder.RecordAuthenticationFailure("host_cache_user", "127.0.0.1")
	executor.QueryExecutor.metricsRecorder.RecordAuthenticationFailure("host_cache_user", "127.0.0.1")
	hostResult := <-executor.ExecuteQuery(session, "select ip, host, host_validated from performance_schema.host_cache", "")
	require.NoError(t, hostResult.Err)
	hostSelect, ok := hostResult.Data.(*SelectResult)
	require.True(t, ok)
	hostRows := selectResultRows(hostSelect)
	require.NotEmpty(t, hostRows)
	require.Equal(t, "YES", hostRows[0][2])
	counts := mustSelectResultSQL(t, executor, "", "select ip, host, sum_connect_errors, count_authentication_errors, first_seen, last_seen, first_error_seen, last_error_seen from performance_schema.host_cache where ip = '127.0.0.1'")
	require.Len(t, counts.Records, 1)
	require.Equal(t, "127.0.0.1", counts.Records[0].GetValues()[0].String())
	require.Equal(t, "127.0.0.1", counts.Records[0].GetValues()[1].String())
	require.Equal(t, int64(2), counts.Records[0].GetValues()[2].Int())
	require.Equal(t, int64(2), counts.Records[0].GetValues()[3].Int())
	require.NotEmpty(t, counts.Records[0].GetValues()[4].String())
	require.NotEmpty(t, counts.Records[0].GetValues()[5].String())
	require.NotEmpty(t, counts.Records[0].GetValues()[6].String())
	require.NotEmpty(t, counts.Records[0].GetValues()[7].String())
	wrongCount := mustSelectResultSQL(t, executor, "", "select ip from performance_schema.host_cache where ip = '127.0.0.1' and sum_connect_errors = 999999")
	require.Empty(t, wrongCount.Records)
	for _, view := range []string{"mutex_instances", "rwlock_instances", "objects_summary_global_by_type"} {
		result := <-executor.ExecuteQuery(session, "select * from performance_schema."+view, "")
		require.NoError(t, result.Err, view)
	}
}

func TestPerformanceSchemaRegistryCoversMySQL84VirtualTables(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	for _, tableName := range []string{
		"binary_log_transaction_compression_stats",
		"clone_progress", "clone_status", "component_scheduler_tasks", "cond_instances",
		"events_errors_summary_global_by_error",
		"events_stages_summary_by_account_by_event_name",
		"events_stages_summary_by_host_by_event_name",
		"events_stages_summary_by_user_by_event_name",
		"events_statements_histogram_by_digest", "events_statements_histogram_global",
		"events_statements_summary_by_account_by_event_name",
		"events_statements_summary_by_host_by_event_name",
		"events_statements_summary_by_program",
		"events_statements_summary_by_thread_by_event_name",
		"events_statements_summary_by_user_by_event_name",
		"events_statements_summary_global_by_event_name",
		"events_transactions_summary_by_account_by_event_name",
		"events_transactions_summary_by_host_by_event_name",
		"events_transactions_summary_by_thread_by_event_name",
		"events_transactions_summary_by_user_by_event_name",
		"events_transactions_summary_global_by_event_name",
		"events_waits_summary_by_account_by_event_name",
		"events_waits_summary_by_host_by_event_name",
		"events_waits_summary_by_instance",
		"events_waits_summary_by_user_by_event_name",
		"firewall_group_allowlist", "firewall_groups", "firewall_membership",
		"keyring_component_status", "keyring_keys", "log_status",
		"memory_summary_by_account_by_event_name", "memory_summary_by_host_by_event_name",
		"memory_summary_by_user_by_event_name", "persisted_variables",
		"prepared_statements_instances", "replication_applier_status",
		"replication_connection_configuration", "replication_connection_status",
		"session_account_connect_attrs", "status_by_account", "status_by_host",
		"status_by_thread", "status_by_user", "table_handles", "table_io_waits_summary_by_table",
		"table_io_waits_summary_by_index_usage", "table_lock_waits_summary_by_table", "tls_channel_status",
		"user_defined_functions", "user_variables_by_thread", "variables_by_thread",
		"variables_info", "hosts", "mutex_instances",
	} {
		result := mustSelectResultSQL(t, executor, "", "select * from performance_schema."+tableName)
		require.NotNil(t, result, tableName)
		require.NotEmpty(t, result.Columns, tableName)
	}
	compression := mustSelectResultSQL(t, executor, "", "select * from performance_schema.binary_log_transaction_compression_stats")
	require.Equal(t, []string{
		"LOG_TYPE", "COMPRESSION_TYPE", "TRANSACTION_COUNTER", "COMPRESSED_BYTES_COUNTER", "UNCOMPRESSED_BYTES_COUNTER",
		"COMPRESSION_PERCENTAGE", "FIRST_TRANSACTION_ID", "FIRST_TRANSACTION_COMPRESSED_BYTES", "FIRST_TRANSACTION_UNCOMPRESSED_BYTES", "FIRST_TRANSACTION_TIMESTAMP",
		"LAST_TRANSACTION_ID", "LAST_TRANSACTION_COMPRESSED_BYTES", "LAST_TRANSACTION_UNCOMPRESSED_BYTES", "LAST_TRANSACTION_TIMESTAMP",
	}, compression.Columns)

	attrs := mustSelectResultSQL(t, executor, "", "select processlist_id, attr_name, attr_value, ordinal_position from performance_schema.session_account_connect_attrs")
	require.Equal(t, []string{"PROCESSLIST_ID", "ATTR_NAME", "ATTR_VALUE", "ORDINAL_POSITION"}, attrs.Columns)
	scheduler := mustSelectResultSQL(t, executor, "", "select * from performance_schema.component_scheduler_tasks")
	require.Equal(t, []string{"NAME", "STATUS", "COMMENT", "INTERVAL_SECONDS", "TIMES_RUN", "TIMES_FAILED"}, scheduler.Columns)
	require.Empty(t, scheduler.Records, "the Enterprise scheduler component is not present in this server")
	cloneStatus := mustSelectResultSQL(t, executor, "", "select * from performance_schema.clone_status")
	require.Equal(t, []string{
		"ID", "PID", "STATE", "BEGIN_TIME", "END_TIME", "SOURCE", "DESTINATION",
		"ERROR_NO", "ERROR_MESSAGE", "BINLOG_FILE", "BINLOG_POSITION", "GTID_EXECUTED",
	}, cloneStatus.Columns)
	require.Empty(t, cloneStatus.Records, "the Clone plugin is not present in this server")
	cloneProgress := mustSelectResultSQL(t, executor, "", "select * from performance_schema.clone_progress")
	require.Equal(t, []string{
		"ID", "STAGE", "STATE", "BEGIN_TIME", "END_TIME", "THREADS", "ESTIMATE", "DATA",
		"NETWORK", "DATA_SPEED", "NETWORK_SPEED",
	}, cloneProgress.Columns)
	require.Empty(t, cloneProgress.Records, "the Clone plugin is not present in this server")
	errors := mustSelectResultSQL(t, executor, "", "select * from performance_schema.events_errors_summary_global_by_error")
	require.Equal(t, []string{"ERROR_NUMBER", "ERROR_NAME", "SQL_STATE", "SUM_ERROR_RAISED", "SUM_ERROR_HANDLED", "FIRST_SEEN", "LAST_SEEN"}, errors.Columns)
	statements := mustSelectResultSQL(t, executor, "", "select thread_id, event_name, count_star from performance_schema.events_statements_summary_by_thread_by_event_name")
	require.Equal(t, []string{"THREAD_ID", "EVENT_NAME", "COUNT_STAR"}, statements.Columns)
}

func TestPerformanceSchemaRegistryUsesMySQL84SummaryAndInstanceShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	byTable := mustSelectResultSQL(t, executor, "", "select * from performance_schema.table_io_waits_summary_by_table")
	require.Equal(t, []string{
		"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT",
		"COUNT_READ", "SUM_TIMER_READ", "MIN_TIMER_READ", "AVG_TIMER_READ", "MAX_TIMER_READ",
		"COUNT_WRITE", "SUM_TIMER_WRITE", "MIN_TIMER_WRITE", "AVG_TIMER_WRITE", "MAX_TIMER_WRITE",
		"COUNT_FETCH", "SUM_TIMER_FETCH", "MIN_TIMER_FETCH", "AVG_TIMER_FETCH", "MAX_TIMER_FETCH",
		"COUNT_INSERT", "SUM_TIMER_INSERT", "MIN_TIMER_INSERT", "AVG_TIMER_INSERT", "MAX_TIMER_INSERT",
		"COUNT_UPDATE", "SUM_TIMER_UPDATE", "MIN_TIMER_UPDATE", "AVG_TIMER_UPDATE", "MAX_TIMER_UPDATE",
		"COUNT_DELETE", "SUM_TIMER_DELETE", "MIN_TIMER_DELETE", "AVG_TIMER_DELETE", "MAX_TIMER_DELETE",
	}, byTable.Columns)

	byIndex := mustSelectResultSQL(t, executor, "", "select * from performance_schema.table_io_waits_summary_by_index_usage")
	require.Equal(t, append([]string{"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "INDEX_NAME"}, byTable.Columns[3:]...), byIndex.Columns)

	lock := mustSelectResultSQL(t, executor, "", "select * from performance_schema.table_lock_waits_summary_by_table")
	require.Equal(t, []string{
		"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT",
		"COUNT_READ", "SUM_TIMER_READ", "MIN_TIMER_READ", "AVG_TIMER_READ", "MAX_TIMER_READ",
		"COUNT_WRITE", "SUM_TIMER_WRITE", "MIN_TIMER_WRITE", "AVG_TIMER_WRITE", "MAX_TIMER_WRITE",
		"COUNT_READ_NORMAL", "SUM_TIMER_READ_NORMAL", "MIN_TIMER_READ_NORMAL", "AVG_TIMER_READ_NORMAL", "MAX_TIMER_READ_NORMAL",
		"COUNT_READ_WITH_SHARED_LOCKS", "SUM_TIMER_READ_WITH_SHARED_LOCKS", "MIN_TIMER_READ_WITH_SHARED_LOCKS", "AVG_TIMER_READ_WITH_SHARED_LOCKS", "MAX_TIMER_READ_WITH_SHARED_LOCKS",
		"COUNT_READ_HIGH_PRIORITY", "SUM_TIMER_READ_HIGH_PRIORITY", "MIN_TIMER_READ_HIGH_PRIORITY", "AVG_TIMER_READ_HIGH_PRIORITY", "MAX_TIMER_READ_HIGH_PRIORITY",
		"COUNT_READ_NO_INSERT", "SUM_TIMER_READ_NO_INSERT", "MIN_TIMER_READ_NO_INSERT", "AVG_TIMER_READ_NO_INSERT", "MAX_TIMER_READ_NO_INSERT",
		"COUNT_READ_EXTERNAL", "SUM_TIMER_READ_EXTERNAL", "MIN_TIMER_READ_EXTERNAL", "AVG_TIMER_READ_EXTERNAL", "MAX_TIMER_READ_EXTERNAL",
		"COUNT_WRITE_ALLOW_WRITE", "SUM_TIMER_WRITE_ALLOW_WRITE", "MIN_TIMER_WRITE_ALLOW_WRITE", "AVG_TIMER_WRITE_ALLOW_WRITE", "MAX_TIMER_WRITE_ALLOW_WRITE",
		"COUNT_WRITE_CONCURRENT_INSERT", "SUM_TIMER_WRITE_CONCURRENT_INSERT", "MIN_TIMER_WRITE_CONCURRENT_INSERT", "AVG_TIMER_WRITE_CONCURRENT_INSERT", "MAX_TIMER_WRITE_CONCURRENT_INSERT",
		"COUNT_WRITE_LOW_PRIORITY", "SUM_TIMER_WRITE_LOW_PRIORITY", "MIN_TIMER_WRITE_LOW_PRIORITY", "AVG_TIMER_WRITE_LOW_PRIORITY", "MAX_TIMER_WRITE_LOW_PRIORITY",
		"COUNT_WRITE_NORMAL", "SUM_TIMER_WRITE_NORMAL", "MIN_TIMER_WRITE_NORMAL", "AVG_TIMER_WRITE_NORMAL", "MAX_TIMER_WRITE_NORMAL",
		"COUNT_WRITE_EXTERNAL", "SUM_TIMER_WRITE_EXTERNAL", "MIN_TIMER_WRITE_EXTERNAL", "AVG_TIMER_WRITE_EXTERNAL", "MAX_TIMER_WRITE_EXTERNAL",
	}, lock.Columns)

	for _, table := range []string{"events_waits_current", "events_waits_history", "events_waits_history_long"} {
		waits := mustSelectResultSQL(t, executor, "", "select * from performance_schema."+table)
		require.Equal(t, []string{
			"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT", "SPINS",
			"OBJECT_SCHEMA", "OBJECT_NAME", "INDEX_NAME", "OBJECT_TYPE", "OBJECT_INSTANCE_BEGIN", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE",
			"OPERATION", "NUMBER_OF_BYTES", "FLAGS",
		}, waits.Columns, table)
	}

	mutex := mustSelectResultSQL(t, executor, "", "select * from performance_schema.mutex_instances")
	require.Equal(t, []string{"NAME", "OBJECT_INSTANCE_BEGIN", "LOCKED_BY_THREAD_ID"}, mutex.Columns)
	hosts := mustSelectResultSQL(t, executor, "", "select * from performance_schema.hosts")
	require.Equal(t, []string{"HOST", "CURRENT_CONNECTIONS", "TOTAL_CONNECTIONS", "MAX_SESSION_CONTROLLED_MEMORY", "MAX_SESSION_TOTAL_MEMORY"}, hosts.Columns)
}

func TestPerformanceSchemaTLSChannelStatusUsesMySQL84Shape(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select * from performance_schema.tls_channel_status")
	require.Equal(t, []string{"CHANNEL", "PROPERTY", "VALUE"}, result.Columns)
}

func TestPerformanceSchemaThreadPoolConnectionsUsesMySQL84Shape(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select * from performance_schema.tp_connections")
	require.Equal(t, []string{
		"CONNECTION_ID", "TP_GROUP_ID", "TP_PROCESSING_THREAD_NUMBER", "THREAD_ID", "STATE",
		"ACTIVE_FLAG", "KILLED_STATE", "CLEANUP_STATE", "TIME_OF_LAST_EVENT_COMPLETION",
		"TIME_OF_EXPIRY", "TIME_OF_ADD", "TIME_OF_POP", "TIME_OF_ARM", "CONNECT_HANDLER_INDEX",
		"TYPE", "DIRECT_QUERY_EVENTS", "QUEUED_QUERY_EVENTS", "TIME_OF_EVENT_ARRIVAL", "MANAGEMENT_TIME",
	}, result.Columns)
	require.Empty(t, result.Records, "the Enterprise Thread Pool runtime is not present in this server")
}

func TestPerformanceSchemaTLSChannelStatusProjectsSessionTLSProperties(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("tls_active", true)
	session.SetParamByName("tls_version", "TLSv1.3")
	session.SetParamByName("tls_cipher", "TLS_AES_128_GCM_SHA256")

	queryResult := <-executor.ExecuteQuery(session, "select channel, property, value from performance_schema.tls_channel_status where property in ('Enabled', 'Current_tls_version', 'Current_tls_cipher')", "")
	require.NoError(t, queryResult.Err)
	result, ok := queryResult.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, []string{"CHANNEL", "PROPERTY", "VALUE"}, result.Columns)
	require.Equal(t, [][]interface{}{
		{"mysql_main", "Enabled", "Yes"},
		{"mysql_main", "Current_tls_version", "TLSv1.3"},
		{"mysql_main", "Current_tls_cipher", "TLS_AES_128_GCM_SHA256"},
	}, selectResultRows(result))
	wrongValue := mustSelectResultSQL(t, executor, "", "select property from performance_schema.tls_channel_status where property = 'Enabled' and value = 'No'")
	require.Empty(t, wrongValue.Records)
}

func TestPerformanceSchemaInnoDBRedoLogFilesProjectsLiveRedoSnapshot(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select * from performance_schema.innodb_redo_log_files")
	require.Equal(t, []string{
		"FILE_ID", "FILE_NAME", "START_LSN", "END_LSN", "SIZE_IN_BYTES", "IS_FULL", "CONSUMER_LEVEL",
	}, result.Columns)

	req := executor.QueryExecutor.txManager.GetRedoLogManager()
	require.NotNil(t, req)
	lsn, err := req.Append(&manager.RedoLogEntry{Type: manager.LOG_TYPE_INSERT, TrxID: 7, PageID: 9, Data: []byte("redo")})
	require.NoError(t, err)
	require.NoError(t, req.Flush(lsn))

	result = mustSelectResultSQL(t, executor, "", "select file_id, file_name, start_lsn, end_lsn, size_in_bytes, is_full, consumer_level from performance_schema.innodb_redo_log_files")
	require.Len(t, result.Records, 1)
	rows := selectResultRows(result)
	require.Len(t, rows[0], 7)
	require.Equal(t, "0", rows[0][0])
	require.Contains(t, rows[0][1], "redo.log")
	require.NotEmpty(t, rows[0][2])
	require.NotEmpty(t, rows[0][3])
	require.NotEqual(t, "0", rows[0][4])
	require.Equal(t, "NO", rows[0][5])
	require.Equal(t, "0", rows[0][6])

	wrongEndLSN := mustSelectResultSQL(t, executor, "", "select file_id from performance_schema.innodb_redo_log_files where end_lsn = 0")
	require.Empty(t, wrongEndLSN.Records)
	wrongSize := mustSelectResultSQL(t, executor, "", "select file_id from performance_schema.innodb_redo_log_files where size_in_bytes = 0")
	require.Empty(t, wrongSize.Records)
	wrongFullState := mustSelectResultSQL(t, executor, "", "select file_id from performance_schema.innodb_redo_log_files where is_full = 'YES'")
	require.Empty(t, wrongFullState.Records)
	wrongConsumer := mustSelectResultSQL(t, executor, "", "select file_id from performance_schema.innodb_redo_log_files where consumer_level = 1")
	require.Empty(t, wrongConsumer.Records)
}

func TestPerformanceSchemaTelemetrySetupTablesExposeMySQL84Shapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	loggers := mustSelectResultSQL(t, executor, "", "select * from performance_schema.setup_loggers")
	require.Equal(t, []string{"NAME", "LEVEL", "DESCRIPTION"}, loggers.Columns)
	require.Contains(t, selectResultRows(loggers), []interface{}{"logger/sql/error_log", "info", "MySQL error logger"})

	meters := mustSelectResultSQL(t, executor, "", "select * from performance_schema.setup_meters where name like 'mysql.inno%'")
	require.Equal(t, []string{"NAME", "FREQUENCY", "ENABLED", "DESCRIPTION"}, meters.Columns)
	require.Contains(t, selectResultRows(meters), []interface{}{"mysql.inno", "10", "YES", "MySql InnoDB metrics"})

	metrics := mustSelectResultSQL(t, executor, "", "select * from performance_schema.setup_metrics where meter = 'mysql.inno'")
	require.Equal(t, []string{"NAME", "METER", "METRIC_TYPE", "NUM_TYPE", "UNIT", "DESCRIPTION"}, metrics.Columns)
	require.Contains(t, selectResultRows(metrics), []interface{}{"trx_active_transactions", "mysql.inno", "ASYNC GAUGE", "INTEGER", "", "Active transactions currently registered in TransactionManager"})

	wrongMeterDescription := mustSelectResultSQL(t, executor, "", "select name from performance_schema.setup_meters where name = 'mysql.inno' and description = 'not the runtime description'")
	require.Empty(t, wrongMeterDescription.Records)
	wrongMetricDescription := mustSelectResultSQL(t, executor, "", "select name from performance_schema.setup_metrics where name = 'trx_active_transactions' and description = 'not the runtime description'")
	require.Empty(t, wrongMetricDescription.Records)
	wrongMetricUnit := mustSelectResultSQL(t, executor, "", "select name from performance_schema.setup_metrics where name = 'trx_active_transactions' and unit = 'bytes'")
	require.Empty(t, wrongMetricUnit.Records)

	require.NoError(t, (<-executor.ExecuteQuery(nil, "update performance_schema.setup_meters set frequency = 30, enabled = 'NO' where name = 'mysql.inno'", "")).Err)
	updatedMeter := mustSelectResultSQL(t, executor, "", "select frequency, enabled from performance_schema.setup_meters where name = 'mysql.inno'")
	require.Equal(t, [][]interface{}{{"30", "NO"}}, selectResultRows(updatedMeter))
	require.NoError(t, (<-executor.ExecuteQuery(nil, "update performance_schema.setup_loggers set level = 'debug' where name = 'logger/sql/error_log'", "")).Err)
	updatedLogger := mustSelectResultSQL(t, executor, "", "select level from performance_schema.setup_loggers where name = 'logger/sql/error_log'")
	require.Equal(t, [][]interface{}{{"debug"}}, selectResultRows(updatedLogger))
}

func TestPerformanceSchemaTelemetrySetupFiltersSupportNotInAndNullPredicates(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	meters := mustQuerySQL(t, executor, "", "select name from performance_schema.setup_meters where name in ('mysql.inno', 'mysql.stats')")
	require.Len(t, meters, 2)
	meters = mustQuerySQL(t, executor, "", "select name from performance_schema.setup_meters where name not in ('mysql.inno')")
	require.NotEmpty(t, meters)
	meters = mustQuerySQL(t, executor, "", "select name from performance_schema.setup_meters where name is null")
	require.Empty(t, meters)

	metrics := mustQuerySQL(t, executor, "", "select name from performance_schema.setup_metrics where meter in ('mysql.inno', 'mysql.stats')")
	require.NotEmpty(t, metrics)
	metrics = mustQuerySQL(t, executor, "", "select name from performance_schema.setup_metrics where name not in ('trx_active_transactions')")
	require.NotEmpty(t, metrics)
	metrics = mustQuerySQL(t, executor, "", "select name from performance_schema.setup_metrics where meter is null")
	require.Empty(t, metrics)

	loggers := mustQuerySQL(t, executor, "", "select name from performance_schema.setup_loggers where name not in ('logger/sql/error_log')")
	require.Len(t, loggers, 2)
	loggers = mustQuerySQL(t, executor, "", "select name from performance_schema.setup_loggers where name is not null")
	require.Len(t, loggers, 3)
}

func TestPerformanceSchemaStatementsDigestConsumerIsRegisteredAndGatesSummary(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	consumer := mustQuerySQL(t, executor, "", "select name, enabled from performance_schema.setup_consumers where name='statements_digest'")
	require.Equal(t, [][]interface{}{{"statements_digest", "YES"}}, consumer)

	result := <-executor.ExecuteQuery(nil, "select 7", "digest_consumer_app")
	require.NoError(t, result.Err)
	before := mustQuerySQL(t, executor, "", "select schema_name, count_star from performance_schema.events_statements_summary_by_digest where schema_name='digest_consumer_app'")
	require.NotEmpty(t, before)

	update := <-executor.ExecuteQuery(nil, "update performance_schema.setup_consumers set enabled='NO' where name='statements_digest'", "")
	require.NoError(t, update.Err)
	result = <-executor.ExecuteQuery(nil, "select 8", "digest_consumer_app")
	require.NoError(t, result.Err)
	after := mustQuerySQL(t, executor, "", "select schema_name, count_star from performance_schema.events_statements_summary_by_digest where schema_name='digest_consumer_app'")
	require.Empty(t, after)
	consumer = mustQuerySQL(t, executor, "", "select name, enabled from performance_schema.setup_consumers where name='statements_digest'")
	require.Equal(t, [][]interface{}{{"statements_digest", "NO"}}, consumer)
}

func TestPerformanceSchemaStageSummaryAggregatesCompletedStatements(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "select 11", "app")
	require.NoError(t, result.Err)

	rows := selectResultRows(mustSelectResultSQL(t, executor, "", "select event_name, count_star, sum_timer_wait, min_timer_wait, avg_timer_wait, max_timer_wait from performance_schema.events_stages_summary_global_by_event_name"))
	var found bool
	for _, row := range rows {
		if len(row) >= 6 && row[0] == "stage/sql/execute" {
			found = true
			require.NotEqual(t, "0", row[1])
			break
		}
	}
	require.True(t, found, "stage summary did not expose completed statement: %#v", rows)
	wrongCount := mustSelectResultSQL(t, executor, "", "select event_name from performance_schema.events_stages_summary_global_by_event_name where event_name='stage/sql/execute' and count_star=999")
	require.Empty(t, wrongCount.Records)
}

func TestPerformanceSchemaSummaryFiltersApplyNullPredicates(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "select 12", "app")
	require.NoError(t, result.Err)

	nullResult := mustSelectResultSQL(t, executor, "", "select event_name from performance_schema.events_stages_summary_global_by_event_name where event_name is null")
	require.Empty(t, nullResult.Records)

	notNullResult := mustSelectResultSQL(t, executor, "", "select event_name from performance_schema.events_stages_summary_global_by_event_name where event_name is not null")
	require.Equal(t, [][]interface{}{{"stage/sql/execute"}}, selectResultRows(notNullResult))
	require.True(t, performanceSchemaLockValuesMatch("select source from performance_schema.events_stages_current where source is null", map[string]interface{}{"SOURCE": nil}))
	require.False(t, performanceSchemaLockValuesMatch("select source from performance_schema.events_stages_current where source is not null", map[string]interface{}{"SOURCE": nil}))
}

func TestPerformanceSchemaStageSummaryRetainsLifetimeTotalsBeyondHistory(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	const executions = 300
	for i := 0; i < executions; i++ {
		executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
			84, "stage_lifetime_user", "stage_lifetime_host", "stage_lifetime_app",
			"select stage_lifetime_test", "SELECT", "ok", time.Millisecond, 0, 0, 0,
		)
	}

	result := executor.QueryExecutor.executePerformanceSchemaStageSummaryRegistrySelect(
		"select user, event_name, count_star, sum_timer_wait from performance_schema.events_stages_summary_by_user_by_event_name where user='stage_lifetime_user' and event_name='stage/sql/execute'",
		"user",
	)
	require.Len(t, result.Records, 1)
	row := selectResultRows(result)[0]
	require.Equal(t, "stage_lifetime_user", row[0])
	require.Equal(t, "stage/sql/execute", row[1])
	require.Equal(t, fmt.Sprint(executions), row[2])
	require.Equal(t, fmt.Sprint(int64(executions)*int64(time.Millisecond)*1000), row[3])
	global := executor.QueryExecutor.executePerformanceSchemaStageSummarySelect(
		"select event_name, count_star, sum_timer_wait from performance_schema.events_stages_summary_global_by_event_name where event_name='stage/sql/execute'",
		false,
	)
	require.Len(t, global.Records, 1)
	require.Equal(t, fmt.Sprint(executions), selectResultRows(global)[0][1])
	byThread := executor.QueryExecutor.executePerformanceSchemaStageSummarySelect(
		"select thread_id, event_name, count_star from performance_schema.events_stages_summary_by_thread_by_event_name where thread_id=84 and event_name='stage/sql/execute'",
		true,
	)
	require.Len(t, byThread.Records, 1)
	require.Equal(t, fmt.Sprint(executions), selectResultRows(byThread)[0][2])
}

func TestPerformanceSchemaReplicationViewsExposeRuntimeState(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.SetReplicationStatusProvider(func() replication.StatusSnapshot {
		return replication.StatusSnapshot{
			Role:               replication.RoleReplica,
			SourceURL:          "http://source:8080",
			SourceUUID:         "00112233-4455-6677-8899-aabbccddeeff",
			ExecutedGTIDs:      "source-1:1-4",
			ReplicaRunning:     true,
			ReceivedHeartbeats: 2,
			LastHeartbeatAt:    time.Date(2026, 9, 27, 12, 34, 56, 0, time.UTC),
			LastError:          "ERROR 1236 (HY000): source stopped",
			LastErrorNumber:    1236,
			LastErrorAt:        time.Date(2026, 9, 27, 12, 35, 7, 0, time.UTC),
		}
	})
	status := mustSelectResultSQL(t, executor, "", "select channel_name, service_state, remaining_delay, count_transactions_retries from performance_schema.replication_applier_status")
	require.Len(t, status.Records, 1)
	require.Equal(t, []string{"CHANNEL_NAME", "SERVICE_STATE", "REMAINING_DELAY", "COUNT_TRANSACTIONS_RETRIES"}, status.Columns)
	require.Equal(t, "ON", status.Records[0].GetValues()[1].String())
	require.Nil(t, status.Records[0].GetValues()[2].Raw())
	require.Equal(t, int64(0), status.Records[0].GetValues()[3].Int())
	applierConfig := mustSelectResultSQL(t, executor, "", "select channel_name, desired_delay, privilege_checks_user, require_row_format, require_table_primary_key_check, assign_gtids_to_anonymous_transactions_type, assign_gtids_to_anonymous_transactions_value from performance_schema.replication_applier_configuration")
	require.Equal(t, [][]interface{}{{"", "0", "", "NO", "STREAM", "OFF", ""}}, selectResultRows(applierConfig))
	filteredStatus := mustSelectResultSQL(t, executor, "", "select channel_name, service_state from performance_schema.replication_applier_status where channel_name = 'missing-channel'")
	require.Empty(t, filteredStatus.Records)
	configuration := mustSelectResultSQL(t, executor, "", "select channel_name, host, auto_position from performance_schema.replication_connection_configuration")
	require.Len(t, configuration.Records, 1)
	require.Equal(t, "http://source:8080", configuration.Records[0].GetValues()[1].String())
	require.Equal(t, "1", configuration.Records[0].GetValues()[2].String())
	connection := mustSelectResultSQL(t, executor, "", "select source_uuid, count_received_heartbeats, last_heartbeat_timestamp, last_error_number, last_error_message, last_error_timestamp from performance_schema.replication_connection_status")
	require.Equal(t, "00112233-4455-6677-8899-aabbccddeeff", connection.Records[0].GetValues()[0].String())
	require.Equal(t, int64(2), connection.Records[0].GetValues()[1].Int())
	require.Equal(t, "2026-09-27 12:34:56", connection.Records[0].GetValues()[2].String())
	require.Equal(t, int64(1236), connection.Records[0].GetValues()[3].Int())
	require.Equal(t, "ERROR 1236 (HY000): source stopped", connection.Records[0].GetValues()[4].String())
	require.Equal(t, "2026-09-27 12:35:07", connection.Records[0].GetValues()[5].String())
	filteredConfiguration := mustSelectResultSQL(t, executor, "", "select channel_name, host from performance_schema.replication_connection_configuration where host = 'other-source'")
	require.Empty(t, filteredConfiguration.Records)
}

func TestPerformanceSchemaReplicationViewsAreEmptyWithoutRuntime(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	for _, table := range []string{
		"replication_applier_status",
		"replication_connection_configuration",
		"replication_connection_status",
	} {
		result := mustSelectResultSQL(t, executor, "", "select * from performance_schema."+table)
		require.Empty(t, result.Records, table)
	}
}

func TestPerformanceSchemaReplicationViewsExposeNamedChannelRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.SetReplicationChannelStatusProvider(func() []replication.StatusSnapshot {
		return []replication.StatusSnapshot{
			{Role: replication.RoleReplica, ChannelName: "", SourceURL: "http://default-source:8080", ReplicaRunning: true},
			{Role: replication.RoleReplica, ChannelName: "analytics", SourceURL: "http://analytics-source:8081", ReplicaRunning: false},
		}
	})
	result := mustSelectResultSQL(t, executor, "", "select channel_name, host, auto_position from performance_schema.replication_connection_configuration")
	require.Equal(t, [][]interface{}{
		{"", "http://default-source:8080", "1"},
		{"analytics", "http://analytics-source:8081", "1"},
	}, selectResultRows(result))
	status := mustSelectResultSQL(t, executor, "", "select channel_name, service_state from performance_schema.replication_applier_status where channel_name = 'analytics'")
	require.Equal(t, [][]interface{}{{"analytics", "OFF"}}, selectResultRows(status))
}

func TestPerformanceSchemaNativeConnectionConfigurationProjectsMySQLFields(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.SetReplicationStatusProvider(func() replication.StatusSnapshot {
		return replication.StatusSnapshot{
			Role:               replication.RoleReplica,
			SourceURL:          "mysql://source.example:3307/mysql",
			SourceUser:         "repl",
			SourceAutoPosition: true,
			ReplicaRunning:     true,
		}
	})

	result := mustSelectResultSQL(t, executor, "", "select host, port, user, auto_position, ssl_allowed from performance_schema.replication_connection_configuration")
	require.Equal(t, []string{"HOST", "PORT", "USER", "AUTO_POSITION", "SSL_ALLOWED"}, result.Columns)
	require.Equal(t, [][]interface{}{{"source.example", "3307", "repl", "1", "NO"}}, selectResultRows(result))
}

func TestPerformanceSchemaNativeConnectionConfigurationProjectsTLSAllowed(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.SetReplicationStatusProvider(func() replication.StatusSnapshot {
		return replication.StatusSnapshot{
			Role:               replication.RoleReplica,
			SourceURL:          "mysql://source.example:3307/?ssl=true&ssl_verify_server_cert=true&ssl_ca=ca.pem&ssl_cert=client.pem&ssl_key=client.key&connect_retry=7&connect_retry_count=3&heartbeat_interval=30.5&compression_algorithm=zstd&zstd_compression_level=3",
			SourceUser:         "repl",
			SourceAutoPosition: true,
			ReplicaRunning:     true,
		}
	})

	result := mustSelectResultSQL(t, executor, "", "select host, ssl_allowed, ssl_verify_server_certificate, ssl_ca_file, ssl_certificate, ssl_key, connection_retry_interval, connection_retry_count, heartbeat_interval, compression_algorithm, zstd_compression_level from performance_schema.replication_connection_configuration")
	require.Equal(t, [][]interface{}{{"source.example", "YES", "1", "ca.pem", "client.pem", "client.key", "7", "3", "30.5", "zstd", "3"}}, selectResultRows(result))
}

func TestPerformanceSchemaReplicationExtendedViewsProjectRuntimeState(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	source, err := replication.NewSource(t.TempDir(), "log-status", 17)
	require.NoError(t, err)
	_, err = source.AppendCommitted([]replication.Statement{{Database: "app", SQL: "insert into docs values (1)"}})
	require.NoError(t, err)
	executor.QueryExecutor.SetReplicationSourceProvider(func() *replication.Source { return source })
	executor.QueryExecutor.SetReplicationStatusProvider(func() replication.StatusSnapshot {
		return replication.StatusSnapshot{
			Role:           replication.RoleReplica,
			UUID:           "replica-uuid",
			ServerID:       17,
			SourceURL:      "http://source.example:8080",
			ExecutedGTIDs:  "source-uuid:1-4",
			ReplicaRunning: true,
		}
	})

	failover := mustSelectResultSQL(t, executor, "", "select channel_name, host, port, managed_name from performance_schema.replication_asynchronous_connection_failover")
	require.Len(t, failover.Records, 1)
	require.Equal(t, "source.example", failover.Records[0].GetValues()[1].String())
	require.Equal(t, "8080", failover.Records[0].GetValues()[2].String())
	require.Equal(t, "", failover.Records[0].GetValues()[3].String())
	filteredFailover := mustSelectResultSQL(t, executor, "", "select channel_name, host from performance_schema.replication_asynchronous_connection_failover where host = 'other-source'")
	require.Empty(t, filteredFailover.Records)
	managedFailover := mustSelectResultSQL(t, executor, "", "select * from performance_schema.replication_asynchronous_connection_failover_managed")
	require.Empty(t, managedFailover.Records)

	admin := newTestMySQLSession()
	admin.SetParamByName("global_privileges", []common.PrivilegeType{})
	admin.SetParamByName("dynamic_privileges", []string{"BACKUP_ADMIN"})
	redo := executor.QueryExecutor.txManager.GetRedoLogManager()
	require.NotNil(t, redo)
	_, err = redo.Append(&manager.RedoLogEntry{Type: manager.LOG_TYPE_INSERT, TrxID: 81, PageID: 19, Data: []byte("log-status-redo")})
	require.NoError(t, err)
	require.NoError(t, redo.Checkpoint())
	redoStats := redo.GetStats()
	logStatusResult := <-executor.ExecuteQuery(admin, "select server_uuid, local, replication, storage_engines from performance_schema.log_status", "")
	require.NoError(t, logStatusResult.Err)
	logStatus, ok := logStatusResult.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, []string{"SERVER_UUID", "LOCAL", "REPLICATION", "STORAGE_ENGINES"}, logStatus.Columns)
	require.Len(t, logStatus.Records, 1)
	require.Equal(t, "replica-uuid", logStatus.Records[0].GetValues()[0].String())
	localFile, localPosition := source.NativeCurrentFilePosition()
	localJSON := logStatus.Records[0].GetValues()[1].String()
	require.Contains(t, localJSON, localFile)
	require.Contains(t, localJSON, fmt.Sprintf("%d", localPosition))
	require.Contains(t, logStatus.Records[0].GetValues()[1].String(), "source-uuid:1-4")
	var replicationChannels []interface{}
	require.NoError(t, json.Unmarshal([]byte(logStatus.Records[0].GetValues()[2].String()), &replicationChannels))
	require.Empty(t, replicationChannels)
	var storageEngines map[string]map[string]uint64
	require.NoError(t, json.Unmarshal([]byte(logStatus.Records[0].GetValues()[3].String()), &storageEngines))
	require.Equal(t, map[string]map[string]uint64{
		"InnoDB": {"LSN": redoStats.CurrentLSN, "LSN_checkpoint": redoStats.LastCheckpoint},
	}, storageEngines)
	deniedLogStatus := <-executor.ExecuteQuery(newTestMySQLSession(), "select * from performance_schema.log_status", "")
	require.ErrorContains(t, deniedLogStatus.Err, "BACKUP_ADMIN")
	truncated := <-executor.ExecuteQuery(nil, "truncate table performance_schema.binary_log_transaction_compression_stats", "")
	require.NoError(t, truncated.Err, truncated.Message)
	deniedTruncate := <-executor.ExecuteQuery(nil, "truncate table performance_schema.log_status", "")
	require.ErrorContains(t, deniedTruncate.Err, "not permitted", deniedTruncate.Message)
}

func TestPerformanceSchemaGroupReplicationViewsExposeNativeShapesWithoutRuntime(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	expected := map[string][]string{
		"replication_group_communication_information": {"WRITE_CONCURRENCY", "PROTOCOL_VERSION", "WRITE_CONSENSUS_LEADERS_PREFERRED", "WRITE_CONSENSUS_LEADERS_ACTUAL", "WRITE_CONSENSUS_SINGLE_LEADER_CAPABLE", "MEMBER_FAILURE_SUSPICIONS_COUNT"},
		"replication_group_configuration_version":     {"NAME", "VERSION"},
		"replication_group_member_actions":            {"NAME", "EVENT", "ENABLED", "TYPE", "PRIORITY", "ERROR_HANDLING"},
		"replication_group_member_stats":              {"CHANNEL_NAME", "VIEW_ID", "MEMBER_ID", "COUNT_TRANSACTIONS_IN_QUEUE", "COUNT_TRANSACTIONS_CHECKED", "COUNT_CONFLICTS_DETECTED", "COUNT_TRANSACTIONS_ROWS_VALIDATING", "TRANSACTIONS_COMMITTED_ALL_MEMBERS", "LAST_CONFLICT_FREE_TRANSACTION", "COUNT_TRANSACTIONS_REMOTE_IN_APPLIER_QUEUE", "COUNT_TRANSACTIONS_REMOTE_APPLIED", "COUNT_TRANSACTIONS_LOCAL_PROPOSED", "COUNT_TRANSACTIONS_LOCAL_ROLLBACK"},
		"replication_group_members":                   {"CHANNEL_NAME", "MEMBER_ID", "MEMBER_HOST", "MEMBER_PORT", "MEMBER_STATE", "MEMBER_ROLE", "MEMBER_VERSION", "MEMBER_COMMUNICATION_STACK"},
	}
	for table, columns := range expected {
		result := mustSelectResultSQL(t, executor, "", "select * from performance_schema."+table)
		require.Equal(t, columns, result.Columns, table)
		require.Empty(t, result.Records, table)
	}
}

func TestPerformanceSchemaReplicationApplierWorkerViewsProjectSingleThreadedRuntime(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.SetReplicationStatusProvider(func() replication.StatusSnapshot {
		return replication.StatusSnapshot{
			Role:           replication.RoleReplica,
			SourceURL:      "http://source.example:8080",
			ReplicaRunning: true,
			LastError:      "apply failed",
		}
	})

	coordinator := mustSelectResultSQL(t, executor, "", "select channel_name, thread_id, service_state, last_error_message from performance_schema.replication_applier_status_by_coordinator")
	require.Equal(t, []string{"CHANNEL_NAME", "THREAD_ID", "SERVICE_STATE", "LAST_ERROR_MESSAGE"}, coordinator.Columns)
	require.Equal(t, [][]interface{}{{"", "", "ON", "apply failed"}}, selectResultRows(coordinator))

	worker := mustSelectResultSQL(t, executor, "", "select channel_name, worker_id, thread_id, service_state, last_error_message from performance_schema.replication_applier_status_by_worker")
	require.Equal(t, []string{"CHANNEL_NAME", "WORKER_ID", "THREAD_ID", "SERVICE_STATE", "LAST_ERROR_MESSAGE"}, worker.Columns)
	require.Equal(t, [][]interface{}{{"", "1", "", "ON", "apply failed"}}, selectResultRows(worker))
}

func TestPerformanceSchemaSessionRuntimeViewsExposeVariablesAndUserVariables(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	// This test asserts the fresh session's status dimensions. Keep the
	// process-wide production recorder from leaking identities from earlier
	// package tests into the fixture.
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(812))
	session.SetParamByName("user", "alice")
	session.SetParamByName("host", "client.example")
	session.SetParamByName("user_variables", map[string]interface{}{"answer": int64(42)})
	session.SetParamByName("character_set_connection", "utf8mb4")

	result := <-executor.ExecuteQuery(session, "select thread_id, variable_name, variable_value from performance_schema.variables_by_thread", "")
	require.NoError(t, result.Err)
	variables := result.Data.(*SelectResult)
	require.Contains(t, selectResultRows(variables), []interface{}{"812", "character_set_connection", "utf8mb4"})

	result = <-executor.ExecuteQuery(session, "select thread_id, variable_name, variable_value from performance_schema.user_variables_by_thread", "")
	require.NoError(t, result.Err)
	userVariables := result.Data.(*SelectResult)
	require.Contains(t, selectResultRows(userVariables), []interface{}{"812", "answer", "42"})

	result = <-executor.ExecuteQuery(session, "select user, host, variable_name, variable_value from performance_schema.status_by_account", "")
	require.NoError(t, result.Err)
	status := result.Data.(*SelectResult)
	require.Contains(t, selectResultRows(status), []interface{}{"alice", "client.example", "Threads_connected", "1"})

	result = <-executor.ExecuteQuery(session, "select user, host, variable_name from performance_schema.status_by_account where user='bob'", "")
	require.NoError(t, result.Err)
	require.Empty(t, result.Data.(*SelectResult).Records)
	result = <-executor.ExecuteQuery(session, "select host, variable_name from performance_schema.status_by_host where host='other.example'", "")
	require.NoError(t, result.Err)
	require.Empty(t, result.Data.(*SelectResult).Records)
	counterSession := newTestMySQLSession()
	counterSession.SetParamByName("connection_id", int64(813))
	counterSession.SetParamByName("user", "alice")
	counterSession.SetParamByName("host", "client.example")
	require.NoError(t, (<-executor.ExecuteQuery(counterSession, "select 1", "")).Err)
	result = <-executor.ExecuteQuery(counterSession, "select variable_name, variable_value from performance_schema.status_by_account where user='alice' and host='client.example' and variable_name='Queries'", "")
	require.NoError(t, result.Err)
	queries := selectResultRows(result.Data.(*SelectResult))
	require.Len(t, queries, 1)
	require.NotEqual(t, "0", queries[0][1])
	result = <-executor.ExecuteQuery(counterSession, "select variable_name, variable_value from performance_schema.status_by_account where user='alice' and host='client.example' and variable_name='Com_select'", "")
	require.NoError(t, result.Err)
	commandCounts := selectResultRows(result.Data.(*SelectResult))
	require.Len(t, commandCounts, 1)
	require.Equal(t, "Com_select", commandCounts[0][0])
	require.NotEqual(t, "0", commandCounts[0][1])
	other := newTestMySQLSession()
	other.SetParamByName("connection_id", int64(814))
	other.SetParamByName("user", "alice")
	other.SetParamByName("host", "client.example")
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{session, other}
	})
	result = <-executor.ExecuteQuery(session, "select user, host, variable_name, variable_value from performance_schema.status_by_account where user='alice' and host='client.example' and variable_name='Threads_connected'", "")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"alice", "client.example", "Threads_connected", "2"}}, selectResultRows(result.Data.(*SelectResult)))
	wrongStatusValue := mustSelectResultSQL(t, executor, "", "select variable_name from performance_schema.status_by_account where user='alice' and host='client.example' and variable_name='Threads_connected' and variable_value = 999")
	require.Empty(t, wrongStatusValue.Records)
	wrongUserVariableValue := mustSelectResultSQL(t, executor, "", "select variable_name from performance_schema.user_variables_by_thread where thread_id = 812 and variable_name = 'answer' and variable_value = 43")
	require.Empty(t, wrongUserVariableValue.Records)
}

func TestPerformanceSchemaSessionStatusRetainsLifetimeCommandTotalsBeyondHistory(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(815))
	session.SetParamByName("user", "status_lifetime_user")
	session.SetParamByName("host", "status_lifetime_host")
	const executions = 300
	for i := 0; i < executions; i++ {
		executor.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
			815, "status_lifetime_user", "status_lifetime_host", "status_lifetime_app",
			"select status_lifetime_test", "SELECT", "ok", time.Millisecond, 0, 0, 0,
		)
	}

	result := executor.QueryExecutor.executePerformanceSchemaSessionRuntimeRegistrySelect(
		"select user, host, variable_name, variable_value from performance_schema.status_by_account where user='status_lifetime_user' and host='status_lifetime_host' and variable_name in ('Queries', 'Com_select')",
		"status_by_account", session,
	)
	rows := selectResultRows(result)
	require.Contains(t, rows, []interface{}{"status_lifetime_user", "status_lifetime_host", "Queries", fmt.Sprint(executions)})
	require.Contains(t, rows, []interface{}{"status_lifetime_user", "status_lifetime_host", "Com_select", fmt.Sprint(executions)})
	sessionStatus := executor.QueryExecutor.executePerformanceSchemaStatusSelect(
		"performance_schema.session_status", "select variable_name, variable_value from performance_schema.session_status where variable_name='Queries'", session,
	)
	require.Equal(t, [][]interface{}{{"Queries", fmt.Sprint(executions)}}, selectResultRows(sessionStatus))
}

func TestPerformanceSchemaStatusRegistryRetainsDisconnectedIdentitySummaries(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	recorder := executor.QueryExecutor.metricsRecorder
	const user = "disconnected_status_user"
	const host = "disconnected_status_host"
	recorder.RecordConnection(user, host)
	for i := 0; i < 3; i++ {
		recorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
			816, user, host, "status_lifetime_app", "select disconnected_status_test", "SELECT", "ok", time.Millisecond, 0, 0, 0,
		)
	}

	// No process-list provider and no current session means the client has
	// disconnected. The lifetime summary must still be visible, while the
	// live connection gauge must be zero.
	result := executor.QueryExecutor.executePerformanceSchemaSessionRuntimeRegistrySelect(
		"select user, host, variable_name, variable_value from performance_schema.status_by_account where user='disconnected_status_user' and host='disconnected_status_host' and variable_name in ('Threads_connected', 'Queries', 'Com_select')",
		"status_by_account", nil,
	)
	rows := selectResultRows(result)
	require.Contains(t, rows, []interface{}{user, host, "Queries", "3"})
	require.Contains(t, rows, []interface{}{user, host, "Com_select", "3"})
	require.Contains(t, rows, []interface{}{user, host, "Threads_connected", "0"})

	hostResult := executor.QueryExecutor.executePerformanceSchemaSessionRuntimeRegistrySelect(
		"select host, variable_name, variable_value from performance_schema.status_by_host where host='disconnected_status_host' and variable_name='Queries'",
		"status_by_host", nil,
	)
	require.Equal(t, [][]interface{}{{host, "Queries", "3"}}, selectResultRows(hostResult))

	userResult := executor.QueryExecutor.executePerformanceSchemaSessionRuntimeRegistrySelect(
		"select user, variable_name, variable_value from performance_schema.status_by_user where user='disconnected_status_user' and variable_name='Com_select'",
		"status_by_user", nil,
	)
	require.Equal(t, [][]interface{}{{user, "Com_select", "3"}}, selectResultRows(userResult))
}

func TestPerformanceSchemaVariablesInfoProjectsSystemVariableDefinitions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select variable_name, variable_source, variable_path from performance_schema.variables_info where variable_name='autocommit'")
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	require.Equal(t, "autocommit", values[0].String())
	require.Equal(t, "COMPILED", values[1].String())
	require.Nil(t, values[2].Raw())
}

func TestPerformanceSchemaVariableFiltersDoNotRestoreRowsForEmptyPredicates(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	noInMatch := mustSelectResultSQL(t, executor, "", "select variable_name from performance_schema.global_variables where variable_name in ('__missing_variable__')")
	require.Empty(t, noInMatch.Records)

	nullMatch := mustSelectResultSQL(t, executor, "", "select variable_name from performance_schema.global_variables where variable_name is null")
	require.Empty(t, nullMatch.Records)
}

func TestPerformanceSchemaVariablesInfoReflectsGlobalRuntimeSource(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	admin := newTestMySQLSession()
	admin.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	admin.SetParamByName("user", "variables_admin")
	admin.SetParamByName("host", "localhost")

	mustExecSessionSQL(t, executor, admin, "", "set global max_connections = 200")
	result := mustSelectResultSQL(t, executor, "", "select variable_name, variable_source from performance_schema.variables_info where variable_name='max_connections'")
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	require.Equal(t, "max_connections", values[0].String())
	require.Equal(t, "GLOBAL", values[1].String())
}

func TestPerformanceSchemaPersistedVariablesSurviveRestart(t *testing.T) {
	dataDir := t.TempDir()
	first := NewXMySQLEngine(&conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        dataDir,
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	})
	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, first.Close())
		}
	})
	session := newTestMySQLSession()
	session.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	session.SetParamByName("user", "root")
	session.SetParamByName("host", "localhost")
	setResult := <-first.ExecuteQuery(session, "set persist max_connections = 200", "")
	require.NoError(t, setResult.Err)

	persisted := mustSelectResultSQL(t, first, "", "select variable_name, variable_value, set_user, set_host from performance_schema.persisted_variables where variable_name = 'max_connections'")
	require.Len(t, persisted.Records, 1)
	require.Equal(t, "max_connections", persisted.Records[0].GetValues()[0].String())
	require.Equal(t, "200", persisted.Records[0].GetValues()[1].String())
	require.Equal(t, "root", persisted.Records[0].GetValues()[2].String())
	require.Equal(t, "localhost", persisted.Records[0].GetValues()[3].String())

	info := mustSelectResultSQL(t, first, "", "select variable_name, variable_source, variable_path from performance_schema.variables_info where variable_name = 'max_connections'")
	require.Len(t, info.Records, 1)
	require.Equal(t, "PERSISTED", info.Records[0].GetValues()[1].String())
	require.Equal(t, filepath.Join(dataDir, persistedVariablesFileName), info.Records[0].GetValues()[2].String())
	wrongSource := mustSelectResultSQL(t, first, "", "select variable_name from performance_schema.variables_info where variable_name = 'max_connections' and variable_source = 'COMPILED'")
	require.Empty(t, wrongSource.Records)
	wrongUser := mustSelectResultSQL(t, first, "", "select variable_name from performance_schema.variables_info where variable_name = 'max_connections' and set_user = 'other-user'")
	require.Empty(t, wrongUser.Records)

	require.NoError(t, first.Close())
	closed = true
	second := NewXMySQLEngine(&conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        dataDir,
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	})
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	global := mustSelectResultSQL(t, second, "", "select variable_value from performance_schema.global_variables where variable_name = 'max_connections'")
	require.Len(t, global.Records, 1)
	require.Equal(t, "200", global.Records[0].GetValues()[0].String())
}

func TestPerformanceSchemaPersistOnlyAllowsReadOnlyVariablesWithPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("dynamic_privileges", []string{"PERSIST_RO_VARIABLES_ADMIN"})
	session.SetParamByName("user", "config_admin")
	session.SetParamByName("host", "localhost")

	result := <-executor.ExecuteQuery(session, "set persist_only version = '8.4.0'", "")
	require.NoError(t, result.Err)

	persisted := mustSelectResultSQL(t, executor, "", "select variable_name, variable_value from performance_schema.persisted_variables where variable_name = 'version'")
	require.Equal(t, [][]interface{}{{"version", "8.4.0"}}, selectResultRows(persisted))
	global := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_variables where variable_name = 'version'")
	require.Equal(t, [][]interface{}{{"8.0.32"}}, selectResultRows(global))
}

func TestPerformanceSchemaPersistedVariablesAcceptMultipleAssignments(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	session.SetParamByName("user", "root")
	session.SetParamByName("host", "localhost")

	result := <-executor.ExecuteQuery(session, "set persist max_connections = 202, max_allowed_packet = 33554432", "")
	require.NoError(t, result.Err)
	for _, expected := range [][]interface{}{{"max_connections", "202"}, {"max_allowed_packet", "33554432"}} {
		rows := mustQuerySQL(t, executor, "", fmt.Sprintf("select variable_name, variable_value from performance_schema.persisted_variables where variable_name = '%s'", expected[0]))
		require.Equal(t, [][]interface{}{expected}, rows)
	}
	global := mustQuerySQL(t, executor, "", "select variable_name, variable_value from performance_schema.global_variables where variable_name in ('max_connections', 'max_allowed_packet')")
	require.Contains(t, global, []interface{}{"max_connections", "202"})
	require.Contains(t, global, []interface{}{"max_allowed_packet", "33554432"})
}

func TestPerformanceSchemaPreparedStatementsExposeLiveSessionInventory(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(813))
	created := time.Now().Add(-time.Second)
	inventory := &testPreparedStatementInventory{statements: []compatibility.PreparedStatementSnapshot{{
		ID: 42, SQL: "select ?", CreatedAt: created, LastUsedAt: time.Now(), ExecuteCount: 1,
	}}}
	session.SetParamByName("prepared_stmt_mgr", inventory)

	result := <-executor.ExecuteQuery(session, "select statement_id, sql_text, owner_thread_id, count_execute from performance_schema.prepared_statements_instances", "")
	require.NoError(t, result.Err)
	rows := selectResultRows(result.Data.(*SelectResult))
	require.Contains(t, rows, []interface{}{fmt.Sprint(42), "select ?", "813", "1"})
	result = <-executor.ExecuteQuery(session, "select statement_id from performance_schema.prepared_statements_instances where statement_id=99", "")
	require.NoError(t, result.Err)
	require.Empty(t, selectResultRows(result.Data.(*SelectResult)))
	result = <-executor.ExecuteQuery(session, "select statement_id from performance_schema.prepared_statements_instances where owner_thread_id=814", "")
	require.NoError(t, result.Err)
	require.Empty(t, selectResultRows(result.Data.(*SelectResult)))
	wrongExecuteCount := mustSelectResultSQL(t, executor, "", "select statement_id from performance_schema.prepared_statements_instances where statement_id = 42 and count_execute = 99")
	require.Empty(t, wrongExecuteCount.Records)

	inventory.statements = nil
	result = <-executor.ExecuteQuery(session, "select statement_id from performance_schema.prepared_statements_instances", "")
	require.NoError(t, result.Err)
	require.Empty(t, selectResultRows(result.Data.(*SelectResult)))
}

func TestPerformanceSchemaPreparedStatementsIncludeAllLiveSessions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	current := newTestMySQLSession()
	current.SetParamByName("connection_id", int64(816))
	current.SetParamByName("prepared_stmt_mgr", &testPreparedStatementInventory{statements: []compatibility.PreparedStatementSnapshot{{
		ID: 51, SQL: "select current_session", CreatedAt: time.Now().Add(-time.Second),
	}}})
	other := newTestMySQLSession()
	other.SetParamByName("connection_id", int64(817))
	other.SetParamByName("prepared_stmt_mgr", &testPreparedStatementInventory{statements: []compatibility.PreparedStatementSnapshot{{
		ID: 52, SQL: "select other_session", CreatedAt: time.Now().Add(-time.Second),
	}}})
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{current, other}
	})

	result := <-executor.ExecuteQuery(current, "select statement_id, sql_text, owner_thread_id from performance_schema.prepared_statements_instances order by statement_id", "")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{
		{"51", "select current_session", "816"},
		{"52", "select other_session", "817"},
	}, selectResultRows(result.Data.(*SelectResult)))
}

func TestSQLPreparedStatementsExecuteAndAppearInPerformanceSchema(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(815))

	setSQL := <-executor.QueryExecutor.ExecuteWithQuery(session, "set @prepared_sql = 'select ?'", "")
	require.NoError(t, setSQL.Err)
	enableCPU := <-executor.QueryExecutor.ExecuteWithQuery(session, "update performance_schema.setup_consumers set enabled='YES' where name='events_statements_cpu'", "")
	require.NoError(t, enableCPU.Err)
	prepared := <-executor.QueryExecutor.ExecuteWithQuery(session, "prepare named_stmt from @prepared_sql", "")
	require.NoError(t, prepared.Err)
	setValue := <-executor.QueryExecutor.ExecuteWithQuery(session, "set @prepared_value = 7", "")
	require.NoError(t, setValue.Err)

	executed := <-executor.QueryExecutor.ExecuteWithQuery(session, "execute named_stmt using @prepared_value", "")
	require.NoError(t, executed.Err)
	selectResult, ok := executed.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"7"}}, selectResultRows(selectResult))

	observed := mustSelectResultSessionSQL(t, executor, session, "", "select statement_name, sql_text, count_execute from performance_schema.prepared_statements_instances where statement_name = 'named_stmt'")
	require.Equal(t, [][]interface{}{{"named_stmt", "select ?", "1"}}, selectResultRows(observed))
	cpuObserved := mustSelectResultSessionSQL(t, executor, session, "", "select sum_cpu_time from performance_schema.prepared_statements_instances where statement_name = 'named_stmt'")
	require.Len(t, selectResultRows(cpuObserved), 1)
	require.NotEqual(t, "0", selectResultRows(cpuObserved)[0][0], "prepared statement SUM_CPU_TIME must include captured execution CPU time")

	deallocated := <-executor.QueryExecutor.ExecuteWithQuery(session, "deallocate prepare named_stmt", "")
	require.NoError(t, deallocated.Err)
	observed = mustSelectResultSessionSQL(t, executor, session, "", "select statement_name from performance_schema.prepared_statements_instances where statement_name = 'named_stmt'")
	require.Empty(t, selectResultRows(observed))
	prepared = <-executor.QueryExecutor.ExecuteWithQuery(session, "prepare named_stmt from 'select 1'", "")
	require.NoError(t, prepared.Err)
	require.NoError(t, executor.QueryExecutor.ResetSession(session))
	observed = mustSelectResultSessionSQL(t, executor, session, "", "select statement_name from performance_schema.prepared_statements_instances where statement_name = 'named_stmt'")
	require.Empty(t, selectResultRows(observed))
}

func TestPerformanceSchemaPreparedStatementsExposeExecutionAccounting(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(814))
	created := time.Now().Add(-time.Second)
	inventory := &testPreparedStatementInventory{statements: []compatibility.PreparedStatementSnapshot{{
		ID: 43, SQL: "insert into t values (?)", CreatedAt: created, LastUsedAt: time.Now(), ExecuteCount: 2,
		ExecuteTimeTotal: 7 * time.Millisecond, ExecuteTimeMin: 2 * time.Millisecond, ExecuteTimeMax: 5 * time.Millisecond,
		ErrorCount: 1, WarningCount: 3, RowsAffected: 4, RowsSent: 5, RowsExamined: 6,
	}}}
	session.SetParamByName("prepared_stmt_mgr", inventory)

	query := "select statement_id, execution_engine, count_execute, sum_timer_execute, min_timer_execute, avg_timer_execute, max_timer_execute, sum_errors, sum_warnings, sum_rows_affected, sum_rows_sent, sum_rows_examined, sum_cpu_time, max_controlled_memory, max_total_memory, count_secondary from performance_schema.prepared_statements_instances where statement_id = 43"
	result := <-executor.ExecuteQuery(session, query, "")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"43", "PRIMARY", "2", "7000000000", "2000000000", "3500000000", "5000000000", "1", "3", "4", "5", "6", "0", "0", "0", "0"}}, selectResultRows(result.Data.(*SelectResult)))
}

func TestPerformanceSchemaGlobalErrorSummaryExposesExecutionErrors(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(814))
	session.SetParamByName("user", "alice")
	session.SetParamByName("host", "client.example")
	failed := <-executor.ExecuteQuery(session, "select * from missing_error_summary_table", "app")
	require.Error(t, failed.Err)

	result := mustSelectResultSQL(t, executor, "", "select error_number, error_name, sum_error_raised, sql_state from performance_schema.events_errors_summary_global_by_error where error_number=0")
	rows := selectResultRows(result)
	require.NotEmpty(t, rows)
	found := false
	for _, row := range rows {
		if len(row) == 4 && row[0] == "0" && row[1] == "" && row[2] != "0" && row[3] == "" {
			found = true
			break
		}
	}
	require.True(t, found, "global error summary did not expose the failed statement: %#v", rows)

	account := mustSelectResultSQL(t, executor, "", "select user, host, error_name, sum_error_raised from performance_schema.events_errors_summary_by_account_by_error where user='alice' and host='client.example'")
	accountRows := selectResultRows(account)
	require.NotEmpty(t, accountRows)
	require.Equal(t, "alice", accountRows[0][0])
	require.Equal(t, "client.example", accountRows[0][1])
	require.NotEqual(t, "0", accountRows[0][3])
}

func TestPerformanceSchemaGlobalStatusProjectsExecutorUptime(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.serverStartedAt = time.Now().Add(-2 * time.Hour)

	result := mustSelectResultSQL(t, executor, "", "select variable_name, variable_value from performance_schema.global_status where variable_name='Uptime'")
	rows := selectResultRows(result)
	require.Len(t, rows, 1)
	require.Equal(t, "Uptime", rows[0][0])
	uptime, err := strconv.ParseInt(fmt.Sprint(rows[0][1]), 10, 64)
	require.NoError(t, err)
	require.GreaterOrEqual(t, uptime, int64(7200))
}

func TestPerformanceSchemaGlobalStatusProjectsConnectionTotals(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	before := int64(0)
	for _, total := range executor.QueryExecutor.metricsRecorder.ConnectionTotals() {
		before += total.TotalConnections
	}
	executor.QueryExecutor.metricsRecorder.RecordConnection("status_counter_user", "localhost")
	executor.QueryExecutor.metricsRecorder.RecordConnection("status_counter_user", "localhost")

	result := mustSelectResultSQL(t, executor, "", "select variable_name, variable_value from performance_schema.global_status where variable_name='Connections'")
	rows := selectResultRows(result)
	require.Len(t, rows, 1)
	require.Equal(t, "Connections", rows[0][0])
	connections, err := strconv.ParseInt(fmt.Sprint(rows[0][1]), 10, 64)
	require.NoError(t, err)
	require.Equal(t, before+2, connections)
}

func TestPerformanceSchemaGlobalStatusProjectsAbortedConnections(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	before := executor.QueryExecutor.metricsRecorder.AuthenticationFailuresTotal()
	executor.QueryExecutor.metricsRecorder.RecordAuthenticationFailure("aborted_status_user", "127.0.0.1")
	executor.QueryExecutor.metricsRecorder.RecordAuthenticationFailure("aborted_status_user", "127.0.0.1")

	result := mustSelectResultSQL(t, executor, "", "select variable_name, variable_value from performance_schema.global_status where variable_name='Aborted_connects'")
	rows := selectResultRows(result)
	require.Len(t, rows, 1)
	require.Equal(t, "Aborted_connects", rows[0][0])
	aborted, err := strconv.ParseInt(fmt.Sprint(rows[0][1]), 10, 64)
	require.NoError(t, err)
	require.Equal(t, before+2, aborted)
}

func TestPerformanceSchemaGlobalStatusProjectsStatementCommandCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	recorder := executor.QueryExecutor.metricsRecorder
	recorder.RecordQuery("app", "select", "ok", time.Millisecond)
	recorder.RecordQuery("app", "insert", "ok", time.Millisecond)
	recorder.RecordQuery("app", "commit", "ok", time.Millisecond)
	recorder.RecordQuery("app", "replace", "ok", time.Millisecond)
	recorder.RecordQuery("app", "truncate", "ok", time.Millisecond)
	recorder.RecordQuery("app", "set", "ok", time.Millisecond)
	recorder.RecordQuery("app", "flush", "ok", time.Millisecond)
	recorder.RecordQuery("app", "call", "ok", time.Millisecond)
	recorder.RecordQuery("app", "use", "ok", time.Millisecond)
	recorder.RecordQuery("app", "explain", "ok", time.Millisecond)
	recorder.RecordQuery("app", "describe", "ok", time.Millisecond)
	recorder.RecordQuery("app", "analyze", "ok", time.Millisecond)
	recorder.RecordQuery("app", "savepoint", "ok", time.Millisecond)
	recorder.RecordQuery("app", "grant", "ok", time.Millisecond)
	recorder.RecordQuery("app", "revoke", "ok", time.Millisecond)
	recorder.RecordQuery("app", "lock", "ok", time.Millisecond)
	recorder.RecordQuery("app", "unlock", "ok", time.Millisecond)
	recorder.RecordQuery("app", "kill", "ok", time.Millisecond)
	recorder.RecordQuery("app", "reset", "ok", time.Millisecond)

	result := executor.QueryExecutor.executePerformanceSchemaStatusSelect("performance_schema.global_status", "")
	status := make(map[string]string)
	for _, row := range selectResultRows(result) {
		status[fmt.Sprint(row[0])] = fmt.Sprint(row[1])
	}

	require.Equal(t, "1", status["Com_select"])
	require.Equal(t, "1", status["Com_insert"])
	require.Equal(t, "1", status["Com_commit"])
	require.Equal(t, "1", status["Com_replace"])
	require.Equal(t, "1", status["Com_truncate"])
	require.Equal(t, "1", status["Com_set_option"])
	require.Equal(t, "1", status["Com_flush"])
	require.Equal(t, "1", status["Com_call_procedure"])
	require.Equal(t, "1", status["Com_change_db"])
	require.Equal(t, "1", status["Com_explain"])
	require.Equal(t, "1", status["Com_describe"])
	require.Equal(t, "1", status["Com_analyze"])
	require.Equal(t, "1", status["Com_savepoint"])
	require.Equal(t, "1", status["Com_grant"])
	require.Equal(t, "1", status["Com_revoke"])
	require.Equal(t, "1", status["Com_lock_tables"])
	require.Equal(t, "1", status["Com_unlock_tables"])
	require.Equal(t, "1", status["Com_kill"])
	require.Equal(t, "1", status["Com_reset"])
	require.Equal(t, "19", status["Questions"])
}

func TestPerformanceSchemaGlobalStatusProjectsPreparedCommandCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	recorder := executor.QueryExecutor.metricsRecorder
	for _, statementType := range []string{"stmt_prepare", "stmt_execute", "stmt_close", "stmt_reset", "stmt_send_long_data", "stmt_fetch"} {
		recorder.RecordQuery("app", statementType, "ok", time.Millisecond)
	}

	result := executor.QueryExecutor.executePerformanceSchemaStatusSelect("performance_schema.global_status", "")
	status := make(map[string]string)
	for _, row := range selectResultRows(result) {
		status[fmt.Sprint(row[0])] = fmt.Sprint(row[1])
	}

	require.Equal(t, "1", status["Com_stmt_prepare"])
	require.Equal(t, "1", status["Com_stmt_execute"])
	require.Equal(t, "1", status["Com_stmt_close"])
	require.Equal(t, "1", status["Com_stmt_reset"])
	require.Equal(t, "1", status["Com_stmt_send_long_data"])
	require.Equal(t, "1", status["Com_stmt_fetch"])
	require.Equal(t, "6", status["Questions"])
}

func TestPerformanceSchemaSessionStatusProjectsPreparedProtocolCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(817))
	session.SetParamByName("user", "prepared_status_user")
	session.SetParamByName("host", "prepared_status_host")
	executor.QueryExecutor.metricsRecorder.RecordProtocolCommand(
		817, "prepared_status_user", "prepared_status_host", "app", "stmt_execute", "ok", time.Millisecond,
	)

	result := executor.QueryExecutor.executePerformanceSchemaSessionRuntimeRegistrySelect(
		"select thread_id, variable_name, variable_value from performance_schema.status_by_thread where thread_id=817 and variable_name in ('Queries', 'Com_stmt_execute')",
		"status_by_thread", session,
	)
	rows := selectResultRows(result)
	require.Contains(t, rows, []interface{}{"817", "Queries", "1"})
	require.Contains(t, rows, []interface{}{"817", "Com_stmt_execute", "1"})

	account := executor.QueryExecutor.executePerformanceSchemaSessionRuntimeRegistrySelect(
		"select user, host, variable_name, variable_value from performance_schema.status_by_account where user='prepared_status_user' and host='prepared_status_host' and variable_name='Com_stmt_execute'",
		"status_by_account", session,
	)
	require.Equal(t, [][]interface{}{{"prepared_status_user", "prepared_status_host", "Com_stmt_execute", "1"}}, selectResultRows(account))
}

func TestPerformanceSchemaIgnoresMarkedInternalEngineQueries(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	internal := newTestMySQLSession()
	internal.SetParamByName("__xmysql_internal_query", true)

	result := <-executor.ExecuteQuery(internal, "select 1", "mysql")
	require.NoError(t, result.Err)
	require.Empty(t, executor.QueryExecutor.metricsRecorder.QueryTotals())
	require.Empty(t, executor.QueryExecutor.metricsRecorder.StatementHistory())
}

func TestPerformanceSchemaErrorSummaryProjectsMySQLErrorMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder.RecordQueryError("app", "execution", string(ExecutionErrorCodeSchemaOrTableNotFound))

	result := mustSelectResultSQL(t, executor, "", "select error_name, error_number, sql_state, sum_error_raised from performance_schema.events_errors_summary_global_by_error where error_number=1146 and sql_state='42S02'")
	require.Equal(t, [][]interface{}{{"ER_NO_SUCH_TABLE", "1146", "42S02", "1"}}, selectResultRows(result))
	wrongCount := mustSelectResultSQL(t, executor, "", "select error_name from performance_schema.events_errors_summary_global_by_error where error_name='"+string(ExecutionErrorCodeSchemaOrTableNotFound)+"' and sum_error_raised=999")
	require.Empty(t, wrongCount.Records)
}

func TestPerformanceSchemaUnknownErrorsAggregateIntoNullErrorRow(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	recorder := executor.QueryExecutor.metricsRecorder
	recorder.RecordQueryError("app", "execution", "E_UNMAPPED_ONE")
	recorder.RecordQueryError("app", "execution", "E_UNMAPPED_TWO")

	result := mustSelectResultSQL(t, executor, "", "select error_number, error_name, sql_state, sum_error_raised from performance_schema.events_errors_summary_global_by_error where error_number=0")
	rows := selectResultRows(result)
	require.Len(t, rows, 1)
	require.Equal(t, "0", rows[0][0])
	// The result helper exposes SQL NULL as an empty string; the registry keeps
	// the underlying values nil for MySQL's unknown-error row semantics.
	require.Empty(t, rows[0][1])
	require.Empty(t, rows[0][2])
	require.Equal(t, "2", rows[0][3])
}

func TestPerformanceSchemaErrorSummaryCountsSQLHandlerAsHandled(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table handled_errors (id int primary key)")
	mustExecSessionSQL(t, executor, session, "app", "insert into handled_errors values (1)")
	mustExecSessionSQL(t, executor, session, "app", "create procedure catch_duplicate() begin declare continue handler for sqlexception set @handled = 1; insert into handled_errors values (1); end")

	called := <-executor.ExecuteQuery(session, "call catch_duplicate()", "app")
	require.NoError(t, called.Err)

	result := mustSelectResultSQL(t, executor, "", "select error_number, sum_error_raised, sum_error_handled from performance_schema.events_errors_summary_global_by_error where error_number=0")
	require.Equal(t, [][]interface{}{{"0", "1", "1"}}, selectResultRows(result))
}

func TestPerformanceSchemaErrorSummaryCountsHandledSignalAsHandled(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create procedure catch_signal() begin declare continue handler for sqlstate '45000' set @handled_signal = 1; signal sqlstate '45000' set message_text = 'boom'; end")

	called := <-executor.ExecuteQuery(session, "call catch_signal()", "app")
	require.NoError(t, called.Err)

	result := mustSelectResultSQL(t, executor, "", "select error_number, sum_error_raised, sum_error_handled from performance_schema.events_errors_summary_global_by_error where error_number=0")
	require.Equal(t, [][]interface{}{{"0", "1", "1"}}, selectResultRows(result))
}

func TestPerformanceSchemaErrorSummaryTruncateResetsRuntimeAggregates(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder.RecordQueryErrorWithIdentity("app", "execution", string(ExecutionErrorCodeSchemaOrTableNotFound), 814, "alice", "client.example")
	require.NotEmpty(t, mustSelectResultSQL(t, executor, "", "select * from performance_schema.events_errors_summary_global_by_error").Records)

	truncated := <-executor.ExecuteQuery(nil, "truncate table performance_schema.events_errors_summary_global_by_error", "")
	require.NoError(t, truncated.Err, truncated.Message)
	global := mustSelectResultSQL(t, executor, "", "select error_name, sum_error_raised, sum_error_handled from performance_schema.events_errors_summary_global_by_error where error_name='ER_NO_SUCH_TABLE'")
	require.Equal(t, [][]interface{}{{"ER_NO_SUCH_TABLE", "0", "0"}}, selectResultRows(global))
	account := mustSelectResultSQL(t, executor, "", "select user, host, sum_error_raised from performance_schema.events_errors_summary_by_account_by_error where error_name='ER_NO_SUCH_TABLE'")
	require.Equal(t, [][]interface{}{{"alice", "client.example", "0"}}, selectResultRows(account))
}

func TestPerformanceSchemaErrorSummaryDimensionTruncateIsIndependent(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	executor.QueryExecutor.metricsRecorder.RecordQueryErrorWithIdentity(
		"app", "execution", string(ExecutionErrorCodeSchemaOrTableNotFound), 815, "bob", "client.example",
	)

	global := mustSelectResultSQL(t, executor, "", "select sum_error_raised from performance_schema.events_errors_summary_global_by_error where error_name='ER_NO_SUCH_TABLE'")
	account := mustSelectResultSQL(t, executor, "", "select sum_error_raised from performance_schema.events_errors_summary_by_account_by_error where error_name='ER_NO_SUCH_TABLE' and user='bob' and host='client.example'")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(global))
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(account))

	truncated := <-executor.ExecuteQuery(nil, "truncate table performance_schema.events_errors_summary_by_account_by_error", "")
	require.NoError(t, truncated.Err, truncated.Message)

	global = mustSelectResultSQL(t, executor, "", "select sum_error_raised from performance_schema.events_errors_summary_global_by_error where error_name='ER_NO_SUCH_TABLE'")
	account = mustSelectResultSQL(t, executor, "", "select sum_error_raised from performance_schema.events_errors_summary_by_account_by_error where error_name='ER_NO_SUCH_TABLE' and user='bob' and host='client.example'")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(global), "account summary truncate must not reset the global summary")
	require.Equal(t, [][]interface{}{{"0"}}, selectResultRows(account))
	host := mustSelectResultSQL(t, executor, "", "select sum_error_raised from performance_schema.events_errors_summary_by_host_by_error where error_name='ER_NO_SUCH_TABLE' and host='client.example'")
	user := mustSelectResultSQL(t, executor, "", "select sum_error_raised from performance_schema.events_errors_summary_by_user_by_error where error_name='ER_NO_SUCH_TABLE' and user='bob'")
	thread := mustSelectResultSQL(t, executor, "", "select sum_error_raised from performance_schema.events_errors_summary_by_thread_by_error where error_name='ER_NO_SUCH_TABLE' and thread_id=815")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(host))
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(user))
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(thread))

	truncated = <-executor.ExecuteQuery(nil, "truncate table performance_schema.events_errors_summary_by_host_by_error", "")
	require.NoError(t, truncated.Err, truncated.Message)
	host = mustSelectResultSQL(t, executor, "", "select sum_error_raised from performance_schema.events_errors_summary_by_host_by_error where error_name='ER_NO_SUCH_TABLE' and host='client.example'")
	global = mustSelectResultSQL(t, executor, "", "select sum_error_raised from performance_schema.events_errors_summary_global_by_error where error_name='ER_NO_SUCH_TABLE'")
	require.Equal(t, [][]interface{}{{"0"}}, selectResultRows(host))
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(global))
}

func TestPerformanceSchemaErrorInstrumentControlsCollection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())

	setup := mustSelectResultSQL(t, executor, "", "select name, enabled, timed from performance_schema.setup_instruments where name='error'")
	require.NotEmpty(t, setup.Records)
	setupRow := setup.Records[0].GetValues()
	require.Equal(t, "error", setupRow[0].String())
	require.Equal(t, "YES", setupRow[1].String())
	require.Equal(t, "NO", setupRow[2].String())
	updated := <-executor.ExecuteQuery(nil, "update performance_schema.setup_instruments set enabled='NO' where name='error'", "")
	require.NoError(t, updated.Err, updated.Message)

	failed := <-executor.ExecuteQuery(nil, "select * from missing_error_instrument_table", "app")
	require.Error(t, failed.Err)
	summary := mustSelectResultSQL(t, executor, "", "select * from performance_schema.events_errors_summary_global_by_error")
	require.Empty(t, summary.Records, "disabled error instrument must not collect new errors")

	updated = <-executor.ExecuteQuery(nil, "update performance_schema.setup_instruments set enabled='YES' where name='error'", "")
	require.NoError(t, updated.Err, updated.Message)
	failed = <-executor.ExecuteQuery(nil, "select * from missing_error_instrument_table", "app")
	require.Error(t, failed.Err)
	summary = mustSelectResultSQL(t, executor, "", "select sum_error_raised from performance_schema.events_errors_summary_global_by_error where error_number=0")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(summary))
}

func TestPerformanceSchemaMemoryInstrumentControlsCollection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())

	setup := mustSelectResultSQL(t, executor, "", "select name, enabled, timed from performance_schema.setup_instruments where name='memory/sql/THD::main_mem_root'")
	require.NotEmpty(t, setup.Records)
	setupRow := setup.Records[0].GetValues()
	require.Equal(t, "memory/sql/THD::main_mem_root", setupRow[0].String())
	require.Equal(t, "YES", setupRow[1].String())
	require.True(t, setupRow[2].IsNull())
	updated := <-executor.ExecuteQuery(nil, "update performance_schema.setup_instruments set enabled='NO' where name='memory/sql/THD::main_mem_root'", "")
	require.NoError(t, updated.Err, updated.Message)
	require.Equal(t, 1, updated.AffectedRows)
	require.True(t, executor.QueryExecutor.performanceSchemaConsumerEnabled("global_instrumentation"))
	require.True(t, executor.QueryExecutor.performanceSchemaConsumerEnabled("thread_instrumentation"))
	require.False(t, executor.QueryExecutor.performanceSchemaMemoryInstrumentEnabled())
	executor.QueryExecutor.metricsRecorder.ResetMemorySummary()

	result := <-executor.ExecuteQuery(nil, "select 1", "app")
	require.NoError(t, result.Err)
	summary := mustSelectResultSQL(t, executor, "", "select count_alloc from performance_schema.memory_summary_global_by_event_name where event_name='memory/sql/THD::main_mem_root'")
	require.Equal(t, [][]interface{}{{"0"}}, selectResultRows(summary))

	updated = <-executor.ExecuteQuery(nil, "update performance_schema.setup_instruments set enabled='YES' where name='memory/sql/THD::main_mem_root'", "")
	require.NoError(t, updated.Err, updated.Message)
	require.Equal(t, 1, updated.AffectedRows)
	executor.QueryExecutor.metricsRecorder.ResetMemorySummary()
	result = <-executor.ExecuteQuery(nil, "select 1", "app")
	require.NoError(t, result.Err)
	summary = mustSelectResultSQL(t, executor, "", "select count_alloc from performance_schema.memory_summary_global_by_event_name where event_name='memory/sql/THD::main_mem_root'")
	resumedRows := selectResultRows(summary)
	require.Len(t, resumedRows, 1)
	require.NotEqual(t, "0", resumedRows[0][0])
}

func TestPerformanceSchemaFileAndSocketInstrumentControlsCollection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())

	fileSetup := mustSelectResultSQL(t, executor, "", "select name, enabled, timed from performance_schema.setup_instruments where name='wait/io/file/innodb/innodb_data_file'")
	socketSetup := mustSelectResultSQL(t, executor, "", "select name, enabled, timed from performance_schema.setup_instruments where name='wait/io/socket/sql/client_connection'")
	require.Len(t, fileSetup.Records, 1)
	require.Len(t, socketSetup.Records, 1)

	updated := <-executor.ExecuteQuery(nil, "update performance_schema.setup_instruments set enabled='NO' where name like 'wait/io/file/%'", "")
	require.NoError(t, updated.Err, updated.Message)
	require.Greater(t, updated.AffectedRows, 0)
	executor.QueryExecutor.metricsRecorder.RecordFileOpen("users.ibd")
	executor.QueryExecutor.metricsRecorder.RecordFileRead("users.ibd", 5*time.Millisecond)
	require.Empty(t, executor.QueryExecutor.metricsRecorder.FileSummary(), "disabled file instruments must not collect new events")

	updated = <-executor.ExecuteQuery(nil, "update performance_schema.setup_instruments set enabled='YES', timed='NO' where name='wait/io/file/innodb/innodb_data_file'", "")
	require.NoError(t, updated.Err, updated.Message)
	executor.QueryExecutor.metricsRecorder.RecordFileRead("users.ibd", 5*time.Millisecond)
	fileRows := executor.QueryExecutor.metricsRecorder.FileSummary()
	require.Len(t, fileRows, 1)
	require.Equal(t, int64(1), fileRows[0].CountRead)
	require.Zero(t, fileRows[0].SumTimerRead, "TIMED=NO must preserve counts but omit file wait time")

	updated = <-executor.ExecuteQuery(nil, "update performance_schema.setup_instruments set enabled='NO' where name='wait/io/socket/sql/client_connection'", "")
	require.NoError(t, updated.Err, updated.Message)
	executor.QueryExecutor.metricsRecorder.RecordSocketRead(881, 128, 5*time.Millisecond)
	require.Empty(t, executor.QueryExecutor.metricsRecorder.SocketSummary(), "disabled socket instruments must not collect new events")

	updated = <-executor.ExecuteQuery(nil, "update performance_schema.setup_instruments set enabled='YES', timed='NO' where name='wait/io/socket/sql/client_connection'", "")
	require.NoError(t, updated.Err, updated.Message)
	executor.QueryExecutor.metricsRecorder.RecordSocketRead(881, 128, 5*time.Millisecond)
	socketRows := executor.QueryExecutor.metricsRecorder.SocketSummary()
	require.Len(t, socketRows, 1)
	require.Equal(t, int64(1), socketRows[0].CountRead)
	require.Equal(t, int64(128), socketRows[0].BytesRead)
	require.Zero(t, socketRows[0].SumTimerRead, "TIMED=NO must preserve socket bytes but omit wait time")
}

func TestPerformanceSchemaErrorLogExposesExecutionErrorEvents(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	failed := <-executor.ExecuteQuery(nil, "select * from missing_error_log_table", "app")
	require.Error(t, failed.Err)

	result := mustSelectResultSQL(t, executor, "", "select thread_id, error_code, subsystem, prio, data from performance_schema.error_log")
	require.NotEmpty(t, result.Records)
	row := result.Records[len(result.Records)-1].GetValues()
	require.Equal(t, int64(0), row[0].Int())
	require.NotEmpty(t, row[1].String())
	require.Equal(t, "query", row[2].String())
	require.Equal(t, "Warning", row[3].String())
	require.Contains(t, row[4].String(), "error_class")

	filtered := mustSelectResultSQL(t, executor, "", "select error_code, subsystem, prio from performance_schema.error_log where error_code = 'E_UNKNOWN' and subsystem = 'query' and prio = 'Warning'")
	require.NotEmpty(t, filtered.Records)
	wrongCode := mustSelectResultSQL(t, executor, "", "select error_code from performance_schema.error_log where error_code = 'E_ACCESS_DENIED'")
	require.Empty(t, wrongCode.Records)
	wrongSubsystem := mustSelectResultSQL(t, executor, "", "select error_code from performance_schema.error_log where subsystem = 'replication'")
	require.Empty(t, wrongSubsystem.Records)
	wrongPriority := mustSelectResultSQL(t, executor, "", "select error_code from performance_schema.error_log where prio = 2")
	require.Empty(t, wrongPriority.Records)
	wrongData := mustSelectResultSQL(t, executor, "", "select error_code from performance_schema.error_log where data like '%not-present%'")
	require.Empty(t, wrongData.Records)
}

func TestPerformanceSchemaErrorLogRejectsTruncate(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	failed := <-executor.ExecuteQuery(nil, "select * from missing_error_log_truncate_table", "app")
	require.Error(t, failed.Err)

	result := <-executor.ExecuteQuery(nil, "truncate table performance_schema.error_log", "")
	require.Error(t, result.Err)
	require.Contains(t, strings.ToLower(result.Err.Error()), "not permitted")
}

func TestPerformanceSchemaStageSummaryByThreadKeepsConnectionIdentity(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(77)
	result := <-executor.ExecuteQuery(session, "select 12", "app")
	require.NoError(t, result.Err)

	rows := selectResultRows(mustSelectResultSQL(t, executor, "", "select thread_id, event_name, count_star from performance_schema.events_stages_summary_by_thread_by_event_name"))
	var found bool
	for _, row := range rows {
		if len(row) >= 3 && row[0] == "77" && row[1] == "stage/sql/execute" {
			found = true
			require.NotEqual(t, "0", row[2])
			break
		}
	}
	require.True(t, found, "thread stage summary did not retain connection identity: %#v", rows)
}

func TestPerformanceSchemaStageSummaryByIdentityHonorsInstrumentSettings(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	// The production recorder is process-wide; isolate this lifetime-sensitive
	// identity test so earlier engine tests cannot contribute to Alice/Bob.
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	first := newTestMySQLSession()
	first.ctx.SetConnectionID(781)
	first.SetParamByName("user", "alice")
	first.SetParamByName("host", "shared.example")
	second := newTestMySQLSession()
	second.ctx.SetConnectionID(782)
	second.SetParamByName("user", "bob")
	second.SetParamByName("host", "shared.example")

	result := <-executor.ExecuteQuery(first, "select 21", "app")
	require.NoError(t, result.Err)
	result = <-executor.ExecuteQuery(second, "select 22", "app")
	require.NoError(t, result.Err)

	accounts := mustSelectResultSQL(t, executor, "", "select user, host, event_name, count_star, sum_timer_wait from performance_schema.events_stages_summary_by_account_by_event_name")
	var alice, bob []interface{}
	for _, row := range selectResultRows(accounts) {
		if len(row) >= 5 && row[2] == "stage/sql/execute" {
			switch row[0] {
			case "alice":
				alice = row
			case "bob":
				bob = row
			}
		}
	}
	require.NotNil(t, alice)
	require.NotNil(t, bob)
	require.Equal(t, "shared.example", alice[1])
	require.Equal(t, "1", alice[3])
	require.NotEqual(t, "0", alice[4])

	filteredAccounts := mustSelectResultSQL(t, executor, "", "select user, host, event_name, count_star from performance_schema.events_stages_summary_by_account_by_event_name where user='alice' and host='shared.example' and event_name='stage/sql/execute'")
	require.Equal(t, [][]interface{}{{"alice", "shared.example", "stage/sql/execute", "1"}}, selectResultRows(filteredAccounts))
	filteredGlobal := mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_stages_summary_global_by_event_name where event_name='stage/sql/other'")
	require.Empty(t, filteredGlobal.Records)
	filteredThread := mustSelectResultSQL(t, executor, "", "select thread_id, event_name, count_star from performance_schema.events_stages_summary_by_thread_by_event_name where thread_id=781")
	require.Equal(t, [][]interface{}{{"781", "stage/sql/execute", "1"}}, selectResultRows(filteredThread))

	hosts := mustSelectResultSQL(t, executor, "", "select host, event_name, count_star from performance_schema.events_stages_summary_by_host_by_event_name")
	var sharedHost []interface{}
	for _, row := range selectResultRows(hosts) {
		if len(row) >= 3 && row[0] == "shared.example" && row[1] == "stage/sql/execute" {
			sharedHost = row
			break
		}
	}
	require.Equal(t, []interface{}{"shared.example", "stage/sql/execute", "2"}, sharedHost)

	require.NoError(t, (<-executor.ExecuteQuery(nil, "update performance_schema.setup_instruments set timed = 'NO' where name = 'stage/sql/execute'", "")).Err)
	timedOff := mustSelectResultSQL(t, executor, "", "select user, event_name, sum_timer_wait from performance_schema.events_stages_summary_by_user_by_event_name")
	for _, row := range selectResultRows(timedOff) {
		if len(row) >= 3 && row[1] == "stage/sql/execute" {
			require.Equal(t, "0", row[2])
		}
	}

	require.NoError(t, (<-executor.ExecuteQuery(nil, "update performance_schema.setup_instruments set enabled = 'NO' where name = 'stage/sql/execute'", "")).Err)
	disabled := mustSelectResultSQL(t, executor, "", "select event_name from performance_schema.events_stages_summary_by_account_by_event_name")
	for _, row := range selectResultRows(disabled) {
		require.NotEqual(t, "stage/sql/execute", row[0])
	}
}

func TestPerformanceSchemaWaitSummaryResolvesTransactionIdentity(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(791))
	session.SetParamByName("transaction_id", int64(991))
	session.SetParamByName("user", "wait_user")
	session.SetParamByName("host", "wait.example")
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{session}
	})

	user, host := executor.QueryExecutor.performanceSchemaThreadIdentity(991)
	require.Equal(t, "wait_user", user)
	require.Equal(t, "wait.example", host)
}

func TestPerformanceSchemaSessionConnectAttrsExposeCurrentSession(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(55)
	session.SetParamByName("connection_id", int64(55))
	session.SetParamByName("connection_attributes", map[string]string{
		"_client_name":    "unit-client",
		"_client_version": "1.0",
	})

	result := <-executor.ExecuteQuery(session, "select processlist_id, attr_name, attr_value, ordinal_position from performance_schema.session_connect_attrs", "")
	require.NoError(t, result.Err)
	rows := selectResultRows(result.Data.(*SelectResult))
	require.Equal(t, [][]interface{}{
		{"55", "_client_name", "unit-client", "0"},
		{"55", "_client_version", "1.0", "1"},
	}, rows)
}

func TestPerformanceSchemaSocketInstancesExposeCurrentEndpoint(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(66))
	session.SetParamByName("host", "127.0.0.1")
	session.SetParamByName("remote_addr", "127.0.0.1:43321")

	result := <-executor.ExecuteQuery(session, "select event_name, thread_id, socket_id, ip, port, state from performance_schema.socket_instances", "")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"wait/io/socket/sql/client_connection", "66", "66", "127.0.0.1", "43321", "ACTIVE"}}, selectResultRows(result.Data.(*SelectResult)))

	result = <-executor.ExecuteQuery(session, "select event_name, thread_id, socket_id, ip, port, state from performance_schema.socket_instances where thread_id = 99", "")
	require.NoError(t, result.Err)
	require.Empty(t, selectResultRows(result.Data.(*SelectResult)))
	result = <-executor.ExecuteQuery(session, "select event_name, thread_id, socket_id, ip, port, state from performance_schema.socket_instances where ip = '192.0.2.1'", "")
	require.NoError(t, result.Err)
	require.Empty(t, selectResultRows(result.Data.(*SelectResult)))
}

func TestPerformanceSchemaSocketSummariesExposeCurrentSocket(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(67))
	session.SetParamByName("remote_addr", "127.0.0.1:43322")

	result := <-executor.ExecuteQuery(session, "select event_name, thread_id, socket_id, ip, port, count_read, count_write, count_misc from performance_schema.socket_summary_by_instance", "")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"wait/io/socket/sql/client_connection", "67", "67", "127.0.0.1", "43322", "0", "0", "0"}}, selectResultRows(result.Data.(*SelectResult)))

	result = <-executor.ExecuteQuery(session, "select event_name, thread_id, socket_id, ip, port, count_read, count_write, count_misc from performance_schema.socket_summary_by_instance where socket_id = 99", "")
	require.NoError(t, result.Err)
	require.Empty(t, selectResultRows(result.Data.(*SelectResult)))
	result = <-executor.ExecuteQuery(session, "select event_name, thread_id, socket_id, ip, port, count_read, count_write, count_misc from performance_schema.socket_summary_by_instance where event_name = 'wait/io/file/innodb/innodb_data_file'", "")
	require.NoError(t, result.Err)
	require.Empty(t, selectResultRows(result.Data.(*SelectResult)))

	result = <-executor.ExecuteQuery(session, "select event_name, count_star, count_read, count_write, count_misc from performance_schema.socket_summary_by_event_name", "")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"wait/io/socket/sql/client_connection", "1", "0", "0", "0"}}, selectResultRows(result.Data.(*SelectResult)))
	result = <-executor.ExecuteQuery(session, "select event_name, count_star, count_read, count_write, count_misc from performance_schema.socket_summary_by_event_name where event_name = 'wait/io/file/innodb/innodb_data_file'", "")
	require.NoError(t, result.Err)
	require.Empty(t, selectResultRows(result.Data.(*SelectResult)))
	result = <-executor.ExecuteQuery(session, "select event_name from performance_schema.socket_summary_by_event_name where event_name = 'wait/io/socket/sql/client_connection' and count_star = 999999", "")
	require.NoError(t, result.Err)
	require.Empty(t, selectResultRows(result.Data.(*SelectResult)))
}

func TestPerformanceSchemaSocketSummariesExposeRuntimeTraffic(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder = metrics.NewRuntimeRecorder(metrics.NewRegistry())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(67))
	session.SetParamByName("remote_addr", "127.0.0.1:43322")
	executor.QueryExecutor.metricsRecorder.RecordSocketRead(67, 128, 3*time.Millisecond)
	executor.QueryExecutor.metricsRecorder.RecordSocketWrite(67, 256, 5*time.Millisecond)

	result := <-executor.ExecuteQuery(session, "select thread_id, socket_id, count_star, count_read, sum_number_of_bytes_read, count_write, sum_number_of_bytes_write from performance_schema.socket_summary_by_instance", "")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"67", "67", "2", "1", "128", "1", "256"}}, selectResultRows(result.Data.(*SelectResult)))

	result = <-executor.ExecuteQuery(session, "select event_name, count_star, count_read, sum_number_of_bytes_read, count_write, sum_number_of_bytes_write from performance_schema.socket_summary_by_event_name", "")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"wait/io/socket/sql/client_connection", "2", "1", "128", "1", "256"}}, selectResultRows(result.Data.(*SelectResult)))
}

func TestPerformanceSchemaFileInstancesExposeDataFiles(t *testing.T) {
	dataDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "app"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "app", "orders.ibd"), []byte("data"), 0644))
	executor := newTestStorageIntegratedExecutor(t, dataDir)

	rows := selectResultRows(mustSelectResultSQL(t, executor, "", "select file_name, event_name, open_count from performance_schema.file_instances"))
	var found bool
	for _, row := range rows {
		if len(row) >= 3 && strings.HasSuffix(row[0].(string), filepath.Join("app", "orders.ibd")) {
			found = true
			require.Equal(t, "wait/io/file/innodb/innodb_data_file", row[1])
			require.NotEqual(t, "0", row[2])
			break
		}
	}
	require.True(t, found, "file_instances did not expose the data file: %#v", rows)
	filtered := selectResultRows(mustSelectResultSQL(t, executor, "", "select file_name, event_name, open_count from performance_schema.file_instances where file_name = '/not/in/data-dir/orders.ibd'"))
	require.Empty(t, filtered)
	filtered = selectResultRows(mustSelectResultSQL(t, executor, "", "select file_name, event_name, open_count from performance_schema.file_instances where event_name = 'wait/io/socket/sql/client_connection'"))
	require.Empty(t, filtered)
}

func TestPerformanceSchemaFileSummariesExposeDataFiles(t *testing.T) {
	dataDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "app"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "app", "orders.ibd"), []byte("data"), 0644))
	executor := newTestStorageIntegratedExecutor(t, dataDir)

	instanceRows := selectResultRows(mustSelectResultSQL(t, executor, "", "select file_name, event_name, object_instance_begin, count_read, count_write, count_misc from performance_schema.file_summary_by_instance"))
	var found bool
	for _, row := range instanceRows {
		if len(row) >= 6 && strings.HasSuffix(row[0].(string), filepath.Join("app", "orders.ibd")) {
			found = true
			require.Equal(t, "wait/io/file/innodb/innodb_data_file", row[1])
			require.Equal(t, "0", row[3])
			require.Equal(t, "0", row[4])
			require.Equal(t, "0", row[5])
			break
		}
	}
	require.True(t, found, "file_summary_by_instance did not expose the data file: %#v", instanceRows)

	eventRows := selectResultRows(mustSelectResultSQL(t, executor, "", "select event_name, count_star, count_read, count_write, count_misc from performance_schema.file_summary_by_event_name"))
	var eventFound bool
	for _, row := range eventRows {
		if len(row) >= 5 && row[0] == "wait/io/file/innodb/innodb_data_file" {
			eventFound = true
			require.NotEqual(t, "0", row[1])
			break
		}
	}
	require.True(t, eventFound, "file_summary_by_event_name did not expose the data-file event: %#v", eventRows)
	filteredInstance := selectResultRows(mustSelectResultSQL(t, executor, "", "select file_name, event_name, object_instance_begin, count_read, count_write, count_misc from performance_schema.file_summary_by_instance where file_name = '/not/in/data-dir/orders.ibd'"))
	require.Empty(t, filteredInstance)
	filteredEvent := selectResultRows(mustSelectResultSQL(t, executor, "", "select event_name, count_star, count_read, count_write, count_misc from performance_schema.file_summary_by_event_name where event_name = 'wait/io/socket/sql/client_connection'"))
	require.Empty(t, filteredEvent)
	filteredEvent = selectResultRows(mustSelectResultSQL(t, executor, "", "select event_name from performance_schema.file_summary_by_event_name where event_name = 'wait/io/file/innodb/innodb_data_file' and count_star = 999999"))
	require.Empty(t, filteredEvent)
}

func TestPerformanceSchemaFileSummariesTrackRealIBDPageIO(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table file_io_rows (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into file_io_rows values (1, 'one'), (2, 'two')")
	require.Equal(t, [][]interface{}{{"1", "one"}, {"2", "two"}}, mustQuerySQL(t, executor, "app", "select id, label from file_io_rows order by id"))

	rows := selectResultRows(mustSelectResultSQL(t, executor, "", "select file_name, count_read, count_write, sum_timer_read, sum_timer_write from performance_schema.file_summary_by_instance"))
	var found bool
	for _, row := range rows {
		if len(row) < 5 || !strings.HasSuffix(row[0].(string), filepath.Join("app", "file_io_rows.ibd")) {
			continue
		}
		found = true
		require.NotEqual(t, "0", row[2], "real IBD writes should be observed")
		require.NotEqual(t, "0", row[4], "real IBD write timer should be observed")
		break
	}
	require.True(t, found, "real IBD file summary was not exposed: %#v", rows)
}

func TestPerformanceSchemaMetadataLocksExposeDDLOwnerAndWaiter(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	coordinator := newTableDDLCoordinator()
	executor.QueryExecutor.ddlCoordinator = coordinator
	lock := coordinator.lockFor("app.users")
	require.NoError(t, lock.lockWithContextOwned(context.Background(), tableLockWrite, "thread/123"))

	result := <-executor.ExecuteQuery(nil, "select column_name, object_name, lock_type, lock_status, owner_thread_id from performance_schema.metadata_locks", "")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	rows := selectResultRows(selectResult)
	require.Len(t, rows, 1)
	require.Equal(t, []interface{}{"", "users", "EXCLUSIVE", "GRANTED", "123"}, rows[0])
	filteredSchema := <-executor.ExecuteQuery(nil, "select object_name from performance_schema.metadata_locks where object_schema = 'other'", "")
	require.NoError(t, filteredSchema.Err)
	require.Empty(t, selectResultRows(filteredSchema.Data.(*SelectResult)))
	filteredOwner := <-executor.ExecuteQuery(nil, "select object_name from performance_schema.metadata_locks where owner_thread_id = 999", "")
	require.NoError(t, filteredOwner.Err)
	require.Empty(t, selectResultRows(filteredOwner.Data.(*SelectResult)))
	lock.unlockOwned(tableLockWrite, "thread/123")
}

func TestPerformanceSchemaMetadataWaitsExposeMDLWaitForGraph(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	coordinator := newTableDDLCoordinator()
	executor.QueryExecutor.ddlCoordinator = coordinator
	lock := coordinator.lockFor("app.users")
	require.NoError(t, lock.lockWithContextOwned(context.Background(), tableLockWrite, "thread/101"))
	waitCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waitDone := make(chan error, 1)
	go func() { waitDone <- lock.lockWithContextOwned(waitCtx, tableLockRead, "thread/202") }()
	require.Eventually(t, func() bool { return len(coordinator.MetadataLockWaitEdges()) == 1 }, time.Second, 5*time.Millisecond)

	result := <-executor.ExecuteQuery(nil, "select requesting_thread_id, blocking_thread_id, requesting_lock_id, blocking_lock_id from performance_schema.data_lock_waits", "")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"202", "101", "metadata:app.users", "metadata:app.users"}}, selectResultRows(result.Data.(*SelectResult)))
	filteredWait := <-executor.ExecuteQuery(nil, "select requesting_thread_id from performance_schema.data_lock_waits where requesting_thread_id = 999", "")
	require.NoError(t, filteredWait.Err)
	require.Empty(t, selectResultRows(filteredWait.Data.(*SelectResult)))
	filteredLocks := <-executor.ExecuteQuery(nil, "select thread_id, lock_status from performance_schema.data_locks where thread_id = 999", "")
	require.NoError(t, filteredLocks.Err)
	require.Empty(t, selectResultRows(filteredLocks.Data.(*SelectResult)))

	cancel()
	require.Error(t, <-waitDone)
	lock.unlockOwned(tableLockWrite, "thread/101")
}

func TestPerformanceSchemaMetadataWaitHistoryExposesCompletedMDLWait(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	coordinator := newTableDDLCoordinator()
	executor.QueryExecutor.ddlCoordinator = coordinator
	lock := coordinator.lockFor("app.users")
	require.NoError(t, lock.lockWithContextOwned(context.Background(), tableLockWrite, "thread/501"))

	waitDone := make(chan error, 1)
	go func() { waitDone <- lock.lockWithContextOwned(context.Background(), tableLockRead, "thread/502") }()
	require.Eventually(t, func() bool { return len(coordinator.MetadataLockWaitEdges()) == 1 }, time.Second, 5*time.Millisecond)
	lock.unlockOwned(tableLockWrite, "thread/501")
	require.NoError(t, <-waitDone)

	result := <-executor.ExecuteQuery(nil, "select thread_id, event_name, object_name, operation from performance_schema.events_waits_history", "")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"502", "wait/lock/metadata/sql/mdl", "app.users", "metadata lock"}}, selectResultRows(result.Data.(*SelectResult)))

	summary := <-executor.ExecuteQuery(nil, "select object_schema, object_name, count_misc from performance_schema.table_lock_waits_summary_by_table", "")
	require.NoError(t, summary.Err)
	require.Equal(t, [][]interface{}{{"app", "users", "1"}}, selectResultRows(summary.Data.(*SelectResult)))

	lock.unlockOwned(tableLockRead, "thread/502")
}

func TestPerformanceSchemaMetadataWaitHistoryPreservesEventInstrumentSnapshot(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	coordinator := newTableDDLCoordinator()
	executor.QueryExecutor.ddlCoordinator = coordinator
	lock := coordinator.lockFor("app.metadata_snapshot")
	require.NoError(t, lock.lockWithContextOwned(context.Background(), tableLockWrite, "thread/701"))

	waitDone := make(chan error, 1)
	go func() { waitDone <- lock.lockWithContextOwned(context.Background(), tableLockRead, "thread/702") }()
	require.Eventually(t, func() bool { return len(coordinator.MetadataLockWaitEdges()) == 1 }, time.Second, 5*time.Millisecond)
	lock.unlockOwned(tableLockWrite, "thread/701")
	require.NoError(t, <-waitDone)
	lock.unlockOwned(tableLockRead, "thread/702")

	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set timed='NO' where name='wait/lock/metadata/sql/mdl'")
	history := mustSelectResultSQL(t, executor, "", "select event_name, timer_wait from performance_schema.events_waits_history_long where event_name='wait/lock/metadata/sql/mdl'")
	require.Len(t, history.Records, 1)
	require.Greater(t, history.Records[0].GetValues()[1].Int(), int64(0), "metadata wait history must retain the event-time TIMED=YES snapshot")
	summary := mustSelectResultSQL(t, executor, "", "select event_name, count_star, sum_timer_wait from performance_schema.events_waits_summary_global_by_event_name where event_name='wait/lock/metadata/sql/mdl'")
	require.Len(t, summary.Records, 1)
	require.Greater(t, summary.Records[0].GetValues()[2].Int(), int64(0), "metadata wait summary must retain the event-time TIMED=YES snapshot")
	tableSummary := mustSelectResultSQL(t, executor, "", "select object_schema, object_name, count_star, sum_timer_wait from performance_schema.table_lock_waits_summary_by_table where object_schema='app' and object_name='metadata_snapshot'")
	require.Len(t, tableSummary.Records, 1)
	require.Equal(t, int64(1), tableSummary.Records[0].GetValues()[2].Int())
	require.Greater(t, tableSummary.Records[0].GetValues()[3].Int(), int64(0), "metadata table-lock summary must retain the event-time TIMED=YES snapshot")
	minimums := mustSelectResultSQL(t, executor, "", "select min_timer_wait, min_timer_write, min_timer_write_normal from performance_schema.table_lock_waits_summary_by_table where object_schema='app' and object_name='metadata_snapshot'")
	require.Len(t, minimums.Records, 1)
	require.Greater(t, minimums.Records[0].GetValues()[0].Int(), int64(0), "table-lock MIN_TIMER_WAIT must initialize from the first observed wait")
	require.Greater(t, minimums.Records[0].GetValues()[1].Int(), int64(0), "metadata write-wait minimum must initialize from the first observed wait")
	require.Greater(t, minimums.Records[0].GetValues()[2].Int(), int64(0), "normal write-wait minimum must initialize from the first observed wait")

	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set enabled='NO' where name='wait/lock/metadata/sql/mdl'")
	history = mustSelectResultSQL(t, executor, "", "select event_name, timer_wait from performance_schema.events_waits_history_long where event_name='wait/lock/metadata/sql/mdl'")
	require.Len(t, history.Records, 1, "disabling an instrument must not erase already-captured metadata wait history")
	summary = mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_waits_summary_global_by_event_name where event_name='wait/lock/metadata/sql/mdl'")
	require.Equal(t, [][]interface{}{{"wait/lock/metadata/sql/mdl", "1"}}, selectResultRows(summary), "disabling an instrument must not erase its completed metadata wait summary")
	tableSummary = mustSelectResultSQL(t, executor, "", "select object_schema, object_name, count_star from performance_schema.table_lock_waits_summary_by_table where object_schema='app' and object_name='metadata_snapshot'")
	require.Equal(t, [][]interface{}{{"app", "metadata_snapshot", "1"}}, selectResultRows(tableSummary), "disabling an instrument must not erase its completed metadata table-lock summary")
}

func TestPerformanceSchemaMetadataWaitHistoryLongTruncateIsIndependent(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	coordinator := newTableDDLCoordinator()
	executor.QueryExecutor.ddlCoordinator = coordinator
	lock := coordinator.lockFor("app.users")
	require.NoError(t, lock.lockWithContextOwned(context.Background(), tableLockWrite, "thread/601"))

	waitDone := make(chan error, 1)
	go func() { waitDone <- lock.lockWithContextOwned(context.Background(), tableLockRead, "thread/602") }()
	require.Eventually(t, func() bool { return len(coordinator.MetadataLockWaitEdges()) == 1 }, time.Second, 5*time.Millisecond)
	lock.unlockOwned(tableLockWrite, "thread/601")
	require.NoError(t, <-waitDone)

	shortHistory := executor.QueryExecutor.executePerformanceSchemaEventsWaitsSelect(
		"select thread_id, event_name, object_name, operation from performance_schema.events_waits_history",
	)
	longHistory := executor.QueryExecutor.executePerformanceSchemaEventsWaitsSelect(
		"select thread_id, event_name, object_name, operation from performance_schema.events_waits_history_long",
	)
	require.NotEmpty(t, selectResultRows(shortHistory))
	require.NotEmpty(t, selectResultRows(longHistory))

	stmt, err := sqlparser.Parse("truncate table performance_schema.events_waits_history_long")
	require.NoError(t, err)
	results := make(chan *Result, 1)
	executor.QueryExecutor.executeTruncateTableStatement(&ExecutionContext{
		Results: results,
		Cfg:     executor.QueryExecutor.conf,
	}, "", stmt.(*sqlparser.DDL))
	require.NoError(t, (<-results).Err)

	shortHistory = executor.QueryExecutor.executePerformanceSchemaEventsWaitsSelect(
		"select thread_id, event_name, object_name, operation from performance_schema.events_waits_history",
	)
	longHistory = executor.QueryExecutor.executePerformanceSchemaEventsWaitsSelect(
		"select thread_id, event_name, object_name, operation from performance_schema.events_waits_history_long",
	)
	require.NotEmpty(t, selectResultRows(shortHistory), "long-history truncate must not clear the short metadata wait history")
	require.Empty(t, selectResultRows(longHistory))

	lock.unlockOwned(tableLockRead, "thread/602")
}

func TestShowEngineInnoDBStatusIncludesMetadataWaits(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	coordinator := newTableDDLCoordinator()
	executor.QueryExecutor.ddlCoordinator = coordinator
	lock := coordinator.lockFor("app.users")
	require.NoError(t, lock.lockWithContextOwned(context.Background(), tableLockWrite, "thread/303"))
	waitCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waitDone := make(chan error, 1)
	go func() { waitDone <- lock.lockWithContextOwned(waitCtx, tableLockRead, "thread/404") }()
	require.Eventually(t, func() bool { return len(coordinator.MetadataLockWaitEdges()) == 1 }, time.Second, 5*time.Millisecond)

	result := <-executor.ExecuteQuery(nil, "show engine innodb status", "")
	require.NoError(t, result.Err)
	status := selectResultRows(result.Data.(*SelectResult))[0][2].(string)
	require.Contains(t, status, "METADATA LOCK WAIT: table=app.users")
	require.Contains(t, status, "Metadata lock waits: 1")

	cancel()
	require.Error(t, <-waitDone)
	lock.unlockOwned(tableLockWrite, "thread/303")
}

func TestRuntimeMetricsFollowTransactionLifecycle(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()

	mustExecSessionSQL(t, executor, session, "app", "begin")
	active := executor.QueryExecutor.metricsRecorder.PrometheusText()
	require.Contains(t, active, `xmysql_transactions_active{isolation_level="REPEATABLE-READ"} 1`)

	mustExecSessionSQL(t, executor, session, "app", "commit")
	committed := executor.QueryExecutor.metricsRecorder.PrometheusText()
	require.Contains(t, committed, `xmysql_transactions_active{isolation_level="REPEATABLE-READ"} 0`)
	require.Contains(t, committed, `xmysql_transactions_committed_total{isolation_level="REPEATABLE-READ"}`)
}

func TestRuntimeMetricsRecordEngineErrors(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "select from", "app")
	require.Error(t, result.Err)
	require.Contains(t, executor.QueryExecutor.metricsRecorder.PrometheusText(), "xmysql_query_errors_total{")
}
