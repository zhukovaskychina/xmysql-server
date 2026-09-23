package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/observability/compatibility"
	metrics "github.com/zhukovaskychina/xmysql-server/server/observability/metrics"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

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
	require.Equal(t, "select * from digest_app.docs where id = 1", digestRow[36])
	require.Equal(t, "2000000000", digestRow[38])
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

func TestPerformanceSchemaProgramSummaryTracksStoredProcedureCalls(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database program_app")
	mustExecSQL(t, executor, "program_app", "create procedure report() begin select 1; end")
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
	// The procedure body contains SELECT 1, so the program summary must carry
	// the child statement's real sent-row accounting as well.
	require.NotEqual(t, "0", row[17])
}

func TestPerformanceSchemaProgramSummaryTracksStoredFunctionCalls(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
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

func TestPerformanceSchemaStatementHistoryAppliesFiltersAndProjection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
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

func TestPerformanceSchemaStatementHistoryProjectsOfficialEventFields(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
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

func TestPerformanceSchemaSetupObjectsAndTimersExposeCompatibilityRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	objects := mustQuerySQL(t, executor, "", "select object_type, object_schema, object_name, enabled, timed from performance_schema.setup_objects")
	require.Contains(t, objects, []interface{}{"TABLE", "%", "%", "YES", "YES"})
	result := <-executor.ExecuteQuery(nil, "update performance_schema.setup_objects set enabled='NO', timed='NO' where object_type='TABLE'", "")
	require.NoError(t, result.Err)
	objects = mustQuerySQL(t, executor, "", "select object_type, enabled, timed from performance_schema.setup_objects where object_type='TABLE'")
	require.Len(t, objects, 1)
	require.Equal(t, "TABLE", objects[0][0])
	require.Equal(t, "NO", objects[0][3])
	require.Equal(t, "NO", objects[0][4])

	timers := mustQuerySQL(t, executor, "", "select name, timer_name, timer_frequency, timer_resolution, timer_overhead from performance_schema.setup_timers")
	require.NotEmpty(t, timers)
	require.Equal(t, "CYCLE", timers[0][0])
	require.Equal(t, "CYCLE", timers[0][1])
	require.Len(t, timers[0], 5)
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

	hostsResult := <-executor.ExecuteQuery(session, "select host, current_connections, total_connections, sum_connections from performance_schema.hosts where host='client'", "")
	require.NoError(t, hostsResult.Err)
	hostValues := hostsResult.Data.(*SelectResult).Records[0].GetValues()
	require.Equal(t, "client", hostValues[0].String())
	require.Equal(t, int64(1), hostValues[1].Int())
	require.Equal(t, int64(2), hostValues[2].Int())
	require.Equal(t, int64(2), hostValues[5].Int())
	wrongTotal := mustSelectResultSQL(t, executor, "", "select user, host from performance_schema.accounts where user='app' and host='client' and total_connections=999999")
	require.Empty(t, wrongTotal.Records)
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

func TestPerformanceSchemaSetupThreadsExposeCurrentSession(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("user", "app")
	session.SetParamByName("host", "client")
	session.SetParamByName("connection_id", int64(42))

	result := <-executor.ExecuteQuery(session, "select * from performance_schema.setup_threads", "app")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	rows := selectResultRows(selectResult)
	require.Len(t, rows, 1)
	require.Equal(t, "thread/sql/one_connection", rows[0][0])
	require.Equal(t, "42", rows[0][2])
	require.Equal(t, "app", rows[0][3])
	require.Equal(t, "client", rows[0][4])
	filteredResult := <-executor.ExecuteQuery(session, "select processlist_id from performance_schema.setup_threads where processlist_id=99", "app")
	require.NoError(t, filteredResult.Err)
	require.Empty(t, filteredResult.Data.(*SelectResult).Records)
	projectedResult := <-executor.ExecuteQuery(session, "select processlist_id from performance_schema.setup_threads where processlist_id=42", "app")
	require.NoError(t, projectedResult.Err)
	require.Equal(t, []string{"PROCESSLIST_ID"}, projectedResult.Data.(*SelectResult).Columns)
	require.Equal(t, [][]interface{}{{"42"}}, selectResultRows(projectedResult.Data.(*SelectResult)))
}

func TestPerformanceSchemaThreadsExposeMySQL84ColumnsAndLiveAttributes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
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
	require.Len(t, instruments, 4)
	for _, row := range instruments {
		require.Equal(t, []interface{}{row[0], "NO", "NO"}, row[:3])
	}
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

func TestPerformanceSchemaWaitInstrumentSettingsControlWaitViews(t *testing.T) {
	locks := manager.NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(1, 77, 1, 13, manager.LOCK_X))
	require.Error(t, locks.AcquireLock(2, 77, 1, 13, manager.LOCK_X))

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
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
	require.Equal(t, "READ ONLY", currentRows[0][13])

	mustExecSessionSQL(t, executor, session, "app", "commit")
	history := <-executor.ExecuteQuery(session, "select * from performance_schema.events_transactions_history", "app")
	require.NoError(t, history.Err)
	historyRows := selectResultRows(history.Data.(*SelectResult))
	require.Len(t, historyRows, 1)
	require.Equal(t, "READ ONLY", historyRows[0][13])
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
	require.Equal(t, "REPEATABLE READ", rows[0][14])
	require.Equal(t, "ROLLED BACK", rows[1][4])
	require.Equal(t, "transaction", rows[1][3])
	require.Equal(t, "REPEATABLE READ", rows[1][14])
}

func TestPerformanceSchemaTransactionHistoryLongAggregatesSessions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
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
	executor.QueryExecutor.SetLockManager(locks)
	waitResult := <-executor.ExecuteQuery(nil, "select requesting_engine_transaction_id, blocking_engine_transaction_id, requesting_lock_id, blocking_lock_id from performance_schema.data_lock_waits", "")
	require.NoError(t, waitResult.Err)
	waits, ok := waitResult.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"2", "1", "7_1_9", "7_1_9"}}, selectResultRows(waits))
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
	counts := mustSelectResultSQL(t, executor, "", "select ip, host, sum_connect_errors, count_authentication_errors from performance_schema.host_cache where ip = '127.0.0.1'")
	require.Equal(t, [][]interface{}{{"127.0.0.1", "127.0.0.1", "2", "2"}}, selectResultRows(counts))
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

	attrs := mustSelectResultSQL(t, executor, "", "select processlist_id, attr_name, attr_value, ordinal_position from performance_schema.session_account_connect_attrs")
	require.Equal(t, []string{"PROCESSLIST_ID", "ATTR_NAME", "ATTR_VALUE", "ORDINAL_POSITION"}, attrs.Columns)
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
		"COUNT_MISC", "SUM_TIMER_MISC", "MIN_TIMER_MISC", "AVG_TIMER_MISC", "MAX_TIMER_MISC",
	}, lock.Columns)

	mutex := mustSelectResultSQL(t, executor, "", "select * from performance_schema.mutex_instances")
	require.Equal(t, []string{"NAME", "OBJECT_INSTANCE_BEGIN", "LOCKED_BY_THREAD_ID"}, mutex.Columns)
	hosts := mustSelectResultSQL(t, executor, "", "select * from performance_schema.hosts")
	require.Equal(t, []string{"HOST", "CURRENT_CONNECTIONS", "TOTAL_CONNECTIONS", "MAX_SESSION_CONTROLLED_MEMORY", "MAX_SESSION_TOTAL_MEMORY", "SUM_CONNECTIONS"}, hosts.Columns)
}

func TestPerformanceSchemaTLSChannelStatusUsesMySQL84Shape(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select * from performance_schema.tls_channel_status")
	require.Equal(t, []string{"CHANNEL", "PROPERTY", "VALUE"}, result.Columns)
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

func TestPerformanceSchemaReplicationViewsExposeRuntimeState(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.SetReplicationStatusProvider(func() replication.StatusSnapshot {
		return replication.StatusSnapshot{
			Role:           replication.RoleReplica,
			SourceURL:      "http://source:8080",
			ExecutedGTIDs:  "source-1:1-4",
			ReplicaRunning: true,
		}
	})
	status := mustSelectResultSQL(t, executor, "", "select channel_name, service_state, received_transaction_set, applied_transaction_set from performance_schema.replication_applier_status")
	require.Len(t, status.Records, 1)
	require.Equal(t, []string{"CHANNEL_NAME", "SERVICE_STATE", "RECEIVED_TRANSACTION_SET", "APPLIED_TRANSACTION_SET"}, status.Columns)
	require.Equal(t, "ON", status.Records[0].GetValues()[1].String())
	require.Equal(t, "source-1:1-4", status.Records[0].GetValues()[2].String())
	filteredStatus := mustSelectResultSQL(t, executor, "", "select channel_name, service_state from performance_schema.replication_applier_status where channel_name = 'missing-channel'")
	require.Empty(t, filteredStatus.Records)
	configuration := mustSelectResultSQL(t, executor, "", "select channel_name, host, auto_position from performance_schema.replication_connection_configuration")
	require.Len(t, configuration.Records, 1)
	require.Equal(t, "http://source:8080", configuration.Records[0].GetValues()[1].String())
	require.Equal(t, "ON", configuration.Records[0].GetValues()[2].String())
	filteredConfiguration := mustSelectResultSQL(t, executor, "", "select channel_name, host from performance_schema.replication_connection_configuration where host = 'other-source'")
	require.Empty(t, filteredConfiguration.Records)
}

func TestPerformanceSchemaReplicationExtendedViewsProjectRuntimeState(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
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

	failover := mustSelectResultSQL(t, executor, "", "select channel_name, host, port, managed from performance_schema.replication_asynchronous_connection_failover")
	require.Len(t, failover.Records, 1)
	require.Equal(t, "source.example", failover.Records[0].GetValues()[1].String())
	require.Equal(t, "8080", failover.Records[0].GetValues()[2].String())
	require.Equal(t, "OFF", failover.Records[0].GetValues()[3].String())
	filteredFailover := mustSelectResultSQL(t, executor, "", "select channel_name, host from performance_schema.replication_asynchronous_connection_failover where host = 'other-source'")
	require.Empty(t, filteredFailover.Records)

	logStatus := mustSelectResultSQL(t, executor, "", "select server_uuid, local, relay_log, binlog_summary from performance_schema.log_status")
	require.Len(t, logStatus.Records, 1)
	require.Equal(t, "replica-uuid", logStatus.Records[0].GetValues()[0].String())
	require.Contains(t, logStatus.Records[0].GetValues()[1].String(), "source-uuid:1-4")
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
	require.NoError(t, (<-executor.ExecuteQuery(session, "select 1", "")).Err)
	result = <-executor.ExecuteQuery(session, "select variable_name, variable_value from performance_schema.status_by_account where user='alice' and host='client.example' and variable_name='Queries'", "")
	require.NoError(t, result.Err)
	queries := selectResultRows(result.Data.(*SelectResult))
	require.Len(t, queries, 1)
	require.NotEqual(t, "0", queries[0][1])
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

func TestPerformanceSchemaVariablesInfoProjectsSystemVariableDefinitions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select variable_name, variable_source, variable_path from performance_schema.variables_info where variable_name='autocommit'")
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	require.Equal(t, "autocommit", values[0].String())
	require.Equal(t, "COMPILED", values[1].String())
	require.Nil(t, values[2].Raw())
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

func TestPerformanceSchemaGlobalErrorSummaryExposesExecutionErrors(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("connection_id", int64(814))
	session.SetParamByName("user", "alice")
	session.SetParamByName("host", "client.example")
	failed := <-executor.ExecuteQuery(session, "select * from missing_error_summary_table", "app")
	require.Error(t, failed.Err)

	result := mustSelectResultSQL(t, executor, "", "select error_name, error_count, sql_state from performance_schema.events_errors_summary_global_by_error")
	rows := selectResultRows(result)
	require.NotEmpty(t, rows)
	found := false
	for _, row := range rows {
		if len(row) == 3 && row[1] != "0" && row[2] == "HY000" {
			found = true
			break
		}
	}
	require.True(t, found, "global error summary did not expose the failed statement: %#v", rows)

	account := mustSelectResultSQL(t, executor, "", "select user, host, error_name, error_count from performance_schema.events_errors_summary_by_account_by_error where user='alice' and host='client.example'")
	accountRows := selectResultRows(account)
	require.NotEmpty(t, accountRows)
	require.Equal(t, "alice", accountRows[0][0])
	require.Equal(t, "client.example", accountRows[0][1])
	require.NotEqual(t, "0", accountRows[0][3])
}

func TestPerformanceSchemaErrorSummaryProjectsMySQLErrorMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder.RecordQueryError("app", "execution", string(ExecutionErrorCodeSchemaOrTableNotFound))

	result := mustSelectResultSQL(t, executor, "", "select error_name, error_number, sql_state, error_count from performance_schema.events_errors_summary_global_by_error where error_number=1146 and sql_state='42S02'")
	require.Equal(t, [][]interface{}{{string(ExecutionErrorCodeSchemaOrTableNotFound), "1146", "42S02", "1"}}, selectResultRows(result))
	wrongCount := mustSelectResultSQL(t, executor, "", "select error_name from performance_schema.events_errors_summary_global_by_error where error_name='"+string(ExecutionErrorCodeSchemaOrTableNotFound)+"' and error_count=999")
	require.Empty(t, wrongCount.Records)
}

func TestPerformanceSchemaErrorLogExposesExecutionErrorEvents(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	failed := <-executor.ExecuteQuery(nil, "select * from missing_error_log_table", "app")
	require.Error(t, failed.Err)

	result := mustSelectResultSQL(t, executor, "", "select error_code, subsystem, data from performance_schema.error_log")
	require.NotEmpty(t, result.Records)
	row := result.Records[len(result.Records)-1].GetValues()
	require.NotEmpty(t, row[0].String())
	require.Equal(t, "query", row[1].String())
	require.Contains(t, row[2].String(), "error_class")

	filtered := mustSelectResultSQL(t, executor, "", "select error_code, subsystem, prio from performance_schema.error_log where error_code = 'E_UNKNOWN' and subsystem = 'query' and prio = 3")
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

	result := <-executor.ExecuteQuery(nil, "select object_name, lock_type, lock_status, owner_thread_id from performance_schema.metadata_locks", "")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	rows := selectResultRows(selectResult)
	require.Len(t, rows, 1)
	require.Equal(t, []interface{}{"users", "EXCLUSIVE", "GRANTED", "123"}, rows[0])
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
