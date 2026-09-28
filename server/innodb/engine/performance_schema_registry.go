package engine

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/observability/compatibility"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

// performanceSchemaTableRegistry is the server-side compatibility registry for
// MySQL 8.4 Performance Schema table names.  The tables that have a dedicated
// executor are handled before this registry.  The registry keeps the remaining
// virtual tables queryable with their native column shape while their runtime
// population is implemented incrementally.
var performanceSchemaEventsWaitColumns = []string{
	"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT", "SPINS",
	"OBJECT_SCHEMA", "OBJECT_NAME", "INDEX_NAME", "OBJECT_TYPE", "OBJECT_INSTANCE_BEGIN", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE",
	"OPERATION", "NUMBER_OF_BYTES", "FLAGS",
}

var performanceSchemaTableLockWaitSummaryColumns = []string{
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
}

// Keeping the names in one place is important: clients commonly discover
// Performance Schema capabilities through SHOW TABLES and INFORMATION_SCHEMA
// before issuing a diagnostic query.
var performanceSchemaTableRegistry = map[string][]string{
	"accounts": {"USER", "HOST", "CURRENT_CONNECTIONS", "TOTAL_CONNECTIONS", "MAX_SESSION_CONTROLLED_MEMORY", "MAX_SESSION_TOTAL_MEMORY"},
	"binary_log_transaction_compression_stats": {"LOG_TYPE", "COMPRESSION_TYPE", "TRANSACTION_COUNTER", "COMPRESSED_BYTES_COUNTER", "UNCOMPRESSED_BYTES_COUNTER", "COMPRESSION_PERCENTAGE", "FIRST_TRANSACTION_ID", "FIRST_TRANSACTION_COMPRESSED_BYTES", "FIRST_TRANSACTION_UNCOMPRESSED_BYTES", "FIRST_TRANSACTION_TIMESTAMP", "LAST_TRANSACTION_ID", "LAST_TRANSACTION_COMPRESSED_BYTES", "LAST_TRANSACTION_UNCOMPRESSED_BYTES", "LAST_TRANSACTION_TIMESTAMP"},
	"clone_progress": {"ID", "STAGE", "STATE", "BEGIN_TIME", "END_TIME", "THREADS", "ESTIMATE", "DATA", "NETWORK", "DATA_SPEED", "NETWORK_SPEED"},
	"clone_status":   {"ID", "PID", "STATE", "BEGIN_TIME", "END_TIME", "SOURCE", "DESTINATION", "ERROR_NO", "ERROR_MESSAGE", "BINLOG_FILE", "BINLOG_POSITION", "GTID_EXECUTED"},
	// MySQL exposes the scheduler component's task state through six columns.
	// The Enterprise scheduler component is not part of this server, so the
	// registry currently exposes the native discovery shape while returning no
	// synthetic task rows.
	"component_scheduler_tasks":                            {"NAME", "STATUS", "COMMENT", "INTERVAL_SECONDS", "TIMES_RUN", "TIMES_FAILED"},
	"cond_instances":                                       {"NAME", "OBJECT_INSTANCE_BEGIN"},
	"events_errors_summary_by_account_by_error":            {"USER", "HOST", "ERROR_NUMBER", "ERROR_NAME", "SQL_STATE", "SUM_ERROR_RAISED", "SUM_ERROR_HANDLED", "FIRST_SEEN", "LAST_SEEN"},
	"events_errors_summary_by_host_by_error":               {"HOST", "ERROR_NUMBER", "ERROR_NAME", "SQL_STATE", "SUM_ERROR_RAISED", "SUM_ERROR_HANDLED", "FIRST_SEEN", "LAST_SEEN"},
	"events_errors_summary_by_thread_by_error":             {"THREAD_ID", "ERROR_NUMBER", "ERROR_NAME", "SQL_STATE", "SUM_ERROR_RAISED", "SUM_ERROR_HANDLED", "FIRST_SEEN", "LAST_SEEN"},
	"events_errors_summary_by_user_by_error":               {"USER", "ERROR_NUMBER", "ERROR_NAME", "SQL_STATE", "SUM_ERROR_RAISED", "SUM_ERROR_HANDLED", "FIRST_SEEN", "LAST_SEEN"},
	"events_errors_summary_global_by_error":                {"ERROR_NUMBER", "ERROR_NAME", "SQL_STATE", "SUM_ERROR_RAISED", "SUM_ERROR_HANDLED", "FIRST_SEEN", "LAST_SEEN"},
	"events_stages_summary_by_account_by_event_name":       performanceSchemaStageSummaryAccountColumns,
	"events_stages_summary_by_host_by_event_name":          performanceSchemaStageSummaryHostColumns,
	"events_stages_summary_by_user_by_event_name":          performanceSchemaStageSummaryUserColumns,
	"events_statements_histogram_by_digest":                {"SCHEMA_NAME", "DIGEST", "BUCKET_NUMBER", "BUCKET_TIMER_LOW", "BUCKET_TIMER_HIGH", "COUNT_BUCKET", "COUNT_BUCKET_AND_LOWER", "BUCKET_QUANTILE"},
	"events_statements_histogram_global":                   {"BUCKET_NUMBER", "BUCKET_TIMER_LOW", "BUCKET_TIMER_HIGH", "COUNT_BUCKET", "COUNT_BUCKET_AND_LOWER", "BUCKET_QUANTILE"},
	"events_statements_summary_by_account_by_event_name":   performanceSchemaStatementSummaryAccountColumns,
	"events_statements_summary_by_host_by_event_name":      performanceSchemaStatementSummaryHostColumns,
	"events_statements_summary_by_program":                 performanceSchemaStatementSummaryProgramColumns,
	"events_statements_summary_by_thread_by_event_name":    performanceSchemaStatementSummaryThreadColumns,
	"events_statements_summary_by_user_by_event_name":      performanceSchemaStatementSummaryUserColumns,
	"events_statements_summary_global_by_event_name":       performanceSchemaStatementSummaryGlobalColumns,
	"events_transactions_summary_by_account_by_event_name": performanceSchemaTransactionSummaryAccountColumns,
	"events_transactions_summary_by_host_by_event_name":    performanceSchemaTransactionSummaryHostColumns,
	"events_transactions_summary_by_thread_by_event_name":  performanceSchemaTransactionSummaryThreadColumns,
	"events_transactions_summary_by_user_by_event_name":    performanceSchemaTransactionSummaryUserColumns,
	"events_transactions_summary_global_by_event_name":     performanceSchemaTransactionSummaryGlobalColumns,
	"events_waits_summary_by_account_by_event_name":        performanceSchemaWaitSummaryAccountColumns,
	"events_waits_summary_by_host_by_event_name":           performanceSchemaWaitSummaryHostColumns,
	"events_waits_summary_by_instance":                     performanceSchemaWaitSummaryInstanceColumns,
	"events_waits_summary_by_user_by_event_name":           performanceSchemaWaitSummaryUserColumns,
	"file_summary_by_event_name":                           {"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "COUNT_READ", "SUM_TIMER_READ", "MIN_TIMER_READ", "AVG_TIMER_READ", "MAX_TIMER_READ", "SUM_NUMBER_OF_BYTES_READ", "COUNT_WRITE", "SUM_TIMER_WRITE", "MIN_TIMER_WRITE", "AVG_TIMER_WRITE", "MAX_TIMER_WRITE", "SUM_NUMBER_OF_BYTES_WRITE", "COUNT_MISC", "SUM_TIMER_MISC", "MIN_TIMER_MISC", "AVG_TIMER_MISC", "MAX_TIMER_MISC"},
	"file_summary_by_instance":                             {"FILE_NAME", "EVENT_NAME", "OBJECT_INSTANCE_BEGIN", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "COUNT_READ", "SUM_TIMER_READ", "MIN_TIMER_READ", "AVG_TIMER_READ", "MAX_TIMER_READ", "SUM_NUMBER_OF_BYTES_READ", "COUNT_WRITE", "SUM_TIMER_WRITE", "MIN_TIMER_WRITE", "AVG_TIMER_WRITE", "MAX_TIMER_WRITE", "SUM_NUMBER_OF_BYTES_WRITE", "COUNT_MISC", "SUM_TIMER_MISC", "MIN_TIMER_MISC", "AVG_TIMER_MISC", "MAX_TIMER_MISC"},
	"firewall_group_allowlist":                             {"USERHOST", "RULE", "ID"},
	"firewall_groups":                                      {"NAME", "MODE", "ENABLED", "WEIGHT", "DESCRIPTION"},
	"firewall_membership":                                  {"GROUP_ID", "MEMBER_ID", "USERHOST"},
	"global_status":                                        {"VARIABLE_NAME", "VARIABLE_VALUE"},
	"global_variables":                                     {"VARIABLE_NAME", "VARIABLE_VALUE"},
	"hosts":                                                {"HOST", "CURRENT_CONNECTIONS", "TOTAL_CONNECTIONS", "MAX_SESSION_CONTROLLED_MEMORY", "MAX_SESSION_TOTAL_MEMORY"},
	"keyring_component_status":                             {"STATUS_KEY", "STATUS_VALUE"},
	"keyring_keys":                                         {"KEY_ID", "KEY_OWNER", "BACKEND_KEY_ID"},
	"error_log":                                            {"LOGGED", "THREAD_ID", "PRIO", "ERROR_CODE", "SUBSYSTEM", "DATA"},
	"innodb_redo_log_files":                                {"FILE_ID", "FILE_NAME", "START_LSN", "END_LSN", "SIZE_IN_BYTES", "IS_FULL", "CONSUMER_LEVEL"},
	"log_status":                                           {"SERVER_UUID", "LOCAL", "REPLICATION", "STORAGE_ENGINES"},
	"memory_summary_by_account_by_event_name":              performanceSchemaMemorySummaryAccountColumns,
	"memory_summary_by_host_by_event_name":                 performanceSchemaMemorySummaryHostColumns,
	"memory_summary_by_user_by_event_name":                 performanceSchemaMemorySummaryUserColumns,
	"metadata_locks":                                       {"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "COLUMN_NAME", "OBJECT_INSTANCE_BEGIN", "LOCK_TYPE", "LOCK_DURATION", "LOCK_STATUS", "SOURCE", "OWNER_THREAD_ID", "OWNER_EVENT_ID"},
	"mutex_instances":                                      {"NAME", "OBJECT_INSTANCE_BEGIN", "LOCKED_BY_THREAD_ID"},
	"ndb_sync_excluded_objects":                            {"DATABASE", "TABLE", "REASON"},
	"ndb_sync_pending_objects":                             {"DATABASE", "TABLE"},
	"persisted_variables":                                  {"VARIABLE_NAME", "VARIABLE_VALUE"},
	"prepared_statements_instances":                        {"OBJECT_INSTANCE_BEGIN", "STATEMENT_ID", "STATEMENT_NAME", "SQL_TEXT", "OWNER_THREAD_ID", "OWNER_EVENT_ID", "OWNER_OBJECT_TYPE", "OWNER_OBJECT_SCHEMA", "OWNER_OBJECT_NAME", "EXECUTION_ENGINE", "TIMER_PREPARE", "COUNT_REPREPARE", "COUNT_EXECUTE", "SUM_TIMER_EXECUTE", "MIN_TIMER_EXECUTE", "AVG_TIMER_EXECUTE", "MAX_TIMER_EXECUTE", "SUM_LOCK_TIME", "SUM_ERRORS", "SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED", "SUM_CREATED_TMP_DISK_TABLES", "SUM_CREATED_TMP_TABLES", "SUM_SELECT_FULL_JOIN", "SUM_SELECT_FULL_RANGE_JOIN", "SUM_SELECT_RANGE", "SUM_SELECT_RANGE_CHECK", "SUM_SELECT_SCAN", "SUM_SORT_MERGE_PASSES", "SUM_SORT_RANGE", "SUM_SORT_ROWS", "SUM_SORT_SCAN", "SUM_NO_INDEX_USED", "SUM_NO_GOOD_INDEX_USED", "SUM_CPU_TIME", "MAX_CONTROLLED_MEMORY", "MAX_TOTAL_MEMORY", "COUNT_SECONDARY"},
	"processlist":                                          {"ID", "USER", "HOST", "DB", "COMMAND", "TIME", "STATE", "INFO", "EXECUTION_ENGINE"},
	"replication_applier_configuration":                    {"CHANNEL_NAME", "DESIRED_DELAY", "PRIVILEGE_CHECKS_USER", "REQUIRE_ROW_FORMAT", "REQUIRE_TABLE_PRIMARY_KEY_CHECK", "ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS_TYPE", "ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS_VALUE"},
	"replication_applier_filters":                          {"CHANNEL_NAME", "FILTER_NAME", "FILTER_RULE", "CONFIGURED_BY", "ACTIVE_SINCE", "COUNTER"},
	"replication_applier_global_filters":                   {"FILTER_NAME", "FILTER_RULE", "CONFIGURED_BY", "ACTIVE_SINCE"},
	"replication_applier_status":                           {"CHANNEL_NAME", "SERVICE_STATE", "REMAINING_DELAY", "COUNT_TRANSACTIONS_RETRIES"},
	"replication_applier_status_by_coordinator":            {"CHANNEL_NAME", "THREAD_ID", "SERVICE_STATE", "LAST_ERROR_NUMBER", "LAST_ERROR_MESSAGE", "LAST_ERROR_TIMESTAMP", "LAST_PROCESSED_TRANSACTION", "LAST_PROCESSED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP", "LAST_PROCESSED_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP", "LAST_PROCESSED_TRANSACTION_START_BUFFER_TIMESTAMP", "LAST_PROCESSED_TRANSACTION_END_BUFFER_TIMESTAMP", "PROCESSING_TRANSACTION", "PROCESSING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP", "PROCESSING_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP", "PROCESSING_TRANSACTION_START_BUFFER_TIMESTAMP"},
	"replication_applier_status_by_worker":                 {"CHANNEL_NAME", "WORKER_ID", "THREAD_ID", "SERVICE_STATE", "LAST_ERROR_NUMBER", "LAST_ERROR_MESSAGE", "LAST_ERROR_TIMESTAMP", "LAST_APPLIED_TRANSACTION", "LAST_APPLIED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP", "LAST_APPLIED_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP", "LAST_APPLIED_TRANSACTION_START_APPLY_TIMESTAMP", "LAST_APPLIED_TRANSACTION_END_APPLY_TIMESTAMP", "APPLYING_TRANSACTION", "APPLYING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP", "APPLYING_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP", "APPLYING_TRANSACTION_START_APPLY_TIMESTAMP", "LAST_APPLIED_TRANSACTION_RETRIES_COUNT", "LAST_APPLIED_TRANSACTION_LAST_TRANSIENT_ERROR_NUMBER", "LAST_APPLIED_TRANSACTION_LAST_TRANSIENT_ERROR_MESSAGE", "LAST_APPLIED_TRANSACTION_LAST_TRANSIENT_ERROR_TIMESTAMP", "APPLYING_TRANSACTION_RETRIES_COUNT", "APPLYING_TRANSACTION_LAST_TRANSIENT_ERROR_NUMBER", "APPLYING_TRANSACTION_LAST_TRANSIENT_ERROR_MESSAGE", "APPLYING_TRANSACTION_LAST_TRANSIENT_ERROR_TIMESTAMP"},
	"replication_asynchronous_connection_failover":         {"CHANNEL_NAME", "HOST", "PORT", "NETWORK_NAMESPACE", "WEIGHT", "MANAGED_NAME"},
	"replication_asynchronous_connection_failover_managed": {"CHANNEL_NAME", "MANAGED_NAME", "MANAGED_TYPE", "CONFIGURATION"},
	"replication_connection_configuration":                 {"CHANNEL_NAME", "HOST", "PORT", "USER", "NETWORK_INTERFACE", "AUTO_POSITION", "SSL_ALLOWED", "SSL_CA_FILE", "SSL_CA_PATH", "SSL_CERTIFICATE", "SSL_CIPHER", "SSL_KEY", "SSL_VERIFY_SERVER_CERTIFICATE", "SSL_CRL_FILE", "SSL_CRL_PATH", "CONNECTION_RETRY_INTERVAL", "CONNECTION_RETRY_COUNT", "HEARTBEAT_INTERVAL", "TLS_VERSION", "PUBLIC_KEY_PATH", "GET_PUBLIC_KEY", "NETWORK_NAMESPACE", "COMPRESSION_ALGORITHM", "ZSTD_COMPRESSION_LEVEL", "TLS_CIPHERSUITES", "SOURCE_CONNECTION_AUTO_FAILOVER", "GTID_ONLY"},
	"replication_connection_status":                        {"CHANNEL_NAME", "GROUP_NAME", "SOURCE_UUID", "THREAD_ID", "SERVICE_STATE", "COUNT_RECEIVED_HEARTBEATS", "LAST_HEARTBEAT_TIMESTAMP", "RECEIVED_TRANSACTION_SET", "LAST_ERROR_NUMBER", "LAST_ERROR_MESSAGE", "LAST_ERROR_TIMESTAMP", "LAST_QUEUED_TRANSACTION", "LAST_QUEUED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP", "LAST_QUEUED_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP", "LAST_QUEUED_TRANSACTION_START_QUEUE_TIMESTAMP", "LAST_QUEUED_TRANSACTION_END_QUEUE_TIMESTAMP", "QUEUEING_TRANSACTION", "QUEUEING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP", "QUEUEING_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP", "QUEUEING_TRANSACTION_START_QUEUE_TIMESTAMP"},
	"replication_group_communication_information":          {"WRITE_CONCURRENCY", "PROTOCOL_VERSION", "WRITE_CONSENSUS_LEADERS_PREFERRED", "WRITE_CONSENSUS_LEADERS_ACTUAL", "WRITE_CONSENSUS_SINGLE_LEADER_CAPABLE", "MEMBER_FAILURE_SUSPICIONS_COUNT"},
	"replication_group_configuration_version":              {"NAME", "VERSION"},
	"replication_group_member_actions":                     {"NAME", "EVENT", "ENABLED", "TYPE", "PRIORITY", "ERROR_HANDLING"},
	"replication_group_member_stats":                       {"CHANNEL_NAME", "VIEW_ID", "MEMBER_ID", "COUNT_TRANSACTIONS_IN_QUEUE", "COUNT_TRANSACTIONS_CHECKED", "COUNT_CONFLICTS_DETECTED", "COUNT_TRANSACTIONS_ROWS_VALIDATING", "TRANSACTIONS_COMMITTED_ALL_MEMBERS", "LAST_CONFLICT_FREE_TRANSACTION", "COUNT_TRANSACTIONS_REMOTE_IN_APPLIER_QUEUE", "COUNT_TRANSACTIONS_REMOTE_APPLIED", "COUNT_TRANSACTIONS_LOCAL_PROPOSED", "COUNT_TRANSACTIONS_LOCAL_ROLLBACK"},
	"replication_group_members":                            {"CHANNEL_NAME", "MEMBER_ID", "MEMBER_HOST", "MEMBER_PORT", "MEMBER_STATE", "MEMBER_ROLE", "MEMBER_VERSION", "MEMBER_COMMUNICATION_STACK"},
	"rwlock_instances":                                     {"NAME", "OBJECT_INSTANCE_BEGIN", "WRITE_LOCKED_BY_THREAD_ID", "READ_LOCKED_BY_COUNT"},
	"setup_loggers":                                        {"NAME", "LEVEL", "DESCRIPTION"},
	"setup_meters":                                         {"NAME", "FREQUENCY", "ENABLED", "DESCRIPTION"},
	"setup_metrics":                                        {"NAME", "METER", "METRIC_TYPE", "NUM_TYPE", "UNIT", "DESCRIPTION"},
	"session_account_connect_attrs":                        {"PROCESSLIST_ID", "ATTR_NAME", "ATTR_VALUE", "ORDINAL_POSITION"},
	"session_status":                                       {"VARIABLE_NAME", "VARIABLE_VALUE"},
	"session_variables":                                    {"VARIABLE_NAME", "VARIABLE_VALUE"},
	"status_by_account":                                    {"USER", "HOST", "VARIABLE_NAME", "VARIABLE_VALUE"},
	"status_by_host":                                       {"HOST", "VARIABLE_NAME", "VARIABLE_VALUE"},
	"status_by_thread":                                     {"THREAD_ID", "VARIABLE_NAME", "VARIABLE_VALUE"},
	"status_by_user":                                       {"USER", "VARIABLE_NAME", "VARIABLE_VALUE"},
	"table_handles":                                        {"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "OBJECT_INSTANCE_BEGIN", "OWNER_THREAD_ID", "OWNER_EVENT_ID", "INTERNAL_LOCK", "EXTERNAL_LOCK"},
	"table_io_waits_summary_by_table":                      {"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "COUNT_READ", "SUM_TIMER_READ", "MIN_TIMER_READ", "AVG_TIMER_READ", "MAX_TIMER_READ", "COUNT_WRITE", "SUM_TIMER_WRITE", "MIN_TIMER_WRITE", "AVG_TIMER_WRITE", "MAX_TIMER_WRITE", "COUNT_FETCH", "SUM_TIMER_FETCH", "MIN_TIMER_FETCH", "AVG_TIMER_FETCH", "MAX_TIMER_FETCH", "COUNT_INSERT", "SUM_TIMER_INSERT", "MIN_TIMER_INSERT", "AVG_TIMER_INSERT", "MAX_TIMER_INSERT", "COUNT_UPDATE", "SUM_TIMER_UPDATE", "MIN_TIMER_UPDATE", "AVG_TIMER_UPDATE", "MAX_TIMER_UPDATE", "COUNT_DELETE", "SUM_TIMER_DELETE", "MIN_TIMER_DELETE", "AVG_TIMER_DELETE", "MAX_TIMER_DELETE"},
	"table_io_waits_summary_by_index_usage":                {"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "INDEX_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "COUNT_READ", "SUM_TIMER_READ", "MIN_TIMER_READ", "AVG_TIMER_READ", "MAX_TIMER_READ", "COUNT_WRITE", "SUM_TIMER_WRITE", "MIN_TIMER_WRITE", "AVG_TIMER_WRITE", "MAX_TIMER_WRITE", "COUNT_FETCH", "SUM_TIMER_FETCH", "MIN_TIMER_FETCH", "AVG_TIMER_FETCH", "MAX_TIMER_FETCH", "COUNT_INSERT", "SUM_TIMER_INSERT", "MIN_TIMER_INSERT", "AVG_TIMER_INSERT", "MAX_TIMER_INSERT", "COUNT_UPDATE", "SUM_TIMER_UPDATE", "MIN_TIMER_UPDATE", "AVG_TIMER_UPDATE", "MAX_TIMER_UPDATE", "COUNT_DELETE", "SUM_TIMER_DELETE", "MIN_TIMER_DELETE", "AVG_TIMER_DELETE", "MAX_TIMER_DELETE"},
	"table_lock_waits_summary_by_table":                    performanceSchemaTableLockWaitSummaryColumns,
	// MySQL 8.4 exposes one row per TLS property, rather than one wide row per
	// channel.  Keep this shape exact so discovery through I_S.COLUMNS and
	// SELECT * both match the native table contract.
	"tls_channel_status":       {"CHANNEL", "PROPERTY", "VALUE"},
	"tp_connections":           {"CONNECTION_ID", "TP_GROUP_ID", "TP_PROCESSING_THREAD_NUMBER", "THREAD_ID", "STATE", "ACTIVE_FLAG", "KILLED_STATE", "CLEANUP_STATE", "TIME_OF_LAST_EVENT_COMPLETION", "TIME_OF_EXPIRY", "TIME_OF_ADD", "TIME_OF_POP", "TIME_OF_ARM", "CONNECT_HANDLER_INDEX", "TYPE", "DIRECT_QUERY_EVENTS", "QUEUED_QUERY_EVENTS", "TIME_OF_EVENT_ARRIVAL", "MANAGEMENT_TIME"},
	"tp_thread_group_state":    {"TP_GROUP_ID", "CONSUMER_THREADS", "RESERVE_THREADS", "CONNECT_THREAD_COUNT", "CONNECTION_COUNT", "QUEUED_QUERIES", "QUEUED_TRANSACTIONS", "STALL_LIMIT", "PRIO_KICKUP_TIMER", "ALGORITHM", "THREAD_COUNT", "ACTIVE_THREAD_COUNT", "STALLED_THREAD_COUNT", "WAITING_THREAD_NUMBER", "OLDEST_QUEUED", "MAX_THREAD_IDS_IN_GROUP", "EFFECTIVE_MAX_TRANSACTIONS_LIMIT", "NUM_QUERY_THREADS", "TIME_OF_LAST_THREAD_CREATION", "NUM_CONNECT_HANDLER_THREAD_IN_SLEEP", "THREADS_BOUND_TO_TRANSACTION", "QUERY_THREADS_COUNT", "TIME_OF_EARLIEST_CON_EXPIRE"},
	"tp_thread_group_stats":    {"TP_GROUP_ID", "CONNECTIONS_STARTED", "CONNECTIONS_CLOSED", "QUERIES_EXECUTED", "QUERIES_QUEUED", "THREADS_STARTED", "PRIO_KICKUPS", "STALLED_QUERIES_EXECUTED", "BECOME_CONSUMER_THREAD", "BECOME_RESERVE_THREAD", "BECOME_WAITING_THREAD", "WAKE_THREAD_STALL_CHECKER", "SLEEP_WAITS", "DISK_IO_WAITS", "ROW_LOCK_WAITS", "GLOBAL_LOCK_WAITS", "META_DATA_LOCK_WAITS", "TABLE_LOCK_WAITS", "USER_LOCK_WAITS", "BINLOG_WAITS", "GROUP_COMMIT_WAITS", "FSYNC_WAITS"},
	"tp_thread_state":          {"TP_GROUP_ID", "TP_THREAD_NUMBER", "PROCESS_COUNT", "WAIT_TYPE", "TP_THREAD_TYPE", "THREAD_ID"},
	"user_defined_functions":   {"UDF_NAME", "UDF_RETURN_TYPE", "UDF_TYPE", "UDF_LIBRARY", "UDF_USAGE_COUNT"},
	"user_variables_by_thread": {"THREAD_ID", "VARIABLE_NAME", "VARIABLE_VALUE"},
	"variables_by_thread":      {"THREAD_ID", "VARIABLE_NAME", "VARIABLE_VALUE"},
	"variables_info":           {"VARIABLE_NAME", "VARIABLE_SOURCE", "VARIABLE_PATH", "MIN_VALUE", "MAX_VALUE", "SET_TIME", "SET_USER", "SET_HOST"},
}

// These tables have dedicated executors in executor.go.  They are repeated in
// the registry only for metadata discovery: INFORMATION_SCHEMA.COLUMNS must
// expose the same ordered shape as SELECT * from the dedicated handler.
var performanceSchemaDedicatedTableColumns = map[string][]string{
	"events_statements_current":                     performanceSchemaStatementEventColumns,
	"events_statements_history":                     performanceSchemaStatementEventColumns,
	"events_statements_history_long":                performanceSchemaStatementEventColumns,
	"events_stages_current":                         {"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT", "WORK_COMPLETED", "WORK_ESTIMATED", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE"},
	"events_stages_history":                         {"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT", "WORK_COMPLETED", "WORK_ESTIMATED", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE"},
	"events_stages_history_long":                    {"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT", "WORK_COMPLETED", "WORK_ESTIMATED", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE"},
	"events_transactions_current":                   performanceSchemaTransactionEventColumns,
	"events_transactions_history":                   performanceSchemaTransactionEventColumns,
	"events_transactions_history_long":              performanceSchemaTransactionEventColumns,
	"events_waits_current":                          performanceSchemaEventsWaitColumns,
	"events_waits_history":                          performanceSchemaEventsWaitColumns,
	"events_waits_history_long":                     performanceSchemaEventsWaitColumns,
	"events_statements_summary_by_digest":           performanceSchemaStatementDigestColumns,
	"data_locks":                                    {"ENGINE", "ENGINE_LOCK_ID", "ENGINE_TRANSACTION_ID", "THREAD_ID", "EVENT_ID", "OBJECT_SCHEMA", "OBJECT_NAME", "PARTITION_NAME", "SUBPARTITION_NAME", "INDEX_NAME", "OBJECT_INSTANCE_BEGIN", "LOCK_TYPE", "LOCK_MODE", "LOCK_STATUS", "LOCK_DATA"},
	"data_lock_waits":                               {"ENGINE", "REQUESTING_ENGINE_LOCK_ID", "REQUESTING_ENGINE_TRANSACTION_ID", "REQUESTING_THREAD_ID", "REQUESTING_EVENT_ID", "REQUESTING_OBJECT_INSTANCE_BEGIN", "BLOCKING_ENGINE_LOCK_ID", "BLOCKING_ENGINE_TRANSACTION_ID", "BLOCKING_THREAD_ID", "BLOCKING_EVENT_ID", "BLOCKING_OBJECT_INSTANCE_BEGIN"},
	"metadata_locks":                                {"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "COLUMN_NAME", "OBJECT_INSTANCE_BEGIN", "LOCK_TYPE", "LOCK_DURATION", "LOCK_STATUS", "SOURCE", "OWNER_THREAD_ID", "OWNER_EVENT_ID"},
	"events_waits_summary_by_thread_by_event_name":  {"THREAD_ID", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_waits_summary_global_by_event_name":     {"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_stages_summary_by_thread_by_event_name": {"THREAD_ID", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_stages_summary_global_by_event_name":    {"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"memory_summary_global_by_event_name":           {"EVENT_NAME", "COUNT_ALLOC", "COUNT_FREE", "SUM_NUMBER_OF_BYTES_ALLOC", "SUM_NUMBER_OF_BYTES_FREE", "LOW_COUNT_USED", "CURRENT_COUNT_USED", "HIGH_COUNT_USED", "LOW_NUMBER_OF_BYTES_USED", "CURRENT_NUMBER_OF_BYTES_USED", "HIGH_NUMBER_OF_BYTES_USED"},
	"memory_summary_by_thread_by_event_name":        {"THREAD_ID", "EVENT_NAME", "COUNT_ALLOC", "COUNT_FREE", "SUM_NUMBER_OF_BYTES_ALLOC", "SUM_NUMBER_OF_BYTES_FREE", "LOW_COUNT_USED", "CURRENT_COUNT_USED", "HIGH_COUNT_USED", "LOW_NUMBER_OF_BYTES_USED", "CURRENT_NUMBER_OF_BYTES_USED", "HIGH_NUMBER_OF_BYTES_USED"},
	"performance_timers":                            {"TIMER_NAME", "TIMER_FREQUENCY", "TIMER_RESOLUTION", "TIMER_OVERHEAD"},
	"host_cache":                                    {"IP", "HOST", "HOST_VALIDATED", "SUM_CONNECT_ERRORS", "COUNT_HOST_BLOCKED_ERRORS", "COUNT_NAMEINFO_TRANSIENT_ERRORS", "COUNT_NAMEINFO_PERMANENT_ERRORS", "COUNT_FORMAT_ERRORS", "COUNT_ADDRINFO_TRANSIENT_ERRORS", "COUNT_ADDRINFO_PERMANENT_ERRORS", "COUNT_FCRDNS_ERRORS", "COUNT_HOST_ACL_ERRORS", "COUNT_NO_AUTH_PLUGIN_ERRORS", "COUNT_AUTH_PLUGIN_ERRORS", "COUNT_HANDSHAKE_ERRORS", "COUNT_PROXY_USER_ERRORS", "COUNT_PROXY_USER_ACL_ERRORS", "COUNT_AUTHENTICATION_ERRORS", "COUNT_SSL_ERRORS", "COUNT_MAX_USER_CONNECTIONS_ERRORS", "COUNT_MAX_USER_CONNECTIONS_PER_HOUR_ERRORS", "COUNT_DEFAULT_DATABASE_ERRORS", "COUNT_INIT_CONNECT_ERRORS", "COUNT_LOCAL_ERRORS", "COUNT_UNKNOWN_ERRORS", "FIRST_SEEN", "LAST_SEEN", "FIRST_ERROR_SEEN", "LAST_ERROR_SEEN"},
	"threads":                                       {"THREAD_ID", "NAME", "TYPE", "PROCESSLIST_ID", "PROCESSLIST_USER", "PROCESSLIST_HOST", "PROCESSLIST_DB", "PROCESSLIST_COMMAND", "PROCESSLIST_TIME", "PROCESSLIST_STATE", "PROCESSLIST_INFO", "PARENT_THREAD_ID", "ROLE", "INSTRUMENTED", "HISTORY", "CONNECTION_TYPE", "THREAD_OS_ID", "RESOURCE_GROUP", "EXECUTION_ENGINE", "CONTROLLED_MEMORY", "MAX_CONTROLLED_MEMORY", "TOTAL_MEMORY", "MAX_TOTAL_MEMORY", "TELEMETRY_ACTIVE"},
	"setup_threads":                                 {"NAME", "ENABLED", "HISTORY", "PROPERTIES", "VOLATILITY", "DOCUMENTATION"},
	"session_connect_attrs":                         {"PROCESSLIST_ID", "ATTR_NAME", "ATTR_VALUE", "ORDINAL_POSITION"},
	"socket_instances":                              {"EVENT_NAME", "OBJECT_INSTANCE_BEGIN", "THREAD_ID", "SOCKET_ID", "IP", "PORT", "STATE"},
	"socket_summary_by_instance":                    {"EVENT_NAME", "OBJECT_INSTANCE_BEGIN", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "COUNT_READ", "SUM_TIMER_READ", "MIN_TIMER_READ", "AVG_TIMER_READ", "MAX_TIMER_READ", "SUM_NUMBER_OF_BYTES_READ", "COUNT_WRITE", "SUM_TIMER_WRITE", "MIN_TIMER_WRITE", "AVG_TIMER_WRITE", "MAX_TIMER_WRITE", "SUM_NUMBER_OF_BYTES_WRITE", "COUNT_MISC", "SUM_TIMER_MISC", "MIN_TIMER_MISC", "AVG_TIMER_MISC", "MAX_TIMER_MISC"},
	"socket_summary_by_event_name":                  {"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "COUNT_READ", "SUM_TIMER_READ", "MIN_TIMER_READ", "AVG_TIMER_READ", "MAX_TIMER_READ", "SUM_NUMBER_OF_BYTES_READ", "COUNT_WRITE", "SUM_TIMER_WRITE", "MIN_TIMER_WRITE", "AVG_TIMER_WRITE", "MAX_TIMER_WRITE", "SUM_NUMBER_OF_BYTES_WRITE", "COUNT_MISC", "SUM_TIMER_MISC", "MIN_TIMER_MISC", "AVG_TIMER_MISC", "MAX_TIMER_MISC"},
	"file_instances":                                {"FILE_NAME", "EVENT_NAME", "OPEN_COUNT"},
	"file_summary_by_instance":                      {"FILE_NAME", "EVENT_NAME", "OBJECT_INSTANCE_BEGIN", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "COUNT_READ", "SUM_TIMER_READ", "MIN_TIMER_READ", "AVG_TIMER_READ", "MAX_TIMER_READ", "SUM_NUMBER_OF_BYTES_READ", "COUNT_WRITE", "SUM_TIMER_WRITE", "MIN_TIMER_WRITE", "AVG_TIMER_WRITE", "MAX_TIMER_WRITE", "SUM_NUMBER_OF_BYTES_WRITE", "COUNT_MISC", "SUM_TIMER_MISC", "MIN_TIMER_MISC", "AVG_TIMER_MISC", "MAX_TIMER_MISC"},
	"file_summary_by_event_name":                    {"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "COUNT_READ", "SUM_TIMER_READ", "MIN_TIMER_READ", "AVG_TIMER_READ", "MAX_TIMER_READ", "SUM_NUMBER_OF_BYTES_READ", "COUNT_WRITE", "SUM_TIMER_WRITE", "MIN_TIMER_WRITE", "AVG_TIMER_WRITE", "MAX_TIMER_WRITE", "SUM_NUMBER_OF_BYTES_WRITE", "COUNT_MISC", "SUM_TIMER_MISC", "MIN_TIMER_MISC", "AVG_TIMER_MISC", "MAX_TIMER_MISC"},
	"objects_summary_global_by_type":                {"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"setup_consumers":                               {"NAME", "ENABLED"},
	"setup_instruments":                             {"NAME", "ENABLED", "TIMED", "PROPERTIES", "FLAGS", "VOLATILITY", "DOCUMENTATION"},
	"setup_actors":                                  {"HOST", "USER", "ROLE", "ENABLED", "HISTORY"},
	"setup_objects":                                 {"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "ENABLED", "TIMED"},
	"setup_timers":                                  {"NAME", "TIMER_NAME", "TIMER_FREQUENCY", "TIMER_RESOLUTION", "TIMER_OVERHEAD"},
	"global_variables":                              {"VARIABLE_NAME", "VARIABLE_VALUE"},
	"session_variables":                             {"VARIABLE_NAME", "VARIABLE_VALUE"},
	"global_status":                                 {"VARIABLE_NAME", "VARIABLE_VALUE"},
	"session_status":                                {"VARIABLE_NAME", "VARIABLE_VALUE"},
	"users":                                         {"USER", "CURRENT_CONNECTIONS", "TOTAL_CONNECTIONS", "MAX_SESSION_CONTROLLED_MEMORY", "MAX_SESSION_TOTAL_MEMORY"},
}

// performanceSchemaTransactionEventColumns follows the MySQL 8.4 native
// definition shared by current, history, and history_long transaction event
// tables.  The executor accepts XID_FORMAT as a legacy read-only alias, but
// it is intentionally not part of SELECT * or INFORMATION_SCHEMA.COLUMNS.
var performanceSchemaTransactionEventColumns = []string{
	"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "STATE", "TRX_ID", "GTID",
	"XID_FORMAT_ID", "XID_GTRID", "XID_BQUAL", "XA_STATE", "SOURCE", "TIMER_START",
	"TIMER_END", "TIMER_WAIT", "ACCESS_MODE", "ISOLATION_LEVEL", "AUTOCOMMIT",
	"NUMBER_OF_SAVEPOINTS", "NUMBER_OF_ROLLBACK_TO_SAVEPOINT", "NUMBER_OF_RELEASE_SAVEPOINT",
	"OBJECT_INSTANCE_BEGIN", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE",
}

func performanceSchemaTableNames() []string {
	seen := make(map[string]struct{}, len(performanceSchemaTableRegistry)+len(performanceSchemaDedicatedTableColumns))
	for name := range performanceSchemaTableRegistry {
		seen[name] = struct{}{}
	}
	for name := range performanceSchemaDedicatedTableColumns {
		seen[name] = struct{}{}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func performanceSchemaTableColumns(name string) []string {
	if columns, ok := performanceSchemaDedicatedTableColumns[name]; ok {
		return columns
	}
	if columns, ok := performanceSchemaTableRegistry[name]; ok {
		return columns
	}
	return []string{"NAME"}
}

var performanceSchemaStageSummaryAccountColumns = append([]string{"USER", "HOST"}, performanceSchemaStageSummaryEventColumns...)
var performanceSchemaStageSummaryHostColumns = append([]string{"HOST"}, performanceSchemaStageSummaryEventColumns...)
var performanceSchemaStageSummaryUserColumns = append([]string{"USER"}, performanceSchemaStageSummaryEventColumns...)
var performanceSchemaStageSummaryEventColumns = []string{"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"}

var performanceSchemaStatementSummaryAccountColumns = append([]string{"USER", "HOST"}, performanceSchemaStatementSummaryEventColumns...)
var performanceSchemaStatementSummaryHostColumns = append([]string{"HOST"}, performanceSchemaStatementSummaryEventColumns...)

// MySQL exposes a distinct program-summary shape. It does not include
// EVENT_NAME; it separates the stored-program invocation aggregate from the
// aggregate of statements executed inside that program.
var performanceSchemaStatementSummaryProgramColumns = []string{
	"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT",
	"COUNT_STATEMENTS", "SUM_STATEMENTS_WAIT", "MIN_STATEMENTS_WAIT", "AVG_STATEMENTS_WAIT", "MAX_STATEMENTS_WAIT",
	"SUM_LOCK_TIME", "SUM_ERRORS", "SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED",
	"SUM_CREATED_TMP_DISK_TABLES", "SUM_CREATED_TMP_TABLES", "SUM_SELECT_FULL_JOIN", "SUM_SELECT_FULL_RANGE_JOIN",
	"SUM_SELECT_RANGE", "SUM_SELECT_RANGE_CHECK", "SUM_SELECT_SCAN", "SUM_SORT_MERGE_PASSES", "SUM_SORT_RANGE",
	"SUM_SORT_ROWS", "SUM_SORT_SCAN", "SUM_NO_INDEX_USED", "SUM_NO_GOOD_INDEX_USED", "SUM_CPU_TIME",
	"MAX_CONTROLLED_MEMORY", "MAX_TOTAL_MEMORY", "COUNT_SECONDARY",
}
var performanceSchemaStatementSummaryThreadColumns = append([]string{"THREAD_ID"}, performanceSchemaStatementSummaryEventColumns...)
var performanceSchemaStatementSummaryUserColumns = append([]string{"USER"}, performanceSchemaStatementSummaryEventColumns...)
var performanceSchemaStatementSummaryGlobalColumns = performanceSchemaStatementSummaryEventColumns
var performanceSchemaStatementSummaryEventColumns = []string{
	"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT",
	"SUM_LOCK_TIME", "SUM_ERRORS", "SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED",
	"SUM_CREATED_TMP_DISK_TABLES", "SUM_CREATED_TMP_TABLES", "SUM_SELECT_FULL_JOIN", "SUM_SELECT_FULL_RANGE_JOIN",
	"SUM_SELECT_RANGE", "SUM_SELECT_RANGE_CHECK", "SUM_SELECT_SCAN", "SUM_SORT_MERGE_PASSES", "SUM_SORT_RANGE",
	"SUM_SORT_ROWS", "SUM_SORT_SCAN", "SUM_NO_INDEX_USED", "SUM_NO_GOOD_INDEX_USED", "SUM_CPU_TIME",
	"MAX_CONTROLLED_MEMORY", "MAX_TOTAL_MEMORY", "COUNT_SECONDARY",
}

var performanceSchemaTransactionSummaryAccountColumns = append([]string{"USER", "HOST"}, performanceSchemaTransactionSummaryEventColumns...)
var performanceSchemaTransactionSummaryHostColumns = append([]string{"HOST"}, performanceSchemaTransactionSummaryEventColumns...)
var performanceSchemaTransactionSummaryThreadColumns = append([]string{"THREAD_ID"}, performanceSchemaTransactionSummaryEventColumns...)
var performanceSchemaTransactionSummaryUserColumns = append([]string{"USER"}, performanceSchemaTransactionSummaryEventColumns...)
var performanceSchemaTransactionSummaryGlobalColumns = performanceSchemaTransactionSummaryEventColumns
var performanceSchemaTransactionSummaryEventColumns = []string{
	"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT",
	"COUNT_READ_WRITE", "SUM_TIMER_READ_WRITE", "MIN_TIMER_READ_WRITE", "AVG_TIMER_READ_WRITE", "MAX_TIMER_READ_WRITE",
	"COUNT_READ_ONLY", "SUM_TIMER_READ_ONLY", "MIN_TIMER_READ_ONLY", "AVG_TIMER_READ_ONLY", "MAX_TIMER_READ_ONLY",
}

var performanceSchemaWaitSummaryAccountColumns = append([]string{"USER", "HOST"}, performanceSchemaWaitSummaryEventColumns...)
var performanceSchemaWaitSummaryHostColumns = append([]string{"HOST"}, performanceSchemaWaitSummaryEventColumns...)
var performanceSchemaWaitSummaryInstanceColumns = append([]string{"EVENT_NAME", "OBJECT_INSTANCE_BEGIN"}, performanceSchemaWaitSummaryEventColumns[1:]...)
var performanceSchemaWaitSummaryUserColumns = append([]string{"USER"}, performanceSchemaWaitSummaryEventColumns...)
var performanceSchemaWaitSummaryEventColumns = []string{"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"}

// performanceSchemaStatementEventColumns follows the MySQL statement-event
// table contract.  Values that the recorder cannot observe remain NULL in
// the row projection; they are still part of the discoverable table shape.
var performanceSchemaStatementEventColumns = []string{
	"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT",
	"LOCK_TIME", "SQL_TEXT", "DIGEST", "DIGEST_TEXT", "CURRENT_SCHEMA", "OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME",
	"OBJECT_INSTANCE_BEGIN", "MYSQL_ERRNO", "RETURNED_SQLSTATE", "MESSAGE_TEXT", "ERRORS", "WARNINGS", "ROWS_AFFECTED",
	"ROWS_SENT", "ROWS_EXAMINED", "CREATED_TMP_DISK_TABLES", "CREATED_TMP_TABLES", "SELECT_FULL_JOIN", "SELECT_FULL_RANGE_JOIN",
	"SELECT_RANGE", "SELECT_RANGE_CHECK", "SELECT_SCAN", "SORT_MERGE_PASSES", "SORT_RANGE", "SORT_ROWS", "SORT_SCAN",
	"NO_INDEX_USED", "NO_GOOD_INDEX_USED", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE", "NESTING_EVENT_LEVEL", "STATEMENT_ID",
	"CPU_TIME", "MAX_CONTROLLED_MEMORY", "MAX_TOTAL_MEMORY", "EXECUTION_ENGINE",
}

// MySQL 8.4 exposes the complete digest aggregate, including latency
// quantiles and the sampled statement metadata. Keep this separate from the
// smaller event-name summary shape because clients discover this table via
// INFORMATION_SCHEMA.COLUMNS before reading it.
var performanceSchemaStatementDigestColumns = []string{
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
}

var performanceSchemaMemorySummaryAccountColumns = append([]string{"USER", "HOST"}, performanceSchemaMemorySummaryEventColumns...)
var performanceSchemaMemorySummaryHostColumns = append([]string{"HOST"}, performanceSchemaMemorySummaryEventColumns...)
var performanceSchemaMemorySummaryUserColumns = append([]string{"USER"}, performanceSchemaMemorySummaryEventColumns...)
var performanceSchemaMemorySummaryEventColumns = []string{"EVENT_NAME", "COUNT_ALLOC", "COUNT_FREE", "SUM_NUMBER_OF_BYTES_ALLOC", "SUM_NUMBER_OF_BYTES_FREE", "LOW_COUNT_USED", "CURRENT_COUNT_USED", "HIGH_COUNT_USED", "LOW_NUMBER_OF_BYTES_USED", "CURRENT_NUMBER_OF_BYTES_USED", "HIGH_NUMBER_OF_BYTES_USED"}

func (e *XMySQLExecutor) executePerformanceSchemaRegistrySelect(query string, session server.MySQLServerSession) *SelectResult {
	name := performanceSchemaTableName(query)
	if name == "processlist" {
		return e.executePerformanceSchemaProcesslistSelect(query, session)
	}
	if name == "variables_info" {
		return e.executePerformanceSchemaVariablesInfoSelect(query)
	}
	if name == "persisted_variables" {
		return e.executePerformanceSchemaPersistedVariablesSelect(query)
	}
	if name == "error_log" {
		return e.executePerformanceSchemaErrorLogSelect(query)
	}
	if name == "tls_channel_status" {
		return e.executePerformanceSchemaTLSChannelStatusSelect(query, session)
	}
	if name == "innodb_redo_log_files" {
		return e.executePerformanceSchemaInnoDBRedoLogFilesSelect(query)
	}
	if name == "setup_loggers" || name == "setup_meters" || name == "setup_metrics" {
		return e.executePerformanceSchemaTelemetrySetupSelect(query, name)
	}
	defaults, ok := performanceSchemaTableRegistry[name]
	if !ok {
		return newInformationSchemaSelectResult("performance_schema."+name, []string{"NAME"}, nil)
	}
	columns := requestedInformationSchemaColumns(query, defaults)
	return newInformationSchemaSelectResult("performance_schema."+name, columns, nil)
}

func (e *XMySQLExecutor) executePerformanceSchemaProcesslistSelect(query string, session server.MySQLServerSession) *SelectResult {
	const table = "processlist"
	defaults := performanceSchemaTableColumns(table)
	columns := requestedInformationSchemaColumns(query, defaults)
	base := e.executeProcesslistSelect(session, query)
	rows := make([][]interface{}, 0, len(base.Records))
	for _, record := range base.Records {
		values := record.GetValues()
		if len(values) < 10 {
			continue
		}
		row := map[string]interface{}{
			"ID":               values[0].Int(),
			"USER":             values[1].String(),
			"HOST":             values[2].String(),
			"DB":               values[3].String(),
			"COMMAND":          values[4].String(),
			"TIME":             values[5].Int(),
			"STATE":            values[6].String(),
			"INFO":             values[7].String(),
			"EXECUTION_ENGINE": "PRIMARY",
			"TIME_MS":          processlistTimeMilliseconds(values[5].Int()),
			"ROWS_SENT":        values[8].Int(),
			"ROWS_EXAMINED":    values[9].Int(),
		}
		if !performanceSchemaLockValuesMatch(query, row) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, row))
	}
	return newInformationSchemaSelectResult("performance_schema."+table, columns, rows)
}

func processlistTimeMilliseconds(value interface{}) int64 {
	seconds := processlistCounter(value)
	return seconds * 1000
}

// executePerformanceSchemaInnoDBRedoLogFilesSelect exposes the live redo file
// owned by the transaction manager. The manager currently has one append-only
// file, so this intentionally reports one source-backed row and leaves the
// circular multi-file/consumer semantics outside this compatibility slice.
func (e *XMySQLExecutor) executePerformanceSchemaInnoDBRedoLogFilesSelect(query string) *SelectResult {
	const table = "innodb_redo_log_files"
	columns := requestedInformationSchemaColumns(query, performanceSchemaTableColumns(table))
	if e == nil || e.txManager == nil || e.txManager.GetRedoLogManager() == nil {
		return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
	}
	snapshot, err := e.txManager.GetRedoLogManager().GetFileSnapshot()
	if err != nil {
		return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
	}
	row := map[string]interface{}{
		"FILE_ID":        snapshot.FileID,
		"FILE_NAME":      snapshot.FileName,
		"START_LSN":      snapshot.StartLSN,
		"END_LSN":        snapshot.EndLSN,
		"SIZE_IN_BYTES":  snapshot.SizeInBytes,
		"IS_FULL":        map[bool]string{true: "YES", false: "NO"}[snapshot.IsFull],
		"CONSUMER_LEVEL": snapshot.ConsumerLevel,
	}
	if !performanceSchemaRedoLogFileQueryMatches(query, snapshot) || !performanceSchemaLockValuesMatch(query, row) {
		return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
	}
	return newInformationSchemaSelectResult("performance_schema."+table, columns, [][]interface{}{projectInformationSchemaRow(columns, row)})
}

func performanceSchemaRedoLogFileQueryMatches(query string, snapshot *manager.RedoLogFileSnapshot) bool {
	if snapshot == nil {
		return false
	}
	return performanceSchemaTLSStatusQueryMatches(query, "file_id", fmt.Sprint(snapshot.FileID)) &&
		performanceSchemaTLSStatusQueryMatches(query, "file_name", snapshot.FileName)
}

// executePerformanceSchemaTLSChannelStatusSelect exposes the TLS properties
// that are known for the current MySQL connection. The network layer records
// these values only after a real TLS handshake, so this handler never invents
// a protocol version or cipher suite. MySQL presents this table as one row per
// channel property (CHANNEL, PROPERTY, VALUE), not as a wide status row.
func (e *XMySQLExecutor) executePerformanceSchemaTLSChannelStatusSelect(query string, session server.MySQLServerSession) *SelectResult {
	const table = "tls_channel_status"
	columns := requestedInformationSchemaColumns(query, performanceSchemaTableColumns(table))
	if session == nil {
		return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
	}
	active, known := performanceSchemaTLSBool(session.GetParamByName("tls_active"))
	if !known {
		return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
	}
	properties := []struct {
		name  string
		value interface{}
	}{
		{name: "Enabled", value: map[bool]string{true: "Yes", false: "No"}[active]},
	}
	if version, ok := session.GetParamByName("tls_version").(string); ok && strings.TrimSpace(version) != "" {
		properties = append(properties, struct {
			name  string
			value interface{}
		}{name: "Current_tls_version", value: version})
	}
	if cipher, ok := session.GetParamByName("tls_cipher").(string); ok && strings.TrimSpace(cipher) != "" {
		properties = append(properties, struct {
			name  string
			value interface{}
		}{name: "Current_tls_cipher", value: cipher})
	}
	rows := make([][]interface{}, 0, len(properties))
	for _, property := range properties {
		if !performanceSchemaTLSStatusQueryMatches(query, "channel", "mysql_main") ||
			!performanceSchemaTLSStatusQueryMatches(query, "property", property.name) ||
			!performanceSchemaTLSStatusQueryMatches(query, "value", fmt.Sprint(property.value)) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, map[string]interface{}{
			"CHANNEL":  "mysql_main",
			"PROPERTY": property.name,
			"VALUE":    property.value,
		}))
	}
	return newInformationSchemaSelectResult("performance_schema."+table, columns, rows)
}

func performanceSchemaTLSBool(value interface{}) (bool, bool) {
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "on", "true", "yes":
			return true, true
		case "0", "off", "false", "no":
			return false, true
		}
	}
	return false, false
}

func performanceSchemaTLSStatusQueryMatches(query, column, value string) bool {
	pattern := regexp.MustCompile(`(?is)\b` + regexp.QuoteMeta(column) + `\s*(=|like)\s*['"]([^'"]*)['"]`)
	if match := pattern.FindStringSubmatch(query); len(match) == 3 {
		return metadataPatternMatches(value, match[2])
	}
	inPattern := regexp.MustCompile(`(?is)\b` + regexp.QuoteMeta(column) + `\s+in\s*\(([^)]*)\)`)
	if match := inPattern.FindStringSubmatch(query); len(match) == 2 {
		for _, candidate := range splitTopLevelComma(match[1]) {
			candidate = strings.Trim(strings.TrimSpace(candidate), "'\"")
			if strings.EqualFold(candidate, value) {
				return true
			}
		}
		return false
	}
	return true
}

func (e *XMySQLExecutor) executePerformanceSchemaVariablesInfoSelect(query string) *SelectResult {
	const table = "variables_info"
	columns := requestedInformationSchemaColumns(query, performanceSchemaTableColumns(table))
	if e == nil || e.storageManager == nil || e.storageManager.GetSystemVariablesManager() == nil {
		return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
	}
	values := e.storageManager.GetSystemVariablesManager().ListVariables("", manager.GlobalScope)
	persisted, _ := e.readPersistedSystemVariables()
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	rows := make([][]interface{}, 0, len(names))
	for _, name := range names {
		if len(filterPerformanceSchemaVariableRows(query, [][]interface{}{{name}})) == 0 {
			continue
		}
		row := map[string]interface{}{
			"VARIABLE_NAME":   name,
			"VARIABLE_SOURCE": "COMPILED",
			"VARIABLE_PATH":   nil,
			"MIN_VALUE":       nil,
			"MAX_VALUE":       nil,
			"SET_TIME":        nil,
			"SET_USER":        nil,
			"SET_HOST":        nil,
		}
		if value, ok := persisted[name]; ok {
			row["VARIABLE_SOURCE"] = "PERSISTED"
			row["VARIABLE_PATH"] = e.persistedVariablesPath()
			row["SET_TIME"] = value.SetTime
			row["SET_USER"] = value.SetUser
			row["SET_HOST"] = value.SetHost
		}
		if !performanceSchemaLockValuesMatch(query, row) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, row))
	}
	return newInformationSchemaSelectResult("performance_schema."+table, columns, rows)
}

func (e *XMySQLExecutor) executePerformanceSchemaErrorLogSelect(query string) *SelectResult {
	const table = "error_log"
	columns := requestedInformationSchemaColumns(query, performanceSchemaTableColumns(table))
	if e == nil || e.metricsRecorder == nil {
		return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
	}
	rows := make([][]interface{}, 0)
	for _, event := range e.metricsRecorder.ErrorLog() {
		if !performanceSchemaSummaryFilterMatches(query, "error_code", event.ErrorCode) ||
			!performanceSchemaSummaryFilterMatches(query, "subsystem", event.Subsystem) ||
			!performanceSchemaSummaryFilterMatches(query, "prio", errorLogPriorityLabel(event.Priority)) &&
				!performanceSchemaSummaryFilterMatches(query, "prio", fmt.Sprint(event.Priority)) {
			continue
		}
		values := map[string]interface{}{
			"LOGGED":     event.Logged,
			"THREAD_ID":  event.ThreadID,
			"PRIO":       errorLogPriorityLabel(event.Priority),
			"ERROR_CODE": event.ErrorCode,
			"SUBSYSTEM":  event.Subsystem,
			"DATA": replicationStatusJSON(map[string]interface{}{
				"database": event.Database, "error_class": event.ErrorClass,
			}),
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("performance_schema."+table, columns, rows)
}

func errorLogPriorityLabel(priority int64) string {
	switch priority {
	case 1:
		return "System"
	case 2:
		return "Error"
	case 3:
		return "Warning"
	case 4:
		return "Note"
	default:
		return "Warning"
	}
}

// executePerformanceSchemaReplicationSelect projects the live replication
// runtime into the P_S replication views. Tables whose semantics require a
// separate MySQL group-replication, filter, or worker runtime remain empty;
// returning a fabricated worker/member row would be worse than exposing the
// absence of that runtime.
func (e *XMySQLExecutor) executePerformanceSchemaReplicationSelect(query, table string) *SelectResult {
	defaults := performanceSchemaTableColumns(table)
	columns := requestedInformationSchemaColumns(query, defaults)
	status := replication.StatusSnapshot{}
	if e != nil && e.replicationStatus != nil {
		status = e.replicationStatus()
	}
	serviceState := "OFF"
	if status.ReplicaRunning {
		serviceState = "ON"
	}
	connectionErrorNumber, connectionErrorMessage, connectionErrorAt := status.LastIOErrorNumber, status.LastIOError, status.LastIOErrorAt
	if connectionErrorNumber == 0 && strings.TrimSpace(connectionErrorMessage) == "" {
		connectionErrorNumber, connectionErrorMessage, connectionErrorAt = status.LastErrorNumber, status.LastError, status.LastErrorAt
	}
	applierErrorNumber, applierErrorMessage, applierErrorAt := status.LastSQLErrorNumber, status.LastSQLError, status.LastSQLErrorAt
	if applierErrorNumber == 0 && strings.TrimSpace(applierErrorMessage) == "" {
		applierErrorNumber, applierErrorMessage, applierErrorAt = status.LastErrorNumber, status.LastError, status.LastErrorAt
	}

	// Group-replication views have no corresponding runtime in xmysql-server.
	// Filter views are backed by the replica's persisted CHANGE REPLICATION
	// FILTER configuration below.
	switch table {
	case "replication_group_communication_information", "replication_group_configuration_version",
		"replication_group_member_actions", "replication_group_member_stats", "replication_group_members":
		return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
	}
	if table == "replication_applier_filters" || table == "replication_applier_global_filters" {
		rows := make([][]interface{}, 0, len(status.ReplicationFilters))
		for _, filter := range status.ReplicationFilters {
			values := map[string]interface{}{
				"CHANNEL_NAME":  "",
				"FILTER_NAME":   filter.Name,
				"FILTER_RULE":   filter.Rule,
				"CONFIGURED_BY": filter.ConfiguredBy,
				"ACTIVE_SINCE":  nil,
				"COUNTER":       int64(filter.Counter),
			}
			if !filter.ActiveSince.IsZero() {
				values["ACTIVE_SINCE"] = filter.ActiveSince
			}
			if table == "replication_applier_global_filters" {
				delete(values, "CHANNEL_NAME")
			}
			if performanceSchemaReplicationValuesMatch(query, values) {
				rows = append(rows, projectInformationSchemaRow(columns, values))
			}
		}
		return newInformationSchemaSelectResult("performance_schema."+table, columns, rows)
	}

	// The xmysql applier is intentionally single-threaded, but it is still a
	// real coordinator/worker pair. Expose that pair from the same authoritative
	// replica status snapshot instead of leaving the views empty or inventing
	// group-replication members. Thread IDs remain NULL because the replication
	// runtime does not expose process-level thread identities.
	if table == "replication_applier_status_by_coordinator" || table == "replication_applier_status_by_worker" {
		if status.Role != replication.RoleReplica || strings.TrimSpace(status.SourceURL) == "" {
			return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
		}
		values := map[string]interface{}{
			"CHANNEL_NAME":         "",
			"SERVICE_STATE":        serviceState,
			"LAST_ERROR_NUMBER":    applierErrorNumber,
			"LAST_ERROR_MESSAGE":   nil,
			"LAST_ERROR_TIMESTAMP": nil,
		}
		if strings.TrimSpace(applierErrorMessage) != "" {
			values["LAST_ERROR_MESSAGE"] = applierErrorMessage
		}
		if !applierErrorAt.IsZero() {
			values["LAST_ERROR_TIMESTAMP"] = applierErrorAt
		}
		if table == "replication_applier_status_by_coordinator" {
			values["THREAD_ID"] = nil
		} else {
			values["WORKER_ID"] = int64(1)
			values["THREAD_ID"] = nil
		}
		if !performanceSchemaReplicationValuesMatch(query, values) {
			return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
		}
		return newInformationSchemaSelectResult("performance_schema."+table, columns, [][]interface{}{projectInformationSchemaRow(columns, values)})
	}

	var sourceUUID interface{}
	if strings.TrimSpace(status.SourceUUID) != "" {
		sourceUUID = status.SourceUUID
	}
	values := map[string]interface{}{
		"CHANNEL_NAME":                                 "",
		"SERVICE_STATE":                                serviceState,
		"REMAINING_DELAY":                              nil,
		"RECEIVED_TRANSACTION_SET":                     status.ExecutedGTIDs,
		"APPLIED_TRANSACTION_SET":                      status.ExecutedGTIDs,
		"COUNT_TRANSACTIONS_RETRIES":                   int64(0),
		"HOST":                                         status.SourceURL,
		"AUTO_POSITION":                                "1",
		"DESIRED_DELAY":                                int64(0),
		"PRIVILEGE_CHECKS_USER":                        nil,
		"REQUIRE_ROW_FORMAT":                           "NO",
		"REQUIRE_TABLE_PRIMARY_KEY_CHECK":              "STREAM",
		"ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS_TYPE":  "OFF",
		"ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS_VALUE": nil,
		"GROUP_NAME":                                   nil,
		"SOURCE_UUID":                                  sourceUUID,
		"THREAD_ID":                                    nil,
		"COUNT_RECEIVED_HEARTBEATS":                    int64(status.ReceivedHeartbeats),
		"LAST_HEARTBEAT_TIMESTAMP":                     nil,
		"LAST_ERROR_NUMBER":                            connectionErrorNumber,
		"LAST_ERROR_MESSAGE":                           nil,
		"LAST_ERROR_TIMESTAMP":                         nil,
		"LAST_QUEUED_TRANSACTION":                      nil,
		"LAST_QUEUED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP":  nil,
		"LAST_QUEUED_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP": nil,
		"LAST_QUEUED_TRANSACTION_START_QUEUE_TIMESTAMP":      nil,
		"LAST_QUEUED_TRANSACTION_END_QUEUE_TIMESTAMP":        nil,
		"QUEUEING_TRANSACTION":                               nil,
		"QUEUEING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP":     nil,
		"QUEUEING_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP":    nil,
		"QUEUEING_TRANSACTION_START_QUEUE_TIMESTAMP":         nil,
	}
	if !status.LastHeartbeatAt.IsZero() {
		values["LAST_HEARTBEAT_TIMESTAMP"] = status.LastHeartbeatAt
	}
	if strings.TrimSpace(connectionErrorMessage) != "" {
		values["LAST_ERROR_MESSAGE"] = connectionErrorMessage
	}
	if !connectionErrorAt.IsZero() {
		values["LAST_ERROR_TIMESTAMP"] = connectionErrorAt
	}

	switch table {
	case "replication_connection_configuration":
		if strings.TrimSpace(status.SourceURL) == "" {
			return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
		}
		parsed, parseErr := url.Parse(strings.TrimSpace(status.SourceURL))
		if parseErr == nil && strings.EqualFold(parsed.Scheme, "mysql") && parsed.Hostname() != "" {
			values["HOST"] = parsed.Hostname()
			if parsedPort := parsed.Port(); parsedPort != "" {
				if port, err := strconv.ParseInt(parsedPort, 10, 64); err == nil {
					values["PORT"] = port
				}
			}
			values["USER"] = status.SourceUser
			values["AUTO_POSITION"] = boolToMySQL(status.SourceAutoPosition)
			values["SSL_ALLOWED"] = "NO"
			queryValues := parsed.Query()
			if rawSSL := strings.TrimSpace(queryValues.Get("ssl")); rawSSL != "" {
				if enabled, parseErr := strconv.ParseBool(rawSSL); parseErr == nil && enabled {
					values["SSL_ALLOWED"] = "YES"
				}
			}
			if rawVerify := strings.TrimSpace(queryValues.Get("ssl_verify_server_cert")); rawVerify != "" {
				if verify, parseErr := strconv.ParseBool(rawVerify); parseErr == nil {
					values["SSL_VERIFY_SERVER_CERTIFICATE"] = boolToMySQL(verify)
				}
			}
			values["SSL_CA_FILE"] = nullableInformationSchemaString(queryValues.Get("ssl_ca"))
			values["SSL_CERTIFICATE"] = nullableInformationSchemaString(queryValues.Get("ssl_cert"))
			values["SSL_KEY"] = nullableInformationSchemaString(queryValues.Get("ssl_key"))
			if rawRetry := strings.TrimSpace(queryValues.Get("connect_retry")); rawRetry != "" {
				if retry, parseErr := strconv.ParseInt(rawRetry, 10, 64); parseErr == nil {
					values["CONNECTION_RETRY_INTERVAL"] = retry
				}
			}
			if rawRetryCount := strings.TrimSpace(queryValues.Get("connect_retry_count")); rawRetryCount != "" {
				if retryCount, parseErr := strconv.ParseInt(rawRetryCount, 10, 64); parseErr == nil {
					values["CONNECTION_RETRY_COUNT"] = retryCount
				}
			}
			if rawHeartbeat := strings.TrimSpace(queryValues.Get("heartbeat_interval")); rawHeartbeat != "" {
				if heartbeat, parseErr := strconv.ParseFloat(rawHeartbeat, 64); parseErr == nil {
					values["HEARTBEAT_INTERVAL"] = heartbeat
				}
			}
			values["COMPRESSION_ALGORITHM"] = nullableInformationSchemaString(queryValues.Get("compression_algorithm"))
			if rawZstd := strings.TrimSpace(queryValues.Get("zstd_compression_level")); rawZstd != "" {
				if level, parseErr := strconv.ParseInt(rawZstd, 10, 64); parseErr == nil {
					values["ZSTD_COMPRESSION_LEVEL"] = level
				}
			}
		} else {
			// The HTTP source is an xmysql-specific control-plane endpoint, so
			// preserve its configured endpoint string for the existing contract.
			values["HOST"] = status.SourceURL
			values["AUTO_POSITION"] = "1"
		}
	case "replication_asynchronous_connection_failover":
		parsed, err := url.Parse(strings.TrimSpace(status.SourceURL))
		if err != nil || parsed.Hostname() == "" {
			return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
		}
		values["HOST"] = parsed.Hostname()
		if port := parsed.Port(); port != "" {
			if parsedPort, parseErr := strconv.ParseInt(port, 10, 64); parseErr == nil {
				values["PORT"] = strconv.FormatInt(parsedPort, 10)
			}
		}
		values["WEIGHT"] = int64(0)
		values["MANAGED_NAME"] = ""
	case "replication_asynchronous_connection_failover_managed":
		// xmysql has no Group Replication managed-source runtime. Do not mirror
		// a plain source endpoint into the managed table.
		return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
	case "log_status":
		if status.UUID == "" && status.Role == "" && status.ExecutedGTIDs == "" {
			return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
		}
		binaryLogFile := ""
		binaryLogPosition := uint64(0)
		if e != nil && e.replicationSource != nil {
			if source := e.replicationSource(); source != nil {
				binaryLogFile, binaryLogPosition = source.NativeCurrentFilePosition()
			}
		}
		values = map[string]interface{}{
			"SERVER_UUID": status.UUID,
			"LOCAL": replicationStatusJSON(map[string]interface{}{
				"binary_log_file":     binaryLogFile,
				"binary_log_position": binaryLogPosition,
				"gtid_executed":       status.ExecutedGTIDs,
			}),
			// MySQL exposes REPLICATION as a JSON object containing a channels
			// array. The current runtime does not retain local relay-log
			// file/position metadata, so keep the official empty-channel shape
			// instead of inventing channel values.
			"REPLICATION": replicationStatusJSON(map[string]interface{}{
				"channels": []interface{}{},
			}),
			"STORAGE_ENGINES": replicationStatusJSON(map[string]interface{}{}),
		}
	}
	if status.Role == "" && status.SourceURL == "" && status.ExecutedGTIDs == "" {
		return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
	}
	if !performanceSchemaReplicationValuesMatch(query, values) {
		return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
	}
	return newInformationSchemaSelectResult("performance_schema."+table, columns, [][]interface{}{projectInformationSchemaRow(columns, values)})
}

func performanceSchemaReplicationValuesMatch(query string, values map[string]interface{}) bool {
	return performanceSchemaLockValuesMatch(query, values)
}

func replicationStatusJSON(values interface{}) string {
	payload, err := json.Marshal(values)
	if err != nil {
		return "{}"
	}
	return string(payload)
}

func (e *XMySQLExecutor) performanceSchemaLiveSessions(current server.MySQLServerSession) []server.MySQLServerSession {
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
	return sessions
}

func performanceSchemaSessionIdentity(session server.MySQLServerSession) (threadID int64, user, host string) {
	if session == nil {
		return 0, "", ""
	}
	threadID = int64Param(session.GetParamByName("connection_id"))
	if threadID == 0 && session.SessionContext() != nil {
		threadID = int64(session.SessionContext().GetConnectionID())
	}
	user, _ = session.GetParamByName("user").(string)
	host, _ = session.GetParamByName("host").(string)
	if user == "" && session.SessionContext() != nil {
		user = session.SessionContext().GetUsername()
	}
	if host == "" && session.SessionContext() != nil {
		host = session.SessionContext().GetHost()
	}
	if host == "" {
		host = "localhost"
	}
	return threadID, user, host
}

func performanceSchemaSessionVariableValues(session server.MySQLServerSession) map[string]interface{} {
	values := map[string]interface{}{
		"autocommit":               "ON",
		"character_set_client":     "utf8mb4",
		"character_set_connection": "utf8mb4",
		"character_set_results":    "utf8mb4",
		"collation_connection":     "utf8mb4_0900_ai_ci",
		"transaction_isolation":    "REPEATABLE-READ",
		"tx_isolation":             "REPEATABLE-READ",
		"time_zone":                "SYSTEM",
		"sql_mode":                 "",
		"max_allowed_packet":       "67108864",
	}
	if session == nil {
		return values
	}
	for name := range values {
		if value := session.GetParamByName(name); value != nil {
			values[name] = value
		}
	}
	return values
}

func performanceSchemaSessionVariableRows(session server.MySQLServerSession) [][]interface{} {
	values := performanceSchemaSessionVariableValues(session)
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	rows := make([][]interface{}, 0, len(names))
	for _, name := range names {
		rows = append(rows, []interface{}{name, values[name]})
	}
	return rows
}

// executePerformanceSchemaSessionRuntimeRegistrySelect supplies live rows for
// the P_S views whose keys are session identity rather than statement events.
// It intentionally uses the same session snapshot as PROCESSLIST/THREADS so a
// disconnect cannot leave a stale user or host row behind.
func (e *XMySQLExecutor) executePerformanceSchemaSessionRuntimeRegistrySelect(query, table string, current server.MySQLServerSession) *SelectResult {
	// status_by_account/host/user are summary tables, not live-session views.
	// Keep their historical identity aggregates after a connection closes while
	// continuing to derive Threads_connected/Threads_running from live sessions.
	// The other tables in this handler (status_by_thread and variable views)
	// remain session-scoped and use the live snapshot below.
	if table == "status_by_account" || table == "status_by_host" || table == "status_by_user" {
		return e.executePerformanceSchemaStatusRegistrySummarySelect(query, table, current)
	}
	columns := requestedInformationSchemaColumns(query, performanceSchemaTableColumns(table))
	rows := make([][]interface{}, 0)
	type statusAggregate struct {
		user, host   string
		statusValues map[string]int64
	}
	aggregates := make(map[string]*statusAggregate)
	for _, session := range e.performanceSchemaLiveSessions(current) {
		if session == nil {
			continue
		}
		threadID, user, host := performanceSchemaSessionIdentity(session)
		if table == "user_variables_by_thread" {
			variables, _ := session.GetParamByName("user_variables").(map[string]interface{})
			keys := make([]string, 0, len(variables))
			for name := range variables {
				keys = append(keys, name)
			}
			sort.Strings(keys)
			for _, name := range keys {
				if !performanceSchemaSummaryFilterMatches(query, "thread_id", fmt.Sprint(threadID)) ||
					!performanceSchemaSummaryFilterMatches(query, "variable_name", name) {
					continue
				}
				values := map[string]interface{}{
					"THREAD_ID": threadID, "VARIABLE_NAME": name, "VARIABLE_VALUE": variables[name],
				}
				if performanceSchemaLockValuesMatch(query, values) {
					rows = append(rows, projectInformationSchemaRow(columns, values))
				}
			}
			continue
		}
		if table == "variables_by_thread" {
			for _, row := range performanceSchemaSessionVariableRows(session) {
				if !performanceSchemaSummaryFilterMatches(query, "thread_id", fmt.Sprint(threadID)) ||
					!performanceSchemaSummaryFilterMatches(query, "variable_name", fmt.Sprint(row[0])) {
					continue
				}
				values := map[string]interface{}{
					"THREAD_ID": threadID, "VARIABLE_NAME": row[0], "VARIABLE_VALUE": row[1],
				}
				if performanceSchemaLockValuesMatch(query, values) {
					rows = append(rows, projectInformationSchemaRow(columns, values))
				}
			}
			continue
		}
		statusValues := map[string]interface{}{
			"Threads_connected": int64(1),
			"Threads_running":   int64(0),
			"Queries":           int64(0),
		}
		if e != nil && e.metricsRecorder != nil {
			for _, event := range e.metricsRecorder.StatementSummary() {
				if event.ThreadID == threadID {
					statusValues["Queries"] = statusValues["Queries"].(int64) + event.Count
					if variableName := performanceSchemaCommandStatusVariable(event.StatementType); variableName != "" {
						current := int64(0)
						if raw := statusValues[variableName]; raw != nil {
							current, _ = raw.(int64)
						}
						statusValues[variableName] = current + event.Count
					}
				}
			}
		}
		for _, event := range e.activeStatementEvents() {
			if event.ThreadID == threadID {
				statusValues["Threads_running"] = int64(1)
				break
			}
		}
		if table == "status_by_account" || table == "status_by_host" || table == "status_by_user" {
			if (table == "status_by_account" && (!performanceSchemaSummaryFilterMatches(query, "user", user) || !performanceSchemaSummaryFilterMatches(query, "host", host))) ||
				(table == "status_by_host" && !performanceSchemaSummaryFilterMatches(query, "host", host)) ||
				(table == "status_by_user" && !performanceSchemaSummaryFilterMatches(query, "user", user)) {
				continue
			}
			key := user
			switch table {
			case "status_by_account":
				key = user + "\x00" + host
			case "status_by_host":
				key = host
			}
			aggregate := aggregates[key]
			if aggregate == nil {
				aggregate = &statusAggregate{user: user, host: host, statusValues: make(map[string]int64)}
				aggregates[key] = aggregate
			}
			for name, value := range statusValues {
				aggregate.statusValues[name] += value.(int64)
			}
			continue
		}
		for name, value := range statusValues {
			if !performanceSchemaSummaryFilterMatches(query, "thread_id", fmt.Sprint(threadID)) ||
				!performanceSchemaSummaryFilterMatches(query, "variable_name", name) {
				continue
			}
			switch table {
			case "status_by_account":
				if !performanceSchemaSummaryFilterMatches(query, "user", user) || !performanceSchemaSummaryFilterMatches(query, "host", host) {
					continue
				}
			case "status_by_host":
				if !performanceSchemaSummaryFilterMatches(query, "host", host) {
					continue
				}
			case "status_by_user":
				if !performanceSchemaSummaryFilterMatches(query, "user", user) {
					continue
				}
			}
			values := map[string]interface{}{
				"USER": user, "HOST": host, "THREAD_ID": threadID,
				"VARIABLE_NAME": name, "VARIABLE_VALUE": value,
			}
			if performanceSchemaLockValuesMatch(query, values) {
				rows = append(rows, projectInformationSchemaRow(columns, values))
			}
		}
	}
	if len(aggregates) > 0 {
		keys := make([]string, 0, len(aggregates))
		for key := range aggregates {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			aggregate := aggregates[key]
			for name, value := range aggregate.statusValues {
				if !performanceSchemaSummaryFilterMatches(query, "variable_name", name) {
					continue
				}
				values := map[string]interface{}{
					"USER": aggregate.user, "HOST": aggregate.host,
					"VARIABLE_NAME": name, "VARIABLE_VALUE": value,
				}
				if performanceSchemaLockValuesMatch(query, values) {
					rows = append(rows, projectInformationSchemaRow(columns, values))
				}
			}
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		return fmt.Sprint(rows[i]) < fmt.Sprint(rows[j])
	})
	return newInformationSchemaSelectResult("performance_schema."+table, columns, rows)
}

// executePerformanceSchemaStatusRegistrySummarySelect projects the
// account/host/user status summary tables from the recorder's lifetime
// aggregates. These rows must survive disconnects; only the connection and
// running-thread gauges are live values.
func (e *XMySQLExecutor) executePerformanceSchemaStatusRegistrySummarySelect(query, table string, current server.MySQLServerSession) *SelectResult {
	columns := requestedInformationSchemaColumns(query, performanceSchemaTableColumns(table))
	type statusAggregate struct {
		user, host string
		status     map[string]int64
	}
	aggregates := make(map[string]*statusAggregate)
	keyFor := func(user, host string) string {
		switch table {
		case "status_by_account":
			return user + "\x00" + host
		case "status_by_host":
			return host
		default:
			return user
		}
	}
	ensure := func(user, host string) *statusAggregate {
		key := keyFor(user, host)
		aggregate := aggregates[key]
		if aggregate == nil {
			aggregate = &statusAggregate{user: user, host: host, status: make(map[string]int64)}
			aggregates[key] = aggregate
		}
		return aggregate
	}

	connected := make(map[string]int64)
	running := make(map[string]int64)
	for _, session := range e.performanceSchemaLiveSessions(current) {
		if session == nil {
			continue
		}
		_, user, host := performanceSchemaSessionIdentity(session)
		key := keyFor(user, host)
		ensure(user, host)
		connected[key]++
	}
	for _, event := range e.activeStatementEvents() {
		key := keyFor(event.User, event.Host)
		ensure(event.User, event.Host)
		running[key] = 1
	}

	if e != nil && e.metricsRecorder != nil {
		for _, total := range e.metricsRecorder.ConnectionTotals() {
			ensure(total.User, total.Host)
		}
		dimension := "account"
		switch table {
		case "status_by_host":
			dimension = "host"
		case "status_by_user":
			dimension = "user"
		}
		for _, summary := range e.metricsRecorder.StatementSummaryForDimension(dimension) {
			aggregate := ensure(summary.User, summary.Host)
			aggregate.status["Queries"] += summary.Count
			if variable := performanceSchemaCommandStatusVariable(summary.StatementType); variable != "" {
				aggregate.status[variable] += summary.Count
			}
		}
	}

	keys := make([]string, 0, len(aggregates))
	for key := range aggregates {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([][]interface{}, 0)
	for _, key := range keys {
		aggregate := aggregates[key]
		if aggregate == nil {
			continue
		}
		aggregate.status["Threads_connected"] = connected[key]
		aggregate.status["Threads_running"] = running[key]
		names := make([]string, 0, len(aggregate.status))
		for name := range aggregate.status {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if !performanceSchemaSummaryFilterMatches(query, "variable_name", name) {
				continue
			}
			values := map[string]interface{}{
				"USER": aggregate.user, "HOST": aggregate.host,
				"VARIABLE_NAME": name, "VARIABLE_VALUE": aggregate.status[name],
			}
			if table == "status_by_account" && (!performanceSchemaSummaryFilterMatches(query, "user", aggregate.user) || !performanceSchemaSummaryFilterMatches(query, "host", aggregate.host)) {
				continue
			}
			if table == "status_by_host" && !performanceSchemaSummaryFilterMatches(query, "host", aggregate.host) {
				continue
			}
			if table == "status_by_user" && !performanceSchemaSummaryFilterMatches(query, "user", aggregate.user) {
				continue
			}
			if performanceSchemaLockValuesMatch(query, values) {
				rows = append(rows, projectInformationSchemaRow(columns, values))
			}
		}
	}
	return newInformationSchemaSelectResult("performance_schema."+table, columns, rows)
}

// executePerformanceSchemaPreparedStatementsSelect exposes the live prepared
// statement inventory owned by the current client session. MySQL's
// prepared_statements_instances is session-scoped in practice for these
// protocol-created statements, so the current session is the authoritative
// source and no stale rows are retained after COM_STMT_CLOSE.
func (e *XMySQLExecutor) executePerformanceSchemaPreparedStatementsSelect(query string, current server.MySQLServerSession) *SelectResult {
	table := "prepared_statements_instances"
	columns := requestedInformationSchemaColumns(query, performanceSchemaTableColumns(table))
	if current == nil {
		return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
	}
	mgr, ok := current.GetParamByName("prepared_stmt_mgr").(interface {
		Snapshot() []compatibility.PreparedStatementSnapshot
	})
	snapshots := make([]compatibility.PreparedStatementSnapshot, 0)
	if ok && mgr != nil {
		snapshots = append(snapshots, mgr.Snapshot()...)
	}
	if sqlManager, sqlOK := current.GetParamByName(sqlPreparedStatementSessionParam).(interface {
		Snapshot() []compatibility.PreparedStatementSnapshot
	}); sqlOK && sqlManager != nil {
		snapshots = append(snapshots, sqlManager.Snapshot()...)
	}
	if len(snapshots) == 0 {
		return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
	}
	threadID, _, _ := performanceSchemaSessionIdentity(current)
	rows := make([][]interface{}, 0)
	for _, stmt := range snapshots {
		timerPrepare := int64(0)
		if !stmt.CreatedAt.IsZero() {
			elapsed := time.Since(stmt.CreatedAt)
			if elapsed < 0 {
				elapsed = 0
			}
			timerPrepare = elapsed.Nanoseconds() * 1000
		}
		sumTimerExecute := stmt.ExecuteTimeTotal.Nanoseconds() * 1000
		minTimerExecute := stmt.ExecuteTimeMin.Nanoseconds() * 1000
		maxTimerExecute := stmt.ExecuteTimeMax.Nanoseconds() * 1000
		avgTimerExecute := int64(0)
		if stmt.ExecuteCount > 0 {
			avgTimerExecute = sumTimerExecute / int64(stmt.ExecuteCount)
		}
		statementName := interface{}(nil)
		if strings.TrimSpace(stmt.Name) != "" {
			statementName = stmt.Name
		}
		values := map[string]interface{}{
			"OBJECT_INSTANCE_BEGIN":       int64(stmt.ID),
			"STATEMENT_ID":                int64(stmt.ID),
			"STATEMENT_NAME":              statementName,
			"SQL_TEXT":                    stmt.SQL,
			"OWNER_THREAD_ID":             threadID,
			"OWNER_EVENT_ID":              int64(0),
			"OWNER_OBJECT_TYPE":           nil,
			"OWNER_OBJECT_SCHEMA":         nil,
			"OWNER_OBJECT_NAME":           nil,
			"EXECUTION_ENGINE":            "PRIMARY",
			"TIMER_PREPARE":               timerPrepare,
			"COUNT_REPREPARE":             int64(0),
			"COUNT_EXECUTE":               int64(stmt.ExecuteCount),
			"SUM_TIMER_EXECUTE":           sumTimerExecute,
			"MIN_TIMER_EXECUTE":           minTimerExecute,
			"AVG_TIMER_EXECUTE":           avgTimerExecute,
			"MAX_TIMER_EXECUTE":           maxTimerExecute,
			"SUM_LOCK_TIME":               int64(0),
			"SUM_ERRORS":                  int64(stmt.ErrorCount),
			"SUM_WARNINGS":                int64(stmt.WarningCount),
			"SUM_ROWS_AFFECTED":           int64(stmt.RowsAffected),
			"SUM_ROWS_SENT":               int64(stmt.RowsSent),
			"SUM_ROWS_EXAMINED":           int64(stmt.RowsExamined),
			"SUM_CREATED_TMP_DISK_TABLES": int64(0),
			"SUM_CREATED_TMP_TABLES":      int64(0),
			"SUM_SELECT_FULL_JOIN":        int64(0),
			"SUM_SELECT_FULL_RANGE_JOIN":  int64(0),
			"SUM_SELECT_RANGE":            int64(0),
			"SUM_SELECT_RANGE_CHECK":      int64(0),
			"SUM_SELECT_SCAN":             int64(0),
			"SUM_SORT_MERGE_PASSES":       int64(0),
			"SUM_SORT_RANGE":              int64(0),
			"SUM_SORT_ROWS":               int64(0),
			"SUM_SORT_SCAN":               int64(0),
			"SUM_NO_INDEX_USED":           int64(0),
			"SUM_NO_GOOD_INDEX_USED":      int64(0),
			"SUM_CPU_TIME":                int64(0),
			"MAX_CONTROLLED_MEMORY":       int64(0),
			"MAX_TOTAL_MEMORY":            int64(0),
			"COUNT_SECONDARY":             int64(0),
		}
		if !performanceSchemaSummaryFilterMatches(query, "statement_id", fmt.Sprint(stmt.ID)) ||
			!performanceSchemaSummaryFilterMatches(query, "owner_thread_id", fmt.Sprint(threadID)) ||
			!performanceSchemaSummaryFilterMatches(query, "sql_text", stmt.SQL) ||
			!performanceSchemaSummaryFilterMatches(query, "statement_name", stmt.Name) {
			continue
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("performance_schema."+table, columns, rows)
}

// executePerformanceSchemaErrorSummarySelect exposes the global error
// aggregate backed by the execution recorder. Identity-specific rows are
// emitted only for errors whose execution path captured a client identity.
func (e *XMySQLExecutor) executePerformanceSchemaErrorSummarySelect(query, table string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, performanceSchemaTableColumns(table))
	if e == nil || e.metricsRecorder == nil {
		return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
	}
	rows := make([][]interface{}, 0)
	if table == "events_errors_summary_global_by_error" {
		var unknown *performanceSchemaUnknownErrorSummary
		for _, summary := range e.metricsRecorder.ErrorSummary() {
			errorNumber, errorName, sqlState, known := performanceSchemaErrorMetadata(summary.ErrorName)
			if !known {
				unknown = mergePerformanceSchemaUnknownErrorSummary(unknown, summary.ErrorCount, summary.HandledCount, summary.FirstSeen, summary.LastSeen)
				continue
			}
			if (!performanceSchemaSummaryFilterMatches(query, "error_name", summary.ErrorName) &&
				!performanceSchemaSummaryFilterMatches(query, "error_name", errorName)) ||
				!performanceSchemaSummaryFilterMatches(query, "error_number", fmt.Sprint(errorNumber)) ||
				!performanceSchemaSummaryFilterMatches(query, "sql_state", sqlState) {
				continue
			}
			values := map[string]interface{}{
				"ERROR_NUMBER":      errorNumber,
				"ERROR_NAME":        errorName,
				"SQL_STATE":         sqlState,
				"SUM_ERROR_RAISED":  summary.ErrorCount,
				"SUM_ERROR_HANDLED": summary.HandledCount,
				"FIRST_SEEN":        summary.FirstSeen,
				"LAST_SEEN":         summary.LastSeen,
			}
			if !performanceSchemaLockValuesMatch(query, values) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
		if unknown != nil && performanceSchemaSummaryFilterMatches(query, "error_number", "0") &&
			performanceSchemaSummaryFilterMatches(query, "error_name", "") &&
			performanceSchemaSummaryFilterMatches(query, "sql_state", "") {
			values := map[string]interface{}{
				"ERROR_NUMBER":      int64(0),
				"ERROR_NAME":        nil,
				"SQL_STATE":         nil,
				"SUM_ERROR_RAISED":  unknown.ErrorCount,
				"SUM_ERROR_HANDLED": unknown.HandledCount,
				"FIRST_SEEN":        unknown.FirstSeen,
				"LAST_SEEN":         unknown.LastSeen,
			}
			if performanceSchemaLockValuesMatch(query, values) {
				rows = append(rows, projectInformationSchemaRow(columns, values))
			}
		}
		return newInformationSchemaSelectResult("performance_schema."+table, columns, rows)
	}
	unknownByKey := make(map[string]*performanceSchemaUnknownErrorSummary)
	dimension := "account"
	switch table {
	case "events_errors_summary_by_host_by_error":
		dimension = "host"
	case "events_errors_summary_by_thread_by_error":
		dimension = "thread"
	case "events_errors_summary_by_user_by_error":
		dimension = "user"
	}
	for _, summary := range e.metricsRecorder.ErrorSummaryByDimension(dimension) {
		var identityKey string
		switch table {
		case "events_errors_summary_by_account_by_error":
			identityKey = summary.User + "\x00" + summary.Host
			if !performanceSchemaSummaryFilterMatches(query, "user", summary.User) || !performanceSchemaSummaryFilterMatches(query, "host", summary.Host) {
				continue
			}
		case "events_errors_summary_by_host_by_error":
			identityKey = summary.Host
			if !performanceSchemaSummaryFilterMatches(query, "host", summary.Host) {
				continue
			}
		case "events_errors_summary_by_thread_by_error":
			identityKey = fmt.Sprint(summary.ThreadID)
			if !performanceSchemaSummaryFilterMatches(query, "thread_id", fmt.Sprint(summary.ThreadID)) {
				continue
			}
		case "events_errors_summary_by_user_by_error":
			identityKey = summary.User
			if !performanceSchemaSummaryFilterMatches(query, "user", summary.User) {
				continue
			}
		}
		errorNumber, errorName, sqlState, known := performanceSchemaErrorMetadata(summary.ErrorName)
		if !known {
			unknownByKey[identityKey] = mergePerformanceSchemaUnknownErrorSummary(unknownByKey[identityKey], summary.ErrorCount, summary.HandledCount, summary.FirstSeen, summary.LastSeen)
			continue
		}
		if (!performanceSchemaSummaryFilterMatches(query, "error_name", summary.ErrorName) &&
			!performanceSchemaSummaryFilterMatches(query, "error_name", errorName)) ||
			!performanceSchemaSummaryFilterMatches(query, "error_number", fmt.Sprint(errorNumber)) ||
			!performanceSchemaSummaryFilterMatches(query, "sql_state", sqlState) {
			continue
		}
		values := map[string]interface{}{
			"USER":              summary.User,
			"HOST":              summary.Host,
			"THREAD_ID":         summary.ThreadID,
			"ERROR_NUMBER":      errorNumber,
			"ERROR_NAME":        errorName,
			"SQL_STATE":         sqlState,
			"SUM_ERROR_RAISED":  summary.ErrorCount,
			"SUM_ERROR_HANDLED": summary.HandledCount,
			"FIRST_SEEN":        summary.FirstSeen,
			"LAST_SEEN":         summary.LastSeen,
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	unknownKeys := make([]string, 0, len(unknownByKey))
	for key := range unknownByKey {
		unknownKeys = append(unknownKeys, key)
	}
	sort.Strings(unknownKeys)
	for _, key := range unknownKeys {
		unknown := unknownByKey[key]
		values := map[string]interface{}{
			"ERROR_NUMBER":      int64(0),
			"ERROR_NAME":        nil,
			"SQL_STATE":         nil,
			"SUM_ERROR_RAISED":  unknown.ErrorCount,
			"SUM_ERROR_HANDLED": unknown.HandledCount,
			"FIRST_SEEN":        unknown.FirstSeen,
			"LAST_SEEN":         unknown.LastSeen,
		}
		switch table {
		case "events_errors_summary_by_account_by_error":
			parts := strings.SplitN(key, "\x00", 2)
			values["USER"], values["HOST"] = parts[0], parts[1]
		case "events_errors_summary_by_host_by_error":
			values["HOST"] = key
		case "events_errors_summary_by_thread_by_error":
			threadID, _ := strconv.ParseInt(key, 10, 64)
			values["THREAD_ID"] = threadID
		case "events_errors_summary_by_user_by_error":
			values["USER"] = key
		}
		if performanceSchemaLockValuesMatch(query, values) {
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("performance_schema."+table, columns, rows)
}

type performanceSchemaUnknownErrorSummary struct {
	ErrorCount   int64
	HandledCount int64
	FirstSeen    time.Time
	LastSeen     time.Time
}

func mergePerformanceSchemaUnknownErrorSummary(current *performanceSchemaUnknownErrorSummary, errorCount, handledCount int64, firstSeen, lastSeen time.Time) *performanceSchemaUnknownErrorSummary {
	if current == nil {
		current = &performanceSchemaUnknownErrorSummary{}
	}
	current.ErrorCount += errorCount
	current.HandledCount += handledCount
	if !firstSeen.IsZero() && (current.FirstSeen.IsZero() || firstSeen.Before(current.FirstSeen)) {
		current.FirstSeen = firstSeen
	}
	if !lastSeen.IsZero() && (current.LastSeen.IsZero() || lastSeen.After(current.LastSeen)) {
		current.LastSeen = lastSeen
	}
	return current
}

func performanceSchemaErrorMetadata(errorName string) (int64, string, string, bool) {
	switch errorName {
	case string(ExecutionErrorCodeDuplicateKey), "1062":
		return 1062, "ER_DUP_ENTRY", "23000", true
	case string(ExecutionErrorCodeSchemaOrTableNotFound), string(ExecutionErrorCodeMetadataMissing), "1146":
		return 1146, "ER_NO_SUCH_TABLE", "42S02", true
	case string(ExecutionErrorCodeValidation), "1064":
		return 1064, "ER_PARSE_ERROR", "42000", true
	case string(ExecutionErrorCodeStorageMissing), string(ExecutionErrorCodeStorageReadFailure), string(ExecutionErrorCodeStorageWriteFailure), "1030":
		return 1030, "ER_GET_ERRNO", "HY000", true
	default:
		return 0, "", "", false
	}
}

var performanceSchemaStatementHistogramBounds = []int64{
	1_000,
	10_000,
	100_000,
	1_000_000,
	10_000_000,
	100_000_000,
	1_000_000_000,
	10_000_000_000,
	100_000_000_000,
	1_000_000_000_000,
}

func performanceSchemaStatementHistogramBucket(timer int64) int {
	for index, bound := range performanceSchemaStatementHistogramBounds {
		if timer <= bound {
			return index
		}
	}
	return len(performanceSchemaStatementHistogramBounds)
}

func performanceSchemaStatementHistogramRange(bucket int) (int64, int64) {
	low := int64(0)
	if bucket > 0 {
		low = performanceSchemaStatementHistogramBounds[bucket-1]
	}
	high := int64(-1)
	if bucket < len(performanceSchemaStatementHistogramBounds) {
		high = performanceSchemaStatementHistogramBounds[bucket]
	}
	return low, high
}

// executePerformanceSchemaStatementHistogramSelect derives the two statement
// histogram views from the bounded statement history. The buckets are stable
// across reads and timer values use Performance Schema's picosecond unit.
func (e *XMySQLExecutor) executePerformanceSchemaStatementHistogramSelect(query string, byDigest bool) *SelectResult {
	table := "events_statements_histogram_global"
	if byDigest {
		table = "events_statements_histogram_by_digest"
	}
	columns := requestedInformationSchemaColumns(query, performanceSchemaTableColumns(table))
	if e == nil || e.metricsRecorder == nil {
		return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
	}
	type bucketCounts struct {
		counts []int64
		total  int64
	}
	grouped := make(map[string]*bucketCounts)
	if byDigest {
		for _, event := range e.metricsRecorder.StatementDigestSummary() {
			key := event.Schema + "\x00" + performanceSchemaDigestText(event.SQL)
			counts := grouped[key]
			if counts == nil {
				counts = &bucketCounts{counts: make([]int64, len(performanceSchemaStatementHistogramBounds)+1)}
				grouped[key] = counts
			}
			for _, timer := range event.LatencySamples {
				counts.counts[performanceSchemaStatementHistogramBucket(timer)]++
				counts.total++
			}
		}
	} else {
		counts := &bucketCounts{counts: make([]int64, len(performanceSchemaStatementHistogramBounds)+1)}
		for _, timer := range e.metricsRecorder.StatementHistogramSamples() {
			counts.counts[performanceSchemaStatementHistogramBucket(timer)]++
			counts.total++
		}
		grouped[""] = counts
	}
	rows := make([][]interface{}, 0)
	keys := make([]string, 0, len(grouped))
	for key := range grouped {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		counts := grouped[key]
		for bucket, count := range counts.counts {
			low, high := performanceSchemaStatementHistogramRange(bucket)
			lower := int64(0)
			for index := 0; index <= bucket; index++ {
				lower += counts.counts[index]
			}
			upper := int64(0)
			for index := bucket; index < len(counts.counts); index++ {
				upper += counts.counts[index]
			}
			values := map[string]interface{}{
				"SCHEMA_NAME":            "",
				"DIGEST":                 "",
				"DIGEST_TEXT":            "",
				"BUCKET_NUMBER":          int64(bucket),
				"BUCKET_TIMER_LOW":       low,
				"BUCKET_TIMER_HIGH":      high,
				"COUNT_BUCKET":           count,
				"COUNT_BUCKET_AND_LOWER": lower,
				"COUNT_BUCKET_AND_UPPER": upper,
				"BUCKET_QUANTILE":        float64(lower) / float64(maxInt64(1, counts.total)),
				"COUNT_STAR":             counts.total,
			}
			if byDigest {
				var schema, sqlText string
				if separator := strings.IndexByte(key, 0); separator >= 0 {
					schema, sqlText = key[:separator], key[separator+1:]
				}
				digest := sha256.Sum256([]byte(sqlText))
				values["SCHEMA_NAME"] = schema
				values["DIGEST"] = fmt.Sprintf("%x", digest[:])
				values["DIGEST_TEXT"] = sqlText
			}
			if !performanceSchemaLockValuesMatch(query, values) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("performance_schema."+table, columns, rows)
}

func (e *XMySQLExecutor) executePerformanceSchemaStatementSummaryRegistrySelect(query, dimension string) *SelectResult {
	defaults := performanceSchemaStatementSummaryGlobalColumns
	name := "performance_schema.events_statements_summary_global_by_event_name"
	switch dimension {
	case "thread":
		defaults = performanceSchemaStatementSummaryThreadColumns
		name = "performance_schema.events_statements_summary_by_thread_by_event_name"
	case "account":
		defaults = performanceSchemaStatementSummaryAccountColumns
		name = "performance_schema.events_statements_summary_by_account_by_event_name"
	case "host":
		defaults = performanceSchemaStatementSummaryHostColumns
		name = "performance_schema.events_statements_summary_by_host_by_event_name"
	case "user":
		defaults = performanceSchemaStatementSummaryUserColumns
		name = "performance_schema.events_statements_summary_by_user_by_event_name"
	}
	columns := requestedInformationSchemaColumns(query, defaults)
	if e == nil || e.metricsRecorder == nil {
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	type summary struct {
		threadID                                                                                                                                                                                                                         int64
		user, host, eventName                                                                                                                                                                                                            string
		count, sum, min, max, errors                                                                                                                                                                                                     int64
		warnings, rowsAffected, rowsSent, rowsExamined, selectScan, selectRange, selectFullJoin, selectFullRangeJoin, selectRangeCheck, sortRows, sortScan, sortRange, noIndexUsed, noGoodIndexUsed, maxControlledMemory, maxTotalMemory int64
	}
	byKey := make(map[string]*summary)
	for _, event := range e.metricsRecorder.StatementSummaryForDimension(dimension) {
		eventName := "statement/sql/" + strings.ToLower(event.StatementType)
		user, host := event.User, event.Host
		if host == "" {
			host = "localhost"
		}
		key := eventName
		switch dimension {
		case "thread":
			key = fmt.Sprintf("%d\x00%s", event.ThreadID, eventName)
		case "account":
			key = fmt.Sprintf("%s\x00%s\x00%s", user, host, eventName)
		case "host":
			key = fmt.Sprintf("%s\x00%s", host, eventName)
		case "user":
			key = fmt.Sprintf("%s\x00%s", user, eventName)
		}
		item := byKey[key]
		if item == nil {
			item = &summary{threadID: event.ThreadID, user: user, host: host, eventName: eventName, min: -1}
			byKey[key] = item
		}
		item.count += event.Count
		item.sum += event.SumTimerWait
		if item.min < 0 || event.MinTimerWait < item.min {
			item.min = event.MinTimerWait
		}
		if event.MaxTimerWait > item.max {
			item.max = event.MaxTimerWait
		}
		item.errors += event.Errors
		item.warnings += event.Warnings
		item.rowsAffected += event.RowsAffected
		item.rowsSent += event.RowsSent
		item.rowsExamined += event.RowsExamined
		item.noIndexUsed += event.NoIndexUsed
		item.noGoodIndexUsed += event.NoGoodIndexUsed
		item.selectScan += event.SelectScan
		item.selectRange += event.SelectRange
		item.selectFullJoin += event.SelectFullJoin
		item.selectFullRangeJoin += event.SelectFullRangeJoin
		item.selectRangeCheck += event.SelectRangeCheck
		item.sortRows += event.SortRows
		item.sortScan += event.SortScan
		item.sortRange += event.SortRange
		if event.MaxControlledMemory > item.maxControlledMemory {
			item.maxControlledMemory = event.MaxControlledMemory
		}
		if event.MaxTotalMemory > item.maxTotalMemory {
			item.maxTotalMemory = event.MaxTotalMemory
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([][]interface{}, 0, len(keys))
	for _, key := range keys {
		item := byKey[key]
		if !performanceSchemaSummaryFilterMatches(query, "user", item.user) ||
			!performanceSchemaSummaryFilterMatches(query, "host", item.host) ||
			!performanceSchemaSummaryFilterMatches(query, "event_name", item.eventName) {
			continue
		}
		avg := int64(0)
		if item.count > 0 {
			avg = item.sum / item.count
		}
		values := map[string]interface{}{
			"THREAD_ID": item.threadID, "EVENT_NAME": item.eventName,
			"USER": item.user, "HOST": item.host,
			"COUNT_STAR": item.count, "SUM_TIMER_WAIT": item.sum,
			"MIN_TIMER_WAIT": item.min, "AVG_TIMER_WAIT": avg, "MAX_TIMER_WAIT": item.max,
			"SUM_LOCK_TIME": int64(0), "SUM_ERRORS": item.errors, "SUM_WARNINGS": item.warnings,
			"SUM_ROWS_AFFECTED": item.rowsAffected, "SUM_ROWS_SENT": item.rowsSent, "SUM_ROWS_EXAMINED": item.rowsExamined,
			"SUM_CREATED_TMP_DISK_TABLES": int64(0), "SUM_CREATED_TMP_TABLES": int64(0),
			"SUM_SELECT_FULL_JOIN": item.selectFullJoin, "SUM_SELECT_FULL_RANGE_JOIN": item.selectFullRangeJoin,
			"SUM_SELECT_RANGE": item.selectRange, "SUM_SELECT_RANGE_CHECK": item.selectRangeCheck, "SUM_SELECT_SCAN": item.selectScan,
			"SUM_SORT_MERGE_PASSES": int64(0), "SUM_SORT_RANGE": item.sortRange, "SUM_SORT_ROWS": item.sortRows,
			"SUM_SORT_SCAN": item.sortScan, "SUM_NO_INDEX_USED": item.noIndexUsed, "SUM_NO_GOOD_INDEX_USED": item.noGoodIndexUsed,
			"SUM_CPU_TIME": int64(0), "MAX_CONTROLLED_MEMORY": item.maxControlledMemory, "MAX_TOTAL_MEMORY": item.maxTotalMemory,
			"COUNT_SECONDARY": int64(0),
		}
		if dimension == "account" || dimension == "host" || dimension == "user" {
			parts := strings.Split(key, "\x00")
			switch dimension {
			case "account":
				if len(parts) >= 2 {
					values["USER"], values["HOST"] = parts[0], parts[1]
				}
			case "host":
				if len(parts) >= 1 {
					values["HOST"] = parts[0]
				}
			case "user":
				if len(parts) >= 1 {
					values["USER"] = parts[0]
				}
			}
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult(name, columns, rows)
}

func (e *XMySQLExecutor) executePerformanceSchemaProgramSummarySelect(query string) *SelectResult {
	const table = "events_statements_summary_by_program"
	columns := requestedInformationSchemaColumns(query, performanceSchemaStatementSummaryProgramColumns)
	if e == nil {
		return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
	}
	type summary struct {
		objectType, objectSchema, objectName                    string
		count, sum, min, max, statementCount, errors            int64
		statementsWaitSum, statementsWaitMin, statementsWaitMax int64
		warnings, rowsAffected, rowsSent, rowsExamined          int64
	}
	byKey := make(map[string]*summary)
	e.performanceSchemaMu.RLock()
	events := make([]performanceSchemaProgramEvent, 0, len(e.performanceSchemaProgramSummaries))
	for _, event := range e.performanceSchemaProgramSummaries {
		events = append(events, event)
	}
	if len(events) == 0 {
		events = append(events, e.performanceSchemaProgramHistoryLong...)
	}
	e.performanceSchemaMu.RUnlock()
	for _, event := range events {
		key := event.ObjectType + "\x00" + event.ObjectSchema + "\x00" + event.ObjectName
		item := byKey[key]
		if item == nil {
			item = &summary{objectType: event.ObjectType, objectSchema: event.ObjectSchema, objectName: event.ObjectName, min: -1}
			byKey[key] = item
		}
		count := event.Count
		if count < 0 {
			count = 0
		}
		item.count += count
		item.sum += event.TimerWait
		timerMin, timerMax := event.TimerWaitMin, event.TimerWaitMax
		if timerMin == 0 && event.TimerWait > 0 {
			timerMin = event.TimerWait
		}
		if timerMax == 0 && event.TimerWait > 0 {
			timerMax = event.TimerWait
		}
		item.statementCount += event.StatementCount
		item.statementsWaitSum += event.StatementsWaitSum
		if event.StatementCount > 0 && (item.statementsWaitMin == 0 || event.StatementsWaitMin < item.statementsWaitMin) {
			item.statementsWaitMin = event.StatementsWaitMin
		}
		if event.StatementsWaitMax > item.statementsWaitMax {
			item.statementsWaitMax = event.StatementsWaitMax
		}
		if item.min < 0 || timerMin < item.min {
			item.min = timerMin
		}
		if timerMax > item.max {
			item.max = timerMax
		}
		item.errors += event.Errors
		item.warnings += event.Warnings
		item.rowsAffected += event.RowsAffected
		item.rowsSent += event.RowsSent
		item.rowsExamined += event.RowsExamined
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([][]interface{}, 0, len(keys))
	for _, key := range keys {
		item := byKey[key]
		if !performanceSchemaSummaryFilterMatches(query, "OBJECT_TYPE", item.objectType) ||
			!performanceSchemaSummaryFilterMatches(query, "OBJECT_SCHEMA", item.objectSchema) ||
			!performanceSchemaSummaryFilterMatches(query, "OBJECT_NAME", item.objectName) {
			continue
		}
		avg := int64(0)
		if item.count > 0 {
			avg = item.sum / item.count
		}
		statementsWaitAvg := int64(0)
		if item.statementCount > 0 {
			statementsWaitAvg = item.statementsWaitSum / item.statementCount
		}
		values := map[string]interface{}{
			"OBJECT_TYPE": item.objectType, "OBJECT_SCHEMA": item.objectSchema, "OBJECT_NAME": item.objectName,
			"COUNT_STAR": item.count, "SUM_TIMER_WAIT": item.sum, "MIN_TIMER_WAIT": item.min,
			"AVG_TIMER_WAIT": avg, "MAX_TIMER_WAIT": item.max, "SUM_LOCK_TIME": int64(0),
			"COUNT_STATEMENTS": item.statementCount, "SUM_STATEMENTS_WAIT": item.statementsWaitSum, "MIN_STATEMENTS_WAIT": item.statementsWaitMin,
			"AVG_STATEMENTS_WAIT": statementsWaitAvg, "MAX_STATEMENTS_WAIT": item.statementsWaitMax, "SUM_ERRORS": item.errors, "SUM_WARNINGS": item.warnings, "SUM_ROWS_AFFECTED": item.rowsAffected,
			"SUM_ROWS_SENT": item.rowsSent, "SUM_ROWS_EXAMINED": item.rowsExamined, "SUM_CREATED_TMP_DISK_TABLES": int64(0),
			"SUM_CREATED_TMP_TABLES": int64(0), "SUM_SELECT_FULL_JOIN": int64(0), "SUM_SELECT_FULL_RANGE_JOIN": int64(0),
			"SUM_SELECT_RANGE": int64(0), "SUM_SELECT_RANGE_CHECK": int64(0), "SUM_SELECT_SCAN": int64(0),
			"SUM_SORT_MERGE_PASSES": int64(0), "SUM_SORT_RANGE": int64(0), "SUM_SORT_ROWS": int64(0),
			"SUM_SORT_SCAN": int64(0), "SUM_NO_INDEX_USED": int64(0), "SUM_NO_GOOD_INDEX_USED": int64(0),
			"SUM_CPU_TIME": int64(0), "MAX_CONTROLLED_MEMORY": int64(0), "MAX_TOTAL_MEMORY": int64(0),
			"COUNT_SECONDARY": int64(0),
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("performance_schema."+table, columns, rows)
}

func (e *XMySQLExecutor) executePerformanceSchemaStageSummaryRegistrySelect(query, dimension string) *SelectResult {
	var defaults []string
	var name string
	switch dimension {
	case "account":
		defaults, name = performanceSchemaStageSummaryAccountColumns, "performance_schema.events_stages_summary_by_account_by_event_name"
	case "host":
		defaults, name = performanceSchemaStageSummaryHostColumns, "performance_schema.events_stages_summary_by_host_by_event_name"
	default:
		defaults, name = performanceSchemaStageSummaryUserColumns, "performance_schema.events_stages_summary_by_user_by_event_name"
	}
	columns := requestedInformationSchemaColumns(query, defaults)
	if e == nil || e.metricsRecorder == nil {
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	instrumentEnabled, instrumentTimed := e.performanceSchemaInstrumentSetting("stage/sql/execute")
	if !instrumentEnabled {
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	type summary struct {
		user, host, eventName string
		count, sum, min, max  int64
	}
	byKey := make(map[string]*summary)
	for _, event := range e.metricsRecorder.StatementStageSummaryForDimension(dimension) {
		count := event.Count
		timerSum, timerMin, timerMax := int64(0), int64(0), int64(0)
		if instrumentTimed {
			timerSum, timerMin, timerMax = event.SumTimerWait, event.MinTimerWait, event.MaxTimerWait
		}
		user, host := event.User, event.Host
		if host == "" {
			host = "localhost"
		}
		key := fmt.Sprintf("%s\x00%s\x00stage/sql/execute", user, host)
		if dimension == "host" {
			key = fmt.Sprintf("%s\x00stage/sql/execute", host)
		} else if dimension == "user" {
			key = fmt.Sprintf("%s\x00stage/sql/execute", user)
		}
		item := byKey[key]
		if item == nil {
			item = &summary{user: user, host: host, eventName: "stage/sql/execute", min: -1}
			byKey[key] = item
		}
		item.count += count
		item.sum += timerSum
		if item.min < 0 || timerMin < item.min {
			item.min = timerMin
		}
		if timerMax > item.max {
			item.max = timerMax
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([][]interface{}, 0, len(keys))
	for _, key := range keys {
		item := byKey[key]
		if !performanceSchemaSummaryFilterMatches(query, "user", item.user) ||
			!performanceSchemaSummaryFilterMatches(query, "host", item.host) ||
			!performanceSchemaSummaryFilterMatches(query, "event_name", item.eventName) {
			continue
		}
		avg := int64(0)
		if item.count > 0 {
			avg = item.sum / item.count
		}
		values := map[string]interface{}{
			"USER": item.user, "HOST": item.host, "EVENT_NAME": item.eventName,
			"COUNT_STAR": item.count, "SUM_TIMER_WAIT": item.sum, "MIN_TIMER_WAIT": item.min,
			"AVG_TIMER_WAIT": avg, "MAX_TIMER_WAIT": item.max,
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult(name, columns, rows)
}

func (e *XMySQLExecutor) executePerformanceSchemaWaitSummaryRegistrySelect(query, dimension string) *SelectResult {
	var defaults []string
	var name string
	switch dimension {
	case "account":
		defaults, name = performanceSchemaWaitSummaryAccountColumns, "performance_schema.events_waits_summary_by_account_by_event_name"
	case "host":
		defaults, name = performanceSchemaWaitSummaryHostColumns, "performance_schema.events_waits_summary_by_host_by_event_name"
	default:
		defaults, name = performanceSchemaWaitSummaryUserColumns, "performance_schema.events_waits_summary_by_user_by_event_name"
	}
	columns := requestedInformationSchemaColumns(query, defaults)
	base := e.executePerformanceSchemaWaitSummarySelectWithDimension("select thread_id, event_name, count_star, sum_timer_wait, min_timer_wait, avg_timer_wait, max_timer_wait from performance_schema.events_waits_summary_by_thread_by_event_name", true, dimension)
	type summary struct {
		user, host, eventName string
		count, sum, min, max  int64
	}
	byKey := make(map[string]*summary)
	resetRows := []performanceSchemaWaitSummaryResetEvent(nil)
	if e != nil {
		e.performanceSchemaMu.RLock()
		resetRows = append(resetRows, e.performanceSchemaWaitSummaryDimensionRows[dimension]...)
		e.performanceSchemaMu.RUnlock()
	}
	for _, record := range base.Records {
		values := record.GetValues()
		if len(values) < 7 {
			continue
		}
		threadID := values[0].Int()
		user, host := e.performanceSchemaThreadIdentity(threadID)
		key := fmt.Sprintf("%s\x00%s\x00%s", user, host, values[1].String())
		if dimension == "host" {
			key = fmt.Sprintf("%s\x00%s", host, values[1].String())
		} else if dimension == "user" {
			key = fmt.Sprintf("%s\x00%s", user, values[1].String())
		}
		item := byKey[key]
		if item == nil {
			item = &summary{user: user, host: host, eventName: values[1].String(), min: values[4].Int(), max: values[6].Int()}
			byKey[key] = item
		}
		item.count += values[2].Int()
		item.sum += values[3].Int()
		if values[4].Int() < item.min {
			item.min = values[4].Int()
		}
		if values[6].Int() > item.max {
			item.max = values[6].Int()
		}
	}
	for _, reset := range resetRows {
		user, host := e.performanceSchemaThreadIdentity(reset.ThreadID)
		if host == "" {
			host = "localhost"
		}
		key := fmt.Sprintf("%s\x00%s\x00%s", user, host, reset.Event)
		if dimension == "host" {
			key = fmt.Sprintf("%s\x00%s", host, reset.Event)
		} else if dimension == "user" {
			key = fmt.Sprintf("%s\x00%s", user, reset.Event)
		}
		if _, exists := byKey[key]; !exists {
			byKey[key] = &summary{user: user, host: host, eventName: reset.Event, min: 0, max: 0}
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([][]interface{}, 0, len(keys))
	for _, key := range keys {
		item := byKey[key]
		if !performanceSchemaSummaryFilterMatches(query, "user", item.user) ||
			!performanceSchemaSummaryFilterMatches(query, "host", item.host) ||
			!performanceSchemaSummaryFilterMatches(query, "event_name", item.eventName) {
			continue
		}
		values := map[string]interface{}{
			"USER": item.user, "HOST": item.host, "EVENT_NAME": item.eventName, "COUNT_STAR": item.count,
			"SUM_TIMER_WAIT": item.sum, "MIN_TIMER_WAIT": item.min, "AVG_TIMER_WAIT": averagePerformanceSchemaTimer(item.sum, item.count), "MAX_TIMER_WAIT": item.max,
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult(name, columns, rows)
}

// executePerformanceSchemaWaitSummaryByInstanceSelect exposes wait counters
// keyed by the stable xmysql lock/resource identity. MySQL represents
// OBJECT_INSTANCE_BEGIN as an internal address; xmysql has no equivalent
// pointer identity, so resource IDs are used consistently with the current
// and history wait views instead of inventing an address.
func (e *XMySQLExecutor) executePerformanceSchemaWaitSummaryByInstanceSelect(query string) *SelectResult {
	const table = "events_waits_summary_by_instance"
	columns := requestedInformationSchemaColumns(query, performanceSchemaTableColumns(table))
	type summary struct {
		event, instance      string
		count, sum, min, max int64
	}
	byKey := make(map[string]*summary)
	resetEvents := make([]performanceSchemaWaitSummaryResetEvent, 0)
	if e != nil {
		e.performanceSchemaMu.RLock()
		resetEvents = append(resetEvents, e.performanceSchemaWaitSummaryReset...)
		e.performanceSchemaMu.RUnlock()
	}
	add := func(event, instance string, wait time.Duration) {
		if strings.TrimSpace(event) == "" || strings.TrimSpace(instance) == "" ||
			!performanceSchemaSummaryFilterMatches(query, "event_name", event) ||
			!performanceSchemaSummaryFilterMatches(query, "object_instance_begin", instance) {
			return
		}
		timer := wait.Nanoseconds() * 1000
		if timer < 0 {
			timer = 0
		}
		key := event + "\x00" + instance
		item := byKey[key]
		if item == nil {
			item = &summary{event: event, instance: instance, min: timer, max: timer}
			byKey[key] = item
		}
		item.count++
		item.sum += timer
		if timer < item.min {
			item.min = timer
		}
		if timer > item.max {
			item.max = timer
		}
	}
	if e != nil && e.lockManager != nil {
		for _, edge := range e.lockManager.WaitSummarySnapshot() {
			included, wait := e.performanceSchemaObservedWait("wait/lock/table/sql/handler", edge.WaitDuration)
			if included {
				add("wait/lock/table/sql/handler", edge.ResourceID, wait)
			}
		}
	}
	for _, edge := range e.performanceSchemaWaitEdges() {
		included, wait := e.performanceSchemaObservedWait("wait/lock/table/sql/handler", edge.WaitDuration)
		if included {
			add("wait/lock/table/sql/handler", edge.ResourceID, wait)
		}
	}
	if e != nil {
		for _, edge := range e.getDDLCoordinator().MetadataLockWaitSummary() {
			included, wait := e.performanceSchemaObservedWait("wait/lock/metadata/sql/mdl", edge.WaitDuration)
			if included {
				add("wait/lock/metadata/sql/mdl", edge.Table, wait)
			}
		}
		for _, edge := range e.performanceSchemaMetadataWaitEdges() {
			included, wait := e.performanceSchemaObservedWait("wait/lock/metadata/sql/mdl", edge.WaitDuration)
			if included {
				add("wait/lock/metadata/sql/mdl", edge.Table, wait)
			}
		}
	}
	for _, event := range resetEvents {
		if strings.TrimSpace(event.Event) == "" || strings.TrimSpace(event.Instance) == "" {
			continue
		}
		key := event.Event + "\x00" + event.Instance
		if _, exists := byKey[key]; !exists {
			byKey[key] = &summary{event: event.Event, instance: event.Instance}
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([][]interface{}, 0, len(keys))
	for _, key := range keys {
		item := byKey[key]
		values := map[string]interface{}{
			"EVENT_NAME": item.event, "OBJECT_INSTANCE_BEGIN": item.instance,
			"COUNT_STAR": item.count, "SUM_TIMER_WAIT": item.sum,
			"MIN_TIMER_WAIT": item.min, "AVG_TIMER_WAIT": averagePerformanceSchemaTimer(item.sum, item.count),
			"MAX_TIMER_WAIT": item.max,
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("performance_schema."+table, columns, rows)
}

func (e *XMySQLExecutor) performanceSchemaThreadIdentity(threadID int64) (string, string) {
	if e != nil && e.processlistProvider != nil {
		for _, session := range e.processlistProvider() {
			if session == nil {
				continue
			}
			connectionID := int64Param(session.GetParamByName("connection_id"))
			transactionID := int64Param(session.GetParamByName("transaction_id"))
			if connectionID != threadID && transactionID != threadID {
				continue
			}
			host := sessionStringParam(session, "host")
			if host == "" {
				host = "localhost"
			}
			return sessionStringParam(session, "user"), host
		}
	}
	return "", "localhost"
}

func performanceSchemaTransactionSummaryKey(threadID int64, user, host, dimension string) string {
	switch dimension {
	case "thread":
		return fmt.Sprintf("%d\x00transaction", threadID)
	case "account":
		return fmt.Sprintf("%s\x00%s\x00transaction", user, host)
	case "host":
		return fmt.Sprintf("%s\x00transaction", host)
	case "user":
		return fmt.Sprintf("%s\x00transaction", user)
	default:
		return "transaction"
	}
}

func (e *XMySQLExecutor) executePerformanceSchemaTransactionSummaryRegistrySelect(query, dimension string) *SelectResult {
	defaults := performanceSchemaTransactionSummaryGlobalColumns
	name := "performance_schema.events_transactions_summary_global_by_event_name"
	switch dimension {
	case "thread":
		defaults = performanceSchemaTransactionSummaryThreadColumns
		name = "performance_schema.events_transactions_summary_by_thread_by_event_name"
	case "account":
		defaults = performanceSchemaTransactionSummaryAccountColumns
		name = "performance_schema.events_transactions_summary_by_account_by_event_name"
	case "host":
		defaults = performanceSchemaTransactionSummaryHostColumns
		name = "performance_schema.events_transactions_summary_by_host_by_event_name"
	case "user":
		defaults = performanceSchemaTransactionSummaryUserColumns
		name = "performance_schema.events_transactions_summary_by_user_by_event_name"
	}
	columns := requestedInformationSchemaColumns(query, defaults)
	if e == nil {
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	type summary struct {
		threadID                                 int64
		user, host                               string
		count, readWrite, readOnly               int64
		sum, min, max                            int64
		sumReadWrite, minReadWrite, maxReadWrite int64
		sumReadOnly, minReadOnly, maxReadOnly    int64
	}
	byKey := make(map[string]*summary)
	e.performanceSchemaMu.RLock()
	events := append([]performanceSchemaTransactionEvent(nil), e.performanceSchemaTransactionSummaries...)
	resetEvents := append([]performanceSchemaTransactionEvent(nil), e.performanceSchemaTransactionSummaryReset...)
	dimensionReset := uint64(0)
	if e.performanceSchemaTransactionSummaryDimensionReset != nil {
		dimensionReset = e.performanceSchemaTransactionSummaryDimensionReset[dimension]
	}
	e.performanceSchemaMu.RUnlock()
	for _, event := range events {
		threadID := int64(0)
		if len(event.Row) > 0 {
			threadID = int64Param(event.Row[0])
		}
		user, host := event.User, event.Host
		if host == "" {
			host = "localhost"
		}
		key := performanceSchemaTransactionSummaryKey(threadID, user, host, dimension)
		if dimensionReset > 0 && event.Sequence <= dimensionReset {
			if _, exists := byKey[key]; !exists {
				byKey[key] = &summary{threadID: threadID, user: user, host: host}
			}
			continue
		}
		item := byKey[key]
		if item == nil {
			item = &summary{threadID: threadID, user: user, host: host}
			byKey[key] = item
		}
		item.count++
		timerWait := event.TimerWait
		item.sum += timerWait
		if item.count == 1 || timerWait < item.min {
			item.min = timerWait
		}
		if timerWait > item.max {
			item.max = timerWait
		}
		if len(event.Row) > 13 && strings.EqualFold(fmt.Sprint(event.Row[13]), "READ ONLY") {
			item.readOnly++
			item.sumReadOnly += timerWait
			if item.readOnly == 1 || timerWait < item.minReadOnly {
				item.minReadOnly = timerWait
			}
			if timerWait > item.maxReadOnly {
				item.maxReadOnly = timerWait
			}
		} else {
			item.readWrite++
			item.sumReadWrite += timerWait
			if item.readWrite == 1 || timerWait < item.minReadWrite {
				item.minReadWrite = timerWait
			}
			if timerWait > item.maxReadWrite {
				item.maxReadWrite = timerWait
			}
		}
	}
	for _, event := range resetEvents {
		threadID := int64(0)
		if len(event.Row) > 0 {
			threadID = int64Param(event.Row[0])
		}
		user, host := event.User, event.Host
		if host == "" {
			host = "localhost"
		}
		key := performanceSchemaTransactionSummaryKey(threadID, user, host, dimension)
		if _, exists := byKey[key]; exists {
			continue
		}
		byKey[key] = &summary{threadID: threadID, user: user, host: host}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([][]interface{}, 0, len(keys))
	for _, key := range keys {
		item := byKey[key]
		if !performanceSchemaSummaryFilterMatches(query, "user", item.user) ||
			!performanceSchemaSummaryFilterMatches(query, "host", item.host) ||
			!performanceSchemaSummaryFilterMatches(query, "event_name", "transaction") {
			continue
		}
		values := map[string]interface{}{
			"THREAD_ID": item.threadID, "USER": "", "HOST": "localhost", "EVENT_NAME": "transaction", "COUNT_STAR": item.count,
			"SUM_TIMER_WAIT": item.sum, "MIN_TIMER_WAIT": item.min, "AVG_TIMER_WAIT": averagePerformanceSchemaTimer(item.sum, item.count), "MAX_TIMER_WAIT": item.max,
			"COUNT_READ_WRITE": item.readWrite, "SUM_TIMER_READ_WRITE": item.sumReadWrite, "MIN_TIMER_READ_WRITE": item.minReadWrite,
			"AVG_TIMER_READ_WRITE": averagePerformanceSchemaTimer(item.sumReadWrite, item.readWrite), "MAX_TIMER_READ_WRITE": item.maxReadWrite, "COUNT_READ_ONLY": item.readOnly,
			"SUM_TIMER_READ_ONLY": item.sumReadOnly, "MIN_TIMER_READ_ONLY": item.minReadOnly, "AVG_TIMER_READ_ONLY": averagePerformanceSchemaTimer(item.sumReadOnly, item.readOnly),
			"MAX_TIMER_READ_ONLY": item.maxReadOnly,
		}
		if dimension == "account" || dimension == "host" || dimension == "user" {
			parts := strings.Split(key, "\x00")
			switch dimension {
			case "account":
				if len(parts) >= 2 {
					values["USER"], values["HOST"] = parts[0], parts[1]
				}
			case "host":
				if len(parts) >= 1 {
					values["HOST"] = parts[0]
				}
			case "user":
				if len(parts) >= 1 {
					values["USER"] = parts[0]
				}
			}
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult(name, columns, rows)
}

func performanceSchemaTableName(query string) string {
	lower := strings.ToLower(query)
	marker := "performance_schema"
	index := strings.Index(lower, marker)
	if index < 0 {
		return ""
	}
	rest := strings.TrimSpace(query[index+len(marker):])
	if strings.HasPrefix(rest, ".") {
		rest = strings.TrimSpace(rest[1:])
	}
	name := make([]rune, 0, len(rest))
	for _, ch := range rest {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
			name = append(name, ch)
			continue
		}
		break
	}
	return strings.ToLower(string(name))
}
