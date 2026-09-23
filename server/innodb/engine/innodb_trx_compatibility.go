package engine

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

// executeInformationSchemaInnoDBTrxSelect exposes the active transaction
// snapshot owned by TransactionManager. Fields that require a live lock-table
// or session registry are kept NULL rather than guessed from transaction IDs.
func (e *XMySQLExecutor) executeInformationSchemaInnoDBTrxSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{
		"TRX_ID", "TRX_STATE", "TRX_STARTED", "TRX_REQUESTED_LOCK_ID", "TRX_WAIT_STARTED",
		"TRX_WEIGHT", "TRX_MYSQL_THREAD_ID", "TRX_QUERY", "TRX_OPERATION_STATE", "TRX_TABLES_IN_USE",
		"TRX_TABLES_LOCKED", "TRX_LOCK_STRUCTS", "TRX_LOCK_MEMORY_BYTES", "TRX_ROWS_LOCKED",
		"TRX_ROWS_MODIFIED", "TRX_CONCURRENCY_TICKETS", "TRX_ISOLATION_LEVEL",
		"TRX_UNIQUE_CHECKS", "TRX_FOREIGN_KEY_CHECKS", "TRX_LAST_FOREIGN_KEY_ERROR",
		"TRX_IS_READ_ONLY", "TRX_AUTOCOMMIT_NON_LOCKING",
	})
	if e == nil {
		return newInformationSchemaSelectResult("information_schema.innodb_trx", columns, nil)
	}
	txManager, err := e.getTransactionManager()
	if err != nil || txManager == nil {
		return newInformationSchemaSelectResult("information_schema.innodb_trx", columns, nil)
	}
	snapshots := txManager.GetActiveTransactionSnapshots()
	trxFilter, hasTrxFilter := informationSchemaUint64Filter(query, "trx_id")
	rows := make([][]interface{}, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if hasTrxFilter && (snapshot.ID < 0 || uint64(snapshot.ID) != trxFilter) {
			continue
		}
		values := map[string]interface{}{
			"TRX_ID":                     snapshot.ID,
			"TRX_STATE":                  innodbTransactionState(snapshot.State),
			"TRX_STARTED":                snapshot.StartTime.Format("2006-01-02 15:04:05.000000"),
			"TRX_REQUESTED_LOCK_ID":      nil,
			"TRX_WAIT_STARTED":           nil,
			"TRX_WEIGHT":                 nil,
			"TRX_MYSQL_THREAD_ID":        nil,
			"TRX_QUERY":                  nil,
			"TRX_OPERATION_STATE":        nil,
			"TRX_TABLES_IN_USE":          nil,
			"TRX_TABLES_LOCKED":          nil,
			"TRX_LOCK_STRUCTS":           snapshot.LockCount,
			"TRX_LOCK_MEMORY_BYTES":      nil,
			"TRX_ROWS_LOCKED":            nil,
			"TRX_ROWS_MODIFIED":          nil,
			"TRX_CONCURRENCY_TICKETS":    nil,
			"TRX_ISOLATION_LEVEL":        innodbIsolationLevel(snapshot.IsolationLevel),
			"TRX_UNIQUE_CHECKS":          int64(1),
			"TRX_FOREIGN_KEY_CHECKS":     int64(1),
			"TRX_LAST_FOREIGN_KEY_ERROR": nil,
			"TRX_IS_READ_ONLY":           boolToInt64(snapshot.IsReadOnly),
			"TRX_AUTOCOMMIT_NON_LOCKING": int64(0),
		}
		if !performanceSchemaSummaryFilterMatches(query, "trx_state", fmt.Sprint(values["TRX_STATE"])) ||
			!performanceSchemaSummaryFilterMatches(query, "trx_started", fmt.Sprint(values["TRX_STARTED"])) ||
			!performanceSchemaSummaryFilterMatches(query, "trx_lock_structs", fmt.Sprint(values["TRX_LOCK_STRUCTS"])) ||
			!performanceSchemaSummaryFilterMatches(query, "trx_isolation_level", fmt.Sprint(values["TRX_ISOLATION_LEVEL"])) ||
			!performanceSchemaSummaryFilterMatches(query, "trx_is_read_only", fmt.Sprint(values["TRX_IS_READ_ONLY"])) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("information_schema.innodb_trx", columns, rows)
}

func informationSchemaUint64Filter(query, column string) (uint64, bool) {
	pattern := regexp.MustCompile(`(?is)\b` + regexp.QuoteMeta(column) + `\b\s*=\s*([0-9]+)`)
	match := pattern.FindStringSubmatch(query)
	if len(match) != 2 {
		return 0, false
	}
	value, err := strconv.ParseUint(strings.TrimSpace(match[1]), 10, 64)
	return value, err == nil
}

func innodbTransactionState(state uint8) string {
	if state == manager.TRX_STATE_PREPARED {
		return "PREPARED"
	}
	if state == manager.TRX_STATE_ACTIVE {
		return "RUNNING"
	}
	return fmt.Sprintf("STATE_%d", state)
}

func innodbIsolationLevel(level uint8) string {
	switch level {
	case manager.TRX_ISO_READ_UNCOMMITTED:
		return "READ UNCOMMITTED"
	case manager.TRX_ISO_READ_COMMITTED:
		return "READ COMMITTED"
	case manager.TRX_ISO_SERIALIZABLE:
		return "SERIALIZABLE"
	default:
		return "REPEATABLE READ"
	}
}

func boolToInt64(value bool) int64 {
	if value {
		return 1
	}
	return 0
}
