package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

// executeInformationSchemaInnoDBLockWaitsSelect projects the row-lock wait
// graph already maintained by LockManager. Metadata-lock waits remain owned by
// Performance Schema because they do not have InnoDB row-lock identifiers.
func (e *XMySQLExecutor) executeInformationSchemaInnoDBLockWaitsSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{
		"REQUESTING_TRX_ID", "REQUESTED_LOCK_ID", "BLOCKING_TRX_ID", "BLOCKING_LOCK_ID",
	})
	rows := make([][]interface{}, 0)
	requestingFilter, hasRequestingFilter := informationSchemaUint64Filter(query, "requesting_trx_id")
	blockingFilter, hasBlockingFilter := informationSchemaUint64Filter(query, "blocking_trx_id")
	for _, edge := range e.performanceSchemaWaitEdges() {
		if hasRequestingFilter && edge.WaitingTxID != requestingFilter || hasBlockingFilter && edge.BlockingTxID != blockingFilter {
			continue
		}
		values := map[string]interface{}{
			"REQUESTING_TRX_ID": int64(edge.WaitingTxID),
			"REQUESTED_LOCK_ID": lockCompatibilityID(edge.ResourceID, edge.WaitingTxID),
			"BLOCKING_TRX_ID":   int64(edge.BlockingTxID),
			"BLOCKING_LOCK_ID":  lockCompatibilityID(edge.ResourceID, edge.BlockingTxID),
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("information_schema.innodb_lock_waits", columns, rows)
}

// executeInformationSchemaInnoDBLocksSelect exposes the live record-lock
// requests retained by LockManager, including granted locks that are not
// involved in a wait. Wait-edge projection remains a compatibility fallback
// for lock sources that do not expose the request inventory directly.
func (e *XMySQLExecutor) executeInformationSchemaInnoDBLocksSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{
		"LOCK_ID", "LOCK_TRX_ID", "LOCK_MODE", "LOCK_TYPE", "LOCK_TABLE", "LOCK_INDEX",
		"LOCK_SPACE", "LOCK_PAGE", "LOCK_REC", "LOCK_DATA",
	})
	rows := make([][]interface{}, 0)
	trxFilter, hasTrxFilter := informationSchemaUint64Filter(query, "lock_trx_id")
	seen := make(map[string]struct{})
	appendLock := func(resourceID string, txID uint64, lockType manager.LockType, lockMode manager.LockMode) {
		if hasTrxFilter && txID != trxFilter {
			return
		}
		lockID := lockCompatibilityID(resourceID, txID)
		if _, ok := seen[lockID]; ok {
			return
		}
		seen[lockID] = struct{}{}
		values := map[string]interface{}{
			"LOCK_ID":     lockID,
			"LOCK_TRX_ID": int64(txID),
			"LOCK_MODE":   performanceSchemaLockMode(lockType, lockMode),
			"LOCK_TYPE":   innodbLockType(lockMode),
			"LOCK_TABLE":  resourceID,
			"LOCK_INDEX":  nil,
			"LOCK_SPACE":  nil,
			"LOCK_PAGE":   nil,
			"LOCK_REC":    nil,
			"LOCK_DATA":   nil,
		}
		if page, record, ok := parseInnoDBLockResourceID(resourceID); ok {
			values["LOCK_PAGE"] = int64(page)
			values["LOCK_REC"] = int64(record)
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			return
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	if e != nil && e.lockManager != nil {
		for _, lock := range e.lockManager.LockSnapshots() {
			appendLock(lock.ResourceID, lock.TransactionID, lock.LockType, lock.Mode)
		}
	}
	for _, edge := range e.performanceSchemaWaitEdges() {
		for _, txID := range []uint64{edge.WaitingTxID, edge.BlockingTxID} {
			appendLock(edge.ResourceID, txID, edge.LockType, edge.Mode)
		}
	}
	return newInformationSchemaSelectResult("information_schema.innodb_locks", columns, rows)
}

func parseInnoDBLockResourceID(resourceID string) (page, record uint64, ok bool) {
	parts := strings.Split(resourceID, "_")
	if len(parts) != 3 {
		return 0, 0, false
	}
	page, pageErr := strconv.ParseUint(parts[1], 10, 32)
	record, recordErr := strconv.ParseUint(parts[2], 10, 64)
	if pageErr != nil || recordErr != nil {
		return 0, 0, false
	}
	return page, record, true
}

func lockCompatibilityID(resource string, txID uint64) string {
	return fmt.Sprintf("%s:%d", resource, txID)
}

func innodbLockType(mode manager.LockMode) string {
	if mode == manager.LOCK_MODE_TABLE {
		return "TABLE"
	}
	return "RECORD"
}
