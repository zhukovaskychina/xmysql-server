package engine

import (
	"fmt"

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

// executeInformationSchemaInnoDBLocksSelect exposes both sides of each wait
// edge. A full InnoDB lock table contains granted locks that are not involved
// in a wait; those are intentionally not fabricated from the wait graph.
func (e *XMySQLExecutor) executeInformationSchemaInnoDBLocksSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{
		"LOCK_ID", "LOCK_TRX_ID", "LOCK_MODE", "LOCK_TYPE", "LOCK_TABLE", "LOCK_INDEX",
		"LOCK_SPACE", "LOCK_PAGE", "LOCK_REC", "LOCK_DATA",
	})
	rows := make([][]interface{}, 0)
	trxFilter, hasTrxFilter := informationSchemaUint64Filter(query, "lock_trx_id")
	seen := make(map[string]struct{})
	for _, edge := range e.performanceSchemaWaitEdges() {
		for _, lock := range []struct {
			txID uint64
			mode string
		}{
			{txID: edge.WaitingTxID, mode: "X"},
			{txID: edge.BlockingTxID, mode: "X"},
		} {
			if hasTrxFilter && lock.txID != trxFilter {
				continue
			}
			lockID := lockCompatibilityID(edge.ResourceID, lock.txID)
			if _, ok := seen[lockID]; ok {
				continue
			}
			seen[lockID] = struct{}{}
			values := map[string]interface{}{
				"LOCK_ID":     lockID,
				"LOCK_TRX_ID": int64(lock.txID),
				"LOCK_MODE":   lock.mode,
				"LOCK_TYPE":   innodbLockType(edge.Mode),
				"LOCK_TABLE":  edge.ResourceID,
				"LOCK_INDEX":  nil,
				"LOCK_SPACE":  nil,
				"LOCK_PAGE":   nil,
				"LOCK_REC":    nil,
				"LOCK_DATA":   nil,
			}
			if !performanceSchemaLockValuesMatch(query, values) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("information_schema.innodb_locks", columns, rows)
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
