package engine

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

var pendingClientTransactionChanges = struct {
	sync.RWMutex
	items map[*StorageTransactionContext][]transactionDMLChange
}{items: make(map[*StorageTransactionContext][]transactionDMLChange)}

const transactionSnapshotEpochParam = "transaction_snapshot_epoch"
const transactionSnapshotCapturedParam = "transaction_snapshot_captured"

type committedClientTransactionRecord struct {
	dataDir string
	epoch   uint64
	changes []transactionDMLChange
}

var committedClientTransactionHistory = struct {
	sync.RWMutex
	nextEpoch      uint64
	items          []committedClientTransactionRecord
	activeSnapshot map[string]uint64
}{items: make([]committedClientTransactionRecord, 0, 128), activeSnapshot: make(map[string]uint64)}

const maxCommittedClientTransactionHistory = 4096

func registerPendingClientTransactionChanges(txn *StorageTransactionContext, changes []transactionDMLChange) {
	if txn == nil || len(changes) == 0 {
		return
	}
	pendingClientTransactionChanges.Lock()
	defer pendingClientTransactionChanges.Unlock()
	for _, change := range changes {
		change.before = cloneTransactionRow(change.before)
		change.after = cloneTransactionRow(change.after)
		pendingClientTransactionChanges.items[txn] = append(pendingClientTransactionChanges.items[txn], change)
	}
}

func clearPendingClientTransactionChanges(txn *StorageTransactionContext) {
	if txn == nil {
		return
	}
	pendingClientTransactionChanges.Lock()
	delete(pendingClientTransactionChanges.items, txn)
	pendingClientTransactionChanges.Unlock()
}

func promotePendingClientTransactionChanges(txn *StorageTransactionContext) {
	if txn == nil {
		return
	}
	pendingClientTransactionChanges.Lock()
	changes := append([]transactionDMLChange(nil), pendingClientTransactionChanges.items[txn]...)
	delete(pendingClientTransactionChanges.items, txn)
	pendingClientTransactionChanges.Unlock()
	if len(changes) == 0 {
		return
	}
	for index := range changes {
		changes[index].before = cloneTransactionRow(changes[index].before)
		changes[index].after = cloneTransactionRow(changes[index].after)
	}
	committedClientTransactionHistory.Lock()
	committedClientTransactionHistory.nextEpoch++
	committedClientTransactionHistory.items = append(committedClientTransactionHistory.items, committedClientTransactionRecord{
		dataDir: strings.TrimSpace(txn.DataDir),
		epoch:   committedClientTransactionHistory.nextEpoch,
		changes: changes,
	})
	pruneCommittedClientTransactionHistoryLocked()
	committedClientTransactionHistory.Unlock()
}

func pruneCommittedClientTransactionHistoryLocked() {
	if len(committedClientTransactionHistory.items) <= maxCommittedClientTransactionHistory {
		return
	}
	minimumEpoch := uint64(0)
	hasActiveSnapshot := false
	for _, epoch := range committedClientTransactionHistory.activeSnapshot {
		if !hasActiveSnapshot || epoch < minimumEpoch {
			minimumEpoch = epoch
			hasActiveSnapshot = true
		}
	}
	start := 0
	if !hasActiveSnapshot {
		start = len(committedClientTransactionHistory.items) - maxCommittedClientTransactionHistory
	} else {
		for start < len(committedClientTransactionHistory.items) && committedClientTransactionHistory.items[start].epoch < minimumEpoch {
			start++
		}
		if len(committedClientTransactionHistory.items)-start > maxCommittedClientTransactionHistory {
			// A live snapshot is older than the normal bounded window. Keep
			// every record it may need rather than silently returning a newer
			// view after the history window is exceeded.
			return
		}
	}
	if start > 0 {
		committedClientTransactionHistory.items = append([]committedClientTransactionRecord(nil), committedClientTransactionHistory.items[start:]...)
	}
}

func recordAutocommitClientTransactionChanges(dataDir string, changes []transactionDMLChange) {
	if len(changes) == 0 {
		return
	}
	txn := &StorageTransactionContext{DataDir: strings.TrimSpace(dataDir)}
	registerPendingClientTransactionChanges(txn, changes)
	promotePendingClientTransactionChanges(txn)
}

func transactionSnapshotEpoch(session server.MySQLServerSession) uint64 {
	if session == nil {
		return 0
	}
	raw := session.GetParamByName(transactionSnapshotEpochParam)
	switch value := raw.(type) {
	case uint64:
		return value
	case uint32:
		return uint64(value)
	case int64:
		if value > 0 {
			return uint64(value)
		}
	case int:
		if value > 0 {
			return uint64(value)
		}
	}
	return 0
}

func transactionSnapshotCaptured(session server.MySQLServerSession) bool {
	if session == nil {
		return false
	}
	captured, _ := session.GetParamByName(transactionSnapshotCapturedParam).(bool)
	return captured
}

func transactionSnapshotSessionKey(session server.MySQLServerSession) string {
	if session == nil {
		return ""
	}
	for _, name := range []string{"connection_id", "session_id"} {
		if value := session.GetParamByName(name); value != nil {
			candidate := strings.TrimSpace(fmt.Sprint(value))
			if candidate != "" && candidate != "<nil>" {
				return name + ":" + candidate
			}
		}
	}
	value := reflect.ValueOf(session)
	if value.IsValid() && (value.Kind() == reflect.Ptr || value.Kind() == reflect.Map || value.Kind() == reflect.Func) {
		return fmt.Sprintf("pointer:%T:%x", session, value.Pointer())
	}
	return fmt.Sprintf("value:%T:%v", session, session)
}

func ensureTransactionSnapshot(session server.MySQLServerSession) {
	if session == nil || !sessionTransactionActive(session) || transactionSnapshotCaptured(session) {
		return
	}
	isolation := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(transactionIsolation(session)), "_", "-"))
	if isolation != "REPEATABLE-READ" && isolation != "SERIALIZABLE" {
		return
	}
	committedClientTransactionHistory.Lock()
	epoch := committedClientTransactionHistory.nextEpoch
	key := transactionSnapshotSessionKey(session)
	if key != "" {
		committedClientTransactionHistory.activeSnapshot[key] = epoch
	}
	committedClientTransactionHistory.Unlock()
	session.SetParamByName(transactionSnapshotEpochParam, epoch)
	session.SetParamByName(transactionSnapshotCapturedParam, true)
}

func clearTransactionSnapshot(session server.MySQLServerSession) {
	if session == nil {
		return
	}
	key := transactionSnapshotSessionKey(session)
	committedClientTransactionHistory.Lock()
	if key != "" {
		delete(committedClientTransactionHistory.activeSnapshot, key)
	}
	pruneCommittedClientTransactionHistoryLocked()
	committedClientTransactionHistory.Unlock()
	session.SetParamByName(transactionSnapshotEpochParam, uint64(0))
	session.SetParamByName(transactionSnapshotCapturedParam, false)
}

func pendingClientTransactionChangesForTable(ctx context.Context, schema, table string) []transactionDMLChange {
	if ctx == nil || strings.TrimSpace(schema) == "" || strings.TrimSpace(table) == "" {
		return nil
	}
	currentSession, hasSession := ctx.Value(storageTransactionSessionContextKey).(server.MySQLServerSession)
	if !hasSession || currentSession == nil {
		// Direct engine calls without a session are administrative/internal
		// reads and retain the historical ability to inspect pending pages.
		return nil
	}
	var currentTxn *StorageTransactionContext
	if currentSession != nil {
		currentTxn, _ = currentSession.GetParamByName(clientStorageTransactionContextKey).(*StorageTransactionContext)
	}
	dataDir, _ := ctx.Value(storageTransactionDataDirContextKey).(string)
	dataDir = strings.TrimSpace(dataDir)
	tableName := strings.ToLower(strings.TrimSpace(schema) + "." + strings.TrimSpace(table))
	pendingClientTransactionChanges.RLock()
	var changes []transactionDMLChange
	for txn, txnChanges := range pendingClientTransactionChanges.items {
		if txn == currentTxn {
			continue
		}
		if dataDir != "" && strings.TrimSpace(txn.DataDir) != dataDir {
			continue
		}
		for _, change := range txnChanges {
			if strings.EqualFold(strings.TrimSpace(change.tableName), tableName) {
				changes = append(changes, change)
			}
		}
	}
	pendingClientTransactionChanges.RUnlock()

	if transactionSnapshotCaptured(currentSession) {
		snapshotEpoch := transactionSnapshotEpoch(currentSession)
		committedClientTransactionHistory.RLock()
		for _, record := range committedClientTransactionHistory.items {
			if record.epoch <= snapshotEpoch || (dataDir != "" && record.dataDir != dataDir) {
				continue
			}
			for _, change := range record.changes {
				if strings.EqualFold(strings.TrimSpace(change.tableName), tableName) {
					changes = append(changes, change)
				}
			}
		}
		committedClientTransactionHistory.RUnlock()
	}
	return changes
}

// applyPendingClientTransactionVisibility reconstructs the committed view
// for a reader that is not the owner of an active client transaction. The
// storage layer currently writes B-tree pages eagerly, so this visibility
// fence is needed until the physical page writer gains transaction-owned
// snapshots. The writer session itself is excluded above and continues to
// read its own writes.
func applyPendingClientTransactionVisibility(ctx context.Context, schema, table string, rows []*InsertRowData, tableMeta *metadata.TableMeta) []*InsertRowData {
	changes := pendingClientTransactionChangesForTable(ctx, schema, table)
	if len(changes) == 0 || len(rows) == 0 {
		if len(changes) == 0 {
			return rows
		}
	}
	inserted := make(map[string]struct{})
	originals := make(map[string]map[string]interface{})
	deleted := make(map[string]struct{})
	for _, change := range changes {
		keys := transactionChangeIdentityKeys(change, tableMeta)
		if len(keys) == 0 {
			continue
		}
		switch change.kind {
		case "insert":
			for _, key := range keys {
				inserted[key] = struct{}{}
			}
		case "update", "delete":
			if len(change.before) > 0 {
				for _, key := range keys {
					if _, exists := originals[key]; !exists {
						originals[key] = cloneTransactionRow(change.before)
					}
				}
			}
			if change.kind == "delete" {
				for _, key := range keys {
					deleted[key] = struct{}{}
				}
			}
		}
	}

	visible := make([]*InsertRowData, 0, len(rows))
	existing := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		key := transactionRowIdentityKey(row, tableMeta)
		if _, hidden := inserted[key]; hidden {
			continue
		}
		if original, ok := originals[key]; ok {
			row = rowDataFromTransactionValues(original, tableMeta)
		}
		if key != "" {
			existing[key] = struct{}{}
		}
		visible = append(visible, row)
	}
	for key, original := range originals {
		if _, isDeleted := deleted[key]; !isDeleted {
			continue
		}
		if _, isInserted := inserted[key]; isInserted {
			continue
		}
		if _, alreadyVisible := existing[key]; alreadyVisible {
			continue
		}
		visible = append(visible, rowDataFromTransactionValues(original, tableMeta))
	}
	return visible
}

func transactionChangeIdentityKeys(change transactionDMLChange, tableMeta *metadata.TableMeta) []string {
	seen := make(map[string]struct{}, 2)
	keys := make([]string, 0, 2)
	for _, values := range []map[string]interface{}{change.before, change.after} {
		if key := transactionValuesIdentityKey(values, tableMeta); key != "" {
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				keys = append(keys, key)
			}
		}
	}
	return keys
}

func transactionRowIdentityKey(row *InsertRowData, tableMeta *metadata.TableMeta) string {
	if row == nil {
		return ""
	}
	return transactionValuesIdentityKey(row.ColumnValues, tableMeta)
}

func transactionValuesIdentityKey(values map[string]interface{}, tableMeta *metadata.TableMeta) string {
	if len(values) == 0 || tableMeta == nil {
		return ""
	}
	primaryKeys := effectivePrimaryKeyColumns(tableMeta)
	if len(primaryKeys) == 0 {
		value, exists := values[hiddenRowIDColumnName]
		if !exists {
			return ""
		}
		return hiddenRowIDColumnName + "=" + fmt.Sprint(value)
	}
	parts := make([]string, 0, len(primaryKeys))
	for _, column := range primaryKeys {
		value, exists := values[column]
		if !exists {
			return ""
		}
		parts = append(parts, strings.ToLower(column)+"="+fmt.Sprint(value))
	}
	return strings.Join(parts, "|")
}
