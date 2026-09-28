package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

const replicationStorageTransactionContextKey = "replication_storage_transaction"
const clientStorageTransactionContextKey = "client_storage_transaction"
const storageTransactionSessionContextKey = "storage_transaction_session"
const storageTransactionForceCommitContextKey = "storage_transaction_force_commit"
const storageTransactionDataDirContextKey = "storage_transaction_data_dir"

func (e *XMySQLExecutor) beginReplicationStorageTransaction(session server.MySQLServerSession) error {
	if e == nil || session == nil {
		return nil
	}
	if _, ok := session.GetParamByName(replicationStorageTransactionContextKey).(*StorageTransactionContext); ok {
		return nil
	}
	dml, err := e.newStorageIntegratedDMLExecutor()
	if err != nil {
		return err
	}
	ctx := transactionContextForSession(context.Background(), session)
	txn, err := dml.beginStorageTransaction(ctx)
	if err != nil {
		return err
	}
	shared, ok := txn.(*StorageTransactionContext)
	if !ok || shared == nil {
		return fmt.Errorf("replication storage transaction has unexpected type %T", txn)
	}
	session.SetParamByName(replicationStorageTransactionContextKey, shared)
	return nil
}

func (e *XMySQLExecutor) commitReplicationStorageTransaction(session server.MySQLServerSession) error {
	if e == nil || session == nil {
		return nil
	}
	shared, ok := session.GetParamByName(replicationStorageTransactionContextKey).(*StorageTransactionContext)
	if !ok || shared == nil {
		return nil
	}
	dml, err := e.newStorageIntegratedDMLExecutor()
	if err != nil {
		return err
	}
	commitContext := transactionContextForSession(context.Background(), session)
	commitContext = context.WithValue(commitContext, replicationStorageTransactionContextKey, nil)
	transactionID := strings.TrimSpace(fmt.Sprint(session.GetParamByName("replication_transaction_id")))
	postCommitBarrierCompleted := false
	shared.AfterRealCommit = func(_ *StorageTransactionContext) error {
		if transactionID == "" || transactionID == "<nil>" {
			postCommitBarrierCompleted = true
			return nil
		}
		if err := e.persistReplicationCommitRecord(transactionID); err != nil {
			return err
		}
		if err := e.syncTransactionJournal(session); err != nil {
			return err
		}
		// The applied GTID/commit marker is part of the same local publication
		// boundary. The outer replay COMMIT keeps its existing idempotent call,
		// but a successful marker must already protect committed storage if the
		// process fails while dirty pages are being flushed.
		if err := e.markReplicationTransactionCommitted(transactionID); err != nil {
			return err
		}
		postCommitBarrierCompleted = true
		return nil
	}
	if err := dml.commitStorageTransaction(commitContext, shared); err != nil {
		if shared.Status == "COMMITTED" && !postCommitBarrierCompleted {
			// TransactionManager.Commit has already made the storage change
			// authoritative. A later page flush can still fail, but recovery must
			// not treat that durable commit as an orphan and roll it back.
			if transactionID != "" && transactionID != "<nil>" {
				if recordErr := e.persistReplicationCommitRecord(transactionID); recordErr != nil {
					return fmt.Errorf("%v; persist replication commit record: %w", err, recordErr)
				}
				if syncErr := e.syncTransactionJournal(session); syncErr != nil {
					return fmt.Errorf("%v; sync replication commit record: %w", err, syncErr)
				}
			}
		}
		return err
	}
	session.SetParamByName(replicationStorageTransactionContextKey, nil)
	return nil
}

func (e *XMySQLExecutor) rollbackReplicationStorageTransaction(session server.MySQLServerSession) error {
	if e == nil || session == nil {
		return nil
	}
	shared, ok := session.GetParamByName(replicationStorageTransactionContextKey).(*StorageTransactionContext)
	if !ok || shared == nil {
		return nil
	}
	dml, err := e.newStorageIntegratedDMLExecutor()
	if err != nil {
		return err
	}
	rollbackContext := transactionContextForSession(context.Background(), session)
	rollbackContext = context.WithValue(rollbackContext, replicationStorageTransactionContextKey, nil)
	if err := dml.rollbackStorageTransaction(rollbackContext, shared); err != nil {
		return err
	}
	session.SetParamByName(replicationStorageTransactionContextKey, nil)
	return nil
}

func (e *XMySQLExecutor) commitClientStorageTransaction(session server.MySQLServerSession) error {
	if e == nil || session == nil {
		return nil
	}
	shared, ok := session.GetParamByName(clientStorageTransactionContextKey).(*StorageTransactionContext)
	if !ok || shared == nil {
		return nil
	}
	dml, err := e.newStorageIntegratedDMLExecutor()
	if err != nil {
		return err
	}
	commitContext := transactionContextForSession(context.Background(), session)
	commitContext = context.WithValue(commitContext, clientStorageTransactionContextKey, nil)
	commitContext = context.WithValue(commitContext, storageTransactionSessionContextKey, nil)
	commitContext = context.WithValue(commitContext, storageTransactionForceCommitContextKey, true)
	postCommitBarrierCompleted := false
	shared.AfterRealCommit = func(_ *StorageTransactionContext) error {
		state := e.sessionTransactionState(session)
		if state == nil || len(state.Changes) == 0 || strings.TrimSpace(state.CommitKey) == "" {
			postCommitBarrierCompleted = true
			return nil
		}
		if err := e.persistTransactionCommitRecord(e.transactionJournalID(session), state.CommitKey, state.Statements); err != nil {
			return err
		}
		if err := e.syncTransactionJournal(session); err != nil {
			return err
		}
		// XA has its own terminal publisher (including the XID-derived key).
		// Do not publish the ordinary client transaction state here or XA COMMIT
		// will append one native transaction in this barrier and another in its
		// XA-specific terminal path.
		xaState := strings.TrimSpace(fmt.Sprint(session.GetParamByName("xa_state")))
		xaXID := strings.TrimSpace(fmt.Sprint(session.GetParamByName("xa_xid")))
		if (xaState != "" && xaState != "<nil>") || (xaXID != "" && xaXID != "<nil>") {
			postCommitBarrierCompleted = true
			return nil
		}
		// The source publisher is part of the same post-storage publication
		// boundary. With a stable commit key this is idempotent, and a failure
		// leaves the transaction state available for the normal retry path.
		if err := e.commitReplicationStatementsWithID(session, state.CommitKey); err != nil {
			return err
		}
		postCommitBarrierCompleted = true
		return nil
	}
	err = dml.commitStorageTransaction(commitContext, shared)
	if shared.Status == "COMMITTED" {
		if state := e.sessionTransactionState(session); state != nil {
			state.StorageCommitted = true
			session.SetParamByName("transaction_dml_state", state)
		}
		// A flush error is reported after the redo transaction has committed.
		// Make the committed changes visible and leave only the replication
		// publication boundary retryable.
		promotePendingClientTransactionChanges(shared)
		session.SetParamByName(clientStorageTransactionContextKey, nil)
		state := e.sessionTransactionState(session)
		if !postCommitBarrierCompleted && state != nil && len(state.Changes) > 0 && strings.TrimSpace(state.CommitKey) != "" {
			// Storage is now the commit authority. Record that fact in the
			// same journal before invoking the external binlog/GTID publisher;
			// a process restart must not undo committed pages merely because
			// the publisher returned an error afterward.
			if err := e.persistTransactionCommitRecord(e.transactionJournalID(session), state.CommitKey, state.Statements); err != nil {
				return err
			}
			if err := e.syncTransactionJournal(session); err != nil {
				return err
			}
		}
	}
	if err != nil {
		return err
	}
	return nil
}

func (e *XMySQLExecutor) rollbackClientStorageTransaction(session server.MySQLServerSession) error {
	if e == nil || session == nil {
		return nil
	}
	shared, ok := session.GetParamByName(clientStorageTransactionContextKey).(*StorageTransactionContext)
	if !ok || shared == nil {
		return nil
	}
	dml, err := e.newStorageIntegratedDMLExecutor()
	if err != nil {
		return err
	}
	rollbackContext := transactionContextForSession(context.Background(), session)
	rollbackContext = context.WithValue(rollbackContext, clientStorageTransactionContextKey, nil)
	rollbackContext = context.WithValue(rollbackContext, storageTransactionSessionContextKey, nil)
	rollbackContext = context.WithValue(rollbackContext, storageTransactionForceCommitContextKey, true)
	if err := dml.rollbackStorageTransaction(rollbackContext, shared); err != nil {
		return err
	}
	clearPendingClientTransactionChanges(shared)
	session.SetParamByName(clientStorageTransactionContextKey, nil)
	return nil
}

func isReplicationDMLQuery(query string) bool {
	lower := strings.ToLower(strings.TrimSpace(strings.TrimRight(query, ";")))
	for _, prefix := range []string{"insert ", "insert\n", "replace ", "replace\n", "update ", "update\n", "delete ", "delete\n"} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

func isReplicationDDLQuery(query string) bool {
	lower := strings.ToLower(strings.TrimSpace(strings.TrimRight(query, ";")))
	for _, prefix := range []string{"create ", "create\n", "alter ", "alter\n", "drop ", "drop\n", "truncate ", "truncate\n", "rename ", "rename\n"} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

func isReplicationWriteQuery(query string) bool {
	if isReplicationDMLQuery(query) || isReplicationDDLQuery(query) {
		return true
	}
	lower := strings.ToLower(strings.TrimSpace(strings.TrimRight(query, ";")))
	for _, prefix := range []string{"create ", "alter ", "drop ", "truncate ", "rename ", "lock tables ", "unlock tables "} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

func (e *XMySQLEngine) replicaWriteBlocked(session server.MySQLServerSession, query string) bool {
	return e.replicationWriteBlockReason(session, query) != ""
}

func (e *XMySQLEngine) replicationWriteBlockReason(session server.MySQLServerSession, query string) string {
	if e == nil || e.replicationRuntime == nil || !isReplicationWriteQuery(query) {
		return ""
	}
	if session != nil {
		if replaying, _ := session.GetParamByName("replication_replay").(bool); replaying {
			return ""
		}
	}
	if e.conf != nil && e.conf.ReplicationReadOnly && e.replicationRuntime.IsReplica() {
		return "replica is read-only while replication is active"
	}
	if e.replicationRuntime.IsFenced() {
		return "source is fenced and rejects client writes"
	}
	return ""
}

// applyReplicationStatements replays one committed source transaction on a
// replica through the same executor path used by client traffic. This keeps
// constraints, indexes and metadata updates inside the target engine rather
// than maintaining a second storage mutation implementation.
func (e *XMySQLEngine) applyReplicationStatements(statements []replication.Statement) error {
	return e.applyReplicationStatementsWithID("", statements)
}

func (e *XMySQLEngine) applyReplicationStatementsWithID(transactionID string, statements []replication.Statement) error {
	if len(statements) == 0 {
		return nil
	}
	if strings.TrimSpace(transactionID) != "" {
		committed, err := e.QueryExecutor.replicationTransactionCommitted(transactionID)
		if err != nil {
			return fmt.Errorf("check replicated transaction %q: %w", transactionID, err)
		}
		if committed {
			return nil
		}
	}
	session := newReplicationSession()
	session.SetParamByName("replication_replay", true)
	if strings.TrimSpace(transactionID) != "" {
		session.SetParamByName("replication_transaction_id", transactionID)
		session.SetParamByName("transaction_journal_id", replicationTransactionJournalID(transactionID))
	}
	session.SetParamByName("autocommit", "0")
	if err := e.QueryExecutor.beginReplicationStorageTransaction(session); err != nil {
		return err
	}
	if err := executeReplicationQuery(e, session, "begin", ""); err != nil {
		_ = e.QueryExecutor.rollbackReplicationStorageTransaction(session)
		return err
	}
	for _, statement := range statements {
		session.SetParamByName("database", statement.Database)
		if err := executeReplicationQuery(e, session, statement.SQL, statement.Database); err != nil {
			_ = executeReplicationQuery(e, session, "rollback", statement.Database)
			return fmt.Errorf("replay %q: %w", statement.SQL, err)
		}
	}
	return executeReplicationQuery(e, session, "commit", "")
}

// applyReplicationRows applies a committed row-image transaction through the
// engine's normal DML path. It is preferred by Replica when row images are
// present, so a replica can consume the same atomic row change set even when
// no SQL statement text is available (for example, a native row-event feed).
func (e *XMySQLEngine) applyReplicationRows(changes []replication.RowChange) error {
	return e.applyReplicationRowsWithID("", changes)
}

func (e *XMySQLEngine) normalizeNativeReplicationRows(changes []replication.RowChange) ([]replication.RowChange, error) {
	if len(changes) == 0 {
		return changes, nil
	}
	normalized := make([]replication.RowChange, len(changes))
	copy(normalized, changes)
	for index := range normalized {
		change := &normalized[index]
		parts := strings.SplitN(strings.TrimSpace(change.Table), ".", 2)
		if len(parts) != 2 {
			continue
		}
		database := strings.Trim(strings.TrimSpace(parts[0]), "`")
		table := strings.Trim(strings.TrimSpace(parts[1]), "`")
		if database == "" || table == "" {
			continue
		}
		needsBinding := false
		for column := range change.After {
			if _, ok := nativeSyntheticColumnIndex(column); ok {
				needsBinding = true
				break
			}
		}
		if !needsBinding {
			for column := range change.Before {
				if _, ok := nativeSyntheticColumnIndex(column); ok {
					needsBinding = true
					break
				}
			}
		}
		if !needsBinding {
			continue
		}
		info, err := e.QueryExecutor.readPersistedTableInfo(database, table)
		if err != nil {
			return nil, fmt.Errorf("resolve native TABLE_MAP %s.%s: %w", database, table, err)
		}
		columns := make([]string, len(info.Columns))
		for columnIndex, rawColumn := range info.Columns {
			name, _ := rawColumn["name"].(string)
			if strings.TrimSpace(name) == "" {
				return nil, fmt.Errorf("resolve native TABLE_MAP %s.%s: column %d has no name", database, table, columnIndex+1)
			}
			columns[columnIndex] = name
		}
		change.After = renameNativeReplicationImage(change.After, columns)
		change.Before = renameNativeReplicationImage(change.Before, columns)
		for columnIndex, name := range change.Columns {
			if syntheticIndex, ok := nativeSyntheticColumnIndex(name); ok && syntheticIndex < len(columns) {
				change.Columns[columnIndex] = columns[syntheticIndex]
			}
		}
		change.ColumnTypes = renameNativeReplicationStringMap(change.ColumnTypes, columns)
		change.ColumnMetadata = renameNativeReplicationBytesMap(change.ColumnMetadata, columns)
		if len(change.PartialJSONUpdates) > 0 {
			updates := make(map[string][]replication.JSONPartialUpdate, len(change.PartialJSONUpdates))
			for name, values := range change.PartialJSONUpdates {
				if syntheticIndex, ok := nativeSyntheticColumnIndex(name); ok && syntheticIndex < len(columns) {
					name = columns[syntheticIndex]
				}
				updates[name] = values
			}
			change.PartialJSONUpdates = updates
		}
	}
	return normalized, nil
}

func nativeSyntheticColumnIndex(column string) (int, bool) {
	column = strings.TrimSpace(column)
	if !strings.HasPrefix(strings.ToLower(column), "column_") {
		return 0, false
	}
	index, err := strconv.Atoi(column[len("column_"):])
	if err != nil || index < 1 {
		return 0, false
	}
	return index - 1, true
}

func renameNativeReplicationImage(image map[string]interface{}, columns []string) map[string]interface{} {
	if len(image) == 0 {
		return image
	}
	rename := make(map[string]interface{}, len(image))
	for name, value := range image {
		if index, ok := nativeSyntheticColumnIndex(name); ok && index < len(columns) {
			name = columns[index]
		}
		rename[name] = value
	}
	return rename
}

func renameNativeReplicationStringMap(values map[string]string, columns []string) map[string]string {
	if len(values) == 0 {
		return values
	}
	renamed := make(map[string]string, len(values))
	for name, value := range values {
		if index, ok := nativeSyntheticColumnIndex(name); ok && index < len(columns) {
			name = columns[index]
		}
		renamed[name] = value
	}
	return renamed
}

func renameNativeReplicationBytesMap(values map[string][]byte, columns []string) map[string][]byte {
	if len(values) == 0 {
		return values
	}
	renamed := make(map[string][]byte, len(values))
	for name, value := range values {
		if index, ok := nativeSyntheticColumnIndex(name); ok && index < len(columns) {
			name = columns[index]
		}
		renamed[name] = value
	}
	return renamed
}

func (e *XMySQLEngine) applyReplicationRowsWithID(transactionID string, changes []replication.RowChange) error {
	if len(changes) == 0 {
		return nil
	}
	var err error
	changes, err = e.normalizeNativeReplicationRows(changes)
	if err != nil {
		return err
	}
	if strings.TrimSpace(transactionID) != "" {
		committed, err := e.QueryExecutor.replicationTransactionCommitted(transactionID)
		if err != nil {
			return fmt.Errorf("check replicated transaction %q: %w", transactionID, err)
		}
		if committed {
			return nil
		}
	}
	session := newReplicationSession()
	session.SetParamByName("replication_replay", true)
	if strings.TrimSpace(transactionID) != "" {
		session.SetParamByName("replication_transaction_id", transactionID)
		session.SetParamByName("transaction_journal_id", replicationTransactionJournalID(transactionID))
	}
	session.SetParamByName("autocommit", "0")
	if err := e.QueryExecutor.beginReplicationStorageTransaction(session); err != nil {
		return err
	}
	if err := executeReplicationQuery(e, session, "begin", ""); err != nil {
		_ = e.QueryExecutor.rollbackReplicationStorageTransaction(session)
		return err
	}
	for _, change := range changes {
		alreadyApplied, err := e.replicationRowChangeAlreadyApplied(session, change)
		if err != nil {
			_ = executeReplicationQuery(e, session, "rollback", "")
			return err
		}
		if alreadyApplied {
			continue
		}
		sql, database, err := replicationRowChangeSQL(change)
		if err != nil {
			_ = executeReplicationQuery(e, session, "rollback", "")
			return err
		}
		session.SetParamByName("database", database)
		if err := executeReplicationQuery(e, session, sql, database); err != nil {
			_ = executeReplicationQuery(e, session, "rollback", database)
			return fmt.Errorf("replay row change %q: %w", sql, err)
		}
	}
	return executeReplicationQuery(e, session, "commit", "")
}

// replicationRowChangeAlreadyApplied makes row-image replay convergent when
// storage has committed before the replica GTID state replacement.  The
// check is deliberately based on the row image, not on a best-effort marker
// file: a restart can lose that marker while the durable table page remains.
// Statement-only replay still requires the caller's transaction boundary.
func (e *XMySQLEngine) replicationRowChangeAlreadyApplied(session server.MySQLServerSession, change replication.RowChange) (bool, error) {
	parts := strings.SplitN(strings.TrimSpace(change.Table), ".", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return false, fmt.Errorf("replication row change table must be schema.table: %q", change.Table)
	}
	action := strings.ToLower(strings.TrimSpace(change.Action))
	var image map[string]interface{}
	switch action {
	case "insert", "replace":
		image = change.After
	case "update":
		var err error
		image, err = replicationCompleteAfterImage(change)
		if err != nil {
			return false, err
		}
	case "delete":
		image = change.Before
	default:
		return false, fmt.Errorf("unsupported replication row action %q", change.Action)
	}
	conditions, err := replicationRowPredicatesSQL(image)
	if err != nil {
		return false, err
	}
	query := fmt.Sprintf("SELECT 1 FROM %s.%s WHERE %s LIMIT 1",
		quotePartitionIdentifier(strings.Trim(parts[0], "` ")),
		quotePartitionIdentifier(strings.Trim(parts[1], "` ")),
		strings.Join(conditions, " AND "))
	result := e.ExecuteQuery(session, query, strings.Trim(parts[0], "` "))
	for item := range result {
		if item == nil {
			continue
		}
		if item.Err != nil {
			return false, item.Err
		}
		selectResult, ok := item.Data.(*SelectResult)
		if !ok || selectResult == nil {
			continue
		}
		found := len(selectResult.Records) > 0
		if action == "delete" {
			return !found, nil
		}
		return found, nil
	}
	return false, nil
}

func replicationRowChangeSQL(change replication.RowChange) (string, string, error) {
	parts := strings.SplitN(strings.TrimSpace(change.Table), ".", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", fmt.Errorf("replication row change table must be schema.table: %q", change.Table)
	}
	database, table := strings.Trim(parts[0], "` "), strings.Trim(parts[1], "` ")
	tableSQL := quotePartitionIdentifier(database) + "." + quotePartitionIdentifier(table)
	action := strings.ToLower(strings.TrimSpace(change.Action))
	switch action {
	case "insert", "replace":
		columns, values, err := replicationRowImageSQL(change.After)
		if err != nil {
			return "", "", err
		}
		verb := "INSERT"
		if action == "replace" {
			verb = "REPLACE"
		}
		return fmt.Sprintf("%s INTO %s (%s) VALUES (%s)", verb, tableSQL, strings.Join(columns, ", "), strings.Join(values, ", ")), database, nil
	case "delete":
		conditions, err := replicationRowPredicatesSQL(change.Before)
		if err != nil {
			return "", "", err
		}
		return fmt.Sprintf("DELETE FROM %s WHERE %s", tableSQL, strings.Join(conditions, " AND ")), database, nil
	case "update":
		after, err := replicationCompleteAfterImage(change)
		if err != nil {
			return "", "", err
		}
		setColumns, setValues, err := replicationRowImageSQL(after)
		if err != nil {
			return "", "", err
		}
		conditions, err := replicationRowPredicatesSQL(change.Before)
		if err != nil {
			return "", "", err
		}
		assignments := make([]string, len(setColumns))
		for index := range setColumns {
			assignments[index] = setColumns[index] + " = " + setValues[index]
		}
		return fmt.Sprintf("UPDATE %s SET %s WHERE %s", tableSQL, strings.Join(assignments, ", "), strings.Join(conditions, " AND ")), database, nil
	default:
		return "", "", fmt.Errorf("unsupported replication row action %q", change.Action)
	}
}

func replicationCompleteAfterImage(change replication.RowChange) (map[string]interface{}, error) {
	after := make(map[string]interface{}, len(change.After)+len(change.PartialJSONUpdates))
	for column, value := range change.After {
		after[column] = value
	}
	for column, updates := range change.PartialJSONUpdates {
		if _, exists := after[column]; exists {
			continue
		}
		before, exists := change.Before[column]
		if !exists {
			return nil, fmt.Errorf("partial JSON row image is missing before value for column %q", column)
		}
		materialized, ok := replication.ApplyJSONPartialUpdates(before, updates)
		if !ok {
			return nil, fmt.Errorf("unable to materialize partial JSON row image for column %q", column)
		}
		var decoded interface{}
		if err := json.Unmarshal([]byte(materialized), &decoded); err != nil {
			return nil, fmt.Errorf("invalid materialized partial JSON for column %q: %w", column, err)
		}
		encoded, err := json.Marshal(decoded)
		if err != nil {
			return nil, fmt.Errorf("encode materialized partial JSON for column %q: %w", column, err)
		}
		after[column] = string(encoded)
	}
	return after, nil
}

func replicationRowImageSQL(values map[string]interface{}) ([]string, []string, error) {
	if len(values) == 0 {
		return nil, nil, fmt.Errorf("replication row image is empty")
	}
	columns := make([]string, 0, len(values))
	for column := range values {
		if strings.TrimSpace(column) == "" {
			return nil, nil, fmt.Errorf("replication row image contains an empty column")
		}
		columns = append(columns, column)
	}
	sort.Strings(columns)
	quoted := make([]string, len(columns))
	valuesSQL := make([]string, len(columns))
	for index, column := range columns {
		quoted[index] = quotePartitionIdentifier(column)
		literal, err := replicationSQLLiteral(values[column])
		if err != nil {
			return nil, nil, err
		}
		valuesSQL[index] = literal
	}
	return quoted, valuesSQL, nil
}

func replicationRowPredicatesSQL(values map[string]interface{}) ([]string, error) {
	columns, valuesSQL, err := replicationRowImageSQL(values)
	if err != nil {
		return nil, err
	}
	conditions := make([]string, len(columns))
	for index := range columns {
		if valuesSQL[index] == "NULL" {
			conditions[index] = columns[index] + " IS NULL"
		} else {
			conditions[index] = columns[index] + " = " + valuesSQL[index]
		}
	}
	return conditions, nil
}

func replicationSQLLiteral(value interface{}) (string, error) {
	if value == nil {
		return "NULL", nil
	}
	switch typed := value.(type) {
	case string:
		return "'" + strings.ReplaceAll(typed, "'", "''") + "'", nil
	case []byte:
		return "'" + strings.ReplaceAll(string(typed), "'", "''") + "'", nil
	case bool:
		if typed {
			return "TRUE", nil
		}
		return "FALSE", nil
	case time.Time:
		return "'" + typed.Format("2006-01-02 15:04:05.999999") + "'", nil
	case float32:
		if math.IsNaN(float64(typed)) || math.IsInf(float64(typed), 0) {
			return "", fmt.Errorf("replication row image contains non-finite float")
		}
		return strconv.FormatFloat(float64(typed), 'f', -1, 32), nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return "", fmt.Errorf("replication row image contains non-finite float")
		}
		return strconv.FormatFloat(typed, 'f', -1, 64), nil
	case int:
		return strconv.Itoa(typed), nil
	case int8:
		return strconv.FormatInt(int64(typed), 10), nil
	case int16:
		return strconv.FormatInt(int64(typed), 10), nil
	case int32:
		return strconv.FormatInt(int64(typed), 10), nil
	case int64:
		return strconv.FormatInt(typed, 10), nil
	case uint:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint64:
		return strconv.FormatUint(typed, 10), nil
	default:
		return "'" + strings.ReplaceAll(fmt.Sprint(value), "'", "''") + "'", nil
	}
}

func executeReplicationQuery(e *XMySQLEngine, session server.MySQLServerSession, query, database string) error {
	if e == nil {
		return fmt.Errorf("nil engine")
	}
	results := e.ExecuteQuery(session, query, database)
	for result := range results {
		if result != nil && result.Err != nil {
			return result.Err
		}
	}
	return nil
}

type replicationSession struct {
	mu     sync.RWMutex
	params map[string]interface{}
	ctx    *server.SessionContext
}

func newReplicationSession() *replicationSession {
	return &replicationSession{params: make(map[string]interface{}), ctx: server.NewSessionContext("replication")}
}

func (s *replicationSession) GetLastActiveTime() time.Time { return time.Now() }
func (s *replicationSession) SendOK()                      {}
func (s *replicationSession) SendHandleOk()                {}
func (s *replicationSession) SendSelectFields()            {}
func (s *replicationSession) SessionContext() *server.SessionContext {
	return s.ctx
}
func (s *replicationSession) GetParamByName(name string) interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if value := s.params[name]; value != nil {
		return value
	}
	return nil
}
func (s *replicationSession) SetParamByName(name string, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.params[name] = value
	if name == "database" {
		s.ctx.SetCurrentDB(fmt.Sprint(value))
	}
	if name == "autocommit" {
		s.ctx.SetAutocommit(fmt.Sprint(value) != "0")
	}
	if name == "in_transaction" {
		if b, ok := value.(bool); ok {
			s.ctx.SetInTransaction(b)
		}
	}
}
