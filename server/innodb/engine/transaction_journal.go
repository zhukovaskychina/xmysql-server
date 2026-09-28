package engine

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

type durableTransactionChange struct {
	RecordType           string                  `json:"record_type,omitempty"`
	TransactionID        string                  `json:"transaction_id,omitempty"`
	StorageTransactionID int64                   `json:"storage_transaction_id,omitempty"`
	TableName            string                  `json:"table_name"`
	Kind                 string                  `json:"kind"`
	RowID                uint64                  `json:"row_id"`
	StorageKey           interface{}             `json:"storage_key"`
	NewStorageKey        interface{}             `json:"new_storage_key,omitempty"`
	ColumnTypes          map[string]string       `json:"column_types,omitempty"`
	Before               map[string]interface{}  `json:"before"`
	After                map[string]interface{}  `json:"after"`
	Statements           []replication.Statement `json:"statements,omitempty"`
}

const replicationCommitJournalRecordType = "replication_commit"

var (
	transactionJournalMu    sync.Mutex
	transactionJournalFiles = make(map[string]*os.File)
)

func transactionJournalPath(dataDir, sessionID string) string {
	clean := strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_").Replace(sessionID)
	if clean == "" {
		clean = "anonymous"
	}
	return filepath.Join(dataDir, "transactions", "active", clean+".json")
}

func replicationTransactionKey(transactionID string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(transactionID)))
	return fmt.Sprintf("%x", digest[:])
}

func replicationTransactionJournalID(transactionID string) string {
	return "replication-" + replicationTransactionKey(transactionID)
}

func replicationCommitMarkerPath(dataDir, transactionID string) string {
	return filepath.Join(dataDir, "transactions", "committed", replicationTransactionKey(transactionID)+".json")
}

func (e *XMySQLExecutor) replicationTransactionCommitted(transactionID string) (bool, error) {
	if e == nil || strings.TrimSpace(transactionID) == "" {
		return false, nil
	}
	transactionID = strings.TrimSpace(transactionID)
	_, err := os.Stat(replicationCommitMarkerPath(e.getDataDir(), transactionID))
	if err == nil {
		return true, nil
	}
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}

	activePath := transactionJournalPath(e.getDataDir(), replicationTransactionJournalID(transactionID))
	raw, readErr := os.ReadFile(activePath)
	if os.IsNotExist(readErr) {
		return false, nil
	}
	if readErr != nil {
		return false, readErr
	}
	return journalHasReplicationCommit(raw, transactionID)
}

func (e *XMySQLExecutor) markReplicationTransactionCommitted(transactionID string) error {
	if e == nil || strings.TrimSpace(transactionID) == "" {
		return nil
	}
	if e.replicationCommitMarkerHook != nil {
		return e.replicationCommitMarkerHook(transactionID)
	}
	path := replicationCommitMarkerPath(e.getDataDir(), transactionID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return writeMetadataFileAtomic(path, []byte(strings.TrimSpace(transactionID)+"\n"))
}

// persistReplicationCommitRecord appends the commit intent to the same active
// journal that contains the transaction's DML changes. The record is synced
// before the separate committed marker is written, so a marker-write failure
// cannot make already-committed storage look like an orphan on restart.
func (e *XMySQLExecutor) persistReplicationCommitRecord(transactionID string) error {
	if e == nil || strings.TrimSpace(transactionID) == "" {
		return nil
	}
	return e.persistTransactionCommitRecord(replicationTransactionJournalID(transactionID), transactionID, nil)
}

// persistTransactionCommitRecord appends a durable commit intent to the
// transaction journal that already contains the transaction's DML changes.
// The recovery pass uses this record to distinguish a storage commit followed
// by a publication/reply failure from an abandoned transaction.
func (e *XMySQLExecutor) persistTransactionCommitRecord(journalID, transactionID string, statements []replication.Statement) error {
	if e == nil || strings.TrimSpace(journalID) == "" || strings.TrimSpace(transactionID) == "" {
		return nil
	}
	transactionID = strings.TrimSpace(transactionID)
	path := transactionJournalPath(e.getDataDir(), journalID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	record := durableTransactionChange{
		RecordType:    replicationCommitJournalRecordType,
		TransactionID: transactionID,
		Statements:    append([]replication.Statement(nil), statements...),
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}

	transactionJournalMu.Lock()
	defer transactionJournalMu.Unlock()
	if err := migrateArrayJournalToJSONL(path); err != nil {
		return err
	}
	file := transactionJournalFiles[path]
	if file == nil {
		file, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		transactionJournalFiles[path] = file
	}
	_, err = file.Write(append(raw, '\n'))
	return err
}

func durableChanges(changes []transactionDMLChange) []durableTransactionChange {
	result := make([]durableTransactionChange, 0, len(changes))
	for _, change := range changes {
		result = append(result, durableTransactionChange{
			TableName: change.tableName, Kind: change.kind, RowID: change.rowID,
			StorageKey: change.storageKey, NewStorageKey: change.newStorageKey,
			ColumnTypes: cloneStringMap(change.columnTypes),
			Before:      cloneTransactionRow(change.before), After: cloneTransactionRow(change.after),
		})
	}
	return result
}

func (e *XMySQLExecutor) transactionJournalID(session server.MySQLServerSession) string {
	if session == nil {
		return ""
	}
	if value := session.GetParamByName("transaction_journal_id"); value != nil {
		candidate := strings.TrimSpace(fmt.Sprint(value))
		if candidate != "" && candidate != "<nil>" && !strings.ContainsAny(candidate, "{}\\/:") {
			return candidate
		}
	}
	if value := session.GetParamByName("session_id"); value != nil {
		candidate := strings.TrimSpace(fmt.Sprint(value))
		if candidate != "" && !strings.ContainsAny(candidate, "{}\\/:") {
			return candidate
		}
	}
	value := reflect.ValueOf(session)
	if value.IsValid() && (value.Kind() == reflect.Ptr || value.Kind() == reflect.Map || value.Kind() == reflect.Func) {
		return fmt.Sprintf("session-%x", value.Pointer())
	}
	return "session"
}

func (e *XMySQLExecutor) persistTransactionJournal(session server.MySQLServerSession, changes []transactionDMLChange) error {
	if e == nil || len(changes) == 0 {
		return nil
	}
	path := transactionJournalPath(e.getDataDir(), e.transactionJournalID(session))
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	journalChanges := durableChanges(changes)
	transactionID, storageTransactionID, statements := e.transactionJournalMetadata(session)
	for index := range journalChanges {
		journalChanges[index].TransactionID = transactionID
		journalChanges[index].StorageTransactionID = storageTransactionID
		journalChanges[index].Statements = append([]replication.Statement(nil), statements...)
	}

	transactionJournalMu.Lock()
	if err := migrateArrayJournalToJSONL(path); err != nil {
		transactionJournalMu.Unlock()
		return err
	}
	file := transactionJournalFiles[path]
	if file == nil {
		var err error
		file, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			transactionJournalMu.Unlock()
			return err
		}
		transactionJournalFiles[path] = file
	}
	for _, change := range journalChanges {
		raw, marshalErr := json.Marshal(change)
		if marshalErr != nil {
			transactionJournalMu.Unlock()
			return marshalErr
		}
		if _, err := file.Write(append(raw, '\n')); err != nil {
			transactionJournalMu.Unlock()
			return err
		}
	}
	// Each change is an independently appended, closed record. The storage
	// engine's WAL/checkpoint path remains responsible for durable page data;
	// avoiding an fsync per prepared batch row prevents transaction journaling
	// from turning a 5,000-row batch into thousands of synchronous flushes.
	transactionJournalMu.Unlock()
	return nil
}

func (e *XMySQLExecutor) transactionJournalMetadata(session server.MySQLServerSession) (string, int64, []replication.Statement) {
	if e == nil || session == nil {
		return "", 0, nil
	}
	transactionID := strings.TrimSpace(fmt.Sprint(session.GetParamByName("replication_transaction_id")))
	if transactionID == "<nil>" {
		transactionID = ""
	}
	state := e.sessionTransactionState(session)
	if transactionID == "" && state != nil {
		transactionID = strings.TrimSpace(state.CommitKey)
	}
	var storageTransactionID int64
	for _, parameter := range []string{clientStorageTransactionContextKey, replicationStorageTransactionContextKey} {
		shared, ok := session.GetParamByName(parameter).(*StorageTransactionContext)
		if !ok || shared == nil || shared.RealTransaction == nil {
			continue
		}
		storageTransactionID = shared.RealTransaction.ID
		break
	}
	if state == nil {
		return transactionID, storageTransactionID, nil
	}
	return transactionID, storageTransactionID, append([]replication.Statement(nil), state.Statements...)
}

func migrateArrayJournalToJSONL(path string) error {
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) || len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	if err != nil {
		return err
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return nil
	}
	var changes []durableTransactionChange
	if err := json.Unmarshal(trimmed, &changes); err != nil {
		return fmt.Errorf("decode legacy transaction journal %s: %w", path, err)
	}
	temporary := path + ".migrate"
	file, err := os.Create(temporary)
	if err != nil {
		return err
	}
	for _, change := range changes {
		encoded, marshalErr := json.Marshal(change)
		if marshalErr != nil {
			_ = file.Close()
			_ = os.Remove(temporary)
			return marshalErr
		}
		if _, writeErr := file.Write(append(encoded, '\n')); writeErr != nil {
			_ = file.Close()
			_ = os.Remove(temporary)
			return writeErr
		}
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(temporary)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return nil
}

func (e *XMySQLExecutor) clearTransactionJournal(session server.MySQLServerSession) {
	if e == nil {
		return
	}
	e.clearTransactionJournalID(e.transactionJournalID(session))
}

func (e *XMySQLExecutor) syncTransactionJournal(session server.MySQLServerSession) error {
	if e == nil || session == nil {
		return nil
	}
	return e.syncTransactionJournalID(e.transactionJournalID(session))
}

func (e *XMySQLExecutor) syncTransactionJournalID(journalID string) error {
	if e == nil || strings.TrimSpace(journalID) == "" {
		return nil
	}
	path := transactionJournalPath(e.getDataDir(), journalID)
	transactionJournalMu.Lock()
	defer transactionJournalMu.Unlock()
	if file := transactionJournalFiles[path]; file != nil {
		return file.Sync()
	}
	return nil
}

func (e *XMySQLExecutor) clearTransactionJournalID(journalID string) {
	if e == nil || strings.TrimSpace(journalID) == "" {
		return
	}
	path := transactionJournalPath(e.getDataDir(), journalID)
	transactionJournalMu.Lock()
	if file := transactionJournalFiles[path]; file != nil {
		_ = file.Sync()
		_ = file.Close()
		delete(transactionJournalFiles, path)
	}
	_ = os.Remove(path)
	transactionJournalMu.Unlock()
}

// closeTransactionJournals closes active journal handles without removing
// their files. The files are recovery input and must survive an engine close;
// closing the handles is also required on Windows before the data directory
// can be removed by a test or an operator.
func (e *XMySQLExecutor) closeTransactionJournals() {
	if e == nil {
		return
	}
	activeDir := filepath.Clean(filepath.Join(e.getDataDir(), "transactions", "active"))
	transactionJournalMu.Lock()
	defer transactionJournalMu.Unlock()
	for path, file := range transactionJournalFiles {
		if filepath.Clean(filepath.Dir(path)) != activeDir {
			continue
		}
		_ = file.Sync()
		_ = file.Close()
		delete(transactionJournalFiles, path)
	}
}

// RecoverOrphanedTransactions rolls back DML journals left by sessions that
// disappeared before COMMIT. It runs after storage metadata and recovery
// managers are initialized, so ordinary rollback code is reused.
func (e *XMySQLExecutor) RecoverOrphanedTransactions() error {
	if e == nil {
		return nil
	}
	dir := filepath.Join(e.getDataDir(), "transactions", "active")
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	preparedJournalIDs := e.preparedXAJournalIDs()
	suspendedJournalIDs := e.suspendedXAJournalIDs()
	if e.tableStorageManager != nil && e.infosSchemaManager != nil {
		if err := e.tableStorageManager.SyncFromInfoSchema(e.infosSchemaManager); err != nil {
			return fmt.Errorf("refresh table storage mapping: %w", err)
		}
	}
	dml, err := e.newStorageIntegratedDMLExecutor()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		journalID := strings.TrimSuffix(entry.Name(), ".json")
		if preparedJournalIDs[journalID] || suspendedJournalIDs[journalID] {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if journalHasReplicationCommitRecord(raw) {
			transactionID, statements, err := journalReplicationCommitRecord(raw)
			if err != nil {
				return fmt.Errorf("read replication commit journal %s: %w", path, err)
			}
			if err := e.recoverPendingReplicationCommit(journalID, transactionID, statements, raw); err != nil {
				return fmt.Errorf("recover replication commit journal %s: %w", path, err)
			}
			// A replication commit record is written and synced after storage
			// commit but before the final marker. If no source publisher is
			// attached, keep the DML in place for an explicit retry. Source
			// engines finalize ordinary client journals above; replication replay
			// journals remain marker-driven and are never republished upstream.
			continue
		}
		changes, err := decodeDurableJournal(raw)
		if err != nil {
			return fmt.Errorf("decode transaction journal %s: %w", path, err)
		}
		transactionID, storageTransactionID, statements := durableJournalMetadata(changes)
		if storageTransactionID != 0 {
			committed, commitErr := e.storageTransactionCommitted(storageTransactionID)
			if commitErr != nil {
				return fmt.Errorf("check WAL commit for transaction journal %s: %w", path, commitErr)
			}
			if committed {
				// The physical storage commit is authoritative even when the
				// higher-level replication commit record was never appended.
				// Reconstruct the native publication from the pre-commit journal
				// instead of rolling back already committed pages.
				if strings.TrimSpace(transactionID) != "" &&
					(e.replicationCommitTransactionHookWithID != nil || e.replicationCommitTransactionHook != nil) {
					if err := e.recoverPendingReplicationCommit(journalID, transactionID, statements, raw); err != nil {
						return fmt.Errorf("republish committed transaction journal %s: %w", path, err)
					}
				}
				continue
			}
		}
		for i := len(changes) - 1; i >= 0; i-- {
			changes[i].Before = normalizeJournalRow(changes[i].Before)
			changes[i].After = normalizeJournalRow(changes[i].After)
			schemaName, tableName, ok := strings.Cut(changes[i].TableName, ".")
			if !ok {
				return fmt.Errorf("invalid orphan journal table %q", changes[i].TableName)
			}
			if e.tableStorageManager != nil && e.infosSchemaManager != nil {
				if err := e.tableStorageManager.EnsureTableStorage(context.Background(), e.infosSchemaManager, schemaName, tableName); err != nil {
					return fmt.Errorf("repair storage mapping for orphan table %s: %w", changes[i].TableName, err)
				}
			}
			change := transactionDMLChange{
				tableName: changes[i].TableName, kind: changes[i].Kind, rowID: changes[i].RowID,
				storageKey: changes[i].StorageKey, newStorageKey: changes[i].NewStorageKey,
				columnTypes: cloneStringMap(changes[i].ColumnTypes),
				before:      changes[i].Before, after: changes[i].After,
			}
			if err := rollbackDMLChange(dml, change); err != nil {
				return fmt.Errorf("rollback orphan journal %s: %w", path, err)
			}
		}
		// Close the append handle before removing the journal. Windows does not
		// permit unlinking an open file; using the same cleanup path as a normal
		// session boundary also removes the in-memory handle cache entry.
		e.clearTransactionJournalID(journalID)
	}
	return nil
}

func (e *XMySQLExecutor) storageTransactionCommitted(transactionID int64) (bool, error) {
	if e == nil || e.txManager == nil || e.txManager.GetRedoLogManager() == nil || transactionID == 0 {
		return false, nil
	}
	return e.txManager.GetRedoLogManager().HasCommittedTransaction(transactionID)
}

func durableJournalMetadata(changes []durableTransactionChange) (string, int64, []replication.Statement) {
	transactionID := ""
	var storageTransactionID int64
	statements := make([]replication.Statement, 0)
	seenStatements := make(map[string]struct{})
	for _, change := range changes {
		if transactionID == "" {
			transactionID = strings.TrimSpace(change.TransactionID)
		}
		if storageTransactionID == 0 && change.StorageTransactionID != 0 {
			storageTransactionID = change.StorageTransactionID
		}
		for _, statement := range change.Statements {
			key := statement.Database + "\x00" + statement.SQL
			if strings.TrimSpace(statement.SQL) == "" {
				continue
			}
			if _, exists := seenStatements[key]; exists {
				continue
			}
			seenStatements[key] = struct{}{}
			statements = append(statements, statement)
		}
	}
	return transactionID, storageTransactionID, statements
}

func journalReplicationCommitRecord(raw []byte) (string, []replication.Statement, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] == '[' {
		return "", nil, nil
	}
	scanner := bufio.NewScanner(bytes.NewReader(trimmed))
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var record durableTransactionChange
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.UseNumber()
		if err := decoder.Decode(&record); err != nil {
			return "", nil, err
		}
		if record.RecordType == replicationCommitJournalRecordType && strings.TrimSpace(record.TransactionID) != "" {
			return strings.TrimSpace(record.TransactionID), append([]replication.Statement(nil), record.Statements...), nil
		}
	}
	if err := scanner.Err(); err != nil {
		return "", nil, err
	}
	return "", nil, nil
}

func (e *XMySQLExecutor) recoverPendingReplicationCommit(journalID, transactionID string, statements []replication.Statement, raw []byte) error {
	if e == nil || strings.TrimSpace(transactionID) == "" || strings.HasPrefix(journalID, "replication-") {
		return nil
	}
	if e.replicationCommitTransactionHookWithID == nil && e.replicationCommitTransactionHook == nil {
		return nil
	}
	if _, err := os.Stat(replicationCommitMarkerPath(e.getDataDir(), transactionID)); err == nil {
		e.clearTransactionJournalID(journalID)
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}

	durableChanges, err := decodeDurableJournal(raw)
	if err != nil {
		return err
	}
	changes := make([]transactionDMLChange, 0, len(durableChanges))
	for _, durable := range durableChanges {
		durable.Before = normalizeJournalRow(durable.Before)
		durable.After = normalizeJournalRow(durable.After)
		changes = append(changes, transactionDMLChange{
			tableName: durable.TableName, kind: durable.Kind, rowID: durable.RowID,
			storageKey: durable.StorageKey, newStorageKey: durable.NewStorageKey,
			columnTypes: cloneStringMap(durable.ColumnTypes),
			before:      durable.Before, after: durable.After,
		})
	}
	rows := replicationRowsFromTransactionChanges(changes)
	if e.replicationCommitTransactionHookWithID != nil {
		if err := e.replicationCommitTransactionHookWithID(transactionID, rows, statements); err != nil {
			return err
		}
	} else if err := e.replicationCommitTransactionHook(rows, statements); err != nil {
		return err
	}
	if err := e.markReplicationTransactionCommitted(transactionID); err != nil {
		return err
	}
	e.clearTransactionJournalID(journalID)
	return nil
}

func decodeDurableJournal(raw []byte) ([]durableTransactionChange, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, nil
	}
	if trimmed[0] == '[' {
		var changes []durableTransactionChange
		decoder := json.NewDecoder(bytes.NewReader(trimmed))
		decoder.UseNumber()
		if err := decoder.Decode(&changes); err != nil {
			return nil, err
		}
		return changes, nil
	}

	changes := make([]durableTransactionChange, 0)
	scanner := bufio.NewScanner(bytes.NewReader(trimmed))
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var change durableTransactionChange
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.UseNumber()
		if err := decoder.Decode(&change); err != nil {
			return nil, err
		}
		if change.RecordType == replicationCommitJournalRecordType {
			continue
		}
		changes = append(changes, change)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return changes, nil
}

func journalHasReplicationCommit(raw []byte, transactionID string) (bool, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] == '[' {
		return false, nil
	}
	scanner := bufio.NewScanner(bytes.NewReader(trimmed))
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var record durableTransactionChange
		decoder := json.NewDecoder(bytes.NewReader(line))
		if err := decoder.Decode(&record); err != nil {
			return false, err
		}
		if record.RecordType != replicationCommitJournalRecordType {
			continue
		}
		if transactionID == "" || strings.TrimSpace(record.TransactionID) == strings.TrimSpace(transactionID) {
			return true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func journalHasReplicationCommitRecord(raw []byte) bool {
	hasCommit, err := journalHasReplicationCommit(raw, "")
	return err == nil && hasCommit
}

func normalizeJournalRow(values map[string]interface{}) map[string]interface{} {
	for key, value := range values {
		switch number := value.(type) {
		case json.Number:
			if integer, err := strconv.ParseInt(string(number), 10, 64); err == nil {
				values[key] = integer
			}
		case float64:
			if number == float64(int64(number)) {
				values[key] = int64(number)
			}
		}
	}
	return values
}
