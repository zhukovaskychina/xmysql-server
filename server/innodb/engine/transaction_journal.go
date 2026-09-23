package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/server"
)

type durableTransactionChange struct {
	TableName     string                 `json:"table_name"`
	Kind          string                 `json:"kind"`
	RowID         uint64                 `json:"row_id"`
	StorageKey    interface{}            `json:"storage_key"`
	NewStorageKey interface{}            `json:"new_storage_key,omitempty"`
	ColumnTypes   map[string]string      `json:"column_types,omitempty"`
	Before        map[string]interface{} `json:"before"`
	After         map[string]interface{} `json:"after"`
}

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
	for _, change := range durableChanges(changes) {
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
		changes, err := decodeDurableJournal(raw)
		if err != nil {
			return fmt.Errorf("decode transaction journal %s: %w", path, err)
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
		if err := os.Remove(path); err != nil {
			return err
		}
	}
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
		changes = append(changes, change)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return changes, nil
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
