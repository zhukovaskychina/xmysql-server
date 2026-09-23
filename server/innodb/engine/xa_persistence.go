package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

const preparedXAManifestVersion = 1
const suspendedXAManifestVersion = 1

type durableXAIdentifier struct {
	GTRID    string `json:"gtrid"`
	BQUAL    string `json:"bqual,omitempty"`
	FormatID uint32 `json:"format_id"`
}

type durablePreparedXAManifest struct {
	Version    int                        `json:"version"`
	XID        durableXAIdentifier        `json:"xid"`
	JournalID  string                     `json:"journal_id"`
	Changes    []durableTransactionChange `json:"changes,omitempty"`
	Statements []replication.Statement    `json:"statements,omitempty"`
}

type durableXASavepoint struct {
	Name   string `json:"name"`
	Offset int    `json:"offset"`
}

type durableSuspendedXAManifest struct {
	Version        int                        `json:"version"`
	XID            durableXAIdentifier        `json:"xid"`
	JournalID      string                     `json:"journal_id"`
	Changes        []durableTransactionChange `json:"changes,omitempty"`
	Statements     []replication.Statement    `json:"statements,omitempty"`
	Savepoints     []durableXASavepoint       `json:"savepoints,omitempty"`
	AccessMode     string                     `json:"access_mode,omitempty"`
	IsolationLevel string                     `json:"isolation_level,omitempty"`
}

func preparedXAManifestDir(dataDir string) string {
	return filepath.Join(dataDir, "transactions", "prepared")
}

func preparedXAManifestPath(dataDir, key string) string {
	digest := sha256.Sum256([]byte(key))
	return filepath.Join(preparedXAManifestDir(dataDir), hex.EncodeToString(digest[:])+".json")
}

func suspendedXAManifestDir(dataDir string) string {
	return filepath.Join(dataDir, "transactions", "suspended")
}

func suspendedXAManifestPath(dataDir, key string) string {
	digest := sha256.Sum256([]byte(key))
	return filepath.Join(suspendedXAManifestDir(dataDir), hex.EncodeToString(digest[:])+".json")
}

func durableXAIdentifierFrom(xid xaIdentifier) durableXAIdentifier {
	return durableXAIdentifier{GTRID: xid.gtrid, BQUAL: xid.bqual, FormatID: xid.formatID}
}

func (x durableXAIdentifier) xaIdentifier() xaIdentifier {
	return xaIdentifier{gtrid: x.GTRID, bqual: x.BQUAL, formatID: x.FormatID}
}

func transactionChangesFromDurable(changes []durableTransactionChange) []transactionDMLChange {
	result := make([]transactionDMLChange, 0, len(changes))
	for _, change := range changes {
		result = append(result, transactionDMLChange{
			tableName:     change.TableName,
			kind:          change.Kind,
			rowID:         change.RowID,
			storageKey:    change.StorageKey,
			newStorageKey: change.NewStorageKey,
			columnTypes:   cloneStringMap(change.ColumnTypes),
			before:        normalizeJournalRow(change.Before),
			after:         normalizeJournalRow(change.After),
		})
	}
	return result
}

func (e *XMySQLExecutor) persistPreparedXAManifest(prepared *xaPreparedTransaction) error {
	if e == nil || prepared == nil {
		return fmt.Errorf("invalid prepared XA transaction")
	}
	if strings.TrimSpace(prepared.xid.gtrid) == "" {
		return fmt.Errorf("prepared XA transaction has an empty gtrid")
	}
	if err := os.MkdirAll(preparedXAManifestDir(e.getDataDir()), 0755); err != nil {
		return err
	}
	if err := e.syncTransactionJournal(prepared.session); err != nil {
		return fmt.Errorf("sync XA transaction journal: %w", err)
	}
	manifest := durablePreparedXAManifest{
		Version:    preparedXAManifestVersion,
		XID:        durableXAIdentifierFrom(prepared.xid),
		JournalID:  prepared.journalID,
		Changes:    durableChanges(prepared.changes),
		Statements: append([]replication.Statement(nil), prepared.statements...),
	}
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	prepared.manifestPath = preparedXAManifestPath(e.getDataDir(), prepared.xid.key())
	if err := writeMetadataFileAtomic(prepared.manifestPath, raw); err != nil {
		return fmt.Errorf("write prepared XA manifest: %w", err)
	}
	return nil
}

func (e *XMySQLExecutor) removePreparedXAManifest(prepared *xaPreparedTransaction) error {
	if e == nil || prepared == nil {
		return nil
	}
	path := prepared.manifestPath
	if path == "" {
		path = preparedXAManifestPath(e.getDataDir(), prepared.xid.key())
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	e.clearTransactionJournalID(prepared.journalID)
	return nil
}

func (e *XMySQLExecutor) loadPreparedXATransactions() error {
	if e == nil {
		return nil
	}
	dir := preparedXAManifestDir(e.getDataDir())
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	loaded := make(map[string]*xaPreparedTransaction)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var manifest durablePreparedXAManifest
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if err := decoder.Decode(&manifest); err != nil {
			return fmt.Errorf("decode prepared XA manifest %s: %w", path, err)
		}
		if manifest.Version != preparedXAManifestVersion {
			return fmt.Errorf("unsupported prepared XA manifest version %d in %s", manifest.Version, path)
		}
		xid := manifest.XID.xaIdentifier()
		if strings.TrimSpace(xid.gtrid) == "" {
			return fmt.Errorf("prepared XA manifest %s has an empty gtrid", path)
		}
		prepared := &xaPreparedTransaction{
			xid:          xid,
			changes:      transactionChangesFromDurable(manifest.Changes),
			statements:   append([]replication.Statement(nil), manifest.Statements...),
			journalID:    manifest.JournalID,
			manifestPath: path,
		}
		if previous := loaded[xid.key()]; previous != nil {
			return fmt.Errorf("duplicate prepared XA XID %s", xid.key())
		}
		loaded[xid.key()] = prepared
	}
	e.xaMu.Lock()
	if e.xaPrepared == nil {
		e.xaPrepared = make(map[string]*xaPreparedTransaction)
	}
	for key, prepared := range loaded {
		e.xaPrepared[key] = prepared
	}
	e.xaMu.Unlock()
	return nil
}

func (e *XMySQLExecutor) persistSuspendedXAManifest(suspended *xaSuspendedTransaction) error {
	if e == nil || suspended == nil || suspended.state == nil {
		return fmt.Errorf("invalid suspended XA transaction")
	}
	if strings.TrimSpace(suspended.xid.gtrid) == "" {
		return fmt.Errorf("suspended XA transaction has an empty gtrid")
	}
	if err := os.MkdirAll(suspendedXAManifestDir(e.getDataDir()), 0755); err != nil {
		return err
	}
	if err := e.syncTransactionJournalID(suspended.journalID); err != nil {
		return fmt.Errorf("sync suspended XA transaction journal: %w", err)
	}
	savepoints := make([]durableXASavepoint, 0, len(suspended.state.Savepoints))
	for _, point := range suspended.state.Savepoints {
		savepoints = append(savepoints, durableXASavepoint{Name: point.name, Offset: point.offset})
	}
	manifest := durableSuspendedXAManifest{
		Version:        suspendedXAManifestVersion,
		XID:            durableXAIdentifierFrom(suspended.xid),
		JournalID:      suspended.journalID,
		Changes:        durableChanges(suspended.state.Changes),
		Statements:     append([]replication.Statement(nil), suspended.state.Statements...),
		Savepoints:     savepoints,
		AccessMode:     suspended.state.AccessMode,
		IsolationLevel: suspended.state.IsolationLevel,
	}
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	return writeMetadataFileAtomic(suspendedXAManifestPath(e.getDataDir(), suspended.xid.key()), raw)
}

func (e *XMySQLExecutor) removeSuspendedXAManifest(suspended *xaSuspendedTransaction) error {
	if e == nil || suspended == nil {
		return nil
	}
	path := suspendedXAManifestPath(e.getDataDir(), suspended.xid.key())
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (e *XMySQLExecutor) loadSuspendedXATransactions() error {
	if e == nil {
		return nil
	}
	dir := suspendedXAManifestDir(e.getDataDir())
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	loaded := make(map[string]*xaSuspendedTransaction)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var manifest durableSuspendedXAManifest
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if err := decoder.Decode(&manifest); err != nil {
			return fmt.Errorf("decode suspended XA manifest %s: %w", path, err)
		}
		if manifest.Version != suspendedXAManifestVersion {
			return fmt.Errorf("unsupported suspended XA manifest version %d in %s", manifest.Version, path)
		}
		xid := manifest.XID.xaIdentifier()
		if strings.TrimSpace(xid.gtrid) == "" {
			return fmt.Errorf("suspended XA manifest %s has an empty gtrid", path)
		}
		state := &sessionTransactionState{
			Changes:        transactionChangesFromDurable(manifest.Changes),
			Statements:     append([]replication.Statement(nil), manifest.Statements...),
			AccessMode:     manifest.AccessMode,
			IsolationLevel: manifest.IsolationLevel,
		}
		for _, point := range manifest.Savepoints {
			state.Savepoints = append(state.Savepoints, sessionSavepoint{name: point.Name, offset: point.Offset})
		}
		suspended := &xaSuspendedTransaction{xid: xid, state: state, journalID: manifest.JournalID}
		if previous := loaded[xid.key()]; previous != nil {
			return fmt.Errorf("duplicate suspended XA XID %s", xid.key())
		}
		loaded[xid.key()] = suspended
	}
	e.xaMu.Lock()
	if e.xaSuspended == nil {
		e.xaSuspended = make(map[string]*xaSuspendedTransaction)
	}
	for key, suspended := range loaded {
		e.xaSuspended[key] = suspended
	}
	e.xaMu.Unlock()
	return nil
}

func (e *XMySQLExecutor) suspendedXAJournalIDs() map[string]bool {
	result := make(map[string]bool)
	if e == nil {
		return result
	}
	e.xaMu.Lock()
	defer e.xaMu.Unlock()
	for _, suspended := range e.xaSuspended {
		if suspended != nil && strings.TrimSpace(suspended.journalID) != "" {
			name := filepath.Base(transactionJournalPath(e.getDataDir(), suspended.journalID))
			result[strings.TrimSuffix(name, ".json")] = true
		}
	}
	return result
}

func (e *XMySQLExecutor) preparedXAJournalIDs() map[string]bool {
	result := make(map[string]bool)
	if e == nil {
		return result
	}
	e.xaMu.Lock()
	defer e.xaMu.Unlock()
	for _, prepared := range e.xaPrepared {
		if prepared != nil && strings.TrimSpace(prepared.journalID) != "" {
			name := filepath.Base(transactionJournalPath(e.getDataDir(), prepared.journalID))
			result[strings.TrimSuffix(name, ".json")] = true
		}
	}
	return result
}
