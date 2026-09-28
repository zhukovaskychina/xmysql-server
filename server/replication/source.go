package replication

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Source struct {
	mu               sync.Mutex
	UUID             string
	Writer           *BinlogWriter
	Executed         GTIDSet
	nextSeq          uint64
	statePath        string
	durableStatePath string
	keyPath          string
	transactionKeys  map[string]GTID
	preparedXA       map[string]GTID
	preparedKeys     map[string]GTID
	// durableStateHook is test-only fault injection for the crash window
	// between the logical/native binlog append and source state replacement.
	durableStateHook func() error
}

// sourceDurableState keeps the GTID execution set and keyed-transaction
// identity map in one atomic replacement. The legacy files remain as mirrors
// for compatibility with existing installations, but this file is the
// recovery source of truth for the crash window between those mirrors.
type sourceDurableState struct {
	Executed        GTIDSet         `json:"executed"`
	TransactionKeys map[string]GTID `json:"transaction_keys"`
	PreparedXA      map[string]GTID `json:"prepared_xa,omitempty"`
	PreparedKeys    map[string]GTID `json:"prepared_keys,omitempty"`
}

// replicationFileSyncHook is test-only fault injection for the durability
// boundary between writing a replacement state file and renaming it into
// place.
var replicationFileSyncHook func(*os.File) error

func NewSource(dataDir, uuid string, serverID uint32) (*Source, error) {
	statePath := filepath.Join(dataDir, "replication", "source_gtid.json")
	durableStatePath := filepath.Join(dataDir, "replication", "source_state.json")
	writer, err := NewBinlogWriter(filepath.Join(dataDir, "replication", "binlog.jsonl"), serverID)
	if err != nil {
		return nil, err
	}
	source := &Source{UUID: uuid, Writer: writer, Executed: GTIDSet{}, nextSeq: 1, statePath: statePath, durableStatePath: durableStatePath, keyPath: filepath.Join(dataDir, "replication", "source_transaction_keys.json"), transactionKeys: map[string]GTID{}, preparedXA: map[string]GTID{}, preparedKeys: map[string]GTID{}}
	loadedDurableState := false
	if raw, err := os.ReadFile(source.durableStatePath); err == nil {
		var durable sourceDurableState
		if json.Unmarshal(raw, &durable) == nil && (durable.Executed != nil || durable.TransactionKeys != nil || durable.PreparedXA != nil || durable.PreparedKeys != nil) {
			if durable.Executed != nil {
				source.Executed = durable.Executed
			}
			if durable.TransactionKeys != nil {
				source.transactionKeys = durable.TransactionKeys
			}
			if durable.PreparedXA != nil {
				source.preparedXA = durable.PreparedXA
			}
			if durable.PreparedKeys != nil {
				source.preparedKeys = durable.PreparedKeys
			}
			loadedDurableState = true
		}
	}
	if !loadedDurableState {
		if raw, err := os.ReadFile(statePath); err == nil {
			_ = json.Unmarshal(raw, &source.Executed)
		}
		if raw, err := os.ReadFile(source.keyPath); err == nil {
			_ = json.Unmarshal(raw, &source.transactionKeys)
		}
	}
	if source.transactionKeys == nil {
		source.transactionKeys = map[string]GTID{}
	}
	if source.preparedXA == nil {
		source.preparedXA = map[string]GTID{}
	}
	if source.preparedKeys == nil {
		source.preparedKeys = map[string]GTID{}
	}
	// Reconcile the durable binlog on startup. This closes the crash window
	// between a successful binlog append and GTID state-file replacement.
	committed, err := writer.ReadFrom(4)
	if err != nil {
		return nil, err
	}
	for _, event := range committed {
		switch event.Type {
		case EventXAPrepare:
			if event.OnePhase {
				source.Executed.Add(event.GTID)
				if strings.TrimSpace(event.TransactionKey) != "" {
					source.transactionKeys[event.TransactionKey] = event.GTID
				}
			} else {
				if event.XA != nil {
					source.preparedXA[event.XA.Key()] = event.GTID
				}
				if strings.TrimSpace(event.TransactionKey) != "" {
					source.preparedKeys[event.TransactionKey] = event.GTID
				}
			}
			if event.GTID.UUID == uuid && event.GTID.Seq >= source.nextSeq {
				source.nextSeq = event.GTID.Seq + 1
			}
		case EventXACommit:
			if event.XA != nil {
				delete(source.preparedXA, event.XA.Key())
			}
			if strings.TrimSpace(event.TransactionKey) != "" {
				delete(source.preparedKeys, event.TransactionKey)
			}
			source.Executed.Add(event.GTID)
			if event.TerminalGTID != nil {
				source.Executed.Add(*event.TerminalGTID)
			}
			if strings.TrimSpace(event.TransactionKey) != "" {
				committedGTID := event.GTID
				if event.TerminalGTID != nil {
					committedGTID = *event.TerminalGTID
				}
				source.transactionKeys[event.TransactionKey] = committedGTID
			}
			if event.GTID.UUID == uuid && event.GTID.Seq >= source.nextSeq {
				source.nextSeq = event.GTID.Seq + 1
			}
			if event.TerminalGTID != nil && event.TerminalGTID.UUID == uuid && event.TerminalGTID.Seq >= source.nextSeq {
				source.nextSeq = event.TerminalGTID.Seq + 1
			}
		case EventXARollback:
			if event.XA != nil {
				delete(source.preparedXA, event.XA.Key())
			}
			if strings.TrimSpace(event.TransactionKey) != "" {
				delete(source.preparedKeys, event.TransactionKey)
			}
			source.Executed.Add(event.GTID)
			if event.TerminalGTID != nil {
				source.Executed.Add(*event.TerminalGTID)
			}
		case EventCommit:
			source.Executed.Add(event.GTID)
			if strings.TrimSpace(event.TransactionKey) != "" {
				source.transactionKeys[event.TransactionKey] = event.GTID
			}
			if event.GTID.UUID == uuid && event.GTID.Seq >= source.nextSeq {
				source.nextSeq = event.GTID.Seq + 1
			}
		}
	}
	if err := source.persistDurableState(); err != nil {
		return nil, err
	}
	return source, nil
}

func (source *Source) Append(seq uint64, changes []RowChange) ([]BinlogEvent, error) {
	return source.AppendTransaction(seq, changes, nil)
}

func (source *Source) AppendTransaction(seq uint64, changes []RowChange, statements []Statement) ([]BinlogEvent, error) {
	if err := source.validateReady(); err != nil {
		return nil, err
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.appendTransactionLocked(seq, changes, statements)
}

func (source *Source) appendTransactionLocked(seq uint64, changes []RowChange, statements []Statement) ([]BinlogEvent, error) {
	return source.appendTransactionLockedWithKey(seq, changes, statements, "")
}

func (source *Source) appendTransactionLockedWithKey(seq uint64, changes []RowChange, statements []Statement, key string) ([]BinlogEvent, error) {
	if seq == 0 {
		seq = source.nextSeq
	}
	gtid := GTID{UUID: source.UUID, Seq: seq}
	if source.Executed.Contains(gtid) {
		return nil, nil
	}
	events, err := source.Writer.AppendTransactionWithKey(gtid, changes, statements, key)
	if err != nil {
		return nil, err
	}
	next := cloneGTIDSet(source.Executed)
	next.Add(gtid)
	source.Executed = next
	if seq >= source.nextSeq {
		source.nextSeq = seq + 1
	}
	if key != "" {
		source.transactionKeys[key] = gtid
	}
	if err := source.persistDurableState(); err != nil {
		return nil, err
	}
	return events, nil
}

// AppendCommitted appends one committed logical transaction and allocates a
// monotonically increasing source-local GTID sequence.
func (source *Source) AppendCommitted(statements []Statement) ([]BinlogEvent, error) {
	return source.AppendCommittedTransaction(nil, statements)
}

// AppendCommittedTransaction appends one committed transaction containing
// both row images and logical statements under a single source GTID. Row
// images serve native/binlog consumers while statements preserve the current
// SQL replay contract for replicas.
func (source *Source) AppendCommittedTransaction(changes []RowChange, statements []Statement) ([]BinlogEvent, error) {
	return source.AppendTransaction(0, changes, statements)
}

// AppendCommittedTransactionWithKey appends a committed transaction using a
// stable coordinator key. XA retries can receive an error after the native
// binlog append already succeeded; the key makes the retry return the same
// committed transaction instead of allocating a second GTID and replaying
// the row images.
func (source *Source) AppendCommittedTransactionWithKey(key string, changes []RowChange, statements []Statement) ([]BinlogEvent, error) {
	if strings.TrimSpace(key) == "" {
		return source.AppendCommittedTransaction(changes, statements)
	}
	if err := source.validateReady(); err != nil {
		return nil, err
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if gtid, ok := source.transactionKeys[key]; ok && source.Executed.Contains(gtid) {
		return nil, nil
	}
	if gtid, ok := source.transactionKeys[key]; ok && !source.Executed.Contains(gtid) {
		delete(source.transactionKeys, key)
	}
	events, err := source.appendTransactionLockedWithKey(0, changes, statements, key)
	if err != nil {
		return nil, err
	}
	return events, nil
}

// AppendOnePhaseXATransaction appends the native XA_PREPARE_EVENT form for
// XA COMMIT ... ONE PHASE. The one-phase event is already committed and is
// therefore added to Executed without entering preparedXA.
func (source *Source) AppendOnePhaseXATransaction(key string, xid XAIdentity, changes []RowChange, statements []Statement) error {
	if err := source.validateReady(); err != nil {
		return err
	}
	if strings.TrimSpace(xid.GTRID) == "" {
		return fmt.Errorf("XA one-phase commit requires a non-empty gtrid")
	}
	key = strings.TrimSpace(key)
	source.mu.Lock()
	defer source.mu.Unlock()
	if key != "" {
		if gtid, ok := source.transactionKeys[key]; ok && source.Executed.Contains(gtid) {
			return nil
		}
		if _, ok := source.transactionKeys[key]; ok {
			delete(source.transactionKeys, key)
		}
	}
	gtid := GTID{UUID: source.UUID, Seq: source.nextSeq}
	if _, err := source.Writer.AppendOnePhaseXATransaction(gtid, changes, statements, xid, key); err != nil {
		return err
	}
	source.Executed.Add(gtid)
	if key != "" {
		source.transactionKeys[key] = gtid
	}
	if source.nextSeq <= gtid.Seq {
		source.nextSeq = gtid.Seq + 1
	}
	return source.persistDurableState()
}

// PrepareXATransaction appends the durable logical/native XA PREPARE
// boundary. The allocated GTID is reserved but is not added to Executed until
// CommitXATransaction succeeds.
func (source *Source) PrepareXATransaction(key string, xid XAIdentity, changes []RowChange, statements []Statement) error {
	if err := source.validateReady(); err != nil {
		return err
	}
	if strings.TrimSpace(xid.GTRID) == "" {
		return fmt.Errorf("XA prepare requires a non-empty gtrid")
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	xaKey := xid.Key()
	key = strings.TrimSpace(key)
	if gtid, ok := source.preparedXA[xaKey]; ok {
		if key == "" || source.preparedKeys[key] == gtid {
			return nil
		}
		return fmt.Errorf("XA transaction %s is already prepared", xaKey)
	}
	if key != "" {
		if gtid, ok := source.preparedKeys[key]; ok {
			return fmt.Errorf("transaction key %q is already prepared for GTID %s", key, gtid.String())
		}
		if gtid, ok := source.transactionKeys[key]; ok && source.Executed.Contains(gtid) {
			return nil
		}
		if _, ok := source.transactionKeys[key]; ok {
			return fmt.Errorf("transaction key %q is already in use", key)
		}
	}
	gtid := GTID{UUID: source.UUID, Seq: source.nextSeq}
	if _, err := source.Writer.AppendXAPrepare(gtid, changes, statements, xid, key); err != nil {
		return err
	}
	source.preparedXA[xaKey] = gtid
	if key != "" {
		source.preparedKeys[key] = gtid
	}
	if source.nextSeq <= gtid.Seq {
		source.nextSeq = gtid.Seq + 1
	}
	return source.persistDurableState()
}

// ImportPreparedXA carries unresolved XA branches from a replica into a new
// source during promotion. The prepared GTID remains the upstream identity so
// a later XA COMMIT can complete the same transaction instead of allocating a
// new local transaction or silently losing the in-doubt branch.
func (source *Source) ImportPreparedXA(prepared map[string]BinlogEvent) error {
	if err := source.validateReady(); err != nil {
		return err
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	for _, event := range prepared {
		if event.XA == nil {
			return fmt.Errorf("prepared XA event is missing XID")
		}
		if event.GTID.UUID == "" || event.GTID.Seq == 0 {
			return fmt.Errorf("prepared XA event has invalid GTID %s", event.GTID.String())
		}
		xaKey := event.XA.Key()
		if existing, ok := source.preparedXA[xaKey]; ok {
			if existing == event.GTID {
				continue
			}
			return fmt.Errorf("prepared XA transaction %s conflicts with GTID %s", xaKey, existing.String())
		}
		if source.Executed.Contains(event.GTID) {
			continue
		}
		transactionKey := strings.TrimSpace(event.TransactionKey)
		if transactionKey != "" {
			if existing, ok := source.preparedKeys[transactionKey]; ok && existing != event.GTID {
				return fmt.Errorf("prepared transaction key %q conflicts with GTID %s", transactionKey, existing.String())
			}
		}
		if _, err := source.Writer.AppendXAPrepare(event.GTID, event.Changes, event.Statements, *event.XA, event.TransactionKey); err != nil {
			return err
		}
		source.preparedXA[xaKey] = event.GTID
		if transactionKey != "" {
			source.preparedKeys[transactionKey] = event.GTID
		}
		if event.GTID.UUID == source.UUID && event.GTID.Seq >= source.nextSeq {
			source.nextSeq = event.GTID.Seq + 1
		}
	}
	return source.persistDurableState()
}

// ImportRelayEvents restores the durable relay history on a newly promoted
// source. The GTIDs remain upstream identities, while the native frames are
// regenerated under the promoted server-id so both logical and native
// consumers can continue from the recovered history.
func (source *Source) ImportRelayEvents(events []BinlogEvent) error {
	if err := source.validateReady(); err != nil {
		return err
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if err := source.Writer.ImportEvents(events); err != nil {
		return err
	}
	for _, event := range events {
		switch event.Type {
		case EventXAPrepare:
			if event.OnePhase {
				source.Executed.Add(event.GTID)
				if key := strings.TrimSpace(event.TransactionKey); key != "" {
					source.transactionKeys[key] = event.GTID
				}
			} else {
				if event.XA != nil {
					source.preparedXA[event.XA.Key()] = event.GTID
				}
				if key := strings.TrimSpace(event.TransactionKey); key != "" {
					source.preparedKeys[key] = event.GTID
				}
			}
		case EventXACommit:
			if event.XA != nil {
				delete(source.preparedXA, event.XA.Key())
			}
			if key := strings.TrimSpace(event.TransactionKey); key != "" {
				delete(source.preparedKeys, key)
				committedGTID := event.GTID
				if event.TerminalGTID != nil {
					committedGTID = *event.TerminalGTID
				}
				source.transactionKeys[key] = committedGTID
			}
			source.Executed.Add(event.GTID)
			if event.TerminalGTID != nil {
				source.Executed.Add(*event.TerminalGTID)
			}
		case EventXARollback:
			if event.XA != nil {
				delete(source.preparedXA, event.XA.Key())
			}
			if key := strings.TrimSpace(event.TransactionKey); key != "" {
				delete(source.preparedKeys, key)
			}
			source.Executed.Add(event.GTID)
			if event.TerminalGTID != nil {
				source.Executed.Add(*event.TerminalGTID)
			}
		case EventCommit:
			source.Executed.Add(event.GTID)
			if key := strings.TrimSpace(event.TransactionKey); key != "" {
				source.transactionKeys[key] = event.GTID
			}
		}
		if event.GTID.UUID == source.UUID && event.GTID.Seq >= source.nextSeq {
			source.nextSeq = event.GTID.Seq + 1
		}
		if event.TerminalGTID != nil && event.TerminalGTID.UUID == source.UUID && event.TerminalGTID.Seq >= source.nextSeq {
			source.nextSeq = event.TerminalGTID.Seq + 1
		}
	}
	return source.persistDurableState()
}

// CommitXATransaction appends XA COMMIT for a previously prepared XID and
// atomically advances the source's executed GTID/key state. Repeating the
// same commit is a no-op after the durable commit event has been observed.
func (source *Source) CommitXATransaction(key string, xid XAIdentity) error {
	if err := source.validateReady(); err != nil {
		return err
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	key = strings.TrimSpace(key)
	preparedGTID, ok := source.preparedXA[xid.Key()]
	if !ok {
		if strings.TrimSpace(key) != "" {
			if committed, exists := source.transactionKeys[key]; exists && source.Executed.Contains(committed) {
				return nil
			}
		}
		return fmt.Errorf("XA transaction %s is not prepared", xid.Key())
	}
	terminalGTID := GTID{UUID: source.UUID, Seq: source.nextSeq}
	if _, err := source.Writer.AppendXACommitWithPreparedGTID(terminalGTID, preparedGTID, xid, key); err != nil {
		return err
	}
	delete(source.preparedXA, xid.Key())
	source.Executed.Add(preparedGTID)
	source.Executed.Add(terminalGTID)
	if key != "" {
		delete(source.preparedKeys, key)
		source.transactionKeys[key] = terminalGTID
	}
	if source.nextSeq <= terminalGTID.Seq {
		source.nextSeq = terminalGTID.Seq + 1
	}
	return source.persistDurableState()
}

// RollbackXATransaction appends XA ROLLBACK and publishes both the reserved
// prepare GTID and the separate terminal GTID. No row changes are applied.
func (source *Source) RollbackXATransaction(key string, xid XAIdentity) error {
	if err := source.validateReady(); err != nil {
		return err
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	key = strings.TrimSpace(key)
	preparedGTID, ok := source.preparedXA[xid.Key()]
	if !ok {
		if key != "" {
			if rolledBack, exists := source.transactionKeys[key]; exists && source.Executed.Contains(rolledBack) {
				return nil
			}
		}
		return fmt.Errorf("XA transaction %s is not prepared", xid.Key())
	}
	terminalGTID := GTID{UUID: source.UUID, Seq: source.nextSeq}
	if _, err := source.Writer.AppendXARollbackWithPreparedGTID(terminalGTID, preparedGTID, xid, key); err != nil {
		return err
	}
	delete(source.preparedXA, xid.Key())
	if key != "" {
		delete(source.preparedKeys, key)
	}
	source.Executed.Add(preparedGTID)
	source.Executed.Add(terminalGTID)
	if key != "" {
		source.transactionKeys[key] = terminalGTID
	}
	if source.nextSeq <= terminalGTID.Seq {
		source.nextSeq = terminalGTID.Seq + 1
	}
	return source.persistDurableState()
}

func (source *Source) Dump(position uint64) ([]BinlogEvent, error) {
	if err := source.validateReady(); err != nil {
		return nil, err
	}
	return source.Writer.ReadFrom(position)
}

func (source *Source) validateReady() error {
	if source == nil {
		return fmt.Errorf("replication source is nil")
	}
	if source.Writer == nil {
		return fmt.Errorf("replication source writer is nil")
	}
	return nil
}

// DumpFile returns the logical events belonging to one rotated native binlog
// file. The internal stream records ROTATE boundaries, so filtering remains
// durable even though replicas consume the JSONL recovery stream.
func (source *Source) DumpFile(logName string, position uint64) ([]BinlogEvent, error) {
	if err := source.validateReady(); err != nil {
		return nil, err
	}
	target, err := nativeBinlogFileIndex(logName)
	if err != nil {
		return nil, err
	}
	knownFile := false
	for _, file := range source.Writer.NativeFiles() {
		if file.Name == logName {
			knownFile = true
			break
		}
	}
	if !knownFile {
		return nil, fmt.Errorf("native binlog file %q does not exist", logName)
	}
	events, err := source.Dump(4)
	if err != nil {
		return nil, err
	}
	current := uint64(1)
	filtered := make([]BinlogEvent, 0)
	for _, event := range events {
		if event.Type == EventRotate {
			if current == target && event.Position >= position {
				filtered = append(filtered, event)
			}
			current++
			continue
		}
		if current == target && event.Position >= position {
			filtered = append(filtered, event)
		}
	}
	return filtered, nil
}

// DumpFileWithNativePositions returns the logical events selected from one
// file while attaching the corresponding physical native offsets. This lets
// SHOW BINLOG EVENTS honor MySQL's file-position contract without changing
// the JSONL positions used by internal replica recovery.
func (source *Source) DumpFileWithNativePositions(logName string, position uint64) ([]BinlogEvent, error) {
	if err := source.validateReady(); err != nil {
		return nil, err
	}
	logical, err := source.DumpFile(logName, 4)
	if err != nil {
		return nil, err
	}
	native, err := source.Writer.NativeEvents(logName)
	if err != nil {
		return nil, err
	}
	result := make([]BinlogEvent, 0, len(logical))
	logicalIndex := 0
	physicalIndex := 0
	for _, physical := range native {
		if physical.Type == 15 || physical.Type == 35 {
			continue
		}
		if logicalIndex >= len(logical) {
			break
		}
		event := logical[logicalIndex]
		physicalIndex++
		if position > 4 && physical.Position < position {
			if physicalIndex >= nativePhysicalEventCount(event) {
				logicalIndex++
				physicalIndex = 0
			}
			continue
		}
		mapped := event
		mapped.NativeFile = logName
		mapped.NativePosition = physical.Position
		mapped.NativeEndPosition = physical.EndPosition
		mapped.NativeEventType = nativeEventTypeName(physical.Type)
		result = append(result, mapped)
		if physicalIndex >= nativePhysicalEventCount(event) {
			logicalIndex++
			physicalIndex = 0
		}
	}
	return result, nil
}

func nativeEventTypeName(eventType byte) string {
	switch eventType {
	case 1:
		return "Start_v3"
	case 2:
		return "Query"
	case 3:
		return "Stop"
	case 4:
		return "Rotate"
	case 5:
		return "Intvar"
	case 6:
		return "Load"
	case 7:
		return "Slave"
	case 8:
		return "Create_file"
	case 9:
		return "Append_block"
	case 10:
		return "Exec_load"
	case 11:
		return "Delete_file"
	case 12:
		return "New_load"
	case 13:
		return "Rand"
	case 14:
		return "User_var"
	case 15:
		return "Format_desc"
	case 16:
		return "Xid"
	case 17:
		return "Begin_load_query"
	case 18:
		return "Execute_load_query"
	case 19:
		return "Table_map"
	case 20:
		return "Pre_gwrite_rows"
	case 21:
		return "Pre_gupdate_rows"
	case 22:
		return "Pre_gdelete_rows"
	case 23:
		return "Write_rows"
	case 24:
		return "Update_rows"
	case 25:
		return "Delete_rows"
	case 26:
		return "Incident"
	case 27:
		return "Heartbeat"
	case 28:
		return "Ignorable"
	case 29:
		return "Rows_query"
	case 30:
		return "Write_rows"
	case 31:
		return "Update_rows"
	case 32:
		return "Delete_rows"
	case 33:
		return "Gtid"
	case 34:
		return "Anonymous_Gtid"
	case 35:
		return "Previous_gtids"
	case 36:
		return "Transaction_context"
	case 37:
		return "View_change"
	case 38:
		return "Xa_prepare"
	case 39:
		return "Partial_update_rows"
	case 40:
		return "Transaction_payload"
	case 41:
		return "Heartbeat_log_event_v2"
	case 42:
		return "Gtid_tagged"
	default:
		return ""
	}
}

func nativePhysicalEventCount(event BinlogEvent) int {
	if event.Type == EventRow && len(event.Changes) > 0 {
		groups := 0
		for index := 0; index < len(event.Changes); {
			first := event.Changes[index]
			groups++
			index++
			for index < len(event.Changes) && nativeChangesCanShareRowEvent(first, event.Changes[index]) {
				index++
			}
		}
		return groups * 2
	}
	return 1
}

// NativeDumpFile returns checksum-validated physical events for a native
// binlog dump.  It filters complete GTID transactions without converting the
// events through the internal JSONL position space, so callers can stream the
// original physical offsets and row-event frames to native replication
// clients.
func (source *Source) NativeDumpFile(logName string, position uint64, executed GTIDSet) ([]NativeBinlogEvent, uint64, error) {
	return source.NativeDumpFileWithIntervals(logName, position, GTIDIntervalsFromSet(executed))
}

// NativeDumpFileWithIntervals is the interval-preserving form of
// NativeDumpFile. It avoids expanding a replica's COM_BINLOG_DUMP_GTID set
// into one entry per sequence number while retaining complete-transaction
// filtering and observed GTID advancement.
func (source *Source) NativeDumpFileWithIntervals(logName string, position uint64, executed GTIDIntervals) ([]NativeBinlogEvent, uint64, error) {
	if err := source.validateReady(); err != nil {
		return nil, position, err
	}
	knownFile := false
	for _, file := range source.Writer.NativeFiles() {
		if file.Name == logName {
			knownFile = true
			break
		}
	}
	if !knownFile {
		return nil, position, fmt.Errorf("native binlog file %q does not exist", logName)
	}
	events, err := source.Writer.NativeEvents(logName)
	if err != nil {
		return nil, position, err
	}
	next := position
	currentGTID := uint64(0)
	skipTransaction := false
	partialTransaction := false
	skipByPosition := false
	result := make([]NativeBinlogEvent, 0, len(events))
	for _, event := range events {
		if event.EndPosition > next {
			next = event.EndPosition
		}
		switch event.Type {
		case 15, 35: // Preserve native file metadata when the dump starts at file position 4.
			if position <= 4 {
				result = appendNativeEventAtPosition(result, event, position)
			}
			continue
		case 33: // GTID_EVENT
			body := nativeEventBody(event)
			seq, ok := nativeGTIDSequenceBody(body)
			if !ok {
				return nil, next, fmt.Errorf("invalid native GTID event at position %d", event.Position)
			}
			currentGTID = seq
			partialTransaction = position > event.Position
			skipByPosition = partialTransaction
			skipTransaction = partialTransaction || nativeGTIDSetContainsBody(executed, body, seq)
			if !skipTransaction {
				result = appendNativeEventAtPosition(result, event, position)
			}
			if executed != nil {
				addNativeGTIDToSetBody(executed, source.UUID, body, seq)
			}
		case 42: // GTID_TAGGED_LOG_EVENT
			gtid, err := decodeNativeTaggedGTID(nativeEventBody(event), source.UUID)
			if err != nil {
				return nil, next, err
			}
			currentGTID = gtid.Seq
			partialTransaction = position > event.Position
			skipByPosition = partialTransaction
			skipTransaction = partialTransaction || logicalGTIDSetContains(executed, gtid)
			if !skipTransaction {
				result = appendNativeEventAtPosition(result, event, position)
			}
			if executed != nil {
				executed.Add(gtid.UUID, gtid.Seq)
			}
		case 34: // ANONYMOUS_GTID_EVENT
			gtid := nativeAnonymousGTID(event, event.Raw)
			currentGTID = gtid.Seq
			partialTransaction = position > event.Position
			skipByPosition = partialTransaction
			skipTransaction = partialTransaction || logicalGTIDSetContains(executed, gtid)
			if !skipTransaction {
				result = appendNativeEventAtPosition(result, event, position)
			}
			if executed != nil {
				executed.Add(gtid.UUID, gtid.Seq)
			}
		case 2: // QUERY_EVENT, including the terminal query of a native XA branch.
			body := nativeEventBody(event)
			statement, _, queryErr := decodeNativeQuery(body)
			if queryErr != nil {
				return nil, next, queryErr
			}
			if _, _, ok := nativeXAQuery(statement); ok {
				// XA COMMIT/ROLLBACK is a terminal event after XA_PREPARE,
				// not a second row transaction. A position immediately after
				// PREPARE must therefore retain it even though the preceding
				// GTID transaction began before the resume point. A GTID that
				// is already executed still suppresses the terminal event.
				if !(skipTransaction && !skipByPosition) || skipByPosition {
					result = appendNativeEventAtPosition(result, event, position)
				}
				currentGTID = 0
				skipTransaction = false
				partialTransaction = false
				skipByPosition = false
				continue
			}
			if currentGTID == 0 || skipTransaction {
				continue
			}
			result = appendNativeEventAtPosition(result, event, position)
		case 16: // XID_EVENT
			if !skipTransaction {
				result = appendNativeEventAtPosition(result, event, position)
			}
			currentGTID = 0
			skipTransaction = false
			partialTransaction = false
			skipByPosition = false
		default:
			if currentGTID != 0 && skipTransaction {
				continue
			}
			result = appendNativeEventAtPosition(result, event, position)
		}
	}
	return result, next, nil
}

// NativeDumpFrom returns a continuous physical dump beginning at logName and
// position and continuing through later rotated files. The executed interval
// set is shared across files, so a GTID transaction is filtered exactly once
// even when a caller resumes at a file boundary.
func (source *Source) NativeDumpFrom(logName string, position uint64, executed GTIDIntervals) ([]NativeBinlogEvent, error) {
	if err := source.validateReady(); err != nil {
		return nil, err
	}
	target, err := nativeBinlogFileIndex(logName)
	if err != nil {
		return nil, err
	}
	files := source.Writer.NativeFiles()
	result := make([]NativeBinlogEvent, 0)
	found := false
	for _, file := range files {
		index, err := nativeBinlogFileIndex(file.Name)
		if err != nil || index < target {
			continue
		}
		if index == target {
			found = true
		}
		start := uint64(4)
		if index == target {
			start = position
		}
		events, _, err := source.NativeDumpFileWithIntervals(file.Name, start, executed)
		if err != nil {
			return nil, err
		}
		result = append(result, events...)
	}
	if !found {
		return nil, fmt.Errorf("native binlog file %q does not exist", logName)
	}
	return result, nil
}

// DecodeNativeDumpFrom provides a transaction-oriented view over the same
// physical stream returned by NativeDumpFrom. Callers should resume at a
// transaction boundary so a GTID_EVENT is available for every ROWS_EVENT.
func (source *Source) DecodeNativeDumpFrom(logName string, position uint64, executed GTIDIntervals) ([]BinlogEvent, error) {
	return source.DecodeNativeDumpFromWithResolver(logName, position, executed, nil)
}

// DecodeNativeDumpFromWithResolver is the schema-aware form of
// DecodeNativeDumpFrom. The resolver is used for standard TABLE_MAP frames
// that do not carry column names, allowing a consumer to bind row images to
// its local dictionary before they are applied.
func (source *Source) DecodeNativeDumpFromWithResolver(logName string, position uint64, executed GTIDIntervals, resolver NativeTableMapResolver) ([]BinlogEvent, error) {
	return source.decodeNativeDumpFromWithResolvers(logName, position, executed, resolver, nil)
}

// DecodeNativeDumpFromWithSchemaResolver is the type-aware form of
// DecodeNativeDumpFromWithResolver. It preserves local SQL type distinctions
// when a standard TABLE_MAP_EVENT omits column names and SQL types.
func (source *Source) DecodeNativeDumpFromWithSchemaResolver(logName string, position uint64, executed GTIDIntervals, resolver NativeTableMapSchemaResolver) ([]BinlogEvent, error) {
	return source.decodeNativeDumpFromWithResolvers(logName, position, executed, nil, resolver)
}

func (source *Source) decodeNativeDumpFromWithResolvers(logName string, position uint64, executed GTIDIntervals, resolver NativeTableMapResolver, schemaResolver NativeTableMapSchemaResolver) ([]BinlogEvent, error) {
	events, err := source.NativeDumpFrom(logName, position, executed)
	if err != nil {
		return nil, err
	}
	decoder := NewNativeBinlogDecoder()
	if source != nil {
		decoder = NewNativeBinlogDecoderForSource(source.UUID)
	}
	if schemaResolver != nil {
		decoder.SetTableMapSchemaResolver(schemaResolver)
	} else {
		decoder.SetTableMapResolver(resolver)
	}
	return decoder.DecodeTransactions(events)
}

func appendNativeEventAtPosition(events []NativeBinlogEvent, event NativeBinlogEvent, position uint64) []NativeBinlogEvent {
	if position > 4 && event.Position < position {
		return events
	}
	return append(events, event)
}

func nativeGTIDSequence(raw []byte) (uint64, bool) {
	if len(raw) < nativeEventHeaderLength+nativeChecksumLength || raw[4] != 33 {
		return 0, false
	}
	return nativeGTIDSequenceBody(raw[nativeEventHeaderLength : len(raw)-nativeChecksumLength])
}

func nativeGTIDSequenceBody(body []byte) (uint64, bool) {
	if len(body) < 1+16+8 {
		return 0, false
	}
	return binary.LittleEndian.Uint64(body[1+16 : 1+16+8]), true
}

func logicalGTIDSetContains(set GTIDIntervals, gtid GTID) bool {
	if set.Contains(gtid) {
		return true
	}
	return set.Contains(GTID{UUID: nativeGTIDUUID(gtid.UUID), Seq: gtid.Seq})
}

func nativeGTIDSetContains(set GTIDIntervals, raw []byte, sequence uint64) bool {
	if len(raw) < nativeEventHeaderLength+nativeChecksumLength {
		return false
	}
	return nativeGTIDSetContainsBody(set, raw[nativeEventHeaderLength:len(raw)-nativeChecksumLength], sequence)
}

func nativeGTIDSetContainsBody(set GTIDIntervals, body []byte, sequence uint64) bool {
	if set == nil || len(body) < 1+16 {
		return false
	}
	sid := body[1 : 1+16]
	for uuid := range set {
		if set.Contains(GTID{UUID: uuid, Seq: sequence}) && bytes.Equal(nativeGTIDSID(uuid), sid) {
			return true
		}
	}
	return false
}

func addNativeGTIDToSet(set GTIDIntervals, sourceUUID string, raw []byte, sequence uint64) {
	if len(raw) < nativeEventHeaderLength+nativeChecksumLength {
		return
	}
	addNativeGTIDToSetBody(set, sourceUUID, raw[nativeEventHeaderLength:len(raw)-nativeChecksumLength], sequence)
}

func addNativeGTIDToSetBody(set GTIDIntervals, sourceUUID string, body []byte, sequence uint64) {
	if set == nil || len(body) < 1+16 {
		return
	}
	sid := body[1 : 1+16]
	if bytes.Equal(nativeGTIDSID(sourceUUID), sid) {
		set.Add(sourceUUID, sequence)
	}
}

func nativeBinlogFileIndex(logName string) (uint64, error) {
	name := strings.TrimSpace(logName)
	if !strings.HasPrefix(name, "binlog.") {
		return 0, fmt.Errorf("invalid binlog file name %q", logName)
	}
	suffix := strings.TrimPrefix(name, "binlog.")
	if len(suffix) != 6 {
		return 0, fmt.Errorf("invalid binlog file name %q", logName)
	}
	index, err := strconv.ParseUint(suffix, 10, 32)
	if err != nil || index == 0 {
		return 0, fmt.Errorf("invalid binlog file name %q", logName)
	}
	return index, nil
}

// filteredDump returns complete transactions that are not present in the
// replica's executed GTID set. Filtering is performed at transaction
// boundaries so a position rollback can never yield a partial transaction.
func (source *Source) filteredDump(position uint64, executed string) ([]BinlogEvent, error) {
	events, _, err := source.DumpWithGTIDPosition(position, executed)
	return events, err
}

// DumpWithGTIDPosition returns GTID-filtered events and the position after
// the raw events observed from the durable stream. Advancing from raw events
// is important when every event is already executed by a replica; otherwise
// the replica would repeatedly request the same filtered batch forever.
func (source *Source) DumpWithGTIDPosition(position uint64, executed string) ([]BinlogEvent, uint64, error) {
	events, err := source.Dump(position)
	if err != nil {
		return nil, position, err
	}
	next := position
	for _, event := range events {
		if event.Position >= next {
			next = event.Position + 1
		}
	}
	if strings.TrimSpace(executed) == "" {
		return events, next, nil
	}
	set, err := ParseGTIDIntervals(executed)
	if err != nil {
		return nil, position, err
	}
	return source.filteredDumpIntervals(events, set), next, nil
}

func (source *Source) filteredDumpSet(events []BinlogEvent, set GTIDSet) []BinlogEvent {
	return source.filteredDumpIntervals(events, GTIDIntervalsFromSet(set))
}

func (source *Source) filteredDumpIntervals(events []BinlogEvent, set GTIDIntervals) []BinlogEvent {
	if len(set) == 0 {
		return events
	}
	filtered := make([]BinlogEvent, 0, len(events))
	for _, transaction := range splitBinlogTransactions(events) {
		if len(transaction) == 0 {
			continue
		}
		if transaction[0].Type == EventRotate || !logicalGTIDSetContains(set, transaction[0].GTID) {
			filtered = append(filtered, transaction...)
		}
	}
	return filtered
}

// FilterEventsByGTID removes complete transactions already present in set.
// It is used by the native COM_BINLOG_DUMP_GTID protocol adapter.
func (source *Source) FilterEventsByGTID(events []BinlogEvent, set GTIDSet) []BinlogEvent {
	return source.filteredDumpSet(events, set)
}

// FilterEventsByGTIDIntervals is the range-preserving form used by protocol
// adapters that already decoded an upstream GTID interval set.
func (source *Source) FilterEventsByGTIDIntervals(events []BinlogEvent, set GTIDIntervals) []BinlogEvent {
	return source.filteredDumpIntervals(events, set)
}

// Position returns the current logical binlog position.
func (source *Source) Position() uint64 {
	if source == nil || source.Writer == nil {
		return 4
	}
	return source.Writer.Position()
}

// NextPosition returns the next position available in the source stream.
func (source *Source) NextPosition() uint64 {
	if source == nil || source.Writer == nil {
		return 4
	}
	return source.Writer.NextPosition()
}

// ServerID returns the source server-id used for native binlog headers.
func (source *Source) ServerID() uint32 {
	if source == nil || source.Writer == nil {
		return 0
	}
	return source.Writer.ServerID()
}

// FileSize returns the durable native binlog size used by administrative
// SHOW BINARY LOGS output.
func (source *Source) FileSize() uint64 {
	if source == nil || source.Writer == nil {
		return 0
	}
	return source.Writer.NativeFileSize()
}

// NativeCurrentFilePosition returns the active native file and durable
// physical end position used by SHOW MASTER/SOURCE STATUS.
func (source *Source) NativeCurrentFilePosition() (string, uint64) {
	if source == nil || source.Writer == nil {
		return "", 4
	}
	return source.Writer.NativeCurrentFilePosition()
}

// NativeGTIDPosition resolves a logical GTID identity to the physical native
// event range recorded by this source. It is the source-level entry point for
// PITR tools that start from GTID rather than a raw file offset.
func (source *Source) NativeGTIDPosition(uuidOrSID string, sequence uint64) (NativeGTIDIndexEntry, bool) {
	if source == nil || source.Writer == nil {
		return NativeGTIDIndexEntry{}, false
	}
	return source.Writer.NativeGTIDPosition(uuidOrSID, sequence)
}

// NativeFiles returns the durable native binlog files in log order.
func (source *Source) NativeFiles() []NativeBinlogFile {
	if source == nil || source.Writer == nil {
		return nil
	}
	return source.Writer.NativeFiles()
}

func (source *Source) Rotate() (BinlogEvent, error) {
	if err := source.validateReady(); err != nil {
		return BinlogEvent{}, err
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.Writer.Rotate()
}

// ResetMaster atomically resets the source's durable logical binlog and GTID
// execution history to an empty stream.
func (source *Source) ResetMaster() error {
	return source.ResetMasterTo(1)
}

// ResetMasterTo is the source-side implementation of MySQL 8.4's RESET
// BINARY LOGS AND GTIDS [TO n]. It resets logical/GTID state and starts the
// native binlog sequence at the requested index.
func (source *Source) ResetMasterTo(index uint32) error {
	if source == nil || source.Writer == nil {
		return nil
	}
	if index == 0 {
		return fmt.Errorf("binlog reset file index must be greater than zero")
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if err := source.Writer.ResetTo(index); err != nil {
		return err
	}
	source.Executed = GTIDSet{}
	source.nextSeq = 1
	source.transactionKeys = map[string]GTID{}
	source.preparedXA = map[string]GTID{}
	source.preparedKeys = map[string]GTID{}
	return source.persistDurableState()
}

// PurgeBinaryLogsTo removes native binlog files older than the requested
// target while preserving the target and the current source coordinates.
func (source *Source) PurgeBinaryLogsTo(logName string) error {
	if source == nil || source.Writer == nil {
		return nil
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.Writer.PurgeTo(logName)
}

// PurgeBinaryLogsBefore removes source native binlog files older than the
// requested timestamp while retaining the file that crosses the cutoff.
func (source *Source) PurgeBinaryLogsBefore(cutoff time.Time) error {
	if source == nil || source.Writer == nil {
		return nil
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.Writer.PurgeBefore(cutoff)
}

func (source *Source) persist() error {
	return source.persistDurableState()
}

func (source *Source) persistDurableState() error {
	if source.durableStateHook != nil {
		if err := source.durableStateHook(); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Dir(source.durableStatePath), 0755); err != nil {
		return err
	}
	durable := sourceDurableState{
		Executed:        cloneGTIDSet(source.Executed),
		TransactionKeys: cloneTransactionKeys(source.transactionKeys),
		PreparedXA:      cloneTransactionKeys(source.preparedXA),
		PreparedKeys:    cloneTransactionKeys(source.preparedKeys),
	}
	raw, err := json.MarshalIndent(durable, "", "  ")
	if err != nil {
		return err
	}
	if err := writeReplicationFileAtomic(source.durableStatePath, raw); err != nil {
		return err
	}
	// Preserve the pre-existing files for tools and upgrades that still read
	// them directly. They are deliberately written after the atomic state.
	if err := source.persistSet(source.Executed); err != nil {
		return err
	}
	return source.persistTransactionKeys()
}

func (source *Source) persistSet(set GTIDSet) error {
	if err := os.MkdirAll(filepath.Dir(source.statePath), 0755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(set, "", "  ")
	if err != nil {
		return err
	}
	return writeReplicationFileAtomic(source.statePath, raw)
}

func (source *Source) persistTransactionKeys() error {
	if err := os.MkdirAll(filepath.Dir(source.keyPath), 0755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(source.transactionKeys, "", "  ")
	if err != nil {
		return err
	}
	return writeReplicationFileAtomic(source.keyPath, raw)
}

func cloneGTIDSet(source GTIDSet) GTIDSet {
	clone := GTIDSet{}
	for uuid, sequences := range source {
		clone[uuid] = map[uint64]struct{}{}
		for sequence := range sequences {
			clone[uuid][sequence] = struct{}{}
		}
	}
	return clone
}

func cloneTransactionKeys(source map[string]GTID) map[string]GTID {
	clone := map[string]GTID{}
	for key, gtid := range source {
		clone[key] = gtid
	}
	return clone
}

func writeReplicationFileAtomic(path string, raw []byte) error {
	temporary := path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer os.Remove(temporary)
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if replicationFileSyncHook != nil {
		if err := replicationFileSyncHook(file); err != nil {
			_ = file.Close()
			return err
		}
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return nil
}
