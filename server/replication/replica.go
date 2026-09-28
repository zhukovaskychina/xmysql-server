package replication

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Replica struct {
	mu                 sync.Mutex
	Executed           GTIDSet
	statePath          string
	filtersPath        string
	filterConfig       ReplicationFilterConfig
	filterConfiguredBy string
	filterActiveSince  time.Time
	filterCounters     map[string]uint64
	AppliedRows        []RowChange
	relayEvents        []BinlogEvent
	nativeRelayEvents  []NativeBinlogEvent
	nativeTableMaps    []NativeBinlogEvent
	preparedXA         map[string]BinlogEvent
	// appliedTransactions is an intermediate durable marker written after a
	// storage callback succeeds but before the executed-GTID replacement. It
	// lets statement-only replay recover from that state replacement window.
	appliedTransactions map[string]bool
	LastSourceFile      string
	LastSourcePosition  uint64
	LastAppliedAt       time.Time
	LastError           string
	NetworkRetries      uint64
	// statePersistHook is test-only fault injection for the crash window
	// between storage apply and replica state replacement.
	statePersistHook func() error
	// ApplyRows is called exactly once for each newly committed GTID that
	// contains row images. When configured, it is preferred over
	// ApplyStatements so a storage engine can apply the row image atomically.
	ApplyRows func([]RowChange) error `json:"-"`
	// ApplyRowsWithID is the identity-aware form of ApplyRows. The GTID must be
	// treated as the storage transaction identity so the target can make its
	// own commit/replay decision using the same durable transaction boundary.
	ApplyRowsWithID func(string, []RowChange) error `json:"-"`
	// ApplyStatements is called exactly once for each newly committed GTID.
	// The callback must apply the complete statement batch atomically.
	ApplyStatements func([]Statement) error `json:"-"`
	// ApplyStatementsWithID is the identity-aware form of ApplyStatements.
	ApplyStatementsWithID func(string, []Statement) error `json:"-"`
}

type replicaState struct {
	Executed            GTIDSet                `json:"executed"`
	AppliedRows         []RowChange            `json:"applied_rows"`
	PreparedXA          map[string]BinlogEvent `json:"prepared_xa,omitempty"`
	RelayEvents         []BinlogEvent          `json:"relay_events,omitempty"`
	NativeRelayEvents   []NativeBinlogEvent    `json:"native_relay_events,omitempty"`
	NativeTableMaps     []NativeBinlogEvent    `json:"native_table_maps,omitempty"`
	AppliedTransactions map[string]bool        `json:"applied_transactions,omitempty"`
	SourceFile          string                 `json:"source_file,omitempty"`
	SourcePosition      uint64                 `json:"source_position,omitempty"`
}

type replicaSourcePosition struct {
	File     string
	Position uint64
}

func NewReplica(dataDir string) (*Replica, error) {
	statePath := filepath.Join(dataDir, "replication", "replica_gtid.json")
	filtersPath := filepath.Join(dataDir, "replication", "filters.json")
	replica := &Replica{Executed: GTIDSet{}, statePath: statePath, filtersPath: filtersPath, AppliedRows: []RowChange{}, relayEvents: []BinlogEvent{}, nativeRelayEvents: []NativeBinlogEvent{}, nativeTableMaps: []NativeBinlogEvent{}, preparedXA: map[string]BinlogEvent{}, appliedTransactions: map[string]bool{}, filterCounters: map[string]uint64{}}
	if raw, err := os.ReadFile(statePath); err == nil {
		var state replicaState
		if json.Unmarshal(raw, &state) == nil && state.Executed != nil {
			replica.Executed = state.Executed
			replica.AppliedRows = state.AppliedRows
			replica.relayEvents = append([]BinlogEvent(nil), state.RelayEvents...)
			replica.nativeRelayEvents = cloneNativeRelayEvents(state.NativeRelayEvents)
			replica.nativeTableMaps = cloneNativeRelayEvents(state.NativeTableMaps)
			replica.preparedXA = cloneNativePreparedXA(state.PreparedXA)
			replica.appliedTransactions = cloneAppliedTransactions(state.AppliedTransactions)
			replica.LastSourceFile = state.SourceFile
			replica.LastSourcePosition = state.SourcePosition
		} else {
			// Read the pre-transactional state format for upgrades.
			_ = json.Unmarshal(raw, &replica.Executed)
		}
	}
	if raw, err := os.ReadFile(filtersPath); err == nil {
		var filters persistedReplicationFilters
		if json.Unmarshal(raw, &filters) == nil {
			replica.filterConfig = normalizeReplicationFilterConfig(filters.Config)
			replica.filterConfiguredBy = filters.ConfiguredBy
			replica.filterActiveSince = filters.ActiveSince
			if filters.Counters != nil {
				replica.filterCounters = filters.Counters
			}
		}
	}
	return replica, nil
}

func (replica *Replica) Apply(events []BinlogEvent) error {
	return replica.applyWithPreparedXA(events, nil)
}

// applyWithPreparedXA commits decoded transactions and the native prepared-XA
// snapshot in one durable replica-state replacement. This prevents a
// restart from observing the GTID/applied-row side of a native stream without
// also observing its unresolved XA prepare records.
func (replica *Replica) applyWithPreparedXA(events []BinlogEvent, preparedXA map[string]BinlogEvent) error {
	return replica.applyWithPreparedXAAndNativeRelay(events, preparedXA, nil, false)
}

func (replica *Replica) applyWithPreparedXAAndNativeRelay(events []BinlogEvent, preparedXA map[string]BinlogEvent, nativeRelay []NativeBinlogEvent, replaceNativeRelay bool) error {
	return replica.applyWithPreparedXAAndNativeRelayAtSource(events, preparedXA, nativeRelay, replaceNativeRelay, nil)
}

func (replica *Replica) applyWithPreparedXAAndNativeRelayAtSource(events []BinlogEvent, preparedXA map[string]BinlogEvent, nativeRelay []NativeBinlogEvent, replaceNativeRelay bool, source *replicaSourcePosition) error {
	return replica.applyWithPreparedXAAndNativeRelayAtSourceAndTableMaps(events, preparedXA, nativeRelay, replaceNativeRelay, nil, source)
}

func (replica *Replica) applyWithPreparedXAAndNativeRelayAtSourceAndTableMaps(events []BinlogEvent, preparedXA map[string]BinlogEvent, nativeRelay []NativeBinlogEvent, replaceNativeRelay bool, nativeTableMaps []NativeBinlogEvent, source *replicaSourcePosition) error {
	replica.mu.Lock()
	defer replica.mu.Unlock()
	if replica.preparedXA == nil {
		replica.preparedXA = make(map[string]BinlogEvent)
	}
	pending := make(map[string][]RowChange)
	pendingStatements := make(map[string][]Statement)
	nextExecuted := cloneGTIDSet(replica.Executed)
	nextRows := append([]RowChange(nil), replica.AppliedRows...)
	nextRelayEvents := append([]BinlogEvent(nil), replica.relayEvents...)
	nextNativeRelayEvents := cloneNativeRelayEvents(replica.nativeRelayEvents)
	nextNativeTableMaps := cloneNativeRelayEvents(replica.nativeTableMaps)
	nextAppliedTransactions := cloneAppliedTransactions(replica.appliedTransactions)
	if replaceNativeRelay {
		nextNativeRelayEvents = cloneNativeRelayEvents(nativeRelay)
	}
	if nativeTableMaps != nil {
		nextNativeTableMaps = cloneNativeRelayEvents(nativeTableMaps)
	}
	nextPreparedXA := cloneNativePreparedXA(replica.preparedXA)
	if preparedXA != nil {
		nextPreparedXA = cloneNativePreparedXA(preparedXA)
	}
	// A network reconnect can split one transaction across Apply calls. The
	// relay log is durable, so rebuild any transaction that has BEGIN/ROW
	// events but no committed GTID before consuming the next batch.
	for _, event := range nextRelayEvents {
		if nextExecuted.Contains(event.GTID) {
			continue
		}
		key := event.GTID.String()
		switch event.Type {
		case EventBegin:
			if _, exists := pending[key]; !exists {
				pending[key] = nil
				pendingStatements[key] = nil
			}
		case EventRow:
			pending[key] = append(pending[key], event.Changes...)
			pendingStatements[key] = append(pendingStatements[key], event.Statements...)
		}
	}
	appendRelayEvent := func(event BinlogEvent) {
		if !relayEventExists(nextRelayEvents, event) {
			nextRelayEvents = append(nextRelayEvents, cloneBinlogEventForRelay(event))
		}
	}
	// The relay boundary must reach durable state before the storage callback
	// runs. If storage commits and the final executed-GTID replacement is then
	// interrupted, a restart still has the complete transaction boundary to
	// retry. Row-aware storage can converge from the durable row image; the
	// statement-only path still relies on its callback's transaction semantics.
	persistRelayBeforeApply := func() error {
		return replica.persistStateLocked(nextExecuted, nextRows, nextPreparedXA, nextRelayEvents, nextNativeRelayEvents, nextNativeTableMaps, nextAppliedTransactions, nil)
	}
	persistAppliedMarker := func(gtid GTID) error {
		nextAppliedTransactions[gtid.String()] = true
		return replica.persistStateLocked(nextExecuted, nextRows, nextPreparedXA, nextRelayEvents, nextNativeRelayEvents, nextNativeTableMaps, nextAppliedTransactions, nil)
	}
	applyCommitted := func(event BinlogEvent, relayEvent BinlogEvent) error {
		key := event.GTID.String()
		if nextExecuted.Contains(event.GTID) {
			delete(pending, key)
			delete(pendingStatements, key)
			return nil
		}
		if nextAppliedTransactions[event.GTID.String()] {
			nextExecuted.Add(event.GTID)
			delete(pending, key)
			delete(pendingStatements, key)
			return nil
		}
		// Transaction-oriented decoders may attach the complete row and
		// statement batch to the commit event. Merge it before invoking the
		// atomic hooks so native streams and logical event streams share the
		// same exactly-once apply semantics.
		if len(event.Changes) > 0 {
			pending[key] = append(pending[key], event.Changes...)
		}
		if len(event.Statements) > 0 {
			pendingStatements[key] = append(pendingStatements[key], event.Statements...)
		}
		appendRelayEvent(relayEvent)
		if err := persistRelayBeforeApply(); err != nil {
			return err
		}
		filteredChanges, filteredStatements := replica.filterAppliedImagesLocked(pending[key], pendingStatements[key])
		if len(pending[key]) > 0 {
			if replica.ApplyRowsWithID != nil && len(filteredChanges) > 0 {
				if err := replica.ApplyRowsWithID(event.GTID.String(), append([]RowChange(nil), filteredChanges...)); err != nil {
					return err
				}
			} else if replica.ApplyRows != nil && len(filteredChanges) > 0 {
				if err := replica.ApplyRows(append([]RowChange(nil), filteredChanges...)); err != nil {
					return err
				}
			}
		} else if replica.ApplyStatementsWithID != nil && len(filteredStatements) > 0 {
			if err := replica.ApplyStatementsWithID(event.GTID.String(), append([]Statement(nil), filteredStatements...)); err != nil {
				return err
			}
		} else if replica.ApplyStatements != nil && len(filteredStatements) > 0 {
			if err := replica.ApplyStatements(append([]Statement(nil), filteredStatements...)); err != nil {
				return err
			}
		}
		nextRows = append(nextRows, filteredChanges...)
		if err := persistAppliedMarker(event.GTID); err != nil {
			return err
		}
		nextExecuted.Add(event.GTID)
		delete(pending, key)
		delete(pendingStatements, key)
		return nil
	}
	for _, event := range events {
		key := event.GTID.String()
		switch event.Type {
		case EventBegin:
			if !nextExecuted.Contains(event.GTID) {
				if _, exists := pending[key]; !exists {
					pending[key] = nil
					pendingStatements[key] = nil
				}
				appendRelayEvent(event)
			}
		case EventRow:
			if !nextExecuted.Contains(event.GTID) {
				if !relayEventExists(nextRelayEvents, event) {
					pending[key] = append(pending[key], event.Changes...)
					pendingStatements[key] = append(pendingStatements[key], event.Statements...)
				}
				appendRelayEvent(event)
			}
		case EventXAPrepare:
			if nextExecuted.Contains(event.GTID) || event.XA == nil {
				continue
			}
			if event.OnePhase {
				committed := event
				committed.Type = EventCommit
				if err := applyCommitted(committed, event); err != nil {
					return err
				}
				continue
			}
			xaKey := event.XA.Key()
			prepared := cloneBinlogEventForRelay(event)
			prepared.Type = EventBegin
			prepared.Changes = append([]RowChange(nil), pending[key]...)
			prepared.Statements = append([]Statement(nil), pendingStatements[key]...)
			if len(event.Changes) > 0 {
				prepared.Changes = append(prepared.Changes, event.Changes...)
			}
			if len(event.Statements) > 0 {
				prepared.Statements = append(prepared.Statements, event.Statements...)
			}
			nextPreparedXA[xaKey] = prepared
			delete(pending, key)
			delete(pendingStatements, key)
			appendRelayEvent(event)
		case EventXACommit:
			if event.XA == nil {
				continue
			}
			xaKey := event.XA.Key()
			prepared, exists := nextPreparedXA[xaKey]
			if !exists {
				if nextExecuted.Contains(event.GTID) {
					continue
				}
				// A complete native dump may contain both XA_PREPARE and
				// XA COMMIT in one call. The decoder consumes the prepare and
				// returns the terminal event with its durable row/statement
				// image, so the replica can apply it atomically even when its
				// prior prepared-XA snapshot was empty.
				if preparedXA == nil && len(event.Changes) == 0 && len(event.Statements) == 0 {
					return fmt.Errorf("XA COMMIT references unknown prepared transaction %s", xaKey)
				}
				prepared = cloneBinlogEventForRelay(event)
				exists = true
			}
			if nextExecuted.Contains(prepared.GTID) {
				if event.TerminalGTID != nil {
					nextExecuted.Add(*event.TerminalGTID)
				}
				delete(nextPreparedXA, xaKey)
				continue
			}
			if nextAppliedTransactions[prepared.GTID.String()] {
				nextExecuted.Add(prepared.GTID)
				if event.TerminalGTID != nil {
					nextExecuted.Add(*event.TerminalGTID)
				}
				delete(nextPreparedXA, xaKey)
				continue
			}
			appendRelayEvent(event)
			if err := persistRelayBeforeApply(); err != nil {
				return err
			}
			filteredChanges, filteredStatements := replica.filterAppliedImagesLocked(prepared.Changes, prepared.Statements)
			if len(prepared.Changes) > 0 {
				if replica.ApplyRowsWithID != nil && len(filteredChanges) > 0 {
					if err := replica.ApplyRowsWithID(prepared.GTID.String(), append([]RowChange(nil), filteredChanges...)); err != nil {
						return err
					}
				} else if replica.ApplyRows != nil && len(filteredChanges) > 0 {
					if err := replica.ApplyRows(append([]RowChange(nil), filteredChanges...)); err != nil {
						return err
					}
				}
			} else if replica.ApplyStatementsWithID != nil && len(filteredStatements) > 0 {
				if err := replica.ApplyStatementsWithID(prepared.GTID.String(), append([]Statement(nil), filteredStatements...)); err != nil {
					return err
				}
			} else if replica.ApplyStatements != nil && len(filteredStatements) > 0 {
				if err := replica.ApplyStatements(append([]Statement(nil), filteredStatements...)); err != nil {
					return err
				}
			}
			nextRows = append(nextRows, filteredChanges...)
			if err := persistAppliedMarker(prepared.GTID); err != nil {
				return err
			}
			nextExecuted.Add(prepared.GTID)
			if event.TerminalGTID != nil {
				nextExecuted.Add(*event.TerminalGTID)
			}
			delete(nextPreparedXA, xaKey)
		case EventXARollback:
			if event.XA != nil {
				delete(nextPreparedXA, event.XA.Key())
			}
			if !nextExecuted.Contains(event.GTID) {
				nextExecuted.Add(event.GTID)
			}
			if event.TerminalGTID != nil {
				nextExecuted.Add(*event.TerminalGTID)
			}
			appendRelayEvent(event)
		case EventCommit:
			if err := applyCommitted(event, event); err != nil {
				return err
			}
		}
	}
	if err := os.MkdirAll(filepath.Dir(replica.statePath), 0755); err != nil {
		return err
	}
	if err := replica.persistStateLocked(nextExecuted, nextRows, nextPreparedXA, nextRelayEvents, nextNativeRelayEvents, nextNativeTableMaps, nil, source); err != nil {
		return err
	}
	replica.Executed = nextExecuted
	replica.AppliedRows = nextRows
	replica.relayEvents = nextRelayEvents
	replica.nativeRelayEvents = nextNativeRelayEvents
	replica.nativeTableMaps = nextNativeTableMaps
	replica.preparedXA = nextPreparedXA
	replica.appliedTransactions = map[string]bool{}
	return nil
}

func (replica *Replica) incrementReplicationFilterCounterLocked(change interface{}) {
	if replica == nil || len(replica.filterCounters) == 0 && len(replica.filterConfig.ReplicateDoDB) == 0 && len(replica.filterConfig.ReplicateIgnoreDB) == 0 && len(replica.filterConfig.ReplicateDoTable) == 0 && len(replica.filterConfig.ReplicateIgnoreTable) == 0 && len(replica.filterConfig.ReplicateWildDoTable) == 0 && len(replica.filterConfig.ReplicateWildIgnoreTable) == 0 {
		return
	}
	name := ""
	rule := ""
	switch value := change.(type) {
	case RowChange:
		name = "REPLICATE_IGNORE_TABLE"
		rule = value.Table
	case Statement:
		name = "REPLICATE_IGNORE_DB"
		rule = value.Database
	}
	if name == "" || rule == "" {
		return
	}
	key := name + "\x00" + rule
	replica.filterCounters[key]++
}

func (replica *Replica) persistStateLocked(executed GTIDSet, appliedRows []RowChange, preparedXA map[string]BinlogEvent, relayEvents []BinlogEvent, nativeRelayEvents []NativeBinlogEvent, nativeTableMaps []NativeBinlogEvent, appliedTransactions map[string]bool, source *replicaSourcePosition) error {
	if replica != nil && replica.statePersistHook != nil {
		if err := replica.statePersistHook(); err != nil {
			return err
		}
	}
	sourceFile := replica.LastSourceFile
	sourcePosition := replica.LastSourcePosition
	if source != nil {
		sourceFile = source.File
		sourcePosition = source.Position
	}
	return persistReplicaStateLocked(replica.statePath, executed, appliedRows, preparedXA, relayEvents, nativeRelayEvents, nativeTableMaps, appliedTransactions, sourceFile, sourcePosition)
}

func persistReplicaStateLocked(path string, executed GTIDSet, appliedRows []RowChange, preparedXA map[string]BinlogEvent, relayEvents []BinlogEvent, nativeRelayEvents []NativeBinlogEvent, nativeTableMaps []NativeBinlogEvent, appliedTransactions map[string]bool, sourceFile string, sourcePosition uint64) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(replicaState{
		Executed:            executed,
		AppliedRows:         appliedRows,
		PreparedXA:          preparedXA,
		RelayEvents:         relayEvents,
		NativeRelayEvents:   nativeRelayEvents,
		NativeTableMaps:     nativeTableMaps,
		AppliedTransactions: appliedTransactions,
		SourceFile:          sourceFile,
		SourcePosition:      sourcePosition,
	}, "", "  ")
	if err != nil {
		return err
	}
	return writeReplicationFileAtomic(path, raw)
}

func cloneNativeRelayEvents(events []NativeBinlogEvent) []NativeBinlogEvent {
	if events == nil {
		return nil
	}
	result := make([]NativeBinlogEvent, len(events))
	for index, event := range events {
		result[index] = event
		result[index].Raw = append([]byte(nil), event.Raw...)
	}
	return result
}

func mergeNativeTableMapEvents(existing, incoming []NativeBinlogEvent) []NativeBinlogEvent {
	result := cloneNativeRelayEvents(existing)
	indexes := make(map[string]int, len(result))
	for index, event := range result {
		if event.Type != 19 {
			continue
		}
		indexes[nativeTableMapEventKey(event)] = index
	}
	for _, event := range incoming {
		if event.Type != 19 {
			continue
		}
		key := nativeTableMapEventKey(event)
		if index, exists := indexes[key]; exists {
			result[index] = cloneNativeRelayEvents([]NativeBinlogEvent{event})[0]
			continue
		}
		indexes[key] = len(result)
		result = append(result, cloneNativeRelayEvents([]NativeBinlogEvent{event})[0])
	}
	return result
}

func nativeTableMapEventKey(event NativeBinlogEvent) string {
	body := nativeEventBody(event)
	if len(body) >= 6 {
		return string(body[:6])
	}
	return string(event.Raw)
}

func cloneAppliedTransactions(transactions map[string]bool) map[string]bool {
	if len(transactions) == 0 {
		return map[string]bool{}
	}
	result := make(map[string]bool, len(transactions))
	for transactionID, applied := range transactions {
		if applied {
			result[transactionID] = true
		}
	}
	return result
}

func relayEventExists(events []BinlogEvent, candidate BinlogEvent) bool {
	for _, event := range events {
		if event.Type != candidate.Type || event.GTID != candidate.GTID || event.Position != candidate.Position {
			continue
		}
		if event.NativeFile != candidate.NativeFile || event.NativePosition != candidate.NativePosition || event.NativeEndPosition != candidate.NativeEndPosition {
			continue
		}
		return true
	}
	return false
}

// RelayLogEvents returns a defensive snapshot of logical events received by
// this replica. It provides the SQL observability surface for SHOW RELAYLOG
// EVENTS while the storage layer remains transaction-oriented.
func (replica *Replica) RelayLogEvents() []BinlogEvent {
	if replica == nil {
		return nil
	}
	replica.mu.Lock()
	defer replica.mu.Unlock()
	result := make([]BinlogEvent, len(replica.relayEvents))
	for index, event := range replica.relayEvents {
		result[index] = cloneBinlogEventForRelay(event)
	}
	return result
}

func cloneBinlogEventForRelay(event BinlogEvent) BinlogEvent {
	copyOfEvent := event
	copyOfEvent.Changes = append([]RowChange(nil), event.Changes...)
	copyOfEvent.Statements = append([]Statement(nil), event.Statements...)
	if event.XA != nil {
		xid := *event.XA
		copyOfEvent.XA = &xid
	}
	return copyOfEvent
}

// ApplyNative decodes a physical native binlog stream into GTID-bounded
// transactions and applies it through the same atomic row/statement hooks as
// the logical replication path. Native callers therefore get the same
// duplicate-GTID and durable-state behavior without converting frames to
// JSONL first.
func (replica *Replica) ApplyNative(events []NativeBinlogEvent) error {
	return replica.applyNativeWithResolvers(events, nil, nil)
}

// ApplyNativeAtSource applies a physical stream and persists the source file
// position in the same state replacement as the relay/native transaction
// boundary. This is the runtime entry point for a network native source.
func (replica *Replica) ApplyNativeAtSource(events []NativeBinlogEvent, sourceFile string, sourcePosition uint64) error {
	if sourcePosition < 4 {
		return fmt.Errorf("native source position requires position >= 4")
	}
	if err := replica.applyNativeWithResolversAtSource(events, nil, nil, &replicaSourcePosition{File: sourceFile, Position: sourcePosition}); err != nil {
		return err
	}
	replica.mu.Lock()
	replica.LastSourceFile = sourceFile
	replica.LastSourcePosition = sourcePosition
	if len(events) == 0 {
		replica.LastAppliedAt = time.Time{}
	} else {
		replica.LastAppliedAt = time.Now().UTC()
	}
	replica.LastError = ""
	replica.mu.Unlock()
	return nil
}

func (replica *Replica) resetNativeTableMapsForFileReplay() error {
	if replica == nil {
		return fmt.Errorf("replica is nil")
	}
	replica.mu.Lock()
	defer replica.mu.Unlock()
	if len(replica.nativeTableMaps) == 0 {
		return nil
	}
	if err := replica.persistStateLocked(replica.Executed, replica.AppliedRows, replica.preparedXA, replica.relayEvents, replica.nativeRelayEvents, nil, replica.appliedTransactions, nil); err != nil {
		return err
	}
	replica.nativeTableMaps = nil
	return nil
}

// ApplyNativeWithResolver applies a physical native stream with a local
// dictionary column-order resolver for TABLE_MAP events that omit names.
func (replica *Replica) ApplyNativeWithResolver(events []NativeBinlogEvent, resolver NativeTableMapResolver) error {
	return replica.applyNativeWithResolvers(events, resolver, nil)
}

// ApplyNativeWithSchemaResolver is the type-aware form of ApplyNative. It
// preserves local CHAR/BINARY and other SQL type distinctions while applying
// an already-fetched native stream.
func (replica *Replica) ApplyNativeWithSchemaResolver(events []NativeBinlogEvent, resolver NativeTableMapSchemaResolver) error {
	return replica.applyNativeWithResolvers(events, nil, resolver)
}

func (replica *Replica) applyNativeWithResolvers(events []NativeBinlogEvent, resolver NativeTableMapResolver, schemaResolver NativeTableMapSchemaResolver) error {
	return replica.applyNativeWithResolversAtSource(events, resolver, schemaResolver, nil)
}

func (replica *Replica) applyNativeWithResolversAtSource(events []NativeBinlogEvent, resolver NativeTableMapResolver, schemaResolver NativeTableMapSchemaResolver, source *replicaSourcePosition) error {
	if replica == nil {
		return fmt.Errorf("replica is nil")
	}
	replica.mu.Lock()
	preparedXA := cloneNativePreparedXA(replica.preparedXA)
	nativeRelay := cloneNativeRelayEvents(replica.nativeRelayEvents)
	nativeTableMaps := cloneNativeRelayEvents(replica.nativeTableMaps)
	replica.mu.Unlock()
	tableMapCandidates := append(cloneNativeRelayEvents(nativeRelay), cloneNativeRelayEvents(events)...)
	nativeTableMaps = mergeNativeTableMapEvents(nativeTableMaps, tableMapCandidates)
	combined := append(cloneNativeRelayEvents(nativeTableMaps), nativeRelay...)
	combined = append(combined, cloneNativeRelayEvents(events)...)
	decoder := NewNativeBinlogDecoder()
	if schemaResolver != nil {
		decoder.SetTableMapSchemaResolver(schemaResolver)
	} else {
		decoder.SetTableMapResolver(resolver)
	}
	decoder.restorePreparedXA(preparedXA)
	decoded, err := decoder.DecodeTransactions(combined)
	if err != nil {
		if errors.Is(err, ErrNativeTransactionIncomplete) {
			prepared := decoder.PreparedXA()
			replica.mu.Lock()
			nativeRelayForRetry := append(cloneNativeRelayEvents(nativeRelay), cloneNativeRelayEvents(events)...)
			persistErr := replica.persistStateLocked(replica.Executed, replica.AppliedRows, prepared, replica.relayEvents, nativeRelayForRetry, nativeTableMaps, replica.appliedTransactions, source)
			if persistErr == nil {
				replica.nativeRelayEvents = nativeRelayForRetry
				replica.nativeTableMaps = cloneNativeRelayEvents(nativeTableMaps)
				replica.preparedXA = prepared
				replica.LastError = ""
			}
			replica.mu.Unlock()
			return persistErr
		}
		return err
	}
	return replica.applyWithPreparedXAAndNativeRelayAtSourceAndTableMaps(decoded, decoder.PreparedXA(), nil, true, nativeTableMaps, source)
}

// ReplicateNativeFrom pulls a checksum-validated physical native binlog file
// from source, decodes it with the source identity available, and applies the
// resulting GTID transactions atomically. It is the native counterpart to
// ReplicateFrom for clients that consume COM_BINLOG_DUMP-style events.
func (replica *Replica) ReplicateNativeFrom(source *Source, logName string, position uint64) (uint64, error) {
	return replica.replicateNativeFrom(source, logName, position, nil)
}

// ReplicateNativeFromWithResolver applies a physical native stream using a
// local dictionary resolver for TABLE_MAP column binding. Prepared XA state
// remains durable across calls just as it does in ReplicateNativeFrom.
func (replica *Replica) ReplicateNativeFromWithResolver(source *Source, logName string, position uint64, resolver NativeTableMapResolver) (uint64, error) {
	return replica.replicateNativeFrom(source, logName, position, resolver)
}

// ReplicateNativeFromWithSchemaResolver is the type-aware form of
// ReplicateNativeFromWithResolver. It preserves local CHAR/BINARY and other
// SQL type distinctions when the upstream TABLE_MAP omits column names.
func (replica *Replica) ReplicateNativeFromWithSchemaResolver(source *Source, logName string, position uint64, resolver NativeTableMapSchemaResolver) (uint64, error) {
	return replica.replicateNativeFromWithSchemaResolver(source, logName, position, resolver)
}

func (replica *Replica) replicateNativeFrom(source *Source, logName string, position uint64, resolver NativeTableMapResolver) (uint64, error) {
	return replica.replicateNativeFromWithResolvers(source, logName, position, resolver, nil)
}

func (replica *Replica) replicateNativeFromWithSchemaResolver(source *Source, logName string, position uint64, resolver NativeTableMapSchemaResolver) (uint64, error) {
	return replica.replicateNativeFromWithResolvers(source, logName, position, nil, resolver)
}

func (replica *Replica) replicateNativeFromWithResolvers(source *Source, logName string, position uint64, resolver NativeTableMapResolver, schemaResolver NativeTableMapSchemaResolver) (uint64, error) {
	if replica == nil {
		return position, fmt.Errorf("replica is nil")
	}
	if source == nil {
		return position, fmt.Errorf("replication source is nil")
	}
	replica.mu.Lock()
	executed := cloneGTIDSet(replica.Executed)
	preparedXA := cloneNativePreparedXA(replica.preparedXA)
	replica.mu.Unlock()
	events, nextPosition, err := source.NativeDumpFile(logName, position, executed)
	if err != nil {
		replica.mu.Lock()
		replica.LastError = err.Error()
		replica.mu.Unlock()
		return position, err
	}
	decoder := NewNativeBinlogDecoderForSource(source.UUID)
	if schemaResolver != nil {
		decoder.SetTableMapSchemaResolver(schemaResolver)
	} else {
		decoder.SetTableMapResolver(resolver)
	}
	decoder.restorePreparedXA(preparedXA)
	decoded, err := decoder.DecodeTransactions(events)
	if err != nil {
		replica.mu.Lock()
		replica.LastError = err.Error()
		replica.mu.Unlock()
		return position, err
	}
	// Keep the physical frames alongside the decoded prepared-XA snapshot. A
	// pull-based native replica must have the same durable relay boundary as a
	// chunked ApplyNative caller; otherwise a restart can retain XA metadata
	// while losing the physical frames needed to resume the stream.
	if err := replica.applyWithPreparedXAAndNativeRelayAtSource(decoded, decoder.PreparedXA(), events, true, &replicaSourcePosition{File: logName, Position: nextPosition}); err != nil {
		replica.mu.Lock()
		replica.LastError = err.Error()
		replica.mu.Unlock()
		return position, err
	}
	replica.mu.Lock()
	replica.LastSourceFile = logName
	replica.LastSourcePosition = nextPosition
	if len(events) == 0 {
		replica.LastAppliedAt = time.Time{}
	} else {
		replica.LastAppliedAt = time.Now().UTC()
	}
	replica.LastError = ""
	replica.mu.Unlock()
	return nextPosition, nil
}

func (replica *Replica) ReplicateFrom(source *Source, position uint64) (uint64, error) {
	if replica == nil {
		return position, fmt.Errorf("replica is nil")
	}
	if source == nil {
		err := fmt.Errorf("replication source is nil")
		replica.mu.Lock()
		replica.LastError = err.Error()
		replica.mu.Unlock()
		return position, err
	}
	events, err := source.Dump(position)
	if err != nil {
		replica.mu.Lock()
		replica.LastError = err.Error()
		replica.mu.Unlock()
		return position, err
	}
	replica.mu.Lock()
	sourceFile := replica.LastSourceFile
	replica.mu.Unlock()
	nextPosition := position
	for _, event := range events {
		if event.Position > nextPosition {
			nextPosition = event.Position + 1
		}
	}
	if err := replica.applyWithPreparedXAAndNativeRelayAtSource(events, nil, nil, false, &replicaSourcePosition{File: sourceFile, Position: nextPosition}); err != nil {
		replica.mu.Lock()
		replica.LastError = err.Error()
		replica.mu.Unlock()
		return position, err
	}
	replica.mu.Lock()
	replica.LastSourcePosition = nextPosition
	if len(events) == 0 {
		replica.LastAppliedAt = time.Time{}
	} else {
		replica.LastAppliedAt = time.Now().UTC()
	}
	replica.LastError = ""
	replica.mu.Unlock()
	return nextPosition, nil
}

// ReplicateFromWithRetry exposes bounded network retry and lag metadata for
// a replica applier loop. A failed attempt never advances the source
// position, so replay remains idempotent.
func (replica *Replica) ReplicateFromWithRetry(source *Source, position uint64, retries int) (uint64, error) {
	if retries < 0 {
		retries = 0
	}
	var err error
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 {
			replica.mu.Lock()
			replica.NetworkRetries++
			replica.mu.Unlock()
		}
		position, err = replica.ReplicateFrom(source, position)
		if err == nil {
			return position, nil
		}
	}
	return position, err
}

func (replica *Replica) LagSeconds(now time.Time) float64 {
	replica.mu.Lock()
	defer replica.mu.Unlock()
	if replica.LastAppliedAt.IsZero() {
		return 0
	}
	lag := now.Sub(replica.LastAppliedAt).Seconds()
	if lag < 0 {
		return 0
	}
	return lag
}
