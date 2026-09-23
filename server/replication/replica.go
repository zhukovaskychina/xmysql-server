package replication

import (
	"encoding/json"
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
	AppliedRows        []RowChange
	relayEvents        []BinlogEvent
	preparedXA         map[string]BinlogEvent
	LastSourcePosition uint64
	LastAppliedAt      time.Time
	LastError          string
	NetworkRetries     uint64
	// ApplyRows is called exactly once for each newly committed GTID that
	// contains row images. When configured, it is preferred over
	// ApplyStatements so a storage engine can apply the row image atomically.
	ApplyRows func([]RowChange) error `json:"-"`
	// ApplyStatements is called exactly once for each newly committed GTID.
	// The callback must apply the complete statement batch atomically.
	ApplyStatements func([]Statement) error `json:"-"`
}

type replicaState struct {
	Executed    GTIDSet                `json:"executed"`
	AppliedRows []RowChange            `json:"applied_rows"`
	PreparedXA  map[string]BinlogEvent `json:"prepared_xa,omitempty"`
	RelayEvents []BinlogEvent          `json:"relay_events,omitempty"`
}

func NewReplica(dataDir string) (*Replica, error) {
	statePath := filepath.Join(dataDir, "replication", "replica_gtid.json")
	replica := &Replica{Executed: GTIDSet{}, statePath: statePath, AppliedRows: []RowChange{}, relayEvents: []BinlogEvent{}, preparedXA: map[string]BinlogEvent{}}
	if raw, err := os.ReadFile(statePath); err == nil {
		var state replicaState
		if json.Unmarshal(raw, &state) == nil && state.Executed != nil {
			replica.Executed = state.Executed
			replica.AppliedRows = state.AppliedRows
			replica.relayEvents = append([]BinlogEvent(nil), state.RelayEvents...)
			replica.preparedXA = cloneNativePreparedXA(state.PreparedXA)
		} else {
			// Read the pre-transactional state format for upgrades.
			_ = json.Unmarshal(raw, &replica.Executed)
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
	nextPreparedXA := cloneNativePreparedXA(replica.preparedXA)
	if preparedXA != nil {
		nextPreparedXA = cloneNativePreparedXA(preparedXA)
	}
	for _, event := range events {
		key := event.GTID.String()
		switch event.Type {
		case EventBegin:
			if !nextExecuted.Contains(event.GTID) {
				pending[key] = nil
				pendingStatements[key] = nil
				nextRelayEvents = append(nextRelayEvents, cloneBinlogEventForRelay(event))
			}
		case EventRow:
			if !nextExecuted.Contains(event.GTID) {
				pending[key] = append(pending[key], event.Changes...)
				pendingStatements[key] = append(pendingStatements[key], event.Statements...)
				nextRelayEvents = append(nextRelayEvents, cloneBinlogEventForRelay(event))
			}
		case EventCommit:
			if nextExecuted.Contains(event.GTID) {
				delete(pending, key)
				delete(pendingStatements, key)
				continue
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
			if replica.ApplyRows != nil && len(pending[key]) > 0 {
				if err := replica.ApplyRows(append([]RowChange(nil), pending[key]...)); err != nil {
					return err
				}
			} else if replica.ApplyStatements != nil && len(pendingStatements[key]) > 0 {
				if err := replica.ApplyStatements(append([]Statement(nil), pendingStatements[key]...)); err != nil {
					return err
				}
			}
			nextRows = append(nextRows, pending[key]...)
			nextRelayEvents = append(nextRelayEvents, cloneBinlogEventForRelay(event))
			nextExecuted.Add(event.GTID)
			delete(pending, key)
			delete(pendingStatements, key)
		}
	}
	if err := os.MkdirAll(filepath.Dir(replica.statePath), 0755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(replicaState{Executed: nextExecuted, AppliedRows: nextRows, PreparedXA: nextPreparedXA, RelayEvents: nextRelayEvents}, "", "  ")
	if err != nil {
		return err
	}
	if err := writeReplicationFileAtomic(replica.statePath, raw); err != nil {
		return err
	}
	replica.Executed = nextExecuted
	replica.AppliedRows = nextRows
	replica.relayEvents = nextRelayEvents
	replica.preparedXA = nextPreparedXA
	return nil
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
	if replica == nil {
		return fmt.Errorf("replica is nil")
	}
	replica.mu.Lock()
	preparedXA := cloneNativePreparedXA(replica.preparedXA)
	replica.mu.Unlock()
	decoder := NewNativeBinlogDecoder()
	if schemaResolver != nil {
		decoder.SetTableMapSchemaResolver(schemaResolver)
	} else {
		decoder.SetTableMapResolver(resolver)
	}
	decoder.restorePreparedXA(preparedXA)
	decoded, err := decoder.DecodeTransactions(events)
	if err != nil {
		return err
	}
	return replica.applyWithPreparedXA(decoded, decoder.PreparedXA())
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
	if err := replica.applyWithPreparedXA(decoded, decoder.PreparedXA()); err != nil {
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
	if err := replica.Apply(events); err != nil {
		replica.mu.Lock()
		replica.LastError = err.Error()
		replica.mu.Unlock()
		return position, err
	}
	for _, event := range events {
		if event.Position > position {
			position = event.Position + 1
		}
	}
	replica.mu.Lock()
	replica.LastSourcePosition = position
	if len(events) == 0 {
		replica.LastAppliedAt = time.Time{}
	} else {
		replica.LastAppliedAt = time.Now().UTC()
	}
	replica.LastError = ""
	replica.mu.Unlock()
	return position, nil
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
