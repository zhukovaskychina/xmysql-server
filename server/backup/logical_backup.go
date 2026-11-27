package backup

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

type LogicalBackup struct {
	Version    int                                `json:"version"`
	CreatedAt  time.Time                          `json:"created_at"`
	Tables     map[string][]replication.RowChange `json:"tables"`
	Statements []replication.Statement            `json:"statements,omitempty"`
}

func Export(w io.Writer, tables map[string][]replication.RowChange) error {
	return export(w, tables, nil)
}

// ExportWithStatements writes a version-2 logical backup that includes the
// committed schema/data SQL statements needed to recreate table metadata
// before applying row changes. Export remains version 1 compatible for callers
// that only have row changes.
func ExportWithStatements(w io.Writer, tables map[string][]replication.RowChange, statements []replication.Statement) error {
	return export(w, tables, statements)
}

func export(w io.Writer, tables map[string][]replication.RowChange, statements []replication.Statement) error {
	if w == nil {
		return fmt.Errorf("logical backup writer is nil")
	}
	version := 1
	if len(statements) > 0 {
		version = 2
	}
	return json.NewEncoder(w).Encode(LogicalBackup{
		Version: version, CreatedAt: time.Now().UTC(), Tables: tables,
		Statements: append([]replication.Statement(nil), statements...),
	})
}

func Import(r io.Reader) (LogicalBackup, error) {
	if r == nil {
		return LogicalBackup{}, fmt.Errorf("logical backup reader is nil")
	}
	var raw struct {
		Version    int                                `json:"version"`
		CreatedAt  time.Time                          `json:"created_at"`
		Tables     map[string][]replication.RowChange `json:"tables"`
		Statements []replication.Statement            `json:"statements,omitempty"`
	}
	decoder := json.NewDecoder(r)
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		return LogicalBackup{}, err
	}
	backup := LogicalBackup{Version: raw.Version, CreatedAt: raw.CreatedAt, Tables: raw.Tables, Statements: raw.Statements}
	for table, changes := range backup.Tables {
		for i := range changes {
			changes[i].Before = normalizeRowValues(changes[i].Before)
			changes[i].After = normalizeRowValues(changes[i].After)
		}
		backup.Tables[table] = changes
	}
	if (backup.Version != 1 && backup.Version != 2) || backup.Tables == nil {
		return LogicalBackup{}, fmt.Errorf("unsupported logical backup format")
	}
	for _, statement := range backup.Statements {
		if strings.TrimSpace(statement.SQL) == "" {
			return LogicalBackup{}, fmt.Errorf("logical backup contains empty schema statement")
		}
	}
	return backup, nil
}

func normalizeRowValues(values map[string]interface{}) map[string]interface{} {
	if values == nil {
		return nil
	}
	for key, value := range values {
		if number, ok := value.(json.Number); ok {
			if integer, err := strconv.ParseInt(string(number), 10, 64); err == nil {
				values[key] = integer
				continue
			}
			if decimal, err := strconv.ParseFloat(string(number), 64); err == nil {
				values[key] = decimal
			}
		}
	}
	return values
}

// RestoreUntilPosition replays only transactions whose COMMIT position is at
// or before the requested position, providing a deterministic PITR boundary.
func RestoreUntilPosition(events []replication.BinlogEvent, position uint64) map[string][]replication.RowChange {
	return RestoreUntilPositionWithStatements(events, position).Tables
}

// RestoreNativeUntilPosition replays only complete logical transactions whose
// physical native binlog frames end at or before position. A row transaction
// can occupy multiple native frames, so the transaction is committed only
// after its XID frame is within the requested physical boundary.
func RestoreNativeUntilPosition(source *replication.Source, logName string, position uint64) (RestoreResult, error) {
	if source == nil {
		return RestoreResult{}, fmt.Errorf("native PITR source is nil")
	}
	return RestoreNativeUntilPositions(source, map[string]uint64{logName: position})
}

// RestoreNativeUntilPositions is the rotated-file form of
// RestoreNativeUntilPosition. Files are replayed in native index order and
// each file has its own physical End_log_pos boundary.
func RestoreNativeUntilPositions(source *replication.Source, boundaries map[string]uint64) (RestoreResult, error) {
	if source == nil {
		return RestoreResult{}, fmt.Errorf("native PITR source is nil")
	}
	knownFiles := make(map[string]struct{})
	for _, file := range source.NativeFiles() {
		knownFiles[file.Name] = struct{}{}
	}
	for file := range boundaries {
		if _, ok := knownFiles[file]; !ok {
			return RestoreResult{}, fmt.Errorf("native binlog file %q does not exist", file)
		}
	}
	logical := make([]replication.BinlogEvent, 0)
	indexes := make(map[nativePITREventKey]int)
	for _, file := range source.NativeFiles() {
		_, ok := boundaries[file.Name]
		if !ok {
			continue
		}
		mapped, err := source.DumpFileWithNativePositions(file.Name, 4)
		if err != nil {
			return RestoreResult{}, err
		}
		for _, event := range mapped {
			key := nativePITREventKey{Type: event.Type, UUID: event.GTID.UUID, Sequence: event.GTID.Seq, Position: event.Position}
			if index, ok := indexes[key]; ok {
				if event.NativeEndPosition > logical[index].NativeEndPosition {
					logical[index].NativeEndPosition = event.NativeEndPosition
				}
				continue
			}
			indexes[key] = len(logical)
			logical = append(logical, event)
		}
	}
	committedBoundary := make([]replication.BinlogEvent, 0, len(logical))
	for _, event := range logical {
		boundary, ok := boundaries[event.NativeFile]
		if ok && event.NativeEndPosition <= boundary {
			committedBoundary = append(committedBoundary, event)
		}
	}
	return RestoreUntilPositionWithStatements(committedBoundary, ^uint64(0)), nil
}

type nativePITREventKey struct {
	Type     replication.EventType
	UUID     string
	Sequence uint64
	Position uint64
}

// RestoreResult is the committed portion of a PITR replay. Statements are
// returned before row changes so callers can recreate schema objects before
// loading their data.
type RestoreResult struct {
	Tables     map[string][]replication.RowChange
	Statements []replication.Statement
}

func RestoreUntilPositionWithStatements(events []replication.BinlogEvent, position uint64) RestoreResult {
	result := RestoreResult{Tables: make(map[string][]replication.RowChange)}
	pending := make(map[string][]replication.RowChange)
	pendingStatements := make(map[string][]replication.Statement)
	for _, event := range events {
		if event.Position > position {
			continue
		}
		key := event.GTID.String()
		switch event.Type {
		case replication.EventBegin:
			pending[key] = nil
			pendingStatements[key] = nil
		case replication.EventRow:
			pending[key] = append(pending[key], event.Changes...)
			pendingStatements[key] = append(pendingStatements[key], event.Statements...)
		case replication.EventCommit:
			result.Statements = append(result.Statements, pendingStatements[key]...)
			for _, change := range pending[key] {
				result.Tables[change.Table] = append(result.Tables[change.Table], change)
			}
			delete(pending, key)
			delete(pendingStatements, key)
		}
	}
	return result
}

// ReplayStatements applies committed schema/data statements in order. The
// callback is intentionally supplied by the engine layer so this package
// remains independent of a particular executor or storage implementation.
func ReplayStatements(statements []replication.Statement, apply func(replication.Statement) error) error {
	if apply == nil {
		return fmt.Errorf("schema statement replay callback is nil")
	}
	for _, statement := range statements {
		if strings.TrimSpace(statement.SQL) == "" {
			return fmt.Errorf("schema statement has empty SQL")
		}
		if err := apply(statement); err != nil {
			return fmt.Errorf("replay schema statement %q: %w", statement.SQL, err)
		}
	}
	return nil
}
