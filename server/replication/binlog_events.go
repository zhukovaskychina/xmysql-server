package replication

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"time"
)

type EventType string

const (
	EventBegin      EventType = "BEGIN"
	EventRow        EventType = "ROW"
	EventCommit     EventType = "COMMIT"
	EventRotate     EventType = "ROTATE"
	EventXAPrepare  EventType = "XA_PREPARE"
	EventXACommit   EventType = "XA_COMMIT"
	EventXARollback EventType = "XA_ROLLBACK"
)

// XAIdentity is the durable XID carried by logical and native replication
// events. Keeping the three protocol components together prevents a native
// XA PREPARE from being reduced to an opaque coordinator string.
type XAIdentity struct {
	GTRID    string `json:"gtrid"`
	BQUAL    string `json:"bqual,omitempty"`
	FormatID uint32 `json:"format_id"`
}

// Key is the canonical identity used by the native XA decoder and replica
// prepared-XA state. It intentionally uses the native decoder's byte-based
// representation, so non-ASCII XIDs remain unambiguous.
func (x XAIdentity) Key() string {
	return fmt.Sprintf("%d:%s:%s", x.FormatID, hex.EncodeToString([]byte(x.GTRID)), hex.EncodeToString([]byte(x.BQUAL)))
}

// JSONPartialUpdate describes one operation encoded in a native
// PARTIAL_UPDATE_ROWS_EVENT after-image. Operation follows MySQL's binary-log
// JSON diff format: replace, insert, or remove. Paths use MySQL JSON path
// syntax, for example $.profile.name.
type JSONPartialUpdate struct {
	Operation byte        `json:"operation"`
	Path      string      `json:"path"`
	Value     interface{} `json:"value,omitempty"`
}

const (
	JSONPartialOperationReplace byte = 0
	JSONPartialOperationInsert  byte = 1
	JSONPartialOperationRemove  byte = 2
)

type RowChange struct {
	Table  string `json:"table"`
	Action string `json:"action"`
	// Flags carries MySQL Rows_event row-level flags. Zero preserves the
	// default used by existing logical callers.
	Flags uint16 `json:"flags,omitempty"`
	// ExtraRowInfo carries an already-negotiated Rows_event extra-row-info
	// payload. It is copied into native events when it fits the protocol's
	// two-byte extra_data_len envelope.
	ExtraRowInfo []byte   `json:"extra_row_info,omitempty"`
	Columns      []string `json:"columns,omitempty"`
	// ColumnTypes optionally carries MySQL type names keyed by column name.
	// It is used only by the native wire encoder; logical replication remains
	// compatible with callers that provide row images without schema hints.
	ColumnTypes map[string]string `json:"column_types,omitempty"`
	// ColumnMetadata optionally carries already-negotiated TABLE_MAP metadata
	// bytes keyed by column name. The native encoder copies these bytes instead
	// of inferring metadata for types whose wire representation is ambiguous.
	ColumnMetadata map[string][]byte      `json:"column_metadata,omitempty"`
	Before         map[string]interface{} `json:"before,omitempty"`
	After          map[string]interface{} `json:"after,omitempty"`
	// PartialJSONUpdates optionally carries native MySQL JSON diffs for the
	// after-image of an UPDATE. Without this field, updates continue to use
	// the regular UPDATE_ROWS_EVENTv2 full-value encoding.
	PartialJSONUpdates map[string][]JSONPartialUpdate `json:"partial_json_updates,omitempty"`
}

// Statement is the logical SQL representation used by the first cluster
// implementation. Keeping the database name with the statement makes replay
// independent from the client session that produced the transaction.
type Statement struct {
	Database string `json:"database,omitempty"`
	SQL      string `json:"sql"`
}

type BinlogEvent struct {
	Timestamp         time.Time `json:"timestamp"`
	Type              EventType `json:"type"`
	ServerID          uint32    `json:"server_id"`
	Position          uint64    `json:"position"`
	NativeFile        string    `json:"native_file,omitempty"`
	NativePosition    uint64    `json:"native_position,omitempty"`
	NativeEndPosition uint64    `json:"native_end_position,omitempty"`
	// NativeEventType identifies the physical MySQL event represented by this
	// logical event when it is projected from a native binlog file. It is
	// intentionally omitted from the logical JSONL stream.
	NativeEventType string `json:"-"`
	GTID            GTID   `json:"gtid"`
	// TerminalGTID is the second GTID assigned to a two-phase XA COMMIT or
	// XA ROLLBACK. GTID remains the prepare-phase identity so the logical event
	// can join the terminal boundary back to the prepared transaction.
	TerminalGTID *GTID       `json:"terminal_gtid,omitempty"`
	XA           *XAIdentity `json:"xa,omitempty"`
	// OnePhase marks an XA_PREPARE event emitted for XA COMMIT ONE PHASE.
	// MySQL encodes this as XA_PREPARE_EVENT(one_phase=1), which commits the
	// transaction immediately and must not enter the prepared-XA set.
	OnePhase bool `json:"one_phase,omitempty"`
	// TransactionKey is an internal coordinator identity used to recover
	// keyed exactly-once commits if source state files are lost after the
	// logical binlog append. Native MySQL event encoding intentionally ignores
	// this field.
	TransactionKey string      `json:"transaction_key,omitempty"`
	Changes        []RowChange `json:"changes,omitempty"`
	Statements     []Statement `json:"statements,omitempty"`
	Checksum       uint32      `json:"checksum"`
}

func (event *BinlogEvent) Encode() ([]byte, error) {
	event.Checksum = 0
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	event.Checksum = crc32.ChecksumIEEE(payload)
	return json.Marshal(event)
}

func DecodeEvent(payload []byte) (BinlogEvent, error) {
	var event BinlogEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return event, err
	}
	checksum := event.Checksum
	event.Checksum = 0
	withoutChecksum, err := json.Marshal(event)
	if err != nil {
		return event, err
	}
	if checksum != crc32.ChecksumIEEE(withoutChecksum) {
		return event, ErrChecksumMismatch
	}
	event.Checksum = checksum
	return event, nil
}
