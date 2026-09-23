package replication

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

// NativeBinlogDecoder keeps TABLE_MAP metadata while consuming physical
// binlog frames. It is intentionally stateful: a native ROWS event does not
// repeat the column names or type metadata required to decode its images.
type NativeBinlogDecoder struct {
	tables         map[uint64]nativeDecoderTable
	sourceUUID     string
	previousGTIDs  GTIDIntervals
	preparedXA     map[string]BinlogEvent
	tableResolver  NativeTableMapResolver
	schemaResolver NativeTableMapSchemaResolver
}

// NativeTableMapResolver supplies the local dictionary column order for a
// physical TABLE_MAP_EVENT. MySQL's standard TABLE_MAP payload carries table
// identity and type metadata but normally omits column names; consumers that
// apply row images therefore need to bind the physical table to their local
// schema before decoding.
type NativeTableMapResolver func(database, table string, columnCount int) ([]string, error)

// NativeTableMapSchemaResolver supplies the local dictionary schema for a
// physical TABLE_MAP_EVENT. Column types are optional for callers that only
// need to bind names; when present they preserve SQL-facing distinctions that
// the native type code alone cannot carry, such as CHAR versus BINARY.
type NativeTableMapSchemaResolver func(database, table string, columnCount int) (columns []string, columnTypes []string, err error)

type nativeDecoderTable struct {
	database               string
	table                  string
	columns                []string
	types                  []byte
	metadata               [][]byte
	unsigned               []bool
	enumMembers            map[int][]string
	setMembers             map[int][]string
	defaultCharsets        nativeDecoderCharsetMetadata
	columnCharsets         []uint64
	enumSetDefaultCharsets nativeDecoderCharsetMetadata
	enumSetColumnCharsets  []uint64
	geometryTypes          []uint64
	vectorDimensions       []uint64
	primaryKey             []nativeDecoderKeyPart
	columnVisibility       []bool
	columnTypes            []string
}

type nativeDecoderCharsetMetadata struct {
	defaultCharset uint64
	pairs          [][2]uint64
}

type nativeDecoderKeyPart struct {
	column uint64
	prefix uint64
}

// NewNativeBinlogDecoder returns an empty decoder. TABLE_MAP events must be
// consumed before their corresponding ROWS events, as in a MySQL binlog.
func NewNativeBinlogDecoder() *NativeBinlogDecoder {
	return &NativeBinlogDecoder{
		tables:        make(map[uint64]nativeDecoderTable),
		previousGTIDs: GTIDIntervals{},
		preparedXA:    make(map[string]BinlogEvent),
	}
}

// NewNativeBinlogDecoderForSource preserves a source UUID that is not itself
// a hexadecimal UUID. The native wire format carries only a 16-byte SID; the
// source context is therefore required to reverse the writer's deterministic
// hash mapping for those logical source names.
func NewNativeBinlogDecoderForSource(sourceUUID string) *NativeBinlogDecoder {
	decoder := NewNativeBinlogDecoder()
	decoder.sourceUUID = strings.TrimSpace(sourceUUID)
	return decoder
}

// SetTableMapResolver configures the local schema lookup used for subsequent
// TABLE_MAP_EVENT frames. Returning an error rejects the stream before any
// associated ROWS_EVENT is applied.
func (d *NativeBinlogDecoder) SetTableMapResolver(resolver NativeTableMapResolver) {
	if d != nil {
		d.tableResolver = resolver
	}
}

// SetTableMapSchemaResolver configures a local schema lookup that can bind
// both column names and SQL types before ROWS_EVENT values are decoded.
// Returning an error rejects the stream before any associated ROWS_EVENT is
// applied. The name-only resolver remains available for existing callers.
func (d *NativeBinlogDecoder) SetTableMapSchemaResolver(resolver NativeTableMapSchemaResolver) {
	if d != nil {
		d.schemaResolver = resolver
	}
}

func (d *NativeBinlogDecoder) decodeTableMap(body []byte) (nativeDecoderTable, uint64, error) {
	table, tableID, err := decodeNativeTableMap(body)
	if err != nil {
		return nativeDecoderTable{}, 0, err
	}
	if d != nil && d.schemaResolver != nil {
		columns, columnTypes, resolveErr := d.schemaResolver(table.database, table.table, len(table.types))
		if resolveErr != nil {
			return nativeDecoderTable{}, 0, fmt.Errorf("resolve TABLE_MAP %s.%s: %w", table.database, table.table, resolveErr)
		}
		if len(columns) != len(table.types) {
			return nativeDecoderTable{}, 0, fmt.Errorf("resolved TABLE_MAP %s.%s columns=%d, want %d", table.database, table.table, len(columns), len(table.types))
		}
		if len(columnTypes) != 0 && len(columnTypes) != len(table.types) {
			return nativeDecoderTable{}, 0, fmt.Errorf("resolved TABLE_MAP %s.%s column types=%d, want %d or zero", table.database, table.table, len(columnTypes), len(table.types))
		}
		for index, column := range columns {
			if strings.TrimSpace(column) == "" {
				return nativeDecoderTable{}, 0, fmt.Errorf("resolved TABLE_MAP %s.%s has empty column %d", table.database, table.table, index)
			}
		}
		table.columns = append([]string(nil), columns...)
		if len(columnTypes) > 0 {
			table.columnTypes = append([]string(nil), columnTypes...)
		}
	} else if d != nil && d.tableResolver != nil {
		columns, resolveErr := d.tableResolver(table.database, table.table, len(table.types))
		if resolveErr != nil {
			return nativeDecoderTable{}, 0, fmt.Errorf("resolve TABLE_MAP %s.%s: %w", table.database, table.table, resolveErr)
		}
		if len(columns) != len(table.types) {
			return nativeDecoderTable{}, 0, fmt.Errorf("resolved TABLE_MAP %s.%s columns=%d, want %d", table.database, table.table, len(columns), len(table.types))
		}
		for index, column := range columns {
			if strings.TrimSpace(column) == "" {
				return nativeDecoderTable{}, 0, fmt.Errorf("resolved TABLE_MAP %s.%s has empty column %d", table.database, table.table, index)
			}
		}
		table.columns = append([]string(nil), columns...)
	}
	return table, tableID, nil
}

// PreparedXA returns a defensive snapshot of the two-phase XA transactions
// that have been prepared but not committed or rolled back yet. A native
// dump may be split between XA_PREPARE_EVENT and the later XA COMMIT query,
// so callers that persist replica state must carry this snapshot forward.
func (d *NativeBinlogDecoder) PreparedXA() map[string]BinlogEvent {
	if d == nil {
		return map[string]BinlogEvent{}
	}
	return cloneNativePreparedXA(d.preparedXA)
}

func (d *NativeBinlogDecoder) restorePreparedXA(prepared map[string]BinlogEvent) {
	if d == nil {
		return
	}
	d.preparedXA = cloneNativePreparedXA(prepared)
}

func cloneNativePreparedXA(source map[string]BinlogEvent) map[string]BinlogEvent {
	result := make(map[string]BinlogEvent, len(source))
	for key, event := range source {
		result[key] = cloneNativeBinlogEvent(event)
	}
	return result
}

func cloneNativeBinlogEvent(event BinlogEvent) BinlogEvent {
	clone := event
	clone.Changes = make([]RowChange, len(event.Changes))
	for index, change := range event.Changes {
		copyChange := change
		copyChange.Columns = append([]string(nil), change.Columns...)
		copyChange.ExtraRowInfo = append([]byte(nil), change.ExtraRowInfo...)
		copyChange.ColumnTypes = cloneNativeStringMap(change.ColumnTypes)
		copyChange.ColumnMetadata = cloneNativeByteMap(change.ColumnMetadata)
		copyChange.Before = cloneNativeValueMap(change.Before)
		copyChange.After = cloneNativeValueMap(change.After)
		if change.PartialJSONUpdates != nil {
			copyChange.PartialJSONUpdates = make(map[string][]JSONPartialUpdate, len(change.PartialJSONUpdates))
			for column, updates := range change.PartialJSONUpdates {
				copyChange.PartialJSONUpdates[column] = append([]JSONPartialUpdate(nil), updates...)
			}
		}
		clone.Changes[index] = copyChange
	}
	clone.Statements = append([]Statement(nil), event.Statements...)
	return clone
}

func cloneNativeStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func cloneNativeByteMap(source map[string][]byte) map[string][]byte {
	if source == nil {
		return nil
	}
	result := make(map[string][]byte, len(source))
	for key, value := range source {
		result[key] = append([]byte(nil), value...)
	}
	return result
}

func cloneNativeValueMap(source map[string]interface{}) map[string]interface{} {
	if source == nil {
		return nil
	}
	result := make(map[string]interface{}, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

// PreviousGTIDs returns the PREVIOUS_GTIDS_EVENT history most recently
// consumed by the decoder. The returned set is a copy and can be modified by
// the caller without changing decoder state.
func (d *NativeBinlogDecoder) PreviousGTIDs() GTIDIntervals {
	if d == nil {
		return GTIDIntervals{}
	}
	return cloneGTIDIntervals(d.previousGTIDs)
}

// Decode consumes validated physical frames and returns logical row images.
// Non-row events are accepted and ignored; this allows callers to pass a
// complete transaction stream without pre-filtering GTID/XID/QUERY events.
func (d *NativeBinlogDecoder) Decode(events []NativeBinlogEvent) ([]RowChange, error) {
	if d == nil {
		return nil, fmt.Errorf("nil native binlog decoder")
	}
	if d.tables == nil {
		d.tables = make(map[uint64]nativeDecoderTable)
	}
	if d.previousGTIDs == nil {
		d.previousGTIDs = GTIDIntervals{}
	}
	if d.preparedXA == nil {
		d.preparedXA = make(map[string]BinlogEvent)
	}
	decoded := make([]RowChange, 0)
	for _, event := range events {
		if err := validateNativeDecoderEvent(event); err != nil {
			return nil, err
		}
		frame := event.Raw
		body := nativeEventBody(event)
		switch frame[4] {
		case 19:
			table, tableID, err := d.decodeTableMap(body)
			if err != nil {
				return nil, err
			}
			d.tables[tableID] = table
		case 35:
			previous, err := decodeNativePreviousGTIDs(body, d.sourceUUID)
			if err != nil {
				return nil, err
			}
			d.previousGTIDs = previous
		case 40:
			inner, err := decodeNativeTransactionPayload(event)
			if err != nil {
				return nil, err
			}
			changes, err := d.Decode(inner)
			if err != nil {
				return nil, err
			}
			decoded = append(decoded, changes...)
		case 23, 24, 25, 30, 31, 32, 39:
			changes, err := d.decodeNativeRows(frame[4], body)
			if err != nil {
				return nil, err
			}
			decoded = append(decoded, changes...)
		case 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 20, 21, 22, 26, 27, 28, 29, 33, 34, 36, 37, 38, 41, 42:
			// STOP/ROTATE/FORMAT_DESCRIPTION and the remaining replication
			// control events carry no row image for this decoder. They are
			// nevertheless known event types and must be consumed so a complete
			// upstream stream remains readable. Transaction-bearing controls are
			// included here because Decode is the row-image-only API; the
			// transaction API handles them explicitly below.
		default:
			return nil, fmt.Errorf("unsupported native event %d", frame[4])
		}
	}
	return decoded, nil
}

// DecodeNativeBinlogEvents is a stateless convenience wrapper for one
// contiguous stream beginning with its TABLE_MAP events.
func DecodeNativeBinlogEvents(events []NativeBinlogEvent) ([]RowChange, error) {
	return NewNativeBinlogDecoder().Decode(events)
}

// DecodeTransactions reconstructs the transaction boundaries represented by
// GTID/XID frames and attaches decoded row images and QUERY_EVENT statements.
// A native SID is rendered as a canonical UUID-shaped value. For source names
// that are not UUIDs the writer uses a deterministic hash, so that identity is
// stable but cannot be losslessly converted back to the original name.
func (d *NativeBinlogDecoder) DecodeTransactions(events []NativeBinlogEvent) ([]BinlogEvent, error) {
	if d == nil {
		return nil, fmt.Errorf("nil native binlog decoder")
	}
	if d.tables == nil {
		d.tables = make(map[uint64]nativeDecoderTable)
	}
	if d.previousGTIDs == nil {
		d.previousGTIDs = GTIDIntervals{}
	}
	if d.preparedXA == nil {
		d.preparedXA = make(map[string]BinlogEvent)
	}
	transactions := make([]BinlogEvent, 0)
	var current *BinlogEvent
	for _, event := range events {
		if err := validateNativeDecoderEvent(event); err != nil {
			return nil, err
		}
		frame := event.Raw
		body := nativeEventBody(event)
		timestamp := time.Unix(int64(binary.LittleEndian.Uint32(frame[0:4])), 0).UTC()
		switch frame[4] {
		case 19:
			table, tableID, err := d.decodeTableMap(body)
			if err != nil {
				return nil, err
			}
			d.tables[tableID] = table
		case 35:
			previous, err := decodeNativePreviousGTIDs(body, d.sourceUUID)
			if err != nil {
				return nil, err
			}
			d.previousGTIDs = previous
		case 40:
			inner, err := decodeNativeTransactionPayload(event)
			if err != nil {
				return nil, err
			}
			for _, nested := range inner {
				nestedFrame := nested.Raw
				nestedBody := nativeEventBody(nested)
				nestedTimestamp := time.Unix(int64(binary.LittleEndian.Uint32(nestedFrame[0:4])), 0).UTC()
				switch nestedFrame[4] {
				case 19:
					table, tableID, err := d.decodeTableMap(nestedBody)
					if err != nil {
						return nil, err
					}
					d.tables[tableID] = table
				case 2:
					statement, database, err := decodeNativeQuery(nestedBody)
					if err != nil {
						return nil, err
					}
					if handled, xaErr := d.consumeNativeXAQuery(statement, event.Position, &transactions); handled {
						if xaErr != nil {
							return nil, xaErr
						}
						continue
					}
					switch nativeQueryBoundary(statement) {
					case "commit":
						if current == nil {
							return nil, fmt.Errorf("native payload COMMIT has no active transaction")
						}
						current.Type = EventCommit
						transactions = append(transactions, *current)
						current = nil
						continue
					case "rollback":
						current = nil
						continue
					}
					if current == nil {
						current = &BinlogEvent{
							Timestamp: nestedTimestamp,
							Type:      EventBegin,
							ServerID:  binary.LittleEndian.Uint32(nestedFrame[5:9]),
							Position:  event.Position,
							GTID:      nativeAnonymousGTID(event, frame),
						}
					}
					if statement != "" {
						current.Statements = append(current.Statements, Statement{Database: database, SQL: statement})
					}
				case 23, 24, 25, 30, 31, 32, 39:
					changes, err := d.decodeNativeRows(nestedFrame[4], nestedBody)
					if err != nil {
						return nil, err
					}
					if current == nil {
						return nil, fmt.Errorf("native payload ROWS_EVENT has no active GTID transaction")
					}
					current.Changes = append(current.Changes, changes...)
				case 16:
					if current == nil {
						return nil, fmt.Errorf("native payload XID_EVENT has no active transaction")
					}
					current.Type = EventCommit
					transactions = append(transactions, *current)
					current = nil
				case 38:
					xaKey, onePhase, err := decodeNativeXAPrepareDetails(nestedBody)
					if err != nil {
						return nil, err
					}
					if !onePhase {
						if current == nil {
							return nil, fmt.Errorf("native payload XA_PREPARE_EVENT has no active transaction")
						}
						prepared := *current
						prepared.Type = EventBegin
						d.preparedXA[xaKey] = prepared
						current = nil
						continue
					}
					if current == nil {
						return nil, fmt.Errorf("native payload XA_PREPARE_EVENT has no active transaction")
					}
					current.Type = EventCommit
					transactions = append(transactions, *current)
					current = nil
				case 35:
					// PREVIOUS_GTIDS_EVENT is file metadata and is not part of the
					// transaction payload in valid MySQL binlogs.
				case 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 20, 21, 22, 26, 27, 28, 29, 36, 37, 41:
					// STOP/ROTATE/FORMAT_DESCRIPTION, INCIDENT, HEARTBEAT,
					// IGNORE, ROWS_QUERY, TRANSACTION_CONTEXT, VIEW_CHANGE and
					// HEARTBEAT_LOG_EVENT_V2 carry no row images. They are safe to
					// ignore after frame validation.
				default:
					return nil, fmt.Errorf("unsupported event %d inside TRANSACTION_PAYLOAD_EVENT", nestedFrame[4])
				}
			}
		case 38:
			xaKey, onePhase, err := decodeNativeXAPrepareDetails(body)
			if err != nil {
				return nil, err
			}
			if !onePhase {
				if current == nil {
					return nil, fmt.Errorf("native XA_PREPARE_EVENT has no active transaction")
				}
				prepared := *current
				prepared.Type = EventBegin
				d.preparedXA[xaKey] = prepared
				current = nil
				continue
			}
			if current == nil {
				return nil, fmt.Errorf("native XA_PREPARE_EVENT has no active transaction")
			}
			current.Type = EventCommit
			transactions = append(transactions, *current)
			current = nil
		case 33:
			gtid, err := decodeNativeGTID(body)
			if err != nil {
				return nil, err
			}
			if d.sourceUUID != "" && bytes.Equal(nativeGTIDSID(d.sourceUUID), frame[20:36]) {
				gtid.UUID = d.sourceUUID
			}
			if current != nil {
				return nil, fmt.Errorf("native GTID_EVENT started before prior transaction committed")
			}
			current = &BinlogEvent{Timestamp: timestamp, Type: EventBegin, ServerID: binary.LittleEndian.Uint32(frame[5:9]), Position: event.Position, GTID: gtid}
		case 42:
			gtid, err := decodeNativeTaggedGTID(body, d.sourceUUID)
			if err != nil {
				return nil, err
			}
			if current != nil {
				return nil, fmt.Errorf("native GTID_TAGGED_LOG_EVENT started before prior transaction committed")
			}
			current = &BinlogEvent{Timestamp: timestamp, Type: EventBegin, ServerID: binary.LittleEndian.Uint32(frame[5:9]), Position: event.Position, GTID: gtid}
		case 34:
			if current != nil {
				return nil, fmt.Errorf("native ANONYMOUS_GTID_EVENT started before prior transaction committed")
			}
			current = &BinlogEvent{
				Timestamp: timestamp,
				Type:      EventBegin,
				ServerID:  binary.LittleEndian.Uint32(frame[5:9]),
				Position:  event.Position,
				GTID:      nativeAnonymousGTID(event, frame),
			}
		case 2:
			statement, database, err := decodeNativeQuery(body)
			if err != nil {
				return nil, err
			}
			if handled, xaErr := d.consumeNativeXAQuery(statement, event.Position, &transactions); handled {
				if xaErr != nil {
					return nil, xaErr
				}
				continue
			}
			switch nativeQueryBoundary(statement) {
			case "commit":
				if current == nil {
					return nil, fmt.Errorf("native COMMIT has no active transaction")
				}
				current.Type = EventCommit
				transactions = append(transactions, *current)
				current = nil
				continue
			case "rollback":
				current = nil
				continue
			}
			if current == nil {
				current = &BinlogEvent{
					Timestamp: timestamp,
					Type:      EventBegin,
					ServerID:  binary.LittleEndian.Uint32(frame[5:9]),
					Position:  event.Position,
					GTID:      nativeAnonymousGTID(event, frame),
				}
			}
			if nativeQueryBoundary(statement) != "begin" && statement != "" {
				current.Statements = append(current.Statements, Statement{Database: database, SQL: statement})
			}
		case 23, 24, 25, 30, 31, 32, 39:
			changes, err := d.decodeNativeRows(frame[4], body)
			if err != nil {
				return nil, err
			}
			if current == nil {
				return nil, fmt.Errorf("native ROWS_EVENT has no active GTID transaction")
			}
			current.Changes = append(current.Changes, changes...)
		case 16:
			if current == nil {
				return nil, fmt.Errorf("native XID_EVENT has no active transaction")
			}
			current.Type = EventCommit
			transactions = append(transactions, *current)
			current = nil
		case 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 20, 21, 22, 26, 27, 28, 29, 36, 37, 41:
			// Known non-transactional control events. They are validated above
			// and do not change the logical transaction state.
		default:
			return nil, fmt.Errorf("unsupported native event %d", frame[4])
		}
	}
	if current != nil {
		return nil, fmt.Errorf("native stream ended before XID_EVENT")
	}
	return transactions, nil
}

func nativeAnonymousGTID(event NativeBinlogEvent, frame []byte) GTID {
	position := event.Position
	if position == 0 && len(frame) >= nativeEventHeaderLength {
		end := uint64(binary.LittleEndian.Uint32(frame[13:17]))
		if end >= uint64(len(frame)) {
			position = end - uint64(len(frame))
		}
	}
	if position == 0 {
		position = 1
	}
	file := strings.TrimSpace(event.File)
	if file == "" {
		file = fmt.Sprintf("server-%d", binary.LittleEndian.Uint32(frame[5:9]))
	}
	return GTID{UUID: "anonymous:" + file, Seq: position}
}

func DecodeNativeBinlogTransactions(events []NativeBinlogEvent) ([]BinlogEvent, error) {
	return NewNativeBinlogDecoder().DecodeTransactions(events)
}

func validateNativeDecoderFrame(frame []byte) error {
	return validateNativeDecoderEvent(NativeBinlogEvent{Raw: frame})
}

func nativeChecksumLengthForEvent(event NativeBinlogEvent) int {
	if event.ChecksumKnown {
		if event.ChecksumLength < 0 {
			return 0
		}
		return event.ChecksumLength
	}
	return nativeChecksumLength
}

func validateNativeDecoderEvent(event NativeBinlogEvent) error {
	frame := event.Raw
	checksumLength := nativeChecksumLengthForEvent(event)
	if checksumLength != 0 && checksumLength != nativeChecksumLength {
		return fmt.Errorf("unsupported native checksum length %d", checksumLength)
	}
	if len(frame) < nativeEventHeaderLength+checksumLength {
		return fmt.Errorf("native event is truncated")
	}
	eventSize := int(binary.LittleEndian.Uint32(frame[9:13]))
	if eventSize != len(frame) || eventSize < nativeEventHeaderLength+checksumLength {
		return fmt.Errorf("native event has invalid size")
	}
	if checksumLength == nativeChecksumLength && !nativeFrameHasChecksum(frame, checksumLength) {
		return ErrChecksumMismatch
	}
	return nil
}

func nativeEventBody(event NativeBinlogEvent) []byte {
	checksumLength := nativeChecksumLengthForEvent(event)
	if len(event.Raw) < nativeEventHeaderLength+checksumLength {
		return nil
	}
	return event.Raw[nativeEventHeaderLength : len(event.Raw)-checksumLength]
}

func decodeNativeGTID(body []byte) (GTID, error) {
	if len(body) < 25 {
		return GTID{}, fmt.Errorf("truncated GTID_EVENT")
	}
	// Native GTID_EVENT carries the 16-byte SID rather than the textual UUID.
	// Render it in MySQL's canonical UUID form when possible; this keeps a
	// physical stream decoded by this package compatible with the logical GTID
	// set used by Replica.Apply. Hashed non-UUID source names remain
	// intentionally lossy, but still have a stable SID-derived identity.
	sid := hex.EncodeToString(body[1:17])
	return GTID{UUID: nativeGTIDUUID(sid), Seq: binary.LittleEndian.Uint64(body[17:25])}, nil
}

// decodeNativeTaggedGTID consumes the MySQL serialization archive used by
// GTID_TAGGED_LOG_EVENT. The archive is deliberately parsed here instead of
// treating the event as an untagged GTID: the tag is part of transaction
// identity and must participate in exactly-once filtering.
func decodeNativeTaggedGTID(body []byte, sourceUUID string) (GTID, error) {
	offset := 0
	version, next, err := readNativeSerializationVarlen(body, offset, false)
	if err != nil || version != 1 {
		return GTID{}, fmt.Errorf("invalid GTID_TAGGED_LOG_EVENT serialization version")
	}
	offset = next
	encodedSize, next, err := readNativeSerializationVarlen(body, offset, false)
	if err != nil || encodedSize > uint64(len(body)-next) {
		return GTID{}, fmt.Errorf("invalid GTID_TAGGED_LOG_EVENT encoded size")
	}
	offset = next
	lastNonIgnorable, next, err := readNativeSerializationVarlen(body, offset, false)
	if err != nil {
		return GTID{}, fmt.Errorf("invalid GTID_TAGGED_LOG_EVENT metadata")
	}
	offset = next
	fieldsEnd := offset + int(encodedSize)
	if fieldsEnd != len(body) || lastNonIgnorable > 12 {
		return GTID{}, fmt.Errorf("invalid GTID_TAGGED_LOG_EVENT field boundary")
	}
	var flags uint64
	var sid []byte
	var gno int64
	var tag string
	seen := make(map[uint64]bool)
	for offset < fieldsEnd {
		fieldID, next, err := readNativeSerializationVarlen(body, offset, false)
		if err != nil || fieldID > 11 || seen[fieldID] {
			return GTID{}, fmt.Errorf("invalid GTID_TAGGED_LOG_EVENT field id")
		}
		seen[fieldID] = true
		offset = next
		switch fieldID {
		case 0:
			flags, offset, err = readNativeSerializationVarlen(body, offset, false)
		case 1:
			if len(body)-offset < 16 {
				return GTID{}, fmt.Errorf("truncated GTID_TAGGED_LOG_EVENT SID")
			}
			sid = append([]byte(nil), body[offset:offset+16]...)
			offset += 16
		case 2:
			gno, offset, err = readNativeSerializationVarlenSigned(body, offset)
		case 3:
			var raw []byte
			raw, offset, err = readNativeSerializationString(body, offset)
			tag = string(raw)
		case 4, 5:
			_, offset, err = readNativeSerializationVarlenSigned(body, offset)
		case 6, 7, 8, 9, 10, 11:
			_, offset, err = readNativeSerializationVarlen(body, offset, false)
		}
		if err != nil {
			return GTID{}, fmt.Errorf("invalid GTID_TAGGED_LOG_EVENT field %d", fieldID)
		}
	}
	if !seen[1] || !seen[2] || !seen[3] || gno <= 0 || flags > 255 {
		return GTID{}, fmt.Errorf("incomplete GTID_TAGGED_LOG_EVENT")
	}
	if !validNativeGTIDTag(tag) {
		return GTID{}, fmt.Errorf("invalid GTID_TAGGED_LOG_EVENT tag")
	}
	uuid := nativeGTIDUUID(hex.EncodeToString(sid))
	if sourceUUID != "" && bytes.Equal(nativeGTIDSID(sourceUUID), sid) {
		uuid = sourceUUID
	}
	if tag != "" {
		uuid += ":" + tag
	}
	return GTID{UUID: uuid, Seq: uint64(gno)}, nil
}

type nativeTaggedGTIDIndexFields struct {
	sid      []byte
	tag      string
	sequence uint64
}

// decodeNativeTaggedGTIDIndexFields extracts only the identity fields needed
// by the physical position index. The full decoder remains the authority for
// archive validation; this pass preserves the original 16-byte SID instead of
// trying to reverse a source-name hash from the rendered GTID.
func decodeNativeTaggedGTIDIndexFields(body []byte) (nativeTaggedGTIDIndexFields, error) {
	if _, err := decodeNativeTaggedGTID(body, ""); err != nil {
		return nativeTaggedGTIDIndexFields{}, err
	}
	offset := 0
	_, next, err := readNativeSerializationVarlen(body, offset, false)
	if err != nil {
		return nativeTaggedGTIDIndexFields{}, err
	}
	offset = next
	encodedSize, next, err := readNativeSerializationVarlen(body, offset, false)
	if err != nil {
		return nativeTaggedGTIDIndexFields{}, err
	}
	offset = next
	_, offset, err = readNativeSerializationVarlen(body, offset, false)
	if err != nil || offset+int(encodedSize) != len(body) {
		return nativeTaggedGTIDIndexFields{}, fmt.Errorf("invalid GTID_TAGGED_LOG_EVENT index boundary")
	}
	fieldsEnd := len(body)
	var result nativeTaggedGTIDIndexFields
	for offset < fieldsEnd {
		fieldID, next, fieldErr := readNativeSerializationVarlen(body, offset, false)
		if fieldErr != nil {
			return nativeTaggedGTIDIndexFields{}, fieldErr
		}
		offset = next
		switch fieldID {
		case 1:
			if len(body)-offset < 16 {
				return nativeTaggedGTIDIndexFields{}, fmt.Errorf("truncated GTID_TAGGED_LOG_EVENT SID")
			}
			result.sid = append([]byte(nil), body[offset:offset+16]...)
			offset += 16
		case 2:
			value, next, fieldErr := readNativeSerializationVarlenSigned(body, offset)
			if fieldErr != nil || value <= 0 {
				return nativeTaggedGTIDIndexFields{}, fmt.Errorf("invalid GTID_TAGGED_LOG_EVENT GNO")
			}
			result.sequence = uint64(value)
			offset = next
		case 3:
			value, next, fieldErr := readNativeSerializationString(body, offset)
			if fieldErr != nil {
				return nativeTaggedGTIDIndexFields{}, fieldErr
			}
			result.tag = string(value)
			offset = next
		case 4, 5:
			_, offset, fieldErr = readNativeSerializationVarlenSigned(body, offset)
			if fieldErr != nil {
				return nativeTaggedGTIDIndexFields{}, fieldErr
			}
		default:
			_, offset, fieldErr = readNativeSerializationVarlen(body, offset, false)
			if fieldErr != nil {
				return nativeTaggedGTIDIndexFields{}, fieldErr
			}
		}
	}
	if len(result.sid) != 16 || result.sequence == 0 || !validNativeGTIDTag(result.tag) {
		return nativeTaggedGTIDIndexFields{}, fmt.Errorf("incomplete GTID_TAGGED_LOG_EVENT index identity")
	}
	return result, nil
}

func readNativeSerializationVarlen(raw []byte, offset int, signed bool) (uint64, int, error) {
	if signed {
		value, next, err := readNativeSerializationVarlenSigned(raw, offset)
		if err != nil || value < 0 {
			return 0, 0, fmt.Errorf("invalid signed serialization integer")
		}
		return uint64(value), next, nil
	}
	if offset < 0 || offset >= len(raw) {
		return 0, 0, fmt.Errorf("truncated serialization integer")
	}
	first := raw[offset]
	numBytes := 1
	for numBytes <= 8 && first&byte(1<<uint(numBytes-1)) != 0 {
		numBytes++
	}
	if numBytes > len(raw)-offset {
		return 0, 0, fmt.Errorf("truncated serialization integer")
	}
	value := uint64(first >> uint(numBytes))
	if numBytes > 1 {
		shift := 8 - numBytes
		if numBytes == 9 {
			shift = 0
		}
		for index := 1; index < numBytes; index++ {
			value |= uint64(raw[offset+index]) << uint(8*(index-1)+shift)
		}
	}
	return value, offset + numBytes, nil
}

func readNativeSerializationVarlenSigned(raw []byte, offset int) (int64, int, error) {
	value, next, err := readNativeSerializationVarlen(raw, offset, false)
	if err != nil {
		return 0, 0, err
	}
	negative := value&1 != 0
	value >>= 1
	if negative {
		if value > uint64(math.MaxInt64) {
			return 0, 0, fmt.Errorf("signed serialization integer overflows")
		}
		return -int64(value) - 1, next, nil
	}
	if value > uint64(math.MaxInt64) {
		return 0, 0, fmt.Errorf("signed serialization integer overflows")
	}
	return int64(value), next, nil
}

func readNativeSerializationString(raw []byte, offset int) ([]byte, int, error) {
	length, next, err := readNativeSerializationVarlen(raw, offset, false)
	if err != nil || length > uint64(len(raw)-next) {
		return nil, 0, fmt.Errorf("truncated serialization string")
	}
	return append([]byte(nil), raw[next:next+int(length)]...), next + int(length), nil
}

func validNativeGTIDTag(tag string) bool {
	if len(tag) > 32 || tag == "" {
		return true
	}
	for index, char := range []byte(tag) {
		if index == 0 {
			if !(char == '_' || char >= 'a' && char <= 'z') {
				return false
			}
			continue
		}
		if !(char == '_' || char >= 'a' && char <= 'z' || char >= '0' && char <= '9') {
			return false
		}
	}
	return true
}

func decodeNativePreviousGTIDs(body []byte, sourceUUID string) (GTIDIntervals, error) {
	if len(body) < 8 {
		return nil, fmt.Errorf("truncated PREVIOUS_GTIDS_EVENT")
	}
	sidCount := binary.LittleEndian.Uint64(body[:8])
	if sidCount > uint64((len(body)-8)/24) {
		return nil, fmt.Errorf("invalid PREVIOUS_GTIDS_EVENT SID count")
	}
	result := GTIDIntervals{}
	offset := 8
	for sidIndex := uint64(0); sidIndex < sidCount; sidIndex++ {
		if len(body)-offset < 24 {
			return nil, fmt.Errorf("truncated PREVIOUS_GTIDS_EVENT SID %d", sidIndex)
		}
		sid := body[offset : offset+16]
		offset += 16
		uuid := nativeGTIDUUID(hex.EncodeToString(sid))
		if sourceUUID != "" && bytes.Equal(nativeGTIDSID(sourceUUID), sid) {
			uuid = sourceUUID
		}
		intervalCount := binary.LittleEndian.Uint64(body[offset : offset+8])
		offset += 8
		if intervalCount > uint64((len(body)-offset)/16) {
			return nil, fmt.Errorf("invalid PREVIOUS_GTIDS_EVENT interval count for SID %d", sidIndex)
		}
		for intervalIndex := uint64(0); intervalIndex < intervalCount; intervalIndex++ {
			start := binary.LittleEndian.Uint64(body[offset : offset+8])
			end := binary.LittleEndian.Uint64(body[offset+8 : offset+16])
			offset += 16
			if err := result.AddHalfOpen(uuid, start, end); err != nil {
				return nil, fmt.Errorf("invalid PREVIOUS_GTIDS_EVENT interval %d for SID %d: %w", intervalIndex, sidIndex, err)
			}
		}
	}
	if offset != len(body) {
		return nil, fmt.Errorf("PREVIOUS_GTIDS_EVENT has %d trailing bytes", len(body)-offset)
	}
	return result, nil
}

func decodeNativeXAPrepare(body []byte) (bool, error) {
	_, onePhase, err := decodeNativeXAPrepareDetails(body)
	return onePhase, err
}

func decodeNativeXAPrepareDetails(body []byte) (string, bool, error) {
	if len(body) < 13 {
		return "", false, fmt.Errorf("truncated XA_PREPARE_EVENT")
	}
	formatID := binary.LittleEndian.Uint32(body[1:5])
	gtridLength := uint64(binary.LittleEndian.Uint32(body[5:9]))
	bqualLength := uint64(binary.LittleEndian.Uint32(body[9:13]))
	if gtridLength > 64 || bqualLength > 64 || gtridLength+bqualLength > 128 {
		return "", false, fmt.Errorf("invalid XA_PREPARE_EVENT XID lengths")
	}
	dataLength := gtridLength + bqualLength
	if dataLength > uint64(len(body)-13) {
		return "", false, fmt.Errorf("truncated XA_PREPARE_EVENT XID")
	}
	gtrid := body[13 : 13+int(gtridLength)]
	bqual := body[13+int(gtridLength) : 13+int(dataLength)]
	return nativeXAKey(formatID, gtrid, bqual), body[0] != 0, nil
}

func decodeNativeTransactionPayload(event NativeBinlogEvent) ([]NativeBinlogEvent, error) {
	if len(event.Raw) < nativeEventHeaderLength+nativeChecksumLengthForEvent(event) {
		return nil, fmt.Errorf("truncated TRANSACTION_PAYLOAD_EVENT")
	}
	body := nativeEventBody(event)
	offset := 0
	var payloadSize, compressionType, uncompressedSize uint64
	havePayloadSize, haveCompressionType, haveUncompressedSize, haveEndMark := false, false, false, false
	for offset < len(body) {
		fieldType, next, err := readNativeLenenc(body, offset)
		if err != nil {
			return nil, fmt.Errorf("invalid TRANSACTION_PAYLOAD_EVENT header: %w", err)
		}
		offset = next
		if fieldType == 0 {
			haveEndMark = true
			break
		}
		fieldLength, next, err := readNativeLenenc(body, offset)
		if err != nil || fieldLength > uint64(len(body)-next) {
			return nil, fmt.Errorf("invalid TRANSACTION_PAYLOAD_EVENT field %d", fieldType)
		}
		fieldStart := next
		fieldEnd := fieldStart + int(fieldLength)
		switch fieldType {
		case 1:
			value, valueEnd, valueErr := readNativeLenenc(body, fieldStart)
			if valueErr != nil || valueEnd != fieldEnd {
				return nil, fmt.Errorf("invalid TRANSACTION_PAYLOAD_EVENT field %d value", fieldType)
			}
			payloadSize, havePayloadSize = value, true
		case 2:
			value, valueEnd, valueErr := readNativeLenenc(body, fieldStart)
			if valueErr != nil || valueEnd != fieldEnd {
				return nil, fmt.Errorf("invalid TRANSACTION_PAYLOAD_EVENT field %d value", fieldType)
			}
			compressionType, haveCompressionType = value, true
		case 3:
			value, valueEnd, valueErr := readNativeLenenc(body, fieldStart)
			if valueErr != nil || valueEnd != fieldEnd {
				return nil, fmt.Errorf("invalid TRANSACTION_PAYLOAD_EVENT field %d value", fieldType)
			}
			uncompressedSize, haveUncompressedSize = value, true
		default:
			// Unknown fields are length-delimited so newer MySQL versions can
			// extend this header without breaking older consumers.
		}
		offset = fieldEnd
	}
	if !haveEndMark || !havePayloadSize || !haveCompressionType {
		return nil, fmt.Errorf("incomplete TRANSACTION_PAYLOAD_EVENT header")
	}
	if payloadSize > uint64(len(body)-offset) {
		return nil, fmt.Errorf("TRANSACTION_PAYLOAD_EVENT payload exceeds event")
	}
	compressed := body[offset : offset+int(payloadSize)]
	var payload []byte
	switch compressionType {
	case 255:
		payload = append([]byte(nil), compressed...)
	case 0:
		decoder, err := zstd.NewReader(nil)
		if err != nil {
			return nil, fmt.Errorf("initialize transaction payload zstd decoder: %w", err)
		}
		payload, err = decoder.DecodeAll(compressed, nil)
		decoder.Close()
		if err != nil {
			return nil, fmt.Errorf("decompress TRANSACTION_PAYLOAD_EVENT: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported TRANSACTION_PAYLOAD_EVENT compression type %d", compressionType)
	}
	if haveUncompressedSize && uint64(len(payload)) != uncompressedSize {
		return nil, fmt.Errorf("TRANSACTION_PAYLOAD_EVENT uncompressed size %d does not match %d", uncompressedSize, len(payload))
	}
	return nativePayloadFrames(event, payload)
}

func nativePayloadFrames(container NativeBinlogEvent, payload []byte) ([]NativeBinlogEvent, error) {
	frames := make([]NativeBinlogEvent, 0)
	for offset := 0; offset < len(payload); {
		if len(payload)-offset < nativeEventHeaderLength {
			return nil, fmt.Errorf("truncated event inside TRANSACTION_PAYLOAD_EVENT")
		}
		eventSize := int(binary.LittleEndian.Uint32(payload[offset+9 : offset+13]))
		if eventSize < nativeEventHeaderLength || eventSize > len(payload)-offset {
			return nil, fmt.Errorf("invalid nested event size %d in TRANSACTION_PAYLOAD_EVENT", eventSize)
		}
		inner := make([]byte, eventSize+nativeChecksumLength)
		copy(inner, payload[offset:offset+eventSize])
		binary.LittleEndian.PutUint32(inner[9:13], uint32(len(inner)))
		binary.LittleEndian.PutUint32(inner[len(inner)-nativeChecksumLength:], crc32IEEE(inner[:len(inner)-nativeChecksumLength]))
		frames = append(frames, NativeBinlogEvent{
			File:           container.File,
			Position:       container.Position,
			EndPosition:    container.EndPosition,
			Type:           inner[4],
			Raw:            inner,
			ChecksumLength: nativeChecksumLength,
			ChecksumKnown:  true,
		})
		offset += eventSize
	}
	return frames, nil
}

func decodeNativeQuery(body []byte) (string, string, error) {
	if len(body) < 14 {
		return "", "", fmt.Errorf("truncated QUERY_EVENT")
	}
	databaseLength := int(body[8])
	if databaseLength > len(body)-14 || body[13+databaseLength] != 0 {
		return "", "", fmt.Errorf("invalid QUERY_EVENT database")
	}
	database := string(body[13 : 13+databaseLength])
	return string(body[14+databaseLength:]), database, nil
}

func (d *NativeBinlogDecoder) consumeNativeXAQuery(statement string, position uint64, transactions *[]BinlogEvent) (bool, error) {
	action, key, ok := nativeXAQuery(statement)
	if !ok {
		return false, nil
	}
	switch action {
	case "COMMIT":
		prepared, exists := d.preparedXA[key]
		if !exists {
			return true, fmt.Errorf("native XA COMMIT references unknown prepared XID")
		}
		prepared.Type = EventCommit
		if position != 0 {
			prepared.Position = position
		}
		*transactions = append(*transactions, prepared)
		delete(d.preparedXA, key)
	case "ROLLBACK":
		if _, exists := d.preparedXA[key]; !exists {
			return true, fmt.Errorf("native XA ROLLBACK references unknown prepared XID")
		}
		delete(d.preparedXA, key)
	}
	return true, nil
}

func nativeXAQuery(statement string) (action, key string, ok bool) {
	fields := strings.Fields(strings.TrimSpace(strings.TrimSuffix(statement, ";")))
	if len(fields) < 3 || !strings.EqualFold(fields[0], "XA") {
		return "", "", false
	}
	action = strings.ToUpper(fields[1])
	if action != "COMMIT" && action != "ROLLBACK" {
		return "", "", false
	}
	argumentText := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(strings.TrimSuffix(statement, ";")), fields[0]))
	argumentText = strings.TrimSpace(strings.TrimPrefix(argumentText, fields[1]))
	argumentText = strings.TrimSpace(strings.TrimSuffix(argumentText, "ONE PHASE"))
	arguments := splitNativeXAArguments(argumentText)
	if len(arguments) != 3 {
		return "", "", false
	}
	formatID, err := strconv.ParseUint(strings.TrimSpace(arguments[2]), 10, 32)
	if err != nil {
		return "", "", false
	}
	return action, nativeXAKey(uint32(formatID), []byte(unquoteNativeXAArgument(arguments[0])), []byte(unquoteNativeXAArgument(arguments[1]))), true
}

func splitNativeXAArguments(raw string) []string {
	arguments := make([]string, 0, 3)
	start := 0
	quoted := byte(0)
	for index := 0; index < len(raw); index++ {
		switch raw[index] {
		case '\'', '"':
			if quoted == 0 {
				quoted = raw[index]
			} else if quoted == raw[index] && (index == 0 || raw[index-1] != '\\') {
				quoted = 0
			}
		case ',':
			if quoted == 0 {
				arguments = append(arguments, strings.TrimSpace(raw[start:index]))
				start = index + 1
			}
		}
	}
	if quoted != 0 {
		return nil
	}
	arguments = append(arguments, strings.TrimSpace(raw[start:]))
	return arguments
}

func unquoteNativeXAArgument(raw string) string {
	raw = strings.TrimSpace(raw)
	if len(raw) >= 2 && ((raw[0] == '\'' && raw[len(raw)-1] == '\'') || (raw[0] == '"' && raw[len(raw)-1] == '"')) {
		return strings.ReplaceAll(raw[1:len(raw)-1], "\\'", "'")
	}
	return raw
}

func nativeXAKey(formatID uint32, gtrid, bqual []byte) string {
	return fmt.Sprintf("%d:%s:%s", formatID, hex.EncodeToString(gtrid), hex.EncodeToString(bqual))
}

func nativeQueryBoundary(statement string) string {
	fields := strings.Fields(strings.TrimSpace(strings.TrimSuffix(statement, ";")))
	if len(fields) == 0 {
		return ""
	}
	switch strings.ToUpper(fields[0]) {
	case "BEGIN", "START":
		return "begin"
	case "COMMIT":
		return "commit"
	case "ROLLBACK":
		if len(fields) > 1 && strings.EqualFold(fields[1], "TO") {
			return ""
		}
		return "rollback"
	default:
		return ""
	}
}

func crc32IEEE(payload []byte) uint32 {
	return crc32.ChecksumIEEE(payload)
}

func decodeNativeTableMap(body []byte) (nativeDecoderTable, uint64, error) {
	var table nativeDecoderTable
	if len(body) < 8 {
		return table, 0, fmt.Errorf("truncated TABLE_MAP_EVENT")
	}
	tableID := nativeUint48(body[:6])
	offset := 8
	database, next, err := readNativeLengthPrefixedString(body, offset)
	if err != nil {
		return table, 0, err
	}
	offset = next
	name, next, err := readNativeLengthPrefixedString(body, offset)
	if err != nil {
		return table, 0, err
	}
	offset = next
	columnCount, next, err := readNativeLenenc(body, offset)
	if err != nil || columnCount == 0 || columnCount > 65535 {
		return table, 0, fmt.Errorf("invalid TABLE_MAP column count")
	}
	offset = next
	if len(body)-offset < int(columnCount) {
		return table, 0, fmt.Errorf("truncated TABLE_MAP type codes")
	}
	types := append([]byte(nil), body[offset:offset+int(columnCount)]...)
	offset += int(columnCount)
	metadataLength, next, err := readNativeLenenc(body, offset)
	if err != nil || metadataLength > uint64(len(body)-next) {
		return table, 0, fmt.Errorf("invalid TABLE_MAP metadata")
	}
	offset = next
	metadataRaw := body[offset : offset+int(metadataLength)]
	offset += int(metadataLength)
	if len(body)-offset < (int(columnCount)+7)/8 {
		return table, 0, fmt.Errorf("truncated TABLE_MAP null bitmap")
	}
	offset += (int(columnCount) + 7) / 8
	metadata, err := splitNativeColumnMetadata(types, metadataRaw)
	if err != nil {
		return table, 0, err
	}
	unsigned := make([]bool, len(types))
	enumMembers := make(map[int][]string)
	setMembers := make(map[int][]string)
	columnNames := make([]string, len(types))
	for index := range columnNames {
		columnNames[index] = fmt.Sprintf("column_%d", index+1)
	}
	for offset < len(body) {
		kind := body[offset]
		offset++
		length, next, err := readNativeLenenc(body, offset)
		if err != nil || length > uint64(len(body)-next) {
			return table, 0, fmt.Errorf("invalid TABLE_MAP optional metadata")
		}
		value := body[next : next+int(length)]
		offset = next + int(length)
		if kind == 1 {
			for index := range types {
				if nativeDecoderNumericType(types[index]) {
					numericIndex := nativeDecoderNumericIndex(types, index)
					if numericIndex/8 < len(value) {
						unsigned[index] = value[numericIndex/8]&(0x80>>uint(numericIndex%8)) != 0
					}
				}
			}
		}
		if kind == 4 {
			nameOffset := 0
			for index := range columnNames {
				if nameOffset >= len(value) {
					break
				}
				length := int(value[nameOffset])
				nameOffset++
				if length > len(value)-nameOffset {
					return table, 0, fmt.Errorf("invalid TABLE_MAP column names")
				}
				columnNames[index] = string(value[nameOffset : nameOffset+length])
				nameOffset += length
			}
		}
		if kind == 5 || kind == 6 {
			listCount := 0
			for index := range types {
				if types[index] == 254 && len(metadata[index]) >= 1 && ((kind == 5 && metadata[index][0] == 248) || (kind == 6 && metadata[index][0] == 247)) {
					listCount++
				}
			}
			members, err := decodeNativeEnumSetOptional(value, listCount)
			if err != nil {
				return table, 0, err
			}
			memberIndex := 0
			for index := range types {
				if types[index] != 254 || len(metadata[index]) < 1 || (kind == 5 && metadata[index][0] != 248) || (kind == 6 && metadata[index][0] != 247) {
					continue
				}
				if memberIndex >= len(members) {
					break
				}
				if kind == 5 {
					setMembers[index] = members[memberIndex]
				} else {
					enumMembers[index] = members[memberIndex]
				}
				memberIndex++
			}
		}
		switch kind {
		case 2:
			defaultCharsets, err := decodeNativeCharsetMetadata(value)
			if err != nil {
				return table, 0, err
			}
			table.defaultCharsets = defaultCharsets
		case 3:
			charsets, err := decodeNativeLenencSequence(value)
			if err != nil {
				return table, 0, err
			}
			table.columnCharsets = charsets
		case 7:
			geometryTypes, err := decodeNativeLenencSequence(value)
			if err != nil {
				return table, 0, err
			}
			table.geometryTypes = geometryTypes
		case 8:
			primaryKey, err := decodeNativePrimaryKeyMetadata(value, false)
			if err != nil {
				return table, 0, err
			}
			table.primaryKey = primaryKey
		case 9:
			primaryKey, err := decodeNativePrimaryKeyMetadata(value, true)
			if err != nil {
				return table, 0, err
			}
			table.primaryKey = primaryKey
		case 10:
			defaultCharsets, err := decodeNativeCharsetMetadata(value)
			if err != nil {
				return table, 0, err
			}
			table.enumSetDefaultCharsets = defaultCharsets
		case 11:
			charsets, err := decodeNativeLenencSequence(value)
			if err != nil {
				return table, 0, err
			}
			table.enumSetColumnCharsets = charsets
		case 12:
			visibility, err := decodeNativeVisibilityMetadata(value)
			if err != nil {
				return table, 0, err
			}
			table.columnVisibility = visibility
		case 13:
			dimensions, err := decodeNativeLenencSequence(value)
			if err != nil {
				return table, 0, err
			}
			table.vectorDimensions = dimensions
		}
	}
	table.database, table.table, table.columns, table.types, table.metadata, table.unsigned = database, name, make([]string, columnCount), types, metadata, unsigned
	table.enumMembers, table.setMembers = enumMembers, setMembers
	for index := range table.columns {
		table.columns[index] = columnNames[index]
	}
	return table, tableID, nil
}

func decodeNativeLenencSequence(raw []byte) ([]uint64, error) {
	values := make([]uint64, 0)
	for offset := 0; offset < len(raw); {
		value, next, err := readNativeLenenc(raw, offset)
		if err != nil || next <= offset {
			return nil, fmt.Errorf("invalid TABLE_MAP optional metadata sequence")
		}
		values = append(values, value)
		offset = next
	}
	return values, nil
}

func decodeNativeCharsetMetadata(raw []byte) (nativeDecoderCharsetMetadata, error) {
	metadata := nativeDecoderCharsetMetadata{}
	defaultCharset, offset, err := readNativeLenenc(raw, 0)
	if err != nil {
		return metadata, fmt.Errorf("invalid TABLE_MAP default charset metadata")
	}
	metadata.defaultCharset = defaultCharset
	for offset < len(raw) {
		column, next, err := readNativeLenenc(raw, offset)
		if err != nil {
			return metadata, fmt.Errorf("invalid TABLE_MAP charset column metadata")
		}
		charset, nextCharset, err := readNativeLenenc(raw, next)
		if err != nil || nextCharset <= next {
			return metadata, fmt.Errorf("invalid TABLE_MAP charset value metadata")
		}
		metadata.pairs = append(metadata.pairs, [2]uint64{column, charset})
		offset = nextCharset
	}
	return metadata, nil
}

func decodeNativePrimaryKeyMetadata(raw []byte, withPrefix bool) ([]nativeDecoderKeyPart, error) {
	parts := make([]nativeDecoderKeyPart, 0)
	for offset := 0; offset < len(raw); {
		column, next, err := readNativeLenenc(raw, offset)
		if err != nil {
			return nil, fmt.Errorf("invalid TABLE_MAP primary key metadata")
		}
		prefix := uint64(0)
		if withPrefix {
			var nextPrefix int
			prefix, nextPrefix, err = readNativeLenenc(raw, next)
			if err != nil || nextPrefix <= next {
				return nil, fmt.Errorf("invalid TABLE_MAP primary key prefix metadata")
			}
			next = nextPrefix
		}
		parts = append(parts, nativeDecoderKeyPart{column: column, prefix: prefix})
		offset = next
	}
	return parts, nil
}

func decodeNativeVisibilityMetadata(raw []byte) ([]bool, error) {
	visibility := make([]bool, 0, len(raw)*8)
	for _, value := range raw {
		for mask := byte(0x80); mask != 0; mask >>= 1 {
			visibility = append(visibility, value&mask != 0)
		}
	}
	return visibility, nil
}

func splitNativeColumnMetadata(types []byte, raw []byte) ([][]byte, error) {
	result := make([][]byte, len(types))
	offset := 0
	for index, typeCode := range types {
		width := nativeDecoderMetadataWidth(typeCode, raw[offset:])
		if width < 0 || len(raw)-offset < width {
			return nil, fmt.Errorf("truncated TABLE_MAP metadata for column %d", index)
		}
		result[index] = append([]byte(nil), raw[offset:offset+width]...)
		offset += width
	}
	if offset != len(raw) {
		return nil, fmt.Errorf("unexpected TABLE_MAP metadata bytes")
	}
	return result, nil
}

func nativeDecoderMetadataWidth(typeCode byte, raw []byte) int {
	switch typeCode {
	case 4, 5, 17, 18, 19, 245, 255, 249, 250, 251, 252:
		return 1
	case 0, 15, 16, 246, 253, 254:
		return 2
	default:
		return 0
	}
}

func (d *NativeBinlogDecoder) decodeNativeRows(eventType byte, body []byte) ([]RowChange, error) {
	if len(body) < 10 {
		return nil, fmt.Errorf("truncated ROWS_EVENT")
	}
	tableID := nativeUint48(body[:6])
	table, ok := d.tables[tableID]
	if !ok {
		return nil, fmt.Errorf("ROWS_EVENT references unknown table id %d", tableID)
	}
	offset := 8
	extra := []byte(nil)
	// v1 row events have only the table-id/flags post-header; v2 adds a
	// two-byte total length followed by extra-row-info before the column count.
	// Accepting both layouts lets the decoder consume older upstream binlogs.
	legacy := eventType == 23 || eventType == 24 || eventType == 25
	if !legacy {
		if len(body)-offset < 2 {
			return nil, fmt.Errorf("truncated ROWS_EVENT extra data length")
		}
		extraLength := int(binary.LittleEndian.Uint16(body[offset : offset+2]))
		offset += 2
		if extraLength < 2 || extraLength-2 > len(body)-offset {
			return nil, fmt.Errorf("invalid ROWS_EVENT extra data length")
		}
		extra = append([]byte(nil), body[offset:offset+extraLength-2]...)
		offset += extraLength - 2
	}
	columnCount, next, err := readNativeLenenc(body, offset)
	if err != nil || int(columnCount) != len(table.types) {
		return nil, fmt.Errorf("ROWS_EVENT column count does not match TABLE_MAP")
	}
	offset = next
	changes := make([]RowChange, 0)
	for offset < len(body) {
		beforeBitmap, next, err := readNativeBitmap(body, offset, len(table.types))
		if err != nil {
			return nil, err
		}
		offset = next
		afterBitmap := beforeBitmap
		var beforeNulls, afterNulls []byte
		isUpdate := eventType == 24 || eventType == 31 || eventType == 39
		if isUpdate {
			beforeNulls, next, err = readNativeNullBitmap(body, offset, table, beforeBitmap)
			if err != nil {
				return nil, err
			}
			offset = next
			afterBitmap, next, err = readNativeBitmap(body, offset, len(table.types))
			if err != nil {
				return nil, err
			}
			offset = next
			afterNulls, next, err = readNativeNullBitmap(body, offset, table, afterBitmap)
			if err != nil {
				return nil, err
			}
			offset = next
		}
		var before, after map[string]interface{}
		var partial map[string][]JSONPartialUpdate
		if isUpdate {
			before, next, err = decodeNativeRowValues(table, beforeBitmap, beforeNulls, body, offset)
			if err != nil {
				return nil, err
			}
			offset = next
		}
		partialColumns := make([]bool, len(table.types))
		if eventType == 39 {
			if len(body)-offset < 1+len(beforeBitmap) {
				return nil, fmt.Errorf("truncated PARTIAL_UPDATE_ROWS_EVENT options")
			}
			if body[offset] != 1 {
				return nil, fmt.Errorf("unsupported ROWS_EVENT value options %d", body[offset])
			}
			offset++
			for index := range partialColumns {
				partialColumns[index] = body[offset+index/8]&(1<<uint(index%8)) != 0
			}
			offset += len(beforeBitmap)
		}
		var decodedImage map[string]interface{}
		var decodedPartial map[string][]JSONPartialUpdate
		var imageEnd int
		if isUpdate {
			decodedImage, decodedPartial, imageEnd, err = decodeNativeRowValuesWithPartial(table, afterBitmap, afterNulls, partialColumns, body, offset)
		} else {
			decodedImage, decodedPartial, imageEnd, err = decodeNativeRowImageWithPartial(table, afterBitmap, partialColumns, body, offset)
		}
		if err != nil {
			return nil, err
		}
		offset = imageEnd
		if isUpdate || eventType == 23 || eventType == 30 {
			after, partial, next = decodedImage, decodedPartial, imageEnd
		} else {
			before, partial, next = decodedImage, decodedPartial, imageEnd
		}
		action := "insert"
		switch eventType {
		case 24, 31, 39:
			action = "update"
		case 25, 32:
			action = "delete"
		}
		change := RowChange{Table: table.database + "." + table.table, Action: action, Columns: append([]string(nil), table.columns...), Before: before, After: after, ExtraRowInfo: extra}
		change.ColumnTypes = make(map[string]string, len(table.types))
		for index, column := range table.columns {
			change.ColumnTypes[column] = nativeDecoderColumnType(table, index)
		}
		if eventType == 25 || eventType == 32 {
			change.After = nil
		}
		if len(partial) > 0 {
			change.PartialJSONUpdates = partial
			for column, updates := range partial {
				if beforeValue, exists := before[column]; exists {
					if reconstructed, ok := ApplyJSONPartialUpdates(beforeValue, updates); ok {
						change.After[column] = reconstructed
					}
				}
			}
		}
		changes = append(changes, change)
	}
	return changes, nil
}

func decodeNativeRowImage(table nativeDecoderTable, bitmap []byte, body []byte, offset int) (map[string]interface{}, int, error) {
	values, _, next, err := decodeNativeRowImageWithPartial(table, bitmap, nil, body, offset)
	return values, next, err
}

func decodeNativeRowImageWithPartial(table nativeDecoderTable, bitmap []byte, partial []bool, body []byte, offset int) (map[string]interface{}, map[string][]JSONPartialUpdate, int, error) {
	nulls, next, err := readNativeNullBitmap(body, offset, table, bitmap)
	if err != nil {
		return nil, nil, 0, err
	}
	return decodeNativeRowValuesWithPartial(table, bitmap, nulls, partial, body, next)
}

func decodeNativeRowValues(table nativeDecoderTable, bitmap, nulls, body []byte, offset int) (map[string]interface{}, int, error) {
	values, _, next, err := decodeNativeRowValuesWithPartial(table, bitmap, nulls, nil, body, offset)
	return values, next, err
}

func decodeNativeRowValuesWithPartial(table nativeDecoderTable, bitmap, nulls []byte, partial []bool, body []byte, offset int) (map[string]interface{}, map[string][]JSONPartialUpdate, int, error) {
	present := 0
	for index := range table.types {
		if bitmap[index/8]&(1<<uint(index%8)) != 0 {
			present++
		}
	}
	if len(nulls) < (present+7)/8 {
		return nil, nil, 0, fmt.Errorf("truncated ROWS_EVENT null bitmap")
	}
	values := make(map[string]interface{}, present)
	partialUpdates := make(map[string][]JSONPartialUpdate)
	presentIndex := 0
	for index, typeCode := range table.types {
		if bitmap[index/8]&(1<<uint(index%8)) == 0 {
			continue
		}
		column := table.columns[index]
		if nulls[presentIndex/8]&(1<<uint(presentIndex%8)) != 0 {
			values[column] = nil
			presentIndex++
			continue
		}
		value, updates, next, err := decodeNativeValue(typeCode, table.metadata[index], table.unsigned[index], partial != nil && index < len(partial) && partial[index], body, offset)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("decode column %s: %w", column, err)
		}
		valueOffset := offset
		offset = next
		if typeCode == 254 && nativeDecoderBinaryStringColumn(table, index) {
			value = append([]byte(nil), body[valueOffset:next]...)
		}
		if typeCode == 254 {
			if members := table.enumMembers[index]; len(members) > 0 {
				value = decodeNativeEnumValue(value, members)
			} else if members := table.setMembers[index]; len(members) > 0 {
				value = decodeNativeSetValue(value, members)
			}
		}
		values[column] = value
		if len(updates) > 0 {
			partialUpdates[column] = updates
		}
		presentIndex++
	}
	return values, partialUpdates, offset, nil
}

func decodeNativeValue(typeCode byte, metadata []byte, unsigned, partial bool, body []byte, offset int) (interface{}, []JSONPartialUpdate, int, error) {
	if typeCode == 6 { // MYSQL_TYPE_NULL is represented by no row-image bytes.
		return nil, nil, offset, nil
	}
	if partial {
		if typeCode != 245 || len(metadata) == 0 {
			return nil, nil, 0, fmt.Errorf("partial value is not JSON")
		}
		width := int(metadata[0])
		if len(body)-offset < width {
			return nil, nil, 0, fmt.Errorf("truncated partial JSON length")
		}
		length := nativeLittleEndianNumber(body[offset : offset+width])
		offset += width
		if length > uint64(len(body)-offset) {
			return nil, nil, 0, fmt.Errorf("truncated partial JSON payload")
		}
		updates, err := decodeNativeJSONDiff(body[offset : offset+int(length)])
		return nil, updates, offset + int(length), err
	}
	width := nativeFixedValueWidth(typeCode, metadata)
	if width > 0 {
		if len(body)-offset < width {
			return nil, nil, 0, fmt.Errorf("truncated fixed-width value")
		}
		value := decodeNativeFixedValue(typeCode, metadata, unsigned, body[offset:offset+width])
		return value, nil, offset + width, nil
	}
	if typeCode == 20 || typeCode == 242 || typeCode == 245 || typeCode == 255 || (typeCode >= 249 && typeCode <= 254) || typeCode == 12 || typeCode == 15 || typeCode == 253 {
		packWidth := 1
		if typeCode == 20 || typeCode == 242 {
			// Replication-only typed arrays and VECTOR values are opaque to
			// the SQL layer here. Their row image is nevertheless length
			// delimited by the TABLE_MAP metadata, so retain the exact bytes
			// instead of rejecting the whole transaction or coercing them to
			// text.
			if len(metadata) > 0 {
				packWidth = int(metadata[0])
			}
			if packWidth < 1 || packWidth > 4 {
				return nil, nil, 0, fmt.Errorf("invalid opaque native value length width %d", packWidth)
			}
		} else if typeCode == 245 || typeCode == 255 {
			packWidth = int(metadata[0])
		} else if typeCode >= 249 && typeCode <= 252 {
			packWidth = int(metadata[0])
		} else if len(metadata) >= 2 {
			fieldLength := int(binary.LittleEndian.Uint16(metadata))
			if typeCode == 254 {
				fieldLength = int(metadata[1]) | ((int(metadata[0]^254) & 0x30) << 4)
			}
			if fieldLength > 255 {
				packWidth = 2
			}
		}
		if len(body)-offset < packWidth {
			return nil, nil, 0, fmt.Errorf("truncated length-encoded value")
		}
		length := nativeLittleEndianNumber(body[offset : offset+packWidth])
		offset += packWidth
		if length > uint64(len(body)-offset) {
			return nil, nil, 0, fmt.Errorf("truncated length-encoded value payload: type=%d offset=%d width=%d length=%d body=%d metadata=%x", typeCode, offset, packWidth, length, len(body), metadata)
		}
		data := append([]byte(nil), body[offset:offset+int(length)]...)
		if typeCode == 245 {
			value, err := decodeNativeJSONBinary(data)
			return value, nil, offset + int(length), err
		}
		if typeCode == 20 || typeCode == 242 || typeCode == 253 || (typeCode >= 249 && typeCode <= 252) || typeCode == 255 {
			return data, nil, offset + int(length), nil
		}
		return string(data), nil, offset + int(length), nil
	}
	return nil, nil, 0, fmt.Errorf("unsupported native column type %d", typeCode)
}

func nativeFixedValueWidth(typeCode byte, metadata []byte) int {
	switch typeCode {
	case 1, 13, 244:
		return 1
	case 7:
		return 4
	case 2:
		return 2
	case 3, 8:
		return map[byte]int{3: 4, 8: 8}[typeCode]
	case 12:
		// Legacy MYSQL_TYPE_DATETIME stores YYYYMMDDHHMMSS as an
		// eight-byte little-endian integer. It predates DATETIME2 and has
		// no fractional-seconds metadata.
		return 8
	case 4:
		return 4
	case 5:
		return 8
	case 9, 10, 11, 14:
		return 3
	case 16:
		if len(metadata) < 2 {
			return 0
		}
		width := int(metadata[1])
		if metadata[0] > 0 {
			width++
		}
		return width
	case 254:
		if len(metadata) >= 2 {
			// MYSQL_TYPE_STRING carries the real type and fixed field
			// length in its two-byte metadata. ENUM/SET use the same
			// type code but keep their existing packed-width handling.
			fieldLength := int(metadata[1])
			if metadata[0] != 247 && metadata[0] != 248 {
				fieldLength |= (int(metadata[0]^254) & 0x30) << 4
			}
			if fieldLength > 0 {
				return fieldLength
			}
		}
		return 0
	case 0, 246:
		if len(metadata) < 2 {
			return 0
		}
		precision, scale := int(metadata[0]), int(metadata[1])
		if precision <= 0 || scale < 0 || scale > precision {
			return 0
		}
		return nativeDecimalBinarySize(precision-scale, scale)
	case 17:
		if len(metadata) == 0 {
			return 3
		}
		return 3 + nativeTemporalFractionWidth(int(metadata[0]))
	case 18:
		if len(metadata) == 0 {
			return 5
		}
		return 5 + nativeTemporalFractionWidth(int(metadata[0]))
	case 19:
		if len(metadata) == 0 {
			return 4
		}
		return 4 + nativeTemporalFractionWidth(int(metadata[0]))
	default:
		return 0
	}
}

func decodeNativeFixedValue(typeCode byte, metadata []byte, unsigned bool, raw []byte) interface{} {
	switch typeCode {
	case 6:
		return "NULL"
	case 244:
		return raw[0] != 0
	case 1:
		if unsigned {
			return uint64(raw[0])
		}
		return int64(int8(raw[0]))
	case 2:
		if unsigned {
			return uint64(binary.LittleEndian.Uint16(raw))
		}
		return int64(int16(binary.LittleEndian.Uint16(raw)))
	case 3:
		if unsigned {
			return uint64(binary.LittleEndian.Uint32(raw))
		}
		return int64(int32(binary.LittleEndian.Uint32(raw)))
	case 4:
		return float64(math.Float32frombits(binary.LittleEndian.Uint32(raw)))
	case 5:
		return math.Float64frombits(binary.LittleEndian.Uint64(raw))
	case 8:
		if unsigned {
			return binary.LittleEndian.Uint64(raw)
		}
		return int64(binary.LittleEndian.Uint64(raw))
	case 9:
		number := int32(raw[0]) | int32(raw[1])<<8 | int32(raw[2])<<16
		if unsigned {
			return uint64(uint32(number))
		}
		if number&0x800000 != 0 {
			number |= ^int32(0xffffff)
		}
		return int64(number)
	case 13:
		if raw[0] == 0 {
			return int64(0)
		}
		return int64(raw[0]) + 1900
	case 10, 14:
		packed := uint32(raw[0]) | uint32(raw[1])<<8 | uint32(raw[2])<<16
		return fmt.Sprintf("%04d-%02d-%02d", packed>>9, (packed>>5)&15, packed&31)
	case 12:
		packed := binary.LittleEndian.Uint64(raw)
		if packed == 0 {
			return "0000-00-00 00:00:00"
		}
		second := packed % 100
		packed /= 100
		minute := packed % 100
		packed /= 100
		hour := packed % 100
		packed /= 100
		day := packed % 100
		packed /= 100
		month := packed % 100
		year := packed / 100
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)
	case 16:
		return nativeLittleEndianNumber(raw)
	case 254:
		if len(metadata) >= 1 && metadata[0] != 247 && metadata[0] != 248 {
			// CHAR values are fixed-width in ROWS_EVENT. The SQL-facing
			// value keeps the historical decoder behavior by removing the
			// padding that the wire image necessarily carries.
			return strings.TrimRight(string(raw), " ")
		}
		return nativeLittleEndianNumber(raw)
	case 7:
		return time.Unix(int64(binary.LittleEndian.Uint32(raw)), 0).UTC().Format("2006-01-02 15:04:05")
	case 11:
		packed := int32(raw[0]) | int32(raw[1])<<8 | int32(raw[2])<<16
		if packed&0x800000 != 0 {
			packed |= ^int32(0xffffff)
		}
		negative := packed < 0
		if negative {
			packed = -packed
		}
		return fmt.Sprintf("%s%02d:%02d:%02d", map[bool]string{true: "-", false: ""}[negative], packed/10000, (packed/100)%100, packed%100)
	case 17:
		return decodeNativeTime2(metadata, raw)
	case 18:
		return decodeNativeDateTime2(metadata, raw)
	case 19:
		seconds := int64(binary.LittleEndian.Uint32(raw[:4]))
		micro := decodeNativeFraction(metadata, raw[4:])
		return formatNativeTimestamp(time.Unix(seconds, int64(micro)*1000).UTC(), micro, metadata)
	case 0, 246:
		return decodeNativeDecimal(metadata, raw)
	}
	return nil
}

func decodeNativeEnumSetOptional(raw []byte, listCount int) ([][]string, error) {
	if listCount < 0 || listCount > 65535 {
		return nil, fmt.Errorf("invalid ENUM/SET optional metadata")
	}
	result := make([][]string, 0, listCount)
	offset := 0
	for index := 0; index < listCount; index++ {
		count, next, err := readNativeLenenc(raw, offset)
		if err != nil || count > 65535 {
			return nil, fmt.Errorf("invalid ENUM/SET member count")
		}
		offset = next
		members := make([]string, 0, count)
		for memberIndex := uint64(0); memberIndex < count; memberIndex++ {
			length, next, err := readNativeLenenc(raw, offset)
			if err != nil || length > uint64(len(raw)-next) {
				return nil, fmt.Errorf("invalid ENUM/SET member metadata")
			}
			members = append(members, string(raw[next:next+int(length)]))
			offset = next + int(length)
		}
		result = append(result, members)
	}
	if offset != len(raw) {
		return nil, fmt.Errorf("unexpected ENUM/SET member metadata")
	}
	return result, nil
}

func decodeNativeEnumValue(value interface{}, members []string) interface{} {
	number, ok := nativeDecoderNumber(value)
	if !ok || number == 0 || number > uint64(len(members)) {
		return value
	}
	return members[number-1]
}

func decodeNativeSetValue(value interface{}, members []string) interface{} {
	number, ok := nativeDecoderNumber(value)
	if !ok {
		return value
	}
	selected := make([]string, 0, len(members))
	for index, member := range members {
		if number&(uint64(1)<<uint(index)) != 0 {
			selected = append(selected, member)
		}
	}
	return strings.Join(selected, ",")
}

func nativeDecoderNumber(value interface{}) (uint64, bool) {
	switch typed := value.(type) {
	case uint64:
		return typed, true
	case int64:
		return uint64(typed), typed >= 0
	default:
		return 0, false
	}
}

func decodeNativeDecimal(metadata, raw []byte) string {
	if len(metadata) < 2 {
		return ""
	}
	precision, scale := int(metadata[0]), int(metadata[1])
	integerDigits := precision - scale
	if precision <= 0 || scale < 0 || scale > precision || len(raw) != nativeDecimalBinarySize(integerDigits, scale) {
		return ""
	}
	data := append([]byte(nil), raw...)
	negative := data[0]&0x80 == 0
	if negative {
		for index := range data {
			data[index] = ^data[index]
		}
	}
	data[0] &= 0x7f
	offset := 0
	readDigits := func(digits int) string {
		width := nativeDecimalGroupBytes(digits)
		if width == 0 || offset+width > len(data) {
			return ""
		}
		value := uint32(0)
		for index := 0; index < width; index++ {
			value = value<<8 | uint32(data[offset+index])
		}
		offset += width
		if digits == 9 {
			return fmt.Sprintf("%09d", value)
		}
		return fmt.Sprintf("%0*d", digits, value)
	}
	integer := ""
	firstIntegerDigits := integerDigits % 9
	if firstIntegerDigits == 0 {
		firstIntegerDigits = 9
	}
	if integerDigits > 0 {
		integer = readDigits(firstIntegerDigits)
		for consumed := firstIntegerDigits; consumed < integerDigits; consumed += 9 {
			integer += readDigits(minNativeInt(9, integerDigits-consumed))
		}
	} else {
		integer = "0"
	}
	integer = strings.TrimLeft(integer, "0")
	if integer == "" {
		integer = "0"
	}
	fraction := ""
	for consumed := 0; consumed < scale; consumed += 9 {
		fraction += readDigits(minNativeInt(9, scale-consumed))
	}
	result := integer
	if scale > 0 {
		result += "." + fraction
	}
	if negative && result != "0" {
		result = "-" + result
	}
	return result
}

func minNativeInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func nativeTemporalFractionWidth(fsp int) int {
	switch {
	case fsp <= 0:
		return 0
	case fsp <= 2:
		return 1
	case fsp <= 4:
		return 2
	default:
		return 3
	}
}

func decodeNativeFraction(metadata, raw []byte) int {
	if len(metadata) == 0 || metadata[0] == 0 {
		return 0
	}
	fsp := int(metadata[0])
	width := nativeTemporalFractionWidth(fsp)
	if len(raw) < width {
		return 0
	}
	value := int(nativeLittleEndianNumber(raw[:width]))
	if fsp <= 2 {
		return value * 10000
	}
	if fsp <= 4 {
		return value * 100
	}
	return value
}

func formatNativeTimestamp(value time.Time, micro int, metadata []byte) string {
	if len(metadata) == 0 || metadata[0] == 0 {
		return value.Format("2006-01-02 15:04:05")
	}
	return value.Format("2006-01-02 15:04:05") + "." + fmt.Sprintf("%06d", micro)[:int(metadata[0])]
}

func decodeNativeTime2(metadata, raw []byte) string {
	fsp := 0
	if len(metadata) > 0 {
		fsp = int(metadata[0])
	}
	negative := false
	var hms int
	var micro int
	if fsp > 4 {
		packed := int64(nativeLittleEndianNumber(raw[:6])) - 0x800000000000
		negative = packed < 0
		if negative {
			packed = -packed
		}
		hms = int((packed >> 24) & 0xffffff)
		micro = int(packed & 0xffffff)
	} else {
		hms = int(nativeLittleEndianNumber(raw[:3])) - 0x800000
		fractionOffset := 3
		if fsp > 0 {
			micro = decodeNativeFraction(metadata, raw[fractionOffset:])
		}
		negative = hms < 0
		if negative {
			hms = -hms
		}
	}
	value := fmt.Sprintf("%02d:%02d:%02d", (hms>>12)&0x3ff, (hms>>6)&0x3f, hms&0x3f)
	if fsp > 0 {
		value += "." + fmt.Sprintf("%06d", micro)[:fsp]
	}
	if negative {
		value = "-" + value
	}
	return value
}

func decodeNativeDateTime2(metadata, raw []byte) string {
	packed := nativeLittleEndianNumber(raw[:5]) - 0x8000000000
	ymd := packed >> 17
	hms := packed & 0x1ffff
	yearMonth := ymd >> 5
	year := yearMonth / 13
	month := yearMonth % 13
	day := ymd & 31
	hour := (hms >> 12) & 0x3ff
	minute := (hms >> 6) & 0x3f
	second := hms & 0x3f
	micro := decodeNativeFraction(metadata, raw[5:])
	value := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)
	if len(metadata) > 0 && metadata[0] > 0 {
		value += "." + fmt.Sprintf("%06d", micro)[:int(metadata[0])]
	}
	return value
}

func decodeNativeJSONDiff(raw []byte) ([]JSONPartialUpdate, error) {
	if len(raw) < 4 {
		return nil, fmt.Errorf("truncated JSON diff")
	}
	length := int(binary.LittleEndian.Uint32(raw[:4]))
	if length != len(raw)-4 {
		return nil, fmt.Errorf("invalid JSON diff length")
	}
	offset := 4
	updates := make([]JSONPartialUpdate, 0)
	for offset < len(raw) {
		op := raw[offset]
		offset++
		pathLength, next, err := readNativePackedUint(raw, offset)
		if err != nil || pathLength > uint64(len(raw)-next) {
			return nil, fmt.Errorf("invalid JSON diff path")
		}
		offset = next
		path := string(raw[offset : offset+int(pathLength)])
		offset += int(pathLength)
		valueLength, next, err := readNativePackedUint(raw, offset)
		if err != nil || valueLength > uint64(len(raw)-next) {
			return nil, fmt.Errorf("invalid JSON diff value")
		}
		offset = next
		update := JSONPartialUpdate{Operation: op, Path: path}
		if op != JSONPartialOperationRemove {
			value, err := decodeNativeJSONBinary(raw[offset : offset+int(valueLength)])
			if err != nil {
				return nil, err
			}
			update.Value = value
		}
		offset += int(valueLength)
		updates = append(updates, update)
	}
	return updates, nil
}

func decodeNativeJSONBinary(raw []byte) (interface{}, error) {
	if len(raw) < 1 {
		return nil, fmt.Errorf("empty binary JSON value")
	}
	typeCode := raw[0]
	return decodeNativeJSONTyped(typeCode, raw[1:])
}

func decodeNativeJSONTyped(typeCode byte, payload []byte) (interface{}, error) {
	switch typeCode {
	case nativeJSONTypeLiteral:
		if len(payload) < 1 {
			return nil, fmt.Errorf("truncated JSON literal")
		}
		switch payload[0] {
		case 0:
			return nil, nil
		case 1:
			return true, nil
		case 2:
			return false, nil
		default:
			return nil, fmt.Errorf("invalid JSON literal")
		}
	case nativeJSONTypeInt16:
		return int64(int16(binary.LittleEndian.Uint16(payload))), nil
	case nativeJSONTypeUint16:
		return uint64(binary.LittleEndian.Uint16(payload)), nil
	case nativeJSONTypeInt32:
		return int64(int32(binary.LittleEndian.Uint32(payload))), nil
	case nativeJSONTypeUint32:
		return uint64(binary.LittleEndian.Uint32(payload)), nil
	case nativeJSONTypeInt64:
		return int64(binary.LittleEndian.Uint64(payload)), nil
	case nativeJSONTypeUint64:
		return binary.LittleEndian.Uint64(payload), nil
	case nativeJSONTypeDouble:
		return math.Float64frombits(binary.LittleEndian.Uint64(payload)), nil
	case nativeJSONTypeString:
		length, offset, err := readNativeJSONLength(payload, 0)
		if err != nil || length > uint64(len(payload)-offset) {
			return nil, fmt.Errorf("invalid JSON string")
		}
		return string(payload[offset : offset+int(length)]), nil
	case nativeJSONTypeSmallObject:
		return decodeNativeJSONContainer(payload, true, false)
	case nativeJSONTypeLargeObject:
		return decodeNativeJSONContainer(payload, true, true)
	case nativeJSONTypeSmallArray:
		return decodeNativeJSONContainer(payload, false, false)
	case nativeJSONTypeLargeArray:
		return decodeNativeJSONContainer(payload, false, true)
	default:
		return nil, fmt.Errorf("unsupported binary JSON type %d", typeCode)
	}
}

func decodeNativeJSONContainer(payload []byte, object, large bool) (interface{}, error) {
	countWidth, sizeWidth := 2, 2
	if large {
		countWidth, sizeWidth = 4, 4
	}
	if len(payload) < countWidth+sizeWidth {
		return nil, fmt.Errorf("truncated binary JSON container")
	}
	var count, size uint64
	if large {
		count = uint64(binary.LittleEndian.Uint32(payload))
		size = uint64(binary.LittleEndian.Uint32(payload[4:]))
	} else {
		count = uint64(binary.LittleEndian.Uint16(payload))
		size = uint64(binary.LittleEndian.Uint16(payload[2:]))
	}
	if size > uint64(len(payload)) || count > 65535 {
		return nil, fmt.Errorf("invalid binary JSON container size")
	}
	keyEntryWidth, valueEntryWidth := 0, 3
	if object {
		keyEntryWidth = 4
	}
	if large {
		if object {
			keyEntryWidth = 6
		}
		valueEntryWidth = 5
	}
	headerSize := countWidth + sizeWidth + int(count)*keyEntryWidth + int(count)*valueEntryWidth
	if headerSize > len(payload) {
		return nil, fmt.Errorf("truncated binary JSON container entries")
	}
	keyNames := make([]string, count)
	if object {
		for index := 0; index < int(count); index++ {
			entryOffset := countWidth + sizeWidth + index*keyEntryWidth
			var keyOffset uint64
			if large {
				keyOffset = uint64(binary.LittleEndian.Uint32(payload[entryOffset:]))
			} else {
				keyOffset = uint64(binary.LittleEndian.Uint16(payload[entryOffset:]))
			}
			keyLength := int(binary.LittleEndian.Uint16(payload[entryOffset+keyEntryWidth-2:]))
			if keyOffset > uint64(size) || keyLength > int(size-keyOffset) || int(keyOffset)+keyLength > len(payload) {
				return nil, fmt.Errorf("invalid binary JSON object key")
			}
			keyNames[index] = string(payload[keyOffset : keyOffset+uint64(keyLength)])
		}
	}
	valueEntryOffset := countWidth + sizeWidth + int(count)*keyEntryWidth
	values := make([]interface{}, count)
	for index := 0; index < int(count); index++ {
		entryOffset := valueEntryOffset + index*valueEntryWidth
		typeCode := payload[entryOffset]
		entry := payload[entryOffset+1 : entryOffset+valueEntryWidth]
		inline := (typeCode == nativeJSONTypeLiteral || typeCode == nativeJSONTypeInt16 || typeCode == nativeJSONTypeUint16)
		var valuePayload []byte
		if inline {
			valuePayload = entry
		} else {
			var valueOffset uint64
			if large {
				valueOffset = uint64(binary.LittleEndian.Uint32(entry))
			} else {
				valueOffset = uint64(binary.LittleEndian.Uint16(entry))
			}
			if valueOffset >= uint64(size) || valueOffset >= uint64(len(payload)) {
				return nil, fmt.Errorf("invalid binary JSON value offset")
			}
			valuePayload = payload[valueOffset:size]
		}
		value, err := decodeNativeJSONTyped(typeCode, valuePayload)
		if err != nil {
			return nil, err
		}
		values[index] = value
	}
	if object {
		result := make(map[string]interface{}, count)
		for index, key := range keyNames {
			result[key] = values[index]
		}
		return result, nil
	}
	return values, nil
}

func readNativeJSONLength(raw []byte, offset int) (uint64, int, error) {
	var result uint64
	for shift := uint(0); offset < len(raw) && shift <= 63; shift += 7 {
		part := raw[offset]
		offset++
		result |= uint64(part&0x7f) << shift
		if part&0x80 == 0 {
			return result, offset, nil
		}
	}
	return 0, offset, fmt.Errorf("invalid JSON length")
}

func readNativeLengthPrefixedString(raw []byte, offset int) (string, int, error) {
	if offset >= len(raw) {
		return "", 0, fmt.Errorf("truncated native string")
	}
	length := int(raw[offset])
	offset++
	if length > len(raw)-offset {
		return "", 0, fmt.Errorf("truncated native string")
	}
	value := string(raw[offset : offset+length])
	offset += length
	if offset >= len(raw) || raw[offset] != 0 {
		return "", 0, fmt.Errorf("unterminated native string")
	}
	return value, offset + 1, nil
}

func readNativeLenenc(raw []byte, offset int) (uint64, int, error) {
	if offset >= len(raw) {
		return 0, 0, fmt.Errorf("truncated length-encoded integer")
	}
	first := raw[offset]
	offset++
	switch first {
	case 0xfc:
		if len(raw)-offset < 2 {
			return 0, 0, fmt.Errorf("truncated length-encoded integer")
		}
		return uint64(binary.LittleEndian.Uint16(raw[offset : offset+2])), offset + 2, nil
	case 0xfd:
		if len(raw)-offset < 3 {
			return 0, 0, fmt.Errorf("truncated length-encoded integer")
		}
		return uint64(raw[offset]) | uint64(raw[offset+1])<<8 | uint64(raw[offset+2])<<16, offset + 3, nil
	case 0xfe:
		if len(raw)-offset < 8 {
			return 0, 0, fmt.Errorf("truncated length-encoded integer")
		}
		return binary.LittleEndian.Uint64(raw[offset : offset+8]), offset + 8, nil
	case 0xfb:
		return 0, 0, fmt.Errorf("NULL length-encoded integer")
	default:
		return uint64(first), offset, nil
	}
}

func readNativePackedUint(raw []byte, offset int) (uint64, int, error) {
	return readNativeLenenc(raw, offset)
}

func readNativeBitmap(raw []byte, offset, columns int) ([]byte, int, error) {
	width := (columns + 7) / 8
	if len(raw)-offset < width {
		return nil, 0, fmt.Errorf("truncated ROWS_EVENT columns bitmap")
	}
	return raw[offset : offset+width], offset + width, nil
}

func readNativeNullBitmap(raw []byte, offset int, table nativeDecoderTable, bitmap []byte) ([]byte, int, error) {
	present := 0
	for index := range table.types {
		if bitmap[index/8]&(1<<uint(index%8)) != 0 {
			present++
		}
	}
	width := (present + 7) / 8
	if len(raw)-offset < width {
		return nil, 0, fmt.Errorf("truncated ROWS_EVENT null bitmap")
	}
	return raw[offset : offset+width], offset + width, nil
}

func nativeUint48(raw []byte) uint64 {
	return uint64(raw[0]) | uint64(raw[1])<<8 | uint64(raw[2])<<16 | uint64(raw[3])<<24 | uint64(raw[4])<<32 | uint64(raw[5])<<40
}

func nativeLittleEndianNumber(raw []byte) uint64 {
	var result uint64
	for index, value := range raw {
		result |= uint64(value) << uint(index*8)
	}
	return result
}

func nativeDecoderNumericType(typeCode byte) bool {
	switch typeCode {
	case 0, 1, 2, 3, 4, 5, 8, 9, 13, 16, 246:
		return true
	default:
		return false
	}
}

func nativeDecoderNumericIndex(types []byte, column int) int {
	index := 0
	for current := 0; current < column; current++ {
		if nativeDecoderNumericType(types[current]) {
			index++
		}
	}
	return index
}

func nativeDecoderColumnType(table nativeDecoderTable, index int) string {
	if index < 0 || index >= len(table.types) {
		return ""
	}
	if index < len(table.columnTypes) && strings.TrimSpace(table.columnTypes[index]) != "" {
		return strings.TrimSpace(table.columnTypes[index])
	}
	typeCode := table.types[index]
	suffix := ""
	if table.unsigned[index] {
		suffix = " UNSIGNED"
	}
	switch typeCode {
	case 6:
		return "NULL"
	case 244:
		return "BOOL"
	case 1:
		return "TINYINT" + suffix
	case 2:
		return "SMALLINT" + suffix
	case 3:
		return "INT" + suffix
	case 4:
		return "FLOAT"
	case 5:
		return "DOUBLE"
	case 7:
		return "TIMESTAMP"
	case 8:
		return "BIGINT" + suffix
	case 9:
		return "MEDIUMINT" + suffix
	case 10, 14:
		return "DATE"
	case 11:
		return "TIME"
	case 12:
		return "DATETIME"
	case 13:
		return "YEAR"
	case 15:
		return "VARCHAR"
	case 16:
		return "BIT"
	case 17:
		return fmt.Sprintf("TIME2(%d)", nativeDecoderFSP(table.metadata[index]))
	case 18:
		return fmt.Sprintf("DATETIME2(%d)", nativeDecoderFSP(table.metadata[index]))
	case 19:
		return fmt.Sprintf("TIMESTAMP2(%d)", nativeDecoderFSP(table.metadata[index]))
	case 245:
		return "JSON"
	case 20:
		return "TYPED_ARRAY"
	case 242:
		return "VECTOR"
	case 0, 246:
		if len(table.metadata[index]) >= 2 {
			return fmt.Sprintf("DECIMAL(%d,%d)", table.metadata[index][0], table.metadata[index][1])
		}
		return "DECIMAL"
	case 247, 254:
		if members := table.enumMembers[index]; len(members) > 0 {
			return "ENUM"
		}
		if members := table.setMembers[index]; len(members) > 0 {
			return "SET"
		}
	case 248:
		if members := table.setMembers[index]; len(members) > 0 {
			return "SET"
		}
	case 249:
		return "TINYBLOB"
	case 250:
		return "MEDIUMBLOB"
	case 251:
		return "LONGBLOB"
	case 252:
		return "BLOB"
	case 253:
		return "VARBINARY"
	case 255:
		return "GEOMETRY"
	}
	return "STRING"
}

func nativeDecoderBinaryStringColumn(table nativeDecoderTable, index int) bool {
	if index < 0 || index >= len(table.columnTypes) {
		return false
	}
	typeName := strings.ToUpper(strings.TrimSpace(table.columnTypes[index]))
	if open := strings.IndexByte(typeName, '('); open >= 0 {
		typeName = typeName[:open]
	}
	return strings.TrimSpace(typeName) == "BINARY"
}

func nativeDecoderFSP(metadata []byte) int {
	if len(metadata) == 0 {
		return 0
	}
	return int(metadata[0])
}
