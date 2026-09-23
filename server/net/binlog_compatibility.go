package net

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func binlogDumpStartPosition(body []byte) uint64 {
	if len(body) > 0 && body[0] == 0x1e { // COM_BINLOG_DUMP_GTID
		// flags(2) + server-id(4) + filename-length(4) + filename +
		// position(8) + data-size(4) + encoded GTID set.
		const fixed = 1 + 2 + 4 + 4
		if len(body) >= fixed {
			filenameLength := int(binary.LittleEndian.Uint32(body[fixed-4 : fixed]))
			position := fixed + filenameLength
			if filenameLength >= 0 && len(body) >= position+8 {
				return binary.LittleEndian.Uint64(body[position : position+8])
			}
		}
	}
	if len(body) < 5 {
		return 4
	}
	return uint64(binary.LittleEndian.Uint32(body[1:5]))
}

func binlogDumpFileName(body []byte) string {
	const defaultName = "binlog.000001"
	if len(body) == 0 {
		return defaultName
	}
	if body[0] == common.COM_BINLOG_DUMP_GTID {
		const fixed = 1 + 2 + 4 + 4
		if len(body) < fixed {
			return defaultName
		}
		filenameLength := int(binary.LittleEndian.Uint32(body[fixed-4 : fixed]))
		start := fixed
		if filenameLength < 0 || len(body) < start+filenameLength {
			return defaultName
		}
		if name := strings.TrimSpace(string(body[start : start+filenameLength])); name != "" {
			return name
		}
		return defaultName
	}
	if body[0] == common.COM_BINLOG_DUMP && len(body) > 11 {
		if name := strings.TrimSpace(strings.TrimRight(string(body[11:]), "\x00")); name != "" {
			return name
		}
	}
	return defaultName
}

// binlogDumpGTIDIntervals decodes the COM_BINLOG_DUMP_GTID interval set
// without expanding ranges into one entry per transaction. MySQL encodes
// each interval as [start, end).
func binlogDumpGTIDIntervals(body []byte) (replication.GTIDIntervals, error) {
	set := replication.GTIDIntervals{}
	if len(body) == 0 || body[0] != 0x1e {
		return set, nil
	}
	const fixedPrefix = 1 + 2 + 4 + 4
	if len(body) < fixedPrefix {
		return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID payload is truncated")
	}
	filenameLength := int(binary.LittleEndian.Uint32(body[fixedPrefix-4 : fixedPrefix]))
	offset := fixedPrefix + filenameLength
	if filenameLength < 0 || len(body) < offset+8+4 {
		return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID filename or position is truncated")
	}
	offset += 8
	dataSize := int(binary.LittleEndian.Uint32(body[offset : offset+4]))
	offset += 4
	if dataSize == 0 {
		return set, nil
	}
	if dataSize < 4 || len(body) < offset+dataSize {
		return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID GTID set is truncated")
	}
	end := offset + dataSize
	sidCount := int(binary.LittleEndian.Uint32(body[offset : offset+4]))
	offset += 4
	for sidIndex := 0; sidIndex < sidCount; sidIndex++ {
		if offset+16+8 > end {
			return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID SID block is truncated")
		}
		uuid := formatGTIDSID(body[offset : offset+16])
		offset += 16
		intervalCount := binary.LittleEndian.Uint64(body[offset : offset+8])
		offset += 8
		for intervalIndex := uint64(0); intervalIndex < intervalCount; intervalIndex++ {
			if offset+16 > end {
				return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID interval is truncated")
			}
			start := binary.LittleEndian.Uint64(body[offset : offset+8])
			finish := binary.LittleEndian.Uint64(body[offset+8 : offset+16])
			offset += 16
			if start == 0 || finish <= start {
				return nil, fmt.Errorf("invalid COM_BINLOG_DUMP_GTID interval %d-%d", start, finish)
			}
			if err := set.AddHalfOpen(uuid, start, finish); err != nil {
				return nil, err
			}
		}
	}
	if offset != end {
		return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID GTID set has %d trailing bytes", end-offset)
	}
	return set, nil
}

// binlogDumpGTIDSet keeps the historical materialized helper for callers and
// tests that need individual sequence membership. The actual native dump
// path uses binlogDumpGTIDIntervals so a large wire interval stays compact.
func binlogDumpGTIDSet(body []byte) (replication.GTIDSet, error) {
	intervals, err := binlogDumpGTIDIntervals(body)
	if err != nil {
		return nil, err
	}
	set := intervals.ToGTIDSet()
	if set == nil && len(intervals) > 0 {
		return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID set is too large to materialize; use interval form")
	}
	return set, nil
}

func formatGTIDSID(raw []byte) string {
	if len(raw) != 16 {
		return ""
	}
	encoded := hex.EncodeToString(raw)
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}

func encodeBinlogEventPacket(event replication.BinlogEvent, sequence byte) ([]byte, error) {
	payloads, err := encodeNativeBinlogEventPayloads(event)
	if err != nil {
		return nil, err
	}
	if len(payloads) != 1 {
		return nil, fmt.Errorf("binlog event expands to %d native events", len(payloads))
	}
	payload := payloads[0]
	// The first byte is the OK/event marker used by a binlog stream packet;
	// the remainder is a native 19-byte-header binlog event.
	streamPayload := append([]byte{0x00}, payload...)
	packets := protocol.EncodePacketWithSplit(streamPayload, sequence)
	if len(packets) != 1 {
		return nil, fmt.Errorf("binlog event packet exceeds single-packet compatibility limit")
	}
	return packets[0], nil
}

func encodeNativeBinlogEventPayloads(event replication.BinlogEvent) ([][]byte, error) {
	if event.Type != replication.EventRow || len(event.Changes) == 0 {
		payload, err := encodeNativeBinlogEvent(event)
		if err != nil {
			return nil, err
		}
		return [][]byte{payload}, nil
	}
	return replication.EncodeNativeBinlogEventPayloads(event), nil
}

func splitNativeTableName(raw string) (string, string) {
	parts := strings.Split(strings.TrimSpace(raw), ".")
	if len(parts) >= 2 {
		return strings.Trim(parts[len(parts)-2], "` "), strings.Trim(parts[len(parts)-1], "` ")
	}
	return "", strings.Trim(raw, "` ")
}

func nativeTableID(table string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(strings.ToLower(strings.TrimSpace(table))))
	return hash.Sum64() & 0x0000FFFFFFFFFFFF
}

func nativeRowColumns(change replication.RowChange) []string {
	seen := map[string]struct{}{}
	for column := range change.Before {
		seen[column] = struct{}{}
	}
	for column := range change.After {
		seen[column] = struct{}{}
	}
	columns := make([]string, 0, len(seen))
	for column := range seen {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}

func nativeColumnType(value interface{}) byte {
	switch value.(type) {
	case bool, int8, uint8:
		return 1 // MYSQL_TYPE_TINY
	case int, int16, int32, uint, uint16, uint32:
		return 3 // MYSQL_TYPE_LONG
	case int64, uint64:
		return 8 // MYSQL_TYPE_LONGLONG
	case float32:
		return 4 // MYSQL_TYPE_FLOAT
	case float64:
		return 5 // MYSQL_TYPE_DOUBLE
	default:
		return 253 // MYSQL_TYPE_VAR_STRING
	}
}

func nativeValueForColumn(change replication.RowChange, column string) interface{} {
	if value, ok := change.After[column]; ok {
		return value
	}
	return change.Before[column]
}

func buildNativeTableMapEvent(event replication.BinlogEvent, tableID uint64, database, table string, columns []string) []byte {
	body := make([]byte, 0, 32+len(columns)*3)
	body = appendNativeUint48(body, tableID)
	body = append(body, 0, 0) // table-map flags
	body = append(body, byte(len(database)))
	body = append(body, database...)
	body = append(body, 0)
	body = append(body, byte(len(table)))
	body = append(body, table...)
	body = append(body, 0)
	body = appendNativeLenencInt(body, len(columns))
	metadata := make([]byte, 0, len(columns)*2)
	for _, column := range columns {
		change := eventChangeForTable(event, tableID)
		value := nativeValueForColumn(change, column)
		typeCode := nativeColumnType(value)
		body = append(body, typeCode)
		metadata = append(metadata, nativeColumnMetadata(typeCode, value)...)
	}
	body = appendNativeLenencInt(body, len(metadata))
	body = append(body, metadata...)
	body = append(body, make([]byte, (len(columns)+7)/8)...)
	return buildNativeEvent(19, body, event)
}

func nativeColumnMetadata(typeCode byte, value interface{}) []byte {
	if typeCode != 253 {
		return nil
	}
	length := len([]byte(fmt.Sprint(value)))
	if length > 0xFFFF {
		length = 0xFFFF
	}
	metadata := make([]byte, 2)
	binary.LittleEndian.PutUint16(metadata, uint16(length))
	return metadata
}

// eventChangeForTable is intentionally empty for table-map type inference;
// callers replace inferred types in buildNativeRowsEvent. Unknown values use
// VAR_STRING, which is the safest wire representation for a logical map.
func eventChangeForTable(event replication.BinlogEvent, tableID uint64) replication.RowChange {
	for _, change := range event.Changes {
		if nativeTableID(change.Table) == tableID {
			return change
		}
	}
	return replication.RowChange{}
}

func buildNativeRowsEvent(event replication.BinlogEvent, tableID uint64, columns []string, change replication.RowChange) []byte {
	rowType := byte(30) // WRITE_ROWS_EVENTv2
	action := strings.ToLower(strings.TrimSpace(change.Action))
	switch action {
	case "update":
		rowType = 31
	case "delete":
		rowType = 32
	}
	body := make([]byte, 0, 48)
	body = appendNativeUint48(body, tableID)
	body = append(body, 0, 0) // flags
	body = append(body, 2, 0) // extra-data length, no extra data
	body = appendNativeLenencInt(body, len(columns))
	if rowType == 31 {
		beforeBitmap, beforeImage := nativeRowImage(columns, change.Before)
		afterBitmap, afterImage := nativeRowImage(columns, change.After)
		body = append(body, beforeBitmap...)
		body = append(body, afterBitmap...)
		body = append(body, beforeImage...)
		body = append(body, afterImage...)
	} else {
		image := change.After
		if rowType == 32 {
			image = change.Before
		}
		bitmap, encoded := nativeRowImage(columns, image)
		body = append(body, bitmap...)
		body = append(body, encoded...)
	}
	return buildNativeEvent(rowType, body, event)
}

func nativeRowImage(columns []string, values map[string]interface{}) ([]byte, []byte) {
	columnsBitmap := make([]byte, (len(columns)+7)/8)
	presentCount := 0
	for index, column := range columns {
		if _, ok := values[column]; !ok {
			continue
		}
		columnsBitmap[index/8] |= 1 << uint(index%8)
		presentCount++
	}
	nullBitmap := make([]byte, (presentCount+7)/8)
	encoded := make([]byte, 0)
	presentIndex := 0
	for _, column := range columns {
		value, ok := values[column]
		if !ok {
			continue
		}
		if value == nil {
			nullBitmap[presentIndex/8] |= 1 << uint(presentIndex%8)
		} else {
			encoded = append(encoded, nativeValueBytes(value)...)
		}
		presentIndex++
	}
	return append(columnsBitmap, nullBitmap...), encoded
}

func nativeValueBytes(value interface{}) []byte {
	var encoded []byte
	switch typed := value.(type) {
	case bool:
		if typed {
			return []byte{1}
		}
		return []byte{0}
	case int8:
		return []byte{byte(typed)}
	case uint8:
		return []byte{typed}
	case int, int16, int32, uint, uint16, uint32:
		number, _ := strconv.ParseInt(fmt.Sprint(typed), 10, 64)
		encoded = make([]byte, 4)
		binary.LittleEndian.PutUint32(encoded, uint32(number))
		return encoded
	case int64:
		encoded = make([]byte, 8)
		binary.LittleEndian.PutUint64(encoded, uint64(typed))
		return encoded
	case uint64:
		encoded = make([]byte, 8)
		binary.LittleEndian.PutUint64(encoded, typed)
		return encoded
	case float32:
		encoded = make([]byte, 4)
		binary.LittleEndian.PutUint32(encoded, math.Float32bits(typed))
		return encoded
	case float64:
		encoded = make([]byte, 8)
		binary.LittleEndian.PutUint64(encoded, math.Float64bits(typed))
		return encoded
	default:
		text := []byte(fmt.Sprint(value))
		encoded := appendNativeLenencInt(nil, len(text))
		return append(encoded, text...)
	}
}

func appendNativeUint48(dst []byte, value uint64) []byte {
	for index := 0; index < 6; index++ {
		dst = append(dst, byte(value>>uint(index*8)))
	}
	return dst
}

func appendNativeLenencInt(dst []byte, value int) []byte {
	if value < 251 {
		return append(dst, byte(value))
	}
	if value <= 0xFFFF {
		dst = append(dst, 0xfc, 0, 0)
		binary.LittleEndian.PutUint16(dst[len(dst)-2:], uint16(value))
		return dst
	}
	dst = append(dst, 0xfd, 0, 0, 0)
	for index := 0; index < 3; index++ {
		dst[len(dst)-3+index] = byte(value >> uint(index*8))
	}
	return dst
}

// encodeNativeBinlogEvent exposes non-row logical events through the native
// event framing understood by mysqlbinlog and replication libraries. Row
// changes are expanded by encodeNativeBinlogEventPayloads into TABLE_MAP and
// row-image events because the logical storage source stores row values rather
// than a pre-built native binlog payload.
func encodeNativeBinlogEvent(event replication.BinlogEvent) ([]byte, error) {
	if event.Timestamp.IsZero() {
		event.Timestamp = timeNowForBinlog()
	}
	const (
		queryEventType  = 2
		rotateEventType = 4
		gtidEventType   = 33
		xidEventType    = 16
	)
	if event.Type == replication.EventBegin {
		return buildNativeEvent(gtidEventType, nativeGTIDBody(event), event), nil
	}
	if event.Type == replication.EventCommit {
		body := make([]byte, 8)
		binary.LittleEndian.PutUint64(body, event.GTID.Seq)
		return buildNativeEvent(xidEventType, body, event), nil
	}
	if event.Type == replication.EventRotate {
		body := make([]byte, 8, 8+len("binlog.000001"))
		binary.LittleEndian.PutUint64(body, 4)
		body = append(body, []byte("binlog.000001")...)
		return buildNativeEvent(rotateEventType, body, event), nil
	}

	database := ""
	queries := make([]string, 0, len(event.Statements))
	for _, statement := range event.Statements {
		if database == "" {
			database = statement.Database
		}
		if strings.TrimSpace(statement.SQL) != "" {
			queries = append(queries, strings.TrimSpace(statement.SQL))
		}
	}
	var query string
	query = strings.Join(queries, "; ")
	if query == "" {
		changes, err := json.Marshal(event.Changes)
		if err != nil {
			return nil, err
		}
		query = "/* XMYSQL ROW " + base64.RawStdEncoding.EncodeToString(changes) + " */"
	}

	databaseBytes := []byte(database)
	queryBytes := []byte(query)
	body := make([]byte, 13, 13+len(databaseBytes)+1+len(queryBytes))
	// QUERY_EVENT post-header: thread-id, execution time, database length,
	// error code, status-vars length. This implementation has no session
	// status variables, so the length is zero.
	binary.LittleEndian.PutUint32(body[0:4], 0)
	binary.LittleEndian.PutUint32(body[4:8], 0)
	body[8] = byte(len(databaseBytes))
	binary.LittleEndian.PutUint16(body[9:11], 0)
	binary.LittleEndian.PutUint16(body[11:13], 0)
	body = append(body, databaseBytes...)
	body = append(body, 0)
	body = append(body, queryBytes...)

	return buildNativeEvent(queryEventType, body, event), nil
}

func buildNativeEvent(eventType byte, body []byte, event replication.BinlogEvent) []byte {
	const eventHeaderLength = 19
	const checksumLength = 4
	raw := make([]byte, eventHeaderLength+len(body)+checksumLength)
	binary.LittleEndian.PutUint32(raw[0:4], uint32(event.Timestamp.Unix()))
	raw[4] = eventType
	binary.LittleEndian.PutUint32(raw[5:9], event.ServerID)
	binary.LittleEndian.PutUint32(raw[9:13], uint32(len(raw)))
	binary.LittleEndian.PutUint32(raw[13:17], uint32(event.Position))
	binary.LittleEndian.PutUint16(raw[17:19], 0)
	copy(raw[19:], body)
	binary.LittleEndian.PutUint32(raw[len(raw)-checksumLength:], crc32.ChecksumIEEE(raw[:len(raw)-checksumLength]))
	return raw
}

func nativeGTIDBody(event replication.BinlogEvent) []byte {
	body := make([]byte, 56)
	// GTID_EVENT: flags, SID, GNO, logical timestamp metadata and two
	// microsecond timestamps. The values are deterministic for this stream.
	body[0] = 0
	copy(body[1:17], nativeGTIDSID(event.GTID.UUID))
	binary.LittleEndian.PutUint64(body[17:25], event.GTID.Seq)
	body[25] = 2 // logical timestamp type
	lastCommitted := uint64(0)
	if event.GTID.Seq > 0 {
		lastCommitted = event.GTID.Seq - 1
	}
	binary.LittleEndian.PutUint64(body[26:34], lastCommitted)
	binary.LittleEndian.PutUint64(body[34:42], event.GTID.Seq)
	micros := uint64(event.Timestamp.UnixNano() / int64(time.Microsecond))
	writeUint56LE(body[42:49], micros)
	writeUint56LE(body[49:56], micros)
	return body
}

func nativeGTIDSID(raw string) []byte {
	compact := strings.ReplaceAll(strings.TrimSpace(raw), "-", "")
	if decoded, err := hex.DecodeString(compact); err == nil && len(decoded) == 16 {
		return decoded
	}
	hash := md5.Sum([]byte(raw))
	return hash[:]
}

func writeUint56LE(dst []byte, value uint64) {
	for i := 0; i < 7 && i < len(dst); i++ {
		dst[i] = byte(value >> (8 * i))
	}
}

var timeNowForBinlog = func() time.Time { return time.Now().UTC() }

const binlogDumpNonBlockFlag uint16 = 0x0001

func binlogDumpFlags(body []byte) uint16 {
	if len(body) == 0 {
		return 0
	}
	if body[0] == common.COM_BINLOG_DUMP_GTID {
		if len(body) < 3 {
			return 0
		}
		return binary.LittleEndian.Uint16(body[1:3])
	}
	if len(body) < 7 {
		return 0
	}
	return binary.LittleEndian.Uint16(body[5:7])
}

func dumpBinlogEvents(session Session, body []byte) error {
	source, ok := session.GetAttribute("replication_source").(*replication.Source)
	if !ok || source == nil {
		return session.WriteBytes(protocol.EncodeNotSupportedError("COM_BINLOG_DUMP requires a configured replication source"))
	}
	if err := validateBinlogDumpRequest(body); err != nil {
		return session.WriteBytes(protocol.EncodeErrorFromGoError(err))
	}
	startPosition := binlogDumpStartPosition(body)
	gtidIntervals, err := binlogDumpGTIDIntervals(body)
	if err != nil {
		return session.WriteBytes(protocol.EncodeErrorFromGoError(err))
	}
	gtidIntervals = normalizeGTIDIntervalsForSource(gtidIntervals, source.UUID)
	blocking := binlogDumpFlags(body)&binlogDumpNonBlockFlag == 0
	logName := binlogDumpFileName(body)
	nextPosition := startPosition
	sequence := byte(0)
	for {
		rawEvents, observedNextPosition, err := source.NativeDumpFileWithIntervals(logName, nextPosition, gtidIntervals)
		if err != nil {
			return session.WriteBytes(protocol.EncodeErrorFromGoError(err))
		}
		for _, event := range rawEvents {
			streamPayload := append([]byte{0x00}, event.Raw...)
			packets := protocol.EncodePacketWithSplit(streamPayload, sequence)
			for _, packet := range packets {
				if err := session.WriteBytes(packet); err != nil {
					return err
				}
				sequence++
			}
		}
		if observedNextPosition > nextPosition {
			nextPosition = observedNextPosition
		}
		if !blocking {
			if len(rawEvents) == 0 {
				return session.WriteBytes(protocol.EncodeOKPacketWithSeq(0, 0, protocol.SERVER_STATUS_AUTOCOMMIT, 0, 0))
			}
			return nil
		}
		if session.IsClosed() {
			return nil
		}
		// A blocking binlog dump follows a durable ROTATE event into the next
		// physical file. Without this transition a replica that starts before a
		// rotation would wait forever at the old file's end position and never
		// observe transactions appended after the rotation.
		if len(rawEvents) > 0 && rawEvents[len(rawEvents)-1].Type == 4 {
			if nextFile, ok := nextNativeBinlogFile(source, logName); ok {
				logName = nextFile
				nextPosition = 4
				continue
			}
		}
		if len(rawEvents) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func nextNativeBinlogFile(source *replication.Source, current string) (string, bool) {
	if source == nil {
		return "", false
	}
	files := source.NativeFiles()
	for index, file := range files {
		if file.Name == current && index+1 < len(files) {
			return files[index+1].Name, true
		}
	}
	return "", false
}

func encodeNativeFormatDescriptionEvent(source *replication.Source) ([]byte, error) {
	const (
		formatDescriptionEventType = 15
		eventHeaderLength          = 19
		serverVersionLength        = 50
		eventTypeCount             = 40
	)
	body := make([]byte, 2+serverVersionLength+4+1+eventTypeCount)
	binary.LittleEndian.PutUint16(body[0:2], 4)
	serverVersion := []byte("8.4.0-xmysql")
	copy(body[2:2+serverVersionLength], serverVersion)
	body[2+serverVersionLength+4] = eventHeaderLength
	postHeaderLengths := body[2+serverVersionLength+5:]
	postHeaderLengths[2] = 13  // QUERY_EVENT
	postHeaderLengths[19] = 8  // TABLE_MAP_EVENT
	postHeaderLengths[30] = 6  // WRITE_ROWS_EVENTv2
	postHeaderLengths[31] = 6  // UPDATE_ROWS_EVENTv2
	postHeaderLengths[32] = 6  // DELETE_ROWS_EVENTv2
	postHeaderLengths[33] = 42 // GTID_EVENT
	serverID := uint32(0)
	if source != nil {
		serverID = source.ServerID()
	}
	return buildNativeEvent(formatDescriptionEventType, body, replication.BinlogEvent{
		Timestamp: timeNowForBinlog(),
		ServerID:  serverID,
		Position:  4,
	}), nil
}

func validateBinlogDumpRequest(body []byte) error {
	if len(body) == 0 {
		return fmt.Errorf("binlog dump request is empty")
	}
	switch body[0] {
	case common.COM_BINLOG_DUMP:
		// command + position + flags + replica server-id
		if len(body) < 1+4+2+4 {
			return fmt.Errorf("COM_BINLOG_DUMP payload is truncated")
		}
	case common.COM_BINLOG_DUMP_GTID:
		// binlogDumpGTIDSet validates the variable filename and GTID block;
		// this early check keeps malformed requests from reaching source.Dump.
		if len(body) < 1+2+4+4+8+4 {
			return fmt.Errorf("COM_BINLOG_DUMP_GTID payload is truncated")
		}
		filenameLength := int(binary.LittleEndian.Uint32(body[7:11]))
		if filenameLength < 0 || len(body) < 11+filenameLength+8+4 {
			return fmt.Errorf("COM_BINLOG_DUMP_GTID filename or position is truncated")
		}
	default:
		return fmt.Errorf("unsupported binlog dump command 0x%02x", body[0])
	}
	return nil
}

// handleRegisterSlave accepts the legacy registration packet used by native
// replication clients before COM_BINLOG_DUMP. The password is parsed only to
// validate packet boundaries and is intentionally never retained in session
// state.
func (h *DecoupledMySQLMessageHandler) handleRegisterSlave(session Session, packet *MySQLPackage) error {
	info, err := parseRegisterSlavePacket(packet.Body)
	if err != nil {
		return session.WriteBytes(protocol.EncodeErrorFromGoError(err))
	}
	session.SetAttribute("replication_slave_registered", true)
	session.SetAttribute("replication_slave_server_id", info.ServerID)
	session.SetAttribute("replication_slave_report_host", info.ReportHost)
	session.SetAttribute("replication_slave_report_user", info.ReportUser)
	session.SetAttribute("replication_slave_report_port", info.ReportPort)
	if h.replicaRegistry != nil {
		h.replicaRegistry.Register(session.Stat(), replication.ReplicaRegistration{
			ServerID:   info.ServerID,
			ReportHost: info.ReportHost,
			ReportUser: info.ReportUser,
			ReportPort: info.ReportPort,
			MasterID:   info.MasterID,
		})
	}
	return session.WriteBytes(protocol.EncodeOKPacketWithSeq(0, 0, protocol.SERVER_STATUS_AUTOCOMMIT, 0, packet.Header.PacketId+1))
}

func (h *DecoupledMySQLMessageHandler) unregisterReplicaSession(session Session) {
	if h == nil || h.replicaRegistry == nil || session == nil {
		return
	}
	serverID, ok := session.GetAttribute("replication_slave_server_id").(uint32)
	if !ok {
		return
	}
	h.replicaRegistry.Unregister(session.Stat(), serverID)
}

type registerSlaveInfo struct {
	ServerID   uint32
	ReportHost string
	ReportUser string
	ReportPort uint16
	MasterID   uint32
}

func parseRegisterSlavePacket(body []byte) (registerSlaveInfo, error) {
	var info registerSlaveInfo
	if len(body) < 5 || body[0] != common.COM_REGISTER_SLAVE {
		return info, fmt.Errorf("invalid COM_REGISTER_SLAVE packet")
	}
	info.ServerID = binary.LittleEndian.Uint32(body[1:5])
	offset := 5
	readField := func(field string) (string, error) {
		if offset >= len(body) {
			return "", fmt.Errorf("COM_REGISTER_SLAVE %s is truncated", field)
		}
		length := int(body[offset])
		offset++
		if length > len(body)-offset {
			return "", fmt.Errorf("COM_REGISTER_SLAVE %s is truncated", field)
		}
		value := string(body[offset : offset+length])
		offset += length
		return value, nil
	}
	var err error
	if info.ReportHost, err = readField("report host"); err != nil {
		return registerSlaveInfo{}, err
	}
	if info.ReportUser, err = readField("report user"); err != nil {
		return registerSlaveInfo{}, err
	}
	// Validate and discard report password; never expose it through session
	// attributes or logs.
	if _, err = readField("report password"); err != nil {
		return registerSlaveInfo{}, err
	}
	if len(body)-offset < 10 {
		return registerSlaveInfo{}, fmt.Errorf("COM_REGISTER_SLAVE fixed fields are truncated")
	}
	info.ReportPort = binary.LittleEndian.Uint16(body[offset : offset+2])
	info.MasterID = binary.LittleEndian.Uint32(body[offset+6 : offset+10])
	return info, nil
}

func normalizeGTIDSetForSource(set replication.GTIDSet, sourceUUID string) replication.GTIDSet {
	if len(set) == 0 || strings.TrimSpace(sourceUUID) == "" {
		return set
	}
	canonicalSID := formatGTIDSID(nativeGTIDSID(sourceUUID))
	if canonicalSID == sourceUUID {
		return set
	}
	sequences, ok := set[canonicalSID]
	if !ok {
		return set
	}
	delete(set, canonicalSID)
	if set[sourceUUID] == nil {
		set[sourceUUID] = map[uint64]struct{}{}
	}
	for sequence := range sequences {
		set[sourceUUID][sequence] = struct{}{}
	}
	return set
}

func normalizeGTIDIntervalsForSource(set replication.GTIDIntervals, sourceUUID string) replication.GTIDIntervals {
	if len(set) == 0 || strings.TrimSpace(sourceUUID) == "" {
		return set
	}
	canonicalSID := formatGTIDSID(nativeGTIDSID(sourceUUID))
	if canonicalSID == sourceUUID {
		return set
	}
	intervals, ok := set[canonicalSID]
	if !ok {
		return set
	}
	delete(set, canonicalSID)
	for _, interval := range intervals {
		set.AddRange(sourceUUID, interval.Start, interval.End)
	}
	return set
}
