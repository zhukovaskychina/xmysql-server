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

	log "github.com/AlexStocks/log4go"
	"github.com/zhukovaskychina/xmysql-server/server"
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
	payload := body[offset:end]
	// Binary v0 has no format marker: its first byte is the low byte of the
	// TSID count. Binary v1/v2 add a marker and a redundant marker at byte 7,
	// so use that pair to disambiguate the legacy layout.
	format := byte(0)
	if len(payload) >= 8 && (payload[0] == 1 || payload[0] == 2) && payload[7] == payload[0] {
		format = payload[0]
	}
	switch format {
	case 0:
		return parseBinlogGTIDSetBinaryV0(payload)
	case 1:
		return parseBinlogGTIDSetBinaryV1(payload)
	case 2:
		return parseBinlogGTIDSetBinaryV2(payload)
	default:
		return nil, fmt.Errorf("unsupported COM_BINLOG_DUMP_GTID GTID set format %d", format)
	}
}

func parseBinlogGTIDSetBinaryV0(payload []byte) (replication.GTIDIntervals, error) {
	// MySQL 8.4 sends the v0 SID count as an 8-byte integer. Older clients
	// and existing fixtures use the historical 4-byte form, so accept both
	// layouts and prefer the lossless 8-byte interpretation when it parses
	// the complete payload.
	if len(payload) >= 8 {
		if set, err := parseBinlogGTIDSetBinaryV0WithCountWidth(payload, 8); err == nil {
			return set, nil
		}
	}
	return parseBinlogGTIDSetBinaryV0WithCountWidth(payload, 4)
}

func parseBinlogGTIDSetBinaryV0WithCountWidth(payload []byte, countWidth int) (replication.GTIDIntervals, error) {
	set := replication.GTIDIntervals{}
	if countWidth != 4 && countWidth != 8 || len(payload) < countWidth {
		return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID Binary v0 GTID set is truncated")
	}
	count := uint64(0)
	if countWidth == 8 {
		count = binary.LittleEndian.Uint64(payload[:8])
	} else {
		count = uint64(binary.LittleEndian.Uint32(payload[:4]))
	}
	if count > uint64((len(payload)-countWidth)/24) {
		return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID SID count is invalid")
	}
	offset := countWidth
	for sidIndex := uint64(0); sidIndex < count; sidIndex++ {
		if offset > len(payload)-24 {
			return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID SID block is truncated")
		}
		uuid := formatGTIDSID(payload[offset : offset+16])
		offset += 16
		intervalCount := binary.LittleEndian.Uint64(payload[offset : offset+8])
		offset += 8
		if intervalCount > uint64((len(payload)-offset)/16) {
			return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID interval count is invalid")
		}
		for intervalIndex := uint64(0); intervalIndex < intervalCount; intervalIndex++ {
			start := binary.LittleEndian.Uint64(payload[offset : offset+8])
			finish := binary.LittleEndian.Uint64(payload[offset+8 : offset+16])
			offset += 16
			if start == 0 || finish <= start {
				return nil, fmt.Errorf("invalid COM_BINLOG_DUMP_GTID interval %d-%d", start, finish)
			}
			if err := set.AddHalfOpen(uuid, start, finish); err != nil {
				return nil, err
			}
		}
	}
	if offset != len(payload) {
		return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID GTID set has %d trailing bytes", len(payload)-offset)
	}
	return set, nil
}

func parseBinlogGTIDSetBinaryV1(payload []byte) (replication.GTIDIntervals, error) {
	set := replication.GTIDIntervals{}
	if len(payload) < 8 || payload[0] != 1 || payload[7] != 1 {
		return nil, fmt.Errorf("invalid COM_BINLOG_DUMP_GTID Binary v1 header")
	}
	sidCount := readBinlogUint48(payload[1:7])
	offset := 8
	for sidIndex := uint64(0); sidIndex < sidCount; sidIndex++ {
		if offset > len(payload)-17 {
			return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID Binary v1 TSID is truncated")
		}
		uuid := formatGTIDSID(payload[offset : offset+16])
		offset += 16
		tag, next, err := readBinlogGTIDTag(payload, offset)
		if err != nil {
			return nil, err
		}
		offset = next
		if offset > len(payload)-8 {
			return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID Binary v1 interval set is truncated")
		}
		intervalCount := binary.LittleEndian.Uint64(payload[offset : offset+8])
		offset += 8
		if intervalCount > uint64((len(payload)-offset)/16) {
			return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID Binary v1 interval count is invalid")
		}
		key := uuid
		if tag != "" {
			key += ":" + tag
		}
		for intervalIndex := uint64(0); intervalIndex < intervalCount; intervalIndex++ {
			start := binary.LittleEndian.Uint64(payload[offset : offset+8])
			finish := binary.LittleEndian.Uint64(payload[offset+8 : offset+16])
			offset += 16
			if start == 0 || finish <= start {
				return nil, fmt.Errorf("invalid COM_BINLOG_DUMP_GTID Binary v1 interval %d-%d", start, finish)
			}
			if err := set.AddHalfOpen(key, start, finish); err != nil {
				return nil, err
			}
		}
	}
	if offset != len(payload) {
		return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID Binary v1 GTID set has %d trailing bytes", len(payload)-offset)
	}
	return set, nil
}

func parseBinlogGTIDSetBinaryV2(payload []byte) (replication.GTIDIntervals, error) {
	set := replication.GTIDIntervals{}
	if len(payload) < 8 || payload[0] != 2 || payload[7] != 2 {
		return nil, fmt.Errorf("invalid COM_BINLOG_DUMP_GTID Binary v2 header")
	}
	sidCount := readBinlogUint48(payload[1:7])
	offset := 8
	tagCount, next, err := readBinlogSerializationVarlen(payload, offset)
	if err != nil {
		return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID Binary v2 tag count: %w", err)
	}
	offset = next
	if tagCount > uint64(len(payload)-offset) {
		return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID Binary v2 tag count is invalid")
	}
	tags := make([]string, tagCount)
	for index := range tags {
		tag, next, tagErr := readBinlogGTIDTag(payload, offset)
		if tagErr != nil {
			return nil, tagErr
		}
		tags[index] = tag
		offset = next
	}
	var previousUUID string
	for sidIndex := uint64(0); sidIndex < sidCount; sidIndex++ {
		code, next, codeErr := readBinlogSerializationVarlen(payload, offset)
		if codeErr != nil {
			return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID Binary v2 TSID code: %w", codeErr)
		}
		offset = next
		tagOrdinal := code >> 1
		if tagOrdinal >= tagCount {
			return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID Binary v2 tag ordinal %d is invalid", tagOrdinal)
		}
		uuid := previousUUID
		if code&1 == 0 {
			if offset > len(payload)-16 {
				return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID Binary v2 UUID is truncated")
			}
			uuid = formatGTIDSID(payload[offset : offset+16])
			offset += 16
		} else if uuid == "" {
			return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID Binary v2 TSID repeats an empty UUID")
		}
		previousUUID = uuid
		key := uuid
		if tags[tagOrdinal] != "" {
			key += ":" + tags[tagOrdinal]
		}
		if err := parseBinlogGTIDIntervalSetV2(payload, &offset, key, set); err != nil {
			return nil, err
		}
	}
	if offset != len(payload) {
		return nil, fmt.Errorf("COM_BINLOG_DUMP_GTID Binary v2 GTID set has %d trailing bytes", len(payload)-offset)
	}
	return set, nil
}

func parseBinlogGTIDIntervalSetV2(payload []byte, offset *int, key string, set replication.GTIDIntervals) error {
	encodedCount, next, err := readBinlogSerializationVarlen(payload, *offset)
	if err != nil {
		return fmt.Errorf("COM_BINLOG_DUMP_GTID Binary v2 interval count: %w", err)
	}
	*offset = next
	if encodedCount > uint64(len(payload)-*offset)+1 {
		return fmt.Errorf("COM_BINLOG_DUMP_GTID Binary v2 interval count is invalid")
	}
	optimizedFirst := encodedCount%2 == 1
	boundaryCount := encodedCount
	if optimizedFirst {
		boundaryCount++
	}
	if boundaryCount == 0 || boundaryCount%2 != 0 {
		return fmt.Errorf("COM_BINLOG_DUMP_GTID Binary v2 interval boundaries are invalid")
	}
	boundaries := make([]uint64, 0, boundaryCount)
	if optimizedFirst {
		boundaries = append(boundaries, 1)
	}
	for len(boundaries) < int(boundaryCount) {
		delta, next, deltaErr := readBinlogSerializationVarlen(payload, *offset)
		if deltaErr != nil {
			return fmt.Errorf("COM_BINLOG_DUMP_GTID Binary v2 interval delta: %w", deltaErr)
		}
		*offset = next
		if len(boundaries) == 0 {
			if delta > ^uint64(0)-2 {
				return fmt.Errorf("COM_BINLOG_DUMP_GTID Binary v2 interval start overflows")
			}
			boundaries = append(boundaries, delta+2)
			continue
		}
		previous := boundaries[len(boundaries)-1]
		if delta > ^uint64(0)-previous-1 {
			return fmt.Errorf("COM_BINLOG_DUMP_GTID Binary v2 interval boundary overflows")
		}
		boundaries = append(boundaries, previous+delta+1)
	}
	for index := 0; index < len(boundaries); index += 2 {
		start, finish := boundaries[index], boundaries[index+1]
		if start == 0 || finish <= start {
			return fmt.Errorf("invalid COM_BINLOG_DUMP_GTID Binary v2 interval %d-%d", start, finish)
		}
		if err := set.AddHalfOpen(key, start, finish); err != nil {
			return err
		}
	}
	return nil
}

func readBinlogUint48(raw []byte) uint64 {
	var value uint64
	for index := 0; index < 6; index++ {
		value |= uint64(raw[index]) << uint(8*index)
	}
	return value
}

func readBinlogGTIDTag(payload []byte, offset int) (string, int, error) {
	if offset < 0 || offset >= len(payload) {
		return "", 0, fmt.Errorf("COM_BINLOG_DUMP_GTID GTID tag is truncated")
	}
	length := int(payload[offset])
	offset++
	if length > 32 || length > len(payload)-offset {
		return "", 0, fmt.Errorf("invalid COM_BINLOG_DUMP_GTID GTID tag length")
	}
	tag := string(payload[offset : offset+length])
	if !validBinlogGTIDTag(tag) {
		return "", 0, fmt.Errorf("invalid COM_BINLOG_DUMP_GTID GTID tag %q", tag)
	}
	return tag, offset + length, nil
}

func validBinlogGTIDTag(tag string) bool {
	if tag == "" {
		return true
	}
	for index, char := range []byte(tag) {
		if index == 0 {
			if !(char == '_' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z') {
				return false
			}
			continue
		}
		if !(char == '_' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9') {
			return false
		}
	}
	return true
}

func readBinlogSerializationVarlen(raw []byte, offset int) (uint64, int, error) {
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
		xidEventType    = 16
	)
	if event.Type == replication.EventBegin {
		gtidEventType, body := nativeGTIDEvent(event)
		return buildNativeEvent(gtidEventType, body, event), nil
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
	sid, _, _ := nativeGTIDParts(event.GTID.UUID)
	copy(body[1:17], sid)
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

func nativeGTIDEvent(event replication.BinlogEvent) (byte, []byte) {
	sid, tag, tagged := nativeGTIDParts(event.GTID.UUID)
	if tagged {
		return 42, nativeTaggedGTIDBody(event, sid, tag)
	}
	return 33, nativeGTIDBody(event)
}

func nativeTaggedGTIDBody(event replication.BinlogEvent, sid []byte, tag string) []byte {
	fields := make([]byte, 0, 32+len(tag))
	fields = appendNativeSerializationVarlen(fields, 0) // flags field id
	fields = appendNativeSerializationVarlen(fields, 0) // flags
	fields = appendNativeSerializationVarlen(fields, 1) // SID field id
	fields = append(fields, sid...)
	fields = appendNativeSerializationVarlen(fields, 2) // GNO field id
	fields = appendNativeSerializationVarlen(fields, event.GTID.Seq<<1)
	fields = appendNativeSerializationVarlen(fields, 3) // tag field id
	fields = appendNativeSerializationVarlen(fields, uint64(len(tag)))
	fields = append(fields, tag...)
	body := make([]byte, 0, 3+len(fields))
	body = appendNativeSerializationVarlen(body, 1)
	body = appendNativeSerializationVarlen(body, uint64(len(fields)))
	body = appendNativeSerializationVarlen(body, 3)
	body = append(body, fields...)
	return body
}

func appendNativeSerializationVarlen(dst []byte, value uint64) []byte {
	for numBytes := 1; numBytes <= 8; numBytes++ {
		payloadBits := 8 - numBytes
		if payloadBits == 0 || value >= uint64(1)<<uint(payloadBits) {
			continue
		}
		dst = append(dst, byte(value<<uint(numBytes))|byte((uint64(1)<<uint(numBytes-1))-1))
		shift := uint(payloadBits)
		for index := 1; index < numBytes; index++ {
			dst = append(dst, byte(value>>shift))
			shift += 8
		}
		return dst
	}
	dst = append(dst, 0xff)
	for index := 0; index < 8; index++ {
		dst = append(dst, byte(value>>uint(index*8)))
	}
	return dst
}

func nativeGTIDParts(raw string) ([]byte, string, bool) {
	parts := strings.SplitN(strings.TrimSpace(raw), ":", 2)
	if len(parts) == 2 && nativeCanonicalUUID(parts[0]) && validNativeGTIDTag(parts[1]) && parts[1] != "" {
		return nativeGTIDSID(parts[0]), parts[1], true
	}
	return nativeGTIDSID(raw), "", false
}

func nativeCanonicalUUID(raw string) bool {
	compact := strings.ReplaceAll(strings.TrimSpace(raw), "-", "")
	decoded, err := hex.DecodeString(compact)
	return err == nil && len(decoded) == 16
}

func validNativeGTIDTag(tag string) bool {
	if tag == "" {
		return true
	}
	for index, char := range []byte(tag) {
		if index == 0 {
			if !(char == '_' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z') {
				return false
			}
			continue
		}
		if !(char == '_' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9') {
			return false
		}
	}
	return len(tag) <= 32
}

func nativeGTIDSID(raw string) []byte {
	if parts := strings.SplitN(strings.TrimSpace(raw), ":", 2); len(parts) == 2 && nativeCanonicalUUID(parts[0]) {
		raw = parts[0]
	}
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
	if err := checkBinlogDumpPrivilege(session); err != nil {
		return session.WriteBytes(protocol.EncodeErrorFromGoError(err))
	}
	source, ok := session.GetAttribute("replication_source").(*replication.Source)
	if !ok || source == nil {
		return session.WriteBytes(protocol.EncodeNotSupportedError("COM_BINLOG_DUMP requires a configured replication source"))
	}
	if err := validateBinlogDumpRequest(body); err != nil {
		log.Warn("COM_BINLOG_DUMP request rejected: %v", err)
		return session.WriteBytes(protocol.EncodeErrorFromGoError(err))
	}
	startPosition := binlogDumpStartPosition(body)
	gtidIntervals, err := binlogDumpGTIDIntervals(body)
	if err != nil {
		log.Warn("COM_BINLOG_DUMP_GTID request rejected: %v", err)
		return session.WriteBytes(protocol.EncodeErrorFromGoError(err))
	}
	gtidIntervals = normalizeGTIDIntervalsForSource(gtidIntervals, source.UUID)
	// MySQL 8.4 already seeds the relay log with its own format and previous
	// GTID metadata before issuing a non-empty COM_BINLOG_DUMP_GTID request.
	// Replaying the source FDE/PREVIOUS_GTIDS pair here makes the official
	// replica synthesize an invalid rotate boundary. Keep metadata for an
	// initial empty-set GTID request and for legacy position-based dumping.
	skipDuplicateGTIDMetadata := body[0] == common.COM_BINLOG_DUMP_GTID && len(gtidIntervals) > 0
	blocking := binlogDumpFlags(body)&binlogDumpNonBlockFlag == 0
	logName := binlogDumpFileName(body)
	nextPosition := startPosition
	// The command packet is sequence 0; the first server response packet is
	// sequence 1, just like a normal COM_QUERY response.
	sequence := byte(1)
	for {
		rawEvents, observedNextPosition, err := source.NativeDumpFileWithIntervals(logName, nextPosition, gtidIntervals)
		if err != nil {
			log.Warn("COM_BINLOG_DUMP source stream failed (file=%s position=%d): %v", logName, nextPosition, err)
			return session.WriteBytes(protocol.EncodeErrorFromGoError(err))
		}
		for _, event := range rawEvents {
			if skipDuplicateGTIDMetadata && (event.Type == 15 || event.Type == 35) {
				continue
			}
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

// checkBinlogDumpPrivilege enforces the authenticated replication-reader
// boundary for the native COM_BINLOG_DUMP commands. Unit-level callers that
// do not bind a MySQLServerSession remain internal projections and preserve
// their existing behavior.
func checkBinlogDumpPrivilege(session Session) error {
	if session == nil {
		return nil
	}
	mysqlSession, ok := session.GetAttribute(mysqlSessionAttribute).(server.MySQLServerSession)
	if !ok || mysqlSession == nil {
		return nil
	}
	privileges, _ := mysqlSession.GetParamByName("global_privileges").([]common.PrivilegeType)
	for _, privilege := range privileges {
		if privilege == common.ReplicationSlavePriv || privilege == common.SuperPriv || privilege == common.AllPriv {
			return nil
		}
	}
	return fmt.Errorf("Access denied; you need (at least one of) the REPLICATION SLAVE privilege(s) for this operation")
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
		// UNKNOWN_EVENT is not represented in the format table. MySQL 8.4
		// therefore carries post-header lengths for event types 1..42.
		eventTypeCount        = 42
		checksumAlgorithmSize = 1
	)
	body := make([]byte, 2+serverVersionLength+4+1+eventTypeCount+checksumAlgorithmSize)
	binary.LittleEndian.PutUint16(body[0:2], 4)
	serverVersion := []byte("8.4.0-xmysql")
	copy(body[2:2+serverVersionLength], serverVersion)
	body[2+serverVersionLength+4] = eventHeaderLength
	postHeaderLengths := body[2+serverVersionLength+5:]
	// This is the MySQL 8.4 FDE table, including the UNKNOWN_EVENT slot at
	// index 0 and the checksum algorithm byte at the end.
	copy(postHeaderLengths, []byte{
		0, 13, 0, 8, 0, 0, 0, 0, 4, 0, 4, 0, 0, 0, 99, 0,
		4, 26, 8, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 10, 10, 10,
		42, 42, 0, 18, 52, 0, 10, 40, 0, 0, 1,
	})
	postHeaderLengths[eventTypeCount] = 1 // CRC32 checksum algorithm
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
	if err := checkBinlogDumpPrivilege(session); err != nil {
		return session.WriteBytes(protocol.EncodeErrorFromGoError(err))
	}
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
