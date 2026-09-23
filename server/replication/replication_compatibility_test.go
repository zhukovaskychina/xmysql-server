package replication

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestBinlogWriterReadFromSerializesWithConcurrentAppend(t *testing.T) {
	writer, err := NewBinlogWriter(filepath.Join(t.TempDir(), "binlog.jsonl"), 17)
	require.NoError(t, err)

	var waitGroup sync.WaitGroup
	errors := make(chan error, 1)
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		for sequence := uint64(1); sequence <= 100; sequence++ {
			if _, appendErr := writer.AppendTransaction(GTID{UUID: "source", Seq: sequence}, nil, nil); appendErr != nil {
				errors <- appendErr
				return
			}
		}
	}()
	for index := 0; index < 200; index++ {
		if _, readErr := writer.ReadFrom(4); readErr != nil {
			errors <- readErr
			break
		}
	}
	waitGroup.Wait()
	select {
	case err := <-errors:
		require.NoError(t, err)
	default:
	}
}

func TestBinlogWriterReadsLargeTransactionRecord(t *testing.T) {
	writer, err := NewBinlogWriter(filepath.Join(t.TempDir(), "binlog.jsonl"), 17)
	require.NoError(t, err)
	largeSQL := "insert into docs values ('" + strings.Repeat("x", 100000) + "')"
	_, err = writer.AppendTransaction(GTID{UUID: "source", Seq: 1}, nil, []Statement{{Database: "app", SQL: largeSQL}})
	require.NoError(t, err)

	events, err := writer.ReadFrom(4)
	require.NoError(t, err)
	require.Len(t, events, 3)
	require.Equal(t, largeSQL, events[1].Statements[0].SQL)
}

func TestBinlogWriterMaintainsNativeBinlogFile(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewBinlogWriter(filepath.Join(dir, "binlog.jsonl"), 17)
	require.NoError(t, err)

	_, err = writer.AppendTransaction(GTID{UUID: "00112233-4455-6677-8899-aabbccddeeff", Seq: 1}, nil, []Statement{{
		Database: "app",
		SQL:      "insert into docs values (1)",
	}})
	require.NoError(t, err)

	raw, err := os.ReadFile(filepath.Join(dir, "binlog.000001"))
	require.NoError(t, err)
	require.Greater(t, len(raw), 4)
	require.Equal(t, []byte{0xfe, 'b', 'i', 'n'}, raw[:4])

	eventTypes := make([]byte, 0, 4)
	offset := 4
	for offset < len(raw) {
		require.GreaterOrEqual(t, len(raw)-offset, 19)
		eventSize := int(binary.LittleEndian.Uint32(raw[offset+9 : offset+13]))
		require.GreaterOrEqual(t, eventSize, 23)
		require.LessOrEqual(t, offset+eventSize, len(raw))
		eventTypes = append(eventTypes, raw[offset+4])
		offset += eventSize
	}
	require.Equal(t, []byte{15, 35, 33, 2, 16}, eventTypes)
	require.Equal(t, uint64(len(raw)), writer.NativeFileSize())
}

func TestNativeEventsReadsChecksumOffFormatDescription(t *testing.T) {
	dir := t.TempDir()
	stripChecksum := func(raw []byte) []byte {
		withoutChecksum := append([]byte(nil), raw[:len(raw)-nativeChecksumLength]...)
		binary.LittleEndian.PutUint32(withoutChecksum[9:13], uint32(len(withoutChecksum)))
		return withoutChecksum
	}
	formatBody := append([]byte(nil), buildNativeFormatDescriptionEvent(17, 4)[nativeEventHeaderLength:len(buildNativeFormatDescriptionEvent(17, 4))-nativeChecksumLength]...)
	formatBody = append(formatBody, 0)
	format := stripChecksum(buildNativeEvent(15, formatBody, BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17, Position: 4}))
	previous := stripChecksum(buildNativePreviousGTIDsEvent(17, uint64(4+len(format)), GTIDSet{}))
	path := filepath.Join(dir, "binlog.000001")
	require.NoError(t, os.WriteFile(path, append(append([]byte{nativeBinlogMagic, 'b', 'i', 'n'}, format...), previous...), 0644))
	writer := &BinlogWriter{path: filepath.Join(dir, "binlog.jsonl"), nativePath: path, nativeIndex: 1}
	events, err := writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, 0, events[0].ChecksumLength)
	require.Equal(t, 0, events[1].ChecksumLength)
	require.Equal(t, len(format), len(events[0].Raw))
	require.Equal(t, len(previous), len(events[1].Raw))
	query := stripChecksum(buildNativeQueryEventForTest(BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17, Position: 100}, "insert into docs values (1)"))
	xid := stripChecksum(buildNativeEvent(16, make([]byte, 8), BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17, Position: 200}))
	decoded, err := NewNativeBinlogDecoder().DecodeTransactions([]NativeBinlogEvent{
		{File: "binlog.000001", Position: 100, Type: 2, Raw: query, ChecksumKnown: true, ChecksumLength: 0},
		{File: "binlog.000001", Position: 200, Type: 16, Raw: xid, ChecksumKnown: true, ChecksumLength: 0},
	})
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Equal(t, "insert into docs values (1)", decoded[0].Statements[0].SQL)
}

func TestBinlogWriterPersistsSchemaAwareNativeRowEvents(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewBinlogWriter(filepath.Join(dir, "binlog.jsonl"), 17)
	require.NoError(t, err)
	_, err = writer.AppendTransaction(GTID{UUID: "source", Seq: 1}, []RowChange{{
		Table:  "app.docs",
		Action: "insert",
		After:  map[string]interface{}{"id": int64(1), "title": "hello"},
	}}, nil)
	require.NoError(t, err)
	raw, err := os.ReadFile(filepath.Join(dir, "binlog.000001"))
	require.NoError(t, err)
	require.Equal(t, []byte{15, 35, 33, 19, 30, 16}, nativeEventTypes(raw))
}

func TestNativeRowFramesCarryPhysicalNextEventPositions(t *testing.T) {
	source, err := NewSource(t.TempDir(), "source-physical-positions", 17)
	require.NoError(t, err)
	_, err = source.AppendTransaction(1, []RowChange{
		{Table: "app.docs", Action: "insert", Columns: []string{"id"}, After: map[string]interface{}{"id": int64(1)}},
		{Table: "app.docs", Action: "insert", Columns: []string{"id"}, After: map[string]interface{}{"id": int64(2)}},
	}, nil)
	require.NoError(t, err)
	native, err := source.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	for _, event := range native {
		require.Equal(t, event.EndPosition, uint64(binary.LittleEndian.Uint32(event.Raw[13:17])))
	}
}

func TestNativeRowColumnsPreferProvidedSchemaOrder(t *testing.T) {
	change := RowChange{
		Table:   "app.docs",
		Action:  "insert",
		Columns: []string{"title", "id"},
		After:   map[string]interface{}{"id": int64(1), "title": "hello"},
	}
	require.Equal(t, []string{"title", "id"}, nativeRowColumnsForChange(change))
}

func TestNativeRowTypeHintsOverrideValueInference(t *testing.T) {
	change := RowChange{ColumnTypes: map[string]string{
		"small":   "SMALLINT UNSIGNED",
		"amount":  "DECIMAL(10,2)",
		"payload": "JSON",
	}}
	require.Equal(t, byte(2), nativeColumnTypeForChange(change, "small", int64(1)))
	require.Equal(t, byte(246), nativeColumnTypeForChange(change, "amount", "1.20"))
	require.Equal(t, byte(245), nativeColumnTypeForChange(change, "payload", "{}"))
}

func TestNativeRowTypeHintsCoverCommonMySQLTypes(t *testing.T) {
	for _, test := range []struct {
		name string
		want byte
	}{
		{name: "CHAR", want: 254},
		{name: "ENUM", want: 254},
		{name: "SET", want: 254},
		{name: "TINYBLOB", want: 249},
		{name: "BLOB", want: 252},
		{name: "MEDIUMTEXT", want: 250},
		{name: "LONGBLOB", want: 251},
		{name: "VARBINARY", want: 253},
		{name: "BINARY", want: 254},
		{name: "GEOMETRY", want: 255},
		{name: "MEDIUMINT", want: 9},
		{name: "TIME2", want: 17},
		{name: "DATETIME2", want: 18},
		{name: "TIMESTAMP2", want: 19},
		{name: "TYPED_ARRAY", want: 20},
		{name: "VECTOR", want: 242},
		{name: "NULL", want: 6},
	} {
		got, recognized := nativeColumnTypeForName(test.name)
		require.True(t, recognized, test.name)
		require.Equal(t, test.want, got, test.name)
	}
}

func TestNativeRowValuesPreserveOpaqueVectorBytes(t *testing.T) {
	for _, typeName := range []string{"TYPED_ARRAY", "VECTOR"} {
		change := RowChange{ColumnTypes: map[string]string{"value": typeName}}
		require.Equal(t, []byte{3, 0xaa, 0xbb, 0xcc}, nativeValueBytesForColumn(change, "value", []byte{0xaa, 0xbb, 0xcc}), typeName)
		large := bytes.Repeat([]byte{0x7f}, 300)
		encoded := nativeValueBytesForColumn(change, "value", large)
		require.Equal(t, byte(0x2c), encoded[0], typeName)
		require.Equal(t, byte(0x01), encoded[1], typeName)
		require.Equal(t, large, encoded[2:], typeName)
	}
}

func TestNativeVectorMetadataCarriesDimensionality(t *testing.T) {
	require.Equal(t, uint64(128), mustNativeVectorDimension("VECTOR(128)", nil))
	require.Equal(t, uint64(2), mustNativeVectorDimension("VECTOR", []byte{0, 0, 0, 0, 0, 0, 0, 0}))
	change := RowChange{
		Columns:     []string{"embedding"},
		ColumnTypes: map[string]string{"embedding": "VECTOR(128)"},
		After:       map[string]interface{}{"embedding": bytes.Repeat([]byte{0}, 128*4)},
	}
	optional := nativeTableMapOptionalMetadata(change, change.Columns, []byte{242})
	require.Contains(t, optional, byte(13), "TABLE_MAP must carry VECTOR_DIMENSIONALITY metadata")
}

func mustNativeVectorDimension(typeName string, value []byte) uint64 {
	if dimension, ok := nativeVectorDimensionForName(typeName); ok {
		return dimension
	}
	dimension, ok := nativeVectorDimensionForValue(value)
	if !ok {
		return 0
	}
	return dimension
}

func TestNativeDecoderConsumesVectorAndTypedArrayValues(t *testing.T) {
	for _, test := range []struct {
		name     string
		typeCode byte
		metadata []byte
	}{
		{name: "typed array", typeCode: 20, metadata: []byte{1}},
		{name: "vector", typeCode: 242, metadata: []byte{2}},
	} {
		body := []byte{3, 0xaa, 0xbb, 0xcc}
		wantNext := 4
		if len(test.metadata) > 0 && test.metadata[0] == 2 {
			body = []byte{3, 0, 0xaa, 0xbb, 0xcc}
			wantNext = 5
		}
		value, updates, next, err := decodeNativeValue(test.typeCode, test.metadata, false, false, body, 0)
		require.NoError(t, err, test.name)
		require.Nil(t, updates, test.name)
		require.Equal(t, []byte{0xaa, 0xbb, 0xcc}, value, test.name)
		require.Equal(t, wantNext, next, test.name)
	}
	table := nativeDecoderTable{types: []byte{20, 242}, metadata: [][]byte{{1}, {2}}, unsigned: []bool{false, false}}
	require.Equal(t, "TYPED_ARRAY", nativeDecoderColumnType(table, 0))
	require.Equal(t, "VECTOR", nativeDecoderColumnType(table, 1))
}

func TestNativeMediumIntUsesThreeByteInt24Encoding(t *testing.T) {
	change := RowChange{ColumnTypes: map[string]string{"value": "MEDIUMINT"}}
	require.Equal(t, []byte{0xff, 0xff, 0x7f}, nativeValueBytesForColumn(change, "value", int32(8388607)))
	require.Equal(t, []byte{0x00, 0x00, 0x80}, nativeValueBytesForColumn(change, "value", int32(-8388608)))
	require.Equal(t, []byte{0xff, 0xff, 0xff}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{"value": "MEDIUMINT UNSIGNED"}}, "value", uint32(0xffffff)))
	require.Len(t, nativeValueBytesForColumn(change, "value", int32(8388608)), 4, "out-of-range signed values keep the conservative fallback")
}

func TestNativeRowFloatPrecisionSelectsWireWidth(t *testing.T) {
	typeCode, recognized := nativeColumnTypeForName("FLOAT(24)")
	require.True(t, recognized)
	require.Equal(t, byte(4), typeCode)
	typeCode, recognized = nativeColumnTypeForName("FLOAT(30)")
	require.True(t, recognized)
	require.Equal(t, byte(5), typeCode)
	require.Equal(t, []byte{4}, nativeColumnMetadataForTypeName("FLOAT(24)", 4))
	require.Equal(t, []byte{8}, nativeColumnMetadataForTypeName("FLOAT(30)", 5))
}

func TestNativeRowMetadataUsesTypeHints(t *testing.T) {
	change := RowChange{ColumnTypes: map[string]string{
		"small":  "SMALLINT",
		"ratio":  "DOUBLE",
		"amount": "DECIMAL(10,2)",
		"name":   "VARCHAR(64)",
	}}
	require.Empty(t, nativeColumnMetadataForChange(change, "small", 2, int64(1)))
	require.Equal(t, []byte{8}, nativeColumnMetadataForChange(change, "ratio", 5, float64(1.5)))
	require.Equal(t, []byte{10, 2}, nativeColumnMetadataForChange(change, "amount", 246, "1.20"))
	require.Equal(t, []byte{64, 0}, nativeColumnMetadataForChange(change, "name", 15, "hello"))
}

func TestNativeRowMetadataCoversBitAndBlobPackLengths(t *testing.T) {
	require.Equal(t, []byte{4}, nativeColumnMetadataForValue(4, float32(1.5)))
	require.Equal(t, []byte{8}, nativeColumnMetadataForValue(5, float64(1.5)))
	require.Equal(t, []byte{1, 1}, nativeColumnMetadataForTypeName("BIT(9)", 16))
	require.Equal(t, []byte{1}, nativeColumnMetadataForTypeName("TINYBLOB", 249))
	require.Equal(t, []byte{2}, nativeColumnMetadataForTypeName("BLOB", 252))
	require.Equal(t, []byte{3}, nativeColumnMetadataForTypeName("MEDIUMBLOB", 250))
	require.Equal(t, []byte{4}, nativeColumnMetadataForTypeName("LONGBLOB", 251))
	require.Equal(t, []byte{2}, nativeColumnMetadataForTypeName("BLOB(1024)", 252))
	require.Equal(t, []byte{4}, nativeColumnMetadataForTypeName("JSON", 245))
	require.Equal(t, []byte{4}, nativeColumnMetadataForTypeName("GEOMETRY", 255))
}

func TestNativeRowMetadataAndValuesUseEnumSetWireTypes(t *testing.T) {
	require.Equal(t, []byte{247, 1}, nativeColumnMetadataForTypeName("ENUM('draft','published')", 254))
	require.Equal(t, []byte{248, 1}, nativeColumnMetadataForTypeName("SET('read','write','admin')", 254))
	require.Equal(t, []byte{2}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"status": "ENUM('draft','published')",
	}}, "status", int64(2)))
	require.Equal(t, []byte{5}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"permissions": "SET('read','write','admin')",
	}}, "permissions", int64(5)))
	require.Equal(t, []byte{2}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"status": "ENUM('draft','published')",
	}}, "status", "published"))
	require.Equal(t, []byte{5}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"permissions": "SET('read','write','admin')",
	}}, "permissions", "read,admin"))
	require.Equal(t, []byte{247, 1}, nativeColumnMetadataForTypeName("ENUM('a,b','c')", 254))
	require.Equal(t, []byte{1}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"status": `ENUM('can''t','ready')`,
	}}, "status", "can't"))
	require.Equal(t, []byte{3}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"permissions": `SET('read','owner''s')`,
	}}, "permissions", "read,owner's"))
}

func TestNativeRowValuesUseEachBlobLengthPrefixWidth(t *testing.T) {
	for _, test := range []struct {
		name string
		want []byte
	}{
		{name: "TINYBLOB", want: []byte{3, 'a', 'b', 'c'}},
		{name: "BLOB", want: []byte{3, 0, 'a', 'b', 'c'}},
		{name: "MEDIUMBLOB", want: []byte{3, 0, 0, 'a', 'b', 'c'}},
		{name: "LONGBLOB", want: []byte{3, 0, 0, 0, 'a', 'b', 'c'}},
	} {
		require.Equal(t, test.want, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
			"payload": test.name,
		}}, "payload", "abc"), test.name)
	}
}

func TestNativeRowStringMetadataAndValuesUseFieldStringPacking(t *testing.T) {
	require.Equal(t, []byte{254, 10}, nativeColumnMetadataForTypeName("CHAR(10)", 254))
	require.Equal(t, []byte{238, 44}, nativeColumnMetadataForTypeName("BINARY(300)", 254))
	require.Equal(t, append([]byte{'a', 'b', 'c'}, []byte("       ")...), nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"name": "CHAR(10)",
	}}, "name", "abc"))
	require.Equal(t, append([]byte{'a', 'b', 'c'}, make([]byte, 297)...), nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"payload": "BINARY(300)",
	}}, "payload", "abc"))
}

func TestNativeRowLegacyTemporalValuesUseMySQLWidths(t *testing.T) {
	require.Equal(t, []byte{0}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"year": "YEAR",
	}}, "year", "0000"))
	require.Equal(t, []byte{124}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"year": "YEAR",
	}}, "year", "2024"))
	require.Equal(t, []byte{0}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"year": "YEAR",
	}}, "year", int64(0)))
	require.Equal(t, []byte{0x80, 0x60, 0xe6, 0x65}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"created": "TIMESTAMP",
	}}, "created", "2024-03-05 00:00:00"))
	require.Equal(t, []byte{0, 0, 0, 0}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"created": "TIMESTAMP",
	}}, "created", "0000-00-00 00:00:00"))
	require.Equal(t, []byte{0x65, 0xd0, 0x0f}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"date": "NEWDATE",
	}}, "date", "2024-03-05"))
	require.Equal(t, []byte{0, 0, 0}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"date": "DATE",
	}}, "date", "0000-00-00"))
	require.Equal(t, []byte{0x3f, 0x25, 0x02}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"time": "TIME",
	}}, "time", "14:06:07"))
	require.Equal(t, []byte{0xc1, 0xda, 0xfd}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"time": "TIME",
	}}, "time", "-14:06:07"))
	require.Equal(t, []byte{0x7f, 0xb3, 0x32, 0x90, 0x68, 0x12, 0x00, 0x00}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"created": "DATETIME",
	}}, "created", "2024-03-05 14:06:07"))
	require.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{
		"created": "DATETIME",
	}}, "created", "0000-00-00 00:00:00"))
	value, updates, next, err := decodeNativeValue(12, nil, false, false, []byte{0x7f, 0xb3, 0x32, 0x90, 0x68, 0x12, 0x00, 0x00}, 0)
	require.NoError(t, err)
	require.Nil(t, updates)
	require.Equal(t, "2024-03-05 14:06:07", value)
	require.Equal(t, 8, next)
	value, updates, next, err = decodeNativeValue(12, nil, false, false, make([]byte, 8), 0)
	require.NoError(t, err)
	require.Nil(t, updates)
	require.Equal(t, "0000-00-00 00:00:00", value)
	require.Equal(t, 8, next)
}

func TestNativeRowMetadataCoversModernTemporalPrecision(t *testing.T) {
	require.Equal(t, []byte{0}, nativeColumnMetadataForTypeName("TIME2", 17))
	require.Equal(t, []byte{6}, nativeColumnMetadataForTypeName("TIME2(6)", 17))
	require.Equal(t, []byte{3}, nativeColumnMetadataForTypeName("DATETIME2(3)", 18))
	require.Equal(t, []byte{0}, nativeColumnMetadataForTypeName("TIMESTAMP2", 19))
}

func TestNativeRowTemporalValuesUseMySQLBinaryEncoding(t *testing.T) {
	instant := time.Date(2024, 3, 5, 14, 6, 7, 123000000, time.UTC)
	require.Equal(t, []byte{0x65, 0xd0, 0x0f}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{"d": "DATE"}}, "d", instant))
	require.Equal(t, []byte{0x87, 0xe1, 0xca, 0xb2, 0x99, 0xce, 0x04}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{"dt": "DATETIME2(3)"}}, "dt", instant))
	require.Equal(t, []byte{0xcf, 0x26, 0xe7, 0x65, 0xce, 0x04}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{"ts": "TIMESTAMP2(3)"}}, "ts", instant))
	require.Equal(t, []byte{0x87, 0xe1, 0x80, 0xce, 0x04}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{"tm": "TIME2(3)"}}, "tm", instant))
}

func TestNativeRowTemporalStringParsingAndSafeFallback(t *testing.T) {
	change := RowChange{ColumnTypes: map[string]string{"d": "DATE", "tm": "TIME2(0)"}}
	require.Equal(t, []byte{0x65, 0xd0, 0x0f}, nativeValueBytesForColumn(change, "d", "2024-03-05"))
	require.Equal(t, []byte{0x87, 0xe1, 0x80}, nativeValueBytesForColumn(change, "tm", "14:06:07"))
	require.Equal(t, []byte{0x79, 0x1e, 0x7f}, nativeValueBytesForColumn(change, "tm", "-14:06:07"))
	require.Equal(t, []byte{0x78, 0x1e, 0x7f, 0x32, 0xfb}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{"tm": "TIME2(3)"}}, "tm", "-14:06:07.123"))
	require.Equal(t, []byte{0x40, 0xe2, 0x01, 0x87, 0xe1, 0x80}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{"tm": "TIME2(6)"}}, "tm", "14:06:07.123456"))
	require.Equal(t, []byte{0xc0, 0x1d, 0xfe, 0x78, 0x1e, 0x7f}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{"tm": "TIME2(6)"}}, "tm", "-14:06:07.123456"))
}

func TestNativeRowEventsGroupCompatibleRows(t *testing.T) {
	event := BinlogEvent{Type: EventRow, GTID: GTID{UUID: "source", Seq: 1}}
	changes := []RowChange{
		{Table: "app.docs", Action: "insert", Columns: []string{"id"}, After: map[string]interface{}{"id": int64(1)}},
		{Table: "app.docs", Action: "insert", Columns: []string{"id"}, After: map[string]interface{}{"id": int64(2)}},
	}
	event.Changes = changes
	payloads := buildNativeEventPayloadsForLogical(event, 17)
	require.Len(t, payloads, 2)
	// One TABLE_MAP plus one WRITE_ROWS event with two row images.
	require.Equal(t, byte(19), payloads[0][4])
	require.Equal(t, byte(30), payloads[1][4])
	require.Equal(t, 31, len(payloads[1])-nativeEventHeaderLength-nativeChecksumLength)
}

func TestNativeRowEventPreservesRowFlags(t *testing.T) {
	event := BinlogEvent{Type: EventRow, GTID: GTID{UUID: "source", Seq: 1}, Changes: []RowChange{{
		Table: "app.docs", Action: "insert", Flags: 1, Columns: []string{"id"}, After: map[string]interface{}{"id": int64(1)},
	}}}
	payloads := buildNativeEventPayloadsForLogical(event, 17)
	require.Len(t, payloads, 2)
	require.Equal(t, []byte{1, 0}, payloads[1][nativeEventHeaderLength+6:nativeEventHeaderLength+8])
}

func TestNativeRowEventPreservesNegotiatedExtraRowInfo(t *testing.T) {
	event := BinlogEvent{Type: EventRow, GTID: GTID{UUID: "source", Seq: 1}, Changes: []RowChange{{
		Table: "app.docs", Action: "insert", ExtraRowInfo: []byte{0x01, 0x02}, Columns: []string{"id"}, After: map[string]interface{}{"id": int64(1)},
	}}}
	payloads := buildNativeEventPayloadsForLogical(event, 17)
	require.Len(t, payloads, 2)
	body := payloads[1][nativeEventHeaderLength:]
	require.Equal(t, []byte{4, 0, 0x01, 0x02}, body[8:12])
}

func TestNativeRowEventPreservesLargeNegotiatedExtraRowInfo(t *testing.T) {
	extra := bytes.Repeat([]byte{0xa5}, 300)
	event := BinlogEvent{Type: EventRow, GTID: GTID{UUID: "source", Seq: 1}, Changes: []RowChange{{
		Table: "app.docs", Action: "insert", ExtraRowInfo: extra, Columns: []string{"id"}, After: map[string]interface{}{"id": int64(1)},
	}}}
	payloads := buildNativeEventPayloadsForLogical(event, 17)
	require.Len(t, payloads, 2)
	body := payloads[1][nativeEventHeaderLength:]
	require.Equal(t, uint16(len(extra)+2), binary.LittleEndian.Uint16(body[8:10]))
	require.Equal(t, extra, body[10:10+len(extra)])
}

func TestNativeRowImageSeparatesPresentAndNullBitmaps(t *testing.T) {
	event := BinlogEvent{Type: EventRow, GTID: GTID{UUID: "source", Seq: 1}, Changes: []RowChange{{
		Table: "app.docs", Action: "insert", Columns: []string{"id", "payload"},
		After: map[string]interface{}{"id": nil},
	}}}
	payloads := buildNativeEventPayloadsForLogical(event, 17)
	require.Len(t, payloads, 2)
	body := payloads[1][nativeEventHeaderLength : len(payloads[1])-nativeChecksumLength]
	// table-id(6), flags(2), extra-data(2), column-count(1),
	// columns-present bitmap(1), null bitmap(1); omitted payload is not NULL.
	require.Equal(t, byte(2), body[10])
	require.Equal(t, byte(0x01), body[11])
	require.Equal(t, byte(0x01), body[12])
	require.Len(t, body, 13)
}

func TestNativeRowMetadataUsesNegotiatedRawBytes(t *testing.T) {
	change := RowChange{ColumnMetadata: map[string][]byte{"opaque": {0x34, 0x12}}}
	metadata := nativeColumnMetadataForChange(change, "opaque", 254, "value")
	require.Equal(t, []byte{0x34, 0x12}, metadata)
	metadata[0] = 0
	require.Equal(t, []byte{0x34, 0x12}, change.ColumnMetadata["opaque"])
}

func TestNativeTableMapCarriesUnsignedOptionalMetadata(t *testing.T) {
	event := BinlogEvent{Type: EventRow, GTID: GTID{UUID: "source", Seq: 1}, Changes: []RowChange{{
		Table: "app.docs", Action: "insert", Columns: []string{"id", "amount", "label"},
		ColumnTypes: map[string]string{"id": "INT UNSIGNED", "amount": "DECIMAL(10,2)", "label": "VARCHAR(32)"},
		After:       map[string]interface{}{"id": uint64(7), "amount": "1.20", "label": "ok"},
	}}}
	payloads := buildNativeEventPayloadsForLogical(event, 17)
	require.Len(t, payloads, 2)
	body := payloads[0][nativeEventHeaderLength : len(payloads[0])-nativeChecksumLength]
	// table-id(6), flags(2), schema/table strings, column-count(1),
	// column types(3), metadata length + metadata, null bitmap(1).
	optionalOffset := 6 + 2 + (1 + len("app") + 1) + (1 + len("docs") + 1) + 1 + 3 + 1 + 4 + 1
	require.GreaterOrEqual(t, len(body), optionalOffset+3)
	optional := body[optionalOffset:]
	require.Equal(t, byte(1), optional[0])    // TABLE_MAP_OPT_META_SIGNEDNESS
	require.Equal(t, byte(1), optional[1])    // one byte for the numeric columns
	require.Equal(t, byte(0x80), optional[2]) // INT UNSIGNED is the first numeric column
	// Explicit schema order also emits COLUMN_NAME as a second optional TLV.
	require.Equal(t, byte(4), optional[3])
	require.Equal(t, byte(16), optional[4])
	require.Equal(t, []byte{2, 'i', 'd', 6, 'a', 'm', 'o', 'u', 'n', 't', 5, 'l', 'a', 'b', 'e', 'l'}, optional[5:])
}

func TestNativeTableMapCarriesEnumSetOptionalMetadata(t *testing.T) {
	event := BinlogEvent{Type: EventRow, GTID: GTID{UUID: "source", Seq: 1}, Changes: []RowChange{{
		Table: "app.docs", Action: "insert", Columns: []string{"state", "permissions"},
		ColumnTypes: map[string]string{
			"state":       "ENUM('draft','published')",
			"permissions": "SET('read','write')",
		},
		After: map[string]interface{}{"state": "draft", "permissions": "read,write"},
	}}}
	payloads := buildNativeEventPayloadsForLogical(event, 17)
	require.Len(t, payloads, 2)
	body := payloads[0][nativeEventHeaderLength : len(payloads[0])-nativeChecksumLength]
	optionalOffset := 6 + 2 + (1 + len("app") + 1) + (1 + len("docs") + 1) + 1 + 2 + 1 + 4 + 1
	optional := body[optionalOffset:]
	require.Contains(t, optional, byte(5))
	require.Contains(t, optional, byte(6))
	require.Contains(t, string(optional), "draft")
	require.Contains(t, string(optional), "published")
	require.Contains(t, string(optional), "read")
	require.Contains(t, string(optional), "write")
}

func TestNativeDecoderUsesFixedWidthForMySQLString(t *testing.T) {
	metadata := []byte{254, 4}
	require.Equal(t, 4, nativeFixedValueWidth(254, metadata))
	require.Equal(t, "AB", decodeNativeFixedValue(254, metadata, false, []byte("AB  ")))
	// ENUM/SET continue to use their packed numeric representation; the
	// real-type byte must not be decoded as text.
	require.Equal(t, 1, nativeFixedValueWidth(254, []byte{247, 1}))
	require.Equal(t, uint64(2), decodeNativeFixedValue(254, []byte{247, 1}, false, []byte{2}))
}

func TestNativeTableMapCarriesGeometryTypeOptionalMetadata(t *testing.T) {
	wkbPoint := []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	change := RowChange{
		Columns:     []string{"shape", "shape_with_srid"},
		ColumnTypes: map[string]string{"shape": "GEOMETRY", "shape_with_srid": "GEOMETRY"},
		After: map[string]interface{}{
			"shape":           wkbPoint,
			"shape_with_srid": append([]byte{0x2a, 0, 0, 0}, wkbPoint...),
		},
	}
	optional := nativeTableMapOptionalMetadata(change, change.Columns, []byte{255, 255})
	require.Equal(t, []byte{4, 22, 5, 's', 'h', 'a', 'p', 'e', 15, 's', 'h', 'a', 'p', 'e', '_', 'w', 'i', 't', 'h', '_', 's', 'r', 'i', 'd', 7, 2, 1, 1}, optional)
}

func TestNativeTableMapDecodesExtendedOptionalMetadata(t *testing.T) {
	charset := appendNativeLenencInt(nil, 45)
	charset = append(charset, appendNativeLenencInt(nil, 1)...)
	charset = append(charset, appendNativeLenencInt(nil, 224)...)
	charset = append(charset, appendNativeLenencInt(nil, 3)...)
	charset = append(charset, appendNativeLenencInt(nil, 46)...)
	columnCharset := appendNativeLenencInt(nil, 45)
	columnCharset = append(columnCharset, appendNativeLenencInt(nil, 224)...)
	geometry := appendNativeLenencInt(nil, 1)
	geometry = append(geometry, appendNativeLenencInt(nil, 2)...)
	vector := appendNativeLenencInt(nil, 128)
	primary := appendNativeLenencInt(nil, 0)
	primary = append(primary, appendNativeLenencInt(nil, 2)...)
	visibility := []byte{0xa0}

	body := nativeTableMapBodyForTest([]byte{1, 2, 3, 8}, []byte{1, 2, 3, 4, 5, 6}, []byte{0})
	for _, optional := range []struct {
		kind  byte
		value []byte
	}{
		{kind: 2, value: charset},
		{kind: 3, value: columnCharset},
		{kind: 7, value: geometry},
		{kind: 8, value: primary},
		{kind: 13, value: vector},
		{kind: 12, value: visibility},
	} {
		body = append(body, optional.kind)
		body = append(body, appendNativeLenencInt(nil, len(optional.value))...)
		body = append(body, optional.value...)
	}
	table, tableID, err := decodeNativeTableMap(body)
	require.NoError(t, err)
	require.Equal(t, uint64(0x060504030201), tableID)
	require.Equal(t, uint64(45), table.defaultCharsets.defaultCharset)
	require.Equal(t, [][2]uint64{{1, 224}, {3, 46}}, table.defaultCharsets.pairs)
	require.Equal(t, []uint64{45, 224}, table.columnCharsets)
	require.Equal(t, []uint64{1, 2}, table.geometryTypes)
	require.Equal(t, []nativeDecoderKeyPart{{column: 0}, {column: 2}}, table.primaryKey)
	require.Equal(t, []uint64{128}, table.vectorDimensions)
	require.Equal(t, []bool{true, false, true, false, false, false, false, false}, table.columnVisibility)
}

func TestNativeDecoderBindsStandardTableMapToLocalColumns(t *testing.T) {
	decoder := NewNativeBinlogDecoder()
	var resolvedDatabase, resolvedTable string
	var resolvedCount int
	decoder.SetTableMapResolver(func(database, table string, columnCount int) ([]string, error) {
		resolvedDatabase, resolvedTable, resolvedCount = database, table, columnCount
		return []string{"id", "payload"}, nil
	})
	body := nativeTableMapBodyForTest([]byte{3, 1}, []byte{1, 0, 0, 0, 0, 0}, []byte{0})
	raw := buildNativeEvent(19, body, BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17, Position: 4})
	_, err := decoder.Decode([]NativeBinlogEvent{{File: "binlog.000001", Type: 19, Raw: raw}})
	require.NoError(t, err)
	require.Equal(t, "app", resolvedDatabase)
	require.Equal(t, "docs", resolvedTable)
	require.Equal(t, 2, resolvedCount)
	require.Equal(t, []string{"id", "payload"}, decoder.tables[1].columns)
}

func nativeTableMapBodyForTest(types []byte, tableIDBytes []byte, nullBits []byte) []byte {
	body := append([]byte{}, tableIDBytes...)
	body = append(body, 0, 0)
	body = append(body, 3, 'a', 'p', 'p', 0)
	body = append(body, 4, 'd', 'o', 'c', 's', 0)
	body = append(body, byte(len(types)))
	body = append(body, types...)
	body = append(body, 0)
	body = append(body, nullBits...)
	return body
}

func TestNativeGeometryTypeForNamedAndWKBValues(t *testing.T) {
	require.Equal(t, uint64(1), mustNativeGeometryType(t, "POINT", nil))
	wkb := []byte{0, 0, 0, 0, 2}
	require.Equal(t, uint64(2), mustNativeGeometryType(t, "GEOMETRY", wkb))
}

func mustNativeGeometryType(t *testing.T, typeName string, value []byte) uint64 {
	t.Helper()
	if geometryType, ok := nativeGeometryTypeForName(typeName); ok {
		return geometryType
	}
	geometryType, ok := nativeGeometryTypeForValue(value)
	require.True(t, ok)
	return geometryType
}

func TestNativeRowValueUsesNegotiatedStringLengthWidth(t *testing.T) {
	change := RowChange{
		ColumnTypes: map[string]string{"payload": "BLOB", "name": "VARCHAR(300)"},
		ColumnMetadata: map[string][]byte{
			"payload": {2},
			"name":    {0x2c, 0x01},
		},
	}
	require.Equal(t, []byte{3, 0, 'a', 'b', 'c'}, nativeValueBytesForColumn(change, "payload", "abc"))
	require.Equal(t, []byte{3, 0, 'a', 'b', 'c'}, nativeValueBytesForColumn(change, "name", "abc"))
}

func TestNativeRowValueDerivesStringLengthWidthFromTypeHints(t *testing.T) {
	change := RowChange{ColumnTypes: map[string]string{"tiny": "TINYBLOB", "large": "VARCHAR(300)"}}
	require.Equal(t, append([]byte{3}, []byte("abc")...), nativeValueBytesForColumn(change, "tiny", "abc"))
	require.Equal(t, append([]byte{3, 0}, []byte("abc")...), nativeValueBytesForColumn(change, "large", "abc"))
}

func TestNativeRowBitValuesUseFixedWidthPacking(t *testing.T) {
	require.Equal(t, []byte{1}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{"b": "BIT(8)"}}, "b", uint64(1)))
	require.Equal(t, []byte{1, 1}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{"b": "BIT(9)"}}, "b", uint64(257)))
	// Values outside BIT(n)'s representable range stay on the conservative
	// legacy path instead of being silently truncated.
	require.NotEqual(t, []byte{1, 1}, nativeValueBytesForColumn(RowChange{ColumnTypes: map[string]string{"b": "BIT(9)"}}, "b", uint64(513)))
}

func TestNativeJSONRowValueUsesBinaryJSONEncoding(t *testing.T) {
	change := RowChange{ColumnTypes: map[string]string{"payload": "JSON"}}
	require.Equal(t, []byte{5, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00}, nativeValueBytesForColumn(change, "payload", "{}"))
	require.Equal(t, []byte{2, 0x00, 0x00, 0x00, 0x04, 0x01}, nativeValueBytesForColumn(change, "payload", "true"))
}

func TestNativeJSONRowValueEncodesObjectsAndArrays(t *testing.T) {
	change := RowChange{ColumnTypes: map[string]string{"payload": "JSON"}}
	for _, raw := range []string{`{"a":1}`, `[1,"x"]`} {
		binaryJSON := nativeJSONBinaryValue(raw)
		encoded := nativeValueBytesForColumn(change, "payload", raw)
		require.GreaterOrEqual(t, len(encoded), 4)
		require.Equal(t, len(binaryJSON), int(binary.LittleEndian.Uint32(encoded[:4])))
		require.Equal(t, binaryJSON, encoded[4:])
	}
}

func TestNativePartialJSONUpdateUsesMySQLDiffEncoding(t *testing.T) {
	change := RowChange{
		Action:      "update",
		ColumnTypes: map[string]string{"id": "INT", "payload": "JSON"},
		Before:      map[string]interface{}{"id": int64(1), "payload": `{"a":1,"b":2}`},
		After:       map[string]interface{}{"id": int64(1), "payload": `{"a":2,"b":2}`},
		PartialJSONUpdates: map[string][]JSONPartialUpdate{
			"payload": {{Operation: JSONPartialOperationReplace, Path: "$.a", Value: int64(2)}},
		},
	}

	partial, ok := nativePartialJSONValue(change, "payload")
	require.True(t, ok)
	require.GreaterOrEqual(t, len(partial), 4)
	require.Equal(t, len(partial)-4, int(binary.LittleEndian.Uint32(partial[:4])))
	require.Equal(t, byte(JSONPartialOperationReplace), partial[4])
	require.Equal(t, byte(3), partial[5])
	require.Equal(t, []byte("$.a"), partial[6:9])
	dataLength := int(partial[9])
	require.Equal(t, len(partial)-10, dataLength)
	require.Equal(t, nativeJSONBinaryValue(int64(2)), partial[10:])

	raw := buildNativeRowsEventForChange(BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 7}, 4, 9, change, []string{"id", "payload"})
	require.Equal(t, byte(39), raw[4], "partial JSON updates use PARTIAL_UPDATE_ROWS_EVENT")
}

func TestNativePartialJSONUpdateFallsBackWhenDiffIsLarger(t *testing.T) {
	change := RowChange{
		Action:      "update",
		ColumnTypes: map[string]string{"payload": "JSON"},
		Before:      map[string]interface{}{"payload": `{"a":1}`},
		After:       map[string]interface{}{"payload": `{"a":2}`},
		PartialJSONUpdates: map[string][]JSONPartialUpdate{
			"payload": {{Operation: JSONPartialOperationReplace, Path: "$................................................................................................................", Value: strings.Repeat("x", 256)}},
		},
	}

	partial, ok := nativePartialJSONValue(change, "payload")
	require.False(t, ok)
	require.Nil(t, partial)
	raw := buildNativeRowsEventForChange(BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 7}, 4, 9, change, []string{"payload"})
	require.Equal(t, byte(31), raw[4], "oversized diffs retain UPDATE_ROWS_EVENTv2")
}

func TestDeriveJSONPartialUpdatesFromRowImages(t *testing.T) {
	before := `{"profile":{"name":"old","age":1},"stale":true}`
	after := `{"profile":{"name":"new","age":1,"city":"Austin"}}`
	updates, ok := DeriveJSONPartialUpdates(before, after)
	if !ok {
		t.Fatal("DeriveJSONPartialUpdates() did not recognize valid JSON documents")
	}
	if len(updates) != 3 {
		t.Fatalf("DeriveJSONPartialUpdates() produced %d updates; want 3: %#v", len(updates), updates)
	}
	assertJSONPartialUpdate(t, updates, JSONPartialOperationReplace, "$.profile.name", "new")
	assertJSONPartialUpdate(t, updates, JSONPartialOperationInsert, "$.profile.city", "Austin")
	assertJSONPartialUpdate(t, updates, JSONPartialOperationRemove, "$.stale", nil)

	change := RowChange{
		ColumnTypes: map[string]string{"payload": "JSON"},
		Before:      map[string]interface{}{"payload": before},
		After:       map[string]interface{}{"payload": after},
	}
	if partial, ok := nativePartialJSONValue(change, "payload"); !ok || len(partial) == 0 {
		t.Fatalf("nativePartialJSONValue() did not derive a compact diff: %#v, %v", partial, ok)
	}
}

func TestApplyJSONPartialUpdatesReconstructsObject(t *testing.T) {
	before := `{"profile":{"name":"old","age":1},"stale":true}`
	updates, ok := DeriveJSONPartialUpdates(before, `{"profile":{"name":"new","age":1,"city":"Austin"}}`)
	require.True(t, ok)
	value, ok := ApplyJSONPartialUpdates(before, updates)
	require.True(t, ok)
	var got map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(value), &got))
	require.Equal(t, map[string]interface{}{"profile": map[string]interface{}{"name": "new", "age": float64(1), "city": "Austin"}}, got)
}

func TestNativeDecoderReconstructsTableMapAndRowImages(t *testing.T) {
	change := RowChange{
		Table:   "app.docs",
		Columns: []string{"id", "name", "active", "score", "code"},
		ColumnTypes: map[string]string{
			"id":     "INT UNSIGNED",
			"name":   "VARCHAR(32)",
			"active": "TINYINT",
			"score":  "DOUBLE",
			"code":   "CHAR(16)",
		},
		After: map[string]interface{}{"id": uint32(7), "name": "hello", "active": int8(1), "score": float64(2.5), "code": "A1"},
	}
	event := BinlogEvent{Timestamp: time.Unix(1, 0), Type: EventRow, ServerID: 7, GTID: GTID{UUID: "source", Seq: 1}, Changes: []RowChange{change}}
	payloads := buildNativeEventPayloadsForLogical(event, 4)
	native := make([]NativeBinlogEvent, 0, len(payloads))
	for _, payload := range payloads {
		native = append(native, NativeBinlogEvent{File: "binlog.000001", Type: payload[4], Raw: payload})
	}
	decoded, err := NewNativeBinlogDecoder().Decode(native)
	if err != nil {
		t.Fatalf("decode native row event: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("decoded changes = %d, want 1", len(decoded))
	}
	if got := decoded[0].Table; got != "app.docs" {
		t.Fatalf("decoded table = %q, want app.docs", got)
	}
	if got := decoded[0].Action; got != "insert" {
		t.Fatalf("decoded action = %q, want insert", got)
	}
	if got := decoded[0].After["id"]; got != uint64(7) {
		t.Fatalf("decoded id = %#v, want uint64(7)", got)
	}
	if got := decoded[0].After["name"]; got != "hello" {
		t.Fatalf("decoded name = %#v, want hello", got)
	}
	if got := decoded[0].After["active"]; got != int64(1) {
		t.Fatalf("decoded active = %#v, want int64(1)", got)
	}
	if got := decoded[0].After["score"]; got != 2.5 {
		t.Fatalf("decoded score = %#v, want 2.5", got)
	}
	if got := decoded[0].After["code"]; got != "A1" {
		t.Fatalf("decoded code = %#v, want A1", got)
	}
}

func TestNativeDecoderSchemaResolverPreservesBinaryFixedValues(t *testing.T) {
	change := RowChange{
		Table:       "app.docs",
		Columns:     []string{"code"},
		ColumnTypes: map[string]string{"code": "BINARY(4)"},
		After:       map[string]interface{}{"code": "A"},
	}
	event := BinlogEvent{Timestamp: time.Unix(1, 0), Type: EventRow, ServerID: 7, Changes: []RowChange{change}}
	payloads := buildNativeEventPayloadsForLogical(event, 4)
	native := make([]NativeBinlogEvent, 0, len(payloads))
	for _, payload := range payloads {
		native = append(native, NativeBinlogEvent{File: "binlog.000001", Type: payload[4], Raw: payload})
	}
	decoder := NewNativeBinlogDecoder()
	decoder.SetTableMapSchemaResolver(func(database, table string, columnCount int) ([]string, []string, error) {
		require.Equal(t, "app", database)
		require.Equal(t, "docs", table)
		require.Equal(t, 1, columnCount)
		return []string{"code"}, []string{"BINARY(4)"}, nil
	})
	decoded, err := decoder.Decode(native)
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Equal(t, "BINARY(4)", decoded[0].ColumnTypes["code"])
	require.Equal(t, []byte{'A', 0, 0, 0}, decoded[0].After["code"])
}

func TestNativeDecoderAcceptsLegacyV1RowEvents(t *testing.T) {
	change := RowChange{
		Table:       "app.docs",
		Columns:     []string{"id", "name"},
		ColumnTypes: map[string]string{"id": "INT", "name": "VARCHAR(32)"},
		After:       map[string]interface{}{"id": int32(7), "name": "legacy"},
	}
	event := BinlogEvent{Timestamp: time.Unix(1, 0), Type: EventRow, ServerID: 7, Position: 4, Changes: []RowChange{change}}
	payloads := buildNativeEventPayloadsForLogical(event, event.Position)
	require.Len(t, payloads, 2)
	v2Body := payloads[1][nativeEventHeaderLength : len(payloads[1])-nativeChecksumLength]
	require.GreaterOrEqual(t, len(v2Body), 10)
	extraLength := int(binary.LittleEndian.Uint16(v2Body[8:10]))
	require.GreaterOrEqual(t, extraLength, 2)
	require.LessOrEqual(t, 8+extraLength, len(v2Body))
	legacyBody := append(append([]byte(nil), v2Body[:8]...), v2Body[8+extraLength:]...)
	legacy := buildNativeEvent(23, legacyBody, eventWithNativePosition(event, event.Position))

	decoded, err := NewNativeBinlogDecoder().Decode([]NativeBinlogEvent{
		{File: "binlog.000001", Type: payloads[0][4], Raw: payloads[0]},
		{File: "binlog.000001", Type: 23, Raw: legacy},
	})
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Equal(t, "insert", decoded[0].Action)
	require.Equal(t, int64(7), decoded[0].After["id"])
	require.Equal(t, "legacy", decoded[0].After["name"])
}

func TestNativeDecoderReconstructsUpdateBeforeAndAfterImages(t *testing.T) {
	change := RowChange{
		Table:   "app.docs",
		Columns: []string{"id", "name"},
		ColumnTypes: map[string]string{
			"id":   "INT",
			"name": "VARCHAR(32)",
		},
		Before: map[string]interface{}{"id": int32(7), "name": "old"},
		After:  map[string]interface{}{"id": int32(7), "name": "new"},
		Action: "update",
	}
	event := BinlogEvent{Timestamp: time.Unix(1, 0), Type: EventRow, ServerID: 7, GTID: GTID{UUID: "source", Seq: 2}, Changes: []RowChange{change}}
	payloads := buildNativeEventPayloadsForLogical(event, 4)
	native := make([]NativeBinlogEvent, 0, len(payloads))
	for _, payload := range payloads {
		native = append(native, NativeBinlogEvent{File: "binlog.000001", Type: payload[4], Raw: payload})
	}
	decoded, err := NewNativeBinlogDecoder().Decode(native)
	if err != nil {
		t.Fatalf("decode native update: %v", err)
	}
	if len(decoded) != 1 || decoded[0].Before["name"] != "old" || decoded[0].After["name"] != "new" {
		t.Fatalf("decoded update = %#v, want old -> new", decoded)
	}
}

func TestNativeDecoderReconstructsPartialJSONUpdate(t *testing.T) {
	before := `{"profile":{"name":"old","age":1},"stale":true}`
	after := `{"profile":{"name":"new","age":1,"city":"Austin"}}`
	change := RowChange{
		Table:   "app.docs",
		Columns: []string{"id", "payload"},
		ColumnTypes: map[string]string{
			"id":      "INT",
			"payload": "JSON",
		},
		Before: map[string]interface{}{"id": int32(7), "payload": before},
		After:  map[string]interface{}{"id": int32(7), "payload": after},
		Action: "update",
	}
	event := BinlogEvent{Timestamp: time.Unix(1, 0), Type: EventRow, ServerID: 7, GTID: GTID{UUID: "source", Seq: 3}, Changes: []RowChange{change}}
	payloads := buildNativeEventPayloadsForLogical(event, 4)
	if len(payloads) != 2 || payloads[1][4] != 39 {
		t.Fatalf("native payloads = %d/type %d, want TABLE_MAP + PARTIAL_UPDATE_ROWS_EVENT", len(payloads), payloads[1][4])
	}
	native := make([]NativeBinlogEvent, 0, len(payloads))
	for _, payload := range payloads {
		native = append(native, NativeBinlogEvent{File: "binlog.000001", Type: payload[4], Raw: payload})
	}
	decoded, err := NewNativeBinlogDecoder().Decode(native)
	if err != nil {
		t.Fatalf("decode partial JSON update: %v", err)
	}
	if len(decoded) != 1 || len(decoded[0].PartialJSONUpdates["payload"]) == 0 {
		t.Fatalf("decoded partial JSON metadata = %#v", decoded)
	}
	var got map[string]interface{}
	if err := json.Unmarshal([]byte(decoded[0].After["payload"].(string)), &got); err != nil {
		t.Fatalf("decoded reconstructed JSON: %v", err)
	}
	require.Equal(t, map[string]interface{}{"profile": map[string]interface{}{"name": "new", "age": float64(1), "city": "Austin"}}, got)
}

func TestNativeDecoderReconstructsTemporalImages(t *testing.T) {
	instant := time.Date(2026, time.July, 15, 12, 34, 56, 123000000, time.UTC)
	change := RowChange{
		Table:   "app.events",
		Columns: []string{"day", "clock", "moment", "legacy_time"},
		ColumnTypes: map[string]string{
			"day":         "DATE",
			"clock":       "DATETIME2(3)",
			"moment":      "TIMESTAMP2(3)",
			"legacy_time": "TIME",
		},
		After: map[string]interface{}{"day": "2026-07-15", "clock": "2026-07-15 12:34:56.123", "moment": instant, "legacy_time": "-14:06:07"},
	}
	event := BinlogEvent{Timestamp: time.Unix(1, 0), Type: EventRow, ServerID: 7, GTID: GTID{UUID: "source", Seq: 4}, Changes: []RowChange{change}}
	payloads := buildNativeEventPayloadsForLogical(event, 4)
	native := make([]NativeBinlogEvent, 0, len(payloads))
	for _, payload := range payloads {
		native = append(native, NativeBinlogEvent{File: "binlog.000001", Type: payload[4], Raw: payload})
	}
	decoded, err := NewNativeBinlogDecoder().Decode(native)
	if err != nil {
		t.Fatalf("decode temporal row: %v", err)
	}
	require.Len(t, decoded, 1)
	require.Equal(t, "2026-07-15", decoded[0].After["day"])
	require.Equal(t, "2026-07-15 12:34:56.123", decoded[0].After["clock"])
	require.Equal(t, "2026-07-15 12:34:56.123", decoded[0].After["moment"])
	require.Equal(t, "-14:06:07", decoded[0].After["legacy_time"])
}

func TestNativeDecoderConsumesMySQLBoolType(t *testing.T) {
	trueValue, updates, next, err := decodeNativeValue(244, nil, false, false, []byte{1}, 0)
	require.NoError(t, err)
	require.Empty(t, updates)
	require.Equal(t, true, trueValue)
	require.Equal(t, 1, next)
	falseValue, _, _, err := decodeNativeValue(244, nil, false, false, []byte{0}, 0)
	require.NoError(t, err)
	require.Equal(t, false, falseValue)
}

func TestNativeDecoderConsumesPreviousGTIDs(t *testing.T) {
	uuid := "00112233-4455-6677-8899-aabbccddeeff"
	raw := buildNativePreviousGTIDsEvent(17, 4, GTIDSet{
		uuid: {1: {}, 2: {}, 4: {}},
	})
	decoder := NewNativeBinlogDecoder()
	decoded, err := decoder.DecodeTransactions([]NativeBinlogEvent{{
		File:     "binlog.000001",
		Position: 4,
		Type:     35,
		Raw:      raw,
	}})
	require.NoError(t, err)
	require.Empty(t, decoded)
	require.Equal(t, GTIDIntervals{
		uuid: {{Start: 1, End: 2}, {Start: 4, End: 4}},
	}, decoder.PreviousGTIDs())

	copyOfSet := decoder.PreviousGTIDs()
	copyOfSet[uuid][0].Start = 99
	require.Equal(t, uint64(1), decoder.PreviousGTIDs()[uuid][0].Start)
}

func TestNativeDecoderMapsPreviousGTIDsToConfiguredSourceName(t *testing.T) {
	raw := buildNativePreviousGTIDsEvent(17, 4, GTIDSet{"source": {7: {}}})
	decoder := NewNativeBinlogDecoderForSource("source")
	_, err := decoder.DecodeTransactions([]NativeBinlogEvent{{Type: 35, Raw: raw}})
	require.NoError(t, err)
	require.Contains(t, decoder.PreviousGTIDs(), "source")
	require.Equal(t, []GTIDInterval{{Start: 7, End: 7}}, decoder.PreviousGTIDs()["source"])
}

func TestNativeDecoderRejectsMalformedPreviousGTIDs(t *testing.T) {
	raw := buildNativePreviousGTIDsEvent(17, 4, GTIDSet{"source": {1: {}}})
	body := append([]byte(nil), raw[nativeEventHeaderLength:len(raw)-nativeChecksumLength]...)
	body[0] = 2
	malformed := buildNativeEvent(35, body, BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17, Position: 4})
	_, err := NewNativeBinlogDecoder().DecodeTransactions([]NativeBinlogEvent{{Type: 35, Raw: malformed}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "PREVIOUS_GTIDS_EVENT")
}

func TestNativeDecoderUnpacksTransactionPayloadEvent(t *testing.T) {
	gtid := GTID{UUID: "00112233-4455-6677-8899-aabbccddeeff", Seq: 11}
	change := RowChange{
		Table:       "app.docs",
		Columns:     []string{"id"},
		ColumnTypes: map[string]string{"id": "INT"},
		After:       map[string]interface{}{"id": int32(11)},
	}
	gtidFrame := buildNativeEventForLogical(BinlogEvent{Timestamp: time.Unix(1, 0), Type: EventBegin, ServerID: 17, GTID: gtid}, 4)
	rowFrames := buildNativeEventPayloadsForLogical(BinlogEvent{Timestamp: time.Unix(1, 0), Type: EventRow, ServerID: 17, GTID: gtid, Changes: []RowChange{change}}, uint64(len(gtidFrame)+4))
	xidFrame := buildNativeEventForLogical(BinlogEvent{Timestamp: time.Unix(1, 0), Type: EventCommit, ServerID: 17, GTID: gtid}, uint64(len(gtidFrame)+4+len(rowFrames[0])+len(rowFrames[1])))
	inner := make([]byte, 0)
	for _, frame := range append(rowFrames, xidFrame) {
		inner = append(inner, nativePayloadInnerFrame(frame)...)
	}
	payloadHeader := []byte{9, 0, 2, 1, 255, 1, 1, byte(len(inner)), 0}
	payload := buildNativeEvent(40, append(payloadHeader, inner...), eventWithNativePosition(BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17}, uint64(len(gtidFrame)+4)))

	decoded, err := NewNativeBinlogDecoder().DecodeTransactions([]NativeBinlogEvent{
		{File: "binlog.000001", Position: 4, Type: gtidFrame[4], Raw: gtidFrame},
		{File: "binlog.000001", Position: uint64(len(gtidFrame) + 4), Type: 40, Raw: payload},
	})
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Equal(t, gtid, decoded[0].GTID)
	require.Equal(t, int64(11), decoded[0].Changes[0].After["id"])
}

func TestNativeDecoderUnpacksZSTransactionPayloadEvent(t *testing.T) {
	queryBody := append(make([]byte, 13), 0)
	queryBody = append(queryBody, []byte("select 1")...)
	query := buildNativeEvent(2, queryBody, BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17, Position: 4})
	xid := buildNativeEvent(16, make([]byte, 8), BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17, Position: uint64(len(query))})
	inner := append(nativePayloadInnerFrame(query), nativePayloadInnerFrame(xid)...)
	encoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	compressed := encoder.EncodeAll(inner, nil)
	require.NoError(t, encoder.Close())
	payloadHeader := []byte{2, 1, 0, 1, 1, byte(len(compressed)), 3, 1, byte(len(inner)), 0}
	payload := buildNativeEvent(40, append(payloadHeader, compressed...), BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17, Position: 4})

	decoded, err := NewNativeBinlogDecoder().DecodeTransactions([]NativeBinlogEvent{{Type: 40, Raw: payload}})
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Equal(t, "select 1", decoded[0].Statements[0].SQL)
}

func TestNativeDecoderSkipsKnownControlEventsInsideTransactionPayload(t *testing.T) {
	queryBody := append(make([]byte, 13), 0)
	queryBody = append(queryBody, []byte("insert into docs values (1)")...)
	query := buildNativeEvent(2, queryBody, BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17, Position: 4})
	rowsQuery := buildNativeEvent(29, []byte("insert into docs values (1)"), BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17, Position: uint64(len(query))})
	incident := buildNativeEvent(26, []byte{1, 2, 3}, BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17, Position: uint64(len(query) + len(rowsQuery))})
	heartbeat := buildNativeEvent(27, nil, BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17, Position: uint64(len(query) + len(rowsQuery) + len(incident))})
	context := buildNativeEvent(36, []byte{4, 5}, BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17, Position: uint64(len(query) + len(rowsQuery) + len(incident) + len(heartbeat))})
	xid := buildNativeEvent(16, make([]byte, 8), BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17, Position: uint64(len(query) + len(rowsQuery) + len(incident) + len(heartbeat) + len(context))})
	inner := make([]byte, 0)
	for _, frame := range [][]byte{query, rowsQuery, incident, heartbeat, context, xid} {
		inner = append(inner, nativePayloadInnerFrame(frame)...)
	}
	payloadHeader := []byte{9, 0, 2, 1, 255, 1, 1, byte(len(inner)), 0}
	payload := buildNativeEvent(40, append(payloadHeader, inner...), BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17, Position: 4})

	decoded, err := NewNativeBinlogDecoder().DecodeTransactions([]NativeBinlogEvent{{Type: 40, Raw: payload}})
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Equal(t, "insert into docs values (1)", decoded[0].Statements[0].SQL)
}

func TestNativeDecoderConsumesKnownTopLevelControlEvents(t *testing.T) {
	base := BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17}
	controlTypes := []byte{1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 20, 21, 22, 26, 27, 28, 29, 36, 37, 41}
	frames := make([]NativeBinlogEvent, 0, len(controlTypes))
	for index, typeCode := range controlTypes {
		raw := buildNativeEvent(typeCode, nil, eventWithNativePosition(base, uint64(4+index*32)))
		frames = append(frames, NativeBinlogEvent{File: "binlog.000001", Position: uint64(4 + index*32), Type: typeCode, Raw: raw})
	}
	decoded, err := NewNativeBinlogDecoder().DecodeTransactions(frames)
	require.NoError(t, err)
	require.Empty(t, decoded)
}

func TestNativeDecoderRejectsUnknownTopLevelControlEvent(t *testing.T) {
	raw := buildNativeEvent(250, nil, BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17, Position: 4})
	_, err := NewNativeBinlogDecoder().DecodeTransactions([]NativeBinlogEvent{{Type: 250, Raw: raw}})
	require.ErrorContains(t, err, "unsupported native event 250")
}

func TestNativeEventTypeNamesCoverUpstreamEventCodes(t *testing.T) {
	want := map[byte]string{
		1: "Start_v3", 2: "Query", 3: "Stop", 4: "Rotate", 5: "Intvar", 6: "Load", 7: "Slave",
		8: "Create_file", 9: "Append_block", 10: "Exec_load", 11: "Delete_file", 12: "New_load",
		13: "Rand", 14: "User_var", 15: "Format_desc", 16: "Xid", 17: "Begin_load_query",
		18: "Execute_load_query", 19: "Table_map", 20: "Pre_gwrite_rows", 21: "Pre_gupdate_rows",
		22: "Pre_gdelete_rows", 23: "Write_rows", 24: "Update_rows", 25: "Delete_rows", 26: "Incident",
		27: "Heartbeat", 28: "Ignorable", 29: "Rows_query", 30: "Write_rows", 31: "Update_rows",
		32: "Delete_rows", 33: "Gtid", 34: "Anonymous_Gtid", 35: "Previous_gtids", 36: "Transaction_context",
		37: "View_change", 38: "Xa_prepare", 39: "Partial_update_rows", 40: "Transaction_payload",
		41: "Heartbeat_log_event_v2", 42: "Gtid_tagged",
	}
	for typeCode, name := range want {
		require.Equal(t, name, nativeEventTypeName(typeCode), typeCode)
	}
	require.Empty(t, nativeEventTypeName(250))
}

func TestNativeDecoderHandlesOnePhaseXAPrepare(t *testing.T) {
	gtid := GTID{UUID: "00112233-4455-6677-8899-aabbccddeeff", Seq: 12}
	gtidFrame := buildNativeEventForLogical(BinlogEvent{Timestamp: time.Unix(1, 0), Type: EventBegin, ServerID: 17, GTID: gtid}, 4)
	xaBody := make([]byte, 13)
	xaBody[0] = 1
	binary.LittleEndian.PutUint32(xaBody[1:5], 7)
	binary.LittleEndian.PutUint32(xaBody[5:9], 2)
	binary.LittleEndian.PutUint32(xaBody[9:13], 3)
	xaBody = append(xaBody, []byte("gbqua")...)
	xa := buildNativeEvent(38, xaBody, BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17, Position: uint64(len(gtidFrame))})
	decoded, err := NewNativeBinlogDecoder().DecodeTransactions([]NativeBinlogEvent{
		{Type: 33, Raw: gtidFrame},
		{Type: 38, Raw: xa},
	})
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Equal(t, gtid, decoded[0].GTID)
}

func TestNativeDecoderRecoversPreparedXAOnCommit(t *testing.T) {
	gtidFrame := buildNativeEventForLogical(BinlogEvent{Timestamp: time.Unix(1, 0), Type: EventBegin, ServerID: 17, GTID: GTID{UUID: "source", Seq: 13}}, 4)
	xaBody := make([]byte, 13)
	binary.LittleEndian.PutUint32(xaBody[5:9], 1)
	xaBody = append(xaBody, byte('x'))
	xa := buildNativeEvent(38, xaBody, BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17, Position: uint64(len(gtidFrame))})
	commit := buildNativeQueryEventForTest(BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17}, "XA COMMIT 'x','',0")
	decoded, err := NewNativeBinlogDecoderForSource("source").DecodeTransactions([]NativeBinlogEvent{{Type: 33, Raw: gtidFrame}, {Type: 38, Raw: xa}, {Type: 2, Position: uint64(len(gtidFrame) + len(xa)), Raw: commit}})
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Equal(t, GTID{UUID: "source", Seq: 13}, decoded[0].GTID)
}

func TestNativeDecoderCarriesPreparedXAAcrossDumpCalls(t *testing.T) {
	gtid := GTID{UUID: "00112233-4455-6677-8899-aabbccddeeff", Seq: 14}
	base := BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17}
	gtidFrame := buildNativeEventForLogical(BinlogEvent{Timestamp: base.Timestamp, Type: EventBegin, ServerID: base.ServerID, GTID: gtid}, 4)
	xaBody := make([]byte, 13)
	binary.LittleEndian.PutUint32(xaBody[5:9], 1)
	xaBody = append(xaBody, byte('x'))
	xa := buildNativeEvent(38, xaBody, eventWithNativePosition(base, uint64(4+len(gtidFrame))))
	decoder := NewNativeBinlogDecoder()
	prepared, err := decoder.DecodeTransactions([]NativeBinlogEvent{
		{File: "binlog.000001", Position: 4, Type: 33, Raw: gtidFrame},
		{File: "binlog.000001", Position: uint64(4 + len(gtidFrame)), Type: 38, Raw: xa},
	})
	require.NoError(t, err)
	require.Empty(t, prepared)
	require.Len(t, decoder.PreparedXA(), 1)

	commitPosition := uint64(4 + len(gtidFrame) + len(xa))
	commit := buildNativeQueryEventForTest(eventWithNativePosition(base, commitPosition), "XA COMMIT 'x','',0")
	committed, err := decoder.DecodeTransactions([]NativeBinlogEvent{{File: "binlog.000001", Position: commitPosition, Type: 2, Raw: commit}})
	require.NoError(t, err)
	require.Len(t, committed, 1)
	require.Equal(t, gtid, committed[0].GTID)
	require.Empty(t, decoder.PreparedXA())
}

func TestReplicaPersistsPreparedXAAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	gtid := GTID{UUID: "00112233-4455-6677-8899-aabbccddeeff", Seq: 15}
	base := BinlogEvent{Timestamp: time.Unix(1, 0), ServerID: 17}
	gtidFrame := buildNativeEventForLogical(BinlogEvent{Timestamp: base.Timestamp, Type: EventBegin, ServerID: base.ServerID, GTID: gtid}, 4)
	xaBody := make([]byte, 13)
	binary.LittleEndian.PutUint32(xaBody[5:9], 1)
	xaBody = append(xaBody, byte('x'))
	xaPosition := uint64(4 + len(gtidFrame))
	xa := buildNativeEvent(38, xaBody, eventWithNativePosition(base, xaPosition))

	replica, err := NewReplica(dir)
	require.NoError(t, err)
	require.NoError(t, replica.ApplyNative([]NativeBinlogEvent{
		{File: "binlog.000001", Position: 4, Type: 33, Raw: gtidFrame},
		{File: "binlog.000001", Position: xaPosition, Type: 38, Raw: xa},
	}))
	require.Empty(t, replica.Executed)

	reloaded, err := NewReplica(dir)
	require.NoError(t, err)
	commitPosition := xaPosition + uint64(len(xa))
	commit := buildNativeQueryEventForTest(eventWithNativePosition(base, commitPosition), "XA COMMIT 'x','',0")
	require.NoError(t, reloaded.ApplyNative([]NativeBinlogEvent{{File: "binlog.000001", Position: commitPosition, Type: 2, Raw: commit}}))
	require.True(t, reloaded.Executed.Contains(gtid))
	require.Empty(t, reloaded.preparedXA)
}

func TestNativeDecoderReconstructsStatementBasedQueryBoundaries(t *testing.T) {
	base := BinlogEvent{Timestamp: time.Unix(3, 0), ServerID: 17}
	begin := buildNativeQueryEventForTest(base, "BEGIN")
	insert := buildNativeQueryEventForTest(base, "insert into docs values (1)")
	commit := buildNativeQueryEventForTest(base, "COMMIT")
	decoded, err := NewNativeBinlogDecoder().DecodeTransactions([]NativeBinlogEvent{
		{File: "binlog.000001", Position: 4, Type: 2, Raw: begin},
		{File: "binlog.000001", Position: uint64(4 + len(begin)), Type: 2, Raw: insert},
		{File: "binlog.000001", Position: uint64(4 + len(begin) + len(insert)), Type: 2, Raw: commit},
	})
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Equal(t, "insert into docs values (1)", decoded[0].Statements[0].SQL)
}

func TestNativeDecoderDropsRolledBackStatementTransaction(t *testing.T) {
	base := BinlogEvent{Timestamp: time.Unix(3, 0), ServerID: 17}
	frames := []NativeBinlogEvent{}
	position := uint64(4)
	for _, statement := range []string{"START TRANSACTION", "insert into docs values (1)", "ROLLBACK"} {
		raw := buildNativeQueryEventForTest(eventWithNativePosition(base, position), statement)
		frames = append(frames, NativeBinlogEvent{File: "binlog.000001", Position: position, Type: 2, Raw: raw})
		position += uint64(len(raw))
	}
	decoded, err := NewNativeBinlogDecoder().DecodeTransactions(frames)
	require.NoError(t, err)
	require.Empty(t, decoded)
}

func buildNativeQueryEventForTest(event BinlogEvent, statement string) []byte {
	body := append(make([]byte, 13), 0)
	body = append(body, []byte(statement)...)
	return buildNativeEvent(2, body, event)
}

func nativePayloadInnerFrame(frame []byte) []byte {
	inner := append([]byte(nil), frame[:len(frame)-nativeChecksumLength]...)
	binary.LittleEndian.PutUint32(inner[9:13], uint32(len(inner)))
	return inner
}

func TestNativeDecoderReconstructsAnonymousTransactions(t *testing.T) {
	base := BinlogEvent{Timestamp: time.Unix(2, 0), ServerID: 17}
	anonymous := buildNativeEvent(34, []byte{0}, eventWithNativePosition(base, 128))
	xid := buildNativeEvent(16, make([]byte, 8), eventWithNativePosition(base, 160))
	decoded, err := NewNativeBinlogDecoder().DecodeTransactions([]NativeBinlogEvent{
		{File: "binlog.000007", Position: 128, Type: 34, Raw: anonymous},
		{File: "binlog.000007", Position: 160, Type: 16, Raw: xid},
	})
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Equal(t, EventCommit, decoded[0].Type)
	require.Equal(t, uint64(128), decoded[0].GTID.Seq)
	require.Equal(t, "anonymous:binlog.000007", decoded[0].GTID.UUID)
}

func TestNativeDecoderReconstructsTaggedGTIDTransaction(t *testing.T) {
	base := BinlogEvent{Timestamp: time.Unix(2, 0), ServerID: 17}
	sid := []byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}
	// MySQL's serialization archive uses the 1-9 byte variable integer
	// encoding.  This is a compact tagged GTID with fields 0..6, 8 and 9;
	// optional fields 7, 10 and 11 are absent.
	body := []byte{0x02, 0x48, 0x00, 0x00, 0x02, 0x02}
	body = append(body, sid...)
	body = append(body, 0x04, 0x0c, 0x06, 0x06, 'f', 'o', 'o', 0x08, 0x00, 0x0a, 0x02, 0x0c, 0x00, 0x10, 0x00, 0x12, 0x00)
	tagged := buildNativeEvent(42, body, eventWithNativePosition(base, 128))
	xid := buildNativeEvent(16, make([]byte, 8), eventWithNativePosition(base, 160))
	decoded, err := NewNativeBinlogDecoder().DecodeTransactions([]NativeBinlogEvent{
		{File: "binlog.000007", Position: 128, Type: 42, Raw: tagged},
		{File: "binlog.000007", Position: 160, Type: 16, Raw: xid},
	})
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Equal(t, EventCommit, decoded[0].Type)
	require.Equal(t, uint64(3), decoded[0].GTID.Seq)
	require.Equal(t, "00112233-4455-6677-8899-aabbccddeeff:foo", decoded[0].GTID.UUID)
}

func TestNativeDecoderRejectsMalformedTaggedGTID(t *testing.T) {
	frame := buildNativeEvent(42, []byte{0x02, 0x02}, BinlogEvent{Timestamp: time.Unix(2, 0), ServerID: 17, Position: 4})
	_, err := NewNativeBinlogDecoder().DecodeTransactions([]NativeBinlogEvent{{Type: 42, Raw: frame}})
	require.ErrorContains(t, err, "GTID_TAGGED_LOG_EVENT")
}

func TestNativeDecoderConsumesMySQLNullColumnType(t *testing.T) {
	table := nativeDecoderTable{types: []byte{6}, unsigned: []bool{false}, metadata: [][]byte{nil}, columns: []string{"n"}}
	require.Equal(t, "NULL", nativeDecoderColumnType(table, 0))
	value, updates, next, err := decodeNativeValue(6, nil, false, false, []byte{0xaa}, 0)
	require.NoError(t, err)
	require.Nil(t, value)
	require.Empty(t, updates)
	require.Zero(t, next, "MYSQL_TYPE_NULL has no row-image bytes")
}

func TestNativeDumpFiltersExecutedTaggedGTIDTransaction(t *testing.T) {
	source, err := NewSource(t.TempDir(), "tagged-source", 17)
	require.NoError(t, err)
	sid := nativeGTIDSID("tagged-source")
	body := []byte{0x02, 0x48, 0x00, 0x00, 0x02, 0x02}
	body = append(body, sid...)
	body = append(body, 0x04, 0x0c, 0x06, 0x06, 'f', 'o', 'o', 0x08, 0x00, 0x0a, 0x02, 0x0c, 0x00, 0x10, 0x00, 0x12, 0x00)
	base := BinlogEvent{Timestamp: time.Unix(2, 0), ServerID: 17}
	tagged := buildNativeEvent(42, body, eventWithNativePosition(base, 4))
	xid := buildNativeEvent(16, make([]byte, 8), eventWithNativePosition(base, uint64(4+len(tagged))))
	nativePath := filepath.Join(filepath.Dir(source.Writer.path), "binlog.000001")
	require.NoError(t, os.WriteFile(nativePath, append(append([]byte{nativeBinlogMagic, 'b', 'i', 'n'}, tagged...), xid...), 0644))

	executed := GTIDIntervals{}
	executed.Add("tagged-source:foo", 3)
	events, _, err := source.NativeDumpFileWithIntervals("binlog.000001", 4, executed)
	require.NoError(t, err)
	require.Empty(t, events)
}

func TestNativeGTIDIndexPersistsTaggedGTIDIdentity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "binlog.jsonl")
	format := buildNativeFormatDescriptionEvent(17, 4)
	previous := buildNativePreviousGTIDsEvent(17, uint64(4+len(format)), GTIDSet{})
	sid := nativeGTIDSID("tagged-source")
	body := []byte{0x02, 0x48, 0x00, 0x00, 0x02, 0x02}
	body = append(body, sid...)
	body = append(body, 0x04, 0x0c, 0x06, 0x06, 'f', 'o', 'o', 0x08, 0x00, 0x0a, 0x02, 0x0c, 0x00, 0x10, 0x00, 0x12, 0x00)
	tagged := buildNativeEvent(42, body, BinlogEvent{Timestamp: time.Unix(2, 0), ServerID: 17, Position: uint64(4 + len(format) + len(previous))})
	xid := buildNativeEvent(16, make([]byte, 8), BinlogEvent{Timestamp: time.Unix(2, 0), ServerID: 17, Position: uint64(4 + len(format) + len(previous) + len(tagged))})
	require.NoError(t, os.WriteFile(filepath.Join(dir, "binlog.000001"), append(append(append([]byte{nativeBinlogMagic, 'b', 'i', 'n'}, format...), previous...), append(tagged, xid...)...), 0644))
	writer := &BinlogWriter{
		path:                path,
		nativePath:          filepath.Join(dir, "binlog.000001"),
		nativeGTIDIndexPath: filepath.Join(dir, "binlog.gtid.index"),
		nativeIndex:         1,
		nativeGTIDIndex:     make(map[string]NativeGTIDIndexEntry),
	}
	require.NoError(t, writer.rebuildNativeGTIDIndexLocked())
	entry, ok := writer.NativeGTIDPositionTagged("tagged-source", "foo", 3)
	require.True(t, ok)
	require.Equal(t, "foo", entry.Tag)
	require.Equal(t, "binlog.000001", entry.File)
	require.Equal(t, uint64(4+len(format)+len(previous)), entry.Position)

	reopened := &BinlogWriter{
		path:                path,
		nativePath:          filepath.Join(dir, "binlog.000001"),
		nativeGTIDIndexPath: filepath.Join(dir, "binlog.gtid.index"),
		nativeIndex:         1,
		nativeGTIDIndex:     make(map[string]NativeGTIDIndexEntry),
	}
	require.NoError(t, reopened.rebuildNativeGTIDIndexLocked())
	entry, ok = reopened.NativeGTIDPositionTagged("tagged-source", "foo", 3)
	require.True(t, ok)
	require.Equal(t, entry.Position, uint64(4+len(format)+len(previous)))
}

func TestNativeDecoderReconstructsDecimalImages(t *testing.T) {
	change := RowChange{
		Table:   "app.amounts",
		Columns: []string{"amount", "negative"},
		ColumnTypes: map[string]string{
			"amount":   "DECIMAL(12,4)",
			"negative": "DECIMAL(8,2)",
		},
		After: map[string]interface{}{"amount": "12345678.9012", "negative": "-12.34"},
	}
	event := BinlogEvent{Timestamp: time.Unix(1, 0), Type: EventRow, ServerID: 7, GTID: GTID{UUID: "source", Seq: 5}, Changes: []RowChange{change}}
	payloads := buildNativeEventPayloadsForLogical(event, 4)
	native := make([]NativeBinlogEvent, 0, len(payloads))
	for _, payload := range payloads {
		native = append(native, NativeBinlogEvent{File: "binlog.000001", Type: payload[4], Raw: payload})
	}
	decoded, err := NewNativeBinlogDecoder().Decode(native)
	if err != nil {
		t.Fatalf("decode decimal row: %v", err)
	}
	require.Len(t, decoded, 1)
	require.Equal(t, "12345678.9012", decoded[0].After["amount"])
	require.Equal(t, "-12.34", decoded[0].After["negative"])
}

func TestNativeDecoderAcceptsLegacyDecimalTypeCode(t *testing.T) {
	change := RowChange{
		Table:       "app.amounts",
		Columns:     []string{"amount"},
		ColumnTypes: map[string]string{"amount": "DECIMAL(12,4)"},
		After:       map[string]interface{}{"amount": "12345678.9012"},
	}
	event := BinlogEvent{Timestamp: time.Unix(1, 0), Type: EventRow, ServerID: 7, Position: 4, Changes: []RowChange{change}}
	payloads := buildNativeEventPayloadsForLogical(event, event.Position)
	require.Len(t, payloads, 2)
	tableBody := append([]byte(nil), payloads[0][nativeEventHeaderLength:len(payloads[0])-nativeChecksumLength]...)
	legacyType := bytes.IndexByte(tableBody, 246)
	require.GreaterOrEqual(t, legacyType, 0)
	tableBody[legacyType] = 0
	legacyTable := buildNativeEvent(19, tableBody, eventWithNativePosition(event, event.Position))

	decoded, err := NewNativeBinlogDecoder().Decode([]NativeBinlogEvent{
		{File: "binlog.000001", Type: 19, Raw: legacyTable},
		{File: "binlog.000001", Type: payloads[1][4], Raw: payloads[1]},
	})
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Equal(t, "12345678.9012", decoded[0].After["amount"])
}

func TestNativeDecoderReconstructsEnumAndSetImages(t *testing.T) {
	change := RowChange{
		Table:   "app.states",
		Columns: []string{"state", "features"},
		ColumnTypes: map[string]string{
			"state":    "ENUM('draft','published')",
			"features": "SET('read','write','admin')",
		},
		After: map[string]interface{}{"state": "published", "features": "read,admin"},
	}
	event := BinlogEvent{Timestamp: time.Unix(1, 0), Type: EventRow, ServerID: 7, GTID: GTID{UUID: "source", Seq: 6}, Changes: []RowChange{change}}
	payloads := buildNativeEventPayloadsForLogical(event, 4)
	native := make([]NativeBinlogEvent, 0, len(payloads))
	for _, payload := range payloads {
		native = append(native, NativeBinlogEvent{File: "binlog.000001", Type: payload[4], Raw: payload})
	}
	decoded, err := NewNativeBinlogDecoder().Decode(native)
	if err != nil {
		t.Fatalf("decode enum/set row: %v", err)
	}
	require.Len(t, decoded, 1)
	require.Equal(t, "published", decoded[0].After["state"])
	require.Equal(t, "read,admin", decoded[0].After["features"])
}

func TestNativeDecoderReconstructsGTIDTransaction(t *testing.T) {
	gtid := GTID{UUID: "00112233-4455-6677-8899-aabbccddeeff", Seq: 9}
	change := RowChange{Table: "app.docs", Columns: []string{"id"}, ColumnTypes: map[string]string{"id": "INT"}, After: map[string]interface{}{"id": int32(9)}}
	begin := buildNativeEventForLogical(BinlogEvent{Timestamp: time.Unix(1, 0), Type: EventBegin, ServerID: 7, GTID: gtid}, 4)
	rowPayloads := buildNativeEventPayloadsForLogical(BinlogEvent{Timestamp: time.Unix(1, 0), Type: EventRow, ServerID: 7, GTID: gtid, Changes: []RowChange{change}}, uint64(len(begin)+4))
	commit := buildNativeEventForLogical(BinlogEvent{Timestamp: time.Unix(1, 0), Type: EventCommit, ServerID: 7, GTID: gtid}, uint64(len(begin)+4+len(rowPayloads[0])+len(rowPayloads[1])))
	native := make([]NativeBinlogEvent, 0, 4)
	native = append(native, NativeBinlogEvent{File: "binlog.000001", Type: begin[4], Raw: begin})
	for _, payload := range rowPayloads {
		native = append(native, NativeBinlogEvent{File: "binlog.000001", Type: payload[4], Raw: payload})
	}
	native = append(native, NativeBinlogEvent{File: "binlog.000001", Type: commit[4], Raw: commit})
	transactions, err := DecodeNativeBinlogTransactions(native)
	if err != nil {
		t.Fatalf("decode native transaction: %v", err)
	}
	require.Len(t, transactions, 1)
	require.Equal(t, uint64(9), transactions[0].GTID.Seq)
	require.Len(t, transactions[0].Changes, 1)
	require.Equal(t, int64(9), transactions[0].Changes[0].After["id"])
	replica := &Replica{Executed: GTIDSet{}, statePath: filepath.Join(t.TempDir(), "replica_gtid.json")}
	applied := 0
	replica.ApplyRows = func(changes []RowChange) error {
		applied += len(changes)
		return nil
	}
	if err := replica.ApplyNative(native); err != nil {
		t.Fatalf("apply native transaction: %v", err)
	}
	require.Equal(t, 1, applied)
	require.True(t, replica.Executed.Contains(gtid))
}

func TestSourceNativeDecodePreservesNonUUIDSourceIdentity(t *testing.T) {
	source, err := NewSource(t.TempDir(), "source", 7)
	require.NoError(t, err)
	_, err = source.Append(1, []RowChange{{
		Table:  "app.docs",
		Action: "insert",
		After:  map[string]interface{}{"id": int32(1)},
	}})
	require.NoError(t, err)

	transactions, err := source.DecodeNativeDumpFrom("binlog.000001", 4, nil)
	require.NoError(t, err)
	require.Len(t, transactions, 1)
	require.Equal(t, "source", transactions[0].GTID.UUID)
	require.Equal(t, uint64(1), transactions[0].GTID.Seq)
}

func TestReplicaReplicateNativeFromIsGTIDIdempotent(t *testing.T) {
	source, err := NewSource(t.TempDir(), "source", 7)
	require.NoError(t, err)
	_, err = source.Append(1, []RowChange{{
		Table:       "app.docs",
		Action:      "insert",
		ColumnTypes: map[string]string{"id": "INT"},
		After:       map[string]interface{}{"id": int32(1)},
	}})
	require.NoError(t, err)
	replica, err := NewReplica(t.TempDir())
	require.NoError(t, err)
	applied := 0
	replica.ApplyRows = func(changes []RowChange) error {
		applied += len(changes)
		return nil
	}

	position, err := replica.ReplicateNativeFrom(source, "binlog.000001", 4)
	require.NoError(t, err)
	require.Greater(t, position, uint64(4))
	require.Equal(t, 1, applied)
	require.True(t, replica.Executed.Contains(GTID{UUID: "source", Seq: 1}))

	_, err = replica.ReplicateNativeFrom(source, "binlog.000001", position)
	require.NoError(t, err)
	require.Equal(t, 1, applied, "replaying from the observed native position must not reapply the GTID")
}

func TestNativeDumpFiltersExecutedAnonymousTransaction(t *testing.T) {
	source, err := NewSource(t.TempDir(), "anonymous-source", 7)
	require.NoError(t, err)
	base := BinlogEvent{Timestamp: time.Unix(2, 0), ServerID: 7}
	anonymous := buildNativeEvent(34, []byte{0}, eventWithNativePosition(base, 4))
	xid := buildNativeEvent(16, make([]byte, 8), eventWithNativePosition(base, uint64(4+len(anonymous))))
	nativePath := filepath.Join(filepath.Dir(source.Writer.path), "binlog.000001")
	native := append([]byte{nativeBinlogMagic, 'b', 'i', 'n'}, anonymous...)
	native = append(native, xid...)
	require.NoError(t, os.WriteFile(nativePath, native, 0644))

	executed := GTIDIntervals{}
	executed.Add("anonymous:binlog.000001", 4)
	events, _, err := source.NativeDumpFileWithIntervals("binlog.000001", 4, executed)
	require.NoError(t, err)
	require.Empty(t, events)
}

func TestNativeDumpDoesNotStartInsideAnExistingTransaction(t *testing.T) {
	source, err := NewSource(t.TempDir(), "partial-start-source", 7)
	require.NoError(t, err)
	for sequence := uint64(1); sequence <= 2; sequence++ {
		_, err = source.AppendTransaction(sequence, []RowChange{{
			Table:  "app.docs",
			Action: "insert",
			After:  map[string]interface{}{"id": int64(sequence)},
		}}, nil)
		require.NoError(t, err)
	}
	native, err := source.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	require.Len(t, native, 10)
	start := native[4].Position
	events, _, err := source.NativeDumpFileWithIntervals("binlog.000001", start, nil)
	require.NoError(t, err)
	require.Len(t, events, 4, "a dump that starts inside transaction 1 must resume at transaction 2")
	require.Equal(t, byte(33), events[0].Type)
	require.Equal(t, uint64(2), mustNativeGTIDSequence(t, events[0]))
}

func mustNativeGTIDSequence(t *testing.T, event NativeBinlogEvent) uint64 {
	t.Helper()
	sequence, ok := nativeGTIDSequenceBody(nativeEventBody(event))
	require.True(t, ok)
	return sequence
}

func assertJSONPartialUpdate(t *testing.T, updates []JSONPartialUpdate, operation byte, path string, value interface{}) {
	t.Helper()
	for _, update := range updates {
		if update.Operation == operation && update.Path == path && (value == nil || update.Value == value) {
			return
		}
	}
	t.Fatalf("missing JSON partial update operation=%d path=%s value=%v in %#v", operation, path, value, updates)
}

func TestNativeJSONObjectKeysUseMySQLLengthThenLexicographicOrder(t *testing.T) {
	raw := nativeJSONBinaryValue(map[string]interface{}{"bb": 1, "a": 2, "aa": 3})
	// The small-object header is 25 bytes after the type byte; key bytes follow
	// it and are ordered by byte length, then lexicographically.
	require.Equal(t, []byte("aaabb"), raw[1+25:1+25+5])
}

func TestNativeDecimalRowValueUsesPackedDecimal(t *testing.T) {
	change := RowChange{ColumnTypes: map[string]string{"amount": "DECIMAL(10,2)"}}
	require.Equal(t, []byte{0x80, 0x00, 0x00, 0x7b, 0x2d}, nativeValueBytesForColumn(change, "amount", "123.45"))
	require.Equal(t, []byte{0x7f, 0xff, 0xff, 0x84, 0xd2}, nativeValueBytesForColumn(change, "amount", "-123.45"))
}

func TestNativeGeometryRowValuePreservesBinaryPayload(t *testing.T) {
	payload := []byte{0x00, 0x00, 0x00, 0x01, 0x01, 0x02}
	change := RowChange{ColumnTypes: map[string]string{"shape": "GEOMETRY"}}
	require.Equal(t, append([]byte{byte(len(payload)), 0, 0, 0}, payload...), nativeValueBytesForColumn(change, "shape", payload))
	largePayload := bytes.Repeat([]byte{0x7f}, 300)
	encoded := nativeValueBytesForColumn(change, "shape", largePayload)
	require.Equal(t, 300, int(binary.LittleEndian.Uint32(encoded[:4])))
	require.Equal(t, largePayload, encoded[4:])
}

func TestDumpFileWithNativePositionsKeepsRowFramesAligned(t *testing.T) {
	source, err := NewSource(t.TempDir(), "source-row-positions", 17)
	require.NoError(t, err)
	_, err = source.AppendTransaction(1, []RowChange{{
		Table:  "app.docs",
		Action: "insert",
		After:  map[string]interface{}{"id": int64(1)},
	}}, nil)
	require.NoError(t, err)

	events, err := source.DumpFileWithNativePositions("binlog.000001", 4)
	require.NoError(t, err)
	require.Len(t, events, 4, "GTID, TABLE_MAP, ROWS and XID must retain physical positions")
	require.Equal(t, EventBegin, events[0].Type)
	require.Equal(t, EventRow, events[1].Type)
	require.Equal(t, EventRow, events[2].Type)
	require.Equal(t, EventCommit, events[3].Type)
	require.Greater(t, events[1].NativeEndPosition, events[1].NativePosition)
	require.Equal(t, events[1].NativeEndPosition, events[2].NativePosition)
	require.Equal(t, events[2].NativeEndPosition, events[3].NativePosition)
}

func TestDumpFileWithNativePositionsKeepsGroupedRowsAligned(t *testing.T) {
	source, err := NewSource(t.TempDir(), "source-grouped-row-positions", 17)
	require.NoError(t, err)
	_, err = source.AppendTransaction(1, []RowChange{
		{Table: "app.docs", Action: "insert", Columns: []string{"id"}, After: map[string]interface{}{"id": int64(1)}},
		{Table: "app.docs", Action: "insert", Columns: []string{"id"}, After: map[string]interface{}{"id": int64(2)}},
	}, nil)
	require.NoError(t, err)

	events, err := source.DumpFileWithNativePositions("binlog.000001", 4)
	require.NoError(t, err)
	require.Len(t, events, 4, "GTID, grouped TABLE_MAP/ROWS and XID must retain physical positions")
	require.Equal(t, EventBegin, events[0].Type)
	require.Equal(t, EventRow, events[1].Type)
	require.Equal(t, EventRow, events[2].Type)
	require.Equal(t, EventCommit, events[3].Type)
	require.Greater(t, events[1].NativeEndPosition, events[1].NativePosition)
	require.Equal(t, events[1].NativeEndPosition, events[2].NativePosition)
	require.Equal(t, events[2].NativeEndPosition, events[3].NativePosition)
}

func TestDumpFileRejectsMissingBinlogFile(t *testing.T) {
	source, err := NewSource(t.TempDir(), "source-missing-binlog", 17)
	require.NoError(t, err)
	_, err = source.DumpFile("binlog.000999", 4)
	require.ErrorContains(t, err, `native binlog file "binlog.000999" does not exist`)
}

func TestNativeDumpFileRejectsMissingBinlogFile(t *testing.T) {
	source, err := NewSource(t.TempDir(), "source-missing-native-binlog", 17)
	require.NoError(t, err)
	_, _, err = source.NativeDumpFile("binlog.000999", 4, nil)
	require.ErrorContains(t, err, `native binlog file "binlog.000999" does not exist`)
}

func TestBinlogWriterRebuildsMissingNativeFileFromLogicalStream(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "binlog.jsonl")
	writer, err := NewBinlogWriter(path, 17)
	require.NoError(t, err)
	_, err = writer.AppendTransaction(GTID{UUID: "source", Seq: 1}, nil, []Statement{{Database: "app", SQL: "insert into docs values (1)"}})
	require.NoError(t, err)
	require.NoError(t, os.Remove(filepath.Join(dir, "binlog.000001")))

	_, err = NewBinlogWriter(path, 17)
	require.NoError(t, err)
	raw, err := os.ReadFile(filepath.Join(dir, "binlog.000001"))
	require.NoError(t, err)
	require.Greater(t, len(raw), 4)
	require.Equal(t, []byte{15, 35, 33, 2, 16}, nativeEventTypes(raw))
}

func TestBinlogWriterRotateCreatesNextNativeBinlogFile(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewBinlogWriter(filepath.Join(dir, "binlog.jsonl"), 17)
	require.NoError(t, err)
	_, err = writer.AppendTransaction(GTID{UUID: "source", Seq: 1}, nil, []Statement{{Database: "app", SQL: "insert into docs values (1)"}})
	require.NoError(t, err)
	_, err = writer.Rotate()
	require.NoError(t, err)
	_, err = writer.AppendTransaction(GTID{UUID: "source", Seq: 2}, nil, []Statement{{Database: "app", SQL: "insert into docs values (2)"}})
	require.NoError(t, err)

	first, err := os.ReadFile(filepath.Join(dir, "binlog.000001"))
	require.NoError(t, err)
	second, err := os.ReadFile(filepath.Join(dir, "binlog.000002"))
	require.NoError(t, err)
	require.Equal(t, []byte{15, 35, 33, 2, 16, 4}, nativeEventTypes(first))
	require.Equal(t, []byte{15, 35, 33, 2, 16}, nativeEventTypes(second))
}

func TestBinlogWriterMaintainsNativeIndexAndPreviousGTIDs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "binlog.jsonl")
	writer, err := NewBinlogWriter(path, 17)
	require.NoError(t, err)

	_, err = writer.AppendTransaction(GTID{UUID: "00112233-4455-6677-8899-aabbccddeeff", Seq: 1}, nil, nil)
	require.NoError(t, err)
	_, err = writer.Rotate()
	require.NoError(t, err)
	_, err = writer.AppendTransaction(GTID{UUID: "00112233-4455-6677-8899-aabbccddeeff", Seq: 2}, nil, nil)
	require.NoError(t, err)

	index, err := os.ReadFile(filepath.Join(dir, "binlog.index"))
	require.NoError(t, err)
	require.Contains(t, string(index), filepath.Join(dir, "binlog.000001"))
	require.Contains(t, string(index), filepath.Join(dir, "binlog.000002"))

	first, err := os.ReadFile(filepath.Join(dir, "binlog.000001"))
	require.NoError(t, err)
	second, err := os.ReadFile(filepath.Join(dir, "binlog.000002"))
	require.NoError(t, err)
	require.Equal(t, []byte{15, 35, 33, 2, 16, 4}, nativeEventTypes(first))
	require.Equal(t, []byte{15, 35, 33, 2, 16}, nativeEventTypes(second))
	gtidIndex, err := os.ReadFile(filepath.Join(dir, "binlog.gtid.index"))
	require.NoError(t, err)
	require.Contains(t, string(gtidIndex), `"sequence": 1`)
	require.Contains(t, string(gtidIndex), `"file": "binlog.000001"`)
	require.Contains(t, string(gtidIndex), `"sequence": 2`)
	require.Contains(t, string(gtidIndex), `"file": "binlog.000002"`)
	require.Len(t, writer.NativeGTIDIndex(), 2)

	reloadedWriter, err := NewBinlogWriter(path, 17)
	require.NoError(t, err)
	require.Len(t, reloadedWriter.NativeGTIDIndex(), 2)
	reloadedIndex, err := os.ReadFile(filepath.Join(dir, "binlog.index"))
	require.NoError(t, err)
	require.Equal(t, string(index), string(reloadedIndex))
}

func TestBinlogWriterLooksUpNativeGTIDPositionAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "binlog.jsonl")
	writer, err := NewBinlogWriter(path, 17)
	require.NoError(t, err)
	_, err = writer.AppendTransaction(GTID{UUID: "source", Seq: 7}, nil, nil)
	require.NoError(t, err)
	entry, ok := writer.NativeGTIDPosition("source", 7)
	require.True(t, ok)
	require.Equal(t, "binlog.000001", entry.File)
	require.NotZero(t, entry.Position)
	require.Greater(t, entry.End, entry.Position)

	reloaded, err := NewBinlogWriter(path, 17)
	require.NoError(t, err)
	reloadedEntry, ok := reloaded.NativeGTIDPosition(hex.EncodeToString(nativeGTIDSID("source")), 7)
	require.True(t, ok)
	require.Equal(t, entry, reloadedEntry)
}

func TestBinlogWriterRebuildsRotatedNativeFilesAfterRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "binlog.jsonl")
	writer, err := NewBinlogWriter(path, 17)
	require.NoError(t, err)
	_, err = writer.AppendTransaction(GTID{UUID: "source", Seq: 1}, nil, []Statement{{Database: "app", SQL: "insert into docs values (1)"}})
	require.NoError(t, err)
	_, err = writer.Rotate()
	require.NoError(t, err)
	_, err = writer.AppendTransaction(GTID{UUID: "source", Seq: 2}, nil, []Statement{{Database: "app", SQL: "insert into docs values (2)"}})
	require.NoError(t, err)
	require.NoError(t, os.Remove(filepath.Join(dir, "binlog.000001")))
	require.NoError(t, os.Remove(filepath.Join(dir, "binlog.000002")))

	_, err = NewBinlogWriter(path, 17)
	require.NoError(t, err)
	first, err := os.ReadFile(filepath.Join(dir, "binlog.000001"))
	require.NoError(t, err)
	second, err := os.ReadFile(filepath.Join(dir, "binlog.000002"))
	require.NoError(t, err)
	require.Equal(t, []byte{15, 35, 33, 2, 16, 4}, nativeEventTypes(first))
	require.Equal(t, []byte{15, 35, 33, 2, 16}, nativeEventTypes(second))
}

func TestNativeDumpFromContinuesAcrossRotatedFiles(t *testing.T) {
	source, err := NewSource(t.TempDir(), "native-pitr-source", 17)
	require.NoError(t, err)
	_, err = source.AppendTransaction(1, nil, []Statement{{Database: "app", SQL: "insert into docs values (1)"}})
	require.NoError(t, err)
	_, err = source.Writer.Rotate()
	require.NoError(t, err)
	_, err = source.AppendTransaction(2, nil, []Statement{{Database: "app", SQL: "insert into docs values (2)"}})
	require.NoError(t, err)

	events, err := source.NativeDumpFrom("binlog.000001", 4, nil)
	require.NoError(t, err)
	types := make([]byte, 0, len(events))
	for _, event := range events {
		types = append(types, event.Type)
	}
	require.Equal(t, []byte{15, 35, 33, 2, 16, 4, 15, 35, 33, 2, 16}, types)
	require.Equal(t, "binlog.000002", events[6].File)
}

func TestBinlogWriterRebuildsMissingRotatedNativeFileAfterRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "binlog.jsonl")
	writer, err := NewBinlogWriter(path, 17)
	require.NoError(t, err)
	_, err = writer.AppendTransaction(GTID{UUID: "source", Seq: 1}, nil, []Statement{{Database: "app", SQL: "insert into docs values (1)"}})
	require.NoError(t, err)
	_, err = writer.Rotate()
	require.NoError(t, err)
	_, err = writer.AppendTransaction(GTID{UUID: "source", Seq: 2}, nil, []Statement{{Database: "app", SQL: "insert into docs values (2)"}})
	require.NoError(t, err)
	require.NoError(t, os.Remove(filepath.Join(dir, "binlog.000002")))

	_, err = NewBinlogWriter(path, 17)
	require.NoError(t, err)
	first, err := os.ReadFile(filepath.Join(dir, "binlog.000001"))
	require.NoError(t, err)
	second, err := os.ReadFile(filepath.Join(dir, "binlog.000002"))
	require.NoError(t, err)
	require.Equal(t, []byte{15, 35, 33, 2, 16, 4}, nativeEventTypes(first))
	require.Equal(t, []byte{15, 35, 33, 2, 16}, nativeEventTypes(second))
}

func nativeEventTypes(raw []byte) []byte {
	types := make([]byte, 0)
	for offset := 4; offset < len(raw); {
		eventSize := int(binary.LittleEndian.Uint32(raw[offset+9 : offset+13]))
		types = append(types, raw[offset+4])
		offset += eventSize
	}
	return types
}

func TestGTIDSetParseMergeAndContainment(t *testing.T) {
	set, err := ParseGTIDSet("uuid-b:4-5:8,uuid-a:1")
	require.NoError(t, err)
	require.True(t, set.Contains(GTID{UUID: "uuid-b", Seq: 5}))
	other, err := ParseGTIDSet("uuid-b:6")
	require.NoError(t, err)
	set.Merge(other)
	require.Equal(t, "uuid-a:1,uuid-b:4-6:8", set.String())
}

func TestGTIDIntervalsParseTaggedGTID(t *testing.T) {
	intervals, err := ParseGTIDIntervals("00112233-4455-6677-8899-aabbccddeeff:foo:1-3:7")
	require.NoError(t, err)
	require.True(t, intervals.Contains(GTID{UUID: "00112233-4455-6677-8899-aabbccddeeff:foo", Seq: 2}))
	require.Equal(t, "00112233-4455-6677-8899-aabbccddeeff:foo:1-3:7", intervals.String())
}

func TestGTIDIntervalsMergeAndFormatWithoutExpansion(t *testing.T) {
	set := GTIDIntervals{}
	set.AddRange("uuid-b", 8, 10)
	set.AddRange("uuid-b", 4, 7)
	set.Add("uuid-a", 2)
	set.AddRange("uuid-a", 3, 5)
	set.AddRange("uuid-b", 10, 12)

	require.True(t, set.Contains(GTID{UUID: "uuid-b", Seq: 12}))
	require.False(t, set.Contains(GTID{UUID: "uuid-b", Seq: 13}))
	require.Equal(t, "uuid-a:2-5,uuid-b:4-12", set.String())
	require.Equal(t, []GTIDInterval{{Start: 4, End: 12}}, set["uuid-b"])
}

func TestGTIDIntervalsFromSetCompactsSequences(t *testing.T) {
	set := GTIDSet{"source": {1: {}, 2: {}, 3: {}, 7: {}}}
	intervals := GTIDIntervalsFromSet(set)
	require.Equal(t, []GTIDInterval{{Start: 1, End: 3}, {Start: 7, End: 7}}, intervals["source"])
}

func TestNativeGTIDFilteringAcceptsWireSIDForLogicalUUID(t *testing.T) {
	events := []BinlogEvent{
		{Type: EventBegin, GTID: GTID{UUID: "source", Seq: 7}},
		{Type: EventCommit, GTID: GTID{UUID: "source", Seq: 7}},
	}
	wireSID := nativeGTIDUUID("source")
	filtered := (&Source{}).filteredDumpIntervals(events, GTIDIntervals{
		wireSID: {{Start: 7, End: 7}},
	})
	require.Empty(t, filtered)
}

func TestNativeGTIDSIDFallsBackForInvalidHexUUID(t *testing.T) {
	invalidHex := "00000000-0000-0000-0000-00000000000g"
	require.NotEqual(t, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, nativeGTIDSID(invalidHex))
}

func TestParseGTIDIntervalsKeepsLargeTextRangesCompact(t *testing.T) {
	intervals, err := ParseGTIDIntervals("source:1-4611686018427387904")
	require.NoError(t, err)
	require.Equal(t, []GTIDInterval{{Start: 1, End: 4611686018427387904}}, intervals["source"])
	require.True(t, intervals.Contains(GTID{UUID: "source", Seq: 4611686018427387904}))
}

func TestParseGTIDSetRejectsUnboundedMaterialization(t *testing.T) {
	_, err := ParseGTIDSet("source:1-4611686018427387904")
	require.Error(t, err)
	require.Contains(t, err.Error(), "materialize")
}

func TestSourceReplicaCommitExactlyOnceAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	source, err := NewSource(dir, "source-uuid", 1)
	require.NoError(t, err)
	events, err := source.Append(1, []RowChange{{Table: "app.docs", Action: "insert", After: map[string]interface{}{"id": 1}}})
	require.NoError(t, err)
	require.Len(t, events, 3)

	replica, err := NewReplica(dir)
	require.NoError(t, err)
	position, err := replica.ReplicateFrom(source, 4)
	require.NoError(t, err)
	require.Len(t, replica.AppliedRows, 1)
	require.Equal(t, events[len(events)-1].Position+1, position)
	_, err = replica.ReplicateFrom(source, position)
	require.NoError(t, err)
	require.Len(t, replica.AppliedRows, 1)

	reloaded, err := NewReplica(dir)
	require.NoError(t, err)
	require.True(t, reloaded.Executed.Contains(GTID{UUID: "source-uuid", Seq: 1}))
}

func TestReplicaRejectsNilLogicalReplicationSourceWithoutPanic(t *testing.T) {
	replica, err := NewReplica(t.TempDir())
	require.NoError(t, err)

	position, err := replica.ReplicateFrom(nil, 17)
	require.Error(t, err)
	require.Equal(t, uint64(17), position)
	require.Contains(t, err.Error(), "replication source is nil")
	require.Equal(t, "replication source is nil", replica.LastError)
}

func TestSourceAppendCommittedTransactionPreservesRowsAndStatements(t *testing.T) {
	source, err := NewSource(t.TempDir(), "source", 1)
	require.NoError(t, err)
	_, err = source.AppendCommittedTransaction([]RowChange{{
		Table:       "app.docs",
		Action:      "update",
		Before:      map[string]interface{}{"id": int64(1), "payload": `{"name":"old"}`},
		After:       map[string]interface{}{"id": int64(1), "payload": `{"name":"new"}`},
		ColumnTypes: map[string]string{"id": "INT", "payload": "JSON"},
	}}, []Statement{{Database: "app", SQL: "update docs set payload = '{\"name\":\"new\"}' where id = 1"}})
	require.NoError(t, err)
	events, err := source.Dump(4)
	require.NoError(t, err)
	require.Len(t, events, 3)
	require.Len(t, events[1].Changes, 1)
	require.Len(t, events[1].Statements, 1)
}

func TestReplicaPrefersAtomicRowApplyWhenRowImagesArePresent(t *testing.T) {
	source, err := NewSource(t.TempDir(), "source", 1)
	require.NoError(t, err)
	events, err := source.AppendCommittedTransaction([]RowChange{{
		Table:  "app.docs",
		Action: "update",
		Before: map[string]interface{}{"id": int64(1), "payload": `{"name":"old"}`},
		After:  map[string]interface{}{"id": int64(1), "payload": `{"name":"new"}`},
	}}, []Statement{{Database: "app", SQL: "update docs set payload = '{\"name\":\"new\"}' where id = 1"}})
	require.NoError(t, err)

	replica, err := NewReplica(t.TempDir())
	require.NoError(t, err)
	var applied []RowChange
	replica.ApplyRows = func(changes []RowChange) error {
		applied = append(applied, changes...)
		return nil
	}
	replica.ApplyStatements = func([]Statement) error {
		t.Fatal("statement apply must not run when atomic row apply is configured")
		return nil
	}
	require.NoError(t, replica.Apply(events))
	require.Len(t, applied, 1)
	require.Equal(t, `{"name":"new"}`, applied[0].After["payload"])
}

func TestReplicationCommitBoundaryAndRestartStateAreDurable(t *testing.T) {
	dir := t.TempDir()
	source, err := NewSource(dir, "source-uuid", 1)
	require.NoError(t, err)
	events, err := source.Append(1, []RowChange{{Table: "app.docs", Action: "insert", After: map[string]interface{}{"id": 1}}})
	require.NoError(t, err)

	replica, err := NewReplica(dir)
	require.NoError(t, err)
	require.NoError(t, replica.Apply(events[:2]))
	require.Empty(t, replica.AppliedRows, "row events must not commit before COMMIT")
	require.NoError(t, replica.Apply(events))
	require.Len(t, replica.AppliedRows, 1)

	reloaded, err := NewReplica(dir)
	require.NoError(t, err)
	require.Len(t, reloaded.AppliedRows, 1, "applied row set must survive restart")
	require.NoError(t, reloaded.Apply(events))
	require.Len(t, reloaded.AppliedRows, 1, "duplicate GTID must not reapply rows")

	statePath := filepath.Join(dir, "replication", "source_gtid.json")
	require.NoError(t, os.Remove(statePath))
	restartedSource, err := NewSource(dir, "source-uuid", 1)
	require.NoError(t, err)
	duplicate, err := restartedSource.Append(1, []RowChange{{Table: "app.docs", Action: "insert"}})
	require.NoError(t, err)
	require.Empty(t, duplicate, "source must reconcile committed GTIDs from binlog after state loss")
}

func TestSourceCommittedTransactionKeySurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	source, err := NewSource(dir, "source-keyed", 1)
	require.NoError(t, err)
	changes := []RowChange{{Table: "app.docs", Action: "insert", After: map[string]interface{}{"id": 1}}}
	_, err = source.AppendCommittedTransactionWithKey("xa:g:b:1", changes, nil)
	require.NoError(t, err)

	restarted, err := NewSource(dir, "source-keyed", 1)
	require.NoError(t, err)
	duplicate, err := restarted.AppendCommittedTransactionWithKey("xa:g:b:1", changes, nil)
	require.NoError(t, err)
	require.Empty(t, duplicate)
	transactions, err := restarted.DecodeNativeDumpFrom("binlog.000001", 4, GTIDIntervals{})
	require.NoError(t, err)
	require.Len(t, transactions, 1)
}

func TestReplicationRotateChecksumPositionAndRetryMetadata(t *testing.T) {
	dir := t.TempDir()
	source, err := NewSource(dir, "source-uuid", 1)
	require.NoError(t, err)
	rotate, err := source.Rotate()
	require.NoError(t, err)
	require.Equal(t, EventRotate, rotate.Type)
	events, err := source.Append(2, []RowChange{{Table: "app.docs", Action: "update"}})
	require.NoError(t, err)
	require.Len(t, events, 3)
	encoded, err := events[1].Encode()
	require.NoError(t, err)
	decoded, err := DecodeEvent(encoded)
	require.NoError(t, err)
	require.Equal(t, events[1].Position, decoded.Position)

	replica, err := NewReplica(t.TempDir())
	require.NoError(t, err)
	position, err := replica.ReplicateFromWithRetry(source, 4, 1)
	require.NoError(t, err)
	require.GreaterOrEqual(t, position, events[2].Position)
	require.Equal(t, uint64(0), replica.NetworkRetries)
	require.GreaterOrEqual(t, replica.LagSeconds(time.Now().UTC()), float64(0))
}

func TestReplicaLagReturnsZeroWhenReplicationIsCaughtUp(t *testing.T) {
	source, err := NewSource(t.TempDir(), "source-uuid", 1)
	require.NoError(t, err)
	replica, err := NewReplica(t.TempDir())
	require.NoError(t, err)
	replica.LastAppliedAt = time.Now().UTC().Add(-time.Hour)

	_, err = replica.ReplicateFrom(source, 4)
	require.NoError(t, err)
	require.True(t, replica.LastAppliedAt.IsZero(), "an empty source response means the replica is caught up")
	require.Zero(t, replica.LagSeconds(time.Now().UTC()))
}
