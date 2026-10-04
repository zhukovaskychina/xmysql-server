package replication

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func requireNativeFramesSemanticallyEqual(t *testing.T, expected, actual []NativeBinlogEvent) {
	t.Helper()
	require.Len(t, actual, len(expected))
	for index := range expected {
		require.Equal(t, expected[index].Type, actual[index].Type)
		require.Equal(t, expected[index].Position, actual[index].Position)
		require.Equal(t, expected[index].EndPosition, actual[index].EndPosition)
		require.Equal(t, binary.LittleEndian.Uint32(expected[index].Raw[5:9]), binary.LittleEndian.Uint32(actual[index].Raw[5:9]))
		require.Equal(t, expected[index].Raw[17:19], actual[index].Raw[17:19])
		require.Equal(t, nativeEventBody(expected[index]), nativeEventBody(actual[index]))
	}
}

func TestSourceImportRelayEventsRecoversStatePersistFailureAcrossRestart(t *testing.T) {
	upstream, err := NewSource(t.TempDir(), "relay-state-upstream", 17)
	require.NoError(t, err)
	_, err = upstream.AppendCommittedTransactionWithKey("relay-state-key", []RowChange{{
		Table:  "app.docs",
		Action: "insert",
		After:  map[string]interface{}{"id": int64(1)},
	}}, nil)
	require.NoError(t, err)
	relay, err := upstream.Dump(4)
	require.NoError(t, err)

	promotedDir := t.TempDir()
	promoted, err := NewSource(promotedDir, "promoted-relay-state", 18)
	require.NoError(t, err)
	promoted.durableStateHook = func() error { return errors.New("injected relay state persist failure") }
	require.ErrorContains(t, promoted.ImportRelayEvents(relay), "injected relay state persist failure")
	promoted.durableStateHook = nil

	// The logical relay/native projection was already durable before the state
	// replacement failed. A fresh source must reconstruct the GTID and stable
	// transaction key from that committed stream, then acknowledge the retry
	// without producing a second logical transaction.
	restarted, err := NewSource(promotedDir, promoted.UUID, 18)
	require.NoError(t, err)
	require.True(t, restarted.Executed.Contains(GTID{UUID: upstream.UUID, Seq: 1}))
	require.Equal(t, GTID{UUID: upstream.UUID, Seq: 1}, restarted.transactionKeys["relay-state-key"])
	require.NoError(t, restarted.ImportRelayEvents(relay))

	logical, err := restarted.Dump(4)
	require.NoError(t, err)
	commits := 0
	for _, event := range logical {
		if event.Type == EventCommit {
			commits++
		}
	}
	require.Equal(t, 1, commits, "state recovery and retry must not duplicate the relay transaction")
}

func TestNativeDecoderSupportsPreGARowEvents(t *testing.T) {
	source, err := NewSource(t.TempDir(), "pre-ga-row-source", 27)
	require.NoError(t, err)
	_, err = source.AppendCommittedTransactionWithKey("pre-ga-row-key", []RowChange{{
		Table:       "app.docs",
		Action:      "insert",
		Columns:     []string{"id", "body"},
		ColumnTypes: map[string]string{"id": "BIGINT", "body": "VARCHAR(32)"},
		After:       map[string]interface{}{"id": int64(7), "body": "legacy"},
	}, {
		Table:       "app.docs",
		Action:      "update",
		Columns:     []string{"id", "body"},
		ColumnTypes: map[string]string{"id": "BIGINT", "body": "VARCHAR(32)"},
		Before:      map[string]interface{}{"id": int64(7), "body": "legacy"},
		After:       map[string]interface{}{"id": int64(8), "body": "updated"},
	}, {
		Table:       "app.docs",
		Action:      "delete",
		Columns:     []string{"id", "body"},
		ColumnTypes: map[string]string{"id": "BIGINT", "body": "VARCHAR(32)"},
		Before:      map[string]interface{}{"id": int64(8), "body": "updated"},
	}}, nil)
	require.NoError(t, err)
	native, err := source.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)

	// Convert the modern WRITE_ROWS_EVENT_V2 frame emitted by the writer into
	// the byte-compatible PRE_GA_WRITE_ROWS_EVENT variant. Older upstream
	// MySQL binlogs can still contain this v1 row layout.
	mutated := make([]NativeBinlogEvent, len(native))
	copy(mutated, native)
	legacyTypes := map[byte]byte{30: 22, 31: 21, 32: 20}
	found := make(map[byte]bool, len(legacyTypes))
	for index := range mutated {
		legacyType, ok := legacyTypes[mutated[index].Type]
		if !ok {
			continue
		}
		modern := mutated[index].Raw
		extraLengthOffset := nativeEventHeaderLength + 8
		require.GreaterOrEqual(t, len(modern), extraLengthOffset+2+nativeChecksumLength)
		require.Equal(t, uint16(2), binary.LittleEndian.Uint16(modern[extraLengthOffset:extraLengthOffset+2]))
		// V1 rows omit the v2 extra-data-length field. The writer's default
		// event has an empty v2 field, so remove exactly its two-byte envelope.
		raw := make([]byte, 0, len(modern)-2)
		raw = append(raw, modern[:extraLengthOffset]...)
		raw = append(raw, modern[extraLengthOffset+2:len(modern)-nativeChecksumLength]...)
		raw = append(raw, make([]byte, nativeChecksumLength)...)
		raw[4] = legacyType
		binary.LittleEndian.PutUint32(raw[9:13], uint32(len(raw)))
		binary.LittleEndian.PutUint32(raw[len(raw)-nativeChecksumLength:], crc32IEEE(raw[:len(raw)-nativeChecksumLength]))
		mutated[index].Type = legacyType
		mutated[index].Raw = raw
		found[legacyType] = true
	}
	for _, legacyType := range []byte{22, 21, 20} {
		require.True(t, found[legacyType], "the source transaction must contain modern row frame for PRE-GA type %d", legacyType)
	}

	decoded, err := NewNativeBinlogDecoderForSource(source.UUID).DecodeTransactions(mutated)
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Len(t, decoded[0].Changes, 3)
	require.Equal(t, "insert", decoded[0].Changes[0].Action)
	require.Equal(t, int64(7), decoded[0].Changes[0].After["id"])
	require.Equal(t, "legacy", decoded[0].Changes[0].After["body"])
	require.Equal(t, "update", decoded[0].Changes[1].Action)
	require.Equal(t, int64(7), decoded[0].Changes[1].Before["id"])
	require.Equal(t, int64(8), decoded[0].Changes[1].After["id"])
	require.Equal(t, "delete", decoded[0].Changes[2].Action)
	require.Equal(t, int64(8), decoded[0].Changes[2].Before["id"])
	require.Nil(t, decoded[0].Changes[2].After)
}

func TestSourceRetryAfterNativeAppendFailureDoesNotDuplicateTransaction(t *testing.T) {
	dir := t.TempDir()
	source, err := NewSource(dir, "native-retry-source", 17)
	require.NoError(t, err)

	previous := nativeAppendHook
	nativeAppendHook = func([]BinlogEvent) error {
		nativeAppendHook = nil
		return errors.New("injected native append failure")
	}
	t.Cleanup(func() { nativeAppendHook = previous })

	changes := []RowChange{{
		Table:  "app.docs",
		Action: "insert",
		After:  map[string]interface{}{"id": int64(1)},
	}}
	_, err = source.AppendCommittedTransactionWithKey("retry-key", changes, nil)
	require.ErrorContains(t, err, "injected native append failure")

	_, err = source.AppendCommittedTransactionWithKey("retry-key", changes, nil)
	require.NoError(t, err)

	logical, err := source.Writer.ReadFrom(4)
	require.NoError(t, err)
	commits := 0
	for _, event := range logical {
		if event.Type == EventCommit {
			commits++
		}
	}
	require.Equal(t, 1, commits)

	native, err := source.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	decoded, err := NewNativeBinlogDecoder().DecodeTransactions(native)
	require.NoError(t, err)
	require.Len(t, decoded, 1)
}

func TestSourceImportRelayEventsRetryRebuildsNativeAfterAppendFailure(t *testing.T) {
	upstream, err := NewSource(t.TempDir(), "00112233-4455-6677-8899-aabbccddeeff", 31)
	require.NoError(t, err)
	changes := []RowChange{{
		Table:   "app.docs",
		Action:  "insert",
		Columns: []string{"id"},
		After:   map[string]interface{}{"id": int64(31)},
	}}
	events, err := upstream.Append(1, changes)
	require.NoError(t, err)

	promoted, err := NewSource(t.TempDir(), "11223344-5566-7788-99aa-bbccddeeff00", 32)
	require.NoError(t, err)
	previous := nativeAppendHook
	nativeAppendHook = func([]BinlogEvent) error {
		nativeAppendHook = nil
		return errors.New("injected promoted native append failure")
	}
	t.Cleanup(func() { nativeAppendHook = previous })

	require.ErrorContains(t, promoted.ImportRelayEvents(events), "injected promoted native append failure")
	require.NoError(t, promoted.ImportRelayEvents(events), "retry must rebuild native frames from the durable logical relay")

	transactions, err := promoted.DecodeNativeDumpFrom("binlog.000001", 4, nil)
	require.NoError(t, err)
	require.Len(t, transactions, 1)
	require.Equal(t, upstream.UUID, transactions[0].GTID.UUID)
	require.EqualValues(t, int64(31), transactions[0].Changes[0].After["id"])
}

func TestSourceImportRelayEventsRetryAfterPartialBatchRebuildsCompleteNativeTransaction(t *testing.T) {
	upstream, err := NewSource(t.TempDir(), "33445566-7788-99aa-bbcc-ddeeff001122", 33)
	require.NoError(t, err)
	events, err := upstream.Append(1, []RowChange{{
		Table:   "app.docs",
		Action:  "insert",
		Columns: []string{"id"},
		After:   map[string]interface{}{"id": int64(33)},
	}})
	require.NoError(t, err)
	require.Len(t, events, 3)

	promoted, err := NewSource(t.TempDir(), "44556677-8899-aabb-ccdd-eeff00112233", 34)
	require.NoError(t, err)
	previous := nativeAppendHook
	nativeAppendHook = func([]BinlogEvent) error {
		nativeAppendHook = nil
		return errors.New("injected partial relay native append failure")
	}
	t.Cleanup(func() { nativeAppendHook = previous })

	// The first relay batch contains a durable logical prefix but its native
	// append fails. The next batch only supplies the missing COMMIT event.
	require.ErrorContains(t, promoted.ImportRelayEvents(events[:2]), "injected partial relay native append failure")
	require.NoError(t, promoted.ImportRelayEvents(events))

	transactions, err := promoted.DecodeNativeDumpFrom("binlog.000001", 4, nil)
	require.NoError(t, err)
	require.Len(t, transactions, 1)
	require.Equal(t, upstream.UUID, transactions[0].GTID.UUID)
	require.EqualValues(t, int64(33), transactions[0].Changes[0].After["id"])
}

func TestSourceImportRelayEventsRebuildsNativeWhenGTIDIdentityIsCorrupted(t *testing.T) {
	upstream, err := NewSource(t.TempDir(), "55667788-99aa-bbcc-ddee-ff0011223344", 35)
	require.NoError(t, err)
	events, err := upstream.Append(1, []RowChange{{
		Table:   "app.docs",
		Action:  "insert",
		Columns: []string{"id"},
		After:   map[string]interface{}{"id": int64(35)},
	}})
	require.NoError(t, err)

	promoted, err := NewSource(t.TempDir(), "66778899-aabb-ccdd-eeff-001122334455", 36)
	require.NoError(t, err)
	require.NoError(t, promoted.ImportRelayEvents(events))

	native, err := promoted.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	var gtidEvent NativeBinlogEvent
	for _, event := range native {
		if event.Type == 33 {
			gtidEvent = event
			break
		}
	}
	require.NotZero(t, gtidEvent.EndPosition)

	raw, err := os.ReadFile(promoted.Writer.nativePath)
	require.NoError(t, err)
	sequenceOffset := int(gtidEvent.Position) + nativeEventHeaderLength + 1 + 16
	binary.LittleEndian.PutUint64(raw[sequenceOffset:sequenceOffset+8], 999)
	frameEnd := int(gtidEvent.EndPosition)
	binary.LittleEndian.PutUint32(raw[frameEnd-nativeChecksumLength:frameEnd], crc32.ChecksumIEEE(raw[int(gtidEvent.Position):frameEnd-nativeChecksumLength]))
	require.NoError(t, os.WriteFile(promoted.Writer.nativePath, raw, 0644))
	corrupted, err := promoted.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	foundCorrupted := false
	for _, event := range corrupted {
		if event.Type != 33 {
			continue
		}
		sequence, ok := nativeGTIDSequence(event.Raw)
		require.True(t, ok)
		require.EqualValues(t, 999, sequence)
		foundCorrupted = true
		break
	}
	require.True(t, foundCorrupted)

	// The logical relay is already present, so this call exercises the
	// no-new-events repair path. A type-only check would incorrectly accept the
	// corrupted GTID frame and leave sequence 999 visible to native consumers.
	require.NoError(t, promoted.ImportRelayEvents(events))

	repaired, err := promoted.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	for _, event := range repaired {
		if event.Type != 33 {
			continue
		}
		sequence, ok := nativeGTIDSequence(event.Raw)
		require.True(t, ok)
		require.EqualValues(t, 1, sequence)
		return
	}
	t.Fatal("repaired native stream has no GTID event")
}

func TestSourceImportRelayEventsRebuildsNativeWhenRowPayloadIsCorrupted(t *testing.T) {
	upstream, err := NewSource(t.TempDir(), "778899aa-bbcc-ddee-ff00-112233445566", 37)
	require.NoError(t, err)
	events, err := upstream.Append(1, []RowChange{{
		Table:   "app.docs",
		Action:  "insert",
		Columns: []string{"id", "title"},
		After:   map[string]interface{}{"id": int64(37), "title": "payload-integrity"},
	}})
	require.NoError(t, err)

	promoted, err := NewSource(t.TempDir(), "8899aabb-ccdd-eeff-0011-223344556677", 38)
	require.NoError(t, err)
	require.NoError(t, promoted.ImportRelayEvents(events))

	baseline, err := os.ReadFile(promoted.Writer.nativePath)
	require.NoError(t, err)
	native, err := promoted.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	var rowEvent NativeBinlogEvent
	for _, event := range native {
		if event.Type == 30 || event.Type == 31 || event.Type == 32 || event.Type == 23 || event.Type == 24 || event.Type == 25 {
			rowEvent = event
			break
		}
	}
	require.NotZero(t, rowEvent.EndPosition, "expected a native row event")

	raw, err := os.ReadFile(promoted.Writer.nativePath)
	require.NoError(t, err)
	lastBodyByte := int(rowEvent.EndPosition) - nativeChecksumLength - 1
	require.GreaterOrEqual(t, lastBodyByte, int(rowEvent.Position)+nativeEventHeaderLength)
	raw[lastBodyByte] ^= 0x01
	frameStart := int(rowEvent.Position)
	frameEnd := int(rowEvent.EndPosition)
	binary.LittleEndian.PutUint32(raw[frameEnd-nativeChecksumLength:frameEnd], crc32.ChecksumIEEE(raw[frameStart:frameEnd-nativeChecksumLength]))
	require.NoError(t, os.WriteFile(promoted.Writer.nativePath, raw, 0644))

	corrupted, err := os.ReadFile(promoted.Writer.nativePath)
	require.NoError(t, err)
	require.NotEqual(t, baseline, corrupted, "test must create a checksum-valid payload corruption")
	// The logical relay is already durable. A type/GTID-only check would accept
	// this checksum-valid ROWS_EVENT and leave the corrupted row visible.
	require.NoError(t, promoted.ImportRelayEvents(events))

	repaired, err := promoted.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	requireNativeFramesSemanticallyEqual(t, native, repaired)
}

func TestSourceImportRelayEventsRebuildsNativeWhenServerIDHeaderIsCorrupted(t *testing.T) {
	upstream, err := NewSource(t.TempDir(), "99aabbcc-ddee-ff00-1122-334455667788", 39)
	require.NoError(t, err)
	events, err := upstream.Append(1, []RowChange{{
		Table:   "app.docs",
		Action:  "insert",
		Columns: []string{"id"},
		After:   map[string]interface{}{"id": int64(39)},
	}})
	require.NoError(t, err)

	promoted, err := NewSource(t.TempDir(), "aabbccdd-eeff-0011-2233-445566778899", 40)
	require.NoError(t, err)
	require.NoError(t, promoted.ImportRelayEvents(events))
	baseline, err := os.ReadFile(promoted.Writer.nativePath)
	require.NoError(t, err)
	native, err := promoted.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	var rowEvent NativeBinlogEvent
	for _, event := range native {
		if event.Type == 30 || event.Type == 31 || event.Type == 32 || event.Type == 23 || event.Type == 24 || event.Type == 25 {
			rowEvent = event
			break
		}
	}
	require.NotZero(t, rowEvent.EndPosition, "expected a native row event")

	raw, err := os.ReadFile(promoted.Writer.nativePath)
	require.NoError(t, err)
	serverIDOffset := int(rowEvent.Position) + 5
	binary.LittleEndian.PutUint32(raw[serverIDOffset:serverIDOffset+4], 999)
	frameStart := int(rowEvent.Position)
	frameEnd := int(rowEvent.EndPosition)
	binary.LittleEndian.PutUint32(raw[frameEnd-nativeChecksumLength:frameEnd], crc32.ChecksumIEEE(raw[frameStart:frameEnd-nativeChecksumLength]))
	require.NoError(t, os.WriteFile(promoted.Writer.nativePath, raw, 0644))

	corrupted, err := os.ReadFile(promoted.Writer.nativePath)
	require.NoError(t, err)
	require.NotEqual(t, baseline, corrupted, "test must create a checksum-valid header corruption")
	require.NoError(t, promoted.ImportRelayEvents(events))

	repaired, err := promoted.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	requireNativeFramesSemanticallyEqual(t, native, repaired)
}

func TestSourceImportRelayEventsRebuildsNativeWhenPreviousGTIDPayloadIsCorrupted(t *testing.T) {
	upstream, err := NewSource(t.TempDir(), "bbccddee-ff00-1122-3344-556677889900", 41)
	require.NoError(t, err)
	events, err := upstream.Append(1, []RowChange{{
		Table:   "app.docs",
		Action:  "insert",
		Columns: []string{"id"},
		After:   map[string]interface{}{"id": int64(41)},
	}})
	require.NoError(t, err)

	promoted, err := NewSource(t.TempDir(), "ccddee00-1122-3344-5566-77889900aabb", 42)
	require.NoError(t, err)
	require.NoError(t, promoted.ImportRelayEvents(events))
	baseline, err := os.ReadFile(promoted.Writer.nativePath)
	require.NoError(t, err)
	native, err := promoted.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	var previousGTIDs NativeBinlogEvent
	for _, event := range native {
		if event.Type == 35 {
			previousGTIDs = event
			break
		}
	}
	require.NotZero(t, previousGTIDs.EndPosition, "expected a PREVIOUS_GTIDS_EVENT")

	raw, err := os.ReadFile(promoted.Writer.nativePath)
	require.NoError(t, err)
	lastBodyByte := int(previousGTIDs.EndPosition) - nativeChecksumLength - 1
	require.GreaterOrEqual(t, lastBodyByte, int(previousGTIDs.Position)+nativeEventHeaderLength)
	raw[lastBodyByte] ^= 0x01
	frameStart := int(previousGTIDs.Position)
	frameEnd := int(previousGTIDs.EndPosition)
	binary.LittleEndian.PutUint32(raw[frameEnd-nativeChecksumLength:frameEnd], crc32.ChecksumIEEE(raw[frameStart:frameEnd-nativeChecksumLength]))
	require.NoError(t, os.WriteFile(promoted.Writer.nativePath, raw, 0644))

	corrupted, err := os.ReadFile(promoted.Writer.nativePath)
	require.NoError(t, err)
	require.NotEqual(t, baseline, corrupted, "test must create a checksum-valid PREVIOUS_GTIDS corruption")
	require.NoError(t, promoted.ImportRelayEvents(events))

	repaired, err := promoted.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	requireNativeFramesSemanticallyEqual(t, native, repaired)
}

func TestBinlogWriterRebuildsRotatedPreviousGTIDPayloadWhenCorrupted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "binlog.jsonl")
	writer, err := NewBinlogWriter(path, 43)
	require.NoError(t, err)
	_, err = writer.AppendTransaction(GTID{UUID: "ddeeff00-1122-3344-5566-77889900aabb", Seq: 1}, nil, nil)
	require.NoError(t, err)
	_, err = writer.Rotate()
	require.NoError(t, err)
	_, err = writer.AppendTransaction(GTID{UUID: "ddeeff00-1122-3344-5566-77889900aabb", Seq: 2}, nil, nil)
	require.NoError(t, err)
	events, err := writer.ReadFrom(4)
	require.NoError(t, err)

	secondPath := filepath.Join(dir, "binlog.000002")
	baseline, err := os.ReadFile(secondPath)
	require.NoError(t, err)
	native, err := writer.NativeEvents("binlog.000002")
	require.NoError(t, err)
	var previousGTIDs NativeBinlogEvent
	for _, event := range native {
		if event.Type == 35 {
			previousGTIDs = event
			break
		}
	}
	require.NotZero(t, previousGTIDs.EndPosition, "expected rotated PREVIOUS_GTIDS_EVENT")

	raw, err := os.ReadFile(secondPath)
	require.NoError(t, err)
	lastBodyByte := int(previousGTIDs.EndPosition) - nativeChecksumLength - 1
	require.GreaterOrEqual(t, lastBodyByte, int(previousGTIDs.Position)+nativeEventHeaderLength)
	raw[lastBodyByte] ^= 0x01
	frameStart := int(previousGTIDs.Position)
	frameEnd := int(previousGTIDs.EndPosition)
	binary.LittleEndian.PutUint32(raw[frameEnd-nativeChecksumLength:frameEnd], crc32.ChecksumIEEE(raw[frameStart:frameEnd-nativeChecksumLength]))
	require.NoError(t, os.WriteFile(secondPath, raw, 0644))

	corrupted, err := os.ReadFile(secondPath)
	require.NoError(t, err)
	require.NotEqual(t, baseline, corrupted, "test must create a checksum-valid rotated header corruption")
	require.NoError(t, writer.ImportEvents(events))

	repaired, err := writer.NativeEvents("binlog.000002")
	require.NoError(t, err)
	requireNativeFramesSemanticallyEqual(t, native, repaired)
}

func TestBinlogWriterRebuildsRotatedEventBodyWhenCorrupted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "binlog.jsonl")
	writer, err := NewBinlogWriter(path, 44)
	require.NoError(t, err)
	_, err = writer.AppendTransaction(GTID{UUID: "eeff0011-2233-4455-6677-8899aabbccdd", Seq: 1}, nil, nil)
	require.NoError(t, err)
	_, err = writer.Rotate()
	require.NoError(t, err)
	_, err = writer.AppendTransaction(GTID{UUID: "eeff0011-2233-4455-6677-8899aabbccdd", Seq: 2}, nil, nil)
	require.NoError(t, err)
	events, err := writer.ReadFrom(4)
	require.NoError(t, err)

	native, err := writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	var rotate NativeBinlogEvent
	for _, event := range native {
		if event.Type == 4 {
			rotate = event
			break
		}
	}
	require.NotZero(t, rotate.EndPosition, "expected a ROTATE_EVENT")

	rotatePath := filepath.Join(dir, "binlog.000001")
	raw, err := os.ReadFile(rotatePath)
	require.NoError(t, err)
	targetStart := int(rotate.Position) + nativeEventHeaderLength + 8
	require.Less(t, targetStart, int(rotate.EndPosition)-nativeChecksumLength)
	raw[targetStart] = 'c'
	frameStart := int(rotate.Position)
	frameEnd := int(rotate.EndPosition)
	binary.LittleEndian.PutUint32(raw[frameEnd-nativeChecksumLength:frameEnd], crc32.ChecksumIEEE(raw[frameStart:frameEnd-nativeChecksumLength]))
	require.NoError(t, os.WriteFile(rotatePath, raw, 0644))

	corrupted, err := os.ReadFile(rotatePath)
	require.NoError(t, err)
	baseline := rotate.Raw
	require.NotEqual(t, baseline, corrupted[frameStart:frameEnd], "test must create a checksum-valid ROTATE_EVENT corruption")
	require.NoError(t, writer.ImportEvents(events))

	repaired, err := writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	requireNativeFramesSemanticallyEqual(t, native, repaired)
}

func TestBinlogWriterRetriesPendingRotateWithoutDuplicatingLogicalBoundary(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewBinlogWriter(filepath.Join(dir, "binlog.jsonl"), 45)
	require.NoError(t, err)
	_, err = writer.AppendTransaction(GTID{UUID: "ff001122-3344-5566-7788-99aabbccddee", Seq: 1}, nil, nil)
	require.NoError(t, err)

	previous := nativeRotateAppendHook
	nativeRotateAppendHook = func(BinlogEvent, string) error {
		nativeRotateAppendHook = nil
		return errors.New("injected native rotate append failure")
	}
	t.Cleanup(func() { nativeRotateAppendHook = previous })

	_, err = writer.Rotate()
	require.ErrorContains(t, err, "injected native rotate append failure")
	logical, err := writer.ReadFrom(4)
	require.NoError(t, err)
	var pending BinlogEvent
	for _, event := range logical {
		if event.Type == EventRotate {
			pending = event
		}
	}
	require.Equal(t, EventRotate, pending.Type)

	retried, err := writer.Rotate()
	require.NoError(t, err)
	require.Equal(t, pending, retried, "retry must acknowledge the pending logical ROTATE")

	logical, err = writer.ReadFrom(4)
	require.NoError(t, err)
	rotateCount := 0
	for _, event := range logical {
		if event.Type == EventRotate {
			rotateCount++
		}
	}
	require.Equal(t, 1, rotateCount, "a failed native append must not create a second logical ROTATE")
	require.Len(t, writer.NativeFiles(), 2, "recovery must materialize the pending next native file")
}

func TestBinlogWriterRetriesRotateAfterNativeIndexPersistFailureWithoutDuplicatingLogicalBoundary(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewBinlogWriter(filepath.Join(dir, "binlog.jsonl"), 46)
	require.NoError(t, err)
	_, err = writer.AppendTransaction(GTID{UUID: "00112233-4455-6677-8899-aabbccddeeff", Seq: 1}, nil, nil)
	require.NoError(t, err)

	previous := replicationFileSyncHook
	replicationFileSyncHook = func(file *os.File) error {
		if strings.HasSuffix(file.Name(), "binlog.index.tmp") {
			replicationFileSyncHook = nil
			return errors.New("injected native index persist failure")
		}
		return nil
	}
	t.Cleanup(func() { replicationFileSyncHook = previous })

	_, err = writer.Rotate()
	require.ErrorContains(t, err, "injected native index persist failure")
	logical, err := writer.ReadFrom(4)
	require.NoError(t, err)
	var pending BinlogEvent
	for _, event := range logical {
		if event.Type == EventRotate {
			pending = event
		}
	}
	require.Equal(t, EventRotate, pending.Type)
	require.Len(t, writer.NativeFiles(), 2, "native rotation is already durable when the index persist fails")

	retried, err := writer.Rotate()
	require.NoError(t, err)
	require.Equal(t, pending, retried, "retry must acknowledge the pending logical ROTATE")

	logical, err = writer.ReadFrom(4)
	require.NoError(t, err)
	rotateCount := 0
	for _, event := range logical {
		if event.Type == EventRotate {
			rotateCount++
		}
	}
	require.Equal(t, 1, rotateCount, "an index persist failure must not create a second logical ROTATE")
	require.Len(t, writer.NativeFiles(), 2, "retry must not create a third native file")
	index, err := os.ReadFile(filepath.Join(dir, "binlog.index"))
	require.NoError(t, err)
	require.Contains(t, string(index), filepath.Join(dir, "binlog.000002"))
}

func TestBinlogWriterRetriesImportAfterNativeGTIDIndexPersistFailure(t *testing.T) {
	sourceDir := t.TempDir()
	source, err := NewBinlogWriter(filepath.Join(sourceDir, "source.jsonl"), 47)
	require.NoError(t, err)
	events, err := source.AppendTransaction(GTID{UUID: "11223344-5566-7788-99aa-bbccddeeff00", Seq: 1}, nil, nil)
	require.NoError(t, err)

	dir := t.TempDir()
	target, err := NewBinlogWriter(filepath.Join(dir, "binlog.jsonl"), 48)
	require.NoError(t, err)
	previous := replicationFileSyncHook
	replicationFileSyncHook = func(file *os.File) error {
		if strings.HasSuffix(file.Name(), "binlog.gtid.index.tmp") {
			replicationFileSyncHook = nil
			return errors.New("injected native GTID index persist failure")
		}
		return nil
	}
	t.Cleanup(func() { replicationFileSyncHook = previous })

	require.ErrorContains(t, target.ImportEvents(events), "injected native GTID index persist failure")
	indexPath := filepath.Join(dir, "binlog.gtid.index")
	index, err := os.ReadFile(indexPath)
	require.NoError(t, err)
	require.NotContains(t, string(index), `"sequence": 1`, "failed import must not claim a durable GTID index entry")

	require.NoError(t, target.ImportEvents(events))
	index, err = os.ReadFile(indexPath)
	require.NoError(t, err)
	require.Contains(t, string(index), `"sequence": 1`, "retry must persist the GTID index after the native frames are already durable")
	entry, ok := target.NativeGTIDPosition("112233445566778899aabbccddeeff00", 1)
	require.True(t, ok)
	require.Equal(t, "binlog.000001", entry.File)
}

func TestSourceOnePhaseXARetryAfterNativeAppendFailureDoesNotDuplicateTransaction(t *testing.T) {
	dir := t.TempDir()
	source, err := NewSource(dir, "native-one-phase-retry-source", 20)
	require.NoError(t, err)
	xid := XAIdentity{GTRID: "native-one-phase-retry", BQUAL: "branch", FormatID: 20}

	previous := nativeAppendHook
	nativeAppendHook = func([]BinlogEvent) error {
		nativeAppendHook = nil
		return errors.New("injected native one-phase append failure")
	}
	t.Cleanup(func() { nativeAppendHook = previous })

	changes := []RowChange{{
		Table:  "app.docs",
		Action: "insert",
		After:  map[string]interface{}{"id": int64(20)},
	}}
	err = source.AppendOnePhaseXATransaction("one-phase-retry-key", xid, changes, nil)
	require.ErrorContains(t, err, "injected native one-phase append failure")

	require.NoError(t, source.AppendOnePhaseXATransaction("one-phase-retry-key", xid, changes, nil))

	logical, err := source.Writer.ReadFrom(4)
	require.NoError(t, err)
	prepareEvents := 0
	for _, event := range logical {
		if event.Type == EventXAPrepare && event.OnePhase {
			prepareEvents++
		}
	}
	require.Equal(t, 1, prepareEvents)

	native, err := source.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	decoded, err := NewNativeBinlogDecoder().DecodeTransactions(native)
	require.NoError(t, err)
	require.Len(t, decoded, 1)
}

func TestSourceKeyedCommitReturnsNewlyAppendedEvents(t *testing.T) {
	dir := t.TempDir()
	source, err := NewSource(dir, "native-keyed-events-source", 19)
	require.NoError(t, err)

	changes := []RowChange{{
		Table:  "app.docs",
		Action: "insert",
		After:  map[string]interface{}{"id": int64(19)},
	}}
	events, err := source.AppendCommittedTransactionWithKey("events-key", changes, nil)
	require.NoError(t, err)
	require.Len(t, events, 3, "keyed commit must return BEGIN/ROW/COMMIT for the new transaction")
	require.Equal(t, EventBegin, events[0].Type)
	require.Equal(t, EventCommit, events[len(events)-1].Type)

	duplicate, err := source.AppendCommittedTransactionWithKey("events-key", changes, nil)
	require.NoError(t, err)
	require.Empty(t, duplicate, "idempotent retry must not return or append a second transaction")
}

func TestSourceXARetryAfterNativeAppendFailureDoesNotDuplicateTerminal(t *testing.T) {
	dir := t.TempDir()
	source, err := NewSource(dir, "native-xa-retry-source", 18)
	require.NoError(t, err)
	xid := XAIdentity{GTRID: "native-xa-retry", BQUAL: "branch", FormatID: 18}
	require.NoError(t, source.PrepareXATransaction("xa-retry-key", xid, nil, nil))

	previous := nativeAppendHook
	nativeAppendHook = func([]BinlogEvent) error {
		nativeAppendHook = nil
		return errors.New("injected native XA append failure")
	}
	t.Cleanup(func() { nativeAppendHook = previous })

	err = source.CommitXATransaction("xa-retry-key", xid)
	require.ErrorContains(t, err, "injected native XA append failure")

	require.NoError(t, source.CommitXATransaction("xa-retry-key", xid))

	logical, err := source.Writer.ReadFrom(4)
	require.NoError(t, err)
	terminals := 0
	for _, event := range logical {
		if event.Type == EventXACommit {
			terminals++
		}
	}
	require.Equal(t, 1, terminals)
}

func TestSourcePrepareXARetryAfterNativeAppendFailureDoesNotDuplicatePrepare(t *testing.T) {
	source, err := NewSource(t.TempDir(), "22334455-6677-8899-aabb-ccddeeff0011", 25)
	require.NoError(t, err)
	xid := XAIdentity{GTRID: "native-prepare-retry", BQUAL: "branch", FormatID: 25}
	changes := []RowChange{{
		Table:   "app.docs",
		Action:  "insert",
		Columns: []string{"id"},
		After:   map[string]interface{}{"id": int64(25)},
	}}

	previous := nativeAppendHook
	nativeAppendHook = func([]BinlogEvent) error {
		nativeAppendHook = nil
		return errors.New("injected native XA prepare append failure")
	}
	t.Cleanup(func() { nativeAppendHook = previous })

	require.ErrorContains(t, source.PrepareXATransaction("prepare-retry-key", xid, changes, nil), "injected native XA prepare append failure")
	require.NoError(t, source.PrepareXATransaction("prepare-retry-key", xid, changes, nil))

	logical, err := source.Writer.ReadFrom(4)
	require.NoError(t, err)
	prepareEvents := 0
	for _, event := range logical {
		if event.Type == EventXAPrepare && !event.OnePhase {
			prepareEvents++
		}
	}
	require.Equal(t, 1, prepareEvents)
	require.Contains(t, source.preparedXA, xid.Key())

	require.NoError(t, source.CommitXATransaction("prepare-retry-key", xid))
	native, err := source.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	decoded, err := NewNativeBinlogDecoder().DecodeTransactions(native)
	require.NoError(t, err)
	require.Len(t, decoded, 1)
}
