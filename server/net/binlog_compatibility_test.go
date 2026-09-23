package net

import (
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func TestBinlogDumpStreamsCommittedEventsFromConfiguredSource(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "source-uuid", 1)
	require.NoError(t, err)
	_, err = source.Append(1, []replication.RowChange{{Table: "app.docs", Action: "insert"}})
	require.NoError(t, err)
	session := NewMockSession("binlog")
	session.SetAttribute("replication_source", source)
	body := make([]byte, 11)
	body[0] = 0x12
	binary.LittleEndian.PutUint32(body[1:5], 4)
	binary.LittleEndian.PutUint16(body[5:7], binlogDumpNonBlockFlag)
	require.NoError(t, dumpBinlogEvents(session, body))
	require.Len(t, session.written, 6)
	expectedTypes := []byte{15, 35, 33, 19, 30, 16} // FDE, PREVIOUS_GTIDS, GTID_EVENT, TABLE_MAP_EVENT, WRITE_ROWS_EVENTv2, XID_EVENT
	for i, packet := range session.written {
		require.Greater(t, len(packet), 5)
		require.Equal(t, expectedTypes[i], packet[5+4])
	}
}

type closingBinlogSession struct {
	*MockSession
	checks int
}

func (s *closingBinlogSession) IsClosed() bool {
	s.checks++
	return s.checks > 1
}

func TestBinlogDumpBlockingWaitsForNewEventsUntilSessionCloses(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "source-uuid", 1)
	require.NoError(t, err)
	session := &closingBinlogSession{MockSession: NewMockSession("binlog-blocking")}
	session.SetAttribute("replication_source", source)
	body := make([]byte, 11)
	body[0] = 0x12
	binary.LittleEndian.PutUint32(body[1:5], 4)

	require.NoError(t, dumpBinlogEvents(session, body))
	require.Len(t, session.written, 2, "blocking dump must not send an OK packet while waiting")
	require.Equal(t, byte(15), session.written[0][9], "the stream starts with the format description event")
}

func TestBinlogDumpRejectsTruncatedLegacyRequest(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "source-uuid", 1)
	require.NoError(t, err)
	session := NewMockSession("binlog-truncated")
	session.SetAttribute("replication_source", source)
	require.NoError(t, dumpBinlogEvents(session, []byte{common.COM_BINLOG_DUMP, 1, 0, 0, 0}))
	require.Len(t, session.written, 1)
	require.Equal(t, byte(0xff), session.written[0][4])
}

func TestBinlogDumpRejectsTruncatedGTIDRequest(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "source-uuid", 1)
	require.NoError(t, err)
	session := NewMockSession("binlog-gtid-truncated")
	session.SetAttribute("replication_source", source)
	require.NoError(t, dumpBinlogEvents(session, []byte{common.COM_BINLOG_DUMP_GTID}))
	require.Len(t, session.written, 1)
	require.Equal(t, byte(0xff), session.written[0][4])
}

func TestRegisterSlaveStoresSafeRegistrationMetadata(t *testing.T) {
	body := []byte{common.COM_REGISTER_SLAVE, 0x2a, 0, 0, 0, 9}
	body = append(body, []byte("replica-1")...)
	body = append(body, 4)
	body = append(body, []byte("user")...)
	body = append(body, 6)
	body = append(body, []byte("secret")...)
	body = append(body, 0x29, 0x0c) // report port 3113
	body = append(body, 0, 0, 0, 0) // replication rank
	body = append(body, 0, 0, 0, 0) // master id
	session := NewMockSession("register-slave")
	registry := replication.NewReplicaRegistry()
	handler := &DecoupledMySQLMessageHandler{replicaRegistry: registry}
	packet := &MySQLPackage{Body: body}
	packet.Header.PacketId = 7
	require.NoError(t, handler.handleRegisterSlave(session, packet))
	require.Equal(t, true, session.GetAttribute("replication_slave_registered"))
	require.Equal(t, uint32(42), session.GetAttribute("replication_slave_server_id"))
	require.Equal(t, "replica-1", session.GetAttribute("replication_slave_report_host"))
	require.Equal(t, "user", session.GetAttribute("replication_slave_report_user"))
	require.Equal(t, uint16(3113), session.GetAttribute("replication_slave_report_port"))
	_, hasPassword := session.GetAttribute("replication_slave_report_password").(string)
	require.False(t, hasPassword)
	require.Len(t, session.written, 1)
	require.Equal(t, byte(0x00), session.written[0][4])
	require.Equal(t, byte(8), session.written[0][3])
	require.Len(t, registry.Snapshot(), 1)
	require.Equal(t, uint32(42), registry.Snapshot()[0].ServerID)
	handler.OnClose(session)
	require.Empty(t, registry.Snapshot())
}

func TestRegisterSlaveRejectsTruncatedPacket(t *testing.T) {
	session := NewMockSession("register-slave-invalid")
	handler := &DecoupledMySQLMessageHandler{}
	packet := &MySQLPackage{Body: []byte{common.COM_REGISTER_SLAVE, 1, 0}}
	require.NoError(t, handler.handleRegisterSlave(session, packet))
	require.Len(t, session.written, 1)
	require.Equal(t, byte(0xff), session.written[0][4])
}

func TestBinlogDumpUsesNativeEventHeaderAndQueryEvent(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "source-uuid", 1)
	require.NoError(t, err)
	_, err = source.AppendTransaction(1, nil, []replication.Statement{{Database: "app", SQL: "insert into docs values (1)"}})
	require.NoError(t, err)
	session := NewMockSession("native-binlog")
	session.SetAttribute("replication_source", source)
	body := make([]byte, 11)
	body[0] = 0x12
	binary.LittleEndian.PutUint32(body[1:5], 4)
	binary.LittleEndian.PutUint16(body[5:7], binlogDumpNonBlockFlag)
	require.NoError(t, dumpBinlogEvents(session, body))
	require.Len(t, session.written, 5)

	expectedTypes := []byte{15, 35, 33, 2, 16} // FDE, PREVIOUS_GTIDS, GTID_EVENT, QUERY_EVENT, XID_EVENT
	for i, packet := range session.written {
		require.GreaterOrEqual(t, len(packet), 4+1+19)
		event := packet[5:] // packet header + binlog stream marker
		require.Equal(t, expectedTypes[i], event[4])
		require.Equal(t, uint32(len(event)), binary.LittleEndian.Uint32(event[9:13]))
	}
}

func TestBinlogDumpUsesNativeRowEventsForLogicalChanges(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "source-uuid", 1)
	require.NoError(t, err)
	_, err = source.Append(1, []replication.RowChange{{Table: "app.docs", Action: "insert", After: map[string]interface{}{"id": int64(1), "title": "hello"}}})
	require.NoError(t, err)
	session := NewMockSession("native-row-binlog")
	session.SetAttribute("replication_source", source)
	body := make([]byte, 11)
	body[0] = 0x12
	binary.LittleEndian.PutUint32(body[1:5], 4)
	binary.LittleEndian.PutUint16(body[5:7], binlogDumpNonBlockFlag)
	require.NoError(t, dumpBinlogEvents(session, body))
	require.Len(t, session.written, 6)
	expectedTypes := []byte{15, 35, 33, 19, 30, 16} // FDE, PREVIOUS_GTIDS, GTID, TABLE_MAP, WRITE_ROWSv2, XID
	for i, packet := range session.written {
		require.GreaterOrEqual(t, len(packet), 4+1+19)
		event := packet[5:]
		require.Equal(t, expectedTypes[i], event[4])
		require.Equal(t, uint32(len(event)), binary.LittleEndian.Uint32(event[9:13]))
	}
}

func TestBinlogDumpSplitsOversizedNativeEvents(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "source-uuid", 1)
	require.NoError(t, err)
	largeSQL := "insert into docs values ('" + strings.Repeat("x", protocol.MaxPacketSize+1024) + "')"
	_, err = source.AppendCommitted([]replication.Statement{{Database: "app", SQL: largeSQL}})
	require.NoError(t, err)

	session := NewMockSession("native-large-binlog")
	session.SetAttribute("replication_source", source)
	body := make([]byte, 11)
	body[0] = common.COM_BINLOG_DUMP
	binary.LittleEndian.PutUint32(body[1:5], 4)
	binary.LittleEndian.PutUint16(body[5:7], binlogDumpNonBlockFlag)
	require.NoError(t, dumpBinlogEvents(session, body))
	// FDE/PREVIOUS/GTID/XID are single packets; the oversized QUERY_EVENT
	// contributes multiple packets with contiguous sequence IDs.
	require.Greater(t, len(session.written), 5)
	queryPackets := session.written[3 : len(session.written)-1]
	require.Greater(t, len(queryPackets), 1)
	for index, packet := range queryPackets {
		require.Equal(t, byte(3+index), packet[3])
	}
	require.Equal(t, byte(16), session.written[len(session.written)-1][5+4])
}

func TestBinlogDumpUsesRequestedRotatedFile(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "source-uuid", 1)
	require.NoError(t, err)
	_, err = source.AppendCommitted([]replication.Statement{{Database: "app", SQL: "insert into docs values (1)"}})
	require.NoError(t, err)
	_, err = source.Rotate()
	require.NoError(t, err)
	_, err = source.AppendCommitted([]replication.Statement{{Database: "app", SQL: "insert into docs values (2)"}})
	require.NoError(t, err)

	session := NewMockSession("native-rotated-binlog")
	session.SetAttribute("replication_source", source)
	body := make([]byte, 11+len("binlog.000002"))
	body[0] = common.COM_BINLOG_DUMP
	binary.LittleEndian.PutUint32(body[1:5], 4)
	binary.LittleEndian.PutUint16(body[5:7], binlogDumpNonBlockFlag)
	binary.LittleEndian.PutUint32(body[7:11], 99)
	copy(body[11:], "binlog.000002")
	require.NoError(t, dumpBinlogEvents(session, body))
	require.Len(t, session.written, 5)
	expectedTypes := []byte{15, 35, 33, 2, 16} // FDE, PREVIOUS_GTIDS, GTID, QUERY_EVENT, XID_EVENT
	for index, packet := range session.written {
		require.Equal(t, expectedTypes[index], packet[9])
	}
}

func TestBlockingBinlogDumpCanFindNextRotatedFile(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "source-uuid", 1)
	require.NoError(t, err)
	_, err = source.Rotate()
	require.NoError(t, err)
	next, ok := nextNativeBinlogFile(source, "binlog.000001")
	require.True(t, ok)
	require.Equal(t, "binlog.000002", next)
	_, ok = nextNativeBinlogFile(source, "binlog.000002")
	require.False(t, ok)
}

func TestBinlogDumpStreamsPhysicalNativePositions(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "source-uuid", 1)
	require.NoError(t, err)
	_, err = source.AppendCommitted([]replication.Statement{{Database: "app", SQL: "insert into docs values (1)"}})
	require.NoError(t, err)
	_, err = source.AppendCommitted([]replication.Statement{{Database: "app", SQL: "insert into docs values (2)"}})
	require.NoError(t, err)
	native, err := source.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	secondGTID := 0
	for index, event := range native {
		if event.Type == 33 {
			secondGTID++
			if secondGTID == 2 {
				startPosition := event.Position
				session := NewMockSession("native-physical-position")
				session.SetAttribute("replication_source", source)
				body := make([]byte, 11)
				body[0] = common.COM_BINLOG_DUMP
				binary.LittleEndian.PutUint32(body[1:5], uint32(startPosition))
				binary.LittleEndian.PutUint16(body[5:7], binlogDumpNonBlockFlag)
				binary.LittleEndian.PutUint32(body[7:11], 99)
				require.NoError(t, dumpBinlogEvents(session, body))
				require.Len(t, session.written, 3)
				for packetIndex, packet := range session.written {
					raw := packet[5:]
					require.Equal(t, native[index+packetIndex].EndPosition, uint64(binary.LittleEndian.Uint32(raw[13:17])))
				}
				return
			}
		}
	}
	t.Fatal("second GTID event not found")
}

func TestNativeRowEventsUseActionSpecificTypes(t *testing.T) {
	for _, test := range []struct {
		action string
		typeID byte
	}{
		{action: "update", typeID: 31},
		{action: "delete", typeID: 32},
	} {
		t.Run(test.action, func(t *testing.T) {
			event := replication.BinlogEvent{
				Type: replication.EventRow,
				Changes: []replication.RowChange{{
					Table:  "app.docs",
					Action: test.action,
					Before: map[string]interface{}{"id": int64(1), "title": "old"},
					After:  map[string]interface{}{"id": int64(1), "title": "new"},
				}},
			}
			payloads, err := encodeNativeBinlogEventPayloads(event)
			require.NoError(t, err)
			require.Len(t, payloads, 2)
			require.Equal(t, byte(19), payloads[0][4])
			require.Equal(t, test.typeID, payloads[1][4])
		})
	}
}

func TestNativeRowPayloadsUseReplicationSchemaAwareCodec(t *testing.T) {
	event := replication.BinlogEvent{
		Type: replication.EventRow,
		Changes: []replication.RowChange{{
			Table: "app.docs", Action: "insert", Columns: []string{"payload"},
			ColumnTypes: map[string]string{"payload": "JSON"},
			After:       map[string]interface{}{"payload": `{"ok":true}`},
		}},
	}
	payloads, err := encodeNativeBinlogEventPayloads(event)
	require.NoError(t, err)
	require.Len(t, payloads, 2)
	require.Contains(t, payloads[0], byte(245), "TABLE_MAP must preserve JSON type metadata")
	require.Contains(t, payloads[1], byte(4), "row image must contain binary JSON boolean payload")
}

func TestNativeGTIDAndXIDEventsCarryTransactionIdentity(t *testing.T) {
	gtid := replication.GTID{UUID: "00112233-4455-6677-8899-aabbccddeeff", Seq: 7}
	begin, err := encodeNativeBinlogEvent(replication.BinlogEvent{Type: replication.EventBegin, ServerID: 9, Position: 10, GTID: gtid})
	require.NoError(t, err)
	require.Equal(t, byte(33), begin[4])
	require.Equal(t, uint32(len(begin)), binary.LittleEndian.Uint32(begin[9:13]))
	require.Equal(t, uint64(7), binary.LittleEndian.Uint64(begin[19+1+16:19+1+16+8]))

	commit, err := encodeNativeBinlogEvent(replication.BinlogEvent{Type: replication.EventCommit, ServerID: 9, Position: 12, GTID: gtid})
	require.NoError(t, err)
	require.Equal(t, byte(16), commit[4])
	require.Equal(t, uint64(7), binary.LittleEndian.Uint64(commit[19:27]))
}

func TestNativeBinlogEventsCarryCRC32Checksum(t *testing.T) {
	event, err := encodeNativeBinlogEvent(replication.BinlogEvent{
		Type: replication.EventBegin, ServerID: 9, Position: 10,
		GTID: replication.GTID{UUID: "00112233-4455-6677-8899-aabbccddeeff", Seq: 7},
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(event), 19+4)
	require.Equal(t, uint32(len(event)), binary.LittleEndian.Uint32(event[9:13]))
	want := crc32.ChecksumIEEE(event[:len(event)-4])
	require.Equal(t, want, binary.LittleEndian.Uint32(event[len(event)-4:]))
}

func TestBinlogDumpGTIDReadsNativePositionField(t *testing.T) {
	body := make([]byte, 1+2+4+4+len("binlog.000001")+8+4)
	body[0] = 0x1e // COM_BINLOG_DUMP_GTID
	offset := 1 + 2 + 4
	binary.LittleEndian.PutUint32(body[offset:offset+4], uint32(len("binlog.000001")))
	offset += 4
	copy(body[offset:], "binlog.000001")
	offset += len("binlog.000001")
	binary.LittleEndian.PutUint64(body[offset:offset+8], 123)
	require.Equal(t, uint64(123), binlogDumpStartPosition(body))
}

func TestBinlogDumpGTIDDecodesHalfOpenIntervals(t *testing.T) {
	uuid := "00112233-4455-6677-8899-aabbccddeeff"
	sid, err := hex.DecodeString("00112233445566778899aabbccddeeff")
	require.NoError(t, err)
	gtidPayload := make([]byte, 4+16+8+16)
	binary.LittleEndian.PutUint32(gtidPayload[0:4], 1)
	copy(gtidPayload[4:20], sid)
	binary.LittleEndian.PutUint64(gtidPayload[20:28], 1)
	binary.LittleEndian.PutUint64(gtidPayload[28:36], 2)
	binary.LittleEndian.PutUint64(gtidPayload[36:44], 5)
	body := make([]byte, 1+2+4+4+len("binlog.000001")+8+4+len(gtidPayload))
	body[0] = 0x1e
	binary.LittleEndian.PutUint16(body[1:3], binlogDumpNonBlockFlag)
	offset := 1 + 2 + 4
	binary.LittleEndian.PutUint32(body[offset:offset+4], uint32(len("binlog.000001")))
	offset += 4
	copy(body[offset:], "binlog.000001")
	offset += len("binlog.000001")
	binary.LittleEndian.PutUint64(body[offset:offset+8], 4)
	offset += 8
	binary.LittleEndian.PutUint32(body[offset:offset+4], uint32(len(gtidPayload)))
	offset += 4
	copy(body[offset:], gtidPayload)

	set, err := binlogDumpGTIDSet(body)
	require.NoError(t, err)
	require.True(t, set.Contains(replication.GTID{UUID: uuid, Seq: 2}))
	require.True(t, set.Contains(replication.GTID{UUID: uuid, Seq: 4}))
	require.False(t, set.Contains(replication.GTID{UUID: uuid, Seq: 5}))
	intervals, err := binlogDumpGTIDIntervals(body)
	require.NoError(t, err)
	require.Equal(t, []replication.GTIDInterval{{Start: 2, End: 4}}, intervals[uuid])
}

func TestBinlogDumpGTIDKeepsLargeIntervalsCompact(t *testing.T) {
	uuid := "00112233-4455-6677-8899-aabbccddeeff"
	sid, err := hex.DecodeString("00112233445566778899aabbccddeeff")
	require.NoError(t, err)
	intervalEnd := uint64(1 << 62)
	gtidPayload := make([]byte, 4+16+8+16)
	binary.LittleEndian.PutUint32(gtidPayload[0:4], 1)
	copy(gtidPayload[4:20], sid)
	binary.LittleEndian.PutUint64(gtidPayload[20:28], 1)
	binary.LittleEndian.PutUint64(gtidPayload[28:36], 1)
	binary.LittleEndian.PutUint64(gtidPayload[36:44], intervalEnd)
	body := make([]byte, 1+2+4+4+len("binlog.000001")+8+4+len(gtidPayload))
	body[0] = 0x1e
	binary.LittleEndian.PutUint16(body[1:3], binlogDumpNonBlockFlag)
	offset := 1 + 2 + 4
	binary.LittleEndian.PutUint32(body[offset:offset+4], uint32(len("binlog.000001")))
	offset += 4
	copy(body[offset:], "binlog.000001")
	offset += len("binlog.000001")
	binary.LittleEndian.PutUint64(body[offset:offset+8], 4)
	offset += 8
	binary.LittleEndian.PutUint32(body[offset:offset+4], uint32(len(gtidPayload)))
	offset += 4
	copy(body[offset:], gtidPayload)

	intervals, err := binlogDumpGTIDIntervals(body)
	require.NoError(t, err)
	require.Equal(t, []replication.GTIDInterval{{Start: 1, End: intervalEnd - 1}}, intervals[uuid])
	require.True(t, intervals.Contains(replication.GTID{UUID: uuid, Seq: intervalEnd - 1}))
}

func TestBinlogDumpGTIDFiltersCommittedTransaction(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "00112233-4455-6677-8899-aabbccddeeff", 1)
	require.NoError(t, err)
	_, err = source.AppendCommitted([]replication.Statement{{Database: "app", SQL: "insert into t values (1)"}})
	require.NoError(t, err)
	_, err = source.AppendCommitted([]replication.Statement{{Database: "app", SQL: "insert into t values (2)"}})
	require.NoError(t, err)

	sid, err := hex.DecodeString("00112233445566778899aabbccddeeff")
	require.NoError(t, err)
	gtidPayload := make([]byte, 4+16+8+16)
	binary.LittleEndian.PutUint32(gtidPayload[0:4], 1)
	copy(gtidPayload[4:20], sid)
	binary.LittleEndian.PutUint64(gtidPayload[20:28], 1)
	binary.LittleEndian.PutUint64(gtidPayload[28:36], 1)
	binary.LittleEndian.PutUint64(gtidPayload[36:44], 2)
	body := make([]byte, 1+2+4+4+len("binlog.000001")+8+4+len(gtidPayload))
	body[0] = 0x1e
	binary.LittleEndian.PutUint16(body[1:3], binlogDumpNonBlockFlag)
	offset := 1 + 2 + 4
	binary.LittleEndian.PutUint32(body[offset:offset+4], uint32(len("binlog.000001")))
	offset += 4
	copy(body[offset:], "binlog.000001")
	offset += len("binlog.000001")
	binary.LittleEndian.PutUint64(body[offset:offset+8], 4)
	offset += 8
	binary.LittleEndian.PutUint32(body[offset:offset+4], uint32(len(gtidPayload)))
	offset += 4
	copy(body[offset:], gtidPayload)

	session := NewMockSession("gtid-filter")
	session.SetAttribute("replication_source", source)
	require.NoError(t, dumpBinlogEvents(session, body))
	require.Len(t, session.written, 5, "FDE/PREVIOUS_GTIDS plus the non-executed transaction must be streamed")
}
