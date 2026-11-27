package common

// Physical MySQL binlog event codes; independent of logical replication EventType.
const (
	BinlogEventUnknown            = 0
	BinlogEventStartV3            = 1
	BinlogEventQuery              = 2
	BinlogEventStop               = 3
	BinlogEventRotate             = 4
	BinlogEventIntvar             = 5
	BinlogEventLoad               = 6
	BinlogEventSlave              = 7
	BinlogEventCreateFile         = 8
	BinlogEventAppendBlock        = 9
	BinlogEventExecLoad           = 10
	BinlogEventDeleteFile         = 11
	BinlogEventNewLoad            = 12
	BinlogEventRand               = 13
	BinlogEventUserVar            = 14
	BinlogEventFormatDescription  = 15
	BinlogEventXid                = 16
	BinlogEventBeginLoadQuery     = 17
	BinlogEventExecuteLoadQuery   = 18
	BinlogEventTableMap           = 19
	BinlogEventWriteRowsPreGA     = 20
	BinlogEventUpdateRowsPreGA    = 21
	BinlogEventDeleteRowsPreGA    = 22
	BinlogEventWriteRowsV1        = 23
	BinlogEventUpdateRowsV1       = 24
	BinlogEventDeleteRowsV1       = 25
	BinlogEventIncident           = 26
	BinlogEventHeartbeat          = 27
	BinlogEventIgnorable          = 28
	BinlogEventRowsQuery          = 29
	BinlogEventWriteRowsV2        = 30
	BinlogEventUpdateRowsV2       = 31
	BinlogEventDeleteRowsV2       = 32
	BinlogEventGtid               = 33
	BinlogEventAnonymousGtid      = 34
	BinlogEventPreviousGtids      = 35
	BinlogEventTransactionContext = 36
	BinlogEventViewChange         = 37
	BinlogEventXaPrepare          = 38
	BinlogEventPartialUpdateRows  = 39
	BinlogEventTransactionPayload = 40
	BinlogEventHeartbeatV2        = 41
	BinlogEventGtidTagged         = 42
)

const (
	BinlogEventHeaderLength = 19
	BinlogChecksumLength    = 4
)

// BinlogPostHeaderLengths returns the MySQL 8.4 FORMAT_DESCRIPTION payload table.
// Entry eventType-1 describes that event; the last byte is the CRC32 algorithm.
// Each caller owns its copy so packet construction cannot mutate the directory.
func BinlogPostHeaderLengths() []byte {
	return []byte{
		0, 13, 0, 8, 0, 0, 0, 0, 4, 0, 4, 0, 0, 0, 99, 0,
		4, 26, 8, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 10, 10, 10,
		42, 42, 0, 18, 52, 0, 10, 40, 0, 0, 1,
	}
}
