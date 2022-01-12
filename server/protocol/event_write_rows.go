package protocol

import "github.com/zhukovaskychina/xmysql-server/util"

type EventWriteRowsBody struct {
	TableID   uint64
	ColumnNum uint64
	BitSet    []int
}

type EventWriteRows struct {
	Header EventHeader
	Body   EventWriteRowsBody
}

func DecodeEventWriteRows(buf []byte) EventWriteRows {
	e := new(EventWriteRows)
	e.Header = DecodeEventHeader(buf)

	var cursor int = 20
	var extInfoLen uint16 = 0
	cursor, e.Body.TableID = util.ReadUB6(buf, cursor)
	cursor, _ = util.ReadUB2(buf, cursor)
	cursor, extInfoLen = util.ReadUB2(buf, cursor)
	cursor, _ = util.ReadBytes(buf, cursor, int(extInfoLen-2))
	cursor, e.Body.ColumnNum = util.ReadLength(buf, cursor)
	cursor, e.Body.BitSet = util.ReadBitSet(buf, cursor, int(e.Body.ColumnNum), true)

	//fmt.Printf("=====================%+v\n", e)
	return EventWriteRows{}
}
