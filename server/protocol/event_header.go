package protocol

import "xmysql-server/util"

type EventHeader struct {
	MySQLPacket
	EventCreateTime   uint32
	EventType         byte
	ServerID          uint32
	EventSize         uint32
	NextEventPosition uint32
	Mark              uint16
}

func DecodeEventHeader(buf []byte) EventHeader {
	var e = new(EventHeader)
	var c int = 0

	c, _ = util.ReadByte(buf, c)
	c, e.EventCreateTime = util.ReadUB4(buf, c)
	c, e.EventType = util.ReadByte(buf, c)
	c, e.ServerID = util.ReadUB4(buf, c)
	c, e.EventSize = util.ReadUB4(buf, c)
	c, e.NextEventPosition = util.ReadUB4(buf, c)
	c, e.Mark = util.ReadUB2(buf, c)
	return *e
}
