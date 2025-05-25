package protocol

import "xmysql-server/util"

var (
	fieldCount = byte(0xFE)
)

type EOFPacket struct {
	MySQLPacket
	FieldCount   byte
	WarningCount int
	Status       int
	PacketId     byte
}

func NewEOFPacket() EOFPacket {

	packet := EOFPacket{
		FieldCount:   fieldCount,
		WarningCount: 0,
		Status:       2,
	}
	return packet
}

func (eofPacket *EOFPacket) InitEofPacket() {
	eofPacket.FieldCount = fieldCount
	eofPacket.Status = 2
}

func (eofPacket *EOFPacket) WriteEOF() []byte {
	data := make([]byte, 0)
	data = util.WriteUB3(data, 5)
	data = util.WriteByte(data, eofPacket.PacketId)
	data = util.WriteByte(data, fieldCount)
	data = util.WriteUB2(data, uint16(eofPacket.WarningCount))
	data = util.WriteUB2(data, uint16(eofPacket.Status))
	return data
}
