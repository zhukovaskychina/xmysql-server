package protocol

import "github.com/zhukovaskychina/xmysql-server/util"

type OK struct {
	MySQLPacket
	PacketType   byte
	AffectedRows uint64
	InsertID     uint64
	ServerStatus uint16
	WarningNum   uint16
	ServerMsg    []byte
}

func DecodeOk(buff []byte) OK {
	var cursor int
	ok := new(OK)
	cursor, ok.PacketType = util.ReadByte(buff, 0)
	cursor, ok.AffectedRows = util.ReadLength(buff, cursor)
	cursor, ok.InsertID = util.ReadLength(buff, cursor)
	cursor, ok.ServerStatus = util.ReadUB2(buff, cursor)
	cursor, ok.WarningNum = util.ReadUB2(buff, cursor)
	return *ok
}

func EncodeOK(buff []byte, affectedRows int64, insertId int64, message []byte) []byte {
	buff = util.WriteUB3(buff, uint32(CalOKPacketSize(affectedRows, insertId, message)))
	buff = util.WriteByte(buff, 0)
	buff = util.WriteByte(buff, 0x00)
	buff = util.WriteLength(buff, affectedRows)
	buff = util.WriteLength(buff, insertId)
	buff = util.WriteUB2(buff, 2)
	buff = util.WriteUB2(buff, 0)
	if len(message) > 0 {
		buff = util.WriteWithLength(buff, message)
	}
	return buff
}

func CalOKPacketSize(affectedRows int64, insertId int64, message []byte) int {
	var i = 1

	i += util.GetLength(affectedRows)
	i += util.GetLength(insertId)
	i += 4
	if len(message) > 0 {
		i += util.GetLengthBytes(message)
	}
	return i
}
