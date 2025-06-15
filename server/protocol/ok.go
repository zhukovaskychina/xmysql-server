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
	// 计算包体长度
	packetSize := CalOKPacketSize(affectedRows, insertId, message)

	// 写入包长度 (3字节，小端序)
	buff = util.WriteUB3(buff, uint32(packetSize))

	// 写入序列号 (1字节) - 响应客户端查询，序列号为1
	buff = util.WriteByte(buff, 1)

	// OK包标识符 (0x00)
	buff = util.WriteByte(buff, 0x00)

	// 受影响的行数 (length-encoded integer)
	buff = util.WriteLength(buff, affectedRows)

	// 最后插入的ID (length-encoded integer)
	buff = util.WriteLength(buff, insertId)

	// 服务器状态标志 (2字节，小端序)
	// SERVER_STATUS_AUTOCOMMIT = 0x0002
	buff = util.WriteUB2(buff, 2)

	// 警告数量 (2字节，小端序)
	buff = util.WriteUB2(buff, 0)

	// 可选的info字符串
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
