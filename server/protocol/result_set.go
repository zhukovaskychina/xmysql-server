package protocol

import "xmysql-server/util"

type ResultSetHeaderPacket struct {
	MySQLPacket
	FieldCount int
	Extra      int64
	PacketId   byte
}

func (rs *ResultSetHeaderPacket) CalRHPSize() int {
	size := util.GetLength(int64(rs.FieldCount))
	if rs.Extra > 0 {
		size += util.GetLength(rs.Extra)
	}
	return size
}

func (rs *ResultSetHeaderPacket) EncodeBuff() []byte {
	buff := make([]byte, 0)
	buff = util.WriteUB3(buff, uint32(rs.CalRHPSize()))
	buff = util.WriteByte(buff, rs.PacketId)
	buff = util.WriteLength(buff, int64(rs.FieldCount))
	if rs.Extra > 0 {
		buff = util.WriteLength(buff, rs.Extra)
	}
	return buff
}

func GetResultSetHeaderPacket(fieldCount int) *ResultSetHeaderPacket {
	rp := new(ResultSetHeaderPacket)
	rp.FieldCount = fieldCount
	rp.Extra = 0
	rp.PacketId = 1
	return rp
}
