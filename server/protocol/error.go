package protocol

import "github.com/zhukovaskychina/xmysql-server/util"

var (
	FieldCount = byte(0xFF)

	SqlstateMarker = byte('#')

	DefaultSqlstate = []byte("HY000")
)

type ErrorPacket struct {
	MySQLPacket
	message    []byte
	errorNo    byte
	sqlState   []byte
	fieldCount byte
	mark       byte
}

func (ep *ErrorPacket) InitErrorPacket() {
	ep.message = make([]byte, 0)
	ep.mark = SqlstateMarker
	ep.fieldCount = FieldCount
	ep.sqlState = DefaultSqlstate
	ep.errorNo = 0
}

func (ep *ErrorPacket) CalculateErrorPacketSize() int {

	size := 9
	if len(ep.message) != 0 {
		size += len(ep.message)
	}
	return size
}

func (ep *ErrorPacket) EncodeErrorPacket(errorMsg string) []byte {
	ep.message = []byte(errorMsg)
	buff := make([]byte, 0)
	buff = util.WriteUB3(buff, uint32(ep.CalculateErrorPacketSize()))
	buff = util.WriteByte(buff, 0)
	buff = util.WriteByte(buff, fieldCount)
	buff = util.WriteUB2(buff, uint16(ep.errorNo))
	buff = util.WriteByte(buff, ep.mark)
	buff = util.WriteBytes(buff, ep.sqlState)
	if ep.message != nil || len(ep.message) != 0 {
		buff = util.WriteBytes(buff, ep.message)
	}
	return buff
}
