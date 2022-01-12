package protocol

import (
	"container/list"
	"xmysql-server/util"
)

var (
	NULL_MARK = byte(251)
)

type RowDataPacket struct {
	MySQLPacket
	FieldCount int

	FieldValues *list.List

	FieldStrings *list.List

	PacketId byte
}

func NewRowDataPacket(fieldCount int) (rd *RowDataPacket) {
	rd = new(RowDataPacket)
	rd.FieldCount = fieldCount
	rd.FieldValues = new(list.List)
	rd.FieldStrings = new(list.List)
	return rd
}

func (rd *RowDataPacket) Add(value []byte) {
	rd.FieldValues.PushBack(value)
}

func (rd *RowDataPacket) CalculateFieldPacketSize() int {
	var size = 0

	for e := rd.FieldValues.Front(); e != nil; e = e.Next() {
		v := e.Value.([]byte)
		if v == nil {
			size = size + 1
		} else if len(v) == 0 {
			size = size + 1
		} else {
			size = util.GetLengthBytes(v)
		}
	}
	return size
}

func (rd *RowDataPacket) EncodeRowPacket() []byte {
	buff := make([]byte, 0)
	buff = util.WriteUB3(buff, uint32(rd.CalculateFieldPacketSize()))
	buff = util.WriteByte(buff, rd.PacketId)
	for e := rd.FieldValues.Front(); e != nil; e = e.Next() {
		v := e.Value.([]byte)
		if v == nil {
			buff = util.WriteByte(buff, NULL_MARK)
		} else if len(v) == 0 {
			buff = util.WriteByte(buff, NULL_MARK)
		} else {
			buff = util.WriteLength(buff, int64(len(v)))
			buff = util.WriteBytes(buff, v)
		}
	}
	return buff
}
