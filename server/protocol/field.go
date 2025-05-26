package protocol

import (
	"github.com/zhukovaskychina/xmysql-server/util"
)

var (
	DEFAULT_CATALOG = []byte("DEF")
	FILLER          = make([]byte, 2)
)

type FieldPacket struct {
	MySQLPacket
	CataLog      []byte
	PacketId     byte
	DBName       []byte
	TableName    []byte
	OrgTableName []byte
	Name         []byte
	OrgName      []byte
	CharsetIndex int
	Length       int64
	types        int
	flags        int
	Decimals     byte
	Definition   []byte
}

func NewFieldPacket() *FieldPacket {
	fieldPacket := new(FieldPacket)
	return fieldPacket
}

func (fp *FieldPacket) CalcPacketSize() int {

	var size = 0
	if fp.CataLog == nil {
		size = size + 1
	} else {
		size = size + util.GetLengthBytes(fp.CataLog)
	}

	if fp.DBName == nil {
		size = size + 1
	} else {
		size = size + util.GetLengthBytes(fp.DBName)
	}

	if fp.TableName == nil {
		size = size + 1
	} else {
		size = size + util.GetLengthBytes(fp.TableName)
	}

	if fp.OrgTableName == nil {
		size = size + 1
	} else {
		size = size + util.GetLengthBytes(fp.OrgTableName)
	}

	if fp.Name == nil {
		size = size + 1
	} else {
		size = size + util.GetLengthBytes(fp.Name)
	}

	if fp.OrgName == nil {
		size = size + 1
	} else {
		size = size + util.GetLengthBytes(fp.OrgName)
	}

	size += 13
	if fp.Definition != nil {
		size = size + util.GetLengthBytes(fp.Definition)
	}
	return size
}
func (fp *FieldPacket) EncodeFieldPacket() []byte {
	buff := make([]byte, 0)
	var nullValue byte = 0
	buff = util.WriteUB3(buff, uint32(fp.CalcPacketSize()))
	buff = util.WriteByte(buff, fp.PacketId)
	buff = util.WriteWithLengthWithNullValue(buff, fp.CataLog, byte(nullValue))
	buff = util.WriteWithLengthWithNullValue(buff, fp.DBName, nullValue)
	buff = util.WriteWithLengthWithNullValue(buff, fp.TableName, nullValue)
	buff = util.WriteWithLengthWithNullValue(buff, fp.OrgTableName, nullValue)
	buff = util.WriteWithLengthWithNullValue(buff, fp.Name, nullValue)
	buff = util.WriteWithLengthWithNullValue(buff, fp.OrgName, nullValue)

	buff = util.WriteByte(buff, 0x0C)
	buff = util.WriteUB2(buff, uint16(fp.CharsetIndex))
	buff = util.WriteUB4(buff, uint32(fp.Length))
	buff = util.WriteByte(buff, byte(fp.types&0xff))
	buff = util.WriteUB2(buff, uint16(fp.flags))
	buff = util.WriteByte(buff, fp.Decimals)
	buff = util.WriteBytes(buff, FILLER)

	if fp.Definition != nil {
		buff = util.WriteWithLength(buff, fp.Definition)
	}
	return buff
}

func GetField(name string, fieldType int) *FieldPacket {

	fieldPacket := new(FieldPacket)
	fieldPacket.Name = []byte(name)
	fieldPacket.CharsetIndex = 8
	fieldPacket.types = fieldType
	fieldPacket.PacketId = 0
	return fieldPacket
}
