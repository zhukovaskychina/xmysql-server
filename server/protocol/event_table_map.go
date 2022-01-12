package protocol

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/util"
)

type EventTableMapBody struct {
	MySQLPacket
	TableID           uint64
	DatabaseName      string
	TableName         string
	ColumnNum         uint64
	ColumnTypes       []byte
	MetaDataLength    uint64
	ColumnMetaData    []uint32
	ColumnNullability []int
}

type EventTableMap struct {
	Header EventHeader
	Body   EventTableMapBody
}

func DecodeEventTableMap(buf []byte) EventTableMap {
	e := new(EventTableMap)
	e.Header = DecodeEventHeader(buf)
	var c int = 20

	c, e.Body.TableID = util.ReadUB6(buf, c)
	c, _ = util.ReadBytes(buf, c, 3) //2个字节保留字段，1个字节的数据库名称长度
	c, e.Body.DatabaseName = util.ReadStringWithNull(buf, c)
	c, _ = util.ReadByte(buf, c) //表名长度
	c, e.Body.TableName = util.ReadStringWithNull(buf, c)
	c, e.Body.ColumnNum = util.ReadLength(buf, c)
	c, e.Body.ColumnTypes = util.ReadBytes(buf, c, int(e.Body.ColumnNum))
	c, e.Body.MetaDataLength = util.ReadLength(buf, c)
	c, e.Body.ColumnMetaData = readMetaData(buf, c, e.Body.ColumnTypes)
	c, e.Body.ColumnNullability = util.ReadBitSet(buf, c, int(e.Body.ColumnNum), true)
	return *e
}

func readMetaData(buf []byte, cursor int, columnTypes []byte) (int, []uint32) {
	metadata := make([]uint32, len(columnTypes))
	for i := 0; i < len(columnTypes); i++ {
		switch columnTypes[i] & 0xFF {
		case common.COLUMN_TYPE_FLOAT:
		case common.COLUMN_TYPE_DOUBLE:
		case common.COLUMN_TYPE_BLOB:
		case common.COLUMN_TYPE_JSON:
		case common.COLUMN_TYPE_GEOMETRY:
			var bt byte
			cursor, bt = util.ReadByte(buf, cursor)
			metadata[i] = uint32(bt)
			break
		case common.COLUMN_TYPE_BIT:
		case common.COLUMN_TYPE_VARCHAR:
		case common.COLUMN_TYPE_NEWDECIMAL:
			var tmp uint16
			cursor, tmp = util.ReadUB2(buf, cursor)
			metadata[i] = uint32(tmp)
			break
		case common.COLUMN_TYPE_SET:
		case common.COLUMN_TYPE_ENUM:
		case common.COLUMN_TYPE_STRING:
			var tmp []byte
			cursor, tmp = util.ReadBytes(buf, cursor, 2)
			metadata[i] = bigEndianInt(tmp)
			break
		case common.COLUMN_TYPE_TIME_V2:
		case common.COLUMN_TYPE_DATETIME_V2:
		case common.COLUMN_TYPE_TIMESTAMP_V2:
			var tmp byte
			cursor, tmp = util.ReadByte(buf, cursor)
			metadata[i] = uint32(tmp)
			break
		default:
			metadata[i] = 0
		}
	}

	return cursor, metadata
}

func bigEndianInt(buf []byte) uint32 {
	var result uint32 = 0
	for i := 0; i < len(buf); i++ {
		b := buf[i]
		if b >= 0 {
			result = (result << 8) | uint32(b)
		} else {
			result = (result << 8) | (uint32(b) + 256)
		}
	}
	return result
}
