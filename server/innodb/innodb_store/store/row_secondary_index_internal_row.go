package store

import (
	"bytes"
	"encoding/binary"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/tuple"
	"github.com/zhukovaskychina/xmysql-server/util"
	"strings"
)

/***
########################################################################################################################
**/

//用于描述非叶子节点的记录

type SecondaryInternalRowHeader struct {
	basic.FieldDataHeader
	deleteFlag bool
	minRecFlag bool   //B+树非叶子项都会增加该标记
	nOwned     uint16 //槽位中最大的值有值，该槽位其他的行皆为0
	heapNo     uint16 //表示当前记录在页面中的相对位置
	recordType uint8  //0 表示普通记录，1表示B+树非叶子节点的目录项记录，2表示Infimum，3表示Supremum
	nextRecord uint16 //表示下一条记录相对位置
	Content    []byte //长度5个字节+长度列表，都是bit
}

func NewSecondaryInternalRowHeader() basic.FieldDataHeader {
	var clr = new(SecondaryInternalRowHeader)
	clr.deleteFlag = false
	clr.minRecFlag = false
	clr.nOwned = 1
	clr.heapNo = 0
	clr.nextRecord = 0
	clr.Content = []byte{util.ConvertBits2Byte("00000000")}
	clr.Content = append(clr.Content, util.ConvertBits2Bytes("0000000000000011")...)
	clr.Content = append(clr.Content, util.ConvertBits2Bytes("0000000000000000")...)
	return clr
}

func (cldr *SecondaryInternalRowHeader) SetDeleteFlag(delete bool) {
	if delete {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.DELETE_OFFSET, common.COMMON_TRUE)

	} else {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.DELETE_OFFSET, common.COMMON_FALSE)
	}
	cldr.deleteFlag = delete
}

func (cldr *SecondaryInternalRowHeader) GetDeleteFlag() bool {
	value := util.ReadBytesByIndexBit(cldr.Content[0], common.DELETE_OFFSET)
	if value == "1" {
		return true
	} else {
		return false
	}
}
func (cldr *SecondaryInternalRowHeader) GetRecMinFlag() bool {
	value := util.ReadBytesByIndexBit(cldr.Content[0], common.DELETE_OFFSET)
	if value == "1" {
		return true
	} else {
		return false
	}
}
func (cldr *SecondaryInternalRowHeader) SetRecMinFlag(flag bool) {
	if flag {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.MIN_REC_OFFSET, common.COMMON_TRUE)

	} else {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.MIN_REC_OFFSET, common.COMMON_FALSE)
	}
	cldr.minRecFlag = flag
}
func (cldr *SecondaryInternalRowHeader) SetNOwned(size byte) {
	cldr.Content[0] = util.ConvertBits2Byte(util.WriteBitsByStart(cldr.Content[0], util.TrimLeftPaddleBitString(size, 4), 4, 8))
	cldr.nOwned = uint16(size)
}
func (cldr *SecondaryInternalRowHeader) GetNOwned() byte {
	return util.LeftPaddleBitString(util.ReadBytesByIndexBitByStart(cldr.Content[0], 4, 8), 4)
}
func (cldr *SecondaryInternalRowHeader) GetHeapNo() uint16 {
	var heapNo = make([]string, 0)
	heapNo = append(heapNo, "0")
	heapNo = append(heapNo, "0")
	heapNo = append(heapNo, "0")
	heapNo = append(heapNo, util.ConvertByte2BitsString(cldr.Content[1])...)
	heapNo = append(heapNo, util.ConvertByte2BitsString(cldr.Content[2])[0:5]...)

	var result = util.ConvertBits2Bytes(strings.Join(heapNo, ""))

	bytesBuffer := bytes.NewBuffer(result)
	var rs uint16
	binary.Read(bytesBuffer, binary.BigEndian, &rs)
	return rs
}
func (cldr *SecondaryInternalRowHeader) SetHeapNo(heapNo uint16) {
	var result = util.ConvertUInt2Bytes(heapNo)
	resultArray := util.ConvertBytes2BitStrings(result)
	//取值
	cldr.Content[1] = util.ConvertString2Byte(strings.Join(resultArray[3:11], ""))
	cldr.Content[2] = util.ConvertString2Byte(util.WriteBitsByStart(cldr.Content[2], resultArray[11:16], 0, 5))
	cldr.nOwned = uint16(heapNo)
}
func (cldr *SecondaryInternalRowHeader) GetRecordType() uint8 {
	var recordType = make([]string, 0)
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, util.ConvertByte2BitsString(cldr.Content[2])[5:8]...)
	return uint8(util.ReadUB2Byte2Int(util.ConvertBits2Bytes(strings.Join(recordType, ""))))
}
func (cldr *SecondaryInternalRowHeader) SetRecordType(recordType uint8) {
	resultArray := util.ConvertByte2BitsString(recordType)
	cldr.Content[2] = util.ConvertString2Byte(util.WriteBitsByStart(cldr.Content[2], resultArray[5:8], 5, 8))
	cldr.recordType = recordType
}
func (cldr *SecondaryInternalRowHeader) GetNextRecord() uint16 {
	return util.ReadUB2Byte2Int(cldr.Content[3:5])
}
func (cldr *SecondaryInternalRowHeader) SetNextRecord(nextRecord uint16) {
	cldr.Content[3] = util.ConvertUInt2Bytes(nextRecord)[0]
	cldr.Content[4] = util.ConvertUInt2Bytes(nextRecord)[1]
}

func (cldr *SecondaryInternalRowHeader) GetRowHeaderLength() uint16 {
	return uint16(len(cldr.Content))
}

func (cldr *SecondaryInternalRowHeader) ToByte() []byte {
	return cldr.Content
}

type SecondaryInternalRowData struct {
	basic.FieldDataValue
	PrimaryKeyMeta *TableTupleMeta
	Content        []byte
}

func NewSecondaryInternalRowData(PrimaryKeyMeta *TableTupleMeta) basic.FieldDataValue {
	var clusterLeafRowData = new(SecondaryInternalRowData)
	clusterLeafRowData.Content = make([]byte, 0)
	clusterLeafRowData.PrimaryKeyMeta = PrimaryKeyMeta
	return clusterLeafRowData
}
func (cld *SecondaryInternalRowData) WriteBytesWithNull(content []byte) {
	cld.Content = util.WriteWithNull(cld.Content, content)
}

func (cld *SecondaryInternalRowData) GetPrimaryKey() basic.Value {
	//return cld.ReadBytesWithNullWithPosition(0)
	return nil
}

func (cld *SecondaryInternalRowData) GetRowDataLength() uint16 {
	return uint16(len(cld.Content))
}

func (cld *SecondaryInternalRowData) ToByte() []byte {
	return cld.Content
}

func (cld *SecondaryInternalRowData) ReadBytesWithNullWithPosition(index int) []byte {
	return cld.Content[0+5*index : 5*index+5][0:4]
}

//大致为  页面号/主键
type SecondaryInternalRow struct {
	basic.Row
	header    basic.FieldDataHeader
	value     basic.FieldDataValue
	TupleMeta tuple.TableRowTuple
}

func NewSecondaryInternalRow(prepareContent []byte, meta tuple.TableRowTuple) basic.Row {
	return &SecondaryInternalRow{
		Row:       nil,
		header:    NewClusterInternalRowHeader(meta),
		value:     NewClusterInternalRowData(meta),
		TupleMeta: meta,
	}
}

func (row *SecondaryInternalRow) Less(than basic.Row) bool {

	if than.IsSupremumRow() {
		return true
	}

	if than.IsInfimumRow() {
		return false
	}

	//thanPrimaryKey := row.GetPrimaryKey()
	//thisPrimaryKey := row.GetPrimaryKey()
	//
	//switch row.PrimaryKeyMeta.PrimaryKeyType {
	//case common.COLUMN_TYPE_TINY:
	//	{
	//
	//	}
	//case common.COLUMN_TYPE_STRING:
	//	{
	//		fmt.Println(string(thanPrimaryKey))
	//		fmt.Println(string(thisPrimaryKey))
	//
	//	}
	//case common.COLUMN_TYPE_VARCHAR:
	//	{
	//
	//	}
	//case common.COLUMN_TYPE_LONG:
	//	{
	//
	//	}
	//case common.COLUMN_TYPE_INT24:
	//	{
	//		var that = util.ReadUB4Byte2UInt32(thanPrimaryKey)
	//		var this = util.ReadUB4Byte2UInt32(thisPrimaryKey)
	//		if that > this {
	//			return true
	//		} else {
	//			return false
	//		}
	//	}
	//}
	return false

}

func (row *SecondaryInternalRow) ToByte() []byte {
	return append(row.header.ToByte(), row.value.ToByte()...)
}

func (row *SecondaryInternalRow) WriteWithNull(content []byte) {

	row.value.WriteBytesWithNull(content)
}

func (row *SecondaryInternalRow) GetRowLength() uint16 {

	return row.header.GetRowHeaderLength() + row.value.GetRowDataLength()
}

func (row *SecondaryInternalRow) GetPrimaryKey() basic.Value {
	return nil
}
func (row *SecondaryInternalRow) GetPageNumber() uint32 {
	return 0
}

func (row *SecondaryInternalRow) IsSupremumRow() bool {
	return false
}

func (row *SecondaryInternalRow) IsInfimumRow() bool {
	return false
}
