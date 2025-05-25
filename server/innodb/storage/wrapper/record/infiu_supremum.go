package record

import (
	"encoding/binary"
	"github.com/smartystreets/assertions"
	"strings"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/basic"
	"xmysql-server/util"
)

type InfimumRow struct {
	basic.Row
	header basic.FieldDataHeader
	value  basic.FieldDataValue
}

type InfimumHeader struct {
	basic.FieldDataHeader
	deleteFlag bool
	minRecFlag bool   //B+树非叶子项都会增加该标记
	nOwned     uint16 //槽位中最大的值有值，该槽位其他的行皆为0
	heapNo     uint16 //表示当前记录在页面中的相对位置
	recordType uint8  //0 表示普通记录，1表示B+树非叶子节点的目录项记录，2表示Infimum，3表示Supremum
	nextRecord uint16 //表示下一条记录相对位置
	Content    []byte //长度5个字节+长度列表，都是bit
}

func NewInfimumHeader() basic.FieldDataHeader {
	var clr = new(InfimumHeader)
	clr.deleteFlag = false
	clr.minRecFlag = false
	clr.nOwned = 1
	clr.heapNo = 0
	clr.nextRecord = 0
	clr.Content = []byte{util.ConvertBits2Byte("00000000")}
	clr.Content = append(clr.Content, util.ConvertBits2Bytes("0000000000000010")...)
	clr.Content = append(clr.Content, util.ConvertBits2Bytes("0000000000000000")...)
	return clr
}

func ParseInfimumHeader(content []byte) basic.FieldDataHeader {
	var clr = new(InfimumHeader)

	clr.Content = make([]byte, 0)
	clr.Content = append(clr.Content, content...)
	return clr
}

func (cldr *InfimumHeader) SetDeleteFlag(delete bool) {
	if delete {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.DELETE_OFFSET, common.COMMON_TRUE)

	} else {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.DELETE_OFFSET, common.COMMON_FALSE)
	}
	cldr.deleteFlag = delete
}

func (cldr *InfimumHeader) GetDeleteFlag() bool {
	value := util.ReadBytesByIndexBit(cldr.Content[0], common.DELETE_OFFSET)
	if value == "1" {
		return true
	} else {
		return false
	}
}
func (cldr *InfimumHeader) GetRecMinFlag() bool {
	value := util.ReadBytesByIndexBit(cldr.Content[0], common.DELETE_OFFSET)
	if value == "1" {
		return true
	} else {
		return false
	}
}
func (cldr *InfimumHeader) SetRecMinFlag(flag bool) {
	if flag {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.MIN_REC_OFFSET, common.COMMON_TRUE)

	} else {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.MIN_REC_OFFSET, common.COMMON_FALSE)
	}
	cldr.minRecFlag = flag
}
func (cldr *InfimumHeader) SetNOwned(size byte) {
	cldr.Content[0] = util.ConvertBits2Byte(util.WriteBitsByStart(cldr.Content[0], util.TrimLeftPaddleBitString(size, 4), 4, 8))
	cldr.nOwned = uint16(size)
}
func (cldr *InfimumHeader) GetNOwned() byte {
	return util.LeftPaddleBitString(util.ReadBytesByIndexBitByStart(cldr.Content[0], 4, 8), 4)
}
func (cldr *InfimumHeader) GetHeapNo() uint16 {
	var heapNo = make([]string, 0)
	heapNo = append(heapNo, "0")
	heapNo = append(heapNo, "0")
	heapNo = append(heapNo, "0")
	heapNo = append(heapNo, util.ConvertByte2BitsString(cldr.Content[1])...)
	heapNo = append(heapNo, util.ConvertByte2BitsString(cldr.Content[2])[0:5]...)
	return binary.BigEndian.Uint16(util.ConvertBits2Bytes(strings.Join(heapNo, "")))
}
func (cldr *InfimumHeader) SetHeapNo(heapNo uint16) {
	var result = util.ConvertUInt2Bytes(heapNo)
	resultArray := util.ConvertBytes2BitStrings(result)
	//取值
	cldr.Content[1] = util.ConvertString2Byte(strings.Join(resultArray[3:11], ""))
	cldr.Content[2] = util.ConvertString2Byte(util.WriteBitsByStart(cldr.Content[2], resultArray[11:16], 0, 5))
	cldr.nOwned = uint16(heapNo)
}
func (cldr *InfimumHeader) GetRecordType() uint8 {
	var recordType = make([]string, 0)
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, util.ConvertByte2BitsString(cldr.Content[2])[5:8]...)
	return uint8(util.ReadUB2Byte2Int(util.ConvertBits2Bytes(strings.Join(recordType, ""))))
}
func (cldr *InfimumHeader) SetRecordType(recordType uint8) {
	resultArray := util.ConvertByte2BitsString(recordType)
	cldr.Content[2] = util.ConvertString2Byte(util.WriteBitsByStart(cldr.Content[2], resultArray[5:8], 5, 8))
	cldr.recordType = recordType
}
func (cldr *InfimumHeader) GetNextRecord() uint16 {
	return util.ReadUB2Byte2Int(cldr.Content[3:5])
}
func (cldr *InfimumHeader) SetNextRecord(nextRecord uint16) {
	cldr.Content[3] = util.ConvertUInt2Bytes(nextRecord)[0]
	cldr.Content[4] = util.ConvertUInt2Bytes(nextRecord)[1]
}

func (cldr *InfimumHeader) GetRowHeaderLength() uint16 {
	return uint16(len(cldr.Content))
}

func (cldr *InfimumHeader) ToByte() []byte {
	return cldr.Content
}

/*****************
########################################################################################################################
*********/

type InfimumValue struct {
	basic.FieldDataValue
	content []byte
}

func NewInfimumValue() basic.FieldDataValue {
	var infimumValue = new(InfimumValue)
	infimumValue.content = []byte{'i', 'n', 'f', 'i', 'm', 'u', 'm', 0}
	return infimumValue
}

func (i *InfimumValue) ToByte() []byte {
	return i.content
}

func (i *InfimumValue) WriteBytesWithNull(content []byte) {
	panic("implement me")
}

func (i *InfimumValue) GetPrimaryKey() basic.FiledDataBytes {
	panic("implement me")
}

func (i *InfimumValue) GetRowDataLength() uint16 {
	return uint16(len(i.content))
}

func NewInfimumRow() basic.Row {
	var ir = &InfimumRow{
		Row:    nil,
		header: NewInfimumHeader(),
		value:  NewInfimumValue(),
	}
	return ir
}

func NewInfimumRowByContent(content []byte) basic.Row {
	var ir = &InfimumRow{
		Row:    nil,
		header: ParseInfimumHeader(content[0:5]),
		value:  NewInfimumValue(),
	}

	return ir
}

func (ir *InfimumRow) Less(than basic.Row) bool {

	return true
}

func (ir *InfimumRow) IsSupremumRow() bool {
	return false
}

func (ir *InfimumRow) IsInfimumRow() bool {
	return true
}
func (ir *InfimumRow) GetRowLength() uint16 {

	return ir.header.GetRowHeaderLength() + ir.value.GetRowDataLength()
}

func (ir *InfimumRow) GetHeaderLength() uint16 {

	return ir.header.GetRowHeaderLength()
}

func (ir *InfimumRow) ToByte() []byte {
	return append(ir.header.ToByte(), ir.value.ToByte()...)
}

func (ir *InfimumRow) GetPageNumber() uint32 {
	panic("implement me")
}

func (ir *InfimumRow) WriteWithNull(content []byte) {
	panic("implement me")
}

func (ir *InfimumRow) WriteBytesWithNullWithsPos(content []byte, index byte) {
	panic("implement me")
}

func (ir *InfimumRow) GetPrimaryKey() basic.Value {
	panic("implement me")
}

func (ir *InfimumRow) GetFieldLength() int {
	panic("implement me")
}

func (ir *InfimumRow) ReadValueByIndex(index int) basic.Value {
	panic("implement me")
}

func (ir *InfimumRow) SetNOwned(cnt byte) {
	assertions.ShouldBeLessThanOrEqualTo(cnt, 8)
	ir.header.SetNOwned(cnt)
}

func (ir *InfimumRow) GetNOwned() byte {
	return ir.header.GetNOwned()
}

func (ir *InfimumRow) GetNextRowOffset() uint16 {

	return ir.header.GetNextRecord()
}

func (ir *InfimumRow) SetNextRowOffset(offset uint16) {
	ir.header.SetNextRecord(offset)
}

// 获取当前记录在本页面中的相对位置
func (ir *InfimumRow) GetHeapNo() uint16 {
	return ir.header.GetHeapNo()
}

// 设置该记录在
func (ir *InfimumRow) SetHeapNo(heapNo uint16) {
	ir.header.SetHeapNo(heapNo)
}

/*
******
########################################################################################################################

****
*/
type SupremumRow struct {
	basic.Row
	header basic.FieldDataHeader
	value  basic.FieldDataValue
}

type SupremumHeader struct {
	basic.FieldDataHeader
	deleteFlag bool
	minRecFlag bool   //B+树非叶子项都会增加该标记
	nOwned     uint16 //槽位中最大的值有值，该槽位其他的行皆为0
	heapNo     uint16 //表示当前记录在页面中的相对位置
	recordType uint8  //0 表示普通记录，1表示B+树非叶子节点的目录项记录，2表示Infimum，3表示Supremum
	nextRecord uint16 //表示下一条记录相对位置
	Content    []byte //长度5个字节+长度列表，都是bit
}

func NewSupremumHeader() basic.FieldDataHeader {
	var clr = new(SupremumHeader)
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

func ParseSupremumHeader(content []byte) basic.FieldDataHeader {
	var clr = new(SupremumHeader)

	clr.Content = make([]byte, 0)
	clr.Content = append(clr.Content, content...)
	return clr
}

func (cldr *SupremumHeader) SetDeleteFlag(delete bool) {
	if delete {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.DELETE_OFFSET, common.COMMON_TRUE)

	} else {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.DELETE_OFFSET, common.COMMON_FALSE)
	}
	cldr.deleteFlag = delete
}

func (cldr *SupremumHeader) GetDeleteFlag() bool {
	value := util.ReadBytesByIndexBit(cldr.Content[0], common.DELETE_OFFSET)
	if value == "1" {
		return true
	} else {
		return false
	}
}
func (cldr *SupremumHeader) GetRecMinFlag() bool {
	value := util.ReadBytesByIndexBit(cldr.Content[0], common.DELETE_OFFSET)
	if value == "1" {
		return true
	} else {
		return false
	}
}
func (cldr *SupremumHeader) SetRecMinFlag(flag bool) {
	if flag {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.MIN_REC_OFFSET, common.COMMON_TRUE)

	} else {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.MIN_REC_OFFSET, common.COMMON_FALSE)
	}
	cldr.minRecFlag = flag
}
func (cldr *SupremumHeader) SetNOwned(size byte) {

	util.WriteBitsByStart(cldr.Content[0], util.TrimLeftPaddleBitString(size, 4), 4, 8)

	cldr.Content[0] = util.ConvertBits2Byte(util.WriteBitsByStart(cldr.Content[0], util.TrimLeftPaddleBitString(size, 4), 4, 8))

	cldr.nOwned = uint16(size)
}
func (cldr *SupremumHeader) GetNOwned() byte {
	return util.LeftPaddleBitString(util.ReadBytesByIndexBitByStart(cldr.Content[0], 4, 8), 4)
}
func (cldr *SupremumHeader) GetHeapNo() uint16 {
	var heapNo = make([]string, 0)
	heapNo = append(heapNo, "0")
	heapNo = append(heapNo, "0")
	heapNo = append(heapNo, "0")
	heapNo = append(heapNo, util.ConvertByte2BitsString(cldr.Content[1])...)
	heapNo = append(heapNo, util.ConvertByte2BitsString(cldr.Content[2])[0:5]...)

	return binary.BigEndian.Uint16(util.ConvertBits2Bytes(strings.Join(heapNo, "")))
	//return util.ReadUB2Byte2Int(util.ConvertBits2Bytes(strings.Join(heapNo, "")))
}
func (cldr *SupremumHeader) SetHeapNo(heapNo uint16) {
	var result = util.ConvertUInt2Bytes(heapNo)
	resultArray := util.ConvertBytes2BitStrings(result)
	//取值
	cldr.Content[1] = util.ConvertString2Byte(strings.Join(resultArray[3:11], ""))
	cldr.Content[2] = util.ConvertString2Byte(util.WriteBitsByStart(cldr.Content[2], resultArray[11:16], 0, 5))
	cldr.nOwned = uint16(heapNo)
}
func (cldr *SupremumHeader) GetRecordType() uint8 {
	var recordType = make([]string, 0)
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, util.ConvertByte2BitsString(cldr.Content[2])[5:8]...)
	return uint8(util.ReadUB2Byte2Int(util.ConvertBits2Bytes(strings.Join(recordType, ""))))
}
func (cldr *SupremumHeader) SetRecordType(recordType uint8) {
	resultArray := util.ConvertByte2BitsString(recordType)
	cldr.Content[2] = util.ConvertString2Byte(util.WriteBitsByStart(cldr.Content[2], resultArray[5:8], 5, 8))
	cldr.recordType = recordType
}
func (cldr *SupremumHeader) GetNextRecord() uint16 {
	return util.ReadUB2Byte2Int(cldr.Content[3:5])
}
func (cldr *SupremumHeader) SetNextRecord(nextRecord uint16) {
	cldr.Content[3] = util.ConvertUInt2Bytes(nextRecord)[0]
	cldr.Content[4] = util.ConvertUInt2Bytes(nextRecord)[1]
}

func (cldr *SupremumHeader) GetRowHeaderLength() uint16 {
	return uint16(len(cldr.Content))
}

func (cldr *SupremumHeader) ToByte() []byte {
	return cldr.Content
}

type SupremumValue struct {
	basic.FieldDataValue
	content []byte
}

func NewSupremumValue() basic.FieldDataValue {
	var infimumValue = new(SupremumValue)
	infimumValue.content = []byte{'s', 'u', 'p', 'r', 'e', 'm', 'u', 'm'}
	return infimumValue
}

func (s *SupremumValue) ToByte() []byte {
	return s.content
}

func (s *SupremumValue) WriteBytesWithNull(content []byte) {
	panic("implement me")
}

func (s *SupremumValue) GetPrimaryKey() basic.FiledDataBytes {
	panic("implement me")
}

func (s *SupremumValue) GetRowDataLength() uint16 {
	return 8
}

func (sr *SupremumRow) Less(than basic.Row) bool {

	return false
}

/*
***************

***
 */
func NewSupremumRow() basic.Row {
	var ir = new(SupremumRow)
	ir.header = NewSupremumHeader()
	ir.value = NewSupremumValue()
	return ir
}

func NewSupremumRowByContent(content []byte) basic.Row {
	var ir = &SupremumRow{
		Row:    nil,
		header: ParseSupremumHeader(content[0:5]),
		value:  NewSupremumValue(),
	}

	return ir
}

func (sr *SupremumRow) IsSupremumRow() bool {
	return true
}

func (sr *SupremumRow) IsInfimumRow() bool {
	return false
}

func (sr *SupremumRow) GetRowLength() uint16 {

	return sr.header.GetRowHeaderLength() + sr.value.GetRowDataLength()
}

func (sr *SupremumRow) ToByte() []byte {
	return append(sr.header.ToByte(), sr.value.ToByte()...)
}

func (sr *SupremumRow) GetPageNumber() uint32 {
	panic("implement me")
}

func (sr *SupremumRow) WriteWithNull(content []byte) {
	panic("implement me")
}

func (sr *SupremumRow) WriteBytesWithNullWithsPos(content []byte, index byte) {
	panic("implement me")
}

func (sr *SupremumRow) GetPrimaryKey() basic.Value {
	panic("implement me")
}

func (ir *SupremumRow) GetHeaderLength() uint16 {

	return ir.header.GetRowHeaderLength()
}

func (sr *SupremumRow) GetFieldLength() int {
	panic("implement me")
}

func (sr *SupremumRow) ReadValueByIndex(index int) basic.Value {
	panic("implement me")
}

func (sr *SupremumRow) SetNOwned(cnt byte) {
	sr.header.SetNOwned(cnt)
}

func (sr *SupremumRow) GetNOwned() byte {
	return sr.header.GetNOwned()
}

func (sr *SupremumRow) GetNextRowOffset() uint16 {
	return sr.header.GetNextRecord()
}

// 获取当前记录在本页面中的相对位置
func (sr *SupremumRow) GetHeapNo() uint16 {
	return sr.header.GetHeapNo()
}

// 设置该记录在
func (sr *SupremumRow) SetHeapNo(heapNo uint16) {

	sr.header.SetHeapNo(heapNo)
}

func (sr *SupremumRow) SetNextRowOffset(offset uint16) {
	sr.header.SetNextRecord(offset)
}
