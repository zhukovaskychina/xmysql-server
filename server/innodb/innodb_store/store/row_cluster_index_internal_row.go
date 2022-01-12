package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/tuple"
	"github.com/zhukovaskychina/xmysql-server/util"
)

/***
########################################################################################################################
**/

//用于描述非叶子节点的记录

type ClusterInternalRowHeader struct {
	basic.FieldDataHeader
	FrmMeta    tuple.TableRowTuple
	deleteFlag bool
	minRecFlag bool   //B+树非叶子项都会增加该标记
	nOwned     uint16 //槽位中最大的值有值，该槽位其他的行皆为0
	heapNo     uint16 //表示当前记录在页面中的相对位置
	recordType uint8  //0 表示普通记录，1表示B+树非叶子节点的目录项记录，2表示Infimum，3表示Supremum
	nextRecord uint16 //表示下一条记录相对位置
	Content    []byte //长度5个字节+长度列表，都是bit

	//数据长度列表
	VarLengthContent []byte //可变长度列表

	//NULL值列表
	NullContent []byte //倒叙，大小取决于列长度，不满8个为一个byte，假设25，则需要4个字节，倒叙

	VarLengthContentMap map[byte]uint16 //key-value 下标和实际长度
}

func NewClusterInternalRowHeader(frmMeta tuple.TableRowTuple) basic.FieldDataHeader {
	var clr = new(ClusterInternalRowHeader)
	clr.FrmMeta = frmMeta
	clr.deleteFlag = false
	clr.minRecFlag = false
	clr.nOwned = 1
	clr.heapNo = 0
	clr.nextRecord = 0
	clr.Content = []byte{util.ConvertBits2Byte("00000000")}
	clr.Content = append(clr.Content, util.ConvertBits2Bytes("0000000000000011")...)
	clr.Content = append(clr.Content, util.ConvertBits2Bytes("0000000000000000")...)

	//空值处理
	clr.NullContent = make([]byte, 0)
	//可变长度
	clr.VarLengthContent = make([]byte, 0)

	clr.VarLengthContentMap = make(map[byte]uint16)
	return clr
}

func NewClusterInternalRowHeaderWithContents(frmMeta tuple.TableRowTuple, content []byte) basic.FieldDataHeader {
	var clr = new(ClusterLeafRowHeader)
	clr.FrmMeta = frmMeta
	clr.VarLengthContentMap = make(map[byte]uint16)
	//计算列长度
	cl := frmMeta.GetColumnLength()
	//根据列长度，计算出NULL表长度计算
	//除以8,得到字节表示长度
	var size int

	if cl&7 == 0 {
		size = cl >> 3
	} else {
		size = cl>>3 + 1
	}

	//获得可变长度
	varColumns := frmMeta.GetVarColumns()

	varColumnContent := content[0 : len(varColumns)*2]

	//	varColumnSize := len(varColumnContent)

	clr.VarLengthContent = varColumnContent
	varLength := frmMeta.GetColumnLength()
	var index = 0
	var beforeCur = 0
	var afterCur = 2
	for i := 0; i < varLength; i++ {
		currentCols := frmMeta.GetColumnInfos(byte(i))
		if currentCols.FieldType == "VARCHAR" {
			//此处和mysql定义的不一致，为了便于实现row的反序列化，特将变长部分二位字节处理，这样一来，字节header部分的长度为
			// 可变变量数量*2+NullSize+5

			//rl := currentCols.FieldLength
			//if (rl * 3) > 255 {

			clr.VarLengthContentMap[byte(i)] = binary.LittleEndian.Uint16(varColumnContent[beforeCur:afterCur])

			beforeCur = beforeCur + 2
			afterCur = afterCur + 2
			//} else {
			//	clr.VarLengthContent = append(clr.VarLengthContent, content[index])
			//	clr.VarLengthContentMap[byte(i)] = uint16(content[index])
			//}
			index = index + 2
		}
	}

	clr.NullContent = content[index : index+size]

	//解析下面的header部分
	clr.Content = content[index+size : index+size+5]
	return clr
}

func (cldr *ClusterInternalRowHeader) SetDeleteFlag(delete bool) {
	if delete {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.DELETE_OFFSET, common.COMMON_TRUE)

	} else {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.DELETE_OFFSET, common.COMMON_FALSE)
	}
	cldr.deleteFlag = delete
}

func (cldr *ClusterInternalRowHeader) GetDeleteFlag() bool {
	value := util.ReadBytesByIndexBit(cldr.Content[0], common.DELETE_OFFSET)
	if value == "1" {
		return true
	} else {
		return false
	}
}
func (cldr *ClusterInternalRowHeader) GetRecMinFlag() bool {
	value := util.ReadBytesByIndexBit(cldr.Content[0], common.DELETE_OFFSET)
	if value == "1" {
		return true
	} else {
		return false
	}
}
func (cldr *ClusterInternalRowHeader) SetRecMinFlag(flag bool) {
	if flag {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.MIN_REC_OFFSET, common.COMMON_TRUE)

	} else {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.MIN_REC_OFFSET, common.COMMON_FALSE)
	}
	cldr.minRecFlag = flag
}
func (cldr *ClusterInternalRowHeader) SetNOwned(size byte) {
	cldr.Content[0] = util.ConvertBits2Byte(util.WriteBitsByStart(cldr.Content[0], util.TrimLeftPaddleBitString(size, 4), 4, 8))
	cldr.nOwned = uint16(size)
}
func (cldr *ClusterInternalRowHeader) GetNOwned() byte {
	return util.LeftPaddleBitString(util.ReadBytesByIndexBitByStart(cldr.Content[0], 4, 8), 4)
}
func (cldr *ClusterInternalRowHeader) GetHeapNo() uint16 {
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
func (cldr *ClusterInternalRowHeader) SetHeapNo(heapNo uint16) {
	var result = util.ConvertUInt2Bytes(heapNo)
	resultArray := util.ConvertBytes2BitStrings(result)
	//取值
	cldr.Content[1] = util.ConvertString2Byte(strings.Join(resultArray[3:11], ""))
	cldr.Content[2] = util.ConvertString2Byte(util.WriteBitsByStart(cldr.Content[2], resultArray[11:16], 0, 5))
	cldr.nOwned = uint16(heapNo)
}
func (cldr *ClusterInternalRowHeader) GetRecordType() uint8 {
	var recordType = make([]string, 0)
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, util.ConvertByte2BitsString(cldr.Content[2])[5:8]...)
	return uint8(util.ReadUB2Byte2Int(util.ConvertBits2Bytes(strings.Join(recordType, ""))))
}
func (cldr *ClusterInternalRowHeader) SetRecordType(recordType uint8) {
	resultArray := util.ConvertByte2BitsString(recordType)
	cldr.Content[2] = util.ConvertString2Byte(util.WriteBitsByStart(cldr.Content[2], resultArray[5:8], 5, 8))
	cldr.recordType = recordType
}
func (cldr *ClusterInternalRowHeader) GetNextRecord() uint16 {
	return util.ReadUB2Byte2Int(cldr.Content[3:5])
}
func (cldr *ClusterInternalRowHeader) SetNextRecord(nextRecord uint16) {
	cldr.Content[3] = util.ConvertUInt2Bytes(nextRecord)[0]
	cldr.Content[4] = util.ConvertUInt2Bytes(nextRecord)[1]
}

func (cldr *ClusterInternalRowHeader) GetRowHeaderLength() uint16 {
	return uint16(len(cldr.Content))
}

func (cldr *ClusterInternalRowHeader) ToByte() []byte {
	return cldr.Content
}

type ClusterInternalRowData struct {
	basic.FieldDataValue
	PrimaryKeyMeta tuple.TableRowTuple
	Content        []byte
}

func NewClusterInternalRowData(PrimaryKeyMeta tuple.TableRowTuple) basic.FieldDataValue {
	var clusterLeafRowData = new(ClusterInternalRowData)
	clusterLeafRowData.Content = make([]byte, 0)
	clusterLeafRowData.PrimaryKeyMeta = PrimaryKeyMeta
	return clusterLeafRowData
}
func (cld *ClusterInternalRowData) WriteBytesWithNull(content []byte) {
	cld.Content = util.WriteWithNull(cld.Content, content)
}

func (cld *ClusterInternalRowData) GetPrimaryKey() basic.Value {
	//return cld.ReadBytesWithNullWithPosition(0)
	return nil
}

func (cld *ClusterInternalRowData) GetRowDataLength() uint16 {
	return uint16(len(cld.Content))
}

func (cld *ClusterInternalRowData) ToByte() []byte {
	return cld.Content
}

func (cld *ClusterInternalRowData) ReadBytesWithNullWithPosition(index int) []byte {
	return cld.Content[0+5*index : 5*index+5][0:4]
}

//大致为  页面号/主键
type ClusterInternalRow struct {
	basic.Row
	header    basic.FieldDataHeader
	value     basic.FieldDataValue
	FrmMeta   tuple.TableRowTuple
	RowValues []basic.Value
}

func NewClusterInternalRow(primaryClusterInternalTuple tuple.TableRowTuple) basic.Row {
	return &ClusterInternalRow{
		Row:     nil,
		header:  NewClusterInternalRowHeader(primaryClusterInternalTuple),
		value:   NewClusterInternalRowData(primaryClusterInternalTuple),
		FrmMeta: primaryClusterInternalTuple,
	}
}

func NewClusterInternalRowWithContent(content []byte, tableTuple tuple.TableRowTuple) basic.Row {
	var currentRow = new(ClusterInternalRow)

	currentRow.FrmMeta = tableTuple

	currentRow.header = NewClusterLeafRowHeaderWithContents(tableTuple, content)
	currentRow.RowValues = make([]basic.Value, 0)

	rowHeaderLength := currentRow.header.GetRowHeaderLength()

	startOffset := rowHeaderLength

	for i := 0; i < tableTuple.GetColumnLength(); i++ {

		if currentRow.header.IsValueNullByIdx(byte(int(i))) {
			fieldType := tableTuple.GetColumnInfos(byte(i)).FieldType
			switch fieldType {
			case "VARCHAR":
				{
					realLength := currentRow.header.GetVarValueLengthByIndex(byte(i))
					currentRow.RowValues = append(currentRow.RowValues, basic.NewVarcharVal(content[startOffset:int(startOffset)+realLength]))
					startOffset = startOffset + uint16(realLength)
					break
				}
			case "BIGINT":
				{

					currentRow.RowValues = append(currentRow.RowValues, basic.NewBigIntValue(content[startOffset:startOffset+8]))
					startOffset = startOffset + 8
					break
				}
			case "INT":
				{
					currentRow.RowValues = append(currentRow.RowValues, basic.NewIntValue(content[startOffset:startOffset+4]))
					startOffset = startOffset + 4
					break
				}
			}

		} else {
			fmt.Println("------------------")
		}

	}
	currentRow.value = NewClusterLeafRowDataWithContents(content[rowHeaderLength:startOffset], tableTuple)
	return currentRow
}

func (row *ClusterInternalRow) Less(than basic.Row) bool {

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

func (row *ClusterInternalRow) ToByte() []byte {
	return append(row.header.ToByte(), row.value.ToByte()...)
}

func (row *ClusterInternalRow) WriteWithNull(content []byte) {

	row.value.WriteBytesWithNull(content)
}

func (row *ClusterInternalRow) GetRowLength() uint16 {

	return row.header.GetRowHeaderLength() + row.value.GetRowDataLength()
}

func (row *ClusterInternalRow) GetHeaderLength() uint16 {
	return row.header.GetRowHeaderLength()
}

func (row *ClusterInternalRow) GetPrimaryKey() basic.Value {
	return nil
}
func (row *ClusterInternalRow) GetPageNumber() uint32 {
	return 0
}

func (row *ClusterInternalRow) IsSupremumRow() bool {
	return false
}

func (row *ClusterInternalRow) IsInfimumRow() bool {
	return false
}

func (row *ClusterInternalRow) ToDatum() []basic.Datum {

	return nil
}
