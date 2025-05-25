package record

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"strings"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/basic"
	"xmysql-server/server/innodb/metadata"
	"xmysql-server/util"
)

// Type aliases for compatibility
type tuple = metadata.RecordTableRowTuple

/*
**
########################################################################################################################
*
*/
type ClusterLeafRowHeader struct {
	basic.FieldDataHeader

	Charset byte
	FrmMeta tuple

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

func NewClusterLeafRowHeader(frmMeta tuple) basic.FieldDataHeader {
	var clr = new(ClusterLeafRowHeader)
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

func NewClusterLeafRowHeaderWithContents(frmMeta tuple, content []byte) basic.FieldDataHeader {
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

	//if cl%8 > 0 {
	//	size++
	//}

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

func (cldr *ClusterLeafRowHeader) SetDeleteFlag(delete bool) {
	if delete {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.DELETE_OFFSET, common.COMMON_TRUE)

	} else {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.DELETE_OFFSET, common.COMMON_FALSE)
	}
	cldr.deleteFlag = delete
}

func (cldr *ClusterLeafRowHeader) GetDeleteFlag() bool {
	value := util.ReadBytesByIndexBit(cldr.Content[0], common.DELETE_OFFSET)
	if value == "1" {
		return true
	} else {
		return false
	}
}
func (cldr *ClusterLeafRowHeader) GetRecMinFlag() bool {
	value := util.ReadBytesByIndexBit(cldr.Content[0], common.DELETE_OFFSET)
	if value == "1" {
		return true
	} else {
		return false
	}
}
func (cldr *ClusterLeafRowHeader) SetRecMinFlag(flag bool) {
	if flag {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.MIN_REC_OFFSET, common.COMMON_TRUE)

	} else {
		cldr.Content[0] = util.ConvertValueOfBitsInBytes(cldr.Content[0], common.MIN_REC_OFFSET, common.COMMON_FALSE)
	}
	cldr.minRecFlag = flag
}
func (cldr *ClusterLeafRowHeader) SetNOwned(size byte) {
	cldr.Content[0] = util.ConvertBits2Byte(util.WriteBitsByStart(cldr.Content[0], util.TrimLeftPaddleBitString(size, 4), 4, 8))
	cldr.nOwned = uint16(size)
}
func (cldr *ClusterLeafRowHeader) GetNOwned() byte {
	return util.LeftPaddleBitString(util.ReadBytesByIndexBitByStart(cldr.Content[0], 4, 8), 4)
}
func (cldr *ClusterLeafRowHeader) GetHeapNo() uint16 {
	var heapNo = make([]string, 0)
	heapNo = append(heapNo, "0")
	heapNo = append(heapNo, "0")
	heapNo = append(heapNo, "0")
	heapNo = append(heapNo, util.ConvertByte2BitsString(cldr.Content[1])...)
	heapNo = append(heapNo, util.ConvertByte2BitsString(cldr.Content[2])[0:5]...)
	//

	var result = util.ConvertBits2Bytes(strings.Join(heapNo, ""))

	bytesBuffer := bytes.NewBuffer(result)
	var rs uint16
	binary.Read(bytesBuffer, binary.BigEndian, &rs)
	return rs
}
func (cldr *ClusterLeafRowHeader) SetHeapNo(heapNo uint16) {
	var result = util.ConvertUInt2Bytes(heapNo)
	resultArray := util.ConvertBytes2BitStrings(result)
	//取值
	cldr.Content[1] = util.ConvertString2Byte(strings.Join(resultArray[3:11], ""))
	cldr.Content[2] = util.ConvertString2Byte(util.WriteBitsByStart(cldr.Content[2], resultArray[11:16], 0, 5))
	cldr.nOwned = uint16(heapNo)
}
func (cldr *ClusterLeafRowHeader) GetRecordType() uint8 {
	var recordType = make([]string, 0)
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, "0")
	recordType = append(recordType, util.ConvertByte2BitsString(cldr.Content[2])[5:8]...)
	return uint8(util.ReadUB2Byte2Int(util.ConvertBits2Bytes(strings.Join(recordType, ""))))
}
func (cldr *ClusterLeafRowHeader) SetRecordType(recordType uint8) {
	resultArray := util.ConvertByte2BitsString(recordType)
	cldr.Content[2] = util.ConvertString2Byte(util.WriteBitsByStart(cldr.Content[2], resultArray[5:8], 5, 8))
	cldr.recordType = recordType
}
func (cldr *ClusterLeafRowHeader) GetNextRecord() uint16 {
	return util.ReadUB2Byte2Int(cldr.Content[3:5])
}
func (cldr *ClusterLeafRowHeader) SetNextRecord(nextRecord uint16) {
	cldr.Content[3] = util.ConvertUInt2Bytes(nextRecord)[0]
	cldr.Content[4] = util.ConvertUInt2Bytes(nextRecord)[1]
}

func (cldr *ClusterLeafRowHeader) GetRowHeaderLength() uint16 {
	return uint16(len(cldr.Content) + len(cldr.VarLengthContent) + len(cldr.NullContent))
}

func (cldr *ClusterLeafRowHeader) ToByte() []byte {
	var result = make([]byte, 0)
	result = append(result, cldr.VarLengthContent...)
	result = append(result, cldr.NullContent...)
	result = append(result, cldr.Content...)
	return result
}

func (cldr *ClusterLeafRowHeader) SetValueNull(nullValue byte, index byte) {
	//不为空
	nullStrArrays := util.ConvertBytes2BitStrings(cldr.NullContent)

	if index < byte(len(nullStrArrays)) {
		if len(nullStrArrays) == 0 {
			nullStrArrays = append(nullStrArrays, "0", "0", "0", "0", "0", "0", "0", "0")
		}
		nullStrArrays[byte(len(nullStrArrays))-1-index] = strconv.Itoa(int(nullValue))
	} else {
		var newNullStrArrays = make([]string, 0)
		newNullStrArrays = append(newNullStrArrays, "0", "0", "0", "0", "0", "0", "0")
		newNullStrArrays = append(newNullStrArrays, strconv.Itoa(int(nullValue)))
		newNullStrArrays = append(newNullStrArrays, nullStrArrays...)
		nullStrArrays = newNullStrArrays
	}

	cldr.NullContent = util.ConvertStringArrays2BytesArrays(nullStrArrays)
}

func (cldr *ClusterLeafRowHeader) IsValueNullByIdx(index byte) bool {

	nullStrArrays := util.ConvertBytes2BitStrings(cldr.NullContent)

	return nullStrArrays[len(nullStrArrays)-int(index)-1] == "0"

}

// 暂时不考虑溢出页
func (cldr *ClusterLeafRowHeader) SetValueLengthByIndex(realLength int, index byte) {

	fieldType := cldr.FrmMeta.GetColumnInfos(index).FieldType
	//fieldLength := cldr.FrmMeta.GetColumnInfos(index).FieldLength
	switch fieldType {
	case "VARCHAR":
		{
			//if fieldLength*3 > 255 {
			//	if realLength > 127 {
			cldr.VarLengthContent = append(cldr.VarLengthContent, util.ConvertInt2Bytes(int32(realLength))...)
			//	} else {
			//		cldr.VarLengthContent = append(cldr.VarLengthContent, byte(realLength))
			//	}
			//} else {
			//	cldr.VarLengthContent = append(cldr.VarLengthContent, byte(realLength))
			//}
			break
		}
	default:
		{

		}

	}

}

// 获取可变变量长度
func (cldr *ClusterLeafRowHeader) GetVarValueLengthByIndex(index byte) int {
	return int(cldr.VarLengthContentMap[index])
}

/*
***

根据header获取实际长度
需要判断NullContent的值
*/
func (cldr *ClusterLeafRowHeader) GetRecordBytesRealLength() int {
	var result = 0
	nullSize := len(cldr.NullContent)
	for i := 0; i < cldr.FrmMeta.GetColumnLength(); i++ {
		formCols := cldr.FrmMeta.GetColumnInfos(byte(i))
		fieldType := formCols.FieldType
		isNull := cldr.NullContent[nullSize-i]

		if isNull == 0 {
			if fieldType == "VARCHAR" {
				result = result + int(cldr.VarLengthContentMap[byte(i)])
			} else {
				result = result + int(formCols.FieldLength)
			}
		}

	}

	return result
}

/*
**
########################################################################################################################
*
*/
type ClusterLeafRowData struct {
	basic.FieldDataValue
	Content   []byte
	meta      tuple
	RowValues []basic.Value
}

// TODO: Uncomment when store package is available
/*
func NewClusterLeafRowData(meta *store.TableTupleMeta) basic.FieldDataValue {
	var clusterLeafRowData = new(ClusterLeafRowData)
	clusterLeafRowData.Content = make([]byte, 0)
	clusterLeafRowData.meta = meta.GetPrimaryClusterLeafTuple()
	return clusterLeafRowData
}
*/

func NewClusterLeafRowDataWithContents(content []byte, meta tuple) basic.FieldDataValue {
	var clusterLeafRowData = new(ClusterLeafRowData)
	clusterLeafRowData.Content = content
	clusterLeafRowData.RowValues = make([]basic.Value, meta.GetColumnLength())
	clusterLeafRowData.meta = meta
	return clusterLeafRowData
}

func (cld *ClusterLeafRowData) WriteBytesWithNull(content []byte) {
	cld.Content = util.WriteWithNull(cld.Content, content)
}

func (cld *ClusterLeafRowData) GetPrimaryKey() []byte {
	return cld.ReadBytesWithNullWithPosition(0)
}

func (cld *ClusterLeafRowData) GetRowDataLength() uint16 {
	return uint16(len(cld.Content))
}

func (cld *ClusterLeafRowData) ToByte() []byte {
	return cld.Content
}

func (cld *ClusterLeafRowData) ReadValue(index int) basic.Value {
	return cld.RowValues[index]
}

func (cld *ClusterLeafRowData) ReadBytesWithNullWithPosition(index int) []byte {
	// TODO: Fix ToByte method call when basic.Value interface is properly defined
	// return cld.RowValues[index].ToByte()
	return nil
}

/***
########################################################################################################################
**/

type ClusterLeafRow struct {
	basic.Row
	header    basic.FieldDataHeader
	value     basic.FieldDataValue
	FrmMeta   tuple
	RowValues []basic.Value
}

func NewClusterLeafRow(content []byte, tableTuple tuple) basic.Row {
	var currentRow = new(ClusterLeafRow)

	currentRow.FrmMeta = tableTuple

	currentRow.header = NewClusterLeafRowHeaderWithContents(tableTuple, content)
	currentRow.RowValues = make([]basic.Value, 0)
	currentRow.value = NewClusterLeafRowDataWithContents(content, tableTuple)
	return currentRow
}

func (row *ClusterLeafRow) ToByte() []byte {
	var buff = make([]byte, 0)
	buff = append(buff, row.header.ToByte()...)
	buff = append(buff, row.value.ToByte()...)
	return buff
}

func (row *ClusterLeafRow) GetPageNumber() uint32 {
	//panic("implement me")
	return util.ReadUB4Byte2UInt32(row.value.ReadBytesWithNullWithPosition(1))
}

func (row *ClusterLeafRow) WriteWithNull(content []byte) {
	row.value.WriteBytesWithNull(content)
}

func (row *ClusterLeafRow) WriteBytesWithNullWithsPos(content []byte, index byte) {
	if content == nil {
		row.header.SetValueNull(1, index)
		row.header.SetValueLengthByIndex(0, index)
	} else {
		row.header.SetValueNull(0, index)
		row.header.SetValueLengthByIndex(len(content), index)
	}

	row.value.WriteBytesWithNull(content)
}

func (row *ClusterLeafRow) GetRowLength() uint16 {

	return uint16(len(row.ToByte()))
}

func (row *ClusterLeafRow) ReadValueByIndex(index int) basic.Value {

	return row.value.ReadValue(index)
}

/***
########################################################################################################################
**/
//根据Row的主键值，或者是比较值做排序
func (row *ClusterLeafRow) Less(than basic.Row) bool {

	if than.IsSupremumRow() {
		return true
	}

	if than.IsInfimumRow() {
		return false
	}

	//thanPrimaryKey := than.GetPrimaryKey()
	//thisPrimaryKey := row.GetPrimaryKey()
	//
	//switch row.FrmMeta.PrimaryKeyType {
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

func (row *ClusterLeafRow) GetPrimaryKey() basic.Value {

	return nil

}

func (row *ClusterLeafRow) GetHeaderLength() uint16 {
	return row.header.GetRowHeaderLength()
}

func (row *ClusterLeafRow) IsSupremumRow() bool {

	return false
}

func (row *ClusterLeafRow) IsInfimumRow() bool {

	return false
}

// TODO: Fix this function when ClusterSysIndexInternalRow and valueImpl are available
/*
func NewClusterLeafRowWithContent(content []byte, tableTuple tuple) basic.Row {
	var currentRow = new(ClusterSysIndexInternalRow)

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
					currentRow.RowValues = append(currentRow.RowValues, valueImpl.NewVarcharVal(content[startOffset:int(startOffset)+realLength]))
					startOffset = startOffset + uint16(realLength)
					break
				}
			case "BIGINT":
				{

					currentRow.RowValues = append(currentRow.RowValues, valueImpl.NewBigIntValue(content[startOffset:startOffset+8]))
					startOffset = startOffset + 8
					break
				}
			case "INT":
				{
					currentRow.RowValues = append(currentRow.RowValues, valueImpl.NewIntValue(content[startOffset:startOffset+4]))
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
*/
