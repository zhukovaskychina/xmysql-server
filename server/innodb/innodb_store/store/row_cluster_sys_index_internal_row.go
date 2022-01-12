package store

import (
	"bytes"
	"fmt"
	"xmysql-server/server/innodb/basic"
	"xmysql-server/server/innodb/tuple"
	"xmysql-server/util"
)

type ClusterSysIndexInternalRow struct {
	header    basic.FieldDataHeader
	value     basic.FieldDataValue
	FrmMeta   tuple.TableRowTuple
	RowValues []basic.Value
}

func (c *ClusterSysIndexInternalRow) ToDatum() []basic.Datum {
	panic("implement me")
}

func (c *ClusterSysIndexInternalRow) GetHeaderLength() uint16 {

	return c.header.GetRowHeaderLength()

}

func (c *ClusterSysIndexInternalRow) GetValueByColName(colName string) basic.Value {
	_, pos := c.FrmMeta.GetColumnDescInfo(colName)
	return c.RowValues[pos]
}

func (c *ClusterSysIndexInternalRow) SetTransactionId(trxId uint64) {
	c.RowValues[1] = basic.NewBigIntValue(util.ConvertULong8Bytes(trxId))
}

func (c *ClusterSysIndexInternalRow) SetNextRowOffset(offset uint16) {
	c.header.SetNextRecord(offset)
}

func (c *ClusterSysIndexInternalRow) GetHeapNo() uint16 {
	return c.header.GetHeapNo()
}

func (c *ClusterSysIndexInternalRow) SetHeapNo(heapNo uint16) {
	c.header.SetHeapNo(heapNo)
}

func (c *ClusterSysIndexInternalRow) GetNextRowOffset() uint16 {
	return c.header.GetNextRecord()
}

func (c *ClusterSysIndexInternalRow) SetNOwned(cnt byte) {
	c.header.SetNOwned(cnt)
}

func (c *ClusterSysIndexInternalRow) GetNOwned() byte {
	return c.header.GetNOwned()
}

func (c *ClusterSysIndexInternalRow) ReadValueByIndex(index int) basic.Value {
	return c.RowValues[index]

}

func (c *ClusterSysIndexInternalRow) WriteBytesWithNullWithsPos(content []byte, index byte) {
	if content == nil {
		c.header.SetValueNull(1, index)
		c.header.SetValueLengthByIndex(0, index)
	} else {
		c.header.SetValueNull(0, index)
		c.header.SetValueLengthByIndex(len(content), index)
	}

	fieldType := c.FrmMeta.GetColumnInfos(index).FieldType

	switch fieldType {
	case "VARCHAR":
		{
			c.RowValues[index] = basic.NewVarcharVal(content)
		}
	case "BIGINT":
		{
			c.RowValues[index] = basic.NewBigIntValue(content)
		}
	case "INT":
		{
			c.RowValues[index] = basic.NewIntValue(content)
		}
	}

	c.value.WriteBytesWithNull(content)
}

type ClusterSysIndexInternalRowData struct {
	basic.FieldDataValue
	Content []byte
	meta    *SysTableTuple
}

func NewClusterSysIndexInternalRowData(sysTableTuple tuple.TableRowTuple) basic.FieldDataValue {
	var fieldValue = new(ClusterSysIndexInternalRowData)
	fieldValue.meta = sysTableTuple.(*SysTableTuple)
	fieldValue.Content = make([]byte, 0)
	return fieldValue
}

func (c *ClusterSysIndexInternalRowData) ToByte() []byte {
	return c.Content
}

func (c *ClusterSysIndexInternalRowData) WriteBytesWithNull(content []byte) {

	c.Content = append(c.Content, content...)
}

func (c *ClusterSysIndexInternalRowData) ReadBytesWithNullWithPosition(index int) []byte {
	panic("implement me")
}

func (c *ClusterSysIndexInternalRowData) GetRowDataLength() uint16 {
	return uint16(len(c.Content))
}

func NewClusterSysIndexInternalRow(sysTableTuple tuple.TableRowTuple, IsHiddenColumn bool) basic.Row {
	var currentRow = new(ClusterSysIndexInternalRow)
	currentRow.header = NewClusterLeafRowHeader(sysTableTuple)
	currentRow.value = NewClusterSysIndexLeafRowData(sysTableTuple)
	if !IsHiddenColumn {
		currentRow.RowValues = make([]basic.Value, sysTableTuple.GetColumnLength())
	} else {
		currentRow.RowValues = make([]basic.Value, sysTableTuple.GetUnHiddenColumnsLength())
	}

	currentRow.FrmMeta = sysTableTuple.(*SysTableTuple)
	return currentRow
}
func NewClusterSysIndexInternalRowWithContent(content []byte, tableTuple tuple.TableRowTuple) basic.Row {
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

func (c *ClusterSysIndexInternalRow) GetHeader() basic.FieldDataHeader {

	return c.header
}

func (c *ClusterSysIndexInternalRow) Less(than basic.Row) bool {

	if than.IsSupremumRow() {
		return true
	}
	if than.IsInfimumRow() {
		return false
	}
	thanPk := than.GetPrimaryKey()
	resultBool, err := c.GetPrimaryKey().LessThan(thanPk)
	if err != nil {
		panic(err)
	}
	return resultBool.Raw().(bool)
}

func (c *ClusterSysIndexInternalRow) ToByte() []byte {
	var buff = make([]byte, 0)
	buff = append(buff, c.header.ToByte()...)
	buff = append(buff, c.value.ToByte()...)
	return buff
}

func (c *ClusterSysIndexInternalRow) IsInfimumRow() bool {
	return false
}

func (c *ClusterSysIndexInternalRow) IsSupremumRow() bool {
	return false
}

func (c *ClusterSysIndexInternalRow) GetPageNumber() uint32 {
	panic("implement me")
}

func (c *ClusterSysIndexInternalRow) WriteWithNull(content []byte) {
	c.value.WriteBytesWithNull(content)
}

func (c *ClusterSysIndexInternalRow) GetRowLength() uint16 {

	return uint16(len(c.ToByte()))
}

func (c *ClusterSysIndexInternalRow) GetPrimaryKey() basic.Value {

	//读取rowid

	return c.RowValues[0]
}

func (c *ClusterSysIndexInternalRow) GetFieldLength() int {
	return len(c.RowValues)
}

func (c *ClusterSysIndexInternalRow) ToString() string {
	var bufferString bytes.Buffer

	for _, v := range c.RowValues {
		bufferString.WriteString(v.ToString() + " ")
	}
	return bufferString.String()
}
