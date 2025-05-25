package record

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// SpecialRow 特殊行实现
type SpecialRow struct {
	isInfimum  bool
	isSupremum bool
	data       []byte
	heapNo     uint16
	nextOffset uint16
	rowLength  uint16
}

// NewInfimumRow 创建最小行
func NewInfimumRow() basic.Row {
	return &SpecialRow{
		isInfimum:  true,
		isSupremum: false,
		data:       make([]byte, 13), // 标准infimum记录长度
		rowLength:  13,
	}
}

// NewSupremumRow 创建最大行
func NewSupremumRow() basic.Row {
	return &SpecialRow{
		isInfimum:  false,
		isSupremum: true,
		data:       make([]byte, 13), // 标准supremum记录长度
		rowLength:  13,
	}
}

// NewInfimumRowByContent 从内容创建最小行
func NewInfimumRowByContent(content []byte) basic.Row {
	row := &SpecialRow{
		isInfimum:  true,
		isSupremum: false,
		data:       make([]byte, len(content)),
		rowLength:  uint16(len(content)),
	}
	copy(row.data, content)
	return row
}

// NewSupremumRowByContent 从内容创建最大行
func NewSupremumRowByContent(content []byte) basic.Row {
	row := &SpecialRow{
		isInfimum:  false,
		isSupremum: true,
		data:       make([]byte, len(content)),
		rowLength:  uint16(len(content)),
	}
	copy(row.data, content)
	return row
}

// 实现basic.Row接口

func (sr *SpecialRow) Less(than basic.Row) bool {
	if sr.isInfimum {
		return !than.IsInfimumRow() // infimum小于所有其他行
	}
	if sr.isSupremum {
		return false // supremum不小于任何行
	}
	return false
}

func (sr *SpecialRow) ToByte() []byte {
	return sr.data
}

func (sr *SpecialRow) IsInfimumRow() bool {
	return sr.isInfimum
}

func (sr *SpecialRow) IsSupremumRow() bool {
	return sr.isSupremum
}

func (sr *SpecialRow) GetPageNumber() uint32 {
	return 0
}

func (sr *SpecialRow) WriteWithNull(content []byte) {
	// 特殊行不支持写入
}

func (sr *SpecialRow) WriteBytesWithNullWithsPos(content []byte, index byte) {
	// 特殊行不支持写入
}

func (sr *SpecialRow) GetRowLength() uint16 {
	return sr.rowLength
}

func (sr *SpecialRow) GetHeaderLength() uint16 {
	return 5 // 标准行头长度
}

func (sr *SpecialRow) GetPrimaryKey() basic.Value {
	return nil
}

func (sr *SpecialRow) GetFieldLength() int {
	return 0
}

func (sr *SpecialRow) ReadValueByIndex(index int) basic.Value {
	return nil
}

func (sr *SpecialRow) SetNOwned(cnt byte) {
	// 特殊行的nOwned处理
}

func (sr *SpecialRow) GetNOwned() byte {
	return 1
}

func (sr *SpecialRow) GetNextRowOffset() uint16 {
	return sr.nextOffset
}

func (sr *SpecialRow) SetNextRowOffset(offset uint16) {
	sr.nextOffset = offset
}

func (sr *SpecialRow) GetHeapNo() uint16 {
	return sr.heapNo
}

func (sr *SpecialRow) SetHeapNo(heapNo uint16) {
	sr.heapNo = heapNo
}

func (sr *SpecialRow) SetTransactionId(trxId uint64) {
	// 特殊行不支持事务ID
}

func (sr *SpecialRow) GetValueByColName(colName string) basic.Value {
	return nil
}

func (sr *SpecialRow) ToString() string {
	if sr.isInfimum {
		return "INFIMUM"
	} else if sr.isSupremum {
		return "SUPREMUM"
	}
	return "SPECIAL"
}
