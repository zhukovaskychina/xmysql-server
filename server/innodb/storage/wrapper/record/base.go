package record

import (
	"bytes"
	"encoding/binary"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// BaseRecord 基础记录实现
type BaseRecord struct {
	basic.Row
	id      uint64
	data    []byte
	header  basic.FieldDataHeader
	value   basic.FieldDataValue
	frmMeta metadata.TableRowTuple
}

// NewBaseRecord 创建基础记录
func NewBaseRecord(id uint64, data []byte, header basic.FieldDataHeader, value basic.FieldDataValue, frmMeta metadata.TableRowTuple) *BaseRecord {
	return &BaseRecord{
		id:      id,
		data:    data,
		header:  header,
		value:   value,
		frmMeta: frmMeta,
	}
}

// Record接口实现
func (r *BaseRecord) GetID() uint64     { return r.id }
func (r *BaseRecord) GetData() []byte   { return r.data }
func (r *BaseRecord) GetLength() uint32 { return uint32(len(r.data)) }
func (r *BaseRecord) GetType() uint8    { return r.header.GetRecordType() }

// Row接口实现
func (r *BaseRecord) Less(than basic.Row) bool {
	// 修复类型断言问题
	thisKey := r.GetPrimaryKey().Raw()
	otherKey := than.GetPrimaryKey().Raw()

	// 安全地进行类型转换
	if thisBytes, ok := thisKey.([]byte); ok {
		if otherBytes, ok := otherKey.([]byte); ok {
			return bytes.Compare(thisBytes, otherBytes) < 0
		}
	}
	// 如果类型转换失败，使用字符串比较作为备选
	return r.GetPrimaryKey().ToString() < than.GetPrimaryKey().ToString()
}

func (r *BaseRecord) ToByte() []byte {
	headerBytes := r.header.ToByte()
	valueBytes := r.value.ToByte()
	return append(headerBytes, valueBytes...)
}

func (r *BaseRecord) IsInfimumRow() bool {
	return r.header.GetRecordType() == 2
}

func (r *BaseRecord) IsSupremumRow() bool {
	return r.header.GetRecordType() == 3
}

func (r *BaseRecord) GetPageNumber() uint32 {
	return binary.LittleEndian.Uint32(r.data[0:4])
}

func (r *BaseRecord) WriteWithNull(content []byte) {
	r.value.WriteBytesWithNull(content)
}

func (r *BaseRecord) WriteBytesWithNullWithsPos(content []byte, index byte) {
	r.value.WriteBytesWithNull(content)
}

func (r *BaseRecord) GetRowLength() uint16 {
	return r.header.GetRowHeaderLength() + r.value.GetRowDataLength()
}

func (r *BaseRecord) GetHeaderLength() uint16 {
	return r.header.GetRowHeaderLength()
}

func (r *BaseRecord) GetPrimaryKey() basic.Value {
	// 获取主键列的值，使用TableMeta中的PrimaryKey信息
	tableMeta := r.frmMeta.GetTableMeta()
	if tableMeta != nil && len(tableMeta.PrimaryKey) > 0 {
		// 假设第一个主键列，找到它的索引
		primaryKeyColName := tableMeta.PrimaryKey[0]
		_, pos := r.frmMeta.GetColumnDescInfo(primaryKeyColName)
		if pos >= 0 {
			return r.value.ReadValue(pos)
		}
	}
	// 如果没有主键定义，返回第一列的值
	return r.value.ReadValue(0)
}

func (r *BaseRecord) GetFieldLength() int {
	return r.frmMeta.GetColumnLength()
}

func (r *BaseRecord) ReadValueByIndex(index int) basic.Value {
	return r.value.ReadValue(index)
}

func (r *BaseRecord) SetNOwned(cnt byte) {
	r.header.SetNOwned(cnt)
}

func (r *BaseRecord) GetNOwned() byte {
	return r.header.GetNOwned()
}

func (r *BaseRecord) GetNextRowOffset() uint16 {
	return r.header.GetNextRecord()
}

func (r *BaseRecord) SetNextRowOffset(offset uint16) {
	r.header.SetNextRecord(offset)
}

func (r *BaseRecord) GetHeapNo() uint16 {
	return r.header.GetHeapNo()
}

func (r *BaseRecord) SetHeapNo(heapNo uint16) {
	r.header.SetHeapNo(heapNo)
}
