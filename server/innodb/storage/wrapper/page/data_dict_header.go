package page

import (
	"encoding/binary"

	"xmysql-server/server/innodb/storage/store/segs"
)

// DataDictionaryHeaderSysPage 数据字典头页
type DataDictionaryHeaderSysPage struct {
	MaxRowID      uint64
	MaxTableID    uint64
	MaxIndexID    uint64
	MaxSpaceID    uint32
	SegmentHeader *segs.SegmentHeader
}

// NewDataDictHeaderPage 创建一个新的数据字典头页
func NewDataDictHeaderPage() *DataDictionaryHeaderSysPage {
	return &DataDictionaryHeaderSysPage{
		MaxRowID:   0,
		MaxTableID: 0,
		MaxIndexID: 0,
		MaxSpaceID: 0,
	}
}

// ParseDataDictHrdPage 从字节数组解析数据字典头页
func ParseDataDictHrdPage(content []byte) *DataDictionaryHeaderSysPage {
	page := &DataDictionaryHeaderSysPage{}
	page.MaxRowID = binary.BigEndian.Uint64(content[0:8])
	page.MaxTableID = binary.BigEndian.Uint64(content[8:16])
	page.MaxIndexID = binary.BigEndian.Uint64(content[16:24])
	page.MaxSpaceID = binary.BigEndian.Uint32(content[24:28])
	return page
}

// GetMaxRowID 获取最大行ID
func (p *DataDictionaryHeaderSysPage) GetMaxRowID() uint64 {
	return p.MaxRowID
}

// GetMaxTableID 获取最大表ID
func (p *DataDictionaryHeaderSysPage) GetMaxTableID() uint64 {
	return p.MaxTableID
}

// GetMaxIndexID 获取最大索引ID
func (p *DataDictionaryHeaderSysPage) GetMaxIndexID() uint64 {
	return p.MaxIndexID
}

// GetMaxSpaceID 获取最大空间ID
func (p *DataDictionaryHeaderSysPage) GetMaxSpaceID() uint32 {
	return p.MaxSpaceID
}

// SetMaxRowID 设置最大行ID
func (p *DataDictionaryHeaderSysPage) SetMaxRowID(id uint64) {
	p.MaxRowID = id
}

// SetMaxTableID 设置最大表ID
func (p *DataDictionaryHeaderSysPage) SetMaxTableID(id uint64) {
	p.MaxTableID = id
}

// SetMaxIndexID 设置最大索引ID
func (p *DataDictionaryHeaderSysPage) SetMaxIndexID(id uint64) {
	p.MaxIndexID = id
}

// SetMaxSpaceID 设置最大空间ID
func (p *DataDictionaryHeaderSysPage) SetMaxSpaceID(id uint32) {
	p.MaxSpaceID = id
}

// GetSerializeBytes 获取序列化的字节数组
func (p *DataDictionaryHeaderSysPage) GetSerializeBytes() []byte {
	buf := make([]byte, 28)
	binary.BigEndian.PutUint64(buf[0:8], p.MaxRowID)
	binary.BigEndian.PutUint64(buf[8:16], p.MaxTableID)
	binary.BigEndian.PutUint64(buf[16:24], p.MaxIndexID)
	binary.BigEndian.PutUint32(buf[24:28], p.MaxSpaceID)
	return buf
}
