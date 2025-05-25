package page

import (
	"encoding/binary"
	"errors"
)

// Record 记录结构
type Record struct {
	Data []byte // 记录数据
}

// IndexEntry 索引项结构
type IndexEntry struct {
	Key  []byte
	Page uint32
}

// DataPageImpl 数据页面实现 - 提供基础的页面操作
type DataPageImpl struct {
	number   uint32
	pageType uint16
	lsn      uint64
	isLeaf   bool
	records  []Record
	nextPage uint32
	entries  []IndexEntry
}

// NewDataPageImpl 创建新数据页面实现
func NewDataPageImpl(number uint32, pageType uint16) *DataPageImpl {
	return &DataPageImpl{
		number:   number,
		pageType: pageType,
		records:  make([]Record, 0),
		entries:  make([]IndexEntry, 0),
	}
}

// GetPageNumber 获取页面号
func (p *DataPageImpl) GetPageNumber() uint32 {
	return p.number
}

// GetPageType 获取页面类型
func (p *DataPageImpl) GetPageType() uint16 {
	return p.pageType
}

// GetPageLSN 获取页面LSN
func (p *DataPageImpl) GetPageLSN() uint64 {
	return p.lsn
}

// SetPageLSN 设置页面LSN
func (p *DataPageImpl) SetPageLSN(lsn uint64) {
	p.lsn = lsn
}

// IsLeafPage 判断是否是叶子页面
func (p *DataPageImpl) IsLeafPage() bool {
	return p.isLeaf
}

// SetLeafPage 设置是否为叶子页面
func (p *DataPageImpl) SetLeafPage(isLeaf bool) {
	p.isLeaf = isLeaf
}

// GetRecords 获取记录列表
func (p *DataPageImpl) GetRecords() []Record {
	return p.records
}

// WriteRecords 写入记录列表
func (p *DataPageImpl) WriteRecords(records []Record) error {
	if records == nil {
		return errors.New("records cannot be nil")
	}
	p.records = records
	return nil
}

// GetNextPage 获取下一个页面号
func (p *DataPageImpl) GetNextPage() uint32 {
	return p.nextPage
}

// SetNextPage 设置下一个页面号
func (p *DataPageImpl) SetNextPage(next uint32) {
	p.nextPage = next
}

// GetIndexEntries 获取索引项列表
func (p *DataPageImpl) GetIndexEntries() []IndexEntry {
	return p.entries
}

// WriteIndexEntries 写入索引项列表
func (p *DataPageImpl) WriteIndexEntries(entries []IndexEntry) error {
	if entries == nil {
		return errors.New("entries cannot be nil")
	}
	p.entries = entries
	return nil
}

// Serialize 序列化页面
func (p *DataPageImpl) Serialize() []byte {
	// 计算总大小
	size := 4 + 2 + 8 + 1 + 4 // 基本字段
	for _, r := range p.records {
		size += 4 + len(r.Data) // 记录长度 + 数据
	}
	for _, e := range p.entries {
		size += 4 + len(e.Key) + 4 // 键长度 + 键 + 页号
	}

	buf := make([]byte, size)
	offset := 0

	// 写入基本字段
	binary.LittleEndian.PutUint32(buf[offset:], p.number)
	offset += 4
	binary.LittleEndian.PutUint16(buf[offset:], p.pageType)
	offset += 2
	binary.LittleEndian.PutUint64(buf[offset:], p.lsn)
	offset += 8
	if p.isLeaf {
		buf[offset] = 1
	}
	offset++
	binary.LittleEndian.PutUint32(buf[offset:], p.nextPage)
	offset += 4

	// 写入记录
	for _, r := range p.records {
		binary.LittleEndian.PutUint32(buf[offset:], uint32(len(r.Data)))
		offset += 4
		copy(buf[offset:], r.Data)
		offset += len(r.Data)
	}

	// 写入索引项
	for _, e := range p.entries {
		binary.LittleEndian.PutUint32(buf[offset:], uint32(len(e.Key)))
		offset += 4
		copy(buf[offset:], e.Key)
		offset += len(e.Key)
		binary.LittleEndian.PutUint32(buf[offset:], e.Page)
		offset += 4
	}

	return buf
}

// Deserialize 反序列化页面
func (p *DataPageImpl) Deserialize(data []byte) error {
	if len(data) < 19 { // 最小长度
		return errors.New("invalid data length")
	}

	offset := 0

	// 读取基本字段
	p.number = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	p.pageType = binary.LittleEndian.Uint16(data[offset:])
	offset += 2
	p.lsn = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	p.isLeaf = data[offset] == 1
	offset++
	p.nextPage = binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// 读取记录
	p.records = make([]Record, 0)
	for offset < len(data) {
		if offset+4 > len(data) {
			break
		}
		size := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		if offset+int(size) > len(data) {
			return errors.New("invalid record size")
		}
		record := Record{
			Data: make([]byte, size),
		}
		copy(record.Data, data[offset:offset+int(size)])
		p.records = append(p.records, record)
		offset += int(size)
	}

	// 读取索引项
	p.entries = make([]IndexEntry, 0)
	for offset < len(data) {
		if offset+4 > len(data) {
			break
		}
		keySize := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		if offset+int(keySize)+4 > len(data) {
			return errors.New("invalid index entry")
		}
		entry := IndexEntry{
			Key: make([]byte, keySize),
		}
		copy(entry.Key, data[offset:offset+int(keySize)])
		offset += int(keySize)
		entry.Page = binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		p.entries = append(p.entries, entry)
	}

	return nil
}
