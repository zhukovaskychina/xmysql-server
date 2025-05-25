package page

import (
	"encoding/binary"
	"errors"
)

// Errors
var (
	ErrInvalidPageHeader = errors.New("invalid page header")
)

// PageHeader represents the header of a page
type PageHeader struct {
	// 基本信息字段
	PageNDirSlots   []byte // 2 bytes, 页面中的槽数量
	PageHeapTop     []byte // 2 bytes, 记录堆的顶部位置
	PageNHeap       []byte // 2 bytes, 堆中记录的数量(包括infimum/supremum和已删除记录)
	PageFree        []byte // 2 bytes, 已删除记录链表的头指针
	PageGarbage     []byte // 2 bytes, 已删除记录占用的字节数
	PageLastInsert  []byte // 2 bytes, 最后插入记录的位置
	PageDirection   []byte // 2 bytes, 记录插入的方向
	PageNDirections []byte // 2 bytes, 同一方向连续插入的数量
	PageNRecs       []byte // 2 bytes, 用户记录数量

	// 事务相关字段
	PageMaxTrxId []byte // 8 bytes, 修改页面的最大事务ID(仅在二级索引页面中使用)

	// B+树相关字段
	PageLevel      []byte // 2 bytes, 在B+树中的层级
	PageIndexId    []byte // 8 bytes, 索引ID
	PageBtrSegLeaf []byte // 10 bytes, 叶子节点段信息
	PageBtrSegTop  []byte // 10 bytes, 非叶子节点段信息
}

// NewPageHeader creates a new PageHeader instance
func NewPageHeader() *PageHeader {
	h := &PageHeader{
		PageNDirSlots:   make([]byte, 2),
		PageHeapTop:     make([]byte, 2),
		PageNHeap:       make([]byte, 2),
		PageFree:        make([]byte, 2),
		PageGarbage:     make([]byte, 2),
		PageLastInsert:  make([]byte, 2),
		PageDirection:   make([]byte, 2),
		PageNDirections: make([]byte, 2),
		PageNRecs:       make([]byte, 2),
		PageMaxTrxId:    make([]byte, 8),
		PageLevel:       make([]byte, 2),
		PageIndexId:     make([]byte, 8),
		PageBtrSegLeaf:  make([]byte, 10),
		PageBtrSegTop:   make([]byte, 10),
	}

	// 初始化默认值
	binary.BigEndian.PutUint16(h.PageNDirSlots, 2) // 默认有2个槽(infimum和supremum)
	binary.BigEndian.PutUint16(h.PageHeapTop, 56)  // 使用固定值而不是未定义常量
	binary.BigEndian.PutUint16(h.PageNHeap, 2)     // 默认有2条记录
	binary.BigEndian.PutUint16(h.PageFree, 0)      // 初始没有删除的记录
	binary.BigEndian.PutUint16(h.PageGarbage, 0)   // 初始没有垃圾空间
	binary.BigEndian.PutUint16(h.PageDirection, 0) // 初始插入方向为0
	binary.BigEndian.PutUint16(h.PageNDirections, 0)
	binary.BigEndian.PutUint16(h.PageNRecs, 0) // 初始没有用户记录

	return h
}

// GetBytes returns the serialized bytes of the PageHeader
func (ph *PageHeader) GetBytes() []byte {
	// 预分配足够的空间
	bytes := make([]byte, 0, 56) // 使用固定值

	// 按照固定顺序序列化各字段
	bytes = append(bytes, ph.PageNDirSlots...)
	bytes = append(bytes, ph.PageHeapTop...)
	bytes = append(bytes, ph.PageNHeap...)
	bytes = append(bytes, ph.PageFree...)
	bytes = append(bytes, ph.PageGarbage...)
	bytes = append(bytes, ph.PageLastInsert...)
	bytes = append(bytes, ph.PageDirection...)
	bytes = append(bytes, ph.PageNDirections...)
	bytes = append(bytes, ph.PageNRecs...)
	bytes = append(bytes, ph.PageMaxTrxId...)
	bytes = append(bytes, ph.PageLevel...)
	bytes = append(bytes, ph.PageIndexId...)
	bytes = append(bytes, ph.PageBtrSegLeaf...)
	bytes = append(bytes, ph.PageBtrSegTop...)

	return bytes
}

// ParseBytes parses the given bytes into PageHeader fields
func (ph *PageHeader) ParseBytes(bytes []byte) error {
	if len(bytes) < 56 { // 使用固定值
		return ErrInvalidPageHeader
	}

	// 按照固定偏移量解析各字段
	offset := 0

	// 基本信息字段
	copy(ph.PageNDirSlots, bytes[offset:offset+2])
	offset += 2
	copy(ph.PageHeapTop, bytes[offset:offset+2])
	offset += 2
	copy(ph.PageNHeap, bytes[offset:offset+2])
	offset += 2
	copy(ph.PageFree, bytes[offset:offset+2])
	offset += 2
	copy(ph.PageGarbage, bytes[offset:offset+2])
	offset += 2
	copy(ph.PageLastInsert, bytes[offset:offset+2])
	offset += 2
	copy(ph.PageDirection, bytes[offset:offset+2])
	offset += 2
	copy(ph.PageNDirections, bytes[offset:offset+2])
	offset += 2
	copy(ph.PageNRecs, bytes[offset:offset+2])
	offset += 2

	// 事务相关字段
	copy(ph.PageMaxTrxId, bytes[offset:offset+8])
	offset += 8

	// B+树相关字段
	copy(ph.PageLevel, bytes[offset:offset+2])
	offset += 2
	copy(ph.PageIndexId, bytes[offset:offset+8])
	offset += 8
	copy(ph.PageBtrSegLeaf, bytes[offset:offset+10])
	offset += 10
	copy(ph.PageBtrSegTop, bytes[offset:offset+10])

	return nil
}

// 以下是各字段的getter和setter方法

// GetNDirSlots returns the number of directory slots
func (ph *PageHeader) GetNDirSlots() uint16 {
	return binary.BigEndian.Uint16(ph.PageNDirSlots)
}

// SetNDirSlots sets the number of directory slots
func (ph *PageHeader) SetNDirSlots(n uint16) {
	binary.BigEndian.PutUint16(ph.PageNDirSlots, n)
}

// GetHeapTop returns the heap top pointer
func (ph *PageHeader) GetHeapTop() uint16 {
	return binary.BigEndian.Uint16(ph.PageHeapTop)
}

// SetHeapTop sets the heap top pointer
func (ph *PageHeader) SetHeapTop(pos uint16) {
	binary.BigEndian.PutUint16(ph.PageHeapTop, pos)
}

// GetNHeap returns the number of heap records
func (ph *PageHeader) GetNHeap() uint16 {
	return binary.BigEndian.Uint16(ph.PageNHeap)
}

// SetNHeap sets the number of heap records
func (ph *PageHeader) SetNHeap(n uint16) {
	binary.BigEndian.PutUint16(ph.PageNHeap, n)
}

// GetFree returns the free list head pointer
func (ph *PageHeader) GetFree() uint16 {
	return binary.BigEndian.Uint16(ph.PageFree)
}

// SetFree sets the free list head pointer
func (ph *PageHeader) SetFree(pos uint16) {
	binary.BigEndian.PutUint16(ph.PageFree, pos)
}

// GetGarbage returns the number of bytes in deleted records
func (ph *PageHeader) GetGarbage() uint16 {
	return binary.BigEndian.Uint16(ph.PageGarbage)
}

// SetGarbage sets the number of bytes in deleted records
func (ph *PageHeader) SetGarbage(n uint16) {
	binary.BigEndian.PutUint16(ph.PageGarbage, n)
}

// GetLastInsert returns the last insert position
func (ph *PageHeader) GetLastInsert() uint16 {
	return binary.BigEndian.Uint16(ph.PageLastInsert)
}

// SetLastInsert sets the last insert position
func (ph *PageHeader) SetLastInsert(pos uint16) {
	binary.BigEndian.PutUint16(ph.PageLastInsert, pos)
}

// GetDirection returns the insert direction
func (ph *PageHeader) GetDirection() uint16 {
	return binary.BigEndian.Uint16(ph.PageDirection)
}

// SetDirection sets the insert direction
func (ph *PageHeader) SetDirection(dir uint16) {
	binary.BigEndian.PutUint16(ph.PageDirection, dir)
}

// GetNDirections returns the number of consecutive inserts in same direction
func (ph *PageHeader) GetNDirections() uint16 {
	return binary.BigEndian.Uint16(ph.PageNDirections)
}

// SetNDirections sets the number of consecutive inserts in same direction
func (ph *PageHeader) SetNDirections(n uint16) {
	binary.BigEndian.PutUint16(ph.PageNDirections, n)
}

// GetNRecs returns the number of user records
func (ph *PageHeader) GetNRecs() uint16 {
	return binary.BigEndian.Uint16(ph.PageNRecs)
}

// SetNRecs sets the number of user records
func (ph *PageHeader) SetNRecs(n uint16) {
	binary.BigEndian.PutUint16(ph.PageNRecs, n)
}

// GetMaxTrxId returns the maximum transaction ID that modified the page
func (ph *PageHeader) GetMaxTrxId() uint64 {
	return binary.BigEndian.Uint64(ph.PageMaxTrxId)
}

// SetMaxTrxId sets the maximum transaction ID that modified the page
func (ph *PageHeader) SetMaxTrxId(id uint64) {
	binary.BigEndian.PutUint64(ph.PageMaxTrxId, id)
}

// GetLevel returns the level in B+ tree
func (ph *PageHeader) GetLevel() uint16 {
	return binary.BigEndian.Uint16(ph.PageLevel)
}

// SetLevel sets the level in B+ tree
func (ph *PageHeader) SetLevel(level uint16) {
	binary.BigEndian.PutUint16(ph.PageLevel, level)
}

// GetIndexId returns the index ID
func (ph *PageHeader) GetIndexId() uint64 {
	return binary.BigEndian.Uint64(ph.PageIndexId)
}

// SetIndexId sets the index ID
func (ph *PageHeader) SetIndexId(id uint64) {
	binary.BigEndian.PutUint64(ph.PageIndexId, id)
}

// GetBtrSegLeaf returns the leaf node segment information
func (ph *PageHeader) GetBtrSegLeaf() []byte {
	return ph.PageBtrSegLeaf
}

// SetBtrSegLeaf sets the leaf node segment information
func (ph *PageHeader) SetBtrSegLeaf(seg []byte) {
	if len(seg) != 10 {
		return
	}
	copy(ph.PageBtrSegLeaf, seg)
}

// GetBtrSegTop returns the non-leaf node segment information
func (ph *PageHeader) GetBtrSegTop() []byte {
	return ph.PageBtrSegTop
}

// SetBtrSegTop sets the non-leaf node segment information
func (ph *PageHeader) SetBtrSegTop(seg []byte) {
	if len(seg) != 10 {
		return
	}
	copy(ph.PageBtrSegTop, seg)
}
