/*
Extent（区）是InnoDB存储引擎中的物理空间管理单位

基本属性：
- 大小：1MB（64个页，每页16KB）
- 用途：批量分配和管理页面
- 所属：每个Extent归属于某个Segment或处于空闲状态

Extent分类：
1. Free Extent: 完全空闲，可分配给任意Segment
2. Free Fragment: 部分页面被使用的Extent
3. Full Extent: 所有页面都已被使用
4. System Extent: 用于存储系统页面（如FSP_HDR、XDES等）

管理方式：
- 使用XDES（区描述符）页面记录Extent信息
- 通过位图记录页面使用状态
- 维护在Segment的不同链表中（Free、Fragment、Full）
*/

package extents

import (
	"encoding/binary"
	"errors"
)

// Extent状态常量
const (
	EXTENT_FREE    = 0 // 空闲Extent
	EXTENT_PARTIAL = 1 // 部分使用的Extent
	EXTENT_FULL    = 2 // 已满的Extent
	EXTENT_SYSTEM  = 3 // 系统Extent
)

// 基本常量定义
const (
	EXTENT_SIZE      = 1024 * 1024 // 1MB
	PAGES_PER_EXTENT = 64          // 每个Extent包含64个页
	PAGE_SIZE        = 16 * 1024   // 16KB
	BITMAP_SIZE      = 16          // 16字节的位图(128位，每页2位)
)

// ExtentEntry 表示区描述符条目
type ExtentEntry struct {
	SegmentID   uint64   // 所属段ID
	State       uint8    // Extent状态
	PageBitmap  [16]byte // 页面使用位图，每页2位
	PageCount   uint8    // 已使用的页面数
	FirstPageNo uint32   // 该Extent中第一个页的页号
}

// NewExtentEntry 创建新的区描述符条目
func NewExtentEntry(firstPageNo uint32) *ExtentEntry {
	return &ExtentEntry{
		SegmentID:   0,
		State:       EXTENT_FREE,
		PageBitmap:  [16]byte{},
		PageCount:   0,
		FirstPageNo: firstPageNo,
	}
}

// IsPageFree 检查指定页面是否空闲
func (e *ExtentEntry) IsPageFree(pageOffset uint8) bool {
	if pageOffset >= PAGES_PER_EXTENT {
		return false
	}

	bytePos := pageOffset / 4      // 每个字节包含4个页的状态
	bitPos := (pageOffset % 4) * 2 // 每页2位

	bits := (e.PageBitmap[bytePos] >> bitPos) & 0x03
	return bits == 0
}

// AllocatePage 分配一个页面
func (e *ExtentEntry) AllocatePage(pageOffset uint8) error {
	if pageOffset >= PAGES_PER_EXTENT {
		return errors.New("页面偏移量超出范围")
	}

	if !e.IsPageFree(pageOffset) {
		return errors.New("页面已被分配")
	}

	bytePos := pageOffset / 4
	bitPos := (pageOffset % 4) * 2

	// 设置页面状态为已使用(01)
	e.PageBitmap[bytePos] |= (0x01 << bitPos)
	e.PageCount++

	// 更新Extent状态
	if e.PageCount == PAGES_PER_EXTENT {
		e.State = EXTENT_FULL
	} else if e.PageCount == 1 {
		e.State = EXTENT_PARTIAL
	}

	return nil
}

// FreePage 释放一个页面
func (e *ExtentEntry) FreePage(pageOffset uint8) error {
	if pageOffset >= PAGES_PER_EXTENT {
		return errors.New("页面偏移量超出范围")
	}

	if e.IsPageFree(pageOffset) {
		return errors.New("页面已经是空闲状态")
	}

	bytePos := pageOffset / 4
	bitPos := (pageOffset % 4) * 2

	// 清除页面状态位
	e.PageBitmap[bytePos] &= ^(0x03 << bitPos)
	e.PageCount--

	// 更新Extent状态
	if e.PageCount == 0 {
		e.State = EXTENT_FREE
	} else {
		e.State = EXTENT_PARTIAL
	}

	return nil
}

// SetSegmentID 设置所属段ID
func (e *ExtentEntry) SetSegmentID(segID uint64) {
	e.SegmentID = segID
}

// GetSegmentID 获取所属段ID
func (e *ExtentEntry) GetSegmentID() uint64 {
	return e.SegmentID
}

// GetState 获取Extent状态
func (e *ExtentEntry) GetState() uint8 {
	return e.State
}

// GetUsedPages 获取已使用页面数
func (e *ExtentEntry) GetUsedPages() uint8 {
	return e.PageCount
}

// Serialize 序列化ExtentEntry
func (e *ExtentEntry) Serialize() []byte {
	buff := make([]byte, 32) // 8(SegID) + 1(State) + 16(Bitmap) + 1(Count) + 4(FirstPage) + 2(Reserved)

	// 写入段ID
	binary.BigEndian.PutUint64(buff[0:8], e.SegmentID)

	// 写入状态
	buff[8] = e.State

	// 写入位图
	copy(buff[9:25], e.PageBitmap[:])

	// 写入已用页数
	buff[25] = e.PageCount

	// 写入首页号
	binary.BigEndian.PutUint32(buff[26:30], e.FirstPageNo)

	return buff
}

// Deserialize 反序列化ExtentEntry
func DeserializeExtentEntry(data []byte) (*ExtentEntry, error) {
	if len(data) < 32 {
		return nil, errors.New("数据长度不足")
	}

	entry := &ExtentEntry{}

	// 读取段ID
	entry.SegmentID = binary.BigEndian.Uint64(data[0:8])

	// 读取状态
	entry.State = data[8]

	// 读取位图
	copy(entry.PageBitmap[:], data[9:25])

	// 读取已用页数
	entry.PageCount = data[25]

	// 读取首页号
	entry.FirstPageNo = binary.BigEndian.Uint32(data[26:30])

	return entry, nil
}
