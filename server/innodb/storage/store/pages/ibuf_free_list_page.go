/*
FIL_PAGE_IBUF_FREE_LIST页面详细说明

基本属性：
- 页面类型：FIL_PAGE_IBUF_FREE_LIST
- 类型编码：0x0004（十进制4）
- 所属模块：InnoDB插入缓冲管理
- 页面作用：管理插入缓冲的空闲页面列表

使用场景：
1. 管理Insert Buffer的空闲页面
2. 优化辅助索引的插入操作
3. 维护插入缓冲页面的分配状态
4. 跟踪可用的插入缓冲页面

注意：该页面类型在较新版本的MySQL中已被废弃，但为了向后兼容性仍需支持

页面结构：
- File Header (38字节)
- IBuf Free List Header (32字节):
  - Free Count: 空闲页面数量 (4字节)
  - Used Count: 已使用页面数量 (4字节)
  - Total Count: 总页面数量 (4字节)
  - First Free: 第一个空闲页面号 (4字节)
  - Last Free: 最后一个空闲页面号 (4字节)
  - Next List Page: 下一个列表页面号 (4字节)
  - Reserved: 保留字段 (8字节)
- Free Page Entries (16306字节): 空闲页面条目
- File Trailer (8字节)
*/

package pages

import (
	"encoding/binary"
	"errors"
	"xmysql-server/server/common"
)

// 插入缓冲空闲列表页面常量
const (
	IBufFreeListHeaderSize = 32                                       // IBuf空闲列表头部大小
	IBufFreeListDataSize   = 16306                                    // IBuf空闲列表数据区大小 (16384 - 38 - 32 - 8)
	IBufPageEntrySize      = 8                                        // 每个页面条目大小 (4字节页号 + 4字节状态)
	MaxIBufFreeEntries     = IBufFreeListDataSize / IBufPageEntrySize // 最大条目数
)

// 插入缓冲页面状态
type IBufPageStatus uint32

const (
	IBufPageStatusFree      IBufPageStatus = 0
	IBufPageStatusAllocated IBufPageStatus = 1
	IBufPageStatusInUse     IBufPageStatus = 2
	IBufPageStatusCorrupted IBufPageStatus = 3
)

// 插入缓冲空闲列表页面错误定义
var (
	ErrIBufFreeListFull     = errors.New("插入缓冲空闲列表已满")
	ErrIBufPageNotFound     = errors.New("插入缓冲页面未找到")
	ErrInvalidIBufPageEntry = errors.New("无效的插入缓冲页面条目")
)

// IBufFreeListHeader 插入缓冲空闲列表头部结构
type IBufFreeListHeader struct {
	FreeCount    uint32 // 空闲页面数量
	UsedCount    uint32 // 已使用页面数量
	TotalCount   uint32 // 总页面数量
	FirstFree    uint32 // 第一个空闲页面号
	LastFree     uint32 // 最后一个空闲页面号
	NextListPage uint32 // 下一个列表页面号
	Reserved     uint64 // 保留字段
}

// IBufPageEntry 插入缓冲页面条目
type IBufPageEntry struct {
	PageNo uint32         // 页面号
	Status IBufPageStatus // 页面状态
}

// IBufFreeListPage 插入缓冲空闲列表页面结构
type IBufFreeListPage struct {
	FileHeader         FileHeader         // 文件头部 (38字节)
	IBufFreeListHeader IBufFreeListHeader // IBuf空闲列表头部 (32字节)
	PageEntries        []IBufPageEntry    // 页面条目列表
	FileTrailer        FileTrailer        // 文件尾部 (8字节)
}

// NewIBufFreeListPage 创建新的插入缓冲空闲列表页面
func NewIBufFreeListPage(spaceID, pageNo uint32) *IBufFreeListPage {
	page := &IBufFreeListPage{
		FileHeader: NewFileHeader(),
		IBufFreeListHeader: IBufFreeListHeader{
			FreeCount:    0,
			UsedCount:    0,
			TotalCount:   0,
			FirstFree:    0,
			LastFree:     0,
			NextListPage: 0,
			Reserved:     0,
		},
		PageEntries: make([]IBufPageEntry, 0, MaxIBufFreeEntries),
		FileTrailer: NewFileTrailer(),
	}

	// 设置页面头部信息
	page.FileHeader.WritePageOffset(pageNo)
	page.FileHeader.WritePageFileType(int16(common.FIL_PAGE_IBUF_FREE_LIST))
	page.FileHeader.WritePageArch(spaceID)

	return page
}

// AddFreePage 添加空闲页面
func (iflp *IBufFreeListPage) AddFreePage(pageNo uint32) error {
	if len(iflp.PageEntries) >= MaxIBufFreeEntries {
		return ErrIBufFreeListFull
	}

	// 检查页面是否已存在
	for _, entry := range iflp.PageEntries {
		if entry.PageNo == pageNo {
			// 如果页面已存在，更新其状态为空闲
			entry.Status = IBufPageStatusFree
			iflp.updateCounts()
			return nil
		}
	}

	// 添加新的空闲页面条目
	entry := IBufPageEntry{
		PageNo: pageNo,
		Status: IBufPageStatusFree,
	}
	iflp.PageEntries = append(iflp.PageEntries, entry)

	// 更新统计信息
	iflp.updateCounts()
	iflp.updateFreeChain()

	return nil
}

// AllocatePage 分配一个空闲页面
func (iflp *IBufFreeListPage) AllocatePage() (uint32, error) {
	// 查找第一个空闲页面
	for i, entry := range iflp.PageEntries {
		if entry.Status == IBufPageStatusFree {
			// 标记为已分配
			iflp.PageEntries[i].Status = IBufPageStatusAllocated

			// 更新统计信息
			iflp.updateCounts()
			iflp.updateFreeChain()

			return entry.PageNo, nil
		}
	}

	return 0, ErrIBufPageNotFound
}

// FreePage 释放一个页面
func (iflp *IBufFreeListPage) FreePage(pageNo uint32) error {
	for i, entry := range iflp.PageEntries {
		if entry.PageNo == pageNo {
			iflp.PageEntries[i].Status = IBufPageStatusFree

			// 更新统计信息
			iflp.updateCounts()
			iflp.updateFreeChain()

			return nil
		}
	}

	return ErrIBufPageNotFound
}

// MarkPageInUse 标记页面为使用中
func (iflp *IBufFreeListPage) MarkPageInUse(pageNo uint32) error {
	for i, entry := range iflp.PageEntries {
		if entry.PageNo == pageNo {
			iflp.PageEntries[i].Status = IBufPageStatusInUse

			// 更新统计信息
			iflp.updateCounts()
			iflp.updateFreeChain()

			return nil
		}
	}

	return ErrIBufPageNotFound
}

// GetPageStatus 获取页面状态
func (iflp *IBufFreeListPage) GetPageStatus(pageNo uint32) (IBufPageStatus, error) {
	for _, entry := range iflp.PageEntries {
		if entry.PageNo == pageNo {
			return entry.Status, nil
		}
	}

	return 0, ErrIBufPageNotFound
}

// GetFreePages 获取所有空闲页面
func (iflp *IBufFreeListPage) GetFreePages() []uint32 {
	var freePages []uint32
	for _, entry := range iflp.PageEntries {
		if entry.Status == IBufPageStatusFree {
			freePages = append(freePages, entry.PageNo)
		}
	}
	return freePages
}

// updateCounts 更新计数统计
func (iflp *IBufFreeListPage) updateCounts() {
	var freeCount, usedCount uint32

	for _, entry := range iflp.PageEntries {
		switch entry.Status {
		case IBufPageStatusFree:
			freeCount++
		case IBufPageStatusAllocated, IBufPageStatusInUse:
			usedCount++
		}
	}

	iflp.IBufFreeListHeader.FreeCount = freeCount
	iflp.IBufFreeListHeader.UsedCount = usedCount
	iflp.IBufFreeListHeader.TotalCount = uint32(len(iflp.PageEntries))
}

// updateFreeChain 更新空闲链
func (iflp *IBufFreeListPage) updateFreeChain() {
	var firstFree, lastFree uint32

	for _, entry := range iflp.PageEntries {
		if entry.Status == IBufPageStatusFree {
			if firstFree == 0 {
				firstFree = entry.PageNo
			}
			lastFree = entry.PageNo
		}
	}

	iflp.IBufFreeListHeader.FirstFree = firstFree
	iflp.IBufFreeListHeader.LastFree = lastFree
}

// SetNextListPage 设置下一个列表页面
func (iflp *IBufFreeListPage) SetNextListPage(pageNo uint32) {
	iflp.IBufFreeListHeader.NextListPage = pageNo
}

// GetNextListPage 获取下一个列表页面
func (iflp *IBufFreeListPage) GetNextListPage() uint32 {
	return iflp.IBufFreeListHeader.NextListPage
}

// GetFreeCount 获取空闲页面数量
func (iflp *IBufFreeListPage) GetFreeCount() uint32 {
	return iflp.IBufFreeListHeader.FreeCount
}

// GetUsedCount 获取已使用页面数量
func (iflp *IBufFreeListPage) GetUsedCount() uint32 {
	return iflp.IBufFreeListHeader.UsedCount
}

// GetTotalCount 获取总页面数量
func (iflp *IBufFreeListPage) GetTotalCount() uint32 {
	return iflp.IBufFreeListHeader.TotalCount
}

// Serialize 序列化页面为字节数组
func (iflp *IBufFreeListPage) Serialize() []byte {
	data := make([]byte, common.PageSize)
	offset := 0

	// 序列化文件头部
	copy(data[offset:], iflp.serializeFileHeader())
	offset += FileHeaderSize

	// 序列化IBuf空闲列表头部
	copy(data[offset:], iflp.serializeIBufFreeListHeader())
	offset += IBufFreeListHeaderSize

	// 序列化页面条目
	entriesData := iflp.serializePageEntries()
	copy(data[offset:], entriesData)
	offset += len(entriesData)

	// 填充剩余空间
	remainingSpace := common.PageSize - FileHeaderSize - IBufFreeListHeaderSize - FileTrailerSize - len(entriesData)
	if remainingSpace > 0 {
		// 填充零字节
		for i := 0; i < remainingSpace; i++ {
			data[offset+i] = 0
		}
		offset += remainingSpace
	}

	// 序列化文件尾部
	copy(data[offset:], iflp.serializeFileTrailer())

	return data
}

// Deserialize 从字节数组反序列化页面
func (iflp *IBufFreeListPage) Deserialize(data []byte) error {
	if len(data) != common.PageSize {
		return ErrInvalidPageSize
	}

	offset := 0

	// 反序列化文件头部
	if err := iflp.deserializeFileHeader(data[offset : offset+FileHeaderSize]); err != nil {
		return err
	}
	offset += FileHeaderSize

	// 反序列化IBuf空闲列表头部
	if err := iflp.deserializeIBufFreeListHeader(data[offset : offset+IBufFreeListHeaderSize]); err != nil {
		return err
	}
	offset += IBufFreeListHeaderSize

	// 反序列化页面条目
	entriesDataSize := int(iflp.IBufFreeListHeader.TotalCount) * IBufPageEntrySize
	if entriesDataSize > IBufFreeListDataSize {
		return ErrInvalidIBufPageEntry
	}

	if err := iflp.deserializePageEntries(data[offset : offset+entriesDataSize]); err != nil {
		return err
	}

	return nil
}

// serializeFileHeader 序列化文件头部
func (iflp *IBufFreeListPage) serializeFileHeader() []byte {
	// 实现文件头部序列化逻辑
	data := make([]byte, FileHeaderSize)
	// 这里应该包含具体的序列化逻辑
	return data
}

// serializeIBufFreeListHeader 序列化IBuf空闲列表头部
func (iflp *IBufFreeListPage) serializeIBufFreeListHeader() []byte {
	data := make([]byte, IBufFreeListHeaderSize)

	binary.LittleEndian.PutUint32(data[0:], iflp.IBufFreeListHeader.FreeCount)
	binary.LittleEndian.PutUint32(data[4:], iflp.IBufFreeListHeader.UsedCount)
	binary.LittleEndian.PutUint32(data[8:], iflp.IBufFreeListHeader.TotalCount)
	binary.LittleEndian.PutUint32(data[12:], iflp.IBufFreeListHeader.FirstFree)
	binary.LittleEndian.PutUint32(data[16:], iflp.IBufFreeListHeader.LastFree)
	binary.LittleEndian.PutUint32(data[20:], iflp.IBufFreeListHeader.NextListPage)
	binary.LittleEndian.PutUint64(data[24:], iflp.IBufFreeListHeader.Reserved)

	return data
}

// serializePageEntries 序列化页面条目
func (iflp *IBufFreeListPage) serializePageEntries() []byte {
	data := make([]byte, len(iflp.PageEntries)*IBufPageEntrySize)

	for i, entry := range iflp.PageEntries {
		offset := i * IBufPageEntrySize
		binary.LittleEndian.PutUint32(data[offset:], entry.PageNo)
		binary.LittleEndian.PutUint32(data[offset+4:], uint32(entry.Status))
	}

	return data
}

// serializeFileTrailer 序列化文件尾部
func (iflp *IBufFreeListPage) serializeFileTrailer() []byte {
	// 实现文件尾部序列化逻辑
	data := make([]byte, FileTrailerSize)
	// 这里应该包含具体的序列化逻辑
	return data
}

// deserializeFileHeader 反序列化文件头部
func (iflp *IBufFreeListPage) deserializeFileHeader(data []byte) error {
	// 实现文件头部反序列化逻辑
	return nil
}

// deserializeIBufFreeListHeader 反序列化IBuf空闲列表头部
func (iflp *IBufFreeListPage) deserializeIBufFreeListHeader(data []byte) error {
	if len(data) < IBufFreeListHeaderSize {
		return ErrInvalidPageSize
	}

	iflp.IBufFreeListHeader.FreeCount = binary.LittleEndian.Uint32(data[0:])
	iflp.IBufFreeListHeader.UsedCount = binary.LittleEndian.Uint32(data[4:])
	iflp.IBufFreeListHeader.TotalCount = binary.LittleEndian.Uint32(data[8:])
	iflp.IBufFreeListHeader.FirstFree = binary.LittleEndian.Uint32(data[12:])
	iflp.IBufFreeListHeader.LastFree = binary.LittleEndian.Uint32(data[16:])
	iflp.IBufFreeListHeader.NextListPage = binary.LittleEndian.Uint32(data[20:])
	iflp.IBufFreeListHeader.Reserved = binary.LittleEndian.Uint64(data[24:])

	return nil
}

// deserializePageEntries 反序列化页面条目
func (iflp *IBufFreeListPage) deserializePageEntries(data []byte) error {
	entryCount := len(data) / IBufPageEntrySize
	iflp.PageEntries = make([]IBufPageEntry, entryCount)

	for i := 0; i < entryCount; i++ {
		offset := i * IBufPageEntrySize
		iflp.PageEntries[i].PageNo = binary.LittleEndian.Uint32(data[offset:])
		iflp.PageEntries[i].Status = IBufPageStatus(binary.LittleEndian.Uint32(data[offset+4:]))
	}

	return nil
}

// Validate 验证页面数据完整性
func (iflp *IBufFreeListPage) Validate() error {
	// 验证条目数量
	if uint32(len(iflp.PageEntries)) != iflp.IBufFreeListHeader.TotalCount {
		return ErrInvalidIBufPageEntry
	}

	// 验证计数统计
	var freeCount, usedCount uint32
	for _, entry := range iflp.PageEntries {
		switch entry.Status {
		case IBufPageStatusFree:
			freeCount++
		case IBufPageStatusAllocated, IBufPageStatusInUse:
			usedCount++
		}
	}

	if freeCount != iflp.IBufFreeListHeader.FreeCount ||
		usedCount != iflp.IBufFreeListHeader.UsedCount {
		return ErrInvalidIBufPageEntry
	}

	return nil
}

// GetFileHeader 获取文件头部
func (iflp *IBufFreeListPage) GetFileHeader() FileHeader {
	return iflp.FileHeader
}

// GetFileTrailer 获取文件尾部
func (iflp *IBufFreeListPage) GetFileTrailer() FileTrailer {
	return iflp.FileTrailer
}

// GetSerializeBytes 获取序列化后的字节数组
func (iflp *IBufFreeListPage) GetSerializeBytes() []byte {
	return iflp.Serialize()
}

// LoadFileHeader 加载文件头部
func (iflp *IBufFreeListPage) LoadFileHeader(content []byte) {
	iflp.deserializeFileHeader(content)
}

// LoadFileTrailer 加载文件尾部
func (iflp *IBufFreeListPage) LoadFileTrailer(content []byte) {
	// 实现文件尾部加载逻辑
}
