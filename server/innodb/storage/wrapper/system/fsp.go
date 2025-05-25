package system

import (
	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrFSPFull = errors.New("file space is full")
)

// FSPFlags 文件空间标志
type FSPFlags uint32

const (
	FSPFlagCompressed FSPFlags = 1 << iota
	FSPFlagEncrypted
	FSPFlagTemporary
)

// FSPPage 文件空间页面
type FSPPage struct {
	*BaseSystemPage
	header FSPPageData
}

// NewFSPPage 创建FSP页面
func NewFSPPage(spaceID, pageNo uint32) *FSPPage {
	fp := &FSPPage{
		BaseSystemPage: NewBaseSystemPage(spaceID, pageNo, SystemPageTypeFSP),
	}

	// 初始化FSP头
	fp.header = FSPPageData{
		SpaceID:    spaceID,
		SpaceFlags: 0,
		Size:       0,
		FreeLimit:  1, // 第一个可用的页面号
		FreePages:  0,
		FragPages:  0,
		Properties: make(map[string]string),
	}

	return fp
}

// GetSize 获取表空间大小
func (fp *FSPPage) GetSize() uint64 {
	fp.RLock()
	defer fp.RUnlock()
	return fp.header.Size
}

// SetSize 设置表空间大小
func (fp *FSPPage) SetSize(size uint64) {
	fp.Lock()
	defer fp.Unlock()

	fp.header.Size = size
	fp.MarkDirty()
}

// GetFreeLimit 获取空闲页面限制
func (fp *FSPPage) GetFreeLimit() uint32 {
	fp.RLock()
	defer fp.RUnlock()
	return fp.header.FreeLimit
}

// SetFreeLimit 设置空闲页面限制
func (fp *FSPPage) SetFreeLimit(limit uint32) {
	fp.Lock()
	defer fp.Unlock()

	fp.header.FreeLimit = limit
	fp.MarkDirty()
}

// GetFreePages 获取空闲页面数
func (fp *FSPPage) GetFreePages() uint32 {
	fp.RLock()
	defer fp.RUnlock()
	return fp.header.FreePages
}

// AddFreePages 增加空闲页面数
func (fp *FSPPage) AddFreePages(count uint32) {
	fp.Lock()
	defer fp.Unlock()

	fp.header.FreePages += count
	fp.MarkDirty()
}

// GetFragPages 获取碎片页面数
func (fp *FSPPage) GetFragPages() uint32 {
	fp.RLock()
	defer fp.RUnlock()
	return fp.header.FragPages
}

// AddFragPages 增加碎片页面数
func (fp *FSPPage) AddFragPages(count uint32) {
	fp.Lock()
	defer fp.Unlock()

	fp.header.FragPages += count
	fp.MarkDirty()
}

// GetFlags 获取标志
func (fp *FSPPage) GetFlags() FSPFlags {
	fp.RLock()
	defer fp.RUnlock()
	return FSPFlags(fp.header.SpaceFlags)
}

// SetFlags 设置标志
func (fp *FSPPage) SetFlags(flags FSPFlags) {
	fp.Lock()
	defer fp.Unlock()

	fp.header.SpaceFlags = uint32(flags)
	fp.MarkDirty()
}

// IsCompressed 是否压缩
func (fp *FSPPage) IsCompressed() bool {
	return fp.GetFlags()&FSPFlagCompressed != 0
}

// IsEncrypted 是否加密
func (fp *FSPPage) IsEncrypted() bool {
	return fp.GetFlags()&FSPFlagEncrypted != 0
}

// IsTemporary 是否临时表空间
func (fp *FSPPage) IsTemporary() bool {
	return fp.GetFlags()&FSPFlagTemporary != 0
}

// GetProperty 获取属性
func (fp *FSPPage) GetProperty(key string) string {
	fp.RLock()
	defer fp.RUnlock()
	return fp.header.Properties[key]
}

// SetProperty 设置属性
func (fp *FSPPage) SetProperty(key, value string) {
	fp.Lock()
	defer fp.Unlock()

	fp.header.Properties[key] = value
	fp.MarkDirty()
}

// Read 实现Page接口
func (fp *FSPPage) Read() error {
	if err := fp.BaseSystemPage.Read(); err != nil {
		return err
	}

	// 读取FSP页面头
	offset := uint32(87)
	fp.header.SpaceID = binary.LittleEndian.Uint32(fp.Content[offset:])
	fp.header.SpaceFlags = binary.LittleEndian.Uint32(fp.Content[offset+4:])
	fp.header.Size = binary.LittleEndian.Uint64(fp.Content[offset+8:])
	fp.header.FreeLimit = binary.LittleEndian.Uint32(fp.Content[offset+16:])
	fp.header.FreePages = binary.LittleEndian.Uint32(fp.Content[offset+20:])
	fp.header.FragPages = binary.LittleEndian.Uint32(fp.Content[offset+24:])

	// 更新统计信息
	fp.stats.LastModified = time.Now().UnixNano()
	fp.stats.Reads.Add(1)

	return nil
}

// Write 实现Page接口
func (fp *FSPPage) Write() error {
	// 写入FSP页面头
	offset := uint32(87)
	binary.LittleEndian.PutUint32(fp.Content[offset:], fp.header.SpaceID)
	binary.LittleEndian.PutUint32(fp.Content[offset+4:], fp.header.SpaceFlags)
	binary.LittleEndian.PutUint64(fp.Content[offset+8:], fp.header.Size)
	binary.LittleEndian.PutUint32(fp.Content[offset+16:], fp.header.FreeLimit)
	binary.LittleEndian.PutUint32(fp.Content[offset+20:], fp.header.FreePages)
	binary.LittleEndian.PutUint32(fp.Content[offset+24:], fp.header.FragPages)

	// 更新统计信息
	fp.stats.LastModified = time.Now().UnixNano()
	fp.stats.Writes.Add(1)

	return fp.BaseSystemPage.Write()
}

// Validate 实现SystemPage接口
func (fp *FSPPage) Validate() error {
	if err := fp.BaseSystemPage.Validate(); err != nil {
		return err
	}

	// 验证FSP页面
	if fp.header.SpaceID != fp.GetSpaceID() {
		return ErrInvalidSystemPage
	}

	if fp.header.FreeLimit > uint32(fp.header.Size) {
		return ErrInvalidSystemPage
	}

	return nil
}

// Recover 实现SystemPage接口
func (fp *FSPPage) Recover() error {
	if err := fp.BaseSystemPage.Recover(); err != nil {
		return err
	}

	// 重置属性
	fp.header.Properties = make(map[string]string)

	return nil
}
