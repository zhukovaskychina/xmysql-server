/*
FIL_PAGE_TYPE_SYS页面详细说明

基本属性：
- 页面类型：FIL_PAGE_TYPE_SYS
- 类型编码：0x0006（十进制6）
- 所属模块：InnoDB系统管理
- 页面作用：存储系统级别的元信息和配置

使用场景：
1. 存储表空间的系统信息
2. 维护系统表的元数据
3. 管理系统级配置参数
4. 记录系统状态信息

页面结构：
- File Header (38字节)
- System Header (32字节):
  - System Version: 系统版本 (4字节)
  - System Flags: 系统标志 (4字节)
  - Created Time: 创建时间 (8字节)
  - Modified Time: 修改时间 (8字节)
  - System ID: 系统ID (4字节)
  - Reserved: 保留字段 (4字节)
- System Data (16306字节): 系统数据
- File Trailer (8字节)
*/

package pages

import (
	"encoding/binary"
	"errors"
	"time"
	"xmysql-server/server/common"
)

// 系统页面常量
const (
	SystemHeaderSize = 32    // 系统头部大小
	SystemDataSize   = 16306 // 系统数据区大小 (16384 - 38 - 32 - 8)
)

// 系统标志
type SystemFlags uint32

const (
	SystemFlagInitialized SystemFlags = 1 << iota
	SystemFlagReadOnly
	SystemFlagMaintenance
	SystemFlagCorrupted
)

// 系统页面错误定义
var (
	ErrSystemPageCorrupted = errors.New("系统页面损坏")
	ErrSystemPageReadOnly  = errors.New("系统页面只读")
	ErrInvalidSystemData   = errors.New("无效的系统数据")
)

// SystemHeader 系统页面头部结构
type SystemHeader struct {
	Version      uint32      // 系统版本
	Flags        SystemFlags // 系统标志
	CreatedTime  int64       // 创建时间
	ModifiedTime int64       // 修改时间
	SystemID     uint32      // 系统ID
	Reserved     uint32      // 保留字段
}

// SystemPage 系统页面结构
type SystemPage struct {
	FileHeader   FileHeader   // 文件头部 (38字节)
	SystemHeader SystemHeader // 系统头部 (32字节)
	SystemData   []byte       // 系统数据 (16306字节)
	FileTrailer  FileTrailer  // 文件尾部 (8字节)
}

// NewSystemPage 创建新的系统页面
func NewSystemPage(spaceID, pageNo uint32, systemID uint32) *SystemPage {
	now := time.Now().UnixNano()

	page := &SystemPage{
		FileHeader: NewFileHeader(),
		SystemHeader: SystemHeader{
			Version:      1,
			Flags:        SystemFlagInitialized,
			CreatedTime:  now,
			ModifiedTime: now,
			SystemID:     systemID,
			Reserved:     0,
		},
		SystemData:  make([]byte, SystemDataSize),
		FileTrailer: NewFileTrailer(),
	}

	// 设置页面头部信息
	page.FileHeader.WritePageOffset(pageNo)
	page.FileHeader.WritePageFileType(int16(common.FIL_PAGE_TYPE_SYS))
	page.FileHeader.WritePageArch(spaceID)

	return page
}

// SetSystemData 设置系统数据
func (sp *SystemPage) SetSystemData(data []byte) error {
	if len(data) > SystemDataSize {
		return ErrInvalidSystemData
	}

	// 清空数据区并复制新数据
	for i := range sp.SystemData {
		sp.SystemData[i] = 0
	}
	copy(sp.SystemData, data)

	// 更新修改时间
	sp.SystemHeader.ModifiedTime = time.Now().UnixNano()

	return nil
}

// GetSystemData 获取系统数据
func (sp *SystemPage) GetSystemData() []byte {
	// 找到实际数据的结尾（忽略尾部的零字节）
	end := len(sp.SystemData)
	for end > 0 && sp.SystemData[end-1] == 0 {
		end--
	}
	return sp.SystemData[:end]
}

// SetFlag 设置系统标志
func (sp *SystemPage) SetFlag(flag SystemFlags) {
	sp.SystemHeader.Flags |= flag
	sp.SystemHeader.ModifiedTime = time.Now().UnixNano()
}

// ClearFlag 清除系统标志
func (sp *SystemPage) ClearFlag(flag SystemFlags) {
	sp.SystemHeader.Flags &^= flag
	sp.SystemHeader.ModifiedTime = time.Now().UnixNano()
}

// HasFlag 检查是否包含指定标志
func (sp *SystemPage) HasFlag(flag SystemFlags) bool {
	return sp.SystemHeader.Flags&flag != 0
}

// IsReadOnly 检查是否为只读模式
func (sp *SystemPage) IsReadOnly() bool {
	return sp.HasFlag(SystemFlagReadOnly)
}

// IsCorrupted 检查是否损坏
func (sp *SystemPage) IsCorrupted() bool {
	return sp.HasFlag(SystemFlagCorrupted)
}

// IsInitialized 检查是否已初始化
func (sp *SystemPage) IsInitialized() bool {
	return sp.HasFlag(SystemFlagInitialized)
}

// GetVersion 获取系统版本
func (sp *SystemPage) GetVersion() uint32 {
	return sp.SystemHeader.Version
}

// GetSystemID 获取系统ID
func (sp *SystemPage) GetSystemID() uint32 {
	return sp.SystemHeader.SystemID
}

// GetCreatedTime 获取创建时间
func (sp *SystemPage) GetCreatedTime() int64 {
	return sp.SystemHeader.CreatedTime
}

// GetModifiedTime 获取修改时间
func (sp *SystemPage) GetModifiedTime() int64 {
	return sp.SystemHeader.ModifiedTime
}

// Serialize 序列化页面为字节数组
func (sp *SystemPage) Serialize() []byte {
	data := make([]byte, common.PageSize)
	offset := 0

	// 序列化文件头部
	copy(data[offset:], sp.serializeFileHeader())
	offset += FileHeaderSize

	// 序列化系统头部
	copy(data[offset:], sp.serializeSystemHeader())
	offset += SystemHeaderSize

	// 序列化系统数据
	copy(data[offset:], sp.SystemData)
	offset += SystemDataSize

	// 序列化文件尾部
	copy(data[offset:], sp.serializeFileTrailer())

	return data
}

// Deserialize 从字节数组反序列化页面
func (sp *SystemPage) Deserialize(data []byte) error {
	if len(data) != common.PageSize {
		return ErrInvalidPageSize
	}

	offset := 0

	// 反序列化文件头部
	if err := sp.deserializeFileHeader(data[offset : offset+FileHeaderSize]); err != nil {
		return err
	}
	offset += FileHeaderSize

	// 反序列化系统头部
	if err := sp.deserializeSystemHeader(data[offset : offset+SystemHeaderSize]); err != nil {
		return err
	}
	offset += SystemHeaderSize

	// 反序列化系统数据
	copy(sp.SystemData, data[offset:offset+SystemDataSize])
	offset += SystemDataSize

	// 反序列化文件尾部
	if err := sp.deserializeFileTrailer(data[offset : offset+FileTrailerSize]); err != nil {
		return err
	}

	return nil
}

// serializeFileHeader 序列化文件头部
func (sp *SystemPage) serializeFileHeader() []byte {
	// 实现文件头部序列化逻辑
	data := make([]byte, FileHeaderSize)
	// 这里应该包含具体的序列化逻辑
	return data
}

// serializeSystemHeader 序列化系统头部
func (sp *SystemPage) serializeSystemHeader() []byte {
	data := make([]byte, SystemHeaderSize)

	binary.LittleEndian.PutUint32(data[0:], sp.SystemHeader.Version)
	binary.LittleEndian.PutUint32(data[4:], uint32(sp.SystemHeader.Flags))
	binary.LittleEndian.PutUint64(data[8:], uint64(sp.SystemHeader.CreatedTime))
	binary.LittleEndian.PutUint64(data[16:], uint64(sp.SystemHeader.ModifiedTime))
	binary.LittleEndian.PutUint32(data[24:], sp.SystemHeader.SystemID)
	binary.LittleEndian.PutUint32(data[28:], sp.SystemHeader.Reserved)

	return data
}

// serializeFileTrailer 序列化文件尾部
func (sp *SystemPage) serializeFileTrailer() []byte {
	// 实现文件尾部序列化逻辑
	data := make([]byte, FileTrailerSize)
	// 这里应该包含具体的序列化逻辑
	return data
}

// deserializeFileHeader 反序列化文件头部
func (sp *SystemPage) deserializeFileHeader(data []byte) error {
	// 实现文件头部反序列化逻辑
	return nil
}

// deserializeSystemHeader 反序列化系统头部
func (sp *SystemPage) deserializeSystemHeader(data []byte) error {
	if len(data) < SystemHeaderSize {
		return ErrInvalidPageSize
	}

	sp.SystemHeader.Version = binary.LittleEndian.Uint32(data[0:])
	sp.SystemHeader.Flags = SystemFlags(binary.LittleEndian.Uint32(data[4:]))
	sp.SystemHeader.CreatedTime = int64(binary.LittleEndian.Uint64(data[8:]))
	sp.SystemHeader.ModifiedTime = int64(binary.LittleEndian.Uint64(data[16:]))
	sp.SystemHeader.SystemID = binary.LittleEndian.Uint32(data[24:])
	sp.SystemHeader.Reserved = binary.LittleEndian.Uint32(data[28:])

	return nil
}

// deserializeFileTrailer 反序列化文件尾部
func (sp *SystemPage) deserializeFileTrailer(data []byte) error {
	// 实现文件尾部反序列化逻辑
	return nil
}

// Validate 验证页面数据完整性
func (sp *SystemPage) Validate() error {
	// 检查是否损坏
	if sp.IsCorrupted() {
		return ErrSystemPageCorrupted
	}

	// 验证系统版本
	if sp.SystemHeader.Version == 0 {
		return ErrInvalidSystemData
	}

	// 验证时间戳
	if sp.SystemHeader.CreatedTime > sp.SystemHeader.ModifiedTime {
		return ErrInvalidSystemData
	}

	return nil
}

// GetFileHeader 获取文件头部
func (sp *SystemPage) GetFileHeader() FileHeader {
	return sp.FileHeader
}

// GetFileTrailer 获取文件尾部
func (sp *SystemPage) GetFileTrailer() FileTrailer {
	return sp.FileTrailer
}

// GetSerializeBytes 获取序列化后的字节数组
func (sp *SystemPage) GetSerializeBytes() []byte {
	return sp.Serialize()
}

// LoadFileHeader 加载文件头部
func (sp *SystemPage) LoadFileHeader(content []byte) {
	sp.deserializeFileHeader(content)
}

// LoadFileTrailer 加载文件尾部
func (sp *SystemPage) LoadFileTrailer(content []byte) {
	sp.deserializeFileTrailer(content)
}
