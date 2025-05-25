package system

import (
	"encoding/binary"
	"errors"
	"time"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/storage/wrapper"
)

var (
	ErrInvalidSystemPage = errors.New("invalid system page")
	ErrCorruptedPage     = errors.New("corrupted system page")
)

// SystemPageHeader 系统页面头部
type SystemPageHeader struct {
	Version      uint32
	Checksum     uint64
	SystemType   SystemPageType
	SystemState  SystemPageState
	LastModified int64
}

// BaseSystemPage 系统页面基类
type BaseSystemPage struct {
	*wrapper.BasePage
	header SystemPageHeader
	stats  SystemPageStats
}

// NewBaseSystemPage 创建系统页面
func NewBaseSystemPage(spaceID, pageNo uint32, sysType SystemPageType) *BaseSystemPage {
	sp := &BaseSystemPage{
		BasePage: wrapper.NewBasePage(spaceID, pageNo, common.FIL_PAGE_TYPE_SYS),
	}

	// 初始化页面头
	sp.header = SystemPageHeader{
		Version:      1,
		SystemType:   sysType,
		SystemState:  SystemPageStateNormal,
		LastModified: time.Now().UnixNano(),
	}

	return sp
}

// GetSystemType 获取系统页面类型
func (sp *BaseSystemPage) GetSystemType() SystemPageType {
	return sp.header.SystemType
}

// GetSystemState 获取系统页面状态
func (sp *BaseSystemPage) GetSystemState() SystemPageState {
	return sp.header.SystemState
}

// SetSystemState 设置系统页面状态
func (sp *BaseSystemPage) SetSystemState(state SystemPageState) {
	sp.Lock()
	defer sp.Unlock()

	sp.header.SystemState = state
	sp.header.LastModified = time.Now().UnixNano()
	sp.MarkDirty()
}

// GetSystemStats 获取系统页面统计信息
func (sp *BaseSystemPage) GetSystemStats() *SystemPageStats {
	return &sp.stats
}

// Recover 恢复页面
func (sp *BaseSystemPage) Recover() error {
	sp.Lock()
	defer sp.Unlock()

	// 验证页面
	if err := sp.Validate(); err != nil {
		return err
	}

	// 设置恢复状态
	sp.header.SystemState = SystemPageStateRecovering
	sp.stats.Recoveries.Add(1)
	sp.stats.LastRecovered = time.Now().UnixNano()

	// 标记页面为脏
	sp.MarkDirty()

	return nil
}

// Validate 验证页面
func (sp *BaseSystemPage) Validate() error {
	sp.RLock()
	defer sp.RUnlock()

	// 验证页面类型
	if sp.GetPageType() != common.FIL_PAGE_TYPE_SYS {
		return ErrInvalidSystemPage
	}

	// 验证校验和
	if !sp.validateChecksum() {
		sp.stats.Corruptions.Add(1)
		return ErrCorruptedPage
	}

	return nil
}

// Backup 备份页面
func (sp *BaseSystemPage) Backup() error {
	sp.RLock()
	defer sp.RUnlock()

	// TODO: 实现页面备份
	return nil
}

// Restore 恢复页面
func (sp *BaseSystemPage) Restore() error {
	sp.Lock()
	defer sp.Unlock()

	// TODO: 实现页面恢复
	return nil
}

// validateChecksum 验证校验和
func (sp *BaseSystemPage) validateChecksum() bool {
	// TODO: 实现校验和验证
	return true
}

// updateChecksum 更新校验和
func (sp *BaseSystemPage) updateChecksum() {
	// TODO: 实现校验和计算
	sp.header.Checksum = 0
}

// Read 实现Page接口
func (sp *BaseSystemPage) Read() error {
	if err := sp.BasePage.Read(); err != nil {
		return err
	}

	// 读取系统页面头
	sp.header.Version = binary.LittleEndian.Uint32(sp.Content[64:])
	sp.header.Checksum = binary.LittleEndian.Uint64(sp.Content[68:])
	sp.header.SystemType = SystemPageType(binary.LittleEndian.Uint16(sp.Content[76:]))
	sp.header.SystemState = SystemPageState(sp.Content[78])
	sp.header.LastModified = int64(binary.LittleEndian.Uint64(sp.Content[79:]))

	// 更新统计信息
	sp.stats.Reads.Add(1)
	sp.stats.LastModified = time.Now().UnixNano()

	return nil
}

// Write 实现Page接口
func (sp *BaseSystemPage) Write() error {
	// 更新校验和
	sp.updateChecksum()

	// 写入系统页面头
	binary.LittleEndian.PutUint32(sp.Content[64:], sp.header.Version)
	binary.LittleEndian.PutUint64(sp.Content[68:], sp.header.Checksum)
	binary.LittleEndian.PutUint16(sp.Content[76:], uint16(sp.header.SystemType))
	sp.Content[78] = byte(sp.header.SystemState)
	binary.LittleEndian.PutUint64(sp.Content[79:], uint64(sp.header.LastModified))

	// 更新统计信息
	sp.stats.Writes.Add(1)
	sp.stats.LastModified = time.Now().UnixNano()

	return sp.BasePage.Write()
}
