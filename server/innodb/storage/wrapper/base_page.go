package wrapper

import (
	"sync"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/basic"
	"xmysql-server/server/innodb/latch"
)

// BasePage 基础页面结构
type BasePage struct {
	SpaceID uint32
	PageNo  uint32
	Type    common.PageType
	LSN     uint64
	State   basic.PageState
	Stats   *basic.PageStats
	Latch   *latch.Latch
	Dirty   bool
	Content []byte
	mutex   sync.RWMutex
}

// NewBasePage 创建一个新的基础页面
func NewBasePage(spaceID uint32, pageNo uint32, pageType common.PageType) *BasePage {
	return &BasePage{
		SpaceID: spaceID,
		PageNo:  pageNo,
		Type:    pageType,
		LSN:     0,
		State:   basic.PageStateActive,
		Stats:   &basic.PageStats{},
		Latch:   latch.NewLatch(),
		Dirty:   false,
		Content: make([]byte, 16384), // 16KB page size
	}
}

// Lock 获取写锁
func (bp *BasePage) Lock() {
	bp.mutex.Lock()
}

// Unlock 释放写锁
func (bp *BasePage) Unlock() {
	bp.mutex.Unlock()
}

// RLock 获取读锁
func (bp *BasePage) RLock() {
	bp.mutex.RLock()
}

// RUnlock 释放读锁
func (bp *BasePage) RUnlock() {
	bp.mutex.RUnlock()
}

// GetContent 获取页面内容
func (bp *BasePage) GetContent() []byte {
	return bp.Content
}

// SetContent 设置页面内容
func (bp *BasePage) SetContent(content []byte) {
	bp.Content = content
}

// GetID 获取页面ID
func (bp *BasePage) GetID() uint32 {
	return bp.PageNo
}

// GetSpaceID 获取空间ID
func (bp *BasePage) GetSpaceID() uint32 {
	return bp.SpaceID
}

// GetPageNo 获取页面号
func (bp *BasePage) GetPageNo() uint32 {
	return bp.PageNo
}

// GetPageType 获取页面类型
func (bp *BasePage) GetPageType() common.PageType {
	return bp.Type
}

// GetLSN 获取LSN
func (bp *BasePage) GetLSN() uint64 {
	return bp.LSN
}

// SetLSN 设置LSN
func (bp *BasePage) SetLSN(lsn uint64) {
	bp.LSN = lsn
}

// IsDirty 判断页面是否脏
func (bp *BasePage) IsDirty() bool {
	return bp.Dirty
}

// MarkDirty 标记页面为脏
func (bp *BasePage) MarkDirty() {
	bp.Dirty = true
}

// GetState 获取页面状态
func (bp *BasePage) GetState() basic.PageState {
	return bp.State
}

// SetState 设置页面状态
func (bp *BasePage) SetState(state basic.PageState) {
	bp.State = state
}

// GetStats 获取页面统计信息
func (bp *BasePage) GetStats() *basic.PageStats {
	return bp.Stats
}

// Pin 固定页面
func (bp *BasePage) Pin() {
	bp.Stats.PinCount++
}

// Unpin 解除页面固定
func (bp *BasePage) Unpin() {
	if bp.Stats.PinCount > 0 {
		bp.Stats.PinCount--
	}
}

// Read 读取页面内容
func (bp *BasePage) Read() error {
	bp.Stats.IncReadCount()
	bp.Stats.LastAccessed = basic.GetCurrentTimestamp()
	return nil
}

// Write 写入页面内容
func (bp *BasePage) Write() error {
	bp.Stats.IncWriteCount()
	bp.Stats.LastModified = basic.GetCurrentTimestamp()
	return nil
}
