package types

import (
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"time"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/basic"
)

// PageType represents the type of a page
type PageType = common.PageType

var (
	ErrInvalidPage = errors.New("invalid page")
	ErrPageLocked  = errors.New("page locked")
)

// BasePage 基础页面实现
type BasePage struct {
	rawPage  *PageHeader // 使用原始页面结构
	state    atomic.Uint32
	stats    basic.PageStats
	pinCount atomic.Int32
	dirty    atomic.Bool
	lock     sync.RWMutex
}

// NewBasePage 创建基础页面
func NewBasePage(spaceID, pageNo uint32, pageType PageType) *BasePage {
	page := &BasePage{
		rawPage: NewPageHeader(16384), // 默认16KB页面大小
	}

	// 设置页面头
	binary.BigEndian.PutUint32(page.rawPage.FileHeader[FHeaderSpaceID:], spaceID)
	binary.BigEndian.PutUint32(page.rawPage.FileHeader[FHeaderPageNo:], pageNo)
	binary.BigEndian.PutUint16(page.rawPage.FileHeader[FHeaderPageType:], uint16(pageType))

	// 初始化统计信息
	page.stats.LastAccessAt = uint64(time.Now().UnixNano())
	page.stats.ReadCount = 0
	page.stats.WriteCount = 0

	return page
}

// GetFileHeader 获取文件头
func (p *BasePage) GetFileHeader() []byte {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.rawPage.FileHeader
}

// GetFileTrailer 获取文件尾
func (p *BasePage) GetFileTrailer() []byte {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.rawPage.FileTrailer
}

// GetSpaceID 获取表空间ID
func (p *BasePage) GetSpaceID() uint32 {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return binary.BigEndian.Uint32(p.rawPage.FileHeader[FHeaderSpaceID:])
}

// GetPageNo 获取页号
func (p *BasePage) GetPageNo() uint32 {
	return binary.BigEndian.Uint32(p.rawPage.FileHeader[FHeaderPageNo:])
}

// GetPageType 获取页面类型
func (p *BasePage) GetPageType() uint16 {
	return binary.BigEndian.Uint16(p.rawPage.FileHeader[FHeaderPageType:])
}

// GetLSN 获取LSN
func (p *BasePage) GetLSN() uint64 {
	return binary.BigEndian.Uint64(p.rawPage.FileHeader[FHeaderLSN:])
}

// SetLSN 设置LSN
func (p *BasePage) SetLSN(lsn uint64) {
	binary.BigEndian.PutUint64(p.rawPage.FileHeader[FHeaderLSN:], lsn)
}

// IsDirty 是否脏页
func (p *BasePage) IsDirty() bool {
	return p.dirty.Load()
}

// MarkDirty 标记为脏页
func (p *BasePage) MarkDirty() {
	p.dirty.Store(true)
}

// GetState 获取页面状态
func (p *BasePage) GetState() basic.PageState {
	return basic.PageState(p.state.Load())
}

// SetState 设置页面状态
func (p *BasePage) SetState(state basic.PageState) {
	p.state.Store(uint32(state))
}

// GetStats 获取统计信息
func (p *BasePage) GetStats() *basic.PageStats {
	return &p.stats
}

// Pin 固定页面
func (p *BasePage) Pin() {
	p.pinCount.Add(1)
}

// Unpin 解除固定
func (p *BasePage) Unpin() {
	p.pinCount.Add(-1)
}

// Read 读取页面
func (p *BasePage) Read() error {
	// 更新统计信息
	p.stats.LastAccessAt = uint64(time.Now().UnixNano())
	p.stats.ReadCount++
	return nil
}

// Write 写入页面
func (p *BasePage) Write() error {
	// 更新统计信息
	p.stats.LastAccessAt = uint64(time.Now().UnixNano())
	p.stats.WriteCount++
	return nil
}
