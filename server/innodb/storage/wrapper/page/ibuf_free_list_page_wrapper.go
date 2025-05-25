package page

import (
	"errors"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/storage/store/pages"
)

var (
	ErrIBufFreeListFull     = errors.New("插入缓冲空闲列表已满")
	ErrIBufPageNotFound     = errors.New("插入缓冲页面未找到")
	ErrInvalidIBufPageEntry = errors.New("无效的插入缓冲页面条目")
)

// IBufFreeListPageWrapper 插入缓冲空闲列表页面包装器
type IBufFreeListPageWrapper struct {
	*BasePageWrapper

	// 底层的插入缓冲空闲列表页面实现
	ibufFreeListPage *pages.IBufFreeListPage
}

// NewIBufFreeListPageWrapper 创建新的插入缓冲空闲列表页面包装器
func NewIBufFreeListPageWrapper(id, spaceID uint32) *IBufFreeListPageWrapper {
	base := NewBasePageWrapper(id, spaceID, common.FIL_PAGE_IBUF_FREE_LIST)
	ibufPage := pages.NewIBufFreeListPage(spaceID, id)

	return &IBufFreeListPageWrapper{
		BasePageWrapper:  base,
		ibufFreeListPage: ibufPage,
	}
}

// 实现IPageWrapper接口

// ParseFromBytes 从字节数据解析插入缓冲空闲列表页面
func (iflpw *IBufFreeListPageWrapper) ParseFromBytes(data []byte) error {
	iflpw.Lock()
	defer iflpw.Unlock()

	if err := iflpw.BasePageWrapper.ParseFromBytes(data); err != nil {
		return err
	}

	// 解析插入缓冲空闲列表页面特有的数据
	if err := iflpw.ibufFreeListPage.Deserialize(data); err != nil {
		return err
	}

	return nil
}

// ToBytes 序列化插入缓冲空闲列表页面为字节数组
func (iflpw *IBufFreeListPageWrapper) ToBytes() ([]byte, error) {
	iflpw.RLock()
	defer iflpw.RUnlock()

	// 序列化插入缓冲空闲列表页面
	data := iflpw.ibufFreeListPage.Serialize()

	// 更新基础包装器的内容
	if len(iflpw.content) != len(data) {
		iflpw.content = make([]byte, len(data))
	}
	copy(iflpw.content, data)

	return data, nil
}

// 插入缓冲空闲列表页面特有的方法

// AddFreePage 添加空闲页面
func (iflpw *IBufFreeListPageWrapper) AddFreePage(pageNo uint32) error {
	iflpw.Lock()
	defer iflpw.Unlock()

	if err := iflpw.ibufFreeListPage.AddFreePage(pageNo); err != nil {
		return err
	}

	iflpw.MarkDirty()
	return nil
}

// AllocatePage 分配一个空闲页面
func (iflpw *IBufFreeListPageWrapper) AllocatePage() (uint32, error) {
	iflpw.Lock()
	defer iflpw.Unlock()

	pageNo, err := iflpw.ibufFreeListPage.AllocatePage()
	if err != nil {
		return 0, err
	}

	iflpw.MarkDirty()
	return pageNo, nil
}

// FreePage 释放一个页面
func (iflpw *IBufFreeListPageWrapper) FreePage(pageNo uint32) error {
	iflpw.Lock()
	defer iflpw.Unlock()

	if err := iflpw.ibufFreeListPage.FreePage(pageNo); err != nil {
		return err
	}

	iflpw.MarkDirty()
	return nil
}

// MarkPageInUse 标记页面为使用中
func (iflpw *IBufFreeListPageWrapper) MarkPageInUse(pageNo uint32) error {
	iflpw.Lock()
	defer iflpw.Unlock()

	if err := iflpw.ibufFreeListPage.MarkPageInUse(pageNo); err != nil {
		return err
	}

	iflpw.MarkDirty()
	return nil
}

// GetPageStatus 获取页面状态
func (iflpw *IBufFreeListPageWrapper) GetPageStatus(pageNo uint32) (pages.IBufPageStatus, error) {
	iflpw.RLock()
	defer iflpw.RUnlock()

	return iflpw.ibufFreeListPage.GetPageStatus(pageNo)
}

// GetFreePages 获取所有空闲页面
func (iflpw *IBufFreeListPageWrapper) GetFreePages() []uint32 {
	iflpw.RLock()
	defer iflpw.RUnlock()

	return iflpw.ibufFreeListPage.GetFreePages()
}

// SetNextListPage 设置下一个列表页面
func (iflpw *IBufFreeListPageWrapper) SetNextListPage(pageNo uint32) {
	iflpw.Lock()
	defer iflpw.Unlock()

	iflpw.ibufFreeListPage.SetNextListPage(pageNo)
	iflpw.MarkDirty()
}

// GetNextListPage 获取下一个列表页面
func (iflpw *IBufFreeListPageWrapper) GetNextListPage() uint32 {
	iflpw.RLock()
	defer iflpw.RUnlock()

	return iflpw.ibufFreeListPage.GetNextListPage()
}

// GetFreeCount 获取空闲页面数量
func (iflpw *IBufFreeListPageWrapper) GetFreeCount() uint32 {
	iflpw.RLock()
	defer iflpw.RUnlock()

	return iflpw.ibufFreeListPage.GetFreeCount()
}

// GetUsedCount 获取已使用页面数量
func (iflpw *IBufFreeListPageWrapper) GetUsedCount() uint32 {
	iflpw.RLock()
	defer iflpw.RUnlock()

	return iflpw.ibufFreeListPage.GetUsedCount()
}

// GetTotalCount 获取总页面数量
func (iflpw *IBufFreeListPageWrapper) GetTotalCount() uint32 {
	iflpw.RLock()
	defer iflpw.RUnlock()

	return iflpw.ibufFreeListPage.GetTotalCount()
}

// Validate 验证插入缓冲空闲列表页面数据完整性
func (iflpw *IBufFreeListPageWrapper) Validate() error {
	iflpw.RLock()
	defer iflpw.RUnlock()

	return iflpw.ibufFreeListPage.Validate()
}

// GetIBufFreeListPage 获取底层的插入缓冲空闲列表页面实现
func (iflpw *IBufFreeListPageWrapper) GetIBufFreeListPage() *pages.IBufFreeListPage {
	return iflpw.ibufFreeListPage
}
