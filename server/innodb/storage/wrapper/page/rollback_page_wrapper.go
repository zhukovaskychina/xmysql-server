package page

import (
	"errors"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/storage/store/pages"
)

var (
	ErrRollbackPageFull    = errors.New("回滚页面已满")
	ErrInvalidRollbackData = errors.New("无效的回滚数据")
	ErrUndoSlotNotFound    = errors.New("Undo slot未找到")
)

// RollbackPageWrapper 回滚页面包装器
type RollbackPageWrapper struct {
	*BasePageWrapper

	// 底层的回滚页面实现
	rollbackPage *pages.RollBackPage
}

// NewRollbackPageWrapper 创建新的回滚页面包装器
func NewRollbackPageWrapper(id, spaceID uint32) *RollbackPageWrapper {
	base := NewBasePageWrapper(id, spaceID, common.FIL_PAGE_UNDO_LOG)
	rollbackPage := pages.NewRollBackPage()

	return &RollbackPageWrapper{
		BasePageWrapper: base,
		rollbackPage:    rollbackPage,
	}
}

// 实现IPageWrapper接口

// ParseFromBytes 从字节数据解析回滚页面
func (rpw *RollbackPageWrapper) ParseFromBytes(data []byte) error {
	rpw.Lock()
	defer rpw.Unlock()

	if err := rpw.BasePageWrapper.ParseFromBytes(data); err != nil {
		return err
	}

	// 解析回滚页面特有的数据
	// 由于store/pages中的RollBackPage实现较简单，这里做基本的数据解析
	if len(data) < common.PageSize {
		return ErrInvalidRollbackData
	}

	// TODO: 根据实际的rollback page结构进行解析
	// 这里需要完善具体的解析逻辑

	return nil
}

// ToBytes 序列化回滚页面为字节数组
func (rpw *RollbackPageWrapper) ToBytes() ([]byte, error) {
	rpw.RLock()
	defer rpw.RUnlock()

	// 由于store/pages中的RollBackPage没有完整的序列化方法
	// 这里使用基础包装器的内容
	return rpw.BasePageWrapper.ToBytes()
}

// 回滚页面特有的方法

// GetTrxRsegMaxSize 获取回滚段最大大小
func (rpw *RollbackPageWrapper) GetTrxRsegMaxSize() []byte {
	rpw.RLock()
	defer rpw.RUnlock()

	if rpw.rollbackPage != nil {
		return rpw.rollbackPage.TrxRsegMaxSize
	}
	return nil
}

// SetTrxRsegMaxSize 设置回滚段最大大小
func (rpw *RollbackPageWrapper) SetTrxRsegMaxSize(maxSize []byte) {
	rpw.Lock()
	defer rpw.Unlock()

	if rpw.rollbackPage != nil {
		rpw.rollbackPage.TrxRsegMaxSize = make([]byte, len(maxSize))
		copy(rpw.rollbackPage.TrxRsegMaxSize, maxSize)
		rpw.MarkDirty()
	}
}

// GetTrxRsegHistorySize 获取History链表占用的页面数量
func (rpw *RollbackPageWrapper) GetTrxRsegHistorySize() []byte {
	rpw.RLock()
	defer rpw.RUnlock()

	if rpw.rollbackPage != nil {
		return rpw.rollbackPage.TrxRsegHistorySize
	}
	return nil
}

// SetTrxRsegHistorySize 设置History链表占用的页面数量
func (rpw *RollbackPageWrapper) SetTrxRsegHistorySize(historySize []byte) {
	rpw.Lock()
	defer rpw.Unlock()

	if rpw.rollbackPage != nil {
		rpw.rollbackPage.TrxRsegHistorySize = make([]byte, len(historySize))
		copy(rpw.rollbackPage.TrxRsegHistorySize, historySize)
		rpw.MarkDirty()
	}
}

// GetTrxRsegHistory 获取History链表的基节点
func (rpw *RollbackPageWrapper) GetTrxRsegHistory() []byte {
	rpw.RLock()
	defer rpw.RUnlock()

	if rpw.rollbackPage != nil {
		return rpw.rollbackPage.TrxRsegHistory
	}
	return nil
}

// SetTrxRsegHistory 设置History链表的基节点
func (rpw *RollbackPageWrapper) SetTrxRsegHistory(history []byte) {
	rpw.Lock()
	defer rpw.Unlock()

	if rpw.rollbackPage != nil {
		rpw.rollbackPage.TrxRsegHistory = make([]byte, len(history))
		copy(rpw.rollbackPage.TrxRsegHistory, history)
		rpw.MarkDirty()
	}
}

// GetTrxRsegFsegHeader 获取对应的段空间header
func (rpw *RollbackPageWrapper) GetTrxRsegFsegHeader() []byte {
	rpw.RLock()
	defer rpw.RUnlock()

	if rpw.rollbackPage != nil {
		return rpw.rollbackPage.TrxRsegFsegHeader
	}
	return nil
}

// SetTrxRsegFsegHeader 设置对应的段空间header
func (rpw *RollbackPageWrapper) SetTrxRsegFsegHeader(fsegHeader []byte) {
	rpw.Lock()
	defer rpw.Unlock()

	if rpw.rollbackPage != nil {
		rpw.rollbackPage.TrxRsegFsegHeader = make([]byte, len(fsegHeader))
		copy(rpw.rollbackPage.TrxRsegFsegHeader, fsegHeader)
		rpw.MarkDirty()
	}
}

// GetTrxRsegUndoSlots 获取undo slot集合
func (rpw *RollbackPageWrapper) GetTrxRsegUndoSlots() []byte {
	rpw.RLock()
	defer rpw.RUnlock()

	if rpw.rollbackPage != nil {
		return rpw.rollbackPage.TrxRsegUndoSlots
	}
	return nil
}

// SetTrxRsegUndoSlots 设置undo slot集合
func (rpw *RollbackPageWrapper) SetTrxRsegUndoSlots(undoSlots []byte) {
	rpw.Lock()
	defer rpw.Unlock()

	if rpw.rollbackPage != nil {
		rpw.rollbackPage.TrxRsegUndoSlots = make([]byte, len(undoSlots))
		copy(rpw.rollbackPage.TrxRsegUndoSlots, undoSlots)
		rpw.MarkDirty()
	}
}

// GetUndoSlot 获取指定索引的undo slot
func (rpw *RollbackPageWrapper) GetUndoSlot(index int) (uint32, error) {
	rpw.RLock()
	defer rpw.RUnlock()

	if rpw.rollbackPage == nil || rpw.rollbackPage.TrxRsegUndoSlots == nil {
		return 0, ErrUndoSlotNotFound
	}

	slotSize := 4 // 每个slot 4字节
	if index < 0 || index*slotSize+slotSize > len(rpw.rollbackPage.TrxRsegUndoSlots) {
		return 0, ErrUndoSlotNotFound
	}

	slotData := rpw.rollbackPage.TrxRsegUndoSlots[index*slotSize : index*slotSize+slotSize]

	// 将4字节转换为uint32
	var pageNo uint32
	if len(slotData) >= 4 {
		pageNo = uint32(slotData[0]) |
			uint32(slotData[1])<<8 |
			uint32(slotData[2])<<16 |
			uint32(slotData[3])<<24
	}

	return pageNo, nil
}

// SetUndoSlot 设置指定索引的undo slot
func (rpw *RollbackPageWrapper) SetUndoSlot(index int, pageNo uint32) error {
	rpw.Lock()
	defer rpw.Unlock()

	if rpw.rollbackPage == nil {
		return ErrInvalidRollbackData
	}

	// 确保undo slots存在且足够大
	slotSize := 4
	requiredSize := (index + 1) * slotSize
	if len(rpw.rollbackPage.TrxRsegUndoSlots) < requiredSize {
		newSlots := make([]byte, requiredSize)
		if rpw.rollbackPage.TrxRsegUndoSlots != nil {
			copy(newSlots, rpw.rollbackPage.TrxRsegUndoSlots)
		}
		rpw.rollbackPage.TrxRsegUndoSlots = newSlots
	}

	// 设置slot值
	offset := index * slotSize
	rpw.rollbackPage.TrxRsegUndoSlots[offset] = byte(pageNo)
	rpw.rollbackPage.TrxRsegUndoSlots[offset+1] = byte(pageNo >> 8)
	rpw.rollbackPage.TrxRsegUndoSlots[offset+2] = byte(pageNo >> 16)
	rpw.rollbackPage.TrxRsegUndoSlots[offset+3] = byte(pageNo >> 24)

	rpw.MarkDirty()
	return nil
}

// GetUndoSlotCount 获取undo slot的数量
func (rpw *RollbackPageWrapper) GetUndoSlotCount() int {
	rpw.RLock()
	defer rpw.RUnlock()

	if rpw.rollbackPage == nil || rpw.rollbackPage.TrxRsegUndoSlots == nil {
		return 0
	}

	return len(rpw.rollbackPage.TrxRsegUndoSlots) / 4 // 每个slot 4字节
}

// GetEmptySpace 获取空闲空间
func (rpw *RollbackPageWrapper) GetEmptySpace() []byte {
	rpw.RLock()
	defer rpw.RUnlock()

	if rpw.rollbackPage != nil {
		return rpw.rollbackPage.EmptySpace
	}
	return nil
}

// Validate 验证回滚页面数据完整性
func (rpw *RollbackPageWrapper) Validate() error {
	rpw.RLock()
	defer rpw.RUnlock()

	if rpw.rollbackPage == nil {
		return ErrInvalidRollbackData
	}

	// 检查基本字段的长度
	if len(rpw.rollbackPage.TrxRsegMaxSize) != 0 && len(rpw.rollbackPage.TrxRsegMaxSize) != 4 {
		return ErrInvalidRollbackData
	}

	if len(rpw.rollbackPage.TrxRsegHistorySize) != 0 && len(rpw.rollbackPage.TrxRsegHistorySize) != 4 {
		return ErrInvalidRollbackData
	}

	if len(rpw.rollbackPage.TrxRsegHistory) != 0 && len(rpw.rollbackPage.TrxRsegHistory) != 20 {
		return ErrInvalidRollbackData
	}

	if len(rpw.rollbackPage.TrxRsegFsegHeader) != 0 && len(rpw.rollbackPage.TrxRsegFsegHeader) != 10 {
		return ErrInvalidRollbackData
	}

	return nil
}

// GetRollbackPage 获取底层的回滚页面实现
func (rpw *RollbackPageWrapper) GetRollbackPage() *pages.RollBackPage {
	return rpw.rollbackPage
}
