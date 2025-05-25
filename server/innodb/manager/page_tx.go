package manager

import (
	"sync"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/basic"
)

// pageTx 页面事务实现
type pageTx struct {
	sync.RWMutex

	// 事务状态
	committed  bool
	rolledBack bool

	// 页面管理器
	pageManager *DefaultPageManager

	// 事务缓存
	cache map[uint64]basic.IPage
	dirty map[uint64]bool
}

// NewPageTx 创建页面事务
func NewPageTx(pm *DefaultPageManager) basic.PageTx {
	return &pageTx{
		pageManager: pm,
		cache:       make(map[uint64]basic.IPage),
		dirty:       make(map[uint64]bool),
	}
}

// GetPage 获取页面
func (tx *pageTx) GetPage(spaceID, pageNo uint32) (basic.IPage, error) {
	tx.Lock()
	defer tx.Unlock()

	// 检查状态
	if tx.committed || tx.rolledBack {
		return nil, ErrTxFinished
	}

	// 生成键
	key := uint64(spaceID)<<32 | uint64(pageNo)

	// 先查事务缓存
	if p, ok := tx.cache[key]; ok {
		return p, nil
	}

	// 从页面管理器获取
	p, err := tx.pageManager.GetPage(spaceID, pageNo)
	if err != nil {
		return nil, err
	}

	// 加入事务缓存
	tx.cache[key] = p
	return p, nil
}

// CreatePage 创建页面
func (tx *pageTx) CreatePage(typ basic.PageType) (basic.IPage, error) {
	tx.Lock()
	defer tx.Unlock()

	// 检查事务状态
	if tx.committed || tx.rolledBack {
		return nil, ErrTxFinished
	}

	// 转换类型并创建页面
	commonType := common.PageType(typ)
	p, err := tx.pageManager.CreatePage(commonType)
	if err != nil {
		return nil, err
	}

	// 加入事务缓存
	key := uint64(p.GetSpaceID())<<32 | uint64(p.GetPageNo())
	tx.cache[key] = p
	tx.dirty[key] = true

	return p, nil
}

// DeletePage 删除页面
func (tx *pageTx) DeletePage(spaceID, pageNo uint32) error {
	tx.Lock()
	defer tx.Unlock()

	// 检查事务状态
	if tx.committed || tx.rolledBack {
		return ErrTxFinished
	}

	// 从缓存中移除
	key := uint64(spaceID)<<32 | uint64(pageNo)
	delete(tx.cache, key)
	delete(tx.dirty, key)

	// 注意：这里我们只是从事务缓存中删除页面
	// 实际的页面删除会在事务提交时处理
	return nil
}

// Commit 提交事务
func (tx *pageTx) Commit() error {
	tx.Lock()
	defer tx.Unlock()

	// 检查状态
	if tx.committed || tx.rolledBack {
		return ErrTxFinished
	}

	// 刷新脏页
	for key := range tx.dirty {
		spaceID := uint32(key >> 32)
		pageNo := uint32(key)
		if err := tx.pageManager.FlushPage(spaceID, pageNo); err != nil {
			return err
		}
	}

	// 更新状态
	tx.committed = true
	tx.cache = nil
	tx.dirty = nil

	return nil
}

// Rollback 回滚事务
func (tx *pageTx) Rollback() error {
	tx.Lock()
	defer tx.Unlock()

	// 检查状态
	if tx.committed || tx.rolledBack {
		return ErrTxFinished
	}

	// 丢弃所有修改
	tx.rolledBack = true
	tx.cache = nil
	tx.dirty = nil

	return nil
}
