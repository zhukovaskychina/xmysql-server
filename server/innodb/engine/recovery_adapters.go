package engine

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

// RecoveryBufferPoolAdapter adapts OptimizedBufferPoolManager to manager.BufferPoolInterface
type RecoveryBufferPoolAdapter struct {
	bpm *manager.OptimizedBufferPoolManager
}

func (a *RecoveryBufferPoolAdapter) FetchPage(pageID uint64) (manager.PageInterface, error) {
	spaceID := uint32(pageID >> 32)
	pageNo := uint32(pageID)
	bp, err := a.bpm.GetPage(spaceID, pageNo)
	if err != nil {
		return nil, err
	}
	return &PageAdapter{bp: bp}, nil
}

func (a *RecoveryBufferPoolAdapter) UnpinPage(pageID uint64, isDirty bool) error {
	spaceID := uint32(pageID >> 32)
	pageNo := uint32(pageID)
	if isDirty {
		a.bpm.MarkDirty(spaceID, pageNo)
	}
	return a.bpm.UnpinPage(spaceID, pageNo)
}

func (a *RecoveryBufferPoolAdapter) FlushPage(pageID uint64) error {
	spaceID := uint32(pageID >> 32)
	pageNo := uint32(pageID)
	return a.bpm.FlushPage(spaceID, pageNo)
}

// PageAdapter adapts buffer_pool.BufferPage to manager.PageInterface
type PageAdapter struct {
	bp *buffer_pool.BufferPage
}

func (p *PageAdapter) GetPageID() uint64 {
	return uint64(p.bp.GetSpaceID())<<32 | uint64(p.bp.GetPageNo())
}
func (p *PageAdapter) GetLSN() uint64      { return p.bp.GetLSN() }
func (p *PageAdapter) SetLSN(lsn uint64)   { p.bp.SetLSN(lsn) }
func (p *PageAdapter) GetData() []byte     { return p.bp.GetContent() }
func (p *PageAdapter) SetData(data []byte) { p.bp.SetContent(data) }
func (p *PageAdapter) IsDirty() bool       { return p.bp.IsDirty() }
func (p *PageAdapter) SetDirty(dirty bool) { p.bp.SetDirty(dirty) }

// RecoveryStorageAdapter adapts StorageManager to manager.StorageInterface
type RecoveryStorageAdapter struct {
	sm *manager.StorageManager
}

func (a *RecoveryStorageAdapter) ReadPage(pageID uint64) ([]byte, error) {
	spaceID := uint32(pageID >> 32)
	pageNo := uint32(pageID)
	// StorageManager.GetPage returns basic.IPage
	page, err := a.sm.GetPage(spaceID, pageNo)
	if err != nil {
		return nil, err
	}
	return page.GetContent(), nil
}

func (a *RecoveryStorageAdapter) WritePage(pageID uint64, data []byte) error {
	spaceID := uint32(pageID >> 32)
	pageNo := uint32(pageID)

	// First get the page (loading into buffer pool if necessary)
	page, err := a.sm.GetPage(spaceID, pageNo)
	if err != nil {
		// If page doesn't exist in buffer pool or storage, we might be writing a new page
		// But ReadPage checks existence.
		return fmt.Errorf("failed to get page for writing: %v", err)
	}

	// Update content
	page.SetContent(data)

	// Flush to disk using PageManager (which StorageManager uses)
	// StorageManager.FreePage calls FlushPage. But we don't want to free it.
	// We need a way to Flush.
	// StorageManager.Sync(spaceID) flushes everything.
	// We need single page flush.
	// We can use the buffer pool manager associated with storage manager to flush.

	bpm := a.sm.GetBufferPoolManager()
	if bpm == nil {
		return fmt.Errorf("buffer pool manager not available")
	}

	// Mark dirty and flush
	bpm.MarkDirty(spaceID, pageNo)
	return bpm.FlushPage(spaceID, pageNo)
}

func (a *RecoveryStorageAdapter) CreatePage(pageID uint64) error {
	spaceID := uint32(pageID >> 32)
	pageNo := uint32(pageID)

	// Check if page exists
	_, err := a.sm.GetPage(spaceID, pageNo)
	if err == nil {
		return nil // Exists
	}

	// Does not exist. Try to allocate.
	// Loop is risky if implementation is not sequential, but it's the best effort.
	// We try to allocate until we reach the desired pageNo.
	// Limit attempts to avoid infinite loop.
	maxAttempts := 1000
	for i := 0; i < maxAttempts; i++ {
		newPage, err := a.sm.AllocPage(spaceID, basic.PageType(common.FIL_PAGE_TYPE_ALLOCATED))
		if err != nil {
			return err
		}

		allocatedPageNo := newPage.GetPageNo()
		if allocatedPageNo == pageNo {
			return nil // Success
		}

		if allocatedPageNo > pageNo {
			return fmt.Errorf("allocated page number %d exceeded target %d", allocatedPageNo, pageNo)
		}
		// allocatedPageNo < pageNo, continue allocating
	}

	return fmt.Errorf("failed to allocate page %d after %d attempts", pageNo, maxAttempts)
}

func (a *RecoveryStorageAdapter) DeletePage(pageID uint64) error {
	spaceID := uint32(pageID >> 32)
	pageNo := uint32(pageID)
	return a.sm.FreePage(spaceID, pageNo)
}
