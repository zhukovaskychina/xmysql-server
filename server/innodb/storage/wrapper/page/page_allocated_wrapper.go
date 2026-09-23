package page

import (
	"bytes"
	"errors"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
)

type Allocated struct {
	FileHeader pages.FileHeader

	body []byte //16384-38-8

	FileTrailer pages.FileTrailer

	state    basic.PageState
	dirty    bool
	stats    basic.PageStats
	pinCount int32
	mu       sync.RWMutex
	storage  basic.StorageProvider
}

// 实现IPageWrapper接口
func (a *Allocated) GetPageID() uint32 {
	return a.FileHeader.GetCurrentPageOffset()
}

func (a *Allocated) GetSpaceID() uint32 {
	return a.FileHeader.GetFilePageArch()
}

func (a *Allocated) GetPageType() common.PageType {
	return common.PageType(a.FileHeader.GetPageType())
}

func (a *Allocated) ParseFromBytes(data []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(data) < common.PageSize {
		return errors.New("invalid allocated page size")
	}

	if err := a.FileHeader.ParseFileHeader(data[:pages.FileHeaderSize]); err != nil {
		return err
	}

	bodyLen := common.PageSize - pages.FileHeaderSize - pages.FileTrailerSize
	a.body = make([]byte, bodyLen)
	copy(a.body, data[pages.FileHeaderSize:common.PageSize-pages.FileTrailerSize])
	copy(a.FileTrailer.FileTrailer[:], data[common.PageSize-pages.FileTrailerSize:])

	return nil
}

func (a *Allocated) ToBytes() ([]byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.toByteLocked(), nil
}

func (a *Allocated) ToByte() []byte {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.toByteLocked()
}

func (a *Allocated) toByteLocked() []byte {
	var buffer bytes.Buffer
	buffer.Write(a.FileHeader.GetSerialBytes())
	buffer.Write(a.body)
	buffer.Write(a.FileTrailer.FileTrailer[:])
	return buffer.Bytes()
}

func (a *Allocated) GetFileHeader() []byte {
	return a.FileHeader.GetSerialBytes()
}

func (a *Allocated) GetFileTrailer() []byte {
	return a.FileTrailer.FileTrailer[:]
}

func (a *Allocated) GetFileHeaderStruct() *pages.FileHeader {
	return &a.FileHeader
}

func (a *Allocated) GetFileTrailerStruct() *pages.FileTrailer {
	return &a.FileTrailer
}

// 实现 types.IPageWrapper 接口的其他方法

func (a *Allocated) GetPageNo() uint32 {
	return a.GetPageID()
}

func (a *Allocated) GetLSN() uint64 {
	return uint64(a.FileHeader.GetPageLSN())
}

func (a *Allocated) SetLSN(lsn uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.FileHeader.WritePageLSN(int64(lsn))
}

func (a *Allocated) GetState() basic.PageState {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.state
}

func (a *Allocated) SetState(state basic.PageState) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.state = state
}

func (a *Allocated) IsDirty() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.dirty
}

func (a *Allocated) MarkDirty() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.dirty = true
	a.stats.DirtyCount++
	a.state = basic.PageStateDirty
}

func (a *Allocated) Pin() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pinCount++
}

func (a *Allocated) Unpin() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.pinCount > 0 {
		a.pinCount--
	}
}

func (a *Allocated) GetPinCount() int32 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.pinCount
}

func (a *Allocated) GetStats() *basic.PageStats {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return &a.stats
}

// Read refreshes the in-memory allocation state. Allocated pages do not own a
// disk provider; persistence belongs to the page allocator/storage manager.
func (a *Allocated) Read() error {
	if a.storage != nil {
		data, err := a.storage.ReadPage(a.GetSpaceID(), a.GetPageID())
		if err != nil {
			return err
		}
		if err := a.ParseFromBytes(data); err != nil {
			return err
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		now := uint64(time.Now().UnixNano())
		a.stats.ReadCount++
		a.stats.AccessTime = now
		a.stats.LastAccessAt = now
		a.stats.LastAccessed = now
		a.state = basic.PageStateLoaded
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.body) != common.PageSize-pages.FileHeaderSize-pages.FileTrailerSize {
		return errors.New("invalid allocated page body size")
	}
	now := uint64(time.Now().UnixNano())
	a.stats.ReadCount++
	a.stats.AccessTime = now
	a.stats.LastAccessAt = now
	a.stats.LastAccessed = now
	a.state = basic.PageStateLoaded
	return nil
}

// Write commits the current in-memory representation and clears its dirty
// marker. The caller must use the owning storage manager to persist ToBytes().
func (a *Allocated) Write() error {
	if a.storage != nil {
		data, err := a.ToBytes()
		if err != nil {
			return err
		}
		if len(data) < common.PageSize {
			return errors.New("invalid allocated page size")
		}
		if err := a.storage.WritePage(a.GetSpaceID(), a.GetPageID(), data[:common.PageSize]); err != nil {
			return err
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		now := uint64(time.Now().UnixNano())
		a.stats.WriteCount++
		a.stats.AccessTime = now
		a.stats.LastAccessAt = now
		a.stats.LastModified = now
		a.dirty = false
		a.state = basic.PageStateFlushed
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.body) != common.PageSize-pages.FileHeaderSize-pages.FileTrailerSize {
		return errors.New("invalid allocated page body size")
	}
	now := uint64(time.Now().UnixNano())
	a.stats.WriteCount++
	a.stats.AccessTime = now
	a.stats.LastAccessAt = now
	a.stats.LastModified = now
	a.dirty = false
	a.state = basic.PageStateFlushed
	return nil
}

func (a *Allocated) Flush() error {
	return a.Write()
}

// 用于实现
func NewAllocatedPage(pageNumber uint32) IPageWrapper {
	return NewAllocatedPageWithStorage(0, pageNumber, nil)
}

// NewAllocatedPageWithStorage creates an allocated page with an optional
// provider. Without a provider it retains the historical in-memory behavior.
func NewAllocatedPageWithStorage(spaceId uint32, pageNumber uint32, storage basic.StorageProvider) IPageWrapper {
	var allocated = new(Allocated)
	allocated.body = make([]byte, 16384-38-8)
	allocated.FileHeader = pages.NewFileHeader()
	allocated.FileHeader.WritePageFileType(int16(common.FIL_PAGE_TYPE_ALLOCATED))
	allocated.FileHeader.WritePageOffset(pageNumber)
	allocated.FileHeader.WritePageArch(spaceId)
	allocated.FileTrailer = pages.NewFileTrailer()
	allocated.state = basic.PageStateClean
	allocated.storage = storage
	return allocated
}

func NewAllocatedPageByBytes(spaceId uint32, pageNumber uint32) IPageWrapper {
	return NewAllocatedPageWithStorage(spaceId, pageNumber, nil)
}
