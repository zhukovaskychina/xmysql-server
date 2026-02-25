package page

import (
	"bytes"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
)

type Allocated struct {
	FileHeader pages.FileHeader

	body []byte //16384-38-8

	FileTrailer pages.FileTrailer
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
	// TODO: 实现从字节数据解析
	return nil
}

func (a *Allocated) ToBytes() ([]byte, error) {
	return a.ToByte(), nil
}

func (a *Allocated) ToByte() []byte {
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
	a.FileHeader.WritePageLSN(int64(lsn))
}

func (a *Allocated) GetState() basic.PageState {
	return basic.PageStateClean
}

func (a *Allocated) SetState(state basic.PageState) {
	// TODO: 添加状态字段
}

func (a *Allocated) IsDirty() bool {
	return false
}

func (a *Allocated) MarkDirty() {
	// TODO: 添加脏页标记
}

func (a *Allocated) Pin() {
	// TODO: 添加引用计数
}

func (a *Allocated) Unpin() {
	// TODO: 添加引用计数
}

func (a *Allocated) GetPinCount() int32 {
	return 0
}

func (a *Allocated) GetStats() *basic.PageStats {
	return &basic.PageStats{}
}

func (a *Allocated) Read() error {
	return nil
}

func (a *Allocated) Write() error {
	return nil
}

func (a *Allocated) Flush() error {
	return nil
}

// 用于实现
func NewAllocatedPage(pageNumber uint32) IPageWrapper {
	var allocated = new(Allocated)
	allocated.body = make([]byte, 16384-38-8)
	allocated.FileHeader = pages.NewFileHeader()
	allocated.FileHeader.WritePageFileType(int16(common.FIL_PAGE_TYPE_ALLOCATED))
	allocated.FileHeader.WritePageOffset(pageNumber)
	allocated.FileTrailer = pages.NewFileTrailer()
	return allocated
}

func NewAllocatedPageByBytes(spaceId uint32, pageNumber uint32) IPageWrapper {
	var allocated = new(Allocated)
	allocated.body = make([]byte, 16384-38-8)
	allocated.FileHeader = pages.NewFileHeader()
	allocated.FileHeader.WritePageFileType(int16(common.FIL_PAGE_TYPE_ALLOCATED))
	allocated.FileHeader.WritePageOffset(pageNumber)
	allocated.FileHeader.WritePageArch(spaceId)
	allocated.FileTrailer = pages.NewFileTrailer()
	return allocated
}
