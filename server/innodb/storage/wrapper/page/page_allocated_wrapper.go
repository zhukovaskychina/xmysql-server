package page

import (
	"bytes"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/storage/store/pages"
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

func (a *Allocated) GetFileHeader() *pages.FileHeader {
	return &a.FileHeader
}

func (a *Allocated) GetFileTrailer() *pages.FileTrailer {
	return &a.FileTrailer
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
