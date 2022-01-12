package store

import (
	"bytes"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/innodb_store/store/storebytes/pages"
)

type Allocated struct {
	FileHeader pages.FileHeader

	body []byte //16384-38-8

	FileTrailer pages.FileTrailer
}

func (a *Allocated) ToByte() []byte {
	var buffer bytes.Buffer
	buffer.Write(a.FileHeader.GetSerialBytes())
	buffer.Write(a.body)
	buffer.Write(a.FileTrailer.FileTrailer)
	return buffer.Bytes()
}

func (a *Allocated) GetFileHeader() *pages.FileHeader {
	panic("implement me")
}

func (a *Allocated) GetFileTrailer() *pages.FileTrailer {
	panic("implement me")
}

//用于实现
func NewAllocatedPage(pageNumber uint32) IPageWrapper {
	var allocated = new(Allocated)
	allocated.body = make([]byte, 16384-38-8)
	allocated.FileHeader = pages.NewFileHeader()
	allocated.FileHeader.WritePageFileType(common.FILE_PAGE_TYPE_ALLOCATED)
	allocated.FileHeader.WritePageOffset(pageNumber)
	allocated.FileTrailer = pages.NewFileTrailer()
	return allocated
}
func NewAllocatedPageByBytes(spaceId uint32, pageNumber uint32) IPageWrapper {
	var allocated = new(Allocated)
	allocated.body = make([]byte, 16384-38-8)
	allocated.FileHeader = pages.NewFileHeader()
	allocated.FileHeader.WritePageFileType(common.FILE_PAGE_TYPE_ALLOCATED)
	allocated.FileHeader.WritePageOffset(pageNumber)
	allocated.FileHeader.WritePageArch(spaceId)
	allocated.FileTrailer = pages.NewFileTrailer()
	return allocated
}
