package page

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	pages2 "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper"
)

type IBuf struct {
	wrapper.IPageWrapper
	iBufPage pages2.IBufBitMapPage
	spaceID  uint32
	pageNo   uint32
	storage  basic.StorageProvider
}

func (b *IBuf) GetSerializeBytes() []byte {
	return b.iBufPage.GetSerializeBytes()
}

func NewIBuf(spaceId uint32) *IBuf {
	return NewIBufWithStorage(0, spaceId, nil)
}

// NewIBufWithStorage creates the historical insert-buffer bitmap wrapper with
// an optional durable provider. The old constructor remains an in-memory
// compatible convenience entry point.
func NewIBufWithStorage(spaceID, pageNo uint32, storage basic.StorageProvider) *IBuf {
	ibuf := pages2.NewIBufBitMapPage(pageNo)
	ibuf.FileHeader.WritePageArch(spaceID)
	return &IBuf{
		iBufPage: ibuf,
		spaceID:  spaceID,
		pageNo:   pageNo,
		storage:  storage,
	}
}

// 用于复盘从文件中加载出来的字节流
func NewIBufByLoadBytes(content []byte) *IBuf {

	var iBufBitMapPage = new(pages2.IBufBitMapPage)
	iBufBitMapPage.FileHeader = pages2.NewFileHeader()
	iBufBitMapPage.FileTrailer = pages2.NewFileTrailer()

	iBufBitMapPage.LoadFileHeader(content[0:38])
	iBufBitMapPage.ChangeBufferBitMap = content[38 : 38+9192]
	iBufBitMapPage.EmptySpace = content[16384-8-8146 : 16384-8]
	iBufBitMapPage.LoadFileTrailer(content[16384-8 : 16384])

	return &IBuf{
		iBufPage: *iBufBitMapPage,
		spaceID:  iBufBitMapPage.FileHeader.GetFilePageArch(),
		pageNo:   iBufBitMapPage.FileHeader.GetCurrentPageOffset(),
	}
}

// Read loads the bitmap page from the configured provider.
func (b *IBuf) Read() error {
	if b.storage == nil {
		return fmt.Errorf("ibuf page storage provider is unavailable")
	}
	data, err := b.storage.ReadPage(b.spaceID, b.pageNo)
	if err != nil {
		return err
	}
	if len(data) < common.PageSize {
		return fmt.Errorf("ibuf page has %d bytes, want at least %d", len(data), common.PageSize)
	}
	loaded := NewIBufByLoadBytes(data[:common.PageSize])
	loaded.storage = b.storage
	*b = *loaded
	return nil
}

// Write persists the bitmap page through the configured provider.
func (b *IBuf) Write() error {
	if b.storage == nil {
		return fmt.Errorf("ibuf page storage provider is unavailable")
	}
	data := b.GetSerializeBytes()
	if len(data) < common.PageSize {
		return fmt.Errorf("ibuf page serializes to %d bytes, want at least %d", len(data), common.PageSize)
	}
	return b.storage.WritePage(b.spaceID, b.pageNo, data[:common.PageSize])
}
