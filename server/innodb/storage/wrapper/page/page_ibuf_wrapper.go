package page

import (
	pages2 "xmysql-server/server/innodb/storage/store/pages"
	"xmysql-server/server/innodb/storage/wrapper"
)

type IBuf struct {
	wrapper.IPageWrapper
	iBufPage pages2.IBufBitMapPage
}

func (b *IBuf) GetSerializeBytes() []byte {
	return b.iBufPage.GetSerializeBytes()
}

func NewIBuf(spaceId uint32) *IBuf {
	ibuf := pages2.NewIBufBitMapPage(spaceId)
	var ibufInstance = new(IBuf)
	ibufInstance.iBufPage = ibuf
	return ibufInstance
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

	return &IBuf{iBufPage: *iBufBitMapPage}
}
