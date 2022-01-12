package store

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/innodb_store/store/storebytes/pages"
	"github.com/zhukovaskychina/xmysql-server/util"
)

type XDesPageWrapper struct {
	IPageWrapper
	xdes         pages.XDesPage
	xdesEntryMap []*XDESEntryWrapper //用于存储和描述XDES的相关信息，方便操作
}

func NewXDesWrapper(pageNo uint32) *XDesPageWrapper {

	return &XDesPageWrapper{xdes: pages.NewXDesPage(pageNo)}
}

func (x *XDesPageWrapper) GetXDesEntryWrapper(id int32) *XDESEntryWrapper {
	return x.xdesEntryMap[id]
}

func ParseXDesPage(content []byte) *XDesPageWrapper {
	var xDesPage = new(pages.XDesPage)
	xDesPage.FileHeader = pages.NewFileHeader()
	xDesPage.FileTrailer = pages.NewFileTrailer()

	xDesPage.LoadFileHeader(content[0:38])
	xDesPage.LoadFileTrailer(content[16384-8 : 16384])
	xDesPage.FirstEmptySpace = content[38:150]
	xDesPage.XDESEntries = make([]pages.XDESEntry, 256)
	for i := 0; i < 256; i++ {
		xDesPage.XDESEntries[i].XDesId = content[150+40*i : 158+40*i]
		xDesPage.XDESEntries[i].XDesFlstNode = content[158+40*i : 170+40*i]
		xDesPage.XDESEntries[i].XDesState = content[170+40*i : 174+40*i]
		xDesPage.XDESEntries[i].XDesBitMap = content[174+40*i : 190+40*i]
	}
	xDesPage.SecondEmptySpace = content[10390:16376]
	//复盘XDESEntry，一共255个

	return &XDesPageWrapper{xdes: *xDesPage}
}

//查找指定段的某个页面的信息
//true 表明页面为空
//false 表明该页面为满
func (xdes *XDesPageWrapper) GetSpecifiedPageStatus(pageoffset int, segmentId uint64) bool {

	for _, v := range xdes.xdes.XDESEntries {

		if util.ReadUB8Byte2Long(v.XDesId) == segmentId {
			return v.GetPageInfo(pageoffset)
		}
	}
	return true
}
