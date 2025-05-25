package pages

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/util"
)

type DESListNode struct {
	PreNodePageNumber  []byte //4个字节	表示指向前一个INode页面号
	PreNodeOffset      []byte //2个字节 65536-1
	NextNodePageNumber []byte //4个字节  表示指向后一个INode页面号
	NextNodeOffSet     []byte //2个字节	65536-1
}

// XDES entry,每个Entry 占用40个字节
// 一个XDES-ENtry 对应一个extent
// XdesId 与xdesstate之间的关系 如果xdesid有值，则xdesstate为fseg
// xdesflstNode 则是将相同状态的extent做了链接
// 根据顺序排序
type XDESEntry struct {
	XDesId       []byte //8 个 byte 每个段都有唯一的编号，分配段的号码
	XDesFlstNode []byte //12 个长度 XDesEntry链表 维持Extent链表的双向指针节点
	XDesState    []byte //4个字节长度，根据该Extent状态信息，包括：XDES_FREE,FREE_FRAG,FULL_FRAG,FSEG
	XDesBitMap   []byte //16个字节，一共128个bit，用两个bit表示Extent中的一个page，一个bit表示该page是否空闲的（XDES_FREE_BIT）,另一个保留位
}

// extentoffset 区的偏移量
func NewXdesEntry() XDESEntry {
	var xdesEntry = new(XDESEntry)
	xdesEntry.XDesId = util.AppendByte(8)
	xdesEntry.XDesFlstNode = util.AppendByte(12)
	xdesEntry.XDesState = util.ConvertUInt4Bytes(uint32(common.XDES_FREE))
	xdesEntry.XDesBitMap = util.AppendByte(16)
	return *xdesEntry
}

func ParseXDesEntry(content []byte) XDESEntry {
	var xdesEntry = new(XDESEntry)
	xdesEntry.XDesId = content[0:8]
	xdesEntry.XDesFlstNode = content[8:20]
	xdesEntry.XDesState = content[20:24]
	xdesEntry.XDesBitMap = content[24:40]
	return *xdesEntry
}

func (x *XDESEntry) SetSegementId(segId uint64) {
	x.XDesId = util.ConvertULong8Bytes(segId)
}

func (x *XDESEntry) SetExtentState(xdesState common.XDES_STATE) {
	x.XDesState = util.ConvertUInt4Bytes(uint32(xdesState))
}

func (x *XDESEntry) SetDesFlstNode(prePageNodeNumber uint32, preOffset uint16, nextPageNodeNo uint32, nextPageOffset uint16) {
	x.XDesFlstNode = make([]byte, 0)
	x.XDesFlstNode = append(x.XDesFlstNode, util.ConvertUInt4Bytes(prePageNodeNumber)...)
	x.XDesFlstNode = append(x.XDesFlstNode, util.ConvertUInt2Bytes(preOffset)...)
	x.XDesFlstNode = append(x.XDesFlstNode, util.ConvertUInt4Bytes(nextPageNodeNo)...)
	x.XDesFlstNode = append(x.XDesFlstNode, util.ConvertUInt2Bytes(nextPageOffset)...)
}

// 获取第几个页面
func (x *XDESEntry) GetPageInfo(pageOffset int) bool {

	index := pageOffset >> 4

	s := util.ConvertByte2BitsString(x.XDesBitMap[index])[2*(pageOffset-index*4)]

	if s == "0" {
		return true
	}
	return false
}

func (x *XDESEntry) GetSerializeByte() []byte {

	var buff = make([]byte, 0)
	buff = append(buff, x.XDesId...)
	buff = append(buff, x.XDesFlstNode...)
	buff = append(buff, x.XDesState...)
	buff = append(buff, x.XDesBitMap...)
	return buff
}

////////////////////
//	 FSPHDR->IBUF_BITMAP_PAGE->INODE_PAGE->MORE PAGES->XDES_ENTRY->IBUF_BITMAP_PAGE->MORE_PAES->XDES_ENTRY->IBUF_BITMAP->More Pages
//////////////////

type XDesPage struct {
	AbstractPage
	FirstEmptySpace  []byte      //38-150
	XDESEntries      []XDESEntry //150-10390
	SecondEmptySpace []byte      //10390-16376
}

func NewXDesPage(pageNumber uint32) XDesPage {
	var fileHeader = NewFileHeader()
	fileHeader.WritePageLSN(0)
	fileHeader.WritePageOffset(pageNumber)
	fileHeader.WritePageFileType(int16(common.FIL_PAGE_TYPE_XDES))
	fileHeader.WritePageNext(0)
	fileHeader.WritePagePrev(0)
	fileHeader.WritePageFileFlushLSN(0)

	var fileTrailer = NewFileTrailer()

	xdesEntries := make([]XDESEntry, 256)
	for i := 0; i < 256; i++ {
		xdesEntries[i] = NewXdesEntry()
	}

	return XDesPage{
		AbstractPage: AbstractPage{
			FileHeader:  fileHeader,
			FileTrailer: fileTrailer,
		},
		FirstEmptySpace:  make([]byte, 112),
		XDESEntries:      xdesEntries,
		SecondEmptySpace: make([]byte, 5986),
	}
}

func (xp *XDesPage) SerializeBytes() []byte {
	var buff = make([]byte, 0)
	buff = append(buff, xp.FileHeader.GetSerialBytes()...)
	buff = append(buff, xp.FirstEmptySpace...)

	for _, entry := range xp.XDESEntries {
		buff = append(buff, entry.GetSerializeByte()...)
	}

	buff = append(buff, xp.SecondEmptySpace...)
	buff = append(buff, xp.FileTrailer.FileTrailer[:]...)
	return buff
}

func (xp *XDesPage) GetSerializeBytes() []byte {
	return xp.SerializeBytes()
}
