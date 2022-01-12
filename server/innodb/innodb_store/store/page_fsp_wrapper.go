package store

import (
	"bytes"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/innodb_store/store/storebytes/pages"
)
import "xmysql-server/util"

type Fsp struct {
	//	IPageWrapper
	fspHrdBinaryPage *pages.FspHrdBinaryPage
	xdesEntryMap     []*XDESEntryWrapper //用于存储和描述XDES的相关信息，方便操作
}

func (fsp *Fsp) GetFileHeader() *pages.FileHeader {
	return &fsp.fspHrdBinaryPage.FileHeader
}

func (fsp *Fsp) GetFileTrailer() *pages.FileTrailer {
	return &fsp.fspHrdBinaryPage.FileTrailer
}

func (fsp *Fsp) ToByte() []byte {
	return fsp.fspHrdBinaryPage.GetSerializeBytes()
}

func (fsp *Fsp) GetXDesEntryWrapper(id int32) *XDESEntryWrapper {
	return fsp.xdesEntryMap[id]
}

//TODO 需要将FSPwrapper 和Fsp之间做更好的衔接
func NewFsp(fspHrdBinaryPage *pages.FspHrdBinaryPage) IPageWrapper {
	var fsp = new(Fsp)
	fsp.fspHrdBinaryPage = fspHrdBinaryPage
	fsp.xdesEntryMap = make([]*XDESEntryWrapper, 256)
	for i := 0; i < 256; i++ {

		xdesId := util.ReadUB8Byte2Long(fspHrdBinaryPage.XDESEntrys[i].XDesId)
		currentXDesEntry := NewXDesEntryWrapper(xdesId, 0, 0, 0, 0, fsp)
		currentXDesEntry.DescPage(0, false)
		for m := 0; m < 16; m++ {
			currentXDesEntry.DescPage(uint8(m), false)
		}
		for j := 16; j < 64; j++ {
			currentXDesEntry.DescPage(uint8(j), true)
		}
		fsp.xdesEntryMap[i] = NewXDesEntryWrapper(xdesId, 0, 0, 0, 0, fsp)
	}
	return fsp
}

func NewFspInitialize(spaceId uint32) IPageWrapper {
	var fsp = new(Fsp)
	fspHrdBinaryPage := pages.NewFspHrdPage(spaceId)
	fsp.fspHrdBinaryPage = fspHrdBinaryPage
	fsp.xdesEntryMap = make([]*XDESEntryWrapper, 256)
	for i := 0; i < 256; i++ {

		xdesId := util.ReadUB8Byte2Long(fspHrdBinaryPage.XDESEntrys[i].XDesId)
		currentXDesEntry := NewXDesEntryWrapper(xdesId, 0, 0, 0, 0, fsp)

		if i == 0 {
			currentXDesEntry.DescPage(0, false)
			for m := 0; m < 18; m++ {
				currentXDesEntry.DescPage(uint8(m), false)
			}
			for j := 18; j < 64; j++ {
				currentXDesEntry.DescPage(uint8(j), true)
			}
			currentXDesEntry.SetDesState(common.XDES_FREE_FRAG)
		} else {
			currentXDesEntry.SetDesState(common.XDES_FREE)
		}

		fsp.xdesEntryMap[i] = currentXDesEntry
	}
	return fsp
}

func NewFspByPage(fspHrdBinaryPage *pages.FspHrdBinaryPage) *Fsp {
	return &Fsp{fspHrdBinaryPage: fspHrdBinaryPage}
}

func NewFspByLoadBytes(content []byte) IPageWrapper {

	var fspBinary = new(pages.FspHrdBinaryPage)
	fspBinary.FileHeader = pages.NewFileHeader()
	fspBinary.FileTrailer = pages.NewFileTrailer()

	fspBinary.LoadFileHeader(content[0:38])
	fspBinary.LoadFileTrailer(content[16384-8 : 16384])
	fspBinary.EmptySpace = content[16384-8-5986 : 16384-8]
	//初始化
	fspBinary.FileSpaceHeader = &pages.FileSpaceHeader{
		SpaceId:                 content[38:42],
		NotUsed:                 content[42:46],
		Size:                    content[46:50],
		FreeLimit:               content[50:54],
		SpaceFlags:              content[54:58],
		FragNUsed:               content[58:62],
		BaseNodeForFreeList:     content[62:78],
		BaseNodeForFragFreeList: content[78:94],
		BaseNodeForFullFragList: content[94:110],
		NextUnusedSegmentId:     content[110:118],
		SegFullINodesList:       content[118:134],
		SegFreeINodesList:       content[134:150],
	}
	fspBinary.XDESEntrys = make([]pages.XDESEntry, 256)
	//复盘XDESEntry，一共256个
	for k, _ := range fspBinary.XDESEntrys {
		fspBinary.XDESEntrys[k] = pages.XDESEntry{
			XDesId:       content[150+k*20 : 158+k*20],
			XDesFlstNode: content[158+k*20 : 170+k*20],
			XDesState:    content[170+k*20 : 174+k*20],
			XDesBitMap:   content[174+k*20 : 190+k*20],
		}
	}

	return NewFsp(fspBinary)
}

//页面最小
func (fsp *Fsp) SetFreeLimit(freePageNo uint32) {
	fsp.fspHrdBinaryPage.FileSpaceHeader.FreeLimit = util.ConvertUInt4Bytes(freePageNo)
}

func (fsp *Fsp) SetXDesEntryInfo(extentNumber uint32, wrapper *XDESEntryWrapper) {
	fsp.xdesEntryMap[extentNumber] = wrapper
	if extentNumber > 256 {
		panic("区间号错误")
	}
	fsp.fspHrdBinaryPage.XDESEntrys[extentNumber] = wrapper.ToXDesEntry()

}

func (fsp *Fsp) GetNextSegmentId() []byte {
	var buff = fsp.fspHrdBinaryPage.FileSpaceHeader.NextUnusedSegmentId
	fsp.ChangeNextSegmentId()
	return buff
}

func (fsp *Fsp) ChangeNextSegmentId() {
	//fsp.fspHrdBinaryPage.FileSpaceHeader.NextUnusedSegmentId
	segmentId := util.ReadUB8Byte2Long(fsp.fspHrdBinaryPage.FileSpaceHeader.NextUnusedSegmentId)
	segmentId = segmentId + 1
	fsp.fspHrdBinaryPage.FileSpaceHeader.NextUnusedSegmentId = util.ConvertULong8Bytes(segmentId)
}

//计算表空间，以Page页面数量计算
func (fsp *Fsp) GetFspSize() uint32 {
	return util.ReadUB4Byte2UInt32(fsp.fspHrdBinaryPage.FileSpaceHeader.Size)
}

func (fsp *Fsp) SetFspSize(size uint32) {
	fsp.fspHrdBinaryPage.FileSpaceHeader.Size = util.ConvertUInt4Bytes(size)
}

//在空闲的Extent上最小的尚未被初始化的Page的PageNumber
func (fsp *Fsp) GetFspFreeLimit() uint32 {
	return util.ReadUB4Byte2UInt32(fsp.fspHrdBinaryPage.FileSpaceHeader.FreeLimit)
}

//获取FreeFrag链表中已经使用的数量
func (fsp *Fsp) GetFragNUsed() uint32 {

	return 0
}

//当一个Extent中所有page都未被使用时，放到该链表上，可以用于随后的分配
func (fsp *Fsp) GetFspFreeExtentListInfo() CommonNodeInfo {
	segFullInodeList := fsp.fspHrdBinaryPage.FileSpaceHeader.BaseNodeForFreeList
	return CommonNodeInfo{
		NodeInfoLength:     util.ReadUB4Byte2UInt32(segFullInodeList[0:4]),
		PreNodePageNumber:  util.ReadUB4Byte2UInt32(segFullInodeList[4:8]),
		PreNodeOffset:      util.ReadUB2Byte2Int(segFullInodeList[8:10]),
		NextNodePageNumber: util.ReadUB4Byte2UInt32(segFullInodeList[10:14]),
		NextNodeOffset:     util.ReadUB2Byte2Int(segFullInodeList[14:16]),
	}
}

//给fsp设置free链表信息
func (fsp *Fsp) SetFspFreeExtentListInfo(info *CommonNodeInfo) {
	fsp.fspHrdBinaryPage.FileSpaceHeader.BaseNodeForFreeList = info.ToBytes()
}

//Extent中所有的page都被使用掉时，会放到该链表上，当有Page从该Extent释放时，则移回FREE_FRAG链表
func (fsp *Fsp) GetFspFullFragListInfo() *CommonNodeInfo {
	segFullInodeList := fsp.fspHrdBinaryPage.FileSpaceHeader.BaseNodeForFullFragList
	return &CommonNodeInfo{
		NodeInfoLength:     util.ReadUB4Byte2UInt32(segFullInodeList[0:4]),
		PreNodePageNumber:  util.ReadUB4Byte2UInt32(segFullInodeList[4:8]),
		PreNodeOffset:      util.ReadUB2Byte2Int(segFullInodeList[8:10]),
		NextNodePageNumber: util.ReadUB4Byte2UInt32(segFullInodeList[10:14]),
		NextNodeOffset:     util.ReadUB2Byte2Int(segFullInodeList[14:16]),
	}
}

//FREE_FRAG链表的Base Node，通常这样的Extent中的Page可能归属于不同的segment，用于segment frag array page的分配（见下文）
func (fsp *Fsp) GetFspFreeFragExtentListInfo() CommonNodeInfo {
	segFullInodeList := fsp.fspHrdBinaryPage.FileSpaceHeader.BaseNodeForFragFreeList
	return CommonNodeInfo{
		NodeInfoLength:     util.ReadUB4Byte2UInt32(segFullInodeList[0:4]),
		PreNodePageNumber:  util.ReadUB4Byte2UInt32(segFullInodeList[4:8]),
		PreNodeOffset:      util.ReadUB2Byte2Int(segFullInodeList[8:10]),
		NextNodePageNumber: util.ReadUB4Byte2UInt32(segFullInodeList[10:14]),
		NextNodeOffset:     util.ReadUB2Byte2Int(segFullInodeList[14:16]),
	}
}

func (fsp *Fsp) SetFspFreeFragExtentListInfo(info *CommonNodeInfo) {
	fsp.fspHrdBinaryPage.FileSpaceHeader.BaseNodeForFragFreeList = info.ToBytes()
}

func (fsp *Fsp) SetFullFragExtentListInfo(info *CommonNodeInfo) {
	fsp.fspHrdBinaryPage.FileSpaceHeader.BaseNodeForFullFragList = info.ToBytes()
}

//segInodeFull链表的基节点
//已被完全用满的Inode Page链表
func (fsp *Fsp) GetFullINodeBaseInfo() CommonNodeInfo {
	segFullInodeList := fsp.fspHrdBinaryPage.FileSpaceHeader.SegFullINodesList
	return CommonNodeInfo{
		NodeInfoLength:     util.ReadUB4Byte2UInt32(segFullInodeList[0:4]),
		PreNodePageNumber:  util.ReadUB4Byte2UInt32(segFullInodeList[4:8]),
		PreNodeOffset:      util.ReadUB2Byte2Int(segFullInodeList[8:10]),
		NextNodePageNumber: util.ReadUB4Byte2UInt32(segFullInodeList[10:14]),
		NextNodeOffset:     util.ReadUB2Byte2Int(segFullInodeList[14:16]),
	}
}

//至少存在一个空闲Inode Entry的Inode Page被放到该链表上
func (fsp *Fsp) GetFreeSegINodeBaseInfo() CommonNodeInfo {
	segFullInodeList := fsp.fspHrdBinaryPage.FileSpaceHeader.SegFullINodesList
	return CommonNodeInfo{
		NodeInfoLength:     util.ReadUB4Byte2UInt32(segFullInodeList[0:4]),
		PreNodePageNumber:  util.ReadUB4Byte2UInt32(segFullInodeList[4:8]),
		PreNodeOffset:      util.ReadUB2Byte2Int(segFullInodeList[8:10]),
		NextNodePageNumber: util.ReadUB4Byte2UInt32(segFullInodeList[10:14]),
		NextNodeOffset:     util.ReadUB2Byte2Int(segFullInodeList[14:16]),
	}
}

func (fsp *Fsp) GetSerializeBytes() []byte {
	for i := 0; i < 256; i++ {
		fsp.fspHrdBinaryPage.XDESEntrys[i] = fsp.xdesEntryMap[i].ToXDesEntry()
	}
	return fsp.fspHrdBinaryPage.GetSerializeBytes()
}

//type DESListNode struct {
//	PreNodePageNumber  []byte //4个字节	表示指向前一个INode页面号
//	PreNodeOffset      []byte //2个字节 65536-1
//	NextNodePageNumber []byte //4个字节  表示指向后一个INode页面号
//	NextNodeOffSet     []byte //2个字节	65536-1
//}
//
////XDES entry,每个Entry 占用40个字节
////一个XDES-ENtry 对应一个extent
////
//type XDESEntry struct {
//	XDesId       []byte //8 个 byte 每个段都有唯一的编号，分配段的号码
//	XDesFlstNode []byte //12 个长度 XDesEntry链表
//	XDesState    []byte //4个字节长度，根据该Extent状态信息，包括：XDES_FREE,FREE_FRAG,FULL_FRAG,FSEG
//	XDesBitMap   []byte //16个字节，一共128个bit，用两个bit表示Extent中的一个page，一个bit表示该page是否空闲的（XDES_FREE_BIT）,另一个保留位
//}

type XDESEntryWrapper struct {
	wrapper            IPageWrapper
	XDesId             uint64         //段ID
	PreNodePageNumber  uint32         //前一个Extent链表
	PreNodeOffset      uint16         //偏移量
	NextNodePageNumber uint32         //下一个Extent
	NextNodeoffset     uint16         //偏移量
	XdesDescPageMap    map[uint8]bool //2个bit 表示一个Page，2个表示一个page，1个表示是否空闲，1个空 一共64个页面

	XDesState common.XDES_STATE
}

func NewXDesEntryWrapper(XdesId uint64, PreNodePageNo uint32, PreNodeOffset uint16, NextNodePageNo uint32, NextNodeOffset uint16, wrapper IPageWrapper) *XDESEntryWrapper {
	var xdesEntryWrapper = new(XDESEntryWrapper)
	xdesEntryWrapper.XDesId = XdesId
	xdesEntryWrapper.PreNodePageNumber = PreNodePageNo
	xdesEntryWrapper.PreNodeOffset = PreNodeOffset
	xdesEntryWrapper.NextNodePageNumber = NextNodePageNo
	xdesEntryWrapper.NextNodeoffset = NextNodeOffset
	xdesEntryWrapper.XdesDescPageMap = make(map[uint8]bool)
	xdesEntryWrapper.XDesState = common.XDES_FREE
	for i := 0; i < 64; i++ {
		xdesEntryWrapper.XdesDescPageMap[byte(i)] = true
	}
	xdesEntryWrapper.wrapper = wrapper
	return xdesEntryWrapper
}

func (xdes *XDESEntryWrapper) SetDesState(state common.XDES_STATE) {
	xdes.XDesState = state
}

func (xdes *XDESEntryWrapper) DescPage(pageOffset uint8, isFree bool) {
	xdes.XdesDescPageMap[pageOffset] = isFree
}

func (xdes *XDESEntryWrapper) GetNearsFreePage() uint8 {
	for i := 0; i < 64; i++ {
		v := xdes.XdesDescPageMap[uint8(i)]
		if v {
			return uint8(i)
		}
	}

	return 0
}

func (xdes *XDESEntryWrapper) ToBytes() []byte {

	var buff = make([]byte, 0)
	buff = append(buff, util.ConvertULong8Bytes(xdes.XDesId)...)
	buff = append(buff, util.ConvertUInt4Bytes(xdes.PreNodePageNumber)...)
	buff = append(buff, util.ConvertUInt2Bytes(xdes.PreNodeOffset)...)
	buff = append(buff, util.ConvertUInt4Bytes(xdes.NextNodePageNumber)...)
	buff = append(buff, util.ConvertUInt2Bytes(xdes.NextNodeoffset)...)
	buff = append(buff, util.ConvertUInt4Bytes(uint32(xdes.XDesState))...)
	var buffer bytes.Buffer
	for k, v := range xdes.XdesDescPageMap {
		bitKey := util.ConvertByte2Bits(k)
		buffer.WriteString(util.Substr(bitKey, 4, 8))
		if v {
			buffer.WriteString("1")
		} else {
			buffer.WriteString("0")
		}
		buffer.WriteString("0")
	}
	buff = append(buff, util.ConvertBits2Bytes(buffer.String())...)

	return buff
}

func (xdes *XDESEntryWrapper) ToXDesEntry() pages.XDESEntry {
	content := xdes.ToBytes()
	return pages.XDESEntry{
		XDesId:       content[0:8],
		XDesFlstNode: content[8:20],
		XDesState:    content[20:24],
		XDesBitMap:   content[24:40],
	}
}
