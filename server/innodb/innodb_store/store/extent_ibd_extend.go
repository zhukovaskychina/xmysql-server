package store

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/tuple"
)

/////////////////////////////////////////////////////////////
///
/// 每个Extent的大小均为1MB 256*16384 Byte
///
//////////////////////////////////////////////////////////////

type Extent interface {

	//获取区的类型，返回first和other
	ExtentType() string

	//释放页面
	FreePage(pageNumber uint32)

	LoadIPageWrapper(wrapper []uint32)

	//分配新的页面
	AllocateNewPage(pageType int, tuple tuple.TableRowTuple) IPageWrapper

	GetExtentId() uint32

	//设置ExtentType
	//初始化状态
	SetExtentState(xdesState common.XDES_STATE)
}

//每个区有64个page
//每次生成一个区就有64个页面
//第一个区有Fsp,BitMap,Inode，管理包括他自身的25
//Inode会有多个
//第一个区，分为系统表空间和一般表空间
//系统表空间有多个
type PrimaryExtent struct {
	IsInit           bool
	fspHrdBinaryPage *Fsp         //FileSpace Header,用于存储表空间的元数据信息，
	IBufBitMapPage   IPageWrapper //Insert Buffer Bookkeeping
	INodePage        *INode       //Index Node Information
	ExtentNumber     uint32       //区的首个页面号码，会管理后面64个页面

	XDESEntryWrapper *XDESEntryWrapper
	//剩下的Page
	Pages []uint32

	extentState common.XDES_STATE

	ExtentId int64 //	extentId

	spaceId    uint32
	tableSpace TableSpace
	pool       *buffer_pool.BufferPool
}

func NewPrimaryExtentWithPages(spaceId uint32, extentPageNumber uint32, entryWrapper *XDESEntryWrapper, pool *buffer_pool.BufferPool) Extent {
	var primaryExtent = new(PrimaryExtent)
	primaryExtent.ExtentNumber = extentPageNumber
	primaryExtent.XDESEntryWrapper = entryWrapper
	primaryExtent.spaceId = spaceId
	primaryExtent.pool = pool
	primaryExtent.extentState = common.XDES_FREE_FRAG
	primaryExtent.Pages = make([]uint32, 3)
	primaryExtent.Pages = append(primaryExtent.Pages, 0, 1, 2)
	pool.GetPageBlock(spaceId, 0)
	pool.GetPageBlock(spaceId, 1)
	pool.GetPageBlock(spaceId, 2)
	return primaryExtent
}

func NewPrimaryExtentWithPagesAtInit(spaceId uint32, extentPageNumber uint32, entryWrapper *XDESEntryWrapper, ts TableSpace) Extent {
	var primaryExtent = new(PrimaryExtent)
	primaryExtent.ExtentNumber = extentPageNumber
	primaryExtent.XDESEntryWrapper = entryWrapper
	primaryExtent.spaceId = spaceId
	primaryExtent.tableSpace = ts
	primaryExtent.extentState = common.XDES_FREE_FRAG
	primaryExtent.Pages = make([]uint32, 0)
	//	primaryExtent.Pages = append(primaryExtent.Pages, 0, 1, 2)
	primaryExtent.IsInit = true
	return primaryExtent
}

func (firstIBDExtent *PrimaryExtent) LoadIPageWrapper(wrapper []uint32) {
	firstIBDExtent.Pages = append(firstIBDExtent.Pages, wrapper...)
}

//判断当前区是否满了
//第一个区无所谓段
func (firstIBDExtent *PrimaryExtent) IsFull() bool {
	return firstIBDExtent.fspHrdBinaryPage.xdesEntryMap[0].XDesState == common.XDES_FULL_FRAG
}

func (firstIBDExtent *PrimaryExtent) GetExtentId() uint32 {
	return 0
}

//释放页面，由用过页面变为allocated
func (firstIBDExtent *PrimaryExtent) FreePage(pageNumber uint32) {
	firstIBDExtent.XDESEntryWrapper.XdesDescPageMap[byte(pageNumber)] = true
	buff := NewAllocatedPage(pageNumber).ToByte()
	if !firstIBDExtent.IsInit {
		bufferBlock := buffer_pool.NewBufferBlock(&(buff), firstIBDExtent.spaceId, pageNumber)
		firstIBDExtent.pool.UpdateBlock(firstIBDExtent.spaceId, pageNumber, bufferBlock)
		return
	}
	firstIBDExtent.tableSpace.FlushToDisk(pageNumber, buff)
}

func (firstIBDExtent *PrimaryExtent) ExtentType() string {
	return "PRIMARY"
}

func (firstIBDExtent *PrimaryExtent) AllocateNewPage(pageType int, tuple tuple.TableRowTuple) IPageWrapper {
	var index IPageWrapper
	//计算pageNo
	//
	var offset uint8
	if firstIBDExtent.IsInit {
		firstIBDExtent.fspHrdBinaryPage = firstIBDExtent.tableSpace.GetFirstFsp()
		//根据pageType
		//firstIBDExtent.fspHrdBinaryPage.xdesEntryMap
		offset = firstIBDExtent.fspHrdBinaryPage.xdesEntryMap[0].GetNearsFreePage()

	} else {
		bufferBlock := firstIBDExtent.pool.GetPageBlock(firstIBDExtent.spaceId, 0)
		fspHrdPage := NewFspByLoadBytes(*bufferBlock.Frame).(*Fsp)
		//根据pageType
		//firstIBDExtent.fspHrdBinaryPage.xdesEntryMap
		offset = fspHrdPage.xdesEntryMap[0].GetNearsFreePage()
	}

	//计算pageNo
	//
	pageNo := uint32(offset)
	switch pageType {
	case common.FILE_PAGE_INDEX:
		{
			if firstIBDExtent.IsInit {
				index = NewPageIndexWithTuple(firstIBDExtent.spaceId, uint32(pageNo), tuple)
				firstIBDExtent.tableSpace.FlushToDisk(pageNo, index.ToByte())
			} else {
				bufferBlockIndex := firstIBDExtent.pool.GetPageBlock(firstIBDExtent.spaceId, uint32(pageNo))
				index = NewPageIndexWithTuple(firstIBDExtent.spaceId, uint32(pageNo), tuple)
				copy(*bufferBlockIndex.Frame, index.ToByte())
				firstIBDExtent.pool.UpdateBlock(firstIBDExtent.spaceId, pageNo, bufferBlockIndex)
			}

		}
	}
	firstIBDExtent.XDESEntryWrapper.DescPage(offset, false)
	if firstIBDExtent.IsInit {

		firstIBDExtent.tableSpace.FlushToDisk(0, firstIBDExtent.XDESEntryWrapper.wrapper.ToByte())
	} else {
		bufferBlock := firstIBDExtent.pool.GetPageBlock(firstIBDExtent.spaceId, pageNo)
		copy(*bufferBlock.Frame, firstIBDExtent.XDESEntryWrapper.wrapper.ToByte())
		firstIBDExtent.pool.UpdateBlock(firstIBDExtent.spaceId, 0, bufferBlock)
	}

	return index

}

func (firstIBDExtent *PrimaryExtent) SetExtentState(xdesState common.XDES_STATE) {
	firstIBDExtent.extentState = xdesState
}

//其他区，以及该区管理的64个页面
//
type OrdinaryExtent struct {
	isInit           bool
	ExtentNumber     uint32   //区的首个页面号码，会管理后面64个页面
	Pages            []uint32 //64个页面
	XDESEntryWrapper *XDESEntryWrapper
	tableSpace       TableSpace
	spaceId          uint32
	pool             *buffer_pool.BufferPool
}

func NewOrdinaryExtent(spaceId uint32, extentPageNumber uint32, entryWrapper *XDESEntryWrapper, pool *buffer_pool.BufferPool) Extent {
	var otherIBDExtent = new(OrdinaryExtent)
	otherIBDExtent.ExtentNumber = extentPageNumber
	otherIBDExtent.XDESEntryWrapper = entryWrapper
	otherIBDExtent.spaceId = spaceId
	otherIBDExtent.pool = pool
	return otherIBDExtent
}

func NewOrdinaryExtentAtInit(spaceId uint32, extentPageNumber uint32, entryWrapper *XDESEntryWrapper, ts TableSpace, isInit bool) Extent {
	var otherIBDExtent = new(OrdinaryExtent)
	otherIBDExtent.ExtentNumber = extentPageNumber
	otherIBDExtent.XDESEntryWrapper = entryWrapper
	otherIBDExtent.spaceId = spaceId
	otherIBDExtent.tableSpace = ts
	otherIBDExtent.isInit = isInit
	return otherIBDExtent
}

func (o *OrdinaryExtent) LoadIPageWrapper(wrapper []uint32) {
	o.Pages = wrapper
}

func (o *OrdinaryExtent) ExtentType() string {
	return "ORDINARY"
}

func (o *OrdinaryExtent) FreePage(pageNumber uint32) {
	offset := pageNumber - 64*o.ExtentNumber
	o.XDESEntryWrapper.XdesDescPageMap[byte(offset)] = true
	buff := NewAllocatedPage(pageNumber).ToByte()
	if !o.isInit {
		bufferBlock := buffer_pool.NewBufferBlock(&(buff), o.spaceId, pageNumber)
		o.pool.UpdateBlock(o.spaceId, pageNumber, bufferBlock)
		return
	}
	o.tableSpace.FlushToDisk(pageNumber, buff)
}

func (o *OrdinaryExtent) AllocateNewPage(pageType int, tuple tuple.TableRowTuple) IPageWrapper {
	offset := o.XDESEntryWrapper.GetNearsFreePage()
	var index IPageWrapper

	//计算pageNo
	//
	pageNo := uint32(offset) + o.ExtentNumber*256

	switch pageType {
	case common.FILE_PAGE_INDEX:
		{
			index = NewPageIndexWithTuple(o.spaceId, pageNo, tuple)
			if o.isInit {
				o.tableSpace.FlushToDisk(pageNo, index.ToByte())
			} else {
				bufferBlock := o.pool.GetPageBlock(o.spaceId, pageNo)
				copy(index.ToByte(), *bufferBlock.Frame)
				o.pool.UpdateBlock(o.spaceId, pageNo, bufferBlock)
			}
		}
	}
	o.XDESEntryWrapper.DescPage(offset, false)
	pageXDesNo := o.XDESEntryWrapper.wrapper.GetFileHeader().GetCurrentPageOffset()
	if o.isInit {
		o.tableSpace.FlushToDisk(pageXDesNo, o.XDESEntryWrapper.wrapper.ToByte())
	} else {
		bufferBlock := o.pool.GetPageBlock(o.spaceId, pageXDesNo)
		copy(*bufferBlock.Frame, o.XDESEntryWrapper.wrapper.ToByte())
		o.pool.UpdateBlock(o.spaceId, pageXDesNo, bufferBlock)
	}
	return index
}

func (o *OrdinaryExtent) GetExtentId() uint32 {
	return o.ExtentNumber
}

func (o *OrdinaryExtent) SetExtentState(xdesState common.XDES_STATE) {
	o.XDESEntryWrapper.XDesState = xdesState
}

//其他区XdesPage,BufBitMap
//fsp 是特殊的fsp
//区只是存储在内存当中
// 第二个header 记录257-512个区
type SecondaryPrimaryExtent struct {
	XDesPage  *XDesPageWrapper
	IBufPages *IBuf
	Pages     []IPageWrapper
	ExtentId  int64
}

func (s SecondaryPrimaryExtent) LoadIPageWrapper(wrapper []uint32) {
	panic("implement me")
}

func (s SecondaryPrimaryExtent) ExtentType() string {
	panic("implement me")
}

func (s SecondaryPrimaryExtent) FreePage(pageNumber uint32) {
	panic("implement me")
}

func (s SecondaryPrimaryExtent) AllocateNewPage(pageType int, tuple tuple.TableRowTuple) IPageWrapper {
	panic("implement me")
}

func (s SecondaryPrimaryExtent) GetExtentId() uint32 {
	panic("implement me")
}

func (s SecondaryPrimaryExtent) SetExtentState(xdesState common.XDES_STATE) {
	panic("implement me")
}

func NewSecondaryPrimaryExtent(initPageNumber uint32) Extent {
	otherIBDExtent := new(SecondaryPrimaryExtent)
	otherIBDExtent.XDesPage = NewXDesWrapper(initPageNumber)
	otherIBDExtent.IBufPages = NewIBuf(initPageNumber + 1)
	otherIBDExtent.ExtentId = int64(initPageNumber >> 6)
	return otherIBDExtent
}
