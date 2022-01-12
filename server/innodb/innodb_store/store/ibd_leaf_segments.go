package store

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/innodb_store/store/storebytes/segs"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/tuple"
	"github.com/zhukovaskychina/xmysql-server/util"
)

//必须先从segment内分配extent和page，创建segment核心是从inode page 中分配空闲的inode
// u
// 段空间，叶子，非叶子,rollback,undo
// 每个索引有两个segment，一个leaf，一个non-leaf
// 每个表的段数，就是索引的*2，段空间
// 得注意系统表和非系统表之间的差别
// 也就是每一个IBD对象会有多个Segments
type DataSegment struct {
	Segment
	SegmentHeader      *segs.SegmentHeader
	iNodePageNo        uint32
	segmentId          uint64
	SegmentType        bool
	IndexName          string
	spaceId            uint32
	FreeExtentList     *ExtentList
	FreeFragExtentList *ExtentList
	FullFragExtentList *ExtentList
	fsp                *Fsp
	inode              *INode
	index              *uint32
	currentTableSpace  TableSpace
	TableTuple         tuple.TableRowTuple //叶子tuple
}

func (d *DataSegment) GetStatsCost(startPageNo, endPageNo uint32) map[string]int64 {
	var resultMap = make(map[string]int64)

	var extentNotFullList = d.inode.SegNotExtentMap[d.segmentId]
	//
	var extentFullList = d.inode.SegFullExtentMap[d.segmentId]

	extentStartNumber := startPageNo >> 6

	extentEndNumber := endPageNo >> 6
	resultMap["BLOCKS"] = int64((extentEndNumber - extentStartNumber) * 64)
	resultMap["nROWS"] = int64(0)
	if extentNotFullList != nil {
		nBlocks := extentNotFullList.RangeCost(extentStartNumber, extentEndNumber)
		resultMap["BLOCKS"] = int64(nBlocks)
		resultMap["nROWS"] = int64(nBlocks * 100)
	}
	if extentFullList != nil {
		nBlocks := extentFullList.RangeCost(extentStartNumber, extentEndNumber)
		resultMap["BLOCKS"] = int64(nBlocks)
		resultMap["nROWS"] = int64(nBlocks * 100)
	}

	return resultMap
}

func NewDataSegmentWithTupleAndBufferPool(spaceId uint32, pageNumber uint32, offset uint16,
	indexName string, tuple tuple.TableRowTuple, bufferPool *buffer_pool.BufferPool) Segment {
	var segment = new(InternalSegment)
	segment.SegmentHeader = segs.NewSegmentHeader(spaceId, pageNumber, offset)
	segment.pool = bufferPool
	segment.spaceId = spaceId
	bufferBlock := segment.pool.GetPageBlock(segment.spaceId, pageNumber)
	currentINodeBytes := *bufferBlock.Frame
	segment.IndexName = indexName
	currentInode := NewINodeByByte(currentINodeBytes, bufferPool).(*INode)
	segment.inode = currentInode
	segment.tuple = tuple

	return segment
}

func NewLeafSegmentWithTuple(spaceId uint32, pageNumber uint32, offset uint16, indexName string, space TableSpace, tuple tuple.TableRowTuple) Segment {
	var segment = new(DataSegment)
	segment.SegmentHeader = segs.NewSegmentHeader(spaceId, pageNumber, offset)
	currentINodeBytes, _ := space.LoadPageByPageNumber(pageNumber)
	segment.IndexName = indexName
	currentInode := NewINodeByByteSpace(currentINodeBytes, space).(*INode)
	segment.inode = currentInode

	segment.TableTuple = tuple

	return segment
}

func (d *DataSegment) AllocatePage() *Index {
	if d.inode.SegMap[d.segmentId].IsFragmentArrayFull() {
		extent := d.AllocateNewExtent()
		index := extent.AllocateNewPage(common.FILE_PAGE_INDEX, d.TableTuple).(*Index)
		return index
	} else {
		currentExtent := d.GetNotFullExtentList().GetLastElement()
		page := currentExtent.AllocateNewPage(common.FILE_PAGE_INDEX, d.TableTuple).(*Index)
		d.inode.SegMap[d.segmentId].ApplyDiscretePage(page.GetPageNumber())
		return page
	}
}

func (d *DataSegment) AllocateLeafPage() *Index {
	return d.AllocatePage()
}

func (d *DataSegment) AllocateInternalPage() *Index {
	panic("implement me")
}

func (d *DataSegment) GetSegmentHeader() *segs.SegmentHeader {
	return d.SegmentHeader
}

//叶子段，非不定长
func NewDataSegmentWithTableSpaceAtInit(spaceId uint32, pageNumber uint32, offset uint16, indexName string, space TableSpace) Segment {
	var segment = new(DataSegment)
	segment.SegmentHeader = segs.NewSegmentHeader(spaceId, pageNumber, offset)
	segment.IndexName = indexName
	segment.spaceId = spaceId
	segment.currentTableSpace = space
	segIdBytes := space.GetFirstFsp().GetNextSegmentId()
	segment.segmentId = util.ReadUB8Byte2Long(segIdBytes)
	segment.inode = space.GetFirstINode()
	segment.inode.AllocateINodeEntry(util.ReadUB8Byte2Long(segIdBytes))
	return segment
}

//申请extent之前，
//在开始向表中插入数据的时候，段是从某个碎片区以某个页面为单位来分配
//当某个段已经占用了32个碎片区后，就会以完整的区为单位来申请分配空间
func (d *DataSegment) AllocateNewExtent() Extent {
	currentExtent := d.currentTableSpace.GetFspFreeExtentList().DequeFirstElement()
	d.inode.GetFreeExtentList()
	//归属于某个段
	currentExtent.SetExtentState(common.XDES_FSEG)
	d.inode.SegFreeExtentMap[d.segmentId].AddExtent(currentExtent)
	return currentExtent
}

func (d *DataSegment) GetNotFullNUsedSize() uint32 {
	panic("implement me")
}

//获取所有FreeExtent链表
func (d *DataSegment) GetFreeExtentList() *ExtentList {
	var extentList = d.inode.SegFreeExtentMap[d.segmentId]
	return extentList
}

//获取所有FULLExtent链表
func (d *DataSegment) GetFullExtentList() *ExtentList {
	var extentList = d.inode.SegFullExtentMap[d.segmentId]
	return extentList
}
func (d *DataSegment) GetNotFullExtentList() *ExtentList {

	var extentList = d.inode.SegNotExtentMap[d.segmentId]
	if extentList == nil {
		extentList = NewExtentList("FREE_FRAG")
		fspFragList := d.currentTableSpace.GetFspFreeFragExtentList()
		extentList.AddExtent(fspFragList.GetFirstElement())
	}
	return extentList
}
