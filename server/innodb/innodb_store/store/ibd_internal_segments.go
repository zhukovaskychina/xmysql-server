package store

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/innodb_store/store/storebytes/pages"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/innodb_store/store/storebytes/segs"
	tuple2 "github.com/zhukovaskychina/xmysql-server/server/innodb/tuple"

	"github.com/zhukovaskychina/xmysql-server/util"
)

type InternalSegment struct {
	Segment
	SegmentHeader     *segs.SegmentHeader
	iNodePageNo       uint32
	segmentId         uint64
	SegmentType       bool
	IndexName         string
	spaceId           uint32
	extents           []Extent //区
	fsp               *Fsp
	inode             *INode
	index             *uint32
	currentTableSpace TableSpace
	inodeEntry        *pages.INodeEntry
	tuple             tuple2.TableRowTuple //节点tuple
	pool              *buffer_pool.BufferPool
}

func (i *InternalSegment) GetStatsCost(startPageNo, endPageNo uint32) map[string]int64 {
	var resultMap = make(map[string]int64)

	var extentNotFullList = i.inode.SegNotExtentMap[i.segmentId]
	//
	var extentFullList = i.inode.SegFullExtentMap[i.segmentId]

	extentStartNumber := startPageNo >> 6
	extentEndNumber := endPageNo >> 6
	resultMap["BLOCKS"] = int64((extentEndNumber - extentStartNumber) * 64)
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

func NewInternalSegmentWithTupleAndBufferPool(spaceId uint32, pageNumber uint32, offset uint16,
	indexName string, tuple tuple2.TableRowTuple, bufferPool *buffer_pool.BufferPool) Segment {
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

func NewInternalSegmentWithTuple(spaceId uint32, pageNumber uint32, offset uint16, indexName string,
	space TableSpace, tuple tuple2.TableRowTuple) Segment {
	var segment = new(InternalSegment)
	segment.SegmentHeader = segs.NewSegmentHeader(spaceId, pageNumber, offset)
	segment.currentTableSpace = space
	currentINodeBytes, _ := space.LoadPageByPageNumber(pageNumber)
	segment.IndexName = indexName
	currentInode := NewINodeByByteSpace(currentINodeBytes, space).(*INode)
	segment.inode = currentInode
	segment.tuple = tuple
	return segment
}

func NewInternalSegmentWithTableSpaceAtInit(spaceId uint32, pageNumber uint32, indexName string, offset uint16, space TableSpace) Segment {
	var segment = new(InternalSegment)
	segment.SegmentHeader = segs.NewSegmentHeader(spaceId, pageNumber, offset)
	segment.IndexName = indexName
	segment.spaceId = spaceId
	segment.currentTableSpace = space
	segIdBytes := space.GetFirstFsp().GetNextSegmentId()
	segment.segmentId = util.ReadUB8Byte2Long(segIdBytes)
	segment.inode = space.GetFirstINode()
	inodeEntry := segment.inode.AllocateINodeEntry(util.ReadUB8Byte2Long(segIdBytes))
	segment.inodeEntry = inodeEntry
	return segment
}

func (i *InternalSegment) AllocatePage() *Index {

	if i.inode.SegMap[i.segmentId].IsFragmentArrayFull() {
		extent := i.AllocateNewExtent()
		index := extent.AllocateNewPage(common.FILE_PAGE_INDEX, i.tuple).(*Index)
		return index
	} else {
		currentExtent := i.GetNotFullExtentList().GetLastElement()
		page := currentExtent.AllocateNewPage(common.FILE_PAGE_INDEX, i.tuple).(*Index)
		i.inode.SegMap[i.segmentId].ApplyDiscretePage(page.GetPageNumber())
		return page
	}
}

func (i *InternalSegment) AllocateLeafPage() *Index {
	panic("implement me")
}

func (i *InternalSegment) AllocateInternalPage() *Index {
	return i.AllocatePage()
}

func (i *InternalSegment) AllocateNewExtent() Extent {
	currentExtent := i.currentTableSpace.GetFspFreeExtentList().DequeFirstElement()
	i.inode.GetFreeExtentList()
	//归属于某个段
	currentExtent.SetExtentState(common.XDES_FSEG)
	i.inode.SegFreeExtentMap[i.segmentId].AddExtent(currentExtent)
	return currentExtent
}

func (i *InternalSegment) GetNotFullNUsedSize() uint32 {
	panic("implement me")
}

func (i *InternalSegment) GetFreeExtentList() *ExtentList {
	var extentList = i.inode.SegFreeExtentMap[i.segmentId]
	return extentList
}

func (i *InternalSegment) GetFullExtentList() *ExtentList {
	var extentList = i.inode.SegFullExtentMap[i.segmentId]
	return extentList
}

func (i *InternalSegment) GetNotFullExtentList() *ExtentList {
	var extentList = i.inode.SegNotExtentMap[i.segmentId]
	if extentList == nil {
		extentList = NewExtentList("FREE_FRAG")
		extentList.AddExtent(i.currentTableSpace.GetFspFreeFragExtentList().GetFirstElement())
	}
	i.inode.SegNotExtentMap[i.segmentId] = extentList
	return extentList
}

func (i *InternalSegment) GetSegmentHeader() *segs.SegmentHeader {
	return i.SegmentHeader
}

//type INodeEntry struct {
//	SegmentId           []byte               //8个字节，该结构体对应的段的编号（ID） 若值为0，则表示该SLot未被泗洪
//	NotFullNUsed        []byte               //4个字节，在Notfull链表中已经使用了多少个页面
//	FreeListBaseNode    []byte               //16个字节，Free链表
//	NotFullListBaseNode []byte               //16个字节，NotFull链表
//	FullListBaseNode    []byte               //16个字节，Full链表
//	MagicNumber         []byte               //4个字节 0x5D669D2
//	FragmentArrayEntry  []FragmentArrayEntry //一共32个array，每个ArrayEntry为零散的页面号
//}

func (i *InternalSegment) NewSegmentByBytes(bytes []byte, spaceId uint32) Segment {
	var segment = new(InternalSegment)

	segment.SegmentHeader = segs.NewSegmentHeader(spaceId, 0, 0)

	return segment
}
