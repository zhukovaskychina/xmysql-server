package store

import (
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/buffer_pool"
	"xmysql-server/server/innodb/innodb_store/store/storebytes/pages"
)
import "xmysql-server/util"

//TODO 这里需要加强对重启的后复盘操作
//用于管理数据文件中的segment,用于存储各种INodeEntry
//第三个page的类型FIL_PAGE_INODE
//每个Inode页面可以存储85个记录，
//PreNodePageNumber  []byte //4个字节	表示指向前一个INode页面号
//PreNodeOffset      []byte //2个字节 65536-1
//NextNodePageNumber []byte //4个字节  表示指向后一个INode页面号
//NextNodeOffSet     []byte //2个字节	65536-1
type INode struct {
	//IPageWrapper
	INodePage          *pages.INodePage
	SegMap             map[uint64]*INodeEntryWrapper
	ts                 TableSpace
	spaceId            uint32
	bufferPool         *buffer_pool.BufferPool
	PreNodePageNumber  uint32
	PreNodeOffset      uint16
	NextNodePageNumber uint32
	NextNodeOffset     uint16

	IsInit           bool
	SegFreeExtentMap map[uint64]*ExtentList
	SegFullExtentMap map[uint64]*ExtentList
	SegNotExtentMap  map[uint64]*ExtentList
}

func (iNode *INode) GetFileHeader() *pages.FileHeader {
	return &iNode.INodePage.FileHeader
}

func (iNode *INode) GetFileTrailer() *pages.FileTrailer {
	return &iNode.INodePage.FileTrailer
}

func (iNode *INode) ToByte() []byte {
	return iNode.INodePage.GetSerializeBytes()
}

func NewINode(spaceNo uint32, pageNo uint32) IPageWrapper {
	var inode = new(INode)
	inodePage := pages.NewINodePage(spaceNo, pageNo)
	inode.INodePage = inodePage
	inode.spaceId = spaceNo
	inode.SegMap = make(map[uint64]*INodeEntryWrapper)
	inode.SegFreeExtentMap = make(map[uint64]*ExtentList)
	inode.SegFullExtentMap = make(map[uint64]*ExtentList)
	inode.SegNotExtentMap = make(map[uint64]*ExtentList)
	inode.PreNodeOffset = util.ReadUB2Byte2Int(inode.INodePage.INodePageList.PreNodeOffset)
	inode.PreNodePageNumber = util.ReadUB4Byte2UInt32(inode.INodePage.INodePageList.PreNodePageNumber)
	inode.NextNodePageNumber = util.ReadUB4Byte2UInt32(inode.INodePage.INodePageList.NextNodePageNumber)
	inode.NextNodeOffset = util.ReadUB2Byte2Int(inode.INodePage.INodePageList.NextNodeOffSet)
	return inode
}

func NewINodeByByteSpace(content []byte, space TableSpace) IPageWrapper {
	var inode = new(INode)
	inode.INodePage = pages.NewINodeByParseBytes(content)
	inode.SegMap = make(map[uint64]*INodeEntryWrapper)
	inode.SegFreeExtentMap = make(map[uint64]*ExtentList)
	inode.SegFullExtentMap = make(map[uint64]*ExtentList)
	inode.SegNotExtentMap = make(map[uint64]*ExtentList)
	inode.ts = space
	inode.spaceId = util.ReadUB4Byte2UInt32(inode.INodePage.GetFileHeader().FilePageArch)
	inode.PreNodeOffset = util.ReadUB2Byte2Int(inode.INodePage.INodePageList.PreNodeOffset)
	inode.PreNodePageNumber = util.ReadUB4Byte2UInt32(inode.INodePage.INodePageList.PreNodePageNumber)
	inode.NextNodePageNumber = util.ReadUB4Byte2UInt32(inode.INodePage.INodePageList.NextNodePageNumber)
	inode.NextNodeOffset = util.ReadUB2Byte2Int(inode.INodePage.INodePageList.NextNodeOffSet)

	inode.IsInit = true

	inode.loadAll()
	return inode
}

func NewINodeByByte(content []byte, pool *buffer_pool.BufferPool) IPageWrapper {
	var inode = new(INode)
	inode.INodePage = pages.NewINodeByParseBytes(content)
	inode.SegMap = make(map[uint64]*INodeEntryWrapper)
	inode.SegFreeExtentMap = make(map[uint64]*ExtentList)
	inode.SegFullExtentMap = make(map[uint64]*ExtentList)
	inode.SegNotExtentMap = make(map[uint64]*ExtentList)
	inode.bufferPool = pool
	inode.spaceId = util.ReadUB4Byte2UInt32(inode.INodePage.GetFileHeader().FilePageArch)
	inode.PreNodeOffset = util.ReadUB2Byte2Int(inode.INodePage.INodePageList.PreNodeOffset)
	inode.PreNodePageNumber = util.ReadUB4Byte2UInt32(inode.INodePage.INodePageList.PreNodePageNumber)
	inode.NextNodePageNumber = util.ReadUB4Byte2UInt32(inode.INodePage.INodePageList.NextNodePageNumber)
	inode.NextNodeOffset = util.ReadUB2Byte2Int(inode.INodePage.INodePageList.NextNodeOffSet)
	return inode
}

func (iNode *INode) loadAll() {
	iNode.GetNotFullExtentList()
	iNode.GetFreeExtentList()
	iNode.GetFullExtentList()
}

func (iNode *INode) AllocateINodeEntry(segmentId uint64) *pages.INodeEntry {

	currentSegIentry := NewINodeEntryWrapper(segmentId, iNode)
	currentSegIentry.Initialize()
	iNode.SegMap[segmentId] = currentSegIentry

	var index = iNode.getCloseZeroSeg()
	INodeEntries := append(iNode.INodePage.INodeEntries[:index], currentSegIentry.ToINodeEntry())
	INodeEntries = append(INodeEntries, iNode.INodePage.INodeEntries[index+1:]...)
	iNode.INodePage.INodeEntries = INodeEntries
	return currentSegIentry.ToINodeEntry()
}

func (iNode *INode) getCloseZeroSeg() int {

	var result = 0
	for i := 0; i < 85; i++ {
		if (util.ReadUB4Byte2UInt32(iNode.INodePage.INodeEntries[i].MagicNumber)) != 0x5D669D2 {
			result = i
			break
		}
	}
	return result
}

//根据
func (iNode *INode) GetInodeEntryBySegmentId(segmentId uint64) (*pages.INodeEntry, bool) {

	for _, v := range iNode.INodePage.INodeEntries {
		flags := util.ReadUB8Byte2Long(v.SegmentId) == segmentId
		if flags {
			return v, true
		}
	}
	return nil, false
}

func (iNode *INode) GetINodeRootPageBySegId(segmentId uint64) (uint32, bool) {
	for _, v := range iNode.INodePage.INodeEntries {
		flags := util.ReadUB8Byte2Long(v.SegmentId) == segmentId
		if flags {
			return util.ReadUB4Byte2UInt32(v.FragmentArrayEntry[0].Byte()), true
		}
	}

	return 0, false
}

//获取所有FreeExtent链表
func (iNode *INode) GetFreeExtentList() {
	var SegFreeExtentMap = make(map[uint64]*ExtentList)

	for k, v := range iNode.SegMap {

		var extentList = NewExtentList("FREE_EXTENT")
		//获取当前free的区链表
		nextNodePgNo := v.FreeLastNodePageNumber
		//下一个offset,最大值256
		nextOffset := v.FreeLastNodeOffset

		//只处理当前256MB的数据,后面拓展

		if iNode.IsInit {
			//只处理当前256MB的数据,后面拓展
			for {
				//递归完成

				nextXDesEntryPage, _ := iNode.ts.LoadPageByPageNumber(nextNodePgNo)
				nextXDESEntry := nextXDesEntryPage[nextOffset : nextOffset+40]
				//
				filePageTypeBytes := nextXDesEntryPage[24:26]
				filePageType := util.ReadUB2Byte2Int(filePageTypeBytes)
				var ipagewrapper IPageWrapper
				if filePageType == common.FILE_PAGE_TYPE_FSP_HDR || filePageType == common.FILE_PAGE_TYPE_XDES {
					//如果是free，则将该区加入到链表中
					if util.ReadUB4Byte2UInt32(nextXDESEntry[20:24]) == uint32(common.XDES_FREE) {
						//计算区号
						//计算逻辑，根据偏移号，计算区号
						//获得当前区的相对位置
						currentPageNodeOffsetNo := (nextOffset - 150) / 40

						//	u := uint32(currentPageNodeOffsetNo) + nextNodePageNo
						var xdesEntryWrapper *XDESEntryWrapper

						if filePageType == common.FILE_PAGE_TYPE_FSP_HDR {
							ipagewrapper = NewFspByLoadBytes(nextXDesEntryPage).(*Fsp)
							//	fmt.Println(ipagewrapper)
							xdesEntryWrapper = ipagewrapper.(*Fsp).GetXDesEntryWrapper(int32(currentPageNodeOffsetNo))
						}
						var extent Extent
						if currentPageNodeOffsetNo == 0 {
							//加载普通区
							extent = NewPrimaryExtentWithPagesAtInit(0, uint32(currentPageNodeOffsetNo), xdesEntryWrapper, iNode.ts)
						} else {
							//加载普通区
							extent = NewOrdinaryExtentAtInit(0, uint32(currentPageNodeOffsetNo), xdesEntryWrapper, iNode.ts, true)
						}

						//
						startNo := uint32(currentPageNodeOffsetNo * 64)
						endNo := uint32((currentPageNodeOffsetNo + 1) * 64)
						//加载extent 到bufferPool
						var pageArrayWrapper = make([]uint32, 0)

						for i := startNo; i < endNo; i++ {
							pageArrayWrapper = append(pageArrayWrapper, i)
						}

						extent.LoadIPageWrapper(pageArrayWrapper)

						extentList.AddExtent(extent)

						nextNodePgNo = util.ReadUB4Byte2UInt32(nextXDESEntry[14:18])
						nextOffset = util.ReadUB2Byte2Int(nextXDESEntry[18:20])
						if nextOffset == 0 {
							break
						}
					}
					if nextOffset == 0 {
						break
					}

				} else {
					break
				}

			}

		} else {
			//只处理当前256MB的数据,后面拓展
			for {
				//递归完成

				bufferblock := iNode.bufferPool.GetPageBlock(iNode.spaceId, nextNodePgNo)
				nextXDesEntryPage := *bufferblock.Frame
				nextXDESEntry := nextXDesEntryPage[nextOffset : nextOffset+40]
				//
				filePageTypeBytes := nextXDesEntryPage[24:26]
				filePageType := util.ReadUB2Byte2Int(filePageTypeBytes)
				var ipagewrapper IPageWrapper
				if filePageType == common.FILE_PAGE_TYPE_FSP_HDR || filePageType == common.FILE_PAGE_TYPE_XDES {
					//如果是free，则将该区加入到链表中
					if util.ReadUB4Byte2UInt32(nextXDESEntry[20:24]) == uint32(common.XDES_FREE) {
						//计算区号
						//计算逻辑，根据偏移号，计算区号
						//获得当前区的相对位置
						currentPageNodeOffsetNo := (nextOffset - 150) / 40

						//	u := uint32(currentPageNodeOffsetNo) + nextNodePageNo
						var xdesEntryWrapper *XDESEntryWrapper

						if filePageType == common.FILE_PAGE_TYPE_FSP_HDR {
							ipagewrapper = NewFspByLoadBytes(nextXDesEntryPage).(*Fsp)
							//	fmt.Println(ipagewrapper)
							xdesEntryWrapper = ipagewrapper.(*Fsp).GetXDesEntryWrapper(int32(currentPageNodeOffsetNo))
						}
						//加载普通区
						extent := NewOrdinaryExtent(0, uint32(currentPageNodeOffsetNo), xdesEntryWrapper, iNode.bufferPool)

						startNo := uint32(currentPageNodeOffsetNo * 64)
						endNo := uint32((currentPageNodeOffsetNo + 1) * 64)
						iNode.bufferPool.RangePageLoad(0, startNo, endNo)
						//加载extent 到bufferPool
						var pageArrayWrapper = make([]uint32, 0)

						for i := startNo; i < endNo; i++ {
							pageArrayWrapper = append(pageArrayWrapper, i)
						}

						extent.LoadIPageWrapper(pageArrayWrapper)

						extentList.AddExtent(extent)

						nextNodePgNo = util.ReadUB4Byte2UInt32(nextXDESEntry[14:18])
						nextOffset = util.ReadUB2Byte2Int(nextXDESEntry[18:20])
						if nextOffset == 0 {
							break
						}
					}
					if nextOffset == 0 {
						break
					}

				} else {
					break
				}

			}

		}

		SegFreeExtentMap[k] = extentList
	}

	iNode.SegFreeExtentMap = SegFreeExtentMap
}

//获取所有FULLExtent链表
func (iNode *INode) GetFullExtentList() {
	var SegFullExtentMap = make(map[uint64]*ExtentList)

	for k, v := range iNode.SegMap {

		var extentList = NewExtentList("FREE_EXTENT")
		//获取当前free的区链表
		nextNodePgNo := v.FreeLastNodePageNumber
		//下一个offset,最大值256
		nextOffset := v.FreeLastNodeOffset

		//只处理当前256MB的数据,后面拓展

		if iNode.IsInit {
			//只处理当前256MB的数据,后面拓展
			for {
				//递归完成

				nextXDesEntryPage, _ := iNode.ts.LoadPageByPageNumber(nextNodePgNo)
				nextXDESEntry := nextXDesEntryPage[nextOffset : nextOffset+40]
				//
				filePageTypeBytes := nextXDesEntryPage[24:26]
				filePageType := util.ReadUB2Byte2Int(filePageTypeBytes)
				var ipagewrapper IPageWrapper
				if filePageType == common.FILE_PAGE_TYPE_FSP_HDR || filePageType == common.FILE_PAGE_TYPE_XDES {
					//如果是free，则将该区加入到链表中
					if util.ReadUB4Byte2UInt32(nextXDESEntry[20:24]) == uint32(common.XDES_FREE) {
						//计算区号
						//计算逻辑，根据偏移号，计算区号
						//获得当前区的相对位置
						currentPageNodeOffsetNo := (nextOffset - 150) / 40

						//	u := uint32(currentPageNodeOffsetNo) + nextNodePageNo
						var xdesEntryWrapper *XDESEntryWrapper

						if filePageType == common.FILE_PAGE_TYPE_FSP_HDR {
							ipagewrapper = NewFspByLoadBytes(nextXDesEntryPage).(*Fsp)
							//	fmt.Println(ipagewrapper)
							xdesEntryWrapper = ipagewrapper.(*Fsp).GetXDesEntryWrapper(int32(currentPageNodeOffsetNo))
						}
						var extent Extent
						if currentPageNodeOffsetNo == 0 {
							//加载普通区
							extent = NewPrimaryExtentWithPagesAtInit(0, uint32(currentPageNodeOffsetNo), xdesEntryWrapper, iNode.ts)
						} else {
							//加载普通区
							extent = NewOrdinaryExtentAtInit(0, uint32(currentPageNodeOffsetNo), xdesEntryWrapper, iNode.ts, true)
						}

						//
						startNo := uint32(currentPageNodeOffsetNo * 64)
						endNo := uint32((currentPageNodeOffsetNo + 1) * 64)
						//加载extent 到bufferPool
						var pageArrayWrapper = make([]uint32, 0)

						for i := startNo; i < endNo; i++ {
							pageArrayWrapper = append(pageArrayWrapper, i)
						}

						extent.LoadIPageWrapper(pageArrayWrapper)

						extentList.AddExtent(extent)

						nextNodePgNo = util.ReadUB4Byte2UInt32(nextXDESEntry[14:18])
						nextOffset = util.ReadUB2Byte2Int(nextXDESEntry[18:20])
						if nextOffset == 0 {
							break
						}
					}
					if nextOffset == 0 {
						break
					}

				} else {
					break
				}

			}

		} else {
			//只处理当前256MB的数据,后面拓展
			for {
				//递归完成

				bufferblock := iNode.bufferPool.GetPageBlock(iNode.spaceId, nextNodePgNo)
				nextXDesEntryPage := *bufferblock.Frame
				nextXDESEntry := nextXDesEntryPage[nextOffset : nextOffset+40]
				//
				filePageTypeBytes := nextXDesEntryPage[24:26]
				filePageType := util.ReadUB2Byte2Int(filePageTypeBytes)
				var ipagewrapper IPageWrapper
				if filePageType == common.FILE_PAGE_TYPE_FSP_HDR || filePageType == common.FILE_PAGE_TYPE_XDES {
					//如果是free，则将该区加入到链表中
					if util.ReadUB4Byte2UInt32(nextXDESEntry[20:24]) == uint32(common.XDES_FREE) {
						//计算区号
						//计算逻辑，根据偏移号，计算区号
						//获得当前区的相对位置
						currentPageNodeOffsetNo := (nextOffset - 150) / 40

						//	u := uint32(currentPageNodeOffsetNo) + nextNodePageNo
						var xdesEntryWrapper *XDESEntryWrapper

						if filePageType == common.FILE_PAGE_TYPE_FSP_HDR {
							ipagewrapper = NewFspByLoadBytes(nextXDesEntryPage).(*Fsp)
							//	fmt.Println(ipagewrapper)
							xdesEntryWrapper = ipagewrapper.(*Fsp).GetXDesEntryWrapper(int32(currentPageNodeOffsetNo))
						}
						//加载普通区
						extent := NewOrdinaryExtent(0, uint32(currentPageNodeOffsetNo), xdesEntryWrapper, iNode.bufferPool)

						startNo := uint32(currentPageNodeOffsetNo * 64)
						endNo := uint32((currentPageNodeOffsetNo + 1) * 64)
						iNode.bufferPool.RangePageLoad(0, startNo, endNo)
						//加载extent 到bufferPool
						var pageArrayWrapper = make([]uint32, 0)

						for i := startNo; i < endNo; i++ {
							pageArrayWrapper = append(pageArrayWrapper, i)
						}

						extent.LoadIPageWrapper(pageArrayWrapper)

						extentList.AddExtent(extent)

						nextNodePgNo = util.ReadUB4Byte2UInt32(nextXDESEntry[14:18])
						nextOffset = util.ReadUB2Byte2Int(nextXDESEntry[18:20])
						if nextOffset == 0 {
							break
						}
					}
					if nextOffset == 0 {
						break
					}

				} else {
					break
				}

			}

		}

		SegFullExtentMap[k] = extentList
	}

	iNode.SegFullExtentMap = SegFullExtentMap
}
func (iNode *INode) GetNotFullExtentList() {
	var SegNotFullExtentMap = make(map[uint64]*ExtentList)

	for k, v := range iNode.SegMap {

		var extentList = NewExtentList("FREE_EXTENT")
		//获取当前free的区链表
		nextNodePgNo := v.FreeLastNodePageNumber
		//下一个offset,最大值256
		nextOffset := v.FreeLastNodeOffset

		//只处理当前256MB的数据,后面拓展

		if iNode.IsInit {
			//只处理当前256MB的数据,后面拓展
			for {
				//递归完成

				nextXDesEntryPage, _ := iNode.ts.LoadPageByPageNumber(nextNodePgNo)
				nextXDESEntry := nextXDesEntryPage[nextOffset : nextOffset+40]
				//
				filePageTypeBytes := nextXDesEntryPage[24:26]
				filePageType := util.ReadUB2Byte2Int(filePageTypeBytes)
				var ipagewrapper IPageWrapper
				if filePageType == common.FILE_PAGE_TYPE_FSP_HDR || filePageType == common.FILE_PAGE_TYPE_XDES {
					//如果是free，则将该区加入到链表中
					if util.ReadUB4Byte2UInt32(nextXDESEntry[20:24]) == uint32(common.XDES_FREE) {
						//计算区号
						//计算逻辑，根据偏移号，计算区号
						//获得当前区的相对位置
						currentPageNodeOffsetNo := (nextOffset - 150) / 40

						//	u := uint32(currentPageNodeOffsetNo) + nextNodePageNo
						var xdesEntryWrapper *XDESEntryWrapper

						if filePageType == common.FILE_PAGE_TYPE_FSP_HDR {
							ipagewrapper = NewFspByLoadBytes(nextXDesEntryPage).(*Fsp)
							//	fmt.Println(ipagewrapper)
							xdesEntryWrapper = ipagewrapper.(*Fsp).GetXDesEntryWrapper(int32(currentPageNodeOffsetNo))
						}
						var extent Extent
						if currentPageNodeOffsetNo == 0 {
							//加载普通区
							extent = NewPrimaryExtentWithPagesAtInit(0, uint32(currentPageNodeOffsetNo), xdesEntryWrapper, iNode.ts)
						} else {
							//加载普通区
							extent = NewOrdinaryExtentAtInit(0, uint32(currentPageNodeOffsetNo), xdesEntryWrapper, iNode.ts, true)
						}

						//
						startNo := uint32(currentPageNodeOffsetNo * 64)
						endNo := uint32((currentPageNodeOffsetNo + 1) * 64)
						//加载extent 到bufferPool
						var pageArrayWrapper = make([]uint32, 0)

						for i := startNo; i < endNo; i++ {
							pageArrayWrapper = append(pageArrayWrapper, i)
						}

						extent.LoadIPageWrapper(pageArrayWrapper)

						extentList.AddExtent(extent)

						nextNodePgNo = util.ReadUB4Byte2UInt32(nextXDESEntry[14:18])
						nextOffset = util.ReadUB2Byte2Int(nextXDESEntry[18:20])
						if nextOffset == 0 {
							break
						}
					}
					if nextOffset == 0 {
						break
					}

				} else {
					break
				}

			}

		} else {
			//只处理当前256MB的数据,后面拓展
			for {
				//递归完成

				bufferblock := iNode.bufferPool.GetPageBlock(iNode.spaceId, nextNodePgNo)
				nextXDesEntryPage := *bufferblock.Frame
				nextXDESEntry := nextXDesEntryPage[nextOffset : nextOffset+40]
				//
				filePageTypeBytes := nextXDesEntryPage[24:26]
				filePageType := util.ReadUB2Byte2Int(filePageTypeBytes)
				var ipagewrapper IPageWrapper
				if filePageType == common.FILE_PAGE_TYPE_FSP_HDR || filePageType == common.FILE_PAGE_TYPE_XDES {
					//如果是free，则将该区加入到链表中
					if util.ReadUB4Byte2UInt32(nextXDESEntry[20:24]) == uint32(common.XDES_FREE) {
						//计算区号
						//计算逻辑，根据偏移号，计算区号
						//获得当前区的相对位置
						currentPageNodeOffsetNo := (nextOffset - 150) / 40

						//	u := uint32(currentPageNodeOffsetNo) + nextNodePageNo
						var xdesEntryWrapper *XDESEntryWrapper

						if filePageType == common.FILE_PAGE_TYPE_FSP_HDR {
							ipagewrapper = NewFspByLoadBytes(nextXDesEntryPage).(*Fsp)
							//	fmt.Println(ipagewrapper)
							xdesEntryWrapper = ipagewrapper.(*Fsp).GetXDesEntryWrapper(int32(currentPageNodeOffsetNo))
						}
						//加载普通区
						extent := NewOrdinaryExtent(0, uint32(currentPageNodeOffsetNo), xdesEntryWrapper, iNode.bufferPool)

						startNo := uint32(currentPageNodeOffsetNo * 64)
						endNo := uint32((currentPageNodeOffsetNo + 1) * 64)
						iNode.bufferPool.RangePageLoad(0, startNo, endNo)
						//加载extent 到bufferPool
						var pageArrayWrapper = make([]uint32, 0)

						for i := startNo; i < endNo; i++ {
							pageArrayWrapper = append(pageArrayWrapper, i)
						}

						extent.LoadIPageWrapper(pageArrayWrapper)

						extentList.AddExtent(extent)

						nextNodePgNo = util.ReadUB4Byte2UInt32(nextXDESEntry[14:18])
						nextOffset = util.ReadUB2Byte2Int(nextXDESEntry[18:20])
						if nextOffset == 0 {
							break
						}
					}
					if nextOffset == 0 {
						break
					}

				} else {
					break
				}

			}

		}

		SegNotFullExtentMap[k] = extentList
	}

	iNode.SegNotExtentMap = SegNotFullExtentMap
}

func (iNode *INode) GetSerializeBytes() []byte {

	return iNode.INodePage.GetSerializeBytes()
}

//
//SegmentId           []byte               //8个字节，该结构体对应的段的编号（ID） 若值为0，则表示该SLot未被泗洪
//NotFullNUsed        []byte               //4个字节，在Notfull链表中已经使用了多少个页面
//FreeListBaseNode    []byte               //16个字节，Free链表
//NotFullListBaseNode []byte               //16个字节，NotFull链表
//FullListBaseNode    []byte               //16个字节，Full链表
//MagicNumber         []byte               //4个字节 0x5D669D2
//FragmentArrayEntry  []FragmentArayEntry //一共32个array，每个ArrayEntry为零散的页面号

func (iNode *INode) GetSegmentByOffset(offset uint16, segmentType int) Segment {

	//entry := iNode.INodePage.INodeEntries[offset]
	//currentINodeEntry:=iNode.INodePage.INodeEntries[offset]

	return nil
}

//NotFullNUsed        []byte               //4个字节，在Notfull链表中已经使用了多少个页面
//FreeListBaseNode    []byte               //16个字节，Free链表 segment上所有page均空闲的extent链表
//NotFullListBaseNode []byte               //16个字节，NotFull链表 至少有一个page分配给当前Segment的Extent链表，全部用完时，转移到FSEG_FULL上，全部释放时，则归还给当前表空间FSP_FREE链表
//FullListBaseNode    []byte               //16个字节，Full链表 segment上page被完全使用的extent链表
//MagicNumber         []byte               //4个字节 0x5D669D2
//FragmentArrayEntry  []FragmentArrayEntry //一共32个array，每个ArrayEntry为零散的页面号
//
type INodeEntryWrapper struct {
	wrapper IPageWrapper

	SegmentId   uint64
	MagicNumber uint32

	NotFullInUsedPages uint32

	FreeListLength          uint32
	FreeFirstNodePageNumber uint32
	FreeFirstNodeOffset     uint16
	FreeLastNodePageNumber  uint32
	FreeLastNodeOffset      uint16

	NotFullListLength          uint32
	NotFullFirstNodePageNumber uint32
	NotFullFirstNodeOffset     uint16
	NotFullLastNodePageNumber  uint32
	NotFullLastNodeOffset      uint16

	FullListLength          uint32
	FullFirstNodePageNumber uint32
	FullFirstNodeOffset     uint16
	FullLastNodePageNumber  uint32
	FullLastNodeOffset      uint16

	fragmentArray []pages.FragmentArrayEntry
}

func NewINodeEntryWrapper(segmentId uint64, wrapper IPageWrapper) *INodeEntryWrapper {
	var inodeEntry = new(INodeEntryWrapper)
	inodeEntry.wrapper = wrapper
	inodeEntry.SegmentId = segmentId
	inodeEntry.fragmentArray = make([]pages.FragmentArrayEntry, 32)
	for i := 0; i < 32; i++ {
		inodeEntry.fragmentArray[i] = pages.FragmentArrayEntry{PageNo: util.AppendByte(4)}
	}

	return inodeEntry
}

func (inodeEntry *INodeEntryWrapper) Initialize() {
	inodeEntry.MagicNumber = 0x5D669D2
}

func (inodeEntry *INodeEntryWrapper) ApplyDiscretePage(pageNumber uint32) {

	for k, v := range inodeEntry.fragmentArray {
		if util.ReadUB4Byte2UInt32(v.PageNo) == 0 {
			inodeEntry.fragmentArray[k].PageNo = util.ConvertUInt4Bytes(pageNumber)
			break
		}
	}
	//用于初始化
	if inodeEntry.wrapper.(*INode).IsInit {
		inodeWrapper := inodeEntry.wrapper.(*INode)
		inodeWrapper.ts.FlushToDisk(inodeWrapper.GetFileHeader().GetCurrentPageOffset(), inodeWrapper.ToByte())
	}

}

func (inodeEntry *INodeEntryWrapper) IsFragmentArrayFull() bool {
	for _, v := range inodeEntry.fragmentArray {
		if util.ReadUB4Byte2UInt32(v.PageNo) == 0 {
			return false
		}
	}
	return true
}

func (inodeEntry *INodeEntryWrapper) ToINodeEntry() *pages.INodeEntry {
	freeInfo := CommonNodeInfo{inodeEntry.FreeListLength,
		inodeEntry.FreeFirstNodePageNumber,
		inodeEntry.FreeFirstNodeOffset,
		inodeEntry.FreeLastNodePageNumber,
		inodeEntry.FreeLastNodeOffset}
	notFullInfo := CommonNodeInfo{
		NodeInfoLength:     inodeEntry.NotFullListLength,
		PreNodePageNumber:  inodeEntry.NotFullFirstNodePageNumber,
		PreNodeOffset:      inodeEntry.NotFullFirstNodeOffset,
		NextNodePageNumber: inodeEntry.NotFullLastNodePageNumber,
		NextNodeOffset:     inodeEntry.NotFullLastNodeOffset,
	}
	fullInfo := CommonNodeInfo{
		NodeInfoLength:     inodeEntry.FullListLength,
		PreNodePageNumber:  inodeEntry.FullFirstNodePageNumber,
		PreNodeOffset:      inodeEntry.FullFirstNodeOffset,
		NextNodePageNumber: inodeEntry.FullLastNodePageNumber,
		NextNodeOffset:     inodeEntry.FullFirstNodeOffset,
	}
	inodeCurrentEntry := pages.INodeEntry{
		SegmentId:           util.ConvertULong8Bytes(inodeEntry.SegmentId),
		NotFullNUsed:        util.ConvertUInt4Bytes(inodeEntry.NotFullInUsedPages),
		FreeListBaseNode:    freeInfo.ToBytes(),
		NotFullListBaseNode: notFullInfo.ToBytes(),
		FullListBaseNode:    fullInfo.ToBytes(),
		MagicNumber:         util.ConvertUInt4Bytes(inodeEntry.MagicNumber),
		FragmentArrayEntry:  inodeEntry.fragmentArray,
	}

	return &inodeCurrentEntry
}
