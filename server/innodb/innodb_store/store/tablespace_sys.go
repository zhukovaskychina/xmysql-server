package store

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/innodb_store/store/storebytes/blocks"
	"path"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/util"
)

/*************

系统表空间和独立空间的前3个页面一致，
但是3-7的页面是系统表空间独有的。

//////////////////////////
// type   //  描述
/////////////////////////
// SYS	  //  Insert Buffer Header  存储ChangeBuffer的头部信息
/////////////////////////
// INDEX  //  Insert Buffer Root   存储ChangeBuffer的根页面
///////////////////////////
// TRX_SYS//  Transaction System 事物系统的相关信息
////////////////////////////
// SYS    //  First Rollback Segment  第一个回滚段的信息
////////////////////////////
// SYS    //  Data Dictionary Header 数据字典头部信息
////////////////////////////
对于一个新的segment，总是优先填满32个frag page数组，之后才会为其分配完整的Extent，可以利用碎片页，并避免小表占用太多空间。
尽量获得hint page;
如果segment上未使用的page太多，则尽量利用segment上的page。

***********/
type SysTableSpace struct {
	conf       *conf.Cfg
	blockFile  *blocks.BlockFile
	IsInit     bool   //是否初始化状态
	Fsp        *Fsp   //0 号页面
	IBuf       *IBuf  //1 号页面
	FirstInode *INode //2 号页面

	insertBufferHeader *Allocated //3

	insertBufferRootIndex *Index //4

	transactionSystem *Allocated //5

	rollBackSegment *Allocated //6

	DataDict *DataDictWrapper //7号页面

	SysTables *Index //8号页面

	SysTablesIds *Index //9号页面

	SysColumns *Index //10号页面

	SysIndexes *Index //11号页面

	SysFields *Index //12号页面

	SysForeign *Index //13号页面

	SysForeignCols *Index //14号页面

	SysTableSpaces *Index //15号页面

	SysDataFiles *Index //16号页面

	SysVirtual *Index //17号页面

	InodeMap map[uint32]*INode

	FirstIBDExtent *PrimaryExtent //第一个区

	OtherExtents []Extent //其他区

	DictionarySys *DictionarySys

	pool *buffer_pool.BufferPool

	lockmu sync.Mutex
}

func (sysTable *SysTableSpace) FlushToDisk(pageNo uint32, content []byte) {
	sysTable.blockFile.WriteContentByPage(int64(pageNo), content)
}

func (sysTable *SysTableSpace) LoadExtentFromDisk(extentNumber int) Extent {
	panic("implement me")
}

//初始化数据库
func NewSysTableSpace(cfg *conf.Cfg, IsInit bool) TableSpace {
	tableSpace := new(SysTableSpace)
	tableSpace.conf = cfg
	tableSpace.IsInit = IsInit
	filePath := path.Join(cfg.BaseDir, "/", "ibdata1")
	isFlag, _ := util.PathExists(filePath)
	blockfile := blocks.NewBlockFile(cfg.BaseDir, "ibdata1", 256*64*16384)
	tableSpace.blockFile = blockfile
	if !isFlag {
		tableSpace.blockFile.CreateFile()
		tableSpace.initHeadPage()

		tableSpace.initFreeLimit()
		tableSpace.flushToDisk()
		tableSpace.initSysTableDataDict()
		tableSpace.initDatabaseDictionary()
		tableSpace.flushToDisk()
		tableSpace.initAllSysTables()
		//	tableSpace.flushToDisk()
	}
	return tableSpace
}

func NewSysTableSpaceByBufferPool(cfg *conf.Cfg, pool *buffer_pool.BufferPool) TableSpace {
	tableSpace := new(SysTableSpace)
	tableSpace.conf = cfg
	filePath := path.Join(cfg.BaseDir, "/", "ibdata1")
	isFlag, _ := util.PathExists(filePath)
	blockfile := blocks.NewBlockFile(cfg.BaseDir, "ibdata1", 256*64*16384)
	tableSpace.blockFile = blockfile
	tableSpace.pool = pool
	tableSpace.pool.FileSystem.AddTableSpace(tableSpace)
	if !isFlag {
		tableSpace.initHeadPage()
		tableSpace.initFreeLimit()
		tableSpace.flushToDisk()
		tableSpace.initSysTableDataDict()
		tableSpace.initDatabaseDictionary()
		tableSpace.flushToDisk()
		tableSpace.initAllSysTables()
	}

	return tableSpace
}

func (sysTable *SysTableSpace) initHeadPage() {
	//初始化FspHrdPage
	sysTable.Fsp = NewFspInitialize(0).(*Fsp)
	sysTable.IBuf = NewIBuf(0)
	sysTable.FirstInode = NewINode(0, 2).(*INode)

	sysTable.insertBufferHeader = NewAllocatedPage(3).(*Allocated)

	sysTable.insertBufferRootIndex = NewPageIndex(4).(*Index)

	sysTable.transactionSystem = NewAllocatedPage(5).(*Allocated)

	sysTable.rollBackSegment = NewAllocatedPage(6).(*Allocated)

	sysTable.DataDict = NewDataDictWrapper().(*DataDictWrapper)
	// TODO	完成这里的数据字典的加载优化
	sysTable.SysTables = NewPageIndexWithTuple(0, 8, NewSysTableTupleWithFlags(common.PAGE_LEAF)).(*Index)
	sysTable.SysTablesIds = NewPageIndexWithTuple(0, 9, NewSysTableTupleWithFlags(common.PAGE_LEAF)).(*Index)
	sysTable.SysColumns = NewPageIndexWithTuple(0, 10, NewSysColumnsTupleWithFlags(common.PAGE_LEAF)).(*Index)
	sysTable.SysIndexes = NewPageIndexWithTuple(0, 11, NewSysTableTupleWithFlags(common.PAGE_LEAF)).(*Index)
	sysTable.SysFields = NewPageIndexWithTuple(0, 12, NewSysIndexTupleWithFlags(common.PAGE_LEAF)).(*Index)
	sysTable.SysTableSpaces = NewPageIndexWithTuple(0, 13, NewSysSpacesTupleWithFlags(common.PAGE_LEAF)).(*Index)
	sysTable.SysForeign = NewPageIndexWithTuple(0, 14, NewSysTableTupleWithFlags(common.PAGE_LEAF)).(*Index)
	sysTable.SysForeignCols = NewPageIndexWithTuple(0, 15, NewSysTableTupleWithFlags(common.PAGE_LEAF)).(*Index)
	sysTable.SysDataFiles = NewPageIndexWithTuple(0, 16, NewSysDataFilesTupleWithFlags(common.PAGE_LEAF)).(*Index)
	sysTable.SysVirtual = NewPageIndexWithTuple(0, 17, NewSysTableTupleWithFlags(common.PAGE_LEAF)).(*Index)

}

//初始化数据字典
func (sysTable *SysTableSpace) initDatabaseDictionary() {
	sysTable.DictionarySys = NewDictionarySysByWrapper(sysTable.DataDict)
	sysTable.DictionarySys.initDictionary(sysTable)
}

//初始化各种系统表

//SysTableSpaces
//SysDataFiles
func (sysTable *SysTableSpace) initAllSysTables() {

	sysTable.DictionarySys.initializeSysTableSpacesTable(common.INFORMATION_SCHEMAS, NewSysSpacesTuple())

	sysTable.DictionarySys.initializeSysDataFilesTable(common.INFORMATION_SCHEMAS, NewSysDataFilesTuple())

}

func (sysTable *SysTableSpace) initFreeLimit() {
	sysTable.initFspExtents()
}

//初始化FspExtent信息
func (sysTable *SysTableSpace) initFspExtents() {
	sysTable.Fsp.SetFspFreeExtentListInfo(&CommonNodeInfo{NodeInfoLength: 1, PreNodePageNumber: 0, PreNodeOffset: 0, NextNodePageNumber: 0, NextNodeOffset: 190})
	sysTable.Fsp.SetFreeLimit(128)
	sysTable.Fsp.SetFspFreeFragExtentListInfo(&CommonNodeInfo{NodeInfoLength: 1, PreNodePageNumber: 0, PreNodeOffset: 0, NextNodePageNumber: 0, NextNodeOffset: 150})
}

//初始化字典段
//
func (sysTable *SysTableSpace) initSysTableClusterSegments() {
	dataDictSegs := NewDataSegmentWithTableSpaceAtInit(0, 2, 0, "SYS_TABLES_CLUSTER", sysTable)
	internalSegs := NewInternalSegmentWithTableSpaceAtInit(0, 2, "SYS_TABLES_CLUSTER", 1, sysTable)

	sysTable.SysTables.SetPageBtrTop(dataDictSegs.GetSegmentHeader().GetBytes())
	sysTable.SysTables.SetPageBtrSegs(internalSegs.GetSegmentHeader().GetBytes())
	internalSegs.AllocateInternalPage()
	dataDictSegs.AllocateLeafPage()
}

//初始化字典段
//
func (sysTable *SysTableSpace) initSysTableIdsSegments() {
	dataDictSegs := NewDataSegmentWithTableSpaceAtInit(0, 2, 2, "SYS_TABLE_IDS", sysTable)
	internalSegs := NewInternalSegmentWithTableSpaceAtInit(0, 2, "SYS_TABLE_IDS", 3, sysTable)
	sysTable.SysTablesIds.SetPageBtrTop(dataDictSegs.GetSegmentHeader().GetBytes())
	sysTable.SysTablesIds.SetPageBtrSegs(internalSegs.GetSegmentHeader().GetBytes())
	internalSegs.AllocateInternalPage()
	dataDictSegs.AllocateLeafPage()
}

func (sysTable *SysTableSpace) initSysTableColumnsSegments() {
	dataDictSegs := NewDataSegmentWithTableSpaceAtInit(0, 2, 4, "SYS_TABLE_COLUMNS", sysTable)
	internalSegs := NewInternalSegmentWithTableSpaceAtInit(0, 2, "SYS_TABLE_COLUMNS", 5, sysTable)
	sysTable.SysColumns.SetPageBtrTop(dataDictSegs.GetSegmentHeader().GetBytes())
	sysTable.SysColumns.SetPageBtrSegs(internalSegs.GetSegmentHeader().GetBytes())
	internalSegs.AllocateInternalPage()
	dataDictSegs.AllocateLeafPage()
}

func (sysTable *SysTableSpace) initSysTableIndexesSegments() {
	dataDictSegs := NewDataSegmentWithTableSpaceAtInit(0, 2, 6, "SYS_TABLE_INDEXES", sysTable)
	internalSegs := NewInternalSegmentWithTableSpaceAtInit(0, 2, "SYS_TABLE_INDEXES", 7, sysTable)
	sysTable.SysIndexes.SetPageBtrTop(dataDictSegs.GetSegmentHeader().GetBytes())
	sysTable.SysIndexes.SetPageBtrSegs(internalSegs.GetSegmentHeader().GetBytes())
	internalSegs.AllocateInternalPage()
	dataDictSegs.AllocateLeafPage()
}

func (sysTable *SysTableSpace) initSysTableFieldsSegments() {
	dataDictSegs := NewDataSegmentWithTableSpaceAtInit(0, 2, 8, "SYS_TABLE_FIELDS", sysTable)
	internalSegs := NewInternalSegmentWithTableSpaceAtInit(0, 2, "SYS_TABLE_FIELDS", 9, sysTable)
	sysTable.SysFields.SetPageBtrTop(dataDictSegs.GetSegmentHeader().GetBytes())
	sysTable.SysFields.SetPageBtrSegs(internalSegs.GetSegmentHeader().GetBytes())
	internalSegs.AllocateInternalPage()
	dataDictSegs.AllocateLeafPage()
}

//初始化数据字典表
func (sysTable *SysTableSpace) initSysTableDataDict() {
	sysTable.initSysTableClusterSegments()
	sysTable.initSysTableIdsSegments()
	sysTable.initSysTableColumnsSegments()
	sysTable.initSysTableIndexesSegments()
	sysTable.initSysTableFieldsSegments()

}

func (sysTable *SysTableSpace) flushToDisk() {
	sysTable.lockmu.Lock()
	sysTable.blockFile.WriteContentByPage(0, sysTable.Fsp.GetSerializeBytes())
	sysTable.lockmu.Unlock()
	sysTable.lockmu.Lock()
	sysTable.blockFile.WriteContentByPage(1, sysTable.IBuf.GetSerializeBytes())
	sysTable.lockmu.Unlock()
	sysTable.lockmu.Lock()
	sysTable.blockFile.WriteContentByPage(2, sysTable.FirstInode.GetSerializeBytes())
	//sysTable.blockFile.WriteContentByPage(3,nil)
	sysTable.lockmu.Unlock()
	sysTable.lockmu.Lock()
	sysTable.blockFile.WriteContentByPage(7, sysTable.DataDict.DataHrdPage.GetSerializeBytes())
	sysTable.lockmu.Unlock()
	sysTable.lockmu.Lock()
	sysTable.blockFile.WriteContentByPage(8, sysTable.SysTables.IndexPage.GetSerializeBytes())
	sysTable.lockmu.Unlock()
	sysTable.lockmu.Lock()
	sysTable.blockFile.WriteContentByPage(9, sysTable.SysTablesIds.IndexPage.GetSerializeBytes())
	sysTable.lockmu.Unlock()
	sysTable.lockmu.Lock()
	sysTable.blockFile.WriteContentByPage(10, sysTable.SysColumns.IndexPage.GetSerializeBytes())
	sysTable.lockmu.Unlock()
	sysTable.lockmu.Lock()
	sysTable.blockFile.WriteContentByPage(11, sysTable.SysIndexes.IndexPage.GetSerializeBytes())
	sysTable.lockmu.Unlock()
	sysTable.lockmu.Lock()
	sysTable.blockFile.WriteContentByPage(12, sysTable.SysFields.IndexPage.GetSerializeBytes())
	sysTable.lockmu.Unlock()
	sysTable.lockmu.Lock()
	sysTable.blockFile.WriteContentByPage(13, sysTable.SysTableSpaces.IndexPage.GetSerializeBytes())
	sysTable.lockmu.Unlock()
	sysTable.lockmu.Lock()
	sysTable.blockFile.WriteContentByPage(16, sysTable.SysDataFiles.IndexPage.GetSerializeBytes())
	sysTable.lockmu.Unlock()
	sysTable.blockFile.WriteContentByPage(17, sysTable.SysVirtual.IndexPage.GetSerializeBytes())
}

func (sysTable *SysTableSpace) initSysTableTable() {
}

func (sysTable *SysTableSpace) LoadPageByPageNumber(pageNo uint32) ([]byte, error) {
	return sysTable.blockFile.ReadPageByNumber(pageNo)
}

//获取所有的完全用满的InodePage链表
func (sysTable *SysTableSpace) GetSegINodeFullList() *INodeList {
	var inodeList = NewINodeList("FULL_LIST")

	bufferBlockINode := sysTable.pool.GetPageBlock(0, 2)

	currentINodeBytes := *bufferBlockINode.Frame
	inode := NewINodeByByte(currentINodeBytes, sysTable.pool).(*INode)

	nextNodePageNo := inode.NextNodePageNumber
	for {

		if nextNodePageNo == 0 {
			break
		}
		bufferBlock := sysTable.pool.GetPageBlock(0, nextNodePageNo)
		nextINode := NewINodeByByte(*bufferBlock.Frame, sysTable.pool).(*INode)
		inodeList.AddINode(nextINode)
		nextNodePageNo = nextINode.NextNodePageNumber

	}

	return inodeList
}

//至少存在一个空闲Inode Entry的Inode Page被放到该链表上
func (sysTable *SysTableSpace) GetSegINodeFreeList() *INodeList {
	var inodeList = NewINodeList("FREE_LIST")

	bufferBlockINode := sysTable.pool.GetPageBlock(0, 2)

	currentINodeBytes := *bufferBlockINode.Frame
	inode := NewINodeByByte(currentINodeBytes, sysTable.pool).(*INode)

	nextNodePageNo := inode.NextNodePageNumber
	for {

		if nextNodePageNo == 0 {
			break
		}
		bufferBlock := sysTable.pool.GetPageBlock(0, nextNodePageNo)
		nextINode := NewINodeByByte(*bufferBlock.Frame, sysTable.pool).(*INode)
		inodeList.AddINode(nextINode)
		nextNodePageNo = nextINode.NextNodePageNumber

	}

	return inodeList
}

//获取所有FreeExtent链表
func (sysTable *SysTableSpace) GetFspFreeExtentList() *ExtentList {

	var extentList = NewExtentList("FREE_EXTENT")

	if sysTable.IsInit {

		//获取当前free的区链表
		freeInitNode := sysTable.Fsp.GetFspFreeExtentListInfo()
		//下一个区的首页面，它的属性应该是xdes类型
		//在FSP页面里面，页面号应该是0，
		//
		nextNodePageNo := freeInitNode.NextNodePageNumber
		//下一个offset,最大值256
		nextOffset := freeInitNode.NextNodeOffset

		nodeLength := freeInitNode.NodeInfoLength

		if nodeLength != 0 {
			//只处理当前256MB的数据,后面拓展
			for {
				//递归完成
				nextXDesEntryPage, _ := sysTable.LoadPageByPageNumber(nextNodePageNo)
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
						extent := NewOrdinaryExtentAtInit(0, uint32(currentPageNodeOffsetNo), xdesEntryWrapper, sysTable, true)

						startNo := uint32(currentPageNodeOffsetNo * 64)
						endNo := uint32((currentPageNodeOffsetNo + 1) * 64)

						//加载extent 到bufferPool
						var pageArrayWrapper = make([]uint32, 0)

						for i := startNo; i < endNo; i++ {
							pageArrayWrapper = append(pageArrayWrapper, i)
						}

						extent.LoadIPageWrapper(pageArrayWrapper)

						extentList.AddExtent(extent)

						nextNodePageNo = util.ReadUB4Byte2UInt32(nextXDESEntry[14:18])
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

	} else {
		bufferBlockFsp := sysTable.pool.GetPageBlock(0, 0)

		currentFspBytes := *bufferBlockFsp.Frame

		//获取当前free的区链表
		freeInitNode := NewFspByLoadBytes(currentFspBytes).(*Fsp).GetFspFreeExtentListInfo()
		//下一个区的首页面，它的属性应该是xdes类型
		//在FSP页面里面，页面号应该是0，
		//
		nextNodePageNo := freeInitNode.NextNodePageNumber
		//下一个offset,最大值256
		//应该是xdes类型的offset
		nextOffset := freeInitNode.NextNodeOffset

		//只处理当前256MB的数据,后面拓展
		for {

			//递归完成
			bufferBlockNextFsp := sysTable.pool.GetPageBlock(0, nextNodePageNo)
			nextXDesEntryPage := *bufferBlockNextFsp.Frame
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
					extent := NewOrdinaryExtent(0, uint32(currentPageNodeOffsetNo), xdesEntryWrapper, sysTable.pool)

					startNo := uint32(currentPageNodeOffsetNo * 64)
					endNo := uint32((currentPageNodeOffsetNo + 1) * 64)
					sysTable.pool.RangePageLoad(0, startNo, endNo)
					//加载extent 到bufferPool
					var pageArrayWrapper = make([]uint32, 0)

					for i := startNo; i < endNo; i++ {
						pageArrayWrapper = append(pageArrayWrapper, i)
					}

					extent.LoadIPageWrapper(pageArrayWrapper)

					extentList.AddExtent(extent)

					nextNodePageNo = util.ReadUB4Byte2UInt32(nextXDESEntry[14:18])
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

	return extentList
}

//获取所有的FreeFragExtent
func (sysTable *SysTableSpace) GetFspFreeFragExtentList() *ExtentList {
	var extentList = NewExtentList("FREE_FRAG")

	if !sysTable.IsInit {
		bufferBlockFsp := sysTable.pool.GetPageBlock(0, 0)

		currentFspBytes := *bufferBlockFsp.Frame

		//获取当前free的区链表
		freeInitNode := NewFspByLoadBytes(currentFspBytes).(*Fsp).GetFspFreeFragExtentListInfo()
		//下一个区的首页面，它的属性应该是xdes类型
		//在FSP页面里面，页面号应该是0，
		//
		nextNodePageNo := freeInitNode.NextNodePageNumber
		//下一个offset,最大值256
		//应该是xdes类型的offset
		nextOffset := freeInitNode.NextNodeOffset

		//只处理当前256MB的数据,后面拓展
		for {

			//递归完成
			bufferBlockNextFsp := sysTable.pool.GetPageBlock(0, nextNodePageNo)
			nextXDesEntryPage := *bufferBlockNextFsp.Frame
			nextXDESEntry := nextXDesEntryPage[nextOffset : nextOffset+40]

			//
			filePageTypeBytes := nextXDesEntryPage[24:26]
			filePageType := util.ReadUB2Byte2Int(filePageTypeBytes)
			var ipagewrapper IPageWrapper
			if filePageType == common.FILE_PAGE_TYPE_FSP_HDR || filePageType == common.FILE_PAGE_TYPE_XDES {
				//如果是free，则将该区加入到链表中
				if util.ReadUB4Byte2UInt32(nextXDESEntry[20:24]) == uint32(common.XDES_FREE_FRAG) {
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
					extent := NewOrdinaryExtent(0, uint32(currentPageNodeOffsetNo), xdesEntryWrapper, sysTable.pool)

					startNo := uint32(currentPageNodeOffsetNo * 64)
					endNo := uint32((currentPageNodeOffsetNo + 1) * 64)
					sysTable.pool.RangePageLoad(0, startNo, endNo)
					//加载extent 到bufferPool
					var pageArrayWrapper = make([]uint32, 0)

					for i := startNo; i < endNo; i++ {
						pageArrayWrapper = append(pageArrayWrapper, i)
					}

					extent.LoadIPageWrapper(pageArrayWrapper)

					extentList.AddExtent(extent)

					nextNodePageNo = util.ReadUB4Byte2UInt32(nextXDESEntry[14:18])
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

		//获取当前free的区链表
		freeInitNode := sysTable.Fsp.GetFspFreeFragExtentListInfo()
		//下一个区的首页面，它的属性应该是xdes类型
		//在FSP页面里面，页面号应该是0，
		//
		nextNodePageNo := freeInitNode.NextNodePageNumber
		//下一个offset,最大值256
		nextOffset := freeInitNode.NextNodeOffset

		nodeLength := freeInitNode.NodeInfoLength

		if nodeLength != 0 {
			//只处理当前256MB的数据,后面拓展
			for {
				//递归完成
				nextXDesEntryPage, err := sysTable.LoadPageByPageNumber(nextNodePageNo)
				if err != nil {
					fmt.Println(err)
				}
				nextXDESEntry := nextXDesEntryPage[nextOffset : nextOffset+40]
				//
				filePageTypeBytes := nextXDesEntryPage[24:26]
				filePageType := util.ReadUB2Byte2Int(filePageTypeBytes)
				var ipagewrapper IPageWrapper
				if filePageType == common.FILE_PAGE_TYPE_FSP_HDR || filePageType == common.FILE_PAGE_TYPE_XDES {
					//如果是free，则将该区加入到链表中
					if util.ReadUB4Byte2UInt32(nextXDESEntry[20:24]) == uint32(common.XDES_FREE_FRAG) {
						//计算区号
						//计算逻辑，根据偏移号，计算区号
						//获得当前区的相对位置
						currentPageNodeOffsetNo := (nextOffset - 150) / 40

						//	u := uint32(currentPageNodeOffsetNo) + nextNodePageNo
						var xdesEntryWrapper *XDESEntryWrapper
						var extent Extent
						if filePageType == common.FILE_PAGE_TYPE_FSP_HDR {
							ipagewrapper = NewFspByLoadBytes(nextXDesEntryPage).(*Fsp)
							//	fmt.Println(ipagewrapper)
							xdesEntryWrapper = ipagewrapper.(*Fsp).GetXDesEntryWrapper(int32(currentPageNodeOffsetNo))
						}
						if currentPageNodeOffsetNo == 0 {
							extent = NewPrimaryExtentWithPagesAtInit(0, uint32(currentPageNodeOffsetNo), xdesEntryWrapper, sysTable)
						} else {
							//加载普通区
							extent = NewOrdinaryExtentAtInit(0, uint32(currentPageNodeOffsetNo), xdesEntryWrapper, sysTable, true)
						}

						startNo := uint32(currentPageNodeOffsetNo * 64)
						endNo := uint32((currentPageNodeOffsetNo + 1) * 64)

						//加载extent 到bufferPool
						var pageArrayWrapper = make([]uint32, 0)

						for i := startNo; i < endNo; i++ {
							pageArrayWrapper = append(pageArrayWrapper, i)
						}

						extent.LoadIPageWrapper(pageArrayWrapper)

						extentList.AddExtent(extent)

						nextNodePageNo = util.ReadUB4Byte2UInt32(nextXDESEntry[14:18])
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

	}

	return extentList
}

//获取所有的FullFragExtent
//
func (sysTable *SysTableSpace) GetFspFullFragExtentList() *ExtentList {
	var extentList = NewExtentList("FULL_FRAG")
	bufferBlockFsp := sysTable.pool.GetPageBlock(0, 0)

	currentFspBytes := *bufferBlockFsp.Frame

	//获取当前free的区链表
	freeInitNode := NewFspByLoadBytes(currentFspBytes).(*Fsp).GetFspFullFragListInfo()
	//下一个区的首页面，它的属性应该是xdes类型
	//在FSP页面里面，页面号应该是0，
	//
	nextNodePageNo := freeInitNode.NextNodePageNumber
	//下一个offset,最大值256
	//应该是xdes类型的offset
	nextOffset := freeInitNode.NextNodeOffset

	//只处理当前256MB的数据,后面拓展
	for {

		//递归完成
		bufferBlockNextFsp := sysTable.pool.GetPageBlock(0, nextNodePageNo)
		nextXDesEntryPage := *bufferBlockNextFsp.Frame
		nextXDESEntry := nextXDesEntryPage[nextOffset : nextOffset+40]

		//
		filePageTypeBytes := nextXDesEntryPage[24:26]
		filePageType := util.ReadUB2Byte2Int(filePageTypeBytes)
		var ipagewrapper IPageWrapper
		if filePageType == common.FILE_PAGE_TYPE_FSP_HDR || filePageType == common.FILE_PAGE_TYPE_XDES {
			//如果是free，则将该区加入到链表中
			if util.ReadUB4Byte2UInt32(nextXDESEntry[20:24]) == uint32(common.XDES_FULL_FRAG) {
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
				extent := NewOrdinaryExtent(0, uint32(currentPageNodeOffsetNo), xdesEntryWrapper, sysTable.pool)

				startNo := uint32(currentPageNodeOffsetNo * 64)
				endNo := uint32((currentPageNodeOffsetNo + 1) * 64)
				sysTable.pool.RangePageLoad(0, startNo, endNo)
				//加载extent 到bufferPool
				var pageArrayWrapper = make([]uint32, 0)

				for i := startNo; i < endNo; i++ {
					pageArrayWrapper = append(pageArrayWrapper, i)
				}

				extent.LoadIPageWrapper(pageArrayWrapper)

				extentList.AddExtent(extent)

				nextNodePageNo = util.ReadUB4Byte2UInt32(nextXDESEntry[14:18])
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

	return extentList
}

func (sysTable *SysTableSpace) GetFirstFsp() *Fsp {
	if sysTable.IsInit {

		return sysTable.Fsp
	}
	bufferBlock := sysTable.pool.GetPageBlock(0, 0)
	fsp := NewFspByLoadBytes(*bufferBlock.Frame).(*Fsp)
	return fsp
}

func (sysTable *SysTableSpace) GetFirstINode() *INode {
	inodeBytes, _ := sysTable.LoadPageByPageNumber(2)
	sysTable.FirstInode = NewINodeByByteSpace(inodeBytes, sysTable).(*INode)
	return sysTable.FirstInode
}

func (sysTable *SysTableSpace) GetSpaceId() uint32 {
	return 0
}
