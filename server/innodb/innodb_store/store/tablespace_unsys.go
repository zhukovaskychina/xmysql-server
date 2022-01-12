package store

import (
	"path"
	"xmysql-server/server/common"
	"xmysql-server/server/conf"
	"xmysql-server/server/innodb/buffer_pool"
	"xmysql-server/server/innodb/innodb_store/store/storebytes/blocks"
	"xmysql-server/server/innodb/innodb_store/store/storebytes/pages"
	"xmysql-server/util"
)

//分区表实际是由多个Tablespace组成的，每个Tablespace有独立的”.ibd”文件和Space_id，
//其中”.ibd”文件的名字会以分区名加以区分，但给用户返回的是一个统一的逻辑表。
//初始化表空间的rootPage

//FSP_SIZE：表空间大小，以Page数量计算
//FSP_FREE_LIMIT：目前在空闲的Extent上最小的尚未被初始化的Page的`Page Number
//FSP_FREE：空闲extent链表，链表中的每一项为代表extent的xdes，所谓空闲extent是指该extent内所有page均未被使用
//FSP_FREE_FRAG：free frag extent链表，链表中的每一项为代表extent的xdes，所谓free frag extent是指该extent内有部分page未被使用
//FSP_FULL_FRAG：full frag extent链表，链表中的每一项为代表extent的xdes，所谓full frag extent是指该extent内所有Page均已被使用
//FSP_SEG_ID：下次待分配的segment id，每次分配新segment时均会使用该字段作为segment id，并将该字段值+1写回
//FSP_SEG_INODES_FULL：full inode page链表，链表中的每一项为inode page，该链表中的每个inode page内的inode entry都已经被使用
//FSP_SEG_INODES_FREE：free inode page链表，链表中的每一项为inode page，该链表中的每个inode page内上有空闲inode entry可分配

//Innodb的逻辑存储形式，表空间
//表空间下面分为段，区，page
//每个索引两个段，叶子段和非叶子段
//回滚段
//每个表文件都对应一个表空间
//非系统表文件加载段的时候，需要从SYS_INDEXES 中查找并且加载出来
type UnSysTableSpace struct {
	TableSpace
	conf         *conf.Cfg
	tableName    string
	spaceId      uint32
	dataBaseName string
	isSys        bool
	filePath     string
	blockFile    *blocks.BlockFile

	Fsp        *Fsp   //0 号页面
	IBuf       *IBuf  //1 号页面
	FirstInode *INode //2 号页面

	InodeMap map[uint32]*INode

	FirstIBDExtent *PrimaryExtent //第一个区

	BtreeMaps map[string]*BTree //每个Btree中有两个段，索引段之叶子段和非叶子段

	tableMeta *TableTupleMeta //表元祖信息

	pool *buffer_pool.BufferPool
}

/***
FSP HEADER PAGE

FSP header page是表空间的root page，存储表空间关键元数据信息。由page file header、fsp header、xdes entries三大部分构成

**/
func NewTableSpaceFile(cfg *conf.Cfg, databaseName string, tableName string, spaceId uint32, isSys bool, pool *buffer_pool.BufferPool) TableSpace {
	tableSpace := new(UnSysTableSpace)
	filePath := path.Join(cfg.DataDir, "/", databaseName)
	isFlag, _ := util.PathExists(filePath)
	if !isFlag {
		util.CreateDataBaseDir(cfg.DataDir, databaseName)
	}
	tableName = tableName + ".ibd"
	blockfile := blocks.NewBlockFile(filePath, tableName, 256*64*16384)
	tableSpace.blockFile = blockfile
	tableSpace.pool = pool
	//pool.FileSystem.AddTableSpace(tableSpace)
	tableSpace.spaceId = spaceId
	tableSpace.conf = cfg
	tableSpace.isSys = isSys
	tableSpace.tableName = tableName
	tableSpace.initHeadPage()

	//tableSpace.loadInodePage()
	return tableSpace
}

// TODO 需要完成头文件部分的初始化工作
/**
初始化创建表文件的时候，首先会创建fsp,ibuf,inode
当重启加载的时候，可以
*/
func (tableSpace *UnSysTableSpace) initHeadPage() {

	//如果存在该文件，则是加载前3个页面
	if tableSpace.blockFile.Exists() {
		tableSpace.blockFile.OpenFile()
		//	fsp, err := tableSpace.blockFile.ReadPageByNumber(0)
		//	ibuf, err := tableSpace.blockFile.ReadPageByNumber(1)
		////	inode, err := tableSpace.blockFile.ReadPageByNumber(2)
		//	if err != nil {
		//		panic("err")
		//	}
		//	tableSpace.Fsp = NewFspByLoadBytes(fsp).(*Fsp)
		//	tableSpace.IBuf = NewIBufByLoadBytes(ibuf)
		////	tableSpace.FirstInode = NewINodeByByte(inode)

		//此处需要初始化段，段目前暂时做索引段之叶子段，索引段和非叶子段

		//读取当前inode里面的段

	} else {
		tableSpace.blockFile.CreateFile()
		//初始化FspHrdPage
		//初始化BitMapPage
		ibuBufMapPage := pages.NewIBufBitMapPage(tableSpace.spaceId)
		//初始化INodePage
		//	iNodePage := pages.NewINodePage(tableSpace.spaceId)
		tableSpace.Fsp = NewFspInitialize(tableSpace.spaceId).(*Fsp)
		tableSpace.Fsp.SetFspSize(64 * 256)
		tableSpace.Fsp.SetFspFreeExtentListInfo(&CommonNodeInfo{
			NodeInfoLength:     256,
			PreNodePageNumber:  0,
			PreNodeOffset:      0,
			NextNodePageNumber: 0, //下一个区，空闲区
			NextNodeOffset:     190,
		})
		tableSpace.Fsp.SetFspFreeFragExtentListInfo(&CommonNodeInfo{
			NodeInfoLength:     1,
			PreNodePageNumber:  0,
			PreNodeOffset:      0,
			NextNodePageNumber: 0,
			NextNodeOffset:     150,
		})
		tableSpace.Fsp.SetFullFragExtentListInfo(&CommonNodeInfo{
			NodeInfoLength:     0,
			PreNodePageNumber:  0,
			PreNodeOffset:      0,
			NextNodePageNumber: 0,
			NextNodeOffset:     0,
		})
		//第一个区的情况设置
		var firstXdesEntryWrapper = NewXDesEntryWrapper(
			0,
			0,
			0,
			0,
			0, tableSpace.Fsp)
		//第一个xdesentry不隶属于任何段
		firstXdesEntryWrapper.DescPage(0, false)
		firstXdesEntryWrapper.DescPage(1, false)
		firstXdesEntryWrapper.DescPage(2, false)
		firstXdesEntryWrapper.SetDesState(common.XDES_FREE_FRAG)

		tableSpace.Fsp.SetXDesEntryInfo(0, firstXdesEntryWrapper)
		tableSpace.IBuf = NewIBuf(tableSpace.spaceId)
		tableSpace.FirstInode = NewINode(tableSpace.spaceId, 2).(*INode)
		tableSpace.FirstInode.NextNodePageNumber = 0
		tableSpace.FirstInode.PreNodeOffset = 0
		tableSpace.blockFile.WriteContentByPage(0, tableSpace.Fsp.GetSerializeBytes())
		tableSpace.blockFile.WriteContentByPage(1, ibuBufMapPage.GetSerializeBytes())
		tableSpace.blockFile.WriteContentByPage(2, tableSpace.FirstInode.GetSerializeBytes())
	}
}

func (tableSpace *UnSysTableSpace) LoadPageByPageNumber(pageNumber uint32) ([]byte, error) {
	return tableSpace.blockFile.ReadPageByNumber(pageNumber)
}

//获取INodeList
func (tableSpace *UnSysTableSpace) GetSegINodeFullList() *INodeList {
	var inodeList = NewINodeList("FULL_LIST")

	bufferBlockINode := tableSpace.pool.GetPageBlock(tableSpace.spaceId, 2)

	currentINodeBytes := *bufferBlockINode.Frame
	inode := NewINodeByByte(currentINodeBytes, tableSpace.pool).(*INode)

	nextNodePageNo := inode.NextNodePageNumber
	for {

		if nextNodePageNo == 0 {
			break
		}
		bufferBlock := tableSpace.pool.GetPageBlock(tableSpace.spaceId, nextNodePageNo)
		nextINode := NewINodeByByte(*bufferBlock.Frame, tableSpace.pool).(*INode)
		inodeList.AddINode(nextINode)
		nextNodePageNo = nextINode.NextNodePageNumber

	}
	return inodeList
}

func (tableSpace *UnSysTableSpace) GetSegINodeFreeList() *INodeList {
	var inodeList = NewINodeList("FREE_LIST")

	bufferBlockINode := tableSpace.pool.GetPageBlock(tableSpace.spaceId, 2)

	currentINodeBytes := *bufferBlockINode.Frame
	inode := NewINodeByByte(currentINodeBytes, tableSpace.pool).(*INode)

	nextNodePageNo := inode.NextNodePageNumber
	for {

		if nextNodePageNo == 0 {
			break
		}
		bufferBlock := tableSpace.pool.GetPageBlock(tableSpace.spaceId, nextNodePageNo)
		nextINode := NewINodeByByte(*bufferBlock.Frame, tableSpace.pool).(*INode)
		inodeList.AddINode(nextINode)
		nextNodePageNo = nextINode.NextNodePageNumber

	}

	return inodeList

}

//TODO 需要解决FSP和XDES之间的衔接问题
func (tableSpace *UnSysTableSpace) GetFspFreeExtentList() *ExtentList {
	var extentList = NewExtentList("FREE_EXTENT")

	bufferBlockFsp := tableSpace.pool.GetPageBlock(tableSpace.spaceId, 0)

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

	infoLength := freeInitNode.NodeInfoLength
	if infoLength != 0 {
		//只处理当前256MB的数据,后面拓展
		for {

			//递归完成
			bufferBlockNextFsp := tableSpace.pool.GetPageBlock(tableSpace.spaceId, nextNodePageNo)
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
					startNo := uint32(currentPageNodeOffsetNo * 64)
					endNo := uint32((currentPageNodeOffsetNo + 1) * 64)
					tableSpace.pool.RangePageLoad(tableSpace.spaceId, startNo, endNo)
					//加载extent 到bufferPool
					var pageArrayWrapper = make([]uint32, 0)

					for i := startNo; i < endNo; i++ {
						pageArrayWrapper = append(pageArrayWrapper, i)
					}
					var extent Extent
					//	u := uint32(currentPageNodeOffsetNo) + nextNodePageNo
					var xdesEntryWrapper *XDESEntryWrapper

					if filePageType == common.FILE_PAGE_TYPE_FSP_HDR {
						ipagewrapper = NewFspByLoadBytes(nextXDesEntryPage).(*Fsp)
						//	fmt.Println(ipagewrapper)
						xdesEntryWrapper = ipagewrapper.(*Fsp).GetXDesEntryWrapper(int32(currentPageNodeOffsetNo))

					} else if filePageType == common.FILE_PAGE_TYPE_XDES {

					}

					{
						//如果是256的倍数
						if nextNodePageNo&255 == 0 {
							if nextNodePageNo == 0 {
								//加载Primary
								extent = NewPrimaryExtentWithPages(tableSpace.spaceId, 0, xdesEntryWrapper, tableSpace.pool)
							} else {
								//加载secondary
							}
						} else {
							//加载普通区
							extent = NewOrdinaryExtent(tableSpace.spaceId, uint32(currentPageNodeOffsetNo), xdesEntryWrapper, tableSpace.pool)
						}
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

func (tableSpace *UnSysTableSpace) GetFspFreeFragExtentList() *ExtentList {
	var extentList = NewExtentList("FREE_FRAG")
	//获取当前free的区链表

	bufferBlockFsp := tableSpace.pool.GetPageBlock(tableSpace.spaceId, 0)

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

	infoLength := freeInitNode.NodeInfoLength

	if infoLength != 0 {
		//只处理当前256MB的数据,后面拓展
		for {

			//递归完成
			bufferBlockNextFsp := tableSpace.pool.GetPageBlock(tableSpace.spaceId, nextNodePageNo)
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
					startNo := uint32(currentPageNodeOffsetNo * 64)
					endNo := uint32((currentPageNodeOffsetNo + 1) * 64)
					tableSpace.pool.RangePageLoad(tableSpace.spaceId, startNo, endNo)
					//加载extent 到bufferPool
					var pageArrayWrapper = make([]uint32, 0)

					for i := startNo; i < endNo; i++ {
						pageArrayWrapper = append(pageArrayWrapper, i)
					}
					var extent Extent
					//	u := uint32(currentPageNodeOffsetNo) + nextNodePageNo
					var xdesEntryWrapper *XDESEntryWrapper

					if filePageType == common.FILE_PAGE_TYPE_FSP_HDR {
						ipagewrapper = NewFspByLoadBytes(nextXDesEntryPage).(*Fsp)
						//	fmt.Println(ipagewrapper)
						xdesEntryWrapper = ipagewrapper.(*Fsp).GetXDesEntryWrapper(int32(currentPageNodeOffsetNo))

					} else if filePageType == common.FILE_PAGE_TYPE_XDES {

					}

					{
						//如果是256的倍数
						if nextNodePageNo&255 == 0 {
							if nextNodePageNo == 0 {
								//加载Primary
								extent = NewPrimaryExtentWithPages(tableSpace.spaceId, 0, xdesEntryWrapper, tableSpace.pool)
							} else {
								//加载secondary
							}
						} else {
							//加载普通区
							extent = NewOrdinaryExtent(tableSpace.spaceId, uint32(currentPageNodeOffsetNo), xdesEntryWrapper, tableSpace.pool)
						}
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

func (tableSpace *UnSysTableSpace) GetFspFullFragExtentList() *ExtentList {
	var extentList = NewExtentList("FULL_FRAG")

	bufferBlockFsp := tableSpace.pool.GetPageBlock(tableSpace.spaceId, 0)

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

	infoLength := freeInitNode.NodeInfoLength

	if infoLength != 0 {
		//只处理当前256MB的数据,后面拓展
		for {

			//递归完成
			bufferBlockNextFsp := tableSpace.pool.GetPageBlock(tableSpace.spaceId, nextNodePageNo)
			nextXDesEntryPage := *bufferBlockNextFsp.Frame
			nextXDESEntry := nextXDesEntryPage[nextOffset : nextOffset+40]

			//
			filePageTypeBytes := nextXDesEntryPage[24:26]
			filePageType := util.ReadUB2Byte2Int(filePageTypeBytes)
			var ipagewrapper IPageWrapper
			if filePageType == common.FILE_PAGE_TYPE_FSP_HDR || filePageType == common.FILE_PAGE_TYPE_XDES {
				//如果是free，则将该区加入到链表中
				if util.ReadUB4Byte2UInt32(nextXDESEntry[20:24]) == uint32(common.XDES_FULL_FRAG) || util.ReadUB4Byte2UInt32(nextXDESEntry[20:24]) == uint32(common.XDES_FSEG) {
					//计算区号
					//计算逻辑，根据偏移号，计算区号
					//获得当前区的相对位置
					currentPageNodeOffsetNo := (nextOffset - 150) / 40
					startNo := uint32(currentPageNodeOffsetNo * 64)
					endNo := uint32((currentPageNodeOffsetNo + 1) * 64)
					tableSpace.pool.RangePageLoad(tableSpace.spaceId, startNo, endNo)
					//加载extent 到bufferPool
					var pageArrayWrapper = make([]uint32, 0)

					for i := startNo; i < endNo; i++ {
						pageArrayWrapper = append(pageArrayWrapper, i)
					}
					var extent Extent
					//	u := uint32(currentPageNodeOffsetNo) + nextNodePageNo
					var xdesEntryWrapper *XDESEntryWrapper

					if filePageType == common.FILE_PAGE_TYPE_FSP_HDR {
						ipagewrapper = NewFspByLoadBytes(nextXDesEntryPage).(*Fsp)
						//	fmt.Println(ipagewrapper)
						xdesEntryWrapper = ipagewrapper.(*Fsp).GetXDesEntryWrapper(int32(currentPageNodeOffsetNo))

					} else if filePageType == common.FILE_PAGE_TYPE_XDES {

					}

					{
						//如果是256的倍数
						if nextNodePageNo&255 == 0 {
							if nextNodePageNo == 0 {
								//加载Primary
								extent = NewPrimaryExtentWithPages(tableSpace.spaceId, 0, xdesEntryWrapper, tableSpace.pool)
							} else {
								//加载secondary
							}
						} else {
							//加载普通区
							extent = NewOrdinaryExtent(tableSpace.spaceId, uint32(currentPageNodeOffsetNo), xdesEntryWrapper, tableSpace.pool)
						}
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

func (tableSpace *UnSysTableSpace) GetFirstFsp() *Fsp {
	return tableSpace.Fsp
}

func (tableSpace *UnSysTableSpace) GetFirstINode() *INode {
	return tableSpace.FirstInode
}

func (tableSpace *UnSysTableSpace) GetSpaceId() uint32 {
	return tableSpace.spaceId
}

func (tableSpace *UnSysTableSpace) LoadExtentFromDisk(extentNumber int) Extent {
	panic("implement me")
}

func (tableSpace *UnSysTableSpace) FlushToDisk(pageNo uint32, content []byte) {
	tableSpace.blockFile.WriteContentByPage(int64(pageNo), content)
}
