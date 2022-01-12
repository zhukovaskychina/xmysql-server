package pages

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/util"
)

type FileSpaceHeader struct {
	SpaceId   []byte //4 表空间ID
	NotUsed   []byte // 4 未被使用
	Size      []byte // 4当前表空间的页面数
	FreeLimit []byte // 4 尚未被初始化的最小页号，大于或者等于这个页号的对应的XDES_ENTRY结构都没有被加入FreeList，
	// 当前尚未初始化的最小Page No。从该Page往后的都尚未加入到表空间的FREE LIST上。
	SpaceFlags              []byte // 4 表空间的一些占用存储空间比较小的属性
	FragNUsed               []byte // 4 FREE_FRAG 链表中已使用的页面数量
	BaseNodeForFreeList     []byte // 16 FREE 链表的基节点
	BaseNodeForFragFreeList []byte // 16 FULL_FRAG	链表
	BaseNodeForFullFragList []byte // 16 FREE_FRAG 链表
	NextUnusedSegmentId     []byte // 8 当前表空间中的下一个未使用的SegmentId
	SegFullINodesList       []byte // 16 SEG_INODES_FULL链表的基节点
	SegFreeINodesList       []byte // 16 SEG_INODES_FREE链表的基节点

}

func NewFileSpaceHeader(spaceId uint32) *FileSpaceHeader {

	var fileSpaceHeader = new(FileSpaceHeader)
	fileSpaceHeader.SpaceId = util.ConvertUInt4Bytes(uint32(spaceId))
	fileSpaceHeader.NotUsed = []byte{0, 0, 0, 0}
	fileSpaceHeader.Size = util.ConvertInt4Bytes(0)
	fileSpaceHeader.FreeLimit = util.ConvertInt4Bytes(0)
	fileSpaceHeader.SpaceFlags = util.ConvertInt4Bytes(0)
	fileSpaceHeader.FragNUsed = util.ConvertUInt4Bytes(0)
	fileSpaceHeader.BaseNodeForFreeList = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	fileSpaceHeader.BaseNodeForFragFreeList = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	fileSpaceHeader.BaseNodeForFullFragList = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	fileSpaceHeader.NextUnusedSegmentId = []byte{0, 0, 0, 0, 0, 1, 0, 0}
	fileSpaceHeader.SegFreeINodesList = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	fileSpaceHeader.SegFullINodesList = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	return fileSpaceHeader
}

func (fsh *FileSpaceHeader) GetSerializeBytes() []byte {

	var buff = make([]byte, 0)

	buff = append(buff, fsh.SpaceId...)
	buff = append(buff, fsh.NotUsed...)
	buff = append(buff, fsh.Size...)
	buff = append(buff, fsh.FreeLimit...)
	buff = append(buff, fsh.SpaceFlags...)
	buff = append(buff, fsh.FragNUsed...)
	buff = append(buff, fsh.BaseNodeForFreeList...)

	buff = append(buff, fsh.BaseNodeForFragFreeList...)
	buff = append(buff, fsh.BaseNodeForFullFragList...)

	buff = append(buff, fsh.NextUnusedSegmentId...)
	buff = append(buff, fsh.SegFreeINodesList...)
	buff = append(buff, fsh.SegFullINodesList...)
	return buff
}

//获取文件页面数
func (f *FileSpaceHeader) GetFilePages() uint32 {

	return util.ReadUB4Byte2UInt32(f.Size)
}

func (f *FileSpaceHeader) WriteFilePage(pageSize uint32) {

}

type SpaceFlags struct {
	IsPostAntelope bool
	ZipSSzie       byte //4个bit位数8+4+2+1=15
	AtomicBlobs    bool
	PageSSize      byte //4个bit
	DataDir        bool
	Shared         bool //是否为共享表空间
	Temporary      bool //是否为临时表空间
	Encryption     bool //表空间是否加密
}

/****

SpaceFlags

PostAntelope 1   表示文件格式是否在Antelope格式之后
ZipSSize 4	表示压缩页面
AtomicBlobs 1 表示是否自动把占用存储空间非常多的字段放到溢出页中
PageSSize 4 页面大小
DataDir 1 表示表空间是否从数据目录中获取的
Shared 1 是否为共享表空间
Temporary 1 是否为临时表空间
Encrytion 1 表空间是否加密
Unused 18 没有使用到的bit

***/
//用来记录整个表空间的一些整体属性以及本组所有的区间，（也就是extent0-extent255个区间）
type FspHrdBinaryPage struct {
	AbstractPage
	FileSpaceHeader *FileSpaceHeader //112字节
	XDESEntrys      []XDESEntry      //10240 byte 存储本组256组区对应的属性信息，每组40byte
	EmptySpace      []byte           //5986 byte

}

//创建Fsp
//也就是RootPage
//
//FSP_SIZE：表空间大小，以Page数量计算
//FSP_FREE_LIMIT：目前在空闲的Extent上最小的尚未被初始化的Page的`Page Number
//FSP_FREE：空闲extent链表，链表中的每一项为代表extent的xdes，所谓空闲extent是指该extent内所有page均未被使用
//FSP_FREE_FRAG：free frag extent链表，链表中的每一项为代表extent的xdes，所谓free frag extent是指该extent内有部分page未被使用
//FSP_FULL_FRAG：full frag extent链表，链表中的每一项为代表extent的xdes，所谓full frag extent是指该extent内所有Page均已被使用
//FSP_SEG_ID：下次待分配的segment id，每次分配新segment时均会使用该字段作为segment id，并将该字段值+1写回
//FSP_SEG_INODES_FULL：full inode page链表，链表中的每一项为inode page，该链表中的每个inode page内的inode entry都已经被使用
//FSP_SEG_INODES_FREE：free inode page链表，链表中的每一项为inode page，该链表中的每个inode page内上有空闲inode entry可分配
func NewFspHrdPage(spaceId uint32) *FspHrdBinaryPage {
	var fileHeader = new(FileHeader)
	//写入FSP文件头
	fileHeader.FilePageType = util.ConvertInt2Bytes(common.FILE_PAGE_TYPE_FSP_HDR)
	fileHeader.FilePagePrev = util.ConvertInt4Bytes(0)
	fileHeader.FilePageOffset = util.ConvertUInt4Bytes(uint32(0))
	fileHeader.WritePageOffset(0)
	fileHeader.WritePagePrev(0)
	fileHeader.WritePageFileType(common.FILE_PAGE_TYPE_FSP_HDR)
	fileHeader.WritePageNext(1)
	fileHeader.WritePageLSN(0)
	fileHeader.WritePageFileFlushLSN(0)
	fileHeader.WritePageArch(0)
	fileHeader.WritePageSpaceCheckSum(nil)
	var fileSpaceHeader = NewFileSpaceHeader(spaceId)
	var fspHrdBinaryPage = new(FspHrdBinaryPage)
	fspHrdBinaryPage.FileHeader = *fileHeader
	fspHrdBinaryPage.FileSpaceHeader = fileSpaceHeader
	fspHrdBinaryPage.EmptySpace = util.AppendByte(5986)
	fspHrdBinaryPage.FileTrailer = NewFileTrailer()

	fspHrdBinaryPage.XDESEntrys = appendXDesEntry()

	return fspHrdBinaryPage
}

func appendXDesEntry() []XDESEntry {
	var xdesEntries = make([]XDESEntry, 0)
	for i := 0; i < 256; i++ {
		xdesEntry := NewXdesEntry()

		xdesEntries = append(xdesEntries, xdesEntry)
	}
	return xdesEntries
}

func (fspHrdPage *FspHrdBinaryPage) SerializeBytes() []byte {
	var buff = make([]byte, 0)
	buff = append(buff, fspHrdPage.FileHeader.GetSerialBytes()...)
	buff = append(buff, fspHrdPage.FileSpaceHeader.GetSerializeBytes()...)
	for _, v := range fspHrdPage.XDESEntrys {
		buff = append(buff, v.GetSerializeByte()...)
	}
	buff = append(buff, fspHrdPage.EmptySpace...)
	buff = append(buff, fspHrdPage.FileTrailer.FileTrailer...)
	return buff
}

func (fspHrdPage *FspHrdBinaryPage) GetSerializeBytes() []byte {
	return fspHrdPage.SerializeBytes()
}

//获取下一个ID
func (fspHrdPage *FspHrdBinaryPage) GetNextSegmentId() []byte {

	return fspHrdPage.FileSpaceHeader.NextUnusedSegmentId
}
