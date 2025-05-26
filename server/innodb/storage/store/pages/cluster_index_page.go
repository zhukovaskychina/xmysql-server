package pages

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/util"
)

/*
*

名称 			大小（单位：bit）		描述
预留位1 			1 					没有使用
预留位2 			1 					没有使用
delete_mask 	1 					标记该记录是否被删除
min_rec_mask 	1					B+树的每层非叶子节点中的最小记录都会添加该标记
n_owned 		4 					表示当前记录拥有的记录数
heap_no 		13		 			表示当前记录在记录堆的位置信息
record_type 	3 					表示当前记录的类型， 0 表示普通记录， 1 表示B+树非叶子节点记录， 2 表示最小记录， 3表示最大记录
next_record 	16 					表示下一条记录的相对位置

*
*/
var (
	INFIMUM_SURERMUM_COMPACT = []byte{
		/* the infimum record */
		0x00, 0x02, /* heap_no=0, REC_STATUS_INFIMUM */
		0x00,       /**record_type*/
		0x00, 0x0d, /* pointer to supremum */ //从页面0位置开始计算
		'i', 'n', 'f', 'i', 'm', 'u', 'm', 0,
		/* the supremum record */
		0x00, 0x0b, /* heap_no=1, REC_STATUS_SUPREMUM */
		0x00,       /***  record_type */
		0x00, 0x00, /* end of record list */
		's', 'u', 'p', 'r', 'e', 'm', 'u', 'm',
	}
)

type PageHeader struct {
	PageNDirSlots   []byte //2个字节 在页面中的槽的数量
	PageHeapTop     []byte //2  还未使用的空间最小地址，也就是该地址后就是FreeSpace
	PageNHeap       []byte //2 第一位表示本记录是否为紧凑的记录，剩余的15位表示本页中的堆中记录的数量（包括infimum和Supremum记录以及标记为已删除的记录）
	PageFree        []byte //2 各个删除的记录通过next_record组成一个单向链表，这个单向链表中的记录所占用的存储空间可以重新被利用;page_free表示该链表头节点对应记录在页面中的偏移量
	PageGarbage     []byte //2 已删除的记录占用的字节数
	PageLastInsert  []byte //2 最后插入记录的位置
	PageDirection   []byte //2  记录插入方向
	PageNDirections []byte //2 一个方向连续插入的记录数量
	PageNRecs       []byte //2 该页中用户记录的数量
	PageMaxTrxId    []byte //8 修改当前页面的最大事务Id,该值尽在二级索引页面中定义
	PageLevel       []byte //当前B+树所在位置  2
	PageIndexId     []byte //8当前页属于那个索引，8
	PageBtrSegLeaf  []byte //10 B+树叶子节点段的头部信息
	PageBtrSegTop   []byte //10 B+树非叶子节点端的头部信息
}

func NewPageHeader() *PageHeader {
	var pageHeader = new(PageHeader)
	pageHeader.WriteBtrSegLeaf(nil)
	pageHeader.WriteBtrSegTop(nil)
	pageHeader.WritePageDirections(0)
	pageHeader.WritePageFree(0)
	pageHeader.WritePageGarbage(0)
	pageHeader.WritePageLevel(0)
	pageHeader.WritePageMaxTrxId(0)
	pageHeader.WritePageNHeap(0)
	pageHeader.WritePageLastInsert(0)
	pageHeader.WritePageHeapTop(0)
	pageHeader.WritePageIndexId(0)
	pageHeader.WritePageNDirections(0)
	pageHeader.WritePageNRecs(0)
	pageHeader.WritePageHeaderNDirSlots(0)
	pageHeader.WriteBtrSegLeaf(util.AppendByte(10))
	pageHeader.WriteBtrSegTop(util.AppendByte(10))
	return pageHeader
}

func (ph *PageHeader) GetPageHeadeNDirSlots() int {
	return int(util.ReadUB2Byte2Int(ph.PageNDirSlots))
}

func (ph *PageHeader) WritePageHeaderNDirSlots(pageNdir uint16) {
	ph.PageNDirSlots = util.ConvertUInt2Bytes(pageNdir)
}
func (ph *PageHeader) WritePageHeapTop(pageHeapTop int) {
	ph.PageHeapTop = util.ConvertInt2Bytes(int32(pageHeapTop))
}
func (ph *PageHeader) WritePageNHeap(pageNHeap int) {
	ph.PageNHeap = util.ConvertInt2Bytes(int32(pageNHeap))
}
func (ph *PageHeader) WritePageFree(pageFree int) {
	ph.PageFree = util.ConvertInt2Bytes(int32(pageFree))
}
func (ph *PageHeader) WritePageGarbage(pageGarbage int) {
	ph.PageGarbage = util.ConvertInt2Bytes(int32(pageGarbage))
}
func (ph *PageHeader) WritePageLastInsert(pageLastInsert int) {
	ph.PageLastInsert = util.ConvertInt2Bytes(int32(pageLastInsert))
}
func (ph *PageHeader) WritePageDirections(pageDirection int) {
	ph.PageDirection = util.ConvertInt2Bytes(int32(pageDirection))
}
func (ph *PageHeader) WritePageNDirections(pageNDirections int) {
	ph.PageNDirections = util.ConvertInt2Bytes(int32(pageNDirections))
}
func (ph *PageHeader) WritePageNRecs(pageNRecs int) {
	ph.PageNRecs = util.ConvertInt2Bytes(int32(pageNRecs))
}
func (ph *PageHeader) WritePageMaxTrxId(pageMaxTrxId int64) {
	ph.PageMaxTrxId = util.ConvertLong8Bytes(pageMaxTrxId)
}
func (ph *PageHeader) WritePageLevel(pageLevel int) {
	ph.PageLevel = util.ConvertInt2Bytes(int32(pageLevel))
}
func (ph *PageHeader) WritePageIndexId(pageIndexId int64) {
	ph.PageIndexId = util.ConvertLong8Bytes(pageIndexId)
}
func (ph *PageHeader) WriteBtrSegLeaf(buff []byte) {
	ph.PageBtrSegLeaf = buff
}
func (ph *PageHeader) WriteBtrSegTop(buff []byte) {
	ph.PageBtrSegTop = buff
}
func (ph *PageHeader) SerializeBytes() []byte {
	var buff = make([]byte, 0)

	buff = append(buff, ph.PageNDirSlots...)
	buff = append(buff, ph.PageHeapTop...)
	buff = append(buff, ph.PageNHeap...)
	buff = append(buff, ph.PageFree...)
	buff = append(buff, ph.PageGarbage...)
	buff = append(buff, ph.PageLastInsert...)
	buff = append(buff, ph.PageDirection...)
	buff = append(buff, ph.PageNDirections...)
	buff = append(buff, ph.PageNRecs...)
	buff = append(buff, ph.PageMaxTrxId...)
	buff = append(buff, ph.PageLevel...)
	buff = append(buff, ph.PageIndexId...)
	buff = append(buff, ph.PageBtrSegLeaf...)
	buff = append(buff, ph.PageBtrSegTop...)
	return buff
}

//页面的分裂条件
//下一个数据页总用户记录的主键值必须大于上一个页中用户记录的主键值

func (ph *PageHeader) parsePageHeader(buff []byte) {
	ph.PageNDirSlots = buff[0:2] //在页面目录中的槽数量
	ph.PageHeapTop = buff[2:4]   //还未使用的空间最小地址，也就是说从地址之后是FreeSpace
	ph.PageNHeap = buff[4:6]     //第一位表示本记录是否紧凑型的记录，剩余的是15位表示本页中记录的数量
	ph.PageFree = buff[6:8]      //各个已删除的记录通过next_record组成一个单项链表，这个单项链表的记录所占用的存储空间可以被重新利用；PageFree 表示
	//该链表头节点对应记录在页面中的偏移量
	ph.PageGarbage = buff[8:10]      //已经删除的记录占用的字节数
	ph.PageLastInsert = buff[10:12]  //最后插入记录的位置
	ph.PageDirection = buff[12:14]   //记录插入的方向
	ph.PageNDirections = buff[14:16] //一个方向连续插入的记录数量
	ph.PageNRecs = buff[16:18]       //该页面中用户记录的数量（不包括Infimum和Supremum 记录以及被删除的记录）
	ph.PageMaxTrxId = buff[18:26]    //修改当前页面的最大事务Id，该值尽在二级索引页面中定义
	ph.PageLevel = buff[26:28]       //当前页在B+树中所处的层级
	ph.PageIndexId = buff[28:36]     //索引ID，表示当前页属于哪个索引
	ph.PageBtrSegLeaf = buff[36:46]  //B+树中叶子节点端的头部信息，尽在B+树中的跟页面中定义
	ph.PageBtrSegTop = buff[46:56]   //B+树中非叶子节点端的头部信息，尽在B+树中的跟页面中定义
}

// 用于实现存储主键索引的数据
type IndexPage struct {
	AbstractPage
	PageHeader      PageHeader
	InfimumSupermum []byte //最大最下 26个字节
	UserRecords     []byte //用户记录
	FreeSpace       []byte //剩余空间
	PageDirectory   []byte //页目录    //页从page尾部插入，每一个页号是当前记录便宜量，从最后开始算起
}

func NewIndexPage(pageNo uint32, spaceId uint32) *IndexPage {
	var fileHeader = NewFileHeader()
	fileHeader.WritePageLSN(0)
	fileHeader.WritePageOffset(pageNo)
	fileHeader.WritePageFileType(int16(common.FILE_PAGE_INDEX))
	fileHeader.WritePageArch(spaceId)
	fileHeader.WritePageSpaceCheckSum(nil)
	fileHeader.WritePageNext(0)
	fileHeader.WritePagePrev(0)
	fileHeader.WritePageFileFlushLSN(0)
	var pageHeader = NewPageHeader()
	var fileTrailer = NewFileTrailer()

	return &IndexPage{
		AbstractPage: AbstractPage{
			FileHeader:  fileHeader,
			FileTrailer: fileTrailer,
		},
		PageHeader:      *pageHeader,
		InfimumSupermum: INFIMUM_SURERMUM_COMPACT,
		UserRecords:     util.AppendByte(0),
		FreeSpace:       util.AppendByte(16384 - 38 - 8 - 26 - 56),
		PageDirectory:   util.AppendByte(0),
	}
}
func NewIndexPageWithParams(pageNo uint32, spaceId uint32) IndexPage {
	var fileHeader = NewFileHeader()
	fileHeader.WritePageLSN(0)
	fileHeader.WritePageOffset(pageNo)
	fileHeader.WritePageFileType(int16(common.FILE_PAGE_INDEX))
	fileHeader.WritePageArch(spaceId)
	return IndexPage{
		AbstractPage:    AbstractPage{},
		PageHeader:      PageHeader{},
		InfimumSupermum: INFIMUM_SURERMUM_COMPACT,
		UserRecords:     util.AppendByte(0),
		FreeSpace:       util.AppendByte(16384 - 38 - 8 - 26 - 56),
		PageDirectory:   util.AppendByte(0),
	}
}

func (ip *IndexPage) ParsePageHeader(content []byte) {
	ip.PageHeader.PageNDirSlots = content[0:2]
	ip.PageHeader.PageHeapTop = content[2:4]
	ip.PageHeader.PageNHeap = content[4:6]
	ip.PageHeader.PageFree = content[6:8]
	ip.PageHeader.PageGarbage = content[8:10]
	ip.PageHeader.PageLastInsert = content[10:12]
	ip.PageHeader.PageDirection = content[12:14]
	ip.PageHeader.PageNDirections = content[14:16]
	ip.PageHeader.PageNRecs = content[16:18]
	ip.PageHeader.PageMaxTrxId = content[18:26]
	ip.PageHeader.PageLevel = content[26:28]
	ip.PageHeader.PageIndexId = content[28:36]
	ip.PageHeader.PageBtrSegLeaf = content[36:46]
	ip.PageHeader.PageBtrSegTop = content[46:56]
}

func (ip *IndexPage) ParseInfimumSupermum(content []byte) {
	ip.InfimumSupermum = content[0:26]
}

func (ip *IndexPage) ParsePageSlots(content []byte) {
	pnd := ip.PageHeader.PageNDirSlots
	_, pncnt := util.ReadUB2(pnd, 0)
	ip.PageDirectory = content[16384-8-pncnt*2 : 16384-8]

}

// infimum最小槽位
// supermum最大槽位
// 其他槽位每一个槽位数量为4-8个，不得超过8个记录
// 槽位的最大是最大值的槽位
// 最大槽位的最后一个值为supermum
// pageDirectory 应该是slotrows数组的大小
func (ip *IndexPage) ParseUserRecordsAndFreeSpace(content []byte) {
	pageDirLength := len(ip.PageDirectory)

	if pageDirLength == 0 {
		ip.UserRecords = make([]byte, 0)
		ip.FreeSpace = content[38+56+26 : 16384-8-len(ip.PageDirectory)]
		return
	}
	//计算最大值偏移量
	//
	supremOffset := ip.PageDirectory[pageDirLength-2:]

	supermumOffsetValue := util.ReadUB2Byte2Int(supremOffset)

	ip.UserRecords = content[38+56+26 : supermumOffsetValue+5+8]

	ip.FreeSpace = content[38+56+26+supermumOffsetValue+5+8 : 16384-8-len(ip.PageDirectory)]
}

func (ip *IndexPage) SerializeBytes() []byte {
	var buff = make([]byte, 0)
	buff = append(buff, ip.FileHeader.GetSerialBytes()...)
	buff = append(buff, ip.PageHeader.SerializeBytes()...)
	buff = append(buff, ip.InfimumSupermum...)
	buff = append(buff, ip.UserRecords...)
	buff = append(buff, ip.FreeSpace...)
	buff = append(buff, ip.PageDirectory...)
	buff = append(buff, ip.FileTrailer.FileTrailer[:]...)
	return buff
}

func (ip *IndexPage) GetSerializeBytes() []byte {
	return ip.SerializeBytes()
}
