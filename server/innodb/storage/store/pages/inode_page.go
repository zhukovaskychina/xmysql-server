package pages

import (
	"fmt"
	"xmysql-server/server/common"
	"xmysql-server/util"
)

//go:generate mockgen -source=inode_page.go -destination ./inode_page_mock.go -package pages

///对于一个新的segment，总是优先填满32个frag page数组，之后才会为其分配完整的Extent，可以利用碎片页，并避免小表占用太多空间。
//尽量获得hint page;
//如果segment上未使用的page太多，则尽量利用segment上的page

// 实现INODE
type INodePage struct {
	AbstractPage
	INodePageList DESListNode   //12 byte 存储上一个和下一个INode的页面指针 38-50
	INodeEntries  []*INodeEntry //16320 byte 用于存储具体的段信息，每个INode 192 byte，一共85 个
	EmptySpace    []byte        //6 byte

}

type FragmentArrayEntry struct {
	PageNo []byte //4个字节,用于记载离散页面号码
}

func (fragmentArray FragmentArrayEntry) Byte() []byte {
	return fragmentArray.PageNo
}

// 192个字节
// 管理了85个段
type INodeEntry struct {
	SegmentId           []byte               //8个字节，该结构体对应的段的编号（ID） 若值为0，则表示该SLot未被使用
	NotFullNUsed        []byte               //4个字节，在Notfull链表中已经使用了多少个页面
	FreeListBaseNode    []byte               //16个字节，Free链表
	NotFullListBaseNode []byte               //16个字节，NotFull链表
	FullListBaseNode    []byte               //16个字节，Full链表
	MagicNumber         []byte               //4个字节 0x5D669D2
	FragmentArrayEntry  []FragmentArrayEntry //一共32个array，每个ArrayEntry为零散的页面号
}

// 每当创建一个新的索引，构建一个新的Btree，先为非叶子节点的额segment段分配一个inodeentry，再创建一个rootpage，
// 并将该色门头的位置记录到rootpage中，然后再分配leafsegment的inode entry，并记录到rootpage中
func NewINodeEntry(SegmentId uint64) *INodeEntry {
	framentArray := make([]FragmentArrayEntry, 32)
	for i := 0; i < 32; i++ {
		framentArray[i] = FragmentArrayEntry{PageNo: util.AppendByte(4)}
	}

	return &INodeEntry{
		SegmentId:           util.ConvertULong8Bytes(SegmentId),
		MagicNumber:         util.AppendByte(4),
		NotFullNUsed:        util.ConvertUInt4Bytes(0),
		FreeListBaseNode:    util.AppendByte(16),
		NotFullListBaseNode: util.AppendByte(16),
		FullListBaseNode:    util.AppendByte(16),
		FragmentArrayEntry:  framentArray,
	}
}

// 判断当前段的离散区间是否满了
func (ientry *INodeEntry) FragmentArrayIsFull() bool {
	for _, v := range ientry.FragmentArrayEntry {
		if util.ReadUB4Byte2UInt32(v.PageNo) == 0 {
			return true
		}
	}
	return false
}

func (ientry *INodeEntry) GetCloseZeroFrag() int {

	var result = -1
	for i := 0; i < 32; i++ {
		if (util.ReadUB4Byte2UInt32(ientry.FragmentArrayEntry[i].PageNo)) == 0 {
			result = i
			break
		}
	}
	return result
}

//构造INode

func NewINodePage(spaceNo uint32, pageNumber uint32) *INodePage {
	var iPage = new(INodePage)
	var fileHeader = new(FileHeader)

	offsetBytes := util.ConvertUInt4Bytes(pageNumber) //第一个byte
	copy(fileHeader.FilePageOffset[:], offsetBytes)
	//写入FSP文件头
	typeBytes := util.ConvertInt2Bytes(int32(common.FIL_PAGE_INODE))
	copy(fileHeader.FilePageType[:], typeBytes)

	prevBytes := util.ConvertInt4Bytes(1)
	copy(fileHeader.FilePagePrev[:], prevBytes)

	fileHeader.WritePageFileType(int16(common.FIL_PAGE_INODE))
	fileHeader.WritePageNext(3)
	fileHeader.WritePageLSN(0)
	fileHeader.WritePageFileFlushLSN(0)
	fileHeader.WritePageArch(spaceNo)
	fileHeader.WritePageSpaceCheckSum(nil)
	iPage.FileHeader = *fileHeader
	iPage.INodePageList = DESListNode{
		PreNodePageNumber:  util.AppendByte(4),
		PreNodeOffset:      util.AppendByte(2),
		NextNodePageNumber: util.AppendByte(4),
		NextNodeOffSet:     util.AppendByte(2),
	}
	iPage.INodeEntries = make([]*INodeEntry, 85)
	for k, v := range iPage.INodeEntries {
		v = NewINodeEntry(0)
		iPage.INodeEntries[k] = v
	}
	iPage.FileTrailer = NewFileTrailer()
	iPage.EmptySpace = make([]byte, 6)
	return iPage
}

func (ibuf *INodePage) SerializeBytes() []byte {
	var buff = make([]byte, 0)
	buff = append(buff, ibuf.FileHeader.GetSerialBytes()...)
	buff = append(buff, util.AppendByte(12)...)
	//SegmentId           []byte               //8个字节，该结构体对应的段的编号（ID） 若值为0，则表示该SLot未被泗洪
	//NotFullNUsed        []byte               //4个字节，在Notfull链表中已经使用了多少个页面
	//FreeListBaseNode    []byte               //16个字节，Free链表
	//NotFullListBaseNode []byte               //16个字节，NotFull链表
	//FullListBaseNode    []byte               //16个字节，Full链表
	//MagicNumber         []byte               //4个字节 0x5D669D2
	//FragmentArrayEntry  []FragmentArrayEntry //一共32个array，每个ArrayEntry为零散的页面号

	for _, v := range ibuf.INodeEntries {
		buff = append(buff, v.SegmentId...)
		buff = append(buff, v.NotFullNUsed...)
		buff = append(buff, v.FreeListBaseNode...)
		buff = append(buff, v.NotFullListBaseNode...)
		buff = append(buff, v.FullListBaseNode...)
		buff = append(buff, v.MagicNumber...)
		buff = append(buff, util.AppendByte(32*4)...)
	}
	buff = append(buff, ibuf.EmptySpace...)
	buff = append(buff, ibuf.FileTrailer.FileTrailer[:]...)
	fmt.Println(len(buff))
	return buff
}

func NewINodeByParseBytes(content []byte) *INodePage {
	var inodePage = new(INodePage)
	inodePage.FileHeader = NewFileHeader()
	inodePage.FileTrailer = NewFileTrailer()

	inodePage.LoadFileHeader(content[0:38])
	inodePage.LoadFileTrailer(content[16384-8 : 16384])

	//PreNodePageNumber  []byte //4个字节	表示指向前一个INode页面号
	//PreNodeOffset      []byte //2个字节 65536-1
	//NextNodePageNumber []byte //4个字节  表示指向后一个INode页面号
	//NextNodeOffSet     []byte //2个字节	65536-1
	inodePage.INodePageList.PreNodePageNumber = content[38:42]
	inodePage.INodePageList.PreNodeOffset = content[42:44]
	inodePage.INodePageList.NextNodePageNumber = content[44:48]
	inodePage.INodePageList.NextNodeOffSet = content[48:50]

	inodePage.EmptySpace = content[16384-8-6 : 16384-8]

	inodePage.INodeEntries = make([]*INodeEntry, 85)

	//NotFullNUsed        []byte               //4个字节，在Notfull链表中已经使用了多少个页面
	//FreeListBaseNode    []byte               //16个字节，Free链表 segment上所有page均空闲的extent链表
	//NotFullListBaseNode []byte               //16个字节，NotFull链表 至少有一个page分配给当前Segment的Extent链表，全部用完时，转移到FSEG_FULL上，全部释放时，则归还给当前表空间FSP_FREE链表
	//FullListBaseNode    []byte               //16个字节，Full链表 segment上page被完全使用的extent链表
	//MagicNumber         []byte               //4个字节 0x5D669D2
	//FragmentArrayEntry  []FragmentArrayEntry //一共32个array，每个ArrayEntry为零散的页面号
	//for k, v := range inodePage.INodeEntries {
	//	v.FragmentArrayEntry = make([]pages.FragmentArrayEntry, 32)
	//	inodePage.INodeEntries[k] = &pages.INodeEntry{
	//		SegmentId:           content[50+192*k : 58+192*k],
	//		NotFullNUsed:        content[58+192*k : 62+192*k],
	//		FreeListBaseNode:    content[62+192*k : 78+192*k],
	//		NotFullListBaseNode: content[78+192*k : 94+192*k],
	//		FullListBaseNode:    content[94+192*k : 110+192*k],
	//		MagicNumber:         content[110+192*k : 114+192*k],
	//		FragmentArrayEntry:  parseFragmentArray(content[114+192*k : 242+192*k]),
	//	}
	//}
	for k := 0; k < 85; k++ {
		//	FragmentArrayEntry := make([]pages.FragmentArrayEntry, 32)
		inodePage.INodeEntries[k] = &INodeEntry{
			SegmentId:           content[50+192*k : 58+192*k],
			NotFullNUsed:        content[58+192*k : 62+192*k],
			FreeListBaseNode:    content[62+192*k : 78+192*k],
			NotFullListBaseNode: content[78+192*k : 94+192*k],
			FullListBaseNode:    content[94+192*k : 110+192*k],
			MagicNumber:         content[110+192*k : 114+192*k],
			FragmentArrayEntry:  parseFragmentArray(content[114+192*k : 242+192*k]),
		}
	}
	return inodePage
}

func (ibuf *INodePage) GetSerializeBytes() []byte {
	return ibuf.SerializeBytes()
}

func parseFragmentArray(content []byte) []FragmentArrayEntry {
	var buff = make([]FragmentArrayEntry, 0)
	for i := 0; i < 32; i++ {
		buff = append(buff, FragmentArrayEntry{PageNo: content[i*4 : i*4+4]})
	}
	return buff
}
