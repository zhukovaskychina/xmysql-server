package pages

import (
	"xmysql-server/server/common"
	"xmysql-server/util"
)

const BYTES_0 = 0

const BYTES_512 = 1

const BYTES_1024 = 2

const BYTES_2048 = 3

// 等待实现
// BitMap
type IBufBitMapPage struct {
	AbstractPage
	ChangeBufferBitMap []byte //8192 byte，每4个bit表示一个页面，即管理一个页面，也就是该bitmap下面的16384个页面
	EmptySpace         []byte // 8146 byte
}

//其中4个bit描述每个page的change_buffer 信息
//IBUF_BITMAP_FREE	2	使用2个bit来描述page的空闲空间范围：0（0 bytes）、1（512 bytes）、2（1024 bytes）、3（2048 bytes）
//IBUF_BITMAP_BUFFERED	1	是否有ibuf操作缓存
//IBUF_BITMAP_IBUF	1	该Page本身是否是Ibuf Btree的节点

type BitMapDesc struct {
	IsBuffCache bool  //是否IBuf缓存
	IsBuffLeaf  bool  // 是否Btree 的节点
	PageDesc    uint8 //使用2个bit来描述page的空闲空间范围：0（0 bytes）、1（512 bytes）、2（1024 bytes）、3（2048 bytes）
}

type IBufRecord struct {
	RecFieldSpace    []byte //4 个字节		索引对应的SpaceId
	RecFieldMarker   byte   //1 个字节，默认0	默认为0，用于区分新老版本的ibuf
	RecFieldPage     []byte //4 个字节	 索引上的pageno
	RecOffsetCounter []byte //2 byte	 同一个page上缓存操作的 递增序号
	RecOffsetType    byte   //1 byte	 缓存操作的类型
	RecOffsetFlags   byte   //1 byte	 表示用户表的rowformat类型值为0时为Redundant，

	RecordNFields []byte //不定长 每个列6字节，，表示列的长度以及Null值的存储，随后用户咧数据
}

func NewIBufBitMapPage(pageNumber uint32) IBufBitMapPage {

	ibufBitMapPage := new(IBufBitMapPage)

	var fileHeader = new(FileHeader)

	offsetBytes := util.ConvertUInt4Bytes(pageNumber) //第一个byte
	copy(fileHeader.FilePageOffset[:], offsetBytes)
	//写入FSP文件头
	typeBytes := util.ConvertInt2Bytes(int32(common.FIL_PAGE_IBUF_BITMAP))
	copy(fileHeader.FilePageType[:], typeBytes)

	prevBytes := util.ConvertInt4Bytes(0)
	copy(fileHeader.FilePagePrev[:], prevBytes)
	//fileHeader.WritePagePrev(0)
	fileHeader.WritePageFileType(int16(common.FIL_PAGE_IBUF_BITMAP))
	fileHeader.WritePageNext(2)
	fileHeader.WritePageLSN(0)
	fileHeader.WritePageFileFlushLSN(0)
	fileHeader.WritePageArch(0)
	fileHeader.WritePageSpaceCheckSum(nil)

	ibufBitMapPage.FileHeader = *fileHeader
	ibufBitMapPage.ChangeBufferBitMap = make([]byte, 8192)
	ibufBitMapPage.EmptySpace = make([]byte, 8146)
	ibufBitMapPage.FileTrailer = NewFileTrailer()
	return *ibufBitMapPage
}

func (ibuf *IBufBitMapPage) GetNextPageInfo(index int) BitMapDesc {
	var bitMapIndex = index / 2
	var descBits = ibuf.ChangeBufferBitMap[bitMapIndex]
	var binaryString = util.ToBinaryString(descBits)
	var firstFour = ""
	//取前四位
	if index%2 == 0 {
		//
		firstFour = util.Substr(binaryString, 0, 4)
		//
	} else {
		firstFour = util.Substr(binaryString, 4, 8)
	}
	var pageDesc = util.ConvertBits2Byte(util.Substr(firstFour, 0, 2))

	var buffCache = util.ConvertBits2Byte(util.Substr(firstFour, 2, 3))

	var IsBuffLeaf = util.ConvertBits2Byte(util.Substr(firstFour, 3, 4))
	bitMapDesc := BitMapDesc{
		IsBuffCache: buffCache == 1,
		IsBuffLeaf:  IsBuffLeaf == 1,
		PageDesc:    pageDesc,
	}
	return bitMapDesc
}

func (ibuf *IBufBitMapPage) SerializeBytes() []byte {
	var buff = make([]byte, 0)
	buff = append(buff, ibuf.FileHeader.GetSerialBytes()...)
	buff = append(buff, ibuf.ChangeBufferBitMap...)

	buff = append(buff, ibuf.EmptySpace...)
	buff = append(buff, ibuf.FileTrailer.FileTrailer[:]...)
	return buff
}

func (ibuf *IBufBitMapPage) GetSerializeBytes() []byte {
	return ibuf.SerializeBytes()
}
