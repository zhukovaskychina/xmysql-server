package pages

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/innodb_store/store/storebytes/segs"

	"github.com/zhukovaskychina/xmysql-server/util"
)

type DataDictHeader struct {
	MaxRowId             []byte //8
	MaxTableId           []byte //8
	MaxIndexId           []byte //8
	MaxSpaceId           []byte //4
	MixedIDLow           []byte //4
	SysTableRootPage     []byte //4
	SysTablesIDSRootPage []byte //4
	SysColumnsRootPage   []byte //4
	SysIndexesRootPage   []byte //4
	SysFieldsRootPage    []byte //4
}

//数据字典头部信息
type DataDictionaryHeaderSysPage struct {
	AbstractPage

	FileHeader FileHeader

	DataDictHeader DataDictHeader //52   记录存储基本系统表的根页面未知以及InnoDB存储引擎的一些全局信息

	UnusedSpace []byte //90-94

	SegmentHeader []byte //94-104

	SecondEmptySpace []byte //104-16376

	FileTrailer FileTrailer
}

/******
Max Row Id
Max Table Id
Max Index Id
Max Space Id
Mix ID Low
Root of Sys_tables_cluster_index
Root of Sys_tables_ids_sec_index
Root of Sys_columns_index
Root of Sys_Indexes_cluster_index
Root of Sys_Fields_cluster_index

********/

func NewDataDictHeaderPage() *DataDictionaryHeaderSysPage {

	var fileHeader = new(FileHeader)
	//写入FSP文件头
	fileHeader.FilePageType = util.ConvertInt2Bytes(common.FILE_PAGE_TYPE_SYS)
	fileHeader.FilePagePrev = util.ConvertInt4Bytes(6)
	fileHeader.FilePageOffset = util.ConvertUInt4Bytes(uint32(7))
	//fileHeader.WritePageOffset(0)
	//fileHeader.WritePagePrev(0)
	fileHeader.WritePageFileType(common.FILE_PAGE_TYPE_SYS)
	fileHeader.WritePageNext(8)
	fileHeader.WritePageLSN(0)
	fileHeader.WritePageFileFlushLSN(0)
	fileHeader.WritePageArch(0)
	fileHeader.WritePageSpaceCheckSum(nil)

	var ddp = new(DataDictionaryHeaderSysPage)
	ddp.FileHeader = *fileHeader
	ddp.DataDictHeader = DataDictHeader{
		MaxRowId:             util.ConvertULong8Bytes(0),
		MaxTableId:           util.ConvertULong8Bytes(0),
		MaxIndexId:           util.ConvertULong8Bytes(0),
		MaxSpaceId:           util.ConvertUInt4Bytes(0),
		MixedIDLow:           util.ConvertUInt4Bytes(0),
		SysTableRootPage:     util.ConvertUInt4Bytes(8),
		SysTablesIDSRootPage: util.ConvertUInt4Bytes(9),
		SysColumnsRootPage:   util.ConvertUInt4Bytes(10),
		SysIndexesRootPage:   util.ConvertUInt4Bytes(11),
		SysFieldsRootPage:    util.ConvertUInt4Bytes(12),
	}
	ddp.SegmentHeader = segs.NewSegmentHeader(0, 7, 0).GetBytes()

	ddp.UnusedSpace = util.AppendByte(4)

	ddp.SecondEmptySpace = util.AppendByte(16376 - 104)

	ddp.FileTrailer = NewFileTrailer()

	return ddp
}

func ParseDataDictHrdPage(content []byte) *DataDictionaryHeaderSysPage {

	var fspBinary = new(DataDictionaryHeaderSysPage)
	fspBinary.FileHeader = NewFileHeader()
	fspBinary.FileTrailer = NewFileTrailer()

	fspBinary.LoadFileHeader(content[0:38])
	fspBinary.LoadFileTrailer(content[16384-8 : 16384])

	fspBinary.DataDictHeader = DataDictHeader{
		MaxRowId:             content[38:46],
		MaxTableId:           content[46:54],
		MaxIndexId:           content[54:62],
		MaxSpaceId:           content[62:66],
		MixedIDLow:           content[66:70],
		SysTableRootPage:     content[70:74],
		SysTablesIDSRootPage: content[74:78],
		SysColumnsRootPage:   content[78:82],
		SysIndexesRootPage:   content[82:86],
		SysFieldsRootPage:    content[86:90],
	}

	fspBinary.UnusedSpace = content[90:94]
	fspBinary.SegmentHeader = content[94:104]
	fspBinary.SecondEmptySpace = content[104:16376]

	return fspBinary
}

func (d *DataDictionaryHeaderSysPage) SetMaxRowId(maxRowId uint64) {
	d.DataDictHeader.MaxRowId = util.ConvertULong8Bytes(maxRowId)
}
func (d *DataDictionaryHeaderSysPage) GetMaxRowId() uint64 {
	return util.ReadUB8Byte2Long(d.DataDictHeader.MaxRowId)
}

func (d *DataDictionaryHeaderSysPage) SetMaxTableId(maxTableId uint64) {
	d.DataDictHeader.MaxTableId = util.ConvertULong8Bytes(maxTableId)
}
func (d *DataDictionaryHeaderSysPage) GetMaxTableId() uint64 {
	return util.ReadUB8Byte2Long(d.DataDictHeader.MaxTableId)
}

func (d *DataDictionaryHeaderSysPage) SetMaxIndexId(MaxIndexId uint64) {
	d.DataDictHeader.MaxIndexId = util.ConvertULong8Bytes(MaxIndexId)
}
func (d *DataDictionaryHeaderSysPage) GetMaxIndexId() uint64 {
	return util.ReadUB8Byte2Long(d.DataDictHeader.MaxIndexId)
}

func (d *DataDictionaryHeaderSysPage) SetMaxSpaceId(MaxSpaceId uint64) {
	d.DataDictHeader.MaxSpaceId = util.ConvertULong8Bytes(MaxSpaceId)
}
func (d *DataDictionaryHeaderSysPage) GetMaxSpaceId() uint64 {
	return util.ReadUB8Byte2Long(d.DataDictHeader.MaxSpaceId)
}

func (d *DataDictionaryHeaderSysPage) GetSerializeBytes() []byte {
	var buff = make([]byte, 0)
	buff = append(buff, d.FileHeader.GetSerialBytes()...)
	buff = append(buff, d.DataDictHeader.MaxRowId...)
	buff = append(buff, d.DataDictHeader.MaxTableId...)
	buff = append(buff, d.DataDictHeader.MaxIndexId...)
	buff = append(buff, d.DataDictHeader.MaxSpaceId...)
	buff = append(buff, d.DataDictHeader.MixedIDLow...)
	buff = append(buff, d.DataDictHeader.SysTableRootPage...)
	buff = append(buff, d.DataDictHeader.SysTablesIDSRootPage...)
	buff = append(buff, d.DataDictHeader.SysColumnsRootPage...)
	buff = append(buff, d.DataDictHeader.SysIndexesRootPage...)
	buff = append(buff, d.DataDictHeader.SysFieldsRootPage...)
	buff = append(buff, d.UnusedSpace...)
	buff = append(buff, d.SegmentHeader...)
	buff = append(buff, d.SecondEmptySpace...)
	buff = append(buff, d.FileTrailer.FileTrailer...)
	return buff
}
