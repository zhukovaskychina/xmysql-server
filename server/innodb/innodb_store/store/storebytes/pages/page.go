package pages

//go:generate mockgen -source=page.go -destination ./age_mock.go -package pages

import "github.com/zhukovaskychina/xmysql-server/util"

type FileHeader struct {
	FilePageSpaceOrCheckSum []byte //4校验，表示该页面的校验和
	FilePageOffset          []byte //页号 4   不能简单翻译为页号
	FilePagePrev            []byte //上一个页号
	FilePageNext            []byte //下一个页号
	FilePageLSN             []byte //8 页面被修改时对应的LSN（log sequence number） 日志序列号
	FilePageType            []byte //2 该页的类型
	FilePageFileFlushLSN    []byte //8 仅在系统表空间的第一个野种定义，该文件至少被刷新到了对应的LSN值
	FilePageArch            []byte //4 表述当前页属于哪个表空间
}

func NewFileHeader() FileHeader {
	return FileHeader{
		FilePageSpaceOrCheckSum: nil,
		FilePageOffset:          nil,
		FilePagePrev:            nil,
		FilePageNext:            nil,
		FilePageLSN:             nil,
		FilePageType:            nil,
		FilePageFileFlushLSN:    nil,
		FilePageArch:            nil,
	}
}

//暂时写死
func (fh *FileHeader) WritePageSpaceCheckSum(checkSum []byte) {
	fh.FilePageSpaceOrCheckSum = []byte{0x01, 0x02, 0x03, 0x04}
	//	fh.FilePageSpaceOrCheckSum = checkSum
}
func (fh *FileHeader) WritePageOffset(pageOffset uint32) {
	fh.FilePageOffset = util.ConvertUInt4Bytes(uint32(pageOffset))
}
func (fh *FileHeader) WritePagePrev(pageOffset int) {
	fh.FilePagePrev = util.ConvertInt4Bytes(int32(pageOffset))
}
func (fh *FileHeader) WritePageNext(pageOffset int) {
	fh.FilePageNext = util.ConvertInt4Bytes(int32(pageOffset))
}

func (fh *FileHeader) WritePageLSN(pageLSN int64) {
	fh.FilePageLSN = util.ConvertLong8Bytes(pageLSN)
}

func (fh *FileHeader) WritePageFileType(filePageType int) {
	fh.FilePageType = util.ConvertInt2Bytes(int32(filePageType))
}
func (fh *FileHeader) WritePageFileFlushLSN(fileFlushLSN int64) {
	fh.FilePageFileFlushLSN = util.ConvertLong8Bytes(fileFlushLSN)
}
func (fh *FileHeader) WritePageArch(pageArch uint32) {
	fh.FilePageArch = util.ConvertUInt4Bytes(uint32(pageArch))
}

func (fh *FileHeader) GetCurrentPageOffset() uint32 {
	return util.ReadUB4Byte2UInt32(fh.FilePageOffset)
}

func (fh *FileHeader) GetLastPageOffset() uint32 {
	return util.ReadUB4Byte2UInt32(fh.FilePagePrev)
}

func (fh *FileHeader) GetNextPageOffset() uint32 {
	return util.ReadUB4Byte2UInt32(fh.FilePageNext)
}

func (fh *FileHeader) GetFilePageArch() uint32 {
	return util.ReadUB4Byte2UInt32(fh.FilePageArch)
}

type FileTrailer struct {
	FileTrailer []byte //8 byte
}

func NewFileTrailer() FileTrailer {
	return FileTrailer{FileTrailer: make([]byte, 8)}
}

//页面的分裂条件
//下一个数据页总用户记录的主键值必须大于上一个页中用户记录的主键值

func (fh FileHeader) ParserFileHeader(buff []byte) {
	fh.FilePageSpaceOrCheckSum = buff[0:4]
	fh.FilePageOffset = buff[4:8]
	fh.FilePagePrev = buff[8:12]
	fh.FilePageNext = buff[12:16]
	fh.FilePageLSN = buff[16:24]
	fh.FilePageType = buff[24:26]
	fh.FilePageFileFlushLSN = buff[26:34]
	fh.FilePageArch = buff[34:38]
}

func (fh *FileHeader) GetSerialBytes() []byte {
	var buff = make([]byte, 0)
	buff = append(buff, fh.FilePageSpaceOrCheckSum...)
	buff = append(buff, fh.FilePageOffset...)
	buff = append(buff, fh.FilePagePrev...)
	buff = append(buff, fh.FilePageNext...)
	buff = append(buff, fh.FilePageLSN...)
	buff = append(buff, fh.FilePageType...)
	buff = append(buff, fh.FilePageFileFlushLSN...)
	buff = append(buff, fh.FilePageArch...)

	return buff
}

//定义抽象Page
/*******
*
*  //////////////////////////
*  //      FileHeader      //
*  //////////////////////////
*  //      FileBody        //
   //////////////////////////
   //      FileTrailer     //
   //////////////////////////
*
*********/
type IPage interface {
	GetFileHeader() FileHeader

	GetFileTrailer() FileTrailer

	GetSerializeBytes() []byte

	LoadFileHeader(content []byte)

	LoadFileTrailer(content []byte)
}
type AbstractPage struct {
	IPage

	FileHeader FileHeader

	FileTrailer FileTrailer
}

func (a *AbstractPage) GetFileHeader() FileHeader {
	return a.FileHeader
}

func (a *AbstractPage) GetFileTrailer() FileTrailer {
	return a.FileTrailer
}

func (a *AbstractPage) LoadFileHeader(content []byte) {
	a.FileHeader.FilePageSpaceOrCheckSum = content[0:4]
	a.FileHeader.FilePageOffset = content[4:8]
	a.FileHeader.FilePagePrev = content[8:12]
	a.FileHeader.FilePageNext = content[12:16]
	a.FileHeader.FilePageLSN = content[16:24]
	a.FileHeader.FilePageType = content[24:26]
	a.FileHeader.FilePageFileFlushLSN = content[26:34]
	a.FileHeader.FilePageArch = content[34:38]
}

func (a *AbstractPage) LoadFileTrailer(content []byte) {
	a.FileTrailer.FileTrailer = content[0:]
}

//获得Bytes
func (a *AbstractPage) SerializeBytes() []byte {
	return nil
}
