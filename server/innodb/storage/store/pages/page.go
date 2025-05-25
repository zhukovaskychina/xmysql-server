// Package pages implements InnoDB page structure and operations
package pages

//go:generate mockgen -source=page.go -destination ./age_mock.go -package pages

import (
	"errors"
	"xmysql-server/util"
)

// Common page size constants
const (
	CheckSumSize      = 4  // Size of page checksum in bytes
	PageOffsetSize    = 4  // Size of page offset in bytes
	PagePrevSize      = 4  // Size of previous page pointer in bytes
	PageNextSize      = 4  // Size of next page pointer in bytes
	PageLSNSize       = 8  // Size of LSN in bytes
	PageTypeSize      = 2  // Size of page type in bytes
	PageFileFlushSize = 8  // Size of file flush LSN in bytes
	PageArchSize      = 4  // Size of page arch in bytes
	FileHeaderSize    = 38 // Total size of file header
)

// Common errors
var (
	ErrInvalidHeaderSize = errors.New("invalid header size")
	ErrInvalidChecksum   = errors.New("invalid page checksum")
)

// FileHeader represents the header structure of an InnoDB page
type FileHeader struct {
	FilePageSpaceOrCheckSum [CheckSumSize]byte      // Page checksum
	FilePageOffset          [PageOffsetSize]byte    // Page number
	FilePagePrev            [PagePrevSize]byte      // Previous page number
	FilePageNext            [PageNextSize]byte      // Next page number
	FilePageLSN             [PageLSNSize]byte       // Log Sequence Number when page was last modified
	FilePageType            [PageTypeSize]byte      // Page type
	FilePageFileFlushLSN    [PageFileFlushSize]byte // System tablespace first page flush LSN
	FilePageArch            [PageArchSize]byte      // Tablespace identifier
}

// NewFileHeader creates a new initialized FileHeader
func NewFileHeader() FileHeader {
	return FileHeader{}
}

// WritePageSpaceCheckSum writes the page checksum
func (fh *FileHeader) WritePageSpaceCheckSum(checkSum []byte) error {
	if len(checkSum) != CheckSumSize {
		return ErrInvalidHeaderSize
	}
	copy(fh.FilePageSpaceOrCheckSum[:], checkSum)
	return nil
}

// WritePageOffset writes the page offset (page number)
func (fh *FileHeader) WritePageOffset(pageOffset uint32) {
	bytes := util.ConvertUInt4Bytes(pageOffset)
	copy(fh.FilePageOffset[:], bytes)
}

// WritePagePrev writes the previous page offset
func (fh *FileHeader) WritePagePrev(pageOffset int32) {
	bytes := util.ConvertInt4Bytes(pageOffset)
	copy(fh.FilePagePrev[:], bytes)
}

// WritePageNext writes the next page offset
func (fh *FileHeader) WritePageNext(pageOffset int32) {
	bytes := util.ConvertInt4Bytes(pageOffset)
	copy(fh.FilePageNext[:], bytes)
}

// WritePageLSN writes the Log Sequence Number
func (fh *FileHeader) WritePageLSN(pageLSN int64) {
	bytes := util.ConvertLong8Bytes(pageLSN)
	copy(fh.FilePageLSN[:], bytes)
}

// WritePageFileType writes the page type
func (fh *FileHeader) WritePageFileType(filePageType int16) {
	bytes := util.ConvertInt2Bytes(int32(filePageType))
	copy(fh.FilePageType[:], bytes)
}

// WritePageFileFlushLSN writes the file flush LSN
func (fh *FileHeader) WritePageFileFlushLSN(fileFlushLSN int64) {
	bytes := util.ConvertLong8Bytes(fileFlushLSN)
	copy(fh.FilePageFileFlushLSN[:], bytes)
}

// WritePageArch writes the tablespace identifier
func (fh *FileHeader) WritePageArch(pageArch uint32) {
	bytes := util.ConvertUInt4Bytes(pageArch)
	copy(fh.FilePageArch[:], bytes)
}

// GetCurrentPageOffset returns the current page number
func (fh *FileHeader) GetCurrentPageOffset() uint32 {
	return util.ReadUB4Byte2UInt32(fh.FilePageOffset[:])
}

// GetLastPageOffset returns the previous page number
func (fh *FileHeader) GetLastPageOffset() uint32 {
	return util.ReadUB4Byte2UInt32(fh.FilePagePrev[:])
}

// GetNextPageOffset returns the next page number
func (fh *FileHeader) GetNextPageOffset() uint32 {
	return util.ReadUB4Byte2UInt32(fh.FilePageNext[:])
}

// GetFilePageArch returns the tablespace identifier
func (fh *FileHeader) GetFilePageArch() uint32 {
	return util.ReadUB4Byte2UInt32(fh.FilePageArch[:])
}

// GetPageLSN returns the page LSN
func (fh *FileHeader) GetPageLSN() int64 {
	return util.ReadB8Byte2Int64(fh.FilePageLSN[:])
}

// GetPageType returns the page type
func (fh *FileHeader) GetPageType() int16 {
	return int16(util.ReadB2Byte2Int32(fh.FilePageType[:]))
}

// FileTrailer represents the trailer structure of an InnoDB page
type FileTrailer struct {
	FileTrailer [8]byte // Fixed 8-byte trailer
}

// NewFileTrailer creates a new initialized FileTrailer
func NewFileTrailer() FileTrailer {
	return FileTrailer{}
}

// SetChecksum sets the checksum in the file trailer
func (ft *FileTrailer) SetChecksum(checksum uint64) {
	bytes := util.ConvertUInt8Bytes(checksum)
	copy(ft.FileTrailer[:], bytes)
}

// GetChecksum returns the checksum from the file trailer
func (ft *FileTrailer) GetChecksum() uint64 {
	return util.ReadUB8Byte2UInt64(ft.FileTrailer[:])
}

//页面的分裂条件
//下一个数据页总用户记录的主键值必须大于上一个页中用户记录的主键值

// ParseFileHeader parses the file header from a byte buffer
func (fh *FileHeader) ParseFileHeader(buff []byte) error {
	if len(buff) < FileHeaderSize {
		return ErrInvalidHeaderSize
	}
	copy(fh.FilePageSpaceOrCheckSum[:], buff[0:4])
	copy(fh.FilePageOffset[:], buff[4:8])
	copy(fh.FilePagePrev[:], buff[8:12])
	copy(fh.FilePageNext[:], buff[12:16])
	copy(fh.FilePageLSN[:], buff[16:24])
	copy(fh.FilePageType[:], buff[24:26])
	copy(fh.FilePageFileFlushLSN[:], buff[26:34])
	copy(fh.FilePageArch[:], buff[34:38])
	return nil
}

// GetSerialBytes serializes the file header to bytes
func (fh *FileHeader) GetSerialBytes() []byte {
	buff := make([]byte, FileHeaderSize)
	offset := 0

	copy(buff[offset:], fh.FilePageSpaceOrCheckSum[:])
	offset += CheckSumSize

	copy(buff[offset:], fh.FilePageOffset[:])
	offset += PageOffsetSize

	copy(buff[offset:], fh.FilePagePrev[:])
	offset += PagePrevSize

	copy(buff[offset:], fh.FilePageNext[:])
	offset += PageNextSize

	copy(buff[offset:], fh.FilePageLSN[:])
	offset += PageLSNSize

	copy(buff[offset:], fh.FilePageType[:])
	offset += PageTypeSize

	copy(buff[offset:], fh.FilePageFileFlushLSN[:])
	offset += PageFileFlushSize

	copy(buff[offset:], fh.FilePageArch[:])

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
	copy(a.FileHeader.FilePageSpaceOrCheckSum[:], content[0:4])
	copy(a.FileHeader.FilePageOffset[:], content[4:8])
	copy(a.FileHeader.FilePagePrev[:], content[8:12])
	copy(a.FileHeader.FilePageNext[:], content[12:16])
	copy(a.FileHeader.FilePageLSN[:], content[16:24])
	copy(a.FileHeader.FilePageType[:], content[24:26])
	copy(a.FileHeader.FilePageFileFlushLSN[:], content[26:34])
	copy(a.FileHeader.FilePageArch[:], content[34:38])
}

func (a *AbstractPage) LoadFileTrailer(content []byte) {
	copy(a.FileTrailer.FileTrailer[:], content[0:])
}

// 获得Bytes
func (a *AbstractPage) SerializeBytes() []byte {
	return nil
}
