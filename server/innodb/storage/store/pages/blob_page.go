/*
FIL_PAGE_TYPE_BLOB页面详细说明

基本属性：
- 页面类型：FIL_PAGE_TYPE_BLOB
- 类型编码：0x000A（十进制10）
- 所属模块：InnoDB大字段存储
- 页面作用：存储BLOB、TEXT等大字段的实际内容

使用场景：
1. 当字段值超过一定大小（通常是前缀长度限制）时，MySQL会将该字段存储在BLOB页面中
2. 主记录页面中只保留前缀数据和指向BLOB页面的指针
3. 支持链式存储，一个BLOB字段可能跨越多个BLOB页面

页面结构：
- File Header (38字节)
- BLOB Header (20字节):
  - Length: BLOB数据总长度 (4字节)
  - Next Page: 下一个BLOB页面号 (4字节)
  - Offset: 当前页面数据偏移量 (4字节)
  - Segment ID: 所属段ID (8字节)
- BLOB Data (16318字节): 实际的BLOB数据
- File Trailer (8字节)

应用示例：
- 大文本字段（TEXT、LONGTEXT）
- 二进制数据（BLOB、LONGBLOB）
- JSON文档存储
- 图片、文档等二进制文件存储
*/

package pages

import (
	"encoding/binary"
	"errors"
	"xmysql-server/server/common"
)

// BLOB页面常量
const (
	BlobHeaderSize  = 20    // BLOB头部大小
	BlobDataSize    = 16318 // BLOB数据区大小 (16384 - 38 - 20 - 8)
	FileTrailerSize = 8     // 文件尾部大小
)

// BLOB页面错误定义
var (
	ErrBlobPageFull    = errors.New("BLOB页面已满")
	ErrInvalidBlobSize = errors.New("无效的BLOB大小")
	ErrBlobNotFound    = errors.New("BLOB数据未找到")
)

// BlobHeader BLOB页面头部结构
type BlobHeader struct {
	Length    uint32 // BLOB数据总长度
	NextPage  uint32 // 下一个BLOB页面号（0表示最后一页）
	Offset    uint32 // 当前页面在整个BLOB中的偏移量
	SegmentID uint64 // 所属段ID
}

// BlobPage BLOB页面结构
type BlobPage struct {
	FileHeader  FileHeader  // 文件头部 (38字节)
	BlobHeader  BlobHeader  // BLOB头部 (20字节)
	Data        []byte      // BLOB数据 (16318字节)
	FileTrailer FileTrailer // 文件尾部 (8字节)
}

// NewBlobPage 创建新的BLOB页面
func NewBlobPage(spaceID, pageNo uint32, segmentID uint64) *BlobPage {
	page := &BlobPage{
		FileHeader: NewFileHeader(),
		BlobHeader: BlobHeader{
			Length:    0,
			NextPage:  0,
			Offset:    0,
			SegmentID: segmentID,
		},
		Data:        make([]byte, BlobDataSize),
		FileTrailer: NewFileTrailer(),
	}

	// 设置页面头部信息
	page.FileHeader.WritePageOffset(pageNo)
	page.FileHeader.WritePageFileType(int16(common.FIL_PAGE_TYPE_BLOB))
	page.FileHeader.WritePageArch(spaceID)

	return page
}

// SetBlobData 设置BLOB数据
func (bp *BlobPage) SetBlobData(data []byte, totalLength uint32, offset uint32, nextPage uint32) error {
	if len(data) > BlobDataSize {
		return ErrInvalidBlobSize
	}

	bp.BlobHeader.Length = totalLength
	bp.BlobHeader.Offset = offset
	bp.BlobHeader.NextPage = nextPage

	// 清空数据区并复制新数据
	for i := range bp.Data {
		bp.Data[i] = 0
	}
	copy(bp.Data, data)

	return nil
}

// GetBlobData 获取BLOB数据
func (bp *BlobPage) GetBlobData() []byte {
	// 计算实际数据长度
	remainingLength := bp.BlobHeader.Length - bp.BlobHeader.Offset
	if remainingLength > BlobDataSize {
		remainingLength = BlobDataSize
	}

	return bp.Data[:remainingLength]
}

// GetNextPageNo 获取下一个页面号
func (bp *BlobPage) GetNextPageNo() uint32 {
	return bp.BlobHeader.NextPage
}

// IsLastPage 判断是否为最后一个页面
func (bp *BlobPage) IsLastPage() bool {
	return bp.BlobHeader.NextPage == 0
}

// GetSegmentID 获取段ID
func (bp *BlobPage) GetSegmentID() uint64 {
	return bp.BlobHeader.SegmentID
}

// GetTotalLength 获取BLOB总长度
func (bp *BlobPage) GetTotalLength() uint32 {
	return bp.BlobHeader.Length
}

// GetCurrentOffset 获取当前偏移量
func (bp *BlobPage) GetCurrentOffset() uint32 {
	return bp.BlobHeader.Offset
}

// Serialize 序列化页面为字节数组
func (bp *BlobPage) Serialize() []byte {
	data := make([]byte, common.PageSize)
	offset := 0

	// 序列化文件头部
	copy(data[offset:], bp.serializeFileHeader())
	offset += FileHeaderSize

	// 序列化BLOB头部
	copy(data[offset:], bp.serializeBlobHeader())
	offset += BlobHeaderSize

	// 序列化BLOB数据
	copy(data[offset:], bp.Data)
	offset += BlobDataSize

	// 序列化文件尾部
	copy(data[offset:], bp.serializeFileTrailer())

	return data
}

// Deserialize 从字节数组反序列化页面
func (bp *BlobPage) Deserialize(data []byte) error {
	if len(data) != common.PageSize {
		return ErrInvalidPageSize
	}

	offset := 0

	// 反序列化文件头部
	if err := bp.deserializeFileHeader(data[offset : offset+FileHeaderSize]); err != nil {
		return err
	}
	offset += FileHeaderSize

	// 反序列化BLOB头部
	if err := bp.deserializeBlobHeader(data[offset : offset+BlobHeaderSize]); err != nil {
		return err
	}
	offset += BlobHeaderSize

	// 反序列化BLOB数据
	copy(bp.Data, data[offset:offset+BlobDataSize])
	offset += BlobDataSize

	// 反序列化文件尾部
	if err := bp.deserializeFileTrailer(data[offset : offset+FileTrailerSize]); err != nil {
		return err
	}

	return nil
}

// serializeFileHeader 序列化文件头部
func (bp *BlobPage) serializeFileHeader() []byte {
	// 实现文件头部序列化逻辑
	data := make([]byte, FileHeaderSize)
	// 这里应该包含具体的序列化逻辑
	return data
}

// serializeBlobHeader 序列化BLOB头部
func (bp *BlobPage) serializeBlobHeader() []byte {
	data := make([]byte, BlobHeaderSize)

	binary.LittleEndian.PutUint32(data[0:], bp.BlobHeader.Length)
	binary.LittleEndian.PutUint32(data[4:], bp.BlobHeader.NextPage)
	binary.LittleEndian.PutUint32(data[8:], bp.BlobHeader.Offset)
	binary.LittleEndian.PutUint64(data[12:], bp.BlobHeader.SegmentID)

	return data
}

// serializeFileTrailer 序列化文件尾部
func (bp *BlobPage) serializeFileTrailer() []byte {
	// 实现文件尾部序列化逻辑
	data := make([]byte, FileTrailerSize)
	// 这里应该包含具体的序列化逻辑
	return data
}

// deserializeFileHeader 反序列化文件头部
func (bp *BlobPage) deserializeFileHeader(data []byte) error {
	// 实现文件头部反序列化逻辑
	return nil
}

// deserializeBlobHeader 反序列化BLOB头部
func (bp *BlobPage) deserializeBlobHeader(data []byte) error {
	if len(data) < BlobHeaderSize {
		return ErrInvalidPageSize
	}

	bp.BlobHeader.Length = binary.LittleEndian.Uint32(data[0:])
	bp.BlobHeader.NextPage = binary.LittleEndian.Uint32(data[4:])
	bp.BlobHeader.Offset = binary.LittleEndian.Uint32(data[8:])
	bp.BlobHeader.SegmentID = binary.LittleEndian.Uint64(data[12:])

	return nil
}

// deserializeFileTrailer 反序列化文件尾部
func (bp *BlobPage) deserializeFileTrailer(data []byte) error {
	// 实现文件尾部反序列化逻辑
	return nil
}

// Validate 验证页面数据完整性
func (bp *BlobPage) Validate() error {
	// 验证BLOB头部
	if bp.BlobHeader.Length == 0 {
		return ErrInvalidBlobSize
	}

	// 验证偏移量
	if bp.BlobHeader.Offset > bp.BlobHeader.Length {
		return ErrInvalidBlobSize
	}

	// 验证段ID
	if bp.BlobHeader.SegmentID == 0 {
		return ErrInvalidBlobSize
	}

	return nil
}

// GetFileHeader 获取文件头部
func (bp *BlobPage) GetFileHeader() FileHeader {
	return bp.FileHeader
}

// GetFileTrailer 获取文件尾部
func (bp *BlobPage) GetFileTrailer() FileTrailer {
	return bp.FileTrailer
}

// GetSerializeBytes 获取序列化后的字节数组
func (bp *BlobPage) GetSerializeBytes() []byte {
	return bp.Serialize()
}

// LoadFileHeader 加载文件头部
func (bp *BlobPage) LoadFileHeader(content []byte) {
	bp.deserializeFileHeader(content)
}

// LoadFileTrailer 加载文件尾部
func (bp *BlobPage) LoadFileTrailer(content []byte) {
	bp.deserializeFileTrailer(content)
}
