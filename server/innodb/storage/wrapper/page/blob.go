package page

import (
	"encoding/binary"
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
)

var (
	ErrBlobTooLarge = errors.New("blob data too large")
	ErrBlobInvalid  = errors.New("invalid blob data")
)

const (
	BlobHeaderSize = 20                         // BLOB页面头大小
	MaxBlobSize    = 16384 - BlobHeaderSize - 8 // 最大BLOB数据大小
	PageTypeBlob   = common.FIL_PAGE_TYPE_BLOB  // BLOB页面类型
)

// BlobPage BLOB页面实现
type BlobPage struct {
	*BasePageWrapper
	header BlobHeader
}

// BlobPageBody BLOB页面体结构
type BlobPageBody struct {
	PartNumber   uint32 // 分片序号
	NextPartPage uint32 // 下一个分片页号
	DataLength   uint32 // 数据长度
	Data         []byte // 实际数据
}

// BlobHeader BLOB页面头
type BlobHeader struct {
	TableID    uint64 // 表ID
	ColumnID   uint32 // 列ID
	PartNumber uint32 // 分片编号
	NextPage   uint32 // 下一个分片页面号
	DataLength uint32 // 数据长度
}

// NewBlobPage 创建BLOB页面
func NewBlobPage(spaceID, pageNo uint32) IPageWrapper {
	bp := &BlobPage{
		BasePageWrapper: NewBasePageWrapper(pageNo, spaceID, PageTypeBlob),
	}

	// 初始化BLOB页面体
	body := &BlobPageBody{
		PartNumber:   0,
		NextPartPage: 0,
		DataLength:   0,
		Data:         make([]byte, 0),
	}

	// 序列化页面体到PageBody
	bp.serializeBlobBody(body)

	return bp
}

// SetData 设置BLOB数据
func (bp *BlobPage) SetData(data []byte, tableID uint64, columnID uint32, partNumber uint32) error {
	bp.Lock()
	defer bp.Unlock()

	// 检查数据大小
	if len(data) > MaxBlobSize {
		return ErrBlobTooLarge
	}

	// 设置头信息
	bp.header = BlobHeader{
		TableID:    tableID,
		ColumnID:   columnID,
		PartNumber: partNumber,
		NextPage:   0,
		DataLength: uint32(len(data)),
	}

	// 设置数据
	body := &BlobPageBody{
		PartNumber:   partNumber,
		NextPartPage: 0,
		DataLength:   uint32(len(data)),
		Data:         make([]byte, len(data)),
	}
	copy(body.Data, data)

	// 序列化页面体
	bp.serializeBlobBody(body)
	bp.MarkDirty()

	return nil
}

// GetData 获取BLOB数据
func (bp *BlobPage) GetData() []byte {
	bp.RLock()
	defer bp.RUnlock()

	body := bp.deserializeBlobBody()
	result := make([]byte, len(body.Data))
	copy(result, body.Data)
	return result
}

// GetHeader 获取BLOB头
func (bp *BlobPage) GetHeader() BlobHeader {
	bp.RLock()
	defer bp.RUnlock()
	return bp.header
}

// SetNextPartPage 设置下一个分片页号
func (bp *BlobPage) SetNextPartPage(pageNo uint32) {
	bp.Lock()
	defer bp.Unlock()
	body := bp.deserializeBlobBody()
	body.NextPartPage = pageNo
	bp.serializeBlobBody(body)
	bp.MarkDirty()
}

// GetDataLength 获取数据长度
func (bp *BlobPage) GetDataLength() uint32 {
	bp.RLock()
	defer bp.RUnlock()
	body := bp.deserializeBlobBody()
	return body.DataLength
}

// IsLastPart 是否最后一个分片
func (bp *BlobPage) IsLastPart() bool {
	bp.RLock()
	defer bp.RUnlock()
	body := bp.deserializeBlobBody()
	return body.NextPartPage == 0
}

// ParseFromBytes 解析字节数据
func (bp *BlobPage) ParseFromBytes(content []byte) error {
	// 解析基础页面
	err := bp.BasePageWrapper.ParseFromBytes(content)
	if err != nil {
		return err
	}

	// 验证页面类型
	if bp.GetPageType() != PageTypeBlob {
		return ErrBlobInvalid
	}

	// 解析BLOB数据
	if len(content) > pages.FileHeaderSize+BlobHeaderSize {
		offset := pages.FileHeaderSize
		body := &BlobPageBody{
			PartNumber:   binary.LittleEndian.Uint32(content[offset:]),
			NextPartPage: binary.LittleEndian.Uint32(content[offset+4:]),
			DataLength:   binary.LittleEndian.Uint32(content[offset+8:]),
		}

		// 读取数据
		dataOffset := offset + 12
		if len(content) >= dataOffset+int(body.DataLength) {
			body.Data = make([]byte, body.DataLength)
			copy(body.Data, content[dataOffset:dataOffset+int(body.DataLength)])
		}

		bp.serializeBlobBody(body)
	}

	return nil
}

// ToBytes 转换为字节数组
func (bp *BlobPage) ToBytes() ([]byte, error) {
	// 获取基础页面字节
	base, err := bp.BasePageWrapper.ToBytes()
	if err != nil {
		return nil, err
	}

	// 复制基础内容
	content := make([]byte, len(base))
	copy(content, base)

	// 写入BLOB数据
	body := bp.deserializeBlobBody()
	offset := pages.FileHeaderSize

	if len(content) > offset+12 {
		binary.LittleEndian.PutUint32(content[offset:], body.PartNumber)
		binary.LittleEndian.PutUint32(content[offset+4:], body.NextPartPage)
		binary.LittleEndian.PutUint32(content[offset+8:], body.DataLength)

		// 写入数据
		dataOffset := offset + 12
		if len(content) >= dataOffset+len(body.Data) {
			copy(content[dataOffset:], body.Data)
		}
	}

	return content, nil
}

// serializeBlobBody 序列化BLOB页面体
func (bp *BlobPage) serializeBlobBody(body *BlobPageBody) {
	// 分配缓冲区
	buff := make([]byte, 12+len(body.Data))

	// 写入头部信息
	binary.LittleEndian.PutUint32(buff[0:], body.PartNumber)
	binary.LittleEndian.PutUint32(buff[4:], body.NextPartPage)
	binary.LittleEndian.PutUint32(buff[8:], body.DataLength)

	// 写入数据
	copy(buff[12:], body.Data)

	// 写入页面体
	bp.content = buff
}

// deserializeBlobBody 反序列化BLOB页面体
func (bp *BlobPage) deserializeBlobBody() *BlobPageBody {
	// 获取页面体内容
	content := bp.content

	if len(content) < 12 {
		return &BlobPageBody{
			PartNumber:   0,
			NextPartPage: 0,
			DataLength:   0,
			Data:         make([]byte, 0),
		}
	}

	// 解析头部信息
	body := &BlobPageBody{
		PartNumber:   binary.LittleEndian.Uint32(content[0:]),
		NextPartPage: binary.LittleEndian.Uint32(content[4:]),
		DataLength:   binary.LittleEndian.Uint32(content[8:]),
	}

	// 解析数据
	if len(content) >= 12+int(body.DataLength) {
		body.Data = make([]byte, body.DataLength)
		copy(body.Data, content[12:12+body.DataLength])
	} else {
		body.Data = make([]byte, 0)
	}

	return body
}

// Read 实现Page接口
func (bp *BlobPage) Read() error {
	return nil // 简化实现
}

// Write 实现Page接口
func (bp *BlobPage) Write() error {
	return nil // 简化实现
}

// GetPageType 实现IPageWrapper接口
func (bp *BlobPage) GetPageType() common.PageType {
	return PageTypeBlob
}

// GetFreeSpace 获取空闲空间
func (bp *BlobPage) GetFreeSpace() uint32 {
	body := bp.deserializeBlobBody()
	used := 12 + len(body.Data) // 头部 + 数据
	return uint32(16384 - used)
}

// GetUsedSpace 获取已使用空间
func (bp *BlobPage) GetUsedSpace() uint32 {
	body := bp.deserializeBlobBody()
	return 12 + uint32(len(body.Data))
}

// CanStoreBlobData 判断是否能存储指定大小的BLOB数据
func (bp *BlobPage) CanStoreBlobData(size uint32) bool {
	return size <= MaxBlobSize
}

// GetIndexEntries 实现Page接口
func (bp *BlobPage) GetIndexEntries() []IndexEntry {
	return nil // BLOB页面不包含索引项
}

// GetNextPage 实现Page接口
func (bp *BlobPage) GetNextPage() uint32 {
	body := bp.deserializeBlobBody()
	return body.NextPartPage
}

// GetPageNo 获取页面号
func (bp *BlobPage) GetPageNo() uint32 {
	return bp.GetPageID()
}
