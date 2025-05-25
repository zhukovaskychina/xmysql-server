/*
FIL_PAGE_TYPE_COMPRESSED页面详细说明

基本属性：
- 页面类型：FIL_PAGE_TYPE_COMPRESSED
- 类型编码：0x000B（十进制11）
- 所属模块：InnoDB页面压缩
- 页面作用：存储经过压缩的页面数据

使用场景：
1. 当开启页面压缩功能时，InnoDB会将页面数据进行压缩存储
2. 减少磁盘I/O和存储空间消耗
3. 在内存中自动解压缩，透明的访问接口

页面结构：
- File Header (38字节)
- Compression Header (16字节):
  - Original Size: 原始未压缩大小 (4字节)
  - Compressed Size: 压缩后大小 (4字节)
  - Compression Algorithm: 压缩算法类型 (2字节)
  - Checksum: 压缩数据校验和 (4字节)
  - Reserved: 保留字段 (2字节)
- Compressed Data (变长): 压缩后的数据
- Padding (变长): 填充数据
- File Trailer (8字节)

压缩算法：
- ZLIB: 通用压缩算法
- LZ4: 快速压缩算法
- SNAPPY: Google压缩算法
*/

package pages

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"io"
	"xmysql-server/server/common"
)

// 压缩页面常量
const (
	CompressionHeaderSize = 16    // 压缩头部大小
	MaxCompressedDataSize = 16322 // 最大压缩数据大小 (16384 - 38 - 16 - 8)
)

// 压缩算法类型
type CompressionAlgorithm uint16

const (
	CompressionNone   CompressionAlgorithm = 0
	CompressionZLIB   CompressionAlgorithm = 1
	CompressionLZ4    CompressionAlgorithm = 2
	CompressionSnappy CompressionAlgorithm = 3
)

// 压缩页面错误定义
var (
	ErrCompressionFailed     = errors.New("压缩失败")
	ErrDecompressionFailed   = errors.New("解压缩失败")
	ErrUnsupportedAlgorithm  = errors.New("不支持的压缩算法")
	ErrInvalidCompressedData = errors.New("无效的压缩数据")
)

// CompressionHeader 压缩页面头部结构
type CompressionHeader struct {
	OriginalSize   uint32               // 原始未压缩大小
	CompressedSize uint32               // 压缩后大小
	Algorithm      CompressionAlgorithm // 压缩算法类型
	Checksum       uint32               // 压缩数据校验和
	Reserved       uint16               // 保留字段
}

// CompressedPage 压缩页面结构
type CompressedPage struct {
	FileHeader        FileHeader        // 文件头部 (38字节)
	CompressionHeader CompressionHeader // 压缩头部 (16字节)
	CompressedData    []byte            // 压缩数据 (变长)
	Padding           []byte            // 填充数据 (变长)
	FileTrailer       FileTrailer       // 文件尾部 (8字节)
}

// NewCompressedPage 创建新的压缩页面
func NewCompressedPage(spaceID, pageNo uint32, algorithm CompressionAlgorithm) *CompressedPage {
	page := &CompressedPage{
		FileHeader: NewFileHeader(),
		CompressionHeader: CompressionHeader{
			OriginalSize:   0,
			CompressedSize: 0,
			Algorithm:      algorithm,
			Checksum:       0,
			Reserved:       0,
		},
		CompressedData: make([]byte, 0),
		Padding:        make([]byte, 0),
		FileTrailer:    NewFileTrailer(),
	}

	// 设置页面头部信息
	page.FileHeader.WritePageOffset(pageNo)
	page.FileHeader.WritePageFileType(int16(common.FIL_PAGE_TYPE_COMPRESSED))
	page.FileHeader.WritePageArch(spaceID)

	return page
}

// CompressData 压缩数据
func (cp *CompressedPage) CompressData(originalData []byte) error {
	if len(originalData) == 0 {
		return ErrInvalidCompressedData
	}

	var compressedData []byte
	var err error

	switch cp.CompressionHeader.Algorithm {
	case CompressionZLIB:
		compressedData, err = cp.compressWithZLIB(originalData)
	case CompressionLZ4:
		return ErrUnsupportedAlgorithm // LZ4暂未实现
	case CompressionSnappy:
		return ErrUnsupportedAlgorithm // Snappy暂未实现
	default:
		return ErrUnsupportedAlgorithm
	}

	if err != nil {
		return err
	}

	// 更新头部信息
	cp.CompressionHeader.OriginalSize = uint32(len(originalData))
	cp.CompressionHeader.CompressedSize = uint32(len(compressedData))
	cp.CompressionHeader.Checksum = cp.calculateChecksum(compressedData)

	// 存储压缩数据
	cp.CompressedData = compressedData

	// 计算需要的填充大小
	usedSize := FileHeaderSize + CompressionHeaderSize + len(compressedData) + FileTrailerSize
	paddingSize := common.PageSize - usedSize
	if paddingSize > 0 {
		cp.Padding = make([]byte, paddingSize)
	}

	return nil
}

// DecompressData 解压缩数据
func (cp *CompressedPage) DecompressData() ([]byte, error) {
	if len(cp.CompressedData) == 0 {
		return nil, ErrInvalidCompressedData
	}

	// 验证校验和
	if cp.calculateChecksum(cp.CompressedData) != cp.CompressionHeader.Checksum {
		return nil, ErrInvalidCompressedData
	}

	var originalData []byte
	var err error

	switch cp.CompressionHeader.Algorithm {
	case CompressionZLIB:
		originalData, err = cp.decompressWithZLIB(cp.CompressedData)
	case CompressionLZ4:
		return nil, ErrUnsupportedAlgorithm // LZ4暂未实现
	case CompressionSnappy:
		return nil, ErrUnsupportedAlgorithm // Snappy暂未实现
	default:
		return nil, ErrUnsupportedAlgorithm
	}

	if err != nil {
		return nil, err
	}

	// 验证解压缩后的大小
	if uint32(len(originalData)) != cp.CompressionHeader.OriginalSize {
		return nil, ErrDecompressionFailed
	}

	return originalData, nil
}

// compressWithZLIB 使用ZLIB压缩数据
func (cp *CompressedPage) compressWithZLIB(data []byte) ([]byte, error) {
	var buf bytes.Buffer

	writer := zlib.NewWriter(&buf)
	defer writer.Close()

	_, err := writer.Write(data)
	if err != nil {
		return nil, ErrCompressionFailed
	}

	err = writer.Close()
	if err != nil {
		return nil, ErrCompressionFailed
	}

	return buf.Bytes(), nil
}

// decompressWithZLIB 使用ZLIB解压缩数据
func (cp *CompressedPage) decompressWithZLIB(data []byte) ([]byte, error) {
	reader, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, ErrDecompressionFailed
	}
	defer reader.Close()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, reader)
	if err != nil {
		return nil, ErrDecompressionFailed
	}

	return buf.Bytes(), nil
}

// calculateChecksum 计算校验和
func (cp *CompressedPage) calculateChecksum(data []byte) uint32 {
	// 简单的校验和计算，实际应该使用更复杂的算法
	var checksum uint32
	for _, b := range data {
		checksum += uint32(b)
	}
	return checksum
}

// GetCompressionRatio 获取压缩比率
func (cp *CompressedPage) GetCompressionRatio() float64 {
	if cp.CompressionHeader.OriginalSize == 0 {
		return 0.0
	}
	return float64(cp.CompressionHeader.CompressedSize) / float64(cp.CompressionHeader.OriginalSize)
}

// GetOriginalSize 获取原始大小
func (cp *CompressedPage) GetOriginalSize() uint32 {
	return cp.CompressionHeader.OriginalSize
}

// GetCompressedSize 获取压缩后大小
func (cp *CompressedPage) GetCompressedSize() uint32 {
	return cp.CompressionHeader.CompressedSize
}

// GetCompressionAlgorithm 获取压缩算法
func (cp *CompressedPage) GetCompressionAlgorithm() CompressionAlgorithm {
	return cp.CompressionHeader.Algorithm
}

// Serialize 序列化页面为字节数组
func (cp *CompressedPage) Serialize() []byte {
	data := make([]byte, common.PageSize)
	offset := 0

	// 序列化文件头部
	copy(data[offset:], cp.serializeFileHeader())
	offset += FileHeaderSize

	// 序列化压缩头部
	copy(data[offset:], cp.serializeCompressionHeader())
	offset += CompressionHeaderSize

	// 序列化压缩数据
	copy(data[offset:], cp.CompressedData)
	offset += len(cp.CompressedData)

	// 序列化填充数据
	copy(data[offset:], cp.Padding)
	offset += len(cp.Padding)

	// 序列化文件尾部
	copy(data[len(data)-FileTrailerSize:], cp.serializeFileTrailer())

	return data
}

// Deserialize 从字节数组反序列化页面
func (cp *CompressedPage) Deserialize(data []byte) error {
	if len(data) != common.PageSize {
		return ErrInvalidPageSize
	}

	offset := 0

	// 反序列化文件头部
	if err := cp.deserializeFileHeader(data[offset : offset+FileHeaderSize]); err != nil {
		return err
	}
	offset += FileHeaderSize

	// 反序列化压缩头部
	if err := cp.deserializeCompressionHeader(data[offset : offset+CompressionHeaderSize]); err != nil {
		return err
	}
	offset += CompressionHeaderSize

	// 反序列化压缩数据
	compressedDataSize := int(cp.CompressionHeader.CompressedSize)
	cp.CompressedData = make([]byte, compressedDataSize)
	copy(cp.CompressedData, data[offset:offset+compressedDataSize])
	offset += compressedDataSize

	// 反序列化填充数据
	paddingSize := common.PageSize - FileHeaderSize - CompressionHeaderSize - compressedDataSize - FileTrailerSize
	if paddingSize > 0 {
		cp.Padding = make([]byte, paddingSize)
		copy(cp.Padding, data[offset:offset+paddingSize])
	}

	// 反序列化文件尾部
	if err := cp.deserializeFileTrailer(data[len(data)-FileTrailerSize:]); err != nil {
		return err
	}

	return nil
}

// serializeFileHeader 序列化文件头部
func (cp *CompressedPage) serializeFileHeader() []byte {
	// 实现文件头部序列化逻辑
	data := make([]byte, FileHeaderSize)
	// 这里应该包含具体的序列化逻辑
	return data
}

// serializeCompressionHeader 序列化压缩头部
func (cp *CompressedPage) serializeCompressionHeader() []byte {
	data := make([]byte, CompressionHeaderSize)

	binary.LittleEndian.PutUint32(data[0:], cp.CompressionHeader.OriginalSize)
	binary.LittleEndian.PutUint32(data[4:], cp.CompressionHeader.CompressedSize)
	binary.LittleEndian.PutUint16(data[8:], uint16(cp.CompressionHeader.Algorithm))
	binary.LittleEndian.PutUint32(data[10:], cp.CompressionHeader.Checksum)
	binary.LittleEndian.PutUint16(data[14:], cp.CompressionHeader.Reserved)

	return data
}

// serializeFileTrailer 序列化文件尾部
func (cp *CompressedPage) serializeFileTrailer() []byte {
	// 实现文件尾部序列化逻辑
	data := make([]byte, FileTrailerSize)
	// 这里应该包含具体的序列化逻辑
	return data
}

// deserializeFileHeader 反序列化文件头部
func (cp *CompressedPage) deserializeFileHeader(data []byte) error {
	// 实现文件头部反序列化逻辑
	return nil
}

// deserializeCompressionHeader 反序列化压缩头部
func (cp *CompressedPage) deserializeCompressionHeader(data []byte) error {
	if len(data) < CompressionHeaderSize {
		return ErrInvalidPageSize
	}

	cp.CompressionHeader.OriginalSize = binary.LittleEndian.Uint32(data[0:])
	cp.CompressionHeader.CompressedSize = binary.LittleEndian.Uint32(data[4:])
	cp.CompressionHeader.Algorithm = CompressionAlgorithm(binary.LittleEndian.Uint16(data[8:]))
	cp.CompressionHeader.Checksum = binary.LittleEndian.Uint32(data[10:])
	cp.CompressionHeader.Reserved = binary.LittleEndian.Uint16(data[14:])

	return nil
}

// deserializeFileTrailer 反序列化文件尾部
func (cp *CompressedPage) deserializeFileTrailer(data []byte) error {
	// 实现文件尾部反序列化逻辑
	return nil
}

// Validate 验证页面数据完整性
func (cp *CompressedPage) Validate() error {
	// 验证压缩头部
	if cp.CompressionHeader.OriginalSize == 0 {
		return ErrInvalidCompressedData
	}

	if cp.CompressionHeader.CompressedSize == 0 {
		return ErrInvalidCompressedData
	}

	// 验证压缩数据长度
	if uint32(len(cp.CompressedData)) != cp.CompressionHeader.CompressedSize {
		return ErrInvalidCompressedData
	}

	// 验证校验和
	if cp.calculateChecksum(cp.CompressedData) != cp.CompressionHeader.Checksum {
		return ErrInvalidCompressedData
	}

	return nil
}

// GetFileHeader 获取文件头部
func (cp *CompressedPage) GetFileHeader() FileHeader {
	return cp.FileHeader
}

// GetFileTrailer 获取文件尾部
func (cp *CompressedPage) GetFileTrailer() FileTrailer {
	return cp.FileTrailer
}

// GetSerializeBytes 获取序列化后的字节数组
func (cp *CompressedPage) GetSerializeBytes() []byte {
	return cp.Serialize()
}

// LoadFileHeader 加载文件头部
func (cp *CompressedPage) LoadFileHeader(content []byte) {
	cp.deserializeFileHeader(content)
}

// LoadFileTrailer 加载文件尾部
func (cp *CompressedPage) LoadFileTrailer(content []byte) {
	cp.deserializeFileTrailer(content)
}
