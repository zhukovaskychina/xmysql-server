package page

import (
	"bytes"
	"compress/zlib"
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"io"
	"sync"
)

// CompressedPageWrapper 压缩页面包装器
type CompressedPageWrapper struct {
	*BasePageWrapper

	// 并发控制
	dataLock sync.RWMutex

	// 底层的压缩页面实现
	compressedPage *pages.CompressedPage

	// 压缩数据
	compressedData []byte
	originalSize   uint32
	compressedSize uint32

	// 压缩参数
	algorithm uint16 // 1:zlib
	level     uint16 // 压缩级别
}

// NewCompressedPageWrapper 创建压缩页面
func NewCompressedPageWrapper(id, spaceID uint32) *CompressedPageWrapper {
	base := NewBasePageWrapper(id, spaceID, common.FIL_PAGE_TYPE_COMPRESSED)
	compressedPage := pages.NewCompressedPage(spaceID, id, pages.CompressionZLIB)

	p := &CompressedPageWrapper{
		BasePageWrapper: base,
		compressedPage:  compressedPage,
		algorithm:       1, // 默认使用zlib
		level:           6, // 默认压缩级别
	}
	return p
}

// 实现IPageWrapper接口

// ParseFromBytes 从字节数据解析压缩页面
func (cpw *CompressedPageWrapper) ParseFromBytes(data []byte) error {
	cpw.Lock()
	defer cpw.Unlock()

	if err := cpw.BasePageWrapper.ParseFromBytes(data); err != nil {
		return err
	}

	// 解析压缩页面特有的数据
	if err := cpw.compressedPage.Deserialize(data); err != nil {
		return err
	}

	return nil
}

// ToBytes 序列化压缩页面为字节数组
func (cpw *CompressedPageWrapper) ToBytes() ([]byte, error) {
	cpw.RLock()
	defer cpw.RUnlock()

	// 序列化压缩页面
	data := cpw.compressedPage.Serialize()

	// 更新基础包装器的内容
	if len(cpw.content) != len(data) {
		cpw.content = make([]byte, len(data))
	}
	copy(cpw.content, data)

	return data, nil
}

// 压缩页面特有的方法

// SetCompressionLevel 设置压缩级别
func (p *CompressedPageWrapper) SetCompressionLevel(level uint16) {
	p.level = level
}

// GetOriginalSize 获取原始大小
func (p *CompressedPageWrapper) GetOriginalSize() uint32 {
	p.dataLock.RLock()
	defer p.dataLock.RUnlock()

	if p.compressedPage != nil {
		return p.compressedPage.GetOriginalSize()
	}
	return p.originalSize
}

// GetCompressedSize 获取压缩后大小
func (p *CompressedPageWrapper) GetCompressedSize() uint32 {
	p.dataLock.RLock()
	defer p.dataLock.RUnlock()

	if p.compressedPage != nil {
		return p.compressedPage.GetCompressedSize()
	}
	return p.compressedSize
}

// SetData 设置原始数据并压缩
func (cpw *CompressedPageWrapper) SetData(data []byte) error {
	cpw.Lock()
	defer cpw.Unlock()

	// 使用底层实现
	if cpw.compressedPage != nil {
		if err := cpw.compressedPage.CompressData(data); err != nil {
			return err
		}
	} else {
		// 手动压缩
		if err := cpw.compressData(data); err != nil {
			return err
		}
	}

	cpw.MarkDirty()
	return nil
}

// GetOriginalData 获取解压缩后的原始数据
func (cpw *CompressedPageWrapper) GetOriginalData() ([]byte, error) {
	cpw.RLock()
	defer cpw.RUnlock()

	if cpw.compressedPage != nil {
		return cpw.compressedPage.DecompressData()
	}

	// 手动解压缩
	return cpw.decompressData()
}

// GetCompressedData 获取压缩后的数据
func (cpw *CompressedPageWrapper) GetCompressedData() []byte {
	cpw.RLock()
	defer cpw.RUnlock()

	if cpw.compressedPage != nil {
		// 从CompressedPage结构中获取压缩数据
		result := make([]byte, len(cpw.compressedPage.CompressedData))
		copy(result, cpw.compressedPage.CompressedData)
		return result
	}

	result := make([]byte, len(cpw.compressedData))
	copy(result, cpw.compressedData)
	return result
}

// GetCompressionRatio 获取压缩比
func (cpw *CompressedPageWrapper) GetCompressionRatio() float64 {
	cpw.RLock()
	defer cpw.RUnlock()

	if cpw.compressedPage != nil {
		return cpw.compressedPage.GetCompressionRatio()
	}

	if cpw.originalSize == 0 {
		return 1.0
	}
	return float64(cpw.compressedSize) / float64(cpw.originalSize)
}

// GetCompressionAlgorithm 获取压缩算法
func (cpw *CompressedPageWrapper) GetCompressionAlgorithm() pages.CompressionAlgorithm {
	cpw.RLock()
	defer cpw.RUnlock()

	if cpw.compressedPage != nil {
		return cpw.compressedPage.GetCompressionAlgorithm()
	}

	return pages.CompressionZLIB
}

// Validate 验证压缩页面数据完整性
func (cpw *CompressedPageWrapper) Validate() error {
	cpw.RLock()
	defer cpw.RUnlock()

	if cpw.compressedPage != nil {
		return cpw.compressedPage.Validate()
	}

	// 简单验证
	if cpw.originalSize == 0 && len(cpw.compressedData) > 0 {
		return errors.New("invalid compressed data: zero original size")
	}

	return nil
}

// GetCompressedPage 获取底层的压缩页面实现
func (cpw *CompressedPageWrapper) GetCompressedPage() *pages.CompressedPage {
	return cpw.compressedPage
}

// 内部方法：手动压缩数据
func (cpw *CompressedPageWrapper) compressData(data []byte) error {
	cpw.originalSize = uint32(len(data))

	// 压缩数据
	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, int(cpw.level))
	if err != nil {
		return err
	}

	if _, err := w.Write(data); err != nil {
		return err
	}
	w.Close()

	// 保存压缩数据
	cpw.compressedData = buf.Bytes()
	cpw.compressedSize = uint32(len(cpw.compressedData))

	return nil
}

// 内部方法：手动解压缩数据
func (cpw *CompressedPageWrapper) decompressData() ([]byte, error) {
	if len(cpw.compressedData) == 0 {
		return nil, errors.New("no compressed data")
	}

	reader := bytes.NewReader(cpw.compressedData)
	z, err := zlib.NewReader(reader)
	if err != nil {
		return nil, err
	}
	defer z.Close()

	result := make([]byte, cpw.originalSize)
	if _, err := io.ReadFull(z, result); err != nil {
		return nil, err
	}

	return result, nil
}
