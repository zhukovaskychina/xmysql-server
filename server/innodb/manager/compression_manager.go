package manager

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

// CompressionManager 管理页面压缩
type CompressionManager struct {
	mu sync.RWMutex

	// 压缩设置映射: space_id -> compression_settings
	spaceSettings map[uint32]*CompressionSettings

	// 压缩统计
	stats CompressionStats

	// 压缩缓冲池
	bufferPool sync.Pool
}

// CompressionSettings 表示压缩设置
type CompressionSettings struct {
	SpaceID     uint32  // 表空间ID
	Method      uint8   // 压缩方法
	Level       uint8   // 压缩级别
	BlockSize   uint32  // 压缩块大小
	MinSavings  float64 // 最小压缩率
	MaxFailures uint32  // 最大失败次数
}

// CompressionStats 表示压缩统计信息
type CompressionStats struct {
	TotalPages      uint64  // 总页面数
	CompressedPages uint64  // 压缩页面数
	TotalSize       uint64  // 总大小
	CompressedSize  uint64  // 压缩后大小
	FailureCount    uint64  // 压缩失败次数
	AvgSavings      float64 // 平均压缩率
}

// 压缩方法常量
const (
	COMPRESSION_NONE uint8 = iota // 不压缩
	COMPRESSION_ZLIB              // zlib压缩
)

// 压缩级别常量
const (
	COMPRESSION_LEVEL_NONE    uint8 = 0 // 不压缩
	COMPRESSION_LEVEL_FASTEST uint8 = 1 // 最快压缩
	COMPRESSION_LEVEL_DEFAULT uint8 = 6 // 默认压缩
	COMPRESSION_LEVEL_BEST    uint8 = 9 // 最佳压缩
)

// 页面头部魔数
var compressedPageMagic = []byte{0xC0, 0x4D, 0x50, 0x52} // "CMPR"

// NewCompressionManager 创建压缩管理器
func NewCompressionManager() *CompressionManager {
	return &CompressionManager{
		spaceSettings: make(map[uint32]*CompressionSettings),
		bufferPool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

// SetCompressionSettings 设置表空间的压缩设置
func (cm *CompressionManager) SetCompressionSettings(spaceID uint32, settings *CompressionSettings) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.spaceSettings[spaceID] = settings
}

// GetCompressionSettings 获取表空间的压缩设置
func (cm *CompressionManager) GetCompressionSettings(spaceID uint32) *CompressionSettings {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.spaceSettings[spaceID]
}

// CompressPage 压缩页面内容
func (cm *CompressionManager) CompressPage(spaceID uint32, pageNo uint32, data []byte) ([]byte, error) {
	settings := cm.GetCompressionSettings(spaceID)
	if settings == nil || settings.Method == COMPRESSION_NONE {
		return data, nil
	}

	// 获取缓冲区
	buf := cm.bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer cm.bufferPool.Put(buf)

	// 写入魔数
	buf.Write(compressedPageMagic)

	// 写入原始大小
	binary.Write(buf, binary.BigEndian, uint32(len(data)))

	var compressed []byte
	var err error

	switch settings.Method {
	case COMPRESSION_ZLIB:
		compressed, err = cm.compressZlib(data, settings.Level)
	default:
		return nil, errors.New("unsupported compression method")
	}

	if err != nil {
		cm.stats.FailureCount++
		return nil, err
	}

	// 检查压缩效果
	savings := 1 - float64(len(compressed))/float64(len(data))
	if savings < settings.MinSavings {
		return data, nil
	}

	// 写入压缩数据
	buf.Write(compressed)

	// 更新统计信息
	cm.updateStats(len(data), len(compressed))

	return buf.Bytes(), nil
}

// DecompressPage 解压页面内容
func (cm *CompressionManager) DecompressPage(spaceID uint32, pageNo uint32, data []byte) ([]byte, error) {
	settings := cm.GetCompressionSettings(spaceID)
	if settings == nil || settings.Method == COMPRESSION_NONE {
		return data, nil
	}

	// 检查魔数
	if len(data) < len(compressedPageMagic) || !bytes.Equal(data[:len(compressedPageMagic)], compressedPageMagic) {
		return data, nil // 未压缩的页面
	}

	// 读取原始大小
	originalSize := binary.BigEndian.Uint32(data[len(compressedPageMagic) : len(compressedPageMagic)+4])
	compressedData := data[len(compressedPageMagic)+4:]

	switch settings.Method {
	case COMPRESSION_ZLIB:
		return cm.decompressZlib(compressedData, int(originalSize))
	default:
		return nil, errors.New("unsupported compression method")
	}
}

// compressZlib 使用zlib压缩数据
func (cm *CompressionManager) compressZlib(data []byte, level uint8) ([]byte, error) {
	buf := cm.bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer cm.bufferPool.Put(buf)

	writer, err := zlib.NewWriterLevel(buf, int(level))
	if err != nil {
		return nil, err
	}

	if _, err := writer.Write(data); err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// decompressZlib 使用zlib解压数据
func (cm *CompressionManager) decompressZlib(data []byte, originalSize int) ([]byte, error) {
	reader, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	result := make([]byte, originalSize)
	if _, err := io.ReadFull(reader, result); err != nil {
		return nil, err
	}

	return result, nil
}

// updateStats 更新压缩统计信息
func (cm *CompressionManager) updateStats(originalSize, compressedSize int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.stats.TotalPages++
	cm.stats.CompressedPages++
	cm.stats.TotalSize += uint64(originalSize)
	cm.stats.CompressedSize += uint64(compressedSize)
	cm.stats.AvgSavings = 1 - float64(cm.stats.CompressedSize)/float64(cm.stats.TotalSize)
}

// GetStats 获取压缩统计信息
func (cm *CompressionManager) GetStats() CompressionStats {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.stats
}

// Close 关闭压缩管理器
func (cm *CompressionManager) Close() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 清理资源
	cm.spaceSettings = nil
	return nil
}
