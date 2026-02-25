package record

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
)

/*
VarcharOptimizer 变长字段存储优化器

优化VARCHAR/VARBINARY等变长字段的存储和访问性能。

核心优化：
1. 长度编码优化：1字节/2字节自适应编码
2. 空间预留：预留扩展空间减少重组
3. 内联存储：小字段内联，大字段外存
4. 压缩存储：重复模式压缩

设计要点：
- 智能编码：根据长度自动选择1/2字节编码
- 预留策略：根据更新频率动态调整预留空间
- 内联阈值：平衡行内/行外存储
- 性能监控：统计访问模式优化策略
*/

const (
	// 长度编码阈值
	VarcharLen1ByteMax = 127   // 1字节编码最大长度
	VarcharLen2ByteMax = 16383 // 2字节编码最大长度

	// 内联阈值
	DefaultInlineThreshold = 255 // 默认内联阈值
	MaxInlineSize          = 768 // 最大内联大小

	// 预留策略
	NoReserve     = 0   // 不预留
	SmallReserve  = 16  // 小预留(16字节)
	MediumReserve = 64  // 中预留(64字节)
	LargeReserve  = 256 // 大预留(256字节)

	// 压缩阈值
	CompressionThreshold = 512 // 超过512字节考虑压缩
)

// VarcharOptimizer 变长字段优化器
type VarcharOptimizer struct {
	// 配置
	config *VarcharConfig

	// 统计
	stats *VarcharStats
}

// VarcharConfig 变长字段配置
type VarcharConfig struct {
	InlineThreshold   int    // 内联阈值
	EnableReserve     bool   // 启用空间预留
	ReserveStrategy   string // 预留策略
	EnableCompression bool   // 启用压缩
	EnableAdaptive    bool   // 启用自适应优化
}

// VarcharStats 变长字段统计
type VarcharStats struct {
	totalFields      uint64 // 总字段数
	inlineFields     uint64 // 内联字段数
	externalFields   uint64 // 外存字段数
	compressedFields uint64 // 压缩字段数

	totalSize uint64 // 总大小
	savedSize uint64 // 节省大小

	avgLength uint64 // 平均长度
	maxLength uint64 // 最大长度
}

// VarcharField 变长字段
type VarcharField struct {
	Data         []byte
	Length       int
	Reserved     int // 预留空间
	IsCompressed bool
	IsExternal   bool   // 是否外存
	ExternalPtr  uint64 // 外存指针
}

// NewVarcharOptimizer 创建优化器
func NewVarcharOptimizer(config *VarcharConfig) *VarcharOptimizer {
	if config == nil {
		config = &VarcharConfig{
			InlineThreshold:   DefaultInlineThreshold,
			EnableReserve:     true,
			ReserveStrategy:   "adaptive",
			EnableCompression: true,
			EnableAdaptive:    true,
		}
	}

	return &VarcharOptimizer{
		config: config,
		stats:  &VarcharStats{},
	}
}

// EncodeVarchar 编码变长字段
func (vo *VarcharOptimizer) EncodeVarchar(data []byte) ([]byte, error) {
	length := len(data)
	atomic.AddUint64(&vo.stats.totalFields, 1)
	atomic.AddUint64(&vo.stats.totalSize, uint64(length))

	// 更新统计
	if uint64(length) > atomic.LoadUint64(&vo.stats.maxLength) {
		atomic.StoreUint64(&vo.stats.maxLength, uint64(length))
	}

	// 1. 判断是否需要外存
	if length > vo.config.InlineThreshold {
		atomic.AddUint64(&vo.stats.externalFields, 1)
		return vo.encodeExternal(data)
	}

	atomic.AddUint64(&vo.stats.inlineFields, 1)

	// 2. 判断是否需要压缩
	if vo.config.EnableCompression && length > CompressionThreshold {
		compressed := vo.compress(data)
		if len(compressed) < length {
			atomic.AddUint64(&vo.stats.compressedFields, 1)
			atomic.AddUint64(&vo.stats.savedSize, uint64(length-len(compressed)))
			return vo.encodeWithCompression(compressed, length)
		}
	}

	// 3. 计算预留空间
	reserved := 0
	if vo.config.EnableReserve {
		reserved = vo.calculateReserve(length)
	}

	// 4. 编码长度 + 数据 + 预留
	return vo.encodeInline(data, reserved)
}

// encodeInline 内联编码
func (vo *VarcharOptimizer) encodeInline(data []byte, reserved int) ([]byte, error) {
	length := len(data)
	totalSize := length + reserved

	// 长度编码
	var lengthBytes []byte
	if totalSize <= VarcharLen1ByteMax {
		// 1字节编码
		lengthBytes = []byte{byte(totalSize)}
	} else {
		// 2字节编码，最高位为1
		lengthBytes = make([]byte, 2)
		lengthBytes[0] = byte((totalSize >> 8) | 0x80)
		lengthBytes[1] = byte(totalSize & 0xFF)
	}

	// 组装：长度 + 实际数据 + 预留空间
	result := make([]byte, len(lengthBytes)+totalSize)
	copy(result, lengthBytes)
	copy(result[len(lengthBytes):], data)
	// 预留空间填充0

	return result, nil
}

// encodeExternal 外存编码
func (vo *VarcharOptimizer) encodeExternal(data []byte) ([]byte, error) {
	// 外存字段编码为固定20字节指针
	// [标记1B][长度4B][外存指针15B]

	result := make([]byte, 20)
	result[0] = 0xFF // 外存标记
	binary.BigEndian.PutUint32(result[1:5], uint32(len(data)))

	// 实际应该存储到外存并返回指针
	// 这里简化处理
	externalPtr := uint64(0) // 应该是实际的外存地址
	copy(result[5:], data[:min(len(data), 15)])
	_ = externalPtr

	return result, nil
}

// encodeWithCompression 压缩编码
func (vo *VarcharOptimizer) encodeWithCompression(compressed []byte, originalLen int) ([]byte, error) {
	// [压缩标记1B][原始长度4B][压缩数据]

	result := make([]byte, 5+len(compressed))
	result[0] = 0xFE // 压缩标记
	binary.BigEndian.PutUint32(result[1:5], uint32(originalLen))
	copy(result[5:], compressed)

	return result, nil
}

// DecodeVarchar 解码变长字段
func (vo *VarcharOptimizer) DecodeVarchar(encoded []byte) ([]byte, error) {
	if len(encoded) == 0 {
		return nil, fmt.Errorf("empty encoded data")
	}

	// 检查特殊标记
	if encoded[0] == 0xFF {
		// 外存字段
		return vo.decodeExternal(encoded)
	}

	if encoded[0] == 0xFE {
		// 压缩字段
		return vo.decodeCompressed(encoded)
	}

	// 普通内联字段
	return vo.decodeInline(encoded)
}

// decodeInline 解码内联字段
func (vo *VarcharOptimizer) decodeInline(encoded []byte) ([]byte, error) {
	// 解析长度
	var length int
	var dataStart int

	if encoded[0]&0x80 == 0 {
		// 1字节编码
		length = int(encoded[0])
		dataStart = 1
	} else {
		// 2字节编码
		if len(encoded) < 2 {
			return nil, fmt.Errorf("invalid 2-byte length encoding")
		}
		length = (int(encoded[0]&0x7F) << 8) | int(encoded[1])
		dataStart = 2
	}

	// 提取数据（去除预留空间）
	if dataStart+length > len(encoded) {
		return nil, fmt.Errorf("data truncated")
	}

	// 实际数据长度需要从元数据获取，这里简化处理
	data := encoded[dataStart : dataStart+length]

	return data, nil
}

// decodeExternal 解码外存字段
func (vo *VarcharOptimizer) decodeExternal(encoded []byte) ([]byte, error) {
	if len(encoded) < 20 {
		return nil, fmt.Errorf("invalid external field")
	}

	originalLen := binary.BigEndian.Uint32(encoded[1:5])

	// 实际应该从外存读取
	// 这里简化返回嵌入的数据
	data := make([]byte, originalLen)
	copy(data, encoded[5:])

	return data, nil
}

// decodeCompressed 解码压缩字段
func (vo *VarcharOptimizer) decodeCompressed(encoded []byte) ([]byte, error) {
	if len(encoded) < 5 {
		return nil, fmt.Errorf("invalid compressed field")
	}

	originalLen := binary.BigEndian.Uint32(encoded[1:5])
	compressed := encoded[5:]

	// 解压缩
	decompressed := vo.decompress(compressed, int(originalLen))

	return decompressed, nil
}

// calculateReserve 计算预留空间
func (vo *VarcharOptimizer) calculateReserve(currentLen int) int {
	if !vo.config.EnableReserve {
		return 0
	}

	switch vo.config.ReserveStrategy {
	case "none":
		return NoReserve

	case "small":
		return SmallReserve

	case "medium":
		return MediumReserve

	case "large":
		return LargeReserve

	case "adaptive":
		// 自适应策略：根据当前长度动态调整
		if currentLen < 64 {
			return SmallReserve
		} else if currentLen < 256 {
			return MediumReserve
		} else {
			return LargeReserve
		}

	case "percentage":
		// 百分比策略：预留25%
		return currentLen / 4

	default:
		return NoReserve
	}
}

// compress 压缩数据
func (vo *VarcharOptimizer) compress(data []byte) []byte {
	// 简化实现：RLE压缩
	if len(data) == 0 {
		return data
	}

	compressed := make([]byte, 0, len(data))
	count := 1
	current := data[0]

	for i := 1; i < len(data); i++ {
		if data[i] == current && count < 255 {
			count++
		} else {
			// 写入：[count][value]
			compressed = append(compressed, byte(count), current)
			current = data[i]
			count = 1
		}
	}

	// 写入最后一组
	compressed = append(compressed, byte(count), current)

	return compressed
}

// decompress 解压缩数据
func (vo *VarcharOptimizer) decompress(compressed []byte, expectedLen int) []byte {
	decompressed := make([]byte, 0, expectedLen)

	for i := 0; i < len(compressed); i += 2 {
		if i+1 >= len(compressed) {
			break
		}
		count := int(compressed[i])
		value := compressed[i+1]

		for j := 0; j < count; j++ {
			decompressed = append(decompressed, value)
		}
	}

	return decompressed
}

// UpdateField 更新字段（利用预留空间）
func (vo *VarcharOptimizer) UpdateField(encoded []byte, newData []byte) ([]byte, bool, error) {
	// 检查是否可以原地更新
	oldLen := len(encoded)
	newLen := len(newData)

	// 解码获取预留空间信息
	var reserved int
	if encoded[0]&0x80 == 0 {
		totalSize := int(encoded[0])
		reserved = totalSize - (oldLen - 1)
	} else {
		totalSize := (int(encoded[0]&0x7F) << 8) | int(encoded[1])
		reserved = totalSize - (oldLen - 2)
	}

	// 如果新数据能放入预留空间，原地更新
	if newLen <= oldLen-1+reserved || newLen <= oldLen-2+reserved {
		// 原地更新
		copy(encoded[1:], newData)
		return encoded, true, nil
	}

	// 否则需要重新编码
	newEncoded, err := vo.EncodeVarchar(newData)
	return newEncoded, false, err
}

// GetStats 获取统计
func (vo *VarcharOptimizer) GetStats() *VarcharStats {
	stats := &VarcharStats{}

	stats.totalFields = atomic.LoadUint64(&vo.stats.totalFields)
	stats.inlineFields = atomic.LoadUint64(&vo.stats.inlineFields)
	stats.externalFields = atomic.LoadUint64(&vo.stats.externalFields)
	stats.compressedFields = atomic.LoadUint64(&vo.stats.compressedFields)
	stats.totalSize = atomic.LoadUint64(&vo.stats.totalSize)
	stats.savedSize = atomic.LoadUint64(&vo.stats.savedSize)
	stats.maxLength = atomic.LoadUint64(&vo.stats.maxLength)

	if stats.totalFields > 0 {
		stats.avgLength = stats.totalSize / stats.totalFields
	}

	return stats
}

// GetCompressionRatio 获取压缩率
func (vo *VarcharOptimizer) GetCompressionRatio() float64 {
	total := atomic.LoadUint64(&vo.stats.totalSize)
	saved := atomic.LoadUint64(&vo.stats.savedSize)

	if total == 0 {
		return 0
	}

	return float64(saved) / float64(total) * 100
}

// GetInlineRate 获取内联率
func (vo *VarcharOptimizer) GetInlineRate() float64 {
	total := atomic.LoadUint64(&vo.stats.totalFields)
	inline := atomic.LoadUint64(&vo.stats.inlineFields)

	if total == 0 {
		return 0
	}

	return float64(inline) / float64(total) * 100
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
