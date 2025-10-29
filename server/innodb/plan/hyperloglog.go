package plan

import (
	"hash"
	"hash/fnv"
	"math"
)

// HyperLogLog HyperLogLog基数估算器
// 用于高效估算不同值数量(NDV)，内存占用固定，误差率<2%
type HyperLogLog struct {
	// 精度参数，决定桶数量 (2^precision)
	precision uint8
	// 寄存器数量 (m = 2^precision)
	m uint32
	// 寄存器数组，存储前导零计数
	registers []uint8
	// alpha常数，用于偏差修正
	alpha float64
}

// NewHyperLogLog 创建HyperLogLog实例
// precision: 精度参数，范围4-16，推荐值14(内存占用16KB，误差约0.81%)
func NewHyperLogLog(precision uint8) *HyperLogLog {
	if precision < 4 || precision > 16 {
		precision = 14 // 默认值
	}

	m := uint32(1 << precision)
	var alpha float64

	// 根据m选择alpha值
	switch m {
	case 16:
		alpha = 0.673
	case 32:
		alpha = 0.697
	case 64:
		alpha = 0.709
	default:
		alpha = 0.7213 / (1 + 1.079/float64(m))
	}

	return &HyperLogLog{
		precision: precision,
		m:         m,
		registers: make([]uint8, m),
		alpha:     alpha,
	}
}

// Add 添加元素到HyperLogLog
func (hll *HyperLogLog) Add(value interface{}) {
	// 计算哈希值
	hash := hll.hash(value)

	// 提取桶索引 (前precision位)
	idx := hash >> (64 - hll.precision)

	// 计算剩余位的前导零数量 + 1
	w := hash << hll.precision
	leadingZeros := hll.countLeadingZeros(w) + 1

	// 更新寄存器(取最大值)
	if leadingZeros > hll.registers[idx] {
		hll.registers[idx] = leadingZeros
	}
}

// Count 估算基数
func (hll *HyperLogLog) Count() int64 {
	// 计算原始估算值
	sum := 0.0
	zeros := 0

	for _, val := range hll.registers {
		sum += 1.0 / math.Pow(2.0, float64(val))
		if val == 0 {
			zeros++
		}
	}

	estimate := hll.alpha * float64(hll.m) * float64(hll.m) / sum

	// 小范围修正
	if estimate <= 2.5*float64(hll.m) {
		if zeros != 0 {
			estimate = float64(hll.m) * math.Log(float64(hll.m)/float64(zeros))
		}
	}

	// 大范围修正
	if estimate > math.Pow(2, 32)/30.0 {
		estimate = -math.Pow(2, 32) * math.Log(1-estimate/math.Pow(2, 32))
	}

	return int64(estimate)
}

// Merge 合并另一个HyperLogLog
func (hll *HyperLogLog) Merge(other *HyperLogLog) error {
	if hll.precision != other.precision {
		return ErrPrecisionMismatch
	}

	for i := range hll.registers {
		if other.registers[i] > hll.registers[i] {
			hll.registers[i] = other.registers[i]
		}
	}

	return nil
}

// Clear 清空HyperLogLog
func (hll *HyperLogLog) Clear() {
	for i := range hll.registers {
		hll.registers[i] = 0
	}
}

// hash 计算值的哈希
func (hll *HyperLogLog) hash(value interface{}) uint64 {
	h := fnv.New64a()

	switch v := value.(type) {
	case int:
		hll.writeInt64(h, int64(v))
	case int8:
		hll.writeInt64(h, int64(v))
	case int16:
		hll.writeInt64(h, int64(v))
	case int32:
		hll.writeInt64(h, int64(v))
	case int64:
		hll.writeInt64(h, v)
	case uint:
		hll.writeUint64(h, uint64(v))
	case uint8:
		hll.writeUint64(h, uint64(v))
	case uint16:
		hll.writeUint64(h, uint64(v))
	case uint32:
		hll.writeUint64(h, uint64(v))
	case uint64:
		hll.writeUint64(h, v)
	case float32:
		hll.writeFloat64(h, float64(v))
	case float64:
		hll.writeFloat64(h, v)
	case string:
		h.Write([]byte(v))
	case []byte:
		h.Write(v)
	default:
		// 其他类型转换为字符串
		h.Write([]byte(toString(value)))
	}

	return h.Sum64()
}

// countLeadingZeros 计算前导零数量
func (hll *HyperLogLog) countLeadingZeros(w uint64) uint8 {
	if w == 0 {
		return 64
	}

	n := uint8(0)
	if w&0xFFFFFFFF00000000 == 0 {
		n += 32
		w <<= 32
	}
	if w&0xFFFF000000000000 == 0 {
		n += 16
		w <<= 16
	}
	if w&0xFF00000000000000 == 0 {
		n += 8
		w <<= 8
	}
	if w&0xF000000000000000 == 0 {
		n += 4
		w <<= 4
	}
	if w&0xC000000000000000 == 0 {
		n += 2
		w <<= 2
	}
	if w&0x8000000000000000 == 0 {
		n += 1
	}

	return n
}

// 辅助方法
func (hll *HyperLogLog) writeInt64(h hash.Hash64, v int64) {
	buf := make([]byte, 8)
	for i := 0; i < 8; i++ {
		buf[i] = byte(v >> (i * 8))
	}
	h.Write(buf)
}

func (hll *HyperLogLog) writeUint64(h hash.Hash64, v uint64) {
	buf := make([]byte, 8)
	for i := 0; i < 8; i++ {
		buf[i] = byte(v >> (i * 8))
	}
	h.Write(buf)
}

func (hll *HyperLogLog) writeFloat64(h hash.Hash64, v float64) {
	bits := math.Float64bits(v)
	hll.writeUint64(h, bits)
}

// 错误定义
var ErrPrecisionMismatch = &StatisticsError{
	Code:    "HLL_PRECISION_MISMATCH",
	Message: "HyperLogLog精度不匹配，无法合并",
}

// StatisticsError 统计信息错误
type StatisticsError struct {
	Code    string
	Message string
}

func (e *StatisticsError) Error() string {
	return e.Message
}
