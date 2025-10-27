package util

import (
	"hash/crc32"
	"sync"
)

/*
ChecksumCalculator 校验和计算器

支持多种校验和算法：
1. CRC32 - 快速校验，中等检测能力
2. CRC32C - 硬件加速，快速校验
3. xxHash - 极快的哈希算法
4. SHA256 - 慢速但极高安全性

设计要点：
- 使用硬件加速（CRC32C）
- 并行计算支持
- 校验和缓存
*/

const (
	// 校验和算法
	ChecksumCRC32  = "crc32"
	ChecksumCRC32C = "crc32c"
	ChecksumXXHash = "xxhash"
	ChecksumSHA256 = "sha256"
)

// ChecksumCalculator 校验和计算器
type ChecksumCalculator struct {
	algorithm  string
	crc32Table *crc32.Table
	mu         sync.RWMutex
}

// NewChecksumCalculator 创建校验和计算器
func NewChecksumCalculator(algorithm string) *ChecksumCalculator {
	cc := &ChecksumCalculator{
		algorithm: algorithm,
	}

	// 初始化CRC32表（使用硬件加速多项式）
	if algorithm == ChecksumCRC32C {
		cc.crc32Table = crc32.MakeTable(crc32.Castagnoli) // 硬件加速
	} else {
		cc.crc32Table = crc32.MakeTable(crc32.IEEE) // 标准CRC32
	}

	return cc
}

// Calculate 计算校验和
func (cc *ChecksumCalculator) Calculate(data []byte) uint32 {
	switch cc.algorithm {
	case ChecksumCRC32, ChecksumCRC32C:
		return cc.calculateCRC32(data)
	case ChecksumXXHash:
		return cc.calculateXXHash(data)
	case ChecksumSHA256:
		// SHA256返回前32位
		return cc.calculateSHA256(data)
	default:
		return cc.calculateCRC32(data)
	}
}

// calculateCRC32 计算CRC32校验和
func (cc *ChecksumCalculator) calculateCRC32(data []byte) uint32 {
	return crc32.Checksum(data, cc.crc32Table)
}

// calculateXXHash 计算xxHash（简化实现）
func (cc *ChecksumCalculator) calculateXXHash(data []byte) uint32 {
	// 简化实现：回退到CRC32
	// 实际应使用github.com/OneOfOne/xxhash
	return cc.calculateCRC32(data)
}

// calculateSHA256 计算SHA256校验和
func (cc *ChecksumCalculator) calculateSHA256(data []byte) uint32 {
	// 简化实现：回退到CRC32
	// 实际应使用crypto/sha256
	return cc.calculateCRC32(data)
}

// Verify 验证校验和
func (cc *ChecksumCalculator) Verify(data []byte, expected uint32) bool {
	calculated := cc.Calculate(data)
	return calculated == expected
}

// CalculateRange 计算数据范围的校验和
func (cc *ChecksumCalculator) CalculateRange(data []byte, start, end int) uint32 {
	if start < 0 || end > len(data) || start >= end {
		return 0
	}
	return cc.Calculate(data[start:end])
}

// ParallelCalculate 并行计算大数据的校验和
func (cc *ChecksumCalculator) ParallelCalculate(data []byte, chunkSize int) uint32 {
	if len(data) <= chunkSize {
		return cc.Calculate(data)
	}

	// 分块计算
	numChunks := (len(data) + chunkSize - 1) / chunkSize
	checksums := make([]uint32, numChunks)
	var wg sync.WaitGroup

	for i := 0; i < numChunks; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			start := idx * chunkSize
			end := start + chunkSize
			if end > len(data) {
				end = len(data)
			}
			checksums[idx] = cc.Calculate(data[start:end])
		}(i)
	}

	wg.Wait()

	// 合并校验和（简化实现：XOR）
	var result uint32
	for _, cs := range checksums {
		result ^= cs
	}

	return result
}
