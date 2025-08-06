package pages

import (
	"errors"
	"hash/crc32"
)

var (
	ErrChecksumMismatch = errors.New("checksum mismatch")
)

// ChecksumAlgorithm 校验和算法类型
type ChecksumAlgorithm int

const (
	ChecksumCRC32 ChecksumAlgorithm = iota
	ChecksumInnoDB
)

// PageIntegrityChecker 页面完整性检查器
type PageIntegrityChecker struct {
	checksumAlgorithm ChecksumAlgorithm
}

// NewPageIntegrityChecker 创建页面完整性检查器
func NewPageIntegrityChecker(algorithm ChecksumAlgorithm) *PageIntegrityChecker {
	return &PageIntegrityChecker{
		checksumAlgorithm: algorithm,
	}
}

// CalculateChecksum 计算页面校验和
func (pic *PageIntegrityChecker) CalculateChecksum(page []byte) uint32 {
	if len(page) < FileHeaderSize+8 {
		return 0
	}

	switch pic.checksumAlgorithm {
	case ChecksumCRC32:
		return pic.calculateCRC32(page)
	case ChecksumInnoDB:
		return pic.calculateInnoDBChecksum(page)
	default:
		return pic.calculateCRC32(page)
	}
}

// calculateCRC32 计算CRC32校验和
func (pic *PageIntegrityChecker) calculateCRC32(page []byte) uint32 {
	// 跳过校验和字段本身(前4字节)和文件尾部校验和(后8字节)
	data := make([]byte, 0, len(page)-12)
	data = append(data, page[4:len(page)-8]...)

	return crc32.ChecksumIEEE(data)
}

// calculateInnoDBChecksum 计算InnoDB风格校验和
func (pic *PageIntegrityChecker) calculateInnoDBChecksum(page []byte) uint32 {
	// InnoDB使用简单的累加校验和
	var checksum uint32

	// 跳过校验和字段，计算其余部分
	for i := 4; i < len(page)-8; i += 4 {
		if i+4 <= len(page)-8 {
			word := uint32(page[i]) | uint32(page[i+1])<<8 |
				uint32(page[i+2])<<16 | uint32(page[i+3])<<24
			checksum += word
		}
	}

	return checksum
}

// ValidateChecksum 验证页面校验和
func (pic *PageIntegrityChecker) ValidateChecksum(page []byte) error {
	if len(page) < FileHeaderSize+8 {
		return ErrPageCorrupted
	}

	// 从页面头部获取存储的校验和
	storedChecksum := uint32(page[0]) | uint32(page[1])<<8 |
		uint32(page[2])<<16 | uint32(page[3])<<24

	// 计算实际校验和
	calculatedChecksum := pic.CalculateChecksum(page)

	if storedChecksum != calculatedChecksum {
		return ErrChecksumMismatch
	}

	return nil
}

// ValidatePage 验证页面完整性
func (pic *PageIntegrityChecker) ValidatePage(page []byte) error {
	if len(page) == 0 {
		return ErrPageCorrupted
	}

	// 检查页面大小
	if len(page) < FileHeaderSize+8 {
		return ErrPageCorrupted
	}

	// 验证校验和
	if err := pic.ValidateChecksum(page); err != nil {
		return err
	}

	// 验证页面头部结构
	if err := pic.validatePageHeader(page); err != nil {
		return err
	}

	return nil
}

// validatePageHeader 验证页面头部
func (pic *PageIntegrityChecker) validatePageHeader(page []byte) error {
	if len(page) < FileHeaderSize {
		return ErrPageCorrupted
	}

	// 检查页面类型是否有效
	pageType := int16(page[24]) | int16(page[25])<<8
	if !pic.isValidPageType(pageType) {
		return ErrPageCorrupted
	}

	return nil
}

// isValidPageType 检查页面类型是否有效
func (pic *PageIntegrityChecker) isValidPageType(pageType int16) bool {
	validTypes := []int16{0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 17, 18, 19}
	for _, validType := range validTypes {
		if pageType == validType {
			return true
		}
	}
	return false
}

// IsPageCorrupted 检查页面是否损坏
func (pic *PageIntegrityChecker) IsPageCorrupted(page []byte) bool {
	return pic.ValidatePage(page) != nil
}

// RepairPage 尝试修复页面(基础实现)
func (pic *PageIntegrityChecker) RepairPage(page []byte) error {
	if len(page) < FileHeaderSize+8 {
		return ErrPageCorrupted
	}

	// 重新计算并设置校验和
	checksum := pic.CalculateChecksum(page)
	page[0] = byte(checksum)
	page[1] = byte(checksum >> 8)
	page[2] = byte(checksum >> 16)
	page[3] = byte(checksum >> 24)

	// 同时更新文件尾部的校验和
	trailerOffset := len(page) - 8
	page[trailerOffset] = byte(checksum)
	page[trailerOffset+1] = byte(checksum >> 8)
	page[trailerOffset+2] = byte(checksum >> 16)
	page[trailerOffset+3] = byte(checksum >> 24)

	return nil
}
