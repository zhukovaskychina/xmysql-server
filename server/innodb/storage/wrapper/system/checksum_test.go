package system

import (
	"encoding/binary"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"testing"
)

// TestSystemPageChecksum_ValidateChecksum 测试系统页面校验和验证
func TestSystemPageChecksum_ValidateChecksum(t *testing.T) {
	// 创建一个系统页面
	sp := NewBaseSystemPage(0, 1, SystemPageTypeFSP)

	// 填充一些测试数据
	content := sp.GetContent()
	for i := 0; i < len(content); i++ {
		content[i] = byte(i % 256)
	}
	sp.SetContent(content)

	// 更新校验和
	sp.updateChecksum()

	// 验证校验和应该通过
	if !sp.validateChecksum() {
		t.Error("Checksum validation should pass after updateChecksum")
	}

	// 验证header中的校验和被正确设置
	if sp.header.Checksum == 0 {
		t.Error("Header checksum should not be zero")
	}

	t.Logf("System page checksum: %d", sp.header.Checksum)
}

// TestSystemPageChecksum_DetectCorruption 测试系统页面能检测数据损坏
func TestSystemPageChecksum_DetectCorruption(t *testing.T) {
	// 创建一个系统页面
	sp := NewBaseSystemPage(0, 2, SystemPageTypeIBuf)

	// 填充测试数据
	content := sp.GetContent()
	for i := 0; i < len(content); i++ {
		content[i] = byte((i * 3) % 256)
	}
	sp.SetContent(content)

	// 更新校验和
	sp.updateChecksum()

	// 验证应该通过
	if !sp.validateChecksum() {
		t.Error("Initial validation should pass")
	}

	// 修改数据（模拟损坏）
	content = sp.GetContent()
	content[200] = ^content[200]
	sp.SetContent(content)

	// 验证应该失败
	if sp.validateChecksum() {
		t.Error("Validation should fail after data corruption")
	}

	t.Log("System page successfully detected data corruption")
}

// TestSystemPageChecksum_UpdateInWrite 测试Write方法中的校验和更新
func TestSystemPageChecksum_UpdateInWrite(t *testing.T) {
	// 创建一个系统页面
	sp := NewBaseSystemPage(0, 3, SystemPageTypeTrx)

	// 填充数据
	content := sp.GetContent()
	for i := 0; i < len(content); i++ {
		content[i] = byte((i * 5) % 256)
	}
	sp.SetContent(content)

	// 直接调用updateChecksum（避免Write方法的其他依赖）
	sp.updateChecksum()

	// 验证校验和
	if !sp.validateChecksum() {
		t.Error("Checksum validation should pass after updateChecksum")
	}

	// 验证页面头部的校验和字段
	content = sp.GetContent()
	headerChecksum := binary.LittleEndian.Uint32(content[0:4])
	if headerChecksum == 0 {
		t.Error("Header checksum field should not be zero")
	}

	t.Logf("updateChecksum correctly updated checksum: %d", headerChecksum)
}

// TestSystemPageChecksum_Validate 测试Validate方法包含校验和验证
func TestSystemPageChecksum_Validate(t *testing.T) {
	// 创建一个系统页面
	sp := NewBaseSystemPage(0, 4, SystemPageTypeDict)

	// 填充数据
	content := sp.GetContent()
	for i := 0; i < len(content); i++ {
		content[i] = byte((i * 7) % 256)
	}
	sp.SetContent(content)

	// 更新校验和
	sp.updateChecksum()

	// Validate方法应该通过（包括校验和验证）
	if err := sp.Validate(); err != nil {
		t.Errorf("Validate should pass with correct checksum: %v", err)
	}

	// 修改数据
	content = sp.GetContent()
	content[300] = ^content[300]
	sp.SetContent(content)

	// Validate方法应该失败
	if err := sp.Validate(); err == nil {
		t.Error("Validate should fail with corrupted data")
	} else if err != ErrCorruptedPage {
		t.Errorf("Expected ErrCorruptedPage, got: %v", err)
	}

	t.Log("Validate method correctly checks checksum")
}

// TestSystemPageChecksum_CRC32Consistency 测试CRC32算法的一致性
func TestSystemPageChecksum_CRC32Consistency(t *testing.T) {
	// 创建一个系统页面
	sp := NewBaseSystemPage(0, 5, SystemPageTypeXDES)

	// 填充已知数据
	content := sp.GetContent()
	for i := 0; i < len(content); i++ {
		content[i] = 0x55
	}
	sp.SetContent(content)

	// 更新校验和
	sp.updateChecksum()

	// 使用PageIntegrityChecker独立计算
	checker := pages.NewPageIntegrityChecker(pages.ChecksumCRC32)
	content = sp.GetContent()
	expectedChecksum := checker.CalculateChecksum(content)

	// 验证header中的校验和
	if sp.header.Checksum != uint64(expectedChecksum) {
		t.Errorf("Checksum mismatch: expected %d, got %d", expectedChecksum, sp.header.Checksum)
	}

	// 验证页面头部字段
	headerChecksum := binary.LittleEndian.Uint32(content[0:4])
	if uint64(headerChecksum) != sp.header.Checksum {
		t.Errorf("Header field checksum (%d) != header.Checksum (%d)", headerChecksum, sp.header.Checksum)
	}

	t.Logf("CRC32 consistency verified: %d", sp.header.Checksum)
}

// TestSystemPageChecksum_MultipleTypes 测试不同类型系统页面的校验和
func TestSystemPageChecksum_MultipleTypes(t *testing.T) {
	types := []struct {
		name     string
		pageType SystemPageType
	}{
		{"FSP", SystemPageTypeFSP},
		{"IBuf", SystemPageTypeIBuf},
		{"Inode", SystemPageTypeInode},
		{"Trx", SystemPageTypeTrx},
		{"XDES", SystemPageTypeXDES},
		{"Dict", SystemPageTypeDict},
	}

	for _, tc := range types {
		t.Run(tc.name, func(t *testing.T) {
			// 创建系统页面
			sp := NewBaseSystemPage(0, 10, tc.pageType)

			// 填充数据
			content := sp.GetContent()
			for i := 0; i < len(content); i++ {
				content[i] = byte((i + int(tc.pageType)) % 256)
			}
			sp.SetContent(content)

			// 更新校验和
			sp.updateChecksum()

			// 验证
			if !sp.validateChecksum() {
				t.Errorf("Checksum validation failed for page type %s", tc.name)
			}

			t.Logf("Page type %s checksum: %d", tc.name, sp.header.Checksum)
		})
	}
}

// TestSystemPageChecksum_EmptyPage 测试空系统页面的校验和
func TestSystemPageChecksum_EmptyPage(t *testing.T) {
	// 创建一个系统页面（不填充数据）
	sp := NewBaseSystemPage(0, 6, SystemPageTypeFSP)

	// 更新校验和
	sp.updateChecksum()

	// 验证
	if !sp.validateChecksum() {
		t.Error("Empty page checksum validation failed")
	}

	// 空页面的校验和不应该为0
	if sp.header.Checksum == 0 {
		t.Error("Empty page checksum should not be zero")
	}

	t.Logf("Empty system page checksum: %d", sp.header.Checksum)
}

// TestSystemPageChecksum_TrailerConsistency 测试trailer中的校验和一致性
func TestSystemPageChecksum_TrailerConsistency(t *testing.T) {
	// 创建一个系统页面
	sp := NewBaseSystemPage(0, 8, SystemPageTypeInode)

	// 填充数据
	content := sp.GetContent()
	for i := 0; i < len(content); i++ {
		content[i] = byte((i * 13) % 256)
	}
	sp.SetContent(content)

	// 更新校验和
	sp.updateChecksum()

	// 获取header中的校验和
	content = sp.GetContent()
	headerChecksum := binary.LittleEndian.Uint32(content[0:4])

	// 获取trailer中的校验和（最后8字节的前4字节）
	trailerOffset := len(content) - 8
	trailerChecksum := binary.LittleEndian.Uint32(content[trailerOffset : trailerOffset+4])

	// 两者应该相等
	if headerChecksum != trailerChecksum {
		t.Errorf("Header checksum (%d) != Trailer checksum (%d)", headerChecksum, trailerChecksum)
	}

	// 都应该等于header.Checksum
	if uint64(headerChecksum) != sp.header.Checksum {
		t.Errorf("Header field (%d) != header.Checksum (%d)", headerChecksum, sp.header.Checksum)
	}

	t.Logf("Trailer consistency verified: header=%d, trailer=%d, struct=%d",
		headerChecksum, trailerChecksum, sp.header.Checksum)
}
