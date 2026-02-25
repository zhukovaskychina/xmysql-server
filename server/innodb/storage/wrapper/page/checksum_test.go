package page

import (
	"encoding/binary"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"testing"
)

// TestPageChecksum_CalculateAndValidate 测试校验和计算和验证
func TestPageChecksum_CalculateAndValidate(t *testing.T) {
	// 创建一个页面包装器
	wrapper := NewBasePageWrapper(1, 0, common.FIL_PAGE_INDEX)

	// 填充一些测试数据到content中
	for i := 0; i < len(wrapper.content); i++ {
		wrapper.content[i] = byte(i % 256)
	}

	// 更新校验和
	wrapper.UpdateChecksum()

	// 验证校验和
	if !wrapper.ValidateChecksum() {
		t.Error("Checksum validation failed after UpdateChecksum")
	}

	// 获取存储的校验和
	storedChecksum := wrapper.trailer.GetChecksum()
	if storedChecksum == 0 {
		t.Error("Stored checksum should not be zero")
	}

	// 验证页面头部的校验和字段也被更新了
	headerChecksum := binary.LittleEndian.Uint32(wrapper.content[0:4])
	if uint64(headerChecksum) != storedChecksum {
		t.Errorf("Header checksum (%d) does not match trailer checksum (%d)", headerChecksum, storedChecksum)
	}

	t.Logf("Checksum calculated successfully: %d", storedChecksum)
}

// TestPageChecksum_DetectCorruption 测试校验和能检测到数据损坏
func TestPageChecksum_DetectCorruption(t *testing.T) {
	// 创建一个页面包装器
	wrapper := NewBasePageWrapper(2, 0, common.FIL_PAGE_INDEX)

	// 填充测试数据
	for i := 0; i < len(wrapper.content); i++ {
		wrapper.content[i] = byte(i % 256)
	}

	// 更新校验和
	wrapper.UpdateChecksum()

	// 验证校验和应该通过
	if !wrapper.ValidateChecksum() {
		t.Error("Initial checksum validation should pass")
	}

	// 修改页面内容（模拟数据损坏）
	wrapper.content[100] = ^wrapper.content[100] // 翻转一个字节

	// 验证校验和应该失败
	if wrapper.ValidateChecksum() {
		t.Error("Checksum validation should fail after data corruption")
	}

	t.Log("Checksum successfully detected data corruption")
}

// TestPageChecksum_CRC32Algorithm 测试CRC32算法的正确性
func TestPageChecksum_CRC32Algorithm(t *testing.T) {
	// 创建一个页面包装器
	wrapper := NewBasePageWrapper(3, 0, common.FIL_PAGE_INDEX)

	// 填充已知数据
	for i := 0; i < len(wrapper.content); i++ {
		wrapper.content[i] = 0xAA
	}

	// 更新校验和
	wrapper.UpdateChecksum()

	// 使用PageIntegrityChecker独立计算校验和
	checker := pages.NewPageIntegrityChecker(pages.ChecksumCRC32)
	expectedChecksum := checker.CalculateChecksum(wrapper.content)

	// 获取存储的校验和
	storedChecksum := wrapper.trailer.GetChecksum()

	// 验证两者应该相等
	if uint64(expectedChecksum) != storedChecksum {
		t.Errorf("Checksum mismatch: expected %d, got %d", expectedChecksum, storedChecksum)
	}

	t.Logf("CRC32 checksum verified: %d", storedChecksum)
}

// TestPageChecksum_MultipleUpdates 测试多次更新校验和
func TestPageChecksum_MultipleUpdates(t *testing.T) {
	// 创建一个页面包装器
	wrapper := NewBasePageWrapper(4, 0, common.FIL_PAGE_INDEX)

	// 第一次更新
	for i := 0; i < len(wrapper.content); i++ {
		wrapper.content[i] = byte(i % 256)
	}
	wrapper.UpdateChecksum()
	checksum1 := wrapper.trailer.GetChecksum()

	// 验证第一次校验和
	if !wrapper.ValidateChecksum() {
		t.Error("First checksum validation failed")
	}

	// 修改数据
	wrapper.content[500] = 0xFF

	// 第二次更新
	wrapper.UpdateChecksum()
	checksum2 := wrapper.trailer.GetChecksum()

	// 验证第二次校验和
	if !wrapper.ValidateChecksum() {
		t.Error("Second checksum validation failed")
	}

	// 两次校验和应该不同
	if checksum1 == checksum2 {
		t.Error("Checksums should be different after data modification")
	}

	t.Logf("Multiple updates successful: checksum1=%d, checksum2=%d", checksum1, checksum2)
}

// TestPageChecksum_EmptyPage 测试空页面的校验和
func TestPageChecksum_EmptyPage(t *testing.T) {
	// 创建一个页面包装器
	wrapper := NewBasePageWrapper(5, 0, common.FIL_PAGE_INDEX)

	// 不填充数据，直接更新校验和
	wrapper.UpdateChecksum()

	// 验证校验和
	if !wrapper.ValidateChecksum() {
		t.Error("Empty page checksum validation failed")
	}

	// 空页面的校验和不应该为0（因为有header和trailer）
	storedChecksum := wrapper.trailer.GetChecksum()
	if storedChecksum == 0 {
		t.Error("Empty page checksum should not be zero")
	}

	t.Logf("Empty page checksum: %d", storedChecksum)
}

// TestPageChecksum_DirtyFlag 测试更新校验和时设置脏页标记
func TestPageChecksum_DirtyFlag(t *testing.T) {
	// 创建一个页面包装器
	wrapper := NewBasePageWrapper(6, 0, common.FIL_PAGE_INDEX)

	// 初始状态不应该是脏页
	if wrapper.IsDirty() {
		t.Error("New page should not be dirty")
	}

	// 更新校验和
	wrapper.UpdateChecksum()

	// 更新后应该被标记为脏页
	if !wrapper.IsDirty() {
		t.Error("Page should be marked as dirty after UpdateChecksum")
	}

	t.Log("Dirty flag correctly set after checksum update")
}

// TestPageChecksum_ConcurrentAccess 测试并发访问时的校验和计算
func TestPageChecksum_ConcurrentAccess(t *testing.T) {
	// 创建一个页面包装器
	wrapper := NewBasePageWrapper(7, 0, common.FIL_PAGE_INDEX)

	// 填充数据
	for i := 0; i < len(wrapper.content); i++ {
		wrapper.content[i] = byte(i % 256)
	}

	// 并发更新和验证
	done := make(chan bool, 2)

	// Goroutine 1: 更新校验和
	go func() {
		for i := 0; i < 10; i++ {
			wrapper.UpdateChecksum()
		}
		done <- true
	}()

	// Goroutine 2: 验证校验和
	go func() {
		for i := 0; i < 10; i++ {
			wrapper.ValidateChecksum()
		}
		done <- true
	}()

	// 等待完成
	<-done
	<-done

	// 最终验证
	if !wrapper.ValidateChecksum() {
		t.Error("Final checksum validation failed after concurrent access")
	}

	t.Log("Concurrent access test passed")
}

// TestPageChecksum_PageIntegrityChecker 测试PageIntegrityChecker的集成
func TestPageChecksum_PageIntegrityChecker(t *testing.T) {
	// 创建一个页面包装器
	wrapper := NewBasePageWrapper(8, 0, common.FIL_PAGE_INDEX)

	// 填充数据
	for i := 0; i < len(wrapper.content); i++ {
		wrapper.content[i] = byte((i * 7) % 256)
	}

	// 更新校验和
	wrapper.UpdateChecksum()

	// 使用PageIntegrityChecker验证
	checker := pages.NewPageIntegrityChecker(pages.ChecksumCRC32)
	if err := checker.ValidateChecksum(wrapper.content); err != nil {
		t.Errorf("PageIntegrityChecker validation failed: %v", err)
	}

	// 修改数据
	wrapper.content[1000] = ^wrapper.content[1000]

	// 验证应该失败
	if err := checker.ValidateChecksum(wrapper.content); err == nil {
		t.Error("PageIntegrityChecker should detect corruption")
	}

	t.Log("PageIntegrityChecker integration test passed")
}
