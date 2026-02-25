package extent

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// TestBaseExtentDefragment 测试BaseExtent碎片整理
func TestBaseExtentDefragment(t *testing.T) {
	// 创建extent
	ext := NewBaseExtent(1, 0, basic.ExtentTypeData)

	// 分配一些不连续的页面
	pages := []uint32{0, 2, 5, 10, 15}
	for _, pageNo := range pages {
		ext.pages[pageNo] = true
		ext.pageList = append(ext.pageList, pageNo)
		ext.header.PageCount++
	}

	// 执行碎片整理前的状态
	if len(ext.pageList) != 5 {
		t.Errorf("Expected 5 pages, got %d", len(ext.pageList))
	}

	// 执行碎片整理
	err := ext.Defragment()
	if err != nil {
		t.Fatalf("Defragment failed: %v", err)
	}

	// 验证页面列表已排序
	expectedOrder := []uint32{0, 2, 5, 10, 15}
	for i, pageNo := range ext.pageList {
		if pageNo != expectedOrder[i] {
			t.Errorf("Page %d: expected %d, got %d", i, expectedOrder[i], pageNo)
		}
	}

	// 验证碎片页数（4个不连续的间隙）
	if ext.stats.FragPages != 4 {
		t.Errorf("Expected 4 frag pages, got %d", ext.stats.FragPages)
	}

	// 验证状态
	if ext.header.State != basic.ExtentStatePartial {
		t.Errorf("Expected partial state, got %v", ext.header.State)
	}

	// 验证LastDefragged已更新
	if ext.stats.LastDefragged == 0 {
		t.Error("LastDefragged should be updated")
	}

	t.Logf("BaseExtent defragmentation successful: %d pages, %d fragments",
		len(ext.pageList), ext.stats.FragPages)
}

// TestBaseExtentDefragmentEmpty 测试空extent的碎片整理
func TestBaseExtentDefragmentEmpty(t *testing.T) {
	ext := NewBaseExtent(1, 0, basic.ExtentTypeData)

	err := ext.Defragment()
	if err != nil {
		t.Fatalf("Defragment failed: %v", err)
	}

	// 验证状态为Free
	if ext.header.State != basic.ExtentStateFree {
		t.Errorf("Expected free state, got %v", ext.header.State)
	}

	// 验证无碎片
	if ext.stats.FragPages != 0 {
		t.Errorf("Expected 0 frag pages, got %d", ext.stats.FragPages)
	}

	t.Log("Empty extent defragmentation successful")
}

// TestBaseExtentDefragmentFull 测试满extent的碎片整理
func TestBaseExtentDefragmentFull(t *testing.T) {
	ext := NewBaseExtent(1, 0, basic.ExtentTypeData)

	// 分配所有64个页面
	for i := uint32(0); i < 64; i++ {
		ext.pages[i] = true
		ext.pageList = append(ext.pageList, i)
		ext.header.PageCount++
	}

	err := ext.Defragment()
	if err != nil {
		t.Fatalf("Defragment failed: %v", err)
	}

	// 验证状态为Full
	if ext.header.State != basic.ExtentStateFull {
		t.Errorf("Expected full state, got %v", ext.header.State)
	}

	// 验证无碎片（连续分配）
	if ext.stats.FragPages != 0 {
		t.Errorf("Expected 0 frag pages, got %d", ext.stats.FragPages)
	}

	t.Log("Full extent defragmentation successful")
}

// TestUnifiedExtentDefragment 测试UnifiedExtent碎片整理
func TestUnifiedExtentDefragment(t *testing.T) {
	// 创建extent
	ext := NewUnifiedExtent(0, 1, 0, basic.ExtentTypeData, basic.ExtentPurposeData)

	// 分配一些页面
	for i := 0; i < 5; i++ {
		_, err := ext.AllocatePage()
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}
	}

	// 执行碎片整理
	err := ext.Defragment()
	if err != nil {
		t.Fatalf("Defragment failed: %v", err)
	}

	// 验证页面列表已排序（使用GetAllocatedPages）
	allocatedPages := ext.GetAllocatedPages()
	if len(allocatedPages) != 5 {
		t.Errorf("Expected 5 pages, got %d", len(allocatedPages))
	}

	// 验证页面列表是排序的
	for i := 1; i < len(allocatedPages); i++ {
		if allocatedPages[i] <= allocatedPages[i-1] {
			t.Errorf("Page list not sorted: %v", allocatedPages)
			break
		}
	}

	// 验证状态
	if ext.state != basic.ExtentStatePartial {
		t.Errorf("Expected partial state, got %v", ext.state)
	}

	// 验证LastDefragged已更新
	if ext.stats.LastDefragged == 0 {
		t.Error("LastDefragged should be updated")
	}

	t.Logf("UnifiedExtent defragmentation successful: %d pages, %d fragments",
		len(allocatedPages), ext.stats.FragPages)
}

// TestUnifiedExtentDefragmentConsistency 测试碎片整理后的一致性
func TestUnifiedExtentDefragmentConsistency(t *testing.T) {
	ext := NewUnifiedExtent(0, 1, 0, basic.ExtentTypeData, basic.ExtentPurposeData)

	// 分配一些页面
	allocatedPages := make([]uint32, 0)
	for i := 0; i < 10; i++ {
		pageNo, err := ext.AllocatePage()
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}
		allocatedPages = append(allocatedPages, pageNo)
	}

	// 执行碎片整理
	err := ext.Defragment()
	if err != nil {
		t.Fatalf("Defragment failed: %v", err)
	}

	// 验证所有分配的页面仍然存在（使用IsPageAllocated）
	for _, pageNo := range allocatedPages {
		if !ext.IsPageAllocated(pageNo) {
			t.Errorf("Page %d lost after defragmentation", pageNo)
		}
	}

	// 验证页面数量一致（使用GetPageCount）
	if ext.GetPageCount() != uint32(len(allocatedPages)) {
		t.Errorf("Page count mismatch: expected %d, got %d",
			len(allocatedPages), ext.GetPageCount())
	}

	// 验证统计信息一致
	expectedFree := PagesPerExtent - uint32(len(allocatedPages))
	if ext.stats.FreePages != expectedFree {
		t.Errorf("Free pages mismatch: expected %d, got %d",
			expectedFree, ext.stats.FreePages)
	}

	t.Log("Defragmentation consistency check passed")
}

// BenchmarkBaseExtentDefragment 基准测试BaseExtent碎片整理
func BenchmarkBaseExtentDefragment(b *testing.B) {
	ext := NewBaseExtent(1, 0, basic.ExtentTypeData)

	// 分配一些页面
	for i := uint32(0); i < 32; i++ {
		ext.pages[i*2] = true // 每隔一个页面分配
		ext.pageList = append(ext.pageList, i*2)
		ext.header.PageCount++
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ext.Defragment()
	}
}

// BenchmarkUnifiedExtentDefragment 基准测试UnifiedExtent碎片整理
func BenchmarkUnifiedExtentDefragment(b *testing.B) {
	ext := NewUnifiedExtent(0, 1, 0, basic.ExtentTypeData, basic.ExtentPurposeData)

	// 分配一些页面
	for i := 0; i < 32; i++ {
		ext.AllocatePage()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ext.Defragment()
	}
}
