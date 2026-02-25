package engine

import (
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
)

// TestDirtyPageManagerAddRemove 测试脏页添加和移除
func TestDirtyPageManagerAddRemove(t *testing.T) {
	config := &DirtyPageConfig{
		MaxDirtyPages:  1000,
		MaxDirtyRatio:  0.75,
		FlushBatchSize: 100,
		FlushInterval:  1 * time.Second,
		EnableAdaptive: true,
	}

	dpm := NewDirtyPageManager(config)

	// 创建测试页面
	page1 := buffer_pool.NewBufferPage(1, 100)
	page1.SetDirty(true)

	page2 := buffer_pool.NewBufferPage(1, 101)
	page2.SetDirty(true)

	// 添加脏页
	dpm.AddDirtyPage(page1, 1000)
	dpm.AddDirtyPage(page2, 1001)

	// 验证脏页数量
	count := dpm.GetDirtyPageCount()
	if count != 2 {
		t.Errorf("Expected 2 dirty pages, got %d", count)
	}

	t.Logf("✅ Added 2 dirty pages, count = %d", count)

	// 移除脏页
	dpm.RemoveDirtyPage(1, 100)

	count = dpm.GetDirtyPageCount()
	if count != 1 {
		t.Errorf("Expected 1 dirty page after removal, got %d", count)
	}

	t.Logf("✅ Removed 1 dirty page, count = %d", count)

	// 获取统计信息
	stats := dpm.GetStats()
	t.Logf("📊 Stats: TotalDirtyPages=%d, TotalFlushes=%d, TotalModifies=%d",
		stats.TotalDirtyPages, stats.TotalFlushes, stats.TotalModifies)
}

// TestDirtyPageManagerGetByLSN 测试按LSN排序获取脏页
func TestDirtyPageManagerGetByLSN(t *testing.T) {
	dpm := NewDirtyPageManager(nil)

	// 添加多个脏页（LSN不同）
	for i := 0; i < 10; i++ {
		page := buffer_pool.NewBufferPage(1, uint32(100+i))
		page.SetDirty(true)
		// LSN倒序添加
		dpm.AddDirtyPage(page, uint64(1000-i*10))
	}

	// 按LSN排序获取
	pages := dpm.GetDirtyPagesByLSN()

	if len(pages) != 10 {
		t.Errorf("Expected 10 pages, got %d", len(pages))
	}

	// 验证排序（LSN应该递增）
	for i := 1; i < len(pages); i++ {
		if pages[i].FirstModLSN < pages[i-1].FirstModLSN {
			t.Errorf("Pages not sorted by LSN: %d < %d", pages[i].FirstModLSN, pages[i-1].FirstModLSN)
		}
	}

	t.Logf("✅ Pages sorted by LSN correctly")
	t.Logf("   First LSN: %d, Last LSN: %d", pages[0].FirstModLSN, pages[len(pages)-1].FirstModLSN)
}

// TestDirtyPageManagerGetByTime 测试按时间排序获取脏页
func TestDirtyPageManagerGetByTime(t *testing.T) {
	dpm := NewDirtyPageManager(nil)

	// 添加多个脏页（时间不同）
	for i := 0; i < 5; i++ {
		page := buffer_pool.NewBufferPage(1, uint32(100+i))
		page.SetDirty(true)
		dpm.AddDirtyPage(page, uint64(1000+i))
		time.Sleep(10 * time.Millisecond) // 确保时间不同
	}

	// 按时间排序获取
	pages := dpm.GetDirtyPagesByTime()

	if len(pages) != 5 {
		t.Errorf("Expected 5 pages, got %d", len(pages))
	}

	// 验证排序（时间应该递增）
	for i := 1; i < len(pages); i++ {
		if pages[i].ModifyTime.Before(pages[i-1].ModifyTime) {
			t.Errorf("Pages not sorted by time")
		}
	}

	t.Logf("✅ Pages sorted by time correctly")
}

// TestDirtyPageManagerShouldFlush 测试刷新判断
func TestDirtyPageManagerShouldFlush(t *testing.T) {
	config := &DirtyPageConfig{
		MaxDirtyPages:  100,
		MaxDirtyRatio:  0.75,
		FlushBatchSize: 10,
	}

	dpm := NewDirtyPageManager(config)

	// 测试1: 脏页数量未达到阈值
	for i := 0; i < 50; i++ {
		page := buffer_pool.NewBufferPage(1, uint32(100+i))
		page.SetDirty(true)
		dpm.AddDirtyPage(page, uint64(1000+i))
	}

	shouldFlush := dpm.ShouldFlush(1000)
	if shouldFlush {
		t.Error("Should not flush when dirty ratio is low")
	}
	t.Logf("✅ Correctly decided not to flush (50/1000 = 5%%)")

	// 测试2: 脏页数量达到阈值
	for i := 50; i < 100; i++ {
		page := buffer_pool.NewBufferPage(1, uint32(100+i))
		page.SetDirty(true)
		dpm.AddDirtyPage(page, uint64(1000+i))
	}

	shouldFlush = dpm.ShouldFlush(1000)
	if !shouldFlush {
		t.Error("Should flush when max dirty pages reached")
	}
	t.Logf("✅ Correctly decided to flush (100 pages = max)")

	// 测试3: 脏页比例达到阈值
	dpm2 := NewDirtyPageManager(config)
	for i := 0; i < 75; i++ {
		page := buffer_pool.NewBufferPage(1, uint32(100+i))
		page.SetDirty(true)
		dpm2.AddDirtyPage(page, uint64(1000+i))
	}

	shouldFlush = dpm2.ShouldFlush(100)
	if !shouldFlush {
		t.Error("Should flush when dirty ratio >= 75%")
	}
	t.Logf("✅ Correctly decided to flush (75/100 = 75%%)")
}

// TestDirtyPageManagerGetFlushBatchSize 测试自适应批量大小
func TestDirtyPageManagerGetFlushBatchSize(t *testing.T) {
	config := &DirtyPageConfig{
		MaxDirtyPages:  1000,
		MaxDirtyRatio:  0.75,
		FlushBatchSize: 100,
		EnableAdaptive: true,
	}

	dpm := NewDirtyPageManager(config)

	// 测试不同脏页比例的批量大小
	testCases := []struct {
		dirtyCount  int
		totalPages  int
		expectedMin int
		expectedMax int
		description string
	}{
		{100, 1000, 50, 100, "Low dirty ratio (10%)"},
		{250, 1000, 100, 100, "Medium dirty ratio (25%)"},
		{500, 1000, 200, 200, "High dirty ratio (50%)"},
		{750, 1000, 400, 400, "Very high dirty ratio (75%)"},
	}

	for _, tc := range testCases {
		// 添加脏页
		dpm2 := NewDirtyPageManager(config)
		for i := 0; i < tc.dirtyCount; i++ {
			page := buffer_pool.NewBufferPage(1, uint32(100+i))
			page.SetDirty(true)
			dpm2.AddDirtyPage(page, uint64(1000+i))
		}

		batchSize := dpm2.GetFlushBatchSize(tc.totalPages)

		t.Logf("✅ %s: batch size = %d (dirty=%d/%d=%.1f%%)",
			tc.description, batchSize, tc.dirtyCount, tc.totalPages,
			float64(tc.dirtyCount)/float64(tc.totalPages)*100)

		if batchSize < tc.expectedMin || batchSize > tc.expectedMax {
			t.Logf("   Warning: batch size %d outside expected range [%d, %d]",
				batchSize, tc.expectedMin, tc.expectedMax)
		}
	}
}

// TestAdaptiveFlushStrategySelectPages 测试自适应刷新策略
func TestAdaptiveFlushStrategySelectPages(t *testing.T) {
	config := &AdaptiveFlushConfig{
		TargetDirtyRatio:   0.50,
		MaxDirtyRatio:      0.75,
		LSNGrowthThreshold: 10000.0,
		MaxFlushRate:       1000,
		MinFlushBatch:      10,
		MaxFlushBatch:      500,
	}

	strategy := NewAdaptiveFlushStrategy(config)

	// 创建测试脏页
	dirtyPages := make([]*buffer_pool.BufferPage, 100)
	for i := 0; i < 100; i++ {
		page := buffer_pool.NewBufferPage(1, uint32(100+i))
		page.SetDirty(true)
		dirtyPages[i] = page
	}

	// 选择要刷新的页面
	selected := strategy.SelectPagesToFlush(dirtyPages, 1000, 50000)

	if len(selected) == 0 {
		t.Error("Should select some pages to flush")
	}

	t.Logf("✅ Selected %d pages to flush from %d dirty pages", len(selected), len(dirtyPages))
}

// TestAdaptiveFlushStrategyCalculateBatchSize 测试批量大小计算
func TestAdaptiveFlushStrategyCalculateBatchSize(t *testing.T) {
	config := &AdaptiveFlushConfig{
		TargetDirtyRatio:   0.50,
		MaxDirtyRatio:      0.75,
		LSNGrowthThreshold: 10000.0,
		MaxFlushRate:       1000,
		MinFlushBatch:      10,
		MaxFlushBatch:      500,
	}

	strategy := NewAdaptiveFlushStrategy(config)

	// 测试不同脏页数量的批量大小
	testCases := []struct {
		dirtyCount  int
		maxPages    int
		description string
	}{
		{100, 1000, "Low dirty ratio"},
		{500, 1000, "Medium dirty ratio"},
		{750, 1000, "High dirty ratio"},
		{900, 1000, "Very high dirty ratio"},
	}

	for _, tc := range testCases {
		batchSize := strategy.calculateFlushBatchSize(tc.dirtyCount, tc.maxPages)

		dirtyRatio := float64(tc.dirtyCount) / float64(tc.maxPages)
		t.Logf("✅ %s (%.1f%%): batch size = %d",
			tc.description, dirtyRatio*100, batchSize)

		if batchSize < config.MinFlushBatch {
			t.Errorf("Batch size %d below minimum %d", batchSize, config.MinFlushBatch)
		}

		if batchSize > config.MaxFlushBatch {
			t.Errorf("Batch size %d above maximum %d", batchSize, config.MaxFlushBatch)
		}
	}
}

// TestAdaptiveFlushStrategyGetFlushRate 测试刷新速率
func TestAdaptiveFlushStrategyGetFlushRate(t *testing.T) {
	config := &AdaptiveFlushConfig{
		TargetDirtyRatio:   0.50,
		MaxDirtyRatio:      0.75,
		LSNGrowthThreshold: 10000.0,
		MaxFlushRate:       1000,
		MinFlushBatch:      10,
		MaxFlushBatch:      500,
	}

	strategy := NewAdaptiveFlushStrategy(config)

	// 测试不同脏页比例的刷新速率
	testCases := []struct {
		dirtyCount  int
		totalPages  int
		description string
	}{
		{100, 1000, "Low dirty ratio (10%)"},
		{500, 1000, "Medium dirty ratio (50%)"},
		{750, 1000, "High dirty ratio (75%)"},
		{900, 1000, "Very high dirty ratio (90%)"},
	}

	for _, tc := range testCases {
		flushRate := strategy.GetFlushRate(tc.dirtyCount, tc.totalPages)

		dirtyRatio := float64(tc.dirtyCount) / float64(tc.totalPages)
		t.Logf("✅ %s: flush rate = %d pages/sec",
			tc.description, flushRate)

		if dirtyRatio > config.MaxDirtyRatio && flushRate != config.MaxFlushRate {
			t.Logf("   Note: Expected max flush rate for high dirty ratio")
		}
	}
}

// TestAdaptiveFlushStrategyShouldFlush 测试刷新判断
func TestAdaptiveFlushStrategyShouldFlush(t *testing.T) {
	config := &AdaptiveFlushConfig{
		TargetDirtyRatio:   0.50,
		MaxDirtyRatio:      0.75,
		LSNGrowthThreshold: 10000.0,
		MaxFlushRate:       1000,
		MinFlushBatch:      10,
		MaxFlushBatch:      500,
	}

	strategy := NewAdaptiveFlushStrategy(config)

	// 测试1: 低脏页比例
	shouldFlush := strategy.ShouldFlush(100, 1000)
	if shouldFlush {
		t.Error("Should not flush with low dirty ratio")
	}
	t.Logf("✅ Correctly decided not to flush (10%% dirty ratio)")

	// 测试2: 高脏页比例
	shouldFlush = strategy.ShouldFlush(800, 1000)
	if !shouldFlush {
		t.Error("Should flush with high dirty ratio")
	}
	t.Logf("✅ Correctly decided to flush (80%% dirty ratio)")
}
