package plan

import (
	"testing"
	"time"
)

// TestSelectRandomPages 测试随机页面选择
func TestSelectRandomPages(t *testing.T) {
	// 创建增强统计信息收集器
	config := &StatisticsConfig{
		AutoUpdateInterval: 1 * time.Hour,
		SampleRate:         0.1,
		HistogramBuckets:   100,
		ExpirationTime:     24 * time.Hour,
		EnableAutoUpdate:   false,
	}

	collector := NewEnhancedStatisticsCollector(config, nil, nil)

	// 测试1: 采样数小于总页数
	totalPages := uint32(1000)
	sampleCount := 100

	selected := collector.selectRandomPages(totalPages, sampleCount)

	if len(selected) != sampleCount {
		t.Errorf("Expected %d pages, got %d", sampleCount, len(selected))
	}

	// 验证所有页面ID在有效范围内
	for _, pageID := range selected {
		if pageID >= totalPages {
			t.Errorf("Page ID %d out of range [0, %d)", pageID, totalPages)
		}
	}

	t.Logf("✅ Selected %d random pages from %d total pages", len(selected), totalPages)
	t.Logf("   Sample pages: %v...", selected[:minInt(10, len(selected))])
}

// TestSelectRandomPagesFullSample 测试全采样
func TestSelectRandomPagesFullSample(t *testing.T) {
	config := &StatisticsConfig{
		SampleRate: 1.0,
	}

	collector := NewEnhancedStatisticsCollector(config, nil, nil)

	totalPages := uint32(100)
	sampleCount := 150 // 大于总页数

	selected := collector.selectRandomPages(totalPages, sampleCount)

	if len(selected) != int(totalPages) {
		t.Errorf("Expected %d pages (all), got %d", totalPages, len(selected))
	}

	// 验证包含所有页面
	pageSet := make(map[uint32]bool)
	for _, pageID := range selected {
		pageSet[pageID] = true
	}

	if len(pageSet) != int(totalPages) {
		t.Errorf("Expected %d unique pages, got %d", totalPages, len(pageSet))
	}

	t.Logf("✅ Full sample: selected all %d pages", len(selected))
}

// TestSelectRandomPagesSmallSet 测试小数据集
func TestSelectRandomPagesSmallSet(t *testing.T) {
	config := &StatisticsConfig{
		SampleRate: 0.5,
	}

	collector := NewEnhancedStatisticsCollector(config, nil, nil)

	totalPages := uint32(10)
	sampleCount := 5

	selected := collector.selectRandomPages(totalPages, sampleCount)

	if len(selected) != sampleCount {
		t.Errorf("Expected %d pages, got %d", sampleCount, len(selected))
	}

	t.Logf("✅ Small set sample: selected %d pages from %d", len(selected), totalPages)
}

// TestGetSampledRowCount 测试基于采样的行数估算
func TestGetSampledRowCount(t *testing.T) {
	config := &StatisticsConfig{
		SampleRate: 0.1,
	}

	collector := NewEnhancedStatisticsCollector(config, nil, nil)

	// 创建模拟空间
	mockSpace := &MockSpace{
		pageCount: 1000,
		usedSpace: 1000 * 16384, // 1000页 * 16KB
	}

	// 估算行数
	rowCount := collector.getSampledRowCount(mockSpace, mockSpace.pageCount)

	t.Logf("✅ Estimated row count: %d", rowCount)
	t.Logf("   Total pages: %d", mockSpace.pageCount)
	t.Logf("   Sample rate: %.2f", config.SampleRate)

	// 验证行数合理性
	if rowCount <= 0 {
		t.Error("Row count should be positive")
	}

	// 基于100行/页的估算，应该在合理范围内
	expectedMin := int64(mockSpace.pageCount) * 50  // 至少50行/页
	expectedMax := int64(mockSpace.pageCount) * 200 // 最多200行/页

	if rowCount < expectedMin || rowCount > expectedMax {
		t.Logf("   Warning: Row count %d outside expected range [%d, %d]", rowCount, expectedMin, expectedMax)
	}
}

// TestGetSampledRowCountZeroPages 测试零页面情况
func TestGetSampledRowCountZeroPages(t *testing.T) {
	config := &StatisticsConfig{
		SampleRate: 0.1,
	}

	collector := NewEnhancedStatisticsCollector(config, nil, nil)

	mockSpace := &MockSpace{
		pageCount: 0,
		usedSpace: 0,
	}

	rowCount := collector.getSampledRowCount(mockSpace, mockSpace.pageCount)

	if rowCount != 0 {
		t.Errorf("Expected 0 rows for empty space, got %d", rowCount)
	}

	t.Logf("✅ Zero pages: row count = %d", rowCount)
}

// TestGetSampledRowCountHighSampleRate 测试高采样率
func TestGetSampledRowCountHighSampleRate(t *testing.T) {
	config := &StatisticsConfig{
		SampleRate: 0.9, // 90% 采样率
	}

	collector := NewEnhancedStatisticsCollector(config, nil, nil)

	mockSpace := &MockSpace{
		pageCount: 100,
		usedSpace: 100 * 16384,
	}

	rowCount := collector.getSampledRowCount(mockSpace, mockSpace.pageCount)

	t.Logf("✅ High sample rate (90%%) row count: %d", rowCount)
	t.Logf("   Total pages: %d", mockSpace.pageCount)

	if rowCount <= 0 {
		t.Error("Row count should be positive")
	}
}

// TestGetSampledRowCountLowSampleRate 测试低采样率
func TestGetSampledRowCountLowSampleRate(t *testing.T) {
	config := &StatisticsConfig{
		SampleRate: 0.01, // 1% 采样率
	}

	collector := NewEnhancedStatisticsCollector(config, nil, nil)

	mockSpace := &MockSpace{
		pageCount: 10000,
		usedSpace: 10000 * 16384,
	}

	rowCount := collector.getSampledRowCount(mockSpace, mockSpace.pageCount)

	t.Logf("✅ Low sample rate (1%%) row count: %d", rowCount)
	t.Logf("   Total pages: %d", mockSpace.pageCount)

	if rowCount <= 0 {
		t.Error("Row count should be positive")
	}
}

// TestAdaptiveSamplerGetSampleSize 测试自适应采样大小
func TestAdaptiveSamplerGetSampleSize(t *testing.T) {
	sampler := NewAdaptiveSampler()

	// 测试不同表大小的采样
	testCases := []struct {
		tableRows   int64
		expectedMin int64
		expectedMax int64
		description string
	}{
		{100, 10, 100, "Very small table"},
		{1000, 100, 1000, "Small table"},
		{10000, 1000, 10000, "Medium table"},
		{100000, 5000, 50000, "Large table"},
		{1000000, 10000, 100000, "Very large table"},
	}

	for _, tc := range testCases {
		sampleSize := sampler.GetSampleSize(tc.tableRows)

		t.Logf("✅ %s (%d rows): sample size = %d", tc.description, tc.tableRows, sampleSize)

		if sampleSize < tc.expectedMin {
			t.Errorf("Sample size %d too small for %s (expected >= %d)", sampleSize, tc.description, tc.expectedMin)
		}

		if sampleSize > tc.expectedMax {
			t.Errorf("Sample size %d too large for %s (expected <= %d)", sampleSize, tc.description, tc.expectedMax)
		}

		// 验证采样率合理
		sampleRate := float64(sampleSize) / float64(tc.tableRows)
		t.Logf("   Sample rate: %.2f%%", sampleRate*100)
	}
}

// TestCountRowsInPage 测试页面行数统计
func TestCountRowsInPage(t *testing.T) {
	config := &StatisticsConfig{
		SampleRate: 0.1,
	}

	collector := NewEnhancedStatisticsCollector(config, nil, nil)

	mockSpace := &MockSpace{
		pageCount: 100,
		usedSpace: 100 * 16384,
	}

	// 统计单个页面的行数
	pageID := uint32(0)
	rowCount := collector.countRowsInPage(mockSpace, pageID)

	t.Logf("✅ Rows in page %d: %d", pageID, rowCount)

	if rowCount < 0 {
		t.Error("Row count should be non-negative")
	}

	// 验证行数在合理范围内（假设每页最多1000行）
	if rowCount > 1000 {
		t.Errorf("Row count %d seems too high for a single page", rowCount)
	}
}

// MockSpace 模拟空间
type MockSpace struct {
	pageCount uint32
	usedSpace uint64
}

func (s *MockSpace) GetPageCount() uint32 {
	return s.pageCount
}

func (s *MockSpace) GetUsedSpace() uint64 {
	return s.usedSpace
}

func (s *MockSpace) Name() string {
	return "mock_space"
}

func (s *MockSpace) GetFilePath() string {
	return "/tmp/mock_space.ibd"
}

// minInt 辅助函数
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
