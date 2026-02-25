package manager

import (
	"context"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
)

// TestFragmentationAnalysis 测试碎片分析
func TestFragmentationAnalysis(t *testing.T) {
	// 创建测试环境
	spaceManager := NewSpaceManager("./testdata/defrag")
	defer cleanupTestData("./testdata/defrag")

	// 创建表空间
	spaceID, err := spaceManager.CreateTableSpace("test_frag")
	if err != nil {
		t.Fatalf("Failed to create tablespace: %v", err)
	}

	// 创建extent管理器
	bufferPool := buffer_pool.NewBufferPool(&buffer_pool.BufferPoolConfig{
		TotalPages:     100,
		PageSize:       16384,
		BufferPoolSize: 100 * 16384,
		StorageManager: spaceManager,
	})
	extentManager := NewExtentManager(bufferPool)

	// 分配一些extent模拟碎片
	for i := 0; i < 10; i++ {
		_, err := extentManager.AllocateExtent(spaceID, basic.ExtentTypeData)
		if err != nil {
			t.Fatalf("Failed to allocate extent: %v", err)
		}
	}

	// 创建碎片整理器
	config := &DefragmenterConfig{
		FragmentationThreshold: 30.0,
		MinSpaceUtilization:    70.0,
		MaxHoleSize:            10,
	}
	defragmenter := NewTablespaceDefragmenter(spaceManager, extentManager, bufferPool, config)

	// 分析碎片
	ctx := context.Background()
	report, err := defragmenter.AnalyzeFragmentation(ctx, spaceID)
	if err != nil {
		t.Fatalf("Failed to analyze fragmentation: %v", err)
	}

	// 验证报告
	if report == nil {
		t.Fatal("Report is nil")
	}

	t.Logf("Fragmentation Report:")
	t.Logf("  Space ID: %d", report.SpaceID)
	t.Logf("  Total Pages: %d", report.TotalPages)
	t.Logf("  Total Extents: %d", report.TotalExtents)
	t.Logf("  Fragmentation Rate: %.2f%%", report.FragmentationRate)
	t.Logf("  Space Utilization: %.2f%%", report.SpaceUtilization)
	t.Logf("  Needs Defragmentation: %v", report.NeedsDefragmentation)
	t.Logf("  Recommended Mode: %s", report.RecommendedMode)
	t.Logf("  Estimated Gain: %d bytes", report.EstimatedGain)

	// 验证基本字段
	if report.SpaceID != spaceID {
		t.Errorf("Expected space ID %d, got %d", spaceID, report.SpaceID)
	}

	if report.TotalExtents != 10 {
		t.Errorf("Expected 10 extents, got %d", report.TotalExtents)
	}
}

// TestOnlineDefragmentation 测试在线碎片整理
func TestOnlineDefragmentation(t *testing.T) {
	// 创建测试环境
	spaceManager := NewSpaceManager("./testdata/defrag_online")
	defer cleanupTestData("./testdata/defrag_online")

	spaceID, err := spaceManager.CreateTableSpace("test_online")
	if err != nil {
		t.Fatalf("Failed to create tablespace: %v", err)
	}

	bufferPool := buffer_pool.NewBufferPool(&buffer_pool.BufferPoolConfig{
		TotalPages:     100,
		PageSize:       16384,
		BufferPoolSize: 100 * 16384,
		StorageManager: spaceManager,
	})
	extentManager := NewExtentManager(bufferPool)

	// 分配extent
	for i := 0; i < 5; i++ {
		_, err := extentManager.AllocateExtent(spaceID, basic.ExtentTypeData)
		if err != nil {
			t.Fatalf("Failed to allocate extent: %v", err)
		}
	}

	// 创建碎片整理器
	config := &DefragmenterConfig{
		FragmentationThreshold: 10.0, // 低阈值，确保触发整理
		IOThrottleDelay:        1 * time.Millisecond,
	}
	defragmenter := NewTablespaceDefragmenter(spaceManager, extentManager, bufferPool, config)

	// 执行在线整理
	ctx := context.Background()
	err = defragmenter.Defragment(ctx, spaceID, DefragmentModeOnline)
	if err != nil {
		t.Fatalf("Failed to defragment: %v", err)
	}

	// 获取统计
	stats := defragmenter.GetCurrentStats()
	if stats == nil {
		t.Fatal("Stats is nil")
	}

	t.Logf("Defragmentation Stats:")
	t.Logf("  Mode: %s", stats.Mode)
	t.Logf("  Status: %s", stats.Status)
	t.Logf("  Duration: %dms", stats.Duration)
	t.Logf("  Pages Processed: %d", stats.PagesProcessed)
	t.Logf("  Extents Processed: %d", stats.ExtentsProcessed)
	t.Logf("  Space Reclaimed: %d bytes", stats.SpaceReclaimed)
	t.Logf("  Progress: %.2f%%", stats.Progress)

	// 验证状态
	if stats.Status != "completed" {
		t.Errorf("Expected status 'completed', got '%s'", stats.Status)
	}

	if stats.Progress != 100.0 {
		t.Errorf("Expected progress 100%%, got %.2f%%", stats.Progress)
	}
}

// TestIncrementalDefragmentation 测试增量碎片整理
func TestIncrementalDefragmentation(t *testing.T) {
	// 创建测试环境
	spaceManager := NewSpaceManager("./testdata/defrag_incremental")
	defer cleanupTestData("./testdata/defrag_incremental")

	spaceID, err := spaceManager.CreateTableSpace("test_incremental")
	if err != nil {
		t.Fatalf("Failed to create tablespace: %v", err)
	}

	bufferPool := buffer_pool.NewBufferPool(&buffer_pool.BufferPoolConfig{
		TotalPages:     100,
		PageSize:       16384,
		BufferPoolSize: 100 * 16384,
		StorageManager: spaceManager,
	})
	extentManager := NewExtentManager(bufferPool)

	// 分配更多extent
	for i := 0; i < 20; i++ {
		_, err := extentManager.AllocateExtent(spaceID, basic.ExtentTypeData)
		if err != nil {
			t.Fatalf("Failed to allocate extent: %v", err)
		}
	}

	// 创建碎片整理器
	config := &DefragmenterConfig{
		FragmentationThreshold: 10.0,
		IncrementalBatchSize:   5,
		IncrementalInterval:    10 * time.Millisecond,
	}
	defragmenter := NewTablespaceDefragmenter(spaceManager, extentManager, bufferPool, config)

	// 执行增量整理
	ctx := context.Background()
	err = defragmenter.Defragment(ctx, spaceID, DefragmentModeIncremental)
	if err != nil {
		t.Fatalf("Failed to defragment: %v", err)
	}

	// 获取统计
	stats := defragmenter.GetCurrentStats()
	if stats == nil {
		t.Fatal("Stats is nil")
	}

	t.Logf("Incremental Defragmentation Stats:")
	t.Logf("  Duration: %dms", stats.Duration)
	t.Logf("  Extents Processed: %d", stats.ExtentsProcessed)
	t.Logf("  Progress: %.2f%%", stats.Progress)

	// 验证状态
	if stats.Status != "completed" {
		t.Errorf("Expected status 'completed', got '%s'", stats.Status)
	}
}

// TestSpaceOptimization 测试空间优化
func TestSpaceOptimization(t *testing.T) {
	// 创建测试环境
	spaceManager := NewSpaceManager("./testdata/defrag_optimize")
	defer cleanupTestData("./testdata/defrag_optimize")

	spaceID, err := spaceManager.CreateTableSpace("test_optimize")
	if err != nil {
		t.Fatalf("Failed to create tablespace: %v", err)
	}

	bufferPool := buffer_pool.NewBufferPool(&buffer_pool.BufferPoolConfig{
		TotalPages:     100,
		PageSize:       16384,
		BufferPoolSize: 100 * 16384,
		StorageManager: spaceManager,
	})
	extentManager := NewExtentManager(bufferPool)

	// 分配extent
	for i := 0; i < 10; i++ {
		_, err := extentManager.AllocateExtent(spaceID, basic.ExtentTypeData)
		if err != nil {
			t.Fatalf("Failed to allocate extent: %v", err)
		}
	}

	// 创建碎片整理器
	defragmenter := NewTablespaceDefragmenter(spaceManager, extentManager, bufferPool, nil)

	// 执行空间优化
	ctx := context.Background()
	err = defragmenter.OptimizeSpace(ctx, spaceID)
	if err != nil {
		t.Fatalf("Failed to optimize space: %v", err)
	}

	t.Log("Space optimization completed successfully")
}

// TestFragmentationLevel 测试碎片等级判断
func TestFragmentationLevel(t *testing.T) {
	// 创建测试环境
	spaceManager := NewSpaceManager("./testdata/defrag_level")
	defer cleanupTestData("./testdata/defrag_level")

	spaceID, err := spaceManager.CreateTableSpace("test_level")
	if err != nil {
		t.Fatalf("Failed to create tablespace: %v", err)
	}

	bufferPool := buffer_pool.NewBufferPool(&buffer_pool.BufferPoolConfig{
		TotalPages:     100,
		PageSize:       16384,
		BufferPoolSize: 100 * 16384,
		StorageManager: spaceManager,
	})
	extentManager := NewExtentManager(bufferPool)
	defragmenter := NewTablespaceDefragmenter(spaceManager, extentManager, bufferPool, nil)

	// 先分析碎片
	ctx := context.Background()
	_, err = defragmenter.AnalyzeFragmentation(ctx, spaceID)
	if err != nil {
		t.Fatalf("Failed to analyze fragmentation: %v", err)
	}

	// 获取碎片等级
	level := defragmenter.GetFragmentationLevel(spaceID)
	t.Logf("Fragmentation Level: %s", level)

	// 验证等级
	validLevels := map[string]bool{
		"low":      true,
		"medium":   true,
		"high":     true,
		"critical": true,
		"unknown":  true,
	}

	if !validLevels[level] {
		t.Errorf("Invalid fragmentation level: %s", level)
	}
}

// TestDefragmentationRecommendation 测试碎片整理建议
func TestDefragmentationRecommendation(t *testing.T) {
	// 创建测试环境
	spaceManager := NewSpaceManager("./testdata/defrag_recommend")
	defer cleanupTestData("./testdata/defrag_recommend")

	spaceID, err := spaceManager.CreateTableSpace("test_recommend")
	if err != nil {
		t.Fatalf("Failed to create tablespace: %v", err)
	}

	bufferPool := buffer_pool.NewBufferPool(&buffer_pool.BufferPoolConfig{
		TotalPages:     100,
		PageSize:       16384,
		BufferPoolSize: 100 * 16384,
		StorageManager: spaceManager,
	})
	extentManager := NewExtentManager(bufferPool)

	// 分配extent
	for i := 0; i < 15; i++ {
		_, err := extentManager.AllocateExtent(spaceID, basic.ExtentTypeData)
		if err != nil {
			t.Fatalf("Failed to allocate extent: %v", err)
		}
	}

	config := &DefragmenterConfig{
		FragmentationThreshold: 20.0,
		MinSpaceUtilization:    70.0,
	}
	defragmenter := NewTablespaceDefragmenter(spaceManager, extentManager, bufferPool, config)

	// 分析碎片
	ctx := context.Background()
	report, err := defragmenter.AnalyzeFragmentation(ctx, spaceID)
	if err != nil {
		t.Fatalf("Failed to analyze fragmentation: %v", err)
	}

	// 验证建议
	t.Logf("Needs Defragmentation: %v", report.NeedsDefragmentation)
	t.Logf("Recommended Mode: %s", report.RecommendedMode)
	t.Logf("Estimated Gain: %d bytes", report.EstimatedGain)

	// 判断是否应该整理
	shouldDefrag := defragmenter.ShouldDefragment(spaceID)
	t.Logf("Should Defragment: %v", shouldDefrag)

	// 估算整理时间
	estimatedTime := defragmenter.EstimateDefragmentTime(spaceID, DefragmentModeOnline)
	t.Logf("Estimated Time (online): %v", estimatedTime)
}

// cleanupTestData 清理测试数据
func cleanupTestData(dir string) {
	// 这里简化处理，实际应该删除测试目录
	// os.RemoveAll(dir)
}
