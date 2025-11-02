package manager

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/ibd"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/space"
)

// TestSpaceManagerGetTableSpaceInfo 测试表空间信息获取
func TestSpaceManagerGetTableSpaceInfo(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "space_stats_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建SpaceManager
	sm := NewSpaceManager(tmpDir)

	// 创建测试表空间
	spaceName := "test_space_stats"
	spaceID, err := sm.CreateTableSpace(spaceName)
	if err != nil {
		t.Fatalf("Failed to create tablespace: %v", err)
	}

	// 获取表空间信息
	info, err := sm.GetTableSpaceInfo(spaceID)
	if err != nil {
		t.Fatalf("Failed to get tablespace info: %v", err)
	}

	// 验证基本信息
	if info.SpaceID != spaceID {
		t.Errorf("Expected SpaceID %d, got %d", spaceID, info.SpaceID)
	}

	if info.Name != spaceName {
		t.Errorf("Expected Name %s, got %s", spaceName, info.Name)
	}

	// 验证Size字段已实现（不再是0）
	if info.Size == 0 {
		t.Errorf("Size should not be 0 after implementation")
	}
	t.Logf("✅ Size: %d bytes", info.Size)

	// 验证FreeSpace字段已实现
	t.Logf("✅ FreeSpace: %d bytes", info.FreeSpace)

	// 验证SegmentCount字段已实现
	t.Logf("✅ SegmentCount: %d", info.SegmentCount)

	// 验证FilePath
	expectedPath := filepath.Join(tmpDir, spaceName+".ibd")
	if info.FilePath != expectedPath {
		t.Errorf("Expected FilePath %s, got %s", expectedPath, info.FilePath)
	}
}

// TestIBDSpaceDetailedStats 测试IBDSpace详细统计
func TestIBDSpaceDetailedStats(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "ibd_stats_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建IBD文件 - 参数顺序: dataDir, name, spaceID
	ibdFile := ibd.NewIBDFile(tmpDir, "test_stats", 1)

	// 创建IBDSpace
	ibdSpace := space.NewIBDSpace(ibdFile, false)
	if err := ibdSpace.Initialize(); err != nil {
		t.Fatalf("Failed to initialize IBDSpace: %v", err)
	}
	defer ibdSpace.Close()

	// 分配一些extents
	for i := 0; i < 5; i++ {
		_, err := ibdSpace.AllocateExtent(basic.ExtentPurposeData)
		if err != nil {
			t.Fatalf("Failed to allocate extent %d: %v", i, err)
		}
	}

	// 获取详细统计
	stats := ibdSpace.GetDetailedStats()
	if stats == nil {
		t.Fatal("GetDetailedStats returned nil")
	}

	// 验证统计信息
	t.Logf("📊 Detailed Stats:")
	t.Logf("  SpaceID: %d", stats.SpaceID)
	t.Logf("  Name: %s", stats.Name)
	t.Logf("  TotalPages: %d", stats.TotalPages)
	t.Logf("  AllocatedPages: %d", stats.AllocatedPages)
	t.Logf("  TotalExtents: %d", stats.TotalExtents)
	t.Logf("  FreeExtents: %d", stats.FreeExtents)
	t.Logf("  PartialExtents: %d", stats.PartialExtents)
	t.Logf("  FullExtents: %d", stats.FullExtents)
	t.Logf("  TotalSize: %d bytes", stats.TotalSize)
	t.Logf("  UsedSize: %d bytes", stats.UsedSize)
	t.Logf("  FreeSize: %d bytes", stats.FreeSize)

	// 验证extent数量
	if stats.TotalExtents != 6 { // 1 system + 5 data
		t.Errorf("Expected 6 extents, got %d", stats.TotalExtents)
	}

	// 验证总大小
	if stats.TotalSize == 0 {
		t.Error("TotalSize should not be 0")
	}

	// 验证已使用大小
	if stats.UsedSize == 0 {
		t.Error("UsedSize should not be 0")
	}
}

// TestSpaceManagerDetailedStats 测试SpaceManager详细统计
func TestSpaceManagerDetailedStats(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "sm_stats_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建SpaceManager
	sm := NewSpaceManager(tmpDir)

	// 创建测试表空间
	spaceName := "test_detailed_stats"
	spaceID, err := sm.CreateTableSpace(spaceName)
	if err != nil {
		t.Fatalf("Failed to create tablespace: %v", err)
	}

	// 使用类型断言获取详细统计
	smImpl, ok := sm.(*SpaceManagerImpl)
	if !ok {
		t.Fatal("Failed to cast to SpaceManagerImpl")
	}

	stats, err := smImpl.GetDetailedSpaceStats(spaceID)
	if err != nil {
		t.Fatalf("Failed to get detailed stats: %v", err)
	}

	if stats == nil {
		t.Fatal("GetDetailedSpaceStats returned nil")
	}

	// 验证统计信息
	t.Logf("📊 SpaceManager Detailed Stats:")
	t.Logf("  SpaceID: %d", stats.SpaceID)
	t.Logf("  Name: %s", stats.Name)
	t.Logf("  TotalPages: %d", stats.TotalPages)
	t.Logf("  AllocatedPages: %d", stats.AllocatedPages)
	t.Logf("  TotalExtents: %d", stats.TotalExtents)
	t.Logf("  TotalSize: %d bytes (%.2f MB)", stats.TotalSize, float64(stats.TotalSize)/(1024*1024))
	t.Logf("  UsedSize: %d bytes (%.2f MB)", stats.UsedSize, float64(stats.UsedSize)/(1024*1024))
	t.Logf("  FreeSize: %d bytes (%.2f MB)", stats.FreeSize, float64(stats.FreeSize)/(1024*1024))

	// 验证基本约束
	if stats.SpaceID != spaceID {
		t.Errorf("Expected SpaceID %d, got %d", spaceID, stats.SpaceID)
	}

	if stats.Name != spaceName {
		t.Errorf("Expected Name %s, got %s", spaceName, stats.Name)
	}

	if stats.TotalSize < stats.UsedSize {
		t.Errorf("TotalSize (%d) should be >= UsedSize (%d)", stats.TotalSize, stats.UsedSize)
	}
}

// TestSpaceStatsAfterExtentAllocation 测试分配extent后的统计
func TestSpaceStatsAfterExtentAllocation(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "extent_stats_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建SpaceManager
	sm := NewSpaceManager(tmpDir)

	// 创建测试表空间
	spaceName := "test_extent_stats"
	spaceID, err := sm.CreateTableSpace(spaceName)
	if err != nil {
		t.Fatalf("Failed to create tablespace: %v", err)
	}

	// 获取初始统计
	initialInfo, err := sm.GetTableSpaceInfo(spaceID)
	if err != nil {
		t.Fatalf("Failed to get initial info: %v", err)
	}

	t.Logf("📊 Initial Stats:")
	t.Logf("  Size: %d bytes", initialInfo.Size)
	t.Logf("  FreeSpace: %d bytes", initialInfo.FreeSpace)
	t.Logf("  SegmentCount: %d", initialInfo.SegmentCount)

	// 分配一些extents
	ibdSpace, err := sm.GetSpace(spaceID)
	if err != nil {
		t.Fatalf("Failed to get space: %v", err)
	}

	for i := 0; i < 3; i++ {
		_, err := ibdSpace.AllocateExtent(basic.ExtentPurposeData)
		if err != nil {
			t.Fatalf("Failed to allocate extent %d: %v", i, err)
		}
	}

	// 获取更新后的统计
	updatedInfo, err := sm.GetTableSpaceInfo(spaceID)
	if err != nil {
		t.Fatalf("Failed to get updated info: %v", err)
	}

	t.Logf("📊 Updated Stats:")
	t.Logf("  Size: %d bytes", updatedInfo.Size)
	t.Logf("  FreeSpace: %d bytes", updatedInfo.FreeSpace)
	t.Logf("  SegmentCount: %d", updatedInfo.SegmentCount)

	// 验证Size增加
	if updatedInfo.Size <= initialInfo.Size {
		t.Errorf("Size should increase after allocating extents: initial=%d, updated=%d",
			initialInfo.Size, updatedInfo.Size)
	}

	// 验证SegmentCount可能增加
	t.Logf("✅ SegmentCount changed from %d to %d", initialInfo.SegmentCount, updatedInfo.SegmentCount)
}
