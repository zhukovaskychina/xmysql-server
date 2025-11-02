package manager

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// TestPreallocateSpace 测试空间预分配
func TestPreallocateSpace(t *testing.T) {
	// 创建测试配置
	cfg := &conf.Cfg{
		DataDir:              "testdata/storage_opt",
		InnodbBufferPoolSize: 16 * 1024 * 1024, // 16MB
		InnodbPageSize:       16384,
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
	}

	// 创建StorageManager
	sm := NewStorageManager(cfg)
	defer sm.Close()

	// 创建测试表空间
	handle, err := sm.CreateTablespace("test_preallocate")
	if err != nil {
		t.Fatalf("Failed to create tablespace: %v", err)
	}

	// 预分配5个extent
	err = sm.PreallocateSpace(handle.SpaceID, 5)
	if err != nil {
		t.Errorf("PreallocateSpace failed: %v", err)
	}

	// 验证空间已分配
	spaceInfo, err := sm.GetSpaceInfo(handle.SpaceID)
	if err != nil {
		t.Errorf("Failed to get space info: %v", err)
	}

	// 计算extent数量：TotalPages / ExtentSize (64 pages per extent)
	extentCount := spaceInfo.TotalPages / uint64(spaceInfo.ExtentSize)
	if extentCount < 5 {
		t.Errorf("Expected at least 5 extents, got %d", extentCount)
	}

	t.Logf("Successfully preallocated space: %d extents", extentCount)
}

// TestDefragmentSpace 测试碎片整理
func TestDefragmentSpace(t *testing.T) {
	// 创建测试配置
	cfg := &conf.Cfg{
		DataDir:              "testdata/storage_opt",
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
	}

	// 创建StorageManager
	sm := NewStorageManager(cfg)
	defer sm.Close()

	// 创建测试表空间
	handle, err := sm.CreateTablespace("test_defragment")
	if err != nil {
		t.Fatalf("Failed to create tablespace: %v", err)
	}

	// 创建一些segment
	_, err = sm.CreateSegment(handle.SpaceID, basic.SegmentPurposeLeaf)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}

	// 执行碎片整理
	err = sm.DefragmentSpace(handle.SpaceID)
	if err != nil {
		t.Errorf("DefragmentSpace failed: %v", err)
	}

	t.Logf("Successfully defragmented space %d", handle.SpaceID)
}

// TestReclaimSpace 测试空间回收
func TestReclaimSpace(t *testing.T) {
	// 创建测试配置
	cfg := &conf.Cfg{
		DataDir:              "testdata/storage_opt",
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
	}

	// 创建StorageManager
	sm := NewStorageManager(cfg)
	defer sm.Close()

	// 创建测试表空间
	handle, err := sm.CreateTablespace("test_reclaim")
	if err != nil {
		t.Fatalf("Failed to create tablespace: %v", err)
	}

	// 创建segment
	_, err = sm.CreateSegment(handle.SpaceID, basic.SegmentPurposeLeaf)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}

	// 执行空间回收
	reclaimed, err := sm.ReclaimSpace(handle.SpaceID)
	if err != nil {
		t.Errorf("ReclaimSpace failed: %v", err)
	}

	t.Logf("Successfully reclaimed %d bytes from space %d", reclaimed, handle.SpaceID)
}

// TestOptimizeStorage 测试综合存储优化
func TestOptimizeStorage(t *testing.T) {
	// 创建测试配置
	cfg := &conf.Cfg{
		DataDir:              "testdata/storage_opt",
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
	}

	// 创建StorageManager
	sm := NewStorageManager(cfg)
	defer sm.Close()

	// 创建测试表空间
	handle, err := sm.CreateTablespace("test_optimize")
	if err != nil {
		t.Fatalf("Failed to create tablespace: %v", err)
	}

	// 创建segment
	_, err = sm.CreateSegment(handle.SpaceID, basic.SegmentPurposeLeaf)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}

	// 执行综合优化
	err = sm.OptimizeStorage(handle.SpaceID)
	if err != nil {
		t.Errorf("OptimizeStorage failed: %v", err)
	}

	t.Logf("Successfully optimized storage for space %d", handle.SpaceID)
}

// TestSegmentDefragment 测试Segment碎片整理
func TestSegmentDefragment(t *testing.T) {
	// 创建测试配置
	cfg := &conf.Cfg{
		DataDir:              "testdata/storage_opt",
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
	}

	// 创建StorageManager
	sm := NewStorageManager(cfg)
	defer sm.Close()

	// 创建测试表空间
	handle, err := sm.CreateTablespace("test_seg_defrag")
	if err != nil {
		t.Fatalf("Failed to create tablespace: %v", err)
	}

	// 创建segment
	segment, err := sm.CreateSegment(handle.SpaceID, basic.SegmentPurposeLeaf)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}

	// 类型断言为SegmentImpl
	segImpl, ok := segment.(*SegmentImpl)
	if !ok {
		t.Fatalf("Segment is not a SegmentImpl")
	}

	// 执行碎片整理
	err = segImpl.Defragment()
	if err != nil {
		t.Errorf("Segment Defragment failed: %v", err)
	}

	// 验证空闲空间
	freeSpace := segImpl.GetFreeSpace()
	t.Logf("Segment %d has %d bytes free space after defragmentation", segImpl.GetID(), freeSpace)
}

// TestFreeSegment 测试释放Segment
func TestFreeSegment(t *testing.T) {
	// 创建测试配置
	cfg := &conf.Cfg{
		DataDir:              "testdata/storage_opt",
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
	}

	// 创建StorageManager
	sm := NewStorageManager(cfg)
	defer sm.Close()

	// 创建测试表空间
	handle, err := sm.CreateTablespace("test_free_seg")
	if err != nil {
		t.Fatalf("Failed to create tablespace: %v", err)
	}

	// 创建segment
	segment, err := sm.CreateSegment(handle.SpaceID, basic.SegmentPurposeLeaf)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}

	// 类型断言获取ID
	segImpl, ok := segment.(*SegmentImpl)
	if !ok {
		t.Fatalf("Segment is not a SegmentImpl")
	}
	segID := uint64(segImpl.GetID())

	// 释放segment
	err = sm.FreeSegment(segID)
	if err != nil {
		t.Errorf("FreeSegment failed: %v", err)
	}

	// 验证segment已被删除
	_, err = sm.GetSegment(segID)
	if err == nil {
		t.Errorf("Expected segment to be deleted, but it still exists")
	}

	t.Logf("Successfully freed segment %d", segID)
}
