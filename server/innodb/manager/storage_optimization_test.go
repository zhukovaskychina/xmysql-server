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

func TestReclaimSpaceReleasesEmptySegmentExtents(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:              "testdata/storage_opt",
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
	}
	sm := NewStorageManager(cfg)
	defer sm.Close()
	handle, err := sm.CreateTablespace("test_reclaim_empty_extent")
	if err != nil {
		t.Fatalf("CreateTablespace: %v", err)
	}
	segment, err := sm.CreateSegment(handle.SpaceID, basic.SegmentPurposeNonLeaf)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}
	segImpl := segment.(*SegmentImpl)
	if err := sm.DefragmentSpace(handle.SpaceID); err != nil {
		t.Fatalf("DefragmentSpace: %v", err)
	}
	if len(segImpl.FreeExtents) != 1 {
		t.Fatalf("expected one empty extent after defragmentation, got %d", len(segImpl.FreeExtents))
	}

	reclaimed, err := sm.ReclaimSpace(handle.SpaceID)
	if err != nil {
		t.Fatalf("ReclaimSpace: %v", err)
	}
	if reclaimed != uint64(PagesPerExtent*PageSize) {
		t.Fatalf("reclaimed = %d, want %d", reclaimed, uint64(PagesPerExtent*PageSize))
	}
	if segImpl.ExtentCount != 0 || len(segImpl.FreeExtents) != 0 {
		t.Fatalf("empty extent still owned by segment: count=%d free=%d", segImpl.ExtentCount, len(segImpl.FreeExtents))
	}
	if got := sm.segmentMgr.extentManager.GetFreeExtentCount(); got != 1 {
		t.Fatalf("free extent count = %d, want 1", got)
	}
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

func TestOptimizeStorageDoesNotPreallocateUnconditionally(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
	}
	sm := NewStorageManager(cfg)
	defer sm.Close()
	handle, err := sm.CreateTablespace("test_optimize_no_growth")
	if err != nil {
		t.Fatalf("CreateTablespace: %v", err)
	}
	beforeInfo, err := sm.GetSpaceInfo(handle.SpaceID)
	if err != nil {
		t.Fatalf("GetSpaceInfo before optimize: %v", err)
	}
	if err := sm.OptimizeStorage(handle.SpaceID); err != nil {
		t.Fatalf("OptimizeStorage: %v", err)
	}
	afterInfo, err := sm.GetSpaceInfo(handle.SpaceID)
	if err != nil {
		t.Fatalf("GetSpaceInfo after optimize: %v", err)
	}
	if afterInfo.TotalPages != beforeInfo.TotalPages {
		t.Fatalf("OptimizeStorage changed tablespace pages from %d to %d", beforeInfo.TotalPages, afterInfo.TotalPages)
	}
}

func TestFirstPageFromNewExtentUpdatesSegmentStats(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
	}
	sm := NewStorageManager(cfg)
	defer sm.Close()
	handle, err := sm.CreateTablespace("test_new_extent_stats")
	if err != nil {
		t.Fatalf("CreateTablespace: %v", err)
	}
	segment, err := sm.GetSegmentManager().CreateSegment(handle.SpaceID, SEGMENT_TYPE_BLOB, false)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}
	seg := segment.(*SegmentImpl)
	statsBefore := *sm.GetSegmentManager().stats
	if _, err := sm.GetSegmentManager().AllocatePage(seg.SegmentID); err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	if seg.PageCount != 1 {
		t.Fatalf("segment page count = %d, want 1", seg.PageCount)
	}
	if seg.ExtentCount != 1 {
		t.Fatalf("segment extent count = %d, want 1", seg.ExtentCount)
	}
	statsAfter := *sm.GetSegmentManager().stats
	if statsAfter.TotalPages != statsBefore.TotalPages+1 {
		t.Fatalf("total pages changed from %d to %d, want +1", statsBefore.TotalPages, statsAfter.TotalPages)
	}
}

func TestAutomaticEmptyExtentReleaseUpdatesFreeSpace(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
	}
	sm := NewStorageManager(cfg)
	defer sm.Close()
	handle, err := sm.CreateTablespace("test_auto_extent_release_stats")
	if err != nil {
		t.Fatalf("CreateTablespace: %v", err)
	}
	segment, err := sm.GetSegmentManager().CreateSegment(handle.SpaceID, SEGMENT_TYPE_BLOB, false)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}
	seg := segment.(*SegmentImpl)
	managerFreeSpaceBefore := sm.GetSegmentManager().stats.FreeSpace
	pageNos := make([]uint32, 0, 3*PagesPerExtent)
	for i := 0; i < 3*PagesPerExtent; i++ {
		pageNo, allocErr := sm.GetSegmentManager().AllocatePage(seg.SegmentID)
		if allocErr != nil {
			t.Fatalf("AllocatePage(%d): %v", i, allocErr)
		}
		pageNos = append(pageNos, pageNo)
	}
	for _, pageNo := range pageNos {
		if freeErr := sm.GetSegmentManager().FreePage(seg.SegmentID, pageNo); freeErr != nil {
			t.Fatalf("FreePage(%d): %v", pageNo, freeErr)
		}
	}
	if seg.ExtentCount != 2 {
		t.Fatalf("extent count after automatic release = %d, want 2", seg.ExtentCount)
	}
	wantFreeSpace := uint64(2 * PagesPerExtent * PageSize)
	if seg.FreeSpace != wantFreeSpace {
		t.Fatalf("segment free space = %d, want %d", seg.FreeSpace, wantFreeSpace)
	}
	wantManagerFreeSpace := managerFreeSpaceBefore + wantFreeSpace
	if sm.GetSegmentManager().stats.FreeSpace != wantManagerFreeSpace {
		t.Fatalf("manager free space = %d, want %d", sm.GetSegmentManager().stats.FreeSpace, wantManagerFreeSpace)
	}
}

func TestOptimizeStorageShrinksTrailingTablespacePages(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
	}
	sm := NewStorageManager(cfg)
	defer sm.Close()
	handle, err := sm.CreateTablespace("test_optimize_shrink_tail")
	if err != nil {
		t.Fatalf("CreateTablespace: %v", err)
	}
	space, err := sm.GetSpaceManager().GetSpace(handle.SpaceID)
	if err != nil {
		t.Fatalf("GetSpace: %v", err)
	}
	extra, err := space.AllocateExtent(basic.ExtentPurposeData)
	if err != nil {
		t.Fatalf("AllocateExtent: %v", err)
	}
	if err := space.FlushToDisk(127, make([]byte, 16384)); err != nil {
		t.Fatalf("FlushToDisk: %v", err)
	}
	if err := space.FreeExtent(extra.GetID()); err != nil {
		t.Fatalf("FreeExtent: %v", err)
	}
	sized, ok := space.(interface{ GetTotalSize() uint64 })
	if !ok {
		t.Fatal("tablespace does not expose physical size")
	}
	if sized.GetTotalSize() != uint64(2*PagesPerExtent*PageSize) {
		t.Fatalf("pre-optimization size = %d, want %d", sized.GetTotalSize(), uint64(2*PagesPerExtent*PageSize))
	}
	if err := sm.OptimizeStorage(handle.SpaceID); err != nil {
		t.Fatalf("OptimizeStorage: %v", err)
	}
	if got := sized.GetTotalSize(); got != uint64(PagesPerExtent*PageSize) {
		t.Fatalf("post-optimization size = %d, want %d", got, uint64(PagesPerExtent*PageSize))
	}
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
