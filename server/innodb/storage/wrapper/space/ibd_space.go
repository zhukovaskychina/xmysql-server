/*
IBDSpace 实现了InnoDB表空间的管理。主要功能包括：

1. 物理存储管理
   - 文件创建、打开、关闭和删除
   - 页面读写和同步
   - 文件空间分配

2. 逻辑空间管理
   - 区(Extent)管理
   - 页面分配和回收
   - 空间使用统计

3. 并发控制
   - 读写锁保护
   - 原子操作
   - 状态管理

设计要点：
1. 分层设计
   - IBD_File: 底层文件操作
   - Extent: 区管理
   - IBDSpace: 表空间管理

2. 状态管理
   - 活动状态跟踪
   - 错误处理
   - 资源清理

3. 性能优化
   - 批量分配
   - 缓存友好
   - 最小化锁竞争
*/

package space

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/ibd"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/extent"
	"sync"
)

// IBDSpace represents a tablespace in the storage engine
type IBDSpace struct {
	sync.RWMutex

	// Physical storage
	ibdFile *ibd.IBD_File // 物理文件管理

	// Metadata
	id       uint32 // 表空间ID
	name     string // 表空间名称
	isSystem bool   // 是否系统表空间
	active   bool   // 活动状态

	// Space management
	nextExtent uint32                           // 下一个可用的区ID
	nextPage   uint32                           // 下一个可用的页号
	extents    map[uint32]*extent.UnifiedExtent // 区管理器 (使用 UnifiedExtent)
	pageAllocs map[uint32]bool                  // 页面分配表

	// Statistics
	pageCount     uint32 // 已分配的页面数
	extentCount   uint32 // 已分配的区数
	fragmentCount uint32 // 碎片数量
}

// GetPageCount returns the number of allocated pages
func (s *IBDSpace) GetPageCount() uint32 {
	s.RLock()
	defer s.RUnlock()
	return s.pageCount
}

// GetExtentCount returns the number of allocated extents
func (s *IBDSpace) GetExtentCount() uint32 {
	s.RLock()
	defer s.RUnlock()
	return s.extentCount
}

// GetFragmentCount returns the number of fragments
func (s *IBDSpace) GetFragmentCount() uint32 {
	s.RLock()
	defer s.RUnlock()
	return s.fragmentCount
}

// GetUsedSpace returns the total used space in bytes
func (s *IBDSpace) GetUsedSpace() uint64 {
	s.RLock()
	defer s.RUnlock()
	return uint64(s.pageCount) * PageSize
}

// IsActive returns whether the tablespace is active
func (s *IBDSpace) IsActive() bool {
	s.RLock()
	defer s.RUnlock()
	return s.active
}

// SetActive sets the active state of the tablespace
func (s *IBDSpace) SetActive(active bool) {
	s.Lock()
	defer s.Unlock()
	s.active = active
}

// NewIBDSpace creates a new IBD space
func NewIBDSpace(ibdFile *ibd.IBD_File, isSystem bool) *IBDSpace {
	return &IBDSpace{
		ibdFile:    ibdFile,
		id:         ibdFile.GetSpaceId(),
		name:       ibdFile.GetTableName(),
		isSystem:   isSystem,
		active:     true,
		nextExtent: 0,
		nextPage:   0,
		extents:    make(map[uint32]*extent.UnifiedExtent),
		pageAllocs: make(map[uint32]bool),
		// Statistics
		pageCount:     0,
		extentCount:   0,
		fragmentCount: 0,
	}
}

// ID returns the space ID
func (s *IBDSpace) ID() uint32 {
	return s.id
}

// Name returns the space name
func (s *IBDSpace) Name() string {
	return s.name
}

// IsSystem returns whether this is a system tablespace
func (s *IBDSpace) IsSystem() bool {
	return s.isSystem
}

// AllocateExtent allocates a new extent
func (s *IBDSpace) AllocateExtent(purpose basic.ExtentPurpose) (basic.Extent, error) {
	s.Lock()
	defer s.Unlock()

	if !s.active {
		return nil, fmt.Errorf("tablespace %d is not active", s.id)
	}

	// Generate new extent ID
	extentID := s.nextExtent
	s.nextExtent++

	// Create new extent starting at current next page
	// Use UnifiedExtent instead of old ExtentImpl
	newExtent := extent.NewUnifiedExtent(
		extentID,
		s.id,
		s.nextPage,
		basic.ExtentTypeData, // Default to data extent
		purpose,
	)

	// Update next page number (each extent has 64 pages)
	startPage := s.nextPage
	s.nextPage += PagesPerExtent

	// Mark all pages in the extent as allocated
	for i := uint32(0); i < PagesPerExtent; i++ {
		s.pageAllocs[startPage+i] = true
		s.pageCount++
	}

	// Save extent
	s.extents[extentID] = newExtent
	s.extentCount++

	return newExtent, nil
}

// FreeExtent frees an extent
func (s *IBDSpace) FreeExtent(extentID uint32) error {
	s.Lock()
	defer s.Unlock()

	if !s.active {
		return fmt.Errorf("tablespace %d is not active", s.id)
	}

	extent, exists := s.extents[extentID]
	if !exists {
		return fmt.Errorf("extent %d not found", extentID)
	}

	// Free all pages in the extent
	for pageNo := extent.StartPage(); pageNo < extent.StartPage()+PagesPerExtent; pageNo++ {
		pageNoUint32 := uint32(pageNo)
		if s.pageAllocs[pageNoUint32] {
			delete(s.pageAllocs, pageNoUint32)
			s.pageCount--
		}
	}

	// Remove extent
	delete(s.extents, extentID)
	s.extentCount--

	return nil
}

// FileTableSpace interface implementation

// FlushToDisk writes a page to disk
func (s *IBDSpace) FlushToDisk(pageNo uint32, content []byte) error {
	s.Lock()
	defer s.Unlock()

	if !s.active {
		return fmt.Errorf("tablespace %d is not active", s.id)
	}

	// Validate page allocation
	if !s.pageAllocs[pageNo] {
		return fmt.Errorf("page %d is not allocated", pageNo)
	}

	// Validate content size
	if len(content) != PageSize {
		return fmt.Errorf("invalid page size: got %d, want %d", len(content), PageSize)
	}

	// Write to file
	if err := s.ibdFile.WritePage(pageNo, content); err != nil {
		return fmt.Errorf("failed to write page %d: %v", pageNo, err)
	}

	return nil
}

// LoadPageByPageNumber reads a page from disk
func (s *IBDSpace) LoadPageByPageNumber(pageNo uint32) ([]byte, error) {
	s.RLock()
	defer s.RUnlock()

	if !s.active {
		return nil, fmt.Errorf("tablespace %d is not active", s.id)
	}

	// Validate page allocation
	if !s.pageAllocs[pageNo] {
		return nil, fmt.Errorf("page %d is not allocated", pageNo)
	}

	// Read from file
	content, err := s.ibdFile.ReadPage(pageNo)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %v", pageNo, err)
	}

	// Validate content size
	if len(content) != PageSize {
		return nil, fmt.Errorf("invalid page size: got %d, want %d", len(content), PageSize)
	}

	return content, nil
}

func (s *IBDSpace) GetSpaceId() uint32 {
	s.RLock()
	defer s.RUnlock()
	return s.id
}

func (s *IBDSpace) GetTableName() string {
	s.RLock()
	defer s.RUnlock()
	return s.name
}

func (s *IBDSpace) GetFilePath() string {
	s.RLock()
	defer s.RUnlock()
	return s.ibdFile.GetFilePath()
}

func (s *IBDSpace) GetNextExtentID() uint32 {
	s.RLock()
	defer s.RUnlock()
	return s.nextExtent
}

func (s *IBDSpace) GetNextPageID() uint32 {
	s.RLock()
	defer s.RUnlock()
	return s.nextPage
}

// Initialize initializes the tablespace
func (s *IBDSpace) Initialize() error {
	s.Lock()
	defer s.Unlock()

	// Create file if it doesn't exist
	if err := s.ibdFile.Create(); err != nil {
		return fmt.Errorf("failed to create IBD file: %v", err)
	}

	// Mark space as active
	s.active = true

	// Allocate first extent for system pages
	extent, err := s.AllocateExtent(basic.ExtentPurposeSystem)
	if err != nil {
		return fmt.Errorf("failed to allocate system extent: %v", err)
	}

	// Mark all pages in first extent as allocated
	for i := extent.StartPage(); i < extent.StartPage()+PagesPerExtent; i++ {
		s.pageAllocs[uint32(i)] = true
		s.pageCount++
	}

	return nil
}

// DropTable drops the tablespace and its physical file
func (s *IBDSpace) DropTable() error {
	s.Lock()
	defer s.Unlock()

	// Mark space as inactive
	s.active = false

	// Clear all allocations and statistics
	s.extents = make(map[uint32]*extent.UnifiedExtent)
	s.pageAllocs = make(map[uint32]bool)
	s.pageCount = 0
	s.extentCount = 0
	s.fragmentCount = 0

	// Delete file
	if err := s.ibdFile.Delete(); err != nil {
		return fmt.Errorf("failed to delete IBD file: %v", err)
	}

	return nil
}

// Close closes the tablespace and its physical file
func (s *IBDSpace) Close() error {
	s.Lock()
	defer s.Unlock()

	// Mark space as inactive
	s.active = false

	// Clear all allocations and statistics
	s.extents = make(map[uint32]*extent.UnifiedExtent)
	s.pageAllocs = make(map[uint32]bool)
	s.pageCount = 0
	s.extentCount = 0
	s.fragmentCount = 0

	// Close file
	if err := s.ibdFile.Close(); err != nil {
		return fmt.Errorf("failed to close IBD file: %v", err)
	}

	return nil
}

// FileTableSpaceAdapter wraps IBDSpace to implement FileTableSpace interface
type FileTableSpaceAdapter struct {
	*IBDSpace
}

// FlushToDisk implements FileTableSpace interface (no error return)
func (adapter *FileTableSpaceAdapter) FlushToDisk(pageNo uint32, content []byte) {
	// Call the underlying method but ignore the error
	_ = adapter.IBDSpace.FlushToDisk(pageNo, content)
}

// AsFileTableSpace returns a FileTableSpace adapter for this IBDSpace
func (s *IBDSpace) AsFileTableSpace() basic.FileTableSpace {
	return &FileTableSpaceAdapter{IBDSpace: s}
}

// GetTotalSize returns the total size of the tablespace in bytes
func (s *IBDSpace) GetTotalSize() uint64 {
	s.RLock()
	defer s.RUnlock()

	// Total size = next page number * page size
	// This represents the allocated space (not necessarily all used)
	return uint64(s.nextPage) * PageSize
}

// GetFreeSpace returns the free space in bytes
func (s *IBDSpace) GetFreeSpace() uint64 {
	s.RLock()
	defer s.RUnlock()

	// Free space = (allocated pages - used pages) * page size
	allocatedPages := s.nextPage
	usedPages := s.pageCount

	if allocatedPages > usedPages {
		return uint64(allocatedPages-usedPages) * PageSize
	}
	return 0
}

// GetSegmentCount returns the number of segments in the tablespace
// Note: This is a simplified implementation
// In a full implementation, this would track actual segments
func (s *IBDSpace) GetSegmentCount() uint32 {
	s.RLock()
	defer s.RUnlock()

	// Simplified: estimate based on extents
	// Typically, each segment has multiple extents
	// For now, we estimate 1 segment per 4 extents (data + index segments)
	if s.extentCount == 0 {
		return 0
	}

	// At minimum, we have 1 segment
	// Add 1 segment for every 4 extents
	segmentCount := uint32(1 + (s.extentCount / 4))
	return segmentCount
}

// GetFreeExtentCount returns the number of free extents
func (s *IBDSpace) GetFreeExtentCount() uint32 {
	s.RLock()
	defer s.RUnlock()

	freeCount := uint32(0)
	for _, ext := range s.extents {
		if ext.IsEmpty() {
			freeCount++
		}
	}
	return freeCount
}

// GetPartialExtentCount returns the number of partially used extents
func (s *IBDSpace) GetPartialExtentCount() uint32 {
	s.RLock()
	defer s.RUnlock()

	partialCount := uint32(0)
	for _, ext := range s.extents {
		if !ext.IsEmpty() && !ext.IsFull() {
			partialCount++
		}
	}
	return partialCount
}

// GetFullExtentCount returns the number of fully used extents
func (s *IBDSpace) GetFullExtentCount() uint32 {
	s.RLock()
	defer s.RUnlock()

	fullCount := uint32(0)
	for _, ext := range s.extents {
		if ext.IsFull() {
			fullCount++
		}
	}
	return fullCount
}

// GetDetailedStats returns detailed statistics about the tablespace
func (s *IBDSpace) GetDetailedStats() *SpaceDetailedStats {
	s.RLock()
	defer s.RUnlock()

	stats := &SpaceDetailedStats{
		SpaceID:        s.id,
		Name:           s.name,
		TotalPages:     s.nextPage,
		AllocatedPages: s.pageCount,
		TotalExtents:   s.extentCount,
		FreeExtents:    0,
		PartialExtents: 0,
		FullExtents:    0,
		TotalSize:      uint64(s.nextPage) * PageSize,
		UsedSize:       uint64(s.pageCount) * PageSize,
		FreeSize:       0,
		FragmentCount:  s.fragmentCount,
	}

	// Calculate extent statistics
	for _, ext := range s.extents {
		if ext.IsEmpty() {
			stats.FreeExtents++
		} else if ext.IsFull() {
			stats.FullExtents++
		} else {
			stats.PartialExtents++
		}
	}

	// Calculate free size
	if stats.TotalPages > stats.AllocatedPages {
		stats.FreeSize = uint64(stats.TotalPages-stats.AllocatedPages) * PageSize
	}

	return stats
}

// SpaceDetailedStats contains detailed statistics about a tablespace
type SpaceDetailedStats struct {
	SpaceID        uint32
	Name           string
	TotalPages     uint32
	AllocatedPages uint32
	TotalExtents   uint32
	FreeExtents    uint32
	PartialExtents uint32
	FullExtents    uint32
	TotalSize      uint64
	UsedSize       uint64
	FreeSize       uint64
	FragmentCount  uint32
}
