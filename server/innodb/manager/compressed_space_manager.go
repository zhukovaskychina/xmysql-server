package manager

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// CompressedSpaceManager applies the provider's transparent page policy to
// direct Space and FileTableSpace users, keeping them on the same durable
// path as the optimized buffer pool.
type CompressedSpaceManager struct {
	inner    basic.SpaceManager
	provider *CompressedStorageProvider
}

func NewCompressedSpaceManager(inner basic.SpaceManager, provider *CompressedStorageProvider) (*CompressedSpaceManager, error) {
	if inner == nil {
		return nil, fmt.Errorf("space manager is required")
	}
	if provider == nil {
		return nil, fmt.Errorf("compressed storage provider is required")
	}
	return &CompressedSpaceManager{inner: inner, provider: provider}, nil
}

func (m *CompressedSpaceManager) GetSpace(spaceID uint32) (basic.Space, error) {
	space, err := m.inner.GetSpace(spaceID)
	if err != nil {
		return nil, err
	}
	if m.provider.IsSpaceCompressed(spaceID) {
		m.provider.ensureSpaceSettings(spaceID)
		return &compressedSpace{inner: space, provider: m.provider}, nil
	}
	return space, nil
}

func (m *CompressedSpaceManager) CreateSpace(spaceID uint32, name string, isSystem bool) (basic.Space, error) {
	space, err := m.inner.CreateSpace(spaceID, name, isSystem)
	if err != nil {
		return nil, err
	}
	if m.provider.IsSpaceCompressed(spaceID) {
		m.provider.SetSpaceCompressed(spaceID, true)
		m.provider.ensureSpaceSettings(spaceID)
		return &compressedSpace{inner: space, provider: m.provider}, nil
	}
	return space, nil
}

func (m *CompressedSpaceManager) DropSpace(spaceID uint32) error {
	if err := m.inner.DropSpace(spaceID); err != nil {
		return err
	}
	m.provider.SetSpaceCompressed(spaceID, false)
	return nil
}

func (m *CompressedSpaceManager) AllocateExtent(spaceID uint32, purpose basic.ExtentPurpose) (basic.Extent, error) {
	return m.inner.AllocateExtent(spaceID, purpose)
}
func (m *CompressedSpaceManager) FreeExtent(spaceID, extentID uint32) error {
	return m.inner.FreeExtent(spaceID, extentID)
}
func (m *CompressedSpaceManager) Begin() (basic.Tx, error) { return m.inner.Begin() }

func (m *CompressedSpaceManager) CreateNewTablespace(name string) uint32 {
	id, err := m.CreateTableSpace(name)
	if err != nil {
		return 0
	}
	return id
}

func (m *CompressedSpaceManager) CreateTableSpace(name string) (uint32, error) {
	id, err := m.inner.CreateTableSpace(name)
	if err != nil {
		return 0, err
	}
	if m.provider.IsSpaceCompressed(id) {
		m.provider.SetSpaceCompressed(id, true)
		m.provider.ensureSpaceSettings(id)
	}
	return id, nil
}

func (m *CompressedSpaceManager) GetTableSpace(spaceID uint32) (basic.FileTableSpace, error) {
	space, err := m.inner.GetTableSpace(spaceID)
	if err != nil {
		return nil, err
	}
	if !m.provider.IsSpaceCompressed(spaceID) {
		return space, nil
	}
	m.provider.ensureSpaceSettings(spaceID)
	return &compressedFileTableSpace{inner: space, provider: m.provider}, nil
}

func (m *CompressedSpaceManager) GetTableSpaceByName(name string) (basic.FileTableSpace, error) {
	space, err := m.inner.GetTableSpaceByName(name)
	if err != nil {
		return nil, err
	}
	if !m.provider.IsSpaceCompressed(space.GetSpaceId()) {
		return space, nil
	}
	m.provider.ensureSpaceSettings(space.GetSpaceId())
	return &compressedFileTableSpace{inner: space, provider: m.provider}, nil
}

func (m *CompressedSpaceManager) GetTableSpaceInfo(spaceID uint32) (*basic.TableSpaceInfo, error) {
	return m.inner.GetTableSpaceInfo(spaceID)
}

func (m *CompressedSpaceManager) DropTableSpace(spaceID uint32) error {
	if err := m.inner.DropTableSpace(spaceID); err != nil {
		return err
	}
	m.provider.SetSpaceCompressed(spaceID, false)
	return nil
}

func (m *CompressedSpaceManager) Close() error { return m.inner.Close() }

type compressedSpace struct {
	inner    basic.Space
	provider *CompressedStorageProvider
}

func (s *compressedSpace) ID() uint32             { return s.inner.ID() }
func (s *compressedSpace) Name() string           { return s.inner.Name() }
func (s *compressedSpace) IsSystem() bool         { return s.inner.IsSystem() }
func (s *compressedSpace) IsActive() bool         { return s.inner.IsActive() }
func (s *compressedSpace) SetActive(v bool)       { s.inner.SetActive(v) }
func (s *compressedSpace) GetPageCount() uint32   { return s.inner.GetPageCount() }
func (s *compressedSpace) GetExtentCount() uint32 { return s.inner.GetExtentCount() }
func (s *compressedSpace) GetUsedSpace() uint64   { return s.inner.GetUsedSpace() }
func (s *compressedSpace) AllocateExtent(p basic.ExtentPurpose) (basic.Extent, error) {
	return s.inner.AllocateExtent(p)
}
func (s *compressedSpace) FreeExtent(id uint32) error { return s.inner.FreeExtent(id) }
func (s *compressedSpace) LoadPageByPageNumber(pageNo uint32) ([]byte, error) {
	return s.provider.ReadPage(s.ID(), pageNo)
}
func (s *compressedSpace) FlushToDisk(pageNo uint32, content []byte) error {
	return s.provider.WritePage(s.ID(), pageNo, content)
}
func (s *compressedSpace) AllocatePage() (uint32, error) {
	if allocator, ok := s.inner.(interface{ AllocatePage() (uint32, error) }); ok {
		return allocator.AllocatePage()
	}
	return 0, fmt.Errorf("page allocation is not supported for space %d", s.ID())
}
func (s *compressedSpace) FreePage(pageNo uint32) error {
	if reclaimer, ok := s.inner.(interface{ FreePage(uint32) error }); ok {
		return reclaimer.FreePage(pageNo)
	}
	return fmt.Errorf("page reclamation is not supported for space %d", s.ID())
}
func (s *compressedSpace) ShrinkToFit() error {
	if shrinker, ok := s.inner.(interface{ ShrinkToFit() error }); ok {
		return shrinker.ShrinkToFit()
	}
	return fmt.Errorf("tablespace shrink is not supported for space %d", s.ID())
}

type compressedFileTableSpace struct {
	inner    basic.FileTableSpace
	provider *CompressedStorageProvider
}

func (s *compressedFileTableSpace) FlushToDisk(pageNo uint32, content []byte) {
	_ = s.provider.WritePage(s.GetSpaceId(), pageNo, content)
}
func (s *compressedFileTableSpace) LoadPageByPageNumber(pageNo uint32) ([]byte, error) {
	return s.provider.ReadPage(s.GetSpaceId(), pageNo)
}
func (s *compressedFileTableSpace) GetSpaceId() uint32 { return s.inner.GetSpaceId() }

var _ basic.SpaceManager = (*CompressedSpaceManager)(nil)
var _ basic.Space = (*compressedSpace)(nil)
var _ basic.FileTableSpace = (*compressedFileTableSpace)(nil)
