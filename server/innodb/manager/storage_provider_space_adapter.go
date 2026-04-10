package manager

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// newStorageProviderSpaceManager 将 basic.StorageProvider 包装为 basic.SpaceManager，
// 供缓冲池 FreeBlockList 通过 GetSpace → LoadPageByPageNumber 读页。
func newStorageProviderSpaceManager(sp basic.StorageProvider) basic.SpaceManager {
	if sp == nil {
		return nil
	}
	return &storageProviderSpaceManager{prov: sp}
}

type storageProviderSpaceManager struct {
	prov basic.StorageProvider
}

func (m *storageProviderSpaceManager) CreateSpace(spaceID uint32, name string, isSystem bool) (basic.Space, error) {
	return newProviderSpace(spaceID, name, isSystem, m.prov), nil
}

func (m *storageProviderSpaceManager) GetSpace(spaceID uint32) (basic.Space, error) {
	return newProviderSpace(spaceID, fmt.Sprintf("space-%d", spaceID), spaceID <= 1, m.prov), nil
}

func (m *storageProviderSpaceManager) DropSpace(spaceID uint32) error {
	return fmt.Errorf("DropSpace not supported on storage provider adapter")
}

func (m *storageProviderSpaceManager) AllocateExtent(spaceID uint32, purpose basic.ExtentPurpose) (basic.Extent, error) {
	return nil, fmt.Errorf("AllocateExtent not supported on storage provider adapter")
}

func (m *storageProviderSpaceManager) FreeExtent(spaceID, extentID uint32) error {
	return fmt.Errorf("FreeExtent not supported on storage provider adapter")
}

func (m *storageProviderSpaceManager) Begin() (basic.Tx, error) {
	return nil, fmt.Errorf("Begin not supported on storage provider adapter")
}

func (m *storageProviderSpaceManager) CreateNewTablespace(name string) uint32 {
	return 1
}

func (m *storageProviderSpaceManager) CreateTableSpace(name string) (uint32, error) {
	return 1, nil
}

func (m *storageProviderSpaceManager) GetTableSpace(spaceID uint32) (basic.FileTableSpace, error) {
	return &fileTableSpaceAdapter{sid: spaceID, prov: m.prov}, nil
}

func (m *storageProviderSpaceManager) GetTableSpaceByName(name string) (basic.FileTableSpace, error) {
	return &fileTableSpaceAdapter{sid: 1, prov: m.prov}, nil
}

func (m *storageProviderSpaceManager) GetTableSpaceInfo(spaceID uint32) (*basic.TableSpaceInfo, error) {
	return nil, fmt.Errorf("GetTableSpaceInfo not supported on storage provider adapter")
}

func (m *storageProviderSpaceManager) DropTableSpace(spaceID uint32) error {
	return fmt.Errorf("DropTableSpace not supported on storage provider adapter")
}

func (m *storageProviderSpaceManager) Close() error {
	if c, ok := m.prov.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

type fileTableSpaceAdapter struct {
	sid  uint32
	prov basic.StorageProvider
}

func (f *fileTableSpaceAdapter) FlushToDisk(pageNo uint32, content []byte) {
	_ = f.prov.WritePage(f.sid, pageNo, content)
}

func (f *fileTableSpaceAdapter) LoadPageByPageNumber(pageNo uint32) ([]byte, error) {
	return f.prov.ReadPage(f.sid, pageNo)
}

func (f *fileTableSpaceAdapter) GetSpaceId() uint32 { return f.sid }

type providerSpace struct {
	id     uint32
	name   string
	isSys  bool
	active bool
	prov   basic.StorageProvider
}

func newProviderSpace(id uint32, name string, isSys bool, prov basic.StorageProvider) *providerSpace {
	return &providerSpace{id: id, name: name, isSys: isSys, active: true, prov: prov}
}

func (s *providerSpace) ID() uint32              { return s.id }
func (s *providerSpace) Name() string            { return s.name }
func (s *providerSpace) IsSystem() bool          { return s.isSys }
func (s *providerSpace) AllocateExtent(purpose basic.ExtentPurpose) (basic.Extent, error) {
	return nil, fmt.Errorf("AllocateExtent not supported")
}
func (s *providerSpace) FreeExtent(extentID uint32) error {
	return fmt.Errorf("FreeExtent not supported")
}
func (s *providerSpace) GetPageCount() uint32    { return 0 }
func (s *providerSpace) GetExtentCount() uint32  { return 0 }
func (s *providerSpace) GetUsedSpace() uint64    { return 0 }
func (s *providerSpace) IsActive() bool          { return s.active }
func (s *providerSpace) SetActive(active bool)   { s.active = active }
func (s *providerSpace) LoadPageByPageNumber(no uint32) ([]byte, error) {
	return s.prov.ReadPage(s.id, no)
}
func (s *providerSpace) FlushToDisk(no uint32, content []byte) error {
	return s.prov.WritePage(s.id, no, content)
}
