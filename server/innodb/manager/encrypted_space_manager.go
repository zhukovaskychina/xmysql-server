package manager

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// EncryptedSpaceManager applies the same page policy to direct Space and
// FileTableSpace consumers that the buffer-pool StorageProvider uses. The
// underlying manager remains responsible for allocation and metadata; this
// decorator only changes page read/write bytes.
type EncryptedSpaceManager struct {
	inner      basic.SpaceManager
	provider   *EncryptedStorageProvider
	encryption *EncryptionManager
	keyring    string
}

func NewEncryptedSpaceManager(inner basic.SpaceManager, provider *EncryptedStorageProvider, encryption *EncryptionManager, keyringPath string) (*EncryptedSpaceManager, error) {
	if inner == nil {
		return nil, fmt.Errorf("space manager is required")
	}
	if provider == nil {
		return nil, fmt.Errorf("encrypted storage provider is required")
	}
	if encryption == nil {
		return nil, fmt.Errorf("encryption manager is required")
	}
	return &EncryptedSpaceManager{inner: inner, provider: provider, encryption: encryption, keyring: keyringPath}, nil
}

func (m *EncryptedSpaceManager) GetSpace(spaceID uint32) (basic.Space, error) {
	space, err := m.inner.GetSpace(spaceID)
	if err != nil {
		return nil, err
	}
	if m.encryption.GetKey(spaceID) == nil {
		return space, nil
	}
	m.provider.SetSpaceEncrypted(spaceID, true)
	return &encryptedSpace{inner: space, provider: m.provider}, nil
}

func (m *EncryptedSpaceManager) provisionSpace(spaceID uint32) error {
	if m.encryption.GetKey(spaceID) == nil {
		if _, err := m.encryption.CreateKey(spaceID); err != nil {
			return err
		}
		if err := m.provider.EncryptSpace(spaceID); err != nil {
			return err
		}
	} else {
		m.provider.SetSpaceEncrypted(spaceID, true)
	}
	if m.keyring != "" {
		return m.encryption.SaveKeyring(m.keyring)
	}
	return nil
}

// MigrateExistingSpaces encrypts plaintext spaces discovered before the
// master-key/keyring feature was enabled. Spaces already represented in the
// keyring are only reattached to the encrypted policy; their pages are not
// rewritten on every restart.
func (m *EncryptedSpaceManager) MigrateExistingSpaces() error {
	enumerator, ok := m.inner.(interface{ ListSpaceIDs() []uint32 })
	if !ok {
		return nil
	}
	for _, spaceID := range enumerator.ListSpaceIDs() {
		if err := m.provisionSpace(spaceID); err != nil {
			return fmt.Errorf("migrate existing space %d: %w", spaceID, err)
		}
	}
	return nil
}

func (m *EncryptedSpaceManager) CreateSpace(spaceID uint32, name string, isSystem bool) (basic.Space, error) {
	space, err := m.inner.CreateSpace(spaceID, name, isSystem)
	if err != nil {
		return nil, err
	}
	if err := m.provisionSpace(spaceID); err != nil {
		return nil, fmt.Errorf("encrypt new space %d: %w", spaceID, err)
	}
	return &encryptedSpace{inner: space, provider: m.provider}, nil
}

func (m *EncryptedSpaceManager) DropSpace(spaceID uint32) error {
	return m.inner.DropSpace(spaceID)
}
func (m *EncryptedSpaceManager) AllocateExtent(spaceID uint32, purpose basic.ExtentPurpose) (basic.Extent, error) {
	return m.inner.AllocateExtent(spaceID, purpose)
}
func (m *EncryptedSpaceManager) FreeExtent(spaceID, extentID uint32) error {
	return m.inner.FreeExtent(spaceID, extentID)
}
func (m *EncryptedSpaceManager) Begin() (basic.Tx, error) { return m.inner.Begin() }
func (m *EncryptedSpaceManager) CreateNewTablespace(name string) uint32 {
	id, err := m.CreateTableSpace(name)
	if err != nil {
		return 0
	}
	return id
}
func (m *EncryptedSpaceManager) CreateTableSpace(name string) (uint32, error) {
	id, err := m.inner.CreateTableSpace(name)
	if err != nil {
		return 0, err
	}
	if err := m.provisionSpace(id); err != nil {
		return 0, fmt.Errorf("encrypt new tablespace %d: %w", id, err)
	}
	return id, nil
}
func (m *EncryptedSpaceManager) GetTableSpace(spaceID uint32) (basic.FileTableSpace, error) {
	space, err := m.inner.GetTableSpace(spaceID)
	if err != nil {
		return nil, err
	}
	if m.encryption.GetKey(spaceID) == nil {
		return space, nil
	}
	m.provider.SetSpaceEncrypted(spaceID, true)
	return &encryptedFileTableSpace{inner: space, provider: m.provider}, nil
}
func (m *EncryptedSpaceManager) GetTableSpaceByName(name string) (basic.FileTableSpace, error) {
	space, err := m.inner.GetTableSpaceByName(name)
	if err != nil {
		return nil, err
	}
	spaceID := space.GetSpaceId()
	if m.encryption.GetKey(spaceID) == nil {
		return space, nil
	}
	m.provider.SetSpaceEncrypted(spaceID, true)
	return &encryptedFileTableSpace{inner: space, provider: m.provider}, nil
}
func (m *EncryptedSpaceManager) GetTableSpaceInfo(spaceID uint32) (*basic.TableSpaceInfo, error) {
	return m.inner.GetTableSpaceInfo(spaceID)
}
func (m *EncryptedSpaceManager) DropTableSpace(spaceID uint32) error {
	return m.inner.DropTableSpace(spaceID)
}
func (m *EncryptedSpaceManager) Close() error { return m.inner.Close() }

type encryptedSpace struct {
	inner    basic.Space
	provider *EncryptedStorageProvider
}

func (s *encryptedSpace) ID() uint32             { return s.inner.ID() }
func (s *encryptedSpace) Name() string           { return s.inner.Name() }
func (s *encryptedSpace) IsSystem() bool         { return s.inner.IsSystem() }
func (s *encryptedSpace) IsActive() bool         { return s.inner.IsActive() }
func (s *encryptedSpace) SetActive(v bool)       { s.inner.SetActive(v) }
func (s *encryptedSpace) GetPageCount() uint32   { return s.inner.GetPageCount() }
func (s *encryptedSpace) GetExtentCount() uint32 { return s.inner.GetExtentCount() }
func (s *encryptedSpace) GetUsedSpace() uint64   { return s.inner.GetUsedSpace() }
func (s *encryptedSpace) AllocateExtent(p basic.ExtentPurpose) (basic.Extent, error) {
	return s.inner.AllocateExtent(p)
}
func (s *encryptedSpace) FreeExtent(id uint32) error { return s.inner.FreeExtent(id) }
func (s *encryptedSpace) LoadPageByPageNumber(pageNo uint32) ([]byte, error) {
	return s.provider.ReadPage(s.ID(), pageNo)
}
func (s *encryptedSpace) FlushToDisk(pageNo uint32, content []byte) error {
	return s.provider.WritePage(s.ID(), pageNo, content)
}
func (s *encryptedSpace) AllocatePage() (uint32, error) {
	if allocator, ok := s.inner.(interface{ AllocatePage() (uint32, error) }); ok {
		return allocator.AllocatePage()
	}
	return 0, fmt.Errorf("page allocation is not supported for space %d", s.ID())
}
func (s *encryptedSpace) FreePage(pageNo uint32) error {
	if reclaimer, ok := s.inner.(interface{ FreePage(uint32) error }); ok {
		return reclaimer.FreePage(pageNo)
	}
	return fmt.Errorf("page reclamation is not supported for space %d", s.ID())
}
func (s *encryptedSpace) ShrinkToFit() error {
	if shrinker, ok := s.inner.(interface{ ShrinkToFit() error }); ok {
		return shrinker.ShrinkToFit()
	}
	return fmt.Errorf("tablespace shrink is not supported for space %d", s.ID())
}

type encryptedFileTableSpace struct {
	inner    basic.FileTableSpace
	provider *EncryptedStorageProvider
}

func (s *encryptedFileTableSpace) FlushToDisk(pageNo uint32, content []byte) {
	_ = s.provider.WritePage(s.GetSpaceId(), pageNo, content)
}
func (s *encryptedFileTableSpace) LoadPageByPageNumber(pageNo uint32) ([]byte, error) {
	return s.provider.ReadPage(s.GetSpaceId(), pageNo)
}
func (s *encryptedFileTableSpace) GetSpaceId() uint32 { return s.inner.GetSpaceId() }
