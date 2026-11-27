package types

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

type basePageWrapperStorage struct {
	pages     map[string][]byte
	syncCalls int
}

func newBasePageWrapperStorage() *basePageWrapperStorage {
	return &basePageWrapperStorage{pages: make(map[string][]byte)}
}

func (s *basePageWrapperStorage) key(spaceID, pageNo uint32) string {
	return fmt.Sprintf("%d/%d", spaceID, pageNo)
}

func (s *basePageWrapperStorage) ReadPage(spaceID, pageNo uint32) ([]byte, error) {
	data, ok := s.pages[s.key(spaceID, pageNo)]
	if !ok {
		return nil, fmt.Errorf("page %d/%d not found", spaceID, pageNo)
	}
	return append([]byte(nil), data...), nil
}

func (s *basePageWrapperStorage) WritePage(spaceID, pageNo uint32, data []byte) error {
	s.pages[s.key(spaceID, pageNo)] = append([]byte(nil), data...)
	return nil
}

func (s *basePageWrapperStorage) AllocatePage(uint32) (uint32, error) { return 0, nil }
func (s *basePageWrapperStorage) FreePage(uint32, uint32) error       { return nil }
func (s *basePageWrapperStorage) CreateSpace(string, uint32) (uint32, error) {
	return 0, nil
}
func (s *basePageWrapperStorage) OpenSpace(uint32) error   { return nil }
func (s *basePageWrapperStorage) CloseSpace(uint32) error  { return nil }
func (s *basePageWrapperStorage) DeleteSpace(uint32) error { return nil }
func (s *basePageWrapperStorage) GetSpaceInfo(uint32) (*basic.SpaceInfo, error) {
	return &basic.SpaceInfo{}, nil
}
func (s *basePageWrapperStorage) ListSpaces() ([]basic.SpaceInfo, error) { return nil, nil }
func (s *basePageWrapperStorage) BeginTransaction() (uint64, error)      { return 0, nil }
func (s *basePageWrapperStorage) CommitTransaction(uint64) error         { return nil }
func (s *basePageWrapperStorage) RollbackTransaction(uint64) error       { return nil }
func (s *basePageWrapperStorage) Sync(uint32) error                      { s.syncCalls++; return nil }
func (s *basePageWrapperStorage) Close() error                           { return nil }

func TestBasePageWrapperUsesConfiguredStorageProvider(t *testing.T) {
	storage := newBasePageWrapperStorage()
	writer := NewBasePageWrapperWithStorage(17, 9, 23, common.FIL_PAGE_INDEX, storage)
	writer.Content[100] = 0x7a
	writer.MarkDirty()

	require.NoError(t, writer.Write())
	require.False(t, writer.IsDirty())

	reader := NewBasePageWrapperWithStorage(17, 9, 23, common.FIL_PAGE_INDEX, storage)
	require.NoError(t, reader.Read())
	require.Equal(t, byte(0x7a), reader.Content[100])
	require.EqualValues(t, 1, reader.GetStats().ReadCount)
	require.Equal(t, basic.PageStateLoaded, reader.GetState())
}

func TestBasePageWrapperProviderRejectsShortPage(t *testing.T) {
	storage := newBasePageWrapperStorage()
	storage.pages[storage.key(3, 4)] = make([]byte, common.PageSize-1)
	reader := NewBasePageWrapperWithStorage(4, 3, 4, common.FIL_PAGE_INDEX, storage)

	require.ErrorIs(t, reader.Read(), ErrPageStorageInvalidSize)
}

func TestBasePageWrapperProviderFlushesDirtyPage(t *testing.T) {
	storage := newBasePageWrapperStorage()
	page := NewBasePageWrapperWithStorage(1, 2, 3, common.FIL_PAGE_INDEX, storage)
	page.Content[200] = 0x55
	page.MarkDirty()

	require.NoError(t, page.Flush())
	require.Equal(t, 1, storage.syncCalls)
	require.False(t, page.IsDirty())
	require.Equal(t, byte(0x55), storage.pages[storage.key(2, 3)][200])
	require.Equal(t, basic.PageStateFlushed, page.GetState())
}

func TestDeprecatedBasePageUsesStorageProvider(t *testing.T) {
	storage := newBasePageWrapperStorage()
	writer := NewBasePageWithStorage(9, 2, common.FIL_PAGE_INDEX, storage)
	writer.SetLSN(77)
	require.NoError(t, writer.Write())

	reader := NewBasePageWithStorage(9, 2, common.FIL_PAGE_INDEX, storage)
	require.NoError(t, reader.Read())
	require.EqualValues(t, 77, reader.GetLSN())
	require.Equal(t, basic.PageStateLoaded, reader.GetState())
}
