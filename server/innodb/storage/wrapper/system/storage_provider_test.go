package system

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

type systemPageStorage struct {
	pages     map[string][]byte
	syncCalls int
}

func newSystemPageStorage() *systemPageStorage {
	return &systemPageStorage{pages: make(map[string][]byte)}
}

func (s *systemPageStorage) key(spaceID, pageNo uint32) string {
	return fmt.Sprintf("%d/%d", spaceID, pageNo)
}

func (s *systemPageStorage) ReadPage(spaceID, pageNo uint32) ([]byte, error) {
	data, ok := s.pages[s.key(spaceID, pageNo)]
	if !ok {
		return nil, fmt.Errorf("page %d/%d not found", spaceID, pageNo)
	}
	return append([]byte(nil), data...), nil
}

func (s *systemPageStorage) WritePage(spaceID, pageNo uint32, data []byte) error {
	s.pages[s.key(spaceID, pageNo)] = append([]byte(nil), data...)
	return nil
}

func (s *systemPageStorage) AllocatePage(uint32) (uint32, error) { return 0, nil }
func (s *systemPageStorage) FreePage(uint32, uint32) error       { return nil }
func (s *systemPageStorage) CreateSpace(string, uint32) (uint32, error) {
	return 0, nil
}
func (s *systemPageStorage) OpenSpace(uint32) error   { return nil }
func (s *systemPageStorage) CloseSpace(uint32) error  { return nil }
func (s *systemPageStorage) DeleteSpace(uint32) error { return nil }
func (s *systemPageStorage) GetSpaceInfo(uint32) (*basic.SpaceInfo, error) {
	return &basic.SpaceInfo{}, nil
}
func (s *systemPageStorage) ListSpaces() ([]basic.SpaceInfo, error) { return nil, nil }
func (s *systemPageStorage) BeginTransaction() (uint64, error)      { return 0, nil }
func (s *systemPageStorage) CommitTransaction(uint64) error         { return nil }
func (s *systemPageStorage) RollbackTransaction(uint64) error       { return nil }
func (s *systemPageStorage) Sync(uint32) error {
	s.syncCalls++
	return nil
}
func (s *systemPageStorage) Close() error { return nil }

func TestSystemPagesUseStorageProvider(t *testing.T) {
	storage := newSystemPageStorage()

	tests := []struct {
		name  string
		page  uint32
		write func() error
		read  func() error
	}{
		{
			name: "fsp",
			page: 10,
			write: func() error {
				page := NewFSPPageWithStorage(1, 10, storage)
				page.SetSize(42)
				return page.Flush()
			},
			read: func() error {
				page := NewFSPPageWithStorage(1, 10, storage)
				return page.Read()
			},
		},
		{
			name: "xdes",
			page: 11,
			write: func() error {
				page := NewXDESPageWithStorage(1, 11, storage)
				return page.Flush()
			},
			read: func() error {
				page := NewXDESPageWithStorage(1, 11, storage)
				return page.Read()
			},
		},
		{
			name: "ibuf",
			page: 12,
			write: func() error {
				page := NewIBufPageWithStorage(1, 12, storage)
				return page.Flush()
			},
			read: func() error {
				page := NewIBufPageWithStorage(1, 12, storage)
				return page.Read()
			},
		},
		{
			name: "dict",
			page: 13,
			write: func() error {
				page := NewDictPageWithStorage(1, 13, storage)
				return page.Flush()
			},
			read: func() error {
				page := NewDictPageWithStorage(1, 13, storage)
				return page.Read()
			},
		},
		{
			name: "trx",
			page: 14,
			write: func() error {
				page := NewTrxPageWithStorage(1, 14, storage)
				return page.Flush()
			},
			read: func() error {
				page := NewTrxPageWithStorage(1, 14, storage)
				return page.Read()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, tc.write())
			require.Len(t, storage.pages[storage.key(1, tc.page)], common.PageSize)
			require.Greater(t, storage.syncCalls, 0)
			require.NoError(t, tc.read())
		})
	}
}
