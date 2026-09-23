package page

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
)

func requireCompletes(t *testing.T, operation func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		operation()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("page mutation did not complete; likely self-deadlocked while marking dirty")
	}
}

type pageWrapperStorage struct {
	pages map[string][]byte
}

func newPageWrapperStorage() *pageWrapperStorage {
	return &pageWrapperStorage{pages: make(map[string][]byte)}
}

func (s *pageWrapperStorage) key(spaceID, pageNo uint32) string {
	return fmt.Sprintf("%d/%d", spaceID, pageNo)
}

func (s *pageWrapperStorage) ReadPage(spaceID, pageNo uint32) ([]byte, error) {
	data, ok := s.pages[s.key(spaceID, pageNo)]
	if !ok {
		return nil, fmt.Errorf("page %d/%d not found", spaceID, pageNo)
	}
	return append([]byte(nil), data...), nil
}

func (s *pageWrapperStorage) WritePage(spaceID, pageNo uint32, data []byte) error {
	s.pages[s.key(spaceID, pageNo)] = append([]byte(nil), data...)
	return nil
}

func (s *pageWrapperStorage) AllocatePage(uint32) (uint32, error) { return 0, nil }
func (s *pageWrapperStorage) FreePage(uint32, uint32) error       { return nil }
func (s *pageWrapperStorage) CreateSpace(string, uint32) (uint32, error) {
	return 0, nil
}
func (s *pageWrapperStorage) OpenSpace(uint32) error   { return nil }
func (s *pageWrapperStorage) CloseSpace(uint32) error  { return nil }
func (s *pageWrapperStorage) DeleteSpace(uint32) error { return nil }
func (s *pageWrapperStorage) GetSpaceInfo(uint32) (*basic.SpaceInfo, error) {
	return &basic.SpaceInfo{}, nil
}
func (s *pageWrapperStorage) ListSpaces() ([]basic.SpaceInfo, error) { return nil, nil }
func (s *pageWrapperStorage) BeginTransaction() (uint64, error)      { return 0, nil }
func (s *pageWrapperStorage) CommitTransaction(uint64) error         { return nil }
func (s *pageWrapperStorage) RollbackTransaction(uint64) error       { return nil }
func (s *pageWrapperStorage) Sync(uint32) error                      { return nil }
func (s *pageWrapperStorage) Close() error                           { return nil }

func TestBasePageWrapperUsesConfiguredStorageProvider(t *testing.T) {
	storage := newPageWrapperStorage()
	writer := NewBasePageWrapperWithStorage(17, 9, common.FIL_PAGE_INDEX, storage)
	writer.content[100] = 0x7a
	writer.MarkDirty()

	require.NoError(t, writer.Write())
	require.False(t, writer.IsDirty())

	reader := NewBasePageWrapperWithStorage(17, 9, common.FIL_PAGE_INDEX, storage)
	require.NoError(t, reader.Read())
	require.Equal(t, byte(0x7a), reader.content[100])
	require.EqualValues(t, 1, reader.GetStats().ReadCount)
	require.Equal(t, basic.PageStateLoaded, reader.GetState())
}

func TestBasicBasePageWrapperUsesStorageProvider(t *testing.T) {
	storage := newPageWrapperStorage()
	writer := basic.NewBasePageWrapperWithStorage(18, 9, 19, uint16(common.FIL_PAGE_INDEX), storage)
	writer.Content[100] = 0x7b
	writer.MarkDirty()
	require.NoError(t, writer.Write())

	reader := basic.NewBasePageWrapperWithStorage(18, 9, 19, uint16(common.FIL_PAGE_INDEX), storage)
	require.NoError(t, reader.Read())
	require.Equal(t, byte(0x7b), reader.Content[100])
}

func TestBasePageWrapperProviderRejectsShortPage(t *testing.T) {
	storage := newPageWrapperStorage()
	storage.pages[storage.key(3, 4)] = make([]byte, common.PageSize-1)
	reader := NewBasePageWrapperWithStorage(4, 3, common.FIL_PAGE_INDEX, storage)

	require.ErrorIs(t, reader.Read(), ErrInvalidPageSize)
}

func TestDeprecatedBasePageUsesStorageProvider(t *testing.T) {
	storage := newPageWrapperStorage()
	writer := NewBasePageWithStorage(17, 9, common.FIL_PAGE_INDEX, storage)
	content := writer.GetContent()
	content[100] = 0x6b
	require.NoError(t, writer.SetContent(content))
	require.NoError(t, writer.Write())

	reader := NewBasePageWithStorage(17, 9, common.FIL_PAGE_INDEX, storage)
	require.NoError(t, reader.Read())
	require.Equal(t, byte(0x6b), reader.GetContent()[100])
}

func TestInodePageWrapperUsesStorageProvider(t *testing.T) {
	storage := newPageWrapperStorage()
	writer := NewInodeWrapperWithStorage(30, 9, storage)
	writer.SetNext(42)
	require.NoError(t, writer.Write())

	reader := NewInodeWrapperWithStorage(30, 9, storage)
	require.NoError(t, reader.Read())
	require.EqualValues(t, 42, reader.GetNext())
}

func TestRemainingDirectLegacyPagesUseStorageProvider(t *testing.T) {
	storage := newPageWrapperStorage()

	t.Run("blob", func(t *testing.T) {
		writer := NewBlobPageWithStorage(32, 9, storage)
		require.NoError(t, writer.SetData([]byte("legacy blob"), 7, 3, 1))
		require.NoError(t, writer.Write())

		reader := NewBlobPageWithStorage(32, 9, storage)
		require.NoError(t, reader.Read())
		require.Equal(t, []byte("legacy blob"), reader.GetData())
	})

	t.Run("ibuf", func(t *testing.T) {
		writer := NewIBufPageWrapperWithStorage(33, 9, storage)
		writer.content[128] = 0x4d
		writer.MarkDirty()
		require.NoError(t, writer.Write())

		reader := NewIBufPageWrapperWithStorage(33, 9, storage)
		require.NoError(t, reader.Read())
		require.Equal(t, byte(0x4d), reader.content[128])
	})
}

func TestDirectInodePageWrapperUsesStorageProvider(t *testing.T) {
	storage := newPageWrapperStorage()
	writer := NewInodePageWrapperWithStorage(34, 9, storage)
	writer.GetInodePage().INodePageList.PreNodePageNumber[0] = 0x5a
	require.NoError(t, writer.Write())

	reader := NewInodePageWrapperWithStorage(34, 9, storage)
	require.NoError(t, reader.Read())
	require.Equal(t, byte(0x5a), reader.GetInodePage().INodePageList.PreNodePageNumber[0])
}

func TestDirectAllocatedPageUsesStorageProvider(t *testing.T) {
	storage := newPageWrapperStorage()
	writer := NewAllocatedPageWithStorage(9, 35, storage).(*Allocated)
	writer.body[0] = 0x6c
	writer.MarkDirty()
	require.NoError(t, writer.Write())

	reader := NewAllocatedPageWithStorage(9, 35, storage).(*Allocated)
	require.NoError(t, reader.Read())
	require.Equal(t, byte(0x6c), reader.body[0])
}

func TestDirectIBufPageUsesStorageProvider(t *testing.T) {
	storage := newPageWrapperStorage()
	writer := NewIBufWithStorage(9, 36, storage)
	writer.iBufPage.ChangeBufferBitMap[0] = 0xa5
	require.NoError(t, writer.Write())

	reader := NewIBufWithStorage(9, 36, storage)
	require.NoError(t, reader.Read())
	require.Equal(t, byte(0xa5), reader.iBufPage.ChangeBufferBitMap[0])
}

func TestLegacyIndexPageUsesStorageProvider(t *testing.T) {
	storage := newPageWrapperStorage()
	writer := NewIndexPageWithStorage(31, 9, storage)
	writer.SetIndexID(0xabc)
	require.NoError(t, writer.Write())

	reader := NewIndexPageWithStorage(31, 9, storage)
	require.NoError(t, reader.Read())
	require.EqualValues(t, 0xabc, reader.GetIndexID())
}

func TestSpecializedPageWrappersUseStorageProvider(t *testing.T) {
	storage := newPageWrapperStorage()

	t.Run("fsp", func(t *testing.T) {
		writer := NewFSPPageWrapperWithStorage(10, 1, nil, storage)
		require.NoError(t, writer.Write())

		reader := NewFSPPageWrapperWithStorage(10, 1, nil, storage)
		require.NoError(t, reader.Read())
	})

	t.Run("xdes", func(t *testing.T) {
		writer := NewXDESPageWrapperWithStorage(11, 1, nil, storage)
		require.NoError(t, writer.Write())

		reader := NewXDESPageWrapperWithStorage(11, 1, nil, storage)
		require.NoError(t, reader.Read())
	})

	t.Run("ibuf-bitmap", func(t *testing.T) {
		writer := NewIBufBitmapPageWrapperWithStorage(12, 1, nil, storage)
		require.NoError(t, writer.Write())

		reader := NewIBufBitmapPageWrapperWithStorage(12, 1, nil, storage)
		require.NoError(t, reader.Read())
	})
}

func TestRemainingLegacyPageWrappersUseStorageProvider(t *testing.T) {
	storage := newPageWrapperStorage()

	t.Run("blob", func(t *testing.T) {
		writer := NewBlobPageWrapperWithStorage(20, 1, 7, storage)
		require.NoError(t, writer.Write())
		reader := NewBlobPageWrapperWithStorage(20, 1, 7, storage)
		require.NoError(t, reader.Read())
	})

	t.Run("compressed", func(t *testing.T) {
		writer := NewCompressedPageWrapperWithStorage(21, 1, storage)
		require.NoError(t, writer.Write())
		reader := NewCompressedPageWrapperWithStorage(21, 1, storage)
		require.NoError(t, reader.Read())
	})

	t.Run("ibuf-free-list", func(t *testing.T) {
		writer := NewIBufFreeListPageWrapperWithStorage(22, 1, storage)
		require.NoError(t, writer.Write())
		reader := NewIBufFreeListPageWrapperWithStorage(22, 1, storage)
		require.NoError(t, reader.Read())
	})

	t.Run("undo", func(t *testing.T) {
		writer := NewUndoLogPageWrapperWithStorage(23, 1, 23, nil, storage)
		require.NoError(t, writer.Write())
		reader := NewUndoLogPageWrapperWithStorage(23, 1, 23, nil, storage)
		require.NoError(t, reader.Read())
	})

	t.Run("trx-sys", func(t *testing.T) {
		writer := NewTrxSysPageWrapperWithStorage(24, 1, nil, storage)
		require.NoError(t, writer.Write())
		reader := NewTrxSysPageWrapperWithStorage(24, 1, nil, storage)
		require.NoError(t, reader.Read())
	})

	t.Run("encrypted", func(t *testing.T) {
		writer := NewEncryptedPageWrapperWithStorage(25, 1, 25, nil, storage)
		require.NoError(t, writer.Write())
		reader := NewEncryptedPageWrapperWithStorage(25, 1, 25, nil, storage)
		require.NoError(t, reader.Read())
	})
}

func TestPageFactoryCreatesProviderBackedPage(t *testing.T) {
	storage := newPageWrapperStorage()
	factory := NewPageFactory()

	pageTypes := []common.PageType{
		common.FIL_PAGE_INDEX,
		common.FIL_PAGE_FSP_HDR,
		common.FIL_PAGE_INODE,
		common.FIL_PAGE_IBUF_FREE_LIST,
		common.FIL_PAGE_TYPE_SYS,
		common.FIL_PAGE_TYPE_XDES,
		common.FIL_PAGE_UNDO_LOG,
		common.FIL_PAGE_TYPE_ALLOCATED,
		common.FIL_PAGE_TYPE_BLOB,
		common.FIL_PAGE_TYPE_COMPRESSED,
		common.FIL_PAGE_TYPE_ENCRYPTED,
		common.FIL_PAGE_IBUF_BITMAP,
		common.FIL_PAGE_TYPE_TRX_SYS,
	}
	for index, pageType := range pageTypes {
		t.Run(fmt.Sprintf("%d", pageType), func(t *testing.T) {
			pageNo := uint32(31 + index)
			writer := factory.CreatePageWithStorage(pageType, pageNo, 9, nil, storage)
			require.NotNil(t, writer)
			require.NoError(t, writer.Write())
			require.Contains(t, storage.pages, storage.key(9, pageNo))

			reader := factory.CreatePageWithStorage(pageType, pageNo, 9, nil, storage)
			require.NotNil(t, reader)
			require.NoError(t, reader.Read())
		})
	}
}

func TestPageFactorySpecialPagesAndParseUseProvider(t *testing.T) {
	storage := newPageWrapperStorage()
	factory := NewPageFactory()

	blob := factory.CreateBlobPageWithStorage(40, 9, 7, storage)
	require.NoError(t, blob.Write())
	require.Contains(t, storage.pages, storage.key(9, 40))

	rollback := factory.CreateRollbackPageWithStorage(41, 9, storage)
	require.NoError(t, rollback.Write())
	require.Contains(t, storage.pages, storage.key(9, 41))

	parsed, err := factory.ParsePageWithStorage(storage.pages[storage.key(9, 40)], storage)
	require.NoError(t, err)
	require.NoError(t, parsed.Read())
}

func TestLegacyPageMutationsDoNotSelfDeadlock(t *testing.T) {
	t.Run("blob", func(t *testing.T) {
		page := NewBlobPageWrapper(50, 9, 7)
		requireCompletes(t, func() {
			require.NoError(t, page.SetBlobData([]byte("blob"), 4, 0, 0))
		})
	})

	t.Run("compressed", func(t *testing.T) {
		page := NewCompressedPageWrapper(51, 9)
		requireCompletes(t, func() {
			require.NoError(t, page.SetData([]byte("compressed")))
		})
	})

	t.Run("ibuf-free-list", func(t *testing.T) {
		page := NewIBufFreeListPageWrapper(52, 9)
		requireCompletes(t, func() {
			require.NoError(t, page.AddFreePage(123))
		})
	})

	t.Run("rollback", func(t *testing.T) {
		page := NewRollbackPageWrapper(53, 9)
		requireCompletes(t, func() {
			page.SetTrxRsegMaxSize([]byte{1, 2, 3, 4})
		})
	})
}

func TestLegacySpecializedWrappersRoundTripPayload(t *testing.T) {
	t.Run("blob", func(t *testing.T) {
		storage := newPageWrapperStorage()
		writer := NewBlobPageWrapperWithStorage(60, 9, 7, storage)
		require.NoError(t, writer.SetBlobData([]byte("blob-payload"), 12, 0, 66))
		require.NoError(t, writer.Write())

		reader := NewBlobPageWrapperWithStorage(60, 9, 7, storage)
		require.NoError(t, reader.Read())
		require.Equal(t, []byte("blob-payload"), reader.GetBlobData())
		require.Equal(t, uint32(66), reader.GetNextPageNo())
	})

	t.Run("compressed", func(t *testing.T) {
		storage := newPageWrapperStorage()
		writer := NewCompressedPageWrapperWithStorage(61, 9, storage)
		require.NoError(t, writer.SetData([]byte("compressed-payload")))
		require.NoError(t, writer.Write())

		reader := NewCompressedPageWrapperWithStorage(61, 9, storage)
		require.NoError(t, reader.Read())
		payload, err := reader.GetOriginalData()
		require.NoError(t, err)
		require.Equal(t, []byte("compressed-payload"), payload)
	})

	t.Run("ibuf-free-list", func(t *testing.T) {
		storage := newPageWrapperStorage()
		writer := NewIBufFreeListPageWrapperWithStorage(62, 9, storage)
		require.NoError(t, writer.AddFreePage(123))
		require.NoError(t, writer.Write())

		reader := NewIBufFreeListPageWrapperWithStorage(62, 9, storage)
		require.NoError(t, reader.Read())
		require.Equal(t, []uint32{123}, reader.GetFreePages())
	})

	t.Run("rollback", func(t *testing.T) {
		storage := newPageWrapperStorage()
		writer := NewRollbackPageWrapperWithStorage(63, 9, storage)
		writer.SetTrxRsegMaxSize([]byte{1, 2, 3, 4})
		require.NoError(t, writer.Write())

		reader := NewRollbackPageWrapperWithStorage(63, 9, storage)
		require.NoError(t, reader.Read())
		require.Equal(t, []byte{1, 2, 3, 4}, reader.GetTrxRsegMaxSize())
	})
}

func TestSpecializedPageWrappersRequireStorageForWrite(t *testing.T) {
	tests := map[string]func() error{
		"data-dictionary": func() error {
			return NewDataDictionaryPageWrapperWithStorage(69, 9, nil, nil).Write()
		},
		"fsp": func() error {
			return NewFSPPageWrapperWithStorage(70, 9, nil, nil).Write()
		},
		"ibuf-bitmap": func() error {
			return NewIBufBitmapPageWrapperWithStorage(71, 9, nil, nil).Write()
		},
		"encrypted": func() error {
			return NewEncryptedPageWrapperWithStorage(72, 9, 72, nil, nil).Write()
		},
		"trx-sys": func() error {
			return NewTrxSysPageWrapperWithStorage(73, 9, nil, nil).Write()
		},
		"xdes": func() error {
			return NewXDESPageWrapperWithStorage(74, 9, nil, nil).Write()
		},
		"undo": func() error {
			return NewUndoLogPageWrapperWithStorage(75, 9, 75, nil, nil).Write()
		},
	}
	for name, write := range tests {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, write(), ErrPageStorageUnavailable)
		})
	}
}

func TestDataDictionaryWrapperFlushesTheUpdatedBufferPage(t *testing.T) {
	storage := newPageWrapperStorage()
	bp := buffer_pool.NewBufferPool(&buffer_pool.BufferPoolConfig{
		TotalPages:       4,
		PageSize:         common.PageSize,
		BufferPoolSize:   uint64(common.PageSize * 4),
		YoungListPercent: 0.8,
		OldListPercent:   0.2,
		OldBlocksTime:    1000,
		PrefetchSize:     1,
		MaxQueueSize:     4,
		PrefetchWorkers:  1,
		StorageProvider:  storage,
	})

	cached := buffer_pool.NewBufferPage(1, 76)
	cached.SetContent(make([]byte, common.PageSize))
	require.NoError(t, bp.PutPage(cached))

	wrapper := NewDataDictionaryPageWrapperWithStorage(76, 1, bp, storage)
	require.NoError(t, wrapper.AddTableDef(&TableDef{ID: 1, Name: "buffered_table"}))
	require.NoError(t, wrapper.Write())

	persisted, err := storage.ReadPage(1, 76)
	require.NoError(t, err)
	require.Len(t, persisted, common.PageSize)
	require.Contains(t, string(persisted), "buffered_table")
}
