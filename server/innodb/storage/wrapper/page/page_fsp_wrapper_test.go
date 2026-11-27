package page

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFspPageWrapper_AllocatePage(t *testing.T) {
	storage := &fspAllocatingStorage{pageWrapperStorage: newPageWrapperStorage(), nextPage: 100}
	wrapper := NewFSPPageWrapperWithStorage(1, 9, nil, storage)

	pages, err := wrapper.AllocatePages(3)
	require.NoError(t, err)
	require.Equal(t, []uint32{100, 101, 102}, pages)
}

type fspAllocatingStorage struct {
	*pageWrapperStorage
	nextPage uint32
}

func (s *fspAllocatingStorage) AllocatePage(uint32) (uint32, error) {
	page := s.nextPage
	s.nextPage++
	return page, nil
}
