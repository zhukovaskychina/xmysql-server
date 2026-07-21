package manager

import (
	"context"
	"fmt"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

type validatingMockBTree struct {
	leafPages []uint32
	leafErr   error
}

func (m *validatingMockBTree) Init(ctx context.Context, spaceId uint32, rootPage uint32) error {
	return nil
}

func (m *validatingMockBTree) GetAllLeafPages(ctx context.Context) ([]uint32, error) {
	if m.leafErr != nil {
		return nil, m.leafErr
	}
	return append([]uint32(nil), m.leafPages...), nil
}

func (m *validatingMockBTree) Search(ctx context.Context, key interface{}) (uint32, int, error) {
	return 0, 0, fmt.Errorf("not found")
}

func (m *validatingMockBTree) Insert(ctx context.Context, key interface{}, value []byte) error {
	return nil
}

func (m *validatingMockBTree) Delete(ctx context.Context, key interface{}) error {
	return nil
}

func (m *validatingMockBTree) RangeSearch(ctx context.Context, startKey, endKey interface{}) ([]basic.Row, error) {
	return nil, nil
}

func (m *validatingMockBTree) GetFirstLeafPage(ctx context.Context) (uint32, error) {
	if len(m.leafPages) == 0 {
		return 0, fmt.Errorf("no leaf pages")
	}
	return m.leafPages[0], nil
}

func TestValidateIndexRejectsInvalidMetadataAndLeafPages(t *testing.T) {
	im := &IndexManager{
		indexes:      map[uint64]*Index{},
		btreeManager: &validatingMockBTree{leafPages: []uint32{9}},
		stats:        &IndexManagerStats{},
		config:       &IndexManagerConfig{},
	}

	im.indexes[1] = &Index{
		IndexID:    1,
		TableID:    10,
		SpaceID:    1,
		Name:       "idx_bad_root",
		Type:       INDEX_TYPE_BTREE,
		State:      IndexStateActive,
		RootPageNo: 0,
		PageCount:  1,
		LeafPages:  1,
	}
	if err := im.ValidateIndex(1); err == nil {
		t.Fatalf("ValidateIndex accepted an active index with root page 0")
	}

	im.indexes[1].RootPageNo = 9
	im.btreeManager = &validatingMockBTree{leafPages: nil}
	if err := im.ValidateIndex(1); err == nil {
		t.Fatalf("ValidateIndex accepted an active index with no leaf pages")
	}
}

func TestCompactIndexRecomputesFragmentedPageCounters(t *testing.T) {
	im := &IndexManager{
		indexes:      map[uint64]*Index{},
		btreeManager: &validatingMockBTree{leafPages: []uint32{5, 6, 7}},
		stats:        &IndexManagerStats{},
		config:       &IndexManagerConfig{},
	}
	im.indexes[1] = &Index{
		IndexID:      1,
		TableID:      10,
		SpaceID:      1,
		Name:         "idx_fragmented",
		Type:         INDEX_TYPE_BTREE,
		State:        IndexStateActive,
		RootPageNo:   5,
		Height:       2,
		PageCount:    20,
		LeafPages:    1,
		NonLeafPages: 4,
	}

	if err := im.CompactIndex(1); err != nil {
		t.Fatalf("CompactIndex failed: %v", err)
	}

	idx := im.indexes[1]
	if idx.LeafPages != 3 {
		t.Fatalf("LeafPages = %d, want 3", idx.LeafPages)
	}
	if idx.PageCount != 4 {
		t.Fatalf("PageCount = %d, want 4", idx.PageCount)
	}
	if idx.NonLeafPages != 1 {
		t.Fatalf("NonLeafPages = %d, want 1", idx.NonLeafPages)
	}
}
