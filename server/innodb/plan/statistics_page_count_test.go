package plan

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

type statsTestSpace struct {
	pages map[uint32][]byte
}

func (s *statsTestSpace) ID() uint32     { return 1 }
func (s *statsTestSpace) Name() string   { return "stats_test" }
func (s *statsTestSpace) IsSystem() bool { return false }
func (s *statsTestSpace) AllocateExtent(purpose basic.ExtentPurpose) (basic.Extent, error) {
	return nil, fmt.Errorf("not implemented")
}
func (s *statsTestSpace) FreeExtent(extentID uint32) error { return fmt.Errorf("not implemented") }
func (s *statsTestSpace) GetPageCount() uint32             { return uint32(len(s.pages)) }
func (s *statsTestSpace) GetExtentCount() uint32           { return 0 }
func (s *statsTestSpace) GetUsedSpace() uint64             { return uint64(len(s.pages)) * 16384 }
func (s *statsTestSpace) IsActive() bool                   { return true }
func (s *statsTestSpace) SetActive(active bool)            {}
func (s *statsTestSpace) LoadPageByPageNumber(no uint32) ([]byte, error) {
	page, ok := s.pages[no]
	if !ok {
		return nil, fmt.Errorf("page %d not found", no)
	}
	return append([]byte(nil), page...), nil
}
func (s *statsTestSpace) FlushToDisk(no uint32, content []byte) error {
	s.pages[no] = append([]byte(nil), content...)
	return nil
}

func TestCountRowsInPageReadsInnoDBIndexHeader(t *testing.T) {
	page := make([]byte, 16384)
	binary.BigEndian.PutUint16(page[24:26], uint16(common.FIL_PAGE_INDEX))
	binary.BigEndian.PutUint16(page[38+16:38+18], 37)

	esc := NewEnhancedStatisticsCollector(&StatisticsConfig{SampleRate: 1.0, EnableAutoUpdate: false}, nil, nil)
	rows := esc.countRowsInPage(&statsTestSpace{pages: map[uint32][]byte{0: page}}, 0)
	if rows != 37 {
		t.Fatalf("countRowsInPage = %d, want PAGE_N_RECS value 37", rows)
	}
}

func TestExactRowCountSumsParsedPageRecordCounts(t *testing.T) {
	page0 := make([]byte, 16384)
	page1 := make([]byte, 16384)
	binary.BigEndian.PutUint16(page0[24:26], uint16(common.FIL_PAGE_INDEX))
	binary.BigEndian.PutUint16(page1[24:26], uint16(common.FIL_PAGE_INDEX))
	binary.BigEndian.PutUint16(page0[38+16:38+18], 11)
	binary.BigEndian.PutUint16(page1[38+16:38+18], 13)

	esc := NewEnhancedStatisticsCollector(&StatisticsConfig{SampleRate: 1.0, EnableAutoUpdate: false}, nil, nil)
	rows := esc.getExactRowCount(&statsTestSpace{pages: map[uint32][]byte{0: page0, 1: page1}})
	if rows != 24 {
		t.Fatalf("getExactRowCount = %d, want 24", rows)
	}
}

func TestSampledRowCountDoesNotDefaultEmptyIndexPagesToHundredRows(t *testing.T) {
	page := make([]byte, 16384)
	binary.BigEndian.PutUint16(page[24:26], uint16(common.FIL_PAGE_INDEX))
	binary.BigEndian.PutUint16(page[38+16:38+18], 0)

	esc := NewEnhancedStatisticsCollector(&StatisticsConfig{SampleRate: 0.5, EnableAutoUpdate: false}, nil, nil)
	rows := esc.getSampledRowCount(&statsTestSpace{pages: map[uint32][]byte{0: page, 1: page}}, 2)
	if rows != 0 {
		t.Fatalf("getSampledRowCount = %d, want 0 for sampled empty index pages", rows)
	}
}

func TestSampleColumnDataDoesNotGenerateRowsForEmptyPages(t *testing.T) {
	page := make([]byte, 16384)
	binary.BigEndian.PutUint16(page[24:26], uint16(common.FIL_PAGE_INDEX))
	binary.BigEndian.PutUint16(page[38+16:38+18], 0)

	esc := NewEnhancedStatisticsCollector(&StatisticsConfig{SampleRate: 1.0, EnableAutoUpdate: false}, nil, nil)
	values := esc.extractColumnValuesFromPage(
		&statsTestSpace{pages: map[uint32][]byte{0: page}},
		0,
		&metadata.Column{Name: "id", DataType: metadata.TypeInt},
	)
	if len(values) != 0 {
		t.Fatalf("extractColumnValuesFromPage returned %d synthetic values for an empty page", len(values))
	}
}
