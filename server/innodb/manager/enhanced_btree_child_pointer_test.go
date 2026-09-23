package manager

import (
	"encoding/binary"
	"testing"
)

func TestEnhancedBTreeReadsFirstChildPointerFromInternalRecord(t *testing.T) {
	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, 73)
	index := &EnhancedBTreeIndex{}
	page := &BTreePage{PageNo: 12, Records: []IndexRecord{{Value: value}}}
	if got := index.getFirstChildPageNo(page); got != 73 {
		t.Fatalf("getFirstChildPageNo() = %d, want 73", got)
	}
}
