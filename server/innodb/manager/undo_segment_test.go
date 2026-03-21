package manager

import (
	"testing"
)

// TestUndoSegment_AllocateAddPurge LOG-007: Undo 段分配、添加日志、释放
func TestUndoSegment_AllocateAddPurge(t *testing.T) {
	seg := NewUndoSegment(1, 1, 100, 4096)
	if seg == nil {
		t.Fatal("NewUndoSegment returned nil")
	}

	err := seg.Allocate(1001)
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}

	entry := &UndoLogEntry{TrxID: 1001, LSN: 1, Type: LOG_TYPE_INSERT, TableID: 1, RecordID: 1, Data: []byte("x")}
	err = seg.AddUndoLog(entry)
	if err != nil {
		t.Fatalf("AddUndoLog: %v", err)
	}

	util := seg.GetUtilization()
	if util <= 0 || util > 1 {
		t.Errorf("GetUtilization should be in (0,1], got %f", util)
	}

	err = seg.Prepare()
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	seg.MarkForPurge()
	err = seg.Purge()
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}

	t.Log("Undo segment allocate/add/purge - passed")
}

// TestUndoSegmentManager_AllocateRelease LOG-007: Undo 段管理器分配与回收
func TestUndoSegmentManager_AllocateRelease(t *testing.T) {
	usm := NewUndoSegmentManager(1, 4096, 20)
	if usm == nil {
		t.Fatal("NewUndoSegmentManager returned nil")
	}

	seg1, err := usm.AllocateSegment(2001)
	if err != nil {
		t.Fatalf("AllocateSegment(2001): %v", err)
	}
	if seg1 == nil {
		t.Fatal("AllocateSegment returned nil segment")
	}

	seg2, err := usm.AllocateSegment(2002)
	if err != nil {
		t.Fatalf("AllocateSegment(2002): %v", err)
	}
	if seg2 == nil || seg2 == seg1 {
		t.Fatal("second segment should be different")
	}

	err = usm.ReleaseSegment(2001)
	if err != nil {
		t.Fatalf("ReleaseSegment(2001): %v", err)
	}

	err = usm.ReleaseSegment(2002)
	if err != nil {
		t.Fatalf("ReleaseSegment(2002): %v", err)
	}

	stats := usm.GetStats()
	if stats.TotalSegments < 2 {
		t.Errorf("expected at least 2 segments, got %d", stats.TotalSegments)
	}
	t.Logf("Undo segment manager: allocate/release ok, total segments=%d", stats.TotalSegments)
}
