package manager

import "testing"

func TestUndoSpaceReclaimerPurgesPreparedCachedSegmentsForReuse(t *testing.T) {
	segmentManager := NewUndoSegmentManager(1, 1024, 16)
	purger := NewUndoPurger(segmentManager)
	purger.SetRetentionTime(0)

	segment, err := segmentManager.AllocateSegment(1001)
	if err != nil {
		t.Fatalf("AllocateSegment failed: %v", err)
	}
	if err := segment.AddUndoLog(&UndoLogEntry{LSN: 1, TrxID: 1001, TableID: 7, Data: []byte("before-image")}); err != nil {
		t.Fatalf("AddUndoLog failed: %v", err)
	}
	if err := segmentManager.ReleaseSegment(1001); err != nil {
		t.Fatalf("ReleaseSegment failed: %v", err)
	}
	if segment.state != SEGMENT_PREPARED {
		t.Fatalf("released segment state = %v, want PREPARED", segment.state)
	}

	reclaimer := NewUndoSpaceReclaimer(purger, segmentManager)
	reclaimer.reclaimSpace()

	if segment.state != SEGMENT_CACHED {
		t.Fatalf("reclaimed segment state = %v, want CACHED", segment.state)
	}
	if segment.usedSize != 0 || len(segment.undoLogs) != 0 {
		t.Fatalf("reclaimed segment still has undo data: used=%d logs=%d", segment.usedSize, len(segment.undoLogs))
	}

	reused, err := segmentManager.AllocateSegment(1002)
	if err != nil {
		t.Fatalf("AllocateSegment after reclaim failed: %v", err)
	}
	if reused.segmentID != segment.segmentID {
		t.Fatalf("allocated segment id = %d, want reclaimed cached segment %d", reused.segmentID, segment.segmentID)
	}

	stats := reclaimer.GetStats()
	if stats.TotalReclaims != 1 {
		t.Fatalf("TotalReclaims = %d, want 1", stats.TotalReclaims)
	}
	if stats.SpaceReclaimed == 0 {
		t.Fatalf("SpaceReclaimed = 0, want reclaimed bytes")
	}
	if stats.SegmentsCompact != 1 {
		t.Fatalf("SegmentsCompact = %d, want 1", stats.SegmentsCompact)
	}
}
