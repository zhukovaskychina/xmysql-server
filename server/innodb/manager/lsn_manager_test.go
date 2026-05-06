package manager

import (
	"testing"
)

func TestNewLSNManager(t *testing.T) {
	lm := NewLSNManager(1)
	if lm.GetCurrentLSN() != 1 {
		t.Errorf("initial LSN = %v, want 1", lm.GetCurrentLSN())
	}
	if lm.GetMinLSN() != 1 || lm.GetMaxLSN() != 1 {
		t.Errorf("MinLSN=%v MaxLSN=%v, want 1,1", lm.GetMinLSN(), lm.GetMaxLSN())
	}

	lm0 := NewLSNManager(0)
	if lm0.GetCurrentLSN() != 1 {
		t.Errorf("LSN(0) should become 1, got %v", lm0.GetCurrentLSN())
	}
}

func TestAllocateLSN(t *testing.T) {
	lm := NewLSNManager(100)

	l1 := lm.AllocateLSN()
	l2 := lm.AllocateLSN()
	l3 := lm.AllocateLSN()

	if l1 != 101 || l2 != 102 || l3 != 103 {
		t.Errorf("AllocateLSN = %v,%v,%v, want 101,102,103", l1, l2, l3)
	}
	if lm.GetCurrentLSN() != 103 {
		t.Errorf("GetCurrentLSN = %v, want 103", lm.GetCurrentLSN())
	}
	if lm.GetMaxLSN() != 103 {
		t.Errorf("GetMaxLSN = %v, want 103", lm.GetMaxLSN())
	}
}

func TestAllocateLSNRange(t *testing.T) {
	lm := NewLSNManager(10)

	start, end := lm.AllocateLSNRange(0)
	if start != 0 || end != 0 {
		t.Errorf("count=0: start=%v end=%v", start, end)
	}

	start, end = lm.AllocateLSNRange(5)
	if start != 11 || end != 15 {
		t.Errorf("count=5: start=%v end=%v, want 11,15", start, end)
	}
	if lm.GetCurrentLSN() != 15 {
		t.Errorf("GetCurrentLSN = %v, want 15", lm.GetCurrentLSN())
	}
}

func TestSetCheckpointLSN(t *testing.T) {
	lm := NewLSNManager(1)
	lm.AllocateLSN()
	lm.AllocateLSN()
	if lm.GetCurrentLSN() != 3 {
		t.Fatalf("current = %v", lm.GetCurrentLSN())
	}

	lm.SetCheckpointLSN(5)
	if lm.GetCurrentLSN() != 5 {
		t.Errorf("after SetCheckpointLSN(5) GetCurrentLSN = %v, want 5", lm.GetCurrentLSN())
	}
	if lm.GetMaxLSN() != 5 {
		t.Errorf("GetMaxLSN = %v, want 5", lm.GetMaxLSN())
	}

	next := lm.AllocateLSN()
	if next != 6 {
		t.Errorf("next AllocateLSN = %v, want 6", next)
	}
}

// TestGetCheckpointLSN TDD: 检查点 LSN 可读回；初始为 0，SetCheckpointLSN 后 GetCheckpointLSN 返回该值
func TestGetCheckpointLSN(t *testing.T) {
	lm := NewLSNManager(1)
	if got := lm.GetCheckpointLSN(); got != 0 {
		t.Errorf("initial GetCheckpointLSN = %v, want 0", got)
	}
	lm.SetCheckpointLSN(100)
	if got := lm.GetCheckpointLSN(); got != 100 {
		t.Errorf("after SetCheckpointLSN(100) GetCheckpointLSN = %v, want 100", got)
	}
	lm.SetCheckpointLSN(200)
	if got := lm.GetCheckpointLSN(); got != 200 {
		t.Errorf("after SetCheckpointLSN(200) GetCheckpointLSN = %v, want 200", got)
	}
}

func TestLSNRange(t *testing.T) {
	r := &LSNRange{Start: 10, End: 20}
	if !r.IsValid() {
		t.Error("range 10-20 should be valid")
	}
	if r.Size() != 11 {
		t.Errorf("Size = %v, want 11", r.Size())
	}
	if !r.Contains(15) || r.Contains(9) || r.Contains(21) {
		t.Error("Contains mismatch")
	}

	other := &LSNRange{Start: 18, End: 25}
	if !r.Overlaps(other) {
		t.Error("10-20 and 18-25 should overlap")
	}
	merged := r.Merge(other)
	if merged == nil || merged.Start != 10 || merged.End != 25 {
		t.Errorf("Merge = %+v", merged)
	}
}
