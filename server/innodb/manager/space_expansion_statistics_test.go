package manager

import (
	"testing"
)

func TestSpaceExpansionUsesRealSpaceCapacity(t *testing.T) {
	space := &MockSpace{
		id:      7,
		size:    2 * pagesPerExtent * pageSizeBytes,
		extents: 4,
	}
	manager := &SpaceExpansionManager{}

	if got := manager.getSpaceSize(space); got != 4*pagesPerExtent*pageSizeBytes {
		t.Fatalf("getSpaceSize() = %d, want %d", got, 4*pagesPerExtent*pageSizeBytes)
	}
	if got := manager.calculateUsageRate(space); got != 50 {
		t.Fatalf("calculateUsageRate() = %v, want 50", got)
	}
}
