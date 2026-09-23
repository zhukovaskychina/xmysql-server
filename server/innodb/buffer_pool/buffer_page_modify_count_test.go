package buffer_pool

import "testing"

func TestBufferPageTracksModifyCountAcrossDirtyTransitions(t *testing.T) {
	page := &BufferPage{}
	page.Init(1, 2, nil)
	page.MarkDirty()
	page.MarkDirty()
	if got := page.GetModifyCount(); got != 2 {
		t.Fatalf("modify count after repeated MarkDirty = %d, want 2", got)
	}
	page.ClearDirty()
	page.SetDirty(true)
	if got := page.GetModifyCount(); got != 3 {
		t.Fatalf("modify count after dirty transition = %d, want 3", got)
	}
}
