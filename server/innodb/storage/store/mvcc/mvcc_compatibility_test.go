package mvcc

import "testing"

func TestDeprecatedMvccCompatibilityLifecycleRemainsFunctional(t *testing.T) {
	var manager Mvcc
	view, trx := manager.CreateView()
	if view == nil || trx == nil {
		t.Fatal("CreateView() returned nil compatibility state")
	}
	if manager.GetActiveReadViewSize() != 1 || !manager.IsReadViewActive(*view) {
		t.Fatalf("active view state = size %d active %v", manager.GetActiveReadViewSize(), manager.IsReadViewActive(*view))
	}
	if !view.IsVisible(int64(view.GetCreatorTrxID())) {
		t.Fatal("a view must see its creator transaction")
	}

	manager.CloneOldestView()
	if manager.GetActiveReadViewSize() != 2 {
		t.Fatalf("CloneOldestView() active size = %d, want 2", manager.GetActiveReadViewSize())
	}
	manager.CloseView(view, true)
	if manager.GetActiveReadViewSize() != 1 {
		t.Fatalf("CloseView() active size = %d, want 1", manager.GetActiveReadViewSize())
	}
}
