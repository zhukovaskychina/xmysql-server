package manager

import (
	"testing"
)

func TestStorageManagerExposesPageManagerAdapter(t *testing.T) {
	sm := &StorageManager{}
	pageManager := sm.GetPageManager()
	if pageManager == nil {
		t.Fatal("GetPageManager() returned nil")
	}
	if _, err := pageManager.AllocatePage(nil); err == nil {
		t.Fatal("AllocatePage() unexpectedly succeeded without storage")
	}
}
