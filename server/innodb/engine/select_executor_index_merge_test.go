package engine

import "testing"

func TestIntersectSecondaryIndexPrimaryKeysPreservesFirstBranchOrder(t *testing.T) {
	got := intersectSecondaryIndexPrimaryKeys([][]string{
		{"pk-2", "pk-1", "pk-2"},
		{"pk-3", "pk-1"},
		{"pk-1", "pk-4"},
	})
	if len(got) != 1 || got[0] != "pk-1" {
		t.Fatalf("intersection keys = %v, want [pk-1]", got)
	}
}
