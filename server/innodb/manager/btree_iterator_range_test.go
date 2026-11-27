package manager

import (
	"context"
	"testing"
)

func TestRangeSearchOptimizedReturnsNoRowsWhenStartIsAfterLastKey(t *testing.T) {
	btree := newTestBPlusTreeManager(t, nil)
	ctx := context.Background()
	if err := btree.Init(ctx, 1, 100); err != nil {
		t.Fatalf("init btree: %v", err)
	}
	for _, key := range []string{"key_001", "key_002", "key_003"} {
		if err := btree.Insert(ctx, key, []byte(key)); err != nil {
			t.Fatalf("insert %s: %v", key, err)
		}
	}

	rows, err := btree.RangeSearchOptimized(ctx, "key_999", "key_1000")
	if err != nil {
		t.Fatalf("range search: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("range search returned %v for a range after the last key", rows)
	}
}
