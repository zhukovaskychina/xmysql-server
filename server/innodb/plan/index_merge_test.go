package plan

import (
	"strings"
	"testing"
)

func TestStatisticsHelpers(t *testing.T) {
	now := getCurrentTime()
	if now <= 0 {
		t.Errorf("invalid time: %d", now)
	}

	if calculateValueSize(int64(1)) == 0 {
		t.Error("size of int64 should be >0")
	}

	key := buildIndexKey([]interface{}{1, "a"})
	if key != "1|a" {
		t.Errorf("unexpected index key %s", key)
	}

	cf := calculateClusterFactor([][]interface{}{{1}, {1}, {2}})
	if cf <= 0 {
		t.Errorf("cluster factor not computed")
	}

	if !less(int64(1), int64(2)) {
		t.Error("less failed for int64")
	}
}

func TestIndexMergeCandidates(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()

	conds := []Expression{
		&BinaryOperation{Op: OpEQ, Left: &Column{Name: "id"}, Right: &Constant{Value: int64(1)}},
		&BinaryOperation{Op: OpEQ, Left: &Column{Name: "email"}, Right: &Constant{Value: "a"}},
	}

	candidate, err := opt.OptimizeIndexAccess(table, conds, []string{"id", "email"})
	if err != nil {
		t.Fatalf("optimize failed: %v", err)
	}
	if candidate == nil || candidate.Index == nil || !strings.Contains(candidate.Index.Name, "+") {
		t.Fatalf("expected merged index candidate, got %#v", candidate)
	}
}

func TestIndexMergeFallbackSingle(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()

	conds := []Expression{
		&BinaryOperation{Op: OpEQ, Left: &Column{Name: "id"}, Right: &Constant{Value: int64(1)}},
	}

	candidate, err := opt.OptimizeIndexAccess(table, conds, []string{"id"})
	if err != nil {
		t.Fatalf("optimize failed: %v", err)
	}
	if candidate == nil || candidate.Index == nil {
		t.Fatalf("no candidate")
	}
	if strings.Contains(candidate.Index.Name, "+") {
		t.Fatalf("expected single index, got merged %s", candidate.Index.Name)
	}
}
