package plan

import (
	"math"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestANDIndexMergeUsesIntersectionSemantics(t *testing.T) {
	first := &IndexCandidate{Index: &metadata.Index{Name: "idx_a"}, Selectivity: 0.10, Cost: 10}
	second := &IndexCandidate{Index: &metadata.Index{Name: "idx_b"}, Selectivity: 0.20, Cost: 20}
	merged := NewIndexPushdownOptimizer().mergeCandidates([]*IndexCandidate{first, second})
	if len(merged) != 1 {
		t.Fatalf("mergeCandidates() count = %d, want 1", len(merged))
	}
	if merged[0].MergeMode != IndexMergeIntersection || merged[0].Index.Name != "INDEX_MERGE_AND" {
		t.Fatalf("merge mode/index = %q/%q, want intersection/INDEX_MERGE_AND", merged[0].MergeMode, merged[0].Index.Name)
	}
	if math.Abs(merged[0].Selectivity-0.02) > 1e-12 {
		t.Fatalf("AND selectivity = %v, want 0.02", merged[0].Selectivity)
	}

	rows := IntersectIndexMergeRows(
		[][]map[string]interface{}{
			{{"id": int64(1)}, {"id": int64(2)}},
			{{"id": int64(2)}, {"id": int64(3)}},
		},
		"id",
	)
	if len(rows) != 1 || rows[0]["id"] != int64(2) {
		t.Fatalf("intersection rows = %#v, want id=2", rows)
	}
}

func TestORIndexMergeBuildsBranchesAndDeduplicatesRows(t *testing.T) {
	table := createUsersTable()
	optimizer := NewIndexPushdownOptimizer()
	setupUsersStatistics(optimizer)
	predicate := &BinaryOperation{
		Op:    OpOr,
		Left:  &BinaryOperation{Op: OpEQ, Left: &Column{Name: "age"}, Right: &Constant{Value: int64(20)}},
		Right: &BinaryOperation{Op: OpEQ, Left: &Column{Name: "city"}, Right: &Constant{Value: "Paris"}},
	}
	candidate, err := optimizer.OptimizeIndexAccess(table, []Expression{predicate}, []string{"age", "city"})
	if err != nil {
		t.Fatalf("build OR index merge: %v", err)
	}
	if candidate == nil || candidate.Index.Name != "INDEX_MERGE_OR" || len(candidate.IndexMergeBranches) != 2 || !candidate.DeduplicateRows {
		t.Fatalf("unexpected OR plan: %#v", candidate)
	}
	rows := DeduplicateIndexMergeRows([]map[string]interface{}{
		{"id": int64(1), "age": int64(20)},
		{"id": int64(1), "city": "Paris"},
		{"id": int64(2), "city": "Paris"},
	}, "id")
	if len(rows) != 2 {
		t.Fatalf("dedup rows = %d, want 2", len(rows))
	}
}

func TestORIndexMergeDistributesBoundedAndPredicate(t *testing.T) {
	table := createUsersTable()
	optimizer := NewIndexPushdownOptimizer()
	setupUsersStatistics(optimizer)
	predicate := &BinaryOperation{
		Op: OpAnd,
		Left: &BinaryOperation{
			Op:    OpOr,
			Left:  &BinaryOperation{Op: OpEQ, Left: &Column{Name: "age"}, Right: &Constant{Value: int64(20)}},
			Right: &BinaryOperation{Op: OpEQ, Left: &Column{Name: "city"}, Right: &Constant{Value: "Paris"}},
		},
		Right: &BinaryOperation{Op: OpEQ, Left: &Column{Name: "name"}, Right: &Constant{Value: "Alice"}},
	}
	candidate, err := optimizer.OptimizeIndexAccess(table, []Expression{predicate}, []string{"id", "name", "age", "city"})
	if err != nil {
		t.Fatalf("build distributed OR index merge: %v", err)
	}
	if candidate == nil || candidate.Index.Name != "INDEX_MERGE_OR" || len(candidate.IndexMergeBranches) != 2 {
		t.Fatalf("unexpected distributed OR plan: %#v", candidate)
	}
	for i, branch := range candidate.IndexMergeBranches {
		if branch == nil || len(branch.Conditions) == 0 {
			t.Fatalf("branch %d = %#v, want an indexable OR branch", i, branch)
		}
	}
}

func TestSQLThreeValuedLogicForORAnd(t *testing.T) {
	tests := []struct {
		name        string
		left, right interface{}
		want        interface{}
		or          bool
	}{
		{"true or null", true, nil, true, true},
		{"false or null", false, nil, nil, true},
		{"false and null", false, nil, false, false},
		{"true and null", true, nil, nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got interface{}
			var err error
			if tc.or {
				got, err = evalOr(tc.left, tc.right)
			} else {
				got, err = evalAnd(tc.left, tc.right)
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %v, err=%v, want %v", got, err, tc.want)
			}
		})
	}
}
