package manager

import (
	"testing"
)

func TestOptimizerRecognizesCompositeIndexPrefixes(t *testing.T) {
	conditions := []string{"tenant_id = 7 AND created_at >= 100"}
	if got := optimizerIndexPrefixLength([]string{"tenant_id", "created_at", "id"}, conditions); got != 2 {
		t.Fatalf("optimizerIndexPrefixLength() = %d, want 2", got)
	}
	if got := optimizerIndexPrefixLength([]string{"tenant_id", "status"}, []string{"status = 'ready'"}); got != 0 {
		t.Fatalf("composite index must not skip its leading column, got prefix %d", got)
	}
}

func TestOptimizerIndexPathCostImprovesWithCompositePrefix(t *testing.T) {
	if got := estimateIndexRows(10000, 1); got != 1000 {
		t.Fatalf("estimateIndexRows(single prefix) = %d, want 1000", got)
	}
	if got := estimateIndexRows(10000, 2); got != 100 {
		t.Fatalf("estimateIndexRows(composite prefix) = %d, want 100", got)
	}
}

func TestOptimizerRecognizesStructuredIndexPredicates(t *testing.T) {
	tests := []struct {
		name      string
		condition string
	}{
		{name: "is null", condition: "tenant_id IS NULL"},
		{name: "is not null", condition: "tenant_id IS NOT NULL"},
		{name: "between", condition: "tenant_id BETWEEN 7 AND 9"},
		{name: "null safe equality", condition: "tenant_id <=> 7"},
		{name: "not in", condition: "tenant_id NOT IN (7, 9)"},
		{name: "not like", condition: "tenant_id NOT LIKE '7%'"},
		{name: "constant-left equality", condition: "7 = tenant_id"},
		{name: "constant-left null-safe equality", condition: "'7' <=> tenant_id"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := optimizerIndexPrefixLength([]string{"tenant_id", "created_at"}, []string{test.condition}); got != 1 {
				t.Fatalf("optimizerIndexPrefixLength(%q) = %d, want 1", test.condition, got)
			}
		})
	}
}

func TestOptimizerRecognizesTupleEqualityIndexPrefix(t *testing.T) {
	condition := "(tenant_id, created_at) = (7, 100)"
	if got := optimizerIndexPrefixLength([]string{"tenant_id", "created_at", "id"}, []string{condition}); got != 2 {
		t.Fatalf("optimizerIndexPrefixLength(%q) = %d, want 2", condition, got)
	}
}

func TestOptimizerJoinOrderFollowsJoinGraph(t *testing.T) {
	optimizer := &OptimizerManager{}
	got := optimizer.OptimizeJoinOrder(
		[]string{"orders", "items", "users"},
		[]JoinCondition{
			{LeftTable: "orders", RightTable: "users", Condition: "orders.user_id = users.id"},
			{LeftTable: "orders", RightTable: "items", Condition: "orders.id = items.order_id"},
		},
	)

	want := []string{"orders", "users", "items"}
	if len(got) != len(want) {
		t.Fatalf("OptimizeJoinOrder() returned %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("OptimizeJoinOrder() = %v, want %v", got, want)
		}
	}
}
