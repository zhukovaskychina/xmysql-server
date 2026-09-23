package engine

import "testing"

func TestSelectExecutorCachesCompiledProjectionExpression(t *testing.T) {
	executor := &SelectExecutor{}
	first, err := executor.evaluateProjectionExpression("amount + 1", map[string]interface{}{"amount": int64(2)})
	if err != nil {
		t.Fatalf("first projection evaluation error = %v", err)
	}
	if first != int64(3) {
		t.Fatalf("first projection value = %#v, want 3", first)
	}
	entry, ok := executor.projectionExpressionCache["amount + 1"]
	if !ok {
		t.Fatal("projection expression was not cached")
	}
	if entry.compiled == nil {
		t.Fatal("supported projection expression was not compiled")
	}
	second, err := executor.evaluateProjectionExpression("amount + 1", map[string]interface{}{"amount": int64(4)})
	if err != nil {
		t.Fatalf("cached projection evaluation error = %v", err)
	}
	if second != int64(5) {
		t.Fatalf("cached projection value = %#v, want 5", second)
	}
}
