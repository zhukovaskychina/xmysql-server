package engine

import (
	"context"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/vector"
)

type queryEmbeddingRuntime struct {
	request EmbeddingRequest
	result  EmbeddingResult
	err     error
	calls   int
}

func (r *queryEmbeddingRuntime) Embed(_ context.Context, request EmbeddingRequest) (EmbeddingResult, error) {
	r.calls++
	r.request = request
	return r.result, r.err
}

func TestAIEmbedTextCreatesUnboundQueryVector(t *testing.T) {
	statement, err := sqlparser.Parse("SELECT AI_EMBED('text_encoder', 'find the matching record')")
	if err != nil {
		t.Fatalf("parse AI_EMBED: %v", err)
	}
	selectStatement, ok := statement.(*sqlparser.Select)
	if !ok || len(selectStatement.SelectExprs) != 1 {
		t.Fatalf("unexpected parsed statement: %#v", statement)
	}
	aliased, ok := selectStatement.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		t.Fatalf("expected aliased expression, got %T", selectStatement.SelectExprs[0])
	}
	function, ok := aliased.Expr.(*sqlparser.FuncExpr)
	if !ok {
		t.Fatalf("expected function expression, got %T", aliased.Expr)
	}
	runtime := &queryEmbeddingRuntime{result: EmbeddingResult{
		ModelID: "model-a", ModelVersion: "2026-01", SpaceID: "shared-text-v1", Elements: []float32{0.1, 0.2},
	}}
	value, err := evaluateAIEmbedSQL(runtime, context.Background(), function, nil)
	if err != nil {
		t.Fatalf("evaluate AI_EMBED: %v", err)
	}
	query, ok := value.(vector.Value)
	if !ok {
		t.Fatalf("AI_EMBED returned %T, want vector.Value", value)
	}
	if query.State != vector.StateUnverified || query.Source != nil || query.SourceBlockID != "" {
		t.Fatalf("query vector unexpectedly has persisted provenance: %#v", query)
	}
	if runtime.calls != 1 || runtime.request.ModelAlias != "text_encoder" || runtime.request.Modality != "text" || runtime.request.Text != "find the matching record" {
		t.Fatalf("unexpected provider request/call count: %#v calls=%d", runtime.request, runtime.calls)
	}
}

func TestSelectProjectionEvaluatesAIEmbedText(t *testing.T) {
	runtime := &queryEmbeddingRuntime{result: EmbeddingResult{
		ModelID: "model-a", ModelVersion: "2026-01", SpaceID: "shared-text-v1", Elements: []float32{0.3, 0.4},
	}}
	selectExecutor := &SelectExecutor{embeddingRuntime: runtime}
	value, err := selectExecutor.evaluateProjectionExpression("AI_EMBED('text_encoder', 'query')", nil)
	if err != nil {
		t.Fatalf("evaluate SQL projection: %v", err)
	}
	if _, ok := value.(vector.Value); !ok {
		t.Fatalf("SQL projection returned %T, want vector.Value", value)
	}
	if runtime.calls != 1 || runtime.request.Text != "query" {
		t.Fatalf("unexpected provider request/call count: %#v calls=%d", runtime.request, runtime.calls)
	}
	statement, err := sqlparser.Parse("SELECT AI_EMBED('text_encoder', 'query')")
	if err != nil {
		t.Fatalf("parse result metadata query: %v", err)
	}
	selectExecutor.selectExprASTs = []sqlparser.Expr{statement.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr}
	if got := selectExecutor.getResultColumnTypes([]string{"AI_EMBED('text_encoder', 'query')"}, nil); len(got) != 1 || got[0] != "vector" {
		t.Fatalf("AI_EMBED result metadata = %v, want [vector]", got)
	}
}

func TestAIEmbedTextRejectsMissingProviderIdentity(t *testing.T) {
	statement, err := sqlparser.Parse("SELECT AI_EMBED('text_encoder', 'query')")
	if err != nil {
		t.Fatalf("parse AI_EMBED: %v", err)
	}
	function := statement.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr.(*sqlparser.FuncExpr)
	runtime := &queryEmbeddingRuntime{result: EmbeddingResult{Elements: []float32{0.1}}}
	if _, err := evaluateAIEmbedSQL(runtime, context.Background(), function, nil); err == nil {
		t.Fatal("expected invalid provider vector identity to be rejected")
	}
}
