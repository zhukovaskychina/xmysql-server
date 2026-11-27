package plan

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestJoinOrderOptimizerMatchesQualifiedColumnsToTables(t *testing.T) {
	t1 := metadata.NewTable("t1")
	t1.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	t2 := metadata.NewTable("t2")
	t2.AddColumn(&metadata.Column{Name: "t1_id", DataType: metadata.TypeInt})
	t2.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	t3 := metadata.NewTable("t3")
	t3.AddColumn(&metadata.Column{Name: "active", DataType: metadata.TypeInt})
	tables := []*metadata.Table{t1, t2, t3}

	optimizer := NewJoinOrderOptimizer(NewDefaultCostModel(), nil, nil, DefaultJoinOrderOptimizerConfig())
	join := &BinaryOperation{
		Op:    OpEQ,
		Left:  &Column{Name: "t1.id"},
		Right: &Column{Name: "t2.t1_id"},
	}
	filter := &BinaryOperation{
		Op:    OpEQ,
		Left:  &Column{Name: "t3.active"},
		Right: &Constant{Value: int64(1)},
	}

	if !optimizer.hasJoinCondition(1, 2, []Expression{join, filter}, tables) {
		t.Fatal("expected t1/t2 join condition to be recognized")
	}
	if optimizer.hasJoinCondition(1, 4, []Expression{join, filter}, tables) {
		t.Fatal("t1/t3 must not be treated as connected by the t1/t2 join")
	}
	conditions := optimizer.extractJoinConditions(1, 2, []Expression{join, filter}, tables)
	if len(conditions) != 1 || conditions[0] != join {
		t.Fatalf("expected only t1/t2 join condition, got %#v", conditions)
	}
}

func TestJoinOrderOptimizerUsesColumnNDVForEquiJoinSelectivity(t *testing.T) {
	stats := &StatisticsCollector{
		tableStats:  make(map[string]*TableStats),
		columnStats: make(map[string]*ColumnStats),
		indexStats:  make(map[string]*IndexStats),
	}
	stats.columnStats["t1.id"] = &ColumnStats{ColumnName: "id", DistinctCount: 100}
	stats.columnStats["t2.t1_id"] = &ColumnStats{ColumnName: "t1_id", DistinctCount: 20}
	optimizer := NewJoinOrderOptimizer(NewDefaultCostModel(), stats, nil, DefaultJoinOrderOptimizerConfig())
	condition := &BinaryOperation{Op: OpEQ, Left: &Column{Name: "t1.id"}, Right: &Column{Name: "t2.t1_id"}}
	selectivity := optimizer.estimateJoinSelectivity([]Expression{condition}, nil)
	if selectivity < 0.009 || selectivity > 0.011 {
		t.Fatalf("expected equi-join selectivity 1/max(NDV)=0.01, got %v", selectivity)
	}
}
