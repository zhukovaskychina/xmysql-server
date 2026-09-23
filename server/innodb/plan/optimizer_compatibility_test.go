package plan

import (
	"math"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestOptimizerCompatibilityAccessPathMatrix(t *testing.T) {
	table := createUsersTable()
	optimizer := NewIndexPushdownOptimizer()
	setupUsersStatistics(optimizer)
	cases := []struct {
		name       string
		conditions []Expression
		wantIndex  string
	}{
		{"composite equality prefix", []Expression{&BinaryOperation{Op: OpEQ, Left: &Column{Name: "age"}, Right: &Constant{Value: int64(20)}}, &BinaryOperation{Op: OpEQ, Left: &Column{Name: "city"}, Right: &Constant{Value: "Paris"}}}, "idx_age_city"},
		{"composite range prefix", []Expression{&BinaryOperation{Op: OpGT, Left: &Column{Name: "age"}, Right: &Constant{Value: int64(20)}}}, "idx_age_city"},
		{"city leading index", []Expression{&BinaryOperation{Op: OpEQ, Left: &Column{Name: "city"}, Right: &Constant{Value: "Paris"}}}, "idx_city_age"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate, err := optimizer.OptimizeIndexAccess(table, tc.conditions, []string{"age", "city"})
			if err != nil {
				t.Fatalf("optimize access path: %v", err)
			}
			if candidate == nil || candidate.Index == nil || candidate.Index.Name != tc.wantIndex {
				t.Fatalf("access path = %#v, want %s", candidate, tc.wantIndex)
			}
		})
	}
}

func TestOptimizerCompatibilityExtractsExpressionBetweenBounds(t *testing.T) {
	table := createUsersTable()
	optimizer := NewIndexPushdownOptimizer()
	candidate, err := optimizer.OptimizeIndexAccess(table, []Expression{&BetweenExpression{
		Column:    &Column{Name: "age"},
		LowerExpr: &Constant{Value: int64(18)},
		UpperExpr: &Constant{Value: int64(60)},
	}}, []string{"age"})
	if err != nil {
		t.Fatalf("structured BETWEEN optimization: %v", err)
	}
	if candidate == nil || len(candidate.Conditions) != 2 {
		t.Fatalf("structured BETWEEN candidate = %#v, want two range conditions", candidate)
	}
	if candidate.Conditions[0].Operator != ">=" || candidate.Conditions[1].Operator != "<=" {
		t.Fatalf("structured BETWEEN conditions = %#v, want >= and <=", candidate.Conditions)
	}
}

func TestOptimizerCompatibilityUsesIndexMergeForNotBetween(t *testing.T) {
	table := createUsersTable()
	optimizer := NewIndexPushdownOptimizer()
	candidate, err := optimizer.OptimizeIndexAccess(table, []Expression{&BetweenExpression{
		Column:    &Column{Name: "age"},
		LowerExpr: &Constant{Value: int64(18)},
		UpperExpr: &Constant{Value: int64(60)},
		Not:       true,
	}}, []string{"age"})
	if err != nil {
		t.Fatalf("NOT BETWEEN optimization: %v", err)
	}
	if candidate == nil || candidate.Index == nil || candidate.Index.Name != "INDEX_MERGE_OR" || len(candidate.IndexMergeBranches) != 2 {
		t.Fatalf("NOT BETWEEN candidate = %#v, want two-branch index merge", candidate)
	}
}

func TestOptimizerCompatibilityUsesCaseInsensitiveStatistics(t *testing.T) {
	table := metadata.NewTable("Orders")
	table.AddColumn(&metadata.Column{Name: "Age"})
	index := &metadata.Index{Name: "idx_age", Columns: []string{"Age"}}
	if err := table.AddIndex(index); err != nil {
		t.Fatalf("AddIndex() error = %v", err)
	}
	optimizer := NewIndexPushdownOptimizer()
	optimizer.SetStatistics(
		map[string]*TableStats{"Orders": {TableName: "Orders", RowCount: 10000}},
		map[string]*IndexStats{"IDX_AGE": {IndexName: "IDX_AGE", Cardinality: 100}},
		map[string]*ColumnStats{"Orders.Age": {ColumnName: "Age", DistinctCount: 100}},
	)
	candidate, err := optimizer.OptimizeIndexAccess(table, []Expression{&BinaryOperation{
		Op:    OpEQ,
		Left:  &Column{Name: "orders.age"},
		Right: &Constant{Value: int64(7)},
	}}, []string{"Age"})
	if err != nil {
		t.Fatalf("case-insensitive statistics optimization: %v", err)
	}
	if candidate == nil || len(candidate.Conditions) != 1 {
		t.Fatalf("case-insensitive statistics candidate = %#v, want one condition", candidate)
	}
	if candidate.Selectivity != 0.01 {
		t.Fatalf("case-insensitive statistics selectivity = %v, want 0.01 from NDV", candidate.Selectivity)
	}
}

func TestOptimizerCompatibilityStatisticsAndJoinCostAreExplainable(t *testing.T) {
	builder := &StatsBuilder{sampleRate: 1, maxSamples: 100}
	column := builder.BuildColumnStats("score", []interface{}{nil, int64(1), int64(1), int64(3)})
	if column.NullCount != 1 || column.DistinctCount != 2 || column.Histogram == nil {
		t.Fatalf("unexpected column statistics: %#v", column)
	}
	left := metadata.NewTable("left_t")
	right := metadata.NewTable("right_t")
	left.Stats.RowCount = 100
	right.Stats.RowCount = 1000
	collector := &StatisticsCollector{tableStats: map[string]*TableStats{
		"left_t":  {TableName: "left_t", RowCount: 100},
		"right_t": {TableName: "right_t", RowCount: 1000},
	}}
	estimator := NewCostEstimator(collector, nil)
	algorithm, choice, err := estimator.ChooseBestJoinAlgorithm(collector.tableStats["left_t"], collector.tableStats["right_t"], nil)
	if err != nil || choice == nil || algorithm == "" {
		t.Fatalf("join cost choice = %s %#v, err=%v", algorithm, choice, err)
	}
	_ = left
	_ = right
}

func TestCostEstimatorUsesColumnNDVAndNullStatsForJoinSelectivity(t *testing.T) {
	collector := &StatisticsCollector{
		tableStats: map[string]*TableStats{
			"left_t":  {TableName: "left_t", RowCount: 1000},
			"right_t": {TableName: "right_t", RowCount: 100},
		},
		columnStats: map[string]*ColumnStats{
			"left_t.id":  {ColumnName: "id", DistinctCount: 100, NotNullCount: 900},
			"right_t.id": {ColumnName: "id", DistinctCount: 10, NotNullCount: 90},
		},
		indexStats: map[string]*IndexStats{},
	}
	estimator := NewCostEstimator(collector, nil)
	condition := &BinaryOperation{Op: OpEQ, Left: &Column{Name: "id"}, Right: &Column{Name: "id"}}

	selectivity := estimator.estimateJoinSelectivity(collector.tableStats["left_t"], collector.tableStats["right_t"], []Expression{condition})
	if math.Abs(selectivity-0.0081) > 0.000001 {
		t.Fatalf("join selectivity = %f, want 0.0081 from max NDV and NULL ratios", selectivity)
	}

	cost, err := estimator.EstimateJoinCost(
		metadata.NewTable("left_t"),
		metadata.NewTable("right_t"),
		JoinTypeInner,
		[]Expression{condition},
	)
	if err != nil {
		t.Fatalf("EstimateJoinCost() error = %v", err)
	}
	if cost.OutputRows != 810 {
		t.Fatalf("join output rows = %d, want 810", cost.OutputRows)
	}
}

func TestCostEstimatorUsesEnhancedStatisticsProvider(t *testing.T) {
	legacy := &StatisticsCollector{
		tableStats:  map[string]*TableStats{},
		columnStats: map[string]*ColumnStats{},
		indexStats:  map[string]*IndexStats{},
	}
	enhanced := NewEnhancedStatisticsCollector(&StatisticsConfig{EnableAutoUpdate: false}, nil, nil)
	enhanced.tableStats["orders"] = &TableStats{TableName: "orders", RowCount: 37}
	estimator := NewCostEstimator(legacy, nil)
	estimator.SetStatisticsProvider(enhanced)

	cost, err := estimator.EstimateTableScanCost(metadata.NewTable("orders"), 1)
	if err != nil {
		t.Fatalf("EstimateTableScanCost() error = %v", err)
	}
	if cost.OutputRows != 37 {
		t.Fatalf("OutputRows = %d, want 37 from enhanced statistics", cost.OutputRows)
	}
}

func TestCostEstimatorUsesMeasuredTableSizeForTableScan(t *testing.T) {
	table := metadata.NewTable("measured_scan")
	table.AddColumn(&metadata.Column{Name: "payload", DataType: metadata.TypeText})
	collector := &StatisticsCollector{
		tableStats: map[string]*TableStats{
			"measured_scan": {
				TableName:    "measured_scan",
				RowCount:     1000,
				AvgRowLength: 16,
				DataLength:   16000,
			},
		},
		columnStats: map[string]*ColumnStats{},
		indexStats:  map[string]*IndexStats{},
	}
	estimator := NewCostEstimator(collector, nil)

	cost, err := estimator.EstimateTableScanCost(table, 1)
	if err != nil {
		t.Fatalf("EstimateTableScanCost() error = %v", err)
	}
	if cost.IOCost != 1 {
		t.Fatalf("IOCost = %v, want one measured 16KB data page", cost.IOCost)
	}
	if cost.CPUCost != 10 {
		t.Fatalf("CPUCost = %v, want 1000 rows * 0.01", cost.CPUCost)
	}
}

func TestCostEstimatorUsesMeasuredIndexPagesAndClusterFactor(t *testing.T) {
	table := metadata.NewTable("measured_index")
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeBigInt})
	index := &metadata.Index{Name: "idx_id", Columns: []string{"id"}}
	if err := table.AddIndex(index); err != nil {
		t.Fatalf("AddIndex() error = %v", err)
	}
	collector := &StatisticsCollector{
		tableStats: map[string]*TableStats{
			"measured_index": {TableName: "measured_index", RowCount: 10000},
		},
		columnStats: map[string]*ColumnStats{},
		indexStats: map[string]*IndexStats{
			"measured_index.idx_id": {
				IndexName:     "idx_id",
				Selectivity:   0.01,
				LeafPages:     100,
				ClusterFactor: 1,
			},
		},
	}
	estimator := NewCostEstimator(collector, nil)
	compact, err := estimator.EstimateIndexScanCost(table, index, 0.1, nil)
	if err != nil {
		t.Fatalf("compact index EstimateIndexScanCost() error = %v", err)
	}
	collector.indexStats["measured_index.idx_id"].LeafPages = 1000
	collector.indexStats["measured_index.idx_id"].ClusterFactor = 2
	scattered, err := estimator.EstimateIndexScanCost(table, index, 0.1, nil)
	if err != nil {
		t.Fatalf("scattered index EstimateIndexScanCost() error = %v", err)
	}
	if scattered.IOCost <= compact.IOCost {
		t.Fatalf("scattered index IOCost = %v, compact = %v; measured pages/cluster factor should increase cost", scattered.IOCost, compact.IOCost)
	}
}
