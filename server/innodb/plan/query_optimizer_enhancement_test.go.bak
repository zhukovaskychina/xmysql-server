package plan

import (
	"context"
	"testing"
)

// TestChooseBestJoinAlgorithm 测试连接算法选择
func TestChooseBestJoinAlgorithm(t *testing.T) {
	// 创建代价估算器
	collector := NewStatisticsCollector(nil)
	estimator := NewCostEstimator(collector, nil)

	// 创建测试表统计信息
	leftStats := &TableStats{
		TableName:    "orders",
		RowCount:     10000,
		AvgRowLength: 100,
	}

	rightStats := &TableStats{
		TableName:    "customers",
		RowCount:     1000,
		AvgRowLength: 80,
	}

	// 测试连接条件
	joinConditions := []Expression{
		&MockExpression{exprType: "EQUAL"},
	}

	// 选择最佳连接算法
	algorithm, cost, err := estimator.ChooseBestJoinAlgorithm(leftStats, rightStats, joinConditions)
	if err != nil {
		t.Fatalf("Failed to choose join algorithm: %v", err)
	}

	t.Logf("✅ Best join algorithm: %s", algorithm)
	t.Logf("   Estimated cost: %.2f", cost.TotalCost)
	t.Logf("   Output rows: %d", cost.OutputRows)
	t.Logf("   Selectivity: %.4f", cost.Selectivity)

	// 验证算法选择合理性
	if algorithm == "" {
		t.Error("Algorithm should not be empty")
	}

	if cost.TotalCost <= 0 {
		t.Error("Cost should be positive")
	}

	if cost.OutputRows < 0 {
		t.Error("Output rows should be non-negative")
	}
}

// TestChooseBestJoinAlgorithmSmallTables 测试小表连接
func TestChooseBestJoinAlgorithmSmallTables(t *testing.T) {
	collector := NewStatisticsCollector(nil)
	estimator := NewCostEstimator(collector, nil)

	// 小表统计信息
	leftStats := &TableStats{
		TableName:    "small_table1",
		RowCount:     100,
		AvgRowLength: 50,
	}

	rightStats := &TableStats{
		TableName:    "small_table2",
		RowCount:     50,
		AvgRowLength: 50,
	}

	joinConditions := []Expression{
		&MockExpression{exprType: "EQUAL"},
	}

	algorithm, cost, err := estimator.ChooseBestJoinAlgorithm(leftStats, rightStats, joinConditions)
	if err != nil {
		t.Fatalf("Failed to choose join algorithm: %v", err)
	}

	t.Logf("✅ Small tables join algorithm: %s", algorithm)
	t.Logf("   Cost: %.2f, Output rows: %d", cost.TotalCost, cost.OutputRows)

	// 对于小表，嵌套循环可能是最优的
	if algorithm != "NESTED_LOOP" && algorithm != "HASH_JOIN" {
		t.Logf("   Note: Algorithm %s chosen (expected NESTED_LOOP or HASH_JOIN for small tables)", algorithm)
	}
}

// TestChooseBestJoinAlgorithmLargeTables 测试大表连接
func TestChooseBestJoinAlgorithmLargeTables(t *testing.T) {
	collector := NewStatisticsCollector(nil)
	estimator := NewCostEstimator(collector, nil)

	// 大表统计信息
	leftStats := &TableStats{
		TableName:    "large_table1",
		RowCount:     1000000,
		AvgRowLength: 200,
	}

	rightStats := &TableStats{
		TableName:    "large_table2",
		RowCount:     500000,
		AvgRowLength: 150,
	}

	joinConditions := []Expression{
		&MockExpression{exprType: "EQUAL"},
	}

	algorithm, cost, err := estimator.ChooseBestJoinAlgorithm(leftStats, rightStats, joinConditions)
	if err != nil {
		t.Fatalf("Failed to choose join algorithm: %v", err)
	}

	t.Logf("✅ Large tables join algorithm: %s", algorithm)
	t.Logf("   Cost: %.2f, Output rows: %d", cost.TotalCost, cost.OutputRows)

	// 对于大表，哈希连接或排序合并连接通常更优
	if algorithm == "NESTED_LOOP" {
		t.Logf("   Warning: NESTED_LOOP chosen for large tables (may not be optimal)")
	}
}

// TestChooseAggregateAlgorithm 测试聚合算法选择
func TestChooseAggregateAlgorithm(t *testing.T) {
	collector := NewStatisticsCollector(nil)
	estimator := NewCostEstimator(collector, nil)

	// 测试数据
	inputRows := int64(10000)
	groupByColumns := []string{"category", "region"}
	aggregateFunctions := []string{"SUM(amount)", "COUNT(*)", "AVG(price)"}

	// 选择最佳聚合算法
	algorithm, cost, err := estimator.ChooseAggregateAlgorithm(inputRows, groupByColumns, aggregateFunctions)
	if err != nil {
		t.Fatalf("Failed to choose aggregate algorithm: %v", err)
	}

	t.Logf("✅ Best aggregate algorithm: %s", algorithm)
	t.Logf("   Estimated cost: %.2f", cost.TotalCost)
	t.Logf("   Output rows: %d", cost.OutputRows)
	t.Logf("   Selectivity: %.4f", cost.Selectivity)

	// 验证算法选择
	if algorithm != "HASH_AGGREGATE" && algorithm != "SORT_AGGREGATE" {
		t.Errorf("Unknown aggregate algorithm: %s", algorithm)
	}

	if cost.TotalCost <= 0 {
		t.Error("Cost should be positive")
	}
}

// TestChooseAggregateAlgorithmSmallInput 测试小数据集聚合
func TestChooseAggregateAlgorithmSmallInput(t *testing.T) {
	collector := NewStatisticsCollector(nil)
	estimator := NewCostEstimator(collector, nil)

	inputRows := int64(100)
	groupByColumns := []string{"status"}
	aggregateFunctions := []string{"COUNT(*)"}

	algorithm, cost, err := estimator.ChooseAggregateAlgorithm(inputRows, groupByColumns, aggregateFunctions)
	if err != nil {
		t.Fatalf("Failed to choose aggregate algorithm: %v", err)
	}

	t.Logf("✅ Small input aggregate algorithm: %s", algorithm)
	t.Logf("   Cost: %.2f, Output rows: %d", cost.TotalCost, cost.OutputRows)
}

// TestChooseAggregateAlgorithmLargeInput 测试大数据集聚合
func TestChooseAggregateAlgorithmLargeInput(t *testing.T) {
	collector := NewStatisticsCollector(nil)
	estimator := NewCostEstimator(collector, nil)

	inputRows := int64(1000000)
	groupByColumns := []string{"year", "month", "day", "hour"}
	aggregateFunctions := []string{"SUM(sales)", "COUNT(*)", "AVG(price)", "MAX(quantity)"}

	algorithm, cost, err := estimator.ChooseAggregateAlgorithm(inputRows, groupByColumns, aggregateFunctions)
	if err != nil {
		t.Fatalf("Failed to choose aggregate algorithm: %v", err)
	}

	t.Logf("✅ Large input aggregate algorithm: %s", algorithm)
	t.Logf("   Cost: %.2f, Output rows: %d", cost.TotalCost, cost.OutputRows)
}

// TestHashJoinCostEstimation 测试哈希连接代价估算
func TestHashJoinCostEstimation(t *testing.T) {
	collector := NewStatisticsCollector(nil)
	estimator := NewCostEstimator(collector, nil)

	leftStats := &TableStats{
		TableName:    "orders",
		RowCount:     10000,
		AvgRowLength: 100,
	}

	rightStats := &TableStats{
		TableName:    "customers",
		RowCount:     1000,
		AvgRowLength: 80,
	}

	joinConditions := []Expression{
		&MockExpression{exprType: "EQUAL"},
	}

	cost, err := estimator.estimateHashJoinCost(leftStats, rightStats, joinConditions)
	if err != nil {
		t.Fatalf("Failed to estimate hash join cost: %v", err)
	}

	t.Logf("✅ Hash join cost estimation:")
	t.Logf("   Total cost: %.2f", cost.TotalCost)
	t.Logf("   CPU cost: %.2f", cost.CPUCost)
	t.Logf("   Output rows: %d", cost.OutputRows)
	t.Logf("   Selectivity: %.4f", cost.Selectivity)

	// 验证代价合理性
	if cost.TotalCost <= 0 {
		t.Error("Total cost should be positive")
	}

	if cost.CPUCost <= 0 {
		t.Error("CPU cost should be positive")
	}

	// 哈希连接应该选择较小的表作为构建端
	// 验证输出行数合理
	maxOutput := leftStats.RowCount * rightStats.RowCount
	if cost.OutputRows > maxOutput {
		t.Errorf("Output rows %d exceeds maximum %d", cost.OutputRows, maxOutput)
	}
}

// TestJoinSelectivityEstimation 测试连接选择率估算
func TestJoinSelectivityEstimation(t *testing.T) {
	collector := NewStatisticsCollector(nil)
	estimator := NewCostEstimator(collector, nil)

	leftStats := &TableStats{
		TableName: "table1",
		RowCount:  10000,
	}

	rightStats := &TableStats{
		TableName: "table2",
		RowCount:  5000,
	}

	// 测试有连接条件的情况
	joinConditions := []Expression{
		&MockExpression{exprType: "EQUAL"},
	}

	selectivity := estimator.estimateJoinSelectivity(leftStats, rightStats, joinConditions)
	t.Logf("✅ Join selectivity with conditions: %.4f", selectivity)

	if selectivity <= 0 || selectivity > 1 {
		t.Errorf("Selectivity %.4f out of range [0, 1]", selectivity)
	}

	// 测试无连接条件的情况（笛卡尔积）
	selectivityCartesian := estimator.estimateJoinSelectivity(leftStats, rightStats, []Expression{})
	t.Logf("✅ Join selectivity without conditions (Cartesian): %.4f", selectivityCartesian)

	if selectivityCartesian != 1.0 {
		t.Errorf("Cartesian product selectivity should be 1.0, got %.4f", selectivityCartesian)
	}
}

// MockExpression 模拟表达式
type MockExpression struct {
	exprType string
}

func (e *MockExpression) Evaluate(ctx context.Context, row []interface{}) (interface{}, error) {
	return true, nil
}

func (e *MockExpression) GetType() string {
	return e.exprType
}

func (e *MockExpression) GetColumns() []string {
	return []string{"col1", "col2"}
}
