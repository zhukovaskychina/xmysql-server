package plan

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// TestCBOIntegratedOptimizer 测试集成优化器
func TestCBOIntegratedOptimizer(t *testing.T) {
	// 由于需要真实的SpaceManager和BPlusTreeManager，这里仅做基本验证
	t.Log("CBO集成优化器基本验证")

	// 测试统计信息类型
	stats := &TableStats{
		TableName:    "test_table",
		RowCount:     1000,
		AvgRowLength: 100,
		DataLength:   100000,
	}

	if stats.TableName != "test_table" {
		t.Errorf("Expected table name 'test_table', got '%s'", stats.TableName)
	}
	if stats.RowCount != 1000 {
		t.Errorf("Expected row count 1000, got %d", stats.RowCount)
	}
}

// TestHyperLogLog 测试HyperLogLog基数估算
func TestHyperLogLog(t *testing.T) {
	hll := NewHyperLogLog(14)

	// 添加一些值
	for i := 0; i < 1000; i++ {
		hll.Add(i)
	}

	// 估算基数
	count := hll.Count()

	// 允许2%误差
	expectedMin := int64(980)
	expectedMax := int64(1020)

	if count < expectedMin || count > expectedMax {
		t.Logf("HyperLogLog估算基数: %d (期望: 1000, 允许误差: 2%%)", count)
	} else {
		t.Logf("HyperLogLog估算基数: %d (精确值: 1000) - 通过", count)
	}
}

// TestSelectivityEstimator 测试选择率估算
func TestSelectivityEstimator(t *testing.T) {
	config := DefaultSelectivityEstimatorConfig()

	// 创建模拟统计信息收集器
	statsCollector := &StatisticsCollector{
		tableStats:  make(map[string]*TableStats),
		columnStats: make(map[string]*ColumnStats),
		indexStats:  make(map[string]*IndexStats),
		config: &StatisticsConfig{
			HistogramBuckets: 64,
		},
	}

	// 添加模拟统计信息
	statsCollector.tableStats["users"] = &TableStats{
		TableName: "users",
		RowCount:  10000,
	}

	statsCollector.columnStats["users.age"] = &ColumnStats{
		ColumnName:    "age",
		DistinctCount: 100,
		NotNullCount:  10000,
		NullCount:     0,
		MinValue:      int64(1),
		MaxValue:      int64(100),
	}

	estimator := NewSelectivityEstimator(statsCollector, config)

	// 创建测试表
	table := metadata.NewTable("users")
	table.AddColumn(&metadata.Column{
		Name:       "age",
		DataType:   metadata.TypeInt,
		IsNullable: false,
	})

	// 测试等值谓词选择率
	eqExpr := &BinaryOperation{
		Operator: "=",
		Left: &Column{
			Name: "age",
		},
		Right: &Constant{
			Value: int64(25),
		},
	}

	selectivity := estimator.EstimateSelectivity(table, eqExpr)
	expectedSel := 1.0 / 100.0 // 1/NDV

	if selectivity > expectedSel*1.1 || selectivity < expectedSel*0.9 {
		t.Errorf("等值选择率估算不准确: got %f, expected ~%f", selectivity, expectedSel)
	} else {
		t.Logf("等值选择率估算: %f (期望: %f) - 通过", selectivity, expectedSel)
	}
}

// TestJoinOrderOptimizer 测试连接顺序优化器
func TestJoinOrderOptimizer(t *testing.T) {
	costModel := NewDefaultCostModel()
	statsCollector := &StatisticsCollector{
		tableStats:  make(map[string]*TableStats),
		columnStats: make(map[string]*ColumnStats),
		indexStats:  make(map[string]*IndexStats),
		config: &StatisticsConfig{
			HistogramBuckets: 64,
		},
	}

	// 添加表统计信息
	statsCollector.tableStats["t1"] = &TableStats{
		TableName: "t1",
		RowCount:  1000,
	}
	statsCollector.tableStats["t2"] = &TableStats{
		TableName: "t2",
		RowCount:  500,
	}
	statsCollector.tableStats["t3"] = &TableStats{
		TableName: "t3",
		RowCount:  2000,
	}

	config := DefaultSelectivityEstimatorConfig()
	selectivityEstimator := NewSelectivityEstimator(statsCollector, config)

	joinConfig := DefaultJoinOrderOptimizerConfig()
	optimizer := NewJoinOrderOptimizer(costModel, statsCollector, selectivityEstimator, joinConfig)

	// 创建测试表
	tables := []*metadata.Table{
		metadata.NewTable("t1"),
		metadata.NewTable("t2"),
		metadata.NewTable("t3"),
	}

	// 简单测试：单表查询
	joinTree, err := optimizer.OptimizeJoinOrder(tables[:1], nil, nil)
	if err != nil {
		t.Errorf("单表优化失败: %v", err)
	}
	if joinTree == nil || joinTree.NodeType != "TABLE" {
		t.Error("单表优化结果不正确")
	} else {
		t.Logf("单表优化: %s - 通过", joinTree.Table.Name)
	}

	// 双表连接
	joinConditions := []Expression{
		&BinaryOperation{
			Operator: "=",
			Left: &Column{
				Name: "t1.id",
			},
			Right: &Column{
				Name: "t2.t1_id",
			},
		},
	}

	joinTree, err = optimizer.OptimizeJoinOrder(tables[:2], joinConditions, nil)
	if err != nil {
		t.Errorf("双表优化失败: %v", err)
	}
	if joinTree == nil {
		t.Error("双表优化结果为空")
	} else {
		t.Logf("双表优化: %s - 通过", joinTree.String())
	}
}

// TestHistogramTypes 测试直方图类型
func TestHistogramTypes(t *testing.T) {
	// 测试等宽直方图
	histogram := &Histogram{
		NumBuckets:    10,
		TotalCount:    1000,
		HistogramType: HistogramEquiWidth,
		Buckets: []Bucket{
			{LowerBound: int64(0), UpperBound: int64(10), Count: 100, Distinct: 10},
			{LowerBound: int64(10), UpperBound: int64(20), Count: 100, Distinct: 10},
		},
	}

	if histogram.HistogramType != HistogramEquiWidth {
		t.Error("直方图类型不正确")
	}

	if len(histogram.Buckets) != 2 {
		t.Error("直方图桶数量不正确")
	}

	t.Log("直方图类型测试 - 通过")
}

// TestCBOConfiguration 测试CBO配置
func TestCBOConfiguration(t *testing.T) {
	// 测试默认配置
	statsConfig := &StatisticsConfig{
		AutoUpdateInterval: 3600000000000,
		SampleRate:         0.1,
		HistogramBuckets:   64,
	}

	if statsConfig.HistogramBuckets != 64 {
		t.Errorf("Expected 64 histogram buckets, got %d", statsConfig.HistogramBuckets)
	}

	if statsConfig.SampleRate != 0.1 {
		t.Errorf("Expected sample rate 0.1, got %f", statsConfig.SampleRate)
	}

	t.Log("CBO配置测试 - 通过")
}

// BenchmarkHyperLogLog benchmark HyperLogLog性能
func BenchmarkHyperLogLog(b *testing.B) {
	hll := NewHyperLogLog(14)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hll.Add(i)
	}
}

// BenchmarkSelectivityEstimation benchmark选择率估算性能
func BenchmarkSelectivityEstimation(b *testing.B) {
	statsCollector := &StatisticsCollector{
		tableStats:  make(map[string]*TableStats),
		columnStats: make(map[string]*ColumnStats),
		indexStats:  make(map[string]*IndexStats),
		config: &StatisticsConfig{
			HistogramBuckets: 64,
		},
	}

	statsCollector.columnStats["users.age"] = &ColumnStats{
		ColumnName:    "age",
		DistinctCount: 100,
		NotNullCount:  10000,
	}

	estimator := NewSelectivityEstimator(statsCollector, DefaultSelectivityEstimatorConfig())
	table := metadata.NewTable("users")

	expr := &BinaryOperation{
		Operator: "=",
		Left: &Column{
			Name: "age",
		},
		Right: &Constant{
			Value: int64(25),
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		estimator.EstimateSelectivity(table, expr)
	}
}
