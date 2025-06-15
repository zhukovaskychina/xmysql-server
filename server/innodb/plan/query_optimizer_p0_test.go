package plan

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// TestIndexPushdownOptimizer 测试索引下推优化器
func TestIndexPushdownOptimizer(t *testing.T) {
	// 创建测试表
	table := createTestTable()

	// 创建索引下推优化器
	optimizer := NewIndexPushdownOptimizer()

	// 设置模拟统计信息
	tableStats := map[string]*TableStats{
		"users": {
			TableName:       "users",
			RowCount:        100000,
			TotalSize:       10000000,
			ModifyCount:     1000,
			LastAnalyzeTime: time.Now().Unix(),
		},
	}

	columnStats := map[string]*ColumnStats{
		"users.id": {
			ColumnName:    "id",
			NotNullCount:  100000,
			DistinctCount: 100000,
			NullCount:     0,
			MinValue:      int64(1),
			MaxValue:      int64(100000),
		},
		"users.name": {
			ColumnName:    "name",
			NotNullCount:  99000,
			DistinctCount: 95000,
			NullCount:     1000,
			MinValue:      "Alice",
			MaxValue:      "Zoe",
		},
	}

	indexStats := map[string]*IndexStats{
		"users.PRIMARY": {
			IndexName:     "PRIMARY",
			Cardinality:   100000,
			ClusterFactor: 1.0,
			PrefixLength:  4,
			Selectivity:   1.0,
		},
		"users.idx_name": {
			IndexName:     "idx_name",
			Cardinality:   95000,
			ClusterFactor: 1.2,
			PrefixLength:  10,
			Selectivity:   0.95,
		},
	}

	optimizer.SetStatistics(tableStats, indexStats, columnStats)

	// 创建WHERE条件
	whereConditions := []Expression{
		&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "id"},
			Right: &Constant{Value: int64(12345)},
		},
		&BinaryOperation{
			Op:    OpLike,
			Left:  &Column{Name: "name"},
			Right: &Constant{Value: "John%"},
		},
	}

	// 测试索引优化
	candidate, err := optimizer.OptimizeIndexAccess(table, whereConditions, []string{"id", "name"})
	if err != nil {
		t.Fatalf("索引优化失败: %v", err)
	}

	if candidate == nil {
		t.Fatal("未找到合适的索引候选")
	}

	t.Logf("选择的索引: %s", candidate.Index.Name)
	t.Logf("索引条件数量: %d", len(candidate.Conditions))
	t.Logf("是否覆盖索引: %v", candidate.CoverIndex)
	t.Logf("选择性: %.4f", candidate.Selectivity)
	t.Logf("代价: %.2f", candidate.Cost)
}

// TestStatisticsCollector 测试统计信息收集器
func TestStatisticsCollector(t *testing.T) {
	// 创建统计信息收集器
	config := &StatisticsConfig{
		AutoUpdateInterval: 1 * time.Hour,
		SampleRate:         0.1,
		HistogramBuckets:   32,
		ExpirationTime:     24 * time.Hour,
		EnableAutoUpdate:   false, // 测试中禁用自动更新
	}

	collector := NewStatisticsCollector(config)
	defer collector.Stop()

	// 创建测试表和列
	table := createTestTable()
	column := table.Columns[0] // id列
	index := table.Indices[0]  // 主键索引

	ctx := context.Background()

	// 测试表统计信息收集
	tableStats, err := collector.CollectTableStatistics(ctx, table)
	if err != nil {
		t.Fatalf("收集表统计信息失败: %v", err)
	}

	if tableStats.RowCount <= 0 {
		t.Fatal("表行数应该大于0")
	}

	t.Logf("表统计信息 - 行数: %d, 大小: %d bytes", tableStats.RowCount, tableStats.TotalSize)

	// 测试列统计信息收集
	columnStats, err := collector.CollectColumnStatistics(ctx, table, column)
	if err != nil {
		t.Fatalf("收集列统计信息失败: %v", err)
	}

	if columnStats.DistinctCount <= 0 {
		t.Fatal("列不同值数量应该大于0")
	}

	t.Logf("列统计信息 - 不同值: %d, 空值: %d", columnStats.DistinctCount, columnStats.NullCount)

	// 测试直方图
	if columnStats.Histogram == nil {
		t.Fatal("应该生成直方图")
	}

	if len(columnStats.Histogram.Buckets) == 0 {
		t.Fatal("直方图应该有桶")
	}

	t.Logf("直方图 - 桶数量: %d, 总数据量: %d",
		columnStats.Histogram.NumBuckets, columnStats.Histogram.TotalCount)

	// 测试索引统计信息收集
	indexStats, err := collector.CollectIndexStatistics(ctx, table, index)
	if err != nil {
		t.Fatalf("收集索引统计信息失败: %v", err)
	}

	if indexStats.Cardinality <= 0 {
		t.Fatal("索引基数应该大于0")
	}

	t.Logf("索引统计信息 - 基数: %d, 选择性: %.4f",
		indexStats.Cardinality, indexStats.Selectivity)

	// 测试统计信息获取
	retrievedTableStats, exists := collector.GetTableStatistics(table.Name)
	if !exists {
		t.Fatal("应该能够获取已收集的表统计信息")
	}

	if retrievedTableStats.RowCount != tableStats.RowCount {
		t.Fatal("获取的表统计信息应该与收集的一致")
	}

	t.Logf("统计信息缓存工作正常")
}

// TestCostEstimator 测试代价估算器
func TestCostEstimator(t *testing.T) {
	// 创建统计信息收集器
	collector := NewStatisticsCollector(nil)
	defer collector.Stop()

	// 创建代价估算器
	estimator := NewCostEstimator(collector, nil)

	// 创建测试表
	table := createTestTable()
	index := table.Indices[0]

	ctx := context.Background()

	// 收集统计信息
	_, err := collector.CollectTableStatistics(ctx, table)
	if err != nil {
		t.Fatalf("收集表统计信息失败: %v", err)
	}

	_, err = collector.CollectIndexStatistics(ctx, table, index)
	if err != nil {
		t.Fatalf("收集索引统计信息失败: %v", err)
	}

	// 测试表扫描代价估算
	tableScanCost, err := estimator.EstimateTableScanCost(table, 1.0)
	if err != nil {
		t.Fatalf("估算表扫描代价失败: %v", err)
	}

	if tableScanCost.TotalCost <= 0 {
		t.Fatal("表扫描代价应该大于0")
	}

	t.Logf("表扫描代价 - I/O: %.2f, CPU: %.2f, 总计: %.2f",
		tableScanCost.IOCost, tableScanCost.CPUCost, tableScanCost.TotalCost)

	// 测试索引扫描代价估算
	conditions := []*IndexCondition{
		{
			Column:      "id",
			Operator:    "=",
			Value:       int64(12345),
			CanPush:     true,
			Selectivity: 0.00001, // 1/100000
		},
	}

	indexScanCost, err := estimator.EstimateIndexScanCost(table, index, 0.00001, conditions)
	if err != nil {
		t.Fatalf("估算索引扫描代价失败: %v", err)
	}

	if indexScanCost.TotalCost <= 0 {
		t.Fatal("索引扫描代价应该大于0")
	}

	t.Logf("索引扫描代价 - I/O: %.2f, CPU: %.2f, 总计: %.2f",
		indexScanCost.IOCost, indexScanCost.CPUCost, indexScanCost.TotalCost)

	// 验证索引扫描通常比全表扫描更便宜
	if indexScanCost.TotalCost >= tableScanCost.TotalCost {
		t.Logf("警告: 索引扫描代价(%.2f) >= 表扫描代价(%.2f)",
			indexScanCost.TotalCost, tableScanCost.TotalCost)
	} else {
		t.Logf("索引扫描比表扫描更便宜 (%.2f vs %.2f)",
			indexScanCost.TotalCost, tableScanCost.TotalCost)
	}

	// 测试聚合代价估算
	aggCost, err := estimator.EstimateAggregationCost(10000, []string{"name"}, []string{"COUNT", "SUM"})
	if err != nil {
		t.Fatalf("估算聚合代价失败: %v", err)
	}

	if aggCost.TotalCost <= 0 {
		t.Fatal("聚合代价应该大于0")
	}

	t.Logf("聚合代价 - CPU: %.2f, 总计: %.2f, 输出行数: %d",
		aggCost.CPUCost, aggCost.TotalCost, aggCost.OutputRows)

	// 测试排序代价估算
	sortCost, err := estimator.EstimateSortCost(10000, []string{"name"})
	if err != nil {
		t.Fatalf("估算排序代价失败: %v", err)
	}

	if sortCost.TotalCost <= 0 {
		t.Fatal("排序代价应该大于0")
	}

	t.Logf("排序代价 - CPU: %.2f, 总计: %.2f",
		sortCost.CPUCost, sortCost.TotalCost)
}

// TestIntegratedP0Features 测试P0功能集成
func TestIntegratedP0Features(t *testing.T) {
	// 创建完整的查询优化环境
	collector := NewStatisticsCollector(nil)
	defer collector.Stop()

	estimator := NewCostEstimator(collector, nil)
	optimizer := NewIndexPushdownOptimizer()

	// 创建测试表
	table := createTestTable()
	ctx := context.Background()

	// 1. 收集统计信息
	tableStats, err := collector.CollectTableStatistics(ctx, table)
	if err != nil {
		t.Fatalf("收集表统计信息失败: %v", err)
	}

	columnStats := make(map[string]*ColumnStats)
	indexStats := make(map[string]*IndexStats)

	for _, col := range table.Columns {
		colStats, err := collector.CollectColumnStatistics(ctx, table, col)
		if err != nil {
			t.Fatalf("收集列统计信息失败: %v", err)
		}
		key := fmt.Sprintf("%s.%s", table.Name, col.Name)
		columnStats[key] = colStats
	}

	for _, idx := range table.Indices {
		idxStats, err := collector.CollectIndexStatistics(ctx, table, idx)
		if err != nil {
			t.Fatalf("收集索引统计信息失败: %v", err)
		}
		key := fmt.Sprintf("%s.%s", table.Name, idx.Name)
		indexStats[key] = idxStats
	}

	// 2. 设置统计信息到优化器
	tableStatsMap := map[string]*TableStats{table.Name: tableStats}
	optimizer.SetStatistics(tableStatsMap, indexStats, columnStats)

	// 3. 创建查询条件
	whereConditions := []Expression{
		&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "id"},
			Right: &Constant{Value: int64(12345)},
		},
	}

	// 4. 索引下推优化
	candidate, err := optimizer.OptimizeIndexAccess(table, whereConditions, []string{"id", "name"})
	if err != nil {
		t.Fatalf("索引优化失败: %v", err)
	}

	// 5. 代价估算
	if candidate != nil {
		indexCost, err := estimator.EstimateIndexScanCost(
			table, candidate.Index, candidate.Selectivity, candidate.Conditions)
		if err != nil {
			t.Fatalf("估算索引代价失败: %v", err)
		}

		t.Logf("集成测试结果:")
		t.Logf("   选择索引: %s", candidate.Index.Name)
		t.Logf("   选择性: %.6f", candidate.Selectivity)
		t.Logf("   估算代价: %.2f", indexCost.TotalCost)
		t.Logf("   输出行数: %d", indexCost.OutputRows)

		// 验证结果合理性
		if indexCost.OutputRows > tableStats.RowCount {
			t.Fatal("输出行数不应该超过表总行数")
		}

		if candidate.Selectivity < 0 || candidate.Selectivity > 1 {
			t.Fatal("选择性应该在0-1之间")
		}

		t.Logf("P0功能集成测试通过")
	} else {
		t.Logf("未找到合适的索引，将使用全表扫描")

		tableCost, err := estimator.EstimateTableScanCost(table, 1.0)
		if err != nil {
			t.Fatalf("估算表扫描代价失败: %v", err)
		}

		t.Logf("   全表扫描代价: %.2f", tableCost.TotalCost)
	}
}

// createTestTable 创建测试表
func createTestTable() *metadata.Table {
	table := &metadata.Table{
		Name: "users",
		Columns: []*metadata.Column{
			{
				Name:          "id",
				DataType:      metadata.TypeBigInt,
				CharMaxLength: 0,
				IsNullable:    false,
			},
			{
				Name:          "name",
				DataType:      metadata.TypeVarchar,
				CharMaxLength: 100,
				IsNullable:    true,
			},
			{
				Name:          "email",
				DataType:      metadata.TypeVarchar,
				CharMaxLength: 255,
				IsNullable:    true,
			},
			{
				Name:          "created_at",
				DataType:      metadata.TypeDateTime,
				CharMaxLength: 0,
				IsNullable:    false,
			},
		},
		Indices: []*metadata.Index{
			{
				Name:      "PRIMARY",
				Columns:   []string{"id"},
				IsUnique:  true,
				IsPrimary: true,
			},
			{
				Name:      "idx_name",
				Columns:   []string{"name"},
				IsUnique:  false,
				IsPrimary: false,
			},
			{
				Name:      "idx_email",
				Columns:   []string{"email"},
				IsUnique:  true,
				IsPrimary: false,
			},
		},
	}

	return table
}
