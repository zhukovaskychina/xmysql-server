package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
)

func main() {
	fmt.Println("🚀 XMySQL Server 查询优化器 P0 功能演示")
	fmt.Println(strings.Repeat("=", 60))

	// 创建演示环境
	demo := NewQueryOptimizerDemo()

	// 运行演示
	if err := demo.Run(); err != nil {
		log.Fatalf("演示失败: %v", err)
	}

	fmt.Println(" 演示完成！")
}

// QueryOptimizerDemo 查询优化器演示
type QueryOptimizerDemo struct {
	statsCollector *plan.StatisticsCollector
	costEstimator  *plan.CostEstimator
	indexOptimizer *plan.IndexPushdownOptimizer
	table          *metadata.Table
}

// NewQueryOptimizerDemo 创建演示实例
func NewQueryOptimizerDemo() *QueryOptimizerDemo {
	// 创建统计信息收集器
	statsConfig := &plan.StatisticsConfig{
		AutoUpdateInterval: 1 * time.Hour,
		SampleRate:         0.1,
		HistogramBuckets:   32,
		ExpirationTime:     24 * time.Hour,
		EnableAutoUpdate:   false,
	}

	statsCollector := plan.NewStatisticsCollector(statsConfig)

	// 创建代价估算器
	costEstimator := plan.NewCostEstimator(statsCollector, nil)

	// 创建索引下推优化器
	indexOptimizer := plan.NewIndexPushdownOptimizer()

	// 创建测试表
	table := createDemoTable()

	return &QueryOptimizerDemo{
		statsCollector: statsCollector,
		costEstimator:  costEstimator,
		indexOptimizer: indexOptimizer,
		table:          table,
	}
}

// Run 运行演示
func (demo *QueryOptimizerDemo) Run() error {
	ctx := context.Background()

	fmt.Println(" 步骤1: 收集统计信息")
	if err := demo.collectStatistics(ctx); err != nil {
		return fmt.Errorf("收集统计信息失败: %v", err)
	}
	fmt.Println(" 统计信息收集完成")

	fmt.Println("\n 步骤2: 索引下推优化")
	if err := demo.demonstrateIndexPushdown(); err != nil {
		return fmt.Errorf("索引下推演示失败: %v", err)
	}
	fmt.Println(" 索引下推优化完成")

	fmt.Println("\n 步骤3: 代价估算对比")
	if err := demo.demonstrateCostEstimation(); err != nil {
		return fmt.Errorf("代价估算演示失败: %v", err)
	}
	fmt.Println(" 代价估算对比完成")

	fmt.Println("\n 步骤4: 综合优化演示")
	if err := demo.demonstrateIntegratedOptimization(); err != nil {
		return fmt.Errorf("综合优化演示失败: %v", err)
	}
	fmt.Println(" 综合优化演示完成")

	return nil
}

// collectStatistics 收集统计信息
func (demo *QueryOptimizerDemo) collectStatistics(ctx context.Context) error {
	// 收集表统计信息
	tableStats, err := demo.statsCollector.CollectTableStatistics(ctx, demo.table)
	if err != nil {
		return err
	}

	logger.Debugf("    表统计信息: 行数=%d, 大小=%d bytes\n",
		tableStats.RowCount, tableStats.TotalSize)

	// 收集列统计信息
	columnStats := make(map[string]*plan.ColumnStats)
	for _, col := range demo.table.Columns {
		colStats, err := demo.statsCollector.CollectColumnStatistics(ctx, demo.table, col)
		if err != nil {
			return err
		}
		key := fmt.Sprintf("%s.%s", demo.table.Name, col.Name)
		columnStats[key] = colStats

		logger.Debugf("    列统计信息 %s: 不同值=%d, 空值=%d\n",
			col.Name, colStats.DistinctCount, colStats.NullCount)
	}

	// 收集索引统计信息
	indexStats := make(map[string]*plan.IndexStats)
	for _, idx := range demo.table.Indices {
		idxStats, err := demo.statsCollector.CollectIndexStatistics(ctx, demo.table, idx)
		if err != nil {
			return err
		}
		key := fmt.Sprintf("%s.%s", demo.table.Name, idx.Name)
		indexStats[key] = idxStats

		logger.Debugf("    索引统计信息 %s: 基数=%d, 选择性=%.4f\n",
			idx.Name, idxStats.Cardinality, idxStats.Selectivity)
	}

	// 设置统计信息到优化器
	tableStatsMap := map[string]*plan.TableStats{demo.table.Name: tableStats}
	demo.indexOptimizer.SetStatistics(tableStatsMap, indexStats, columnStats)

	return nil
}

// demonstrateIndexPushdown 演示索引下推优化
func (demo *QueryOptimizerDemo) demonstrateIndexPushdown() error {
	fmt.Println("    测试查询: SELECT * FROM users WHERE id = 12345")

	// 创建WHERE条件
	whereConditions := []plan.Expression{
		&plan.BinaryOperation{
			Op:    plan.OpEQ,
			Left:  &plan.Column{Name: "id"},
			Right: &plan.Constant{Value: int64(12345)},
		},
	}

	// 进行索引优化
	candidate, err := demo.indexOptimizer.OptimizeIndexAccess(
		demo.table, whereConditions, []string{"id", "name", "email"})
	if err != nil {
		return err
	}

	if candidate != nil {
		logger.Debugf("    选择索引: %s\n", candidate.Index.Name)
		logger.Debugf("    索引条件数: %d\n", len(candidate.Conditions))
		logger.Debugf("    覆盖索引: %v\n", candidate.CoverIndex)
		logger.Debugf("    选择性: %.6f\n", candidate.Selectivity)
		logger.Debugf("    优化器评分: %.2f\n", candidate.Cost)
	} else {
		fmt.Println("     未找到合适的索引，将使用全表扫描")
	}

	// 测试复杂查询
	fmt.Println("\n    测试复杂查询: SELECT * FROM users WHERE name LIKE 'John%' AND email = 'john@example.com.xmysql.server'")

	complexConditions := []plan.Expression{
		&plan.Function{
			FuncName: "LIKE",
			FuncArgs: []plan.Expression{
				&plan.Column{Name: "name"},
				&plan.Constant{Value: "John%"},
			},
		},
		&plan.BinaryOperation{
			Op:    plan.OpEQ,
			Left:  &plan.Column{Name: "email"},
			Right: &plan.Constant{Value: "john@example.com.xmysql.server"},
		},
	}

	complexCandidate, err := demo.indexOptimizer.OptimizeIndexAccess(
		demo.table, complexConditions, []string{"id", "name", "email"})
	if err != nil {
		return err
	}

	if complexCandidate != nil {
		logger.Debugf("    选择索引: %s\n", complexCandidate.Index.Name)
		logger.Debugf("    选择性: %.6f\n", complexCandidate.Selectivity)
	} else {
		fmt.Println("     复杂查询未找到合适的索引")
	}

	return nil
}

// demonstrateCostEstimation 演示代价估算
func (demo *QueryOptimizerDemo) demonstrateCostEstimation() error {
	// 全表扫描代价
	tableScanCost, err := demo.costEstimator.EstimateTableScanCost(demo.table, 1.0)
	if err != nil {
		return err
	}

	logger.Debugf("   📈 全表扫描代价: I/O=%.2f, CPU=%.2f, 总计=%.2f\n",
		tableScanCost.IOCost, tableScanCost.CPUCost, tableScanCost.TotalCost)

	// 主键索引扫描代价
	primaryIndex := demo.table.Indices[0] // PRIMARY
	conditions := []*plan.IndexCondition{
		{
			Column:      "id",
			Operator:    "=",
			Value:       int64(12345),
			CanPush:     true,
			Selectivity: 0.00001, // 1/100000
		},
	}

	indexScanCost, err := demo.costEstimator.EstimateIndexScanCost(
		demo.table, primaryIndex, 0.00001, conditions)
	if err != nil {
		return err
	}

	logger.Debugf("   📈 主键索引扫描代价: I/O=%.2f, CPU=%.2f, 总计=%.2f\n",
		indexScanCost.IOCost, indexScanCost.CPUCost, indexScanCost.TotalCost)

	// 代价对比
	ratio := tableScanCost.TotalCost / indexScanCost.TotalCost
	logger.Debugf("    性能提升: 索引扫描比全表扫描快 %.1fx\n", ratio)

	// 聚合查询代价
	aggCost, err := demo.costEstimator.EstimateAggregationCost(
		10000, []string{"name"}, []string{"COUNT", "SUM"})
	if err != nil {
		return err
	}

	logger.Debugf("   📈 聚合查询代价: CPU=%.2f, 总计=%.2f, 输出行数=%d\n",
		aggCost.CPUCost, aggCost.TotalCost, aggCost.OutputRows)

	// 排序代价
	sortCost, err := demo.costEstimator.EstimateSortCost(10000, []string{"name"})
	if err != nil {
		return err
	}

	logger.Debugf("   📈 排序代价: CPU=%.2f, 总计=%.2f\n",
		sortCost.CPUCost, sortCost.TotalCost)

	return nil
}

// demonstrateIntegratedOptimization 演示综合优化
func (demo *QueryOptimizerDemo) demonstrateIntegratedOptimization() error {
	fmt.Println("    综合优化场景: 复杂查询优化")

	// 模拟复杂查询: SELECT id, name FROM users WHERE id > 1000 AND id < 5000 ORDER BY name
	whereConditions := []plan.Expression{
		&plan.BinaryOperation{
			Op:    plan.OpGT,
			Left:  &plan.Column{Name: "id"},
			Right: &plan.Constant{Value: int64(1000)},
		},
		&plan.BinaryOperation{
			Op:    plan.OpLT,
			Left:  &plan.Column{Name: "id"},
			Right: &plan.Constant{Value: int64(5000)},
		},
	}

	// 1. 索引选择
	candidate, err := demo.indexOptimizer.OptimizeIndexAccess(
		demo.table, whereConditions, []string{"id", "name"})
	if err != nil {
		return err
	}

	var scanCost *plan.CostEstimate
	if candidate != nil {
		logger.Debugf("    选择索引扫描: %s (选择性: %.4f)\n",
			candidate.Index.Name, candidate.Selectivity)

		// 2. 索引扫描代价
		scanCost, err = demo.costEstimator.EstimateIndexScanCost(
			demo.table, candidate.Index, candidate.Selectivity, candidate.Conditions)
		if err != nil {
			return err
		}
	} else {
		fmt.Println("     使用全表扫描")

		// 2. 全表扫描代价
		scanCost, err = demo.costEstimator.EstimateTableScanCost(demo.table, 0.04) // 4%选择性
		if err != nil {
			return err
		}
	}

	// 3. 排序代价
	sortCost, err := demo.costEstimator.EstimateSortCost(scanCost.OutputRows, []string{"name"})
	if err != nil {
		return err
	}

	// 4. 总代价
	totalCost := scanCost.TotalCost + sortCost.TotalCost

	logger.Debugf("    扫描代价: %.2f\n", scanCost.TotalCost)
	logger.Debugf("    排序代价: %.2f\n", sortCost.TotalCost)
	logger.Debugf("    总代价: %.2f\n", totalCost)
	logger.Debugf("    预计输出行数: %d\n", scanCost.OutputRows)

	// 5. 优化建议
	fmt.Println("   💡 优化建议:")
	if candidate != nil && candidate.Index.Name == "PRIMARY" {
		fmt.Println("      - 主键范围扫描效率较高")
		if !candidate.CoverIndex {
			fmt.Println("      - 考虑创建覆盖索引 (id, name) 避免回表")
		}
	} else {
		fmt.Println("      - 考虑在 id 列上创建索引")
	}

	if sortCost.TotalCost > scanCost.TotalCost {
		fmt.Println("      - 排序代价较高，考虑创建 (id, name) 复合索引")
	}

	return nil
}

// createDemoTable 创建演示表
func createDemoTable() *metadata.Table {
	return &metadata.Table{
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
}
