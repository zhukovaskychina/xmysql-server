package plan

import (
	"context"
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// CBOIntegratedOptimizer 基于代价的集成优化器
// 整合OPT-016, OPT-017, OPT-018的所有功能
type CBOIntegratedOptimizer struct {
	// 统计信息收集器（增强版）
	statsCollector *EnhancedStatisticsCollector
	// 选择率估算器
	selectivityEstimator *SelectivityEstimator
	// 连接顺序优化器
	joinOrderOptimizer *JoinOrderOptimizer
	// 成本模型
	costModel *CostModel
	// 存储引擎组件
	spaceManager basic.SpaceManager
	btreeManager basic.BPlusTreeManager
}

// NewCBOIntegratedOptimizer 创建集成优化器
func NewCBOIntegratedOptimizer(
	spaceManager basic.SpaceManager,
	btreeManager basic.BPlusTreeManager,
) *CBOIntegratedOptimizer {
	// 1. 创建统计信息收集器
	statsCollector := NewEnhancedStatisticsCollector(
		nil, // 使用默认配置
		spaceManager,
		btreeManager,
	)

	// 2. 创建成本模型
	costModel := NewDefaultCostModel()

	// 3. 创建选择率估算器
	selectivityEstimator := NewSelectivityEstimatorWithEnhanced(
		statsCollector,
		nil, // 使用默认配置
	)

	// 4. 创建连接顺序优化器
	// 需要包装statsCollector为旧版接口
	legacyStatsCollector := &StatisticsCollector{
		tableStats:  statsCollector.tableStats,
		columnStats: statsCollector.columnStats,
		indexStats:  statsCollector.indexStats,
		config:      statsCollector.config,
	}

	joinOrderOptimizer := NewJoinOrderOptimizer(
		costModel,
		legacyStatsCollector,
		selectivityEstimator,
		nil, // 使用默认配置
	)

	return &CBOIntegratedOptimizer{
		statsCollector:       statsCollector,
		selectivityEstimator: selectivityEstimator,
		joinOrderOptimizer:   joinOrderOptimizer,
		costModel:            costModel,
		spaceManager:         spaceManager,
		btreeManager:         btreeManager,
	}
}

// OptimizedQueryPlan 优化后的查询计划
type OptimizedQueryPlan struct {
	// 逻辑计划
	LogicalPlan LogicalPlan
	// 物理计划
	PhysicalPlan PhysicalPlan
	// 连接树（多表查询）
	JoinTree *JoinNode
	// 估算成本
	EstimatedCost *QueryCost
	// 估算行数
	EstimatedRows int64
	// 使用的统计信息
	Statistics *QueryStatistics
}

// QueryStatistics 查询统计信息
type QueryStatistics struct {
	// 涉及的表统计
	TableStats map[string]*TableStats
	// 涉及的列统计
	ColumnStats map[string]*ColumnStats
	// 涉及的索引统计
	IndexStats map[string]*IndexStats
}

// OptimizeQuery 优化查询（主入口）
func (cbo *CBOIntegratedOptimizer) OptimizeQuery(
	ctx context.Context,
	logicalPlan LogicalPlan,
) (*OptimizedQueryPlan, error) {
	// 1. 收集涉及的表
	tables := cbo.extractTables(logicalPlan)
	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables found in logical plan")
	}

	// 2. 收集统计信息
	queryStats, err := cbo.collectQueryStatistics(ctx, tables)
	if err != nil {
		return nil, fmt.Errorf("failed to collect statistics: %v", err)
	}

	// 3. 应用逻辑优化规则
	optimizedLogical := OptimizeLogicalPlan(logicalPlan)

	// 4. 提取WHERE条件和JOIN条件
	whereConditions, joinConditions := cbo.extractConditions(optimizedLogical)

	// 5. 优化连接顺序（多表查询）
	var joinTree *JoinNode
	if len(tables) > 1 {
		joinTree, err = cbo.joinOrderOptimizer.OptimizeJoinOrder(tables, joinConditions, whereConditions)
		if err != nil {
			return nil, fmt.Errorf("failed to optimize join order: %v", err)
		}
	} else {
		// 单表查询
		joinTree = &JoinNode{
			NodeType:      "TABLE",
			Table:         tables[0],
			EstimatedRows: cbo.getTableRowCount(tables[0]),
		}
		joinTree.EstimatedCost = cbo.estimateTableScanCost(tables[0], whereConditions)
	}

	// 6. 生成物理计划
	physicalPlan := cbo.generatePhysicalPlan(optimizedLogical, joinTree)

	// 7. 构建优化后的查询计划
	result := &OptimizedQueryPlan{
		LogicalPlan:   optimizedLogical,
		PhysicalPlan:  physicalPlan,
		JoinTree:      joinTree,
		EstimatedCost: joinTree.EstimatedCost,
		EstimatedRows: joinTree.EstimatedRows,
		Statistics:    queryStats,
	}

	return result, nil
}

// collectQueryStatistics 收集查询统计信息
func (cbo *CBOIntegratedOptimizer) collectQueryStatistics(
	ctx context.Context,
	tables []*metadata.Table,
) (*QueryStatistics, error) {
	stats := &QueryStatistics{
		TableStats:  make(map[string]*TableStats),
		ColumnStats: make(map[string]*ColumnStats),
		IndexStats:  make(map[string]*IndexStats),
	}

	for _, table := range tables {
		// 收集表统计信息
		tableStats, err := cbo.statsCollector.CollectTableStatistics(ctx, table)
		if err != nil {
			// 降级处理，使用估算值
			continue
		}
		stats.TableStats[table.Name] = tableStats

		// 收集列统计信息
		for _, column := range table.Columns {
			colStats, err := cbo.statsCollector.CollectColumnStatistics(ctx, table, column)
			if err != nil {
				continue
			}
			key := fmt.Sprintf("%s.%s", table.Name, column.Name)
			stats.ColumnStats[key] = colStats
		}

		// 收集索引统计信息
		for _, index := range table.Indices {
			idxStats, err := cbo.statsCollector.CollectIndexStatistics(ctx, table, index)
			if err != nil {
				continue
			}
			key := fmt.Sprintf("%s.%s", table.Name, index.Name)
			stats.IndexStats[key] = idxStats
		}
	}

	return stats, nil
}

// extractTables 提取逻辑计划中的表
func (cbo *CBOIntegratedOptimizer) extractTables(plan LogicalPlan) []*metadata.Table {
	var tables []*metadata.Table

	switch p := plan.(type) {
	case *LogicalTableScan:
		tables = append(tables, p.Table)
	case *LogicalIndexScan:
		tables = append(tables, p.Table)
	case *LogicalJoin:
		tables = append(tables, cbo.extractTables(p.Children()[0])...)
		tables = append(tables, cbo.extractTables(p.Children()[1])...)
	default:
		// 递归提取子节点的表
		for _, child := range plan.Children() {
			tables = append(tables, cbo.extractTables(child)...)
		}
	}

	return tables
}

// extractConditions 提取WHERE和JOIN条件
func (cbo *CBOIntegratedOptimizer) extractConditions(plan LogicalPlan) ([]Expression, []Expression) {
	var whereConditions []Expression
	var joinConditions []Expression

	switch p := plan.(type) {
	case *LogicalSelection:
		whereConditions = append(whereConditions, p.Conditions...)
		// 递归处理子节点
		whereSub, joinSub := cbo.extractConditions(p.Children()[0])
		whereConditions = append(whereConditions, whereSub...)
		joinConditions = append(joinConditions, joinSub...)
	case *LogicalJoin:
		joinConditions = append(joinConditions, p.Conditions...)
		// 递归处理子节点
		whereSub1, joinSub1 := cbo.extractConditions(p.Children()[0])
		whereSub2, joinSub2 := cbo.extractConditions(p.Children()[1])
		whereConditions = append(whereConditions, whereSub1...)
		whereConditions = append(whereConditions, whereSub2...)
		joinConditions = append(joinConditions, joinSub1...)
		joinConditions = append(joinConditions, joinSub2...)
	default:
		// 递归处理其他节点
		for _, child := range plan.Children() {
			whereSub, joinSub := cbo.extractConditions(child)
			whereConditions = append(whereConditions, whereSub...)
			joinConditions = append(joinConditions, joinSub...)
		}
	}

	return whereConditions, joinConditions
}

// generatePhysicalPlan 生成物理计划
func (cbo *CBOIntegratedOptimizer) generatePhysicalPlan(
	logicalPlan LogicalPlan,
	joinTree *JoinNode,
) PhysicalPlan {
	// 简化实现：从逻辑计划转换为物理计划
	switch lp := logicalPlan.(type) {
	case *LogicalTableScan:
		return &PhysicalTableScan{
			BasePhysicalPlan: BasePhysicalPlan{},
			Table:            lp.Table,
		}
	case *LogicalIndexScan:
		// 需要把逻辑计划的Index转换为metadata.Index
		metaIndex := &metadata.Index{
			Name:     lp.Index.Name,
			Columns:  lp.Index.Columns,
			IsUnique: lp.Index.Unique,
		}
		return &PhysicalIndexScan{
			BasePhysicalPlan: BasePhysicalPlan{},
			Table:            lp.Table,
			Index:            metaIndex,
		}
	case *LogicalSelection:
		child := cbo.generatePhysicalPlan(lp.Children()[0], joinTree)
		return &PhysicalSelection{
			BasePhysicalPlan: BasePhysicalPlan{
				children: []PhysicalPlan{child},
			},
			Conditions: lp.Conditions, // 使用所有条件
		}
	case *LogicalJoin:
		// 使用joinTree的信息生成物理连接计划
		left := cbo.generatePhysicalPlan(lp.Children()[0], joinTree.LeftChild)
		right := cbo.generatePhysicalPlan(lp.Children()[1], joinTree.RightChild)

		if joinTree.JoinMethod == "HASH_JOIN" {
			return &PhysicalHashJoin{
				BasePhysicalPlan: BasePhysicalPlan{
					children: []PhysicalPlan{left, right},
				},
				JoinType:   lp.JoinType,
				Conditions: lp.Conditions,
			}
		} else if joinTree.JoinMethod == "MERGE_JOIN" {
			return &PhysicalMergeJoin{
				BasePhysicalPlan: BasePhysicalPlan{
					children: []PhysicalPlan{left, right},
				},
				JoinType:   lp.JoinType,
				Conditions: lp.Conditions,
			}
		} else {
			// 默认使用HashJoin
			return &PhysicalHashJoin{
				BasePhysicalPlan: BasePhysicalPlan{
					children: []PhysicalPlan{left, right},
				},
				JoinType:   lp.JoinType,
				Conditions: lp.Conditions,
			}
		}
	default:
		// 默认处理
		return &PhysicalTableScan{}
	}
}

// getTableRowCount 获取表行数
func (cbo *CBOIntegratedOptimizer) getTableRowCount(table *metadata.Table) int64 {
	cbo.statsCollector.mu.RLock()
	defer cbo.statsCollector.mu.RUnlock()

	if stats, exists := cbo.statsCollector.tableStats[table.Name]; exists {
		return stats.RowCount
	}

	return 1000 // 默认值
}

// estimateTableScanCost 估算表扫描成本
func (cbo *CBOIntegratedOptimizer) estimateTableScanCost(
	table *metadata.Table,
	whereConditions []Expression,
) *QueryCost {
	rowCount := cbo.getTableRowCount(table)

	// 估算选择率
	selectivity := 1.0
	for _, cond := range whereConditions {
		sel := cbo.selectivityEstimator.EstimateSelectivity(table, cond)
		selectivity *= sel
	}

	// 计算页数
	pageSize := int64(100)
	numPages := (rowCount + pageSize - 1) / pageSize

	cost := cbo.costModel.EstimateSeqScanCost(numPages, rowCount)
	cost.Cardinality = int64(float64(rowCount) * selectivity)

	return cost
}

// GetStatisticsCollector 获取统计信息收集器
func (cbo *CBOIntegratedOptimizer) GetStatisticsCollector() *EnhancedStatisticsCollector {
	return cbo.statsCollector
}

// GetSelectivityEstimator 获取选择率估算器
func (cbo *CBOIntegratedOptimizer) GetSelectivityEstimator() *SelectivityEstimator {
	return cbo.selectivityEstimator
}

// GetJoinOrderOptimizer 获取连接顺序优化器
func (cbo *CBOIntegratedOptimizer) GetJoinOrderOptimizer() *JoinOrderOptimizer {
	return cbo.joinOrderOptimizer
}

// Stop 停止优化器
func (cbo *CBOIntegratedOptimizer) Stop() {
	if cbo.statsCollector != nil {
		cbo.statsCollector.Stop()
	}
}
