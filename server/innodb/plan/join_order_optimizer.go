package plan

import (
	"fmt"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// JoinOrderOptimizer 连接顺序优化器
// 实现OPT-018：为多表JOIN生成最优连接顺序
type JoinOrderOptimizer struct {
	// 成本模型
	costModel *CostModel
	// 统计信息收集器
	statsCollector *StatisticsCollector
	// 选择率估算器
	selectivityEstimator *SelectivityEstimator
	// 配置参数
	config *JoinOrderOptimizerConfig
}

// JoinOrderOptimizerConfig 连接顺序优化器配置
type JoinOrderOptimizerConfig struct {
	// 最大DP表数
	MaxDPTables int
	// 贪心算法阈值
	GreedyThreshold int
	// 优化超时时间
	OptimizerTimeout time.Duration
	// 内存限制（字节）
	MemoryLimit int64
	// 启用贪心算法
	EnableGreedy bool
	// 启用笛卡尔积剪枝
	EnableCartesianPruning bool
	// 启用成本上界剪枝
	EnableCostPruning bool
}

// DefaultJoinOrderOptimizerConfig 默认配置
func DefaultJoinOrderOptimizerConfig() *JoinOrderOptimizerConfig {
	return &JoinOrderOptimizerConfig{
		MaxDPTables:            8,
		GreedyThreshold:        12,
		OptimizerTimeout:       5 * time.Second,
		MemoryLimit:            256 * 1024 * 1024, // 256MB
		EnableGreedy:           true,
		EnableCartesianPruning: true,
		EnableCostPruning:      true,
	}
}

// NewJoinOrderOptimizer 创建连接顺序优化器
func NewJoinOrderOptimizer(
	costModel *CostModel,
	statsCollector *StatisticsCollector,
	selectivityEstimator *SelectivityEstimator,
	config *JoinOrderOptimizerConfig,
) *JoinOrderOptimizer {
	if config == nil {
		config = DefaultJoinOrderOptimizerConfig()
	}

	return &JoinOrderOptimizer{
		costModel:            costModel,
		statsCollector:       statsCollector,
		selectivityEstimator: selectivityEstimator,
		config:               config,
	}
}

// JoinNode 连接节点（表示连接树的节点）
type JoinNode struct {
	// 节点类型：TABLE（叶子）或 JOIN（内部节点）
	NodeType string
	// 表信息（叶子节点）
	Table *metadata.Table
	// 连接类型：INNER, LEFT, RIGHT, FULL
	JoinType string
	// 连接方法：NESTED_LOOP, HASH_JOIN, MERGE_JOIN
	JoinMethod string
	// 左子树
	LeftChild *JoinNode
	// 右子树
	RightChild *JoinNode
	// 连接条件
	Conditions []Expression
	// 估算成本
	EstimatedCost *QueryCost
	// 估算行数
	EstimatedRows int64
	// 涉及的表集合（位图）
	TableSet uint64
}

// String 返回节点的字符串表示
func (jn *JoinNode) String() string {
	if jn.NodeType == "TABLE" {
		return jn.Table.Name
	}
	return fmt.Sprintf("(%s %s %s)", jn.LeftChild.String(), jn.JoinMethod, jn.RightChild.String())
}

// ============ 主入口方法 ============

// OptimizeJoinOrder 优化连接顺序（主入口）
func (joo *JoinOrderOptimizer) OptimizeJoinOrder(
	tables []*metadata.Table,
	joinConditions []Expression,
	whereConditions []Expression,
) (*JoinNode, error) {
	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables to join")
	}

	if len(tables) == 1 {
		// 单表，无需优化
		return &JoinNode{
			NodeType:      "TABLE",
			Table:         tables[0],
			EstimatedCost: &QueryCost{},
			EstimatedRows: joo.getTableRowCount(tables[0]),
			TableSet:      1,
		}, nil
	}

	// 选择算法
	numTables := len(tables)
	startTime := time.Now()

	var result *JoinNode
	var err error

	if numTables <= joo.config.MaxDPTables {
		// 使用动态规划
		result, err = joo.optimizeWithDP(tables, joinConditions, whereConditions, startTime)
	} else if numTables <= joo.config.GreedyThreshold && joo.config.EnableGreedy {
		// 使用贪心算法
		result, err = joo.optimizeWithGreedy(tables, joinConditions, whereConditions, startTime)
	} else {
		// 使用启发式算法
		result, err = joo.optimizeWithHeuristic(tables, joinConditions, whereConditions, startTime)
	}

	return result, err
}

// ============ OPT-018.1: 动态规划算法 ============

// optimizeWithDP 使用动态规划优化连接顺序
func (joo *JoinOrderOptimizer) optimizeWithDP(
	tables []*metadata.Table,
	joinConditions []Expression,
	whereConditions []Expression,
	startTime time.Time,
) (*JoinNode, error) {
	numTables := len(tables)
	if numTables > 20 {
		return nil, fmt.Errorf("too many tables for DP algorithm: %d", numTables)
	}

	// DP状态空间: dp[S] = 连接表集合S的最优计划
	// 使用位图表示表集合
	maxState := 1 << numTables
	dp := make([]*JoinNode, maxState)

	// 初始化：单表状态
	for i := 0; i < numTables; i++ {
		tableSet := uint64(1 << i)
		dp[tableSet] = &JoinNode{
			NodeType:      "TABLE",
			Table:         tables[i],
			EstimatedRows: joo.getTableRowCount(tables[i]),
			TableSet:      tableSet,
		}
		// 估算表扫描成本
		dp[tableSet].EstimatedCost = joo.estimateTableScanCost(tables[i], whereConditions)
	}

	// 动态规划：逐步扩展表集合
	for size := 2; size <= numTables; size++ {
		// 枚举大小为size的表集合
		joo.enumerateSubsets(numTables, size, func(subset uint64) {
			// 检查超时
			if time.Since(startTime) > joo.config.OptimizerTimeout {
				return
			}

			// 枚举子集的划分：subset = left ∪ right
			bestPlan := joo.findBestPartition(subset, dp, joinConditions, tables, numTables)
			if bestPlan != nil {
				dp[subset] = bestPlan
			}
		})
	}

	// 返回全集的最优计划
	fullSet := uint64((1 << numTables) - 1)
	if dp[fullSet] == nil {
		return nil, fmt.Errorf("failed to find optimal join order")
	}

	return dp[fullSet], nil
}

// findBestPartition 查找最优划分
func (joo *JoinOrderOptimizer) findBestPartition(
	subset uint64,
	dp []*JoinNode,
	joinConditions []Expression,
	tables []*metadata.Table,
	numTables int,
) *JoinNode {
	var bestPlan *JoinNode
	var bestCost *QueryCost

	// 枚举subset的所有非空真子集作为左侧
	joo.enumerateProperSubsets(subset, func(left uint64) {
		right := subset ^ left // 补集

		// 检查左右子集的计划是否存在
		if dp[left] == nil || dp[right] == nil {
			return
		}

		// 检查是否有连接条件（笛卡尔积剪枝）
		if joo.config.EnableCartesianPruning {
			if !joo.hasJoinCondition(left, right, joinConditions, numTables) {
				// 无连接条件，跳过（笛卡尔积）
				return
			}
		}

		// 提取连接条件
		conditions := joo.extractJoinConditions(left, right, joinConditions, numTables)

		// 尝试不同的连接方法
		joinMethods := []string{"NESTED_LOOP", "HASH_JOIN", "MERGE_JOIN"}

		for _, method := range joinMethods {
			// 估算连接成本
			joinNode := &JoinNode{
				NodeType:   "JOIN",
				JoinType:   "INNER",
				JoinMethod: method,
				LeftChild:  dp[left],
				RightChild: dp[right],
				Conditions: conditions,
				TableSet:   subset,
			}

			// 计算连接选择率
			selectivity := joo.estimateJoinSelectivity(conditions, tables)

			// 估算连接成本
			joinNode.EstimatedCost = joo.costModel.EstimateJoinCost(
				dp[left].EstimatedCost,
				dp[right].EstimatedCost,
				method,
				selectivity,
			)
			joinNode.EstimatedRows = int64(float64(dp[left].EstimatedRows*dp[right].EstimatedRows) * selectivity)

			// 更新最优计划
			if bestPlan == nil || joinNode.EstimatedCost.CompareTo(bestCost) < 0 {
				bestPlan = joinNode
				bestCost = joinNode.EstimatedCost
			}
		}
	})

	return bestPlan
}

// enumerateSubsets 枚举大小为size的子集
func (joo *JoinOrderOptimizer) enumerateSubsets(n, size int, callback func(uint64)) {
	// 使用递归生成组合
	var generate func(int, int, uint64)
	generate = func(start, remaining int, current uint64) {
		if remaining == 0 {
			callback(current)
			return
		}
		for i := start; i < n; i++ {
			generate(i+1, remaining-1, current|(1<<i))
		}
	}
	generate(0, size, 0)
}

// enumerateProperSubsets 枚举真子集
func (joo *JoinOrderOptimizer) enumerateProperSubsets(subset uint64, callback func(uint64)) {
	// 枚举subset的所有非空真子集
	// 使用位运算技巧
	for left := (subset - 1) & subset; left > 0; left = (left - 1) & subset {
		callback(left)
	}
}

// ============ OPT-018.2: 贪心算法 ============

// optimizeWithGreedy 使用贪心算法优化连接顺序
func (joo *JoinOrderOptimizer) optimizeWithGreedy(
	tables []*metadata.Table,
	joinConditions []Expression,
	whereConditions []Expression,
	startTime time.Time,
) (*JoinNode, error) {
	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables to join")
	}

	// 初始化：选择基数最小的表作为起点
	remaining := make([]*metadata.Table, len(tables))
	copy(remaining, tables)

	// 找到基数最小的表
	minIdx := 0
	minCard := joo.getTableRowCount(remaining[0])
	for i := 1; i < len(remaining); i++ {
		card := joo.getTableRowCount(remaining[i])
		if card < minCard {
			minCard = card
			minIdx = i
		}
	}

	// 构建初始节点
	result := &JoinNode{
		NodeType:      "TABLE",
		Table:         remaining[minIdx],
		EstimatedRows: minCard,
		TableSet:      1 << minIdx,
	}
	result.EstimatedCost = joo.estimateTableScanCost(remaining[minIdx], whereConditions)

	// 从remaining中移除
	remaining = append(remaining[:minIdx], remaining[minIdx+1:]...)

	// 贪心选择：每次选择成本最低的表加入
	for len(remaining) > 0 {
		// 检查超时
		if time.Since(startTime) > joo.config.OptimizerTimeout {
			break
		}

		bestIdx := -1
		var bestJoinNode *JoinNode
		var bestCost *QueryCost

		// 尝试每个剩余的表
		for i, table := range remaining {
			// 检查是否有连接条件
			hasJoin := false
			var conditions []Expression

			for _, cond := range joinConditions {
				if joo.involvesTable(cond, table) && joo.involvesJoinedTables(cond, result) {
					hasJoin = true
					conditions = append(conditions, cond)
				}
			}

			// 笛卡尔积剪枝
			if joo.config.EnableCartesianPruning && !hasJoin {
				continue
			}

			// 尝试不同的连接方法
			joinMethods := []string{"NESTED_LOOP", "HASH_JOIN"}

			for _, method := range joinMethods {
				tableNode := &JoinNode{
					NodeType:      "TABLE",
					Table:         table,
					EstimatedRows: joo.getTableRowCount(table),
				}
				tableNode.EstimatedCost = joo.estimateTableScanCost(table, whereConditions)

				joinNode := &JoinNode{
					NodeType:   "JOIN",
					JoinType:   "INNER",
					JoinMethod: method,
					LeftChild:  result,
					RightChild: tableNode,
					Conditions: conditions,
				}

				// 估算成本
				selectivity := joo.estimateJoinSelectivity(conditions, tables)
				joinNode.EstimatedCost = joo.costModel.EstimateJoinCost(
					result.EstimatedCost,
					tableNode.EstimatedCost,
					method,
					selectivity,
				)
				joinNode.EstimatedRows = int64(float64(result.EstimatedRows*tableNode.EstimatedRows) * selectivity)

				// 更新最优选择
				if bestIdx == -1 || joinNode.EstimatedCost.CompareTo(bestCost) < 0 {
					bestIdx = i
					bestJoinNode = joinNode
					bestCost = joinNode.EstimatedCost
				}
			}
		}

		if bestIdx == -1 {
			// 没有找到合适的表，可能是笛卡尔积
			// 强制加入第一个表
			table := remaining[0]
			tableNode := &JoinNode{
				NodeType:      "TABLE",
				Table:         table,
				EstimatedRows: joo.getTableRowCount(table),
			}
			tableNode.EstimatedCost = joo.estimateTableScanCost(table, whereConditions)

			result = &JoinNode{
				NodeType:   "JOIN",
				JoinType:   "INNER",
				JoinMethod: "NESTED_LOOP",
				LeftChild:  result,
				RightChild: tableNode,
				Conditions: []Expression{},
			}
			result.EstimatedCost = joo.costModel.EstimateJoinCost(
				result.LeftChild.EstimatedCost,
				tableNode.EstimatedCost,
				"NESTED_LOOP",
				1.0,
			)
			result.EstimatedRows = result.LeftChild.EstimatedRows * tableNode.EstimatedRows

			remaining = remaining[1:]
		} else {
			// 加入最优表
			result = bestJoinNode
			remaining = append(remaining[:bestIdx], remaining[bestIdx+1:]...)
		}
	}

	return result, nil
}

// ============ OPT-018.3: 启发式算法 ============

// optimizeWithHeuristic 使用启发式算法优化连接顺序
func (joo *JoinOrderOptimizer) optimizeWithHeuristic(
	tables []*metadata.Table,
	joinConditions []Expression,
	whereConditions []Expression,
	startTime time.Time,
) (*JoinNode, error) {
	// 启发式规则：
	// 1. 优先连接有索引的表
	// 2. 优先连接选择率高的过滤条件
	// 3. 小表驱动大表

	// 简化实现：使用贪心算法的变体
	return joo.optimizeWithGreedy(tables, joinConditions, whereConditions, startTime)
}
