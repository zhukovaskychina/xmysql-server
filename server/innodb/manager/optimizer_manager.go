package manager

import (
	"context"
	"regexp"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"math"
)

// OptimizerManager 查询优化器管理器
type OptimizerManager struct {
	schemaManager metadata.InfoSchemaManager
}

// NewOptimizerManager 创建优化器管理器
func NewOptimizerManager(schemaManager metadata.InfoSchemaManager) *OptimizerManager {
	return &OptimizerManager{
		schemaManager: schemaManager,
	}
}

// PlanType 计划类型
type PlanType int

const (
	PLAN_TYPE_SEQUENTIAL_SCAN PlanType = iota
	PLAN_TYPE_INDEX_SCAN
	PLAN_TYPE_HASH_JOIN
	PLAN_TYPE_NESTED_LOOP_JOIN
	PLAN_TYPE_MERGE_JOIN
	PLAN_TYPE_SORT
	PLAN_TYPE_GROUP_BY
	PLAN_TYPE_AGGREGATE
)

// PlanNode 物理执行计划节点
type PlanNode struct {
	PlanType    PlanType
	Cost        float64
	RowCount    uint64
	Children    []*PlanNode
	TableName   string
	IndexName   string
	Conditions  []string
	JoinType    string
	SortColumns []string
	GroupByCols []string
}

// GeneratePhysicalPlan 生成物理执行计划
func (om *OptimizerManager) GeneratePhysicalPlan(ctx context.Context, tableName string, conditions []string) (*PlanNode, error) {
	stats := om.collectTableStatsFor(ctx, tableName)
	paths := om.generateAccessPaths(tableName, conditions, stats)
	if om != nil && om.schemaManager != nil {
		schemaName, bareTableName := optimizerTableNameParts(tableName)
		if meta, err := om.lookupTableMetadata(ctx, schemaName, bareTableName); err == nil && meta != nil {
			for _, index := range meta.Indices {
				prefixLength := optimizerIndexPrefixLength(index.Columns, conditions)
				if prefixLength == 0 {
					continue
				}
				estimatedRows := estimateIndexRows(stats.RowCount, prefixLength)
				paths = append(paths, AccessPath{
					PlanType: PLAN_TYPE_INDEX_SCAN, Cost: float64(estimatedRows) + 1,
					RowCount: estimatedRows, IndexName: index.Name, Filter: conditions,
				})
			}
		}
	}
	best := om.chooseBestPath(paths)
	plan := om.buildPhysicalPlan(best)
	plan.TableName = tableName
	plan.Conditions = append([]string(nil), conditions...)
	return plan, nil
}

// collectTableStats 收集表统计信息
func (om *OptimizerManager) collectTableStats(tableName string) *TableStats {
	return om.collectTableStatsFor(context.Background(), tableName)
}

func (om *OptimizerManager) collectTableStatsFor(ctx context.Context, tableName string) *TableStats {
	defaultStats := &TableStats{RowCount: 1000, DataSize: 100000, IndexSize: 10000}
	if om == nil || om.schemaManager == nil {
		return defaultStats
	}
	schemaName, bareTableName := optimizerTableNameParts(tableName)
	if schemaName != "" {
		if stats, err := om.schemaManager.GetTableStats(ctx, schemaName, bareTableName); err == nil && stats != nil {
			return optimizerTableStats(stats)
		}
		return defaultStats
	}
	if schemas, err := om.schemaManager.GetAllSchemaNames(ctx); err == nil {
		for _, schema := range schemas {
			if stats, err := om.schemaManager.GetTableStats(ctx, schema, bareTableName); err == nil && stats != nil {
				return optimizerTableStats(stats)
			}
		}
	}
	return defaultStats
}

func (om *OptimizerManager) lookupTableMetadata(ctx context.Context, schemaName, tableName string) (*metadata.TableMeta, error) {
	if schemaName != "" {
		return om.schemaManager.GetTableMetadata(ctx, schemaName, tableName)
	}
	schemas, err := om.schemaManager.GetAllSchemaNames(ctx)
	if err != nil {
		return nil, err
	}
	for _, schema := range schemas {
		if meta, metaErr := om.schemaManager.GetTableMetadata(ctx, schema, tableName); metaErr == nil && meta != nil {
			return meta, nil
		}
	}
	return nil, nil
}

func optimizerTableNameParts(tableName string) (string, string) {
	parts := strings.Split(strings.TrimSpace(tableName), ".")
	if len(parts) == 2 {
		return strings.Trim(parts[0], "` "), strings.Trim(parts[1], "` ")
	}
	return "", strings.Trim(tableName, "` ")
}

func optimizerTableStats(stats *metadata.InfoTableStats) *TableStats {
	rowCount := stats.RowCount
	if rowCount == 0 {
		return &TableStats{RowCount: 1, DataSize: stats.DataSize, IndexSize: stats.IndexSize, ModifyCount: stats.ModifyCount}
	}
	return &TableStats{RowCount: rowCount, DataSize: stats.DataSize, IndexSize: stats.IndexSize, ModifyCount: stats.ModifyCount}
}

func optimizerIndexMatchesConditions(column string, conditions []string) bool {
	column = strings.ToLower(strings.Trim(strings.TrimSpace(column), "`"))
	pattern := regexp.MustCompile(`(?i)(^|[^a-z0-9_$])` + regexp.QuoteMeta(column) + `\s*(?:<=>|=|<=|>=|<>|<|>|\b(?:not\s+)?in\b|\b(?:not\s+)?like\b|\b(?:not\s+)?between\b|\bis\s+(?:not\s+)?null\b)`)
	reversePattern := regexp.MustCompile(`(?i)(^|[^a-z0-9_$])(?:'[^']*'|"[^"]*"|[-+]?[0-9]+(?:\.[0-9]+)?|\bnull\b)\s*(?:<=>|=|<=|>=|<>|<|>)\s*` + regexp.QuoteMeta(column) + `($|[^a-z0-9_$])`)
	tuplePattern := regexp.MustCompile(`(?i)\([^()]*\b` + regexp.QuoteMeta(column) + `\b[^()]*\)\s*(?:<=>|=)\s*\(`)
	for _, condition := range conditions {
		compact := strings.ToLower(strings.ReplaceAll(condition, "`", ""))
		if pattern.MatchString(compact) || reversePattern.MatchString(compact) || tuplePattern.MatchString(compact) {
			return true
		}
	}
	return false
}

func optimizerIndexPrefixLength(columns []string, conditions []string) int {
	prefixLength := 0
	for _, column := range columns {
		if !optimizerIndexMatchesConditions(column, conditions) {
			break
		}
		prefixLength++
	}
	return prefixLength
}

func estimateIndexRows(rowCount uint64, prefixLength int) uint64 {
	if rowCount == 0 {
		return 1
	}
	if prefixLength < 1 {
		return rowCount
	}
	divisor := uint64(1)
	for index := 0; index < prefixLength; index++ {
		if divisor > ^uint64(0)/10 {
			divisor = ^uint64(0)
			break
		}
		divisor *= 10
	}
	estimated := rowCount / divisor
	if estimated == 0 {
		return 1
	}
	return estimated
}

// AccessPath 访问路径
type AccessPath struct {
	PlanType  PlanType
	Cost      float64
	RowCount  uint64
	IndexName string
	Filter    []string
}

// generateAccessPaths 生成访问路径
func (om *OptimizerManager) generateAccessPaths(tableName string, conditions []string, stats *TableStats) []AccessPath {
	var paths []AccessPath

	// 1. 全表扫描路径
	seqScanPath := AccessPath{
		PlanType: PLAN_TYPE_SEQUENTIAL_SCAN,
		Cost:     float64(stats.RowCount) * 100.0, // 假设每行100个单位的代价
		RowCount: stats.RowCount,
	}
	paths = append(paths, seqScanPath)

	return paths
}

// chooseBestPath 选择最优访问路径
func (om *OptimizerManager) chooseBestPath(paths []AccessPath) AccessPath {
	var bestPath AccessPath
	minCost := math.MaxFloat64

	for _, path := range paths {
		if path.Cost < minCost {
			minCost = path.Cost
			bestPath = path
		}
	}

	return bestPath
}

// buildPhysicalPlan 构建物理计划树
func (om *OptimizerManager) buildPhysicalPlan(path AccessPath) *PlanNode {
	return &PlanNode{
		PlanType:  path.PlanType,
		Cost:      path.Cost,
		RowCount:  path.RowCount,
		IndexName: path.IndexName,
	}
}

// JoinCondition 连接条件
type JoinCondition struct {
	LeftTable  string
	RightTable string
	Condition  string
}

// OptimizeJoinOrder 优化连接顺序
func (om *OptimizerManager) OptimizeJoinOrder(tables []string, joins []JoinCondition) []string {
	if len(tables) < 2 || len(joins) == 0 {
		return append([]string(nil), tables...)
	}

	// Build a greedy connected order.  The first input table remains the
	// anchor, while each next table is selected from the join graph so a
	// connected plan is preferred over introducing a Cartesian step.  Join
	// clause order is used as a deterministic tie breaker.
	ordered := make([]string, 0, len(tables))
	used := make([]bool, len(tables))
	ordered = append(ordered, tables[0])
	used[0] = true

	for len(ordered) < len(tables) {
		best := -1
		bestConnected := -1
		bestFirstJoin := len(joins) + 1
		bestDegree := -1

		for candidateIndex := 1; candidateIndex < len(tables); candidateIndex++ {
			if used[candidateIndex] {
				continue
			}
			connected := 0
			firstJoin := len(joins) + 1
			degree := 0
			for joinIndex, join := range joins {
				if !optimizerJoinMentionsTable(join, tables[candidateIndex]) {
					continue
				}
				degree++
				if joinIndex < firstJoin {
					firstJoin = joinIndex
				}
				for _, selected := range ordered {
					if optimizerJoinConnectsTables(join, selected, tables[candidateIndex]) {
						connected++
						break
					}
				}
			}

			if connected > bestConnected ||
				(connected == bestConnected && firstJoin < bestFirstJoin) ||
				(connected == bestConnected && firstJoin == bestFirstJoin && degree > bestDegree) {
				best = candidateIndex
				bestConnected = connected
				bestFirstJoin = firstJoin
				bestDegree = degree
			}
		}

		if best < 0 {
			break
		}
		used[best] = true
		ordered = append(ordered, tables[best])
	}

	return ordered
}

func optimizerJoinMentionsTable(join JoinCondition, table string) bool {
	return strings.EqualFold(join.LeftTable, table) || strings.EqualFold(join.RightTable, table)
}

func optimizerJoinConnectsTables(join JoinCondition, left, right string) bool {
	return (strings.EqualFold(join.LeftTable, left) && strings.EqualFold(join.RightTable, right)) ||
		(strings.EqualFold(join.LeftTable, right) && strings.EqualFold(join.RightTable, left))
}
