package plan

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// SelectivityEstimator 选择率估算器
// 实现OPT-017：根据WHERE条件和统计信息估算查询选择率
type SelectivityEstimator struct {
	// 统计信息收集器
	statsCollector *StatisticsCollector
	// 增强版统计信息收集器
	enhancedCollector *EnhancedStatisticsCollector
	// 配置参数
	config *SelectivityEstimatorConfig
	// 缓存
	cache *SelectivityCache
}

// SelectivityEstimatorConfig 选择率估算器配置
type SelectivityEstimatorConfig struct {
	// 相关性修正系数 (0.0-1.0)
	CorrelationFactor float64
	// 默认选择率（无统计信息时）
	DefaultSelectivity float64
	// LIKE模式前缀长度阈值
	LikePrefixThreshold int
	// 启用缓存
	EnableCache bool
	// 缓存过期时间
	CacheExpiration time.Duration
}

// DefaultSelectivityEstimatorConfig 默认配置
func DefaultSelectivityEstimatorConfig() *SelectivityEstimatorConfig {
	return &SelectivityEstimatorConfig{
		CorrelationFactor:   0.8,
		DefaultSelectivity:  0.1,
		LikePrefixThreshold: 3,
		EnableCache:         true,
		CacheExpiration:     5 * time.Minute,
	}
}

// SelectivityCache 选择率缓存
type SelectivityCache struct {
	cache      map[string]*CachedSelectivity
	expiration time.Duration
}

// CachedSelectivity 缓存的选择率
type CachedSelectivity struct {
	Selectivity float64
	CachedAt    time.Time
}

// NewSelectivityCache 创建选择率缓存
func NewSelectivityCache(expiration time.Duration) *SelectivityCache {
	return &SelectivityCache{
		cache:      make(map[string]*CachedSelectivity),
		expiration: expiration,
	}
}

// Get 获取缓存的选择率
func (sc *SelectivityCache) Get(key string) (float64, bool) {
	if cached, exists := sc.cache[key]; exists {
		if time.Since(cached.CachedAt) < sc.expiration {
			return cached.Selectivity, true
		}
		// 过期，删除
		delete(sc.cache, key)
	}
	return 0, false
}

// Set 设置缓存
func (sc *SelectivityCache) Set(key string, selectivity float64) {
	sc.cache[key] = &CachedSelectivity{
		Selectivity: selectivity,
		CachedAt:    time.Now(),
	}
}

// NewSelectivityEstimator 创建选择率估算器
func NewSelectivityEstimator(statsCollector *StatisticsCollector, config *SelectivityEstimatorConfig) *SelectivityEstimator {
	if config == nil {
		config = DefaultSelectivityEstimatorConfig()
	}

	var cache *SelectivityCache
	if config.EnableCache {
		cache = NewSelectivityCache(config.CacheExpiration)
	}

	return &SelectivityEstimator{
		statsCollector: statsCollector,
		config:         config,
		cache:          cache,
	}
}

// NewSelectivityEstimatorWithEnhanced 创建带增强收集器的选择率估算器
func NewSelectivityEstimatorWithEnhanced(
	enhancedCollector *EnhancedStatisticsCollector,
	config *SelectivityEstimatorConfig,
) *SelectivityEstimator {
	if config == nil {
		config = DefaultSelectivityEstimatorConfig()
	}

	var cache *SelectivityCache
	if config.EnableCache {
		cache = NewSelectivityCache(config.CacheExpiration)
	}

	return &SelectivityEstimator{
		enhancedCollector: enhancedCollector,
		config:            config,
		cache:             cache,
	}
}

// ============ 主入口方法 ============

// EstimateSelectivity 估算表达式的选择率（主入口）
func (se *SelectivityEstimator) EstimateSelectivity(
	table *metadata.Table,
	expr Expression,
) float64 {
	if expr == nil {
		return 1.0
	}

	// 检查缓存
	cacheKey := se.getCacheKey(table.Name, expr)
	if se.cache != nil {
		if selectivity, found := se.cache.Get(cacheKey); found {
			return selectivity
		}
	}

	// 估算选择率
	selectivity := se.estimateExpressionSelectivity(table, expr)

	// 限制范围 [0.0, 1.0]
	selectivity = math.Max(0.0, math.Min(1.0, selectivity))

	// 缓存结果
	if se.cache != nil {
		se.cache.Set(cacheKey, selectivity)
	}

	return selectivity
}

// estimateExpressionSelectivity 估算表达式选择率
func (se *SelectivityEstimator) estimateExpressionSelectivity(
	table *metadata.Table,
	expr Expression,
) float64 {
	switch e := expr.(type) {
	case *BinaryOperation:
		return se.estimateBinaryOperation(table, e)
	case *NotExpression:
		return se.estimateNotExpression(table, e)
	case *InExpression:
		return se.estimateInExpression(table, e)
	case *LikeExpression:
		return se.estimateLikeExpression(table, e)
	case *IsNullExpression:
		return se.estimateIsNullExpression(table, e)
	case *BetweenExpression:
		return se.estimateBetweenExpression(table, e)
	default:
		// 未知表达式类型，使用默认值
		return se.config.DefaultSelectivity
	}
}

// ============ OPT-017.1: 基础选择率估算 ============

// estimateBinaryOperation 估算二元操作选择率
func (se *SelectivityEstimator) estimateBinaryOperation(
	table *metadata.Table,
	expr *BinaryOperation,
) float64 {
	switch strings.ToUpper(expr.Operator) {
	case "AND":
		return se.CombineAnd([]float64{
			se.estimateExpressionSelectivity(table, expr.Left),
			se.estimateExpressionSelectivity(table, expr.Right),
		})
	case "OR":
		return se.CombineOr([]float64{
			se.estimateExpressionSelectivity(table, expr.Left),
			se.estimateExpressionSelectivity(table, expr.Right),
		})
	case "=":
		return se.estimateEquality(table, expr.Left, expr.Right)
	case "<", "<=":
		return se.estimateLessThan(table, expr.Left, expr.Right, expr.Operator == "<=")
	case ">", ">=":
		return se.estimateGreaterThan(table, expr.Left, expr.Right, expr.Operator == ">=")
	case "!=", "<>":
		// 不等于 = 1 - 等于
		return 1.0 - se.estimateEquality(table, expr.Left, expr.Right)
	default:
		return se.config.DefaultSelectivity
	}
}

// estimateEquality 估算等值谓词选择率 (column = value)
func (se *SelectivityEstimator) estimateEquality(
	table *metadata.Table,
	left Expression,
	right Expression,
) float64 {
	// 提取列和值
	column, value := se.extractColumnAndValue(left, right)
	if column == nil {
		return se.config.DefaultSelectivity
	}

	// 获取列统计信息
	columnStats := se.getColumnStats(table.Name, column.Name)
	if columnStats == nil {
		return se.config.DefaultSelectivity
	}

	// 基于NDV估算
	if columnStats.DistinctCount > 0 {
		selectivity := 1.0 / float64(columnStats.DistinctCount)

		// 如果有直方图，使用直方图精确估算
		if columnStats.Histogram != nil && value != nil {
			histSel := se.estimateEqualityWithHistogram(columnStats, value)
			if histSel > 0 {
				return histSel
			}
		}

		return selectivity
	}

	return se.config.DefaultSelectivity
}

// estimateLessThan 估算小于谓词选择率 (column < value)
func (se *SelectivityEstimator) estimateLessThan(
	table *metadata.Table,
	left Expression,
	right Expression,
	inclusive bool,
) float64 {
	// 提取列和值
	column, value := se.extractColumnAndValue(left, right)
	if column == nil || value == nil {
		return se.config.DefaultSelectivity
	}

	// 获取列统计信息
	columnStats := se.getColumnStats(table.Name, column.Name)
	if columnStats == nil {
		return se.config.DefaultSelectivity
	}

	// 使用直方图估算
	if columnStats.Histogram != nil {
		return se.estimateLessThanWithHistogram(columnStats, value, inclusive)
	}

	// 使用最大最小值估算
	if columnStats.MinValue != nil && columnStats.MaxValue != nil {
		minFloat := toFloat64(columnStats.MinValue)
		maxFloat := toFloat64(columnStats.MaxValue)
		valFloat := toFloat64(value)

		if valFloat <= minFloat {
			return 0.0
		}
		if valFloat >= maxFloat {
			return 1.0
		}

		selectivity := (valFloat - minFloat) / (maxFloat - minFloat)
		if inclusive {
			// <= 比 < 稍微大一点
			selectivity += 1.0 / float64(columnStats.DistinctCount)
		}
		return selectivity
	}

	return se.config.DefaultSelectivity
}

// estimateGreaterThan 估算大于谓词选择率 (column > value)
func (se *SelectivityEstimator) estimateGreaterThan(
	table *metadata.Table,
	left Expression,
	right Expression,
	inclusive bool,
) float64 {
	// 提取列和值
	column, value := se.extractColumnAndValue(left, right)
	if column == nil || value == nil {
		return se.config.DefaultSelectivity
	}

	// 获取列统计信息
	columnStats := se.getColumnStats(table.Name, column.Name)
	if columnStats == nil {
		return se.config.DefaultSelectivity
	}

	// 使用直方图估算
	if columnStats.Histogram != nil {
		return se.estimateGreaterThanWithHistogram(columnStats, value, inclusive)
	}

	// 使用最大最小值估算
	if columnStats.MinValue != nil && columnStats.MaxValue != nil {
		minFloat := toFloat64(columnStats.MinValue)
		maxFloat := toFloat64(columnStats.MaxValue)
		valFloat := toFloat64(value)

		if valFloat >= maxFloat {
			return 0.0
		}
		if valFloat <= minFloat {
			return 1.0
		}

		selectivity := (maxFloat - valFloat) / (maxFloat - minFloat)
		if inclusive {
			// >= 比 > 稍微大一点
			selectivity += 1.0 / float64(columnStats.DistinctCount)
		}
		return selectivity
	}

	return se.config.DefaultSelectivity
}

// estimateInExpression 估算IN表达式选择率 (column IN (v1, v2, ...))
func (se *SelectivityEstimator) estimateInExpression(
	table *metadata.Table,
	expr *InExpression,
) float64 {
	column, ok := expr.Column.(*Column)
	if !ok {
		return se.config.DefaultSelectivity
	}

	// 获取列统计信息
	columnStats := se.getColumnStats(table.Name, column.Name)
	if columnStats == nil {
		return se.config.DefaultSelectivity
	}

	// 基于NDV和值列表大小估算
	valueCount := len(expr.Values)
	if columnStats.DistinctCount > 0 {
		selectivity := float64(valueCount) / float64(columnStats.DistinctCount)
		return math.Min(selectivity, 1.0)
	}

	return se.config.DefaultSelectivity
}

// estimateLikeExpression 估算LIKE表达式选择率 (column LIKE pattern)
func (se *SelectivityEstimator) estimateLikeExpression(
	table *metadata.Table,
	expr *LikeExpression,
) float64 {
	column, ok := expr.Column.(*Column)
	if !ok {
		return se.config.DefaultSelectivity
	}

	pattern := expr.Pattern

	// 前缀匹配 (pattern like 'prefix%')
	if strings.HasSuffix(pattern, "%") && !strings.Contains(pattern[:len(pattern)-1], "%") {
		prefix := strings.TrimSuffix(pattern, "%")
		if len(prefix) >= se.config.LikePrefixThreshold {
			// 前缀足够长，选择率较低
			return se.estimatePrefixMatch(table, column, prefix)
		}
	}

	// 包含通配符，选择率较高
	if strings.Contains(pattern, "%") || strings.Contains(pattern, "_") {
		return se.config.DefaultSelectivity * 2 // 通配符匹配，默认值的2倍
	}

	// 精确匹配
	return se.estimateEquality(table, column, &Constant{Value: pattern})
}

// estimateIsNullExpression 估算IS NULL表达式选择率
func (se *SelectivityEstimator) estimateIsNullExpression(
	table *metadata.Table,
	expr *IsNullExpression,
) float64 {
	column, ok := expr.Column.(*Column)
	if !ok {
		return se.config.DefaultSelectivity
	}

	// 获取列统计信息
	columnStats := se.getColumnStats(table.Name, column.Name)
	if columnStats == nil {
		return se.config.DefaultSelectivity
	}

	// 基于NULL计数估算
	totalCount := columnStats.NotNullCount + columnStats.NullCount
	if totalCount > 0 {
		if expr.IsNull {
			return float64(columnStats.NullCount) / float64(totalCount)
		} else {
			// IS NOT NULL
			return float64(columnStats.NotNullCount) / float64(totalCount)
		}
	}

	return se.config.DefaultSelectivity
}

// estimateBetweenExpression 估算BETWEEN表达式选择率 (column BETWEEN lower AND upper)
func (se *SelectivityEstimator) estimateBetweenExpression(
	table *metadata.Table,
	expr *BetweenExpression,
) float64 {
	column, ok := expr.Column.(*Column)
	if !ok {
		return se.config.DefaultSelectivity
	}

	// 获取列统计信息
	columnStats := se.getColumnStats(table.Name, column.Name)
	if columnStats == nil {
		return se.config.DefaultSelectivity
	}

	// 使用直方图估算
	if columnStats.Histogram != nil {
		return se.estimateBetweenWithHistogram(columnStats, expr.Lower, expr.Upper)
	}

	// 使用最大最小值估算
	if columnStats.MinValue != nil && columnStats.MaxValue != nil {
		minFloat := toFloat64(columnStats.MinValue)
		maxFloat := toFloat64(columnStats.MaxValue)
		lowerFloat := toFloat64(expr.Lower)
		upperFloat := toFloat64(expr.Upper)

		if upperFloat <= minFloat || lowerFloat >= maxFloat {
			return 0.0
		}

		// 计算范围重叠部分
		effectiveLower := math.Max(lowerFloat, minFloat)
		effectiveUpper := math.Min(upperFloat, maxFloat)

		selectivity := (effectiveUpper - effectiveLower) / (maxFloat - minFloat)
		return math.Max(0.0, math.Min(1.0, selectivity))
	}

	return se.config.DefaultSelectivity
}

// estimateNotExpression 估算NOT表达式选择率
func (se *SelectivityEstimator) estimateNotExpression(
	table *metadata.Table,
	expr *NotExpression,
) float64 {
	operandSel := se.estimateExpressionSelectivity(table, expr.Operand)
	return 1.0 - operandSel
}

// ============ OPT-017.3: 多谓词组合规则 ============

// CombineAnd 组合AND谓词选择率
func (se *SelectivityEstimator) CombineAnd(selectivities []float64) float64 {
	if len(selectivities) == 0 {
		return 1.0
	}

	// 独立性假设: P(A AND B) = P(A) × P(B)
	result := 1.0
	for _, sel := range selectivities {
		result *= sel
	}

	// 应用相关性修正
	if len(selectivities) > 1 {
		// α^(n-1) 修正因子
		correctionFactor := math.Pow(se.config.CorrelationFactor, float64(len(selectivities)-1))
		result = result * correctionFactor
	}

	return result
}

// CombineOr 组合OR谓词选择率
func (se *SelectivityEstimator) CombineOr(selectivities []float64) float64 {
	if len(selectivities) == 0 {
		return 0.0
	}

	// 独立性假设: P(A OR B) = P(A) + P(B) - P(A) × P(B)
	result := selectivities[0]
	for i := 1; i < len(selectivities); i++ {
		result = result + selectivities[i] - result*selectivities[i]
	}

	return result
}

// ============ 辅助方法 ============

// extractColumnAndValue 提取列和值
func (se *SelectivityEstimator) extractColumnAndValue(
	left Expression,
	right Expression,
) (*Column, interface{}) {
	// 尝试 left = column, right = value
	if col, ok := left.(*Column); ok {
		if constant, ok := right.(*Constant); ok {
			return col, constant.Value
		}
		return col, nil
	}

	// 尝试 left = value, right = column
	if col, ok := right.(*Column); ok {
		if constant, ok := left.(*Constant); ok {
			return col, constant.Value
		}
		return col, nil
	}

	return nil, nil
}

// getColumnStats 获取列统计信息
func (se *SelectivityEstimator) getColumnStats(tableName, columnName string) *ColumnStats {
	if se.enhancedCollector != nil {
		stats, exists := se.enhancedCollector.columnStats[fmt.Sprintf("%s.%s", tableName, columnName)]
		if exists {
			return stats
		}
	}

	if se.statsCollector != nil {
		stats, exists := se.statsCollector.GetColumnStatistics(tableName, columnName)
		if exists {
			return stats
		}
	}

	return nil
}

// getCacheKey 生成缓存键
func (se *SelectivityEstimator) getCacheKey(tableName string, expr Expression) string {
	return fmt.Sprintf("%s:%v", tableName, expr)
}

// estimatePrefixMatch 估算前缀匹配选择率
func (se *SelectivityEstimator) estimatePrefixMatch(
	table *metadata.Table,
	column *Column,
	prefix string,
) float64 {
	// 简化实现：根据前缀长度估算
	// 前缀越长，选择率越低
	prefixLength := len(prefix)

	// 假设每个字符位置有26种可能（字母）
	distinctPossibilities := math.Pow(26, float64(prefixLength))

	columnStats := se.getColumnStats(table.Name, column.Name)
	if columnStats != nil && columnStats.DistinctCount > 0 {
		selectivity := distinctPossibilities / float64(columnStats.DistinctCount)
		return math.Min(selectivity, 1.0)
	}

	return se.config.DefaultSelectivity
}
