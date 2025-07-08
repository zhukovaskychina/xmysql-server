package plan

import (
	"fmt"
	_ "math"
	"sort"
	"strings"
	"time"
)

// Statistics 统计信息
type Statistics struct {
	// 表级统计信息
	TableStats *TableStats
	// 列统计信息
	ColumnStats map[string]*ColumnStats
	// 索引统计信息
	IndexStats map[string]*IndexStats
}

// TableStats 表统计信息
type TableStats struct {
	// 表名
	TableName string
	// 总行数
	RowCount int64
	// 总大小(字节)
	TotalSize int64
	// 修改计数
	ModifyCount int64
	// 上次分析时间
	LastAnalyzeTime int64
}

// ColumnStats 列统计信息
type ColumnStats struct {
	// 列名
	ColumnName string
	// 非空值数量
	NotNullCount int64
	// 不同值数量
	DistinctCount int64
	// 空值数量
	NullCount int64
	// 最大值
	MaxValue interface{}
	// 最小值
	MinValue interface{}
	// 直方图
	Histogram *Histogram
	// 常用值及其频率
	TopN []ValueFreq
}

// IndexStats 索引统计信息
type IndexStats struct {
	// 索引名
	IndexName string
	// 基数
	Cardinality int64
	// 聚簇因子
	ClusterFactor float64
	// 前缀长度
	PrefixLength int
	// 选择性
	Selectivity float64
}

// Histogram 直方图
type Histogram struct {
	// 桶数量
	NumBuckets int
	// 桶
	Buckets []Bucket
	// 总数据量
	TotalCount int64
	// NDV (Number of Distinct Values)
	NDV int64
}

// Bucket 直方图桶
type Bucket struct {
	// 下界
	LowerBound interface{}
	// 上界
	UpperBound interface{}
	// 重复次数
	Count int64
	// 不同值数量
	Distinct int64
}

// ValueFreq 值频率对
type ValueFreq struct {
	Value interface{}
	Freq  int64
}

// StatsBuilder 统计信息构建器
type StatsBuilder struct {
	sampleRate float64
	maxSamples int64
}

// BuildTableStats 构建表统计信息
func (b *StatsBuilder) BuildTableStats(tableName string, rows [][]interface{}) *TableStats {
	stats := &TableStats{
		TableName:       tableName,
		RowCount:        int64(len(rows)),
		LastAnalyzeTime: getCurrentTime(),
	}

	// 计算总大小
	for _, row := range rows {
		stats.TotalSize += calculateRowSize(row)
	}

	return stats
}

// BuildColumnStats 构建列统计信息
func (b *StatsBuilder) BuildColumnStats(columnName string, values []interface{}) *ColumnStats {
	stats := &ColumnStats{
		ColumnName: columnName,
	}

	// 计算基本统计量
	for _, v := range values {
		if v == nil {
			stats.NullCount++
		} else {
			stats.NotNullCount++
		}
	}

	// 计算不同值数量
	distinct := make(map[interface{}]int64)
	for _, v := range values {
		if v != nil {
			distinct[v]++
		}
	}
	stats.DistinctCount = int64(len(distinct))

	// 构建TopN
	stats.TopN = buildTopN(distinct, 10)

	// 构建直方图
	stats.Histogram = buildHistogram(values, 100)

	// 计算最大最小值
	stats.MaxValue, stats.MinValue = findMinMax(values)

	return stats
}

// BuildIndexStats 构建索引统计信息
func (b *StatsBuilder) BuildIndexStats(indexName string, keys [][]interface{}) *IndexStats {
	stats := &IndexStats{
		IndexName: indexName,
	}

	// 计算基数
	distinct := make(map[string]struct{})
	for _, key := range keys {
		distinct[buildIndexKey(key)] = struct{}{}
	}
	stats.Cardinality = int64(len(distinct))

	// 计算选择性
	stats.Selectivity = float64(stats.Cardinality) / float64(len(keys))

	// 计算聚簇因子
	stats.ClusterFactor = calculateClusterFactor(keys)

	return stats
}

// 辅助函数

func getCurrentTime() int64 {
	return time.Now().Unix()
}

func calculateRowSize(row []interface{}) int64 {
	size := int64(0)
	for _, v := range row {
		size += calculateValueSize(v)
	}
	return size
}

func calculateValueSize(v interface{}) int64 {
	switch val := v.(type) {
	case nil:
		return 0
	case int8, uint8, bool:
		return 1
	case int16, uint16:
		return 2
	case int32, uint32, float32:
		return 4
	case int, int64, uint64, float64:
		return 8
	case string:
		return int64(len(val))
	case []byte:
		return int64(len(val))
	case time.Time:
		return 8
	default:
		return 8
	}
}

func buildTopN(freq map[interface{}]int64, n int) []ValueFreq {
	var pairs []ValueFreq
	for v, f := range freq {
		pairs = append(pairs, ValueFreq{v, f})
	}

	// 按频率排序
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Freq > pairs[j].Freq
	})

	if len(pairs) > n {
		pairs = pairs[:n]
	}
	return pairs
}

func buildHistogram(values []interface{}, numBuckets int) *Histogram {
	if len(values) == 0 {
		return nil
	}

	// 排序
	sortedVals := make([]interface{}, len(values))
	copy(sortedVals, values)
	sort.Slice(sortedVals, func(i, j int) bool {
		return less(sortedVals[i], sortedVals[j])
	})

	// 计算桶大小
	bucketSize := float64(len(values)) / float64(numBuckets)

	// 构建桶
	buckets := make([]Bucket, 0, numBuckets)
	currentBucket := Bucket{}
	distinctValues := make(map[interface{}]struct{})

	for i, v := range sortedVals {
		if float64(i) >= bucketSize*float64(len(buckets)+1) {
			// 完成当前桶
			currentBucket.UpperBound = v
			currentBucket.Distinct = int64(len(distinctValues))
			buckets = append(buckets, currentBucket)

			// 开始新桶
			currentBucket = Bucket{
				LowerBound: v,
				Count:      0,
			}
			distinctValues = make(map[interface{}]struct{})
		}

		currentBucket.Count++
		distinctValues[v] = struct{}{}
	}

	// 添加最后一个桶
	if currentBucket.Count > 0 {
		currentBucket.UpperBound = sortedVals[len(sortedVals)-1]
		currentBucket.Distinct = int64(len(distinctValues))
		buckets = append(buckets, currentBucket)
	}

	return &Histogram{
		NumBuckets: len(buckets),
		Buckets:    buckets,
		TotalCount: int64(len(values)),
		NDV:        calculateNDV(values),
	}
}

func findMinMax(values []interface{}) (max, min interface{}) {
	if len(values) == 0 {
		return nil, nil
	}

	max = values[0]
	min = values[0]

	for _, v := range values {
		if v == nil {
			continue
		}
		if max == nil || less(max, v) {
			max = v
		}
		if min == nil || less(v, min) {
			min = v
		}
	}

	return max, min
}

func buildIndexKey(key []interface{}) string {
	parts := make([]string, len(key))
	for i, v := range key {
		parts[i] = fmt.Sprintf("%v", v)
	}
	return strings.Join(parts, "|")
}

func calculateClusterFactor(keys [][]interface{}) float64 {
	if len(keys) == 0 {
		return 0
	}

	distinct := make(map[string]struct{})
	for _, k := range keys {
		distinct[buildIndexKey(k)] = struct{}{}
	}
	if len(distinct) == 0 {
		return 0
	}
	return float64(len(keys)) / float64(len(distinct))
}

func calculateNDV(values []interface{}) int64 {
	distinct := make(map[interface{}]struct{})
	for _, v := range values {
		if v != nil {
			distinct[v] = struct{}{}
		}
	}
	return int64(len(distinct))
}

func less(a, b interface{}) bool {
	switch va := a.(type) {
	case int, int8, int16, int32, int64:
		return toInt64(va) < toInt64(b)
	case uint, uint8, uint16, uint32, uint64:
		return toUint64(va) < toUint64(b)
	case float32, float64:
		return toFloat64(va) < toFloat64(b)
	case string:
		if vb, ok := b.(string); ok {
			return va < vb
		}
	case time.Time:
		if vb, ok := b.(time.Time); ok {
			return va.Before(vb)
		}
	}
	return false
}

func toInt64(v interface{}) int64 {
	switch t := v.(type) {
	case int:
		return int64(t)
	case int8:
		return int64(t)
	case int16:
		return int64(t)
	case int32:
		return int64(t)
	case int64:
		return t
	case uint:
		return int64(t)
	case uint8:
		return int64(t)
	case uint16:
		return int64(t)
	case uint32:
		return int64(t)
	case uint64:
		return int64(t)
	case float32:
		return int64(t)
	case float64:
		return int64(t)
	default:
		return 0
	}
}

func toUint64(v interface{}) uint64 {
	switch t := v.(type) {
	case uint:
		return uint64(t)
	case uint8:
		return uint64(t)
	case uint16:
		return uint64(t)
	case uint32:
		return uint64(t)
	case uint64:
		return t
	case int:
		return uint64(t)
	case int8:
		return uint64(t)
	case int16:
		return uint64(t)
	case int32:
		return uint64(t)
	case int64:
		return uint64(t)
	case float32:
		return uint64(t)
	case float64:
		return uint64(t)
	default:
		return 0
	}
}

func toFloat64(v interface{}) float64 {
	switch t := v.(type) {
	case float32:
		return float64(t)
	case float64:
		return t
	case int:
		return float64(t)
	case int8:
		return float64(t)
	case int16:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case uint:
		return float64(t)
	case uint8:
		return float64(t)
	case uint16:
		return float64(t)
	case uint32:
		return float64(t)
	case uint64:
		return float64(t)
	default:
		return 0
	}
}
