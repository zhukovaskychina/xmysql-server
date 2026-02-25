package plan

import (
	"math"
)

// ============ OPT-017.2: 直方图辅助估算 ============

// estimateEqualityWithHistogram 使用直方图估算等值选择率
func (se *SelectivityEstimator) estimateEqualityWithHistogram(
	columnStats *ColumnStats,
	value interface{},
) float64 {
	histogram := columnStats.Histogram
	if histogram == nil || len(histogram.Buckets) == 0 {
		return 0
	}

	// 查找值所在的桶
	bucket := se.findBucket(histogram, value)
	if bucket == nil {
		return 0
	}

	// 估算选择率 = 桶内计数 / (总计数 * 桶内不同值)
	if bucket.Distinct > 0 && histogram.TotalCount > 0 {
		bucketSelectivity := float64(bucket.Count) / float64(histogram.TotalCount)
		valueSelectivity := bucketSelectivity / float64(bucket.Distinct)
		return valueSelectivity
	}

	return 0
}

// estimateLessThanWithHistogram 使用直方图估算小于选择率
func (se *SelectivityEstimator) estimateLessThanWithHistogram(
	columnStats *ColumnStats,
	value interface{},
	inclusive bool,
) float64 {
	histogram := columnStats.Histogram
	if histogram == nil || len(histogram.Buckets) == 0 {
		return 0
	}

	totalCount := int64(0)
	valueFloat := toFloat64(value)

	for _, bucket := range histogram.Buckets {
		upperBoundFloat := toFloat64(bucket.UpperBound)
		lowerBoundFloat := toFloat64(bucket.LowerBound)

		if upperBoundFloat < valueFloat {
			// 整个桶都在范围内
			totalCount += bucket.Count
		} else if lowerBoundFloat < valueFloat {
			// 值在桶的中间，估算部分计数
			if upperBoundFloat != lowerBoundFloat {
				ratio := (valueFloat - lowerBoundFloat) / (upperBoundFloat - lowerBoundFloat)
				totalCount += int64(float64(bucket.Count) * ratio)
			} else {
				totalCount += bucket.Count / 2
			}
			break
		} else {
			// 值小于桶的下界，停止
			break
		}
	}

	if inclusive {
		// 加上等于的部分
		equalSel := se.estimateEqualityWithHistogram(columnStats, value)
		lessSel := float64(totalCount) / float64(histogram.TotalCount)
		return lessSel + equalSel
	}

	if histogram.TotalCount > 0 {
		return float64(totalCount) / float64(histogram.TotalCount)
	}

	return 0
}

// estimateGreaterThanWithHistogram 使用直方图估算大于选择率
func (se *SelectivityEstimator) estimateGreaterThanWithHistogram(
	columnStats *ColumnStats,
	value interface{},
	inclusive bool,
) float64 {
	histogram := columnStats.Histogram
	if histogram == nil || len(histogram.Buckets) == 0 {
		return 0
	}

	totalCount := int64(0)
	valueFloat := toFloat64(value)

	for i := len(histogram.Buckets) - 1; i >= 0; i-- {
		bucket := histogram.Buckets[i]
		upperBoundFloat := toFloat64(bucket.UpperBound)
		lowerBoundFloat := toFloat64(bucket.LowerBound)

		if lowerBoundFloat > valueFloat {
			// 整个桶都在范围内
			totalCount += bucket.Count
		} else if upperBoundFloat > valueFloat {
			// 值在桶的中间，估算部分计数
			if upperBoundFloat != lowerBoundFloat {
				ratio := (upperBoundFloat - valueFloat) / (upperBoundFloat - lowerBoundFloat)
				totalCount += int64(float64(bucket.Count) * ratio)
			} else {
				totalCount += bucket.Count / 2
			}
			break
		} else {
			// 值大于桶的上界，停止
			break
		}
	}

	if inclusive {
		// 加上等于的部分
		equalSel := se.estimateEqualityWithHistogram(columnStats, value)
		greaterSel := float64(totalCount) / float64(histogram.TotalCount)
		return greaterSel + equalSel
	}

	if histogram.TotalCount > 0 {
		return float64(totalCount) / float64(histogram.TotalCount)
	}

	return 0
}

// estimateBetweenWithHistogram 使用直方图估算BETWEEN选择率
func (se *SelectivityEstimator) estimateBetweenWithHistogram(
	columnStats *ColumnStats,
	lower interface{},
	upper interface{},
) float64 {
	histogram := columnStats.Histogram
	if histogram == nil || len(histogram.Buckets) == 0 {
		return 0
	}

	lowerFloat := toFloat64(lower)
	upperFloat := toFloat64(upper)

	if lowerFloat > upperFloat {
		return 0
	}

	totalCount := int64(0)

	for _, bucket := range histogram.Buckets {
		bucketLowerFloat := toFloat64(bucket.LowerBound)
		bucketUpperFloat := toFloat64(bucket.UpperBound)

		// 计算桶与范围的交集
		intersectLower := math.Max(lowerFloat, bucketLowerFloat)
		intersectUpper := math.Min(upperFloat, bucketUpperFloat)

		if intersectLower <= intersectUpper {
			// 有交集
			if bucketLowerFloat == bucketUpperFloat {
				// 桶只有一个值
				if lowerFloat <= bucketLowerFloat && bucketLowerFloat <= upperFloat {
					totalCount += bucket.Count
				}
			} else {
				// 计算交集占桶的比例
				bucketRange := bucketUpperFloat - bucketLowerFloat
				intersectRange := intersectUpper - intersectLower
				ratio := intersectRange / bucketRange
				totalCount += int64(float64(bucket.Count) * ratio)
			}
		}
	}

	if histogram.TotalCount > 0 {
		return float64(totalCount) / float64(histogram.TotalCount)
	}

	return 0
}

// findBucket 查找值所在的桶
func (se *SelectivityEstimator) findBucket(histogram *Histogram, value interface{}) *Bucket {
	valueFloat := toFloat64(value)

	for i := range histogram.Buckets {
		bucket := &histogram.Buckets[i]
		lowerFloat := toFloat64(bucket.LowerBound)
		upperFloat := toFloat64(bucket.UpperBound)

		if valueFloat >= lowerFloat && valueFloat <= upperFloat {
			return bucket
		}
	}

	return nil
}

// toFloat64 转换为float64（辅助函数）
func toFloat64(val interface{}) float64 {
	switch v := val.(type) {
	case int:
		return float64(v)
	case int8:
		return float64(v)
	case int16:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint:
		return float64(v)
	case uint8:
		return float64(v)
	case uint16:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	case string:
		// 字符串按字典序转换为数值（简化）
		if len(v) > 0 {
			return float64(v[0])
		}
		return 0
	default:
		return 0
	}
}

// toString 转换为字符串（辅助函数）
func toString(val interface{}) string {
	if val == nil {
		return ""
	}

	switch v := val.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		// 使用fmt包会导致循环依赖，这里简化处理
		return ""
	}
}
