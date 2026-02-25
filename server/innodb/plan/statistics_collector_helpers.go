package plan

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// ============ 辅助方法 ============

// getTableSpaceID 获取表空间ID
func (esc *EnhancedStatisticsCollector) getTableSpaceID(table *metadata.Table) uint32 {
	// 简化实现：根据表名生成空间ID
	// 实际应该从表元数据获取
	hash := 0
	for _, c := range table.Name {
		hash = hash*31 + int(c)
	}
	if hash < 0 {
		hash = -hash
	}
	return uint32(hash%10000 + 1) // 生成一个合理的空间ID
}

// ============ 真实行数统计 (OPT-016.1) ============

// getRealRowCount 获取真实行数
func (esc *EnhancedStatisticsCollector) getRealRowCount(space basic.Space) int64 {
	pageCount := space.GetPageCount()
	if pageCount == 0 {
		return 0
	}

	// 根据配置选择统计模式
	if esc.config.SampleRate >= 1.0 {
		// 精确统计模式：遍历所有页面
		return esc.getExactRowCount(space)
	} else {
		// 采样估算模式：基于采样页面估算
		return esc.getSampledRowCount(space, pageCount)
	}
}

// getExactRowCount 获取精确行数（遍历所有页面）
func (esc *EnhancedStatisticsCollector) getExactRowCount(space basic.Space) int64 {
	// 如果有B+树管理器，使用B+树统计
	if esc.btreeManager != nil {
		// TODO: 实现B+树叶子节点遍历统计
		// 这需要访问B+树的叶子节点链表
		// 暂时降级为采样估算
	}

	// 降级为采样估算
	pageCount := space.GetPageCount()
	return esc.getSampledRowCount(space, pageCount)
}

// getSampledRowCount 基于采样获取估算行数
func (esc *EnhancedStatisticsCollector) getSampledRowCount(space basic.Space, totalPages uint32) int64 {
	if totalPages == 0 {
		return 0
	}

	// 确定采样页面数
	samplePages := int(float64(totalPages) * esc.config.SampleRate)
	if samplePages < 1 {
		samplePages = 1
	}
	if samplePages > int(totalPages) {
		samplePages = int(totalPages)
	}

	// 随机采样页面
	sampledPageIDs := esc.selectRandomPages(totalPages, samplePages)

	// 统计采样页面的行数
	totalRowsInSample := int64(0)
	validSampleCount := 0

	for _, pageID := range sampledPageIDs {
		rowCount := esc.countRowsInPage(space, pageID)
		if rowCount > 0 {
			totalRowsInSample += rowCount
			validSampleCount++
		}
	}

	// 基于采样结果估算总行数
	if validSampleCount == 0 {
		// 使用默认估算
		return int64(totalPages) * 100
	}

	avgRowsPerPage := float64(totalRowsInSample) / float64(validSampleCount)
	estimatedRows := int64(avgRowsPerPage * float64(totalPages))

	return estimatedRows
}

// selectRandomPages 随机选择页面
func (esc *EnhancedStatisticsCollector) selectRandomPages(totalPages uint32, sampleCount int) []uint32 {
	if sampleCount >= int(totalPages) {
		// 返回所有页面
		pages := make([]uint32, totalPages)
		for i := uint32(0); i < totalPages; i++ {
			pages[i] = i
		}
		return pages
	}

	// 使用水塘采样算法
	rand.Seed(time.Now().UnixNano())
	selected := make([]uint32, sampleCount)

	// 初始化前sampleCount个页面
	for i := 0; i < sampleCount; i++ {
		selected[i] = uint32(i)
	}

	// 对剩余页面进行采样
	for i := sampleCount; i < int(totalPages); i++ {
		j := rand.Intn(i + 1)
		if j < sampleCount {
			selected[j] = uint32(i)
		}
	}

	return selected
}

// countRowsInPage 统计页面中的行数
func (esc *EnhancedStatisticsCollector) countRowsInPage(space basic.Space, pageID uint32) int64 {
	// 简化实现：假设每页平均100行
	// TODO: 实现真实的页面解析逻辑
	// 这需要：
	// 1. 读取页面数据
	// 2. 解析页面头部
	// 3. 统计记录数

	// 暂时返回估算值
	return 100
}

// getSpaceSize 获取空间大小
func (esc *EnhancedStatisticsCollector) getSpaceSize(space basic.Space) (dataSize int64, indexSize int64) {
	usedSpace := space.GetUsedSpace()

	// 简单估算：70%为数据，30%为索引
	dataSize = int64(float64(usedSpace) * 0.7)
	indexSize = int64(float64(usedSpace) * 0.3)

	return dataSize, indexSize
}

// getFreeSpace 获取空闲空间
func (esc *EnhancedStatisticsCollector) getFreeSpace(space basic.Space) int64 {
	totalSpace := int64(space.GetPageCount()) * 16384 // 16KB per page
	usedSpace := int64(space.GetUsedSpace())
	return totalSpace - usedSpace
}

// getAutoIncrementValue 获取自增值
func (esc *EnhancedStatisticsCollector) getAutoIncrementValue(table *metadata.Table) uint64 {
	// TODO: 从表元数据或系统表获取
	return 0
}

// ============ NDV统计 (OPT-016.2) ============

// sampleColumnData 采样列数据
func (esc *EnhancedStatisticsCollector) sampleColumnData(
	space basic.Space,
	column *metadata.Column,
	tableRowCount int64,
) []interface{} {
	// 确定采样大小
	sampleSize := esc.sampler.GetSampleSize(tableRowCount)

	// 根据配置选择采样策略
	if esc.config.SampleRate >= 1.0 {
		// 精确模式：扫描所有数据
		return esc.scanAllColumnData(space, column, tableRowCount)
	} else {
		// 采样模式：基于页面采样
		return esc.sampleColumnDataFromPages(space, column, sampleSize)
	}
}

// scanAllColumnData 扫描所有列数据（精确模式）
func (esc *EnhancedStatisticsCollector) scanAllColumnData(
	space basic.Space,
	column *metadata.Column,
	tableRowCount int64,
) []interface{} {
	// TODO: 实现真实的全表扫描逻辑
	// 这需要：
	// 1. 遍历所有数据页
	// 2. 解析每个记录
	// 3. 提取列值

	// 暂时降级为采样模式
	sampleSize := esc.sampler.GetSampleSize(tableRowCount)
	return esc.sampleColumnDataFromPages(space, column, sampleSize)
}

// sampleColumnDataFromPages 从页面采样列数据
func (esc *EnhancedStatisticsCollector) sampleColumnDataFromPages(
	space basic.Space,
	column *metadata.Column,
	sampleSize int64,
) []interface{} {
	pageCount := space.GetPageCount()
	if pageCount == 0 {
		return []interface{}{}
	}

	// 确定需要采样的页面数
	samplePages := int(float64(pageCount) * esc.config.SampleRate)
	if samplePages < 1 {
		samplePages = 1
	}

	// 随机选择页面
	sampledPageIDs := esc.selectRandomPages(pageCount, samplePages)

	// 从采样页面中提取列值
	sampleData := make([]interface{}, 0, sampleSize)

	for _, pageID := range sampledPageIDs {
		// 从页面中提取列值
		pageValues := esc.extractColumnValuesFromPage(space, pageID, column)
		sampleData = append(sampleData, pageValues...)

		// 如果已经收集足够的样本，停止
		if int64(len(sampleData)) >= sampleSize {
			break
		}
	}

	// 如果样本不足，使用生成的数据补充
	if int64(len(sampleData)) < sampleSize {
		rand.Seed(time.Now().UnixNano())
		for i := int64(len(sampleData)); i < sampleSize; i++ {
			sampleData = append(sampleData, esc.generateSampleValue(column, i, sampleSize))
		}
	}

	// 限制样本大小
	if int64(len(sampleData)) > sampleSize {
		sampleData = sampleData[:sampleSize]
	}

	return sampleData
}

// extractColumnValuesFromPage 从页面中提取列值
func (esc *EnhancedStatisticsCollector) extractColumnValuesFromPage(
	space basic.Space,
	pageID uint32,
	column *metadata.Column,
) []interface{} {
	// TODO: 实现真实的页面解析逻辑
	// 这需要：
	// 1. 读取页面数据
	// 2. 解析页面格式（InnoDB页面格式）
	// 3. 遍历记录
	// 4. 提取指定列的值

	// 暂时返回模拟数据
	values := make([]interface{}, 0, 100)
	rand.Seed(time.Now().UnixNano() + int64(pageID))

	for i := 0; i < 100; i++ {
		values = append(values, esc.generateSampleValue(column, int64(i), 100))
	}

	return values
}

// generateSampleValue 生成采样值
func (esc *EnhancedStatisticsCollector) generateSampleValue(
	column *metadata.Column,
	index int64,
	totalRows int64,
) interface{} {
	// 5%概率为NULL（如果列允许NULL）
	if column.IsNullable && rand.Float64() < 0.05 {
		return nil
	}

	switch column.DataType {
	case metadata.TypeInt, metadata.TypeBigInt:
		return rand.Int63n(totalRows) + 1
	case metadata.TypeVarchar, metadata.TypeText:
		// 生成随机字符串
		length := rand.Intn(20) + 5
		return esc.randomString(length)
	case metadata.TypeDateTime, metadata.TypeTimestamp:
		// 生成过去一年内的随机时间
		days := rand.Intn(365)
		return time.Now().AddDate(0, 0, -days)
	default:
		return index
	}
}

// randomString 生成随机字符串
func (esc *EnhancedStatisticsCollector) randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// ============ 直方图构建 (OPT-016.3) ============

// determineHistogramType 确定直方图类型
func (esc *EnhancedStatisticsCollector) determineHistogramType(column *metadata.Column) HistogramType {
	switch column.DataType {
	case metadata.TypeInt, metadata.TypeBigInt, metadata.TypeDecimal:
		return HistogramEquiWidth // 数值型使用等宽
	case metadata.TypeVarchar, metadata.TypeText:
		return HistogramEquiDepth // 字符串使用等深
	case metadata.TypeDateTime, metadata.TypeTimestamp:
		return HistogramEquiWidth // 日期时间使用等宽
	default:
		return HistogramFrequency // 其他使用频率直方图
	}
}

// buildEnhancedHistogram 构建增强版直方图
func (esc *EnhancedStatisticsCollector) buildEnhancedHistogram(
	sampleData []interface{},
	column *metadata.Column,
	histType HistogramType,
	totalRowCount int64,
) *Histogram {
	histogram := &Histogram{
		NumBuckets:    esc.config.HistogramBuckets,
		TotalCount:    totalRowCount,
		HistogramType: histType,
		SampleRows:    int64(len(sampleData)),
		Buckets:       make([]Bucket, 0, esc.config.HistogramBuckets),
	}

	// 过滤NULL值
	nonNullData := esc.filterNonNull(sampleData)
	if len(nonNullData) == 0 {
		return histogram
	}

	switch histType {
	case HistogramEquiWidth:
		esc.buildEquiWidthHistogram(histogram, nonNullData, column)
	case HistogramEquiDepth:
		esc.buildEquiDepthHistogram(histogram, nonNullData, column)
	case HistogramFrequency:
		esc.buildFrequencyHistogram(histogram, nonNullData, column)
	}

	// 计算NDV
	hll := NewHyperLogLog(14)
	for _, val := range nonNullData {
		hll.Add(val)
	}
	histogram.NDV = hll.Count()

	return histogram
}

// buildEquiWidthHistogram 构建等宽直方图（数值型）
func (esc *EnhancedStatisticsCollector) buildEquiWidthHistogram(
	histogram *Histogram,
	data []interface{},
	column *metadata.Column,
) {
	if len(data) == 0 {
		return
	}

	// 找到最大最小值
	maxVal, minVal := esc.findMinMax(data)
	if maxVal == nil || minVal == nil {
		return
	}

	// 转换为float64进行计算
	maxFloat := esc.toFloat64(maxVal)
	minFloat := esc.toFloat64(minVal)

	if maxFloat == minFloat {
		// 所有值相同
		bucket := Bucket{
			LowerBound: minVal,
			UpperBound: maxVal,
			Count:      histogram.TotalCount,
			Distinct:   1,
		}
		histogram.Buckets = append(histogram.Buckets, bucket)
		return
	}

	// 计算桶宽度
	bucketWidth := (maxFloat - minFloat) / float64(histogram.NumBuckets)

	// 构建桶
	for i := 0; i < histogram.NumBuckets; i++ {
		lowerBound := minFloat + float64(i)*bucketWidth
		upperBound := minFloat + float64(i+1)*bucketWidth

		if i == histogram.NumBuckets-1 {
			upperBound = maxFloat // 最后一个桶包含最大值
		}

		// 计算桶中的数据量（基于采样）
		count := esc.countInRange(data, lowerBound, upperBound)

		// 推算总体计数
		sampleRatio := float64(len(data)) / float64(histogram.TotalCount)
		totalCount := int64(float64(count) / sampleRatio)

		bucket := Bucket{
			LowerBound: esc.fromFloat64(lowerBound, column.DataType),
			UpperBound: esc.fromFloat64(upperBound, column.DataType),
			Count:      totalCount,
			Distinct:   esc.estimateDistinctInBucket(count),
		}
		histogram.Buckets = append(histogram.Buckets, bucket)
	}
}

// buildEquiDepthHistogram 构建等深直方图（字符串型）
func (esc *EnhancedStatisticsCollector) buildEquiDepthHistogram(
	histogram *Histogram,
	data []interface{},
	column *metadata.Column,
) {
	if len(data) == 0 {
		return
	}

	// 排序数据
	sortedData := esc.sortData(data)

	// 计算每个桶的目标数据量
	targetCount := len(sortedData) / histogram.NumBuckets
	if targetCount == 0 {
		targetCount = 1
	}

	currentBucket := Bucket{
		Count:    0,
		Distinct: 0,
	}
	distinctSet := make(map[string]struct{})

	for i, val := range sortedData {
		if i == 0 {
			currentBucket.LowerBound = val
		}

		currentBucket.Count++
		if strVal, ok := val.(string); ok {
			distinctSet[strVal] = struct{}{}
		}

		// 如果达到目标数量或是最后一个值，结束当前桶
		if currentBucket.Count >= int64(targetCount) || i == len(sortedData)-1 {
			currentBucket.UpperBound = val
			currentBucket.Distinct = int64(len(distinctSet))

			// 推算总体计数
			sampleRatio := float64(len(data)) / float64(histogram.TotalCount)
			currentBucket.Count = int64(float64(currentBucket.Count) / sampleRatio)

			histogram.Buckets = append(histogram.Buckets, currentBucket)

			// 开始新桶
			if i < len(sortedData)-1 {
				currentBucket = Bucket{
					Count:    0,
					Distinct: 0,
				}
				distinctSet = make(map[string]struct{})
			}
		}
	}
}

// buildFrequencyHistogram 构建频率直方图
func (esc *EnhancedStatisticsCollector) buildFrequencyHistogram(
	histogram *Histogram,
	data []interface{},
	column *metadata.Column,
) {
	if len(data) == 0 {
		return
	}

	// 统计频率
	freqMap := make(map[string]int64)
	for _, val := range data {
		key := fmt.Sprintf("%v", val)
		freqMap[key]++
	}

	// 按频率排序
	type freqPair struct {
		value string
		count int64
	}

	freqList := make([]freqPair, 0, len(freqMap))
	for val, count := range freqMap {
		freqList = append(freqList, freqPair{val, count})
	}

	// 排序（频率降序）
	for i := 0; i < len(freqList); i++ {
		for j := i + 1; j < len(freqList); j++ {
			if freqList[j].count > freqList[i].count {
				freqList[i], freqList[j] = freqList[j], freqList[i]
			}
		}
	}

	// 取Top N作为桶
	numBuckets := histogram.NumBuckets
	if len(freqList) < numBuckets {
		numBuckets = len(freqList)
	}

	for i := 0; i < numBuckets; i++ {
		// 推算总体计数
		sampleRatio := float64(len(data)) / float64(histogram.TotalCount)
		totalCount := int64(float64(freqList[i].count) / sampleRatio)

		bucket := Bucket{
			LowerBound: freqList[i].value,
			UpperBound: freqList[i].value,
			Count:      totalCount,
			Distinct:   1,
		}
		histogram.Buckets = append(histogram.Buckets, bucket)
	}
}

// ============ 辅助方法 ============

// filterNonNull 过滤NULL值
func (esc *EnhancedStatisticsCollector) filterNonNull(data []interface{}) []interface{} {
	result := make([]interface{}, 0, len(data))
	for _, val := range data {
		if val != nil {
			result = append(result, val)
		}
	}
	return result
}

// findMinMax 查找最大最小值
func (esc *EnhancedStatisticsCollector) findMinMax(data []interface{}) (max, min interface{}) {
	if len(data) == 0 {
		return nil, nil
	}

	max = data[0]
	min = data[0]

	for _, val := range data {
		if val == nil {
			continue
		}
		if esc.compare(val, max) > 0 {
			max = val
		}
		if esc.compare(val, min) < 0 {
			min = val
		}
	}

	return max, min
}

// compare 比较两个值
func (esc *EnhancedStatisticsCollector) compare(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	aFloat := esc.toFloat64(a)
	bFloat := esc.toFloat64(b)

	if aFloat < bFloat {
		return -1
	} else if aFloat > bFloat {
		return 1
	}
	return 0
}

// toFloat64 转换为float64
func (esc *EnhancedStatisticsCollector) toFloat64(val interface{}) float64 {
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
	case time.Time:
		return float64(v.Unix())
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

// fromFloat64 从float64转换回原类型
func (esc *EnhancedStatisticsCollector) fromFloat64(val float64, dataType metadata.DataType) interface{} {
	switch dataType {
	case metadata.TypeInt:
		return int32(val)
	case metadata.TypeBigInt:
		return int64(val)
	case metadata.TypeDecimal:
		return val
	case metadata.TypeDateTime, metadata.TypeTimestamp:
		return time.Unix(int64(val), 0)
	default:
		return val
	}
}

// countInRange 统计范围内的数据量
func (esc *EnhancedStatisticsCollector) countInRange(data []interface{}, lower, upper float64) int64 {
	count := int64(0)
	for _, val := range data {
		if val == nil {
			continue
		}
		valFloat := esc.toFloat64(val)
		if valFloat >= lower && valFloat <= upper {
			count++
		}
	}
	return count
}

// estimateDistinctInBucket 估算桶中的不同值数量
func (esc *EnhancedStatisticsCollector) estimateDistinctInBucket(count int64) int64 {
	// 简单估算：假设10%的唯一性
	distinct := int64(float64(count) * 0.1)
	if distinct < 1 && count > 0 {
		distinct = 1
	}
	return distinct
}

// sortData 排序数据
func (esc *EnhancedStatisticsCollector) sortData(data []interface{}) []interface{} {
	sorted := make([]interface{}, len(data))
	copy(sorted, data)

	// 简单冒泡排序（对于大数据集应使用更高效的算法）
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if esc.compare(sorted[j], sorted[i]) < 0 {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	return sorted
}

// estimateTreeDepth 估算B+树深度
func (esc *EnhancedStatisticsCollector) estimateTreeDepth(rowCount int64) int {
	if rowCount <= 0 {
		return 0
	}

	// 假设每个节点平均100个键
	avgKeysPerNode := 100.0
	depth := int(math.Ceil(math.Log(float64(rowCount)) / math.Log(avgKeysPerNode)))

	return depth
}

// estimateLeafPages 估算叶子页面数
func (esc *EnhancedStatisticsCollector) estimateLeafPages(rowCount int64) int64 {
	if rowCount <= 0 {
		return 0
	}

	// 假设每页平均100行
	avgRowsPerPage := int64(100)
	return (rowCount + avgRowsPerPage - 1) / avgRowsPerPage
}

// estimateNonLeafPages 估算非叶子页面数
func (esc *EnhancedStatisticsCollector) estimateNonLeafPages(treeDepth int, leafPages int64) int64 {
	if treeDepth <= 1 {
		return 0
	}

	// 假设每个内部节点平均100个子节点
	avgFanout := int64(100)
	nonLeafPages := int64(0)

	for level := 1; level < treeDepth; level++ {
		pagesAtLevel := leafPages / int64(math.Pow(float64(avgFanout), float64(treeDepth-level-1)))
		nonLeafPages += pagesAtLevel
	}

	return nonLeafPages
}

// estimateKeysPerPage 估算每页键数
func (esc *EnhancedStatisticsCollector) estimateKeysPerPage(rowCount, leafPages int64) float64 {
	if leafPages <= 0 {
		return 0
	}
	return float64(rowCount) / float64(leafPages)
}

// histogramTypeToString 直方图类型转字符串
func (esc *EnhancedStatisticsCollector) histogramTypeToString(histType HistogramType) string {
	switch histType {
	case HistogramEquiWidth:
		return "EQUI_WIDTH"
	case HistogramEquiDepth:
		return "EQUI_DEPTH"
	case HistogramFrequency:
		return "FREQUENCY"
	default:
		return "UNKNOWN"
	}
}

// ============ 降级估算方法 ============

// estimateTableStats 估算表统计信息（降级模式）
func (esc *EnhancedStatisticsCollector) estimateTableStats(table *metadata.Table) *TableStats {
	// 简化实现：根据表名生成模拟数据
	hash := 0
	for _, c := range table.Name {
		hash = hash*31 + int(c)
	}
	if hash < 0 {
		hash = -hash
	}

	rowCount := int64(hash%1000000 + 1000) // 1000-1000000行
	avgRowLength := int64(100)
	dataLength := rowCount * avgRowLength
	indexLength := dataLength / 4

	return &TableStats{
		TableName:       table.Name,
		RowCount:        rowCount,
		TotalSize:       dataLength + indexLength,
		LastAnalyzeTime: time.Now().Unix(),
		AvgRowLength:    avgRowLength,
		DataLength:      dataLength,
		IndexLength:     indexLength,
		DataFree:        dataLength / 10,
		SampleSize:      esc.sampler.GetSampleSize(rowCount),
	}
}

// estimateColumnStats 估算列统计信息（降级模式）
func (esc *EnhancedStatisticsCollector) estimateColumnStats(
	table *metadata.Table,
	column *metadata.Column,
	tableRowCount int64,
) *ColumnStats {
	stats := &ColumnStats{
		ColumnName:  column.Name,
		LastUpdated: time.Now().Unix(),
	}

	// 根据数据类型估算统计信息
	switch column.DataType {
	case metadata.TypeInt, metadata.TypeBigInt:
		stats.DistinctCount = int64(math.Min(float64(tableRowCount), 1000000))
		stats.MinValue = int64(1)
		stats.MaxValue = tableRowCount
	case metadata.TypeVarchar, metadata.TypeText:
		stats.DistinctCount = int64(math.Min(float64(tableRowCount)*0.8, 100000))
		stats.MinValue = "a"
		stats.MaxValue = "zzz"
	case metadata.TypeDateTime, metadata.TypeTimestamp:
		stats.DistinctCount = int64(math.Min(float64(tableRowCount)*0.9, 1000000))
		stats.MinValue = time.Now().AddDate(-1, 0, 0)
		stats.MaxValue = time.Now()
	default:
		stats.DistinctCount = int64(math.Min(float64(tableRowCount)*0.5, 10000))
	}

	if !column.IsNullable {
		stats.NullCount = 0
		stats.NotNullCount = tableRowCount
	} else {
		stats.NullCount = tableRowCount / 20 // 5% NULL
		stats.NotNullCount = tableRowCount - stats.NullCount
	}

	return stats
}

// ============ 后台更新任务 ============

// backgroundUpdateWorker 后台更新工作器
func (esc *EnhancedStatisticsCollector) backgroundUpdateWorker() {
	ticker := time.NewTicker(esc.config.AutoUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-esc.stopCh:
			return
		case <-ticker.C:
			esc.performPeriodicUpdate()
		case req := <-esc.updateCh:
			esc.handleUpdateRequest(req)
		}
	}
}

// performPeriodicUpdate 执行周期性更新
func (esc *EnhancedStatisticsCollector) performPeriodicUpdate() {
	esc.mu.RLock()
	now := time.Now()
	expiredCount := 0

	// 检查过期的表统计信息
	for key, stats := range esc.tableStats {
		if now.Sub(time.Unix(stats.LastAnalyzeTime, 0)) > esc.config.ExpirationTime {
			expiredCount++
			esc.mu.RUnlock()
			esc.mu.Lock()
			delete(esc.tableStats, key)
			esc.mu.Unlock()
			esc.mu.RLock()
		}
	}

	esc.mu.RUnlock()
}

// handleUpdateRequest 处理更新请求
func (esc *EnhancedStatisticsCollector) handleUpdateRequest(req *StatisticsUpdateRequest) {
	esc.mu.Lock()
	defer esc.mu.Unlock()

	switch req.UpdateType {
	case UpdateTypeTable:
		delete(esc.tableStats, req.TableName)
	case UpdateTypeColumn:
		key := fmt.Sprintf("%s.%s", req.TableName, req.ColumnName)
		delete(esc.columnStats, key)
	case UpdateTypeIndex:
		key := fmt.Sprintf("%s.%s", req.TableName, req.IndexName)
		delete(esc.indexStats, key)
	case UpdateTypeAll:
		// 清空所有相关统计信息
		for key := range esc.tableStats {
			if key == req.TableName {
				delete(esc.tableStats, key)
			}
		}
		for key := range esc.columnStats {
			if len(key) > len(req.TableName) && key[:len(req.TableName)] == req.TableName {
				delete(esc.columnStats, key)
			}
		}
		for key := range esc.indexStats {
			if len(key) > len(req.TableName) && key[:len(req.TableName)] == req.TableName {
				delete(esc.indexStats, key)
			}
		}
	}
}

// Stop 停止统计信息收集器
func (esc *EnhancedStatisticsCollector) Stop() {
	close(esc.stopCh)
}
