package plan

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// ============ 辅助方法 ============

// getTableSpaceID 获取表空间ID
func (esc *EnhancedStatisticsCollector) getTableSpaceID(table *metadata.Table) uint32 {
	if esc != nil && esc.storageAccessor != nil && table != nil {
		if resolver, ok := esc.storageAccessor.(interface {
			GetTableSpaceID(schemaName, tableName string) (uint32, error)
		}); ok {
			schemaName := ""
			if table.Schema != nil {
				schemaName = table.Schema.Name
			}
			if spaceID, err := resolver.GetTableSpaceID(schemaName, table.Name); err == nil && spaceID > 0 {
				return spaceID
			}
		}
	}

	// 简化实现：根据表名生成空间ID
	// 仅用于没有表存储映射能力的测试/兼容 accessor。
	hash := 0
	if table != nil && table.Schema != nil {
		for _, c := range table.Schema.Name {
			hash = hash*31 + int(c)
		}
	}
	hash = hash*31 + int('.')
	if table == nil {
		return 0
	}
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
	if esc != nil && esc.storageAccessor != nil && space != nil {
		if rowCount, err := esc.storageAccessor.GetTableRowCount(space.ID()); err == nil && rowCount >= 0 {
			return rowCount
		}
	}
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
	if esc.btreeManager != nil {
		// Count only clustered-index leaf pages when the B+Tree can expose them;
		// scanning every INDEX page would incorrectly include internal nodes whose
		// PAGE_N_RECS field is not a table-row count.
		if leafPages, err := esc.btreeManager.GetAllLeafPages(context.Background()); err == nil && len(leafPages) > 0 {
			var total int64
			for _, pageID := range leafPages {
				total += esc.countRowsInPage(space, pageID)
			}
			return total
		}
	}

	pageCount := space.GetPageCount()
	var total int64
	for pageID := uint32(0); pageID < pageCount; pageID++ {
		total += esc.countRowsInPage(space, pageID)
	}
	return total
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
		rowCount, valid := esc.countRowsInPageWithValidity(space, pageID)
		if valid {
			validSampleCount++
			totalRowsInSample += rowCount
		}
	}

	// 基于采样结果估算总行数
	if validSampleCount == 0 {
		// Do not turn unreadable or non-index pages into synthetic rows. The
		// caller can retry with a real storage accessor or an exact scan once
		// the page source becomes available.
		return 0
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
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	selected := make([]uint32, sampleCount)

	// 初始化前sampleCount个页面
	for i := 0; i < sampleCount; i++ {
		selected[i] = uint32(i)
	}

	// 对剩余页面进行采样
	for i := sampleCount; i < int(totalPages); i++ {
		j := random.Intn(i + 1)
		if j < sampleCount {
			selected[j] = uint32(i)
		}
	}

	return selected
}

// countRowsInPage 统计页面中的行数
func (esc *EnhancedStatisticsCollector) countRowsInPage(space basic.Space, pageID uint32) int64 {
	rowCount, _ := esc.countRowsInPageWithValidity(space, pageID)
	return rowCount
}

func (esc *EnhancedStatisticsCollector) countRowsInPageWithValidity(space basic.Space, pageID uint32) (int64, bool) {
	if space == nil {
		return 0, false
	}
	page, err := space.LoadPageByPageNumber(pageID)
	if err != nil || len(page) < 56 {
		return 0, false
	}
	if len(page) >= 26 {
		pageType := binary.BigEndian.Uint16(page[24:26])
		if pageType != uint16(common.FIL_PAGE_INDEX) {
			return 0, false
		}
	}

	const pageHeaderOffset = 38
	const pageNRecsOffset = pageHeaderOffset + 16
	if len(page) < pageNRecsOffset+2 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint16(page[pageNRecsOffset : pageNRecsOffset+2])), true
}

// getSpaceSize 获取空间大小
func (esc *EnhancedStatisticsCollector) getSpaceSize(space basic.Space) (dataSize int64, indexSize int64) {
	if esc != nil && esc.storageAccessor != nil && space != nil {
		if dataSize, indexSize, err := esc.storageAccessor.GetTableSpaceSize(space.ID()); err == nil && dataSize >= 0 && indexSize >= 0 {
			return dataSize, indexSize
		}
	}
	usedSpace := space.GetUsedSpace()

	// 简单估算：70%为数据，30%为索引
	dataSize = int64(float64(usedSpace) * 0.7)
	indexSize = int64(float64(usedSpace) * 0.3)

	return dataSize, indexSize
}

// getFreeSpace 获取空闲空间
func (esc *EnhancedStatisticsCollector) getFreeSpace(space basic.Space) int64 {
	if space == nil {
		return 0
	}
	totalSpace := int64(space.GetPageCount()) * 16384 // 16KB per page
	usedSpace := int64(space.GetUsedSpace())
	if totalSpace <= usedSpace {
		return 0
	}
	return totalSpace - usedSpace
}

// getAutoIncrementValue 获取自增值
func (esc *EnhancedStatisticsCollector) getAutoIncrementValue(table *metadata.Table) uint64 {
	if table == nil || table.Stats == nil || table.Stats.AutoIncrement <= 0 {
		return 0
	}
	return uint64(table.Stats.AutoIncrement)
}

// ============ NDV统计 (OPT-016.2) ============

// sampleColumnData 采样列数据
func (esc *EnhancedStatisticsCollector) sampleColumnData(
	space basic.Space,
	column *metadata.Column,
	tableRowCount int64,
) []interface{} {
	sampleSize := esc.sampler.GetSampleSize(tableRowCount)
	if esc.storageAccessor != nil && space != nil {
		if values, ok := esc.sampleColumnDataFromAccessor(space.ID(), column, tableRowCount); ok {
			return values
		}
	}

	// 根据配置选择采样策略
	if esc.config.SampleRate >= 1.0 {
		// 精确模式：扫描所有数据
		return esc.scanAllColumnData(space, column, tableRowCount)
	} else {
		// 采样模式：基于页面采样
		return esc.sampleColumnDataFromPages(space, column, sampleSize)
	}
}

func (esc *EnhancedStatisticsCollector) sampleColumnDataFromAccessor(
	spaceID uint32,
	column *metadata.Column,
	tableRowCount int64,
) ([]interface{}, bool) {
	if esc == nil || esc.storageAccessor == nil {
		return nil, false
	}
	sampleSize := esc.sampler.GetSampleSize(tableRowCount)
	exactMode := esc.config != nil && esc.config.SampleRate >= 1.0
	if exactMode && tableRowCount > 0 {
		// ANALYZE with SampleRate=1 is an explicit full-scan request. Do not
		// apply AdaptiveSampler's 50,000-row cap in that mode.
		sampleSize = tableRowCount
	}
	rate := esc.config.SampleRate
	if rate <= 0 || exactMode {
		rate = 1.0
	}
	if records, err := esc.storageAccessor.SampleTableRecords(spaceID, rate); err == nil {
		return sampleColumnValuesFromRecords(records, column, sampleSize), true
	}
	return nil, false
}

func sampleColumnValuesFromRecords(records [][]interface{}, column *metadata.Column, sampleSize int64) []interface{} {
	if column == nil || len(records) == 0 || sampleSize <= 0 {
		return []interface{}{}
	}
	columnIndex := column.OrdinalPosition - 1
	if columnIndex < 0 {
		columnIndex = 0
	}
	values := make([]interface{}, 0, len(records))
	for _, record := range records {
		if columnIndex >= len(record) {
			continue
		}
		values = append(values, record[columnIndex])
		if int64(len(values)) >= sampleSize {
			break
		}
	}
	return values
}

// scanAllColumnData 扫描所有列数据（精确模式）
func (esc *EnhancedStatisticsCollector) scanAllColumnData(
	space basic.Space,
	column *metadata.Column,
	tableRowCount int64,
) []interface{} {
	// The generic basic.Space contract exposes page bytes but no clustered-row
	// decoder. Production ANALYZE uses StorageAccessor for decoded full scans;
	// without that capability retain the explicit page-sampling fallback.
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
	rowCount := esc.countRowsInPage(space, pageID)
	if rowCount <= 0 || column == nil {
		return []interface{}{}
	}

	// The generic page abstraction exposes row counts but not decoded column
	// values. Returning no values is intentional: callers can keep explicit
	// fallback statistics instead of persisting synthetic values. Production
	// ANALYZE uses StorageEngineAccessor.SampleTableRecords for real values.
	return nil
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
	if totalRowCount <= 0 {
		totalRowCount = int64(len(sampleData))
	}
	bucketLimit := esc.config.HistogramBuckets
	if bucketLimit <= 0 {
		bucketLimit = 1
	}
	histogram := &Histogram{
		NumBuckets:    bucketLimit,
		TotalCount:    totalRowCount,
		HistogramType: histType,
		SampleRows:    int64(len(sampleData)),
		Buckets:       make([]Bucket, 0, bucketLimit),
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
	histogram.NumBuckets = len(histogram.Buckets)

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
	bucketCount := histogram.NumBuckets
	if bucketCount <= 0 {
		bucketCount = 1
	}
	if bucketCount > len(data) {
		bucketCount = len(data)
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
			Count:      esc.scaleHistogramCount(int64(len(data)), histogram),
			Distinct:   1,
		}
		histogram.Buckets = append(histogram.Buckets, bucket)
		return
	}

	// 计算桶宽度
	bucketWidth := (maxFloat - minFloat) / float64(bucketCount)

	// 构建桶
	for i := 0; i < bucketCount; i++ {
		lowerBound := minFloat + float64(i)*bucketWidth
		upperBound := minFloat + float64(i+1)*bucketWidth

		lastBucket := i == bucketCount-1
		if lastBucket {
			upperBound = maxFloat // 最后一个桶包含最大值
		}

		// 只让最后一个桶包含上界，避免相邻桶在边界处重复计数。
		count := esc.countInRangeBounded(data, lowerBound, upperBound, lastBucket)
		distinct := esc.distinctInRange(data, lowerBound, upperBound, lastBucket)

		bucket := Bucket{
			LowerBound: esc.fromFloat64(lowerBound, column.DataType),
			UpperBound: esc.fromFloat64(upperBound, column.DataType),
			Count:      esc.scaleHistogramCount(count, histogram),
			Distinct:   int64(len(distinct)),
		}
		histogram.Buckets = append(histogram.Buckets, bucket)
	}
	esc.rebalanceHistogramCounts(histogram, int64(len(data)))
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

	bucketCount := histogram.NumBuckets
	if bucketCount <= 0 {
		bucketCount = 1
	}
	if bucketCount > len(sortedData) {
		bucketCount = len(sortedData)
	}
	// 使用向上取整，确保输出桶数不超过配置上限。
	targetCount := (len(sortedData) + bucketCount - 1) / bucketCount

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
		distinctSet[statisticsValueKey(val)] = struct{}{}

		// 如果达到目标数量或是最后一个值，结束当前桶
		if currentBucket.Count >= int64(targetCount) || i == len(sortedData)-1 {
			currentBucket.UpperBound = val
			currentBucket.Distinct = int64(len(distinctSet))

			currentBucket.Count = esc.scaleHistogramCount(currentBucket.Count, histogram)

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
	esc.rebalanceHistogramCounts(histogram, int64(len(data)))
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
	type frequency struct {
		value interface{}
		count int64
	}
	freqMap := make(map[string]frequency)
	for _, val := range data {
		key := statisticsValueKey(val)
		entry := freqMap[key]
		entry.value = val
		entry.count++
		freqMap[key] = entry
	}

	// 按频率排序
	type freqPair struct {
		key   string
		value interface{}
		count int64
	}

	freqList := make([]freqPair, 0, len(freqMap))
	for key, entry := range freqMap {
		freqList = append(freqList, freqPair{key, entry.value, entry.count})
	}

	// 排序（频率降序；频率相同按稳定键排序，保证 ANALYZE 可复现）
	sort.Slice(freqList, func(i, j int) bool {
		if freqList[i].count != freqList[j].count {
			return freqList[i].count > freqList[j].count
		}
		return freqList[i].key < freqList[j].key
	})

	// 取Top N作为桶
	numBuckets := histogram.NumBuckets
	if numBuckets <= 0 {
		numBuckets = 1
	}
	if len(freqList) < numBuckets {
		numBuckets = len(freqList)
	}

	for i := 0; i < numBuckets; i++ {
		bucket := Bucket{
			LowerBound: freqList[i].value,
			UpperBound: freqList[i].value,
			Count:      esc.scaleHistogramCount(freqList[i].count, histogram),
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

	for _, val := range data {
		if val == nil {
			continue
		}
		if max == nil {
			max = val
		}
		if min == nil {
			min = val
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
	return esc.countInRangeBounded(data, lower, upper, true)
}

func (esc *EnhancedStatisticsCollector) countInRangeBounded(data []interface{}, lower, upper float64, includeUpper bool) int64 {
	count := int64(0)
	for _, val := range data {
		if val == nil {
			continue
		}
		valFloat := esc.toFloat64(val)
		if valFloat >= lower && (valFloat < upper || includeUpper) {
			count++
		}
	}
	return count
}

func (esc *EnhancedStatisticsCollector) distinctInRange(data []interface{}, lower, upper float64, includeUpper bool) map[string]struct{} {
	distinct := make(map[string]struct{})
	for _, val := range data {
		if val == nil {
			continue
		}
		valFloat := esc.toFloat64(val)
		if valFloat >= lower && (valFloat < upper || includeUpper) {
			distinct[statisticsValueKey(val)] = struct{}{}
		}
	}
	return distinct
}

func statisticsValueKey(value interface{}) string {
	return fmt.Sprintf("%T:%v", value, value)
}

func (esc *EnhancedStatisticsCollector) scaleHistogramCount(sampleCount int64, histogram *Histogram) int64 {
	if sampleCount <= 0 {
		return 0
	}
	if histogram == nil || histogram.SampleRows <= 0 || histogram.TotalCount <= histogram.SampleRows {
		return sampleCount
	}
	scaled := int64(math.Round(float64(sampleCount) * float64(histogram.TotalCount) / float64(histogram.SampleRows)))
	if scaled < 1 {
		return 1
	}
	return scaled
}

func (esc *EnhancedStatisticsCollector) rebalanceHistogramCounts(histogram *Histogram, sampleCount int64) {
	if histogram == nil || len(histogram.Buckets) == 0 {
		return
	}
	target := esc.scaleHistogramCount(sampleCount, histogram)
	var current int64
	for _, bucket := range histogram.Buckets {
		current += bucket.Count
	}
	delta := target - current
	if delta > 0 {
		histogram.Buckets[len(histogram.Buckets)-1].Count += delta
		return
	}
	for i := len(histogram.Buckets) - 1; i >= 0 && delta < 0; i-- {
		remove := int64(math.Min(float64(histogram.Buckets[i].Count), float64(-delta)))
		histogram.Buckets[i].Count -= remove
		delta += remove
	}
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
	esc.stopOnce.Do(func() { close(esc.stopCh) })
}
