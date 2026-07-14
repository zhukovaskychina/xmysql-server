package plan

import (
	"context"
	"fmt"
	"sync"
	"sort"
)

// ParallelExecutor 并行执行器
type ParallelExecutor struct {
	workers    int           // 工作线程数
	chunkSize  int           // 数据块大小
	workerPool chan struct{} // 工作线程池
}

// NewParallelExecutor 创建并行执行器
func NewParallelExecutor(workers, chunkSize int) *ParallelExecutor {
	return &ParallelExecutor{
		workers:    workers,
		chunkSize:  chunkSize,
		workerPool: make(chan struct{}, workers),
	}
}

// ParallelizePhysicalPlan 并行化物理计划
func (e *ParallelExecutor) ParallelizePhysicalPlan(plan PhysicalPlan) PhysicalPlan {
	switch p := plan.(type) {
	case *PhysicalTableScan:
		return e.parallelizeTableScan(p)
	case *PhysicalHashJoin:
		return e.parallelizeHashJoin(p)
	case *PhysicalHashAgg:
		return e.parallelizeHashAgg(p)
	case *PhysicalSort:
		return e.parallelizeSort(p)
	default:
		// 递归并行化子计划
		children := p.Children()
		for i, child := range children {
			children[i] = e.ParallelizePhysicalPlan(child)
		}
		p.SetChildren(children)
		return p
	}
}

// ParallelTableScan 并行表扫描
type ParallelTableScan struct {
	PhysicalTableScan
	chunks   []DataChunk // 数据分片
	executor *ParallelExecutor
}

// DataChunk 数据分片
type DataChunk struct {
	StartRowID int64
	EndRowID   int64
}

func (e *ParallelExecutor) parallelizeTableScan(scan *PhysicalTableScan) PhysicalPlan {
	// 1. 数据分片
	rowCount := scan.Table.Stats.RowCount
	chunkCount := (rowCount + int64(e.chunkSize) - 1) / int64(e.chunkSize)
	chunks := make([]DataChunk, chunkCount)

	for i := int64(0); i < chunkCount; i++ {
		chunks[i] = DataChunk{
			StartRowID: i * int64(e.chunkSize),
			EndRowID:   min((i+1)*int64(e.chunkSize), rowCount),
		}
	}

	return &ParallelTableScan{
		PhysicalTableScan: *scan,
		chunks:            chunks,
		executor:          e,
	}
}

// ParallelHashJoin 并行哈希连接
type ParallelHashJoin struct {
	PhysicalHashJoin
	partitions int        // 分区数
	hashTable  []sync.Map // 分区哈希表
	executor   *ParallelExecutor
}

func (e *ParallelExecutor) parallelizeHashJoin(join *PhysicalHashJoin) PhysicalPlan {
	// 使用工作线程数作为分区数
	return &ParallelHashJoin{
		PhysicalHashJoin: *join,
		partitions:       e.workers,
		hashTable:        make([]sync.Map, e.workers),
		executor:         e,
	}
}

// ParallelHashAgg 并行哈希聚合
type ParallelHashAgg struct {
	PhysicalHashAgg
	partitions int        // 分区数
	localAggs  []sync.Map // 本地聚合结果
	executor   *ParallelExecutor
}

func (e *ParallelExecutor) parallelizeHashAgg(agg *PhysicalHashAgg) PhysicalPlan {
	return &ParallelHashAgg{
		PhysicalHashAgg: *agg,
		partitions:      e.workers,
		localAggs:       make([]sync.Map, e.workers),
		executor:        e,
	}
}

// ParallelSort 并行排序
type ParallelSort struct {
	PhysicalSort
	chunks   []DataChunk // 数据分片
	executor *ParallelExecutor
}

func (e *ParallelExecutor) parallelizeSort(sort *PhysicalSort) PhysicalPlan {
	// 1. 数据分片
	rowCount := estimateRowCount(sort.Children()[0])
	chunkCount := (rowCount + int64(e.chunkSize) - 1) / int64(e.chunkSize)
	chunks := make([]DataChunk, chunkCount)

	for i := int64(0); i < chunkCount; i++ {
		chunks[i] = DataChunk{
			StartRowID: i * int64(e.chunkSize),
			EndRowID:   min((i+1)*int64(e.chunkSize), rowCount),
		}
	}

	return &ParallelSort{
		PhysicalSort: *sort,
		chunks:       chunks,
		executor:     e,
	}
}

// Execute 执行并行计划
func (e *ParallelExecutor) Execute(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
	switch p := plan.(type) {
	case *ParallelTableScan:
		return e.executeParallelTableScan(ctx, p)
	case *ParallelHashJoin:
		return e.executeParallelHashJoin(ctx, p)
	case *ParallelHashAgg:
		return e.executeParallelHashAgg(ctx, p)
	case *ParallelSort:
		return e.executeParallelSort(ctx, p)
	default:
		return nil, nil
	}
}

func (e *ParallelExecutor) executeParallelTableScan(ctx context.Context, scan *ParallelTableScan) ([][]interface{}, error) {
	results := make([][]interface{}, 0)
	var mu sync.Mutex
	var wg sync.WaitGroup

	// 并行扫描每个数据分片
	for _, chunk := range scan.chunks {
		wg.Add(1)
		go func(chunk DataChunk) {
			defer wg.Done()

			// 获取工作线程
			e.workerPool <- struct{}{}
			defer func() { <-e.workerPool }()

			// 扫描分片
			rows, err := scanChunk(scan, chunk)
			if err != nil {
				return
			}

			// 合并结果
			mu.Lock()
			results = append(results, rows...)
			mu.Unlock()
		}(chunk)
	}

	wg.Wait()
	return results, nil
}

func (e *ParallelExecutor) executeParallelHashJoin(ctx context.Context, join *ParallelHashJoin) ([][]interface{}, error) {
	// 1. 并行构建哈希表
	var wg sync.WaitGroup
	for i := 0; i < join.partitions; i++ {
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()

			// 获取工作线程
			e.workerPool <- struct{}{}
			defer func() { <-e.workerPool }()

			// 构建分区哈希表
			buildPartitionHashTable(join, partition)
		}(i)
	}
	wg.Wait()

	// 2. 并行探测
	results := make([][]interface{}, 0)
	var mu sync.Mutex

	for i := 0; i < join.partitions; i++ {
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()

			// 获取工作线程
			e.workerPool <- struct{}{}
			defer func() { <-e.workerPool }()

			// 探测并连接
			rows := probePartition(join, partition)

			// 合并结果
			mu.Lock()
			results = append(results, rows...)
			mu.Unlock()
		}(i)
	}

	wg.Wait()
	return results, nil
}

func (e *ParallelExecutor) executeParallelHashAgg(ctx context.Context, agg *ParallelHashAgg) ([][]interface{}, error) {
	// 1. 并行局部聚合
	var wg sync.WaitGroup
	for i := 0; i < agg.partitions; i++ {
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()

			// 获取工作线程
			e.workerPool <- struct{}{}
			defer func() { <-e.workerPool }()

			// 局部聚合
			localAggregate(agg, partition)
		}(i)
	}
	wg.Wait()

	// 2. 全局聚合
	return mergeAggregates(agg)
}

func (e *ParallelExecutor) executeParallelSort(ctx context.Context, sort *ParallelSort) ([][]interface{}, error) {
	// 1. 并行局部排序
	sortedChunks := make([][][]interface{}, len(sort.chunks))
	var wg sync.WaitGroup

	for i, chunk := range sort.chunks {
		wg.Add(1)
		go func(i int, chunk DataChunk) {
			defer wg.Done()

			// 获取工作线程
			e.workerPool <- struct{}{}
			defer func() { <-e.workerPool }()

			// 局部排序
			rows := sortChunk(sort, chunk)
			sortedChunks[i] = rows
		}(i, chunk)
	}
	wg.Wait()

	// 2. 归并排序
	return mergeSortedChunks(sortedChunks, sort.ByItems)
}

// 辅助函数

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func estimateRowCount(plan PhysicalPlan) int64 {
	if plan == nil {
		return 0
	}

	if rows := plan.GetEstimateRows(); rows > 0 {
		return rows
	}

	return 1000
}

func scanChunk(scan *ParallelTableScan, chunk DataChunk) ([][]interface{}, error) {
	if chunk.EndRowID < chunk.StartRowID {
		return nil, fmt.Errorf("invalid chunk range: %d-%d", chunk.StartRowID, chunk.EndRowID)
	}

	rows := make([][]interface{}, 0, chunk.EndRowID-chunk.StartRowID)
	for rowID := chunk.StartRowID; rowID < chunk.EndRowID; rowID++ {
		rows = append(rows, []interface{}{rowID})
	}

	return rows, nil
}

func buildPartitionHashTable(join *ParallelHashJoin, partition int) {
	if partition < 0 || partition >= len(join.hashTable) {
		return
	}

	// 简化实现：为每个分区预先放置一个可探测的占位键
	join.hashTable[partition].Store("partition", partition)
}

func probePartition(join *ParallelHashJoin, partition int) [][]interface{} {
	if partition < 0 || partition >= len(join.hashTable) {
		return nil
	}

	// 简化实现：返回一个代表连接命中的占位行
	rows := make([][]interface{}, 0)
	if _, ok := join.hashTable[partition].Load("partition"); ok {
		rows = append(rows, []interface{}{partition})
	}

	return rows
}

func localAggregate(agg *ParallelHashAgg, partition int) {
	if partition < 0 || agg.localAggs == nil || partition >= len(agg.localAggs) {
		return
	}

	// 简化实现：每个分区记录一条本地聚合结果
	agg.localAggs[partition].Store("partition", partition)
}

func mergeAggregates(agg *ParallelHashAgg) ([][]interface{}, error) {
	result := make([][]interface{}, 0)
	for partition := range agg.localAggs {
		if _, ok := agg.localAggs[partition].Load("partition"); ok {
			result = append(result, []interface{}{partition, 1})
		}
	}

	// 返回统一后的聚合摘要
	return result, nil
}

func sortChunk(parallelSort *ParallelSort, chunk DataChunk) [][]interface{} {
	if chunk.EndRowID < chunk.StartRowID {
		return nil
	}

	rows := make([][]interface{}, 0, chunk.EndRowID-chunk.StartRowID)
	for rowID := chunk.StartRowID; rowID < chunk.EndRowID; rowID++ {
		rows = append(rows, []interface{}{rowID})
	}

	sort.Slice(rows, func(i, j int) bool {
		return rowLess(rows[i], rows[j], parallelSort.ByItems)
	})

	return rows
}

func mergeSortedChunks(chunks [][][]interface{}, byItems []ByItem) ([][]interface{}, error) {
	merged := make([][]interface{}, 0)
	for _, chunk := range chunks {
		merged = append(merged, chunk...)
	}

	sort.Slice(merged, func(i, j int) bool {
		return rowLess(merged[i], merged[j], byItems)
	})

	return merged, nil
}

func rowLess(left, right []interface{}, byItems []ByItem) bool {
	if len(byItems) == 0 || len(left) == 0 || len(right) == 0 {
		if len(left) == 0 {
			return len(right) != 0
		}
		if len(right) == 0 {
			return false
		}
		return compareRows(left, right) < 0
	}

	for i, byItem := range byItems {
		if i >= len(left) || i >= len(right) {
			continue
		}

		cmp := compareRowValues(left[i], right[i])
		if cmp != 0 {
			if byItem.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
	}

	return compareRows(left, right) < 0
}

func compareRows(left, right []interface{}) int {
	leftLen := len(left)
	rightLen := len(right)
	for i := 0; i < leftLen && i < rightLen; i++ {
		if cmp := compareRowValues(left[i], right[i]); cmp != 0 {
			return cmp
		}
	}

	if leftLen < rightLen {
		return -1
	}
	if leftLen > rightLen {
		return 1
	}
	return 0
}

func compareRowValues(left, right interface{}) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}

	lf := toFloat64Value(left)
	rf := toFloat64Value(right)
	if lf < rf {
		return -1
	}
	if lf > rf {
		return 1
	}
	return 0
}

func toFloat64Value(value interface{}) float64 {
	switch v := value.(type) {
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
		var parsed float64
		if n, err := fmt.Sscanf(v, "%f", &parsed); n == 1 && err == nil {
			return parsed
		}
		return 0
	default:
		return 0
	}
}
