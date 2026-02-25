package engine

import (
	"context"
	"fmt"
	"sort"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// ========================================
// Window Function Types
// ========================================

// WindowFunctionType 窗口函数类型
type WindowFunctionType int

const (
	WindowFuncRowNumber  WindowFunctionType = iota // ROW_NUMBER()
	WindowFuncRank                                 // RANK()
	WindowFuncDenseRank                            // DENSE_RANK()
	WindowFuncNTile                                // NTILE(n)
	WindowFuncLag                                  // LAG(expr, offset)
	WindowFuncLead                                 // LEAD(expr, offset)
	WindowFuncFirstValue                           // FIRST_VALUE(expr)
	WindowFuncLastValue                            // LAST_VALUE(expr)
)

// WindowFrameType 窗口帧类型
type WindowFrameType int

const (
	FrameTypeRows  WindowFrameType = iota // ROWS
	FrameTypeRange                        // RANGE
)

// WindowFrameBound 窗口帧边界
type WindowFrameBound struct {
	Type   string // UNBOUNDED_PRECEDING, CURRENT_ROW, UNBOUNDED_FOLLOWING, N_PRECEDING, N_FOLLOWING
	Offset int64  // 偏移量（用于N_PRECEDING和N_FOLLOWING）
}

// WindowFrame 窗口帧定义
type WindowFrame struct {
	Type  WindowFrameType  // ROWS or RANGE
	Start WindowFrameBound // 起始边界
	End   WindowFrameBound // 结束边界
}

// WindowSpec 窗口规范
type WindowSpec struct {
	PartitionBy []int         // PARTITION BY列索引
	OrderBy     []OrderBySpec // ORDER BY规范
	Frame       *WindowFrame  // 窗口帧（可选）
}

// OrderBySpec 排序规范
type OrderBySpec struct {
	ColumnIndex int  // 列索引
	Ascending   bool // 是否升序
}

// WindowFunction 窗口函数定义
type WindowFunction struct {
	Type        WindowFunctionType // 函数类型
	ColumnIndex int                // 输入列索引（用于LAG/LEAD/FIRST_VALUE/LAST_VALUE）
	Offset      int64              // 偏移量（用于LAG/LEAD）
	N           int64              // 分桶数量（用于NTILE）
}

// ========================================
// WindowFunctionOperator - 窗口函数算子
// ========================================

// WindowFunctionOperator 窗口函数算子
type WindowFunctionOperator struct {
	BaseOperator
	child      Operator
	windowSpec WindowSpec
	windowFunc WindowFunction

	// 执行状态
	allRows      []Record // 所有输入行
	partitions   [][]int  // 分区索引（每个分区包含行索引列表）
	currentIndex int      // 当前输出行索引
	resultCache  []Record // 结果缓存
	computed     bool     // 是否已计算
}

// NewWindowFunctionOperator 创建窗口函数算子
func NewWindowFunctionOperator(
	child Operator,
	windowSpec WindowSpec,
	windowFunc WindowFunction,
) *WindowFunctionOperator {
	return &WindowFunctionOperator{
		BaseOperator: BaseOperator{
			children: []Operator{child},
		},
		child:      child,
		windowSpec: windowSpec,
		windowFunc: windowFunc,
	}
}

// Open 初始化窗口函数算子
func (w *WindowFunctionOperator) Open(ctx context.Context) error {
	if err := w.BaseOperator.Open(ctx); err != nil {
		return err
	}

	// 构建输出schema（原始列 + 窗口函数结果列）
	childSchema := w.child.Schema()
	if childSchema == nil {
		return fmt.Errorf("child schema is nil")
	}

	// 创建新的schema，包含原始列和窗口函数结果列
	w.schema = childSchema.Clone()

	// 添加窗口函数结果列
	w.schema.AddColumn(&metadata.QueryColumn{
		Name:       w.getWindowFunctionName(),
		DataType:   metadata.TypeBigInt, // 大多数窗口函数返回整数
		IsNullable: true,
	})

	logger.Debugf("WindowFunctionOperator opened with function: %s", w.getWindowFunctionName())
	return nil
}

// Next 获取下一条记录
func (w *WindowFunctionOperator) Next(ctx context.Context) (Record, error) {
	if !w.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	// 第一次调用时，读取所有输入并计算窗口函数
	if !w.computed {
		if err := w.computeWindowFunction(ctx); err != nil {
			return nil, fmt.Errorf("failed to compute window function: %w", err)
		}
		w.computed = true
	}

	// 返回下一条结果
	if w.currentIndex >= len(w.resultCache) {
		return nil, nil // EOF
	}

	result := w.resultCache[w.currentIndex]
	w.currentIndex++
	return result, nil
}

// computeWindowFunction 计算窗口函数
func (w *WindowFunctionOperator) computeWindowFunction(ctx context.Context) error {
	// 1. 读取所有输入行
	if err := w.readAllRows(ctx); err != nil {
		return fmt.Errorf("failed to read all rows: %w", err)
	}

	// 2. 分区
	w.partitionRows()

	// 3. 对每个分区排序
	w.sortPartitions()

	// 4. 计算窗口函数值
	w.resultCache = make([]Record, len(w.allRows))
	for partitionIdx, partition := range w.partitions {
		if err := w.computePartitionWindowFunction(partitionIdx, partition); err != nil {
			return fmt.Errorf("failed to compute partition %d: %w", partitionIdx, err)
		}
	}

	logger.Debugf("WindowFunctionOperator computed %d rows", len(w.resultCache))
	return nil
}

// readAllRows 读取所有输入行
func (w *WindowFunctionOperator) readAllRows(ctx context.Context) error {
	w.allRows = make([]Record, 0)
	for {
		record, err := w.child.Next(ctx)
		if err != nil {
			return err
		}
		if record == nil {
			break // EOF
		}
		w.allRows = append(w.allRows, record)
	}
	return nil
}

// partitionRows 分区行
func (w *WindowFunctionOperator) partitionRows() {
	if len(w.windowSpec.PartitionBy) == 0 {
		// 没有PARTITION BY，所有行在一个分区
		partition := make([]int, len(w.allRows))
		for i := range w.allRows {
			partition[i] = i
		}
		w.partitions = [][]int{partition}
		return
	}

	// 按PARTITION BY列分组
	partitionMap := make(map[string][]int)
	for i, row := range w.allRows {
		key := w.getPartitionKey(row)
		partitionMap[key] = append(partitionMap[key], i)
	}

	// 转换为分区列表
	w.partitions = make([][]int, 0, len(partitionMap))
	for _, partition := range partitionMap {
		w.partitions = append(w.partitions, partition)
	}
}

// getPartitionKey 获取分区键
func (w *WindowFunctionOperator) getPartitionKey(row Record) string {
	values := row.GetValues()
	key := ""
	for _, colIdx := range w.windowSpec.PartitionBy {
		if colIdx < len(values) {
			key += fmt.Sprintf("%v|", values[colIdx].Raw())
		}
	}
	return key
}

// sortPartitions 对每个分区排序
func (w *WindowFunctionOperator) sortPartitions() {
	if len(w.windowSpec.OrderBy) == 0 {
		return // 没有ORDER BY，不需要排序
	}

	for _, partition := range w.partitions {
		sort.Slice(partition, func(i, j int) bool {
			return w.compareRows(w.allRows[partition[i]], w.allRows[partition[j]]) < 0
		})
	}
}

// compareRows 比较两行（用于排序）
func (w *WindowFunctionOperator) compareRows(row1, row2 Record) int {
	values1 := row1.GetValues()
	values2 := row2.GetValues()

	for _, orderBy := range w.windowSpec.OrderBy {
		if orderBy.ColumnIndex >= len(values1) || orderBy.ColumnIndex >= len(values2) {
			continue
		}

		cmp := w.compareValues(values1[orderBy.ColumnIndex], values2[orderBy.ColumnIndex])
		if cmp != 0 {
			if orderBy.Ascending {
				return cmp
			}
			return -cmp
		}
	}
	return 0
}

// compareValues 比较两个值
func (w *WindowFunctionOperator) compareValues(v1, v2 basic.Value) int {
	// NULL值处理
	if v1.IsNull() && v2.IsNull() {
		return 0
	}
	if v1.IsNull() {
		return -1
	}
	if v2.IsNull() {
		return 1
	}

	// 数值比较
	f1 := v1.Float64()
	f2 := v2.Float64()
	if f1 < f2 {
		return -1
	}
	if f1 > f2 {
		return 1
	}
	return 0
}

// getWindowFunctionName 获取窗口函数名称
func (w *WindowFunctionOperator) getWindowFunctionName() string {
	switch w.windowFunc.Type {
	case WindowFuncRowNumber:
		return "ROW_NUMBER"
	case WindowFuncRank:
		return "RANK"
	case WindowFuncDenseRank:
		return "DENSE_RANK"
	case WindowFuncNTile:
		return fmt.Sprintf("NTILE(%d)", w.windowFunc.N)
	case WindowFuncLag:
		return fmt.Sprintf("LAG(%d)", w.windowFunc.Offset)
	case WindowFuncLead:
		return fmt.Sprintf("LEAD(%d)", w.windowFunc.Offset)
	case WindowFuncFirstValue:
		return "FIRST_VALUE"
	case WindowFuncLastValue:
		return "LAST_VALUE"
	default:
		return "UNKNOWN_WINDOW_FUNC"
	}
}

// computePartitionWindowFunction 计算分区的窗口函数
func (w *WindowFunctionOperator) computePartitionWindowFunction(partitionIdx int, partition []int) error {
	switch w.windowFunc.Type {
	case WindowFuncRowNumber:
		return w.computeRowNumber(partition)
	case WindowFuncRank:
		return w.computeRank(partition)
	case WindowFuncDenseRank:
		return w.computeDenseRank(partition)
	case WindowFuncNTile:
		return w.computeNTile(partition)
	case WindowFuncLag:
		return w.computeLag(partition)
	case WindowFuncLead:
		return w.computeLead(partition)
	case WindowFuncFirstValue:
		return w.computeFirstValue(partition)
	case WindowFuncLastValue:
		return w.computeLastValue(partition)
	default:
		return fmt.Errorf("unsupported window function type: %d", w.windowFunc.Type)
	}
}

// computeRowNumber 计算ROW_NUMBER()
func (w *WindowFunctionOperator) computeRowNumber(partition []int) error {
	for i, rowIdx := range partition {
		rowNumber := int64(i + 1)
		w.resultCache[rowIdx] = w.appendWindowFunctionResult(w.allRows[rowIdx], rowNumber)
	}
	return nil
}

// computeRank 计算RANK()
func (w *WindowFunctionOperator) computeRank(partition []int) error {
	rank := int64(1)
	for i, rowIdx := range partition {
		if i > 0 {
			// 检查当前行是否与前一行相同（按ORDER BY列）
			if !w.rowsEqualByOrderBy(w.allRows[partition[i-1]], w.allRows[rowIdx]) {
				rank = int64(i + 1)
			}
		}
		w.resultCache[rowIdx] = w.appendWindowFunctionResult(w.allRows[rowIdx], rank)
	}
	return nil
}

// computeDenseRank 计算DENSE_RANK()
func (w *WindowFunctionOperator) computeDenseRank(partition []int) error {
	denseRank := int64(1)
	for i, rowIdx := range partition {
		if i > 0 {
			// 检查当前行是否与前一行相同（按ORDER BY列）
			if !w.rowsEqualByOrderBy(w.allRows[partition[i-1]], w.allRows[rowIdx]) {
				denseRank++
			}
		}
		w.resultCache[rowIdx] = w.appendWindowFunctionResult(w.allRows[rowIdx], denseRank)
	}
	return nil
}

// computeNTile 计算NTILE(n)
func (w *WindowFunctionOperator) computeNTile(partition []int) error {
	n := w.windowFunc.N
	if n <= 0 {
		return fmt.Errorf("NTILE requires positive n, got %d", n)
	}

	partitionSize := int64(len(partition))
	bucketSize := partitionSize / n
	remainder := partitionSize % n

	for i, rowIdx := range partition {
		// 计算当前行所属的桶
		bucket := int64(i) / bucketSize
		if bucket >= n {
			bucket = n - 1
		}

		// 处理余数：前remainder个桶多分配一行
		if int64(i) < remainder*(bucketSize+1) {
			bucket = int64(i) / (bucketSize + 1)
		} else {
			adjustedIdx := int64(i) - remainder*(bucketSize+1)
			bucket = remainder + adjustedIdx/bucketSize
		}

		w.resultCache[rowIdx] = w.appendWindowFunctionResult(w.allRows[rowIdx], bucket+1)
	}
	return nil
}

// computeLag 计算LAG(expr, offset)
func (w *WindowFunctionOperator) computeLag(partition []int) error {
	offset := w.windowFunc.Offset
	colIdx := w.windowFunc.ColumnIndex

	for i, rowIdx := range partition {
		var lagValue basic.Value
		lagIdx := i - int(offset)

		if lagIdx >= 0 && lagIdx < len(partition) {
			// 获取前offset行的值
			lagRowIdx := partition[lagIdx]
			lagRow := w.allRows[lagRowIdx]
			values := lagRow.GetValues()
			if colIdx < len(values) {
				lagValue = values[colIdx]
			} else {
				lagValue = basic.NewNull()
			}
		} else {
			// 超出范围，返回NULL
			lagValue = basic.NewNull()
		}

		w.resultCache[rowIdx] = w.appendWindowFunctionResultValue(w.allRows[rowIdx], lagValue)
	}
	return nil
}

// computeLead 计算LEAD(expr, offset)
func (w *WindowFunctionOperator) computeLead(partition []int) error {
	offset := w.windowFunc.Offset
	colIdx := w.windowFunc.ColumnIndex

	for i, rowIdx := range partition {
		var leadValue basic.Value
		leadIdx := i + int(offset)

		if leadIdx >= 0 && leadIdx < len(partition) {
			// 获取后offset行的值
			leadRowIdx := partition[leadIdx]
			leadRow := w.allRows[leadRowIdx]
			values := leadRow.GetValues()
			if colIdx < len(values) {
				leadValue = values[colIdx]
			} else {
				leadValue = basic.NewNull()
			}
		} else {
			// 超出范围，返回NULL
			leadValue = basic.NewNull()
		}

		w.resultCache[rowIdx] = w.appendWindowFunctionResultValue(w.allRows[rowIdx], leadValue)
	}
	return nil
}

// computeFirstValue 计算FIRST_VALUE(expr)
func (w *WindowFunctionOperator) computeFirstValue(partition []int) error {
	if len(partition) == 0 {
		return nil
	}

	colIdx := w.windowFunc.ColumnIndex

	// 获取窗口帧中的第一个值
	for _, rowIdx := range partition {
		frame := w.getWindowFrame(partition, rowIdx)
		var firstValue basic.Value

		if len(frame) > 0 {
			firstRowIdx := frame[0]
			firstRow := w.allRows[firstRowIdx]
			values := firstRow.GetValues()
			if colIdx < len(values) {
				firstValue = values[colIdx]
			} else {
				firstValue = basic.NewNull()
			}
		} else {
			firstValue = basic.NewNull()
		}

		w.resultCache[rowIdx] = w.appendWindowFunctionResultValue(w.allRows[rowIdx], firstValue)
	}
	return nil
}

// computeLastValue 计算LAST_VALUE(expr)
func (w *WindowFunctionOperator) computeLastValue(partition []int) error {
	if len(partition) == 0 {
		return nil
	}

	colIdx := w.windowFunc.ColumnIndex

	// 获取窗口帧中的最后一个值
	for _, rowIdx := range partition {
		frame := w.getWindowFrame(partition, rowIdx)
		var lastValue basic.Value

		if len(frame) > 0 {
			lastRowIdx := frame[len(frame)-1]
			lastRow := w.allRows[lastRowIdx]
			values := lastRow.GetValues()
			if colIdx < len(values) {
				lastValue = values[colIdx]
			} else {
				lastValue = basic.NewNull()
			}
		} else {
			lastValue = basic.NewNull()
		}

		w.resultCache[rowIdx] = w.appendWindowFunctionResultValue(w.allRows[rowIdx], lastValue)
	}
	return nil
}

// getWindowFrame 获取窗口帧（用于FIRST_VALUE/LAST_VALUE）
func (w *WindowFunctionOperator) getWindowFrame(partition []int, currentRowIdx int) []int {
	// 如果没有定义窗口帧，默认为RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	if w.windowSpec.Frame == nil {
		// 找到当前行在分区中的位置
		currentPos := -1
		for i, idx := range partition {
			if idx == currentRowIdx {
				currentPos = i
				break
			}
		}
		if currentPos < 0 {
			return []int{}
		}
		// 返回从开始到当前行的所有行
		return partition[:currentPos+1]
	}

	// TODO: 实现完整的窗口帧逻辑（ROWS/RANGE BETWEEN ... AND ...）
	// 简化实现：返回整个分区
	return partition
}

// rowsEqualByOrderBy 检查两行在ORDER BY列上是否相等
func (w *WindowFunctionOperator) rowsEqualByOrderBy(row1, row2 Record) bool {
	if len(w.windowSpec.OrderBy) == 0 {
		return true // 没有ORDER BY，所有行相等
	}

	values1 := row1.GetValues()
	values2 := row2.GetValues()

	for _, orderBy := range w.windowSpec.OrderBy {
		if orderBy.ColumnIndex >= len(values1) || orderBy.ColumnIndex >= len(values2) {
			return false
		}

		if w.compareValues(values1[orderBy.ColumnIndex], values2[orderBy.ColumnIndex]) != 0 {
			return false
		}
	}
	return true
}

// appendWindowFunctionResult 追加窗口函数结果（整数值）
func (w *WindowFunctionOperator) appendWindowFunctionResult(row Record, result int64) Record {
	return w.appendWindowFunctionResultValue(row, basic.NewInt64Value(result))
}

// appendWindowFunctionResultValue 追加窗口函数结果（任意值）
func (w *WindowFunctionOperator) appendWindowFunctionResultValue(row Record, result basic.Value) Record {
	values := row.GetValues()
	newValues := make([]basic.Value, len(values)+1)
	copy(newValues, values)
	newValues[len(values)] = result
	return NewExecutorRecordFromValues(newValues, w.schema)
}
