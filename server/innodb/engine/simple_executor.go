package engine

import (
	"fmt"
	"io"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// SimpleTableScanExecutor 简单的表扫描执行器
type SimpleTableScanExecutor struct {
	BaseExecutor
	tableName  string
	rows       [][]interface{}
	currentIdx int
}

// NewSimpleTableScanExecutor 创建简单的表扫描执行器
func NewSimpleTableScanExecutor(ctx *ExecutionContext, tableName string) *SimpleTableScanExecutor {
	return &SimpleTableScanExecutor{
		BaseExecutor: BaseExecutor{
			ctx:    ctx,
			closed: false,
			schema: nil, // 简化处理，设为nil
		},
		tableName:  tableName,
		currentIdx: -1,
	}
}

// Schema 返回表的模式信息
func (e *SimpleTableScanExecutor) Schema() *metadata.Schema {
	return e.schema
}

// Children 返回子执行器（表扫描没有子执行器）
func (e *SimpleTableScanExecutor) Children() []Executor {
	return e.children
}

// SetChildren 设置子执行器
func (e *SimpleTableScanExecutor) SetChildren(children []Executor) {
	e.children = children
}

// Init 初始化表扫描执行器
func (e *SimpleTableScanExecutor) Init() error {
	if e.closed {
		return nil
	}

	// 模拟表数据
	e.rows = [][]interface{}{
		{1, "Alice", 25},
		{2, "Bob", 30},
		{3, "Charlie", 35},
		{4, "Diana", 28},
		{5, "Eve", 32},
	}

	fmt.Printf("Initialized table scan for table: %s with %d rows\n", e.tableName, len(e.rows))
	return nil
}

// Next 获取下一行数据
func (e *SimpleTableScanExecutor) Next() error {
	if e.closed {
		return io.EOF
	}

	e.currentIdx++
	if e.currentIdx >= len(e.rows) {
		return io.EOF
	}

	return nil
}

// GetRow 获取当前行数据
func (e *SimpleTableScanExecutor) GetRow() []interface{} {
	if e.currentIdx < 0 || e.currentIdx >= len(e.rows) {
		return nil
	}
	return e.rows[e.currentIdx]
}

// Close 关闭执行器
func (e *SimpleTableScanExecutor) Close() error {
	if e.closed {
		return nil
	}

	e.closed = true
	fmt.Printf("Closed table scan executor for table: %s\n", e.tableName)
	return nil
}

// SimpleProjectionExecutor 简单的投影执行器
type SimpleProjectionExecutor struct {
	BaseExecutor
	child      Executor
	columnIdxs []int // 要投影的列索引
	currentRow []interface{}
}

// NewSimpleProjectionExecutor 创建简单的投影执行器
func NewSimpleProjectionExecutor(ctx *ExecutionContext, child Executor, columnIdxs []int) *SimpleProjectionExecutor {
	return &SimpleProjectionExecutor{
		BaseExecutor: BaseExecutor{
			ctx:    ctx,
			closed: false,
			schema: nil,
		},
		child:      child,
		columnIdxs: columnIdxs,
	}
}

// Schema 返回投影后的模式信息
func (e *SimpleProjectionExecutor) Schema() *metadata.Schema {
	return e.schema
}

// Children 返回子执行器
func (e *SimpleProjectionExecutor) Children() []Executor {
	if e.child != nil {
		return []Executor{e.child}
	}
	return []Executor{}
}

// SetChildren 设置子执行器
func (e *SimpleProjectionExecutor) SetChildren(children []Executor) {
	if len(children) > 0 {
		e.child = children[0]
	}
}

// Init 初始化投影执行器
func (e *SimpleProjectionExecutor) Init() error {
	if e.closed {
		return nil
	}

	// 初始化子执行器
	if e.child != nil {
		if err := e.child.Init(); err != nil {
			return fmt.Errorf("failed to initialize child executor: %v", err)
		}
	}

	fmt.Printf("Initialized projection executor with columns: %v\n", e.columnIdxs)
	return nil
}

// Next 获取下一行数据并进行投影
func (e *SimpleProjectionExecutor) Next() error {
	if e.closed {
		return io.EOF
	}

	if e.child == nil {
		return io.EOF
	}

	// 从子执行器获取下一行
	if err := e.child.Next(); err != nil {
		return err
	}

	// 获取子执行器的当前行
	childRow := e.child.GetRow()
	if childRow == nil {
		return fmt.Errorf("child executor returned nil row")
	}

	// 执行投影
	projectedRow := make([]interface{}, 0, len(e.columnIdxs))
	for _, idx := range e.columnIdxs {
		if idx < len(childRow) {
			projectedRow = append(projectedRow, childRow[idx])
		} else {
			projectedRow = append(projectedRow, nil)
		}
	}

	e.currentRow = projectedRow
	return nil
}

// GetRow 获取当前投影后的行数据
func (e *SimpleProjectionExecutor) GetRow() []interface{} {
	return e.currentRow
}

// Close 关闭执行器
func (e *SimpleProjectionExecutor) Close() error {
	if e.closed {
		return nil
	}

	e.closed = true
	if e.child != nil {
		return e.child.Close()
	}
	return nil
}

// SimpleFilterExecutor 简单的过滤执行器
type SimpleFilterExecutor struct {
	BaseExecutor
	child     Executor
	predicate func([]interface{}) bool
}

// NewSimpleFilterExecutor 创建简单的过滤执行器
func NewSimpleFilterExecutor(ctx *ExecutionContext, child Executor, predicate func([]interface{}) bool) *SimpleFilterExecutor {
	return &SimpleFilterExecutor{
		BaseExecutor: BaseExecutor{
			ctx:    ctx,
			closed: false,
			schema: nil,
		},
		child:     child,
		predicate: predicate,
	}
}

// Schema 返回过滤后的模式信息
func (e *SimpleFilterExecutor) Schema() *metadata.Schema {
	return e.schema
}

// Children 返回子执行器
func (e *SimpleFilterExecutor) Children() []Executor {
	if e.child != nil {
		return []Executor{e.child}
	}
	return []Executor{}
}

// SetChildren 设置子执行器
func (e *SimpleFilterExecutor) SetChildren(children []Executor) {
	if len(children) > 0 {
		e.child = children[0]
	}
}

// Init 初始化过滤执行器
func (e *SimpleFilterExecutor) Init() error {
	if e.closed {
		return nil
	}

	// 初始化子执行器
	if e.child != nil {
		if err := e.child.Init(); err != nil {
			return fmt.Errorf("failed to initialize child executor: %v", err)
		}
	}

	fmt.Println("Initialized filter executor")
	return nil
}

// Next 获取下一行满足条件的数据
func (e *SimpleFilterExecutor) Next() error {
	if e.closed {
		return io.EOF
	}

	if e.child == nil {
		return io.EOF
	}

	// 循环查找满足条件的行
	for {
		// 从子执行器获取下一行
		if err := e.child.Next(); err != nil {
			return err
		}

		// 获取子执行器的当前行
		childRow := e.child.GetRow()
		if childRow == nil {
			continue
		}

		// 检查是否满足过滤条件
		if e.predicate == nil || e.predicate(childRow) {
			return nil
		}
	}
}

// GetRow 获取当前行数据
func (e *SimpleFilterExecutor) GetRow() []interface{} {
	if e.child != nil {
		return e.child.GetRow()
	}
	return nil
}

// Close 关闭执行器
func (e *SimpleFilterExecutor) Close() error {
	if e.closed {
		return nil
	}

	e.closed = true
	if e.child != nil {
		return e.child.Close()
	}
	return nil
}
