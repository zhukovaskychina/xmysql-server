package engine

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/logger"
	"io"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// AggregateType 聚合函数类型
type AggregateType int

const (
	AggregateCount AggregateType = iota
	AggregateSum
	AggregateAvg
	AggregateMin
	AggregateMax
)

// AggregateFunction 聚合函数定义
type AggregateFunction struct {
	Type      AggregateType
	ColumnIdx int    // 聚合的列索引
	Alias     string // 结果列别名
}

// SimpleAggregateExecutor 简单的聚合执行器
type SimpleAggregateExecutor struct {
	BaseExecutor
	child     Executor
	functions []AggregateFunction
	result    []interface{}
	finished  bool
}

// NewSimpleAggregateExecutor 创建简单的聚合执行器
func NewSimpleAggregateExecutor(ctx *ExecutionContext, child Executor, functions []AggregateFunction) *SimpleAggregateExecutor {
	return &SimpleAggregateExecutor{
		BaseExecutor: BaseExecutor{
			ctx:    ctx,
			closed: false,
			schema: nil,
		},
		child:     child,
		functions: functions,
		finished:  false,
	}
}

// Schema 返回聚合后的模式信息
func (e *SimpleAggregateExecutor) Schema() *metadata.Schema {
	return e.schema
}

// Children 返回子执行器
func (e *SimpleAggregateExecutor) Children() []Executor {
	if e.child != nil {
		return []Executor{e.child}
	}
	return []Executor{}
}

// SetChildren 设置子执行器
func (e *SimpleAggregateExecutor) SetChildren(children []Executor) {
	if len(children) > 0 {
		e.child = children[0]
	}
}

// Init 初始化聚合执行器
func (e *SimpleAggregateExecutor) Init() error {
	if e.closed {
		return nil
	}

	// 初始化子执行器
	if e.child != nil {
		if err := e.child.Init(); err != nil {
			return fmt.Errorf("failed to initialize child executor: %v", err)
		}
	}

	// 执行聚合计算
	if err := e.computeAggregates(); err != nil {
		return fmt.Errorf("failed to compute aggregates: %v", err)
	}

	logger.Debugf("Initialized aggregate executor with %d functions", len(e.functions))
	return nil
}

// Next 获取聚合结果（只返回一行）
func (e *SimpleAggregateExecutor) Next() error {
	if e.closed || e.finished {
		return io.EOF
	}

	e.finished = true
	return nil
}

// GetRow 获取聚合结果行
func (e *SimpleAggregateExecutor) GetRow() []interface{} {
	if e.finished {
		return e.result
	}
	return nil
}

// Close 关闭执行器
func (e *SimpleAggregateExecutor) Close() error {
	if e.closed {
		return nil
	}

	e.closed = true
	if e.child != nil {
		return e.child.Close()
	}
	return nil
}

// computeAggregates 计算聚合函数
func (e *SimpleAggregateExecutor) computeAggregates() error {
	if e.child == nil {
		return fmt.Errorf("no child executor")
	}

	// 初始化聚合状态
	aggregateStates := make([]interface{}, len(e.functions))
	rowCounts := make([]int, len(e.functions))

	for i, fn := range e.functions {
		switch fn.Type {
		case AggregateCount:
			aggregateStates[i] = 0
		case AggregateSum, AggregateAvg:
			aggregateStates[i] = 0.0
		case AggregateMin:
			aggregateStates[i] = nil // 将在第一行时设置
		case AggregateMax:
			aggregateStates[i] = nil // 将在第一行时设置
		}
	}

	// 遍历所有行进行聚合计算
	for {
		err := e.child.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading from child: %v", err)
		}

		row := e.child.GetRow()
		if row == nil {
			continue
		}

		// 对每个聚合函数进行计算
		for i, fn := range e.functions {
			if err := e.updateAggregate(i, fn, row, aggregateStates, rowCounts); err != nil {
				return fmt.Errorf("error updating aggregate %d: %v", i, err)
			}
		}
	}

	// 完成聚合计算（特别是AVG）
	e.result = make([]interface{}, len(e.functions))
	for i, fn := range e.functions {
		switch fn.Type {
		case AggregateAvg:
			if rowCounts[i] > 0 {
				if sum, ok := aggregateStates[i].(float64); ok {
					e.result[i] = sum / float64(rowCounts[i])
				} else {
					e.result[i] = 0.0
				}
			} else {
				e.result[i] = 0.0
			}
		default:
			e.result[i] = aggregateStates[i]
		}
	}

	return nil
}

// updateAggregate 更新单个聚合函数的状态
func (e *SimpleAggregateExecutor) updateAggregate(index int, fn AggregateFunction, row []interface{}, states []interface{}, counts []int) error {
	switch fn.Type {
	case AggregateCount:
		if count, ok := states[index].(int); ok {
			states[index] = count + 1
		}

	case AggregateSum, AggregateAvg:
		if fn.ColumnIdx < len(row) {
			value := row[fn.ColumnIdx]
			if numValue, err := convertToFloat64(value); err == nil {
				if sum, ok := states[index].(float64); ok {
					states[index] = sum + numValue
					counts[index]++
				}
			}
		}

	case AggregateMin:
		if fn.ColumnIdx < len(row) {
			value := row[fn.ColumnIdx]
			if states[index] == nil {
				states[index] = value
			} else {
				if compareValues(value, states[index]) < 0 {
					states[index] = value
				}
			}
		}

	case AggregateMax:
		if fn.ColumnIdx < len(row) {
			value := row[fn.ColumnIdx]
			if states[index] == nil {
				states[index] = value
			} else {
				if compareValues(value, states[index]) > 0 {
					states[index] = value
				}
			}
		}
	}

	return nil
}

// convertToFloat64 将值转换为float64
func convertToFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

// compareValues 比较两个值的大小
func compareValues(a, b interface{}) int {
	// 简化实现：只比较数字和字符串
	switch va := a.(type) {
	case int:
		if vb, ok := b.(int); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case string:
		if vb, ok := b.(string); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	}
	return 0
}
