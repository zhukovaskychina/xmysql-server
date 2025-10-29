package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// ========================================
// HashJoin 性能基准测试
// ========================================

func BenchmarkHashJoin_SmallTables(b *testing.B) {
	benchmarkHashJoin(b, 100, 100)
}

func BenchmarkHashJoin_MediumTables(b *testing.B) {
	benchmarkHashJoin(b, 1000, 1000)
}

func BenchmarkHashJoin_LargeTables(b *testing.B) {
	benchmarkHashJoin(b, 10000, 10000)
}

func BenchmarkHashJoin_SkewedData(b *testing.B) {
	// 测试数据倾斜场景
	ctx := context.Background()

	// 左表：10000行，ID分布正常
	leftData := generateSequentialData(10000)
	leftSchema := &metadata.Schema{
		Columns: []*metadata.Column{
			{Name: "id", Type: metadata.TypeInt},
			{Name: "value", Type: metadata.TypeDouble},
		},
	}
	leftOp := NewMockDataOperator(leftData, leftSchema)

	// 右表：10000行，但90%的数据集中在少数几个ID上
	rightData := generateSkewedData(10000, 10)
	rightSchema := &metadata.Schema{
		Columns: []*metadata.Column{
			{Name: "user_id", Type: metadata.TypeInt},
			{Name: "score", Type: metadata.TypeDouble},
		},
	}

	keyFunc := func(r Record) string {
		values := r.GetValues()
		if len(values) > 0 {
			return values[0].ToString()
		}
		return ""
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rightOp := NewMockDataOperator(rightData, rightSchema)
		hashJoin := NewHashJoinOperator(leftOp, rightOp, "INNER", keyFunc, keyFunc)

		_ = hashJoin.Open(ctx)
		count := 0
		for {
			record, _ := hashJoin.Next(ctx)
			if record == nil {
				break
			}
			count++
		}
		_ = hashJoin.Close()

		// 重置左表
		leftOp.idx = 0
	}
}

func benchmarkHashJoin(b *testing.B, leftSize, rightSize int) {
	ctx := context.Background()

	// 生成测试数据
	leftData := generateSequentialData(leftSize)
	rightData := generateSequentialData(rightSize)

	leftSchema := &metadata.Schema{
		Columns: []*metadata.Column{
			{Name: "id", Type: metadata.TypeInt},
			{Name: "value", Type: metadata.TypeDouble},
		},
	}
	rightSchema := &metadata.Schema{
		Columns: []*metadata.Column{
			{Name: "id", Type: metadata.TypeInt},
			{Name: "score", Type: metadata.TypeDouble},
		},
	}

	keyFunc := func(r Record) string {
		values := r.GetValues()
		if len(values) > 0 {
			return values[0].ToString()
		}
		return ""
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		leftOp := NewMockDataOperator(leftData, leftSchema)
		rightOp := NewMockDataOperator(rightData, rightSchema)

		hashJoin := NewHashJoinOperator(leftOp, rightOp, "INNER", keyFunc, keyFunc)

		_ = hashJoin.Open(ctx)
		count := 0
		for {
			record, _ := hashJoin.Next(ctx)
			if record == nil {
				break
			}
			count++
		}
		_ = hashJoin.Close()
	}
}

// ========================================
// HashAggregate 性能基准测试
// ========================================

func BenchmarkHashAgg_SmallGroups(b *testing.B) {
	benchmarkHashAgg(b, 1000, 10)
}

func BenchmarkHashAgg_MediumGroups(b *testing.B) {
	benchmarkHashAgg(b, 10000, 100)
}

func BenchmarkHashAgg_LargeGroups(b *testing.B) {
	benchmarkHashAgg(b, 100000, 1000)
}

func BenchmarkHashAgg_ManyAggFuncs(b *testing.B) {
	ctx := context.Background()

	// 生成测试数据
	data := generateGroupedData(10000, 100)
	schema := &metadata.Schema{
		Columns: []*metadata.Column{
			{Name: "group_key", Type: metadata.TypeVarchar},
			{Name: "value", Type: metadata.TypeDouble},
		},
	}

	// 多个聚合函数: COUNT, SUM, AVG, MIN, MAX
	groupByExprs := []int{0}
	aggFuncs := []AggregateFunc{
		&CountAgg{},
		&SumAgg{},
		&AvgAgg{},
		&MinAgg{},
		&MaxAgg{},
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		childOp := NewMockDataOperator(data, schema)
		hashAgg := NewHashAggregateOperator(childOp, groupByExprs, aggFuncs)

		_ = hashAgg.Open(ctx)
		count := 0
		for {
			record, _ := hashAgg.Next(ctx)
			if record == nil {
				break
			}
			count++
		}
		_ = hashAgg.Close()
	}
}

func benchmarkHashAgg(b *testing.B, rowCount, groupCount int) {
	ctx := context.Background()

	// 生成分组数据
	data := generateGroupedData(rowCount, groupCount)
	schema := &metadata.Schema{
		Columns: []*metadata.Column{
			{Name: "group_key", Type: metadata.TypeVarchar},
			{Name: "value", Type: metadata.TypeDouble},
		},
	}

	groupByExprs := []int{0}
	aggFuncs := []AggregateFunc{
		&CountAgg{},
		&SumAgg{},
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		childOp := NewMockDataOperator(data, schema)
		hashAgg := NewHashAggregateOperator(childOp, groupByExprs, aggFuncs)

		_ = hashAgg.Open(ctx)
		count := 0
		for {
			record, _ := hashAgg.Next(ctx)
			if record == nil {
				break
			}
			count++
		}
		_ = hashAgg.Close()
	}
}

// ========================================
// 数据生成辅助函数
// ========================================

// generateSequentialData 生成顺序数据
func generateSequentialData(count int) [][]basic.Value {
	data := make([][]basic.Value, count)
	for i := 0; i < count; i++ {
		data[i] = []basic.Value{
			basic.NewInt64(int64(i)),
			basic.NewFloat64(float64(i) * 1.5),
		}
	}
	return data
}

// generateGroupedData 生成分组数据
func generateGroupedData(rowCount, groupCount int) [][]basic.Value {
	data := make([][]basic.Value, rowCount)
	for i := 0; i < rowCount; i++ {
		groupIdx := i % groupCount
		data[i] = []basic.Value{
			basic.NewString(fmt.Sprintf("group_%d", groupIdx)),
			basic.NewFloat64(float64(i) * 1.5),
		}
	}
	return data
}

// generateSkewedData 生成倾斜数据
func generateSkewedData(rowCount, hotKeys int) [][]basic.Value {
	data := make([][]basic.Value, rowCount)
	for i := 0; i < rowCount; i++ {
		var key int64
		// 90%的数据集中在少数几个key上
		if i < rowCount*9/10 {
			key = int64(i % hotKeys)
		} else {
			key = int64(hotKeys + i)
		}
		data[i] = []basic.Value{
			basic.NewInt64(key),
			basic.NewFloat64(float64(i) * 1.5),
		}
	}
	return data
}
