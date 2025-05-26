package main

import (
	"fmt"
	"io"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
)

func main() {
	fmt.Println("=== 执行器演示程序 ===")
	fmt.Println()

	// 创建执行上下文
	ctx := &engine.ExecutionContext{}

	// 演示1: 简单表扫描
	fmt.Println("1. 表扫描演示")
	fmt.Println("================")
	demoTableScan(ctx)
	fmt.Println()

	// 演示2: 投影执行器
	fmt.Println("2. 投影执行器演示")
	fmt.Println("==================")
	demoProjection(ctx)
	fmt.Println()

	// 演示3: 过滤执行器
	fmt.Println("3. 过滤执行器演示")
	fmt.Println("==================")
	demoFilter(ctx)
	fmt.Println()

	// 演示4: 组合执行器（表扫描 + 过滤 + 投影）
	fmt.Println("4. 组合执行器演示")
	fmt.Println("==================")
	demoComposite(ctx)
	fmt.Println()

	// 演示5: 聚合执行器
	fmt.Println("5. 聚合执行器演示")
	fmt.Println("==================")
	demoAggregate(ctx)
	fmt.Println()

	fmt.Println("=== 演示完成 ===")
}

// demoTableScan 演示表扫描执行器
func demoTableScan(ctx *engine.ExecutionContext) {
	// 创建表扫描执行器
	tableScan := engine.NewSimpleTableScanExecutor(ctx, "users")

	// 初始化
	if err := tableScan.Init(); err != nil {
		fmt.Printf("❌ 初始化失败: %v\n", err)
		return
	}

	// 扫描所有行
	fmt.Println("扫描结果:")
	rowCount := 0
	for {
		err := tableScan.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("❌ 扫描错误: %v\n", err)
			break
		}

		row := tableScan.GetRow()
		rowCount++
		fmt.Printf("  行 %d: %v\n", rowCount, row)
	}

	// 关闭执行器
	tableScan.Close()
	fmt.Printf("✅ 共扫描 %d 行数据\n", rowCount)
}

// demoProjection 演示投影执行器
func demoProjection(ctx *engine.ExecutionContext) {
	// 创建表扫描执行器作为数据源
	tableScan := engine.NewSimpleTableScanExecutor(ctx, "users")

	// 创建投影执行器，只选择第0列和第1列（id和name）
	projection := engine.NewSimpleProjectionExecutor(ctx, tableScan, []int{0, 1})

	// 初始化
	if err := projection.Init(); err != nil {
		fmt.Printf("❌ 初始化失败: %v\n", err)
		return
	}

	// 获取投影后的数据
	fmt.Println("投影结果 (只显示 id 和 name):")
	rowCount := 0
	for {
		err := projection.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("❌ 投影错误: %v\n", err)
			break
		}

		row := projection.GetRow()
		rowCount++
		fmt.Printf("  行 %d: %v\n", rowCount, row)
	}

	// 关闭执行器
	projection.Close()
	fmt.Printf("✅ 共投影 %d 行数据\n", rowCount)
}

// demoFilter 演示过滤执行器
func demoFilter(ctx *engine.ExecutionContext) {
	// 创建表扫描执行器作为数据源
	tableScan := engine.NewSimpleTableScanExecutor(ctx, "users")

	// 创建过滤执行器，只保留年龄大于30的记录
	filter := engine.NewSimpleFilterExecutor(ctx, tableScan, func(row []interface{}) bool {
		if len(row) >= 3 {
			if age, ok := row[2].(int); ok {
				return age > 30
			}
		}
		return false
	})

	// 初始化
	if err := filter.Init(); err != nil {
		fmt.Printf("❌ 初始化失败: %v\n", err)
		return
	}

	// 获取过滤后的数据
	fmt.Println("过滤结果 (年龄 > 30):")
	rowCount := 0
	for {
		err := filter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("❌ 过滤错误: %v\n", err)
			break
		}

		row := filter.GetRow()
		rowCount++
		fmt.Printf("  行 %d: %v\n", rowCount, row)
	}

	// 关闭执行器
	filter.Close()
	fmt.Printf("✅ 共过滤出 %d 行数据\n", rowCount)
}

// demoComposite 演示组合执行器
func demoComposite(ctx *engine.ExecutionContext) {
	// 创建表扫描执行器作为数据源
	tableScan := engine.NewSimpleTableScanExecutor(ctx, "users")

	// 创建过滤执行器，只保留年龄大于等于30的记录
	filter := engine.NewSimpleFilterExecutor(ctx, tableScan, func(row []interface{}) bool {
		if len(row) >= 3 {
			if age, ok := row[2].(int); ok {
				return age >= 30
			}
		}
		return false
	})

	// 创建投影执行器，只选择name和age列
	projection := engine.NewSimpleProjectionExecutor(ctx, filter, []int{1, 2})

	// 初始化
	if err := projection.Init(); err != nil {
		fmt.Printf("❌ 初始化失败: %v\n", err)
		return
	}

	// 获取最终结果
	fmt.Println("组合结果 (年龄 >= 30 的用户的 name 和 age):")
	fmt.Println("相当于 SQL: SELECT name, age FROM users WHERE age >= 30")
	rowCount := 0
	for {
		err := projection.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("❌ 执行错误: %v\n", err)
			break
		}

		row := projection.GetRow()
		rowCount++
		if len(row) >= 2 {
			fmt.Printf("  行 %d: name=%v, age=%v\n", rowCount, row[0], row[1])
		} else {
			fmt.Printf("  行 %d: %v\n", rowCount, row)
		}
	}

	// 关闭执行器
	projection.Close()
	fmt.Printf("✅ 共输出 %d 行数据\n", rowCount)
}

// demoAggregate 演示聚合执行器
func demoAggregate(ctx *engine.ExecutionContext) {
	// 创建表扫描执行器作为数据源
	tableScan := engine.NewSimpleTableScanExecutor(ctx, "users")

	// 创建聚合执行器，计算平均年龄
	aggregate := engine.NewSimpleAggregateExecutor(ctx, tableScan, func(row []interface{}) interface{} {
		if len(row) >= 3 {
			if age, ok := row[2].(int); ok {
				return age
			}
		}
		return 0
	}, func(a, b interface{}) interface{} {
		return a.(int) + b.(int)
	}, func(a interface{}) interface{} {
		return a.(int) / 2
	})

	// 初始化
	if err := aggregate.Init(); err != nil {
		fmt.Printf("❌ 初始化失败: %v\n", err)
		return
	}

	// 获取聚合结果
	fmt.Println("聚合结果 (平均年龄):")
	rowCount := 0
	for {
		err := aggregate.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("❌ 聚合错误: %v\n", err)
			break
		}

		result := aggregate.GetResult()
		rowCount++
		fmt.Printf("  行 %d: %v\n", rowCount, result)
	}

	// 关闭执行器
	aggregate.Close()
	fmt.Printf("✅ 共计算 %d 次聚合\n", rowCount)
}
