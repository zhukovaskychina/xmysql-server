package main

import (
	"fmt"
	"log"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func main() {
	fmt.Println("=== XMySQL SELECT功能测试 ===")

	// 创建配置并加载配置文件
	cfg := conf.NewCfg()
	args := &conf.CommandLineArgs{
		ConfigPath: "./my.ini",
	}
	cfg = cfg.Load(args)

	// 创建XMySQL引擎
	fmt.Println("正在初始化XMySQL引擎...")
	xmysqlEngine := engine.NewXMySQLEngine(cfg)
	fmt.Println("XMySQL引擎初始化完成")

	// 测试SQL查询
	testQueries := []string{
		"SELECT * FROM user",
		"SELECT id, name FROM user WHERE id = 1",
		"SELECT * FROM users ORDER BY id LIMIT 5",
	}

	for i, query := range testQueries {
		fmt.Printf("\n=== 测试查询 %d: %s ===\n", i+1, query)

		// 解析SQL
		_, err := sqlparser.Parse(query)
		if err != nil {
			log.Printf("SQL解析错误: %v", err)
			continue
		}
		fmt.Println("✓ SQL解析成功")

		// 使用引擎的ExecuteQuery方法
		fmt.Println("正在执行查询...")
		resultChan := xmysqlEngine.ExecuteQuery(nil, query, "mysql")

		// 等待结果
		var result *engine.Result
		for r := range resultChan {
			result = r
			break // 只取第一个结果
		}

		if result.Err != nil {
			log.Printf("查询执行错误: %v", result.Err)
			continue
		}

		fmt.Printf("✓ 查询执行成功，结果类型: %s\n", result.ResultType)

		// 检查结果数据类型
		if selectResult, ok := result.Data.(*engine.SelectResult); ok {
			fmt.Printf("✓ 返回行数: %d\n", selectResult.RowCount)
			fmt.Printf("✓ 列信息: %v\n", selectResult.Columns)

			// 打印前几行数据
			for i, record := range selectResult.Records {
				if i >= 3 { // 只显示前3行
					break
				}
				fmt.Printf("  行 %d: %v\n", i+1, record.Values)
			}

			if len(selectResult.Records) > 3 {
				fmt.Printf("  ... 还有 %d 行数据\n", len(selectResult.Records)-3)
			}
		} else {
			fmt.Printf("结果数据类型: %T\n", result.Data)
		}
	}

	fmt.Println("\n=== SELECT功能测试完成 ===")
}
