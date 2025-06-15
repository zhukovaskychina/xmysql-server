package main

import (
	"fmt"
	"log"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println(" 测试SchemataGenerator数据生成功能")

	// 直接测试SchemataGenerator，不依赖复杂的存储管理器
	fmt.Println(" 创建并测试SchemataGenerator...")

	// 创建SchemataGenerator（不依赖InfoSchemaManager）
	generator := &manager.SchemataGenerator{}

	// 生成数据
	rows, err := generator.Generate()
	if err != nil {
		log.Fatalf(" 生成数据失败: %v", err)
	}

	logger.Debugf(" 成功生成 %d 行数据\n\n", len(rows))

	// 显示生成的数据
	fmt.Println(" SCHEMATA表数据:")
	fmt.Println("CATALOG_NAME | SCHEMA_NAME | DEFAULT_CHARACTER_SET_NAME | DEFAULT_COLLATION_NAME | SQL_PATH")
	fmt.Println("-------------|-------------|---------------------------|------------------------|----------")

	for i, row := range rows {
		if len(row) >= 5 {
			catalogName := formatValue(row[0])
			schemaName := formatValue(row[1])
			charset := formatValue(row[2])
			collation := formatValue(row[3])
			sqlPath := formatValue(row[4])

			logger.Debugf("%-12s | %-11s | %-25s | %-22s | %-8s\n",
				catalogName, schemaName, charset, collation, sqlPath)
		} else {
			logger.Debugf("行 %d: 数据不完整 %v\n", i+1, row)
		}
	}

	// 特别查找demo_db
	fmt.Println("\n 查找demo_db数据库:")
	found := false
	for _, row := range rows {
		if len(row) >= 2 && row[1] == "demo_db" {
			found = true
			logger.Debugf(" 找到demo_db: SCHEMA_NAME=%s, DEFAULT_CHARACTER_SET_NAME=%s, DEFAULT_COLLATION_NAME=%s\n",
				row[1], row[2], row[3])
			break
		}
	}

	if !found {
		fmt.Println(" 未找到demo_db数据库")
	}

	// 验证查询结果符合预期
	fmt.Println("\n 验证查询结果:")
	demoDB := findSchemaByName(rows, "demo_db")
	if demoDB != nil {
		logger.Debugf(" demo_db验证成功:\n")
		logger.Debugf("   - SCHEMA_NAME: %s\n", demoDB[1])
		logger.Debugf("   - DEFAULT_CHARACTER_SET_NAME: %s\n", demoDB[2])
		logger.Debugf("   - DEFAULT_COLLATION_NAME: %s\n", demoDB[3])

		// 验证字符集和排序规则
		expectedCharset := "utf8mb4"
		expectedCollation := "utf8mb4_general_ci"

		if demoDB[2] == expectedCharset && demoDB[3] == expectedCollation {
			fmt.Println(" demo_db的字符集和排序规则符合预期")
		} else {
			logger.Debugf(" demo_db的字符集或排序规则不符合预期，期望: %s/%s, 实际: %s/%s\n",
				expectedCharset, expectedCollation, demoDB[2], demoDB[3])
		}
	}

	fmt.Println("\n🎉 测试完成！")
}

func formatValue(value interface{}) string {
	if value == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", value)
}

func findSchemaByName(rows [][]interface{}, schemaName string) []interface{} {
	for _, row := range rows {
		if len(row) >= 2 && row[1] == schemaName {
			return row
		}
	}
	return nil
}
