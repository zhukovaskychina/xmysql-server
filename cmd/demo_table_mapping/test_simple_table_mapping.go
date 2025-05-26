package main

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println("=== 测试表存储映射管理器 ===")

	// 创建一个模拟的存储管理器
	config := &conf.Cfg{
		DataDir:              "test_data",
		InnodbDataDir:        "test_data/innodb",
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
		InnodbBufferPoolSize: 134217728, // 128MB
		InnodbPageSize:       16384,     // 16KB
	}

	fmt.Println("1. 创建存储管理器...")
	storageManager := manager.NewStorageManager(config)

	fmt.Println("2. 创建表存储映射管理器...")
	tableStorageManager := manager.NewTableStorageManager(storageManager)

	fmt.Println("3. 测试系统表映射...")
	testSystemTableMapping(tableStorageManager)

	fmt.Println("4. 测试获取特定表的存储信息...")
	testSpecificTableInfo(tableStorageManager)

	fmt.Println("5. 测试SQL解析和表名提取...")
	testSQLParsingAndTableMapping()

	fmt.Println("\n=== 所有测试完成 ===")
}

func testSystemTableMapping(tsm *manager.TableStorageManager) {
	fmt.Println("\n--- 测试系统表映射 ---")

	// 获取所有系统表信息
	systemTables := tsm.GetSystemTableInfo()
	fmt.Printf("✓ 初始化了 %d 个系统表的存储映射\n", len(systemTables))

	// 显示前10个系统表
	fmt.Println("系统表列表（前10个）:")
	for i, table := range systemTables {
		if i >= 10 {
			break
		}
		fmt.Printf("  %d. %s.%s: SpaceID=%d, RootPage=%d, Type=%v\n",
			i+1, table.SchemaName, table.TableName, table.SpaceID, table.RootPageNo, table.Type)
	}

	// 列出所有注册的表
	allTables := tsm.ListAllTables()
	fmt.Printf("✓ 总共注册了 %d 个表\n", len(allTables))
}

func testSpecificTableInfo(tsm *manager.TableStorageManager) {
	fmt.Println("\n--- 测试特定表信息获取 ---")

	// 测试获取mysql.user表信息
	testTables := []struct {
		schema string
		table  string
	}{
		{"mysql", "user"},
		{"mysql", "db"},
		{"mysql", "tables_priv"},
		{"mysql", "plugin"},
	}

	for _, tt := range testTables {
		fmt.Printf("\n测试表: %s.%s\n", tt.schema, tt.table)

		info, err := tsm.GetTableStorageInfo(tt.schema, tt.table)
		if err != nil {
			fmt.Printf("❌ 获取失败: %v\n", err)
			continue
		}

		fmt.Printf("✓ 获取成功:\n")
		fmt.Printf("  - SpaceID: %d\n", info.SpaceID)
		fmt.Printf("  - RootPage: %d\n", info.RootPageNo)
		fmt.Printf("  - IndexPage: %d\n", info.IndexPageNo)
		fmt.Printf("  - DataSegmentID: %d\n", info.DataSegmentID)
		fmt.Printf("  - Type: %v\n", info.Type)

		// 测试根据SpaceID反向查找
		tableBySpace, err := tsm.GetTableBySpaceID(info.SpaceID)
		if err != nil {
			fmt.Printf("❌ 根据SpaceID反向查找失败: %v\n", err)
		} else {
			fmt.Printf("✓ 反向查找成功: %s.%s\n", tableBySpace.SchemaName, tableBySpace.TableName)
		}
	}
}

func testSQLParsingAndTableMapping() {
	fmt.Println("\n--- 测试SQL解析和表名提取 ---")

	testSQLs := []string{
		"SELECT * FROM user",
		"SELECT * FROM mysql.user",
		"SELECT id, name FROM user WHERE id = 1",
		"SELECT u.id, u.name FROM mysql.user u",
		"SELECT COUNT(*) FROM mysql.user",
	}

	for _, sql := range testSQLs {
		fmt.Printf("\n测试SQL: %s\n", sql)

		stmt, err := sqlparser.Parse(sql)
		if err != nil {
			fmt.Printf("❌ SQL解析失败: %v\n", err)
			continue
		}

		selectStmt, ok := stmt.(*sqlparser.Select)
		if !ok {
			fmt.Printf("❌ 不是SELECT语句\n")
			continue
		}

		fmt.Printf("✓ SQL解析成功\n")

		// 提取表名
		tableName, schemaName := extractTableNameFromSelect(selectStmt)
		fmt.Printf("  提取的表名: %s\n", tableName)
		fmt.Printf("  提取的数据库名: %s\n", schemaName)

		// 模拟查找表的存储信息
		if tableName != "" {
			if schemaName == "" {
				schemaName = "mysql" // 默认数据库
			}
			fmt.Printf("  → 将查找表: %s.%s 的存储信息\n", schemaName, tableName)
		}
	}
}

// extractTableNameFromSelect 从SELECT语句中提取表名
func extractTableNameFromSelect(stmt *sqlparser.Select) (tableName string, schemaName string) {
	// 简化实现：只处理单表查询
	if len(stmt.From) > 0 {
		if tableExpr, ok := stmt.From[0].(*sqlparser.AliasedTableExpr); ok {
			if tableName, ok := tableExpr.Expr.(sqlparser.TableName); ok {
				if tableName.Qualifier.String() != "" && tableName.Qualifier.String() != "" {
					schemaName = tableName.Qualifier.String()
				}
				return tableName.Name.String(), schemaName
			}
		}
	}
	return "", ""
}

// 演示如何使用表存储映射来解决B+树管理器的表特定问题
func demonstrateTableSpecificBTreeManager() {
	fmt.Println("\n--- 演示表特定B+树管理器解决方案 ---")

	fmt.Println(`
问题: 之前的B+树管理器不知道操作哪个表的数据
解决方案: 使用表存储映射管理器

步骤:
1. SQL解析 → 提取表名 (例如: mysql.user)
2. 表存储映射管理器 → 获取表的存储信息 (SpaceID=1, RootPage=3)
3. 创建表特定的B+树管理器 → 初始化为指定的SpaceID和RootPage
4. 执行查询 → B+树管理器现在知道操作哪个表的数据

这样就解决了原来的问题:
- 之前: btreeManager.GetAllLeafPages() // 不知道是哪个表的叶子页面
- 现在: tableBTreeManager.GetAllLeafPages() // 明确知道是mysql.user表的叶子页面

关键改进:
1. TableStorageManager 管理表名到存储结构的映射
2. 每个表有独立的B+树管理器实例
3. SELECT执行器使用表特定的B+树管理器
4. 缓冲池操作使用正确的SpaceID
`)
}
