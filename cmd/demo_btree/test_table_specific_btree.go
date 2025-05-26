package main

import (
	"context"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"os"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println("=== 测试表特定B+树管理器功能 ===")

	// 创建配置
	config := &conf.Cfg{
		DataDir:              "test_data",
		InnodbDataDir:        "test_data/innodb",
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
		InnodbBufferPoolSize: 134217728, // 128MB
		InnodbPageSize:       16384,     // 16KB
	}

	// 确保测试目录存在
	if err := os.MkdirAll(config.InnodbDataDir, 0755); err != nil {
		fmt.Printf("Failed to create test directory: %v\n", err)
		return
	}

	fmt.Println("1. 创建并初始化XMySQL引擎...")
	engine := engine.NewXMySQLEngine(config)

	// 测试表存储映射
	fmt.Println("\n2. 测试表存储映射功能...")
	testTableStorageMapping(engine)

	// 测试SELECT查询
	fmt.Println("\n3. 测试SELECT查询功能...")
	testSelectQuery(engine)

	fmt.Println("\n=== 所有测试完成 ===")
}

func testTableStorageMapping(engine *engine.XMySQLEngine) {
	fmt.Println("正在测试表存储映射...")

	// 获取存储管理器
	storageManager := getStorageManager(engine)
	if storageManager == nil {
		fmt.Println("❌ 无法获取存储管理器")
		return
	}

	// 创建表存储映射管理器
	tableStorageManager := manager.NewTableStorageManager(storageManager)

	// 测试获取系统表信息
	systemTables := tableStorageManager.GetSystemTableInfo()
	fmt.Printf("✓ 找到 %d 个系统表\n", len(systemTables))

	// 显示部分系统表信息
	for i, table := range systemTables {
		if i < 5 { // 只显示前5个
			fmt.Printf("  - %s.%s: SpaceID=%d, RootPage=%d\n",
				table.SchemaName, table.TableName, table.SpaceID, table.RootPageNo)
		}
	}

	// 测试获取mysql.user表的存储信息
	fmt.Println("\n测试获取mysql.user表的存储信息...")
	userTableInfo, err := tableStorageManager.GetTableStorageInfo("mysql", "user")
	if err != nil {
		fmt.Printf("❌ 获取mysql.user表存储信息失败: %v\n", err)
		return
	}

	fmt.Printf("✓ mysql.user表存储信息:\n")
	fmt.Printf("  - SpaceID: %d\n", userTableInfo.SpaceID)
	fmt.Printf("  - RootPage: %d\n", userTableInfo.RootPageNo)
	fmt.Printf("  - Type: %v\n", userTableInfo.Type)

	// 测试创建表特定的B+树管理器
	fmt.Println("\n测试为mysql.user表创建B+树管理器...")
	ctx := context.Background()
	userBTreeManager, err := tableStorageManager.CreateBTreeManagerForTable(ctx, "mysql", "user")
	if err != nil {
		fmt.Printf("❌ 创建mysql.user表B+树管理器失败: %v\n", err)
		return
	}

	fmt.Println("✓ 成功创建mysql.user表的B+树管理器")

	// 测试获取第一个叶子页面
	firstLeafPage, err := userBTreeManager.GetFirstLeafPage(ctx)
	if err != nil {
		fmt.Printf("❌ 获取第一个叶子页面失败: %v\n", err)
	} else {
		fmt.Printf("✓ 第一个叶子页面: %d\n", firstLeafPage)
	}

	// 测试获取所有叶子页面
	leafPages, err := userBTreeManager.GetAllLeafPages(ctx)
	if err != nil {
		fmt.Printf("❌ 获取所有叶子页面失败: %v\n", err)
	} else {
		fmt.Printf("✓ 总共有 %d 个叶子页面\n", len(leafPages))
		if len(leafPages) > 0 {
			fmt.Printf("  叶子页面: %v\n", leafPages)
		}
	}
}

func testSelectQuery(engine *engine.XMySQLEngine) {
	fmt.Println("正在测试SELECT查询...")

	// 获取查询执行器
	queryExecutor := getQueryExecutor(engine)
	if queryExecutor == nil {
		fmt.Println("❌ 无法获取查询执行器")
		return
	}

	// 测试解析和执行SELECT语句
	sql := "SELECT * FROM user"
	fmt.Printf("执行SQL: %s\n", sql)

	_, err := sqlparser.Parse(sql)
	if err != nil {
		fmt.Printf("❌ SQL解析失败: %v\n", err)
		return
	}

}

// getStorageManager 获取存储管理器（使用反射或类型断言）
func getStorageManager(engine *engine.XMySQLEngine) *manager.StorageManager {
	// 这里需要根据实际的engine结构来获取存储管理器
	// 由于没有直接的public方法，这里是一个简化的实现
	// 实际项目中可能需要添加getter方法
	return nil // 暂时返回nil，需要根据实际engine结构实现
}

// getQueryExecutor 获取查询执行器
func getQueryExecutor(engine *engine.XMySQLEngine) interface{} {
	// 同样需要根据实际的engine结构来获取查询执行器
	return nil // 暂时返回nil，需要根据实际engine结构实现
}

// 临时的测试实现，模拟存储管理器功能
func createTestStorageManager() *manager.StorageManager {
	fmt.Println("创建测试存储管理器...")

	// 这里应该创建一个真实的存储管理器
	// 但由于依赖复杂，先创建一个模拟版本用于演示
	return nil
}
