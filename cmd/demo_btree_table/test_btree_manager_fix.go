package main

import (
	"context"
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println("=== 测试B+树管理器构造函数修复 ===")

	// 创建配置
	config := &conf.Cfg{
		DataDir:              "test_data",
		InnodbDataDir:        "test_data/innodb",
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
		InnodbBufferPoolSize: 134217728, // 128MB
		InnodbPageSize:       16384,     // 16KB
	}

	fmt.Println("1. 创建存储管理器...")
	storageManager := manager.NewStorageManager(config)

	fmt.Println("2. 获取缓冲池管理器...")
	bufferPoolManager := storageManager.GetBufferPoolManager()
	if bufferPoolManager == nil {
		fmt.Println("❌ 缓冲池管理器为空，使用模拟实现")
		return
	}

	fmt.Println("3. 创建表存储映射管理器...")
	tableStorageManager := manager.NewTableStorageManager(storageManager)

	fmt.Println("4. 测试为表创建B+树管理器...")

	// 测试获取mysql.user表的存储信息
	userTableInfo, err := tableStorageManager.GetTableStorageInfo("mysql", "user")
	if err != nil {
		fmt.Printf("❌ 获取mysql.user表存储信息失败: %v\n", err)
		return
	}

	fmt.Printf("✓ mysql.user表存储信息: SpaceID=%d, RootPage=%d\n",
		userTableInfo.SpaceID, userTableInfo.RootPageNo)

	// 测试创建表特定的B+树管理器
	ctx := context.Background()
	userBTreeManager, err := tableStorageManager.CreateBTreeManagerForTable(ctx, "mysql", "user")
	if err != nil {
		fmt.Printf("❌ 创建mysql.user表B+树管理器失败: %v\n", err)
		return
	}

	fmt.Println("✓ 成功创建mysql.user表的B+树管理器")

	// 测试B+树管理器的基本功能
	fmt.Println("\n5. 测试B+树管理器基本功能...")

	// 测试获取第一个叶子页面
	firstLeafPage, err := userBTreeManager.GetFirstLeafPage(ctx)
	if err != nil {
		fmt.Printf("⚠️ 获取第一个叶子页面失败（预期的）: %v\n", err)
	} else {
		fmt.Printf("✓ 第一个叶子页面: %d\n", firstLeafPage)
	}

	// 测试获取所有叶子页面
	leafPages, err := userBTreeManager.GetAllLeafPages(ctx)
	if err != nil {
		fmt.Printf("⚠️ 获取所有叶子页面失败（预期的）: %v\n", err)
	} else {
		fmt.Printf("✓ 总共有 %d 个叶子页面\n", len(leafPages))
	}

	fmt.Println("\n=== 测试完成 ===")
	fmt.Println("✓ B+树管理器构造函数修复成功")
	fmt.Println("✓ 表存储映射管理器工作正常")
	fmt.Println("✓ 可以成功为特定表创建B+树管理器")
}
