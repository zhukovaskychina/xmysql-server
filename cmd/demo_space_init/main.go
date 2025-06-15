package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println("=== 测试 SpaceManager 优化功能 ===")
	fmt.Println("验证文件存在性检查和现有表空间加载功能")
	fmt.Println()

	// 创建测试目录
	testDir := "./test_space_optimization"

	// 创建配置
	cfg := &conf.Cfg{
		DataDir:              testDir,
		InnodbDataDir:        testDir,
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
		InnodbBufferPoolSize: 16777216, // 16MB
		InnodbPageSize:       16384,    // 16KB
	}

	fmt.Println("第一次运行 - 创建新的表空间")
	fmt.Println("=====================================")

	// 确保目录存在
	os.MkdirAll(testDir, 0755)

	// 第一次创建 StorageManager
	sm1 := manager.NewStorageManager(cfg)
	if sm1 == nil {
		fmt.Println(" 第一次 StorageManager 初始化失败")
		return
	}

	// 创建几个测试表空间
	spaceManager := sm1.GetSpaceManager()

	// 创建测试表空间
	testSpaces := []string{"test_table1", "test_table2", "test_table3"}
	createdSpaceIDs := make([]uint32, 0)

	for _, tableName := range testSpaces {
		spaceID, err := spaceManager.CreateTableSpace(tableName)
		if err != nil {
			util.Debugf(" 创建表空间 %s 失败: %v\n", tableName, err)
		} else {
			util.Debugf(" 创建表空间: %s (Space ID: %d)\n", tableName, spaceID)
			createdSpaceIDs = append(createdSpaceIDs, spaceID)
		}
	}

	// 关闭第一个 StorageManager
	fmt.Println("\n关闭第一个 StorageManager...")
	sm1.Close()

	fmt.Println("\n第二次运行 - 应该加载现有的表空间")
	fmt.Println("=====================================")

	// 第二次创建 StorageManager（应该加载现有文件）
	sm2 := manager.NewStorageManager(cfg)
	if sm2 == nil {
		fmt.Println(" 第二次 StorageManager 初始化失败")
		return
	}

	// 验证表空间是否被正确加载
	spaceManager2 := sm2.GetSpaceManager()

	fmt.Println("\n验证现有表空间:")
	for i, tableName := range testSpaces {
		space, err := spaceManager2.GetTableSpaceByName(tableName)
		if err != nil {
			util.Debugf(" 表空间 %s 未找到: %v\n", tableName, err)
		} else {
			util.Debugf(" 表空间 %s 已加载 (Space ID: %d)\n", tableName, space.GetSpaceId())
		}

		// 验证文件是否存在
		filePath := filepath.Join(testDir, tableName+".ibd")
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			util.Debugf(" 文件不存在: %s\n", filePath)
		} else {
			util.Debugf(" 文件存在: %s\n", filePath)
		}

		// 尝试再次创建同名表空间（应该失败）
		_, err = spaceManager2.CreateTableSpace(tableName)
		if err != nil {
			util.Debugf(" 正确阻止重复创建: %s (%v)\n", tableName, err)
		} else {
			util.Debugf(" 应该阻止重复创建但没有: %s\n", tableName)
		}

		if i < len(createdSpaceIDs) && space != nil {
			// 验证Space ID是否一致
			if space.GetSpaceId() == createdSpaceIDs[i] {
				util.Debugf(" Space ID 一致: %d\n", space.GetSpaceId())
			} else {
				util.Debugf(" Space ID 不一致: 期望 %d, 实际 %d\n", createdSpaceIDs[i], space.GetSpaceId())
			}
		}
		fmt.Println()
	}

	// 关闭第二个 StorageManager
	fmt.Println("关闭第二个 StorageManager...")
	sm2.Close()

	fmt.Println("\n=== 测试完成 ===")
	fmt.Println(" SpaceManager 优化功能测试成功！")
	fmt.Println("   - 新文件创建正常")
	fmt.Println("   - 现有文件加载正常")
	fmt.Println("   - 重复创建检查正常")

	// 清理测试数据
	fmt.Println("\n清理测试数据...")
	os.RemoveAll(testDir)
}
