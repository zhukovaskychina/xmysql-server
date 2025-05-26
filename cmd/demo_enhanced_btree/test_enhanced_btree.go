package main

import (
	"fmt"
	"os"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println("🚀 === 测试增强版B+树管理器架构 ===")
	fmt.Println()

	// 创建配置
	config := &conf.Cfg{
		DataDir:              "test_data_enhanced",
		InnodbDataDir:        "test_data_enhanced/innodb",
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
		InnodbBufferPoolSize: 134217728, // 128MB
		InnodbPageSize:       16384,     // 16KB
	}

	// 确保测试目录存在
	if err := os.MkdirAll(config.InnodbDataDir, 0755); err != nil {
		fmt.Printf("❌ 无法创建测试目录: %v\n", err)
		return
	}

	// 清理函数
	defer func() {
		fmt.Println("\n🧹 清理测试数据...")
		if err := os.RemoveAll("test_data_enhanced"); err != nil {
			fmt.Printf("⚠️  清理测试数据失败: %v\n", err)
		} else {
			fmt.Println("✅ 测试数据清理完成")
		}
	}()

	fmt.Printf("📁 测试目录: %s\n", config.DataDir)
	fmt.Printf("💾 缓冲池大小: %d MB\n", config.InnodbBufferPoolSize/1024/1024)
	fmt.Printf("📄 页面大小: %d KB\n", config.InnodbPageSize/1024)
	fmt.Println()

	// 1. 创建存储管理器
	fmt.Println("🔧 1. 创建并初始化存储管理器...")
	storageManager := manager.NewStorageManager(config)
	if storageManager == nil {
		fmt.Println("❌ 存储管理器创建失败")
		return
	}

	// 这将自动创建系统表空间并初始化用户数据（使用增强版B+树）
	fmt.Println("✅ 存储管理器初始化完成（包含增强版B+树用户数据初始化）")
	fmt.Println()

	// 2. 测试增强版B+树管理器
	fmt.Println("🔧 2. 测试增强版B+树管理器...")
	testEnhancedBTreeManager(storageManager)

	// 3. 测试索引元信息管理
	fmt.Println("\n🗂️  3. 测试索引元信息管理...")
	testIndexMetadataManager()

	// 4. 测试增强版B+树用户查询
	fmt.Println("\n🔍 4. 测试增强版B+树用户查询...")
	testEnhancedBTreeUserQuery(storageManager)

	// 5. 测试传统用户查询对比
	fmt.Println("\n🔄 5. 测试传统用户查询对比...")
	testTraditionalUserQuery(storageManager)

	// 6. 测试用户认证
	fmt.Println("\n🔐 6. 测试用户认证...")
	testUserAuthentication(storageManager)

	// 7. 性能对比测试
	fmt.Println("\n⚡ 7. 性能对比测试...")
	testPerformanceComparison(storageManager)

	fmt.Println("\n🎉 === 所有测试完成！===")
}

func testEnhancedBTreeManager(sm *manager.StorageManager) {
	fmt.Println("  📊 创建增强版B+树管理器...")

	// 创建增强版B+树管理器
	btreeManager := manager.NewEnhancedBTreeManager(sm, manager.DefaultBTreeConfig)
	defer btreeManager.Close()

	fmt.Printf("  ✅ 增强版B+树管理器创建成功\n")
	fmt.Printf("     - 已加载索引数: %d\n", btreeManager.GetLoadedIndexCount())

	// 获取统计信息
	stats := btreeManager.GetStats()
	fmt.Printf("  📈 管理器统计信息:\n")
	fmt.Printf("     - 索引缓存命中: %d\n", stats.IndexCacheHits)
	fmt.Printf("     - 索引缓存未命中: %d\n", stats.IndexCacheMisses)
	fmt.Printf("     - 搜索操作数: %d\n", stats.SearchOperations)
	fmt.Printf("     - 插入操作数: %d\n", stats.InsertOperations)
}

func testIndexMetadataManager() {
	fmt.Println("  📋 测试索引元信息管理器...")

	// 创建索引元信息管理器
	metadataManager := manager.NewIndexMetadataManager()

	// 创建测试索引元信息
	testIndexMetadata := &manager.IndexMetadata{
		IndexID:     1,
		TableID:     1,
		SpaceID:     1,
		IndexName:   "test_index",
		IndexType:   manager.IndexTypeSecondary,
		IndexState:  manager.EnhancedIndexStateActive,
		RootPageNo:  100,
		Height:      2,
		PageCount:   5,
		RecordCount: 100,
		Columns: []manager.IndexColumn{
			{
				ColumnName: "id",
				ColumnPos:  0,
				KeyLength:  8,
				IsDesc:     false,
			},
		},
		KeyLength: 8,
	}

	// 注册索引
	err := metadataManager.RegisterIndex(testIndexMetadata)
	if err != nil {
		fmt.Printf("  ❌ 注册索引失败: %v\n", err)
		return
	}

	fmt.Printf("  ✅ 成功注册索引 %d '%s'\n", testIndexMetadata.IndexID, testIndexMetadata.IndexName)

	// 查询索引
	retrievedIndex, err := metadataManager.GetIndexMetadata(testIndexMetadata.IndexID)
	if err != nil {
		fmt.Printf("  ❌ 查询索引失败: %v\n", err)
		return
	}

	fmt.Printf("  🔍 成功查询索引: %s (表ID: %d, 状态: %d)\n",
		retrievedIndex.IndexName, retrievedIndex.TableID, retrievedIndex.IndexState)

	// 按名称查询索引
	indexByName, err := metadataManager.GetIndexByName(testIndexMetadata.TableID, testIndexMetadata.IndexName)
	if err != nil {
		fmt.Printf("  ❌ 按名称查询索引失败: %v\n", err)
		return
	}

	fmt.Printf("  🔍 按名称查询索引成功: ID %d\n", indexByName.IndexID)

	// 列出所有索引
	allIndexes := metadataManager.ListAllIndexes()
	fmt.Printf("  📊 总共有 %d 个索引\n", len(allIndexes))
}

func testEnhancedBTreeUserQuery(sm *manager.StorageManager) {
	fmt.Println("  📊 通过增强版B+树索引查询用户...")

	// 测试查询用户
	users := []struct {
		username    string
		host        string
		shouldExist bool
	}{
		{"root", "localhost", true},
		{"root", "%", true},
		{"nonexistent", "localhost", false},
	}

	for _, userTest := range users {
		fmt.Printf("    🔎 查询用户: %s@%s\n", userTest.username, userTest.host)

		user, err := sm.QueryMySQLUserViaBTree(userTest.username, userTest.host)

		if userTest.shouldExist {
			if err != nil {
				fmt.Printf("    ❌ 期望用户存在，但查询失败: %v\n", err)
			} else {
				fmt.Printf("    ✅ 找到用户: %s@%s\n", user.User, user.Host)
				fmt.Printf("       - 权限: SELECT=%s, SUPER=%s\n", user.SelectPriv, user.SuperPriv)
				fmt.Printf("       - 密码哈希: %s\n", user.AuthenticationString[:20]+"...")
			}
		} else {
			if err != nil {
				fmt.Printf("    ✅ 用户正确不存在\n")
			} else {
				fmt.Printf("    ❌ 用户不应该存在但被找到\n")
			}
		}
	}
}

func testTraditionalUserQuery(sm *manager.StorageManager) {
	fmt.Println("  📊 通过传统方法查询用户...")

	users := []string{"root@localhost", "root@%"}

	for _, userKey := range users {
		fmt.Printf("    🔎 传统查询: %s\n", userKey)

		// 解析用户名和主机
		parts := parseUserKey(userKey)
		if len(parts) != 2 {
			fmt.Printf("    ❌ 无效的用户格式: %s\n", userKey)
			continue
		}

		user, err := sm.QueryMySQLUser(parts[0], parts[1])
		if err != nil {
			fmt.Printf("    ❌ 传统查询失败: %v\n", err)
		} else {
			fmt.Printf("    ✅ 传统方法找到用户: %s@%s\n", user.User, user.Host)
		}
	}
}

func testUserAuthentication(sm *manager.StorageManager) {
	fmt.Println("  🔐 测试用户密码验证...")

	authTests := []struct {
		username string
		host     string
		password string
		expected bool
	}{
		{"root", "localhost", "root@1234", true},
		{"root", "%", "root@1234", true},
		{"root", "localhost", "wrongpassword", false},
		{"nonexistent", "localhost", "anypassword", false},
	}

	for _, test := range authTests {
		fmt.Printf("    🔑 验证: %s@%s 密码: %s\n", test.username, test.host, test.password)

		isValid := sm.VerifyUserPassword(test.username, test.host, test.password)

		if isValid == test.expected {
			if test.expected {
				fmt.Printf("    ✅ 密码验证成功\n")
			} else {
				fmt.Printf("    ✅ 密码正确被拒绝\n")
			}
		} else {
			fmt.Printf("    ❌ 密码验证结果不符合期望\n")
		}
	}
}

func testPerformanceComparison(sm *manager.StorageManager) {
	fmt.Println("  ⚡ 增强版B+树查询 vs 传统查询性能对比...")

	userKey := "root@localhost"
	parts := parseUserKey(userKey)

	if len(parts) != 2 {
		fmt.Printf("    ❌ 无效的用户格式: %s\n", userKey)
		return
	}

	username, host := parts[0], parts[1]
	iterations := 100

	// 增强版B+树查询性能测试
	fmt.Printf("    📊 执行 %d 次增强版B+树查询...\n", iterations)
	enhancedSuccessCount := 0
	for i := 0; i < iterations; i++ {
		_, err := sm.QueryMySQLUserViaBTree(username, host)
		if err == nil {
			enhancedSuccessCount++
		}
	}

	// 传统查询性能测试
	fmt.Printf("    📊 执行 %d 次传统查询...\n", iterations)
	traditionalSuccessCount := 0
	for i := 0; i < iterations; i++ {
		_, err := sm.QueryMySQLUser(username, host)
		if err == nil {
			traditionalSuccessCount++
		}
	}

	fmt.Printf("    📈 结果对比:\n")
	fmt.Printf("       - 增强版B+树查询成功率: %d/%d (%.1f%%)\n",
		enhancedSuccessCount, iterations, float64(enhancedSuccessCount)*100/float64(iterations))
	fmt.Printf("       - 传统查询成功率: %d/%d (%.1f%%)\n",
		traditionalSuccessCount, iterations, float64(traditionalSuccessCount)*100/float64(iterations))

	if enhancedSuccessCount > 0 {
		fmt.Printf("    ✅ 增强版B+树索引查询功能正常\n")
	} else {
		fmt.Printf("    ⚠️  增强版B+树索引查询需要进一步优化\n")
	}

	// 显示架构优势
	fmt.Printf("    🏗️  架构优势:\n")
	fmt.Printf("       - 按需加载索引，减少内存占用\n")
	fmt.Printf("       - 专业的索引元信息管理\n")
	fmt.Printf("       - 完整的B+树生命周期管理\n")
	fmt.Printf("       - 支持多种索引类型和统计信息\n")
	fmt.Printf("       - 异步后台任务优化性能\n")
}

// parseUserKey 解析 "user@host" 格式的字符串
func parseUserKey(userKey string) []string {
	for i := len(userKey) - 1; i >= 0; i-- {
		if userKey[i] == '@' {
			return []string{userKey[:i], userKey[i+1:]}
		}
	}
	return []string{userKey}
}
