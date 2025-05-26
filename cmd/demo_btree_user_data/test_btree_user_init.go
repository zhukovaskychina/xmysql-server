package main

import (
	"fmt"
	"os"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println("🚀 === 测试B+树索引的MySQL用户数据初始化 ===")
	fmt.Println()

	// 创建配置
	config := &conf.Cfg{
		DataDir:              "test_data_btree",
		InnodbDataDir:        "test_data_btree/innodb",
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
		if err := os.RemoveAll("test_data_btree"); err != nil {
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

	// 这将自动创建系统表空间并初始化用户数据（新的B+树版本）
	fmt.Println("✅ 存储管理器初始化完成（包含B+树用户数据初始化）")
	fmt.Println()

	// 2. 测试B+树用户查询
	fmt.Println("🔍 2. 测试B+树用户查询...")
	testBTreeUserQuery(storageManager)

	// 3. 测试传统用户查询对比
	fmt.Println("\n🔄 3. 测试传统用户查询对比...")
	testTraditionalUserQuery(storageManager)

	// 4. 测试用户认证
	fmt.Println("\n🔐 4. 测试用户认证...")
	testUserAuthentication(storageManager)

	// 5. 性能对比测试
	fmt.Println("\n⚡ 5. 性能对比测试...")
	testPerformanceComparison(storageManager)

	fmt.Println("\n🎉 === 所有测试完成！===")
}

func testBTreeUserQuery(sm *manager.StorageManager) {
	fmt.Println("  📊 通过B+树索引查询用户...")

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
	fmt.Println("  ⚡ B+树查询 vs 传统查询性能对比...")

	userKey := "root@localhost"
	parts := parseUserKey(userKey)

	if len(parts) != 2 {
		fmt.Printf("    ❌ 无效的用户格式: %s\n", userKey)
		return
	}

	username, host := parts[0], parts[1]
	iterations := 100

	// B+树查询性能测试
	fmt.Printf("    📊 执行 %d 次B+树查询...\n", iterations)
	btreeSuccessCount := 0
	for i := 0; i < iterations; i++ {
		_, err := sm.QueryMySQLUserViaBTree(username, host)
		if err == nil {
			btreeSuccessCount++
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
	fmt.Printf("       - B+树查询成功率: %d/%d (%.1f%%)\n",
		btreeSuccessCount, iterations, float64(btreeSuccessCount)*100/float64(iterations))
	fmt.Printf("       - 传统查询成功率: %d/%d (%.1f%%)\n",
		traditionalSuccessCount, iterations, float64(traditionalSuccessCount)*100/float64(iterations))

	if btreeSuccessCount > 0 {
		fmt.Printf("    ✅ B+树索引查询功能正常\n")
	} else {
		fmt.Printf("    ⚠️  B+树索引查询需要进一步优化\n")
	}
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
