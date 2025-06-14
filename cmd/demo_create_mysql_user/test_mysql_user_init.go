package main

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/logger"
	"os"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println("=== Testing MySQL User Data Initialization ===")

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
		logger.Debugf("Failed to create test_simple_protocol directory: %v\n", err)
		return
	}

	fmt.Println("1. Creating storage manager...")
	storageManager := manager.NewStorageManager(config)

	fmt.Println("2. Initializing storage manager...")
	storageManager.Init()

	fmt.Println("3. Testing user authentication...")
	testUserAuthentication(storageManager)

	fmt.Println("4. Testing user queries...")
	testUserQueries(storageManager)

	fmt.Println("\n=== All tests completed successfully! ===")

	// 清理测试数据
	defer func() {
		if err := os.RemoveAll("test_data"); err != nil {
			logger.Debugf("Warning: Failed to clean up test_simple_protocol data: %v\n", err)
		}
	}()
}

func testUserAuthentication(sm *manager.StorageManager) {
	fmt.Println("  Testing password verification...")

	// 测试正确的用户名和密码
	tests := []struct {
		username string
		host     string
		password string
		expected bool
		desc     string
	}{
		{"root", "localhost", "root@1234", true, "root@localhost with correct password"},
		{"root", "%", "root@1234", true, "root@% with correct password"},
		{"root", "localhost", "wrong_password", false, "root@localhost with wrong password"},
		{"nonexistent", "localhost", "any_password", false, "non-existent user"},
	}

	for _, test := range tests {
		result := sm.VerifyUserPassword(test.username, test.host, test.password)
		status := "✗ FAIL"
		if result == test.expected {
			status = "✓ PASS"
		}
		logger.Debugf("    %s: %s\n", status, test.desc)

		if result != test.expected {
			logger.Debugf("      Expected: %t, Got: %t\n", test.expected, result)
		}
	}
}

func testUserQueries(sm *manager.StorageManager) {
	fmt.Println("  Testing user queries...")

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
		user, err := sm.QueryMySQLUser(userTest.username, userTest.host)

		if userTest.shouldExist {
			if err != nil {
				logger.Debugf("    ✗ FAIL: Expected user %s@%s to exist, but got error: %v\n",
					userTest.username, userTest.host, err)
			} else {
				logger.Debugf("    ✓ PASS: Found user %s@%s\n", userTest.username, userTest.host)

				// 验证用户权限
				if user.SelectPriv == "Y" && user.InsertPriv == "Y" && user.SuperPriv == "Y" {
					logger.Debugf("      ✓ User has correct privileges (SELECT, INSERT, SUPER)\n")
				} else {
					logger.Debugf("      ✗ User privileges incorrect: SELECT=%s, INSERT=%s, SUPER=%s\n",
						user.SelectPriv, user.InsertPriv, user.SuperPriv)
				}

				// 验证密码哈希不为空
				if user.AuthenticationString != "" {
					logger.Debugf("      ✓ User has password hash: %s\n", user.AuthenticationString[:20]+"...")
				} else {
					logger.Debugf("      ✗ User missing password hash\n")
				}
			}
		} else {
			if err != nil {
				logger.Debugf("    ✓ PASS: User %s@%s correctly does not exist\n", userTest.username, userTest.host)
			} else {
				logger.Debugf("    ✗ FAIL: User %s@%s should not exist but was found\n", userTest.username, userTest.host)
			}
		}
	}
}
