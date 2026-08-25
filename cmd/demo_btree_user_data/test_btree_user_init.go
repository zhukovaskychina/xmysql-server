//go:build demo
// +build demo

package main

import (
	"fmt"
	"os"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println("馃殌 === 娴嬭瘯B+鏍戠储寮曠殑MySQL鐢ㄦ埛鏁版嵁鍒濆鍖?===")
	fmt.Println()

	// 鍒涘缓閰嶇疆
	config := &conf.Cfg{
		DataDir:              "test_data_btree",
		InnodbDataDir:        "test_data_btree/innodb",
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
		InnodbBufferPoolSize: 134217728, // 128MB
		InnodbPageSize:       16384,     // 16KB
	}

	// 纭繚娴嬭瘯鐩綍瀛樺湪
	if err := os.MkdirAll(config.InnodbDataDir, 0755); err != nil {
		logger.Debugf(" 鏃犳硶鍒涘缓娴嬭瘯鐩綍: %v\n", err)
		return
	}

	// 娓呯悊鍑芥暟
	defer func() {
		fmt.Println("\n馃Ч 娓呯悊娴嬭瘯鏁版嵁...")
		if err := os.RemoveAll("test_data_btree"); err != nil {
			logger.Debugf("  娓呯悊娴嬭瘯鏁版嵁澶辫触: %v\n", err)
		} else {
			fmt.Println(" 娴嬭瘯鏁版嵁娓呯悊瀹屾垚")
		}
	}()

	logger.Debugf("馃搧 娴嬭瘯鐩綍: %s\n", config.DataDir)
	logger.Debugf("馃捑 缂撳啿姹犲ぇ灏? %d MB\n", config.InnodbBufferPoolSize/1024/1024)
	logger.Debugf("馃搫 椤甸潰澶у皬: %d KB\n", config.InnodbPageSize/1024)
	fmt.Println()

	// 1. 鍒涘缓瀛樺偍绠＄悊鍣?
	fmt.Println(" 1. 鍒涘缓骞跺垵濮嬪寲瀛樺偍绠＄悊鍣?..")
	storageManager := manager.NewStorageManager(config)
	if storageManager == nil {
		fmt.Println(" 瀛樺偍绠＄悊鍣ㄥ垱寤哄け璐?)
		return
	}

	// 杩欏皢鑷姩鍒涘缓绯荤粺琛ㄧ┖闂村苟鍒濆鍖栫敤鎴锋暟鎹紙鏂扮殑B+鏍戠増鏈級
	fmt.Println(" 瀛樺偍绠＄悊鍣ㄥ垵濮嬪寲瀹屾垚锛堝寘鍚獴+鏍戠敤鎴锋暟鎹垵濮嬪寲锛?)
	fmt.Println()

	// 2. 娴嬭瘯B+鏍戠敤鎴锋煡璇?
	fmt.Println(" 2. 娴嬭瘯B+鏍戠敤鎴锋煡璇?..")
	testBTreeUserQuery(storageManager)

	// 3. 娴嬭瘯浼犵粺鐢ㄦ埛鏌ヨ瀵规瘮
	fmt.Println("\n馃攧 3. 娴嬭瘯浼犵粺鐢ㄦ埛鏌ヨ瀵规瘮...")
	testTraditionalUserQuery(storageManager)

	// 4. 娴嬭瘯鐢ㄦ埛璁よ瘉
	fmt.Println("\n 4. 娴嬭瘯鐢ㄦ埛璁よ瘉...")
	testUserAuthentication(storageManager)

	// 5. 鎬ц兘瀵规瘮娴嬭瘯
	fmt.Println("\n鈿?5. 鎬ц兘瀵规瘮娴嬭瘯...")
	testPerformanceComparison(storageManager)

	fmt.Println("\n馃帀 === 鎵€鏈夋祴璇曞畬鎴愶紒===")
}

func testBTreeUserQuery(sm *manager.StorageManager) {
	fmt.Println("   閫氳繃B+鏍戠储寮曟煡璇㈢敤鎴?..")

	// 娴嬭瘯鏌ヨ鐢ㄦ埛
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
		logger.Debugf("    馃攷 鏌ヨ鐢ㄦ埛: %s@%s\n", userTest.username, userTest.host)

		user, err := sm.QueryMySQLUserViaBTree(userTest.username, userTest.host)

		if userTest.shouldExist {
			if err != nil {
				logger.Debugf("     鏈熸湜鐢ㄦ埛瀛樺湪锛屼絾鏌ヨ澶辫触: %v\n", err)
			} else {
				logger.Debugf("     鎵惧埌鐢ㄦ埛: %s@%s\n", user.User, user.Host)
				logger.Debugf("       - 鏉冮檺: SELECT=%s, SUPER=%s\n", user.SelectPriv, user.SuperPriv)
				logger.Debugf("       - 瀵嗙爜鍝堝笇: %s\n", user.AuthenticationString[:20]+"...")
			}
		} else {
			if err != nil {
				logger.Debugf("     鐢ㄦ埛姝ｇ‘涓嶅瓨鍦╘n")
			} else {
				logger.Debugf("     鐢ㄦ埛涓嶅簲璇ュ瓨鍦ㄤ絾琚壘鍒癨n")
			}
		}
	}
}

func testTraditionalUserQuery(sm *manager.StorageManager) {
	fmt.Println("   閫氳繃浼犵粺鏂规硶鏌ヨ鐢ㄦ埛...")

	users := []string{"root@localhost", "root@%"}

	for _, userKey := range users {
		logger.Debugf("    馃攷 浼犵粺鏌ヨ: %s\n", userKey)

		// 瑙ｆ瀽鐢ㄦ埛鍚嶅拰涓绘満
		parts := parseUserKey(userKey)
		if len(parts) != 2 {
			logger.Debugf("     鏃犳晥鐨勭敤鎴锋牸寮? %s\n", userKey)
			continue
		}

		user, err := sm.QueryMySQLUser(parts[0], parts[1])
		if err != nil {
			logger.Debugf("     浼犵粺鏌ヨ澶辫触: %v\n", err)
		} else {
			logger.Debugf("     浼犵粺鏂规硶鎵惧埌鐢ㄦ埛: %s@%s\n", user.User, user.Host)
		}
	}
}

func testUserAuthentication(sm *manager.StorageManager) {
	fmt.Println("   娴嬭瘯鐢ㄦ埛瀵嗙爜楠岃瘉...")

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
		logger.Debugf("    馃攽 楠岃瘉: %s@%s 瀵嗙爜: %s\n", test.username, test.host, test.password)

		isValid := sm.VerifyUserPassword(test.username, test.host, test.password)

		if isValid == test.expected {
			if test.expected {
				logger.Debugf("     瀵嗙爜楠岃瘉鎴愬姛\n")
			} else {
				logger.Debugf("     瀵嗙爜姝ｇ‘琚嫆缁漒n")
			}
		} else {
			logger.Debugf("     瀵嗙爜楠岃瘉缁撴灉涓嶇鍚堟湡鏈沑n")
		}
	}
}

func testPerformanceComparison(sm *manager.StorageManager) {
	fmt.Println("  鈿?B+鏍戞煡璇?vs 浼犵粺鏌ヨ鎬ц兘瀵规瘮...")

	userKey := "root@localhost"
	parts := parseUserKey(userKey)

	if len(parts) != 2 {
		logger.Debugf("     鏃犳晥鐨勭敤鎴锋牸寮? %s\n", userKey)
		return
	}

	username, host := parts[0], parts[1]
	iterations := 100

	// B+鏍戞煡璇㈡€ц兘娴嬭瘯
	logger.Debugf("     鎵ц %d 娆+鏍戞煡璇?..\n", iterations)
	btreeSuccessCount := 0
	for i := 0; i < iterations; i++ {
		_, err := sm.QueryMySQLUserViaBTree(username, host)
		if err == nil {
			btreeSuccessCount++
		}
	}

	// 浼犵粺鏌ヨ鎬ц兘娴嬭瘯
	logger.Debugf("     鎵ц %d 娆′紶缁熸煡璇?..\n", iterations)
	traditionalSuccessCount := 0
	for i := 0; i < iterations; i++ {
		_, err := sm.QueryMySQLUser(username, host)
		if err == nil {
			traditionalSuccessCount++
		}
	}

	logger.Debugf("    馃搱 缁撴灉瀵规瘮:\n")
	logger.Debugf("       - B+鏍戞煡璇㈡垚鍔熺巼: %d/%d (%.1f%%)\n",
		btreeSuccessCount, iterations, float64(btreeSuccessCount)*100/float64(iterations))
	logger.Debugf("       - 浼犵粺鏌ヨ鎴愬姛鐜? %d/%d (%.1f%%)\n",
		traditionalSuccessCount, iterations, float64(traditionalSuccessCount)*100/float64(iterations))

	if btreeSuccessCount > 0 {
		logger.Debugf("     B+鏍戠储寮曟煡璇㈠姛鑳芥甯竆n")
	} else {
		logger.Debugf("      B+鏍戠储寮曟煡璇㈤渶瑕佽繘涓€姝ヤ紭鍖朶n")
	}
}

// parseUserKey 瑙ｆ瀽 "user@host" 鏍煎紡鐨勫瓧绗︿覆
func parseUserKey(userKey string) []string {
	for i := len(userKey) - 1; i >= 0; i-- {
		if userKey[i] == '@' {
			return []string{userKey[:i], userKey[i+1:]}
		}
	}
	return []string{userKey}
}
