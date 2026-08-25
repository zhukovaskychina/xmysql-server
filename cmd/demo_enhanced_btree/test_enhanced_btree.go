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
	fmt.Println("馃殌 === 娴嬭瘯澧炲己鐗圔+鏍戠鐞嗗櫒鏋舵瀯 ===")
	fmt.Println()

	// 鍒涘缓閰嶇疆
	config := &conf.Cfg{
		DataDir:              "test_data_enhanced",
		InnodbDataDir:        "test_data_enhanced/innodb",
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
		if err := os.RemoveAll("test_data_enhanced"); err != nil {
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

	// 杩欏皢鑷姩鍒涘缓绯荤粺琛ㄧ┖闂村苟鍒濆鍖栫敤鎴锋暟鎹紙浣跨敤澧炲己鐗圔+鏍戯級
	fmt.Println(" 瀛樺偍绠＄悊鍣ㄥ垵濮嬪寲瀹屾垚锛堝寘鍚寮虹増B+鏍戠敤鎴锋暟鎹垵濮嬪寲锛?)
	fmt.Println()

	// 2. 娴嬭瘯澧炲己鐗圔+鏍戠鐞嗗櫒
	fmt.Println(" 2. 娴嬭瘯澧炲己鐗圔+鏍戠鐞嗗櫒...")
	testEnhancedBTreeManager(storageManager)

	// 3. 娴嬭瘯绱㈠紩鍏冧俊鎭鐞?
	fmt.Println("\n  3. 娴嬭瘯绱㈠紩鍏冧俊鎭鐞?..")
	testIndexMetadataManager()

	// 4. 娴嬭瘯澧炲己鐗圔+鏍戠敤鎴锋煡璇?
	fmt.Println("\n 4. 娴嬭瘯澧炲己鐗圔+鏍戠敤鎴锋煡璇?..")
	testEnhancedBTreeUserQuery(storageManager)

	// 5. 娴嬭瘯浼犵粺鐢ㄦ埛鏌ヨ瀵规瘮
	fmt.Println("\n馃攧 5. 娴嬭瘯浼犵粺鐢ㄦ埛鏌ヨ瀵规瘮...")
	testTraditionalUserQuery(storageManager)

	// 6. 娴嬭瘯鐢ㄦ埛璁よ瘉
	fmt.Println("\n 6. 娴嬭瘯鐢ㄦ埛璁よ瘉...")
	testUserAuthentication(storageManager)

	// 7. 鎬ц兘瀵规瘮娴嬭瘯
	fmt.Println("\n鈿?7. 鎬ц兘瀵规瘮娴嬭瘯...")
	testPerformanceComparison(storageManager)

	fmt.Println("\n馃帀 === 鎵€鏈夋祴璇曞畬鎴愶紒===")
}

func testEnhancedBTreeManager(sm *manager.StorageManager) {
	fmt.Println("   鍒涘缓澧炲己鐗圔+鏍戠鐞嗗櫒...")

	// 鍒涘缓澧炲己鐗圔+鏍戠鐞嗗櫒
	btreeManager := manager.NewEnhancedBTreeManager(sm, manager.DefaultBTreeConfig)
	defer btreeManager.Close()

	logger.Debugf("   澧炲己鐗圔+鏍戠鐞嗗櫒鍒涘缓鎴愬姛\n")
	logger.Debugf("     - 宸插姞杞界储寮曟暟: %d\n", btreeManager.GetLoadedIndexCount())

	// 鑾峰彇缁熻淇℃伅
	stats := btreeManager.GetStats()
	logger.Debugf("  馃搱 绠＄悊鍣ㄧ粺璁′俊鎭?\n")
	logger.Debugf("     - 绱㈠紩缂撳瓨鍛戒腑: %d\n", stats.IndexCacheHits)
	logger.Debugf("     - 绱㈠紩缂撳瓨鏈懡涓? %d\n", stats.IndexCacheMisses)
	logger.Debugf("     - 鎼滅储鎿嶄綔鏁? %d\n", stats.SearchOperations)
	logger.Debugf("     - 鎻掑叆鎿嶄綔鏁? %d\n", stats.InsertOperations)
}

func testIndexMetadataManager() {
	fmt.Println("   娴嬭瘯绱㈠紩鍏冧俊鎭鐞嗗櫒...")

	// 鍒涘缓绱㈠紩鍏冧俊鎭鐞嗗櫒
	metadataManager := manager.NewIndexMetadataManager()

	// 鍒涘缓娴嬭瘯绱㈠紩鍏冧俊鎭?
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

	// 娉ㄥ唽绱㈠紩
	err := metadataManager.RegisterIndex(testIndexMetadata)
	if err != nil {
		logger.Debugf("   娉ㄥ唽绱㈠紩澶辫触: %v\n", err)
		return
	}

	logger.Debugf("   鎴愬姛娉ㄥ唽绱㈠紩 %d '%s'\n", testIndexMetadata.IndexID, testIndexMetadata.IndexName)

	// 鏌ヨ绱㈠紩
	retrievedIndex, err := metadataManager.GetIndexMetadata(testIndexMetadata.IndexID)
	if err != nil {
		logger.Debugf("   鏌ヨ绱㈠紩澶辫触: %v\n", err)
		return
	}

	logger.Debugf("   鎴愬姛鏌ヨ绱㈠紩: %s (琛↖D: %d, 鐘舵€? %d)\n",
		retrievedIndex.IndexName, retrievedIndex.TableID, retrievedIndex.IndexState)

	// 鎸夊悕绉版煡璇㈢储寮?
	indexByName, err := metadataManager.GetIndexByName(testIndexMetadata.TableID, testIndexMetadata.IndexName)
	if err != nil {
		logger.Debugf("   鎸夊悕绉版煡璇㈢储寮曞け璐? %v\n", err)
		return
	}

	logger.Debugf("   鎸夊悕绉版煡璇㈢储寮曟垚鍔? ID %d\n", indexByName.IndexID)

	// 鍒楀嚭鎵€鏈夌储寮?
	allIndexes := metadataManager.ListAllIndexes()
	logger.Debugf("   鎬诲叡鏈?%d 涓储寮昞n", len(allIndexes))
}

func testEnhancedBTreeUserQuery(sm *manager.StorageManager) {
	fmt.Println("   閫氳繃澧炲己鐗圔+鏍戠储寮曟煡璇㈢敤鎴?..")

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
	fmt.Println("  鈿?澧炲己鐗圔+鏍戞煡璇?vs 浼犵粺鏌ヨ鎬ц兘瀵规瘮...")

	userKey := "root@localhost"
	parts := parseUserKey(userKey)

	if len(parts) != 2 {
		logger.Debugf("     鏃犳晥鐨勭敤鎴锋牸寮? %s\n", userKey)
		return
	}

	username, host := parts[0], parts[1]
	iterations := 100

	// 澧炲己鐗圔+鏍戞煡璇㈡€ц兘娴嬭瘯
	logger.Debugf("     鎵ц %d 娆″寮虹増B+鏍戞煡璇?..\n", iterations)
	enhancedSuccessCount := 0
	for i := 0; i < iterations; i++ {
		_, err := sm.QueryMySQLUserViaBTree(username, host)
		if err == nil {
			enhancedSuccessCount++
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
	logger.Debugf("       - 澧炲己鐗圔+鏍戞煡璇㈡垚鍔熺巼: %d/%d (%.1f%%)\n",
		enhancedSuccessCount, iterations, float64(enhancedSuccessCount)*100/float64(iterations))
	logger.Debugf("       - 浼犵粺鏌ヨ鎴愬姛鐜? %d/%d (%.1f%%)\n",
		traditionalSuccessCount, iterations, float64(traditionalSuccessCount)*100/float64(iterations))

	if enhancedSuccessCount > 0 {
		logger.Debugf("     澧炲己鐗圔+鏍戠储寮曟煡璇㈠姛鑳芥甯竆n")
	} else {
		logger.Debugf("      澧炲己鐗圔+鏍戠储寮曟煡璇㈤渶瑕佽繘涓€姝ヤ紭鍖朶n")
	}

	// 鏄剧ず鏋舵瀯浼樺娍
	logger.Debugf("      鏋舵瀯浼樺娍:\n")
	logger.Debugf("       - 鎸夐渶鍔犺浇绱㈠紩锛屽噺灏戝唴瀛樺崰鐢╘n")
	logger.Debugf("       - 涓撲笟鐨勭储寮曞厓淇℃伅绠＄悊\n")
	logger.Debugf("       - 瀹屾暣鐨凚+鏍戠敓鍛藉懆鏈熺鐞哱n")
	logger.Debugf("       - 鏀寔澶氱绱㈠紩绫诲瀷鍜岀粺璁′俊鎭痋n")
	logger.Debugf("       - 寮傛鍚庡彴浠诲姟浼樺寲鎬ц兘\n")
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
