//go:build demo
// +build demo

package main

import (
	"context"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"os"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println("=== 娴嬭瘯琛ㄧ壒瀹欱+鏍戠鐞嗗櫒鍔熻兘 ===")

	// 鍒涘缓閰嶇疆
	config := &conf.Cfg{
		DataDir:              "test_data",
		InnodbDataDir:        "test_data/innodb",
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
		InnodbBufferPoolSize: 134217728, // 128MB
		InnodbPageSize:       16384,     // 16KB
	}

	// 纭繚娴嬭瘯鐩綍瀛樺湪
	if err := os.MkdirAll(config.InnodbDataDir, 0755); err != nil {
		logger.Debugf("Failed to create test_simple_protocol directory: %v\n", err)
		return
	}

	fmt.Println("1. 鍒涘缓骞跺垵濮嬪寲XMySQL寮曟搸...")
	engine := engine.NewXMySQLEngine(config)

	// 娴嬭瘯琛ㄥ瓨鍌ㄦ槧灏?
	fmt.Println("\n2. 娴嬭瘯琛ㄥ瓨鍌ㄦ槧灏勫姛鑳?..")
	testTableStorageMapping(engine)

	// 娴嬭瘯SELECT鏌ヨ
	fmt.Println("\n3. 娴嬭瘯SELECT鏌ヨ鍔熻兘...")
	testSelectQuery(engine)

	fmt.Println("\n=== 鎵€鏈夋祴璇曞畬鎴?===")
}

func testTableStorageMapping(engine *engine.XMySQLEngine) {
	fmt.Println("姝ｅ湪娴嬭瘯琛ㄥ瓨鍌ㄦ槧灏?..")

	// 鑾峰彇瀛樺偍绠＄悊鍣?
	storageManager := getStorageManager(engine)
	if storageManager == nil {
		fmt.Println(" 鏃犳硶鑾峰彇瀛樺偍绠＄悊鍣?)
		return
	}

	// 鍒涘缓琛ㄥ瓨鍌ㄦ槧灏勭鐞嗗櫒
	tableStorageManager := manager.NewTableStorageManager(storageManager)

	// 娴嬭瘯鑾峰彇绯荤粺琛ㄤ俊鎭?
	systemTables := tableStorageManager.GetSystemTableInfo()
	logger.Debugf("鉁?鎵惧埌 %d 涓郴缁熻〃\n", len(systemTables))

	// 鏄剧ず閮ㄥ垎绯荤粺琛ㄤ俊鎭?
	for i, table := range systemTables {
		if i < 5 { // 鍙樉绀哄墠5涓?
			logger.Debugf("  - %s.%s: SpaceID=%d, RootPage=%d\n",
				table.SchemaName, table.TableName, table.SpaceID, table.RootPageNo)
		}
	}

	// 娴嬭瘯鑾峰彇mysql.user琛ㄧ殑瀛樺偍淇℃伅
	fmt.Println("\n娴嬭瘯鑾峰彇mysql.user琛ㄧ殑瀛樺偍淇℃伅...")
	userTableInfo, err := tableStorageManager.GetTableStorageInfo("mysql", "user")
	if err != nil {
		logger.Debugf(" 鑾峰彇mysql.user琛ㄥ瓨鍌ㄤ俊鎭け璐? %v\n", err)
		return
	}

	logger.Debugf("鉁?mysql.user琛ㄥ瓨鍌ㄤ俊鎭?\n")
	logger.Debugf("  - SpaceID: %d\n", userTableInfo.SpaceID)
	logger.Debugf("  - RootPage: %d\n", userTableInfo.RootPageNo)
	logger.Debugf("  - Type: %v\n", userTableInfo.Type)

	// 娴嬭瘯鍒涘缓琛ㄧ壒瀹氱殑B+鏍戠鐞嗗櫒
	fmt.Println("\n娴嬭瘯涓簃ysql.user琛ㄥ垱寤築+鏍戠鐞嗗櫒...")
	ctx := context.Background()
	userBTreeManager, err := tableStorageManager.CreateBTreeManagerForTable(ctx, "mysql", "user")
	if err != nil {
		logger.Debugf(" 鍒涘缓mysql.user琛˙+鏍戠鐞嗗櫒澶辫触: %v\n", err)
		return
	}

	fmt.Println("鉁?鎴愬姛鍒涘缓mysql.user琛ㄧ殑B+鏍戠鐞嗗櫒")

	// 娴嬭瘯鑾峰彇绗竴涓彾瀛愰〉闈?
	firstLeafPage, err := userBTreeManager.GetFirstLeafPage(ctx)
	if err != nil {
		logger.Debugf(" 鑾峰彇绗竴涓彾瀛愰〉闈㈠け璐? %v\n", err)
	} else {
		logger.Debugf("鉁?绗竴涓彾瀛愰〉闈? %d\n", firstLeafPage)
	}

	// 娴嬭瘯鑾峰彇鎵€鏈夊彾瀛愰〉闈?
	leafPages, err := userBTreeManager.GetAllLeafPages(ctx)
	if err != nil {
		logger.Debugf(" 鑾峰彇鎵€鏈夊彾瀛愰〉闈㈠け璐? %v\n", err)
	} else {
		logger.Debugf("鉁?鎬诲叡鏈?%d 涓彾瀛愰〉闈n", len(leafPages))
		if len(leafPages) > 0 {
			logger.Debugf("  鍙跺瓙椤甸潰: %v\n", leafPages)
		}
	}
}

func testSelectQuery(engine *engine.XMySQLEngine) {
	fmt.Println("姝ｅ湪娴嬭瘯SELECT鏌ヨ...")

	// 鑾峰彇鏌ヨ鎵ц鍣?
	queryExecutor := getQueryExecutor(engine)
	if queryExecutor == nil {
		fmt.Println(" 鏃犳硶鑾峰彇鏌ヨ鎵ц鍣?)
		return
	}

	// 娴嬭瘯瑙ｆ瀽鍜屾墽琛孲ELECT璇彞
	sql := "SELECT * FROM user"
	logger.Debugf("鎵цSQL: %s\n", sql)

	_, err := sqlparser.Parse(sql)
	if err != nil {
		logger.Debugf(" SQL瑙ｆ瀽澶辫触: %v\n", err)
		return
	}

}

// getStorageManager 鑾峰彇瀛樺偍绠＄悊鍣紙浣跨敤鍙嶅皠鎴栫被鍨嬫柇瑷€锛?
func getStorageManager(engine *engine.XMySQLEngine) *manager.StorageManager {
	// 杩欓噷闇€瑕佹牴鎹疄闄呯殑engine缁撴瀯鏉ヨ幏鍙栧瓨鍌ㄧ鐞嗗櫒
	// 鐢变簬娌℃湁鐩存帴鐨刾ublic鏂规硶锛岃繖閲屾槸涓€涓畝鍖栫殑瀹炵幇
	// 瀹為檯椤圭洰涓彲鑳介渶瑕佹坊鍔爂etter鏂规硶
	return nil // 鏆傛椂杩斿洖nil锛岄渶瑕佹牴鎹疄闄卐ngine缁撴瀯瀹炵幇
}

// getQueryExecutor 鑾峰彇鏌ヨ鎵ц鍣?
func getQueryExecutor(engine *engine.XMySQLEngine) interface{} {
	// 鍚屾牱闇€瑕佹牴鎹疄闄呯殑engine缁撴瀯鏉ヨ幏鍙栨煡璇㈡墽琛屽櫒
	return nil // 鏆傛椂杩斿洖nil锛岄渶瑕佹牴鎹疄闄卐ngine缁撴瀯瀹炵幇
}

// 涓存椂鐨勬祴璇曞疄鐜帮紝妯℃嫙瀛樺偍绠＄悊鍣ㄥ姛鑳?
func createTestStorageManager() *manager.StorageManager {
	fmt.Println("鍒涘缓娴嬭瘯瀛樺偍绠＄悊鍣?..")

	// 杩欓噷搴旇鍒涘缓涓€涓湡瀹炵殑瀛樺偍绠＄悊鍣?
	// 浣嗙敱浜庝緷璧栧鏉傦紝鍏堝垱寤轰竴涓ā鎷熺増鏈敤浜庢紨绀?
	return nil
}
