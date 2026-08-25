//go:build demo
// +build demo

package main

import (
	"context"
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println("=== 娴嬭瘯B+鏍戠鐞嗗櫒鏋勯€犲嚱鏁颁慨澶?===")

	// 鍒涘缓閰嶇疆
	config := &conf.Cfg{
		DataDir:              "test_data",
		InnodbDataDir:        "test_data/innodb",
		InnodbDataFilePath:   "ibdata1:100M:autoextend",
		InnodbBufferPoolSize: 134217728, // 128MB
		InnodbPageSize:       16384,     // 16KB
	}

	fmt.Println("1. 鍒涘缓瀛樺偍绠＄悊鍣?..")
	storageManager := manager.NewStorageManager(config)

	fmt.Println("2. 鑾峰彇缂撳啿姹犵鐞嗗櫒...")
	bufferPoolManager := storageManager.GetBufferPoolManager()
	if bufferPoolManager == nil {
		fmt.Println(" 缂撳啿姹犵鐞嗗櫒涓虹┖锛屼娇鐢ㄦā鎷熷疄鐜?)
		return
	}

	fmt.Println("3. 鍒涘缓琛ㄥ瓨鍌ㄦ槧灏勭鐞嗗櫒...")
	tableStorageManager := manager.NewTableStorageManager(storageManager)

	fmt.Println("4. 娴嬭瘯涓鸿〃鍒涘缓B+鏍戠鐞嗗櫒...")

	// 娴嬭瘯鑾峰彇mysql.user琛ㄧ殑瀛樺偍淇℃伅
	userTableInfo, err := tableStorageManager.GetTableStorageInfo("mysql", "user")
	if err != nil {
		logger.Debugf(" 鑾峰彇mysql.user琛ㄥ瓨鍌ㄤ俊鎭け璐? %v\n", err)
		return
	}

	logger.Debugf("鉁?mysql.user琛ㄥ瓨鍌ㄤ俊鎭? SpaceID=%d, RootPage=%d\n",
		userTableInfo.SpaceID, userTableInfo.RootPageNo)

	// 娴嬭瘯鍒涘缓琛ㄧ壒瀹氱殑B+鏍戠鐞嗗櫒
	ctx := context.Background()
	userBTreeManager, err := tableStorageManager.CreateBTreeManagerForTable(ctx, "mysql", "user")
	if err != nil {
		logger.Debugf(" 鍒涘缓mysql.user琛˙+鏍戠鐞嗗櫒澶辫触: %v\n", err)
		return
	}

	fmt.Println("鉁?鎴愬姛鍒涘缓mysql.user琛ㄧ殑澧炲己鐗圔+鏍戠鐞嗗櫒")

	// 娴嬭瘯B+鏍戠鐞嗗櫒鐨勫熀鏈姛鑳?
	fmt.Println("\n5. 娴嬭瘯澧炲己鐗圔+鏍戠鐞嗗櫒鍩烘湰鍔熻兘...")

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
		logger.Debugf("鉁?鍙跺瓙椤甸潰鏁伴噺: %d\n", len(leafPages))
		if len(leafPages) > 0 {
			logger.Debugf("  鍙跺瓙椤甸潰: %v\n", leafPages)
		}
	}

	// 娴嬭瘯鎻掑叆鏁版嵁
	fmt.Println("\n6. 娴嬭瘯鎻掑叆鏁版嵁鍒板寮虹増B+鏍?..")
	testKey := "test_user"
	testValue := []byte("test_user_data")

	err = userBTreeManager.Insert(ctx, testKey, testValue)
	if err != nil {
		logger.Debugf(" 鎻掑叆鏁版嵁澶辫触: %v\n", err)
	} else {
		logger.Debugf("鉁?鎴愬姛鎻掑叆鏁版嵁: key=%s\n", testKey)
	}

	// 娴嬭瘯鎼滅储鏁版嵁
	fmt.Println("\n7. 娴嬭瘯鎼滅储鏁版嵁...")
	pageNo, slot, err := userBTreeManager.Search(ctx, testKey)
	if err != nil {
		logger.Debugf(" 鎼滅储鏁版嵁澶辫触: %v\n", err)
	} else {
		logger.Debugf("鉁?鎵惧埌鏁版嵁: page=%d, slot=%d\n", pageNo, slot)
	}

	// 娴嬭瘯鑼冨洿鏌ヨ
	fmt.Println("\n8. 娴嬭瘯鑼冨洿鏌ヨ...")
	rows, err := userBTreeManager.RangeSearch(ctx, "a", "z")
	if err != nil {
		logger.Debugf(" 鑼冨洿鏌ヨ澶辫触: %v\n", err)
	} else {
		logger.Debugf("鉁?鑼冨洿鏌ヨ缁撴灉鏁伴噺: %d\n", len(rows))
	}

	fmt.Println("\n 澧炲己鐗圔+鏍戠鐞嗗櫒娴嬭瘯瀹屾垚锛?)

	fmt.Println("\n=== 娴嬭瘯瀹屾垚 ===")
	fmt.Println("鉁?B+鏍戠鐞嗗櫒鏋勯€犲嚱鏁颁慨澶嶆垚鍔?)
	fmt.Println("鉁?琛ㄥ瓨鍌ㄦ槧灏勭鐞嗗櫒宸ヤ綔姝ｅ父")
	fmt.Println("鉁?鍙互鎴愬姛涓虹壒瀹氳〃鍒涘缓B+鏍戠鐞嗗櫒")
}
