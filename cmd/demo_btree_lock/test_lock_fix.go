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
	fmt.Println("=== 娴嬭瘯閿佷慨澶?===")

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
		fmt.Println(" 缂撳啿姹犵鐞嗗櫒涓虹┖")
		return
	}

	fmt.Println("3. 鍒涘缓琛ㄥ瓨鍌ㄦ槧灏勭鐞嗗櫒...")
	tableStorageManager := manager.NewTableStorageManager(storageManager)

	fmt.Println("4. 娴嬭瘯鑾峰彇琛ㄤ俊鎭?..")
	userTableInfo, err := tableStorageManager.GetTableStorageInfo("mysql", "user")
	if err != nil {
		logger.Debugf(" 鑾峰彇mysql.user琛ㄥ瓨鍌ㄤ俊鎭け璐? %v\n", err)
		return
	}

	logger.Debugf("鉁?mysql.user琛ㄥ瓨鍌ㄤ俊鎭? SpaceID=%d, RootPage=%d\n",
		userTableInfo.SpaceID, userTableInfo.RootPageNo)

	fmt.Println("5. 娴嬭瘯鍒涘缓B+鏍戠鐞嗗櫒...")
	ctx := context.Background()
	userBTreeManager, err := tableStorageManager.CreateBTreeManagerForTable(ctx, "mysql", "user")
	if err != nil {
		logger.Debugf(" 鍒涘缓mysql.user琛˙+鏍戠鐞嗗櫒澶辫触: %v\n", err)
		return
	}

	fmt.Println("鉁?鎴愬姛鍒涘缓mysql.user琛ㄧ殑B+鏍戠鐞嗗櫒")

	fmt.Println("6. 娴嬭瘯B+鏍戝熀鏈搷浣滐紙搴旇涓嶄細鏈夐攣闂锛?..")

	// 娴嬭瘯鑾峰彇绗竴涓彾瀛愰〉闈?
	fmt.Println("  娴嬭瘯GetFirstLeafPage...")
	firstLeafPage, err := userBTreeManager.GetFirstLeafPage(ctx)
	if err != nil {
		logger.Debugf(" 鑾峰彇绗竴涓彾瀛愰〉闈㈠け璐ワ紙鍙兘鏄鏈熺殑锛? %v\n", err)
	} else {
		logger.Debugf("鉁?绗竴涓彾瀛愰〉闈? %d\n", firstLeafPage)
	}

	// 娴嬭瘯鑾峰彇鎵€鏈夊彾瀛愰〉闈?
	fmt.Println("  娴嬭瘯GetAllLeafPages...")
	leafPages, err := userBTreeManager.GetAllLeafPages(ctx)
	if err != nil {
		logger.Debugf(" 鑾峰彇鎵€鏈夊彾瀛愰〉闈㈠け璐ワ紙鍙兘鏄鏈熺殑锛? %v\n", err)
	} else {
		logger.Debugf("鉁?鎬诲叡鏈?%d 涓彾瀛愰〉闈n", len(leafPages))
		if len(leafPages) > 0 && len(leafPages) <= 5 {
			logger.Debugf("  鍙跺瓙椤甸潰: %v\n", leafPages)
		}
	}

	// 娴嬭瘯鎼滅储鍔熻兘
	fmt.Println("  娴嬭瘯Search...")
	pageNum, slot, err := userBTreeManager.Search(ctx, "root")
	if err != nil {
		logger.Debugf(" 鎼滅储澶辫触锛堝彲鑳芥槸棰勬湡鐨勶級: %v\n", err)
	} else {
		logger.Debugf("鉁?鎵惧埌璁板綍: PageNum=%d, Slot=%d\n", pageNum, slot)
	}

	fmt.Println("\n=== 閿佷慨澶嶆祴璇曞畬鎴?===")
	fmt.Println("鉁?娌℃湁閬囧埌sync: RUnlock of unlocked RWMutex閿欒")
	fmt.Println("鉁?B+鏍戠鐞嗗櫒閿佷娇鐢ㄦ甯?)
	fmt.Println("鉁?琛ㄥ瓨鍌ㄦ槧灏勫姛鑳芥甯?)
}
