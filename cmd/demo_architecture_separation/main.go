//go:build demo
// +build demo

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println("=" + strings.Repeat("=", 60))
	fmt.Println("  XMySQL InnoDB 瀛樺偍鏋舵瀯鑱岃矗鍒嗙婕旂ず")
	fmt.Println("=" + strings.Repeat("=", 60))

	// 鍒涘缓婕旂ず鐩綍
	demoDir := "demo_architecture_separation"
	os.RemoveAll(demoDir) // 娓呯悊涔嬪墠鐨勬暟鎹?
	os.MkdirAll(demoDir, 0755)
	defer func() {
		fmt.Println("\n馃Ч 娓呯悊婕旂ず鏁版嵁...")
		os.RemoveAll(demoDir)
	}()

	// 鍒涘缓婕旂ず閰嶇疆
	cfg := &conf.Cfg{
		DataDir:              demoDir,
		InnodbDataDir:        demoDir,
		InnodbDataFilePath:   "ibdata1:50M:autoextend",
		InnodbBufferPoolSize: 16777216, // 16MB
		InnodbPageSize:       16384,    // 16KB
		InnodbLogFileSize:    10485760, // 10MB
		InnodbLogBufferSize:  1048576,  // 1MB
	}

	fmt.Println("\n 婕旂ず閰嶇疆:")
	logger.Debugf("  - 鏁版嵁鐩綍: %s\n", cfg.DataDir)
	logger.Debugf("  - 绯荤粺琛ㄧ┖闂? %s\n", cfg.InnodbDataFilePath)
	logger.Debugf("  - 缂撳啿姹犲ぇ灏? %d MB\n", cfg.InnodbBufferPoolSize/1024/1024)

	// 绗竴娆¤繍琛岋細鍒涘缓鍏ㄦ柊鐨勫瓨鍌ㄧ郴缁?
	fmt.Println("\n馃殌 绗竴娆¤繍琛岋細鍒涘缓鍏ㄦ柊鐨勫瓨鍌ㄧ郴缁?)
	fmt.Println(strings.Repeat("-", 50))

	storageManager1 := manager.NewStorageManager(cfg)
	if storageManager1 == nil {
		logger.Debugf(" 鍒涘缓StorageManager澶辫触\n")
		return
	}

	fmt.Println("\n 绗竴娆¤繍琛屽悗鐨勭姸鎬?")
	displayManagerStatus(storageManager1, cfg)

	// 鍏抽棴绗竴涓疄渚?
	fmt.Println("\n馃攧 鍏抽棴StorageManager...")
	storageManager1.Close()

	// 绗簩娆¤繍琛岋細閲嶆柊鎵撳紑宸插瓨鍦ㄧ殑瀛樺偍绯荤粺
	fmt.Println("\n馃攧 绗簩娆¤繍琛岋細閲嶆柊鎵撳紑宸插瓨鍦ㄧ殑瀛樺偍绯荤粺")
	fmt.Println(strings.Repeat("-", 50))

	storageManager2 := manager.NewStorageManager(cfg)
	if storageManager2 == nil {
		logger.Debugf(" 閲嶆柊鎵撳紑StorageManager澶辫触\n")
		return
	}

	fmt.Println("\n 绗簩娆¤繍琛屽悗鐨勭姸鎬?")
	displayManagerStatus(storageManager2, cfg)

	// 灞曠ず鑱岃矗鍒嗙
	fmt.Println("\n 鑱岃矗鍒嗙婕旂ず:")
	fmt.Println(strings.Repeat("-", 50))
	demonstrateResponsibilitySeparation(storageManager2)

	// 鍏抽棴绗簩涓疄渚?
	fmt.Println("\n馃攧 鍏抽棴StorageManager...")
	storageManager2.Close()

	fmt.Println("\n 鏋舵瀯鑱岃矗鍒嗙婕旂ず瀹屾垚!")
}

func displayManagerStatus(sm *manager.StorageManager, cfg *conf.Cfg) {
	// 妫€鏌paceManager鐘舵€?
	spaceManager := sm.GetSpaceManager()
	if spaceManager != nil {
		fmt.Println("   SpaceManager: 姝ｅ父杩愯")

		// 妫€鏌ョ郴缁熻〃绌洪棿
		if systemSpace, err := spaceManager.GetSpace(0); err == nil {
			logger.Debugf("    - 绯荤粺琛ㄧ┖闂?Space ID 0): %s\n",
				getSpaceStatus(systemSpace))
		}

		// 妫€鏌ラ儴鍒嗙敤鎴疯〃绌洪棿
		userSpaces := []uint32{1, 2, 3, 100, 200}
		existingSpaces := 0
		for _, spaceID := range userSpaces {
			if userSpace, err := spaceManager.GetSpace(spaceID); err == nil {
				if existingSpaces < 3 { // 鍙樉绀哄墠3涓?
					logger.Debugf("    - 鐢ㄦ埛琛ㄧ┖闂?Space ID %d): %s\n",
						spaceID, getSpaceStatus(userSpace))
				}
				existingSpaces++
			}
		}
		if existingSpaces > 3 {
			logger.Debugf("    - ... 杩樻湁 %d 涓敤鎴疯〃绌洪棿\n", existingSpaces-3)
		}
	} else {
		fmt.Println("   SpaceManager: 鏈垵濮嬪寲")
	}

	// 妫€鏌ュ叾浠栫鐞嗗櫒鐘舵€?
	fmt.Println("   SegmentManager: 姝ｅ父杩愯")
	if sm.GetSegmentManager() != nil {
		fmt.Println("    - 娈电鐞嗗姛鑳? 宸插垵濮嬪寲")
	}

	fmt.Println("   BufferPoolManager: 姝ｅ父杩愯")
	if bpm := sm.GetBufferPoolManager(); bpm != nil {
		fmt.Println("    - 缂撳啿姹? 宸插垵濮嬪寲骞朵紭鍖?)
	}

	// 鏄剧ず琛ㄧ┖闂寸紦瀛樼姸鎬?
	if spaces, err := sm.ListSpaces(); err == nil {
		logger.Debugf("   琛ㄧ┖闂寸紦瀛? %d 涓〃绌洪棿\n", len(spaces))
		systemSpaces := 0
		userSpaces := 0
		for _, space := range spaces {
			if space.SpaceID < 100 {
				systemSpaces++
			} else {
				userSpaces++
			}
		}
		logger.Debugf("    - 绯荤粺琛ㄧ┖闂? %d 涓猏n", systemSpaces)
		logger.Debugf("    - 鐢ㄦ埛琛ㄧ┖闂? %d 涓猏n", userSpaces)
	}

	// 鏄剧ず鏂囦欢鐘舵€?
	fmt.Println("  馃搧 鏂囦欢绯荤粺鐘舵€?")
	files, _ := filepath.Glob(filepath.Join(cfg.DataDir, "*.ibd"))
	logger.Debugf("    - IBD鏂囦欢鏁伴噺: %d\n", len(files))
	for _, file := range files[:min(5, len(files))] { // 鍙樉绀哄墠5涓?
		basename := filepath.Base(file)
		if info, err := os.Stat(file); err == nil {
			logger.Debugf("    - %s: %d KB\n", basename, info.Size()/1024)
		}
	}
	if len(files) > 5 {
		logger.Debugf("    - ... 杩樻湁 %d 涓枃浠禱n", len(files)-5)
	}
}

func demonstrateResponsibilitySeparation(sm *manager.StorageManager) {
	fmt.Println("1. 馃梽锔? SpaceManager鑱岃矗婕旂ず:")
	spaceManager := sm.GetSpaceManager()
	if spaceManager != nil {
		fmt.Println("   - 绠＄悊鎵€鏈塈BD鏂囦欢(鍖呮嫭绯荤粺琛ㄧ┖闂磗pace_id=0)")
		fmt.Println("   - 璐熻矗琛ㄧ┖闂寸殑鍒涘缓銆佹墦寮€銆佸叧闂€佸垹闄?)
		fmt.Println("   - 澶勭悊鍖烘鍒嗛厤鍜岄〉闈/O鎿嶄綔")

		// 婕旂ず鍒涘缓鏂拌〃绌洪棿
		testSpaceID := uint32(999)
		logger.Debugf("   - 灏濊瘯鍒涘缓娴嬭瘯琛ㄧ┖闂?Space ID %d)...\n", testSpaceID)

		if testSpace, err := spaceManager.CreateSpace(testSpaceID, "test_table", false); err == nil {
			logger.Debugf("    SpaceManager鎴愬姛鍒涘缓琛ㄧ┖闂? %s\n", getSpaceStatus(testSpace))
		} else {
			logger.Debugf("     琛ㄧ┖闂村垱寤哄け璐ユ垨宸插瓨鍦? %v\n", err)
		}

		// 鏄剧ず绯荤粺琛ㄧ┖闂翠篃鐢盨paceManager绠＄悊
		if systemSpace, err := spaceManager.GetSpace(0); err == nil {
			logger.Debugf("   - 绯荤粺琛ㄧ┖闂?Space ID 0)涔熺敱SpaceManager缁熶竴绠＄悊: %s\n",
				getSpaceStatus(systemSpace))
		}
	}

	fmt.Println("\n2.  StorageManager鍗忚皟鑱岃矗婕旂ず:")
	fmt.Println("   - 椤跺眰缁熶竴鍗忚皟鍣紝绠＄悊鎵€鏈夊瓨鍌ㄧ粍浠?)
	fmt.Println("   - 鍗忚皟SpaceManager銆丼egmentManager銆丅ufferPool绛?)
	fmt.Println("   - 绠＄悊琛ㄧ┖闂寸紦瀛樺拰鐢熷懡鍛ㄦ湡")
	logger.Debugf("   - 褰撳墠绠＄悊鐨勮〃绌洪棿缂撳瓨鏁伴噺: %d\n", getTablespaceCount(sm))

	// 鏂板锛氬睍绀篠ystemSpaceManager鍔熻兘
	fmt.Println("\n3. 馃彌锔? SystemSpaceManager鑱岃矗婕旂ず:")
	systemSpaceManager := sm.GetSystemSpaceManager()
	if systemSpaceManager != nil {
		fmt.Println("    SystemSpaceManager姝ｅ父杩愯")
		logger.Debugf("   - 鐙珛琛ㄧ┖闂存ā寮? %v (innodb_file_per_table=ON)\n",
			systemSpaceManager.IsFilePerTableEnabled())

		// 灞曠ずibdata1缁勪欢
		fmt.Println("   - ibdata1 (Space ID 0) 鍖呭惈鐨勭郴缁熺粍浠?")
		if components := systemSpaceManager.GetIBData1Components(); components != nil {
			fmt.Println("      Undo鏃ュ織绠＄悊鍣?(浜嬪姟鍥炴粴)")
			fmt.Println("      鎻掑叆缂撳啿绠＄悊鍣?(浼樺寲绱㈠紩鎻掑叆)")
			fmt.Println("      鍙屽啓缂撳啿绠＄悊鍣?(闃叉椤甸潰鎹熷潖)")
			fmt.Println("      琛ㄧ┖闂寸鐞嗛〉闈?(FSP_HDR, XDES, INODE)")
			fmt.Println("      浜嬪姟绯荤粺鏁版嵁 (閿佷俊鎭€佷簨鍔＄姸鎬?")
			fmt.Println("      鏁版嵁瀛楀吀鏍归〉闈?(Page 5)")
		}

		// 灞曠ず鐙珛琛ㄧ┖闂存槧灏?
		fmt.Println("   - 鐙珛琛ㄧ┖闂存槧灏勫叧绯?")
		independentSpaces := systemSpaceManager.ListIndependentTablespaces()
		mysqlSystemCount := 0
		for spaceID, info := range independentSpaces {
			if mysqlSystemCount < 5 { // 鍙樉绀哄墠5涓狹ySQL绯荤粺琛?
				logger.Debugf("     - %s -> Space ID %d (%s)\n",
					info.Name, spaceID, info.FilePath)
				mysqlSystemCount++
			}
		}
		if len(independentSpaces) > 5 {
			logger.Debugf("     - ... 杩樻湁 %d 涓嫭绔嬭〃绌洪棿\n", len(independentSpaces)-5)
		}

		// 灞曠ず缁熻淇℃伅
		if stats := systemSpaceManager.GetTablespaceStats(); stats != nil {
			fmt.Println("   - 琛ㄧ┖闂寸粺璁′俊鎭?")
			logger.Debugf("     - 绯荤粺琛ㄧ┖闂? Space ID %d (ibdata1)\n", stats.SystemSpaceID)
			logger.Debugf("     - MySQL绯荤粺琛? %d 涓嫭绔嬭〃绌洪棿\n", stats.MySQLSystemTableCount)
			logger.Debugf("     - 鐢ㄦ埛琛? %d 涓嫭绔嬭〃绌洪棿\n", stats.UserTableCount)
			logger.Debugf("     - information_schema: %d 涓〃绌洪棿\n", stats.InformationSchemaTableCount)
			logger.Debugf("     - performance_schema: %d 涓〃绌洪棿\n", stats.PerformanceSchemaTableCount)
		}
	}

	fmt.Println("\n4.  SegmentManager鑱岃矗婕旂ず:")
	if segMgr := sm.GetSegmentManager(); segMgr != nil {
		fmt.Println("    SegmentManager姝ｅ父杩愯")
		fmt.Println("   - 绠＄悊鏁版嵁娈靛拰绱㈠紩娈?)
		fmt.Println("   - 璐熻矗娈电殑鍒涘缓銆佸垎閰嶃€佸洖鏀?)
	}

	fmt.Println("\n5. 馃殌 BufferPoolManager鑱岃矗婕旂ず:")
	if bpm := sm.GetBufferPoolManager(); bpm != nil {
		fmt.Println("    OptimizedBufferPoolManager姝ｅ父杩愯")
		fmt.Println("   - 绠＄悊椤甸潰缂撳瓨鍜孡RU绛栫暐")
		fmt.Println("   - 浼樺寲I/O鎿嶄綔鍜岄璇绘満鍒?)
	}

	// 婕旂ず鑱岃矗濮旀墭
	fmt.Println("\n6. 馃攧 鑱岃矗濮旀墭娴佺▼婕旂ず:")
	fmt.Println("   鍦烘櫙: 鍒涘缓鏂扮敤鎴疯〃")
	fmt.Println("   StorageManager -> 濮旀墭缁橲paceManager鍒涘缓琛ㄧ┖闂?)
	fmt.Println("   StorageManager -> 濮旀墭缁橲egmentManager鍒涘缓鏁版嵁娈?)
	fmt.Println("   StorageManager -> 濮旀墭缁橞ufferPoolManager绠＄悊椤甸潰缂撳瓨")

	fmt.Println("\n   鍦烘櫙: 绯荤粺琛ㄧ┖闂寸鐞?(innodb_file_per_table=ON)")
	fmt.Println("   StorageManager -> 濮旀墭缁橲paceManager绠＄悊ibdata1鏂囦欢")
	fmt.Println("   SystemSpaceManager -> 绠＄悊ibdata1鍐呴儴绯荤粺缁勪欢(Undo, 鎻掑叆缂撳啿绛?")
	fmt.Println("   SystemSpaceManager -> 鏄犲皠MySQL绯荤粺琛ㄥ埌鐙珛琛ㄧ┖闂?Space ID 1-46)")
	fmt.Println("   SpaceManager -> 缁熶竴绠＄悊鎵€鏈塈BD鏂囦欢(ibdata1 + 鐙珛琛ㄧ┖闂?")

	fmt.Println("\n7. 鉁?鍩轰簬innodb_file_per_table=ON鐨勬灦鏋勪紭鍔?")
	fmt.Println("   - 娓呮櫚鐨勫瓨鍌ㄥ垎绂? ibdata1涓撻棬瀛樺偍绯荤粺绾ф暟鎹?)
	fmt.Println("   - 鐙珛琛ㄧ┖闂? MySQL绯荤粺琛ㄣ€佺敤鎴疯〃鍚勮嚜鐙珛鐨?ibd鏂囦欢")
	fmt.Println("   - 缁熶竴鐨勬枃浠剁鐞? SpaceManager缁熶竴绠＄悊鎵€鏈塈BD鏂囦欢")
	fmt.Println("   - 涓撲笟鐨勭郴缁熺鐞? SystemSpaceManager涓撻棬绠＄悊绯荤粺绾х粍浠?)
	fmt.Println("   - 閬垮厤閲嶅鍒濆鍖? 鏅鸿兘妫€娴嬪凡瀛樺湪鐨処BD鏂囦欢")
	fmt.Println("   - 娓呮櫚鐨勮亴璐ｅ垎绂? 姣忎釜绠＄悊鍣ㄦ湁鏄庣‘鐨勮矗浠昏竟鐣?)
}

func getSpaceStatus(space interface{}) string {
	// 杩欓噷鍙互鏍规嵁瀹為檯鐨凷pace鎺ュ彛瀹炵幇鑾峰彇鐘舵€佷俊鎭?
	return "娲昏穬"
}

func getTablespaceCount(sm *manager.StorageManager) int {
	if spaces, err := sm.ListSpaces(); err == nil {
		return len(spaces)
	}
	return 0
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
