//go:build demo
// +build demo

package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println("=" + strings.Repeat("=", 80))
	fmt.Println("馃彌锔? XMySQL InnoDB SystemSpaceManager 鍔熻兘婕旂ず")
	fmt.Println("   鍩轰簬 innodb_file_per_table=ON 閰嶇疆鐨勭郴缁熻〃绌洪棿绠＄悊")
	fmt.Println("=" + strings.Repeat("=", 80))

	// 鍒涘缓婕旂ず鐩綍
	demoDir := "demo_system_space_manager"
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
		InnodbBufferPoolSize: 32 * 1024 * 1024, // 32MB
		InnodbPageSize:       16384,            // 16KB
		InnodbLogFileSize:    10485760,         // 10MB
		InnodbLogBufferSize:  1048576,          // 1MB
	}

	logger.Debugf("\n 婕旂ず閰嶇疆 (innodb_file_per_table=ON):\n")
	logger.Debugf("  - 鏁版嵁鐩綍: %s\n", cfg.DataDir)
	logger.Debugf("  - 绯荤粺琛ㄧ┖闂? %s\n", cfg.InnodbDataFilePath)
	logger.Debugf("  - 缂撳啿姹犲ぇ灏? %d MB\n", cfg.InnodbBufferPoolSize/1024/1024)
	logger.Debugf("  - 椤甸潰澶у皬: %d KB\n", cfg.InnodbPageSize/1024)

	// 鍒濆鍖栧瓨鍌ㄧ鐞嗗櫒
	fmt.Println("\n馃殌 鍒濆鍖?StorageManager...")
	fmt.Println(strings.Repeat("-", 60))

	storageManager := manager.NewStorageManager(cfg)
	if storageManager == nil {
		logger.Debugf(" 鍒涘缓StorageManager澶辫触\n")
		return
	}

	// 鑾峰彇SystemSpaceManager
	systemSpaceManager := storageManager.GetSystemSpaceManager()
	if systemSpaceManager == nil {
		logger.Debugf(" SystemSpaceManager鏈垵濮嬪寲\n")
		return
	}

	fmt.Println(" StorageManager 鍜?SystemSpaceManager 鍒濆鍖栧畬鎴?)

	// 婕旂ず1: 绯荤粺琛ㄧ┖闂存灦鏋勫垎鏋?
	demonstrateSystemSpaceArchitecture(systemSpaceManager)

	// 婕旂ず2: ibdata1缁勪欢绠＄悊
	demonstrateIBData1Components(systemSpaceManager)

	// 婕旂ず3: 鐙珛琛ㄧ┖闂存槧灏?
	demonstrateIndependentTablespaces(systemSpaceManager)

	// 婕旂ず4: Space ID鍒嗛厤绛栫暐
	demonstrateSpaceIDAllocation(systemSpaceManager)

	// 婕旂ず5: 缁熻淇℃伅鍜岀洃鎺?
	demonstrateStatisticsAndMonitoring(systemSpaceManager)

	// 鍏抽棴绠＄悊鍣?
	fmt.Println("\n馃攧 鍏抽棴SystemSpaceManager...")
	systemSpaceManager.Close()
	storageManager.Close()

	fmt.Println("\n SystemSpaceManager鍔熻兘婕旂ず瀹屾垚!")
	fmt.Println("\n馃挕 鍏抽敭鐗规€ф€荤粨:")
	fmt.Println("  鈥?ibdata1涓撻棬瀛樺偍绯荤粺绾ф暟鎹紝涓嶅啀瀛樺偍鐢ㄦ埛琛ㄦ暟鎹?)
	fmt.Println("  鈥?MySQL绯荤粺琛ㄩ噰鐢ㄧ嫭绔嬭〃绌洪棿锛屼究浜庣鐞嗗拰缁存姢")
	fmt.Println("  鈥?娓呮櫚鐨凷pace ID鍒嗛厤绛栫暐锛岄伩鍏嶅啿绐?)
	fmt.Println("  鈥?缁熶竴鐨勬枃浠剁鐞嗭紝鎵€鏈塈BD鏂囦欢鐢盨paceManager缁熶竴澶勭悊")
	fmt.Println("  鈥?涓撲笟鐨勭郴缁熺粍浠剁鐞嗭紝姣忎釜缁勪欢鑱岃矗鏄庣‘")
}

func demonstrateSystemSpaceArchitecture(ssm *manager.SystemSpaceManager) {
	fmt.Println("\n  婕旂ず1: 绯荤粺琛ㄧ┖闂存灦鏋勫垎鏋?)
	fmt.Println(strings.Repeat("-", 60))

	logger.Debugf("鐙珛琛ㄧ┖闂存ā寮? %s\n", getEnabledStatus(ssm.IsFilePerTableEnabled()))

	if systemSpace := ssm.GetSystemSpace(); systemSpace != nil {
		logger.Debugf("绯荤粺琛ㄧ┖闂?(ibdata1): Space ID = 0\n")
		logger.Debugf("  - 鏂囦欢鍚? %s\n", systemSpace.Name())
		logger.Debugf("  - 椤甸潰鏁伴噺: %d\n", systemSpace.GetPageCount())
		logger.Debugf("  - 宸茬敤绌洪棿: %d KB\n", systemSpace.GetUsedSpace()/1024)
		logger.Debugf("  - 鐘舵€? %s\n", getActiveStatus(systemSpace.IsActive()))
	}

	fmt.Println("\n馃摉 ibdata1鑱岃矗 (鍩轰簬innodb_file_per_table=ON):")
	fmt.Println("   Undo Logs - 浜嬪姟鍥炴粴鏁版嵁")
	fmt.Println("   Insert Buffer - 寤惰繜绱㈠紩鎻掑叆浼樺寲")
	fmt.Println("   Double Write Buffer - 宕╂簝鎭㈠淇濇姢")
	fmt.Println("   System Management Pages - FSP_HDR, XDES, INODE椤甸潰")
	fmt.Println("   Transaction System Data - 浜嬪姟閿佷俊鎭?)
	fmt.Println("   Data Dictionary Root - 鏁版嵁瀛楀吀鏍归〉闈?(Page 5)")
	fmt.Println("   涓嶅啀瀛樺偍: 鐢ㄦ埛琛ㄦ暟鎹拰绱㈠紩")
}

func demonstrateIBData1Components(ssm *manager.SystemSpaceManager) {
	fmt.Println("\n 婕旂ず2: ibdata1缁勪欢绠＄悊")
	fmt.Println(strings.Repeat("-", 60))

	components := ssm.GetIBData1Components()
	if components == nil {
		fmt.Println(" IBData1缁勪欢鏈垵濮嬪寲")
		return
	}

	fmt.Println("IBData1绯荤粺缁勪欢鐘舵€?")

	// Undo鏃ュ織绠＄悊鍣?
	if components.UndoLogs != nil {
		fmt.Println("   UndoLogManager: 姝ｅ父杩愯")
		fmt.Println("     - 鑱岃矗: 绠＄悊浜嬪姟鍥炴粴鏃ュ織")
		fmt.Println("     - 浣嶇疆: ibdata1 澶氫釜椤甸潰")
	}

	// 鎻掑叆缂撳啿绠＄悊鍣?
	if components.InsertBuffer != nil {
		fmt.Println("   InsertBufferManager: 姝ｅ父杩愯")
		fmt.Println("     - 鑱岃矗: 浼樺寲浜岀骇绱㈠紩鎻掑叆鎬ц兘")
		fmt.Println("     - 浣嶇疆: ibdata1 涓撶敤椤甸潰")
	}

	// 鍙屽啓缂撳啿绠＄悊鍣?
	if components.DoubleWriteBuffer != nil {
		fmt.Println("   DoubleWriteBufferManager: 姝ｅ父杩愯")
		fmt.Println("     - 鑱岃矗: 闃叉椤甸潰閮ㄥ垎鍐欏叆瀵艰嚧鐨勬暟鎹崯鍧?)
		fmt.Println("     - 浣嶇疆: ibdata1 杩炵画64+64椤甸潰")
	}

	// 琛ㄧ┖闂寸鐞嗛〉闈?
	if components.SpaceManagementPages != nil {
		fmt.Println("   SpaceManagementPages: 姝ｅ父杩愯")
		fmt.Println("     - 鑱岃矗: FSP_HDR, XDES, INODE椤甸潰绠＄悊")
		fmt.Println("     - 浣嶇疆: ibdata1 鍓嶅嚑涓〉闈?)
	}

	// 浜嬪姟绯荤粺绠＄悊鍣?
	if components.TransactionSystemData != nil {
		fmt.Println("   TransactionSystemManager: 姝ｅ父杩愯")
		fmt.Println("     - 鑱岃矗: 浜嬪姟鐘舵€佸拰閿佷俊鎭鐞?)
		fmt.Println("     - 浣嶇疆: ibdata1 椤甸潰6寮€濮?)
	}

	// 閿佷俊鎭鐞嗗櫒
	if components.LockInfoManager != nil {
		fmt.Println("   LockInfoManager: 姝ｅ父杩愯")
		fmt.Println("     - 鑱岃矗: 琛岄攣鍜岃〃閿佷俊鎭鐞?)
		fmt.Println("     - 浣嶇疆: ibdata1 浜嬪姟绯荤粺椤甸潰")
	}

	// 鏁版嵁瀛楀吀鏍归〉闈?
	if components.DataDictionaryRoot != nil {
		fmt.Println("   DataDictionaryRoot: 姝ｅ父杩愯")
		fmt.Println("     - 鑱岃矗: 鏁版嵁瀛楀吀鍏冩暟鎹牴椤甸潰")
		fmt.Println("     - 浣嶇疆: ibdata1 椤甸潰5 (鍥哄畾浣嶇疆)")
		logger.Debugf("     - 鏈€澶ц〃ID: %d\n", components.DataDictionaryRoot.GetMaxTableId())
		logger.Debugf("     - 鏈€澶х储寮旾D: %d\n", components.DataDictionaryRoot.GetMaxIndexId())
		logger.Debugf("     - 鏈€澶pace ID: %d\n", components.DataDictionaryRoot.GetMaxSpaceId())
	}
}

func demonstrateIndependentTablespaces(ssm *manager.SystemSpaceManager) {
	fmt.Println("\n 婕旂ず3: 鐙珛琛ㄧ┖闂存槧灏?)
	fmt.Println(strings.Repeat("-", 60))

	independentSpaces := ssm.ListIndependentTablespaces()
	logger.Debugf("鐙珛琛ㄧ┖闂存€绘暟: %d\n", len(independentSpaces))

	// 鍒嗙被缁熻
	mysqlSystemTables := make([]string, 0)
	infoSchemaTables := make([]string, 0)
	perfSchemaTables := make([]string, 0)
	userTables := make([]string, 0)

	for _, info := range independentSpaces {
		switch info.TableType {
		case "system":
			mysqlSystemTables = append(mysqlSystemTables, info.Name)
		case "information_schema":
			infoSchemaTables = append(infoSchemaTables, info.Name)
		case "performance_schema":
			perfSchemaTables = append(perfSchemaTables, info.Name)
		case "user":
			userTables = append(userTables, info.Name)
		}
	}

	// 鏄剧ずMySQL绯荤粺琛ㄦ槧灏?
	logger.Debugf("\n MySQL绯荤粺琛ㄧ嫭绔嬭〃绌洪棿 (%d涓?:\n", len(mysqlSystemTables))
	count := 0
	for spaceID, info := range independentSpaces {
		if info.TableType == "system" && count < 8 {
			logger.Debugf("  鈥?%s -> Space ID %d (%s)\n", info.Name, spaceID, info.FilePath)
			count++
		}
	}
	if len(mysqlSystemTables) > 8 {
		logger.Debugf("  鈥?... 杩樻湁 %d 涓狹ySQL绯荤粺琛╘n", len(mysqlSystemTables)-8)
	}

	// 鏄剧ず铏氭嫙琛ㄦ槧灏勭ず渚?
	logger.Debugf("\n information_schema 琛ㄧ┖闂?(%d涓?:\n", len(infoSchemaTables))
	if len(infoSchemaTables) > 0 {
		fmt.Println("  鈥?Space ID鑼冨洿: 100-199 (铏氭嫙琛?")
		fmt.Println("  鈥?鐗圭偣: 鍔ㄦ€佺敓鎴愶紝涓嶅瓨鍌ㄦ寔涔呮暟鎹?)
	}

	logger.Debugf("\n鈿?performance_schema 琛ㄧ┖闂?(%d涓?:\n", len(perfSchemaTables))
	if len(perfSchemaTables) > 0 {
		fmt.Println("  鈥?Space ID鑼冨洿: 200-299 (鎬ц兘鐩戞帶)")
		fmt.Println("  鈥?鐗圭偣: 鍐呭瓨琛紝閲嶅惎鍚庨噸鏂扮敓鎴?)
	}

	// 灞曠ず鐗瑰畾绯荤粺琛ㄧ殑鏄犲皠
	fmt.Println("\n馃攽 鍏抽敭绯荤粺琛ㄦ槧灏勭ず渚?")
	keyTables := []string{"mysql.user", "mysql.db", "mysql.tables_priv", "mysql.plugin"}
	for _, tableName := range keyTables {
		if spaceID, exists := ssm.GetMySQLSystemTableSpaceID(tableName); exists {
			logger.Debugf("  鈥?%s -> Space ID %d\n", tableName, spaceID)
		}
	}
}

func demonstrateSpaceIDAllocation(ssm *manager.SystemSpaceManager) {
	fmt.Println("\n 婕旂ず4: Space ID鍒嗛厤绛栫暐")
	fmt.Println(strings.Repeat("-", 60))

	fmt.Println("Space ID鍒嗛厤瑙勫垯 (鍩轰簬innodb_file_per_table=ON):")
	fmt.Println()
	fmt.Println("鈹屸攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?)
	fmt.Println("鈹?   Space ID     鈹?   鐢ㄩ€?    鈹?          璇存槑              鈹?)
	fmt.Println("鈹溾攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹尖攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹尖攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?)
	fmt.Println("鈹?      0         鈹?绯荤粺琛ㄧ┖闂? 鈹?ibdata1 (绯荤粺绾ф暟鎹?       鈹?)
	fmt.Println("鈹?    1 - 46      鈹?MySQL绯荤粺琛?鈹?mysql.user, mysql.db绛?    鈹?)
	fmt.Println("鈹?  100 - 199     鈹?info_schema 鈹?铏氭嫙琛?(鍔ㄦ€佺敓鎴?          鈹?)
	fmt.Println("鈹?  200 - 299     鈹?perf_schema 鈹?鎬ц兘鐩戞帶琛?(鍐呭瓨琛?        鈹?)
	fmt.Println("鈹?   1000+        鈹?  鐢ㄦ埛琛?   鈹?鐢ㄦ埛鑷畾涔夎〃               鈹?)
	fmt.Println("鈹斺攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹粹攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹粹攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?)

	// 楠岃瘉褰撳墠鍒嗛厤鎯呭喌
	fmt.Println("\n馃搱 褰撳墠Space ID鍒嗛厤鐘跺喌:")
	independentSpaces := ssm.ListIndependentTablespaces()

	systemCount := 0
	infoSchemaCount := 0
	perfSchemaCount := 0
	userCount := 0

	for spaceID, info := range independentSpaces {
		switch {
		case spaceID == 0:
			// 绯荤粺琛ㄧ┖闂达紝宸插崟鐙鐞?
		case spaceID >= 1 && spaceID <= 46:
			systemCount++
		case spaceID >= 100 && spaceID <= 199:
			infoSchemaCount++
		case spaceID >= 200 && spaceID <= 299:
			perfSchemaCount++
		case spaceID >= 1000:
			userCount++
		}
		_ = info // 閬垮厤鏈娇鐢ㄥ彉閲忚鍛?
	}

	logger.Debugf("  鈥?绯荤粺琛ㄧ┖闂?(0): 1涓?(ibdata1)\n")
	logger.Debugf("  鈥?MySQL绯荤粺琛?(1-46): %d涓猏n", systemCount)
	logger.Debugf("  鈥?information_schema (100-199): %d涓猏n", infoSchemaCount)
	logger.Debugf("  鈥?performance_schema (200-299): %d涓猏n", perfSchemaCount)
	logger.Debugf("  鈥?鐢ㄦ埛琛?(1000+): %d涓猏n", userCount)

	fmt.Println("\n鉁?鍒嗛厤绛栫暐浼樺娍:")
	fmt.Println("  鈥?閬垮厤Space ID鍐茬獊")
	fmt.Println("  鈥?渚夸簬鎸夌被鍨嬬鐞嗚〃绌洪棿")
	fmt.Println("  鈥?鏀寔澶ц妯￠儴缃叉墿灞?)
	fmt.Println("  鈥?鍏煎MySQL瀹樻柟瀹炵幇")
}

func demonstrateStatisticsAndMonitoring(ssm *manager.SystemSpaceManager) {
	fmt.Println("\n 婕旂ず5: 缁熻淇℃伅鍜岀洃鎺?)
	fmt.Println(strings.Repeat("-", 60))

	stats := ssm.GetTablespaceStats()
	if stats == nil {
		fmt.Println(" 缁熻淇℃伅涓嶅彲鐢?)
		return
	}

	fmt.Println("馃搱 琛ㄧ┖闂寸粺璁′俊鎭?")
	logger.Debugf("  鈥?绯荤粺琛ㄧ┖闂碔D: %d (ibdata1)\n", stats.SystemSpaceID)
	logger.Debugf("  鈥?绯荤粺琛ㄧ┖闂村ぇ灏? %d KB\n", stats.SystemSpaceSize/1024)
	logger.Debugf("  鈥?鐙珛琛ㄧ┖闂存€绘暟: %d\n", stats.IndependentSpaceCount)
	logger.Debugf("  鈥?MySQL绯荤粺琛ㄦ暟閲? %d\n", stats.MySQLSystemTableCount)
	logger.Debugf("  鈥?鐢ㄦ埛琛ㄦ暟閲? %d\n", stats.UserTableCount)
	logger.Debugf("  鈥?information_schema琛? %d\n", stats.InformationSchemaTableCount)
	logger.Debugf("  鈥?performance_schema琛? %d\n", stats.PerformanceSchemaTableCount)

	// 璁＄畻瀛樺偍鍒╃敤鐜?
	totalIndependentSpaces := stats.MySQLSystemTableCount +
		stats.UserTableCount +
		stats.InformationSchemaTableCount +
		stats.PerformanceSchemaTableCount

	fmt.Println("\n 瀛樺偍鍒嗗竷鍒嗘瀽:")
	if totalIndependentSpaces > 0 {
		mysqlPct := float64(stats.MySQLSystemTableCount) / float64(totalIndependentSpaces) * 100
		userPct := float64(stats.UserTableCount) / float64(totalIndependentSpaces) * 100
		infoPct := float64(stats.InformationSchemaTableCount) / float64(totalIndependentSpaces) * 100
		perfPct := float64(stats.PerformanceSchemaTableCount) / float64(totalIndependentSpaces) * 100

		logger.Debugf("  鈥?MySQL绯荤粺琛? %.1f%%\n", mysqlPct)
		logger.Debugf("  鈥?鐢ㄦ埛琛? %.1f%%\n", userPct)
		logger.Debugf("  鈥?information_schema: %.1f%%\n", infoPct)
		logger.Debugf("  鈥?performance_schema: %.1f%%\n", perfPct)
	}

	fmt.Println("\n 鐩戞帶寤鸿:")
	fmt.Println("  鈥?瀹氭湡妫€鏌bdata1澧為暱鎯呭喌")
	fmt.Println("  鈥?鐩戞帶鐙珛琛ㄧ┖闂存枃浠跺ぇ灏?)
	fmt.Println("  鈥?鍏虫敞Undo鏃ュ織绌洪棿浣跨敤")
	fmt.Println("  鈥?瑙傚療鎻掑叆缂撳啿浣跨敤鐜?)
}

// 杈呭姪鍑芥暟
func getEnabledStatus(enabled bool) string {
	if enabled {
		return "鍚敤 (innodb_file_per_table=ON)"
	}
	return "绂佺敤 (innodb_file_per_table=OFF)"
}

func getActiveStatus(active bool) string {
	if active {
		return "娲昏穬"
	}
	return "闈炴椿璺?
}
