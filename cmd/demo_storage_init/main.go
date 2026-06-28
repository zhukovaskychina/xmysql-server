//go:build demo
// +build demo

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println("=== XMySQL StorageManager 绯荤粺琛ㄧ┖闂村垵濮嬪寲婕旂ず ===")
	fmt.Println()

	// 鍒涘缓涓存椂婕旂ず鐩綍
	demoDir := "D:\\GolangProjects\\github\\xmysql-server\\demo_data"
	os.RemoveAll(demoDir) // 娓呯悊涔嬪墠鐨勬紨绀烘暟鎹?
	os.MkdirAll(demoDir, 0755)
	defer func() {
		fmt.Println("娓呯悊婕旂ず鏁版嵁...")
		os.RemoveAll(demoDir)
	}()

	// 鍒涘缓婕旂ず閰嶇疆
	cfg := &conf.Cfg{
		DataDir:                   demoDir,
		InnodbDataDir:             demoDir,
		InnodbDataFilePath:        "ibdata1:100M:autoextend",
		InnodbBufferPoolSize:      16777216, // 16MB
		InnodbPageSize:            16384,    // 16KB
		InnodbLogFileSize:         10485760, // 10MB
		InnodbLogBufferSize:       1048576,  // 1MB
		InnodbFlushLogAtTrxCommit: 1,
		InnodbFileFormat:          "Barracuda",
		InnodbDefaultRowFormat:    "DYNAMIC",
		InnodbDoublewrite:         true,
		InnodbAdaptiveHashIndex:   true,
		InnodbRedoLogDir:          filepath.Join(demoDir, "redo"),
		InnodbUndoLogDir:          filepath.Join(demoDir, "undo"),
	}

	// 鍒涘缓蹇呰鐨勫瓙鐩綍
	os.MkdirAll(cfg.InnodbRedoLogDir, 0755)
	os.MkdirAll(cfg.InnodbUndoLogDir, 0755)

	logger.Infof("鏁版嵁鐩綍: %s\n", demoDir)
	logger.Infof("閰嶇疆淇℃伅:\n")
	logger.Infof("  - 缂撳啿姹犲ぇ灏? %d MB\n", cfg.InnodbBufferPoolSize/1024/1024)
	logger.Infof("  - 椤甸潰澶у皬: %d KB\n", cfg.InnodbPageSize/1024)
	logger.Infof("  - 绯荤粺琛ㄧ┖闂? %s\n", cfg.InnodbDataFilePath)
	fmt.Println()

	// 鍒濆鍖?StorageManager
	fmt.Println("姝ｅ湪鍒濆鍖?StorageManager...")
	fmt.Println("杩欏皢鑷姩鍒涘缓鎵€鏈夌郴缁熻〃绌洪棿锛屽氨鍍?MySQL 涓€鏍?..")
	fmt.Println()

	sm := manager.NewStorageManager(cfg)
	if sm == nil {
		fmt.Println(" StorageManager 鍒濆鍖栧け璐?)
		return
	}

	fmt.Println(" StorageManager 鍒濆鍖栨垚鍔?")
	fmt.Println()

	// 楠岃瘉绯荤粺琛ㄧ┖闂?
	fmt.Println("=== 楠岃瘉绯荤粺琛ㄧ┖闂村垱寤?===")

	// 1. 楠岃瘉绯荤粺琛ㄧ┖闂?(ibdata1)
	fmt.Println("1. 绯荤粺琛ㄧ┖闂?(ibdata1):")
	systemSpace, err := sm.GetSpaceInfo(0)
	if err != nil {
		logger.Infof("    鑾峰彇澶辫触: %v\n", err)
	} else {
		logger.Infof("    Space ID: %d\n", systemSpace.SpaceID)
		logger.Infof("    鍚嶇О: %s\n", systemSpace.Name)
		logger.Infof("    椤甸潰澶у皬: %d bytes\n", systemSpace.PageSize)
		logger.Infof("    鐘舵€? %s\n", systemSpace.State)
	}
	fmt.Println()

	// 2. 楠岃瘉 MySQL 绯荤粺琛?
	fmt.Println("2. MySQL 绯荤粺鏁版嵁搴撹〃:")
	systemTables := []struct {
		spaceID uint32
		name    string
	}{
		{1, "mysql/user"},
		{2, "mysql/db"},
		{3, "mysql/tables_priv"},
		{4, "mysql/columns_priv"},
		{5, "mysql/procs_priv"},
	}

	for _, table := range systemTables {
		space, err := sm.GetSpaceInfo(table.spaceID)
		if err != nil {
			logger.Infof("    %s (Space ID %d): %v\n", table.name, table.spaceID, err)
		} else {
			logger.Infof("    %s (Space ID %d)\n", table.name, space.SpaceID)
		}
	}
	fmt.Println()

	// 3. 楠岃瘉 information_schema 琛?
	fmt.Println("3. information_schema 琛?")
	infoSpace, err := sm.GetSpaceInfo(100)
	if err != nil {
		logger.Infof("    鑾峰彇澶辫触: %v\n", err)
	} else {
		logger.Infof("    information_schema/schemata (Space ID %d)\n", infoSpace.SpaceID)
	}

	// 4. 楠岃瘉 performance_schema 琛?
	fmt.Println("4. performance_schema 琛?")
	perfSpace, err := sm.GetSpaceInfo(200)
	if err != nil {
		logger.Infof("    鑾峰彇澶辫触: %v\n", err)
	} else {
		logger.Infof("    performance_schema/accounts (Space ID %d)\n", perfSpace.SpaceID)
	}
	fmt.Println()

	// 5. 鍒楀嚭鎵€鏈夎〃绌洪棿
	fmt.Println("=== 鎵€鏈夎〃绌洪棿鍒楄〃 ===")
	spaces, err := sm.ListSpaces()
	if err != nil {
		logger.Infof(" 鑾峰彇琛ㄧ┖闂村垪琛ㄥけ璐? %v\n", err)
	} else {
		logger.Infof("鎬诲叡鍒涘缓浜?%d 涓〃绌洪棿:\n", len(spaces))
		for i, space := range spaces {
			if i < 10 { // 鍙樉绀哄墠10涓?
				logger.Debugf("  %d. Space ID %d: %s (%s)\n",
					i+1, space.SpaceID, space.Name, space.State)
			}
		}
		if len(spaces) > 10 {
			logger.Debugf("  ... 杩樻湁 %d 涓〃绌洪棿\n", len(spaces)-10)
		}
	}
	fmt.Println()

	// 6. 楠岃瘉鏂囦欢鍒涘缓
	fmt.Println("=== 楠岃瘉鏂囦欢鍒涘缓 ===")
	files := []string{
		"ibdata1.ibd",
		"mysql/user.ibd",
		"information_schema/schemata.ibd",
		"performance_schema/accounts.ibd",
	}

	for _, file := range files {
		fullPath := filepath.Join(demoDir, file)
		if _, err := os.Stat(fullPath); os.IsNotExist(err) {
			logger.Debugf(" 鏂囦欢鏈垱寤? %s\n", file)
		} else {
			logger.Debugf(" 鏂囦欢宸插垱寤? %s\n", file)
		}
	}
	fmt.Println()

	// 7. 鏄剧ず鐩綍缁撴瀯
	fmt.Println("=== 鏁版嵁鐩綍缁撴瀯 ===")
	showDirectoryStructure(demoDir, 0, 2)
	fmt.Println()

	// 娓呯悊璧勬簮
	fmt.Println("姝ｅ湪鍏抽棴 StorageManager...")
	err = sm.Close()
	if err != nil {
		logger.Debugf(" 鍏抽棴澶辫触: %v\n", err)
	} else {
		fmt.Println(" StorageManager 宸叉垚鍔熷叧闂?)
	}

	fmt.Println()
	fmt.Println("=== 婕旂ず瀹屾垚 ===")
	fmt.Println("StorageManager 宸叉垚鍔熷垵濮嬪寲鎵€鏈夌郴缁熻〃绌洪棿锛?)
	fmt.Println("灏卞儚 MySQL 鏈嶅姟鍣ㄩ娆″惎鍔ㄦ椂涓€鏍凤紒")
}

// showDirectoryStructure 鏄剧ず鐩綍缁撴瀯
func showDirectoryStructure(dir string, level int, maxLevel int) {
	if level > maxLevel {
		return
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		indent := ""
		for i := 0; i < level; i++ {
			indent += "  "
		}

		if entry.IsDir() {
			logger.Debugf("%s馃搧 %s/\n", indent, entry.Name())
			if level < maxLevel {
				showDirectoryStructure(filepath.Join(dir, entry.Name()), level+1, maxLevel)
			}
		} else {
			logger.Debugf("%s馃搫 %s\n", indent, entry.Name())
		}
	}
}
