package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/util"
)

func main() {
	fmt.Println("=== XMySQL StorageManager 系统表空间初始化演示 ===")
	fmt.Println()

	// 创建临时演示目录
	demoDir := "D:\\GolangProjects\\github\\xmysql-server\\demo_data"
	os.RemoveAll(demoDir) // 清理之前的演示数据
	os.MkdirAll(demoDir, 0755)
	defer func() {
		fmt.Println("清理演示数据...")
		os.RemoveAll(demoDir)
	}()

	// 创建演示配置
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

	// 创建必要的子目录
	os.MkdirAll(cfg.InnodbRedoLogDir, 0755)
	os.MkdirAll(cfg.InnodbUndoLogDir, 0755)

	logger.Infof("数据目录: %s\n", demoDir)
	logger.Infof("配置信息:\n")
	logger.Infof("  - 缓冲池大小: %d MB\n", cfg.InnodbBufferPoolSize/1024/1024)
	logger.Infof("  - 页面大小: %d KB\n", cfg.InnodbPageSize/1024)
	logger.Infof("  - 系统表空间: %s\n", cfg.InnodbDataFilePath)
	fmt.Println()

	// 初始化 StorageManager
	fmt.Println("正在初始化 StorageManager...")
	fmt.Println("这将自动创建所有系统表空间，就像 MySQL 一样...")
	fmt.Println()

	sm := manager.NewStorageManager(cfg)
	if sm == nil {
		fmt.Println(" StorageManager 初始化失败")
		return
	}

	fmt.Println(" StorageManager 初始化成功!")
	fmt.Println()

	// 验证系统表空间
	fmt.Println("=== 验证系统表空间创建 ===")

	// 1. 验证系统表空间 (ibdata1)
	fmt.Println("1. 系统表空间 (ibdata1):")
	systemSpace, err := sm.GetSpaceInfo(0)
	if err != nil {
		logger.Infof("    获取失败: %v\n", err)
	} else {
		logger.Infof("    Space ID: %d\n", systemSpace.SpaceID)
		logger.Infof("    名称: %s\n", systemSpace.Name)
		logger.Infof("    页面大小: %d bytes\n", systemSpace.PageSize)
		logger.Infof("    状态: %s\n", systemSpace.State)
	}
	fmt.Println()

	// 2. 验证 MySQL 系统表
	fmt.Println("2. MySQL 系统数据库表:")
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

	// 3. 验证 information_schema 表
	fmt.Println("3. information_schema 表:")
	infoSpace, err := sm.GetSpaceInfo(100)
	if err != nil {
		logger.Infof("    获取失败: %v\n", err)
	} else {
		logger.Infof("    information_schema/schemata (Space ID %d)\n", infoSpace.SpaceID)
	}

	// 4. 验证 performance_schema 表
	fmt.Println("4. performance_schema 表:")
	perfSpace, err := sm.GetSpaceInfo(200)
	if err != nil {
		logger.Infof("    获取失败: %v\n", err)
	} else {
		logger.Infof("    performance_schema/accounts (Space ID %d)\n", perfSpace.SpaceID)
	}
	fmt.Println()

	// 5. 列出所有表空间
	fmt.Println("=== 所有表空间列表 ===")
	spaces, err := sm.ListSpaces()
	if err != nil {
		logger.Infof(" 获取表空间列表失败: %v\n", err)
	} else {
		logger.Infof("总共创建了 %d 个表空间:\n", len(spaces))
		for i, space := range spaces {
			if i < 10 { // 只显示前10个
				logger.Debugf("  %d. Space ID %d: %s (%s)\n",
					i+1, space.SpaceID, space.Name, space.State)
			}
		}
		if len(spaces) > 10 {
			logger.Debugf("  ... 还有 %d 个表空间\n", len(spaces)-10)
		}
	}
	fmt.Println()

	// 6. 验证文件创建
	fmt.Println("=== 验证文件创建 ===")
	files := []string{
		"ibdata1.ibd",
		"mysql/user.ibd",
		"information_schema/schemata.ibd",
		"performance_schema/accounts.ibd",
	}

	for _, file := range files {
		fullPath := filepath.Join(demoDir, file)
		if _, err := os.Stat(fullPath); os.IsNotExist(err) {
			logger.Debugf(" 文件未创建: %s\n", file)
		} else {
			logger.Debugf(" 文件已创建: %s\n", file)
		}
	}
	fmt.Println()

	// 7. 显示目录结构
	fmt.Println("=== 数据目录结构 ===")
	showDirectoryStructure(demoDir, 0, 2)
	fmt.Println()

	// 清理资源
	fmt.Println("正在关闭 StorageManager...")
	err = sm.Close()
	if err != nil {
		logger.Debugf(" 关闭失败: %v\n", err)
	} else {
		fmt.Println(" StorageManager 已成功关闭")
	}

	fmt.Println()
	fmt.Println("=== 演示完成 ===")
	fmt.Println("StorageManager 已成功初始化所有系统表空间，")
	fmt.Println("就像 MySQL 服务器首次启动时一样！")
}

// showDirectoryStructure 显示目录结构
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
			logger.Debugf("%s📁 %s/\n", indent, entry.Name())
			if level < maxLevel {
				showDirectoryStructure(filepath.Join(dir, entry.Name()), level+1, maxLevel)
			}
		} else {
			logger.Debugf("%s📄 %s\n", indent, entry.Name())
		}
	}
}
