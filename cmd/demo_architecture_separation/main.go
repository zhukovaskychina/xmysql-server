package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/util"
)

func main() {
	fmt.Println("=" + strings.Repeat("=", 60))
	fmt.Println("  XMySQL InnoDB 存储架构职责分离演示")
	fmt.Println("=" + strings.Repeat("=", 60))

	// 创建演示目录
	demoDir := "demo_architecture_separation"
	os.RemoveAll(demoDir) // 清理之前的数据
	os.MkdirAll(demoDir, 0755)
	defer func() {
		fmt.Println("\n🧹 清理演示数据...")
		os.RemoveAll(demoDir)
	}()

	// 创建演示配置
	cfg := &conf.Cfg{
		DataDir:              demoDir,
		InnodbDataDir:        demoDir,
		InnodbDataFilePath:   "ibdata1:50M:autoextend",
		InnodbBufferPoolSize: 16777216, // 16MB
		InnodbPageSize:       16384,    // 16KB
		InnodbLogFileSize:    10485760, // 10MB
		InnodbLogBufferSize:  1048576,  // 1MB
	}

	fmt.Println("\n 演示配置:")
	logger.Debugf("  - 数据目录: %s\n", cfg.DataDir)
	logger.Debugf("  - 系统表空间: %s\n", cfg.InnodbDataFilePath)
	logger.Debugf("  - 缓冲池大小: %d MB\n", cfg.InnodbBufferPoolSize/1024/1024)

	// 第一次运行：创建全新的存储系统
	fmt.Println("\n🚀 第一次运行：创建全新的存储系统")
	fmt.Println(strings.Repeat("-", 50))

	storageManager1 := manager.NewStorageManager(cfg)
	if storageManager1 == nil {
		logger.Debugf(" 创建StorageManager失败\n")
		return
	}

	fmt.Println("\n 第一次运行后的状态:")
	displayManagerStatus(storageManager1, cfg)

	// 关闭第一个实例
	fmt.Println("\n🔄 关闭StorageManager...")
	storageManager1.Close()

	// 第二次运行：重新打开已存在的存储系统
	fmt.Println("\n🔄 第二次运行：重新打开已存在的存储系统")
	fmt.Println(strings.Repeat("-", 50))

	storageManager2 := manager.NewStorageManager(cfg)
	if storageManager2 == nil {
		logger.Debugf(" 重新打开StorageManager失败\n")
		return
	}

	fmt.Println("\n 第二次运行后的状态:")
	displayManagerStatus(storageManager2, cfg)

	// 展示职责分离
	fmt.Println("\n 职责分离演示:")
	fmt.Println(strings.Repeat("-", 50))
	demonstrateResponsibilitySeparation(storageManager2)

	// 关闭第二个实例
	fmt.Println("\n🔄 关闭StorageManager...")
	storageManager2.Close()

	fmt.Println("\n 架构职责分离演示完成!")
}

func displayManagerStatus(sm *manager.StorageManager, cfg *conf.Cfg) {
	// 检查SpaceManager状态
	spaceManager := sm.GetSpaceManager()
	if spaceManager != nil {
		fmt.Println("   SpaceManager: 正常运行")

		// 检查系统表空间
		if systemSpace, err := spaceManager.GetSpace(0); err == nil {
			logger.Debugf("    - 系统表空间(Space ID 0): %s\n",
				getSpaceStatus(systemSpace))
		}

		// 检查部分用户表空间
		userSpaces := []uint32{1, 2, 3, 100, 200}
		existingSpaces := 0
		for _, spaceID := range userSpaces {
			if userSpace, err := spaceManager.GetSpace(spaceID); err == nil {
				if existingSpaces < 3 { // 只显示前3个
					logger.Debugf("    - 用户表空间(Space ID %d): %s\n",
						spaceID, getSpaceStatus(userSpace))
				}
				existingSpaces++
			}
		}
		if existingSpaces > 3 {
			logger.Debugf("    - ... 还有 %d 个用户表空间\n", existingSpaces-3)
		}
	} else {
		fmt.Println("   SpaceManager: 未初始化")
	}

	// 检查其他管理器状态
	fmt.Println("   SegmentManager: 正常运行")
	if sm.GetSegmentManager() != nil {
		fmt.Println("    - 段管理功能: 已初始化")
	}

	fmt.Println("   BufferPoolManager: 正常运行")
	if bpm := sm.GetBufferPoolManager(); bpm != nil {
		fmt.Println("    - 缓冲池: 已初始化并优化")
	}

	// 显示表空间缓存状态
	if spaces, err := sm.ListSpaces(); err == nil {
		logger.Debugf("   表空间缓存: %d 个表空间\n", len(spaces))
		systemSpaces := 0
		userSpaces := 0
		for _, space := range spaces {
			if space.SpaceID < 100 {
				systemSpaces++
			} else {
				userSpaces++
			}
		}
		logger.Debugf("    - 系统表空间: %d 个\n", systemSpaces)
		logger.Debugf("    - 用户表空间: %d 个\n", userSpaces)
	}

	// 显示文件状态
	fmt.Println("  📁 文件系统状态:")
	files, _ := filepath.Glob(filepath.Join(cfg.DataDir, "*.ibd"))
	logger.Debugf("    - IBD文件数量: %d\n", len(files))
	for _, file := range files[:min(5, len(files))] { // 只显示前5个
		basename := filepath.Base(file)
		if info, err := os.Stat(file); err == nil {
			logger.Debugf("    - %s: %d KB\n", basename, info.Size()/1024)
		}
	}
	if len(files) > 5 {
		logger.Debugf("    - ... 还有 %d 个文件\n", len(files)-5)
	}
}

func demonstrateResponsibilitySeparation(sm *manager.StorageManager) {
	fmt.Println("1. 🗄️  SpaceManager职责演示:")
	spaceManager := sm.GetSpaceManager()
	if spaceManager != nil {
		fmt.Println("   - 管理所有IBD文件(包括系统表空间space_id=0)")
		fmt.Println("   - 负责表空间的创建、打开、关闭、删除")
		fmt.Println("   - 处理区段分配和页面I/O操作")

		// 演示创建新表空间
		testSpaceID := uint32(999)
		logger.Debugf("   - 尝试创建测试表空间(Space ID %d)...\n", testSpaceID)

		if testSpace, err := spaceManager.CreateSpace(testSpaceID, "test_table", false); err == nil {
			logger.Debugf("    SpaceManager成功创建表空间: %s\n", getSpaceStatus(testSpace))
		} else {
			logger.Debugf("     表空间创建失败或已存在: %v\n", err)
		}

		// 显示系统表空间也由SpaceManager管理
		if systemSpace, err := spaceManager.GetSpace(0); err == nil {
			logger.Debugf("   - 系统表空间(Space ID 0)也由SpaceManager统一管理: %s\n",
				getSpaceStatus(systemSpace))
		}
	}

	fmt.Println("\n2.  StorageManager协调职责演示:")
	fmt.Println("   - 顶层统一协调器，管理所有存储组件")
	fmt.Println("   - 协调SpaceManager、SegmentManager、BufferPool等")
	fmt.Println("   - 管理表空间缓存和生命周期")
	logger.Debugf("   - 当前管理的表空间缓存数量: %d\n", getTablespaceCount(sm))

	// 新增：展示SystemSpaceManager功能
	fmt.Println("\n3. 🏛️  SystemSpaceManager职责演示:")
	systemSpaceManager := sm.GetSystemSpaceManager()
	if systemSpaceManager != nil {
		fmt.Println("    SystemSpaceManager正常运行")
		logger.Debugf("   - 独立表空间模式: %v (innodb_file_per_table=ON)\n",
			systemSpaceManager.IsFilePerTableEnabled())

		// 展示ibdata1组件
		fmt.Println("   - ibdata1 (Space ID 0) 包含的系统组件:")
		if components := systemSpaceManager.GetIBData1Components(); components != nil {
			fmt.Println("      Undo日志管理器 (事务回滚)")
			fmt.Println("      插入缓冲管理器 (优化索引插入)")
			fmt.Println("      双写缓冲管理器 (防止页面损坏)")
			fmt.Println("      表空间管理页面 (FSP_HDR, XDES, INODE)")
			fmt.Println("      事务系统数据 (锁信息、事务状态)")
			fmt.Println("      数据字典根页面 (Page 5)")
		}

		// 展示独立表空间映射
		fmt.Println("   - 独立表空间映射关系:")
		independentSpaces := systemSpaceManager.ListIndependentTablespaces()
		mysqlSystemCount := 0
		for spaceID, info := range independentSpaces {
			if mysqlSystemCount < 5 { // 只显示前5个MySQL系统表
				logger.Debugf("     - %s -> Space ID %d (%s)\n",
					info.Name, spaceID, info.FilePath)
				mysqlSystemCount++
			}
		}
		if len(independentSpaces) > 5 {
			logger.Debugf("     - ... 还有 %d 个独立表空间\n", len(independentSpaces)-5)
		}

		// 展示统计信息
		if stats := systemSpaceManager.GetTablespaceStats(); stats != nil {
			fmt.Println("   - 表空间统计信息:")
			logger.Debugf("     - 系统表空间: Space ID %d (ibdata1)\n", stats.SystemSpaceID)
			logger.Debugf("     - MySQL系统表: %d 个独立表空间\n", stats.MySQLSystemTableCount)
			logger.Debugf("     - 用户表: %d 个独立表空间\n", stats.UserTableCount)
			logger.Debugf("     - information_schema: %d 个表空间\n", stats.InformationSchemaTableCount)
			logger.Debugf("     - performance_schema: %d 个表空间\n", stats.PerformanceSchemaTableCount)
		}
	}

	fmt.Println("\n4.  SegmentManager职责演示:")
	if segMgr := sm.GetSegmentManager(); segMgr != nil {
		fmt.Println("    SegmentManager正常运行")
		fmt.Println("   - 管理数据段和索引段")
		fmt.Println("   - 负责段的创建、分配、回收")
	}

	fmt.Println("\n5. 🚀 BufferPoolManager职责演示:")
	if bpm := sm.GetBufferPoolManager(); bpm != nil {
		fmt.Println("    OptimizedBufferPoolManager正常运行")
		fmt.Println("   - 管理页面缓存和LRU策略")
		fmt.Println("   - 优化I/O操作和预读机制")
	}

	// 演示职责委托
	fmt.Println("\n6. 🔄 职责委托流程演示:")
	fmt.Println("   场景: 创建新用户表")
	fmt.Println("   StorageManager -> 委托给SpaceManager创建表空间")
	fmt.Println("   StorageManager -> 委托给SegmentManager创建数据段")
	fmt.Println("   StorageManager -> 委托给BufferPoolManager管理页面缓存")

	fmt.Println("\n   场景: 系统表空间管理 (innodb_file_per_table=ON)")
	fmt.Println("   StorageManager -> 委托给SpaceManager管理ibdata1文件")
	fmt.Println("   SystemSpaceManager -> 管理ibdata1内部系统组件(Undo, 插入缓冲等)")
	fmt.Println("   SystemSpaceManager -> 映射MySQL系统表到独立表空间(Space ID 1-46)")
	fmt.Println("   SpaceManager -> 统一管理所有IBD文件(ibdata1 + 独立表空间)")

	fmt.Println("\n7. ✨ 基于innodb_file_per_table=ON的架构优势:")
	fmt.Println("   - 清晰的存储分离: ibdata1专门存储系统级数据")
	fmt.Println("   - 独立表空间: MySQL系统表、用户表各自独立的.ibd文件")
	fmt.Println("   - 统一的文件管理: SpaceManager统一管理所有IBD文件")
	fmt.Println("   - 专业的系统管理: SystemSpaceManager专门管理系统级组件")
	fmt.Println("   - 避免重复初始化: 智能检测已存在的IBD文件")
	fmt.Println("   - 清晰的职责分离: 每个管理器有明确的责任边界")
}

func getSpaceStatus(space interface{}) string {
	// 这里可以根据实际的Space接口实现获取状态信息
	return "活跃"
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
