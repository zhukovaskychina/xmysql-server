package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println("=== XMySQL InnoDB 存储引擎架构演示 ===")
	fmt.Println()

	// 创建临时演示目录
	demoDir := "demo_storage_arch"
	os.RemoveAll(demoDir) // 清理之前的演示数据
	os.MkdirAll(demoDir, 0755)
	defer func() {
		fmt.Println("\n清理演示数据...")
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

	logger.Debugf("演示目录: %s\n", demoDir)
	fmt.Println()

	// === 1. 初始化 StorageManager ===
	fmt.Println(" 第一步: 初始化 StorageManager")
	fmt.Println("StorageManager 是顶层存储管理器，统一协调所有存储操作")
	fmt.Println()

	sm := manager.NewStorageManager(cfg)
	if sm == nil {
		fmt.Println(" StorageManager 初始化失败")
		return
	}
	fmt.Println(" StorageManager 初始化成功")
	fmt.Println()

	// === 2. 展示架构组件关系 ===
	fmt.Println("  第二步: 查看架构组件关系")
	fmt.Println()

	// 2.1 SpaceManager (表空间管理)
	spaceManager := sm.GetSpaceManager()
	fmt.Println("📁 SpaceManager (表空间管理器):")
	fmt.Println("   - 职责: 管理所有表空间(.ibd文件)")
	fmt.Println("   - 管理: 系统表空间(space_id=0) + 用户表空间(space_id>0)")

	// 验证系统表空间
	systemSpace, err := spaceManager.GetSpace(0)
	if err == nil {
		logger.Debugf("   - 系统表空间: space_id=0, name=%s, active=%v\n",
			systemSpace.Name(), systemSpace.IsActive())
	}
	fmt.Println()

	// 2.2 SystemSpaceManager (系统表空间专用管理)
	systemSpaceManager := sm.GetSystemSpaceManager()
	fmt.Println("  SystemSpaceManager (系统表空间专用管理器):")
	fmt.Println("   - 职责: 专门管理系统表空间的特殊页面")
	fmt.Println("   - 管理: 页面0-7的系统页面，特别是第5页(数据字典根页面)")

	if systemSpaceManager != nil {
		// 获取系统页面信息
		for pageNo := uint32(0); pageNo <= 7; pageNo++ {
			pageInfo := systemSpaceManager.GetSystemPageInfo(pageNo)
			if pageInfo != nil {
				logger.Debugf("   - 系统页面%d: 类型=%d, 已加载=%v\n",
					pageNo, pageInfo.PageType, pageInfo.IsLoaded)
			}
		}

		// 加载数据字典根页面
		dictRootPage, err := systemSpaceManager.LoadDictRootPage()
		if err == nil {
			logger.Debugf("   - 数据字典根页面: MaxTableID=%d, MaxIndexID=%d\n",
				dictRootPage.MaxTableID, dictRootPage.MaxIndexID)
		}
	}
	fmt.Println()

	// 2.3 DictionaryManager (数据字典管理)
	dictManager := sm.GetDictionaryManager()
	fmt.Println(" DictionaryManager (数据字典管理器):")
	fmt.Println("   - 职责: 管理表、列、索引的元数据")
	fmt.Println("   - 存储: 在系统表空间的第5页作为根页面")

	if dictManager != nil {
		stats := dictManager.GetStats()
		logger.Debugf("   - 统计: 总表数=%d, 总索引数=%d, 缓存命中=%d\n",
			stats.TotalTables, stats.TotalIndexes, stats.CacheHits)
	}
	fmt.Println()

	// 2.4 SegmentManager (段管理)
	segmentManager := sm.GetSegmentManager()
	fmt.Println(" SegmentManager (段管理器):")
	fmt.Println("   - 职责: 管理表空间内的段(数据段、索引段)")
	fmt.Println("   - 协调: 与ExtentManager合作管理区和页面")
	logger.Debugf("   - 实例: %T\n", segmentManager)
	fmt.Println()

	// === 3. 演示创建用户表空间 ===
	fmt.Println("🆕 第三步: 演示创建用户表空间")
	fmt.Println()

	// 创建用户表空间
	tablespace, err := sm.CreateTablespace("test_db/user_table")
	if err != nil {
		logger.Debugf(" 创建用户表空间失败: %v\n", err)
	} else {
		logger.Debugf(" 创建用户表空间成功: space_id=%d, name=%s\n",
			tablespace.SpaceID, tablespace.Name)

		// 验证表空间已被SpaceManager管理
		userSpace, err := spaceManager.GetSpace(tablespace.SpaceID)
		if err == nil {
			logger.Debugf("   - SpaceManager中的表空间: name=%s, active=%v\n",
				userSpace.Name(), userSpace.IsActive())
		}
	}
	fmt.Println()

	// === 4. 演示数据字典操作 ===
	fmt.Println(" 第四步: 演示数据字典操作")
	fmt.Println()

	if dictManager != nil {
		// 创建表定义
		columns := []manager.ColumnDef{
			{
				ColumnID: 1,
				Name:     "id",
				Type:     1, // INT
				Length:   4,
				Nullable: false,
			},
			{
				ColumnID: 2,
				Name:     "name",
				Type:     15, // VARCHAR
				Length:   255,
				Nullable: true,
			},
		}

		table, err := dictManager.CreateTable("user_table", tablespace.SpaceID, columns)
		if err != nil {
			logger.Debugf(" 创建表定义失败: %v\n", err)
		} else {
			logger.Debugf(" 创建表定义成功: table_id=%d, name=%s\n",
				table.TableID, table.Name)
			logger.Debugf("   - 表空间ID: %d\n", table.SpaceID)
			logger.Debugf("   - 列数: %d\n", len(table.Columns))
			logger.Debugf("   - 段ID: %d\n", table.SegmentID)

			// 验证数据字典根页面已更新
			if systemSpaceManager != nil {
				dictRootPage, err := systemSpaceManager.LoadDictRootPage()
				if err == nil {
					logger.Debugf("   - 更新后的MaxTableID: %d\n", dictRootPage.MaxTableID)
				}
			}
		}
	}
	fmt.Println()

	// === 5. 展示架构优势 ===
	fmt.Println(" 第五步: 架构设计优势")
	fmt.Println()
	fmt.Println("1. 职责分离:")
	fmt.Println("   - SpaceManager: 专注表空间和IBD文件管理")
	fmt.Println("   - SystemSpaceManager: 专门处理系统表空间特殊需求")
	fmt.Println("   - StorageManager: 提供统一协调和事务管理")
	fmt.Println()
	fmt.Println("2. 扩展性:")
	fmt.Println("   - 各组件相对独立，便于优化和测试")
	fmt.Println("   - 新存储特性可在相应层次添加")
	fmt.Println()
	fmt.Println("3. 兼容性:")
	fmt.Println("   - 系统表空间设计兼容 MySQL InnoDB")
	fmt.Println("   - 数据字典根页面存储在标准第5页")
	fmt.Println()

	// === 6. 资源清理 ===
	fmt.Println("🧹 第六步: 资源清理")
	err = sm.Close()
	if err != nil {
		logger.Debugf("  关闭StorageManager时出现警告: %v\n", err)
	} else {
		fmt.Println(" StorageManager 已正常关闭")
	}

	fmt.Println()
	fmt.Println("=== 演示完成 ===")
}
