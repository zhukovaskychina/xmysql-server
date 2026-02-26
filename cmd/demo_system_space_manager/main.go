package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/util"
)

func main() {
	fmt.Println("=" + strings.Repeat("=", 80))
	fmt.Println("🏛️  XMySQL InnoDB SystemSpaceManager 功能演示")
	fmt.Println("   基于 innodb_file_per_table=ON 配置的系统表空间管理")
	fmt.Println("=" + strings.Repeat("=", 80))

	// 创建演示目录
	demoDir := "demo_system_space_manager"
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
		InnodbBufferPoolSize: 32 * 1024 * 1024, // 32MB
		InnodbPageSize:       16384,            // 16KB
		InnodbLogFileSize:    10485760,         // 10MB
		InnodbLogBufferSize:  1048576,          // 1MB
	}

	logger.Debugf("\n 演示配置 (innodb_file_per_table=ON):\n")
	logger.Debugf("  - 数据目录: %s\n", cfg.DataDir)
	logger.Debugf("  - 系统表空间: %s\n", cfg.InnodbDataFilePath)
	logger.Debugf("  - 缓冲池大小: %d MB\n", cfg.InnodbBufferPoolSize/1024/1024)
	logger.Debugf("  - 页面大小: %d KB\n", cfg.InnodbPageSize/1024)

	// 初始化存储管理器
	fmt.Println("\n🚀 初始化 StorageManager...")
	fmt.Println(strings.Repeat("-", 60))

	storageManager := manager.NewStorageManager(cfg)
	if storageManager == nil {
		logger.Debugf(" 创建StorageManager失败\n")
		return
	}

	// 获取SystemSpaceManager
	systemSpaceManager := storageManager.GetSystemSpaceManager()
	if systemSpaceManager == nil {
		logger.Debugf(" SystemSpaceManager未初始化\n")
		return
	}

	fmt.Println(" StorageManager 和 SystemSpaceManager 初始化完成")

	// 演示1: 系统表空间架构分析
	demonstrateSystemSpaceArchitecture(systemSpaceManager)

	// 演示2: ibdata1组件管理
	demonstrateIBData1Components(systemSpaceManager)

	// 演示3: 独立表空间映射
	demonstrateIndependentTablespaces(systemSpaceManager)

	// 演示4: Space ID分配策略
	demonstrateSpaceIDAllocation(systemSpaceManager)

	// 演示5: 统计信息和监控
	demonstrateStatisticsAndMonitoring(systemSpaceManager)

	// 关闭管理器
	fmt.Println("\n🔄 关闭SystemSpaceManager...")
	systemSpaceManager.Close()
	storageManager.Close()

	fmt.Println("\n SystemSpaceManager功能演示完成!")
	fmt.Println("\n💡 关键特性总结:")
	fmt.Println("  • ibdata1专门存储系统级数据，不再存储用户表数据")
	fmt.Println("  • MySQL系统表采用独立表空间，便于管理和维护")
	fmt.Println("  • 清晰的Space ID分配策略，避免冲突")
	fmt.Println("  • 统一的文件管理，所有IBD文件由SpaceManager统一处理")
	fmt.Println("  • 专业的系统组件管理，每个组件职责明确")
}

func demonstrateSystemSpaceArchitecture(ssm *manager.SystemSpaceManager) {
	fmt.Println("\n  演示1: 系统表空间架构分析")
	fmt.Println(strings.Repeat("-", 60))

	logger.Debugf("独立表空间模式: %s\n", getEnabledStatus(ssm.IsFilePerTableEnabled()))

	if systemSpace := ssm.GetSystemSpace(); systemSpace != nil {
		logger.Debugf("系统表空间 (ibdata1): Space ID = 0\n")
		logger.Debugf("  - 文件名: %s\n", systemSpace.Name())
		logger.Debugf("  - 页面数量: %d\n", systemSpace.GetPageCount())
		logger.Debugf("  - 已用空间: %d KB\n", systemSpace.GetUsedSpace()/1024)
		logger.Debugf("  - 状态: %s\n", getActiveStatus(systemSpace.IsActive()))
	}

	fmt.Println("\n📖 ibdata1职责 (基于innodb_file_per_table=ON):")
	fmt.Println("   Undo Logs - 事务回滚数据")
	fmt.Println("   Insert Buffer - 延迟索引插入优化")
	fmt.Println("   Double Write Buffer - 崩溃恢复保护")
	fmt.Println("   System Management Pages - FSP_HDR, XDES, INODE页面")
	fmt.Println("   Transaction System Data - 事务锁信息")
	fmt.Println("   Data Dictionary Root - 数据字典根页面 (Page 5)")
	fmt.Println("   不再存储: 用户表数据和索引")
}

func demonstrateIBData1Components(ssm *manager.SystemSpaceManager) {
	fmt.Println("\n 演示2: ibdata1组件管理")
	fmt.Println(strings.Repeat("-", 60))

	components := ssm.GetIBData1Components()
	if components == nil {
		fmt.Println(" IBData1组件未初始化")
		return
	}

	fmt.Println("IBData1系统组件状态:")

	// Undo日志管理器
	if components.UndoLogs != nil {
		fmt.Println("   UndoLogManager: 正常运行")
		fmt.Println("     - 职责: 管理事务回滚日志")
		fmt.Println("     - 位置: ibdata1 多个页面")
	}

	// 插入缓冲管理器
	if components.InsertBuffer != nil {
		fmt.Println("   InsertBufferManager: 正常运行")
		fmt.Println("     - 职责: 优化二级索引插入性能")
		fmt.Println("     - 位置: ibdata1 专用页面")
	}

	// 双写缓冲管理器
	if components.DoubleWriteBuffer != nil {
		fmt.Println("   DoubleWriteBufferManager: 正常运行")
		fmt.Println("     - 职责: 防止页面部分写入导致的数据损坏")
		fmt.Println("     - 位置: ibdata1 连续64+64页面")
	}

	// 表空间管理页面
	if components.SpaceManagementPages != nil {
		fmt.Println("   SpaceManagementPages: 正常运行")
		fmt.Println("     - 职责: FSP_HDR, XDES, INODE页面管理")
		fmt.Println("     - 位置: ibdata1 前几个页面")
	}

	// 事务系统管理器
	if components.TransactionSystemData != nil {
		fmt.Println("   TransactionSystemManager: 正常运行")
		fmt.Println("     - 职责: 事务状态和锁信息管理")
		fmt.Println("     - 位置: ibdata1 页面6开始")
	}

	// 锁信息管理器
	if components.LockInfoManager != nil {
		fmt.Println("   LockInfoManager: 正常运行")
		fmt.Println("     - 职责: 行锁和表锁信息管理")
		fmt.Println("     - 位置: ibdata1 事务系统页面")
	}

	// 数据字典根页面
	if components.DataDictionaryRoot != nil {
		fmt.Println("   DataDictionaryRoot: 正常运行")
		fmt.Println("     - 职责: 数据字典元数据根页面")
		fmt.Println("     - 位置: ibdata1 页面5 (固定位置)")
		logger.Debugf("     - 最大表ID: %d\n", components.DataDictionaryRoot.GetMaxTableId())
		logger.Debugf("     - 最大索引ID: %d\n", components.DataDictionaryRoot.GetMaxIndexId())
		logger.Debugf("     - 最大Space ID: %d\n", components.DataDictionaryRoot.GetMaxSpaceId())
	}
}

func demonstrateIndependentTablespaces(ssm *manager.SystemSpaceManager) {
	fmt.Println("\n 演示3: 独立表空间映射")
	fmt.Println(strings.Repeat("-", 60))

	independentSpaces := ssm.ListIndependentTablespaces()
	logger.Debugf("独立表空间总数: %d\n", len(independentSpaces))

	// 分类统计
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

	// 显示MySQL系统表映射
	logger.Debugf("\n MySQL系统表独立表空间 (%d个):\n", len(mysqlSystemTables))
	count := 0
	for spaceID, info := range independentSpaces {
		if info.TableType == "system" && count < 8 {
			logger.Debugf("  • %s -> Space ID %d (%s)\n", info.Name, spaceID, info.FilePath)
			count++
		}
	}
	if len(mysqlSystemTables) > 8 {
		logger.Debugf("  • ... 还有 %d 个MySQL系统表\n", len(mysqlSystemTables)-8)
	}

	// 显示虚拟表映射示例
	logger.Debugf("\n information_schema 表空间 (%d个):\n", len(infoSchemaTables))
	if len(infoSchemaTables) > 0 {
		fmt.Println("  • Space ID范围: 100-199 (虚拟表)")
		fmt.Println("  • 特点: 动态生成，不存储持久数据")
	}

	logger.Debugf("\n⚡ performance_schema 表空间 (%d个):\n", len(perfSchemaTables))
	if len(perfSchemaTables) > 0 {
		fmt.Println("  • Space ID范围: 200-299 (性能监控)")
		fmt.Println("  • 特点: 内存表，重启后重新生成")
	}

	// 展示特定系统表的映射
	fmt.Println("\n🔑 关键系统表映射示例:")
	keyTables := []string{"mysql.user", "mysql.db", "mysql.tables_priv", "mysql.plugin"}
	for _, tableName := range keyTables {
		if spaceID, exists := ssm.GetMySQLSystemTableSpaceID(tableName); exists {
			logger.Debugf("  • %s -> Space ID %d\n", tableName, spaceID)
		}
	}
}

func demonstrateSpaceIDAllocation(ssm *manager.SystemSpaceManager) {
	fmt.Println("\n 演示4: Space ID分配策略")
	fmt.Println(strings.Repeat("-", 60))

	fmt.Println("Space ID分配规则 (基于innodb_file_per_table=ON):")
	fmt.Println()
	fmt.Println("┌─────────────────┬─────────────┬─────────────────────────────┐")
	fmt.Println("│    Space ID     │    用途     │           说明              │")
	fmt.Println("├─────────────────┼─────────────┼─────────────────────────────┤")
	fmt.Println("│       0         │ 系统表空间  │ ibdata1 (系统级数据)       │")
	fmt.Println("│     1 - 46      │ MySQL系统表 │ mysql.user, mysql.db等     │")
	fmt.Println("│   100 - 199     │ info_schema │ 虚拟表 (动态生成)          │")
	fmt.Println("│   200 - 299     │ perf_schema │ 性能监控表 (内存表)        │")
	fmt.Println("│    1000+        │   用户表    │ 用户自定义表               │")
	fmt.Println("└─────────────────┴─────────────┴─────────────────────────────┘")

	// 验证当前分配情况
	fmt.Println("\n📈 当前Space ID分配状况:")
	independentSpaces := ssm.ListIndependentTablespaces()

	systemCount := 0
	infoSchemaCount := 0
	perfSchemaCount := 0
	userCount := 0

	for spaceID, info := range independentSpaces {
		switch {
		case spaceID == 0:
			// 系统表空间，已单独处理
		case spaceID >= 1 && spaceID <= 46:
			systemCount++
		case spaceID >= 100 && spaceID <= 199:
			infoSchemaCount++
		case spaceID >= 200 && spaceID <= 299:
			perfSchemaCount++
		case spaceID >= 1000:
			userCount++
		}
		_ = info // 避免未使用变量警告
	}

	logger.Debugf("  • 系统表空间 (0): 1个 (ibdata1)\n")
	logger.Debugf("  • MySQL系统表 (1-46): %d个\n", systemCount)
	logger.Debugf("  • information_schema (100-199): %d个\n", infoSchemaCount)
	logger.Debugf("  • performance_schema (200-299): %d个\n", perfSchemaCount)
	logger.Debugf("  • 用户表 (1000+): %d个\n", userCount)

	fmt.Println("\n✨ 分配策略优势:")
	fmt.Println("  • 避免Space ID冲突")
	fmt.Println("  • 便于按类型管理表空间")
	fmt.Println("  • 支持大规模部署扩展")
	fmt.Println("  • 兼容MySQL官方实现")
}

func demonstrateStatisticsAndMonitoring(ssm *manager.SystemSpaceManager) {
	fmt.Println("\n 演示5: 统计信息和监控")
	fmt.Println(strings.Repeat("-", 60))

	stats := ssm.GetTablespaceStats()
	if stats == nil {
		fmt.Println(" 统计信息不可用")
		return
	}

	fmt.Println("📈 表空间统计信息:")
	logger.Debugf("  • 系统表空间ID: %d (ibdata1)\n", stats.SystemSpaceID)
	logger.Debugf("  • 系统表空间大小: %d KB\n", stats.SystemSpaceSize/1024)
	logger.Debugf("  • 独立表空间总数: %d\n", stats.IndependentSpaceCount)
	logger.Debugf("  • MySQL系统表数量: %d\n", stats.MySQLSystemTableCount)
	logger.Debugf("  • 用户表数量: %d\n", stats.UserTableCount)
	logger.Debugf("  • information_schema表: %d\n", stats.InformationSchemaTableCount)
	logger.Debugf("  • performance_schema表: %d\n", stats.PerformanceSchemaTableCount)

	// 计算存储利用率
	totalIndependentSpaces := stats.MySQLSystemTableCount +
		stats.UserTableCount +
		stats.InformationSchemaTableCount +
		stats.PerformanceSchemaTableCount

	fmt.Println("\n 存储分布分析:")
	if totalIndependentSpaces > 0 {
		mysqlPct := float64(stats.MySQLSystemTableCount) / float64(totalIndependentSpaces) * 100
		userPct := float64(stats.UserTableCount) / float64(totalIndependentSpaces) * 100
		infoPct := float64(stats.InformationSchemaTableCount) / float64(totalIndependentSpaces) * 100
		perfPct := float64(stats.PerformanceSchemaTableCount) / float64(totalIndependentSpaces) * 100

		logger.Debugf("  • MySQL系统表: %.1f%%\n", mysqlPct)
		logger.Debugf("  • 用户表: %.1f%%\n", userPct)
		logger.Debugf("  • information_schema: %.1f%%\n", infoPct)
		logger.Debugf("  • performance_schema: %.1f%%\n", perfPct)
	}

	fmt.Println("\n 监控建议:")
	fmt.Println("  • 定期检查ibdata1增长情况")
	fmt.Println("  • 监控独立表空间文件大小")
	fmt.Println("  • 关注Undo日志空间使用")
	fmt.Println("  • 观察插入缓冲使用率")
}

// 辅助函数
func getEnabledStatus(enabled bool) string {
	if enabled {
		return "启用 (innodb_file_per_table=ON)"
	}
	return "禁用 (innodb_file_per_table=OFF)"
}

func getActiveStatus(active bool) string {
	if active {
		return "活跃"
	}
	return "非活跃"
}
