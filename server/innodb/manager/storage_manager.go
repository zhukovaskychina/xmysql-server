package manager

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
)

/***

1. B+Tree Manager 的角色和职责
btreeManager 负责管理数据库中的所有B+Tree索引结构，包括：

主键索引树（clustered index）

二级索引树（secondary indexes）

可能还有临时索引或者系统内置索引等

它维护的是所有活跃索引的元数据与结构，管理索引的创建、加载、缓存、查找、插入、删除等操作。

B+Tree在数据库里表现为页的链式结构，需要**页缓存管理（buffer pool）**配合进行读写。

2. B+Tree的创建与内存加载时机
索引的B+Tree结构并不是一次性全加载到内存的。
B+Tree的节点页是按需加载的，也就是说：

首次访问索引（第一次查询或扫描）时，才会触发从磁盘（表空间）加载根节点页到内存，之后根据查找路径逐步加载子节点页。

索引的叶子节点页和内部节点页根据访问频率会缓存在BufferPool中。

索引本身的元信息（比如根页号、页数、页号范围、索引字段、类型等）会被管理模块维护在内存中，用于快速定位根节点及索引结构。

索引的具体页数据，实际是被BufferPool管理按需加载、淘汰。

3. btreeManager的设计建议
内存中存储索引元信息的Map结构，key可能是 (tablespaceID, indexID) 或 (tableID, indexName)，value是索引树的入口信息，比如根页ID，索引元数据结构等。

当请求访问某个索引时，btreeManager负责：

如果索引元信息不存在，则加载索引元信息（元数据页）；

维护索引树根节点的引用，触发后续页按需加载；

对索引的增删改查操作，委托BufferPool和PageManager进行具体页的加载与写入。

索引的创建由数据库DDL流程调用，最终通过btreeManager.CreateIndex()等接口创建B+Tree元信息，初始化根页，持久化索引元数据。

4. btreeManager 和 StorageManager 的关系
StorageManager 管理存储资源、表空间和页的读写，提供统一的页访问接口。

btreeManager 管理索引树的逻辑结构和操作，是建立在StorageManager之上的一层。

btreeManager依赖StorageManager提供的页访问接口（如GetPage(spaceID, pageNo)，AllocPage等）来操作B+Tree的具体页。

5. 举例说明典型工作流程
数据库启动/加载表时：

StorageManager会确保表空间、段、页、缓冲池等初始化；

btreeManager可能根据元数据，加载索引的元信息，但不会立即加载所有节点页；

执行查询索引时：

btreeManager根据根页号，从StorageManager中获取页，按查找路径逐级加载索引节点页；

访问的页缓存到BufferPool；

创建索引时：

btreeManager新建索引元信息结构，分配根页，初始化空树结构；

更新系统元数据，持久化；

关闭表时：

btreeManager可能清理内存索引元数据引用，StorageManager负责flush所有页和资源释放。

6.计的b+tree管理器内容
一个索引树的元信息结构体，保存根页号、索引字段、类型、统计信息等。

一个索引树管理结构，负责：

查找路径的递归或迭代算法；

插入、删除、更新索引项的逻辑；

处理页分裂、合并；

协调BufferPool对页的加载和写回。

一个顶层管理结构（btreeManager），管理所有索引树的创建、销毁、查找。

总结
组件	主要职责	生命周期
StorageManager	管理表空间、段、页、BufferPool、事务管理	启动时创建，运行时长存
btreeManager	管理B+Tree索引元信息，索引树结构操作，依赖StorageManager页访问	启动时加载索引元信息，按需加载页，按需创建索引树**/
// TablespaceHandle represents a handle to a tablespace
type TablespaceHandle struct {
	SpaceID       uint32
	DataSegmentID uint64
	Name          string
}

// StorageManager implements the storage management interface
type StorageManager struct {
	spaceMgr    basic.SpaceManager
	segmentMgr  *SegmentManager
	bufferPool  *buffer_pool.BufferPool
	pageMgr     *DefaultPageManager
	tablespaces map[string]*TablespaceHandle
	nextTxID    uint64
	mu          sync.RWMutex
}

func (sm *StorageManager) Init() {
	// 初始化存储管理器
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 确保所有组件都已初始化
	if sm.spaceMgr == nil || sm.bufferPool == nil || sm.pageMgr == nil || sm.segmentMgr == nil {
		panic("storage manager components not properly initialized")
	}
}

func (sm *StorageManager) GetBufferPoolManager() *OptimizedBufferPoolManager {
	// 创建优化的BufferPoolManager的配置
	config := &BufferPoolConfig{
		PoolSize:        16384,       // 默认缓冲池大小
		PageSize:        16384,       // 页面大小
		FlushInterval:   time.Second, // 刷新间隔
		YoungListRatio:  0.75,        // young区比例
		OldListRatio:    0.25,        // old区比例
		OldBlockTime:    1000,        // old区块时间（毫秒）
		PrefetchWorkers: 2,           // 预读工作线程数
		MaxQueueSize:    1000,        // 最大队列大小
		StorageProvider: &StorageProviderAdapter{
			spaceManager: sm.spaceMgr,
			sm:           sm,
		},
	}

	// 尝试创建优化的BufferPoolManager
	optimizedBpm, err := NewOptimizedBufferPoolManager(config)
	if err == nil {
		// 成功创建优化版本，直接返回
		return optimizedBpm
	}

	return nil
}

func (sm *StorageManager) OpenSpace(spaceID uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 委托给SpaceManager处理
	space, err := sm.spaceMgr.GetSpace(spaceID)
	if err != nil {
		return fmt.Errorf("failed to open space %d: %v", spaceID, err)
	}

	// 激活空间
	space.SetActive(true)
	return nil
}

func (sm *StorageManager) CloseSpace(spaceID uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 获取空间
	space, err := sm.spaceMgr.GetSpace(spaceID)
	if err != nil {
		return fmt.Errorf("failed to get space %d: %v", spaceID, err)
	}

	// 先刷新所有脏页
	if err := sm.Flush(); err != nil {
		return fmt.Errorf("failed to flush space %d: %v", spaceID, err)
	}

	// 停用空间
	space.SetActive(false)
	return nil
}

func (sm *StorageManager) DeleteSpace(spaceID uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 先关闭空间
	if err := sm.CloseSpace(spaceID); err != nil {
		return err
	}

	// 从tablespaces中删除
	for name, handle := range sm.tablespaces {
		if handle.SpaceID == spaceID {
			delete(sm.tablespaces, name)
			break
		}
	}

	// 委托给SpaceManager删除
	return sm.spaceMgr.DropSpace(spaceID)
}

func (sm *StorageManager) GetSpaceInfo(spaceID uint32) (*basic.SpaceInfo, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// 获取空间
	space, err := sm.spaceMgr.GetSpace(spaceID)
	if err != nil {
		return nil, fmt.Errorf("space %d not found: %v", spaceID, err)
	}

	// 构建SpaceInfo
	info := &basic.SpaceInfo{
		SpaceID:      space.ID(),
		Name:         space.Name(),
		PageSize:     16384, // 固定16KB页面大小
		TotalPages:   uint64(space.GetPageCount()),
		ExtentSize:   64,    // 标准64页一个区
		IsCompressed: false, // 暂不支持压缩
		State:        "active",
	}

	if space.IsActive() {
		info.State = "active"
	} else {
		info.State = "inactive"
	}

	return info, nil
}

func (sm *StorageManager) ListSpaces() ([]basic.SpaceInfo, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var spaces []basic.SpaceInfo

	// 遍历所有tablespace handles
	for _, handle := range sm.tablespaces {
		info, err := sm.GetSpaceInfo(handle.SpaceID)
		if err != nil {
			continue // 跳过错误的空间
		}
		spaces = append(spaces, *info)
	}

	return spaces, nil
}

func (sm *StorageManager) BeginTransaction() (uint64, error) {
	txID := atomic.AddUint64(&sm.nextTxID, 1)
	return txID, nil
}

func (sm *StorageManager) CommitTransaction(txID uint64) error {
	// 实现事务提交逻辑
	// 1. 刷新所有脏页
	if err := sm.Flush(); err != nil {
		return fmt.Errorf("failed to flush during commit: %v", err)
	}

	// 2. TODO: 写入事务日志

	return nil
}

func (sm *StorageManager) RollbackTransaction(txID uint64) error {
	// 实现事务回滚逻辑
	// TODO: 恢复到事务开始前的状态
	return nil
}

func (sm *StorageManager) Sync(spaceID uint32) error {
	// 同步指定空间的所有数据到磁盘
	// 使用Flush方法来刷新所有数据
	return sm.Flush()
}

// NewStorageManager creates a new StorageManager instance with conf
func NewStorageManager(conf *conf.Cfg) *StorageManager {
	// 获取配置参数
	dataDir := conf.InnodbDataDir
	if dataDir == "" {
		dataDir = conf.DataDir // 回退到主数据目录
	}
	if dataDir == "" {
		dataDir = "data" // 默认数据目录
	}

	bufferPoolSize := conf.InnodbBufferPoolSize
	if bufferPoolSize <= 0 {
		bufferPoolSize = 134217728 // 默认128MB
	}

	pageSize := conf.InnodbPageSize
	if pageSize <= 0 {
		pageSize = 16384 // 默认16KB
	}

	// Create storage manager instance
	sm := &StorageManager{
		tablespaces: make(map[string]*TablespaceHandle),
		nextTxID:    1,
	}

	// Initialize space manager with data directory
	sm.spaceMgr = NewSpaceManager(dataDir)

	// Initialize buffer pool
	bufferPoolConfig := &buffer_pool.BufferPoolConfig{
		TotalPages:     uint32(bufferPoolSize / pageSize),
		PageSize:       uint32(pageSize),
		BufferPoolSize: uint64(bufferPoolSize),
		StorageManager: sm.spaceMgr,
	}
	sm.bufferPool = buffer_pool.NewBufferPool(bufferPoolConfig)

	// Initialize page manager
	pageConfig := &PageConfig{
		CacheSize:      1000,
		DirtyThreshold: 0.7,
		EvictionPolicy: "LRU",
	}
	sm.pageMgr = NewPageManager(sm.bufferPool, pageConfig)

	// Initialize segment manager
	sm.segmentMgr = NewSegmentManager(sm.bufferPool)

	// 初始化系统表空间和文件，就像MySQL一样
	if err := sm.initializeSystemTablespaces(conf); err != nil {
		panic(fmt.Sprintf("Failed to initialize system tablespaces: %v", err))
	}

	return sm
}

// initializeSystemTablespaces 初始化系统表空间，创建必要的系统ibd文件
func (sm *StorageManager) initializeSystemTablespaces(conf *conf.Cfg) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 1. 创建系统表空间 (ibdata1)
	if err := sm.createSystemTablespace(conf); err != nil {
		return fmt.Errorf("failed to create system tablespace: %v", err)
	}

	// 2. 创建MySQL系统数据库表空间
	if err := sm.createMySQLSystemTablespaces(); err != nil {
		return fmt.Errorf("failed to create MySQL system tablespaces: %v", err)
	}

	// 3. 创建information_schema表空间
	if err := sm.createInformationSchemaTablespaces(); err != nil {
		return fmt.Errorf("failed to create information_schema tablespaces: %v", err)
	}

	// 4. 创建performance_schema表空间
	if err := sm.createPerformanceSchemaTablespaces(); err != nil {
		return fmt.Errorf("failed to create performance_schema tablespaces: %v", err)
	}

	// 5. 初始化mysql.user表的默认数据
	if err := sm.initializeMySQLUserData(); err != nil {
		return fmt.Errorf("failed to initialize MySQL user data: %v", err)
	}

	return nil
}

// createSystemTablespace 创建系统表空间 (ibdata1)
func (sm *StorageManager) createSystemTablespace(conf *conf.Cfg) error {
	// 解析数据文件路径配置 (例如: ibdata1:100M:autoextend)
	dataFilePath := conf.InnodbDataFilePath
	if dataFilePath == "" {
		dataFilePath = "ibdata1:100M:autoextend"
	}

	// 解析文件名和大小
	parts := strings.Split(dataFilePath, ":")
	if len(parts) < 2 {
		return fmt.Errorf("invalid data file path format: %s", dataFilePath)
	}

	fileName := parts[0]

	// 检查系统表空间是否已经存在
	if existingSpace, err := sm.spaceMgr.GetSpace(0); err == nil {
		// 系统表空间已存在，创建handle
		handle := &TablespaceHandle{
			SpaceID:       0,
			DataSegmentID: 0,
			Name:          fileName,
		}
		sm.tablespaces[fileName] = handle

		// 确保表空间是活动的
		existingSpace.SetActive(true)

		fmt.Printf("System tablespace already exists: %s (Space ID: 0)\n", fileName)
		return nil
	}

	// 创建系统表空间 (Space ID = 0)
	systemSpace, err := sm.spaceMgr.CreateSpace(0, fileName, true)
	if err != nil {
		return fmt.Errorf("failed to create system space: %v", err)
	}

	// 创建系统表空间的handle
	handle := &TablespaceHandle{
		SpaceID:       0,
		DataSegmentID: 0,
		Name:          fileName,
	}
	sm.tablespaces[fileName] = handle

	// 激活系统表空间
	systemSpace.SetActive(true)

	fmt.Printf("Created system tablespace: %s (Space ID: 0)\n", fileName)
	return nil
}

// createMySQLSystemTablespaces 创建MySQL系统数据库的表空间
func (sm *StorageManager) createMySQLSystemTablespaces() error {
	systemTables := []string{
		"mysql/user",                      // 用户表
		"mysql/db",                        // 数据库权限表
		"mysql/tables_priv",               // 表权限表
		"mysql/columns_priv",              // 列权限表
		"mysql/procs_priv",                // 存储过程权限表
		"mysql/proxies_priv",              // 代理权限表
		"mysql/role_edges",                // 角色边表
		"mysql/default_roles",             // 默认角色表
		"mysql/global_grants",             // 全局授权表
		"mysql/password_history",          // 密码历史表
		"mysql/component",                 // 组件表
		"mysql/server_cost",               // 服务器成本表
		"mysql/engine_cost",               // 引擎成本表
		"mysql/time_zone",                 // 时区表
		"mysql/time_zone_name",            // 时区名称表
		"mysql/time_zone_transition",      // 时区转换表
		"mysql/time_zone_transition_type", // 时区转换类型表
		"mysql/help_topic",                // 帮助主题表
		"mysql/help_category",             // 帮助分类表
		"mysql/help_relation",             // 帮助关系表
		"mysql/help_keyword",              // 帮助关键字表
		"mysql/plugin",                    // 插件表
		"mysql/servers",                   // 服务器表
		"mysql/func",                      // 函数表
		"mysql/general_log",               // 通用日志表
		"mysql/slow_log",                  // 慢查询日志表
	}

	for i, tableName := range systemTables {
		spaceID := uint32(i + 1) // 从Space ID 1开始

		// 检查表空间是否已经存在（先检查我们的 tablespaces map）
		if existingHandle, exists := sm.tablespaces[tableName]; exists {
			fmt.Printf("System table already exists in map: %s (Space ID: %d)\n", tableName, existingHandle.SpaceID)
			continue
		}

		// 检查 space manager 中是否已经存在
		if existingSpace, err := sm.spaceMgr.GetSpace(spaceID); err == nil {
			// 表空间已存在，创建handle
			handle := &TablespaceHandle{
				SpaceID:       spaceID,
				DataSegmentID: uint64(spaceID),
				Name:          tableName,
			}
			sm.tablespaces[tableName] = handle

			// 确保表空间是活动的
			existingSpace.SetActive(true)

			fmt.Printf("System table already exists in space manager: %s (Space ID: %d)\n", tableName, spaceID)
			continue
		}

		// 创建表空间
		_, err := sm.spaceMgr.CreateSpace(spaceID, tableName, true)
		if err != nil {
			// 如果创建失败但是错误是已存在，则尝试获取已存在的表空间
			if strings.Contains(err.Error(), "already exists") {
				fmt.Printf("System table already exists (caught in CreateSpace): %s (Space ID: %d)\n", tableName, spaceID)
				// 创建handle
				handle := &TablespaceHandle{
					SpaceID:       spaceID,
					DataSegmentID: uint64(spaceID),
					Name:          tableName,
				}
				sm.tablespaces[tableName] = handle
				continue
			}
			return fmt.Errorf("failed to create system table %s: %v", tableName, err)
		}

		// 创建handle
		handle := &TablespaceHandle{
			SpaceID:       spaceID,
			DataSegmentID: uint64(spaceID),
			Name:          tableName,
		}
		sm.tablespaces[tableName] = handle

		fmt.Printf("Created system table: %s (Space ID: %d)\n", tableName, spaceID)
	}

	return nil
}

// createInformationSchemaTablespaces 创建information_schema表空间
func (sm *StorageManager) createInformationSchemaTablespaces() error {
	infoSchemaTables := []string{
		"information_schema/schemata",
		"information_schema/tables",
		"information_schema/columns",
		"information_schema/statistics",
		"information_schema/key_column_usage",
		"information_schema/table_constraints",
		"information_schema/referential_constraints",
		"information_schema/views",
		"information_schema/triggers",
		"information_schema/routines",
		"information_schema/parameters",
		"information_schema/events",
		"information_schema/partitions",
		"information_schema/engines",
		"information_schema/plugins",
		"information_schema/processlist",
		"information_schema/user_privileges",
		"information_schema/schema_privileges",
		"information_schema/table_privileges",
		"information_schema/column_privileges",
	}

	baseSpaceID := uint32(100) // information_schema从Space ID 100开始

	for i, tableName := range infoSchemaTables {
		spaceID := baseSpaceID + uint32(i)

		// 检查表空间是否已经存在（先检查我们的 tablespaces map）
		if existingHandle, exists := sm.tablespaces[tableName]; exists {
			fmt.Printf("Information_schema table already exists in map: %s (Space ID: %d)\n", tableName, existingHandle.SpaceID)
			continue
		}

		// 检查 space manager 中是否已经存在
		if existingSpace, err := sm.spaceMgr.GetSpace(spaceID); err == nil {
			// 表空间已存在，创建handle
			handle := &TablespaceHandle{
				SpaceID:       spaceID,
				DataSegmentID: uint64(spaceID),
				Name:          tableName,
			}
			sm.tablespaces[tableName] = handle

			// 确保表空间是活动的
			existingSpace.SetActive(true)

			fmt.Printf("Information_schema table already exists in space manager: %s (Space ID: %d)\n", tableName, spaceID)
			continue
		}

		// 创建表空间
		_, err := sm.spaceMgr.CreateSpace(spaceID, tableName, true)
		if err != nil {
			// 如果创建失败但是错误是已存在，则尝试获取已存在的表空间
			if strings.Contains(err.Error(), "already exists") {
				fmt.Printf("Information_schema table already exists (caught in CreateSpace): %s (Space ID: %d)\n", tableName, spaceID)
				// 创建handle
				handle := &TablespaceHandle{
					SpaceID:       spaceID,
					DataSegmentID: uint64(spaceID),
					Name:          tableName,
				}
				sm.tablespaces[tableName] = handle
				continue
			}
			return fmt.Errorf("failed to create information_schema table %s: %v", tableName, err)
		}

		// 创建handle
		handle := &TablespaceHandle{
			SpaceID:       spaceID,
			DataSegmentID: uint64(spaceID),
			Name:          tableName,
		}
		sm.tablespaces[tableName] = handle

		fmt.Printf("Created information_schema table: %s (Space ID: %d)\n", tableName, spaceID)
	}

	return nil
}

// createPerformanceSchemaTablespaces 创建performance_schema表空间
func (sm *StorageManager) createPerformanceSchemaTablespaces() error {
	perfSchemaTables := []string{
		"performance_schema/accounts",
		"performance_schema/cond_instances",
		"performance_schema/events_stages_current",
		"performance_schema/events_stages_history",
		"performance_schema/events_stages_history_long",
		"performance_schema/events_statements_current",
		"performance_schema/events_statements_history",
		"performance_schema/events_statements_history_long",
		"performance_schema/events_waits_current",
		"performance_schema/events_waits_history",
		"performance_schema/events_waits_history_long",
		"performance_schema/file_instances",
		"performance_schema/file_summary_by_event_name",
		"performance_schema/file_summary_by_instance",
		"performance_schema/host_cache",
		"performance_schema/hosts",
		"performance_schema/mutex_instances",
		"performance_schema/objects_summary_global_by_type",
		"performance_schema/performance_timers",
		"performance_schema/rwlock_instances",
		"performance_schema/setup_actors",
		"performance_schema/setup_consumers",
		"performance_schema/setup_instruments",
		"performance_schema/setup_objects",
		"performance_schema/setup_timers",
		"performance_schema/socket_instances",
		"performance_schema/socket_summary_by_event_name",
		"performance_schema/socket_summary_by_instance",
		"performance_schema/table_io_waits_summary_by_index_usage",
		"performance_schema/table_io_waits_summary_by_table",
		"performance_schema/table_lock_waits_summary_by_table",
		"performance_schema/threads",
		"performance_schema/users",
	}

	baseSpaceID := uint32(200) // performance_schema从Space ID 200开始

	for i, tableName := range perfSchemaTables {
		spaceID := baseSpaceID + uint32(i)

		// 检查表空间是否已经存在（先检查我们的 tablespaces map）
		if existingHandle, exists := sm.tablespaces[tableName]; exists {
			fmt.Printf("Performance_schema table already exists in map: %s (Space ID: %d)\n", tableName, existingHandle.SpaceID)
			continue
		}

		// 检查 space manager 中是否已经存在
		if existingSpace, err := sm.spaceMgr.GetSpace(spaceID); err == nil {
			// 表空间已存在，创建handle
			handle := &TablespaceHandle{
				SpaceID:       spaceID,
				DataSegmentID: uint64(spaceID),
				Name:          tableName,
			}
			sm.tablespaces[tableName] = handle

			// 确保表空间是活动的
			existingSpace.SetActive(true)

			fmt.Printf("Performance_schema table already exists in space manager: %s (Space ID: %d)\n", tableName, spaceID)
			continue
		}

		// 创建表空间
		_, err := sm.spaceMgr.CreateSpace(spaceID, tableName, true)
		if err != nil {
			// 如果创建失败但是错误是已存在，则尝试获取已存在的表空间
			if strings.Contains(err.Error(), "already exists") {
				fmt.Printf("Performance_schema table already exists (caught in CreateSpace): %s (Space ID: %d)\n", tableName, spaceID)
				// 创建handle
				handle := &TablespaceHandle{
					SpaceID:       spaceID,
					DataSegmentID: uint64(spaceID),
					Name:          tableName,
				}
				sm.tablespaces[tableName] = handle
				continue
			}
			return fmt.Errorf("failed to create performance_schema table %s: %v", tableName, err)
		}

		// 创建handle
		handle := &TablespaceHandle{
			SpaceID:       spaceID,
			DataSegmentID: uint64(spaceID),
			Name:          tableName,
		}
		sm.tablespaces[tableName] = handle

		fmt.Printf("Created performance_schema table: %s (Space ID: %d)\n", tableName, spaceID)
	}

	return nil
}

// CreateSegment creates a new segment
func (sm *StorageManager) CreateSegment(spaceID uint32, purpose basic.SegmentPurpose) (basic.Segment, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 根据purpose选择合适的segment类型
	segType := SEGMENT_TYPE_DATA
	if purpose == basic.SegmentPurposeNonLeaf {
		segType = SEGMENT_TYPE_INDEX
	}

	return sm.segmentMgr.CreateSegment(spaceID, segType, false)
}

// GetSegment retrieves an existing segment
func (sm *StorageManager) GetSegment(segmentID uint64) (basic.Segment, error) {
	segment := sm.segmentMgr.GetSegment(uint32(segmentID))
	if segment == nil {
		return nil, fmt.Errorf("segment %d not found", segmentID)
	}
	return segment, nil
}

// FreeSegment frees a segment
func (sm *StorageManager) FreeSegment(segmentID uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// TODO: 实现segment释放逻辑
	// 1. 释放segment的所有页面
	// 2. 从segment管理器中删除
	// 暂时返回nil，等待SegmentManager实现FreeSegment方法
	return nil
}

// AllocateExtent allocates a new extent
func (sm *StorageManager) AllocateExtent(spaceID uint32, purpose basic.ExtentPurpose) (basic.Extent, error) {
	return sm.spaceMgr.AllocateExtent(spaceID, purpose)
}

// FreeExtent frees an extent
func (sm *StorageManager) FreeExtent(spaceID, extentID uint32) error {
	return sm.spaceMgr.FreeExtent(spaceID, extentID)
}

// GetPage retrieves a page using DefaultPageManager
func (sm *StorageManager) GetPage(spaceID, pageNo uint32) (basic.IPage, error) {
	// 直接使用DefaultPageManager获取页面
	return sm.pageMgr.GetPage(spaceID, pageNo)
}

// AllocPage allocates a new page using DefaultPageManager
func (sm *StorageManager) AllocPage(spaceID uint32, pageType basic.PageType) (basic.IPage, error) {
	// Convert basic.PageType to common.PageType
	commonPageType := common.PageType(pageType)

	// 直接使用DefaultPageManager创建页面
	page, err := sm.pageMgr.CreatePage(commonPageType)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate page: %v", err)
	}

	return page, nil
}

// FreePage frees a page
func (sm *StorageManager) FreePage(spaceID, pageNo uint32) error {
	// Use page manager to flush the page before freeing
	return sm.pageMgr.FlushPage(spaceID, pageNo)
}

// Begin starts a new transaction
func (sm *StorageManager) Begin() (basic.Transaction, error) {
	txID := atomic.AddUint64(&sm.nextTxID, 1)
	return newTransaction(txID, sm), nil
}

// Commit commits a transaction
func (sm *StorageManager) Commit(tx basic.Transaction) error {
	return tx.Commit()
}

// Rollback rolls back a transaction
func (sm *StorageManager) Rollback(tx basic.Transaction) error {
	return tx.Rollback()
}

// Flush flushes all changes to disk
func (sm *StorageManager) Flush() error {
	// Use page manager to flush all pages
	return sm.pageMgr.FlushAll()
}

// Close releases all resources
func (sm *StorageManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Flush all changes
	if err := sm.Flush(); err != nil {
		return fmt.Errorf("failed to flush during close: %v", err)
	}

	// TODO: Close buffer pool when method is available
	// if err := sm.bufferPool.Close(); err != nil {
	//     return fmt.Errorf("failed to close buffer pool: %v", err)
	// }

	// Close space manager
	if err := sm.spaceMgr.Close(); err != nil {
		return fmt.Errorf("failed to close space manager: %v", err)
	}

	return nil
}

// CreateTablespace creates a new tablespace
func (sm *StorageManager) CreateTablespace(name string) (*TablespaceHandle, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 检查是否已存在
	if _, exists := sm.tablespaces[name]; exists {
		return nil, fmt.Errorf("tablespace %s already exists", name)
	}

	// 创建新的表空间
	spaceID, err := sm.spaceMgr.CreateTableSpace(name)
	if err != nil {
		return nil, fmt.Errorf("failed to create tablespace: %v", err)
	}

	// 创建数据段
	_, err = sm.CreateSegment(spaceID, basic.SegmentPurposeLeaf)
	if err != nil {
		return nil, fmt.Errorf("failed to create data segment: %v", err)
	}

	// 创建handle
	handle := &TablespaceHandle{
		SpaceID:       spaceID,
		DataSegmentID: uint64(spaceID), // 暂时使用spaceID作为segmentID
		Name:          name,
	}

	sm.tablespaces[name] = handle
	return handle, nil
}

// GetTablespace gets a tablespace handle
func (sm *StorageManager) GetTablespace(name string) (*TablespaceHandle, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	handle, exists := sm.tablespaces[name]
	if !exists {
		return nil, fmt.Errorf("tablespace %s not found", name)
	}

	return handle, nil
}

func (sm *StorageManager) GetSegmentManager() *SegmentManager {
	return sm.segmentMgr
}

func (sm *StorageManager) GetSpaceManager() basic.SpaceManager {
	return sm.spaceMgr
}

func (sm *StorageManager) GetPageManager() basic.PageManager {
	return nil
}

// Transaction implementation
type txImpl struct {
	id        uint64
	sm        *StorageManager
	writes    []func()
	committed bool
	mu        sync.Mutex
}

// newTransaction creates a new transaction
func newTransaction(id uint64, sm *StorageManager) *txImpl {
	return &txImpl{
		id:     id,
		sm:     sm,
		writes: make([]func(), 0),
	}
}

// ID returns the transaction ID
func (t *txImpl) ID() uint64 {
	return t.id
}

// Commit commits the transaction
func (t *txImpl) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.committed {
		return nil
	}

	// 执行所有写操作
	for _, write := range t.writes {
		write()
	}

	// 提交事务
	if err := t.sm.CommitTransaction(t.id); err != nil {
		return fmt.Errorf("failed to commit transaction %d: %v", t.id, err)
	}

	t.committed = true
	return nil
}

// Rollback rolls back the transaction
func (t *txImpl) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.committed {
		return fmt.Errorf("transaction %d already committed", t.id)
	}

	// 回滚事务
	if err := t.sm.RollbackTransaction(t.id); err != nil {
		return fmt.Errorf("failed to rollback transaction %d: %v", t.id, err)
	}

	t.writes = nil
	return nil
}

// AddWrite adds a write operation to the transaction
func (t *txImpl) AddWrite(writeFn func()) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.committed {
		t.writes = append(t.writes, writeFn)
	}
}

// StorageProviderAdapter 适配器，将SpaceManager适配为StorageProvider
type StorageProviderAdapter struct {
	spaceManager basic.SpaceManager
	sm           *StorageManager
}

// ReadPage 从存储中读取页面
func (spa *StorageProviderAdapter) ReadPage(spaceID, pageNo uint32) ([]byte, error) {
	// 简化实现：返回一个空页面
	pageSize := uint32(16384) // 16KB页面
	data := make([]byte, pageSize)
	return data, nil
}

// WritePage 将页面写入存储
func (spa *StorageProviderAdapter) WritePage(spaceID, pageNo uint32, data []byte) error {
	// 简化实现：暂时不做实际写入
	return nil
}

// AllocatePage 分配新页面
func (spa *StorageProviderAdapter) AllocatePage(spaceID uint32) (uint32, error) {
	// 简化实现：返回一个固定的页面号
	return 1, nil
}

// FreePage 释放页面
func (spa *StorageProviderAdapter) FreePage(spaceID, pageNo uint32) error {
	// 简化实现：暂时不做实际释放
	return nil
}

// CreateSpace 创建空间
func (spa *StorageProviderAdapter) CreateSpace(name string, pageSize uint32) (uint32, error) {
	return spa.spaceManager.CreateTableSpace(name)
}

// OpenSpace 打开空间
func (spa *StorageProviderAdapter) OpenSpace(spaceID uint32) error {
	if spa.sm != nil {
		return spa.sm.OpenSpace(spaceID)
	}
	return nil
}

// CloseSpace 关闭空间
func (spa *StorageProviderAdapter) CloseSpace(spaceID uint32) error {
	if spa.sm != nil {
		return spa.sm.CloseSpace(spaceID)
	}
	return nil
}

// DeleteSpace 删除空间
func (spa *StorageProviderAdapter) DeleteSpace(spaceID uint32) error {
	if spa.sm != nil {
		return spa.sm.DeleteSpace(spaceID)
	}
	return nil
}

// GetSpaceInfo 获取空间信息
func (spa *StorageProviderAdapter) GetSpaceInfo(spaceID uint32) (*basic.SpaceInfo, error) {
	if spa.sm != nil {
		return spa.sm.GetSpaceInfo(spaceID)
	}
	return nil, fmt.Errorf("storage manager not available")
}

// ListSpaces 列出所有空间
func (spa *StorageProviderAdapter) ListSpaces() ([]basic.SpaceInfo, error) {
	if spa.sm != nil {
		return spa.sm.ListSpaces()
	}
	return nil, fmt.Errorf("storage manager not available")
}

// BeginTransaction 开始事务
func (spa *StorageProviderAdapter) BeginTransaction() (uint64, error) {
	if spa.sm != nil {
		return spa.sm.BeginTransaction()
	}
	return 0, fmt.Errorf("storage manager not available")
}

// CommitTransaction 提交事务
func (spa *StorageProviderAdapter) CommitTransaction(txID uint64) error {
	if spa.sm != nil {
		return spa.sm.CommitTransaction(txID)
	}
	return fmt.Errorf("storage manager not available")
}

// RollbackTransaction 回滚事务
func (spa *StorageProviderAdapter) RollbackTransaction(txID uint64) error {
	if spa.sm != nil {
		return spa.sm.RollbackTransaction(txID)
	}
	return fmt.Errorf("storage manager not available")
}

// Sync 同步数据到磁盘
func (spa *StorageProviderAdapter) Sync(spaceID uint32) error {
	if spa.sm != nil {
		return spa.sm.Sync(spaceID)
	}
	return nil
}

// Close 关闭存储提供者
func (spa *StorageProviderAdapter) Close() error {
	if spa.sm != nil {
		return spa.sm.Close()
	}
	return nil
}
