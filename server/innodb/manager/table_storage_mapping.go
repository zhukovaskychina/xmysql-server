package manager

import (
	"context"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/logger"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// TableStorageInfo 表的存储信息
type TableStorageInfo struct {
	SchemaName    string    // 数据库名
	TableName     string    // 表名
	SpaceID       uint32    // 表空间ID
	RootPageNo    uint32    // B+树根页面号
	IndexPageNo   uint32    // 主键索引根页面号
	DataSegmentID uint64    // 数据段ID
	Type          TableType // 表类型
}

// TableType 表类型
type TableType int

const (
	TableTypeUser   TableType = iota // 用户表
	TableTypeSystem                  // 系统表
	TableTypeTemp                    // 临时表
)

// TableStorageManager 表存储映射管理器
type TableStorageManager struct {
	mu sync.RWMutex

	// 表名到存储信息的映射
	// key: "schema.table", value: TableStorageInfo
	tableStorageMap map[string]*TableStorageInfo

	// 表空间ID到表名的反向映射
	// key: spaceID, value: "schema.table"
	spaceToTableMap map[uint32]string

	// 存储管理器引用
	storageManager *StorageManager
}

// NewTableStorageManager 创建表存储映射管理器
func NewTableStorageManager(sm *StorageManager) *TableStorageManager {
	tsm := &TableStorageManager{
		tableStorageMap: make(map[string]*TableStorageInfo),
		spaceToTableMap: make(map[uint32]string),
		storageManager:  sm,
	}

	// 初始化系统表映射
	tsm.initializeSystemTablesMapping()

	return tsm
}

// initializeSystemTablesMapping 初始化系统表映射
func (tsm *TableStorageManager) initializeSystemTablesMapping() {
	// MySQL系统表映射
	systemTables := []struct {
		schema   string
		table    string
		spaceID  uint32
		rootPage uint32
	}{
		{"mysql", "user", 1, 3},                       // mysql.user表
		{"mysql", "db", 2, 3},                         // mysql.db表
		{"mysql", "tables_priv", 3, 3},                // mysql.tables_priv表
		{"mysql", "columns_priv", 4, 3},               // mysql.columns_priv表
		{"mysql", "procs_priv", 5, 3},                 // mysql.procs_priv表
		{"mysql", "proxies_priv", 6, 3},               // mysql.proxies_priv表
		{"mysql", "role_edges", 7, 3},                 // mysql.role_edges表
		{"mysql", "default_roles", 8, 3},              // mysql.default_roles表
		{"mysql", "global_grants", 9, 3},              // mysql.global_grants表
		{"mysql", "password_history", 10, 3},          // mysql.password_history表
		{"mysql", "component", 11, 3},                 // mysql.component表
		{"mysql", "server_cost", 12, 3},               // mysql.server_cost表
		{"mysql", "engine_cost", 13, 3},               // mysql.engine_cost表
		{"mysql", "time_zone", 14, 3},                 // mysql.time_zone表
		{"mysql", "time_zone_name", 15, 3},            // mysql.time_zone_name表
		{"mysql", "time_zone_transition", 16, 3},      // mysql.time_zone_transition表
		{"mysql", "time_zone_transition_type", 17, 3}, // mysql.time_zone_transition_type表
		{"mysql", "help_topic", 18, 3},                // mysql.help_topic表
		{"mysql", "help_category", 19, 3},             // mysql.help_category表
		{"mysql", "help_relation", 20, 3},             // mysql.help_relation表
		{"mysql", "help_keyword", 21, 3},              // mysql.help_keyword表
		{"mysql", "plugin", 22, 3},                    // mysql.plugin表
		{"mysql", "servers", 23, 3},                   // mysql.servers表
		{"mysql", "func", 24, 3},                      // mysql.func表
		{"mysql", "general_log", 25, 3},               // mysql.general_log表
		{"mysql", "slow_log", 26, 3},                  // mysql.slow_log表
	}

	for _, table := range systemTables {
		key := fmt.Sprintf("%s.%s", table.schema, table.table)
		info := &TableStorageInfo{
			SchemaName:    table.schema,
			TableName:     table.table,
			SpaceID:       table.spaceID,
			RootPageNo:    table.rootPage,
			IndexPageNo:   table.rootPage,
			DataSegmentID: uint64(table.spaceID),
			Type:          TableTypeSystem,
		}

		tsm.tableStorageMap[key] = info
		tsm.spaceToTableMap[table.spaceID] = key
	}

	logger.Debugf("Initialized storage mapping for %d system tables\n", len(systemTables))
}

// RegisterTable 注册表的存储信息
func (tsm *TableStorageManager) RegisterTable(ctx context.Context, info *TableStorageInfo) error {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	key := fmt.Sprintf("%s.%s", info.SchemaName, info.TableName)

	// 检查表是否已存在
	if _, exists := tsm.tableStorageMap[key]; exists {
		return fmt.Errorf("table %s already registered", key)
	}

	// 检查表空间ID是否已被使用
	if existingTable, exists := tsm.spaceToTableMap[info.SpaceID]; exists {
		return fmt.Errorf("space ID %d already used by table %s", info.SpaceID, existingTable)
	}

	// 注册表
	tsm.tableStorageMap[key] = info
	tsm.spaceToTableMap[info.SpaceID] = key

	logger.Debugf("Registered table storage: %s (Space ID: %d, Root Page: %d)\n",
		key, info.SpaceID, info.RootPageNo)

	return nil
}

// GetTableStorageInfo 获取表的存储信息
func (tsm *TableStorageManager) GetTableStorageInfo(schemaName, tableName string) (*TableStorageInfo, error) {
	tsm.mu.RLock()
	defer tsm.mu.RUnlock()

	key := fmt.Sprintf("%s.%s", schemaName, tableName)
	info, exists := tsm.tableStorageMap[key]
	if !exists {
		return nil, fmt.Errorf("table %s not found in storage mapping", key)
	}

	return info, nil
}

// GetTableBySpaceID 根据表空间ID获取表信息
func (tsm *TableStorageManager) GetTableBySpaceID(spaceID uint32) (*TableStorageInfo, error) {
	tsm.mu.RLock()
	defer tsm.mu.RUnlock()

	tableKey, exists := tsm.spaceToTableMap[spaceID]
	if !exists {
		return nil, fmt.Errorf("no table found for space ID %d", spaceID)
	}

	info, exists := tsm.tableStorageMap[tableKey]
	if !exists {
		return nil, fmt.Errorf("table info not found for key %s", tableKey)
	}

	return info, nil
}

// CreateBTreeManagerForTable 为指定表创建B+树管理器
func (tsm *TableStorageManager) CreateBTreeManagerForTable(ctx context.Context, schemaName, tableName string) (basic.BPlusTreeManager, error) {
	// 获取表的存储信息
	info, err := tsm.GetTableStorageInfo(schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("get table storage info failed: %v", err)
	}

	// 创建增强版B+树管理器配置
	btreeConfig := &BTreeConfig{
		MaxCacheSize:   1000,
		CachePolicy:    "LRU",
		PrefetchSize:   4,
		PageSize:       16384,
		FillFactor:     0.8,
		MinFillFactor:  0.4,
		SplitThreshold: 0.9,
		MergeThreshold: 0.3,
		AsyncIO:        true,
		EnableStats:    true,
		StatsInterval:  time.Minute * 5,
		EnableLogging:  true,
		LogLevel:       "INFO",
	}

	// 创建增强版B+树管理器适配器
	btreeManager := NewEnhancedBTreeAdapter(tsm.storageManager, btreeConfig)

	// 初始化B+树管理器，指定表空间和根页面
	err = btreeManager.Init(ctx, info.SpaceID, info.RootPageNo)
	if err != nil {
		return nil, fmt.Errorf("init btree manager failed: %v", err)
	}

	logger.Debugf("Created Enhanced BTreeManager for table %s.%s (Space: %d, Root: %d)\n",
		schemaName, tableName, info.SpaceID, info.RootPageNo)

	return btreeManager, nil
}

// ListAllTables 列出所有注册的表
func (tsm *TableStorageManager) ListAllTables() map[string]*TableStorageInfo {
	tsm.mu.RLock()
	defer tsm.mu.RUnlock()

	// 创建副本返回
	result := make(map[string]*TableStorageInfo)
	for key, info := range tsm.tableStorageMap {
		result[key] = &TableStorageInfo{
			SchemaName:    info.SchemaName,
			TableName:     info.TableName,
			SpaceID:       info.SpaceID,
			RootPageNo:    info.RootPageNo,
			IndexPageNo:   info.IndexPageNo,
			DataSegmentID: info.DataSegmentID,
			Type:          info.Type,
		}
	}

	return result
}

// UnregisterTable 注销表的存储信息
func (tsm *TableStorageManager) UnregisterTable(schemaName, tableName string) error {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	key := fmt.Sprintf("%s.%s", schemaName, tableName)
	info, exists := tsm.tableStorageMap[key]
	if !exists {
		return fmt.Errorf("table %s not registered", key)
	}

	// 删除映射
	delete(tsm.tableStorageMap, key)
	delete(tsm.spaceToTableMap, info.SpaceID)

	logger.Debugf("Unregistered table storage: %s\n", key)
	return nil
}

// GetSystemTableInfo 获取系统表信息
func (tsm *TableStorageManager) GetSystemTableInfo() []*TableStorageInfo {
	tsm.mu.RLock()
	defer tsm.mu.RUnlock()

	var systemTables []*TableStorageInfo
	for _, info := range tsm.tableStorageMap {
		if info.Type == TableTypeSystem {
			systemTables = append(systemTables, info)
		}
	}

	return systemTables
}
