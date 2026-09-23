package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// TableStorageInfo 表的存储信息
type TableStorageInfo struct {
	SchemaName     string                 // 数据库名
	TableName      string                 // 表名
	SpaceID        uint32                 // 表空间ID
	RootPageNo     uint32                 // B+树根页面号
	IndexPageNo    uint32                 // 主键索引根页面号
	DataSegmentID  uint64                 // 数据段ID
	Type           TableType              // 表类型
	Partitioning   map[string]interface{} `json:"partitioning,omitempty"`
	Partitions     []PartitionStorageInfo `json:"partitions,omitempty"`
	TablespaceName string                 `json:"tablespace_name,omitempty"`
	OwnsTablespace bool                   `json:"owns_tablespace,omitempty"`
}

// PartitionStorageInfo is the physical clustered-index mapping for one
// logical table partition.  The parent table mapping remains the dictionary
// owner; partition spaces are deliberately not exposed as independent tables.
type PartitionStorageInfo struct {
	Name           string `json:"name"`
	Ordinal        int    `json:"ordinal"`
	SpaceID        uint32 `json:"space_id"`
	RootPageNo     uint32 `json:"root_page_no"`
	DataSegmentID  uint64 `json:"data_segment_id"`
	TablespaceName string `json:"tablespace_name,omitempty"`
	OwnsTablespace bool   `json:"owns_tablespace,omitempty"`
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

	// Per-table B+Tree managers are long-lived for the engine lifetime. Creating
	// one for every statement starts background workers and causes avoidable
	// latency; they are closed when a table or the engine is closed.
	btreeManagers map[string]basic.BPlusTreeManager
}

// NewTableStorageManager 创建表存储映射管理器
func NewTableStorageManager(sm *StorageManager) *TableStorageManager {
	tsm := &TableStorageManager{
		tableStorageMap: make(map[string]*TableStorageInfo),
		spaceToTableMap: make(map[uint32]string),
		btreeManagers:   make(map[string]basic.BPlusTreeManager),
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
		return fmt.Errorf("%w: %s", ErrTableStorageAlreadyRegistered, key)
	}

	// 检查表空间ID是否已被使用
	if existingTable, exists := tsm.spaceToTableMap[info.SpaceID]; exists {
		existingInfo := tsm.tableStorageMap[existingTable]
		if info.OwnsTablespace || existingInfo == nil || existingInfo.OwnsTablespace {
			return fmt.Errorf("space ID %d already used by table %s", info.SpaceID, existingTable)
		}
	}

	// 注册表
	tsm.tableStorageMap[key] = info
	tsm.spaceToTableMap[info.SpaceID] = key

	logger.Debugf("Registered table storage: %s (Space ID: %d, Root Page: %d)\n",
		key, info.SpaceID, info.RootPageNo)

	return nil
}

// ReplaceTableStorage atomically replaces a table's storage mapping.
func (tsm *TableStorageManager) ReplaceTableStorage(ctx context.Context, info *TableStorageInfo) error {
	if info == nil {
		return fmt.Errorf("table storage info cannot be nil")
	}

	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	key := fmt.Sprintf("%s.%s", info.SchemaName, info.TableName)
	if existingTable, exists := tsm.spaceToTableMap[info.SpaceID]; exists && existingTable != key {
		return fmt.Errorf("space ID %d already used by table %s", info.SpaceID, existingTable)
	}

	if oldInfo, exists := tsm.tableStorageMap[key]; exists && oldInfo.SpaceID != info.SpaceID {
		delete(tsm.spaceToTableMap, oldInfo.SpaceID)
	}
	tsm.tableStorageMap[key] = info
	tsm.spaceToTableMap[info.SpaceID] = key

	logger.Debugf("Replaced table storage: %s (Space ID: %d, Root Page: %d)\n",
		key, info.SpaceID, info.RootPageNo)
	return nil
}

// SyncFromInfoSchema 基于信息模式重建表存储映射，适用于服务重启后内存映射丢失场景。
func (tsm *TableStorageManager) SyncFromInfoSchema(infoSchemaManager metadata.InfoSchemaManager) error {
	if infoSchemaManager == nil {
		return fmt.Errorf("info schema manager is nil")
	}

	schemaNames, err := infoSchemaManager.GetAllSchemaNames(context.Background())
	if err != nil {
		return fmt.Errorf("load schema names failed: %v", err)
	}

	for _, schemaName := range schemaNames {
		if strings.EqualFold(schemaName, "INFORMATION_SCHEMA") {
			continue
		}

		tables, err := infoSchemaManager.GetAllTables(context.Background(), schemaName)
		if err != nil {
			logger.Warnf("Sync table storage mapping skip schema=%q: %v", schemaName, err)
			continue
		}

		for _, table := range tables {
			if table == nil || table.Name == "" {
				continue
			}

			if _, err := tsm.GetTableStorageInfo(schemaName, table.Name); err == nil {
				logger.Debugf("Table storage mapping already exists, skip recovery: %s.%s", schemaName, table.Name)
				continue
			}

			spaceName := fmt.Sprintf("%s/%s", schemaName, table.Name)
			rootPageNo := uint32(3)
			ownsTablespace := true
			if raw, readErr := os.ReadFile(filepath.Join(tsm.storageManager.configDataDir(), schemaName, table.Name+".frm")); readErr == nil {
				var persisted struct {
					StorageRootPage uint32 `json:"storage_root_page"`
					TablespaceName  string `json:"tablespace_name"`
					OwnsTablespace  *bool  `json:"owns_tablespace"`
					Discarded       bool   `json:"tablespace_discarded"`
				}
				var persistedFields map[string]json.RawMessage
				if json.Unmarshal(raw, &persisted) == nil {
					_ = json.Unmarshal(raw, &persistedFields)
					if _, hasRoot := persistedFields["storage_root_page"]; hasRoot {
						rootPageNo = persisted.StorageRootPage
					}
					if strings.TrimSpace(persisted.TablespaceName) != "" {
						spaceName = persisted.TablespaceName
					}
					if persisted.OwnsTablespace != nil {
						ownsTablespace = *persisted.OwnsTablespace
					}
					if persisted.Discarded {
						logger.Debugf("Skip discarded table storage mapping schema=%q table=%q", schemaName, table.Name)
						continue
					}
				}
			}
			handle, err := tsm.storageManager.GetTablespace(spaceName)
			if err != nil && ownsTablespace {
				handle, err = tsm.storageManager.CreateTablespace(spaceName)
			}
			if err != nil {
				logger.Warnf("Sync table storage mapping failed create tablespace schema=%q table=%q space=%q: %v", schemaName, table.Name, spaceName, err)
				continue
			}

			info := &TableStorageInfo{
				SchemaName:     schemaName,
				TableName:      table.Name,
				SpaceID:        handle.SpaceID,
				RootPageNo:     rootPageNo,
				IndexPageNo:    rootPageNo,
				DataSegmentID:  handle.DataSegmentID,
				Type:           TableTypeUser,
				TablespaceName: spaceName,
				OwnsTablespace: true,
			}
			info.OwnsTablespace = ownsTablespace
			info.TablespaceName = spaceName

			if err := tsm.RegisterTable(context.Background(), info); err != nil {
				if !errors.Is(err, ErrTableStorageAlreadyRegistered) {
					return fmt.Errorf("register table storage mapping failed schema=%q table=%q: %v", schemaName, table.Name, err)
				}
			}

			logger.Debugf("Recovered table storage mapping schema=%q table=%q spaceID=%d", schemaName, table.Name, handle.SpaceID)
		}
	}

	return nil
}

// EnsureTableStorage makes the storage mapping for one metadata table
// available after a restart. Full dictionary enumeration is normally enough,
// but recovery journals know the exact tables that need to be rolled back and
// can repair those mappings deterministically.
func (tsm *TableStorageManager) EnsureTableStorage(ctx context.Context, infoSchemaManager metadata.InfoSchemaManager, schemaName, tableName string) error {
	if tsm == nil || tsm.storageManager == nil {
		return fmt.Errorf("table storage manager is not initialized")
	}
	if infoSchemaManager == nil {
		return fmt.Errorf("info schema manager is nil")
	}
	if _, err := tsm.GetTableStorageInfo(schemaName, tableName); err == nil {
		return nil
	}
	if _, err := infoSchemaManager.GetTableMetadata(ctx, schemaName, tableName); err != nil {
		// The dictionary cache may be empty immediately after a crash even
		// though the durable .frm definition is already present. The DML
		// executor has the same .frm fallback, so accept that source here.
		dataDir := ""
		if tsm.storageManager.config != nil {
			dataDir = tsm.storageManager.config.InnodbDataDir
			if dataDir == "" {
				dataDir = tsm.storageManager.config.DataDir
			}
		}
		frmPath := filepath.Join(dataDir, schemaName, tableName+".frm")
		if _, statErr := os.Stat(frmPath); statErr != nil {
			return fmt.Errorf("load table metadata %s.%s: %w", schemaName, tableName, err)
		}
	}
	handle, err := tsm.storageManager.CreateTablespace(fmt.Sprintf("%s/%s", schemaName, tableName))
	if err != nil {
		return fmt.Errorf("create tablespace for %s.%s: %w", schemaName, tableName, err)
	}
	rootPageNo := uint32(3)
	if dataDir := tsm.storageManager.configDataDir(); dataDir != "" {
		frmPath := filepath.Join(dataDir, schemaName, tableName+".frm")
		if raw, readErr := os.ReadFile(frmPath); readErr == nil {
			var identity struct {
				StorageRootPage uint32 `json:"storage_root_page"`
				StorageSpaceID  uint32 `json:"storage_space_id"`
			}
			if json.Unmarshal(raw, &identity) == nil && identity.StorageRootPage != 0 {
				rootPageNo = identity.StorageRootPage
			}
		}
	}
	info := &TableStorageInfo{
		SchemaName:     schemaName,
		TableName:      tableName,
		SpaceID:        handle.SpaceID,
		RootPageNo:     rootPageNo,
		IndexPageNo:    rootPageNo,
		DataSegmentID:  handle.DataSegmentID,
		Type:           TableTypeUser,
		TablespaceName: fmt.Sprintf("%s/%s", schemaName, tableName),
		OwnsTablespace: true,
	}
	if err := tsm.RegisterTable(ctx, info); err != nil && !errors.Is(err, ErrTableStorageAlreadyRegistered) {
		return fmt.Errorf("register table storage %s.%s: %w", schemaName, tableName, err)
	}
	return nil
}

func (sm *StorageManager) configDataDir() string {
	if sm == nil || sm.config == nil {
		return ""
	}
	if sm.config.InnodbDataDir != "" {
		return sm.config.InnodbDataDir
	}
	return sm.config.DataDir
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
	key := fmt.Sprintf("%s.%s", schemaName, tableName)
	tsm.mu.RLock()
	if existing := tsm.btreeManagers[key]; existing != nil {
		tsm.mu.RUnlock()
		return existing, nil
	}
	tsm.mu.RUnlock()

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
	if btreeManager.rootPageNo != 0 && btreeManager.rootPageNo != info.RootPageNo {
		tsm.mu.Lock()
		info.RootPageNo = btreeManager.rootPageNo
		info.IndexPageNo = btreeManager.rootPageNo
		tsm.mu.Unlock()
	}
	tsm.mu.Lock()
	if existing := tsm.btreeManagers[key]; existing != nil {
		tsm.mu.Unlock()
		_ = btreeManager.Close()
		return existing, nil
	}
	tsm.btreeManagers[key] = btreeManager
	tsm.mu.Unlock()

	logger.Debugf("Created Enhanced BTreeManager for table %s.%s (Space: %d, Root: %d)\n",
		schemaName, tableName, info.SpaceID, info.RootPageNo)

	return btreeManager, nil
}

// CreateBTreeManagerForPartition opens the clustered index belonging to one
// physical partition.  Partition managers use a distinct cache key so they
// cannot accidentally share the parent table's root page.
func (tsm *TableStorageManager) CreateBTreeManagerForPartition(ctx context.Context, schemaName, tableName, partitionName string) (basic.BPlusTreeManager, error) {
	info, err := tsm.GetTableStorageInfo(schemaName, tableName)
	if err != nil {
		return nil, err
	}
	partitionName = strings.TrimSpace(partitionName)
	var partition *PartitionStorageInfo
	for index := range info.Partitions {
		if strings.EqualFold(info.Partitions[index].Name, partitionName) {
			copy := info.Partitions[index]
			partition = &copy
			break
		}
	}
	if partition == nil {
		return nil, fmt.Errorf("partition %s.%s.%s not found", schemaName, tableName, partitionName)
	}

	key := fmt.Sprintf("%s.%s#%s", schemaName, tableName, partitionName)
	tsm.mu.RLock()
	if existing := tsm.btreeManagers[key]; existing != nil {
		tsm.mu.RUnlock()
		return existing, nil
	}
	tsm.mu.RUnlock()

	config := &BTreeConfig{
		MaxCacheSize: 1000, CachePolicy: "LRU", PrefetchSize: 4, PageSize: 16384,
		FillFactor: 0.8, MinFillFactor: 0.4, SplitThreshold: 0.9,
		MergeThreshold: 0.3, AsyncIO: true, EnableStats: true,
		StatsInterval: time.Minute * 5, EnableLogging: true, LogLevel: "INFO",
	}
	adapter := NewEnhancedBTreeAdapter(tsm.storageManager, config)
	if err := adapter.Init(ctx, partition.SpaceID, partition.RootPageNo); err != nil {
		return nil, fmt.Errorf("init partition btree manager failed: %v", err)
	}
	if adapter.RootPageNo() != 0 && adapter.RootPageNo() != partition.RootPageNo {
		partition.RootPageNo = adapter.RootPageNo()
		tsm.mu.Lock()
		if current := tsm.tableStorageMap[fmt.Sprintf("%s.%s", schemaName, tableName)]; current != nil {
			for index := range current.Partitions {
				if strings.EqualFold(current.Partitions[index].Name, partitionName) {
					current.Partitions[index].RootPageNo = partition.RootPageNo
				}
			}
		}
		tsm.mu.Unlock()
	}

	tsm.mu.Lock()
	if existing := tsm.btreeManagers[key]; existing != nil {
		tsm.mu.Unlock()
		_ = adapter.Close()
		return existing, nil
	}
	tsm.btreeManagers[key] = adapter
	tsm.mu.Unlock()
	return adapter, nil
}

// ResetTableBTree allocates a fresh empty clustered-index root in the
// existing tablespace. It is the safe bulk implementation used by TRUNCATE:
// the .ibd file remains open and no row-by-row delete or file unlink is
// required, while the table definition and tablespace identity are preserved.
func (tsm *TableStorageManager) ResetTableBTree(ctx context.Context, schemaName, tableName string) (uint32, error) {
	info, err := tsm.GetTableStorageInfo(schemaName, tableName)
	if err != nil {
		return 0, err
	}
	if tsm.storageManager == nil {
		return 0, fmt.Errorf("storage manager is not initialized")
	}
	key := fmt.Sprintf("%s.%s", schemaName, tableName)
	tsm.mu.RLock()
	existingManager := tsm.btreeManagers[key]
	tsm.mu.RUnlock()
	if resetter, ok := existingManager.(interface {
		Reset(context.Context) (uint32, error)
	}); ok {
		rootPageNo, err := resetter.Reset(ctx)
		if err != nil {
			return 0, err
		}
		updated := *info
		updated.RootPageNo = rootPageNo
		updated.IndexPageNo = rootPageNo
		updated.Partitioning = clonePartitioning(info.Partitioning)
		if err := tsm.ReplaceTableStorage(ctx, &updated); err != nil {
			return 0, fmt.Errorf("persist reset btree mapping failed: %v", err)
		}
		return rootPageNo, nil
	}

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
	adapter := NewEnhancedBTreeAdapter(tsm.storageManager, btreeConfig)
	if err := adapter.Init(ctx, info.SpaceID, 0); err != nil {
		return 0, fmt.Errorf("reset btree root failed: %v", err)
	}
	rootPageNo := adapter.RootPageNo()
	if rootPageNo == 0 {
		return 0, fmt.Errorf("reset btree root page is empty")
	}

	updated := *info
	updated.RootPageNo = rootPageNo
	updated.IndexPageNo = rootPageNo
	updated.Partitioning = clonePartitioning(info.Partitioning)
	if err := tsm.ReplaceTableStorage(ctx, &updated); err != nil {
		return 0, fmt.Errorf("persist reset btree mapping failed: %v", err)
	}
	tsm.mu.Lock()
	tsm.btreeManagers[key] = adapter
	tsm.mu.Unlock()
	return rootPageNo, nil
}

// ListAllTables 列出所有注册的表
func (tsm *TableStorageManager) ListAllTables() map[string]*TableStorageInfo {
	tsm.mu.RLock()
	defer tsm.mu.RUnlock()

	// 创建副本返回
	result := make(map[string]*TableStorageInfo)
	for key, info := range tsm.tableStorageMap {
		result[key] = &TableStorageInfo{
			SchemaName:     info.SchemaName,
			TableName:      info.TableName,
			SpaceID:        info.SpaceID,
			RootPageNo:     info.RootPageNo,
			IndexPageNo:    info.IndexPageNo,
			DataSegmentID:  info.DataSegmentID,
			Type:           info.Type,
			Partitioning:   clonePartitioning(info.Partitioning),
			Partitions:     clonePartitionStorageInfos(info.Partitions),
			TablespaceName: info.TablespaceName,
			OwnsTablespace: info.OwnsTablespace,
		}
	}

	return result
}

// SetPartitioning stores the logical partition descriptor and materializes one
// physical tablespace per partition.  The operation is idempotent: existing
// partition spaces retain their identity and newly added definitions receive
// new spaces.
func (tsm *TableStorageManager) SetPartitioning(schemaName, tableName string, partitioning map[string]interface{}) error {
	previous, err := tsm.GetTableStorageInfo(schemaName, tableName)
	if err != nil {
		return err
	}
	previousPartitions := clonePartitionStorageInfos(previous.Partitions)
	if err := tsm.EnsurePartitionStorage(context.Background(), schemaName, tableName, partitioning); err != nil {
		return err
	}
	tsm.mu.Lock()
	key := fmt.Sprintf("%s.%s", schemaName, tableName)
	info, exists := tsm.tableStorageMap[key]
	if !exists {
		tsm.mu.Unlock()
		return fmt.Errorf("table %s not registered", key)
	}
	info.Partitioning = clonePartitioning(partitioning)
	currentPartitions := clonePartitionStorageInfos(info.Partitions)
	tsm.mu.Unlock()
	return tsm.dropRemovedPartitionSpaces(schemaName, tableName, previousPartitions, currentPartitions)
}

func (tsm *TableStorageManager) dropRemovedPartitionSpaces(schemaName, tableName string, previous, current []PartitionStorageInfo) error {
	currentNames := make(map[string]struct{}, len(current))
	for _, partition := range current {
		currentNames[strings.ToLower(strings.TrimSpace(partition.Name))] = struct{}{}
	}
	for _, partition := range previous {
		name := strings.TrimSpace(partition.Name)
		if name == "" {
			continue
		}
		if _, stillUsed := currentNames[strings.ToLower(name)]; stillUsed {
			continue
		}
		key := fmt.Sprintf("%s.%s#%s", schemaName, tableName, name)
		tsm.mu.Lock()
		btreeManager := tsm.btreeManagers[key]
		delete(tsm.btreeManagers, key)
		tsm.mu.Unlock()
		if btreeManager != nil {
			if closer, ok := btreeManager.(interface{ Close() error }); ok {
				if err := closer.Close(); err != nil {
					return fmt.Errorf("close removed partition %s: %w", name, err)
				}
			}
		}
		if !partition.OwnsTablespace && strings.TrimSpace(partition.TablespaceName) != "" {
			continue
		}
		spaceName := partition.TablespaceName
		if strings.TrimSpace(spaceName) == "" {
			spaceName = fmt.Sprintf("%s/%s#%s", schemaName, tableName, name)
		}
		if err := tsm.storageManager.DropTablespace(spaceName); err != nil {
			return fmt.Errorf("drop removed partition tablespace %s: %w", spaceName, err)
		}
	}
	return nil
}

// EnsurePartitionStorage reconciles a logical descriptor with physical
// partition spaces.  It intentionally leaves removed spaces alone until the
// DDL layer has deleted their rows and completed its metadata transaction;
// this makes failed ALTER operations recoverable instead of orphaning data.
func (tsm *TableStorageManager) EnsurePartitionStorage(ctx context.Context, schemaName, tableName string, partitioning map[string]interface{}) error {
	if tsm == nil || tsm.storageManager == nil {
		return fmt.Errorf("storage manager is not initialized")
	}
	info, err := tsm.GetTableStorageInfo(schemaName, tableName)
	if err != nil {
		return err
	}
	definitions := partitionDefinitionMaps(partitioning)
	if len(definitions) == 0 {
		updated := *info
		updated.Partitions = nil
		return tsm.ReplaceTableStorage(ctx, &updated)
	}

	existing := make(map[string]PartitionStorageInfo, len(info.Partitions))
	for _, partition := range info.Partitions {
		existing[strings.ToLower(strings.TrimSpace(partition.Name))] = partition
	}
	partitions := make([]PartitionStorageInfo, 0, len(definitions))
	for ordinal, definition := range definitions {
		name := strings.TrimSpace(fmt.Sprint(definition["name"]))
		if name == "" {
			return fmt.Errorf("partition %d has no name", ordinal+1)
		}
		key := strings.ToLower(name)
		partition, ok := existing[key]
		if !ok || partition.SpaceID == 0 {
			spaceName := fmt.Sprintf("%s/%s#%s", schemaName, tableName, name)
			if persistedName, persistedOK := definition["tablespace_name"].(string); persistedOK && strings.TrimSpace(persistedName) != "" {
				spaceName = strings.TrimSpace(persistedName)
			}
			var handle *TablespaceHandle
			handle, createErr := tsm.storageManager.GetTablespace(spaceName)
			if createErr != nil {
				handle, createErr = tsm.storageManager.CreateTablespace(spaceName)
			}
			if createErr != nil {
				return fmt.Errorf("create tablespace for partition %s.%s.%s: %w", schemaName, tableName, name, createErr)
			}
			partition = PartitionStorageInfo{
				Name:           name,
				Ordinal:        ordinal + 1,
				SpaceID:        handle.SpaceID,
				DataSegmentID:  handle.DataSegmentID,
				TablespaceName: spaceName,
				OwnsTablespace: true,
			}
			if spaceID, ok := partitionUint32(definition["storage_space_id"]); ok && spaceID != 0 {
				partition.SpaceID = spaceID
			}
			if rootPage, ok := partitionUint32(definition["storage_root_page"]); ok {
				partition.RootPageNo = rootPage
			}
			if segmentID, ok := partitionUint64(definition["data_segment_id"]); ok && segmentID != 0 {
				partition.DataSegmentID = segmentID
			}
			if owns, ok := definition["owns_tablespace"].(bool); ok {
				partition.OwnsTablespace = owns
			}
		} else {
			partition.Name = name
			partition.Ordinal = ordinal + 1
			if persistedName, persistedOK := definition["tablespace_name"].(string); persistedOK && strings.TrimSpace(persistedName) != "" {
				partition.TablespaceName = strings.TrimSpace(persistedName)
			}
			if owns, ownsOK := definition["owns_tablespace"].(bool); ownsOK {
				partition.OwnsTablespace = owns
			}
			if strings.TrimSpace(partition.TablespaceName) == "" {
				partition.TablespaceName = fmt.Sprintf("%s/%s#%s", schemaName, tableName, name)
				partition.OwnsTablespace = true
			}
		}
		partitions = append(partitions, partition)
	}

	updated := *info
	updated.Partitioning = clonePartitioning(partitioning)
	updated.Partitions = partitions
	return tsm.ReplaceTableStorage(ctx, &updated)
}

// PartitionStorageInfos returns a stable copy for the engine's fan-out
// scanner/router.  Callers must not mutate the returned mappings.
func (tsm *TableStorageManager) PartitionStorageInfos(schemaName, tableName string) ([]PartitionStorageInfo, error) {
	info, err := tsm.GetTableStorageInfo(schemaName, tableName)
	if err != nil {
		return nil, err
	}
	return clonePartitionStorageInfos(info.Partitions), nil
}

func partitionDefinitionMaps(partitioning map[string]interface{}) []map[string]interface{} {
	if partitioning == nil {
		return nil
	}
	switch definitions := partitioning["partitions"].(type) {
	case []map[string]interface{}:
		return definitions
	case []interface{}:
		result := make([]map[string]interface{}, 0, len(definitions))
		for _, raw := range definitions {
			if definition, ok := raw.(map[string]interface{}); ok {
				result = append(result, definition)
			}
		}
		return result
	default:
		return nil
	}
}

func clonePartitionStorageInfos(input []PartitionStorageInfo) []PartitionStorageInfo {
	if input == nil {
		return nil
	}
	return append([]PartitionStorageInfo(nil), input...)
}

func partitionUint32(value interface{}) (uint32, bool) {
	switch typed := value.(type) {
	case uint32:
		return typed, true
	case uint64:
		return uint32(typed), uint64(uint32(typed)) == typed
	case float64:
		return uint32(typed), typed >= 0 && float64(uint32(typed)) == typed
	case int:
		return uint32(typed), typed >= 0
	case int64:
		return uint32(typed), typed >= 0 && int64(uint32(typed)) == typed
	default:
		return 0, false
	}
}

func partitionUint64(value interface{}) (uint64, bool) {
	switch typed := value.(type) {
	case uint64:
		return typed, true
	case uint32:
		return uint64(typed), true
	case float64:
		return uint64(typed), typed >= 0 && uint64(typed) == uint64(typed)
	case int:
		return uint64(typed), typed >= 0
	case int64:
		return uint64(typed), typed >= 0
	default:
		return 0, false
	}
}

func clonePartitioning(input map[string]interface{}) map[string]interface{} {
	if input == nil {
		return nil
	}
	clone := make(map[string]interface{}, len(input))
	for key, value := range input {
		clone[key] = value
	}
	return clone
}

// UnregisterTable 注销表的存储信息
func (tsm *TableStorageManager) UnregisterTable(schemaName, tableName string) error {
	tsm.mu.Lock()

	key := fmt.Sprintf("%s.%s", schemaName, tableName)
	info, exists := tsm.tableStorageMap[key]
	if !exists {
		tsm.mu.Unlock()
		return fmt.Errorf("table %s not registered", key)
	}
	btreeManagers := make([]basic.BPlusTreeManager, 0, 1+len(info.Partitions))
	if btreeManager := tsm.btreeManagers[key]; btreeManager != nil {
		btreeManagers = append(btreeManagers, btreeManager)
	}
	delete(tsm.btreeManagers, key)
	for _, partition := range info.Partitions {
		partitionKey := fmt.Sprintf("%s#%s", key, partition.Name)
		if btreeManager := tsm.btreeManagers[partitionKey]; btreeManager != nil {
			btreeManagers = append(btreeManagers, btreeManager)
		}
		delete(tsm.btreeManagers, partitionKey)
	}

	// 删除映射
	delete(tsm.tableStorageMap, key)
	delete(tsm.spaceToTableMap, info.SpaceID)
	tsm.mu.Unlock()
	for _, btreeManager := range btreeManagers {
		if closer, ok := btreeManager.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}
	for _, partition := range info.Partitions {
		if !partition.OwnsTablespace && strings.TrimSpace(partition.TablespaceName) != "" {
			continue
		}
		spaceName := partition.TablespaceName
		if strings.TrimSpace(spaceName) == "" {
			spaceName = fmt.Sprintf("%s/%s#%s", schemaName, tableName, partition.Name)
		}
		if err := tsm.storageManager.DropTablespace(spaceName); err != nil {
			logger.Warnf("failed to drop partition tablespace %s during unregister: %v", spaceName, err)
		}
	}

	logger.Debugf("Unregistered table storage: %s\n", key)
	return nil
}

// CloseBTreeManagers releases all per-table background workers before the
// underlying tablespace files are closed or removed.
func (tsm *TableStorageManager) CloseBTreeManagers() error {
	if tsm == nil {
		return nil
	}
	tsm.mu.Lock()
	managers := make([]basic.BPlusTreeManager, 0, len(tsm.btreeManagers))
	for key, manager := range tsm.btreeManagers {
		managers = append(managers, manager)
		delete(tsm.btreeManagers, key)
	}
	tsm.mu.Unlock()
	for _, manager := range managers {
		if closer, ok := manager.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				return err
			}
		}
	}
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
