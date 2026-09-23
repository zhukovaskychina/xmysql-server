package manager

import (
	"encoding/json"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/logger"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/ibd"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/space"
)

const firstUserSpaceID uint32 = 1000

// SpaceManagerImpl implements the SpaceManager interface
type SpaceManagerImpl struct {
	sync.RWMutex
	spaces   map[uint32]*space.IBDSpace
	ibdFiles map[uint32]*ibd.IBD_File
	nameToID map[string]uint32
	nextID   uint32
	dataDir  string
	txID     uint64
}

func (sm *SpaceManagerImpl) FlushToDisk(pageNo uint32, content []byte) {
	sm.RLock()
	defer sm.RUnlock()

	// 操作系统表空间 (Space ID 0)
	systemSpace, exists := sm.spaces[0]
	if !exists {
		// 如果系统表空间不存在，忽略操作（避免panic）
		return
	}

	// 委托给系统表空间处理
	_ = systemSpace.FlushToDisk(pageNo, content)
}

func (sm *SpaceManagerImpl) LoadPageByPageNumber(pageNo uint32) ([]byte, error) {
	sm.RLock()
	defer sm.RUnlock()

	// 操作系统表空间 (Space ID 0)
	systemSpace, exists := sm.spaces[0]
	if !exists {
		return nil, fmt.Errorf("system tablespace not found")
	}

	// 委托给系统表空间处理
	return systemSpace.LoadPageByPageNumber(pageNo)
}

func (sm *SpaceManagerImpl) GetSpaceId() uint32 {
	// 返回系统表空间的ID
	return 0
}

// NewSpaceManager creates a new space manager
func NewSpaceManager(dataDir string) basic.SpaceManager {
	sm := &SpaceManagerImpl{
		spaces:   make(map[uint32]*space.IBDSpace),
		ibdFiles: make(map[uint32]*ibd.IBD_File),
		nameToID: make(map[string]uint32),
		nextID:   firstUserSpaceID,
		dataDir:  dataDir,
		txID:     1,
	}

	// 尝试加载现有的表空间（如果目录存在）
	if _, err := os.Stat(dataDir); err == nil {
		if err := sm.LoadExistingTablespaces(); err != nil {
			logger.Debugf("Warning: failed to load existing tablespaces: %v", err)
		}
	}

	return sm
}

func (sm *SpaceManagerImpl) CreateSpace(spaceID uint32, name string, isSystem bool) (basic.Space, error) {
	sm.Lock()
	defer sm.Unlock()

	if _, exists := sm.nameToID[name]; exists {
		return nil, fmt.Errorf("%w: %s", ErrTablespaceExists, name)
	}

	// 创建 IBD 文件实例
	ibdFile := ibd.NewIBDFile(sm.dataDir, name, spaceID)

	// 检查文件是否已存在
	fileExists := ibdFile.Exists()

	if fileExists {
		// 文件已存在，打开并读取
		logger.Debugf("IBD file already exists, opening: %s (Space ID: %d)", name, spaceID)
		err := ibdFile.Open()
		if err != nil {
			return nil, fmt.Errorf("failed to open existing tablespace %s: %v", name, err)
		}
	} else {
		// 文件不存在，创建新文件
		logger.Debugf("Creating new IBD file: %s (Space ID: %d)", name, spaceID)
		err := ibdFile.Create()
		if err != nil {
			return nil, fmt.Errorf("failed to create tablespace %s: %v", name, err)
		}
	}

	// 创建 IBDSpace 实例
	ibdSpace := space.NewIBDSpace(ibdFile, isSystem)

	// 设置为活动状态
	ibdSpace.SetActive(true)

	// 如果是新创建的文件，分配第一个extent用于系统页面
	if !fileExists {
		// 分配第一个extent用于系统页面
		extent, err := ibdSpace.AllocateExtent(basic.ExtentPurposeSystem)
		if err != nil {
			ibdFile.Close()
			return nil, fmt.Errorf("failed to allocate system extent for %s: %v", name, err)
		}

		// 标记前几个页面为已分配（FSP header, IBUF bitmap等）
		for i := 0; i < 2; i++ {
			// 这里应该通过IBDSpace的内部方法来标记页面为已分配
			// 但由于IBDSpace的pageAllocs是私有的，我们暂时跳过这个步骤
			_ = extent // 避免未使用变量警告
		}
	}

	// 注册到管理器
	sm.spaces[spaceID] = ibdSpace
	sm.ibdFiles[spaceID] = ibdFile
	sm.nameToID[name] = spaceID
	if spaceID != 0 && spaceID >= sm.nextID {
		sm.nextID = spaceID + 1
	}

	return ibdSpace, nil
}

func (sm *SpaceManagerImpl) GetSpace(spaceID uint32) (basic.Space, error) {
	sm.RLock()
	defer sm.RUnlock()

	space, exists := sm.spaces[spaceID]
	if !exists {
		return nil, fmt.Errorf("tablespace %d not found", spaceID)
	}
	return space, nil
}

// ListSpaceIDs returns a stable snapshot of all tablespaces currently loaded
// by the manager. It is intentionally an optional capability rather than an
// addition to basic.SpaceManager, so existing embedders keep source
// compatibility while lifecycle decorators can migrate every discovered
// space during startup.
func (sm *SpaceManagerImpl) ListSpaceIDs() []uint32 {
	sm.RLock()
	defer sm.RUnlock()
	ids := make([]uint32, 0, len(sm.spaces))
	for id := range sm.spaces {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (sm *SpaceManagerImpl) DropSpace(spaceID uint32) error {
	sm.Lock()
	defer sm.Unlock()

	ibdSpace, exists := sm.spaces[spaceID]
	if !exists {
		return fmt.Errorf("tablespace %d not found", spaceID)
	}
	err := ibdSpace.DropTable()
	if err != nil {
		return fmt.Errorf("failed to drop tablespace: %v", err)
	}

	delete(sm.nameToID, ibdSpace.GetTableName())
	delete(sm.spaces, spaceID)
	delete(sm.ibdFiles, spaceID)

	return nil
}

func (sm *SpaceManagerImpl) AllocateExtent(spaceID uint32, purpose basic.ExtentPurpose) (basic.Extent, error) {
	sm.RLock()
	defer sm.RUnlock()

	space, exists := sm.spaces[spaceID]
	if !exists {
		return nil, fmt.Errorf("tablespace %d not found", spaceID)
	}

	return space.AllocateExtent(purpose)
}

func (sm *SpaceManagerImpl) FreeExtent(spaceID, extentID uint32) error {
	sm.RLock()
	defer sm.RUnlock()

	space, exists := sm.spaces[spaceID]
	if !exists {
		return fmt.Errorf("tablespace %d not found", spaceID)
	}

	return space.FreeExtent(extentID)
}

func (sm *SpaceManagerImpl) Begin() (basic.Tx, error) {
	txID := atomic.AddUint64(&sm.txID, 1)
	return newSpaceTx(txID), nil
}

func (sm *SpaceManagerImpl) CreateNewTablespace(name string) uint32 {
	sm.Lock()
	spaceID := sm.nextID
	sm.nextID++
	sm.Unlock()

	_, err := sm.CreateSpace(spaceID, name, false)
	if err != nil {
		logger.Warnf("failed to create tablespace %s (spaceID=%d): %v", name, spaceID, err)
		return 0
	}

	return spaceID
}

func (sm *SpaceManagerImpl) CreateTableSpace(name string) (uint32, error) {
	sm.Lock()
	defer sm.Unlock()

	if _, exists := sm.nameToID[name]; exists {
		return 0, fmt.Errorf("%w: %s", ErrTablespaceExists, name)
	}

	spaceID := sm.nextID
	sm.nextID++

	// 创建 IBD 文件实例
	ibdFile := ibd.NewIBDFile(sm.dataDir, name, spaceID)

	// 检查文件是否已存在
	fileExists := ibdFile.Exists()

	if fileExists {
		// 文件已存在，打开并读取
		logger.Debugf("IBD file already exists, opening: %s (Space ID: %d)", name, spaceID)
		err := ibdFile.Open()
		if err != nil {
			return 0, fmt.Errorf("failed to open existing tablespace %s: %v", name, err)
		}
	} else {
		// 文件不存在，创建新文件
		logger.Debugf("Creating new IBD file: %s (Space ID: %d)", name, spaceID)
		err := ibdFile.Create()
		if err != nil {
			return 0, fmt.Errorf("failed to create tablespace %s: %v", name, err)
		}
	}

	// 创建 IBDSpace 实例
	ibdSpace := space.NewIBDSpace(ibdFile, false)

	// 设置为活动状态
	ibdSpace.SetActive(true)
	if fileExists {
		if err := ibdSpace.RecoverAllocationsFromFileSize(); err != nil {
			ibdFile.Close()
			return 0, fmt.Errorf("failed to recover page allocations for %s: %v", name, err)
		}
	}

	// 如果是新创建的文件，分配第一个extent用于系统页面
	if !fileExists {
		// 分配第一个extent用于系统页面
		extent, err := ibdSpace.AllocateExtent(basic.ExtentPurposeSystem)
		if err != nil {
			ibdFile.Close()
			return 0, fmt.Errorf("failed to allocate system extent for %s: %v", name, err)
		}

		// 标记前几个页面为已分配（FSP header, IBUF bitmap等）
		for i := 0; i < 2; i++ {
			// 这里应该通过IBDSpace的内部方法来标记页面为已分配
			// 但由于IBDSpace的pageAllocs是私有的，我们暂时跳过这个步骤
			_ = extent // 避免未使用变量警告
		}
	}

	// 注册到管理器
	sm.spaces[spaceID] = ibdSpace
	sm.ibdFiles[spaceID] = ibdFile
	sm.nameToID[name] = spaceID

	return spaceID, nil
}

// RenameTableSpace moves a user tablespace file and updates the in-memory
// name index while preserving its space ID. The file handle must be closed
// before the move on Windows; the replacement IBDSpace then reconstructs its
// allocation state from the renamed file.
func (sm *SpaceManagerImpl) RenameTableSpace(oldName, newName string) error {
	sm.Lock()
	defer sm.Unlock()

	if oldName == newName {
		return nil
	}
	spaceID, exists := sm.nameToID[oldName]
	if !exists {
		return fmt.Errorf("tablespace %s not found", oldName)
	}
	if _, exists := sm.nameToID[newName]; exists {
		return fmt.Errorf("tablespace %s already exists", newName)
	}
	oldSpace, exists := sm.spaces[spaceID]
	if !exists || oldSpace == nil {
		return fmt.Errorf("tablespace %s is not loaded", oldName)
	}
	isSystem := oldSpace.IsSystem()
	oldPath := filepath.Join(sm.dataDir, oldName+".ibd")
	newPath := filepath.Join(sm.dataDir, newName+".ibd")
	if _, err := os.Stat(newPath); err == nil {
		return fmt.Errorf("tablespace %s already exists", newName)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("check destination tablespace failed: %v", err)
	}
	if err := oldSpace.Close(); err != nil {
		return fmt.Errorf("close tablespace %s failed: %v", oldName, err)
	}
	reopenOld := func() {
		file := ibd.NewIBDFile(sm.dataDir, oldName, spaceID)
		if err := file.Open(); err != nil {
			return
		}
		restored := space.NewIBDSpace(file, isSystem)
		if err := restored.RecoverAllocationsFromFileSize(); err != nil {
			_ = file.Close()
			return
		}
		sm.spaces[spaceID] = restored
		sm.ibdFiles[spaceID] = file
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0755); err != nil {
		reopenOld()
		return fmt.Errorf("create destination tablespace directory failed: %v", err)
	}
	if err := os.Rename(oldPath, newPath); err != nil {
		reopenOld()
		return fmt.Errorf("rename tablespace file failed: %v", err)
	}
	newFile := ibd.NewIBDFile(sm.dataDir, newName, spaceID)
	if err := newFile.Open(); err != nil {
		_ = os.Rename(newPath, oldPath)
		reopenOld()
		return fmt.Errorf("reopen renamed tablespace failed: %v", err)
	}
	newSpace := space.NewIBDSpace(newFile, isSystem)
	if err := newSpace.RecoverAllocationsFromFileSize(); err != nil {
		_ = newFile.Close()
		_ = os.Rename(newPath, oldPath)
		reopenOld()
		return fmt.Errorf("recover renamed tablespace failed: %v", err)
	}
	delete(sm.nameToID, oldName)
	sm.nameToID[newName] = spaceID
	sm.spaces[spaceID] = newSpace
	sm.ibdFiles[spaceID] = newFile
	return nil
}

func (sm *SpaceManagerImpl) GetTableSpace(spaceID uint32) (basic.FileTableSpace, error) {
	sm.RLock()
	defer sm.RUnlock()

	space, exists := sm.spaces[spaceID]
	if !exists {
		return nil, fmt.Errorf("tablespace %d not found", spaceID)
	}
	return space.AsFileTableSpace(), nil
}

func (sm *SpaceManagerImpl) GetTableSpaceByName(name string) (basic.FileTableSpace, error) {
	sm.RLock()
	defer sm.RUnlock()

	spaceID, exists := sm.nameToID[name]
	if !exists {
		return nil, fmt.Errorf("tablespace %s not found", name)
	}
	return sm.spaces[uint32(spaceID)].AsFileTableSpace(), nil
}

func (sm *SpaceManagerImpl) GetTableSpaceInfo(spaceID uint32) (*basic.TableSpaceInfo, error) {
	sm.RLock()
	defer sm.RUnlock()

	space, exists := sm.spaces[spaceID]
	if !exists {
		return nil, fmt.Errorf("tablespace %d not found", spaceID)
	}

	// Get total size (allocated space)
	totalSize := space.GetTotalSize()

	// Get free space
	freeSpace := space.GetFreeSpace()

	// Get segment count
	segmentCount := space.GetSegmentCount()

	info := &basic.TableSpaceInfo{
		SpaceID:      spaceID,
		Name:         space.Name(),
		FilePath:     space.GetFilePath(),
		Size:         totalSize,
		FreeSpace:    freeSpace,
		SegmentCount: segmentCount,
	}
	return info, nil
}

// GetDetailedSpaceStats returns detailed statistics for a tablespace
func (sm *SpaceManagerImpl) GetDetailedSpaceStats(spaceID uint32) (*space.SpaceDetailedStats, error) {
	sm.RLock()
	defer sm.RUnlock()

	ibdSpace, exists := sm.spaces[spaceID]
	if !exists {
		return nil, fmt.Errorf("tablespace %d not found", spaceID)
	}

	return ibdSpace.GetDetailedStats(), nil
}

func (sm *SpaceManagerImpl) DropTableSpace(spaceID uint32) error {
	sm.Lock()
	defer sm.Unlock()

	space, exists := sm.spaces[spaceID]
	if !exists {
		return fmt.Errorf("tablespace %d not found", spaceID)
	}

	err := space.DropTable()
	if err != nil {
		return fmt.Errorf("failed to drop tablespace: %v", err)
	}

	delete(sm.nameToID, space.Name())
	delete(sm.spaces, spaceID)
	delete(sm.ibdFiles, spaceID)
	return nil
}

func (sm *SpaceManagerImpl) Close() error {
	sm.Lock()
	defer sm.Unlock()

	var lastErr error
	for _, space := range sm.spaces {
		if err := space.Close(); err != nil {
			lastErr = err
		}
	}

	sm.spaces = make(map[uint32]*space.IBDSpace)
	sm.ibdFiles = make(map[uint32]*ibd.IBD_File)
	sm.nameToID = make(map[string]uint32)

	return lastErr
}

// spaceTx implements the Tx interface for space transactions
type spaceTx struct {
	sync.Mutex
	id        uint64
	committed bool
	writes    []func()
}

// newSpaceTx creates a new space transaction
func newSpaceTx(id uint64) *spaceTx {
	return &spaceTx{
		id:        id,
		committed: false,
		writes:    make([]func(), 0),
	}
}

// AddWrite adds a write operation to the transaction
func (tx *spaceTx) AddWrite(writeFn func()) {
	tx.Lock()
	defer tx.Unlock()

	tx.writes = append(tx.writes, writeFn)
}

// Commit commits all write operations in the transaction
func (tx *spaceTx) Commit() error {
	tx.Lock()
	defer tx.Unlock()

	if tx.committed {
		return nil
	}

	// Execute all write operations
	for _, write := range tx.writes {
		write()
	}

	tx.committed = true
	return nil
}

// Rollback discards all write operations
func (tx *spaceTx) Rollback() error {
	tx.Lock()
	defer tx.Unlock()

	if tx.committed {
		return nil
	}

	// Clear write operations
	tx.writes = tx.writes[:0]
	return nil
}

// LoadExistingTablespaces 扫描数据目录并加载所有现有的IBD文件
func (sm *SpaceManagerImpl) LoadExistingTablespaces() error {
	sm.Lock()
	defer sm.Unlock()

	logger.Debug("Scanning for existing IBD files...")

	// 扫描数据目录
	return sm.scanDirectory(sm.dataDir, "")
}

// scanDirectory 递归扫描目录查找IBD文件
func (sm *SpaceManagerImpl) scanDirectory(dirPath, relativePath string) error {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("failed to read directory %s: %v", dirPath, err)
	}

	for _, entry := range entries {
		fullPath := filepath.Join(dirPath, entry.Name())
		currentRelativePath := relativePath
		if currentRelativePath != "" {
			currentRelativePath = filepath.Join(currentRelativePath, entry.Name())
		} else {
			currentRelativePath = entry.Name()
		}

		if entry.IsDir() {
			// 递归扫描子目录
			if err := sm.scanDirectory(fullPath, currentRelativePath); err != nil {
				return err
			}
		} else if strings.HasSuffix(entry.Name(), ".ibd") {
			// 找到IBD文件，尝试加载
			tableName := filepath.ToSlash(strings.TrimSuffix(currentRelativePath, ".ibd"))
			if tableName != "ibdata1" && (strings.HasPrefix(tableName, "mysql/") ||
				strings.HasPrefix(tableName, "information_schema/") ||
				strings.HasPrefix(tableName, "performance_schema/")) {
				continue
			}

			// 跳过已经加载的表空间
			if _, exists := sm.nameToID[tableName]; exists {
				continue
			}

			// 为系统表空间分配正确的Space ID
			var spaceID uint32
			if tableName == "ibdata1" {
				spaceID = 0 // 系统表空间固定为Space ID 0
			} else if reservedID, ok := reservedSystemSpaceID(tableName); ok {
				spaceID = reservedID
			} else if persistedID, ok := persistedTablespaceID(strings.TrimSuffix(fullPath, ".ibd") + ".frm"); ok {
				spaceID = persistedID
			} else if persistedID, ok := persistedPartitionTablespaceID(sm.dataDir, tableName); ok {
				spaceID = persistedID
			} else {
				spaceID = sm.getNextAvailableSpaceID()
			}

			logger.Debugf("Found existing IBD file: %s, assigning Space ID: %d", tableName, spaceID)

			// 创建IBD文件实例并打开
			ibdFile := ibd.NewIBDFile(sm.dataDir, tableName, spaceID)
			if err := ibdFile.Open(); err != nil {
				logger.Debugf("Warning: failed to open existing IBD file %s: %v", tableName, err)
				continue
			}

			// 创建IBDSpace实例
			isSystem := strings.HasPrefix(tableName, "mysql/") ||
				strings.HasPrefix(tableName, "information_schema/") ||
				strings.HasPrefix(tableName, "performance_schema/") ||
				tableName == "ibdata1"

			ibdSpace := space.NewIBDSpace(ibdFile, isSystem)
			if err := ibdSpace.RecoverAllocationsFromFileSize(); err != nil {
				logger.Debugf("Warning: failed to recover allocations for %s: %v", tableName, err)
				_ = ibdFile.Close()
				continue
			}

			// 注册到管理器
			sm.spaces[spaceID] = ibdSpace
			sm.ibdFiles[spaceID] = ibdFile
			sm.nameToID[tableName] = spaceID

			// 更新nextID（但不要影响系统表空间的ID分配）
			if spaceID != 0 && spaceID >= sm.nextID {
				sm.nextID = spaceID + 1
			}
		}
	}

	return nil
}

func reservedSystemSpaceID(tableName string) (uint32, bool) {
	mysqlTables := []string{
		"mysql/user", "mysql/db", "mysql/tables_priv", "mysql/columns_priv", "mysql/procs_priv",
		"mysql/proxies_priv", "mysql/role_edges", "mysql/default_roles", "mysql/global_grants",
		"mysql/password_history", "mysql/component", "mysql/server_cost", "mysql/engine_cost",
		"mysql/time_zone", "mysql/time_zone_name", "mysql/time_zone_transition",
		"mysql/time_zone_transition_type", "mysql/help_topic", "mysql/help_category",
		"mysql/help_relation", "mysql/help_keyword", "mysql/plugin", "mysql/servers",
		"mysql/func", "mysql/general_log", "mysql/slow_log",
	}
	for i, name := range mysqlTables {
		if tableName == name {
			return uint32(i + 1), true
		}
	}
	infoTables := []string{
		"information_schema/schemata", "information_schema/tables", "information_schema/columns",
		"information_schema/statistics", "information_schema/key_column_usage",
		"information_schema/table_constraints", "information_schema/referential_constraints",
		"information_schema/views", "information_schema/triggers", "information_schema/routines",
		"information_schema/parameters", "information_schema/events", "information_schema/partitions",
		"information_schema/engines", "information_schema/plugins", "information_schema/processlist",
		"information_schema/user_privileges", "information_schema/schema_privileges",
		"information_schema/table_privileges", "information_schema/column_privileges",
	}
	for i, name := range infoTables {
		if tableName == name {
			return uint32(100 + i), true
		}
	}
	performanceTables := []string{
		"performance_schema/accounts", "performance_schema/cond_instances",
		"performance_schema/events_stages_current", "performance_schema/events_stages_history",
		"performance_schema/events_stages_history_long", "performance_schema/events_statements_current",
		"performance_schema/events_statements_history", "performance_schema/events_statements_history_long",
		"performance_schema/events_waits_current", "performance_schema/events_waits_history",
		"performance_schema/events_waits_history_long", "performance_schema/file_instances",
		"performance_schema/file_summary_by_event_name", "performance_schema/file_summary_by_instance",
		"performance_schema/host_cache", "performance_schema/hosts", "performance_schema/mutex_instances",
		"performance_schema/objects_summary_global_by_type", "performance_schema/performance_timers",
		"performance_schema/rwlock_instances", "performance_schema/setup_actors",
		"performance_schema/setup_consumers", "performance_schema/setup_instruments",
		"performance_schema/setup_objects", "performance_schema/setup_timers",
		"performance_schema/socket_instances", "performance_schema/socket_summary_by_event_name",
		"performance_schema/socket_summary_by_instance", "performance_schema/table_io_waits_summary_by_index_usage",
		"performance_schema/table_io_waits_summary_by_table", "performance_schema/table_lock_waits_summary_by_table",
		"performance_schema/threads", "performance_schema/users",
	}
	for i, name := range performanceTables {
		if tableName == name {
			return uint32(200 + i), true
		}
	}
	return 0, false
}

func persistedTablespaceID(frmPath string) (uint32, bool) {
	raw, err := os.ReadFile(frmPath)
	if err != nil {
		return 0, false
	}
	var definition struct {
		StorageSpaceID   uint32 `json:"storage_space_id"`
		DiscardedSpaceID uint32 `json:"discarded_space_id"`
		Discarded        bool   `json:"tablespace_discarded"`
	}
	if err := json.Unmarshal(raw, &definition); err != nil {
		return 0, false
	}
	if definition.StorageSpaceID != 0 {
		return definition.StorageSpaceID, true
	}
	if definition.Discarded && definition.DiscardedSpaceID != 0 {
		return definition.DiscardedSpaceID, true
	}
	return 0, false
}

func persistedPartitionTablespaceID(dataDir, tablespaceName string) (uint32, bool) {
	separator := strings.LastIndex(tablespaceName, "#")
	if separator <= 0 || separator == len(tablespaceName)-1 {
		return 0, false
	}
	tableName := tablespaceName[:separator]
	partitionName := tablespaceName[separator+1:]
	frmPath := filepath.Join(dataDir, filepath.FromSlash(tableName)+".frm")
	raw, err := os.ReadFile(frmPath)
	if err != nil {
		return 0, false
	}
	var definition struct {
		Partitioning struct {
			Partitions []struct {
				Name           string `json:"name"`
				StorageSpaceID uint32 `json:"storage_space_id"`
			} `json:"partitions"`
		} `json:"partitioning"`
	}
	if err := json.Unmarshal(raw, &definition); err != nil {
		return 0, false
	}
	for _, partition := range definition.Partitioning.Partitions {
		if strings.EqualFold(strings.TrimSpace(partition.Name), strings.TrimSpace(partitionName)) && partition.StorageSpaceID != 0 {
			return partition.StorageSpaceID, true
		}
	}
	return 0, false
}

// getNextAvailableSpaceID 获取下一个可用的Space ID
func (sm *SpaceManagerImpl) getNextAvailableSpaceID() uint32 {
	for {
		if _, exists := sm.spaces[sm.nextID]; !exists {
			id := sm.nextID
			sm.nextID++
			return id
		}
		sm.nextID++
	}
}
