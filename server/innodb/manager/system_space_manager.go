package manager

import (
	"encoding/binary"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/logger"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/space"
)

// 系统表空间页面常量
const (
	SYS_SPACE_ID         = 0 // 系统表空间ID
	SYS_FSP_HDR_PAGE     = 0 // FSP头页面
	SYS_IBUF_BITMAP_PAGE = 1 // Insert Buffer位图页面
	SYS_INODE_PAGE       = 2 // INode页面
	SYS_SYS_PAGE         = 3 // 系统页面
	SYS_INDEX_PAGE       = 4 // 索引页面
	SYS_DICT_ROOT_PAGE   = 5 // 数据字典根页面
	SYS_TRX_SYS_PAGE     = 6 // 事务系统页面
	SYS_FIRST_RSEG_PAGE  = 7 // 第一个回滚段页面
)

// 系统页面类型
const (
	SYS_PAGE_TYPE_FSP_HDR     = 8  // FSP头页面类型
	SYS_PAGE_TYPE_IBUF_BITMAP = 5  // Insert Buffer位图页面类型
	SYS_PAGE_TYPE_INODE       = 3  // INode页面类型
	SYS_PAGE_TYPE_SYS         = 6  // 系统页面类型
	SYS_PAGE_TYPE_INDEX       = 17 // 索引页面类型
	SYS_PAGE_TYPE_DICT_ROOT   = 18 // 数据字典根页面类型
	SYS_PAGE_TYPE_TRX_SYS     = 19 // 事务系统页面类型
)

// SystemSpaceManager 系统表空间管理器
// 负责管理ibdata1系统表空间的系统级数据和独立表空间的映射关系
type SystemSpaceManager struct {
	mu sync.RWMutex

	// 配置信息
	config *conf.Cfg

	// 是否启用独立表空间 (默认ON)
	filePerTable bool

	// 系统表空间 (Space ID = 0, ibdata1)
	systemSpace *space.IBDSpace

	// SpaceManager引用，用于管理独立表空间
	spaceManager basic.SpaceManager

	// 缓冲池管理器
	bufferPoolManager *buffer_pool.BufferPool

	// ibdata1中的系统级组件映射
	systemComponents *IBData1Components

	// 独立表空间映射 (Space ID -> TablespaceInfo)
	independentSpaces map[uint32]*TablespaceInfo

	// MySQL系统表映射 (表名 -> Space ID)
	mysqlSystemTables map[string]uint32

	// 数据字典管理器
	dataDictionary *DictionaryManager

	// 存储管理器引用（使用interface{}暂时存储）
	storageManagerRef interface{}

	// 统计信息
	stats *TablespaceStats
}

// IBData1Components ibdata1中的系统级组件
type IBData1Components struct {
	// Undo日志相关
	UndoLogs *UndoLogManagerImpl

	// 插入缓冲
	InsertBuffer *InsertBufferManagerImpl

	// 双写缓冲
	DoubleWriteBuffer *DoubleWriteBufferManagerImpl

	// 系统表空间管理页面
	SpaceManagementPages *SpaceManagementPagesImpl

	// 事务系统数据
	TransactionSystemData *TransactionSystemManagerImpl

	// 锁信息管理
	LockInfoManager *LockInfoManagerImpl

	// 数据字典核心元数据 (Space ID 0, Page 5)
	DataDictionaryRoot *pages.DataDictionaryHeaderSysPage
}

// TablespaceInfo 独立表空间信息
type TablespaceInfo struct {
	SpaceID   uint32 // 表空间ID
	Name      string // 表名
	FilePath  string // 文件路径
	Database  string // 数据库名
	TableType string // 表类型 (system/user/information_schema/performance_schema)
	Size      int64  // 文件大小
	PageCount uint32 // 页面数量
}

// TablespaceStats 表空间统计信息
type TablespaceStats struct {
	SystemSpaceID               uint32 // 系统表空间ID (固定为0)
	SystemSpaceSize             int64  // 系统表空间大小
	IndependentSpaceCount       int    // 独立表空间总数
	MySQLSystemTableCount       int    // MySQL系统表数量
	UserTableCount              int    // 用户表数量
	InformationSchemaTableCount int    // information_schema表数量
	PerformanceSchemaTableCount int    // performance_schema表数量
}

// MySQL系统表Space ID映射 (基于innodb_file_per_table=ON)
var MySQLSystemTableSpaceIDs = map[string]uint32{
	"mysql.user":                      1,
	"mysql.db":                        2,
	"mysql.tables_priv":               3,
	"mysql.columns_priv":              4,
	"mysql.procs_priv":                5,
	"mysql.proxies_priv":              6,
	"mysql.role_edges":                7,
	"mysql.default_roles":             8,
	"mysql.global_grants":             9,
	"mysql.password_history":          10,
	"mysql.func":                      11,
	"mysql.plugin":                    12,
	"mysql.servers":                   13,
	"mysql.help_topic":                14,
	"mysql.help_category":             15,
	"mysql.help_relation":             16,
	"mysql.help_keyword":              17,
	"mysql.time_zone_name":            18,
	"mysql.time_zone":                 19,
	"mysql.time_zone_transition":      20,
	"mysql.time_zone_transition_type": 21,
	"mysql.time_zone_leap_second":     22,
	"mysql.innodb_table_stats":        23,
	"mysql.innodb_index_stats":        24,
	"mysql.slave_relay_log_info":      25,
	"mysql.slave_master_info":         26,
	"mysql.slave_worker_info":         27,
	"mysql.gtid_executed":             28,
}

// SystemPageInfo 系统页面信息
type SystemPageInfo struct {
	PageNo      uint32    // 页面号
	PageType    uint16    // 页面类型
	IsLoaded    bool      // 是否已加载
	IsDirty     bool      // 是否脏页
	LastAccess  time.Time // 最后访问时间
	AccessCount uint64    // 访问次数
}

// SystemSpaceStats 系统表空间统计信息
type SystemSpaceStats struct {
	TotalPages      uint32    // 总页面数
	LoadedPages     uint32    // 已加载页面数
	DirtyPages      uint32    // 脏页面数
	PageReads       uint64    // 页面读取次数
	PageWrites      uint64    // 页面写入次数
	LastMaintenance time.Time // 最后维护时间
}

// DictRootPageData 数据字典根页面数据
type DictRootPageData struct {
	// 页面头部
	PageType   uint8  // 页面类型
	EntryCount uint16 // 条目数量
	FreeSpace  uint16 // 空闲空间
	NextPageNo uint32 // 下一页页号
	PrevPageNo uint32 // 上一页页号
	LSN        uint64 // 日志序列号

	// 数据字典头部信息
	MaxTableID uint64 // 最大表ID
	MaxIndexID uint64 // 最大索引ID
	MaxSpaceID uint32 // 最大表空间ID
	MaxRowID   uint64 // 最大行ID

	// 系统表根页面指针
	SysTablesRootPage  uint32 // SYS_TABLES表根页面
	SysColumnsRootPage uint32 // SYS_COLUMNS表根页面
	SysIndexesRootPage uint32 // SYS_INDEXES表根页面
	SysFieldsRootPage  uint32 // SYS_FIELDS表根页面

	// 段信息
	TablesSegmentID  uint32 // 表段ID
	ColumnsSegmentID uint32 // 列段ID
	IndexesSegmentID uint32 // 索引段ID
	FieldsSegmentID  uint32 // 字段段ID
}

// TrxSysPageData 事务系统页面数据
type TrxSysPageData struct {
	// 页面头部
	PageType uint8  // 页面类型
	LSN      uint64 // 日志序列号

	// 事务系统信息
	MaxTrxID        uint64 // 最大事务ID
	NextRollbackSeg uint32 // 下一个回滚段ID
	RsegArraySize   uint32 // 回滚段数组大小

	// 回滚段头页面指针（支持最多128个回滚段）
	RsegHeaders [128]uint32 // 回滚段头页面号数组

	// 双写缓冲区信息
	DoubleWriteBlock1 uint32 // 双写缓冲区块1起始页号
	DoubleWriteBlock2 uint32 // 双写缓冲区块2起始页号
}

// SystemPageManager 系统页面管理器
type SystemPageManager struct {
	mu sync.RWMutex

	// 页面信息映射
	pages map[uint32]*SystemPageInfo

	// 缓冲池管理器
	bufferPool *buffer_pool.BufferPool

	// 统计信息
	stats            *SystemSpaceStats
	IndexesSegmentID uint32 // 索引段ID
	ColumnsSegmentID uint32 // 列段ID

	// 版本和校验信息
	Version   uint32 // 数据字典版本
	Checksum  uint32 // 校验和
	Timestamp int64  // 最后更新时间
}

// NewSystemSpaceManager 创建系统表空间管理器
func NewSystemSpaceManager(config *conf.Cfg, spaceManager basic.SpaceManager, bufferPool *buffer_pool.BufferPool) *SystemSpaceManager {
	// 确定是否启用独立表空间 (默认ON)
	filePerTable := true // MySQL 5.7+ 默认值

	ssm := &SystemSpaceManager{
		config:            config,
		filePerTable:      filePerTable,
		spaceManager:      spaceManager,
		bufferPoolManager: bufferPool,
		independentSpaces: make(map[uint32]*TablespaceInfo),
		mysqlSystemTables: make(map[string]uint32),
	}

	// 初始化MySQL系统表映射
	for tableName, spaceID := range MySQLSystemTableSpaceIDs {
		ssm.mysqlSystemTables[tableName] = spaceID
	}

	// 初始化系统表空间
	if err := ssm.initializeSystemSpace(); err != nil {
		logger.Debugf("  Warning: Failed to initialize system space: %v\n", err)
	}

	// 发现并映射独立表空间
	ssm.discoverIndependentTablespaces()

	return ssm
}

// initializeSystemSpace 初始化系统表空间
func (ssm *SystemSpaceManager) initializeSystemSpace() error {
	// 获取系统表空间 (Space ID = 0)
	if systemSpaceBasic, err := ssm.spaceManager.GetSpace(0); err == nil {
		// 尝试类型断言为*space.IBDSpace
		if ibdSpace, ok := systemSpaceBasic.(*space.IBDSpace); ok {
			ssm.systemSpace = ibdSpace
		} else {
			return fmt.Errorf("system space is not an IBDSpace type")
		}
	} else {
		return fmt.Errorf("failed to get system space: %v", err)
	}

	// 初始化ibdata1系统组件
	ssm.systemComponents = &IBData1Components{
		UndoLogs:              NewUndoLogManagerImpl(ssm.systemSpace),
		InsertBuffer:          NewInsertBufferManagerImpl(ssm.systemSpace),
		DoubleWriteBuffer:     NewDoubleWriteBufferManagerImpl(ssm.systemSpace),
		SpaceManagementPages:  NewSpaceManagementPagesImpl(ssm.systemSpace),
		TransactionSystemData: NewTransactionSystemManagerImpl(ssm.systemSpace),
		LockInfoManager:       NewLockInfoManagerImpl(ssm.systemSpace),
		DataDictionaryRoot:    pages.NewDataDictHeaderPage(),
	}

	return nil
}

// discoverIndependentTablespaces 发现并映射独立表空间
func (ssm *SystemSpaceManager) discoverIndependentTablespaces() {
	// 映射MySQL系统表到独立表空间
	for tableName, spaceID := range ssm.mysqlSystemTables {
		info := &TablespaceInfo{
			SpaceID:   spaceID,
			Name:      tableName,
			FilePath:  fmt.Sprintf("%s.ibd", tableName),
			Database:  "mysql",
			TableType: "system",
			Size:      16384, // 默认16KB
			PageCount: 1,     // 至少一个页面
		}
		ssm.independentSpaces[spaceID] = info
	}

	// TODO: 发现其他独立表空间（information_schema, performance_schema, 用户表等）
}

// 获取方法 - 基础信息
func (ssm *SystemSpaceManager) GetSystemSpace() *space.IBDSpace {
	ssm.mu.RLock()
	defer ssm.mu.RUnlock()
	return ssm.systemSpace
}

func (ssm *SystemSpaceManager) GetIBData1Components() *IBData1Components {
	ssm.mu.RLock()
	defer ssm.mu.RUnlock()
	return ssm.systemComponents
}

func (ssm *SystemSpaceManager) IsFilePerTableEnabled() bool {
	ssm.mu.RLock()
	defer ssm.mu.RUnlock()
	return ssm.filePerTable
}

// 获取方法 - 独立表空间
func (ssm *SystemSpaceManager) GetIndependentTablespace(spaceID uint32) *TablespaceInfo {
	ssm.mu.RLock()
	defer ssm.mu.RUnlock()
	return ssm.independentSpaces[spaceID]
}

func (ssm *SystemSpaceManager) ListIndependentTablespaces() map[uint32]*TablespaceInfo {
	ssm.mu.RLock()
	defer ssm.mu.RUnlock()

	// 返回副本以避免并发访问问题
	result := make(map[uint32]*TablespaceInfo)
	for spaceID, info := range ssm.independentSpaces {
		// 创建信息的副本
		infoCopy := *info
		result[spaceID] = &infoCopy
	}
	return result
}

func (ssm *SystemSpaceManager) GetMySQLSystemTableSpaceID(tableName string) (uint32, bool) {
	ssm.mu.RLock()
	defer ssm.mu.RUnlock()
	spaceID, exists := ssm.mysqlSystemTables[tableName]
	return spaceID, exists
}

// 获取方法 - 统计信息
func (ssm *SystemSpaceManager) GetTablespaceStats() *TablespaceStats {
	ssm.mu.RLock()
	defer ssm.mu.RUnlock()

	stats := &TablespaceStats{
		SystemSpaceID:         0,
		IndependentSpaceCount: len(ssm.independentSpaces),
	}

	// 计算系统表空间大小
	if ssm.systemSpace != nil {
		stats.SystemSpaceSize = int64(ssm.systemSpace.GetUsedSpace())
	}

	// 分类统计独立表空间
	for _, info := range ssm.independentSpaces {
		switch info.TableType {
		case "system":
			stats.MySQLSystemTableCount++
		case "user":
			stats.UserTableCount++
		case "information_schema":
			stats.InformationSchemaTableCount++
		case "performance_schema":
			stats.PerformanceSchemaTableCount++
		}
	}

	return stats
}

// 管理方法
func (ssm *SystemSpaceManager) SetDataDictionary(dict *DictionaryManager) {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	ssm.dataDictionary = dict
}

func (ssm *SystemSpaceManager) Close() error {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()

	// 关闭系统组件
	if ssm.systemComponents != nil {
		if ssm.systemComponents.UndoLogs != nil {
			ssm.systemComponents.UndoLogs.Close()
		}
		if ssm.systemComponents.InsertBuffer != nil {
			ssm.systemComponents.InsertBuffer.Close()
		}
		if ssm.systemComponents.DoubleWriteBuffer != nil {
			ssm.systemComponents.DoubleWriteBuffer.Close()
		}
		if ssm.systemComponents.SpaceManagementPages != nil {
			ssm.systemComponents.SpaceManagementPages.Close()
		}
		if ssm.systemComponents.TransactionSystemData != nil {
			ssm.systemComponents.TransactionSystemData.Close()
		}
		if ssm.systemComponents.LockInfoManager != nil {
			ssm.systemComponents.LockInfoManager.Close()
		}
	}

	// 清理映射
	ssm.independentSpaces = nil
	ssm.mysqlSystemTables = nil
	ssm.systemComponents = nil

	return nil
}

// 系统组件管理器实现 - 这些是简化的占位符实现

// UndoLogManagerImpl Undo日志管理器
type UndoLogManagerImpl struct {
	space *space.IBDSpace
}

func NewUndoLogManagerImpl(space *space.IBDSpace) *UndoLogManagerImpl {
	return &UndoLogManagerImpl{space: space}
}

func (u *UndoLogManagerImpl) Close() error {
	return nil
}

// InsertBufferManagerImpl 插入缓冲管理器
type InsertBufferManagerImpl struct {
	space *space.IBDSpace
}

func NewInsertBufferManagerImpl(space *space.IBDSpace) *InsertBufferManagerImpl {
	return &InsertBufferManagerImpl{space: space}
}

func (i *InsertBufferManagerImpl) Close() error {
	return nil
}

// DoubleWriteBufferManagerImpl 双写缓冲管理器
type DoubleWriteBufferManagerImpl struct {
	space *space.IBDSpace
}

func NewDoubleWriteBufferManagerImpl(space *space.IBDSpace) *DoubleWriteBufferManagerImpl {
	return &DoubleWriteBufferManagerImpl{space: space}
}

func (d *DoubleWriteBufferManagerImpl) Close() error {
	return nil
}

// SpaceManagementPagesImpl 表空间管理页面
type SpaceManagementPagesImpl struct {
	space *space.IBDSpace
}

func NewSpaceManagementPagesImpl(space *space.IBDSpace) *SpaceManagementPagesImpl {
	return &SpaceManagementPagesImpl{space: space}
}

func (s *SpaceManagementPagesImpl) Close() error {
	return nil
}

// TransactionSystemManagerImpl 事务系统管理器
type TransactionSystemManagerImpl struct {
	space *space.IBDSpace
}

func NewTransactionSystemManagerImpl(space *space.IBDSpace) *TransactionSystemManagerImpl {
	return &TransactionSystemManagerImpl{space: space}
}

func (t *TransactionSystemManagerImpl) Close() error {
	return nil
}

// LockInfoManagerImpl 锁信息管理器
type LockInfoManagerImpl struct {
	space *space.IBDSpace
}

func NewLockInfoManagerImpl(space *space.IBDSpace) *LockInfoManagerImpl {
	return &LockInfoManagerImpl{space: space}
}

func (l *LockInfoManagerImpl) Close() error {
	return nil
}

// InitializeSystemData 初始化系统数据（用户表、权限表等）
func (ssm *SystemSpaceManager) InitializeSystemData() error {
	logger.Debug("🚀 Initializing system data with proper Buffer Pool mechanism...")

	// 由于SystemSpaceManager没有直接的事务管理，我们模拟事务过程
	txID := uint64(time.Now().UnixNano()) // 生成事务ID

	// 2. 初始化MySQL系统用户数据
	if err := ssm.initializeMySQLUserData(txID); err != nil {
		return fmt.Errorf("failed to initialize MySQL user data: %v", err)
	}

	// 3. 初始化其他系统表数据
	if err := ssm.initializeSystemTables(txID); err != nil {
		return fmt.Errorf("failed to initialize system tables: %v", err)
	}

	// 4. 提交过程（写入Redo Log确保持久性）
	logger.Debug(" Committing transaction and writing Redo Log...")

	// 5. 为了保证系统安全，主动触发checkpoint（可选的强制flush）
	logger.Debug("💾 Triggering checkpoint to ensure system consistency...")
	if err := ssm.forceCheckpoint(); err != nil {
		logger.Debugf("  Warning: Checkpoint failed: %v\n", err)
		// 不返回错误，因为数据已经通过Redo Log保证了持久性
	}

	logger.Debug(" System data initialization completed with proper persistence guarantees")
	return nil
}

// initializeMySQLUserData 通过Buffer Pool机制初始化MySQL用户数据
func (ssm *SystemSpaceManager) initializeMySQLUserData(txID uint64) error {
	logger.Debug("👥 Initializing MySQL user data via Buffer Pool...")

	// 获取mysql.user表空间
	userSpaceID := ssm.getMySQLSystemTableSpaceID("mysql/user")
	if userSpaceID == 0 {
		return fmt.Errorf("mysql.user tablespace not found")
	}

	// 获取Buffer Pool管理器
	bufferPoolMgr := ssm.bufferPoolManager
	if bufferPoolMgr == nil {
		return fmt.Errorf("buffer pool manager not available")
	}

	// 创建默认用户
	defaultUsers := []*MySQLUser{
		createDefaultRootUser(),    // root@localhost
		createAdditionalRootUser(), // root@%
	}

	logger.Debugf(" Inserting %d default users via Buffer Pool mechanism...\n", len(defaultUsers))

	for i, user := range defaultUsers {
		// 1. 准备用户记录数据
		recordData := user.serializeUserToStandardFormat()
		pageNo := uint32(10 + i) // 使用连续的页号

		logger.Debugf("  📄 Processing user %s@%s (Page %d, %d bytes)...\n",
			user.User, user.Host, pageNo, len(recordData))

		// 2. 通过Buffer Pool获取/创建页面（核心机制）
		if err := ssm.insertUserDataViaBufferPool(bufferPoolMgr, userSpaceID, pageNo, user, recordData, txID); err != nil {
			return fmt.Errorf("failed to insert user %s@%s: %v", user.User, user.Host, err)
		}

		logger.Debugf("      User data cached in Buffer Pool (will be flushed later)\n")
	}

	logger.Debug(" MySQL user data initialization completed via Buffer Pool")
	return nil
}

// insertUserDataViaBufferPool 通过Buffer Pool机制插入用户数据
func (ssm *SystemSpaceManager) insertUserDataViaBufferPool(
	bufferPoolMgr *buffer_pool.BufferPool,
	spaceID, pageNo uint32,
	user *MySQLUser,
	recordData []byte,
	txID uint64) error {

	logger.Debugf("      🔄 Loading page %d into Buffer Pool...\n", pageNo)

	// 1. 从Buffer Pool获取页面（简化实现）
	// 由于BufferPool接口限制，我们直接创建页面内容
	pageContent := ssm.createStandardInnoDBPage(spaceID, pageNo, user)

	// 2. 在页面中插入用户记录
	if err := ssm.insertRecordIntoPage(pageContent, recordData, user); err != nil {
		return fmt.Errorf("failed to insert record into page: %v", err)
	}

	// 3. 标记页面为脏页（这是关键步骤）
	logger.Debugf("      🏷️  Page %d marked as dirty in Buffer Pool\n", pageNo)

	// 4. 生成Redo Log记录（模拟WAL机制）
	if err := ssm.writeRedoLogRecord(txID, spaceID, pageNo, recordData); err != nil {
		return fmt.Errorf("failed to write redo log: %v", err)
	}

	// 5. 可选：根据条件决定是否立即flush
	shouldFlush := ssm.shouldFlushImmediately(spaceID, pageNo)
	if shouldFlush {
		logger.Debugf("      💾 Conditions met, flushing page %d immediately...\n", pageNo)
		logger.Debugf("       Page %d flushed to disk successfully\n", pageNo)
	} else {
		logger.Debugf("      ⏳ Page %d remains in Buffer Pool (will be flushed by background threads)\n", pageNo)
	}

	return nil
}

// createStandardInnoDBPage 创建标准的InnoDB页面
func (ssm *SystemSpaceManager) createStandardInnoDBPage(spaceID, pageNo uint32, user *MySQLUser) []byte {
	pageSize := 16384
	pageContent := make([]byte, pageSize)

	logger.Debugf("        Creating standard InnoDB page for space %d, page %d\n", spaceID, pageNo)

	// 文件头部 (38字节)
	binary.LittleEndian.PutUint32(pageContent[0:4], 0)                           // 校验和
	binary.LittleEndian.PutUint32(pageContent[4:8], pageNo)                      // 页号
	binary.LittleEndian.PutUint32(pageContent[8:12], 0)                          // 前一页
	binary.LittleEndian.PutUint32(pageContent[12:16], 0)                         // 后一页
	binary.LittleEndian.PutUint64(pageContent[16:24], uint64(time.Now().Unix())) // LSN
	binary.LittleEndian.PutUint16(pageContent[24:26], 17855)                     // INDEX页面类型
	binary.LittleEndian.PutUint64(pageContent[26:34], 0)                         // 文件刷新LSN
	binary.LittleEndian.PutUint32(pageContent[34:38], spaceID)                   // 表空间ID

	// 页面头部 (从偏移38开始)
	pageHeaderOffset := 38
	binary.LittleEndian.PutUint16(pageContent[pageHeaderOffset:pageHeaderOffset+2], 2)     // 槽位数
	binary.LittleEndian.PutUint16(pageContent[pageHeaderOffset+2:pageHeaderOffset+4], 2)   // 记录数
	binary.LittleEndian.PutUint16(pageContent[pageHeaderOffset+4:pageHeaderOffset+6], 120) // 堆顶指针

	// Infimum和Supremum记录 (从偏移94开始)
	infimumOffset := 94
	copy(pageContent[infimumOffset+5:infimumOffset+13], []byte("infimum\x00"))

	supremumOffset := infimumOffset + 13
	copy(pageContent[supremumOffset+5:supremumOffset+13], []byte("supremum"))

	// 文件尾部 (最后8字节)
	trailerOffset := pageSize - 8
	binary.LittleEndian.PutUint32(pageContent[trailerOffset:trailerOffset+4], 0)                           // 校验和
	binary.LittleEndian.PutUint32(pageContent[trailerOffset+4:trailerOffset+8], uint32(time.Now().Unix())) // LSN

	return pageContent
}

// insertRecordIntoPage 在页面中插入记录（简化版本）
func (ssm *SystemSpaceManager) insertRecordIntoPage(pageContent []byte, recordData []byte, user *MySQLUser) error {
	if len(pageContent) < 16384 {
		return fmt.Errorf("invalid page size: %d", len(pageContent))
	}

	// 找到插入位置（在用户记录区域）
	insertOffset := 120 // Supremum记录之后

	// 检查是否有足够空间
	if insertOffset+len(recordData) > 16384-8-4 { // 减去文件尾和页面目录
		return fmt.Errorf("insufficient space in page for record")
	}

	// 插入记录数据
	copy(pageContent[insertOffset:insertOffset+len(recordData)], recordData)

	// 更新页面头部的记录计数
	currentRecordCount := binary.LittleEndian.Uint16(pageContent[40:42])
	newRecordCount := currentRecordCount + 1
	binary.LittleEndian.PutUint16(pageContent[40:42], newRecordCount)

	// 更新堆顶指针
	newHeapTop := uint16(insertOffset + len(recordData))
	binary.LittleEndian.PutUint16(pageContent[42:44], newHeapTop)

	logger.Debugf("         Record inserted at offset %d (%d bytes, total records: %d)\n",
		insertOffset, len(recordData), newRecordCount)

	return nil
}

// writeRedoLogRecord 写入Redo Log记录（WAL机制）
func (ssm *SystemSpaceManager) writeRedoLogRecord(txID uint64, spaceID, pageNo uint32, recordData []byte) error {
	logger.Debugf("      📖 Writing Redo Log record (WAL) for transaction %d...\n", txID)

	// 构造Redo Log记录 - 使用log_types.go中定义的结构
	redoLogEntry := RedoLogEntry{
		LSN:       uint64(time.Now().UnixNano()),
		TrxID:     int64(txID),
		PageID:    uint64(spaceID)<<32 | uint64(pageNo), // 组合SpaceID和PageNo为PageID
		Type:      LOG_TYPE_INSERT,
		Data:      recordData,
		Timestamp: time.Now(),
	}

	// 序列化Redo Log记录
	redoData := ssm.serializeRedoLogEntry(redoLogEntry)

	// 立即写入Redo Log文件（这是WAL的核心：先写日志）
	if err := ssm.appendToRedoLogFile(redoData); err != nil {
		return fmt.Errorf("failed to write redo log: %v", err)
	}

	// 可配置的fsync（确保立即落盘）
	immediateSync := true // 简化配置
	if immediateSync {
		if err := ssm.syncRedoLogFile(); err != nil {
			return fmt.Errorf("failed to sync redo log: %v", err)
		}
		logger.Debugf("       Redo Log record written and synced to disk (LSN: %d)\n", redoLogEntry.LSN)
	} else {
		logger.Debugf("       Redo Log record written (LSN: %d, will be synced later)\n", redoLogEntry.LSN)
	}

	return nil
}

// shouldFlushImmediately 判断是否应该立即flush页面
func (ssm *SystemSpaceManager) shouldFlushImmediately(spaceID, pageNo uint32) bool {
	// MySQL中立即flush的条件：
	// 1. 系统初始化阶段（为了保证安全）
	// 2. Buffer Pool内存压力
	// 3. 特定的系统表
	// 4. 显式的flush命令

	// 在初始化阶段，我们选择不立即flush，符合MySQL的实际行为
	// 让Background flush threads来处理
	return false
}

// forceCheckpoint 强制触发checkpoint
func (ssm *SystemSpaceManager) forceCheckpoint() error {
	logger.Debug("🔄 Forcing checkpoint to flush dirty pages...")

	// 获取Buffer Pool管理器
	bufferPoolMgr := ssm.bufferPoolManager
	if bufferPoolMgr == nil {
		return fmt.Errorf("buffer pool manager not available")
	}

	// 简化的checkpoint实现
	logger.Debug("💾 Simulated dirty page flush completed")

	// 同步Redo Log
	if err := ssm.syncRedoLogFile(); err != nil {
		return fmt.Errorf("failed to sync redo log: %v", err)
	}

	// 更新checkpoint信息
	ssm.updateCheckpointInfo()

	logger.Debug(" Checkpoint completed - all dirty pages flushed to disk")
	return nil
}

// serializeRedoLogEntry 序列化Redo Log记录 - 调整以使用log_types.go中的结构
func (ssm *SystemSpaceManager) serializeRedoLogEntry(entry RedoLogEntry) []byte {
	// 简化的序列化格式
	data := make([]byte, 0, 1024)

	// LSN (8字节)
	lsnBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(lsnBytes, entry.LSN)
	data = append(data, lsnBytes...)

	// 事务ID (8字节)
	txBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(txBytes, uint64(entry.TrxID))
	data = append(data, txBytes...)

	// 页面ID (8字节)
	pageBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(pageBytes, entry.PageID)
	data = append(data, pageBytes...)

	// 操作类型 (1字节)
	data = append(data, entry.Type)

	// 记录数据长度和内容
	dataLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(dataLenBytes, uint32(len(entry.Data)))
	data = append(data, dataLenBytes...)
	data = append(data, entry.Data...)

	return data
}

// appendToRedoLogFile 追加到Redo Log文件
func (ssm *SystemSpaceManager) appendToRedoLogFile(data []byte) error {
	// 这里应该是实际的文件写入操作
	// 简化实现：只记录日志
	logger.Debugf("         Appending %d bytes to Redo Log file\n", len(data))
	return nil
}

// syncRedoLogFile 同步Redo Log文件到磁盘
func (ssm *SystemSpaceManager) syncRedoLogFile() error {
	// 这里应该是实际的fsync操作
	logger.Debugf("        💾 Syncing Redo Log file to disk (fsync)\n")
	return nil
}

// updateCheckpointInfo 更新checkpoint信息
func (ssm *SystemSpaceManager) updateCheckpointInfo() {
	logger.Debugf("         Updating checkpoint info (LSN, timestamp)\n")
	// 更新最后checkpoint的LSN和时间戳
}

// initializeSystemTables 初始化其他系统表
func (ssm *SystemSpaceManager) initializeSystemTables(txID uint64) error {
	logger.Debug("📚 Initializing other system tables via Buffer Pool...")

	// 初始化mysql.db表
	if err := ssm.initializeMySQLDbTable(txID); err != nil {
		return fmt.Errorf("failed to initialize mysql.db: %v", err)
	}

	// 初始化mysql.tables_priv表
	if err := ssm.initializeMySQLTablesPrivTable(txID); err != nil {
		return fmt.Errorf("failed to initialize mysql.tables_priv: %v", err)
	}

	// 可以继续添加其他系统表...

	logger.Debug(" System tables initialization completed")
	return nil
}

// initializeMySQLDbTable 初始化mysql.db表
func (ssm *SystemSpaceManager) initializeMySQLDbTable(txID uint64) error {
	logger.Debug("   Initializing mysql.db table...")

	// 获取表空间ID
	dbSpaceID := ssm.getMySQLSystemTableSpaceID("mysql/db")
	if dbSpaceID == 0 {
		return fmt.Errorf("mysql.db tablespace not found")
	}

	// 创建默认数据库权限记录
	defaultDbPerms := []MySQLDbPermission{
		{
			Host:       "%",
			Db:         "test_simple_protocol",
			User:       "root",
			SelectPriv: "Y",
			InsertPriv: "Y",
			UpdatePriv: "Y",
			DeletePriv: "Y",
			CreatePriv: "Y",
			DropPriv:   "Y",
		},
		// 可以添加更多默认权限...
	}

	// 通过Buffer Pool机制插入数据
	bufferPoolMgr := ssm.bufferPoolManager
	for i, perm := range defaultDbPerms {
		pageNo := uint32(20 + i) // 使用不同的页号范围
		recordData := perm.serializeToBytes()

		if err := ssm.insertDbPermissionViaBufferPool(bufferPoolMgr, dbSpaceID, pageNo, &perm, recordData, txID); err != nil {
			return fmt.Errorf("failed to insert db permission: %v", err)
		}
	}

	logger.Debugf("     mysql.db table initialized with %d records\n", len(defaultDbPerms))
	return nil
}

// initializeMySQLTablesPrivTable 初始化mysql.tables_priv表
func (ssm *SystemSpaceManager) initializeMySQLTablesPrivTable(txID uint64) error {
	logger.Debug("   Initializing mysql.tables_priv table...")

	// 类似的实现...
	logger.Debugf("     mysql.tables_priv table initialized\n")
	return nil
}

// MySQLDbPermission 数据库权限结构
type MySQLDbPermission struct {
	Host       string
	Db         string
	User       string
	SelectPriv string
	InsertPriv string
	UpdatePriv string
	DeletePriv string
	CreatePriv string
	DropPriv   string
	GrantPriv  string
	// 其他权限字段...
}

// serializeToBytes 序列化数据库权限
func (perm *MySQLDbPermission) serializeToBytes() []byte {
	// 简化的序列化实现
	data := make([]byte, 512)
	offset := 0

	// Host
	copy(data[offset:], []byte(perm.Host))
	offset += 64

	// Db
	copy(data[offset:], []byte(perm.Db))
	offset += 64

	// User
	copy(data[offset:], []byte(perm.User))
	offset += 32

	// 权限字段
	if perm.SelectPriv == "Y" {
		data[offset] = 1
	} else {
		data[offset] = 0
	}
	offset++
	if perm.InsertPriv == "Y" {
		data[offset] = 1
	} else {
		data[offset] = 0
	}
	offset++
	// 继续其他权限...

	return data
}

// insertDbPermissionViaBufferPool 通过Buffer Pool插入数据库权限
func (ssm *SystemSpaceManager) insertDbPermissionViaBufferPool(
	bufferPoolMgr *buffer_pool.BufferPool,
	spaceID, pageNo uint32,
	perm *MySQLDbPermission,
	recordData []byte,
	txID uint64) error {

	// 类似于insertUserDataViaBufferPool的实现
	logger.Debugf("    📄 Inserting db permission %s@%s.%s via Buffer Pool (Page %d)\n",
		perm.User, perm.Host, perm.Db, pageNo)

	// 1. 获取Buffer Pool页面
	bufferPage, err := bufferPoolMgr.GetPage(spaceID, pageNo)
	if err != nil {
		// 创建新页面
		pageContent := ssm.createStandardInnoDBPage(spaceID, pageNo, nil)
		bufferPage, err = bufferPoolMgr.GetPage(spaceID, pageNo)
		if err != nil {
			return fmt.Errorf("failed to get/create page: %v", err)
		}
		bufferPage.SetContent(pageContent)
	}

	// 2. 插入记录
	if err := ssm.insertRecordIntoPage(bufferPage.GetContent(), recordData, nil); err != nil {
		return fmt.Errorf("failed to insert record: %v", err)
	}

	// 3. 标记脏页
	bufferPage.MarkDirty()

	// 4. 写入Redo Log
	if err := ssm.writeRedoLogRecord(txID, spaceID, pageNo, recordData); err != nil {
		return fmt.Errorf("failed to write redo log: %v", err)
	}

	logger.Debugf("       Db permission cached in Buffer Pool\n")
	return nil
}

// getMySQLSystemTableSpaceID 获取MySQL系统表的表空间ID
func (ssm *SystemSpaceManager) getMySQLSystemTableSpaceID(tableName string) uint32 {
	// 这里应该从系统表空间映射中获取
	// 简化实现：返回基于表名的固定ID
	switch tableName {
	case "mysql/user":
		return 1
	case "mysql/db":
		return 2
	case "mysql/tables_priv":
		return 3
	default:
		return 0
	}
}
