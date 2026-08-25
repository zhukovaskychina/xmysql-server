package manager

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/logger"
	"strconv"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/page"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/types"
)

// 系统表空间常量 - 使用不同的前缀避免重复定义
const (
	SysSpaceID     = 0   // 系统表空间ID
	DictRootPageNo = 5   // 数据字典根页面号
	DictHeaderSize = 64  // 数据字典头部大小
	MaxDictEntries = 100 // 每页最大字典条目数
)

// DictionaryManager 数据字典管理器
type DictionaryManager struct {
	mu sync.RWMutex

	// 表定义映射: table_id -> table
	tables map[uint64]*TableDef

	// 表空间映射: space_id -> tables
	tableSpaces map[uint32][]uint64

	// 段管理器
	segmentManager *SegmentManager

	// 缓冲池管理器
	bufferPoolManager *OptimizedBufferPoolManager

	// 数据字典页面包装器 - 使用现有的存储层组件
	dictPageWrapper *page.DataDictionaryPageWrapper

	// 底层数据字典页面 - 使用现有的页面实现
	dictPage *pages.DataDictionaryHeaderSysPage

	// 系统表B+树索引
	sysTablesIndex  *DictBTreeIndex // SYS_TABLES索引
	sysColumnsIndex *DictBTreeIndex // SYS_COLUMNS索引
	sysIndexesIndex *DictBTreeIndex // SYS_INDEXES索引

	// 统计信息
	stats *DictStats
}

// DictRootPage 数据字典根页面
type DictRootPage struct {
	// 页面头部
	PageHeader DictPageHeader

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
	IndexesSegmentID uint32 // 索引段ID
	ColumnsSegmentID uint32 // 列段ID

	// 版本和校验信息
	Version   uint32 // 数据字典版本
	Checksum  uint32 // 校验和
	Timestamp int64  // 最后更新时间
}

// DictPageHeader 数据字典页面头部
type DictPageHeader struct {
	PageType   uint8  // 页面类型
	EntryCount uint16 // 条目数量
	FreeSpace  uint16 // 空闲空间
	NextPageNo uint32 // 下一页页号
	PrevPageNo uint32 // 上一页页号
	LSN        uint64 // 日志序列号
}

// DictBTreeIndex 数据字典B+树索引
type DictBTreeIndex struct {
	IndexID    uint64   // 索引ID
	RootPageNo uint32   // 根页面号
	SegmentID  uint32   // 段ID
	IndexType  uint8    // 索引类型
	KeyColumns []string // 索引列
}

// DictStats 数据字典统计信息
type DictStats struct {
	TotalTables  uint64 // 总表数
	TotalIndexes uint64 // 总索引数
	TotalColumns uint64 // 总列数
	LastUpdate   int64  // 最后更新时间
	CacheHits    uint64 // 缓存命中数
	CacheMisses  uint64 // 缓存未命中数
}

// TableDef 表示表定义
type TableDef struct {
	TableID     uint64       // 表ID
	Name        string       // 表名
	SpaceID     uint32       // 表空间ID
	Columns     []ColumnDef  // 列定义
	Indexes     []IndexDef   // 索引定义
	PrimaryKey  *IndexDef    // 主键索引
	ForeignKeys []ForeignKey // 外键约束
	SegmentID   uint32       // 数据段ID
	AutoIncr    uint64       // 自增值
	CreateTime  int64        // 创建时间
	UpdateTime  int64        // 更新时间
	RowFormat   uint8        // 行格式
	Charset     string       // 字符集
	Collation   string       // 排序规则
	Comment     string       // 表注释
}

// ColumnDef 表示列定义
type ColumnDef struct {
	ColumnID     uint64 // 列ID
	Name         string // 列名
	Type         uint8  // 数据类型
	Length       uint16 // 长度
	Precision    uint8  // 精度
	Scale        uint8  // 小数位数
	Nullable     bool   // 是否可空
	DefaultValue []byte // 默认值
	OnUpdate     string // ON UPDATE
	Comment      string // 注释
	Position     uint16 // 列位置
}

// IndexDef 表示索引定义
type IndexDef struct {
	IndexID    uint64   // 索引ID
	Name       string   // 索引名
	TableID    uint64   // 所属表ID
	Type       uint8    // 索引类型
	Columns    []string // 索引列
	IsUnique   bool     // 是否唯一
	IsPrimary  bool     // 是否主键
	RootPageNo uint32   // 根页面号
	SegmentID  uint32   // 段ID
	Comment    string   // 注释
}

// ForeignKey 表示外键约束
type ForeignKey struct {
	Name       string   // 约束名
	Columns    []string // 本表列
	RefTable   string   // 引用表
	RefColumns []string // 引用列
	OnDelete   string   // ON DELETE
	OnUpdate   string   // ON UPDATE
}

// NewDictionaryManager 创建数据字典管理器
func NewDictionaryManager(segmentManager *SegmentManager, bufferPoolManager *OptimizedBufferPoolManager) *DictionaryManager {
	dm := &DictionaryManager{
		tables:            make(map[uint64]*TableDef),
		tableSpaces:       make(map[uint32][]uint64),
		segmentManager:    segmentManager,
		bufferPoolManager: bufferPoolManager,
		stats:             &DictStats{},
	}

	// 初始化数据字典
	if err := dm.initialize(); err != nil {
		// 记录错误但不阻止创建
		logger.Debugf("Warning: Failed to initialize dictionary: %v", err)
	}

	return dm
}

// initialize 初始化数据字典
func (dm *DictionaryManager) initialize() error {
	// 创建数据字典页面包装器
	var bufferPool *buffer_pool.BufferPool
	if dm.bufferPoolManager != nil {
		bufferPool = dm.getBufferPool()
	}

	dm.dictPageWrapper = page.NewDataDictionaryPageWrapper(DictRootPageNo, SysSpaceID, bufferPool)
	if dm.bufferPoolManager != nil {
		dm.dictPageWrapper.SetStorageProvider(dm.bufferPoolManager.storage)
	}

	// 尝试加载现有的数据字典根页面
	if err := dm.loadRootPage(); err != nil {
		// 如果加载失败，创建新的数据字典
		return dm.createNewDictionary()
	}

	// 加载系统表索引
	return dm.loadSystemIndexes()
}

// getBufferPool 获取底层BufferPool - 这个方法需要根据实际的BufferPoolManager接口实现
func (dm *DictionaryManager) getBufferPool() *buffer_pool.BufferPool {
	// 当前管理器持有的是 OptimizedBufferPoolManager，未在该类型中暴露原始 BufferPool 实例。
	// 若后续统一为统一接口，可在此返回底层 BufferPool。当前阶段返回 nil 即可降级走 storage 读写。
	return nil
}

// loadRootPage 加载数据字典根页面
func (dm *DictionaryManager) loadRootPage() error {
	if dm.dictPageWrapper == nil {
		return fmt.Errorf("dict page wrapper not initialized")
	}

	// 使用页面包装器读取数据
	if err := dm.dictPageWrapper.Read(); err != nil {
		return fmt.Errorf("failed to read dict root page: %v", err)
	}

	// 获取底层的数据字典页面
	dm.dictPage = dm.dictPageWrapper.GetDataDictPage()
	if dm.dictPage == nil {
		return fmt.Errorf("failed to get dict page from wrapper")
	}

	if err := dm.rebuildCacheFromWrapper(); err != nil {
		return fmt.Errorf("failed to rebuild dict cache: %v", err)
	}

	return nil
}

// rebuildCacheFromWrapper 根据页面包装器中的定义，重建内存表/表空间映射
func (dm *DictionaryManager) rebuildCacheFromWrapper() error {
	if dm.dictPageWrapper == nil {
		return fmt.Errorf("dict page wrapper not initialized")
	}

	tableDefs, err := dm.dictPageWrapper.ListTableDefs()
	if err != nil {
		return err
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()

	dm.tables = make(map[uint64]*TableDef)
	dm.tableSpaces = make(map[uint32][]uint64)
	dm.stats.TotalTables = 0
	dm.stats.TotalIndexes = 0

	for _, tableDef := range tableDefs {
		converted := convertPageTableDefToManager(tableDef)
		if converted == nil {
			continue
		}

		dm.tables[converted.TableID] = converted
		dm.tableSpaces[converted.SpaceID] = append(dm.tableSpaces[converted.SpaceID], converted.TableID)
		dm.stats.TotalTables++
		dm.stats.TotalIndexes += uint64(len(converted.Indexes))
	}

	return nil
}

// createNewDictionary 创建新的数据字典
func (dm *DictionaryManager) createNewDictionary() error {
	// 创建新的数据字典页面
	dm.dictPage = pages.NewDataDictHeaderPage()

	// 创建系统表段
	if err := dm.createSystemSegments(); err != nil {
		return fmt.Errorf("failed to create system segments: %v", err)
	}

	// 创建系统表B+树索引
	if err := dm.createSystemIndexes(); err != nil {
		return fmt.Errorf("failed to create system indexes: %v", err)
	}

	// 保存根页面
	return dm.saveRootPage()
}

// createSystemSegments 创建系统表段
func (dm *DictionaryManager) createSystemSegments() error {
	if dm.segmentManager == nil {
		return fmt.Errorf("segment manager not available")
	}

	// 创建表段
	tablesSegment, err := dm.segmentManager.CreateSegment(SysSpaceID, SEGMENT_TYPE_DATA, false)
	if err != nil {
		return fmt.Errorf("failed to create tables segment: %v", err)
	}

	// 创建索引段
	indexesSegment, err := dm.segmentManager.CreateSegment(SysSpaceID, SEGMENT_TYPE_INDEX, false)
	if err != nil {
		return fmt.Errorf("failed to create indexes segment: %v", err)
	}

	// 创建列段
	columnsSegment, err := dm.segmentManager.CreateSegment(SysSpaceID, SEGMENT_TYPE_DATA, false)
	if err != nil {
		return fmt.Errorf("failed to create columns segment: %v", err)
	}

	// 这里可以保存段ID到数据字典页面
	// 具体实现取决于pages.DataDictionaryHeaderSysPage的接口
	_ = tablesSegment
	_ = indexesSegment
	_ = columnsSegment

	return nil
}

// createSystemIndexes 创建系统表B+树索引
func (dm *DictionaryManager) createSystemIndexes() error {
	// 分配根页面
	sysTablesRootPage, err := dm.allocateNewPage()
	if err != nil {
		return fmt.Errorf("failed to allocate sys_tables root page: %v", err)
	}

	sysColumnsRootPage, err := dm.allocateNewPage()
	if err != nil {
		return fmt.Errorf("failed to allocate sys_columns root page: %v", err)
	}

	sysIndexesRootPage, err := dm.allocateNewPage()
	if err != nil {
		return fmt.Errorf("failed to allocate sys_indexes root page: %v", err)
	}

	// 创建B+树索引对象
	dm.sysTablesIndex = &DictBTreeIndex{
		IndexID:    1,
		RootPageNo: sysTablesRootPage,
		IndexType:  1, // 聚簇索引
		KeyColumns: []string{"table_id"},
	}

	dm.sysColumnsIndex = &DictBTreeIndex{
		IndexID:    2,
		RootPageNo: sysColumnsRootPage,
		IndexType:  1,
		KeyColumns: []string{"table_id", "column_id"},
	}

	dm.sysIndexesIndex = &DictBTreeIndex{
		IndexID:    3,
		RootPageNo: sysIndexesRootPage,
		IndexType:  1,
		KeyColumns: []string{"index_id"},
	}

	return nil
}

// allocateNewPage 分配新页面
func (dm *DictionaryManager) allocateNewPage() (uint32, error) {
	if dm.segmentManager == nil {
		return 0, fmt.Errorf("segment manager not available")
	}

	// 这里需要一个默认的段ID来分配页面
	// 实际实现中应该使用系统表段的ID
	pageNo, err := dm.segmentManager.AllocatePage(1) // 使用段ID 1作为默认
	if err != nil {
		return 0, fmt.Errorf("failed to allocate page: %v", err)
	}

	return pageNo, nil
}

// loadSystemIndexes 加载系统表索引
func (dm *DictionaryManager) loadSystemIndexes() error {
	// 从数据字典页面加载系统表索引信息
	// 这里需要根据实际的页面结构来实现

	dm.sysTablesIndex = &DictBTreeIndex{
		IndexID:    1,
		IndexType:  1,
		KeyColumns: []string{"table_id"},
	}

	dm.sysColumnsIndex = &DictBTreeIndex{
		IndexID:    2,
		IndexType:  1,
		KeyColumns: []string{"table_id", "column_id"},
	}

	dm.sysIndexesIndex = &DictBTreeIndex{
		IndexID:    3,
		IndexType:  1,
		KeyColumns: []string{"index_id"},
	}

	return nil
}

// saveRootPage 保存根页面
func (dm *DictionaryManager) saveRootPage() error {
	if dm.dictPageWrapper == nil {
		return fmt.Errorf("dict page wrapper not initialized")
	}

	// 使用页面包装器写入数据
	return dm.dictPageWrapper.Write()
}

// CreateTable 创建新表
func (dm *DictionaryManager) CreateTable(name string, spaceID uint32, cols []ColumnDef) (*TableDef, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// 生成新的表ID
	var tableID uint64
	if dm.dictPage != nil {
		tableID = dm.dictPage.GetMaxTableId() + 1
		dm.dictPage.SetMaxTableId(tableID)
	} else {
		tableID = uint64(len(dm.tables)) + 1
	}

	// 为表创建数据段
	seg, err := dm.segmentManager.CreateSegment(spaceID, SEGMENT_TYPE_DATA, false)
	if err != nil {
		return nil, err
	}

	// 获取段ID
	var segmentID uint32
	if segImpl, ok := seg.(*SegmentImpl); ok {
		segmentID = segImpl.SegmentID
	}

	// 创建表定义
	table := &TableDef{
		TableID:    tableID,
		Name:       name,
		SpaceID:    spaceID,
		Columns:    cols,
		SegmentID:  segmentID,
		CreateTime: time.Now().Unix(),
		UpdateTime: time.Now().Unix(),
		RowFormat:  1, // 默认行格式
		Charset:    "utf8mb4",
		Collation:  "utf8mb4_general_ci",
	}

	// 保存表定义到内存
	dm.tables[tableID] = table
	dm.tableSpaces[spaceID] = append(dm.tableSpaces[spaceID], tableID)

	// 持久化到系统表
	if err := dm.persistTableDef(table); err != nil {
		// 回滚内存操作
		delete(dm.tables, tableID)
		if dm.dictPage != nil {
			dm.dictPage.SetMaxTableId(tableID - 1)
		}
		return nil, fmt.Errorf("failed to persist table definition: %v", err)
	}

	// 更新统计信息
	dm.stats.TotalTables++
	dm.stats.LastUpdate = time.Now().Unix()

	// 保存根页面
	dm.saveRootPage()

	return table, nil
}

// persistTableDef 持久化表定义到系统表
func (dm *DictionaryManager) persistTableDef(table *TableDef) error {
	// 使用页面包装器添加表定义
	if dm.dictPageWrapper != nil {
		// 转换列定义
		var columns []*page.ColumnDef
		for _, col := range table.Columns {
			columns = append(columns, &page.ColumnDef{
				ID:       col.ColumnID,
				Name:     col.Name,
				Type:     basic.ValueType(col.Type),
				Nullable: col.Nullable,
				Comment:  col.Comment,
				MaxLen:   uint32(col.Length),
			})
		}

		// 转换索引定义
		var indexes []*page.IndexDef
		for _, idx := range table.Indexes {
			indexes = append(indexes, &page.IndexDef{
				ID:       idx.IndexID,
				Name:     idx.Name,
				Columns:  idx.Columns,
				Unique:   idx.IsUnique,
				Primary:  idx.IsPrimary,
				RootPage: idx.RootPageNo,
				Comment:  idx.Comment,
			})
		}

		tableDefForWrapper := page.TableDef{
			ID:      table.TableID,
			Name:    table.Name,
			Columns: columns,
			Indexes: indexes,
			Properties: map[string]string{
				"space_id":   fmt.Sprintf("%d", table.SpaceID),
				"segment_id": fmt.Sprintf("%d", table.SegmentID),
				"charset":    table.Charset,
				"collation":  table.Collation,
				"comment":    table.Comment,
				"row_format": fmt.Sprintf("%d", table.RowFormat),
			},
		}
		err := dm.dictPageWrapper.AddTableDef(&tableDefForWrapper)
		if err == nil {
			dm.saveRootPage()
		}
		return err
	}
	return nil
}

// GetTable 获取表定义
func (dm *DictionaryManager) GetTable(tableID uint64) *TableDef {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if table, exists := dm.tables[tableID]; exists {
		dm.stats.CacheHits++
		return table
	}

	dm.stats.CacheMisses++
	if dm.dictPageWrapper == nil {
		return nil
	}

	tableDef, err := dm.dictPageWrapper.GetTableDef(tableID)
	if err != nil || tableDef == nil {
		return nil
	}

	managerTable := convertPageTableDefToManager(tableDef)
	if managerTable == nil {
		return nil
	}

	dm.tables[managerTable.TableID] = managerTable
	dm.tableSpaces[managerTable.SpaceID] = append(dm.tableSpaces[managerTable.SpaceID], managerTable.TableID)
	dm.stats.TotalTables++
	dm.stats.TotalIndexes += uint64(len(managerTable.Indexes))
	return managerTable
}

// GetTableByName 根据表名获取表定义
func (dm *DictionaryManager) GetTableByName(name string) *TableDef {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	for _, table := range dm.tables {
		if table.Name == name {
			dm.stats.CacheHits++
			return table
		}
	}

	dm.stats.CacheMisses++
	if dm.dictPageWrapper == nil {
		return nil
	}

	tableDefs, err := dm.dictPageWrapper.ListTableDefs()
	if err != nil {
		return nil
	}

	for _, tableDef := range tableDefs {
		if tableDef == nil || tableDef.Name != name {
			continue
		}

		managerTable := convertPageTableDefToManager(tableDef)
		if managerTable == nil {
			return nil
		}

		dm.tables[managerTable.TableID] = managerTable
		dm.tableSpaces[managerTable.SpaceID] = append(dm.tableSpaces[managerTable.SpaceID], managerTable.TableID)
		dm.stats.TotalTables++
		dm.stats.TotalIndexes += uint64(len(managerTable.Indexes))
		return managerTable
	}

	return nil
}

// GetStats 获取统计信息
func (dm *DictionaryManager) GetStats() *DictStats {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	// 返回统计信息的副本
	stats := *dm.stats
	return &stats
}

// GetDictPage 获取数据字典页面
func (dm *DictionaryManager) GetDictPage() *pages.DataDictionaryHeaderSysPage {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.dictPage
}

// GetDictPageWrapper 获取数据字典页面包装器
func (dm *DictionaryManager) GetDictPageWrapper() *page.DataDictionaryPageWrapper {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.dictPageWrapper
}

// GetDataDictWrapper 获取数据字典包装器 - 兼容现有接口
func (dm *DictionaryManager) GetDataDictWrapper() *types.DataDictWrapper {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if dm.dictPage == nil {
		return &types.DataDictWrapper{}
	}

	return &types.DataDictWrapper{
		MaxTableID: dm.dictPage.GetMaxTableId(),
		MaxIndexID: dm.dictPage.GetMaxIndexId(),
		MaxSpaceID: uint32(dm.dictPage.GetMaxSpaceId()), // 类型转换
		MaxRowID:   dm.dictPage.GetMaxRowId(),
	}
}

// Close 关闭数据字典管理器
func (dm *DictionaryManager) Close() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// 保存根页面
	if err := dm.saveRootPage(); err != nil {
		return fmt.Errorf("failed to save root page: %v", err)
	}

	// 清理资源
	dm.tables = nil
	dm.tableSpaces = nil
	dm.dictPage = nil
	dm.dictPageWrapper = nil
	dm.sysTablesIndex = nil
	dm.sysColumnsIndex = nil
	dm.sysIndexesIndex = nil

	return nil
}

// AddIndex 添加索引
func (dm *DictionaryManager) AddIndex(tableID uint64, index IndexDef) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	table := dm.tables[tableID]
	if table == nil {
		return ErrTableNotFound
	}

	// 生成新的索引ID
	var indexID uint64
	if dm.dictPage != nil {
		indexID = dm.dictPage.GetMaxIndexId() + 1
		dm.dictPage.SetMaxIndexId(indexID)
	} else {
		indexID = uint64(len(table.Indexes)) + 1
	}

	index.IndexID = indexID
	index.TableID = tableID

	// 检查索引名是否已存在
	for _, idx := range table.Indexes {
		if idx.Name == index.Name {
			if dm.dictPage != nil {
				dm.dictPage.SetMaxIndexId(indexID - 1) // 回滚ID
			}
			return ErrIndexExists
		}
	}

	// 为索引创建段
	seg, err := dm.segmentManager.CreateSegment(table.SpaceID, SEGMENT_TYPE_INDEX, false)
	if err != nil {
		if dm.dictPage != nil {
			dm.dictPage.SetMaxIndexId(indexID - 1) // 回滚ID
		}
		return err
	}

	if segImpl, ok := seg.(*SegmentImpl); ok {
		index.SegmentID = segImpl.SegmentID
	}

	// 分配根页面
	rootPageNo, err := dm.allocateNewPage()
	if err != nil {
		if dm.dictPage != nil {
			dm.dictPage.SetMaxIndexId(indexID - 1) // 回滚ID
		}
		return err
	}
	index.RootPageNo = rootPageNo

	// 添加索引定义
	table.Indexes = append(table.Indexes, index)

	// 如果是主键索引，更新表的主键引用
	if index.IsPrimary {
		table.PrimaryKey = &index
	}

	// 持久化索引定义
	if err := dm.persistIndexDef(&index); err != nil {
		// 回滚操作
		table.Indexes = table.Indexes[:len(table.Indexes)-1]
		if index.IsPrimary {
			table.PrimaryKey = nil
		}
		if dm.dictPage != nil {
			dm.dictPage.SetMaxIndexId(indexID - 1)
		}
		return fmt.Errorf("failed to persist index definition: %v", err)
	}

	// 更新统计信息
	dm.stats.TotalIndexes++
	dm.stats.LastUpdate = time.Now().Unix()

	// 保存根页面
	dm.saveRootPage()

	return nil
}

// persistIndexDef 持久化索引定义
func (dm *DictionaryManager) persistIndexDef(index *IndexDef) error {
	// 使用页面包装器添加索引定义
	if dm.dictPageWrapper != nil {
		indexDefForWrapper := page.IndexDef{
			ID:       index.IndexID,
			Name:     index.Name,
			Columns:  index.Columns,
			Unique:   index.IsUnique,
			Primary:  index.IsPrimary,
			RootPage: index.RootPageNo,
			Comment:  index.Comment,
		}
		return dm.dictPageWrapper.AddIndex(&indexDefForWrapper)
	}
	return nil
}

// DropTable 删除表
func (dm *DictionaryManager) DropTable(tableID uint64) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	table := dm.tables[tableID]
	if table == nil {
		return ErrTableNotFound
	}

	// 删除表的数据段
	if err := dm.segmentManager.DropSegment(table.SegmentID); err != nil {
		return err
	}

	// 删除表的所有索引段
	for _, index := range table.Indexes {
		if err := dm.segmentManager.DropSegment(index.SegmentID); err != nil {
			return err
		}
	}

	// 从表空间映射中删除
	spaceID := table.SpaceID
	tables := dm.tableSpaces[spaceID]
	for i, id := range tables {
		if id == tableID {
			dm.tableSpaces[spaceID] = append(tables[:i], tables[i+1:]...)
			break
		}
	}

	// 删除表定义
	if dm.dictPageWrapper != nil {
		if err := dm.dictPageWrapper.RemoveTableDef(tableID); err != nil {
			return err
		}
	}
	delete(dm.tables, tableID)

	// 更新统计信息
	if dm.stats.TotalTables > 0 {
		dm.stats.TotalTables--
	}
	dm.stats.TotalIndexes -= uint64(len(table.Indexes))
	dm.stats.LastUpdate = time.Now().Unix()

	// 保存根页面
	dm.saveRootPage()

	return nil
}

func convertPageTableDefToManager(tableDef *page.TableDef) *TableDef {
	if tableDef == nil {
		return nil
	}

	managerTable := &TableDef{
		TableID:    tableDef.ID,
		Name:       tableDef.Name,
		SpaceID:    parseUint32FromProperty(tableDef.Properties, "space_id", 0),
		SegmentID:  parseUint32FromProperty(tableDef.Properties, "segment_id", 0),
		Charset:    tableDef.Properties["charset"],
		Collation:  tableDef.Properties["collation"],
		Comment:    tableDef.Properties["comment"],
		Columns:    make([]ColumnDef, 0, len(tableDef.Columns)),
		Indexes:    make([]IndexDef, 0, len(tableDef.Indexes)),
		CreateTime: time.Now().Unix(),
		UpdateTime: time.Now().Unix(),
		RowFormat:  1,
	}

	if managerTable.Charset == "" {
		managerTable.Charset = "utf8mb4"
	}

	if managerTable.Collation == "" {
		managerTable.Collation = "utf8mb4_general_ci"
	}

	if rowFormat := parseUint32FromProperty(tableDef.Properties, "row_format", uint32(managerTable.RowFormat)); rowFormat != 0 {
		managerTable.RowFormat = uint8(rowFormat)
	}

	for _, col := range tableDef.Columns {
		if col == nil {
			continue
		}

		colDef := ColumnDef{
			ColumnID: col.ID,
			Name:     col.Name,
			Type:     uint8(col.Type),
			Length:   uint16(col.MaxLen),
			Nullable: col.Nullable,
			Comment:  col.Comment,
		}
		managerTable.Columns = append(managerTable.Columns, colDef)
	}

	for _, idx := range tableDef.Indexes {
		if idx == nil {
			continue
		}

		indexDef := IndexDef{
			IndexID:    idx.ID,
			Name:       idx.Name,
			TableID:    tableDef.ID,
			Columns:    append([]string{}, idx.Columns...),
			IsUnique:   idx.Unique,
			IsPrimary:  idx.Primary,
			RootPageNo: idx.RootPage,
		}
		managerTable.Indexes = append(managerTable.Indexes, indexDef)

		if idx.Primary {
			pk := indexDef
			managerTable.PrimaryKey = &pk
		}
	}

	return managerTable
}

func parseUint32FromProperty(props map[string]string, key string, defaultValue uint32) uint32 {
	if props == nil {
		return defaultValue
	}

	if raw, ok := props[key]; ok && raw != "" {
		if v, err := strconv.ParseUint(raw, 10, 32); err == nil {
			return uint32(v)
		}
	}

	return defaultValue
}
