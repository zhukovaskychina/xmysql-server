package manager

import (
	"sync"
	"time"
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
}

// ColumnDef 表示列定义
type ColumnDef struct {
	Name         string // 列名
	Type         uint8  // 数据类型
	Length       uint16 // 长度
	Nullable     bool   // 是否可空
	DefaultValue []byte // 默认值
	OnUpdate     string // ON UPDATE
	Comment      string // 注释
}

// IndexDef 表示索引定义
type IndexDef struct {
	IndexID   uint64   // 索引ID
	Name      string   // 索引名
	Type      uint8    // 索引类型
	Columns   []string // 索引列
	IsUnique  bool     // 是否唯一
	IsPrimary bool     // 是否主键
	Comment   string   // 注释
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
func NewDictionaryManager(segmentManager *SegmentManager) *DictionaryManager {
	return &DictionaryManager{
		tables:         make(map[uint64]*TableDef),
		tableSpaces:    make(map[uint32][]uint64),
		segmentManager: segmentManager,
	}
}

// CreateTable 创建新表
func (dm *DictionaryManager) CreateTable(name string, spaceID uint32, cols []ColumnDef) (*TableDef, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// 生成新的表ID
	tableID := uint64(len(dm.tables) + 1)

	// 为表创建数据段
	seg, err := dm.segmentManager.CreateSegment(spaceID, SEGMENT_TYPE_DATA, false)
	if err != nil {
		return nil, err
	}

	// 获取段ID（通过类型断言）
	var segmentID uint32
	if segImpl, ok := seg.(*SegmentImpl); ok {
		segmentID = segImpl.SegmentID
	} else {
		// 如果无法获取段ID，使用默认值
		segmentID = 0
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
	}

	// 保存表定义
	dm.tables[tableID] = table
	dm.tableSpaces[spaceID] = append(dm.tableSpaces[spaceID], tableID)

	return table, nil
}

// GetTable 获取表定义
func (dm *DictionaryManager) GetTable(tableID uint64) *TableDef {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.tables[tableID]
}

// GetTableByName 根据表名获取表定义
func (dm *DictionaryManager) GetTableByName(name string) *TableDef {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	for _, table := range dm.tables {
		if table.Name == name {
			return table
		}
	}
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

	// 检查索引名是否已存在
	for _, idx := range table.Indexes {
		if idx.Name == index.Name {
			return ErrIndexExists
		}
	}

	// 添加索引定义
	table.Indexes = append(table.Indexes, index)

	// 如果是主键索引，更新表的主键引用
	if index.IsPrimary {
		table.PrimaryKey = &index
	}

	return nil
}

// AddForeignKey 添加外键约束
func (dm *DictionaryManager) AddForeignKey(tableID uint64, fk ForeignKey) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	table := dm.tables[tableID]
	if table == nil {
		return ErrTableNotFound
	}

	// 检查外键名是否已存在
	for _, existingFK := range table.ForeignKeys {
		if existingFK.Name == fk.Name {
			return ErrForeignKeyExists
		}
	}

	// 检查引用表是否存在
	refTable := dm.GetTableByName(fk.RefTable)
	if refTable == nil {
		return ErrRefTableNotFound
	}

	// 添加外键约束
	table.ForeignKeys = append(table.ForeignKeys, fk)
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
	delete(dm.tables, tableID)
	return nil
}

// Close 关闭数据字典管理器
func (dm *DictionaryManager) Close() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// 清理资源
	dm.tables = nil
	dm.tableSpaces = nil
	return nil
}
