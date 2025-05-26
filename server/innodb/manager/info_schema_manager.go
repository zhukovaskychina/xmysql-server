package manager

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// InfoSchemaManager 管理INFORMATION_SCHEMA数据库
type InfoSchemaManager struct {
	mu sync.RWMutex

	// 表定义缓存
	tables map[string]*InfoSchemaTable

	// 统计信息
	stats InfoSchemaStats

	// 依赖的管理器
	dictManager  *DictionaryManager
	spaceManager basic.SpaceManager
	indexManager *IndexManager

	// Schema缓存
	schemaCache map[string]metadata.Schema

	// 表元数据缓存
	tableMetaCache map[string]*metadata.TableMeta

	// 表统计信息缓存
	tableStatsCache map[string]*metadata.InfoTableStats
}

// SimpleSchema 简单的Schema实现
type SimpleSchema struct {
	name        string
	description string
	tables      []*metadata.Table
}

func (s *SimpleSchema) GetName() string {
	return s.name
}

func (s *SimpleSchema) GetCharset() string {
	return "utf8mb4"
}

func (s *SimpleSchema) GetCollation() string {
	return "utf8mb4_general_ci"
}

func (s *SimpleSchema) GetTables() []*metadata.Table {
	return s.tables
}

func (im *InfoSchemaManager) GetSchemaByName(ctx context.Context, name string) (metadata.Schema, error) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	// INFORMATION_SCHEMA是一个虚拟数据库，不存储真实的用户schema
	if name == "INFORMATION_SCHEMA" {
		return &SimpleSchema{
			name:        "INFORMATION_SCHEMA",
			description: "Information Schema Virtual Database",
		}, nil
	}

	// 检查缓存
	if schema, exists := im.schemaCache[name]; exists {
		return schema, nil
	}

	// 从字典管理器获取schema
	if im.dictManager != nil {
		// 查找所有表，按schema分组
		tables := im.getAllTablesFromDict()
		schemaMap := make(map[string][]*metadata.Table)

		for _, table := range tables {
			// 假设表名格式为 "schema.table" 或者有其他方式确定schema
			// 这里简化处理，假设所有表都属于默认schema
			schemaName := "default"
			if strings.Contains(table.Name, ".") {
				parts := strings.Split(table.Name, ".")
				if len(parts) >= 2 {
					schemaName = parts[0]
					table.Name = parts[1]
				}
			}
			schemaMap[schemaName] = append(schemaMap[schemaName], table)
		}

		if schemaTables, exists := schemaMap[name]; exists {
			schema := &SimpleSchema{
				name:   name,
				tables: schemaTables,
			}
			im.schemaCache[name] = schema
			return schema, nil
		}
	}

	return nil, fmt.Errorf("schema '%s' not found", name)
}

func (im *InfoSchemaManager) HasSchema(ctx context.Context, name string) bool {
	if name == "INFORMATION_SCHEMA" {
		return true
	}

	// 检查缓存
	im.mu.RLock()
	_, exists := im.schemaCache[name]
	im.mu.RUnlock()

	if exists {
		return true
	}

	// 从字典管理器检查
	if im.dictManager != nil {
		tables := im.getAllTablesFromDict()
		for _, table := range tables {
			if strings.Contains(table.Name, ".") {
				parts := strings.Split(table.Name, ".")
				if len(parts) >= 2 && parts[0] == name {
					return true
				}
			}
		}
	}

	return false
}

func (im *InfoSchemaManager) GetAllSchemaNames(ctx context.Context) ([]string, error) {
	schemas := []string{"INFORMATION_SCHEMA"}

	// 从字典管理器获取用户schema
	if im.dictManager != nil {
		tables := im.getAllTablesFromDict()
		schemaSet := make(map[string]bool)

		for _, table := range tables {
			schemaName := "default"
			if strings.Contains(table.Name, ".") {
				parts := strings.Split(table.Name, ".")
				if len(parts) >= 2 {
					schemaName = parts[0]
				}
			}
			schemaSet[schemaName] = true
		}

		for schemaName := range schemaSet {
			schemas = append(schemas, schemaName)
		}
	}

	return schemas, nil
}

func (im *InfoSchemaManager) GetAllSchemas(ctx context.Context) ([]metadata.Schema, error) {
	schemas := []metadata.Schema{
		&SimpleSchema{
			name:        "INFORMATION_SCHEMA",
			description: "Information Schema Virtual Database",
		},
	}

	// 从字典管理器获取用户schema
	if im.dictManager != nil {
		tables := im.getAllTablesFromDict()
		schemaMap := make(map[string][]*metadata.Table)

		for _, table := range tables {
			schemaName := "default"
			if strings.Contains(table.Name, ".") {
				parts := strings.Split(table.Name, ".")
				if len(parts) >= 2 {
					schemaName = parts[0]
					table.Name = parts[1]
				}
			}
			schemaMap[schemaName] = append(schemaMap[schemaName], table)
		}

		for schemaName, schemaTables := range schemaMap {
			schema := &SimpleSchema{
				name:   schemaName,
				tables: schemaTables,
			}
			schemas = append(schemas, schema)
		}
	}

	return schemas, nil
}

func (im *InfoSchemaManager) CreateSchema(ctx context.Context, schema metadata.Schema) error {
	// 实现创建schema
	if schema.GetName() == "INFORMATION_SCHEMA" {
		return fmt.Errorf("cannot create INFORMATION_SCHEMA")
	}

	im.mu.Lock()
	defer im.mu.Unlock()

	// 检查schema是否已存在
	if _, exists := im.schemaCache[schema.GetName()]; exists {
		return fmt.Errorf("schema '%s' already exists", schema.GetName())
	}

	// 添加到缓存
	im.schemaCache[schema.GetName()] = schema

	return nil
}

func (im *InfoSchemaManager) DropSchema(ctx context.Context, name string) error {
	// INFORMATION_SCHEMA不允许删除
	if name == "INFORMATION_SCHEMA" {
		return fmt.Errorf("cannot drop INFORMATION_SCHEMA")
	}

	im.mu.Lock()
	defer im.mu.Unlock()

	// 检查schema是否存在
	if _, exists := im.schemaCache[name]; !exists {
		return fmt.Errorf("schema '%s' not found", name)
	}

	// 从缓存中删除
	delete(im.schemaCache, name)

	// 清理相关的表缓存
	for key := range im.tableMetaCache {
		if strings.HasPrefix(key, name+".") {
			delete(im.tableMetaCache, key)
		}
	}

	for key := range im.tableStatsCache {
		if strings.HasPrefix(key, name+".") {
			delete(im.tableStatsCache, key)
		}
	}

	return nil
}

func (im *InfoSchemaManager) GetTableByName(ctx context.Context, schemaName, tableName string) (*metadata.Table, error) {
	// 从字典管理器获取表
	if im.dictManager != nil {
		// 构造完整表名
		fullTableName := tableName
		if schemaName != "default" && schemaName != "" {
			fullTableName = schemaName + "." + tableName
		}

		// 查找表定义
		tableDef := im.dictManager.GetTableByName(fullTableName)
		if tableDef == nil {
			// 尝试直接用表名查找
			tableDef = im.dictManager.GetTableByName(tableName)
		}

		if tableDef != nil {
			return im.convertTableDefToMetadataTable(tableDef), nil
		}
	}

	return nil, fmt.Errorf("table '%s.%s' not found", schemaName, tableName)
}

func (im *InfoSchemaManager) HasTable(ctx context.Context, schemaName, tableName string) bool {
	table, err := im.GetTableByName(ctx, schemaName, tableName)
	return err == nil && table != nil
}

func (im *InfoSchemaManager) GetAllTables(ctx context.Context, schemaName string) ([]*metadata.Table, error) {
	var tables []*metadata.Table

	if im.dictManager != nil {
		allTables := im.getAllTablesFromDict()

		for _, table := range allTables {
			tableSchemaName := "default"
			tableName := table.Name

			if strings.Contains(table.Name, ".") {
				parts := strings.Split(table.Name, ".")
				if len(parts) >= 2 {
					tableSchemaName = parts[0]
					tableName = parts[1]
				}
			}

			if tableSchemaName == schemaName {
				table.Name = tableName
				tables = append(tables, table)
			}
		}
	}

	return tables, nil
}

func (im *InfoSchemaManager) CreateTable(ctx context.Context, schemaName string, table *metadata.Table) error {
	if im.dictManager == nil {
		return fmt.Errorf("dictionary manager not available")
	}

	// 转换metadata.Table到TableDef
	columns := make([]ColumnDef, len(table.Columns))
	for i, col := range table.Columns {
		columns[i] = ColumnDef{
			Name:         col.Name,
			Type:         im.convertDataTypeToMySQLType(col.DataType),
			Length:       uint16(col.CharMaxLength),
			Nullable:     col.IsNullable,
			DefaultValue: im.convertDefaultValue(col.DefaultValue),
			Comment:      col.Comment,
		}
	}

	// 创建表
	spaceID := uint32(1) // 默认表空间ID
	tableDef, err := im.dictManager.CreateTable(table.Name, spaceID, columns)
	if err != nil {
		return fmt.Errorf("create table failed: %v", err)
	}

	// 添加索引
	for _, index := range table.Indices {
		indexDef := IndexDef{
			IndexID:   uint64(len(tableDef.Indexes) + 1),
			Name:      index.Name,
			Type:      0, // BTREE
			Columns:   index.Columns,
			IsUnique:  index.IsUnique,
			IsPrimary: index.IsPrimary,
			Comment:   index.Comment,
		}

		if err := im.dictManager.AddIndex(tableDef.TableID, indexDef); err != nil {
			return fmt.Errorf("add index failed: %v", err)
		}
	}

	return nil
}

func (im *InfoSchemaManager) DropTable(ctx context.Context, schemaName, tableName string) error {
	if im.dictManager == nil {
		return fmt.Errorf("dictionary manager not available")
	}

	// 查找表
	fullTableName := tableName
	if schemaName != "default" && schemaName != "" {
		fullTableName = schemaName + "." + tableName
	}

	tableDef := im.dictManager.GetTableByName(fullTableName)
	if tableDef == nil {
		tableDef = im.dictManager.GetTableByName(tableName)
	}

	if tableDef == nil {
		return fmt.Errorf("table '%s.%s' not found", schemaName, tableName)
	}

	// 删除表
	if err := im.dictManager.DropTable(tableDef.TableID); err != nil {
		return fmt.Errorf("drop table failed: %v", err)
	}

	// 清理缓存
	cacheKey := fmt.Sprintf("%s.%s", schemaName, tableName)
	im.mu.Lock()
	delete(im.tableMetaCache, cacheKey)
	delete(im.tableStatsCache, cacheKey)
	im.mu.Unlock()

	return nil
}

func (im *InfoSchemaManager) RefreshMetadata(ctx context.Context, schemaName string) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	// 清理指定schema的缓存
	if schemaName == "" {
		// 清理所有缓存
		im.schemaCache = make(map[string]metadata.Schema)
		im.tableMetaCache = make(map[string]*metadata.TableMeta)
		im.tableStatsCache = make(map[string]*metadata.InfoTableStats)
	} else {
		// 清理指定schema的缓存
		delete(im.schemaCache, schemaName)

		for key := range im.tableMetaCache {
			if strings.HasPrefix(key, schemaName+".") {
				delete(im.tableMetaCache, key)
			}
		}

		for key := range im.tableStatsCache {
			if strings.HasPrefix(key, schemaName+".") {
				delete(im.tableStatsCache, key)
			}
		}
	}

	return nil
}

func (im *InfoSchemaManager) GetTableMetadata(ctx context.Context, schemaName, tableName string) (*metadata.TableMeta, error) {
	cacheKey := fmt.Sprintf("%s.%s", schemaName, tableName)

	// 检查缓存
	im.mu.RLock()
	if meta, exists := im.tableMetaCache[cacheKey]; exists {
		im.mu.RUnlock()
		return meta, nil
	}
	im.mu.RUnlock()

	// 从字典管理器获取表元数据
	table, err := im.GetTableByName(ctx, schemaName, tableName)
	if err != nil {
		return nil, err
	}

	// 转换为TableMeta
	meta := &metadata.TableMeta{
		Name:      table.Name,
		Engine:    table.Engine,
		Charset:   table.Charset,
		Collation: table.Collation,
		RowFormat: table.RowFormat,
		Comment:   table.Comment,
	}

	// 转换列信息
	for _, col := range table.Columns {
		colMeta := &metadata.ColumnMeta{
			Name:            col.Name,
			Type:            col.DataType,
			Length:          col.CharMaxLength,
			IsNullable:      col.IsNullable,
			IsPrimary:       false, // 需要从索引信息中确定
			IsUnique:        false, // 需要从索引信息中确定
			IsAutoIncrement: col.IsAutoIncrement,
			DefaultValue:    col.DefaultValue,
			Charset:         col.Charset,
			Collation:       col.Collation,
			Comment:         col.Comment,
		}
		meta.Columns = append(meta.Columns, colMeta)
	}

	// 转换索引信息并设置列的主键和唯一性标志
	for _, index := range table.Indices {
		indexMeta := metadata.IndexMeta{
			Name:    index.Name,
			Columns: index.Columns,
			Unique:  index.IsUnique,
		}
		meta.Indices = append(meta.Indices, indexMeta)

		// 设置列的主键和唯一性标志
		for _, colName := range index.Columns {
			for _, colMeta := range meta.Columns {
				if colMeta.Name == colName {
					if index.IsPrimary {
						colMeta.IsPrimary = true
					}
					if index.IsUnique {
						colMeta.IsUnique = true
					}
					break
				}
			}
		}
	}

	// 设置主键
	if table.PrimaryKey != nil {
		meta.PrimaryKey = table.PrimaryKey.Columns
	}

	// 缓存结果
	im.mu.Lock()
	im.tableMetaCache[cacheKey] = meta
	im.mu.Unlock()

	return meta, nil
}

func (im *InfoSchemaManager) GetTableStats(ctx context.Context, schemaName, tableName string) (*metadata.InfoTableStats, error) {
	cacheKey := fmt.Sprintf("%s.%s", schemaName, tableName)

	// 检查缓存
	im.mu.RLock()
	if stats, exists := im.tableStatsCache[cacheKey]; exists {
		im.mu.RUnlock()
		return stats, nil
	}
	im.mu.RUnlock()

	// 获取表定义
	table, err := im.GetTableByName(ctx, schemaName, tableName)
	if err != nil {
		return nil, err
	}

	// 创建默认统计信息
	stats := &metadata.InfoTableStats{
		RowCount:    0,
		AvgRowSize:  0,
		DataSize:    0,
		IndexSize:   0,
		ColumnStats: make(map[string]metadata.Stats),
		IndexStats:  make(map[string]metadata.Stats),
	}

	// 如果有表统计信息，使用实际值
	if table.Stats != nil {
		stats.RowCount = uint64(table.Stats.RowCount)
		stats.DataSize = uint64(table.Stats.DataLength)
		stats.IndexSize = uint64(table.Stats.IndexLength)
		if stats.RowCount > 0 {
			stats.AvgRowSize = uint32(stats.DataSize / stats.RowCount)
		}
	}

	// 缓存结果
	im.mu.Lock()
	im.tableStatsCache[cacheKey] = stats
	im.mu.Unlock()

	return stats, nil
}

func (im *InfoSchemaManager) UpdateTableStats(ctx context.Context, schemaName, tableName string, stats *metadata.InfoTableStats) error {
	cacheKey := fmt.Sprintf("%s.%s", schemaName, tableName)

	// 更新缓存
	im.mu.Lock()
	im.tableStatsCache[cacheKey] = stats
	im.mu.Unlock()

	// TODO: 将统计信息持久化到存储层

	return nil
}

func (im *InfoSchemaManager) DatabaseExists(name string) (bool, error) {
	// 检查数据库是否存在
	if name == "INFORMATION_SCHEMA" {
		return true, nil
	}

	// 从字典管理器检查
	if im.dictManager != nil {
		tables := im.getAllTablesFromDict()
		for _, table := range tables {
			if strings.Contains(table.Name, ".") {
				parts := strings.Split(table.Name, ".")
				if len(parts) >= 2 && parts[0] == name {
					return true, nil
				}
			}
		}
	}

	return false, nil
}

func (im *InfoSchemaManager) DropDatabase(name string) bool {
	if name == "INFORMATION_SCHEMA" {
		return false
	}

	// 删除数据库下的所有表
	if im.dictManager != nil {
		tables := im.getAllTablesFromDict()
		for _, table := range tables {
			if strings.Contains(table.Name, ".") {
				parts := strings.Split(table.Name, ".")
				if len(parts) >= 2 && parts[0] == name {
					// 删除表
					if tableDef := im.dictManager.GetTableByName(table.Name); tableDef != nil {
						im.dictManager.DropTable(tableDef.TableID)
					}
				}
			}
		}
	}

	// 清理缓存
	im.mu.Lock()
	delete(im.schemaCache, name)
	for key := range im.tableMetaCache {
		if strings.HasPrefix(key, name+".") {
			delete(im.tableMetaCache, key)
		}
	}
	for key := range im.tableStatsCache {
		if strings.HasPrefix(key, name+".") {
			delete(im.tableStatsCache, key)
		}
	}
	im.mu.Unlock()

	return true
}

// 辅助方法

// getAllTablesFromDict 从字典管理器获取所有表
func (im *InfoSchemaManager) getAllTablesFromDict() []*metadata.Table {
	var tables []*metadata.Table

	if im.dictManager != nil {
		im.dictManager.mu.RLock()
		for _, tableDef := range im.dictManager.tables {
			table := im.convertTableDefToMetadataTable(tableDef)
			tables = append(tables, table)
		}
		im.dictManager.mu.RUnlock()
	}

	return tables
}

// convertTableDefToMetadataTable 转换TableDef到metadata.Table
func (im *InfoSchemaManager) convertTableDefToMetadataTable(tableDef *TableDef) *metadata.Table {
	table := metadata.NewTable(tableDef.Name)
	table.Engine = "InnoDB"
	table.Charset = "utf8mb4"
	table.Collation = "utf8mb4_general_ci"
	table.RowFormat = "Dynamic"

	// 转换列
	for _, colDef := range tableDef.Columns {
		col := &metadata.Column{
			Name:          colDef.Name,
			DataType:      im.convertMySQLTypeToDataType(colDef.Type),
			CharMaxLength: int(colDef.Length),
			IsNullable:    colDef.Nullable,
			DefaultValue:  colDef.DefaultValue,
			Comment:       colDef.Comment,
		}
		table.AddColumn(col)
	}

	// 转换索引
	for _, indexDef := range tableDef.Indexes {
		index := &metadata.Index{
			Name:      indexDef.Name,
			Columns:   indexDef.Columns,
			IsUnique:  indexDef.IsUnique,
			IsPrimary: indexDef.IsPrimary,
			IndexType: "BTREE",
			Comment:   indexDef.Comment,
		}
		table.AddIndex(index)

		if indexDef.IsPrimary {
			table.PrimaryKey = index
		}
	}

	// 设置统计信息
	table.Stats = &metadata.TableStatistics{
		RowCount:      0,
		DataLength:    16384, // 默认页大小
		IndexLength:   0,
		TotalSize:     16384,
		AutoIncrement: int64(tableDef.AutoIncr),
		CreateTime:    time.Unix(tableDef.CreateTime, 0).Format("2006-01-02 15:04:05"),
		UpdateTime:    time.Unix(tableDef.UpdateTime, 0).Format("2006-01-02 15:04:05"),
	}

	return table
}

// convertDataTypeToMySQLType 转换DataType到MySQL类型
func (im *InfoSchemaManager) convertDataTypeToMySQLType(dataType metadata.DataType) uint8 {
	switch dataType {
	case metadata.TypeTinyInt:
		return MYSQL_TYPE_TINY
	case metadata.TypeSmallInt:
		return MYSQL_TYPE_SHORT
	case metadata.TypeInt:
		return MYSQL_TYPE_LONG
	case metadata.TypeBigInt:
		return MYSQL_TYPE_LONGLONG
	case metadata.TypeFloat:
		return MYSQL_TYPE_FLOAT
	case metadata.TypeDouble:
		return MYSQL_TYPE_DOUBLE
	case metadata.TypeDecimal:
		return MYSQL_TYPE_NEWDECIMAL
	case metadata.TypeDate:
		return MYSQL_TYPE_DATE
	case metadata.TypeTime:
		return MYSQL_TYPE_TIME
	case metadata.TypeDateTime:
		return MYSQL_TYPE_DATETIME
	case metadata.TypeTimestamp:
		return MYSQL_TYPE_TIMESTAMP
	case metadata.TypeYear:
		return MYSQL_TYPE_YEAR
	case metadata.TypeChar:
		return MYSQL_TYPE_STRING
	case metadata.TypeVarchar:
		return MYSQL_TYPE_VARCHAR
	case metadata.TypeText:
		return MYSQL_TYPE_BLOB
	case metadata.TypeJSON:
		return MYSQL_TYPE_JSON
	default:
		return MYSQL_TYPE_VARCHAR
	}
}

// convertMySQLTypeToDataType 转换MySQL类型到DataType
func (im *InfoSchemaManager) convertMySQLTypeToDataType(mysqlType uint8) metadata.DataType {
	switch mysqlType {
	case MYSQL_TYPE_TINY:
		return metadata.TypeTinyInt
	case MYSQL_TYPE_SHORT:
		return metadata.TypeSmallInt
	case MYSQL_TYPE_LONG:
		return metadata.TypeInt
	case MYSQL_TYPE_LONGLONG:
		return metadata.TypeBigInt
	case MYSQL_TYPE_FLOAT:
		return metadata.TypeFloat
	case MYSQL_TYPE_DOUBLE:
		return metadata.TypeDouble
	case MYSQL_TYPE_NEWDECIMAL:
		return metadata.TypeDecimal
	case MYSQL_TYPE_DATE:
		return metadata.TypeDate
	case MYSQL_TYPE_TIME:
		return metadata.TypeTime
	case MYSQL_TYPE_DATETIME:
		return metadata.TypeDateTime
	case MYSQL_TYPE_TIMESTAMP:
		return metadata.TypeTimestamp
	case MYSQL_TYPE_YEAR:
		return metadata.TypeYear
	case MYSQL_TYPE_STRING:
		return metadata.TypeChar
	case MYSQL_TYPE_VARCHAR:
		return metadata.TypeVarchar
	case MYSQL_TYPE_BLOB: // MYSQL_TYPE_BLOB 和 MYSQL_TYPE_LONGTEXT 都是 252
		return metadata.TypeText
	case MYSQL_TYPE_JSON:
		return metadata.TypeJSON
	default:
		return metadata.TypeVarchar
	}
}

// convertDefaultValue 转换默认值
func (im *InfoSchemaManager) convertDefaultValue(value interface{}) []byte {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case string:
		return []byte(v)
	case []byte:
		return v
	case int, int32, int64:
		return []byte(fmt.Sprintf("%v", v))
	case float32, float64:
		return []byte(fmt.Sprintf("%v", v))
	default:
		return []byte(fmt.Sprintf("%v", v))
	}
}

// InfoSchemaTable INFORMATION_SCHEMA表定义
type InfoSchemaTable struct {
	Name       string              // 表名
	Columns    []InfoSchemaColumn  // 列定义
	RowCount   uint64              // 行数
	UpdateTime time.Time           // 最后更新时间
	Generator  InfoSchemaGenerator // 数据生成器
}

// InfoSchemaColumn INFORMATION_SCHEMA列定义
type InfoSchemaColumn struct {
	Name     string // 列名
	Type     uint8  // 数据类型
	Length   uint16 // 长度
	Nullable bool   // 是否可空
}

// InfoSchemaStats INFORMATION_SCHEMA统计信息
type InfoSchemaStats struct {
	QueryCount  uint64    // 查询次数
	CacheHits   uint64    // 缓存命中次数
	CacheMisses uint64    // 缓存未命中次数
	LastRefresh time.Time // 最后刷新时间
}

// InfoSchemaGenerator 表数据生成器接口
type InfoSchemaGenerator interface {
	// Generate 生成表数据
	Generate() ([][]interface{}, error)
}

// NewInfoSchemaManager 创建INFORMATION_SCHEMA管理器
func NewInfoSchemaManager(dictManager *DictionaryManager, spaceManager basic.SpaceManager, indexManager *IndexManager) *InfoSchemaManager {
	return &InfoSchemaManager{
		tables:          make(map[string]*InfoSchemaTable),
		dictManager:     dictManager,
		spaceManager:    spaceManager,
		indexManager:    indexManager,
		schemaCache:     make(map[string]metadata.Schema),
		tableMetaCache:  make(map[string]*metadata.TableMeta),
		tableStatsCache: make(map[string]*metadata.InfoTableStats),
	}
}

// InitializeTables 初始化INFORMATION_SCHEMA表
func (im *InfoSchemaManager) InitializeTables() error {
	im.mu.Lock()
	defer im.mu.Unlock()

	// TABLES表
	im.tables["TABLES"] = &InfoSchemaTable{
		Name: "TABLES",
		Columns: []InfoSchemaColumn{
			{Name: "TABLE_CATALOG", Type: MYSQL_TYPE_VARCHAR, Length: 512, Nullable: true},
			{Name: "TABLE_SCHEMA", Type: MYSQL_TYPE_VARCHAR, Length: 64, Nullable: false},
			{Name: "TABLE_NAME", Type: MYSQL_TYPE_VARCHAR, Length: 64, Nullable: false},
			{Name: "TABLE_TYPE", Type: MYSQL_TYPE_VARCHAR, Length: 64, Nullable: false},
			{Name: "ENGINE", Type: MYSQL_TYPE_VARCHAR, Length: 64, Nullable: true},
			{Name: "VERSION", Type: MYSQL_TYPE_LONGLONG, Length: 21, Nullable: true},
			{Name: "ROW_FORMAT", Type: MYSQL_TYPE_VARCHAR, Length: 10, Nullable: true},
			{Name: "TABLE_ROWS", Type: MYSQL_TYPE_LONGLONG, Length: 21, Nullable: true},
			{Name: "AVG_ROW_LENGTH", Type: MYSQL_TYPE_LONGLONG, Length: 21, Nullable: true},
			{Name: "DATA_LENGTH", Type: MYSQL_TYPE_LONGLONG, Length: 21, Nullable: true},
			{Name: "MAX_DATA_LENGTH", Type: MYSQL_TYPE_LONGLONG, Length: 21, Nullable: true},
			{Name: "INDEX_LENGTH", Type: MYSQL_TYPE_LONGLONG, Length: 21, Nullable: true},
			{Name: "DATA_FREE", Type: MYSQL_TYPE_LONGLONG, Length: 21, Nullable: true},
			{Name: "AUTO_INCREMENT", Type: MYSQL_TYPE_LONGLONG, Length: 21, Nullable: true},
			{Name: "CREATE_TIME", Type: MYSQL_TYPE_DATETIME, Length: 0, Nullable: true},
			{Name: "UPDATE_TIME", Type: MYSQL_TYPE_DATETIME, Length: 0, Nullable: true},
			{Name: "CHECK_TIME", Type: MYSQL_TYPE_DATETIME, Length: 0, Nullable: true},
			{Name: "TABLE_COLLATION", Type: MYSQL_TYPE_VARCHAR, Length: 32, Nullable: true},
			{Name: "CHECKSUM", Type: MYSQL_TYPE_LONGLONG, Length: 21, Nullable: true},
			{Name: "CREATE_OPTIONS", Type: MYSQL_TYPE_VARCHAR, Length: 255, Nullable: true},
			{Name: "TABLE_COMMENT", Type: MYSQL_TYPE_VARCHAR, Length: 2048, Nullable: false},
		},
		Generator: &TablesGenerator{dictManager: im.dictManager},
	}

	// COLUMNS表
	im.tables["COLUMNS"] = &InfoSchemaTable{
		Name: "COLUMNS",
		Columns: []InfoSchemaColumn{
			{Name: "TABLE_CATALOG", Type: MYSQL_TYPE_VARCHAR, Length: 512, Nullable: true},
			{Name: "TABLE_SCHEMA", Type: MYSQL_TYPE_VARCHAR, Length: 64, Nullable: false},
			{Name: "TABLE_NAME", Type: MYSQL_TYPE_VARCHAR, Length: 64, Nullable: false},
			{Name: "COLUMN_NAME", Type: MYSQL_TYPE_VARCHAR, Length: 64, Nullable: false},
			{Name: "ORDINAL_POSITION", Type: MYSQL_TYPE_LONGLONG, Length: 21, Nullable: false},
			{Name: "COLUMN_DEFAULT", Type: MYSQL_TYPE_LONGTEXT, Length: 0, Nullable: true},
			{Name: "IS_NULLABLE", Type: MYSQL_TYPE_VARCHAR, Length: 3, Nullable: false},
			{Name: "DATA_TYPE", Type: MYSQL_TYPE_VARCHAR, Length: 64, Nullable: false},
			{Name: "CHARACTER_MAXIMUM_LENGTH", Type: MYSQL_TYPE_LONGLONG, Length: 21, Nullable: true},
			{Name: "CHARACTER_OCTET_LENGTH", Type: MYSQL_TYPE_LONGLONG, Length: 21, Nullable: true},
			{Name: "NUMERIC_PRECISION", Type: MYSQL_TYPE_LONGLONG, Length: 21, Nullable: true},
			{Name: "NUMERIC_SCALE", Type: MYSQL_TYPE_LONGLONG, Length: 21, Nullable: true},
			{Name: "DATETIME_PRECISION", Type: MYSQL_TYPE_LONGLONG, Length: 21, Nullable: true},
			{Name: "CHARACTER_SET_NAME", Type: MYSQL_TYPE_VARCHAR, Length: 32, Nullable: true},
			{Name: "COLLATION_NAME", Type: MYSQL_TYPE_VARCHAR, Length: 32, Nullable: true},
			{Name: "COLUMN_TYPE", Type: MYSQL_TYPE_LONGTEXT, Length: 0, Nullable: false},
			{Name: "COLUMN_KEY", Type: MYSQL_TYPE_VARCHAR, Length: 3, Nullable: false},
			{Name: "EXTRA", Type: MYSQL_TYPE_VARCHAR, Length: 30, Nullable: false},
			{Name: "PRIVILEGES", Type: MYSQL_TYPE_VARCHAR, Length: 80, Nullable: false},
			{Name: "COLUMN_COMMENT", Type: MYSQL_TYPE_VARCHAR, Length: 1024, Nullable: false},
		},
		Generator: &ColumnsGenerator{dictManager: im.dictManager},
	}

	// STATISTICS表
	im.tables["STATISTICS"] = &InfoSchemaTable{
		Name: "STATISTICS",
		Columns: []InfoSchemaColumn{
			{Name: "TABLE_CATALOG", Type: MYSQL_TYPE_VARCHAR, Length: 512, Nullable: true},
			{Name: "TABLE_SCHEMA", Type: MYSQL_TYPE_VARCHAR, Length: 64, Nullable: false},
			{Name: "TABLE_NAME", Type: MYSQL_TYPE_VARCHAR, Length: 64, Nullable: false},
			{Name: "NON_UNIQUE", Type: MYSQL_TYPE_LONGLONG, Length: 1, Nullable: false},
			{Name: "INDEX_SCHEMA", Type: MYSQL_TYPE_VARCHAR, Length: 64, Nullable: false},
			{Name: "INDEX_NAME", Type: MYSQL_TYPE_VARCHAR, Length: 64, Nullable: false},
			{Name: "SEQ_IN_INDEX", Type: MYSQL_TYPE_LONGLONG, Length: 21, Nullable: false},
			{Name: "COLUMN_NAME", Type: MYSQL_TYPE_VARCHAR, Length: 64, Nullable: false},
			{Name: "COLLATION", Type: MYSQL_TYPE_VARCHAR, Length: 1, Nullable: true},
			{Name: "CARDINALITY", Type: MYSQL_TYPE_LONGLONG, Length: 21, Nullable: true},
			{Name: "SUB_PART", Type: MYSQL_TYPE_LONGLONG, Length: 21, Nullable: true},
			{Name: "PACKED", Type: MYSQL_TYPE_VARCHAR, Length: 10, Nullable: true},
			{Name: "NULLABLE", Type: MYSQL_TYPE_VARCHAR, Length: 3, Nullable: false},
			{Name: "INDEX_TYPE", Type: MYSQL_TYPE_VARCHAR, Length: 16, Nullable: false},
			{Name: "COMMENT", Type: MYSQL_TYPE_VARCHAR, Length: 16, Nullable: true},
			{Name: "INDEX_COMMENT", Type: MYSQL_TYPE_VARCHAR, Length: 1024, Nullable: false},
		},
		Generator: &StatisticsGenerator{indexManager: im.indexManager},
	}

	return nil
}

// GetTable 获取表定义
func (im *InfoSchemaManager) GetTable(name string) *InfoSchemaTable {
	im.mu.RLock()
	defer im.mu.RUnlock()
	return im.tables[name]
}

// Query 查询INFORMATION_SCHEMA表
func (im *InfoSchemaManager) Query(tableName string, columns []string) ([][]interface{}, error) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	table := im.tables[tableName]
	if table == nil {
		return nil, ErrTableNotFound
	}

	im.stats.QueryCount++

	// 生成数据
	rows, err := table.Generator.Generate()
	if err != nil {
		im.stats.CacheMisses++
		return nil, err
	}

	im.stats.CacheHits++
	table.UpdateTime = time.Now()
	return rows, nil
}

// RefreshTable 刷新表数据
func (im *InfoSchemaManager) RefreshTable(tableName string) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	table := im.tables[tableName]
	if table == nil {
		return ErrTableNotFound
	}

	// 重新生成数据
	_, err := table.Generator.Generate()
	if err != nil {
		return err
	}

	table.UpdateTime = time.Now()
	im.stats.LastRefresh = time.Now()
	return nil
}

// GetStats 获取统计信息
func (im *InfoSchemaManager) GetStats() InfoSchemaStats {
	im.mu.RLock()
	defer im.mu.RUnlock()
	return im.stats
}

// Close 关闭管理器
func (im *InfoSchemaManager) Close() error {
	im.mu.Lock()
	defer im.mu.Unlock()

	// 清理资源
	im.tables = nil
	return nil
}

// MySQL数据类型常量
const (
	MYSQL_TYPE_DECIMAL    uint8 = 0
	MYSQL_TYPE_TINY       uint8 = 1
	MYSQL_TYPE_SHORT      uint8 = 2
	MYSQL_TYPE_LONG       uint8 = 3
	MYSQL_TYPE_FLOAT      uint8 = 4
	MYSQL_TYPE_DOUBLE     uint8 = 5
	MYSQL_TYPE_NULL       uint8 = 6
	MYSQL_TYPE_TIMESTAMP  uint8 = 7
	MYSQL_TYPE_LONGLONG   uint8 = 8
	MYSQL_TYPE_INT24      uint8 = 9
	MYSQL_TYPE_DATE       uint8 = 10
	MYSQL_TYPE_TIME       uint8 = 11
	MYSQL_TYPE_DATETIME   uint8 = 12
	MYSQL_TYPE_YEAR       uint8 = 13
	MYSQL_TYPE_VARCHAR    uint8 = 15
	MYSQL_TYPE_BIT        uint8 = 16
	MYSQL_TYPE_JSON       uint8 = 245
	MYSQL_TYPE_NEWDECIMAL uint8 = 246
	MYSQL_TYPE_ENUM       uint8 = 247
	MYSQL_TYPE_SET        uint8 = 248
	MYSQL_TYPE_TINY_BLOB  uint8 = 249
	MYSQL_TYPE_BLOB       uint8 = 252
	MYSQL_TYPE_VAR_STRING uint8 = 253
	MYSQL_TYPE_STRING     uint8 = 254
	MYSQL_TYPE_GEOMETRY   uint8 = 255
	MYSQL_TYPE_LONGTEXT   uint8 = 252
)
