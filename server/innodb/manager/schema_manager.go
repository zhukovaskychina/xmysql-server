package manager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"io/ioutil"
	"path"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// Table 定义表接口，用于metadata.Database
type Table interface {
	Name() string
	GetName() string
	GetColumns() []Column
	GetIndices() []Index
	GetConstraints() []ForeignKey
	GetOptions() map[string]string
}

// SimpleTable 简单的表实现
type SimpleTable struct {
	name string
}

func (t *SimpleTable) Name() string {
	return t.name
}

func (t *SimpleTable) GetName() string {
	return t.name
}

func (t *SimpleTable) GetColumns() []Column {
	return nil
}

func (t *SimpleTable) GetIndices() []Index {
	return nil
}

func (t *SimpleTable) GetConstraints() []ForeignKey {
	return nil
}

func (t *SimpleTable) GetOptions() map[string]string {
	return nil
}

// SimpleDatabase 简单的数据库实现
type SimpleDatabase struct {
	name    string
	path    string
	manager *SchemaManager             // 添加对SchemaManager的引用
	tables  map[string]*metadata.Table // 表缓存
	mu      sync.RWMutex               // 保护tables map
}

func (d *SimpleDatabase) Name() string {
	return d.name
}

func (d *SimpleDatabase) GetTable(name string) (*metadata.Table, error) {
	d.mu.RLock()

	// 1. 先从缓存中查找
	if table, exists := d.tables[name]; exists {
		d.mu.RUnlock()
		return table, nil
	}
	d.mu.RUnlock()

	// 2. 从文件系统加载表定义
	table, err := d.loadTableFromFilesystem(name)
	if err != nil {
		return nil, fmt.Errorf("table %s not found: %w", name, err)
	}

	// 3. 缓存表定义
	d.mu.Lock()
	if d.tables == nil {
		d.tables = make(map[string]*metadata.Table)
	}
	d.tables[name] = table
	d.mu.Unlock()

	return table, nil
}

func (d *SimpleDatabase) ListTables() []*metadata.Table {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// 如果缓存为空，从文件系统加载所有表
	if len(d.tables) == 0 {
		d.mu.RUnlock()
		d.loadAllTablesFromFilesystem()
		d.mu.RLock()
	}

	tables := make([]*metadata.Table, 0, len(d.tables))
	for _, table := range d.tables {
		tables = append(tables, table)
	}

	return tables
}

func (d *SimpleDatabase) CreateTable(conf *conf.Cfg, stmt *sqlparser.DDL) (*metadata.Table, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	tableName := stmt.Table.Name.String()

	// 1. 检查表是否已存在
	if _, exists := d.tables[tableName]; exists {
		return nil, fmt.Errorf("table %s already exists", tableName)
	}

	// 2. 从DDL语句构建表定义
	table, err := d.buildTableFromDDL(stmt)
	if err != nil {
		return nil, fmt.Errorf("failed to build table from DDL: %w", err)
	}

	// 3. 创建表文件（.frm文件或JSON格式）
	if err := d.createTableFile(tableName, table); err != nil {
		return nil, fmt.Errorf("failed to create table file: %w", err)
	}

	// 4. 创建表空间文件（.ibd文件）
	if err := d.createTablespace(tableName); err != nil {
		// 回滚：删除表文件
		d.deleteTableFile(tableName)
		return nil, fmt.Errorf("failed to create tablespace: %w", err)
	}

	// 5. 缓存表定义
	if d.tables == nil {
		d.tables = make(map[string]*metadata.Table)
	}
	d.tables[tableName] = table

	logger.Infof("Created table %s.%s", d.name, tableName)
	return table, nil
}

func (d *SimpleDatabase) DropTable(name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 1. 检查表是否存在
	if _, exists := d.tables[name]; !exists {
		return fmt.Errorf("table %s does not exist", name)
	}

	// 2. 删除表空间文件
	if err := d.deleteTablespace(name); err != nil {
		logger.Warnf("Failed to delete tablespace for table %s: %v", name, err)
	}

	// 3. 删除表文件
	if err := d.deleteTableFile(name); err != nil {
		return fmt.Errorf("failed to delete table file: %w", err)
	}

	// 4. 从缓存中删除
	delete(d.tables, name)

	logger.Infof("Dropped table %s.%s", d.name, name)
	return nil
}

func (d *SimpleDatabase) ListTableName() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// 如果缓存为空，从文件系统加载
	if len(d.tables) == 0 {
		d.mu.RUnlock()
		d.loadAllTablesFromFilesystem()
		d.mu.RLock()
	}

	names := make([]string, 0, len(d.tables))
	for name := range d.tables {
		names = append(names, name)
	}

	return names
}

func (d *SimpleDatabase) GetName() string {
	return d.name
}

func (d *SimpleDatabase) GetPath() string {
	return d.path
}

// loadTableFromFilesystem 从文件系统加载表定义
func (d *SimpleDatabase) loadTableFromFilesystem(tableName string) (*metadata.Table, error) {
	// 1. 查找表定义文件（支持.json和.frm格式）
	tableDefPath := filepath.Join(d.path, tableName+".json")
	if _, err := os.Stat(tableDefPath); os.IsNotExist(err) {
		// 尝试.frm格式
		tableDefPath = filepath.Join(d.path, tableName+".frm")
		if _, err := os.Stat(tableDefPath); os.IsNotExist(err) {
			return nil, fmt.Errorf("table definition file not found")
		}
	}

	// 2. 读取表定义文件
	data, err := ioutil.ReadFile(tableDefPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read table definition: %w", err)
	}

	// 3. 解析表定义（假设使用JSON格式）
	var tableMeta metadata.TableMeta
	if err := json.Unmarshal(data, &tableMeta); err != nil {
		return nil, fmt.Errorf("failed to parse table definition: %w", err)
	}

	// 4. 转换为metadata.Table
	table := metadata.NewTable(tableName)
	table.Engine = tableMeta.Engine
	table.Charset = tableMeta.Charset
	table.Collation = tableMeta.Collation
	table.Comment = tableMeta.Comment

	// 添加列
	for _, colMeta := range tableMeta.Columns {
		col := &metadata.Column{
			Name:            colMeta.Name,
			DataType:        colMeta.Type,
			CharMaxLength:   colMeta.Length,
			IsNullable:      colMeta.IsNullable,
			IsAutoIncrement: colMeta.IsAutoIncrement,
			DefaultValue:    colMeta.DefaultValue,
			Charset:         colMeta.Charset,
			Collation:       colMeta.Collation,
			Comment:         colMeta.Comment,
		}
		table.AddColumn(col)

		// 如果是主键列，创建主键索引
		if colMeta.IsPrimary {
			if table.PrimaryKey == nil {
				table.PrimaryKey = &metadata.Index{
					Name:      "PRIMARY",
					Columns:   []string{colMeta.Name},
					IsPrimary: true,
					IsUnique:  true,
				}
			} else {
				table.PrimaryKey.Columns = append(table.PrimaryKey.Columns, colMeta.Name)
			}
		}

		// 如果是唯一列，创建唯一索引
		if colMeta.IsUnique {
			uniqueIdx := &metadata.Index{
				Name:     fmt.Sprintf("uk_%s", colMeta.Name),
				Columns:  []string{colMeta.Name},
				IsUnique: true,
			}
			table.AddIndex(uniqueIdx)
		}
	}

	return table, nil
}

// loadAllTablesFromFilesystem 从文件系统加载所有表
func (d *SimpleDatabase) loadAllTablesFromFilesystem() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.tables == nil {
		d.tables = make(map[string]*metadata.Table)
	}

	// 读取数据库目录
	files, err := ioutil.ReadDir(d.path)
	if err != nil {
		logger.Warnf("Failed to read database directory %s: %v", d.path, err)
		return
	}

	// 查找所有表定义文件
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fileName := file.Name()
		// 检查是否是表定义文件
		if strings.HasSuffix(fileName, ".json") {
			tableName := strings.TrimSuffix(fileName, ".json")

			// 加载表定义
			d.mu.Unlock()
			table, err := d.loadTableFromFilesystem(tableName)
			d.mu.Lock()

			if err != nil {
				logger.Warnf("Failed to load table %s: %v", tableName, err)
				continue
			}

			d.tables[tableName] = table
		}
	}
}

// buildTableFromDDL 从DDL语句构建表定义
func (d *SimpleDatabase) buildTableFromDDL(stmt *sqlparser.DDL) (*metadata.Table, error) {
	tableName := stmt.Table.Name.String()
	table := metadata.NewTable(tableName)

	// 设置默认值
	table.Engine = "InnoDB"
	table.Charset = "utf8mb4"
	table.Collation = "utf8mb4_general_ci"

	// 解析列定义
	if stmt.TableSpec != nil {
		for _, colDef := range stmt.TableSpec.Columns {
			col := &metadata.Column{
				Name:       colDef.Name.String(),
				IsNullable: true, // 默认可空
			}

			// 解析列类型
			if colDef.Type.Type != "" {
				col.DataType = metadata.DataType(colDef.Type.Type)
				if colDef.Type.Length != nil {
					// 简化处理，实际需要解析sqlparser.SQLVal
					col.CharMaxLength = 255
				}
			}

			// 解析列选项
			if colDef.Type.NotNull {
				col.IsNullable = false
			}
			if colDef.Type.Autoincrement {
				col.IsAutoIncrement = true
			}
			if colDef.Type.Unsigned {
				col.IsUnsigned = true
			}
			if colDef.Type.Zerofill {
				col.IsZerofill = true
			}
			if colDef.Type.Charset != "" {
				col.Charset = colDef.Type.Charset
			}
			if colDef.Type.Collate != "" {
				col.Collation = colDef.Type.Collate
			}

			table.AddColumn(col)
		}
	}

	return table, nil
}

// createTableFile 创建表定义文件
func (d *SimpleDatabase) createTableFile(tableName string, table *metadata.Table) error {
	// 转换为TableMeta
	tableMeta := &metadata.TableMeta{
		Name:      tableName,
		Engine:    table.Engine,
		Charset:   table.Charset,
		Collation: table.Collation,
		Comment:   table.Comment,
		Columns:   make([]*metadata.ColumnMeta, 0, len(table.Columns)),
	}

	for _, col := range table.Columns {
		// 检查列是否是主键
		isPrimary := false
		if table.PrimaryKey != nil {
			for _, pkCol := range table.PrimaryKey.Columns {
				if pkCol == col.Name {
					isPrimary = true
					break
				}
			}
		}

		// 检查列是否有唯一索引
		isUnique := false
		for _, idx := range table.Indices {
			if idx.IsUnique && len(idx.Columns) == 1 && idx.Columns[0] == col.Name {
				isUnique = true
				break
			}
		}

		colMeta := &metadata.ColumnMeta{
			Name:            col.Name,
			Type:            col.DataType,
			Length:          col.CharMaxLength,
			IsNullable:      col.IsNullable,
			IsPrimary:       isPrimary,
			IsUnique:        isUnique,
			IsAutoIncrement: col.IsAutoIncrement,
			DefaultValue:    col.DefaultValue,
			Charset:         col.Charset,
			Collation:       col.Collation,
			Comment:         col.Comment,
		}
		tableMeta.Columns = append(tableMeta.Columns, colMeta)
	}

	// 序列化为JSON
	data, err := json.MarshalIndent(tableMeta, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal table metadata: %w", err)
	}

	// 写入文件
	tableDefPath := filepath.Join(d.path, tableName+".json")
	if err := ioutil.WriteFile(tableDefPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write table definition file: %w", err)
	}

	return nil
}

// deleteTableFile 删除表定义文件
func (d *SimpleDatabase) deleteTableFile(tableName string) error {
	tableDefPath := filepath.Join(d.path, tableName+".json")
	if err := os.Remove(tableDefPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete table definition file: %w", err)
	}
	return nil
}

// createTablespace 创建表空间文件
func (d *SimpleDatabase) createTablespace(tableName string) error {
	// 创建.ibd文件
	ibdPath := filepath.Join(d.path, tableName+".ibd")

	// 创建一个空的表空间文件（16KB初始大小）
	file, err := os.Create(ibdPath)
	if err != nil {
		return fmt.Errorf("failed to create tablespace file: %w", err)
	}
	defer file.Close()

	// 写入初始页面（简化实现）
	initialData := make([]byte, 16384) // 16KB
	if _, err := file.Write(initialData); err != nil {
		return fmt.Errorf("failed to write initial tablespace data: %w", err)
	}

	return nil
}

// deleteTablespace 删除表空间文件
func (d *SimpleDatabase) deleteTablespace(tableName string) error {
	ibdPath := filepath.Join(d.path, tableName+".ibd")
	if err := os.Remove(ibdPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete tablespace file: %w", err)
	}
	return nil
}

// SchemaManager 管理数据库schema
type SchemaManager struct {
	conf      *conf.Cfg
	schemaMap map[string]metadata.Database
	pool      *buffer_pool.BufferPool
	mu        sync.RWMutex // 添加读写锁保护并发访问
}

// NewSchemaManager 创建schema管理器
func NewSchemaManager(conf *conf.Cfg, pool *buffer_pool.BufferPool) *SchemaManager {
	var schemaManager = new(SchemaManager)
	schemaManager.conf = conf
	schemaManager.schemaMap = make(map[string]metadata.Database)
	schemaManager.pool = pool
	schemaManager.initSysSchemas()
	return schemaManager
}

func (m *SchemaManager) initSysSchemas() {
	m.loadDatabase()
}

// loadDatabase 加载所有数据库
func (m *SchemaManager) loadDatabase() {
	logger.Debugf("Loading databases from filesystem and data dictionary")

	// 1. 从文件系统加载数据库目录
	m.loadDatabasesFromFilesystem()

	// 2. 从数据字典加载数据库元数据
	m.loadDatabasesFromDataDictionary()

	// 3. 同步文件系统和数据字典的差异
	m.syncDatabasesWithDataDictionary()

	logger.Infof("Loaded %d databases into memory", len(m.schemaMap))
}

// loadDatabasesFromFilesystem 从文件系统加载数据库
func (m *SchemaManager) loadDatabasesFromFilesystem() {
	files, err := ioutil.ReadDir(m.conf.DataDir)
	if err != nil {
		logger.Warnf("Failed to read data directory: %v", err)
		return
	}

	for _, f := range files {
		if !f.IsDir() {
			continue
		}

		dbName := f.Name()
		dbPath := path.Join(m.conf.DataDir, dbName)

		// 跳过系统文件和临时目录
		if strings.HasPrefix(dbName, "#") || strings.HasPrefix(dbName, "@") || strings.HasPrefix(dbName, ".") {
			continue
		}

		// 跳过非数据库目录（如果没有db.opt文件）
		dbOptPath := filepath.Join(dbPath, "db.opt")
		if _, err := os.Stat(dbOptPath); os.IsNotExist(err) {
			logger.Debugf("Skipping directory '%s' - no db.opt file found", dbName)
			continue
		}

		// 创建数据库对象
		db := &SimpleDatabase{
			name:    dbName,
			path:    dbPath,
			manager: m,
			tables:  make(map[string]*metadata.Table),
		}
		m.schemaMap[dbName] = db

		logger.Debugf("Loaded database from filesystem: %s", dbName)
	}
}

// loadDatabasesFromDataDictionary 从数据字典加载数据库元数据
func (m *SchemaManager) loadDatabasesFromDataDictionary() {
	dataDictPath := filepath.Join(m.conf.DataDir, "data_dictionary.json")

	data, err := ioutil.ReadFile(dataDictPath)
	if err != nil {
		logger.Debugf("Data dictionary file not found, will be created on first database creation")
		return
	}

	var dataDictionary DataDictionary
	if err := json.Unmarshal(data, &dataDictionary); err != nil {
		logger.Warnf("Failed to parse data dictionary: %v", err)
		return
	}

	if dataDictionary.Databases == nil {
		return
	}

	for dbName, _ := range dataDictionary.Databases {
		// 检查数据库是否已从文件系统加载
		if _, exists := m.schemaMap[dbName]; exists {
			logger.Debugf("Database '%s' metadata loaded from data dictionary", dbName)
			continue
		}

		// 如果数据字典中有记录但文件系统中没有，记录警告
		logger.Warnf("Database '%s' found in data dictionary but not in filesystem", dbName)
	}

	logger.Debugf("Data dictionary loaded with %d database records", len(dataDictionary.Databases))
}

// syncDatabasesWithDataDictionary 同步文件系统和数据字典的差异
func (m *SchemaManager) syncDatabasesWithDataDictionary() {
	dataDictPath := filepath.Join(m.conf.DataDir, "data_dictionary.json")

	// 读取现有数据字典
	var dataDictionary DataDictionary
	if data, err := ioutil.ReadFile(dataDictPath); err == nil {
		json.Unmarshal(data, &dataDictionary)
	}

	if dataDictionary.Databases == nil {
		dataDictionary.Databases = make(map[string]*DatabaseMetadata)
	}

	needsUpdate := false

	// 检查文件系统中的数据库是否都在数据字典中
	for dbName := range m.schemaMap {
		if _, exists := dataDictionary.Databases[dbName]; !exists {
			// 从db.opt文件读取字符集信息
			charset, collation := m.readDatabaseCharsetFromFile(dbName)

			// 添加到数据字典
			dataDictionary.Databases[dbName] = &DatabaseMetadata{
				Name:         dbName,
				Charset:      charset,
				Collation:    collation,
				CreatedTime:  time.Now(), // 无法获取真实创建时间，使用当前时间
				ModifiedTime: time.Now(),
			}

			needsUpdate = true
			logger.Infof("Added missing database '%s' to data dictionary", dbName)
		}
	}

	// 检查数据字典中的数据库是否都在文件系统中
	for dbName := range dataDictionary.Databases {
		if _, exists := m.schemaMap[dbName]; !exists {
			// 数据字典中有记录但文件系统中没有，从数据字典中删除
			delete(dataDictionary.Databases, dbName)
			needsUpdate = true
			logger.Warnf("Removed orphaned database '%s' from data dictionary", dbName)
		}
	}

	// 如果需要更新，写回数据字典文件
	if needsUpdate {
		dataDictionary.LastModified = time.Now()

		data, err := json.MarshalIndent(dataDictionary, "", "  ")
		if err == nil {
			ioutil.WriteFile(dataDictPath, data, 0644)
			logger.Debugf("Data dictionary synchronized")
		}
	}
}

// readDatabaseCharsetFromFile 从db.opt文件读取数据库字符集信息
func (m *SchemaManager) readDatabaseCharsetFromFile(dbName string) (charset, collation string) {
	dbPath := filepath.Join(m.conf.DataDir, dbName)
	dbOptPath := filepath.Join(dbPath, "db.opt")

	// 设置默认值
	charset = "utf8mb4"
	collation = "utf8mb4_general_ci"

	content, err := ioutil.ReadFile(dbOptPath)
	if err != nil {
		return charset, collation
	}

	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "default-character-set=") {
			charset = strings.TrimPrefix(line, "default-character-set=")
		} else if strings.HasPrefix(line, "default-collation=") {
			collation = strings.TrimPrefix(line, "default-collation=")
		}
	}

	return charset, collation
}

func (m *SchemaManager) GetSchemaByName(schemaName string) (metadata.Database, error) {
	if db, ok := m.schemaMap[schemaName]; ok {
		return db, nil
	}
	return nil, fmt.Errorf("schema %s not found", schemaName)
}

func (m *SchemaManager) GetSchemaExist(schemaName string) bool {
	_, ok := m.schemaMap[schemaName]
	return ok
}

func (m *SchemaManager) GetTableByName(schema string, tableName string) (*metadata.Table, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 1. 获取数据库
	db, ok := m.schemaMap[schema]
	if !ok {
		return nil, fmt.Errorf("schema %s not found", schema)
	}

	// 2. 从数据库获取表
	table, err := db.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("table %s.%s not found: %w", schema, tableName, err)
	}

	return table, nil
}

func (m *SchemaManager) GetTableExist(schemaName string, tableName string) bool {
	_, err := m.GetTableByName(schemaName, tableName)
	return err == nil
}

func (m *SchemaManager) GetAllSchemaNames() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var names []string
	for name := range m.schemaMap {
		names = append(names, name)
	}
	return names
}

func (m *SchemaManager) GetAllSchemas() []metadata.Database {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var dbs []metadata.Database
	for _, db := range m.schemaMap {
		dbs = append(dbs, db)
	}
	return dbs
}

func (m *SchemaManager) GetAllSchemaTablesByName(schemaName string) []*metadata.Table {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 1. 获取数据库
	db, ok := m.schemaMap[schemaName]
	if !ok {
		return nil
	}

	// 2. 获取数据库下的所有表
	return db.ListTables()
}

func (m *SchemaManager) PutDatabaseCache(databaseCache metadata.Database) {
	m.schemaMap[databaseCache.Name()] = databaseCache
}

// CreateDatabase 创建新数据库
func (m *SchemaManager) CreateDatabase(name string, charset string, collation string, ifNotExists bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logger.Debugf("Creating database: %s (charset: %s, collation: %s)", name, charset, collation)

	// 1. 验证数据库名称
	if err := m.validateDatabaseName(name); err != nil {
		return fmt.Errorf("invalid database name '%s': %v", name, err)
	}

	// 2. 检查数据库是否已存在（内存中的schemaMap）
	if _, exists := m.schemaMap[name]; exists {
		if ifNotExists {
			logger.Debugf("Database '%s' already exists, skipping creation due to IF NOT EXISTS", name)
			return nil // IF NOT EXISTS，不报错
		}
		return fmt.Errorf("database '%s' already exists", name)
	}

	// 3. 检查文件系统中是否已存在（防止不一致）
	dbPath := filepath.Join(m.conf.DataDir, name)
	if _, err := os.Stat(dbPath); err == nil {
		if ifNotExists {
			logger.Debugf("Database directory '%s' already exists, skipping creation due to IF NOT EXISTS", name)
			// 重新加载到内存中
			m.loadSingleDatabase(name, dbPath)
			return nil
		}
		return fmt.Errorf("database directory '%s' already exists", dbPath)
	}

	// 4. 设置默认字符集和排序规则
	if charset == "" {
		charset = "utf8mb4"
	}
	if collation == "" {
		collation = "utf8mb4_general_ci"
	}

	// 5. 验证字符集和排序规则的有效性
	if err := m.validateCharsetAndCollation(charset, collation); err != nil {
		return fmt.Errorf("invalid charset or collation: %v", err)
	}

	// 6. 创建文件系统目录
	if err := m.createDatabaseDirectory(dbPath); err != nil {
		return fmt.Errorf("failed to create database directory: %v", err)
	}

	// 7. 创建数据库元数据文件 (db.opt)
	if err := m.createDatabaseMetadata(dbPath, name, charset, collation); err != nil {
		// 回滚：删除已创建的目录
		os.RemoveAll(dbPath)
		return fmt.Errorf("failed to create database metadata: %v", err)
	}

	// 8. 写入数据字典（系统表空间）
	if err := m.writeDatabaseToDataDictionary(name, charset, collation); err != nil {
		// 回滚：删除已创建的目录和文件
		os.RemoveAll(dbPath)
		return fmt.Errorf("failed to write database to data dictionary: %v", err)
	}

	// 9. 创建数据库对象并注册到schemaMap（内存缓存）
	db := &SimpleDatabase{
		name:    name,
		path:    dbPath,
		manager: m,
		tables:  make(map[string]*metadata.Table),
	}
	m.schemaMap[name] = db

	logger.Infof("Database '%s' created successfully at %s", name, dbPath)
	logger.Debugf("Database metadata: charset=%s, collation=%s", charset, collation)
	return nil
}

// loadSingleDatabase 加载单个数据库到内存
func (m *SchemaManager) loadSingleDatabase(name, dbPath string) {
	db := &SimpleDatabase{
		name:    name,
		path:    dbPath,
		manager: m,
		tables:  make(map[string]*metadata.Table),
	}
	m.schemaMap[name] = db
	logger.Debugf("Loaded existing database '%s' into memory", name)
}

// validateCharsetAndCollation 验证字符集和排序规则
func (m *SchemaManager) validateCharsetAndCollation(charset, collation string) error {
	// MySQL标准字符集验证
	validCharsets := map[string][]string{
		"utf8mb4": {"utf8mb4_general_ci", "utf8mb4_unicode_ci", "utf8mb4_bin", "utf8mb4_0900_ai_ci"},
		"utf8":    {"utf8_general_ci", "utf8_unicode_ci", "utf8_bin"},
		"latin1":  {"latin1_swedish_ci", "latin1_general_ci", "latin1_bin"},
		"ascii":   {"ascii_general_ci", "ascii_bin"},
		"binary":  {"binary"},
	}

	// 检查字符集是否支持
	collations, exists := validCharsets[charset]
	if !exists {
		return fmt.Errorf("unsupported charset '%s'", charset)
	}

	// 检查排序规则是否与字符集匹配
	for _, validCollation := range collations {
		if collation == validCollation {
			return nil
		}
	}

	return fmt.Errorf("collation '%s' is not valid for charset '%s'", collation, charset)
}

// writeDatabaseToDataDictionary 将数据库信息写入数据字典
func (m *SchemaManager) writeDatabaseToDataDictionary(name, charset, collation string) error {
	logger.Debugf("Writing database '%s' to data dictionary", name)

	// 创建数据库元数据记录
	dbMetadata := &DatabaseMetadata{
		Name:         name,
		Charset:      charset,
		Collation:    collation,
		CreatedTime:  time.Now(),
		ModifiedTime: time.Now(),
	}

	// 写入数据字典的具体实现
	// 在真实的MySQL实现中，这里会写入到系统表空间的数据字典表
	// 对于XMySQL，我们可以：
	// 1. 写入到专门的数据字典文件
	// 2. 写入到系统数据库的表中
	// 3. 写入到内存数据结构中（当前简化实现）

	if err := m.persistDatabaseMetadata(dbMetadata); err != nil {
		return fmt.Errorf("failed to persist database metadata: %v", err)
	}

	logger.Debugf("Database '%s' metadata written to data dictionary", name)
	return nil
}

// persistDatabaseMetadata 持久化数据库元数据
func (m *SchemaManager) persistDatabaseMetadata(metadata *DatabaseMetadata) error {
	// 方案1: 写入到数据字典文件
	dataDictPath := filepath.Join(m.conf.DataDir, "data_dictionary.json")

	// 读取现有的数据字典
	var dataDictionary DataDictionary
	if data, err := ioutil.ReadFile(dataDictPath); err == nil {
		json.Unmarshal(data, &dataDictionary)
	}

	// 初始化数据字典结构
	if dataDictionary.Databases == nil {
		dataDictionary.Databases = make(map[string]*DatabaseMetadata)
	}

	// 添加新数据库
	dataDictionary.Databases[metadata.Name] = metadata
	dataDictionary.LastModified = time.Now()

	// 写回文件
	data, err := json.MarshalIndent(dataDictionary, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal data dictionary: %v", err)
	}

	if err := ioutil.WriteFile(dataDictPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write data dictionary file: %v", err)
	}

	logger.Debugf("Data dictionary updated: %s", dataDictPath)
	return nil
}

// DatabaseMetadata 数据库元数据结构
type DatabaseMetadata struct {
	Name         string    `json:"name"`
	Charset      string    `json:"charset"`
	Collation    string    `json:"collation"`
	CreatedTime  time.Time `json:"created_time"`
	ModifiedTime time.Time `json:"modified_time"`
}

// DataDictionary 数据字典结构
type DataDictionary struct {
	Databases    map[string]*DatabaseMetadata `json:"databases"`
	LastModified time.Time                    `json:"last_modified"`
}

// DropDatabase 删除数据库
func (m *SchemaManager) DropDatabase(name string, ifExists bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logger.Debugf("Dropping database: %s", name)

	// 1. 检查数据库是否存在（内存中的schemaMap）
	db, exists := m.schemaMap[name]
	if !exists {
		if ifExists {
			logger.Debugf("Database '%s' does not exist in memory, skipping drop due to IF EXISTS", name)
			return nil // IF EXISTS，不报错
		}
		return fmt.Errorf("database '%s' does not exist", name)
	}

	// 2. 检查是否为系统数据库
	if m.isSystemDatabase(name) {
		return fmt.Errorf("cannot drop system database '%s'", name)
	}

	// 3. 检查数据库是否包含表（防止意外删除）
	if err := m.checkDatabaseEmpty(name); err != nil {
		return fmt.Errorf("cannot drop database '%s': %v", name, err)
	}

	// 4. 从数据字典中删除数据库记录
	if err := m.removeDatabaseFromDataDictionary(name); err != nil {
		return fmt.Errorf("failed to remove database from data dictionary: %v", err)
	}

	// 5. 删除文件系统目录
	if simpleDB, ok := db.(*SimpleDatabase); ok {
		if err := os.RemoveAll(simpleDB.path); err != nil {
			// 如果文件删除失败，尝试回滚数据字典
			m.rollbackDataDictionaryDeletion(name)
			return fmt.Errorf("failed to remove database directory: %v", err)
		}
	}

	// 6. 从schemaMap中移除（内存缓存）
	delete(m.schemaMap, name)

	logger.Infof("Database '%s' dropped successfully", name)
	return nil
}

// checkDatabaseEmpty 检查数据库是否为空（不包含表）
func (m *SchemaManager) checkDatabaseEmpty(dbName string) error {
	db, exists := m.schemaMap[dbName]
	if !exists {
		return nil // 数据库不存在，认为是空的
	}

	// 检查数据库目录下是否有表文件
	if simpleDB, ok := db.(*SimpleDatabase); ok {
		files, err := ioutil.ReadDir(simpleDB.path)
		if err != nil {
			return fmt.Errorf("failed to read database directory: %v", err)
		}

		tableCount := 0
		for _, file := range files {
			// 跳过db.opt文件和其他非表文件
			if file.Name() == "db.opt" || strings.HasPrefix(file.Name(), ".") {
				continue
			}
			// 检查是否为表文件（.ibd, .frm等）
			if strings.HasSuffix(file.Name(), ".ibd") ||
				strings.HasSuffix(file.Name(), ".frm") ||
				strings.HasSuffix(file.Name(), ".MYD") ||
				strings.HasSuffix(file.Name(), ".MYI") {
				tableCount++
			}
		}

		if tableCount > 0 {
			return fmt.Errorf("database contains %d table(s), drop tables first", tableCount)
		}
	}

	return nil
}

// removeDatabaseFromDataDictionary 从数据字典中删除数据库
func (m *SchemaManager) removeDatabaseFromDataDictionary(name string) error {
	logger.Debugf("Removing database '%s' from data dictionary", name)

	dataDictPath := filepath.Join(m.conf.DataDir, "data_dictionary.json")

	// 读取现有的数据字典
	var dataDictionary DataDictionary
	if data, err := ioutil.ReadFile(dataDictPath); err != nil {
		// 如果数据字典文件不存在，认为删除成功
		logger.Debugf("Data dictionary file not found, assuming database not in dictionary")
		return nil
	} else {
		if err := json.Unmarshal(data, &dataDictionary); err != nil {
			return fmt.Errorf("failed to parse data dictionary: %v", err)
		}
	}

	// 检查数据库是否存在于数据字典中
	if dataDictionary.Databases == nil {
		return nil // 数据字典为空，认为删除成功
	}

	if _, exists := dataDictionary.Databases[name]; !exists {
		logger.Debugf("Database '%s' not found in data dictionary", name)
		return nil // 数据库不在数据字典中，认为删除成功
	}

	// 删除数据库记录
	delete(dataDictionary.Databases, name)
	dataDictionary.LastModified = time.Now()

	// 写回文件
	data, err := json.MarshalIndent(dataDictionary, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal data dictionary: %v", err)
	}

	if err := ioutil.WriteFile(dataDictPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write data dictionary file: %v", err)
	}

	logger.Debugf("Database '%s' removed from data dictionary", name)
	return nil
}

// rollbackDataDictionaryDeletion 回滚数据字典删除操作
func (m *SchemaManager) rollbackDataDictionaryDeletion(name string) {
	logger.Warnf("Attempting to rollback data dictionary deletion for database '%s'", name)

	// 这里可以实现更复杂的回滚逻辑
	// 例如从备份中恢复数据字典记录
	// 当前简化实现只记录警告日志

	logger.Warnf("Manual intervention may be required to restore database '%s' in data dictionary", name)
}

// validateDatabaseName 验证数据库名称
func (m *SchemaManager) validateDatabaseName(name string) error {
	// 1. 检查长度
	if len(name) == 0 {
		return fmt.Errorf("database name cannot be empty")
	}
	if len(name) > 64 {
		return fmt.Errorf("database name too long (max 64 characters)")
	}

	// 2. 检查字符合法性 (MySQL标准)
	validName := regexp.MustCompile(`^[a-zA-Z0-9_$]+$`)
	if !validName.MatchString(name) {
		return fmt.Errorf("database name contains invalid characters (only letters, numbers, underscore, and dollar sign allowed)")
	}

	// 3. 检查是否以数字开头
	if name[0] >= '0' && name[0] <= '9' {
		return fmt.Errorf("database name cannot start with a number")
	}

	// 4. 检查保留字
	reservedWords := []string{
		"information_schema", "mysql", "performance_schema", "sys",
		"test", "tmp", "temp",
	}
	lowerName := strings.ToLower(name)
	for _, reserved := range reservedWords {
		if lowerName == reserved {
			return fmt.Errorf("'%s' is a reserved database name", name)
		}
	}

	return nil
}

// createDatabaseDirectory 创建数据库目录
func (m *SchemaManager) createDatabaseDirectory(dbPath string) error {
	// 检查目录是否已存在
	if _, err := os.Stat(dbPath); err == nil {
		return fmt.Errorf("database directory already exists: %s", dbPath)
	}

	// 创建目录
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %v", dbPath, err)
	}

	logger.Debugf("Created database directory: %s", dbPath)
	return nil
}

// createDatabaseMetadata 创建数据库元数据文件
func (m *SchemaManager) createDatabaseMetadata(dbPath, name, charset, collation string) error {
	// 创建 db.opt 文件 (MySQL兼容)
	dbOptPath := filepath.Join(dbPath, "db.opt")
	dbOptContent := fmt.Sprintf("default-character-set=%s\ndefault-collation=%s\n", charset, collation)

	if err := ioutil.WriteFile(dbOptPath, []byte(dbOptContent), 0644); err != nil {
		return fmt.Errorf("failed to create db.opt file: %v", err)
	}

	logger.Debugf("Created database metadata file: %s", dbOptPath)
	return nil
}

// isSystemDatabase 检查是否为系统数据库
func (m *SchemaManager) isSystemDatabase(name string) bool {
	systemDatabases := []string{
		"information_schema",
		"mysql",
		"performance_schema",
		"sys",
	}

	lowerName := strings.ToLower(name)
	for _, sysDB := range systemDatabases {
		if lowerName == sysDB {
			return true
		}
	}
	return false
}

// GetDatabaseInfo 获取数据库信息
func (m *SchemaManager) GetDatabaseInfo(name string) (*DatabaseInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	db, exists := m.schemaMap[name]
	if !exists {
		return nil, fmt.Errorf("database '%s' not found", name)
	}

	info := &DatabaseInfo{
		Name:      name,
		Charset:   "utf8mb4",            // 默认值
		Collation: "utf8mb4_general_ci", // 默认值
	}

	// 尝试从db.opt文件读取字符集信息
	if simpleDB, ok := db.(*SimpleDatabase); ok {
		dbOptPath := filepath.Join(simpleDB.path, "db.opt")
		if content, err := ioutil.ReadFile(dbOptPath); err == nil {
			lines := strings.Split(string(content), "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "default-character-set=") {
					info.Charset = strings.TrimPrefix(line, "default-character-set=")
				} else if strings.HasPrefix(line, "default-collation=") {
					info.Collation = strings.TrimPrefix(line, "default-collation=")
				}
			}
		}
	}

	return info, nil
}

// DatabaseInfo 数据库信息结构
type DatabaseInfo struct {
	Name      string
	Charset   string
	Collation string
	Path      string
	Tables    []string
}
