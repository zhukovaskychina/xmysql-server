package manager

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"io/ioutil"
	"path"
	"strings"
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
	name string
	path string
}

func (d *SimpleDatabase) Name() string {
	return d.name
}

func (d *SimpleDatabase) GetTable(name string) (*metadata.Table, error) {
	// TODO: 实现获取表
	return nil, fmt.Errorf("table %s not found", name)
}

func (d *SimpleDatabase) ListTables() []*metadata.Table {
	// TODO: 实现列出所有表
	return nil
}

func (d *SimpleDatabase) CreateTable(conf *conf.Cfg, stmt *sqlparser.DDL) (*metadata.Table, error) {
	// TODO: 实现创建表
	return nil, fmt.Errorf("create table not implemented")
}

func (d *SimpleDatabase) DropTable(name string) error {
	// TODO: 实现删除表
	return fmt.Errorf("drop table not implemented")
}

func (d *SimpleDatabase) ListTableName() []string {
	// TODO: 实现列出表名
	return nil
}

func (d *SimpleDatabase) GetName() string {
	return d.name
}

func (d *SimpleDatabase) GetPath() string {
	return d.path
}

// SchemaManager 管理数据库schema
type SchemaManager struct {
	conf      *conf.Cfg
	schemaMap map[string]metadata.Database
	pool      *buffer_pool.BufferPool
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
	files, _ := ioutil.ReadDir(m.conf.DataDir)
	for _, f := range files {
		if !f.IsDir() {
			continue
		}
		dbPath := path.Join(m.conf.DataDir, f.Name())
		if strings.HasPrefix(f.Name(), "#") {
			continue
		}
		if strings.HasPrefix(f.Name(), "@") {
			continue
		}
		// 创建简单的database实现
		db := &SimpleDatabase{
			name: f.Name(),
			path: dbPath,
		}
		m.schemaMap[f.Name()] = db
	}
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
	// TODO: 实现从数据字典获取表
	return nil, fmt.Errorf("table %s.%s not found", schema, tableName)
}

func (m *SchemaManager) GetTableExist(schemaName string, tableName string) bool {
	_, err := m.GetTableByName(schemaName, tableName)
	return err == nil
}

func (m *SchemaManager) GetAllSchemaNames() []string {
	var names []string
	for name := range m.schemaMap {
		names = append(names, name)
	}
	return names
}

func (m *SchemaManager) GetAllSchemas() []metadata.Database {
	var dbs []metadata.Database
	for _, db := range m.schemaMap {
		dbs = append(dbs, db)
	}
	return dbs
}

func (m *SchemaManager) GetAllSchemaTablesByName(schemaName string) []*metadata.Table {
	// TODO: 实现获取schema下所有表
	return nil
}

func (m *SchemaManager) PutDatabaseCache(databaseCache metadata.Database) {
	m.schemaMap[databaseCache.Name()] = databaseCache
}
