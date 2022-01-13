package store

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/model"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/schemas"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/tuple"
	"github.com/zhukovaskychina/xmysql-server/util"
	"io/ioutil"
	"path"
	"strings"
)

type InfoSchemaManager struct {
	//infoschema.InfoSchema
	conf            *conf.Cfg
	sysTableSpace   TableSpace
	dictionarySys   *DictionarySys
	schemaDBInfoMap map[string]*model.DBInfo
	schemaMap       map[string]schemas.Database
	pool            *buffer_pool.BufferPool
	tuplelru        schemas.TupleLRUCache
}

func (i *InfoSchemaManager) SchemaByID(id int64) (*model.DBInfo, bool) {
	panic("implement me")
}

func (i *InfoSchemaManager) TableByID(id int64) (schemas.Table, bool) {
	panic("implement me")
}

func (i *InfoSchemaManager) AllocByID(id int64) (schemas.Allocator, bool) {
	panic("implement me")
}

func NewInfoSchemaManager(conf *conf.Cfg, pool *buffer_pool.BufferPool) schemas.InfoSchema {
	var infoSchemaManager = new(InfoSchemaManager)
	infoSchemaManager.conf = conf
	infoSchemaManager.schemaDBInfoMap = make(map[string]*model.DBInfo)
	infoSchemaManager.schemaMap = make(map[string]schemas.Database)
	infoSchemaManager.sysTableSpace = NewSysTableSpace(conf, false)
	infoSchemaManager.pool = pool
	infoSchemaManager.tuplelru = NewTupleLRUCache()
	infoSchemaManager.initSysSchemas()
	return infoSchemaManager
}

func (i *InfoSchemaManager) initSysSchemas() {

	i.loadDatabase()

	bufferBlockSeven := i.pool.GetPageBlock(0, 7)
	//数据字典页面
	sysDict := NewDataDictWrapperByBytes(*(bufferBlockSeven.Frame))

	//加载或者初始化系统表，预热
	i.dictionarySys = NewDictionarySysByWrapper(sysDict)
	//加载数据字典表，初始化
	i.dictionarySys.loadDictionary(i.pool)

	//加载数据字典表
	sysDb := NewInfoSchemasDB().(*InfoSchemasDB)
	memorySystemTable := NewMemoryInnodbSysTable(i.dictionarySys)
	sysDb.addSystemTable(common.INNODB_SYS_TABLES, memorySystemTable)
	i.tuplelru.Set(common.INFORMATION_SCHEMAS, common.INNODB_SYS_TABLES, memorySystemTable)
	memoryColumnTable := NewMemoryInnodbSysColumns(i.dictionarySys)
	sysDb.addSystemTable(common.INNODB_SYS_COLUMNS, memoryColumnTable)
	i.tuplelru.Set(common.INFORMATION_SCHEMAS, common.INNODB_SYS_COLUMNS, memoryColumnTable)
	memoryIndexTable := NewMemorySysIndexTable(i.dictionarySys)
	sysDb.addSystemTable(common.INNODB_SYS_INDEXES, memoryIndexTable)
	i.tuplelru.Set(common.INFORMATION_SCHEMAS, common.INNODB_SYS_INDEXES, memoryIndexTable)
	currentDataFilesTable := NewMemoryInnodbDataFiles(16, "SYS_DATAFILES_SPACE", i.pool)
	sysDb.addSystemTable(common.INNODB_SYS_DATAFILES, currentDataFilesTable)
	i.tuplelru.Set(common.INFORMATION_SCHEMAS, common.INNODB_SYS_DATAFILES, currentDataFilesTable)
	currentTableSpaceTable := NewMemoryInnodbDataFiles(13, "SYS_TABLESPACES_SPACE", i.pool)
	sysDb.addSystemTable(common.INNODB_SYS_TABLESPACES, currentTableSpaceTable)
	i.tuplelru.Set(common.INFORMATION_SCHEMAS, common.INNODB_SYS_TABLESPACES, currentTableSpaceTable)

	//d := &model.DBInfo{Tables: nil, Name: nil}
	//TODO 初始化DATAFILES
	//memoryDataFiles:=NewMemoryInnodbDataFiles()
	//sysDb.addSystemTable(common.INNODB_SYS_DATAFILES, nil)
	i.schemaMap[common.INFORMATION_SCHEMAS] = sysDb

	dbInfo := model.NewDBInfo(model.CIStr{L: strings.ToLower(common.INFORMATION_SCHEMAS), O: common.INFORMATION_SCHEMAS})

	i.schemaDBInfoMap[common.INFORMATION_SCHEMAS] = dbInfo

}

//mysql 不管是啥，在硬盘上创建文件夹，mysql会认为是数据库
func (i *InfoSchemaManager) loadDatabase() {
	dataDir := i.conf.DataDir
	fs, errors := ioutil.ReadDir(dataDir)
	if errors != nil {
		panic("出现异常")
	}
	for _, v := range fs {
		if v.IsDir() {
			dirName := v.Name()
			dataBaseDir := path.Join(dataDir, "/", dirName)
			//如果db.opt存在，则是mysql的数据库
			isExist, err := util.PathExists(path.Join(dataBaseDir, "db.opt"))
			if err != nil {
				panic("出现异常")
			}
			if isExist {
				currentDB := &model.DBInfo{
					Name: model.CIStr{
						O: dirName,
						L: strings.ToLower(dirName),
					},
					Tables: nil,
				}
				i.schemaDBInfoMap[dirName] = currentDB
			}
		}
	}
}

func (i *InfoSchemaManager) SchemaByName(schema model.CIStr) (*model.DBInfo, bool) {

	if i.schemaDBInfoMap[schema.L] != nil {
		return i.schemaDBInfoMap[schema.L], true
	}
	return nil, false
}

func (i *InfoSchemaManager) SchemaExists(schema model.CIStr) bool {
	panic("implement me")
}

func (i *InfoSchemaManager) TableByName(schema, table model.CIStr) (schemas.Table, error) {

	return i.tuplelru.Get(schema.O, table.O)

}

func (i *InfoSchemaManager) TableExists(schema, table model.CIStr) bool {
	return i.tuplelru.Has(schema.O, table.O)
}

func (i *InfoSchemaManager) AllSchemaNames() []string {
	panic("implement me")
}

func (i *InfoSchemaManager) AllSchemas() []*model.DBInfo {
	panic("implement me")
}

func (i *InfoSchemaManager) Clone() (result []*model.DBInfo) {
	panic("implement me")
}

func (i *InfoSchemaManager) SchemaTables(schema model.CIStr) []schemas.Table {
	panic("implement me")
}

func (i *InfoSchemaManager) SchemaMetaVersion() int64 {
	panic("implement me")
}
func (i *InfoSchemaManager) GetSchemaByName(schemaName string) (schemas.Database, error) {
	return i.schemaMap[schemaName], nil
}

//初始化加载已经有了的表
//加载sys_table
//加载sys_fields
//加载sys_columns
//加载sys_index

func (i *InfoSchemaManager) GetSchemaExist(schemaName string) bool {
	if i.schemaMap[schemaName] != nil {
		return true
	}
	return false
}

func (i *InfoSchemaManager) GetTableByName(schema string, tableName string) (schemas.Table, error) {
	if strings.ToUpper(schema) == common.INFORMATION_SCHEMAS {
		tableName = strings.ToUpper(tableName)
	}
	table, err := i.tuplelru.Get(schema, tableName)
	//没有查找到
	if err != nil {
		err = nil
		//查找表的元祖信息
		memorySystemTable, _ := i.schemaMap[common.INFORMATION_SCHEMAS].GetTable(common.INNODB_SYS_TABLES)
		memoryIndexTable, _ := i.schemaMap[common.INFORMATION_SCHEMAS].GetTable(common.INNODB_SYS_INDEXES)
		searchKey := basic.NewVarcharVal([]byte(schema + "/" + tableName))
		var ordinaryTable schemas.Table
		iterator, _ := memorySystemTable.GetBtree("PRIMARY").Find(searchKey)
		var found bool
		found = false
		for _, _, currentRow, err, iterator := iterator(); iterator != nil; _, _, currentRow, err, iterator = iterator() {
			found = true
			if err != nil {
				fmt.Println(err)
				break
			}
			if currentRow != nil {
				tableNameValue := currentRow.GetValueByColName("NAME")

				tableIdValue := currentRow.GetValueByColName("TABLE_ID")

				spaceId := currentRow.GetValueByColName("SPACE")
				ordinaryTable = NewOrdinaryTable(i.conf, spaceId.Raw().(uint32), tableIdValue.Raw().(uint64), tableNameValue.ToString())
				ordinaryTable.(*OrdinaryTable).ReadFrmTuples()

				//构建TableSpace
				currentTableSpace := NewTableSpaceFile(i.conf, schema, tableName, spaceId.Raw().(uint32), false, i.pool)
				i.pool.FileSystem.AddTableSpace(currentTableSpace)

				//构建表的主键索引，secondary 索引
				for _, indexWrapperIterator := range ordinaryTable.(*OrdinaryTable).GetInfoWrappers() {
					indexNameValue := basic.NewVarcharVal([]byte(indexWrapperIterator.IndexName))
					var complexValues = make([]basic.Value, 0)
					complexValues = append(complexValues, indexNameValue)
					complexValues = append(complexValues, tableIdValue)
					//获取当前表的btree,用于给当前表构建btree
					primaryValue := basic.NewComplexValue(complexValues)
					//获取根页面号构建Index，构建Index
					memoryIndexTable.GetBtree("PRIMARY").DoFind(primaryValue, func(key basic.Value, row basic.Row) error {
						rootPageValue := row.GetValueByColName("PAGE_NO")
						spaceIdValue := row.GetValueByColName("SPACE")
						currentIndexNameValue := row.GetValueByColName("NAME")

						var leafTuple tuple.TableRowTuple
						var internalTuple tuple.TableRowTuple
						if strings.Compare("PRIMARY", currentIndexNameValue.ToString()) == 0 {
							leafTuple = NewClusterLeafTuple(ordinaryTable.(*OrdinaryTable).tableTupleMeta)
							internalTuple = NewClusterInternalTuple(ordinaryTable.(*OrdinaryTable).tableTupleMeta)
						} else {

						}
						//获取根页面
						bufferBlock := i.pool.GetPageBlock(spaceIdValue.Raw().(uint32), rootPageValue.Raw().(uint32))
						var leafOrInternal string
						leafOrInternal = i.getCurrentPageType(*bufferBlock.Frame, leafOrInternal)
						var rootIndex *Index
						if leafOrInternal == common.PAGE_LEAF {

							rootIndex = NewPageIndexByLoadBytesWithTuple(*bufferBlock.Frame, leafTuple).(*Index)

						} else {
							rootIndex = NewPageIndexByLoadBytesWithTuple(*bufferBlock.Frame, internalTuple).(*Index)
						}
						//获取节点段
						internalSegTop := rootIndex.GetSegTop()
						internalSegments := NewInternalSegmentWithTupleAndBufferPool(
							util.ReadUB4Byte2UInt32(internalSegTop[0:4]),
							util.ReadUB4Byte2UInt32(internalSegTop[4:8]),
							util.ReadUB2Byte2Int(internalSegTop[8:10]),
							currentIndexNameValue.ToString(),
							internalTuple,
							i.pool,
						)
						//获取叶子段
						leafSegTop := rootIndex.GetSegLeaf()
						dataSegments := NewDataSegmentWithTupleAndBufferPool(
							util.ReadUB4Byte2UInt32(leafSegTop[0:4]),
							util.ReadUB4Byte2UInt32(leafSegTop[4:8]),
							util.ReadUB2Byte2Int(leafSegTop[8:10]), currentIndexNameValue.ToString(),
							leafTuple,
							i.pool,
						)
						btree := NewBtreeWithBufferPool(spaceIdValue.Raw().(uint32), rootPageValue.Raw().(uint32),
							currentIndexNameValue.ToString(), internalSegments, dataSegments, rootIndex, i.pool, internalTuple, leafTuple)
						ordinaryTable.(*OrdinaryTable).AddBTree(currentIndexNameValue.ToString(), btree)

						return nil
					})
				}
				i.tuplelru.Set(schema, tableName, ordinaryTable)
			}
		}

		if found != true {
			err = common.NewErr(common.ErrNoSuchTable, schema, tableName)
		}
	}
	return table, err

}
func (i *InfoSchemaManager) getCurrentPageType(bytes []byte, leafOrInternal string) string {
	filePageTypeBytes := bytes[24:26]
	filePageType := util.ReadUB2Byte2Int(filePageTypeBytes)
	if filePageType == common.FILE_PAGE_INDEX {
		infimumSupremum := bytes[38+56 : 38+56+26]
		flags := util.ConvertByte2BitsString(infimumSupremum[0])[3]
		if flags == common.PAGE_LEAF {
			leafOrInternal = common.PAGE_LEAF
		} else {
			leafOrInternal = common.PAGE_INTERNAL
		}
	}
	return leafOrInternal
}

func (i *InfoSchemaManager) GetTableExist(schemaName string, tableName string) bool {
	panic("implement me")
}

func (i *InfoSchemaManager) GetAllSchemaNames() []string {
	panic("implement me")
}

func (i *InfoSchemaManager) GetAllSchemas() []schemas.Database {
	panic("implement me")
}

func (i *InfoSchemaManager) GetAllSchemaTablesByName(schemaName string) []schemas.Table {
	panic("implement me")
}

func (i *InfoSchemaManager) PutDatabaseCache(databaseCache schemas.Database) {
	i.schemaMap[databaseCache.Name()] = databaseCache
}
