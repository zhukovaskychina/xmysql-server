package store

import (
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/schemas"
)

type DataBaseImpl struct {
	schemas.Database
	dictionarySys *DictionarySys
	infos         schemas.InfoSchema
	dataBaseName  string
	conf          *conf.Cfg
	tableCache    map[string]schemas.Table
}

func NewDataBaseImpl(infos schemas.InfoSchema, conf *conf.Cfg, databaseName string) (schemas.Database, error) {
	var database = new(DataBaseImpl)
	database.infos = infos
	database.conf = conf
	//isExist := database.infos.GetSchemaExist(databaseName)
	//if isExist {
	//	return nil, errors.New("数据库已经存在")
	//}
	//database.tableCache = make(map[string]schemas.Table)
	//database.dataBaseName = databaseName
	//database.infos.PutDatabaseCache(database)
	return database, nil
}

func (d *DataBaseImpl) Name() string {
	return d.dataBaseName
}

func (d *DataBaseImpl) GetTable(name string) (schemas.Table, error) {
	if d.tableCache[name] == nil {
		return nil, errors.New("没有该表")
	}
	return d.tableCache[name], nil
}

func (d *DataBaseImpl) ListTables() []schemas.Table {
	var tableArrays = make([]schemas.Table, 0)

	for _, v := range d.tableCache {
		tableArrays = append(tableArrays, v)
	}
	return tableArrays
}

func (d *DataBaseImpl) DropTable(name string) error {
	delete(d.tableCache, name)
	return nil
}

func (d *DataBaseImpl) ListTableName() []string {
	panic("implement me")
}
