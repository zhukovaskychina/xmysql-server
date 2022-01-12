package store

import (
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/schemas"
)

type InfoSchemasDB struct {
	schemas.Database
	tableCache map[string]schemas.Table
}

func NewInfoSchemasDB() schemas.Database {
	var infoschemasDB = new(InfoSchemasDB)
	infoschemasDB.tableCache = make(map[string]schemas.Table)
	return infoschemasDB
}

func (i *InfoSchemasDB) addSystemTable(tableName string, systemTable schemas.Table) {
	i.tableCache[tableName] = systemTable
}

func (i *InfoSchemasDB) Name() string {
	return common.INFORMATION_SCHEMAS
}

func (i *InfoSchemasDB) GetTable(name string) (schemas.Table, error) {
	return i.tableCache[name], nil
}

func (i *InfoSchemasDB) ListTables() []schemas.Table {
	var schemaTableList = make([]schemas.Table, 0)
	for _, table := range i.tableCache {
		schemaTableList = append(schemaTableList, table)
	}
	return schemaTableList
}

func (i *InfoSchemasDB) DropTable(name string) error {
	panic("implement me")
}

func (i *InfoSchemasDB) ListTableName() []string {
	panic("implement me")
}
