package store

import (
	"strings"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/schemas"
	tuple2 "github.com/zhukovaskychina/xmysql-server/server/innodb/tuple"
)

//定义一般业务表
//包括了表信息，存储路径，索引信息
//需要SysIndex构建出BTree
type OrdinaryTable struct {
	schemas.Table

	conf *conf.Cfg
	//Btree
	btreeMap map[string]basic.Tree

	//索引集合
	indexMap map[string]basic.Index

	tuple tuple2.TableRowTuple

	spaceId uint32

	tableId uint64

	ibdFilePath string

	databaseName string

	tableName string

	fullName string

	tableTupleMeta *TableTupleMeta
}

func (o OrdinaryTable) TableName() string {
	return o.tableTupleMeta.GetTableName()
}

func (o OrdinaryTable) TableId() uint64 {
	return o.tableId
}

func (o OrdinaryTable) SpaceId() uint32 {
	return o.spaceId
}

func (o OrdinaryTable) ColNums() int {
	panic("implement me")
}

func (o OrdinaryTable) RowIter() (basic.RowIterator, error) {
	panic("implement me")
}

func (o OrdinaryTable) GetTuple() tuple2.TableRowTuple {
	return o.tuple
}

func (o OrdinaryTable) GetBtree(indexName string) basic.Tree {
	return o.btreeMap[indexName]
}

func (o OrdinaryTable) GetTableTupleMeta() tuple2.TableTuple {
	return o.tableTupleMeta
}

func NewOrdinaryTable(conf *conf.Cfg, spaceId uint32, tableId uint64, fullName string) schemas.Table {
	var table = new(OrdinaryTable)
	table.spaceId = spaceId
	table.tableId = tableId
	table.conf = conf
	table.fullName = fullName
	table.btreeMap = make(map[string]basic.Tree)

	return table
}

func (o *OrdinaryTable) AddFilePath(filePath string) {
	o.ibdFilePath = o.conf.DataDir + filePath
}

func (o *OrdinaryTable) ReadFrmTuples() {
	nameCopy := strings.Split(o.fullName, "/")
	o.databaseName = nameCopy[0]
	o.tableName = nameCopy[1]
	o.tableTupleMeta = NewTupleMeta(o.databaseName, o.tableName, o.conf)
	o.tableTupleMeta.ReadFrmFromDisk()
}

func (o *OrdinaryTable) GetInfoWrappers() []*tuple2.IndexInfoWrapper {
	var indexInfoWrappers = make([]*tuple2.IndexInfoWrapper, 0)
	indexInfoWrappers = append(indexInfoWrappers, o.tableTupleMeta.PrimaryIndexInfos)
	indexInfoWrappers = append(indexInfoWrappers, o.tableTupleMeta.SecondaryIndexInfos...)
	return indexInfoWrappers
}

func (o *OrdinaryTable) AddBTree(indexName string, tree basic.Tree) {
	o.btreeMap[indexName] = tree
	//if indexName == "PRIMARY" {
	//	o.indexMap[indexName] = index.NewPrimaryIndex(tree)
	//} else {
	//	o.indexMap[indexName] = index.NewSecondaryIndex(indexName, tree)
	//}

}

func (o OrdinaryTable) GetIndex(indexName string) basic.Index {
	return o.indexMap[indexName]
}
