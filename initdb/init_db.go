package initdb

import (
	"os"
	"xmysql-server/server/conf"
	"xmysql-server/server/innodb/innodb_store/store"
)

func InitDBDir(cfg *conf.Cfg) {
	InitSysSpace(cfg)
}

func InitSysSpace(conf *conf.Cfg) {

	os.Remove(conf.BaseDir + "/" + "ibdata1")

	store.NewSysTableSpace(conf, true)

	//storebytes.NewTableSpaceFile(conf,"mysql","innodb_index_stats",13,true)
	//storebytes.NewTableSpaceFile(conf,"mysql","innodb_table_stats",14,true)
}
