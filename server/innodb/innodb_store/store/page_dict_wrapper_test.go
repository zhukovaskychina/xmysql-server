package store

import (
	"testing"
	"xmysql-server/server/conf"
	"xmysql-server/server/innodb/basic"
	"xmysql-server/server/innodb/buffer_pool"
)

func TestNewDataDictWrapper(t *testing.T) {

	t.Run("init dict innodb_store sysColumns", func(t *testing.T) {
		conf := conf.NewCfg()
		conf.DataDir = "/Users/zhukovasky/xmysql/data"
		conf.BaseDir = "/Users/zhukovasky/xmysql"
		var fileSystem = basic.NewFileSystem(conf)
		fileSystem.AddTableSpace(NewSysTableSpace(conf, true))

	})

	t.Run("test data dict innodb_store", func(t *testing.T) {
		conf := conf.NewCfg()
		conf.DataDir = "/Users/zhukovasky/xmysql/data"
		conf.BaseDir = "/Users/zhukovasky/xmysql"
		var fileSystem = basic.NewFileSystem(conf)
		fileSystem.AddTableSpace(NewSysTableSpace(conf, true))
		var bufferPool = buffer_pool.NewBufferPool(256*16384, 0.75, 0.25, 1000, fileSystem)
		dict := NewDictionarySys(bufferPool)

		dict.loadDictionary(bufferPool)

		//currentiterator, _ := dict.SysColumns.BTree.Iterate()
		//for _, currentRow, err, iterator, _ := currentiterator(); currentiterator != nil; _, currentRow, err, currentiterator, _ = currentiterator() {
		//	if err != nil {
		//		fmt.Println(err)
		//	}
		//
		//	fmt.Println(currentRow.ToString())
		//
		//}
		//fmt.Println("------------AAAAAAAAAAAAAAAAA----------------------")

	})
}
