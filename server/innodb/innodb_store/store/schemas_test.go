package store

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"testing"
)

func TestSchemaInit(t *testing.T) {
	t.Parallel()
	t.Run("", func(t *testing.T) {
		t.Parallel()
		conf := conf.NewCfg()
		conf.DataDir = "/Users/zhukovasky/xmysql/data"
		conf.BaseDir = "/Users/zhukovasky/xmysql"
		var fileSystem = basic.NewFileSystem(conf)
		fileSystem.AddTableSpace(NewSysTableSpace(conf, true))

		var bufferPool = buffer_pool.NewBufferPool(256*16384, 0.75, 0.25, 1000, fileSystem)
		schemaManager := NewInfoSchemaManager(conf, bufferPool)

		//	schemaManager.GetAllSchemas()
		fmt.Println(schemaManager)
	})
}
