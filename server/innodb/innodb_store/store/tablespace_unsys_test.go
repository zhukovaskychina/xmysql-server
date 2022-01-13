package store

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"testing"
)

func TestUnSysTableSpaces(t *testing.T) {
	t.Parallel()

	t.Run("createUnSysTableSpace", func(t *testing.T) {
		conf := conf.NewCfg()
		conf.DataDir = "/Users/zhukovasky/xmysql/data"
		conf.BaseDir = "/Users/zhukovasky/xmysql"
		var fileSystem = basic.NewFileSystem(conf)

		var bufferPool = buffer_pool.NewBufferPool(256*16384, 0.75, 0.25, 1000, fileSystem)

		ts := NewTableSpaceFile(conf, "RUNOOB", "student", 1, false, bufferPool)
		fileSystem.AddTableSpace(ts)
		//fmt.Println(ts.GetFspFreeExtentList())
		extentList := ts.GetFspFreeExtentList()
		fmt.Println(extentList.IsEmpty())
		fullFragExtentList := ts.GetFspFullFragExtentList()
		fmt.Println(fullFragExtentList.IsEmpty())
	})

}
