package store

import (
	"fmt"
	"github.com/smartystreets/assertions"
	"testing"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

//测试256Extent
func TestFsp(t *testing.T) {

	t.Parallel()

	t.Run("testFsp", func(t *testing.T) {
		conf := conf.NewCfg()
		conf.DataDir = "/Users/zhukovasky/xmysql/data"
		conf.BaseDir = "/Users/zhukovasky/xmysql"

		ts := NewTableSpaceFile(conf, "RUNOOB", "student", 1, false, nil)

		//获取Fsp
		fsp := ts.GetFirstFsp()

		fmt.Println(fsp.GetFspSize())

		extentList := ts.GetFspFreeExtentList()

		fmt.Println(extentList.Size())
		assertions.ShouldEqual(extentList.Size(), 1)

		fspFragExtentList := ts.GetFspFreeFragExtentList()

		assertions.ShouldEqual(fspFragExtentList.Size(), 1)

	})
}
