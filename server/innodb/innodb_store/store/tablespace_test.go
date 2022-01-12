package store

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"path"
	"testing"
	"xmysql-server/server/common"
	"xmysql-server/server/conf"
	"xmysql-server/server/innodb/basic"
	"xmysql-server/server/innodb/buffer_pool"
	"xmysql-server/server/innodb/innodb_store/store/storebytes/blocks"
	"xmysql-server/util"
)

func TestNewSysTableSpace(t *testing.T) {
	t.Parallel()

	t.Run("test First Data Block", func(t *testing.T) {
		cfg := conf.NewCfg()
		cfg.DataDir = "/Users/zhukovasky/xmysql/data"
		cfg.BaseDir = "/Users/zhukovasky/xmysql"
		sysTs := NewSysTableSpace(cfg, true)
		pageBytes, _ := sysTs.LoadPageByPageNumber(0)
		fmt.Println(pageBytes[0:4])
	})
	t.Run("8thData", func(t *testing.T) {
		cfg := conf.NewCfg()
		cfg.DataDir = "/Users/zhukovasky/xmysql/data"
		cfg.BaseDir = "/Users/zhukovasky/xmysql"
		sysTs := NewSysTableSpace(cfg, true)
		pageBytes, _ := sysTs.LoadPageByPageNumber(8)

		index := NewPageIndexByLoadBytesWithTuple(pageBytes, NewSysTableTuple()).(*Index)

		assert.Equal(t, len(index.ToByte()), common.PAGE_SIZE)

		assert.Equal(t, len(index.SlotRowData.FullRowList()), 3)
	})
	t.Run("10thData", func(t *testing.T) {
		cfg := conf.NewCfg()
		cfg.DataDir = "/Users/zhukovasky/xmysql/data"
		cfg.BaseDir = "/Users/zhukovasky/xmysql"
		sysTs := NewSysTableSpace(cfg, true)
		pageBytes, _ := sysTs.LoadPageByPageNumber(10)

		index := NewPageIndexByLoadBytesWithTuple(pageBytes, NewSysColumnsTuple()).(*Index)

		assert.Equal(t, len(index.ToByte()), common.PAGE_SIZE)

		assert.Equal(t, len(index.SlotRowData.FullRowList()), 3)
	})

	t.Run("test fsp fullList", func(t *testing.T) {
		cfg := conf.NewCfg()
		cfg.DataDir = "/Users/zhukovasky/xmysql/data"
		cfg.BaseDir = "/Users/zhukovasky/xmysql"

		var fileSystem = basic.NewFileSystem(cfg)

		var bufferPool = buffer_pool.NewBufferPool(256*16384, 0.75, 0.25, 1000, fileSystem)
		sysTs := NewSysTableSpaceByBufferPool(cfg, bufferPool)
		fileSystem.AddTableSpace(sysTs)

		fspFreeFragExtentList := sysTs.GetFspFreeFragExtentList()
		fmt.Println(fspFreeFragExtentList.Size())
	})
}

func TestFileIBData1(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = "/Users/zhukovasky/xmysql/data"
	cfg.BaseDir = "/Users/zhukovasky/xmysql"
	filePath := path.Join(cfg.BaseDir, "/", "ibdata1")
	isFlag, _ := util.PathExists(filePath)
	blockfile := blocks.NewBlockFile(cfg.BaseDir, "ibdata1", 256*64*16384)
	if isFlag {
		content, _ := blockfile.ReadPageByNumber(8)
		index := NewPageIndex(8).(*Index)
		index.IndexPage.FileHeader.FilePageArch = util.ConvertUInt4Bytes(8)
		blockfile.WriteContentByPage(8, index.IndexPage.GetSerializeBytes())
		content2, _ := blockfile.ReadPageByNumber(8)
		fmt.Println(content2[34:38])
		fmt.Println(content)
	}
}
