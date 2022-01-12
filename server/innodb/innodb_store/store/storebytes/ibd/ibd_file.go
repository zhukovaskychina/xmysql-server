package ibd

import (
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/innodb_store/store/storebytes/blocks"
)

//理论上应该是16384的整数倍
//

/****

当我们需要打开一张表时，需要从ibdata的数据词典表中load元数据信息，
其中SYS_INDEXES系统表中记录了表，索引，及索引根页对应的page no（DICT_FLD__SYS_INDEXES__PAGE_NO）
，进而找到btree根page，就可以对整个用户数据btree进行操作。
***/
type IBD_File struct {
	conf         *conf.Cfg
	tableName    string
	spaceId      uint32
	dataBaseName string
	isSys        bool
	filePath     string

	blockFile *blocks.BlockFile
}

//理论上初始化256MB
func NewIBDFile(tableName string, dataBaseName string, spaceId uint32, filePath string, fileName string, fileSize int64) *IBD_File {
	blockFile := blocks.NewBlockFile(filePath, fileName, fileSize)
	ibdFile := new(IBD_File)
	ibdFile.blockFile = blockFile
	return ibdFile
}
